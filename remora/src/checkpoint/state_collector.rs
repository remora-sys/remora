use crate::checkpoint::{EpochId, EpochObjectStates};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::object::Object;

/// Concurrent in-memory state for snapshots and merged objects
pub struct StateCollector {
    /// Persisted state: Objects acknowledged by ALL proxies (stable, safe for recovery)
    /// This is the "commit point" - all proxies have reached consensus on these versions
    pub merged_state: DashMap<ObjectID, Object>,

    /// Temporary per-proxy state: Snapshots received but not yet fully acknowledged
    /// Key: (ProxyId, ObjectID) -> Object
    /// Once all proxies report for an epoch, temp states are promoted to merged_state
    temp_state_by_proxy: DashMap<(crate::proxy::core::ProxyId, ObjectID), Object>,

    /// Per-proxy persist index: tracks the last acknowledged epoch for each proxy
    /// Key: ProxyId, Value: last acknowledged epoch ID
    pub(crate) per_proxy_persist_index: DashMap<crate::proxy::core::ProxyId, AtomicU64>,

    /// Version ownership: tracks which proxy wrote each persisted version
    /// Key: (ObjectID, SequenceNumber) -> ProxyId
    /// Updated when promoting temp state to merged_state during snapshot merging
    /// Used during recovery to identify all versions owned by a failed proxy
    /// Public for recovery coordinator to check latest versions owned by healthy proxies
    pub(crate) version_ownership: DashMap<(ObjectID, SequenceNumber), crate::proxy::core::ProxyId>,

    /// Number of expected proxies (for determining when all have acknowledged)
    expected_proxies: usize,
}

impl StateCollector {
    pub fn new(expected_proxies: usize) -> Self {
        Self {
            merged_state: DashMap::new(),
            temp_state_by_proxy: DashMap::new(),
            per_proxy_persist_index: DashMap::new(),
            version_ownership: DashMap::new(),
            expected_proxies,
        }
    }

    /// Process a state snapshot from a proxy
    ///
    /// Two-phase commit protocol:
    /// 1. Store snapshot in temp_state_by_proxy (per-proxy temporary storage)
    /// 2. Check if ALL proxies have reported for this epoch
    /// 3. If yes, promote temp states to merged_state (persisted, stable)
    ///
    /// This ensures merged_state only contains versions acknowledged by all proxies,
    /// eliminating version gaps during recovery.
    pub fn process_snapshot<T>(
        &self,
        proxy_id: crate::proxy::core::ProxyId,
        epoch: EpochId,
        snapshot: EpochObjectStates,
        _expected_proxies: usize,
        _epoch_logger: Option<&crate::recovery::EpochLogger<T>>,
    ) where
        T: crate::executor::api::ExecutableTransaction + Clone,
    {
        tracing::info!(
            "Process_snapshot: Received snapshot from proxy {} for epoch {}: {} objects",
            proxy_id,
            epoch.0,
            snapshot.len()
        );

        // Phase 1: Store in temporary per-proxy state
        for (obj_id, obj) in snapshot.into_iter() {
            self.temp_state_by_proxy.insert((proxy_id, obj_id), obj);
        }

        // Update this proxy's persist index to the epoch just completed
        let consensus_index = epoch.0;
        self.per_proxy_persist_index
            .entry(proxy_id)
            .or_insert_with(|| AtomicU64::new(0))
            .store(consensus_index, Ordering::SeqCst);

        tracing::info!(
            "Stored temp snapshot for proxy {} at epoch {}",
            proxy_id,
            epoch.0
        );

        // Phase 2: Check if all proxies have reached this epoch
        // If yes, promote temp states to merged_state (commit point)
        if self.is_epoch_complete(epoch, self.expected_proxies) {
            self.commit_epoch(epoch);
        }
    }

    /// Commit an epoch: promote all temp states to merged_state
    /// This happens when ALL proxies have acknowledged the epoch
    fn commit_epoch(&self, epoch: EpochId) {
        let mut committed_count = 0;

        // Collect all object IDs and their latest versions from temp state
        // Track which proxy wrote each latest version for recovery
        let mut objects_by_id: std::collections::HashMap<
            ObjectID,
            (SequenceNumber, Object, crate::proxy::core::ProxyId),
        > = std::collections::HashMap::new();

        // Scan all temp states to find latest version of each object
        for entry in self.temp_state_by_proxy.iter() {
            let ((proxy_id, obj_id), obj) = entry.pair();
            let version = obj.version();

            // Keep the latest version for each object, and track which proxy wrote it
            objects_by_id
                .entry(*obj_id)
                .and_modify(|(current_version, current_obj, writer_proxy)| {
                    if version > *current_version {
                        *current_version = version;
                        *current_obj = obj.clone();
                        *writer_proxy = *proxy_id;
                    }
                })
                .or_insert((version, obj.clone(), *proxy_id));
        }

        // Commit all latest versions to merged_state and update version_ownership
        for (obj_id, (version, obj, writer_proxy)) in objects_by_id {
            // Insert the new latest version
            self.merged_state.insert(obj_id, obj);

            // Record which proxy wrote this version
            // Note: We don't remove old version entries because:
            // 1. Cleanup cost is O(N×M) - extremely expensive with millions of entries
            // 2. Old entries don't affect correctness - get_versions_by_proxy filters to latest
            // 3. Storage cost is negligible compared to CPU cost of iteration
            // 4. Old entries will be naturally pruned when objects are eventually deleted
            self.version_ownership
                .insert((obj_id, version), writer_proxy);
            committed_count += 1;
        }

        // Clear temp states for committed objects (optional optimization)
        // Keep temp states for now to support partial epochs during recovery

        tracing::info!(
            "Committed epoch {} to merged_state: {} objects promoted",
            epoch.0,
            committed_count
        );
    }

    /// Get an object from the in-memory store (persisted state only).
    pub fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        self.merged_state.get(object_id).map(|e| e.clone())
    }

    /// Get an object for a specific proxy at a specific version.
    /// ONLY returns the exact version if it was written by the specified proxy.
    ///
    /// Checks:
    /// 1. Proxy's temp_state (for uncommitted versions)
    /// 2. merged_state with version_ownership verification
    ///
    /// This ensures we only return objects that truly belong to the proxy,
    /// preventing duplicates with different versions during recovery.
    pub fn get_object_for_proxy(
        &self,
        object_id: &ObjectID,
        version: SequenceNumber,
        proxy_id: crate::proxy::core::ProxyId,
    ) -> Option<Object> {
        // First try this specific proxy's temp state (for uncommitted versions)
        if let Some(obj) = self.temp_state_by_proxy.get(&(proxy_id, *object_id)) {
            if obj.version() == version {
                return Some(obj.clone());
            }
            // Don't return if version doesn't match - wrong version
        }

        // Then try persisted state (for committed versions)
        // CRITICAL: Verify ownership - only return if this proxy wrote this version
        if let Some(obj) = self.merged_state.get(object_id) {
            if obj.version() == version {
                // Exact version match - now verify ownership
                if let Some(writer) = self.version_ownership.get(&(*object_id, version)) {
                    if *writer.value() == proxy_id {
                        return Some(obj.clone());
                    } else {
                        tracing::debug!(
                            "Object {:?} v{} found but was written by proxy {}, not {}",
                            object_id,
                            version.value(),
                            writer.value(),
                            proxy_id
                        );
                    }
                }
                // No ownership record or wrong owner - don't return
            }
            // Don't return newer/older versions - must be exact match owned by this proxy
        }

        // Version not found or not owned by this proxy
        None
    }

    /// Get the persisted version for an object without cloning the entire object.
    pub fn get_persisted_version(&self, object_id: &ObjectID) -> Option<SequenceNumber> {
        self.merged_state.get(object_id).map(|e| e.version())
    }

    /// Get all (ObjectID, SequenceNumber) pairs owned by a specific proxy.
    /// Used during recovery to identify all versions that need to be transferred to standby.
    ///
    /// Returns versions from both:
    /// 1. version_ownership (committed versions)
    /// 2. temp_state_by_proxy (uncommitted versions from incomplete epochs)
    pub fn get_versions_by_proxy(
        &self,
        proxy_id: crate::proxy::core::ProxyId,
    ) -> Vec<(ObjectID, SequenceNumber)> {
        use std::collections::HashMap;

        // Use HashMap to keep only the LATEST version per object
        // Key: ObjectID, Value: SequenceNumber
        let mut versions: HashMap<ObjectID, SequenceNumber> = HashMap::new();

        // Collect from version_ownership (committed versions)
        for entry in self.version_ownership.iter() {
            let ((obj_id, version), writer) = entry.pair();
            if *writer == proxy_id {
                versions
                    .entry(*obj_id)
                    .and_modify(|v| {
                        if *version > *v {
                            *v = *version;
                        }
                    })
                    .or_insert(*version);
            }
        }

        // Also collect from temp_state_by_proxy (uncommitted versions)
        // These are important for incomplete epochs that never committed
        for entry in self.temp_state_by_proxy.iter() {
            let ((pid, obj_id), obj) = entry.pair();
            if *pid == proxy_id {
                let version = obj.version();
                versions
                    .entry(*obj_id)
                    .and_modify(|v| {
                        if version > *v {
                            *v = version;
                        }
                    })
                    .or_insert(version);
            }
        }

        versions.into_iter().collect()
    }

    /// Current number of objects in memory.
    pub fn merged_state_len(&self) -> usize {
        self.merged_state.len()
    }

    /// Check if an epoch is complete (all proxies have reported at least this epoch).
    /// Returns true if ALL proxies (including failed ones) have a persist_index >= the epoch.
    pub fn is_epoch_complete(&self, epoch: EpochId, expected_proxies: usize) -> bool {
        if self.per_proxy_persist_index.len() < expected_proxies {
            return false;
        }

        // BUG FIX: Check the MINIMUM persist_index across ALL proxies.
        //
        // Problem: .take(expected_proxies) uses arbitrary DashMap iteration order.
        // When a proxy fails but stays in the map (stuck at old epoch) and standby is promoted,
        // .take(N) might skip the failed proxy, allowing merged_state to advance incorrectly.
        //
        // Solution: Check if minimum persist_index >= epoch. This ensures ALL proxies,
        // including any failed ones, have advanced. A failed proxy acts as a "brake" to keep
        // merged_state frozen at the safe snapshot point for recovery.
        let min_persist_index = self
            .per_proxy_persist_index
            .iter()
            .map(|entry| entry.value().load(Ordering::SeqCst))
            .min()
            .unwrap_or(0);

        let complete = min_persist_index >= epoch.0;

        tracing::debug!(
            "Epoch {} completion check: min_persist_index={}, complete={}",
            epoch.0,
            min_persist_index,
            complete
        );
        complete
    }

    /// Get the current primary persist index (replay cut).
    ///
    /// This returns the minimum persist_index across all proxies, which is
    /// the safe point for pruning - we can only prune transactions that all
    /// proxies have completed and acknowledged.
    pub fn get_persist_index(&self) -> u64 {
        if self.per_proxy_persist_index.is_empty() {
            return 0;
        }

        self.per_proxy_persist_index
            .iter()
            .map(|entry| entry.value().load(Ordering::SeqCst))
            .min()
            .unwrap_or(0)
    }

    /// Get the persist index for a specific proxy (for debugging/diagnostics).
    pub fn get_proxy_persist_index(&self, proxy_id: crate::proxy::core::ProxyId) -> u64 {
        self.per_proxy_persist_index
            .get(&proxy_id)
            .map(|atomic| atomic.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Get the persist index excluding a specific proxy.
    /// This is used during recovery to calculate the persist_index without
    /// including the failed proxy, which would otherwise prevent finding
    /// dirty transactions for that proxy.
    pub fn get_persist_index_excluding(&self, excluded_proxy: crate::proxy::core::ProxyId) -> u64 {
        if self.per_proxy_persist_index.is_empty() {
            return 0;
        }

        self.per_proxy_persist_index
            .iter()
            .filter(|entry| *entry.key() != excluded_proxy)
            .map(|entry| entry.value().load(Ordering::SeqCst))
            .min()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui_types::base_types::ObjectID;
    use sui_types::object::Object;

    fn create_test_object(id: ObjectID) -> Object {
        Object::immutable_with_id_for_testing(id)
    }

    #[test]
    fn test_state_collector_new() {
        let collector = StateCollector::new(3);
        assert_eq!(collector.merged_state_len(), 0);
    }

    #[test]
    fn test_state_collector_with_store() {
        let collector = StateCollector::new(2);
        assert_eq!(collector.merged_state_len(), 0);
    }

    #[test]
    fn test_state_collector_process_snapshot() {
        let collector = StateCollector::new(2);

        // Create test objects
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj1 = create_test_object(obj_id1);
        let obj2 = create_test_object(obj_id2);

        let mut snapshot = EpochObjectStates::new();
        snapshot.insert(obj_id1, obj1);
        snapshot.insert(obj_id2, obj2);

        // Process snapshot from proxy 1
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            EpochId(5),
            snapshot.clone(),
            2,
            None,
        );
        // Check proxy 1's persist index
        assert_eq!(collector.get_proxy_persist_index(1), 5);
        // merged_state should be EMPTY until all proxies report (two-phase commit)
        assert_eq!(collector.merged_state_len(), 0);

        // Process snapshot from proxy 2
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            2,
            EpochId(5),
            snapshot,
            2,
            None,
        );
        // Check proxy 2's persist index
        assert_eq!(collector.get_proxy_persist_index(2), 5);
        // Minimum should be 5
        assert_eq!(collector.get_persist_index(), 5);
        // NOW merged_state should contain both objects (all proxies reported)
        assert_eq!(collector.merged_state_len(), 2);
    }

    #[test]
    fn test_state_collector_multiple_epochs() {
        let collector = StateCollector::new(2);

        // Process snapshots for different epochs - no ordering required
        let snapshot = EpochObjectStates::new();
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            EpochId(6),
            snapshot.clone(),
            2,
            None,
        );

        // Proxy 1 should be at epoch 6
        assert_eq!(collector.get_proxy_persist_index(1), 6);

        // Process snapshot for epoch 5 from proxy 2 - out of order is fine
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            2,
            EpochId(5),
            snapshot,
            2,
            None,
        );

        // Proxy 2 should be at epoch 5
        assert_eq!(collector.get_proxy_persist_index(2), 5);
        // Minimum should be 5
        assert_eq!(collector.get_persist_index(), 5);
    }

    #[test]
    fn test_state_collector_per_proxy_tracking() {
        let collector = StateCollector::new(3);

        // Process snapshots from different proxies at different epochs
        let snapshot = EpochObjectStates::new();

        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            0,
            EpochId(10),
            snapshot.clone(),
            3,
            None,
        );
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            EpochId(5),
            snapshot.clone(),
            3,
            None,
        );
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            2,
            EpochId(7),
            snapshot,
            3,
            None,
        );

        // Check individual persist indices
        assert_eq!(collector.get_proxy_persist_index(0), 10);
        assert_eq!(collector.get_proxy_persist_index(1), 5);
        assert_eq!(collector.get_proxy_persist_index(2), 7);

        // Global persist index should be minimum (5)
        assert_eq!(collector.get_persist_index(), 5);
    }

    #[test]
    fn test_state_collector_merge_snapshots() {
        let collector = StateCollector::new(2);

        // Create snapshots with overlapping objects
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj_id3 = ObjectID::random();

        let obj1_v1 = create_test_object(obj_id1);
        let obj1_v2 = create_test_object(obj_id1); // Same ID, different object
        let obj2 = create_test_object(obj_id2);
        let obj3 = create_test_object(obj_id3);

        // Proxy 1 snapshot
        let mut snapshot1 = EpochObjectStates::new();
        snapshot1.insert(obj_id1, obj1_v1);
        snapshot1.insert(obj_id2, obj2);

        // Proxy 2 snapshot (overlaps with obj_id1)
        let mut snapshot2 = EpochObjectStates::new();
        snapshot2.insert(obj_id1, obj1_v2); // Different version of obj_id1
        snapshot2.insert(obj_id3, obj3);

        // Process both snapshots
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            EpochId(5),
            snapshot1,
            2,
            None,
        );
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            2,
            EpochId(5),
            snapshot2,
            2,
            None,
        );

        // Both proxies should be at epoch 5
        assert_eq!(collector.get_proxy_persist_index(1), 5);
        assert_eq!(collector.get_proxy_persist_index(2), 5);
        // All 3 objects should be in merged state (last-writer-wins for obj_id1)
        assert_eq!(collector.merged_state_len(), 3);
    }

    #[test]
    fn test_per_proxy_independent_progress() {
        // Test that each proxy can progress independently through epochs
        let collector = StateCollector::new(3);
        let snapshot = EpochObjectStates::new();

        // Proxy 0 reports up to epoch 10
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            0,
            EpochId(10),
            snapshot.clone(),
            3,
            None,
        );

        // Proxy 1 reports up to epoch 5
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            EpochId(5),
            snapshot.clone(),
            3,
            None,
        );

        // Proxy 2 reports up to epoch 7
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            2,
            EpochId(7),
            snapshot,
            3,
            None,
        );

        // Each proxy should track its own progress
        assert_eq!(collector.get_proxy_persist_index(0), 10);
        assert_eq!(collector.get_proxy_persist_index(1), 5);
        assert_eq!(collector.get_proxy_persist_index(2), 7);

        // Global persist index is the minimum (safe point for pruning)
        assert_eq!(collector.get_persist_index(), 5);
    }
}
