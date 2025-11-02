use crate::checkpoint::{EpochId, EpochObjectStates};
use crate::proxy::core::ProxyId;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::object::Object;

/// Concurrent in-memory state for snapshots and merged objects
pub struct StateCollector {
    /// Persisted state: Objects acknowledged by ALL proxies (stable, safe for recovery)
    /// This is the "commit point" - all proxies have reached consensus on these versions
    pub merged_state: DashMap<ObjectID, Object>,

    /// Temporary state, grouped by epoch: (EpochId -> (ProxyId, ObjectID) -> Object)
    /// This structure isolates epochs to prevent race conditions during commits.
    /// An epoch's data is atomically removed from here and processed by `commit_epoch`.
    temp_state_by_epoch: DashMap<EpochId, DashMap<(ProxyId, ObjectID), Object>>,

    /// Per-proxy persist index: tracks the last acknowledged epoch for each proxy
    /// Key: ProxyId, Value: last acknowledged epoch ID
    pub(crate) per_proxy_persist_index: DashMap<ProxyId, AtomicU64>,

    /// Number of expected proxies (for determining when all have acknowledged)
    expected_proxies: usize,
    /// The last epoch that was successfully committed to merged_state.
    last_committed_epoch: AtomicU64,
}

impl StateCollector {
    pub fn new(expected_proxies: usize) -> Self {
        Self {
            merged_state: DashMap::new(),
            temp_state_by_epoch: DashMap::new(),
            per_proxy_persist_index: DashMap::new(),
            expected_proxies,
            last_committed_epoch: AtomicU64::new(0),
        }
    }

    /// Process a state snapshot from a proxy
    ///
    /// Two-phase commit protocol:
    /// 1. Store snapshot in temp_state_by_epoch (per-proxy temporary storage)
    /// 2. Check if ALL proxies have reported for this epoch
    /// 3. If yes, promote temp states to merged_state (persisted, stable)
    ///
    /// This ensures merged_state only contains versions acknowledged by all proxies,
    /// eliminating version gaps during recovery.
    pub fn process_snapshot<T>(
        &self,
        proxy_id: ProxyId,
        completed_up_to: u64,
        snapshot: EpochObjectStates,
        _expected_proxies: usize,
        epoch_logger: Option<&crate::recovery::EpochLogger<T>>,
    ) where
        T: crate::executor::api::ExecutableTransaction + Clone,
    {
        let last_committed = self.last_committed_epoch.load(Ordering::SeqCst);
        let is_stale = completed_up_to > 0 && completed_up_to <= last_committed;

        if is_stale {
            tracing::warn!(
                "Received stale snapshot for already-committed epoch {}. Last committed epoch is {}. Discarding snapshot data.",
                completed_up_to,
                last_committed
            );
        } else if completed_up_to > 0 {
            // Only store snapshot data if it's for a future, non-stale epoch.
            let epoch = EpochId(completed_up_to);
            let epoch_state = self.temp_state_by_epoch.entry(epoch).or_default();
            for (obj_id, obj) in snapshot.into_iter() {
                tracing::debug!(
                    "Storing temp snapshot for proxy {} in epoch {} with obj_id {}: {}",
                    proxy_id,
                    epoch.0,
                    obj_id,
                    obj.version().value()
                );
                epoch_state.insert((proxy_id, obj_id), obj);
            }
        }

        // Atomically update the persist index for the reporting proxy.
        self.per_proxy_persist_index
            .entry(proxy_id)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_max(completed_up_to, Ordering::SeqCst);

        // ALWAYS run the commit loop. The index update (even from a stale snapshot)
        // might be what completes the next sequential epoch.
        loop {
            let current_last_committed = self.last_committed_epoch.load(Ordering::SeqCst);
            let next_epoch_to_commit = current_last_committed + 1;

            // If the next epoch isn't ready to commit, we're done.
            if !self.is_epoch_complete(EpochId(next_epoch_to_commit), self.expected_proxies) {
                break;
            }

            // Try to atomically advance the committed epoch. If we fail, another
            // thread has already done it, so we can stop.
            if self
                .last_committed_epoch
                .compare_exchange(
                    current_last_committed,
                    next_epoch_to_commit,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_err()
            {
                break;
            }

            // We successfully claimed this epoch, so now we can commit it.
            self.commit_epoch(EpochId(next_epoch_to_commit), epoch_logger);
        }
    }

    /// Commit an epoch: promote all temp states to merged_state
    /// This happens when ALL proxies have acknowledged the epoch
    fn commit_epoch<T>(
        &self,
        epoch: EpochId,
        epoch_logger: Option<&crate::recovery::EpochLogger<T>>,
    ) where
        T: crate::executor::api::ExecutableTransaction + Clone,
    {
        // Atomically remove the epoch's data to ensure we are the only ones processing it.
        if let Some((_, epoch_state)) = self.temp_state_by_epoch.remove(&epoch) {
            let mut committed_count = 0;

            // Collect all object IDs and their latest versions from the isolated epoch state.
            let mut objects_by_id: std::collections::HashMap<
                ObjectID,
                (SequenceNumber, Object, ProxyId),
            > = std::collections::HashMap::new();

            // Scan all temp states for this epoch to find the latest version of each object.
            for entry in epoch_state.iter() {
                let ((proxy_id, obj_id), obj) = entry.pair();
                let version = obj.version();

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

            // Commit all latest versions to merged_state.
            for (obj_id, (version, obj, _writer_proxy)) in objects_by_id {
                if let Some(existing_obj) = self.merged_state.get(&obj_id) {
                    if existing_obj.version() >= version {
                        tracing::warn!(
                            "Stale version update rejected for obj_id {}: existing_version={}, new_version={}",
                            obj_id,
                            existing_obj.version(),
                            version
                        );
                        continue; // Skip insertion
                    }
                }
                tracing::debug!(
                    "Inserting new latest version for obj_id {}: {}",
                    obj_id,
                    version.value()
                );
                self.merged_state.insert(obj_id, obj);
                committed_count += 1;
            }

            tracing::info!(
                "Committed epoch {} to merged_state: {} objects promoted",
                epoch.0,
                committed_count
            );

            // Prune the epoch logger after successful commit.
            if let Some(logger) = epoch_logger {
                logger.prune_epoch(epoch);
                tracing::info!("Pruned epoch {} from epoch logger after commit", epoch.0);
            }
        }
    }

    /// Get an object from the in-memory store (persisted state only).
    pub fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        self.merged_state.get(object_id).map(|e| e.clone())
    }

    /// Get the persisted version for an object without cloning the entire object.
    pub fn get_persisted_version(&self, object_id: &ObjectID) -> Option<SequenceNumber> {
        self.merged_state.get(object_id).map(|e| e.version())
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
    /// This returns the last epoch that was successfully committed to merged_state,
    /// which represents the stable "commit point" for the entire system. It is the
    /// safe point for pruning or recovery.
    pub fn get_persist_index(&self) -> u64 {
        self.last_committed_epoch.load(Ordering::SeqCst)
    }

    /// Get the persist index for a specific proxy (for debugging/diagnostics).
    pub fn get_proxy_persist_index(&self, proxy_id: ProxyId) -> u64 {
        self.per_proxy_persist_index
            .get(&proxy_id)
            .map(|atomic| atomic.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Get the persist index excluding a specific proxy.
    /// This is used during recovery to calculate the persist_index without
    /// including the failed proxy, which would otherwise prevent finding
    /// dirty transactions for that proxy.
    pub fn get_persist_index_excluding(&self, excluded_proxy: ProxyId) -> u64 {
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

    fn create_test_object_with_version(id: ObjectID, version: u64) -> Object {
        use sui_types::object::MoveObject;
        let v = SequenceNumber::from(version);
        let o = MoveObject::new_gas_coin(v, id, 100);
        Object::new_move(o, sui_types::object::Owner::Immutable, Default::default())
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

        // Process snapshot from proxy 1 with completed_up_to = 5
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            5,
            snapshot.clone(),
            2,
            None,
        );
        // Check proxy 1's persist index
        assert_eq!(collector.get_proxy_persist_index(1), 5);
        // merged_state should be EMPTY until all proxies report (two-phase commit)
        assert_eq!(collector.merged_state_len(), 0);

        // Process snapshot from proxy 2
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(2, 5, snapshot, 2, None);
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

        // Process snapshots for different batches - no ordering required
        let snapshot = EpochObjectStates::new();
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            6,
            snapshot.clone(),
            2,
            None,
        );

        // Proxy 1 should be at completed_up_to 6
        assert_eq!(collector.get_proxy_persist_index(1), 6);

        // Process snapshot for batch 5 from proxy 2 - out of order is fine
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(2, 5, snapshot, 2, None);

        // Proxy 2 should be at completed_up_to 5
        assert_eq!(collector.get_proxy_persist_index(2), 5);
        // Minimum should be 5
        assert_eq!(collector.get_persist_index(), 5);
    }

    #[test]
    fn test_state_collector_per_proxy_tracking() {
        let collector = StateCollector::new(3);

        // Process snapshots from different proxies at different completed_up_to values
        let snapshot = EpochObjectStates::new();

        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            0,
            10,
            snapshot.clone(),
            3,
            None,
        );
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            5,
            snapshot.clone(),
            3,
            None,
        );
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(2, 7, snapshot, 3, None);

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
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(1, 5, snapshot1, 2, None);
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(2, 5, snapshot2, 2, None);

        // Both proxies should be at completed_up_to 5
        assert_eq!(collector.get_proxy_persist_index(1), 5);
        assert_eq!(collector.get_proxy_persist_index(2), 5);
        // All 3 objects should be in merged state (last-writer-wins for obj_id1)
        assert_eq!(collector.merged_state_len(), 3);
    }

    #[test]
    fn test_per_proxy_independent_progress() {
        // Test that each proxy can progress independently through batches
        let collector = StateCollector::new(3);
        let snapshot = EpochObjectStates::new();

        // Proxy 0 reports up to batch 10
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            0,
            10,
            snapshot.clone(),
            3,
            None,
        );

        // Proxy 1 reports up to batch 5
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            5,
            snapshot.clone(),
            3,
            None,
        );

        // Proxy 2 reports up to batch 7
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(2, 7, snapshot, 3, None);

        // Each proxy should track its own progress
        assert_eq!(collector.get_proxy_persist_index(0), 10);
        assert_eq!(collector.get_proxy_persist_index(1), 5);
        assert_eq!(collector.get_proxy_persist_index(2), 7);

        // Global persist index is the minimum (safe point for pruning)
        assert_eq!(collector.get_persist_index(), 5);
    }

    #[test]
    fn test_initial_snapshot_with_zero_completed_up_to() {
        // Test that initial snapshots with completed_up_to = 0 don't trigger commit
        let collector = StateCollector::new(2);

        // Create test objects
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj1 = create_test_object(obj_id1);
        let obj2 = create_test_object(obj_id2);

        let mut snapshot = EpochObjectStates::new();
        snapshot.insert(obj_id1, obj1);
        snapshot.insert(obj_id2, obj2);

        // Both proxies send initial snapshots with completed_up_to = 0
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            0,
            0,
            snapshot.clone(),
            2,
            None,
        );

        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(1, 0, snapshot, 2, None);

        // Both proxies should be at completed_up_to 0
        assert_eq!(collector.get_proxy_persist_index(0), 0);
        assert_eq!(collector.get_proxy_persist_index(1), 0);
        assert_eq!(collector.get_persist_index(), 0);

        // IMPORTANT: merged_state should be EMPTY because completed_up_to = 0
        // means no batches have been completed yet, so we shouldn't commit
        assert_eq!(collector.merged_state_len(), 0);
    }

    #[test]
    fn test_initial_then_real_snapshots() {
        // Test the progression: initial snapshot (0) -> real snapshot (1+)
        let collector = StateCollector::new(2);

        let obj_id1 = ObjectID::random();
        let obj1_v1 = create_test_object(obj_id1);
        let obj1_v2 = create_test_object(obj_id1);

        // Initial snapshots with completed_up_to = 0
        let mut initial_snapshot = EpochObjectStates::new();
        initial_snapshot.insert(obj_id1, obj1_v1);

        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            0,
            0,
            initial_snapshot.clone(),
            2,
            None,
        );
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            0,
            initial_snapshot,
            2,
            None,
        );

        // No commit should happen
        assert_eq!(collector.merged_state_len(), 0);

        // Now proxies complete batch 1 and send real snapshots
        let mut real_snapshot = EpochObjectStates::new();
        real_snapshot.insert(obj_id1, obj1_v2);

        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            0,
            1,
            real_snapshot.clone(),
            2,
            None,
        );

        // Still no commit (only 1 proxy reported batch 1)
        assert_eq!(collector.merged_state_len(), 0);

        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            1,
            real_snapshot,
            2,
            None,
        );

        // NOW commit should happen (both proxies at batch 1)
        assert_eq!(collector.get_persist_index(), 1);
        assert_eq!(collector.merged_state_len(), 1);
    }

    #[test]
    fn test_mixed_zero_and_nonzero_snapshots() {
        // Test when one proxy is at 0 and another is ahead
        let collector = StateCollector::new(2);

        let snapshot = EpochObjectStates::new();

        // Proxy 0 sends initial snapshot (completed_up_to = 0)
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            0,
            0,
            snapshot.clone(),
            2,
            None,
        );

        // Proxy 1 has already completed batch 5
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(1, 5, snapshot, 2, None);

        // Persist indices should be tracked independently
        assert_eq!(collector.get_proxy_persist_index(0), 0);
        assert_eq!(collector.get_proxy_persist_index(1), 5);

        // Global persist index should be the minimum (0)
        assert_eq!(collector.get_persist_index(), 0);

        // No commit should happen (can't commit batch 0)
        assert_eq!(collector.merged_state_len(), 0);
    }

    #[test]
    fn test_epoch_commit_is_isolated() {
        let collector = StateCollector::new(2);

        // --- Epoch 5 Setup ---
        let obj_id_5 = ObjectID::random();
        let obj_5 = create_test_object(obj_id_5);
        let mut snapshot_5 = EpochObjectStates::new();
        snapshot_5.insert(obj_id_5, obj_5);

        // --- Epoch 6 Setup ---
        let obj_id_6 = ObjectID::random();
        let obj_6 = create_test_object(obj_id_6);
        let mut snapshot_6 = EpochObjectStates::new();
        snapshot_6.insert(obj_id_6, obj_6);

        // --- Simulate Race ---

        // 1. Proxy 1 reports for epoch 5. This should not trigger a commit.
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            5,
            snapshot_5.clone(),
            2,
            None,
        );
        assert_eq!(collector.merged_state_len(), 0);
        assert!(collector.temp_state_by_epoch.contains_key(&EpochId(5)));

        // 2. Before epoch 5 is committed, proxy 1 reports for epoch 6.
        // This should also not trigger a commit.
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            1,
            6,
            snapshot_6.clone(),
            2,
            None,
        );
        assert_eq!(collector.merged_state_len(), 0);
        assert!(collector.temp_state_by_epoch.contains_key(&EpochId(6)));

        // 3. Now, proxy 2 reports for epoch 5. This SHOULD trigger the commit for epoch 5.
        collector.process_snapshot::<crate::executor::fake::FakeTransaction>(
            2,
            5,
            snapshot_5.clone(),
            2,
            None,
        );

        // --- Assertions ---

        // The commit for epoch 5 should be complete.
        assert_eq!(collector.get_persist_index(), 5);

        // merged_state should contain ONLY the object from epoch 5.
        assert_eq!(collector.merged_state_len(), 1);
        assert!(collector.get_object(&obj_id_5).is_some());
        assert!(collector.get_object(&obj_id_6).is_none()); // Crucial check

        // The temp state for epoch 5 should be gone.
        assert!(!collector.temp_state_by_epoch.contains_key(&EpochId(5)));

        // The temp state for epoch 6 should still be there.
    }

    #[test]
    fn test_out_of_order_commit_preserves_latest_version() {
        let collector = StateCollector::new(2);
        let obj_id = ObjectID::random();

        // 1. Simulate commit of a newer version (v4 from epoch 3)
        let epoch_3_state = DashMap::new();
        let obj_v4 = create_test_object_with_version(obj_id, 4);
        epoch_3_state.insert((0, obj_id), obj_v4);
        collector
            .temp_state_by_epoch
            .insert(EpochId(3), epoch_3_state);

        collector.commit_epoch::<crate::executor::fake::FakeTransaction>(EpochId(3), None);

        // Assert that merged_state has v4
        assert_eq!(
            collector.get_persisted_version(&obj_id),
            Some(SequenceNumber::from(4))
        );

        // 2. Simulate a delayed, out-of-order commit of an older version (v3 from epoch 2)
        let epoch_2_state = DashMap::new();
        let obj_v3 = create_test_object_with_version(obj_id, 3);
        epoch_2_state.insert((0, obj_id), obj_v3);
        collector
            .temp_state_by_epoch
            .insert(EpochId(2), epoch_2_state);

        collector.commit_epoch::<crate::executor::fake::FakeTransaction>(EpochId(2), None);

        // 3. Assert that the stale update was rejected
        // The version in merged_state should still be v4, not overwritten by v3.
        assert_eq!(
            collector.get_persisted_version(&obj_id),
            Some(SequenceNumber::from(4)),
            "Stale version v3 should have been rejected"
        );
    }

    #[test]
    fn test_late_duplicate_snapshot_is_ignored() {
        let collector = StateCollector::new(1);
        let obj_id = ObjectID::random();
        let obj_v5 = create_test_object_with_version(obj_id, 5);
        let mut snapshot_5 = EpochObjectStates::new();
        snapshot_5.insert(obj_id, obj_v5);

        // 1. Process a snapshot for epoch 5.
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(0, 5, snapshot_5, 1, None);

        // Assert that the proxy's index is 5 and epoch 5 was committed.
        assert_eq!(collector.get_proxy_persist_index(0), 5);
        assert_eq!(collector.last_committed_epoch.load(Ordering::SeqCst), 5);
        assert!(collector.temp_state_by_epoch.is_empty()); // Should be cleaned up after commit.

        // 2. Process a late/duplicate snapshot for epoch 4.
        let obj_v4 = create_test_object_with_version(obj_id, 4);
        let mut snapshot_4 = EpochObjectStates::new();
        snapshot_4.insert(obj_id, obj_v4);
        collector
            .process_snapshot::<crate::executor::fake::FakeTransaction>(0, 4, snapshot_4, 1, None);

        // 3. Assert that the state has not changed.
        // The proxy's index should NOT regress to 4.
        assert_eq!(collector.get_proxy_persist_index(0), 5);
        // The last committed epoch should still be 5.
        assert_eq!(collector.last_committed_epoch.load(Ordering::SeqCst), 5);
        // No new temp state for epoch 4 should have been created.
        assert!(!collector.temp_state_by_epoch.contains_key(&EpochId(4)));
    }
}
