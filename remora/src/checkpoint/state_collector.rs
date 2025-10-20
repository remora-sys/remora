use crate::checkpoint::{EpochId, EpochObjectStates};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::object::Object;

/// Concurrent in-memory state for snapshots and merged objects
pub struct StateCollector {
    /// In-memory latest object states (no disk persistence)
    pub merged_state: DashMap<ObjectID, Object>,
    /// Per-proxy persist index: tracks the last acknowledged epoch for each proxy
    /// Key: ProxyId, Value: last acknowledged epoch ID
    per_proxy_persist_index: DashMap<crate::proxy::core::ProxyId, AtomicU64>,
}

impl StateCollector {
    pub fn new(_expected_proxies: usize) -> Self {
        Self {
            merged_state: DashMap::new(),
            per_proxy_persist_index: DashMap::new(),
        }
    }

    /// Process a state snapshot from a proxy
    ///
    /// This updates the per-proxy persist index for the proxy that reported the snapshot.
    /// Since each proxy reports snapshots in order, we can simply update that proxy's
    /// persist index without complex buffering logic.
    ///
    /// Note: Pruning is handled separately by the caller (e.g., LoadBalancer::prune_epoch_logger)
    /// to avoid inefficient per-snapshot pruning.
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

        // Move directly into the in-memory merged state (no per-epoch merge)
        for (obj_id, obj) in snapshot.into_iter() {
            self.merged_state.insert(obj_id, obj);
        }

        // Update this proxy's persist index to the epoch just completed
        // Since proxies report epochs in order, we can directly update without checks
        let consensus_index = epoch.0;

        // Initialize or update the proxy's persist index
        self.per_proxy_persist_index
            .entry(proxy_id)
            .or_insert_with(|| AtomicU64::new(0))
            .store(consensus_index, Ordering::SeqCst);

        tracing::info!(
            "Updated persist_index for proxy {} to {} (epoch {})",
            proxy_id,
            consensus_index,
            epoch.0
        );
    }

    /// Get an object from the in-memory store.
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
    /// Returns true if all proxies have a persist_index >= the epoch.
    pub fn is_epoch_complete(&self, epoch: EpochId, expected_proxies: usize) -> bool {
        if self.per_proxy_persist_index.len() < expected_proxies {
            return false;
        }

        let complete = self
            .per_proxy_persist_index
            .iter()
            .take(expected_proxies)
            .all(|entry| entry.value().load(Ordering::SeqCst) >= epoch.0);

        tracing::info!("Epoch {} completion check: complete: {}", epoch.0, complete);
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
        // merged_state should contain both objects after first snapshot
        assert_eq!(collector.merged_state_len(), 2);

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
