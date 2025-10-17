use crate::checkpoint::{EpochId, EpochObjectStates};
use dashmap::{DashMap, DashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::object::Object;
use tracing::debug;

/// Concurrent in-memory state for snapshots and merged objects
pub struct StateCollector {
    /// Proxies that have reported per-epoch: epoch -> set(proxy_id)
    pub collecting_snapshots: DashMap<EpochId, DashSet<crate::proxy::core::ProxyId>>,
    /// In-memory latest object states (no disk persistence)
    pub merged_state: DashMap<ObjectID, Object>,
    /// Primary-level persist index: last fully acknowledged epoch's consensus index
    persist_index: AtomicU64,
}

impl StateCollector {
    pub fn new(_expected_proxies: usize) -> Self {
        Self {
            collecting_snapshots: DashMap::new(),
            merged_state: DashMap::new(),
            persist_index: AtomicU64::new(0),
        }
    }

    /// Ensure an epoch entry exists
    pub fn ensure_epoch(&self, epoch: EpochId) {
        self.collecting_snapshots
            .entry(epoch)
            .or_insert_with(DashSet::new);
    }

    /// Process a state snapshot from a proxy
    pub fn process_snapshot(
        &self,
        proxy_id: crate::proxy::core::ProxyId,
        epoch: EpochId,
        snapshot: EpochObjectStates,
        expected_proxies: usize,
    ) {
        // Upsert per-epoch snapshots and global merged state concurrently-safe
        let epoch_entry = self
            .collecting_snapshots
            .entry(epoch)
            .or_insert_with(DashSet::new);

        debug!(
            "Received snapshot from proxy {} for epoch {}: {} objects",
            proxy_id,
            epoch.0,
            snapshot.len()
        );

        // Move directly into the in-memory merged state (no per-epoch merge)
        for (obj_id, obj) in snapshot.into_iter() {
            self.merged_state.insert(obj_id, obj);
        }

        epoch_entry.insert(proxy_id);

        // Check if epoch is complete and advance persist index if so
        let current_persist_index = self.get_persist_index();
        let epoch_proxy_count = epoch_entry.len();
        tracing::debug!(
            "Epoch {} progress: {}/{} proxies reported, current persist index: {}",
            epoch.0,
            epoch_proxy_count,
            expected_proxies,
            current_persist_index
        );
        
        if self.is_epoch_complete(epoch, expected_proxies) {
            // The consensus index should be the current persist index + 1
            let consensus_index = current_persist_index + 1;
            tracing::info!(
                "Epoch {} completed with {}/{} proxies, advancing persist index from {} to {}",
                epoch.0,
                epoch_proxy_count,
                expected_proxies,
                current_persist_index,
                consensus_index
            );
            self.acknowledge_epoch(epoch, consensus_index);
        }
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

    /// Check if an epoch is complete (all proxies have reported snapshots).
    /// Returns true if the epoch can be acknowledged and pruned.
    pub fn is_epoch_complete(&self, epoch: EpochId, expected_proxies: usize) -> bool {
        let is_complete = self.collecting_snapshots
            .get(&epoch)
            .map(|proxies| {
                let proxy_count = proxies.len();
                let complete = proxy_count >= expected_proxies;
                tracing::debug!(
                    "Epoch {} completion check: {}/{} proxies, complete: {}",
                    epoch.0,
                    proxy_count,
                    expected_proxies,
                    complete
                );
                complete
            })
            .unwrap_or_else(|| {
                tracing::debug!("Epoch {} not found in collecting_snapshots", epoch.0);
                false
            });
        is_complete
    }

    /// Mark an epoch as acknowledged, advance persist index, and remove it from tracking.
    pub fn acknowledge_epoch(&self, epoch: EpochId, consensus_index: u64) {
        let old_persist_index = self.persist_index.load(Ordering::SeqCst);
        self.persist_index.store(consensus_index, Ordering::SeqCst);
        let removed_epoch = self.collecting_snapshots.remove(&epoch);
        tracing::info!(
            "Epoch {} acknowledged; persist index advanced from {} to {}, removed {} tracking entries",
            epoch.0,
            old_persist_index,
            consensus_index,
            removed_epoch.map(|(_, proxies)| proxies.len()).unwrap_or(0)
        );
    }

    /// Get the current primary persist index (replay cut).
    pub fn get_persist_index(&self) -> u64 {
        self.persist_index.load(Ordering::SeqCst)
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
    fn test_state_collector_start_epoch() {
        let collector = StateCollector::new(2);
        collector.ensure_epoch(EpochId(5));
        assert!(collector.collecting_snapshots.get(&EpochId(5)).is_some());
    }

    #[test]
    fn test_state_collector_process_snapshot() {
        let collector = StateCollector::new(2);
        collector.ensure_epoch(EpochId(5));

        // Create test objects
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj1 = create_test_object(obj_id1);
        let obj2 = create_test_object(obj_id2);

        let mut snapshot = EpochObjectStates::new();
        snapshot.insert(obj_id1, obj1);
        snapshot.insert(obj_id2, obj2);

        // Process snapshot from proxy 1
        collector.process_snapshot(1, EpochId(5), snapshot.clone(), 2);
        assert_eq!(
            collector
                .collecting_snapshots
                .get(&EpochId(5))
                .unwrap()
                .len(),
            1
        );
        // merged_state should contain both objects after first snapshot
        assert_eq!(collector.merged_state_len(), 2);

        // Process snapshot from proxy 2
        collector.process_snapshot(2, EpochId(5), snapshot, 2);
        assert_eq!(
            collector
                .collecting_snapshots
                .get(&EpochId(5))
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn test_state_collector_multiple_epochs_buffering() {
        let collector = StateCollector::new(2);
        collector.ensure_epoch(EpochId(5));

        // Process snapshot for epoch 6 as well (out of order is allowed, but completion is ordered)
        let snapshot = EpochObjectStates::new();
        collector.process_snapshot(1, EpochId(6), snapshot, 2);

        // Epochs should be present
        assert!(collector.collecting_snapshots.get(&EpochId(5)).is_some());
        assert!(collector.collecting_snapshots.get(&EpochId(6)).is_some());
    }

    #[test]
    fn test_state_collector_no_epoch() {
        let collector = StateCollector::new(2);

        // Process snapshot without starting epoch - should create epoch entry
        let snapshot = EpochObjectStates::new();
        collector.process_snapshot(1, EpochId(5), snapshot, 2);

        // Should have buffered epoch 5
        assert!(collector.collecting_snapshots.get(&EpochId(5)).is_some());
    }

    #[test]
    fn test_state_collector_merge_snapshots() {
        let collector = StateCollector::new(2);
        collector.ensure_epoch(EpochId(5));

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
        collector.process_snapshot(1, EpochId(5), snapshot1, 2);
        collector.process_snapshot(2, EpochId(5), snapshot2, 2);

        // Epoch should have two proxies recorded
        assert_eq!(
            collector
                .collecting_snapshots
                .get(&EpochId(5))
                .unwrap()
                .len(),
            2
        );
    }
}
