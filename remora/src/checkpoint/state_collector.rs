use crate::checkpoint::storage::RocksSnapshotStore;
use crate::checkpoint::{EpochId, EpochObjectStates};
use std::collections::BTreeMap;
use sui_types::object::Object;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info, warn};

use crate::executor::api::ProxyToPrimaryMessage;

/// Collects state snapshots from all proxies and manages atomic state updates
pub struct StateCollector {
    /// Expected number of proxies
    expected_proxies: usize,
    /// Current epoch being collected
    current_epoch: Option<EpochId>,
    /// Snapshots received for current epoch (proxy_id -> snapshot)
    pending_snapshots: BTreeMap<crate::proxy::core::ProxyId, EpochObjectStates>,
    /// Sender to notify when epoch is complete
    tx_epoch_complete: Option<Sender<EpochId>>,
    /// Optional RocksDB store for persisting merged snapshots
    snapshot_store: Option<RocksSnapshotStore>,
}

impl StateCollector {
    pub fn new(expected_proxies: usize) -> Self {
        Self {
            expected_proxies,
            current_epoch: None,
            pending_snapshots: BTreeMap::new(),
            tx_epoch_complete: None,
            snapshot_store: None,
        }
    }

    pub fn with_store(mut self, store: RocksSnapshotStore) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Start collecting snapshots for a new epoch
    pub fn start_epoch(&mut self, epoch: EpochId, tx_complete: Sender<EpochId>) {
        debug!("Starting epoch collection for epoch {}", epoch.0);
        self.current_epoch = Some(epoch);
        self.pending_snapshots.clear();
        self.tx_epoch_complete = Some(tx_complete);
    }

    /// Process a state snapshot from a proxy
    pub fn process_snapshot(
        &mut self,
        proxy_id: crate::proxy::core::ProxyId,
        epoch: EpochId,
        snapshot: EpochObjectStates,
    ) {
        if let Some(current_epoch) = self.current_epoch {
            if current_epoch != epoch {
                warn!(
                    "Received snapshot for epoch {} but current epoch is {}",
                    epoch.0, current_epoch.0
                );
                return;
            }

            debug!(
                "Received snapshot from proxy {} for epoch {}: {} objects",
                proxy_id,
                epoch.0,
                snapshot.len()
            );

            self.pending_snapshots.insert(proxy_id, snapshot);

            // Check if we have all snapshots
            if self.pending_snapshots.len() >= self.expected_proxies {
                self.complete_epoch(epoch);
            }
        } else {
            warn!("Received snapshot but no epoch is being collected");
        }
    }

    /// Complete the current epoch and merge all snapshots
    fn complete_epoch(&mut self, epoch: EpochId) {
        info!(
            "Completing epoch {} with {} snapshots",
            epoch.0,
            self.pending_snapshots.len()
        );

        // Merge all snapshots into a single state update
        let mut merged_state = BTreeMap::<sui_types::base_types::ObjectID, Object>::new();
        for (proxy_id, snapshot) in &self.pending_snapshots {
            debug!("Merging {} objects from proxy {}", snapshot.len(), proxy_id);
            for (obj_id, obj) in snapshot {
                // Keep the latest object for each object ID
                merged_state.insert(*obj_id, obj.clone());
            }
        }

        info!(
            "Epoch {} complete: {} unique objects in merged state",
            epoch.0,
            merged_state.len()
        );

        if let Some(store) = &self.snapshot_store {
            if let Err(e) = store.persist_objects(&merged_state) {
                warn!(
                    "Failed to persist merged objects for epoch {}: {:?}",
                    epoch.0, e
                );
            }
        }

        // Reset for next epoch
        self.current_epoch = None;
        self.pending_snapshots.clear();

        // Notify completion
        if let Some(tx) = self.tx_epoch_complete.take() {
            let _ = tx.try_send(epoch);
        }
    }

    /// Run the state collector, processing messages from proxies
    pub async fn run(&mut self, mut rx_snapshots: Receiver<ProxyToPrimaryMessage>) {
        while let Some(message) = rx_snapshots.recv().await {
            match message {
                ProxyToPrimaryMessage::StateSnapshot(proxy_id, epoch, snapshot) => {
                    self.process_snapshot(proxy_id, epoch, snapshot);
                }
            }
        }
        info!("State collector shutting down");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui_types::base_types::ObjectID;
    use sui_types::object::Object;
    use tokio::sync::mpsc;

    fn create_test_object(id: ObjectID) -> Object {
        Object::immutable_with_id_for_testing(id)
    }

    #[test]
    fn test_state_collector_new() {
        let collector = StateCollector::new(3);
        assert_eq!(collector.expected_proxies, 3);
        assert!(collector.current_epoch.is_none());
        assert!(collector.pending_snapshots.is_empty());
    }

    #[test]
    fn test_state_collector_with_store() {
        let temp_dir = std::env::temp_dir().join("test_checkpoint");
        let store = RocksSnapshotStore::open(temp_dir).unwrap();
        let collector = StateCollector::new(2).with_store(store);
        assert!(collector.snapshot_store.is_some());
    }

    #[test]
    fn test_state_collector_start_epoch() {
        let mut collector = StateCollector::new(2);
        let (tx, _rx) = mpsc::channel(1);

        collector.start_epoch(EpochId(5), tx);
        assert_eq!(collector.current_epoch, Some(EpochId(5)));
        assert!(collector.pending_snapshots.is_empty());
    }

    #[test]
    fn test_state_collector_process_snapshot() {
        let mut collector = StateCollector::new(2);
        let (tx, _rx) = mpsc::channel(1);

        // Start epoch
        collector.start_epoch(EpochId(5), tx);

        // Create test objects
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj1 = create_test_object(obj_id1);
        let obj2 = create_test_object(obj_id2);

        let mut snapshot = EpochObjectStates::new();
        snapshot.insert(obj_id1, obj1);
        snapshot.insert(obj_id2, obj2);

        // Process snapshot from proxy 1
        collector.process_snapshot(1, EpochId(5), snapshot.clone());
        assert_eq!(collector.pending_snapshots.len(), 1);
        assert_eq!(collector.pending_snapshots.get(&1).unwrap().len(), 2);

        // Process snapshot from proxy 2 - should trigger completion
        collector.process_snapshot(2, EpochId(5), snapshot);
        assert!(collector.current_epoch.is_none());
        assert!(collector.pending_snapshots.is_empty());
    }

    #[test]
    fn test_state_collector_wrong_epoch() {
        let mut collector = StateCollector::new(2);
        let (tx, _rx) = mpsc::channel(1);

        // Start epoch 5
        collector.start_epoch(EpochId(5), tx);

        // Try to process snapshot for epoch 6 - should be ignored
        let snapshot = EpochObjectStates::new();
        collector.process_snapshot(1, EpochId(6), snapshot);

        // Should still be waiting for epoch 5
        assert_eq!(collector.current_epoch, Some(EpochId(5)));
        assert!(collector.pending_snapshots.is_empty());
    }

    #[test]
    fn test_state_collector_no_epoch() {
        let mut collector = StateCollector::new(2);

        // Try to process snapshot without starting epoch - should be ignored
        let snapshot = EpochObjectStates::new();
        collector.process_snapshot(1, EpochId(5), snapshot);

        // Should still be in initial state
        assert!(collector.current_epoch.is_none());
        assert!(collector.pending_snapshots.is_empty());
    }

    #[test]
    fn test_state_collector_merge_snapshots() {
        let mut collector = StateCollector::new(2);
        let (tx, _rx) = mpsc::channel(1);

        // Start epoch
        collector.start_epoch(EpochId(5), tx);

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
        collector.process_snapshot(1, EpochId(5), snapshot1);
        collector.process_snapshot(2, EpochId(5), snapshot2);

        // Should be completed and reset
        assert!(collector.current_epoch.is_none());
        assert!(collector.pending_snapshots.is_empty());
    }
}
