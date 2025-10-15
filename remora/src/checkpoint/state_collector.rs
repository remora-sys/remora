use crate::checkpoint::storage::RocksSnapshotStore;
use crate::checkpoint::{EpochId, EpochObjectStates};
use std::collections::BTreeMap;
use sui_types::base_types::ObjectID;
use sui_types::object::Object;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, info};

use crate::executor::api::ProxyToPrimaryMessage;

/// Collects state snapshots from all proxies and manages atomic state updates
pub struct StateCollector {
    /// Expected number of proxies
    expected_proxies: usize,
    /// Last epoch fully persisted by the collector (persistence epoch)
    last_persisted_epoch: Option<EpochId>,
    /// Snapshots grouped by epoch: epoch -> (proxy_id -> snapshot)
    collecting_snapshots:
        BTreeMap<EpochId, BTreeMap<crate::proxy::core::ProxyId, EpochObjectStates>>,
    /// Sender to notify when epoch is complete
    tx_epoch_complete: Option<Sender<EpochId>>,
    /// Optional RocksDB store for persisting merged snapshots
    snapshot_store: Option<RocksSnapshotStore>,
    /// In-memory latest object states (no disk persistence)
    merged_state: BTreeMap<ObjectID, Object>,
}

impl StateCollector {
    pub fn new(expected_proxies: usize) -> Self {
        Self {
            expected_proxies,
            last_persisted_epoch: None,
            collecting_snapshots: BTreeMap::new(),
            tx_epoch_complete: None,
            snapshot_store: None,
            merged_state: BTreeMap::new(),
        }
    }

    pub fn with_store(mut self, store: RocksSnapshotStore) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Start collecting snapshots for a new epoch
    pub fn start_epoch(&mut self, epoch: EpochId, tx_complete: Sender<EpochId>) {
        debug!("Starting epoch collection for epoch {}", epoch.0);
        // Pre-create entry for the epoch; not strictly necessary due to on-demand insertion.
        self.collecting_snapshots
            .entry(epoch)
            .or_insert_with(BTreeMap::new);
        self.tx_epoch_complete = Some(tx_complete);
    }

    /// Process a state snapshot from a proxy
    pub fn process_snapshot(
        &mut self,
        proxy_id: crate::proxy::core::ProxyId,
        epoch: EpochId,
        snapshot: EpochObjectStates,
    ) {
        // Record snapshot for its epoch
        let epoch_entry = self
            .collecting_snapshots
            .entry(epoch)
            .or_insert_with(BTreeMap::new);

        debug!(
            "Received snapshot from proxy {} for epoch {}: {} objects",
            proxy_id,
            epoch.0,
            snapshot.len()
        );

        // Write directly into the in-memory merged state (no per-epoch merge)
        for (obj_id, obj) in snapshot.iter() {
            self.merged_state.insert(*obj_id, obj.clone());
        }

        // Keep the snapshot for epoch-completion bookkeeping
        epoch_entry.insert(proxy_id, snapshot);

        // After updating, attempt to complete any ready epochs in order
        self.try_complete_ready_epochs();
    }

    /// Complete the current epoch (no merge/persist; state already updated on receipt)
    fn complete_epoch(&mut self, epoch: EpochId) {
        info!(
            "Completing epoch {} with {} snapshots",
            epoch.0,
            self.collecting_snapshots
                .get(&epoch)
                .map(|m| m.len())
                .unwrap_or(0)
        );

        // The in-memory state is already updated incrementally in process_snapshot.

        // Mark epoch as persisted and drop collected snapshots for this epoch
        self.last_persisted_epoch = Some(epoch);
        self.collecting_snapshots.remove(&epoch);

        // Notify completion
        if let Some(tx) = self.tx_epoch_complete.take() {
            let _ = tx.try_send(epoch);
        }
    }

    /// Try to complete epochs in order starting from the next after last_persisted_epoch
    fn try_complete_ready_epochs(&mut self) {
        // Determine the next epoch we expect to persist
        let next_epoch = match self.last_persisted_epoch {
            Some(prev) => EpochId(prev.0 + 1),
            None => {
                // If we have never persisted, take the smallest epoch present
                if let Some((&first_epoch, _)) = self.collecting_snapshots.iter().next() {
                    first_epoch
                } else {
                    return;
                }
            }
        };

        // Keep completing epochs as long as the next in-order epoch is ready
        let mut current = next_epoch;
        loop {
            let ready = self
                .collecting_snapshots
                .get(&current)
                .map(|m| m.len() >= self.expected_proxies)
                .unwrap_or(false);
            if !ready {
                break;
            }
            self.complete_epoch(current);
            current = EpochId(current.0 + 1);
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

    /// Get an object from the in-memory store.
    pub fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        self.merged_state.get(object_id).cloned()
    }

    /// Current number of objects in memory.
    pub fn merged_state_len(&self) -> usize {
        self.merged_state.len()
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
        assert!(collector.last_persisted_epoch.is_none());
        assert!(collector.collecting_snapshots.is_empty());
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
        assert!(collector.last_persisted_epoch.is_none());
        assert!(collector
            .collecting_snapshots
            .get(&EpochId(5))
            .map(|m| m.is_empty())
            .unwrap_or(false));
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
        assert_eq!(
            collector
                .collecting_snapshots
                .get(&EpochId(5))
                .map(|m| m.len())
                .unwrap_or(0),
            1
        );
        assert_eq!(
            collector
                .collecting_snapshots
                .get(&EpochId(5))
                .unwrap()
                .get(&1)
                .unwrap()
                .len(),
            2
        );

        // Process snapshot from proxy 2 - should trigger completion
        collector.process_snapshot(2, EpochId(5), snapshot);
        assert_eq!(collector.last_persisted_epoch, Some(EpochId(5)));
        assert!(!collector.collecting_snapshots.contains_key(&EpochId(5)));
    }

    #[test]
    fn test_state_collector_multiple_epochs_buffering() {
        let mut collector = StateCollector::new(2);
        let (tx, _rx) = mpsc::channel(1);

        // Start epoch 5
        collector.start_epoch(EpochId(5), tx);

        // Process snapshot for epoch 6 as well (out of order is allowed, but completion is ordered)
        let snapshot = EpochObjectStates::new();
        collector.process_snapshot(1, EpochId(6), snapshot);

        // Epoch 6 should be buffered, not completed
        assert!(collector.last_persisted_epoch.is_none());
        assert!(collector.collecting_snapshots.contains_key(&EpochId(5)));
        assert!(collector.collecting_snapshots.contains_key(&EpochId(6)));
    }

    #[test]
    fn test_state_collector_no_epoch() {
        let mut collector = StateCollector::new(2);

        // Process snapshot without starting epoch - should create epoch entry
        let snapshot = EpochObjectStates::new();
        collector.process_snapshot(1, EpochId(5), snapshot);

        // Should have buffered epoch 5, not persisted yet
        assert!(collector.last_persisted_epoch.is_none());
        assert!(collector.collecting_snapshots.contains_key(&EpochId(5)));
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

        // Should be completed and cleared for epoch 5
        assert_eq!(collector.last_persisted_epoch, Some(EpochId(5)));
        assert!(!collector.collecting_snapshots.contains_key(&EpochId(5)));
    }
}
