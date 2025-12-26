use crate::checkpoint::{EpochId, EpochObjectStates};
use dashmap::{mapref::entry::Entry, DashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use sui_types::base_types::ObjectID;
use sui_types::object::Object;

/// Tracks the current epoch on a proxy.
#[derive(Clone)]
pub struct EpochTracker {
    current_epoch: Arc<AtomicU64>,
}

impl EpochTracker {
    pub fn new(initial: EpochId) -> Self {
        Self {
            current_epoch: Arc::new(AtomicU64::new(initial.0)),
        }
    }

    pub fn current(&self) -> EpochId {
        EpochId(self.current_epoch.load(Ordering::SeqCst))
    }

    pub fn update_epoch(&self, epoch: EpochId) {
        self.current_epoch.store(epoch.0, Ordering::SeqCst);
    }
}

/// Tracks modified objects in the current epoch.
#[derive(Clone)]
pub struct ModifiedObjectTracker {
    // Map of epoch -> (object -> latest object state in that epoch)
    modified: Arc<DashMap<EpochId, DashMap<ObjectID, Object>>>,
    // Highest epoch that has been fully snapped/committed.
    committed_up_to: Arc<AtomicU64>,
}

impl ModifiedObjectTracker {
    pub fn new() -> Self {
        Self {
            modified: Arc::new(DashMap::new()),
            committed_up_to: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_object(&self, epoch: EpochId, object_id: ObjectID, object: Object) {
        let committed = self.committed_up_to.load(Ordering::SeqCst);
        // Only drop objects for epochs strictly BEFORE the committed watermark.
        // Objects for the current epoch (epoch == committed) should still be recorded
        // since transactions may complete slightly after the snapshot is taken.
        if epoch.0 < committed {
            tracing::warn!(
                "dropping object for obj_id {} in committed epoch {} (committed_up_to={})",
                object_id,
                epoch.0,
                committed
            );
            return;
        }

        tracing::debug!(
            "recording object for epoch {} obj_id {}: version {}",
            epoch.0,
            object_id,
            object.version().value()
        );

        match self.modified.entry(epoch) {
            Entry::Occupied(entry) => {
                entry.get().insert(object_id, object);
            }
            Entry::Vacant(entry) => {
                let inner = DashMap::new();
                inner.insert(object_id, object);
                entry.insert(inner);
            }
        }
    }

    /// Drain current epoch modifications and reset.
    pub fn take_epoch_snapshot(&self, epoch: EpochId) -> EpochObjectStates {
        let mut out = EpochObjectStates::new();
        self.update_committed_up_to(epoch);
        if let Some((_, epoch_map)) = self.modified.remove(&epoch) {
            for entry in epoch_map.into_iter() {
                let (obj_id, obj) = entry;
                tracing::debug!(
                    "taking epoch {} snapshot for obj_id {}: version {}",
                    epoch.0,
                    obj_id,
                    obj.version().value()
                );
                out.insert(obj_id, obj);
            }
        } else {
            tracing::warn!(
                "take_epoch_snapshot called for already-drained epoch {}",
                epoch.0
            );
        }
        out
    }

    fn update_committed_up_to(&self, epoch: EpochId) {
        let mut current = self.committed_up_to.load(Ordering::SeqCst);
        while epoch.0 > current {
            match self.committed_up_to.compare_exchange(
                current,
                epoch.0,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(new_current) => current = new_current,
            }
        }
    }
}

/// Tracks per-epoch transaction completion for elastic scaling.
///
/// Unlike the static batch_size approach, this tracks actual received
/// transactions per epoch and triggers snapshots when:
/// 1. The epoch is "sealed" (Checkpoint message received for next epoch)
/// 2. All received transactions for that epoch have completed
#[derive(Clone)]
pub struct EpochCompletionTracker {
    /// Per-epoch state: (received_count, completed_count, sealed)
    /// Sealed means no more transactions will arrive for this epoch.
    epoch_state: Arc<DashMap<u64, EpochState>>,
}

struct EpochState {
    received: AtomicUsize,
    completed: AtomicUsize,
    sealed: std::sync::atomic::AtomicBool,
}

impl EpochCompletionTracker {
    pub fn new() -> Self {
        Self {
            epoch_state: Arc::new(DashMap::new()),
        }
    }

    /// Record that a transaction was received for this epoch.
    pub fn record_received(&self, epoch: EpochId) {
        let entry = self
            .epoch_state
            .entry(epoch.0)
            .or_insert_with(|| EpochState {
                received: AtomicUsize::new(0),
                completed: AtomicUsize::new(0),
                sealed: std::sync::atomic::AtomicBool::new(false),
            });
        entry.received.fetch_add(1, Ordering::SeqCst);
    }

    /// Record that a transaction completed for this epoch.
    /// Returns true if the epoch is now ready for snapshot (sealed AND all completed).
    pub fn record_completed(&self, epoch: EpochId) -> bool {
        if let Some(state) = self.epoch_state.get(&epoch.0) {
            let completed = state.completed.fetch_add(1, Ordering::SeqCst) + 1;
            let received = state.received.load(Ordering::SeqCst);
            let sealed = state.sealed.load(Ordering::SeqCst);

            tracing::debug!(
                "Epoch {} completion: {}/{} (sealed={})",
                epoch.0,
                completed,
                received,
                sealed
            );

            sealed && completed == received && received > 0
        } else {
            false
        }
    }

    /// Mark an epoch as sealed (no more transactions will arrive).
    /// Returns true if the epoch is now ready for snapshot (all completed).
    pub fn seal_epoch(&self, epoch: EpochId) -> bool {
        if let Some(state) = self.epoch_state.get(&epoch.0) {
            state.sealed.store(true, Ordering::SeqCst);
            let completed = state.completed.load(Ordering::SeqCst);
            let received = state.received.load(Ordering::SeqCst);

            tracing::info!(
                "Epoch {} sealed: {}/{} completed",
                epoch.0,
                completed,
                received
            );

            // Ready if all received transactions are completed
            // Special case: if received == 0, the proxy got no transactions for this epoch
            completed == received
        } else {
            // No transactions received for this epoch - still "complete" with empty snapshot
            tracing::info!("Epoch {} sealed with no transactions", epoch.0);
            // Insert a sealed empty state
            self.epoch_state.insert(
                epoch.0,
                EpochState {
                    received: AtomicUsize::new(0),
                    completed: AtomicUsize::new(0),
                    sealed: std::sync::atomic::AtomicBool::new(true),
                },
            );
            true
        }
    }

    /// Check if an epoch is ready for snapshot.
    pub fn is_ready(&self, epoch: EpochId) -> bool {
        if let Some(state) = self.epoch_state.get(&epoch.0) {
            let completed = state.completed.load(Ordering::SeqCst);
            let received = state.received.load(Ordering::SeqCst);
            let sealed = state.sealed.load(Ordering::SeqCst);
            sealed && completed == received
        } else {
            false
        }
    }

    /// Clean up tracking data for epochs up to and including the given epoch.
    pub fn cleanup_up_to(&self, epoch: EpochId) {
        self.epoch_state.retain(|e, _| *e > epoch.0);
    }

    /// Initialize the tracker for proxy activation during elastic scaling.
    ///
    /// When a proxy is activated at epoch N, it needs to have epoch states for
    /// epochs 1 through N-1 marked as sealed with 0 transactions. This allows
    /// the try_advance_watermark logic to advance past epochs the proxy didn't
    /// participate in.
    ///
    /// # Arguments
    /// * `first_active_epoch` - The first epoch this proxy will actively participate in
    pub fn initialize_for_activation(&self, first_active_epoch: EpochId) {
        // Mark all epochs before first_active_epoch as sealed with 0 transactions
        // This allows is_ready() to return true for these epochs
        for epoch in 1..first_active_epoch.0 {
            if !self.epoch_state.contains_key(&epoch) {
                self.epoch_state.insert(
                    epoch,
                    EpochState {
                        received: AtomicUsize::new(0),
                        completed: AtomicUsize::new(0),
                        sealed: std::sync::atomic::AtomicBool::new(true),
                    },
                );
                tracing::debug!(
                    "Initialized epoch {} as sealed (prior to activation epoch {})",
                    epoch,
                    first_active_epoch.0
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui_types::object::Object;

    fn create_test_object(id: ObjectID) -> Object {
        Object::immutable_with_id_for_testing(id)
    }

    #[test]
    fn test_epoch_tracker_new() {
        let tracker = EpochTracker::new(EpochId(5));
        assert_eq!(tracker.current(), EpochId(5));
    }

    #[test]
    fn test_epoch_tracker_update() {
        let tracker = EpochTracker::new(EpochId(1));
        assert_eq!(tracker.current(), EpochId(1));

        tracker.update_epoch(EpochId(10));
        assert_eq!(tracker.current(), EpochId(10));
    }

    #[test]
    fn test_epoch_tracker_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(EpochTracker::new(EpochId(1)));
        let mut handles = vec![];

        // Spawn multiple threads to update epochs concurrently
        for i in 0..10 {
            let tracker = Arc::clone(&tracker);
            let handle = thread::spawn(move || {
                tracker.update_epoch(EpochId(i + 10));
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Final epoch should be one of the updated values
        let current = tracker.current();
        assert!(current.0 >= 10 && current.0 < 20);
    }

    #[test]
    fn test_modified_object_tracker_new() {
        let tracker = ModifiedObjectTracker::new();
        let snapshot = tracker.take_epoch_snapshot(EpochId(1));
        assert!(snapshot.is_empty());
    }

    #[test]
    fn test_modified_object_tracker_record_and_snapshot() {
        let tracker = ModifiedObjectTracker::new();
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj1 = create_test_object(obj_id1);
        let obj2 = create_test_object(obj_id2);

        // Record objects
        tracker.record_object(EpochId(1), obj_id1, obj1.clone());
        tracker.record_object(EpochId(1), obj_id2, obj2.clone());

        // Take snapshot
        let snapshot = tracker.take_epoch_snapshot(EpochId(1));
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get(&obj_id1), Some(&obj1));
        assert_eq!(snapshot.get(&obj_id2), Some(&obj2));

        // Snapshot should be empty after taking
        let empty_snapshot = tracker.take_epoch_snapshot(EpochId(1));
        assert!(empty_snapshot.is_empty());
    }

    #[test]
    fn test_modified_object_tracker_overwrite() {
        let tracker = ModifiedObjectTracker::new();
        let obj_id = ObjectID::random();
        let obj1 = create_test_object(obj_id);
        let obj2 = create_test_object(obj_id); // Same ID, different object

        // Record first object
        tracker.record_object(EpochId(1), obj_id, obj1.clone());

        // Overwrite with second object
        tracker.record_object(EpochId(1), obj_id, obj2.clone());

        // Snapshot should contain only the second object
        let snapshot = tracker.take_epoch_snapshot(EpochId(1));
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot.get(&obj_id), Some(&obj2));
    }

    #[test]
    fn test_modified_object_tracker_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(ModifiedObjectTracker::new());
        let mut handles = vec![];

        // Spawn multiple threads to record objects concurrently
        for _i in 0..10 {
            let tracker = Arc::clone(&tracker);
            let handle = thread::spawn(move || {
                let obj_id = ObjectID::random();
                let obj = create_test_object(obj_id);
                tracker.record_object(EpochId(1), obj_id, obj);
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Snapshot should contain all recorded objects
        let snapshot = tracker.take_epoch_snapshot(EpochId(1));
        assert_eq!(snapshot.len(), 10);
    }

    // ============================================================================
    // EpochCompletionTracker Tests
    // ============================================================================

    #[test]
    fn test_epoch_completion_tracker_basic() {
        let tracker = EpochCompletionTracker::new();

        // Record 5 transactions for epoch 1
        for _ in 0..5 {
            tracker.record_received(EpochId(1));
        }

        // Complete 4 transactions - should not be ready
        for _ in 0..4 {
            assert!(!tracker.record_completed(EpochId(1)));
        }

        // Not sealed yet, so should not be ready even though all completed
        assert!(!tracker.record_completed(EpochId(1)));

        // Seal the epoch - now it should be ready (all 5 completed)
        assert!(tracker.seal_epoch(EpochId(1)));
        assert!(tracker.is_ready(EpochId(1)));
    }

    #[test]
    fn test_epoch_completion_tracker_seal_before_complete() {
        let tracker = EpochCompletionTracker::new();

        // Record 3 transactions
        for _ in 0..3 {
            tracker.record_received(EpochId(1));
        }

        // Complete 1 transaction
        assert!(!tracker.record_completed(EpochId(1)));

        // Seal the epoch (2 still pending)
        assert!(!tracker.seal_epoch(EpochId(1)));
        assert!(!tracker.is_ready(EpochId(1)));

        // Complete remaining - last one should return true
        assert!(!tracker.record_completed(EpochId(1)));
        assert!(tracker.record_completed(EpochId(1)));
        assert!(tracker.is_ready(EpochId(1)));
    }

    #[test]
    fn test_epoch_completion_tracker_empty_epoch() {
        let tracker = EpochCompletionTracker::new();

        // Seal an epoch with no transactions - should be immediately ready
        assert!(tracker.seal_epoch(EpochId(1)));
        assert!(tracker.is_ready(EpochId(1)));
    }

    #[test]
    fn test_epoch_completion_tracker_various_batch_sizes() {
        // Test with various batch sizes to simulate elastic scaling
        let batch_sizes = [1, 10, 100, 500, 1000, 5000];

        for batch_size in batch_sizes {
            let tracker = EpochCompletionTracker::new();

            // Record batch_size transactions
            for _ in 0..batch_size {
                tracker.record_received(EpochId(1));
            }

            // Complete all but one
            for _ in 0..(batch_size - 1) {
                assert!(!tracker.record_completed(EpochId(1)));
            }

            // Seal the epoch
            assert!(!tracker.seal_epoch(EpochId(1)));
            assert!(!tracker.is_ready(EpochId(1)));

            // Complete the last one
            assert!(tracker.record_completed(EpochId(1)));
            assert!(tracker.is_ready(EpochId(1)));
        }
    }

    #[test]
    fn test_epoch_completion_tracker_different_epochs_different_sizes() {
        // Simulate elastic scaling where different epochs have different batch sizes
        let tracker = EpochCompletionTracker::new();

        // Epoch 1: 100 transactions (initial single proxy)
        for _ in 0..100 {
            tracker.record_received(EpochId(1));
        }

        // Epoch 2: 50 transactions (scaled to 2 proxies)
        for _ in 0..50 {
            tracker.record_received(EpochId(2));
        }

        // Epoch 3: 33 transactions (scaled to 3 proxies)
        for _ in 0..33 {
            tracker.record_received(EpochId(3));
        }

        // Complete epoch 2 first (out of order)
        for _ in 0..50 {
            tracker.record_completed(EpochId(2));
        }
        tracker.seal_epoch(EpochId(2));
        assert!(tracker.is_ready(EpochId(2)));

        // Epoch 1 still not ready
        assert!(!tracker.is_ready(EpochId(1)));

        // Complete epoch 1
        for _ in 0..100 {
            tracker.record_completed(EpochId(1));
        }
        tracker.seal_epoch(EpochId(1));
        assert!(tracker.is_ready(EpochId(1)));

        // Complete epoch 3
        for _ in 0..33 {
            tracker.record_completed(EpochId(3));
        }
        tracker.seal_epoch(EpochId(3));
        assert!(tracker.is_ready(EpochId(3)));
    }

    #[test]
    fn test_epoch_completion_tracker_cleanup() {
        let tracker = EpochCompletionTracker::new();

        // Create and complete epochs 1, 2, 3
        for epoch in 1..=3 {
            for _ in 0..10 {
                tracker.record_received(EpochId(epoch));
            }
            for _ in 0..10 {
                tracker.record_completed(EpochId(epoch));
            }
            tracker.seal_epoch(EpochId(epoch));
        }

        // All should be ready
        assert!(tracker.is_ready(EpochId(1)));
        assert!(tracker.is_ready(EpochId(2)));
        assert!(tracker.is_ready(EpochId(3)));

        // Cleanup up to epoch 2
        tracker.cleanup_up_to(EpochId(2));

        // Epoch 1 and 2 should no longer be tracked (is_ready returns false for non-existent)
        assert!(!tracker.is_ready(EpochId(1)));
        assert!(!tracker.is_ready(EpochId(2)));
        // Epoch 3 should still be ready
        assert!(tracker.is_ready(EpochId(3)));
    }

    #[test]
    fn test_epoch_completion_tracker_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(EpochCompletionTracker::new());
        let num_transactions = 1000;
        let num_threads = 10;
        let per_thread = num_transactions / num_threads;

        // Record all transactions first
        for _ in 0..num_transactions {
            tracker.record_received(EpochId(1));
        }

        let mut handles = vec![];

        // Complete transactions from multiple threads
        for _ in 0..num_threads {
            let tracker = Arc::clone(&tracker);
            let handle = thread::spawn(move || {
                for _ in 0..per_thread {
                    tracker.record_completed(EpochId(1));
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Seal the epoch
        assert!(tracker.seal_epoch(EpochId(1)));
        assert!(tracker.is_ready(EpochId(1)));
    }

    #[test]
    fn test_epoch_completion_tracker_interleaved_epochs() {
        let tracker = EpochCompletionTracker::new();

        // Simulate transactions arriving interleaved for epochs 1, 2, 3
        for i in 0..30 {
            let epoch = (i % 3) + 1;
            tracker.record_received(EpochId(epoch));
        }

        // Complete transactions interleaved
        for i in 0..30 {
            let epoch = (i % 3) + 1;
            tracker.record_completed(EpochId(epoch));
        }

        // Seal epochs in order
        for epoch in 1..=3 {
            assert!(tracker.seal_epoch(EpochId(epoch)));
            assert!(tracker.is_ready(EpochId(epoch)));
        }
    }

    #[test]
    fn test_epoch_completion_tracker_zero_transactions_sealed() {
        let tracker = EpochCompletionTracker::new();

        // This simulates a proxy that received no transactions for an epoch
        // (possible in elastic scaling when load is light)
        for epoch in 1..=5 {
            // Seal immediately without any transactions
            assert!(tracker.seal_epoch(EpochId(epoch)));
            assert!(tracker.is_ready(EpochId(epoch)));
        }
    }

    #[test]
    fn test_epoch_completion_tracker_single_transaction_epochs() {
        let tracker = EpochCompletionTracker::new();

        // Simulate epochs with exactly 1 transaction each
        for epoch in 1..=10 {
            tracker.record_received(EpochId(epoch));
            tracker.seal_epoch(EpochId(epoch));
            assert!(!tracker.is_ready(EpochId(epoch)));

            assert!(tracker.record_completed(EpochId(epoch)));
            assert!(tracker.is_ready(EpochId(epoch)));
        }
    }

    #[test]
    fn test_epoch_completion_tracker_high_contention() {
        use std::sync::Arc;
        use std::thread;

        let tracker = Arc::new(EpochCompletionTracker::new());
        let num_epochs = 10;
        let transactions_per_epoch = 100;

        // Pre-record all transactions
        for epoch in 1..=num_epochs {
            for _ in 0..transactions_per_epoch {
                tracker.record_received(EpochId(epoch));
            }
        }

        let mut handles = vec![];

        // Spawn threads that complete transactions for random epochs
        for _ in 0..20 {
            let tracker = Arc::clone(&tracker);
            let handle = thread::spawn(move || {
                for epoch in 1..=num_epochs {
                    for _ in 0..(transactions_per_epoch / 20) {
                        tracker.record_completed(EpochId(epoch));
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all completions
        for handle in handles {
            handle.join().unwrap();
        }

        // Seal all epochs
        for epoch in 1..=num_epochs {
            tracker.seal_epoch(EpochId(epoch));
            assert!(tracker.is_ready(EpochId(epoch)));
        }
    }
}
