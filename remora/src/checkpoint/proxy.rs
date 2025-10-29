use crate::checkpoint::{EpochId, EpochObjectStates};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    // map of object -> latest object state in this epoch
    modified: Arc<ArcSwap<DashMap<ObjectID, Object>>>,
}

impl ModifiedObjectTracker {
    pub fn new() -> Self {
        Self {
            modified: Arc::new(ArcSwap::from(Arc::new(DashMap::new()))),
        }
    }

    pub fn record_object(&self, object_id: ObjectID, object: Object) {
        tracing::info!(
            "recording object for obj_id {}: version {}",
            object_id,
            object.version().value()
        );
        self.modified.load().insert(object_id, object);
    }

    /// Drain current epoch modifications and reset.
    pub fn take_epoch_snapshot(&self) -> EpochObjectStates {
        let mut out = EpochObjectStates::new();
        let old_map = self.modified.swap(Arc::new(DashMap::new()));
        for entry in old_map.iter() {
            tracing::info!(
                "taking epoch snapshot for obj_id {}: version {}",
                entry.key(),
                entry.value().version().value()
            );
            out.insert(*entry.key(), entry.value().clone());
        }
        out
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
        let snapshot = tracker.take_epoch_snapshot();
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
        tracker.record_object(obj_id1, obj1.clone());
        tracker.record_object(obj_id2, obj2.clone());

        // Take snapshot
        let snapshot = tracker.take_epoch_snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get(&obj_id1), Some(&obj1));
        assert_eq!(snapshot.get(&obj_id2), Some(&obj2));

        // Snapshot should be empty after taking
        let empty_snapshot = tracker.take_epoch_snapshot();
        assert!(empty_snapshot.is_empty());
    }

    #[test]
    fn test_modified_object_tracker_overwrite() {
        let tracker = ModifiedObjectTracker::new();
        let obj_id = ObjectID::random();
        let obj1 = create_test_object(obj_id);
        let obj2 = create_test_object(obj_id); // Same ID, different object

        // Record first object
        tracker.record_object(obj_id, obj1.clone());

        // Overwrite with second object
        tracker.record_object(obj_id, obj2.clone());

        // Snapshot should contain only the second object
        let snapshot = tracker.take_epoch_snapshot();
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
                tracker.record_object(obj_id, obj);
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Snapshot should contain all recorded objects
        let snapshot = tracker.take_epoch_snapshot();
        assert_eq!(snapshot.len(), 10);
    }
}
