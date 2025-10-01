use crate::checkpoint::{EpochId, EpochObjectStates, EpochObjectVersions};
use dashmap::DashMap;
use std::sync::Arc;
use sui_types::base_types::{ObjectID, SequenceNumber};
use sui_types::object::Object;

/// Tracks the current epoch on a proxy.
#[derive(Clone)]
pub struct EpochTracker {
    current_epoch: Arc<DashMap<(), EpochId>>, // singleton key
}

impl EpochTracker {
    pub fn new(initial: EpochId) -> Self {
        let map = DashMap::new();
        map.insert((), initial);
        Self {
            current_epoch: Arc::new(map),
        }
    }

    pub fn current(&self) -> EpochId {
        self.current_epoch.get(&()).map(|e| *e.value()).unwrap()
    }

    pub fn update_epoch(&self, epoch: EpochId) {
        self.current_epoch.insert((), epoch);
    }
}

/// Tracks modified objects in the current epoch.
#[derive(Clone)]
pub struct ModifiedObjectTracker {
    // map of object -> latest object state in this epoch
    modified: Arc<DashMap<ObjectID, Object>>,
}

impl ModifiedObjectTracker {
    pub fn new() -> Self {
        Self {
            modified: Arc::new(DashMap::new()),
        }
    }

    pub fn record_object(&self, object_id: ObjectID, object: Object) {
        self.modified.insert(object_id, object);
    }

    /// Drain current epoch modifications and reset.
    pub fn take_epoch_snapshot(&self) -> EpochObjectStates {
        let mut out = EpochObjectStates::new();
        // DashMap has no drain; iterate and then clear
        for entry in self.modified.iter() {
            out.insert(*entry.key(), entry.value().clone());
        }
        self.modified.clear();
        out
    }
}
