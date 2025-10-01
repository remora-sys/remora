//! Persistent storage for checkpoint snapshots using RocksDB.
//!
//! This module provides a RocksDB-backed storage layer for checkpoint snapshots.
//! Only the primary node persists merged snapshots; proxies maintain state in memory only.
//!
//! ## Key Format
//! - Object state: `<object_id>` -> `(version, object_data)`
//!
//! ## Usage
//! ```rust
//! let store = RocksSnapshotStore::open(PathBuf::from("./data/snapshots"))?;
//! store.persist_objects(&object_map)?;
//! ```

use rocksdb::{DBWithThreadMode, MultiThreaded, Options};
use std::path::PathBuf;
use sui_types::base_types::ObjectID;
use sui_types::object::Object;

/// RocksDB-backed storage for checkpoint snapshots.
///
/// This store persists object states from the primary node's StateCollector.
/// Each object is stored with its ID as key and (version, object_data) as value.
///
/// # Thread Safety
/// Uses RocksDB's multi-threaded mode for concurrent access from multiple threads.
pub struct RocksSnapshotStore {
    /// The underlying RocksDB instance
    db: DBWithThreadMode<MultiThreaded>,
}

impl RocksSnapshotStore {
    /// Open a new RocksDB instance at the specified path.
    ///
    /// # Arguments
    /// * `path` - Directory path where the RocksDB database will be created
    ///
    /// # Returns
    /// * `Ok(RocksSnapshotStore)` - Successfully opened store
    /// * `Err(anyhow::Error)` - Failed to open database (e.g., permission denied, disk full)
    ///
    /// # Example
    /// ```rust
    /// let store = RocksSnapshotStore::open(PathBuf::from("./data/checkpoints"))?;
    /// ```
    pub fn open(path: PathBuf) -> anyhow::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DBWithThreadMode::open(&opts, path)?;
        Ok(Self { db })
    }

    /// Persist object states to storage.
    ///
    /// This is the primary persistence method used by the StateCollector.
    /// It stores the actual object states after collecting snapshots from all proxies.
    ///
    /// # Arguments
    /// * `objects` - Map of object ID to Object containing version and data
    ///
    /// # Key Format
    /// `<object_id>` -> `(version, serialized_object_data)`
    ///
    /// # Example
    /// ```rust
    /// let objects = BTreeMap::from([
    ///     (object_id_1, object_1),
    ///     (object_id_2, object_2),
    /// ]);
    /// store.persist_objects(&objects)?;
    /// ```
    pub fn persist_objects(
        &self,
        objects: &std::collections::BTreeMap<ObjectID, Object>,
    ) -> anyhow::Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        for (obj_id, obj) in objects.iter() {
            let key = obj_id.as_ref();
            let value = bincode::serialize(obj)?;
            batch.put(key, value);
        }
        self.db.write(batch)?;
        Ok(())
    }
}
