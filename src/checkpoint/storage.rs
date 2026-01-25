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

    /// Retrieve an object by its ID.
    ///
    /// # Arguments
    /// * `object_id` - The ID of the object to retrieve
    ///
    /// # Returns
    /// * `Ok(Some(Object))` - Object found and deserialized
    /// * `Ok(None)` - Object not found
    /// * `Err(anyhow::Error)` - Deserialization error
    pub fn get_object(&self, object_id: &ObjectID) -> anyhow::Result<Option<Object>> {
        match self.db.get(object_id.as_ref())? {
            Some(data) => {
                let object = bincode::deserialize(&data)?;
                Ok(Some(object))
            }
            None => Ok(None),
        }
    }

    /// List all stored object IDs.
    ///
    /// # Returns
    /// * `Vec<ObjectID>` - All stored object IDs
    pub fn list_object_ids(&self) -> anyhow::Result<Vec<ObjectID>> {
        let mut object_ids = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, _) = item?;
            if let Ok(object_id) = ObjectID::from_bytes(&key) {
                object_ids.push(object_id);
            }
        }

        Ok(object_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use sui_types::object::Object;
    use tempfile::TempDir;

    fn create_test_object(id: ObjectID) -> Object {
        Object::immutable_with_id_for_testing(id)
    }

    #[test]
    fn test_rocks_snapshot_store_open() {
        let temp_dir = TempDir::new().unwrap();
        let store = RocksSnapshotStore::open(temp_dir.path().to_path_buf());
        assert!(store.is_ok());
    }

    #[test]
    fn test_rocks_snapshot_store_persist_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let store = RocksSnapshotStore::open(temp_dir.path().to_path_buf()).unwrap();

        // Create test objects
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj1 = create_test_object(obj_id1);
        let obj2 = create_test_object(obj_id2);

        // Persist objects
        let mut objects = BTreeMap::new();
        objects.insert(obj_id1, obj1.clone());
        objects.insert(obj_id2, obj2.clone());

        store.persist_objects(&objects).unwrap();

        // Retrieve objects
        let retrieved_obj1 = store.get_object(&obj_id1).unwrap();
        let retrieved_obj2 = store.get_object(&obj_id2).unwrap();

        assert_eq!(retrieved_obj1, Some(obj1));
        assert_eq!(retrieved_obj2, Some(obj2));
    }

    #[test]
    fn test_rocks_snapshot_store_get_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let store = RocksSnapshotStore::open(temp_dir.path().to_path_buf()).unwrap();

        let obj_id = ObjectID::random();
        let retrieved = store.get_object(&obj_id).unwrap();
        assert_eq!(retrieved, None);
    }

    #[test]
    fn test_rocks_snapshot_store_list_object_ids() {
        let temp_dir = TempDir::new().unwrap();
        let store = RocksSnapshotStore::open(temp_dir.path().to_path_buf()).unwrap();

        // Create and persist test objects
        let obj_id1 = ObjectID::random();
        let obj_id2 = ObjectID::random();
        let obj_id3 = ObjectID::random();

        let mut objects = BTreeMap::new();
        objects.insert(obj_id1, create_test_object(obj_id1));
        objects.insert(obj_id2, create_test_object(obj_id2));
        objects.insert(obj_id3, create_test_object(obj_id3));

        store.persist_objects(&objects).unwrap();

        // List object IDs
        let mut object_ids = store.list_object_ids().unwrap();
        object_ids.sort();

        let mut expected_ids = vec![obj_id1, obj_id2, obj_id3];
        expected_ids.sort();

        assert_eq!(object_ids, expected_ids);
    }

    #[test]
    fn test_rocks_snapshot_store_overwrite() {
        let temp_dir = TempDir::new().unwrap();
        let store = RocksSnapshotStore::open(temp_dir.path().to_path_buf()).unwrap();

        let obj_id = ObjectID::random();
        let obj1 = create_test_object(obj_id);
        let obj2 = create_test_object(obj_id); // Same ID, different object

        // Persist first object
        let mut objects1 = BTreeMap::new();
        objects1.insert(obj_id, obj1.clone());
        store.persist_objects(&objects1).unwrap();

        // Overwrite with second object
        let mut objects2 = BTreeMap::new();
        objects2.insert(obj_id, obj2.clone());
        store.persist_objects(&objects2).unwrap();

        // Should retrieve the second object
        let retrieved = store.get_object(&obj_id).unwrap();
        assert_eq!(retrieved, Some(obj2));
    }

    #[test]
    fn test_rocks_snapshot_store_empty() {
        let temp_dir = TempDir::new().unwrap();
        let store = RocksSnapshotStore::open(temp_dir.path().to_path_buf()).unwrap();

        // Persist empty map
        let objects = BTreeMap::new();
        store.persist_objects(&objects).unwrap();

        // Should have no objects
        let object_ids = store.list_object_ids().unwrap();
        assert!(object_ids.is_empty());
    }
}
