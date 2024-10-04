pub mod helpers;
pub mod traits;

use openmls_traits::storage::*;
use sled::Db;
use std::path::Path;

pub struct SledStorage {
    db: Db,
}
/// Errors thrown by the key store.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum SledStorageError {
    #[error("Sled error: {0}")]
    SledError(#[from] sled::Error),
    #[error("Serialization error")]
    SerializationError,
    #[error("Value does not exist.")]
    None,
}

impl From<serde_json::Error> for SledStorageError {
    fn from(_: serde_json::Error) -> Self {
        Self::SerializationError
    }
}

impl SledStorage {
    /// Creates a new SledStorage instance from a given path.
    ///
    /// # Arguments
    ///
    /// * `path` - A path-like object representing the location to store the database.
    ///
    /// # Returns
    ///
    /// A Result containing the new SledStorage instance or a SledStorageError.
    pub fn new_from_path<P: AsRef<Path>>(path: P) -> Result<Self, SledStorageError> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    /// Creates a new SledStorage instance from an existing Sled database.
    ///
    /// # Arguments
    ///
    /// * `db` - An existing Sled database instance.
    ///
    /// # Returns
    ///
    /// A new SledStorage instance.
    pub fn new_from_db(db: Db) -> Result<Self, SledStorageError> {
        Ok(Self { db })
    }

    /// Writes a value to the storage with the given tree and key.
    ///
    /// # Arguments
    ///
    /// * `tree` - The tree for the storage entry. A Tree in Sled represents a single logical keyspace / namespace / bucket.
    /// * `key` - The key for the storage entry.
    /// * `value` - The value to be stored.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a SledStorageError.
    fn write<const VERSION: u16>(
        &self,
        tree: &[u8],
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let active_tree = self.db.open_tree(tree)?;

        #[cfg(feature = "test-utils")]
        log::debug!(target: "openmls-sled-storage", "Writing to key: {:?} in tree: {:?}", hex::encode(&key), hex::encode(&tree));

        // Serialize the value before storing
        let serialized_value =
            serde_json::to_vec(&value).map_err(|_| SledStorageError::SerializationError)?;

        match active_tree.insert(key, serialized_value) {
            Ok(_res) => Ok(()),
            Err(e) => Err(SledStorageError::SledError(e)),
        }
    }

    /// Appends a value to a list stored at the given label and key.
    ///
    /// # Arguments
    ///
    /// * `tree` - The tree for the storage entry. A Tree in Sled represents a single logical keyspace / namespace / bucket.
    /// * `key` - The key for the storage entry.
    /// * `value` - The value to be appended.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a SledStorageError.
    fn append<const VERSION: u16>(
        &self,
        tree: &[u8],
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let active_tree = self.db.open_tree(tree)?;

        #[cfg(feature = "test-utils")]
        log::debug!(target: "openmls-sled-storage", "Appending to key: {:?} in tree: {:?}", hex::encode(&key), hex::encode(&tree));

        let list_bytes = active_tree.get(key)?;
        let mut list: Vec<Vec<u8>> = Vec::new();
        if let Some(list_bytes) = list_bytes {
            list = serde_json::from_slice(&list_bytes)
                .map_err(|_e| SledStorageError::SerializationError)?;
        }

        list.push(value);

        let updated_list_bytes =
            serde_json::to_vec(&list).map_err(|_e| SledStorageError::SerializationError)?;

        match active_tree.insert(key, updated_list_bytes) {
            Ok(_res) => Ok(()),
            Err(e) => Err(SledStorageError::SledError(e)),
        }
    }

    /// Reads a value from the storage with the given label and key.
    ///
    /// # Arguments
    ///
    /// * `tree` - The tree for the storage entry. A Tree in Sled represents a single logical keyspace / namespace / bucket.
    /// * `key` - The key for the storage entry.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    /// * `V` - The type of the entity to be read, which must implement the `Entity<VERSION>` trait.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option` with the value (if found) or a `SledStorageError`.
    fn read<const VERSION: u16, V: Entity<VERSION>>(
        &self,
        tree: &[u8],
        key: &[u8],
    ) -> Result<Option<V>, <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let active_tree = self.db.open_tree(tree)?;

        #[cfg(feature = "test-utils")]
        log::debug!(target: "openmls-sled-storage", "Reading key: {:?} in tree: {:?}", hex::encode(&key), hex::encode(&tree));

        match active_tree.get(key) {
            Ok(None) => Ok(None),
            Ok(Some(value)) => {
                let deserialized: Vec<u8> = serde_json::from_slice(&value)?;
                Ok(Some(serde_json::from_slice(&deserialized)?))
            }
            Err(e) => Err(SledStorageError::SledError(e)),
        }
    }

    /// Reads a list of entities from the storage with the given label and key.
    ///
    /// # Arguments
    ///
    /// * `tree` - The tree for the storage entry. A Tree in Sled represents a single logical keyspace / namespace / bucket.
    /// * `key` - The key for the storage entry.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    /// * `V` - The type of the entity to be read, which must implement the `Entity<VERSION>` trait.
    ///
    /// # Returns
    ///
    /// A Result containing a Vec of entities or a SledStorageError.
    fn read_list<const VERSION: u16, V: Entity<VERSION>>(
        &self,
        tree: &[u8],
        key: &[u8],
    ) -> Result<Vec<V>, <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let active_tree = self.db.open_tree(tree)?;

        #[cfg(feature = "test-utils")]
        log::debug!(target: "openmls-sled-storage", "Reading list from key: {:?} in tree: {:?}", hex::encode(&key), hex::encode(&tree));

        let value: Vec<Vec<u8>> = match active_tree.get(key) {
            Ok(Some(list_bytes)) => serde_json::from_slice(&list_bytes).unwrap(),
            Ok(None) => vec![],
            Err(e) => return Err(SledStorageError::SledError(e)),
        };

        value
            .iter()
            .map(|value_bytes| serde_json::from_slice(value_bytes))
            .collect::<Result<Vec<V>, _>>()
            .map_err(|_| SledStorageError::SerializationError)
    }

    /// Removes a specific item from a list stored at the given label and key.
    ///
    /// # Arguments
    ///
    /// * `tree` - The tree for the storage entry. A Tree in Sled represents a single logical keyspace / namespace / bucket.
    /// * `key` - The key for the storage entry.
    /// * `value` - The value to be removed.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a SledStorageError.
    fn remove_item<const VERSION: u16>(
        &self,
        tree: &[u8],
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let active_tree = self.db.open_tree(tree)?;

        #[cfg(feature = "test-utils")]
        log::debug!(target: "openmls-sled-storage", "Removing item from key: {:?} in tree: {:?}", hex::encode(&key), hex::encode(&tree));

        // fetch value from db, falling back to an empty list if doens't exist
        let list = match active_tree.get(key) {
            Ok(Some(list)) => list,
            Ok(None) => Vec::new().into(),
            Err(e) => return Err(SledStorageError::SledError(e)),
        };

        // parse old value, find value to delete and remove it from list
        let mut parsed_list: Vec<Vec<u8>> = serde_json::from_slice(&list)?;
        if let Some(pos) = parsed_list
            .iter()
            .position(|stored_item| stored_item == &value)
        {
            parsed_list.remove(pos);
        }

        // write back, reusing the old buffer
        let updated_list_bytes =
            serde_json::to_vec(&parsed_list).map_err(|_e| SledStorageError::SerializationError)?;

        match active_tree.insert(key, updated_list_bytes) {
            Ok(_res) => Ok(()),
            Err(e) => Err(SledStorageError::SledError(e)),
        }
    }

    /// Deletes an entry from the storage with the given label and key.
    ///
    /// # Arguments
    ///
    /// * `tree` - The tree for the storage entry. A Tree in Sled represents a single logical keyspace / namespace / bucket.
    /// * `key` - The key for the storage entry.
    ///
    /// # Type Parameters
    ///
    /// * `VERSION` - The version of the storage format.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a SledStorageError.
    fn delete<const VERSION: u16>(
        &self,
        tree: &[u8],
        key: &[u8],
    ) -> Result<(), <Self as StorageProvider<CURRENT_VERSION>>::Error> {
        let active_tree = self.db.open_tree(tree)?;

        #[cfg(feature = "test-utils")]
        log::debug!(target: "openmls-sled-storage", "Deleting key: {:?} in tree: {:?}", hex::encode(&storage_key), hex::encode(&tree));

        match active_tree.remove(key) {
            Ok(_res) => Ok(()),
            Err(e) => Err(SledStorageError::SledError(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const CURRENT_VERSION: u16 = 1; // Assuming CURRENT_VERSION is 1, adjust if needed

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct TestEntity {
        data: String,
    }

    impl Entity<CURRENT_VERSION> for TestEntity {}

    fn setup_storage() -> SledStorage {
        let dir = tempdir().unwrap();
        SledStorage::new_from_path(dir.path()).unwrap()
    }

    #[test]
    fn test_new_from_path() {
        let dir = tempdir().unwrap();
        let storage = SledStorage::new_from_path(dir.path());
        assert!(storage.is_ok());
        assert!(storage.unwrap().db.tree_names().len() == 1);
    }

    #[test]
    fn test_new_from_db() {
        let db = sled::open(tempdir().unwrap()).unwrap();
        let storage = SledStorage::new_from_db(db);
        assert!(storage.is_ok());
        assert!(storage.unwrap().db.tree_names().len() == 1);
    }

    #[test]
    fn test_write_and_read() {
        let storage = setup_storage();
        let tree = b"test_tree";
        let key = b"test_key";
        let value = TestEntity {
            data: "test_data".to_string(),
        };

        let write_result =
            storage.write::<CURRENT_VERSION>(tree, key, serde_json::to_vec(&value).unwrap());
        assert!(write_result.is_ok());

        let read_result: Result<Option<TestEntity>, _> =
            storage.read::<CURRENT_VERSION, _>(tree, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), Some(value));
    }

    #[test]
    fn test_append_and_read_list() {
        let storage = setup_storage();
        let tree = b"test_tree";
        let key = b"test_key";
        let values = vec![
            TestEntity {
                data: "data1".to_string(),
            },
            TestEntity {
                data: "data2".to_string(),
            },
        ];

        for value in &values {
            let append_result =
                storage.append::<CURRENT_VERSION>(tree, key, serde_json::to_vec(value).unwrap());
            assert!(append_result.is_ok());
        }

        let read_result: Result<Vec<TestEntity>, _> =
            storage.read_list::<CURRENT_VERSION, _>(tree, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), values);
    }

    #[test]
    fn test_remove_item() {
        let storage = setup_storage();
        let tree = b"test_tree";
        let key = b"test_key";
        let values = vec![
            TestEntity {
                data: "data1".to_string(),
            },
            TestEntity {
                data: "data2".to_string(),
            },
        ];

        for value in &values {
            storage
                .append::<CURRENT_VERSION>(tree, key, serde_json::to_vec(value).unwrap())
                .unwrap();
        }

        let remove_result = storage.remove_item::<CURRENT_VERSION>(
            tree,
            key,
            serde_json::to_vec(&values[0]).unwrap(),
        );
        assert!(remove_result.is_ok());

        let read_result: Result<Vec<TestEntity>, _> =
            storage.read_list::<CURRENT_VERSION, _>(tree, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), vec![values[1].clone()]);
    }

    #[test]
    fn test_delete() {
        let storage = setup_storage();
        let tree = b"test_tree";
        let key = b"test_key";
        let value = TestEntity {
            data: "test_data".to_string(),
        };

        storage
            .write::<CURRENT_VERSION>(tree, key, serde_json::to_vec(&value).unwrap())
            .unwrap();

        let delete_result = storage.delete::<CURRENT_VERSION>(tree, key);
        assert!(delete_result.is_ok());

        let read_result: Result<Option<TestEntity>, _> =
            storage.read::<CURRENT_VERSION, _>(tree, key);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), None);
    }
}
