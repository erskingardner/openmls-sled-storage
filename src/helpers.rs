use crate::SledStorage;
use openmls_traits::storage::*;
use serde::Serialize;

/// Builds a key with version and label.
///
/// This function takes a label and a key as byte slices, and returns a new Vec<u8>
/// that combines them with a version number.
///
/// # Arguments
///
/// * `label` - A byte slice representing the label.
/// * `key` - A Vec<u8> representing the key.
///
/// # Returns
///
/// A Vec<u8> containing the label, key, and version number.
pub fn build_key_from_vec<const V: u16>(label: &[u8], key: Vec<u8>) -> Vec<u8> {
    let mut key_out = label.to_vec();
    key_out.extend_from_slice(&key);
    key_out.extend_from_slice(&u16::to_be_bytes(V));
    key_out
}

/// Builds a key with version and label, serializing the key.
///
/// This function is similar to `build_key_from_vec`, but it takes a serializable key
/// and serializes it before building the final key.
///
/// # Arguments
///
/// * `label` - A byte slice representing the label.
/// * `key` - A value of type K that implements Serialize.
///
/// # Returns
///
/// A Vec<u8> containing the label, serialized key, and version number.
pub fn build_key<const V: u16, K: Serialize>(label: &[u8], key: K) -> Vec<u8> {
    build_key_from_vec::<V>(label, serde_json::to_vec(&key).unwrap())
}

/// Generates a unique identifier for epoch key pairs.
///
/// This function creates a unique identifier by combining the group ID, epoch, and leaf index.
///
/// # Arguments
///
/// * `group_id` - A reference to a type implementing GroupId trait.
/// * `epoch` - A reference to a type implementing EpochKey trait.
/// * `leaf_index` - A u32 representing the leaf index.
///
/// # Returns
///
/// A Result containing a Vec<u8> if successful, or an error if serialization fails.
pub fn epoch_key_pairs_id(
    group_id: &impl traits::GroupId<CURRENT_VERSION>,
    epoch: &impl traits::EpochKey<CURRENT_VERSION>,
    leaf_index: u32,
) -> Result<Vec<u8>, <SledStorage as StorageProvider<CURRENT_VERSION>>::Error> {
    let mut key = serde_json::to_vec(group_id)?;
    key.extend_from_slice(&serde_json::to_vec(epoch)?);
    key.extend_from_slice(&serde_json::to_vec(&leaf_index)?);
    Ok(key)
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[test]
    fn test_build_key_from_vec() {
        let label = b"test_label";
        let key = vec![1, 2, 3, 4];
        let result = build_key_from_vec::<1>(label, key);
        assert_eq!(
            result,
            vec![116, 101, 115, 116, 95, 108, 97, 98, 101, 108, 1, 2, 3, 4, 0, 1]
        );
    }

    #[test]
    fn test_build_key() {
        #[derive(Serialize)]
        struct TestKey {
            id: u32,
        }
        let label = b"test_label";
        let key = TestKey { id: 42 };
        let result = build_key::<1, _>(label, key);
        assert_eq!(
            result,
            vec![
                116, 101, 115, 116, 95, 108, 97, 98, 101, 108, 123, 34, 105, 100, 34, 58, 52, 50,
                125, 0, 1
            ]
        );
    }

    #[derive(Serialize)]
    struct MockGroupId {
        id: String,
    }

    impl Key<CURRENT_VERSION> for MockGroupId {}
    impl traits::GroupId<CURRENT_VERSION> for MockGroupId {}

    #[derive(Serialize)]
    struct MockEpochKey {
        epoch: u64,
    }

    impl Key<CURRENT_VERSION> for MockEpochKey {}
    impl traits::EpochKey<CURRENT_VERSION> for MockEpochKey {}

    #[test]
    fn test_epoch_key_pairs_id() {
        let group_id = MockGroupId {
            id: "test_group".to_string(),
        };
        let epoch_key = MockEpochKey { epoch: 42 };
        let leaf_index = 123u32;

        let result = epoch_key_pairs_id(&group_id, &epoch_key, leaf_index).unwrap();

        // Verify the result contains all components
        let expected_group_id = serde_json::to_vec(&group_id).unwrap();
        let expected_epoch = serde_json::to_vec(&epoch_key).unwrap();
        let expected_leaf = serde_json::to_vec(&leaf_index).unwrap();

        // Check that the result starts with the group_id
        assert!(result.starts_with(&expected_group_id));

        // Check that epoch and leaf_index components are present in the correct order
        let epoch_start = expected_group_id.len();
        let leaf_start = epoch_start + expected_epoch.len();

        assert_eq!(&result[epoch_start..leaf_start], expected_epoch);
        assert_eq!(&result[leaf_start..], expected_leaf);
    }

    #[test]
    fn test_build_key_from_vec_empty() {
        let label = b"";
        let key = Vec::<u8>::new();
        let result = build_key_from_vec::<1>(label, key);
        assert_eq!(result, vec![0, 1]); // Just contains version number
    }

    #[test]
    fn test_build_key_different_versions() {
        let label = b"test";
        let key = vec![1, 2, 3];

        let result_v1 = build_key_from_vec::<1>(label, key.clone());
        let result_v2 = build_key_from_vec::<2>(label, key.clone());

        assert_eq!(result_v1[result_v1.len() - 2..], vec![0, 1]);
        assert_eq!(result_v2[result_v2.len() - 2..], vec![0, 2]);
    }

    #[test]
    fn test_build_key_with_special_characters() {
        #[derive(Serialize)]
        struct TestKey {
            value: String,
        }
        let label = b"test_label";
        let key = TestKey {
            value: "special@#$%^&*".to_string(),
        };
        let result = build_key::<1, _>(label, key);
        // We don't assert exact bytes since JSON serialization might vary,
        // but we ensure the key is properly constructed
        assert!(result.starts_with(label));
        assert_eq!(result[result.len() - 2..], vec![0, 1]);
    }

    #[test]
    fn test_epoch_key_pairs_id_with_large_values() {
        let group_id = MockGroupId {
            id: "a".repeat(1000), // Test with a large string
        };
        let epoch_key = MockEpochKey {
            epoch: u64::MAX, // Test with maximum epoch value
        };
        let leaf_index = u32::MAX; // Test with maximum leaf index

        let result = epoch_key_pairs_id(&group_id, &epoch_key, leaf_index).unwrap();

        // Verify components are present
        let expected_group_id = serde_json::to_vec(&group_id).unwrap();
        let expected_epoch = serde_json::to_vec(&epoch_key).unwrap();
        let expected_leaf = serde_json::to_vec(&leaf_index).unwrap();

        assert!(result.starts_with(&expected_group_id));
        assert_eq!(
            result.len(),
            expected_group_id.len() + expected_epoch.len() + expected_leaf.len()
        );
    }

    #[test]
    fn test_build_key_with_unicode() {
        #[derive(Serialize)]
        struct TestKey {
            value: String,
        }
        let label = "🔑".as_bytes(); // Unicode label
        let key = TestKey {
            value: "Hello 世界".to_string(),
        };
        let result = build_key::<1, _>(label, key);
        assert!(result.starts_with(label));
        assert_eq!(result[result.len() - 2..], vec![0, 1]);
    }
}
