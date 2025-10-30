use bytes::Bytes;
use kore::Cache;

#[tokio::test]
async fn test_config_get_max_entry_size() {
    // Create cache with default max entry size (500MB)
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);
    
    // Verify default value
    assert_eq!(cache.get_max_entry_size(), 500 * 1024 * 1024);
}

#[tokio::test]
async fn test_config_set_max_entry_size() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);
    
    // Set to 100MB
    let new_size = 100 * 1024 * 1024;
    assert!(cache.set_max_entry_size(new_size).is_ok());
    assert_eq!(cache.get_max_entry_size(), new_size);
    
    // Set to 10MB
    let new_size = 10 * 1024 * 1024;
    assert!(cache.set_max_entry_size(new_size).is_ok());
    assert_eq!(cache.get_max_entry_size(), new_size);
}

#[tokio::test]
async fn test_config_set_max_entry_size_validation() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 500 * 1024 * 1024, false);
    
    // Test minimum size validation (less than 1KB should fail)
    assert!(cache.set_max_entry_size(512).is_err());
    assert!(cache.set_max_entry_size(1023).is_err());
    
    // Test that 1KB exactly should work
    assert!(cache.set_max_entry_size(1024).is_ok());
    
    // Test exceeding max memory should fail
    assert!(cache.set_max_entry_size(200 * 1024 * 1024).is_err());
}

#[tokio::test]
async fn test_max_entry_size_enforcement() {
    // Create cache with small max entry size (1MB)
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 1024 * 1024, false);
    
    // Try to store an entry larger than max_entry_size
    let key = Bytes::from("large_key");
    let large_value = Bytes::from(vec![b'x'; 2 * 1024 * 1024]); // 2MB
    
    let result = cache.store(key.clone(), large_value, Default::default());
    assert!(result.is_err());
    
    // Verify error type
    match result {
        Err(kore::error::Error::EntryTooLarge) => {
            // Expected error
        }
        _ => panic!("Expected EntryTooLarge error"),
    }
}

#[tokio::test]
async fn test_max_entry_size_allows_smaller_entries() {
    // Create cache with 10MB max entry size
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 10 * 1024 * 1024, false);
    
    // Store entry smaller than max_entry_size (1MB)
    let key = Bytes::from("normal_key");
    let value = Bytes::from(vec![b'x'; 1024 * 1024]); // 1MB
    
    let result = cache.store(key.clone(), value.clone(), Default::default());
    assert!(result.is_ok());
    
    // Verify we can retrieve it
    let retrieved = cache.load(&key, Default::default()).unwrap();
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().value, value);
}

#[tokio::test]
async fn test_runtime_max_entry_size_change() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 10 * 1024 * 1024, false);
    
    // Initially, 5MB entry should work
    let key1 = Bytes::from("key1");
    let value1 = Bytes::from(vec![b'x'; 5 * 1024 * 1024]); // 5MB
    assert!(cache.store(key1, value1, Default::default()).is_ok());
    
    // Reduce max entry size to 2MB
    assert!(cache.set_max_entry_size(2 * 1024 * 1024).is_ok());
    
    // Now 5MB entry should fail
    let key2 = Bytes::from("key2");
    let value2 = Bytes::from(vec![b'y'; 5 * 1024 * 1024]); // 5MB
    assert!(cache.store(key2, value2, Default::default()).is_err());
    
    // But 1MB entry should work
    let key3 = Bytes::from("key3");
    let value3 = Bytes::from(vec![b'z'; 1024 * 1024]); // 1MB
    assert!(cache.store(key3, value3, Default::default()).is_ok());
}

#[tokio::test]
async fn test_max_entry_size_boundary() {
    let max_size = 5 * 1024 * 1024; // 5MB
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, max_size, false);

    // Calculate the overhead size (Entry struct size)
    let entry_overhead = std::mem::size_of::<kore::entry::Entry>();

    // Test exactly at the boundary (key + value + overhead = max_size)
    let key = Bytes::from("test_key");
    let key_len = key.len();
    let value = Bytes::from(vec![b'x'; max_size - key_len - entry_overhead]);

    let result = cache.store(key.clone(), value, Default::default());
    assert!(result.is_ok());

    // Test just over the boundary
    let key2 = Bytes::from("test_key2");
    let key2_len = key2.len();
    let value2 = Bytes::from(vec![b'y'; max_size - key2_len - entry_overhead + 1]);

    let result2 = cache.store(key2, value2, Default::default());
    assert!(result2.is_err());
}

#[tokio::test]
async fn test_max_entry_size_with_update() {
    let cache = Cache::new_with_sweep(16, 1024 * 1024 * 100, 10 * 1024 * 1024, false);
    
    // Store initial value
    let key = Bytes::from("update_key");
    let value1 = Bytes::from(vec![b'x'; 1024 * 1024]); // 1MB
    assert!(cache.store(key.clone(), value1, Default::default()).is_ok());
    
    // Reduce max entry size to 512KB
    assert!(cache.set_max_entry_size(512 * 1024).is_ok());
    
    // Try to update with larger value (should fail)
    let value2 = Bytes::from(vec![b'y'; 1024 * 1024]); // 1MB
    assert!(cache.store(key.clone(), value2, Default::default()).is_err());
    
    // Update with smaller value (should work)
    let value3 = Bytes::from(vec![b'z'; 256 * 1024]); // 256KB
    assert!(cache.store(key, value3, Default::default()).is_ok());
}
