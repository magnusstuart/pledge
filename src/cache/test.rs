use super::lfu::CachedResponse;
use super::lfu::{Cache, CacheInner, Entry};
use std::{
    sync::{Arc, atomic::AtomicU64},
    thread::sleep,
    time::{Duration, Instant},
};

#[cfg(test)]
fn helper_get_data_length(entry: &Entry) -> u64 {
    let data_length: u64 = {
        let row_desc = match entry.data.row_desc.as_ref() {
            Some(value) => value,
            None => &Vec::new(),
        };
        let param_desc = match entry.data.param_desc.as_ref() {
            Some(value) => value,
            None => &Vec::new(),
        };
        entry.data.data.len() as u64 + row_desc.len() as u64 + param_desc.len() as u64
    };
    data_length
}

#[cfg(test)]
fn helper_create_1mb_entry_data(key: &str) -> Vec<u8> {
    let one_mb_in_bytes: u64 = 1_048_576;

    let entry = Entry {
        data: Arc::new(CachedResponse {
            data: vec![1],
            row_desc: None,
            param_desc: None,
        }),
        counter: AtomicU64::new(0),
        expires_at: Instant::now(),
    };

    let overhead = entry.get_size(key) - helper_get_data_length(&entry);

    helper_create_vector(one_mb_in_bytes - overhead)
}

#[cfg(test)]
fn helper_create_custom_size_entry_data(key: &str, total_bytes_size: u64) -> Vec<u8> {
    let entry = Entry {
        data: Arc::new(CachedResponse {
            data: vec![1],
            row_desc: None,
            param_desc: None,
        }),
        counter: AtomicU64::new(0),
        expires_at: Instant::now(),
    };
    if (total_bytes_size as i64 - (entry.get_size(key) - helper_get_data_length(&entry)) as i64)
        <= 0
    {
        panic!(
            "Total bytes size is lower than the overhead, not a bug in the code just one for the helper"
        );
    }
    let overhead = entry.get_size(key) - helper_get_data_length(&entry);

    helper_create_vector(total_bytes_size - overhead)
}

#[cfg(test)]
fn helper_create_vector(size: u64) -> Vec<u8> {
    (0..size).map(|_| 255).collect()
}

#[cfg(test)]
fn helper_insert_entry(
    key: &str,
    data: Arc<CachedResponse>,
    expires_in: u64,
    inner: &mut CacheInner,
) {
    inner.insert(
        key.to_string(),
        data,
        Instant::now() + Duration::from_secs(expires_in),
    );
}

#[test]
fn test_new_cache_is_empty() {
    let inner = CacheInner::new(10 * 1024 * 1024);
    assert_eq!(inner.entries.len(), 0);
}

#[test]
fn test_insert_and_get() {
    let mut inner = CacheInner::new(10 * 1024 * 1024);
    inner.insert(
        "key1".to_string(),
        Arc::new(CachedResponse {
            data: b"value1".to_vec(),
            param_desc: None,
            row_desc: None,
        }),
        Instant::now() + Duration::from_secs(60),
    );

    let result = inner.get("key1");
    assert_eq!(
        result,
        (
            Some(Arc::new(CachedResponse {
                data: b"value1".to_vec(),
                row_desc: None,
                param_desc: None
            })),
            false
        )
    );
}

#[test]
fn test_get_nonexistent_key() {
    let inner = CacheInner::new(10 * 1024 * 1024);
    assert_eq!(inner.get("missing"), (None, false));
}

#[test]
fn test_remove() {
    let mut inner = CacheInner::new(10 * 1024 * 1024);
    inner.insert(
        "key1".to_string(),
        b"value1".to_vec(),
        Instant::now() + Duration::from_secs(60),
    );

    inner.remove("key1");

    assert_eq!(inner.get("key1"), (None, false));
    assert_eq!(inner.entries.len(), 0);
}

#[test]
fn test_insert_duplicate_key_updates_value() {
    let mut inner = CacheInner::new(10 * 1024 * 1024);
    inner.insert(
        "key1".to_string(),
        b"value1".to_vec(),
        Instant::now() + Duration::from_secs(60),
    );
    inner.insert(
        "key1".to_string(),
        b"updated".to_vec(),
        Instant::now() + Duration::from_secs(60),
    );

    assert_eq!(inner.get("key1"), (Some(b"updated".to_vec()), false));
    assert_eq!(inner.entries.len(), 1);
}

// === TTL Expiration ===

#[test]
fn test_ttl_expiration() {
    let mut inner = CacheInner::new(10 * 1024 * 1024);
    inner.insert(
        "key1".to_string(),
        b"value1".to_vec(),
        Instant::now() + Duration::from_secs(1),
    ); // 1 second TTL

    // Should exist immediately
    assert_eq!(inner.get("key1"), (Some(b"value1".to_vec()), false));

    // Wait for expiration
    sleep(Duration::from_secs(2));

    // Should be gone
    assert_eq!(inner.get("key1"), (None, true));
}

#[test]
fn test_evict_lfu_empty_cache() {
    let mut inner = CacheInner::new(10 * 1024 * 1024);
    inner.evict_lfu(); // should not panic
    assert_eq!(inner.entries.len(), 0);
}

#[test]
fn test_remove_nonexistent_key() {
    let mut cache = CacheInner::new(10 * 1024 * 1024);
    cache.remove("ghost"); // should not panic
}

#[test]
fn test_full_cache_evicts_non_accessed() {
    let mut inner = CacheInner::new(4 * 1024 * 1024);

    helper_insert_entry("key1", helper_create_1mb_entry_data("key1"), 60, &mut inner);
    helper_insert_entry("key2", helper_create_1mb_entry_data("key2"), 60, &mut inner);
    helper_insert_entry("key3", helper_create_1mb_entry_data("key3"), 60, &mut inner);
    helper_insert_entry("key4", helper_create_1mb_entry_data("key4"), 60, &mut inner);

    inner.get("key2");
    for _ in 0..10 {
        inner.get("key3");
        inner.get("key4");
    }
    helper_insert_entry("key5", helper_create_1mb_entry_data("key5"), 60, &mut inner);

    assert_eq!(inner.entries.len(), 4);
    assert_eq!(inner.entries.get("key1").is_none(), true);
    assert_eq!(inner.entries.get("key2").is_some(), true);
    assert_eq!(inner.entries.get("key3").is_some(), true);
    assert_eq!(inner.entries.get("key4").is_some(), true);
    assert_eq!(inner.entries.get("key5").is_some(), true);
}

#[test]
fn test_counter_increment() {
    let mut inner = CacheInner::new(1 * 1024 * 1024);
    helper_insert_entry("key1", vec![1], 60, &mut inner);

    assert_eq!(
        inner
            .entries
            .get("key1")
            .unwrap()
            .counter
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    for _ in 1..5 {
        inner.get("key1");
    }
    assert_eq!(
        inner
            .entries
            .get("key1")
            .unwrap()
            .counter
            .load(std::sync::atomic::Ordering::Relaxed),
        5
    );

    for _ in 5..100 {
        inner.get("key1");
    }
    assert_eq!(
        inner
            .entries
            .get("key1")
            .unwrap()
            .counter
            .load(std::sync::atomic::Ordering::Relaxed),
        100
    );
    for _ in 100..1027 {
        inner.get("key1");
    }
    assert_eq!(
        inner
            .entries
            .get("key1")
            .unwrap()
            .counter
            .load(std::sync::atomic::Ordering::Relaxed),
        1027
    );
}

#[test]
fn test_reinsert_preserves_counter() {
    let mut inner = CacheInner::new(1 * 1024 * 1024);
    helper_insert_entry("key1", vec![1], 60, &mut inner);

    assert_eq!(
        inner
            .entries
            .get("key1")
            .unwrap()
            .counter
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );

    for _ in 1..5 {
        inner.get("key1");
    }
    assert_eq!(
        inner
            .entries
            .get("key1")
            .unwrap()
            .counter
            .load(std::sync::atomic::Ordering::Relaxed),
        5
    );

    helper_insert_entry("key1", vec![1, 2], 60, &mut inner);
    assert_eq!(
        inner
            .entries
            .get("key1")
            .unwrap()
            .counter
            .load(std::sync::atomic::Ordering::Relaxed),
        5
    );
}

#[test]
fn test_evict_equal_frequencies() {
    let mut inner = CacheInner::new(1 * 1024 * 1024);

    for i in 0..500 {
        helper_insert_entry(format!("key{}", i).as_str(), vec![1], 60, &mut inner);
    }
    assert_eq!(inner.entries.len(), 500);

    for i in 0..500 {
        inner.evict_lfu();
        assert_eq!(inner.entries.len(), 500 - (i + 1));
    }
    assert_eq!(inner.entries.len(), 0);
}

#[test]
fn test_evict_gradual_frequencies() {
    let mut inner = CacheInner::new(1 * 1024 * 1024);
    helper_insert_entry("key1", vec![1], 60, &mut inner);
    helper_insert_entry("key2", vec![1], 60, &mut inner);
    helper_insert_entry("key3", vec![1], 60, &mut inner);
    helper_insert_entry("key4", vec![1], 60, &mut inner);
    helper_insert_entry("key5", vec![1], 60, &mut inner);

    for _ in 0..1 {
        inner.get("key2");
    }
    for _ in 0..2 {
        inner.get("key3");
    }
    for _ in 0..3 {
        inner.get("key4");
    }
    for _ in 0..4 {
        inner.get("key5");
    }

    for i in 1..5 {
        assert_eq!(
            inner
                .entries
                .get(format!("key{}", i).as_str())
                .unwrap()
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            i
        );
    }

    inner.evict_lfu();
    assert_eq!(inner.entries.contains_key("key1"), false);
    for i in 2..5 {
        assert_eq!(
            inner.entries.contains_key(format!("key{}", i).as_str()),
            true
        );
    }

    inner.evict_lfu();
    assert_eq!(inner.entries.contains_key("key2"), false);
    for i in 3..5 {
        assert_eq!(
            inner.entries.contains_key(format!("key{}", i).as_str()),
            true
        );
    }

    inner.evict_lfu();
    assert_eq!(inner.entries.contains_key("key3"), false);
    for i in 4..5 {
        assert_eq!(
            inner.entries.contains_key(format!("key{}", i).as_str()),
            true
        );
    }

    inner.evict_lfu();
    assert_eq!(inner.entries.contains_key("key4"), false);
    assert_eq!(inner.entries.contains_key("key5"), true);
}

#[test]
fn test_current_size_bytes_adjusts_correctly() {
    let mut inner = CacheInner::new(10 * 1024 * 1024);

    assert_eq!(inner.current_size_bytes, 0);

    helper_insert_entry(
        "key1",
        helper_create_custom_size_entry_data("key1", 500),
        60,
        &mut inner,
    );

    assert_eq!(inner.current_size_bytes, 500);

    helper_insert_entry(
        "key2",
        helper_create_custom_size_entry_data("key2", 33),
        60,
        &mut inner,
    );
    assert_eq!(inner.current_size_bytes, 533);

    helper_insert_entry(
        "key3",
        helper_create_custom_size_entry_data("key3", 10_000_000),
        60,
        &mut inner,
    );
    assert_eq!(inner.current_size_bytes, 10_000_533);

    helper_insert_entry(
        "key4",
        helper_create_custom_size_entry_data("key4", 122),
        60,
        &mut inner,
    );
    assert_eq!(inner.current_size_bytes, 10_000_655);

    helper_insert_entry(
        "key5",
        helper_create_custom_size_entry_data("key5", inner.max_size_bytes - (10_000_655)),
        60,
        &mut inner,
    );
    assert_eq!(inner.current_size_bytes, inner.max_size_bytes);

    inner.get("key1");
    inner.get("key2");
    inner.get("key3");
    inner.get("key5");

    helper_insert_entry(
        "key6",
        helper_create_custom_size_entry_data("key6", 61), // Half of key4 that will get removed later, just to simplify the "math" beneath
        60,
        &mut inner,
    );

    assert_eq!(inner.current_size_bytes, inner.max_size_bytes - 61);

    inner.remove("key1");
    assert_eq!(inner.current_size_bytes, inner.max_size_bytes - 561);

    inner.remove("key3");
    assert_eq!(inner.current_size_bytes, inner.max_size_bytes - 10_000_561);

    inner.evict_lfu();
    assert_eq!(inner.current_size_bytes, inner.max_size_bytes - 10_000_622);

    inner.remove("key2");
    assert_eq!(inner.current_size_bytes, inner.max_size_bytes - 10_000_655);
}

#[test]
fn test_concurrent_inserts_and_gets() {
    use std::thread;

    let cache = Arc::new(Cache::new(10, 6));
    let mut handles = vec![];

    // Spawn writers
    for i in 0..100 {
        let cache = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            cache.insert(
                format!("key{}", i),
                vec![i as u8],
                Instant::now() + Duration::from_secs(60),
            );
        }));
    }

    // Spawn readers
    for i in 0..100 {
        let cache = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            cache.get(&format!("key{}", i));
        }));
    }

    for h in handles {
        h.join().unwrap(); // no panics = no poisoned locks
    }
}
