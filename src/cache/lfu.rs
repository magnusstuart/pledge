use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

pub struct Cache {
    inner: Arc<Vec<RwLock<CacheInner>>>,
    shards: usize,
}

impl Cache {
    pub fn new(max_cache_size_mib: u64, cache_shards: usize) -> Self {
        let max_shard_size = (max_cache_size_mib * 1024 * 1024) / (cache_shards as u64);
        let inner = Arc::new(
            (0..cache_shards)
                .map(|_| RwLock::new(CacheInner::new(max_shard_size)))
                .collect::<Vec<_>>(),
        );

        Cache {
            inner,
            shards: cache_shards,
        }
    }

    pub fn get(&self, key: &str) -> Option<Arc<CachedResponse>> {
        let shard = self.get_cache_shard(key);
        let (data, should_remove) = shard.read().unwrap().get(key);
        if should_remove {
            shard.write().unwrap().remove_if_expired(key);
        }
        data
    }

    pub fn remove(&self, key: &str) {
        let shard = self.get_cache_shard(key);
        let mut inner = shard.write().unwrap();
        inner.remove(key);
    }

    pub fn insert(&self, key: String, data: CachedResponse, expires_at: Instant) {
        let arc_data = Arc::new(data);
        let shard = self.get_cache_shard(&key);
        let mut inner = shard.write().unwrap();
        inner.insert(key, arc_data, expires_at);
    }

    pub fn entry_count(&self) -> u64 {
        self.inner
            .iter()
            .map(|inner| inner.read().unwrap().entries.len() as u64)
            .sum()
    }
    pub fn cache_size_bytes(&self) -> u64 {
        self.inner
            .iter()
            .map(|inner| inner.read().unwrap().current_size_bytes)
            .sum()
    }

    fn get_cache_shard(&self, key: &str) -> &RwLock<CacheInner> {
        let cache_shard_key = self.get_cache_shard_key(key);
        &self.inner[cache_shard_key]
    }

    fn get_cache_shard_key(&self, key: impl Hash) -> usize {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish() as usize % self.shards
    }
}

pub(super) struct CacheInner {
    pub(super) entries: HashMap<String, Entry>,
    pub(super) current_size_bytes: u64,
    pub(super) max_size_bytes: u64,
}

impl CacheInner {
    pub(super) fn new(max_size_bytes: u64) -> Self {
        CacheInner {
            entries: HashMap::new(),
            current_size_bytes: 0,
            max_size_bytes,
        }
    }

    /// Returns 2 values in a tuple
    ///
    /// The first value is the data, the second value is a boolean indicating if the entry should be removed
    pub(super) fn get(&self, key: &str) -> (Option<Arc<CachedResponse>>, bool) {
        let entry = match self.entries.get(key) {
            Some(entry) => entry,
            None => return (None, false),
        };
        if entry.expires_at < Instant::now() {
            return (None, true);
        }
        entry.counter.fetch_add(1, Ordering::Relaxed);
        let data = entry.data.clone();
        (Some(data), false)
    }

    pub(super) fn remove_if_expired(&mut self, key: &str) {
        if let Some(entry) = self.entries.get(key) {
            if entry.expires_at >= Instant::now() {
                return; // no longer expired
            }
        }
        self.remove(key);
    }

    pub(super) fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.current_size_bytes -= entry.get_size(key);
        }
    }

    pub(super) fn insert(&mut self, key: String, data: Arc<CachedResponse>, expires_at: Instant) {
        let entry = Entry {
            data,
            expires_at,
            counter: AtomicU64::new(1),
        };
        let entry_size = entry.get_size(&key);
        if entry_size > self.max_size_bytes {
            return;
        } // too big, drop it
        self.remove(&key);
        while self.current_size_bytes + entry_size > self.max_size_bytes {
            self.evict_lfu();
        }
        self.entries.insert(key, entry);
        self.current_size_bytes += entry_size;
    }

    pub(super) fn evict_lfu(&mut self) {
        if let Some((key, _)) = self
            .entries
            .iter()
            .min_by_key(|(_, e)| e.counter.load(Ordering::Relaxed))
        {
            let key = key.clone();
            self.remove(&key);
        }
    }
}

pub(super) struct Entry {
    pub(super) data: Arc<CachedResponse>,
    pub(super) expires_at: Instant,
    pub(super) counter: AtomicU64,
}

impl Entry {
    pub(super) fn get_size(&self, key: &str) -> u64 {
        let data_length: u64 = {
            let row_desc_len = self.data.row_desc.as_ref().map_or(0, Vec::len);
            let param_desc_len = self.data.param_desc.as_ref().map_or(0, Vec::len);
            self.data.data.len() as u64 + row_desc_len as u64 + param_desc_len as u64
        };
        data_length
            + key.len() as u64
            + std::mem::size_of_val(&self.counter) as u64
            + std::mem::size_of_val(&self.expires_at) as u64
    }
}

#[derive(Debug, PartialEq)]
pub struct CachedResponse {
    pub(crate) param_desc: Option<Vec<u8>>,
    pub(crate) row_desc: Option<Vec<u8>>,
    pub(crate) data: Vec<u8>,
}

impl CachedResponse {
    pub fn has_data(&self) -> bool {
        !self.data.is_empty()
    }
    pub fn has_row_desc(&self) -> bool {
        self.row_desc.is_some()
    }
    pub fn has_param_desc(&self) -> bool {
        self.param_desc.is_some()
    }
}
