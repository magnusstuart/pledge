use std::hash::{DefaultHasher, Hash, Hasher};

pub fn cache_key(query: &str, params: &[serde_json::Value]) -> String {
    let mut hasher = DefaultHasher::new();
    query.hash(&mut hasher);
    params.hash(&mut hasher);
    hasher.finish().to_string()
}

pub fn cache_key_wire(query: &str, params: &Vec<Option<Vec<u8>>>) -> String {
    let mut hasher = DefaultHasher::new();
    query.hash(&mut hasher);
    params.hash(&mut hasher);
    hasher.finish().to_string()
}
