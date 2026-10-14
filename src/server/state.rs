use std::sync::Arc;

use crate::{QueryMatcher, cache::lfu::Cache, config::DatabaseConfig};

#[derive(Clone)]
pub struct AppState {
    pub database_config: DatabaseConfig,
    pub matcher: Arc<QueryMatcher>,
    pub cache: Arc<Cache>,
    pub global_ttl: u64,
}
