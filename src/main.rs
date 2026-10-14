use std::{sync::Arc, time::Duration};

mod cache;
mod config;
mod database;
mod handlers;
mod metrics;
mod server;
mod wire;
pub use cache::matcher::QueryMatcher;
pub use server::state::AppState;

use crate::{
    cache::lfu::Cache,
    metrics::{CACHE_MEMORY_BYTES, CACHE_SIZE},
};

#[tokio::main]
async fn main() {
    let config = config::load_config().expect("Failed to load config");
    let matcher = Arc::new(QueryMatcher::new(&config));

    let cache_config = config.cache.get_cache_settings();

    let cache = Arc::new(Cache::new(
        cache_config.cache_size,
        cache_config.cache_shards,
    ));

    println!("Cache initialized: {} MiB", cache_config.cache_size);
    {
        let sysinfo = sysinfo::System::new_all();
        let total_ram = sysinfo.total_memory();
        if (cache_config.cache_size as f64) > (total_ram as f64 * 0.8) {
            // Using 80% of system RAM
            eprintln!(
                "WARNING: Cache size {}MiB is close to total system RAM {}MiB",
                cache_config.cache_size / (1_024 * 1_024),
                total_ram / (1_024 * 1_024)
            );
            eprintln!("Consider reducing cache size or increasing system RAM");
        }
    }

    // Register metrics
    metrics::register_metrics();
    let cache_clone = cache.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            CACHE_SIZE.set(cache_clone.entry_count() as f64);
            CACHE_MEMORY_BYTES.set(cache_clone.cache_size_bytes() as f64);
        }
    });

    let state = AppState {
        matcher,
        cache,
        global_ttl: cache_config.global_ttl,
        database_config: config.database,
    };

    server::run_server(&config.server, state).await;
}
