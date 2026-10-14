use std::fs;

use crate::cache::QueryTemplate;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub queries: Vec<QueryTemplate>,
    pub cache: CacheConfig,
    pub server: ServerConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    pub host: String,
    pub port: String,
    pub user: String,
    pub password: String,
    pub database: String,
}

pub struct CacheSettings {
    pub global_ttl: u64,
    pub cache_size: u64,
    pub cache_shards: usize,
}

#[derive(Debug, Deserialize)]
pub struct CacheConfig {
    pub global_ttl: Option<u64>,
    pub max_size_mib: Option<u64>,
    pub cache_shards: Option<u8>,
}
impl CacheConfig {
    pub fn get_cache_settings(&self) -> CacheSettings {
        let global_ttl = match self.global_ttl {
            Some(ttl) => ttl,
            None => 60, // Default to 60 second TTL
        };
        let cache_size = match self.max_size_mib {
            Some(size) => size,
            None => 100, // Default to 100MiB cache size
        };

        let cache_shards = match self.cache_shards {
            Some(shards) => shards as usize,
            None => 16 as usize,
        };
        CacheSettings {
            global_ttl,
            cache_size,
            cache_shards,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    pub https_port: Option<u16>,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
}

pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string("pledge.toml")?;
    let config: Config = toml::from_str(&contents)?;
    Ok(config)
}
