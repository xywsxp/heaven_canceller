use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub nats_url: String,
    pub topic: String,
    pub grpc_server_url: String,
    pub telepath_name: String,
    pub sender_agent: String,
    pub authority_level: String,
}

impl Config {
    pub fn from_toml_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}
