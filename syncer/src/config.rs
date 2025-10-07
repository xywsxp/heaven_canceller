use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// 本地模式配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalConfig {
    /// 要导出的表列表
    pub tables: Vec<String>,
    
    /// 表名 -> ClickhouseEvent 类型名的映射（用于序列化/反序列化）
    pub table_event_mappings: HashMap<String, String>,
    
    /// 导出起始时间（chrono 自动处理 "2025-10-01" 格式）
    pub start_time: NaiveDate,
    
    /// 本地存储路径
    pub local_storage_path: PathBuf,
    
    /// 远程服务器配置
    pub remote_server: RemoteServerConfig,
}

/// 远程模式配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    /// 远程存储路径
    pub remote_storage_path: PathBuf,
    
    /// 源表文件夹 -> 目标表名映射
    pub import_mappings: HashMap<String, String>,
    
    /// 表名 -> 事件类型映射（用于反序列化）
    pub table_event_mappings: HashMap<String, String>,
}

/// 远程服务器配置（用于 rsync/SSH）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteServerConfig {
    pub address: String,
    pub port: u16,
    pub username: String,
    pub private_key_path: PathBuf,
    pub remote_path: PathBuf,
}

impl LocalConfig {
    /// 从 TOML 文件加载本地配置
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    }
}

impl RemoteConfig {
    /// 从 TOML 文件加载远程配置
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    }
}
