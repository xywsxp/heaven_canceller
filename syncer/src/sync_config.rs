use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// 同步检查器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// 本地 ClickHouse URL
    pub local_url: String,
    
    /// 本地数据库名
    pub local_database: String,
    
    /// 本地用户名
    pub local_user: String,
    
    /// 本地密码
    pub local_password: String,
    
    /// 远程 ClickHouse URL
    pub remote_url: String,
    
    /// 远程数据库名
    pub remote_database: String,
    
    /// 远程用户名
    pub remote_user: String,
    
    /// 远程密码
    pub remote_password: String,
    
    /// 表映射：本地表名 -> 远程表名
    pub table_mappings: HashMap<String, String>,
    
    /// 检查天数（默认 7 天）
    #[serde(default = "default_check_days")]
    pub check_days: u32,
    
    /// 本地延迟小时数（默认 2 小时）
    #[serde(default = "default_lag_hours")]
    pub lag_hours: u32,
}

fn default_check_days() -> u32 {
    7
}

fn default_lag_hours() -> u32 {
    2
}

impl SyncConfig {
    /// 从 TOML 文件加载配置
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    }
}
