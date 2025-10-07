use std::error::Error;
use std::path::Path;
use utils::clickhouse_client::ClickHouseClient;
use utils::clickhouse_events::*;

use crate::parquet_helper::ParquetHelper;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// 宏：根据事件类型反序列化并批量插入 ClickHouse
macro_rules! deserialize_and_insert {
    ($batch:expr, $event_type:expr, $table:expr, $client:expr, $( $variant:literal => $type:ty ),* $(,)?) => {
        match $event_type {
            $(
                $variant => {
                    // 使用 utils 提供的转换函数
                    let events: Vec<$type> = arrow_batch_to_vec(&$batch);
                    let row_count = events.len() as u64;
                    
                    // 批量插入
                    let mut insert = $client.insert($table)?;
                    for event in events {
                        insert.write(&event).await?;
                    }
                    insert.end().await?;
                    
                    Ok(row_count)
                }
            )*
            _ => Err(format!("Unknown event type: {}", $event_type).into()),
        }
    };
}

/// ClickHouse 导入器
pub struct ClickHouseImporter {
    parquet_helper: ParquetHelper,
}

impl ClickHouseImporter {
    pub fn new() -> Self {
        Self {
            parquet_helper: ParquetHelper::new(),
        }
    }

    /// 导入 Parquet 文件到 ClickHouse 表
    /// 
    /// # Arguments
    /// * `file_path` - Parquet 文件路径
    /// * `target_table` - 目标表名
    /// * `event_type` - 事件类型（用于反序列化）
    /// 
    /// # Returns
    /// * `u64` - 导入的行数
    pub async fn import_parquet(
        &self,
        file_path: &Path,
        target_table: &str,
        event_type: &str,
    ) -> Result<u64> {
        // 1. 读取 Parquet 文件
        let batch = self.parquet_helper.read_parquet(file_path).await?;
        
        // 2. 获取 ClickHouse 客户端
        let client = ClickHouseClient::instance().client();
        
        // 3. 根据事件类型反序列化并插入
        deserialize_and_insert!(
            batch,
            event_type,
            target_table,
            client,
            "PumpfunTradeEventV2" => PumpfunTradeEventV2,
            "PumpfunCreateEventV2" => PumpfunCreateEventV2,
            "PumpfunMigrateEventV2" => PumpfunMigrateEventV2,
            "PumpfunAmmCreatePoolEventV2" => PumpfunAmmCreatePoolEventV2,
            "PumpfunAmmDepositEventV2" => PumpfunAmmDepositEventV2,
            "PumpfunAmmWithdrawEventV2" => PumpfunAmmWithdrawEventV2,
            "PumpfunAmmBuyEventV2" => PumpfunAmmBuyEventV2,
            "PumpfunAmmSellEventV2" => PumpfunAmmSellEventV2,
        )
    }
}

impl Default for ClickHouseImporter {
    fn default() -> Self {
        Self::new()
    }
}
