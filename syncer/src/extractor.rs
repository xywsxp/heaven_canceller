use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::error::Error;
use utils::clickhouse_client::ClickHouseClient;
use utils::clickhouse_events::*;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// 宏：简化事件类型的查询和转换逻辑
macro_rules! query_and_convert {
    ($self:ident, $query:expr, $event_type:expr, $($type_name:literal => $struct_type:ty),+ $(,)?) => {
        match $event_type {
            $(
                $type_name => {
                    let rows = $self
                        .client
                        .client()
                        .query($query)
                        .fetch_all::<$struct_type>()
                        .await?;
                    vec_to_arrow_batch(&rows)
                }
            )+
            _ => {
                return Err(format!("Unknown event type: {}", $event_type).into());
            }
        }
    };
}

/// ClickHouse 数据提取器
pub struct ClickHouseExtractor {
    client: &'static ClickHouseClient,
}

impl ClickHouseExtractor {
    pub fn new() -> Self {
        Self {
            client: ClickHouseClient::instance(),
        }
    }

    /// 提取单天的事件数据
    /// 
    /// # Arguments
    /// * `table` - ClickHouse 表名
    /// * `event_type` - 事件类型名（用于反序列化）
    /// * `date` - 目标日期
    /// 
    /// # Returns
    /// * `RecordBatch` - Arrow 格式的数据批次
    pub async fn extract_daily_events(
        &self,
        table: &str,
        event_type: &str,
        date: NaiveDate,
    ) -> Result<RecordBatch> {
        // 计算起始和结束时间戳（UTC）
        let start_timestamp = date
            .and_hms_opt(0, 0, 0)
            .ok_or("Invalid date")?
            .and_utc()
            .timestamp() as u32;
        let end_timestamp = start_timestamp + 86400; // +24小时

        // 构造 SQL 查询
        let query = format!(
            "SELECT * FROM {} WHERE timestamp >= {} AND timestamp < {} ORDER BY slot, transaction_index, instruction_index",
            table, start_timestamp, end_timestamp
        );

        // 使用宏处理所有事件类型
        let batch = query_and_convert!(
            self,
            &query,
            event_type,
            "PumpfunTradeEventV2" => PumpfunTradeEventV2,
            "PumpfunCreateEventV2" => PumpfunCreateEventV2,
            "PumpfunMigrateEventV2" => PumpfunMigrateEventV2,
            "PumpfunAmmBuyEventV2" => PumpfunAmmBuyEventV2,
            "PumpfunAmmSellEventV2" => PumpfunAmmSellEventV2,
            "PumpfunAmmCreatePoolEventV2" => PumpfunAmmCreatePoolEventV2,
            "PumpfunAmmDepositEventV2" => PumpfunAmmDepositEventV2,
            "PumpfunAmmWithdrawEventV2" => PumpfunAmmWithdrawEventV2,
        );

        Ok(batch)
    }
}

impl Default for ClickHouseExtractor {
    fn default() -> Self {
        Self::new()
    }
}
