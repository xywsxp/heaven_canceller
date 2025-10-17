use super::transaction_processor::TransactionProcessor;
use common::nats_client::NatsClient;
use prost::Message;
use proto_lib::transaction::solana::Transaction;
use std::sync::Arc;
use tokio_stream::StreamExt;
use toml;

/// TransactionSubscriber服务 - 从NATS订阅交易数据并处理
pub struct TransactionSubscriberService {
    nats_client: NatsClient,
    processor: Arc<TransactionProcessor>,
    topic: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub nats_url: String,
    pub topic: String,
    pub max_concurrent_clickhouse_tasks: usize,
    pub table_names: TableNames,
}

#[derive(Debug, Clone)]
pub struct TableNames {
    pub pumpfun_trade_event: String,
    pub pumpfun_create_event: String,
    pub pumpfun_migrate_event: String,
    pub pumpfun_amm_buy_event: String,
    pub pumpfun_amm_sell_event: String,
    pub pumpfun_amm_create_pool_event: String,
    pub pumpfun_amm_deposit_event: String,
    pub pumpfun_amm_withdraw_event: String,
}

impl Config {
    /// 从TOML文件加载配置
    pub fn from_toml_file(config_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_content = std::fs::read_to_string(config_path)?;
        let toml_value: toml::Value = toml::from_str(&config_content)?;
        Self::from_toml_value(&toml_value)
    }

    /// 从TOML值加载配置
    pub fn from_toml_value(toml_value: &toml::Value) -> Result<Self, Box<dyn std::error::Error>> {
        // 解析表名映射
        let tables = toml_value
            .get("tables")
            .ok_or("Missing 'tables' section in config")?;

        let table_names = TableNames {
            pumpfun_trade_event: tables
                .get("pumpfun_trade_event")
                .and_then(|v| v.as_str())
                .unwrap_or("pumpfun_trade_event_v2")
                .to_string(),
            pumpfun_create_event: tables
                .get("pumpfun_create_event")
                .and_then(|v| v.as_str())
                .unwrap_or("pumpfun_create_event_v2")
                .to_string(),
            pumpfun_migrate_event: tables
                .get("pumpfun_migrate_event")
                .and_then(|v| v.as_str())
                .unwrap_or("pumpfun_migrate_event_v2")
                .to_string(),
            pumpfun_amm_buy_event: tables
                .get("pumpfun_amm_buy_event")
                .and_then(|v| v.as_str())
                .unwrap_or("pumpfun_amm_buy_event_v2")
                .to_string(),
            pumpfun_amm_sell_event: tables
                .get("pumpfun_amm_sell_event")
                .and_then(|v| v.as_str())
                .unwrap_or("pumpfun_amm_sell_event_v2")
                .to_string(),
            pumpfun_amm_create_pool_event: tables
                .get("pumpfun_amm_create_pool_event")
                .and_then(|v| v.as_str())
                .unwrap_or("pumpfun_amm_create_pool_event_v2")
                .to_string(),
            pumpfun_amm_deposit_event: tables
                .get("pumpfun_amm_deposit_event")
                .and_then(|v| v.as_str())
                .unwrap_or("pumpfun_amm_deposit_event_v2")
                .to_string(),
            pumpfun_amm_withdraw_event: tables
                .get("pumpfun_amm_withdraw_event")
                .and_then(|v| v.as_str())
                .unwrap_or("pumpfun_amm_withdraw_event_v2")
                .to_string(),
        };

        let config = Config {
            nats_url: toml_value
                .get("nats_url")
                .and_then(|v| v.as_str())
                .ok_or("Missing 'nats_url' in config")?
                .to_string(),
            topic: toml_value
                .get("topic")
                .and_then(|v| v.as_str())
                .ok_or("Missing 'topic' in config")?
                .to_string(),
            max_concurrent_clickhouse_tasks: toml_value
                .get("max_concurrent_clickhouse_tasks")
                .and_then(|v| v.as_integer())
                .unwrap_or(10) as usize,
            table_names,
        };

        Ok(config)
    }
}

impl TransactionSubscriberService {
    /// 创建新的TransactionSubscriber服务
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        // 连接NATS
        let nats_client = NatsClient::new(&config.nats_url).await?;

        // 创建处理器，传入表名配置
        let processor = Arc::new(TransactionProcessor::new(
            config.max_concurrent_clickhouse_tasks,
            config.table_names.clone(),
        ));

        Ok(Self {
            nats_client,
            processor,
            topic: config.topic,
        })
    }

    /// 主运行循环 - 订阅NATS并处理交易
    /// 架构：
    /// - 主循环：从NATS接收消息并快速反序列化
    /// - process_transaction：快速解析并通过channel发送到批处理任务
    /// - 独立批处理任务：累积事件，100ms或100条触发刷新到ClickHouse
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        println!("TransactionSubscriberService starting...");
        println!("NATS topic: {}", self.topic);

        // 订阅NATS主题
        let mut subscriber = self.nats_client.subscribe(&self.topic).await?;

        // 主循环：持续接收NATS消息
        while let Some(message) = subscriber.next().await {
            let payload_size = message.payload.len();
            // 反序列化protobuf消息（失败时打印堆栈并退出进程）
            let parsed_tx = Self::deserialize_transaction(&message.payload);
            // 直接处理（process_transaction 内部会通过 channel 异步发送）
            self.processor.process_transaction(parsed_tx, payload_size);
        }

        println!("NATS stream ended");
        Ok(())
    }

    /// 反序列化SubscribeUpdateTransaction (使用prost protobuf)
    /// 失败时打印堆栈并退出进程
    fn deserialize_transaction(payload: &[u8]) -> Transaction {
        
        Transaction::decode(payload).unwrap_or_else(|e| {
            eprintln!("❌ FATAL: Failed to deserialize transaction: {:?}", e);
            eprintln!("Payload length: {} bytes", payload.len());
            std::process::exit(1);
        })
    }

    /// 优雅关闭：等待所有任务完成
    pub async fn shutdown(self) {
        println!("Shutting down TransactionSubscriberService...");
        self.processor.wait_all_tasks().await;
        println!("All tasks completed");
    }
}
