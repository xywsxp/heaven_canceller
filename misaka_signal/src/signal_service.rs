use crate::config::Config;
use crate::event_bundle::EventBundle;
use crate::grpc_client::{misaka_network::*, GrpcClient};
use common::nats_client::NatsClient;
use prost::Message;
use proto_lib::transaction::solana::Transaction;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tokio_stream::StreamExt;
use utils::convert_transaction::TransactionConverter;

pub struct SignalService {
    nats_client: NatsClient,
    grpc_client: Arc<GrpcClient>,
    config: Arc<Config>,
    // 统计计数器
    nats_messages_received: Arc<AtomicU64>,
    signals_sent: Arc<AtomicU64>,
    // 性能指标（累积值，单位：微秒）
    total_conversion_time_us: Arc<AtomicU64>,
    total_serialization_time_us: Arc<AtomicU64>,
    total_grpc_time_us: Arc<AtomicU64>,
    total_bytes_sent: Arc<AtomicU64>,
}

impl SignalService {
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        // 连接 NATS
        let nats_client = NatsClient::new(&config.nats_url).await?;
        println!("✅ Connected to NATS: {}", config.nats_url);

        // 连接 gRPC
        let grpc_client = GrpcClient::new(&config.grpc_server_url).await?;
        println!("✅ Connected to gRPC: {}", config.grpc_server_url);

        Ok(Self {
            nats_client,
            grpc_client: Arc::new(grpc_client),
            config: Arc::new(config),
            nats_messages_received: Arc::new(AtomicU64::new(0)),
            signals_sent: Arc::new(AtomicU64::new(0)),
            total_conversion_time_us: Arc::new(AtomicU64::new(0)),
            total_serialization_time_us: Arc::new(AtomicU64::new(0)),
            total_grpc_time_us: Arc::new(AtomicU64::new(0)),
            total_bytes_sent: Arc::new(AtomicU64::new(0)),
        })
    }

    async fn start_statistics_task(&self) {
        let mut timer = interval(Duration::from_secs(60));
        let nats_counter = Arc::clone(&self.nats_messages_received);
        let signals_counter = Arc::clone(&self.signals_sent);
        let conversion_time_counter = Arc::clone(&self.total_conversion_time_us);
        let serialization_time_counter = Arc::clone(&self.total_serialization_time_us);
        let grpc_time_counter = Arc::clone(&self.total_grpc_time_us);
        let bytes_counter = Arc::clone(&self.total_bytes_sent);

        tokio::spawn(async move {
            loop {
                timer.tick().await;

                let nats_count = nats_counter.swap(0, Ordering::Relaxed);
                let signals_count = signals_counter.swap(0, Ordering::Relaxed);
                let total_conversion_us = conversion_time_counter.swap(0, Ordering::Relaxed);
                let total_serialization_us = serialization_time_counter.swap(0, Ordering::Relaxed);
                let total_grpc_us = grpc_time_counter.swap(0, Ordering::Relaxed);
                let total_bytes = bytes_counter.swap(0, Ordering::Relaxed);

                // 计算平均值
                let avg_conversion_us = if nats_count > 0 {
                    total_conversion_us / nats_count
                } else {
                    0
                };

                let avg_serialization_us = if signals_count > 0 {
                    total_serialization_us / signals_count
                } else {
                    0
                };

                let avg_grpc_us = if signals_count > 0 {
                    total_grpc_us / signals_count
                } else {
                    0
                };

                let avg_bytes = if signals_count > 0 {
                    total_bytes / signals_count
                } else {
                    0
                };

                // 格式化时间
                let now = chrono::Local::now();
                let timestamp = now.format("%H:%M:00").to_string();

                println!(
                    "[Summary] {} NATS: {} | Signals: {} | Avg conv: {} us | Avg serial: {} us | Avg gRPC: {} us | Avg size: {} bytes | Total data: {:.2} MB",
                    timestamp,
                    nats_count,
                    signals_count,
                    avg_conversion_us,
                    avg_serialization_us,
                    avg_grpc_us,
                    avg_bytes,
                    total_bytes as f64 / (1024.0 * 1024.0)
                );
            }
        });
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 SignalService starting...");
        println!("📡 NATS topic: {}", self.config.topic);
        println!("🎯 Telepath: {}", self.config.telepath_name);

        // 启动统计任务
        self.start_statistics_task().await;

        let mut subscriber = self.nats_client.subscribe(&self.config.topic).await?;

        while let Some(message) = subscriber.next().await {
            // 增加 NATS 消息接收计数
            self.nats_messages_received.fetch_add(1, Ordering::Relaxed);

            // 1. 反序列化 Transaction
            let tx = Transaction::decode(message.payload.as_ref()).unwrap_or_else(|e| {
                eprintln!("❌ FATAL: Failed to decode transaction: {:?}", e);
                std::process::exit(1);
            });

            // 2. 转换为 Events (主线程快速处理，记录时间)
            let start = std::time::Instant::now();
            let event_bundle = self.convert_transaction(&tx);
            let conversion_time_us = start.elapsed().as_micros() as u64;
            self.total_conversion_time_us.fetch_add(conversion_time_us, Ordering::Relaxed);

            // 3. 跳过空事件
            if event_bundle.is_empty() {
                continue;
            }

            // 4. Spawn 异步任务发送 (不阻塞主循环)
            let grpc_client = Arc::clone(&self.grpc_client);
            let config = Arc::clone(&self.config);
            let signals_counter = Arc::clone(&self.signals_sent);
            let serialization_time_counter = Arc::clone(&self.total_serialization_time_us);
            let grpc_time_counter = Arc::clone(&self.total_grpc_time_us);
            let bytes_counter = Arc::clone(&self.total_bytes_sent);

            tokio::spawn(async move {
                if let Err(e) = Self::send_signal(
                    grpc_client,
                    config,
                    event_bundle,
                    signals_counter,
                    serialization_time_counter,
                    grpc_time_counter,
                    bytes_counter,
                ).await {
                    eprintln!("❌ FATAL: Failed to send signal: {:?}", e);
                    std::process::exit(1);
                }
            });
        }

        println!("NATS stream ended");
        Ok(())
    }

    /// 转换单个 Transaction 为 EventBundle
    fn convert_transaction(&self, tx: &Transaction) -> EventBundle {
        let mut bundle = EventBundle::default();

        TransactionConverter::convert(
            tx,
            &mut bundle.pumpfun_trade_event,
            &mut bundle.pumpfun_create_event,
            &mut bundle.pumpfun_migrate_event,
            &mut bundle.pumpfun_amm_buy_event,
            &mut bundle.pumpfun_amm_sell_event,
            &mut bundle.pumpfun_amm_create_pool_event,
            &mut bundle.pumpfun_amm_deposit_event,
            &mut bundle.pumpfun_amm_withdraw_event,
        );

        bundle
    }

    /// 发送 Signal 到 gRPC 服务
    async fn send_signal(
        grpc_client: Arc<GrpcClient>,
        config: Arc<Config>,
        event_bundle: EventBundle,
        signals_counter: Arc<AtomicU64>,
        serialization_time_counter: Arc<AtomicU64>,
        grpc_time_counter: Arc<AtomicU64>,
        bytes_counter: Arc<AtomicU64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 1. 序列化为 MessagePack（记录时间）
        // 使用 to_vec_named 以生成 map 格式（字段名作为 key），而非 compact 数组格式
        let start = std::time::Instant::now();
        let msgpack_bytes = rmp_serde::to_vec_named(&event_bundle).unwrap_or_else(|e| {
            eprintln!("❌ FATAL: Failed to serialize EventBundle: {:?}", e);
            std::process::exit(1);
        });
        let serialization_time_us = start.elapsed().as_micros() as u64;
        serialization_time_counter.fetch_add(serialization_time_us, Ordering::Relaxed);

        // 记录字节数
        let bytes_len = msgpack_bytes.len() as u64;
        bytes_counter.fetch_add(bytes_len, Ordering::Relaxed);

        // 2. 创建 MisakaSignal
        let signal = Self::create_signal(&config, msgpack_bytes);

        // 3. 发送 gRPC（记录时间）
        let start = std::time::Instant::now();
        grpc_client
            .emit_signal(&config.telepath_name, signal)
            .await?;
        let grpc_time_us = start.elapsed().as_micros() as u64;
        grpc_time_counter.fetch_add(grpc_time_us, Ordering::Relaxed);

        // 增加发送成功计数
        signals_counter.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// 创建 MisakaSignal
    fn create_signal(config: &Config, binary_data: Vec<u8>) -> MisakaSignal {
        use prost_types::Timestamp;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();

        let authority = Self::parse_authority_level(&config.authority_level);

        MisakaSignal {
            signal_type: "bytes".to_string(),
            timestamp: Some(Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
            uuid: uuid::Uuid::new_v4().to_string(),
            parent_uuid: String::new(),
            sender_agent: config.sender_agent.clone(),
            authority: authority as i32,
            content: Some(misaka_signal::Content::BinaryData(binary_data)),
        }
    }

    /// 解析权限级别
    fn parse_authority_level(level: &str) -> misaka_signal::AuthorityLevel {
        match level.to_uppercase().as_str() {
            "LV0" => misaka_signal::AuthorityLevel::Lv0,
            "LV1" => misaka_signal::AuthorityLevel::Lv1,
            "LV2" => misaka_signal::AuthorityLevel::Lv2,
            "LV3" => misaka_signal::AuthorityLevel::Lv3,
            "LV4" => misaka_signal::AuthorityLevel::Lv4,
            "LV5" => misaka_signal::AuthorityLevel::Lv5,
            _ => misaka_signal::AuthorityLevel::Lv0,
        }
    }
}
