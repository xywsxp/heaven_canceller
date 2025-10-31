use crate::config::Config;
use common::nats_client::NatsClient;
use misaka_network::MisakaNetwork;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tokio_stream::StreamExt;

pub struct SignalService {
    nats_client: NatsClient,
    network: Arc<MisakaNetwork>,
    config: Arc<Config>,
    // 统计计数器
    nats_messages_received: Arc<AtomicU64>,
    signals_sent: Arc<AtomicU64>,
    // 性能指标（累积值，单位：微秒）
    total_emit_time_us: Arc<AtomicU64>,
    total_bytes_sent: Arc<AtomicU64>,
}

impl SignalService {
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        // 连接 NATS
        let nats_client = NatsClient::new(&config.nats_url).await?;
        println!("✅ Connected to NATS: {}", config.nats_url);

        // 创建 MisakaNetwork 客户端（new 已经包含连接）
        let network = MisakaNetwork::new(&config.nats_url).await?;
        println!("✅ MisakaNetwork connected");

        // 创建 Telepath（如果不存在）
        let telepath_config = misaka_network::TelepathConfig::default();
        match network.create_telepath(&config.telepath_name, telepath_config).await {
            Ok(_) => println!("✅ Telepath '{}' created", config.telepath_name),
            Err(e) => {
                // 如果已存在，忽略错误
                if e.to_string().contains("already exists") || e.to_string().contains("name already in use") {
                    println!("ℹ️  Telepath '{}' already exists", config.telepath_name);
                } else {
                    return Err(e.into());
                }
            }
        }

        Ok(Self {
            nats_client,
            network: Arc::new(network),
            config: Arc::new(config),
            nats_messages_received: Arc::new(AtomicU64::new(0)),
            signals_sent: Arc::new(AtomicU64::new(0)),
            total_emit_time_us: Arc::new(AtomicU64::new(0)),
            total_bytes_sent: Arc::new(AtomicU64::new(0)),
        })
    }

    async fn start_statistics_task(&self) {
        let mut timer = interval(Duration::from_secs(60));
        let nats_counter = Arc::clone(&self.nats_messages_received);
        let signals_counter = Arc::clone(&self.signals_sent);
        let emit_time_counter = Arc::clone(&self.total_emit_time_us);
        let bytes_counter = Arc::clone(&self.total_bytes_sent);

        tokio::spawn(async move {
            loop {
                timer.tick().await;

                let nats_count = nats_counter.swap(0, Ordering::Relaxed);
                let signals_count = signals_counter.swap(0, Ordering::Relaxed);
                let total_emit_us = emit_time_counter.swap(0, Ordering::Relaxed);
                let total_bytes = bytes_counter.swap(0, Ordering::Relaxed);

                // 计算平均值
                let avg_emit_us = if signals_count > 0 {
                    total_emit_us / signals_count
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
                    "[Summary] {} NATS: {} | Signals: {} | Avg emit: {} us | Avg size: {} bytes | Total data: {:.2} MB",
                    timestamp,
                    nats_count,
                    signals_count,
                    avg_emit_us,
                    avg_bytes,
                    total_bytes as f64 / (1024.0 * 1024.0)
                );
            }
        });
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 SignalService V2 starting...");
        println!("📡 NATS topic: {}", self.config.topic);
        println!("🎯 Telepath: {}", self.config.telepath_name);

        // 启动统计任务
        self.start_statistics_task().await;

        let mut subscriber = self.nats_client.subscribe(&self.config.topic).await?;

        while let Some(message) = subscriber.next().await {
            // 增加 NATS 消息接收计数
            self.nats_messages_received.fetch_add(1, Ordering::Relaxed);

            // 直接获取 bytes，不需要反序列化
            let tx_bytes = message.payload.to_vec();

            // Spawn 异步任务发送
            let network = Arc::clone(&self.network);
            let config = Arc::clone(&self.config);
            let signals_counter = Arc::clone(&self.signals_sent);
            let emit_time_counter = Arc::clone(&self.total_emit_time_us);
            let bytes_counter = Arc::clone(&self.total_bytes_sent);

            tokio::spawn(async move {
                if let Err(e) = Self::send_signal(
                    network,
                    config,
                    tx_bytes,
                    signals_counter,
                    emit_time_counter,
                    bytes_counter,
                )
                .await
                {
                    eprintln!("❌ Failed to send signal: {:?}", e);
                }
            });
        }

        println!("NATS stream ended");
        Ok(())
    }

    /// 发送 Signal 到 MisakaNetwork
    async fn send_signal(
        network: Arc<MisakaNetwork>,
        config: Arc<Config>,
        tx_bytes: Vec<u8>,
        signals_counter: Arc<AtomicU64>,
        emit_time_counter: Arc<AtomicU64>,
        bytes_counter: Arc<AtomicU64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 记录字节数
        let bytes_len = tx_bytes.len() as u64;
        bytes_counter.fetch_add(bytes_len, Ordering::Relaxed);

        // 创建 MisakaSignal
        let signal = Self::create_signal(&config, tx_bytes);

        // 发送（记录时间）
        let start = std::time::Instant::now();
        network
            .emit_signal(&config.telepath_name, signal)
            .await?;
        let emit_time_us = start.elapsed().as_micros() as u64;
        emit_time_counter.fetch_add(emit_time_us, Ordering::Relaxed);

        // 增加发送成功计数
        signals_counter.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// 创建 MisakaSignal
    fn create_signal(config: &Config, binary_data: Vec<u8>) -> misaka_network::MisakaSignal {
        use prost_types::Timestamp;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();

        let authority = Self::parse_authority_level(&config.authority_level);

        misaka_network::MisakaSignal {
            timestamp: Some(Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
            uuid: uuid::Uuid::new_v4().to_string(),
            parent_uuid: String::new(),
            sender_agent: config.sender_agent.clone(),
            authority: authority as i32,
            content_type: "parsed_transaction".to_string(),
            payload: binary_data,
        }
    }

    /// 解析权限级别
    fn parse_authority_level(level: &str) -> misaka_network::misaka_signal::AuthorityLevel {
        match level.to_uppercase().as_str() {
            "LV0" => misaka_network::misaka_signal::AuthorityLevel::Lv0,
            "LV1" => misaka_network::misaka_signal::AuthorityLevel::Lv1,
            "LV2" => misaka_network::misaka_signal::AuthorityLevel::Lv2,
            "LV3" => misaka_network::misaka_signal::AuthorityLevel::Lv3,
            "LV4" => misaka_network::misaka_signal::AuthorityLevel::Lv4,
            "LV5" => misaka_network::misaka_signal::AuthorityLevel::Lv5,
            _ => misaka_network::misaka_signal::AuthorityLevel::Lv0,
        }
    }
}
