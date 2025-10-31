use anyhow::Result;
use async_nats::jetstream;
use futures::StreamExt;
use prost::Message;
use std::time::Duration;

use crate::proto::MisakaSignal;

pub struct MisakaNetwork {
    client: async_nats::Client,
    jetstream: jetstream::Context,
}

pub struct TelepathConfig {
    pub ttl: Duration,
    pub max_msgs: i64,
    pub max_bytes: i64,
}

impl Default for TelepathConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(300),
            max_msgs: 10_000,
            max_bytes: -1, // No limit
        }
    }
}

pub enum AckPolicy {
    Explicit,
    None,
    All,
}

impl MisakaNetwork {
    /// 连接到 NATS
    pub async fn new(url: &str) -> Result<Self> {
        let client = async_nats::connect(url).await?;
        let jetstream = jetstream::new(client.clone());
        Ok(Self { client, jetstream })
    }

    /// 创建 Telepath (Stream)
    pub async fn create_telepath(&self, name: &str, config: TelepathConfig) -> Result<()> {
        let stream_name = format!("telepath_{}", name);
        
        self.jetstream
            .create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![format!("{}.>", stream_name)],
                max_age: config.ttl,
                max_messages: config.max_msgs,
                max_bytes: config.max_bytes,
                storage: jetstream::stream::StorageType::Memory,
                retention: jetstream::stream::RetentionPolicy::Limits,
                discard: jetstream::stream::DiscardPolicy::Old,
                num_replicas: 1,
                ..Default::default()
            })
            .await?;
        
        Ok(())
    }

    /// 发布 Signal
    pub async fn emit_signal(&self, telepath: &str, signal: MisakaSignal) -> Result<String> {
        let stream_name = format!("telepath_{}", telepath);
        let subject = format!("{}.lv{}", stream_name, signal.authority);
        
        let data = signal.encode_to_vec();
        let ack = self.jetstream.publish(subject, data.into()).await?;
        let ack = ack.await?;
        
        Ok(ack.sequence.to_string())
    }

    /// 订阅 Telepath
    pub async fn subscribe_telepath<F>(
        &self,
        telepath: &str,
        _ack_policy: AckPolicy,
        mut handler: F,
    ) -> Result<()>
    where
        F: FnMut(MisakaSignal) -> Result<()> + Send + 'static,
    {
        let stream_name = format!("telepath_{}", telepath);
        
        let consumer = self
            .jetstream
            .create_consumer_on_stream(
                jetstream::consumer::pull::Config {
                    name: Some(format!("{}_consumer", telepath)),
                    ack_policy: jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                },
                &stream_name,
            )
            .await?;
        
        let mut messages = consumer.messages().await?;
        
        while let Some(msg) = messages.next().await {
            let msg = msg?;
            let signal = MisakaSignal::decode(&msg.payload[..])?;
            
            match handler(signal) {
                Ok(_) => {
                    if let Err(e) = msg.ack().await {
                        eprintln!("Ack error: {}", e);
                    }
                }
                Err(e) => {
                    // Handler returned error - acknowledge the message and exit
                    let _ = msg.ack().await;
                    return Err(e);
                }
            }
        }
        
        Ok(())
    }
}
