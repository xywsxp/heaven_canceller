use super::transaction_subscriber_service::TableNames;
use common::async_pool::AsyncPool;
use proto_lib::transaction::solana::Transaction;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use utils::clickhouse_client::ClickHouseClient;
use utils::clickhouse_events;
use utils::convert_transaction::TransactionConverter;

const BATCH_SIZE: usize = 100;
const FLUSH_INTERVAL_MS: u64 = 100;

pub struct TransactionProcessor {
    event_sender: mpsc::UnboundedSender<ProcessedEvents>,
    async_pool: Arc<AsyncPool>,
    stats_sender: mpsc::UnboundedSender<ProcessingStats>,
}

#[derive(Default)]
struct ProcessedEvents {
    pumpfun_trade_event: Vec<clickhouse_events::PumpfunTradeEventV2>,
    pumpfun_create_event: Vec<clickhouse_events::PumpfunCreateEventV2>,
    pumpfun_migrate_event: Vec<clickhouse_events::PumpfunMigrateEventV2>,
    pumpfun_amm_buy_event: Vec<clickhouse_events::PumpfunAmmBuyEventV2>,
    pumpfun_amm_sell_event: Vec<clickhouse_events::PumpfunAmmSellEventV2>,
    pumpfun_amm_create_pool_event: Vec<clickhouse_events::PumpfunAmmCreatePoolEventV2>,
    pumpfun_amm_deposit_event: Vec<clickhouse_events::PumpfunAmmDepositEventV2>,
    pumpfun_amm_withdraw_event: Vec<clickhouse_events::PumpfunAmmWithdrawEventV2>,
}

/// 处理统计信息
#[derive(Clone)]
struct ProcessingStats {
    payload_size: usize,
    processing_time_micros: u64,
}

impl ProcessedEvents {
    fn is_empty(&self) -> bool {
        self.pumpfun_trade_event.is_empty()
            && self.pumpfun_create_event.is_empty()
            && self.pumpfun_migrate_event.is_empty()
            && self.pumpfun_amm_buy_event.is_empty()
            && self.pumpfun_amm_sell_event.is_empty()
            && self.pumpfun_amm_create_pool_event.is_empty()
            && self.pumpfun_amm_deposit_event.is_empty()
            && self.pumpfun_amm_withdraw_event.is_empty()
    }
}

#[derive(Default)]
struct BatchAccumulator {
    pumpfun_trade_event: Vec<clickhouse_events::PumpfunTradeEventV2>,
    pumpfun_create_event: Vec<clickhouse_events::PumpfunCreateEventV2>,
    pumpfun_migrate_event: Vec<clickhouse_events::PumpfunMigrateEventV2>,
    pumpfun_amm_buy_event: Vec<clickhouse_events::PumpfunAmmBuyEventV2>,
    pumpfun_amm_sell_event: Vec<clickhouse_events::PumpfunAmmSellEventV2>,
    pumpfun_amm_create_pool_event: Vec<clickhouse_events::PumpfunAmmCreatePoolEventV2>,
    pumpfun_amm_deposit_event: Vec<clickhouse_events::PumpfunAmmDepositEventV2>,
    pumpfun_amm_withdraw_event: Vec<clickhouse_events::PumpfunAmmWithdrawEventV2>,
}

impl BatchAccumulator {
    fn add(&mut self, events: ProcessedEvents) {
        self.pumpfun_trade_event.extend(events.pumpfun_trade_event);
        self.pumpfun_create_event
            .extend(events.pumpfun_create_event);
        self.pumpfun_migrate_event
            .extend(events.pumpfun_migrate_event);
        self.pumpfun_amm_buy_event
            .extend(events.pumpfun_amm_buy_event);
        self.pumpfun_amm_sell_event
            .extend(events.pumpfun_amm_sell_event);
        self.pumpfun_amm_create_pool_event
            .extend(events.pumpfun_amm_create_pool_event);
        self.pumpfun_amm_deposit_event
            .extend(events.pumpfun_amm_deposit_event);
        self.pumpfun_amm_withdraw_event
            .extend(events.pumpfun_amm_withdraw_event);
    }

    fn should_flush(&self) -> bool {
        self.pumpfun_trade_event.len() >= BATCH_SIZE
            || self.pumpfun_create_event.len() >= BATCH_SIZE
            || self.pumpfun_migrate_event.len() >= BATCH_SIZE
            || self.pumpfun_amm_buy_event.len() >= BATCH_SIZE
            || self.pumpfun_amm_sell_event.len() >= BATCH_SIZE
            || self.pumpfun_amm_create_pool_event.len() >= BATCH_SIZE
            || self.pumpfun_amm_deposit_event.len() >= BATCH_SIZE
            || self.pumpfun_amm_withdraw_event.len() >= BATCH_SIZE
    }

    fn is_empty(&self) -> bool {
        self.pumpfun_trade_event.is_empty()
            && self.pumpfun_create_event.is_empty()
            && self.pumpfun_migrate_event.is_empty()
            && self.pumpfun_amm_buy_event.is_empty()
            && self.pumpfun_amm_sell_event.is_empty()
            && self.pumpfun_amm_create_pool_event.is_empty()
            && self.pumpfun_amm_deposit_event.is_empty()
            && self.pumpfun_amm_withdraw_event.is_empty()
    }

    fn take(&mut self) -> ProcessedEvents {
        ProcessedEvents {
            pumpfun_trade_event: std::mem::take(&mut self.pumpfun_trade_event),
            pumpfun_create_event: std::mem::take(&mut self.pumpfun_create_event),
            pumpfun_migrate_event: std::mem::take(&mut self.pumpfun_migrate_event),
            pumpfun_amm_buy_event: std::mem::take(&mut self.pumpfun_amm_buy_event),
            pumpfun_amm_sell_event: std::mem::take(&mut self.pumpfun_amm_sell_event),
            pumpfun_amm_create_pool_event: std::mem::take(&mut self.pumpfun_amm_create_pool_event),
            pumpfun_amm_deposit_event: std::mem::take(&mut self.pumpfun_amm_deposit_event),
            pumpfun_amm_withdraw_event: std::mem::take(&mut self.pumpfun_amm_withdraw_event),
        }
    }
}

impl TransactionProcessor {
    pub fn new(max_concurrent_clickhouse_tasks: usize, table_names: TableNames) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (stats_tx, stats_rx) = mpsc::unbounded_channel();

        let async_pool = Arc::new(AsyncPool::new(max_concurrent_clickhouse_tasks));
        let pool_clone = Arc::clone(&async_pool);
        tokio::spawn(async move {
            Self::batch_flusher_task(rx, stats_rx, pool_clone, table_names).await;
        });

        Self {
            event_sender: tx,
            async_pool,
            stats_sender: stats_tx,
        }
    }

    pub fn process_transaction(&self, parsed_tx: Transaction, payload_size: usize) {
        let start = std::time::Instant::now();
        let mut events = ProcessedEvents::default();
        
        TransactionConverter::convert(
            &parsed_tx,
            &mut events.pumpfun_trade_event,
            &mut events.pumpfun_create_event,
            &mut events.pumpfun_migrate_event,
            &mut events.pumpfun_amm_buy_event,
            &mut events.pumpfun_amm_sell_event,
            &mut events.pumpfun_amm_create_pool_event,
            &mut events.pumpfun_amm_deposit_event,
            &mut events.pumpfun_amm_withdraw_event,
        );

        let processing_time = start.elapsed().as_micros() as u64;
        
        // 发送统计信息（即使没有事件也要统计）
        let _ = self.stats_sender.send(ProcessingStats {
            payload_size,
            processing_time_micros: processing_time,
        });

        if !events.is_empty() {
            let _ = self.event_sender.send(events);
        }
    }

    async fn batch_flusher_task(
        mut receiver: mpsc::UnboundedReceiver<ProcessedEvents>,
        mut stats_receiver: mpsc::UnboundedReceiver<ProcessingStats>,
        async_pool: Arc<AsyncPool>,
        table_names: TableNames,
    ) {
        let mut batches = BatchAccumulator::default();
        let mut interval = tokio::time::interval(Duration::from_millis(FLUSH_INTERVAL_MS));

        // 周期内的增量统计
        let mut period_transactions = 0usize;
        let mut period_events = 0usize;
        let mut period_rows_flushed = 0usize;
        let mut period_bytes_received = 0usize;
        let mut period_processing_time_micros = 0u64;
        
        let start_time = std::time::Instant::now();
        let mut last_summary_time = std::time::Instant::now();

        // 根据编译模式决定汇总间隔：debug 10秒，release 60秒
        #[cfg(debug_assertions)]
        const SUMMARY_INTERVAL_SECS: u64 = 10;
        #[cfg(not(debug_assertions))]
        const SUMMARY_INTERVAL_SECS: u64 = 60;

        loop {
            tokio::select! {
                Some(stats) = stats_receiver.recv() => {
                    period_transactions += 1;
                    period_bytes_received += stats.payload_size;
                    period_processing_time_micros += stats.processing_time_micros;
                }
                Some(events) = receiver.recv() => {
                    period_events += 1;
                    batches.add(events);
                    if batches.should_flush() {
                        let rows = Self::flush_batches(&mut batches, &async_pool, &table_names);
                        period_rows_flushed += rows;
                    }
                }
                _ = interval.tick() => {
                    if !batches.is_empty() {
                        let rows = Self::flush_batches(&mut batches, &async_pool, &table_names);
                        period_rows_flushed += rows;
                    }
                    
                    // 定期打印汇总信息
                    if last_summary_time.elapsed().as_secs() >= SUMMARY_INTERVAL_SECS {
                        let period_duration = last_summary_time.elapsed().as_secs_f64();
                        let total_uptime = start_time.elapsed().as_secs_f64();
                        let avg_processing_time = if period_transactions > 0 {
                            period_processing_time_micros as f64 / period_transactions as f64
                        } else {
                            0.0
                        };
                        
                        println!("📈 [{}s] TX: {} ({:.0}/s) | Events: {} | Rows: {} | Data: {:.2}MB ({:.2}MB/s) | Avg processing: {:.1}μs | Uptime: {:.1}min",
                            SUMMARY_INTERVAL_SECS,
                            period_transactions,
                            period_transactions as f64 / period_duration,
                            period_events,
                            period_rows_flushed,
                            period_bytes_received as f64 / (1024.0 * 1024.0),
                            (period_bytes_received as f64 / (1024.0 * 1024.0)) / period_duration,
                            avg_processing_time,
                            total_uptime / 60.0
                        );
                        
                        // 重置周期统计
                        period_transactions = 0;
                        period_events = 0;
                        period_rows_flushed = 0;
                        period_bytes_received = 0;
                        period_processing_time_micros = 0;
                        last_summary_time = std::time::Instant::now();
                    }
                }
            }
        }
    }

    fn flush_batches(
        batches: &mut BatchAccumulator,
        async_pool: &Arc<AsyncPool>,
        table_names: &TableNames,
    ) -> usize {
        let data = batches.take();
        let mut total_rows = 0usize;

        macro_rules! submit_insert {
            ($rows:expr, $table_field:ident) => {
                if !$rows.is_empty() {
                    let row_count = $rows.len();
                    total_rows += row_count;
                    let table_name = table_names.$table_field.clone();
                    
                    // Debug模式下打印详细信息
                    #[cfg(debug_assertions)]
                    println!("📊 Flushing {} rows to table: {}", row_count, table_name);

                    let rows = $rows;
                    async_pool.submit(move || async move {
                        let client = ClickHouseClient::instance().client();
                    
                        let mut insert = match client.insert(&table_name) {
                            Ok(insert) => insert,
                            Err(e) => {
                                eprintln!(
                                    "❌ FATAL ERROR: Failed to create insert for table {}: {}",
                                    table_name, e
                                );
                                std::process::exit(1);
                            }
                        };
                    
                        for (i, row) in rows.iter().enumerate() {
                            if let Err(e) = insert.write(row).await {
                                eprintln!(
                                    "❌ FATAL ERROR: Failed to write row {} to table {}: {}",
                                    i, table_name, e
                                );
                                std::process::exit(1);
                            }
                        }
                    
                        if let Err(e) = insert.end().await {
                            eprintln!(
                                "❌ FATAL ERROR: Failed to end insert for table {}: {}",
                                table_name, e
                            );
                            std::process::exit(1);
                        }
                    });
                }
            };
        }

        submit_insert!(data.pumpfun_trade_event, pumpfun_trade_event);
        submit_insert!(data.pumpfun_create_event, pumpfun_create_event);
        submit_insert!(data.pumpfun_migrate_event, pumpfun_migrate_event);
        submit_insert!(data.pumpfun_amm_buy_event, pumpfun_amm_buy_event);
        submit_insert!(data.pumpfun_amm_sell_event, pumpfun_amm_sell_event);
        submit_insert!(
            data.pumpfun_amm_create_pool_event,
            pumpfun_amm_create_pool_event
        );
        submit_insert!(data.pumpfun_amm_deposit_event, pumpfun_amm_deposit_event);
        submit_insert!(data.pumpfun_amm_withdraw_event, pumpfun_amm_withdraw_event);

        total_rows
    }

    /// 等待所有ClickHouse插入任务完成
    pub async fn wait_all_tasks(&self) {
        self.async_pool.wait_all_tasks().await;
    }
}
