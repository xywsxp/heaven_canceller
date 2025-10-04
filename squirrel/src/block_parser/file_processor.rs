use crate::clickhouse_util;
use crate::common::slot_meta::SlotMeta;
use common::async_pool::AsyncPool;
use common::clickhouse_client::ClickHouseClient;
use indicatif::{ProgressBar, ProgressStyle};
use rmp_serde::from_slice;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use tweezers::combinator::solana_combinator::SolanaCombinator;
use tweezers::normalizer::Normalizer;
use zstd::stream::read::Decoder;

pub struct FileProcessor {
    async_pool: AsyncPool,
    // 批量积累的数据
    pumpfun_trade_event_batch: Vec<clickhouse_util::clickhouse_events::PumpfunTradeEventV2>,
    pumpfun_create_event_batch: Vec<clickhouse_util::clickhouse_events::PumpfunCreateEventV2>,
    pumpfun_migrate_event_batch: Vec<clickhouse_util::clickhouse_events::PumpfunMigrateEventV2>,
    pumpfun_amm_buy_event_batch: Vec<clickhouse_util::clickhouse_events::PumpfunAmmBuyEventV2>,
    pumpfun_amm_sell_event_batch: Vec<clickhouse_util::clickhouse_events::PumpfunAmmSellEventV2>,
    pumpfun_amm_create_pool_event_batch:
        Vec<clickhouse_util::clickhouse_events::PumpfunAmmCreatePoolEventV2>,
    pumpfun_amm_deposit_event_batch:
        Vec<clickhouse_util::clickhouse_events::PumpfunAmmDepositEventV2>,
    pumpfun_amm_withdraw_event_batch:
        Vec<clickhouse_util::clickhouse_events::PumpfunAmmWithdrawEventV2>,
    batch_size: usize, // 批量大小
}

impl FileProcessor {
    pub fn new(max_concurrent_clickhouse_tasks: usize) -> Self {
        Self {
            async_pool: AsyncPool::new(max_concurrent_clickhouse_tasks),
            pumpfun_trade_event_batch: Vec::new(),
            pumpfun_create_event_batch: Vec::new(),
            pumpfun_migrate_event_batch: Vec::new(),
            pumpfun_amm_buy_event_batch: Vec::new(),
            pumpfun_amm_sell_event_batch: Vec::new(),
            pumpfun_amm_create_pool_event_batch: Vec::new(),
            pumpfun_amm_deposit_event_batch: Vec::new(),
            pumpfun_amm_withdraw_event_batch: Vec::new(),
            batch_size: 1000, // 每1000条记录提交一次
        }
    }

    /// 处理单个文件对
    pub async fn process_file_pair(
        &mut self,
        meta_path: &Path,
        bin_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let slot_meta = self.load_slot_meta(meta_path)?;

        let mut f = File::open(bin_path)?;

        // 创建进度条
        let pb = ProgressBar::new(slot_meta.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg} ({eta})")
                .unwrap()
                .progress_chars("##-"),
        );
        pb.set_message(format!("Processing {}", bin_path.display()));

        let mut packed_data = Vec::with_capacity(12 * 1024 * 1024); // 预分配12MB

        for slot in &slot_meta {
            let (offset, length) = match slot.offset {
                Some(offset) => (offset, slot.size),
                None => continue,
            };

            if f.seek(SeekFrom::Start(offset)).is_err() {
                continue;
            }
            let mut compressed_data = vec![0u8; length as usize];
            if f.read_exact(&mut compressed_data).is_err() {
                continue;
            }

            // 解压数据
            let mut decoder = match Decoder::new(&compressed_data[..]) {
                Ok(d) => d,
                Err(_) => continue,
            };
            packed_data.clear();
            if decoder.read_to_end(&mut packed_data).is_err() {
                continue;
            }

            // 解析Block
            if let Ok(block) = from_slice::<structure::block::Block>(&packed_data) {
                self.handle_block(&block).await;
            }

            // 更新进度条
            pb.inc(1);
        }

        // 完成进度条
        pb.finish_with_message(format!("Completed processing {}", bin_path.display()));

        // 刷新剩余的批量数据
        self.flush_all_batches().await;

        // 等待所有 ClickHouse 插入任务完成
        println!("Waiting for all ClickHouse insertions to complete...");
        self.async_pool.wait_all_tasks().await;
        println!("All insertions completed for this file");

        Ok(())
    }

    /// 加载slot元数据
    fn load_slot_meta(
        &self,
        meta_path: &Path,
    ) -> Result<Vec<SlotMeta>, Box<dyn std::error::Error>> {
        let mut file = File::open(meta_path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let slots: Vec<SlotMeta> = rmp_serde::from_slice(&buf)?;
        Ok(slots)
    }

    /// 处理单个block - 直接积累到批量中，减少拷贝
    async fn handle_block(&mut self, block: &structure::block::Block) {
        let parsed_block = Normalizer::normalize_block(block);

        if let Ok(parsed_block) = parsed_block {
            if let Some(combined_block) = SolanaCombinator::combine_block(&parsed_block) {
                for tx in combined_block.transactions.iter() {
                    // 直接在 batch Vec 上操作，避免临时 Vec
                    clickhouse_util::convert_transaction::TransactionConverter::convert(
                        tx,
                        &mut self.pumpfun_trade_event_batch,
                        &mut self.pumpfun_create_event_batch,
                        &mut self.pumpfun_migrate_event_batch,
                        &mut self.pumpfun_amm_buy_event_batch,
                        &mut self.pumpfun_amm_sell_event_batch,
                        &mut self.pumpfun_amm_create_pool_event_batch,
                        &mut self.pumpfun_amm_deposit_event_batch,
                        &mut self.pumpfun_amm_withdraw_event_batch,
                    );
                }

                // 检查是否需要刷新批量
                self.check_and_flush_batches().await;
            }
        }
    }

    /// 检查批量大小并在需要时刷新
    async fn check_and_flush_batches(&mut self) {
        let mut should_flush = false;

        // 检查任意一个批量是否达到阈值
        if self.pumpfun_trade_event_batch.len() >= self.batch_size
            || self.pumpfun_create_event_batch.len() >= self.batch_size
            || self.pumpfun_migrate_event_batch.len() >= self.batch_size
            || self.pumpfun_amm_buy_event_batch.len() >= self.batch_size
            || self.pumpfun_amm_sell_event_batch.len() >= self.batch_size
            || self.pumpfun_amm_create_pool_event_batch.len() >= self.batch_size
            || self.pumpfun_amm_deposit_event_batch.len() >= self.batch_size
            || self.pumpfun_amm_withdraw_event_batch.len() >= self.batch_size
        {
            should_flush = true;
        }

        if should_flush {
            self.flush_all_batches().await;
        }
    }

    /// 刷新所有批量数据到 ClickHouse
    async fn flush_all_batches(&mut self) {
        let trade_batch = std::mem::take(&mut self.pumpfun_trade_event_batch);
        let create_batch = std::mem::take(&mut self.pumpfun_create_event_batch);
        let migrate_batch = std::mem::take(&mut self.pumpfun_migrate_event_batch);
        let buy_batch = std::mem::take(&mut self.pumpfun_amm_buy_event_batch);
        let sell_batch = std::mem::take(&mut self.pumpfun_amm_sell_event_batch);
        let create_pool_batch = std::mem::take(&mut self.pumpfun_amm_create_pool_event_batch);
        let deposit_batch = std::mem::take(&mut self.pumpfun_amm_deposit_event_batch);
        let withdraw_batch = std::mem::take(&mut self.pumpfun_amm_withdraw_event_batch);

        self.submit_clickhouse_inserts(
            trade_batch,
            create_batch,
            migrate_batch,
            buy_batch,
            sell_batch,
            create_pool_batch,
            deposit_batch,
            withdraw_batch,
        );
    }

    /// 提交ClickHouse插入任务  
    fn submit_clickhouse_inserts(
        &self,
        pumpfun_trade_event_rows: Vec<clickhouse_util::clickhouse_events::PumpfunTradeEventV2>,
        pumpfun_create_event_rows: Vec<clickhouse_util::clickhouse_events::PumpfunCreateEventV2>,
        pumpfun_migrate_event_rows: Vec<clickhouse_util::clickhouse_events::PumpfunMigrateEventV2>,
        pumpfun_amm_buy_event_rows: Vec<clickhouse_util::clickhouse_events::PumpfunAmmBuyEventV2>,
        pumpfun_amm_sell_event_rows: Vec<clickhouse_util::clickhouse_events::PumpfunAmmSellEventV2>,
        pumpfun_amm_create_pool_event_rows: Vec<
            clickhouse_util::clickhouse_events::PumpfunAmmCreatePoolEventV2,
        >,
        pumpfun_amm_deposit_event_rows: Vec<
            clickhouse_util::clickhouse_events::PumpfunAmmDepositEventV2,
        >,
        pumpfun_amm_withdraw_event_rows: Vec<
            clickhouse_util::clickhouse_events::PumpfunAmmWithdrawEventV2,
        >,
    ) {
        // 宏来减少重复代码 - 错误会打印到控制台并终止程序
        macro_rules! submit_insert {
            ($rows:expr, $table:literal) => {
                if !$rows.is_empty() {
                    let rows = $rows;
                    self.async_pool.submit(move || async move {
                        let client = ClickHouseClient::instance().client();

                        let mut insert = match client.insert($table) {
                            Ok(insert) => insert,
                            Err(e) => {
                                eprintln!(
                                    "❌ FATAL ERROR: Failed to create insert for table {}: {}",
                                    $table, e
                                );
                                std::process::exit(1);
                            }
                        };

                        for (i, row) in rows.iter().enumerate() {
                            if let Err(e) = insert.write(row).await {
                                eprintln!(
                                    "❌ FATAL ERROR: Failed to write row {} to table {}: {}",
                                    i, $table, e
                                );
                                std::process::exit(1);
                            }
                        }

                        if let Err(e) = insert.end().await {
                            eprintln!(
                                "❌ FATAL ERROR: Failed to end insert for table {}: {}",
                                $table, e
                            );
                            std::process::exit(1);
                        }
                    });
                }
            };
        }

        submit_insert!(pumpfun_trade_event_rows, "pumpfun_trade_event_v2");
        submit_insert!(pumpfun_create_event_rows, "pumpfun_create_event_v2");
        submit_insert!(pumpfun_migrate_event_rows, "pumpfun_migrate_event_v2");
        submit_insert!(pumpfun_amm_buy_event_rows, "pumpfun_amm_buy_event_v2");
        submit_insert!(pumpfun_amm_sell_event_rows, "pumpfun_amm_sell_event_v2");
        submit_insert!(
            pumpfun_amm_create_pool_event_rows,
            "pumpfun_amm_create_pool_event_v2"
        );
        submit_insert!(
            pumpfun_amm_deposit_event_rows,
            "pumpfun_amm_deposit_event_v2"
        );
        submit_insert!(
            pumpfun_amm_withdraw_event_rows,
            "pumpfun_amm_withdraw_event_v2"
        );
    }

    /// 完成所有任务并等待协程池关闭
    pub async fn finish(self) {
        self.async_pool.join();
    }
}
