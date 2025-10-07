use std::error::Error;
use chrono::Utc;

use crate::config::{LocalConfig, RemoteConfig};

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;
use crate::extractor::ClickHouseExtractor;
use crate::importer::ClickHouseImporter;
use crate::parquet_helper::ParquetHelper;
use crate::transport::RsyncTransport;

/// 本地模式流水线
/// 
/// 负责: 提取 -> 写入 Parquet -> 传输
pub struct LocalPipeline {
    extractor: ClickHouseExtractor,
    parquet_helper: ParquetHelper,
    transport: RsyncTransport,
    config: LocalConfig,
}

impl LocalPipeline {
    pub fn new(config: LocalConfig) -> Self {
        Self {
            extractor: ClickHouseExtractor::new(),
            parquet_helper: ParquetHelper::new(),
            transport: RsyncTransport::new(),
            config,
        }
    }

    /// 运行本地模式流水线
    pub async fn run(&self) -> Result<()> {
        let today = Utc::now().date_naive();
        
        println!("🚀 Starting Local Pipeline");
        println!("   Start date: {}", self.config.start_time);
        println!("   Today: {}", today);
        println!("   Tables: {:?}", self.config.tables);
        println!();

        // 遍历所有表
        for (table_idx, table) in self.config.tables.iter().enumerate() {
            println!("📊 Processing table {}/{}: {}", 
                table_idx + 1, 
                self.config.tables.len(), 
                table
            );

            // 获取事件类型
            let event_type = self.config.table_event_mappings.get(table)
                .ok_or_else(|| format!("Event type not found for table: {}", table))?;

            let table_dir = self.config.local_storage_path.join(table);
            
            // 计算日期范围
            let mut current_date = self.config.start_time;
            let mut day_count = 0;
            
            // 按天处理
            while current_date <= today {
                day_count += 1;
                
                println!("   📅 Day {}: {} ({})", 
                    day_count, 
                    current_date, 
                    current_date.format("%A")
                );

                // 1. 提取数据
                print!("      → Extracting data... ");
                let batch = self.extractor
                    .extract_daily_events(table, event_type, current_date)
                    .await?;
                println!("✓ ({} rows)", batch.num_rows());

                // 2. 写入 Parquet
                print!("      → Writing Parquet... ");
                let file_path = self.parquet_helper
                    .write_daily_parquet(
                        table,
                        current_date,
                        batch,
                        &self.config.local_storage_path,
                    )
                    .await?;
                println!("✓ {:?}", file_path.file_name().unwrap());

                // 3. 立即传输该文件
                print!("      → Syncing to remote... ");
                self.transport
                    .sync_directory(&table_dir, &self.config.remote_server)
                    .await?;
                println!("✓");

                // 4. 删除本地文件以节省空间
                print!("      → Cleaning up local file... ");
                std::fs::remove_file(&file_path)?;
                println!("✓");

                // 移动到下一天
                current_date = current_date
                    .succ_opt()
                    .ok_or("Failed to get next date")?;
            }
            
            println!("   ✅ Table {} completed ({} days)\n", table, day_count);
        }

        println!("🎉 Local Pipeline completed successfully!");
        println!("   Total tables processed: {}", self.config.tables.len());
        
        Ok(())
    }
}

/// 远程模式流水线
/// 
/// 负责: 扫描文件 -> 读取 Parquet -> 导入
pub struct RemotePipeline {
    parquet_helper: ParquetHelper,
    importer: ClickHouseImporter,
    config: RemoteConfig,
}

impl RemotePipeline {
    pub fn new(config: RemoteConfig) -> Self {
        Self {
            parquet_helper: ParquetHelper::new(),
            importer: ClickHouseImporter::new(),
            config,
        }
    }

    /// 运行远程模式流水线
    pub async fn run(&self) -> Result<()> {
        println!("🚀 Starting Remote Pipeline");
        println!("   Storage path: {:?}", self.config.remote_storage_path);
        println!("   Import mappings: {} folders", self.config.import_mappings.len());
        println!();

        let mut total_files = 0;
        let mut total_rows = 0u64;

        // 遍历所有导入映射
        for (folder_idx, (source_folder, target_table)) in self.config.import_mappings.iter().enumerate() {
            println!("📂 Processing folder {}/{}: {} → {}", 
                folder_idx + 1, 
                self.config.import_mappings.len(),
                source_folder,
                target_table
            );

            // 获取事件类型
            let event_type = self.config.table_event_mappings.get(source_folder)
                .ok_or_else(|| format!("Event type not found for folder: {}", source_folder))?;

            // 构建文件夹路径
            let folder_path = self.config.remote_storage_path.join(source_folder);
            
            if !folder_path.exists() {
                println!("   ⚠️  Folder not found, skipping: {:?}", folder_path);
                continue;
            }

            // 扫描并收集所有 .parquet 文件
            let mut entries: Vec<_> = std::fs::read_dir(&folder_path)?
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().extension()
                        .and_then(|ext| ext.to_str())
                        .map(|ext| ext == "parquet")
                        .unwrap_or(false)
                })
                .collect();

            // 按文件名排序（确保按日期顺序处理）
            entries.sort_by_key(|entry| entry.file_name());

            if entries.is_empty() {
                println!("   ⚠️  No parquet files found in {:?}", folder_path);
                continue;
            }

            println!("   Found {} parquet files", entries.len());

            // 逐个导入文件
            for (file_idx, entry) in entries.iter().enumerate() {
                let file_path = entry.path();
                let file_name = file_path.file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");

                print!("   📄 File {}/{}: {} ... ", 
                    file_idx + 1, 
                    entries.len(),
                    file_name
                );

                // 导入文件
                let rows = self.importer
                    .import_parquet(&file_path, target_table, event_type)
                    .await?;

                total_rows += rows;
                total_files += 1;

                println!("✓ ({} rows)", rows);
            }

            println!("   ✅ Folder {} completed ({} files, {} rows)\n", 
                source_folder, 
                entries.len(),
                total_rows
            );
        }

        println!("🎉 Remote Pipeline completed successfully!");
        println!("   Total files processed: {}", total_files);
        println!("   Total rows imported: {}", total_rows);
        
        Ok(())
    }
}
