use super::file_scanner::{FileScanner, FilePair};
use super::file_processor::FileProcessor;
use super::processed_tracker::ProcessedTracker;
use std::path::PathBuf;
use tokio::time::{sleep, Duration};
use toml;

pub struct BlockParserService {
    scanner: FileScanner,
    tracker: ProcessedTracker,
    processor: FileProcessor,
    scan_interval_seconds: u64,
    enable_watch: bool,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: String,
    pub processed_dir: String,
    pub scan_interval_seconds: u64,
    pub enable_watch: bool,
    pub max_concurrent_clickhouse_tasks: usize,
}

impl Config {
    pub fn from_toml_file(config_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_content = std::fs::read_to_string(config_path)?;
        let toml_value: toml::Value = toml::from_str(&config_content)?;
        
        let config = Config {
            data_dir: toml_value.get("data_dir")
                .and_then(|v| v.as_str())
                .ok_or("Missing 'data_dir' in config")?
                .to_string(),
            processed_dir: toml_value.get("processed_dir")
                .and_then(|v| v.as_str())
                .ok_or("Missing 'processed_dir' in config")?
                .to_string(),
            scan_interval_seconds: toml_value.get("scan_interval_seconds")
                .and_then(|v| v.as_integer())
                .unwrap_or(600) as u64,
            enable_watch: toml_value.get("enable_watch")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            max_concurrent_clickhouse_tasks: toml_value.get("max_concurrent_clickhouse_tasks")
                .and_then(|v| v.as_integer())
                .unwrap_or(3) as usize,
        };
        
        Ok(config)
    }
    
    pub fn from_toml_value(toml_value: &toml::Value) -> Result<Self, Box<dyn std::error::Error>> {
        let config = Config {
            data_dir: toml_value.get("data_dir")
                .and_then(|v| v.as_str())
                .ok_or("Missing 'data_dir' in config")?
                .to_string(),
            processed_dir: toml_value.get("processed_dir")
                .and_then(|v| v.as_str())
                .ok_or("Missing 'processed_dir' in config")?
                .to_string(),
            scan_interval_seconds: toml_value.get("scan_interval_seconds")
                .and_then(|v| v.as_integer())
                .unwrap_or(600) as u64,
            enable_watch: toml_value.get("enable_watch")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            max_concurrent_clickhouse_tasks: toml_value.get("max_concurrent_clickhouse_tasks")
                .and_then(|v| v.as_integer())
                .unwrap_or(3) as usize,
        };
        
        Ok(config)
    }
}

impl BlockParserService {
    pub fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        let scanner = FileScanner::new(PathBuf::from(&config.data_dir));
        let mut tracker = ProcessedTracker::new(PathBuf::from(&config.processed_dir));
        let processor = FileProcessor::new(config.max_concurrent_clickhouse_tasks);
        
        // 加载已处理文件列表
        tracker.load_processed_list()?;
        
        Ok(Self {
            scanner,
            tracker,
            processor,
            scan_interval_seconds: config.scan_interval_seconds,
            enable_watch: config.enable_watch,
        })
    }

    /// 主循环：扫描->处理->等待  
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("BlockParserService starting...");
        println!("Enable watch mode: {}", self.enable_watch);
        println!("Scan interval: {} seconds", self.scan_interval_seconds);
        
        loop {
            match self.process_pending_files().await {
                Ok(processed_count) => {
                    if processed_count > 0 {
                        println!("Processed {} file pairs", processed_count);
                    }
                }
                Err(e) => {
                    eprintln!("Error processing files: {}", e);
                    // 错误时直接panic，便于调试
                    panic!("Processing failed: {}", e);
                }
            }
            
            if !self.enable_watch {
                println!("Watch mode disabled, exiting after single scan");
                break;
            }
            
            // 等待下一次扫描
            sleep(Duration::from_secs(self.scan_interval_seconds)).await;
        }
        
        // 所有文件已处理完成，每个文件都已等待其插入任务完成
        println!("BlockParserService stopped");
        
        Ok(())
    }

    /// 单次扫描处理
    pub async fn process_pending_files(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        // 扫描可用的文件对
        let file_pairs = self.scanner.scan_available_files()?;
        
        if file_pairs.is_empty() {
            println!("No file pairs found");
            return Ok(0);
        }
        
        println!("Found {} file pairs", file_pairs.len());
        
        // 过滤出未处理的文件对
        let pending_pairs: Vec<FilePair> = file_pairs
            .into_iter()
            .filter(|pair| !self.tracker.is_processed(&pair.prefix))
            .collect();
            
        if pending_pairs.is_empty() {
            println!("All file pairs already processed");
            return Ok(0);
        }
        
        println!("Processing {} pending file pairs", pending_pairs.len());
        
        // 处理每个文件对
        let mut processed_count = 0;
        for pair in pending_pairs {
            println!("Processing file pair: {}", pair.prefix);
            
            match self.processor.process_file_pair(&pair.meta_path, &pair.bin_path).await {
                Ok(_) => {
                    // 标记为已处理
                    self.tracker.mark_as_processed(&pair.prefix)?;
                    processed_count += 1;
                    println!("Successfully processed: {}", pair.prefix);
                }
                Err(e) => {
                    eprintln!("Failed to process {}: {}", pair.prefix, e);
                    // 根据需求，处理失败直接panic
                    panic!("Processing failed for {}: {}", pair.prefix, e);
                }
            }
        }
        
        Ok(processed_count)
    }
    
    /// 获取已处理文件的统计信息
    pub fn get_stats(&self) -> ServiceStats {
        ServiceStats {
            processed_count: self.tracker.processed_count(),
            processed_prefixes: self.tracker.get_processed_prefixes(),
        }
    }
    
    /// 清理日志文件中的重复条目
    pub fn cleanup_processed_log(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.tracker.cleanup_log()
    }
}

#[derive(Debug)]
pub struct ServiceStats {
    pub processed_count: usize,
    pub processed_prefixes: Vec<String>,
}

impl ServiceStats {
    pub fn print_summary(&self) {
        println!("=== BlockParserService Statistics ===");
        println!("Total processed files: {}", self.processed_count);
        
        if !self.processed_prefixes.is_empty() {
            println!("Recently processed files:");
            let show_count = std::cmp::min(10, self.processed_prefixes.len());
            for prefix in &self.processed_prefixes[self.processed_prefixes.len() - show_count..] {
                println!("  - {}", prefix);
            }
            
            if self.processed_prefixes.len() > 10 {
                println!("  ... and {} more", self.processed_prefixes.len() - 10);
            }
        }
        println!("====================================");
    }
}