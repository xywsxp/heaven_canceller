use squirrel::block_parser::block_parser_service::{BlockParserService, Config};
use squirrel::common::slot_meta::SlotMeta;
use tempfile::TempDir;
use std::fs::File;
use rmp_serde;

#[tokio::test]
async fn test_config_from_toml_value() {
    let toml_str = r#"
        data_dir = "/tmp/data"
        processed_dir = "/tmp/processed"
        scan_interval_seconds = 300
        enable_watch = false
        max_concurrent_clickhouse_tasks = 5
    "#;
    
    let toml_value: toml::Value = toml::from_str(toml_str).unwrap();
    let config = Config::from_toml_value(&toml_value).unwrap();
    
    assert_eq!(config.data_dir, "/tmp/data");
    assert_eq!(config.processed_dir, "/tmp/processed");
    assert_eq!(config.scan_interval_seconds, 300);
    assert_eq!(config.enable_watch, false);
    assert_eq!(config.max_concurrent_clickhouse_tasks, 5);
}

#[tokio::test]
async fn test_config_with_defaults() {
    let toml_str = r#"
        data_dir = "/tmp/data"
        processed_dir = "/tmp/processed"
    "#;
    
    let toml_value: toml::Value = toml::from_str(toml_str).unwrap();
    let config = Config::from_toml_value(&toml_value).unwrap();
    
    assert_eq!(config.data_dir, "/tmp/data");
    assert_eq!(config.processed_dir, "/tmp/processed");
    assert_eq!(config.scan_interval_seconds, 600); // 默认值
    assert_eq!(config.enable_watch, true); // 默认值
    assert_eq!(config.max_concurrent_clickhouse_tasks, 3); // 默认值
}

#[tokio::test]
async fn test_service_creation() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let processed_dir = temp_dir.path().join("processed");
    
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&processed_dir).unwrap();
    
    let config = Config {
        data_dir: data_dir.to_string_lossy().to_string(),
        processed_dir: processed_dir.to_string_lossy().to_string(),
        scan_interval_seconds: 60,
        enable_watch: false,
        max_concurrent_clickhouse_tasks: 2,
    };
    
    let service = BlockParserService::new(config).unwrap();
    let stats = service.get_stats();
    
    assert_eq!(stats.processed_count, 0);
    assert!(stats.processed_prefixes.is_empty());
}

#[tokio::test]
async fn test_process_empty_directory() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let processed_dir = temp_dir.path().join("processed");
    
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&processed_dir).unwrap();
    
    let config = Config {
        data_dir: data_dir.to_string_lossy().to_string(),
        processed_dir: processed_dir.to_string_lossy().to_string(),
        scan_interval_seconds: 60,
        enable_watch: false,
        max_concurrent_clickhouse_tasks: 2,
    };
    
    let mut service = BlockParserService::new(config).unwrap();
    let result = service.process_pending_files().await.unwrap();
    
    assert_eq!(result, 0); // 没有文件被处理
}

#[tokio::test]
async fn test_process_single_file_pair() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let processed_dir = temp_dir.path().join("processed");
    
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&processed_dir).unwrap();
    
    // 创建测试文件对
    let meta_path = data_dir.join("100_200.meta");
    let bin_path = data_dir.join("100_200.bin");
    
    let empty_slots: Vec<SlotMeta> = vec![];
    let serialized = rmp_serde::to_vec(&empty_slots).unwrap();
    std::fs::write(&meta_path, serialized).unwrap();
    File::create(&bin_path).unwrap();
    
    let config = Config {
        data_dir: data_dir.to_string_lossy().to_string(),
        processed_dir: processed_dir.to_string_lossy().to_string(),
        scan_interval_seconds: 60,
        enable_watch: false,
        max_concurrent_clickhouse_tasks: 2,
    };
    
    let mut service = BlockParserService::new(config).unwrap();
    let result = service.process_pending_files().await.unwrap();
    
    assert_eq!(result, 1); // 一个文件被处理
    
    let stats = service.get_stats();
    assert_eq!(stats.processed_count, 1);
    assert!(stats.processed_prefixes.contains(&"100_200".to_string()));
}

#[tokio::test]
async fn test_skip_already_processed_files() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let processed_dir = temp_dir.path().join("processed");
    
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&processed_dir).unwrap();
    
    // 创建测试文件对
    let meta_path = data_dir.join("300_400.meta");
    let bin_path = data_dir.join("300_400.bin");
    
    let empty_slots: Vec<SlotMeta> = vec![];
    let serialized = rmp_serde::to_vec(&empty_slots).unwrap();
    std::fs::write(&meta_path, serialized).unwrap();
    File::create(&bin_path).unwrap();
    
    let config = Config {
        data_dir: data_dir.to_string_lossy().to_string(),
        processed_dir: processed_dir.to_string_lossy().to_string(),
        scan_interval_seconds: 60,
        enable_watch: false,
        max_concurrent_clickhouse_tasks: 2,
    };
    
    let mut service = BlockParserService::new(config).unwrap();
    
    // 第一次处理
    let result1 = service.process_pending_files().await.unwrap();
    assert_eq!(result1, 1);
    
    // 第二次处理，应该跳过已处理的文件
    let result2 = service.process_pending_files().await.unwrap();
    assert_eq!(result2, 0);
    
    let stats = service.get_stats();
    assert_eq!(stats.processed_count, 1);
}

#[tokio::test]
async fn test_service_stats() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let processed_dir = temp_dir.path().join("processed");
    
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&processed_dir).unwrap();
    
    // 创建多个测试文件对
    for i in 1..=3 {
        let meta_path = data_dir.join(format!("{}_{}00.meta", i * 100, i * 100 + 1));
        let bin_path = data_dir.join(format!("{}_{}00.bin", i * 100, i * 100 + 1));
        
        let empty_slots: Vec<SlotMeta> = vec![];
        let serialized = rmp_serde::to_vec(&empty_slots).unwrap();
        std::fs::write(&meta_path, serialized).unwrap();
        File::create(&bin_path).unwrap();
    }
    
    let config = Config {
        data_dir: data_dir.to_string_lossy().to_string(),
        processed_dir: processed_dir.to_string_lossy().to_string(),
        scan_interval_seconds: 60,
        enable_watch: false,
        max_concurrent_clickhouse_tasks: 2,
    };
    
    let mut service = BlockParserService::new(config).unwrap();
    let result = service.process_pending_files().await.unwrap();
    
    assert_eq!(result, 3);
    
    let stats = service.get_stats();
    assert_eq!(stats.processed_count, 3);
    
    // 验证stats的打印功能不会panic
    stats.print_summary();
}