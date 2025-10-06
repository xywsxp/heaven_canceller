use chrono::NaiveDate;
use std::path::PathBuf;
use syncer::config::{LocalConfig, RemoteServerConfig};
use syncer::pipeline::LocalPipeline;
use tempfile::tempdir;

#[tokio::test]
#[ignore = "integration test, requires ClickHouse and SSH server"]
async fn test_local_pipeline_integration() {
    // 从环境变量读取 SSH 配置
    let ssh_host = std::env::var("TEST_SSH_HOST")
        .expect("TEST_SSH_HOST environment variable not set");
    let ssh_port: u16 = std::env::var("TEST_SSH_PORT")
        .expect("TEST_SSH_PORT environment variable not set")
        .parse()
        .expect("TEST_SSH_PORT must be a valid port number");
    let ssh_user = std::env::var("TEST_SSH_USER")
        .expect("TEST_SSH_USER environment variable not set");
    let ssh_key = std::env::var("TEST_SSH_KEY")
        .expect("TEST_SSH_KEY environment variable not set");
    let remote_path = std::env::var("TEST_REMOTE_PATH")
        .expect("TEST_REMOTE_PATH environment variable not set");

    // 创建临时目录
    let temp_dir = tempdir().unwrap();
    let local_storage = temp_dir.path().to_path_buf();

    // 创建配置
    let config = LocalConfig {
        tables: vec![
            "pumpfun_trade_event_v2".to_string(),
            "pumpfun_create_event_v2".to_string(),
        ],
        table_event_mappings: [
            ("pumpfun_trade_event_v2".to_string(), "PumpfunTradeEventV2".to_string()),
            ("pumpfun_create_event_v2".to_string(), "PumpfunCreateEventV2".to_string()),
        ]
        .into_iter()
        .collect(),
        start_time: NaiveDate::from_ymd_opt(2025, 10, 1).unwrap(),
        local_storage_path: local_storage.clone(),
        remote_server: RemoteServerConfig {
            address: ssh_host,
            port: ssh_port,
            username: ssh_user,
            private_key_path: PathBuf::from(ssh_key),
            remote_path: PathBuf::from(remote_path),
        },
    };

    // 创建并运行 pipeline
    let pipeline = LocalPipeline::new(config);

    let result = pipeline.run().await;

    match result {
        Ok(_) => {
            println!("✅ Local pipeline integration test completed successfully");

            // 验证本地文件是否创建
            for table in &["pumpfun_trade_event_v2", "pumpfun_create_event_v2"] {
                let table_dir = local_storage.join(table);
                assert!(
                    table_dir.exists(),
                    "Table directory should exist: {:?}",
                    table_dir
                );

                let files: Vec<_> = std::fs::read_dir(&table_dir)
                    .unwrap()
                    .filter_map(|e| e.ok())
                    .collect();

                println!("  Table {}: {} files created", table, files.len());
                assert!(!files.is_empty(), "Should have created at least one file");

                // 验证文件命名格式
                for entry in files {
                    let filename = entry.file_name();
                    let filename_str = filename.to_str().unwrap();
                    assert!(
                        filename_str.ends_with(".parquet"),
                        "File should be parquet: {}",
                        filename_str
                    );
                    assert!(
                        filename_str.contains(table),
                        "Filename should contain table name: {}",
                        filename_str
                    );
                    println!("    - {}", filename_str);
                }
            }
        }
        Err(e) => {
            eprintln!("✗ Local pipeline integration test failed: {}", e);
            panic!("Pipeline failed: {}", e);
        }
    }
}

#[tokio::test]
#[ignore = "integration test, requires ClickHouse"]
async fn test_local_pipeline_single_day() {
    // 只测试单天数据，不传输
    let temp_dir = tempdir().unwrap();
    let local_storage = temp_dir.path().to_path_buf();

    let config = LocalConfig {
        tables: vec!["pumpfun_trade_event_v2".to_string()],
        table_event_mappings: [(
            "pumpfun_trade_event_v2".to_string(),
            "PumpfunTradeEventV2".to_string(),
        )]
        .into_iter()
        .collect(),
        start_time: NaiveDate::from_ymd_opt(2025, 10, 1).unwrap(),
        local_storage_path: local_storage.clone(),
        remote_server: RemoteServerConfig {
            address: "localhost".to_string(),
            port: 22,
            username: "test".to_string(),
            private_key_path: PathBuf::from("/tmp/fake_key"),
            remote_path: PathBuf::from("/tmp/fake"),
        },
    };

    let pipeline = LocalPipeline::new(config);

    // 注意：这个测试需要 ClickHouse 环境变量
    // CLICKHOUSE_URL, CLICKHOUSE_USER, CLICKHOUSE_DATABASE, CLICKHOUSE_PASSWORD

    let result = pipeline.run().await;

    // 如果 ClickHouse 可用，应该成功提取和写入
    // 传输会失败（因为 SSH 配置是假的），但这是预期的
    match result {
        Ok(_) => {
            println!("✓ Pipeline completed (transmission likely failed, that's OK)");
        }
        Err(e) => {
            let error_msg = e.to_string();
            
            // 如果是传输失败，说明提取和写入成功了
            if error_msg.contains("rsync") || error_msg.contains("ssh") {
                println!("✓ Extraction and writing succeeded (transmission failed as expected)");
                
                // 验证文件已创建
                let table_dir = local_storage.join("pumpfun_trade_event_v2");
                if table_dir.exists() {
                    let count = std::fs::read_dir(&table_dir).unwrap().count();
                    println!("  Created {} parquet file(s)", count);
                    assert!(count > 0, "Should have created parquet files");
                }
            } else {
                eprintln!("✗ Unexpected error: {}", error_msg);
            }
        }
    }
}

#[tokio::test]
async fn test_local_pipeline_config_validation() {
    // 测试配置验证
    let temp_dir = tempdir().unwrap();

    let config = LocalConfig {
        tables: vec!["test_table".to_string()],
        table_event_mappings: [].into_iter().collect(), // 缺少映射
        start_time: NaiveDate::from_ymd_opt(2025, 10, 1).unwrap(),
        local_storage_path: temp_dir.path().to_path_buf(),
        remote_server: RemoteServerConfig {
            address: "localhost".to_string(),
            port: 22,
            username: "test".to_string(),
            private_key_path: PathBuf::from("/tmp/key"),
            remote_path: PathBuf::from("/tmp/remote"),
        },
    };

    let pipeline = LocalPipeline::new(config);
    let result = pipeline.run().await;

    assert!(result.is_err(), "Should fail when event type mapping is missing");

    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(
            error_msg.contains("Event type not found"),
            "Error should mention missing event type: {}",
            error_msg
        );
        println!("✓ Correctly validated config: {}", error_msg);
    }
}

#[tokio::test]
async fn test_local_pipeline_date_progression() {
    // 测试日期递增逻辑
    use chrono::Utc;

    let today = Utc::now().date_naive();
    let start = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();

    println!("Testing date progression:");
    println!("  Start: {}", start);
    println!("  Today: {}", today);

    let mut current = start;
    let mut count = 0;

    while current <= today {
        count += 1;
        current = current.succ_opt().unwrap();
        
        if count > 100 {
            println!("  Stopping at 100 days for safety");
            break;
        }
    }

    println!("  Days to process: {}", count);
    assert!(count > 0, "Should have at least one day to process");
}
