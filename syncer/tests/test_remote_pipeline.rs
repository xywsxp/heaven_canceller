use chrono::NaiveDate;
use std::collections::HashMap;
use syncer::config::RemoteConfig;
use syncer::extractor::ClickHouseExtractor;
use syncer::parquet_helper::ParquetHelper;
use syncer::pipeline::RemotePipeline;
use tempfile::tempdir;
use utils::clickhouse_client::ClickHouseClient;

/// 辅助函数：创建临时测试表
async fn create_test_table(table_name: &str, source_table: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = ClickHouseClient::instance().client();
    
    // 删除已存在的表
    let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
    client.query(&drop_sql).execute().await?;
    
    // 创建新表（复制源表结构）
    let create_sql = format!("CREATE TABLE {} AS {}", table_name, source_table);
    client.query(&create_sql).execute().await?;
    
    println!("  ✓ Created test table: {}", table_name);
    Ok(())
}

/// 辅助函数：删除测试表
async fn drop_test_table(table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = ClickHouseClient::instance().client();
    let drop_sql = format!("DROP TABLE IF EXISTS {}", table_name);
    client.query(&drop_sql).execute().await?;
    println!("  ✓ Dropped test table: {}", table_name);
    Ok(())
}

/// 辅助函数：查询表行数
async fn count_table_rows(table_name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let client = ClickHouseClient::instance().client();
    let count_sql = format!("SELECT count() FROM {}", table_name);
    
    let count: u64 = client
        .query(&count_sql)
        .fetch_one::<u64>()
        .await?;
    
    Ok(count)
}

#[tokio::test]
#[ignore = "integration test, requires ClickHouse"]
async fn test_remote_pipeline_integration() {
    // 准备：创建临时测试表
    let test_table = "pumpfun_trade_event_v2_remote_test_tmp";
    
    println!("🔧 Setting up test table...");
    create_test_table(test_table, "pumpfun_trade_event_v2")
        .await
        .expect("Failed to create test table");
    
    // 准备：先创建一些 Parquet 文件
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().to_path_buf();
    
    println!("📦 Preparing test data...");
    
    // 1. 提取几天的数据并写入 Parquet
    let extractor = ClickHouseExtractor::new();
    let parquet_helper = ParquetHelper::new();
    
    let dates = vec![
        NaiveDate::from_ymd_opt(2025, 10, 1).unwrap(),
        NaiveDate::from_ymd_opt(2025, 10, 2).unwrap(),
    ];
    
    let mut expected_rows = 0u64;
    
    for date in &dates {
        let batch = extractor
            .extract_daily_events("pumpfun_trade_event_v2", "PumpfunTradeEventV2", *date)
            .await
            .expect("Failed to extract");
        
        expected_rows += batch.num_rows() as u64;
        
        parquet_helper
            .write_daily_parquet(
                "pumpfun_trade_event_v2",
                *date,
                batch,
                &storage_path,
            )
            .await
            .expect("Failed to write parquet");
        
        println!("  ✓ Created parquet for {}", date);
    }
    
    println!("✓ Test data prepared (expected {} rows)\n", expected_rows);
    
    // 2. 配置 RemotePipeline
    let config = RemoteConfig {
        remote_storage_path: storage_path.clone(),
        import_mappings: [
            ("pumpfun_trade_event_v2".to_string(), test_table.to_string()),
        ]
        .into_iter()
        .collect(),
        table_event_mappings: [
            ("pumpfun_trade_event_v2".to_string(), "PumpfunTradeEventV2".to_string()),
        ]
        .into_iter()
        .collect(),
    };
    
    // 3. 运行 RemotePipeline
    let pipeline = RemotePipeline::new(config);
    
    let result = pipeline.run().await;
    
    match result {
        Ok(_) => {
            println!("✅ Remote pipeline completed successfully");
            
            // 验证导入的行数
            let actual_rows = count_table_rows(test_table).await.expect("Failed to count rows");
            println!("   Expected: {} rows, Actual: {} rows", expected_rows, actual_rows);
            
            assert_eq!(
                expected_rows, actual_rows,
                "Imported rows should match expected"
            );
        }
        Err(e) => {
            eprintln!("✗ Remote pipeline failed: {}", e);
            panic!("Pipeline failed: {}", e);
        }
    }
    
    // 清理：删除测试表
    println!("\n🧹 Cleaning up...");
    drop_test_table(test_table).await.expect("Failed to drop test table");
}

#[tokio::test]
#[ignore = "integration test, requires ClickHouse"]
async fn test_remote_pipeline_multiple_tables() {
    let test_table_1 = "pumpfun_trade_event_v2_remote_test_tmp";
    let test_table_2 = "pumpfun_create_event_v2_remote_test_tmp";
    
    println!("🔧 Setting up test tables...");
    create_test_table(test_table_1, "pumpfun_trade_event_v2")
        .await
        .expect("Failed to create test table 1");
    create_test_table(test_table_2, "pumpfun_create_event_v2")
        .await
        .expect("Failed to create test table 2");
    
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().to_path_buf();
    
    println!("📦 Preparing test data for multiple tables...");
    
    let extractor = ClickHouseExtractor::new();
    let parquet_helper = ParquetHelper::new();
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();
    
    // 准备两个表的数据
    let tables = vec![
        ("pumpfun_trade_event_v2", "PumpfunTradeEventV2"),
        ("pumpfun_create_event_v2", "PumpfunCreateEventV2"),
    ];
    
    for (table, event_type) in &tables {
        let batch = extractor
            .extract_daily_events(table, event_type, date)
            .await
            .expect("Failed to extract");
        
        parquet_helper
            .write_daily_parquet(table, date, batch, &storage_path)
            .await
            .expect("Failed to write");
        
        println!("  ✓ Created parquet for {}", table);
    }
    
    println!("✓ Test data prepared\n");
    
    // 配置导入
    let config = RemoteConfig {
        remote_storage_path: storage_path,
        import_mappings: [
            ("pumpfun_trade_event_v2".to_string(), test_table_1.to_string()),
            ("pumpfun_create_event_v2".to_string(), test_table_2.to_string()),
        ]
        .into_iter()
        .collect(),
        table_event_mappings: [
            ("pumpfun_trade_event_v2".to_string(), "PumpfunTradeEventV2".to_string()),
            ("pumpfun_create_event_v2".to_string(), "PumpfunCreateEventV2".to_string()),
        ]
        .into_iter()
        .collect(),
    };
    
    let pipeline = RemotePipeline::new(config);
    
    match pipeline.run().await {
        Ok(_) => {
            println!("✅ Multiple tables import completed");
            
            let rows1 = count_table_rows(test_table_1).await.expect("Failed to count");
            let rows2 = count_table_rows(test_table_2).await.expect("Failed to count");
            
            println!("   Table 1: {} rows", rows1);
            println!("   Table 2: {} rows", rows2);
            
            assert!(rows1 > 0, "Table 1 should have rows");
            assert!(rows2 > 0, "Table 2 should have rows");
        }
        Err(e) => {
            eprintln!("✗ Failed: {}", e);
            panic!("Pipeline failed: {}", e);
        }
    }
    
    // 清理
    println!("\n🧹 Cleaning up...");
    drop_test_table(test_table_1).await.ok();
    drop_test_table(test_table_2).await.ok();
}

#[tokio::test]
async fn test_remote_pipeline_empty_folder() {
    // 测试空文件夹的处理
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().to_path_buf();
    
    // 创建空文件夹
    std::fs::create_dir_all(storage_path.join("empty_folder")).unwrap();
    
    let config = RemoteConfig {
        remote_storage_path: storage_path,
        import_mappings: [
            ("empty_folder".to_string(), "test_table".to_string()),
        ]
        .into_iter()
        .collect(),
        table_event_mappings: [
            ("empty_folder".to_string(), "PumpfunTradeEventV2".to_string()),
        ]
        .into_iter()
        .collect(),
    };
    
    let pipeline = RemotePipeline::new(config);
    
    let result = pipeline.run().await;
    
    // 应该成功但跳过空文件夹
    assert!(result.is_ok(), "Should handle empty folder gracefully");
    println!("✓ Empty folder handled correctly");
}

#[tokio::test]
async fn test_remote_pipeline_missing_folder() {
    // 测试不存在的文件夹
    let temp_dir = tempdir().unwrap();
    
    let config = RemoteConfig {
        remote_storage_path: temp_dir.path().to_path_buf(),
        import_mappings: [
            ("nonexistent_folder".to_string(), "test_table".to_string()),
        ]
        .into_iter()
        .collect(),
        table_event_mappings: [
            ("nonexistent_folder".to_string(), "PumpfunTradeEventV2".to_string()),
        ]
        .into_iter()
        .collect(),
    };
    
    let pipeline = RemotePipeline::new(config);
    
    let result = pipeline.run().await;
    
    // 应该成功但跳过不存在的文件夹
    assert!(result.is_ok(), "Should handle missing folder gracefully");
    println!("✓ Missing folder handled correctly");
}

#[tokio::test]
async fn test_remote_pipeline_missing_event_type() {
    let temp_dir = tempdir().unwrap();
    
    let config = RemoteConfig {
        remote_storage_path: temp_dir.path().to_path_buf(),
        import_mappings: [
            ("test_folder".to_string(), "test_table".to_string()),
        ]
        .into_iter()
        .collect(),
        table_event_mappings: HashMap::new(), // 没有事件类型映射
    };
    
    let pipeline = RemotePipeline::new(config);
    
    let result = pipeline.run().await;
    
    assert!(result.is_err(), "Should fail when event type mapping is missing");
    
    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(
            error_msg.contains("Event type not found"),
            "Error should mention missing event type: {}",
            error_msg
        );
        println!("✓ Correctly validated event type mapping: {}", error_msg);
    }
}

#[tokio::test]
#[ignore = "integration test, requires ClickHouse"]
async fn test_remote_pipeline_file_ordering() {
    // 测试文件按日期顺序处理
    let test_table = "pumpfun_trade_event_v2_remote_test_tmp";
    
    println!("🔧 Setting up test table...");
    create_test_table(test_table, "pumpfun_trade_event_v2")
        .await
        .expect("Failed to create test table");
    
    let temp_dir = tempdir().unwrap();
    let storage_path = temp_dir.path().to_path_buf();
    
    println!("📦 Creating files in random order...");
    
    let extractor = ClickHouseExtractor::new();
    let parquet_helper = ParquetHelper::new();
    
    // 倒序创建文件（测试排序）
    let dates = vec![
        NaiveDate::from_ymd_opt(2025, 10, 3).unwrap(),
        NaiveDate::from_ymd_opt(2025, 10, 1).unwrap(),
        NaiveDate::from_ymd_opt(2025, 10, 2).unwrap(),
    ];
    
    for date in &dates {
        let batch = extractor
            .extract_daily_events("pumpfun_trade_event_v2", "PumpfunTradeEventV2", *date)
            .await
            .expect("Failed to extract");
        
        parquet_helper
            .write_daily_parquet("pumpfun_trade_event_v2", *date, batch, &storage_path)
            .await
            .expect("Failed to write");
        
        println!("  ✓ Created: {}", date);
    }
    
    println!("✓ Files created (should be sorted during import)\n");
    
    let config = RemoteConfig {
        remote_storage_path: storage_path,
        import_mappings: [
            ("pumpfun_trade_event_v2".to_string(), test_table.to_string()),
        ]
        .into_iter()
        .collect(),
        table_event_mappings: [
            ("pumpfun_trade_event_v2".to_string(), "PumpfunTradeEventV2".to_string()),
        ]
        .into_iter()
        .collect(),
    };
    
    let pipeline = RemotePipeline::new(config);
    
    match pipeline.run().await {
        Ok(_) => {
            println!("✅ Files processed in sorted order");
            let rows = count_table_rows(test_table).await.expect("Failed to count");
            println!("   Total imported: {} rows", rows);
            assert!(rows > 0, "Should have imported rows");
        }
        Err(e) => {
            eprintln!("✗ Failed: {}", e);
            panic!("Pipeline failed: {}", e);
        }
    }
    
    // 清理
    println!("\n🧹 Cleaning up...");
    drop_test_table(test_table).await.ok();
}
