use chrono::NaiveDate;
use syncer::extractor::ClickHouseExtractor;
use syncer::importer::ClickHouseImporter;
use syncer::parquet_helper::ParquetHelper;
use tempfile::tempdir;

#[tokio::test]
#[ignore = "integration test, requires ClickHouse"]
async fn test_import_trade_event() {
    // 1. 准备：提取数据并写入 Parquet
    let temp_dir = tempdir().unwrap();
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();
    
    let extractor = ClickHouseExtractor::new();
    let parquet_helper = ParquetHelper::new();
    
    // 提取数据
    let batch = extractor
        .extract_daily_events("pumpfun_trade_event_v2", "PumpfunTradeEventV2", date)
        .await
        .expect("Failed to extract data");
    
    println!("✓ Extracted {} rows", batch.num_rows());
    
    // 写入 Parquet
    let parquet_file = parquet_helper
        .write_daily_parquet(
            "pumpfun_trade_event_v2",
            date,
            batch,
            temp_dir.path(),
        )
        .await
        .expect("Failed to write parquet");
    
    println!("✓ Written to: {:?}", parquet_file);
    
    // 2. 导入到测试表
    let importer = ClickHouseImporter::new();
    let target_table = "pumpfun_trade_event_v2_test";
    
    let rows = importer
        .import_parquet(&parquet_file, target_table, "PumpfunTradeEventV2")
        .await;
    
    match rows {
        Ok(count) => {
            println!("✓ Imported {} rows to {}", count, target_table);
            assert!(count > 0, "Should have imported at least one row");
        }
        Err(e) => {
            eprintln!("✗ Import failed: {}", e);
            eprintln!("  Note: Make sure test table exists:");
            eprintln!("  CREATE TABLE {} AS pumpfun_trade_event_v2", target_table);
            panic!("Import failed: {}", e);
        }
    }
}

#[tokio::test]
#[ignore = "integration test, requires ClickHouse"]
async fn test_import_create_event() {
    let temp_dir = tempdir().unwrap();
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();
    
    let extractor = ClickHouseExtractor::new();
    let parquet_helper = ParquetHelper::new();
    
    // 提取
    let batch = extractor
        .extract_daily_events("pumpfun_create_event_v2", "PumpfunCreateEventV2", date)
        .await
        .expect("Failed to extract data");
    
    println!("✓ Extracted {} create events", batch.num_rows());
    
    // 写入
    let parquet_file = parquet_helper
        .write_daily_parquet(
            "pumpfun_create_event_v2",
            date,
            batch,
            temp_dir.path(),
        )
        .await
        .expect("Failed to write parquet");
    
    // 导入
    let importer = ClickHouseImporter::new();
    let target_table = "pumpfun_create_event_v2_test";
    
    let rows = importer
        .import_parquet(&parquet_file, target_table, "PumpfunCreateEventV2")
        .await;
    
    match rows {
        Ok(count) => {
            println!("✓ Imported {} create events to {}", count, target_table);
            assert!(count > 0);
        }
        Err(e) => {
            eprintln!("✗ Import failed: {}", e);
            eprintln!("  CREATE TABLE {} AS pumpfun_create_event_v2", target_table);
            panic!("Import failed: {}", e);
        }
    }
}

#[tokio::test]
#[ignore = "integration test, requires ClickHouse"]
async fn test_round_trip_data_integrity() {
    // 完整的往返测试：提取 → Parquet → 导入 → 验证
    let temp_dir = tempdir().unwrap();
    let date = NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();
    
    let extractor = ClickHouseExtractor::new();
    let parquet_helper = ParquetHelper::new();
    let importer = ClickHouseImporter::new();
    
    // 1. 从源表提取
    let original_batch = extractor
        .extract_daily_events("pumpfun_trade_event_v2", "PumpfunTradeEventV2", date)
        .await
        .expect("Failed to extract");
    
    let original_rows = original_batch.num_rows();
    println!("✓ Original data: {} rows", original_rows);
    
    // 2. 写入 Parquet
    let parquet_file = parquet_helper
        .write_daily_parquet(
            "test_round_trip",
            date,
            original_batch,
            temp_dir.path(),
        )
        .await
        .expect("Failed to write");
    
    println!("✓ Written to Parquet");
    
    // 3. 导入到测试表
    let target_table = "pumpfun_trade_event_v2_round_trip_test";
    let imported_rows = importer
        .import_parquet(&parquet_file, target_table, "PumpfunTradeEventV2")
        .await
        .expect("Failed to import");
    
    println!("✓ Imported {} rows", imported_rows);
    
    // 4. 从测试表重新提取验证
    let imported_batch = extractor
        .extract_daily_events(target_table, "PumpfunTradeEventV2", date)
        .await
        .expect("Failed to extract from test table");
    
    let final_rows = imported_batch.num_rows();
    println!("✓ Re-extracted {} rows from test table", final_rows);
    
    // 验证行数一致
    assert_eq!(
        original_rows, imported_rows as usize,
        "Imported rows should match original"
    );
    assert_eq!(
        original_rows, final_rows,
        "Re-extracted rows should match original"
    );
    
    println!("✅ Round-trip data integrity verified!");
    println!("   Original: {} → Parquet → Import: {} → Re-extract: {}",
        original_rows, imported_rows, final_rows);
}

#[tokio::test]
async fn test_invalid_event_type() {
    let temp_dir = tempdir().unwrap();
    let fake_file = temp_dir.path().join("fake.parquet");
    
    // 创建一个假文件
    std::fs::write(&fake_file, b"fake data").unwrap();
    
    let importer = ClickHouseImporter::new();
    
    let result = importer
        .import_parquet(&fake_file, "test_table", "InvalidEventType")
        .await;
    
    assert!(result.is_err(), "Should fail with invalid event type");
    
    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(
            error_msg.contains("Unknown event type") || error_msg.contains("InvalidEventType"),
            "Error should mention invalid event type: {}",
            error_msg
        );
        println!("✓ Correctly rejected invalid event type: {}", error_msg);
    }
}

#[tokio::test]
#[ignore = "integration test, needs empty test table"]
async fn test_import_empty_parquet() {
    // 测试导入空 Parquet 文件
    let temp_dir = tempdir().unwrap();
    let date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    
    let extractor = ClickHouseExtractor::new();
    let parquet_helper = ParquetHelper::new();
    let importer = ClickHouseImporter::new();
    
    // 提取一个可能为空的日期
    let batch = extractor
        .extract_daily_events("pumpfun_trade_event_v2", "PumpfunTradeEventV2", date)
        .await
        .expect("Failed to extract");
    
    if batch.num_rows() == 0 {
        println!("✓ Got empty batch as expected for {}", date);
        
        // 写入空 Parquet
        let parquet_file = parquet_helper
            .write_daily_parquet("test_empty", date, batch, temp_dir.path())
            .await
            .expect("Failed to write empty parquet");
        
        // 导入空文件
        let rows = importer
            .import_parquet(&parquet_file, "pumpfun_trade_event_v2_test", "PumpfunTradeEventV2")
            .await
            .expect("Failed to import empty file");
        
        assert_eq!(rows, 0, "Should import 0 rows from empty file");
        println!("✓ Successfully handled empty Parquet file");
    } else {
        println!("⊘ Skipping empty test, date {} has {} rows", date, batch.num_rows());
    }
}
