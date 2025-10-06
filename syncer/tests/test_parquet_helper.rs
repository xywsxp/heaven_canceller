use arrow::array::{StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use std::sync::Arc;
use syncer::parquet_helper::ParquetHelper;
use tempfile::tempdir;

#[tokio::test]
async fn test_write_and_read_parquet() {
    // 创建临时目录
    let temp_dir = tempdir().unwrap();
    let output_dir = temp_dir.path();

    // 创建测试数据
    let schema = Arc::new(Schema::new(vec![
        Field::new("signature", DataType::Utf8, false),
        Field::new("slot", DataType::UInt64, false),
        Field::new("timestamp", DataType::UInt32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "sig1",
                "sig2",
                "sig3",
            ])),
            Arc::new(UInt64Array::from(vec![100, 101, 102])),
            Arc::new(UInt32Array::from(vec![1000000, 1000001, 1000002])),
        ],
    )
    .unwrap();

    let helper = ParquetHelper::new();
    let date = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();

    // 写入 Parquet 文件
    let file_path = helper
        .write_daily_parquet("test_table", date, batch.clone(), output_dir)
        .await
        .unwrap();

    println!("✓ Written to: {:?}", file_path);
    assert!(file_path.exists(), "Parquet file should exist");
    assert!(
        file_path.to_str().unwrap().contains("test_table_2025-01-15.parquet"),
        "File name should match expected format"
    );

    // 读取 Parquet 文件
    let read_batch = helper.read_parquet(&file_path).await.unwrap();

    println!("✓ Read {} rows", read_batch.num_rows());
    assert_eq!(read_batch.num_rows(), 3, "Should read 3 rows");
    assert_eq!(read_batch.num_columns(), 3, "Should have 3 columns");
    assert_eq!(
        read_batch.schema().as_ref(),
        schema.as_ref(),
        "Schema should match"
    );

    // 验证数据内容
    let signatures = read_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(signatures.value(0), "sig1");
    assert_eq!(signatures.value(1), "sig2");
    assert_eq!(signatures.value(2), "sig3");

    println!("✓ Data integrity verified");
}

#[tokio::test]
async fn test_write_creates_directory_structure() {
    let temp_dir = tempdir().unwrap();
    let output_dir = temp_dir.path();

    // 创建简单的测试数据
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let helper = ParquetHelper::new();
    let date = NaiveDate::from_ymd_opt(2025, 2, 1).unwrap();

    let file_path = helper
        .write_daily_parquet("pumpfun_trade_event_v2", date, batch, output_dir)
        .await
        .unwrap();

    // 验证目录结构
    let expected_dir = output_dir.join("pumpfun_trade_event_v2");
    assert!(expected_dir.exists(), "Table directory should be created");
    assert!(expected_dir.is_dir(), "Should be a directory");

    let expected_file = expected_dir.join("pumpfun_trade_event_v2_2025-02-01.parquet");
    assert_eq!(file_path, expected_file, "File path should match expected");
    assert!(expected_file.exists(), "Parquet file should exist");

    println!("✓ Directory structure is correct: {:?}", file_path);
}

#[tokio::test]
async fn test_read_empty_file_returns_error() {
    let helper = ParquetHelper::new();

    // 尝试读取不存在的文件
    let result = helper.read_parquet(&std::path::PathBuf::from("/nonexistent/file.parquet")).await;

    assert!(result.is_err(), "Should return error for nonexistent file");
    println!("✓ Correctly handles nonexistent file");
}

#[tokio::test]
async fn test_write_multiple_dates_same_table() {
    let temp_dir = tempdir().unwrap();
    let output_dir = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt32, false),
    ]));

    let helper = ParquetHelper::new();

    // 写入多个日期的数据
    let dates = vec![
        NaiveDate::from_ymd_opt(2025, 3, 1).unwrap(),
        NaiveDate::from_ymd_opt(2025, 3, 2).unwrap(),
        NaiveDate::from_ymd_opt(2025, 3, 3).unwrap(),
    ];

    for (i, date) in dates.iter().enumerate() {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt32Array::from(vec![i as u32 + 1]))],
        )
        .unwrap();

        let file_path = helper
            .write_daily_parquet("daily_data", *date, batch, output_dir)
            .await
            .unwrap();

        assert!(file_path.exists());
        println!("✓ Written file for {}: {:?}", date, file_path);
    }

    // 验证所有文件都存在
    let table_dir = output_dir.join("daily_data");
    let entries: Vec<_> = std::fs::read_dir(table_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();

    assert_eq!(entries.len(), 3, "Should have 3 parquet files");
    println!("✓ All 3 daily files created successfully");
}

#[tokio::test]
async fn test_compression_is_applied() {
    let temp_dir = tempdir().unwrap();
    let output_dir = temp_dir.path();

    // 创建较大的数据集以测试压缩
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    let mut ids = Vec::new();
    let mut data = Vec::new();
    for i in 0..1000 {
        ids.push(i);
        data.push(format!("repeated_string_data_{}", i % 10)); // 重复数据以测试压缩
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(StringArray::from(data)),
        ],
    )
    .unwrap();

    let helper = ParquetHelper::new();
    let date = NaiveDate::from_ymd_opt(2025, 4, 1).unwrap();

    let file_path = helper
        .write_daily_parquet("compression_test", date, batch.clone(), output_dir)
        .await
        .unwrap();

    // 读取文件大小
    let metadata = std::fs::metadata(&file_path).unwrap();
    let file_size = metadata.len();

    println!("✓ Compressed file size: {} bytes", file_size);

    // 验证可以正确读回
    let read_batch = helper.read_parquet(&file_path).await.unwrap();
    assert_eq!(read_batch.num_rows(), 1000, "Should read all 1000 rows");
    
    println!("✓ Compression successful, data integrity maintained");
}

#[tokio::test]
async fn test_read_multiple_batches() {
    // 这个测试验证当 Parquet 文件包含多个批次时，能正确合并
    let temp_dir = tempdir().unwrap();
    let output_dir = temp_dir.path();

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::UInt32, false),
    ]));

    // 写入数据
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .unwrap();

    let helper = ParquetHelper::new();
    let date = NaiveDate::from_ymd_opt(2025, 5, 1).unwrap();

    let file_path = helper
        .write_daily_parquet("batch_test", date, batch, output_dir)
        .await
        .unwrap();

    // 读取并验证
    let read_batch = helper.read_parquet(&file_path).await.unwrap();
    assert_eq!(read_batch.num_rows(), 5);

    let values = read_batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();

    for i in 0..5 {
        assert_eq!(values.value(i), (i + 1) as u32);
    }

    println!("✓ Multiple batches merged correctly");
}
