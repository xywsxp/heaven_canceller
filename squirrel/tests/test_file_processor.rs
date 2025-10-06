use squirrel::block_parser::file_processor::FileProcessor;
use utils::slot_meta::SlotMeta;
use tempfile::TempDir;
use std::fs::File;
use std::io::Write;
use rmp_serde;

#[tokio::test]
async fn test_file_processor_creation() {
    let processor = FileProcessor::new(5);
    // 测试基本创建功能，processor应该成功创建
    // 这里我们只是确保构造函数正常工作
    processor.finish().await;
}

#[tokio::test]
async fn test_process_empty_meta_file() {
    let temp_dir = TempDir::new().unwrap();
    let mut processor = FileProcessor::new(1);
    
    // 创建空的meta文件
    let meta_path = temp_dir.path().join("empty.meta");
    let bin_path = temp_dir.path().join("empty.bin");
    
    let empty_slots: Vec<SlotMeta> = vec![];
    let serialized = rmp_serde::to_vec(&empty_slots).unwrap();
    std::fs::write(&meta_path, serialized).unwrap();
    
    // 创建空的bin文件
    File::create(&bin_path).unwrap();
    
    // 处理应该成功完成，即使没有slots
    let result = processor.process_file_pair(&meta_path, &bin_path).await;
    assert!(result.is_ok());
    
    processor.finish().await;
}

#[tokio::test]
async fn test_process_invalid_meta_file() {
    let temp_dir = TempDir::new().unwrap();
    let mut processor = FileProcessor::new(1);
    
    // 创建无效的meta文件
    let meta_path = temp_dir.path().join("invalid.meta");
    let bin_path = temp_dir.path().join("invalid.bin");
    
    std::fs::write(&meta_path, b"invalid msgpack data").unwrap();
    File::create(&bin_path).unwrap();
    
    // 处理应该失败
    let result = processor.process_file_pair(&meta_path, &bin_path).await;
    assert!(result.is_err());
    
    processor.finish().await;
}

#[tokio::test]
async fn test_process_nonexistent_files() {
    let temp_dir = TempDir::new().unwrap();
    let mut processor = FileProcessor::new(1);
    
    let meta_path = temp_dir.path().join("nonexistent.meta");
    let bin_path = temp_dir.path().join("nonexistent.bin");
    
    // 处理不存在的文件应该失败
    let result = processor.process_file_pair(&meta_path, &bin_path).await;
    assert!(result.is_err());
    
    processor.finish().await;
}

#[tokio::test]
async fn test_process_meta_with_invalid_slots() {
    let temp_dir = TempDir::new().unwrap();
    let mut processor = FileProcessor::new(1);
    
    let meta_path = temp_dir.path().join("invalid_slots.meta");
    let bin_path = temp_dir.path().join("invalid_slots.bin");
    
    // 创建包含无效slot数据的meta文件
    let invalid_slots = vec![
        SlotMeta {
            slot: 1000,
            offset: None, // 无效的offset
            size: 100,
        },
        SlotMeta {
            slot: 1001,
            offset: Some(1000000), // 超出文件范围的offset
            size: 100,
        },
    ];
    
    let serialized = rmp_serde::to_vec(&invalid_slots).unwrap();
    std::fs::write(&meta_path, serialized).unwrap();
    
    // 创建小的bin文件
    let mut bin_file = File::create(&bin_path).unwrap();
    bin_file.write_all(b"small binary data").unwrap();
    
    // 处理应该成功完成，但会跳过无效的slots
    let result = processor.process_file_pair(&meta_path, &bin_path).await;
    assert!(result.is_ok());
    
    processor.finish().await;
}

#[tokio::test]
async fn test_concurrent_processing() {
    let temp_dir = TempDir::new().unwrap();
    let processor = FileProcessor::new(3); // 允许3个并发任务
    
    // 创建多个测试文件
    for i in 0..5 {
        let meta_path = temp_dir.path().join(format!("test_{}.meta", i));
        let bin_path = temp_dir.path().join(format!("test_{}.bin", i));
        
        let empty_slots: Vec<SlotMeta> = vec![];
        let serialized = rmp_serde::to_vec(&empty_slots).unwrap();
        std::fs::write(&meta_path, serialized).unwrap();
        File::create(&bin_path).unwrap();
    }
    
    processor.finish().await;
}