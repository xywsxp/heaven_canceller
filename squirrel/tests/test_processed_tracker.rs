use squirrel::block_parser::processed_tracker::ProcessedTracker;
use tempfile::TempDir;
use std::fs;
use std::io::Write;

#[test]
fn test_processed_tracker_persistence() {
    let temp_dir = TempDir::new().unwrap();
    
    // 第一个tracker实例
    {
        let mut tracker1 = ProcessedTracker::new(temp_dir.path().to_path_buf());
        tracker1.mark_as_processed("persistent_001").unwrap();
        tracker1.mark_as_processed("persistent_002").unwrap();
        
        assert_eq!(tracker1.processed_count(), 2);
    }
    
    // 第二个tracker实例，应该能加载之前的数据
    {
        let mut tracker2 = ProcessedTracker::new(temp_dir.path().to_path_buf());
        assert_eq!(tracker2.processed_count(), 0); // 加载前为空
        
        tracker2.load_processed_list().unwrap();
        assert_eq!(tracker2.processed_count(), 2); // 加载后应该有数据
        assert!(tracker2.is_processed("persistent_001"));
        assert!(tracker2.is_processed("persistent_002"));
    }
}

#[test]
fn test_processed_tracker_empty_log_file() {
    let temp_dir = TempDir::new().unwrap();
    let mut tracker = ProcessedTracker::new(temp_dir.path().to_path_buf());
    
    // 在没有日志文件的情况下加载
    tracker.load_processed_list().unwrap();
    assert_eq!(tracker.processed_count(), 0);
}

#[test]
fn test_processed_tracker_corrupted_log_entries() {
    let temp_dir = TempDir::new().unwrap();
    let log_path = temp_dir.path().join("processed_files.log");
    
    // 创建包含各种格式的日志文件
    let mut file = fs::File::create(&log_path).unwrap();
    writeln!(file, "# This is a comment").unwrap();
    writeln!(file, "").unwrap(); // 空行
    writeln!(file, "2025-01-01T00:00:00Z,valid_001,completed").unwrap();
    writeln!(file, "invalid_line_without_commas").unwrap();
    writeln!(file, "2025-01-01T00:00:00Z,valid_002").unwrap(); // 缺少状态
    writeln!(file, "2025-01-01T00:00:00Z,failed_003,failed").unwrap(); // 非completed状态
    writeln!(file, "2025-01-01T00:00:00Z,valid_004,completed").unwrap();
    
    let mut tracker = ProcessedTracker::new(temp_dir.path().to_path_buf());
    tracker.load_processed_list().unwrap();
    
    // 只有valid_001和valid_004应该被加载
    assert_eq!(tracker.processed_count(), 2);
    assert!(tracker.is_processed("valid_001"));
    assert!(tracker.is_processed("valid_004"));
    assert!(!tracker.is_processed("valid_002"));
    assert!(!tracker.is_processed("failed_003"));
}

#[test]
fn test_get_processed_prefixes_sorted() {
    let temp_dir = TempDir::new().unwrap();
    let mut tracker = ProcessedTracker::new(temp_dir.path().to_path_buf());
    
    // 以非排序顺序添加
    tracker.mark_as_processed("zzz_last").unwrap();
    tracker.mark_as_processed("aaa_first").unwrap();
    tracker.mark_as_processed("mmm_middle").unwrap();
    
    let prefixes = tracker.get_processed_prefixes();
    assert_eq!(prefixes, vec!["aaa_first", "mmm_middle", "zzz_last"]);
}

#[test]
fn test_cleanup_log_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let mut tracker = ProcessedTracker::new(temp_dir.path().to_path_buf());
    
    // 添加一些重复的条目
    tracker.mark_as_processed("duplicate_001").unwrap();
    tracker.mark_as_processed("duplicate_001").unwrap(); // 重复
    tracker.mark_as_processed("unique_002").unwrap();
    tracker.mark_as_processed("duplicate_001").unwrap(); // 再次重复
    
    // 清理前应该有重复
    assert_eq!(tracker.processed_count(), 2); // 内存中去重了
    
    // 清理日志文件
    tracker.cleanup_log().unwrap();
    
    // 重新加载验证清理效果
    let mut new_tracker = ProcessedTracker::new(temp_dir.path().to_path_buf());
    new_tracker.load_processed_list().unwrap();
    
    assert_eq!(new_tracker.processed_count(), 2);
    assert!(new_tracker.is_processed("duplicate_001"));
    assert!(new_tracker.is_processed("unique_002"));
}

#[test]
fn test_cleanup_log_with_nonexistent_file() {
    let temp_dir = TempDir::new().unwrap();
    let tracker = ProcessedTracker::new(temp_dir.path().to_path_buf());
    
    // 在没有日志文件的情况下清理应该成功
    tracker.cleanup_log().unwrap();
}

#[test]
fn test_mark_batch_as_processed() {
    let temp_dir = TempDir::new().unwrap();
    let mut tracker = ProcessedTracker::new(temp_dir.path().to_path_buf());
    
    let batch = vec![
        "batch_alpha".to_string(),
        "batch_beta".to_string(),
        "batch_gamma".to_string(),
    ];
    
    tracker.mark_batch_as_processed(&batch).unwrap();
    
    assert_eq!(tracker.processed_count(), 3);
    for prefix in &batch {
        assert!(tracker.is_processed(prefix));
    }
    
    // 验证持久化
    let mut new_tracker = ProcessedTracker::new(temp_dir.path().to_path_buf());
    new_tracker.load_processed_list().unwrap();
    
    assert_eq!(new_tracker.processed_count(), 3);
    for prefix in &batch {
        assert!(new_tracker.is_processed(prefix));
    }
}