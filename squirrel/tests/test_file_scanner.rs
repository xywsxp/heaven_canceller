use squirrel::block_parser::file_scanner::FileScanner;
use std::fs::File;
use tempfile::TempDir;

#[test]
fn test_file_scanner_basic_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = FileScanner::new(temp_dir.path().to_path_buf());

    // 测试空目录
    let result = scanner.scan_available_files().unwrap();
    assert_eq!(result.len(), 0);

    // 创建一些测试文件，注意slot编号顺序
    File::create(temp_dir.path().join("100_200.meta")).unwrap();
    File::create(temp_dir.path().join("100_200.bin")).unwrap();
    
    File::create(temp_dir.path().join("300_400.meta")).unwrap();
    File::create(temp_dir.path().join("300_400.bin")).unwrap();

    let result = scanner.scan_available_files().unwrap();
    assert_eq!(result.len(), 2);

    // 验证按slot起始编号降序排序（新slot优先）
    assert_eq!(result[0].prefix, "300_400"); // 300 > 100
    assert_eq!(result[1].prefix, "100_200");
}

#[test]
fn test_file_scanner_incomplete_pairs() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = FileScanner::new(temp_dir.path().to_path_buf());

    // 创建不完整的文件对
    File::create(temp_dir.path().join("500_600.meta")).unwrap();
    // 缺少 500_600.bin
    
    File::create(temp_dir.path().join("700_800.bin")).unwrap();
    // 缺少 700_800.meta

    // 创建完整的文件对
    File::create(temp_dir.path().join("900_1000.meta")).unwrap();
    File::create(temp_dir.path().join("900_1000.bin")).unwrap();

    let result = scanner.scan_available_files().unwrap();
    
    // 只应该返回完整的文件对
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].prefix, "900_1000");
}

#[test]
fn test_file_scanner_invalid_filenames() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = FileScanner::new(temp_dir.path().to_path_buf());

    // 创建各种无效的文件名
    File::create(temp_dir.path().join("invalid.txt")).unwrap();
    File::create(temp_dir.path().join("no_extension")).unwrap();
    File::create(temp_dir.path().join("wrong.extension")).unwrap();
    File::create(temp_dir.path().join(".meta")).unwrap(); // 空prefix
    File::create(temp_dir.path().join(".bin")).unwrap(); // 空prefix

    // 创建有效的文件对
    File::create(temp_dir.path().join("valid_123.meta")).unwrap();
    File::create(temp_dir.path().join("valid_123.bin")).unwrap();

    let result = scanner.scan_available_files().unwrap();
    
    // 只应该返回有效的文件对
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].prefix, "valid_123");
}

#[test]
fn test_get_file_pair_paths() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = FileScanner::new(temp_dir.path().to_path_buf());

    let (meta_path, bin_path) = scanner.get_file_pair_paths("test_prefix");
    
    assert_eq!(meta_path, temp_dir.path().join("test_prefix.meta"));
    assert_eq!(bin_path, temp_dir.path().join("test_prefix.bin"));
}

#[test]
fn test_has_complete_file_pair_edge_cases() {
    let temp_dir = TempDir::new().unwrap();
    let scanner = FileScanner::new(temp_dir.path().to_path_buf());

    // 测试不存在的文件
    assert!(!scanner.has_complete_file_pair("nonexistent"));

    // 创建目录而不是文件（边界情况）
    std::fs::create_dir_all(temp_dir.path().join("dir_test.meta")).unwrap();
    std::fs::create_dir_all(temp_dir.path().join("dir_test.bin")).unwrap();
    
    // 应该返回false，因为是目录不是文件
    assert!(!scanner.has_complete_file_pair("dir_test"));
}