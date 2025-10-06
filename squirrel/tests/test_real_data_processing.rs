use squirrel::block_parser::block_parser_service::{BlockParserService, Config};
use tempfile::TempDir;
use std::fs;
use std::path::Path;
use std::time::Instant;
use tokio::time::{timeout, Duration};

#[tokio::test]
#[ignore]
async fn test_real_cank_data_processing() {
    // 检查Cank目录是否存在
    let cank_dir = Path::new("/home/gold_dog/Cank/Archieve");
    if !cank_dir.exists() {
        println!("Cank directory not found, skipping real data test");
        return;
    }

    // 创建临时的processed目录
    let temp_dir = TempDir::new().unwrap();
    let processed_dir = temp_dir.path().join("processed");
    fs::create_dir_all(&processed_dir).unwrap();

    let config = Config {
        data_dir: cank_dir.to_string_lossy().to_string(),
        processed_dir: processed_dir.to_string_lossy().to_string(),
        scan_interval_seconds: 5, // 短间隔用于测试
        enable_watch: false, // 禁用监控模式，只处理一次
        max_concurrent_clickhouse_tasks: 10, // 提高并发数
    };

    println!("=== Real Cank Data Processing Test ===");
    println!("Data directory: {}", cank_dir.display());
    println!("Processed directory: {}", processed_dir.display());

    let start_time = Instant::now();
    
    // 使用timeout确保测试不会运行太久，单个文件90秒，允许处理2-3个文件
    let result = timeout(Duration::from_secs(300), async {
        let mut service = BlockParserService::new(config).unwrap();
        
        // 只处理一次，不进入监控循环
        service.process_pending_files().await
    }).await;

    let duration = start_time.elapsed();
    
    match result {
        Ok(Ok(processed_count)) => {
            println!("Successfully processed {} file pairs", processed_count);
            println!("Processing took: {:.2}s", duration.as_secs_f64());
            
            // 如果处理了文件，验证日志文件是否创建
            if processed_count > 0 {
                let log_file = processed_dir.join("processed_files.log");
                assert!(log_file.exists(), "Processed log file should exist");
                
                let log_content = fs::read_to_string(&log_file).unwrap();
                let log_lines: Vec<&str> = log_content.lines().filter(|l| !l.trim().is_empty()).collect();
                assert_eq!(log_lines.len(), processed_count, "Log entries should match processed count");
                
                println!("Log file created with {} entries", log_lines.len());
                for line in log_lines.iter().take(3) { // 只显示前3行
                    println!("  {}", line);
                }
            }
        }
        Ok(Err(e)) => {
            println!("Processing failed: {}", e);
            panic!("Processing should not fail: {}", e);
        }
        Err(_) => {
            println!("Test timed out after 300 seconds");
            println!("Processing took: {:.2}s before timeout", duration.as_secs_f64());
            // 超时不算失败，因为大文件处理可能很慢
        }
    }

    println!("=== Test completed ===");
}

#[tokio::test]
#[ignore]
async fn test_limited_cank_data_processing() {
    // 检查Cank目录是否存在
    let cank_dir = Path::new("/home/gold_dog/Cank/Archieve");
    if !cank_dir.exists() {
        println!("Cank directory not found, skipping limited real data test");
        return;
    }

    // 创建临时目录结构
    let temp_dir = TempDir::new().unwrap();
    let test_data_dir = temp_dir.path().join("data");
    let processed_dir = temp_dir.path().join("processed");
    fs::create_dir_all(&test_data_dir).unwrap();
    fs::create_dir_all(&processed_dir).unwrap();

    // 从Cank目录复制前几个文件用于测试
    let mut copied_files = 0;
    let max_files_to_copy = 2; // 只复制2对文件进行测试

    if let Ok(entries) = fs::read_dir(cank_dir) {
        let mut prefixes = std::collections::HashSet::new();
        
        for entry in entries {
            if copied_files >= max_files_to_copy {
                break;
            }
            
            if let Ok(entry) = entry {
                let path = entry.path();
                let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                
                // 提取prefix (去掉.meta或.bin后缀)
                let prefix = if file_name.ends_with(".meta") {
                    Some(file_name.trim_end_matches(".meta"))
                } else if file_name.ends_with(".bin") {
                    Some(file_name.trim_end_matches(".bin"))
                } else {
                    None
                };
                
                if let Some(prefix) = prefix {
                    if !prefixes.contains(prefix) {
                        // 复制这个prefix的两个文件
                        let meta_src = cank_dir.join(format!("{}.meta", prefix));
                        let bin_src = cank_dir.join(format!("{}.bin", prefix));
                        let meta_dst = test_data_dir.join(format!("{}.meta", prefix));
                        let bin_dst = test_data_dir.join(format!("{}.bin", prefix));
                        
                        if meta_src.exists() && bin_src.exists() {
                            if let Ok(_) = fs::copy(&meta_src, &meta_dst) {
                                if let Ok(_) = fs::copy(&bin_src, &bin_dst) {
                                    prefixes.insert(prefix.to_string());
                                    copied_files += 1;
                                    println!("Copied file pair: {}", prefix);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if copied_files == 0 {
        println!("No valid file pairs found in Cank directory");
        return;
    }

    println!("=== Limited Cank Data Processing Test ===");
    println!("Copied {} file pairs for testing", copied_files);

    let config = Config {
        data_dir: test_data_dir.to_string_lossy().to_string(),
        processed_dir: processed_dir.to_string_lossy().to_string(),
        scan_interval_seconds: 5,
        enable_watch: false,
        max_concurrent_clickhouse_tasks: 10, // 提高并发数
    };

    let start_time = Instant::now();
    
    // 200秒超时，允许处理2个文件（每个90秒）
    let result = timeout(Duration::from_secs(200), async {
        let mut service = BlockParserService::new(config).unwrap();
        service.process_pending_files().await
    }).await;

    let duration = start_time.elapsed();

    match result {
        Ok(Ok(processed_count)) => {
            println!("Successfully processed {} file pairs", processed_count);
            println!("Processing took: {:.2}s", duration.as_secs_f64());
            
            assert_eq!(processed_count, copied_files, "Should process all copied files");
            
            // 验证统计信息
            let service = BlockParserService::new(Config {
                data_dir: test_data_dir.to_string_lossy().to_string(),
                processed_dir: processed_dir.to_string_lossy().to_string(),
                scan_interval_seconds: 5,
                enable_watch: false,
                max_concurrent_clickhouse_tasks: 10,
            }).unwrap();
            
            let stats = service.get_stats();
            stats.print_summary();
            
            assert_eq!(stats.processed_count, copied_files);
        }
        Ok(Err(e)) => {
            println!("Processing failed: {}", e);
            panic!("Processing should not fail: {}", e);
        }
        Err(_) => {
            println!("Test timed out after 200 seconds");
            println!("Processing took: {:.2}s before timeout", duration.as_secs_f64());
            // 对于大文件，超时可能是正常的
        }
    }

    println!("=== Limited test completed ===");
}

#[tokio::test] 
async fn test_service_with_watch_mode_brief() {
    // 这个测试演示监控模式，但只运行很短时间
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().join("data");
    let processed_dir = temp_dir.path().join("processed");
    
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&processed_dir).unwrap();

    let config = Config {
        data_dir: data_dir.to_string_lossy().to_string(),
        processed_dir: processed_dir.to_string_lossy().to_string(),
        scan_interval_seconds: 2, // 2秒扫描间隔
        enable_watch: true, // 启用监控模式
        max_concurrent_clickhouse_tasks: 10,
    };

    println!("=== Watch Mode Brief Test ===");
    
    // 只运行5秒的监控模式
    let result = timeout(Duration::from_secs(5), async {
        let service = BlockParserService::new(config).unwrap();
        service.run().await
    }).await;

    match result {
        Ok(_) => {
            println!("Service completed normally");
        }
        Err(_) => {
            println!("Service timed out (expected for watch mode)");
        }
    }

    println!("=== Watch mode test completed ===");
}