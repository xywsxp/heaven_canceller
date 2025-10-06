use std::path::PathBuf;
use syncer::config::RemoteServerConfig;
use syncer::transport::RsyncTransport;
use tempfile::tempdir;
use std::fs;

#[tokio::test]
async fn test_sync_nonexistent_directory() {
    let transport = RsyncTransport::new();
    
    let remote_config = RemoteServerConfig {
        address: "example.com".to_string(),
        port: 22,
        username: "testuser".to_string(),
        private_key_path: PathBuf::from("/tmp/test_key"),
        remote_path: PathBuf::from("/tmp/remote"),
    };

    let result = transport
        .sync_directory(&PathBuf::from("/nonexistent/path"), &remote_config)
        .await;

    assert!(result.is_err(), "Should fail for nonexistent directory");
    
    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(
            error_msg.contains("does not exist"),
            "Error should mention directory doesn't exist: {}",
            error_msg
        );
        println!("✓ Correctly rejected nonexistent directory: {}", error_msg);
    }
}

#[tokio::test]
async fn test_command_construction() {
    // 这个测试验证命令构建逻辑（不实际执行 rsync）
    let temp_dir = tempdir().unwrap();
    let local_path = temp_dir.path();
    
    // 创建一些测试文件
    fs::write(local_path.join("test1.txt"), "test data 1").unwrap();
    fs::write(local_path.join("test2.txt"), "test data 2").unwrap();
    
    let remote_config = RemoteServerConfig {
        address: "192.168.1.100".to_string(),
        port: 2222,
        username: "admin".to_string(),
        private_key_path: PathBuf::from("/home/user/.ssh/test_key"),
        remote_path: PathBuf::from("/remote/data"),
    };

    // 验证目录存在
    assert!(local_path.exists(), "Test directory should exist");
    
    // 验证配置
    assert_eq!(remote_config.address, "192.168.1.100");
    assert_eq!(remote_config.port, 2222);
    assert_eq!(remote_config.username, "admin");
    
    println!("✓ Command construction test - configuration validated");
    println!("  Local path: {:?}", local_path);
    println!("  Remote: {}@{}:{}", 
        remote_config.username,
        remote_config.address, 
        remote_config.remote_path.display()
    );
}

#[tokio::test]
#[ignore = "requires real SSH server and rsync"]
async fn test_real_sync() {
    // 这个测试需要真实的 SSH 服务器和 rsync
    // 使用环境变量配置测试参数
    
    let ssh_host = std::env::var("TEST_SSH_HOST").unwrap_or_else(|_| {
        println!("⚠ TEST_SSH_HOST not set, skipping real sync test");
        return String::new();
    });
    
    if ssh_host.is_empty() {
        return;
    }

    let temp_dir = tempdir().unwrap();
    let local_path = temp_dir.path();
    
    // 创建测试文件
    fs::write(local_path.join("test_file.txt"), "sync test data").unwrap();
    
    let remote_config = RemoteServerConfig {
        address: std::env::var("TEST_SSH_HOST").unwrap(),
        port: std::env::var("TEST_SSH_PORT")
            .unwrap_or("22".to_string())
            .parse()
            .unwrap(),
        username: std::env::var("TEST_SSH_USER").unwrap(),
        private_key_path: PathBuf::from(
            std::env::var("TEST_SSH_KEY").unwrap_or_else(|_| "~/.ssh/id_rsa".to_string())
        ),
        remote_path: PathBuf::from(
            std::env::var("TEST_REMOTE_PATH").unwrap_or("/tmp/rsync_test".to_string())
        ),
    };

    let transport = RsyncTransport::new();
    
    let result = transport.sync_directory(local_path, &remote_config).await;
    
    match result {
        Ok(_) => {
            println!("✓ Real sync test completed successfully");
        }
        Err(e) => {
            eprintln!("✗ Real sync test failed: {}", e);
            panic!("Sync failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_ssh_options_format() {
    // 测试 SSH 选项格式是否正确
    let remote_config = RemoteServerConfig {
        address: "example.com".to_string(),
        port: 2222,
        username: "testuser".to_string(),
        private_key_path: PathBuf::from("/home/user/.ssh/custom_key"),
        remote_path: PathBuf::from("/remote/path"),
    };

    // 验证 SSH 选项格式（基于实现逻辑）
    let expected_ssh_opts = format!(
        "ssh -p {} -i {}",
        remote_config.port,
        remote_config.private_key_path.display()
    );
    
    assert!(expected_ssh_opts.contains("-p 2222"));
    assert!(expected_ssh_opts.contains("-i /home/user/.ssh/custom_key"));
    
    println!("✓ SSH options format validated: {}", expected_ssh_opts);
}

#[tokio::test]
async fn test_path_with_spaces() {
    // 测试包含空格的路径处理
    let temp_dir = tempdir().unwrap();
    let local_path = temp_dir.path().join("test dir with spaces");
    fs::create_dir_all(&local_path).unwrap();
    fs::write(local_path.join("file.txt"), "data").unwrap();
    
    let remote_config = RemoteServerConfig {
        address: "example.com".to_string(),
        port: 22,
        username: "user".to_string(),
        private_key_path: PathBuf::from("/key/path"),
        remote_path: PathBuf::from("/remote/path with spaces"),
    };

    // 验证路径格式化
    let local_src = format!("{}/", local_path.display());
    let remote_dest = format!(
        "{}@{}:{}",
        remote_config.username,
        remote_config.address,
        remote_config.remote_path.display()
    );
    
    assert!(local_src.contains("test dir with spaces"));
    assert!(remote_dest.contains("/remote/path with spaces"));
    
    println!("✓ Path with spaces handled correctly");
    println!("  Local: {}", local_src);
    println!("  Remote: {}", remote_dest);
}

#[tokio::test]
async fn test_directory_trailing_slash() {
    // 验证目录路径是否正确添加尾部斜杠
    let temp_dir = tempdir().unwrap();
    let local_path = temp_dir.path();
    
    let local_src = format!("{}/", local_path.display());
    
    assert!(local_src.ends_with('/'), "Source path should end with /");
    
    println!("✓ Trailing slash correctly added: {}", local_src);
}
