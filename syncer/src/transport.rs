use std::error::Error;
use std::path::Path;
use crate::config::RemoteServerConfig;
use tokio::process::Command;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// 基于 rsync 的传输器
pub struct RsyncTransport;

impl RsyncTransport {
    pub fn new() -> Self {
        Self
    }

    /// 同步本地目录到远程服务器
    /// 
    /// # Arguments
    /// * `local_dir` - 本地目录路径（表目录）
    /// * `remote_config` - 远程服务器配置
    /// 
    /// # Returns
    /// * `Result<()>` - 传输成功或失败
    pub async fn sync_directory(
        &self,
        local_dir: &Path,
        remote_config: &RemoteServerConfig,
    ) -> Result<()> {
        // 确保本地目录存在
        if !local_dir.exists() {
            return Err(format!("Local directory does not exist: {:?}", local_dir).into());
        }

        // 构建 SSH 选项
        let ssh_opts = format!(
            "ssh -p {} -i {}",
            remote_config.port,
            remote_config.private_key_path.display()
        );

        // 构建源路径（添加尾部斜杠以同步目录内容）
        let local_src = format!("{}/", local_dir.display());

        // 构建目标路径
        let remote_dest = format!(
            "{}@{}:{}",
            remote_config.username,
            remote_config.address,
            remote_config.remote_path.display()
        );

        println!("🚀 Starting rsync transfer...");
        println!("   Source: {}", local_src);
        println!("   Destination: {}", remote_dest);

        // 执行 rsync 命令
        let output = Command::new("rsync")
            .arg("-avz")           // archive, verbose, compress
            .arg("--progress")     // 显示进度
            .arg("--update")       // 跳过接收端更新的文件（断点续传）
            .arg("-e")
            .arg(&ssh_opts)        // SSH 选项
            .arg(&local_src)       // 源路径
            .arg(&remote_dest)     // 目标路径
            .output()
            .await?;

        // 检查退出码
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            
            eprintln!("❌ rsync failed with exit code: {:?}", output.status.code());
            eprintln!("STDOUT:\n{}", stdout);
            eprintln!("STDERR:\n{}", stderr);
            
            return Err(format!(
                "rsync failed: exit code {:?}\nSTDERR: {}",
                output.status.code(),
                stderr
            ).into());
        }

        // 输出成功信息
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("✅ rsync completed successfully");
        
        // 解析并显示传输统计（如果有）
        if let Some(stats_line) = stdout.lines().find(|line| line.contains("sent") || line.contains("total size")) {
            println!("   📊 {}", stats_line.trim());
        }

        Ok(())
    }
}

impl Default for RsyncTransport {
    fn default() -> Self {
        Self::new()
    }
}
