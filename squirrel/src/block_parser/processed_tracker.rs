use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write, BufWriter};
use std::path::PathBuf;
use chrono::{DateTime, Utc};

pub struct ProcessedTracker {
    log_path: PathBuf,
    processed_set: HashSet<String>,
}

impl ProcessedTracker {
    pub fn new(processed_dir: PathBuf) -> Self {
        let log_path = processed_dir.join("processed_files.log");
        Self {
            log_path,
            processed_set: HashSet::new(),
        }
    }

    /// 从日志文件加载已处理的文件列表
    pub fn load_processed_list(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.processed_set.clear();

        // 如果日志文件不存在，就创建空的集合
        if !self.log_path.exists() {
            return Ok(());
        }

        let file = File::open(&self.log_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            
            if line.is_empty() || line.starts_with('#') {
                continue; // 跳过空行和注释行
            }

            // 解析日志行格式: timestamp,prefix,status
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() >= 3 && parts[2] == "completed" {
                self.processed_set.insert(parts[1].to_string());
            }
        }

        Ok(())
    }

    /// 检查文件是否已处理
    pub fn is_processed(&self, prefix: &str) -> bool {
        self.processed_set.contains(prefix)
    }

    /// 标记文件为已处理
    pub fn mark_as_processed(&mut self, prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
        // 确保processed目录存在
        if let Some(parent) = self.log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // 追加写入日志文件
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;

        let timestamp = Utc::now().to_rfc3339();
        writeln!(file, "{},{},completed", timestamp, prefix)?;

        // 同时更新内存中的集合
        self.processed_set.insert(prefix.to_string());

        Ok(())
    }

    /// 获取已处理文件的数量
    pub fn processed_count(&self) -> usize {
        self.processed_set.len()
    }

    /// 获取所有已处理的prefix列表（按字母序排序）
    pub fn get_processed_prefixes(&self) -> Vec<String> {
        let mut prefixes: Vec<String> = self.processed_set.iter().cloned().collect();
        prefixes.sort();
        prefixes
    }

    /// 批量标记多个文件为已处理
    pub fn mark_batch_as_processed(&mut self, prefixes: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        // 确保processed目录存在
        if let Some(parent) = self.log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        
        let mut writer = BufWriter::new(file);
        let timestamp = Utc::now().to_rfc3339();

        for prefix in prefixes {
            writeln!(writer, "{},{},completed", timestamp, prefix)?;
            self.processed_set.insert(prefix.clone());
        }

        writer.flush()?;
        Ok(())
    }

    /// 清理日志文件中的重复条目（保留最新的状态）
    pub fn cleanup_log(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.log_path.exists() {
            return Ok(());
        }

        // 读取所有日志条目
        let file = File::open(&self.log_path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            
            if line.is_empty() || line.starts_with('#') {
                entries.push(line.to_string());
                continue;
            }

            entries.push(line.to_string());
        }

        // 重新创建日志文件，只保留每个prefix的最新条目
        let temp_path = self.log_path.with_extension("log.tmp");
        let mut temp_file = File::create(&temp_path)?;
        let mut seen_prefixes = HashSet::new();

        // 从后往前遍历，只保留每个prefix的第一次出现（最新的）
        for entry in entries.iter().rev() {
            if entry.is_empty() || entry.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = entry.split(',').collect();
            if parts.len() >= 2 {
                let prefix = parts[1];
                if !seen_prefixes.contains(prefix) {
                    seen_prefixes.insert(prefix);
                }
            }
        }

        // 正向写入需要保留的条目
        for entry in &entries {
            if entry.is_empty() || entry.starts_with('#') {
                writeln!(temp_file, "{}", entry)?;
                continue;
            }

            let parts: Vec<&str> = entry.split(',').collect();
            if parts.len() >= 2 && seen_prefixes.contains(parts[1]) {
                writeln!(temp_file, "{}", entry)?;
                seen_prefixes.remove(parts[1]); // 确保只写入一次
            }
        }

        // 替换原文件
        std::fs::rename(temp_path, &self.log_path)?;
        Ok(())
    }
}
