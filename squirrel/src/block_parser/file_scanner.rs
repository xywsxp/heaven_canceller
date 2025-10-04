use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct FilePair {
    pub prefix: String,
    pub meta_path: PathBuf,
    pub bin_path: PathBuf,
}

pub struct FileScanner {
    data_dir: PathBuf,
}

impl FileScanner {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// 扫描数据目录，返回所有可用的文件对
    /// 按slot起始编号降序排序（新slot优先）
    pub fn scan_available_files(&self) -> Result<Vec<FilePair>, Box<dyn std::error::Error>> {
        let mut file_pairs = Vec::new();
        let mut meta_files = HashMap::new();
        let mut bin_files = HashMap::new();

        // 读取目录中的所有文件
        let entries = fs::read_dir(&self.data_dir)?;
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if !path.is_file() {
                continue;
            }

            let file_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            // 解析文件名，提取prefix
            if let Some(prefix) = self.extract_prefix_from_filename(file_name) {
                if file_name.ends_with(".meta") {
                    meta_files.insert(prefix, path);
                } else if file_name.ends_with(".bin") {
                    bin_files.insert(prefix, path);
                }
            }
        }

        // 匹配meta和bin文件对
        for (prefix, meta_path) in meta_files {
            if let Some(bin_path) = bin_files.get(&prefix) {
                file_pairs.push(FilePair {
                    prefix: prefix.clone(),
                    meta_path,
                    bin_path: bin_path.clone(),
                });
            }
        }

        // 按slot起始编号降序排序（新slot优先）
        file_pairs.sort_by(|a, b| {
            let a_start = self.extract_start_slot(&a.prefix).unwrap_or(0);
            let b_start = self.extract_start_slot(&b.prefix).unwrap_or(0);
            b_start.cmp(&a_start)
        });

        Ok(file_pairs)
    }

    /// 从文件名中提取prefix
    /// 例如: "123_456.meta" -> Some("123_456")
    ///      "123_456.bin" -> Some("123_456")  
    ///      "invalid.txt" -> None
    ///      ".meta" -> None (空prefix)
    fn extract_prefix_from_filename(&self, filename: &str) -> Option<String> {
        if filename.ends_with(".meta") {
            let prefix = filename.trim_end_matches(".meta");
            if prefix.is_empty() {
                None
            } else {
                Some(prefix.to_string())
            }
        } else if filename.ends_with(".bin") {
            let prefix = filename.trim_end_matches(".bin");
            if prefix.is_empty() {
                None
            } else {
                Some(prefix.to_string())
            }
        } else {
            None
        }
    }

    /// 从prefix中提取起始slot编号
    /// 例如: "123_456" -> Some(123)
    ///      "invalid" -> None
    fn extract_start_slot(&self, prefix: &str) -> Option<u64> {
        prefix.split('_').next()?.parse::<u64>().ok()
    }

    /// 检查给定的prefix是否有完整的文件对
    pub fn has_complete_file_pair(&self, prefix: &str) -> bool {
        let meta_path = self.data_dir.join(format!("{}.meta", prefix));
        let bin_path = self.data_dir.join(format!("{}.bin", prefix));
        
        meta_path.exists() && meta_path.is_file() && 
        bin_path.exists() && bin_path.is_file()
    }

    /// 获取指定prefix的文件对路径
    pub fn get_file_pair_paths(&self, prefix: &str) -> (PathBuf, PathBuf) {
        let meta_path = self.data_dir.join(format!("{}.meta", prefix));
        let bin_path = self.data_dir.join(format!("{}.bin", prefix));
        (meta_path, bin_path)
    }
}
