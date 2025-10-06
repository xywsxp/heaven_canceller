use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// Parquet 文件助手（读写）
pub struct ParquetHelper;

impl ParquetHelper {
    pub fn new() -> Self {
        Self
    }

    /// 将 RecordBatch 写入 Parquet 文件
    /// 
    /// # Arguments
    /// * `table` - 表名
    /// * `date` - 日期
    /// * `batch` - Arrow RecordBatch 数据
    /// * `output_dir` - 输出目录
    /// 
    /// # Returns
    /// * `PathBuf` - 生成的文件路径
    pub async fn write_daily_parquet(
        &self,
        table: &str,
        date: NaiveDate,
        batch: RecordBatch,
        output_dir: &Path,
    ) -> Result<PathBuf> {
        // 创建表目录: output_dir/table/
        let table_dir = output_dir.join(table);
        fs::create_dir_all(&table_dir)?;

        // 生成文件名: {table}_{YYYY-MM-DD}.parquet
        let filename = format!("{}_{}.parquet", table, date.format("%Y-%m-%d"));
        let file_path = table_dir.join(&filename);

        // 配置 Snappy 压缩
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        // 写入 Parquet 文件
        let file = File::create(&file_path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(file_path)
    }

    /// 从 Parquet 文件读取数据
    /// 
    /// # Arguments
    /// * `file_path` - Parquet 文件路径
    /// 
    /// # Returns
    /// * `RecordBatch` - Arrow RecordBatch 数据（所有行合并）
    pub async fn read_parquet(&self, file_path: &Path) -> Result<RecordBatch> {
        // 打开 Parquet 文件
        let file = File::open(file_path)?;

        // 创建 reader
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.build()?;

        // 读取所有批次并合并（天级别数据，全加载）
        let mut batches = Vec::new();
        while let Some(batch) = reader.next() {
            batches.push(batch?);
        }

        // 如果没有数据，返回空的 RecordBatch
        if batches.is_empty() {
            return Err("Parquet file is empty".into());
        }

        // 如果只有一个批次，直接返回
        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        // 合并多个批次
        let schema = batches[0].schema();
        let merged = arrow::compute::concat_batches(&schema, &batches)?;

        Ok(merged)
    }
}

impl Default for ParquetHelper {
    fn default() -> Self {
        Self::new()
    }
}
