use chrono::{Duration, NaiveDateTime, Utc};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;

use crate::sync_config::SyncConfig;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// 小时级对比结果
#[derive(Debug, Row, Serialize, Deserialize)]
struct HourCount {
    hour: u32,  // Unix timestamp
    unique_count: u64,
}

/// 分钟级对比结果
#[derive(Debug, Row, Serialize, Deserialize)]
struct MinuteCount {
    minute: u32,  // Unix timestamp
    unique_count: u64,
}

/// 同步统计信息
#[derive(Debug, Default)]
pub struct SyncStats {
    pub total_tables: usize,
    pub diff_hours: usize,
    pub diff_minutes: usize,
    pub synced_records: u64,
    pub errors: Vec<String>,
}

impl SyncStats {
    pub fn print_summary(&self) {
        println!("\n📊 Sync Summary:");
        println!("   Total tables checked: {}", self.total_tables);
        println!("   Hours with differences: {}", self.diff_hours);
        println!("   Minutes synced: {}", self.diff_minutes);
        println!("   Total records synced: {}", self.synced_records);
        
        if !self.errors.is_empty() {
            println!("   ⚠️  Errors: {}", self.errors.len());
            for error in &self.errors {
                println!("      - {}", error);
            }
        } else {
            println!("   ✅ No errors");
        }
    }
}

/// 同步检查器
pub struct SyncChecker {
    local_client: Client,
    remote_client: Client,
    config: SyncConfig,
}

impl SyncChecker {
    /// 创建新的同步检查器
    pub fn new(config: SyncConfig) -> Self {
        // 创建本地客户端
        let local_client = Client::default()
            .with_url(&config.local_url)
            .with_user(&config.local_user)
            .with_database(&config.local_database)
            .with_password(&config.local_password);

        // 创建远程客户端
        let remote_client = Client::default()
            .with_url(&config.remote_url)
            .with_user(&config.remote_user)
            .with_database(&config.remote_database)
            .with_password(&config.remote_password);

        Self {
            local_client,
            remote_client,
            config,
        }
    }

    /// 主入口：检查并同步所有表
    pub async fn check_and_sync(&self) -> Result<SyncStats> {
        let mut stats = SyncStats::default();
        let (start_time, end_time) = self.calculate_time_range();

        println!("🚀 Starting Sync Checker");
        println!("   Time range: {} to {}", start_time, end_time);
        println!("   Tables to check: {}", self.config.table_mappings.len());
        println!();

        stats.total_tables = self.config.table_mappings.len();

        // 遍历所有表映射
        for (local_table, remote_table) in &self.config.table_mappings {
            println!("🔍 Checking: {} -> {}", local_table, remote_table);

            // 1. 小时级对比
            match self
                .compare_hourly(local_table, remote_table, start_time, end_time)
                .await
            {
                Ok(diff_hours) => {
                    if diff_hours.is_empty() {
                        println!("   ✅ No differences found");
                        continue;
                    }

                    println!("   ⚠️  Found {} hours with differences", diff_hours.len());
                    stats.diff_hours += diff_hours.len();

                    // 2. 对每个有差异的小时，进行分钟级对比和同步
                    for hour_ts in diff_hours {
                        let hour_start = chrono::DateTime::from_timestamp(hour_ts as i64, 0)
                            .unwrap()
                            .naive_utc();
                        let hour_end = hour_start + Duration::hours(1);

                        match self
                            .compare_and_sync_minutely(
                                local_table,
                                remote_table,
                                hour_start,
                                hour_end,
                                &mut stats,
                            )
                            .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                let error_msg =
                                    format!("{} -> {}: hour {}: {}", local_table, remote_table, hour_start, e);
                                stats.errors.push(error_msg.clone());
                                eprintln!("      ✗ Error: {}", error_msg);
                            }
                        }
                    }
                }
                Err(e) => {
                    let error_msg = format!("{} -> {}: {}", local_table, remote_table, e);
                    stats.errors.push(error_msg.clone());
                    eprintln!("   ✗ Error comparing hours: {}", error_msg);
                }
            }

            println!();
        }

        Ok(stats)
    }

    /// 计算时间范围：now() - lag_hours 到 check_days 天前
    fn calculate_time_range(&self) -> (NaiveDateTime, NaiveDateTime) {
        let now = Utc::now();
        let end_time = (now - Duration::hours(self.config.lag_hours as i64)).naive_utc();
        let start_time = (end_time.and_utc() - Duration::days(self.config.check_days as i64)).naive_utc();
        (start_time, end_time)
    }

    /// 小时级对比，返回有差异的小时（Unix timestamp）
    async fn compare_hourly(
        &self,
        local_table: &str,
        remote_table: &str,
        start_time: NaiveDateTime,
        end_time: NaiveDateTime,
    ) -> Result<Vec<u32>> {
        let start_ts = start_time.and_utc().timestamp() as u32;
        let end_ts = end_time.and_utc().timestamp() as u32;

        // 查询本地小时级统计
        let query = format!(
            "SELECT 
                toUnixTimestamp(toStartOfHour(toDateTime(timestamp))) as hour,
                uniqExact(tuple(signature, instruction_index)) as unique_count
            FROM {}
            WHERE timestamp >= {} AND timestamp < {}
            GROUP BY hour
            ORDER BY hour",
            local_table, start_ts, end_ts
        );

        let local_counts: Vec<HourCount> = self.local_client.query(&query).fetch_all().await?;

        // 查询远程小时级统计
        let query = format!(
            "SELECT 
                toUnixTimestamp(toStartOfHour(toDateTime(timestamp))) as hour,
                uniqExact(tuple(signature, instruction_index)) as unique_count
            FROM {}
            WHERE timestamp >= {} AND timestamp < {}
            GROUP BY hour
            ORDER BY hour",
            remote_table, start_ts, end_ts
        );

        let remote_counts: Vec<HourCount> = self.remote_client.query(&query).fetch_all().await?;

        // 转换为 HashMap 便于对比
        let mut remote_map: HashMap<u32, u64> = remote_counts
            .into_iter()
            .map(|h| (h.hour, h.unique_count))
            .collect();

        // 找出有差异的小时
        let mut diff_hours = Vec::new();
        for local in local_counts {
            let remote_count = remote_map.remove(&local.hour).unwrap_or(0);
            if local.unique_count != remote_count {
                diff_hours.push(local.hour);
            }
        }

        // 远程有但本地没有的小时（理论上不应该发生，因为本地是完整的）
        for (hour, _) in remote_map {
            if !diff_hours.contains(&hour) {
                diff_hours.push(hour);
            }
        }

        diff_hours.sort();
        Ok(diff_hours)
    }

    /// 分钟级对比并同步
    async fn compare_and_sync_minutely(
        &self,
        local_table: &str,
        remote_table: &str,
        hour_start: NaiveDateTime,
        hour_end: NaiveDateTime,
        stats: &mut SyncStats,
    ) -> Result<()> {
        let start_ts = hour_start.and_utc().timestamp() as u32;
        let end_ts = hour_end.and_utc().timestamp() as u32;

        println!(
            "      📅 Processing hour: {}",
            hour_start.format("%Y-%m-%d %H:00")
        );

        // 查询本地分钟级统计
        let query = format!(
            "SELECT 
                toUnixTimestamp(toStartOfMinute(toDateTime(timestamp))) as minute,
                uniqExact(tuple(signature, instruction_index)) as unique_count
            FROM {}
            WHERE timestamp >= {} AND timestamp < {}
            GROUP BY minute
            ORDER BY minute",
            local_table, start_ts, end_ts
        );

        let local_counts: Vec<MinuteCount> = self.local_client.query(&query).fetch_all().await?;

        // 查询远程分钟级统计
        let query = format!(
            "SELECT 
                toUnixTimestamp(toStartOfMinute(toDateTime(timestamp))) as minute,
                uniqExact(tuple(signature, instruction_index)) as unique_count
            FROM {}
            WHERE timestamp >= {} AND timestamp < {}
            GROUP BY minute
            ORDER BY minute",
            remote_table, start_ts, end_ts
        );

        let remote_counts: Vec<MinuteCount> = self.remote_client.query(&query).fetch_all().await?;

        // 转换为 HashMap 便于对比
        let mut remote_map: HashMap<u32, u64> = remote_counts
            .into_iter()
            .map(|m| (m.minute, m.unique_count))
            .collect();

        // 找出有差异的分钟并同步
        let mut diff_count = 0;
        for local in local_counts {
            let remote_count = remote_map.remove(&local.minute).unwrap_or(0);
            if local.unique_count != remote_count {
                diff_count += 1;
                // 同步这一分钟的数据
                match self
                    .sync_minute_data(local_table, remote_table, local.minute)
                    .await
                {
                    Ok(count) => {
                        stats.synced_records += count;
                        let minute_time = chrono::DateTime::from_timestamp(local.minute as i64, 0)
                            .unwrap()
                            .naive_utc();
                        println!(
                            "         ✓ Synced minute {} ({} records)",
                            minute_time.format("%H:%M"),
                            count
                        );
                    }
                    Err(e) => {
                        let error_msg = format!("minute {}: {}", local.minute, e);
                        stats.errors.push(error_msg.clone());
                        eprintln!("         ✗ Error: {}", error_msg);
                    }
                }
            }
        }

        // 远程有但本地没有的分钟（理论上不应该发生）
        for (minute, _) in remote_map {
            diff_count += 1;
            match self.sync_minute_data(local_table, remote_table, minute).await {
                Ok(count) => {
                    stats.synced_records += count;
                    let minute_time = chrono::DateTime::from_timestamp(minute as i64, 0)
                        .unwrap()
                        .naive_utc();
                    println!(
                        "         ✓ Synced minute {} ({} records)",
                        minute_time.format("%H:%M"),
                        count
                    );
                }
                Err(e) => {
                    let error_msg = format!("minute {}: {}", minute, e);
                    stats.errors.push(error_msg.clone());
                    eprintln!("         ✗ Error: {}", error_msg);
                }
            }
        }

        stats.diff_minutes += diff_count;
        println!("      → {} minutes with differences", diff_count);

        Ok(())
    }

    /// 同步单个分钟的数据
    async fn sync_minute_data(
        &self,
        local_table: &str,
        remote_table: &str,
        minute_ts: u32,
    ) -> Result<u64> {
        let minute_start = minute_ts;
        let minute_end = minute_ts + 60;

        // 查询本地数据的记录数
        let count_query = format!(
            "SELECT count() as cnt FROM {} WHERE timestamp >= {} AND timestamp < {}",
            local_table, minute_start, minute_end
        );
        
        #[derive(Row, Deserialize)]
        struct CountResult {
            cnt: u64,
        }
        
        let count_result: Vec<CountResult> = self.local_client.query(&count_query).fetch_all().await?;
        let record_count = count_result.first().map(|r| r.cnt).unwrap_or(0);

        // 如果有数据，则通过 remote INSERT ... SELECT 直接从本地拉取并插入
        if record_count > 0 {
            // 使用 remote() 函数让远程 ClickHouse 直接从本地查询数据
            let insert_query = format!(
                "INSERT INTO {} SELECT * FROM remote('{}', {}, {}, '{}', '{}') WHERE timestamp >= {} AND timestamp < {}",
                remote_table,
                self.config.local_url.trim_start_matches("http://").trim_start_matches("https://"),
                self.config.local_database,
                local_table,
                self.config.local_user,
                self.config.local_password,
                minute_start,
                minute_end
            );
            
            self.remote_client.query(&insert_query).execute().await?;
        }

        Ok(record_count)
    }
}
