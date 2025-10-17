use clap::Parser;
use std::error::Error;

use syncer::{LocalConfig, LocalPipeline, RemoteConfig, RemotePipeline, SyncChecker, SyncConfig};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Parser, Debug)]
#[command(name = "syncer")]
#[command(about = "ClickHouse data export/import/sync pipeline", long_about = None)]
struct Cli {
    /// Pipeline mode: "local", "remote", or "sync-check"
    #[arg(long)]
    mode: String,

    /// Path to the configuration file (optional for sync-check)
    #[arg(short, long)]
    config: Option<String>,

    // --- Optional flags for sync-check (used when --config is omitted) ---
    /// Local ClickHouse URL (e.g. http://localhost:18123)
    #[arg(long)]
    local_url: Option<String>,
    #[arg(long)]
    local_database: Option<String>,
    #[arg(long)]
    local_user: Option<String>,
    #[arg(long)]
    local_password: Option<String>,

    /// Remote ClickHouse URL (e.g. http://remote-host:28123)
    #[arg(long)]
    remote_url: Option<String>,
    #[arg(long)]
    remote_database: Option<String>,
    #[arg(long)]
    remote_user: Option<String>,
    #[arg(long)]
    remote_password: Option<String>,

    /// Check days (default 7)
    #[arg(long)]
    check_days: Option<u32>,

    /// Lag hours (default 2)
    #[arg(long)]
    lag_hours: Option<u32>,

    /// Table mappings in the form local:remote (can be repeated)
    #[arg(long = "map")]
    table_mappings: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.mode.as_str() {
        "local" => {
            let config_path = cli.config.as_ref().ok_or("--config is required for local mode")?;
            let config = LocalConfig::from_file(config_path)?;
            let pipeline = LocalPipeline::new(config);
            
            println!("Starting local mode pipeline...");
            pipeline.run().await?;
            println!("Local mode completed!");
        }
        "remote" => {
            let config_path = cli.config.as_ref().ok_or("--config is required for remote mode")?;
            let config = RemoteConfig::from_file(config_path)?;
            let pipeline = RemotePipeline::new(config);
            
            println!("Starting remote mode pipeline...");
            pipeline.run().await?;
            println!("Remote mode completed!");
        }
        "sync-check" => {
            // build config from file if provided, otherwise from CLI flags
            let config = if let Some(path) = &cli.config {
                SyncConfig::from_file(path)?
            } else {
                // require required flags
                let local_url = cli.local_url.clone().ok_or("--local-url is required when --config is not provided")?;
                let local_database = cli.local_database.clone().unwrap_or_else(|| "default".to_string());
                let local_user = cli.local_user.clone().unwrap_or_else(|| "default".to_string());
                let local_password = cli.local_password.clone().unwrap_or_else(|| "".to_string());

                let remote_url = cli.remote_url.clone().ok_or("--remote-url is required when --config is not provided")?;
                let remote_database = cli.remote_database.clone().unwrap_or_else(|| "default".to_string());
                let remote_user = cli.remote_user.clone().unwrap_or_else(|| "default".to_string());
                let remote_password = cli.remote_password.clone().unwrap_or_else(|| "".to_string());

                let check_days = cli.check_days.unwrap_or(7);
                let lag_hours = cli.lag_hours.unwrap_or(2);

                // parse table mappings
                let mut mappings = std::collections::HashMap::new();
                for entry in &cli.table_mappings {
                    if let Some((l, r)) = entry.split_once(':') {
                        mappings.insert(l.to_string(), r.to_string());
                    } else {
                        return Err(format!("Invalid --map entry: {}. Use local:remote", entry).into());
                    }
                }

                SyncConfig {
                    local_url,
                    local_database,
                    local_user,
                    local_password,
                    remote_url,
                    remote_database,
                    remote_user,
                    remote_password,
                    table_mappings: mappings,
                    check_days,
                    lag_hours,
                }
            };

            let checker = SyncChecker::new(config);
            
            println!("Starting sync check mode...");
            let stats = checker.check_and_sync().await?;
            stats.print_summary();
            
            if !stats.errors.is_empty() {
                return Err(format!("Sync completed with {} errors", stats.errors.len()).into());
            }
            
            println!("\n✅ Sync check completed successfully!");
        }
        _ => {
            return Err(format!(
                "Invalid mode: {}. Use 'local', 'remote', or 'sync-check'",
                cli.mode
            )
            .into());
        }
    }

    Ok(())
}
