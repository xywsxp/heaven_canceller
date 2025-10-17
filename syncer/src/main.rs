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

    /// Path to the configuration file
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.mode.as_str() {
        "local" => {
            let config = LocalConfig::from_file(&cli.config)?;
            let pipeline = LocalPipeline::new(config);
            
            println!("Starting local mode pipeline...");
            pipeline.run().await?;
            println!("Local mode completed!");
        }
        "remote" => {
            let config = RemoteConfig::from_file(&cli.config)?;
            let pipeline = RemotePipeline::new(config);
            
            println!("Starting remote mode pipeline...");
            pipeline.run().await?;
            println!("Remote mode completed!");
        }
        "sync-check" => {
            let config = SyncConfig::from_file(&cli.config)?;
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
