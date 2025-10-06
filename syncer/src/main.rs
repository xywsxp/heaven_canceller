use clap::Parser;
use std::error::Error;

use syncer::{LocalConfig, LocalPipeline, RemoteConfig, RemotePipeline};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Parser, Debug)]
#[command(name = "syncer")]
#[command(about = "ClickHouse data export/import pipeline", long_about = None)]
struct Cli {
    /// Pipeline mode: "local" or "remote"
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
        _ => {
            return Err(format!("Invalid mode: {}. Use 'local' or 'remote'", cli.mode).into());
        }
    }

    Ok(())
}
