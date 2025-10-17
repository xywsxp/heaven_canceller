use clap::Parser;
use misaka_signal::config::Config;
use misaka_signal::signal_service::SignalService;

#[derive(Parser, Debug)]
#[command(name = "misaka_signal")]
#[command(about = "Misaka Signal - Forward Solana transactions to Misaka Network", long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("🔧 Loading config from: {}", args.config);
    let config = Config::from_toml_file(&args.config)?;
    println!("✅ Configuration loaded successfully");

    // 创建并启动服务
    let service = SignalService::new(config).await?;
    println!("✅ SignalService initialized");

    // 运行服务
    service.run().await?;

    Ok(())
}

