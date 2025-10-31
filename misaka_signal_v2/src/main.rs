use clap::Parser;
use misaka_signal_v2::{Config, SignalService};

#[derive(Parser, Debug)]
#[command(name = "misaka_signal_v2")]
#[command(about = "Misaka Signal V2 - 直接转发 Parsed Transaction", long_about = None)]
struct Args {
    /// 配置文件路径
    #[arg(short, long, default_value = "config/signal_config.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // 加载配置
    let config = Config::from_toml_file(&args.config)?;
    println!("📋 Config loaded from: {}", args.config);

    // 创建并运行服务
    let service = SignalService::new(config).await?;
    service.run().await?;

    Ok(())
}
