use std::env;
use squirrel::block_parser::block_parser_service::{BlockParserService, Config as BlockParserConfig};
use squirrel::transaction_subscriber::transaction_subscriber_service::{TransactionSubscriberService, Config as TransactionSubscriberConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 3 {
        print_usage();
        std::process::exit(1);
    }
    
    let mut mode: Option<String> = None;
    let mut config_path: Option<String> = None;
    
    // 解析命令行参数
    for i in 1..args.len() {
        let arg = &args[i];
        if arg.starts_with("--mode=") {
            mode = Some(arg.trim_start_matches("--mode=").to_string());
        } else if arg.starts_with("--config=") {
            config_path = Some(arg.trim_start_matches("--config=").to_string());
        }
    }
    
    let mode = mode.ok_or("Missing --mode parameter")?;
    let config_path = config_path.ok_or("Missing --config parameter")?;
    
    match mode.as_str() {
        "block_parser" => {
            println!("Starting Block Parser Service...");
            println!("Config file: {}", config_path);
            
            // 加载配置文件
            let config = BlockParserConfig::from_toml_file(&config_path)?;
            println!("Configuration loaded successfully");
            
            // 创建并启动服务
            let service = BlockParserService::new(config)?;
            println!("BlockParserService initialized, starting processing...");
            
            // 启动服务（这会消费 service）
            service.run().await?;
        }
        "transaction_subscriber" => {
            println!("Starting Transaction Subscriber Service...");
            println!("Config file: {}", config_path);
            
            // 加载配置文件
            let config = TransactionSubscriberConfig::from_toml_file(&config_path)?;
            println!("Configuration loaded successfully");
            
            // 创建并启动服务
            let service = TransactionSubscriberService::new(config).await?;
            println!("TransactionSubscriberService initialized, starting processing...");
            
            // 启动服务（这会消费 service）
            service.run().await?;
        }
        _ => {
            eprintln!("Unknown mode: {}", mode);
            print_usage();
            std::process::exit(1);
        }
    }
    
    Ok(())
}

fn print_usage() {
    println!("Usage: squirrel --mode=<MODE> --config=<CONFIG_FILE>");
    println!("Modes:");
    println!("  block_parser            Start the block parser service");
    println!("  transaction_subscriber  Start the transaction subscriber service");
    println!("");
    println!("Examples:");
    println!("  squirrel --mode=block_parser --config=config/block_parser_config.toml");
    println!("  squirrel --mode=transaction_subscriber --config=config/transaction_subscriber.toml");
}
