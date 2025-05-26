mod storage;

use tracing::{error, info};
use tracing_subscriber;

#[tokio::main]
async fn main() {
    // Initialize logging with environment-based filter
    tracing_subscriber::fmt()
        .with_env_filter("seneca=info")
        .init();

    // Show a welcome banner
    show_banner();

    // Load config (placeholder for now)
    let config = load_config();

    // Start TCP listener (stub)
    if let Err(e) = start_network(&config).await {
        error!("Failed to start network: {:?}", e);
    }
}

fn show_banner() {
    println!(
        r#"
     _____                           
    /  ___|                          
    \ `--.  ___ _ __   ___  ___ __ _ 
     `--. \/ _ \ '_ \ / _ \/ __/ _` |
    /\__/ /  __/ | | |  __/ (_| (_| |
    \____/ \___|_| |_|\___|\___\__,_|

 A Kafka-compatible broker written in Rust
    "#
    );
}

#[derive(Debug)]
struct Config {
    listen_addr: String,
}

fn load_config() -> Config {
    // This can later read from env, files, etc.
    Config {
        listen_addr: "127.0.0.1:9092".into(),
    }
}

async fn start_network(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(&config.listen_addr).await?;
    info!("Seneca broker listening on {}", config.listen_addr);

    loop {
        let (_socket, addr) = listener.accept().await?;
        info!("Accepted connection from {}", addr);
        // TODO: Handle Kafka protocol here
    }
}
