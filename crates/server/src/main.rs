use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use volsgames::{Config, start};
use volsgames_gateway::Limits;

#[derive(Parser)]
#[command(name = "volsgames", version, about = "VolsGames real-time gaming network engine")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Serve(Serve),
}

#[derive(Parser)]
struct Serve {
    #[arg(long, env = "VOLSGAMES_QUIC", default_value = "0.0.0.0:4433")]
    quic: SocketAddr,
    #[arg(long, env = "VOLSGAMES_WS")]
    ws: Option<SocketAddr>,
    #[arg(long, env = "VOLSGAMES_HTTP", default_value = "127.0.0.1:8080")]
    http: SocketAddr,
    #[arg(long, env = "VOLSGAMES_PUBLIC_HOST", default_value = "127.0.0.1")]
    public_host: String,
    #[arg(long, env = "VOLSGAMES_CERT")]
    cert: Option<PathBuf>,
    #[arg(long, env = "VOLSGAMES_KEY")]
    key: Option<PathBuf>,
    #[arg(long, env = "VOLSGAMES_SIGNING_KEY")]
    signing_key: Option<String>,
    #[arg(long, env = "VOLSGAMES_API_KEY")]
    api_key: Option<String>,
    #[arg(long, env = "VOLSGAMES_TENANT", default_value = "local")]
    tenant: String,
    #[arg(long, env = "VOLSGAMES_STATE_DIR", default_value = ".volsgames")]
    state_dir: PathBuf,
    #[arg(long, env = "VOLSGAMES_WORKERS")]
    workers: Option<usize>,
    #[arg(long, env = "VOLSGAMES_MSGS_PER_SEC", default_value_t = 240)]
    messages_per_sec: u32,
    #[arg(long, env = "VOLSGAMES_BYTES_PER_SEC", default_value_t = 262_144)]
    bytes_per_sec: u32,
    #[arg(long, env = "VOLSGAMES_MAX_PAYLOAD", default_value_t = 16_384)]
    max_payload: usize,
    #[arg(long, env = "VOLSGAMES_LOG_JSON", default_value_t = false)]
    log_json: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Cli { cmd: Cmd::Serve(s) } = Cli::parse();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    if s.log_json {
        tracing_subscriber::fmt().json().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    let limits = Limits { messages_per_sec: s.messages_per_sec, bytes_per_sec: s.bytes_per_sec, max_payload: s.max_payload, ..Default::default() };
    let mut run = start(Config {
        quic: s.quic,
        ws: s.ws,
        http: s.http,
        public_host: s.public_host,
        cert: s.cert,
        key: s.key,
        signing_key: s.signing_key,
        api_key: s.api_key,
        tenant: s.tenant,
        state_dir: s.state_dir,
        workers: s.workers,
        limits,
    })
    .await?;
    if run.dev {
        tracing::info!(cert_hash = %run.tls.hash_base64(), api_key_file = %run.state_dir.join("api.key").display(), "dev mode");
    }
    run.wait().await;
    run.stop();
    Ok(())
}
