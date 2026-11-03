use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use ed25519_dalek::SigningKey;
use volsgames_control::{Control, Endpoints, hash_key, random_key};
use volsgames_gateway::{DevCert, Gateway, Limits, Tls, load_pem};
use volsgames_room::{Host, Metrics};

pub struct Config {
    pub quic: SocketAddr,
    pub ws: Option<SocketAddr>,
    pub http: SocketAddr,
    pub public_host: String,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub signing_key: Option<String>,
    pub api_key: Option<String>,
    pub tenant: String,
    pub state_dir: PathBuf,
    pub workers: Option<usize>,
    pub limits: Limits,
}

pub struct Running {
    pub quic: SocketAddr,
    pub ws: Option<SocketAddr>,
    pub http: SocketAddr,
    pub api_key: String,
    pub tls: Tls,
    pub dev: bool,
    pub state_dir: PathBuf,
    pub host: Arc<Host>,
    endpoint: quinn::Endpoint,
    control: tokio::task::JoinHandle<()>,
}

impl Running {
    pub async fn wait(&mut self) {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = &mut self.control => {}
        }
    }

    pub fn stop(&self) {
        self.endpoint.close(0u32.into(), b"shutdown");
        self.control.abort();
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<[u8; 32]> {
    let s = s.trim();
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn signing_key(opt: Option<String>, dir: &Path) -> anyhow::Result<SigningKey> {
    if let Some(s) = opt {
        return unhex(&s).map(|b| SigningKey::from_bytes(&b)).context("signing key must be 64 hex chars");
    }
    let path = dir.join("signing.key");
    if let Some(b) = std::fs::read_to_string(&path).ok().and_then(|s| unhex(&s)) {
        return Ok(SigningKey::from_bytes(&b));
    }
    std::fs::create_dir_all(dir)?;
    let b: [u8; 32] = rand::random();
    std::fs::write(&path, hex(&b))?;
    Ok(SigningKey::from_bytes(&b))
}

pub fn api_key(opt: Option<String>, dir: &Path) -> anyhow::Result<String> {
    if let Some(k) = opt {
        return Ok(k);
    }
    let path = dir.join("api.key");
    if let Some(k) = std::fs::read_to_string(&path).ok().filter(|k| k.trim().len() >= 16) {
        return Ok(k.trim().to_string());
    }
    std::fs::create_dir_all(dir)?;
    let k = random_key("vg_");
    std::fs::write(&path, &k)?;
    Ok(k)
}

pub async fn start(c: Config) -> anyhow::Result<Running> {
    let dev = c.cert.is_none();
    let tls = match (&c.cert, &c.key) {
        (Some(cert), Some(key)) => load_pem(cert, key)?,
        (None, None) => {
            let mut names = vec!["localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string()];
            if !names.contains(&c.public_host) {
                names.push(c.public_host.clone());
            }
            DevCert::load_or_create(&c.state_dir, &names)?
        }
        _ => anyhow::bail!("--cert and --key go together"),
    };
    let signing = signing_key(c.signing_key, &c.state_dir)?;
    let api_key = api_key(c.api_key, &c.state_dir)?;
    let workers = c.workers.unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2));
    let host = Host::start(workers, Arc::new(Metrics::default()));
    let gw = Gateway::new(host.clone(), signing.verifying_key(), c.limits);
    let quic = gw.run_quic(c.quic, tls.clone()).await?;
    let ws = match c.ws {
        Some(a) => Some(gw.run_ws(a).await?),
        None => None,
    };
    let endpoints = Endpoints {
        quic: format!("{}:{}", c.public_host, quic.addr.port()),
        webtransport: format!("https://{}:{}/", c.public_host, quic.addr.port()),
        websocket: ws.map(|a| format!("ws://{}:{}/", c.public_host, a.port())),
        cert_hash: dev.then(|| tls.hash_base64()),
    };
    let ctl = Arc::new(Control {
        host: host.clone(),
        signing,
        api_key_hash: hash_key(&api_key),
        tenant: c.tenant,
        endpoints,
        max_token_ttl: Duration::from_secs(86_400),
    });
    let (http, control) = volsgames_control::serve(ctl, c.http).await?;
    tracing::info!(quic = %quic.addr, ws = ?ws, %http, workers, "volsgames up");
    Ok(Running { quic: quic.addr, ws, http, api_key, tls, dev, state_dir: c.state_dir, host, endpoint: quic.endpoint, control })
}
