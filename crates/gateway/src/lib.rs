mod limits;
mod link;
mod quic;
mod session;
mod tls;
mod ws;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use ed25519_dalek::VerifyingKey;
use volsgames_room::{Host, Metrics};

pub use limits::Limits;
pub use tls::{DevCert, Tls, cert_hash, load_pem};

pub struct Gateway {
    pub host: Arc<Host>,
    pub metrics: Arc<Metrics>,
    pub key: VerifyingKey,
    pub limits: Limits,
    pub hello_timeout: Duration,
    pub idle_timeout: Duration,
    pub outbound_queue: usize,
    next_conn: AtomicU64,
}

impl Gateway {
    pub fn new(host: Arc<Host>, key: VerifyingKey, limits: Limits) -> Arc<Self> {
        let metrics = host.metrics().clone();
        Arc::new(Self {
            host,
            metrics,
            key,
            limits,
            hello_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(20),
            outbound_queue: 512,
            next_conn: AtomicU64::new(1),
        })
    }

    pub async fn run_quic(self: &Arc<Self>, addr: SocketAddr, tls: Tls) -> anyhow::Result<quic::Running> {
        quic::run(self.clone(), addr, tls).await
    }

    pub async fn run_ws(self: &Arc<Self>, addr: SocketAddr) -> anyhow::Result<SocketAddr> {
        ws::run(self.clone(), addr).await
    }
}
