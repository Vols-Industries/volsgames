use std::net::SocketAddr;
use std::sync::Arc;

use volsgames_proto::{ALPN_H3, StreamBuffer};
use web_transport_quinn::{Request, Session};

use crate::Gateway;
use crate::link::{Rx, Tx};
use crate::session::serve;
use crate::tls::Tls;

pub struct Running {
    pub addr: SocketAddr,
    pub endpoint: quinn::Endpoint,
}

pub async fn run(gw: Arc<Gateway>, addr: SocketAddr, tls: Tls) -> anyhow::Result<Running> {
    let endpoint = quinn::Endpoint::server(tls.quic_server_config(gw.idle_timeout)?, addr)?;
    let addr = endpoint.local_addr()?;
    let ep = endpoint.clone();
    tokio::spawn(async move {
        while let Some(incoming) = ep.accept().await {
            let gw = gw.clone();
            tokio::spawn(async move {
                let remote = incoming.remote_address();
                let conn = match incoming.await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::debug!(%remote, error = %e, "handshake failed");
                        return;
                    }
                };
                let alpn =
                    conn.handshake_data().and_then(|d| d.downcast::<quinn::crypto::rustls::HandshakeData>().ok()).and_then(|d| d.protocol).unwrap_or_default();
                let (session, transport) = if alpn == ALPN_H3 {
                    let req = match Request::accept(conn).await {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::debug!(%remote, error = %e, "webtransport request failed");
                            return;
                        }
                    };
                    match req.ok().await {
                        Ok(s) => (s, "webtransport"),
                        Err(e) => {
                            tracing::debug!(%remote, error = %e, "webtransport accept failed");
                            return;
                        }
                    }
                } else {
                    (Session::raw(conn), "quic")
                };
                let (send, recv) = match tokio::time::timeout(gw.hello_timeout, session.accept_bi()).await {
                    Ok(Ok(s)) => s,
                    _ => {
                        session.close(1, b"no stream");
                        return;
                    }
                };
                let rx = Rx::Quic { session: session.clone(), stream: recv, buf: StreamBuffer::default(), chunk: vec![0u8; 16 * 1024] };
                let tx = Tx::Quic { session, stream: send };
                serve(gw, rx, tx, remote, transport).await;
            });
        }
    });
    Ok(Running { addr, endpoint })
}
