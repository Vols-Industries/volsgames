use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::StreamExt;
use tokio::net::TcpListener;

use crate::Gateway;
use crate::link::{Rx, Tx};
use crate::session::serve;

pub async fn run(gw: Arc<Gateway>, addr: SocketAddr) -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        loop {
            let Ok((stream, remote)) = listener.accept().await else { continue };
            let gw = gw.clone();
            tokio::spawn(async move {
                let _ = stream.set_nodelay(true);
                let ws = match tokio::time::timeout(gw.hello_timeout, tokio_tungstenite::accept_async(stream)).await {
                    Ok(Ok(ws)) => ws,
                    _ => return,
                };
                let (sink, stream) = ws.split();
                serve(gw, Rx::Ws(stream), Tx::Ws(sink), remote, "websocket").await;
            });
        }
    });
    Ok(addr)
}
