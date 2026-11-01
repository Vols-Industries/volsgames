use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use volsgames_proto::{Claims, ClientMessage, Datagram, Lane, ServerMessage, code, seq_newer, server_record};
use volsgames_room::{Command, ConnHandle, JoinRequest, Metrics, Outbound, RoomId, now_ms};

use crate::Gateway;
use crate::limits::Bucket;
use crate::link::{Frame, Rx, Tx};

pub async fn serve(gw: Arc<Gateway>, mut rx: Rx, tx: Tx, remote: SocketAddr, transport: &'static str) {
    let conn = gw.next_conn.fetch_add(1, Relaxed);
    Metrics::inc(&gw.metrics.connections);
    Metrics::inc(&gw.metrics.connections_total);
    tracing::debug!(conn, %remote, transport, "connection");
    let outcome = run(&gw, conn, &mut rx, tx, remote).await;
    Metrics::dec(&gw.metrics.connections);
    if let Err((code, msg)) = outcome {
        Metrics::inc(&gw.metrics.rejected);
        tracing::info!(conn, %remote, code, msg, "rejected");
    }
}

async fn run(gw: &Arc<Gateway>, conn: u64, rx: &mut Rx, tx: Tx, remote: SocketAddr) -> Result<(), (u16, &'static str)> {
    let hello = match timeout(gw.hello_timeout, rx.next()).await {
        Ok(Some(Frame::Record(r))) => ClientMessage::read(r).map_err(|_| (code::PROTOCOL, "bad hello"))?,
        Ok(_) => return Err((code::PROTOCOL, "no hello")),
        Err(_) => return Err((code::HELLO_TIMEOUT, "hello timeout")),
    };
    let ClientMessage::Hello { token } = hello else {
        return Err((code::PROTOCOL, "first message not hello"));
    };
    let claims = match Claims::verify(&token, &gw.key, now_ms() / 1000) {
        Ok(c) => c,
        Err(e) => {
            let (c, m) = match e {
                volsgames_proto::TokenError::Expired => (code::EXPIRED, "token expired"),
                _ => (code::BAD_TOKEN, "bad token"),
            };
            refuse(tx, c, m).await;
            return Err((c, m));
        }
    };
    let room: RoomId = Arc::from(claims.room.as_str());
    let Some((_, cfg, _, _)) = gw.host.get(&room) else {
        refuse(tx, code::NO_ROOM, "no such room").await;
        return Err((code::NO_ROOM, "no such room"));
    };
    if cfg.game.as_ref() != claims.game {
        refuse(tx, code::WRONG_GAME, "token is for another game").await;
        return Err((code::WRONG_GAME, "wrong game"));
    }
    let (out_tx, out_rx) = mpsc::channel::<Outbound>(gw.outbound_queue);
    let (reply, joined) = oneshot::channel();
    let handle = ConnHandle { id: conn, tx: out_tx };
    let Some(worker) =
        gw.host.join(JoinRequest { room: room.clone(), conn: handle, identity: Arc::from(claims.player.as_str()), name: claims.name.clone(), reply })
    else {
        refuse(tx, code::NO_ROOM, "no such room").await;
        return Err((code::NO_ROOM, "room gone"));
    };
    let joined = match timeout(gw.hello_timeout, joined).await {
        Ok(Ok(Ok(j))) => j,
        Ok(Ok(Err(c))) => {
            refuse(tx, c, "join refused").await;
            return Err((c, "join refused"));
        }
        _ => {
            refuse(tx, code::NO_ROOM, "join failed").await;
            return Err((code::NO_ROOM, "join failed"));
        }
    };
    let player = joined.player;
    tracing::info!(conn, %remote, room = %room, player, tenant = %claims.tenant, game = %claims.game, "session");
    let writer = tokio::spawn(write_loop(out_rx, tx, gw.metrics.clone()));
    let mut bucket = Bucket::new(gw.limits);
    let mut last_seq: Option<u16> = None;
    let max_payload = gw.limits.max_payload;
    let mut live = true;
    while live {
        let Some(frame) = rx.next().await else { break };
        let now = Instant::now();
        match frame {
            Frame::Record(r) => {
                live = inbound(gw, worker, &room, player, conn, Lane::Reliable, r, &mut bucket, now, max_payload);
            }
            Frame::Datagram(d) => {
                let Ok(d) = Datagram::parse(d) else { continue };
                if let Some(prev) = last_seq
                    && !seq_newer(d.seq, prev)
                {
                    Metrics::inc(&gw.metrics.dropped_unreliable);
                    continue;
                }
                last_seq = Some(d.seq);
                for r in d.records {
                    let Ok(r) = r else { break };
                    live = inbound(gw, worker, &room, player, conn, Lane::Unreliable, r, &mut bucket, now, max_payload);
                    if !live {
                        break;
                    }
                }
            }
        }
    }
    gw.host.send(worker, Command::Disconnect { room, player, conn });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    writer.abort();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn inbound(
    gw: &Gateway,
    worker: usize,
    room: &RoomId,
    player: u32,
    conn: u64,
    lane: Lane,
    rec: Bytes,
    bucket: &mut Bucket,
    now: Instant,
    max_payload: usize,
) -> bool {
    if rec.len() > max_payload + 16 || !bucket.allow(now, rec.len()) {
        Metrics::inc(&gw.metrics.rate_limited);
        return !bucket.exhausted();
    }
    let Ok(msg) = ClientMessage::read(rec) else {
        return false;
    };
    if matches!(msg, ClientMessage::Hello { .. }) {
        return false;
    }
    let leaving = matches!(msg, ClientMessage::Leave);
    gw.host.send(worker, Command::Message { room: room.clone(), player, conn, lane, msg });
    !leaving
}

async fn write_loop(mut out: mpsc::Receiver<Outbound>, mut tx: Tx, metrics: Arc<Metrics>) {
    let mut seq: u16 = 0;
    let mut recs = Vec::with_capacity(32);
    loop {
        let n = out.recv_many(&mut recs, 32).await;
        if n == 0 {
            break;
        }
        let mut closing = false;
        for o in recs.drain(..) {
            match o {
                Outbound::Frame(lane, body) => {
                    if lane == Lane::Unreliable {
                        seq = seq.wrapping_add(1);
                    }
                    Metrics::add(&metrics.bytes_out, body.len() as u64);
                    if !tx.write(lane, seq, body).await {
                        closing = true;
                        break;
                    }
                }
                Outbound::Close => {
                    closing = true;
                    break;
                }
            }
        }
        if closing {
            break;
        }
    }
    tx.close().await;
}

async fn refuse(mut tx: Tx, code: u16, message: &str) {
    let rec = server_record(&ServerMessage::Error { code, message: message.to_string() });
    let _ = tx.write(Lane::Reliable, 0, rec).await;
    tx.close().await;
}
