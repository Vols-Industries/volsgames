use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use clap::Parser;
use hdrhistogram::Histogram;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use serde::Deserialize;
use tokio::sync::Mutex;
use volsgames_proto::{ALPN_QUIC, ClientMessage, Datagram, ServerMessage, StreamBuffer, Target, client_record, seq_newer};
use web_transport_quinn::Session;

#[derive(Parser)]
#[command(name = "volsgames-loadgen", version, about = "Bots that play against a VolsGames server and report latency")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    control: String,
    #[arg(long, env = "VOLSGAMES_API_KEY")]
    api_key: String,
    #[arg(long)]
    quic: Option<SocketAddr>,
    #[arg(long, default_value = "loadgen")]
    game: String,
    #[arg(long, default_value_t = 1)]
    rooms: u32,
    #[arg(long, default_value_t = 20)]
    bots: u32,
    #[arg(long, default_value_t = 20.0)]
    rate: f64,
    #[arg(long, default_value_t = 64)]
    payload: usize,
    #[arg(long, default_value_t = 20)]
    seconds: u64,
    #[arg(long, default_value = "unreliable")]
    lane: String,
    #[arg(long, default_value_t = 0)]
    tick_rate: u16,
    #[arg(long, default_value_t = false)]
    flush_on_tick: bool,
    #[arg(long, default_value_t = false)]
    insecure: bool,
}

#[derive(Deserialize)]
struct RoomView {
    room: String,
}

#[derive(Deserialize)]
struct Endpoints {
    quic: String,
}

#[derive(Deserialize)]
struct TokenView {
    token: String,
    endpoints: Endpoints,
}

#[derive(Default)]
struct Counters {
    sent: AtomicU64,
    received: AtomicU64,
    pings: AtomicU64,
    connected: AtomicU64,
    errors: AtomicU64,
}

#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(&self, m: &[u8], c: &CertificateDer<'_>, d: &rustls::DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn verify_tls13_signature(&self, m: &[u8], c: &CertificateDer<'_>, d: &rustls::DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(m, c, d, &self.0.signature_verification_algorithms)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

fn endpoint(insecure: bool) -> anyhow::Result<quinn::Endpoint> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone()).with_protocol_versions(&[&rustls::version::TLS13])?;
    let mut cfg = if insecure {
        builder.dangerous().with_custom_certificate_verifier(Arc::new(NoVerify(provider))).with_no_client_auth()
    } else {
        let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        builder.with_root_certificates(roots).with_no_client_auth()
    };
    cfg.alpn_protocols = vec![ALPN_QUIC.to_vec()];
    let qcfg: quinn::crypto::rustls::QuicClientConfig = cfg.try_into()?;
    let mut client = quinn::ClientConfig::new(Arc::new(qcfg));
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(Duration::from_secs(20).try_into()?));
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    client.transport_config(Arc::new(transport));
    let mut ep = quinn::Endpoint::client("[::]:0".parse()?).or_else(|_| quinn::Endpoint::client("0.0.0.0:0".parse().unwrap()))?;
    ep.set_default_client_config(client);
    Ok(ep)
}

struct Shared {
    relay: Mutex<Histogram<u64>>,
    rtt: Mutex<Histogram<u64>>,
    counters: Counters,
}

async fn bot(ep: quinn::Endpoint, addr: SocketAddr, token: String, args: Arc<Args>, shared: Arc<Shared>, deadline: Instant) -> anyhow::Result<()> {
    let conn = ep.connect(addr, "localhost")?.await.context("connect")?;
    let session = Session::raw(conn);
    let (mut send, mut recv) = session.open_bi().await.context("open stream")?;
    send.write_all(&client_record(&ClientMessage::Hello { token })).await?;
    let mut sb = StreamBuffer::default();
    let mut chunk = vec![0u8; 16 * 1024];
    let welcome = loop {
        if let Some(r) = sb.pop() {
            break ServerMessage::read(r)?;
        }
        match recv.read(&mut chunk).await? {
            Some(n) => sb.push(&chunk[..n]),
            None => anyhow::bail!("closed before welcome"),
        }
    };
    let me = match welcome {
        ServerMessage::Welcome { player, .. } => player,
        ServerMessage::Error { code, message } => anyhow::bail!("refused {code}: {message}"),
        other => anyhow::bail!("unexpected {other:?}"),
    };
    shared.counters.connected.fetch_add(1, Relaxed);
    let reliable = args.lane == "reliable";
    let interval = Duration::from_secs_f64(1.0 / args.rate.max(0.1));
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ping = tokio::time::interval(Duration::from_secs(1));
    let mut seq: u16 = 0;
    let mut last_in: Option<u16> = None;
    let payload_len = args.payload.max(12);
    let mut buf = BytesMut::with_capacity(payload_len + 32);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if Instant::now() >= deadline { break; }
                buf.clear();
                buf.put_u32_le(me);
                buf.put_u64_le(now_ns());
                buf.resize(payload_len, 0xAB);
                let msg = ClientMessage::Data { target: Target::Others, payload: buf.split().freeze() };
                let rec = client_record(&msg);
                if reliable {
                    send.write_all(&rec).await?;
                } else {
                    seq = seq.wrapping_add(1);
                    let _ = session.send_datagram(Datagram::frame(seq, &rec));
                }
                shared.counters.sent.fetch_add(1, Relaxed);
            }
            _ = ping.tick() => {
                let rec = client_record(&ClientMessage::Ping { client_time: now_ns() });
                if reliable { send.write_all(&rec).await?; } else { seq = seq.wrapping_add(1); let _ = session.send_datagram(Datagram::frame(seq, &rec)); }
            }
            n = recv.read(&mut chunk) => {
                match n? {
                    Some(n) => {
                        sb.push(&chunk[..n]);
                        while let Some(r) = sb.pop() { handle(r, &shared).await?; }
                    }
                    None => break,
                }
            }
            d = session.read_datagram() => {
                let d = Datagram::parse(d?)?;
                if last_in.is_some_and(|p| !seq_newer(d.seq, p)) { continue; }
                last_in = Some(d.seq);
                for r in d.records { handle(r?, &shared).await?; }
            }
        }
    }
    let _ = send.write_all(&client_record(&ClientMessage::Leave)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    session.close(0, b"done");
    Ok(())
}

async fn handle(rec: Bytes, shared: &Shared) -> anyhow::Result<()> {
    match ServerMessage::read(rec)? {
        ServerMessage::Data { payload, .. } => {
            if payload.len() >= 12 {
                let mut p = payload;
                let _from = p.get_u32_le();
                let sent = p.get_u64_le();
                let now = now_ns();
                if now > sent {
                    shared.relay.lock().await.record((now - sent) / 1000).ok();
                }
            }
            shared.counters.received.fetch_add(1, Relaxed);
        }
        ServerMessage::Pong { client_time, .. } => {
            let now = now_ns();
            if now > client_time {
                shared.rtt.lock().await.record((now - client_time) / 1000).ok();
            }
            shared.counters.pings.fetch_add(1, Relaxed);
        }
        ServerMessage::Bye { reason } => anyhow::bail!("bye {reason}"),
        ServerMessage::Error { code, message } => anyhow::bail!("error {code}: {message}"),
        _ => {}
    }
    Ok(())
}

fn api<T: for<'de> Deserialize<'de>>(args: &Args, method: &str, path: &str, body: Option<serde_json::Value>) -> anyhow::Result<T> {
    let url = format!("{}{}", args.control.trim_end_matches('/'), path);
    let auth = format!("Bearer {}", args.api_key);
    let resp = match (method, body) {
        ("POST", Some(b)) => ureq::post(&url).header("authorization", &auth).send_json(b)?,
        _ => ureq::get(&url).header("authorization", &auth).call()?,
    };
    Ok(resp.into_body().read_json::<T>()?)
}

fn pct(h: &Histogram<u64>, q: f64) -> f64 {
    h.value_at_quantile(q) as f64 / 1000.0
}

fn report(name: &str, h: &Histogram<u64>) {
    if h.is_empty() {
        println!("{name:>12}  no samples");
        return;
    }
    println!(
        "{name:>12}  n={:<9} p50={:>8.3}ms  p90={:>8.3}ms  p99={:>8.3}ms  p99.9={:>8.3}ms  max={:>8.3}ms",
        h.len(),
        pct(h, 0.5),
        pct(h, 0.9),
        pct(h, 0.99),
        pct(h, 0.999),
        h.max() as f64 / 1000.0
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into())).init();
    let args = Arc::new(Args::parse());
    let shared = Arc::new(Shared { relay: Mutex::new(Histogram::new(3)?), rtt: Mutex::new(Histogram::new(3)?), counters: Counters::default() });
    let ep = endpoint(args.insecure)?;
    let per_room = args.bots.div_ceil(args.rooms.max(1));
    let mut tokens: Vec<(String, String)> = Vec::new();
    for r in 0..args.rooms.max(1) {
        let mut body = serde_json::json!({ "game": args.game, "max_players": per_room.max(1), "flush_on_tick": args.flush_on_tick });
        if args.tick_rate > 0 {
            body["tick_rate"] = serde_json::json!(args.tick_rate);
        }
        let room: RoomView = api(&args, "POST", "/v1/rooms", Some(body))?;
        for b in 0..per_room {
            if tokens.len() as u32 >= args.bots {
                break;
            }
            let t: TokenView = api(
                &args,
                "POST",
                &format!("/v1/rooms/{}/tokens", room.room),
                Some(serde_json::json!({ "player": format!("bot-{r}-{b}"), "name": format!("bot {b}") })),
            )?;
            tokens.push((t.token, t.endpoints.quic));
        }
    }
    let addr = match args.quic {
        Some(a) => a,
        None => tokens[0].1.parse().context("quic endpoint from control")?,
    };
    let deadline = Instant::now() + Duration::from_secs(args.seconds);
    let mut tasks = Vec::new();
    for (i, (token, _)) in tokens.into_iter().enumerate() {
        let (ep, args, shared) = (ep.clone(), args.clone(), shared.clone());
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis((i as u64 * 5) % 1000)).await;
            if let Err(e) = bot(ep, addr, token, args, shared.clone(), deadline).await {
                shared.counters.errors.fetch_add(1, Relaxed);
                tracing::warn!(bot = i, error = %e, "bot failed");
            }
        }));
    }
    let start = Instant::now();
    for t in tasks {
        let _ = t.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let c = &shared.counters;
    println!(
        "bots={} rooms={} lane={} rate={}/s payload={}B seconds={} connected={} errors={}",
        args.bots,
        args.rooms,
        args.lane,
        args.rate,
        args.payload,
        args.seconds,
        c.connected.load(Relaxed),
        c.errors.load(Relaxed)
    );
    println!(
        "sent={} received={} ({:.0}/s in, {:.0}/s out) pongs={}",
        c.sent.load(Relaxed),
        c.received.load(Relaxed),
        c.sent.load(Relaxed) as f64 / elapsed,
        c.received.load(Relaxed) as f64 / elapsed,
        c.pings.load(Relaxed)
    );
    report("relay", &*shared.relay.lock().await);
    report("ping rtt", &*shared.rtt.lock().await);
    ep.wait_idle().await;
    Ok(())
}
