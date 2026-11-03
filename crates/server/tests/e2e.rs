use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{Value, json};
use volsgames::{Config, Running, start};
use volsgames_control::fleet::{Fleet, Node};
use volsgames_control::hash_key;
use volsgames_gateway::Limits;
use volsgames_proto::{ALPN_QUIC, ClientMessage, Datagram, ServerMessage, StreamBuffer, Target, client_record, code, reason};
use web_transport_quinn::Session;

#[derive(Debug)]
struct Trust(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for Trust {
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

struct Stack {
    run: Running,
    _dir: tempfile::TempDir,
    ep: quinn::Endpoint,
}

impl Drop for Stack {
    fn drop(&mut self) {
        self.run.stop();
    }
}

async fn stack() -> Stack {
    let dir = tempfile::tempdir().unwrap();
    let run = start(Config {
        quic: "127.0.0.1:0".parse().unwrap(),
        ws: Some("127.0.0.1:0".parse().unwrap()),
        http: "127.0.0.1:0".parse().unwrap(),
        public_host: "127.0.0.1".into(),
        cert: None,
        key: None,
        signing_key: None,
        api_key: Some("test-key-0123456789".into()),
        tenant: "t".into(),
        state_dir: dir.path().to_path_buf(),
        workers: Some(2),
        limits: Limits { messages_per_sec: 50, bytes_per_sec: 100_000, burst_seconds: 1.0, max_payload: 1024, strikes: 20 },
    })
    .await
    .unwrap();
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(Trust(provider)))
        .with_no_client_auth();
    cfg.alpn_protocols = vec![ALPN_QUIC.to_vec()];
    let qcfg: quinn::crypto::rustls::QuicClientConfig = cfg.try_into().unwrap();
    let mut ep = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    ep.set_default_client_config(quinn::ClientConfig::new(Arc::new(qcfg)));
    Stack { run, _dir: dir, ep }
}

fn api(http: SocketAddr, method: &str, path: &str, body: Option<Value>, key: &str) -> (u16, Value) {
    let url = format!("http://{http}{path}");
    let auth = format!("Bearer {key}");
    let agent = ureq::Agent::new_with_config(ureq::config::Config::builder().http_status_as_error(false).build());
    let resp = match (method, body) {
        ("POST", Some(b)) => agent.post(&url).header("authorization", &auth).send_json(b).unwrap(),
        ("DELETE", _) => agent.delete(&url).header("authorization", &auth).call().unwrap(),
        _ => agent.get(&url).header("authorization", &auth).call().unwrap(),
    };
    let status = resp.status().as_u16();
    let text = resp.into_body().read_to_string().unwrap_or_default();
    (status, serde_json::from_str(&text).unwrap_or(Value::Null))
}

impl Stack {
    fn api(&self, method: &str, path: &str, body: Option<Value>, key: &str) -> (u16, Value) {
        api(self.run.http, method, path, body, key)
    }

    fn room(&self, room: &str, extra: Value) -> Value {
        let mut body = json!({ "game": "g", "room": room });
        for (k, v) in extra.as_object().unwrap() {
            body[k] = v.clone();
        }
        let (s, v) = self.api("POST", "/v1/rooms", Some(body), &self.run.api_key);
        assert_eq!(s, 201, "{v}");
        v
    }

    fn token(&self, room: &str, player: &str) -> String {
        let (s, v) = self.api("POST", &format!("/v1/rooms/{room}/tokens"), Some(json!({ "player": player, "name": player })), &self.run.api_key);
        assert_eq!(s, 201, "{v}");
        v["token"].as_str().unwrap().to_string()
    }

    async fn connect(&self, token: &str) -> Client {
        let conn = self.ep.connect(self.run.quic, "localhost").unwrap().await.unwrap();
        let session = Session::raw(conn);
        let (send, recv) = session.open_bi().await.unwrap();
        let mut c = Client { session, send, recv, buf: StreamBuffer::default(), seq: 0 };
        c.send(&ClientMessage::Hello { token: token.into() }).await;
        c
    }
}

struct Client {
    session: Session,
    send: web_transport_quinn::SendStream,
    recv: web_transport_quinn::RecvStream,
    buf: StreamBuffer,
    seq: u16,
}

impl Client {
    async fn send(&mut self, m: &ClientMessage) {
        self.send.write_all(&client_record(m)).await.unwrap();
    }

    fn datagram(&mut self, m: &ClientMessage) {
        self.seq = self.seq.wrapping_add(1);
        self.session.send_datagram(Datagram::frame(self.seq, &client_record(m))).unwrap();
    }

    async fn next(&mut self) -> ServerMessage {
        let mut chunk = [0u8; 4096];
        loop {
            if let Some(r) = self.buf.pop() {
                return ServerMessage::read(r).unwrap();
            }
            tokio::select! {
                n = self.recv.read(&mut chunk) => match n.unwrap() {
                    Some(n) => self.buf.push(&chunk[..n]),
                    None => panic!("stream closed"),
                },
                d = self.session.read_datagram() => {
                    let d = Datagram::parse(d.unwrap()).unwrap();
                    for r in d.records {
                        self.buf.push(&client_record_raw(r.unwrap()));
                    }
                }
            }
        }
    }

    async fn expect(&mut self) -> ServerMessage {
        tokio::time::timeout(Duration::from_secs(5), self.next()).await.expect("message in time")
    }

    async fn closed(&mut self) -> bool {
        tokio::time::timeout(Duration::from_secs(5), self.session.closed()).await.is_ok()
    }
}

fn client_record_raw(body: Bytes) -> Vec<u8> {
    let mut v = (body.len() as u16).to_le_bytes().to_vec();
    v.extend_from_slice(&body);
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_api_auth_and_rooms() {
    let s = stack().await;
    assert_eq!(s.api("GET", "/v1/rooms", None, "wrong").0, 401);
    assert_eq!(s.api("POST", "/v1/rooms", Some(json!({ "game": "bad id!" })), &s.run.api_key).0, 400);
    let v = s.room("lobby", json!({ "tick_rate": 60, "max_players": 2 }));
    assert_eq!(v["tick_rate"], 60);
    assert_eq!(s.api("POST", "/v1/rooms", Some(json!({ "game": "g", "room": "lobby" })), &s.run.api_key).0, 409);
    assert_eq!(s.api("GET", "/v1/rooms/lobby", None, &s.run.api_key).1["max_players"], 2);
    assert_eq!(s.api("GET", "/v1/rooms", None, &s.run.api_key).1.as_array().unwrap().len(), 1);
    assert_eq!(s.api("POST", "/v1/rooms/nope/tokens", Some(json!({ "player": "p" })), &s.run.api_key).0, 404);
    assert_eq!(s.api("DELETE", "/v1/rooms/lobby", None, &s.run.api_key).0, 204);
    assert_eq!(s.api("DELETE", "/v1/rooms/lobby", None, &s.run.api_key).0, 404);
    let (st, ep) = s.api("GET", "/v1/endpoints", None, "");
    assert_eq!(st, 200);
    assert_eq!(ep["quic"].as_str().unwrap(), s.run.quic.to_string());
    assert!(ep["cert_hash"].as_str().unwrap().len() > 40);
    let text = ureq::get(format!("http://{}/metrics", s.run.http)).call().unwrap().into_body().read_to_string().unwrap();
    assert!(text.contains("volsgames_rooms_total 1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn relay_over_quic_both_lanes() {
    let s = stack().await;
    s.room("arena", json!({}));
    let mut a = s.connect(&s.token("arena", "alice")).await;
    assert!(matches!(a.expect().await, ServerMessage::Welcome { player: 1, reconnected: false, .. }));
    let mut b = s.connect(&s.token("arena", "bob")).await;
    assert!(matches!(b.expect().await, ServerMessage::Welcome { player: 2, ref players, .. } if players.len() == 2));
    assert_eq!(a.expect().await, ServerMessage::Joined { player: 2, name: "bob".into() });
    a.send(&ClientMessage::Data { target: Target::Others, payload: Bytes::from_static(b"reliable") }).await;
    assert_eq!(b.expect().await, ServerMessage::Data { from: 1, payload: Bytes::from_static(b"reliable") });
    a.datagram(&ClientMessage::Data { target: Target::All, payload: Bytes::from_static(b"fast") });
    assert_eq!(b.expect().await, ServerMessage::Data { from: 1, payload: Bytes::from_static(b"fast") });
    assert_eq!(a.expect().await, ServerMessage::Data { from: 1, payload: Bytes::from_static(b"fast") });
    b.send(&ClientMessage::Data { target: Target::Player(1), payload: Bytes::from_static(b"dm") }).await;
    assert_eq!(a.expect().await, ServerMessage::Data { from: 2, payload: Bytes::from_static(b"dm") });
    a.datagram(&ClientMessage::Ping { client_time: 42 });
    assert!(matches!(a.expect().await, ServerMessage::Pong { client_time: 42, .. }));
    b.send(&ClientMessage::Leave).await;
    assert_eq!(b.expect().await, ServerMessage::Bye { reason: reason::LEFT });
    assert_eq!(a.expect().await, ServerMessage::Left { player: 2, reason: reason::LEFT });
    assert!(b.closed().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusals() {
    let s = stack().await;
    s.room("tiny", json!({ "max_players": 1 }));
    let mut bad = s.connect("not.a-token").await;
    assert_eq!(bad.expect().await, ServerMessage::Error { code: code::BAD_TOKEN, message: "bad token".into() });
    let tok = s.token("tiny", "p1");
    s.api("DELETE", "/v1/rooms/tiny", None, &s.run.api_key);
    let mut gone = s.connect(&tok).await;
    assert!(matches!(gone.expect().await, ServerMessage::Error { code: code::NO_ROOM, .. }));
    s.room("tiny", json!({ "max_players": 1 }));
    let mut one = s.connect(&s.token("tiny", "p1")).await;
    assert!(matches!(one.expect().await, ServerMessage::Welcome { .. }));
    let mut two = s.connect(&s.token("tiny", "p2")).await;
    assert!(matches!(two.expect().await, ServerMessage::Error { code: code::FULL, .. }));
    s.room("other", json!({}));
    let (_, v) = s.api("POST", "/v1/rooms", Some(json!({ "game": "h", "room": "h1" })), &s.run.api_key);
    assert_eq!(v["game"], "h");
    let conn = s.ep.connect(s.run.quic, "localhost").unwrap().await.unwrap();
    let session = Session::raw(conn);
    let (mut send, _recv) = session.open_bi().await.unwrap();
    send.write_all(&client_record(&ClientMessage::Ping { client_time: 1 })).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(5), session.closed()).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_replaces_old_connection() {
    let s = stack().await;
    s.room("r", json!({}));
    let tok = s.token("r", "same");
    let mut first = s.connect(&tok).await;
    assert!(matches!(first.expect().await, ServerMessage::Welcome { player: 1, reconnected: false, .. }));
    let mut second = s.connect(&tok).await;
    assert!(matches!(second.expect().await, ServerMessage::Welcome { player: 1, reconnected: true, .. }));
    assert_eq!(first.expect().await, ServerMessage::Bye { reason: reason::REPLACED });
    assert!(first.closed().await);
    let mut other = s.connect(&s.token("r", "other")).await;
    assert!(matches!(other.expect().await, ServerMessage::Welcome { player: 2, ref players, .. } if players.len() == 2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limit_closes_abusers() {
    let s = stack().await;
    s.room("r", json!({}));
    let mut c = s.connect(&s.token("r", "spammer")).await;
    assert!(matches!(c.expect().await, ServerMessage::Welcome { .. }));
    for _ in 0..200 {
        c.datagram(&ClientMessage::Data { target: Target::All, payload: Bytes::from_static(b"x") });
    }
    assert!(c.closed().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_fallback() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let s = stack().await;
    s.room("w", json!({}));
    let ws: SocketAddr = s.run.ws.unwrap();
    let (mut sock, _) = tokio_tungstenite::connect_async(format!("ws://{ws}/")).await.unwrap();
    let mut hello = vec![0u8];
    hello.extend_from_slice(&client_record(&ClientMessage::Hello { token: s.token("w", "web") }));
    sock.send(Message::Binary(hello.into())).await.unwrap();
    let mut quic = s.connect(&s.token("w", "native")).await;
    assert!(matches!(quic.expect().await, ServerMessage::Welcome { player: 2, .. }));
    let Some(Ok(Message::Binary(mut b))) = sock.next().await else { panic!("no welcome") };
    assert_eq!(b[0], 0);
    let _ = b.split_to(1);
    let mut sb = StreamBuffer::default();
    sb.push(&b);
    assert!(matches!(ServerMessage::read(sb.pop().unwrap()).unwrap(), ServerMessage::Welcome { player: 1, .. }));
    let mut dg = vec![1u8];
    dg.extend_from_slice(&Datagram::frame(1, &client_record(&ClientMessage::Data { target: Target::Others, payload: Bytes::from_static(b"from web") })));
    sock.send(Message::Binary(dg.into())).await.unwrap();
    assert_eq!(quic.expect().await, ServerMessage::Data { from: 1, payload: Bytes::from_static(b"from web") });
    quic.send(&ClientMessage::Data { target: Target::All, payload: Bytes::from_static(b"to web") }).await;
    loop {
        let Some(Ok(Message::Binary(mut b))) = sock.next().await else { panic!("no data") };
        let lane = b.split_to(1)[0];
        assert_eq!(lane, 0);
        let mut sb = StreamBuffer::default();
        sb.push(&b);
        match ServerMessage::read(sb.pop().unwrap()).unwrap() {
            ServerMessage::Joined { .. } => continue,
            m => {
                assert_eq!(m, ServerMessage::Data { from: 2, payload: Bytes::from_static(b"to web") });
                break;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_routes_rooms_across_nodes() {
    let a = stack().await;
    let b = stack().await;
    let nodes = || {
        vec![
            Node { url: format!("http://{}/", a.run.http), key: a.run.api_key.clone() },
            Node { url: format!("http://{}", b.run.http), key: b.run.api_key.clone() },
        ]
    };
    let (fl, task) = volsgames_control::fleet::serve(Arc::new(Fleet::new(nodes(), hash_key("fleet-key"))), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let f = |m, p: &str, body| api(fl, m, p, body, "fleet-key");
    assert_eq!(api(fl, "GET", "/v1/rooms", None, "wrong").0, 401);
    for r in ["r1", "r2", "r3", "r4"] {
        let (s, v) = f("POST", "/v1/rooms", Some(json!({ "game": "g", "room": r })));
        assert_eq!(s, 201, "{v}");
        assert!(v["node"].as_str().unwrap().starts_with("http://"));
    }
    assert_eq!(a.api("GET", "/v1/rooms", None, &a.run.api_key).1.as_array().unwrap().len(), 2);
    assert_eq!(b.api("GET", "/v1/rooms", None, &b.run.api_key).1.as_array().unwrap().len(), 2);
    assert_eq!(f("GET", "/v1/rooms", None).1.as_array().unwrap().len(), 4);
    let (s, v) = f("GET", "/v1/rooms/r2", None);
    assert_eq!(s, 200);
    assert_eq!(v["node"], format!("http://{}", b.run.http));
    let (s, tok) = f("POST", "/v1/rooms/r2/tokens", Some(json!({ "player": "p" })));
    assert_eq!(s, 201, "{tok}");
    assert_eq!(tok["endpoints"]["quic"], b.run.quic.to_string());
    let mut c = b.connect(tok["token"].as_str().unwrap()).await;
    assert!(matches!(c.expect().await, ServerMessage::Welcome { player: 1, .. }));
    let nodes_view = f("GET", "/v1/nodes", None).1;
    assert_eq!(nodes_view[1]["players"], 1);
    assert_eq!(nodes_view[0]["rooms"], 2);
    let (fl2, task2) = volsgames_control::fleet::serve(Arc::new(Fleet::new(nodes(), hash_key("k2"))), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    assert_eq!(api(fl2, "GET", "/v1/rooms/r3", None, "k2").1["node"], format!("http://{}", a.run.http));
    assert_eq!(api(fl2, "GET", "/v1/rooms/nope", None, "k2").0, 404);
    assert_eq!(f("DELETE", "/v1/rooms/r1", None).0, 204);
    assert_eq!(f("DELETE", "/v1/rooms/r1", None).0, 404);
    b.run.stop();
    assert_eq!(f("GET", "/v1/rooms/r4", None).0, 503);
    let (s, v) = f("POST", "/v1/rooms", Some(json!({ "game": "g", "room": "r5" })));
    assert_eq!(s, 201, "{v}");
    assert_eq!(v["node"], format!("http://{}", a.run.http));
    let nodes_view = f("GET", "/v1/nodes", None).1;
    assert_eq!(nodes_view[1]["up"], false);
    assert_eq!(f("GET", "/v1/rooms", None).1.as_array().unwrap().len(), 2);
    task.abort();
    task2.abort();
}
