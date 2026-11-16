use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Request;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;
use volsgames_proto::{Claims, code};
use volsgames_room::{Admin, Dump, Host, RoomConfig, RoomInfo, SetError, Write, now_ms};

pub mod fleet;

#[derive(Clone, Serialize)]
pub struct Endpoints {
    pub quic: String,
    pub webtransport: String,
    pub websocket: Option<String>,
    pub cert_hash: Option<String>,
}

pub struct Control {
    pub host: Arc<Host>,
    pub signing: SigningKey,
    pub api_key_hash: [u8; 32],
    pub tenant: String,
    pub endpoints: Endpoints,
    pub max_token_ttl: Duration,
}

pub fn hash_key(key: &str) -> [u8; 32] {
    Sha256::digest(key.as_bytes()).into()
}

pub fn random_key(prefix: &str) -> String {
    let bytes: [u8; 24] = rand::random();
    let mut s = String::with_capacity(prefix.len() + 48);
    s.push_str(prefix);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[derive(Deserialize)]
pub struct CreateRoom {
    pub game: String,
    pub room: Option<String>,
    pub tick_rate: Option<u16>,
    pub max_players: Option<u16>,
    pub reconnect_grace_ms: Option<u64>,
    pub empty_ttl_ms: Option<u64>,
    pub flush_on_tick: Option<bool>,
    pub max_state_bytes: Option<usize>,
}

#[derive(Deserialize)]
pub struct PatchRoom {
    pub locked: Option<bool>,
}

#[derive(Serialize)]
pub struct RoomView {
    pub room: String,
    pub game: String,
    pub tick_rate: u16,
    pub max_players: u16,
    pub players: u32,
    pub created_ms: u64,
    pub reconnect_grace_ms: u64,
    pub empty_ttl_ms: u64,
    pub flush_on_tick: bool,
    pub max_state_bytes: usize,
    pub locked: bool,
}

#[derive(Serialize)]
pub struct PlayerView {
    pub id: u32,
    pub player: String,
    pub name: String,
    pub connected: bool,
}

#[derive(Serialize)]
pub struct PlayersView {
    pub host: Option<u32>,
    pub players: Vec<PlayerView>,
}

#[derive(Serialize)]
pub struct EntryView {
    pub scope: u32,
    pub key: String,
    pub value: String,
    pub rev: u64,
    pub by: u32,
}

#[derive(Serialize)]
pub struct StateView {
    pub rev: u64,
    pub entries: Vec<EntryView>,
}

#[derive(Deserialize)]
pub struct SetState {
    pub key: String,
    pub value: Value,
    pub player: Option<String>,
    pub expect: Option<u64>,
}

#[derive(Serialize)]
pub struct SetView {
    pub scope: u32,
    pub key: String,
    pub rev: u64,
}

#[derive(Deserialize)]
pub struct MintToken {
    pub player: String,
    pub name: Option<String>,
    pub ttl_s: Option<u64>,
}

#[derive(Serialize)]
pub struct TokenView {
    pub token: String,
    pub exp: u64,
    pub room: String,
    pub endpoints: Endpoints,
}

#[derive(Serialize)]
struct Problem {
    error: &'static str,
    detail: String,
}

pub(crate) fn problem(status: StatusCode, error: &'static str, detail: impl Into<String>) -> Response {
    (status, Json(Problem { error, detail: detail.into() })).into_response()
}

pub(crate) async fn auth(State(hash): State<[u8; 32]>, req: Request, next: Next) -> Response {
    let given = req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")).unwrap_or("");
    if given.is_empty() || hash_key(given) != hash {
        return problem(StatusCode::UNAUTHORIZED, "unauthorized", "missing or wrong API key");
    }
    next.run(req).await
}

fn valid_id(s: &str, max: usize) -> bool {
    !s.is_empty() && s.len() <= max && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b':')
}

fn view(room: &str, info: &RoomInfo) -> RoomView {
    let cfg = &info.config;
    RoomView {
        room: room.to_string(),
        game: cfg.game.to_string(),
        tick_rate: cfg.tick_rate,
        max_players: cfg.max_players,
        players: info.players,
        created_ms: info.created_ms,
        reconnect_grace_ms: cfg.reconnect_grace.as_millis() as u64,
        empty_ttl_ms: cfg.empty_ttl.as_millis() as u64,
        flush_on_tick: cfg.flush_on_tick,
        max_state_bytes: cfg.max_state,
        locked: info.locked,
    }
}

fn no_room() -> Response {
    problem(StatusCode::NOT_FOUND, "no_room", "no such room")
}

async fn ask<T>(ctl: &Control, room: &str, op: impl FnOnce(oneshot::Sender<T>) -> Admin) -> Option<T> {
    let (reply, rx) = oneshot::channel();
    if !ctl.host.admin(room, op(reply)) {
        return None;
    }
    tokio::time::timeout(Duration::from_secs(5), rx).await.ok()?.ok()
}

async fn inspect(ctl: &Control, room: &str) -> Option<Dump> {
    ask(ctl, room, Admin::Inspect).await
}

async fn create_room(State(ctl): State<Arc<Control>>, Json(req): Json<CreateRoom>) -> Response {
    if !valid_id(&req.game, 64) {
        return problem(StatusCode::BAD_REQUEST, "bad_game", "game id: 1-64 chars of [A-Za-z0-9-_.:]");
    }
    let room = req.room.unwrap_or_else(|| random_key("r_")[..18].to_string());
    if !valid_id(&room, 64) {
        return problem(StatusCode::BAD_REQUEST, "bad_room", "room id: 1-64 chars of [A-Za-z0-9-_.:]");
    }
    let tick_rate = req.tick_rate.unwrap_or(30).clamp(1, 240);
    let max_players = req.max_players.unwrap_or(64).clamp(1, 1000);
    let cfg = RoomConfig {
        game: Arc::from(req.game.as_str()),
        tick_rate,
        max_players,
        reconnect_grace: Duration::from_millis(req.reconnect_grace_ms.unwrap_or(20_000).min(600_000)),
        empty_ttl: Duration::from_millis(req.empty_ttl_ms.unwrap_or(60_000).clamp(1_000, 3_600_000)),
        flush_on_tick: req.flush_on_tick.unwrap_or(false),
        max_groups: 16,
        max_state: req.max_state_bytes.unwrap_or(65_536).min(1 << 20),
    };
    if !ctl.host.create(Arc::from(room.as_str()), cfg.clone()) {
        return problem(StatusCode::CONFLICT, "exists", "room already exists");
    }
    tracing::info!(room, game = %cfg.game, "room create");
    let info = RoomInfo { worker: 0, config: cfg, created_ms: now_ms(), players: 0, locked: false };
    (StatusCode::CREATED, Json(view(&room, &info))).into_response()
}

async fn list_rooms(State(ctl): State<Arc<Control>>) -> Response {
    let rooms: Vec<RoomView> = ctl.host.list().iter().map(|(id, info)| view(id, info)).collect();
    Json(rooms).into_response()
}

async fn get_room(State(ctl): State<Arc<Control>>, Path(room): Path<String>) -> Response {
    match ctl.host.get(&room) {
        Some(info) => Json(view(&room, &info)).into_response(),
        None => no_room(),
    }
}

async fn patch_room(State(ctl): State<Arc<Control>>, Path(room): Path<String>, Json(req): Json<PatchRoom>) -> Response {
    let info = match req.locked {
        Some(locked) => ctl.host.lock(&room, locked),
        None => ctl.host.get(&room),
    };
    let Some(info) = info else { return no_room() };
    if let Some(locked) = req.locked {
        tracing::info!(room, locked, "room lock");
    }
    Json(view(&room, &info)).into_response()
}

async fn delete_room(State(ctl): State<Arc<Control>>, Path(room): Path<String>) -> Response {
    if ctl.host.close(&room) {
        tracing::info!(room, "room close");
        StatusCode::NO_CONTENT.into_response()
    } else {
        no_room()
    }
}

async fn players(State(ctl): State<Arc<Control>>, Path(room): Path<String>) -> Response {
    let Some(d) = inspect(&ctl, &room).await else { return no_room() };
    let players = d.players.into_iter().map(|p| PlayerView { id: p.id, player: p.identity.to_string(), name: p.name, connected: p.connected }).collect();
    Json(PlayersView { host: (d.host != 0).then_some(d.host), players }).into_response()
}

async fn kick(State(ctl): State<Arc<Control>>, Path((room, player)): Path<(String, String)>) -> Response {
    match ask(&ctl, &room, |reply| Admin::Kick(Arc::from(player.as_str()), reply)).await {
        None => no_room(),
        Some(false) => problem(StatusCode::NOT_FOUND, "no_player", "no such player in this room"),
        Some(true) => StatusCode::NO_CONTENT.into_response(),
    }
}

async fn get_state(State(ctl): State<Arc<Control>>, Path(room): Path<String>) -> Response {
    let Some(d) = inspect(&ctl, &room).await else { return no_room() };
    let entries = d.state.into_iter().map(|e| EntryView { scope: e.scope, key: e.key, value: B64.encode(&e.value), rev: e.rev, by: e.by }).collect();
    Json(StateView { rev: d.rev, entries }).into_response()
}

async fn set_state(State(ctl): State<Arc<Control>>, Path(room): Path<String>, Json(req): Json<SetState>) -> Response {
    let value = match &req.value {
        Value::Null => None,
        Value::String(s) => match B64.decode(s) {
            Ok(v) => Some(Bytes::from(v)),
            Err(_) => return problem(StatusCode::BAD_REQUEST, "bad_value", "value: base64 string or null"),
        },
        _ => return problem(StatusCode::BAD_REQUEST, "bad_value", "value: base64 string or null"),
    };
    let write = Write { player: req.player.as_deref().map(Arc::from), key: req.key.clone(), value, expect: req.expect };
    match ask(&ctl, &room, |reply| Admin::Set(write, reply)).await {
        None => no_room(),
        Some(Ok(w)) => Json(SetView { scope: w.scope, key: req.key, rev: w.rev }).into_response(),
        Some(Err(SetError::NoPlayer)) => problem(StatusCode::NOT_FOUND, "no_player", "no such player in this room"),
        Some(Err(SetError::Refused(code::CONFLICT))) => problem(StatusCode::CONFLICT, "conflict", "the entry changed since the expected revision"),
        Some(Err(SetError::Refused(code::TOO_LARGE))) => {
            problem(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "key over 64 bytes, value over 8192 bytes or room state budget used up")
        }
        Some(Err(SetError::Refused(_))) => problem(StatusCode::BAD_REQUEST, "bad_key", "key: 1-64 bytes"),
    }
}

async fn mint_token(State(ctl): State<Arc<Control>>, Path(room): Path<String>, Json(req): Json<MintToken>) -> Response {
    let Some(RoomInfo { config: cfg, .. }) = ctl.host.get(&room) else {
        return no_room();
    };
    if !valid_id(&req.player, 128) {
        return problem(StatusCode::BAD_REQUEST, "bad_player", "player id: 1-128 chars of [A-Za-z0-9-_.:]");
    }
    let ttl = Duration::from_secs(req.ttl_s.unwrap_or(300)).min(ctl.max_token_ttl);
    let exp = now_ms() / 1000 + ttl.as_secs();
    let mut name = req.name.unwrap_or_else(|| req.player.clone());
    name.truncate(volsgames_proto::MAX_NAME);
    let claims = Claims { exp, tenant: ctl.tenant.clone(), game: cfg.game.to_string(), room: room.clone(), player: req.player, name };
    let token = claims.sign(&ctl.signing);
    tracing::info!(room, "token mint");
    (StatusCode::CREATED, Json(TokenView { token, exp, room, endpoints: ctl.endpoints.clone() })).into_response()
}

async fn endpoints(State(ctl): State<Arc<Control>>) -> Response {
    Json(ctl.endpoints.clone()).into_response()
}

async fn metrics(State(ctl): State<Arc<Control>>) -> Response {
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], ctl.host.metrics().render()).into_response()
}

async fn healthz() -> &'static str {
    "ok"
}

pub fn router(ctl: Arc<Control>) -> Router {
    let private = Router::new()
        .route("/v1/rooms", post(create_room).get(list_rooms))
        .route("/v1/rooms/{room}", get(get_room).patch(patch_room).delete(delete_room))
        .route("/v1/rooms/{room}/tokens", post(mint_token))
        .route("/v1/rooms/{room}/players", get(players))
        .route("/v1/rooms/{room}/players/{player}", delete(kick))
        .route("/v1/rooms/{room}/state", get(get_state).post(set_state))
        .route_layer(middleware::from_fn_with_state(ctl.api_key_hash, auth));
    Router::new().route("/healthz", get(healthz)).route("/metrics", get(metrics)).route("/v1/endpoints", get(endpoints)).merge(private).with_state(ctl)
}

pub async fn serve(ctl: Arc<Control>, addr: SocketAddr) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    let app = router(ctl);
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((addr, task))
}
