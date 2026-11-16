mod host;
mod metrics;
mod room;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use volsgames_proto::{ClientMessage, Lane};

pub use host::{Host, RoomEntry, RoomInfo};
pub use metrics::Metrics;

pub type RoomId = Arc<str>;

#[derive(Clone, Debug)]
pub struct RoomConfig {
    pub game: Arc<str>,
    pub tick_rate: u16,
    pub max_players: u16,
    pub reconnect_grace: Duration,
    pub empty_ttl: Duration,
    pub flush_on_tick: bool,
    pub max_groups: u8,
    pub max_state: usize,
}

impl Default for RoomConfig {
    fn default() -> Self {
        Self {
            game: Arc::from(""),
            tick_rate: 30,
            max_players: 64,
            reconnect_grace: Duration::from_secs(20),
            empty_ttl: Duration::from_secs(60),
            flush_on_tick: false,
            max_groups: 16,
            max_state: 65_536,
        }
    }
}

#[derive(Debug)]
pub enum Outbound {
    Frame(Lane, Bytes),
    Close,
}

#[derive(Clone, Debug)]
pub struct ConnHandle {
    pub id: u64,
    pub tx: mpsc::Sender<Outbound>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Joined {
    pub player: u32,
    pub reconnected: bool,
}

pub struct JoinRequest {
    pub room: RoomId,
    pub conn: ConnHandle,
    pub identity: Arc<str>,
    pub name: String,
    pub reply: oneshot::Sender<Result<Joined, u16>>,
}

#[derive(Default, Debug)]
pub struct Live {
    pub players: AtomicU32,
    pub locked: AtomicBool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerInfo {
    pub id: u32,
    pub identity: Arc<str>,
    pub name: String,
    pub connected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateEntry {
    pub scope: u32,
    pub key: String,
    pub value: Bytes,
    pub rev: u64,
    pub by: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dump {
    pub host: u32,
    pub rev: u64,
    pub players: Vec<PlayerInfo>,
    pub state: Vec<StateEntry>,
}

#[derive(Debug, Clone)]
pub struct Write {
    pub player: Option<Arc<str>>,
    pub key: String,
    pub value: Option<Bytes>,
    pub expect: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    pub scope: u32,
    pub rev: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetError {
    NoPlayer,
    Refused(u16),
}

pub enum Admin {
    Inspect(oneshot::Sender<Dump>),
    Kick(Arc<str>, oneshot::Sender<bool>),
    Set(Write, oneshot::Sender<Result<Written, SetError>>),
}

pub enum Command {
    Create { room: RoomId, config: RoomConfig, live: Arc<Live> },
    Join(JoinRequest),
    Message { room: RoomId, player: u32, conn: u64, lane: Lane, msg: ClientMessage },
    Disconnect { room: RoomId, player: u32, conn: u64 },
    Close { room: RoomId },
    Admin { room: RoomId, op: Admin },
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}
