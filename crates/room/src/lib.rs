mod host;
mod metrics;
mod room;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use volsgames_proto::{ClientMessage, Lane};

pub use host::{Host, RoomEntry};
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

pub enum Command {
    Create { room: RoomId, config: RoomConfig },
    Join(JoinRequest),
    Message { room: RoomId, player: u32, conn: u64, lane: Lane, msg: ClientMessage },
    Disconnect { room: RoomId, player: u32, conn: u64 },
    Close { room: RoomId },
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}
