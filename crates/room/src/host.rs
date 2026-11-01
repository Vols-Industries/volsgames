use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::time::Instant;

use dashmap::DashMap;

use crate::room::Room;
use crate::{Command, JoinRequest, Metrics, RoomConfig, RoomId, now_ms};

pub struct RoomEntry {
    pub worker: usize,
    pub config: RoomConfig,
    pub created_ms: u64,
    pub players: Arc<AtomicU32>,
}

pub struct Host {
    workers: Vec<Sender<Command>>,
    rooms: Arc<DashMap<RoomId, RoomEntry>>,
    metrics: Arc<Metrics>,
}

impl Host {
    pub fn start(workers: usize, metrics: Arc<Metrics>) -> Arc<Self> {
        let rooms: Arc<DashMap<RoomId, RoomEntry>> = Arc::new(DashMap::new());
        let mut senders = Vec::with_capacity(workers.max(1));
        for i in 0..workers.max(1) {
            let (tx, rx) = channel();
            let rooms = rooms.clone();
            let metrics = metrics.clone();
            std::thread::Builder::new().name(format!("room-{i}")).spawn(move || run(i, rx, rooms, metrics)).expect("spawn room worker");
            senders.push(tx);
        }
        Arc::new(Self { workers: senders, rooms, metrics })
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    pub fn workers(&self) -> usize {
        self.workers.len()
    }

    pub fn create(&self, room: RoomId, config: RoomConfig) -> bool {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        room.hash(&mut h);
        let worker = (h.finish() % self.workers.len() as u64) as usize;
        let entry = RoomEntry { worker, config: config.clone(), created_ms: now_ms(), players: Arc::new(AtomicU32::new(0)) };
        if self.rooms.contains_key(&room) {
            return false;
        }
        self.rooms.insert(room.clone(), entry);
        Metrics::inc(&self.metrics.rooms);
        Metrics::inc(&self.metrics.rooms_total);
        let _ = self.workers[worker].send(Command::Create { room, config });
        true
    }

    pub fn get(&self, room: &str) -> Option<(usize, RoomConfig, u64, u32)> {
        self.rooms.get(room).map(|e| (e.worker, e.config.clone(), e.created_ms, e.players.load(Relaxed)))
    }

    pub fn list(&self) -> Vec<(RoomId, RoomConfig, u64, u32)> {
        self.rooms.iter().map(|e| (e.key().clone(), e.config.clone(), e.created_ms, e.players.load(Relaxed))).collect()
    }

    pub fn join(&self, req: JoinRequest) -> Option<usize> {
        let worker = self.rooms.get(&req.room)?.worker;
        self.workers[worker].send(Command::Join(req)).ok()?;
        Some(worker)
    }

    pub fn send(&self, worker: usize, cmd: Command) {
        let _ = self.workers[worker].send(cmd);
    }

    pub fn close(&self, room: &str) -> bool {
        let Some(e) = self.rooms.get(room) else { return false };
        let worker = e.worker;
        drop(e);
        let _ = self.workers[worker].send(Command::Close { room: Arc::from(room) });
        true
    }
}

fn run(index: usize, rx: Receiver<Command>, rooms: Arc<DashMap<RoomId, RoomEntry>>, metrics: Arc<Metrics>) {
    let mut local: HashMap<RoomId, Room> = HashMap::new();
    let mut closed: Vec<RoomId> = Vec::new();
    tracing::debug!(worker = index, "room worker up");
    loop {
        let now = Instant::now();
        let next = local.values().map(|r| r.next_tick).min();
        let first = match next {
            Some(t) if t > now => rx.recv_timeout(t - now),
            Some(_) => rx.try_recv().map_err(|_| RecvTimeoutError::Timeout),
            None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
        };
        match first {
            Ok(cmd) => handle(cmd, &mut local, &rooms, &metrics),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        for _ in 0..1024 {
            match rx.try_recv() {
                Ok(cmd) => handle(cmd, &mut local, &rooms, &metrics),
                Err(_) => break,
            }
        }
        let now = Instant::now();
        for (id, room) in local.iter_mut() {
            if room.next_tick <= now {
                room.tick(now);
                if room.next_tick + room.interval < now {
                    Metrics::inc(&metrics.tick_late);
                    room.next_tick = now;
                }
            }
            if room.closed {
                closed.push(id.clone());
            }
        }
        for id in closed.drain(..) {
            local.remove(&id);
            rooms.remove(&id);
            Metrics::dec(&metrics.rooms);
            tracing::info!(room = %id, "room closed");
        }
    }
}

fn handle(cmd: Command, local: &mut HashMap<RoomId, Room>, rooms: &DashMap<RoomId, RoomEntry>, metrics: &Arc<Metrics>) {
    match cmd {
        Command::Create { room, config } => {
            let counter = rooms.get(&room).map(|e| e.players.clone()).unwrap_or_default();
            local.entry(room.clone()).or_insert_with(|| Room::new(room, config, counter, metrics.clone()));
        }
        Command::Join(req) => match local.get_mut(&req.room) {
            Some(r) => r.join(req),
            None => {
                let _ = req.reply.send(Err(volsgames_proto::code::NO_ROOM));
            }
        },
        Command::Message { room, player, conn, lane, msg } => {
            if let Some(r) = local.get_mut(&room) {
                r.message(player, conn, lane, msg);
            }
        }
        Command::Disconnect { room, player, conn } => {
            if let Some(r) = local.get_mut(&room) {
                r.disconnect(player, conn);
            }
        }
        Command::Close { room } => {
            if let Some(r) = local.get_mut(&room) {
                r.close();
            }
        }
    }
}
