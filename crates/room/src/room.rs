use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc::error::TrySendError;
use volsgames_proto::{ClientMessage, DATAGRAM_BUDGET, Lane, MAX_NAME, ServerMessage, Target, code, reason, server_record};

use crate::{ConnHandle, JoinRequest, Joined, Metrics, Outbound, RoomConfig, RoomId, now_ms};

struct Player {
    identity: Arc<str>,
    name: String,
    conn: Option<ConnHandle>,
    gone_since: Option<Instant>,
    groups: Vec<u32>,
    batch: BytesMut,
}

pub struct Room {
    id: RoomId,
    cfg: RoomConfig,
    players: Vec<Option<Player>>,
    by_identity: HashMap<Arc<str>, u32>,
    pub interval: Duration,
    pub next_tick: Instant,
    tick: u64,
    empty_since: Option<Instant>,
    pub closed: bool,
    counter: Arc<AtomicU32>,
    metrics: Arc<Metrics>,
    kick: Vec<(u32, u8)>,
}

impl Room {
    pub fn new(id: RoomId, cfg: RoomConfig, counter: Arc<AtomicU32>, metrics: Arc<Metrics>) -> Self {
        let interval = Duration::from_secs_f64(1.0 / f64::from(cfg.tick_rate.max(1)));
        tracing::info!(room = %id, game = %cfg.game, tick_rate = cfg.tick_rate, "room created");
        Self {
            id,
            players: Vec::with_capacity(cfg.max_players as usize),
            cfg,
            by_identity: HashMap::new(),
            interval,
            next_tick: Instant::now() + interval,
            tick: 0,
            empty_since: Some(Instant::now()),
            closed: false,
            counter,
            metrics,
            kick: Vec::new(),
        }
    }

    fn slot(&mut self, player: u32) -> Option<&mut Player> {
        self.players.get_mut(player.wrapping_sub(1) as usize).and_then(|p| p.as_mut())
    }

    fn present(&self) -> u32 {
        self.players.iter().flatten().filter(|p| p.conn.is_some() || p.gone_since.is_some()).count() as u32
    }

    fn roster(&self) -> Vec<(u32, String)> {
        self.players.iter().enumerate().filter_map(|(i, p)| p.as_ref().map(|p| (i as u32 + 1, p.name.clone()))).collect()
    }

    pub fn join(&mut self, req: JoinRequest) {
        if self.closed {
            let _ = req.reply.send(Err(code::NO_ROOM));
            return;
        }
        let mut name = req.name;
        name.truncate(MAX_NAME);
        let existing = self.by_identity.get(&req.identity).copied();
        let (player, reconnected) = match existing {
            Some(id) => {
                let p = self.slot(id).expect("identity index");
                if let Some(old) = p.conn.take() {
                    let _ = old.tx.try_send(Outbound::Frame(Lane::Reliable, server_record(&ServerMessage::Bye { reason: reason::REPLACED })));
                    let _ = old.tx.try_send(Outbound::Close);
                }
                p.gone_since = None;
                p.conn = Some(req.conn.clone());
                p.batch.clear();
                (id, true)
            }
            None => {
                if self.present() >= u32::from(self.cfg.max_players) {
                    Metrics::inc(&self.metrics.rejected);
                    let _ = req.reply.send(Err(code::FULL));
                    return;
                }
                let free = self.players.iter().position(|p| p.is_none());
                let idx = match free {
                    Some(i) => i,
                    None => {
                        self.players.push(None);
                        self.players.len() - 1
                    }
                };
                let id = idx as u32 + 1;
                self.players[idx] = Some(Player {
                    identity: req.identity.clone(),
                    name: name.clone(),
                    conn: Some(req.conn.clone()),
                    gone_since: None,
                    groups: Vec::new(),
                    batch: BytesMut::new(),
                });
                self.by_identity.insert(req.identity.clone(), id);
                let rec = server_record(&ServerMessage::Joined { player: id, name: name.clone() });
                self.fanout(Lane::Reliable, rec, id, Target::Others);
                Metrics::inc(&self.metrics.players);
                (id, false)
            }
        };
        self.empty_since = None;
        self.counter.store(self.present(), Relaxed);
        let welcome = ServerMessage::Welcome {
            player,
            room: self.id.to_string(),
            tick_rate: self.cfg.tick_rate,
            server_time: now_ms(),
            reconnected,
            players: self.roster(),
        };
        if req.conn.tx.try_send(Outbound::Frame(Lane::Reliable, server_record(&welcome))).is_err() {
            self.disconnect(player, req.conn.id);
            let _ = req.reply.send(Err(code::PROTOCOL));
            return;
        }
        tracing::info!(room = %self.id, player, conn = req.conn.id, reconnected, "joined");
        let _ = req.reply.send(Ok(Joined { player, reconnected }));
        self.flush_kicks();
    }

    pub fn message(&mut self, player: u32, conn: u64, lane: Lane, msg: ClientMessage) {
        let Some(p) = self.slot(player) else { return };
        if p.conn.as_ref().map(|c| c.id) != Some(conn) {
            return;
        }
        Metrics::inc(&self.metrics.messages_in);
        match msg {
            ClientMessage::Data { target, payload } => {
                Metrics::add(&self.metrics.bytes_in, payload.len() as u64);
                let rec = server_record(&ServerMessage::Data { from: player, payload });
                self.fanout(lane, rec, player, target);
            }
            ClientMessage::Ping { client_time } => {
                let rec = server_record(&ServerMessage::Pong { client_time, server_time: now_ms() });
                self.send_to(player, lane, rec);
            }
            ClientMessage::Subscribe { group } => {
                let max = usize::from(self.cfg.max_groups);
                if let Some(p) = self.slot(player)
                    && !p.groups.contains(&group)
                    && p.groups.len() < max
                {
                    p.groups.push(group);
                }
            }
            ClientMessage::Unsubscribe { group } => {
                if let Some(p) = self.slot(player) {
                    p.groups.retain(|g| *g != group);
                }
            }
            ClientMessage::Leave => {
                self.remove(player, reason::LEFT);
            }
            ClientMessage::Hello { .. } => {}
        }
        self.flush_kicks();
    }

    pub fn disconnect(&mut self, player: u32, conn: u64) {
        let grace = self.cfg.reconnect_grace;
        let Some(p) = self.slot(player) else { return };
        if p.conn.as_ref().map(|c| c.id) != Some(conn) {
            return;
        }
        p.conn = None;
        p.batch.clear();
        if grace.is_zero() {
            self.remove(player, reason::TIMEOUT);
        } else {
            p.gone_since = Some(Instant::now());
            tracing::info!(room = %self.id, player, conn, "disconnected, grace started");
        }
        self.flush_kicks();
    }

    fn remove(&mut self, player: u32, why: u8) {
        let Some(slot) = self.players.get_mut(player.wrapping_sub(1) as usize) else { return };
        let Some(p) = slot.take() else { return };
        self.by_identity.remove(&p.identity);
        if let Some(c) = p.conn {
            let _ = c.tx.try_send(Outbound::Frame(Lane::Reliable, server_record(&ServerMessage::Bye { reason: why })));
            let _ = c.tx.try_send(Outbound::Close);
        }
        Metrics::dec(&self.metrics.players);
        tracing::info!(room = %self.id, player, why, "left");
        let rec = server_record(&ServerMessage::Left { player, reason: why });
        self.fanout(Lane::Reliable, rec, player, Target::Others);
        let n = self.present();
        self.counter.store(n, Relaxed);
        if n == 0 {
            self.empty_since = Some(Instant::now());
        }
    }

    fn send_to(&mut self, player: u32, lane: Lane, rec: Bytes) {
        let flush_on_tick = self.cfg.flush_on_tick;
        let metrics = self.metrics.clone();
        let Some(p) = self.slot(player) else { return };
        if let Some(why) = deliver(p, lane, &rec, flush_on_tick, &metrics) {
            self.kick.push((player, why));
        }
    }

    fn fanout(&mut self, lane: Lane, rec: Bytes, from: u32, target: Target) {
        let flush_on_tick = self.cfg.flush_on_tick;
        let metrics = self.metrics.clone();
        match target {
            Target::Player(id) => {
                if let Some(p) = self.slot(id)
                    && let Some(why) = deliver(p, lane, &rec, flush_on_tick, &metrics)
                {
                    self.kick.push((id, why));
                }
            }
            _ => {
                for (i, p) in self.players.iter_mut().enumerate() {
                    let id = i as u32 + 1;
                    let Some(p) = p else { continue };
                    let wanted = match target {
                        Target::All => true,
                        Target::Others => id != from,
                        Target::Group(g) => id != from && p.groups.contains(&g),
                        Target::Player(_) => unreachable!(),
                    };
                    if wanted && let Some(why) = deliver(p, lane, &rec, flush_on_tick, &metrics) {
                        self.kick.push((id, why));
                    }
                }
            }
        }
    }

    fn flush_kicks(&mut self) {
        while let Some((id, why)) = self.kick.pop() {
            Metrics::inc(&self.metrics.kicked_slow);
            self.remove(id, why);
        }
    }

    pub fn tick(&mut self, now: Instant) {
        self.tick += 1;
        self.next_tick += self.interval;
        Metrics::inc(&self.metrics.ticks);
        let grace = self.cfg.reconnect_grace;
        let mut expired = Vec::new();
        for (i, p) in self.players.iter_mut().enumerate() {
            let Some(p) = p else { continue };
            if !p.batch.is_empty() {
                flush(p, &self.metrics);
            }
            if let Some(t) = p.gone_since
                && now.duration_since(t) >= grace
            {
                expired.push(i as u32 + 1);
            }
        }
        for id in expired {
            self.remove(id, reason::TIMEOUT);
        }
        self.flush_kicks();
        if let Some(t) = self.empty_since
            && self.present() == 0
            && now.duration_since(t) >= self.cfg.empty_ttl
        {
            self.close();
        }
    }

    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        for id in 1..=self.players.len() as u32 {
            self.remove(id, reason::ROOM_CLOSED);
        }
        self.closed = true;
    }
}

fn deliver(p: &mut Player, lane: Lane, rec: &Bytes, flush_on_tick: bool, metrics: &Metrics) -> Option<u8> {
    let c = p.conn.as_ref()?;
    match lane {
        Lane::Reliable => match c.tx.try_send(Outbound::Frame(Lane::Reliable, rec.clone())) {
            Ok(()) => {
                Metrics::inc(&metrics.messages_out);
                Metrics::add(&metrics.bytes_out, rec.len() as u64);
                None
            }
            Err(TrySendError::Full(_)) => Some(reason::SLOW),
            Err(TrySendError::Closed(_)) => None,
        },
        Lane::Unreliable => {
            if !flush_on_tick {
                match c.tx.try_send(Outbound::Frame(Lane::Unreliable, rec.clone())) {
                    Ok(()) => {
                        Metrics::inc(&metrics.messages_out);
                        Metrics::add(&metrics.bytes_out, rec.len() as u64);
                    }
                    Err(_) => Metrics::inc(&metrics.dropped_unreliable),
                }
                return None;
            }
            if p.batch.len() + rec.len() > DATAGRAM_BUDGET && !p.batch.is_empty() {
                flush(p, metrics);
            }
            p.batch.extend_from_slice(rec);
            Metrics::inc(&metrics.messages_out);
            None
        }
    }
}

fn flush(p: &mut Player, metrics: &Metrics) {
    let Some(c) = p.conn.as_ref() else {
        p.batch.clear();
        return;
    };
    let frame = p.batch.split().freeze();
    if c.tx.try_send(Outbound::Frame(Lane::Unreliable, frame)).is_err() {
        Metrics::inc(&metrics.dropped_unreliable);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, oneshot};
    use volsgames_proto::Records;

    fn conn(id: u64) -> (ConnHandle, mpsc::Receiver<Outbound>) {
        let (tx, rx) = mpsc::channel(64);
        (ConnHandle { id, tx }, rx)
    }

    fn room() -> Room {
        Room::new(Arc::from("r1"), RoomConfig { game: Arc::from("g"), max_players: 2, ..Default::default() }, Arc::default(), Arc::default())
    }

    fn join(r: &mut Room, c: &ConnHandle, who: &str) -> Result<Joined, u16> {
        let (reply, mut rx) = oneshot::channel();
        r.join(JoinRequest { room: Arc::from("r1"), conn: c.clone(), identity: Arc::from(who), name: who.to_string(), reply });
        rx.try_recv().unwrap()
    }

    fn drain(rx: &mut mpsc::Receiver<Outbound>) -> Vec<ServerMessage> {
        let mut out = Vec::new();
        while let Ok(o) = rx.try_recv() {
            if let Outbound::Frame(_, b) = o {
                for rec in Records::new(b) {
                    out.push(ServerMessage::read(rec.unwrap()).unwrap());
                }
            }
        }
        out
    }

    #[test]
    fn join_broadcast_and_relay() {
        let mut r = room();
        let (a, mut arx) = conn(1);
        let (b, mut brx) = conn(2);
        assert_eq!(join(&mut r, &a, "alice"), Ok(Joined { player: 1, reconnected: false }));
        assert_eq!(join(&mut r, &b, "bob"), Ok(Joined { player: 2, reconnected: false }));
        let (c, _crx) = conn(3);
        assert_eq!(join(&mut r, &c, "carol"), Err(code::FULL));
        let am = drain(&mut arx);
        assert!(matches!(am[0], ServerMessage::Welcome { player: 1, .. }));
        assert_eq!(am[1], ServerMessage::Joined { player: 2, name: "bob".into() });
        r.message(1, 1, Lane::Unreliable, ClientMessage::Data { target: Target::Others, payload: Bytes::from_static(b"hi") });
        let bm = drain(&mut brx);
        assert!(matches!(&bm[0], ServerMessage::Welcome { player: 2, players, .. } if players.len() == 2));
        assert_eq!(bm[1], ServerMessage::Data { from: 1, payload: Bytes::from_static(b"hi") });
        assert!(drain(&mut arx).is_empty());
        r.message(1, 99, Lane::Reliable, ClientMessage::Data { target: Target::All, payload: Bytes::from_static(b"stale") });
        assert!(drain(&mut brx).is_empty());
    }

    #[test]
    fn reconnect_keeps_slot_and_replaces_connection() {
        let mut r = room();
        let (a, mut arx) = conn(1);
        let (b, mut brx) = conn(2);
        join(&mut r, &a, "alice").unwrap();
        join(&mut r, &b, "bob").unwrap();
        drain(&mut arx);
        drain(&mut brx);
        r.disconnect(1, 1);
        assert!(drain(&mut brx).is_empty());
        let (a2, mut a2rx) = conn(3);
        assert_eq!(join(&mut r, &a2, "alice"), Ok(Joined { player: 1, reconnected: true }));
        assert!(matches!(drain(&mut a2rx)[0], ServerMessage::Welcome { player: 1, reconnected: true, .. }));
        assert!(drain(&mut brx).is_empty());
        let (a3, _a3rx) = conn(4);
        assert_eq!(join(&mut r, &a3, "alice"), Ok(Joined { player: 1, reconnected: true }));
        assert_eq!(drain(&mut a2rx), vec![ServerMessage::Bye { reason: reason::REPLACED }]);
    }

    #[test]
    fn grace_expires_on_tick() {
        let mut r = Room::new(Arc::from("r1"), RoomConfig { reconnect_grace: Duration::from_millis(0), ..Default::default() }, Arc::default(), Arc::default());
        let (a, _arx) = conn(1);
        let (b, mut brx) = conn(2);
        join(&mut r, &a, "alice").unwrap();
        join(&mut r, &b, "bob").unwrap();
        drain(&mut brx);
        r.disconnect(1, 1);
        assert_eq!(drain(&mut brx), vec![ServerMessage::Left { player: 1, reason: reason::TIMEOUT }]);
        let (c, _crx) = conn(3);
        assert_eq!(join(&mut r, &c, "carol"), Ok(Joined { player: 1, reconnected: false }));
    }

    #[test]
    fn groups_and_batching() {
        let mut r = Room::new(Arc::from("r1"), RoomConfig { flush_on_tick: true, ..Default::default() }, Arc::default(), Arc::default());
        let (a, _arx) = conn(1);
        let (b, mut brx) = conn(2);
        let (c, mut crx) = conn(3);
        join(&mut r, &a, "a").unwrap();
        join(&mut r, &b, "b").unwrap();
        join(&mut r, &c, "c").unwrap();
        drain(&mut brx);
        drain(&mut crx);
        r.message(2, 2, Lane::Reliable, ClientMessage::Subscribe { group: 5 });
        r.message(1, 1, Lane::Unreliable, ClientMessage::Data { target: Target::Group(5), payload: Bytes::from_static(b"g") });
        r.message(1, 1, Lane::Unreliable, ClientMessage::Data { target: Target::Group(5), payload: Bytes::from_static(b"h") });
        assert!(drain(&mut brx).is_empty());
        r.tick(Instant::now());
        assert_eq!(drain(&mut brx).len(), 2);
        assert!(drain(&mut crx).is_empty());
    }

    #[test]
    fn slow_reliable_consumer_is_kicked() {
        let mut r = room();
        let (a, _arx) = conn(1);
        let (tx, mut brx) = mpsc::channel(2);
        let b = ConnHandle { id: 2, tx };
        join(&mut r, &a, "a").unwrap();
        join(&mut r, &b, "b").unwrap();
        for _ in 0..4 {
            r.message(1, 1, Lane::Reliable, ClientMessage::Data { target: Target::All, payload: Bytes::from_static(b"x") });
        }
        assert_eq!(r.present(), 1);
        let mut n = 0;
        while brx.try_recv().is_ok() {
            n += 1;
        }
        assert_eq!(n, 2);
    }

    #[test]
    fn close_room_says_bye() {
        let mut r = room();
        let (a, mut arx) = conn(1);
        join(&mut r, &a, "a").unwrap();
        drain(&mut arx);
        r.close();
        assert!(r.closed);
        assert_eq!(drain(&mut arx), vec![ServerMessage::Bye { reason: reason::ROOM_CLOSED }]);
    }
}
