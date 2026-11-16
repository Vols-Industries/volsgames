use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc::error::TrySendError;
use volsgames_proto::{ClientMessage, DATAGRAM_BUDGET, Lane, MAX_KEY, MAX_NAME, MAX_VALUE, ServerMessage, Target, code, reason, record, server_record};

use crate::{Admin, ConnHandle, Dump, JoinRequest, Joined, Live, Metrics, Outbound, PlayerInfo, RoomConfig, RoomId, SetError, StateEntry, Written, now_ms};

const ENTRY_COST: usize = 32;

struct Player {
    identity: Arc<str>,
    name: String,
    conn: Option<ConnHandle>,
    gone_since: Option<Instant>,
    groups: Vec<u32>,
    batch: BytesMut,
    since: u64,
}

struct Entry {
    value: Bytes,
    rev: u64,
    by: u32,
}

fn cost(key: &str, value: &[u8]) -> usize {
    key.len() + value.len() + ENTRY_COST
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
    live: Arc<Live>,
    metrics: Arc<Metrics>,
    kick: Vec<(u32, u8)>,
    state: BTreeMap<(u32, String), Entry>,
    state_bytes: usize,
    rev: u64,
    host: u32,
    joins: u64,
    banned: HashSet<Arc<str>>,
}

impl Room {
    pub fn new(id: RoomId, cfg: RoomConfig, live: Arc<Live>, metrics: Arc<Metrics>) -> Self {
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
            live,
            metrics,
            kick: Vec::new(),
            state: BTreeMap::new(),
            state_bytes: 0,
            rev: 0,
            host: 0,
            joins: 0,
            banned: HashSet::new(),
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
        if self.banned.contains(&req.identity) {
            let _ = req.reply.send(Err(code::FORBIDDEN));
            return;
        }
        let mut name = req.name;
        name.truncate(MAX_NAME);
        let existing = self.by_identity.get(&req.identity).copied();
        if existing.is_none() && self.live.locked.load(Relaxed) {
            let _ = req.reply.send(Err(code::LOCKED));
            return;
        }
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
                self.joins += 1;
                self.players[idx] = Some(Player {
                    identity: req.identity.clone(),
                    name: name.clone(),
                    conn: Some(req.conn.clone()),
                    gone_since: None,
                    groups: Vec::new(),
                    batch: BytesMut::new(),
                    since: self.joins,
                });
                self.by_identity.insert(req.identity.clone(), id);
                let rec = server_record(&ServerMessage::Joined { player: id, name: name.clone() });
                self.fanout(Lane::Reliable, rec, id, Target::Others);
                Metrics::inc(&self.metrics.players);
                (id, false)
            }
        };
        self.empty_since = None;
        self.live.players.store(self.present(), Relaxed);
        self.elect(player);
        let welcome = ServerMessage::Welcome {
            player,
            room: self.id.to_string(),
            tick_rate: self.cfg.tick_rate,
            server_time: now_ms(),
            reconnected,
            players: self.roster(),
            host: self.host,
            entries: self.state.len() as u32,
        };
        let mut out = BytesMut::with_capacity(256);
        record(&mut out, |o| welcome.write(o));
        for ((scope, key), e) in &self.state {
            let m = ServerMessage::State { scope: *scope, key: key.clone(), rev: e.rev, by: e.by, value: Some(e.value.clone()) };
            record(&mut out, |o| m.write(o));
        }
        if req.conn.tx.try_send(Outbound::Frame(Lane::Reliable, out.freeze())).is_err() {
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
            ClientMessage::Set { scope, key, value, expect } => {
                if let Err(c) = self.write(player, scope, key.clone(), value, expect) {
                    let rec = server_record(&ServerMessage::Refused { scope, key, code: c });
                    self.send_to(player, Lane::Reliable, rec);
                }
            }
            ClientMessage::Hello { .. } => {}
        }
        self.flush_kicks();
    }

    fn write(&mut self, by: u32, scope: u32, key: String, value: Option<Bytes>, expect: Option<u64>) -> Result<u64, u16> {
        if key.is_empty() {
            return Err(code::PROTOCOL);
        }
        if key.len() > MAX_KEY || value.as_ref().is_some_and(|v| v.len() > MAX_VALUE) {
            return Err(code::TOO_LARGE);
        }
        if by != 0 && scope != 0 && scope != by {
            return Err(code::FORBIDDEN);
        }
        let slot = (scope, key);
        let current = self.state.get(&slot);
        if by != 0 && current.is_some_and(|e| e.by == 0) {
            return Err(code::FORBIDDEN);
        }
        let matches = match expect {
            None => true,
            Some(0) => current.is_none(),
            Some(r) => current.is_some_and(|e| e.rev == r),
        };
        if !matches {
            return Err(code::CONFLICT);
        }
        let old = current.map_or(0, |e| cost(&slot.1, &e.value));
        let new = value.as_ref().map_or(0, |v| cost(&slot.1, v));
        if new > old && self.state_bytes - old + new > self.cfg.max_state {
            return Err(code::TOO_LARGE);
        }
        self.rev += 1;
        let rev = self.rev;
        self.state_bytes = self.state_bytes - old + new;
        let (scope, key) = slot;
        let rec = server_record(&ServerMessage::State { scope, key: key.clone(), rev, by, value: value.clone() });
        match value {
            Some(value) => {
                self.state.insert((scope, key), Entry { value, rev, by });
            }
            None => {
                self.state.remove(&(scope, key));
            }
        }
        self.fanout(Lane::Reliable, rec, 0, Target::All);
        Ok(rev)
    }

    fn elect(&mut self, skip: u32) {
        if self.closed || self.slot(self.host).is_some_and(|p| p.conn.is_some()) {
            return;
        }
        let next = self
            .players
            .iter()
            .enumerate()
            .filter_map(|(i, p)| p.as_ref().filter(|p| p.conn.is_some()).map(|p| (p.since, i as u32 + 1)))
            .min()
            .map_or(0, |(_, id)| id);
        if next == self.host {
            return;
        }
        self.host = next;
        tracing::info!(room = %self.id, host = next, "host");
        let rec = server_record(&ServerMessage::Host { player: next });
        self.fanout(Lane::Reliable, rec, skip, Target::Others);
    }

    pub fn admin(&mut self, op: Admin) {
        match op {
            Admin::Inspect(reply) => {
                let _ = reply.send(self.dump());
            }
            Admin::Kick(identity, reply) => {
                let id = self.by_identity.get(&identity).copied();
                if let Some(id) = id {
                    self.banned.insert(identity);
                    self.remove(id, reason::KICKED);
                    self.flush_kicks();
                    tracing::info!(room = %self.id, player = id, "kicked");
                }
                let _ = reply.send(id.is_some());
            }
            Admin::Set(w, reply) => {
                let scope = match &w.player {
                    None => Some(0),
                    Some(identity) => self.by_identity.get(identity).copied(),
                };
                let result = match scope {
                    None => Err(SetError::NoPlayer),
                    Some(scope) => self.write(0, scope, w.key, w.value, w.expect).map(|rev| Written { scope, rev }).map_err(SetError::Refused),
                };
                if let Ok(d) = &result {
                    tracing::info!(room = %self.id, scope = d.scope, rev = d.rev, "state set");
                }
                self.flush_kicks();
                let _ = reply.send(result);
            }
        }
    }

    fn dump(&self) -> Dump {
        let players = self
            .players
            .iter()
            .enumerate()
            .filter_map(|(i, p)| {
                p.as_ref().map(|p| PlayerInfo { id: i as u32 + 1, identity: p.identity.clone(), name: p.name.clone(), connected: p.conn.is_some() })
            })
            .collect();
        let state =
            self.state.iter().map(|((scope, key), e)| StateEntry { scope: *scope, key: key.clone(), value: e.value.clone(), rev: e.rev, by: e.by }).collect();
        Dump { host: self.host, rev: self.rev, players, state }
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
            self.elect(0);
        }
        self.flush_kicks();
    }

    fn remove(&mut self, player: u32, why: u8) {
        let Some(slot) = self.players.get_mut(player.wrapping_sub(1) as usize) else { return };
        let Some(p) = slot.take() else { return };
        self.by_identity.remove(&p.identity);
        let mut freed = 0;
        self.state.retain(|(scope, key), e| {
            let keep = *scope != player;
            if !keep {
                freed += cost(key, &e.value);
            }
            keep
        });
        self.state_bytes -= freed;
        if let Some(c) = p.conn {
            let _ = c.tx.try_send(Outbound::Frame(Lane::Reliable, server_record(&ServerMessage::Bye { reason: why })));
            let _ = c.tx.try_send(Outbound::Close);
        }
        Metrics::dec(&self.metrics.players);
        tracing::info!(room = %self.id, player, why, "left");
        let rec = server_record(&ServerMessage::Left { player, reason: why });
        self.fanout(Lane::Reliable, rec, player, Target::Others);
        let n = self.present();
        self.live.players.store(n, Relaxed);
        if n == 0 {
            self.empty_since = Some(Instant::now());
        }
        self.elect(0);
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
        self.closed = true;
        for id in 1..=self.players.len() as u32 {
            self.remove(id, reason::ROOM_CLOSED);
        }
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
    use crate::Write;
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
        assert_eq!(drain(&mut brx), vec![ServerMessage::Host { player: 2 }]);
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
        assert_eq!(drain(&mut brx), vec![ServerMessage::Left { player: 1, reason: reason::TIMEOUT }, ServerMessage::Host { player: 2 }]);
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

    fn room_of(max_players: u16, max_state: usize) -> Room {
        Room::new(Arc::from("r1"), RoomConfig { game: Arc::from("g"), max_players, max_state, ..Default::default() }, Arc::default(), Arc::default())
    }

    fn set(r: &mut Room, player: u32, scope: u32, key: &str, value: Option<&'static [u8]>, expect: Option<u64>) {
        r.message(player, u64::from(player), Lane::Reliable, ClientMessage::Set { scope, key: key.into(), value: value.map(Bytes::from_static), expect });
    }

    fn state(scope: u32, key: &str, rev: u64, by: u32, value: Option<&'static [u8]>) -> ServerMessage {
        ServerMessage::State { scope, key: key.into(), rev, by, value: value.map(Bytes::from_static) }
    }

    fn refused(scope: u32, key: &str, code: u16) -> ServerMessage {
        ServerMessage::Refused { scope, key: key.into(), code }
    }

    fn admin_set(r: &mut Room, player: Option<&str>, key: &str, value: Option<&'static [u8]>, expect: Option<u64>) -> Result<Written, SetError> {
        let (reply, mut rx) = oneshot::channel();
        r.admin(Admin::Set(Write { player: player.map(Arc::from), key: key.into(), value: value.map(Bytes::from_static), expect }, reply));
        rx.try_recv().unwrap()
    }

    fn dump(r: &mut Room) -> Dump {
        let (reply, mut rx) = oneshot::channel();
        r.admin(Admin::Inspect(reply));
        rx.try_recv().unwrap()
    }

    #[test]
    fn state_is_shared_checked_and_sent_to_joiners() {
        let mut r = room_of(3, 65_536);
        let (a, mut arx) = conn(1);
        join(&mut r, &a, "a").unwrap();
        assert!(matches!(drain(&mut arx)[0], ServerMessage::Welcome { host: 1, entries: 0, .. }));
        set(&mut r, 1, 0, "phase", Some(b"lobby"), None);
        set(&mut r, 1, 1, "ready", Some(b"1"), Some(0));
        assert_eq!(drain(&mut arx), vec![state(0, "phase", 1, 1, Some(b"lobby")), state(1, "ready", 2, 1, Some(b"1"))]);
        let (b, mut brx) = conn(2);
        join(&mut r, &b, "b").unwrap();
        let bm = drain(&mut brx);
        assert!(matches!(bm[0], ServerMessage::Welcome { player: 2, host: 1, entries: 2, .. }));
        assert_eq!(bm[1..], [state(0, "phase", 1, 1, Some(b"lobby")), state(1, "ready", 2, 1, Some(b"1"))]);
        drain(&mut arx);
        set(&mut r, 2, 1, "ready", Some(b"0"), None);
        set(&mut r, 2, 0, "phase", Some(b"play"), Some(0));
        set(&mut r, 2, 0, "phase", Some(b"play"), Some(2));
        set(&mut r, 2, 0, "", Some(b"x"), None);
        set(&mut r, 2, 0, &"k".repeat(MAX_KEY + 1), Some(b"x"), None);
        r.message(2, 2, Lane::Reliable, ClientMessage::Set { scope: 0, key: "big".into(), value: Some(Bytes::from(vec![0; MAX_VALUE + 1])), expect: None });
        set(&mut r, 2, 0, "phase", Some(b"play"), Some(1));
        let long = "k".repeat(MAX_KEY + 1);
        assert_eq!(
            drain(&mut brx),
            vec![
                refused(1, "ready", code::FORBIDDEN),
                refused(0, "phase", code::CONFLICT),
                refused(0, "phase", code::CONFLICT),
                refused(0, "", code::PROTOCOL),
                refused(0, &long, code::TOO_LARGE),
                refused(0, "big", code::TOO_LARGE),
                state(0, "phase", 3, 2, Some(b"play")),
            ]
        );
        assert_eq!(drain(&mut arx), vec![state(0, "phase", 3, 2, Some(b"play"))]);
        set(&mut r, 2, 0, "gone", None, None);
        assert_eq!(drain(&mut arx), vec![state(0, "gone", 4, 2, None)]);
        r.message(1, 1, Lane::Reliable, ClientMessage::Leave);
        assert_eq!(
            drain(&mut brx),
            vec![state(0, "gone", 4, 2, None), ServerMessage::Left { player: 1, reason: reason::LEFT }, ServerMessage::Host { player: 2 }]
        );
        let d = dump(&mut r);
        assert_eq!(d.rev, 4);
        assert_eq!(d.state, vec![StateEntry { scope: 0, key: "phase".into(), value: Bytes::from_static(b"play"), rev: 3, by: 2 }]);
        assert_eq!(r.state_bytes, cost("phase", b"play"));
    }

    #[test]
    fn state_budget_counts_keys_values_and_entries() {
        let mut r = room_of(2, 100);
        let (a, mut arx) = conn(1);
        join(&mut r, &a, "a").unwrap();
        drain(&mut arx);
        r.message(1, 1, Lane::Reliable, ClientMessage::Set { scope: 0, key: "a".into(), value: Some(Bytes::from(vec![0; 60])), expect: None });
        set(&mut r, 1, 0, "b", Some(b"1"), None);
        set(&mut r, 1, 0, "a", Some(b"small"), None);
        set(&mut r, 1, 0, "b", Some(b"1"), None);
        let m = drain(&mut arx);
        assert!(matches!(m[0], ServerMessage::State { rev: 1, .. }));
        assert_eq!(m[1..], [refused(0, "b", code::TOO_LARGE), state(0, "a", 2, 1, Some(b"small")), state(0, "b", 3, 1, Some(b"1"))]);
        assert_eq!(r.state_bytes, cost("a", b"small") + cost("b", b"1"));
    }

    #[test]
    fn backend_entries_are_read_only_for_players() {
        let mut r = room();
        let (a, mut arx) = conn(1);
        join(&mut r, &a, "alice").unwrap();
        drain(&mut arx);
        assert_eq!(admin_set(&mut r, None, "seed", Some(b"42"), Some(0)), Ok(Written { scope: 0, rev: 1 }));
        assert_eq!(admin_set(&mut r, None, "seed", Some(b"43"), Some(0)), Err(SetError::Refused(code::CONFLICT)));
        assert_eq!(admin_set(&mut r, Some("alice"), "team", Some(b"red"), None), Ok(Written { scope: 1, rev: 2 }));
        assert_eq!(admin_set(&mut r, Some("nobody"), "team", Some(b"red"), None), Err(SetError::NoPlayer));
        set(&mut r, 1, 0, "seed", Some(b"1"), None);
        set(&mut r, 1, 1, "team", Some(b"blue"), None);
        assert_eq!(
            drain(&mut arx),
            vec![
                state(0, "seed", 1, 0, Some(b"42")),
                state(1, "team", 2, 0, Some(b"red")),
                refused(0, "seed", code::FORBIDDEN),
                refused(1, "team", code::FORBIDDEN)
            ]
        );
        assert_eq!(admin_set(&mut r, None, "seed", None, Some(1)), Ok(Written { scope: 0, rev: 3 }));
        set(&mut r, 1, 0, "seed", Some(b"1"), Some(0));
        assert_eq!(drain(&mut arx), vec![state(0, "seed", 3, 0, None), state(0, "seed", 4, 1, Some(b"1"))]);
    }

    #[test]
    fn host_moves_to_the_longest_present_connected_player() {
        let mut r = room_of(3, 65_536);
        let (a, mut arx) = conn(1);
        let (b, mut brx) = conn(2);
        let (c, mut crx) = conn(3);
        join(&mut r, &a, "a").unwrap();
        join(&mut r, &b, "b").unwrap();
        join(&mut r, &c, "c").unwrap();
        assert!(matches!(drain(&mut crx)[0], ServerMessage::Welcome { host: 1, .. }));
        drain(&mut arx);
        drain(&mut brx);
        r.disconnect(1, 1);
        assert_eq!(drain(&mut brx), vec![ServerMessage::Host { player: 2 }]);
        assert_eq!(drain(&mut crx), vec![ServerMessage::Host { player: 2 }]);
        let (a2, mut a2rx) = conn(4);
        join(&mut r, &a2, "a").unwrap();
        assert!(matches!(drain(&mut a2rx)[0], ServerMessage::Welcome { player: 1, host: 2, reconnected: true, .. }));
        r.message(2, 2, Lane::Reliable, ClientMessage::Leave);
        assert_eq!(drain(&mut a2rx), vec![ServerMessage::Left { player: 2, reason: reason::LEFT }, ServerMessage::Host { player: 1 }]);
        assert_eq!(drain(&mut crx), vec![ServerMessage::Left { player: 2, reason: reason::LEFT }, ServerMessage::Host { player: 1 }]);
        r.disconnect(1, 4);
        assert_eq!(drain(&mut crx), vec![ServerMessage::Host { player: 3 }]);
        r.disconnect(3, 3);
        assert_eq!(r.host, 0);
        let (c2, mut c2rx) = conn(5);
        join(&mut r, &c2, "c").unwrap();
        assert!(matches!(drain(&mut c2rx)[0], ServerMessage::Welcome { player: 3, host: 3, .. }));
        assert_eq!(dump(&mut r).host, 3);
    }

    #[test]
    fn lock_refuses_newcomers_and_kick_keeps_the_player_out() {
        let mut r = room_of(3, 65_536);
        let (a, mut arx) = conn(1);
        let (b, mut brx) = conn(2);
        join(&mut r, &a, "a").unwrap();
        join(&mut r, &b, "b").unwrap();
        drain(&mut arx);
        drain(&mut brx);
        r.live.locked.store(true, Relaxed);
        let (c, _crx) = conn(3);
        assert_eq!(join(&mut r, &c, "c"), Err(code::LOCKED));
        let (b2, mut b2rx) = conn(4);
        assert_eq!(join(&mut r, &b2, "b"), Ok(Joined { player: 2, reconnected: true }));
        drain(&mut b2rx);
        drain(&mut brx);
        let (reply, mut rx) = oneshot::channel();
        r.admin(Admin::Kick(Arc::from("a"), reply));
        assert_eq!(rx.try_recv(), Ok(true));
        assert_eq!(drain(&mut arx), vec![ServerMessage::Bye { reason: reason::KICKED }]);
        assert_eq!(drain(&mut b2rx), vec![ServerMessage::Left { player: 1, reason: reason::KICKED }, ServerMessage::Host { player: 2 }]);
        r.live.locked.store(false, Relaxed);
        let (a2, _a2rx) = conn(5);
        assert_eq!(join(&mut r, &a2, "a"), Err(code::FORBIDDEN));
        let (reply, mut rx) = oneshot::channel();
        r.admin(Admin::Kick(Arc::from("nobody"), reply));
        assert_eq!(rx.try_recv(), Ok(false));
        let d = dump(&mut r);
        assert_eq!(d.players, vec![PlayerInfo { id: 2, identity: Arc::from("b"), name: "b".into(), connected: true }]);
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
