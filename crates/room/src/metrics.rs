use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Default, Debug)]
pub struct Metrics {
    pub connections: AtomicU64,
    pub connections_total: AtomicU64,
    pub players: AtomicU64,
    pub rooms: AtomicU64,
    pub rooms_total: AtomicU64,
    pub messages_in: AtomicU64,
    pub messages_out: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub dropped_unreliable: AtomicU64,
    pub kicked_slow: AtomicU64,
    pub rate_limited: AtomicU64,
    pub rejected: AtomicU64,
    pub ticks: AtomicU64,
    pub tick_late: AtomicU64,
}

impl Metrics {
    pub fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Relaxed);
    }
    pub fn inc(c: &AtomicU64) {
        c.fetch_add(1, Relaxed);
    }
    pub fn dec(c: &AtomicU64) {
        c.fetch_sub(1, Relaxed);
    }
    pub fn get(c: &AtomicU64) -> u64 {
        c.load(Relaxed)
    }

    pub fn render(&self) -> String {
        let mut s = String::with_capacity(1024);
        let mut line = |name: &str, kind: &str, v: &AtomicU64| {
            s.push_str(&format!("# TYPE volsgames_{name} {kind}\nvolsgames_{name} {}\n", v.load(Relaxed)));
        };
        line("connections", "gauge", &self.connections);
        line("connections_total", "counter", &self.connections_total);
        line("players", "gauge", &self.players);
        line("rooms", "gauge", &self.rooms);
        line("rooms_total", "counter", &self.rooms_total);
        line("messages_in_total", "counter", &self.messages_in);
        line("messages_out_total", "counter", &self.messages_out);
        line("bytes_in_total", "counter", &self.bytes_in);
        line("bytes_out_total", "counter", &self.bytes_out);
        line("dropped_unreliable_total", "counter", &self.dropped_unreliable);
        line("kicked_slow_total", "counter", &self.kicked_slow);
        line("rate_limited_total", "counter", &self.rate_limited);
        line("rejected_total", "counter", &self.rejected);
        line("ticks_total", "counter", &self.ticks);
        line("tick_late_total", "counter", &self.tick_late);
        s
    }
}
