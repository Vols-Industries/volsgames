use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub messages_per_sec: u32,
    pub bytes_per_sec: u32,
    pub burst_seconds: f64,
    pub max_payload: usize,
    pub strikes: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self { messages_per_sec: 240, bytes_per_sec: 256 * 1024, burst_seconds: 1.0, max_payload: 16 * 1024, strikes: 2000 }
    }
}

pub struct Bucket {
    messages: f64,
    bytes: f64,
    last: Instant,
    limits: Limits,
    pub strikes: u32,
}

impl Bucket {
    pub fn new(limits: Limits) -> Self {
        Self {
            messages: f64::from(limits.messages_per_sec) * limits.burst_seconds,
            bytes: f64::from(limits.bytes_per_sec) * limits.burst_seconds,
            last: Instant::now(),
            limits,
            strikes: 0,
        }
    }

    pub fn allow(&mut self, now: Instant, size: usize) -> bool {
        let dt = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        let l = &self.limits;
        self.messages = (self.messages + dt * f64::from(l.messages_per_sec)).min(f64::from(l.messages_per_sec) * l.burst_seconds);
        self.bytes = (self.bytes + dt * f64::from(l.bytes_per_sec)).min(f64::from(l.bytes_per_sec) * l.burst_seconds);
        if self.messages >= 1.0 && self.bytes >= size as f64 {
            self.messages -= 1.0;
            self.bytes -= size as f64;
            true
        } else {
            self.strikes += 1;
            false
        }
    }

    pub fn exhausted(&self) -> bool {
        self.strikes >= self.limits.strikes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn refills_over_time() {
        let mut b = Bucket::new(Limits { messages_per_sec: 10, bytes_per_sec: 1000, burst_seconds: 1.0, max_payload: 100, strikes: 3 });
        let t0 = Instant::now();
        for _ in 0..10 {
            assert!(b.allow(t0, 10));
        }
        assert!(!b.allow(t0, 10));
        assert!(b.allow(t0 + Duration::from_millis(100), 10));
        assert!(!b.allow(t0 + Duration::from_millis(100), 10));
        assert!(!b.allow(t0 + Duration::from_millis(100), 10));
        assert!(b.exhausted());
    }
}
