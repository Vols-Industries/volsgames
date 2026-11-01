use crate::codec::{DecodeError, Reader, Writer};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const VERSION: u8 = 0;
pub const ALPN_QUIC: &[u8] = b"volsgames-0";
pub const ALPN_H3: &[u8] = b"h3";
pub const MAX_RECORD: usize = u16::MAX as usize;
pub const DATAGRAM_BUDGET: usize = 1150;
pub const MAX_NAME: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lane {
    Reliable,
    Unreliable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    All,
    Others,
    Player(u32),
    Group(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    Hello { token: String },
    Data { target: Target, payload: Bytes },
    Ping { client_time: u64 },
    Subscribe { group: u32 },
    Unsubscribe { group: u32 },
    Leave,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage {
    Welcome { player: u32, room: String, tick_rate: u16, server_time: u64, reconnected: bool, players: Vec<(u32, String)> },
    Joined { player: u32, name: String },
    Left { player: u32, reason: u8 },
    Data { from: u32, payload: Bytes },
    Pong { client_time: u64, server_time: u64 },
    Error { code: u16, message: String },
    Bye { reason: u8 },
}

pub mod reason {
    pub const LEFT: u8 = 0;
    pub const TIMEOUT: u8 = 1;
    pub const KICKED: u8 = 2;
    pub const ROOM_CLOSED: u8 = 3;
    pub const REPLACED: u8 = 4;
    pub const SLOW: u8 = 5;
    pub const LIMIT: u8 = 6;
}

pub mod code {
    pub const BAD_TOKEN: u16 = 1;
    pub const EXPIRED: u16 = 2;
    pub const NO_ROOM: u16 = 3;
    pub const FULL: u16 = 4;
    pub const PROTOCOL: u16 = 5;
    pub const HELLO_TIMEOUT: u16 = 6;
    pub const WRONG_GAME: u16 = 7;
}

const C_HELLO: u8 = 1;
const C_DATA: u8 = 2;
const C_PING: u8 = 3;
const C_SUBSCRIBE: u8 = 4;
const C_UNSUBSCRIBE: u8 = 5;
const C_LEAVE: u8 = 6;
const S_WELCOME: u8 = 0x81;
const S_JOINED: u8 = 0x82;
const S_LEFT: u8 = 0x83;
const S_DATA: u8 = 0x84;
const S_PONG: u8 = 0x85;
const S_ERROR: u8 = 0x86;
const S_BYE: u8 = 0x87;

impl Target {
    fn write(self, w: &mut Writer) {
        match self {
            Target::All => w.u8(0),
            Target::Others => w.u8(1),
            Target::Player(p) => {
                w.u8(2);
                w.u32(p);
            }
            Target::Group(g) => {
                w.u8(3);
                w.u32(g);
            }
        }
    }
    fn read(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(match r.u8()? {
            0 => Target::All,
            1 => Target::Others,
            2 => Target::Player(r.u32()?),
            3 => Target::Group(r.u32()?),
            _ => return Err(DecodeError::Malformed),
        })
    }
}

impl ClientMessage {
    pub fn write(&self, out: &mut BytesMut) {
        let mut w = Writer(out);
        match self {
            ClientMessage::Hello { token } => {
                w.u8(C_HELLO);
                w.u8(VERSION);
                w.str(token);
            }
            ClientMessage::Data { target, payload } => {
                w.u8(C_DATA);
                target.write(&mut w);
                w.bytes(payload);
            }
            ClientMessage::Ping { client_time } => {
                w.u8(C_PING);
                w.u64(*client_time);
            }
            ClientMessage::Subscribe { group } => {
                w.u8(C_SUBSCRIBE);
                w.u32(*group);
            }
            ClientMessage::Unsubscribe { group } => {
                w.u8(C_UNSUBSCRIBE);
                w.u32(*group);
            }
            ClientMessage::Leave => w.u8(C_LEAVE),
        }
    }

    pub fn read(buf: Bytes) -> Result<Self, DecodeError> {
        let mut r = Reader::new(buf);
        let m = match r.u8()? {
            C_HELLO => {
                if r.u8()? != VERSION {
                    return Err(DecodeError::Malformed);
                }
                ClientMessage::Hello { token: r.str()? }
            }
            C_DATA => ClientMessage::Data { target: Target::read(&mut r)?, payload: r.bytes()? },
            C_PING => ClientMessage::Ping { client_time: r.u64()? },
            C_SUBSCRIBE => ClientMessage::Subscribe { group: r.u32()? },
            C_UNSUBSCRIBE => ClientMessage::Unsubscribe { group: r.u32()? },
            C_LEAVE => ClientMessage::Leave,
            k => return Err(DecodeError::UnknownKind(k)),
        };
        r.finish()?;
        Ok(m)
    }
}

impl ServerMessage {
    pub fn write(&self, out: &mut BytesMut) {
        let mut w = Writer(out);
        match self {
            ServerMessage::Welcome { player, room, tick_rate, server_time, reconnected, players } => {
                w.u8(S_WELCOME);
                w.u8(VERSION);
                w.u32(*player);
                w.str(room);
                w.u16(*tick_rate);
                w.u64(*server_time);
                w.u8(u8::from(*reconnected));
                w.varint(players.len() as u64);
                for (id, name) in players {
                    w.u32(*id);
                    w.str(name);
                }
            }
            ServerMessage::Joined { player, name } => {
                w.u8(S_JOINED);
                w.u32(*player);
                w.str(name);
            }
            ServerMessage::Left { player, reason } => {
                w.u8(S_LEFT);
                w.u32(*player);
                w.u8(*reason);
            }
            ServerMessage::Data { from, payload } => {
                w.u8(S_DATA);
                w.u32(*from);
                w.bytes(payload);
            }
            ServerMessage::Pong { client_time, server_time } => {
                w.u8(S_PONG);
                w.u64(*client_time);
                w.u64(*server_time);
            }
            ServerMessage::Error { code, message } => {
                w.u8(S_ERROR);
                w.u16(*code);
                w.str(message);
            }
            ServerMessage::Bye { reason } => {
                w.u8(S_BYE);
                w.u8(*reason);
            }
        }
    }

    pub fn read(buf: Bytes) -> Result<Self, DecodeError> {
        let mut r = Reader::new(buf);
        let m = match r.u8()? {
            S_WELCOME => {
                if r.u8()? != VERSION {
                    return Err(DecodeError::Malformed);
                }
                let player = r.u32()?;
                let room = r.str()?;
                let tick_rate = r.u16()?;
                let server_time = r.u64()?;
                let reconnected = r.u8()? != 0;
                let n = r.varint()? as usize;
                if n > 65_536 {
                    return Err(DecodeError::TooLong);
                }
                let mut players = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    players.push((r.u32()?, r.str()?));
                }
                ServerMessage::Welcome { player, room, tick_rate, server_time, reconnected, players }
            }
            S_JOINED => ServerMessage::Joined { player: r.u32()?, name: r.str()? },
            S_LEFT => ServerMessage::Left { player: r.u32()?, reason: r.u8()? },
            S_DATA => ServerMessage::Data { from: r.u32()?, payload: r.bytes()? },
            S_PONG => ServerMessage::Pong { client_time: r.u64()?, server_time: r.u64()? },
            S_ERROR => ServerMessage::Error { code: r.u16()?, message: r.str()? },
            S_BYE => ServerMessage::Bye { reason: r.u8()? },
            k => return Err(DecodeError::UnknownKind(k)),
        };
        r.finish()?;
        Ok(m)
    }
}

pub fn record<F: FnOnce(&mut BytesMut)>(out: &mut BytesMut, body: F) {
    let at = out.len();
    out.put_u16_le(0);
    body(out);
    let len = out.len() - at - 2;
    assert!(len <= MAX_RECORD);
    out[at..at + 2].copy_from_slice(&(len as u16).to_le_bytes());
}

pub fn client_record(m: &ClientMessage) -> Bytes {
    let mut out = BytesMut::with_capacity(64);
    record(&mut out, |o| m.write(o));
    out.freeze()
}

pub fn server_record(m: &ServerMessage) -> Bytes {
    let mut out = BytesMut::with_capacity(64);
    record(&mut out, |o| m.write(o));
    out.freeze()
}

pub struct Records {
    buf: Bytes,
}

impl Records {
    pub fn new(buf: Bytes) -> Self {
        Self { buf }
    }
}

impl Iterator for Records {
    type Item = Result<Bytes, DecodeError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() {
            return None;
        }
        if self.buf.len() < 2 {
            self.buf.clear();
            return Some(Err(DecodeError::Short));
        }
        let n = self.buf.get_u16_le() as usize;
        if n > self.buf.len() {
            self.buf.clear();
            return Some(Err(DecodeError::Short));
        }
        Some(Ok(self.buf.split_to(n)))
    }
}

#[derive(Default)]
pub struct StreamBuffer {
    buf: BytesMut,
}

impl StreamBuffer {
    pub fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }
    pub fn len(&self) -> usize {
        self.buf.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
    pub fn pop(&mut self) -> Option<Bytes> {
        if self.buf.len() < 2 {
            return None;
        }
        let n = u16::from_le_bytes([self.buf[0], self.buf[1]]) as usize;
        if self.buf.len() < 2 + n {
            return None;
        }
        self.buf.advance(2);
        Some(self.buf.split_to(n).freeze())
    }
}

pub struct Datagram {
    pub seq: u16,
    pub records: Records,
}

impl Datagram {
    pub fn parse(mut buf: Bytes) -> Result<Self, DecodeError> {
        if buf.len() < 2 {
            return Err(DecodeError::Short);
        }
        let seq = buf.get_u16_le();
        Ok(Self { seq, records: Records::new(buf) })
    }
    pub fn frame(seq: u16, records: &[u8]) -> Bytes {
        let mut out = BytesMut::with_capacity(2 + records.len());
        out.put_u16_le(seq);
        out.put_slice(records);
        out.freeze()
    }
}

pub fn seq_newer(a: u16, b: u16) -> bool {
    a != b && a.wrapping_sub(b) < 0x8000
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_c(m: ClientMessage) {
        let rec = client_record(&m);
        let mut recs = Records::new(rec);
        let body = recs.next().unwrap().unwrap();
        assert!(recs.next().is_none());
        assert_eq!(ClientMessage::read(body).unwrap(), m);
    }

    fn round_s(m: ServerMessage) {
        let rec = server_record(&m);
        let mut recs = Records::new(rec);
        let body = recs.next().unwrap().unwrap();
        assert_eq!(ServerMessage::read(body).unwrap(), m);
    }

    #[test]
    fn client_messages() {
        round_c(ClientMessage::Hello { token: "a.b".into() });
        round_c(ClientMessage::Data { target: Target::All, payload: Bytes::from_static(b"xyz") });
        round_c(ClientMessage::Data { target: Target::Player(7), payload: Bytes::new() });
        round_c(ClientMessage::Data { target: Target::Group(9), payload: Bytes::from(vec![1u8; 300]) });
        round_c(ClientMessage::Ping { client_time: u64::MAX });
        round_c(ClientMessage::Subscribe { group: 3 });
        round_c(ClientMessage::Unsubscribe { group: 3 });
        round_c(ClientMessage::Leave);
    }

    #[test]
    fn server_messages() {
        round_s(ServerMessage::Welcome { player: 1, room: "r".into(), tick_rate: 60, server_time: 5, reconnected: true, players: vec![(2, "bob".into())] });
        round_s(ServerMessage::Joined { player: 2, name: "x".into() });
        round_s(ServerMessage::Left { player: 2, reason: reason::TIMEOUT });
        round_s(ServerMessage::Data { from: 2, payload: Bytes::from_static(b"q") });
        round_s(ServerMessage::Pong { client_time: 1, server_time: 2 });
        round_s(ServerMessage::Error { code: code::FULL, message: "full".into() });
        round_s(ServerMessage::Bye { reason: reason::KICKED });
    }

    #[test]
    fn stream_buffer_reassembles() {
        let a = client_record(&ClientMessage::Leave);
        let b = client_record(&ClientMessage::Ping { client_time: 9 });
        let mut all = Vec::new();
        all.extend_from_slice(&a);
        all.extend_from_slice(&b);
        let mut sb = StreamBuffer::default();
        sb.push(&all[..3]);
        assert_eq!(sb.pop().map(|x| ClientMessage::read(x).unwrap()), Some(ClientMessage::Leave));
        assert!(sb.pop().is_none());
        sb.push(&all[3..]);
        assert_eq!(sb.pop().map(|x| ClientMessage::read(x).unwrap()), Some(ClientMessage::Ping { client_time: 9 }));
        assert!(sb.is_empty());
    }

    #[test]
    fn datagram_seq_and_records() {
        let mut body = BytesMut::new();
        body.extend_from_slice(&client_record(&ClientMessage::Leave));
        body.extend_from_slice(&client_record(&ClientMessage::Leave));
        let d = Datagram::parse(Datagram::frame(65535, &body)).unwrap();
        assert_eq!(d.seq, 65535);
        assert_eq!(d.records.count(), 2);
        assert!(seq_newer(0, 65535));
        assert!(!seq_newer(65535, 0));
        assert!(seq_newer(10, 9));
        assert!(!seq_newer(9, 9));
    }

    #[test]
    fn trailing_bytes_rejected() {
        let mut out = BytesMut::new();
        ClientMessage::Leave.write(&mut out);
        out.put_u8(0);
        assert_eq!(ClientMessage::read(out.freeze()), Err(DecodeError::Malformed));
    }
}
