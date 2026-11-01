use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("short")]
    Short,
    #[error("malformed")]
    Malformed,
    #[error("unknown kind {0}")]
    UnknownKind(u8),
    #[error("too long")]
    TooLong,
}

pub struct Writer<'a>(pub &'a mut BytesMut);

impl Writer<'_> {
    pub fn u8(&mut self, v: u8) {
        self.0.put_u8(v);
    }
    pub fn u16(&mut self, v: u16) {
        self.0.put_u16_le(v);
    }
    pub fn u32(&mut self, v: u32) {
        self.0.put_u32_le(v);
    }
    pub fn u64(&mut self, v: u64) {
        self.0.put_u64_le(v);
    }
    pub fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.0.put_u8((v as u8) | 0x80);
            v >>= 7;
        }
        self.0.put_u8(v as u8);
    }
    pub fn bytes(&mut self, v: &[u8]) {
        self.varint(v.len() as u64);
        self.0.put_slice(v);
    }
    pub fn str(&mut self, v: &str) {
        self.bytes(v.as_bytes());
    }
}

pub struct Reader {
    buf: Bytes,
}

impl Reader {
    pub fn new(buf: Bytes) -> Self {
        Self { buf }
    }
    pub fn remaining(&self) -> usize {
        self.buf.len()
    }
    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        if self.buf.is_empty() {
            return Err(DecodeError::Short);
        }
        Ok(self.buf.get_u8())
    }
    pub fn u16(&mut self) -> Result<u16, DecodeError> {
        if self.buf.len() < 2 {
            return Err(DecodeError::Short);
        }
        Ok(self.buf.get_u16_le())
    }
    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        if self.buf.len() < 4 {
            return Err(DecodeError::Short);
        }
        Ok(self.buf.get_u32_le())
    }
    pub fn u64(&mut self) -> Result<u64, DecodeError> {
        if self.buf.len() < 8 {
            return Err(DecodeError::Short);
        }
        Ok(self.buf.get_u64_le())
    }
    pub fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.u8()?;
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(DecodeError::Malformed)
    }
    pub fn bytes(&mut self) -> Result<Bytes, DecodeError> {
        let n = self.varint()? as usize;
        if n > self.buf.len() {
            return Err(DecodeError::Short);
        }
        Ok(self.buf.split_to(n))
    }
    pub fn str(&mut self) -> Result<String, DecodeError> {
        let b = self.bytes()?;
        String::from_utf8(b.to_vec()).map_err(|_| DecodeError::Malformed)
    }
    pub fn finish(self) -> Result<(), DecodeError> {
        if self.buf.is_empty() { Ok(()) } else { Err(DecodeError::Malformed) }
    }
}
