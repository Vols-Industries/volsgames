use crate::codec::{DecodeError, Reader, Writer};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use bytes::{Bytes, BytesMut};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

const TOKEN_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    pub exp: u64,
    pub tenant: String,
    pub game: String,
    pub room: String,
    pub player: String,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    #[error("malformed")]
    Malformed,
    #[error("bad signature")]
    Signature,
    #[error("expired")]
    Expired,
}

impl From<DecodeError> for TokenError {
    fn from(_: DecodeError) -> Self {
        TokenError::Malformed
    }
}

impl Claims {
    fn body(&self) -> Bytes {
        let mut out = BytesMut::with_capacity(128);
        let mut w = Writer(&mut out);
        w.u8(TOKEN_VERSION);
        w.u64(self.exp);
        w.str(&self.tenant);
        w.str(&self.game);
        w.str(&self.room);
        w.str(&self.player);
        w.str(&self.name);
        out.freeze()
    }

    pub fn sign(&self, key: &SigningKey) -> String {
        let body = self.body();
        let sig = key.sign(&body);
        format!("{}.{}", B64.encode(&body), B64.encode(sig.to_bytes()))
    }

    pub fn verify(token: &str, key: &VerifyingKey, now: u64) -> Result<Claims, TokenError> {
        let (b, s) = token.split_once('.').ok_or(TokenError::Malformed)?;
        let body = B64.decode(b).map_err(|_| TokenError::Malformed)?;
        let sig = B64.decode(s).map_err(|_| TokenError::Malformed)?;
        let sig = Signature::from_slice(&sig).map_err(|_| TokenError::Malformed)?;
        key.verify(&body, &sig).map_err(|_| TokenError::Signature)?;
        let mut r = Reader::new(Bytes::from(body));
        if r.u8()? != TOKEN_VERSION {
            return Err(TokenError::Malformed);
        }
        let c = Claims { exp: r.u64()?, tenant: r.str()?, game: r.str()?, room: r.str()?, player: r.str()?, name: r.str()? };
        r.finish()?;
        if c.exp <= now {
            return Err(TokenError::Expired);
        }
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[7u8; 32])
    }

    fn claims() -> Claims {
        Claims { exp: 1_000, tenant: "t".into(), game: "g".into(), room: "r".into(), player: "p".into(), name: "n".into() }
    }

    #[test]
    fn roundtrip() {
        let t = claims().sign(&key());
        assert_eq!(Claims::verify(&t, &key().verifying_key(), 999).unwrap(), claims());
    }

    #[test]
    fn expired() {
        let t = claims().sign(&key());
        assert_eq!(Claims::verify(&t, &key().verifying_key(), 1_000), Err(TokenError::Expired));
    }

    #[test]
    fn tampered() {
        let t = claims().sign(&key());
        let other = SigningKey::from_bytes(&[8u8; 32]).verifying_key();
        assert_eq!(Claims::verify(&t, &other, 1), Err(TokenError::Signature));
        let mut bad = t.clone();
        bad.replace_range(2..3, if &t[2..3] == "A" { "B" } else { "A" });
        assert!(Claims::verify(&bad, &key().verifying_key(), 1).is_err());
        assert_eq!(Claims::verify("nodot", &key().verifying_key(), 1), Err(TokenError::Malformed));
    }
}
