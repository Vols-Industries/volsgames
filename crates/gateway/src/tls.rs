use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use base64::Engine;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use sha2::{Digest, Sha256};
use volsgames_proto::{ALPN_H3, ALPN_QUIC};

#[derive(Clone)]
pub struct Tls {
    pub chain: Vec<CertificateDer<'static>>,
    pub key: Arc<PrivateKeyDer<'static>>,
}

impl Tls {
    pub fn hash(&self) -> [u8; 32] {
        cert_hash(&self.chain[0])
    }

    pub fn hash_base64(&self) -> String {
        base64::engine::general_purpose::STANDARD.encode(self.hash())
    }

    pub fn quic_server_config(&self, idle: Duration) -> anyhow::Result<quinn::ServerConfig> {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(self.chain.clone(), self.key.clone_key())?;
        cfg.alpn_protocols = vec![ALPN_H3.to_vec(), ALPN_QUIC.to_vec()];
        let qcfg: quinn::crypto::rustls::QuicServerConfig = cfg.try_into()?;
        let mut server = quinn::ServerConfig::with_crypto(Arc::new(qcfg));
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(idle.try_into()?));
        transport.keep_alive_interval(Some(idle / 3));
        transport.max_concurrent_uni_streams(0u32.into());
        transport.max_concurrent_bidi_streams(4u32.into());
        transport.datagram_receive_buffer_size(Some(256 * 1024));
        transport.datagram_send_buffer_size(256 * 1024);
        server.transport_config(Arc::new(transport));
        Ok(server)
    }
}

pub fn cert_hash(cert: &CertificateDer<'_>) -> [u8; 32] {
    Sha256::digest(cert.as_ref()).into()
}

pub fn load_pem(cert: &Path, key: &Path) -> anyhow::Result<Tls> {
    let chain =
        rustls_pemfile::certs(&mut std::io::BufReader::new(fs::File::open(cert).context("open cert")?)).collect::<Result<Vec<_>, _>>().context("parse cert")?;
    anyhow::ensure!(!chain.is_empty(), "no certificate in {}", cert.display());
    let key =
        rustls_pemfile::private_key(&mut std::io::BufReader::new(fs::File::open(key).context("open key")?)).context("parse key")?.context("no private key")?;
    Ok(Tls { chain, key: Arc::new(key) })
}

pub struct DevCert;

impl DevCert {
    pub fn load_or_create(dir: &Path, names: &[String]) -> anyhow::Result<Tls> {
        fs::create_dir_all(dir)?;
        let cert = dir.join("dev-cert.pem");
        let key = dir.join("dev-key.pem");
        let stamp = dir.join("dev-cert.names");
        let want = names.join("\n");
        let fresh =
            fs::metadata(&cert).and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok()).is_some_and(|age| age < Duration::from_secs(11 * 86_400));
        let same = fs::read_to_string(&stamp).map(|s| s == want).unwrap_or(false);
        if fresh && same && key.exists() {
            return load_pem(&cert, &key);
        }
        let kp = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)?;
        let mut params = rcgen::CertificateParams::new(names.to_vec())?;
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now - time::Duration::hours(1);
        params.not_after = now + time::Duration::days(13);
        params.distinguished_name.push(rcgen::DnType::CommonName, "volsgames dev");
        let c = params.self_signed(&kp)?;
        fs::write(&cert, c.pem())?;
        fs::write(&key, kp.serialize_pem())?;
        fs::write(&stamp, want)?;
        load_pem(&cert, &key)
    }
}
