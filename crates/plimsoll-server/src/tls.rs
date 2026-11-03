//! In-process TLS for the query API (ADR 0099).
//!
//! `PLIMSOLL_TLS_CERT` / `PLIMSOLL_TLS_KEY` (PEM files) turn it on for the
//! whole listener: SQL, maintenance, metrics and worker fragments. The
//! files are read at boot (a bad pair fails it) and again whenever either
//! changes on disk, so a rotated certificate (cert-manager, a renewed
//! secret) is served to new connections without a restart; a pair that
//! does not load keeps the previous one.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use crate::log::{self, Level};

/// A handshake that has not finished by then is dropped: a client that
/// never speaks costs a task, never the listener.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Where the certificate chain and its private key are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    pub cert: PathBuf,
    pub key: PathBuf,
}

/// The certificate chain and key of `files`, as rustls signs with them.
fn load(files: &TlsFiles) -> Result<Arc<CertifiedKey>, String> {
    let chain = CertificateDer::pem_file_iter(&files.cert)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|e| format!("{}: {e}", files.cert.display()))?;
    if chain.is_empty() {
        return Err(format!("{}: no certificate", files.cert.display()));
    }
    let key = PrivateKeyDer::from_pem_file(&files.key)
        .map_err(|e| format!("{}: {e}", files.key.display()))?;
    let key = rustls::crypto::ring::sign::any_supported_type(&key)
        .map_err(|e| format!("{}: {e}", files.key.display()))?;
    let certified = CertifiedKey::new(chain, key);
    certified
        .keys_match()
        .map_err(|e| format!("the certificate and key do not match: {e}"))?;
    Ok(Arc::new(certified))
}

fn modified(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// The certificate's and key's modification times.
type Stamps = (Option<SystemTime>, Option<SystemTime>);

/// Serves the files' pair, reloading it when either file changes.
#[derive(Debug)]
struct Reloading {
    files: TlsFiles,
    /// The files' modification times when last loaded, and the pair.
    current: RwLock<(Stamps, Arc<CertifiedKey>)>,
}

impl ResolvesServerCert for Reloading {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let stamps = (modified(&self.files.cert), modified(&self.files.key));
        {
            let current = self.current.read().unwrap_or_else(|e| e.into_inner());
            if current.0 == stamps {
                return Some(current.1.clone());
            }
        }
        let mut current = self.current.write().unwrap_or_else(|e| e.into_inner());
        if current.0 != stamps {
            match load(&self.files) {
                Ok(pair) => {
                    *current = (stamps, pair);
                    log::emit(
                        Level::Info,
                        "tls_reloaded",
                        serde_json::json!({ "cert": self.files.cert.display().to_string() }),
                    );
                }
                Err(e) => {
                    // Keep serving the previous pair; try again when the
                    // files change again (a half-written rotation).
                    current.0 = stamps;
                    log::emit(
                        Level::Warn,
                        "tls_reload_failed",
                        serde_json::json!({ "error": e }),
                    );
                }
            }
        }
        Some(current.1.clone())
    }
}

/// The acceptor for `files`; `Err` when they do not load (boot fails).
pub fn acceptor(files: &TlsFiles) -> Result<TlsAcceptor, String> {
    let stamps = (modified(&files.cert), modified(&files.key));
    let pair = load(files)?;
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| e.to_string())?
    .with_no_client_auth()
    .with_cert_resolver(Arc::new(Reloading {
        files: files.clone(),
        current: RwLock::new((stamps, pair)),
    }));
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// A listener whose connections are TLS: TCP accepts here, each handshake
/// in a task of its own, so a slow client delays nobody else.
pub struct TlsListener {
    local: SocketAddr,
    ready: tokio::sync::mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
}

impl TlsListener {
    pub fn new(tcp: TcpListener, acceptor: TlsAcceptor) -> std::io::Result<Self> {
        let local = tcp.local_addr()?;
        let (tx, ready) = tokio::sync::mpsc::channel(256);
        tokio::spawn(async move {
            loop {
                let (stream, peer) = match tcp.accept().await {
                    Ok(accepted) => accepted,
                    Err(e) => {
                        // As axum's own listener: transient (out of file
                        // descriptors, …), so back off and go on.
                        log::emit(
                            Level::Warn,
                            "accept_failed",
                            serde_json::json!({ "error": e.to_string() }),
                        );
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let (acceptor, tx) = (acceptor.clone(), tx.clone());
                tokio::spawn(async move {
                    let shake =
                        tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await;
                    // A client that is not speaking TLS, or not at all: its
                    // connection is closed, nobody else's.
                    if let Ok(Ok(tls)) = shake {
                        let _ = tx.send((tls, peer)).await;
                    }
                });
            }
        });
        Ok(Self { local, ready })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.ready.recv().await {
            Some(conn) => conn,
            // The accept loop never ends while the listener lives.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn missing_or_mismatched_files_fail() {
        let dir = std::env::temp_dir().join(format!("plimsoll-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let files = TlsFiles {
            cert: dir.join("nope.pem"),
            key: dir.join("nope.key"),
        };
        assert!(acceptor(&files).is_err());
        std::fs::write(&files.cert, "not a certificate").unwrap();
        std::fs::write(&files.key, "not a key").unwrap();
        assert!(acceptor(&files).is_err());
    }
}
