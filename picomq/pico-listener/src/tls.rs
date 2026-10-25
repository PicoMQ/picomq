use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use rustls::ServerConfig;
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tracing::{debug, info, warn};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const RELOAD_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    pub certificate: PathBuf,
    pub key: PathBuf,
    pub handshake_timeout: Duration,
    pub reload_interval: Duration,
}

impl TlsConfig {
    pub fn new(certificate: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        Self {
            certificate: certificate.into(),
            key: key.into(),
            handshake_timeout: HANDSHAKE_TIMEOUT,
            reload_interval: RELOAD_INTERVAL,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path} holds no PEM {kind}")]
    Pem { path: PathBuf, kind: &'static str },
    #[error("the certificate and key are not usable together: {0}")]
    Key(rustls::Error),
}

#[derive(Clone)]
pub struct Tls {
    source: Arc<Source>,
    acceptor: TlsAcceptor,
}

impl Tls {
    pub fn load(config: TlsConfig) -> Result<Self, TlsError> {
        let provider = Arc::new(aws_lc_rs::default_provider());
        let files = Files::read(&config)?;
        let current = Arc::new(Current(ArcSwap::from_pointee(
            files.certified(&config, &provider)?,
        )));

        let source = Arc::new(Source {
            seen: Mutex::new(files),
            config,
            provider,
            current,
        });
        tokio::spawn(watch(
            Arc::downgrade(&source),
            source.config.reload_interval,
        ));

        Ok(Self {
            acceptor: source.acceptor(Vec::new()),
            source,
        })
    }

    pub fn alpn(&self, protocols: &[&[u8]]) -> Self {
        Self {
            acceptor: self
                .source
                .acceptor(protocols.iter().map(|protocol| protocol.to_vec()).collect()),
            source: self.source.clone(),
        }
    }

    pub fn reload(&self) -> Result<bool, TlsError> {
        self.source.reload()
    }

    pub(crate) async fn handshake(
        &self,
        socket: TcpStream,
        peer: SocketAddr,
    ) -> Option<TlsStream<TcpStream>> {
        let accept = self.acceptor.accept(socket);
        match tokio::time::timeout(self.source.config.handshake_timeout, accept).await {
            Ok(Ok(stream)) => Some(stream),
            Ok(Err(error)) => {
                debug!(%peer, %error, "tls handshake failed");
                None
            }
            Err(_) => {
                debug!(%peer, "tls handshake timed out");
                None
            }
        }
    }
}

struct Source {
    config: TlsConfig,
    provider: Arc<CryptoProvider>,
    current: Arc<Current>,
    seen: Mutex<Files>,
}

impl Source {
    fn acceptor(&self, alpn: Vec<Vec<u8>>) -> TlsAcceptor {
        let mut server = ServerConfig::builder_with_provider(self.provider.clone())
            .with_safe_default_protocol_versions()
            .expect("aws-lc-rs supports the default protocol versions")
            .with_no_client_auth()
            .with_cert_resolver(self.current.clone());
        server.alpn_protocols = alpn;

        TlsAcceptor::from(Arc::new(server))
    }

    fn reload(&self) -> Result<bool, TlsError> {
        let files = Files::read(&self.config)?;
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        if *seen == files {
            return Ok(false);
        }

        *seen = files.clone();
        let certified = files.certified(&self.config, &self.provider)?;
        self.current.0.store(Arc::new(certified));
        Ok(true)
    }
}

async fn watch(source: Weak<Source>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let Some(source) = source.upgrade() else {
            return;
        };

        match source.reload() {
            Ok(true) => {
                info!(certificate = %source.config.certificate.display(), "tls certificate reloaded")
            }
            Ok(false) => {}
            Err(error) => warn!(%error, "tls certificate not reloaded, keeping the current one"),
        }
    }
}

#[derive(Debug)]
struct Current(ArcSwap<CertifiedKey>);

impl ResolvesServerCert for Current {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.load_full())
    }
}

#[derive(Clone, PartialEq, Eq)]
struct Files {
    certificate: Vec<u8>,
    key: Vec<u8>,
}

impl Files {
    fn read(config: &TlsConfig) -> Result<Self, TlsError> {
        let read = |path: &PathBuf| {
            std::fs::read(path).map_err(|source| TlsError::Read {
                path: path.clone(),
                source,
            })
        };

        Ok(Self {
            certificate: read(&config.certificate)?,
            key: read(&config.key)?,
        })
    }

    fn certified(
        &self,
        config: &TlsConfig,
        provider: &CryptoProvider,
    ) -> Result<CertifiedKey, TlsError> {
        let chain = CertificateDer::pem_slice_iter(&self.certificate)
            .collect::<Result<Vec<_>, _>>()
            .ok()
            .filter(|chain| !chain.is_empty())
            .ok_or_else(|| TlsError::Pem {
                path: config.certificate.clone(),
                kind: "certificate",
            })?;
        let key = PrivateKeyDer::from_pem_slice(&self.key).map_err(|_| TlsError::Pem {
            path: config.key.clone(),
            kind: "private key",
        })?;

        CertifiedKey::from_der(chain, key, provider).map_err(TlsError::Key)
    }
}
