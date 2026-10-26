use std::io::ErrorKind;
use std::net::SocketAddr;
use std::time::Duration;

use picomq_metrics::Listener as Kind;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::stream::Stream;
use crate::tls::Tls;

pub struct Listener {
    local_addr: SocketAddr,
    incoming: mpsc::Receiver<(Stream, SocketAddr)>,
    task: JoinHandle<()>,
}

impl Listener {
    pub fn bind(
        kind: Kind,
        addr: SocketAddr,
        backlog: i32,
        tls: Option<Tls>,
    ) -> std::io::Result<Self> {
        let domain = if addr.is_ipv6() {
            Domain::IPV6
        } else {
            Domain::IPV4
        };
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_reuse_address(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(backlog)?;

        let tcp = TcpListener::from_std(socket.into())?;
        let local_addr = tcp.local_addr()?;
        let (accepted, incoming) = mpsc::channel(backlog.max(1) as usize);

        Ok(Self {
            local_addr,
            incoming,
            task: tokio::spawn(accept(kind, tcp, tls, accepted)),
        })
    }

    pub async fn accept(&mut self) -> Option<(Stream, SocketAddr)> {
        self.incoming.recv().await
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn accept(
    kind: Kind,
    tcp: TcpListener,
    tls: Option<Tls>,
    accepted: mpsc::Sender<(Stream, SocketAddr)>,
) {
    loop {
        let (socket, peer) = match tcp.accept().await {
            Ok(connection) => connection,
            Err(error) if is_connection_error(&error) => continue,
            Err(error) => {
                warn!(%error, "accept failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let Some(tls) = &tls else {
            let stream = Stream::plain(socket, picomq_metrics::connection(kind));
            if accepted.send((stream, peer)).await.is_err() {
                return;
            }
            continue;
        };

        let (tls, accepted) = (tls.clone(), accepted.clone());
        tokio::spawn(async move {
            if let Some(stream) = tls.handshake(socket, peer).await {
                let stream = Stream::tls(stream, picomq_metrics::connection(kind));
                let _ = accepted.send((stream, peer)).await;
            }
        });
    }
}

fn is_connection_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::ConnectionRefused | ErrorKind::ConnectionAborted | ErrorKind::ConnectionReset
    )
}
