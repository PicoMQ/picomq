//! Binding one node's protocol frontend (plus the admin surface) to sockets.
//!
//! One implementation takes the protocol as a parameter. Argument parsing
//! and process wiring belong to the binary. This module owns start, the
//! advertised base URL, and the shutdown ordering (drain, stop accepting,
//! close the node).
//!
//! Each listener is a task and [`RunningServer`] owns their
//! graceful shutdown, so an embedded server (tests, `pico serve`) starts and
//! stops in-process with no globals.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use pico_auth::Authorizer;
use pico_server::PicoNode;
use socket2::{Domain, Protocol as SockProtocol, Socket, Type};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::admin::{self, AdminState};
use crate::{DsFrontend, PicoFrontend, RoutingMode};

/// Which stream protocol this process speaks.
///
/// `DurableStreamsServer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    #[default]
    Pico,
    Ds,
    Kafka,
}

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub protocol: Protocol,
    /// Where the protocol listener binds. Port 0 binds an ephemeral port. Read
    /// the effective address back from [`RunningServer::local_addr`].
    pub addr: SocketAddr,
    /// Where `/health` and `/ready` bind. `None` disables the admin listener
    pub admin_addr: Option<SocketAddr>,
    pub routing_mode: RoutingMode,
    pub long_poll_timeout: Duration,
    pub sse_max_duration: Duration,
    pub max_chunk_size: usize,
    /// Cap on a single request body. Oversized bodies get `413`.
    pub max_request_size: usize,
    /// How long to keep failing readiness before closing listeners, so a
    /// load balancer can drain traffic first.
    pub shutdown_drain: Duration,
    /// The listener accept queue size.
    pub backlog: i32,
    /// Maintenance-lease holdership for the admin API, when the host runs a
    /// lease keeper. `None` reports `leaseHolder: null`.
    pub leadership: Option<watch::Receiver<bool>>,
    /// Bearer-token enforcement on the protocol listener. `None` disables it.
    pub authorizer: Option<Arc<Authorizer>>,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            protocol: Protocol::Pico,
            addr: SocketAddr::from(([127, 0, 0, 1], 4437)),
            admin_addr: Some(SocketAddr::from(([127, 0, 0, 1], 9090))),
            routing_mode: RoutingMode::Redirect,
            long_poll_timeout: Duration::from_secs(25),
            sse_max_duration: Duration::from_secs(55),
            max_chunk_size: 64 * 1024,
            max_request_size: 32 * 1024 * 1024,
            shutdown_drain: Duration::ZERO,
            backlog: 1024,
            leadership: None,
            authorizer: None,
        }
    }
}

/// A bound, serving frontend. Dropping it stops the listeners. Call
/// [`RunningServer::shutdown`] for a graceful stop.
pub struct RunningServer {
    node: Arc<PicoNode>,
    admin: AdminState,
    local_addr: SocketAddr,
    admin_addr: Option<SocketAddr>,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    shutdown_drain: Duration,
}

/// Bind `options.protocol` on `options.addr` and, when configured, the admin
/// routes on `options.admin_addr`.
///
/// The node is constructed by the caller, which lets one node be served by
/// different sockets in tests and keeps this crate free of metadata-backend
/// choices.
pub async fn serve(node: Arc<PicoNode>, options: ServeOptions) -> std::io::Result<RunningServer> {
    let protocol_router = match options.protocol {
        Protocol::Pico => Arc::new(
            PicoFrontend::with_tuning(
                node.service(),
                node.ownership(),
                options.routing_mode,
                options.long_poll_timeout,
                options.sse_max_duration,
                options.max_chunk_size,
                options.max_request_size,
            )
            .with_authorizer(options.authorizer.clone()),
        )
        .router(),
        Protocol::Ds => Arc::new(
            DsFrontend::with_tuning(
                node.service(),
                node.ownership(),
                options.routing_mode,
                options.long_poll_timeout,
                options.sse_max_duration,
                options.max_chunk_size,
                options.max_request_size,
            )
            .with_authorizer(options.authorizer.clone()),
        )
        .router(),
        Protocol::Kafka => Router::new(),
    };

    let (stop, _) = watch::channel(false);
    let admin = AdminState::new(node.clone())
        .with_leadership(options.leadership.clone())
        .with_authorizer(options.authorizer.clone());
    let mut tasks = Vec::with_capacity(2);

    let (local_addr, task) =
        spawn_listener(options.addr, options.backlog, protocol_router, &stop).await?;
    tasks.push(task);

    let mut admin_addr = None;
    if let Some(addr) = options.admin_addr {
        let (bound, task) =
            spawn_listener(addr, options.backlog, admin::router(admin.clone()), &stop).await?;
        admin_addr = Some(bound);
        tasks.push(task);
    }

    tracing::info!(
        node_id = node.config().node_id,
        protocol = ?options.protocol,
        %local_addr,
        admin = ?admin_addr,
        advertised = node.advertised_address(),
        "picomq server started"
    );
    Ok(RunningServer {
        node,
        admin,
        local_addr,
        admin_addr,
        stop,
        tasks,
        shutdown_drain: options.shutdown_drain,
    })
}

/// Bind a listener with an explicit accept queue. `TcpListener::bind` gives
/// no way to set the backlog, so the socket is built by hand for the one
/// call that matters (`listen`).
fn bind(addr: SocketAddr, backlog: i32) -> std::io::Result<TcpListener> {
    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::STREAM, Some(SockProtocol::TCP))?;
    // Restarting a node should not have to wait out TIME_WAIT.
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(backlog)?;
    TcpListener::from_std(socket.into())
}

async fn spawn_listener(
    addr: SocketAddr,
    backlog: i32,
    router: Router,
    stop: &watch::Sender<bool>,
) -> std::io::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = bind(addr, backlog)?;
    let bound = listener.local_addr()?;
    let mut stop = stop.subscribe();
    let task = tokio::spawn(async move {
        let result = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                // Only the shutdown path sends, and the sender outlives these
                // tasks, so a receive error cannot mean "stop" was missed.
                let _ = stop.wait_for(|stop| *stop).await;
            })
            .await;
        if let Err(e) = result {
            tracing::error!(%bound, error = %e, "listener stopped");
        }
    });
    Ok((bound, task))
}

impl RunningServer {
    /// The bound protocol address (resolves port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The bound admin address, when the admin listener is enabled.
    pub fn admin_addr(&self) -> Option<SocketAddr> {
        self.admin_addr
    }

    pub fn base_url(&self) -> &str {
        self.node.advertised_address()
    }

    pub fn node(&self) -> Arc<PicoNode> {
        self.node.clone()
    }

    /// (fail readiness, wait out the drain window), stop accepting and let
    /// in-flight requests finish, then `node.close()`.
    pub async fn shutdown(self) {
        self.admin.stop_serving();
        if !self.shutdown_drain.is_zero() {
            tracing::info!(drain = ?self.shutdown_drain, "draining before shutdown");
            tokio::time::sleep(self.shutdown_drain).await;
        }
        let _ = self.stop.send(true);
        for task in self.tasks {
            let _ = task.await;
        }
        self.node.close().await;
    }
}
