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
use picomq_auth::{Audience, Authorizer};
use picomq_listener::{Listener, Stream};
use picomq_metrics::Metrics;
use picomq_server::PicoNode;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::admin::{self, AdminState};
use crate::forward::Forward;
use crate::observe::observe;
use crate::{DsFrontend, PicoFrontend, RoutingMode};
use crate::{common, groups};

/// Which HTTP stream protocol this process mounts on its data listener. The
/// Kafka listener is separate and runs alongside either one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpProtocol {
    #[default]
    Pico,
    Ds,
}

pub const ALPN: &[&[u8]] = &[b"h2", b"http/1.1"];

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub protocol: HttpProtocol,
    pub routing_mode: RoutingMode,
    pub long_poll_timeout: Duration,
    pub sse_max_duration: Duration,
    pub max_chunk_size: usize,
    /// Cap on a single request body. Oversized bodies get `413`.
    pub max_request_size: usize,
    /// How long to keep failing readiness before closing listeners, so a
    /// load balancer can drain traffic first.
    pub shutdown_drain: Duration,
    pub shutdown_timeout: Duration,
    pub forward: Option<Forward>,
    pub metrics: Option<Metrics>,
    /// Maintenance-lease holdership for the admin API, when the host runs a
    /// lease keeper. `None` reports `leaseHolder: null`.
    pub leadership: Option<watch::Receiver<bool>>,
    /// Bearer-token enforcement on the protocol listener. `None` disables it.
    pub authorizer: Option<Arc<Authorizer>>,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            protocol: HttpProtocol::Pico,
            routing_mode: RoutingMode::Redirect,
            long_poll_timeout: Duration::from_secs(25),
            sse_max_duration: Duration::from_secs(55),
            max_chunk_size: 64 * 1024,
            max_request_size: 32 * 1024 * 1024,
            shutdown_drain: Duration::ZERO,
            shutdown_timeout: Duration::from_secs(10),
            forward: None,
            metrics: None,
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
    shutdown_timeout: Duration,
}

/// Serve `options.protocol` on `listener` and, when given, the admin routes
/// on `admin`.
///
/// The node is constructed by the caller, which lets one node be served by
/// different sockets in tests and keeps this crate free of metadata-backend
/// choices.
pub async fn serve(
    node: Arc<PicoNode>,
    listener: Listener,
    admin: Option<Listener>,
    options: ServeOptions,
) -> RunningServer {
    let audience = match options.protocol {
        HttpProtocol::Pico => Audience::Pico,
        HttpProtocol::Ds => Audience::DurableStreams,
    };
    let common_router = common::router(
        node.topics(),
        node.ownership(),
        options.routing_mode,
        options.authorizer.clone(),
        audience,
        options.max_request_size,
    );
    let protocol_router = match options.protocol {
        HttpProtocol::Pico => common_router
            .merge(groups::router(
                groups::GroupState {
                    groups: node.groups(),
                    topics: node.topics(),
                    ownership: node.ownership(),
                    mode: options.routing_mode,
                    authorizer: options.authorizer.clone(),
                },
                options.max_request_size,
            ))
            .fallback_service(
                Arc::new(
                    PicoFrontend::with_tuning(
                        node.topics(),
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
            ),
        HttpProtocol::Ds => common_router.fallback_service(
            Arc::new(
                DsFrontend::with_tuning(
                    node.topics(),
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
        ),
    };

    let (stop, _) = watch::channel(false);
    let admin_state = AdminState::new(node.clone())
        .with_leadership(options.leadership.clone())
        .with_authorizer(options.authorizer.clone())
        .with_forward(options.forward.clone())
        .with_metrics(options.metrics);
    let mut tasks = Vec::with_capacity(2);

    let protocol_router =
        protocol_router.layer(axum::middleware::from_fn_with_state(audience, observe));
    let (local_addr, task) = spawn_listener(listener, protocol_router, &stop);
    tasks.push(task);

    let mut admin_addr = None;
    if let Some(listener) = admin {
        let admin_router = admin::router(admin_state.clone()).layer(
            axum::middleware::from_fn_with_state(Audience::Admin, observe),
        );
        let (bound, task) = spawn_listener(listener, admin_router, &stop);
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
    RunningServer {
        node,
        admin: admin_state,
        local_addr,
        admin_addr,
        stop,
        tasks,
        shutdown_drain: options.shutdown_drain,
        shutdown_timeout: options.shutdown_timeout,
    }
}

fn spawn_listener(
    listener: Listener,
    router: Router,
    stop: &watch::Sender<bool>,
) -> (SocketAddr, JoinHandle<()>) {
    let bound = listener.local_addr();
    let mut stop = stop.subscribe();
    let task = tokio::spawn(async move {
        let result = axum::serve(HttpListener(listener), router)
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
    (bound, task)
}

struct HttpListener(Listener);

impl axum::serve::Listener for HttpListener {
    type Io = Stream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Stream, SocketAddr) {
        match self.0.accept().await {
            Some(connection) => connection,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.0.local_addr())
    }
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

    /// (fail readiness, wait out the drain window), end live reads, stop
    /// accepting and let in-flight requests finish, then `node.close()`.
    pub async fn shutdown(self) {
        self.admin.stop_serving();
        if !self.shutdown_drain.is_zero() {
            tracing::info!(drain = ?self.shutdown_drain, "draining before shutdown");
            tokio::time::sleep(self.shutdown_drain).await;
        }
        self.node.service().stop_live_reads();
        let _ = self.stop.send(true);
        let deadline = tokio::time::Instant::now() + self.shutdown_timeout;
        for task in self.tasks {
            let listener = task.abort_handle();
            if tokio::time::timeout_at(deadline, task).await.is_err() {
                tracing::warn!(timeout = ?self.shutdown_timeout, "connections still open at the shutdown timeout, handing off without them");
                listener.abort();
            }
        }
        self.node.close().await;
    }
}
