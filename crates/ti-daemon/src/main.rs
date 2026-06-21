//! `ti-daemon` — TI's long-lived headless daemon.
//!
//! The single source of truth for every Session. Ships as a LaunchAgent inside
//! a code-signed TI.app bundle (see `docs/adr/0003-tcc-drives-app-bundle-launchagent.md`).
//! Driving Agents connect through the MCP listener over streamable-HTTP/SSE on a
//! localhost port, authenticated by a Bearer Token. Non-MCP Observers (e.g. the
//! native Inspector) connect over a local Unix domain socket.
//!
//! ## Configuration (environment variables)
//!
//! - `MCP_PORT` — TCP port the MCP listener binds on (default: `3000`).
//! - `MCP_BEARER_TOKEN` — shared secret required on every MCP connection.
//!   Must be set; the daemon refuses to start without it to enforce the
//!   "even localhost requires auth" rule from CONTEXT.md (Bearer Token).
//! - `OBSERVER_SOCKET_PATH` — path for the Observer Unix socket
//!   (default: `/tmp/ti-observer.sock`).
//!
//! ## Architecture
//!
//! The daemon binds `127.0.0.1` only (v1 local-only rule per CONTEXT.md).
//! The MCP listener is a clean module over TI Core so other transports can be
//! added later (ADR-0001). Bearer Token auth is a single axum middleware layer
//! so it applies to all routes uniformly. The Observer socket runs in a separate
//! tokio task sharing the same SessionRegistry — no additional lock is needed.

use std::sync::Arc;

use axum::middleware;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpService,
};
use ti_daemon::{
    auth::bearer_auth, observer_socket::ObserverSocketServer, McpListener, SessionRegistry,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Structured logging — respects RUST_LOG; defaults to info.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("ti_daemon=info".parse()?))
        .init();

    let port: u16 = std::env::var("MCP_PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .map_err(|_| anyhow::anyhow!("MCP_PORT must be a valid port number"))?;

    let token = std::env::var("MCP_BEARER_TOKEN").map_err(|_| {
        anyhow::anyhow!(
            "MCP_BEARER_TOKEN must be set — the daemon requires a Bearer Token even on localhost \
             to guard Session Write Locks (see CONTEXT.md)"
        )
    })?;
    let token = Arc::new(token);

    // The SessionRegistry is the daemon's single source of truth for Sessions.
    // All McpListener instances (one per MCP client connection) share it.
    // The Observer socket also shares the same registry.
    let registry = SessionRegistry::new();

    let observer_socket_path = std::env::var("OBSERVER_SOCKET_PATH")
        .unwrap_or_else(|_| "/tmp/ti-observer.sock".to_string());
    let observer_server = ObserverSocketServer::bind(
        std::path::Path::new(&observer_socket_path),
        registry.clone(),
    )
    .await?;
    let shutdown_ct = CancellationToken::new();
    let obs_ct = shutdown_ct.child_token();
    tokio::spawn(async move { observer_server.run(obs_ct).await });

    let mcp_service: StreamableHttpService<McpListener, LocalSessionManager> =
        StreamableHttpService::new(
            {
                let registry = registry.clone();
                move || Ok(McpListener::new(registry.clone()))
            },
            LocalSessionManager::default().into(),
            Default::default(),
        );

    let app = axum::Router::new()
        .nest_service("/mcp", mcp_service)
        .route_layer(middleware::from_fn_with_state(token, bearer_auth));

    let addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("ti-daemon MCP listener on http://{addr}/mcp");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl-C handler");
            tracing::info!("ti-daemon shutting down");
            shutdown_ct.cancel();
        })
        .await?;

    Ok(())
}
