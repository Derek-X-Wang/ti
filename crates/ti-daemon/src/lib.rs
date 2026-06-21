//! `ti-daemon` — library interface exposed for integration testing.
//!
//! The daemon binary lives in `main.rs`. This module re-exports the types
//! and helpers that integration tests need to spin up an in-process server
//! and drive it through the public API without spawning a subprocess.

pub mod mcp_listener;
pub mod observer_socket;
pub mod registry;

pub use mcp_listener::McpListener;
pub use registry::{SessionInfo, SessionRegistry};
pub use ti_core::{ObserverHandle, ScreenUpdate, StyledSnapshot};

/// Axum middleware that enforces Bearer Token authentication on every request.
///
/// Reads `Authorization: Bearer <token>` from the request header and compares it
/// against the expected token stored in axum `State`. Returns 401 Unauthorized
/// for missing or incorrect tokens.
///
/// Used by both the daemon `main.rs` and integration tests — kept here so the
/// auth logic has a single canonical implementation.
pub mod auth {
    use std::sync::Arc;

    use axum::{
        extract::{Request, State},
        http::StatusCode,
        middleware::Next,
        response::IntoResponse,
    };

    pub async fn bearer_auth(
        State(expected): State<Arc<String>>,
        request: Request,
        next: Next,
    ) -> impl IntoResponse {
        let provided = request
            .headers()
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));

        match provided {
            Some(token) if token == expected.as_str() => next.run(request).await,
            _ => StatusCode::UNAUTHORIZED.into_response(),
        }
    }
}
