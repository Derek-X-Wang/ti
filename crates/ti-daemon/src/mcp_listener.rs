//! [`McpListener`] — the MCP Server inside `ti-daemon`.
//!
//! Implements the rmcp [`ServerHandler`] trait, exposing MCP tools to Driving
//! Agents (see CONTEXT.md):
//!
//! - `create_session` — spawns a Session (Hosted Process in a PTY), registers
//!   it under a stable id, and returns that id. The creating client automatically
//!   becomes the Session's Writer (holds the Write Lock).
//! - `take_snapshot` — returns the text Snapshot (visible-screen plain text +
//!   cursor) for a given session id. Available to Writers and Observers alike.
//! - `read_output` — returns raw bytes from the Session's output history since
//!   a given byte offset, covering scrolled-off content beyond the visible screen.
//!
//! ## Write Lock
//!
//! Each `McpListener` instance is assigned a unique `client_id` at construction
//! time. When `create_session` is called, `client_id` is stored as the Session's
//! Writer in the [`SessionRegistry`]. Future write operations (send_keys, etc.)
//! must pass `client_id` so the registry can enforce the Write Lock.
//!
//! The MCP listener is kept as a clean internal module over TI Core so other
//! transports (stdio adapter, gRPC) can be layered later per ADR-0001.

use std::sync::atomic::{AtomicU64, Ordering};

use rmcp::{
    handler::server::router::tool::ToolRouter,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, ErrorData, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ServerHandler,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::registry::SessionRegistry;

/// Monotonic counter used to generate unique `client_id` values.
///
/// Each [`McpListener`] instance (one per MCP client connection) gets a unique
/// numeric id stamped at construction time. This avoids a dependency on `uuid`
/// or `rand` while still guaranteeing uniqueness within a daemon process lifetime.
static CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Input schema for the `create_session` MCP tool.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct CreateSessionInput {
    /// A stable, caller-chosen identifier for the new Session.
    ///
    /// Must be unique within this daemon instance. The daemon will reject a
    /// duplicate id with an error.
    pub session_id: String,

    /// The program to run as the Hosted Process (e.g. `"bash"`, `"zsh"`).
    ///
    /// Defaults to the user's login shell (`$SHELL`, falling back to `"bash"`)
    /// when omitted.
    #[serde(default)]
    pub program: Option<String>,
}

/// Input schema for the `take_snapshot` MCP tool.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct TakeSnapshotInput {
    /// The id of the Session to snapshot — as returned by `create_session`.
    pub session_id: String,
}

/// Input schema for the `read_output` MCP tool.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ReadOutputInput {
    /// The id of the Session to read output from.
    pub session_id: String,

    /// Byte offset to read from. Pass `0` for the full history since session
    /// start. Use the `next_offset` from a previous `read_output` response to
    /// page forward without re-reading old bytes.
    #[serde(default)]
    pub since: u64,
}

/// The MCP Server handler embedded in the TI Daemon.
///
/// Each MCP client session gets its own [`McpListener`] instance (the factory
/// pattern used by [`rmcp::transport::streamable_http_server::StreamableHttpService`]),
/// but all instances share the same [`SessionRegistry`] — the registry is the
/// daemon's single source of truth for Sessions.
///
/// `client_id` is unique per instance and is used as the Writer identity when
/// creating Sessions. The Write Lock is enforced by [`SessionRegistry`].
#[derive(Clone)]
pub struct McpListener {
    registry: SessionRegistry,
    /// Unique identity for this MCP client connection.
    ///
    /// Assigned at construction from a process-global counter. Stored as the
    /// Session's `writer_id` in [`SessionRegistry`] when `create_session` is
    /// called, establishing this client as the Session's Writer.
    pub(crate) client_id: String,
    // The `#[tool_router]` macro reads this field at runtime to dispatch
    // `tools/list` and `tools/call`. The dead_code lint can't see through
    // macro-generated code, so we suppress it explicitly.
    #[allow(dead_code)]
    tool_router: ToolRouter<McpListener>,
}

impl McpListener {
    /// Create a new MCP listener backed by the given registry.
    ///
    /// Each call generates a unique `client_id` via a process-global counter.
    pub fn new(registry: SessionRegistry) -> Self {
        let client_id = format!(
            "client-{}",
            CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        Self::with_client_id(registry, client_id)
    }

    /// Create a new MCP listener with an explicit `client_id`.
    ///
    /// Intended for tests that need a deterministic, known Writer identity.
    /// Scoped to `#[cfg(test)]` so production code always goes through `new`
    /// with the counter-generated id — callers cannot impersonate another client.
    #[cfg(test)]
    pub fn with_client_id(registry: SessionRegistry, client_id: impl Into<String>) -> Self {
        Self {
            registry,
            client_id: client_id.into(),
            tool_router: Self::tool_router(),
        }
    }

    /// Internal constructor shared by `new` and the test-only `with_client_id`.
    #[cfg(not(test))]
    fn with_client_id(registry: SessionRegistry, client_id: impl Into<String>) -> Self {
        Self {
            registry,
            client_id: client_id.into(),
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl McpListener {
    /// Spawn a Session (a Hosted Process running inside a PTY) and register it.
    ///
    /// Returns the session id on success. The session id is stable for the
    /// lifetime of the daemon.
    #[tool(
        description = "Spawn a Hosted Process in a PTY Session and return its session id. \
                       The calling client becomes the Session's Writer and holds the Write Lock."
    )]
    fn create_session(
        &self,
        Parameters(CreateSessionInput {
            session_id,
            program,
        }): Parameters<CreateSessionInput>,
    ) -> Result<CallToolResult, ErrorData> {
        let program = program
            .or_else(|| std::env::var("SHELL").ok())
            .unwrap_or_else(|| "bash".to_string());

        self.registry
            .create_session(session_id.clone(), &program, &[], self.client_id.clone())
            .map(|id| CallToolResult::success(vec![Content::text(id)]))
            .map_err(|e| ErrorData::internal_error(e.to_string(), None))
    }

    /// Capture the current visible-screen text Snapshot of a Session.
    ///
    /// Returns the plain-text contents of the visible screen followed by the
    /// cursor position. The Driving Agent uses this to "see" what is on screen
    /// in the Hosted Process.
    #[tool(
        description = "Return the visible-screen text Snapshot (plain text + cursor) for a Session."
    )]
    fn take_snapshot(
        &self,
        Parameters(TakeSnapshotInput { session_id }): Parameters<TakeSnapshotInput>,
    ) -> Result<CallToolResult, ErrorData> {
        self.registry
            .take_snapshot(&session_id)
            .map(|snap| {
                let body = format!(
                    "{}\n[cursor col={} row={} visible={}]",
                    snap.text(),
                    snap.cursor_col,
                    snap.cursor_row,
                    snap.cursor_visible,
                );
                CallToolResult::success(vec![Content::text(body)])
            })
            .map_err(|e| ErrorData::internal_error(e.to_string(), None))
    }

    /// Read raw output from a Session's output history.
    ///
    /// Returns all bytes from `since` (a byte offset) to the current end of the
    /// output stream, along with the starting offset and the next offset for
    /// pagination. Covers scrolled-off content that avt's thin visible screen
    /// does not retain (see ADR-0002).
    ///
    /// Retention policy: all output since session start is kept in memory for the
    /// lifetime of the Session. No cap is applied in v1.
    #[tool(
        description = "Return raw output from a Session's history since a byte offset. \
                       Use next_offset from the response to page forward."
    )]
    fn read_output(
        &self,
        Parameters(ReadOutputInput { session_id, since }): Parameters<ReadOutputInput>,
    ) -> Result<CallToolResult, ErrorData> {
        self.registry
            .read_output(&session_id, since)
            .map(|chunk| {
                // Decode raw bytes to text here — at the MCP boundary where the
                // text-content constraint is known. `ti-core` stays byte-agnostic.
                let text = String::from_utf8_lossy(&chunk.data);
                let byte_count = chunk.data.len() as u64;
                let next_offset = chunk.offset + byte_count;
                let body = format!(
                    "[offset={} next_offset={} bytes={}]\n{}",
                    chunk.offset, next_offset, byte_count, text,
                );
                CallToolResult::success(vec![Content::text(body)])
            })
            .map_err(|e| ErrorData::internal_error(e.to_string(), None))
    }
}

/// Wire the tool router into `ServerHandler` so `tools/list` and `tools/call`
/// dispatch through `self.tool_router`. Without `#[tool_handler]`, the default
/// `ServerHandler::call_tool` returns `method_not_found`.
#[tool_handler]
impl ServerHandler for McpListener {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "TI Daemon — headless terminal. \
                 Use create_session to spawn a Hosted Process, \
                 take_snapshot to read the visible screen, \
                 and read_output to page through the full output history \
                 (including content scrolled off the visible screen).",
        )
    }
}
