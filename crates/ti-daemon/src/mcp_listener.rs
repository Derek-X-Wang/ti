//! [`McpListener`] — the MCP Server inside `ti-daemon`.
//!
//! Implements the rmcp [`ServerHandler`] trait, exposing two MCP tools to
//! Driving Agents (see CONTEXT.md):
//!
//! - `create_session` — spawns a Session (Hosted Process in a PTY), registers
//!   it under a stable id, and returns that id.
//! - `take_snapshot` — returns the text Snapshot (visible-screen plain text +
//!   cursor) for a given session id.
//!
//! The MCP listener is kept as a clean internal module over TI Core so other
//! transports (stdio adapter, gRPC) can be layered later per ADR-0001.

use rmcp::{
    handler::server::router::tool::ToolRouter,
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, ErrorData, ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ServerHandler,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::registry::SessionRegistry;

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

/// The MCP Server handler embedded in the TI Daemon.
///
/// Each MCP client session gets its own [`McpListener`] instance (the factory
/// pattern used by [`rmcp::transport::streamable_http_server::StreamableHttpService`]),
/// but all instances share the same [`SessionRegistry`] — the registry is the
/// daemon's single source of truth for Sessions.
#[derive(Clone)]
pub struct McpListener {
    registry: SessionRegistry,
    // The `#[tool_router]` macro reads this field at runtime to dispatch
    // `tools/list` and `tools/call`. The dead_code lint can't see through
    // macro-generated code, so we suppress it explicitly.
    #[allow(dead_code)]
    tool_router: ToolRouter<McpListener>,
}

impl McpListener {
    /// Create a new MCP listener backed by the given registry.
    pub fn new(registry: SessionRegistry) -> Self {
        Self {
            registry,
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
    #[tool(description = "Spawn a Hosted Process in a PTY Session and return its session id.")]
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
            .create_session(session_id.clone(), &program, &[])
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
                 then take_snapshot to read the visible screen.",
        )
    }
}
