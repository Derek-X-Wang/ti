//! [`McpListener`] — the MCP Server inside `ti-daemon`.
//!
//! Implements the rmcp [`ServerHandler`] trait, exposing MCP tools to Driving
//! Agents (see CONTEXT.md):
//!
//! - `create_session` — spawns a Session (Hosted Process in a PTY), registers
//!   it under a stable id, and returns that id. The creating client automatically
//!   becomes the Session's Writer (holds the Write Lock).
//! - `take_snapshot` — returns the text Snapshot (visible-screen plain text +
//!   cursor) for a given session id. Pass `include_styles: true` for per-cell
//!   color/attribute data and the alt-screen flag. Available to all callers.
//! - `read_output` — returns raw bytes from the Session's output history since
//!   a given byte offset, covering scrolled-off content beyond the visible screen.
//! - `send_keys` — types keystrokes into the Hosted Process via the Write Lock;
//!   supports `<ANGLE>` notation for special keys (`<ENTER>`, `<TAB>`, `<ESC>`, …).
//! - `list_sessions` — returns all active Sessions with id, command, dimensions,
//!   and alive/exited status.
//! - `close_session` — terminates a Session's Hosted Process and removes it from
//!   the registry; only the Writer may close.
//! - `resize` — resize the PTY and emulator to new `cols` × `rows` dimensions.
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

    /// When `true`, return per-cell color and attribute data (a [`StyledSnapshot`])
    /// plus the alt-screen flag. Default is `false` (plain-text Snapshot only).
    #[serde(default)]
    pub include_styles: bool,
}

/// Input schema for the `resize` MCP tool.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ResizeInput {
    /// The id of the Session to resize.
    pub session_id: String,
    /// New PTY width in columns.
    pub cols: u16,
    /// New PTY height in rows.
    pub rows: u16,
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

/// Input schema for the `send_keys` MCP tool.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct SendKeysInput {
    /// The id of the Session to type into.
    pub session_id: String,

    /// Sequence of key tokens to type. Each token is either:
    /// - A literal string (e.g. `"echo hi"`) typed verbatim.
    /// - A special key in `<ANGLE>` notation, e.g.:
    ///   `<ENTER>` (CR), `<TAB>`, `<ESC>`, `<BACKSPACE>`, `<DEL>`,
    ///   `<UP>`, `<DOWN>`, `<LEFT>`, `<RIGHT>`,
    ///   `<HOME>`, `<END>`, `<PGUP>`, `<PGDN>`, `<F1>`…`<F12>`,
    ///   `<CTRL-C>`, `<CTRL-D>`, `<CTRL-Z>`.
    pub keys: Vec<String>,
}

/// Input schema for the `close_session` MCP tool.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct CloseSessionInput {
    /// The id of the Session to close. Only the Writer may close a Session.
    pub session_id: String,
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

/// Convert a sequence of key tokens (including `<ANGLE>` special keys) to raw bytes.
///
/// Tokens that match a known `<ANGLE>` escape are replaced by their byte
/// sequence. Everything else is passed through as UTF-8. Unknown `<ANGLE>`
/// tokens are returned verbatim (no silent discard) so the caller sees them.
fn encode_keys(keys: &[String]) -> Vec<u8> {
    let mut out = Vec::new();
    for token in keys {
        match token.as_str() {
            "<ENTER>" => out.push(b'\r'),
            "<TAB>" => out.push(b'\t'),
            "<ESC>" => out.push(b'\x1b'),
            "<BACKSPACE>" => out.push(b'\x7f'),
            "<DEL>" => out.extend_from_slice(b"\x1b[3~"),
            "<UP>" => out.extend_from_slice(b"\x1b[A"),
            "<DOWN>" => out.extend_from_slice(b"\x1b[B"),
            "<RIGHT>" => out.extend_from_slice(b"\x1b[C"),
            "<LEFT>" => out.extend_from_slice(b"\x1b[D"),
            "<HOME>" => out.extend_from_slice(b"\x1b[H"),
            "<END>" => out.extend_from_slice(b"\x1b[F"),
            "<PGUP>" => out.extend_from_slice(b"\x1b[5~"),
            "<PGDN>" => out.extend_from_slice(b"\x1b[6~"),
            "<F1>" => out.extend_from_slice(b"\x1bOP"),
            "<F2>" => out.extend_from_slice(b"\x1bOQ"),
            "<F3>" => out.extend_from_slice(b"\x1bOR"),
            "<F4>" => out.extend_from_slice(b"\x1bOS"),
            "<F5>" => out.extend_from_slice(b"\x1b[15~"),
            "<F6>" => out.extend_from_slice(b"\x1b[17~"),
            "<F7>" => out.extend_from_slice(b"\x1b[18~"),
            "<F8>" => out.extend_from_slice(b"\x1b[19~"),
            "<F9>" => out.extend_from_slice(b"\x1b[20~"),
            "<F10>" => out.extend_from_slice(b"\x1b[21~"),
            "<F11>" => out.extend_from_slice(b"\x1b[23~"),
            "<F12>" => out.extend_from_slice(b"\x1b[24~"),
            "<CTRL-C>" => out.push(b'\x03'),
            "<CTRL-D>" => out.push(b'\x04'),
            "<CTRL-Z>" => out.push(b'\x1a'),
            other => out.extend_from_slice(other.as_bytes()),
        }
    }
    out
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

    /// Capture the current visible-screen Snapshot of a Session.
    ///
    /// By default returns the plain-text screen contents and cursor position.
    /// Pass `include_styles: true` to instead return the full structured
    /// [`StyledSnapshot`] as JSON: per-cell character + foreground/background
    /// color + attributes for every visible row, plus cursor position and the
    /// alt-screen flag. Use the styled form to reason about colors or paint a
    /// faithful replica; the plain-text form is smaller and simpler.
    #[tool(description = "Return the visible-screen Snapshot for a Session. \
                       Pass include_styles=true to return structured per-cell color/attribute JSON.")]
    fn take_snapshot(
        &self,
        Parameters(TakeSnapshotInput {
            session_id,
            include_styles,
        }): Parameters<TakeSnapshotInput>,
    ) -> Result<CallToolResult, ErrorData> {
        if include_styles {
            self.registry
                .take_snapshot_styled(&session_id)
                .and_then(|snap| {
                    serde_json::to_string(&snap)
                        .map_err(|e| anyhow::anyhow!("failed to serialize styled snapshot: {e}"))
                })
                .map(|json| CallToolResult::success(vec![Content::text(json)]))
                .map_err(|e| ErrorData::internal_error(e.to_string(), None))
        } else {
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

    /// Resize the PTY and emulator of a Session to new dimensions.
    ///
    /// Sends `SIGWINCH` to the Hosted Process and updates the avt screen buffer
    /// to the new `cols` × `rows` size. TUIs and shells will reflow their output
    /// to fit the new dimensions. Available to all callers.
    #[tool(description = "Resize a Session's PTY and emulator to cols × rows. \
                       Sends SIGWINCH to the Hosted Process so TUIs reflow.")]
    fn resize(
        &self,
        Parameters(ResizeInput {
            session_id,
            cols,
            rows,
        }): Parameters<ResizeInput>,
    ) -> Result<CallToolResult, ErrorData> {
        self.registry
            .resize(&session_id, cols, rows)
            .map(|()| {
                let body = format!("resized session '{session_id}' to {cols}×{rows}");
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

    /// Type keystrokes into a Session's Hosted Process.
    ///
    /// Accepts a sequence of key tokens. Literal strings are typed verbatim;
    /// special keys use `<ANGLE>` notation (e.g. `<ENTER>`, `<CTRL-C>`). Only
    /// the Session's Writer may call this tool — the Write Lock is enforced by
    /// the registry.
    #[tool(
        description = "Type keystrokes into a Session. Keys are a list of tokens: literals \
                       are typed verbatim; use <ENTER>, <TAB>, <ESC>, <CTRL-C> etc. for \
                       special keys. Only the Writer (session creator) may send keys."
    )]
    fn send_keys(
        &self,
        Parameters(SendKeysInput { session_id, keys }): Parameters<SendKeysInput>,
    ) -> Result<CallToolResult, ErrorData> {
        let bytes = encode_keys(&keys);
        self.registry
            .write_input(&session_id, &self.client_id, &bytes)
            .map(|()| {
                CallToolResult::success(vec![Content::text(format!(
                    "sent {} byte(s)",
                    bytes.len()
                ))])
            })
            .map_err(|e| ErrorData::internal_error(e.to_string(), None))
    }

    /// List all Sessions in the registry.
    ///
    /// Returns each Session's id, command, PTY dimensions (cols×rows), and
    /// alive/exited status with exit code or signal name when available.
    #[tool(
        description = "List all Sessions with id, command, dimensions, and alive/exited status."
    )]
    fn list_sessions(&self) -> Result<CallToolResult, ErrorData> {
        self.registry
            .list_sessions()
            .map(|infos| {
                if infos.is_empty() {
                    return CallToolResult::success(vec![Content::text("(no sessions)")]);
                }
                let mut lines = Vec::with_capacity(infos.len());
                for info in &infos {
                    let status = match &info.exit_status {
                        None => "alive".to_string(),
                        Some(s) => format!("exited ({})", s),
                    };
                    lines.push(format!(
                        "{}\t{}\t{}x{}\t{}",
                        info.id, info.command, info.cols, info.rows, status
                    ));
                }
                CallToolResult::success(vec![Content::text(lines.join("\n"))])
            })
            .map_err(|e| ErrorData::internal_error(e.to_string(), None))
    }

    /// Terminate a Session's Hosted Process and remove it from the registry.
    ///
    /// Sends a kill signal to the Hosted Process (if still running) then removes
    /// the Session. Only the Writer (the client that created the Session) may
    /// close it.
    #[tool(description = "Terminate a Session's Hosted Process and remove it. \
                       Only the Writer (session creator) may close a Session.")]
    fn close_session(
        &self,
        Parameters(CloseSessionInput { session_id }): Parameters<CloseSessionInput>,
    ) -> Result<CallToolResult, ErrorData> {
        self.registry
            .close_session(&session_id, &self.client_id)
            .map(|()| {
                CallToolResult::success(vec![Content::text(format!(
                    "closed session '{session_id}'"
                ))])
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
                 send_keys to type input (only the Writer may do this), \
                 take_snapshot to read the visible screen (pass include_styles=true for color data), \
                 read_output to page through the full output history \
                 (including content scrolled off the visible screen), \
                 resize to change the PTY dimensions, \
                 list_sessions to enumerate active Sessions, \
                 and close_session to terminate and remove a Session.",
        )
    }
}
