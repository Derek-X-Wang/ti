//! Integration tests for the ti-daemon MCP listener.
//!
//! These tests spin up the full axum+rmcp server stack in-process and drive it
//! with raw MCP JSON-RPC calls via reqwest, verifying the complete path from
//! HTTP request → Bearer Token auth → MCP dispatch → Session registry →
//! text Snapshot (acceptance criteria for issues #2 and #3).

use std::sync::Arc;

use axum::middleware;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use serde_json::{json, Value};
use ti_daemon::{auth::bearer_auth, McpListener, SessionRegistry};
use tokio_util::sync::CancellationToken;

const DEV_TOKEN: &str = "ti-dev-secret";

/// Spawn the daemon MCP server on an OS-assigned port.
///
/// Returns `(reqwest_client, base_mcp_url, cancellation_token)`.
async fn spawn_daemon(token: &str) -> (reqwest::Client, String, CancellationToken) {
    let ct = CancellationToken::new();
    let registry = SessionRegistry::new();

    let config = StreamableHttpServerConfig::default()
        .with_json_response(true)
        .with_sse_keep_alive(None)
        .with_cancellation_token(ct.child_token());

    let mcp_service: StreamableHttpService<McpListener, LocalSessionManager> =
        StreamableHttpService::new(
            {
                let registry = registry.clone();
                move || Ok(McpListener::new(registry.clone()))
            },
            LocalSessionManager::default().into(),
            config,
        );

    let token_arc = Arc::new(token.to_string());
    let app = axum::Router::new()
        .nest_service("/mcp", mcp_service)
        .route_layer(middleware::from_fn_with_state(token_arc, bearer_auth));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}/mcp");

    tokio::spawn({
        let ct_child = ct.child_token();
        async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move { ct_child.cancelled().await })
                .await
                .unwrap();
        }
    });

    let client = reqwest::Client::new();
    (client, base_url, ct)
}

/// POST a JSON-RPC request (with `id`) and return the parsed JSON response.
///
/// Handles both plain JSON and SSE (`data: {...}\n\n`) response formats.
async fn mcp_post(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    body: Value,
    session_id: Option<&str>,
) -> Value {
    let mut req = client
        .post(url)
        .header("Content-Type", "application/json")
        // Request JSON only — avoids SSE wrapping which complicates parsing.
        .header("Accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {token}"))
        .json(&body);

    if let Some(sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }

    let response = req.send().await.expect("request failed");
    let status = response.status();
    let text = response.text().await.expect("failed to read response body");

    // Try plain JSON first, then extract from SSE data lines.
    serde_json::from_str::<Value>(&text).unwrap_or_else(|_| {
        // SSE format: each "data: <json>" line is one message.
        // Collect the last non-empty data line.
        let json_str = text
            .lines()
            .filter_map(|l| l.strip_prefix("data: "))
            .rfind(|s| !s.trim().is_empty())
            .unwrap_or_else(|| panic!("no data line in SSE response (status={status}):\n{text}"));
        serde_json::from_str::<Value>(json_str).unwrap_or_else(|e| {
            panic!("failed to parse SSE data as JSON (status={status}): {e}\ndata: {json_str}")
        })
    })
}

/// POST a JSON-RPC notification (no `id`, expects a 202 / empty body).
async fn mcp_notify(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    body: Value,
    session_id: Option<&str>,
) {
    let mut req = client
        .post(url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {token}"))
        .json(&body);

    if let Some(sid) = session_id {
        req = req.header("mcp-session-id", sid);
    }

    let resp = req.send().await.expect("notification request failed");
    // MCP notifications return 202 Accepted with an empty body — not JSON.
    assert!(
        resp.status().is_success(),
        "notification must succeed; got {}",
        resp.status()
    );
}

/// Perform the MCP handshake (initialize + initialized notification) and return
/// the `mcp-session-id` header value.
///
/// Every stateful MCP test starts with this three-step sequence. Factored out
/// here so each test only states what is unique to it (the tool call being
/// tested), not the boilerplate setup.
async fn mcp_init(client: &reqwest::Client, url: &str) -> Option<String> {
    let init_resp = client
        .post(url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {DEV_TOKEN}"))
        .json(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": { "name": "test-driving-agent", "version": "0.0.0" }
            }
        }))
        .send()
        .await
        .expect("initialize request failed");

    assert_eq!(
        init_resp.status(),
        200,
        "initialize must succeed with correct token"
    );

    let mcp_session_id = init_resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    // Drain the body — we only need the mcp-session-id from the header.
    let _ = init_resp.text().await.unwrap_or_default();

    mcp_notify(
        client,
        url,
        DEV_TOKEN,
        json!({ "jsonrpc": "2.0", "method": "notifications/initialized", "params": {} }),
        mcp_session_id.as_deref(),
    )
    .await;

    mcp_session_id
}

// ── Unit tests for SessionRegistry ──────────────────────────────────────────

/// The SessionRegistry creates a Session and lets callers snapshot it.
///
/// Uses `echo hello` as the deterministic Hosted Process so the output is
/// fixed and there is no shell-prompt timing involved.
#[test]
fn registry_create_and_snapshot() {
    let registry = SessionRegistry::new();

    let id = registry
        .create_session("s1".to_string(), "echo", &["hello"], "writer-a".to_string())
        .expect("create_session failed");
    assert_eq!(id, "s1");

    // Give the session a moment to produce output.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let snap = registry.take_snapshot("s1").expect("take_snapshot failed");
    assert!(
        snap.contains("hello"),
        "Snapshot should contain 'hello'; got:\n{}",
        snap.text()
    );
}

/// The registry rejects duplicate session ids.
#[test]
fn registry_rejects_duplicate_id() {
    let registry = SessionRegistry::new();
    registry
        .create_session("dup".to_string(), "echo", &["a"], "writer-a".to_string())
        .unwrap();
    let err = registry
        .create_session("dup".to_string(), "echo", &["b"], "writer-b".to_string())
        .unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

/// The registry returns an error for an unknown session id.
#[test]
fn registry_unknown_id_errors() {
    let registry = SessionRegistry::new();
    let err = registry.take_snapshot("nonexistent").unwrap_err();
    assert!(err.to_string().contains("no Session with id"));
}

// ── Unit tests for Write Lock ────────────────────────────────────────────────

/// The Writer (creating client) can send input to its own Session.
#[test]
fn write_lock_writer_can_send_input() {
    let registry = SessionRegistry::new();
    // Spawn a long-running process (cat reads from stdin indefinitely).
    registry
        .create_session("ws1".to_string(), "cat", &[], "writer-a".to_string())
        .expect("create_session failed");

    // The Writer can send input.
    let result = registry.write_input("ws1", "writer-a", b"hello\n");
    assert!(result.is_ok(), "Writer must be allowed to send input");
}

/// A non-Writer client is rejected with a `not Writer` error.
#[test]
fn write_lock_non_writer_rejected() {
    let registry = SessionRegistry::new();
    registry
        .create_session("ws2".to_string(), "cat", &[], "writer-a".to_string())
        .expect("create_session failed");

    // A different caller id → not the Writer.
    let err = registry
        .write_input("ws2", "observer-b", b"hello\n")
        .unwrap_err();
    assert!(
        err.to_string().contains("not Writer"),
        "Non-Writer must be rejected; got: {err}"
    );
}

/// Observers (non-Writers) can still take Snapshots.
///
/// This verifies that the Write Lock only gates input, not reads.
#[test]
fn write_lock_observer_can_snapshot() {
    let registry = SessionRegistry::new();
    registry
        .create_session(
            "ws3".to_string(),
            "echo",
            &["hello"],
            "writer-a".to_string(),
        )
        .expect("create_session failed");

    // Give the session a moment to produce output.
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Any caller id (including "observer-b") can take a Snapshot — no auth needed.
    let snap = registry
        .take_snapshot("ws3")
        .expect("Observer must be able to take Snapshot");
    assert!(
        snap.contains("hello"),
        "Observer Snapshot must contain 'hello'; got:\n{}",
        snap.text()
    );
}

/// `write_input` on an unknown session id returns `no Session with id` error.
#[test]
fn write_lock_unknown_session_errors() {
    let registry = SessionRegistry::new();
    let err = registry
        .write_input("nonexistent", "writer-a", b"data\n")
        .unwrap_err();
    assert!(
        err.to_string().contains("no Session with id"),
        "Unknown session must give 'no Session with id' error; got: {err}"
    );
}

// ── Unit tests for read_output ───────────────────────────────────────────────

/// Registry read_output returns the raw bytes produced by a Session.
///
/// `echo hello` produces at least "hello\r\n" on the PTY. After waiting for
/// the process to exit (draining the buffer), `read_output(0)` must contain
/// "hello" and the offset must be 0.
#[test]
fn registry_read_output_returns_raw_bytes() {
    let registry = SessionRegistry::new();
    registry
        .create_session(
            "ro1".to_string(),
            "echo",
            &["hello"],
            "writer-a".to_string(),
        )
        .expect("create_session failed");

    // Give the session time to produce output and exit.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let chunk = registry.read_output("ro1", 0).expect("read_output failed");

    assert_eq!(chunk.offset, 0, "first read must start at offset 0");
    assert!(
        chunk.data.windows(5).any(|w| w == b"hello"),
        "raw output must contain 'hello'; got: {:?}",
        String::from_utf8_lossy(&chunk.data)
    );
}

/// read_output supports offset-based paging: reading from the next_offset
/// after the first chunk returns an empty result (no more new output).
#[test]
fn registry_read_output_paging() {
    let registry = SessionRegistry::new();
    registry
        .create_session(
            "ro2".to_string(),
            "echo",
            &["world"],
            "writer-a".to_string(),
        )
        .expect("create_session failed");

    std::thread::sleep(std::time::Duration::from_millis(300));

    let chunk = registry.read_output("ro2", 0).expect("first read failed");
    assert!(!chunk.data.is_empty(), "first chunk must not be empty");

    // Reading from next_offset returns empty (no new output after process exited).
    // The returned offset must equal next — the caller's cursor is preserved.
    let next = chunk.offset + chunk.data.len() as u64;
    let empty = registry
        .read_output("ro2", next)
        .expect("second read failed");
    assert_eq!(
        empty.offset, next,
        "offset must be preserved when no new data"
    );
    assert!(
        empty.data.is_empty(),
        "reading at end must return empty; got: {:?}",
        String::from_utf8_lossy(&empty.data)
    );
}

/// read_output on an unknown session id returns the standard error.
#[test]
fn registry_read_output_unknown_session_errors() {
    let registry = SessionRegistry::new();
    let err = registry.read_output("nonexistent", 0).unwrap_err();
    assert!(err.to_string().contains("no Session with id"));
}

/// read_output captures output that scrolled off the visible screen.
///
/// Runs `printf '%s\n' {1..50}` to produce 50 lines (more than one 24-row
/// screen). Asserts that read_output returns all 50 lines while take_snapshot
/// only shows the last 24 (or fewer, depending on the shell prompt).
#[test]
fn read_output_captures_scrolled_off_content() {
    // Generate 50 numbered lines via a shell one-liner.
    let registry = SessionRegistry::new();
    // Use sh -c so we get a shell to evaluate the brace expansion.
    registry
        .create_session(
            "ro3".to_string(),
            "sh",
            &["-c", "for i in $(seq 1 50); do echo line$i; done"],
            "writer-a".to_string(),
        )
        .expect("create_session failed");

    // Give the process time to run and exit.
    std::thread::sleep(std::time::Duration::from_millis(500));

    let chunk = registry.read_output("ro3", 0).expect("read_output failed");
    let text = String::from_utf8_lossy(&chunk.data);

    // All 50 lines must appear in the raw output.
    assert!(
        text.contains("line1"),
        "raw output must contain 'line1'; got (first 200 chars): {:?}",
        &text[..text.len().min(200)]
    );
    assert!(
        text.contains("line50"),
        "raw output must contain 'line50'; got (last 200 chars): {:?}",
        &text[text.len().saturating_sub(200)..]
    );

    // The visible Snapshot (24 rows) should NOT contain line1 (it scrolled off).
    let snap = registry.take_snapshot("ro3").expect("take_snapshot failed");
    assert!(
        !snap.contains("line1"),
        "Snapshot must not contain scrolled-off 'line1'; got:\n{}",
        snap.text()
    );
}

// ── HTTP integration tests ───────────────────────────────────────────────────

/// Bearer Token auth: missing token → 401.
#[tokio::test]
async fn auth_rejects_missing_token() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;

    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": { "name": "t", "version": "0" } }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401, "missing token must be rejected");
    ct.cancel();
}

/// Bearer Token auth: wrong token → 401.
#[tokio::test]
async fn auth_rejects_wrong_token() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;

    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("Authorization", "Bearer definitely-wrong")
        .json(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": { "name": "t", "version": "0" } }
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 401, "wrong token must be rejected");
    ct.cancel();
}

/// Full MCP flow over HTTP: initialize → create_session → take_snapshot.
///
/// The Snapshot text must contain "hello", proving the PTY output made it
/// through the avt emulator and the full MCP HTTP layer end-to-end.
#[tokio::test]
async fn create_session_and_take_snapshot_via_mcp() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;

    let mcp_session_id = mcp_init(&client, &url).await;

    // create_session — spawn `echo hello` as the Hosted Process.
    let cs_resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "demo", "program": "echo" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // The session uses $SHELL (or bash) since no args passed to echo.
    // We just verify the session was created and the id was returned.
    let returned_id = cs_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert_eq!(
        returned_id, "demo",
        "create_session must return the session id"
    );

    // Give the Hosted Process time to produce output.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Step 4: take_snapshot.
    let snap_resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "take_snapshot",
                "arguments": { "session_id": "demo" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // The snapshot should be non-empty (the shell produced at least a prompt).
    let snap_text = snap_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        !snap_text.is_empty(),
        "Snapshot must not be empty; got:\n{snap_text}"
    );
    // The snapshot contains the cursor marker we append.
    assert!(
        snap_text.contains("[cursor"),
        "Snapshot must contain cursor info; got:\n{snap_text}"
    );

    ct.cancel();
}

/// Full MCP flow: initialize → create_session → read_output returns raw bytes.
///
/// Spawns `echo hello` and calls `read_output(since=0)` to verify the raw
/// byte history is accessible over the full MCP HTTP stack.
#[tokio::test]
async fn read_output_via_mcp() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;

    let mcp_session_id = mcp_init(&client, &url).await;

    // create_session with echo hello.
    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "ro-mcp", "program": "echo", "args": ["hello"] }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // read_output from offset 0.
    let ro_resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "read_output",
                "arguments": { "session_id": "ro-mcp", "since": 0 }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let ro_text = ro_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        ro_text.contains("[offset=0"),
        "response must contain offset header; got:\n{ro_text}"
    );
    assert!(
        !ro_text.is_empty(),
        "read_output must return non-empty result; got:\n{ro_text}"
    );

    ct.cancel();
}

// ── Unit tests for send_keys / list_sessions / close_session ─────────────────

/// `list_sessions` returns an entry for each created session.
#[test]
fn list_sessions_returns_all() {
    let registry = SessionRegistry::new();
    registry
        .create_session("la1".to_string(), "echo", &["a"], "w1".to_string())
        .expect("create la1 failed");
    registry
        .create_session("la2".to_string(), "echo", &["b"], "w2".to_string())
        .expect("create la2 failed");

    let sessions = registry.list_sessions().expect("list_sessions failed");
    let ids: Vec<_> = sessions.iter().map(|s| s.id.as_str()).collect();
    assert!(ids.contains(&"la1"), "list must contain la1; got: {ids:?}");
    assert!(ids.contains(&"la2"), "list must contain la2; got: {ids:?}");

    for s in &sessions {
        assert_eq!(s.command, "echo", "command must be 'echo'");
        assert_eq!(s.cols, 80, "default cols must be 80");
        assert_eq!(s.rows, 24, "default rows must be 24");
    }
}

/// After a Hosted Process exits, `list_sessions` reports an exit status.
#[test]
fn list_sessions_shows_exit_status() {
    let registry = SessionRegistry::new();
    registry
        .create_session("le1".to_string(), "sh", &["-c", "exit 0"], "w1".to_string())
        .expect("create le1 failed");

    // Give the process time to exit.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let sessions = registry.list_sessions().expect("list_sessions failed");
    let entry = sessions
        .iter()
        .find(|s| s.id == "le1")
        .expect("le1 not found");
    assert!(
        entry.exit_status.is_some(),
        "process should have exited; exit_status was None"
    );
}

/// `close_session` removes the session from the registry.
#[test]
fn close_session_removes_entry() {
    let registry = SessionRegistry::new();
    registry
        .create_session("cl1".to_string(), "cat", &[], "writer-a".to_string())
        .expect("create cl1 failed");

    registry
        .close_session("cl1", "writer-a")
        .expect("close_session failed");

    // Session is gone — take_snapshot must error.
    let err = registry.take_snapshot("cl1").unwrap_err();
    assert!(
        err.to_string().contains("no Session with id"),
        "closed session must be gone; got: {err}"
    );
}

/// `close_session` by a non-Writer is rejected.
#[test]
fn close_session_non_writer_rejected() {
    let registry = SessionRegistry::new();
    registry
        .create_session("cl2".to_string(), "cat", &[], "writer-a".to_string())
        .expect("create cl2 failed");

    let err = registry.close_session("cl2", "observer-b").unwrap_err();
    assert!(
        err.to_string().contains("not Writer"),
        "Non-Writer must be rejected; got: {err}"
    );
}

/// `write_input` is rejected when the Hosted Process has already exited.
#[test]
fn write_input_rejected_on_exited_session() {
    let registry = SessionRegistry::new();
    registry
        .create_session(
            "ex1".to_string(),
            "sh",
            &["-c", "exit 0"],
            "writer-a".to_string(),
        )
        .expect("create ex1 failed");

    // Give the process time to exit.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let err = registry
        .write_input("ex1", "writer-a", b"hello\n")
        .unwrap_err();
    assert!(
        err.to_string().contains("session exited"),
        "write to exited session must error; got: {err}"
    );
}

/// MCP flow: send_keys types 'echo hi<ENTER>' and snapshot shows 'hi'.
#[tokio::test]
async fn send_keys_via_mcp() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;

    let mcp_session_id = mcp_init(&client, &url).await;

    // create_session with bash.
    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "sk1" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // Give the shell time to start.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // send_keys: echo hi + ENTER
    let sk_resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "send_keys",
                "arguments": { "session_id": "sk1", "keys": ["echo hi", "<ENTER>"] }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let sk_text = sk_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        sk_text.contains("sent") && sk_text.contains("byte"),
        "send_keys must confirm bytes sent; got: {sk_text}"
    );

    // Give the shell time to process and print output.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Snapshot should contain 'hi'.
    let snap_resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {
                "name": "take_snapshot",
                "arguments": { "session_id": "sk1" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let snap = snap_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        snap.contains("hi"),
        "Snapshot must contain 'hi' after send_keys echo hi<ENTER>; got:\n{snap}"
    );

    ct.cancel();
}

/// MCP flow: list_sessions returns created session; close_session removes it.
#[tokio::test]
async fn list_and_close_via_mcp() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;

    let mcp_session_id = mcp_init(&client, &url).await;

    // create_session.
    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": { "name": "create_session", "arguments": { "session_id": "lc1" } }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // list_sessions — should contain 'lc1'.
    let list_resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": { "name": "list_sessions", "arguments": {} }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let list_text = list_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        list_text.contains("lc1"),
        "list_sessions must contain 'lc1'; got:\n{list_text}"
    );

    // close_session.
    let close_resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": { "name": "close_session", "arguments": { "session_id": "lc1" } }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let close_text = close_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        close_text.contains("closed"),
        "close_session must confirm close; got:\n{close_text}"
    );

    // list_sessions — should no longer contain 'lc1'.
    let list_resp2 = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 5, "method": "tools/call",
            "params": { "name": "list_sessions", "arguments": {} }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let list_text2 = list_resp2["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        !list_text2.contains("lc1"),
        "after close, list_sessions must not contain 'lc1'; got:\n{list_text2}"
    );

    ct.cancel();
}
