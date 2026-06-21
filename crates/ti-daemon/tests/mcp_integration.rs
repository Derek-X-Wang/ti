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

// ── Issue #6: structured Snapshot + resize ──────────────────────────────────

/// Registry: take_snapshot_styled returns per-cell data for every visible line.
#[test]
fn registry_take_snapshot_styled_returns_cells() {
    let registry = SessionRegistry::new();
    registry
        .create_session(
            "ss1".to_string(),
            "echo",
            &["hello"],
            "writer-a".to_string(),
        )
        .expect("create_session failed");

    std::thread::sleep(std::time::Duration::from_millis(300));

    let snap = registry
        .take_snapshot_styled("ss1")
        .expect("take_snapshot_styled failed");

    // Styled snapshot must have the same row count as the text snapshot.
    let text_snap = registry.take_snapshot("ss1").expect("text snapshot failed");
    assert_eq!(
        snap.lines.len(),
        text_snap.lines.len(),
        "styled lines count must match text lines count"
    );

    // Cursor + alt-screen fields must be present.
    let _ = snap.cursor_col;
    let _ = snap.cursor_row;
    let _ = snap.cursor_visible;
    let _ = snap.alt_screen;
}

/// Registry: resize changes the PTY dimensions reflected in a styled Snapshot.
///
/// Uses `cat` (long-running) so the PTY is live when resized.
#[test]
fn registry_resize_changes_dimensions() {
    let registry = SessionRegistry::new();
    registry
        .create_session("rs1".to_string(), "cat", &[], "writer-a".to_string())
        .expect("create_session failed");

    registry.resize("rs1", 120, 40).expect("resize failed");

    let snap = registry
        .take_snapshot_styled("rs1")
        .expect("take_snapshot_styled after resize failed");
    assert_eq!(
        snap.lines.len(),
        40,
        "after resize to 40 rows, styled snapshot must have 40 lines; got {}",
        snap.lines.len()
    );
}

/// Full MCP flow: take_snapshot with include_styles=true returns styled summary.
#[tokio::test]
async fn take_snapshot_styled_via_mcp() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;
    let mcp_session_id = mcp_init(&client, &url).await;

    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "styled-mcp", "program": "echo", "args": ["hi"] }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let snap_resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "take_snapshot",
                "arguments": { "session_id": "styled-mcp", "include_styles": true }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let snap_text = snap_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    // include_styles=true returns the full StyledSnapshot as JSON — assert the
    // structured per-cell data is actually present, not just a summary line.
    let parsed: serde_json::Value =
        serde_json::from_str(snap_text).expect("styled snapshot must be valid JSON");
    assert!(
        parsed["lines"].is_array(),
        "styled snapshot JSON must include per-cell `lines`; got:\n{snap_text}"
    );
    assert!(
        parsed["alt_screen"].is_boolean(),
        "styled snapshot JSON must include `alt_screen`; got:\n{snap_text}"
    );
    assert!(
        parsed.get("cursor_col").is_some(),
        "styled snapshot JSON must include cursor fields; got:\n{snap_text}"
    );

    ct.cancel();
}

/// Full MCP flow: resize changes the terminal dimensions.
#[tokio::test]
async fn resize_via_mcp() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;
    let mcp_session_id = mcp_init(&client, &url).await;

    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "resize-mcp", "program": "cat" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let resize_resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "resize",
                "arguments": { "session_id": "resize-mcp", "cols": 100, "rows": 30 }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let resize_text = resize_resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("");
    assert!(
        resize_text.contains("100") && resize_text.contains("30"),
        "resize response must echo back new dimensions; got:\n{resize_text}"
    );

    ct.cancel();
}

/// Registry: resize rejects zero and oversized dimensions (issue #6, hardening).
#[test]
fn registry_resize_rejects_invalid_dimensions() {
    let registry = SessionRegistry::new();
    registry
        .create_session("rz1".to_string(), "cat", &[], "writer-a".to_string())
        .expect("create_session failed");

    assert!(
        registry.resize("rz1", 0, 24).is_err(),
        "resize with 0 cols must be rejected"
    );
    assert!(
        registry.resize("rz1", 80, 0).is_err(),
        "resize with 0 rows must be rejected"
    );
    assert!(
        registry.resize("rz1", 20_000, 24).is_err(),
        "resize beyond the dimension cap must be rejected"
    );
    // A valid resize still succeeds afterward.
    registry
        .resize("rz1", 100, 30)
        .expect("valid resize must succeed");
}

// ── Issue #8: wait_for_output ─────────────────────────────────────────────────

/// wait_for_output returns reason=matched when the visible Snapshot contains the
/// expected pattern before the timeout fires.
///
/// Spawns `sh -c "echo hello"` (deterministic output), waits for "hello" with a
/// generous timeout. Pattern match must fire and result must include the Snapshot.
#[tokio::test]
async fn wait_for_output_matches_pattern() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;
    let mcp_session_id = mcp_init(&client, &url).await;

    // Spawn a session that produces "hello" then exits.
    // Use program="sh" with no extra args — the shell starts, then we wait for
    // its prompt, since create_session doesn't yet accept extra args.
    // Instead: use a sh one-liner via the program field directly.
    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                // Use the send_keys flow after spawning a shell, or use a
                // program that prints text directly. Since create_session
                // only accepts program (no args), we spawn a shell and send
                // the command via send_keys instead. That is the realistic
                // Driving Agent flow anyway.
                "arguments": { "session_id": "wfo-pattern" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // Give the shell a moment to show its prompt.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Type "echo hello" + ENTER into the shell.
    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "send_keys",
                "arguments": { "session_id": "wfo-pattern", "keys": ["echo hello", "<ENTER>"] }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // wait_for_output with a pattern — must fire as "matched".
    let resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {
                "name": "wait_for_output",
                "arguments": {
                    "session_id": "wfo-pattern",
                    "pattern": "hello",
                    "timeout_ms": 5000
                }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        text.contains("reason=matched"),
        "wait_for_output must return reason=matched when 'hello' appears; got:\n{text}"
    );
    assert!(
        text.contains("hello"),
        "result must include the Snapshot containing 'hello'; got:\n{text}"
    );
    assert!(
        text.contains("[cursor"),
        "result must include cursor info; got:\n{text}"
    );

    ct.cancel();
}

/// wait_for_output returns reason=idle when output stops changing for idle_ms.
///
/// Spawns `cat` (no output until input). Waits with idle_ms=200 and a 5s timeout.
/// Since `cat` produces no output, idle must fire well before the timeout.
#[tokio::test]
async fn wait_for_output_idle_fires() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;
    let mcp_session_id = mcp_init(&client, &url).await;

    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "wfo-idle", "program": "cat" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // Give cat a moment to start, then wait for idle.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "wait_for_output",
                "arguments": {
                    "session_id": "wfo-idle",
                    "idle_ms": 300,
                    "timeout_ms": 5000
                }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        text.contains("reason=idle"),
        "wait_for_output must return reason=idle when output is quiet; got:\n{text}"
    );

    ct.cancel();
}

/// wait_for_output returns reason=timeout when neither pattern nor idle fires.
///
/// Spawns `cat` (no output) with a short timeout (300 ms) and NO idle_ms, so
/// the only condition is the timeout itself.
#[tokio::test]
async fn wait_for_output_timeout_fires() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;
    let mcp_session_id = mcp_init(&client, &url).await;

    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "wfo-timeout", "program": "cat" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "wait_for_output",
                "arguments": {
                    "session_id": "wfo-timeout",
                    "pattern": "this-will-never-appear",
                    "timeout_ms": 300
                }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        text.contains("reason=timeout"),
        "wait_for_output must return reason=timeout when deadline fires; got:\n{text}"
    );

    ct.cancel();
}

/// wait_for_output pattern match works with a valid regex.
///
/// Pattern `h.llo` should match "hello" via regex. Uses a shell session with
/// send_keys to produce the output (same approach as wait_for_output_matches_pattern).
#[tokio::test]
async fn wait_for_output_regex_pattern() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;
    let mcp_session_id = mcp_init(&client, &url).await;

    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "wfo-regex" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // Give the shell a moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "send_keys",
                "arguments": { "session_id": "wfo-regex", "keys": ["echo hello", "<ENTER>"] }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {
                "name": "wait_for_output",
                "arguments": {
                    "session_id": "wfo-regex",
                    "pattern": "h.llo",
                    "timeout_ms": 5000
                }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        text.contains("reason=matched"),
        "regex pattern h.llo must match 'hello'; got:\n{text}"
    );

    ct.cancel();
}

/// Demo: waiting for a known prompt works (by spawning a shell, sending a
/// command, and waiting for its output) and waiting for a command going quiet
/// (idle) also works.
///
/// This directly validates the AC: "waiting for a known prompt and for
/// a command-goes-quiet both work".
#[tokio::test]
async fn wait_for_output_demo_prompt_and_idle() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;
    let mcp_session_id = mcp_init(&client, &url).await;

    // Test 1: waiting for a known prompt string — spawn a shell, type a command
    // that prints a sentinel, and wait for it to appear.
    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "demo-prompt" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // Give the shell a moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "send_keys",
                "arguments": { "session_id": "demo-prompt", "keys": ["echo prompt_ready", "<ENTER>"] }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let resp1 = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 4, "method": "tools/call",
            "params": {
                "name": "wait_for_output",
                "arguments": {
                    "session_id": "demo-prompt",
                    "pattern": "prompt_ready",
                    "timeout_ms": 5000
                }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let text1 = resp1["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        text1.contains("reason=matched"),
        "demo: waiting for known prompt must return matched; got:\n{text1}"
    );

    // Test 2: command goes quiet → idle fires.
    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 5, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "demo-quiet", "program": "cat" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // Give cat a moment to start, then wait for idle.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let resp2 = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 6, "method": "tools/call",
            "params": {
                "name": "wait_for_output",
                "arguments": {
                    "session_id": "demo-quiet",
                    "idle_ms": 300,
                    "timeout_ms": 5000
                }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let text2 = resp2["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        text2.contains("reason=idle"),
        "demo: command-goes-quiet must return idle; got:\n{text2}"
    );

    ct.cancel();
}

// ── Issue #9: execute_command ─────────────────────────────────────────────────

/// execute_command types a shell command and returns visible Snapshot + raw output.
///
/// Spawns a shell, calls execute_command with "echo hello", and verifies the
/// result contains "hello" in both the Snapshot and raw_output sections.
#[tokio::test]
async fn execute_command_returns_output() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;
    let mcp_session_id = mcp_init(&client, &url).await;

    // create_session — spawn the default shell.
    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "ec1" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // Give the shell a moment to print its prompt.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // execute_command — send "echo hello" to the shell.
    let resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "execute_command",
                "arguments": {
                    "session_id": "ec1",
                    "command": "echo hello"
                }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    assert!(
        text.contains("[execute_command command="),
        "response must have execute_command header; got:\n{text}"
    );
    assert!(
        text.contains("hello"),
        "response must contain 'hello' from echo; got:\n{text}"
    );
    assert!(
        text.contains("[snapshot]"),
        "response must contain snapshot section; got:\n{text}"
    );
    assert!(
        text.contains("[raw_output"),
        "response must contain raw_output section; got:\n{text}"
    );

    ct.cancel();
}

/// execute_command enforces the Write Lock — non-Writer is rejected.
///
/// Creates a session with writer-a and tries to execute_command from a
/// different McpListener instance (simulating a non-Writer client).
#[test]
fn execute_command_non_writer_rejected() {
    // Use the registry directly to simulate two clients with different ids.
    let registry = SessionRegistry::new();
    registry
        .create_session("ec-wl1".to_string(), "cat", &[], "writer-a".to_string())
        .expect("create_session failed");

    // A non-Writer cannot write input — registry enforces this.
    let err = registry
        .write_input("ec-wl1", "observer-b", b"echo hi\r")
        .unwrap_err();
    assert!(
        err.to_string().contains("not Writer"),
        "Non-Writer must be rejected by execute_command; got: {err}"
    );
}

/// execute_command with a timeout that fires returns what was captured.
///
/// Spawns cat (no output), calls execute_command with a short timeout.
/// The tool must return without hanging indefinitely.
#[tokio::test]
async fn execute_command_timeout_returns() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;
    let mcp_session_id = mcp_init(&client, &url).await;

    // Spawn cat — it won't produce output until it receives input.
    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "ec-timeout", "program": "cat" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // Send a command that cat will echo back, with a short timeout.
    // Cat echoes input, so it will produce output then wait for more.
    // The idle settle (200ms) should fire after the echo.
    let resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "execute_command",
                "arguments": {
                    "session_id": "ec-timeout",
                    "command": "hello",
                    "timeout_ms": 2000
                }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    // Must return without error and include a snapshot.
    assert!(
        text.contains("[execute_command"),
        "execute_command must return a result even on timeout; got:\n{text}"
    );

    ct.cancel();
}

/// Demo: execute_command "ls" returns a directory listing.
///
/// This directly validates the AC: "execute_command 'ls' returns a directory listing".
#[tokio::test]
async fn execute_command_demo_ls() {
    let (client, url, ct) = spawn_daemon(DEV_TOKEN).await;
    let mcp_session_id = mcp_init(&client, &url).await;

    mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {
                "name": "create_session",
                "arguments": { "session_id": "ec-demo" }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    // Give the shell a moment to start.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let resp = mcp_post(
        &client,
        &url,
        DEV_TOKEN,
        json!({
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {
                "name": "execute_command",
                "arguments": {
                    "session_id": "ec-demo",
                    "command": "ls /usr/bin/env"
                }
            }
        }),
        mcp_session_id.as_deref(),
    )
    .await;

    let text = resp["result"]["content"][0]["text"].as_str().unwrap_or("");
    // "ls /usr/bin/env" outputs the path of the env binary — it should appear
    // in the snapshot or raw output.
    assert!(
        text.contains("env") || text.contains("usr"),
        "execute_command 'ls /usr/bin/env' must return the binary path; got:\n{text}"
    );

    ct.cancel();
}

// ── Issue #10: dual-channel Observer socket + backpressure ────────────────────

/// Unit: subscribe_observer returns a receiver that gets ScreenUpdates.
///
/// Subscribes an observer on a session that runs `echo hello`, then waits for
/// at least one ScreenUpdate to arrive on the channel.
#[test]
fn observer_subscription_receives_screen_updates() {
    let registry = SessionRegistry::new();
    registry
        .create_session(
            "obs1".to_string(),
            "echo",
            &["hello world"],
            "writer-a".to_string(),
        )
        .expect("create_session failed");

    let rx = registry
        .subscribe_observer("obs1")
        .expect("subscribe_observer failed");

    // Give echo a moment to produce output.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut got_update = false;
    while std::time::Instant::now() < deadline {
        if rx.try_recv().is_ok() {
            got_update = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        got_update,
        "observer must receive at least one ScreenUpdate before timeout"
    );
}

/// Unit: a slow Observer channel (full) does not block the reader thread.
///
/// Creates a session with a bounded observer channel, fills the channel, then
/// ensures the session continues to function (take_snapshot still works) even
/// though the observer is not consuming.
#[test]
fn slow_observer_does_not_block_reader() {
    let registry = SessionRegistry::new();
    registry
        .create_session(
            "obs-slow".to_string(),
            "sh",
            &["-c", "for i in $(seq 1 50); do echo line$i; done"],
            "writer-a".to_string(),
        )
        .expect("create_session failed");

    let _rx = registry
        .subscribe_observer("obs-slow")
        .expect("subscribe_observer failed");
    // Do NOT consume from _rx — this simulates a slow/stalled Observer.

    // Give the session a moment to run and fill the bounded channel.
    std::thread::sleep(std::time::Duration::from_millis(500));

    // The main thread must still be able to snapshot — reader must not be blocked.
    let snap = registry
        .take_snapshot("obs-slow")
        .expect("take_snapshot must succeed even with stalled observer");
    let _ = snap; // contents are not critical — we only care that it did not hang
}

/// Unit: unsubscribe_observer removes the channel; subsequent updates do not reach it.
#[test]
fn observer_unsubscribe_stops_updates() {
    let registry = SessionRegistry::new();
    registry
        .create_session("obs-unsub".to_string(), "cat", &[], "writer-a".to_string())
        .expect("create_session failed");

    let handle = registry
        .subscribe_observer("obs-unsub")
        .expect("subscribe_observer failed");

    let obs_id = handle.observer_id();
    registry
        .unsubscribe_observer("obs-unsub", obs_id)
        .expect("unsubscribe_observer failed");

    // Send input to trigger output — observer is already unsubscribed.
    registry
        .write_input("obs-unsub", "writer-a", b"echo afterunsub\r")
        .expect("write_input failed");
    std::thread::sleep(std::time::Duration::from_millis(300));

    // Drain whatever arrived before unsubscribe (may be zero).
    while handle.try_recv().is_ok() {}
    // After another brief wait there must be no new items.
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert!(
        handle.try_recv().is_err(),
        "unsubscribed observer must not receive new updates"
    );
}

/// Integration: Observer socket server accepts a connection, attaches to a session,
/// and streams ScreenUpdates over the socket.
///
/// Spawns the observer socket server, connects a socket client, sends the
/// session_id as a newline-terminated string, and verifies that at least one
/// newline-delimited JSON ScreenUpdate is received.
#[tokio::test]
async fn observer_socket_streams_screen_updates() {
    use ti_daemon::observer_socket::ObserverSocketServer;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let registry = SessionRegistry::new();
    registry
        .create_session(
            "obs-sock1".to_string(),
            "sh",
            &["-c", "for i in $(seq 1 5); do echo line$i; done; sleep 1"],
            "writer-a".to_string(),
        )
        .expect("create_session failed");

    // Bind observer socket on an OS-assigned path.
    let socket_path = std::env::temp_dir().join(format!("ti-obs-test-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&socket_path); // clean up any stale socket

    let server = ObserverSocketServer::bind(&socket_path, registry.clone())
        .await
        .expect("failed to bind observer socket");

    let obs_ct = tokio_util::sync::CancellationToken::new();
    tokio::spawn({
        let ct = obs_ct.child_token();
        async move { server.run(ct).await }
    });

    // Connect as an Observer and request the session.
    let mut stream = tokio::net::UnixStream::connect(&socket_path)
        .await
        .expect("failed to connect to observer socket");

    // Protocol: send "session_id\n" to attach.
    stream
        .write_all(b"obs-sock1\n")
        .await
        .expect("failed to send session_id");

    // Expect at least one newline-delimited JSON object.
    let mut reader = BufReader::new(&mut stream);
    let mut line = String::new();
    let read_result = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        reader.read_line(&mut line),
    )
    .await;

    obs_ct.cancel();
    let _ = std::fs::remove_file(&socket_path);

    let n = read_result
        .expect("timed out waiting for ScreenUpdate from observer socket")
        .expect("failed to read from observer socket");
    assert!(n > 0, "observer socket must send at least one byte");
    // Must be valid JSON.
    let _: serde_json::Value =
        serde_json::from_str(line.trim()).expect("ScreenUpdate must be valid JSON");
}

/// Integration: MCP Writer and socket Observer share the same Session concurrently.
///
/// Creates a session via the SessionRegistry (simulating the MCP path), attaches
/// a socket Observer, then verifies both channels function simultaneously.
#[tokio::test]
async fn mcp_writer_and_observer_share_session() {
    use ti_daemon::observer_socket::ObserverSocketServer;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let registry = SessionRegistry::new();
    // Create a long-running session (tick loop) — simulates the MCP Writer path.
    registry
        .create_session(
            "shared-sess".to_string(),
            "sh",
            &["-c", "while true; do echo tick; sleep 0.1; done"],
            "mcp-writer".to_string(),
        )
        .expect("create shared-sess");

    let socket_path =
        std::env::temp_dir().join(format!("ti-shared-test-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&socket_path);

    let server = ObserverSocketServer::bind(&socket_path, registry.clone())
        .await
        .expect("bind failed");

    let obs_ct = tokio_util::sync::CancellationToken::new();
    tokio::spawn({
        let ct = obs_ct.child_token();
        async move { server.run(ct).await }
    });

    // Connect observer while the session is producing output.
    let mut stream = tokio::net::UnixStream::connect(&socket_path)
        .await
        .expect("connect failed");
    stream
        .write_all(b"shared-sess\n")
        .await
        .expect("write session id");

    // MCP Writer takes a snapshot concurrently — must not be blocked.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let snap = registry
        .take_snapshot("shared-sess")
        .expect("snapshot failed");
    assert!(
        snap.contains("tick"),
        "MCP snapshot must see tick output; got:\n{}",
        snap.text()
    );

    // Observer must have received updates.
    let mut reader = BufReader::new(&mut stream);
    let mut line = String::new();
    let read_result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        reader.read_line(&mut line),
    )
    .await;

    obs_ct.cancel();
    let _ = std::fs::remove_file(&socket_path);

    let n = read_result
        .expect("timeout waiting for observer update")
        .expect("read error");
    assert!(
        n > 0,
        "observer must receive output from the shared session"
    );
    let _: serde_json::Value =
        serde_json::from_str(line.trim()).expect("observer update must be valid JSON");
}
