//! [`ObserverSocketServer`] — local Unix socket channel for Observer clients.
//!
//! A non-MCP attachment point that lets clients (e.g. the native Inspector)
//! subscribe to a Session's rendered screen stream without going through the MCP
//! protocol. The server listens on a Unix domain socket; clients send a single
//! newline-terminated session id to attach, then receive a stream of
//! newline-delimited JSON [`ScreenUpdate`] frames.
//!
//! ## Protocol
//!
//! 1. Client connects.
//! 2. Client sends: `<session_id>\n`
//! 3. Server sends the current screen as the first [`ScreenUpdate`] frame.
//! 4. Server streams subsequent frames as the Session produces output.
//! 5. Connection closes when the Session ends or the client disconnects.
//!
//! ## Backpressure
//!
//! Each Observer has a bounded internal channel (capacity
//! [`ti_core::OBSERVER_CHANNEL_CAPACITY`]). When the channel is full, new
//! [`ScreenUpdate`]s are **dropped** — the Observer may miss frames but the
//! PTY reader, emulator, Snapshots, and MCP tools are never blocked.
//!
//! ## Concurrency
//!
//! The server accepts connections in a tokio task per client. The
//! [`crate::registry::SessionRegistry`] is shared with the MCP listener and all
//! other daemon components via `Arc<Mutex<…>>` — no separate lock is needed here.

use std::path::Path;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::sync::CancellationToken;

use crate::registry::SessionRegistry;
use ti_core::ScreenUpdate;

/// The observer socket server.
///
/// Binds a Unix domain socket and accepts Observer connections. Each connection
/// gets its own tokio task; the server task accepts in a loop until `ct` is cancelled.
pub struct ObserverSocketServer {
    listener: UnixListener,
    registry: SessionRegistry,
}

impl ObserverSocketServer {
    /// Bind the observer socket at `path` backed by `registry`.
    ///
    /// Removes any stale socket file at `path` before binding so the server can
    /// restart cleanly after a crash.
    pub async fn bind(path: &Path, registry: SessionRegistry) -> anyhow::Result<Self> {
        // Clean up stale socket from a prior run.
        let _ = tokio::fs::remove_file(path).await;
        let listener = UnixListener::bind(path)
            .map_err(|e| anyhow::anyhow!("failed to bind observer socket at {path:?}: {e}"))?;
        tracing::info!("ti-daemon Observer socket at {:?}", path);
        Ok(Self { listener, registry })
    }

    /// Accept connections in a loop until `ct` is cancelled.
    ///
    /// Each accepted connection is handled in a dedicated tokio task so one slow
    /// client does not block others.
    pub async fn run(self, ct: CancellationToken) {
        loop {
            tokio::select! {
                _ = ct.cancelled() => break,
                accept = self.listener.accept() => {
                    match accept {
                        Ok((stream, _addr)) => {
                            let registry = self.registry.clone();
                            let ct_child = ct.child_token();
                            tokio::spawn(async move {
                                if let Err(e) = handle_observer(stream, registry, ct_child).await {
                                    tracing::debug!("observer connection ended: {e}");
                                }
                            });
                        }
                        Err(e) => {
                            tracing::warn!("observer socket accept error: {e}");
                        }
                    }
                }
            }
        }
    }
}

/// Handle one Observer connection.
///
/// Reads the session id, subscribes atomically with an initial snapshot, sends
/// the initial frame, then streams subsequent [`ScreenUpdate`]s until the client
/// disconnects, the Session ends, or `ct` is cancelled.
///
/// ## Initial frame ordering
///
/// Subscription and the initial snapshot are taken atomically under the vt lock
/// via [`SessionRegistry::subscribe_observer_with_snapshot`]. This prevents the
/// "time goes backwards" race where chunks processed between `subscribe` and
/// `snapshot` would be queued before the initial frame but reflect an earlier
/// screen state than the initial frame.
///
/// ## Cleanup on disconnect
///
/// The observer is explicitly unsubscribed on every exit path. Without this, a
/// disconnected-but-idle client would leak one OS thread (bridge) and one
/// `SyncSender` in the session's observer list indefinitely — the thread blocks
/// in `handle.recv()` and the session has no way to detect the disconnection
/// without a new PTY chunk triggering `try_send → Disconnected`.
async fn handle_observer(
    mut stream: UnixStream,
    registry: SessionRegistry,
    ct: CancellationToken,
) -> anyhow::Result<()> {
    // Read the session id — a single newline-terminated UTF-8 string.
    let (read_half, mut write_half) = stream.split();
    let mut reader = BufReader::new(read_half);
    let mut session_id = String::new();
    reader.read_line(&mut session_id).await?;
    let session_id = session_id.trim().to_string();
    if session_id.is_empty() {
        anyhow::bail!("observer sent empty session id");
    }

    // Subscribe and capture the initial snapshot atomically under the vt lock.
    // Any PTY chunk processed after this point will be queued on `handle` and
    // will reflect a screen state >= `initial`, so no time-reversal is possible.
    let (handle, initial) = registry
        .subscribe_observer_with_snapshot(&session_id)
        .map_err(|e| anyhow::anyhow!("subscribe_observer_with_snapshot({session_id:?}): {e}"))?;

    // Capture the observer id before moving the handle into the bridge thread —
    // we need it to unsubscribe on every exit path.
    let observer_id = handle.observer_id();

    // Send the initial frame.
    send_update(&mut write_half, &initial).await?;

    // Bridge the std::mpsc receiver to the async writer via a dedicated OS thread.
    // `std::thread::spawn` is appropriate for this long-lived blocking loop;
    // `tokio::task::spawn_blocking` is intended for short-lived tasks and would
    // deplete the blocking thread pool if held for the session lifetime.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<std::sync::Arc<ScreenUpdate>>(256);
    std::thread::spawn(move || {
        for update in handle.recv() {
            if tx.blocking_send(update).is_err() {
                break; // writer task dropped the receiver — stop iterating
            }
        }
    });

    // Stream subsequent ScreenUpdates from the bridge channel.
    let result = loop {
        tokio::select! {
            _ = ct.cancelled() => break Ok(()),
            maybe_update = rx.recv() => {
                match maybe_update {
                    Some(update) => {
                        if let Err(e) = send_update(&mut write_half, &update).await {
                            break Err(e);
                        }
                    }
                    None => break Ok(()), // bridge thread ended (session ended)
                }
            }
        }
    };

    // Drop the bridge receiver — signals the bridge thread to stop blocking_send.
    // The thread will exit when handle.recv() returns None (session ended or
    // unsubscribed) or when it observes the broken tx on next blocking_send.
    drop(rx);

    // Unsubscribe on every exit path. Without this, an idle session would keep the
    // SyncSender alive indefinitely; a busy session would only detect the dead
    // observer on the next try_send (Disconnected), which may be arbitrarily late.
    // Ignore "not found" — the reader thread may have already pruned it via
    // try_send → Disconnected if PTY output arrived after we dropped rx above.
    let _ = registry.unsubscribe_observer(&session_id, observer_id);

    result
}

/// Serialize a [`ScreenUpdate`] as newline-delimited JSON and write it to the socket.
///
/// `ScreenUpdate` is a type alias for [`StyledSnapshot`], which derives `Serialize`,
/// so we delegate directly to `serde_json::to_string` rather than re-enumerating fields.
async fn send_update(
    writer: &mut tokio::net::unix::WriteHalf<'_>,
    update: &ScreenUpdate,
) -> anyhow::Result<()> {
    let json = serde_json::to_string(update)?;
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    Ok(())
}
