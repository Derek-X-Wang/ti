//! [`Session`] — one running terminal: a PTY, the Hosted Process inside it,
//! the avt emulation state, and the screen buffer.
//!
//! The unit of lifecycle (create / close) and the unit a Driving Agent or
//! Inspector connects to. See CONTEXT.md for full glossary definitions.

use std::io::Read;
use std::sync::{Arc, Mutex};

use anyhow::Context as _;
use avt::Vt;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};

use crate::Snapshot;

/// Default PTY dimensions used when none are specified.
const DEFAULT_COLS: u16 = 80;
const DEFAULT_ROWS: u16 = 24;

/// One running terminal Session.
///
/// Owns the PTY, the Hosted Process running inside it, and the avt virtual
/// terminal that processes the output into a queryable screen buffer. Multiple
/// callers may take Snapshots concurrently; the screen buffer is protected by
/// an `Arc<Mutex<Vt>>`.
pub struct Session {
    /// avt virtual terminal — the screen buffer.
    vt: Arc<Mutex<Vt>>,
    /// Handle to the Hosted Process (so callers can wait for it).
    child: Box<dyn portable_pty::Child + Send + Sync>,
    /// Background reader thread. Wrapped in `Option` so `wait()` can join it,
    /// which guarantees all PTY output is in the screen buffer before a
    /// post-wait Snapshot is taken — no sleep needed.
    reader_thread: Option<std::thread::JoinHandle<()>>,
}

impl Session {
    /// Spawn a new Session running `program` with `args` in a PTY of the given
    /// dimensions (defaults to 80×24 if `None`).
    ///
    /// Output is read in a background thread and fed into the avt emulator
    /// continuously. Call [`Session::snapshot`] at any point to capture the
    /// current visible screen. Call [`Session::wait`] to block until the
    /// Hosted Process exits and all output has been emulated.
    pub fn spawn(
        program: &str,
        args: &[&str],
        cols: Option<u16>,
        rows: Option<u16>,
    ) -> anyhow::Result<Self> {
        let cols = cols.unwrap_or(DEFAULT_COLS);
        let rows = rows.unwrap_or(DEFAULT_ROWS);

        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .context("failed to open PTY pair")?;

        let mut cmd = CommandBuilder::new(program);
        for arg in args {
            cmd.arg(arg);
        }
        // Set TERM so the Hosted Process knows what sequences to emit.
        cmd.env("TERM", "xterm-256color");

        let child = pair
            .slave
            .spawn_command(cmd)
            .context("failed to spawn Hosted Process")?;

        // Drop slave after spawning so EOF propagates to the master reader when
        // the Hosted Process exits.
        drop(pair.slave);

        let mut reader = pair
            .master
            .try_clone_reader()
            .context("failed to clone PTY master reader")?;

        let vt = Arc::new(Mutex::new(Vt::new(cols as usize, rows as usize)));
        let vt_clone = Arc::clone(&vt);

        let reader_thread = std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break, // EOF — Hosted Process exited and PTY drained
                    Ok(n) => {
                        let text = String::from_utf8_lossy(&buf[..n]);
                        if let Ok(mut vt) = vt_clone.lock() {
                            vt.feed_str(&text);
                        }
                    }
                    Err(_) => break, // PTY closed unexpectedly
                }
            }
        });

        Ok(Self {
            vt,
            child,
            reader_thread: Some(reader_thread),
        })
    }

    /// Capture a text Snapshot of the current visible screen.
    ///
    /// Locks the screen buffer briefly to extract line text and cursor position,
    /// then releases it immediately. Safe to call at any time, including while
    /// the Hosted Process is still running.
    pub fn snapshot(&self) -> anyhow::Result<Snapshot> {
        let vt = self
            .vt
            .lock()
            .map_err(|_| anyhow::anyhow!("screen buffer lock poisoned"))?;

        let lines: Vec<String> = vt.view().map(|line| line.text()).collect();
        let cursor = vt.cursor();

        Ok(Snapshot {
            lines,
            cursor_col: cursor.col,
            cursor_row: cursor.row,
            cursor_visible: cursor.visible,
        })
    }

    /// Wait for the Hosted Process to exit and for all PTY output to be emulated.
    ///
    /// Blocks until:
    /// 1. The Hosted Process exits (child wait).
    /// 2. The background reader thread drains any remaining PTY bytes and exits.
    ///
    /// After this returns, [`Session::snapshot`] reflects the complete final
    /// screen state with no timing dependency.
    pub fn wait(&mut self) -> anyhow::Result<portable_pty::ExitStatus> {
        let status = self
            .child
            .wait()
            .context("failed to wait for Hosted Process")?;

        // Join the reader thread so we know all PTY output has been fed into avt
        // before the caller takes a Snapshot. This eliminates any need for a
        // sleep-based drain in callers.
        if let Some(handle) = self.reader_thread.take() {
            // Ignore join errors — if the thread panicked, the buffer is already
            // in a partial state and the snapshot will reflect that.
            let _ = handle.join();
        }

        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spawn `echo hello`, wait for it to exit (draining all PTY output), take a
    /// Snapshot, and assert the visible screen contains "hello".
    ///
    /// This is the tracer-bullet test described in issue #1: it exercises the
    /// full path from Hosted Process spawn → PTY output → avt emulation →
    /// text Snapshot, all without any daemon or MCP layer.
    #[test]
    fn snapshot_contains_echo_output() {
        let mut session =
            Session::spawn("echo", &["hello"], None, None).expect("failed to spawn Session");

        // Wait for `echo` to exit and for the reader thread to drain all PTY
        // output into the avt screen buffer.
        session.wait().expect("Hosted Process did not exit cleanly");

        let snap = session.snapshot().expect("failed to take Snapshot");
        assert!(
            snap.contains("hello"),
            "Snapshot should contain 'hello', got:\n{}",
            snap.text()
        );
    }
}
