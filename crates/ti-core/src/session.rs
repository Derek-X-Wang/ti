//! [`Session`] — one running terminal: a PTY, the Hosted Process inside it,
//! the avt emulation state, and the screen buffer.
//!
//! The unit of lifecycle (create / close) and the unit a Driving Agent or
//! Inspector connects to. See CONTEXT.md for full glossary definitions.

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use anyhow::Context as _;
use avt::Vt;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};

use crate::Snapshot;

/// Default PTY dimensions used when none are specified.
const DEFAULT_COLS: u16 = 80;
const DEFAULT_ROWS: u16 = 24;

/// A slice of raw output from a Session's output history.
///
/// Returned by [`Session::read_output`]. `offset` is the byte offset in the
/// total output stream at which `data` starts — use it as the `since` value for
/// the next `read_output` call to page forward without re-reading old bytes.
///
/// `data` carries the raw bytes from the PTY. Callers decide how to decode them
/// (UTF-8 lossy, lossless, binary) according to their transport constraints.
/// The MCP adapter converts to `String` via `from_utf8_lossy` at the tool-result
/// boundary where that constraint is actually known.
#[derive(Debug, Clone)]
pub struct OutputChunk {
    /// Byte offset of the first byte in `data` within the total output stream.
    ///
    /// When `since >= total_len` (no new data available), `offset` is returned
    /// unchanged as `since` so the caller can distinguish "empty result with
    /// cursor preserved" from an error.
    pub offset: u64,
    /// Raw bytes from the output stream starting at `offset`.
    ///
    /// Empty when `since` is at or beyond the current end of the buffer.
    pub data: Vec<u8>,
}

/// One running terminal Session.
///
/// Owns the PTY, the Hosted Process running inside it, and the avt virtual
/// terminal that processes the output into a queryable screen buffer. Multiple
/// callers may take Snapshots concurrently; the screen buffer is protected by
/// an `Arc<Mutex<Vt>>`.
///
/// Input (keystrokes / raw bytes) is sent through the PTY master writer. The
/// Write Lock lives *above* this layer in [`SessionRegistry`] — `send_input`
/// performs no access-control check; callers are responsible for enforcing that
/// only the Writer may call it.
///
/// All PTY output is also accumulated in `output_buf` (a `Vec<u8>`) for
/// unbounded replay via [`Session::read_output`]. This covers the raw output
/// beyond avt's thin scrollback as described in ADR-0002.
///
/// ## Output retention policy
///
/// All output since session start is retained in memory for the lifetime of the
/// Session. No cap is applied in v1 — suitable for interactive sessions where
/// typical output is measured in kilobytes. Long-running batch jobs producing
/// gigabytes of output could exhaust memory; that is an acceptable v1 trade-off
/// documented here so it is visible at the point of retention.
pub struct Session {
    /// avt virtual terminal — the visible screen buffer.
    vt: Arc<Mutex<Vt>>,
    /// PTY master writer — sends bytes into the Hosted Process's stdin.
    ///
    /// `portable_pty` allows only one writer to be taken per PTY master, so we
    /// hold it for the lifetime of the Session.
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    /// Unbounded raw byte history of all PTY output since session start.
    ///
    /// Appended to by the reader thread; readable by callers via `read_output`.
    /// Protected by an `Arc<Mutex<…>>` so the reader thread and caller can share it.
    output_buf: Arc<Mutex<Vec<u8>>>,
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
    /// **and** accumulated in an unbounded raw-bytes buffer. Call
    /// [`Session::snapshot`] at any point to capture the current visible screen.
    /// Call [`Session::read_output`] to page through the full output history.
    /// Call [`Session::wait`] to block until the Hosted Process exits and all
    /// output has been emulated.
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

        // Take the writer before the reader loop starts. `take_writer` can only
        // be called once per PTY master, so we hold it for the Session lifetime.
        let pty_writer = pair
            .master
            .take_writer()
            .context("failed to take PTY master writer")?;
        let writer = Arc::new(Mutex::new(pty_writer));

        let vt = Arc::new(Mutex::new(Vt::new(cols as usize, rows as usize)));
        let vt_clone = Arc::clone(&vt);

        let output_buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let output_buf_clone = Arc::clone(&output_buf);

        let reader_thread = std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break, // EOF — Hosted Process exited and PTY drained
                    Ok(n) => {
                        let chunk = &buf[..n];
                        // Feed into avt for visible-screen emulation.
                        let text = String::from_utf8_lossy(chunk);
                        if let Ok(mut vt) = vt_clone.lock() {
                            vt.feed_str(&text);
                        }
                        // Accumulate raw bytes for read_output replay.
                        if let Ok(mut out) = output_buf_clone.lock() {
                            out.extend_from_slice(chunk);
                        }
                    }
                    Err(_) => break, // PTY closed unexpectedly
                }
            }
        });

        Ok(Self {
            vt,
            writer,
            output_buf,
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

    /// Send raw bytes into the Hosted Process's stdin via the PTY master writer.
    ///
    /// **Low-level primitive — do not call directly.** Access control (Write Lock
    /// enforcement) is the caller's responsibility. The canonical call site is
    /// [`SessionRegistry::write_input`] in `ti-daemon`, which checks the caller's
    /// Writer identity before dispatching here.
    pub fn send_input(&self, data: &[u8]) -> anyhow::Result<()> {
        let mut w = self
            .writer
            .lock()
            .map_err(|_| anyhow::anyhow!("PTY writer lock poisoned"))?;
        w.write_all(data).context("failed to write to PTY master")?;
        w.flush().context("failed to flush PTY master writer")
    }

    /// Read raw output from the Session's output history starting at `since`.
    ///
    /// `since` is a byte offset into the total output stream. Pass `0` to read
    /// from the beginning, or `chunk.offset + chunk.data.len() as u64` from a
    /// previous call to page forward.
    ///
    /// When `since >= total output length` (no new data), returns an
    /// [`OutputChunk`] with `offset == since` and empty `data` — the caller's
    /// cursor is preserved so "nothing new yet" is unambiguous.
    pub fn read_output(&self, since: u64) -> anyhow::Result<OutputChunk> {
        let buf = self
            .output_buf
            .lock()
            .map_err(|_| anyhow::anyhow!("output buffer lock poisoned"))?;

        let total_len = buf.len() as u64;
        if since >= total_len {
            // No new data yet — return the caller's cursor unchanged so they
            // can tell "empty result" from "invalid offset was clamped."
            return Ok(OutputChunk {
                offset: since,
                data: Vec::new(),
            });
        }

        let data = buf[since as usize..].to_vec();
        Ok(OutputChunk {
            offset: since,
            data,
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

    /// Spawn `echo hello`, wait, and verify read_output returns the raw bytes.
    ///
    /// Verifies the output buffer is populated and that the offset-based paging
    /// API works: reading from 0 gets the full output, reading from the next
    /// offset gets an empty result.
    #[test]
    fn read_output_returns_raw_bytes() {
        let mut session =
            Session::spawn("echo", &["hello"], None, None).expect("failed to spawn Session");

        session.wait().expect("Hosted Process did not exit cleanly");

        // Reading from offset 0 should return the full output.
        let chunk = session.read_output(0).expect("read_output failed");
        assert_eq!(chunk.offset, 0, "first chunk must start at offset 0");
        assert!(
            chunk.data.windows(5).any(|w| w == b"hello"),
            "raw output must contain 'hello'; got: {:?}",
            String::from_utf8_lossy(&chunk.data)
        );

        // Reading from the next offset should return nothing new; the caller's
        // cursor (next_offset) must be preserved in the returned offset.
        let next_offset = chunk.offset + chunk.data.len() as u64;
        let empty_chunk = session
            .read_output(next_offset)
            .expect("read_output at end failed");
        assert_eq!(
            empty_chunk.offset, next_offset,
            "offset must be preserved when no new data; got offset={}",
            empty_chunk.offset
        );
        assert!(
            empty_chunk.data.is_empty(),
            "reading at end must return empty data; got: {:?}",
            String::from_utf8_lossy(&empty_chunk.data)
        );
    }
}
