//! [`SessionRegistry`] — the daemon's in-memory store of live Sessions.
//!
//! The TI Daemon is the single source of truth for every Session (see ADR-0001
//! and CONTEXT.md). This module holds the map from stable session IDs to running
//! [`ti_core::Session`] instances plus the Write Lock metadata for each Session.
//!
//! ## Write Lock
//!
//! Every Session has exactly one **Writer** (the client that created it) and
//! zero or more **Observers**. Only the Writer may send input; Observers may
//! take Snapshots and read output. The lock is checked here — in the daemon
//! layer — so it applies uniformly regardless of the client transport (MCP,
//! socket, etc.). See CONTEXT.md "Write Lock" and ADR-0004.
//!
//! ## Error vocabulary
//!
//! Canonical error message prefixes for callers to match:
//!
//! | Situation                   | Error message prefix             |
//! |-----------------------------|----------------------------------|
//! | Unknown session id          | `"no Session with id '…'"`       |
//! | Duplicate session id        | `"Session id '…' already exists"` |
//! | Caller is not the Writer    | `"not Writer for session '…'"`   |
//! | Session has exited          | `"session exited"`               |

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::Context as _;
use ti_core::{ExitStatus, OutputChunk, Session, Snapshot, StyledSnapshot};

/// Holds a [`Session`] together with its Write Lock identity and metadata.
struct SessionEntry {
    session: Session,
    /// The client id of the Writer — the only client allowed to send input.
    writer_id: String,
    /// The program name used when spawning the Hosted Process. Informational.
    command: String,
}

/// A summary of a single Session returned by [`SessionRegistry::list_sessions`].
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// The stable session id.
    pub id: String,
    /// The program name of the Hosted Process (e.g. `"bash"`).
    pub command: String,
    /// PTY width in columns.
    pub cols: u16,
    /// PTY height in rows.
    pub rows: u16,
    /// `None` if the Hosted Process is still running; `Some(status)` if it has exited.
    pub exit_status: Option<ExitStatus>,
}

/// A thread-safe registry of live Sessions keyed by a stable string ID.
///
/// Cloning the registry is cheap — it shares the same underlying `Arc<Mutex<…>>`.
///
/// ## Lock ordering
///
/// The registry holds an outer map lock (`inner`) and each [`ti_core::Session`]
/// holds inner per-field locks (e.g. `child`). Always acquire the outer map lock
/// before calling any `Session` method that acquires an inner lock. Never acquire
/// a `Session` inner lock first and then try to acquire the map lock — that would
/// invert the ordering and risk deadlock.
#[derive(Clone)]
pub struct SessionRegistry {
    inner: Arc<Mutex<HashMap<String, SessionEntry>>>,
}

impl SessionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Acquire the inner map lock, mapping poison into an `anyhow::Error`.
    fn lock(&self) -> anyhow::Result<MutexGuard<'_, HashMap<String, SessionEntry>>> {
        self.inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry lock poisoned"))
    }

    /// Look up an entry by session id, returning a well-typed error on miss.
    fn get_entry<'m>(
        map: &'m HashMap<String, SessionEntry>,
        id: &str,
    ) -> anyhow::Result<&'m SessionEntry> {
        map.get(id)
            .ok_or_else(|| anyhow::anyhow!("no Session with id '{id}'"))
    }

    /// Spawn a new Session running `program` with `args`, register it under
    /// `id`, and assign `writer_id` as its Writer.
    ///
    /// Returns the session id on success. Returns an error if a Session with
    /// this id already exists, or if spawning the Hosted Process fails.
    pub fn create_session(
        &self,
        id: String,
        program: &str,
        args: &[&str],
        writer_id: String,
    ) -> anyhow::Result<String> {
        let mut map = self.lock()?;

        if map.contains_key(&id) {
            anyhow::bail!("Session id '{id}' already exists");
        }

        let session =
            Session::spawn(program, args, None, None).context("failed to spawn Session")?;

        map.insert(
            id.clone(),
            SessionEntry {
                session,
                writer_id,
                command: program.to_string(),
            },
        );
        Ok(id)
    }

    /// Send raw bytes into the Hosted Process via the Write Lock.
    ///
    /// Only the Writer (the client that created the Session) may call this.
    /// Returns a `"not Writer"` error if `caller_id` does not match the stored
    /// `writer_id` for the Session, or a `"session exited"` error if the
    /// Hosted Process has already terminated.
    pub fn write_input(
        &self,
        session_id: &str,
        caller_id: &str,
        data: &[u8],
    ) -> anyhow::Result<()> {
        let map = self.lock()?;
        let entry = Self::get_entry(&map, session_id)?;

        if entry.writer_id != caller_id {
            anyhow::bail!("not Writer for session '{session_id}'");
        }

        // Refuse to write to a dead process — the bytes would be silently dropped
        // by the PTY and the caller would get no feedback.
        if let Some(status) = entry.session.try_exit_status()? {
            anyhow::bail!("session exited (status: {status})");
        }

        entry.session.send_input(data)
    }

    /// Take a text Snapshot of the Session identified by `id`.
    ///
    /// Available to any caller — Writers and Observers alike. Returns the last
    /// visible screen state whether the process is running or has exited. Returns
    /// an error if no Session with this id exists or if the Snapshot fails.
    pub fn take_snapshot(&self, id: &str) -> anyhow::Result<Snapshot> {
        let map = self.lock()?;
        let entry = Self::get_entry(&map, id)?;
        entry.session.snapshot()
    }

    /// Take a structured Snapshot of the Session including per-cell style data.
    ///
    /// Available to any caller — Writers and Observers alike. Returns an error
    /// if no Session with this id exists or if the Snapshot fails.
    pub fn take_snapshot_styled(&self, id: &str) -> anyhow::Result<StyledSnapshot> {
        let map = self.lock()?;
        let entry = Self::get_entry(&map, id)?;
        entry.session.snapshot_styled()
    }

    /// Resize the PTY of the Session identified by `id` to `cols` × `rows`.
    ///
    /// Sends `SIGWINCH` to the Hosted Process and updates the avt screen buffer.
    /// Available to any caller — resize is not guarded by the Write Lock in v1.
    pub fn resize(&self, id: &str, cols: u16, rows: u16) -> anyhow::Result<()> {
        let map = self.lock()?;
        let entry = Self::get_entry(&map, id)?;
        entry.session.resize(cols, rows)
    }

    /// Read raw output from the Session's output history starting at `since`.
    ///
    /// `since` is a byte offset into the total output stream. Pass `0` for the
    /// full history, or the next offset from a previous call to page forward.
    /// Returns an error if no Session with this id exists.
    ///
    /// Available on exited Sessions — callers can still page through historical
    /// output after the process exits.
    ///
    /// See [`ti_core::OutputChunk`] for the retention policy and offset semantics.
    pub fn read_output(&self, id: &str, since: u64) -> anyhow::Result<OutputChunk> {
        let map = self.lock()?;
        Self::get_entry(&map, id)?.session.read_output(since)
    }

    /// Return a summary of every Session in the registry.
    ///
    /// Each entry includes the session id, command name, PTY dimensions, and
    /// alive/exited status. The list is unordered (hash map iteration order).
    pub fn list_sessions(&self) -> anyhow::Result<Vec<SessionInfo>> {
        let map = self.lock()?;
        let mut infos = Vec::with_capacity(map.len());
        for (id, entry) in &*map {
            let exit_status = entry.session.try_exit_status()?;
            infos.push(SessionInfo {
                id: id.clone(),
                command: entry.command.clone(),
                cols: entry.session.cols(),
                rows: entry.session.rows(),
                exit_status,
            });
        }
        Ok(infos)
    }

    /// Terminate the Hosted Process and remove the Session from the registry.
    ///
    /// Sends a kill signal to the Hosted Process if it is still running, then
    /// removes the entry from the map. The Session (and all its output history)
    /// is dropped when removed.
    ///
    /// Returns a `"not Writer"` error if `caller_id` is not the Writer of the
    /// Session. Returns `"no Session with id"` if the id does not exist.
    pub fn close_session(&self, session_id: &str, caller_id: &str) -> anyhow::Result<()> {
        let mut map = self.lock()?;
        let entry = Self::get_entry(&map, session_id)?;

        if entry.writer_id != caller_id {
            anyhow::bail!("not Writer for session '{session_id}'");
        }

        // Best-effort kill — process may have already exited.
        let _ = entry.session.kill();

        map.remove(session_id);
        Ok(())
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
