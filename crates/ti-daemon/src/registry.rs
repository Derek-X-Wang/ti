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
//! Error vocabulary established by this module and reused by all callers:
//!
//! | Situation                   | Error message prefix             |
//! |-----------------------------|----------------------------------|
//! | Unknown session id          | `"no Session with id '…'"`       |
//! | Duplicate session id        | `"Session id '…' already exists"` |
//! | Caller is not the Writer    | `"not Writer for session '…'"`   |

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::Context as _;
use ti_core::{Session, Snapshot};

/// Holds a [`Session`] together with the id of its current Writer.
struct SessionEntry {
    session: Session,
    /// The client id of the Writer — the only client allowed to send input.
    writer_id: String,
}

/// A thread-safe registry of live Sessions keyed by a stable string ID.
///
/// Cloning the registry is cheap — it shares the same underlying `Arc<Mutex<…>>`.
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

        map.insert(id.clone(), SessionEntry { session, writer_id });
        Ok(id)
    }

    /// Send raw bytes into the Hosted Process via the Write Lock.
    ///
    /// Only the Writer (the client that created the Session) may call this.
    /// Returns a `"not Writer"` error if `caller_id` does not match the stored
    /// `writer_id` for the Session.
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

        entry.session.send_input(data)
    }

    /// Take a text Snapshot of the Session identified by `id`.
    ///
    /// Available to any caller — Writers and Observers alike. Returns an error
    /// if no Session with this id exists or if the Snapshot fails.
    pub fn take_snapshot(&self, id: &str) -> anyhow::Result<Snapshot> {
        let map = self.lock()?;
        let entry = Self::get_entry(&map, id)?;
        entry.session.snapshot()
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
