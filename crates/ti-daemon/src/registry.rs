//! [`SessionRegistry`] — the daemon's in-memory store of live Sessions.
//!
//! The TI Daemon is the single source of truth for every Session (see ADR-0001
//! and CONTEXT.md). This module holds the map from stable session IDs to running
//! [`ti_core::Session`] instances. All MCP tools go through the registry.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::Context as _;
use ti_core::{Session, Snapshot};

/// A thread-safe registry of live Sessions keyed by a stable string ID.
///
/// Cloning the registry is cheap — it shares the same underlying `Arc<Mutex<…>>`.
#[derive(Clone)]
pub struct SessionRegistry {
    inner: Arc<Mutex<HashMap<String, Session>>>,
}

impl SessionRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Spawn a new Session running `program` with `args`, insert it under `id`,
    /// and return the id.
    ///
    /// Returns an error if a Session with this id already exists, or if spawning
    /// the Hosted Process fails.
    pub fn create_session(
        &self,
        id: String,
        program: &str,
        args: &[&str],
    ) -> anyhow::Result<String> {
        let mut map = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry lock poisoned"))?;

        if map.contains_key(&id) {
            anyhow::bail!("Session id '{id}' already exists");
        }

        let session =
            Session::spawn(program, args, None, None).context("failed to spawn Session")?;

        map.insert(id.clone(), session);
        Ok(id)
    }

    /// Take a text Snapshot of the Session identified by `id`.
    ///
    /// Returns an error if no Session with this id exists or if the Snapshot
    /// fails.
    pub fn take_snapshot(&self, id: &str) -> anyhow::Result<Snapshot> {
        let map = self
            .inner
            .lock()
            .map_err(|_| anyhow::anyhow!("session registry lock poisoned"))?;

        let session = map
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("no Session with id '{id}'"))?;

        session.snapshot()
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
