//! `ti-daemon` — TI's long-lived headless daemon.
//!
//! The single source of truth for every Session. Ships as a LaunchAgent inside
//! a code-signed TI.app bundle (see `docs/adr/0003-tcc-drives-app-bundle-launchagent.md`).
//! Driving Agents connect through the MCP listener; the Inspector attaches over
//! the Observer socket. Both are clients — the daemon owns the state.
//!
//! This is a skeleton. The MCP listener, Session registry, and tools land via
//! issues #2 onward. See `docs/adr/0001-client-server-daemon.md`.

fn main() {
    eprintln!("ti-daemon: not yet implemented — see https://github.com/Derek-X-Wang/ti/issues/2");
}
