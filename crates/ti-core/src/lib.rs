//! `ti-core` — TI's embeddable terminal core.
//!
//! Owns the real work of a [`Session`]: PTY management, VT/ANSI emulation (via
//! the `avt` crate), the queryable screen buffer, and the event stream. Kept as
//! a library so the daemon — and, in-process, the MCP listener — can embed it
//! with zero subprocess overhead.
//!
//! This is a skeleton. The tracer-bullet implementation (spawn a Hosted Process
//! in a PTY, drive it through avt, produce a text Snapshot) lands via issue #1.
//! See `docs/adr/0002-rust-core-avt-emulator.md`.
//!
//! [`Session`]: https://github.com/Derek-X-Wang/ti/blob/main/CONTEXT.md
