//! `ti-core` — TI's embeddable terminal core.
//!
//! Owns the real work of a [`Session`]: PTY management, VT/ANSI emulation (via
//! the `avt` crate), the queryable screen buffer, and the event stream. Kept as
//! a library so the daemon — and, in-process, the MCP listener — can embed it
//! with zero subprocess overhead.
//!
//! See `docs/adr/0002-rust-core-avt-emulator.md` for the architectural rationale
//! (avt chosen over SwiftTerm / libghostty).
//!
//! [`Session`]: https://github.com/Derek-X-Wang/ti/blob/main/CONTEXT.md

pub mod session;
pub mod snapshot;

pub use session::{OutputChunk, Session};
pub use snapshot::{Attrs, Color, Snapshot, StyledCell, StyledSnapshot};
// Re-export so ti-daemon doesn't need a direct portable_pty dependency for ExitStatus.
pub use portable_pty::ExitStatus;
