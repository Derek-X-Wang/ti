//! [`Snapshot`] — a point-in-time capture of a Session's visible screen.
//!
//! Plain text + cursor position by default. This is the primary way a Driving
//! Agent "sees" a Session (see CONTEXT.md). Distinct from the raw output stream,
//! which is the unbounded byte history beyond the visible screen.

/// A point-in-time capture of a Session's visible screen.
///
/// Contains the plain-text content of each visible row and the cursor position.
/// Produced on demand via [`Session::snapshot`].
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Plain-text content of each visible line (top to bottom).
    pub lines: Vec<String>,
    /// Cursor column (0-based).
    pub cursor_col: usize,
    /// Cursor row (0-based).
    pub cursor_row: usize,
    /// Whether the cursor is currently visible.
    pub cursor_visible: bool,
}

impl Snapshot {
    /// Returns all visible lines joined by newlines.
    pub fn text(&self) -> String {
        self.lines.join("\n")
    }

    /// Returns `true` if the visible screen contains `needle` anywhere.
    ///
    /// Searches the plain-text content of every line.
    pub fn contains(&self, needle: &str) -> bool {
        self.lines.iter().any(|line| line.contains(needle))
    }
}
