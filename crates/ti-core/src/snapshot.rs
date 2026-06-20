//! [`Snapshot`] — a point-in-time capture of a Session's visible screen.
//!
//! Plain text + cursor position by default (see [`Snapshot`]).
//! Optional structured mode (see [`StyledSnapshot`]) adds per-cell colors and
//! attributes, alt-screen flag, and cursor key mode — for callers that need to
//! paint a faithful replica of the terminal (e.g., the native Inspector).
//!
//! This is the primary way a Driving Agent "sees" a Session (see CONTEXT.md).
//! Distinct from the raw output stream, which is the unbounded byte history
//! beyond the visible screen.

use serde::Serialize;

/// An ANSI terminal color.
///
/// Mirrors `avt::Color` but re-exported here so callers of `ti-core` do not
/// need to take a direct dependency on `avt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Color {
    /// One of the 256 indexed colors (0–255).
    Indexed(u8),
    /// A 24-bit RGB color.
    Rgb { r: u8, g: u8, b: u8 },
}

impl From<avt::Color> for Color {
    fn from(c: avt::Color) -> Self {
        match c {
            avt::Color::Indexed(i) => Color::Indexed(i),
            avt::Color::RGB(rgb) => Color::Rgb {
                r: rgb.r,
                g: rgb.g,
                b: rgb.b,
            },
        }
    }
}

/// Per-cell text attributes extracted from a terminal cell's `Pen`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct Attrs {
    pub bold: bool,
    pub faint: bool,
    pub italic: bool,
    pub underline: bool,
    pub blink: bool,
    pub inverse: bool,
    pub strikethrough: bool,
}

/// A single terminal cell with its character and visual styling.
#[derive(Debug, Clone, Serialize)]
pub struct StyledCell {
    /// The character occupying this cell (space for blank cells).
    pub ch: char,
    /// Foreground color (`None` = terminal default).
    pub fg: Option<Color>,
    /// Background color (`None` = terminal default).
    pub bg: Option<Color>,
    /// Text attributes (bold, italic, etc.).
    pub attrs: Attrs,
}

/// A structured Snapshot that includes per-cell color and attribute data.
///
/// Produced on demand via [`Session::snapshot_styled`]. Use this when you need
/// to paint a faithful visual replica of the terminal screen (e.g., for the
/// native macOS Inspector or an agent that reasons about visual layout).
///
/// For most agent use-cases, the plain-text [`Snapshot`] is simpler and faster.
#[derive(Debug, Clone, Serialize)]
pub struct StyledSnapshot {
    /// One `Vec<StyledCell>` per visible row, top to bottom.
    ///
    /// Every row has exactly `cols` cells (the PTY width at snapshot time).
    pub lines: Vec<Vec<StyledCell>>,
    /// Cursor column (0-based).
    pub cursor_col: usize,
    /// Cursor row (0-based).
    pub cursor_row: usize,
    /// Whether the cursor is currently visible.
    pub cursor_visible: bool,
    /// `true` when the terminal is in the alternate screen buffer (e.g., vim,
    /// less, any TUI using `\x1b[?1049h`).
    pub alt_screen: bool,
}

impl StyledSnapshot {
    /// Returns all visible lines as plain text joined by newlines.
    ///
    /// Extracts the character from each cell, mirrors [`Snapshot::text`].
    pub fn text(&self) -> String {
        self.lines
            .iter()
            .map(|row| row.iter().map(|c| c.ch).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

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
