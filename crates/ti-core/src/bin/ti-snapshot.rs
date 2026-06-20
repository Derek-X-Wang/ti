//! `ti-snapshot` — CLI harness for verifying the ti-core tracer bullet.
//!
//! Spawns a Hosted Process in a PTY, waits for it to exit (which also drains
//! all PTY output into the screen buffer), takes a text Snapshot, and prints it.
//! Exit code 0 means the Snapshot was produced successfully; non-zero means an
//! error occurred.
//!
//! Usage:
//!   ti-snapshot <program> [args...]
//!
//! Example (the acceptance-criteria demo from issue #1):
//!   ti-snapshot echo hello
//!   # prints a Snapshot whose text contains "hello"

fn main() -> anyhow::Result<()> {
    let mut argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.is_empty() {
        anyhow::bail!("Usage: ti-snapshot <program> [args...]");
    }

    let program = argv.remove(0);
    let args: Vec<&str> = argv.iter().map(String::as_str).collect();

    let session = ti_core::Session::spawn(&program, &args, None, None)?;

    // wait() blocks until the Hosted Process exits AND the reader thread has
    // drained all remaining PTY output into avt — no sleep needed.
    let status = session.wait()?;

    let snap = session.snapshot()?;

    println!("=== Snapshot ===");
    println!("{}", snap.text());
    println!(
        "=== cursor col={} row={} visible={} ===",
        snap.cursor_col, snap.cursor_row, snap.cursor_visible
    );
    println!("=== exit status: {:?} ===", status);

    Ok(())
}
