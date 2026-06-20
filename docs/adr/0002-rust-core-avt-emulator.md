# Rust core with the avt emulator (not SwiftTerm or libghostty)

The terminal core (TI Core: PTY management, VT/ANSI emulation, screen buffer, event
stream) is a **Rust** library modeled on andyk/ht's proven architecture, using the
**avt** emulator crate. We did not write it in Swift on SwiftTerm's HeadlessTerminal,
and did not embed libghostty via FFI.

Why: an always-on headless daemon (see ADR-0001) is most natural in Rust, not as a
Swift server or a GPU-render core. SwiftTerm's VT/ANSI fidelity for demanding TUIs
(alt-screen, mouse, truecolor, vim/emacs) is unbenchmarked, and its bugs would become
ours; libghostty is render-oriented and a heavy (~200-call) FFI fit for a headless
screen-scrape use case. avt is purpose-built for "give me the screen as a structured
snapshot" and is already proven driving vim/emacs through ht. We model ht's design
rather than embedding ht itself, keeping TI's code and license our own.

## Considered Options

- **SwiftTerm HeadlessTerminal (pure Swift)** — rejected: unbenchmarked TUI fidelity;
  Swift-as-server is a less-trodden path; emulator bugs become ours.
- **libghostty via FFI** — rejected: render-oriented, large FFI surface, overkill for
  headless.
- **Wrap tmux** — rejected: cedes emulation control, coarser snapshots, runtime dep.

## Consequences

avt's scrollback is thin; long output that scrolls off the visible screen is served by
the raw `read_output` byte stream, not the emulator's scrollback. Re-evaluate
`alacritty_terminal` if avt drops sequences real-world TUIs emit.
