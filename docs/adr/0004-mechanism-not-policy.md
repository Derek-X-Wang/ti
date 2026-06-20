# TI is mechanism, not policy: no command-level permission gating

TI does **not** implement command-level permission policy — there is no allow/deny of
specific commands (no gating of `rm -rf`, `sudo`, etc.), and no per-command approval
prompt. That responsibility belongs to the **Driving Agent's own harness** (e.g. Claude
Code's permission model), which decides what its agent is allowed to do before it ever
sends keystrokes to TI.

TI guards only **its own resources**: who may connect (Bearer Token auth), who holds a
Session's Write Lock (write vs observe, with human takeover), and which macOS OS
Permissions the app holds. It gates *who connects and who types* and *what OS grants
exist* — never *what commands run*.

This is recorded as an explicit "no" because the original framing mentioned
"permissions," and a future engineer would otherwise be tempted to rebuild Claude-Code-
style command gating inside the terminal. Duplicating policy in a mechanism layer is
both redundant and a place to get the security boundary subtly wrong. ("Permissions"
that TI *does* own are macOS TCC grants — a different concept; see ADR-0003.)
