# TI (Terminal Interface)

A headless terminal exposed so external AI agents can drive interactive terminal
sessions over MCP, with an optional human interface for inspection and permission
control. Session state lives in a long-lived daemon; agents and humans connect to
it as clients.

## Language

**TI Daemon**:
The long-lived, headless server process that owns all session state. The single
source of truth for every Session. Written in Rust. Runs with no GUI by default;
clients attach to it. Ships as a LaunchAgent inside a code-signed TI.app bundle,
running in the user login session — required so macOS TCC permissions work (see
OS Permission).
_Avoid_: server, backend, host (too generic)

**TI Core**:
The embeddable Rust library inside the daemon that does the real work of a Session:
PTY management, VT/ANSI emulation (via a third-party emulator crate), the queryable
screen buffer, and the event stream. Separated from the daemon so the MCP Server can
embed it directly with zero subprocess overhead. Mirrors the ht-core ↔ ht-mcp split.
_Avoid_: engine, terminal lib (be specific)

**Session**:
One running terminal: a PTY, the program inside it, the terminal-emulation state,
and the screen buffer. The unit of lifecycle (create / list / close) and the unit
a Driving Agent or Inspector connects to.
_Avoid_: terminal, tab, pane, window

**Hosted Process**:
The program running inside a Session's PTY — e.g. bash, vim, or an AI coding-agent
CLI. This is "the AI agent in the terminal" from the original framing, but it can
be any binary. TI treats it as an opaque process it feeds input to and reads output
from.
_Avoid_: inner agent, the agent, child (ambiguous with Driving Agent)

**Driving Agent**:
An external AI agent that controls a Session over MCP. It talks to the MCP Server,
never to the Hosted Process directly. The primary intended user of TI.
_Avoid_: agent (unqualified), controller, client (overloaded)

**MCP Server**:
The agent-facing interface of the TI Daemon. A listener *inside* the daemon process
that speaks MCP over streamable-HTTP/SSE on a port, exposing terminal control as MCP
tools to Driving Agents (local or remote). Kept as a clean internal module over TI
Core so other transports (stdio adapter, gRPC) can be added later.
_Avoid_: API, gateway

**Inspector**:
The optional human-facing client (a native macOS app) that attaches to the daemon
to watch Sessions live, take over a Session's Write Lock, and surface missing OS
Permissions (deep-linking System Settings). Transient — comes and goes without
affecting Session state.
_Avoid_: GUI, viewer, dashboard

**OS Permission**:
A macOS TCC grant (Full Disk Access, Files & Folders, Accessibility, Screen
Recording, Automation) that a Hosted Process needs but which macOS attributes to the
**TI.app bundle**, not the child process — via responsible-process attribution, the
same way scripts inside Terminal.app inherit Terminal's grants. The human grants each
once to TI; all Sessions inherit. This — NOT per-command policy — is what "giving
different permissions" means for TI. Command-level policy (what `rm` an agent may
run) belongs to the Driving Agent's own harness, never to TI.
_Avoid_: permission (unqualified — collides with Write Lock and with agent-side policy)

**Writer**:
The single client (a Driving Agent, or the Inspector after takeover) that currently
holds exclusive input rights to a Session. Exactly one per Session at a time.
_Avoid_: owner, controller, master

**Observer**:
Any attached client receiving a Session's output read-only, with no input rights. A
Session has one Writer and zero-or-more Observers. The Inspector is an Observer until
it takes over.
_Avoid_: viewer, listener, subscriber

**Write Lock**:
The exclusive input right over a Session. Held by the Writer; can be handed off or
grabbed (takeover). The single chokepoint through which all input flows — and
therefore the only place a permission decision can be enforced before keystrokes
reach the Hosted Process.
_Avoid_: input lock, mutex (too generic)

**Snapshot**:
A point-in-time capture of a Session's visible screen. Plain text + cursor by
default; per-cell styles, terminal modes, and alt-screen flag on request. The
primary way a Driving Agent "sees" a Session. Distinct from the raw output stream,
which is the unbounded byte history beyond the visible screen.
_Avoid_: screenshot, dump, capture (unqualified)

**Bearer Token**:
The credential a Driving Agent presents to authenticate to the MCP Server. Required
on every connection, even on localhost (any local process could otherwise drive a
Session). The daemon binds `127.0.0.1` only in v1.
_Avoid_: API key, password

## Flagged ambiguities

- **"Agent"** is forbidden unqualified. Always **Driving Agent** (external,
  controls TI) or **Hosted Process** (runs inside the PTY). The original request
  used "AI agent" for both; they are different things on opposite sides of TI.

## Example dialogue

> **Dev:** When a Driving Agent calls `send-keys`, does that go to the Hosted Process?
> **Domain:** Through the MCP Server to the daemon, which writes the keys into that
> Session's PTY. The Hosted Process — say a Claude Code CLI — sees them as if a human
> typed. The Driving Agent never speaks to the Hosted Process directly.
> **Dev:** And if I open the Inspector?
> **Domain:** It attaches to the same daemon and renders that Session's screen buffer
> live. Two clients — one Driving Agent, one human — on one Session. Close the
> Inspector and the Session keeps running; only the daemon owns its state.
