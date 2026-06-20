# Client-server daemon topology

TI's Session state (PTY, emulator, screen buffer) lives in a long-lived headless
**daemon**; Driving Agents (via MCP) and the human Inspector are both **clients** that
attach to it. We chose this over a monolithic GUI app (cmux-style, where state dies
with the window) and over a GUI-owns-the-core model, because the core premise is a
headless terminal that external agents drive: Sessions must outlive any window, there
must be one source of truth for Session state, and the agent control-plane must be
cleanly separable from the optional human control-plane. The cost we accept is
defining an internal control protocol between the daemon and its clients.
