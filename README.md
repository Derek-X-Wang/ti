# TI — Terminal Interface

A **headless terminal** that external AI agents drive over MCP, with an optional
native macOS **Inspector** for live viewing and macOS permission brokering.

A long-lived Rust daemon owns every terminal Session (PTY + `avt` emulation +
screen buffer). Driving Agents connect through an MCP listener; a human can
attach the Inspector to watch or take over. Modeled on [`andyk/ht`](https://github.com/andyk/ht).

## Layout

| Path | What |
| --- | --- |
| `crates/ti-core` | Embeddable terminal core: PTY, avt emulation, screen buffer, events |
| `crates/ti-daemon` | Headless daemon: Session registry, MCP listener, Observer socket |
| `CONTEXT.md` | Domain glossary — the project's vocabulary |
| `docs/adr/` | Architecture decisions (topology, core, packaging, scope) |
| `docs/agents/` | Conventions for AI agents working in this repo |

## Status

Greenfield. The architecture is locked (see `CONTEXT.md` + `docs/adr/`); the
build is tracked as [GitHub issues](https://github.com/Derek-X-Wang/ti/issues)
as thin end-to-end vertical slices, starting with the core PTY + snapshot tracer.

## Develop

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --all-targets
```
