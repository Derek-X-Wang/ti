# TI

Headless terminal (Rust daemon) that external AI agents drive over MCP, with an
optional native macOS Inspector for live viewing and macOS permission brokering.
See `CONTEXT.md` for domain language and `docs/adr/` for architecture decisions.

## Agent skills

### Issue tracker

Issues and PRDs are tracked as GitHub issues (`Derek-X-Wang/ti`) via the `gh` CLI.
See `docs/agents/issue-tracker.md`.

### Triage labels

Five canonical triage labels, default vocabulary (label string = role name). See
`docs/agents/triage-labels.md`.

### Domain docs

Single-context repo: one `CONTEXT.md` + `docs/adr/` at the root. See
`docs/agents/domain.md`.
