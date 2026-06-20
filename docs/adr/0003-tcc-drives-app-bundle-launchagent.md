# TCC drives packaging: signed .app bundle + user-session LaunchAgent

The "headless daemon" (ADR-0001) ships **inside a code-signed `TI.app` bundle** and
runs as a **LaunchAgent in the user login session** — not as a root LaunchDaemon and
not as a bare CLI binary. This looks surprising: why wrap a headless thing in a GUI app?

Because of macOS TCC (Transparency, Consent & Control). A Hosted Process inside a
Session (bash, vim, an agent CLI) that reads protected files, uses Accessibility, or
screen-records has that access **attributed to TI's app bundle**, not to the child —
via responsible-process attribution, the same mechanism by which scripts inside
Terminal.app inherit Terminal's grants. TCC permissions attach to a bundle's code
signature, and the consent prompts need a foreground GUI / user-session context to
appear. A root LaunchDaemon is TCC-hostile (runs as root, can't cleanly surface or
hold user grants, mis-attributes child access); a bare unsigned binary has no stable
bundle identity to grant permissions to.

So: one signed bundle holds the TCC grants, the daemon runs in the user session so
attribution and prompts work, and the human grants each OS Permission once — every
Session inherits it. The Inspector (the bundle's foreground app) brokers missing-
permission prompts lazily and deep-links System Settings.

## Consequences

The daemon cannot start before user login (acceptable — it serves user-session
agents). Distribution requires Developer ID signing + notarization and the relevant
entitlements/usage strings (Accessibility, Screen Recording, Files & Folders).
