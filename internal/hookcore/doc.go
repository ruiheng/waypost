// Package hookcore holds the harness-independent pieces of the Waypost
// lifecycle-hook integrations: shell command lexing, Waypost CLI receive
// output parsing, the pending-nudge state store with its MCP probe cache, the
// managed-hook document merge, and atomic file replacement. Codex and Devin
// keep only their wire-protocol adapters (payload fields, output contract,
// event names, probe commands) in internal/codexhook and internal/devinhook.
package hookcore
