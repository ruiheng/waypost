// Package hookcore holds the harness-independent pieces of the Waypost
// lifecycle-hook integrations: the spec-driven hook lifecycle engine, shell
// command lexing, Waypost CLI receive output parsing, the pending-nudge
// state store with its MCP probe cache, the managed-hook document merge and
// install/doctor loops, and atomic file replacement. Codex, Devin, and Claude
// Code keep only their wire-protocol adapters and spec declarations (payload
// fields, output envelopes, event and tool names, probe commands) in
// internal/codexhook, internal/devinhook, and internal/claudehook.
package hookcore
