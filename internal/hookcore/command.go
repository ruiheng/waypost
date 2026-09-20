package hookcore

import (
	"encoding/json"
	"fmt"
	"runtime"
	"slices"
	"strings"

	"mvdan.cc/sh/v3/syntax"
)

// Shared hook copy. These strings are identical across harnesses; each
// harness package re-exports them under its own constant names.
const (
	AdditionalContext = `A prior Waypost nudge in this session was already handled before compaction (either a receive completed or no message was available). Do not repeat the receive merely because of compaction. Continue from the compacted context.`

	MCPNudgeContext = `The waypost_recv MCP tool is available. Use it instead of the Waypost CLI.`

	CLINudgeContext = `The Waypost MCP tool waypost_recv is unavailable. Receive the pending delivery with ` + "`waypost recv --json`" + `.`

	MCPProbeFailedNudgeContext = `Look for the waypost_recv MCP tool. If it is unavailable, receive the pending delivery with ` + "`waypost recv --json`" + `.`

	WaitPollingContext = `Do not poll Waypost. Continue other available work; if none remains, stop completely.`

	StatusDenialReason = `The Waypost MCP tool waypost_status is available. Use it instead of running waypost status.`
)

// WaypostMCPCommandBlacklist maps a `waypost <subcommand>` to the MCP tool
// that replaces it. The empty replacement marks a command that has no MCP
// equivalent and must always be denied (`waypost mcp`).
var WaypostMCPCommandBlacklist = map[string]string{
	"ack":     "waypost_ack",
	"recv":    "waypost_recv",
	"receive": "waypost_recv",
	"send":    "waypost_send",
	"mcp":     "",
}

// WaypostMCPTool reports the MCP tool guarding a direct `waypost
// <subcommand>` invocation. guarded is false for unrelated or unguarded
// commands; an empty tool with guarded set marks a command that has no MCP
// equivalent and must always be denied (`waypost mcp`).
func WaypostMCPTool(command string) (tool string, guarded bool) {
	for _, subcommand := range WaypostCommands(command) {
		if subcommand == "status" {
			return "waypost_status", true
		}
		if tool, guarded = WaypostMCPCommandBlacklist[subcommand]; guarded {
			return tool, true
		}
	}
	return "", false
}

// WaypostCommandDenialReason renders the deny reason for a guarded command.
// tool is the result of WaypostMCPTool; an empty tool uses serverReason,
// which names the harness managing the MCP server.
func WaypostCommandDenialReason(tool, serverReason string) string {
	if tool == "" {
		return serverReason
	}
	if tool == "waypost_status" {
		return StatusDenialReason
	}
	return fmt.Sprintf("The Waypost MCP tool %s is available. Use it instead of the Waypost CLI.", tool)
}

// LooksLikeWaypostWaitCommand reports whether command runs `waypost wait`.
func LooksLikeWaypostWaitCommand(command string) bool {
	return slices.Contains(WaypostCommands(command), "wait")
}

// RunsWaypostReceive reports whether command runs `waypost recv` (or its
// `receive` alias), including invocations nested in compound commands,
// command substitutions, and wrapper commands.
func RunsWaypostReceive(command string) bool {
	return slices.ContainsFunc(WaypostCommands(command), func(subcommand string) bool {
		return subcommand == "recv" || subcommand == "receive"
	})
}

// shellCommandWrappers execute one of their arguments as a command, so a
// waypost invocation nested under them still runs waypost.
var shellCommandWrappers = map[string]bool{
	"builtin": true, "command": true, "doas": true, "env": true,
	"nice": true, "nohup": true, "stdbuf": true, "sudo": true,
	"time": true, "watch": true, "xargs": true,
}

// shellDashCCommands interpret the argument following -c (possibly bundled
// as -lc, -ic, ...) as a command line.
var shellDashCCommands = map[string]bool{
	"ash": true, "bash": true, "dash": true, "fish": true,
	"ksh": true, "sh": true, "zsh": true,
}

// WaypostCommands returns the subcommand of every `waypost <subcommand>`
// invocation in a shell command, in source order. The command is parsed as
// shell syntax, so invocations behind &&, ;, |, command substitutions,
// subshells, wrapper commands (env, sudo, ...), and `sh -c` strings are all
// recognized, while waypost merely mentioned inside quoted arguments or
// other commands' operands is not. Unparseable commands report no
// invocations — the guard fails open.
func WaypostCommands(command string) []string {
	// Windows PowerShell invocation prefix.
	command = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(command), "&"))
	file, err := syntax.NewParser().Parse(strings.NewReader(command), "")
	if err != nil {
		return nil
	}
	var subcommands []string
	syntax.Walk(file, func(node syntax.Node) bool {
		call, ok := node.(*syntax.CallExpr)
		if !ok {
			return true
		}
		if subcommand, ok := waypostCallSubcommand(call); ok {
			subcommands = append(subcommands, subcommand)
		}
		if inner, ok := dashCCommand(call); ok {
			subcommands = append(subcommands, WaypostCommands(inner)...)
		}
		return true
	})
	return subcommands
}

// waypostCallSubcommand extracts the subcommand of a call whose executable
// is waypost, skipping --state-dir flags. For wrapper commands it first
// locates the waypost token among the wrapper's arguments.
func waypostCallSubcommand(call *syntax.CallExpr) (string, bool) {
	if len(call.Args) == 0 {
		return "", false
	}
	executable, ok := wordLiteral(call.Args[0])
	if !ok {
		return "", false
	}
	args := call.Args[1:]
	if shellCommandWrappers[commandBaseName(executable)] {
		index := -1
		for i, arg := range args {
			if literal, ok := wordLiteral(arg); ok && IsWaypostExecutable(literal) {
				index = i
				break
			}
		}
		if index < 0 {
			return "", false
		}
		args = args[index+1:]
	} else if !IsWaypostExecutable(executable) {
		return "", false
	}
	for i := 0; i < len(args); i++ {
		arg, ok := wordLiteral(args[i])
		if !ok {
			return "", false
		}
		switch {
		case arg == "--state-dir":
			i++
		case strings.HasPrefix(arg, "--state-dir="):
		default:
			return arg, true
		}
	}
	return "", false
}

// dashCCommand returns the command line a `sh -c`-style call interprets.
func dashCCommand(call *syntax.CallExpr) (string, bool) {
	if len(call.Args) < 3 {
		return "", false
	}
	executable, ok := wordLiteral(call.Args[0])
	if !ok || !shellDashCCommands[commandBaseName(executable)] {
		return "", false
	}
	for i := 1; i < len(call.Args)-1; i++ {
		flag, ok := wordLiteral(call.Args[i])
		if !ok {
			continue
		}
		if flag == "-c" || (strings.HasPrefix(flag, "-") && !strings.HasPrefix(flag, "--") && strings.ContainsRune(flag[1:], 'c')) {
			return wordLiteral(call.Args[i+1])
		}
	}
	return "", false
}

// wordLiteral renders a word consisting solely of literal text — bare,
// single-quoted, or double-quoted — and reports whether it was one.
func wordLiteral(word *syntax.Word) (string, bool) {
	var literal strings.Builder
	for _, part := range word.Parts {
		switch part := part.(type) {
		case *syntax.Lit:
			literal.WriteString(part.Value)
		case *syntax.SglQuoted:
			literal.WriteString(part.Value)
		case *syntax.DblQuoted:
			for _, inner := range part.Parts {
				text, ok := inner.(*syntax.Lit)
				if !ok {
					return "", false
				}
				literal.WriteString(text.Value)
			}
		default:
			return "", false
		}
	}
	return literal.String(), len(word.Parts) > 0
}

// IsWaypostExecutable reports whether executable names a waypost binary,
// tolerating slash and backslash separators, case differences, and the .exe
// suffix.
func IsWaypostExecutable(executable string) bool {
	return commandBaseName(executable) == "waypost"
}

// commandBaseName normalizes an executable path to a lowercase base name
// without the .exe suffix.
func commandBaseName(executable string) string {
	normalized := strings.ReplaceAll(executable, `\`, "/")
	if separator := strings.LastIndexByte(normalized, '/'); separator >= 0 {
		normalized = normalized[separator+1:]
	}
	return strings.TrimSuffix(strings.ToLower(normalized), ".exe")
}

// SplitHookCommand extracts the executable token and the remaining argument
// text from a hook command, honoring a quoted executable path and an optional
// Windows `&` invocation prefix.
func SplitHookCommand(command string) (exe, rest string, ok bool) {
	command = strings.TrimSpace(command)
	command = strings.TrimSpace(strings.TrimPrefix(command, "&"))
	if command == "" {
		return "", "", false
	}
	if quote := command[0]; quote == '"' || quote == '\'' {
		end := strings.IndexByte(command[1:], quote)
		if end < 0 {
			return "", "", false
		}
		return command[1 : 1+end], strings.TrimSpace(command[2+end:]), true
	}
	fields := strings.Fields(command)
	return fields[0], strings.Join(fields[1:], " "), true
}

// IsManagedHookCommand reports whether command consists solely of a waypost
// executable followed by the given hook subcommand, so hooks written by an
// older Waypost executable path are recognized as managed. Commands that
// merely contain the subcommand text (for example `echo devin-hook` or
// `my-wrapper devin-hook --audit`) are user hooks and must be kept.
func IsManagedHookCommand(command, subcommand string) bool {
	exe, rest, ok := SplitHookCommand(command)
	return ok && rest == subcommand && IsWaypostExecutable(exe)
}

// DefaultNudgeMessage is the prompt text the harness emits when Waypost
// asks it to check for a pending delivery.
const DefaultNudgeMessage = "NOTICE: There might be new delivery in waypost."

// LooksLikeWaypostNudge reports whether prompt is the Waypost nudge,
// ignoring case and surrounding whitespace.
func LooksLikeWaypostNudge(prompt string) bool {
	return strings.EqualFold(strings.TrimSpace(prompt), DefaultNudgeMessage)
}

// MCPProbeFailureMessage renders the system message for a failed MCP
// availability probe.
func MCPProbeFailureMessage(err error) string {
	if err == nil {
		return "Waypost MCP probe failed for an unknown reason."
	}
	return fmt.Sprintf("Waypost MCP probe failed: %v", err)
}

// CommandToolInput decodes the {command: string} tool_input of a shell
// command tool. toolLabel names the harness tool (for example "Codex Bash"
// or "Devin exec") in the error.
func CommandToolInput(raw json.RawMessage, toolLabel string) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var input struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return "", fmt.Errorf("parse %s tool input: %w", toolLabel, err)
	}
	return input.Command, nil
}

// QuoteCommandPath shell-quotes a path for embedding in a hook command.
func QuoteCommandPath(path string) string {
	if runtime.GOOS == "windows" {
		return `"` + strings.ReplaceAll(path, `"`, `\"`) + `"`
	}
	return "'" + strings.ReplaceAll(path, "'", "'\"'\"'") + "'"
}
