package hookcore

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
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
	subcommand, ok := DirectWaypostCommand(command)
	if !ok {
		return "", false
	}
	if subcommand == "status" {
		return "waypost_status", true
	}
	tool, guarded = WaypostMCPCommandBlacklist[subcommand]
	return tool, guarded
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

// LooksLikeWaypostWaitCommand reports whether command is a direct
// `waypost wait` invocation.
func LooksLikeWaypostWaitCommand(command string) bool {
	subcommand, ok := DirectWaypostCommand(command)
	return ok && subcommand == "wait"
}

// DirectWaypostCommand extracts the subcommand from a shell command whose
// first word is a waypost executable, skipping --state-dir flags. An optional
// Windows `&` invocation prefix is honored.
func DirectWaypostCommand(command string) (string, bool) {
	rest := strings.TrimSpace(command)
	executable, rest, ok := ConsumeCommandWord(rest)
	if !ok {
		return "", false
	}
	if executable == "&" {
		executable, rest, ok = ConsumeCommandWord(rest)
		if !ok {
			return "", false
		}
	}
	if !IsWaypostExecutable(executable) {
		return "", false
	}

	for {
		argument, remaining, ok := ConsumeCommandWord(rest)
		if !ok {
			return "", false
		}
		switch {
		case argument == "--state-dir":
			_, rest, ok = ConsumeCommandWord(remaining)
			if !ok {
				return "", false
			}
		case strings.HasPrefix(argument, "--state-dir=") && len(argument) > len("--state-dir="):
			rest = remaining
		default:
			return argument, true
		}
	}
}

// ConsumeCommandWord reads the next shell word, honoring single and double
// quotes and returning shell metacharacters as single-character words.
func ConsumeCommandWord(input string) (string, string, bool) {
	input = strings.TrimLeft(input, " \t\r")
	if input == "" || input[0] == '\n' {
		return "", input, false
	}
	if strings.ContainsRune(";&|<>()", rune(input[0])) {
		return input[:1], input[1:], true
	}

	var word strings.Builder
	var quote byte
	for index := 0; index < len(input); index++ {
		character := input[index]
		if quote != 0 {
			if character == quote {
				quote = 0
				continue
			}
			word.WriteByte(character)
			continue
		}
		switch character {
		case '\'', '"':
			quote = character
		case ' ', '\t', '\r':
			return word.String(), input[index:], word.Len() != 0
		case '\n', ';', '&', '|', '<', '>', '(', ')':
			return word.String(), input[index:], word.Len() != 0
		default:
			word.WriteByte(character)
		}
	}
	if quote != 0 || word.Len() == 0 {
		return "", input, false
	}
	return word.String(), "", true
}

// IsWaypostExecutable reports whether executable names a waypost binary,
// tolerating slash and backslash separators, case differences, and the .exe
// suffix.
func IsWaypostExecutable(executable string) bool {
	normalized := strings.ReplaceAll(executable, `\`, "/")
	base := normalized
	if separator := strings.LastIndexByte(normalized, '/'); separator >= 0 {
		base = normalized[separator+1:]
	}
	return strings.EqualFold(base, "waypost") || strings.EqualFold(base, "waypost.exe")
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
