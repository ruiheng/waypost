package claudehook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ruiheng/waypost/internal/hookcore"
	"github.com/ruiheng/waypost/internal/launchpath"
)

const (
	shellToolName          = "Bash"
	receiveMCPToolName     = "mcp__waypost__waypost_recv"
	hookStateDirectoryName = "waypost-hook-state"
	configFileName         = "settings.json"
	defaultNudgeMessage    = hookcore.DefaultNudgeMessage
	harnessLabel           = "Claude Code"
)

const hookTimeoutSeconds int64 = 5
const cleanupHookTimeoutSeconds int64 = 3

const AdditionalContext = hookcore.AdditionalContext
const MCPNudgeContext = hookcore.MCPNudgeContext
const CLINudgeContext = hookcore.CLINudgeContext
const MCPProbeFailedNudgeContext = hookcore.MCPProbeFailedNudgeContext
const WaitPollingContext = hookcore.WaitPollingContext
const MCPStatusDenialReason = hookcore.StatusDenialReason
const MCPServerCommandDenialReason = `The Waypost MCP server is managed by Claude Code. Never run the Waypost CLI command ` + "`waypost mcp`" + `.`

type hookInput = hookcore.HookInput

type hookOutput struct {
	SystemMessage      string              `json:"systemMessage,omitempty"`
	HookSpecificOutput *hookSpecificOutput `json:"hookSpecificOutput,omitempty"`
}

type hookSpecificOutput struct {
	HookEventName            string `json:"hookEventName"`
	AdditionalContext        string `json:"additionalContext,omitempty"`
	PermissionDecision       string `json:"permissionDecision,omitempty"`
	PermissionDecisionReason string `json:"permissionDecisionReason,omitempty"`
}

type InstallResult = hookcore.InstallHooksResult

type DoctorResult = hookcore.DoctorHooksResult

func Run(ctx context.Context, r io.Reader, w io.Writer) error {
	return run(ctx, r, w)
}

func run(ctx context.Context, r io.Reader, w io.Writer) error {
	store, err := defaultNudgeStateStore()
	if err != nil {
		return err
	}
	return runWithDependencies(ctx, r, w, func(ctx context.Context) (bool, error) {
		return probeWaypostMCPWithTimeout(ctx, mcpProbeTimeout)
	}, store)
}

func runWithMCPProbe(ctx context.Context, r io.Reader, w io.Writer, probe waypostMCPProbe) error {
	return runWithDependencies(ctx, r, w, probe, newMemoryNudgeStateStore())
}

func runWithDependencies(
	ctx context.Context,
	r io.Reader,
	w io.Writer,
	probe waypostMCPProbe,
	store nudgeStateStore,
) error {
	return hookcore.RunHook(ctx, r, w, harnessSpec, recordProbe(probe), store)
}

func recordProbe(probe waypostMCPProbe) func(context.Context) (hookcore.MCPProbeRecord, error) {
	return func(ctx context.Context) (hookcore.MCPProbeRecord, error) {
		available, err := probe(ctx)
		if err != nil {
			return hookcore.MCPProbeRecord{}, err
		}
		return hookcore.MCPProbeRecord{Available: available}, nil
	}
}

// harnessSpec declares Claude Code's variation of the shared Waypost hook
// lifecycle: compaction arrives as SessionStart with source "compact", but
// Claude Code drops that event's additionalContext, so the compact guard is
// marked pending there and re-injected through the next honored event. The
// shell tool is Bash; denials use the permissionDecision envelope and probe
// failures surface as systemMessage.
var harnessSpec = hookcore.HarnessSpec{
	Label:                harnessLabel,
	ShellTool:            shellToolName,
	CompactSource:        "compact",
	CompactSourceDropped: true,
	FallbackEvent:        "SessionStart",
	ServerDenialReason:   MCPServerCommandDenialReason,
	EmitDeny:             writeDenyOutput,
	EmitProbeFailure: func(w io.Writer, probeErr error) error {
		return writeSystemMessage(w, hookcore.MCPProbeFailureMessage(probeErr))
	},
	EmitNudgeProbeFailure: func(w io.Writer, probeErr error) error {
		return writeOutputWithSystemMessage(w, "UserPromptSubmit", MCPProbeFailedNudgeContext, hookcore.MCPProbeFailureMessage(probeErr))
	},
	EmitInputTimeout: func(w io.Writer) error {
		return writeSystemMessage(w, "Waypost Claude Code hook input timed out waiting for the harness payload")
	},
	ReceiveSucceeded: successfulWaypostReceive,
}

// managedHooks declares Claude Code's managed hook groups in the user-level
// settings.json hooks object. PostToolUse carries no matcher: Claude Code
// drops compact-source SessionStart additionalContext, so the compact guard
// is delivered through the next PostToolUse event and the group must match
// every tool, not only receive completions. The same applies to the
// matcherless SessionStart, UserPromptSubmit, and SessionEnd groups.
var managedHooks = hookcore.ManagedHooksSpec{
	Label:          harnessLabel,
	HookSubcommand: "claude-hook",
	InstallHint:    "waypost install claude-hook",
	KnownEvent:     knownHookEvent,
	Events: []hookcore.ManagedEventSpec{
		{Event: "SessionStart", DoctorLabel: "SessionStart compact guard", TimeoutSeconds: hookTimeoutSeconds, EligibleGroup: matcherFiresForNonToolEvent},
		{Event: "UserPromptSubmit", DoctorLabel: "UserPromptSubmit nudge", TimeoutSeconds: hookTimeoutSeconds, EligibleGroup: matcherFiresForNonToolEvent},
		{
			Event: "PreToolUse", DoctorLabel: "PreToolUse wait guard", TimeoutSeconds: hookTimeoutSeconds, Matcher: "^Bash$",
			EligibleGroup: func(group map[string]any) bool {
				return hookcore.MatcherTargetsOnly(group["matcher"], shellToolName)
			},
		},
		{
			Event: "PostToolUse", DoctorLabel: "PostToolUse receive tracker", TimeoutSeconds: hookTimeoutSeconds,
			EligibleGroup: func(group map[string]any) bool {
				return hookcore.MatcherTargetsReceiveCompletionOnly(group["matcher"], shellToolName, receiveMCPToolName,
					[]string{"Edit", "Write", "mcp__waypost__waypost_send", "mcp__waypost__waypost_status"}) ||
					hookcore.MatcherFiresFor(group, "Edit")
			},
		},
		{Event: "SessionEnd", DoctorLabel: "SessionEnd cleanup", TimeoutSeconds: cleanupHookTimeoutSeconds, EligibleGroup: matcherFiresForNonToolEvent},
	},
}

func WriteOutput(w io.Writer) error {
	return hookcore.WriteHookContext(w, harnessSpec.FallbackEvent, AdditionalContext)
}

func writeOutputWithSystemMessage(w io.Writer, eventName, additionalContext, systemMessage string) error {
	return json.NewEncoder(w).Encode(hookOutput{
		SystemMessage: systemMessage,
		HookSpecificOutput: &hookSpecificOutput{
			HookEventName:     eventName,
			AdditionalContext: additionalContext,
		},
	})
}

func writeSystemMessage(w io.Writer, systemMessage string) error {
	return json.NewEncoder(w).Encode(hookOutput{SystemMessage: systemMessage})
}

func writeDenyOutput(w io.Writer, reason string) error {
	return json.NewEncoder(w).Encode(hookOutput{
		HookSpecificOutput: &hookSpecificOutput{
			HookEventName:            "PreToolUse",
			PermissionDecision:       "deny",
			PermissionDecisionReason: reason,
		},
	})
}

type nudgeStateStore = hookcore.NudgeStore

type fileNudgeStateStore = hookcore.FileNudgeStore

type memoryNudgeStateStore = hookcore.MemoryNudgeStore

func newMemoryNudgeStateStore() *memoryNudgeStateStore {
	store := hookcore.NewMemoryNudgeStore()
	store.Label = harnessLabel
	return store
}

func defaultNudgeStateStore() (nudgeStateStore, error) {
	dir, err := DefaultConfigDir()
	if err != nil {
		return nil, err
	}
	return fileNudgeStateStore{Dir: filepath.Join(dir, hookStateDirectoryName), Label: harnessLabel}, nil
}

func successfulWaypostReceive(input hookInput) bool {
	switch input.ToolName {
	case receiveMCPToolName:
		return successfulMCPResponse(input.ToolResponse)
	case shellToolName:
		command, err := bashCommand(input.ToolInput)
		if err != nil {
			return false
		}
		return hookcore.RunsWaypostReceive(command) && successfulShellResponse(input.ToolResponse)
	default:
		return false
	}
}

// successfulMCPResponse inspects a Claude Code PostToolUse tool_response for
// the waypost_recv MCP tool. Claude reports the MCP result object; its
// content blocks or structuredContent carry the serialized receive outcome.
func successfulMCPResponse(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	// A bare JSON string carries the serialized result on some versions.
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return hookcore.MCPReceiveResultSucceeded(text)
	}
	var response struct {
		IsError           bool   `json:"isError"`
		Status            string `json:"status"`
		StructuredContent struct {
			Status string `json:"status"`
		} `json:"structuredContent"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return false
	}
	if response.IsError {
		return false
	}
	if hookcore.IsReceiveStatus(response.StructuredContent.Status) || hookcore.IsReceiveStatus(response.Status) {
		return true
	}
	for _, block := range response.Content {
		if block.Type == "text" && hookcore.MCPReceiveResultSucceeded(block.Text) {
			return true
		}
	}
	return hookcore.MCPReceiveResultSucceeded(string(raw))
}

// successfulShellResponse inspects a Claude Code Bash tool_response for a
// completed `waypost recv` invocation. The response carries the command
// output in stdout (older shapes use a bare string or an output field).
func successfulShellResponse(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return hookcore.ReceiveOutputSucceeded(text)
	}
	var response struct {
		Stdout string `json:"stdout"`
		Output string `json:"output"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return false
	}
	output := response.Stdout
	if output == "" {
		output = response.Output
	}
	return hookcore.ReceiveOutputSucceeded(output)
}

func bashCommand(raw json.RawMessage) (string, error) {
	return hookcore.CommandToolInput(raw, "Claude Code Bash")
}

// DefaultConfigDir returns the directory Claude Code reads user-level
// settings from: $CLAUDE_CONFIG_DIR when set, else ~/.claude.
func DefaultConfigDir() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("CLAUDE_CONFIG_DIR")); configured != "" {
		return filepath.Abs(configured)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home for Claude Code hooks: %w", err)
	}
	return filepath.Join(home, ".claude"), nil
}

func CurrentCommand() (string, error) {
	executable, err := launchpath.CurrentExecutable()
	if err != nil {
		return "", fmt.Errorf("resolve waypost executable: %w", err)
	}
	command := hookcore.QuoteCommandPath(executable) + " claude-hook"
	if runtime.GOOS == "windows" {
		command = "& " + command
	}
	return command, nil
}

func Install(configDir, command string) (InstallResult, error) {
	return hookcore.InstallManagedHooks(filepath.Join(configDir, configFileName), managedHooks, command)
}

func Doctor(configDir, command string) (DoctorResult, error) {
	return hookcore.DoctorManagedHooks(filepath.Join(configDir, configFileName), managedHooks, command)
}

func matcherFiresForNonToolEvent(group map[string]any) bool {
	return hookcore.MatcherFiresFor(group, "")
}

func knownHookEvent(name string) bool {
	switch name {
	case "PreToolUse", "PostToolUse", "PostToolUseFailure", "PostToolBatch",
		"PermissionRequest", "PermissionDenied", "UserPromptSubmit",
		"UserPromptExpansion", "Notification", "MessageDisplay", "Stop",
		"StopFailure", "SubagentStart", "SubagentStop", "TaskCreated",
		"TaskCompleted", "TeammateIdle", "InstructionsLoaded", "Setup",
		"SessionStart", "SessionEnd", "PreCompact":
		return true
	default:
		return false
	}
}
