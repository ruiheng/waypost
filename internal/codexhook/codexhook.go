package codexhook

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
	compactManagedDescription = "Waypost Codex compact-context guard"
	promptManagedDescription  = "Waypost Codex nudge MCP hint"
	waitManagedDescription    = "Waypost Codex wait polling guard"
	receiveManagedDescription = "Waypost Codex receive completion tracker"
	cleanupManagedDescription = "Waypost Codex nudge state cleanup"
	compactStatusMessage      = "Restoring Waypost compact context"
	promptStatusMessage       = "Preparing Waypost receive hint"
	waitStatusMessage         = "Checking Waypost wait usage"
	legacyPromptStatusMessage = "Checking Waypost MCP availability"
	receiveMCPToolName        = "mcp__waypost__waypost_recv"
	defaultNudgeMessage       = hookcore.DefaultNudgeMessage
	hookStateDirectoryName    = "waypost-hook-state"
	harnessLabel              = "Codex"
)

const hookTimeoutSeconds int64 = 5
const hookTimeoutJSON json.Number = "5"
const cleanupHookTimeoutSeconds int64 = 3
const cleanupHookTimeoutJSON json.Number = "3"

const AdditionalContext = hookcore.AdditionalContext
const MCPNudgeContext = hookcore.MCPNudgeContext
const CLINudgeContext = hookcore.CLINudgeContext
const MCPProbeFailedNudgeContext = hookcore.MCPProbeFailedNudgeContext
const WaitPollingContext = hookcore.WaitPollingContext
const MCPStatusDenialReason = hookcore.StatusDenialReason
const MCPServerCommandDenialReason = `The Waypost MCP server is managed by Codex. Never run the Waypost CLI command ` + "`waypost mcp`" + `.`

type hookInput = hookcore.HookInput

type hookOutput struct {
	SystemMessage      string             `json:"systemMessage,omitempty"`
	HookSpecificOutput hookSpecificOutput `json:"hookSpecificOutput"`
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
	return runWithDependencies(ctx, r, w, CurrentDirectoryWaypostMCPAvailable, store)
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

// harnessSpec declares Codex's variation of the shared Waypost hook
// lifecycle: compaction arrives as SessionStart with source "compact" and
// its context is honored, so no PostCompaction event or pending guard is
// declared. The shell tool is Bash; denials use the permissionDecision
// envelope and probe failures surface as systemMessage.
var harnessSpec = hookcore.HarnessSpec{
	Label:              harnessLabel,
	ShellTool:          "Bash",
	CompactSource:      "compact",
	FallbackEvent:      "SessionStart",
	ServerDenialReason: MCPServerCommandDenialReason,
	EmitDeny:           writeDenyOutput,
	EmitProbeFailure: func(w io.Writer, probeErr error) error {
		return writeSystemMessage(w, hookcore.MCPProbeFailureMessage(probeErr))
	},
	EmitNudgeProbeFailure: func(w io.Writer, probeErr error) error {
		return writeOutputWithSystemMessage(w, "UserPromptSubmit", MCPProbeFailedNudgeContext, hookcore.MCPProbeFailureMessage(probeErr))
	},
	EmitInputTimeout: func(w io.Writer) error {
		return writeSystemMessage(w, "Waypost Codex hook input timed out waiting for the harness payload")
	},
	ReceiveSucceeded: successfulWaypostReceive,
}

// managedHooks declares Codex's managed hook groups. PostToolUse keeps a
// receive-only matcher: Codex honors SessionStart compact context, so the
// compact guard is emitted directly and PostToolUse only needs to track
// receive completion.
var managedHooks = hookcore.ManagedHooksSpec{
	Label:          harnessLabel,
	HookSubcommand: "codex-hook",
	InstallHint:    "waypost install codex-hook",
	KnownEvent:     knownHookEvent,
	Events: []hookcore.ManagedEventSpec{
		{
			Event: "SessionStart", DoctorLabel: "compact", TimeoutSeconds: hookTimeoutSeconds,
			Matcher: "^compact$", Description: compactManagedDescription,
			StatusMessages: []string{compactStatusMessage},
			EligibleGroup: func(group map[string]any) bool {
				return hookcore.MatcherTargetsCompactOnly(group["matcher"])
			},
		},
		{
			Event: "UserPromptSubmit", DoctorLabel: "Waypost nudge", TimeoutSeconds: hookTimeoutSeconds,
			Description:    promptManagedDescription,
			StatusMessages: []string{promptStatusMessage, legacyPromptStatusMessage},
			EligibleGroup:  func(map[string]any) bool { return true },
		},
		{
			Event: "PreToolUse", DoctorLabel: "Waypost wait polling guard", TimeoutSeconds: hookTimeoutSeconds,
			Matcher: "^Bash$", Description: waitManagedDescription,
			StatusMessages: []string{waitStatusMessage},
			EligibleGroup: func(group map[string]any) bool {
				return hookcore.MatcherTargetsOnly(group["matcher"], "Bash")
			},
		},
		{
			Event: "PostToolUse", DoctorLabel: "Waypost receive completion", TimeoutSeconds: hookTimeoutSeconds,
			Matcher: "^(Bash|mcp__waypost__waypost_recv)$", Description: receiveManagedDescription,
			EligibleGroup: func(group map[string]any) bool {
				return hookcore.MatcherTargetsReceiveCompletionOnly(group["matcher"], "Bash", receiveMCPToolName,
					[]string{"apply_patch", "mcp__waypost__waypost_send", "mcp__waypost__waypost_status"})
			},
		},
		{
			Event: "SessionEnd", DoctorLabel: "Waypost nudge state cleanup", TimeoutSeconds: cleanupHookTimeoutSeconds,
			Description: cleanupManagedDescription,
			EligibleGroup: func(group map[string]any) bool {
				return hookcore.MatcherFiresFor(group, "other")
			},
		},
	},
}

func WriteOutput(w io.Writer) error {
	return hookcore.WriteHookContext(w, harnessSpec.FallbackEvent, AdditionalContext)
}

func writeOutputWithSystemMessage(w io.Writer, eventName, additionalContext, systemMessage string) error {
	return json.NewEncoder(w).Encode(hookOutput{
		SystemMessage: systemMessage,
		HookSpecificOutput: hookSpecificOutput{
			HookEventName:     eventName,
			AdditionalContext: additionalContext,
		},
	})
}

func writeSystemMessage(w io.Writer, systemMessage string) error {
	return json.NewEncoder(w).Encode(struct {
		SystemMessage string `json:"systemMessage"`
	}{SystemMessage: systemMessage})
}

func writeDenyOutput(w io.Writer, reason string) error {
	return json.NewEncoder(w).Encode(hookOutput{
		HookSpecificOutput: hookSpecificOutput{
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
	home, err := DefaultHome()
	if err != nil {
		return nil, err
	}
	return fileNudgeStateStore{Dir: filepath.Join(home, hookStateDirectoryName), Label: harnessLabel}, nil
}

func successfulWaypostReceive(input hookInput) bool {
	switch input.ToolName {
	case receiveMCPToolName:
		return hookcore.MCPReceiveResultSucceeded(string(input.ToolResponse))
	case "Bash":
		command, err := bashCommand(input.ToolInput)
		if err != nil {
			return false
		}
		subcommand, ok := directWaypostCommand(command)
		return ok && (subcommand == "recv" || subcommand == "receive") && hookcore.ShellReceiveOutputSucceeded(input.ToolResponse)
	default:
		return false
	}
}

func directWaypostCommand(command string) (string, bool) {
	return hookcore.DirectWaypostCommand(command)
}

func bashCommand(raw json.RawMessage) (string, error) {
	return hookcore.CommandToolInput(raw, "Codex Bash")
}

func DefaultHome() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("CODEX_HOME")); configured != "" {
		return filepath.Abs(configured)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home for Codex hooks: %w", err)
	}
	return filepath.Join(home, ".codex"), nil
}

func CurrentCommand() (string, error) {
	executable, err := launchpath.CurrentExecutable()
	if err != nil {
		return "", fmt.Errorf("resolve waypost executable: %w", err)
	}
	command := hookcore.QuoteCommandPath(executable) + " codex-hook"
	if runtime.GOOS == "windows" {
		command = "& " + command
	}
	return command, nil
}

func Install(codexHome, command string) (InstallResult, error) {
	return hookcore.InstallManagedHooks(filepath.Join(codexHome, "hooks.json"), managedHooks, command)
}

func Doctor(codexHome, command string) (DoctorResult, error) {
	return hookcore.DoctorManagedHooks(filepath.Join(codexHome, "hooks.json"), managedHooks, command)
}

func knownHookEvent(name string) bool {
	switch name {
	case "PreToolUse", "PermissionRequest", "PostToolUse", "PreCompact", "PostCompact",
		"SessionStart", "SessionEnd", "UserPromptSubmit", "SubagentStart", "SubagentStop",
		"Stop", "Interrupt":
		return true
	default:
		return false
	}
}
