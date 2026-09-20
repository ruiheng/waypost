package devinhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/ruiheng/waypost/internal/devinconfig"
	"github.com/ruiheng/waypost/internal/hookcore"
	"github.com/ruiheng/waypost/internal/launchpath"
)

const (
	execToolName           = "exec"
	receiveMCPToolName     = "mcp__waypost__waypost_recv"
	hookStateDirectoryName = "waypost-hook-state"
	configFileName         = "config.json"
	defaultNudgeMessage    = hookcore.DefaultNudgeMessage
	harnessLabel           = "Devin"
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
const MCPServerCommandDenialReason = `The Waypost MCP server is managed by Devin. Never run the Waypost CLI command ` + "`waypost mcp`" + `.`

type hookInput = hookcore.HookInput

type hookOutput struct {
	Decision           string              `json:"decision,omitempty"`
	Reason             string              `json:"reason,omitempty"`
	HookSpecificOutput *hookSpecificOutput `json:"hookSpecificOutput,omitempty"`
}

type hookSpecificOutput struct {
	HookEventName     string `json:"hookEventName"`
	AdditionalContext string `json:"additionalContext,omitempty"`
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
	return runWithDependencies(ctx, r, w, func(ctx context.Context) (waypostMCPStatus, error) {
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
		status, err := probe(ctx)
		if err != nil {
			return hookcore.MCPProbeRecord{}, err
		}
		return status.probeRecord(), nil
	}
}

// harnessSpec declares Devin's variation of the shared Waypost hook
// lifecycle: Devin runs a PostCompaction event but drops its
// additionalContext, so the compact guard is marked pending there and
// re-injected through the next honored event. The shell tool is exec and
// denials use the decision/block envelope.
var harnessSpec = hookcore.HarnessSpec{
	Label:               harnessLabel,
	ShellTool:           execToolName,
	CompactSource:       "compact",
	PostCompactionEvent: "PostCompaction",
	FallbackEvent:       "PostCompaction",
	ServerDenialReason:  MCPServerCommandDenialReason,
	EmitDeny:            writeDenyOutput,
	EmitProbeFailure: func(w io.Writer, probeErr error) error {
		return hookcore.WriteHookContext(w, "PreToolUse", hookcore.MCPProbeFailureMessage(probeErr))
	},
	EmitNudgeProbeFailure: func(w io.Writer, probeErr error) error {
		return hookcore.WriteHookContext(w, "UserPromptSubmit", hookcore.MCPProbeFailureMessage(probeErr)+" "+MCPProbeFailedNudgeContext)
	},
	ReceiveSucceeded: successfulWaypostReceive,
}

// managedHooks declares Devin's managed hook groups. PostToolUse carries no
// matcher: Devin drops PostCompaction additionalContext, so the compact
// guard is delivered through the next PostToolUse event and the group must
// match every tool, not only receive completions.
var managedHooks = hookcore.ManagedHooksSpec{
	Label:          harnessLabel,
	HookSubcommand: "devin-hook",
	InstallHint:    "waypost install devin-hook",
	AllowJSONC:     true,
	KnownEvent:     knownHookEvent,
	Events: []hookcore.ManagedEventSpec{
		{Event: "PostCompaction", DoctorLabel: "PostCompaction hook", TimeoutSeconds: hookTimeoutSeconds, EligibleGroup: matcherFiresForNonToolEvent},
		{Event: "SessionStart", DoctorLabel: "SessionStart hook", TimeoutSeconds: hookTimeoutSeconds, EligibleGroup: matcherFiresForNonToolEvent},
		{Event: "UserPromptSubmit", DoctorLabel: "UserPromptSubmit hook", TimeoutSeconds: hookTimeoutSeconds, EligibleGroup: matcherFiresForNonToolEvent},
		{
			Event: "PreToolUse", DoctorLabel: "PreToolUse hook", TimeoutSeconds: hookTimeoutSeconds, Matcher: "^exec$",
			EligibleGroup: func(group map[string]any) bool {
				return hookcore.MatcherTargetsOnly(group["matcher"], execToolName)
			},
		},
		{
			Event: "PostToolUse", DoctorLabel: "PostToolUse hook", TimeoutSeconds: hookTimeoutSeconds,
			EligibleGroup: func(group map[string]any) bool {
				return hookcore.MatcherTargetsReceiveCompletionOnly(group["matcher"], execToolName, receiveMCPToolName,
					[]string{"edit", "mcp__waypost__waypost_send", "mcp__waypost__waypost_status"}) ||
					hookcore.MatcherFiresFor(group, "edit")
			},
		},
		{Event: "SessionEnd", DoctorLabel: "SessionEnd hook", TimeoutSeconds: cleanupHookTimeoutSeconds, EligibleGroup: matcherFiresForNonToolEvent},
	},
}

func WriteOutput(w io.Writer) error {
	return hookcore.WriteHookContext(w, harnessSpec.FallbackEvent, AdditionalContext)
}

func writeDenyOutput(w io.Writer, reason string) error {
	return json.NewEncoder(w).Encode(hookOutput{
		Decision: "block",
		Reason:   reason,
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

func (status waypostMCPStatus) probeRecord() hookcore.MCPProbeRecord {
	disabled := make([]string, 0, len(status.disabledTools))
	for tool := range status.disabledTools {
		disabled = append(disabled, tool)
	}
	sort.Strings(disabled)
	return hookcore.MCPProbeRecord{Available: status.available, DisabledTools: disabled}
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
		return successfulDevinMCPResponse(input.ToolResponse)
	case execToolName:
		command, err := execCommand(input.ToolInput)
		if err != nil {
			return false
		}
		return hookcore.RunsWaypostReceive(command) && successfulExecResponse(input.ToolResponse)
	default:
		return false
	}
}

// successfulDevinMCPResponse inspects a Devin PostToolUse tool_response for an
// MCP tool. Devin reports {success, output, error}; the output carries the
// serialized MCP result, whose shape depends on the transport.
func successfulDevinMCPResponse(raw json.RawMessage) bool {
	response, ok := decodeToolResponse(raw)
	if !ok || (response.Success != nil && !*response.Success) {
		return false
	}
	return hookcore.MCPReceiveResultSucceeded(response.Output)
}

func successfulExecResponse(raw json.RawMessage) bool {
	response, ok := decodeToolResponse(raw)
	if !ok || (response.Success != nil && !*response.Success) {
		return false
	}
	return hookcore.ReceiveOutputSucceeded(response.Output)
}

type devinToolResponse struct {
	Success *bool  `json:"success"`
	Output  string `json:"output"`
}

func decodeToolResponse(raw json.RawMessage) (devinToolResponse, bool) {
	if len(raw) == 0 {
		return devinToolResponse{}, false
	}
	var response devinToolResponse
	if err := json.Unmarshal(raw, &response); err != nil {
		return devinToolResponse{}, false
	}
	return response, true
}

func execCommand(raw json.RawMessage) (string, error) {
	return hookcore.CommandToolInput(raw, "Devin exec")
}

func DefaultConfigDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home for Devin hooks: %w", err)
	}
	return devinconfig.UserConfigDir(home), nil
}

func CurrentCommand() (string, error) {
	executable, err := launchpath.CurrentExecutable()
	if err != nil {
		return "", fmt.Errorf("resolve waypost executable: %w", err)
	}
	command := hookcore.QuoteCommandPath(executable) + " devin-hook"
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
	case "PreToolUse", "PostToolUse", "PermissionRequest", "UserPromptSubmit",
		"Stop", "PostCompaction", "SessionStart", "SessionEnd":
		return true
	default:
		return false
	}
}
