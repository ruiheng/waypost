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
	"strings"

	"github.com/ruiheng/waypost/internal/devinconfig"
	"github.com/ruiheng/waypost/internal/hookcore"
	"github.com/ruiheng/waypost/internal/launchpath"
)

const (
	execToolName           = "exec"
	receiveMCPToolName     = "mcp__waypost__waypost_recv"
	receiveWaypostToolName = "waypost_recv"
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

type InstallResult struct {
	Path     string
	Changed  bool
	Warnings []string
}

type DoctorResult struct {
	Path    string
	Command string
}

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
	input, hasInput, err := hookcore.ReadHookInput(r, harnessLabel)
	if err != nil {
		return err
	}
	if !hasInput {
		return writeContextOutput(w, "PostCompaction", AdditionalContext)
	}

	// Devin guarantees a stable session_id in every hook payload; a missing
	// value means the event came from a different producer, so state tracking
	// degrades gracefully instead of failing the hook.
	sessionID := strings.TrimSpace(input.SessionID)
	switch input.HookEventName {
	case "SessionStart", "PostCompaction":
		if input.HookEventName == "SessionStart" && input.Source != "compact" {
			return nil
		}
		if sessionID == "" {
			return nil
		}
		state, err := store.Load(sessionID)
		if err != nil {
			return err
		}
		if state != nudgeConsumed {
			return nil
		}
		return writeContextOutput(w, input.HookEventName, AdditionalContext)
	case "UserPromptSubmit":
		if !LooksLikeWaypostNudge(input.Prompt) {
			if sessionID == "" {
				return nil
			}
			return store.Clear(sessionID)
		}
		if sessionID != "" {
			if err := store.Save(sessionID, nudgePending); err != nil {
				return err
			}
		}
		mcpStatus, probeErr := probeWaypostMCP(ctx, sessionID, probe, store)
		switch {
		case probeErr != nil:
			return writeContextOutput(w, "UserPromptSubmit", hookcore.MCPProbeFailureMessage(probeErr)+" "+MCPProbeFailedNudgeContext)
		case mcpStatus.toolUsable(receiveWaypostToolName):
			return writeContextOutput(w, "UserPromptSubmit", MCPNudgeContext)
		default:
			return writeContextOutput(w, "UserPromptSubmit", CLINudgeContext)
		}
	case "PostToolUse":
		if !successfulWaypostReceive(input) || sessionID == "" {
			return nil
		}
		state, err := store.Load(sessionID)
		if err != nil {
			return err
		}
		if state != nudgePending {
			return nil
		}
		return store.Save(sessionID, nudgeConsumed)
	case "PreToolUse":
		if input.ToolName != execToolName {
			return nil
		}
		command, err := execCommand(input.ToolInput)
		if err != nil {
			return err
		}
		if LooksLikeWaypostWaitCommand(command) {
			return writeContextOutput(w, "PreToolUse", WaitPollingContext)
		}
		denialReason, tool, guarded := waypostMCPDenialReason(command)
		if !guarded {
			return nil
		}
		if tool == "" {
			// `waypost mcp` is always denied because Devin manages the server.
			return writeDenyOutput(w, denialReason)
		}
		mcpStatus, probeErr := probeWaypostMCP(ctx, sessionID, probe, store)
		if probeErr != nil {
			return writeContextOutput(w, "PreToolUse", hookcore.MCPProbeFailureMessage(probeErr))
		}
		if !mcpStatus.toolUsable(tool) {
			// The MCP tool is absent or disabled; keep the CLI fallback usable.
			return nil
		}
		return writeDenyOutput(w, denialReason)
	case "SessionEnd":
		if sessionID == "" {
			return nil
		}
		if err := store.Clear(sessionID); err != nil {
			return err
		}
		return store.ClearMCPProbe(sessionID)
	default:
		return nil
	}
}

func WriteOutput(w io.Writer) error {
	return writeContextOutput(w, "PostCompaction", AdditionalContext)
}

func writeContextOutput(w io.Writer, eventName, additionalContext string) error {
	return json.NewEncoder(w).Encode(hookOutput{
		HookSpecificOutput: &hookSpecificOutput{
			HookEventName:     eventName,
			AdditionalContext: additionalContext,
		},
	})
}

func writeDenyOutput(w io.Writer, reason string) error {
	return json.NewEncoder(w).Encode(hookOutput{
		Decision: "block",
		Reason:   reason,
	})
}

func LooksLikeWaypostNudge(prompt string) bool {
	return hookcore.LooksLikeWaypostNudge(prompt)
}

type nudgeState = hookcore.NudgeState

const (
	nudgeNone     = hookcore.NudgeNone
	nudgePending  = hookcore.NudgePending
	nudgeConsumed = hookcore.NudgeConsumed
)

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

func waypostMCPStatusFromRecord(record hookcore.MCPProbeRecord) waypostMCPStatus {
	status := waypostMCPStatus{available: record.Available, disabledTools: map[string]bool{}}
	for _, tool := range record.DisabledTools {
		// Cached records written before namespaced tool ids were normalized,
		// or produced externally, are canonicalized here as well.
		status.disabledTools[canonicalWaypostToolName(tool)] = true
	}
	return status
}

func probeWaypostMCP(ctx context.Context, sessionID string, probe waypostMCPProbe, store nudgeStateStore) (waypostMCPStatus, error) {
	// Devin always sends session_id, but non-Devin producers may omit it.
	// Without an ID there is nothing to cache against, so simply probe.
	record, err := hookcore.ProbeAndCache(ctx, sessionID, func(ctx context.Context) (hookcore.MCPProbeRecord, error) {
		status, err := probe(ctx)
		if err != nil {
			return hookcore.MCPProbeRecord{}, err
		}
		return status.probeRecord(), nil
	}, store)
	if err != nil {
		return waypostMCPStatus{}, err
	}
	return waypostMCPStatusFromRecord(record), nil
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
		subcommand, ok := directWaypostCommand(command)
		return ok && (subcommand == "recv" || subcommand == "receive") && successfulExecResponse(input.ToolResponse)
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

func LooksLikeWaypostWaitCommand(command string) bool {
	return hookcore.LooksLikeWaypostWaitCommand(command)
}

// waypostMCPDenialReason maps a direct `waypost <subcommand>` invocation to
// the MCP tool that replaces it. tool is empty for commands that have no MCP
// equivalent and are always denied (`waypost mcp`).
func waypostMCPDenialReason(command string) (reason, tool string, guarded bool) {
	tool, guarded = hookcore.WaypostMCPTool(command)
	if !guarded {
		return "", "", false
	}
	return hookcore.WaypostCommandDenialReason(tool, MCPServerCommandDenialReason), tool, true
}

func directWaypostCommand(command string) (string, bool) {
	return hookcore.DirectWaypostCommand(command)
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
	path := filepath.Join(configDir, configFileName)
	document, mode, wasJSONC, err := hookcore.ReadConfigDocument(path, harnessLabel, true)
	if err != nil {
		return InstallResult{}, err
	}

	hooks, err := hookcore.ObjectField(document, "hooks")
	if err != nil {
		return InstallResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	if err := hookcore.ValidateHooksStructure(hooks, knownHookEvent); err != nil {
		return InstallResult{}, fmt.Errorf("validate %q: %w", path, err)
	}

	changed := false
	for _, spec := range managedHookSpecs(command) {
		groups, err := hookcore.ArrayField(hooks, spec.event)
		if err != nil {
			return InstallResult{}, fmt.Errorf("read %q: %w", path, err)
		}
		updated, eventChanged := hookcore.MergeManagedHandler(groups, spec.desired(command), spec.recognition())
		hooks[spec.event] = updated
		changed = changed || eventChanged
	}
	document["hooks"] = hooks

	if !changed {
		return InstallResult{Path: path, Changed: false}, nil
	}
	if err := hookcore.WriteConfigDocument(path, document, mode, harnessLabel); err != nil {
		return InstallResult{}, err
	}
	result := InstallResult{Path: path, Changed: true}
	if wasJSONC {
		result.Warnings = append(result.Warnings, fmt.Sprintf("%s contained JSONC comments or trailing commas; the rewrite emitted strict JSON and removed them", path))
	}
	return result, nil
}

func Doctor(configDir, command string) (DoctorResult, error) {
	path := filepath.Join(configDir, configFileName)
	document, _, _, err := hookcore.ReadExistingConfigDocument(path, harnessLabel, true)
	if err != nil {
		return DoctorResult{}, err
	}
	hooks, err := hookcore.ExistingObjectField(document, "hooks")
	if err != nil {
		return DoctorResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	if err := hookcore.ValidateHooksStructure(hooks, knownHookEvent); err != nil {
		return DoctorResult{}, fmt.Errorf("validate %q: %w", path, err)
	}
	for _, spec := range managedHookSpecs(command) {
		groups, err := hookcore.ArrayField(hooks, spec.event)
		if err != nil {
			return DoctorResult{}, fmt.Errorf("read %q: %w", path, err)
		}
		installed := false
		for _, item := range groups {
			group, ok := item.(map[string]any)
			if !ok || !spec.eligibleGroup(group) {
				continue
			}
			if hookcore.GroupHasCommandWithTimeout(group, command, spec.timeout) {
				installed = true
				break
			}
		}
		if !installed {
			return DoctorResult{}, fmt.Errorf("Devin %s hook is not installed in %q; run `waypost install devin-hook`", spec.event, path)
		}
	}
	return DoctorResult{Path: path, Command: command}, nil
}

type managedHandlerSpec struct {
	event         string
	command       string
	timeout       int64
	desired       func(command string) map[string]any
	eligibleGroup func(group map[string]any) bool
}

func (spec managedHandlerSpec) recognition() hookcore.ManagedHandlerSpec {
	return hookcore.ManagedHandlerSpec{
		Command:        spec.command,
		HookSubcommand: "devin-hook",
		EligibleGroup:  spec.eligibleGroup,
	}
}

func managedHookSpecs(command string) []managedHandlerSpec {
	specs := []managedHandlerSpec{
		{
			event:   "PostCompaction",
			timeout: hookTimeoutSeconds,
			desired: func(command string) map[string]any {
				return commandGroup(nil, command, hookTimeoutJSON)
			},
			eligibleGroup: matcherFiresForNonToolEvent,
		},
		{
			event:   "SessionStart",
			timeout: hookTimeoutSeconds,
			desired: func(command string) map[string]any {
				return commandGroup(nil, command, hookTimeoutJSON)
			},
			eligibleGroup: matcherFiresForNonToolEvent,
		},
		{
			event:   "UserPromptSubmit",
			timeout: hookTimeoutSeconds,
			desired: func(command string) map[string]any {
				return commandGroup(nil, command, hookTimeoutJSON)
			},
			eligibleGroup: matcherFiresForNonToolEvent,
		},
		{
			event:   "PreToolUse",
			timeout: hookTimeoutSeconds,
			desired: func(command string) map[string]any {
				return commandGroup("^exec$", command, hookTimeoutJSON)
			},
			eligibleGroup: func(group map[string]any) bool {
				return matcherTargetsExecOnly(group["matcher"])
			},
		},
		{
			event:   "PostToolUse",
			timeout: hookTimeoutSeconds,
			desired: func(command string) map[string]any {
				return commandGroup("^(exec|mcp__waypost__waypost_recv)$", command, hookTimeoutJSON)
			},
			eligibleGroup: func(group map[string]any) bool {
				return matcherTargetsReceiveCompletionOnly(group["matcher"])
			},
		},
		{
			event:   "SessionEnd",
			timeout: cleanupHookTimeoutSeconds,
			desired: func(command string) map[string]any {
				return commandGroup(nil, command, cleanupHookTimeoutJSON)
			},
			eligibleGroup: matcherFiresForNonToolEvent,
		},
	}
	for index := range specs {
		specs[index].command = command
	}
	return specs
}

func commandGroup(matcher any, command string, timeout json.Number) map[string]any {
	group := map[string]any{
		"hooks": []any{
			map[string]any{
				"type":    "command",
				"command": command,
				"timeout": timeout,
			},
		},
	}
	if matcher != nil {
		group["matcher"] = matcher
	}
	return group
}

func matcherFiresForNonToolEvent(group map[string]any) bool {
	return hookcore.MatcherFiresForNonToolEvent(group, "")
}

func matcherTargetsExecOnly(value any) bool {
	return hookcore.MatcherTargetsOnly(value, execToolName)
}

func matcherTargetsReceiveCompletionOnly(value any) bool {
	return hookcore.MatcherTargetsReceiveCompletionOnly(value, execToolName, receiveMCPToolName,
		[]string{"edit", "mcp__waypost__waypost_send", "mcp__waypost__waypost_status"})
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
