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

type InstallResult struct {
	Path    string
	Changed bool
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
	input, hasInput, err := hookcore.ReadHookInput(r, harnessLabel)
	if err != nil {
		return err
	}
	if !hasInput {
		return writeOutput(w, "SessionStart", AdditionalContext)
	}

	// Codex sends a stable session_id in every hook payload; a missing value
	// means the event came from a different producer, so state tracking
	// degrades gracefully instead of failing the hook.
	sessionID := strings.TrimSpace(input.SessionID)
	switch input.HookEventName {
	case "SessionStart":
		if input.Source != "compact" {
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
		return writeOutput(w, "SessionStart", AdditionalContext)
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
		availability, probeErr := probeWaypostMCP(ctx, sessionID, probe, store)
		switch availability {
		case waypostMCPUnknown:
			return writeOutputWithSystemMessage(w, "UserPromptSubmit", MCPProbeFailedNudgeContext, hookcore.MCPProbeFailureMessage(probeErr))
		case waypostMCPAvailable:
			return writeOutput(w, "UserPromptSubmit", MCPNudgeContext)
		default:
			return writeOutput(w, "UserPromptSubmit", CLINudgeContext)
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
		if input.ToolName != "Bash" {
			return nil
		}
		command, err := bashCommand(input.ToolInput)
		if err != nil {
			return err
		}
		if LooksLikeWaypostWaitCommand(command) {
			return writeOutput(w, "PreToolUse", WaitPollingContext)
		}
		denialReason, guarded := waypostMCPDenialReason(command)
		if !guarded {
			return nil
		}
		if waypostMCPCommandAlwaysDenied(command) {
			return writeDenyOutput(w, denialReason)
		}
		availability, probeErr := probeWaypostMCP(ctx, sessionID, probe, store)
		if availability == waypostMCPUnknown {
			return writeSystemMessage(w, hookcore.MCPProbeFailureMessage(probeErr))
		}
		if availability == waypostMCPUnavailable {
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
	return writeOutput(w, "SessionStart", AdditionalContext)
}

func writeOutput(w io.Writer, eventName, additionalContext string) error {
	return writeOutputWithSystemMessage(w, eventName, additionalContext, "")
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

func probeWaypostMCP(ctx context.Context, sessionID string, probe waypostMCPProbe, store nudgeStateStore) (waypostMCPAvailability, error) {
	record, err := hookcore.ProbeAndCache(ctx, sessionID, func(ctx context.Context) (hookcore.MCPProbeRecord, error) {
		availability, err := detectWaypostMCP(ctx, probe)
		if err != nil {
			return hookcore.MCPProbeRecord{}, err
		}
		return hookcore.MCPProbeRecord{Available: availability == waypostMCPAvailable}, nil
	}, store)
	if err != nil {
		return waypostMCPUnknown, err
	}
	if record.Available {
		return waypostMCPAvailable, nil
	}
	return waypostMCPUnavailable, nil
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

func LooksLikeWaypostWaitCommand(command string) bool {
	return hookcore.LooksLikeWaypostWaitCommand(command)
}

func waypostMCPDenialReason(command string) (string, bool) {
	tool, guarded := hookcore.WaypostMCPTool(command)
	if !guarded {
		return "", false
	}
	return hookcore.WaypostCommandDenialReason(tool, MCPServerCommandDenialReason), true
}

func waypostMCPCommandAlwaysDenied(command string) bool {
	tool, guarded := hookcore.WaypostMCPTool(command)
	return guarded && tool == ""
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

// managedHookSpec describes one managed hook definition: which event it
// belongs to, the desired group contents, and how groups written by this
// installer are recognized.
type managedHookSpec struct {
	event          string
	doctorName     string
	timeoutSeconds int64
	description    string
	statusMessages []string
	desired        func(command string) map[string]any
	eligibleGroup  func(group map[string]any) bool
}

func managedHookSpecs() []managedHookSpec {
	return []managedHookSpec{
		{
			event:          "SessionStart",
			doctorName:     "compact",
			timeoutSeconds: hookTimeoutSeconds,
			description:    compactManagedDescription,
			statusMessages: []string{compactStatusMessage},
			desired:        compactManagedGroup,
			eligibleGroup: func(group map[string]any) bool {
				return hookcore.MatcherTargetsCompactOnly(group["matcher"])
			},
		},
		{
			event:          "UserPromptSubmit",
			doctorName:     "Waypost nudge",
			timeoutSeconds: hookTimeoutSeconds,
			description:    promptManagedDescription,
			statusMessages: []string{promptStatusMessage, legacyPromptStatusMessage},
			desired:        promptManagedGroup,
			eligibleGroup:  func(map[string]any) bool { return true },
		},
		{
			event:          "PreToolUse",
			doctorName:     "Waypost wait polling guard",
			timeoutSeconds: hookTimeoutSeconds,
			description:    waitManagedDescription,
			statusMessages: []string{waitStatusMessage},
			desired:        waitManagedGroup,
			eligibleGroup: func(group map[string]any) bool {
				return hookcore.MatcherTargetsOnly(group["matcher"], "Bash")
			},
		},
		{
			event:          "PostToolUse",
			doctorName:     "Waypost receive completion",
			timeoutSeconds: hookTimeoutSeconds,
			description:    receiveManagedDescription,
			desired:        receiveManagedGroup,
			eligibleGroup: func(group map[string]any) bool {
				return matcherTargetsReceiveCompletionOnly(group["matcher"])
			},
		},
		{
			event:          "SessionEnd",
			doctorName:     "Waypost nudge state cleanup",
			timeoutSeconds: cleanupHookTimeoutSeconds,
			description:    cleanupManagedDescription,
			desired:        cleanupManagedGroup,
			eligibleGroup:  matcherTargetsEverySessionEnd,
		},
	}
}

func (spec managedHookSpec) recognition(command string) hookcore.ManagedHandlerSpec {
	return hookcore.ManagedHandlerSpec{
		Description:    spec.description,
		StatusMessages: spec.statusMessages,
		Command:        command,
		HookSubcommand: "codex-hook",
		EligibleGroup:  spec.eligibleGroup,
	}
}

func Install(codexHome, command string) (InstallResult, error) {
	path := filepath.Join(codexHome, "hooks.json")
	document, mode, _, err := hookcore.ReadConfigDocument(path, harnessLabel, false)
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
	for _, spec := range managedHookSpecs() {
		groups, err := hookcore.ArrayField(hooks, spec.event)
		if err != nil {
			return InstallResult{}, fmt.Errorf("read %q: %w", path, err)
		}
		updated, eventChanged := hookcore.MergeManagedHandler(groups, spec.desired(command), spec.recognition(command))
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
	return InstallResult{Path: path, Changed: true}, nil
}

func Doctor(codexHome, command string) (DoctorResult, error) {
	path := filepath.Join(codexHome, "hooks.json")
	document, _, _, err := hookcore.ReadExistingConfigDocument(path, harnessLabel, false)
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
	for _, spec := range managedHookSpecs() {
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
			if hookcore.GroupHasCommandWithTimeout(group, command, spec.timeoutSeconds) {
				installed = true
				break
			}
		}
		if !installed {
			return DoctorResult{}, fmt.Errorf("Codex %s is not installed in %q; run `waypost install codex-hook`", spec.doctorName, path)
		}
	}
	return DoctorResult{Path: path, Command: command}, nil
}

func compactManagedGroup(command string) map[string]any {
	return map[string]any{
		"description": compactManagedDescription,
		"matcher":     "^compact$",
		"hooks": []any{
			map[string]any{
				"type":          "command",
				"command":       command,
				"statusMessage": compactStatusMessage,
				"timeout":       hookTimeoutJSON,
			},
		},
	}
}

func promptManagedGroup(command string) map[string]any {
	return map[string]any{
		"description": promptManagedDescription,
		"hooks": []any{
			map[string]any{
				"type":          "command",
				"command":       command,
				"statusMessage": promptStatusMessage,
				"timeout":       hookTimeoutJSON,
			},
		},
	}
}

func waitManagedGroup(command string) map[string]any {
	return map[string]any{
		"description": waitManagedDescription,
		"matcher":     "^Bash$",
		"hooks": []any{
			map[string]any{
				"type":          "command",
				"command":       command,
				"statusMessage": waitStatusMessage,
				"timeout":       hookTimeoutJSON,
			},
		},
	}
}

func receiveManagedGroup(command string) map[string]any {
	return map[string]any{
		"description": receiveManagedDescription,
		"matcher":     "^(Bash|mcp__waypost__waypost_recv)$",
		"hooks": []any{
			map[string]any{
				"type":    "command",
				"command": command,
				"timeout": hookTimeoutJSON,
			},
		},
	}
}

func cleanupManagedGroup(command string) map[string]any {
	return map[string]any{
		"description": cleanupManagedDescription,
		"hooks": []any{
			map[string]any{
				"type":    "command",
				"command": command,
				"timeout": cleanupHookTimeoutJSON,
			},
		},
	}
}

func matcherTargetsReceiveCompletionOnly(value any) bool {
	return hookcore.MatcherTargetsReceiveCompletionOnly(value, "Bash", receiveMCPToolName,
		[]string{"apply_patch", "mcp__waypost__waypost_send", "mcp__waypost__waypost_status"})
}

func matcherTargetsEverySessionEnd(group map[string]any) bool {
	return hookcore.MatcherFiresForNonToolEvent(group, "other")
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
