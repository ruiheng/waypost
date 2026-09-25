package claudehook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ruiheng/waypost/internal/hookcore"
)

func TestWriteOutputEmitsSessionStartAdditionalContext(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	if err := WriteOutput(&output); err != nil {
		t.Fatalf("WriteOutput() error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal(output) error = %v", err)
	}
	specific, ok := payload["hookSpecificOutput"].(map[string]any)
	if !ok {
		t.Fatalf("hookSpecificOutput = %#v, want object", payload["hookSpecificOutput"])
	}
	if got := specific["hookEventName"]; got != "SessionStart" {
		t.Fatalf("hookEventName = %v, want SessionStart", got)
	}
	context, _ := specific["additionalContext"].(string)
	if context != AdditionalContext {
		t.Fatalf("additionalContext = %q, want compact guard %q", context, AdditionalContext)
	}
}

// TestNudgeLifecycleControlsCompactGuard pins the Claude Code compact-guard
// lifecycle: a compact-source SessionStart emits the guard AND keeps it armed
// because Claude Code drops that event's additionalContext, so the next
// honored event delivers it again.
func TestNudgeLifecycleControlsCompactGuard(t *testing.T) {
	t.Parallel()

	const sessionID = "session-nudge-lifecycle"
	store := newMemoryNudgeStateStore()
	probe := func(context.Context) (bool, error) { return true, nil }

	if context := runCompactHook(t, store, sessionID); context != "" {
		t.Fatalf("compact context before nudge = %q, want empty", context)
	}
	output, emitted := runHook(t, store, probe, hookInput{
		HookEventName: "UserPromptSubmit",
		SessionID:     sessionID,
		Prompt:        defaultNudgeMessage,
	})
	if !emitted || output.HookSpecificOutput == nil || output.HookSpecificOutput.AdditionalContext != MCPNudgeContext {
		t.Fatalf("nudge output = %+v, %v; want MCP receive context", output, emitted)
	}
	if state, err := store.Load(sessionID); err != nil || state != hookcore.NudgePending {
		t.Fatalf("state after nudge = %q, %v; want pending", state, err)
	}
	if context := runCompactHook(t, store, sessionID); context != "" {
		t.Fatalf("compact context while pending = %q, want empty", context)
	}

	toolResponse := json.RawMessage(`{"structuredContent":{"status":"received"}}`)
	if output, emitted := runHook(t, store, probe, hookInput{
		HookEventName: "PostToolUse",
		SessionID:     sessionID,
		ToolName:      receiveMCPToolName,
		ToolResponse:  toolResponse,
	}); emitted {
		t.Fatalf("PostToolUse output = %+v, want empty", output)
	}
	if state, err := store.Load(sessionID); err != nil || state != hookcore.NudgeConsumed {
		t.Fatalf("state after receive = %q, %v; want consumed", state, err)
	}

	// The compact-source SessionStart emits the guard for harness versions
	// that honor it and keeps it armed for re-injection.
	if context := runCompactHook(t, store, sessionID); context != AdditionalContext {
		t.Fatalf("compact context = %q, want guard %q", context, AdditionalContext)
	}
	if state, err := store.Load(sessionID); err != nil || state != hookcore.NudgeGuardPending {
		t.Fatalf("state after compact = %q, %v; want guard pending", state, err)
	}
	// A second compact start while pending emits nothing and stays armed.
	if context := runCompactHook(t, store, sessionID); context != "" {
		t.Fatalf("compact context while guard pending = %q, want empty", context)
	}
	if state, err := store.Load(sessionID); err != nil || state != hookcore.NudgeGuardPending {
		t.Fatalf("state after second compact = %q, %v; want guard pending", state, err)
	}

	// The next honored event delivers the guard and settles the nudge.
	output, emitted = runHook(t, store, probe, hookInput{
		HookEventName: "PostToolUse",
		SessionID:     sessionID,
		ToolName:      "Edit",
	})
	if !emitted || output.HookSpecificOutput == nil || output.HookSpecificOutput.AdditionalContext != AdditionalContext {
		t.Fatalf("post-compaction PostToolUse output = %+v, %v; want compact guard", output, emitted)
	}
	if state, err := store.Load(sessionID); err != nil || state != hookcore.NudgeConsumed {
		t.Fatalf("state after guard delivery = %q, %v; want consumed", state, err)
	}

	if output, emitted := runHook(t, store, probe, hookInput{
		HookEventName: "UserPromptSubmit",
		SessionID:     sessionID,
		Prompt:        "Continue the original task.",
	}); emitted {
		t.Fatalf("ordinary prompt output = %+v, want empty", output)
	}
	if state, err := store.Load(sessionID); err != nil || state != hookcore.NudgeNone {
		t.Fatalf("state after ordinary prompt = %q, %v; want none", state, err)
	}
	if context := runCompactHook(t, store, sessionID); context != "" {
		t.Fatalf("compact context after ordinary prompt = %q, want empty", context)
	}
}

func TestPendingGuardDeliveredByOrdinaryPromptAndNonCompactStart(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		input     hookInput
		wantState hookcore.NudgeState
	}{
		{
			name: "ordinary prompt",
			input: hookInput{
				HookEventName: "UserPromptSubmit",
				Prompt:        "unrelated question",
			},
			wantState: hookcore.NudgeNone,
		},
		{
			name: "startup session start",
			input: hookInput{
				HookEventName: "SessionStart",
				Source:        "startup",
			},
			wantState: hookcore.NudgeConsumed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const sessionID = "session-guard-delivery"
			store := newMemoryNudgeStateStore()
			if err := store.Save(sessionID, hookcore.NudgeGuardPending); err != nil {
				t.Fatalf("Save(guard pending) error = %v", err)
			}
			input := tc.input
			input.SessionID = sessionID
			output, emitted := runHook(t, store, nil, input)
			if !emitted || output.HookSpecificOutput == nil || output.HookSpecificOutput.AdditionalContext != AdditionalContext {
				t.Fatalf("output = %+v, %v; want compact guard", output, emitted)
			}
			if state, err := store.Load(sessionID); err != nil || state != tc.wantState {
				t.Fatalf("state = %q, %v; want %q", state, err, tc.wantState)
			}
		})
	}
}

func TestPostToolUseConsumesPendingNudgeOnlyAfterSuccessfulReceive(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		toolName     string
		toolInput    json.RawMessage
		toolResponse json.RawMessage
		wantConsumed bool
	}{
		{name: "MCP structured content received", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"structuredContent":{"status":"received"}}`), wantConsumed: true},
		{name: "MCP structured content no message", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"structuredContent":{"status":"no_message"}}`), wantConsumed: true},
		{name: "MCP top-level status", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"status":"received"}`), wantConsumed: true},
		{name: "MCP content text block", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"content":[{"type":"text","text":"{\"status\":\"received\"}"}]}`), wantConsumed: true},
		{name: "MCP serialized string result", toolName: receiveMCPToolName, toolResponse: jsonText(`{"structuredContent":{"status":"received"}}`), wantConsumed: true},
		{name: "MCP active leases", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"structuredContent":{"status":"active_leases"}}`)},
		{name: "MCP error", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"isError":true,"structuredContent":{"status":"received"}}`)},
		{name: "Bash stdout JSON received", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: json.RawMessage(`{"stdout":"{\"status\":\"received\",\"delivery\":{\"delivery_id\":\"dlv_1\"}}","stderr":"","interrupted":false}`), wantConsumed: true},
		{name: "Bash stdout JSON no message", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: json.RawMessage(`{"stdout":"{\"status\":\"no_message\"}","stderr":"","interrupted":false}`), wantConsumed: true},
		{name: "Bash stdout text received", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv"}`), toolResponse: json.RawMessage(`{"stdout":"delivery_id=dlv_1 recipient_address=agent/reviewer lease_token=lease_1 content_type=text/plain\nbody\n","stderr":""}`), wantConsumed: true},
		{name: "Bash stdout text no message", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv"}`), toolResponse: json.RawMessage(`{"stdout":"status=no_message\n","stderr":""}`), wantConsumed: true},
		{name: "Bash bare string response", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: jsonText(`{"status":"received"}`), wantConsumed: true},
		{name: "Bash stdout JSON failure", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: json.RawMessage(`{"stdout":"{\"status\":\"error\",\"error_code\":\"busy\"}","stderr":""}`)},
		{name: "Bash stderr only", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: json.RawMessage(`{"stdout":"","stderr":"database is locked"}`)},
		{name: "unrelated Bash", toolName: "Bash", toolInput: json.RawMessage(`{"command":"go test ./..."}`), toolResponse: json.RawMessage(`{"stdout":"ok"}`)},
		{name: "unrelated tool", toolName: "Edit", toolResponse: json.RawMessage(`{"filePath":"/tmp/x","success":true}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const sessionID = "session-receive-result"
			store := newMemoryNudgeStateStore()
			if err := store.Save(sessionID, hookcore.NudgePending); err != nil {
				t.Fatalf("Save(pending) error = %v", err)
			}
			if output, emitted := runHook(t, store, nil, hookInput{
				HookEventName: "PostToolUse",
				SessionID:     sessionID,
				ToolName:      tc.toolName,
				ToolInput:     tc.toolInput,
				ToolResponse:  tc.toolResponse,
			}); emitted {
				t.Fatalf("PostToolUse output = %+v, want empty", output)
			}
			state, err := store.Load(sessionID)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			want := hookcore.NudgePending
			if tc.wantConsumed {
				want = hookcore.NudgeConsumed
			}
			if state != want {
				t.Fatalf("state = %q, want %q", state, want)
			}
		})
	}
}

func TestReceiveWithoutPendingNudgeDoesNotEnableCompactGuard(t *testing.T) {
	t.Parallel()

	const sessionID = "session-explicit-receive"
	store := newMemoryNudgeStateStore()
	if output, emitted := runHook(t, store, nil, hookInput{
		HookEventName: "PostToolUse",
		SessionID:     sessionID,
		ToolName:      receiveMCPToolName,
		ToolResponse:  json.RawMessage(`{"structuredContent":{"status":"received"}}`),
	}); emitted {
		t.Fatalf("PostToolUse output = %+v, want empty", output)
	}
	if state, err := store.Load(sessionID); err != nil || state != hookcore.NudgeNone {
		t.Fatalf("state after explicit receive = %q, %v; want none", state, err)
	}
	if context := runCompactHook(t, store, sessionID); context != "" {
		t.Fatalf("compact context after explicit receive = %q, want empty", context)
	}
}

func TestSessionEndClearsNudgeState(t *testing.T) {
	t.Parallel()

	const sessionID = "session-end"
	store := newMemoryNudgeStateStore()
	if err := store.Save(sessionID, hookcore.NudgeConsumed); err != nil {
		t.Fatalf("Save(consumed) error = %v", err)
	}
	if output, emitted := runHook(t, store, nil, hookInput{HookEventName: "SessionEnd", SessionID: sessionID}); emitted {
		t.Fatalf("SessionEnd output = %+v, want empty", output)
	}
	if state, err := store.Load(sessionID); err != nil || state != hookcore.NudgeNone {
		t.Fatalf("state after SessionEnd = %q, %v; want none", state, err)
	}
}

func TestFileNudgeStateStorePersistsAndClearsSessionState(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "hook-state")
	store := fileNudgeStateStore{Dir: stateDir, Label: harnessLabel}
	const sessionID = "session/with unsafe path characters"
	if err := store.Save(sessionID, hookcore.NudgePending); err != nil {
		t.Fatalf("Save(pending) error = %v", err)
	}
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", stateDir, err)
	}
	if len(entries) != 1 || entries[0].IsDir() {
		t.Fatalf("state entries = %#v, want one file", entries)
	}
	info, err := entries[0].Info()
	if err != nil {
		t.Fatalf("state file Info() error = %v", err)
	}
	if got := info.Mode().Perm(); runtime.GOOS != "windows" && got != 0o600 {
		t.Fatalf("state file mode = %o, want 600", got)
	}
	if state, err := store.Load(sessionID); err != nil || state != hookcore.NudgePending {
		t.Fatalf("Load(pending) = %q, %v; want pending", state, err)
	}
	if err := store.Clear(sessionID); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}
	if state, err := store.Load(sessionID); err != nil || state != hookcore.NudgeNone {
		t.Fatalf("Load(after clear) = %q, %v; want none", state, err)
	}
}

func TestRunUserPromptNudgeSelectsReceivePathFromClaudeProbe(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		available   bool
		probeErr    error
		wantContext string
		wantWarning string
	}{
		{
			name:        "MCP available",
			available:   true,
			wantContext: MCPNudgeContext,
		},
		{
			name:        "MCP unavailable",
			wantContext: CLINudgeContext,
		},
		{
			name:        "probe failed",
			probeErr:    errors.New("claude unavailable"),
			wantContext: MCPProbeFailedNudgeContext,
			wantWarning: "Waypost MCP probe failed: claude unavailable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			probeCalls := 0
			probe := func(context.Context) (bool, error) {
				probeCalls++
				return tc.available, tc.probeErr
			}

			input := strings.NewReader(`{
  "hook_event_name": "UserPromptSubmit",
  "session_id": "session-probe-selection",
  "prompt": "NOTICE: There might be new delivery in waypost."
}`)
			var output bytes.Buffer
			if err := runWithMCPProbe(context.Background(), input, &output, probe); err != nil {
				t.Fatalf("runWithMCPProbe(UserPromptSubmit) error = %v", err)
			}
			if probeCalls != 1 {
				t.Fatalf("probe calls = %d, want 1", probeCalls)
			}
			var payload hookOutput
			if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
				t.Fatalf("Unmarshal(output) error = %v", err)
			}
			specific := payload.HookSpecificOutput
			if specific == nil || specific.HookEventName != "UserPromptSubmit" {
				t.Fatalf("hookSpecificOutput = %+v, want UserPromptSubmit", specific)
			}
			if specific.AdditionalContext != tc.wantContext {
				t.Fatalf("additionalContext = %q, want %q", specific.AdditionalContext, tc.wantContext)
			}
			if payload.SystemMessage != tc.wantWarning {
				t.Fatalf("systemMessage = %q, want %q", payload.SystemMessage, tc.wantWarning)
			}
		})
	}
}

func TestRunUserPromptNudgeStartsClaudeMCPProbe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}

	binDir := t.TempDir()
	claudePath := filepath.Join(binDir, "claude")
	probe := `#!/bin/sh
if [ "$1" != "mcp" ] || [ "$2" != "get" ] || [ "$3" != "waypost" ]; then
  exit 9
fi
printf '%s\n' 'waypost:' '  Status: ✔ Connected' '  Type: stdio' '  Command: /opt/waypost' '  Args: mcp'
`
	if err := os.WriteFile(claudePath, []byte(probe), 0o700); err != nil {
		t.Fatalf("WriteFile(claude probe) error = %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())

	previousBudget := hookcore.RunBudget
	hookcore.RunBudget = 30 * time.Second
	defer func() { hookcore.RunBudget = previousBudget }()

	realProbe := func(ctx context.Context) (bool, error) {
		return probeWaypostMCPWithTimeout(ctx, 30*time.Second)
	}
	input := strings.NewReader(`{
  "hook_event_name": "UserPromptSubmit",
  "session_id": "session-real-probe",
  "prompt": "NOTICE: There might be new delivery in waypost."
}`)
	var output bytes.Buffer
	if err := runWithMCPProbe(context.Background(), input, &output, realProbe); err != nil {
		t.Fatalf("run(UserPromptSubmit) error = %v", err)
	}
	var payload hookOutput
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal(output) error = %v", err)
	}
	if payload.HookSpecificOutput == nil || payload.HookSpecificOutput.AdditionalContext != MCPNudgeContext {
		t.Fatalf("additionalContext = %+v, want %q", payload.HookSpecificOutput, MCPNudgeContext)
	}
}

func TestRunReusesMCPProbeForSession(t *testing.T) {
	store := newMemoryNudgeStateStore()
	probeCalls := 0
	probe := func(context.Context) (bool, error) {
		probeCalls++
		return true, nil
	}
	for _, input := range []hookInput{
		{HookEventName: "UserPromptSubmit", SessionID: "cached-session", Prompt: defaultNudgeMessage},
		{HookEventName: "UserPromptSubmit", SessionID: "cached-session", Prompt: "ordinary prompt"},
		{HookEventName: "SessionStart", SessionID: "cached-session", Source: "compact"},
		{HookEventName: "PreToolUse", SessionID: "cached-session", ToolName: "Bash", ToolInput: json.RawMessage(`{"command":"waypost recv"}`)},
	} {
		var output bytes.Buffer
		encoded, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		if err := runWithDependencies(context.Background(), bytes.NewReader(encoded), &output, probe, store); err != nil {
			t.Fatal(err)
		}
	}
	if probeCalls != 1 {
		t.Fatalf("probe calls = %d, want 1", probeCalls)
	}
}

func TestRunUserPromptSkipsOrdinaryWaypostDiscussion(t *testing.T) {
	t.Parallel()

	input := strings.NewReader(`{
  "hook_event_name": "UserPromptSubmit",
  "session_id": "session-ordinary-prompt",
  "prompt": "Please explain how the Waypost nudge message works."
}`)
	var output bytes.Buffer
	err := runWithMCPProbe(context.Background(), input, &output, func(context.Context) (bool, error) {
		t.Fatal("MCP probe called for an ordinary prompt")
		return false, nil
	})
	if err != nil {
		t.Fatalf("run(UserPromptSubmit) error = %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("output = %q, want empty", output.String())
	}
}

func TestRunPreToolUseWarnsBeforeWaypostWait(t *testing.T) {
	t.Parallel()

	input := strings.NewReader(`{
  "hook_event_name": "PreToolUse",
  "tool_name": "Bash",
  "tool_input": {
    "command": "waypost --state-dir /tmp/waypost-state wait --for workflow/reviewer --timeout 30s"
  }
}`)
	var output bytes.Buffer
	if err := runWithMCPProbe(context.Background(), input, &output, nil); err != nil {
		t.Fatalf("runWithMCPProbe(PreToolUse) error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal(output) error = %v", err)
	}
	specific := payload["hookSpecificOutput"].(map[string]any)
	if got := specific["hookEventName"]; got != "PreToolUse" {
		t.Fatalf("hookEventName = %v, want PreToolUse", got)
	}
	if _, denied := specific["permissionDecision"]; denied {
		t.Fatalf("permissionDecision = %#v, want soft warning instead of deny", specific["permissionDecision"])
	}
	additionalContext, _ := specific["additionalContext"].(string)
	if !strings.Contains(additionalContext, "Do not poll Waypost") || !strings.Contains(additionalContext, "Continue other available work") || !strings.Contains(additionalContext, "stop completely") {
		t.Fatalf("additionalContext = %q, want wait polling warning", additionalContext)
	}
}

func TestRunPreToolUseDeniesMCPPreferredWaypostCLICommands(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		command    string
		wantReason string
	}{
		{name: "status", command: "waypost status", wantReason: "MCP tool waypost_status is available"},
		{name: "ack", command: "waypost ack --delivery dlv_1 --lease-token lease_1", wantReason: "MCP tool waypost_ack is available"},
		{name: "recv", command: "waypost recv --for workflow/reviewer", wantReason: "MCP tool waypost_recv is available"},
		{name: "receive alias", command: "waypost receive --for workflow/reviewer", wantReason: "MCP tool waypost_recv is available"},
		{name: "send", command: "waypost send --to workflow/reviewer --body-file /tmp/body", wantReason: "MCP tool waypost_send is available"},
		{name: "mcp subcommand", command: "waypost mcp", wantReason: "managed by Claude Code"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			output, emitted := runPreToolHook(t, tc.command, func(context.Context) (bool, error) {
				return true, nil
			})
			if !emitted || output.HookSpecificOutput == nil {
				t.Fatalf("output = %+v, %v; want deny", output, emitted)
			}
			if output.HookSpecificOutput.PermissionDecision != "deny" {
				t.Fatalf("permissionDecision = %q, want deny", output.HookSpecificOutput.PermissionDecision)
			}
			if !strings.Contains(output.HookSpecificOutput.PermissionDecisionReason, tc.wantReason) {
				t.Fatalf("permissionDecisionReason = %q, want %q", output.HookSpecificOutput.PermissionDecisionReason, tc.wantReason)
			}
		})
	}
}

func TestRunPreToolUseAllowsWaypostCLIWhenMCPUnavailable(t *testing.T) {
	t.Parallel()

	output, emitted := runPreToolHook(t, "waypost recv --for workflow/reviewer", func(context.Context) (bool, error) {
		return false, nil
	})
	if emitted {
		t.Fatalf("output = %+v, want CLI fallback allowed", output)
	}
}

func TestRunPreToolUseLeavesCLIUntouchedWhenProbeFails(t *testing.T) {
	t.Parallel()

	output, emitted := runPreToolHook(t, "waypost recv --for workflow/reviewer", func(context.Context) (bool, error) {
		return false, errors.New("claude unavailable")
	})
	if !emitted {
		t.Fatal("output = empty, want probe failure warning")
	}
	if output.HookSpecificOutput != nil && output.HookSpecificOutput.PermissionDecision == "deny" {
		t.Fatalf("permissionDecision = deny, want CLI untouched on probe failure")
	}
	if !strings.Contains(output.SystemMessage, "Waypost MCP probe failed: claude unavailable") {
		t.Fatalf("systemMessage = %q, want probe failure warning", output.SystemMessage)
	}
}

func TestRunPreToolUseAlwaysDeniesWaypostMCPWithoutProbe(t *testing.T) {
	t.Parallel()

	output, emitted := runPreToolHook(t, "waypost mcp", func(context.Context) (bool, error) {
		t.Fatal("MCP probe called for always-denied command")
		return true, nil
	})
	if !emitted || output.HookSpecificOutput == nil || output.HookSpecificOutput.PermissionDecision != "deny" {
		t.Fatalf("output = %+v, %v; want deny", output, emitted)
	}
	if !strings.Contains(output.HookSpecificOutput.PermissionDecisionReason, "managed by Claude Code") {
		t.Fatalf("permissionDecisionReason = %q, want managed-by-Claude-Code reason", output.HookSpecificOutput.PermissionDecisionReason)
	}
}

func TestRunPreToolUseIgnoresNonShellTools(t *testing.T) {
	t.Parallel()

	input := strings.NewReader(`{
  "hook_event_name": "PreToolUse",
  "session_id": "session-non-shell",
  "tool_name": "Edit",
  "tool_input": {"file_path": "/tmp/x"}
}`)
	var output bytes.Buffer
	err := runWithMCPProbe(context.Background(), input, &output, func(context.Context) (bool, error) {
		t.Fatal("MCP probe called for non-shell tool")
		return true, nil
	})
	if err != nil {
		t.Fatalf("run(PreToolUse) error = %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("output = %q, want empty", output.String())
	}
}

func TestParseWaypostMCPAvailable(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		output    string
		want      bool
		wantError bool
	}{
		{
			name: "connected",
			output: `waypost:
  Scope: User config (available in all your projects)
  Status: ✔ Connected
  Type: stdio
  Command: /opt/waypost
  Args: mcp

To remove this server, run: claude mcp remove "waypost" -s user
`,
			want: true,
		},
		{
			name: "cached",
			output: `waypost:
  Status: cached 2h ago · connects on first use · 5 tools
`,
			want: true,
		},
		{
			name: "failed to connect",
			output: `waypost:
  Status: ✘ Failed to connect
`,
			want: false,
		},
		{
			name: "pending approval",
			output: `waypost:
  Status: ⏸ Pending approval (run ` + "`claude`" + ` to approve)
`,
			want: false,
		},
		{
			name: "needs authentication",
			output: `waypost:
  Status: ! Needs authentication
`,
			want: false,
		},
		{
			name: "tools fetch failed",
			output: `waypost:
  Status: ! Connected · tools fetch failed
`,
			want: false,
		},
		{
			name: "rejected",
			output: `waypost:
  Status: ✘ Rejected (see disabledMcpjsonServers in settings)
`,
			want: false,
		},
		{
			name: "no status line",
			output: `waypost:
  Scope: User config (available in all your projects)
  Type: stdio
  Command: /opt/waypost
`,
			want: true,
		},
		{
			name:   "server missing in output",
			output: `No MCP server found with name: waypost`,
			want:   false,
		},
		{
			name:      "unexpected output",
			output:    `garbage report`,
			wantError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			available, err := parseWaypostMCPAvailable([]byte(tc.output))
			if tc.wantError {
				if err == nil {
					t.Fatalf("parseWaypostMCPAvailable() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseWaypostMCPAvailable() error = %v", err)
			}
			if available != tc.want {
				t.Fatalf("parseWaypostMCPAvailable() = %v, want %v", available, tc.want)
			}
		})
	}
}

func TestIsMissingWaypostMCPError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		detail string
		want   bool
	}{
		{detail: `No MCP server named "waypost". Run ` + "`claude mcp add`" + ` to add one.`, want: true},
		{detail: `No MCP server found with name: waypost`, want: true},
		{detail: `Error: MCP server "waypost" not found`, want: false},
		{detail: `server waypost does not exist`, want: false},
		{detail: `waypost config not found`, want: false},
		{detail: `unexpected connection failure`, want: false},
	} {
		if got := isMissingWaypostMCPError(tc.detail); got != tc.want {
			t.Fatalf("isMissingWaypostMCPError(%q) = %v, want %v", tc.detail, got, tc.want)
		}
	}
}

func TestDefaultConfigDirHonorsClaudeConfigDir(t *testing.T) {
	configured := filepath.Join(t.TempDir(), "claude-config")
	t.Setenv("CLAUDE_CONFIG_DIR", configured)
	dir, err := DefaultConfigDir()
	if err != nil {
		t.Fatalf("DefaultConfigDir() error = %v", err)
	}
	absolute, err := filepath.Abs(configured)
	if err != nil {
		t.Fatal(err)
	}
	if dir != absolute {
		t.Fatalf("DefaultConfigDir() = %q, want %q", dir, absolute)
	}
}

func TestInstallCreatesSettingsWhenMissing(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	command := "'/opt/waypost' claude-hook"
	result, err := Install(configDir, command)
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Install() changed = false, want created settings")
	}
	document := readSettingsDocument(t, filepath.Join(configDir, configFileName))
	hooks := document["hooks"].(map[string]any)
	for _, event := range []string{"SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "SessionEnd"} {
		groups, ok := hooks[event].([]any)
		if !ok || len(groups) == 0 {
			t.Fatalf("hooks[%q] = %#v, want installed group", event, hooks[event])
		}
		if !groupHasCommand(groups[0].(map[string]any), command) {
			t.Fatalf("hooks[%q][0] = %#v, want %q", event, groups[0], command)
		}
	}
	preToolUse := hooks["PreToolUse"].([]any)
	if matcher, _ := preToolUse[0].(map[string]any)["matcher"].(string); matcher != "^Bash$" {
		t.Fatalf("PreToolUse matcher = %q, want ^Bash$", matcher)
	}
	postToolUse := hooks["PostToolUse"].([]any)
	if _, exists := postToolUse[0].(map[string]any)["matcher"]; exists {
		t.Fatalf("PostToolUse matcher = %#v, want absent so every tool call fires", postToolUse[0])
	}
}

func TestInstallPreservesUnrelatedSettingsAndHooksAndIsIdempotent(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, configFileName)
	original := `{
  "model": "opus",
  "permissions": {"allow": ["Bash(go test:*)"]},
  "hooks": {
    "SessionStart": [
      {"matcher": "compact", "hooks": [{"type": "command", "command": "restore-context"}]}
    ],
    "UserPromptSubmit": [
      {"hooks": [{"type": "command", "command": "inspect-prompt"}]}
    ],
    "PreToolUse": [
      {"matcher": "^Bash$", "hooks": [{"type": "command", "command": "check-bash"}]}
    ],
    "PostToolUse": [
      {"matcher": "^Edit$", "hooks": [{"type": "command", "command": "review-edit"}]}
    ],
    "SessionEnd": [
      {"hooks": [{"type": "command", "command": "archive-session"}]}
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(settings.json) error = %v", err)
	}

	command := "'/opt/waypost' claude-hook"
	result, err := Install(configDir, command)
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Install() changed = false, want merged hooks")
	}
	document := readSettingsDocument(t, path)
	if model, _ := document["model"].(string); model != "opus" {
		t.Fatalf("model = %v, want preserved", document["model"])
	}
	if _, ok := document["permissions"].(map[string]any); !ok {
		t.Fatalf("permissions = %#v, want preserved", document["permissions"])
	}
	hooks := document["hooks"].(map[string]any)

	// The user's compact-only SessionStart group stays; the managed group is
	// appended so it also fires for non-compact sources.
	sessionStart := hooks["SessionStart"].([]any)
	if len(sessionStart) != 2 {
		t.Fatalf("SessionStart groups = %#v, want user group plus managed group", sessionStart)
	}
	if !groupHasCommand(sessionStart[0].(map[string]any), "restore-context") {
		t.Fatalf("SessionStart[0] = %#v, want user group preserved", sessionStart[0])
	}
	if !groupHasCommand(sessionStart[1].(map[string]any), command) {
		t.Fatalf("SessionStart[1] = %#v, want managed group", sessionStart[1])
	}

	// Unrelated groups stay at their original positions.
	if !groupHasCommand(hooks["UserPromptSubmit"].([]any)[0].(map[string]any), "inspect-prompt") {
		t.Fatal("UserPromptSubmit[0] user group not preserved")
	}
	preToolUse := hooks["PreToolUse"].([]any)
	if !groupHasCommand(preToolUse[0].(map[string]any), "check-bash") || !groupHasCommand(preToolUse[1].(map[string]any), command) {
		t.Fatalf("PreToolUse = %#v, want user group then managed group", preToolUse)
	}
	postToolUse := hooks["PostToolUse"].([]any)
	if !groupHasCommand(postToolUse[0].(map[string]any), "review-edit") || !groupHasCommand(postToolUse[1].(map[string]any), command) {
		t.Fatalf("PostToolUse = %#v, want user group then managed group", postToolUse)
	}
	sessionEnd := hooks["SessionEnd"].([]any)
	if !groupHasCommand(sessionEnd[0].(map[string]any), "archive-session") || !groupHasCommand(sessionEnd[1].(map[string]any), command) {
		t.Fatalf("SessionEnd = %#v, want user group then managed group", sessionEnd)
	}

	second, err := Install(configDir, command)
	if err != nil {
		t.Fatalf("Install(second) error = %v", err)
	}
	if second.Changed {
		t.Fatalf("Install(second) = %+v, want idempotent", second)
	}
}

func TestInstallAdoptsHooksFromOlderWaypostPath(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, configFileName)
	original := `{
  "hooks": {
    "SessionStart": [
      {"hooks": [{"type": "command", "command": "'/old/waypost' claude-hook", "timeout": 5}]}
    ],
    "UserPromptSubmit": [
      {"hooks": [{"type": "command", "command": "'/old/waypost' claude-hook", "timeout": 5}]}
    ],
    "PreToolUse": [
      {"matcher": "^Bash$", "hooks": [{"type": "command", "command": "'/old/waypost' claude-hook", "timeout": 5}]}
    ],
    "PostToolUse": [
      {"hooks": [{"type": "command", "command": "'/old/waypost' claude-hook", "timeout": 5}]}
    ],
    "SessionEnd": [
      {"hooks": [{"type": "command", "command": "'/old/waypost' claude-hook", "timeout": 3}]}
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(settings.json) error = %v", err)
	}

	command := "'/opt/waypost' claude-hook"
	result, err := Install(configDir, command)
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Install() changed = false, want adopted hooks refreshed")
	}
	document := readSettingsDocument(t, path)
	hooks := document["hooks"].(map[string]any)
	for _, event := range []string{"SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "SessionEnd"} {
		groups := hooks[event].([]any)
		if len(groups) != 1 || !groupHasCommand(groups[0].(map[string]any), command) {
			t.Fatalf("hooks[%q] = %#v, want single refreshed managed group", event, groups)
		}
	}
}

func TestInstallPreservesCommandsMerelyMentioningClaudeHook(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, configFileName)
	original := `{
  "hooks": {
    "UserPromptSubmit": [
      {"hooks": [{"type": "command", "command": "echo claude-hook"}]}
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(settings.json) error = %v", err)
	}

	command := "'/opt/waypost' claude-hook"
	if _, err := Install(configDir, command); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	document := readSettingsDocument(t, path)
	groups := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
	if len(groups) != 2 {
		t.Fatalf("UserPromptSubmit groups = %#v, want user group plus managed group", groups)
	}
	if !groupHasCommand(groups[0].(map[string]any), "echo claude-hook") {
		t.Fatalf("UserPromptSubmit[0] = %#v, want user command preserved", groups[0])
	}
}

func TestInstallPreservesSiblingHandlersWhenAdoptingGroups(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, configFileName)
	original := `{
  "hooks": {
    "UserPromptSubmit": [
      {
        "custom": "keep-prompt",
        "hooks": [
          {"type": "command", "command": "'/old/waypost' claude-hook"},
          {"type": "command", "command": "keep-prompt-sibling"}
        ]
      }
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(settings.json) error = %v", err)
	}

	command := "'/opt/waypost' claude-hook"
	if _, err := Install(configDir, command); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	document := readSettingsDocument(t, path)
	groups := document["hooks"].(map[string]any)["UserPromptSubmit"].([]any)
	if len(groups) != 1 {
		t.Fatalf("UserPromptSubmit groups = %#v, want mixed group updated in place", groups)
	}
	preserved := groups[0].(map[string]any)
	if preserved["custom"] != "keep-prompt" || !groupHasCommand(preserved, "keep-prompt-sibling") {
		t.Fatalf("preserved group = %#v, want custom field and sibling handler", preserved)
	}
	handlers := preserved["hooks"].([]any)
	if !handlerHasCommand(handlers[0], command) || !handlerHasCommand(handlers[1], "keep-prompt-sibling") {
		t.Fatalf("handlers = %#v, want Waypost and sibling handler positions preserved", handlers)
	}
}

func TestInstallRejectsMalformedMatcherGroupsWithoutChangingFile(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, configFileName)
	original := `{
  "hooks": {
    "PreToolUse": [
      {"matcher": 42, "hooks": [{"type": "command", "command": "check-bash"}]}
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(settings.json) error = %v", err)
	}

	if _, err := Install(configDir, "'/opt/waypost' claude-hook"); err == nil || !strings.Contains(err.Error(), "matcher") {
		t.Fatalf("Install() error = %v, want malformed matcher rejection", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(settings.json) error = %v", err)
	}
	if string(contents) != original {
		t.Fatal("settings.json changed despite rejected install")
	}
}

func TestInstallRejectsInvalidJSONWithoutChangingFile(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, configFileName)
	original := `{"hooks": {`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(settings.json) error = %v", err)
	}
	if _, err := Install(configDir, "'/opt/waypost' claude-hook"); err == nil {
		t.Fatal("Install() error = nil, want parse failure")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(settings.json) error = %v", err)
	}
	if string(contents) != original {
		t.Fatal("settings.json changed despite rejected install")
	}
}

func TestDoctorReportsInstalledHooks(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	command := "'/opt/waypost' claude-hook"
	if _, err := Install(configDir, command); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	result, err := Doctor(configDir, command)
	if err != nil {
		t.Fatalf("Doctor() error = %v", err)
	}
	if result.Command != command {
		t.Fatalf("Doctor() command = %q, want %q", result.Command, command)
	}
	if !strings.HasSuffix(result.Path, configFileName) {
		t.Fatalf("Doctor() path = %q, want settings.json", result.Path)
	}
}

func TestDoctorRequiresAllManagedHooks(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, configFileName)
	contents := `{
  "hooks": {
    "SessionStart": [{
      "hooks": [{"type": "command", "command": "waypost claude-hook", "timeout": 5}]
    }],
    "UserPromptSubmit": [{
      "hooks": [{"type": "command", "command": "waypost claude-hook", "timeout": 5}]
    }],
    "PreToolUse": [{
      "matcher": "^Bash$",
      "hooks": [{"type": "command", "command": "waypost claude-hook", "timeout": 5}]
    }],
    "SessionEnd": [{
      "hooks": [{"type": "command", "command": "waypost claude-hook", "timeout": 3}]
    }]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(settings.json) error = %v", err)
	}
	_, err := Doctor(configDir, "waypost claude-hook")
	if err == nil || !strings.Contains(err.Error(), "PostToolUse receive tracker") {
		t.Fatalf("Doctor() error = %v, want missing PostToolUse error", err)
	}
}

func TestDoctorRejectsMatcherThatNeverFiresForNonToolEvent(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, configFileName)
	contents := `{
  "hooks": {
    "SessionStart": [{
      "matcher": "^compact$",
      "hooks": [{"type": "command", "command": "waypost claude-hook", "timeout": 5}]
    }],
    "UserPromptSubmit": [{
      "hooks": [{"type": "command", "command": "waypost claude-hook", "timeout": 5}]
    }],
    "PreToolUse": [{
      "matcher": "^Bash$",
      "hooks": [{"type": "command", "command": "waypost claude-hook", "timeout": 5}]
    }],
    "PostToolUse": [{
      "hooks": [{"type": "command", "command": "waypost claude-hook", "timeout": 5}]
    }],
    "SessionEnd": [{
      "hooks": [{"type": "command", "command": "waypost claude-hook", "timeout": 3}]
    }]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(settings.json) error = %v", err)
	}
	// A SessionStart group bound to compact-only cannot deliver a pending
	// guard on startup/resume/clear, so it does not satisfy the managed spec.
	_, err := Doctor(configDir, "waypost claude-hook")
	if err == nil || !strings.Contains(err.Error(), "SessionStart compact guard") {
		t.Fatalf("Doctor() error = %v, want SessionStart eligibility rejection", err)
	}
}

func TestDoctorRejectsManagedHandlersWithoutShortTimeout(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, configFileName)
	contents := `{
  "hooks": {
    "SessionStart": [{
      "hooks": [{"type": "command", "command": "waypost claude-hook"}]
    }]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(settings.json) error = %v", err)
	}
	if _, err := Doctor(configDir, "waypost claude-hook"); err == nil {
		t.Fatal("Doctor() error = nil, want missing timeout rejection")
	}
}

// TestRunReportsStalledHookInputWithinBudget feeds run() a stdin pipe whose
// payload never arrives: the internal budget must end the wait below the
// harness deadline and emit a systemMessage instead of dying to an opaque
// kill.
func TestRunReportsStalledHookInputWithinBudget(t *testing.T) {
	previous := hookcore.RunBudget
	hookcore.RunBudget = 50 * time.Millisecond
	defer func() { hookcore.RunBudget = previous }()
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())

	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	defer readEnd.Close()
	defer writeEnd.Close() // payload never arrives

	var output bytes.Buffer
	start := time.Now()
	if err := run(context.Background(), readEnd, &output); err != nil {
		t.Fatalf("run() error = %v, want graceful systemMessage", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("run() took %v, want exit well below the harness deadline", elapsed)
	}
	var payload map[string]any
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal(hook output) error = %v", err)
	}
	message, _ := payload["systemMessage"].(string)
	if !strings.Contains(message, "timed out") {
		t.Fatalf("systemMessage = %q, want timeout notice", message)
	}
}

func readSettingsDocument(t *testing.T, path string) map[string]any {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	var document map[string]any
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("Unmarshal(%q) error = %v", path, err)
	}
	return document
}

func groupHasCommand(group map[string]any, command string) bool {
	handlers, ok := group["hooks"].([]any)
	if !ok {
		return false
	}
	for _, handler := range handlers {
		if handlerHasCommand(handler, command) {
			return true
		}
	}
	return false
}

func jsonText(value string) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func handlerHasCommand(value any, command string) bool {
	handler, ok := value.(map[string]any)
	if !ok {
		return false
	}
	handlerType, _ := handler["type"].(string)
	handlerCommand, _ := handler["command"].(string)
	return handlerType == "command" && handlerCommand == command
}

func runCompactHook(t *testing.T, store nudgeStateStore, sessionID string) string {
	t.Helper()
	output, emitted := runHook(t, store, nil, hookInput{
		HookEventName: "SessionStart",
		SessionID:     sessionID,
		Source:        "compact",
	})
	if !emitted || output.HookSpecificOutput == nil {
		return ""
	}
	return output.HookSpecificOutput.AdditionalContext
}

func runHook(
	t *testing.T,
	store nudgeStateStore,
	probe waypostMCPProbe,
	input hookInput,
) (hookOutput, bool) {
	t.Helper()
	encodedInput, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("Marshal(hook input) error = %v", err)
	}
	var encodedOutput bytes.Buffer
	if err := runWithDependencies(context.Background(), bytes.NewReader(encodedInput), &encodedOutput, probe, store); err != nil {
		t.Fatalf("runWithDependencies(%s) error = %v", input.HookEventName, err)
	}
	if encodedOutput.Len() == 0 {
		return hookOutput{}, false
	}
	var output hookOutput
	if err := json.Unmarshal(encodedOutput.Bytes(), &output); err != nil {
		t.Fatalf("Unmarshal(hook output) error = %v", err)
	}
	return output, true
}

func runPreToolHook(t *testing.T, command string, probe waypostMCPProbe) (hookOutput, bool) {
	t.Helper()
	toolInput, err := json.Marshal(map[string]string{"command": command})
	if err != nil {
		t.Fatalf("Marshal(tool input) error = %v", err)
	}
	input, err := json.Marshal(hookInput{
		HookEventName: "PreToolUse",
		ToolName:      "Bash",
		ToolInput:     toolInput,
	})
	if err != nil {
		t.Fatalf("Marshal(hook input) error = %v", err)
	}
	var encoded bytes.Buffer
	if err := runWithMCPProbe(context.Background(), bytes.NewReader(input), &encoded, probe); err != nil {
		t.Fatalf("runWithMCPProbe(PreToolUse) error = %v", err)
	}
	if encoded.Len() == 0 {
		return hookOutput{}, false
	}
	var output hookOutput
	if err := json.Unmarshal(encoded.Bytes(), &output); err != nil {
		t.Fatalf("Unmarshal(hook output) error = %v", err)
	}
	return output, true
}
