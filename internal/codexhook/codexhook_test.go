package codexhook

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

	"github.com/ruiheng/waypost/internal/launchpath"
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
	if !emitted || output.HookSpecificOutput.AdditionalContext != MCPNudgeContext {
		t.Fatalf("nudge output = %+v, %v; want MCP receive context", output, emitted)
	}
	if state, err := store.Load(sessionID); err != nil || state != nudgePending {
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
	if state, err := store.Load(sessionID); err != nil || state != nudgeConsumed {
		t.Fatalf("state after receive = %q, %v; want consumed", state, err)
	}
	for compact := 1; compact <= 2; compact++ {
		if context := runCompactHook(t, store, sessionID); context != AdditionalContext {
			t.Fatalf("compact %d context = %q, want guard %q", compact, context, AdditionalContext)
		}
	}

	if output, emitted := runHook(t, store, probe, hookInput{
		HookEventName: "UserPromptSubmit",
		SessionID:     sessionID,
		Prompt:        "Continue the original task.",
	}); emitted {
		t.Fatalf("ordinary prompt output = %+v, want empty", output)
	}
	if state, err := store.Load(sessionID); err != nil || state != nudgeNone {
		t.Fatalf("state after ordinary prompt = %q, %v; want none", state, err)
	}
	if context := runCompactHook(t, store, sessionID); context != "" {
		t.Fatalf("compact context after ordinary prompt = %q, want empty", context)
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
		{name: "MCP received", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"structuredContent":{"status":"received"}}`), wantConsumed: true},
		{name: "MCP no message", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"structuredContent":{"status":"no_message"}}`), wantConsumed: true},
		{name: "MCP active leases", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"structuredContent":{"status":"active_leases"}}`)},
		{name: "MCP recovery required", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"structuredContent":{"status":"receive_recovery_required"}}`)},
		{name: "MCP error", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"isError":true,"structuredContent":{"status":"received"}}`)},
		{name: "CLI JSON received", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: jsonText(`{"status":"received","delivery":{"delivery_id":"dlv_1"}}`), wantConsumed: true},
		{name: "CLI JSON no message", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: jsonText(`{"status":"no_message"}`), wantConsumed: true},
		{name: "CLI JSON full personal", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json --full"}`), toolResponse: jsonText(`{"delivery_id":"dlv_1","message_id":"msg_1","recipient_address":"agent/reviewer","lease_token":"lease_1","body":"review"}`), wantConsumed: true},
		{name: "CLI JSON full batch", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json --full --max 2"}`), toolResponse: jsonText(`{"messages":[{"delivery_id":"dlv_1","recipient_address":"agent/reviewer","lease_token":"lease_1"},{"delivery_id":"dlv_2","recipient_address":"agent/reviewer","lease_token":"lease_2"}]}`), wantConsumed: true},
		{name: "CLI JSON full group", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --for group/review --as alice --json --full"}`), toolResponse: jsonText(`{"message_id":"msg_1","group_address":"group/review","person":"alice","first_read_at":"2026-08-30T00:00:00Z","body":"review"}`), wantConsumed: true},
		{name: "CLI YAML received", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --yaml"}`), toolResponse: jsonText("status: \"received\"\naddresses:\n  - \"agent/reviewer\"\ndelivery:\n  delivery_id: \"dlv_1\"\n"), wantConsumed: true},
		{name: "CLI YAML no message", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --yaml"}`), toolResponse: jsonText("status: \"no_message\"\naddresses:\n  - \"agent/reviewer\"\n"), wantConsumed: true},
		{name: "CLI YAML full personal", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --yaml --full"}`), toolResponse: jsonText("delivery_id: \"dlv_1\"\nmessage_id: \"msg_1\"\nrecipient_address: \"agent/reviewer\"\nlease_token: \"lease_1\"\nbody: \"review\"\n"), wantConsumed: true},
		{name: "CLI YAML full batch", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --yaml --full --max 2"}`), toolResponse: jsonText("messages:\n  -\n    delivery_id: \"dlv_1\"\n    recipient_address: \"agent/reviewer\"\n    lease_token: \"lease_1\"\n"), wantConsumed: true},
		{name: "CLI YAML full group", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --for group/review --as alice --yaml --full"}`), toolResponse: jsonText("message_id: \"msg_1\"\ngroup_address: \"group/review\"\nperson: \"alice\"\nfirst_read_at: \"2026-08-30T00:00:00Z\"\nbody: \"review\"\n"), wantConsumed: true},
		{name: "CLI text received", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv"}`), toolResponse: jsonText("delivery_id=dlv_1 recipient_address=agent/reviewer lease_token=lease_1 content_type=text/plain subject=\"review\"\nbody\n"), wantConsumed: true},
		{name: "CLI group text received", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --for group/review --as alice"}`), toolResponse: jsonText("message_id=msg_1 group=group/review person=alice first_read_at=2026-08-30T00:00:00Z content_type=text/plain subject=\"review\" read_count=1 eligible_count=1\nbody\n"), wantConsumed: true},
		{name: "CLI text no message", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv"}`), toolResponse: jsonText("status=no_message\n"), wantConsumed: true},
		{name: "CLI JSON failure", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: jsonText(`{"status":"error","error_code":"busy"}`)},
		{name: "CLI JSON error status overrides delivery fields", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: jsonText(`{"status":"error","delivery_id":"dlv_1","recipient_address":"agent/reviewer","lease_token":"lease_1"}`)},
		{name: "CLI YAML failure", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --yaml"}`), toolResponse: jsonText("status: \"error\"\nerror_code: \"busy\"\n")},
		{name: "CLI YAML error status overrides delivery fields", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv --yaml"}`), toolResponse: jsonText("status: \"error\"\ndetails:\n  delivery_id: \"dlv_1\"\n  recipient_address: \"agent/reviewer\"\n  lease_token: \"lease_1\"\n")},
		{name: "CLI text failure", toolName: "Bash", toolInput: json.RawMessage(`{"command":"waypost recv"}`), toolResponse: jsonText("database is locked")},
		{name: "unrelated Bash", toolName: "Bash", toolInput: json.RawMessage(`{"command":"go test ./..."}`), toolResponse: jsonText("ok")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const sessionID = "session-receive-result"
			store := newMemoryNudgeStateStore()
			if err := store.Save(sessionID, nudgePending); err != nil {
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
			want := nudgePending
			if tc.wantConsumed {
				want = nudgeConsumed
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
	if state, err := store.Load(sessionID); err != nil || state != nudgeNone {
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
	if err := store.Save(sessionID, nudgeConsumed); err != nil {
		t.Fatalf("Save(consumed) error = %v", err)
	}
	if output, emitted := runHook(t, store, nil, hookInput{HookEventName: "SessionEnd", SessionID: sessionID}); emitted {
		t.Fatalf("SessionEnd output = %+v, want empty", output)
	}
	if state, err := store.Load(sessionID); err != nil || state != nudgeNone {
		t.Fatalf("state after SessionEnd = %q, %v; want none", state, err)
	}
}

func TestFileNudgeStateStorePersistsAndClearsSessionState(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "hook-state")
	store := fileNudgeStateStore{dir: stateDir}
	const sessionID = "session/with unsafe path characters"
	if err := store.Save(sessionID, nudgePending); err != nil {
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
	if state, err := store.Load(sessionID); err != nil || state != nudgePending {
		t.Fatalf("Load(pending) = %q, %v; want pending", state, err)
	}
	if err := store.Save(sessionID, nudgeConsumed); err != nil {
		t.Fatalf("Save(consumed) error = %v", err)
	}
	if state, err := store.Load(sessionID); err != nil || state != nudgeConsumed {
		t.Fatalf("Load(consumed) = %q, %v; want consumed", state, err)
	}
	if err := store.Clear(sessionID); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}
	if state, err := store.Load(sessionID); err != nil || state != nudgeNone {
		t.Fatalf("Load(after clear) = %q, %v; want none", state, err)
	}
}

func TestRunUserPromptNudgeSelectsReceivePathFromCodexProbe(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		available   bool
		probeErr    error
		wantContext string
		wantText    string
		rejectText  string
		wantWarning string
	}{
		{
			name:        "MCP available",
			available:   true,
			wantContext: MCPNudgeContext,
			wantText:    "The waypost_recv MCP tool is available. Use it instead of the Waypost CLI.",
			rejectText:  "Use it—not the Waypost CLI",
		},
		{
			name:        "MCP unavailable",
			wantContext: CLINudgeContext,
			wantText:    "MCP tool waypost_recv is unavailable",
			rejectText:  "MCP tool waypost_recv is available",
		},
		{
			name:        "probe failed",
			probeErr:    errors.New("codex unavailable"),
			wantContext: MCPProbeFailedNudgeContext,
			wantText:    "Look for the waypost_recv MCP tool",
			rejectText:  "Receive the pending delivery with the Waypost CLI",
			wantWarning: "Waypost MCP probe failed: codex unavailable",
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
			if specific.HookEventName != "UserPromptSubmit" {
				t.Fatalf("hookEventName = %q, want UserPromptSubmit", specific.HookEventName)
			}
			if specific.AdditionalContext != tc.wantContext {
				t.Fatalf("additionalContext = %q, want %q", specific.AdditionalContext, tc.wantContext)
			}
			if !strings.Contains(specific.AdditionalContext, tc.wantText) {
				t.Fatalf("additionalContext = %q, want %q", specific.AdditionalContext, tc.wantText)
			}
			if strings.Contains(specific.AdditionalContext, tc.rejectText) {
				t.Fatalf("additionalContext = %q, reject %q", specific.AdditionalContext, tc.rejectText)
			}
			if payload.SystemMessage != tc.wantWarning {
				t.Fatalf("systemMessage = %q, want %q", payload.SystemMessage, tc.wantWarning)
			}
		})
	}
}

func TestRunUserPromptNudgeStartsCodexMCPProbe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}

	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	probe := `#!/bin/sh
if [ "$1" != "mcp" ] || [ "$2" != "get" ] || [ "$3" != "waypost" ] || [ "$4" != "--json" ]; then
  exit 9
fi
printf '%s\n' '{"name":"waypost","enabled":true}'
`
	if err := os.WriteFile(codexPath, []byte(probe), 0o700); err != nil {
		t.Fatalf("WriteFile(codex probe) error = %v", err)
	}
	t.Setenv("PATH", binDir)
	t.Setenv("CODEX_HOME", t.TempDir())

	input := strings.NewReader(`{
  "hook_event_name": "UserPromptSubmit",
  "session_id": "session-real-probe",
  "prompt": "NOTICE: There might be new delivery in waypost."
}`)
	var output bytes.Buffer
	if err := run(context.Background(), input, &output); err != nil {
		t.Fatalf("run(UserPromptSubmit) error = %v", err)
	}
	var payload hookOutput
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal(output) error = %v", err)
	}
	if payload.HookSpecificOutput.AdditionalContext != MCPNudgeContext {
		t.Fatalf("additionalContext = %q, want %q", payload.HookSpecificOutput.AdditionalContext, MCPNudgeContext)
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

func TestFileMCPProbeRejectsMissingAvailability(t *testing.T) {
	store := fileNudgeStateStore{dir: t.TempDir()}
	sessionID := "incomplete-probe"
	path, err := store.probePath(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"session_id":"incomplete-probe"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.LoadMCPProbe(sessionID); err == nil || ok {
		t.Fatalf("LoadMCPProbe() = ok %v, err %v; want parse error", ok, err)
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
	if err := run(context.Background(), input, &output); err != nil {
		t.Fatalf("run(PreToolUse) error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal(output) error = %v", err)
	}
	specific := payload["hookSpecificOutput"].(map[string]any)
	if got := specific["hookEventName"]; got != "PreToolUse" {
		t.Fatalf("hookEventName = %v, want PreToolUse", got)
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
		{
			name:       "status",
			command:    "waypost status",
			wantReason: "MCP tool waypost_status is available",
		},
		{
			name:       "ack",
			command:    "waypost ack --delivery dlv_1 --lease-token lease_1",
			wantReason: "MCP tool waypost_ack is available",
		},
		{
			name:       "recv",
			command:    "waypost recv --for workflow/reviewer",
			wantReason: "MCP tool waypost_recv is available",
		},
		{
			name:       "receive alias",
			command:    "waypost receive --for workflow/reviewer",
			wantReason: "MCP tool waypost_recv is available",
		},
		{
			name:       "send",
			command:    "waypost --state-dir /tmp/waypost send --to workflow/reviewer",
			wantReason: "MCP tool waypost_send is available",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			probeCalls := 0
			output, emitted := runPreToolHook(t, tc.command, func(context.Context) (bool, error) {
				probeCalls++
				return true, nil
			})
			if !emitted {
				t.Fatal("PreToolUse output is empty, want deny")
			}
			if probeCalls != 1 {
				t.Fatalf("probe calls = %d, want 1", probeCalls)
			}
			specific := output.HookSpecificOutput
			if specific.HookEventName != "PreToolUse" || specific.PermissionDecision != "deny" {
				t.Fatalf("hookSpecificOutput = %+v, want PreToolUse deny", specific)
			}
			if !strings.Contains(specific.PermissionDecisionReason, tc.wantReason) {
				t.Fatalf("permissionDecisionReason = %q, want %q", specific.PermissionDecisionReason, tc.wantReason)
			}
			if specific.AdditionalContext != "" {
				t.Fatalf("additionalContext = %q, want empty deny output", specific.AdditionalContext)
			}
		})
	}
}

func TestRunPreToolUseAlwaysDeniesWaypostMCPCommand(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		probe waypostMCPProbe
	}{
		{
			name:  "MCP available",
			probe: func(context.Context) (bool, error) { return true, nil },
		},
		{
			name:  "MCP unavailable",
			probe: func(context.Context) (bool, error) { return false, nil },
		},
		{
			name:  "MCP probe failed",
			probe: func(context.Context) (bool, error) { return false, errors.New("codex unavailable") },
		},
		{
			name:  "without probe",
			probe: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			output, emitted := runPreToolHook(t, "waypost mcp", tc.probe)
			if !emitted {
				t.Fatal("PreToolUse output is empty, want deny")
			}
			specific := output.HookSpecificOutput
			if specific.HookEventName != "PreToolUse" || specific.PermissionDecision != "deny" {
				t.Fatalf("hookSpecificOutput = %+v, want PreToolUse deny", specific)
			}
			if specific.PermissionDecisionReason != MCPServerCommandDenialReason {
				t.Fatalf("permissionDecisionReason = %q, want %q", specific.PermissionDecisionReason, MCPServerCommandDenialReason)
			}
		})
	}
}

func TestRunPreToolUseAllowsMCPPreferredWaypostCLIWhenMCPIsNotKnownAvailable(t *testing.T) {
	t.Parallel()

	_, emitted := runPreToolHook(t, "waypost recv --for workflow/reviewer", func(context.Context) (bool, error) {
		return false, nil
	})
	if emitted {
		t.Fatal("PreToolUse emitted output, want unavailable MCP to leave CLI command untouched")
	}
}

func TestRunPreToolUseReportsMCPProbeFailureWithoutDenyingCLI(t *testing.T) {
	t.Parallel()

	output, emitted := runPreToolHook(t, "waypost recv --for workflow/reviewer", func(context.Context) (bool, error) {
		return false, errors.New("codex unavailable")
	})
	if !emitted {
		t.Fatal("PreToolUse output is empty, want probe failure warning")
	}
	if output.SystemMessage != "Waypost MCP probe failed: codex unavailable" {
		t.Fatalf("systemMessage = %q, want probe failure reason", output.SystemMessage)
	}
	if output.HookSpecificOutput.PermissionDecision != "" {
		t.Fatalf("permissionDecision = %q, want CLI command allowed", output.HookSpecificOutput.PermissionDecision)
	}
}

func TestRunPreToolUseSkipsUnrelatedToolCalls(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		`{"hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{"command":"waypost read --latest --for workflow/reviewer"}}`,
		`{"hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{"command":"echo waypost send"}}`,
		`{"hook_event_name":"PreToolUse","tool_name":"apply_patch","tool_input":{"command":"waypost wait --for workflow/reviewer"}}`,
	} {
		var output bytes.Buffer
		if err := runWithMCPProbe(context.Background(), strings.NewReader(input), &output, func(context.Context) (bool, error) {
			t.Fatal("MCP probe called for an unrelated tool call")
			return false, nil
		}); err != nil {
			t.Fatalf("run(PreToolUse) error = %v", err)
		}
		if output.Len() != 0 {
			t.Fatalf("output = %q, want empty", output.String())
		}
	}
}

func TestLooksLikeWaypostNudge(t *testing.T) {
	t.Parallel()

	for _, prompt := range []string{
		"NOTICE: There might be new delivery in waypost.",
		"notice: there might be new delivery in WAYPOST.",
		"  NOTICE: There might be new delivery in waypost.\n",
	} {
		if !LooksLikeWaypostNudge(prompt) {
			t.Errorf("LooksLikeWaypostNudge(%q) = false, want true", prompt)
		}
	}
	for _, prompt := range []string{
		"Please explain the Waypost nudge message.",
		"NOTICE: deployment completed",
		"NOTICE: Waypost is configured",
		"Waypost delivery is pending",
		"NOTICE: investigate the Waypost message delivery bug",
		"NOTICE: investigate a pending Waypost message delivery bug",
		"NUDGE: check Waypost mail",
	} {
		if LooksLikeWaypostNudge(prompt) {
			t.Errorf("LooksLikeWaypostNudge(%q) = true, want false", prompt)
		}
	}
}

func TestLooksLikeWaypostWaitCommand(t *testing.T) {
	t.Parallel()

	for _, command := range []string{
		"waypost wait --for workflow/reviewer",
		"/home/alice/.local/bin/waypost --state-dir /tmp/waypost wait --timeout 30s",
		`'/opt/Waypost' --state-dir '/tmp/state with spaces' wait --json`,
		`& "C:\Users\alice\.local\bin\waypost.exe" wait --for workflow/reviewer`,
		`waypost.exe --state-dir "C:\Users\alice\Waypost State" wait --timeout 30s`,
		`waypost --state-dir=/tmp/waypost wait`,
	} {
		if !LooksLikeWaypostWaitCommand(command) {
			t.Errorf("LooksLikeWaypostWaitCommand(%q) = false, want true", command)
		}
	}
	for _, command := range []string{
		"echo 'waypost wait --for workflow/reviewer'",
		"waypost recv --for workflow/reviewer",
		"waypost doc wait",
		"my-waypost wait",
		"waypost --state-dir wait",
		"waypost\nwait --for workflow/reviewer",
		"cd /tmp && waypost wait --for workflow/reviewer",
	} {
		if LooksLikeWaypostWaitCommand(command) {
			t.Errorf("LooksLikeWaypostWaitCommand(%q) = true, want false", command)
		}
	}
}

func TestWaypostMCPDenialReason(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		command  string
		wantTool string
	}{
		{"waypost status", "waypost_status"},
		{"waypost ack --delivery dlv_1 --lease-token lease_1", "waypost_ack"},
		{"/home/alice/.local/bin/waypost --state-dir /tmp/state recv", "waypost_recv"},
		{`& "C:\Users\alice\.local\bin\waypost.exe" receive`, "waypost_recv"},
		{"waypost --state-dir=/tmp/state send", "waypost_send"},
	} {
		reason, guarded := waypostMCPDenialReason(tc.command)
		if !guarded || !strings.Contains(reason, tc.wantTool) {
			t.Errorf("waypostMCPDenialReason(%q) = %q, %v; want tool %q", tc.command, reason, guarded, tc.wantTool)
		}
	}
	if reason, guarded := waypostMCPDenialReason("waypost mcp --include-debug-tool"); !guarded || reason != MCPServerCommandDenialReason {
		t.Errorf("waypostMCPDenialReason(%q) = %q, %v; want unconditional MCP server denial", "waypost mcp --include-debug-tool", reason, guarded)
	}

	for _, command := range []string{
		"waypost wait --for workflow/reviewer",
		"waypost read --latest",
		"echo waypost send",
		"cd /tmp && waypost recv",
	} {
		if reason, guarded := waypostMCPDenialReason(command); guarded {
			t.Errorf("waypostMCPDenialReason(%q) = %q, true; want unguarded", command, reason)
		}
	}
}

func TestInstallRefreshesManagedGroupsInPlace(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	command := "'/opt/waypost' codex-hook"
	original := `{
  "hooks": {
    "SessionStart": [
      {
        "description": "Waypost Codex compact-context guard",
        "matcher": "^compact$",
        "hooks": [{"type": "command", "command": "'/old/waypost' codex-hook", "statusMessage": "Restoring Waypost compact context"}]
      },
      {
        "description": "trusted compact sibling",
        "matcher": "^resume$",
        "hooks": [{"type": "command", "command": "keep-compact-position"}]
      }
    ],
    "UserPromptSubmit": [
      {
        "description": "Waypost Codex nudge MCP hint",
        "hooks": [{"type": "command", "command": "'/old/waypost' codex-hook", "statusMessage": "Checking Waypost MCP availability"}]
      },
      {
        "description": "trusted prompt sibling",
        "hooks": [{"type": "command", "command": "keep-prompt-position"}]
      }
    ],
    "PreToolUse": [
      {
        "description": "Waypost Codex wait polling guard",
        "matcher": "^Bash$",
        "hooks": [{"type": "command", "command": "'/old/waypost' codex-hook", "statusMessage": "Checking Waypost wait usage"}]
      },
      {
        "description": "trusted tool sibling",
        "matcher": "^Bash$",
        "hooks": [{"type": "command", "command": "keep-tool-position"}]
      }
    ],
    "PostToolUse": [
      {
        "description": "Waypost Codex receive completion tracker",
        "matcher": "^(Bash|mcp__waypost__waypost_recv)$",
        "hooks": [{"type": "command", "command": "'/old/waypost' codex-hook"}]
      },
      {
        "description": "trusted post-tool sibling",
        "matcher": "^apply_patch$",
        "hooks": [{"type": "command", "command": "keep-post-tool-position"}]
      }
    ],
    "SessionEnd": [
      {
        "description": "Waypost Codex nudge state cleanup",
        "hooks": [{"type": "command", "command": "'/old/waypost' codex-hook"}]
      },
      {
        "description": "trusted session-end sibling",
        "hooks": [{"type": "command", "command": "keep-session-end-position"}]
      }
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}

	result, err := Install(home, command)
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Install() changed = false, want stale managed commands refreshed")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(hooks.json) error = %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("Unmarshal(hooks.json) error = %v", err)
	}
	hooks := document["hooks"].(map[string]any)

	compactGroups := hooks["SessionStart"].([]any)
	if !groupHasCommand(compactGroups[0].(map[string]any), command) {
		t.Fatalf("SessionStart[0] = %#v, want refreshed Waypost group", compactGroups[0])
	}
	if !groupHasCommand(compactGroups[1].(map[string]any), "keep-compact-position") {
		t.Fatalf("SessionStart[1] = %#v, want unrelated group at original position", compactGroups[1])
	}

	promptGroups := hooks["UserPromptSubmit"].([]any)
	if !groupHasCommand(promptGroups[0].(map[string]any), command) {
		t.Fatalf("UserPromptSubmit[0] = %#v, want refreshed Waypost group", promptGroups[0])
	}
	if !groupHasCommand(promptGroups[1].(map[string]any), "keep-prompt-position") {
		t.Fatalf("UserPromptSubmit[1] = %#v, want unrelated group at original position", promptGroups[1])
	}

	waitGroups := hooks["PreToolUse"].([]any)
	if !groupHasCommand(waitGroups[0].(map[string]any), command) {
		t.Fatalf("PreToolUse[0] = %#v, want refreshed Waypost group", waitGroups[0])
	}
	if !groupHasCommand(waitGroups[1].(map[string]any), "keep-tool-position") {
		t.Fatalf("PreToolUse[1] = %#v, want unrelated group at original position", waitGroups[1])
	}

	receiveGroups := hooks["PostToolUse"].([]any)
	if !groupHasCommand(receiveGroups[0].(map[string]any), command) {
		t.Fatalf("PostToolUse[0] = %#v, want refreshed Waypost group", receiveGroups[0])
	}
	if !groupHasCommand(receiveGroups[1].(map[string]any), "keep-post-tool-position") {
		t.Fatalf("PostToolUse[1] = %#v, want unrelated group at original position", receiveGroups[1])
	}

	cleanupGroups := hooks["SessionEnd"].([]any)
	if !groupHasCommand(cleanupGroups[0].(map[string]any), command) {
		t.Fatalf("SessionEnd[0] = %#v, want refreshed Waypost group", cleanupGroups[0])
	}
	if !groupHasCommand(cleanupGroups[1].(map[string]any), "keep-session-end-position") {
		t.Fatalf("SessionEnd[1] = %#v, want unrelated group at original position", cleanupGroups[1])
	}
}

func TestParseWaypostMCPAvailable(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		payload   string
		available bool
		wantError bool
	}{
		{name: "enabled", payload: `{"name":"waypost","enabled":true}`, available: true},
		{name: "disabled", payload: `{"name":"waypost","enabled":false}`},
		{name: "wrong server", payload: `{"name":"other","enabled":true}`, wantError: true},
		{name: "invalid", payload: `{`, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			available, err := parseWaypostMCPAvailable([]byte(tc.payload))
			if (err != nil) != tc.wantError {
				t.Fatalf("parseWaypostMCPAvailable() error = %v, wantError %v", err, tc.wantError)
			}
			if available != tc.available {
				t.Fatalf("parseWaypostMCPAvailable() = %v, want %v", available, tc.available)
			}
		})
	}
}

func TestCurrentDirectoryWaypostMCPAvailableTreatsMissingServerAsUnavailable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}

	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	probe := `#!/bin/sh
i=0
while [ "$i" -lt 100 ]; do
  printf '%s' 'verbose startup warning '
  i=$((i + 1))
done >&2
printf '\n' >&2
printf '%s\n' "Error: No MCP server named 'waypost' found." >&2
exit 1
`
	if err := os.WriteFile(codexPath, []byte(probe), 0o700); err != nil {
		t.Fatalf("WriteFile(codex probe) error = %v", err)
	}
	t.Setenv("PATH", binDir)

	available, err := CurrentDirectoryWaypostMCPAvailable(context.Background())
	if err != nil || available {
		t.Fatalf("CurrentDirectoryWaypostMCPAvailable() = %v, %v; want unavailable", available, err)
	}
}

func TestBoundedProbeErrorDetailPreservesHeadAndTail(t *testing.T) {
	t.Parallel()

	detail := boundedProbeErrorDetail([]byte("error prefix: " + strings.Repeat("x", 600) + " :root cause"))
	if !strings.HasPrefix(detail, "error prefix: ") || !strings.HasSuffix(detail, " :root cause") {
		t.Fatalf("boundedProbeErrorDetail() = %q, want preserved head and tail", detail)
	}
	if got := len([]rune(detail)); got != 500 {
		t.Fatalf("boundedProbeErrorDetail() length = %d, want 500 runes", got)
	}
}

func TestCurrentDirectoryWaypostMCPAvailableReportsCommandStderr(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}

	binDir := t.TempDir()
	codexPath := filepath.Join(binDir, "codex")
	probe := `#!/bin/sh
printf '%s\n' 'invalid Codex MCP configuration' >&2
exit 9
`
	if err := os.WriteFile(codexPath, []byte(probe), 0o700); err != nil {
		t.Fatalf("WriteFile(codex probe) error = %v", err)
	}
	t.Setenv("PATH", binDir)

	available, err := CurrentDirectoryWaypostMCPAvailable(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid Codex MCP configuration") {
		t.Fatalf("CurrentDirectoryWaypostMCPAvailable() = %v, %v; want stderr detail", available, err)
	}
}

func TestInstallPreservesUnrelatedHooksAndIsIdempotent(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	original := `{
  "custom": {"keep": true},
  "hooks": {
    "SessionStart": [
      {
        "description": "existing startup hook",
        "matcher": "^startup$",
        "hooks": [{"type": "command", "command": "load-startup"}]
      }
    ],
    "UserPromptSubmit": [
      {
        "description": "existing prompt hook",
        "hooks": [{"type": "command", "command": "inspect-prompt"}]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "^Bash$",
        "hooks": [{"type": "command", "command": "check-bash"}]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "^apply_patch$",
        "hooks": [{"type": "command", "command": "review-patch"}]
      }
    ],
    "SessionEnd": [
      {
        "hooks": [{"type": "command", "command": "archive-session"}]
      }
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o640); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}
	command := "'/opt/waypost' codex-hook"

	first, err := Install(home, command)
	if err != nil {
		t.Fatalf("Install(first) error = %v", err)
	}
	if !first.Changed || first.Path != path {
		t.Fatalf("Install(first) = %+v, want changed path %q", first, path)
	}
	firstContents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(first install) error = %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(firstContents, &document); err != nil {
		t.Fatalf("Unmarshal(first install) error = %v", err)
	}
	custom := document["custom"].(map[string]any)
	if custom["keep"] != true {
		t.Fatalf("custom field = %#v, want preserved", custom)
	}
	hooks := document["hooks"].(map[string]any)
	preToolGroups := hooks["PreToolUse"].([]any)
	if got := len(preToolGroups); got != 2 {
		t.Fatalf("PreToolUse hooks = %d, want existing plus Waypost", got)
	}
	if !groupHasCommand(preToolGroups[0].(map[string]any), "check-bash") || !groupHasCommand(preToolGroups[1].(map[string]any), command) {
		t.Fatalf("PreToolUse hooks = %#v, want preserved existing group followed by Waypost", preToolGroups)
	}
	if got := len(hooks["SessionStart"].([]any)); got != 2 {
		t.Fatalf("SessionStart hooks = %d, want existing plus Waypost", got)
	}
	if got := len(hooks["UserPromptSubmit"].([]any)); got != 2 {
		t.Fatalf("UserPromptSubmit hooks = %d, want existing plus Waypost", got)
	}
	postToolGroups := hooks["PostToolUse"].([]any)
	if got := len(postToolGroups); got != 2 {
		t.Fatalf("PostToolUse hooks = %d, want existing plus Waypost", got)
	}
	if !groupHasCommand(postToolGroups[0].(map[string]any), "review-patch") || !groupHasCommand(postToolGroups[1].(map[string]any), command) {
		t.Fatalf("PostToolUse hooks = %#v, want preserved existing group followed by Waypost", postToolGroups)
	}
	sessionEndGroups := hooks["SessionEnd"].([]any)
	if got := len(sessionEndGroups); got != 2 {
		t.Fatalf("SessionEnd hooks = %d, want existing plus Waypost", got)
	}
	if !groupHasCommand(sessionEndGroups[0].(map[string]any), "archive-session") || !groupHasCommand(sessionEndGroups[1].(map[string]any), command) {
		t.Fatalf("SessionEnd hooks = %#v, want preserved existing group followed by Waypost", sessionEndGroups)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(hooks.json) error = %v", err)
	}
	if got := info.Mode().Perm(); runtime.GOOS != "windows" && got != 0o640 {
		t.Fatalf("hooks.json mode = %o, want 640", got)
	}

	second, err := Install(home, command)
	if err != nil {
		t.Fatalf("Install(second) error = %v", err)
	}
	if second.Changed {
		t.Fatalf("Install(second) = %+v, want unchanged", second)
	}
	secondContents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(second install) error = %v", err)
	}
	if !bytes.Equal(firstContents, secondContents) {
		t.Fatal("idempotent install rewrote hooks.json")
	}

	diagnosis, err := Doctor(home, command)
	if err != nil {
		t.Fatalf("Doctor() error = %v", err)
	}
	if diagnosis.Path != path || diagnosis.Command != command {
		t.Fatalf("Doctor() = %+v, want path %q and command %q", diagnosis, path, command)
	}
}

func TestInstallPreservesSiblingHandlersWhenAdoptingGroups(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	command := "'/opt/waypost' codex-hook"
	original := `{
  "hooks": {
    "SessionStart": [
      {
        "matcher": "^compact$",
        "custom": "keep-compact",
        "hooks": [
          {"type": "command", "command": "'/opt/waypost' codex-hook"},
          {"type": "command", "command": "keep-compact-sibling"}
        ]
      }
    ],
    "UserPromptSubmit": [
      {
        "description": "Waypost Codex nudge MCP hint",
        "custom": "keep-prompt",
        "hooks": [
          {
            "type": "command",
            "command": "C:\\old-version\\waypost.exe codex-hook",
            "statusMessage": "Checking Waypost MCP availability"
          },
          {"type": "command", "command": "keep-prompt-sibling"}
        ]
      }
    ],
    "PreToolUse": [
      {
        "description": "Waypost Codex wait polling guard",
        "matcher": "^Bash$",
        "custom": "keep-wait",
        "hooks": [
          {
            "type": "command",
            "command": "C:\\old-version\\waypost.exe codex-hook",
            "statusMessage": "Checking Waypost wait usage"
          },
          {"type": "command", "command": "keep-wait-sibling"}
        ]
      }
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}

	if _, err := Install(home, command); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(hooks.json) error = %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("Unmarshal(hooks.json) error = %v", err)
	}
	hooks := document["hooks"].(map[string]any)

	compactGroups := hooks["SessionStart"].([]any)
	if got := len(compactGroups); got != 1 {
		t.Fatalf("SessionStart groups = %d, want mixed group updated in place", got)
	}
	preservedCompact := compactGroups[0].(map[string]any)
	if preservedCompact["custom"] != "keep-compact" || !groupHasCommand(preservedCompact, "keep-compact-sibling") {
		t.Fatalf("preserved compact group = %#v, want custom field and sibling handler", preservedCompact)
	}
	compactHandlers := preservedCompact["hooks"].([]any)
	if !handlerHasCommand(compactHandlers[0], command) || !handlerHasCommand(compactHandlers[1], "keep-compact-sibling") {
		t.Fatalf("compact handlers = %#v, want Waypost and sibling handler positions preserved", compactHandlers)
	}

	promptGroups := hooks["UserPromptSubmit"].([]any)
	if got := len(promptGroups); got != 1 {
		t.Fatalf("UserPromptSubmit groups = %d, want mixed group updated in place", got)
	}
	preservedPrompt := promptGroups[0].(map[string]any)
	if preservedPrompt["custom"] != "keep-prompt" || !groupHasCommand(preservedPrompt, "keep-prompt-sibling") {
		t.Fatalf("preserved prompt group = %#v, want custom field and sibling handler", preservedPrompt)
	}
	promptHandlers := preservedPrompt["hooks"].([]any)
	if !handlerHasCommand(promptHandlers[0], command) || !handlerHasCommand(promptHandlers[1], "keep-prompt-sibling") {
		t.Fatalf("prompt handlers = %#v, want Waypost and sibling handler positions preserved", promptHandlers)
	}

	waitGroups := hooks["PreToolUse"].([]any)
	if got := len(waitGroups); got != 1 {
		t.Fatalf("PreToolUse groups = %d, want mixed group updated in place", got)
	}
	preservedWait := waitGroups[0].(map[string]any)
	if preservedWait["custom"] != "keep-wait" || !groupHasCommand(preservedWait, "keep-wait-sibling") {
		t.Fatalf("preserved wait group = %#v, want custom field and sibling handler", preservedWait)
	}
	waitHandlers := preservedWait["hooks"].([]any)
	if !handlerHasCommand(waitHandlers[0], command) || !handlerHasCommand(waitHandlers[1], "keep-wait-sibling") {
		t.Fatalf("wait handlers = %#v, want Waypost and sibling handler positions preserved", waitHandlers)
	}

	second, err := Install(home, command)
	if err != nil {
		t.Fatalf("Install(second) error = %v", err)
	}
	if second.Changed {
		t.Fatalf("Install(second) = %+v, want migrated mixed groups to be idempotent", second)
	}
	secondContents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(second hooks.json) error = %v", err)
	}
	if !bytes.Equal(contents, secondContents) {
		t.Fatal("second install rewrote migrated mixed groups")
	}
}

func TestCurrentCommandUsesStableLauncherPath(t *testing.T) {
	stable := filepath.Join(t.TempDir(), "waypost.exe")
	t.Setenv(launchpath.StableExecutableEnv, stable)

	command, err := CurrentCommand()
	if err != nil {
		t.Fatalf("CurrentCommand() error = %v", err)
	}
	want := quoteCommandPath(stable) + " codex-hook"
	if runtime.GOOS == "windows" {
		want = "& " + want
	}
	if command != want {
		t.Fatalf("CurrentCommand() = %q, want %q", command, want)
	}
}

func TestCurrentCommandRejectsRelativeLauncherPath(t *testing.T) {
	t.Setenv(launchpath.StableExecutableEnv, "relative/waypost.exe")
	if _, err := CurrentCommand(); err == nil {
		t.Fatal("CurrentCommand() error = nil, want relative stable launcher rejection")
	}
}

func TestManagedGroupsUseShortTimeout(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		group   map[string]any
		timeout json.Number
	}{
		"compact": {group: compactManagedGroup("waypost codex-hook"), timeout: hookTimeoutJSON},
		"prompt":  {group: promptManagedGroup("waypost codex-hook"), timeout: hookTimeoutJSON},
		"wait":    {group: waitManagedGroup("waypost codex-hook"), timeout: hookTimeoutJSON},
		"receive": {group: receiveManagedGroup("waypost codex-hook"), timeout: hookTimeoutJSON},
		"cleanup": {group: cleanupManagedGroup("waypost codex-hook"), timeout: cleanupHookTimeoutJSON},
	} {
		handlers := tc.group["hooks"].([]any)
		handler := handlers[0].(map[string]any)
		if got := handler["timeout"]; got != tc.timeout {
			t.Errorf("%s timeout = %#v, want %s", name, got, tc.timeout)
		}
	}
}

func TestInstallRejectsMalformedMatcherGroupsWithoutChangingFile(t *testing.T) {
	t.Parallel()

	for _, event := range []string{"SessionStart", "PreToolUse", "PostToolUse", "SessionEnd"} {
		t.Run(event, func(t *testing.T) {
			t.Parallel()

			home := t.TempDir()
			path := filepath.Join(home, "hooks.json")
			original := []byte(`{"hooks":{"` + event + `":["malformed"]}}`)
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatalf("WriteFile(hooks.json) error = %v", err)
			}
			if _, err := Install(home, "waypost codex-hook"); err == nil {
				t.Fatal("Install() error = nil, want malformed matcher group rejection")
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile(hooks.json) error = %v", err)
			}
			if !bytes.Equal(contents, original) {
				t.Fatalf("hooks.json = %q, want unchanged %q", contents, original)
			}
		})
	}
}

func TestInstallPreservesSymlinkAndUpdatesTarget(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	home := filepath.Join(root, "codex")
	managedDir := filepath.Join(root, "managed")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatalf("MkdirAll(codex home) error = %v", err)
	}
	if err := os.MkdirAll(managedDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(managed dir) error = %v", err)
	}
	target := filepath.Join(managedDir, "hooks.json")
	if err := os.WriteFile(target, []byte("{}\n"), 0o640); err != nil {
		t.Fatalf("WriteFile(target hooks.json) error = %v", err)
	}
	path := filepath.Join(home, "hooks.json")
	if err := os.Symlink(filepath.Join("..", "managed", "hooks.json"), path); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable on Windows: %v", err)
		}
		t.Fatalf("Symlink(hooks.json) error = %v", err)
	}

	command := "waypost codex-hook"
	if _, err := Install(home, command); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat(hooks.json) error = %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("hooks.json mode = %v, want symlink preserved", info.Mode())
	}
	targetContents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile(target hooks.json) error = %v", err)
	}
	if !bytes.Contains(targetContents, []byte(command)) {
		t.Fatalf("target hooks.json = %q, want installed command", targetContents)
	}
	targetInfo, err := os.Stat(target)
	if err != nil {
		t.Fatalf("Stat(target hooks.json) error = %v", err)
	}
	if got := targetInfo.Mode().Perm(); runtime.GOOS != "windows" && got != 0o640 {
		t.Fatalf("target hooks.json mode = %o, want 640", got)
	}
}

func TestInstallPreservesExactJSONNumbers(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	const preciseNumber = "9007199254740993"
	original := []byte(`{"external":{"id":` + preciseNumber + `}}`)
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}
	if _, err := Install(home, "waypost codex-hook"); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(hooks.json) error = %v", err)
	}
	if !bytes.Contains(contents, []byte(preciseNumber)) {
		t.Fatalf("hooks.json = %q, want exact number %s", contents, preciseNumber)
	}
}

func TestInstallRejectsInvalidJSONWithoutChangingFile(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	original := []byte("{not-json\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}
	if _, err := Install(home, "waypost codex-hook"); err == nil {
		t.Fatal("Install() error = nil, want invalid JSON error")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(hooks.json) error = %v", err)
	}
	if !bytes.Equal(contents, original) {
		t.Fatalf("hooks.json = %q, want original invalid contents %q", contents, original)
	}
}

func TestDoctorRejectsMatcherThatAlsoRunsOnStartup(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	contents := `{
  "hooks": {
    "SessionStart": [
      {
        "matcher": ".*",
        "hooks": [{"type": "command", "command": "waypost codex-hook"}]
      }
    ]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}
	if _, err := Doctor(home, "waypost codex-hook"); err == nil {
		t.Fatal("Doctor() error = nil, want broad matcher rejection")
	}
}

func TestDoctorRequiresWaypostWaitPollingGuard(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	contents := `{
  "hooks": {
    "SessionStart": [{
      "matcher": "^compact$",
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }],
    "UserPromptSubmit": [{
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}
	_, err := Doctor(home, "waypost codex-hook")
	if err == nil || !strings.Contains(err.Error(), "wait polling guard") {
		t.Fatalf("Doctor() error = %v, want missing wait polling guard error", err)
	}
}

func TestDoctorRequiresWaypostReceiveCompletionTracker(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	contents := `{
  "hooks": {
    "SessionStart": [{
      "matcher": "^compact$",
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }],
    "UserPromptSubmit": [{
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }],
    "PreToolUse": [{
      "matcher": "^Bash$",
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}
	_, err := Doctor(home, "waypost codex-hook")
	if err == nil || !strings.Contains(err.Error(), "receive completion") {
		t.Fatalf("Doctor() error = %v, want missing receive completion tracker error", err)
	}
}

func TestDoctorRequiresWaypostNudgeStateCleanup(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	contents := `{
  "hooks": {
    "SessionStart": [{
      "matcher": "^compact$",
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }],
    "UserPromptSubmit": [{
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }],
    "PreToolUse": [{
      "matcher": "^Bash$",
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }],
    "PostToolUse": [{
      "matcher": "^(Bash|mcp__waypost__waypost_recv)$",
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}
	_, err := Doctor(home, "waypost codex-hook")
	if err == nil || !strings.Contains(err.Error(), "nudge state cleanup") {
		t.Fatalf("Doctor() error = %v, want missing nudge state cleanup error", err)
	}
}

func TestDoctorRejectsMalformedHookSource(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	contents := `{
  "hooks": {
    "SessionStart": [{
      "matcher": "^compact$",
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }],
    "UserPromptSubmit": [{
      "hooks": [{"type": "command", "command": "waypost codex-hook", "timeout": 5}]
    }],
    "PreToolUse": ["malformed"]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}
	_, err := Doctor(home, "waypost codex-hook")
	if err == nil || !strings.Contains(err.Error(), "PreToolUse") {
		t.Fatalf("Doctor() error = %v, want malformed PreToolUse rejection", err)
	}
}

func TestDoctorRejectsManagedHandlersWithoutShortTimeout(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	contents := `{
  "hooks": {
    "SessionStart": [{
      "matcher": "^compact$",
      "hooks": [{"type": "command", "command": "waypost codex-hook"}]
    }],
    "UserPromptSubmit": [{
      "hooks": [{"type": "command", "command": "waypost codex-hook"}]
    }]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}
	if _, err := Doctor(home, "waypost codex-hook"); err == nil {
		t.Fatal("Doctor() error = nil, want missing timeout rejection")
	}
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
	if !emitted {
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
