package devinhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ruiheng/waypost/internal/hookcore"
	"github.com/ruiheng/waypost/internal/launchpath"
)

func TestWriteOutputEmitsPostCompactionAdditionalContext(t *testing.T) {
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
	if got := specific["hookEventName"]; got != "PostCompaction" {
		t.Fatalf("hookEventName = %v, want PostCompaction", got)
	}
	context, _ := specific["additionalContext"].(string)
	if context != AdditionalContext {
		t.Fatalf("additionalContext = %q, want compact guard %q", context, AdditionalContext)
	}
}

func TestRunWithoutInputEmitsCompactGuard(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	if err := runWithMCPProbe(context.Background(), strings.NewReader(""), &output, nil); err != nil {
		t.Fatalf("runWithMCPProbe(empty input) error = %v", err)
	}
	var payload hookOutput
	if err := json.Unmarshal(output.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal(output) error = %v", err)
	}
	if payload.HookSpecificOutput == nil || payload.HookSpecificOutput.HookEventName != "PostCompaction" {
		t.Fatalf("hookSpecificOutput = %+v, want PostCompaction guard", payload.HookSpecificOutput)
	}
	if payload.HookSpecificOutput.AdditionalContext != AdditionalContext {
		t.Fatalf("additionalContext = %q, want %q", payload.HookSpecificOutput.AdditionalContext, AdditionalContext)
	}
}

func TestNudgeLifecycleControlsCompactGuard(t *testing.T) {
	t.Parallel()

	const sessionID = "session-nudge-lifecycle"
	store := newMemoryNudgeStateStore()
	probe := func(context.Context) (waypostMCPStatus, error) { return waypostMCPStatus{available: true}, nil }

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

	toolResponse := json.RawMessage(`{"success":true,"output":"{\"structuredContent\":{\"status\":\"received\"}}"}`)
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

func TestSessionStartCompactSourceTriggersGuard(t *testing.T) {
	t.Parallel()

	const sessionID = "session-start-sources"
	store := newMemoryNudgeStateStore()
	if err := store.Save(sessionID, nudgeConsumed); err != nil {
		t.Fatalf("Save(consumed) error = %v", err)
	}
	output, emitted := runHook(t, store, nil, hookInput{
		HookEventName: "SessionStart",
		SessionID:     sessionID,
		Source:        "compact",
	})
	if !emitted || output.HookSpecificOutput.AdditionalContext != AdditionalContext {
		t.Fatalf("SessionStart(compact) output = %+v, %v; want compact guard", output, emitted)
	}
	for _, source := range []string{"startup", "resume", "clear", ""} {
		if output, emitted := runHook(t, store, nil, hookInput{
			HookEventName: "SessionStart",
			SessionID:     sessionID,
			Source:        source,
		}); emitted {
			t.Fatalf("SessionStart(%q) output = %+v, want empty", source, output)
		}
	}
}

func TestPostToolUseConsumesPendingNudgeOnlyAfterSuccessfulReceive(t *testing.T) {
	t.Parallel()

	mcpResponse := func(output string) json.RawMessage {
		encoded, err := json.Marshal(map[string]any{"success": true, "output": output})
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	execResponse := func(output string) json.RawMessage {
		encoded, err := json.Marshal(map[string]any{"success": true, "output": output})
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}

	for _, tc := range []struct {
		name         string
		toolName     string
		toolInput    json.RawMessage
		toolResponse json.RawMessage
		wantConsumed bool
	}{
		{name: "MCP received", toolName: receiveMCPToolName, toolResponse: mcpResponse(`{"structuredContent":{"status":"received"}}`), wantConsumed: true},
		{name: "MCP no message", toolName: receiveMCPToolName, toolResponse: mcpResponse(`{"structuredContent":{"status":"no_message"}}`), wantConsumed: true},
		{name: "MCP top-level status", toolName: receiveMCPToolName, toolResponse: mcpResponse(`{"status":"received"}`), wantConsumed: true},
		{name: "MCP active leases", toolName: receiveMCPToolName, toolResponse: mcpResponse(`{"structuredContent":{"status":"active_leases"}}`)},
		{name: "MCP recovery required", toolName: receiveMCPToolName, toolResponse: mcpResponse(`{"structuredContent":{"status":"receive_recovery_required"}}`)},
		{name: "MCP error", toolName: receiveMCPToolName, toolResponse: mcpResponse(`{"isError":true,"structuredContent":{"status":"received"}}`)},
		{name: "MCP tool failure", toolName: receiveMCPToolName, toolResponse: json.RawMessage(`{"success":false,"output":"{\"structuredContent\":{\"status\":\"received\"}}"}`)},
		{name: "CLI JSON received", toolName: execToolName, toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: execResponse(`{"status":"received","delivery":{"delivery_id":"dlv_1"}}`), wantConsumed: true},
		{name: "CLI JSON no message", toolName: execToolName, toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: execResponse(`{"status":"no_message"}`), wantConsumed: true},
		{name: "CLI JSON full personal", toolName: execToolName, toolInput: json.RawMessage(`{"command":"waypost recv --json --full"}`), toolResponse: execResponse(`{"delivery_id":"dlv_1","message_id":"msg_1","recipient_address":"agent/reviewer","lease_token":"lease_1","body":"review"}`), wantConsumed: true},
		{name: "CLI text no message", toolName: execToolName, toolInput: json.RawMessage(`{"command":"waypost recv"}`), toolResponse: execResponse("status=no_message\n"), wantConsumed: true},
		{name: "CLI text received", toolName: execToolName, toolInput: json.RawMessage(`{"command":"waypost recv"}`), toolResponse: execResponse("delivery_id=dlv_1 recipient_address=agent/reviewer lease_token=lease_1 content_type=text/plain subject=\"review\"\nbody\n"), wantConsumed: true},
		{name: "CLI exec failure", toolName: execToolName, toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: json.RawMessage(`{"success":false,"output":"{\"status\":\"received\"}"}`)},
		{name: "CLI JSON failure", toolName: execToolName, toolInput: json.RawMessage(`{"command":"waypost recv --json"}`), toolResponse: execResponse(`{"status":"error","error_code":"busy"}`)},
		{name: "unrelated exec", toolName: execToolName, toolInput: json.RawMessage(`{"command":"go test ./..."}`), toolResponse: execResponse("ok")},
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
		ToolResponse:  json.RawMessage(`{"success":true,"output":"{\"status\":\"received\"}"}`),
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
	if err := store.SaveMCPProbe(sessionID, waypostMCPStatus{available: true}.probeRecord()); err != nil {
		t.Fatalf("SaveMCPProbe() error = %v", err)
	}
	if output, emitted := runHook(t, store, nil, hookInput{HookEventName: "SessionEnd", SessionID: sessionID}); emitted {
		t.Fatalf("SessionEnd output = %+v, want empty", output)
	}
	if state, err := store.Load(sessionID); err != nil || state != nudgeNone {
		t.Fatalf("state after SessionEnd = %q, %v; want none", state, err)
	}
	if _, ok, err := store.LoadMCPProbe(sessionID); err != nil || ok {
		t.Fatalf("probe after SessionEnd = ok %v, %v; want cleared", ok, err)
	}
}

func TestFileNudgeStateStorePersistsAndClearsSessionState(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "hook-state")
	store := fileNudgeStateStore{Dir: stateDir, Label: harnessLabel}
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

func TestRunUserPromptNudgeSelectsReceivePathFromDevinProbe(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		available   bool
		probeErr    error
		wantContext string
		wantText    string
		rejectText  string
	}{
		{
			name:        "MCP available",
			available:   true,
			wantContext: MCPNudgeContext,
			wantText:    "The waypost_recv MCP tool is available. Use it instead of the Waypost CLI.",
			rejectText:  "waypost recv --json",
		},
		{
			name:        "MCP unavailable",
			wantContext: CLINudgeContext,
			wantText:    "waypost recv --json",
			rejectText:  "MCP tool is available",
		},
		{
			name:        "probe failed",
			probeErr:    errors.New("devin unavailable"),
			wantContext: "Waypost MCP probe failed: devin unavailable " + MCPProbeFailedNudgeContext,
			wantText:    "Look for the waypost_recv MCP tool",
			rejectText:  "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			probeCalls := 0
			probe := func(context.Context) (waypostMCPStatus, error) {
				probeCalls++
				return waypostMCPStatus{available: tc.available}, tc.probeErr
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
			if !strings.Contains(specific.AdditionalContext, tc.wantText) {
				t.Fatalf("additionalContext = %q, want %q", specific.AdditionalContext, tc.wantText)
			}
			if tc.rejectText != "" && strings.Contains(specific.AdditionalContext, tc.rejectText) {
				t.Fatalf("additionalContext = %q, reject %q", specific.AdditionalContext, tc.rejectText)
			}
			if payload.Decision != "" {
				t.Fatalf("decision = %q, want empty", payload.Decision)
			}
		})
	}
}

func TestRunUserPromptNudgeStartsDevinMCPProbe(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}

	binDir := t.TempDir()
	devinPath := filepath.Join(binDir, "devin")
	probe := `#!/bin/sh
if [ "$1" != "mcp" ] || [ "$2" != "get" ] || [ "$3" != "waypost" ]; then
  exit 9
fi
printf '%s\n' 'Server: waypost' '    Command: waypost mcp'
`
	if err := os.WriteFile(devinPath, []byte(probe), 0o700); err != nil {
		t.Fatalf("WriteFile(devin probe) error = %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	realProbe := func(ctx context.Context) (waypostMCPStatus, error) {
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
	if payload.HookSpecificOutput.AdditionalContext != MCPNudgeContext {
		t.Fatalf("additionalContext = %q, want %q", payload.HookSpecificOutput.AdditionalContext, MCPNudgeContext)
	}
}

func TestRunReusesMCPProbeForSession(t *testing.T) {
	store := newMemoryNudgeStateStore()
	probeCalls := 0
	probe := func(context.Context) (waypostMCPStatus, error) {
		probeCalls++
		return waypostMCPStatus{available: true}, nil
	}
	for _, input := range []hookInput{
		{HookEventName: "UserPromptSubmit", SessionID: "cached-session", Prompt: defaultNudgeMessage},
		{HookEventName: "UserPromptSubmit", SessionID: "cached-session", Prompt: "ordinary prompt"},
		{HookEventName: "PostCompaction", SessionID: "cached-session"},
		{HookEventName: "PreToolUse", SessionID: "cached-session", ToolName: execToolName, ToolInput: json.RawMessage(`{"command":"waypost recv"}`)},
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
	store := fileNudgeStateStore{Dir: t.TempDir(), Label: harnessLabel}
	sessionID := "incomplete-probe"
	path, err := store.ProbePath(sessionID)
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

func TestFileMCPProbeCanonicalizesNamespacedDisabledTools(t *testing.T) {
	t.Parallel()

	store := fileNudgeStateStore{Dir: t.TempDir(), Label: harnessLabel}
	const sessionID = "namespaced-probe"
	path, err := store.ProbePath(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	// Cache written before namespaced tool ids were normalized, containing
	// duplicates and an entry belonging to a different MCP server.
	cache := `{"session_id":"namespaced-probe","available":true,"disabled_tools":["mcp__waypost__waypost_recv","mcp__waypost__waypost_recv","mcp__other__waypost_send","waypost_send"]}`
	if err := os.WriteFile(path, []byte(cache), 0o600); err != nil {
		t.Fatal(err)
	}
	status, err := probeWaypostMCP(context.Background(), sessionID, func(context.Context) (waypostMCPStatus, error) {
		t.Fatal("probe called despite cache hit")
		return waypostMCPStatus{}, nil
	}, store)
	if err != nil {
		t.Fatalf("probeWaypostMCP() error = %v", err)
	}
	if !status.available {
		t.Fatal("status.available = false, want true")
	}
	if status.toolUsable("waypost_recv") || status.toolUsable("waypost_send") {
		t.Fatalf("toolUsable() = %v/%v, want cached namespaced disabled tools honored", status.toolUsable("waypost_recv"), status.toolUsable("waypost_send"))
	}
	if !status.toolUsable("waypost_status") {
		t.Fatal("toolUsable(waypost_status) = false, want usable")
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
	err := runWithMCPProbe(context.Background(), input, &output, func(context.Context) (waypostMCPStatus, error) {
		t.Fatal("MCP probe called for an ordinary prompt")
		return waypostMCPStatus{}, nil
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
  "tool_name": "exec",
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
	if _, denied := payload["decision"]; denied {
		t.Fatalf("decision = %#v, want soft warning instead of block", payload["decision"])
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
			output, emitted := runPreToolHook(t, tc.command, func(context.Context) (waypostMCPStatus, error) {
				probeCalls++
				return waypostMCPStatus{available: true}, nil
			})
			if !emitted {
				t.Fatal("PreToolUse output is empty, want block")
			}
			if probeCalls != 1 {
				t.Fatalf("probe calls = %d, want 1", probeCalls)
			}
			if output.Decision != "block" {
				t.Fatalf("decision = %q, want block", output.Decision)
			}
			if !strings.Contains(output.Reason, tc.wantReason) {
				t.Fatalf("reason = %q, want %q", output.Reason, tc.wantReason)
			}
			if output.HookSpecificOutput != nil {
				t.Fatalf("hookSpecificOutput = %+v, want empty block output", output.HookSpecificOutput)
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
			probe: func(context.Context) (waypostMCPStatus, error) { return waypostMCPStatus{available: true}, nil },
		},
		{
			name:  "MCP unavailable",
			probe: func(context.Context) (waypostMCPStatus, error) { return waypostMCPStatus{}, nil },
		},
		{
			name: "MCP probe failed",
			probe: func(context.Context) (waypostMCPStatus, error) {
				return waypostMCPStatus{}, errors.New("devin unavailable")
			},
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
				t.Fatal("PreToolUse output is empty, want block")
			}
			if output.Decision != "block" {
				t.Fatalf("decision = %q, want block", output.Decision)
			}
			if output.Reason != MCPServerCommandDenialReason {
				t.Fatalf("reason = %q, want %q", output.Reason, MCPServerCommandDenialReason)
			}
		})
	}
}

func TestRunPreToolUseAllowsMCPPreferredWaypostCLIWhenMCPIsNotKnownAvailable(t *testing.T) {
	t.Parallel()

	_, emitted := runPreToolHook(t, "waypost recv --for workflow/reviewer", func(context.Context) (waypostMCPStatus, error) {
		return waypostMCPStatus{}, nil
	})
	if emitted {
		t.Fatal("PreToolUse emitted output, want unavailable MCP to leave CLI command untouched")
	}
}

func TestRunPreToolUseReportsMCPProbeFailureWithoutDenyingCLI(t *testing.T) {
	t.Parallel()

	output, emitted := runPreToolHook(t, "waypost recv --for workflow/reviewer", func(context.Context) (waypostMCPStatus, error) {
		return waypostMCPStatus{}, errors.New("devin unavailable")
	})
	if !emitted {
		t.Fatal("PreToolUse output is empty, want probe failure warning")
	}
	if output.Decision != "" {
		t.Fatalf("decision = %q, want CLI command allowed", output.Decision)
	}
	if output.HookSpecificOutput == nil || !strings.Contains(output.HookSpecificOutput.AdditionalContext, "Waypost MCP probe failed: devin unavailable") {
		t.Fatalf("additionalContext = %+v, want probe failure detail", output.HookSpecificOutput)
	}
}

func TestRunPreToolUseSkipsUnrelatedToolCalls(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		`{"hook_event_name":"PreToolUse","tool_name":"exec","tool_input":{"command":"waypost read --latest --for workflow/reviewer"}}`,
		`{"hook_event_name":"PreToolUse","tool_name":"exec","tool_input":{"command":"echo waypost send"}}`,
		`{"hook_event_name":"PreToolUse","tool_name":"edit","tool_input":{"command":"waypost wait --for workflow/reviewer"}}`,
	} {
		var output bytes.Buffer
		if err := runWithMCPProbe(context.Background(), strings.NewReader(input), &output, func(context.Context) (waypostMCPStatus, error) {
			t.Fatal("MCP probe called for an unrelated tool call")
			return waypostMCPStatus{}, nil
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
		{"waypost --state-dir=/tmp/state send", "waypost_send"},
	} {
		reason, tool, guarded := waypostMCPDenialReason(tc.command)
		if !guarded || tool != tc.wantTool || !strings.Contains(reason, tc.wantTool) {
			t.Errorf("waypostMCPDenialReason(%q) = %q, %q, %v; want tool %q", tc.command, reason, tool, guarded, tc.wantTool)
		}
	}
	if reason, tool, guarded := waypostMCPDenialReason("waypost mcp"); !guarded || tool != "" || reason != MCPServerCommandDenialReason {
		t.Errorf("waypostMCPDenialReason(%q) = %q, %q, %v; want unconditional MCP server denial", "waypost mcp", reason, tool, guarded)
	}

	for _, command := range []string{
		"waypost wait --for workflow/reviewer",
		"waypost read --latest",
		"echo waypost send",
		"cd /tmp && waypost recv",
	} {
		if reason, _, guarded := waypostMCPDenialReason(command); guarded {
			t.Errorf("waypostMCPDenialReason(%q) = %q, true; want unguarded", command, reason)
		}
	}
}

func TestInstallRefreshesManagedGroupsInPlace(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	command := "'/opt/waypost' devin-hook"
	original := `{
  "hooks": {
    "PostCompaction": [
      {
        "hooks": [{"type": "command", "command": "'/old/waypost' devin-hook"}]
      },
      {
        "hooks": [{"type": "command", "command": "keep-compact-position"}]
      }
    ],
    "UserPromptSubmit": [
      {
        "hooks": [{"type": "command", "command": "'/old/waypost' devin-hook"}]
      },
      {
        "hooks": [{"type": "command", "command": "keep-prompt-position"}]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "^exec$",
        "hooks": [{"type": "command", "command": "'/old/waypost' devin-hook"}]
      },
      {
        "matcher": "^exec$",
        "hooks": [{"type": "command", "command": "keep-tool-position"}]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "^(exec|mcp__waypost__waypost_recv)$",
        "hooks": [{"type": "command", "command": "'/old/waypost' devin-hook"}]
      },
      {
        "matcher": "^edit$",
        "hooks": [{"type": "command", "command": "keep-post-tool-position"}]
      }
    ],
    "SessionEnd": [
      {
        "hooks": [{"type": "command", "command": "'/old/waypost' devin-hook"}]
      },
      {
        "hooks": [{"type": "command", "command": "keep-session-end-position"}]
      }
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}

	result, err := Install(configDir, command)
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Install() changed = false, want stale managed commands refreshed")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(config.json) error = %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("Unmarshal(config.json) error = %v", err)
	}
	hooks := document["hooks"].(map[string]any)

	compactGroups := hooks["PostCompaction"].([]any)
	if !groupHasCommand(compactGroups[0].(map[string]any), command) {
		t.Fatalf("PostCompaction[0] = %#v, want refreshed Waypost group", compactGroups[0])
	}
	if !groupHasCommand(compactGroups[1].(map[string]any), "keep-compact-position") {
		t.Fatalf("PostCompaction[1] = %#v, want unrelated group at original position", compactGroups[1])
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

func TestInstallPreservesSiblingHandlersWhenAdoptingGroups(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	command := "'/opt/waypost' devin-hook"
	original := `{
  "hooks": {
    "UserPromptSubmit": [
      {
        "custom": "keep-prompt",
        "hooks": [
          {"type": "command", "command": "'/old/waypost' devin-hook"},
          {"type": "command", "command": "keep-prompt-sibling"}
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "^exec$",
        "custom": "keep-wait",
        "hooks": [
          {"type": "command", "command": "C:\\old-version\\waypost.exe devin-hook"},
          {"type": "command", "command": "keep-wait-sibling"}
        ]
      }
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}

	if _, err := Install(configDir, command); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(config.json) error = %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("Unmarshal(config.json) error = %v", err)
	}
	hooks := document["hooks"].(map[string]any)

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

	second, err := Install(configDir, command)
	if err != nil {
		t.Fatalf("Install(second) error = %v", err)
	}
	if second.Changed {
		t.Fatalf("Install(second) = %+v, want migrated mixed groups to be idempotent", second)
	}
	secondContents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(second config.json) error = %v", err)
	}
	if !bytes.Equal(contents, secondContents) {
		t.Fatal("second install rewrote migrated mixed groups")
	}
}

func TestInstallPreservesUnrelatedHooksAndIsIdempotent(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	original := `{
  "custom": {"keep": true},
  "hooks": {
    "PostCompaction": [
      {
        "hooks": [{"type": "command", "command": "restore-context"}]
      }
    ],
    "UserPromptSubmit": [
      {
        "hooks": [{"type": "command", "command": "inspect-prompt"}]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "^exec$",
        "hooks": [{"type": "command", "command": "check-exec"}]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "^edit$",
        "hooks": [{"type": "command", "command": "review-edit"}]
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
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}
	command := "'/opt/waypost' devin-hook"

	first, err := Install(configDir, command)
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
	if !groupHasCommand(preToolGroups[0].(map[string]any), "check-exec") || !groupHasCommand(preToolGroups[1].(map[string]any), command) {
		t.Fatalf("PreToolUse hooks = %#v, want preserved existing group followed by Waypost", preToolGroups)
	}
	if got := len(hooks["PostCompaction"].([]any)); got != 2 {
		t.Fatalf("PostCompaction hooks = %d, want existing plus Waypost", got)
	}
	if got := len(hooks["SessionStart"].([]any)); got != 1 {
		t.Fatalf("SessionStart hooks = %d, want Waypost group", got)
	}
	if got := len(hooks["UserPromptSubmit"].([]any)); got != 2 {
		t.Fatalf("UserPromptSubmit hooks = %d, want existing plus Waypost", got)
	}
	postToolGroups := hooks["PostToolUse"].([]any)
	if got := len(postToolGroups); got != 2 {
		t.Fatalf("PostToolUse hooks = %d, want existing plus Waypost", got)
	}
	if !groupHasCommand(postToolGroups[0].(map[string]any), "review-edit") || !groupHasCommand(postToolGroups[1].(map[string]any), command) {
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
		t.Fatalf("Stat(config.json) error = %v", err)
	}
	if got := info.Mode().Perm(); runtime.GOOS != "windows" && got != 0o640 {
		t.Fatalf("config.json mode = %o, want 640", got)
	}

	second, err := Install(configDir, command)
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
		t.Fatal("idempotent install rewrote config.json")
	}

	diagnosis, err := Doctor(configDir, command)
	if err != nil {
		t.Fatalf("Doctor() error = %v", err)
	}
	if diagnosis.Path != path || diagnosis.Command != command {
		t.Fatalf("Doctor() = %+v, want path %q and command %q", diagnosis, path, command)
	}
}

func TestManagedGroupsUseShortTimeout(t *testing.T) {
	t.Parallel()

	for _, spec := range managedHookSpecs("waypost devin-hook") {
		want := hookTimeoutJSON
		if spec.event == "SessionEnd" {
			want = cleanupHookTimeoutJSON
		}
		group := spec.desired("waypost devin-hook")
		handlers := group["hooks"].([]any)
		handler := handlers[0].(map[string]any)
		if got := handler["timeout"]; got != want {
			t.Errorf("%s timeout = %#v, want %s", spec.event, got, want)
		}
	}
}

func TestManagedMatchersTargetDevinTools(t *testing.T) {
	t.Parallel()

	var preTool, postTool map[string]any
	for _, spec := range managedHookSpecs("waypost devin-hook") {
		switch spec.event {
		case "PreToolUse":
			preTool = spec.desired("waypost devin-hook")
		case "PostToolUse":
			postTool = spec.desired("waypost devin-hook")
		case "PostCompaction", "SessionStart", "UserPromptSubmit", "SessionEnd":
			group := spec.desired("waypost devin-hook")
			if _, exists := group["matcher"]; exists {
				t.Errorf("%s group = %#v, want no matcher for lifecycle event", spec.event, group)
			}
		}
	}
	if got := preTool["matcher"]; got != "^exec$" {
		t.Fatalf("PreToolUse matcher = %v, want ^exec$", got)
	}
	if got := postTool["matcher"]; got != "^(exec|mcp__waypost__waypost_recv)$" {
		t.Fatalf("PostToolUse matcher = %v, want exec plus waypost_recv", got)
	}
}

func TestInstallRejectsMalformedMatcherGroupsWithoutChangingFile(t *testing.T) {
	t.Parallel()

	for _, event := range []string{"PostCompaction", "SessionStart", "PreToolUse", "PostToolUse", "SessionEnd"} {
		t.Run(event, func(t *testing.T) {
			t.Parallel()

			configDir := t.TempDir()
			path := filepath.Join(configDir, "config.json")
			original := []byte(`{"hooks":{"` + event + `":["malformed"]}}`)
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatalf("WriteFile(config.json) error = %v", err)
			}
			if _, err := Install(configDir, "waypost devin-hook"); err == nil {
				t.Fatal("Install() error = nil, want malformed matcher group rejection")
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile(config.json) error = %v", err)
			}
			if !bytes.Equal(contents, original) {
				t.Fatalf("config.json = %q, want unchanged %q", contents, original)
			}
		})
	}
}

func TestInstallPreservesSymlinkAndUpdatesTarget(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	configDir := filepath.Join(root, "devin")
	managedDir := filepath.Join(root, "managed")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(devin config dir) error = %v", err)
	}
	if err := os.MkdirAll(managedDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(managed dir) error = %v", err)
	}
	target := filepath.Join(managedDir, "config.json")
	if err := os.WriteFile(target, []byte("{}\n"), 0o640); err != nil {
		t.Fatalf("WriteFile(target config.json) error = %v", err)
	}
	path := filepath.Join(configDir, "config.json")
	if err := os.Symlink(filepath.Join("..", "managed", "config.json"), path); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable on Windows: %v", err)
		}
		t.Fatalf("Symlink(config.json) error = %v", err)
	}

	command := "waypost devin-hook"
	if _, err := Install(configDir, command); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat(config.json) error = %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("config.json mode = %v, want symlink preserved", info.Mode())
	}
	targetContents, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile(target config.json) error = %v", err)
	}
	if !bytes.Contains(targetContents, []byte(command)) {
		t.Fatalf("target config.json = %q, want installed command", targetContents)
	}
	targetInfo, err := os.Stat(target)
	if err != nil {
		t.Fatalf("Stat(target config.json) error = %v", err)
	}
	if got := targetInfo.Mode().Perm(); runtime.GOOS != "windows" && got != 0o640 {
		t.Fatalf("target config.json mode = %o, want 640", got)
	}
}

func TestInstallPreservesExactJSONNumbers(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	const preciseNumber = "9007199254740993"
	original := []byte(`{"external":{"id":` + preciseNumber + `}}`)
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}
	if _, err := Install(configDir, "waypost devin-hook"); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(config.json) error = %v", err)
	}
	if !bytes.Contains(contents, []byte(preciseNumber)) {
		t.Fatalf("config.json = %q, want exact number %s", contents, preciseNumber)
	}
}

func TestInstallRejectsInvalidJSONWithoutChangingFile(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	original := []byte("{not-json\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}
	if _, err := Install(configDir, "waypost devin-hook"); err == nil {
		t.Fatal("Install() error = nil, want invalid JSON error")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(config.json) error = %v", err)
	}
	if !bytes.Equal(contents, original) {
		t.Fatalf("config.json = %q, want original invalid contents %q", contents, original)
	}
}

func TestInstallCreatesConfigWhenMissing(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	command := "'/opt/waypost' devin-hook"
	result, err := Install(configDir, command)
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if !result.Changed || result.Path != filepath.Join(configDir, "config.json") {
		t.Fatalf("Install() = %+v, want changed config.json", result)
	}
	contents, err := os.ReadFile(result.Path)
	if err != nil {
		t.Fatalf("ReadFile(config.json) error = %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("Unmarshal(config.json) error = %v", err)
	}
	hooks := document["hooks"].(map[string]any)
	for _, event := range []string{"PostCompaction", "SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "SessionEnd"} {
		if got := len(hooks[event].([]any)); got != 1 {
			t.Fatalf("%s hooks = %d, want one Waypost group", event, got)
		}
	}
	if _, err := Doctor(configDir, command); err != nil {
		t.Fatalf("Doctor() after install error = %v", err)
	}
}

func TestInstallAcceptsJSONCConfigAndWarnsAboutRewrite(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	contents := `{
  // user note
  "theme": "dark",
  "hooks": {
    "UserPromptSubmit": [
      {"hooks": [{"type": "command", "command": "other-tool hook"}]},
    ],
  },
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}
	result, err := Install(configDir, "waypost devin-hook")
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Install() Changed = false, want true")
	}
	if len(result.Warnings) == 0 {
		t.Fatal("Install() Warnings empty, want JSONC rewrite warning")
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(config.json) error = %v", err)
	}
	if !json.Valid(updated) {
		t.Fatalf("rewritten config is not strict JSON: %s", updated)
	}
	var document map[string]any
	if err := json.Unmarshal(updated, &document); err != nil {
		t.Fatalf("Unmarshal(config.json) error = %v", err)
	}
	if document["theme"] != "dark" {
		t.Fatalf("theme = %#v, want preserved", document["theme"])
	}
	hooks := document["hooks"].(map[string]any)
	if got := len(hooks["UserPromptSubmit"].([]any)); got != 2 {
		t.Fatalf("UserPromptSubmit groups = %d, want user group plus Waypost group", got)
	}
}

func TestDoctorRejectsMatcherThatNeverFiresForNonToolEvent(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	contents := `{
  "hooks": {
    "UserPromptSubmit": [
      {
        "matcher": "^exec$",
        "hooks": [{"type": "command", "command": "waypost devin-hook", "timeout": 5}]
      }
    ]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}
	if _, err := Doctor(configDir, "waypost devin-hook"); err == nil {
		t.Fatal("Doctor() error = nil, want tool matcher rejection for prompt event")
	}
}

func TestDoctorRequiresAllManagedHooks(t *testing.T) {
	t.Parallel()

	for _, missing := range []string{"PostCompaction", "SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "SessionEnd"} {
		t.Run(missing, func(t *testing.T) {
			t.Parallel()

			groups := map[string]string{
				"PostCompaction":   `{"hooks":[{"type":"command","command":"waypost devin-hook","timeout":5}]}`,
				"SessionStart":     `{"hooks":[{"type":"command","command":"waypost devin-hook","timeout":5}]}`,
				"UserPromptSubmit": `{"hooks":[{"type":"command","command":"waypost devin-hook","timeout":5}]}`,
				"PreToolUse":       `{"matcher":"^exec$","hooks":[{"type":"command","command":"waypost devin-hook","timeout":5}]}`,
				"PostToolUse":      `{"matcher":"^(exec|mcp__waypost__waypost_recv)$","hooks":[{"type":"command","command":"waypost devin-hook","timeout":5}]}`,
				"SessionEnd":       `{"hooks":[{"type":"command","command":"waypost devin-hook","timeout":3}]}`,
			}
			var builder strings.Builder
			builder.WriteString(`{"hooks":{`)
			first := true
			for event, group := range groups {
				if event == missing {
					continue
				}
				if !first {
					builder.WriteString(",")
				}
				first = false
				builder.WriteString(`"` + event + `":[` + group + `]`)
			}
			builder.WriteString("}}")

			configDir := t.TempDir()
			path := filepath.Join(configDir, "config.json")
			if err := os.WriteFile(path, []byte(builder.String()), 0o600); err != nil {
				t.Fatalf("WriteFile(config.json) error = %v", err)
			}
			_, err := Doctor(configDir, "waypost devin-hook")
			if err == nil || !strings.Contains(err.Error(), missing) {
				t.Fatalf("Doctor() error = %v, want missing %s hook error", err, missing)
			}
		})
	}
}

func TestDoctorRejectsMalformedHookSource(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	contents := `{
  "hooks": {
    "PostCompaction": [{
      "hooks": [{"type": "command", "command": "waypost devin-hook", "timeout": 5}]
    }],
    "PreToolUse": ["malformed"]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}
	_, err := Doctor(configDir, "waypost devin-hook")
	if err == nil || !strings.Contains(err.Error(), "PreToolUse") {
		t.Fatalf("Doctor() error = %v, want malformed PreToolUse rejection", err)
	}
}

func TestDoctorRejectsManagedHandlersWithoutShortTimeout(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	contents := `{
  "hooks": {
    "PostCompaction": [{
      "hooks": [{"type": "command", "command": "waypost devin-hook"}]
    }],
    "UserPromptSubmit": [{
      "hooks": [{"type": "command", "command": "waypost devin-hook"}]
    }]
  }
}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}
	if _, err := Doctor(configDir, "waypost devin-hook"); err == nil {
		t.Fatal("Doctor() error = nil, want missing timeout rejection")
	}
}

func TestParseWaypostMCPStatus(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		payload       string
		available     bool
		disabledTools []string
		wantError     bool
	}{
		{name: "listed", payload: "Server: waypost\n    Command: waypost mcp\n", available: true},
		{name: "disabled", payload: "Server: waypost\n    Command: waypost mcp\n    Disabled: true\n"},
		{name: "status disabled", payload: "Server: waypost\nStatus: Disabled — disabled by user\n    Command: /bin/echo hi\n"},
		{name: "disabled false", payload: "Server: waypost\n    Disabled: false\n    Command: waypost mcp\n", available: true},
		{name: "enabled false", payload: "Server: waypost\n    Enabled: false\n"},
		{name: "disabled tools", payload: "Server: waypost\nDisabled tools: waypost_recv\n    Command: /bin/echo hi\n", available: true, disabledTools: []string{"waypost_recv"}},
		{name: "disabled tools multiple", payload: "Server: waypost\nDisabled tools: waypost_recv, waypost_status, other_thing\n", available: true, disabledTools: []string{"other_thing", "waypost_recv", "waypost_status"}},
		{name: "disabled tools namespaced", payload: "Server: waypost\nDisabled tools: mcp__waypost__waypost_recv, mcp__other__unrelated\n", available: true, disabledTools: []string{"mcp__other__unrelated", "waypost_recv"}},
		{name: "waypost mention only", payload: "Server: other\n    Command: waypost mcp\n", wantError: true},
		{name: "wrong server", payload: "Server: other\n    Command: other serve\n", wantError: true},
		{name: "empty", payload: "", wantError: true},
		{name: "invalid", payload: "{", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			status, err := parseWaypostMCPStatus([]byte(tc.payload))
			if (err != nil) != tc.wantError {
				t.Fatalf("parseWaypostMCPStatus() error = %v, wantError %v", err, tc.wantError)
			}
			if status.available != tc.available {
				t.Fatalf("parseWaypostMCPStatus() available = %v, want %v", status.available, tc.available)
			}
			if got := status.exported().DisabledTools; !reflect.DeepEqual(got, tc.disabledTools) && !(len(got) == 0 && len(tc.disabledTools) == 0) {
				t.Fatalf("parseWaypostMCPStatus() disabledTools = %v, want %v", got, tc.disabledTools)
			}
		})
	}
}

func TestParseWaypostMCPStatusCanonicalizesNamespacedDisabledTools(t *testing.T) {
	t.Parallel()

	status, err := parseWaypostMCPStatus([]byte("Server: waypost\nDisabled tools: mcp__waypost__waypost_recv, mcp__waypost__waypost_send\n"))
	if err != nil {
		t.Fatalf("parseWaypostMCPStatus() error = %v", err)
	}
	if status.toolUsable("waypost_recv") || status.toolUsable("waypost_send") {
		t.Fatalf("toolUsable() = %v/%v, want namespaced disabled tools honored", status.toolUsable("waypost_recv"), status.toolUsable("waypost_send"))
	}
	if !status.toolUsable("waypost_status") {
		t.Fatal("toolUsable(waypost_status) = false, want usable")
	}
}

func TestCurrentDirectoryWaypostMCPAvailableTreatsMissingServerAsUnavailable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}

	binDir := t.TempDir()
	devinPath := filepath.Join(binDir, "devin")
	probe := `#!/bin/sh
printf '%s\n' "Error: server 'waypost' not found" >&2
exit 1
`
	if err := os.WriteFile(devinPath, []byte(probe), 0o700); err != nil {
		t.Fatalf("WriteFile(devin probe) error = %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	status, err := probeWaypostMCPWithTimeout(context.Background(), 30*time.Second)
	if err != nil || status.available {
		t.Fatalf("probeWaypostMCPWithTimeout() = %v, %v; want unavailable", status, err)
	}
}

func TestCurrentDirectoryWaypostMCPAvailableReportsCommandStderr(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a POSIX shell script")
	}

	binDir := t.TempDir()
	devinPath := filepath.Join(binDir, "devin")
	probe := `#!/bin/sh
printf '%s\n' 'invalid Devin MCP configuration' >&2
exit 9
`
	if err := os.WriteFile(devinPath, []byte(probe), 0o700); err != nil {
		t.Fatalf("WriteFile(devin probe) error = %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	available, err := probeWaypostMCPWithTimeout(context.Background(), 30*time.Second)
	if err == nil || !strings.Contains(err.Error(), "invalid Devin MCP configuration") {
		t.Fatalf("probeWaypostMCPWithTimeout() = %v, %v; want stderr detail", available, err)
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

func TestCurrentCommandUsesStableLauncherPath(t *testing.T) {
	stable := filepath.Join(t.TempDir(), "waypost.exe")
	t.Setenv(launchpath.StableExecutableEnv, stable)

	command, err := CurrentCommand()
	if err != nil {
		t.Fatalf("CurrentCommand() error = %v", err)
	}
	want := hookcore.QuoteCommandPath(stable) + " devin-hook"
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

func groupHasCommand(group map[string]any, command string) bool {
	handlers, ok := group["hooks"].([]any)
	if !ok {
		return false
	}
	for _, item := range handlers {
		if handlerHasCommand(item, command) {
			return true
		}
	}
	return false
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
		HookEventName: "PostCompaction",
		SessionID:     sessionID,
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
		SessionID:     "pretool-session",
		ToolName:      execToolName,
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

func TestRunEventsWithoutSessionIDDegenerateGracefully(t *testing.T) {
	t.Parallel()

	mcpResponse := json.RawMessage(`{"success":true,"output":"{\"structuredContent\":{\"status\":\"received\"}}"}`)
	execResponse := json.RawMessage(`{"success":true,"output":"{\"status\":\"received\"}"}`)

	for _, tc := range []struct {
		name  string
		input hookInput
	}{
		{name: "MCP receive", input: hookInput{HookEventName: "PostToolUse", ToolName: receiveMCPToolName, ToolResponse: mcpResponse}},
		{name: "exec receive", input: hookInput{HookEventName: "PostToolUse", ToolName: execToolName, ToolInput: json.RawMessage(`{"command":"waypost recv --json"}`), ToolResponse: execResponse}},
		{name: "SessionEnd", input: hookInput{HookEventName: "SessionEnd"}},
		{name: "PostCompaction", input: hookInput{HookEventName: "PostCompaction"}},
		{name: "SessionStart compact", input: hookInput{HookEventName: "SessionStart", Source: "compact"}},
		{name: "ordinary prompt", input: hookInput{HookEventName: "UserPromptSubmit", Prompt: "hello"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if output, emitted := runHook(t, newMemoryNudgeStateStore(), nil, tc.input); emitted {
				t.Fatalf("%s output = %+v, want empty", tc.input.HookEventName, output)
			}
		})
	}
}

func TestRunUserPromptNudgeWithoutSessionIDStillEmitsContext(t *testing.T) {
	t.Parallel()

	output, emitted := runHook(t, newMemoryNudgeStateStore(), func(context.Context) (waypostMCPStatus, error) {
		return waypostMCPStatus{available: true}, nil
	}, hookInput{HookEventName: "UserPromptSubmit", Prompt: defaultNudgeMessage})
	if !emitted || output.HookSpecificOutput.AdditionalContext != MCPNudgeContext {
		t.Fatalf("UserPromptSubmit output = %+v, emitted %v; want MCP nudge context", output, emitted)
	}
}

func TestInstallPreservesCommandsMerelyMentioningDevinHook(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	original := `{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {"type": "command", "command": "echo devin-hook"},
          {"type": "command", "command": "my-wrapper devin-hook --audit"},
          {"type": "command", "command": "legacy devin-hook"}
        ]
      }
    ],
    "PreToolUse": [
      {
        "matcher": "^exec$",
        "hooks": [{"type": "command", "command": "run devin-hook-check"}]
      }
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}
	command := "'/opt/waypost' devin-hook"

	if _, err := Install(configDir, command); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(after install) error = %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("Unmarshal(after install) error = %v", err)
	}
	hooks := document["hooks"].(map[string]any)
	promptGroups := hooks["UserPromptSubmit"].([]any)
	if got := len(promptGroups); got != 2 {
		t.Fatalf("UserPromptSubmit groups = %d, want existing plus Waypost", got)
	}
	existing := promptGroups[0].(map[string]any)
	for _, want := range []string{"echo devin-hook", "my-wrapper devin-hook --audit", "legacy devin-hook"} {
		if !groupHasCommand(existing, want) {
			t.Fatalf("UserPromptSubmit group = %#v, want preserved %q", existing, want)
		}
	}
	preToolGroups := hooks["PreToolUse"].([]any)
	if got := len(preToolGroups); got != 2 {
		t.Fatalf("PreToolUse groups = %d, want existing plus Waypost", got)
	}
	if !groupHasCommand(preToolGroups[0].(map[string]any), "run devin-hook-check") {
		t.Fatalf("PreToolUse group = %#v, want preserved run devin-hook-check", preToolGroups[0])
	}
}

func TestInstallAdoptsHooksFromOlderWaypostPath(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	path := filepath.Join(configDir, "config.json")
	original := `{
  "hooks": {
    "SessionEnd": [
      {
        "hooks": [
          {"type": "command", "command": "/old/bin/waypost devin-hook", "timeout": 30},
          {"type": "command", "command": "\"/old dir/waypost.exe\" devin-hook"}
        ]
      }
    ]
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(config.json) error = %v", err)
	}
	command := "'/opt/waypost' devin-hook"

	if _, err := Install(configDir, command); err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(after install) error = %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("Unmarshal(after install) error = %v", err)
	}
	groups := document["hooks"].(map[string]any)["SessionEnd"].([]any)
	if got := len(groups); got != 1 {
		t.Fatalf("SessionEnd groups = %d, want single managed group", got)
	}
	group := groups[0].(map[string]any)
	handlers := group["hooks"].([]any)
	if got := len(handlers); got != 1 {
		t.Fatalf("SessionEnd handlers = %d, want old Waypost hooks replaced by one", got)
	}
	if got := handlers[0].(map[string]any)["command"]; got != command {
		t.Fatalf("SessionEnd command = %v, want %q", got, command)
	}
}

func TestRunUserPromptNudgeUsesCLIWhenRecvToolDisabled(t *testing.T) {
	t.Parallel()

	status := waypostMCPStatus{available: true, disabledTools: map[string]bool{"waypost_recv": true}}
	output, emitted := runHook(t, newMemoryNudgeStateStore(), func(context.Context) (waypostMCPStatus, error) {
		return status, nil
	}, hookInput{HookEventName: "UserPromptSubmit", SessionID: "session-disabled-recv", Prompt: defaultNudgeMessage})
	if !emitted || output.HookSpecificOutput.AdditionalContext != CLINudgeContext {
		t.Fatalf("UserPromptSubmit output = %+v, emitted %v; want CLI nudge context", output, emitted)
	}
}

func TestRunPreToolUseHonorsDisabledWaypostTools(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		command       string
		disabledTools map[string]bool
		wantDenied    bool
	}{
		{name: "recv blocked when usable", command: "waypost recv", disabledTools: map[string]bool{"waypost_status": true}, wantDenied: true},
		{name: "recv allowed when disabled", command: "waypost recv", disabledTools: map[string]bool{"waypost_recv": true}},
		{name: "send denied while recv disabled", command: "waypost send --to agent/x hi", disabledTools: map[string]bool{"waypost_recv": true}, wantDenied: true},
		{name: "status allowed when disabled", command: "waypost status", disabledTools: map[string]bool{"waypost_status": true}},
		{name: "status denied when usable", command: "waypost status", disabledTools: map[string]bool{"waypost_recv": true}, wantDenied: true},
		{name: "ack allowed when disabled", command: "waypost ack --delivery dlv_1 --lease-token lease_1", disabledTools: map[string]bool{"waypost_ack": true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			status := waypostMCPStatus{available: true, disabledTools: tc.disabledTools}
			output, emitted := runPreToolHook(t, tc.command, func(context.Context) (waypostMCPStatus, error) {
				return status, nil
			})
			if tc.wantDenied {
				if !emitted || output.Decision != "block" {
					t.Fatalf("PreToolUse(%q) output = %+v, emitted %v; want block", tc.command, output, emitted)
				}
				return
			}
			if emitted {
				t.Fatalf("PreToolUse(%q) output = %+v; want no output so the CLI fallback stays usable", tc.command, output)
			}
		})
	}
}
