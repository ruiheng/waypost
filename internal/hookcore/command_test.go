package hookcore

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDirectWaypostCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		command string
		want    string
		ok      bool
	}{
		{"waypost recv", "recv", true},
		{"waypost recv --json", "recv", true},
		{"/usr/local/bin/waypost status", "status", true},
		{`'C:\Tools\waypost.exe' send`, "send", true},
		{"waypost --state-dir /tmp/x recv", "recv", true},
		{"waypost --state-dir=/tmp/x recv", "recv", true},
		{`& "C:\Program Files\Waypost\waypost.exe" recv`, "recv", true},
		{"waypost", "", false},
		{"waypost --state-dir", "", false},
		{"echo waypost recv", "", false},
		{"my-wrapper waypost recv", "", false},
		{"env WAYPOST=1 waypost recv", "", false},
		{"", "", false},
	}
	for _, test := range tests {
		got, ok := DirectWaypostCommand(test.command)
		if ok != test.ok || got != test.want {
			t.Errorf("DirectWaypostCommand(%q) = %q, %v; want %q, %v", test.command, got, ok, test.want, test.ok)
		}
	}
}

func TestIsWaypostExecutable(t *testing.T) {
	t.Parallel()

	for _, exe := range []string{"waypost", "waypost.exe", "/usr/bin/waypost", `C:\bin\waypost.exe`, "WAYPOST"} {
		if !IsWaypostExecutable(exe) {
			t.Errorf("IsWaypostExecutable(%q) = false, want true", exe)
		}
	}
	for _, exe := range []string{"waypost-devin", "echo", "wayposter", "/bin/waypost-tool"} {
		if IsWaypostExecutable(exe) {
			t.Errorf("IsWaypostExecutable(%q) = true, want false", exe)
		}
	}
}

func TestIsManagedHookCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		command string
		want    bool
	}{
		{"waypost devin-hook", true},
		{"/old/path/waypost devin-hook", true},
		{`"C:\old\waypost.exe" devin-hook`, true},
		{"& waypost devin-hook", true},
		{"echo devin-hook", false},
		{"my-wrapper devin-hook --audit", false},
		{"waypost devin-hook --extra", false},
		{"run devin-hook-check", false},
		{"waypost codex-hook", false},
	}
	for _, test := range tests {
		if got := IsManagedHookCommand(test.command, "devin-hook"); got != test.want {
			t.Errorf("IsManagedHookCommand(%q) = %v, want %v", test.command, got, test.want)
		}
	}
}

func TestWaypostMCPToolAndDenialReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		command string
		tool    string
		guarded bool
	}{
		{"waypost status", "waypost_status", true},
		{"waypost recv", "waypost_recv", true},
		{"waypost receive", "waypost_recv", true},
		{"waypost send", "waypost_send", true},
		{"waypost ack", "waypost_ack", true},
		{"waypost mcp", "", true},
		{"waypost doctor", "", false},
		{"echo hi", "", false},
	}
	for _, test := range tests {
		tool, guarded := WaypostMCPTool(test.command)
		if guarded != test.guarded || tool != test.tool {
			t.Errorf("WaypostMCPTool(%q) = %q, %v; want %q, %v", test.command, tool, guarded, test.tool, test.guarded)
		}
	}

	if got := WaypostCommandDenialReason("", "server reason"); got != "server reason" {
		t.Errorf("WaypostCommandDenialReason(empty) = %q, want server reason", got)
	}
	if got := WaypostCommandDenialReason("waypost_status", "x"); got != StatusDenialReason {
		t.Errorf("WaypostCommandDenialReason(status) = %q, want %q", got, StatusDenialReason)
	}
	if got := WaypostCommandDenialReason("waypost_recv", "x"); !strings.Contains(got, "waypost_recv") {
		t.Errorf("WaypostCommandDenialReason(recv) = %q, want tool name", got)
	}
}

func TestLooksLikeWaypostWaitCommand(t *testing.T) {
	t.Parallel()

	for _, command := range []string{"waypost wait", "waypost wait --timeout 30"} {
		if !LooksLikeWaypostWaitCommand(command) {
			t.Errorf("LooksLikeWaypostWaitCommand(%q) = false, want true", command)
		}
	}
	for _, command := range []string{"waypost recv", "echo waypost wait", "waypost awaiting"} {
		if LooksLikeWaypostWaitCommand(command) {
			t.Errorf("LooksLikeWaypostWaitCommand(%q) = true, want false", command)
		}
	}
}

func TestLooksLikeWaypostNudge(t *testing.T) {
	t.Parallel()

	if !LooksLikeWaypostNudge("  notice: there might be new delivery in waypost.  ") {
		t.Error("LooksLikeWaypostNudge(canonical) = false, want true")
	}
	if LooksLikeWaypostNudge("tell me about waypost") {
		t.Error("LooksLikeWaypostNudge(unrelated) = true, want false")
	}
}

func TestCommandToolInput(t *testing.T) {
	t.Parallel()

	command, err := CommandToolInput(json.RawMessage(`{"command":"waypost recv"}`), "Test shell")
	if err != nil || command != "waypost recv" {
		t.Fatalf("CommandToolInput() = %q, %v; want waypost recv", command, err)
	}
	if command, err := CommandToolInput(nil, "Test shell"); err != nil || command != "" {
		t.Fatalf("CommandToolInput(empty) = %q, %v; want empty", command, err)
	}
	if _, err := CommandToolInput(json.RawMessage(`{`), "Test shell"); err == nil || !strings.Contains(err.Error(), "Test shell") {
		t.Fatalf("CommandToolInput(invalid) error = %v, want labeled parse error", err)
	}
}
