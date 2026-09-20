package hookcore

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestWaypostCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		command string
		want    []string
	}{
		{"waypost recv", []string{"recv"}},
		{"waypost recv --json", []string{"recv"}},
		{"/usr/local/bin/waypost status", []string{"status"}},
		{`'C:\Tools\waypost.exe' send`, []string{"send"}},
		{`"waypost" send`, []string{"send"}},
		{`waypost se"nd"`, []string{"send"}},
		{"waypost --state-dir /tmp/x recv", []string{"recv"}},
		{"waypost --state-dir=/tmp/x recv", []string{"recv"}},
		{`& "C:\Program Files\Waypost\waypost.exe" recv`, []string{"recv"}},
		{"cd /tmp && waypost send --to a", []string{"send"}},
		{"echo ok; waypost recv", []string{"recv"}},
		{"waypost send | tee /tmp/log", []string{"send"}},
		{"FOO=1 waypost recv", []string{"recv"}},
		{"env WAYPOST=1 waypost recv", []string{"recv"}},
		{"sudo waypost send", []string{"send"}},
		{"sudo -u root waypost send", []string{"send"}},
		{"nice -n 5 waypost send", []string{"send"}},
		{"xargs waypost send", []string{"send"}},
		{"bash -c 'cd /tmp && waypost send'", []string{"send"}},
		{"sh -c \"waypost recv --json\"", []string{"recv"}},
		{"echo $(waypost recv)", []string{"recv"}},
		{"if true; then waypost send; fi", []string{"send"}},
		{"waypost status && waypost send", []string{"status", "send"}},
		{"waypost", nil},
		{"waypost --state-dir", nil},
		{"echo waypost recv", nil},
		{"my-wrapper waypost recv", nil},
		{`echo "waypost send"`, nil},
		{"cat <<EOF\nwaypost send\nEOF", nil},
		{"sudo less /var/log/waypost", nil},
		{"", nil},
		{"waypost send --body 'unclosed", nil},
	}
	for _, test := range tests {
		if got := WaypostCommands(test.command); !slices.Equal(got, test.want) {
			t.Errorf("WaypostCommands(%q) = %v; want %v", test.command, got, test.want)
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
