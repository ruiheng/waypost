package rootcmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ruiheng/waypost/internal/mcpinstall"
	"github.com/ruiheng/waypost/internal/mcpserver"
	"github.com/ruiheng/waypost/internal/version"
	"github.com/ruiheng/waypost/internal/waypost"
	"github.com/ruiheng/waypost/internal/webui"
)

func TestRunRootHelpIncludesMCP(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})

	err := app.Run(context.Background(), []string{"--help"})
	if !errors.Is(err, waypost.ErrHelpRequested) {
		t.Fatalf("Run(--help) error = %v, want ErrHelpRequested", err)
	}
	if !strings.Contains(stdout.String(), "mcp") {
		t.Fatalf("root help = %q, want mcp command", stdout.String())
	}
	if strings.Contains(stdout.String(), "  web") {
		t.Fatalf("root help = %q, want no top-level web command", stdout.String())
	}
	if !strings.Contains(stdout.String(), "--version") {
		t.Fatalf("root help = %q, want version option", stdout.String())
	}
}

func TestRunRootHelpIncludesCodexHookCommands(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})

	err := app.Run(context.Background(), []string{"--help"})
	if !errors.Is(err, waypost.ErrHelpRequested) {
		t.Fatalf("Run(--help) error = %v, want ErrHelpRequested", err)
	}
	for _, command := range []string{"codex-hook", "install", "doctor"} {
		if !strings.Contains(stdout.String(), command) {
			t.Fatalf("root help = %q, want %q command", stdout.String(), command)
		}
	}
}

func TestRunCodexHookEmitsHookSpecificOutput(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})

	if err := app.Run(context.Background(), []string{"codex-hook"}); err != nil {
		t.Fatalf("Run(codex-hook) error = %v", err)
	}
	if !strings.Contains(stdout.String(), `"hookEventName":"SessionStart"`) {
		t.Fatalf("codex-hook output = %q, want SessionStart hook output", stdout.String())
	}
	if !strings.Contains(stdout.String(), "already handled before compaction") ||
		!strings.Contains(stdout.String(), "Do not repeat the receive merely because of compaction") {
		t.Fatalf("codex-hook output = %q, want compact guard", stdout.String())
	}
}

func TestRunInstallAndDoctorCodexHook(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)

	var installOutput bytes.Buffer
	installApp := New(strings.NewReader(""), &installOutput, &bytes.Buffer{})
	if err := installApp.Run(context.Background(), []string{"install", "codex-hook"}); err != nil {
		t.Fatalf("Run(install codex-hook) error = %v", err)
	}
	if !strings.Contains(installOutput.String(), "Codex hooks installed") {
		t.Fatalf("install output = %q, want installed status", installOutput.String())
	}
	if !strings.Contains(installOutput.String(), "/hooks") {
		t.Fatalf("install output = %q, want Codex trust review instruction", installOutput.String())
	}
	if _, err := os.Stat(filepath.Join(home, "hooks.json")); err != nil {
		t.Fatalf("installed hooks.json = %v", err)
	}

	var secondInstallOutput bytes.Buffer
	secondInstallApp := New(strings.NewReader(""), &secondInstallOutput, &bytes.Buffer{})
	if err := secondInstallApp.Run(context.Background(), []string{"install", "codex-hook"}); err != nil {
		t.Fatalf("Run(second install codex-hook) error = %v", err)
	}
	if !strings.Contains(secondInstallOutput.String(), "already installed") {
		t.Fatalf("second install output = %q, want already installed", secondInstallOutput.String())
	}

	var doctorOutput bytes.Buffer
	doctorApp := New(strings.NewReader(""), &doctorOutput, &bytes.Buffer{})
	doctorApp.currentDirectoryWaypostMCPAvailable = func(context.Context) (bool, error) {
		return true, nil
	}
	if err := doctorApp.Run(context.Background(), []string{"doctor", "codex-hook"}); err != nil {
		t.Fatalf("Run(doctor codex-hook) error = %v", err)
	}
	if !strings.Contains(doctorOutput.String(), "Codex compact hook: configured") {
		t.Fatalf("doctor output = %q, want configured status", doctorOutput.String())
	}
	if !strings.Contains(doctorOutput.String(), "Codex nudge hook: configured") || !strings.Contains(doctorOutput.String(), "Waypost MCP: available to a new Codex process in the current directory") {
		t.Fatalf("doctor output = %q, want nudge hook and MCP status", doctorOutput.String())
	}
	if !strings.Contains(doctorOutput.String(), "Codex wait polling guard: configured") {
		t.Fatalf("doctor output = %q, want wait polling guard status", doctorOutput.String())
	}
	if !strings.Contains(doctorOutput.String(), "Codex receive completion tracker: configured") {
		t.Fatalf("doctor output = %q, want receive completion tracker status", doctorOutput.String())
	}
	if !strings.Contains(doctorOutput.String(), "Codex nudge state cleanup: configured") {
		t.Fatalf("doctor output = %q, want nudge state cleanup status", doctorOutput.String())
	}
	if !strings.Contains(doctorOutput.String(), "trust: not checked; verify with `/hooks`") {
		t.Fatalf("doctor output = %q, want explicit unverified trust status", doctorOutput.String())
	}
}

func TestRunInstallMCPServer(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	called := false
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})
	app.installMCPServer = func(ctx context.Context) (mcpinstall.Result, error) {
		called = true
		if ctx == nil {
			t.Fatal("MCP install context = nil")
		}
		return mcpinstall.Result{
			Path:    "/tmp/codex/config.toml",
			Command: "/opt/waypost mcp",
			Changed: true,
		}, nil
	}

	if err := app.Run(context.Background(), []string{"install", "mcp-server"}); err != nil {
		t.Fatalf("Run(install mcp-server) error = %v", err)
	}
	if !called {
		t.Fatal("MCP installer was not called")
	}
	if got := stdout.String(); !strings.Contains(got, "Waypost MCP server installed") || !strings.Contains(got, "/tmp/codex/config.toml") || !strings.Contains(got, "/opt/waypost mcp") {
		t.Fatalf("install output = %q, want MCP installation summary", got)
	}
}

func TestRunInstallMCPServerPrintsPartialSuccessOnError(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &stderr)
	app.installMCPServer = func(context.Context) (mcpinstall.Result, error) {
		return mcpinstall.Result{
			Configured: []mcpinstall.ConfiguredAgent{
				{Name: "Codex", Path: "/tmp/codex/config.toml"},
				{Name: "Claude Code", Path: "/tmp/home/.claude.json"},
			},
		}, errors.New("configure Devin MCP server: permission denied")
	}

	err := app.Run(context.Background(), []string{"install", "mcp-server"})
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("Run(install mcp-server) error = %v, want configure failure", err)
	}
	got := stderr.String()
	if !strings.Contains(got, "partially installed") || !strings.Contains(got, "Codex: /tmp/codex/config.toml") || !strings.Contains(got, "Claude Code: /tmp/home/.claude.json") || !strings.Contains(got, "Re-run") {
		t.Fatalf("stderr = %q, want partial-success summary with recovery hint", got)
	}
}

func TestRunInstallMCPServerHelp(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})
	err := app.Run(context.Background(), []string{"install", "mcp-server", "--help"})
	if !errors.Is(err, waypost.ErrHelpRequested) {
		t.Fatalf("Run(install mcp-server --help) error = %v, want ErrHelpRequested", err)
	}
	for _, expected := range []string{"waypost install mcp-server", "Codex's global config", "660-second tool timeout"} {
		if !strings.Contains(stdout.String(), expected) {
			t.Fatalf("MCP install help = %q, want %q", stdout.String(), expected)
		}
	}
}

func TestRunDoctorCodexHookReportsUnavailableCurrentDirectoryMCPWithoutFailing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)

	installApp := New(strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	if err := installApp.Run(context.Background(), []string{"install", "codex-hook"}); err != nil {
		t.Fatalf("Run(install codex-hook) error = %v", err)
	}

	var doctorOutput bytes.Buffer
	doctorApp := New(strings.NewReader(""), &doctorOutput, &bytes.Buffer{})
	doctorApp.currentDirectoryWaypostMCPAvailable = func(context.Context) (bool, error) {
		return false, nil
	}
	if err := doctorApp.Run(context.Background(), []string{"doctor", "codex-hook"}); err != nil {
		t.Fatalf("Run(doctor codex-hook) error = %v", err)
	}
	if !strings.Contains(doctorOutput.String(), "not available to a new Codex process in the current directory") || !strings.Contains(doctorOutput.String(), "already-running session, profile, or `-c` override may differ") {
		t.Fatalf("doctor output = %q, want scoped current-directory MCP status", doctorOutput.String())
	}
}

func TestRunDoctorCodexHookReportsCurrentDirectoryMCPProbeErrorWithoutFailing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)

	installApp := New(strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	if err := installApp.Run(context.Background(), []string{"install", "codex-hook"}); err != nil {
		t.Fatalf("Run(install codex-hook) error = %v", err)
	}

	var doctorOutput bytes.Buffer
	doctorApp := New(strings.NewReader(""), &doctorOutput, &bytes.Buffer{})
	doctorApp.currentDirectoryWaypostMCPAvailable = func(context.Context) (bool, error) {
		return false, errors.New("codex unavailable")
	}
	if err := doctorApp.Run(context.Background(), []string{"doctor", "codex-hook"}); err != nil {
		t.Fatalf("Run(doctor codex-hook) error = %v", err)
	}
	if !strings.Contains(doctorOutput.String(), "availability to a new Codex process in the current directory is unknown: codex unavailable") {
		t.Fatalf("doctor output = %q, want nonfatal current-directory probe error", doctorOutput.String())
	}
}

func TestRunVersion(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})

	if err := app.Run(context.Background(), []string{"--version"}); err != nil {
		t.Fatalf("Run(--version) error = %v", err)
	}
	if got, want := stdout.String(), "waypost "+version.Version+"\n"; got != want {
		t.Fatalf("Run(--version) output = %q, want %q", got, want)
	}
}

func TestRunRootHelpIncludesForward(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})

	err := app.Run(context.Background(), []string{"--help"})
	if !errors.Is(err, waypost.ErrHelpRequested) {
		t.Fatalf("Run(--help) error = %v, want ErrHelpRequested", err)
	}
	if !strings.Contains(stdout.String(), "  forward             Forward a stored message or delivery") {
		t.Fatalf("root help = %q, want forward command", stdout.String())
	}
}

func TestRunWithoutCommandMentionsForward(t *testing.T) {
	t.Parallel()

	app := New(strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})

	err := app.Run(context.Background(), nil)
	if err == nil {
		t.Fatal("Run() error = nil, want missing command error")
	}
	if !strings.Contains(err.Error(), "forward") {
		t.Fatalf("Run() error = %q, want forward in command list", err.Error())
	}
}

func TestRunMCPHelp(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})

	err := app.Run(context.Background(), []string{"mcp", "--help"})
	if !errors.Is(err, waypost.ErrHelpRequested) {
		t.Fatalf("Run(mcp --help) error = %v, want ErrHelpRequested", err)
	}
	if !strings.Contains(stdout.String(), "waypost mcp") {
		t.Fatalf("mcp help = %q, want usage text", stdout.String())
	}
	if !strings.Contains(stdout.String(), "--session-host-config") {
		t.Fatalf("mcp help = %q, want session-host configuration flag", stdout.String())
	}
	if !strings.Contains(stdout.String(), "deprecated; accepted and ignored") {
		t.Fatalf("mcp help = %q, want deprecated no-op wording", stdout.String())
	}
	if !strings.Contains(stdout.String(), "--include-debug-tool") {
		t.Fatalf("mcp help = %q, want debug tool opt-in", stdout.String())
	}
}

func TestRunMigrateMovesLegacyState(t *testing.T) {
	source := filepath.Join(t.TempDir(), "legacy-state")
	destination := filepath.Join(t.TempDir(), "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}

	var stdout bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})
	if err := app.Run(context.Background(), []string{
		"--state-dir", destination,
		"migrate",
		"--from", source,
	}); err != nil {
		t.Fatalf("Run(migrate) error = %v", err)
	}
	if _, err := os.Lstat(destination); err != nil {
		t.Fatalf("migrated destination = %v, want directory", err)
	}
	if !strings.Contains(stdout.String(), "migrated legacy state") {
		t.Fatalf("migrate output = %q, want migration summary", stdout.String())
	}
}

func TestRunMCPInvokesRunner(t *testing.T) {
	t.Parallel()

	called := false
	app := &App{
		stdin:  strings.NewReader(""),
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
		runMCP: func(context.Context, mcpserver.Options) error {
			called = true
			return nil
		},
	}

	if err := app.Run(context.Background(), []string{"mcp"}); err != nil {
		t.Fatalf("Run(mcp) error = %v", err)
	}
	if !called {
		t.Fatal("Run(mcp) did not invoke MCP runner")
	}
}

func TestRunMCPForwardsStateDir(t *testing.T) {
	t.Parallel()

	var gotOptions mcpserver.Options
	app := &App{
		stdin:  strings.NewReader(""),
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
		runMCP: func(_ context.Context, options mcpserver.Options) error {
			gotOptions = options
			return nil
		},
	}

	stateDir := t.TempDir()
	if err := app.Run(context.Background(), []string{"--state-dir", stateDir, "mcp"}); err != nil {
		t.Fatalf("Run(--state-dir mcp) error = %v", err)
	}
	if gotOptions.StateDir != stateDir {
		t.Fatalf("mcp state dir = %q, want %q", gotOptions.StateDir, stateDir)
	}
}

func TestRunMCPForwardsDebugToolOptIn(t *testing.T) {
	t.Parallel()

	var gotOptions mcpserver.Options
	app := &App{
		stdin:  strings.NewReader(""),
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
		runMCP: func(_ context.Context, options mcpserver.Options) error {
			gotOptions = options
			return nil
		},
	}

	if err := app.Run(context.Background(), []string{"mcp", "--include-debug-tool"}); err != nil {
		t.Fatalf("Run(mcp --include-debug-tool) error = %v", err)
	}
	if !gotOptions.IncludeDebugTool {
		t.Fatalf("mcp options = %#v, want IncludeDebugTool", gotOptions)
	}
}

func TestRunMCPAcceptsDeprecatedSessionHostConfigWithoutReading(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "does-not-exist.json")
	started := false
	var gotOptions mcpserver.Options
	app := &App{
		stdin:  strings.NewReader(""),
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
		runMCP: func(_ context.Context, options mcpserver.Options) error {
			started = true
			gotOptions = options
			return nil
		},
	}
	if err := app.Run(context.Background(), []string{"mcp", "--session-host-config", configPath}); err != nil {
		t.Fatalf("Run(mcp --session-host-config) error = %v", err)
	}
	if !started {
		t.Fatal("MCP server did not start with deprecated session-host flag")
	}
	if gotOptions.StateDir != "" {
		t.Fatalf("deprecated flag altered MCP options: %#v", gotOptions)
	}
	if gotOptions.IncludeDebugTool {
		t.Fatalf("deprecated flag enabled debug tool: %#v", gotOptions)
	}
}

func TestRunGroupWebForwardsOptions(t *testing.T) {
	t.Parallel()

	var got webui.Options
	app := &App{
		stdin:  strings.NewReader(""),
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
		runWeb: func(_ context.Context, opts webui.Options) error {
			got = opts
			return nil
		},
	}

	stateDir := t.TempDir()
	if err := app.Run(context.Background(), []string{
		"--state-dir", stateDir,
		"group",
		"web",
		"--listen", "127.0.0.1:0",
		"--group", "group/review",
	}); err != nil {
		t.Fatalf("Run(group web) error = %v", err)
	}
	if got.StateDir != stateDir {
		t.Fatalf("web state dir = %q, want %q", got.StateDir, stateDir)
	}
	if got.Listen != "127.0.0.1:0" {
		t.Fatalf("web listen = %q, want 127.0.0.1:0", got.Listen)
	}
	if got.Group != "group/review" {
		t.Fatalf("web group = %q, want group/review", got.Group)
	}
	if got.Stdin == nil {
		t.Fatal("web stdin = nil, want forwarded stdin")
	}
	if got.Interactive {
		t.Fatal("web interactive = true for non-terminal test stdin, want false")
	}
}

func TestInteractiveTerminalRequiresTerminalOutput(t *testing.T) {
	t.Parallel()

	input, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open dev null error = %v", err)
	}
	defer input.Close()

	if isInteractiveTerminal(input, &bytes.Buffer{}) {
		t.Fatal("interactive terminal = true with non-terminal stdout, want false")
	}
}

func TestRunGroupWebHelp(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})

	err := app.Run(context.Background(), []string{"group", "web", "--help"})
	if !errors.Is(err, waypost.ErrHelpRequested) {
		t.Fatalf("Run(group web --help) error = %v, want ErrHelpRequested", err)
	}
	if !strings.Contains(stdout.String(), "waypost [--state-dir PATH] group web") {
		t.Fatalf("group web help = %q, want usage text", stdout.String())
	}
}

func TestRunDelegatesWaypostCommandsWithStateDir(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	var stdout bytes.Buffer
	app := New(strings.NewReader(""), &stdout, &bytes.Buffer{})

	err := app.Run(context.Background(), []string{
		"--state-dir", stateDir,
		"list",
		"--for", "workflow/reviewer/task-123",
		"--json",
	})
	if err != nil {
		t.Fatalf("Run(list) error = %v", err)
	}
	if stdout.String() != "{\n  \"items\": []\n}\n" {
		t.Fatalf("list output = %q, want empty paginated JSON object", stdout.String())
	}
}

func TestRunSendNotifyUsesConfiguredNotifier(t *testing.T) {
	t.Parallel()

	stateDir := t.TempDir()
	var stdout bytes.Buffer
	app := New(strings.NewReader("body\n"), &stdout, &bytes.Buffer{})
	app.notifyWaypostSend = func(_ context.Context, _ *waypost.Store, request waypost.SendNotificationRequest) waypost.SendNotificationOutcome {
		if request.Params.ToAddress != "agent-deck/coder" || request.Params.FromAddress != "agent-deck/supervisor" {
			t.Fatalf("notify request params = %+v", request.Params)
		}
		return waypost.SendNotificationOutcome{Status: "sent", Scheme: "agent-deck"}
	}

	err := app.Run(context.Background(), []string{
		"--state-dir", stateDir,
		"send",
		"--to", "agent-deck/coder",
		"--from", "agent-deck/supervisor",
		"--body-file", "-",
		"--notify",
		"--json",
	})
	if err != nil {
		t.Fatalf("Run(send --notify) error = %v", err)
	}
	if !strings.Contains(stdout.String(), `"notify_status": "sent"`) || !strings.Contains(stdout.String(), `"notify_scheme": "agent-deck"`) || !strings.Contains(stdout.String(), `"notify_error": null`) {
		t.Fatalf("send --notify output = %q", stdout.String())
	}
}
