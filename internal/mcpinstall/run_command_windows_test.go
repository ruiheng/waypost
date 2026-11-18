//go:build windows

package mcpinstall

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ruiheng/waypost/internal/hideconsole"
)

func TestCommandInvocationUsesComSpecForBatchShim(t *testing.T) {
	t.Setenv("ComSpec", `C:\Windows\System32\cmd.exe`)

	name, args := commandInvocation(`C:\Program Files\node\codex.cmd`, "mcp", "get", "waypost")
	if name != `C:\Windows\System32\cmd.exe` {
		t.Fatalf("commandInvocation() name = %q, want ComSpec", name)
	}
	want := []string{"/d", "/s", "/c", `C:\Program Files\node\codex.cmd`, "mcp", "get", "waypost"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("commandInvocation() args = %#v, want %#v", args, want)
	}
	if got := windowsCommandLine(name, args); got != `C:\Windows\System32\cmd.exe /d /s /c ""C:\Program Files\node\codex.cmd" "mcp" "get" "waypost""` {
		t.Fatalf("windowsCommandLine() = %q, want nested quoted command", got)
	}
}

func TestCommandInvocationLeavesNativeExecutableUnchanged(t *testing.T) {
	name, args := commandInvocation(`C:\Program Files\node\codex.exe`, "mcp", "get")
	if name != `C:\Program Files\node\codex.exe` {
		t.Fatalf("commandInvocation() name = %q, want native executable", name)
	}
	want := []string{"mcp", "get"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("commandInvocation() args = %#v, want %#v", args, want)
	}
}

func TestRunCommandExecutesBatchShimWithSpaces(t *testing.T) {
	binDir := filepath.Join(t.TempDir(), "node installation with spaces")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	path := filepath.Join(binDir, "codex.cmd")
	contents := "@echo off\r\necho %~1 %~2 %~3 %~4\r\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(codex.cmd) error = %v", err)
	}

	output, err := runCommand(context.Background(), path, "mcp", "get", "waypost", "--json")
	if err != nil {
		t.Fatalf("runCommand() error = %v; stderr = %q", err, output.stderr)
	}
	if got := strings.TrimSpace(string(output.stdout)); got != "mcp get waypost --json" {
		t.Fatalf("runCommand() stdout = %q, want batch arguments", got)
	}
}

func TestConfigureCommandPreservesHideConsoleFlagsForBatchShims(t *testing.T) {
	const createNoWindow = 0x08000000

	for _, name := range []string{"tool.cmd", "tool.bat"} {
		t.Run(name, func(t *testing.T) {
			commandName, commandArgs := commandInvocation(name, "--probe")
			command := exec.CommandContext(context.Background(), commandName, commandArgs...)
			hideconsole.Hide(command)
			configureCommand(command, name, commandName, commandArgs)

			attr := command.SysProcAttr
			if attr == nil {
				t.Fatalf("%s: SysProcAttr is nil after configureCommand", name)
			}
			if attr.CreationFlags&createNoWindow == 0 {
				t.Fatalf("%s: CreationFlags = %#x, want CREATE_NO_WINDOW (%#x)", name, attr.CreationFlags, createNoWindow)
			}
			if !attr.HideWindow {
				t.Fatalf("%s: HideWindow = false, want true", name)
			}
			wantCmdLine := windowsCommandLine(commandName, commandArgs)
			if attr.CmdLine != wantCmdLine {
				t.Fatalf("%s: CmdLine = %q, want %q", name, attr.CmdLine, wantCmdLine)
			}
		})
	}

	t.Run("tool.exe", func(t *testing.T) {
		commandName, commandArgs := commandInvocation("tool.exe", "--probe")
		command := exec.CommandContext(context.Background(), commandName, commandArgs...)
		hideconsole.Hide(command)
		configureCommand(command, "tool.exe", commandName, commandArgs)

		attr := command.SysProcAttr
		if attr == nil {
			t.Fatal("tool.exe: SysProcAttr is nil")
		}
		if attr.CreationFlags&createNoWindow == 0 || !attr.HideWindow {
			t.Fatalf("tool.exe: hide flags lost: CreationFlags=%#x HideWindow=%v", attr.CreationFlags, attr.HideWindow)
		}
		if attr.CmdLine != "" {
			t.Fatalf("tool.exe: CmdLine = %q, want empty for native executable", attr.CmdLine)
		}
	})
}
