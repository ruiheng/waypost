//go:build windows

package mcpinstall

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

func commandInvocation(name string, args ...string) (string, []string) {
	extension := strings.ToLower(filepath.Ext(name))
	if extension != ".cmd" && extension != ".bat" {
		return name, args
	}
	commandShell := strings.TrimSpace(os.Getenv("ComSpec"))
	if commandShell == "" {
		commandShell = "cmd.exe"
	}
	commandArgs := make([]string, 0, len(args)+4)
	commandArgs = append(commandArgs, "/d", "/s", "/c", name)
	commandArgs = append(commandArgs, args...)
	return commandShell, commandArgs
}

func configureCommand(command *exec.Cmd, originalName, commandName string, commandArgs []string) {
	extension := strings.ToLower(filepath.Ext(originalName))
	if extension != ".cmd" && extension != ".bat" {
		return
	}
	attr := command.SysProcAttr
	if attr == nil {
		attr = &syscall.SysProcAttr{}
	}
	attr.CmdLine = windowsCommandLine(commandName, commandArgs)
	command.SysProcAttr = attr
}

func windowsCommandLine(commandName string, commandArgs []string) string {
	if len(commandArgs) < 4 {
		return ""
	}
	command := make([]string, 0, len(commandArgs)-3)
	for _, arg := range commandArgs[3:] {
		command = append(command, quoteCmdArgument(arg))
	}
	return quoteCreateProcessArgument(commandName) + " /d /s /c \"" + strings.Join(command, " ") + "\""
}

func quoteCreateProcessArgument(value string) string {
	if value == "" || strings.ContainsAny(value, " \t") {
		return `"` + value + `"`
	}
	return value
}

// cmd.exe does not use the CommandLineToArgvW escaping performed by
// os/exec. Quoting every command argument inside the /c string keeps paths
// containing spaces as one argument to a batch shim.
func quoteCmdArgument(value string) string {
	// TODO: Validate this quoting on Windows with paths containing literal
	// %VAR% segments. cmd.exe may expand environment variables even inside
	// quotes; do not change the escaping strategy without a Windows test.
	return `"` + strings.ReplaceAll(value, `"`, `\"`) + `"`
}
