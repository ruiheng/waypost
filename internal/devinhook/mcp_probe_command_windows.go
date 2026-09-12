//go:build windows

package devinhook

import "os"

func mcpProbeInvocation(args ...string) (string, []string) {
	commandShell := os.Getenv("ComSpec")
	if commandShell == "" {
		commandShell = "cmd.exe"
	}
	commandArgs := make([]string, 0, len(args)+4)
	commandArgs = append(commandArgs, "/d", "/s", "/c", "devin")
	commandArgs = append(commandArgs, args...)
	return commandShell, commandArgs
}
