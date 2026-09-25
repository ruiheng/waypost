//go:build windows

package hookcore

import "os"

func mcpGetInvocation(binary string, args []string) (string, []string) {
	commandShell := os.Getenv("ComSpec")
	if commandShell == "" {
		commandShell = "cmd.exe"
	}
	commandArgs := make([]string, 0, len(args)+4)
	commandArgs = append(commandArgs, "/d", "/s", "/c", binary)
	commandArgs = append(commandArgs, args...)
	return commandShell, commandArgs
}
