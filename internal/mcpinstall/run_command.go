package mcpinstall

import (
	"bytes"
	"context"
	"os/exec"

	"github.com/ruiheng/waypost/internal/hideconsole"
)

func runCommand(ctx context.Context, name string, args ...string) (commandOutput, error) {
	commandName, commandArgs := commandInvocation(name, args...)
	command := exec.CommandContext(ctx, commandName, commandArgs...)
	hideconsole.Hide(command)
	configureCommand(command, name, commandName, commandArgs)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, err := command.Output()
	return commandOutput{stdout: stdout, stderr: stderr.Bytes()}, err
}
