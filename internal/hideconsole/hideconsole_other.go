//go:build !windows

package hideconsole

import "os/exec"

// Hide is a no-op off Windows; consoles there are a Windows concept.
func Hide(cmd *exec.Cmd) {
}
