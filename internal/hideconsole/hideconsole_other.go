//go:build !windows

package hideconsole

import "os/exec"

// Hide is a no-op off Windows; consoles there are a Windows concept.
func Hide(cmd *exec.Cmd) {
}

// HideIfConsoleless is a no-op off Windows; consoles there are a Windows concept.
func HideIfConsoleless(cmd *exec.Cmd) {
}
