//go:build windows

package hideconsole

import (
	"os/exec"
	"syscall"
)

// createNoWindow is the Windows CREATE_NO_WINDOW process flag; defined here
// so this package needs no golang.org/x/sys dependency.
const createNoWindow = 0x08000000

// Hide keeps a console-subsystem child from allocating a new console window
// when its parent owns no console (MCP servers and detached helpers are
// spawned without one). Without it every such spawn flashes a console window
// on the user's desktop. stdin/stdout/stderr plumbing is unaffected.
func Hide(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	attr := cmd.SysProcAttr
	if attr == nil {
		attr = &syscall.SysProcAttr{}
	}
	attr.CreationFlags |= createNoWindow
	attr.HideWindow = true
	cmd.SysProcAttr = attr
}
