//go:build windows

package hideconsole

import (
	"os/exec"
	"syscall"
)

// createNoWindow is the Windows CREATE_NO_WINDOW process flag; defined here
// so this package needs no golang.org/x/sys dependency.
const createNoWindow = 0x08000000

var (
	kernel32DLL      = syscall.NewLazyDLL("kernel32.dll")
	getConsoleCPProc = kernel32DLL.NewProc("GetConsoleCP")
)

func processHasConsole() bool {
	cp, _, _ := getConsoleCPProc.Call()
	return cp != 0
}

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

// HideIfConsoleless hides cmd only when the current process owns no console
// (for example, when spawned with DETACHED_PROCESS). When the current process
// already owns a console — interactive, hidden, or windowless — the child
// inherits it without flashing a window and stays in the same console group
// for Ctrl+C/Ctrl+Break delivery.
func HideIfConsoleless(cmd *exec.Cmd) {
	hideIfConsoleless(cmd, processHasConsole)
}

func hideIfConsoleless(cmd *exec.Cmd, hasConsole func() bool) {
	if cmd == nil {
		return
	}
	if hasConsole != nil && hasConsole() {
		return
	}
	Hide(cmd)
}
