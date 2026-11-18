//go:build windows

package hideconsole

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestHideSetsFlagsAndPreservesExistingSysProcAttr(t *testing.T) {
	const existingFlag = 0x00000200 // CREATE_NEW_PROCESS_GROUP

	cmd := exec.Command("cmd.exe", "/c", "exit", "0")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CmdLine:       "custom",
		CreationFlags: existingFlag,
	}

	Hide(cmd)

	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil after Hide")
	}
	if cmd.SysProcAttr.CmdLine != "custom" {
		t.Fatalf("CmdLine = %q, want %q", cmd.SysProcAttr.CmdLine, "custom")
	}
	if cmd.SysProcAttr.CreationFlags&existingFlag == 0 {
		t.Fatalf("CreationFlags = %#x, want existing flag %#x preserved", cmd.SysProcAttr.CreationFlags, existingFlag)
	}
	if cmd.SysProcAttr.CreationFlags&createNoWindow == 0 {
		t.Fatalf("CreationFlags = %#x, want CREATE_NO_WINDOW (%#x)", cmd.SysProcAttr.CreationFlags, createNoWindow)
	}
	if !cmd.SysProcAttr.HideWindow {
		t.Fatal("HideWindow = false, want true")
	}
}

func TestHideIfConsolelessDependsOnConsoleAttachment(t *testing.T) {
	attachedCmd := exec.Command("cmd.exe", "/c", "exit", "0")
	hideIfConsoleless(attachedCmd, func() bool { return true })
	if attachedCmd.SysProcAttr != nil {
		t.Fatalf("attached child SysProcAttr = %#v, want nil", attachedCmd.SysProcAttr)
	}

	consolelessCmd := exec.Command("cmd.exe", "/c", "exit", "0")
	hideIfConsoleless(consolelessCmd, func() bool { return false })
	if consolelessCmd.SysProcAttr == nil {
		t.Fatal("consoleless child SysProcAttr is nil, want hidden flags")
	}
	if consolelessCmd.SysProcAttr.CreationFlags&createNoWindow == 0 {
		t.Fatalf("consoleless CreationFlags = %#x, want CREATE_NO_WINDOW (%#x)", consolelessCmd.SysProcAttr.CreationFlags, createNoWindow)
	}
	if !consolelessCmd.SysProcAttr.HideWindow {
		t.Fatal("consoleless HideWindow = false, want true")
	}
}
