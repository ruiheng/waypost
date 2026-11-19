//go:build windows

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ruiheng/waypost/internal/launchpath"
)

const (
	consoleTestRoleEnv         = "WAYPOST_LAUNCHER_CONSOLE_TEST_ROLE"
	consoleTestPrefixEnv       = "WAYPOST_LAUNCHER_CONSOLE_TEST_PREFIX"
	consoleTestRoleParent      = "helper-parent"
	consoleTestRoleChild       = "helper-child"
	consoleTestRoleRunLauncher = "run-launcher"
	consoleTestRoleDetached    = "helper-detached"
	consoleTestInheritName     = "^TestNewChildCommandInheritsConsoleControlEvents$"
	consoleTestDetachedName    = "^TestNewChildCommandHidesConsoleWhenLauncherDetached$"
)

const (
	consoleTestCtrlBreakEvent    = 1
	consoleTestDetachedProcess   = 0x00000008
	consoleTestCreateNewConsole  = 0x00000010
	consoleTestCreateNoWindow    = 0x08000000
	consoleTestLauncherChildExit = 42
)

var (
	consoleTestKernel32 = syscall.NewLazyDLL("kernel32.dll")
	consoleTestCPProc   = consoleTestKernel32.NewProc("GetConsoleCP")
	consoleTestCtrlProc = consoleTestKernel32.NewProc("GenerateConsoleCtrlEvent")
)

type consoleTestChild struct {
	name     string
	cmd      *exec.Cmd
	stderr   *bytes.Buffer
	scanDone chan struct{}
}

func consoleTestCP() uintptr {
	cp, _, _ := consoleTestCPProc.Call()
	return cp
}

func consoleTestEnvWith(env []string, updates map[string]string) []string {
	out := make([]string, 0, len(env)+len(updates))
	for _, item := range env {
		key, _, hasEq := strings.Cut(item, "=")
		if !hasEq {
			out = append(out, item)
			continue
		}
		skip := false
		for target := range updates {
			if strings.EqualFold(key, target) {
				skip = true
				break
			}
		}
		if !skip {
			out = append(out, item)
		}
	}
	for k, v := range updates {
		if v != "" {
			out = append(out, k+"="+v)
		}
	}
	return out
}

func consoleTestParseLine(line string) (name, kind string, kv map[string]string, ok bool) {
	fields := strings.Fields(line)
	if len(fields) < 3 {
		return "", "", nil, false
	}
	name, kind = fields[0], fields[1]
	kv = make(map[string]string, len(fields)-2)
	for _, field := range fields[2:] {
		if k, v, found := strings.Cut(field, "="); found {
			kv[k] = v
		}
	}
	return name, kind, kv, true
}

func consoleTestKillAll(children map[string]*consoleTestChild) {
	for _, child := range children {
		if child.cmd.Process != nil {
			_ = child.cmd.Process.Kill()
		}
	}
}

func copyTestExecutable(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}

// TestNewChildCommandInheritsConsoleControlEvents runs inside an isolated,
// hidden CREATE_NEW_CONSOLE subprocess so CTRL_BREAK_EVENT is never broadcast
// to the outer go test console.
func TestNewChildCommandInheritsConsoleControlEvents(t *testing.T) {
	switch os.Getenv(consoleTestRoleEnv) {
	case consoleTestRoleParent:
		consoleTestParentMain()
		return
	case consoleTestRoleChild:
		consoleTestChildMain(0)
		return
	case consoleTestRoleRunLauncher:
		if os.Getenv(launchpath.StableExecutableEnv) != "" {
			consoleTestChildMain(consoleTestLauncherChildExit)
			return
		}
		if err := run(os.Args[1:]); err != nil {
			fmt.Fprintf(os.Stderr, "run() error = %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	prefix := t.TempDir()
	testExe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	launcherExe := filepath.Join(prefix, "bin", "waypost.exe")
	if err := copyTestExecutable(testExe, launcherExe); err != nil {
		t.Fatalf("copyTestExecutable() error = %v", err)
	}
	manifestPath := filepath.Join(prefix, filepath.FromSlash(manifestRelativePath))
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(manifest) error = %v", err)
	}
	manifestBytes, err := json.Marshal(activeVersionManifest{
		Version:    "test",
		Executable: testExe,
	})
	if err != nil {
		t.Fatalf("json.Marshal(manifest) error = %v", err)
	}
	if err := os.WriteFile(manifestPath, manifestBytes, 0o644); err != nil {
		t.Fatalf("WriteFile(manifest) error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run="+consoleTestInheritName)
	cmd.Env = consoleTestEnvWith(os.Environ(), map[string]string{
		consoleTestRoleEnv:   consoleTestRoleParent,
		consoleTestPrefixEnv: prefix,
	})
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: consoleTestCreateNewConsole,
		HideWindow:    true,
	}
	out, err := cmd.CombinedOutput()
	t.Logf("helper-parent output:\n%s", out)
	if ctx.Err() != nil {
		t.Fatalf("helper-parent timed out: %v", ctx.Err())
	}
	if err != nil {
		t.Fatalf("helper-parent failed: %v", err)
	}

	outcomes := map[string]string{}
	exits := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "CHILD_RESULT" {
			continue
		}
		kv := map[string]string{}
		for _, field := range fields[1:] {
			if k, v, ok := strings.Cut(field, "="); ok {
				kv[k] = v
			}
		}
		if name := kv["name"]; name != "" {
			outcomes[name] = kv["outcome"]
			exits[name] = kv["exit"]
		}
	}

	if outcomes["direct"] != "GOT_BREAK" {
		t.Fatalf("direct child outcome = %q (exit=%q), want GOT_BREAK", outcomes["direct"], exits["direct"])
	}
	if exits["direct"] != "0" {
		t.Fatalf("direct child exit = %q, want 0", exits["direct"])
	}
	if outcomes["launcher"] != "GOT_BREAK" {
		t.Fatalf("launcher run() child outcome = %q (exit=%q), want GOT_BREAK", outcomes["launcher"], exits["launcher"])
	}
	if exits["launcher"] != strconv.Itoa(consoleTestLauncherChildExit) {
		t.Fatalf("launcher run() exit = %q, want %d (child exit code preserved across CTRL_BREAK)", exits["launcher"], consoleTestLauncherChildExit)
	}
}

func TestNewChildCommandHidesConsoleWhenLauncherDetached(t *testing.T) {
	if os.Getenv(consoleTestRoleEnv) == consoleTestRoleDetached {
		cp := consoleTestCP()
		cmd, err := newChildCommand("versioned-waypost", []string{"status"})
		if err != nil {
			fmt.Printf("ERR=%v\n", err)
			os.Exit(1)
		}
		var flags uint32
		var hideWindow bool
		if cmd.SysProcAttr != nil {
			flags = cmd.SysProcAttr.CreationFlags
			hideWindow = cmd.SysProcAttr.HideWindow
		}
		fmt.Printf("DETACHED consoleCP=%d flags=%#x hideWindow=%v\n", cp, flags, hideWindow)
		os.Exit(0)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run="+consoleTestDetachedName)
	cmd.Env = consoleTestEnvWith(os.Environ(), map[string]string{
		consoleTestRoleEnv: consoleTestRoleDetached,
	})
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: consoleTestDetachedProcess,
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("detached helper failed: %v\noutput:\n%s", err, out)
	}

	var kv map[string]string
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "DETACHED" {
			kv = map[string]string{}
			for _, field := range fields[1:] {
				if k, v, ok := strings.Cut(field, "="); ok {
					kv[k] = v
				}
			}
			break
		}
	}
	if kv == nil {
		t.Fatalf("missing DETACHED output line:\n%s", out)
	}
	if kv["consoleCP"] != "0" {
		t.Fatalf("detached helper consoleCP = %q, want 0", kv["consoleCP"])
	}
	flags, err := strconv.ParseUint(kv["flags"], 0, 32)
	if err != nil {
		t.Fatalf("parse flags %q: %v", kv["flags"], err)
	}
	if uint32(flags)&consoleTestCreateNoWindow == 0 {
		t.Fatalf("detached child CreationFlags = %#x, want CREATE_NO_WINDOW (%#x)", flags, consoleTestCreateNoWindow)
	}
	if kv["hideWindow"] != "true" {
		t.Fatalf("detached child HideWindow = %q, want true", kv["hideWindow"])
	}
}

func consoleTestParentMain() {
	drained := make(chan os.Signal, 8)
	signal.Notify(drained, os.Interrupt)
	defer signal.Stop(drained)
	go func() {
		for range drained {
		}
	}()

	cp := consoleTestCP()
	fmt.Printf("PARENT consoleCP=%d\n", cp)
	if cp == 0 {
		fmt.Println("PARENT_ERROR helper-parent owns no console; CREATE_NEW_CONSOLE unavailable")
		os.Exit(2)
	}

	prefix := os.Getenv(consoleTestPrefixEnv)
	launcherExe := filepath.Join(prefix, "bin", "waypost.exe")
	lines := make(chan string, 256)
	children := map[string]*consoleTestChild{}
	start := func(name string, build func() (*exec.Cmd, error), extraEnv map[string]string) bool {
		cmd, err := build()
		if err != nil {
			fmt.Printf("PARENT_ERROR build %s: %v\n", name, err)
			return false
		}
		base := cmd.Env
		if base == nil {
			base = os.Environ()
		}
		cmd.Env = consoleTestEnvWith(base, extraEnv)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			fmt.Printf("PARENT_ERROR stdout pipe %s: %v\n", name, err)
			return false
		}
		stderr := &bytes.Buffer{}
		cmd.Stderr = stderr
		if err := cmd.Start(); err != nil {
			fmt.Printf("PARENT_ERROR start %s: %v\n", name, err)
			return false
		}
		scanDone := make(chan struct{})
		children[name] = &consoleTestChild{name: name, cmd: cmd, stderr: stderr, scanDone: scanDone}
		go func(pipe io.Reader, done chan struct{}) {
			defer close(done)
			scanner := bufio.NewScanner(pipe)
			for scanner.Scan() {
				lines <- name + " " + scanner.Text()
			}
		}(stdout, scanDone)
		return true
	}

	if !start("direct", func() (*exec.Cmd, error) {
		return newChildCommand(os.Args[0], []string{"-test.run=" + consoleTestInheritName})
	}, map[string]string{
		consoleTestRoleEnv: consoleTestRoleChild,
	}) || !start("launcher", func() (*exec.Cmd, error) {
		return exec.Command(launcherExe, "-test.run="+consoleTestInheritName), nil
	}, map[string]string{
		consoleTestRoleEnv:             consoleTestRoleRunLauncher,
		launchpath.StableExecutableEnv: "",
	}) {
		consoleTestKillAll(children)
		os.Exit(2)
	}

	ready := map[string]bool{}
	readyDeadline := time.After(15 * time.Second)
	for len(ready) < 2 {
		select {
		case line := <-lines:
			fmt.Println("CHILD_LINE " + line)
			name, kind, kv, ok := consoleTestParseLine(line)
			if ok && kind == "READY" {
				if _, err := strconv.ParseUint(kv["consoleCP"], 10, 64); err == nil {
					ready[name] = true
				}
			}
		case <-readyDeadline:
			fmt.Println("PARENT_ERROR timed out waiting for child READY")
			consoleTestKillAll(children)
			os.Exit(2)
		}
	}

	r1, _, callErr := consoleTestCtrlProc.Call(consoleTestCtrlBreakEvent, 0)
	if r1 == 0 {
		fmt.Printf("PARENT_ERROR GenerateConsoleCtrlEvent failed: %v\n", callErr)
		consoleTestKillAll(children)
		os.Exit(2)
	}
	fmt.Println("PARENT broadcast CTRL_BREAK_EVENT to own private console")

	outcomes := map[string]string{}
	outcomeDeadline := time.After(8 * time.Second)
outcomeLoop:
	for len(outcomes) < 2 {
		select {
		case line := <-lines:
			fmt.Println("CHILD_LINE " + line)
			name, kind, kv, ok := consoleTestParseLine(line)
			if ok && kind == "OUTCOME" {
				outcomes[name] = kv["outcome"]
			}
		case <-outcomeDeadline:
			fmt.Println("PARENT_ERROR timed out waiting for child OUTCOME")
			consoleTestKillAll(children)
			break outcomeLoop
		}
	}

	type childExit struct {
		name string
		code string
	}
	exitCh := make(chan childExit, len(children))
	for _, child := range children {
		go func(c *consoleTestChild) {
			select {
			case <-c.scanDone:
			case <-time.After(8 * time.Second):
				if c.cmd.Process != nil {
					_ = c.cmd.Process.Kill()
				}
				<-c.scanDone
			}
			err := c.cmd.Wait()
			if err == nil {
				exitCh <- childExit{name: c.name, code: "0"}
				return
			}
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				exitCh <- childExit{name: c.name, code: strconv.Itoa(exitErr.ExitCode())}
				return
			}
			exitCh <- childExit{name: c.name, code: "err:" + err.Error()}
		}(child)
	}
	exits := map[string]string{}
	waitDeadline := time.After(10 * time.Second)
waitLoop:
	for len(exits) < len(children) {
		select {
		case res := <-exitCh:
			exits[res.name] = res.code
		case <-waitDeadline:
			fmt.Println("PARENT_ERROR child wait timeout; killing probe children")
			consoleTestKillAll(children)
			break waitLoop
		}
	}
	for name, child := range children {
		if trimmed := strings.TrimSpace(child.stderr.String()); trimmed != "" {
			fmt.Printf("CHILD_STDERR %s %q\n", name, trimmed)
		}
	}

	for _, name := range []string{"direct", "launcher"} {
		outcome := outcomes[name]
		if outcome == "" {
			outcome = "MISSING"
		}
		code := exits[name]
		if code == "" {
			code = "MISSING"
		}
		fmt.Printf("CHILD_RESULT name=%s outcome=%s exit=%s\n", name, outcome, code)
	}
	os.Exit(0)
}

func consoleTestChildMain(breakExit int) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt)
	cp := consoleTestCP()
	fmt.Printf("READY consoleCP=%d\n", cp)
	select {
	case sig := <-signals:
		fmt.Printf("OUTCOME outcome=GOT_BREAK sig=%s consoleCP=%d\n", sig, cp)
		os.Exit(breakExit)
	case <-time.After(1500 * time.Millisecond):
		fmt.Printf("OUTCOME outcome=NO_BREAK consoleCP=%d\n", cp)
		os.Exit(breakExit + 1)
	}
}
