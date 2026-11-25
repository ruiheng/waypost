package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/ruiheng/waypost/internal/launchpath"
)

func TestNewChildCommandPublishesStableLauncherPath(t *testing.T) {
	t.Setenv(launchpath.StableExecutableEnv, "stale-value")

	cmd, err := newChildCommand("versioned-waypost", []string{"codex-hook"})
	if err != nil {
		t.Fatalf("newChildCommand() error = %v", err)
	}
	current, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	current, err = filepath.Abs(current)
	if err != nil {
		t.Fatalf("filepath.Abs(current executable) error = %v", err)
	}

	var got string
	prefix := launchpath.StableExecutableEnv + "="
	for _, item := range cmd.Env {
		if strings.HasPrefix(item, prefix) {
			got = strings.TrimPrefix(item, prefix)
		}
	}
	if got != current {
		t.Fatalf("%s = %q, want current launcher %q", launchpath.StableExecutableEnv, got, current)
	}
}

func TestExecutableNameForOS(t *testing.T) {
	t.Parallel()

	cases := []struct {
		goos  string
		argv0 string
		want  string
	}{
		{goos: "windows", argv0: `C:\bin\waypost-launcher`, want: "waypost.exe"},
		{goos: "windows", argv0: `C:\bin\waypost-launcher.exe`, want: "waypost.exe"},
		{goos: "linux", argv0: `/usr/local/bin/waypost-launcher`, want: "waypost"},
		{goos: "darwin", argv0: `/opt/bin/waypost-launcher.EXE`, want: "waypost.exe"},
	}
	for _, tc := range cases {
		if got := executableNameForOS(tc.goos, tc.argv0); got != tc.want {
			t.Errorf("executableNameForOS(%q, %q) = %q, want %q", tc.goos, tc.argv0, got, tc.want)
		}
	}

	wantCurrent := "waypost"
	if runtime.GOOS == "windows" {
		wantCurrent = "waypost.exe"
	}
	if got := executableName(); got != wantCurrent {
		t.Fatalf("executableName() = %q, want %q", got, wantCurrent)
	}
}
