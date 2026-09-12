//go:build windows

package codexhook

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallPreservesWindowsHooksDACL(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "hooks.json")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("WriteFile(hooks.json) error = %v", err)
	}

	// Convert inherited ACEs to explicit ACEs. A replacement that merely
	// inherits the parent directory DACL will then differ, while the current
	// token keeps every permission it already had.
	output, err := exec.Command("icacls.exe", path, "/inheritance:d").CombinedOutput()
	if err != nil {
		t.Fatalf("set hooks DACL: %v; output = %q", err, output)
	}
	before := windowsFileACL(t, path)

	result, err := Install(home, `"C:\Program Files\Waypost\waypost.exe" codex-hook`)
	if err != nil {
		t.Fatalf("Install() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Install() changed = false, want updated hooks")
	}
	after := windowsFileACL(t, path)
	if after != before {
		t.Fatalf("hooks DACL changed across replacement:\nbefore: %s\nafter:  %s", before, after)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(hooks.json) error = %v", err)
	}
	if !strings.Contains(string(contents), "codex-hook") {
		t.Fatalf("hooks.json = %q, want installed hook", contents)
	}
}

func TestInstallSupportsWindowsLongHooksPath(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "new"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			home := longWindowsHooksHome(t, name)
			path := filepath.Join(home, "hooks.json")
			if existing {
				if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
					t.Fatalf("WriteFile(hooks.json) error = %v", err)
				}
			}
			result, err := Install(home, `"C:\Program Files\Waypost\waypost.exe" codex-hook`)
			if err != nil {
				t.Fatalf("Install() error = %v", err)
			}
			if !result.Changed {
				t.Fatal("Install() changed = false, want updated hooks")
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile(hooks.json) error = %v", err)
			}
			if !strings.Contains(string(contents), "codex-hook") {
				t.Fatalf("hooks.json = %q, want installed hook", contents)
			}
		})
	}
}

func longWindowsHooksHome(t *testing.T, suffix string) string {
	t.Helper()
	home := t.TempDir()
	for index := 0; len(filepath.Join(home, "hooks.json")) <= 300; index++ {
		home = filepath.Join(home, fmt.Sprintf("segment-%02d-%s", index, strings.Repeat("x", 24)))
	}
	home = filepath.Join(home, suffix)
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatalf("MkdirAll(long hooks home) error = %v", err)
	}
	return home
}

func windowsFileACL(t *testing.T, path string) string {
	t.Helper()
	output, err := exec.Command("icacls.exe", path).CombinedOutput()
	if err != nil {
		t.Fatalf("read hooks DACL: %v; output = %q", err, output)
	}
	return strings.TrimSpace(strings.ReplaceAll(string(output), "\r\n", "\n"))
}
