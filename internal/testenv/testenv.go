// Package testenv provides shared environment isolation for tests that reach
// code resolving per-user directories. Pinning these variables through one
// helper keeps a missed platform override (for example %APPDATA% on Windows,
// where XDG_CONFIG_HOME has no effect) from leaking reads — or writes — into
// the real user profile.
package testenv

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// IsolateUserDirs redirects every environment variable consulted for
// user-level directories to subdirectories of home, and points HOME and
// USERPROFILE at home so os.UserHomeDir stays inside the sandbox on every
// platform. Pass a per-test directory (t.TempDir()) so tests do not share
// state.
//
// t.Setenv forbids parallel tests: do not mark a test t.Parallel when it
// calls this helper. Tests asserting a specific resolver layout may re-pin an
// individual variable after calling IsolateUserDirs — the later Setenv wins.
func IsolateUserDirs(t *testing.T, home string) {
	t.Helper()
	PinHome(t, home)
	for _, pair := range floorVars(home) {
		t.Setenv(pair[0], pair[1])
	}
}

// PinHome redirects HOME and USERPROFILE — the variables os.UserHomeDir
// consults on Unix and Windows respectively — to home.
func PinHome(t *testing.T, home string) {
	t.Helper()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
}

// RunMain floors the same user-directory variables IsolateUserDirs pins at a
// process-wide temporary root, then runs m. It is the TestMain form of
// IsolateUserDirs for packages where any test may reach user-directory
// resolution without per-test isolation:
//
//	func TestMain(m *testing.M) { os.Exit(testenv.RunMain(m)) }
//
// Per-test t.Setenv overrides still take precedence inside each test.
func RunMain(m *testing.M) int {
	home, err := os.MkdirTemp("", "waypost-test-home")
	if err != nil {
		fmt.Fprintln(os.Stderr, "testenv: create floor home:", err)
		return 1
	}
	defer os.RemoveAll(home)
	for _, pair := range floorVars(home) {
		if err := os.Setenv(pair[0], pair[1]); err != nil {
			fmt.Fprintf(os.Stderr, "testenv: set %s: %v\n", pair[0], err)
			return 1
		}
	}
	// HOME/USERPROFILE ride along too so os.UserHomeDir resolves under the
	// floor for tests that do not pin it.
	for _, name := range []string{"HOME", "USERPROFILE"} {
		if err := os.Setenv(name, home); err != nil {
			fmt.Fprintf(os.Stderr, "testenv: set %s: %v\n", name, err)
			return 1
		}
	}
	return m.Run()
}

func floorVars(home string) [][2]string {
	return [][2]string{
		{"APPDATA", filepath.Join(home, "AppData", "Roaming")},
		{"XDG_CONFIG_HOME", filepath.Join(home, "xdg_config")},
		{"XDG_STATE_HOME", filepath.Join(home, "xdg_state")},
		{"XDG_DATA_HOME", filepath.Join(home, "xdg_data")},
		{"CLAUDE_CONFIG_DIR", filepath.Join(home, ".claude")},
		{"CODEX_HOME", filepath.Join(home, ".codex")},
		{"WAYPOST_STATE_DIR", filepath.Join(home, "waypost_state")},
		{"AGENTDECK_PROFILE", ""},
	}
}
