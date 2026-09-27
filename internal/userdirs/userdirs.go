// Package userdirs centralizes resolution of the per-user directories Waypost
// and the agents it integrates with consult. Environment overrides and
// platform fallbacks live here so there is one owner of the convention — and
// one surface tests must isolate (see internal/testenv).
//
// Resolvers take resolveHome so callers never pay for os.UserHomeDir when an
// environment override is already set: pass os.UserHomeDir for lazy lookup or
// AtHome(home) when the home directory is already known.
package userdirs

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// HomeFunc resolves the user's home directory, matching os.UserHomeDir.
type HomeFunc = func() (string, error)

// AtHome adapts an already-resolved home directory to HomeFunc.
func AtHome(home string) HomeFunc {
	return func() (string, error) { return home, nil }
}

// ConfigRoot returns the platform's per-user configuration root: %APPDATA% on
// Windows (falling back to home\AppData\Roaming), $XDG_CONFIG_HOME or
// home/.config on other platforms.
func ConfigRoot(resolveHome HomeFunc) (string, error) {
	if runtime.GOOS == "windows" {
		if dir := envValue("APPDATA"); dir != "" {
			return dir, nil
		}
		return homeFallback(resolveHome, "AppData", "Roaming")
	}
	if dir := envValue("XDG_CONFIG_HOME"); dir != "" {
		return dir, nil
	}
	return homeFallback(resolveHome, ".config")
}

// StateRoot returns $XDG_STATE_HOME or home/.local/state.
func StateRoot(resolveHome HomeFunc) (string, error) {
	if dir := envValue("XDG_STATE_HOME"); dir != "" {
		return dir, nil
	}
	return homeFallback(resolveHome, ".local", "state")
}

// DataRoot returns $XDG_DATA_HOME or home/.local/share. Per the XDG base-dir
// specification a relative override is ignored rather than resolved against
// the process working directory.
func DataRoot(resolveHome HomeFunc) (string, error) {
	if dir := envValue("XDG_DATA_HOME"); dir != "" && filepath.IsAbs(dir) {
		return dir, nil
	}
	return homeFallback(resolveHome, ".local", "share")
}

// EnvOrHome resolves the "override env, else a dot-directory under home"
// convention used by agent CLIs (CODEX_HOME, CLAUDE_CONFIG_DIR, ...): when env
// is set its value is returned as an absolute path; otherwise the resolved
// home is joined with fallback.
func EnvOrHome(env string, resolveHome HomeFunc, fallback ...string) (string, error) {
	if dir := envValue(env); dir != "" {
		absolute, err := filepath.Abs(dir)
		if err != nil {
			return "", fmt.Errorf("resolve %s directory: %w", env, err)
		}
		return absolute, nil
	}
	return homeFallback(resolveHome, fallback...)
}

func envValue(name string) string {
	return strings.TrimSpace(os.Getenv(name))
}

func homeFallback(resolveHome HomeFunc, elems ...string) (string, error) {
	home, err := resolveHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(append([]string{home}, elems...)...), nil
}
