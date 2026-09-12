// Package devinconfig resolves Devin's user-level configuration directory so
// the hook installer and the MCP installer agree on where Devin looks.
package devinconfig

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// UserConfigDir returns the directory Devin reads user-level configuration
// (config.json, mcp_config.json) from: %APPDATA%\devin on Windows with a
// AppData\Roaming fallback, $XDG_CONFIG_HOME/devin or ~/.config/devin on
// other platforms.
func UserConfigDir(home string) string {
	if runtime.GOOS == "windows" {
		if appData := strings.TrimSpace(os.Getenv("APPDATA")); appData != "" {
			return filepath.Join(appData, "devin")
		}
		return filepath.Join(home, "AppData", "Roaming", "devin")
	}
	if xdg := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); xdg != "" {
		return filepath.Join(xdg, "devin")
	}
	return filepath.Join(home, ".config", "devin")
}
