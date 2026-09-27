// Package devinconfig resolves Devin's user-level configuration directory so
// the hook installer and the MCP installer agree on where Devin looks.
package devinconfig

import (
	"path/filepath"

	"github.com/ruiheng/waypost/internal/userdirs"
)

// UserConfigDir returns the directory Devin reads user-level configuration
// (config.json, mcp_config.json) from: %APPDATA%\devin on Windows with a
// AppData\Roaming fallback, $XDG_CONFIG_HOME/devin or ~/.config/devin on
// other platforms.
func UserConfigDir(home string) string {
	// AtHome never fails, so the platform root always resolves here.
	root, _ := userdirs.ConfigRoot(userdirs.AtHome(home))
	return filepath.Join(root, "devin")
}
