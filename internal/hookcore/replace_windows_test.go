//go:build windows

package hookcore

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsAPIPathAddsExtendedPrefixWhenNeeded(t *testing.T) {
	volume := filepath.VolumeName(t.TempDir())
	longTail := strings.Repeat(`segment\`, 40) + "hooks.json"
	longDrivePath := volume + `\` + longTail
	longUNCPath := `\\server\share\` + longTail
	longRelativePath := longTail
	shortPath := filepath.Join(t.TempDir(), "hooks.json")
	wantRelative, err := filepath.Abs(longRelativePath)
	if err != nil {
		t.Fatalf("filepath.Abs(long relative path) error = %v", err)
	}

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "short absolute", path: shortPath, want: shortPath},
		{name: "long drive", path: longDrivePath, want: `\\?\` + longDrivePath},
		{name: "long UNC", path: longUNCPath, want: `\\?\UNC\server\share\` + longTail},
		{name: "long relative", path: longRelativePath, want: `\\?\` + wantRelative},
		{name: "extended", path: `\\?\C:\already\extended`, want: `\\?\C:\already\extended`},
		{name: "NT extended", path: `\??\C:\already\extended`, want: `\??\C:\already\extended`},
		{name: "device", path: `\\.\C:\device\path`, want: `\\.\C:\device\path`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := windowsAPIPath(test.path)
			if err != nil {
				t.Fatalf("windowsAPIPath(%q) error = %v", test.path, err)
			}
			if got != test.want {
				t.Fatalf("windowsAPIPath(%q) = %q, want %q", test.path, got, test.want)
			}
		})
	}
}
