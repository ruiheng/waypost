package devinconfig

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestUserConfigDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Run("appdata", func(t *testing.T) {
			t.Setenv("APPDATA", `C:\Users\test\AppData\Roaming`)
			got := UserConfigDir(`C:\Users\test`)
			if want := filepath.Join(`C:\Users\test\AppData\Roaming`, "devin"); got != want {
				t.Fatalf("UserConfigDir() = %q, want %q", got, want)
			}
		})
		t.Run("appdata fallback", func(t *testing.T) {
			t.Setenv("APPDATA", "")
			got := UserConfigDir(`C:\Users\test`)
			if want := filepath.Join(`C:\Users\test`, "AppData", "Roaming", "devin"); got != want {
				t.Fatalf("UserConfigDir() = %q, want %q", got, want)
			}
		})
		return
	}

	t.Run("xdg", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "/custom/config")
		got := UserConfigDir("/home/test")
		if want := "/custom/config/devin"; got != want {
			t.Fatalf("UserConfigDir() = %q, want %q", got, want)
		}
	})
	t.Run("xdg whitespace ignored", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "   ")
		got := UserConfigDir("/home/test")
		if want := "/home/test/.config/devin"; got != want {
			t.Fatalf("UserConfigDir() = %q, want %q", got, want)
		}
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv("XDG_CONFIG_HOME", "")
		got := UserConfigDir("/home/test")
		if want := "/home/test/.config/devin"; got != want {
			t.Fatalf("UserConfigDir() = %q, want %q", got, want)
		}
	})
}
