package userdirs

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestConfigRootHonorsPlatformOverride(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Setenv("APPDATA", `C:\Test\AppData\Roaming`)
		got, err := ConfigRoot(AtHome(`C:\Test\home`))
		if err != nil {
			t.Fatalf("ConfigRoot() error = %v", err)
		}
		if got != `C:\Test\AppData\Roaming` {
			t.Fatalf("ConfigRoot() = %q, want APPDATA", got)
		}
		return
	}
	t.Setenv("XDG_CONFIG_HOME", "/custom/config")
	got, err := ConfigRoot(AtHome("/home/test"))
	if err != nil {
		t.Fatalf("ConfigRoot() error = %v", err)
	}
	if got != "/custom/config" {
		t.Fatalf("ConfigRoot() = %q, want XDG_CONFIG_HOME", got)
	}
}

func TestConfigRootFallsBackUnderHome(t *testing.T) {
	t.Setenv("APPDATA", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	got, err := ConfigRoot(AtHome("/home/test"))
	if err != nil {
		t.Fatalf("ConfigRoot() error = %v", err)
	}
	want := "/home/test/.config"
	if runtime.GOOS == "windows" {
		want = filepath.Join("/home/test", "AppData", "Roaming")
	}
	if got != want {
		t.Fatalf("ConfigRoot() = %q, want %q", got, want)
	}
}

func TestStateRoot(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", "/custom/state")
	got, err := StateRoot(AtHome("/home/test"))
	if err != nil {
		t.Fatalf("StateRoot() error = %v", err)
	}
	if got != "/custom/state" {
		t.Fatalf("StateRoot() = %q, want override", got)
	}

	t.Setenv("XDG_STATE_HOME", "")
	got, err = StateRoot(AtHome("/home/test"))
	if err != nil {
		t.Fatalf("StateRoot() error = %v", err)
	}
	if want := filepath.Join("/home/test", ".local", "state"); got != want {
		t.Fatalf("StateRoot() = %q, want %q", got, want)
	}
}

func TestDataRootIgnoresRelativeOverride(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", "relative/data")
	got, err := DataRoot(AtHome("/home/test"))
	if err != nil {
		t.Fatalf("DataRoot() error = %v", err)
	}
	if want := filepath.Join("/home/test", ".local", "share"); got != want {
		t.Fatalf("DataRoot() = %q, want home fallback %q", got, want)
	}
}

func TestEnvOrHome(t *testing.T) {
	t.Setenv("TEST_AGENT_HOME", "")
	got, err := EnvOrHome("TEST_AGENT_HOME", AtHome("/home/test"), ".agent")
	if err != nil {
		t.Fatalf("EnvOrHome() error = %v", err)
	}
	if want := filepath.Join("/home/test", ".agent"); got != want {
		t.Fatalf("EnvOrHome() = %q, want %q", got, want)
	}

	override := filepath.Join(t.TempDir(), "agent-dir")
	t.Setenv("TEST_AGENT_HOME", override)
	got, err = EnvOrHome("TEST_AGENT_HOME", AtHome("/home/test"), ".agent")
	if err != nil {
		t.Fatalf("EnvOrHome() error = %v", err)
	}
	if got != override {
		t.Fatalf("EnvOrHome() = %q, want override %q", got, override)
	}
}

func TestEnvOrHomeSkipsHomeWhenOverrideSet(t *testing.T) {
	t.Setenv("TEST_AGENT_HOME", t.TempDir())
	resolveHome := func() (string, error) {
		t.Fatal("resolveHome called despite env override")
		return "", nil
	}
	if _, err := EnvOrHome("TEST_AGENT_HOME", resolveHome, ".agent"); err != nil {
		t.Fatalf("EnvOrHome() error = %v", err)
	}
}
