package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ruiheng/waypost/internal/hideconsole"
	"github.com/ruiheng/waypost/internal/launchpath"
)

const manifestRelativePath = "lib/waypost/active-version.json"

type activeVersionManifest struct {
	Version    string `json:"version"`
	Executable string `json:"executable"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	manifestPath, err := defaultManifestPath()
	if err != nil {
		return err
	}
	manifest, err := readManifest(manifestPath)
	if err != nil {
		return err
	}

	executable := manifest.Executable
	if executable == "" {
		if manifest.Version == "" {
			return fmt.Errorf("launcher manifest %q is missing version and executable", manifestPath)
		}
		executable = filepath.Join("versions", manifest.Version, executableName())
	}
	if !filepath.IsAbs(executable) {
		executable = filepath.Join(filepath.Dir(manifestPath), executable)
	}

	cmd, err := newChildCommand(executable, args)
	if err != nil {
		return err
	}
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	defer suppressParentInterrupt()()
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		return fmt.Errorf("launch %q: %w", executable, err)
	}
	return nil
}

func newChildCommand(executable string, args []string) (*exec.Cmd, error) {
	launcherExecutable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve launcher executable path: %w", err)
	}
	launcherExecutable, err = filepath.Abs(launcherExecutable)
	if err != nil {
		return nil, fmt.Errorf("resolve absolute launcher executable path: %w", err)
	}
	cmd := exec.Command(executable, args...)
	hideconsole.HideIfConsoleless(cmd)
	cmd.Env = append(os.Environ(), launchpath.StableExecutableEnv+"="+launcherExecutable)
	return cmd, nil
}

func defaultManifestPath() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve launcher executable path: %w", err)
	}
	prefix := filepath.Dir(filepath.Dir(executable))
	return filepath.Join(prefix, filepath.FromSlash(manifestRelativePath)), nil
}

func readManifest(path string) (activeVersionManifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return activeVersionManifest{}, fmt.Errorf("read launcher manifest %q: %w", path, err)
	}
	var manifest activeVersionManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return activeVersionManifest{}, fmt.Errorf("parse launcher manifest %q: %w", path, err)
	}
	return manifest, nil
}

func executableName() string {
	argv0 := ""
	if len(os.Args) > 0 {
		argv0 = os.Args[0]
	}
	return executableNameForOS(runtime.GOOS, argv0)
}

func executableNameForOS(goos, argv0 string) string {
	if goos == "windows" || strings.EqualFold(filepath.Ext(argv0), ".exe") {
		return "waypost.exe"
	}
	return "waypost"
}
