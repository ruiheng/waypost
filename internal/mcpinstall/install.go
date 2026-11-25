package mcpinstall

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ruiheng/waypost/internal/codexhook"
	"github.com/ruiheng/waypost/internal/devinconfig"
	"github.com/ruiheng/waypost/internal/hookcore"
	"github.com/ruiheng/waypost/internal/jsonc"
	"github.com/ruiheng/waypost/internal/launchpath"
	"github.com/ruiheng/waypost/internal/userdirs"
)

const (
	serverName          = "waypost"
	serverSubcommand    = "mcp"
	configFileName      = "config.toml"
	claudeConfigName    = ".claude.json"
	agyMCPConfigName    = ".gemini/config/mcp_config.json"
	toolTimeoutSeconds  = 660
	commandFailureLimit = 500
)

var requiredEnvVars = []string{
	"TMUX",
	"AGENTDECK_INSTANCE_ID",
	"CODEX_THREAD_ID",
	"CODEX_SESSION_ID",
	"WAYPOST_STATE_DIR",
	"XDG_STATE_HOME",
}

// Result describes the agent configurations changed by an MCP installation.
type Result struct {
	// Path is the Codex config path; it is empty when Codex was not detected.
	Path       string
	Command    string
	Changed    bool
	Configured []ConfiguredAgent
	// Warnings describes non-fatal side effects of the installation, such as
	// JSONC syntax being normalized to strict JSON on rewrite.
	Warnings []string
}

// ConfiguredAgent names one agent configuration ensured by an installation.
type ConfiguredAgent struct {
	Name string
	Path string
}

type commandOutput struct {
	stdout []byte
	stderr []byte
}

type commandRunner func(context.Context, string, ...string) (commandOutput, error)

type dependencies struct {
	run               commandRunner
	resolveExecutable func() (string, error)
	resolveHome       func() (string, error)
	resolveUserHome   func() (string, error)
	lookPath          func(string) (string, error)
	getwd             func() (string, error)
	writeConfig       func(string, []byte, os.FileMode) error
}

// Install registers the built-in Waypost MCP server in the configuration of
// every detected agent: Codex, Claude Code, agy, and Devin. An agent is
// detected when its CLI is installed or its configuration already exists. It
// uses the stable Waypost executable path so upgrades do not leave an agent
// pointing at a versioned binary.
func Install(ctx context.Context) (Result, error) {
	return installWithDependencies(ctx, dependencies{
		run:               runCommand,
		resolveExecutable: launchpath.CurrentExecutable,
		resolveHome:       codexhook.DefaultHome,
		resolveUserHome:   os.UserHomeDir,
		lookPath:          exec.LookPath,
		getwd:             os.Getwd,
	})
}

func installWithDependencies(ctx context.Context, deps dependencies) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if deps.run == nil || deps.resolveExecutable == nil || deps.resolveHome == nil || deps.lookPath == nil {
		return Result{}, errors.New("MCP installer dependencies are incomplete")
	}

	executable, err := deps.resolveExecutable()
	if err != nil {
		return Result{}, fmt.Errorf("resolve Waypost executable: %w", err)
	}
	var userHome string
	if deps.resolveUserHome != nil {
		userHome, err = deps.resolveUserHome()
		if err != nil {
			return Result{}, fmt.Errorf("resolve user home for optional MCP integrations: %w", err)
		}
		if err := validateOptionalAgents(userHome, deps.lookPath); err != nil {
			return Result{}, err
		}
	}

	// Codex is configured when its CLI is installed or its global config
	// already exists. It is no longer a mandatory target: a Devin-only
	// machine must still be able to install the Waypost MCP server.
	codexPath, codexLookErr := deps.lookPath("codex")
	home, homeErr := deps.resolveHome()
	configureCodex := codexLookErr == nil
	configPath := ""
	codexConfigExists := false
	if homeErr == nil {
		configPath = filepath.Join(home, configFileName)
		if _, statErr := os.Stat(configPath); statErr == nil {
			configureCodex = true
			codexConfigExists = true
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return Result{}, fmt.Errorf("inspect Codex config %q: %w", configPath, statErr)
		}
	}
	if configureCodex && homeErr != nil {
		return Result{}, homeErr
	}

	result := Result{Command: executable + " " + serverSubcommand}
	changed := false
	if configureCodex {
		if err := os.MkdirAll(home, 0o700); err != nil {
			return Result{}, fmt.Errorf("create Codex config directory %q: %w", home, err)
		}
		if codexLookErr == nil && codexConfigExists {
			existing, err := os.ReadFile(configPath)
			if err != nil {
				return Result{}, fmt.Errorf("read Codex config %q: %w", configPath, err)
			}
			if _, _, err := ensureWaypostSection(string(existing), executable); err != nil {
				return Result{}, fmt.Errorf("update Codex config %q: %w", configPath, err)
			}
		}
		// Without the Codex CLI the registered args cannot be discovered, so
		// ensureCodexConfig preserves any args already present in config.toml.
		var args []string
		// codexRegistered records whether the Codex CLI has already written
		// the waypost server entry, so a later failure still reports Codex as
		// configured.
		codexRegistered := false
		if codexLookErr == nil {
			args = []string{serverSubcommand}
			getOutput, getErr := deps.run(ctx, codexPath, "mcp", "get", serverName, "--json")
			if getErr == nil {
				server, err := parseCodexServer(getOutput.stdout)
				if err != nil {
					return Result{}, fmt.Errorf("inspect Codex MCP server %q: %w", serverName, err)
				}
				if len(server.Transport.Args) > 0 && server.Transport.Args[0] == serverSubcommand {
					args = append([]string(nil), server.Transport.Args...)
				}
			} else {
				if err := ctx.Err(); err != nil {
					return Result{}, err
				}
				if !isMissingCodexMCPError(string(getOutput.stderr)) {
					return Result{}, commandError("inspect Codex MCP server", getOutput, getErr)
				}
				if output, err := deps.run(ctx, codexPath, "mcp", "add", serverName, "--", executable, serverSubcommand); err != nil {
					return Result{}, commandError("add Waypost MCP server to Codex", output, err)
				}
				codexRegistered = true
				changed = true
			}
		}

		configChanged, err := ensureCodexConfig(configPath, executable, deps.writeConfig, args...)
		if err != nil {
			if codexRegistered {
				result.Path = configPath
				result.Changed = changed
				result.Configured = append(result.Configured, ConfiguredAgent{Name: "Codex", Path: configPath})
				return result, fmt.Errorf("MCP server configured for Codex; failed to update Codex config: %w", err)
			}
			return Result{}, err
		}
		changed = changed || configChanged
		result.Path = configPath
		result.Configured = append(result.Configured, ConfiguredAgent{Name: "Codex", Path: configPath})
	}

	if deps.resolveUserHome != nil {
		configured, warnings, optional, err := installOptionalAgents(userHome, executable, deps.getwd, deps.lookPath)
		changed = changed || optional
		result.Configured = append(result.Configured, configured...)
		result.Warnings = append(result.Warnings, warnings...)
		if err != nil {
			if len(result.Configured) > 0 {
				result.Changed = changed
				return result, fmt.Errorf("MCP server configured for %s; remaining integrations failed: %w", configuredAgentNames(result.Configured), err)
			}
			result.Changed = changed
			return result, fmt.Errorf("optional MCP integrations failed: %w", err)
		}
	}
	if len(result.Configured) == 0 {
		return Result{}, fmt.Errorf("find Codex CLI: %w; no supported agent detected, install Codex, Claude Code, agy, or Devin before running `waypost install mcp-server`", codexLookErr)
	}
	result.Changed = changed
	return result, nil
}

type codexMCPServer struct {
	Name      string `json:"name"`
	Transport struct {
		Args []string `json:"args"`
	} `json:"transport"`
}

func parseCodexServer(output []byte) (codexMCPServer, error) {
	var server codexMCPServer
	if err := json.Unmarshal(output, &server); err != nil {
		return codexMCPServer{}, err
	}
	if server.Name != serverName {
		return codexMCPServer{}, fmt.Errorf("returned server %q", server.Name)
	}
	return server, nil
}

func ensureCodexConfig(path, executable string, writeConfig func(string, []byte, os.FileMode) error, args ...string) (bool, error) {
	contents, err := os.ReadFile(path)
	mode := os.FileMode(0o600)
	if err == nil {
		if info, statErr := os.Stat(path); statErr == nil {
			mode = info.Mode().Perm()
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("read Codex config %q: %w", path, err)
	}

	updated, changed, err := ensureWaypostSection(string(contents), executable, args...)
	if err != nil {
		return false, fmt.Errorf("update Codex config %q: %w", path, err)
	}
	if !changed {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, fmt.Errorf("create Codex config directory %q: %w", filepath.Dir(path), err)
	}
	if writeConfig == nil {
		writeConfig = writeFileAtomically
	}
	if err := writeConfig(path, []byte(updated), mode); err != nil {
		return false, fmt.Errorf("write Codex config %q: %w", path, err)
	}
	return true, nil
}

func installOptionalAgents(home, executable string, getwd func() (string, error), lookPath func(string) (string, error)) ([]ConfiguredAgent, []string, bool, error) {
	var configured []ConfiguredAgent
	var warnings []string
	changed := false
	claudePath := claudeConfigPath(home)
	configureClaude, err := shouldConfigureOptionalAgent(claudePath, "claude", lookPath)
	if err != nil {
		return configured, warnings, changed, fmt.Errorf("inspect Claude Code MCP config %q: %w", claudePath, err)
	}
	if configureClaude {
		claudeChanged, _, err := ensureClaudeConfig(claudePath, executable)
		if err != nil {
			return configured, warnings, changed, fmt.Errorf("configure Claude Code MCP server: %w", err)
		}
		changed = changed || claudeChanged
		configured = append(configured, ConfiguredAgent{Name: "Claude Code", Path: claudePath})
	}

	agyPath := filepath.Join(home, agyMCPConfigName)
	configureAgy, err := shouldConfigureOptionalAgent(agyPath, "agy", lookPath)
	if err != nil {
		return configured, warnings, changed, fmt.Errorf("inspect agy MCP config %q: %w", agyPath, err)
	}
	if configureAgy {
		agyChanged, _, err := ensureAgyConfig(agyPath, executable)
		if err != nil {
			return configured, warnings, changed, fmt.Errorf("configure agy MCP server: %w", err)
		}
		changed = changed || agyChanged
		configured = append(configured, ConfiguredAgent{Name: "agy", Path: agyPath})
	}

	devinPath := devinMCPConfigPath(home)
	configureDevin, err := shouldConfigureDevin(home, devinPath, lookPath)
	if err != nil {
		return configured, warnings, changed, fmt.Errorf("inspect Devin MCP config %q: %w", devinPath, err)
	}
	if configureDevin {
		devinChanged, wasJSONC, err := ensureDevinConfig(devinPath, executable)
		if err != nil {
			return configured, warnings, changed, fmt.Errorf("configure Devin MCP server: %w", err)
		}
		changed = changed || devinChanged
		configured = append(configured, ConfiguredAgent{Name: "Devin", Path: devinPath})
		if devinChanged && wasJSONC {
			warnings = append(warnings, fmt.Sprintf("%s contained JSONC comments or trailing commas; the rewrite emitted strict JSON and removed them", devinPath))
		}
		workDir := ""
		if getwd != nil {
			if dir, err := getwd(); err != nil {
				warnings = append(warnings, fmt.Sprintf("project-level Devin MCP override check skipped: resolve working directory: %v", err))
			} else {
				workDir = dir
			}
		}
		warnings = append(warnings, devinProjectOverrideWarnings(workDir)...)
	}
	return configured, warnings, changed, nil
}

// devinProjectOverrideWarnings reports project-level Devin MCP configs under
// dir (.devin/mcp_config.json, .devin/mcp_config.local.json) that define
// their own waypost server. Project entries take precedence over the
// user-level config the installer maintains, so a Devin session launched in
// dir may resolve a different definition.
func devinProjectOverrideWarnings(dir string) []string {
	if dir == "" {
		return nil
	}
	var warnings []string
	for _, name := range []string{"mcp_config.json", "mcp_config.local.json"} {
		path := filepath.Join(dir, ".devin", name)
		contents, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if normalized, _, err := jsonc.Normalize(contents); err == nil {
			contents = normalized
		}
		var root map[string]json.RawMessage
		if err := json.Unmarshal(contents, &root); err != nil {
			continue
		}
		var servers map[string]json.RawMessage
		if raw := root["mcpServers"]; len(raw) > 0 {
			if err := json.Unmarshal(raw, &servers); err != nil {
				continue
			}
		}
		if _, ok := servers[serverName]; ok {
			warnings = append(warnings, fmt.Sprintf("%s defines a %q server that takes precedence over the user-level entry; update or remove it to use the installed configuration", path, serverName))
		}
	}
	return warnings
}

func configuredAgentNames(agents []ConfiguredAgent) string {
	names := make([]string, 0, len(agents))
	for _, agent := range agents {
		names = append(names, agent.Name)
	}
	return strings.Join(names, ", ")
}

// shouldConfigureDevin detects Devin by its CLI, an existing MCP config, or
// an existing user-level config.json (for example left by
// `waypost install devin-hook`). Unlike the other agents, Devin keeps its
// main settings and MCP servers in separate files, so both count.
func shouldConfigureDevin(home, mcpConfigPath string, lookPath func(string) (string, error)) (bool, error) {
	if _, err := os.Stat(filepath.Join(devinconfig.UserConfigDir(home), "config.json")); err == nil {
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return shouldConfigureOptionalAgent(mcpConfigPath, "devin", lookPath)
}

func claudeConfigPath(home string) string {
	// AtHome never fails; env overrides come back normalized to absolute paths.
	dir, _ := userdirs.EnvOrHome("CLAUDE_CONFIG_DIR", userdirs.AtHome(home))
	return filepath.Join(dir, claudeConfigName)
}

func devinMCPConfigPath(home string) string {
	return filepath.Join(devinconfig.UserConfigDir(home), "mcp_config.json")
}

func validateOptionalAgents(home string, lookPath func(string) (string, error)) error {
	claudePath := claudeConfigPath(home)
	configureClaude, err := shouldConfigureOptionalAgent(claudePath, "claude", lookPath)
	if err != nil {
		return fmt.Errorf("inspect Claude Code MCP config %q: %w", claudePath, err)
	}
	if configureClaude {
		if err := validateJSONMCPConfig(claudePath, false); err != nil {
			return fmt.Errorf("validate Claude Code MCP config %q: %w", claudePath, err)
		}
	}

	agyPath := filepath.Join(home, agyMCPConfigName)
	configureAgy, err := shouldConfigureOptionalAgent(agyPath, "agy", lookPath)
	if err != nil {
		return fmt.Errorf("inspect agy MCP config %q: %w", agyPath, err)
	}
	if configureAgy {
		if err := validateJSONMCPConfig(agyPath, false); err != nil {
			return fmt.Errorf("validate agy MCP config %q: %w", agyPath, err)
		}
	}

	devinPath := devinMCPConfigPath(home)
	configureDevin, err := shouldConfigureDevin(home, devinPath, lookPath)
	if err != nil {
		return fmt.Errorf("inspect Devin MCP config %q: %w", devinPath, err)
	}
	if configureDevin {
		if err := validateJSONMCPConfig(devinPath, true); err != nil {
			return fmt.Errorf("validate Devin MCP config %q: %w", devinPath, err)
		}
	}
	return nil
}

func validateJSONMCPConfig(path string, allowJSONC bool) error {
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if allowJSONC {
		contents, _, err = jsonc.Normalize(contents)
		if err != nil {
			return fmt.Errorf("parse MCP config: %w", err)
		}
	}
	if len(bytes.TrimSpace(contents)) == 0 {
		return nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(contents, &root); err != nil {
		return fmt.Errorf("parse MCP config: %w", err)
	}
	if raw := root["mcpServers"]; len(raw) > 0 {
		var servers map[string]json.RawMessage
		if err := json.Unmarshal(raw, &servers); err != nil {
			return fmt.Errorf("parse MCP servers: %w", err)
		}
		if raw := servers[serverName]; len(raw) > 0 {
			var server map[string]json.RawMessage
			if err := json.Unmarshal(raw, &server); err != nil {
				return fmt.Errorf("parse %s MCP server: %w", serverName, err)
			}
		}
	}
	return nil
}

func shouldConfigureOptionalAgent(path, cli string, lookPath func(string) (string, error)) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	_, err := lookPath(cli)
	return err == nil, nil
}

func ensureClaudeConfig(path, executable string) (bool, bool, error) {
	return ensureJSONMCPServer(path, executable, false, func(server map[string]json.RawMessage) error {
		if err := setJSONField(server, "type", "stdio"); err != nil {
			return err
		}
		if err := setJSONField(server, "command", executable); err != nil {
			return err
		}
		if err := setJSONField(server, "args", []string{serverSubcommand}); err != nil {
			return err
		}
		if err := setJSONField(server, "timeout", toolTimeoutSeconds*1000); err != nil {
			return err
		}
		if _, present := server["env"]; present {
			return nil
		}
		return setJSONField(server, "env", map[string]string{})
	})
}

func ensureAgyConfig(path, executable string) (bool, bool, error) {
	return ensureJSONMCPServer(path, executable, false, func(server map[string]json.RawMessage) error {
		if err := setJSONField(server, "command", executable); err != nil {
			return err
		}
		if err := setJSONField(server, "args", []string{serverSubcommand}); err != nil {
			return err
		}
		// Remove fields that make agy treat the entry as an HTTP server.
		delete(server, "serverUrl")
		delete(server, "url")
		delete(server, "headers")
		// agy mcp enable removes this field. Removing a persisted true value
		// makes the server active without introducing an undocumented schema key.
		delete(server, "disabled")
		return nil
	})
}

func ensureDevinConfig(path, executable string) (bool, bool, error) {
	return ensureJSONMCPServer(path, executable, true, func(server map[string]json.RawMessage) error {
		if err := setJSONField(server, "command", executable); err != nil {
			return err
		}
		if err := setJSONField(server, "args", []string{serverSubcommand}); err != nil {
			return err
		}
		if err := setJSONField(server, "transport", "stdio"); err != nil {
			return err
		}
		// Remove fields that make Devin treat the entry as a remote server or
		// keep it disabled. Installing the server implies enabling it, matching
		// `devin mcp enable waypost`. Per-tool disabledTools entries are a user
		// policy the hook can degrade around (it probes `devin mcp get` and
		// allows the CLI fallback for disabled tools), so they are preserved.
		delete(server, "type")
		delete(server, "url")
		delete(server, "headers")
		delete(server, "disabled")
		return nil
	})
}

type jsonMCPMutator func(map[string]json.RawMessage) error

// ensureJSONMCPServer updates the waypost entry in a JSON MCP config file.
// When allowJSONC is set, existing files may use the JSONC subset Devin
// accepts (//- and /* */-comments, trailing commas); the rewrite then emits
// strict JSON and the second return value reports that the input was JSONC.
func ensureJSONMCPServer(path, executable string, allowJSONC bool, mutate jsonMCPMutator) (bool, bool, error) {
	original, err := os.ReadFile(path)
	mode := os.FileMode(0o600)
	wasJSONC := false
	if err == nil {
		if allowJSONC {
			original, wasJSONC, err = jsonc.Normalize(original)
			if err != nil {
				return false, false, fmt.Errorf("parse MCP config %q: %w", path, err)
			}
		}
		if info, statErr := os.Stat(path); statErr == nil {
			mode = info.Mode().Perm()
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, false, fmt.Errorf("read MCP config %q: %w", path, err)
	}

	root := make(map[string]json.RawMessage)
	if len(bytes.TrimSpace(original)) > 0 {
		if err := json.Unmarshal(original, &root); err != nil {
			return false, false, fmt.Errorf("parse MCP config %q: %w", path, err)
		}
		if root == nil {
			root = make(map[string]json.RawMessage)
		}
	}
	mcpServers := make(map[string]json.RawMessage)
	if raw := root["mcpServers"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &mcpServers); err != nil {
			return false, false, fmt.Errorf("parse MCP servers in %q: %w", path, err)
		}
		if mcpServers == nil {
			mcpServers = make(map[string]json.RawMessage)
		}
	}
	server := make(map[string]json.RawMessage)
	if raw := mcpServers[serverName]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &server); err != nil {
			return false, false, fmt.Errorf("parse %s MCP server in %q: %w", serverName, path, err)
		}
		if server == nil {
			server = make(map[string]json.RawMessage)
		}
	}
	if err := mutate(server); err != nil {
		return false, false, fmt.Errorf("build %s MCP server in %q: %w", serverName, path, err)
	}
	serverJSON, err := json.Marshal(server)
	if err != nil {
		return false, false, fmt.Errorf("encode %s MCP server: %w", serverName, err)
	}
	mcpServers[serverName] = serverJSON
	mcpJSON, err := json.Marshal(mcpServers)
	if err != nil {
		return false, false, fmt.Errorf("encode MCP servers: %w", err)
	}
	root["mcpServers"] = mcpJSON
	updated, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return false, false, fmt.Errorf("encode MCP config: %w", err)
	}
	updated = append(updated, '\n')
	if bytes.Equal(original, updated) {
		return false, wasJSONC, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, false, fmt.Errorf("create MCP config directory %q: %w", filepath.Dir(path), err)
	}
	if err := writeFileAtomically(path, updated, mode); err != nil {
		return false, false, fmt.Errorf("write MCP config %q: %w", path, err)
	}
	return true, wasJSONC, nil
}

func writeFileAtomically(path string, contents []byte, mode os.FileMode) error {
	writePath := path
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return fmt.Errorf("resolve config symlink %q: %w", path, err)
		}
		writePath = resolved
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect config %q: %w", path, err)
	}

	temporary, err := os.CreateTemp(filepath.Dir(writePath), ".waypost-mcp-config-*")
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(mode); err != nil {
		return fmt.Errorf("set temporary config permissions: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		return fmt.Errorf("write temporary config: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary config: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary config: %w", err)
	}
	if err := hookcore.ReplaceFile(temporaryPath, writePath); err != nil {
		return fmt.Errorf("replace config %q: %w", path, err)
	}
	return nil
}

func setJSONField(fields map[string]json.RawMessage, key string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode %s: %w", key, err)
	}
	fields[key] = encoded
	return nil
}

func isMissingCodexMCPError(detail string) bool {
	detail = strings.ToLower(detail)
	singleQuoted := "no mcp server named '" + serverName + "' found"
	doubleQuoted := `no mcp server named "` + serverName + `" found`
	return strings.Contains(detail, singleQuoted) || strings.Contains(detail, doubleQuoted)
}

func commandError(action string, output commandOutput, err error) error {
	detail := strings.TrimSpace(string(output.stderr))
	if detail == "" {
		return fmt.Errorf("%s: %w", action, err)
	}
	runes := []rune(detail)
	if len(runes) > commandFailureLimit {
		detail = string(runes[:commandFailureLimit]) + "…"
	}
	return fmt.Errorf("%s: %w: %s", action, err, detail)
}
