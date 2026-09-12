package mcpinstall

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ruiheng/waypost/internal/devinconfig"
)

func TestEnsureWaypostSectionCreatesConfig(t *testing.T) {
	t.Parallel()

	got, changed, err := ensureWaypostSection("model = \"gpt-5\"\n", "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	want := "model = \"gpt-5\"\n\n[mcp_servers.waypost]\ncommand = \"/opt/waypost\"\nargs = [\"mcp\"]\nrequired = true\nenv_vars = [\"TMUX\", \"AGENTDECK_INSTANCE_ID\", \"CODEX_THREAD_ID\", \"CODEX_SESSION_ID\", \"WAYPOST_STATE_DIR\", \"XDG_STATE_HOME\"]\ntool_timeout_sec = 660\n"
	if !changed {
		t.Fatal("ensureWaypostSection() changed = false, want true")
	}
	if got != want {
		t.Fatalf("ensureWaypostSection() = %q, want %q", got, want)
	}
}

func TestEnsureWaypostSectionPreservesUnrelatedConfigAndIsIdempotent(t *testing.T) {
	t.Parallel()

	contents := `model = "gpt-5"

[mcp_servers.waypost]
command = "/old/waypost"
args = ["mcp"]

[mcp_servers.waypost.tools.waypost_status]
approval_mode = "approve"

[projects."/tmp"]
trust_level = "trusted"
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureWaypostSection() changed = false, want true")
	}
	for _, expected := range []string{
		`model = "gpt-5"`,
		`command = "/opt/waypost"`,
		`args = ["mcp"]`,
		`required = true`,
		`env_vars = ["TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]`,
		`tool_timeout_sec = 660`,
		`[mcp_servers.waypost.tools.waypost_status]`,
		`approval_mode = "approve"`,
		`[projects."/tmp"]`,
	} {
		if !strings.Contains(updated, expected) {
			t.Fatalf("updated config = %q, want %q", updated, expected)
		}
	}
	if _, changed, err := ensureWaypostSection(updated, "/opt/waypost"); err != nil {
		t.Fatalf("ensureWaypostSection() second error = %v", err)
	} else if changed {
		t.Fatal("ensureWaypostSection() second run changed = true, want false")
	}
}

func TestEnsureWaypostSectionEnablesRequiredServer(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers.waypost]
command = "/opt/waypost"
args = ["mcp"]
enabled = false
required = false
env_vars = ["TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]
tool_timeout_sec = 660
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureWaypostSection() changed = false, want true")
	}
	if !strings.Contains(updated, "required = true") || strings.Contains(updated, "required = false") {
		t.Fatalf("updated config = %q, want required=true only", updated)
	}
	if strings.Contains(updated, "enabled = false") {
		t.Fatalf("updated config = %q, want disabled override removed", updated)
	}
}

func TestEnsureWaypostSectionPreservesExtraArguments(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers.waypost]
command = "/old/waypost"
args = ["mcp", "--include-debug-tool"]
required = true
env_vars = ["TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID"]
tool_timeout_sec = 660
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureWaypostSection() changed = false, want true")
	}
	if !strings.Contains(updated, `args = ["mcp", "--include-debug-tool"]`) {
		t.Fatalf("updated config = %q, want extra argument preserved", updated)
	}
}

func TestEnsureWaypostSectionRecognizesHeaderComment(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers.waypost]   # managed by waypost
command = "/opt/waypost"
args = ["mcp"]
required = true
env_vars = ["TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]
tool_timeout_sec = 660
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if changed {
		t.Fatal("ensureWaypostSection() changed = true, want false")
	}
	if strings.Count(updated, "[mcp_servers.waypost]") != 1 {
		t.Fatalf("updated config = %q, want one Waypost section", updated)
	}
}

func TestEnsureWaypostSectionRecognizesQuotedHeader(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers."waypost"] # managed by waypost
command = "/opt/waypost"
args = ["mcp"]
required = true
env_vars = ["TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]
tool_timeout_sec = 660
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if changed {
		t.Fatal("ensureWaypostSection() changed = true, want false")
	}
	if !strings.Contains(updated, `[mcp_servers."waypost"] # managed by waypost`) {
		t.Fatalf("updated config = %q, want quoted Waypost header preserved", updated)
	}
	if strings.Contains(updated, "[mcp_servers.waypost]\n") {
		t.Fatalf("updated config = %q, want no duplicate unquoted Waypost section", updated)
	}
}

func TestEnsureWaypostSectionRecognizesSpacedHeader(t *testing.T) {
	t.Parallel()

	contents := `[ mcp_servers . "waypost" ] # managed by waypost
command = "/opt/waypost"
args = ["mcp"]
required = true
env_vars = ["TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]
tool_timeout_sec = 660
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if changed {
		t.Fatal("ensureWaypostSection() changed = true, want false")
	}
	if !strings.Contains(updated, `[ mcp_servers . "waypost" ] # managed by waypost`) {
		t.Fatalf("updated config = %q, want original spaced header preserved", updated)
	}
	if strings.Count(updated, "mcp_servers") != 1 {
		t.Fatalf("updated config = %q, want one Waypost entry", updated)
	}
}

func TestEnsureWaypostSectionDoesNotAliasQuotedServerWithSpaces(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers."way post"]
command = "/other/server"
args = ["run"]
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureWaypostSection() changed = false, want Waypost section appended")
	}
	if !strings.Contains(updated, `[mcp_servers."way post"]
command = "/other/server"
args = ["run"]`) {
		t.Fatalf("updated config = %q, want unrelated quoted server unchanged", updated)
	}
	if !strings.Contains(updated, "[mcp_servers.waypost]\n") {
		t.Fatalf("updated config = %q, want Waypost section appended", updated)
	}
}

func TestEnsureWaypostSectionRejectsInlineWaypostTable(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers]
waypost = { command = "/old/waypost", args = ["mcp"] }
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost")
	if err == nil || !strings.Contains(err.Error(), "inline mcp_servers.waypost") {
		t.Fatalf("ensureWaypostSection() error = %v, want unsupported inline table error", err)
	}
	if changed || updated != "" {
		t.Fatalf("ensureWaypostSection() = (%q, %v, %v), want no update", updated, changed, err)
	}
}

func TestEnsureWaypostSectionRejectsInlineMCPServersTable(t *testing.T) {
	t.Parallel()

	contents := `mcp_servers = { waypost = { command = "/old/waypost", args = ["mcp"] }, other = { command = "/other" } }
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost")
	if err == nil || !strings.Contains(err.Error(), "inline mcp_servers tables") {
		t.Fatalf("ensureWaypostSection() error = %v, want unsupported inline mcp_servers error", err)
	}
	if changed || updated != "" {
		t.Fatalf("ensureWaypostSection() = (%q, %v, %v), want no update", updated, changed, err)
	}
}

func TestEnsureWaypostSectionRecognizesQuotedAssignmentKeys(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers.waypost]
"command" = "/old/waypost"
"args" = ["mcp"]
"required" = true
"env_vars" = ["TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]
"tool_timeout_sec" = 660
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost", "mcp")
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureWaypostSection() changed = false, want true")
	}
	if strings.Count(updated, "\ncommand =") != 1 || strings.Count(updated, "\nargs =") != 1 {
		t.Fatalf("updated config = %q, want one canonical command and args assignment", updated)
	}
	if strings.Contains(updated, `"command" =`) || strings.Contains(updated, `"args" =`) {
		t.Fatalf("updated config = %q, want quoted assignments normalized", updated)
	}
}

func TestEnsureWaypostSectionRemovesHTTPOnlySettings(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers.waypost]
command = "/opt/waypost"
args = ["mcp"]
url = "https://example.com"
oauth_resource = "example"

[mcp_servers.waypost.oauth]
client_id = "client"

[mcp_servers.waypost.http_headers]
Authorization = "Bearer token"

[mcp_servers.waypost.tools.waypost_status]
approval_mode = "approve"
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureWaypostSection() changed = false, want true")
	}
	for _, removed := range []string{"url =", "oauth_resource =", "[mcp_servers.waypost.oauth]", "client_id =", "[mcp_servers.waypost.http_headers]", "Authorization ="} {
		if strings.Contains(updated, removed) {
			t.Fatalf("updated config = %q, want HTTP-only setting %q removed", updated, removed)
		}
	}
	if !strings.Contains(updated, "[mcp_servers.waypost.tools.waypost_status]") {
		t.Fatalf("updated config = %q, want per-tool settings preserved", updated)
	}
}

func TestEnsureWaypostSectionReplacesMultilineArrays(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers.waypost]
command = "/old/waypost"
args = [
  "mcp",
  "--include-debug-tool",
]
required = false
env_vars = [
  "OLD_ENV",
]
tool_timeout_sec = 10

[mcp_servers.waypost.tools.waypost_status]
approval_mode = "approve"
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost", serverSubcommand)
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureWaypostSection() changed = false, want true")
	}
	for _, expected := range []string{
		`command = "/opt/waypost"`,
		`args = ["mcp"]`,
		`required = true`,
		`env_vars = ["OLD_ENV", "TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]`,
		`tool_timeout_sec = 660`,
		`[mcp_servers.waypost.tools.waypost_status]`,
	} {
		if !strings.Contains(updated, expected) {
			t.Fatalf("updated config = %q, want %q", updated, expected)
		}
	}
	if strings.Contains(updated, "--include-debug-tool") {
		t.Fatalf("updated config = %q, want multiline array continuations removed", updated)
	}
}

func TestEnsureWaypostSectionPreservesCustomEnvVars(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers.waypost]
command = "/opt/waypost"
args = ["mcp"]
required = true
env_vars = ["CUSTOM_ENV", "TMUX"]
tool_timeout_sec = 660
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost", serverSubcommand)
	if err != nil {
		t.Fatalf("ensureWaypostSection() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureWaypostSection() changed = false, want true")
	}
	if !strings.Contains(updated, `env_vars = ["CUSTOM_ENV", "TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]`) {
		t.Fatalf("updated config = %q, want custom and required env vars", updated)
	}
}

func TestEnsureWaypostSectionRejectsUnterminatedMultilineArray(t *testing.T) {
	t.Parallel()

	contents := `[mcp_servers.waypost]
command = "/old/waypost"
args = [
  "mcp",
`
	updated, changed, err := ensureWaypostSection(contents, "/opt/waypost", serverSubcommand)
	if err == nil {
		t.Fatal("ensureWaypostSection() error = nil, want unterminated array error")
	}
	if changed {
		t.Fatalf("ensureWaypostSection() changed = true on parse failure; output = %q", updated)
	}
}

func TestEnsureClaudeConfigPreservesUnrelatedSettings(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, claudeConfigName)
	contents := `{
  "numStartups": 4,
  "mcpServers": {
	    "waypost": {
	      "command": "waypost",
	      "args": ["old"],
	      "timeout": 1000,
	      "env": {"WAYPOST_STATE_DIR": "/tmp/mailbox", "TOKEN": "secret"},
	      "custom": "preserved"
    },
    "other": {"command": "other"}
  }
}
`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(config) error = %v", err)
	}
	changed, _, err := ensureClaudeConfig(path, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureClaudeConfig() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureClaudeConfig() changed = false, want true")
	}
	var config map[string]any
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(config) error = %v", err)
	}
	if err := json.Unmarshal(updated, &config); err != nil {
		t.Fatalf("json.Unmarshal(config) error = %v", err)
	}
	servers := config["mcpServers"].(map[string]any)
	waypost := servers[serverName].(map[string]any)
	if waypost["command"] != "/opt/waypost" || waypost["type"] != "stdio" || waypost["timeout"] != float64(660000) {
		t.Fatalf("waypost config = %#v, want command/type/timeout", waypost)
	}
	if !reflect.DeepEqual(waypost["args"], []any{"mcp"}) || !reflect.DeepEqual(waypost["env"], map[string]any{"WAYPOST_STATE_DIR": "/tmp/mailbox", "TOKEN": "secret"}) {
		t.Fatalf("waypost config = %#v, want args/env", waypost)
	}
	if waypost["custom"] != "preserved" || servers["other"] == nil || config["numStartups"] != float64(4) {
		t.Fatalf("config = %#v, want unrelated settings preserved", config)
	}
	if changed, _, err := ensureClaudeConfig(path, "/opt/waypost"); err != nil {
		t.Fatalf("ensureClaudeConfig() second error = %v", err)
	} else if changed {
		t.Fatal("ensureClaudeConfig() second changed = true, want false")
	}
}

func TestEnsureAgyConfigEnablesServer(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	path := filepath.Join(home, agyMCPConfigName)
	contents := `{
  "mcpServers": {
		"waypost": {
			"command": "waypost",
			"args": ["mcp"],
			"serverUrl": "https://example.com",
			"headers": {"Authorization": "Bearer token"},
			"disabled": true,
      "custom": "preserved"
    },
    "other": {"command": "other"}
	  },
	  "otherSetting": true
	}
`
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll(config) error = %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(config) error = %v", err)
	}
	changed, _, err := ensureAgyConfig(path, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureAgyConfig() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureAgyConfig() changed = false, want true")
	}
	var config map[string]any
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(config) error = %v", err)
	}
	if err := json.Unmarshal(updated, &config); err != nil {
		t.Fatalf("json.Unmarshal(config) error = %v", err)
	}
	servers := config["mcpServers"].(map[string]any)
	waypost := servers[serverName].(map[string]any)
	if _, disabled := waypost["disabled"]; disabled {
		t.Fatalf("waypost config = %#v, want enabled server without disabled field", waypost)
	}
	for _, field := range []string{"serverUrl", "url", "headers"} {
		if _, present := waypost[field]; present {
			t.Fatalf("waypost config = %#v, want HTTP field %q removed", waypost, field)
		}
	}
	if waypost["command"] != "/opt/waypost" || !reflect.DeepEqual(waypost["args"], []any{"mcp"}) || waypost["custom"] != "preserved" || config["otherSetting"] != true {
		t.Fatalf("config = %#v, want enabled Waypost and unrelated settings preserved", config)
	}
	if changed, _, err := ensureAgyConfig(path, "/opt/waypost"); err != nil {
		t.Fatalf("ensureAgyConfig() second error = %v", err)
	} else if changed {
		t.Fatal("ensureAgyConfig() second changed = true, want false")
	}
}

func TestEnsureDevinConfigMergesStdioServerAndIsIdempotent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "mcp_config.json")
	original := `{
  "mcpServers": {
    "other": {"command": "other", "args": ["serve"]},
    "waypost": {
      "command": "/old/waypost",
      "args": ["mcp"],
      "transport": "http",
      "type": "http",
      "url": "http://localhost:9999/mcp",
      "headers": {"Authorization": "Bearer x"},
      "disabled": true,
      "custom": "preserved"
    }
  },
  "otherSetting": true
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(devin config) error = %v", err)
	}
	changed, _, err := ensureDevinConfig(path, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureDevinConfig() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureDevinConfig() changed = false, want true")
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(devin config) error = %v", err)
	}
	var config map[string]any
	if err := json.Unmarshal(updated, &config); err != nil {
		t.Fatalf("json.Unmarshal(config) error = %v", err)
	}
	servers := config["mcpServers"].(map[string]any)
	if other := servers["other"].(map[string]any); other["command"] != "other" || !reflect.DeepEqual(other["args"], []any{"serve"}) {
		t.Fatalf("other server = %#v, want preserved", other)
	}
	waypost := servers["waypost"].(map[string]any)
	if waypost["command"] != "/opt/waypost" || !reflect.DeepEqual(waypost["args"], []any{"mcp"}) || waypost["transport"] != "stdio" || waypost["custom"] != "preserved" || config["otherSetting"] != true {
		t.Fatalf("config = %#v, want stdio Waypost with unrelated settings preserved", config)
	}
	for _, field := range []string{"type", "url", "headers", "disabled"} {
		if _, present := waypost[field]; present {
			t.Fatalf("waypost config = %#v, want remote/disabled field %q removed", waypost, field)
		}
	}
	if changed, _, err := ensureDevinConfig(path, "/opt/waypost"); err != nil {
		t.Fatalf("ensureDevinConfig() second error = %v", err)
	} else if changed {
		t.Fatal("ensureDevinConfig() second changed = true, want false")
	}
}

func TestEnsureDevinConfigPreservesDisabledToolsPolicy(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "mcp_config.json")
	original := `{
  "mcpServers": {
    "waypost": {
      "command": "/old/waypost",
      "disabledTools": ["waypost_recv", "mcp__waypost__waypost_send", "other_tool"]
    }
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(devin config) error = %v", err)
	}
	changed, _, err := ensureDevinConfig(path, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureDevinConfig() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureDevinConfig() changed = false, want true")
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(devin config) error = %v", err)
	}
	var config map[string]any
	if err := json.Unmarshal(updated, &config); err != nil {
		t.Fatalf("json.Unmarshal(config) error = %v", err)
	}
	waypost := config["mcpServers"].(map[string]any)["waypost"].(map[string]any)
	want := []any{"waypost_recv", "mcp__waypost__waypost_send", "other_tool"}
	if tools := waypost["disabledTools"]; !reflect.DeepEqual(tools, want) {
		t.Fatalf("disabledTools = %#v, want user policy preserved %v", tools, want)
	}
}

func TestEnsureDevinConfigAcceptsJSONC(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "mcp_config.json")
	original := `{
  // kept out of the rewrite
  "mcpServers": {
    "waypost": {
      "command": "/old/waypost",
      "args": ["mcp"],
      "transport": "stdio",
    },
  },
}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(devin config) error = %v", err)
	}
	changed, wasJSONC, err := ensureDevinConfig(path, "/opt/waypost")
	if err != nil {
		t.Fatalf("ensureDevinConfig() error = %v", err)
	}
	if !changed {
		t.Fatal("ensureDevinConfig() changed = false, want true")
	}
	if !wasJSONC {
		t.Fatal("ensureDevinConfig() wasJSONC = false, want true")
	}
	updated, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(devin config) error = %v", err)
	}
	if !json.Valid(updated) {
		t.Fatalf("rewritten config is not strict JSON: %s", updated)
	}
	var config map[string]any
	if err := json.Unmarshal(updated, &config); err != nil {
		t.Fatalf("json.Unmarshal(config) error = %v", err)
	}
	waypost := config["mcpServers"].(map[string]any)["waypost"].(map[string]any)
	if waypost["command"] != "/opt/waypost" {
		t.Fatalf("waypost command = %#v, want /opt/waypost", waypost["command"])
	}
}

func TestEnsureDevinConfigRejectsSingleQuotedJSON5(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "mcp_config.json")
	if err := os.WriteFile(path, []byte(`{'mcpServers': {}}`), 0o600); err != nil {
		t.Fatalf("WriteFile(devin config) error = %v", err)
	}
	if _, _, err := ensureDevinConfig(path, "/opt/waypost"); err == nil {
		t.Fatal("ensureDevinConfig() error = nil, want single-quote rejection")
	}
}

func TestInstallOptionalAgentsDetectsDevinByConfigJSON(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, "claude"))
	devinDir := devinconfig.UserConfigDir(home)
	if err := os.MkdirAll(devinDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(devin config dir) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(devinDir, "config.json"), []byte(`{"theme": "dark"}`), 0o600); err != nil {
		t.Fatalf("WriteFile(devin config.json) error = %v", err)
	}
	configured, _, changed, err := installOptionalAgents(home, "/opt/waypost", func() (string, error) { return t.TempDir(), nil }, func(name string) (string, error) {
		return "", errors.New("not installed")
	})
	if err != nil {
		t.Fatalf("installOptionalAgents() error = %v", err)
	}
	if !changed {
		t.Fatal("installOptionalAgents() changed = false, want Devin config written")
	}
	want := []ConfiguredAgent{{Name: "Devin", Path: devinMCPConfigPath(home)}}
	if !reflect.DeepEqual(configured, want) {
		t.Fatalf("configured = %#v, want %v", configured, want)
	}
	contents, err := os.ReadFile(devinMCPConfigPath(home))
	if err != nil {
		t.Fatalf("ReadFile(devin mcp_config.json) error = %v", err)
	}
	var config map[string]any
	if err := json.Unmarshal(contents, &config); err != nil {
		t.Fatalf("Unmarshal(devin mcp_config.json) error = %v", err)
	}
	if _, ok := config["mcpServers"].(map[string]any)["waypost"]; !ok {
		t.Fatalf("mcp_config.json = %#v, want waypost server", config)
	}
}

func TestDevinProjectOverrideWarnings(t *testing.T) {
	t.Parallel()

	workDir := t.TempDir()
	if got := devinProjectOverrideWarnings(workDir); len(got) != 0 {
		t.Fatalf("devinProjectOverrideWarnings() = %v, want none without project config", got)
	}
	devinDir := filepath.Join(workDir, ".devin")
	if err := os.MkdirAll(devinDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(.devin) error = %v", err)
	}
	// A project server with another name is not an override.
	if err := os.WriteFile(filepath.Join(devinDir, "mcp_config.json"), []byte(`{"mcpServers": {"other": {}}}`), 0o600); err != nil {
		t.Fatalf("WriteFile(project mcp_config.json) error = %v", err)
	}
	if got := devinProjectOverrideWarnings(workDir); len(got) != 0 {
		t.Fatalf("devinProjectOverrideWarnings() = %v, want none for unrelated server", got)
	}
	if err := os.WriteFile(filepath.Join(devinDir, "mcp_config.local.json"), []byte(`{"mcpServers": {"waypost": {"command": "/old/waypost"}}}`), 0o600); err != nil {
		t.Fatalf("WriteFile(project mcp_config.local.json) error = %v", err)
	}
	got := devinProjectOverrideWarnings(workDir)
	if len(got) != 1 || !strings.Contains(got[0], "mcp_config.local.json") {
		t.Fatalf("devinProjectOverrideWarnings() = %v, want one local-override warning", got)
	}
	// Malformed project config is ignored rather than failing the install.
	if err := os.WriteFile(filepath.Join(devinDir, "mcp_config.json"), []byte(`{bad`), 0o600); err != nil {
		t.Fatalf("WriteFile(malformed project config) error = %v", err)
	}
	if got := devinProjectOverrideWarnings(workDir); len(got) != 1 {
		t.Fatalf("devinProjectOverrideWarnings() = %v, want malformed config ignored", got)
	}
	if got := devinProjectOverrideWarnings(""); got != nil {
		t.Fatalf("devinProjectOverrideWarnings(\"\") = %v, want nil", got)
	}
}

func TestInstallOptionalAgentsOnlyTouchesInstalledAgents(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	claudePath := filepath.Join(home, claudeConfigName)
	if err := os.WriteFile(claudePath, []byte(`{"mcpServers": {"other": {"command": "other"}}}`), 0o600); err != nil {
		t.Fatalf("WriteFile(Claude config) error = %v", err)
	}
	lookedUp := make([]string, 0, 2)
	configured, _, changed, err := installOptionalAgents(home, "/opt/waypost", func() (string, error) { return t.TempDir(), nil }, func(name string) (string, error) {
		lookedUp = append(lookedUp, name)
		return "", errors.New("not installed")
	})
	if err != nil {
		t.Fatalf("installOptionalAgents() error = %v", err)
	}
	if !reflect.DeepEqual(lookedUp, []string{"agy", "devin"}) {
		t.Fatalf("optional agent lookups = %#v, want agy and devin after existing Claude config", lookedUp)
	}
	if !changed {
		t.Fatal("installOptionalAgents() changed = false, want changed Claude result")
	}
	if !reflect.DeepEqual(configured, []ConfiguredAgent{{Name: "Claude Code", Path: claudePath}}) {
		t.Fatalf("configured = %#v, want Claude Code only", configured)
	}
	if _, err := os.Stat(filepath.Join(home, agyMCPConfigName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("agy config stat error = %v, want absent", err)
	}
	if _, err := os.Stat(devinMCPConfigPath(home)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("devin config stat error = %v, want absent", err)
	}
}

func TestClaudeConfigPathHonorsOverride(t *testing.T) {
	home := t.TempDir()
	configDir := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", configDir)
	path := filepath.Join(configDir, claudeConfigName)
	if err := os.WriteFile(path, []byte(`{"mcpServers": {}}`), 0o600); err != nil {
		t.Fatalf("WriteFile(Claude config) error = %v", err)
	}
	_, _, changed, err := installOptionalAgents(home, "/opt/waypost", func() (string, error) { return t.TempDir(), nil }, func(name string) (string, error) {
		if name == "claude" {
			t.Fatalf("lookPath(%q) called despite custom Claude config", name)
		}
		return "", errors.New("not installed")
	})
	if err != nil {
		t.Fatalf("installOptionalAgents() error = %v", err)
	}
	if !changed {
		t.Fatal("installOptionalAgents() changed = false, want custom Claude config changed")
	}
	if _, err := os.Stat(filepath.Join(home, claudeConfigName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("default Claude config stat error = %v, want absent", err)
	}
}

func TestInstallWithDependenciesAddsMissingServer(t *testing.T) {
	t.Parallel()

	home := filepath.Join(t.TempDir(), "codex")
	var calls [][]string
	deps := dependencies{
		lookPath:          func(string) (string, error) { return "/usr/local/bin/codex", nil },
		resolveHome:       func() (string, error) { return home, nil },
		resolveExecutable: func() (string, error) { return "/opt/waypost", nil },
		run: func(_ context.Context, name string, args ...string) (commandOutput, error) {
			calls = append(calls, append([]string{name}, args...))
			if len(calls) == 1 {
				if _, statErr := os.Stat(home); statErr != nil {
					t.Fatalf("Codex home stat error before probe = %v", statErr)
				}
				return commandOutput{stderr: []byte("Error: No MCP server named 'waypost' found.")}, errors.New("exit status 1")
			}
			return commandOutput{}, nil
		},
	}

	result, err := installWithDependencies(context.Background(), deps)
	if err != nil {
		t.Fatalf("installWithDependencies() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("result.Changed = false, want true")
	}
	wantCalls := [][]string{
		{"/usr/local/bin/codex", "mcp", "get", "waypost", "--json"},
		{"/usr/local/bin/codex", "mcp", "add", "waypost", "--", "/opt/waypost", "mcp"},
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", calls, wantCalls)
	}
	contents, err := os.ReadFile(filepath.Join(home, configFileName))
	if err != nil {
		t.Fatalf("ReadFile(config) error = %v", err)
	}
	if !strings.Contains(string(contents), `[mcp_servers.waypost]`) || !strings.Contains(string(contents), `command = "/opt/waypost"`) || !strings.Contains(string(contents), `required = true`) {
		t.Fatalf("config = %q, want Waypost server", contents)
	}
}

func TestInstallWithDependenciesDoesNotAddAfterUnknownGetError(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	var calls [][]string
	deps := dependencies{
		lookPath:          func(string) (string, error) { return "/usr/local/bin/codex", nil },
		resolveHome:       func() (string, error) { return home, nil },
		resolveExecutable: func() (string, error) { return "/opt/waypost", nil },
		run: func(_ context.Context, name string, args ...string) (commandOutput, error) {
			calls = append(calls, append([]string{name}, args...))
			return commandOutput{stderr: []byte("Error: failed to parse Codex config")}, errors.New("exit status 1")
		},
	}

	_, err := installWithDependencies(context.Background(), deps)
	if err == nil || !strings.Contains(err.Error(), "failed to parse Codex config") {
		t.Fatalf("installWithDependencies() error = %v, want original inspection error", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %#v, want only get", calls)
	}
	if _, statErr := os.Stat(filepath.Join(home, configFileName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Codex config stat error = %v, want config untouched", statErr)
	}
}

func TestInstallWithDependenciesValidatesOptionalConfigsBeforeCodexMutation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	codexHome := filepath.Join(root, "codex")
	userHome := filepath.Join(root, "user")
	if err := os.MkdirAll(userHome, 0o700); err != nil {
		t.Fatalf("MkdirAll(user home) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(userHome, claudeConfigName), []byte(`{"mcpServers":`), 0o600); err != nil {
		t.Fatalf("WriteFile(Claude config) error = %v", err)
	}
	called := false
	deps := dependencies{
		lookPath: func(name string) (string, error) {
			if name == "codex" {
				return "/usr/local/bin/codex", nil
			}
			return "", errors.New("not installed")
		},
		resolveHome:       func() (string, error) { return codexHome, nil },
		resolveUserHome:   func() (string, error) { return userHome, nil },
		resolveExecutable: func() (string, error) { return "/opt/waypost", nil },
		run: func(context.Context, string, ...string) (commandOutput, error) {
			called = true
			return commandOutput{}, errors.New("Codex should not be invoked")
		},
	}
	_, err := installWithDependencies(context.Background(), deps)
	if err == nil || !strings.Contains(err.Error(), "validate Claude Code MCP config") {
		t.Fatalf("installWithDependencies() error = %v, want Claude validation error", err)
	}
	if called {
		t.Fatal("Codex was invoked before optional config validation")
	}
	if _, statErr := os.Stat(codexHome); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Codex home stat error = %v, want no Codex mutation", statErr)
	}
}

func TestInstallWithDependenciesLeavesCorrectServerAndConfigUnchanged(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	contents := `[mcp_servers.waypost]
command = "/opt/waypost"
args = ["mcp"]
required = true
env_vars = ["TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]
tool_timeout_sec = 660
`
	configPath := filepath.Join(home, configFileName)
	if err := os.WriteFile(configPath, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile(config) error = %v", err)
	}
	var calls [][]string
	encoded, err := json.Marshal(map[string]any{
		"name":    "waypost",
		"enabled": true,
		"transport": map[string]any{
			"command": "/opt/waypost",
			"args":    []string{"mcp"},
		},
	})
	if err != nil {
		t.Fatalf("json.Marshal(server) error = %v", err)
	}
	deps := dependencies{
		lookPath:          func(string) (string, error) { return "/usr/local/bin/codex", nil },
		resolveHome:       func() (string, error) { return home, nil },
		resolveExecutable: func() (string, error) { return "/opt/waypost", nil },
		run: func(_ context.Context, name string, args ...string) (commandOutput, error) {
			calls = append(calls, append([]string{name}, args...))
			return commandOutput{stdout: encoded}, nil
		},
	}

	result, err := installWithDependencies(context.Background(), deps)
	if err != nil {
		t.Fatalf("installWithDependencies() error = %v", err)
	}
	if result.Changed {
		t.Fatal("result.Changed = true, want false")
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %#v, want only get", calls)
	}
	after, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("ReadFile(config after) error = %v", err)
	}
	if string(after) != contents {
		t.Fatalf("config after = %q, want unchanged %q", after, contents)
	}
}

func TestInstallWithDependenciesReplacesWrongServer(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	configPath := filepath.Join(home, configFileName)
	if err := os.WriteFile(configPath, []byte(`[mcp_servers.waypost]
command = "/old/waypost"
args = ["mcp", "--include-debug-tool"]

[mcp_servers.waypost.tools.waypost_status]
approval_mode = "approve"
`), 0o600); err != nil {
		t.Fatalf("WriteFile(config) error = %v", err)
	}
	var calls [][]string
	getOutput, err := json.Marshal(map[string]any{
		"name":    "waypost",
		"enabled": true,
		"transport": map[string]any{
			"command": "/old/waypost",
			"args":    []string{"mcp", "--include-debug-tool"},
		},
	})
	if err != nil {
		t.Fatalf("json.Marshal(server) error = %v", err)
	}
	deps := dependencies{
		lookPath:          func(string) (string, error) { return "codex", nil },
		resolveHome:       func() (string, error) { return home, nil },
		resolveExecutable: func() (string, error) { return "/opt/new/waypost", nil },
		run: func(_ context.Context, name string, args ...string) (commandOutput, error) {
			calls = append(calls, append([]string{name}, args...))
			if len(calls) == 1 {
				return commandOutput{stdout: getOutput}, nil
			}
			return commandOutput{}, nil
		},
	}

	result, err := installWithDependencies(context.Background(), deps)
	if err != nil {
		t.Fatalf("installWithDependencies() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("result.Changed = false, want true")
	}
	wantCalls := [][]string{{"codex", "mcp", "get", "waypost", "--json"}}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("calls = %#v, want %#v", calls, wantCalls)
	}
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("ReadFile(config) error = %v", err)
	}
	for _, expected := range []string{`command = "/opt/new/waypost"`, `args = ["mcp", "--include-debug-tool"]`, `required = true`, `[mcp_servers.waypost.tools.waypost_status]`, `approval_mode = "approve"`} {
		if !strings.Contains(string(contents), expected) {
			t.Fatalf("config = %q, want %q", contents, expected)
		}
	}
}

func TestInstallWithDependenciesRequiresADetectedAgent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		resolveUserHome func() (string, error)
	}{
		{name: "without user home"},
		{name: "with user home", resolveUserHome: func() (string, error) { return t.TempDir(), nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			deps := dependencies{
				lookPath: func(string) (string, error) { return "", errors.New("not found") },
				resolveHome: func() (string, error) {
					return t.TempDir(), nil
				},
				resolveUserHome:   tc.resolveUserHome,
				resolveExecutable: func() (string, error) { return "/opt/waypost", nil },
				run: func(context.Context, string, ...string) (commandOutput, error) {
					t.Fatal("run called when no agent is detected")
					return commandOutput{}, nil
				},
			}

			_, err := installWithDependencies(context.Background(), deps)
			if err == nil || !strings.Contains(err.Error(), "no supported agent detected") {
				t.Fatalf("installWithDependencies() error = %v, want no-agent guidance", err)
			}
		})
	}
}

func TestInstallWithDependenciesInstallsDevinWithoutCodex(t *testing.T) {
	t.Parallel()

	codexHome := filepath.Join(t.TempDir(), "codex")
	userHome := t.TempDir()
	deps := dependencies{
		lookPath: func(name string) (string, error) {
			if name == "devin" {
				return "/usr/local/bin/devin", nil
			}
			return "", errors.New("not installed")
		},
		resolveHome:       func() (string, error) { return codexHome, nil },
		resolveUserHome:   func() (string, error) { return userHome, nil },
		resolveExecutable: func() (string, error) { return "/opt/waypost", nil },
		run: func(context.Context, string, ...string) (commandOutput, error) {
			t.Fatal("run called when Codex is unavailable")
			return commandOutput{}, nil
		},
	}

	result, err := installWithDependencies(context.Background(), deps)
	if err != nil {
		t.Fatalf("installWithDependencies() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("result.Changed = false, want true")
	}
	if result.Path != "" {
		t.Fatalf("result.Path = %q, want empty without Codex", result.Path)
	}
	devinPath := devinMCPConfigPath(userHome)
	if !reflect.DeepEqual(result.Configured, []ConfiguredAgent{{Name: "Devin", Path: devinPath}}) {
		t.Fatalf("result.Configured = %#v, want Devin only", result.Configured)
	}
	contents, err := os.ReadFile(devinPath)
	if err != nil {
		t.Fatalf("ReadFile(devin config) error = %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(contents, &document); err != nil {
		t.Fatalf("Unmarshal(devin config) error = %v", err)
	}
	server := document["mcpServers"].(map[string]any)[serverName].(map[string]any)
	if server["command"] != "/opt/waypost" || !reflect.DeepEqual(server["args"], []any{"mcp"}) || server["transport"] != "stdio" {
		t.Fatalf("devin server = %#v, want stdio waypost entry", server)
	}
	if _, statErr := os.Stat(codexHome); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Codex home stat error = %v, want untouched", statErr)
	}
}

func TestInstallWithDependenciesConfiguresExistingCodexConfigWithoutCLI(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	configPath := filepath.Join(home, configFileName)
	original := `[mcp_servers.waypost]
command = "/old/waypost"
args = ["mcp", "--include-debug-tool"]
`
	if err := os.WriteFile(configPath, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile(config) error = %v", err)
	}
	deps := dependencies{
		lookPath:          func(string) (string, error) { return "", errors.New("not installed") },
		resolveHome:       func() (string, error) { return home, nil },
		resolveExecutable: func() (string, error) { return "/opt/new/waypost", nil },
		run: func(context.Context, string, ...string) (commandOutput, error) {
			t.Fatal("run called when Codex CLI is unavailable")
			return commandOutput{}, nil
		},
	}

	result, err := installWithDependencies(context.Background(), deps)
	if err != nil {
		t.Fatalf("installWithDependencies() error = %v", err)
	}
	if !result.Changed || result.Path != configPath {
		t.Fatalf("result = %+v, want changed Codex config %q", result, configPath)
	}
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("ReadFile(config) error = %v", err)
	}
	for _, expected := range []string{`command = "/opt/new/waypost"`, `args = ["mcp", "--include-debug-tool"]`, `required = true`} {
		if !strings.Contains(string(contents), expected) {
			t.Fatalf("config = %q, want %q preserved", contents, expected)
		}
	}
}

func TestInstallOptionalAgentsKeepsConfiguredAgentsOnLaterFailure(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, "claude"))

	// Claude is configured first, then Devin detection fails because its
	// config directory is a regular file (ENOTDIR on stat).
	claudePath := claudeConfigPath(home)
	if err := os.MkdirAll(filepath.Dir(claudePath), 0o700); err != nil {
		t.Fatalf("MkdirAll(claude dir) error = %v", err)
	}
	if err := os.WriteFile(claudePath, []byte(`{"mcpServers": {}}`), 0o600); err != nil {
		t.Fatalf("WriteFile(Claude config) error = %v", err)
	}
	devinBlocker := devinconfig.UserConfigDir(home)
	if err := os.MkdirAll(filepath.Dir(devinBlocker), 0o700); err != nil {
		t.Fatalf("MkdirAll(devin parent) error = %v", err)
	}
	if err := os.WriteFile(devinBlocker, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("WriteFile(devin blocker) error = %v", err)
	}

	configured, _, changed, err := installOptionalAgents(home, "/opt/waypost", func() (string, error) { return t.TempDir(), nil }, func(string) (string, error) {
		return "", errors.New("not installed")
	})
	if err == nil {
		t.Fatal("installOptionalAgents() error = nil, want Devin inspect failure")
	}
	if !changed {
		t.Fatal("installOptionalAgents() changed = false, want Claude change preserved")
	}
	want := []ConfiguredAgent{{Name: "Claude Code", Path: claudePath}}
	if !reflect.DeepEqual(configured, want) {
		t.Fatalf("configured = %#v, want %v", configured, want)
	}
}

func TestInstallWithDependenciesReportsCodexConfiguredOnConfigWriteFailure(t *testing.T) {
	t.Parallel()

	home := t.TempDir()
	// config.toml exists as a directory: detection succeeds, the CLI registers
	// the server, then the direct config write fails.
	if err := os.MkdirAll(filepath.Join(home, configFileName), 0o700); err != nil {
		t.Fatalf("MkdirAll(config.toml) error = %v", err)
	}
	var calls [][]string
	deps := dependencies{
		lookPath:          func(string) (string, error) { return "/usr/local/bin/codex", nil },
		resolveHome:       func() (string, error) { return home, nil },
		resolveExecutable: func() (string, error) { return "/opt/waypost", nil },
		run: func(_ context.Context, name string, args ...string) (commandOutput, error) {
			calls = append(calls, append([]string{name}, args...))
			if len(calls) == 1 {
				return commandOutput{stderr: []byte("Error: No MCP server named 'waypost' found.")}, errors.New("exit status 1")
			}
			return commandOutput{}, nil
		},
	}

	result, err := installWithDependencies(context.Background(), deps)
	if err == nil || !strings.Contains(err.Error(), "configured for Codex") {
		t.Fatalf("installWithDependencies() error = %v, want Codex partial-result error", err)
	}
	if !result.Changed {
		t.Fatal("result.Changed = false, want true (codex mcp add succeeded)")
	}
	want := []ConfiguredAgent{{Name: "Codex", Path: filepath.Join(home, configFileName)}}
	if !reflect.DeepEqual(result.Configured, want) {
		t.Fatalf("result.Configured = %#v, want %v", result.Configured, want)
	}
}

func TestInstallWithDependenciesWarnsWhenGetwdFails(t *testing.T) {
	home := t.TempDir()
	userHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(userHome, "xdg"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(userHome, "claude"))

	// Devin is detected through an existing user-level config.json.
	devinDir := devinconfig.UserConfigDir(userHome)
	if err := os.MkdirAll(devinDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(devin dir) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(devinDir, "config.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatalf("WriteFile(devin config.json) error = %v", err)
	}

	deps := dependencies{
		lookPath:          func(string) (string, error) { return "", errors.New("not installed") },
		resolveHome:       func() (string, error) { return home, nil },
		resolveUserHome:   func() (string, error) { return userHome, nil },
		resolveExecutable: func() (string, error) { return "/opt/waypost", nil },
		getwd:             func() (string, error) { return "", errors.New("getwd: no such directory") },
		run: func(context.Context, string, ...string) (commandOutput, error) {
			return commandOutput{}, errors.New("no codex CLI")
		},
	}

	result, err := installWithDependencies(context.Background(), deps)
	if err != nil {
		t.Fatalf("installWithDependencies() error = %v", err)
	}
	found := false
	for _, warning := range result.Warnings {
		if strings.Contains(warning, "override check skipped") {
			found = true
		}
	}
	if !found {
		t.Fatalf("result.Warnings = %v, want project-override check warning", result.Warnings)
	}
}

func TestInstallWithDependenciesSkipsGetwdWarningWithoutDevin(t *testing.T) {
	home := t.TempDir()
	userHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(userHome, "xdg"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(userHome, "claude"))

	// Only Codex is detected (via an existing config.toml); no Devin
	// artifacts exist, so a getwd failure must not surface a Devin warning.
	configPath := filepath.Join(home, configFileName)
	if err := os.WriteFile(configPath, []byte("[mcp_servers.waypost]\n"), 0o600); err != nil {
		t.Fatalf("WriteFile(config) error = %v", err)
	}

	deps := dependencies{
		lookPath:          func(string) (string, error) { return "", errors.New("not installed") },
		resolveHome:       func() (string, error) { return home, nil },
		resolveUserHome:   func() (string, error) { return userHome, nil },
		resolveExecutable: func() (string, error) { return "/opt/waypost", nil },
		getwd:             func() (string, error) { return "", errors.New("getwd: no such directory") },
		run: func(context.Context, string, ...string) (commandOutput, error) {
			return commandOutput{}, errors.New("no codex CLI")
		},
	}

	result, err := installWithDependencies(context.Background(), deps)
	if err != nil {
		t.Fatalf("installWithDependencies() error = %v", err)
	}
	for _, warning := range result.Warnings {
		if strings.Contains(warning, "override check skipped") {
			t.Fatalf("result.Warnings = %v, want no Devin override warning without Devin", result.Warnings)
		}
	}
}
