package codexhook

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ruiheng/waypost/internal/hookcore"
)

const mcpProbeTimeout = 4 * time.Second

const waypostMCPServerName = "waypost"

type waypostMCPProbe func(context.Context) (bool, error)

// CurrentDirectoryWaypostMCPAvailable reports whether Waypost is enabled in
// the effective configuration visible to a new Codex process started in the
// caller's current directory. Trusted project configuration may contribute to
// the result. An already-running session's profile or command-line overrides
// may differ because Codex does not expose its live MCP inventory to command
// hooks.
func CurrentDirectoryWaypostMCPAvailable(ctx context.Context) (bool, error) {
	return probeWaypostMCPWithTimeout(ctx, mcpProbeTimeout)
}

func probeWaypostMCPWithTimeout(ctx context.Context, timeout time.Duration) (bool, error) {
	output, missing, err := hookcore.RunMCPGetProbe(ctx, timeout, "codex", isMissingWaypostMCPError, "--json")
	if err != nil || missing {
		return false, err
	}
	return parseWaypostMCPAvailable(output)
}

func isMissingWaypostMCPError(detail string) bool {
	detail = strings.ToLower(detail)
	singleQuoted := "no mcp server named '" + waypostMCPServerName + "' found"
	doubleQuoted := `no mcp server named "` + waypostMCPServerName + `" found`
	return strings.Contains(detail, singleQuoted) || strings.Contains(detail, doubleQuoted)
}

func parseWaypostMCPAvailable(output []byte) (bool, error) {
	var server struct {
		Name    string `json:"name"`
		Enabled bool   `json:"enabled"`
	}
	if err := json.Unmarshal(output, &server); err != nil {
		return false, fmt.Errorf("parse `codex mcp get waypost --json`: %w", err)
	}
	if server.Name != waypostMCPServerName {
		return false, fmt.Errorf("parse `codex mcp get waypost --json`: returned server %q", server.Name)
	}
	return server.Enabled, nil
}
