package claudehook

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ruiheng/waypost/internal/hookcore"
)

const mcpProbeTimeout = 4 * time.Second

const waypostMCPServerName = "waypost"

type waypostMCPProbe func(context.Context) (bool, error)

// CurrentDirectoryWaypostMCPAvailable reports whether Waypost is usable in
// the effective configuration visible to a new Claude Code process started
// in the caller's current directory. User, project, and local scopes all
// contribute to the result. An already-running session's inventory may
// differ because Claude Code does not expose its live MCP inventory to
// command hooks.
func CurrentDirectoryWaypostMCPAvailable(ctx context.Context) (bool, error) {
	return probeWaypostMCPWithTimeout(ctx, mcpProbeTimeout)
}

func probeWaypostMCPWithTimeout(ctx context.Context, timeout time.Duration) (bool, error) {
	output, missing, err := hookcore.RunMCPGetProbe(ctx, timeout, "claude", isMissingWaypostMCPError)
	if err != nil || missing {
		return false, err
	}
	return parseWaypostMCPAvailable(output)
}

// isMissingWaypostMCPError matches the missing-server diagnostic
// `No MCP server named "waypost".` anchored on both the phrase and the
// server name so unrelated stderr is not mistaken for an absent entry.
func isMissingWaypostMCPError(detail string) bool {
	detail = strings.ToLower(detail)
	return strings.Contains(detail, "no mcp server") && strings.Contains(detail, waypostMCPServerName)
}

// parseWaypostMCPAvailable reads the `claude mcp get waypost` report:
//
//	waypost:
//	  Scope: User config (available in all your projects)
//	  Status: ✔ Connected
//	  Type: stdio
//	  Command: /path/to/waypost
//	  Args: mcp
//
// A server entry is usable unless its Status line reports failure, pending
// approval, rejection, missing authentication, or a failed tool listing. A
// missing Status line means the entry is configured and counts as usable.
func parseWaypostMCPAvailable(output []byte) (bool, error) {
	found := false
	available := true
	for _, line := range strings.Split(string(output), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == waypostMCPServerName+":" {
			found = true
			continue
		}
		key, value, ok := strings.Cut(trimmed, ":")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "status") {
			continue
		}
		available = claudeMCPStatusUsable(value)
	}
	if !found {
		if isMissingWaypostMCPError(string(output)) {
			return false, nil
		}
		return false, fmt.Errorf("parse `claude mcp get waypost`: unexpected output")
	}
	return available, nil
}

func claudeMCPStatusUsable(status string) bool {
	value := strings.TrimSpace(strings.ToLower(status))
	switch {
	case value == "":
		return true
	case strings.HasPrefix(value, "✔"), strings.HasPrefix(value, "✓"):
		return true
	case strings.HasPrefix(value, "connected"), strings.HasPrefix(value, "cached"):
		return true
	default:
		return false
	}
}
