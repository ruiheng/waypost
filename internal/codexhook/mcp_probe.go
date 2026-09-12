package codexhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const mcpProbeTimeout = 4 * time.Second

const waypostMCPServerName = "waypost"

type waypostMCPAvailability uint8

const (
	waypostMCPUnknown waypostMCPAvailability = iota
	waypostMCPUnavailable
	waypostMCPAvailable
)

type waypostMCPProbe func(context.Context) (bool, error)

func detectWaypostMCP(ctx context.Context, probe waypostMCPProbe) (waypostMCPAvailability, error) {
	available, err := probe(ctx)
	if err != nil {
		return waypostMCPUnknown, err
	}
	if available {
		return waypostMCPAvailable, nil
	}
	return waypostMCPUnavailable, nil
}

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
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	commandName, commandArgs := mcpProbeInvocation("mcp", "get", waypostMCPServerName, "--json")
	output, err := exec.CommandContext(probeCtx, commandName, commandArgs...).Output()
	if err != nil {
		if probeErr := probeCtx.Err(); probeErr != nil {
			return false, fmt.Errorf("run `codex mcp get waypost --json`: %w", probeErr)
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if isMissingWaypostMCPError(string(exitErr.Stderr)) {
				return false, nil
			}
			if detail := boundedProbeErrorDetail(exitErr.Stderr); detail != "" {
				return false, fmt.Errorf("run `codex mcp get waypost --json`: %w: %s", err, detail)
			}
		}
		return false, fmt.Errorf("run `codex mcp get waypost --json`: %w", err)
	}
	return parseWaypostMCPAvailable(output)
}

func boundedProbeErrorDetail(stderr []byte) string {
	detail := strings.TrimSpace(string(stderr))
	const maxRunes = 500
	runes := []rune(detail)
	if len(runes) > maxRunes {
		const marker = "…"
		headRunes := maxRunes / 2
		tailRunes := maxRunes - headRunes - len([]rune(marker))
		detail = string(runes[:headRunes]) + marker + string(runes[len(runes)-tailRunes:])
	}
	return detail
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
