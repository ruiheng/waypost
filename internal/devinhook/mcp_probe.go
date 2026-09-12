package devinhook

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"
)

const mcpProbeTimeout = 4 * time.Second

const waypostMCPServerName = "waypost"

// waypostMCPStatus reports what a new Devin process would see for the Waypost
// MCP server: whether the server itself is usable and which of its tools are
// hidden through Devin's disabledTools setting.
type waypostMCPStatus struct {
	available     bool
	disabledTools map[string]bool
}

func (status waypostMCPStatus) toolUsable(tool string) bool {
	return status.available && !status.disabledTools[tool]
}

// WaypostMCPStatus is the exported form of waypostMCPStatus for doctor and
// other callers that need tool-level detail.
type WaypostMCPStatus struct {
	Available     bool
	DisabledTools []string
}

func (status waypostMCPStatus) exported() WaypostMCPStatus {
	disabled := make([]string, 0, len(status.disabledTools))
	for tool := range status.disabledTools {
		disabled = append(disabled, tool)
	}
	sort.Strings(disabled)
	return WaypostMCPStatus{Available: status.available, DisabledTools: disabled}
}

type waypostMCPProbe func(context.Context) (waypostMCPStatus, error)

// CurrentDirectoryWaypostMCPAvailable reports whether Waypost is enabled in
// the effective configuration visible to a new Devin process started in the
// caller's current directory. Imported configurations from other agents may
// contribute to the result. An already-running session's effective MCP
// inventory may differ because Devin does not expose it to command hooks.
func CurrentDirectoryWaypostMCPAvailable(ctx context.Context) (bool, error) {
	status, err := probeWaypostMCPWithTimeout(ctx, mcpProbeTimeout)
	return status.available, err
}

// CurrentDirectoryWaypostMCPStatus reports server- and tool-level availability
// for the same probe as CurrentDirectoryWaypostMCPAvailable.
func CurrentDirectoryWaypostMCPStatus(ctx context.Context) (WaypostMCPStatus, error) {
	status, err := probeWaypostMCPWithTimeout(ctx, mcpProbeTimeout)
	return status.exported(), err
}

func probeWaypostMCPWithTimeout(ctx context.Context, timeout time.Duration) (waypostMCPStatus, error) {
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	commandName, commandArgs := mcpProbeInvocation("mcp", "get", waypostMCPServerName)
	output, err := exec.CommandContext(probeCtx, commandName, commandArgs...).Output()
	if err != nil {
		if probeErr := probeCtx.Err(); probeErr != nil {
			return waypostMCPStatus{}, fmt.Errorf("run `devin mcp get waypost`: %w", probeErr)
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if isMissingWaypostMCPError(string(exitErr.Stderr)) {
				return waypostMCPStatus{}, nil
			}
			if detail := boundedProbeErrorDetail(exitErr.Stderr); detail != "" {
				return waypostMCPStatus{}, fmt.Errorf("run `devin mcp get waypost`: %w: %s", err, detail)
			}
		}
		return waypostMCPStatus{}, fmt.Errorf("run `devin mcp get waypost`: %w", err)
	}
	return parseWaypostMCPStatus(output)
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
	return strings.Contains(detail, "server '"+waypostMCPServerName+"' not found") ||
		strings.Contains(detail, `server "`+waypostMCPServerName+`" not found`)
}

var (
	disabledStatusPattern = regexp.MustCompile(`(?i)^status:\s*disabled\b`)
	disabledFlagPattern   = regexp.MustCompile(`(?i)disabled["']?\s*:\s*true\b`)
	enabledFlagPattern    = regexp.MustCompile(`(?i)enabled["']?\s*:\s*false\b`)
	// Devin prints `Disabled tools: waypost_recv, ...` for entries listed in
	// the server's disabledTools configuration array.
	disabledToolsPattern = regexp.MustCompile(`(?i)^disabled tools?:\s*(.+?)\s*$`)
)

func parseWaypostMCPStatus(output []byte) (waypostMCPStatus, error) {
	found := false
	status := waypostMCPStatus{disabledTools: map[string]bool{}}
	for _, line := range strings.Split(string(output), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "Server: "+waypostMCPServerName {
			found = true
			continue
		}
		if disabledStatusPattern.MatchString(trimmed) ||
			disabledFlagPattern.MatchString(trimmed) ||
			enabledFlagPattern.MatchString(trimmed) {
			return waypostMCPStatus{}, nil
		}
		if match := disabledToolsPattern.FindStringSubmatch(trimmed); match != nil {
			for _, name := range strings.Split(match[1], ",") {
				if name = canonicalWaypostToolName(strings.TrimSpace(name)); name != "" {
					status.disabledTools[name] = true
				}
			}
		}
	}
	if !found {
		return waypostMCPStatus{}, fmt.Errorf("parse `devin mcp get waypost`: unexpected output")
	}
	status.available = true
	return status, nil
}

// canonicalWaypostToolName maps Devin's namespaced MCP tool id
// (mcp__waypost__<tool>) to the bare tool name used for availability checks.
// Entries for other servers are left untouched.
func canonicalWaypostToolName(name string) string {
	if rest, ok := strings.CutPrefix(name, "mcp__waypost__"); ok && rest != "" {
		return rest
	}
	return name
}
