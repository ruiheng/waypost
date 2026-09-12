package mcpserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *Service) waypostDebug(_ context.Context, _ *mcp.CallToolRequest, _ waypostDebugInput) (*mcp.CallToolResult, map[string]any, error) {
	return nil, s.sessions.debugState(), nil
}

func (m *sessionManager) debugState() map[string]any {
	snapshot := m.snapshotState()
	cwd, err := os.Getwd()
	process := map[string]any{
		"pid":  os.Getpid(),
		"ppid": os.Getppid(),
	}
	if err == nil {
		process["cwd"] = cwd
	} else {
		process["cwd_error"] = err.Error()
	}

	return map[string]any{
		"status":              "debug",
		"process":             process,
		"debug_env":           currentDebugEnvDiagnostics(),
		"tool_session_env":    currentToolSessionEnvDiagnostics(),
		"session_state":       debugSessionState(snapshot),
		"parent_process_env":  parentProcessEnvDiagnostics(os.Getppid()),
		"privacy_constraints": "Only allowlisted debug environment variables are inspected; the full environment is not exposed.",
	}
}

func debugSessionState(snapshot stateSnapshot) map[string]any {
	out := map[string]any{
		"bound_addresses":                  snapshot.BoundAddresses,
		"default_sender":                   snapshot.DefaultSender,
		"default_workdir":                  snapshot.DefaultWorkdir,
		"status_tool_called":               snapshot.StatusToolCalled,
		"auto_bind_attempted":              snapshot.AutoBindAttempted,
		"auto_bind_empty_result":           snapshot.AutoBindEmptyResult,
		"auto_bound_tool_fallback":         snapshot.AutoBoundToolFallback,
		"auto_bind_warnings":               snapshot.AutoBindWarnings,
		"detected_agent_deck_session_id":   snapshot.DetectedAgentDeckSession,
		"detected_thurbox_session_id":      snapshot.DetectedThurboxSession,
		"detected_tool_session_addresses":  detectedToolSessionAddresses(snapshot),
		"bound_tool_session_addresses":     boundToolSessionAddresses(snapshot.BoundAddresses),
		"bound_agent_deck_session_address": boundAddressesByScheme(snapshot.BoundAddresses, "agent-deck"),
		"bound_thurbox_session_address":    boundAddressesByScheme(snapshot.BoundAddresses, "thurbox"),
	}
	for key, value := range detectedToolSessionOutputFields(snapshot.DetectedToolSessions, func(value string) any { return value }) {
		out[key] = value
	}
	return out
}

func currentToolSessionEnvDiagnostics() map[string]any {
	lookup := func(name string) (string, bool) {
		return os.LookupEnv(name)
	}
	return toolSessionEnvDiagnostics(lookup)
}

func currentDebugEnvDiagnostics() map[string]any {
	lookup := func(name string) (string, bool) {
		return os.LookupEnv(name)
	}
	return debugEnvDiagnostics(lookup)
}

func debugEnvDiagnostics(lookup func(string) (string, bool)) map[string]any {
	out := make(map[string]any, len(debugEnvNames()))
	for _, name := range debugEnvNames() {
		value, ok := lookup(name)
		if isToolSessionEnvName(name) {
			out[name] = toolSessionEnvDiagnostic(name, value, ok)
			continue
		}
		out[name] = envDiagnostic(value, ok)
	}
	return out
}

func toolSessionEnvDiagnostics(lookup func(string) (string, bool)) map[string]any {
	out := make(map[string]any, len(toolSessionEnvNames()))
	for _, name := range toolSessionEnvNames() {
		value, ok := lookup(name)
		out[name] = toolSessionEnvDiagnostic(name, value, ok)
	}
	return out
}

func debugEnvNames() []string {
	names := append([]string(nil), toolSessionEnvNames()...)
	return append(names, "AGENTDECK_INSTANCE_ID", "THURBOX_SESSION", "TMUX")
}

func isToolSessionEnvName(name string) bool {
	for _, candidate := range toolSessionEnvNames() {
		if name == candidate {
			return true
		}
	}
	return false
}

func envDiagnostic(value string, present bool) map[string]any {
	diagnostic := map[string]any{
		"present":       present,
		"trimmed_empty": present && strings.TrimSpace(value) == "",
	}
	if present {
		diagnostic["value"] = value
	}
	return diagnostic
}

func toolSessionEnvDiagnostic(name, value string, present bool) map[string]any {
	trimmed := strings.TrimSpace(value)
	diagnostic := map[string]any{
		"present":                present,
		"trimmed_empty":          present && trimmed == "",
		"accepted_by_validation": false,
	}
	if !present {
		diagnostic["failure_reason"] = "not set"
		return diagnostic
	}
	diagnostic["value"] = value
	if failure := toolSessionValidationFailureForEnv(name, value); failure != "" {
		diagnostic["failure_reason"] = failure
		return diagnostic
	}
	diagnostic["accepted_by_validation"] = true
	diagnostic["address"] = toolSessionAddressForEnv(name, trimmed)
	return diagnostic
}

func toolSessionAddressForEnv(name, sessionID string) string {
	for _, descriptor := range toolSessionDescriptors {
		if name == descriptor.Env {
			return toolSessionAddress(descriptor.Scheme, sessionID)
		}
	}
	return ""
}

func parentProcessEnvDiagnostics(startPID int) map[string]any {
	out := map[string]any{
		"available": false,
		"chain":     []any{},
	}
	if runtime.GOOS != "linux" {
		out["error"] = "parent process environment inspection is only supported on linux"
		return out
	}
	if startPID <= 1 {
		out["error"] = "no parent process to inspect"
		return out
	}

	chain := make([]any, 0, 8)
	seen := map[int]bool{}
	for pid := startPID; pid > 1 && !seen[pid] && len(chain) < 8; {
		seen[pid] = true
		entry, ppid, err := procProcessEnvEntry(pid)
		if err != nil {
			chain = append(chain, map[string]any{
				"pid":   pid,
				"error": err.Error(),
			})
			break
		}
		chain = append(chain, entry)
		pid = ppid
	}
	out["available"] = len(chain) > 0
	out["chain"] = chain
	return out
}

func procProcessEnvEntry(pid int) (map[string]any, int, error) {
	ppid, comm, err := procProcessStat(pid)
	if err != nil {
		return nil, 0, err
	}
	values, err := readProcEnviron(pid)
	if err != nil {
		return nil, 0, err
	}
	return map[string]any{
		"pid":  pid,
		"ppid": ppid,
		"comm": comm,
		"debug_env": debugEnvDiagnostics(func(name string) (string, bool) {
			value, ok := values[name]
			return value, ok
		}),
		"tool_session_env": toolSessionEnvDiagnostics(func(name string) (string, bool) {
			value, ok := values[name]
			return value, ok
		}),
	}, ppid, nil
}

func procProcessStat(pid int) (int, string, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, "", err
	}
	stat := string(data)
	open := strings.IndexByte(stat, '(')
	close := strings.LastIndexByte(stat, ')')
	if open < 0 || close <= open || close+2 >= len(stat) {
		return 0, "", fmt.Errorf("invalid /proc stat format for pid %d", pid)
	}
	fields := strings.Fields(stat[close+2:])
	if len(fields) < 2 {
		return 0, "", fmt.Errorf("invalid /proc stat fields for pid %d", pid)
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, "", fmt.Errorf("invalid parent pid in /proc stat for pid %d: %w", pid, err)
	}
	return ppid, stat[open+1 : close], nil
}

func readProcEnviron(pid int) (map[string]string, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "environ"))
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, item := range strings.Split(string(data), "\x00") {
		if item == "" {
			continue
		}
		name, value, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		for _, allowed := range debugEnvNames() {
			if name == allowed {
				values[name] = value
				break
			}
		}
	}
	return values, nil
}
