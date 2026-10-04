package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	thurboxUUIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

	// Source-backed Thurbox v1.7.1 evidence: src/cli/sessions.rs List calls
	// Database::list_active_sessions, while src/session/mod.rs declares
	// HOOK_STATES as working, blocked, done, and idle (or null in JSON). That
	// pinned list grammar has no stopped state we can safely restart. Keep the
	// stopped set explicit (and empty) rather than guessing from tmux or an
	// unrelated field. The restart fixture remains parser-grammar evidence.
	thurboxActiveSessionStatuses = map[string]bool{
		"":        true,
		"working": true,
		"blocked": true,
		"done":    true,
		"idle":    true,
	}
	thurboxStoppedSessionStatuses = map[string]bool{}
)

func detectCurrentThurboxSessionID() (string, []string) {
	sessionID := strings.TrimSpace(os.Getenv("THURBOX_SESSION"))
	if sessionID == "" {
		return "", nil
	}
	if !thurboxUUIDPattern.MatchString(sessionID) {
		return "", []string{"THURBOX_SESSION is set but is not a valid Thurbox v1.7.1 session UUID; ignoring it for auto-bind"}
	}
	return sessionID, nil
}

func thurboxAddress(sessionID string) string {
	return "thurbox/" + sessionID
}

// createThurboxSession runs thurbox session create and parses its receipt. A
// nil result with nil error means the host returned unusable output and the
// caller must report create recovery rather than a generic failure.
func (m *sessionManager) createThurboxSession(ctx context.Context, name, agentKey, parentSessionID, repoPath string) (*hostSessionData, error) {
	result, err := runRedactedCommand(ctx, m.runner, []string{
		"thurbox-cli", "session", "create", "--json", "--name", name,
		"--repo-path", repoPath, "--agent", agentKey, "--parent", parentSessionID,
	}, runOptions{}, "generic thurbox session create")
	if err != nil {
		return nil, err
	}
	created, err := parseThurboxCreatedSession(result.Stdout)
	if err != nil || created == nil || strings.TrimSpace(created.ID) == "" {
		return nil, nil
	}
	return created, nil
}

// resolveThurboxSession follows the v1.7.1 CLI contract: session get for a
// UUID and session list for exact-name resolution. List output is not used as
// a permissive fallback for malformed get output.
func (m *sessionManager) resolveThurboxSession(ctx context.Context, identifier string, timeout time.Duration) (*hostSessionData, error) {
	if thurboxUUIDPattern.MatchString(identifier) {
		result, err := runProbe(ctx, m.runner, []string{"thurbox-cli", "session", "get", "--json", identifier}, runOptions{timeout: timeout}, true)
		if err != nil {
			return nil, err
		}
		if thurboxSessionGetIsMissing(result, identifier) {
			return nil, nil
		}
		if result == nil || result.ExitCode != 0 {
			return nil, thurboxProbeFailure("thurbox v1.7.1 session get", result)
		}
		data, err := parseThurboxSessionRecord(result.Stdout, "thurbox v1.7.1 session get")
		if err != nil {
			return nil, hostSessionOutputFailure(err)
		}
		if data.ID != identifier {
			return nil, hostSessionIdentityFailure(fmt.Errorf("thurbox v1.7.1 session get returned session id %q, want %q", data.ID, identifier))
		}
		return data, nil
	}

	result, err := runProbe(ctx, m.runner, []string{"thurbox-cli", "session", "list", "--json"}, runOptions{timeout: timeout}, true)
	if err != nil {
		return nil, err
	}
	if result == nil || result.ExitCode != 0 {
		return nil, thurboxProbeFailure("thurbox v1.7.1 session list", result)
	}
	sessions, err := parseThurboxSessionList(result.Stdout)
	if err != nil {
		return nil, hostSessionOutputFailure(err)
	}

	var found *hostSessionData
	for index := range sessions {
		candidate := sessions[index]
		if candidate.ID != identifier && candidate.Name != identifier {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("thurbox session reference is ambiguous: %s", identifier)
		}
		found = &candidate
	}
	return found, nil
}

// In pinned Thurbox v1.7.1, sessions::resolve formats a missing UUID as
// "Session not found: <uuid>". src/bin/thurbox-cli.rs adds the "error: "
// prefix and exits 1. No other non-zero get/list result means not found.
func thurboxSessionGetIsMissing(result *RunResult, identifier string) bool {
	return result != nil &&
		result.ExitCode == 1 &&
		strings.TrimSpace(result.Stdout) == "" &&
		strings.TrimSpace(result.Stderr) == "error: Session not found: "+identifier
}

func thurboxProbeFailure(operation string, result *RunResult) error {
	if result == nil {
		return fmt.Errorf("%s returned no result", operation)
	}
	return fmt.Errorf("%s failed with exit code %d", operation, result.ExitCode)
}

func parseThurboxSessionList(text string) ([]hostSessionData, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	var rawSessions []json.RawMessage
	if err := decoder.Decode(&rawSessions); err != nil {
		return nil, fmt.Errorf("thurbox v1.7.1 session list returned invalid JSON: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("thurbox v1.7.1 session list returned invalid JSON: %w", err)
	}

	sessions := make([]hostSessionData, 0, len(rawSessions))
	for _, raw := range rawSessions {
		data, err := parseThurboxSessionRecord(string(raw), "thurbox v1.7.1 session list")
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, *data)
	}
	return sessions, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return fmt.Errorf("contains multiple JSON values")
	}
	return err
}

// parseThurboxSessionRecord accepts precisely the get/list object emitted by
// Thurbox v1.7.1. In particular cwd is the one effective-workdir field; this
// adapter never falls back to repo_path, worktree_path, or another lookalike.
func parseThurboxSessionRecord(text, context string) (*hostSessionData, error) {
	fields, err := parseStrictJSONObject(text, context, []string{
		"id", "name", "agent", "backend_type", "agent_session_id", "cwd",
		"parent_session_id", "display_order", "worktrees", "hook_state",
	})
	if err != nil {
		return nil, err
	}

	id, err := thurboxRequiredString(fields, "id", context)
	if err != nil {
		return nil, err
	}
	if !thurboxUUIDPattern.MatchString(id) {
		return nil, fmt.Errorf("%s returned invalid session id %q", context, id)
	}
	name, err := thurboxRequiredString(fields, "name", context)
	if err != nil {
		return nil, err
	}
	if _, err := thurboxRequiredString(fields, "agent", context); err != nil {
		return nil, err
	}
	if _, err := thurboxRequiredString(fields, "backend_type", context); err != nil {
		return nil, err
	}
	if _, err := thurboxOptionalString(fields, "agent_session_id", context); err != nil {
		return nil, err
	}
	path, err := thurboxOptionalString(fields, "cwd", context)
	if err != nil {
		return nil, err
	}
	parentSessionID, err := thurboxOptionalString(fields, "parent_session_id", context)
	if err != nil {
		return nil, err
	}
	if parentSessionID != "" && !thurboxUUIDPattern.MatchString(parentSessionID) {
		return nil, fmt.Errorf("%s returned invalid parent_session_id %q", context, parentSessionID)
	}
	if err := thurboxOptionalInteger(fields, "display_order", context); err != nil {
		return nil, err
	}
	if err := thurboxWorktrees(fields, context); err != nil {
		return nil, err
	}
	status, err := thurboxOptionalString(fields, "hook_state", context)
	if err != nil {
		return nil, err
	}
	status = strings.TrimSpace(status)
	if !thurboxActiveSessionStatuses[status] && !thurboxStoppedSessionStatuses[status] {
		return nil, fmt.Errorf("%s returned unclassified hook_state %q", context, status)
	}

	return &hostSessionData{
		Host:            sessionHostThurbox,
		ID:              id,
		Name:            name,
		Status:          status,
		Path:            path,
		ParentSessionID: parentSessionID,
	}, nil
}

// parseThurboxCreatedSession accepts precisely the v1.7.1 create object. A
// successful create with any other output is a recovery condition, not a
// generic tool error, because the host may already have created the session.
func parseThurboxCreatedSession(text string) (*hostSessionData, error) {
	const context = "thurbox v1.7.1 session create"
	fields, err := parseStrictJSONObject(text, context, []string{
		"id", "name", "agent", "agent_session_id", "cwd", "parent_session_id",
	})
	if err != nil {
		return nil, err
	}
	id, err := thurboxRequiredString(fields, "id", context)
	if err != nil {
		return nil, err
	}
	if !thurboxUUIDPattern.MatchString(id) {
		return nil, fmt.Errorf("%s returned invalid session id %q", context, id)
	}
	name, err := thurboxRequiredString(fields, "name", context)
	if err != nil {
		return nil, err
	}
	if _, err := thurboxRequiredString(fields, "agent", context); err != nil {
		return nil, err
	}
	if _, err := thurboxOptionalString(fields, "agent_session_id", context); err != nil {
		return nil, err
	}
	path, err := thurboxOptionalString(fields, "cwd", context)
	if err != nil {
		return nil, err
	}
	parentSessionID, err := thurboxOptionalString(fields, "parent_session_id", context)
	if err != nil {
		return nil, err
	}
	if parentSessionID != "" && !thurboxUUIDPattern.MatchString(parentSessionID) {
		return nil, fmt.Errorf("%s returned invalid parent_session_id %q", context, parentSessionID)
	}
	return &hostSessionData{
		Host:            sessionHostThurbox,
		ID:              id,
		Name:            name,
		Path:            path,
		ParentSessionID: parentSessionID,
	}, nil
}

func parseThurboxRestartResult(text, sessionID string) error {
	const context = "thurbox v1.7.1 session restart"
	fields, err := parseStrictJSONObject(text, context, []string{"restarted", "session_id", "session_name"})
	if err != nil {
		return err
	}
	rawRestarted := fields["restarted"]
	var restarted bool
	if err := json.Unmarshal(rawRestarted, &restarted); err != nil || !restarted {
		return fmt.Errorf("%s returned invalid restarted state", context)
	}
	returnedID, err := thurboxRequiredString(fields, "session_id", context)
	if err != nil {
		return err
	}
	if returnedID != sessionID {
		return fmt.Errorf("%s returned session_id %q, want %q", context, returnedID, sessionID)
	}
	if _, err := thurboxRequiredString(fields, "session_name", context); err != nil {
		return err
	}
	return nil
}

func parseStrictJSONObject(text, context string, allowed []string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil {
		return nil, fmt.Errorf("%s returned invalid JSON: %w", context, err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("%s returned invalid JSON: %w", context, err)
	}
	if fields == nil {
		return nil, fmt.Errorf("%s returned a non-object JSON value", context)
	}
	allowedSet := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = true
		if _, ok := fields[key]; !ok {
			return nil, fmt.Errorf("%s returned no %q field", context, key)
		}
	}
	unknown := make([]string, 0)
	for key := range fields {
		if !allowedSet[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("%s returned unknown fields: %s", context, strings.Join(unknown, ", "))
	}
	return fields, nil
}

func thurboxRequiredString(fields map[string]json.RawMessage, key, context string) (string, error) {
	value, err := thurboxOptionalString(fields, key, context)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s returned an empty %q field", context, key)
	}
	return strings.TrimSpace(value), nil
}

func thurboxOptionalString(fields map[string]json.RawMessage, key, context string) (string, error) {
	raw, ok := fields[key]
	if !ok {
		return "", fmt.Errorf("%s returned no %q field", context, key)
	}
	if strings.TrimSpace(string(raw)) == "null" {
		return "", nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s returned invalid %q field: %w", context, key, err)
	}
	return strings.TrimSpace(value), nil
}

func thurboxOptionalInteger(fields map[string]json.RawMessage, key, context string) error {
	raw, ok := fields[key]
	if !ok {
		return fmt.Errorf("%s returned no %q field", context, key)
	}
	if strings.TrimSpace(string(raw)) == "null" {
		return nil
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%s returned invalid %q field: %w", context, key, err)
	}
	return nil
}

func thurboxWorktrees(fields map[string]json.RawMessage, context string) error {
	raw, ok := fields["worktrees"]
	if !ok {
		return fmt.Errorf("%s returned no %q field", context, "worktrees")
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return fmt.Errorf("%s returned invalid %q field: %w", context, "worktrees", err)
	}
	for _, entry := range entries {
		fields, err := parseStrictJSONObject(string(entry), context+" worktree", []string{"repo_path", "worktree_path", "branch"})
		if err != nil {
			return err
		}
		for _, key := range []string{"repo_path", "worktree_path", "branch"} {
			if _, err := thurboxRequiredString(fields, key, context+" worktree"); err != nil {
				return err
			}
		}
	}
	return nil
}

func thurboxSessionNeedsRestart(status string) bool {
	return thurboxStoppedSessionStatuses[strings.ToLower(strings.TrimSpace(status))]
}
