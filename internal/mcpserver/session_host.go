package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// sessionHost is deliberately a closed set. Adding another host is an
// explicit product change, not a runtime registration mechanism.
type sessionHost string

const (
	sessionHostAgentDeck sessionHost = "agent-deck"
	sessionHostThurbox   sessionHost = "thurbox"

	genericSessionCreateOutputUnparseableDetail = "generic session create returned unusable output"
)

var genericSessionNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

type hostSessionData struct {
	Host            sessionHost
	ID              string
	Name            string
	Status          string
	Group           string
	Path            string
	ParentSessionID string
}

type hostWorkdirVerification struct {
	State        string
	ObservedPath string
	Err          error
}

type createdHostSessionExpectation struct {
	Name                string
	ParentSessionID     string
	VerifyGroup         bool
	Group               string
	GroupMismatchDetail string
	RequestedWorkdir    string
	CanonicalWorkdir    string
}

type createdHostSessionVerification struct {
	State        string
	ObservedPath string
	Detail       string
	UseRefreshed bool
}

type hostSessionOutputFailureKind string

const (
	hostSessionOutputGrammarFailure  hostSessionOutputFailureKind = "grammar"
	hostSessionOutputIdentityFailure hostSessionOutputFailureKind = "identity"
)

// hostSessionOutputError distinguishes a host's parseable command execution
// from output that no longer matches the approved session grammar. Recovery
// after a confirmed start must not classify this through error text.
type hostSessionOutputError struct {
	cause error
	kind  hostSessionOutputFailureKind
}

func (e *hostSessionOutputError) Error() string {
	return e.cause.Error()
}

func (e *hostSessionOutputError) Unwrap() error {
	return e.cause
}

func hostSessionOutputFailure(err error) error {
	if err == nil {
		return nil
	}
	return &hostSessionOutputError{cause: err, kind: hostSessionOutputGrammarFailure}
}

func hostSessionIdentityFailure(err error) error {
	if err == nil {
		return nil
	}
	return &hostSessionOutputError{cause: err, kind: hostSessionOutputIdentityFailure}
}

func isHostSessionOutputFailure(err error) bool {
	var outputErr *hostSessionOutputError
	return errors.As(err, &outputErr)
}

func isHostSessionIdentityFailure(err error) bool {
	var outputErr *hostSessionOutputError
	return errors.As(err, &outputErr) && outputErr.kind == hostSessionOutputIdentityFailure
}

func parseSessionHost(value string) (sessionHost, error) {
	switch sessionHost(value) {
	case sessionHostAgentDeck:
		return sessionHostAgentDeck, nil
	case sessionHostThurbox:
		return sessionHostThurbox, nil
	default:
		return "", fmt.Errorf("unsupported session host %q; supported hosts are agent-deck and thurbox", value)
	}
}

// selectSessionHost intentionally treats a valid Thurbox environment as the
// immediate host. A nested Thurbox session commonly retains an outer Agent
// Deck identity, which is context rather than an ambiguity.
func (m *sessionManager) selectSessionHost(ctx context.Context, override string) (sessionHost, error) {
	if override != "" {
		return parseSessionHost(override)
	}
	if sessionID, _ := detectCurrentThurboxSessionID(); sessionID != "" {
		return sessionHostThurbox, nil
	}

	snapshot := m.snapshotState()
	if strings.TrimSpace(snapshot.DetectedAgentDeckSession) != "" {
		return sessionHostAgentDeck, nil
	}
	agentDeckSessionID, _, _, _, err := m.detectCurrentAgentDeckSessionID(ctx, snapshot.DetectedToolSessions["codex"], snapshot.DefaultWorkdir)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(agentDeckSessionID) != "" {
		return sessionHostAgentDeck, nil
	}
	return "", errors.New("session host is unknown; set host explicitly")
}

func (m *sessionManager) resolveHostSession(ctx context.Context, host sessionHost, identifier string, timeout time.Duration) (*hostSessionData, error) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return nil, errors.New("session identifier is required")
	}

	switch host {
	case sessionHostAgentDeck:
		data, err := m.resolveSessionShow(ctx, identifier, timeout)
		if err != nil || data == nil {
			return nil, err
		}
		hostData := hostSessionFromAgentDeck(data)
		if hostData.ID == "" {
			return nil, hostSessionOutputFailure(errors.New("agent-deck session show returned a session without an id"))
		}
		return hostData, nil
	case sessionHostThurbox:
		return m.resolveThurboxSession(ctx, identifier, timeout)
	default:
		return nil, fmt.Errorf("unsupported session host %q", host)
	}
}

func validateGenericSessionName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("session_name is required when creating a target session")
	}
	if !genericSessionNamePattern.MatchString(name) {
		return "", errors.New("session_name must use letters, digits, dot, underscore, or hyphen")
	}
	return name, nil
}

func selectedHostLaunchValue(host sessionHost, fullCommandLine, thurboxAgentKey string) (string, error) {
	switch host {
	case sessionHostAgentDeck:
		fullCommandLine = strings.TrimSpace(fullCommandLine)
		if fullCommandLine == "" {
			return "", errors.New("full_command_line is required when creating an agent-deck session")
		}
		return fullCommandLine, nil
	case sessionHostThurbox:
		thurboxAgentKey = strings.TrimSpace(thurboxAgentKey)
		if thurboxAgentKey == "" {
			return "", errors.New("thurbox_agent_key is required when creating a thurbox session")
		}
		return thurboxAgentKey, nil
	default:
		return "", fmt.Errorf("unsupported session host %q", host)
	}
}

func (m *sessionManager) createHostSession(ctx context.Context, host sessionHost, name, workdir, parentSessionID, fullCommandLine, thurboxAgentKey string, switches agentDeckSwitches) (map[string]any, error) {
	name, err := validateGenericSessionName(name)
	if err != nil {
		return nil, err
	}
	launchValue, err := selectedHostLaunchValue(host, fullCommandLine, thurboxAgentKey)
	if err != nil {
		return nil, err
	}
	canonicalWorkdir, err := canonicalizeTargetWorkdir(workdir, "creating")
	if err != nil {
		return nil, err
	}
	parentSessionID = strings.TrimSpace(parentSessionID)
	if parentSessionID == "" {
		return nil, errors.New("parent_session_id is required when creating a target session")
	}
	if strings.Contains(parentSessionID, "/") {
		return nil, errors.New("parent_session_id must be a same-host session ID, not an address")
	}

	parent, err := m.resolveHostSession(ctx, host, parentSessionID, ensureSessionShowTimeout)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		return nil, fmt.Errorf("parent_session_id not found: %s", parentSessionID)
	}
	if parent.ID != parentSessionID {
		return nil, fmt.Errorf("parent_session_id must be a same-host session ID, not a session name: %s", parentSessionID)
	}
	parentGroupSnapshot := ""
	if host == sessionHostAgentDeck {
		if parent.ParentSessionID != "" {
			return nil, errors.New(genericAgentDeckNestedParentDetail)
		}
		parentGroupSnapshot = strings.TrimSpace(parent.Group)
		if parentGroupSnapshot == "" {
			return nil, errors.New(genericAgentDeckEmptyParentGroupDetail)
		}
	}

	existing, err := m.resolveHostSession(ctx, host, name, ensureSessionShowTimeout)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if err := validateHostSessionWorkdir(existing, workdir, canonicalWorkdir); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("target session already exists: %s", name)
	}

	var created *hostSessionData
	switch host {
	case sessionHostAgentDeck:
		created, err = m.launchAgentDeckSession(ctx, name, launchValue, parentGroupSnapshot, parentSessionID, canonicalWorkdir, switches)
	case sessionHostThurbox:
		created, err = m.createThurboxSession(ctx, name, launchValue, parentSessionID, canonicalWorkdir)
	default:
		return nil, fmt.Errorf("unsupported session host %q", host)
	}
	if err != nil {
		return nil, err
	}
	if created == nil {
		return createRecoveryResult(host, name, parentSessionID, canonicalWorkdir), nil
	}

	refreshed, err := m.resolveHostSession(ctx, host, created.ID, ensureSessionShowTimeout)
	if err != nil {
		state := "post_create_lookup_failed"
		if isHostSessionIdentityFailure(err) {
			state = "post_create_identity_mismatch"
		}
		return createdUnverifiedResult(created, name, canonicalWorkdir, state, "", err.Error()), nil
	}
	if refreshed == nil {
		return createdUnverifiedResult(created, name, canonicalWorkdir, "post_create_lookup_failed", "", "target session not found after create"), nil
	}
	verification := verifyCreatedHostSession(host, created, refreshed, createdHostSessionExpectation{
		Name:                name,
		ParentSessionID:     parentSessionID,
		VerifyGroup:         host == sessionHostAgentDeck,
		Group:               parentGroupSnapshot,
		GroupMismatchDetail: genericAgentDeckGroupMismatchDetail,
		RequestedWorkdir:    workdir,
		CanonicalWorkdir:    canonicalWorkdir,
	})
	if verification.State != "verified" {
		resultData := created
		if verification.UseRefreshed {
			resultData = refreshed
		}
		return createdUnverifiedResult(resultData, name, canonicalWorkdir, verification.State, verification.ObservedPath, verification.Detail), nil
	}
	return createdVerifiedResult(refreshed, name, canonicalWorkdir), nil
}

func verifyCreatedHostSession(host sessionHost, created, refreshed *hostSessionData, expected createdHostSessionExpectation) createdHostSessionVerification {
	if err := verifyCreatedHostSessionIdentity(host, created, refreshed, expected.Name, expected.ParentSessionID); err != nil {
		useRefreshed := created != nil && refreshed != nil && strings.TrimSpace(created.ID) == strings.TrimSpace(refreshed.ID)
		observedPath := ""
		if useRefreshed {
			observedPath = refreshed.Path
		}
		return createdHostSessionVerification{
			State:        "post_create_identity_mismatch",
			ObservedPath: observedPath,
			Detail:       err.Error(),
			UseRefreshed: useRefreshed,
		}
	}
	if expected.VerifyGroup && strings.TrimSpace(refreshed.Group) != expected.Group {
		return createdHostSessionVerification{
			State:        "post_create_group_mismatch",
			ObservedPath: refreshed.Path,
			Detail:       expected.GroupMismatchDetail,
			UseRefreshed: true,
		}
	}
	workdir := verifyHostSessionWorkdir(refreshed, expected.RequestedWorkdir, expected.CanonicalWorkdir)
	if workdir.State != "verified" {
		return createdHostSessionVerification{
			State:        workdir.State,
			ObservedPath: workdir.ObservedPath,
			Detail:       workdir.Err.Error(),
			UseRefreshed: true,
		}
	}
	return createdHostSessionVerification{
		State:        "verified",
		ObservedPath: refreshed.Path,
		UseRefreshed: true,
	}
}

func verifyCreatedHostSessionIdentity(host sessionHost, created, refreshed *hostSessionData, requestedName, requestedParentSessionID string) error {
	if created == nil || refreshed == nil {
		return errors.New("target session identity unavailable after create")
	}
	if created.ID != refreshed.ID {
		return fmt.Errorf("created session id %q does not match refreshed session id %q", created.ID, refreshed.ID)
	}
	switch host {
	case sessionHostAgentDeck:
		// Agent Deck's launch output is a receipt: only its session ID is needed
		// to retrieve the authoritative post-create session record below.
	case sessionHostThurbox:
		if created.Name != requestedName {
			return fmt.Errorf("created session name %q does not match requested name %q", created.Name, requestedName)
		}
		if created.ParentSessionID != requestedParentSessionID {
			return fmt.Errorf("created session parent %q does not match requested parent %q", created.ParentSessionID, requestedParentSessionID)
		}
	default:
		return fmt.Errorf("unsupported session host %q", host)
	}
	if refreshed.Name != requestedName {
		return fmt.Errorf("refreshed session name %q does not match requested name %q", refreshed.Name, requestedName)
	}
	if refreshed.ParentSessionID != requestedParentSessionID {
		return fmt.Errorf("refreshed session parent %q does not match requested parent %q", refreshed.ParentSessionID, requestedParentSessionID)
	}
	return nil
}

type hostSessionSelectorKind string

const (
	hostSessionSelectorID  hostSessionSelectorKind = "session_id"
	hostSessionSelectorRef hostSessionSelectorKind = "session_ref"
)

func (m *sessionManager) requireHostSession(ctx context.Context, host sessionHost, identifier, workdir string, selectorKind hostSessionSelectorKind, autoRestart bool) (map[string]any, error) {
	canonicalWorkdir, err := canonicalizeTargetWorkdir(workdir, "requiring")
	if err != nil {
		return nil, err
	}
	return m.requireHostSessionWithCanonicalWorkdir(ctx, host, identifier, workdir, canonicalWorkdir, selectorKind, autoRestart)
}

func (m *sessionManager) requireHostSessionWithCanonicalWorkdir(ctx context.Context, host sessionHost, identifier, requestedWorkdir, canonicalWorkdir string, selectorKind hostSessionSelectorKind, autoRestart bool) (map[string]any, error) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return nil, errors.New("session identifier is required when requiring a target session")
	}
	data, err := m.resolveHostSession(ctx, host, identifier, ensureSessionShowTimeout)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return notFoundHostSessionResult(host, identifier), nil
	}
	if selectorKind == hostSessionSelectorID && data.ID != identifier {
		return nil, fmt.Errorf("session_id must exactly match the resolved host session ID: %s", identifier)
	}
	if err := validateHostSessionWorkdir(data, requestedWorkdir, canonicalWorkdir); err != nil {
		return nil, err
	}
	if hostSessionIsReady(host, data) {
		return readyVerifiedResult(data, identifier, canonicalWorkdir, false), nil
	}
	if !autoRestart {
		return notReadyVerifiedResult(data, identifier, canonicalWorkdir), nil
	}

	switch host {
	case sessionHostAgentDeck:
		if _, err := runCommand(ctx, m.runner, []string{"agent-deck", "session", "start", "--json", data.ID}, runOptions{}); err != nil {
			return nil, err
		}
	case sessionHostThurbox:
		if !thurboxSessionNeedsRestart(data.Status) {
			return nil, fmt.Errorf("thurbox session status %q is not restartable", data.Status)
		}
		result, err := runCommand(ctx, m.runner, []string{"thurbox-cli", "session", "restart", "--json", data.ID}, runOptions{})
		if err != nil {
			return nil, err
		}
		if err := parseThurboxRestartResult(result.Stdout, data.ID); err != nil {
			return readyUnverifiedResult(data, identifier, canonicalWorkdir, "post_start_output_unparseable", "", err.Error()), nil
		}
	default:
		return nil, fmt.Errorf("unsupported session host %q", host)
	}

	return m.reverifyStartedHostSession(ctx, host, data, identifier, requestedWorkdir, canonicalWorkdir)
}

func (m *sessionManager) reverifyStartedHostSession(ctx context.Context, host sessionHost, previous *hostSessionData, sessionRef, requestedWorkdir, canonicalWorkdir string) (map[string]any, error) {
	refreshed, err := m.resolveHostSession(ctx, host, previous.ID, ensureSessionShowTimeout)
	if err != nil {
		state := "post_start_lookup_failed"
		if isHostSessionOutputFailure(err) {
			state = "post_start_output_unparseable"
		}
		return readyUnverifiedResult(previous, sessionRef, canonicalWorkdir, state, "", err.Error()), nil
	}
	if refreshed == nil {
		return readyUnverifiedResult(previous, sessionRef, canonicalWorkdir, "post_start_disappeared", "", "target session not found after start"), nil
	}
	if refreshed.ID != previous.ID {
		return readyUnverifiedResult(previous, sessionRef, canonicalWorkdir, "post_start_output_unparseable", "", fmt.Sprintf("refreshed session id %q does not match started session id %q", refreshed.ID, previous.ID)), nil
	}
	if !hostSessionIsReady(host, refreshed) {
		return readyUnverifiedResult(previous, sessionRef, canonicalWorkdir, "post_start_not_ready", refreshed.Path, "target session is not ready after start"), nil
	}
	verification := verifyHostSessionWorkdir(refreshed, requestedWorkdir, canonicalWorkdir)
	if verification.State != "verified" {
		return readyUnverifiedResult(previous, sessionRef, canonicalWorkdir, postStartVerificationState(verification.State), verification.ObservedPath, verification.Err.Error()), nil
	}
	return readyVerifiedResult(refreshed, sessionRef, canonicalWorkdir, true), nil
}

func postStartVerificationState(state string) string {
	switch state {
	case "path_mismatch":
		return "post_start_path_mismatch"
	case "path_unavailable":
		return "post_start_path_unavailable"
	default:
		return "post_start_lookup_failed"
	}
}

func hostSessionIsReady(host sessionHost, data *hostSessionData) bool {
	if data == nil {
		return false
	}
	status := strings.ToLower(strings.TrimSpace(data.Status))
	switch host {
	case sessionHostAgentDeck:
		return activeSessionStatuses[status]
	case sessionHostThurbox:
		return thurboxActiveSessionStatuses[status]
	default:
		return false
	}
}

func verifyHostSessionWorkdir(data *hostSessionData, requestedWorkdir, canonicalWorkdir string) hostWorkdirVerification {
	if data == nil || strings.TrimSpace(data.Path) == "" {
		return hostWorkdirVerification{
			State: "path_unavailable",
			Err:   errors.New("existing session path unavailable: cannot verify workdir match"),
		}
	}
	canonicalExistingPath, err := canonicalizeExistingPath(data.Path)
	if err != nil {
		return hostWorkdirVerification{
			State:        "path_unavailable",
			ObservedPath: data.Path,
			Err:          fmt.Errorf("canonicalize existing session path %q: %w", data.Path, err),
		}
	}
	if canonicalExistingPath != canonicalWorkdir {
		return hostWorkdirVerification{
			State:        "path_mismatch",
			ObservedPath: data.Path,
			Err:          fmt.Errorf("session path mismatch: existing='%s' expected='%s'", data.Path, requestedWorkdir),
		}
	}
	return hostWorkdirVerification{State: "verified", ObservedPath: data.Path}
}

func validateHostSessionWorkdir(data *hostSessionData, requestedWorkdir, canonicalWorkdir string) error {
	verification := verifyHostSessionWorkdir(data, requestedWorkdir, canonicalWorkdir)
	return verification.Err
}

func createdVerifiedResult(data *hostSessionData, sessionRef, canonicalWorkdir string) map[string]any {
	out := hostSessionInfoMap(data, sessionRef)
	out["status"] = "created"
	out["created_target"] = true
	out["started_session"] = true
	out["recovery_required"] = false
	out["verification"] = verificationMap("verified", canonicalWorkdir, data.Path, "")
	return out
}

func createdUnverifiedResult(data *hostSessionData, sessionRef, canonicalWorkdir, state, observedPath, detail string) map[string]any {
	out := hostSessionInfoMap(data, sessionRef)
	out["status"] = "created_unverified"
	out["created_target"] = true
	out["started_session"] = true
	out["recovery_required"] = true
	out["verification"] = verificationMap(state, canonicalWorkdir, observedPath, detail)
	return out
}

func createRecoveryResult(host sessionHost, sessionRef, parentSessionID, canonicalWorkdir string) map[string]any {
	return map[string]any{
		"host":              string(host),
		"status":            "create_recovery_required",
		"created_target":    nil,
		"started_session":   nil,
		"recovery_required": true,
		"verification":      verificationMap("create_output_unparseable", canonicalWorkdir, "", genericSessionCreateOutputUnparseableDetail),
		"session_id":        nil,
		"session_ref":       sessionRef,
		"session_name":      sessionRef,
		"session_status":    nil,
		"path":              nil,
		"parent_session_id": nilIfEmpty(parentSessionID),
		"addresses":         []string{},
	}
}

func readyVerifiedResult(data *hostSessionData, sessionRef, canonicalWorkdir string, started bool) map[string]any {
	out := hostSessionInfoMap(data, sessionRef)
	out["status"] = "ready"
	out["created_target"] = false
	out["started_session"] = started
	out["recovery_required"] = false
	out["verification"] = verificationMap("verified", canonicalWorkdir, data.Path, "")
	return out
}

func notReadyVerifiedResult(data *hostSessionData, sessionRef, canonicalWorkdir string) map[string]any {
	out := hostSessionInfoMap(data, sessionRef)
	out["status"] = "not_ready"
	out["created_target"] = false
	out["started_session"] = false
	out["recovery_required"] = false
	out["verification"] = verificationMap("verified", canonicalWorkdir, data.Path, "")
	return out
}

func notFoundHostSessionResult(host sessionHost, sessionRef string) map[string]any {
	out := hostSessionInfoMap(nil, sessionRef)
	out["host"] = string(host)
	out["status"] = "not_found"
	out["created_target"] = false
	out["started_session"] = false
	out["recovery_required"] = false
	return out
}

func readyUnverifiedResult(previous *hostSessionData, sessionRef, canonicalWorkdir, state, observedPath, detail string) map[string]any {
	out := hostSessionInfoMap(previous, sessionRef)
	out["status"] = "ready_unverified"
	out["created_target"] = false
	out["started_session"] = true
	out["recovery_required"] = true
	out["verification"] = verificationMap(state, canonicalWorkdir, observedPath, detail)
	return out
}

func verificationMap(state, requestedWorkdir, observedPath, detail string) map[string]any {
	out := map[string]any{
		"state":             state,
		"requested_workdir": nilIfEmpty(requestedWorkdir),
		"observed_path":     nilIfEmpty(observedPath),
	}
	if strings.TrimSpace(detail) != "" {
		out["error"] = detail
	}
	return out
}

func hostSessionInfoMap(data *hostSessionData, sessionRef string) map[string]any {
	addresses := []string{}
	if data != nil && strings.TrimSpace(data.ID) != "" {
		addresses = append(addresses, hostSessionAddress(data.Host, data.ID))
	}
	if data == nil {
		return map[string]any{
			"session_id":        nil,
			"session_ref":       sessionRef,
			"session_name":      nil,
			"session_status":    nil,
			"path":              nil,
			"parent_session_id": nil,
			"addresses":         addresses,
		}
	}
	return map[string]any{
		"host":              string(data.Host),
		"session_id":        data.ID,
		"session_ref":       firstNonEmpty(sessionRef, data.Name, data.ID),
		"session_name":      nilIfEmpty(data.Name),
		"session_status":    nilIfEmpty(data.Status),
		"path":              nilIfEmpty(data.Path),
		"parent_session_id": nilIfEmpty(data.ParentSessionID),
		"addresses":         addresses,
	}
}

func hostSessionAddress(host sessionHost, sessionID string) string {
	switch host {
	case sessionHostAgentDeck:
		return agentDeckAddress(sessionID)
	case sessionHostThurbox:
		return thurboxAddress(sessionID)
	default:
		return ""
	}
}
