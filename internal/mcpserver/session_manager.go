package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ruiheng/waypost/internal/waypost"
)

var (
	activeSessionStatuses = map[string]bool{
		"running": true,
		"waiting": true,
		"idle":    true,
	}
	codexResumePattern      = regexp.MustCompile(`\bresume\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b`)
	codexSessionFilePattern = regexp.MustCompile(`/\.codex/sessions/.*-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$`)
	codexCommandPattern     = regexp.MustCompile(`(^|/)codex(\s|$)`)
	devinResumePattern      = regexp.MustCompile(`(?:^|\s)(?:--resume[= ]+|-r[= ]+)([A-Za-z0-9][A-Za-z0-9._-]*)`)
	devinSessionLockPattern = regexp.MustCompile(`/devin/cli/session_locks/([A-Za-z0-9][A-Za-z0-9._-]*)\.lock`)
	devinCommandPattern     = regexp.MustCompile(`(^|/)devin(\s|$)`)
	devinSessionIDPattern   = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$`)
	toolSessionIDPattern    = regexp.MustCompile(`^[0-9a-fA-F][0-9a-fA-F-]*[0-9a-fA-F]$|^[0-9a-fA-F]$`)
)

type serverState struct {
	mu                       sync.Mutex
	boundAddresses           []string
	defaultSender            string
	defaultWorkdir           string
	manualBinding            bool
	autoBindAttempted        bool
	autoBindEmptyResult      bool
	autoBoundToolFallback    bool
	autoBindWarnings         []string
	detectedAgentDeckSession string
	detectedThurboxSession   string
	detectedToolSessions     toolSessionIDs
	statusToolCalled         bool
}

type stateSnapshot struct {
	BoundAddresses           []string
	DefaultSender            string
	DefaultWorkdir           string
	ManualBinding            bool
	AutoBindAttempted        bool
	AutoBindEmptyResult      bool
	AutoBoundToolFallback    bool
	AutoBindWarnings         []string
	DetectedAgentDeckSession string
	DetectedThurboxSession   string
	DetectedToolSessions     toolSessionIDs
	StatusToolCalled         bool
}

type boundState struct {
	BoundAddresses               []string       `json:"bound_addresses"`
	DefaultSender                string         `json:"default_sender"`
	DefaultWorkdir               string         `json:"default_workdir"`
	DetectedAgentDeckSession     string         `json:"detected_agent_deck_session_id"`
	DetectedThurboxSession       string         `json:"detected_thurbox_session_id"`
	DetectedToolSessions         toolSessionIDs `json:"-"`
	DetectedToolSessionAddresses []string       `json:"detected_tool_session_addresses"`
	Warnings                     []string       `json:"warnings"`
}

type toolSessionDescriptor struct {
	Scheme        string
	Env           string
	StatusJSONKey string
	Validate      func(string) string
	InvalidHint   string
}

type toolSessionIDs map[string]string

var toolSessionDescriptors = []toolSessionDescriptor{
	{Scheme: "codex", Env: "CODEX_THREAD_ID", StatusJSONKey: "detected_agent_session_id"},
	{Scheme: "claude", Env: "CLAUDE_CODE_SESSION_ID", StatusJSONKey: "detected_claude_code_session_id"},
	{Scheme: "gemini", Env: "GEMINI_SESSION_ID", StatusJSONKey: "detected_gemini_session_id"},
	{Scheme: "opencode", Env: "OPENCODE_SESSION_ID", StatusJSONKey: "detected_opencode_session_id"},
	{Scheme: "devin", Env: "DEVIN_SESSION_ID", StatusJSONKey: "detected_devin_session_id", Validate: devinSessionIDValidationFailure, InvalidHint: "a devin session id"},
}

func (d toolSessionDescriptor) validationFailure(sessionID string) string {
	if d.Validate != nil {
		return d.Validate(sessionID)
	}
	return toolSessionIDValidationFailure(sessionID)
}

func (d toolSessionDescriptor) invalidHint() string {
	if d.InvalidHint != "" {
		return d.InvalidHint
	}
	return "a hex session id"
}

type sessionData struct {
	ID              string `json:"id"`
	Title           string `json:"title"`
	Status          string `json:"status"`
	Group           string `json:"group"`
	Path            string `json:"path"`
	ParentSessionID string `json:"parent_session_id,omitempty"`
	Success         *bool  `json:"success,omitempty"`
}

type psRow struct {
	PID  int
	PPID int
	Comm string
	Args string
}

type sessionManager struct {
	runner    Runner
	state     *serverState
	parentPID func() int
}

type sessionShowProbeStatus string

const (
	sessionShowProbeFound    sessionShowProbeStatus = "found"
	sessionShowProbeNotFound sessionShowProbeStatus = "not_found"
	sessionShowProbeUnknown  sessionShowProbeStatus = "unknown"
)

type sessionShowProbeResult struct {
	Status sessionShowProbeStatus
	Data   *sessionData
}

type sessionStartVerification struct {
	State        string
	ObservedPath string
	Detail       string
}

type sessionStartResult struct {
	Data                     *sessionData
	StartedSession           bool
	NotifyNeeded             bool
	StartupInstructionStatus string
	Verification             *sessionStartVerification
}

func newSessionManager(runner Runner, state *serverState) *sessionManager {
	return &sessionManager{
		runner:    runner,
		state:     state,
		parentPID: os.Getppid,
	}
}

func (m *sessionManager) bind(ctx context.Context, input waypostBindInput) (boundState, error) {
	if err := validateMCPItems("addresses", len(input.Addresses)); err != nil {
		return boundState{}, err
	}
	boundAddresses, err := waypost.NormalizeAddressList(input.Addresses)
	if err != nil {
		return boundState{}, err
	}
	boundAddresses = personalAddressesOnly(boundAddresses)
	defaultSender := strings.TrimSpace(input.DefaultSender)
	if defaultSender == "" && len(boundAddresses) > 0 {
		defaultSender = boundAddresses[0]
	}
	if defaultSender != "" {
		defaultSender, err = waypost.NormalizeAddress(defaultSender)
		if err != nil {
			return boundState{}, fmt.Errorf("invalid default_sender: %w", err)
		}
		if waypost.IsGroupAddress(defaultSender) {
			return boundState{}, errors.New("default_sender cannot be a group address")
		}
		if !slices.Contains(boundAddresses, defaultSender) {
			return boundState{}, fmt.Errorf("default_sender %q must be one of the bound addresses", defaultSender)
		}
	}

	m.state.mu.Lock()
	m.state.boundAddresses = boundAddresses
	m.state.defaultSender = defaultSender
	m.state.defaultWorkdir = strings.TrimSpace(input.DefaultWorkdir)
	m.state.manualBinding = true
	m.state.autoBindAttempted = true
	m.state.autoBindEmptyResult = false
	m.state.autoBoundToolFallback = false
	m.state.autoBindWarnings = nil
	m.state.detectedAgentDeckSession = ""
	m.state.detectedThurboxSession = ""
	m.state.detectedToolSessions = nil
	m.state.mu.Unlock()

	return m.boundState(ctx)
}

func personalAddressesOnly(addresses []string) []string {
	out := addresses[:0]
	for _, address := range addresses {
		if waypost.IsGroupAddress(address) {
			continue
		}
		out = append(out, address)
	}
	return out
}

func (m *sessionManager) boundState(ctx context.Context) (boundState, error) {
	if err := m.tryAutoBindCurrentSession(ctx); err != nil {
		return boundState{}, err
	}
	snapshot := m.snapshotState()

	warnings := make([]string, 0, 3)
	warnings = append(warnings, snapshot.AutoBindWarnings...)
	hasAgentDeckBinding := snapshot.DetectedAgentDeckSession != "" || len(boundAddressesByScheme(snapshot.BoundAddresses, "agent-deck")) > 0
	hasThurboxBinding := snapshot.DetectedThurboxSession != "" || len(boundAddressesByScheme(snapshot.BoundAddresses, "thurbox")) > 0
	if !hasAgentDeckBinding && !hasThurboxBinding {
		warnings = append(warnings, agentDeckBindRecoveryHint)
	}
	toolAddresses := detectedToolSessionAddresses(snapshot)
	if !hasThurboxBinding && len(toolAddresses) == 0 && len(boundToolSessionAddresses(snapshot.BoundAddresses)) == 0 {
		warnings = append(warnings, toolSessionBindRecoveryHint)
	}
	if len(snapshot.BoundAddresses) == 0 {
		warnings = append(warnings, "no waypost addresses are currently bound")
	}

	return boundState{
		BoundAddresses:               snapshot.BoundAddresses,
		DefaultSender:                snapshot.DefaultSender,
		DefaultWorkdir:               snapshot.DefaultWorkdir,
		DetectedAgentDeckSession:     snapshot.DetectedAgentDeckSession,
		DetectedThurboxSession:       snapshot.DetectedThurboxSession,
		DetectedToolSessions:         snapshot.DetectedToolSessions.clone(),
		DetectedToolSessionAddresses: toolAddresses,
		Warnings:                     warnings,
	}, nil
}

func (m *sessionManager) snapshotState() stateSnapshot {
	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	return stateSnapshot{
		BoundAddresses:           append([]string(nil), m.state.boundAddresses...),
		DefaultSender:            m.state.defaultSender,
		DefaultWorkdir:           m.state.defaultWorkdir,
		ManualBinding:            m.state.manualBinding,
		AutoBindAttempted:        m.state.autoBindAttempted,
		AutoBindEmptyResult:      m.state.autoBindEmptyResult,
		AutoBoundToolFallback:    m.state.autoBoundToolFallback,
		AutoBindWarnings:         append([]string(nil), m.state.autoBindWarnings...),
		DetectedAgentDeckSession: m.state.detectedAgentDeckSession,
		DetectedThurboxSession:   m.state.detectedThurboxSession,
		DetectedToolSessions:     m.state.detectedToolSessions.clone(),
		StatusToolCalled:         m.state.statusToolCalled,
	}
}

func (m *sessionManager) tryAutoBindCurrentSession(ctx context.Context) error {
	snapshot := m.snapshotState()
	// waypost_bind is an explicit operator choice. Discovery must never append
	// Agent Deck or Thurbox addresses to that user-supplied binding later.
	if snapshot.ManualBinding {
		return nil
	}

	thurboxSessionID, thurboxWarnings := detectCurrentThurboxSessionID()
	if len(snapshot.BoundAddresses) > 0 {
		if len(thurboxWarnings) > 0 {
			m.state.mu.Lock()
			if !m.state.manualBinding {
				m.state.autoBindWarnings = dedupe(append(m.state.autoBindWarnings, thurboxWarnings...))
			}
			m.state.mu.Unlock()
		}
		if thurboxSessionID != "" && snapshot.DetectedThurboxSession != thurboxSessionID {
			m.state.mu.Lock()
			if !m.state.manualBinding && len(m.state.boundAddresses) > 0 {
				m.state.boundAddresses = dedupe(append([]string{thurboxAddress(thurboxSessionID)}, m.state.boundAddresses...))
				m.state.detectedThurboxSession = thurboxSessionID
				m.state.defaultSender = thurboxAddress(thurboxSessionID)
				m.state.autoBindWarnings = dedupe(append(m.state.autoBindWarnings, thurboxWarnings...))
			}
			m.state.mu.Unlock()
			snapshot = m.snapshotState()
		}
		if snapshot.AutoBoundToolFallback && snapshot.DetectedAgentDeckSession == "" && len(detectedToolSessionAddresses(snapshot)) > 0 {
			return m.tryUpgradeAgentDeckBinding(ctx, snapshot)
		}
		if snapshot.DetectedAgentDeckSession != "" && snapshot.DetectedToolSessions["codex"] == "" {
			return m.tryCompleteCodexBindingFromAgentDeckDB(ctx, snapshot)
		}
		return nil
	}
	if snapshot.AutoBindAttempted && !snapshot.AutoBindEmptyResult {
		return nil
	}

	detectedToolSessions, autoBindWarnings := m.detectCurrentToolSessionIDs(ctx)
	codexSessionID := detectedToolSessions["codex"]
	autoBindWarnings = append(autoBindWarnings, thurboxWarnings...)

	defaultWorkdir := snapshot.DefaultWorkdir
	agentDeckSessionID, defaultWorkdir, probeCompleted, agentDeckWarnings, err := m.detectCurrentAgentDeckSessionID(ctx, codexSessionID, defaultWorkdir)
	if err != nil {
		// A nested Thurbox session is usable even when an outer Agent Deck probe
		// is broken. Preserve that probe only as a diagnostic warning.
		if thurboxSessionID == "" {
			return err
		}
		autoBindWarnings = append(autoBindWarnings, fmt.Sprintf("agent-deck auto-bind probe failed while Thurbox was detected: %v", err))
		agentDeckSessionID = ""
		probeCompleted = false
	}
	autoBindWarnings = append(autoBindWarnings, agentDeckWarnings...)
	if agentDeckSessionID != "" {
		data, err := m.resolveSessionShowBestEffort(ctx, agentDeckSessionID)
		if err != nil {
			autoBindWarnings = append(autoBindWarnings, fmt.Sprintf("agent-deck session show returned invalid data during auto-bind: %v", err))
		} else if data != nil && strings.TrimSpace(data.Path) != "" {
			defaultWorkdir = strings.TrimSpace(data.Path)
		}
	}
	if codexSessionID == "" && agentDeckSessionID != "" {
		match, lookupWarnings := lookupAgentDeckSessionByWorkdir(ctx, firstNonEmpty(defaultWorkdir, currentWorkingDir()), agentDeckSessionID)
		autoBindWarnings = append(autoBindWarnings, lookupWarnings...)
		if match != nil {
			codexSessionID = match.CodexSessionID
			detectedToolSessions["codex"] = codexSessionID
			if strings.TrimSpace(match.ProjectPath) != "" && defaultWorkdir == "" {
				defaultWorkdir = strings.TrimSpace(match.ProjectPath)
			}
		}
	}

	addresses := make([]string, 0, 6)
	if thurboxSessionID != "" {
		// The immediate nested host is first, which also defines the automatic
		// default sender. An outer Agent Deck address remains bound for durable
		// queue accounting and compatibility.
		addresses = append(addresses, thurboxAddress(thurboxSessionID))
	}
	detectedAgentDeckSession := ""
	if agentDeckSessionID != "" {
		detectedAgentDeckSession = agentDeckSessionID
		addresses = append(addresses, agentDeckAddress(agentDeckSessionID))
	}

	addresses = append(addresses, detectedToolSessions.addresses()...)

	if !probeCompleted && len(addresses) == 0 && len(autoBindWarnings) == 0 {
		return nil
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	if len(m.state.boundAddresses) > 0 {
		return nil
	}
	m.state.boundAddresses = dedupe(addresses)
	m.state.detectedAgentDeckSession = detectedAgentDeckSession
	m.state.detectedThurboxSession = thurboxSessionID
	m.state.detectedToolSessions = detectedToolSessions.clone()
	m.state.defaultWorkdir = defaultWorkdir
	m.state.manualBinding = false
	m.state.autoBoundToolFallback = detectedAgentDeckSession == "" && thurboxSessionID == "" && len(addresses) > 0
	m.state.autoBindEmptyResult = len(addresses) == 0
	m.state.autoBindWarnings = append([]string(nil), autoBindWarnings...)
	if thurboxSessionID != "" {
		m.state.defaultSender = thurboxAddress(thurboxSessionID)
	} else if detectedAgentDeckSession != "" {
		m.state.defaultSender = agentDeckAddress(detectedAgentDeckSession)
	} else if len(addresses) > 0 {
		m.state.defaultSender = addresses[0]
	}
	m.state.autoBindAttempted = true
	return nil
}

func (m *sessionManager) tryCompleteCodexBindingFromAgentDeckDB(ctx context.Context, snapshot stateSnapshot) error {
	match, lookupWarnings := lookupAgentDeckSessionByWorkdir(ctx, firstNonEmpty(snapshot.DefaultWorkdir, currentWorkingDir()), snapshot.DetectedAgentDeckSession)
	if match == nil || strings.TrimSpace(match.CodexSessionID) == "" {
		if len(lookupWarnings) > 0 {
			m.state.mu.Lock()
			if m.state.detectedAgentDeckSession == snapshot.DetectedAgentDeckSession &&
				m.state.detectedToolSessions["codex"] == "" &&
				slices.Contains(m.state.boundAddresses, agentDeckAddress(snapshot.DetectedAgentDeckSession)) {
				m.state.autoBindWarnings = dedupe(append(m.state.autoBindWarnings, lookupWarnings...))
			}
			m.state.mu.Unlock()
		}
		return nil
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	if m.state.detectedAgentDeckSession != snapshot.DetectedAgentDeckSession ||
		m.state.detectedToolSessions["codex"] != "" ||
		!slices.Contains(m.state.boundAddresses, agentDeckAddress(snapshot.DetectedAgentDeckSession)) {
		return nil
	}
	m.state.boundAddresses = dedupe(append(m.state.boundAddresses, toolSessionAddress("codex", match.CodexSessionID)))
	if m.state.detectedToolSessions == nil {
		m.state.detectedToolSessions = toolSessionIDs{}
	}
	m.state.detectedToolSessions["codex"] = match.CodexSessionID
	m.state.autoBindEmptyResult = false
	m.state.autoBindWarnings = dedupe(append(append(toolSessionEnvWarnings(), lookupWarnings...), m.state.autoBindWarnings...))
	if strings.TrimSpace(match.ProjectPath) != "" && m.state.defaultWorkdir == "" {
		m.state.defaultWorkdir = strings.TrimSpace(match.ProjectPath)
	}
	return nil
}

func (m *sessionManager) tryUpgradeAgentDeckBinding(ctx context.Context, snapshot stateSnapshot) error {
	defaultWorkdir := snapshot.DefaultWorkdir
	agentDeckSessionID, defaultWorkdir, _, autoBindWarnings, err := m.detectCurrentAgentDeckSessionID(ctx, snapshot.DetectedToolSessions["codex"], defaultWorkdir)
	if err != nil {
		return err
	}
	autoBindWarnings = append(toolSessionEnvWarnings(), autoBindWarnings...)
	if agentDeckSessionID == "" {
		m.state.mu.Lock()
		if m.state.autoBoundToolFallback && m.state.detectedAgentDeckSession == "" && currentToolFallbackMatchesSnapshot(m.state, snapshot) {
			m.state.autoBindWarnings = append([]string(nil), autoBindWarnings...)
		}
		m.state.mu.Unlock()
		return nil
	}

	if data, err := m.resolveSessionShowBestEffort(ctx, agentDeckSessionID); err != nil {
		autoBindWarnings = append(autoBindWarnings, fmt.Sprintf("agent-deck session show returned invalid data during auto-bind: %v", err))
	} else if data != nil && strings.TrimSpace(data.Path) != "" {
		defaultWorkdir = strings.TrimSpace(data.Path)
	}

	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	if !m.state.autoBoundToolFallback ||
		m.state.detectedAgentDeckSession != "" ||
		!currentToolFallbackMatchesSnapshot(m.state, snapshot) {
		return nil
	}
	addresses := append([]string{agentDeckAddress(agentDeckSessionID)}, m.state.boundAddresses...)
	m.state.boundAddresses = dedupe(addresses)
	m.state.detectedAgentDeckSession = agentDeckSessionID
	m.state.defaultWorkdir = defaultWorkdir
	m.state.autoBoundToolFallback = false
	m.state.autoBindEmptyResult = false
	m.state.autoBindWarnings = append([]string(nil), autoBindWarnings...)
	if m.state.defaultSender == "" || slices.Contains(detectedToolSessionAddresses(snapshot), m.state.defaultSender) {
		m.state.defaultSender = agentDeckAddress(agentDeckSessionID)
	}
	m.state.autoBindAttempted = true
	return nil
}

func (m *sessionManager) detectCurrentAgentDeckSessionID(ctx context.Context, codexSessionID, defaultWorkdir string) (string, string, bool, []string, error) {
	envAgentDeckID := strings.TrimSpace(os.Getenv("AGENTDECK_INSTANCE_ID"))
	agentDeckSessionID := envAgentDeckID
	probeCompleted := envAgentDeckID != ""
	var warnings []string

	if agentDeckSessionID == "" {
		result, err := runProbe(ctx, m.runner, []string{"agent-deck", "session", "current", "--json"}, runOptions{timeout: syncCmdTimeout}, false)
		if err != nil {
			return "", defaultWorkdir, probeCompleted, warnings, err
		}
		if result != nil {
			probeCompleted = true
			if result.ExitCode == 0 {
				var current struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal([]byte(result.Stdout), &current); err != nil {
					warnings = append(warnings, fmt.Sprintf("agent-deck session current returned invalid JSON during auto-bind: %v", err))
				} else {
					agentDeckSessionID = strings.TrimSpace(current.ID)
				}
			}
		}
	}

	if envAgentDeckID == "" && codexSessionID != "" {
		match, lookupWarnings := lookupAgentDeckSessionByCodexID(ctx, codexSessionID)
		warnings = append(warnings, lookupWarnings...)
		if match != nil && strings.TrimSpace(match.SessionID) != "" {
			matchedSessionID := strings.TrimSpace(match.SessionID)
			if agentDeckSessionID != "" && agentDeckSessionID != matchedSessionID {
				warnings = append(warnings, fmt.Sprintf("agent-deck session current returned %q, but state database maps current codex session to %q; using codex-linked session", agentDeckSessionID, matchedSessionID))
			}
			agentDeckSessionID = matchedSessionID
			if strings.TrimSpace(match.ProjectPath) != "" && defaultWorkdir == "" {
				defaultWorkdir = strings.TrimSpace(match.ProjectPath)
			}
		}
	}

	return agentDeckSessionID, defaultWorkdir, probeCompleted, warnings, nil
}

func (m *sessionManager) detectCurrentCodexSessionID(ctx context.Context) (string, []string) {
	var warnings []string
	if sessionID, envWarnings := detectToolSessionIDFromEnv("CODEX_THREAD_ID"); sessionID != "" {
		return sessionID, nil
	} else if len(envWarnings) > 0 {
		warnings = append(warnings, envWarnings...)
	}
	if runtime.GOOS == "windows" {
		return "", warnings
	}
	found, probeWarnings := m.probeAncestorSessions(ctx, map[string]bool{"codex": true})
	return found["codex"], append(warnings, probeWarnings...)
}

func (m *sessionManager) detectCurrentToolSessionIDs(ctx context.Context) (toolSessionIDs, []string) {
	ids := toolSessionIDs{}
	var warnings []string
	for _, descriptor := range toolSessionDescriptors {
		sessionID, envWarnings := detectToolSessionIDFromEnv(descriptor.Env)
		if sessionID != "" {
			ids[descriptor.Scheme] = sessionID
		}
		warnings = append(warnings, envWarnings...)
	}
	if runtime.GOOS == "windows" {
		return ids, warnings
	}

	pending := map[string]bool{}
	if ids["codex"] == "" {
		pending["codex"] = true
	}
	if ids["devin"] == "" {
		pending["devin"] = true
	}
	// The ancestor walk always runs when a codex session is unresolved, so
	// Devin ancestors are resolved for free on the same chain. When Codex is
	// already identified, only probe for Devin when the environment indicates
	// a Devin-spawned process.
	if !pending["codex"] && !(pending["devin"] && devinProcessProbeEnabled()) {
		return ids, warnings
	}
	found, probeWarnings := m.probeAncestorSessions(ctx, pending)
	warnings = append(warnings, probeWarnings...)
	for scheme, sessionID := range found {
		if ids[scheme] == "" {
			ids[scheme] = sessionID
		}
	}
	return ids, warnings
}

// devinProcessProbeEnabled reports whether the environment indicates that this
// process was spawned by Devin. Devin propagates CHISEL_SESSION_DB to its
// children; DEVIN_SESSION_ID is the manual override for the session address.
func devinProcessProbeEnabled() bool {
	return strings.TrimSpace(os.Getenv("DEVIN_SESSION_ID")) != "" ||
		strings.TrimSpace(os.Getenv("CHISEL_SESSION_DB")) != ""
}

// probeAncestorSessions walks the parent process chain once and resolves the
// session ids of the tool schemes in pending, removing each from pending when
// its first matching ancestor is handled.
func (m *sessionManager) probeAncestorSessions(ctx context.Context, pending map[string]bool) (toolSessionIDs, []string) {
	ids := toolSessionIDs{}
	var warnings []string
	seen := map[int]bool{}
	pid := m.parentPID()
	for pid > 1 && !seen[pid] {
		seen[pid] = true
		row, err := m.getProcessRow(ctx, pid)
		if err != nil {
			for _, scheme := range []string{"codex", "devin"} {
				if pending[scheme] {
					warnings = append(warnings, fmt.Sprintf("%s session auto-bind probe failed: %v", scheme, err))
				}
			}
			return ids, warnings
		}
		if row == nil {
			break
		}
		if pending["codex"] && (row.Comm == "codex" || codexCommandPattern.MatchString(row.Args) || strings.Contains(row.Args, "@openai/codex")) {
			delete(pending, "codex")
			if fromArgs := extractCodexSessionIDFromArgs(row.Args); fromArgs != "" {
				ids["codex"] = fromArgs
			} else if fromLsof, err := m.extractCodexSessionIDFromLsof(ctx, row.PID); err != nil {
				warnings = append(warnings, fmt.Sprintf("codex session auto-bind probe failed: %v", err))
				return ids, warnings
			} else if fromLsof != "" {
				ids["codex"] = fromLsof
			}
		}
		if pending["devin"] && (row.Comm == "devin" || devinCommandPattern.MatchString(row.Args)) {
			delete(pending, "devin")
			devinID := ""
			if fromArgs := extractDevinSessionIDFromArgs(row.Args); fromArgs != "" {
				if failure := devinSessionIDValidationFailure(fromArgs); failure != "" {
					warnings = append(warnings, fmt.Sprintf("devin --resume session id %q is invalid (%s); ignoring it for auto-bind", fromArgs, failure))
				} else {
					devinID = fromArgs
				}
			}
			if devinID == "" {
				if fromLsof, err := m.extractDevinSessionIDFromLsof(ctx, row.PID); err != nil {
					warnings = append(warnings, fmt.Sprintf("devin session auto-bind probe failed: %v", err))
					return ids, warnings
				} else if fromLsof != "" {
					if failure := devinSessionIDValidationFailure(fromLsof); failure != "" {
						warnings = append(warnings, fmt.Sprintf("devin session lock id %q is invalid (%s); ignoring it for auto-bind", fromLsof, failure))
					} else {
						devinID = fromLsof
					}
				}
			}
			if devinID != "" {
				ids["devin"] = devinID
			}
		}
		if len(pending) == 0 {
			break
		}
		pid = row.PPID
	}
	return ids, warnings
}

func detectToolSessionIDFromEnv(name string) (string, []string) {
	sessionID := strings.TrimSpace(os.Getenv(name))
	if sessionID == "" {
		return "", nil
	}
	descriptor := toolSessionDescriptorForEnv(name)
	if descriptor.validationFailure(sessionID) == "" {
		return sessionID, nil
	}
	return "", []string{fmt.Sprintf("%s is set but does not look like %s; ignoring it for auto-bind", name, descriptor.invalidHint())}
}

func toolSessionDescriptorForEnv(name string) toolSessionDescriptor {
	for _, descriptor := range toolSessionDescriptors {
		if descriptor.Env == name {
			return descriptor
		}
	}
	return toolSessionDescriptor{Env: name}
}

func toolSessionValidationFailureForEnv(name, sessionID string) string {
	return toolSessionDescriptorForEnv(name).validationFailure(sessionID)
}

func toolSessionEnvWarnings() []string {
	var warnings []string
	for _, name := range toolSessionEnvNames() {
		_, envWarnings := detectToolSessionIDFromEnv(name)
		warnings = append(warnings, envWarnings...)
	}
	return warnings
}

func toolSessionEnvNames() []string {
	names := make([]string, 0, len(toolSessionDescriptors))
	for _, descriptor := range toolSessionDescriptors {
		names = append(names, descriptor.Env)
	}
	return names
}

func toolSessionSchemes() []string {
	schemes := make([]string, 0, len(toolSessionDescriptors))
	for _, descriptor := range toolSessionDescriptors {
		schemes = append(schemes, descriptor.Scheme)
	}
	return schemes
}

func (ids toolSessionIDs) clone() toolSessionIDs {
	if len(ids) == 0 {
		return nil
	}
	out := make(toolSessionIDs, len(ids))
	for scheme, sessionID := range ids {
		out[scheme] = sessionID
	}
	return out
}

func (ids toolSessionIDs) addresses() []string {
	addresses := make([]string, 0, len(ids))
	for _, descriptor := range toolSessionDescriptors {
		if sessionID := ids[descriptor.Scheme]; sessionID != "" {
			addresses = append(addresses, toolSessionAddress(descriptor.Scheme, sessionID))
		}
	}
	return addresses
}

func (ids toolSessionIDs) equal(other toolSessionIDs) bool {
	for _, descriptor := range toolSessionDescriptors {
		if ids[descriptor.Scheme] != other[descriptor.Scheme] {
			return false
		}
	}
	return true
}

func looksLikeHexSessionID(sessionID string) bool {
	return toolSessionIDValidationFailure(sessionID) == ""
}

func toolSessionIDValidationFailure(sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "empty"
	}
	if strings.Contains(sessionID, "--") {
		return "contains consecutive hyphen"
	}
	if !toolSessionIDPattern.MatchString(sessionID) {
		return "must contain only hex digits and single hyphens, and start and end with a hex digit"
	}
	hexDigits := strings.ReplaceAll(sessionID, "-", "")
	if len(hexDigits) < 8 {
		return "must contain at least 8 hex digits"
	}
	return ""
}

func detectedToolSessionAddresses(snapshot stateSnapshot) []string {
	return snapshot.DetectedToolSessions.addresses()
}

func detectedToolSessionOutputFields(ids toolSessionIDs, value func(string) any) map[string]any {
	out := make(map[string]any, len(toolSessionDescriptors))
	for _, descriptor := range toolSessionDescriptors {
		out[descriptor.StatusJSONKey] = value(ids[descriptor.Scheme])
	}
	return out
}

func boundToolSessionAddresses(boundAddresses []string) []string {
	return boundAddressesByScheme(boundAddresses, toolSessionSchemes()...)
}

func boundAddressesByScheme(boundAddresses []string, schemes ...string) []string {
	if len(schemes) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(schemes))
	for _, scheme := range schemes {
		allowed[scheme] = struct{}{}
	}
	addresses := make([]string, 0, len(boundAddresses))
	for _, address := range boundAddresses {
		parsed, err := waypost.ParseAddress(address)
		if err != nil {
			continue
		}
		if _, ok := allowed[parsed.Scheme]; ok {
			addresses = append(addresses, address)
		}
	}
	return addresses
}

func currentToolFallbackMatchesSnapshot(state *serverState, snapshot stateSnapshot) bool {
	if !state.detectedToolSessions.equal(snapshot.DetectedToolSessions) {
		return false
	}
	for _, address := range detectedToolSessionAddresses(snapshot) {
		if !slices.Contains(state.boundAddresses, address) {
			return false
		}
	}
	return true
}

func (m *sessionManager) getProcessRow(ctx context.Context, pid int) (*psRow, error) {
	if pid <= 1 {
		return nil, nil
	}
	result, err := runProbe(ctx, m.runner, []string{"ps", "-p", strconv.Itoa(pid), "-o", "pid=,ppid=,comm=,args="}, runOptions{timeout: syncCmdTimeout}, true)
	if err != nil {
		return nil, err
	}
	if result == nil || result.ExitCode != 0 {
		return nil, nil
	}
	return parsePSRow(result.Stdout), nil
}

func (m *sessionManager) extractCodexSessionIDFromLsof(ctx context.Context, pid int) (string, error) {
	return m.extractSessionIDFromLsof(ctx, pid, codexSessionFilePattern)
}

func (m *sessionManager) extractDevinSessionIDFromLsof(ctx context.Context, pid int) (string, error) {
	return m.extractSessionIDFromLsof(ctx, pid, devinSessionLockPattern)
}

func (m *sessionManager) extractSessionIDFromLsof(ctx context.Context, pid int, pattern *regexp.Regexp) (string, error) {
	if pid <= 1 {
		return "", nil
	}
	result, err := runProbe(ctx, m.runner, []string{"lsof", "-p", strconv.Itoa(pid)}, runOptions{timeout: syncCmdTimeout}, true)
	if err != nil {
		return "", err
	}
	if result == nil || result.ExitCode != 0 {
		return "", nil
	}
	for _, line := range strings.Split(result.Stdout, "\n") {
		match := pattern.FindStringSubmatch(line)
		if len(match) == 2 {
			return match[1], nil
		}
	}
	return "", nil
}

func extractDevinSessionIDFromArgs(args string) string {
	match := devinResumePattern.FindStringSubmatch(args)
	if len(match) != 2 {
		return ""
	}
	return match[1]
}

func devinSessionIDValidationFailure(sessionID string) string {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "empty"
	}
	if strings.Contains(sessionID, "--") {
		return "contains consecutive hyphen"
	}
	if !devinSessionIDPattern.MatchString(sessionID) {
		return "must contain only letters, digits, dots, underscores, and single hyphens, and start and end with a letter or digit"
	}
	if len(sessionID) < 3 {
		return "must contain at least 3 characters"
	}
	return ""
}

func (m *sessionManager) waypostAddresses(ctx context.Context, addresses []string) ([]string, error) {
	if err := validateMCPItems("addresses", len(addresses)); err != nil {
		return nil, err
	}
	if len(addresses) > 0 {
		return waypost.NormalizeAddressList(addresses)
	}
	bound, err := m.boundState(ctx)
	if err != nil {
		return nil, err
	}
	if len(bound.BoundAddresses) == 0 {
		return nil, errors.New("no waypost addresses provided and no waypost addresses are bound")
	}
	return append([]string(nil), bound.BoundAddresses...), nil
}

// waypostReceiveAddresses resolves a personal receive scope. Explicit
// addresses are intentionally limited to this MCP server's bound addresses;
// otherwise waypost_recv could inspect queues outside the session's ownership
// boundary.
func (m *sessionManager) waypostReceiveAddresses(ctx context.Context, addresses []string) ([]string, error) {
	resolved, err := m.waypostAddresses(ctx, addresses)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return resolved, nil
	}

	bound, err := m.boundState(ctx)
	if err != nil {
		return nil, err
	}
	for _, address := range resolved {
		if !slices.Contains(bound.BoundAddresses, address) {
			return nil, fmt.Errorf("receive address %q is not bound to this MCP server", address)
		}
	}
	return resolved, nil
}

func (m *sessionManager) senderAddress(ctx context.Context, override string) (string, error) {
	if strings.TrimSpace(override) != "" {
		address, err := waypost.NormalizeAddress(override)
		if err != nil {
			return "", err
		}
		if waypost.IsGroupAddress(address) {
			return "", fmt.Errorf("sender address %q uses reserved group/ prefix", address)
		}
		bound, err := m.boundState(ctx)
		if err != nil {
			return "", err
		}
		if !slices.Contains(bound.BoundAddresses, address) {
			return "", fmt.Errorf("from_address %q is not bound to this MCP server", address)
		}
		return address, nil
	}
	bound, err := m.boundState(ctx)
	if err != nil {
		return "", err
	}
	switch {
	case bound.DefaultSender != "":
		return bound.DefaultSender, nil
	case len(bound.BoundAddresses) > 0:
		return bound.BoundAddresses[0], nil
	default:
		return "", errors.New("waypost_send requires from_address or a bound default_sender")
	}
}

func (m *sessionManager) isLocalAddress(ctx context.Context, address string) bool {
	bound, err := m.boundState(ctx)
	if err != nil {
		return false
	}
	for _, candidate := range bound.BoundAddresses {
		if candidate == address {
			return true
		}
	}
	return false
}

func (m *sessionManager) resolveSessionShow(ctx context.Context, identifier string, timeout time.Duration) (*sessionData, error) {
	result, err := runProbe(ctx, m.runner, []string{"agent-deck", "session", "show", identifier, "--json"}, runOptions{timeout: timeout}, true)
	if err != nil {
		return nil, err
	}
	switch classifyAgentDeckSessionShowExit(result) {
	case sessionShowProbeNotFound:
		return nil, nil
	case sessionShowProbeUnknown:
		if result == nil {
			return nil, errors.New("agent-deck session show returned no result")
		}
		return nil, fmt.Errorf("agent-deck session show failed with exit code %d", result.ExitCode)
	}
	data, err := parseSessionData(result.Stdout, "agent-deck session show")
	if err != nil {
		return nil, hostSessionOutputFailure(err)
	}
	if data.Success != nil && !*data.Success {
		return nil, hostSessionOutputFailure(errors.New("agent-deck session show reported failure with exit code 0"))
	}
	return data, nil
}

func (m *sessionManager) resolveSessionShowBestEffort(ctx context.Context, identifier string) (*sessionData, error) {
	probe, err := m.probeSessionShowBestEffort(ctx, identifier)
	if err != nil {
		return nil, err
	}
	if probe.Status != sessionShowProbeFound {
		return nil, nil
	}
	return probe.Data, nil
}

func (m *sessionManager) probeSessionShowBestEffort(ctx context.Context, identifier string) (sessionShowProbeResult, error) {
	result, err := runProbe(ctx, m.runner, []string{"agent-deck", "session", "show", identifier, "--json"}, runOptions{timeout: syncCmdTimeout}, false)
	if err != nil {
		return sessionShowProbeResult{}, err
	}
	status := classifyAgentDeckSessionShowExit(result)
	if status != sessionShowProbeFound {
		return sessionShowProbeResult{Status: status}, nil
	}
	data, err := parseSessionData(result.Stdout, "agent-deck session show")
	if err != nil {
		return sessionShowProbeResult{}, err
	}
	if data.Success != nil && !*data.Success {
		return sessionShowProbeResult{Status: sessionShowProbeUnknown}, nil
	}
	return sessionShowProbeResult{Status: sessionShowProbeFound, Data: data}, nil
}

// agentDeckSessionNotFoundDetail explains a missing agent-deck target and,
// when possible, suggests a nearby currently-known session address. The
// durable Waypost send remains successful; this text is informational.
func (m *sessionManager) agentDeckSessionNotFoundDetail(ctx context.Context, identifier string) string {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return "warning: agent-deck session not found"
	}

	// Bound addresses cover sessions known by this server instance. The state
	// database adds recorded sessions that have not been explicitly bound as
	// Waypost endpoints; it is intentionally only a source of suggestions.
	snapshot := m.snapshotState()
	candidates := make([]string, 0)
	for _, address := range snapshot.BoundAddresses {
		parsed, err := waypost.ParseAddress(address)
		if err == nil && parsed.Scheme == "agent-deck" {
			candidates = append(candidates, parsed.Address)
		}
	}
	if databaseIDs := listAgentDeckSessionIDs(ctx); len(databaseIDs) > 0 {
		for _, id := range databaseIDs {
			candidates = append(candidates, agentDeckAddress(id))
		}
	}
	candidates = dedupe(candidates)

	detail := fmt.Sprintf("warning: agent-deck session %q not found (does not exist)", identifier)
	if suggestion := closestAgentDeckAddress(identifier, candidates); suggestion != "" {
		detail += ". Did you mean " + suggestion + "?"
	}
	return detail
}

func closestAgentDeckAddress(target string, candidates []string) string {
	target = strings.TrimSpace(target)
	if target == "" {
		return ""
	}
	targetLower := strings.ToLower(target)
	best := ""
	bestDistance := int(^uint(0) >> 1)
	for _, address := range candidates {
		parsed, err := waypost.ParseAddress(address)
		if err != nil || parsed.Scheme != "agent-deck" || strings.EqualFold(parsed.ID, target) {
			continue
		}
		candidateID := strings.ToLower(parsed.ID)
		distance := levenshteinDistance(targetLower, candidateID)
		// Session IDs are commonly made of hyphen-separated chunks. Agents
		// often copy one chunk incorrectly while preserving another; an exact
		// chunk is a strong signal even when the total edit distance is large.
		chunkMatch := false
		targetChunks, candidateChunks := strings.Split(targetLower, "-"), strings.Split(candidateID, "-")
		for _, left := range targetChunks {
			for _, right := range candidateChunks {
				if left != "" && left == right {
					chunkMatch = true
				}
			}
		}
		// Allow several edits for longer identifiers, while retaining a
		// bounded threshold for short names.
		maxDistance := maxInt(4, len([]rune(targetLower))/3)
		if !chunkMatch && (distance > maxDistance || distance > bestDistance) {
			continue
		}
		if distance == bestDistance && (best == "" || parsed.Address >= best) {
			continue
		}
		bestDistance = distance
		best = parsed.Address
	}
	return best
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func levenshteinDistance(left, right string) int {
	a, b := []rune(left), []rune(right)
	if len(a) < len(b) {
		a, b = b, a
	}
	previous := make([]int, len(b)+1)
	for index := range previous {
		previous[index] = index
	}
	for i, leftRune := range a {
		current := make([]int, len(b)+1)
		current[0] = i + 1
		for j, rightRune := range b {
			cost := 0
			if leftRune != rightRune {
				cost = 1
			}
			current[j+1] = min(current[j]+1, previous[j+1]+1, previous[j]+cost)
		}
		previous = current
	}
	return previous[len(b)]
}

func classifyAgentDeckSessionShowExit(result *RunResult) sessionShowProbeStatus {
	if result == nil {
		return sessionShowProbeUnknown
	}
	switch result.ExitCode {
	case 0:
		return sessionShowProbeFound
	case 2:
		return sessionShowProbeNotFound
	default:
		return sessionShowProbeUnknown
	}
}

func (m *sessionManager) createSession(ctx context.Context, input agentDeckCreateSessionInput) (map[string]any, error) {
	workdir, err := canonicalizeTargetWorkdir(input.Workdir, "creating")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.EnsureTitle) == "" {
		return nil, errors.New("ensure_title is required when creating a target session")
	}
	if strings.TrimSpace(input.EnsureCmd) == "" {
		return nil, errors.New("ensure_cmd is required when creating a target session")
	}

	existing, err := m.resolveSessionShow(ctx, input.EnsureTitle, ensureSessionShowTimeout)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if err := validateExistingSessionWorkdir(existing, input.Workdir, workdir); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("target session already exists: %s", input.EnsureTitle)
	}

	targetGroupPath, launchParentSessionID, launchNoParentLink, moveToRootGroupAfterLaunch, err := m.prepareCreateSessionLaunch(ctx, input)
	if err != nil {
		return nil, err
	}
	if targetGroupPath != "" {
		if err := m.ensureGroupPath(ctx, targetGroupPath); err != nil {
			return nil, err
		}
	}

	launchArgs := buildCreateSessionLaunchArgs(createSessionLaunchInput{
		EnsureTitle:        input.EnsureTitle,
		EnsureCmd:          input.EnsureCmd,
		Workdir:            workdir,
		ParentSessionID:    launchParentSessionID,
		NoParentLink:       launchNoParentLink,
		StartupInstruction: strings.TrimSpace(input.StartupInstruction),
		GroupPath:          targetGroupPath,
	})
	launchResult, err := runCommand(ctx, m.runner, launchArgs, runOptions{})
	if err != nil {
		return nil, err
	}
	receipt, err := parseSessionData(launchResult.Stdout, "agent-deck launch")
	if err != nil || receipt == nil || strings.TrimSpace(receipt.ID) == "" || (receipt.Success != nil && !*receipt.Success) {
		return agentDeckCreateRecoveryResult(input.EnsureTitle, workdir), nil
	}
	receipt.ID = strings.TrimSpace(receipt.ID)
	if moveToRootGroupAfterLaunch {
		if _, err := runCommand(ctx, m.runner, []string{"agent-deck", "group", "move", receipt.ID, ""}, runOptions{}); err != nil {
			return agentDeckCreatedUnverifiedResult(receipt, input.EnsureTitle, workdir, "post_create_group_move_failed", "", err.Error(), input.StartupInstruction), nil
		}
	}

	refreshed, err := m.resolveSessionShow(ctx, receipt.ID, ensureSessionShowTimeout)
	if err != nil {
		return agentDeckCreatedUnverifiedResult(receipt, input.EnsureTitle, workdir, "post_create_lookup_failed", "", err.Error(), input.StartupInstruction), nil
	}
	if refreshed == nil {
		return agentDeckCreatedUnverifiedResult(receipt, input.EnsureTitle, workdir, "post_create_lookup_failed", "", "target session not found after create", input.StartupInstruction), nil
	}

	expectedParentSessionID := ""
	if !launchNoParentLink {
		expectedParentSessionID = launchParentSessionID
	}
	receiptHost := hostSessionFromAgentDeck(receipt)
	refreshedHost := hostSessionFromAgentDeck(refreshed)
	// Dedicated Agent Deck titles are launched verbatim and historically use
	// exact post-create identity verification rather than normalized names.
	refreshedHost.Name = refreshed.Title
	verification := verifyCreatedHostSession(sessionHostAgentDeck, receiptHost, refreshedHost, createdHostSessionExpectation{
		Name:                input.EnsureTitle,
		ParentSessionID:     expectedParentSessionID,
		VerifyGroup:         true,
		Group:               targetGroupPath,
		GroupMismatchDetail: "refreshed agent-deck session group does not match requested group placement",
		RequestedWorkdir:    input.Workdir,
		CanonicalWorkdir:    workdir,
	})
	if verification.State != "verified" {
		resultData := receipt
		if verification.UseRefreshed {
			resultData = refreshed
		}
		return agentDeckCreatedUnverifiedResult(resultData, input.EnsureTitle, workdir, verification.State, verification.ObservedPath, verification.Detail, input.StartupInstruction), nil
	}

	out := sessionInfoMap(refreshed, input.EnsureTitle)
	out["status"] = "created"
	out["created_target"] = true
	out["started_session"] = true
	out["notify_needed"] = false
	if strings.TrimSpace(input.StartupInstruction) != "" {
		out["startup_instruction_status"] = "started_waiting"
	} else {
		out["startup_instruction_status"] = "started"
	}
	return out, nil
}

func agentDeckCreatedUnverifiedResult(data *sessionData, sessionRef, canonicalWorkdir, state, observedPath, detail, startupInstruction string) map[string]any {
	out := sessionInfoMap(data, sessionRef)
	out["status"] = "created_unverified"
	out["created_target"] = true
	out["started_session"] = true
	out["notify_needed"] = false
	out["recovery_required"] = true
	out["verification"] = verificationMap(state, canonicalWorkdir, observedPath, detail)
	if strings.TrimSpace(startupInstruction) != "" {
		out["startup_instruction_status"] = "started_waiting"
	} else {
		out["startup_instruction_status"] = "started"
	}
	return out
}

func agentDeckCreateRecoveryResult(sessionRef, canonicalWorkdir string) map[string]any {
	return map[string]any{
		"status":            "create_recovery_required",
		"created_target":    nil,
		"started_session":   nil,
		"notify_needed":     false,
		"recovery_required": true,
		"verification":      verificationMap("create_output_unparseable", canonicalWorkdir, "", "agent-deck session create returned unusable output"),
		"session_id":        nil,
		"session_ref":       sessionRef,
		"title":             nil,
		"session_status":    nil,
		"group":             nil,
		"path":              nil,
		"addresses":         []string{},
	}
}

func (m *sessionManager) requireSession(ctx context.Context, input agentDeckRequireSessionInput) (map[string]any, error) {
	if firstNonEmpty(input.SessionID, input.SessionRef) == "" {
		return nil, errors.New("session_id or session_ref is required when requiring a target session")
	}
	workdir, err := canonicalizeTargetWorkdir(input.Workdir, "requiring")
	if err != nil {
		return nil, err
	}
	return m.requireSessionWithCanonicalWorkdir(ctx, input, workdir)
}

func (m *sessionManager) requireSessionWithCanonicalWorkdir(ctx context.Context, input agentDeckRequireSessionInput, workdir string) (map[string]any, error) {
	identifier := firstNonEmpty(input.SessionID, input.SessionRef)
	if identifier == "" {
		return nil, errors.New("session_id or session_ref is required when requiring a target session")
	}

	data, err := m.resolveSessionShow(ctx, identifier, ensureSessionShowTimeout)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return map[string]any{
			"status":          "not_found",
			"session_id":      nil,
			"session_ref":     identifier,
			"title":           nil,
			"session_status":  nil,
			"group":           nil,
			"path":            nil,
			"addresses":       []string{},
			"created_target":  false,
			"started_session": false,
			"notify_needed":   false,
		}, nil
	}
	if err := validateExistingSessionWorkdir(data, input.Workdir, workdir); err != nil {
		return nil, err
	}
	if !autoRestartEnabled(input.AutoRestart) && !activeSessionStatuses[strings.TrimSpace(data.Status)] {
		out := sessionInfoMap(data, firstNonEmpty(input.SessionRef, identifier))
		out["status"] = "not_ready"
		out["created_target"] = false
		out["started_session"] = false
		out["notify_needed"] = false
		out["startup_instruction_status"] = "not_started_auto_restart_disabled"
		return out, nil
	}

	start, err := m.startSessionIfNeeded(ctx, data, "", input.Workdir, workdir)
	if err != nil {
		return nil, err
	}

	out := sessionInfoMap(start.Data, firstNonEmpty(input.SessionRef, identifier))
	if start.Verification != nil {
		out["status"] = "ready_unverified"
		out["recovery_required"] = true
		out["verification"] = verificationMap(start.Verification.State, workdir, start.Verification.ObservedPath, start.Verification.Detail)
	} else {
		out["status"] = "ready"
	}
	out["created_target"] = false
	out["started_session"] = start.StartedSession
	out["notify_needed"] = start.NotifyNeeded
	out["startup_instruction_status"] = start.StartupInstructionStatus
	return out, nil
}

func canonicalizeTargetWorkdir(workdir, action string) (string, error) {
	trimmed := strings.TrimSpace(workdir)
	if trimmed == "" {
		return "", fmt.Errorf("workdir is required when %s a target session", action)
	}
	canonicalWorkdir, err := canonicalizeExistingPath(trimmed)
	if err != nil {
		return "", fmt.Errorf("workdir does not exist: %s", workdir)
	}
	return canonicalWorkdir, nil
}

func validateExistingSessionWorkdir(data *sessionData, requestedWorkdir, canonicalWorkdir string) error {
	existingPath := strings.TrimSpace(data.Path)
	if existingPath == "" {
		return errors.New("existing session path unavailable: cannot verify workdir match")
	}
	canonicalExistingPath, err := canonicalizeExistingPath(existingPath)
	if err != nil {
		return fmt.Errorf("canonicalize existing session path %q: %w", existingPath, err)
	}
	if canonicalExistingPath != canonicalWorkdir {
		return fmt.Errorf("session path mismatch: existing='%s' expected='%s'", data.Path, requestedWorkdir)
	}
	return nil
}

func (m *sessionManager) prepareCreateSessionLaunch(ctx context.Context, input agentDeckCreateSessionInput) (string, string, bool, bool, error) {
	targetGroupPath := strings.TrimSpace(input.GroupPath)
	noParentLink := input.NoParentLink
	if noParentLink && strings.TrimSpace(input.ParentSessionID) != "" {
		return "", "", false, false, errors.New("no_parent_link cannot be combined with parent_session_id")
	}

	if targetGroupPath == "" && strings.TrimSpace(input.GroupParentSessionID) != "" {
		parentData, err := m.resolveSessionShow(ctx, input.GroupParentSessionID, ensureSessionShowTimeout)
		if err != nil {
			return "", "", false, false, err
		}
		if parentData == nil {
			return "", "", false, false, fmt.Errorf("group_parent_session_id not found: %s", input.GroupParentSessionID)
		}
		childGroupName := firstNonEmpty(input.ChildGroupName, input.EnsureTitle)
		targetGroupPath, err = deriveGroupPathFromParentGroup(strings.TrimSpace(parentData.Group), childGroupName)
		if err != nil {
			return "", "", false, false, err
		}
	}

	launchParentSessionID := strings.TrimSpace(input.ParentSessionID)
	launchNoParentLink := noParentLink
	if launchParentSessionID == "" && targetGroupPath == "" && !launchNoParentLink {
		return "", "", false, false, errors.New("creating a target session requires either group_path/group_parent_session_id or parent_session_id")
	}
	if launchParentSessionID == "" {
		return targetGroupPath, launchParentSessionID, launchNoParentLink, false, nil
	}

	parentData, err := m.resolveSessionShow(ctx, launchParentSessionID, ensureSessionShowTimeout)
	if err != nil {
		return "", "", false, false, err
	}
	if parentData == nil {
		return "", "", false, false, fmt.Errorf("parent_session_id not found: %s", input.ParentSessionID)
	}
	if strings.TrimSpace(parentData.ParentSessionID) == "" {
		if targetGroupPath == "" {
			targetGroupPath = strings.TrimSpace(parentData.Group)
			if targetGroupPath == "" {
				return targetGroupPath, launchParentSessionID, launchNoParentLink, true, nil
			}
		}
		return targetGroupPath, launchParentSessionID, launchNoParentLink, false, nil
	}
	if targetGroupPath == "" {
		targetGroupPath, err = deriveGroupPathFromParentGroup(strings.TrimSpace(parentData.Group), firstNonEmpty(parentData.Title, input.ParentSessionID, parentData.ID))
		if err != nil {
			return "", "", false, false, err
		}
	}
	return targetGroupPath, "", true, false, nil
}

func (m *sessionManager) startSessionIfNeeded(ctx context.Context, data *sessionData, startupInstruction, requestedWorkdir, canonicalWorkdir string) (sessionStartResult, error) {
	if activeSessionStatuses[strings.TrimSpace(data.Status)] {
		return sessionStartResult{
			Data:                     data,
			NotifyNeeded:             true,
			StartupInstructionStatus: "not_needed_existing_session",
		}, nil
	}

	startArgs := []string{"agent-deck", "session", "start", "--json"}
	if startupInstruction != "" {
		startArgs = append(startArgs, "-m", startupInstruction)
	}
	startArgs = append(startArgs, data.ID)
	if _, err := runCommand(ctx, m.runner, startArgs, runOptions{}); err != nil {
		return sessionStartResult{}, err
	}

	startupInstructionStatus := "started"
	if startupInstruction != "" {
		startupInstructionStatus = "started_waiting"
	}
	unverified := func(state, observedPath, detail string) sessionStartResult {
		return sessionStartResult{
			Data:                     data,
			StartedSession:           true,
			StartupInstructionStatus: startupInstructionStatus,
			Verification: &sessionStartVerification{
				State:        state,
				ObservedPath: observedPath,
				Detail:       detail,
			},
		}
	}

	refreshed, err := m.resolveSessionShow(ctx, data.ID, ensureSessionShowTimeout)
	if err != nil {
		state := "post_start_lookup_failed"
		if isHostSessionOutputFailure(err) {
			state = "post_start_output_unparseable"
		}
		return unverified(state, "", err.Error()), nil
	}
	if refreshed == nil {
		return unverified("post_start_disappeared", "", "target session not found after start"), nil
	}
	if refreshed.ID != data.ID {
		return unverified("post_start_output_unparseable", "", fmt.Sprintf("refreshed session id %q does not match started session id %q", refreshed.ID, data.ID)), nil
	}
	if !activeSessionStatuses[strings.ToLower(strings.TrimSpace(refreshed.Status))] {
		return unverified("post_start_not_ready", refreshed.Path, "target session is not ready after start"), nil
	}
	workdir := verifyHostSessionWorkdir(hostSessionFromAgentDeck(refreshed), requestedWorkdir, canonicalWorkdir)
	if workdir.State != "verified" {
		return unverified(postStartVerificationState(workdir.State), workdir.ObservedPath, workdir.Err.Error()), nil
	}
	return sessionStartResult{
		Data:                     refreshed,
		StartedSession:           true,
		StartupInstructionStatus: startupInstructionStatus,
	}, nil
}

func (m *sessionManager) listGroupPaths(ctx context.Context) (map[string]bool, error) {
	result, err := runCommand(ctx, m.runner, []string{"agent-deck", "group", "list", "--json"}, runOptions{})
	if err != nil {
		return nil, err
	}
	var payload struct {
		Groups []struct {
			Path string `json:"path"`
		} `json:"groups"`
	}
	if err := json.Unmarshal([]byte(result.Stdout), &payload); err != nil {
		return nil, fmt.Errorf("agent-deck group list returned invalid JSON: %w", err)
	}
	paths := map[string]bool{}
	for _, group := range payload.Groups {
		if trimmed := strings.TrimSpace(group.Path); trimmed != "" {
			paths[trimmed] = true
		}
	}
	return paths, nil
}

func (m *sessionManager) ensureGroupPath(ctx context.Context, groupPath string) error {
	trimmed := strings.TrimSpace(groupPath)
	if trimmed == "" {
		return nil
	}
	existing, err := m.listGroupPaths(ctx)
	if err != nil {
		return err
	}
	current := ""
	for _, rawSegment := range strings.Split(trimmed, "/") {
		segment := strings.TrimSpace(rawSegment)
		if segment == "" {
			return fmt.Errorf("invalid group path: %s", groupPath)
		}
		next := segment
		if current != "" {
			next = current + "/" + segment
		}
		if !existing[next] {
			createArgs := []string{"agent-deck", "group", "create", segment}
			if current != "" {
				createArgs = append(createArgs, "--parent", current)
			}
			if _, err := runCommand(ctx, m.runner, createArgs, runOptions{}); err != nil {
				return err
			}
			existing[next] = true
		}
		current = next
	}
	return nil
}

func canonicalizeExistingPath(path string) (string, error) {
	absolutePath, err := filepath.Abs(strings.TrimSpace(path))
	if err != nil {
		return "", err
	}
	resolvedPath, err := filepath.EvalSymlinks(absolutePath)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolvedPath)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("not a directory")
	}
	return resolvedPath, nil
}

func parseSessionData(text, context string) (*sessionData, error) {
	var data sessionData
	if err := json.Unmarshal([]byte(text), &data); err != nil {
		return nil, fmt.Errorf("%s returned invalid JSON: %w", context, err)
	}
	return &data, nil
}

func sessionInfoMap(data *sessionData, sessionRef string) map[string]any {
	return map[string]any{
		"session_id":     data.ID,
		"session_ref":    firstNonEmpty(sessionRef, data.Title, data.ID),
		"title":          nilIfEmpty(data.Title),
		"session_status": nilIfEmpty(data.Status),
		"group":          nilIfEmpty(data.Group),
		"path":           nilIfEmpty(data.Path),
		"addresses":      []string{agentDeckAddress(data.ID)},
	}
}

func parsePSRow(text string) *psRow {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) < 3 {
		return nil
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil {
		return nil
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return nil
	}
	args := ""
	if len(fields) > 3 {
		args = strings.Join(fields[3:], " ")
	}
	return &psRow{
		PID:  pid,
		PPID: ppid,
		Comm: fields[2],
		Args: args,
	}
}

func extractCodexSessionIDFromArgs(args string) string {
	match := codexResumePattern.FindStringSubmatch(args)
	if len(match) != 2 {
		return ""
	}
	return match[1]
}
