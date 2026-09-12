package hookcore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// NudgeState tracks whether the harness was nudged about a pending Waypost
// delivery and whether that receive completed.
type NudgeState string

const (
	NudgeNone     NudgeState = ""
	NudgePending  NudgeState = "pending"
	NudgeConsumed NudgeState = "consumed"
)

// MCPProbeRecord is the cached result of probing the harness CLI for the
// Waypost MCP server. DisabledTools lists per-tool disabled entries for
// harnesses that report them; harnesses without tool-level reporting leave it
// empty.
type MCPProbeRecord struct {
	Available     bool
	DisabledTools []string
}

// NudgeStore persists per-session nudge state and MCP probe results.
type NudgeStore interface {
	Load(sessionID string) (NudgeState, error)
	Save(sessionID string, state NudgeState) error
	LoadMCPProbe(sessionID string) (MCPProbeRecord, bool, error)
	SaveMCPProbe(sessionID string, record MCPProbeRecord) error
	ClearMCPProbe(sessionID string) error
	Clear(sessionID string) error
}

// ProbeAndCache runs the MCP probe at most once per session: a cached record
// wins, and a fresh successful probe is persisted. A missing or empty
// sessionID skips all cache access because there is nothing to key on; the
// probe still runs so the nudge can react to MCP availability.
func ProbeAndCache(ctx context.Context, sessionID string, run func(context.Context) (MCPProbeRecord, error), store NudgeStore) (MCPProbeRecord, error) {
	if strings.TrimSpace(sessionID) == "" {
		return run(ctx)
	}
	if record, ok, err := store.LoadMCPProbe(sessionID); err != nil {
		return MCPProbeRecord{}, err
	} else if ok {
		return record, nil
	}
	record, err := run(ctx)
	if err != nil {
		return MCPProbeRecord{}, err
	}
	if err := store.SaveMCPProbe(sessionID, record); err != nil {
		return MCPProbeRecord{}, err
	}
	return record, nil
}

// FileNudgeStore persists nudge state as JSON files under Dir. Label names
// the harness (for example "Codex" or "Devin") in error messages.
type FileNudgeStore struct {
	Dir   string
	Label string
}

type nudgeStateRecord struct {
	SessionID string     `json:"session_id"`
	State     NudgeState `json:"state"`
}

func (store FileNudgeStore) Load(sessionID string) (NudgeState, error) {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return NudgeNone, err
	}
	path, err := store.StatePath(sessionID)
	if err != nil {
		return NudgeNone, err
	}
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return NudgeNone, nil
	}
	if err != nil {
		return NudgeNone, fmt.Errorf("read %s Waypost nudge state %q: %w", store.Label, path, err)
	}
	var record nudgeStateRecord
	if err := json.Unmarshal(contents, &record); err != nil {
		return NudgeNone, fmt.Errorf("parse %s Waypost nudge state %q: %w", store.Label, path, err)
	}
	if record.SessionID != sessionID {
		return NudgeNone, fmt.Errorf("parse %s Waypost nudge state %q: session id mismatch", store.Label, path)
	}
	if record.State != NudgePending && record.State != NudgeConsumed {
		return NudgeNone, fmt.Errorf("parse %s Waypost nudge state %q: invalid state %q", store.Label, path, record.State)
	}
	return record.State, nil
}

func (store FileNudgeStore) Save(sessionID string, state NudgeState) error {
	if state != NudgePending && state != NudgeConsumed {
		return fmt.Errorf("save %s Waypost nudge state: invalid state %q", store.Label, state)
	}
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return err
	}
	path, err := store.StatePath(sessionID)
	if err != nil {
		return err
	}
	contents, err := json.Marshal(nudgeStateRecord{SessionID: sessionID, State: state})
	if err != nil {
		return fmt.Errorf("encode %s Waypost nudge state: %w", store.Label, err)
	}
	contents = append(contents, '\n')
	if err := store.writeAtomic(path, contents, ".state.tmp-*"); err != nil {
		return fmt.Errorf("save %s Waypost nudge state: %w", store.Label, err)
	}
	return nil
}

func (store FileNudgeStore) LoadMCPProbe(sessionID string) (MCPProbeRecord, bool, error) {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return MCPProbeRecord{}, false, err
	}
	path, err := store.ProbePath(sessionID)
	if err != nil {
		return MCPProbeRecord{}, false, err
	}
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return MCPProbeRecord{}, false, nil
	}
	if err != nil {
		return MCPProbeRecord{}, false, fmt.Errorf("read %s Waypost MCP probe %q: %w", store.Label, path, err)
	}
	var record struct {
		SessionID     string   `json:"session_id"`
		Available     *bool    `json:"available"`
		DisabledTools []string `json:"disabled_tools"`
	}
	if err := json.Unmarshal(contents, &record); err != nil {
		return MCPProbeRecord{}, false, fmt.Errorf("parse %s Waypost MCP probe %q: %w", store.Label, path, err)
	}
	if record.SessionID != sessionID {
		return MCPProbeRecord{}, false, fmt.Errorf("parse %s Waypost MCP probe %q: session id mismatch", store.Label, path)
	}
	if record.Available == nil {
		return MCPProbeRecord{}, false, fmt.Errorf("parse %s Waypost MCP probe %q: missing available field", store.Label, path)
	}
	return MCPProbeRecord{Available: *record.Available, DisabledTools: record.DisabledTools}, true, nil
}

func (store FileNudgeStore) SaveMCPProbe(sessionID string, record MCPProbeRecord) error {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return err
	}
	path, err := store.ProbePath(sessionID)
	if err != nil {
		return err
	}
	disabled := append([]string(nil), record.DisabledTools...)
	sort.Strings(disabled)
	contents, err := json.Marshal(struct {
		SessionID     string   `json:"session_id"`
		Available     bool     `json:"available"`
		DisabledTools []string `json:"disabled_tools,omitempty"`
	}{sessionID, record.Available, disabled})
	if err != nil {
		return fmt.Errorf("encode %s Waypost MCP probe: %w", store.Label, err)
	}
	contents = append(contents, '\n')
	if err := store.writeAtomic(path, contents, ".mcp-state.tmp-*"); err != nil {
		return fmt.Errorf("save %s Waypost MCP probe: %w", store.Label, err)
	}
	return nil
}

func (store FileNudgeStore) ClearMCPProbe(sessionID string) error {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return err
	}
	path, err := store.ProbePath(sessionID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s Waypost MCP probe %q: %w", store.Label, path, err)
	}
	return nil
}

func (store FileNudgeStore) Clear(sessionID string) error {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return err
	}
	path, err := store.StatePath(sessionID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s Waypost nudge state %q: %w", store.Label, path, err)
	}
	return nil
}

func (store FileNudgeStore) writeAtomic(path string, contents []byte, tempPattern string) error {
	if err := os.MkdirAll(store.Dir, 0o700); err != nil {
		return fmt.Errorf("create %s Waypost nudge state directory %q: %w", store.Label, store.Dir, err)
	}
	temporary, err := os.CreateTemp(store.Dir, tempPattern)
	if err != nil {
		return fmt.Errorf("create temporary %s Waypost state: %w", store.Label, err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("set %s Waypost state permissions: %w", store.Label, err)
	}
	if _, err := temporary.Write(contents); err != nil {
		return fmt.Errorf("write %s Waypost state: %w", store.Label, err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync %s Waypost state: %w", store.Label, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close %s Waypost state: %w", store.Label, err)
	}
	if err := ReplaceFile(temporaryPath, path); err != nil {
		return fmt.Errorf("replace %s Waypost state %q: %w", store.Label, path, err)
	}
	return nil
}

// StatePath returns the hashed state file for sessionID.
func (store FileNudgeStore) StatePath(sessionID string) (string, error) {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(sessionID))
	return filepath.Join(store.Dir, fmt.Sprintf("%x.json", digest)), nil
}

// ProbePath returns the MCP probe cache file for sessionID.
func (store FileNudgeStore) ProbePath(sessionID string) (string, error) {
	path, err := store.StatePath(sessionID)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(path, ".json") + ".mcp.json", nil
}

// MemoryNudgeStore is an in-memory NudgeStore for tests.
type MemoryNudgeStore struct {
	states map[string]NudgeState
	probes map[string]MCPProbeRecord
	Label  string
}

func NewMemoryNudgeStore() *MemoryNudgeStore {
	return &MemoryNudgeStore{states: make(map[string]NudgeState), probes: make(map[string]MCPProbeRecord), Label: "Hook"}
}

func (store *MemoryNudgeStore) Load(sessionID string) (NudgeState, error) {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return NudgeNone, err
	}
	return store.states[sessionID], nil
}

func (store *MemoryNudgeStore) Save(sessionID string, state NudgeState) error {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return err
	}
	store.states[sessionID] = state
	return nil
}

func (store *MemoryNudgeStore) LoadMCPProbe(sessionID string) (MCPProbeRecord, bool, error) {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return MCPProbeRecord{}, false, err
	}
	record, ok := store.probes[sessionID]
	return record, ok, nil
}

func (store *MemoryNudgeStore) SaveMCPProbe(sessionID string, record MCPProbeRecord) error {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return err
	}
	store.probes[sessionID] = record
	return nil
}

func (store *MemoryNudgeStore) ClearMCPProbe(sessionID string) error {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return err
	}
	delete(store.probes, sessionID)
	return nil
}

func (store *MemoryNudgeStore) Clear(sessionID string) error {
	sessionID, err := NormalizeSessionID(sessionID, store.Label)
	if err != nil {
		return err
	}
	delete(store.states, sessionID)
	return nil
}

// NormalizeSessionID trims the id and rejects empty values. Label names the
// harness in the error.
func NormalizeSessionID(sessionID, label string) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "", fmt.Errorf("%s hook input is missing session_id", label)
	}
	return sessionID, nil
}
