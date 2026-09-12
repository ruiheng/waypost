package hookcore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileNudgeStorePersistsAndClearsSessionState(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "hook-state")
	store := FileNudgeStore{Dir: stateDir, Label: "Test"}
	const sessionID = "session/with unsafe path characters"
	if err := store.Save(sessionID, NudgePending); err != nil {
		t.Fatalf("Save(pending) error = %v", err)
	}
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		t.Fatalf("ReadDir(%q) error = %v", stateDir, err)
	}
	if len(entries) != 1 || entries[0].IsDir() {
		t.Fatalf("state entries = %#v, want one file", entries)
	}
	if state, err := store.Load(sessionID); err != nil || state != NudgePending {
		t.Fatalf("Load(pending) = %q, %v; want pending", state, err)
	}
	if err := store.Save(sessionID, NudgeConsumed); err != nil {
		t.Fatalf("Save(consumed) error = %v", err)
	}
	if err := store.Clear(sessionID); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}
	if state, err := store.Load(sessionID); err != nil || state != NudgeNone {
		t.Fatalf("Load(after clear) = %q, %v; want none", state, err)
	}
}

func TestFileNudgeStoreProbeRecordRoundTrip(t *testing.T) {
	t.Parallel()

	store := FileNudgeStore{Dir: t.TempDir(), Label: "Test"}
	const sessionID = "probe-session"
	want := MCPProbeRecord{Available: true, DisabledTools: []string{"waypost_send"}}
	if err := store.SaveMCPProbe(sessionID, want); err != nil {
		t.Fatalf("SaveMCPProbe() error = %v", err)
	}
	record, ok, err := store.LoadMCPProbe(sessionID)
	if err != nil || !ok {
		t.Fatalf("LoadMCPProbe() = ok %v, err %v; want cached record", ok, err)
	}
	if !record.Available || len(record.DisabledTools) != 1 || record.DisabledTools[0] != "waypost_send" {
		t.Fatalf("LoadMCPProbe() = %+v, want %+v", record, want)
	}
	if err := store.ClearMCPProbe(sessionID); err != nil {
		t.Fatalf("ClearMCPProbe() error = %v", err)
	}
	if _, ok, err := store.LoadMCPProbe(sessionID); err != nil || ok {
		t.Fatalf("LoadMCPProbe(after clear) = ok %v, err %v; want cleared", ok, err)
	}
}

func TestFileMCPProbeRejectsMissingAvailability(t *testing.T) {
	t.Parallel()

	store := FileNudgeStore{Dir: t.TempDir(), Label: "Test"}
	const sessionID = "incomplete-probe"
	path, err := store.ProbePath(sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(store.Dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"session_id":"incomplete-probe"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.LoadMCPProbe(sessionID); err == nil || ok {
		t.Fatalf("LoadMCPProbe() = ok %v, err %v; want parse error", ok, err)
	}
}

func TestProbeAndCacheRunsOncePerSession(t *testing.T) {
	t.Parallel()

	store := NewMemoryNudgeStore()
	calls := 0
	run := func(context.Context) (MCPProbeRecord, error) {
		calls++
		return MCPProbeRecord{Available: true}, nil
	}
	for i := 0; i < 3; i++ {
		record, err := ProbeAndCache(context.Background(), "session-1", run, store)
		if err != nil || !record.Available {
			t.Fatalf("ProbeAndCache() = %+v, %v; want available", record, err)
		}
	}
	if calls != 1 {
		t.Fatalf("probe calls = %d, want 1 (cached)", calls)
	}

	// A different session gets its own probe; an empty session skips caching.
	if _, err := ProbeAndCache(context.Background(), "session-2", run, store); err != nil {
		t.Fatal(err)
	}
	if _, err := ProbeAndCache(context.Background(), "", run, store); err != nil {
		t.Fatal(err)
	}
	if _, err := ProbeAndCache(context.Background(), "", run, store); err != nil {
		t.Fatal(err)
	}
	if calls != 4 {
		t.Fatalf("probe calls = %d, want 4", calls)
	}
}

func TestProbeAndCachePropagatesProbeError(t *testing.T) {
	t.Parallel()

	store := NewMemoryNudgeStore()
	probeErr := errors.New("probe exploded")
	_, err := ProbeAndCache(context.Background(), "session-1", func(context.Context) (MCPProbeRecord, error) {
		return MCPProbeRecord{}, probeErr
	}, store)
	if !errors.Is(err, probeErr) {
		t.Fatalf("ProbeAndCache() error = %v, want %v", err, probeErr)
	}
	// Failed probes are not cached: the next call retries.
	if _, err := ProbeAndCache(context.Background(), "session-1", func(context.Context) (MCPProbeRecord, error) {
		return MCPProbeRecord{Available: true}, nil
	}, store); err != nil {
		t.Fatalf("ProbeAndCache(retry) error = %v", err)
	}
}

func TestNormalizeSessionID(t *testing.T) {
	t.Parallel()

	if got, err := NormalizeSessionID("  abc  ", "Test"); err != nil || got != "abc" {
		t.Fatalf("NormalizeSessionID() = %q, %v; want abc", got, err)
	}
	if _, err := NormalizeSessionID("   ", "Test"); err == nil || !strings.Contains(err.Error(), "Test hook input is missing session_id") {
		t.Fatalf("NormalizeSessionID(blank) error = %v, want labeled error", err)
	}
}
