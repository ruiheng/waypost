package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/ruiheng/waypost/internal/waypost"
)

func boolPtr(v bool) *bool {
	return &v
}

func TestBindingModeCLISuppressedIgnoresThurbox(t *testing.T) {
	isolateAutoBindEnv(t)
	t.Setenv("THURBOX_SESSION", thurboxAuthorID)

	svc := newService(Options{
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, _ string) (RunResult, error) {
			t.Fatalf("unexpected command execution in cli_suppressed mode: %v", args)
			return RunResult{}, nil
		}},
		DisableLeaseRenewLoop: true,
		DisableWakeScheduler:  true,
	})
	defer svc.Close()

	svc.state.setCLISuppressed("  codex/cli-sender  ")

	bound, err := svc.sessions.boundState(context.Background())
	if err != nil {
		t.Fatalf("boundState() error = %v", err)
	}
	if !slices.Equal(bound.BoundAddresses, []string{"codex/cli-sender"}) {
		t.Fatalf("BoundAddresses = %v, want [codex/cli-sender]", bound.BoundAddresses)
	}
	if bound.DetectedThurboxSession != "" {
		t.Fatalf("DetectedThurboxSession = %q, want empty", bound.DetectedThurboxSession)
	}
	snap := svc.sessions.snapshotState()
	if snap.BindingMode != bindingModeCLISuppressed {
		t.Fatalf("BindingMode = %q, want %q", snap.BindingMode, bindingModeCLISuppressed)
	}
}

func TestBindingModeManualEmptyBindWinsOverConcurrentAutoBind(t *testing.T) {
	isolateAutoBindEnv(t)

	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	svc := newService(Options{
		CommandRunner: &fakeRunner{
			t: t,
			ctxHandler: func(_ context.Context, args []string, _ string) (RunResult, error) {
				if len(args) >= 3 && args[0] == "agent-deck" && args[1] == "session" && args[2] == "current" {
					close(probeStarted)
					<-releaseProbe
					return RunResult{Stdout: `{"id":"auto-session","title":"auto","path":"/tmp/auto"}`}, nil
				}
				return RunResult{ExitCode: 1}, nil
			},
		},
		DisableLeaseRenewLoop: true,
		DisableWakeScheduler:  true,
	})
	defer svc.Close()

	done := make(chan error, 1)
	go func() {
		_, err := svc.sessions.boundState(context.Background())
		done <- err
	}()

	<-probeStarted
	svc.state.setManualBinding(nil, "", "")
	close(releaseProbe)

	if err := <-done; err != nil {
		t.Fatalf("concurrent boundState() error = %v", err)
	}
	snap := svc.sessions.snapshotState()
	if snap.BindingMode != bindingModeManual {
		t.Fatalf("BindingMode = %q, want %q", snap.BindingMode, bindingModeManual)
	}
	if len(snap.BoundAddresses) != 0 {
		t.Fatalf("BoundAddresses = %v, want empty manual binding preserved", snap.BoundAddresses)
	}
}

func TestBindingModeThurboxInjectionClearsToolFallback(t *testing.T) {
	isolateAutoBindEnv(t)
	t.Setenv("THURBOX_SESSION", thurboxAuthorID)

	svc := newService(Options{
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, _ string) (RunResult, error) {
			return RunResult{ExitCode: 1}, nil
		}},
		DisableLeaseRenewLoop: true,
		DisableWakeScheduler:  true,
	})
	defer svc.Close()

	svc.state.mu.Lock()
	svc.state.mode = bindingModeAutoToolFallback
	svc.state.boundAddresses = []string{"codex/0123456789abcdef"}
	svc.state.defaultSender = "codex/0123456789abcdef"
	svc.state.detectedToolSessions = toolSessionIDs{"codex": "0123456789abcdef"}
	svc.state.mu.Unlock()

	if err := svc.sessions.tryAutoBindCurrentSession(context.Background()); err != nil {
		t.Fatalf("tryAutoBindCurrentSession() error = %v", err)
	}
	snap := svc.sessions.snapshotState()
	if snap.BindingMode != bindingModeAutoHostBound {
		t.Fatalf("BindingMode = %q, want %q", snap.BindingMode, bindingModeAutoHostBound)
	}
	if snap.AutoBoundToolFallback {
		t.Fatal("AutoBoundToolFallback = true, want false after Thurbox injection")
	}
	if !slices.Contains(snap.BoundAddresses, "thurbox/"+thurboxAuthorID) {
		t.Fatalf("BoundAddresses = %v, want thurbox session included", snap.BoundAddresses)
	}
	if snap.DefaultSender != "thurbox/"+thurboxAuthorID {
		t.Fatalf("DefaultSender = %q, want thurbox/%s", snap.DefaultSender, thurboxAuthorID)
	}
}

func TestActiveLeaseManagerSingleMapTokenABAAndLockFreeReconcile(t *testing.T) {
	t.Parallel()

	mgr := newActiveLeaseManager()
	mgr.trackReceive(waypost.ReceiveResult{Messages: []waypost.ReceivedMessage{{
		DeliveryID:       "dlv_1",
		RecipientAddress: "codex/self",
		LeaseToken:       "tok_1",
		LeaseExpiresAt:   "2026-04-03T10:00:00Z",
	}}}, "2026-04-03T09:59:00Z")

	// Definitive failure with matching token sets lastError and blocks terminal mutation.
	mgr.markRenewalFailure(activeLease{DeliveryID: "dlv_1", LeaseToken: "tok_1"}, waypost.ErrLeaseTokenMismatch)
	if err := mgr.lastRenewalError(); err == nil {
		t.Fatal("lastRenewalError() = nil, want definitive renewal error")
	}
	if err := mgr.terminalMutationAllowed("dlv_1"); err == nil {
		t.Fatal("terminalMutationAllowed() = nil, want error on definitive failure")
	}

	// ABA mismatched tokens are ignored everywhere.
	mgr.markRenewalFailure(activeLease{DeliveryID: "dlv_1", LeaseToken: "stale_tok"}, waypost.ErrLeaseExpired)
	mgr.updateRenewed("stale_tok", waypost.LeaseRenewResult{
		DeliveryID:     "dlv_1",
		LeaseToken:     "tok_bad",
		LeaseExpiresAt: "2026-04-03T10:05:00Z",
	}, "2026-04-03T10:00:01Z")
	mgr.markTerminal("dlv_1", "stale_tok", "acked", "2026-04-03T10:00:02Z")

	snap := mgr.snapshot()
	if len(snap) != 1 || snap[0].LeaseToken != "tok_1" {
		t.Fatalf("snapshot after stale ABA calls = %+v, want intact tok_1", snap)
	}
	if err := mgr.terminalMutationAllowed("dlv_1"); err == nil {
		t.Fatal("terminalMutationAllowed() = nil after stale updateRenewed/markTerminal")
	}

	// isActive matches only active entry with matching trimmed delivery ID and lease token.
	if !mgr.isActive(" dlv_1 ", " tok_1 ") || mgr.isActive("dlv_1", "wrong") || mgr.isActive("dlv_other", "tok_1") {
		t.Fatal("isActive() returned unexpected ownership verdict")
	}

	// Valid markTerminal clears lastError and active lease while keeping terminal history.
	mgr.markTerminal("dlv_1", "tok_1", "acked", "2026-04-03T10:00:03Z")
	if mgr.hasTrackedLeases() {
		t.Fatal("hasTrackedLeases() = true after markTerminal")
	}
	if err := mgr.lastRenewalError(); err != nil {
		t.Fatalf("lastRenewalError() = %v, want nil after markTerminal", err)
	}
	hist := mgr.historySnapshot(true)
	if len(hist) != 1 || hist[0].Status != "acked" || hist[0].LeaseToken != "" {
		t.Fatalf("historySnapshot(true) = %+v, want acked with cleared token", hist)
	}

	// Re-receive clears stale error on trackReceive.
	mgr.trackReceive(waypost.ReceiveResult{Messages: []waypost.ReceivedMessage{
		{DeliveryID: "dlv_a", RecipientAddress: "codex/self", LeaseToken: "tok_a"},
		{DeliveryID: "dlv_b", RecipientAddress: "codex/self", LeaseToken: "tok_b"},
	}}, "2026-04-03T10:01:00Z")
	mgr.markRenewalFailure(activeLease{DeliveryID: "dlv_a", LeaseToken: "tok_a"}, errors.New("transient"))
	mgr.trackReceive(waypost.ReceiveResult{Messages: []waypost.ReceivedMessage{
		{DeliveryID: "dlv_a", RecipientAddress: "codex/self", LeaseToken: "tok_a_reclaimed"},
	}}, "2026-04-03T10:01:05Z")
	if err := mgr.lastRenewalError(); err != nil {
		t.Fatalf("lastRenewalError() = %v, want nil after re-track", err)
	}

	// Lock-free inspect & no partial mutation if any inspect fails.
	errInspect := errors.New("boom")
	reconcileErr := mgr.reconcileTrackedLeases(context.Background(), "2026-04-03T10:02:00Z", func(_ context.Context, deliveryID string) (waypost.DeliveryLeaseState, error) {
		// Must not deadlock calling snapshot() inside inspect.
		_ = mgr.snapshot()
		if deliveryID == "dlv_a" {
			return waypost.DeliveryLeaseState{Found: true, State: "acked"}, nil
		}
		return waypost.DeliveryLeaseState{}, errInspect
	})
	if !errors.Is(reconcileErr, errInspect) {
		t.Fatalf("reconcileTrackedLeases() err = %v, want %v", reconcileErr, errInspect)
	}
	if len(mgr.snapshot()) != 2 {
		t.Fatalf("snapshot() len = %d, want 2 (no partial mutation on inspect error)", len(mgr.snapshot()))
	}

	// Token superseded concurrently during inspect is not overwritten by stale reconcile verdict.
	reconcileErr = mgr.reconcileTrackedLeases(context.Background(), "2026-04-03T10:03:00Z", func(_ context.Context, deliveryID string) (waypost.DeliveryLeaseState, error) {
		if deliveryID == "dlv_a" {
			mgr.updateRenewed("tok_a_reclaimed", waypost.LeaseRenewResult{
				DeliveryID:     "dlv_a",
				LeaseToken:     "tok_a_renewed",
				LeaseExpiresAt: "2026-04-03T10:08:00Z",
			}, "2026-04-03T10:02:50Z")
			return waypost.DeliveryLeaseState{Found: false}, nil
		}
		return waypost.DeliveryLeaseState{Found: true, State: "acked"}, nil
	})
	if reconcileErr != nil {
		t.Fatalf("reconcileTrackedLeases() err = %v", reconcileErr)
	}
	snap = mgr.snapshot()
	if len(snap) != 1 || snap[0].DeliveryID != "dlv_a" || snap[0].LeaseToken != "tok_a_renewed" {
		t.Fatalf("snapshot() after concurrent renew during reconcile = %+v, want only dlv_a with tok_a_renewed", snap)
	}
}

func TestWaypostRecvGuardSequentialNilSessionAndActiveLeases(t *testing.T) {
	t.Parallel()

	guard := newWaypostRecvGuard()
	t0 := time.Date(2026, 4, 3, 10, 0, 0, 0, time.UTC)

	lease1, err := guard.begin(nil, func() time.Time { return t0 })
	if err != nil {
		t.Fatalf("first begin(nil) error = %v", err)
	}
	lease1.finish("no_message", t0)

	// Sequential call with req == nil (session == nil) within throttle interval
	// must NOT self-evict during prune().
	if _, err := guard.begin(nil, func() time.Time { return t0.Add(50 * time.Millisecond) }); err == nil {
		t.Fatal("second begin(nil) within min interval = nil, want throttle error")
	}

	// Active-leases early return passes "" to finishRecv, preserving hasNoMessage=false.
	tLater := t0.Add(waypostRecvNoMessageMinInterval + time.Second)
	lease2, err := guard.begin(nil, func() time.Time { return tLater })
	if err != nil {
		t.Fatalf("begin(nil) after min interval error = %v", err)
	}
	lease2.finish("received", tLater)
	lease3, err := guard.begin(nil, func() time.Time { return tLater.Add(10 * time.Millisecond) })
	if err != nil {
		t.Fatalf("begin(nil) after received error = %v", err)
	}
	if warn := lease3.finish("", tLater.Add(10*time.Millisecond)); warn {
		t.Fatal("finish(\"\") reported polling warning, want false")
	}
	lease4, err := guard.begin(nil, func() time.Time { return tLater.Add(20 * time.Millisecond) })
	if err != nil {
		t.Fatalf("begin(nil) after finish(\"\") error = %v, want no throttle", err)
	}
	lease4.finish("received", tLater.Add(20*time.Millisecond))
}

func TestRunProbePropagatesCancelAndWrapsTimeout(t *testing.T) {
	t.Parallel()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	runner := &fakeRunner{
		t: t,
		ctxHandler: func(ctx context.Context, _ []string, _ string) (RunResult, error) {
			<-ctx.Done()
			return RunResult{}, ctx.Err()
		},
	}

	if _, err := runProbe(canceledCtx, runner, []string{"probe"}, runOptions{timeout: time.Second}, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("runProbe(canceledCtx, failOnError=false) error = %v, want context.Canceled", err)
	}

	deadlineCtx, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancelDeadline()
	if _, err := runProbe(deadlineCtx, runner, []string{"probe"}, runOptions{timeout: time.Second}, false); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runProbe(deadlineCtx, failOnError=false) error = %v, want context.DeadlineExceeded", err)
	}
	if _, err := runProbe(deadlineCtx, runner, []string{"probe"}, runOptions{timeout: time.Second}, true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runProbe(deadlineCtx, failOnError=true) error = %v, want context.DeadlineExceeded", err)
	}

	_, err := runProbe(context.Background(), runner, []string{"probe"}, runOptions{timeout: 15 * time.Millisecond}, true)
	var cmdTimeout *commandTimeoutError
	if !errors.As(err, &cmdTimeout) {
		t.Fatalf("runProbe(timeout, failOnError=true) error = %v, want *commandTimeoutError", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runProbe(timeout, failOnError=true) error = %v, want distinct per-command timeout instead of context.DeadlineExceeded", err)
	}
	if err == nil || !strings.Contains(err.Error(), "timed out after") {
		t.Fatalf("runProbe(timeout, failOnError=true) error = %v, want timed out message", err)
	}
	_, failFalseErr := runProbe(context.Background(), runner, []string{"probe"}, runOptions{timeout: 15 * time.Millisecond}, false)
	if !errors.As(failFalseErr, &cmdTimeout) || !strings.Contains(failFalseErr.Error(), "command failed: probe :: timed out after") {
		t.Fatalf("runProbe(timeout, failOnError=false) error = %v, want *commandTimeoutError with command context", failFalseErr)
	}

	noTimeoutRunner := &fakeRunner{
		t: t,
		ctxHandler: func(_ context.Context, _ []string, _ string) (RunResult, error) {
			return RunResult{}, context.DeadlineExceeded
		},
	}
	_, noTimeoutErr := execSubprocess(context.Background(), noTimeoutRunner, []string{"probe"}, runOptions{timeout: 0})
	if !errors.Is(noTimeoutErr, context.DeadlineExceeded) {
		t.Fatalf("execSubprocess(timeout=0) error = %v, want context.DeadlineExceeded", noTimeoutErr)
	}
	var zeroTimeoutErr *commandTimeoutError
	if errors.As(noTimeoutErr, &zeroTimeoutErr) {
		t.Fatalf("execSubprocess(timeout=0) unexpectedly returned *commandTimeoutError: %+v", zeroTimeoutErr)
	}
}

func TestSelectSessionHostSurfacesProbeTimeoutWarning(t *testing.T) {
	isolateAutoBindEnv(t)
	t.Setenv("THURBOX_SESSION", "not-a-uuid")

	svc := newService(Options{
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, _ string) (RunResult, error) {
			if len(args) >= 3 && args[0] == "agent-deck" && args[1] == "session" && args[2] == "current" {
				return RunResult{}, &commandTimeoutError{timeout: 15 * time.Millisecond}
			}
			return RunResult{}, nil
		}},
		DisableLeaseRenewLoop: true,
		DisableWakeScheduler:  true,
	})
	defer svc.Close()

	_, err := svc.sessions.selectSessionHost(context.Background(), "")
	if err == nil {
		t.Fatal("selectSessionHost() = nil, want error")
	}
	if !strings.Contains(err.Error(), "session host is unknown; set host explicitly") ||
		!strings.Contains(err.Error(), "THURBOX_SESSION is set but is not a valid") ||
		!strings.Contains(err.Error(), "agent-deck session current timed out") {
		t.Fatalf("selectSessionHost() error = %q, want unknown-host message with thurbox and agent-deck warnings", err.Error())
	}
}

func TestNotifierFixesStage2(t *testing.T) {
	t.Parallel()

	// classifyAgentDeckNudgeResult preserves typed/no_evidence unconfirmed contract
	// while treating submitted/unknown with success:false as failed.
	for _, tc := range []struct {
		name    string
		payload agentDeckNudgeResult
		exit    int
		want    string
	}{
		{name: "submitted bool with success false", payload: agentDeckNudgeResult{Submitted: boolPtr(true), Success: boolPtr(false)}, exit: 1, want: "failed"},
		{name: "submitted delivery with success false", payload: agentDeckNudgeResult{Delivery: "submitted", Success: boolPtr(false)}, exit: 1, want: "failed"},
		{name: "submitted delivery with state failed", payload: agentDeckNudgeResult{Delivery: "submitted", State: "failed"}, exit: 1, want: "failed"},
		{name: "unknown delivery with success false", payload: agentDeckNudgeResult{Delivery: "custom", Success: boolPtr(false)}, exit: 1, want: "failed"},
		{name: "typed with success false stays unconfirmed", payload: agentDeckNudgeResult{Delivery: "typed", Success: boolPtr(false)}, exit: 1, want: "unconfirmed"},
		{name: "no_evidence with success false stays unconfirmed", payload: agentDeckNudgeResult{Delivery: "no_evidence", Success: boolPtr(false)}, exit: 1, want: "unconfirmed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyAgentDeckNudgeResult("agent-deck", RunResult{ExitCode: tc.exit}, tc.payload)
			if got.Status != tc.want {
				t.Fatalf("Status = %q, want %q", got.Status, tc.want)
			}
		})
	}

	// aggregateGroupSubscriberNotifications mixed unconfirmed + target_not_found -> unconfirmed.
	mixed := aggregateGroupSubscriberNotifications([]groupSubscriberNotificationResult{
		{attempted: true, schemes: []string{"agent-deck"}, status: "unconfirmed"},
		{attempted: true, schemes: []string{"agent-deck"}, status: "target_not_found"},
	})
	if mixed.Status != "unconfirmed" {
		t.Fatalf("mixed aggregate status = %q, want unconfirmed", mixed.Status)
	}

	// notifyGroupSubscriber skips any local bound address, not only FromAddress.
	state := &serverState{
		boundAddresses: []string{"codex/self", "agent-deck/local-ad"},
		defaultSender:  "codex/self",
		mode:           bindingModeManual,
	}
	sessions := newSessionManager(&fakeRunner{t: t, handler: func(args []string, _ string) (RunResult, error) {
		t.Fatalf("unexpected notification command for local address: %v", args)
		return RunResult{}, nil
	}}, state)
	mgr := newNotificationManager(sessions.runner, sessions)
	res := mgr.notifyGroupSubscriber(context.Background(), waypostSendInput{FromAddress: "codex/self"}, "  agent-deck/local-ad ")
	if res.attempted {
		t.Fatalf("notifyGroupSubscriber(local sibling) attempted = true, want false")
	}
}

func TestReverifyStartedHostSessionReturnsRefreshedFields(t *testing.T) {
	t.Parallel()

	payload, err := json.Marshal(map[string]any{
		"id":     "sess-1",
		"title":  "worker",
		"status": "error",
		"path":   "/refreshed/path",
	})
	if err != nil {
		t.Fatal(err)
	}
	svc := newService(Options{
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, _ string) (RunResult, error) {
			return RunResult{Stdout: string(payload)}, nil
		}},
		DisableLeaseRenewLoop: true,
		DisableWakeScheduler:  true,
	})
	defer svc.Close()

	previous := &hostSessionData{
		Host:   sessionHostAgentDeck,
		ID:     "sess-1",
		Name:   "worker",
		Status: "running",
		Path:   "/old/path",
	}
	out, err := svc.sessions.reverifyStartedHostSession(context.Background(), sessionHostAgentDeck, previous, "worker", "/expected/path", "/expected/path")
	if err != nil {
		t.Fatalf("reverifyStartedHostSession() error = %v", err)
	}
	if out["session_status"] != "error" {
		t.Fatalf("session_status = %v, want refreshed 'error'", out["session_status"])
	}
	if out["path"] != "/refreshed/path" {
		t.Fatalf("path = %v, want refreshed '/refreshed/path'", out["path"])
	}
}

func TestWakeSchedulerStage2Fixes(t *testing.T) {
	isolateAutoBindEnv(t)

	now := time.Date(2026, 4, 3, 10, 0, 0, 0, time.UTC)
	svc := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{
			t: t,
			listClaimableFunc: func(_ context.Context, _ []string) ([]waypost.ClaimableAddress, error) {
				return []waypost.ClaimableAddress{{
					Address:          "agent-deck/worker",
					ClaimableCount:   1,
					OldestEligibleAt: now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
				}}, nil
			},
		}},
		Now:                   func() time.Time { return now },
		DisableLeaseRenewLoop: true,
		DisableWakeScheduler:  true,
	})
	defer svc.Close()

	svc.state.setManualBinding([]string{"agent-deck/worker"}, "agent-deck/worker", "/tmp")

	// Note: s.server is nil; calling emitWaypostOverviewUpdated & tryWakeChannel
	// must NOT instantiate s.server.
	if outcome := svc.emitWaypostOverviewUpdated(context.Background()); outcome.Status != "unsupported" {
		t.Fatalf("emitWaypostOverviewUpdated with nil server = %q, want unsupported", outcome.Status)
	}
	if svc.existingServer() != nil {
		t.Fatal("emitWaypostOverviewUpdated lazily initialized s.server")
	}
	if svc.tryWakeChannel(context.Background(), wakeSnapshot{
		ScopeID:            "local/agent-deck/worker",
		PendingSince:       now.Add(-2 * time.Minute).Format(time.RFC3339Nano),
		QueuedVisibleCount: 1,
	}, wakeRuntime{}, now, WakeHintMCPResourceUpdated) {
		t.Fatal("tryWakeChannel with nil server unexpectedly succeeded")
	}
	if svc.existingServer() != nil {
		t.Fatal("tryWakeChannel lazily initialized s.server")
	}

	// Connect a live subscribed session so WakeHintMCPResourceUpdated succeeds
	// and takes the early return path; stale runtime scopes must still be pruned.
	clientSession, cleanup := connectTestClientSession(t, svc.Server(), nil)
	defer cleanup()
	if err := clientSession.Subscribe(context.Background(), &mcp.SubscribeParams{URI: waypostOverviewURI}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	emitterCalled := false
	svc.waypostOverviewEmitter = func(context.Context) notificationOutcome {
		emitterCalled = true
		return notificationOutcome{Status: "sent", Scheme: string(WakeHintMCPResourceUpdated)}
	}
	svc.wakeSchedulerState.mu.Lock()
	svc.wakeSchedulerState.runtimes["stale/scope"] = wakeRuntime{ScopeID: "stale/scope"}
	svc.wakeSchedulerState.mu.Unlock()

	if err := svc.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler() error = %v", err)
	}
	if !emitterCalled {
		t.Fatal("waypostOverviewEmitter was not called; early-return path not exercised")
	}
	svc.wakeSchedulerState.mu.Lock()
	_, staleExists := svc.wakeSchedulerState.runtimes["stale/scope"]
	svc.wakeSchedulerState.mu.Unlock()
	if staleExists {
		t.Fatal("stale/scope runtime was not pruned before early channel wake return")
	}

	// Mixed timezone offsets compare chronologically.
	if !isEarlierRFC3339("2026-04-03T12:00:00+02:00", "2026-04-03T10:30:00Z") {
		t.Fatal("12:00:00+02:00 (10:00Z) should be earlier than 10:30:00Z")
	}
	if isEarlierRFC3339("", "2026-04-03T10:30:00Z") {
		t.Fatal("empty candidate must not be considered earlier")
	}
}

func TestClassifyAgentDeckNudgeResultEdgeTruthTable(t *testing.T) {
	t.Parallel()

	f := false
	cases := []struct {
		name       string
		result     RunResult
		payload    agentDeckNudgeResult
		wantStatus string
		wantQueued bool
		wantDetail string
	}{
		{
			name:       "queued with failed state",
			result:     RunResult{ExitCode: 1},
			payload:    agentDeckNudgeResult{Delivery: "queued", State: "failed", Error: "queue rejected"},
			wantStatus: "failed",
		},
		{
			name:       "submitted true contradicts submitted false",
			result:     RunResult{ExitCode: 0},
			payload:    agentDeckNudgeResult{Delivery: "submitted", Submitted: &f},
			wantStatus: "unconfirmed",
			wantDetail: "agent-deck returned contradictory submission verdict",
		},
		{
			name:       "typed stays unconfirmed despite success false",
			result:     RunResult{ExitCode: 1},
			payload:    agentDeckNudgeResult{Delivery: "typed", Success: &f},
			wantStatus: "unconfirmed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome := classifyAgentDeckNudgeResult("agent-deck", tc.result, tc.payload)
			if outcome.Status != tc.wantStatus {
				t.Fatalf("Status = %q, want %q (outcome=%+v)", outcome.Status, tc.wantStatus, outcome)
			}
			if outcome.Queued != tc.wantQueued {
				t.Fatalf("Queued = %t, want %t", outcome.Queued, tc.wantQueued)
			}
			if tc.wantDetail != "" && outcome.Detail != tc.wantDetail {
				t.Fatalf("Detail = %q, want %q", outcome.Detail, tc.wantDetail)
			}
		})
	}
}

func TestAgentDeckProbeTimeoutIsNonRetryable(t *testing.T) {
	t.Parallel()

	attempts := 0
	runner := &fakeRunner{t: t, handler: func(args []string, _ string) (RunResult, error) {
		attempts++
		return RunResult{}, context.DeadlineExceeded
	}}
	sessions := newSessionManager(runner, &serverState{})
	mgr := newNotificationManager(runner, sessions)
	mgr.retryWait = func(context.Context, time.Duration) error {
		t.Fatal("retryWait must not be called on command timeout")
		return nil
	}

	probe := mgr.probeRouteWithRetry(context.Background(), notificationRoute{Manager: "agent-deck", Target: "worker"})
	if attempts != 1 {
		t.Fatalf("probe attempts = %d, want 1 (no 4x retry on timeout)", attempts)
	}
	if probe.Status != "failed" {
		t.Fatalf("probe.Status = %q, want failed", probe.Status)
	}
	var cmdTimeout *commandTimeoutError
	if !errors.As(probe.Err, &cmdTimeout) {
		t.Fatalf("probe.Err = %v, want *commandTimeoutError", probe.Err)
	}
	if notificationProbeRetryable(probe) {
		t.Fatal("notificationProbeRetryable = true on *commandTimeoutError, want false")
	}
}

func TestActiveLeaseManagerIndexAndMonotonicFailures(t *testing.T) {
	t.Parallel()

	mgr := newActiveLeaseManager()
	mgr.trackReceive(waypost.ReceiveResult{
		Messages: []waypost.ReceivedMessage{
			{DeliveryID: " dlv_1 ", LeaseToken: " tok_1 ", RecipientAddress: "agent-deck/self"},
			{DeliveryID: "dlv_2", LeaseToken: "tok_2", RecipientAddress: "agent-deck/self"},
		},
	}, "2026-04-03T10:00:00Z")

	if len(mgr.activeIDs) != 2 {
		t.Fatalf("activeIDs len = %d, want 2", len(mgr.activeIDs))
	}

	// Blank token must be rejected by markTerminal.
	mgr.markTerminal("dlv_1", "   ", "acked", "2026-04-03T10:01:00Z")
	if len(mgr.activeIDs) != 2 {
		t.Fatalf("activeIDs after blank token markTerminal = %d, want 2", len(mgr.activeIDs))
	}

	// Definitive failure is not downgraded by a subsequent transient error.
	mgr.markRenewalFailure(activeLease{DeliveryID: " dlv_1 ", LeaseToken: " tok_1 "}, waypost.ErrLeaseExpired)
	mgr.markRenewalFailure(activeLease{DeliveryID: "dlv_1", LeaseToken: "tok_1"}, errors.New("transient sqlite busy"))
	var failure *leaseRenewalFailure
	if !errors.As(mgr.lastRenewalError(), &failure) || !failure.Definitive {
		t.Fatalf("lastRenewalError = %#v, want Definitive=true preserved", mgr.lastRenewalError())
	}

	// Settling dlv_1 with trimmed inputs recomputes lastError back to nil.
	mgr.markTerminal(" dlv_1 ", " tok_1 ", "acked", "2026-04-03T10:02:00Z")
	if len(mgr.activeIDs) != 1 {
		t.Fatalf("activeIDs len after markTerminal = %d, want 1", len(mgr.activeIDs))
	}
	if mgr.lastRenewalError() != nil {
		t.Fatalf("lastRenewalError after terminal = %v, want nil", mgr.lastRenewalError())
	}

	// Updating renewal on dlv_2 clears a prior failure and recomputes lastError.
	mgr.markRenewalFailure(activeLease{DeliveryID: "dlv_2", LeaseToken: "tok_2"}, errors.New("transient"))
	if mgr.lastRenewalError() == nil {
		t.Fatal("lastRenewalError = nil, want transient failure")
	}
	mgr.updateRenewed(" tok_2 ", waypost.LeaseRenewResult{
		DeliveryID:     " dlv_2 ",
		LeaseToken:     " tok_2b ",
		LeaseExpiresAt: "2026-04-03T10:10:00Z",
	}, "2026-04-03T10:03:00Z")
	if mgr.lastRenewalError() != nil {
		t.Fatalf("lastRenewalError after updateRenewed = %v, want nil", mgr.lastRenewalError())
	}
}

func TestIsEarlierRFC3339HardenedAgainstCorruptStrings(t *testing.T) {
	t.Parallel()

	if isEarlierRFC3339("0000-corrupt", "2026-04-03T10:30:00Z") {
		t.Fatal("corrupt candidate beat valid RFC3339 timestamp")
	}
	if !isEarlierRFC3339("2026-04-03T10:30:00Z", "0000-corrupt") {
		t.Fatal("valid RFC3339 candidate did not beat corrupt current string")
	}
	if isEarlierRFC3339("aaa", "bbb") {
		t.Fatal("corrupt pair should return false")
	}
}

func TestMCPNilParamsAndMultiClaimRecvGuards(t *testing.T) {
	t.Parallel()

	fake := &fakeWaypostService{
		t: t,
		receiveBatchWithTTLFunc: func(context.Context, waypost.ReceiveBatchParams, time.Duration) (waypost.ReceiveResult, error) {
			return waypost.ReceiveResult{
				Messages: []waypost.ReceivedMessage{
					{DeliveryID: "dlv_multi_1", LeaseToken: "tok_1", RecipientAddress: "agent-deck/self"},
					{DeliveryID: "dlv_multi_2", LeaseToken: "tok_2", RecipientAddress: "agent-deck/self"},
				},
			}, nil
		},
	}
	svc := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: fake},
		CommandRunner:         &fakeRunner{t: t},
		DisableLeaseRenewLoop: true,
		DisableWakeScheduler:  true,
	})
	defer svc.Close()

	if _, err := svc.readWaypostOverviewResource(context.Background(), &mcp.ReadResourceRequest{}); err == nil {
		t.Fatal("readWaypostOverviewResource with nil Params = nil, want error")
	}
	if err := svc.subscribeResource(context.Background(), &mcp.SubscribeRequest{}); err == nil {
		t.Fatal("subscribeResource with nil Params = nil, want error")
	}
	if err := svc.unsubscribeResource(context.Background(), &mcp.UnsubscribeRequest{}); err == nil {
		t.Fatal("unsubscribeResource with nil Params = nil, want error")
	}
	if _, err := validateGenericRequireSessionArgs(&mcp.CallToolRequest{}, sessionRequireInput{}); err == nil {
		t.Fatal("validateGenericRequireSessionArgs with nil Params = nil, want error")
	}

	svc.state.boundAddresses = []string{"agent-deck/self"}
	svc.state.mode = bindingModeAutoHostBound
	if _, _, err := svc.waypostRecv(context.Background(), &mcp.CallToolRequest{}, waypostRecvInput{}); err == nil {
		t.Fatal("waypostRecv with 2 messages = nil, want unexpected delivery count error")
	}
	if tracked := svc.activeLeases.snapshot(); len(tracked) != 2 {
		t.Fatalf("tracked leases after multi-claim error = %d, want 2 so leases are renewed", len(tracked))
	}
}

func TestSessionManagerBatchCEdgeCases(t *testing.T) {
	isolateAutoBindEnv(t)

	// 1. setCLISuppressed resets stale detected state & empty fromAddress clears addresses.
	state := &serverState{
		boundAddresses:           []string{"agent-deck/old"},
		defaultSender:            "agent-deck/old",
		defaultWorkdir:           "/tmp/old",
		autoBindWarnings:         []string{"stale warning"},
		detectedAgentDeckSession: "old",
		detectedThurboxSession:   thurboxAuthorID,
		detectedToolSessions:     toolSessionIDs{"claude": "c1"},
	}
	state.setCLISuppressed("")
	if len(state.boundAddresses) != 0 || state.defaultSender != "" || state.defaultWorkdir != "" ||
		len(state.autoBindWarnings) != 0 || state.detectedAgentDeckSession != "" ||
		state.detectedThurboxSession != "" || state.detectedToolSessions != nil {
		t.Fatalf("setCLISuppressed(\"\") left stale state: %+v", state)
	}

	// 2. Replacing old thurbox/... on ID change and skipping SQLite scan when claude is bound.
	home := t.TempDir()
	workdir := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, ".local", "share"))
	t.Setenv("AGENTDECK_PROFILE", "work")
	writeAgentDeckStateDB(t, home, "work", "deck-1", workdir, "codex-should-not-bind")

	t.Setenv("THURBOX_SESSION", thurboxReviewID)
	mgr := newSessionManager(&fakeRunner{t: t}, &serverState{
		mode:                     bindingModeAutoHostBound,
		boundAddresses:           []string{"thurbox/" + thurboxAuthorID, "agent-deck/deck-1", "claude/claude-1"},
		defaultSender:            "thurbox/" + thurboxAuthorID,
		defaultWorkdir:           workdir,
		detectedAgentDeckSession: "deck-1",
		detectedThurboxSession:   thurboxAuthorID,
		detectedToolSessions:     toolSessionIDs{"claude": "claude-1"},
	})
	if err := mgr.tryAutoBindCurrentSession(context.Background()); err != nil {
		t.Fatalf("tryAutoBindCurrentSession() error = %v", err)
	}
	snap := mgr.snapshotState()
	if slices.Contains(snap.BoundAddresses, "thurbox/"+thurboxAuthorID) {
		t.Fatalf("stale thurbox address remained in %v", snap.BoundAddresses)
	}
	if !slices.Contains(snap.BoundAddresses, "thurbox/"+thurboxReviewID) {
		t.Fatalf("new thurbox address missing from %v", snap.BoundAddresses)
	}
	if snap.DetectedToolSessions["codex"] != "" {
		t.Fatalf("SQLite scan should be skipped when claude session is bound, got %+v", snap.DetectedToolSessions)
	}

	// 3. tryUpgradeAgentDeckBinding preserves non-Agent-Deck warnings across retry.
	runner := &fakeRunner{t: t, handler: func(args []string, _ string) (RunResult, error) {
		return RunResult{ExitCode: 0, Stdout: `{"id":"deck-upgraded"}`}, nil
	}}
	upgradeMgr := newSessionManager(runner, &serverState{
		mode:                 bindingModeAutoToolFallback,
		boundAddresses:       []string{"claude/claude-1"},
		defaultSender:        "claude/claude-1",
		detectedToolSessions: toolSessionIDs{"claude": "claude-1"},
		autoBindWarnings:     []string{"thurbox warning preserved", "agent-deck old probe error"},
	})
	if err := upgradeMgr.tryUpgradeAgentDeckBinding(context.Background(), upgradeMgr.snapshotState()); err != nil {
		t.Fatalf("tryUpgradeAgentDeckBinding() error = %v", err)
	}
	upgraded := upgradeMgr.snapshotState()
	if !slices.Contains(upgraded.AutoBindWarnings, "thurbox warning preserved") {
		t.Fatalf("AutoBindWarnings = %v, want thurbox warning preserved", upgraded.AutoBindWarnings)
	}
	if slices.Contains(upgraded.AutoBindWarnings, "agent-deck old probe error") {
		t.Fatalf("stale agent-deck warning remained: %v", upgraded.AutoBindWarnings)
	}
}

func TestAutoBindAndUpgradeDegradeCleanlyOnCommandTimeout(t *testing.T) {
	isolateAutoBindEnv(t)
	const codexID = "019cf503-5555-7000-8000-000000000001"
	t.Setenv("CODEX_THREAD_ID", codexID)

	timeoutRunner := &fakeRunner{
		t: t,
		ctxHandler: func(_ context.Context, args []string, _ string) (RunResult, error) {
			if slices.Equal(args, []string{"agent-deck", "session", "current", "--json"}) {
				return RunResult{}, context.DeadlineExceeded
			}
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		},
	}

	// Non-Thurbox auto-bind degrades to codex fallback instead of failing and locking out waypost_bind.
	mgr := newSessionManager(timeoutRunner, &serverState{})
	if err := mgr.tryAutoBindCurrentSession(context.Background()); err != nil {
		t.Fatalf("tryAutoBindCurrentSession(agent-deck timeout) error = %v, want graceful fallback", err)
	}
	snap := mgr.snapshotState()
	if snap.BindingMode != bindingModeAutoToolFallback || !slices.Contains(snap.BoundAddresses, "codex/"+codexID) {
		t.Fatalf("snapshot after timeout = %+v, want auto_tool_fallback with codex address", snap)
	}
	if !slices.ContainsFunc(snap.AutoBindWarnings, func(w string) bool {
		return strings.Contains(w, "agent-deck session current timed out")
	}) {
		t.Fatalf("AutoBindWarnings = %v, want timeout warning", snap.AutoBindWarnings)
	}

	// Already-bound tool-fallback sessions also stay usable when upgrade probe times out.
	if err := mgr.tryUpgradeAgentDeckBinding(context.Background(), snap); err != nil {
		t.Fatalf("tryUpgradeAgentDeckBinding(agent-deck timeout) error = %v, want graceful warning", err)
	}
	if got := mgr.snapshotState(); !slices.Contains(got.BoundAddresses, "codex/"+codexID) {
		t.Fatalf("bound addresses lost after upgrade timeout: %+v", got)
	}
}

func TestLeaseSafetyGatesAndRecvGuardAndThurboxTimeout(t *testing.T) {
	t.Parallel()

	// 1. terminalMutationAllowed trims whitespace and updateRenewed rejects blank newToken.
	mgr := newActiveLeaseManager()
	mgr.trackReceive(waypost.ReceiveResult{
		Messages: []waypost.ReceivedMessage{
			{DeliveryID: "dlv_1", LeaseToken: "tok_1", RecipientAddress: "agent-deck/self"},
		},
	}, "2026-04-03T10:00:00Z")
	mgr.markRenewalFailure(activeLease{DeliveryID: "dlv_1", LeaseToken: "tok_1"}, waypost.ErrLeaseExpired)
	if err := mgr.terminalMutationAllowed(" dlv_1 "); err == nil {
		t.Fatal("terminalMutationAllowed(\" dlv_1 \") = nil, want definitive failure error")
	}
	mgr.updateRenewed("tok_1", waypost.LeaseRenewResult{
		DeliveryID:     "dlv_1",
		LeaseToken:     "   ",
		LeaseExpiresAt: "2026-04-03T10:10:00Z",
	}, "2026-04-03T10:01:00Z")
	if got := mgr.snapshot()[0].LeaseToken; got != "tok_1" {
		t.Fatalf("LeaseToken = %q after blank renewal token, want tok_1", got)
	}

	// 2. nil-session throttle in waypostRecvGuard survives intervening real-session begin calls.
	guard := newWaypostRecvGuard()
	t0 := time.Date(2026, 4, 3, 10, 0, 0, 0, time.UTC)
	leaseNil, err := guard.begin(nil, func() time.Time { return t0 })
	if err != nil {
		t.Fatalf("begin(nil) error = %v", err)
	}
	leaseNil.finish("no_message", t0)

	realReq := &mcp.CallToolRequest{Session: &mcp.ServerSession{}}
	leaseReal, err := guard.begin(realReq, func() time.Time { return t0.Add(50 * time.Millisecond) })
	if err != nil {
		t.Fatalf("begin(realReq) error = %v", err)
	}
	leaseReal.finish("received", t0.Add(50*time.Millisecond))

	if _, err := guard.begin(nil, func() time.Time { return t0.Add(100 * time.Millisecond) }); err == nil {
		t.Fatal("second begin(nil) after realReq = nil error, want throttle error preserved")
	}

	// 3. thurboxNotifier.Notify treats pre-canceled context as failed and post-start timeout as unconfirmed.
	notifier := thurboxNotifier{
		runner: &fakeRunner{t: t, ctxHandler: func(_ context.Context, _ []string, _ string) (RunResult, error) {
			return RunResult{}, context.DeadlineExceeded
		}},
	}
	event := notificationEvent{Kind: notificationDelivery, Route: notificationRoute{Target: thurboxAuthorID}}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if out := notifier.Notify(canceledCtx, event); out.Status != "failed" {
		t.Fatalf("thurbox Notify(pre-canceled) Status = %q, want failed", out.Status)
	}
	if out := notifier.Notify(context.Background(), event); out.Status != "unconfirmed" {
		t.Fatalf("thurbox Notify(post-start timeout) Status = %q, want unconfirmed", out.Status)
	}

	// 4. agentDeckNotifier.runNudge keeps ExitCode=0, runErr=nil as "sent" even if ctx cancels as Run returns.
	parentCtx, cancelParent := context.WithCancel(context.Background())
	raceRunner := &fakeRunner{t: t, handler: func(_ []string, _ string) (RunResult, error) {
		cancelParent()
		return RunResult{ExitCode: 0, Stdout: "Sent"}, nil
	}}
	if out := (agentDeckNotifier{runner: raceRunner}).runNudge(parentCtx, []string{"agent-deck", "session", "send", "--no-wait", "s1", "hi"}); out.Status != "sent" {
		t.Fatalf("agentDeck runNudge(post-completion ctx cancel) Status = %q, want sent", out.Status)
	}
}

func TestProcessLeaseRenewalsRetriesErrLeaseRenewChangedWhenStillOwned(t *testing.T) {
	t.Parallel()

	t.Run("transient concurrent same-token extension retries and succeeds", func(t *testing.T) {
		t.Parallel()

		fakeSvc := &fakeWaypostService{t: t}
		renewCalls := 0
		fakeSvc.renewFunc = func(_ context.Context, deliveryID, leaseToken string, _ time.Duration) (waypost.LeaseRenewResult, error) {
			renewCalls++
			if renewCalls == 1 {
				return waypost.LeaseRenewResult{}, fmt.Errorf("delivery changed: %w", waypost.ErrLeaseRenewChanged)
			}
			return waypost.LeaseRenewResult{
				DeliveryID:     deliveryID,
				LeaseToken:     leaseToken,
				LeaseExpiresAt: "2026-04-03T10:15:00Z",
			}, nil
		}
		fakeSvc.inspectLeaseFunc = func(context.Context, string) (waypost.DeliveryLeaseState, error) {
			return waypost.DeliveryLeaseState{Found: true, State: "leased", LeaseToken: "tok_1"}, nil
		}

		svc := newService(Options{
			WaypostServiceFactory: fakeWaypostServiceFactory{service: fakeSvc},
			DisableLeaseRenewLoop: true,
			DisableWakeScheduler:  true,
		})
		defer svc.Close()

		svc.activeLeases.trackReceive(waypost.ReceiveResult{Messages: []waypost.ReceivedMessage{{
			DeliveryID: "dlv_1", LeaseToken: "tok_1", RecipientAddress: "agent-deck/self",
		}}}, "2026-04-03T10:00:00Z")

		if err := svc.processLeaseRenewals(context.Background()); err != nil {
			t.Fatalf("processLeaseRenewals() error = %v, want nil after retry", err)
		}
		if renewCalls != 2 {
			t.Fatalf("renewCalls = %d, want 2", renewCalls)
		}
		if err := svc.activeLeases.terminalMutationAllowed("dlv_1"); err != nil {
			t.Fatalf("terminalMutationAllowed() = %v, want nil", err)
		}
		if snap := svc.activeLeases.snapshot(); len(snap) != 1 || snap[0].LeaseExpiresAt != "2026-04-03T10:15:00Z" {
			t.Fatalf("snapshot = %+v, want updated expiry", snap)
		}
	})

	t.Run("repeated ErrLeaseRenewChanged while still owned does not block terminal mutation", func(t *testing.T) {
		t.Parallel()

		fakeSvc := &fakeWaypostService{t: t}
		renewCalls := 0
		fakeSvc.renewFunc = func(_ context.Context, _ string, _ string, _ time.Duration) (waypost.LeaseRenewResult, error) {
			renewCalls++
			return waypost.LeaseRenewResult{}, fmt.Errorf("delivery changed: %w", waypost.ErrLeaseRenewChanged)
		}
		fakeSvc.inspectLeaseFunc = func(context.Context, string) (waypost.DeliveryLeaseState, error) {
			return waypost.DeliveryLeaseState{Found: true, State: "leased", LeaseToken: "tok_1"}, nil
		}

		svc := newService(Options{
			WaypostServiceFactory: fakeWaypostServiceFactory{service: fakeSvc},
			DisableLeaseRenewLoop: true,
			DisableWakeScheduler:  true,
		})
		defer svc.Close()

		svc.activeLeases.trackReceive(waypost.ReceiveResult{Messages: []waypost.ReceivedMessage{{
			DeliveryID: "dlv_1", LeaseToken: "tok_1", RecipientAddress: "agent-deck/self",
		}}}, "2026-04-03T10:00:00Z")

		if err := svc.processLeaseRenewals(context.Background()); err == nil {
			t.Fatal("processLeaseRenewals() = nil, want non-definitive renewal error recorded")
		}
		if err := svc.activeLeases.terminalMutationAllowed("dlv_1"); err != nil {
			t.Fatalf("terminalMutationAllowed() = %v, want nil when reconciliation confirmed ownership", err)
		}
		if err := svc.activeLeases.lastRenewalError(); err == nil {
			t.Fatal("lastRenewalError() = nil, want transient ErrLeaseRenewChanged surfaced in status")
		}
	})
}
