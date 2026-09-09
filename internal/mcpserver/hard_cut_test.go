package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ruiheng/waypost/internal/version"
	"github.com/ruiheng/waypost/internal/waypost"
)

func TestWaypostStatusReportsAuthoritativeCLIContext(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	executable := filepath.Join(t.TempDir(), "bin", "waypost")
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         &fakeRunner{t: t, handler: func([]string, string) (RunResult, error) { return RunResult{}, nil }},
		StateDir:              stateDir,
		Executable:            executable,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.autoBindAttempted = true
	service.state.boundAddresses = []string{"agent-deck/self", "codex/self"}
	service.state.defaultSender = "agent-deck/self"

	clientSession, cleanup := connectTestClientSession(t, service.Server(), nil)
	defer cleanup()
	serverInfo := clientSession.InitializeResult().ServerInfo
	if serverInfo == nil || serverInfo.Version != version.Version {
		t.Fatalf("MCP server info = %v, want version %q", serverInfo, version.Version)
	}
	compact := callServiceTool(t, service, "waypost_status", map[string]any{})
	if compact["status"] != "ready" {
		t.Fatalf("compact status = %v, want ready", compact)
	}
	for _, field := range []string{"executable", "resolved_state_dir", "active_lease_count", "default_workdir"} {
		if _, ok := compact[field]; ok {
			t.Fatalf("compact status unexpectedly includes %q: %v", field, compact)
		}
	}

	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_cli_context": true})
	for _, field := range []string{
		"server_version",
		"detected_agent_deck_session_id",
		"detected_thurbox_session_id",
		"detected_tool_session_addresses",
		"detected_agent_session_id",
		"detected_claude_code_session_id",
		"detected_gemini_session_id",
		"detected_opencode_session_id",
		"active_leases",
		"next_cursor",
		"warnings",
	} {
		if _, ok := status[field]; ok {
			t.Fatalf("default waypost_status unexpectedly includes %q: %v", field, status)
		}
	}
	if _, ok := status["active_lease_count"]; ok {
		t.Fatalf("default waypost_status exposed zero active_lease_count: %v", status)
	}
	if got := status["default_sender"]; got != "agent-deck/self" {
		t.Fatalf("default_sender = %v, want agent-deck/self", got)
	}
	if _, ok := status["default_workdir"]; ok {
		t.Fatalf("default waypost_status exposed empty default_workdir: %v", status)
	}
	if got := status["executable"]; got != executable {
		t.Fatalf("executable = %v, want authoritative executable", got)
	}
	wantStateDir, err := filepath.Abs(stateDir)
	if err != nil {
		t.Fatalf("filepath.Abs(stateDir): %v", err)
	}
	if got := status["resolved_state_dir"]; got != wantStateDir {
		t.Fatalf("resolved_state_dir = %v, want %q", got, wantStateDir)
	}

	diagnostics := callServiceTool(t, service, "waypost_status", map[string]any{
		"include_diagnostics": true,
	})
	if got := diagnostics["server_version"]; got != version.Version {
		t.Fatalf("diagnostic server_version = %v, want %q", got, version.Version)
	}
	if _, ok := diagnostics["detected_agent_deck_session_id"]; !ok {
		t.Fatalf("diagnostic status omits detected fields: %v", diagnostics)
	}
}

func TestWaypostStatusReportsActiveLeaseTokens(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir:             stateDir,
		CommandRunner:        &fakeRunner{t: t, handler: func([]string, string) (RunResult, error) { return RunResult{}, nil }},
		DisableWakeScheduler: true,
		LeaseRenewInterval:   time.Hour,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	sent := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/self",
		"subject": "status lease",
		"body":    "body",
	})
	deliveryID := sent["delivery_id"].(string)
	received := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	leaseToken := received["delivery"].(map[string]any)["lease_token"].(string)

	status := callServiceTool(t, service, "waypost_status", map[string]any{})
	if got := status["active_lease_count"]; got != float64(1) {
		t.Fatalf("active_lease_count = %v, want 1", got)
	}
	if _, ok := status["active_leases"]; ok {
		t.Fatalf("default waypost_status exposed active lease detail: %v", status)
	}

	detailedStatus := callServiceTool(t, service, "waypost_status", map[string]any{
		"include_active_leases": true,
	})
	leases := detailedStatus["active_leases"].([]any)
	if len(leases) != 1 {
		t.Fatalf("active_leases = %v, want one lease", leases)
	}
	lease := leases[0].(map[string]any)
	if got := lease["delivery_id"]; got != deliveryID {
		t.Fatalf("active lease delivery_id = %v, want %q", got, deliveryID)
	}
	if got := lease["lease_token"]; got != leaseToken {
		t.Fatalf("active lease token = %v, want %q", got, leaseToken)
	}
	if got := lease["recipient_address"]; got != "agent-deck/self" {
		t.Fatalf("active lease recipient_address = %v, want agent-deck/self", got)
	}
	if _, ok := lease["lease_expires_at"]; ok {
		t.Fatalf("active lease unexpectedly exposed lease_expires_at: %v", lease)
	}
}

func TestWaypostStatusPaginatesActiveLeaseDetailsWhenRequested(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         &fakeRunner{t: t, handler: func([]string, string) (RunResult, error) { return RunResult{}, nil }},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	messages := []waypost.ReceivedMessage{
		{DeliveryID: "dlv_status_one", LeaseToken: "lease_status_one", RecipientAddress: "agent-deck/self"},
		{DeliveryID: "dlv_status_two", LeaseToken: "lease_status_two", RecipientAddress: "agent-deck/self"},
	}
	service.activeLeases.trackReceive(waypost.ReceiveResult{Messages: messages}, time.Now().UTC().Format(time.RFC3339Nano))
	waypostService.recordLeases(messages)

	first := callServiceTool(t, service, "waypost_status", map[string]any{
		"include_active_leases": true,
		"limit":                 1,
	})
	if got := first["active_lease_count"]; got != float64(2) {
		t.Fatalf("active_lease_count = %v, want 2", got)
	}
	firstLeases := first["active_leases"].([]any)
	if len(firstLeases) != 1 {
		t.Fatalf("first active_leases = %v, want one item", firstLeases)
	}
	cursor, ok := first["next_cursor"].(string)
	if !ok || cursor == "" {
		t.Fatalf("first next_cursor = %v, want cursor", first["next_cursor"])
	}

	second := callServiceTool(t, service, "waypost_status", map[string]any{
		"include_active_leases": true,
		"limit":                 1,
		"cursor":                cursor,
	})
	secondLeases := second["active_leases"].([]any)
	if len(secondLeases) != 1 {
		t.Fatalf("second active_leases = %v, want one item", secondLeases)
	}
	if firstLeases[0].(map[string]any)["delivery_id"] == secondLeases[0].(map[string]any)["delivery_id"] {
		t.Fatalf("pagination repeated lease: first = %v, second = %v", firstLeases, secondLeases)
	}
	if _, ok := second["next_cursor"]; ok {
		t.Fatalf("final active lease page unexpectedly has next_cursor: %v", second)
	}
}

func TestWaypostStatusCLIReplacementForward(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir:              stateDir,
		Executable:            os.Args[0],
		CommandRunner:         &fakeRunner{t: t, handler: func([]string, string) (RunResult, error) { return RunResult{}, nil }},
		NotifyDelay:           -1,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	setBoundTestSender(service, "agent/sender")

	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_cli_context": true})
	executable, ok := status["executable"].(string)
	if !ok || executable == "" {
		t.Fatalf("waypost_status executable = %v, want path", status["executable"])
	}
	resolvedStateDir, ok := status["resolved_state_dir"].(string)
	if !ok || resolvedStateDir == "" {
		t.Fatalf("waypost_status resolved_state_dir = %v, want path", status["resolved_state_dir"])
	}

	sent := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":           "workflow/source",
		"from_address": "agent/sender",
		"subject":      "source",
		"body":         "body",
	})
	deliveryID := sent["delivery_id"].(string)

	cmd := exec.Command(executable,
		"-test.run=TestWaypostCLIHelperProcess", "--",
		"--state-dir", resolvedStateDir,
		"forward", "--delivery", deliveryID, "--to", "workflow/target", "--json",
	)
	cmd.Env = append(os.Environ(), "GO_WANT_WAYPOST_CLI_HELPER=1")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("reported CLI forward failed: %v; stderr = %q", err, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("reported CLI forward stderr = %q, want empty", stderr.String())
	}

	var forwarded map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &forwarded); err != nil {
		t.Fatalf("decode reported CLI forward output: %v; stdout = %q", err, stdout.String())
	}
	if forwarded["source_delivery_id"] != deliveryID || forwarded["delivery_id"] == "" {
		t.Fatalf("reported CLI forward output = %v", forwarded)
	}
}

func TestWaypostCLIHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_WAYPOST_CLI_HELPER") != "1" {
		return
	}

	separator := -1
	for index, arg := range os.Args {
		if arg == "--" {
			separator = index
			break
		}
	}
	if separator == -1 {
		os.Exit(97)
	}

	args := os.Args[separator+1:]
	stateDir := ""
	if len(args) >= 2 && args[0] == "--state-dir" {
		stateDir = args[1]
		args = args[2:]
	}
	app := waypost.NewApp(os.Stdin, os.Stdout, os.Stderr)
	err := app.RunWithStateDir(context.Background(), stateDir, args)
	if err == nil || errors.Is(err, waypost.ErrHelpRequested) {
		os.Exit(0)
	}
	if errors.Is(err, waypost.ErrNoMessage) {
		os.Exit(2)
	}
	if !waypost.WriteCLIJSONError(os.Stderr, args, err) {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
	}
	os.Exit(1)
}

func TestExternalDurableFailReconcilesMCPLeaseHistoryAndStatus(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir:              stateDir,
		CommandRunner:         &fakeRunner{t: t, handler: func([]string, string) (RunResult, error) { return RunResult{}, nil }},
		NotifyDelay:           -1,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	sent := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/self",
		"subject": "external fail",
		"body":    "body",
	})
	deliveryID := sent["delivery_id"].(string)
	received := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	delivery := received["delivery"].(map[string]any)
	leaseToken := delivery["lease_token"].(string)

	_, err := withWaypostService[waypost.DeliveryTransitionResult, *waypost.Operations](context.Background(), service.waypostServices, func(ops *waypost.Operations) (waypost.DeliveryTransitionResult, error) {
		return ops.Fail(context.Background(), deliveryID, leaseToken, "external CLI failure")
	})
	if err != nil {
		t.Fatalf("durable Fail() error = %v", err)
	}

	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_active_leases": true})
	if got := status["active_lease_count"]; got != float64(0) {
		t.Fatalf("active_lease_count after external fail = %v, want 0", got)
	}
	if leases := status["active_leases"].([]any); len(leases) != 0 {
		t.Fatalf("active_leases after external fail = %v, want empty", leases)
	}
	detailedStatus := callServiceTool(t, service, "waypost_status", map[string]any{
		"include_active_leases": true,
	})
	if leases := detailedStatus["active_leases"].([]any); len(leases) != 0 {
		t.Fatalf("active_leases after external fail = %v, want empty", leases)
	}
	if service.activeLeases.hasTrackedLeases() {
		t.Fatal("externally failed lease remains active after waypost_status")
	}

	history := callServiceTool(t, service, "waypost_claim_history", map[string]any{
		"delivery_id":         deliveryID,
		"include_terminal":    true,
		"recover_lease_token": true,
	})
	items := history["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("claim history items = %d, want 1", len(items))
	}
	item := items[0].(map[string]any)
	if got := item["status"]; got != "queued" {
		t.Fatalf("claim history status = %v, want durable queued state", got)
	}
	if got := item["terminal_at"]; got == nil || got == "" {
		t.Fatalf("claim history terminal_at = %v, want durable transition time", got)
	}
	if _, ok := item["lease_token"]; ok {
		t.Fatalf("claim history exposed obsolete lease token: %v", item)
	}
	if err := service.processLeaseRenewals(context.Background()); err != nil {
		t.Fatalf("processLeaseRenewals() after reconciliation = %v", err)
	}
}

func TestTrackedLeaseInspectionErrorDoesNotReturnCachedHint(t *testing.T) {
	inspectErr := context.DeadlineExceeded
	waypostService := &fakeWaypostService{t: t}
	waypostService.inspectLeaseFunc = func(context.Context, string) (waypost.DeliveryLeaseState, error) {
		return waypost.DeliveryLeaseState{}, inspectErr
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         &fakeRunner{t: t, handler: func([]string, string) (RunResult, error) { return RunResult{}, nil }},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true
	service.activeLeases.trackReceive(waypost.ReceiveResult{Messages: []waypost.ReceivedMessage{{
		DeliveryID:       "dlv_inspect",
		LeaseToken:       "lease_inspect",
		LeaseExpiresAt:   time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano),
		RecipientAddress: "agent-deck/self",
	}}}, time.Now().UTC().Format(time.RFC3339Nano))

	err := callServiceToolExpectError(t, service, "waypost_status", map[string]any{})
	if err == nil || !strings.Contains(err.Error(), inspectErr.Error()) {
		t.Fatalf("waypost_status inspection error = %v, want %v", err, inspectErr)
	}
	if service.state.statusToolCalled {
		t.Fatal("waypost_status marked successful after lease inspection failed")
	}

	err = callServiceToolExpectError(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if err == nil || !strings.Contains(err.Error(), inspectErr.Error()) {
		t.Fatalf("waypost_recv inspection error = %v, want %v", err, inspectErr)
	}
}

func TestInvalidStatusPaginationDoesNotOpenStatusGate(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	err := callServiceToolExpectErrorWithoutStatusBootstrap(t, service, "waypost_status", map[string]any{
		"limit": 1,
	})
	if err == nil || !strings.Contains(err.Error(), "include_active_leases") {
		t.Fatalf("waypost_status pagination without active lease details error = %v", err)
	}
	if service.state.statusToolCalled {
		t.Fatal("inactive lease detail pagination opened the status gate")
	}

	err = callServiceToolExpectErrorWithoutStatusBootstrap(t, service, "waypost_status", map[string]any{
		"include_active_leases": true,
		"limit":                 waypost.MaxPageSize + 1,
	})
	if err == nil || !strings.Contains(err.Error(), "limit must be between") {
		t.Fatalf("waypost_status invalid pagination error = %v", err)
	}
	if service.state.statusToolCalled {
		t.Fatal("invalid waypost_status opened the status gate")
	}

	err = callServiceToolExpectErrorWithoutStatusBootstrap(t, service, "waypost_send", map[string]any{
		"from_address": "codex/source",
		"to":           "codex/target",
		"subject":      "hello",
		"body":         "body",
	})
	if err == nil || !strings.Contains(err.Error(), "waypost_status") {
		t.Fatalf("waypost_send error after failed status = %v, want status gate", err)
	}
}

func TestReceiveRecoveryTracksAndRenewsEveryUnreleasedClaim(t *testing.T) {
	claims := []waypost.ReceivedMessage{
		{
			DeliveryID:       "dlv_recovery_one",
			LeaseToken:       "lease_recovery_one",
			RecipientAddress: "agent-deck/one",
			LeaseExpiresAt:   "2026-07-25T10:00:00Z",
		},
		{
			DeliveryID:       "dlv_recovery_two",
			LeaseToken:       "lease_recovery_two",
			RecipientAddress: "agent-deck/two",
			LeaseExpiresAt:   "2026-07-25T10:01:00Z",
		},
	}
	renewedTokens := map[string]string{
		"dlv_recovery_one": "lease_recovery_one_renewed",
		"dlv_recovery_two": "lease_recovery_two_renewed",
	}
	renewCalls := make(map[string][]string)
	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchWithTTLFunc = func(context.Context, waypost.ReceiveBatchParams, time.Duration) (waypost.ReceiveResult, error) {
		return waypost.ReceiveResult{}, &waypost.ReceiveRecoveryRequiredError{
			Cause:  errors.New("remaining-state query failed"),
			Claims: claims,
		}
	}
	waypostService.renewFunc = func(_ context.Context, deliveryID, leaseToken string, _ time.Duration) (waypost.LeaseRenewResult, error) {
		renewedToken, found := renewedTokens[deliveryID]
		if !found {
			t.Fatalf("unexpected renewal for %q", deliveryID)
		}
		renewCalls[deliveryID] = append(renewCalls[deliveryID], leaseToken)
		return waypost.LeaseRenewResult{
			DeliveryID:     deliveryID,
			LeaseToken:     renewedToken,
			LeaseExpiresAt: "2026-07-25T11:00:00Z",
		}, nil
	}
	waypostService.leaseStates = map[string]waypost.DeliveryLeaseState{}
	for _, claim := range claims {
		waypostService.leaseStates[claim.DeliveryID] = waypost.DeliveryLeaseState{
			Found:      true,
			State:      "leased",
			LeaseToken: claim.LeaseToken,
		}
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         &fakeRunner{t: t, handler: func([]string, string) (RunResult, error) { return RunResult{}, nil }},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.autoBindAttempted = true
	service.state.boundAddresses = []string{"agent-deck/one", "agent-deck/two"}

	output := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses":   []string{"agent-deck/one", "agent-deck/two"},
		"diagnostics": true,
	})
	if output["status"] != "receive_recovery_required" || output["remaining_by_state_status"] != "unavailable" {
		t.Fatalf("receive recovery output = %v", output)
	}
	responseClaims, ok := output["claims"].([]any)
	if !ok || len(responseClaims) != len(claims) {
		t.Fatalf("receive recovery claims = %v, want %d claims", output["claims"], len(claims))
	}
	for index, want := range claims {
		got, ok := responseClaims[index].(map[string]any)
		if !ok {
			t.Fatalf("receive recovery claim[%d] = %T, want map", index, responseClaims[index])
		}
		if got["delivery_id"] != want.DeliveryID || got["lease_token"] != want.LeaseToken || got["recipient_address"] != want.RecipientAddress || got["lease_expires_at"] != want.LeaseExpiresAt {
			t.Fatalf("receive recovery claim[%d] = %v, want %+v", index, got, want)
		}
	}

	tracked := service.activeLeases.snapshot()
	if len(tracked) != len(claims) {
		t.Fatalf("tracked recovery leases = %+v, want %d", tracked, len(claims))
	}
	trackedTokens := make(map[string]string, len(tracked))
	for _, lease := range tracked {
		trackedTokens[lease.DeliveryID] = lease.LeaseToken
	}
	for _, claim := range claims {
		if trackedTokens[claim.DeliveryID] != claim.LeaseToken {
			t.Fatalf("tracked recovery token for %q = %q, want %q", claim.DeliveryID, trackedTokens[claim.DeliveryID], claim.LeaseToken)
		}
	}

	if err := service.processLeaseRenewals(context.Background()); err != nil {
		t.Fatalf("processLeaseRenewals() = %v", err)
	}
	if len(renewCalls) != len(claims) {
		t.Fatalf("renew calls = %v, want every recovery claim", renewCalls)
	}
	for _, claim := range claims {
		callTokens := renewCalls[claim.DeliveryID]
		if len(callTokens) != 1 || callTokens[0] != claim.LeaseToken {
			t.Fatalf("renew calls for %q = %v, want exactly [%q]", claim.DeliveryID, callTokens, claim.LeaseToken)
		}
	}
	trackedAfterRenewal := service.activeLeases.snapshot()
	if len(trackedAfterRenewal) != len(claims) {
		t.Fatalf("tracked leases after renewal = %+v, want %d", trackedAfterRenewal, len(claims))
	}
	for _, lease := range trackedAfterRenewal {
		if want := renewedTokens[lease.DeliveryID]; lease.LeaseToken != want {
			t.Fatalf("renewed tracked lease = %+v, want token %q", lease, want)
		}
	}
}
