package mcpserver

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"weak"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/ruiheng/waypost/internal/waypost"
)

func TestWaypostRecvEnforcesNoMessageIntervalPerConnection(t *testing.T) {
	current := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	calls := 0
	fake := &fakeWaypostService{
		t: t,
		receiveBatchWithTTLFunc: func(context.Context, waypost.ReceiveBatchParams, time.Duration) (waypost.ReceiveResult, error) {
			calls++
			return waypost.ReceiveResult{}, waypost.ErrNoMessage
		},
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: fake},
		CommandRunner:         &fakeRunner{t: t},
		Now:                   func() time.Time { return current },
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.autoBindAttempted = true

	req := &mcp.CallToolRequest{Session: &mcp.ServerSession{}}
	_, firstOutput, err := service.waypostRecv(context.Background(), req, waypostRecvInput{Addresses: []string{"agent-deck/self"}})
	if err != nil {
		t.Fatalf("first recv error = %v", err)
	}
	if firstOutput["status"] != "no_message" {
		t.Fatalf("first recv status = %v, want no_message", firstOutput["status"])
	}
	_, secondOutput, err := service.waypostRecv(context.Background(), req, waypostRecvInput{Addresses: []string{"agent-deck/self"}})
	if err == nil || !strings.Contains(err.Error(), "delivery notification") {
		t.Fatalf("second recv error = %v, want wait-for-notification error", err)
	}
	if strings.Contains(err.Error(), "retry") {
		t.Fatalf("second recv error = %v, must not suggest a retry time", err)
	}
	if secondOutput != nil {
		t.Fatalf("second recv output = %v, want nil on interval error", secondOutput)
	}
	if calls != 1 {
		t.Fatalf("receive calls = %d, want one call before interval rejection", calls)
	}

	current = current.Add(15 * time.Second)
	_, output, err := service.waypostRecv(context.Background(), req, waypostRecvInput{Addresses: []string{"agent-deck/self"}})
	if err != nil {
		t.Fatalf("third recv error = %v", err)
	}
	if output["status"] != "no_message" {
		t.Fatalf("third recv status = %v, want no_message", output["status"])
	}
	warnings, ok := output["warnings"].([]string)
	if !ok || len(warnings) != 1 || !strings.Contains(warnings[0], "meaningless polling") {
		t.Fatalf("third recv warnings = %#v, want meaningless-polling warning", output["warnings"])
	}
}

func TestWaypostRecvNoMessageWarningUsesThreeMinuteWindow(t *testing.T) {
	guard := newWaypostRecvGuard()
	session := &mcp.ServerSession{}
	req := &mcp.CallToolRequest{Session: session}
	active := map[*mcp.ServerSession]struct{}{session: {}}
	current := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	finishNoMessage := func() bool {
		t.Helper()
		lease, err := guard.begin(req, func() time.Time { return current }, active)
		if err != nil {
			t.Fatalf("begin at %s: %v", current, err)
		}
		return lease.finish("no_message", current)
	}

	if warn := finishNoMessage(); warn {
		t.Fatal("first no_message result warned")
	}
	current = current.Add(waypostRecvNoMessageWarningWindow - time.Second)
	if warn := finishNoMessage(); !warn {
		t.Fatal("repeated no_message just inside three-minute window did not warn")
	}
	current = current.Add(waypostRecvNoMessageWarningWindow)
	if warn := finishNoMessage(); warn {
		t.Fatal("repeated no_message at three-minute boundary warned")
	}
}

func TestWaypostRecvSuccessfulResultClearsNoMessageSequence(t *testing.T) {
	guard := newWaypostRecvGuard()
	session := &mcp.ServerSession{}
	req := &mcp.CallToolRequest{Session: session}
	active := map[*mcp.ServerSession]struct{}{session: {}}
	current := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	first, err := guard.begin(req, func() time.Time { return current }, active)
	if err != nil {
		t.Fatalf("first begin: %v", err)
	}
	first.finish("no_message", current)

	current = current.Add(waypostRecvNoMessageMinInterval)
	success, err := guard.begin(req, func() time.Time { return current }, active)
	if err != nil {
		t.Fatalf("successful begin: %v", err)
	}
	success.finish("received", current)

	current = current.Add(time.Second)
	afterSuccess, err := guard.begin(req, func() time.Time { return current }, active)
	if err != nil {
		t.Fatalf("begin after successful result: %v", err)
	}
	if warn := afterSuccess.finish("no_message", current); warn {
		t.Fatal("first no_message after a successful result warned")
	}
}

func TestWaypostRecvConcurrentBeginIgnoresLaterNoMessage(t *testing.T) {
	guard := newWaypostRecvGuard()
	session := &mcp.ServerSession{}
	req := &mcp.CallToolRequest{Session: session}
	active := map[*mcp.ServerSession]struct{}{session: {}}
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)

	// Seed the state map without an outstanding no_message result.
	seed, err := guard.begin(req, func() time.Time { return now }, active)
	if err != nil {
		t.Fatalf("seed begin: %v", err)
	}
	seed.finish("received", now)

	state := guard.sessions[weak.Make(session)]
	if state == nil {
		t.Fatal("seed did not create session state")
	}

	// Hold the state mutex after A has been admitted. This puts A precisely
	// between inFlight.Add(1) and its timing check. Simulate B completing a
	// no_message result while A is waiting for the mutex.
	state.mu.Lock()
	released := false
	defer func() {
		if !released {
			state.mu.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() {
		lease, err := guard.begin(req, func() time.Time { return now }, active)
		if err == nil {
			lease.finish("received", now)
		}
		done <- err
	}()

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for state.inFlight.Load() == 0 {
		select {
		case <-deadline.C:
			t.Fatal("concurrent begin did not become in-flight")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	state.lastNoMessage = now
	state.hasNoMessage = true
	state.resultVersion.Add(1)
	state.mu.Unlock()
	released = true

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("receive that started before no_message was throttled: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent begin remained blocked")
	}
}

func TestWaypostRecvNoMessageGuardDoesNotCrossConnections(t *testing.T) {
	current := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	fake := &fakeWaypostService{
		t: t,
		receiveBatchWithTTLFunc: func(context.Context, waypost.ReceiveBatchParams, time.Duration) (waypost.ReceiveResult, error) {
			return waypost.ReceiveResult{}, waypost.ErrNoMessage
		},
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: fake},
		CommandRunner:         &fakeRunner{t: t},
		Now:                   func() time.Time { return current },
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.autoBindAttempted = true

	firstReq := &mcp.CallToolRequest{Session: &mcp.ServerSession{}}
	secondReq := &mcp.CallToolRequest{Session: &mcp.ServerSession{}}
	if _, _, err := service.waypostRecv(context.Background(), firstReq, waypostRecvInput{Addresses: []string{"agent-deck/self"}}); err != nil {
		t.Fatalf("first connection recv error = %v", err)
	}
	if _, output, err := service.waypostRecv(context.Background(), secondReq, waypostRecvInput{Addresses: []string{"agent-deck/self"}}); err != nil {
		t.Fatalf("second connection recv error = %v", err)
	} else if output["status"] != "no_message" {
		t.Fatalf("second connection status = %v, want no_message", output["status"])
	}
}

func TestWaypostRecvGroupEnforcesNoMessageIntervalPerConnection(t *testing.T) {
	current := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	calls := 0
	fake := &fakeWaypostService{
		t: t,
		receiveGroupMessageFunc: func(context.Context, waypost.GroupReceiveParams) (waypost.GroupReceivedMessage, error) {
			calls++
			return waypost.GroupReceivedMessage{}, waypost.ErrNoMessage
		},
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: fake},
		CommandRunner:         &fakeRunner{t: t},
		Now:                   func() time.Time { return current },
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.autoBindAttempted = true

	req := &mcp.CallToolRequest{Session: &mcp.ServerSession{}}
	input := waypostRecvInput{Addresses: []string{"group/review"}, AsPerson: "alice"}
	_, output, err := service.waypostRecv(context.Background(), req, input)
	if err != nil {
		t.Fatalf("first group recv error = %v", err)
	}
	if output["status"] != "no_message" {
		t.Fatalf("first group recv status = %v, want no_message", output["status"])
	}

	_, secondOutput, err := service.waypostRecv(context.Background(), req, input)
	if err == nil || !strings.Contains(err.Error(), "delivery notification") {
		t.Fatalf("second group recv error = %v, want wait-for-notification error", err)
	}
	if strings.Contains(err.Error(), "retry") {
		t.Fatalf("second group recv error = %v, must not suggest a retry time", err)
	}
	if secondOutput != nil {
		t.Fatalf("second group recv output = %v, want nil on interval error", secondOutput)
	}
	if calls != 1 {
		t.Fatalf("group receive calls = %d, want one call before interval rejection", calls)
	}

	current = current.Add(waypostRecvNoMessageMinInterval)
	_, output, err = service.waypostRecv(context.Background(), req, input)
	if err != nil {
		t.Fatalf("third group recv error = %v", err)
	}
	warnings, ok := output["warnings"].([]string)
	if !ok || len(warnings) != 1 || !strings.Contains(warnings[0], "meaningless polling") {
		t.Fatalf("third group recv warnings = %#v, want meaningless-polling warning", output["warnings"])
	}
}

func TestWaypostRecvGuardPrunesInactiveSessions(t *testing.T) {
	guard := newWaypostRecvGuard()
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	firstSession := &mcp.ServerSession{}
	firstReq := &mcp.CallToolRequest{Session: firstSession}
	activeFirst := map[*mcp.ServerSession]struct{}{firstSession: {}}

	firstLease, err := guard.begin(firstReq, func() time.Time { return now }, activeFirst)
	if err != nil {
		t.Fatalf("first begin error = %v", err)
	}
	firstLease.finish("no_message", now)

	secondSession := &mcp.ServerSession{}
	secondReq := &mcp.CallToolRequest{Session: secondSession}
	activeSecond := map[*mcp.ServerSession]struct{}{secondSession: {}}
	secondLease, err := guard.begin(secondReq, func() time.Time { return now }, activeSecond)
	if err != nil {
		t.Fatalf("second begin error = %v", err)
	}
	secondLease.finish("received", now)

	if got := len(guard.sessions); got != 1 {
		t.Fatalf("guard session entries = %d, want one active session entry", got)
	}
	if _, ok := guard.sessions[weak.Make(firstSession)]; ok {
		t.Fatal("guard retained inactive session entry")
	}
}

func TestWaypostRecvConcurrentCallsSameConnectionDoNotBlock(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	var callsMu sync.Mutex
	calls := 0
	fake := &fakeWaypostService{
		t: t,
		receiveBatchWithTTLFunc: func(context.Context, waypost.ReceiveBatchParams, time.Duration) (waypost.ReceiveResult, error) {
			callsMu.Lock()
			calls++
			callsMu.Unlock()
			started <- struct{}{}
			<-release
			return waypost.ReceiveResult{}, waypost.ErrNoMessage
		},
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: fake},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.autoBindAttempted = true

	session := &mcp.ServerSession{}
	req := &mcp.CallToolRequest{Session: session}
	done := make(chan error, 2)
	for range 2 {
		go func() {
			_, _, err := service.waypostRecv(context.Background(), req, waypostRecvInput{Addresses: []string{"agent-deck/self"}})
			done <- err
		}()
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("concurrent receive did not reach Waypost service")
		}
	}
	callsMu.Lock()
	if calls != 2 {
		t.Fatalf("receive calls = %d, want two concurrent calls", calls)
	}
	callsMu.Unlock()
	releaseAll()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("concurrent recv error = %v", err)
		}
	}
}

func TestWaypostRecvSlowConnectionDoesNotBlockAnotherConnection(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()
	fake := &fakeWaypostService{
		t: t,
		receiveBatchWithTTLFunc: func(context.Context, waypost.ReceiveBatchParams, time.Duration) (waypost.ReceiveResult, error) {
			started <- struct{}{}
			<-release
			return waypost.ReceiveResult{}, waypost.ErrNoMessage
		},
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: fake},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.autoBindAttempted = true

	firstReq := &mcp.CallToolRequest{Session: &mcp.ServerSession{}}
	secondReq := &mcp.CallToolRequest{Session: &mcp.ServerSession{}}
	done := make(chan error, 2)
	go func() {
		_, _, err := service.waypostRecv(context.Background(), firstReq, waypostRecvInput{Addresses: []string{"agent-deck/self"}})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first receive did not reach Waypost service")
	}
	go func() {
		_, _, err := service.waypostRecv(context.Background(), secondReq, waypostRecvInput{Addresses: []string{"agent-deck/self"}})
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second connection was blocked by first receive")
	}
	releaseAll()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("concurrent recv error = %v", err)
		}
	}
}
