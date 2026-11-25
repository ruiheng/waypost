package waypost

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestWatchTraversesEveryBoundedPage(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }
	want := make([]string, 0, MaxPageSize+1)
	for index := 0; index < MaxPageSize+1; index++ {
		sent := mustSendMessage(t, store, "workflow/watch-pages", "agent/sender", "paged", "body")
		want = append(want, sent.DeliveryID)
		current = current.Add(time.Second)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make([]string, 0, len(want))
	err := store.Watch(ctx, WatchParams{Address: "workflow/watch-pages"}, func(delivery ListedDelivery) error {
		got = append(got, delivery.DeliveryID)
		if len(got) == len(want) {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch() error = %v, want context.Canceled", err)
	}
	assertStringSlicesEqual(t, got, want)
}

func TestWatchEmitsQueuedDeliveryWithoutClaiming(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	sent := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	var deliveries []ListedDelivery
	if err := store.Watch(context.Background(), WatchParams{
		Addresses: []string{"workflow/reviewer/task-123"},
		Timeout:   120 * time.Millisecond,
	}, func(delivery ListedDelivery) error {
		deliveries = append(deliveries, delivery)
		return nil
	}); err != nil {
		t.Fatalf("Watch() error = %v", err)
	}

	if len(deliveries) != 1 {
		t.Fatalf("len(deliveries) = %d, want 1", len(deliveries))
	}
	if deliveries[0].DeliveryID != sent.DeliveryID {
		t.Fatalf("watch delivery id = %q, want %q", deliveries[0].DeliveryID, sent.DeliveryID)
	}
	if deliveries[0].RecipientAddress != "workflow/reviewer/task-123" {
		t.Fatalf("watch recipient address = %q, want workflow/reviewer/task-123", deliveries[0].RecipientAddress)
	}

	queued, err := store.List(context.Background(), ListParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(queued))
	}
	if queued[0].DeliveryID != sent.DeliveryID {
		t.Fatalf("queued delivery id = %q, want %q", queued[0].DeliveryID, sent.DeliveryID)
	}

	eventTypes := readDeliveryEventTypes(t, runtime, sent.DeliveryID)
	want := []string{"delivery_queued"}
	assertStringSlicesEqual(t, eventTypes, want)
}

func TestWaitReturnsQueuedDeliveryWithoutClaiming(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	sent := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	delivery, err := store.Wait(context.Background(), WaitParams{
		Addresses: []string{"workflow/reviewer/task-123"},
	})
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if delivery.DeliveryID != sent.DeliveryID {
		t.Fatalf("wait delivery id = %q, want %q", delivery.DeliveryID, sent.DeliveryID)
	}
	if delivery.RecipientAddress != "workflow/reviewer/task-123" {
		t.Fatalf("wait recipient address = %q, want workflow/reviewer/task-123", delivery.RecipientAddress)
	}

	queued, err := store.List(context.Background(), ListParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(queued))
	}
	if queued[0].DeliveryID != sent.DeliveryID {
		t.Fatalf("queued delivery id = %q, want %q", queued[0].DeliveryID, sent.DeliveryID)
	}

	eventTypes := readDeliveryEventTypes(t, runtime, sent.DeliveryID)
	want := []string{"delivery_queued"}
	assertStringSlicesEqual(t, eventTypes, want)
}

func TestWatchMultipleAddressesUsesOldestFirstUnion(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	current := time.Date(2026, 3, 18, 15, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	older := mustSendMessage(t, store, "workflow/older", "agent/sender", "older", "older body")
	current = current.Add(time.Second)
	newer := mustSendMessage(t, store, "workflow/newer", "agent/sender", "newer", "newer body")

	var deliveries []ListedDelivery
	if err := store.Watch(context.Background(), WatchParams{
		Addresses: []string{"workflow/newer", "workflow/older", "workflow/newer"},
		Timeout:   120 * time.Millisecond,
	}, func(delivery ListedDelivery) error {
		deliveries = append(deliveries, delivery)
		return nil
	}); err != nil {
		t.Fatalf("Watch(multi) error = %v", err)
	}

	if len(deliveries) != 2 {
		t.Fatalf("len(deliveries) = %d, want 2", len(deliveries))
	}
	if deliveries[0].DeliveryID != older.DeliveryID {
		t.Fatalf("deliveries[0].delivery_id = %q, want %q", deliveries[0].DeliveryID, older.DeliveryID)
	}
	if deliveries[0].RecipientAddress != "workflow/older" {
		t.Fatalf("deliveries[0].recipient_address = %q, want workflow/older", deliveries[0].RecipientAddress)
	}
	if deliveries[1].DeliveryID != newer.DeliveryID {
		t.Fatalf("deliveries[1].delivery_id = %q, want %q", deliveries[1].DeliveryID, newer.DeliveryID)
	}
	if deliveries[1].RecipientAddress != "workflow/newer" {
		t.Fatalf("deliveries[1].recipient_address = %q, want workflow/newer", deliveries[1].RecipientAddress)
	}
}

func TestWaitMultipleAddressesUsesOldestFirstUnion(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	current := time.Date(2026, 3, 18, 15, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	older := mustSendMessage(t, store, "workflow/older", "agent/sender", "older", "older body")
	current = current.Add(time.Second)
	mustSendMessage(t, store, "workflow/newer", "agent/sender", "newer", "newer body")

	delivery, err := store.Wait(context.Background(), WaitParams{
		Addresses: []string{"workflow/newer", "workflow/older", "workflow/newer"},
	})
	if err != nil {
		t.Fatalf("Wait(multi) error = %v", err)
	}
	if delivery.DeliveryID != older.DeliveryID {
		t.Fatalf("wait delivery id = %q, want %q", delivery.DeliveryID, older.DeliveryID)
	}
	if delivery.RecipientAddress != "workflow/older" {
		t.Fatalf("wait recipient_address = %q, want workflow/older", delivery.RecipientAddress)
	}
}

func TestWatchSeesAddressCreatedByLaterSend(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	var deliveries []ListedDelivery
	done := make(chan error, 1)
	go func() {
		done <- store.Watch(context.Background(), WatchParams{
			Address: "workflow/later-watch",
			Timeout: 300 * time.Millisecond,
		}, func(delivery ListedDelivery) error {
			deliveries = append(deliveries, delivery)
			return nil
		})
	}()

	time.Sleep(75 * time.Millisecond)
	sent := mustSendMessage(t, store, "workflow/later-watch", "agent/sender", "watch later", "watch body")

	if err := <-done; err != nil {
		t.Fatalf("Watch(wait unseen) error = %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("len(deliveries) = %d, want 1", len(deliveries))
	}
	if deliveries[0].DeliveryID != sent.DeliveryID {
		t.Fatalf("watch later delivery id = %q, want %q", deliveries[0].DeliveryID, sent.DeliveryID)
	}
	if deliveries[0].RecipientAddress != "workflow/later-watch" {
		t.Fatalf("watch later recipient address = %q, want workflow/later-watch", deliveries[0].RecipientAddress)
	}
}

func TestWaitSeesAddressCreatedByLaterSend(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	type waitResult struct {
		delivery ListedDelivery
		err      error
	}

	resultCh := make(chan waitResult, 1)
	go func() {
		delivery, err := store.Wait(context.Background(), WaitParams{
			Address: "workflow/later-wait",
			Timeout: time.Second,
		})
		resultCh <- waitResult{delivery: delivery, err: err}
	}()

	time.Sleep(75 * time.Millisecond)
	sent := mustSendMessage(t, store, "workflow/later-wait", "agent/sender", "wait later", "wait body")

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("Wait(wait unseen) error = %v", result.err)
	}
	if result.delivery.DeliveryID != sent.DeliveryID {
		t.Fatalf("wait later delivery id = %q, want %q", result.delivery.DeliveryID, sent.DeliveryID)
	}
	if result.delivery.RecipientAddress != "workflow/later-wait" {
		t.Fatalf("wait later recipient address = %q, want workflow/later-wait", result.delivery.RecipientAddress)
	}
}

func TestListReadsQueuedDeliveryWhileAnotherRuntimeHoldsWriterLock(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	lockerRuntime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(locker) error = %v", err)
	}
	defer lockerRuntime.Close()

	readerRuntime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(reader) error = %v", err)
	}
	defer readerRuntime.Close()

	const address = "workflow/list-under-lock"
	sent := mustSendMessage(t, lockerRuntime.Store(), address, "agent/sender", "queued", "queued body")

	lockTx, err := lockerRuntime.DB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx(writer lock) error = %v", err)
	}
	defer lockTx.Rollback()

	start := time.Now()
	deliveries, err := readerRuntime.Store().List(context.Background(), ListParams{Address: address, State: "queued"})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("List(queued under writer lock) error = %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("List(queued under writer lock) took %v, want under 1s", elapsed)
	}
	if len(deliveries) != 1 {
		t.Fatalf("len(deliveries) = %d, want 1", len(deliveries))
	}
	if deliveries[0].DeliveryID != sent.DeliveryID {
		t.Fatalf("list delivery id = %q, want %q", deliveries[0].DeliveryID, sent.DeliveryID)
	}
}

func TestWatchReEmitsDeliveryOnRepeatedLeaseExpiry(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	base := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	now := base
	store.now = func() time.Time { return now }

	const address = "workflow/watch-reexpire"
	sent := mustSendMessage(t, store, address, "agent/sender", "reexpire", "body")

	// Consumer A claims then lets lease expire.
	now = base.Add(time.Second)
	if _, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{Address: address, Max: 1}); err != nil {
		t.Fatalf("first ReceiveBatch() error = %v", err)
	}

	now = now.Add(defaultLeaseTimeout + time.Second)
	var emitted []string
	var claim2Once bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := store.Watch(ctx, WatchParams{Address: address, Timeout: time.Second}, func(d ListedDelivery) error {
		emitted = append(emitted, d.DeliveryID)
		if len(emitted) == 1 && !claim2Once {
			claim2Once = true
			// Consumer B reclaims with a new lease_token, then lets THAT lease expire too.
			now = now.Add(time.Second)
			if _, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{Address: address, Max: 1}); err != nil {
				return err
			}
			now = now.Add(defaultLeaseTimeout + time.Second)
		} else if len(emitted) >= 2 {
			cancel()
		}
		return nil
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch() error = %v", err)
	}
	if len(emitted) != 2 || emitted[0] != sent.DeliveryID || emitted[1] != sent.DeliveryID {
		t.Fatalf("emitted = %v, want [%q, %q] across two expirations", emitted, sent.DeliveryID, sent.DeliveryID)
	}
}

func TestWatchLeasedDoesNotReEmitOnLeaseRenewal(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	base := time.Date(2026, 8, 13, 13, 0, 0, 0, time.UTC)
	now := base
	store.now = func() time.Time { return now }

	const address = "workflow/watch-renew"
	receivedCh := make(chan ReceivedMessage, 1)
	sent := mustSendMessage(t, store, address, "agent/sender", "leased", "body")
	msg, err := store.Receive(context.Background(), ReceiveParams{Address: address})
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	receivedCh <- msg

	var emitted []string
	var renewedOnce bool
	err = store.Watch(context.Background(), WatchParams{
		Address: address,
		State:   "leased",
		Timeout: 150 * time.Millisecond,
	}, func(d ListedDelivery) error {
		emitted = append(emitted, d.DeliveryID)
		if !renewedOnce {
			renewedOnce = true
			claimed := <-receivedCh
			now = now.Add(time.Minute)
			if _, err := store.Renew(context.Background(), claimed.DeliveryID, claimed.LeaseToken, 10*time.Minute); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Watch(leased) error = %v, want clean idle timeout", err)
	}
	if len(emitted) != 1 || emitted[0] != sent.DeliveryID {
		t.Fatalf("emitted = %v, want [%q] once without renewal duplicate", emitted, sent.DeliveryID)
	}
}
