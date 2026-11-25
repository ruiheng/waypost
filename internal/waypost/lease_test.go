package waypost

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReceiveReclaimsExpiredLeaseAndRejectsStaleToken(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	sent := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	first, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive(first) error = %v", err)
	}
	if first.SenderAddress == nil || *first.SenderAddress != "agent/sender" {
		t.Fatalf("Receive(first) sender_address = %v, want agent/sender", first.SenderAddress)
	}
	if first.DeliveryID != sent.DeliveryID {
		t.Fatalf("Receive(first) delivery id = %q, want %q", first.DeliveryID, sent.DeliveryID)
	}
	if first.LeaseExpiresAt != formatTimestamp(current.Add(defaultLeaseTimeout)) {
		t.Fatalf("Receive(first) lease expiry = %q", first.LeaseExpiresAt)
	}

	current = current.Add(defaultLeaseTimeout + time.Second)

	second, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive(second) error = %v", err)
	}
	if second.DeliveryID != first.DeliveryID {
		t.Fatalf("Receive(second) delivery id = %q, want %q", second.DeliveryID, first.DeliveryID)
	}
	if second.LeaseToken == first.LeaseToken {
		t.Fatal("Receive(second) reused the old lease token")
	}

	if _, err := store.Ack(context.Background(), first.DeliveryID, first.LeaseToken); !errors.Is(err, ErrLeaseTokenMismatch) {
		t.Fatalf("Ack(stale token) error = %v, want ErrLeaseTokenMismatch", err)
	}

	acked, err := store.Ack(context.Background(), second.DeliveryID, second.LeaseToken)
	if err != nil {
		t.Fatalf("Ack(current token) error = %v", err)
	}
	if acked.State != "acked" {
		t.Fatalf("Ack(current token) state = %q, want acked", acked.State)
	}
	if acked.AckedAt == "" {
		t.Fatal("Ack(current token) acked_at = empty, want timestamp")
	}

	ackedDeliveries, err := store.List(context.Background(), ListParams{Address: "workflow/reviewer/task-123", State: "acked"})
	if err != nil {
		t.Fatalf("List(acked) error = %v", err)
	}
	if len(ackedDeliveries) != 1 {
		t.Fatalf("len(acked deliveries) = %d, want 1", len(ackedDeliveries))
	}
	if ackedDeliveries[0].State != "acked" {
		t.Fatalf("acked delivery state = %q, want acked", ackedDeliveries[0].State)
	}
	if ackedDeliveries[0].AckedAt == nil {
		t.Fatal("acked delivery acked_at = nil, want timestamp")
	}
	if *ackedDeliveries[0].AckedAt != acked.AckedAt {
		t.Fatalf("acked delivery acked_at = %q, want %q", *ackedDeliveries[0].AckedAt, acked.AckedAt)
	}

	eventTypes := readDeliveryEventTypes(t, runtime, sent.DeliveryID)
	want := []string{"delivery_queued", "delivery_leased", "delivery_leased", "delivery_acked"}
	assertStringSlicesEqual(t, eventTypes, want)
}

func TestRenewExtendsActiveLeaseAndLogsEvent(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	sent := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	received, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}

	current = current.Add(time.Minute)
	renewed, err := store.Renew(context.Background(), received.DeliveryID, received.LeaseToken, 10*time.Minute)
	if err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if renewed.DeliveryID != received.DeliveryID {
		t.Fatalf("Renew() delivery id = %q, want %q", renewed.DeliveryID, received.DeliveryID)
	}
	if renewed.LeaseToken != received.LeaseToken {
		t.Fatalf("Renew() lease token = %q, want %q", renewed.LeaseToken, received.LeaseToken)
	}
	wantExpiry := formatTimestamp(current.Add(10 * time.Minute))
	if renewed.LeaseExpiresAt != wantExpiry {
		t.Fatalf("Renew() lease expiry = %q, want %q", renewed.LeaseExpiresAt, wantExpiry)
	}

	if _, err := store.Ack(context.Background(), renewed.DeliveryID, renewed.LeaseToken); err != nil {
		t.Fatalf("Ack(renewed lease) error = %v", err)
	}

	assertStringSlicesEqual(t, readDeliveryEventTypes(t, runtime, sent.DeliveryID), []string{
		"delivery_queued",
		"delivery_leased",
		"delivery_lease_renewed",
		"delivery_acked",
	})

	eventDetail := readDeliveryEventDetail(t, runtime, sent.DeliveryID, "delivery_lease_renewed")
	if eventDetail["previous_lease_expires_at"] != received.LeaseExpiresAt {
		t.Fatalf("delivery_lease_renewed previous_lease_expires_at = %v, want %q", eventDetail["previous_lease_expires_at"], received.LeaseExpiresAt)
	}
	if eventDetail["lease_expires_at"] != renewed.LeaseExpiresAt {
		t.Fatalf("delivery_lease_renewed lease_expires_at = %v, want %q", eventDetail["lease_expires_at"], renewed.LeaseExpiresAt)
	}
}

func TestReceiveLogsClaimMetadata(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	sent := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")
	ctx := WithClaimMetadata(context.Background(), ClaimMetadata{
		Source:             "mcp",
		Tool:               "waypost_recv",
		BoundAddresses:     []string{"workflow/reviewer/task-123"},
		AgentDeckSessionID: "session-123",
		AgentSessionID:     "codex-session-456",
		Workdir:            "test-workdir",
	})

	if _, err := store.Receive(ctx, ReceiveParams{Address: "workflow/reviewer/task-123"}); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}

	detail := readDeliveryEventDetail(t, runtime, sent.DeliveryID, "delivery_leased")
	if detail["claim_source"] != "mcp" {
		t.Fatalf("claim_source = %v, want mcp", detail["claim_source"])
	}
	if detail["claim_tool"] != "waypost_recv" {
		t.Fatalf("claim_tool = %v, want waypost_recv", detail["claim_tool"])
	}
	if detail["claim_agent_deck_session_id"] != "session-123" {
		t.Fatalf("claim_agent_deck_session_id = %v, want session-123", detail["claim_agent_deck_session_id"])
	}
	if detail["claim_agent_session_id"] != "codex-session-456" {
		t.Fatalf("claim_agent_session_id = %v, want codex-session-456", detail["claim_agent_session_id"])
	}
	if detail["claim_workdir"] != "test-workdir" {
		t.Fatalf("claim_workdir = %v, want test-workdir", detail["claim_workdir"])
	}
	if detail["claim_pid"] == nil {
		t.Fatal("claim_pid missing")
	}
	addresses, ok := detail["claim_bound_addresses"].([]any)
	if !ok || len(addresses) != 1 || addresses[0] != "workflow/reviewer/task-123" {
		t.Fatalf("claim_bound_addresses = %#v, want [workflow/reviewer/task-123]", detail["claim_bound_addresses"])
	}
}

func TestRenewExtendsExpiredLeaseWhenTokenIsStillCurrent(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	current := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	received, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}

	current = current.Add(defaultLeaseTimeout + time.Second)
	renewed, err := store.Renew(context.Background(), received.DeliveryID, received.LeaseToken, time.Minute)
	if err != nil {
		t.Fatalf("Renew(expired current lease) error = %v", err)
	}
	if renewed.LeaseToken != received.LeaseToken {
		t.Fatalf("Renew(expired current lease) token = %q, want %q", renewed.LeaseToken, received.LeaseToken)
	}
	if renewed.LeaseExpiresAt != formatTimestamp(current.Add(time.Minute)) {
		t.Fatalf("Renew(expired current lease) expiry = %q, want %q", renewed.LeaseExpiresAt, formatTimestamp(current.Add(time.Minute)))
	}
}

func TestAckAcceptsExpiredLeaseWhenTokenIsStillCurrent(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	current := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	received, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}

	current = current.Add(defaultLeaseTimeout + time.Second)
	acked, err := store.Ack(context.Background(), received.DeliveryID, received.LeaseToken)
	if err != nil {
		t.Fatalf("Ack(expired current lease) error = %v", err)
	}
	if acked.State != "acked" {
		t.Fatalf("Ack(expired current lease) state = %q, want acked", acked.State)
	}
}

func TestRenewRejectsStaleLeaseToken(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	received, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}

	if _, err := store.Renew(context.Background(), received.DeliveryID, received.LeaseToken+"-stale", time.Minute); !errors.Is(err, ErrLeaseTokenMismatch) {
		t.Fatalf("Renew(stale token) error = %v, want ErrLeaseTokenMismatch", err)
	}
}

func TestRenewReportsLeaseSentinelErrors(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	if _, err := store.Renew(context.Background(), "dlv_missing", "lease_missing", time.Minute); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("Renew(missing delivery) error = %v, want ErrLeaseNotFound", err)
	} else if !strings.Contains(err.Error(), `delivery "dlv_missing" not found`) {
		t.Fatalf("Renew(missing delivery) error = %q, want original message", err)
	}

	mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")
	received, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	if _, err := store.Ack(context.Background(), received.DeliveryID, received.LeaseToken); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if _, err := store.Renew(context.Background(), received.DeliveryID, received.LeaseToken, time.Minute); !errors.Is(err, ErrLeaseNotLeased) {
		t.Fatalf("Renew(acked delivery) error = %v, want ErrLeaseNotLeased", err)
	} else if !strings.Contains(err.Error(), "want leased") {
		t.Fatalf("Renew(acked delivery) error = %q, want original message", err)
	}
}

func TestReceiveLeasePolicyKeepsLegacyPublicTTLAndAllowsShortInternalTTL(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	current := time.Date(2026, 4, 2, 13, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	mustSendMessage(t, store, "workflow/legacy-single", "agent/sender", "legacy single", "legacy single body")
	legacySingle, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/legacy-single"})
	if err != nil {
		t.Fatalf("Receive(legacy public) error = %v", err)
	}
	if legacySingle.LeaseExpiresAt != formatTimestamp(current.Add(defaultLeaseTimeout)) {
		t.Fatalf("Receive(legacy public) lease expiry = %q, want %q", legacySingle.LeaseExpiresAt, formatTimestamp(current.Add(defaultLeaseTimeout)))
	}
	if _, err := store.Ack(context.Background(), legacySingle.DeliveryID, legacySingle.LeaseToken); err != nil {
		t.Fatalf("Ack(legacy public) error = %v", err)
	}

	mustSendMessage(t, store, "workflow/legacy-batch", "agent/sender", "legacy batch", "legacy batch body")
	legacyBatch, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{Address: "workflow/legacy-batch", Max: 1})
	if err != nil {
		t.Fatalf("ReceiveBatch(legacy public) error = %v", err)
	}
	if len(legacyBatch.Messages) != 1 {
		t.Fatalf("len(ReceiveBatch(legacy public).Messages) = %d, want 1", len(legacyBatch.Messages))
	}
	if legacyBatch.Messages[0].LeaseExpiresAt != formatTimestamp(current.Add(defaultLeaseTimeout)) {
		t.Fatalf("ReceiveBatch(legacy public) lease expiry = %q, want %q", legacyBatch.Messages[0].LeaseExpiresAt, formatTimestamp(current.Add(defaultLeaseTimeout)))
	}
	if _, err := store.Ack(context.Background(), legacyBatch.Messages[0].DeliveryID, legacyBatch.Messages[0].LeaseToken); err != nil {
		t.Fatalf("Ack(legacy batch) error = %v", err)
	}

	mustSendMessage(t, store, "workflow/internal-short", "agent/sender", "internal short", "internal short body")
	internalShort, err := store.receiveOnceWithLeasePolicy(context.Background(), []string{"workflow/internal-short"}, receiveLeasePolicy{LeaseTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("receiveOnceWithLeasePolicy(short ttl) error = %v", err)
	}
	if internalShort.LeaseExpiresAt != formatTimestamp(current.Add(30*time.Second)) {
		t.Fatalf("receiveOnceWithLeasePolicy(short ttl) lease expiry = %q, want %q", internalShort.LeaseExpiresAt, formatTimestamp(current.Add(30*time.Second)))
	}
}

func TestReadDeliveryReturnsBodyForAckedDelivery(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	sent := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	message, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
	acked, err := store.Ack(context.Background(), message.DeliveryID, message.LeaseToken)
	if err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	read, err := store.ReadDelivery(context.Background(), sent.DeliveryID)
	if err != nil {
		t.Fatalf("ReadDelivery() error = %v", err)
	}
	if read.DeliveryID != sent.DeliveryID {
		t.Fatalf("ReadDelivery() delivery_id = %q, want %q", read.DeliveryID, sent.DeliveryID)
	}
	if read.State != "acked" {
		t.Fatalf("ReadDelivery() state = %q, want acked", read.State)
	}
	if read.Body != "hello reviewer" {
		t.Fatalf("ReadDelivery() body = %q, want hello reviewer", read.Body)
	}
	if read.AckedAt == nil {
		t.Fatal("ReadDelivery() acked_at = nil, want timestamp")
	}
	if *read.AckedAt != acked.AckedAt {
		t.Fatalf("ReadDelivery() acked_at = %q, want %q", *read.AckedAt, acked.AckedAt)
	}
}

func TestReadMessageReturnsBodyByMessageID(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	sent := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	read, err := store.ReadMessage(context.Background(), sent.MessageID)
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	if read.MessageID != sent.MessageID {
		t.Fatalf("ReadMessage() message_id = %q, want %q", read.MessageID, sent.MessageID)
	}
	if read.Body != "hello reviewer" {
		t.Fatalf("ReadMessage() body = %q, want hello reviewer", read.Body)
	}
	if read.Subject != "review request" {
		t.Fatalf("ReadMessage() subject = %q, want review request", read.Subject)
	}
}

func TestReadLatestDeliveriesReturnsMostRecentAckedForAddress(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "first", "first body")
	firstReceived, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive(first) error = %v", err)
	}
	if _, err := store.Ack(context.Background(), firstReceived.DeliveryID, firstReceived.LeaseToken); err != nil {
		t.Fatalf("Ack(first) error = %v", err)
	}

	current = current.Add(time.Minute)

	second := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "second", "second body")
	secondReceived, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive(second) error = %v", err)
	}
	if _, err := store.Ack(context.Background(), secondReceived.DeliveryID, secondReceived.LeaseToken); err != nil {
		t.Fatalf("Ack(second) error = %v", err)
	}

	read, hasMore, err := store.ReadLatestDeliveries(context.Background(), []string{"workflow/reviewer/task-123"}, "acked", 1)
	if err != nil {
		t.Fatalf("ReadLatestDeliveries(acked) error = %v", err)
	}
	if hasMore != true {
		t.Fatalf("ReadLatestDeliveries(acked) has_more = %t, want true", hasMore)
	}
	if len(read) != 1 {
		t.Fatalf("len(ReadLatestDeliveries(acked)) = %d, want 1", len(read))
	}
	if read[0].DeliveryID != second.DeliveryID {
		t.Fatalf("ReadLatestDeliveries(acked) delivery_id = %q, want %q", read[0].DeliveryID, second.DeliveryID)
	}
	if read[0].Body != "second body" {
		t.Fatalf("ReadLatestDeliveries(acked) body = %q, want second body", read[0].Body)
	}

	current = current.Add(time.Minute)
	queued := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "queued", "queued body")

	readQueued, queuedHasMore, err := store.ReadLatestDeliveries(context.Background(), []string{"workflow/reviewer/task-123"}, "queued", 1)
	if err != nil {
		t.Fatalf("ReadLatestDeliveries(queued) error = %v", err)
	}
	if queuedHasMore {
		t.Fatalf("ReadLatestDeliveries(queued) has_more = true, want false")
	}
	if len(readQueued) != 1 {
		t.Fatalf("len(ReadLatestDeliveries(queued)) = %d, want 1", len(readQueued))
	}
	if readQueued[0].DeliveryID != queued.DeliveryID {
		t.Fatalf("ReadLatestDeliveries(queued) delivery_id = %q, want %q", readQueued[0].DeliveryID, queued.DeliveryID)
	}
	if readQueued[0].Body != "queued body" {
		t.Fatalf("ReadLatestDeliveries(queued) body = %q, want queued body", readQueued[0].Body)
	}
}

func TestReadLatestDeliveriesDefaultsToAnyState(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "older acked", "older acked body")
	firstReceived, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive(first) error = %v", err)
	}
	if _, err := store.Ack(context.Background(), firstReceived.DeliveryID, firstReceived.LeaseToken); err != nil {
		t.Fatalf("Ack(first) error = %v", err)
	}

	current = current.Add(time.Minute)
	queued := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "new queued", "new queued body")

	read, hasMore, err := store.ReadLatestDeliveries(context.Background(), []string{"workflow/reviewer/task-123"}, "", 1)
	if err != nil {
		t.Fatalf("ReadLatestDeliveries(any) error = %v", err)
	}
	if !hasMore {
		t.Fatalf("ReadLatestDeliveries(any) has_more = false, want true")
	}
	if len(read) != 1 {
		t.Fatalf("len(ReadLatestDeliveries(any)) = %d, want 1", len(read))
	}
	if read[0].DeliveryID != queued.DeliveryID {
		t.Fatalf("ReadLatestDeliveries(any) delivery_id = %q, want %q", read[0].DeliveryID, queued.DeliveryID)
	}
	if read[0].State != "queued" {
		t.Fatalf("ReadLatestDeliveries(any) state = %q, want queued", read[0].State)
	}
	if read[0].Body != "new queued body" {
		t.Fatalf("ReadLatestDeliveries(any) body = %q, want new queued body", read[0].Body)
	}
}

func TestPersonalHistoryFiltersBySenderAddress(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }

	fromAlice := mustSendMessage(t, store, "workflow/inbox", "agent/alice", "alice", "alice body")
	current = current.Add(time.Second)
	fromBob := mustSendMessage(t, store, "workflow/inbox", "agent/bob", "bob", "bob body")

	listed, err := store.List(context.Background(), ListParams{
		Address:     "workflow/inbox",
		FromAddress: "agent/alice",
	})
	if err != nil {
		t.Fatalf("List(from alice) error = %v", err)
	}
	if len(listed) != 1 || listed[0].DeliveryID != fromAlice.DeliveryID {
		t.Fatalf("List(from alice) = %+v, want only %q", listed, fromAlice.DeliveryID)
	}
	if listed[0].SenderAddress == nil || *listed[0].SenderAddress != "agent/alice" {
		t.Fatalf("List(from alice) sender_address = %v, want agent/alice", listed[0].SenderAddress)
	}

	read, hasMore, err := store.ReadLatestDeliveriesFiltered(context.Background(), ReadLatestParams{
		Addresses:   []string{"workflow/inbox"},
		FromAddress: "agent/bob",
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("ReadLatestDeliveries(from bob) error = %v", err)
	}
	if hasMore || len(read) != 1 || read[0].DeliveryID != fromBob.DeliveryID || read[0].Body != "bob body" {
		t.Fatalf("ReadLatestDeliveries(from bob) = %+v hasMore=%t", read, hasMore)
	}
	if read[0].SenderAddress == nil || *read[0].SenderAddress != "agent/bob" {
		t.Fatalf("ReadLatestDeliveries(from bob) sender_address = %v, want agent/bob", read[0].SenderAddress)
	}

	missing, missingHasMore, err := store.ReadLatestDeliveriesFiltered(context.Background(), ReadLatestParams{
		Addresses:   []string{"workflow/inbox"},
		FromAddress: "agent/missing",
		Limit:       1,
	})
	if err != nil {
		t.Fatalf("ReadLatestDeliveries(from missing) error = %v", err)
	}
	if len(missing) != 0 || missing == nil || missingHasMore {
		t.Fatalf("ReadLatestDeliveries(from missing) = %+v hasMore=%t, want empty", missing, missingHasMore)
	}
}

func TestReleaseDeferAndReceiveTimeout(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 13, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	first, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive(first) error = %v", err)
	}

	released, err := store.Release(context.Background(), first.DeliveryID, first.LeaseToken)
	if err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if released.State != "queued" {
		t.Fatalf("Release() state = %q, want queued", released.State)
	}

	second, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive(second) error = %v", err)
	}

	until := current.Add(10 * time.Minute)
	deferred, err := store.Defer(context.Background(), second.DeliveryID, second.LeaseToken, until)
	if err != nil {
		t.Fatalf("Defer() error = %v", err)
	}
	if deferred.VisibleAt != formatTimestamp(until) {
		t.Fatalf("Defer() visible_at = %q, want %q", deferred.VisibleAt, formatTimestamp(until))
	}

	visible, err := store.List(context.Background(), ListParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("List(visible queued) error = %v", err)
	}
	if len(visible) != 0 {
		t.Fatalf("len(visible queued) = %d, want 0", len(visible))
	}

	queued, err := store.List(context.Background(), ListParams{Address: "workflow/reviewer/task-123", State: "queued"})
	if err != nil {
		t.Fatalf("List(all queued) error = %v", err)
	}
	if len(queued) != 1 {
		t.Fatalf("len(all queued) = %d, want 1", len(queued))
	}
	if queued[0].VisibleAt != formatTimestamp(until) {
		t.Fatalf("queued visible_at = %q, want %q", queued[0].VisibleAt, formatTimestamp(until))
	}

	if _, err := store.Receive(context.Background(), ReceiveParams{
		Address: "workflow/empty",
	}); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("Receive(empty) error = %v, want ErrNoMessage", err)
	}
}

func TestUndeferMakesDeferredDeliveryClaimableWithoutRevivingOldLease(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 13, 20, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	sent := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")
	received, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive() error = %v", err)
	}

	until := current.Add(10 * time.Minute)
	if _, err := store.Defer(context.Background(), received.DeliveryID, received.LeaseToken, until); err != nil {
		t.Fatalf("Defer() error = %v", err)
	}
	if _, err := store.Ack(context.Background(), received.DeliveryID, received.LeaseToken); err == nil {
		t.Fatal("Ack(old deferred lease) error = nil, want non-nil")
	}

	current = current.Add(time.Minute)
	undeferred, err := store.Undefer(context.Background(), received.DeliveryID)
	if err != nil {
		t.Fatalf("Undefer() error = %v", err)
	}
	if undeferred.State != "queued" {
		t.Fatalf("Undefer() state = %q, want queued", undeferred.State)
	}
	if undeferred.VisibleAt != formatTimestamp(current) {
		t.Fatalf("Undefer() visible_at = %q, want %q", undeferred.VisibleAt, formatTimestamp(current))
	}

	next, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive(after undefer) error = %v", err)
	}
	if next.DeliveryID != received.DeliveryID {
		t.Fatalf("Receive(after undefer) delivery id = %q, want %q", next.DeliveryID, received.DeliveryID)
	}
	if next.LeaseToken == received.LeaseToken {
		t.Fatal("Receive(after undefer) reused old lease token")
	}
	if _, err := store.Ack(context.Background(), next.DeliveryID, next.LeaseToken); err != nil {
		t.Fatalf("Ack(new lease) error = %v", err)
	}

	assertStringSlicesEqual(t, readDeliveryEventTypes(t, runtime, sent.DeliveryID), []string{
		"delivery_queued",
		"delivery_leased",
		"delivery_deferred",
		"delivery_undeferred",
		"delivery_leased",
		"delivery_acked",
	})
}

func TestReceiveMultipleAddressesOrdersUnionOldestFirst(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 13, 30, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	older := mustSendMessage(t, store, "workflow/older", "agent/sender", "older", "older body")
	current = current.Add(time.Second)
	newer := mustSendMessage(t, store, "workflow/newer", "agent/sender", "newer", "newer body")

	first, err := store.Receive(context.Background(), ReceiveParams{
		Addresses: []string{"workflow/newer", "workflow/older", "workflow/newer"},
	})
	if err != nil {
		t.Fatalf("Receive(first multi) error = %v", err)
	}
	if first.DeliveryID != older.DeliveryID {
		t.Fatalf("Receive(first multi) delivery id = %q, want %q", first.DeliveryID, older.DeliveryID)
	}
	if first.RecipientAddress != "workflow/older" {
		t.Fatalf("Receive(first multi) recipient address = %q, want workflow/older", first.RecipientAddress)
	}

	if _, err := store.Ack(context.Background(), first.DeliveryID, first.LeaseToken); err != nil {
		t.Fatalf("Ack(first multi) error = %v", err)
	}

	second, err := store.Receive(context.Background(), ReceiveParams{
		Addresses: []string{"workflow/newer", "workflow/older", "workflow/newer"},
	})
	if err != nil {
		t.Fatalf("Receive(second multi) error = %v", err)
	}
	if second.DeliveryID != newer.DeliveryID {
		t.Fatalf("Receive(second multi) delivery id = %q, want %q", second.DeliveryID, newer.DeliveryID)
	}
	if second.RecipientAddress != "workflow/newer" {
		t.Fatalf("Receive(second multi) recipient address = %q, want workflow/newer", second.RecipientAddress)
	}
}

func TestReceiveBatchClaimsUpToMaxAndReportsRemainingState(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 13, 35, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	firstSent := mustSendMessage(t, store, "workflow/batch", "agent/sender", "first", "first body")
	current = current.Add(time.Second)
	secondSent := mustSendMessage(t, store, "workflow/batch", "agent/sender", "second", "second body")
	current = current.Add(time.Second)
	thirdSent := mustSendMessage(t, store, "workflow/batch", "agent/sender", "third", "third body")

	result, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{
		Address: "workflow/batch",
		Max:     2,
	})
	if err != nil {
		t.Fatalf("ReceiveBatch(first) error = %v", err)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("len(ReceiveBatch(first).Messages) = %d, want 2", len(result.Messages))
	}
	if got := result.RemainingByState["queued"]; got != 1 {
		t.Fatalf("ReceiveBatch(first).remaining_by_state[queued] = %d, want 1", got)
	}
	if result.Messages[0].DeliveryID != firstSent.DeliveryID {
		t.Fatalf("ReceiveBatch(first).Messages[0].delivery_id = %q, want %q", result.Messages[0].DeliveryID, firstSent.DeliveryID)
	}
	if result.Messages[1].DeliveryID != secondSent.DeliveryID {
		t.Fatalf("ReceiveBatch(first).Messages[1].delivery_id = %q, want %q", result.Messages[1].DeliveryID, secondSent.DeliveryID)
	}

	last, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{
		Address: "workflow/batch",
		Max:     2,
	})
	if err != nil {
		t.Fatalf("ReceiveBatch(second) error = %v", err)
	}
	if len(last.Messages) != 1 {
		t.Fatalf("len(ReceiveBatch(second).Messages) = %d, want 1", len(last.Messages))
	}
	if got := last.RemainingByState["leased"]; got != 2 {
		t.Fatalf("ReceiveBatch(second).remaining_by_state[leased] = %d, want 2", got)
	}
	if last.Messages[0].DeliveryID != thirdSent.DeliveryID {
		t.Fatalf("ReceiveBatch(second).Messages[0].delivery_id = %q, want %q", last.Messages[0].DeliveryID, thirdSent.DeliveryID)
	}
}

func TestReceiveBatchNoMessageCountsDeferredButOmitsAcked(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 7, 25, 8, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }

	deferred := mustSendMessage(t, store, "workflow/remaining", "agent/sender", "deferred", "body")
	claimed, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{Address: "workflow/remaining", Max: 1})
	if err != nil {
		t.Fatalf("ReceiveBatch(claim deferred) error = %v", err)
	}
	if _, err := store.Defer(context.Background(), deferred.DeliveryID, claimed.Messages[0].LeaseToken, current.Add(time.Hour)); err != nil {
		t.Fatalf("Defer() error = %v", err)
	}

	noMessage, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{Address: "workflow/remaining", Max: 1})
	if !errors.Is(err, ErrNoMessage) {
		t.Fatalf("ReceiveBatch(deferred) error = %v, want ErrNoMessage", err)
	}
	if got := noMessage.RemainingByState["queued"]; got != 1 {
		t.Fatalf("deferred remaining_by_state[queued] = %d, want 1", got)
	}
	if _, found := noMessage.RemainingByState["leased"]; found {
		t.Fatalf("deferred remaining_by_state = %v, do not want leased", noMessage.RemainingByState)
	}

	acked := mustSendMessage(t, store, "workflow/acked-only", "agent/sender", "acked", "body")
	ackedClaim, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{Address: "workflow/acked-only", Max: 1})
	if err != nil {
		t.Fatalf("ReceiveBatch(claim acked) error = %v", err)
	}
	if _, err := store.Ack(context.Background(), acked.DeliveryID, ackedClaim.Messages[0].LeaseToken); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	remaining, err := store.RemainingByState(context.Background(), []string{"workflow/acked-only"}, nil)
	if err != nil {
		t.Fatalf("RemainingByState() error = %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("acked remaining_by_state = %v, want omitted/empty", remaining)
	}
}

func TestReceiveBatchRollsBackClaimsWhenRemainingStateCountFails(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 7, 25, 8, 10, 0, 0, time.UTC)
	store.now = func() time.Time { return current }
	sent := mustSendMessage(t, store, "workflow/count-rollback", "agent/sender", "rollback", "body")
	countErr := errors.New("remaining-state count failed")

	_, err := store.receiveBatchWithLeasePolicyAndRemainingCounter(
		context.Background(),
		[]string{"workflow/count-rollback"},
		1,
		legacyReceiveLeasePolicy,
		func(context.Context, personalAvailabilityScope, []string) (map[string]int, error) {
			return nil, countErr
		},
	)
	if !errors.Is(err, countErr) {
		t.Fatalf("ReceiveBatch(count failure) error = %v, want %v", err, countErr)
	}
	var recovery *ReceiveRecoveryRequiredError
	if errors.As(err, &recovery) {
		t.Fatalf("ReceiveBatch(count failure) returned recovery claims = %+v, want complete rollback", recovery.Claims)
	}

	listed, err := store.List(context.Background(), ListParams{Address: "workflow/count-rollback", State: "queued"})
	if err != nil {
		t.Fatalf("List(queued after rollback) error = %v", err)
	}
	if len(listed) != 1 || listed[0].DeliveryID != sent.DeliveryID || listed[0].VisibleAt != sent.VisibleAtUTC {
		t.Fatalf("queued delivery after rollback = %+v, want original queued delivery %q at %q", listed, sent.DeliveryID, sent.VisibleAtUTC)
	}

	received, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/count-rollback"})
	if err != nil {
		t.Fatalf("Receive(after complete rollback) error = %v", err)
	}
	if received.DeliveryID != sent.DeliveryID {
		t.Fatalf("Receive(after complete rollback) delivery_id = %q, want %q", received.DeliveryID, sent.DeliveryID)
	}
}

func TestReceiveBatchReturnsOnlyUnreleasedClaimsWhenCountRollbackFails(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 7, 25, 8, 20, 0, 0, time.UTC)
	store.now = func() time.Time { return current }
	sent := mustSendMessage(t, store, "workflow/count-recovery", "agent/sender", "recovery", "body")
	countErr := errors.New("remaining-state count failed")

	_, err := store.receiveBatchWithLeasePolicyAndRemainingCounter(
		context.Background(),
		[]string{"workflow/count-recovery"},
		1,
		legacyReceiveLeasePolicy,
		func(context.Context, personalAvailabilityScope, []string) (map[string]int, error) {
			if _, err := runtime.DB().Exec(`
CREATE TRIGGER reject_count_rollback
BEFORE UPDATE OF state ON deliveries
WHEN OLD.state = 'leased' AND NEW.state = 'queued'
BEGIN
  SELECT RAISE(ABORT, 'reject count rollback');
END;
`); err != nil {
				t.Fatalf("create rollback trigger: %v", err)
			}
			return nil, countErr
		},
	)
	var recovery *ReceiveRecoveryRequiredError
	if !errors.As(err, &recovery) {
		t.Fatalf("ReceiveBatch(incomplete rollback) error = %v, want ReceiveRecoveryRequiredError", err)
	}
	if recovery.Phase != ReceiveRecoveryPhaseRemainingState {
		t.Fatalf("recovery.Phase = %q, want %q", recovery.Phase, ReceiveRecoveryPhaseRemainingState)
	}
	if !errors.Is(err, countErr) {
		t.Fatalf("ReceiveBatch(incomplete rollback) error = %v, want count failure", err)
	}
	if len(recovery.Claims) != 1 {
		t.Fatalf("recovery claims = %+v, want one", recovery.Claims)
	}
	claim := recovery.Claims[0]
	if claim.DeliveryID != sent.DeliveryID || claim.LeaseToken == "" || claim.RecipientAddress != "workflow/count-recovery" {
		t.Fatalf("recovery claim = %+v, want leased delivery %q", claim, sent.DeliveryID)
	}

	var state, leaseToken string
	if err := runtime.DB().QueryRow(`SELECT state, lease_token FROM deliveries WHERE delivery_id = ?`, sent.DeliveryID).Scan(&state, &leaseToken); err != nil {
		t.Fatalf("query recovery delivery: %v", err)
	}
	if state != "leased" || leaseToken != claim.LeaseToken {
		t.Fatalf("recovery delivery state/token = %q/%q, want leased/%q", state, leaseToken, claim.LeaseToken)
	}

	if _, err := runtime.DB().Exec(`DROP TRIGGER reject_count_rollback`); err != nil {
		t.Fatalf("drop rollback trigger: %v", err)
	}
	if _, err := store.Release(context.Background(), claim.DeliveryID, claim.LeaseToken); err != nil {
		t.Fatalf("Release(recovery claim) error = %v", err)
	}
}

func TestReceiveBatchReturnsRecoveryFailureAndReleasesEarlierClaims(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 13, 40, 0, 0, time.UTC)
	store.now = func() time.Time { return current }

	firstSent := mustSendMessage(t, store, "workflow/batch-recovery", "agent/sender", "first", "first body")
	current = current.Add(time.Second)
	secondSent := mustSendMessage(t, store, "workflow/batch-recovery", "agent/sender", "second", "second body")

	blobPath := filepath.Join(runtime.BlobDir(), secondSent.BodyBlobRef)
	if err := os.WriteFile(blobPath, []byte{}, 0o600); err != nil {
		t.Fatalf("os.WriteFile(corrupt blob) error = %v", err)
	}

	nowCalls := 0
	replacedSecondLeaseToken := false
	store.now = func() time.Time {
		nowCalls++
		if nowCalls == 5 {
			if !replacedSecondLeaseToken {
				replaceDeliveryLeaseToken(t, runtime, secondSent.DeliveryID, "lease_replaced_by_other_receiver")
				replacedSecondLeaseToken = true
			}
		}
		return current
	}

	_, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{
		Address: "workflow/batch-recovery",
		Max:     2,
	})
	if !errors.Is(err, ErrReceiveRecovery) {
		t.Fatalf("ReceiveBatch(recovery failure) error = %v, want ErrReceiveRecovery", err)
	}
	if !errors.Is(err, ErrBodyIntegrity) {
		t.Fatalf("ReceiveBatch(recovery failure) error = %v, want ErrBodyIntegrity", err)
	}
	var recoveryRequired *ReceiveRecoveryRequiredError
	if errors.As(err, &recoveryRequired) {
		t.Fatalf("ReceiveBatch(stolen lease) unexpectedly returned ReceiveRecoveryRequiredError: %+v", recoveryRequired)
	}

	firstState, firstAttempts := readDeliveryStateAndAttemptCount(t, runtime, firstSent.DeliveryID)
	if firstState != "queued" {
		t.Fatalf("first delivery state after batch recovery failure = %q, want queued", firstState)
	}
	if firstAttempts != 0 {
		t.Fatalf("first delivery attempt_count after batch recovery failure = %d, want 0", firstAttempts)
	}
	firstVisibleAt := readDeliveryVisibleAt(t, runtime, firstSent.DeliveryID)
	if firstVisibleAt != firstSent.VisibleAtUTC {
		t.Fatalf("first delivery visible_at after batch recovery failure = %q, want %q", firstVisibleAt, firstSent.VisibleAtUTC)
	}

	secondState, secondAttempts := readDeliveryStateAndAttemptCount(t, runtime, secondSent.DeliveryID)
	if secondState != "leased" {
		t.Fatalf("second delivery state after batch recovery failure = %q, want leased", secondState)
	}
	if secondAttempts != 0 {
		t.Fatalf("second delivery attempt_count after batch recovery failure = %d, want 0", secondAttempts)
	}

	assertStringSlicesEqual(t, readDeliveryEventTypes(t, runtime, firstSent.DeliveryID), []string{
		"delivery_queued",
		"delivery_leased",
		"delivery_released",
	})
	assertStringSlicesEqual(t, readDeliveryEventTypes(t, runtime, secondSent.DeliveryID), []string{
		"delivery_queued",
		"delivery_leased",
	})
}

func TestReceiveBatchRejectsTooLargeMax(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	_, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{
		Address: "workflow/batch",
		Max:     maxReceiveBatchSize + 1,
	})
	if err == nil {
		t.Fatal("ReceiveBatch(too large max) error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "--max must be between 1 and 10") {
		t.Fatalf("ReceiveBatch(too large max) error = %q, want max range message", err)
	}
}

func TestReceiveMultipleAddressesUsesDeliveryIDTiebreak(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 13, 45, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	alpha := mustSendMessage(t, store, "workflow/alpha", "agent/sender", "alpha", "alpha body")
	beta := mustSendMessage(t, store, "workflow/beta", "agent/sender", "beta", "beta body")

	wantDeliveryID := alpha.DeliveryID
	wantAddress := "workflow/alpha"
	if beta.DeliveryID < wantDeliveryID {
		wantDeliveryID = beta.DeliveryID
		wantAddress = "workflow/beta"
	}

	message, err := store.Receive(context.Background(), ReceiveParams{
		Addresses: []string{"workflow/beta", "workflow/alpha"},
	})
	if err != nil {
		t.Fatalf("Receive(tiebreak multi) error = %v", err)
	}
	if message.DeliveryID != wantDeliveryID {
		t.Fatalf("Receive(tiebreak multi) delivery id = %q, want %q", message.DeliveryID, wantDeliveryID)
	}
	if message.RecipientAddress != wantAddress {
		t.Fatalf("Receive(tiebreak multi) recipient address = %q, want %q", message.RecipientAddress, wantAddress)
	}
}

func TestReceiveMultipleAddressesReclaimsExpiredLeaseAcrossUnion(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 14, 30, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	reclaimed := mustSendMessage(t, store, "workflow/reclaim", "agent/sender", "reclaim me", "reclaim body")
	first, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reclaim"})
	if err != nil {
		t.Fatalf("Receive(initial reclaim) error = %v", err)
	}

	current = current.Add(defaultLeaseTimeout + time.Second)
	fresh := mustSendMessage(t, store, "workflow/fresh", "agent/sender", "fresh", "fresh body")

	second, err := store.Receive(context.Background(), ReceiveParams{
		Addresses: []string{"workflow/fresh", "workflow/reclaim"},
	})
	if err != nil {
		t.Fatalf("Receive(reclaim multi) error = %v", err)
	}
	if second.DeliveryID != reclaimed.DeliveryID {
		t.Fatalf("Receive(reclaim multi) delivery id = %q, want %q", second.DeliveryID, reclaimed.DeliveryID)
	}
	if second.RecipientAddress != "workflow/reclaim" {
		t.Fatalf("Receive(reclaim multi) recipient address = %q, want workflow/reclaim", second.RecipientAddress)
	}
	if second.LeaseToken == first.LeaseToken {
		t.Fatal("Receive(reclaim multi) reused the expired lease token")
	}

	if _, err := store.Ack(context.Background(), second.DeliveryID, second.LeaseToken); err != nil {
		t.Fatalf("Ack(reclaim multi current token) error = %v", err)
	}

	third, err := store.Receive(context.Background(), ReceiveParams{
		Addresses: []string{"workflow/fresh", "workflow/reclaim"},
	})
	if err != nil {
		t.Fatalf("Receive(fresh after reclaim) error = %v", err)
	}
	if third.DeliveryID != fresh.DeliveryID {
		t.Fatalf("Receive(fresh after reclaim) delivery id = %q, want %q", third.DeliveryID, fresh.DeliveryID)
	}
}

func TestFailRetryPolicyDeadLettersAfterThirdFailure(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 14, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	sent := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	for attempt := 1; attempt <= 3; attempt++ {
		message, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
		if err != nil {
			t.Fatalf("Receive(attempt %d) error = %v", attempt, err)
		}

		failed, err := store.Fail(context.Background(), message.DeliveryID, message.LeaseToken, "tool crashed")
		if err != nil {
			t.Fatalf("Fail(attempt %d) error = %v", attempt, err)
		}

		wantState := "queued"
		if attempt == 3 {
			wantState = "dead_letter"
		}
		if failed.State != wantState {
			t.Fatalf("Fail(attempt %d) state = %q, want %q", attempt, failed.State, wantState)
		}
		if failed.AttemptCount != attempt {
			t.Fatalf("Fail(attempt %d) attempt_count = %d, want %d", attempt, failed.AttemptCount, attempt)
		}

		current = current.Add(time.Second)
	}

	deadLetters, err := store.List(context.Background(), ListParams{Address: "workflow/reviewer/task-123", State: "dead_letter"})
	if err != nil {
		t.Fatalf("List(dead_letter) error = %v", err)
	}
	if len(deadLetters) != 1 {
		t.Fatalf("len(dead_letter) = %d, want 1", len(deadLetters))
	}
	if deadLetters[0].DeliveryID != sent.DeliveryID {
		t.Fatalf("dead-letter delivery id = %q, want %q", deadLetters[0].DeliveryID, sent.DeliveryID)
	}

	eventTypes := readDeliveryEventTypes(t, runtime, sent.DeliveryID)
	want := []string{
		"delivery_queued",
		"delivery_leased",
		"delivery_failed",
		"delivery_leased",
		"delivery_failed",
		"delivery_leased",
		"delivery_failed",
		"delivery_dead_letter",
	}
	assertStringSlicesEqual(t, eventTypes, want)
}

func TestDeadLetterStopsRetryWithoutIncrementingAttemptCount(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	sent := mustSendMessage(t, store, "workflow/reviewer/task-123", "agent/sender", "review request", "hello reviewer")

	first, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive(first) error = %v", err)
	}
	if _, err := store.Fail(context.Background(), first.DeliveryID, first.LeaseToken, "transient failure"); err != nil {
		t.Fatalf("Fail() error = %v", err)
	}

	second, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"})
	if err != nil {
		t.Fatalf("Receive(second) error = %v", err)
	}
	deadLettered, err := store.DeadLetter(context.Background(), second.DeliveryID, second.LeaseToken, "unsupported request")
	if err != nil {
		t.Fatalf("DeadLetter() error = %v", err)
	}
	if deadLettered.State != "dead_letter" {
		t.Fatalf("DeadLetter() state = %q, want dead_letter", deadLettered.State)
	}
	if deadLettered.AttemptCount != 1 {
		t.Fatalf("DeadLetter() attempt_count = %d, want unchanged count 1", deadLettered.AttemptCount)
	}

	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/reviewer/task-123"}); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("Receive(after dead-letter) error = %v, want ErrNoMessage", err)
	}
	read, err := store.ReadDelivery(context.Background(), sent.DeliveryID)
	if err != nil {
		t.Fatalf("ReadDelivery() error = %v", err)
	}
	if read.State != "dead_letter" || read.Body != "hello reviewer" {
		t.Fatalf("ReadDelivery() = state %q body %q, want retained dead-letter body", read.State, read.Body)
	}

	var lastErrorText string
	if err := runtime.readDB.QueryRowContext(context.Background(), `
SELECT last_error_text
FROM deliveries
WHERE delivery_id = ?
`, sent.DeliveryID).Scan(&lastErrorText); err != nil {
		t.Fatalf("read last_error_text error = %v", err)
	}
	if lastErrorText != "unsupported request" {
		t.Fatalf("last_error_text = %q, want unsupported request", lastErrorText)
	}

	eventTypes := readDeliveryEventTypes(t, runtime, sent.DeliveryID)
	want := []string{
		"delivery_queued",
		"delivery_leased",
		"delivery_failed",
		"delivery_leased",
		"delivery_dead_letter",
	}
	assertStringSlicesEqual(t, eventTypes, want)
}

func newLeaseTestStore(t *testing.T) (*Runtime, *Store) {
	t.Helper()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("Runtime.Close() error = %v", err)
		}
	})
	return runtime, runtime.Store()
}

func mustSendMessage(t *testing.T, store *Store, toAddress, fromAddress, subject, body string) SendResult {
	t.Helper()

	result, err := store.Send(context.Background(), SendParams{
		ToAddress:     toAddress,
		FromAddress:   fromAddress,
		Subject:       subject,
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte(body),
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	return result
}

func TestReceiveRejectsCorruptBodyBlob(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 15, 0, 0, 0, time.UTC)
	store.now = func() time.Time {
		return current
	}

	sent := mustSendMessage(t, store, "workflow/corrupt", "agent/sender", "corrupt", "expected body")
	blobPath := filepath.Join(runtime.BlobDir(), sent.BodyBlobRef)
	if err := os.WriteFile(blobPath, []byte{}, 0o600); err != nil {
		t.Fatalf("os.WriteFile(corrupt blob) error = %v", err)
	}

	_, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/corrupt"})
	if !errors.Is(err, ErrBodyIntegrity) {
		t.Fatalf("Receive(corrupt blob) error = %v, want ErrBodyIntegrity", err)
	}
	if !strings.Contains(err.Error(), sent.BodyBlobRef) {
		t.Fatalf("Receive(corrupt blob) error = %q, want blob ref %q", err, sent.BodyBlobRef)
	}

	state, attemptCount := readDeliveryStateAndAttemptCount(t, runtime, sent.DeliveryID)
	if state != "queued" {
		t.Fatalf("delivery state after corrupt receive = %q, want queued", state)
	}
	if attemptCount != 1 {
		t.Fatalf("delivery attempt_count after corrupt receive = %d, want 1", attemptCount)
	}

	eventTypes := readDeliveryEventTypes(t, runtime, sent.DeliveryID)
	want := []string{"delivery_queued", "delivery_leased", "delivery_failed"}
	assertStringSlicesEqual(t, eventTypes, want)
}

func TestReceiveCorruptBodyReportsRecoveryFailureWhenFailTransitionFails(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 3, 18, 15, 30, 0, 0, time.UTC)
	store.now = func() time.Time { return current }

	sent := mustSendMessage(t, store, "workflow/corrupt-recovery", "agent/sender", "corrupt", "expected body")
	blobPath := filepath.Join(runtime.BlobDir(), sent.BodyBlobRef)
	if err := os.WriteFile(blobPath, []byte{}, 0o600); err != nil {
		t.Fatalf("os.WriteFile(corrupt blob) error = %v", err)
	}

	nowCalls := 0
	replacedLeaseToken := false
	store.now = func() time.Time {
		nowCalls++
		if nowCalls >= 3 {
			if !replacedLeaseToken {
				replaceDeliveryLeaseToken(t, runtime, sent.DeliveryID, "lease_replaced_by_other_receiver")
				replacedLeaseToken = true
			}
		}
		return current
	}

	_, err := store.Receive(context.Background(), ReceiveParams{Address: "workflow/corrupt-recovery"})
	if !errors.Is(err, ErrReceiveRecovery) {
		t.Fatalf("Receive(corrupt blob recovery failure) error = %v, want ErrReceiveRecovery", err)
	}
	if !errors.Is(err, ErrBodyIntegrity) {
		t.Fatalf("Receive(corrupt blob recovery failure) error = %v, want ErrBodyIntegrity", err)
	}

	var recoveryErr *ReceiveRecoveryError
	if !errors.As(err, &recoveryErr) {
		t.Fatalf("Receive(corrupt blob recovery failure) error = %T, want *ReceiveRecoveryError", err)
	}
	if recoveryErr.DeliveryID != sent.DeliveryID {
		t.Fatalf("Receive(corrupt blob recovery failure) delivery id = %q, want %q", recoveryErr.DeliveryID, sent.DeliveryID)
	}
	if !strings.Contains(err.Error(), sent.DeliveryID) {
		t.Fatalf("Receive(corrupt blob recovery failure) error = %q, want delivery id %q", err, sent.DeliveryID)
	}

	state, attemptCount := readDeliveryStateAndAttemptCount(t, runtime, sent.DeliveryID)
	if state != "leased" {
		t.Fatalf("delivery state after recovery failure = %q, want leased", state)
	}
	if attemptCount != 0 {
		t.Fatalf("delivery attempt_count after recovery failure = %d, want 0", attemptCount)
	}

	eventTypes := readDeliveryEventTypes(t, runtime, sent.DeliveryID)
	want := []string{"delivery_queued", "delivery_leased"}
	assertStringSlicesEqual(t, eventTypes, want)
}

func TestReceiveRetriesClaimContentionUntilLockClears(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	lockerRuntime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(locker) error = %v", err)
	}
	defer lockerRuntime.Close()

	receiverRuntime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(receiver) error = %v", err)
	}
	defer receiverRuntime.Close()

	const address = "workflow/claim-retry"
	sent := mustSendMessage(t, lockerRuntime.Store(), address, "agent/sender", "retry", "retry body")

	lockTx, err := lockerRuntime.DB().BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx(writer lock) error = %v", err)
	}

	releaseDone := make(chan error, 1)
	go func() {
		time.Sleep(2 * claimRetryDelay)
		releaseDone <- lockTx.Rollback()
	}()

	message, err := receiverRuntime.Store().Receive(context.Background(), ReceiveParams{Address: address})
	if err != nil {
		t.Fatalf("Receive(retry after contention) error = %v", err)
	}
	if rollbackErr := <-releaseDone; rollbackErr != nil {
		t.Fatalf("Rollback(writer lock) error = %v", rollbackErr)
	}
	if message.DeliveryID != sent.DeliveryID {
		t.Fatalf("Receive(retry after contention) delivery id = %q, want %q", message.DeliveryID, sent.DeliveryID)
	}
	if message.RecipientAddress != address {
		t.Fatalf("Receive(retry after contention) recipient address = %q, want %q", message.RecipientAddress, address)
	}
}

func TestReceiveBatchReturnsClaimContentionWhenLockRetriesExhausted(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	lockerRuntime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(locker) error = %v", err)
	}
	defer lockerRuntime.Close()

	receiverRuntime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(receiver) error = %v", err)
	}
	defer receiverRuntime.Close()

	const address = "workflow/claim-contention"
	sendTime := time.Date(2026, 3, 18, 16, 0, 0, 0, time.UTC)
	lockerRuntime.Store().now = func() time.Time { return sendTime }
	firstSent := mustSendMessage(t, lockerRuntime.Store(), address, "agent/sender", "first", "first body")
	sendTime = sendTime.Add(time.Second)
	secondSent := mustSendMessage(t, lockerRuntime.Store(), address, "agent/sender", "second", "second body")

	receiverStore := receiverRuntime.Store()
	receiveTime := sendTime.Add(time.Second)
	nowCalls := 0
	var lockBeginErr error
	releaseDone := make(chan error, 1)
	receiverStore.now = func() time.Time {
		nowCalls++
		if nowCalls == 3 {
			lockTx, err := lockerRuntime.DB().BeginTx(context.Background(), nil)
			if err != nil {
				lockBeginErr = err
				return receiveTime
			}
			go func() {
				time.Sleep(time.Duration(maxClaimRetryCount+2) * claimRetryDelay)
				releaseDone <- lockTx.Rollback()
			}()
		}
		return receiveTime
	}

	_, err = receiverStore.ReceiveBatch(context.Background(), ReceiveBatchParams{
		Address: address,
		Max:     2,
	})
	if !errors.Is(err, ErrClaimContention) {
		t.Fatalf("ReceiveBatch(contention exhaustion) error = %v, want ErrClaimContention", err)
	}
	if errors.Is(err, ErrNoMessage) {
		t.Fatalf("ReceiveBatch(contention exhaustion) error = %v, must not look like ErrNoMessage", err)
	}
	if lockBeginErr != nil {
		t.Fatalf("BeginTx(writer lock during batch) error = %v", lockBeginErr)
	}
	if rollbackErr := <-releaseDone; rollbackErr != nil {
		t.Fatalf("Rollback(writer lock) error = %v", rollbackErr)
	}

	var contentionErr *ClaimContentionError
	if !errors.As(err, &contentionErr) {
		t.Fatalf("ReceiveBatch(contention exhaustion) error = %T, want *ClaimContentionError", err)
	}
	if contentionErr.Attempts != maxClaimRetryCount+1 {
		t.Fatalf("ReceiveBatch(contention exhaustion) attempts = %d, want %d", contentionErr.Attempts, maxClaimRetryCount+1)
	}

	firstState, firstAttemptCount := readDeliveryStateAndAttemptCount(t, lockerRuntime, firstSent.DeliveryID)
	if firstState != "queued" {
		t.Fatalf("first delivery state after contention exhaustion = %q, want queued", firstState)
	}
	if firstAttemptCount != 0 {
		t.Fatalf("first delivery attempt_count after contention exhaustion = %d, want 0", firstAttemptCount)
	}
	firstVisibleAt := readDeliveryVisibleAt(t, lockerRuntime, firstSent.DeliveryID)
	if firstVisibleAt != firstSent.VisibleAtUTC {
		t.Fatalf("first delivery visible_at after contention exhaustion = %q, want %q", firstVisibleAt, firstSent.VisibleAtUTC)
	}

	secondState, secondAttemptCount := readDeliveryStateAndAttemptCount(t, lockerRuntime, secondSent.DeliveryID)
	if secondState != "queued" {
		t.Fatalf("second delivery state after contention exhaustion = %q, want queued", secondState)
	}
	if secondAttemptCount != 0 {
		t.Fatalf("second delivery attempt_count after contention exhaustion = %d, want 0", secondAttemptCount)
	}
	secondVisibleAt := readDeliveryVisibleAt(t, lockerRuntime, secondSent.DeliveryID)
	if secondVisibleAt != secondSent.VisibleAtUTC {
		t.Fatalf("second delivery visible_at after contention exhaustion = %q, want %q", secondVisibleAt, secondSent.VisibleAtUTC)
	}

	firstMessage, err := receiverStore.Receive(context.Background(), ReceiveParams{Address: address})
	if err != nil {
		t.Fatalf("Receive(first after contention rollback) error = %v", err)
	}
	if firstMessage.DeliveryID != firstSent.DeliveryID {
		t.Fatalf("Receive(first after contention rollback) delivery id = %q, want %q", firstMessage.DeliveryID, firstSent.DeliveryID)
	}
	if _, err := receiverStore.Ack(context.Background(), firstMessage.DeliveryID, firstMessage.LeaseToken); err != nil {
		t.Fatalf("Ack(first after contention rollback) error = %v", err)
	}

	secondMessage, err := receiverStore.Receive(context.Background(), ReceiveParams{Address: address})
	if err != nil {
		t.Fatalf("Receive(second after contention rollback) error = %v", err)
	}
	if secondMessage.DeliveryID != secondSent.DeliveryID {
		t.Fatalf("Receive(second after contention rollback) delivery id = %q, want %q", secondMessage.DeliveryID, secondSent.DeliveryID)
	}

	assertStringSlicesEqual(t, readDeliveryEventTypes(t, lockerRuntime, firstSent.DeliveryID), []string{
		"delivery_queued",
		"delivery_leased",
		"delivery_released",
		"delivery_leased",
		"delivery_acked",
	})
	assertStringSlicesEqual(t, readDeliveryEventTypes(t, lockerRuntime, secondSent.DeliveryID), []string{
		"delivery_queued",
		"delivery_leased",
	})
}

func TestReceiveConcurrentClaimUsesSingleClaimerAcrossRuntimes(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	firstRuntime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(first) error = %v", err)
	}
	defer firstRuntime.Close()

	secondRuntime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(second) error = %v", err)
	}
	defer secondRuntime.Close()

	const address = "workflow/single-claimer"
	sent := mustSendMessage(t, firstRuntime.Store(), address, "agent/sender", "race", "race body")

	type receiveResult struct {
		message ReceivedMessage
		err     error
	}

	start := make(chan struct{})
	results := make(chan receiveResult, 2)
	receive := func(store *Store) {
		<-start
		message, err := store.Receive(context.Background(), ReceiveParams{Address: address})
		results <- receiveResult{message: message, err: err}
	}

	go receive(firstRuntime.Store())
	go receive(secondRuntime.Store())
	close(start)

	var got []receiveResult
	for i := 0; i < 2; i++ {
		select {
		case result := <-results:
			got = append(got, result)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for concurrent receive results")
		}
	}

	successCount := 0
	noMessageCount := 0
	for i, result := range got {
		switch {
		case result.err == nil:
			successCount++
			if result.message.DeliveryID != sent.DeliveryID {
				t.Fatalf("receive[%d] delivery id = %q, want %q", i, result.message.DeliveryID, sent.DeliveryID)
			}
		case errors.Is(result.err, ErrNoMessage):
			noMessageCount++
		default:
			t.Fatalf("receive[%d] error = %v, want nil or ErrNoMessage", i, result.err)
		}
	}
	if successCount != 1 || noMessageCount != 1 {
		t.Fatalf("concurrent receive results = %+v, want one success and one ErrNoMessage", got)
	}
}

func TestListUnseenAddressReturnsEmpty(t *testing.T) {
	t.Parallel()

	_, store := newLeaseTestStore(t)

	deliveries, err := store.List(context.Background(), ListParams{Address: "workflow/unseen"})
	if err != nil {
		t.Fatalf("List(unseen) error = %v", err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("len(List unseen) = %d, want 0", len(deliveries))
	}
	if deliveries == nil {
		t.Fatal("List(unseen) returned nil slice, want empty slice")
	}
}

func readDeliveryEventTypes(t *testing.T, runtime *Runtime, deliveryID string) []string {
	t.Helper()

	rows, err := runtime.DB().Query(`
SELECT event_type
FROM events
WHERE delivery_id = ?
ORDER BY rowid
`, deliveryID)
	if err != nil {
		t.Fatalf("Query(events) error = %v", err)
	}
	defer rows.Close()

	var eventTypes []string
	for rows.Next() {
		var eventType string
		if err := rows.Scan(&eventType); err != nil {
			t.Fatalf("Scan(event_type) error = %v", err)
		}
		eventTypes = append(eventTypes, eventType)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() = %v", err)
	}
	return eventTypes
}

func readDeliveryEventDetail(t *testing.T, runtime *Runtime, deliveryID, eventType string) map[string]any {
	t.Helper()

	var raw string
	if err := runtime.DB().QueryRow(`
SELECT detail_json
FROM events
WHERE delivery_id = ? AND event_type = ?
ORDER BY rowid DESC
LIMIT 1
`, deliveryID, eventType).Scan(&raw); err != nil {
		t.Fatalf("QueryRow(event detail) error = %v", err)
	}

	var detail map[string]any
	if err := json.Unmarshal([]byte(raw), &detail); err != nil {
		t.Fatalf("json.Unmarshal(event detail) error = %v; raw = %q", err, raw)
	}
	return detail
}

func readDeliveryStateAndAttemptCount(t *testing.T, runtime *Runtime, deliveryID string) (string, int) {
	t.Helper()

	var state string
	var attemptCount int
	if err := runtime.DB().QueryRow(`
SELECT state, attempt_count
FROM deliveries
WHERE delivery_id = ?
`, deliveryID).Scan(&state, &attemptCount); err != nil {
		t.Fatalf("QueryRow(delivery state/attempt_count) error = %v", err)
	}
	return state, attemptCount
}

func readDeliveryVisibleAt(t *testing.T, runtime *Runtime, deliveryID string) string {
	t.Helper()

	var visibleAt string
	if err := runtime.DB().QueryRow(`
SELECT visible_at
FROM deliveries
WHERE delivery_id = ?
`, deliveryID).Scan(&visibleAt); err != nil {
		t.Fatalf("QueryRow(delivery visible_at) error = %v", err)
	}
	return visibleAt
}

func replaceDeliveryLeaseToken(t *testing.T, runtime *Runtime, deliveryID, leaseToken string) {
	t.Helper()

	result, err := runtime.DB().Exec(`
UPDATE deliveries
SET lease_token = ?
WHERE delivery_id = ?
`, leaseToken, deliveryID)
	if err != nil {
		t.Fatalf("Exec(replace delivery lease token) error = %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected(replace delivery lease token) error = %v", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("replace delivery lease token rows affected = %d, want 1", rowsAffected)
	}
}

func assertStringSlicesEqual(t *testing.T, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("len(got) = %d, want %d (got=%v want=%v)", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got[%d] = %q, want %q (got=%v want=%v)", i, got[i], want[i], got, want)
		}
	}
}

func TestDeliveriesSchemaRejectsIllegalStateCombinations(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	sent := mustSendMessage(t, store, "workflow/check", "agent/sender", "check", "body")
	var endpointID string
	if err := runtime.DB().QueryRow(`SELECT recipient_endpoint_id FROM deliveries WHERE delivery_id = ?`, sent.DeliveryID).Scan(&endpointID); err != nil {
		t.Fatalf("query recipient_endpoint_id: %v", err)
	}

	cases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "invalid state",
			sql: `INSERT INTO deliveries (delivery_id, message_id, recipient_endpoint_id, state, visible_at, lease_token, lease_expires_at, acked_at, attempt_count)
			      VALUES ('dlv_bad_state', ?, ?, 'bogus', '2026-01-01T00:00:00Z', NULL, NULL, NULL, 0)`,
			args: []any{sent.MessageID, endpointID},
		},
		{
			name: "leased without lease token",
			sql: `INSERT INTO deliveries (delivery_id, message_id, recipient_endpoint_id, state, visible_at, lease_token, lease_expires_at, acked_at, attempt_count)
			      VALUES ('dlv_bad_lease', ?, ?, 'leased', '2026-01-01T00:00:00Z', NULL, '2026-01-01T00:05:00Z', NULL, 0)`,
			args: []any{sent.MessageID, endpointID},
		},
		{
			name: "queued with lease token",
			sql: `INSERT INTO deliveries (delivery_id, message_id, recipient_endpoint_id, state, visible_at, lease_token, lease_expires_at, acked_at, attempt_count)
			      VALUES ('dlv_bad_queued', ?, ?, 'queued', '2026-01-01T00:00:00Z', 'lease_1', '2026-01-01T00:05:00Z', NULL, 0)`,
			args: []any{sent.MessageID, endpointID},
		},
		{
			name: "acked without acked_at",
			sql: `INSERT INTO deliveries (delivery_id, message_id, recipient_endpoint_id, state, visible_at, lease_token, lease_expires_at, acked_at, attempt_count)
			      VALUES ('dlv_bad_acked', ?, ?, 'acked', '2026-01-01T00:00:00Z', NULL, NULL, NULL, 0)`,
			args: []any{sent.MessageID, endpointID},
		},
		{
			name: "negative attempt count",
			sql: `INSERT INTO deliveries (delivery_id, message_id, recipient_endpoint_id, state, visible_at, lease_token, lease_expires_at, acked_at, attempt_count)
			      VALUES ('dlv_bad_attempt', ?, ?, 'queued', '2026-01-01T00:00:00Z', NULL, NULL, NULL, -1)`,
			args: []any{sent.MessageID, endpointID},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := runtime.DB().Exec(tc.sql, tc.args...); err == nil {
				t.Fatalf("Exec(%s) succeeded, want SQLite CHECK constraint failure", tc.name)
			}
		})
	}
}

func TestWaitAndWatchObserveExpiredClaimableLeasesAndNormalizeStateFilters(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 4, 5, 10, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }

	const address = "workflow/expired-watch"
	sent := mustSendMessage(t, store, address, "agent/sender", "subject", "body")
	if _, err := store.Receive(context.Background(), ReceiveParams{Address: address}); err != nil {
		t.Fatalf("Receive() error = %v", err)
	}

	current = current.Add(defaultLeaseTimeout + time.Second)

	waited, err := store.Wait(context.Background(), WaitParams{
		Address: address,
		Timeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Wait(expired lease) error = %v", err)
	}
	if waited.DeliveryID != sent.DeliveryID || waited.State != "leased" {
		t.Fatalf("Wait(expired lease) = %+v, want leased delivery %q", waited, sent.DeliveryID)
	}

	for _, stateFilter := range []string{"", "claimed"} {
		ctx, cancel := context.WithCancel(context.Background())
		var got []ListedDelivery
		err := store.Watch(ctx, WatchParams{
			Address: address,
			State:   stateFilter,
			Timeout: time.Second,
		}, func(delivery ListedDelivery) error {
			got = append(got, delivery)
			cancel()
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Watch(state=%q) error = %v", stateFilter, err)
		}
		if len(got) != 1 || got[0].DeliveryID != sent.DeliveryID {
			t.Fatalf("Watch(state=%q) deliveries = %+v, want [%q]", stateFilter, got, sent.DeliveryID)
		}
	}

	page, err := store.ReadLatestDeliveriesPage(context.Background(), ReadLatestParams{
		Addresses: []string{address},
		State:     "claimed",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ReadLatestDeliveriesPage(claimed) error = %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].DeliveryID != sent.DeliveryID {
		t.Fatalf("ReadLatestDeliveriesPage(claimed) = %+v, want [%q]", page.Items, sent.DeliveryID)
	}

	if _, err := store.Wait(context.Background(), WaitParams{Address: address, Timeout: -time.Second}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Wait(negative timeout) error = %v, want ErrInvalidArgument", err)
	}
	if err := store.Watch(context.Background(), WatchParams{Address: address, Timeout: -time.Second}, func(ListedDelivery) error { return nil }); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Watch(negative timeout) error = %v, want ErrInvalidArgument", err)
	}
	if err := store.Watch(context.Background(), WatchParams{Address: address, State: "bogus"}, func(ListedDelivery) error { return nil }); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Watch(invalid state) error = %v, want ErrInvalidArgument", err)
	}
	if _, err := store.ReadLatestDeliveriesPage(context.Background(), ReadLatestParams{Addresses: []string{address}, State: "bogus", Limit: 10}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ReadLatestDeliveriesPage(invalid state) error = %v, want ErrInvalidArgument", err)
	}
}

func TestReceiveBatchReturnsRecoveryRequiredWhenReceiveRecoveryRollbackFails(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	current := time.Date(2026, 7, 25, 9, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }

	const address = "workflow/blob-recovery-rollback"
	first := mustSendMessage(t, store, address, "agent/sender", "first", "first body")
	current = current.Add(time.Second)
	second := mustSendMessage(t, store, address, "agent/sender", "second", "second body")

	var blob2 string
	if err := runtime.DB().QueryRow(`SELECT body_blob_ref FROM messages WHERE message_id = ?`, second.MessageID).Scan(&blob2); err != nil {
		t.Fatalf("query second blob ref: %v", err)
	}
	if err := os.Remove(filepath.Join(store.blobDir, blob2)); err != nil {
		t.Fatalf("remove second blob: %v", err)
	}
	if _, err := runtime.DB().Exec(`
CREATE TRIGGER reject_leased_to_queued_rollback
BEFORE UPDATE OF state ON deliveries
WHEN OLD.state = 'leased' AND NEW.state = 'queued'
BEGIN
  SELECT RAISE(ABORT, 'reject leased->queued');
END;
`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	_, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{
		Address: address,
		Max:     2,
	})
	var recovery *ReceiveRecoveryRequiredError
	if !errors.As(err, &recovery) {
		t.Fatalf("ReceiveBatch() error = %v, want *ReceiveRecoveryRequiredError", err)
	}
	if recovery.Phase != "claim_recovery" {
		t.Fatalf("recovery.Phase = %q, want %q", recovery.Phase, "claim_recovery")
	}
	if len(recovery.Claims) != 2 || recovery.Claims[0].DeliveryID != first.DeliveryID || recovery.Claims[1].DeliveryID != second.DeliveryID {
		t.Fatalf("recovery.Claims = %+v, want [%q %q]", recovery.Claims, first.DeliveryID, second.DeliveryID)
	}
}

func TestReceiveReturnsRecoveryRequiredWhenSingleClaimRecoveryFails(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	const address = "workflow/single-claim-recovery"
	sent := mustSendMessage(t, store, address, "agent/sender", "only", "body")
	second := mustSendMessage(t, store, address, "agent/sender", "second", "body2")

	for _, messageID := range []string{sent.MessageID, second.MessageID} {
		var blobRef string
		if err := runtime.DB().QueryRow(`SELECT body_blob_ref FROM messages WHERE message_id = ?`, messageID).Scan(&blobRef); err != nil {
			t.Fatalf("query blob ref(%s): %v", messageID, err)
		}
		if err := os.Remove(filepath.Join(store.blobDir, blobRef)); err != nil {
			t.Fatalf("remove blob(%s): %v", messageID, err)
		}
	}
	if _, err := runtime.DB().Exec(`
CREATE TRIGGER reject_leased_to_queued
BEFORE UPDATE OF state ON deliveries
WHEN OLD.state = 'leased' AND NEW.state = 'queued'
BEGIN
  SELECT RAISE(ABORT, 'reject leased->queued');
END;
`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	_, err := store.ReceiveBatch(context.Background(), ReceiveBatchParams{
		Address: address,
		Max:     1,
	})
	var recovery *ReceiveRecoveryRequiredError
	if !errors.As(err, &recovery) {
		t.Fatalf("ReceiveBatch(Max=1) error = %v, want *ReceiveRecoveryRequiredError", err)
	}
	if len(recovery.Claims) != 1 || recovery.Claims[0].DeliveryID != sent.DeliveryID || recovery.Claims[0].LeaseToken == "" {
		t.Fatalf("recovery.Claims = %+v, want leaked single claim %q", recovery.Claims, sent.DeliveryID)
	}
	if recovery.Claims[0].SenderAddress == nil || *recovery.Claims[0].SenderAddress != "agent/sender" {
		t.Fatalf("recovery.Claims[0].SenderAddress = %v, want agent/sender", recovery.Claims[0].SenderAddress)
	}
	if !strings.Contains(recovery.Error(), "receive recovery failed") {
		t.Fatalf("recovery.Error() = %q, want claim recovery failure text", recovery.Error())
	}

	_, singleErr := store.Receive(context.Background(), ReceiveParams{Address: address})
	var singleRecovery *ReceiveRecoveryRequiredError
	if !errors.As(singleErr, &singleRecovery) || singleRecovery.Phase != ReceiveRecoveryPhaseClaimRecovery {
		t.Fatalf("Receive() error = %v, want *ReceiveRecoveryRequiredError(claim_recovery)", singleErr)
	}
	if len(singleRecovery.Claims) != 1 || singleRecovery.Claims[0].DeliveryID != second.DeliveryID {
		t.Fatalf("Receive() recovery claims = %+v, want %q", singleRecovery.Claims, second.DeliveryID)
	}
}

func TestReceiveBatchPreservesPrimaryErrorWithoutSentinelHijackWhenRollbackLosesOwnership(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	const address = "workflow/count-rollback-lost-ownership"
	mustSendMessage(t, store, address, "agent/sender", "subject", "body")
	primaryErr := errors.New("primary remaining-state query failure")

	_, err := store.receiveBatchWithLeasePolicyAndRemainingCounter(
		context.Background(),
		[]string{address},
		1,
		legacyReceiveLeasePolicy,
		func(ctx context.Context, _ personalAvailabilityScope, claimedIDs []string) (map[string]int, error) {
			if len(claimedIDs) != 1 {
				t.Fatalf("claimedIDs = %v, want 1 claimed delivery", claimedIDs)
			}
			var token string
			if qErr := runtime.DB().QueryRowContext(ctx, `SELECT lease_token FROM deliveries WHERE delivery_id = ?`, claimedIDs[0]).Scan(&token); qErr != nil {
				t.Fatalf("query claimed lease token: %v", qErr)
			}
			if _, ackErr := store.Ack(ctx, claimedIDs[0], token); ackErr != nil {
				t.Fatalf("concurrent Ack() error = %v", ackErr)
			}
			return nil, primaryErr
		},
	)
	if !errors.Is(err, primaryErr) {
		t.Fatalf("ReceiveBatch() error = %v, want primary error %v preserved", err, primaryErr)
	}
	var recovery *ReceiveRecoveryRequiredError
	if errors.As(err, &recovery) {
		t.Fatalf("ReceiveBatch() returned *ReceiveRecoveryRequiredError (%+v), want none when ownership definitely lost", recovery)
	}
	if !strings.Contains(err.Error(), "rollback failed:") {
		t.Fatalf("ReceiveBatch() error = %q, want rollback failure context", err.Error())
	}
	if doc := cliErrorFor(err); doc.ErrorCode != "internal" {
		t.Fatalf("cliErrorFor(err).ErrorCode = %q, want internal without rollback sentinel hijack", doc.ErrorCode)
	}
}

func TestClaimNextDeliveryDoesNotRetrySQLiteBusyInsideReceiveRecoveryError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	rt1, err := OpenRuntime(ctx, filepath.Join(t.TempDir(), "state1"))
	if err != nil {
		t.Fatalf("OpenRuntime(rt1) error = %v", err)
	}
	defer rt1.Close()

	rt2, err := OpenRuntime(ctx, filepath.Join(t.TempDir(), "state2"))
	if err != nil {
		t.Fatalf("OpenRuntime(rt2) error = %v", err)
	}
	defer rt2.Close()

	lockTx, err := rt2.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("rt2.DB().BeginTx() error = %v", err)
	}
	defer func() { _ = lockTx.Rollback() }()

	lockedWriteDB, err := openDatabase(ctx, rt2.dbPath, "immediate", 0)
	if err != nil {
		t.Fatalf("openDatabase(lockedWriteDB) error = %v", err)
	}
	defer lockedWriteDB.Close()

	base := time.Date(2026, 8, 1, 11, 0, 0, 0, time.UTC)
	rt1.Store().now = func() time.Time { return base }

	const address = "workflow/busy-recovery"
	first := mustSendMessage(t, rt1.Store(), address, "agent/sender", "first", "body1")
	rt1.Store().now = func() time.Time { return base.Add(time.Second) }
	second := mustSendMessage(t, rt1.Store(), address, "agent/sender", "second", "body2")

	var firstBlobRef string
	if err := rt1.DB().QueryRow(`SELECT body_blob_ref FROM messages WHERE message_id = ?`, first.MessageID).Scan(&firstBlobRef); err != nil {
		t.Fatalf("query first blob ref: %v", err)
	}
	if err := os.Remove(filepath.Join(rt1.blobDir, firstBlobRef)); err != nil {
		t.Fatalf("remove first blob: %v", err)
	}

	store := NewStore(rt1.readDB, lockedWriteDB, rt1.claimDB, rt1.blobDir)
	store.now = func() time.Time { return base.Add(2 * time.Second) }
	_, recvErr := store.Receive(ctx, ReceiveParams{Address: address})
	var recovery *ReceiveRecoveryRequiredError
	if !errors.As(recvErr, &recovery) || recovery.Phase != ReceiveRecoveryPhaseClaimRecovery {
		t.Fatalf("Receive() error = %v, want *ReceiveRecoveryRequiredError(claim_recovery)", recvErr)
	}
	if !isSQLiteBusy(recvErr) {
		t.Fatalf("Receive() error = %v, want underlying SQLITE_BUSY recovery failure", recvErr)
	}
	if len(recovery.Claims) != 1 || recovery.Claims[0].DeliveryID != first.DeliveryID {
		t.Fatalf("recovery.Claims = %+v, want only leaked claim %q", recovery.Claims, first.DeliveryID)
	}

	var secondState string
	if err := rt1.DB().QueryRow(`SELECT state FROM deliveries WHERE delivery_id = ?`, second.DeliveryID).Scan(&secondState); err != nil {
		t.Fatalf("query second state: %v", err)
	}
	if secondState != "queued" {
		t.Fatalf("second delivery state = %q, want queued (must not claim via busy retry after recovery failure)", secondState)
	}
}

func TestReceiveBatchClaimLoopCancelUsesClaimRecoveryPhaseAndSkipsRemainingCounter(t *testing.T) {
	t.Parallel()

	t.Run("successful rollback on second claim cancellation preserves context.Canceled", func(t *testing.T) {
		t.Parallel()

		runtime, store := newLeaseTestStore(t)
		defer runtime.Close()

		base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
		store.now = func() time.Time { return base }

		const address = "workflow/cancel-mid-batch-ok"
		first := mustSendMessage(t, store, address, "agent/sender", "one", "body1")
		store.now = func() time.Time { return base.Add(time.Second) }
		mustSendMessage(t, store, address, "agent/sender", "two", "body2")

		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		store.now = func() time.Time {
			calls++
			if calls == 3 {
				cancel()
			}
			return base.Add(2 * time.Second)
		}

		_, err := store.receiveBatchWithLeasePolicyAndRemainingCounter(
			ctx,
			[]string{address},
			2,
			legacyReceiveLeasePolicy,
			func(context.Context, personalAvailabilityScope, []string) (map[string]int, error) {
				t.Fatal("remainingCounter must not be called after claim-loop cancellation")
				return nil, nil
			},
		)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReceiveBatch(canceled mid-loop) error = %v, want context.Canceled", err)
		}

		var firstState string
		if qErr := runtime.DB().QueryRow(`SELECT state FROM deliveries WHERE delivery_id = ?`, first.DeliveryID).Scan(&firstState); qErr != nil {
			t.Fatalf("query first state: %v", qErr)
		}
		if firstState != "queued" {
			t.Fatalf("first delivery state = %q, want queued after clean rollback", firstState)
		}
	})

	t.Run("failed rollback on second claim cancellation reports claim_recovery phase", func(t *testing.T) {
		t.Parallel()

		runtime, store := newLeaseTestStore(t)
		defer runtime.Close()

		base := time.Date(2026, 8, 1, 12, 5, 0, 0, time.UTC)
		store.now = func() time.Time { return base }

		const address = "workflow/cancel-mid-batch-recovery"
		first := mustSendMessage(t, store, address, "agent/sender", "one", "body1")
		store.now = func() time.Time { return base.Add(time.Second) }
		mustSendMessage(t, store, address, "agent/sender", "two", "body2")

		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		store.now = func() time.Time {
			calls++
			if calls == 3 {
				if _, execErr := runtime.DB().Exec(`
CREATE TRIGGER reject_cancel_rollback
BEFORE UPDATE OF state ON deliveries
WHEN OLD.state = 'leased' AND NEW.state = 'queued'
BEGIN
  SELECT RAISE(ABORT, 'reject cancel rollback');
END;
`); execErr != nil {
					t.Fatalf("create trigger: %v", execErr)
				}
				cancel()
			}
			return base.Add(2 * time.Second)
		}

		_, err := store.receiveBatchWithLeasePolicyAndRemainingCounter(
			ctx,
			[]string{address},
			2,
			legacyReceiveLeasePolicy,
			func(context.Context, personalAvailabilityScope, []string) (map[string]int, error) {
				t.Fatal("remainingCounter must not be called after claim-loop cancellation")
				return nil, nil
			},
		)
		var recovery *ReceiveRecoveryRequiredError
		if !errors.As(err, &recovery) || recovery.Phase != ReceiveRecoveryPhaseClaimRecovery {
			t.Fatalf("ReceiveBatch(canceled mid-loop, failed rollback) error = %v, want *ReceiveRecoveryRequiredError(claim_recovery)", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReceiveBatch() error = %v, want underlying context.Canceled", err)
		}
		if len(recovery.Claims) != 1 || recovery.Claims[0].DeliveryID != first.DeliveryID {
			t.Fatalf("recovery.Claims = %+v, want %q", recovery.Claims, first.DeliveryID)
		}
	})
}
