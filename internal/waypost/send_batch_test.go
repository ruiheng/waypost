package waypost

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type sendBatchSenderFunc func(context.Context, SendParams) (SendResult, error)

func (f sendBatchSenderFunc) Send(ctx context.Context, params SendParams) (SendResult, error) {
	return f(ctx, params)
}

func TestNormalizeSendRecipients(t *testing.T) {
	t.Parallel()

	t.Run("normalizes and keeps first seen order", func(t *testing.T) {
		recipients, err := NormalizeSendRecipients([]string{
			" agent-deck/one ",
			"agent-deck/two",
			"agent-deck/one",
		}, false)
		if err != nil {
			t.Fatalf("NormalizeSendRecipients() error = %v", err)
		}
		want := []string{"agent-deck/one", "agent-deck/two"}
		if !reflect.DeepEqual(recipients, want) {
			t.Fatalf("recipients = %v, want %v", recipients, want)
		}
	})

	t.Run("rejects invalid input before execution", func(t *testing.T) {
		for _, values := range [][]string{
			nil,
			{"agent-deck/one", "not an address"},
			makeRecipientValues(MaxSendRecipients + 1),
		} {
			if _, err := NormalizeSendRecipients(values, false); err == nil {
				t.Fatalf("NormalizeSendRecipients(%v) error = nil", values)
			}
		}
	})

	t.Run("validates batch mode", func(t *testing.T) {
		if _, err := NormalizeSendRecipients([]string{"agent-deck/one", "group/review"}, false); err == nil {
			t.Fatal("NormalizeSendRecipients(personal group target) error = nil")
		}
		if _, err := NormalizeSendRecipients([]string{"group/review", "agent-deck/one"}, true); err == nil {
			t.Fatal("NormalizeSendRecipients(group personal target) error = nil")
		}
	})
}

func TestExecuteSendBatchContinuesAfterDurableFailure(t *testing.T) {
	t.Parallel()

	var calls []string
	var notifications []string
	batch, err := ExecuteSendBatch(
		context.Background(),
		sendBatchSenderFunc(func(_ context.Context, params SendParams) (SendResult, error) {
			calls = append(calls, params.ToAddress)
			if params.ToAddress == "agent-deck/two" {
				return SendResult{}, errors.New("commit send transaction: test failure")
			}
			return SendResult{DeliveryID: "dlv_" + strings.TrimPrefix(params.ToAddress, "agent-deck/")}, nil
		}),
		SendParams{FromAddress: "agent-deck/sender", Body: []byte("body")},
		[]string{"agent-deck/one", "agent-deck/two", "agent-deck/three"},
		func(_ context.Context, request SendNotificationRequest) SendNotificationOutcome {
			notifications = append(notifications, request.Params.ToAddress)
			return SendNotificationOutcome{Status: "failed", Err: errors.New("wake failed")}
		},
	)
	if err != nil {
		t.Fatalf("ExecuteSendBatch() error = %v", err)
	}
	if want := []string{"agent-deck/one", "agent-deck/two", "agent-deck/three"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("send calls = %v, want %v", calls, want)
	}
	if want := []string{"agent-deck/one", "agent-deck/three"}; !reflect.DeepEqual(notifications, want) {
		t.Fatalf("notification calls = %v, want %v", notifications, want)
	}
	if batch.SentCount != 2 || batch.FailedCount != 1 || batch.Status() != "partial_failed" {
		t.Fatalf("batch = %+v, want 2 sent, 1 failed, partial_failed", batch)
	}
	if len(batch.Items) != 3 || batch.Items[1].Err == nil || batch.Items[1].Notification != nil {
		t.Fatalf("batch items = %+v, want durable failure only at index 1", batch.Items)
	}
	if batch.Items[0].Notification == nil || batch.Items[0].Notification.Err == nil {
		t.Fatalf("first notification = %+v, want informational failure", batch.Items[0].Notification)
	}
}

func TestExecuteSendBatchStopsOnCancellationBeforeAnotherTarget(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	batch, err := ExecuteSendBatch(
		ctx,
		sendBatchSenderFunc(func(_ context.Context, params SendParams) (SendResult, error) {
			calls++
			cancel()
			return SendResult{DeliveryID: "dlv_" + params.ToAddress}, nil
		}),
		SendParams{Body: []byte("body")},
		[]string{"agent-deck/one", "agent-deck/two"},
		nil,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ExecuteSendBatch() error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("send calls = %d, want 1", calls)
	}
	if batch.SentCount != 1 || len(batch.Items) != 1 {
		t.Fatalf("batch = %+v, want one completed result", batch)
	}
}

func TestExecuteSendBatchDoesNotSerializeCanceledSendAsItemFailure(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batch, err := ExecuteSendBatch(
		ctx,
		sendBatchSenderFunc(func(_ context.Context, _ SendParams) (SendResult, error) {
			cancel()
			return SendResult{}, context.Canceled
		}),
		SendParams{Body: []byte("body")},
		[]string{"agent-deck/one", "agent-deck/two"},
		nil,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ExecuteSendBatch() error = %v, want context.Canceled", err)
	}
	if len(batch.Items) != 0 || batch.FailedCount != 0 {
		t.Fatalf("batch = %+v, want no synthetic failed item", batch)
	}
}

func TestSendBatchResultEmptyStatusIsNotAttempted(t *testing.T) {
	t.Parallel()

	if got := (SendBatchResult{}).Status(); got != "not_attempted" {
		t.Fatalf("SendBatchResult{}.Status() = %q, want not_attempted", got)
	}
}

func makeRecipientValues(count int) []string {
	values := make([]string, count)
	for index := range values {
		values[index] = fmt.Sprintf("agent-deck/recipient-%d", index)
	}
	return values
}
