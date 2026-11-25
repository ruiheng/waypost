package waypost

import (
	"context"
	"errors"
	"fmt"
)

// MaxSendRecipients bounds one coordinated send request before duplicate
// recipients are removed. It is deliberately separate from MaxInputItems:
// every send recipient can create durable records and trigger notification.
const MaxSendRecipients = 10

// NormalizeSendRecipients validates a send batch before the first durable
// operation. It preserves normalized first-seen order and removes duplicates.
func NormalizeSendRecipients(values []string, group bool) ([]string, error) {
	if len(values) == 0 {
		return nil, invalidArgumentError(errors.New("recipient address is required"))
	}
	if len(values) > MaxSendRecipients {
		return nil, invalidArgumentError(fmt.Errorf("send accepts at most %d recipients", MaxSendRecipients))
	}

	recipients := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		recipient, err := NormalizeAddress(value)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[recipient]; ok {
			continue
		}
		seen[recipient] = struct{}{}
		recipients = append(recipients, recipient)
	}

	for _, recipient := range recipients {
		if group && !IsGroupAddress(recipient) {
			return nil, invalidArgumentError(fmt.Errorf("invalid group address %q: group addresses must start with group/", recipient))
		}
		if !group && IsGroupAddress(recipient) {
			return nil, invalidArgumentError(fmt.Errorf("endpoint address %q uses reserved group/ prefix", recipient))
		}
	}
	return recipients, nil
}

// SendBatchItem is one attempted recipient's durable and notification outcome.
// A non-nil Err means its durable send failed and Notification is nil.
type SendBatchItem struct {
	ToAddress    string
	Result       SendResult
	Notification *SendNotificationOutcome
	Err          error
}

// SendBatchResult contains completed recipient outcomes in normalized order.
type SendBatchResult struct {
	ToAddresses []string
	Items       []SendBatchItem
	SentCount   int
	FailedCount int
}

// Status returns the durable aggregate status for a completed batch.
func (r SendBatchResult) Status() string {
	switch {
	case r.SentCount == 0 && r.FailedCount == 0:
		return "not_attempted"
	case r.FailedCount == 0:
		return "sent"
	case r.SentCount == 0:
		return "failed"
	default:
		return "partial_failed"
	}
}

// SendBatchNotifier performs the optional best-effort post-send notification.
// Its outcome is informational and never changes durable batch counts.
type SendBatchNotifier func(context.Context, SendNotificationRequest) SendNotificationOutcome

type sendBatchSender interface {
	Send(context.Context, SendParams) (SendResult, error)
}

// ExecuteSendBatch sends recipients sequentially. Ordinary per-recipient
// failures are recorded and do not stop later recipients. Context cancellation
// is batch-wide: it stops before another target and is returned directly rather
// than becoming a synthetic item failure.
func ExecuteSendBatch(
	ctx context.Context,
	sender sendBatchSender,
	base SendParams,
	recipients []string,
	notify SendBatchNotifier,
) (SendBatchResult, error) {
	batch := SendBatchResult{
		ToAddresses: append([]string(nil), recipients...),
		Items:       make([]SendBatchItem, 0, len(recipients)),
	}
	for _, recipient := range recipients {
		if err := ctx.Err(); err != nil {
			return batch, err
		}

		params := base
		params.ToAddress = recipient
		result, err := sender.Send(ctx, params)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return batch, ctxErr
			}
			batch.Items = append(batch.Items, SendBatchItem{ToAddress: recipient, Err: err})
			batch.FailedCount++
			continue
		}

		item := SendBatchItem{ToAddress: recipient, Result: result}
		if notify != nil {
			outcome := notify(ctx, SendNotificationRequest{Params: params, Result: result})
			item.Notification = &outcome
		}
		batch.Items = append(batch.Items, item)
		batch.SentCount++
	}
	return batch, nil
}

// SendBatchIncompleteError reports a completed CLI batch that had one or more
// durable failures. The normal batch envelope has already been written.
type SendBatchIncompleteError struct {
	FailedCount    int
	RecipientCount int
}

func (e *SendBatchIncompleteError) Error() string {
	return fmt.Sprintf(
		"send batch failed for %d of %d recipients; inspect stdout results",
		e.FailedCount,
		e.RecipientCount,
	)
}
