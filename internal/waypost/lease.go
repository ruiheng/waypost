package waypost

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

const (
	defaultLeaseTimeout     = 5 * time.Minute
	initialPollDelay        = 50 * time.Millisecond
	maxPollDelay            = time.Second
	maxClaimRetryCount      = 3
	claimRetryDelay         = 10 * time.Millisecond
	maxDeliveryAttemptCount = 3
	maxReceiveBatchSize     = 10
)

var (
	ErrNoMessage               = errors.New("no message available")
	ErrClaimContention         = errors.New("receive claim contention")
	ErrBodyIntegrity           = errors.New("body blob failed integrity check")
	ErrLeaseExpired            = errors.New("lease expired")
	ErrLeaseNotFound           = errors.New("lease delivery not found")
	ErrLeaseNotLeased          = errors.New("delivery is not leased")
	ErrLeaseRenewChanged       = errors.New("lease changed while renewing")
	ErrLeaseTokenMismatch      = errors.New("lease token does not match current lease")
	ErrReceiveRecovery         = errors.New("receive recovery failed")
	ErrReceiveRecoveryRequired = errors.New("receive recovery required")
)

type leaseSentinelError struct {
	text string
	err  error
}

func (e leaseSentinelError) Error() string {
	return e.text
}

func (e leaseSentinelError) Unwrap() error {
	return e.err
}

func leaseError(text string, err error) error {
	return leaseSentinelError{text: text, err: err}
}

type ClaimContentionError struct {
	Attempts int
	Cause    error
}

func (e *ClaimContentionError) Error() string {
	return fmt.Sprintf("receive claim contention after %d attempts: %v", e.Attempts, e.Cause)
}

func (e *ClaimContentionError) Unwrap() []error {
	return []error{ErrClaimContention, e.Cause}
}

const (
	ReceiveRecoveryPhaseRemainingState = "remaining_state"
	ReceiveRecoveryPhaseClaimRecovery  = "claim_recovery"
)

type ReceiveRecoveryError struct {
	DeliveryID  string
	ReceiveErr  error
	RecoveryErr error
	Claim       *ReceivedMessage
}

// ReceiveRecoveryRequiredError reports a receive failure that left one or more
// newly claimed deliveries leased. Callers must settle every claim before
// attempting another receive.
type ReceiveRecoveryRequiredError struct {
	Cause  error
	Claims []ReceivedMessage
	Phase  string
}

func (e *ReceiveRecoveryRequiredError) Error() string {
	if e.Phase == ReceiveRecoveryPhaseClaimRecovery {
		return "receive recovery failed and left claimed deliveries leased"
	}
	return "remaining-state query failed and claim rollback was incomplete"
}

func (e *ReceiveRecoveryRequiredError) Unwrap() []error {
	return []error{ErrReceiveRecoveryRequired, e.Cause}
}

func (e *ReceiveRecoveryError) Error() string {
	return fmt.Sprintf("receive recovery failed for delivery %q: read error: %v: recovery error: %v", e.DeliveryID, e.ReceiveErr, e.RecoveryErr)
}

func (e *ReceiveRecoveryError) Unwrap() []error {
	return []error{ErrReceiveRecovery, e.ReceiveErr, e.RecoveryErr}
}

type ReceiveParams struct {
	Address   string
	Addresses []string
}

type GroupReceiveParams struct {
	Address string
	Person  string
}

type ReceiveBatchParams struct {
	Address   string
	Addresses []string
	Max       int
}

type receiveLeasePolicy struct {
	LeaseTTL time.Duration
}

type ReceivedMessage struct {
	DeliveryID           string  `json:"delivery_id"`
	MessageID            string  `json:"message_id"`
	ForwardedMessageID   *string `json:"-"`
	ForwardedFromAddress *string `json:"forwarded_from_address,omitempty"`
	RecipientAddress     string  `json:"recipient_address"`
	RecipientEndpointID  string  `json:"recipient_endpoint_id"`
	SenderEndpointID     *string `json:"sender_endpoint_id,omitempty"`
	SenderAddress        *string `json:"sender_address,omitempty"`
	State                string  `json:"state"`
	VisibleAt            string  `json:"visible_at"`
	LeaseToken           string  `json:"lease_token"`
	LeaseExpiresAt       string  `json:"lease_expires_at"`
	AttemptCount         int     `json:"attempt_count"`
	MessageCreatedAt     string  `json:"message_created_at"`
	Subject              string  `json:"subject"`
	ContentType          string  `json:"content_type"`
	SchemaVersion        string  `json:"schema_version"`
	BodyBlobRef          string  `json:"body_blob_ref"`
	BodySize             int64   `json:"body_size"`
	BodySHA256           string  `json:"body_sha256"`
	Body                 string  `json:"body"`
}

type GroupReceivedMessage struct {
	MessageID            string  `json:"message_id"`
	ForwardedMessageID   *string `json:"-"`
	ForwardedFromAddress *string `json:"forwarded_from_address,omitempty"`
	GroupID              string  `json:"group_id"`
	GroupAddress         string  `json:"group_address"`
	Person               string  `json:"person"`
	SenderEndpointID     *string `json:"sender_endpoint_id,omitempty"`
	SenderAddress        *string `json:"sender_address,omitempty"`
	MessageCreatedAt     string  `json:"message_created_at"`
	Subject              string  `json:"subject"`
	ContentType          string  `json:"content_type"`
	SchemaVersion        string  `json:"schema_version"`
	BodyBlobRef          string  `json:"body_blob_ref"`
	BodySize             int64   `json:"body_size"`
	BodySHA256           string  `json:"body_sha256"`
	Body                 string  `json:"body"`
	ReadCount            int     `json:"read_count"`
	EligibleCount        int     `json:"eligible_count"`
	FirstReadAt          string  `json:"first_read_at"`
}

type ReceiveResult struct {
	Messages         []ReceivedMessage `json:"messages"`
	RemainingByState map[string]int    `json:"remaining_by_state,omitempty"`
}

// DeliveryLeaseState is the durable delivery state used to reconcile a live
// MCP lease cache. A missing delivery has Found set to false.
type DeliveryLeaseState struct {
	Found        bool
	State        string
	LeaseToken   string
	TransitionAt string
}

type DeliveryTransitionResult struct {
	DeliveryID   string `json:"delivery_id"`
	State        string `json:"state"`
	VisibleAt    string `json:"visible_at,omitempty"`
	AckedAt      string `json:"acked_at,omitempty"`
	AttemptCount int    `json:"attempt_count"`
}

type LeaseRenewResult struct {
	DeliveryID     string `json:"delivery_id"`
	LeaseToken     string `json:"lease_token"`
	LeaseExpiresAt string `json:"lease_expires_at"`
}

type leasedDeliveryRecord struct {
	DeliveryID          string
	MessageID           string
	RecipientEndpointID string
	State               string
	VisibleAt           string
	LeaseToken          sql.NullString
	LeaseExpiresAt      sql.NullString
	AttemptCount        int
}

type deliveryTransitionSpec struct {
	State                string
	VisibleAt            string
	AckedAt              any
	AttemptCount         int
	LastErrorText        any
	PrimaryEventType     string
	PrimaryEventDetail   map[string]any
	SecondaryEventType   string
	SecondaryEventDetail map[string]any
}

var legacyReceiveLeasePolicy = receiveLeasePolicy{LeaseTTL: defaultLeaseTimeout}

func (s *Store) Receive(ctx context.Context, params ReceiveParams) (ReceivedMessage, error) {
	addresses, err := normalizeAddresses(params.Address, params.Addresses, "--for")
	if err != nil {
		return ReceivedMessage{}, err
	}

	message, err := s.receiveOnceWithLeasePolicy(ctx, addresses, legacyReceiveLeasePolicy)
	if err != nil {
		var recoveryErr *ReceiveRecoveryError
		if errors.As(err, &recoveryErr) && recoveryErr.Claim != nil {
			return ReceivedMessage{}, &ReceiveRecoveryRequiredError{
				Cause:  recoveryErr,
				Claims: []ReceivedMessage{*recoveryErr.Claim},
				Phase:  ReceiveRecoveryPhaseClaimRecovery,
			}
		}
		return ReceivedMessage{}, err
	}
	return message, nil
}

func (s *Store) ReceiveBatch(ctx context.Context, params ReceiveBatchParams) (ReceiveResult, error) {
	addresses, err := normalizeAddresses(params.Address, params.Addresses, "--for")
	if err != nil {
		return ReceiveResult{}, err
	}

	maxMessages := params.Max
	if maxMessages < 1 || maxMessages > maxReceiveBatchSize {
		return ReceiveResult{}, invalidArgumentError(fmt.Errorf("--max must be between 1 and %d", maxReceiveBatchSize))
	}

	return s.receiveBatchWithLeasePolicy(ctx, addresses, maxMessages, legacyReceiveLeasePolicy)
}

func (s *Store) receiveBatchWithLeasePolicy(ctx context.Context, addresses []string, maxMessages int, policy receiveLeasePolicy) (ReceiveResult, error) {
	return s.receiveBatchWithLeasePolicyAndRemainingCounter(ctx, addresses, maxMessages, policy, s.remainingByStateForScope)
}

func (s *Store) receiveBatchWithLeasePolicyAndRemainingCounter(ctx context.Context, addresses []string, maxMessages int, policy receiveLeasePolicy, remainingCounter func(context.Context, personalAvailabilityScope, []string) (map[string]int, error)) (ReceiveResult, error) {
	policy, err := normalizeReceiveLeasePolicy(policy)
	if err != nil {
		return ReceiveResult{}, err
	}
	scope, err := s.resolvePersonal(ctx, s.readDB, addresses)
	if err != nil {
		return ReceiveResult{}, err
	}

	messages := make([]ReceivedMessage, 0, maxMessages)
	if !scope.empty() {
		for len(messages) < maxMessages {
			message, err := s.claimNextDelivery(ctx, scope, policy)
			if err == nil {
				messages = append(messages, message)
				continue
			}
			if errors.Is(err, ErrNoMessage) {
				break
			}
			if errors.Is(err, ErrReceiveRecovery) || errors.Is(err, ErrClaimContention) ||
				errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				unreleased, releaseErr := s.rollbackReceivedBatchMessages(ctx, messages)
				var recoveryErr *ReceiveRecoveryError
				if errors.As(err, &recoveryErr) && recoveryErr.Claim != nil {
					unreleased = append(unreleased, *recoveryErr.Claim)
				}
				if len(unreleased) > 0 {
					return ReceiveResult{}, &ReceiveRecoveryRequiredError{
						Cause:  errors.Join(err, releaseErr),
						Claims: unreleased,
						Phase:  ReceiveRecoveryPhaseClaimRecovery,
					}
				}
				if releaseErr != nil {
					return ReceiveResult{}, fmt.Errorf("%w (rollback failed: %v)", err, releaseErr)
				}
				return ReceiveResult{}, err
			}
			if len(messages) > 0 {
				break
			}
			return ReceiveResult{}, err
		}
	}

	remainingByState, err := remainingCounter(ctx, scope, receivedDeliveryIDs(messages))
	if err != nil {
		unreleased, rollbackErr := s.rollbackReceivedBatchMessages(ctx, messages)
		if len(unreleased) > 0 {
			return ReceiveResult{}, &ReceiveRecoveryRequiredError{
				Cause:  errors.Join(err, rollbackErr),
				Claims: unreleased,
				Phase:  ReceiveRecoveryPhaseRemainingState,
			}
		}
		if rollbackErr != nil {
			return ReceiveResult{}, fmt.Errorf("%w (rollback failed: %v)", err, rollbackErr)
		}
		return ReceiveResult{}, err
	}

	result := ReceiveResult{
		Messages:         messages,
		RemainingByState: remainingByState,
	}
	if len(messages) == 0 {
		return result, ErrNoMessage
	}
	return result, nil
}

func (s *Store) Ack(ctx context.Context, deliveryID, leaseToken string) (DeliveryTransitionResult, error) {
	return s.transitionLeasedDelivery(ctx, deliveryID, leaseToken, func(now time.Time, delivery leasedDeliveryRecord) (deliveryTransitionSpec, error) {
		transitionAt := formatTimestamp(now)
		return deliveryTransitionSpec{
			State:            "acked",
			VisibleAt:        transitionAt,
			AckedAt:          transitionAt,
			AttemptCount:     delivery.AttemptCount,
			LastErrorText:    nil,
			PrimaryEventType: "delivery_acked",
			PrimaryEventDetail: map[string]any{
				"previous_state":   delivery.State,
				"lease_expires_at": delivery.LeaseExpiresAt.String,
			},
		}, nil
	})
}

func (s *Store) Release(ctx context.Context, deliveryID, leaseToken string) (DeliveryTransitionResult, error) {
	return s.transitionLeasedDelivery(ctx, deliveryID, leaseToken, func(now time.Time, delivery leasedDeliveryRecord) (deliveryTransitionSpec, error) {
		visibleAt := formatTimestamp(now)
		return deliveryTransitionSpec{
			State:            "queued",
			VisibleAt:        visibleAt,
			AttemptCount:     delivery.AttemptCount,
			LastErrorText:    nil,
			PrimaryEventType: "delivery_released",
			PrimaryEventDetail: map[string]any{
				"previous_state":   delivery.State,
				"visible_at":       visibleAt,
				"lease_expires_at": delivery.LeaseExpiresAt.String,
			},
		}, nil
	})
}

func (s *Store) Defer(ctx context.Context, deliveryID, leaseToken string, until time.Time) (DeliveryTransitionResult, error) {
	return s.transitionLeasedDelivery(ctx, deliveryID, leaseToken, func(now time.Time, delivery leasedDeliveryRecord) (deliveryTransitionSpec, error) {
		if !until.UTC().After(now) {
			return deliveryTransitionSpec{}, invalidArgumentError(errors.New("defer time must be in the future"))
		}
		visibleAt := formatTimestamp(until)
		return deliveryTransitionSpec{
			State:            "queued",
			VisibleAt:        visibleAt,
			AttemptCount:     delivery.AttemptCount,
			LastErrorText:    nil,
			PrimaryEventType: "delivery_deferred",
			PrimaryEventDetail: map[string]any{
				"previous_state":   delivery.State,
				"visible_at":       visibleAt,
				"lease_expires_at": delivery.LeaseExpiresAt.String,
			},
		}, nil
	})
}

func (s *Store) Undefer(ctx context.Context, deliveryID string) (DeliveryTransitionResult, error) {
	deliveryID, err := validateDeliveryIDInput(deliveryID)
	if err != nil {
		return DeliveryTransitionResult{}, err
	}

	now := s.now()
	visibleAt := formatTimestamp(now)
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return DeliveryTransitionResult{}, fmt.Errorf("begin undefer transaction: %w", err)
	}
	defer tx.Rollback()

	delivery, err := loadDeliveryRecord(ctx, tx, deliveryID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DeliveryTransitionResult{}, missingResourceError(fmt.Sprintf("delivery %q not found", deliveryID), ErrDeliveryNotFound)
		}
		return DeliveryTransitionResult{}, fmt.Errorf("load delivery %q: %w", deliveryID, err)
	}
	if delivery.State != "queued" {
		return DeliveryTransitionResult{}, invalidStateError(fmt.Errorf("delivery %q is in state %q, want queued", deliveryID, delivery.State))
	}
	if delivery.VisibleAt <= visibleAt {
		return DeliveryTransitionResult{}, invalidStateError(fmt.Errorf("delivery %q is already visible", deliveryID))
	}

	result, err := tx.ExecContext(ctx, `
UPDATE deliveries
SET visible_at = ?
WHERE delivery_id = ?
  AND state = 'queued'
  AND visible_at = ?
`, visibleAt, delivery.DeliveryID, delivery.VisibleAt)
	if err != nil {
		return DeliveryTransitionResult{}, fmt.Errorf("undefer delivery %q: %w", deliveryID, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return DeliveryTransitionResult{}, fmt.Errorf("read undefer rows affected for %q: %w", deliveryID, err)
	}
	if rowsAffected == 0 {
		return DeliveryTransitionResult{}, invalidStateError(fmt.Errorf("delivery %q changed while undeferring", deliveryID))
	}

	eventTimestamp := formatTimestamp(now)
	if err := insertDeliveryEvent(ctx, tx, "delivery_undeferred", delivery.RecipientEndpointID, delivery.MessageID, delivery.DeliveryID, map[string]any{
		"previous_state":      delivery.State,
		"previous_visible_at": delivery.VisibleAt,
		"visible_at":          visibleAt,
	}, eventTimestamp); err != nil {
		return DeliveryTransitionResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return DeliveryTransitionResult{}, fmt.Errorf("commit undefer for %q: %w", deliveryID, err)
	}

	return DeliveryTransitionResult{
		DeliveryID:   delivery.DeliveryID,
		State:        "queued",
		VisibleAt:    visibleAt,
		AttemptCount: delivery.AttemptCount,
	}, nil
}

func (s *Store) Fail(ctx context.Context, deliveryID, leaseToken, reason string) (DeliveryTransitionResult, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return DeliveryTransitionResult{}, invalidArgumentError(errors.New("failure reason is required"))
	}

	return s.transitionLeasedDelivery(ctx, deliveryID, leaseToken, func(now time.Time, delivery leasedDeliveryRecord) (deliveryTransitionSpec, error) {
		attemptCount := delivery.AttemptCount + 1
		visibleAt := formatTimestamp(now)
		nextState := "queued"
		secondaryEventType := ""
		var secondaryEventDetail map[string]any
		if attemptCount >= maxDeliveryAttemptCount {
			nextState = "dead_letter"
			secondaryEventType = "delivery_dead_letter"
			secondaryEventDetail = map[string]any{
				"reason":        reason,
				"attempt_count": attemptCount,
			}
		}

		return deliveryTransitionSpec{
			State:            nextState,
			VisibleAt:        visibleAt,
			AttemptCount:     attemptCount,
			LastErrorText:    reason,
			PrimaryEventType: "delivery_failed",
			PrimaryEventDetail: map[string]any{
				"previous_state": delivery.State,
				"reason":         reason,
				"attempt_count":  attemptCount,
				"next_state":     nextState,
			},
			SecondaryEventType:   secondaryEventType,
			SecondaryEventDetail: secondaryEventDetail,
		}, nil
	})
}

// DeadLetter permanently stops retrying a leased delivery without recording
// another processing attempt. The current lease token is required so only the
// active owner can make this terminal decision.
func (s *Store) DeadLetter(ctx context.Context, deliveryID, leaseToken, reason string) (DeliveryTransitionResult, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return DeliveryTransitionResult{}, invalidArgumentError(errors.New("dead-letter reason is required"))
	}

	return s.transitionLeasedDelivery(ctx, deliveryID, leaseToken, func(now time.Time, delivery leasedDeliveryRecord) (deliveryTransitionSpec, error) {
		return deliveryTransitionSpec{
			State:            "dead_letter",
			VisibleAt:        formatTimestamp(now),
			AttemptCount:     delivery.AttemptCount,
			LastErrorText:    reason,
			PrimaryEventType: "delivery_dead_letter",
			PrimaryEventDetail: map[string]any{
				"previous_state": delivery.State,
				"reason":         reason,
				"attempt_count":  delivery.AttemptCount,
				"manual":         true,
			},
		}, nil
	})
}

func (s *Store) Renew(ctx context.Context, deliveryID, leaseToken string, extendBy time.Duration) (LeaseRenewResult, error) {
	deliveryID, leaseToken, err := validateLeaseMutationInput(deliveryID, leaseToken)
	if err != nil {
		return LeaseRenewResult{}, err
	}
	if extendBy <= 0 {
		return LeaseRenewResult{}, invalidArgumentError(errors.New("renew duration must be greater than 0"))
	}

	now := s.now()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return LeaseRenewResult{}, fmt.Errorf("begin renew transaction: %w", err)
	}
	defer tx.Rollback()

	delivery, err := loadCurrentLease(ctx, tx, deliveryID, leaseToken)
	if err != nil {
		return LeaseRenewResult{}, err
	}

	newLeaseExpiresAt := formatTimestamp(now.Add(extendBy))
	result, err := tx.ExecContext(ctx, `
UPDATE deliveries
SET lease_expires_at = ?
WHERE delivery_id = ?
  AND state = 'leased'
  AND lease_token = ?
  AND lease_expires_at = ?
`, newLeaseExpiresAt, delivery.DeliveryID, leaseToken, delivery.LeaseExpiresAt.String)
	if err != nil {
		return LeaseRenewResult{}, fmt.Errorf("update delivery %q: %w", deliveryID, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return LeaseRenewResult{}, fmt.Errorf("read renew rows affected for %q: %w", deliveryID, err)
	}
	if rowsAffected == 0 {
		return LeaseRenewResult{}, leaseError(fmt.Sprintf("delivery %q changed while renewing", deliveryID), ErrLeaseRenewChanged)
	}

	eventTimestamp := formatTimestamp(now)
	if err := insertDeliveryEvent(ctx, tx, "delivery_lease_renewed", delivery.RecipientEndpointID, delivery.MessageID, delivery.DeliveryID, map[string]any{
		"previous_lease_expires_at": delivery.LeaseExpiresAt.String,
		"lease_expires_at":          newLeaseExpiresAt,
	}, eventTimestamp); err != nil {
		return LeaseRenewResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return LeaseRenewResult{}, fmt.Errorf("commit renew for %q: %w", deliveryID, err)
	}

	return LeaseRenewResult{
		DeliveryID:     delivery.DeliveryID,
		LeaseToken:     leaseToken,
		LeaseExpiresAt: newLeaseExpiresAt,
	}, nil
}

// InspectDeliveryLease reads the durable authority for one tracked lease. It
// intentionally does not infer ownership from in-memory state.
func (s *Store) InspectDeliveryLease(ctx context.Context, deliveryID string) (DeliveryLeaseState, error) {
	var state string
	var leaseToken sql.NullString
	err := s.readDB.QueryRowContext(ctx, `
SELECT state, lease_token
FROM deliveries
WHERE delivery_id = ?
`, deliveryID).Scan(&state, &leaseToken)
	if errors.Is(err, sql.ErrNoRows) {
		return DeliveryLeaseState{}, nil
	}
	if err != nil {
		return DeliveryLeaseState{}, fmt.Errorf("inspect delivery lease %q: %w", deliveryID, err)
	}

	result := DeliveryLeaseState{
		Found: true,
		State: state,
	}
	if leaseToken.Valid {
		result.LeaseToken = leaseToken.String
	}

	// The latest delivery event is the durable state-transition timestamp for
	// a terminal or replacement observation. If an older database lacks that
	// event, reconciliation records its observation time instead.
	var transitionAt string
	err = s.readDB.QueryRowContext(ctx, `
SELECT created_at
FROM events
WHERE delivery_id = ?
ORDER BY created_at DESC, event_id DESC
LIMIT 1
`, deliveryID).Scan(&transitionAt)
	if err == nil {
		result.TransitionAt = transitionAt
	} else if !errors.Is(err, sql.ErrNoRows) {
		return DeliveryLeaseState{}, fmt.Errorf("inspect delivery lease event %q: %w", deliveryID, err)
	}
	return result, nil
}

func (s *Store) receiveOnceWithLeasePolicy(ctx context.Context, addresses []string, policy receiveLeasePolicy) (ReceivedMessage, error) {
	policy, err := normalizeReceiveLeasePolicy(policy)
	if err != nil {
		return ReceivedMessage{}, err
	}

	scope, err := s.resolvePersonal(ctx, s.readDB, addresses)
	if err != nil {
		return ReceivedMessage{}, err
	}
	if scope.empty() {
		return ReceivedMessage{}, ErrNoMessage
	}

	return s.claimNextDelivery(ctx, scope, policy)
}

func normalizeReceiveLeasePolicy(policy receiveLeasePolicy) (receiveLeasePolicy, error) {
	if policy.LeaseTTL <= 0 {
		return receiveLeasePolicy{}, invalidArgumentError(errors.New("receive lease ttl must be greater than 0"))
	}
	return policy, nil
}

func (s *Store) claimNextDelivery(ctx context.Context, scope personalAvailabilityScope, policy receiveLeasePolicy) (ReceivedMessage, error) {
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return ReceivedMessage{}, err
		}
		nowText := formatTimestamp(s.now())
		candidate, err := s.claimablePersonalDelivery(ctx, s.readDB, scope, nowText)
		if errors.Is(err, sql.ErrNoRows) {
			return ReceivedMessage{}, ErrNoMessage
		}
		if err != nil {
			return ReceivedMessage{}, fmt.Errorf("query claimable delivery: %w", err)
		}

		message, err := s.claimNextDeliveryOnce(ctx, scope, candidate, nowText, policy)
		if err == nil {
			return message, nil
		}
		if errors.Is(err, ErrReceiveRecovery) || !isSQLiteBusy(err) {
			return ReceivedMessage{}, err
		}
		if attempt > maxClaimRetryCount {
			return ReceivedMessage{}, &ClaimContentionError{
				Attempts: attempt,
				Cause:    err,
			}
		}
		if err := waitForClaimRetry(ctx); err != nil {
			return ReceivedMessage{}, err
		}
	}
}

func (s *Store) claimNextDeliveryOnce(ctx context.Context, scope personalAvailabilityScope, candidate personalDeliveryRecord, nowText string, policy receiveLeasePolicy) (ReceivedMessage, error) {
	for {
		if err := ctx.Err(); err != nil {
			return ReceivedMessage{}, err
		}
		tx, err := s.claimDB.BeginTx(ctx, nil)
		if err != nil {
			return ReceivedMessage{}, fmt.Errorf("begin receive transaction: %w", err)
		}

		leaseToken, err := newPrefixedID("lease")
		if err != nil {
			_ = tx.Rollback()
			return ReceivedMessage{}, err
		}
		leaseExpiresAt := formatTimestamp(s.now().Add(policy.LeaseTTL))

		result, err := tx.ExecContext(ctx, `
UPDATE deliveries
SET state = 'leased',
    lease_token = ?,
    lease_expires_at = ?,
    acked_at = NULL
WHERE delivery_id = ?
  AND recipient_endpoint_id = ?
  AND (
    (state = 'queued' AND visible_at <= ?)
    OR
    (state = 'leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
  )
`, leaseToken, leaseExpiresAt, candidate.DeliveryID, candidate.RecipientEndpointID, nowText, nowText)
		if err != nil {
			_ = tx.Rollback()
			return ReceivedMessage{}, fmt.Errorf("claim delivery: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			return ReceivedMessage{}, fmt.Errorf("read claim rows affected: %w", err)
		}
		if rowsAffected == 0 {
			_ = tx.Rollback()
			refreshedCandidate, err := s.claimablePersonalDelivery(ctx, s.readDB, scope, nowText)
			if errors.Is(err, sql.ErrNoRows) {
				return ReceivedMessage{}, ErrNoMessage
			}
			if err != nil {
				return ReceivedMessage{}, fmt.Errorf("query claimable delivery: %w", err)
			}
			candidate = refreshedCandidate
			continue
		}

		address := scope.addressByEndpointID[candidate.RecipientEndpointID]
		if err := insertDeliveryEvent(ctx, tx, "delivery_leased", candidate.RecipientEndpointID, candidate.MessageID, candidate.DeliveryID, deliveryLeasedEventDetail(ctx, candidate.State, address, leaseExpiresAt), nowText); err != nil {
			_ = tx.Rollback()
			return ReceivedMessage{}, err
		}
		if err := ctx.Err(); err != nil {
			_ = tx.Rollback()
			return ReceivedMessage{}, err
		}

		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return ReceivedMessage{}, fmt.Errorf("commit receive transaction: %w", err)
		}

		claim := ReceivedMessage{
			DeliveryID:          candidate.DeliveryID,
			MessageID:           candidate.MessageID,
			RecipientAddress:    address,
			RecipientEndpointID: candidate.RecipientEndpointID,
			State:               "leased",
			VisibleAt:           candidate.VisibleAt,
			LeaseToken:          leaseToken,
			LeaseExpiresAt:      leaseExpiresAt,
			AttemptCount:        candidate.AttemptCount,
			MessageCreatedAt:    candidate.MessageCreatedAt,
			Subject:             candidate.Subject,
			ContentType:         candidate.ContentType,
			SchemaVersion:       candidate.SchemaVersion,
			BodyBlobRef:         candidate.BodyBlobRef,
			BodySize:            candidate.BodySize,
			BodySHA256:          candidate.BodySHA256,
		}
		if candidate.ForwardedMessageID.Valid {
			claim.ForwardedMessageID = &candidate.ForwardedMessageID.String
		}
		if candidate.ForwardedFromAddress.Valid {
			claim.ForwardedFromAddress = &candidate.ForwardedFromAddress.String
		}
		if candidate.SenderEndpointID.Valid {
			claim.SenderEndpointID = &candidate.SenderEndpointID.String
		}
		if candidate.SenderAddress.Valid {
			claim.SenderAddress = &candidate.SenderAddress.String
		}

		body, err := s.readBlob(candidate.BodyBlobRef, candidate.BodySize, candidate.BodySHA256)
		if err != nil {
			return ReceivedMessage{}, s.recoverReceiveFailure(ctx, claim, err)
		}

		claim.Body = string(body)
		return claim, nil
	}
}

func deliveryLeasedEventDetail(ctx context.Context, previousState, recipientAddress, leaseExpiresAt string) map[string]any {
	metadata := claimMetadataFromContext(ctx)
	source := metadata.Source
	if source == "" {
		source = "unknown"
	}
	detail := map[string]any{
		"previous_state":    previousState,
		"recipient_address": recipientAddress,
		"lease_expires_at":  leaseExpiresAt,
		"reclaimed":         previousState == "leased",
		"claim_source":      source,
		"claim_pid":         os.Getpid(),
	}
	if metadata.Tool != "" {
		detail["claim_tool"] = metadata.Tool
	}
	if len(metadata.BoundAddresses) > 0 {
		detail["claim_bound_addresses"] = metadata.BoundAddresses
	}
	if metadata.AgentDeckSessionID != "" {
		detail["claim_agent_deck_session_id"] = metadata.AgentDeckSessionID
	}
	if metadata.AgentSessionID != "" {
		detail["claim_agent_session_id"] = metadata.AgentSessionID
	}
	if metadata.Workdir != "" {
		detail["claim_workdir"] = metadata.Workdir
	}
	return detail
}

func waitForClaimRetry(ctx context.Context) error {
	timer := time.NewTimer(claimRetryDelay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func receivedDeliveryIDs(messages []ReceivedMessage) []string {
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.DeliveryID)
	}
	return ids
}

const leaseRecoveryCleanupTimeout = 5 * time.Second

func leaseOwnershipDefinitelyLost(err error) bool {
	return errors.Is(err, ErrLeaseTokenMismatch) ||
		errors.Is(err, ErrLeaseNotLeased) ||
		errors.Is(err, ErrLeaseNotFound)
}

func (s *Store) rollbackReceivedBatchMessages(ctx context.Context, messages []ReceivedMessage) ([]ReceivedMessage, error) {
	var rollbackErrs []error
	unreleased := make([]ReceivedMessage, 0)
	for _, message := range messages {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseRecoveryCleanupTimeout)
		_, err := s.transitionLeasedDelivery(cleanupCtx, message.DeliveryID, message.LeaseToken, func(_ time.Time, delivery leasedDeliveryRecord) (deliveryTransitionSpec, error) {
			return deliveryTransitionSpec{
				State:            "queued",
				VisibleAt:        message.VisibleAt,
				AckedAt:          nil,
				AttemptCount:     delivery.AttemptCount,
				LastErrorText:    nil,
				PrimaryEventType: "delivery_released",
				PrimaryEventDetail: map[string]any{
					"previous_state": delivery.State,
					"state":          "queued",
					"visible_at":     message.VisibleAt,
				},
			}, nil
		})
		cancel()
		if err != nil {
			rollbackErrs = append(rollbackErrs, fmt.Errorf("rollback received batch delivery %q: %w", message.DeliveryID, err))
			if !leaseOwnershipDefinitelyLost(err) {
				unreleased = append(unreleased, message)
			}
		}
	}
	if len(rollbackErrs) == 0 {
		return nil, nil
	}
	return unreleased, errors.Join(rollbackErrs...)
}

func (s *Store) recoverReceiveFailure(ctx context.Context, claim ReceivedMessage, readErr error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseRecoveryCleanupTimeout)
	defer cancel()

	if _, err := s.Fail(cleanupCtx, claim.DeliveryID, claim.LeaseToken, readErr.Error()); err != nil {
		var unreleasedClaim *ReceivedMessage
		if !leaseOwnershipDefinitelyLost(err) {
			claimCopy := claim
			unreleasedClaim = &claimCopy
		}
		return &ReceiveRecoveryError{
			DeliveryID:  claim.DeliveryID,
			ReceiveErr:  readErr,
			RecoveryErr: err,
			Claim:       unreleasedClaim,
		}
	}
	return readErr
}

func (s *Store) hasClaimableDelivery(ctx context.Context, addresses []string) (bool, error) {
	scope, err := s.resolvePersonal(ctx, s.readDB, addresses)
	if err != nil {
		return false, err
	}
	if scope.empty() {
		return false, nil
	}

	nowText := formatTimestamp(s.now())
	if _, err := s.claimablePersonalDelivery(ctx, s.readDB, scope, nowText); errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("query claimable delivery: %w", err)
	}

	return true, nil
}

func (s *Store) transitionLeasedDelivery(ctx context.Context, deliveryID, leaseToken string, build func(time.Time, leasedDeliveryRecord) (deliveryTransitionSpec, error)) (DeliveryTransitionResult, error) {
	deliveryID, leaseToken, err := validateLeaseMutationInput(deliveryID, leaseToken)
	if err != nil {
		return DeliveryTransitionResult{}, err
	}

	now := s.now()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return DeliveryTransitionResult{}, fmt.Errorf("begin delivery transition transaction: %w", err)
	}
	defer tx.Rollback()

	delivery, err := loadCurrentLease(ctx, tx, deliveryID, leaseToken)
	if err != nil {
		return DeliveryTransitionResult{}, err
	}

	spec, err := build(now, delivery)
	if err != nil {
		return DeliveryTransitionResult{}, err
	}

	result, err := tx.ExecContext(ctx, `
UPDATE deliveries
SET state = ?,
    visible_at = ?,
    lease_token = NULL,
    lease_expires_at = NULL,
    acked_at = ?,
    attempt_count = ?,
    last_error_code = NULL,
    last_error_text = ?
WHERE delivery_id = ?
  AND state = 'leased'
  AND lease_token = ?
  AND lease_expires_at = ?
`, spec.State, spec.VisibleAt, spec.AckedAt, spec.AttemptCount, spec.LastErrorText, delivery.DeliveryID, leaseToken, delivery.LeaseExpiresAt.String)
	if err != nil {
		return DeliveryTransitionResult{}, fmt.Errorf("update delivery %q: %w", deliveryID, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return DeliveryTransitionResult{}, fmt.Errorf("read transition rows affected for %q: %w", deliveryID, err)
	}
	if rowsAffected == 0 {
		return DeliveryTransitionResult{}, invalidStateError(fmt.Errorf("delivery %q changed while updating", deliveryID))
	}

	eventTimestamp := formatTimestamp(now)
	if err := insertDeliveryEvent(ctx, tx, spec.PrimaryEventType, delivery.RecipientEndpointID, delivery.MessageID, delivery.DeliveryID, spec.PrimaryEventDetail, eventTimestamp); err != nil {
		return DeliveryTransitionResult{}, err
	}
	if spec.SecondaryEventType != "" {
		if err := insertDeliveryEvent(ctx, tx, spec.SecondaryEventType, delivery.RecipientEndpointID, delivery.MessageID, delivery.DeliveryID, spec.SecondaryEventDetail, eventTimestamp); err != nil {
			return DeliveryTransitionResult{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return DeliveryTransitionResult{}, fmt.Errorf("commit transition for %q: %w", deliveryID, err)
	}

	resultSummary := DeliveryTransitionResult{
		DeliveryID:   delivery.DeliveryID,
		State:        spec.State,
		VisibleAt:    spec.VisibleAt,
		AttemptCount: spec.AttemptCount,
	}
	if ackedAt, ok := spec.AckedAt.(string); ok {
		resultSummary.AckedAt = ackedAt
	}
	return resultSummary, nil
}

func validateLeaseMutationInput(deliveryID, leaseToken string) (string, string, error) {
	deliveryID, err := validateDeliveryIDInput(deliveryID)
	if err != nil {
		return "", "", err
	}
	leaseToken = strings.TrimSpace(leaseToken)
	if leaseToken == "" {
		return "", "", invalidArgumentError(errors.New("lease token is required"))
	}
	return deliveryID, leaseToken, nil
}

func validateDeliveryIDInput(deliveryID string) (string, error) {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return "", invalidArgumentError(errors.New("delivery id is required"))
	}
	return deliveryID, nil
}

// loadCurrentLease intentionally allows renewing or settling an expired lease
// as long as the delivery is still in the leased state and its lease_token
// matches. Another consumer may reclaim the expired lease before then, at
// which point ErrLeaseTokenMismatch is returned to the stale holder.
func loadCurrentLease(ctx context.Context, tx *sql.Tx, deliveryID, leaseToken string) (leasedDeliveryRecord, error) {
	delivery, err := loadDeliveryRecord(ctx, tx, deliveryID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return leasedDeliveryRecord{}, leaseError(fmt.Sprintf("delivery %q not found", deliveryID), ErrLeaseNotFound)
		}
		return leasedDeliveryRecord{}, fmt.Errorf("load delivery %q: %w", deliveryID, err)
	}
	if delivery.State != "leased" {
		return leasedDeliveryRecord{}, leaseError(fmt.Sprintf("delivery %q is in state %q, want leased", deliveryID, delivery.State), ErrLeaseNotLeased)
	}
	if !delivery.LeaseToken.Valid || delivery.LeaseToken.String != leaseToken {
		return leasedDeliveryRecord{}, fmt.Errorf("validate lease token for %q: %w", deliveryID, ErrLeaseTokenMismatch)
	}
	if !delivery.LeaseExpiresAt.Valid {
		return leasedDeliveryRecord{}, fmt.Errorf("delivery %q is missing lease expiry", deliveryID)
	}

	return delivery, nil
}

func isSQLiteBusy(err error) bool {
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	return sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked
}

func loadDeliveryRecord(ctx context.Context, tx *sql.Tx, deliveryID string) (leasedDeliveryRecord, error) {
	var delivery leasedDeliveryRecord
	err := tx.QueryRowContext(ctx, `
SELECT delivery_id, message_id, recipient_endpoint_id, state, visible_at, lease_token, lease_expires_at, attempt_count
FROM deliveries
WHERE delivery_id = ?
`, deliveryID).Scan(
		&delivery.DeliveryID,
		&delivery.MessageID,
		&delivery.RecipientEndpointID,
		&delivery.State,
		&delivery.VisibleAt,
		&delivery.LeaseToken,
		&delivery.LeaseExpiresAt,
		&delivery.AttemptCount,
	)
	if err != nil {
		return leasedDeliveryRecord{}, err
	}
	return delivery, nil
}

func insertDeliveryEvent(ctx context.Context, tx *sql.Tx, eventType, endpointID, messageID, deliveryID string, detail map[string]any, createdAt string) error {
	eventID, err := newPrefixedID("evt")
	if err != nil {
		return err
	}
	detailJSON, err := marshalDetail(detail)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO events (event_id, created_at, event_type, endpoint_id, message_id, delivery_id, detail_json)
VALUES (?, ?, ?, ?, ?, ?, ?)
`, eventID, createdAt, eventType, endpointID, messageID, deliveryID, detailJSON); err != nil {
		return fmt.Errorf("insert %s event: %w", eventType, err)
	}
	return nil
}

func (s *Store) readBlob(blobRef string, expectedSize int64, expectedSHA256 string) ([]byte, error) {
	body, err := os.ReadFile(filepath.Join(s.blobDir, blobRef))
	if err != nil {
		return nil, fmt.Errorf("read body blob %q: %w", blobRef, err)
	}
	if int64(len(body)) != expectedSize {
		return nil, fmt.Errorf("%w for %q: size=%d want=%d", ErrBodyIntegrity, blobRef, len(body), expectedSize)
	}

	bodySHA256 := sha256.Sum256(body)
	if actualSHA256 := hex.EncodeToString(bodySHA256[:]); actualSHA256 != expectedSHA256 {
		return nil, fmt.Errorf("%w for %q: sha256=%s want=%s", ErrBodyIntegrity, blobRef, actualSHA256, expectedSHA256)
	}
	return body, nil
}
