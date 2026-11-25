package waypost

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"
)

type WatchParams struct {
	Address   string
	Addresses []string
	State     string
	Timeout   time.Duration
}

type WaitParams struct {
	Address   string
	Addresses []string
	Timeout   time.Duration
}

type GroupWaitParams struct {
	Address string
	Person  string
	Timeout time.Duration
}

func (s *Store) Wait(ctx context.Context, params WaitParams) (ListedDelivery, error) {
	if params.Timeout < 0 {
		return ListedDelivery{}, invalidArgumentError(errors.New("timeout must not be negative"))
	}
	addresses, err := normalizeAddresses(params.Address, params.Addresses, "--for")
	if err != nil {
		return ListedDelivery{}, err
	}

	var deadline time.Time
	if params.Timeout > 0 {
		deadline = time.Now().Add(params.Timeout)
	}

	delay := initialPollDelay
	for {
		attemptCtx := ctx
		cancel := func() {}
		if !deadline.IsZero() {
			attemptCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		delivery, err := s.waitOnce(attemptCtx, addresses)
		cancel()
		if err == nil {
			return delivery, nil
		}
		if !deadline.IsZero() && errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return ListedDelivery{}, ErrNoMessage
		}
		if !errors.Is(err, ErrNoMessage) {
			return ListedDelivery{}, err
		}
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return ListedDelivery{}, ErrNoMessage
			}
			if delay > remaining {
				delay = remaining
			}
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ListedDelivery{}, ctx.Err()
		case <-timer.C:
		}

		if delay < maxPollDelay {
			delay *= 2
			if delay > maxPollDelay {
				delay = maxPollDelay
			}
		}
	}
}

func (s *Store) Watch(ctx context.Context, params WatchParams, emit func(ListedDelivery) error) error {
	if params.Timeout < 0 {
		return invalidArgumentError(errors.New("timeout must not be negative"))
	}
	addresses, err := normalizeAddresses(params.Address, params.Addresses, "--for")
	if err != nil {
		return err
	}

	state, err := normalizeDeliveryStateFilter(params.State)
	if err != nil {
		return err
	}
	emitted := make(map[string]string)
	idleDeadline := time.Time{}
	if params.Timeout > 0 {
		idleDeadline = time.Now().Add(params.Timeout)
	}

	delay := initialPollDelay
	for {
		scope, err := s.resolvePersonal(ctx, s.readDB, addresses)
		if err != nil {
			return err
		}

		observedNewDelivery := false
		nowText := formatTimestamp(s.now())
		var cursorKeys []string
		seenInScan := make(map[string]struct{})
		for {
			items, err := s.listWatchDeliveriesPage(ctx, s.readDB, scope, state, nowText, cursorKeys, MaxPageSize)
			if err != nil {
				return err
			}

			for _, item := range items {
				seenInScan[item.delivery.DeliveryID] = struct{}{}
				fingerprint := watchFingerprint(item)
				if emitted[item.delivery.DeliveryID] == fingerprint {
					continue
				}
				if err := emit(item.delivery); err != nil {
					return err
				}
				emitted[item.delivery.DeliveryID] = fingerprint
				observedNewDelivery = true
			}

			if len(items) < MaxPageSize {
				break
			}
			last := items[len(items)-1].delivery
			cursorKeys = []string{last.VisibleAt, last.MessageCreatedAt, last.DeliveryID}
		}
		for deliveryID := range emitted {
			if _, ok := seenInScan[deliveryID]; !ok {
				delete(emitted, deliveryID)
			}
		}

		if observedNewDelivery {
			delay = initialPollDelay
			if params.Timeout > 0 {
				idleDeadline = time.Now().Add(params.Timeout)
			}
		} else if delay < maxPollDelay {
			delay *= 2
			if delay > maxPollDelay {
				delay = maxPollDelay
			}
		}

		waitFor := delay
		if !idleDeadline.IsZero() {
			remaining := time.Until(idleDeadline)
			if remaining <= 0 {
				return nil
			}
			if waitFor > remaining {
				waitFor = remaining
			}
		}

		timer := time.NewTimer(waitFor)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Store) waitOnce(ctx context.Context, addresses []string) (ListedDelivery, error) {
	scope, err := s.resolvePersonal(ctx, s.readDB, addresses)
	if err != nil {
		return ListedDelivery{}, err
	}

	deliveries, err := s.listClaimablePersonalDeliveriesPage(ctx, s.readDB, scope, formatTimestamp(s.now()), nil, 1)
	if err != nil {
		return ListedDelivery{}, err
	}
	if len(deliveries) == 0 {
		return ListedDelivery{}, ErrNoMessage
	}

	return deliveries[0], nil
}

func watchFingerprint(row personalDeliveryRow) string {
	delivery := row.delivery
	senderEndpointID := ""
	if delivery.SenderEndpointID != nil {
		senderEndpointID = *delivery.SenderEndpointID
	}

	parts := []string{
		delivery.MessageID,
		delivery.RecipientAddress,
		delivery.RecipientEndpointID,
		senderEndpointID,
		delivery.State,
		delivery.VisibleAt,
		delivery.MessageCreatedAt,
		row.leaseToken,
		strconv.Itoa(row.attemptCount),
		delivery.Subject,
		delivery.ContentType,
		delivery.SchemaVersion,
		delivery.BodyBlobRef,
		strconv.FormatInt(delivery.BodySize, 10),
		delivery.BodySHA256,
	}
	return strings.Join(parts, "\x00")
}
