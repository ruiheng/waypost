package mcpserver

import (
	"context"
	"errors"
	"time"

	"github.com/ruiheng/waypost/internal/waypost"
)

const (
	leaseRenewRetryDelay    = 50 * time.Millisecond
	leaseRenewRetryAttempts = 3
)

func (s *Service) startLeaseRenewLoop() {
	if s.disableLeaseRenewLoop || !s.activeLeases.hasTrackedLeases() {
		return
	}
	s.startBackgroundLoop(&s.leaseRenewLoopOnce, s.runLeaseRenewLoop)
}

func (s *Service) runLeaseRenewLoop() {
	ticker := time.NewTicker(s.leaseRenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.ctx.Err() != nil {
				return
			}
			s.processLeaseRenewals(s.ctx)
		}
	}
}

func (s *Service) processLeaseRenewals(ctx context.Context) error {
	if err := s.reconcileTrackedLeases(ctx); err != nil {
		return err
	}
	leases := s.activeLeases.snapshot()
	if len(leases) == 0 {
		return nil
	}

	var firstErr error
	for _, lease := range leases {
		renewed, err := s.renewLeaseWithRetry(ctx, lease)
		if err != nil {
			if renewalFailureDefinitive(err) {
				reconcileErr := s.reconcileTrackedLeases(ctx)
				if reconcileErr != nil && firstErr == nil {
					firstErr = reconcileErr
				}
				if reconcileErr == nil && errors.Is(err, waypost.ErrLeaseRenewChanged) && s.activeLeases.isActive(lease.DeliveryID, lease.LeaseToken) {
					retryRenewed, retryErr := s.renewLeaseWithRetry(ctx, lease)
					if retryErr == nil {
						s.activeLeases.updateRenewed(lease.LeaseToken, retryRenewed, s.now().Format(time.RFC3339Nano))
						continue
					}
					if errors.Is(retryErr, waypost.ErrLeaseRenewChanged) {
						retryReconcileErr := s.reconcileTrackedLeases(ctx)
						if retryReconcileErr != nil && firstErr == nil {
							firstErr = retryReconcileErr
						}
						if retryReconcileErr == nil && s.activeLeases.isActive(lease.DeliveryID, lease.LeaseToken) {
							recorded := s.activeLeases.markRenewalFailureWithSeverity(lease, retryErr, false)
							if firstErr == nil {
								firstErr = recorded
							}
							continue
						}
					} else if renewalFailureDefinitive(retryErr) {
						if retryReconcileErr := s.reconcileTrackedLeases(ctx); retryReconcileErr != nil && firstErr == nil {
							firstErr = retryReconcileErr
						}
					}
					err = retryErr
				}
			}
			recorded := s.activeLeases.markRenewalFailure(lease, err)
			if firstErr == nil {
				firstErr = recorded
			}
			continue
		}
		s.activeLeases.updateRenewed(lease.LeaseToken, renewed, s.now().Format(time.RFC3339Nano))
	}
	return firstErr
}

func (s *Service) reconcileTrackedLeases(ctx context.Context) error {
	_, err := withWaypostService(ctx, s.waypostServices, func(service waypostLeaseInspector) (struct{}, error) {
		return struct{}{}, s.activeLeases.reconcileTrackedLeases(ctx, s.now().Format(time.RFC3339Nano), service.InspectDeliveryLease)
	})
	return err
}

func (s *Service) renewLeaseWithRetry(ctx context.Context, lease activeLease) (waypost.LeaseRenewResult, error) {
	var lastErr error
	for attempt := 0; attempt < leaseRenewRetryAttempts; attempt++ {
		renewed, err := withWaypostService(ctx, s.waypostServices, func(service waypostLeaseRenewer) (waypost.LeaseRenewResult, error) {
			return service.Renew(ctx, lease.DeliveryID, lease.LeaseToken, s.mcpLeaseTTL)
		})
		if err == nil {
			return renewed, nil
		}
		lastErr = err
		if renewalFailureDefinitive(err) || attempt == leaseRenewRetryAttempts-1 {
			break
		}
		if err := waitForLeaseRenewRetry(ctx, leaseRenewRetryDelay); err != nil {
			return waypost.LeaseRenewResult{}, err
		}
	}
	return waypost.LeaseRenewResult{}, lastErr
}

func waitForLeaseRenewRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
