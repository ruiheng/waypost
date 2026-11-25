package mcpserver

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/ruiheng/waypost/internal/waypost"
)

type activeLease struct {
	DeliveryID       string
	RecipientAddress string
	LeaseToken       string
	LeaseExpiresAt   string
	Subject          string
	ContentType      string
	ClaimedAt        string
	LastRenewedAt    string
	Status           string
	TerminalAt       string
}

type leaseRenewalFailure struct {
	DeliveryID string
	Cause      error
	Definitive bool
}

func (e *leaseRenewalFailure) Error() string {
	return "lease ownership is no longer guaranteed after renewal failure"
}

func (e *leaseRenewalFailure) Unwrap() error {
	return e.Cause
}

type trackedLeaseEntry struct {
	lease   activeLease
	active  bool
	failure *leaseRenewalFailure
}

type activeLeaseManager struct {
	mu        sync.Mutex
	entries   map[string]trackedLeaseEntry
	activeIDs map[string]struct{}
	lastError error
}

func newActiveLeaseManager() *activeLeaseManager {
	return &activeLeaseManager{
		entries:   map[string]trackedLeaseEntry{},
		activeIDs: map[string]struct{}{},
	}
}

func (m *activeLeaseManager) setActiveIDLocked(deliveryID string, active bool) {
	if active {
		if m.activeIDs == nil {
			m.activeIDs = map[string]struct{}{}
		}
		m.activeIDs[deliveryID] = struct{}{}
		return
	}
	delete(m.activeIDs, deliveryID)
}

func (m *activeLeaseManager) recomputeLastErrorLocked() {
	deliveryIDs := make([]string, 0, len(m.activeIDs))
	for deliveryID := range m.activeIDs {
		entry, ok := m.entries[deliveryID]
		if ok && entry.active && entry.failure != nil {
			deliveryIDs = append(deliveryIDs, deliveryID)
		}
	}
	if len(deliveryIDs) == 0 {
		m.lastError = nil
		return
	}
	sort.Strings(deliveryIDs)
	var firstFailure *leaseRenewalFailure
	for _, deliveryID := range deliveryIDs {
		failure := m.entries[deliveryID].failure
		if firstFailure == nil {
			firstFailure = failure
		}
		if failure.Definitive {
			copied := *failure
			m.lastError = &copied
			return
		}
	}
	copied := *firstFailure
	m.lastError = &copied
}

func (m *activeLeaseManager) trackReceive(result waypost.ReceiveResult, claimedAt string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, message := range result.Messages {
		deliveryID := strings.TrimSpace(message.DeliveryID)
		leaseToken := strings.TrimSpace(message.LeaseToken)
		if deliveryID == "" {
			continue
		}
		lease := activeLease{
			DeliveryID:       deliveryID,
			RecipientAddress: message.RecipientAddress,
			LeaseToken:       leaseToken,
			LeaseExpiresAt:   message.LeaseExpiresAt,
			Subject:          message.Subject,
			ContentType:      message.ContentType,
			ClaimedAt:        claimedAt,
			Status:           "active",
		}
		m.entries[deliveryID] = trackedLeaseEntry{
			lease:  lease,
			active: true,
		}
		m.setActiveIDLocked(deliveryID, true)
	}
	m.recomputeLastErrorLocked()
}

func (m *activeLeaseManager) updateRenewed(expectedToken string, result waypost.LeaseRenewResult, renewedAt string) {
	deliveryID := strings.TrimSpace(result.DeliveryID)
	expectedToken = strings.TrimSpace(expectedToken)
	newToken := strings.TrimSpace(result.LeaseToken)
	if deliveryID == "" || expectedToken == "" || newToken == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[deliveryID]
	if !ok || !entry.active || entry.lease.LeaseToken != expectedToken {
		return
	}
	entry.lease.LeaseToken = newToken
	entry.lease.LeaseExpiresAt = result.LeaseExpiresAt
	entry.lease.LastRenewedAt = renewedAt
	entry.failure = nil
	m.entries[deliveryID] = entry
	m.recomputeLastErrorLocked()
}

func (m *activeLeaseManager) markTerminal(deliveryID, expectedToken, status, terminalAt string) {
	deliveryID = strings.TrimSpace(deliveryID)
	expectedToken = strings.TrimSpace(expectedToken)
	if deliveryID == "" || expectedToken == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[deliveryID]
	if !ok || !entry.active || entry.lease.LeaseToken != expectedToken {
		return
	}
	entry.active = false
	entry.failure = nil
	entry.lease.Status = status
	entry.lease.TerminalAt = terminalAt
	entry.lease.LeaseToken = ""
	m.entries[deliveryID] = entry
	m.setActiveIDLocked(deliveryID, false)
	m.recomputeLastErrorLocked()
}

// reconcileTrackedLeases makes durable delivery state the single authority for
// MCP lease ownership. The caller supplies the storage inspection so this
// cache stays independent of any particular runtime implementation.
func (m *activeLeaseManager) reconcileTrackedLeases(ctx context.Context, observedAt string, inspect func(context.Context, string) (waypost.DeliveryLeaseState, error)) error {
	m.mu.Lock()
	snaps := make([]activeLease, 0, len(m.activeIDs))
	for deliveryID := range m.activeIDs {
		if entry, ok := m.entries[deliveryID]; ok && entry.active {
			snaps = append(snaps, entry.lease)
		}
	}
	if len(snaps) == 0 {
		m.recomputeLastErrorLocked()
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].DeliveryID < snaps[j].DeliveryID
	})

	type reconciledLease struct {
		snap    activeLease
		durable waypost.DeliveryLeaseState
	}
	results := make([]reconciledLease, 0, len(snaps))
	for _, snap := range snaps {
		durable, err := inspect(ctx, snap.DeliveryID)
		if err != nil {
			// Keep existing entries intact for a later retry without partial
			// mutation. Presenting cached state as authoritative after an
			// inspection error would be worse.
			return err
		}
		results = append(results, reconciledLease{snap: snap, durable: durable})
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, item := range results {
		entry, ok := m.entries[item.snap.DeliveryID]
		if !ok || !entry.active || entry.lease.LeaseToken != item.snap.LeaseToken {
			continue
		}
		if item.durable.Found && item.durable.State == "leased" && item.durable.LeaseToken == item.snap.LeaseToken {
			continue
		}

		status := "missing"
		terminalAt := observedAt
		if item.durable.Found {
			if item.durable.State == "leased" {
				status = "lease_replaced"
			} else {
				status = item.durable.State
			}
			if item.durable.TransitionAt != "" {
				terminalAt = item.durable.TransitionAt
			}
		}

		entry.active = false
		entry.failure = nil
		entry.lease.Status = status
		entry.lease.TerminalAt = terminalAt
		entry.lease.LeaseToken = ""
		m.entries[item.snap.DeliveryID] = entry
		m.setActiveIDLocked(item.snap.DeliveryID, false)
	}
	m.recomputeLastErrorLocked()
	return nil
}

func (m *activeLeaseManager) snapshot() []activeLease {
	m.mu.Lock()
	defer m.mu.Unlock()

	leases := make([]activeLease, 0, len(m.activeIDs))
	for deliveryID := range m.activeIDs {
		if entry, ok := m.entries[deliveryID]; ok && entry.active {
			leases = append(leases, entry.lease)
		}
	}
	return leases
}

func (m *activeLeaseManager) historySnapshot(includeTerminal bool) []activeLease {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !includeTerminal {
		leases := make([]activeLease, 0, len(m.activeIDs))
		for deliveryID := range m.activeIDs {
			if entry, ok := m.entries[deliveryID]; ok && entry.active {
				leases = append(leases, entry.lease)
			}
		}
		return leases
	}
	leases := make([]activeLease, 0, len(m.entries))
	for _, entry := range m.entries {
		leases = append(leases, entry.lease)
	}
	return leases
}

func (m *activeLeaseManager) hasTrackedLeases() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.activeIDs) > 0
}

func (m *activeLeaseManager) isActive(deliveryID, leaseToken string) bool {
	deliveryID = strings.TrimSpace(deliveryID)
	leaseToken = strings.TrimSpace(leaseToken)
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.entries[deliveryID]
	return ok && entry.active && entry.lease.LeaseToken == leaseToken
}

func (m *activeLeaseManager) markRenewalFailure(lease activeLease, cause error) error {
	return m.markRenewalFailureWithSeverity(lease, cause, renewalFailureDefinitive(cause))
}

func (m *activeLeaseManager) markRenewalFailureWithSeverity(lease activeLease, cause error, definitive bool) error {
	deliveryID := strings.TrimSpace(lease.DeliveryID)
	leaseToken := strings.TrimSpace(lease.LeaseToken)
	failure := &leaseRenewalFailure{
		DeliveryID: deliveryID,
		Cause:      cause,
		Definitive: definitive,
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// A definitive renewal response is reconciled against durable state by the
	// caller before this method runs, and an ABA token mismatch means the
	// tracked entry has already moved on.
	entry, ok := m.entries[deliveryID]
	if !ok || !entry.active || entry.lease.LeaseToken != leaseToken {
		return failure
	}
	// Preserve monotonic severity: never downgrade a definitive failure to a
	// transient one while the lease is still waiting to settle.
	if entry.failure == nil || definitive || !entry.failure.Definitive {
		copied := *failure
		entry.failure = &copied
		m.entries[deliveryID] = entry
	}
	m.recomputeLastErrorLocked()
	if entry.failure != nil {
		copied := *entry.failure
		return &copied
	}
	return failure
}

func (m *activeLeaseManager) terminalMutationAllowed(deliveryID string) error {
	deliveryID = strings.TrimSpace(deliveryID)
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.entries[deliveryID]
	if !ok || !entry.active || entry.failure == nil || !entry.failure.Definitive {
		return nil
	}
	copied := *entry.failure
	return &copied
}

func (m *activeLeaseManager) lastRenewalError() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastError
}

func isLeaseRenewalFailure(err error) bool {
	var target *leaseRenewalFailure
	return errors.As(err, &target)
}

func renewalFailureDefinitive(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, waypost.ErrLeaseExpired) ||
		errors.Is(err, waypost.ErrLeaseNotFound) ||
		errors.Is(err, waypost.ErrLeaseNotLeased) ||
		errors.Is(err, waypost.ErrLeaseRenewChanged) ||
		errors.Is(err, waypost.ErrLeaseTokenMismatch)
}
