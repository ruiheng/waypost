package mcpserver

import (
	"context"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

type WakeChannelName string

const (
	WakeChannelAgentDeck       WakeChannelName = "agent_deck"
	WakeChannelThurbox         WakeChannelName = "thurbox"
	WakeHintMCPResourceUpdated WakeChannelName = "mcp_resource_updated"
)

type WakeTarget struct {
	Channel WakeChannelName
	Target  string
}

type wakeScope struct {
	ScopeID          string
	WaypostAddresses []string
	WakeTargets      []WakeTarget
}

type wakeSnapshot struct {
	ScopeID                   string
	HasPendingVisibleDelivery bool
	PendingSince              string
	BoundAddresses            []string
	WakeTargets               []WakeTarget
	QueuedVisibleCount        int
}

type visibleWaypostState struct {
	QueuedVisibleCount int
	OldestEligibleAt   string
}

type queuedWakeState struct {
	QueuedAt   string
	Until      string
	QueueKnown bool
}

type wakeRuntime struct {
	ScopeID           string
	PendingSince      string
	LastAnyWakeAt     string
	LastWakeByChannel map[WakeChannelName]string
	QueuedByChannel   map[WakeChannelName]queuedWakeState
}

type wakeSchedulerState struct {
	mu       sync.Mutex
	runtimes map[string]wakeRuntime
}

type wakeChannelConfig struct {
	Category     string
	InitialDelay time.Duration
	Cooldown     time.Duration
}

func newWakeSchedulerState() *wakeSchedulerState {
	return &wakeSchedulerState{
		runtimes: map[string]wakeRuntime{},
	}
}

func (s *Service) startWakeSchedulerLoop() {
	if s.disableWakeScheduler {
		return
	}
	scope, err := s.currentLocalWakeScope(s.ctx)
	if err != nil || scope == nil {
		return
	}
	s.startBackgroundLoop(&s.wakeSchedulerLoopOnce, s.runWakeSchedulerLoop)
}

func (s *Service) runWakeSchedulerLoop() {
	_ = s.processWakeScheduler(s.ctx)
	ticker := time.NewTicker(s.wakePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.ctx.Err() != nil {
				return
			}
			_ = s.processWakeScheduler(s.ctx)
		}
	}
}

func (s *Service) processWakeScheduler(ctx context.Context) error {
	scope, err := s.currentLocalWakeScope(ctx)
	if err != nil {
		return err
	}
	if scope == nil {
		s.wakeSchedulerState.reset()
		return nil
	}

	snapshot, err := s.currentWakeSnapshot(ctx, *scope)
	if err != nil {
		return err
	}
	if !snapshot.HasPendingVisibleDelivery {
		s.wakeSchedulerState.reset()
		return nil
	}

	now := s.now().UTC()
	runtime := s.wakeSchedulerState.runtimeForScope(scope.ScopeID, snapshot.PendingSince)
	if s.tryWakeChannel(ctx, snapshot, runtime, now, WakeHintMCPResourceUpdated) {
		return nil
	}
	if s.tryWakeChannel(ctx, snapshot, runtime, now, WakeChannelAgentDeck) {
		return nil
	}
	if s.tryWakeChannel(ctx, snapshot, runtime, now, WakeChannelThurbox) {
		return nil
	}

	s.wakeSchedulerState.pruneExcept([]string{scope.ScopeID})
	return nil
}

func (s *Service) tryWakeChannel(ctx context.Context, snapshot wakeSnapshot, runtime wakeRuntime, now time.Time, channel WakeChannelName) bool {
	config := wakeChannelConfigs[channel]
	if allowed, reason := allowWakeAttempt(snapshot, runtime, now, channel, config); !allowed {
		logWakeSuppressed(snapshot, runtime, now, channel, config.Category, "", reason)
		return false
	}

	switch channel {
	case WakeHintMCPResourceUpdated:
		if !s.overviewSubscriptions.hasLiveSubscribers(s.Server()) {
			logWakeSuppressed(snapshot, runtime, now, channel, config.Category, waypostOverviewURI, "unavailable:no_subscribers")
			return false
		}
		outcome := s.waypostOverviewEmitter(ctx)
		logWakeAttempt(snapshot, now, channel, config.Category, waypostOverviewURI, outcome)
		if notificationOutcomeAttempted(outcome) {
			s.wakeSchedulerState.markAttempted(snapshot.ScopeID, channel, now, snapshot.PendingSince)
			return true
		}
		return false
	case WakeChannelAgentDeck, WakeChannelThurbox:
		timeout := s.targetedWakeTimeout
		if timeout <= 0 {
			timeout = defaultTargetedWakeTimeout
		}
		wakeCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		attemptedWakeableTarget := false
		budgetExhausted := false
		for _, target := range snapshotTargetsForChannel(snapshot, channel) {
			if wakeCtx.Err() != nil {
				budgetExhausted = true
				break
			}
			manager := notificationManagerForWakeChannel(target.Channel)
			if manager == "" {
				continue
			}
			route := notificationRoute{
				Manager: manager,
				Target:  target.Target,
			}
			probe := s.notifications.probeRouteWithRetry(wakeCtx, route)
			if !probe.Wakeable {
				reason := "unavailable:" + probe.Status
				if wakeCtx.Err() != nil {
					reason = "unavailable:channel_budget_exhausted"
					budgetExhausted = true
				}
				logWakeSuppressed(snapshot, runtime, now, channel, config.Category, target.Target, reason)
				if budgetExhausted {
					break
				}
				continue
			}
			if wakeCtx.Err() != nil {
				budgetExhausted = true
				logWakeSuppressed(snapshot, runtime, now, channel, config.Category, target.Target, "unavailable:channel_budget_exhausted")
				break
			}
			if probe.PendingNudge {
				logWakeSuppressed(snapshot, runtime, now, channel, config.Category, target.Target, "suppressed:queued_nudge_pending")
				s.wakeSchedulerState.markQueuedPending(snapshot.ScopeID, channel, now, snapshot.PendingSince)
				return false
			}
			if probe.QueueKnown {
				s.wakeSchedulerState.clearQueued(snapshot.ScopeID, channel)
			}
			attemptedWakeableTarget = true
			outcome := s.notifications.notifyRoute(wakeCtx, notificationEvent{
				Kind:  notificationFallbackWake,
				Route: route,
			})
			logWakeAttempt(snapshot, now, channel, config.Category, target.Target, outcome)
			if notificationOutcomeAttempted(outcome) {
				s.wakeSchedulerState.markAttemptedOutcome(snapshot.ScopeID, channel, now, snapshot.PendingSince, outcome, probe.QueueKnown)
				return true
			}
			if wakeCtx.Err() != nil {
				budgetExhausted = true
				break
			}
		}
		if budgetExhausted {
			logWakeSuppressed(snapshot, runtime, now, channel, config.Category, "", "unavailable:channel_budget_exhausted")
			return false
		}
		if attemptedWakeableTarget {
			logWakeSuppressed(snapshot, runtime, now, channel, config.Category, "", "delivery_failed:all_targets")
			return false
		}
		logWakeSuppressed(snapshot, runtime, now, channel, config.Category, "", "unavailable:no_wakeable_target")
		return false
	default:
		return false
	}
}

func (s *Service) currentLocalWakeScope(ctx context.Context) (*wakeScope, error) {
	ctx = ctxOrBackground(ctx)
	if err := s.sessions.tryAutoBindCurrentSession(ctx); err != nil {
		return nil, err
	}
	snapshot := s.sessions.snapshotState()
	if len(snapshot.BoundAddresses) == 0 {
		return nil, nil
	}
	targets, err := resolveWakeTargets(snapshot.BoundAddresses)
	if err != nil {
		return nil, err
	}
	scopeID := localWakeScopeID(snapshot)
	// THURBOX_SESSION identifies the immediate nested host. Keep every bound
	// Waypost address in the accounting scope, but never turn an outer Agent
	// Deck identity into an additional wake target for the nested worker.
	if thurboxSessionID, _ := detectCurrentThurboxSessionID(); thurboxSessionID != "" {
		targets = []WakeTarget{{
			Channel: WakeChannelThurbox,
			Target:  thurboxSessionID,
		}}
		scopeID = "local/thurbox/" + thurboxSessionID
	}
	return &wakeScope{
		ScopeID:          scopeID,
		WaypostAddresses: append([]string(nil), snapshot.BoundAddresses...),
		WakeTargets:      targets,
	}, nil
}

func directWakeScopeForAddress(address string) (*wakeScope, string, error) {
	target, ok, scheme, err := wakeTargetForAddress(address)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, scheme, nil
	}
	return &wakeScope{
		ScopeID:          "direct/" + strings.TrimSpace(address),
		WaypostAddresses: []string{strings.TrimSpace(address)},
		WakeTargets:      []WakeTarget{target},
	}, scheme, nil
}

func localWakeScopeID(snapshot stateSnapshot) string {
	switch {
	case strings.TrimSpace(snapshot.DetectedThurboxSession) != "":
		return "local/thurbox/" + strings.TrimSpace(snapshot.DetectedThurboxSession)
	case strings.TrimSpace(snapshot.DetectedAgentDeckSession) != "":
		return "local/agent-deck/" + strings.TrimSpace(snapshot.DetectedAgentDeckSession)
	case strings.TrimSpace(snapshot.DetectedToolSessions["codex"]) != "":
		return "local/agent/" + strings.TrimSpace(snapshot.DetectedToolSessions["codex"])
	default:
		addresses := append([]string(nil), snapshot.BoundAddresses...)
		sort.Strings(addresses)
		return "local/bound/" + strings.Join(addresses, ",")
	}
}

func resolveWakeTargets(addresses []string) ([]WakeTarget, error) {
	targets := make([]WakeTarget, 0, len(addresses))
	seen := map[string]bool{}
	for _, address := range addresses {
		target, ok, _, err := wakeTargetForAddress(address)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		key := string(target.Channel) + "\x00" + target.Target
		if seen[key] {
			continue
		}
		seen[key] = true
		targets = append(targets, target)
	}
	return targets, nil
}

func wakeTargetForAddress(address string) (WakeTarget, bool, string, error) {
	parsed, err := parseAddress(address)
	if err != nil {
		return WakeTarget{}, false, "", err
	}
	switch parsed.Scheme {
	case "agent-deck":
		return WakeTarget{Channel: WakeChannelAgentDeck, Target: parsed.ID}, true, parsed.Scheme, nil
	case "thurbox":
		return WakeTarget{Channel: WakeChannelThurbox, Target: parsed.ID}, true, parsed.Scheme, nil
	default:
		return WakeTarget{}, false, parsed.Scheme, nil
	}
}

func (s *Service) currentWakeSnapshot(ctx context.Context, scope wakeScope) (wakeSnapshot, error) {
	visible, err := s.visibleWaypostSnapshot(ctx, scope.WaypostAddresses)
	if err != nil {
		return wakeSnapshot{}, err
	}
	return wakeSnapshot{
		ScopeID:                   scope.ScopeID,
		HasPendingVisibleDelivery: visible.QueuedVisibleCount > 0,
		PendingSince:              visible.OldestEligibleAt,
		BoundAddresses:            append([]string(nil), scope.WaypostAddresses...),
		WakeTargets:               append([]WakeTarget(nil), scope.WakeTargets...),
		QueuedVisibleCount:        visible.QueuedVisibleCount,
	}, nil
}

func (s *Service) visibleWaypostSnapshot(ctx context.Context, addresses []string) (visibleWaypostState, error) {
	if len(addresses) == 0 {
		return visibleWaypostState{}, nil
	}
	return withWaypostService(ctx, s.waypostServices, func(service waypostClaimableLister) (visibleWaypostState, error) {
		state := visibleWaypostState{}
		claimable, err := service.ListClaimableAddresses(ctx, addresses)
		if err != nil {
			return visibleWaypostState{}, err
		}
		for _, address := range claimable {
			state.QueuedVisibleCount += address.ClaimableCount
			if state.OldestEligibleAt == "" || address.OldestEligibleAt < state.OldestEligibleAt {
				state.OldestEligibleAt = address.OldestEligibleAt
			}
		}
		return state, nil
	})
}

func snapshotTargetsForChannel(snapshot wakeSnapshot, channel WakeChannelName) []WakeTarget {
	targets := make([]WakeTarget, 0, len(snapshot.WakeTargets))
	for _, target := range snapshot.WakeTargets {
		if target.Channel == channel {
			targets = append(targets, target)
		}
	}
	return targets
}

func (s wakeScope) targetsForChannel(channel WakeChannelName) []WakeTarget {
	targets := make([]WakeTarget, 0, len(s.WakeTargets))
	for _, target := range s.WakeTargets {
		if target.Channel == channel {
			targets = append(targets, target)
		}
	}
	return targets
}

var wakeChannelConfigs = map[WakeChannelName]wakeChannelConfig{
	WakeHintMCPResourceUpdated: {
		Category:     "local_hint",
		InitialDelay: defaultMCPHintInitialDelay,
		Cooldown:     defaultMCPHintCooldown,
	},
	WakeChannelAgentDeck: {
		Category:     "targeted_wake",
		InitialDelay: defaultAgentDeckInitialDelay,
		Cooldown:     defaultAgentDeckCooldown,
	},
	WakeChannelThurbox: {
		Category:     "targeted_wake",
		InitialDelay: defaultAgentDeckInitialDelay,
		Cooldown:     defaultAgentDeckCooldown,
	},
}

func notificationManagerForWakeChannel(channel WakeChannelName) string {
	switch channel {
	case WakeChannelAgentDeck:
		return "agent-deck"
	case WakeChannelThurbox:
		return "thurbox"
	default:
		return ""
	}
}

func allowWakeAttempt(snapshot wakeSnapshot, runtime wakeRuntime, now time.Time, channel WakeChannelName, config wakeChannelConfig) (bool, string) {
	if snapshot.PendingSince == "" {
		return false, "suppressed:pending_since_missing"
	}
	pendingSince, err := time.Parse(time.RFC3339Nano, snapshot.PendingSince)
	if err != nil {
		return false, "suppressed:pending_since_invalid"
	}
	if now.Before(pendingSince.Add(config.InitialDelay)) {
		return false, "suppressed:initial_delay"
	}
	if inWakeCooldown(runtime.LastAnyWakeAt, now, defaultWakeInterChannelGap) {
		return false, "suppressed:inter_channel_gap"
	}
	if inWakeCooldown(runtime.LastWakeByChannel[channel], now, config.Cooldown) {
		return false, "suppressed:channel_cooldown"
	}
	if hasUnprobedQueuedWake(snapshot, runtime, now, channel) {
		return false, "suppressed:queued_nudge_pending"
	}
	return true, ""
}

func hasUnprobedQueuedWake(snapshot wakeSnapshot, runtime wakeRuntime, now time.Time, channel WakeChannelName) bool {
	queued, ok := runtime.QueuedByChannel[channel]
	if !ok || queued.QueueKnown {
		return false
	}
	pendingSince, err := time.Parse(time.RFC3339Nano, snapshot.PendingSince)
	if err != nil {
		return false
	}
	queuedAt, err := time.Parse(time.RFC3339Nano, queued.QueuedAt)
	if err != nil {
		return false
	}
	until, err := time.Parse(time.RFC3339Nano, queued.Until)
	if err != nil {
		return false
	}
	return !pendingSince.After(queuedAt) && now.Before(until)
}

func inWakeCooldown(lastWakeAt string, now time.Time, cooldown time.Duration) bool {
	if cooldown <= 0 || strings.TrimSpace(lastWakeAt) == "" {
		return false
	}
	wakeAt, err := time.Parse(time.RFC3339Nano, lastWakeAt)
	if err != nil {
		return false
	}
	return now.Sub(wakeAt) < cooldown
}

func logWakeSuppressed(snapshot wakeSnapshot, runtime wakeRuntime, now time.Time, channel WakeChannelName, category, target, reason string) {
	log.Printf(
		"mcpserver wake_suppressed scope_id=%s channel=%s category=%s target=%s reason=%s pending_since=%s last_any_wake_at=%s last_channel_wake_at=%s addresses=%q now=%s",
		snapshot.ScopeID,
		channel,
		category,
		target,
		reason,
		snapshot.PendingSince,
		runtime.LastAnyWakeAt,
		runtime.LastWakeByChannel[channel],
		strings.Join(snapshot.BoundAddresses, ","),
		now.Format(time.RFC3339Nano),
	)
}

func logWakeAttempt(snapshot wakeSnapshot, now time.Time, channel WakeChannelName, category, target string, outcome notificationOutcome) {
	errText := ""
	if outcome.Err != nil {
		errText = outcome.Err.Error()
	}
	log.Printf(
		"mcpserver wake_attempt scope_id=%s channel=%s category=%s target=%s status=%s scheme=%s pending_since=%s addresses=%q now=%s detail=%q err=%q",
		snapshot.ScopeID,
		channel,
		category,
		target,
		outcome.Status,
		outcome.Scheme,
		snapshot.PendingSince,
		strings.Join(snapshot.BoundAddresses, ","),
		now.Format(time.RFC3339Nano),
		outcome.Detail,
		errText,
	)
}

func (s *wakeSchedulerState) ensureRuntimeLocked(scopeID, pendingSince string) wakeRuntime {
	runtime := s.runtimes[scopeID]
	runtime.ScopeID = scopeID
	runtime.PendingSince = pendingSince
	if runtime.LastWakeByChannel == nil {
		runtime.LastWakeByChannel = map[WakeChannelName]string{}
	}
	if runtime.QueuedByChannel == nil {
		runtime.QueuedByChannel = map[WakeChannelName]queuedWakeState{}
	}
	return runtime
}

func (s *wakeSchedulerState) runtimeForScope(scopeID, pendingSince string) wakeRuntime {
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime := s.ensureRuntimeLocked(scopeID, pendingSince)
	s.runtimes[scopeID] = runtime

	snapshot := runtime
	snapshot.LastWakeByChannel = make(map[WakeChannelName]string, len(runtime.LastWakeByChannel))
	for k, v := range runtime.LastWakeByChannel {
		snapshot.LastWakeByChannel[k] = v
	}
	snapshot.QueuedByChannel = make(map[WakeChannelName]queuedWakeState, len(runtime.QueuedByChannel))
	for k, v := range runtime.QueuedByChannel {
		snapshot.QueuedByChannel[k] = v
	}
	return snapshot
}

func (s *wakeSchedulerState) markAttempted(scopeID string, channel WakeChannelName, now time.Time, pendingSince string) {
	s.markAttemptedOutcome(scopeID, channel, now, pendingSince, notificationOutcome{}, false)
}

func (s *wakeSchedulerState) markAttemptedOutcome(scopeID string, channel WakeChannelName, now time.Time, pendingSince string, outcome notificationOutcome, queueKnown bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime := s.ensureRuntimeLocked(scopeID, pendingSince)
	timestamp := now.Format(time.RFC3339Nano)
	runtime.LastAnyWakeAt = timestamp
	runtime.LastWakeByChannel[channel] = timestamp
	if outcome.Queued {
		runtime.QueuedByChannel[channel] = queuedWakeState{
			QueuedAt:   timestamp,
			Until:      resolveQueuedWakeUntil(outcome.QueuedDeadline, now),
			QueueKnown: queueKnown,
		}
	} else {
		delete(runtime.QueuedByChannel, channel)
	}
	s.runtimes[scopeID] = runtime
}

func (s *wakeSchedulerState) markQueuedPending(scopeID string, channel WakeChannelName, now time.Time, pendingSince string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime := s.ensureRuntimeLocked(scopeID, pendingSince)
	timestamp := now.Format(time.RFC3339Nano)
	runtime.LastWakeByChannel[channel] = timestamp
	runtime.QueuedByChannel[channel] = queuedWakeState{
		QueuedAt:   timestamp,
		Until:      now.Add(defaultAgentDeckQueueBudget).Format(time.RFC3339Nano),
		QueueKnown: true,
	}
	s.runtimes[scopeID] = runtime
}

func (s *wakeSchedulerState) clearQueued(scopeID string, channel WakeChannelName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime, ok := s.runtimes[scopeID]
	if !ok || runtime.QueuedByChannel == nil {
		return
	}
	delete(runtime.QueuedByChannel, channel)
	s.runtimes[scopeID] = runtime
}

func resolveQueuedWakeUntil(rawDeadline string, now time.Time) string {
	if trimmed := strings.TrimSpace(rawDeadline); trimmed != "" {
		if deadline, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
			return deadline.UTC().Format(time.RFC3339Nano)
		}
	}
	return now.Add(defaultAgentDeckQueueBudget).Format(time.RFC3339Nano)
}

func (s *wakeSchedulerState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runtimes = map[string]wakeRuntime{}
}

func (s *wakeSchedulerState) pruneExcept(activeScopeIDs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keep := make(map[string]bool, len(activeScopeIDs))
	for _, scopeID := range activeScopeIDs {
		keep[scopeID] = true
	}
	for scopeID := range s.runtimes {
		if !keep[scopeID] {
			delete(s.runtimes, scopeID)
		}
	}
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}
