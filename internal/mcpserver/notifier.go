package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type notificationRoute struct {
	Manager string
	Target  string
}

type notificationEvent struct {
	Kind                 string
	Route                notificationRoute
	Subject              string
	Body                 string
	DisableNotifyMessage *bool
}

type notificationOutcome struct {
	Status         string
	Scheme         string
	Detail         string
	Err            error
	Queued         bool
	QueuedDeadline string
}

type notificationProbe struct {
	Status       string
	Scheme       string
	Wakeable     bool
	QueueKnown   bool
	PendingNudge bool
	Err          error
}

type managerNotifier interface {
	Name() string
	Probe(ctx context.Context, route notificationRoute) notificationProbe
	Notify(ctx context.Context, event notificationEvent) notificationOutcome
}

type notificationManager struct {
	notifiers              map[string]managerNotifier
	sessions               *sessionManager
	retryWait              func(context.Context, time.Duration) error
	groupNotifyTimeout     time.Duration
	groupNotifyConcurrency int
}

type agentDeckNotifier struct {
	runner   Runner
	sessions *sessionManager
	now      func() time.Time
}

type thurboxNotifier struct {
	runner Runner
}

func newNotificationManager(runner Runner, sessions *sessionManager) *notificationManager {
	return newNotificationManagerWithClock(runner, sessions, nil)
}

func newNotificationManagerWithClock(runner Runner, sessions *sessionManager, now func() time.Time) *notificationManager {
	if now == nil {
		now = func() time.Time {
			return time.Now().UTC()
		}
	}
	manager := &notificationManager{
		notifiers:              map[string]managerNotifier{},
		sessions:               sessions,
		retryWait:              waitForNotificationRetry,
		groupNotifyTimeout:     groupNotifyFanoutTimeout,
		groupNotifyConcurrency: groupNotifyFanoutConcurrency,
	}
	manager.notifiers["agent-deck"] = agentDeckNotifier{
		runner:   runner,
		sessions: sessions,
		now:      now,
	}
	manager.notifiers["thurbox"] = thurboxNotifier{runner: runner}
	return manager
}

func (m *notificationManager) notifyWaypostSend(ctx context.Context, input waypostSendInput) notificationOutcome {
	if m.sessions.isLocalAddress(ctx, input.ToAddress) {
		return notificationOutcome{Status: "skipped_local"}
	}

	scope, scheme, err := directWakeScopeForAddress(input.ToAddress)
	if err != nil {
		return notificationOutcome{Status: "failed", Err: err}
	}
	if scope == nil {
		return notificationOutcome{Status: "unsupported", Scheme: scheme}
	}

	return m.notifyDirectWakeScope(ctx, *scope, input)
}

func (m *notificationManager) notifyGroupSubscribers(ctx context.Context, input waypostSendInput, notifyAddresses []string) notificationOutcome {
	if len(notifyAddresses) == 0 {
		return notificationOutcome{Status: "no_subscribers"}
	}
	if wakeNotifyDisabled(input.DisableNotifyMessage) {
		return notificationOutcome{
			Status: "skipped_disabled",
			Scheme: notificationSchemeForAddresses(notifyAddresses),
		}
	}

	timeout := m.groupNotifyTimeout
	if timeout <= 0 {
		timeout = groupNotifyFanoutTimeout
	}
	fanoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	results := make([]groupSubscriberNotificationResult, len(notifyAddresses))
	workerCount := m.groupNotifyConcurrency
	if workerCount <= 0 {
		workerCount = groupNotifyFanoutConcurrency
	}
	if workerCount > len(notifyAddresses) {
		workerCount = len(notifyAddresses)
	}
	jobs := make(chan int, len(notifyAddresses))
	for index := range notifyAddresses {
		jobs <- index
	}
	close(jobs)

	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				results[index] = m.notifyGroupSubscriber(fanoutCtx, input, notifyAddresses[index])
			}
		}()
	}
	workers.Wait()

	return aggregateGroupSubscriberNotifications(results)
}

type groupSubscriberNotificationResult struct {
	attempted bool
	schemes   []string
	status    string
	delivered bool
	detail    string
	err       error
}

func (m *notificationManager) notifyGroupSubscriber(ctx context.Context, input waypostSendInput, notifyAddress string) groupSubscriberNotificationResult {
	result := groupSubscriberNotificationResult{}
	if strings.TrimSpace(notifyAddress) == strings.TrimSpace(input.FromAddress) {
		return result
	}
	result.attempted = true

	scope, scheme, err := directWakeScopeForAddress(notifyAddress)
	if scheme != "" {
		result.schemes = append(result.schemes, scheme)
	}
	if err != nil {
		result.status = "failed"
		result.err = err
		return result
	}
	if scope == nil {
		result.status = "unsupported"
		return result
	}
	if err := ctx.Err(); err != nil {
		result.status = "failed"
		result.err = fmt.Errorf("notify %s: group notification budget exhausted: %w", strings.TrimSpace(notifyAddress), err)
		return result
	}

	candidate := m.notifyDirectWakeScope(ctx, *scope, input)
	if candidate.Scheme != "" {
		result.schemes = append(result.schemes, candidate.Scheme)
	}
	result.status = strings.TrimSpace(candidate.Status)
	result.delivered = notificationOutcomeDelivered(candidate)
	result.detail = candidate.Detail
	if !result.delivered {
		result.err = candidate.Err
	}
	return result
}

func aggregateGroupSubscriberNotifications(results []groupSubscriberNotificationResult) notificationOutcome {
	attemptedCount := 0
	sentCount := 0
	unconfirmedCount := 0
	schemes := map[string]bool{}
	statuses := map[string]bool{}
	var failures []error
	var details []string
	for _, result := range results {
		if !result.attempted {
			continue
		}
		attemptedCount++
		for _, scheme := range result.schemes {
			if scheme != "" {
				schemes[scheme] = true
			}
		}
		if result.status != "" {
			statuses[result.status] = true
		}
		if result.status == "unconfirmed" {
			unconfirmedCount++
		}
		if result.delivered {
			sentCount++
		} else if result.err != nil {
			failures = append(failures, result.err)
		}
		if detail := strings.TrimSpace(result.detail); detail != "" {
			details = append(details, detail)
		}
	}
	aggregateScheme := notificationSchemeFromSet(schemes)
	aggregateDetail := strings.Join(dedupe(details), "; ")
	if attemptedCount > 0 && sentCount == attemptedCount {
		return notificationOutcome{Status: "sent", Scheme: aggregateScheme}
	}
	if sentCount > 0 {
		if sentCount+unconfirmedCount == attemptedCount {
			return notificationOutcome{
				Status: "unconfirmed",
				Scheme: aggregateScheme,
				Detail: aggregateDetail,
			}
		}
		return notificationOutcome{Status: "partial_failed", Scheme: aggregateScheme, Detail: aggregateDetail, Err: errors.Join(failures...)}
	}
	if len(failures) > 0 {
		return notificationOutcome{Status: "failed", Scheme: aggregateScheme, Detail: aggregateDetail, Err: errors.Join(failures...)}
	}
	return notificationOutcome{
		Status: notificationStatusFromSet(statuses),
		Scheme: aggregateScheme,
		Detail: aggregateDetail,
	}
}

func notificationStatusFromSet(statuses map[string]bool) string {
	if len(statuses) == 0 {
		return "skipped_sender"
	}
	if len(statuses) > 1 {
		return "unavailable"
	}
	for status := range statuses {
		return status
	}
	return "unavailable"
}

func notificationSchemeForAddresses(addresses []string) string {
	schemes := map[string]bool{}
	for _, address := range addresses {
		parsed, err := parseAddress(address)
		if err != nil || parsed.Scheme == "" {
			continue
		}
		schemes[parsed.Scheme] = true
	}
	return notificationSchemeFromSet(schemes)
}

func notificationSchemeFromSet(schemes map[string]bool) string {
	if len(schemes) == 0 {
		return ""
	}
	if len(schemes) > 1 {
		return "mixed"
	}
	for scheme := range schemes {
		return scheme
	}
	return ""
}

func (m *notificationManager) notifyDirectWakeScope(ctx context.Context, scope wakeScope, input waypostSendInput) notificationOutcome {
	outcome := notificationOutcome{Status: "unsupported"}
	for _, target := range scope.WakeTargets {
		manager := notificationManagerForWakeChannel(target.Channel)
		if manager == "" {
			continue
		}
		if wakeNotifyDisabled(input.DisableNotifyMessage) {
			return notificationOutcome{
				Status: "skipped_disabled",
				Scheme: manager,
			}
		}
		route := notificationRoute{
			Manager: manager,
			Target:  target.Target,
		}
		outcome = m.notifyRouteWithRetry(ctx, notificationEvent{
			Kind:                 notificationDelivery,
			Route:                route,
			Subject:              input.Subject,
			Body:                 input.Body,
			DisableNotifyMessage: input.DisableNotifyMessage,
		})
		if notificationOutcomeAttempted(outcome) {
			return outcome
		}
	}
	return outcome
}

func (m *notificationManager) notifyRoute(ctx context.Context, event notificationEvent) notificationOutcome {
	notifier, ok := m.notifiers[event.Route.Manager]
	if !ok {
		return notificationOutcome{
			Status: "unsupported",
			Scheme: event.Route.Manager,
		}
	}
	outcome := notifier.Notify(ctx, event)
	if outcome.Scheme == "" {
		outcome.Scheme = notifier.Name()
	}
	return outcome
}

var notificationRetryDelays = [...]time.Duration{
	500 * time.Millisecond,
	time.Second,
	2 * time.Second,
}

const (
	groupNotifyFanoutTimeout     = 15 * time.Second
	groupNotifyFanoutConcurrency = 4
	defaultAgentDeckQueueBudget  = 30 * time.Minute
)

func (m *notificationManager) notifyRouteWithRetry(ctx context.Context, event notificationEvent) notificationOutcome {
	probe := m.probeRouteForSendWithRetry(ctx, event.Route)
	if probe.Wakeable {
		if !wakeNotifyDisabled(event.DisableNotifyMessage) && probe.PendingNudge {
			return notificationOutcome{
				Status: "unconfirmed",
				Scheme: probe.Scheme,
				Detail: "nudge already pending in the durable send queue; it is typed when the target is idle",
				Queued: true,
			}
		}
		// Notify is a non-idempotent side effect. Once it has been attempted,
		// its error is ambiguous: the target may already have received the
		// message. Only retry failures that happen before Notify is called.
		return m.notifyRoute(ctx, event)
	}
	detail := ""
	if probe.Status == "target_not_found" && event.Route.Manager == "agent-deck" {
		// Probe retries are useful for newly-created sessions. Only perform the
		// potentially expensive candidate lookup once, after retries settle.
		detail = m.sessions.agentDeckSessionNotFoundDetail(ctx, event.Route.Target)
	}
	return notificationOutcome{
		Status: probe.Status,
		Scheme: probe.Scheme,
		Detail: detail,
		Err:    probe.Err,
	}
}

func (m *notificationManager) probeRouteWithRetry(ctx context.Context, route notificationRoute) notificationProbe {
	return m.probeRouteWithRetryWhen(ctx, route, notificationProbeRetryable)
}

func (m *notificationManager) probeRouteForSendWithRetry(ctx context.Context, route notificationRoute) notificationProbe {
	return m.probeRouteWithRetryWhen(ctx, route, notificationSendProbeRetryable)
}

func (m *notificationManager) probeRouteWithRetryWhen(ctx context.Context, route notificationRoute, retryable func(notificationProbe) bool) notificationProbe {
	for attempt := 0; ; attempt++ {
		probe := m.probeRoute(ctx, route)
		if probe.Wakeable || !retryable(probe) || attempt == len(notificationRetryDelays) {
			return probe
		}
		delay := notificationRetryDelays[attempt]
		wait := m.retryWait
		if wait == nil {
			wait = waitForNotificationRetry
		}
		if err := wait(ctx, delay); err != nil {
			return probe
		}
	}
}

func notificationSendProbeRetryable(probe notificationProbe) bool {
	if notificationProbeRetryable(probe) {
		return true
	}
	if probe.Scheme != "agent-deck" {
		return false
	}
	switch probe.Status {
	case "target_not_found", "target_queued", "target_not_ready":
		// Fresh Agent Deck sessions can spend a short time absent from the
		// session list, queued, or reporting an unready status before they
		// become wakeable. Neither outcome has attempted the non-idempotent
		// nudge yet, so bounded send-time probe retries are safe.
		return true
	default:
		return false
	}
}

func notificationProbeRetryable(probe notificationProbe) bool {
	switch probe.Status {
	case "target_stopped", "target_error":
		return true
	case "failed":
		return !errors.Is(probe.Err, context.Canceled) && !errors.Is(probe.Err, context.DeadlineExceeded)
	default:
		return false
	}
}

func waitForNotificationRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (m *notificationManager) probeRoute(ctx context.Context, route notificationRoute) notificationProbe {
	notifier, ok := m.notifiers[route.Manager]
	if !ok {
		return notificationProbe{
			Status: "unsupported",
			Scheme: route.Manager,
		}
	}
	probe := notifier.Probe(ctx, route)
	if probe.Scheme == "" {
		probe.Scheme = notifier.Name()
	}
	return probe
}

func (n agentDeckNotifier) Name() string {
	return "agent-deck"
}

func (n agentDeckNotifier) Probe(ctx context.Context, route notificationRoute) notificationProbe {
	probe, err := n.sessions.probeSessionShowBestEffort(ctx, route.Target)
	if err != nil {
		return notificationProbe{
			Status: "failed",
			Scheme: n.Name(),
			Err:    err,
		}
	}
	switch probe.Status {
	case sessionShowProbeUnknown:
		return notificationProbe{
			Status: "failed",
			Scheme: n.Name(),
			Err:    errors.New("agent-deck session lookup returned an unknown result"),
		}
	case sessionShowProbeNotFound:
		return notificationProbe{
			Status: "target_not_found",
			Scheme: n.Name(),
		}
	}
	targetSession := probe.Data
	if targetSession == nil {
		return notificationProbe{
			Status: "failed",
			Scheme: n.Name(),
			Err:    errors.New("agent-deck session lookup returned no session data"),
		}
	}

	status := strings.ToLower(strings.TrimSpace(targetSession.Status))
	switch status {
	case "queued":
		return notificationProbe{
			Status: "target_queued",
			Scheme: n.Name(),
		}
	case "stopped", "error":
		return notificationProbe{
			Status: "target_" + status,
			Scheme: n.Name(),
			Err:    errors.New("agent-deck target session is " + status),
		}
	}
	if activeSessionStatuses[status] {
		entries, queueKnown := parseSessionQueue(targetSession.Queue)
		now := time.Now().UTC()
		if n.now != nil {
			now = n.now().UTC()
		}
		return notificationProbe{
			Status:       "wakeable",
			Scheme:       n.Name(),
			Wakeable:     true,
			QueueKnown:   queueKnown,
			PendingNudge: queueKnown && hasPendingWaypostNudge(entries, now),
		}
	}
	return notificationProbe{
		Status: "target_not_ready",
		Scheme: n.Name(),
	}
}

func (n agentDeckNotifier) Notify(ctx context.Context, event notificationEvent) notificationOutcome {
	if event.Kind != notificationDelivery && event.Kind != notificationFallbackWake {
		return notificationOutcome{
			Status: "unsupported",
			Scheme: n.Name(),
		}
	}
	if wakeNotifyDisabled(event.DisableNotifyMessage) {
		return notificationOutcome{
			Status: "skipped_disabled",
			Scheme: n.Name(),
		}
	}
	notifyMessage := resolveWakeNotifyMessage(event.DisableNotifyMessage, defaultNotifyMessage)
	if notifyMessage == "" {
		return notificationOutcome{
			Status: "skipped_disabled",
			Scheme: n.Name(),
		}
	}

	// --queue parks the nudge in the durable send queue; a background worker
	// types it when the target idles. A busy Codex session legitimately runs
	// turns for minutes, so a bounded --defer-if-busy here expired and the
	// nudge was dropped — the durable queue is the wake semantic that fits.
	return n.runNudge(ctx, []string{
		"agent-deck", "session", "send",
		"--json",
		"--queue",
		event.Route.Target, notifyMessage,
	})
}

type agentDeckNudgeResult struct {
	Success   *bool  `json:"success"`
	Delivery  string `json:"delivery"`
	Submitted *bool  `json:"submitted"`
	SendID    string `json:"send_id"`
	State     string `json:"state"`
	Deadline  string `json:"deadline"`
	Reason    string `json:"reason"`
	Error     string `json:"error"`
	Code      string `json:"code"`
}

func (n agentDeckNotifier) runNudge(ctx context.Context, args []string) notificationOutcome {
	runCtx, cancel := context.WithTimeout(ctx, syncCmdTimeout)
	defer cancel()
	if err := runCtx.Err(); err != nil {
		return notificationOutcome{
			Status: "failed",
			Scheme: n.Name(),
			Err:    fmt.Errorf("agent-deck nudge was not attempted: %w", err),
		}
	}

	result, runErr := n.runner.Run(runCtx, args, "")
	payload, structured := parseAgentDeckNudgeResult(result.Stdout)
	if structured {
		return classifyAgentDeckNudgeResult(n.Name(), result, payload)
	}

	if errors.Is(runErr, context.DeadlineExceeded) || errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return notificationOutcome{
			Status: "unconfirmed",
			Scheme: n.Name(),
			Detail: "nudge command timed out after delivery may already have been attempted",
		}
	}
	if errors.Is(runErr, context.Canceled) || errors.Is(runCtx.Err(), context.Canceled) {
		return notificationOutcome{
			Status: "unconfirmed",
			Scheme: n.Name(),
			Detail: "nudge command was canceled after delivery may already have been attempted",
		}
	}
	if runErr != nil {
		return notificationOutcome{
			Status: "failed",
			Scheme: n.Name(),
			Err:    fmt.Errorf("agent-deck nudge could not start or complete: %w", runErr),
		}
	}
	if result.ExitCode == 0 {
		return notificationOutcome{Status: "sent", Scheme: n.Name()}
	}
	return notificationOutcome{
		Status: "failed",
		Scheme: n.Name(),
		Err:    agentDeckNudgeError(result, payload),
	}
}

func parseAgentDeckNudgeResult(stdout string) (agentDeckNudgeResult, bool) {
	trimmed := strings.TrimSpace(stdout)
	var payload agentDeckNudgeResult
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return agentDeckNudgeResult{}, false
	}
	structured := payload.Success != nil ||
		payload.Submitted != nil ||
		strings.TrimSpace(payload.Delivery) != "" ||
		strings.TrimSpace(payload.SendID) != "" ||
		strings.TrimSpace(payload.State) != "" ||
		strings.TrimSpace(payload.Reason) != "" ||
		strings.TrimSpace(payload.Error) != "" ||
		strings.TrimSpace(payload.Code) != ""
	return payload, structured
}

func classifyAgentDeckNudgeResult(scheme string, result RunResult, payload agentDeckNudgeResult) notificationOutcome {
	delivery := strings.ToLower(strings.TrimSpace(payload.Delivery))
	if payload.Submitted != nil && *payload.Submitted || delivery == "submitted" {
		return notificationOutcome{Status: "sent", Scheme: scheme}
	}

	switch delivery {
	case "typed":
		return notificationOutcome{
			Status: "unconfirmed",
			Scheme: scheme,
			Detail: "nudge reached the target pane but turn submission was not confirmed",
		}
	case "unverified":
		return notificationOutcome{
			Status: "unconfirmed",
			Scheme: scheme,
			Detail: "nudge was sent but agent-deck could not verify whether the target accepted it",
		}
	case "queued":
		if payload.Success != nil && !*payload.Success {
			return notificationOutcome{
				Status: "failed",
				Scheme: scheme,
				Err:    agentDeckNudgeError(result, payload),
			}
		}
		return queuedAgentDeckOutcome(scheme, payload.Deadline)
	case "no_evidence":
		return notificationOutcome{
			Status: "unconfirmed",
			Scheme: scheme,
			Detail: "nudge was attempted but no delivery evidence was observed",
		}
	case "typed_not_submitted", "line_too_long", "send_failed", "pane_gone":
		return notificationOutcome{
			Status: "failed",
			Scheme: scheme,
			Err:    agentDeckNudgeError(result, payload),
		}
	case "":
		state := strings.ToLower(strings.TrimSpace(payload.State))
		if state == "failed" {
			return notificationOutcome{Status: "failed", Scheme: scheme, Err: agentDeckNudgeError(result, payload)}
		}
		if payload.Success != nil && !*payload.Success {
			return notificationOutcome{Status: "failed", Scheme: scheme, Err: agentDeckNudgeError(result, payload)}
		}
		if state == "queued" {
			return queuedAgentDeckOutcome(scheme, payload.Deadline)
		}
		if payload.Success != nil && *payload.Success && result.ExitCode == 0 {
			return notificationOutcome{Status: "sent", Scheme: scheme}
		}
		if result.ExitCode == 0 {
			return notificationOutcome{
				Status: "unconfirmed",
				Scheme: scheme,
				Detail: "agent-deck returned structured output without a delivery verdict",
			}
		}
		return notificationOutcome{Status: "failed", Scheme: scheme, Err: agentDeckNudgeError(result, payload)}
	default:
		if strings.EqualFold(strings.TrimSpace(payload.State), "failed") {
			return notificationOutcome{
				Status: "failed",
				Scheme: scheme,
				Err:    agentDeckNudgeError(result, payload),
			}
		}
		return notificationOutcome{
			Status: "unconfirmed",
			Scheme: scheme,
			Detail: fmt.Sprintf("agent-deck returned an unknown delivery verdict %q", delivery),
		}
	}
}

func queuedAgentDeckOutcome(scheme, deadline string) notificationOutcome {
	return notificationOutcome{
		Status:         "unconfirmed",
		Scheme:         scheme,
		Detail:         "nudge accepted into the durable send queue; it is typed when the target is idle",
		Queued:         true,
		QueuedDeadline: strings.TrimSpace(deadline),
	}
}

func agentDeckNudgeError(result RunResult, payload agentDeckNudgeResult) error {
	detail := strings.TrimSpace(payload.Error)
	if detail == "" {
		detail = strings.TrimSpace(result.Stderr)
	}
	if detail == "" {
		detail = strings.TrimSpace(payload.Reason)
	}
	if detail == "" && strings.TrimSpace(payload.Delivery) != "" {
		detail = "agent-deck delivery result: " + strings.TrimSpace(payload.Delivery)
	}
	if detail == "" && strings.TrimSpace(payload.Code) != "" {
		detail = "agent-deck error code: " + strings.TrimSpace(payload.Code)
	}
	if detail == "" {
		detail = fmt.Sprintf("agent-deck exited with code %d", result.ExitCode)
	}
	return errors.New(detail)
}

func (n thurboxNotifier) Name() string {
	return "thurbox"
}

func (n thurboxNotifier) Probe(_ context.Context, route notificationRoute) notificationProbe {
	if strings.TrimSpace(route.Target) == "" {
		return notificationProbe{
			Status: "target_not_found",
			Scheme: n.Name(),
		}
	}
	// Thurbox's native mailbox is intentionally not part of Waypost workflow.
	// A session-send attempt is the best available targeted wake check and is
	// performed only after the durable Waypost send has completed.
	return notificationProbe{
		Status:   "wakeable",
		Scheme:   n.Name(),
		Wakeable: true,
	}
}

func (n thurboxNotifier) Notify(ctx context.Context, event notificationEvent) notificationOutcome {
	if event.Kind != notificationDelivery && event.Kind != notificationFallbackWake {
		return notificationOutcome{
			Status: "unsupported",
			Scheme: n.Name(),
		}
	}
	if wakeNotifyDisabled(event.DisableNotifyMessage) {
		return notificationOutcome{
			Status: "skipped_disabled",
			Scheme: n.Name(),
		}
	}
	notifyMessage := resolveWakeNotifyMessage(event.DisableNotifyMessage, defaultNotifyMessage)
	if notifyMessage == "" {
		return notificationOutcome{
			Status: "skipped_disabled",
			Scheme: n.Name(),
		}
	}
	if _, err := runCommand(ctx, n.runner, []string{
		"thurbox-cli", "session", "send", event.Route.Target, notifyMessage,
	}, runOptions{timeout: syncCmdTimeout}); err != nil {
		return notificationOutcome{
			Status: "failed",
			Scheme: n.Name(),
			Err:    err,
		}
	}
	return notificationOutcome{
		Status: "sent",
		Scheme: n.Name(),
	}
}

func notificationOutcomeDelivered(outcome notificationOutcome) bool {
	return strings.TrimSpace(outcome.Status) == "sent"
}

func notificationOutcomeAttempted(outcome notificationOutcome) bool {
	switch strings.TrimSpace(outcome.Status) {
	case "sent", "unconfirmed":
		return true
	default:
		return false
	}
}

func wakeNotifyDisabled(disableNotifyMessage *bool) bool {
	return disableNotifyMessage != nil && *disableNotifyMessage
}

func resolveWakeNotifyMessage(disableNotifyMessage *bool, defaultMessage string) string {
	if wakeNotifyDisabled(disableNotifyMessage) {
		return ""
	}
	return defaultMessage
}
