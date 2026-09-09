package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/ruiheng/waypost/internal/waypost"
)

type waypostBindInput struct {
	Addresses      []string `json:"addresses"`
	DefaultSender  string   `json:"default_sender,omitempty"`
	DefaultWorkdir string   `json:"default_workdir,omitempty"`
}

type waypostStatusInput struct {
	IncludeDiagnostics  bool   `json:"include_diagnostics,omitempty"`
	IncludeCLIContext   bool   `json:"include_cli_context,omitempty"`
	IncludeActiveLeases bool   `json:"include_active_leases,omitempty"`
	Limit               *int   `json:"limit,omitempty"`
	Cursor              string `json:"cursor,omitempty"`
}

type waypostDebugInput struct{}

type waypostSendTarget struct {
	Addresses []string
	Batch     bool
}

func (target *waypostSendTarget) UnmarshalJSON(data []byte) error {
	var address string
	if err := json.Unmarshal(data, &address); err == nil {
		target.Addresses = []string{address}
		target.Batch = false
		return nil
	}

	var addresses []string
	if err := json.Unmarshal(data, &addresses); err != nil {
		return errors.New("to must be a string or an array of strings")
	}
	if len(addresses) == 0 {
		return errors.New("to must contain at least one recipient")
	}
	target.Addresses = addresses
	target.Batch = true
	return nil
}

type waypostSendInput struct {
	To                   waypostSendTarget `json:"to"`
	FromAddress          string            `json:"from_address,omitempty"`
	AsPerson             string            `json:"as_person,omitempty"`
	Subject              string            `json:"subject"`
	Body                 string            `json:"body,omitempty"`
	BodyFile             string            `json:"body_file,omitempty"`
	ContentType          string            `json:"content_type,omitempty"`
	SchemaVersion        string            `json:"schema_version,omitempty"`
	DisableNotifyMessage *bool             `json:"disable_notify_message,omitempty"`
	Group                bool              `json:"group,omitempty"`
	Diagnostics          bool              `json:"diagnostics,omitempty"`
	ToAddress            string            `json:"-"`
	ToAddresses          []string          `json:"-"`
	forwardedMessageID   string
	forwardedFromAddress string
}

func waypostSendInputSchema() *jsonschema.Schema {
	schema, err := jsonschema.For[waypostSendInput](nil)
	if err != nil {
		panic(fmt.Errorf("build waypost_send input schema: %w", err))
	}
	schema.Properties["to"] = &jsonschema.Schema{
		Types:       []string{"string", "array"},
		Items:       &jsonschema.Schema{Type: "string"},
		MinItems:    jsonschema.Ptr(1),
		MaxItems:    jsonschema.Ptr(waypost.MaxSendRecipients),
		Description: "One recipient address, or an array of 1-10 recipient addresses for a batch send.",
	}
	fromAddress, ok := schema.Properties["from_address"]
	if !ok {
		panic("build waypost_send input schema: missing from_address")
	}
	fromAddress.Description = "Optional sender address. When supplied, it must be one of this MCP server's currently bound personal addresses."
	diagnostics, ok := schema.Properties["diagnostics"]
	if !ok {
		panic("build waypost_send input schema: missing diagnostics")
	}
	diagnostics.Description = "Unnecessary for normal send."
	body, ok := schema.Properties["body"]
	if !ok {
		panic("build waypost_send input schema: missing body")
	}
	body.Description = "Inline message body. Supply exactly one of body or body_file."
	bodyFile, ok := schema.Properties["body_file"]
	if !ok {
		panic("build waypost_send input schema: missing body_file")
	}
	bodyFile.Description = "Message body file inside the bound default_workdir. Relative paths resolve from default_workdir. Supply exactly one of body or body_file. Files outside that workspace, non-regular files, and files larger than 10 MiB are rejected."
	return schema
}

type waypostForwardInput struct {
	MessageID            string `json:"message_id,omitempty"`
	DeliveryID           string `json:"delivery_id,omitempty"`
	ToAddress            string `json:"to_address"`
	FromAddress          string `json:"from_address,omitempty"`
	Subject              string `json:"subject,omitempty"`
	Group                bool   `json:"group,omitempty"`
	DisableNotifyMessage *bool  `json:"disable_notify_message,omitempty"`
}

type waypostWaitInput struct {
	Addresses []string `json:"addresses,omitempty"`
	AsPerson  string   `json:"as_person,omitempty"`
	Timeout   string   `json:"timeout,omitempty"`
}

type waypostRecvInput struct {
	Addresses         []string `json:"addresses,omitempty"`
	AsPerson          string   `json:"as_person,omitempty"`
	KnownDeliveryIDs  []string `json:"known_delivery_ids,omitempty"`
	ActiveLeaseCursor string   `json:"active_lease_cursor,omitempty"`
	Diagnostics       bool     `json:"diagnostics,omitempty"`
}

func waypostRecvInputSchema() *jsonschema.Schema {
	schema, err := jsonschema.For[waypostRecvInput](nil)
	if err != nil {
		panic(fmt.Errorf("build waypost_recv input schema: %w", err))
	}
	diagnostics, ok := schema.Properties["diagnostics"]
	if !ok {
		panic("build waypost_recv input schema: missing diagnostics")
	}
	diagnostics.Description = "Unnecessary for normal receive or sender verification."
	return schema
}

type waypostClaimHistoryInput struct {
	DeliveryID        string `json:"delivery_id,omitempty"`
	IncludeTerminal   bool   `json:"include_terminal,omitempty"`
	RecoverLeaseToken bool   `json:"recover_lease_token,omitempty"`
	Limit             *int   `json:"limit,omitempty"`
	Cursor            string `json:"cursor,omitempty"`
	Diagnostics       bool   `json:"diagnostics,omitempty"`
}

func waypostClaimHistoryInputSchema() *jsonschema.Schema {
	schema, err := jsonschema.For[waypostClaimHistoryInput](nil)
	if err != nil {
		panic(fmt.Errorf("build waypost_claim_history input schema: %w", err))
	}
	diagnostics, ok := schema.Properties["diagnostics"]
	if !ok {
		panic("build waypost_claim_history input schema: missing diagnostics")
	}
	diagnostics.Description = "Unnecessary for normal claim listing or lease-token recovery."
	return schema
}

type waypostListInput struct {
	Address  string `json:"address,omitempty"`
	AsPerson string `json:"as_person,omitempty"`
	State    string `json:"state,omitempty"`
}

type waypostReadInput struct {
	MessageIDs  []string `json:"message_ids,omitempty"`
	DeliveryIDs []string `json:"delivery_ids,omitempty"`
	Latest      bool     `json:"latest,omitempty"`
	Addresses   []string `json:"addresses,omitempty"`
	State       string   `json:"state,omitempty"`
	Limit       *int     `json:"limit,omitempty"`
}

type waypostGroupInput struct {
	GroupAddress string `json:"group_address"`
}

type waypostGroupMemberInput struct {
	GroupAddress string `json:"group_address"`
	Person       string `json:"person"`
}

type waypostGroupSubscriberInput struct {
	GroupAddress  string `json:"group_address"`
	NotifyAddress string `json:"notify_address"`
	Person        string `json:"person"`
}

type waypostGroupSubscriberRemoveInput struct {
	GroupAddress  string `json:"group_address"`
	NotifyAddress string `json:"notify_address"`
}

type waypostAddressInspectInput struct {
	Address string `json:"address"`
}

type waypostAckInput struct {
	DeliveryID string `json:"delivery_id"`
	LeaseToken string `json:"lease_token"`
}

type waypostDeferInput struct {
	DeliveryID string `json:"delivery_id"`
	LeaseToken string `json:"lease_token"`
	Until      string `json:"until"`
}

type waypostUndeferInput struct {
	DeliveryID string `json:"delivery_id"`
}

type waypostFailInput struct {
	DeliveryID string `json:"delivery_id"`
	LeaseToken string `json:"lease_token"`
	Reason     string `json:"reason"`
}

type readLatestResult struct {
	Items   []waypost.ReadDelivery
	HasMore bool
}

func (s *Service) registerWaypostTools(server *mcp.Server) {
	addToolRequiringWaypostStatus(server, s, &mcp.Tool{
		Name:        "waypost_bind",
		Description: "Bind Waypost addresses to this MCP server.",
	}, s.waypostBind)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "waypost_status",
		Description: "Show compact operational state. Set include_cli_context, include_diagnostics, or include_active_leases only when that detail is needed.",
	}, s.waypostStatus)
	if s.includeDebugTool {
		mcp.AddTool(server, &mcp.Tool{
			Name:        "waypost_debug",
			Description: "Show read-only MCP and allowlisted session-environment diagnostics without binding or changing state.",
		}, s.waypostDebug)
	}
	addToolRequiringWaypostStatus(server, s, &mcp.Tool{
		Name:        "waypost_send",
		Description: "Send a Waypost message to one recipient or a batch of up to 10. Provide exactly one of body or body_file; from_address must be bound, and body_file must be under default_workdir.",
		InputSchema: waypostSendInputSchema(),
	}, s.waypostSend)
	addToolRequiringWaypostStatus(server, s, &mcp.Tool{
		Name:        "waypost_recv",
		Description: "Claim one available delivery immediately; never blocks. Defaults to all bound addresses, and explicit personal addresses must be bound. After no_message, wait 15 seconds before retrying.",
		InputSchema: waypostRecvInputSchema(),
	}, s.waypostRecv)
	addToolRequiringWaypostStatus(server, s, &mcp.Tool{
		Name:        "waypost_claim_history",
		Description: "List deliveries claimed by this MCP process. Use delivery_id with recover_lease_token to recover a lease token; diagnostics adds claim details.",
		InputSchema: waypostClaimHistoryInputSchema(),
	}, s.waypostClaimHistory)
	addToolRequiringWaypostStatus(server, s, &mcp.Tool{
		Name:        "waypost_ack",
		Description: "Acknowledge a claimed delivery; it remains readable through the reported CLI.",
	}, s.waypostAck)
	addToolRequiringWaypostStatus(server, s, &mcp.Tool{
		Name:        "waypost_release",
		Description: "Release a claimed waypost delivery back to the queue.",
	}, s.waypostRelease)
	addToolRequiringWaypostStatus(server, s, &mcp.Tool{
		Name:        "waypost_defer",
		Description: "Defer a claimed delivery until an RFC3339 time.",
	}, s.waypostDefer)
}

func (s *Service) waypostBind(ctx context.Context, _ *mcp.CallToolRequest, input waypostBindInput) (*mcp.CallToolResult, map[string]any, error) {
	bound, err := s.sessions.bind(ctx, input)
	if err != nil {
		return nil, nil, err
	}
	out := boundStateMap(bound)
	out["status"] = "bound"
	return s.waypostMutationToolResult(ctx, out)
}

func (s *Service) waypostStatus(ctx context.Context, _ *mcp.CallToolRequest, input waypostStatusInput) (*mcp.CallToolResult, map[string]any, error) {
	pageSize := 0
	after := ""
	if input.IncludeActiveLeases {
		var err error
		pageSize, after, err = normalizeMemoryPage(input.Limit, input.Cursor, "active-leases", "active")
		if err != nil {
			return nil, nil, err
		}
	} else if input.Limit != nil || input.Cursor != "" {
		return nil, nil, errors.New("limit and cursor require include_active_leases=true")
	}
	bound, err := s.sessions.boundState(ctx)
	if err != nil {
		return nil, nil, err
	}
	var executable, stateDir string
	if input.IncludeCLIContext {
		executable, stateDir, err = s.executableAndStateDir()
		if err != nil {
			return nil, nil, err
		}
	}
	if s.activeLeases.hasTrackedLeases() {
		if err := s.reconcileTrackedLeases(ctx); err != nil {
			return nil, nil, err
		}
	}
	out := compactWaypostStatusState(bound)
	if input.IncludeDiagnostics {
		out = boundStateMap(bound)
		out["server_version"] = serverVersion
		out["default_sender"] = orUnset(bound.DefaultSender)
		out["default_workdir"] = orUnset(bound.DefaultWorkdir)
	}
	out["status"] = "ready"
	if input.IncludeCLIContext {
		out["executable"] = executable
		out["resolved_state_dir"] = stateDir
	}
	leases := s.activeLeases.snapshot()
	if len(leases) > 0 || input.IncludeActiveLeases {
		out["active_lease_count"] = len(leases)
	}
	if input.IncludeActiveLeases {
		activeLeases, nextCursor, err := activeLeaseStatusPage(leases, pageSize, after, "active-leases", "active")
		if err != nil {
			return nil, nil, err
		}
		out["active_leases"] = activeLeases
		if nextCursor != "" {
			out["next_cursor"] = nextCursor
		}
	}
	result, structured, err := s.waypostToolResult(ctx, out)
	if err != nil {
		return nil, nil, err
	}
	s.markWaypostStatusCalled()
	return result, structured, nil
}

func compactWaypostStatusState(bound boundState) map[string]any {
	out := map[string]any{}
	if len(bound.BoundAddresses) > 0 {
		out["bound_addresses"] = bound.BoundAddresses
	}
	if defaultSender := strings.TrimSpace(bound.DefaultSender); defaultSender != "" {
		out["default_sender"] = defaultSender
	}
	if len(bound.Warnings) > 0 {
		out["warnings"] = bound.Warnings
	}
	return out
}

func activeLeaseStatusPage(leases []activeLease, limit int, after, kind, scope string) ([]map[string]any, string, error) {
	sort.Slice(leases, func(i, j int) bool {
		return leases[i].DeliveryID < leases[j].DeliveryID
	})

	items := make([]map[string]any, 0, min(len(leases), limit))
	more := false
	for _, lease := range leases {
		if lease.DeliveryID <= after {
			continue
		}
		if len(items) == limit {
			more = true
			break
		}
		items = append(items, map[string]any{
			"delivery_id":       lease.DeliveryID,
			"recipient_address": lease.RecipientAddress,
			"lease_token":       lease.LeaseToken,
			"last_renewed_at":   nilIfEmpty(lease.LastRenewedAt),
		})
	}
	if !more || len(items) == 0 {
		return items, "", nil
	}
	nextCursor, err := encodeMemoryPageCursor(kind, scope, items[len(items)-1]["delivery_id"].(string))
	return items, nextCursor, err
}

func (s *Service) sendWaypostMessage(ctx context.Context, input waypostSendInput) (map[string]any, error) {
	return withWaypostService(ctx, s.waypostServices, func(service waypostSender) (map[string]any, error) {
		return s.sendWaypostMessageWithService(ctx, input, service)
	})
}

func (s *Service) sendWaypostMessageWithService(ctx context.Context, input waypostSendInput, service waypostSender) (map[string]any, error) {
	toAddress, err := waypost.NormalizeAddress(input.ToAddress)
	if err != nil {
		if strings.TrimSpace(input.ToAddress) == "" {
			return nil, errors.New("recipient address is required")
		}
		return nil, err
	}
	input.ToAddress = toAddress

	fromAddress, err := s.sessions.senderAddress(ctx, input.FromAddress)
	if err != nil {
		return nil, err
	}
	input.FromAddress = fromAddress

	sendResult, err := service.Send(ctx, waypost.SendParams{
		ToAddress:            input.ToAddress,
		FromAddress:          fromAddress,
		AsPerson:             strings.TrimSpace(input.AsPerson),
		Subject:              input.Subject,
		ContentType:          strings.TrimSpace(input.ContentType),
		SchemaVersion:        strings.TrimSpace(input.SchemaVersion),
		ForwardedMessageID:   strings.TrimSpace(input.forwardedMessageID),
		ForwardedFromAddress: strings.TrimSpace(input.forwardedFromAddress),
		Body:                 []byte(input.Body),
		Group:                input.Group,
	})
	if err != nil {
		return nil, err
	}

	notify := s.notifyWaypostSend(ctx, input, sendResult, service)
	return waypostSendResultMap(input, fromAddress, sendResult, notify), nil
}

func waypostSendResultMap(input waypostSendInput, fromAddress string, sendResult waypost.SendResult, notify notificationOutcome) map[string]any {
	out := map[string]any{
		"status":        "sent",
		"from_address":  fromAddress,
		"to_address":    input.ToAddress,
		"notify_status": notify.Status,
	}
	if notify.Scheme != "" {
		out["notify_scheme"] = notify.Scheme
	}
	if notify.Detail != "" {
		out["notify_detail"] = notify.Detail
	}
	if notify.Err != nil {
		out["notify_error"] = notify.Err.Error()
	}
	if sendResult.Mode == waypost.SendModeGroup {
		out["mode"] = sendResult.Mode
		out["message_id"] = sendResult.MessageID
		out["group_id"] = sendResult.GroupID
		out["group_address"] = sendResult.GroupAddress
		out["eligible_count"] = sendResult.EligibleCount
		out["message_created_at"] = sendResult.MessageCreatedAt
	} else {
		out["delivery_id"] = sendResult.DeliveryID
	}
	return out
}

func prepareWaypostSendTarget(req *mcp.CallToolRequest, input *waypostSendInput) (bool, error) {
	if req == nil || len(req.Params.Arguments) == 0 {
		return false, errors.New("waypost_send requires to")
	}

	var rawArgs map[string]json.RawMessage
	if err := json.Unmarshal(req.Params.Arguments, &rawArgs); err != nil {
		return false, fmt.Errorf("invalid tool arguments: %w", err)
	}
	if _, ok := rawArgs["to"]; !ok {
		return false, errors.New("waypost_send requires to")
	}
	if input.To.Batch {
		input.ToAddresses = input.To.Addresses
		return true, nil
	}
	if len(input.To.Addresses) != 1 {
		return false, errors.New("waypost_send to must be a recipient address or an array of recipient addresses")
	}
	input.ToAddress = input.To.Addresses[0]
	return false, nil
}

func (s *Service) prepareWaypostSendBody(req *mcp.CallToolRequest, input *waypostSendInput) error {
	if req == nil || len(req.Params.Arguments) == 0 {
		return errors.New("waypost_send requires body or body_file")
	}

	var rawArgs map[string]json.RawMessage
	if err := json.Unmarshal(req.Params.Arguments, &rawArgs); err != nil {
		return fmt.Errorf("invalid tool arguments: %w", err)
	}
	_, hasBody := rawArgs["body"]
	_, hasBodyFile := rawArgs["body_file"]
	if hasBody && hasBodyFile {
		return errors.New("waypost_send accepts exactly one of body or body_file")
	}
	if !hasBody && !hasBodyFile {
		return errors.New("waypost_send requires body or body_file")
	}
	if hasBody {
		if input.Body == "" {
			return waypost.ErrEmptyBody
		}
		return nil
	}

	bodyFile := input.BodyFile
	if strings.TrimSpace(bodyFile) == "" {
		return errors.New("waypost_send body_file must not be empty")
	}
	body, resolvedBodyFile, err := readWaypostSendBodyFile(bodyFile, s.sessions.snapshotState().DefaultWorkdir)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return waypost.ErrEmptyBody
	}
	input.Body = string(body)
	input.BodyFile = resolvedBodyFile
	return nil
}

func (s *Service) sendWaypostBatchMessage(ctx context.Context, input waypostSendInput) (map[string]any, error) {
	recipients, err := waypost.NormalizeSendRecipients(input.ToAddresses, input.Group)
	if err != nil {
		return nil, err
	}
	input.AsPerson = strings.TrimSpace(input.AsPerson)
	input.Subject = strings.TrimSpace(input.Subject)
	input.ContentType = strings.TrimSpace(input.ContentType)
	input.SchemaVersion = strings.TrimSpace(input.SchemaVersion)
	input.forwardedMessageID = strings.TrimSpace(input.forwardedMessageID)
	input.forwardedFromAddress = strings.TrimSpace(input.forwardedFromAddress)
	if input.AsPerson != "" && !input.Group {
		return nil, errors.New("as_person requires group send")
	}
	if input.Body == "" {
		return nil, waypost.ErrEmptyBody
	}

	fromAddress, err := s.sessions.senderAddress(ctx, input.FromAddress)
	if err != nil {
		return nil, err
	}
	if waypost.IsGroupAddress(fromAddress) {
		return nil, fmt.Errorf("sender address %q uses reserved group/ prefix", fromAddress)
	}
	input.FromAddress = fromAddress
	base := waypost.SendParams{
		FromAddress:          fromAddress,
		AsPerson:             input.AsPerson,
		Subject:              input.Subject,
		ContentType:          input.ContentType,
		SchemaVersion:        input.SchemaVersion,
		ForwardedMessageID:   input.forwardedMessageID,
		ForwardedFromAddress: input.forwardedFromAddress,
		Body:                 []byte(input.Body),
		Group:                input.Group,
	}

	return withWaypostService(ctx, s.waypostServices, func(service waypostSender) (map[string]any, error) {
		notify := func(ctx context.Context, request waypost.SendNotificationRequest) waypost.SendNotificationOutcome {
			notificationInput := input
			notificationInput.ToAddress = request.Params.ToAddress
			outcome := s.notifyWaypostSend(ctx, notificationInput, request.Result, service)
			return waypost.SendNotificationOutcome{
				Status: outcome.Status,
				Scheme: outcome.Scheme,
				Detail: outcome.Detail,
				Err:    outcome.Err,
			}
		}
		batch, err := waypost.ExecuteSendBatch(ctx, service, base, recipients, notify)
		if err != nil {
			return nil, err
		}
		return waypostSendBatchResult(input, batch), nil
	})
}

func waypostSendBatchResult(input waypostSendInput, batch waypost.SendBatchResult) map[string]any {
	results := make([]map[string]any, 0, len(batch.Items))
	for _, item := range batch.Items {
		if item.Err != nil {
			results = append(results, map[string]any{
				"status":        "failed",
				"from_address":  input.FromAddress,
				"to_address":    item.ToAddress,
				"subject":       input.Subject,
				"error":         item.Err.Error(),
				"notify_status": "not_attempted",
			})
			continue
		}

		successInput := input
		successInput.ToAddress = item.ToAddress
		notify := notificationOutcome{Status: "unknown"}
		if item.Notification != nil {
			notify = notificationOutcome{
				Status: item.Notification.Status,
				Scheme: item.Notification.Scheme,
				Detail: item.Notification.Detail,
				Err:    item.Notification.Err,
			}
		}
		result := compactWaypostSendResult(
			waypostSendResultMap(successInput, input.FromAddress, item.Result, notify),
			input.Diagnostics,
		)
		// Batch results retain their target, resolved sender, and subject even
		// in compact mode so partial failures can be retried safely.
		result["from_address"] = input.FromAddress
		result["to_address"] = item.ToAddress
		result["subject"] = input.Subject
		results = append(results, result)
	}

	return map[string]any{
		"status":          batch.Status(),
		"to_addresses":    batch.ToAddresses,
		"recipient_count": len(batch.ToAddresses),
		"sent_count":      batch.SentCount,
		"failed_count":    batch.FailedCount,
		"results":         results,
	}
}

func (s *Service) notifyWaypostSend(ctx context.Context, input waypostSendInput, sendResult waypost.SendResult, service any) notificationOutcome {
	notifyCtx := context.WithoutCancel(ctx)
	if sendResult.Mode != waypost.SendModeGroup {
		if s.sessions.isLocalAddress(notifyCtx, input.ToAddress) || wakeNotifyDisabled(input.DisableNotifyMessage) {
			return s.notifications.notifyWaypostSend(notifyCtx, input)
		}
		scope, _, err := directWakeScopeForAddress(input.ToAddress)
		if err != nil {
			return notificationOutcome{Status: "failed", Err: err}
		}
		hasSessionHostWakeTarget := scope != nil && len(scope.WakeTargets) > 0
		if hasSessionHostWakeTarget && strings.TrimSpace(sendResult.DeliveryID) != "" {
			if err := s.waitBeforeNotify(notifyCtx); err != nil {
				return notificationOutcome{Status: "failed", Scheme: "waypost", Err: err}
			}
			stillQueued, err := s.deliveryStillQueued(notifyCtx, service, sendResult.DeliveryID)
			if err != nil {
				return notificationOutcome{Status: "failed", Scheme: "waypost", Err: err}
			}
			if !stillQueued {
				return notificationOutcome{Status: "skipped_already_claimed", Scheme: "waypost"}
			}
		}
		return s.notifications.notifyWaypostSend(notifyCtx, input)
	}
	return s.notifications.notifyGroupSubscribers(notifyCtx, input, sendResult.GroupNotificationAddresses)
}

func (s *Service) waitBeforeNotify(ctx context.Context) error {
	if s.notifyDelay <= 0 {
		return nil
	}
	timer := time.NewTimer(s.notifyDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *Service) deliveryStillQueued(ctx context.Context, service any, deliveryID string) (bool, error) {
	reader, ok := service.(waypostDeliveryReader)
	if !ok {
		return false, fmt.Errorf("waypost service %T does not satisfy %T", service, reader)
	}
	deliveries, err := reader.ReadDeliveries(ctx, []string{deliveryID})
	if err != nil {
		return false, err
	}
	if len(deliveries) != 1 {
		return false, fmt.Errorf("read delivery %q: got %d deliveries, want 1", deliveryID, len(deliveries))
	}
	return strings.TrimSpace(deliveries[0].State) == "queued", nil
}

func (s *Service) waypostSend(ctx context.Context, req *mcp.CallToolRequest, input waypostSendInput) (*mcp.CallToolResult, map[string]any, error) {
	batch, err := prepareWaypostSendTarget(req, &input)
	if err != nil {
		return nil, nil, err
	}
	if err := s.prepareWaypostSendBody(req, &input); err != nil {
		return nil, nil, err
	}
	var out map[string]any
	if batch {
		out, err = s.sendWaypostBatchMessage(ctx, input)
	} else {
		out, err = s.sendWaypostMessage(ctx, input)
	}
	if err != nil {
		return nil, nil, err
	}
	if batch {
		return s.waypostMutationToolResult(ctx, out)
	}
	return s.waypostMutationToolResult(ctx, compactWaypostSendResult(out, input.Diagnostics))
}

func compactWaypostSendResult(out map[string]any, includeDetails bool) map[string]any {
	if includeDetails {
		return out
	}
	compact := map[string]any{
		"status":        out["status"],
		"notify_status": out["notify_status"],
	}
	for _, field := range []string{"delivery_id", "message_id", "eligible_count"} {
		if value := out[field]; value != nil && value != "" {
			compact[field] = value
		}
	}
	if notifyError := out["notify_error"]; notifyError != nil {
		compact["notify_error"] = notifyError
	}
	if notifyDetail := out["notify_detail"]; notifyDetail != nil {
		compact["notify_detail"] = notifyDetail
	}
	return compact
}

func (s *Service) waypostForward(ctx context.Context, _ *mcp.CallToolRequest, input waypostForwardInput) (*mcp.CallToolResult, map[string]any, error) {
	out, err := withWaypostService(ctx, s.waypostServices, func(service interface {
		waypost.ForwardSourceReader
		waypostSender
	}) (map[string]any, error) {
		prepared, err := waypost.PrepareForward(ctx, service, "waypost_forward", waypost.ForwardParams{
			MessageID:   input.MessageID,
			DeliveryID:  input.DeliveryID,
			ToAddress:   input.ToAddress,
			FromAddress: input.FromAddress,
			Subject:     input.Subject,
			Group:       input.Group,
		})
		if err != nil {
			return nil, err
		}

		sendInput := waypostSendInput{
			ToAddress:            prepared.SendParams.ToAddress,
			FromAddress:          prepared.SendParams.FromAddress,
			Subject:              prepared.SendParams.Subject,
			Body:                 string(prepared.SendParams.Body),
			ContentType:          prepared.SendParams.ContentType,
			SchemaVersion:        prepared.SendParams.SchemaVersion,
			DisableNotifyMessage: input.DisableNotifyMessage,
			forwardedMessageID:   prepared.SendParams.ForwardedMessageID,
			forwardedFromAddress: prepared.SendParams.ForwardedFromAddress,
			Group:                prepared.SendParams.Group,
		}

		out, err := s.sendWaypostMessageWithService(ctx, sendInput, service)
		if err != nil {
			return nil, err
		}
		out["status"] = "forwarded"
		out["source_message_id"] = prepared.SourceMessageID
		out["source_delivery_id"] = nilIfEmpty(prepared.SourceDeliveryID)
		return out, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostMutationToolResult(ctx, out)
}

func (s *Service) waypostWait(ctx context.Context, _ *mcp.CallToolRequest, input waypostWaitInput) (*mcp.CallToolResult, map[string]any, error) {
	if err := validateMCPItems("addresses", len(input.Addresses)); err != nil {
		return nil, nil, err
	}
	addresses, err := s.sessions.waypostAddresses(ctx, input.Addresses)
	if err != nil {
		return nil, nil, err
	}
	warnings := s.waypostReceiveWarnings(ctx, len(input.Addresses) > 0)

	timeoutText := strings.TrimSpace(input.Timeout)
	timeout := time.Duration(0)
	if timeoutText != "" {
		timeout, err = time.ParseDuration(timeoutText)
		if err != nil {
			return nil, nil, fmt.Errorf("parse timeout: %w", err)
		}
	}
	if person := strings.TrimSpace(input.AsPerson); person != "" {
		return s.waypostWaitGroup(ctx, addresses, person, timeout)
	}

	delivery, err := withWaypostService(ctx, s.waypostServices, func(service waypostWaiter) (waypost.ListedDelivery, error) {
		return service.Wait(ctx, waypost.WaitParams{
			Addresses: addresses,
			Timeout:   timeout,
		})
	})
	if errors.Is(err, waypost.ErrNoMessage) {
		return s.waypostToolResult(ctx, map[string]any{
			"status":    "no_message",
			"addresses": addresses,
			"warnings":  warnings,
		})
	}
	if err != nil {
		return nil, nil, err
	}
	return s.waypostToolResult(ctx, map[string]any{
		"status":    "message_available",
		"addresses": addresses,
		"delivery":  waypost.CompactListedDelivery(delivery),
		"warnings":  warnings,
	})
}

func (s *Service) waypostWaitGroup(ctx context.Context, addresses []string, person string, timeout time.Duration) (*mcp.CallToolResult, map[string]any, error) {
	address, err := singleGroupAddress(addresses, "waypost_wait")
	if err != nil {
		return nil, nil, err
	}
	message, err := withWaypostService(ctx, s.waypostServices, func(service waypostGroupMessageWaiter) (waypost.GroupListedMessage, error) {
		return service.WaitGroupMessage(ctx, waypost.GroupWaitParams{
			Address: address,
			Person:  person,
			Timeout: timeout,
		})
	})
	if errors.Is(err, waypost.ErrNoMessage) {
		return s.waypostToolResult(ctx, map[string]any{
			"status":    "no_message",
			"addresses": []string{address},
			"as_person": person,
		})
	}
	if err != nil {
		return nil, nil, err
	}
	return s.waypostToolResult(ctx, map[string]any{
		"status":    "message_available",
		"addresses": []string{address},
		"as_person": person,
		"message":   waypost.CompactGroupListedMessage(message),
	})
}

func (s *Service) waypostRecv(ctx context.Context, req *mcp.CallToolRequest, input waypostRecvInput) (*mcp.CallToolResult, map[string]any, error) {
	if err := validateMCPItems("addresses", len(input.Addresses)); err != nil {
		return nil, nil, err
	}
	if err := validateMCPItems("known_delivery_ids", len(input.KnownDeliveryIDs)); err != nil {
		return nil, nil, err
	}
	person := strings.TrimSpace(input.AsPerson)
	var addresses []string
	var err error
	if person != "" {
		// Group addresses are not MCP bindings; group membership is the
		// authorization boundary for this receive mode.
		addresses, err = s.sessions.waypostAddresses(ctx, input.Addresses)
	} else {
		addresses, err = s.sessions.waypostReceiveAddresses(ctx, input.Addresses)
	}
	if err != nil {
		return nil, nil, err
	}
	if person != "" {
		return s.waypostRecvGroup(ctx, req, addresses, person, input.Diagnostics)
	}
	recvGuard, err := s.waypostRecvGuard.begin(req, s.now, s.waypostRecvActiveSessions())
	if err != nil {
		return nil, nil, err
	}
	recvFinished := false
	finishRecv := func(status string) bool {
		if recvFinished {
			return false
		}
		recvFinished = true
		return recvGuard.finish(status, s.now())
	}
	defer func() {
		finishRecv("")
	}()
	if err := s.reconcileTrackedLeases(ctx); err != nil {
		return nil, nil, err
	}
	warnings := s.waypostReceiveWarnings(ctx, len(input.Addresses) > 0)
	activeLeasePage, err := s.activeLeaseHintPage(addresses, input.KnownDeliveryIDs, input.ActiveLeaseCursor)
	if err != nil {
		return nil, nil, err
	}
	if activeLeasePage.Total > 0 {
		var remainingByState map[string]int
		if input.Diagnostics {
			remainingByState, err = s.remainingByState(ctx, addresses, nil)
			if err != nil {
				return nil, nil, err
			}
		}
		out := map[string]any{
			"status":               "active_leases",
			"active_lease_count":   activeLeasePage.Total,
			"claimed_delivery_ids": activeLeasePage.DeliveryIDs,
		}
		if len(warnings) > 0 {
			out["warnings"] = warnings
		}
		if activeLeasePage.NextCursor != "" {
			out["next_cursor"] = activeLeasePage.NextCursor
		}
		if input.Diagnostics {
			out["addresses"] = addresses
		}
		if input.Diagnostics && len(remainingByState) > 0 {
			out["remaining_by_state"] = remainingByState
		}
		finishRecv("active_leases")
		return s.waypostToolResult(ctx, out)
	}

	delivery, err := s.receivePersonalNow(ctx, addresses)
	if errors.Is(err, waypost.ErrNoMessage) {
		out := map[string]any{
			"status": "no_message",
		}
		// Make remaining queued work visible in the default MCP response. The
		// diagnostics-only remaining_by_state map is easy for agents to miss.
		// This is an informational hint; the next receive still decides whether
		// a queued delivery is currently visible/claimable.
		if delivery.RemainingByState["queued"] > 0 {
			out["notice"] = "more_messages_available"
		}
		if finishRecv("no_message") {
			warnings = append(warnings, waypostRecvPollingWarning)
		}
		if len(warnings) > 0 {
			out["warnings"] = warnings
		}
		if input.Diagnostics {
			out["addresses"] = addresses
		}
		if input.Diagnostics && len(delivery.RemainingByState) > 0 {
			out["remaining_by_state"] = delivery.RemainingByState
		}
		return s.waypostToolResult(ctx, out)
	}
	var recovery *waypost.ReceiveRecoveryRequiredError
	if errors.As(err, &recovery) {
		s.activeLeases.trackReceive(waypost.ReceiveResult{Messages: recovery.Claims}, s.now().Format(time.RFC3339Nano))
		s.startLeaseRenewLoop()
		claims := make([]map[string]any, 0, min(len(recovery.Claims), waypost.MaxPageSize))
		for index, claim := range recovery.Claims {
			if index == waypost.MaxPageSize {
				break
			}
			claims = append(claims, map[string]any{
				"delivery_id":       claim.DeliveryID,
				"lease_token":       claim.LeaseToken,
				"recipient_address": claim.RecipientAddress,
				"lease_expires_at":  claim.LeaseExpiresAt,
			})
		}
		out := map[string]any{
			"status":  "receive_recovery_required",
			"message": recovery.Error(),
			"claims":  claims,
		}
		if input.Diagnostics {
			out["addresses"] = addresses
			out["remaining_by_state_status"] = "unavailable"
		}
		finishRecv("receive_recovery_required")
		return s.waypostMutationToolResult(ctx, out)
	}
	if err != nil {
		return nil, nil, err
	}
	s.activeLeases.trackReceive(delivery, s.now().Format(time.RFC3339Nano))
	s.startLeaseRenewLoop()
	if len(delivery.Messages) != 1 {
		return nil, nil, errors.New("receive returned an unexpected delivery count")
	}
	finishRecv("received")
	out := map[string]any{
		"status":   "received",
		"delivery": waypost.CompactReceivedMessage(delivery.Messages[0]),
	}
	if delivery.RemainingByState["queued"] > 0 {
		out["notice"] = "more_messages_available"
	}
	if len(warnings) > 0 {
		out["warnings"] = warnings
	}
	if input.Diagnostics {
		out["addresses"] = addresses
	}
	if input.Diagnostics && len(delivery.RemainingByState) > 0 {
		out["remaining_by_state"] = delivery.RemainingByState
	}
	return s.waypostMutationToolResult(ctx, out)
}

type activeLeaseHintPage struct {
	DeliveryIDs []string
	Total       int
	NextCursor  string
}

func (s *Service) activeLeaseHintPage(addresses []string, knownDeliveryIDs []string, rawCursor string) (activeLeaseHintPage, error) {
	addressSet := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		addressSet[address] = struct{}{}
	}
	knownSet := knownDeliveryIDSet(knownDeliveryIDs)
	scopeAddresses := append([]string(nil), addresses...)
	sort.Strings(scopeAddresses)
	scope := memoryCursorScope("active-lease-hint", strings.Join(scopeAddresses, "\x00"), strings.Join(normalizedKnownDeliveryIDs(knownDeliveryIDs), "\x00"))
	limit := waypost.MaxPageSize
	_, after, err := normalizeMemoryPage(&limit, rawCursor, "active-lease-hint", scope)
	if err != nil {
		return activeLeaseHintPage{}, err
	}
	leases := s.activeLeases.snapshot()
	sort.Slice(leases, func(i, j int) bool {
		return leases[i].DeliveryID < leases[j].DeliveryID
	})
	page := activeLeaseHintPage{DeliveryIDs: make([]string, 0, waypost.MaxPageSize)}
	more := false
	for _, lease := range leases {
		if _, known := knownSet[lease.DeliveryID]; known {
			continue
		}
		if _, wanted := addressSet[lease.RecipientAddress]; !wanted {
			continue
		}
		page.Total++
		if lease.DeliveryID <= after {
			continue
		}
		if len(page.DeliveryIDs) == waypost.MaxPageSize {
			more = true
			continue
		}
		page.DeliveryIDs = append(page.DeliveryIDs, lease.DeliveryID)
	}
	if more {
		page.NextCursor, err = encodeMemoryPageCursor("active-lease-hint", scope, page.DeliveryIDs[len(page.DeliveryIDs)-1])
		if err != nil {
			return activeLeaseHintPage{}, err
		}
	}
	return page, nil
}

func knownDeliveryIDSet(deliveryIDs []string) map[string]struct{} {
	knownSet := make(map[string]struct{}, len(deliveryIDs))
	for _, deliveryID := range deliveryIDs {
		deliveryID = strings.TrimSpace(deliveryID)
		if deliveryID == "" {
			continue
		}
		knownSet[deliveryID] = struct{}{}
	}
	return knownSet
}

func normalizedKnownDeliveryIDs(deliveryIDs []string) []string {
	known := make([]string, 0, len(deliveryIDs))
	for deliveryID := range knownDeliveryIDSet(deliveryIDs) {
		known = append(known, deliveryID)
	}
	sort.Strings(known)
	return known
}

func (s *Service) waypostClaimHistory(ctx context.Context, _ *mcp.CallToolRequest, input waypostClaimHistoryInput) (*mcp.CallToolResult, map[string]any, error) {
	deliveryID := strings.TrimSpace(input.DeliveryID)
	if input.RecoverLeaseToken && deliveryID == "" {
		return nil, nil, errors.New("recover_lease_token requires delivery_id")
	}
	if deliveryID != "" && (input.Limit != nil || strings.TrimSpace(input.Cursor) != "") {
		return nil, nil, errors.New("limit and cursor are not supported with delivery_id")
	}
	if err := s.reconcileTrackedLeases(ctx); err != nil {
		return nil, nil, err
	}
	leases := s.activeLeases.historySnapshot(input.IncludeTerminal || deliveryID != "")
	sort.Slice(leases, func(i, j int) bool {
		return leases[i].DeliveryID < leases[j].DeliveryID
	})
	pageSize, after, err := normalizeMemoryPage(input.Limit, input.Cursor, "claim-history", fmt.Sprintf("terminal=%t", input.IncludeTerminal))
	if err != nil {
		return nil, nil, err
	}

	items := make([]map[string]any, 0, min(len(leases), pageSize))
	for _, lease := range leases {
		if deliveryID != "" && lease.DeliveryID != deliveryID {
			continue
		}
		if deliveryID == "" && lease.DeliveryID <= after {
			continue
		}
		if deliveryID == "" && len(items) == pageSize {
			break
		}
		item := compactWaypostClaimHistoryItem(lease, input.Diagnostics)
		if input.RecoverLeaseToken && lease.LeaseToken != "" {
			item["lease_token"] = lease.LeaseToken
		}
		items = append(items, item)
	}
	if deliveryID != "" && len(items) == 0 {
		return s.waypostToolResult(ctx, map[string]any{
			"status":      "not_found",
			"delivery_id": deliveryID,
		})
	}
	out := map[string]any{
		"status": "listed",
		"items":  items,
	}
	if deliveryID == "" && len(items) == pageSize {
		lastID := items[len(items)-1]["delivery_id"].(string)
		for _, lease := range leases {
			if lease.DeliveryID > lastID {
				nextCursor, err := encodeMemoryPageCursor("claim-history", fmt.Sprintf("terminal=%t", input.IncludeTerminal), lastID)
				if err != nil {
					return nil, nil, err
				}
				out["next_cursor"] = nextCursor
				break
			}
		}
	}
	return s.waypostToolResult(ctx, out)
}

func compactWaypostClaimHistoryItem(lease activeLease, diagnostics bool) map[string]any {
	item := map[string]any{
		"delivery_id":       lease.DeliveryID,
		"recipient_address": lease.RecipientAddress,
		"subject":           lease.Subject,
		"status":            lease.Status,
	}
	if lease.Status == "active" && lease.LeaseExpiresAt != "" {
		item["lease_expires_at"] = lease.LeaseExpiresAt
	}
	if lease.Status != "active" && lease.TerminalAt != "" {
		item["terminal_at"] = lease.TerminalAt
	}
	if diagnostics {
		if lease.ContentType != "" {
			item["content_type"] = lease.ContentType
		}
		if lease.ClaimedAt != "" {
			item["claimed_at"] = lease.ClaimedAt
		}
		if lease.LastRenewedAt != "" {
			item["last_renewed_at"] = lease.LastRenewedAt
		}
	}
	return item
}

func (s *Service) waypostRecvGroup(ctx context.Context, req *mcp.CallToolRequest, addresses []string, person string, includeDetails bool) (*mcp.CallToolResult, map[string]any, error) {
	address, err := singleGroupAddress(addresses, "waypost_recv")
	if err != nil {
		return nil, nil, err
	}
	recvGuard, err := s.waypostRecvGuard.begin(req, s.now, s.waypostRecvActiveSessions())
	if err != nil {
		return nil, nil, err
	}
	recvFinished := false
	finishRecv := func(status string) bool {
		if recvFinished {
			return false
		}
		recvFinished = true
		return recvGuard.finish(status, s.now())
	}
	defer func() {
		finishRecv("")
	}()
	message, err := s.receiveGroupNow(ctx, address, person)
	if errors.Is(err, waypost.ErrNoMessage) {
		out := map[string]any{
			"status": "no_message",
		}
		if finishRecv("no_message") {
			out["warnings"] = []string{waypostRecvPollingWarning}
		}
		if includeDetails {
			out["addresses"] = []string{address}
			out["as_person"] = person
		}
		return s.waypostToolResult(ctx, out)
	}
	if err != nil {
		return nil, nil, err
	}
	finishRecv("received")
	out := map[string]any{
		"status":  "received",
		"message": compactWaypostGroupReceivedMessage(message, includeDetails),
	}
	if includeDetails {
		out["addresses"] = []string{address}
		out["as_person"] = person
	}
	return s.waypostMutationToolResult(ctx, out)
}

func compactWaypostGroupReceivedMessage(message waypost.GroupReceivedMessage, includeDetails bool) any {
	full := waypost.CompactGroupReceivedMessage(message)
	if includeDetails {
		return full
	}
	compact := map[string]any{
		"message_id": full.MessageID,
		"subject":    full.Subject,
		"body":       full.Body,
	}
	if full.ForwardedFromAddress != nil {
		compact["forwarded_from_address"] = full.ForwardedFromAddress
	}
	if full.SenderAddress != nil {
		compact["sender_address"] = full.SenderAddress
	}
	if full.ContentType != "" {
		compact["content_type"] = full.ContentType
	}
	return compact
}

func (s *Service) receivePersonalNow(ctx context.Context, addresses []string) (waypost.ReceiveResult, error) {
	claimMetadata := s.receiveClaimMetadata(addresses)
	return withWaypostService(ctx, s.waypostServices, func(service waypostBatchReceiver) (waypost.ReceiveResult, error) {
		// MCP claims only immediately visible work. Waiting stays in waypost_wait so
		// abandoned tool calls cannot later claim delivery into an unreachable result.
		return service.ReceiveBatchWithLeaseTTL(waypost.WithClaimMetadata(ctx, claimMetadata), waypost.ReceiveBatchParams{
			Addresses: addresses,
			Max:       1,
		}, s.mcpLeaseTTL)
	})
}

func (s *Service) remainingByState(ctx context.Context, addresses, excludedDeliveryIDs []string) (map[string]int, error) {
	return withWaypostService(ctx, s.waypostServices, func(service waypostRemainingCounter) (map[string]int, error) {
		return service.RemainingByState(ctx, addresses, excludedDeliveryIDs)
	})
}

func (s *Service) receiveClaimMetadata(addresses []string) waypost.ClaimMetadata {
	snapshot := s.sessions.snapshotState()
	return waypost.ClaimMetadata{
		Source:             "mcp",
		Tool:               "waypost_recv",
		BoundAddresses:     addresses,
		AgentDeckSessionID: snapshot.DetectedAgentDeckSession,
		AgentSessionID:     snapshot.DetectedToolSessions["codex"],
		Workdir:            snapshot.DefaultWorkdir,
	}
}

func (s *Service) receiveGroupNow(ctx context.Context, address, person string) (waypost.GroupReceivedMessage, error) {
	return withWaypostService(ctx, s.waypostServices, func(service waypostGroupMessageReceiver) (waypost.GroupReceivedMessage, error) {
		return service.ReceiveGroupMessage(ctx, waypost.GroupReceiveParams{
			Address: address,
			Person:  person,
		})
	})
}

func singleGroupAddress(addresses []string, toolName string) (string, error) {
	if len(addresses) != 1 {
		return "", fmt.Errorf("%s with as_person requires exactly one group address", toolName)
	}
	address := addresses[0]
	if !waypost.IsGroupAddress(address) {
		return "", fmt.Errorf("%s with as_person requires a group address", toolName)
	}
	return address, nil
}

func (s *Service) waypostReceiveWarnings(ctx context.Context, explicitAddresses bool) []string {
	if explicitAddresses {
		return nil
	}
	bound, err := s.sessions.boundState(ctx)
	if err != nil {
		return []string{"unable to verify bound agent session state: " + err.Error()}
	}
	toolSessionBound := len(bound.DetectedToolSessionAddresses) > 0 || len(boundToolSessionAddresses(bound.BoundAddresses)) > 0
	agentDeckBound := bound.DetectedAgentDeckSession != "" || len(boundAddressesByScheme(bound.BoundAddresses, "agent-deck")) > 0
	if !toolSessionBound || agentDeckBound {
		return nil
	}
	return []string{agentDeckBindRecoveryHint}
}

func (s *Service) waypostList(ctx context.Context, _ *mcp.CallToolRequest, input waypostListInput) (*mcp.CallToolResult, map[string]any, error) {
	var address string
	if strings.TrimSpace(input.Address) != "" {
		address = strings.TrimSpace(input.Address)
	} else {
		boundAddresses, err := s.sessions.waypostAddresses(ctx, nil)
		if err != nil {
			return nil, nil, err
		}
		if len(boundAddresses) != 1 {
			return nil, nil, errors.New("waypost_list requires address when multiple waypost addresses are bound")
		}
		address = boundAddresses[0]
	}
	if input.AsPerson != "" && input.State != "" {
		return nil, nil, errors.New("waypost_list does not support state together with as_person")
	}

	deliveries, err := withWaypostService(ctx, s.waypostServices, func(service interface {
		waypostLister
		waypostGroupMessageLister
	}) (any, error) {
		if input.AsPerson != "" {
			messages, err := service.ListGroupMessages(ctx, waypost.GroupListParams{
				Address: address,
				Person:  input.AsPerson,
			})
			if err != nil {
				return nil, err
			}
			summaries := make([]waypost.GroupListedMessageCompact, 0, len(messages))
			for _, message := range messages {
				summaries = append(summaries, waypost.CompactGroupListedMessage(message))
			}
			return summaries, nil
		}
		return service.List(ctx, waypost.ListParams{
			Address: address,
			State:   input.State,
		})
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostToolResult(ctx, map[string]any{
		"status":     "listed",
		"address":    address,
		"as_person":  nilIfEmpty(input.AsPerson),
		"state":      nilIfEmpty(input.State),
		"deliveries": deliveries,
	})
}

func (s *Service) waypostRead(ctx context.Context, _ *mcp.CallToolRequest, input waypostReadInput) (*mcp.CallToolResult, map[string]any, error) {
	if err := validateMCPItems("message_ids", len(input.MessageIDs)); err != nil {
		return nil, nil, err
	}
	if err := validateMCPItems("delivery_ids", len(input.DeliveryIDs)); err != nil {
		return nil, nil, err
	}
	if err := validateMCPItems("addresses", len(input.Addresses)); err != nil {
		return nil, nil, err
	}
	if input.Limit != nil && (*input.Limit < 1 || *input.Limit > waypost.MaxPageSize) {
		return nil, nil, fmt.Errorf("limit must be between 1 and %d", waypost.MaxPageSize)
	}
	hasMessageIDs := len(input.MessageIDs) > 0
	hasDeliveryIDs := len(input.DeliveryIDs) > 0
	wantsLatest := input.Latest
	modeCount := 0
	if hasMessageIDs {
		modeCount++
	}
	if hasDeliveryIDs {
		modeCount++
	}
	if wantsLatest {
		modeCount++
	}
	if modeCount != 1 {
		return nil, nil, errors.New("waypost_read requires exactly one mode: message_ids, delivery_ids, or latest=true")
	}

	result := map[string]any{
		"status": "read",
		"mode":   "unknown",
	}

	switch {
	case wantsLatest:
		addresses, err := s.sessions.waypostAddresses(ctx, input.Addresses)
		if err != nil {
			return nil, nil, err
		}
		result["mode"] = "latest"
		result["addresses"] = addresses
		if input.State == "" {
			result["state"] = "any"
		} else {
			result["state"] = input.State
		}
		if input.Limit == nil {
			result["limit"] = nil
		} else {
			result["limit"] = *input.Limit
		}
	case hasMessageIDs:
		if len(input.Addresses) > 0 || input.State != "" || input.Limit != nil {
			return nil, nil, errors.New("waypost_read message_ids mode does not support addresses, state, or limit")
		}
		messageIDs := dedupe(input.MessageIDs)
		result["mode"] = "message_ids"
		result["message_ids"] = messageIDs
		messages, err := withWaypostService(ctx, s.waypostServices, func(service waypostMessageReader) ([]waypost.ReadMessage, error) {
			return service.ReadMessages(ctx, messageIDs)
		})
		if err != nil {
			return nil, nil, err
		}
		result["items"] = messages
		return s.waypostToolResult(ctx, result)
	default:
		if len(input.Addresses) > 0 || input.State != "" || input.Limit != nil {
			return nil, nil, errors.New("waypost_read delivery_ids mode does not support addresses, state, or limit")
		}
		deliveryIDs := dedupe(input.DeliveryIDs)
		result["mode"] = "delivery_ids"
		result["delivery_ids"] = deliveryIDs
		deliveries, err := withWaypostService(ctx, s.waypostServices, func(service waypostDeliveryReader) ([]waypost.ReadDelivery, error) {
			return service.ReadDeliveries(ctx, deliveryIDs)
		})
		if err != nil {
			return nil, nil, err
		}
		result["items"] = deliveries
		return s.waypostToolResult(ctx, result)
	}

	latest, err := withWaypostService(ctx, s.waypostServices, func(service waypostLatestDeliveryReader) (readLatestResult, error) {
		limit := 1
		if input.Limit != nil {
			limit = *input.Limit
		}
		items, hasMore, err := service.ReadLatestDeliveries(ctx, result["addresses"].([]string), input.State, limit)
		if err != nil {
			return readLatestResult{}, err
		}
		return readLatestResult{Items: items, HasMore: hasMore}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	result["items"] = latest.Items
	if latest.HasMore {
		result["has_more"] = true
	}
	return s.waypostToolResult(ctx, result)
}

func (s *Service) waypostAck(ctx context.Context, _ *mcp.CallToolRequest, input waypostAckInput) (*mcp.CallToolResult, map[string]any, error) {
	if err := s.activeLeases.terminalMutationAllowed(input.DeliveryID); err != nil {
		return nil, nil, err
	}
	result, err := withWaypostService(ctx, s.waypostServices, func(service waypostDeliveryTransitioner) (waypost.DeliveryTransitionResult, error) {
		return service.Ack(ctx, input.DeliveryID, input.LeaseToken)
	})
	if err != nil {
		return nil, nil, err
	}
	s.activeLeases.markTerminal(input.DeliveryID, result.State, s.now().Format(time.RFC3339Nano))
	return s.waypostMutationToolResult(ctx, map[string]any{"status": "acked", "delivery_id": input.DeliveryID})
}

func (s *Service) waypostRelease(ctx context.Context, _ *mcp.CallToolRequest, input waypostAckInput) (*mcp.CallToolResult, map[string]any, error) {
	if err := s.activeLeases.terminalMutationAllowed(input.DeliveryID); err != nil {
		return nil, nil, err
	}
	result, err := withWaypostService(ctx, s.waypostServices, func(service waypostDeliveryTransitioner) (waypost.DeliveryTransitionResult, error) {
		return service.Release(ctx, input.DeliveryID, input.LeaseToken)
	})
	if err != nil {
		return nil, nil, err
	}
	s.activeLeases.markTerminal(input.DeliveryID, result.State, s.now().Format(time.RFC3339Nano))
	return s.waypostMutationToolResult(ctx, map[string]any{"status": "released", "delivery_id": input.DeliveryID})
}

func (s *Service) waypostDefer(ctx context.Context, _ *mcp.CallToolRequest, input waypostDeferInput) (*mcp.CallToolResult, map[string]any, error) {
	if err := s.activeLeases.terminalMutationAllowed(input.DeliveryID); err != nil {
		return nil, nil, err
	}
	until, err := time.Parse(time.RFC3339Nano, input.Until)
	if err != nil {
		return nil, nil, fmt.Errorf("parse until: %w", err)
	}
	result, err := withWaypostService(ctx, s.waypostServices, func(service waypostDeliveryTransitioner) (waypost.DeliveryTransitionResult, error) {
		return service.Defer(ctx, input.DeliveryID, input.LeaseToken, until)
	})
	if err != nil {
		return nil, nil, err
	}
	s.activeLeases.markTerminal(input.DeliveryID, result.State, s.now().Format(time.RFC3339Nano))
	return s.waypostMutationToolResult(ctx, map[string]any{"status": "deferred", "delivery_id": input.DeliveryID, "until": input.Until})
}

func (s *Service) waypostUndefer(ctx context.Context, _ *mcp.CallToolRequest, input waypostUndeferInput) (*mcp.CallToolResult, map[string]any, error) {
	result, err := withWaypostService(ctx, s.waypostServices, func(service waypostDeliveryTransitioner) (waypost.DeliveryTransitionResult, error) {
		return service.Undefer(ctx, input.DeliveryID)
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostMutationToolResult(ctx, map[string]any{
		"status":      "undeferred",
		"delivery_id": result.DeliveryID,
		"visible_at":  result.VisibleAt,
	})
}

func (s *Service) waypostFail(ctx context.Context, _ *mcp.CallToolRequest, input waypostFailInput) (*mcp.CallToolResult, map[string]any, error) {
	if err := s.activeLeases.terminalMutationAllowed(input.DeliveryID); err != nil {
		return nil, nil, err
	}
	_, err := withWaypostService(ctx, s.waypostServices, func(service waypostDeliveryTransitioner) (waypost.DeliveryTransitionResult, error) {
		return service.Fail(ctx, input.DeliveryID, input.LeaseToken, input.Reason)
	})
	if err != nil {
		return nil, nil, err
	}
	s.activeLeases.markTerminal(input.DeliveryID, "failed", s.now().Format(time.RFC3339Nano))
	return s.waypostMutationToolResult(ctx, map[string]any{"status": "failed", "delivery_id": input.DeliveryID, "reason": input.Reason})
}

func (s *Service) waypostGroupCreate(ctx context.Context, _ *mcp.CallToolRequest, input waypostGroupInput) (*mcp.CallToolResult, map[string]any, error) {
	group, err := withWaypostService(ctx, s.waypostServices, func(service waypostGroupManager) (waypost.GroupRecord, error) {
		return service.CreateGroup(ctx, input.GroupAddress)
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostMutationToolResult(ctx, map[string]any{
		"status": "created",
		"group":  group,
	})
}

func (s *Service) waypostGroupAddMember(ctx context.Context, _ *mcp.CallToolRequest, input waypostGroupMemberInput) (*mcp.CallToolResult, map[string]any, error) {
	membership, err := withWaypostService(ctx, s.waypostServices, func(service waypostGroupManager) (waypost.GroupMembershipRecord, error) {
		return service.AddGroupMember(ctx, input.GroupAddress, input.Person)
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostMutationToolResult(ctx, map[string]any{
		"status":     "added",
		"membership": membership,
	})
}

func (s *Service) waypostGroupRemoveMember(ctx context.Context, _ *mcp.CallToolRequest, input waypostGroupMemberInput) (*mcp.CallToolResult, map[string]any, error) {
	membership, err := withWaypostService(ctx, s.waypostServices, func(service waypostGroupManager) (waypost.GroupMembershipRecord, error) {
		return service.RemoveGroupMember(ctx, input.GroupAddress, input.Person)
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostMutationToolResult(ctx, map[string]any{
		"status":     "removed",
		"membership": membership,
	})
}

func (s *Service) waypostGroupMembers(ctx context.Context, _ *mcp.CallToolRequest, input waypostGroupInput) (*mcp.CallToolResult, map[string]any, error) {
	memberships, err := withWaypostService(ctx, s.waypostServices, func(service waypostGroupManager) ([]waypost.GroupMembershipRecord, error) {
		return service.ListGroupMembers(ctx, input.GroupAddress)
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostToolResult(ctx, map[string]any{
		"status":      "listed",
		"group":       input.GroupAddress,
		"memberships": memberships,
	})
}

func (s *Service) waypostGroupAddSubscriber(ctx context.Context, _ *mcp.CallToolRequest, input waypostGroupSubscriberInput) (*mcp.CallToolResult, map[string]any, error) {
	subscriber, err := withWaypostService(ctx, s.waypostServices, func(service waypostGroupSubscriberManager) (waypost.GroupNotificationSubscriberRecord, error) {
		return service.AddGroupNotificationSubscriber(ctx, input.GroupAddress, input.NotifyAddress, input.Person)
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostMutationToolResult(ctx, map[string]any{
		"status":     "added",
		"subscriber": subscriber,
	})
}

func (s *Service) waypostGroupRemoveSubscriber(ctx context.Context, _ *mcp.CallToolRequest, input waypostGroupSubscriberRemoveInput) (*mcp.CallToolResult, map[string]any, error) {
	subscriber, err := withWaypostService(ctx, s.waypostServices, func(service waypostGroupSubscriberManager) (waypost.GroupNotificationSubscriberRecord, error) {
		return service.RemoveGroupNotificationSubscriber(ctx, input.GroupAddress, input.NotifyAddress)
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostMutationToolResult(ctx, map[string]any{
		"status":     "removed",
		"subscriber": subscriber,
	})
}

func (s *Service) waypostGroupSubscribers(ctx context.Context, _ *mcp.CallToolRequest, input waypostGroupInput) (*mcp.CallToolResult, map[string]any, error) {
	subscribers, err := withWaypostService(ctx, s.waypostServices, func(service waypostGroupSubscriberManager) ([]waypost.GroupNotificationSubscriberRecord, error) {
		return service.ListGroupNotificationSubscribers(ctx, input.GroupAddress)
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostToolResult(ctx, map[string]any{
		"status":      "listed",
		"group":       input.GroupAddress,
		"subscribers": subscribers,
	})
}

func (s *Service) waypostAddressInspect(ctx context.Context, _ *mcp.CallToolRequest, input waypostAddressInspectInput) (*mcp.CallToolResult, map[string]any, error) {
	inspection, err := withWaypostService(ctx, s.waypostServices, func(service waypostAddressInspector) (waypost.AddressInspection, error) {
		return service.InspectAddress(ctx, input.Address)
	})
	if err != nil {
		return nil, nil, err
	}
	return s.waypostToolResult(ctx, map[string]any{
		"status":     "inspected",
		"inspection": inspection,
	})
}

func (s *Service) waypostToolResult(ctx context.Context, result map[string]any) (*mcp.CallToolResult, map[string]any, error) {
	return s.toolResult(ctx, result)
}

func (s *Service) waypostMutationToolResult(ctx context.Context, result map[string]any) (*mcp.CallToolResult, map[string]any, error) {
	s.emitWaypostOverviewUpdatedBestEffort(ctx)
	return s.toolResult(ctx, result)
}
