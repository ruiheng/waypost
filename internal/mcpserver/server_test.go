package mcpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/ruiheng/waypost/internal/waypost"
)

type fakeWaypostService struct {
	t *testing.T

	sendFunc                  func(context.Context, waypost.SendParams) (waypost.SendResult, error)
	listFunc                  func(context.Context, waypost.ListParams) ([]waypost.ListedDelivery, error)
	listGroupMessagesFunc     func(context.Context, waypost.GroupListParams) ([]waypost.GroupListedMessage, error)
	waitGroupMessageFunc      func(context.Context, waypost.GroupWaitParams) (waypost.GroupListedMessage, error)
	receiveGroupMessageFunc   func(context.Context, waypost.GroupReceiveParams) (waypost.GroupReceivedMessage, error)
	createGroupFunc           func(context.Context, string) (waypost.GroupRecord, error)
	addGroupMemberFunc        func(context.Context, string, string) (waypost.GroupMembershipRecord, error)
	removeGroupMemberFunc     func(context.Context, string, string) (waypost.GroupMembershipRecord, error)
	listGroupMembersFunc      func(context.Context, string) ([]waypost.GroupMembershipRecord, error)
	addGroupSubscriberFunc    func(context.Context, string, string, string) (waypost.GroupNotificationSubscriberRecord, error)
	removeGroupSubscriberFunc func(context.Context, string, string) (waypost.GroupNotificationSubscriberRecord, error)
	listGroupSubscribersFunc  func(context.Context, string) ([]waypost.GroupNotificationSubscriberRecord, error)
	inspectAddressFunc        func(context.Context, string) (waypost.AddressInspection, error)
	listClaimableFunc         func(context.Context, []string) ([]waypost.ClaimableAddress, error)
	receiveBatchFunc          func(context.Context, waypost.ReceiveBatchParams) (waypost.ReceiveResult, error)
	receiveBatchWithTTLFunc   func(context.Context, waypost.ReceiveBatchParams, time.Duration) (waypost.ReceiveResult, error)
	waitFunc                  func(context.Context, waypost.WaitParams) (waypost.ListedDelivery, error)
	readMessagesFunc          func(context.Context, []string) ([]waypost.ReadMessage, error)
	readLatestFunc            func(context.Context, []string, string, int) ([]waypost.ReadDelivery, bool, error)
	readDeliveriesFunc        func(context.Context, []string) ([]waypost.ReadDelivery, error)
	ackFunc                   func(context.Context, string, string) (waypost.DeliveryTransitionResult, error)
	renewFunc                 func(context.Context, string, string, time.Duration) (waypost.LeaseRenewResult, error)
	releaseFunc               func(context.Context, string, string) (waypost.DeliveryTransitionResult, error)
	deferFunc                 func(context.Context, string, string, time.Time) (waypost.DeliveryTransitionResult, error)
	undeferFunc               func(context.Context, string) (waypost.DeliveryTransitionResult, error)
	failFunc                  func(context.Context, string, string, string) (waypost.DeliveryTransitionResult, error)
	inspectLeaseFunc          func(context.Context, string) (waypost.DeliveryLeaseState, error)
	remainingByStateFunc      func(context.Context, []string, []string) (map[string]int, error)
	leaseMu                   sync.Mutex
	leaseStates               map[string]waypost.DeliveryLeaseState
}

func TestAgentDeckCreateSessionSchemaRequiresWorkdir(t *testing.T) {
	schema, err := jsonschema.For[agentDeckCreateSessionInput](nil)
	if err != nil {
		t.Fatalf("jsonschema.For() error = %v", err)
	}
	if !slices.Contains(schema.Required, "workdir") {
		t.Fatalf("required fields = %v, want workdir", schema.Required)
	}
}

func TestAgentDeckRequireSessionSchemaSupportsOptionalAutoRestart(t *testing.T) {
	schema, err := jsonschema.For[agentDeckRequireSessionInput](nil)
	if err != nil {
		t.Fatalf("jsonschema.For() error = %v", err)
	}
	if _, ok := schema.Properties["auto_restart"]; !ok {
		t.Fatalf("schema.Properties missing auto_restart: %v", schema.Properties)
	}
	if slices.Contains(schema.Required, "auto_restart") {
		t.Fatalf("required fields = %v, do not want auto_restart", schema.Required)
	}
}

func TestAgentDeckRequireSessionSchemaRequiresWorkdir(t *testing.T) {
	schema, err := jsonschema.For[agentDeckRequireSessionInput](nil)
	if err != nil {
		t.Fatalf("jsonschema.For() error = %v", err)
	}
	if !slices.Contains(schema.Required, "workdir") {
		t.Fatalf("required fields = %v, want workdir", schema.Required)
	}
	if _, ok := schema.Properties["sessions"]; !ok {
		t.Fatalf("schema.Properties missing sessions: %v", schema.Properties)
	}
}

func TestAgentDeckRequireSessionSchemaOmitsCreateOnlyFields(t *testing.T) {
	schema, err := jsonschema.For[agentDeckRequireSessionInput](nil)
	if err != nil {
		t.Fatalf("jsonschema.For() error = %v", err)
	}

	for _, field := range []string{
		"ensure_title",
		"ensure_cmd",
		"parent_session_id",
		"group_path",
		"group_parent_session_id",
		"child_group_name",
		"no_parent_link",
		"startup_instruction",
	} {
		if _, ok := schema.Properties[field]; ok {
			t.Fatalf("schema.Properties[%q] unexpectedly present", field)
		}
	}
}

func TestWaypostSendSchemaExposesSingleOrBatchTarget(t *testing.T) {
	schema := waypostSendInputSchema()
	if _, ok := schema.Properties["group"]; !ok {
		t.Fatalf("schema.Properties missing group: %v", schema.Properties)
	}
	if _, ok := schema.Properties["as_person"]; !ok {
		t.Fatalf("schema.Properties missing as_person: %v", schema.Properties)
	}
	diagnostics, ok := schema.Properties["diagnostics"]
	if !ok {
		t.Fatalf("schema.Properties missing diagnostics: %v", schema.Properties)
	}
	if _, ok := schema.Properties["include_details"]; ok {
		t.Fatalf("schema.Properties unexpectedly includes include_details: %v", schema.Properties)
	}
	if got, want := diagnostics.Description, "Unnecessary for normal send."; got != want {
		t.Fatalf("diagnostics description = %q, want %q", got, want)
	}
	if !slices.Contains(schema.Required, "to") {
		t.Fatalf("required fields = %v, want to", schema.Required)
	}
	if len(schema.OneOf) != 0 {
		t.Fatalf("oneOf = %#v, want no top-level alternatives", schema.OneOf)
	}
	to := schema.Properties["to"]
	if to == nil || !slices.Equal(to.Types, []string{"string", "array"}) || to.Items == nil || to.Items.Type != "string" {
		t.Fatalf("to schema = %#v, want string or string array", to)
	}
	if to.MinItems == nil || *to.MinItems != 1 || to.MaxItems == nil || *to.MaxItems != waypost.MaxSendRecipients {
		t.Fatalf("to schema = %#v, want array length 1-%d", to, waypost.MaxSendRecipients)
	}
	if _, ok := schema.Properties["to_address"]; ok {
		t.Fatalf("schema.Properties unexpectedly contains to_address: %v", schema.Properties)
	}
	if _, ok := schema.Properties["to_addresses"]; ok {
		t.Fatalf("schema.Properties unexpectedly contains to_addresses: %v", schema.Properties)
	}
	for _, field := range []string{"body", "body_file"} {
		property, ok := schema.Properties[field]
		if !ok {
			t.Fatalf("schema.Properties missing %s: %v", field, schema.Properties)
		}
		if slices.Contains(schema.Required, field) {
			t.Fatalf("required fields = %v, want runtime choice between body and body_file", schema.Required)
		}
		if !strings.Contains(property.Description, "exactly one of body or body_file") {
			t.Fatalf("%s description = %q, want mutual-exclusion guidance", field, property.Description)
		}
	}
}

func TestWaypostSendReadsBodyFile(t *testing.T) {
	workdir := t.TempDir()
	bodyFile := filepath.Join(workdir, " message.md ")
	if err := os.WriteFile(bodyFile, []byte("review from file\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if got, want := string(params.Body), "review from file\n"; got != want {
			t.Fatalf("send body = %q, want %q", got, want)
		}
		return waypost.SendResult{DeliveryID: "dlv_file"}, nil
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(_ []string, _ string) (RunResult, error) {
			return RunResult{ExitCode: 1}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.defaultWorkdir = workdir
	setBoundTestSender(service, "agent/sender")

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":                     "workflow/reviewer",
		"from_address":           "agent/sender",
		"subject":                "file body",
		"body_file":              filepath.Base(bodyFile),
		"disable_notify_message": true,
	})
	if output["delivery_id"] != "dlv_file" {
		t.Fatalf("delivery_id = %v, want dlv_file", output["delivery_id"])
	}
}

func TestWaypostSendBodyFileBatchUsesOneSnapshot(t *testing.T) {
	workdir := t.TempDir()
	bodyFile := filepath.Join(workdir, "message.md")
	if err := os.WriteFile(bodyFile, []byte("original body"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	waypostService := &fakeWaypostService{t: t}
	var bodies []string
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		bodies = append(bodies, string(params.Body))
		if len(bodies) == 1 {
			if err := os.WriteFile(bodyFile, []byte("changed body"), 0o600); err != nil {
				t.Fatalf("WriteFile(changed body) error = %v", err)
			}
		}
		return waypost.SendResult{DeliveryID: "dlv_" + strings.TrimPrefix(params.ToAddress, "workflow/")}, nil
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(_ []string, _ string) (RunResult, error) {
			return RunResult{ExitCode: 1}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.defaultWorkdir = workdir
	setBoundTestSender(service, "agent/sender")

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":                     []string{"workflow/one", "workflow/two"},
		"from_address":           "agent/sender",
		"subject":                "file snapshot",
		"body_file":              bodyFile,
		"disable_notify_message": true,
	})
	if output["status"] != "sent" {
		t.Fatalf("status = %v, want sent", output["status"])
	}
	if want := []string{"original body", "original body"}; !reflect.DeepEqual(bodies, want) {
		t.Fatalf("send bodies = %q, want one file snapshot %q", bodies, want)
	}
}

func TestWaypostSendRejectsInvalidBodySourcesBeforeSending(t *testing.T) {
	workdir := t.TempDir()
	emptyFile := filepath.Join(workdir, "empty.md")
	if err := os.WriteFile(emptyFile, nil, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	missingFile := filepath.Join(workdir, "missing.md")

	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		t.Fatalf("unexpected Send call: %+v", params)
		return waypost.SendResult{}, nil
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.defaultWorkdir = workdir

	tests := []struct {
		name        string
		bodyArgs    map[string]any
		wantMessage string
	}{
		{name: "missing source", bodyArgs: map[string]any{}, wantMessage: "requires body or body_file"},
		{name: "both sources", bodyArgs: map[string]any{"body": "inline", "body_file": missingFile}, wantMessage: "exactly one of body or body_file"},
		{name: "empty inline body", bodyArgs: map[string]any{"body": ""}, wantMessage: waypost.ErrEmptyBody.Error()},
		{name: "empty file path", bodyArgs: map[string]any{"body_file": "  "}, wantMessage: "body_file must not be empty"},
		{name: "empty file", bodyArgs: map[string]any{"body_file": emptyFile}, wantMessage: waypost.ErrEmptyBody.Error()},
		{name: "missing file", bodyArgs: map[string]any{"body_file": missingFile}, wantMessage: "resolve waypost_send body_file"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			arguments := map[string]any{
				"to":      "workflow/reviewer",
				"subject": "invalid body source",
			}
			for key, value := range test.bodyArgs {
				arguments[key] = value
			}
			err := callServiceToolExpectError(t, service, "waypost_send", arguments)
			if err == nil || !strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf("waypost_send error = %v, want containing %q", err, test.wantMessage)
			}
		})
	}
}

func TestWaypostSendBatchReturnsOrderedPartialResults(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	var calls []waypost.SendParams
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		calls = append(calls, params)
		switch params.ToAddress {
		case "workflow/two":
			return waypost.SendResult{}, errors.New("commit send transaction: test failure")
		default:
			return waypost.SendResult{DeliveryID: "dlv_" + strings.TrimPrefix(params.ToAddress, "workflow/")}, nil
		}
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(_ []string, _ string) (RunResult, error) {
			return RunResult{ExitCode: 1}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	setBoundTestSender(service, "agent/sender")

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":                     []string{" workflow/one ", "workflow/two", "workflow/one", "workflow/three"},
		"from_address":           "agent/sender",
		"subject":                " batch subject ",
		"body":                   "body",
		"disable_notify_message": true,
	})

	if got := output["status"]; got != "partial_failed" {
		t.Fatalf("status = %v, want partial_failed", got)
	}
	if output["recipient_count"] != float64(3) || output["sent_count"] != float64(2) || output["failed_count"] != float64(1) {
		t.Fatalf("batch counts = %v, want 3 recipients, 2 sent, 1 failed", output)
	}
	if want := []string{"workflow/one", "workflow/two", "workflow/three"}; !reflect.DeepEqual(sendParamAddresses(calls), want) {
		t.Fatalf("send addresses = %v, want %v", sendParamAddresses(calls), want)
	}
	if got := output["to_addresses"]; !reflect.DeepEqual(got, []any{"workflow/one", "workflow/two", "workflow/three"}) {
		t.Fatalf("to_addresses = %#v, want normalized first-seen order", got)
	}

	results, ok := output["results"].([]any)
	if !ok || len(results) != 3 {
		t.Fatalf("results = %#v, want three items", output["results"])
	}
	first := results[0].(map[string]any)
	if first["status"] != "sent" || first["to_address"] != "workflow/one" || first["from_address"] != "agent/sender" || first["subject"] != "batch subject" {
		t.Fatalf("first batch result = %v", first)
	}
	second := results[1].(map[string]any)
	if second["status"] != "failed" || second["notify_status"] != "not_attempted" || !strings.Contains(second["error"].(string), "test failure") {
		t.Fatalf("failed batch result = %v", second)
	}
	third := results[2].(map[string]any)
	if third["status"] != "sent" || third["to_address"] != "workflow/three" {
		t.Fatalf("third batch result = %v", third)
	}
}

func TestWaypostSendBatchPluralOneUsesEnvelope(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if params.FromAddress != "agent/sender" || params.ToAddress != "group/review" || !params.Group {
			t.Fatalf("send params = %+v, want explicit sender and group target", params)
		}
		return waypost.SendResult{
			Mode:             waypost.SendModeGroup,
			MessageID:        "msg_review",
			GroupID:          "grp_review",
			GroupAddress:     params.ToAddress,
			MessageCreatedAt: "2026-08-18T00:00:00Z",
		}, nil
	}

	openCount := 0
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService, openCount: &openCount},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	setBoundTestSender(service, "agent/sender")

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":           []string{"group/review"},
		"from_address": "agent/sender",
		"subject":      "single batch item",
		"body":         "body",
		"group":        true,
	})

	if output["status"] != "sent" || output["recipient_count"] != float64(1) || output["sent_count"] != float64(1) || output["failed_count"] != float64(0) {
		t.Fatalf("plural-one batch output = %v, want one successful batch item", output)
	}
	if got := output["to_addresses"]; !reflect.DeepEqual(got, []any{"group/review"}) {
		t.Fatalf("plural-one to_addresses = %#v, want one batch recipient", got)
	}
	results, ok := output["results"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("plural-one results = %#v, want one batch result", output["results"])
	}
	item := results[0].(map[string]any)
	if item["from_address"] != "agent/sender" || item["to_address"] != "group/review" || item["message_id"] != "msg_review" {
		t.Fatalf("plural-one result = %v, want batch receipt", item)
	}
	if openCount != 1 {
		t.Fatalf("waypost service opens = %d, want one per batch", openCount)
	}
}

func TestWaypostSendBatchResolvesEffectiveSenderAndOpensOnce(t *testing.T) {
	// Keep the tool fallback intentionally pending: each sender resolution probes
	// `agent-deck session current`, making an accidental per-recipient resolution
	// observable through the runner call count.
	t.Setenv("AGENTDECK_INSTANCE_ID", "")
	t.Setenv("THURBOX_SESSION", "")

	waypostService := &fakeWaypostService{t: t}
	var sendAddresses []string
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if params.FromAddress != "claude/aaaaaaaaaaaaaaaa" || !params.Group {
			t.Fatalf("send params = %+v, want one resolved sender for every group item", params)
		}
		sendAddresses = append(sendAddresses, params.ToAddress)
		return waypost.SendResult{
			Mode:             waypost.SendModeGroup,
			MessageID:        "msg_" + strings.TrimPrefix(params.ToAddress, "group/"),
			GroupID:          "grp_" + strings.TrimPrefix(params.ToAddress, "group/"),
			GroupAddress:     params.ToAddress,
			MessageCreatedAt: "2026-08-18T00:00:00Z",
		}, nil
	}

	openCount := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		if !reflect.DeepEqual(args, []string{"agent-deck", "session", "current", "--json"}) {
			t.Fatalf("unexpected command call: %v", args)
		}
		return RunResult{ExitCode: 1}, nil
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService, openCount: &openCount},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"claude/aaaaaaaaaaaaaaaa"}
	service.state.defaultSender = "claude/aaaaaaaaaaaaaaaa"
	service.state.autoBindAttempted = true
	service.state.autoBoundToolFallback = true
	service.state.detectedToolSessions = toolSessionIDs{"claude": "aaaaaaaaaaaaaaaa"}

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      []string{"group/one", "group/two"},
		"subject": "resolved sender batch",
		"body":    "body",
		"group":   true,
	})

	if output["status"] != "sent" || output["sent_count"] != float64(2) {
		t.Fatalf("resolved sender batch output = %v, want two sent items", output)
	}
	if want := []string{"group/one", "group/two"}; !reflect.DeepEqual(sendAddresses, want) {
		t.Fatalf("batch send addresses = %v, want %v", sendAddresses, want)
	}
	if openCount != 1 {
		t.Fatalf("waypost service opens = %d, want one per batch", openCount)
	}
	if calls := commandRunner.Calls(); len(calls) != 1 {
		t.Fatalf("sender resolution probes = %v, want one for the entire batch", calls)
	}
}

func TestWaypostSendBatchAllFailedReturnsEnvelope(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	var calls []string
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		calls = append(calls, params.ToAddress)
		return waypost.SendResult{}, errors.New("commit send transaction: test failure")
	}

	openCount := 0
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService, openCount: &openCount},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	setBoundTestSender(service, "agent/sender")

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":                     []string{"workflow/one", "workflow/two"},
		"from_address":           "agent/sender",
		"subject":                "all fail",
		"body":                   "body",
		"disable_notify_message": true,
	})

	if output["status"] != "failed" || output["recipient_count"] != float64(2) || output["sent_count"] != float64(0) || output["failed_count"] != float64(2) {
		t.Fatalf("all-failed batch output = %v, want failed envelope", output)
	}
	if want := []string{"workflow/one", "workflow/two"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("all-failed send calls = %v, want %v", calls, want)
	}
	results, ok := output["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("all-failed results = %#v, want two failed items", output["results"])
	}
	for index, address := range []string{"workflow/one", "workflow/two"} {
		item := results[index].(map[string]any)
		if item["status"] != "failed" || item["from_address"] != "agent/sender" || item["to_address"] != address || item["subject"] != "all fail" || item["notify_status"] != "not_attempted" {
			t.Fatalf("all-failed result %d = %v", index, item)
		}
		if _, ok := item["delivery_id"]; ok {
			t.Fatalf("all-failed result %d unexpectedly contains a receipt: %v", index, item)
		}
	}
	if openCount != 1 {
		t.Fatalf("waypost service opens = %d, want one", openCount)
	}
}

func TestWaypostSendBatchKeepsReceiptsForNotificationOutcomes(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		return waypost.SendResult{DeliveryID: "dlv_" + strings.TrimPrefix(params.ToAddress, "agent-deck/")}, nil
	}
	waypostService.readDeliveriesFunc = func(_ context.Context, deliveryIDs []string) ([]waypost.ReadDelivery, error) {
		if !reflect.DeepEqual(deliveryIDs, []string{"dlv_target"}) {
			t.Fatalf("ReadDeliveries ids = %v, want [dlv_target]", deliveryIDs)
		}
		return []waypost.ReadDelivery{{DeliveryID: "dlv_target", State: "queued"}}, nil
	}

	openCount := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "target", "--json"}):
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			return RunResult{ExitCode: 1, Stderr: "wakeup failed"}, nil
		default:
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService, openCount: &openCount},
		CommandRunner:         commandRunner,
		NotifyDelay:           -1,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.manualBinding = true
	service.state.autoBindAttempted = true
	service.notifications.retryWait = func(context.Context, time.Duration) error { return nil }

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      []string{"agent-deck/target", "agent-deck/self"},
		"subject": "notification outcomes",
		"body":    "body",
	})

	if output["status"] != "sent" || output["sent_count"] != float64(2) || output["failed_count"] != float64(0) {
		t.Fatalf("notification batch output = %v, want durable success", output)
	}
	results, ok := output["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("notification batch results = %#v, want two items", output["results"])
	}
	first := results[0].(map[string]any)
	second := results[1].(map[string]any)
	if first["delivery_id"] != "dlv_target" || first["notify_status"] != "failed" || !strings.Contains(first["notify_error"].(string), "wakeup failed") {
		t.Fatalf("failed notification batch item = %v, want durable receipt and notification failure", first)
	}
	if second["delivery_id"] != "dlv_self" || second["notify_status"] != "skipped_local" {
		t.Fatalf("local notification batch item = %v, want durable receipt and skipped_local", second)
	}
	if openCount != 1 {
		t.Fatalf("waypost service opens = %d, want one", openCount)
	}
}

func TestWaypostSendBatchRetriesNewAgentDeckTargetsBeforeNudging(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	var callMu sync.Mutex
	sendCounts := map[string]int{}
	probeCounts := map[string]int{}
	nudgeCounts := map[string]int{}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		callMu.Lock()
		sendCounts[params.ToAddress]++
		callMu.Unlock()
		return waypost.SendResult{DeliveryID: "dlv_" + strings.TrimPrefix(params.ToAddress, "agent-deck/")}, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case len(args) == 5 && args[0] == "agent-deck" && args[1] == "session" && args[2] == "show" && args[4] == "--json":
			target := args[3]
			if target != "first" && target != "second" {
				t.Fatalf("probe target = %q, want first or second", target)
			}
			callMu.Lock()
			probeCounts[target]++
			attempt := probeCounts[target]
			callMu.Unlock()
			switch attempt {
			case 1:
				return RunResult{ExitCode: 2, Stderr: "not found"}, nil
			case 2:
				return RunResult{ExitCode: 0, Stdout: `{"id":"` + target + `","status":"queued"}`}, nil
			default:
				return RunResult{ExitCode: 0, Stdout: `{"id":"` + target + `","status":"running"}`}, nil
			}
		case isAgentDeckDeferredSend(args):
			target := args[9]
			callMu.Lock()
			nudgeCounts[target]++
			callMu.Unlock()
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		NotifyDelay:           -1,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.manualBinding = true
	service.state.autoBindAttempted = true
	service.notifications.retryWait = func(context.Context, time.Duration) error { return nil }

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      []string{"agent-deck/first", "agent-deck/second"},
		"subject": "new targets",
		"body":    "body",
	})

	if output["status"] != "sent" || output["sent_count"] != float64(2) || output["failed_count"] != float64(0) {
		t.Fatalf("batch output = %v, want durable success", output)
	}
	results, ok := output["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("batch results = %#v, want two items", output["results"])
	}
	for _, raw := range results {
		result := raw.(map[string]any)
		if result["notify_status"] != "sent" {
			t.Fatalf("batch notification result = %v, want sent", result)
		}
	}

	callMu.Lock()
	defer callMu.Unlock()
	for _, target := range []string{"first", "second"} {
		address := "agent-deck/" + target
		if sendCounts[address] != 1 || probeCounts[target] != 3 || nudgeCounts[target] != 1 {
			t.Fatalf("%s counts: sends=%d probes=%d nudges=%d, want 1, 3, 1", target, sendCounts[address], probeCounts[target], nudgeCounts[target])
		}
	}
}

func TestWaypostSendBatchCancellationStopsBeforeNextRecipient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waypostService := &fakeWaypostService{t: t}
	sendCalls := 0
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		sendCalls++
		if params.ToAddress != "group/one" {
			t.Fatalf("send recipient = %q, want only first recipient", params.ToAddress)
		}
		cancel()
		return waypost.SendResult{
			Mode:             waypost.SendModeGroup,
			MessageID:        "msg_one",
			GroupID:          "grp_one",
			GroupAddress:     params.ToAddress,
			MessageCreatedAt: "2026-08-18T00:00:00Z",
		}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	setBoundTestSender(service, "agent/sender")

	output, err := service.sendWaypostBatchMessage(ctx, waypostSendInput{
		ToAddresses: []string{"group/one", "group/two"},
		FromAddress: "agent/sender",
		Subject:     "cancel batch",
		Body:        "body",
		Group:       true,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sendWaypostBatchMessage(canceled) error = %v, want context.Canceled", err)
	}
	if output != nil {
		t.Fatalf("canceled batch output = %v, want no normal envelope", output)
	}
	if sendCalls != 1 {
		t.Fatalf("send calls = %d, want one completed first recipient", sendCalls)
	}
}

func TestWaypostSendBatchRejectsInvalidSelectorsBeforeSending(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		t.Fatalf("unexpected Send call: %+v", params)
		return waypost.SendResult{}, nil
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(_ []string, _ string) (RunResult, error) {
			return RunResult{ExitCode: 1}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()

	for _, arguments := range []map[string]any{
		{
			"to_address": "workflow/one",
			"subject":    "subject",
			"body":       "body",
		},
		{
			"to_addresses": []string{"workflow/two"},
			"subject":      "subject",
			"body":         "body",
		},
		{
			"subject": "subject",
			"body":    "body",
		},
		{
			"to":      makeMCPRecipientAddresses(waypost.MaxSendRecipients + 1),
			"subject": "subject",
			"body":    "body",
		},
		{
			"to":      []string{},
			"subject": "subject",
			"body":    "body",
		},
	} {
		if err := callServiceToolExpectError(t, service, "waypost_send", arguments); err == nil {
			t.Fatalf("waypost_send(%v) error = nil", arguments)
		}
	}
}

func TestWaypostSendBatchGroupUsesSharedGroupMetadata(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	var calls []waypost.SendParams
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		calls = append(calls, params)
		return waypost.SendResult{
			Mode:         waypost.SendModeGroup,
			MessageID:    "msg_" + strings.TrimPrefix(params.ToAddress, "group/"),
			GroupID:      "grp_" + strings.TrimPrefix(params.ToAddress, "group/"),
			GroupAddress: params.ToAddress,
		}, nil
	}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(_ []string, _ string) (RunResult, error) {
			return RunResult{ExitCode: 1}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	setBoundTestSender(service, "agent/sender")

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":           []string{"group/one", "group/two"},
		"from_address": "agent/sender",
		"as_person":    " alice ",
		"subject":      "group batch",
		"body":         "body",
		"group":        true,
	})
	if output["status"] != "sent" {
		t.Fatalf("batch status = %v, want sent", output["status"])
	}
	if len(calls) != 2 {
		t.Fatalf("send calls = %d, want 2", len(calls))
	}
	for _, params := range calls {
		if !params.Group || params.AsPerson != "alice" || params.FromAddress != "agent/sender" {
			t.Fatalf("group batch params = %+v, want shared group metadata", params)
		}
	}
}

func sendParamAddresses(params []waypost.SendParams) []string {
	addresses := make([]string, len(params))
	for index := range params {
		addresses[index] = params[index].ToAddress
	}
	return addresses
}

func makeMCPRecipientAddresses(count int) []string {
	addresses := make([]string, count)
	for index := range addresses {
		addresses[index] = fmt.Sprintf("workflow/recipient-%d", index)
	}
	return addresses
}

func setBoundTestSender(service *Service, address string) {
	service.state.boundAddresses = []string{address}
	service.state.defaultSender = address
	service.state.autoBindAttempted = true
}

func TestCompactWaypostSendResultKeepsOnlyActionableFields(t *testing.T) {
	full := map[string]any{
		"status":             "sent",
		"delivery_id":        "dlv_1",
		"from_address":       "agent/sender",
		"to_address":         "agent/receiver",
		"subject":            "subject",
		"notify_status":      "failed",
		"notify_scheme":      "agent-deck",
		"notify_error":       "wake failed",
		"message_created_at": "2026-08-17T00:00:00Z",
	}
	got := compactWaypostSendResult(full, false)
	want := map[string]any{
		"status":        "sent",
		"delivery_id":   "dlv_1",
		"notify_status": "failed",
		"notify_error":  "wake failed",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compact send result = %v, want %v", got, want)
	}
	if detailed := compactWaypostSendResult(full, true); !reflect.DeepEqual(detailed, full) {
		t.Fatalf("detailed send result = %v, want %v", detailed, full)
	}
}

func TestCompactWaypostClaimHistoryItemSeparatesOperationalAndDiagnosticFields(t *testing.T) {
	lease := activeLease{
		DeliveryID:       "dlv_1",
		RecipientAddress: "agent-deck/self",
		LeaseExpiresAt:   "2026-08-24T12:00:00Z",
		Subject:          "review",
		ContentType:      "text/plain",
		ClaimedAt:        "2026-08-24T11:00:00Z",
		LastRenewedAt:    "2026-08-24T11:30:00Z",
		Status:           "active",
	}
	compact := compactWaypostClaimHistoryItem(lease, false)
	wantCompact := map[string]any{
		"delivery_id":       "dlv_1",
		"recipient_address": "agent-deck/self",
		"subject":           "review",
		"status":            "active",
		"lease_expires_at":  "2026-08-24T12:00:00Z",
	}
	if !reflect.DeepEqual(compact, wantCompact) {
		t.Fatalf("compact claim history item = %v, want %v", compact, wantCompact)
	}

	detailed := compactWaypostClaimHistoryItem(lease, true)
	wantDetailed := map[string]any{
		"delivery_id":       "dlv_1",
		"recipient_address": "agent-deck/self",
		"subject":           "review",
		"status":            "active",
		"lease_expires_at":  "2026-08-24T12:00:00Z",
		"content_type":      "text/plain",
		"claimed_at":        "2026-08-24T11:00:00Z",
		"last_renewed_at":   "2026-08-24T11:30:00Z",
	}
	if !reflect.DeepEqual(detailed, wantDetailed) {
		t.Fatalf("diagnostic claim history item = %v, want %v", detailed, wantDetailed)
	}
}

func TestCompactWaypostClaimHistoryItemUsesSparseTerminalFields(t *testing.T) {
	item := compactWaypostClaimHistoryItem(activeLease{
		DeliveryID:       "dlv_1",
		RecipientAddress: "agent-deck/self",
		LeaseExpiresAt:   "2026-08-24T12:00:00Z",
		Subject:          "review",
		Status:           "acked",
		TerminalAt:       "2026-08-24T11:45:00Z",
	}, false)
	want := map[string]any{
		"delivery_id":       "dlv_1",
		"recipient_address": "agent-deck/self",
		"subject":           "review",
		"status":            "acked",
		"terminal_at":       "2026-08-24T11:45:00Z",
	}
	if !reflect.DeepEqual(item, want) {
		t.Fatalf("terminal claim history item = %v, want %v", item, want)
	}
}

func TestCompactWaypostGroupReceivedMessageOmitsReadMetadata(t *testing.T) {
	message := waypost.GroupReceivedMessage{
		MessageID:        "msg_1",
		GroupID:          "grp_1",
		GroupAddress:     "group/review",
		Person:           "alice",
		Subject:          "review",
		ContentType:      "text/plain",
		Body:             "body",
		ReadCount:        2,
		EligibleCount:    3,
		FirstReadAt:      "2026-08-17T00:00:00Z",
		MessageCreatedAt: "2026-08-16T00:00:00Z",
	}
	got := compactWaypostGroupReceivedMessage(message, false)
	want := map[string]any{
		"message_id":   "msg_1",
		"subject":      "review",
		"content_type": "text/plain",
		"body":         "body",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compact group message = %v, want %v", got, want)
	}
	if detailed := compactWaypostGroupReceivedMessage(message, true); reflect.DeepEqual(detailed, got) {
		t.Fatalf("detailed group message unexpectedly equals compact result: %v", detailed)
	}
}

func TestWaypostRecvSchemaOmitsTimeout(t *testing.T) {
	schema := waypostRecvInputSchema()
	if _, ok := schema.Properties["timeout"]; ok {
		t.Fatalf("schema.Properties unexpectedly includes timeout: %v", schema.Properties)
	}
	if _, ok := schema.Properties["known_delivery_ids"]; !ok {
		t.Fatalf("schema.Properties missing known_delivery_ids: %v", schema.Properties)
	}
	if _, ok := schema.Properties["active_lease_cursor"]; !ok {
		t.Fatalf("schema.Properties missing active_lease_cursor: %v", schema.Properties)
	}
	diagnostics, ok := schema.Properties["diagnostics"]
	if !ok {
		t.Fatalf("schema.Properties missing diagnostics: %v", schema.Properties)
	}
	if _, ok := schema.Properties["include_details"]; ok {
		t.Fatalf("schema.Properties unexpectedly includes include_details: %v", schema.Properties)
	}
	if got, want := diagnostics.Description, "Unnecessary for normal receive or sender verification."; got != want {
		t.Fatalf("diagnostics description = %q, want %q", got, want)
	}
}

func TestWaypostStatusSchemaExposesOptionalDetailControls(t *testing.T) {
	schema, err := jsonschema.For[waypostStatusInput](nil)
	if err != nil {
		t.Fatalf("jsonschema.For() error = %v", err)
	}
	for _, field := range []string{"include_diagnostics", "include_cli_context", "include_active_leases"} {
		if _, ok := schema.Properties[field]; !ok {
			t.Fatalf("schema.Properties missing %q: %v", field, schema.Properties)
		}
		if slices.Contains(schema.Required, field) {
			t.Fatalf("required fields = %v, do not want %q", schema.Required, field)
		}
	}
}

func TestActiveLeaseHintIsPaginatedWithoutLosingTotalOrAddressOrder(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.autoBindAttempted = true
	messages := make([]waypost.ReceivedMessage, 0, waypost.MaxPageSize+1)
	for index := 0; index < waypost.MaxPageSize+1; index++ {
		recipient := "agent-deck/one"
		if index%2 == 1 {
			recipient = "agent-deck/two"
		}
		messages = append(messages, waypost.ReceivedMessage{
			DeliveryID:       fmt.Sprintf("dlv_%03d", index),
			LeaseToken:       fmt.Sprintf("lease_%03d", index),
			RecipientAddress: recipient,
		})
	}
	service.activeLeases.trackReceive(waypost.ReceiveResult{Messages: messages}, time.Now().UTC().Format(time.RFC3339Nano))

	first, err := service.activeLeaseHintPage([]string{"agent-deck/one", "agent-deck/two"}, nil, "")
	if err != nil {
		t.Fatalf("activeLeaseHintPage(first) error = %v", err)
	}
	if first.Total != waypost.MaxPageSize+1 || len(first.DeliveryIDs) != waypost.MaxPageSize || first.NextCursor == "" {
		t.Fatalf("activeLeaseHintPage(first) = %+v", first)
	}
	second, err := service.activeLeaseHintPage([]string{"agent-deck/two", "agent-deck/one"}, nil, first.NextCursor)
	if err != nil {
		t.Fatalf("activeLeaseHintPage(second) error = %v", err)
	}
	if second.Total != waypost.MaxPageSize+1 || len(second.DeliveryIDs) != 1 || second.NextCursor != "" {
		t.Fatalf("activeLeaseHintPage(second) = %+v", second)
	}
}

func TestWaypostRecvReturnsPaginatedActiveLeaseHint(t *testing.T) {
	fake := &fakeWaypostService{t: t}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: fake},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.autoBindAttempted = true
	messages := make([]waypost.ReceivedMessage, 0, waypost.MaxPageSize+1)
	for index := 0; index < waypost.MaxPageSize+1; index++ {
		messages = append(messages, waypost.ReceivedMessage{
			DeliveryID:       fmt.Sprintf("dlv_%03d", index),
			LeaseToken:       fmt.Sprintf("lease_%03d", index),
			RecipientAddress: "agent-deck/self",
		})
	}
	fake.recordLeases(messages)
	service.activeLeases.trackReceive(waypost.ReceiveResult{Messages: messages}, time.Now().UTC().Format(time.RFC3339Nano))

	first := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if first["status"] != "active_leases" || first["active_lease_count"] != float64(waypost.MaxPageSize+1) {
		t.Fatalf("first active lease response = %+v", first)
	}
	if ids := first["claimed_delivery_ids"].([]any); len(ids) != waypost.MaxPageSize {
		t.Fatalf("first claimed_delivery_ids length = %d, want %d", len(ids), waypost.MaxPageSize)
	}
	cursor, ok := first["next_cursor"].(string)
	if !ok || cursor == "" {
		t.Fatalf("first next_cursor = %v, want non-empty", first["next_cursor"])
	}

	second := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses":           []string{"agent-deck/self"},
		"active_lease_cursor": cursor,
	})
	if second["active_lease_count"] != float64(waypost.MaxPageSize+1) {
		t.Fatalf("second active lease response = %+v", second)
	}
	if _, ok := second["next_cursor"]; ok {
		t.Fatalf("second response unexpectedly has next_cursor: %+v", second)
	}
}

func TestWaypostClaimHistorySchemaExposesRecoveryFields(t *testing.T) {
	schema := waypostClaimHistoryInputSchema()
	for _, field := range []string{"delivery_id", "include_terminal", "recover_lease_token", "diagnostics"} {
		if _, ok := schema.Properties[field]; !ok {
			t.Fatalf("schema.Properties missing %s: %v", field, schema.Properties)
		}
	}
	if got, want := schema.Properties["diagnostics"].Description, "Unnecessary for normal claim listing or lease-token recovery."; got != want {
		t.Fatalf("diagnostics description = %q, want %q", got, want)
	}
	if _, ok := schema.Properties["include_lease_token"]; ok {
		t.Fatalf("schema.Properties unexpectedly includes include_lease_token: %v", schema.Properties)
	}
}

func TestWaypostClaimHistoryReturnsCompactPaginatedResults(t *testing.T) {
	fake := &fakeWaypostService{t: t}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: fake},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.autoBindAttempted = true
	messages := make([]waypost.ReceivedMessage, 0, waypost.DefaultPageSize+1)
	for index := 0; index < waypost.DefaultPageSize+1; index++ {
		messages = append(messages, waypost.ReceivedMessage{
			DeliveryID:       fmt.Sprintf("dlv_history_%03d", index),
			LeaseToken:       fmt.Sprintf("lease_history_%03d", index),
			RecipientAddress: "agent-deck/self",
			LeaseExpiresAt:   "2026-08-24T12:00:00Z",
			Subject:          "history subject",
		})
	}
	fake.recordLeases(messages)
	service.activeLeases.trackReceive(waypost.ReceiveResult{Messages: messages}, time.Now().UTC().Format(time.RFC3339Nano))

	first := callServiceTool(t, service, "waypost_claim_history", map[string]any{})
	if len(first) != 3 || first["status"] != "listed" {
		t.Fatalf("first compact claim history = %v, want status, items, and next_cursor only", first)
	}
	items := first["items"].([]any)
	if len(items) != waypost.DefaultPageSize {
		t.Fatalf("first claim history items = %d, want %d", len(items), waypost.DefaultPageSize)
	}
	firstItem := items[0].(map[string]any)
	if len(firstItem) != 5 || firstItem["delivery_id"] != "dlv_history_000" || firstItem["status"] != "active" {
		t.Fatalf("first compact claim history item = %v", firstItem)
	}
	for _, field := range []string{"content_type", "claimed_at", "last_renewed_at", "terminal_at", "lease_token"} {
		if _, ok := firstItem[field]; ok {
			t.Fatalf("compact claim history item unexpectedly includes %q: %v", field, firstItem)
		}
	}
	cursor, ok := first["next_cursor"].(string)
	if !ok || cursor == "" {
		t.Fatalf("first claim history next_cursor = %v", first["next_cursor"])
	}
	second := callServiceTool(t, service, "waypost_claim_history", map[string]any{"cursor": cursor})
	if len(second) != 2 || second["status"] != "listed" {
		t.Fatalf("second compact claim history = %v, want status and items only", second)
	}
	if items := second["items"].([]any); len(items) != 1 {
		t.Fatalf("second claim history items = %d, want 1", len(items))
	}
}

func TestWaypostClaimHistoryEmptyAndNotFoundResultsAreCompact(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.autoBindAttempted = true

	empty := callServiceTool(t, service, "waypost_claim_history", map[string]any{})
	if want := map[string]any{"status": "listed", "items": []any{}}; !reflect.DeepEqual(empty, want) {
		t.Fatalf("empty claim history = %#v, want %#v", empty, want)
	}
	notFound := callServiceTool(t, service, "waypost_claim_history", map[string]any{"delivery_id": "dlv_missing"})
	if want := map[string]any{"status": "not_found", "delivery_id": "dlv_missing"}; !reflect.DeepEqual(notFound, want) {
		t.Fatalf("missing claim history = %#v, want %#v", notFound, want)
	}
}

func TestWaypostGroupAddSubscriberSchemaRequiresPerson(t *testing.T) {
	schema, err := jsonschema.For[waypostGroupSubscriberInput](nil)
	if err != nil {
		t.Fatalf("jsonschema.For() error = %v", err)
	}
	if !slices.Contains(schema.Required, "person") {
		t.Fatalf("required fields = %v, want person", schema.Required)
	}
}

func TestWaypostGroupRemoveSubscriberSchemaOmitsPerson(t *testing.T) {
	schema, err := jsonschema.For[waypostGroupSubscriberRemoveInput](nil)
	if err != nil {
		t.Fatalf("jsonschema.For() error = %v", err)
	}
	if _, ok := schema.Properties["person"]; ok {
		t.Fatalf("schema.Properties unexpectedly includes person: %v", schema.Properties)
	}
}

func TestWaypostUndeferSchemaExposesDeliveryID(t *testing.T) {
	schema, err := jsonschema.For[waypostUndeferInput](nil)
	if err != nil {
		t.Fatalf("jsonschema.For() error = %v", err)
	}
	if _, ok := schema.Properties["delivery_id"]; !ok {
		t.Fatalf("schema.Properties missing delivery_id: %v", schema.Properties)
	}
}

func (f *fakeWaypostService) Send(ctx context.Context, params waypost.SendParams) (waypost.SendResult, error) {
	if f.sendFunc == nil {
		f.t.Fatalf("unexpected Send call: %+v", params)
	}
	return f.sendFunc(ctx, params)
}

func (f *fakeWaypostService) List(ctx context.Context, params waypost.ListParams) ([]waypost.ListedDelivery, error) {
	if f.listFunc == nil {
		return []waypost.ListedDelivery{}, nil
	}
	return f.listFunc(ctx, params)
}

func (f *fakeWaypostService) ListGroupMessages(ctx context.Context, params waypost.GroupListParams) ([]waypost.GroupListedMessage, error) {
	if f.listGroupMessagesFunc == nil {
		f.t.Fatalf("unexpected ListGroupMessages call: %+v", params)
	}
	return f.listGroupMessagesFunc(ctx, params)
}

func (f *fakeWaypostService) WaitGroupMessage(ctx context.Context, params waypost.GroupWaitParams) (waypost.GroupListedMessage, error) {
	if f.waitGroupMessageFunc == nil {
		f.t.Fatalf("unexpected WaitGroupMessage call: %+v", params)
	}
	return f.waitGroupMessageFunc(ctx, params)
}

func (f *fakeWaypostService) ReceiveGroupMessage(ctx context.Context, params waypost.GroupReceiveParams) (waypost.GroupReceivedMessage, error) {
	if f.receiveGroupMessageFunc == nil {
		f.t.Fatalf("unexpected ReceiveGroupMessage call: %+v", params)
	}
	return f.receiveGroupMessageFunc(ctx, params)
}

func (f *fakeWaypostService) CreateGroup(ctx context.Context, groupAddress string) (waypost.GroupRecord, error) {
	if f.createGroupFunc == nil {
		f.t.Fatalf("unexpected CreateGroup call: %q", groupAddress)
	}
	return f.createGroupFunc(ctx, groupAddress)
}

func (f *fakeWaypostService) AddGroupMember(ctx context.Context, groupAddress, person string) (waypost.GroupMembershipRecord, error) {
	if f.addGroupMemberFunc == nil {
		f.t.Fatalf("unexpected AddGroupMember call: group=%q person=%q", groupAddress, person)
	}
	return f.addGroupMemberFunc(ctx, groupAddress, person)
}

func (f *fakeWaypostService) RemoveGroupMember(ctx context.Context, groupAddress, person string) (waypost.GroupMembershipRecord, error) {
	if f.removeGroupMemberFunc == nil {
		f.t.Fatalf("unexpected RemoveGroupMember call: group=%q person=%q", groupAddress, person)
	}
	return f.removeGroupMemberFunc(ctx, groupAddress, person)
}

func (f *fakeWaypostService) ListGroupMembers(ctx context.Context, groupAddress string) ([]waypost.GroupMembershipRecord, error) {
	if f.listGroupMembersFunc == nil {
		f.t.Fatalf("unexpected ListGroupMembers call: %q", groupAddress)
	}
	return f.listGroupMembersFunc(ctx, groupAddress)
}

func (f *fakeWaypostService) AddGroupNotificationSubscriber(ctx context.Context, groupAddress, notifyAddress, person string) (waypost.GroupNotificationSubscriberRecord, error) {
	if f.addGroupSubscriberFunc == nil {
		f.t.Fatalf("unexpected AddGroupNotificationSubscriber call: group=%q notify=%q person=%q", groupAddress, notifyAddress, person)
	}
	return f.addGroupSubscriberFunc(ctx, groupAddress, notifyAddress, person)
}

func (f *fakeWaypostService) RemoveGroupNotificationSubscriber(ctx context.Context, groupAddress, notifyAddress string) (waypost.GroupNotificationSubscriberRecord, error) {
	if f.removeGroupSubscriberFunc == nil {
		f.t.Fatalf("unexpected RemoveGroupNotificationSubscriber call: group=%q notify=%q", groupAddress, notifyAddress)
	}
	return f.removeGroupSubscriberFunc(ctx, groupAddress, notifyAddress)
}

func (f *fakeWaypostService) ListGroupNotificationSubscribers(ctx context.Context, groupAddress string) ([]waypost.GroupNotificationSubscriberRecord, error) {
	if f.listGroupSubscribersFunc == nil {
		return nil, nil
	}
	return f.listGroupSubscribersFunc(ctx, groupAddress)
}

func (f *fakeWaypostService) InspectAddress(ctx context.Context, address string) (waypost.AddressInspection, error) {
	if f.inspectAddressFunc == nil {
		f.t.Fatalf("unexpected InspectAddress call: %q", address)
	}
	return f.inspectAddressFunc(ctx, address)
}

func (f *fakeWaypostService) ListClaimableAddresses(ctx context.Context, addresses []string) ([]waypost.ClaimableAddress, error) {
	if f.listClaimableFunc == nil {
		return []waypost.ClaimableAddress{}, nil
	}
	return f.listClaimableFunc(ctx, addresses)
}

func (f *fakeWaypostService) ReceiveBatchWithLeaseTTL(ctx context.Context, params waypost.ReceiveBatchParams, ttl time.Duration) (waypost.ReceiveResult, error) {
	var result waypost.ReceiveResult
	var err error
	if f.receiveBatchWithTTLFunc != nil {
		result, err = f.receiveBatchWithTTLFunc(ctx, params, ttl)
	} else {
		if f.receiveBatchFunc == nil {
			f.t.Fatalf("unexpected ReceiveBatchWithLeaseTTL call: %+v ttl=%s", params, ttl)
		}
		result, err = f.receiveBatchFunc(ctx, params)
	}
	if err == nil {
		f.recordLeases(result.Messages)
	}
	return result, err
}

func (f *fakeWaypostService) Wait(ctx context.Context, params waypost.WaitParams) (waypost.ListedDelivery, error) {
	if f.waitFunc == nil {
		f.t.Fatalf("unexpected Wait call: %+v", params)
	}
	return f.waitFunc(ctx, params)
}

func (f *fakeWaypostService) ReadMessages(ctx context.Context, messageIDs []string) ([]waypost.ReadMessage, error) {
	if f.readMessagesFunc == nil {
		f.t.Fatalf("unexpected ReadMessages call: %v", messageIDs)
	}
	return f.readMessagesFunc(ctx, messageIDs)
}

func (f *fakeWaypostService) ReadLatestDeliveries(ctx context.Context, addresses []string, state string, limit int) ([]waypost.ReadDelivery, bool, error) {
	if f.readLatestFunc == nil {
		f.t.Fatalf("unexpected ReadLatestDeliveries call: addresses=%v state=%q limit=%d", addresses, state, limit)
	}
	return f.readLatestFunc(ctx, addresses, state, limit)
}

func (f *fakeWaypostService) ReadDeliveries(ctx context.Context, deliveryIDs []string) ([]waypost.ReadDelivery, error) {
	if f.readDeliveriesFunc == nil {
		deliveries := make([]waypost.ReadDelivery, 0, len(deliveryIDs))
		for _, deliveryID := range deliveryIDs {
			deliveries = append(deliveries, waypost.ReadDelivery{
				DeliveryID: deliveryID,
				State:      "queued",
			})
		}
		return deliveries, nil
	}
	return f.readDeliveriesFunc(ctx, deliveryIDs)
}

func (f *fakeWaypostService) Ack(ctx context.Context, deliveryID, leaseToken string) (waypost.DeliveryTransitionResult, error) {
	if f.ackFunc == nil {
		f.t.Fatalf("unexpected Ack call: delivery=%q lease=%q", deliveryID, leaseToken)
	}
	return f.ackFunc(ctx, deliveryID, leaseToken)
}

func (f *fakeWaypostService) Renew(ctx context.Context, deliveryID, leaseToken string, extendBy time.Duration) (waypost.LeaseRenewResult, error) {
	if f.renewFunc == nil {
		f.t.Fatalf("unexpected Renew call: delivery=%q lease=%q extendBy=%s", deliveryID, leaseToken, extendBy)
	}
	result, err := f.renewFunc(ctx, deliveryID, leaseToken, extendBy)
	if err == nil {
		f.recordLeases([]waypost.ReceivedMessage{{
			DeliveryID:     result.DeliveryID,
			LeaseToken:     result.LeaseToken,
			LeaseExpiresAt: result.LeaseExpiresAt,
		}})
	}
	return result, err
}

func (f *fakeWaypostService) RemainingByState(ctx context.Context, addresses, excludedDeliveryIDs []string) (map[string]int, error) {
	if f.remainingByStateFunc != nil {
		return f.remainingByStateFunc(ctx, addresses, excludedDeliveryIDs)
	}
	return nil, nil
}

func (f *fakeWaypostService) InspectDeliveryLease(ctx context.Context, deliveryID string) (waypost.DeliveryLeaseState, error) {
	if f.inspectLeaseFunc != nil {
		return f.inspectLeaseFunc(ctx, deliveryID)
	}
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	return f.leaseStates[deliveryID], nil
}

func (f *fakeWaypostService) recordLeases(messages []waypost.ReceivedMessage) {
	f.leaseMu.Lock()
	defer f.leaseMu.Unlock()
	if f.leaseStates == nil {
		f.leaseStates = make(map[string]waypost.DeliveryLeaseState)
	}
	for _, message := range messages {
		if message.DeliveryID == "" {
			continue
		}
		state := f.leaseStates[message.DeliveryID]
		if state.State == "" {
			state.State = "leased"
			state.Found = true
		}
		if message.LeaseToken != "" {
			state.LeaseToken = message.LeaseToken
		}
		f.leaseStates[message.DeliveryID] = state
	}
}

func (f *fakeWaypostService) Release(ctx context.Context, deliveryID, leaseToken string) (waypost.DeliveryTransitionResult, error) {
	if f.releaseFunc == nil {
		f.t.Fatalf("unexpected Release call: delivery=%q lease=%q", deliveryID, leaseToken)
	}
	return f.releaseFunc(ctx, deliveryID, leaseToken)
}

func (f *fakeWaypostService) Defer(ctx context.Context, deliveryID, leaseToken string, until time.Time) (waypost.DeliveryTransitionResult, error) {
	if f.deferFunc == nil {
		f.t.Fatalf("unexpected Defer call: delivery=%q lease=%q until=%s", deliveryID, leaseToken, until)
	}
	return f.deferFunc(ctx, deliveryID, leaseToken, until)
}

func (f *fakeWaypostService) Undefer(ctx context.Context, deliveryID string) (waypost.DeliveryTransitionResult, error) {
	if f.undeferFunc == nil {
		f.t.Fatalf("unexpected Undefer call: delivery=%q", deliveryID)
	}
	return f.undeferFunc(ctx, deliveryID)
}

func (f *fakeWaypostService) Fail(ctx context.Context, deliveryID, leaseToken, reason string) (waypost.DeliveryTransitionResult, error) {
	if f.failFunc == nil {
		f.t.Fatalf("unexpected Fail call: delivery=%q lease=%q reason=%q", deliveryID, leaseToken, reason)
	}
	return f.failFunc(ctx, deliveryID, leaseToken, reason)
}

type fakeWaypostServiceFactory struct {
	service   any
	openCount *int
}

func (f fakeWaypostServiceFactory) Open(context.Context) (any, func() error, error) {
	if f.openCount != nil {
		(*f.openCount)++
	}
	return f.service, func() error { return nil }, nil
}

type failOpenWaypostServiceFactory struct {
	t *testing.T
}

func (f failOpenWaypostServiceFactory) Open(context.Context) (any, func() error, error) {
	f.t.Fatal("unexpected waypost service open")
	return nil, nil, nil
}

func TestDefaultWaypostServiceReusesRuntimeUntilServiceClose(t *testing.T) {
	service := newService(Options{
		StateDir:              filepath.Join(t.TempDir(), "waypost-state"),
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	factory := service.waypostServices.(*runtimeWaypostServiceFactory)
	closeCount := 0
	factory.closeRuntime = func(runtime *waypost.Runtime) error {
		closeCount++
		return runtime.Close()
	}

	first, err := withWaypostService[string, *waypost.Operations](context.Background(), service.waypostServices, func(ops *waypost.Operations) (string, error) {
		return fmt.Sprintf("%p", ops), nil
	})
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	second, err := withWaypostService[string, *waypost.Operations](context.Background(), service.waypostServices, func(ops *waypost.Operations) (string, error) {
		return fmt.Sprintf("%p", ops), nil
	})
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	if first != second {
		t.Fatalf("waypost service pointer changed: first=%s second=%s", first, second)
	}
	if closeCount != 0 {
		t.Fatalf("runtime closes before Service.Close = %d, want 0", closeCount)
	}

	service.Close()
	service.Close()

	if closeCount != 1 {
		t.Fatalf("runtime closes = %d, want 1", closeCount)
	}
	_, err = withWaypostService[string, *waypost.Operations](context.Background(), service.waypostServices, func(ops *waypost.Operations) (string, error) {
		return fmt.Sprintf("%p", ops), nil
	})
	if err == nil || !strings.Contains(err.Error(), "waypost runtime is closed") {
		t.Fatalf("Open() after Service.Close error = %v, want closed runtime error", err)
	}
}

func TestLegacyNewServerCanCloseService(t *testing.T) {
	server := New(Options{
		StateDir:              filepath.Join(t.TempDir(), "waypost-state"),
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	value, ok := legacyServerServices.Load(server)
	if !ok {
		t.Fatal("legacy server service was not registered")
	}
	service := value.(*Service)
	factory := service.waypostServices.(*runtimeWaypostServiceFactory)
	closeCount := 0
	factory.closeRuntime = func(runtime *waypost.Runtime) error {
		closeCount++
		return runtime.Close()
	}

	if _, err := withWaypostService[string, *waypost.Operations](context.Background(), service.waypostServices, func(ops *waypost.Operations) (string, error) {
		return fmt.Sprintf("%p", ops), nil
	}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	CloseServer(server)
	CloseServer(server)

	if closeCount != 1 {
		t.Fatalf("runtime closes = %d, want 1", closeCount)
	}
	if _, ok := legacyServerServices.Load(server); ok {
		t.Fatal("legacy server service still registered after CloseServer")
	}
	_, err := withWaypostService[string, *waypost.Operations](context.Background(), service.waypostServices, func(ops *waypost.Operations) (string, error) {
		return fmt.Sprintf("%p", ops), nil
	})
	if err == nil || !strings.Contains(err.Error(), "waypost runtime is closed") {
		t.Fatalf("Open() after CloseServer error = %v, want closed runtime error", err)
	}
}

type fakeRunner struct {
	t          *testing.T
	handler    func(args []string, input string) (RunResult, error)
	ctxHandler func(context.Context, []string, string) (RunResult, error)

	mu    sync.Mutex
	calls []runnerCall
}

type runnerCall struct {
	Args  []string
	Input string
}

func waitForTestSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func (r *fakeRunner) Run(ctx context.Context, args []string, input string) (RunResult, error) {
	r.mu.Lock()
	r.calls = append(r.calls, runnerCall{Args: append([]string(nil), args...), Input: input})
	r.mu.Unlock()

	type result struct {
		runResult RunResult
		err       error
		completed bool
	}
	resultCh := make(chan result, 1)
	go func() {
		out := result{}
		defer func() {
			resultCh <- out
		}()
		if r.ctxHandler != nil {
			out.runResult, out.err = r.ctxHandler(ctx, args, input)
		} else {
			out.runResult, out.err = r.handler(args, input)
		}
		out.completed = true
	}()
	out := <-resultCh
	if !out.completed {
		return RunResult{}, fmt.Errorf("fake command handler stopped before returning for args: %v", args)
	}
	return out.runResult, out.err
}

func (r *fakeRunner) Calls() []runnerCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runnerCall(nil), r.calls...)
}

func canonicalTestWorkdir(t *testing.T, path string) string {
	t.Helper()
	workdir, err := canonicalizeExistingPath(path)
	if err != nil {
		t.Fatalf("canonicalize test workdir %q: %v", path, err)
	}
	return workdir
}

func agentDeckDeferredSendArgs(target, message string) []string {
	return []string{
		"agent-deck", "session", "send",
		"--json",
		"-defer-if-busy",
		"-defer-timeout", agentDeckNotifyDeferTimeout.String(),
		"-timeout", agentDeckNotifyReadyTimeout.String(),
		target, message,
	}
}

func isAgentDeckDeferredSend(args []string) bool {
	return len(args) == 11 &&
		args[0] == "agent-deck" &&
		args[1] == "session" &&
		args[2] == "send" &&
		args[3] == "--json" &&
		args[4] == "-defer-if-busy" &&
		args[5] == "-defer-timeout" &&
		args[6] == agentDeckNotifyDeferTimeout.String() &&
		args[7] == "-timeout" &&
		args[8] == agentDeckNotifyReadyTimeout.String()
}

func TestAgentDeckNudgeStructuredResultMapping(t *testing.T) {
	tests := []struct {
		name       string
		result     RunResult
		wantStatus string
		wantDetail bool
		wantError  bool
	}{
		{name: "submitted boolean", result: RunResult{ExitCode: 0, Stdout: `{"success":true,"delivery":"submitted","submitted":true}`}, wantStatus: "sent"},
		{name: "typed", result: RunResult{ExitCode: 1, Stdout: `{"success":false,"delivery":"typed","submitted":false,"error":"not confirmed","code":"DELIVERY_FAILED"}`}, wantStatus: "unconfirmed", wantDetail: true},
		{name: "unverified", result: RunResult{ExitCode: 0, Stdout: `{"success":true,"delivery":"unverified","submitted":false}`}, wantStatus: "unconfirmed", wantDetail: true},
		{name: "no evidence", result: RunResult{ExitCode: 1, Stdout: `{"success":false,"delivery":"no_evidence","submitted":false,"error":"no evidence","code":"DELIVERY_FAILED"}`}, wantStatus: "unconfirmed", wantDetail: true},
		{name: "typed not submitted", result: RunResult{ExitCode: 1, Stdout: `{"success":false,"delivery":"typed_not_submitted","submitted":false,"error":"not submitted","code":"DELIVERY_FAILED"}`}, wantStatus: "failed", wantError: true},
		{name: "line too long", result: RunResult{ExitCode: 1, Stdout: `{"success":false,"delivery":"line_too_long","submitted":false,"error":"too long","code":"DELIVERY_FAILED"}`}, wantStatus: "failed", wantError: true},
		{name: "send failed", result: RunResult{ExitCode: 1, Stdout: `{"success":false,"delivery":"send_failed","submitted":false,"error":"send failed","code":"INVALID_OPERATION"}`}, wantStatus: "failed", wantError: true},
		{name: "legacy exit zero", result: RunResult{ExitCode: 0, Stdout: "Sent message"}, wantStatus: "sent"},
		{name: "legacy nonzero", result: RunResult{ExitCode: 1, Stdout: "", Stderr: "legacy failure"}, wantStatus: "failed", wantError: true},
		{name: "structured pre-send failure", result: RunResult{ExitCode: 2, Stdout: `{"success":false,"error":"session not found","code":"NOT_FOUND"}`}, wantStatus: "failed", wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				if !slices.Contains(args, "--json") {
					t.Fatalf("agent-deck nudge args = %v, want --json", args)
				}
				return test.result, nil
			}}
			outcome := (agentDeckNotifier{runner: runner}).runNudge(context.Background(), agentDeckDeferredSendArgs("target", defaultNotifyMessage))
			if outcome.Status != test.wantStatus {
				t.Fatalf("status = %q, want %q", outcome.Status, test.wantStatus)
			}
			if (outcome.Detail != "") != test.wantDetail {
				t.Fatalf("detail = %q, want present=%v", outcome.Detail, test.wantDetail)
			}
			if (outcome.Err != nil) != test.wantError {
				t.Fatalf("error = %v, want present=%v", outcome.Err, test.wantError)
			}
		})
	}
}

func TestAgentDeckNudgeDoesNotRunWithPreCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		t.Fatalf("runner called with pre-canceled context: %v", args)
		return RunResult{}, nil
	}}

	outcome := (agentDeckNotifier{runner: runner}).runNudge(ctx, agentDeckDeferredSendArgs("target", defaultNotifyMessage))
	if outcome.Status != "failed" || !errors.Is(outcome.Err, context.Canceled) {
		t.Fatalf("outcome = %+v, want failed context cancellation before attempt", outcome)
	}
}

func TestAgentDeckNudgeCancellationAfterRunnerCallIsUnconfirmed(t *testing.T) {
	called := false
	runner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		called = true
		return RunResult{}, context.Canceled
	}}

	outcome := (agentDeckNotifier{runner: runner}).runNudge(context.Background(), agentDeckDeferredSendArgs("target", defaultNotifyMessage))
	if !called || outcome.Status != "unconfirmed" || outcome.Err != nil {
		t.Fatalf("called = %v, outcome = %+v; want attempted unconfirmed cancellation", called, outcome)
	}
}

func TestResolveWakeNotifyMessageUsesFixedWakeText(t *testing.T) {
	if got := resolveWakeNotifyMessage(nil, defaultNotifyMessage); got != defaultNotifyMessage {
		t.Fatalf("resolveWakeNotifyMessage(nil) = %q, want %q", got, defaultNotifyMessage)
	}
	if !strings.Contains(defaultNotifyMessage, "NOTICE:") {
		t.Fatalf("defaultNotifyMessage = %q, want notification-only notice", defaultNotifyMessage)
	}
	if strings.Contains(defaultNotifyMessage, "check-agent-delivery") || strings.Contains(defaultNotifyMessage, "requested action") {
		t.Fatalf("defaultNotifyMessage = %q, want no workflow action instruction", defaultNotifyMessage)
	}

	disabled := true
	if got := resolveWakeNotifyMessage(&disabled, defaultNotifyMessage); got != "" {
		t.Fatalf("resolveWakeNotifyMessage(true) = %q, want empty", got)
	}

	enabled := false
	if got := resolveWakeNotifyMessage(&enabled, defaultNotifyMessage); got != defaultNotifyMessage {
		t.Fatalf("resolveWakeNotifyMessage(false) = %q, want %q", got, defaultNotifyMessage)
	}
}

func TestWaypostSendNotifiesWorkerTarget(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if params.ToAddress != "agent-deck/target" || params.FromAddress != "agent-deck/self" || params.Subject != "delegate" {
			t.Fatalf("send params = %+v", params)
		}
		if string(params.Body) != "body" {
			t.Fatalf("send body = %q, want body", string(params.Body))
		}
		return waypost.SendResult{DeliveryID: "dlv_1"}, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "target", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","title":"coder-123","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			if args[9] != "target" {
				t.Fatalf("notify target = %q, want target", args[9])
			}
			if args[10] != defaultNotifyMessage {
				t.Fatalf("notify message = %q, want fixed default", args[10])
			}
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		NotifyDelay:           -1,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":          "agent-deck/target",
		"subject":     "delegate",
		"body":        "body",
		"diagnostics": true,
	})

	if got := output["delivery_id"]; got != "dlv_1" {
		t.Fatalf("delivery_id = %v, want dlv_1", got)
	}
	if got := output["notify_status"]; got != "sent" {
		t.Fatalf("notify_status = %v, want sent", got)
	}
	if got := output["notify_scheme"]; got != "agent-deck" {
		t.Fatalf("notify_scheme = %v, want agent-deck", got)
	}
	if got := output["notify_error"]; got != nil {
		t.Fatalf("notify_error = %v, want nil", got)
	}
}

func TestAgentDeckNotifyDefersWhenTargetIsRunning(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "target", "--json"}):
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","status":"running"}`}, nil
		case reflect.DeepEqual(args, agentDeckDeferredSendArgs("target", defaultNotifyMessage)):
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))

	outcome := manager.notifyRouteWithRetry(context.Background(), notificationEvent{
		Kind: notificationDelivery,
		Route: notificationRoute{
			Manager: "agent-deck",
			Target:  "target",
		},
	})

	if outcome.Status != "sent" || outcome.Scheme != "agent-deck" || outcome.Err != nil {
		t.Fatalf("outcome = %+v, want sent agent-deck notification", outcome)
	}
	if calls := commandRunner.Calls(); len(calls) != 2 {
		t.Fatalf("command calls = %v, want probe + deferred send", calls)
	}
}

func TestAgentDeckNotifyBoundsDeferredSendPhases(t *testing.T) {
	commandRunner := &fakeRunner{t: t, ctxHandler: func(ctx context.Context, args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "target", "--json"}):
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","status":"running"}`}, nil
		case reflect.DeepEqual(args, agentDeckDeferredSendArgs("target", defaultNotifyMessage)):
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("deferred send context has no deadline")
			}
			remaining := time.Until(deadline)
			if remaining <= syncCmdTimeout-2*time.Second || remaining > syncCmdTimeout {
				t.Fatalf("deferred send deadline remaining = %v, want approximately %v", remaining, syncCmdTimeout)
			}
			if remaining <= agentDeckNotifyDeferTimeout+agentDeckNotifyReadyTimeout {
				t.Fatalf("deferred send deadline remaining = %v, want greater than the internal phase total", remaining)
			}
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))

	outcome := manager.notifyRouteWithRetry(context.Background(), notificationEvent{
		Kind: notificationDelivery,
		Route: notificationRoute{
			Manager: "agent-deck",
			Target:  "target",
		},
	})

	if outcome.Status != "sent" || outcome.Err != nil {
		t.Fatalf("outcome = %+v, want bounded sent notification", outcome)
	}
}

func TestAgentDeckNotifyRetriesQueuedTargetBeforeNudging(t *testing.T) {
	probeAttempts := 0
	nudgeAttempts := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "target", "--json"}):
			probeAttempts++
			if probeAttempts < 3 {
				return RunResult{ExitCode: 0, Stdout: `{"id":"target","status":"queued"}`}, nil
			}
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","status":"running"}`}, nil
		case reflect.DeepEqual(args, agentDeckDeferredSendArgs("target", defaultNotifyMessage)):
			nudgeAttempts++
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))
	retryDelays := []time.Duration{}
	manager.retryWait = func(_ context.Context, delay time.Duration) error {
		retryDelays = append(retryDelays, delay)
		return nil
	}

	outcome := manager.notifyRouteWithRetry(context.Background(), notificationEvent{
		Kind: notificationDelivery,
		Route: notificationRoute{
			Manager: "agent-deck",
			Target:  "target",
		},
	})

	if outcome.Status != "sent" || outcome.Scheme != "agent-deck" || outcome.Err != nil {
		t.Fatalf("outcome = %+v, want sent agent-deck notification", outcome)
	}
	if probeAttempts != 3 || nudgeAttempts != 1 {
		t.Fatalf("probe attempts = %d, nudge attempts = %d, want 3 and 1", probeAttempts, nudgeAttempts)
	}
	if want := []time.Duration{500 * time.Millisecond, time.Second}; !reflect.DeepEqual(retryDelays, want) {
		t.Fatalf("retry delays = %v, want %v", retryDelays, want)
	}
}

func TestAgentDeckNotifyRetriesUnreadyTargetBeforeNudging(t *testing.T) {
	tests := []struct {
		name          string
		initialStatus string
	}{
		{name: "unknown", initialStatus: "unknown"},
		{name: "empty", initialStatus: ""},
		{name: "unrecognized", initialStatus: "starting"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probeAttempts := 0
			nudgeAttempts := 0
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch {
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "target", "--json"}):
					probeAttempts++
					status := test.initialStatus
					if probeAttempts > 1 {
						status = "running"
					}
					return RunResult{ExitCode: 0, Stdout: fmt.Sprintf(`{"id":"target","status":%q}`, status)}, nil
				case reflect.DeepEqual(args, agentDeckDeferredSendArgs("target", defaultNotifyMessage)):
					nudgeAttempts++
					return RunResult{ExitCode: 0}, nil
				default:
					t.Fatalf("unexpected command args: %v", args)
					return RunResult{}, nil
				}
			}}
			manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))
			retryDelays := []time.Duration{}
			manager.retryWait = func(_ context.Context, delay time.Duration) error {
				retryDelays = append(retryDelays, delay)
				return nil
			}

			outcome := manager.notifyRouteWithRetry(context.Background(), notificationEvent{
				Kind: notificationDelivery,
				Route: notificationRoute{
					Manager: "agent-deck",
					Target:  "target",
				},
			})

			if outcome.Status != "sent" || outcome.Scheme != "agent-deck" || outcome.Err != nil {
				t.Fatalf("outcome = %+v, want sent agent-deck notification", outcome)
			}
			if probeAttempts != 2 || nudgeAttempts != 1 {
				t.Fatalf("probe attempts = %d, nudge attempts = %d, want 2 and 1", probeAttempts, nudgeAttempts)
			}
			if want := []time.Duration{500 * time.Millisecond}; !reflect.DeepEqual(retryDelays, want) {
				t.Fatalf("retry delays = %v, want %v", retryDelays, want)
			}
		})
	}
}

func TestAgentDeckNotifyStopsAfterExhaustingNewTargetProbeRetries(t *testing.T) {
	tests := []struct {
		name       string
		exitCode   int
		stdout     string
		wantStatus string
	}{
		{
			name:       "target session not found",
			exitCode:   2,
			wantStatus: "target_not_found",
		},
		{
			name:       "queued",
			exitCode:   0,
			stdout:     `{"id":"target","status":"queued"}`,
			wantStatus: "target_queued",
		},
		{
			name:       "unknown",
			exitCode:   0,
			stdout:     `{"id":"target","status":"unknown"}`,
			wantStatus: "target_not_ready",
		},
		{
			name:       "empty",
			exitCode:   0,
			stdout:     `{"id":"target","status":""}`,
			wantStatus: "target_not_ready",
		},
		{
			name:       "unrecognized",
			exitCode:   0,
			stdout:     `{"id":"target","status":"starting"}`,
			wantStatus: "target_not_ready",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			probeAttempts := 0
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				if !reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "target", "--json"}) {
					t.Fatalf("unexpected command args: %v", args)
				}
				probeAttempts++
				return RunResult{ExitCode: test.exitCode, Stdout: test.stdout}, nil
			}}
			manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))
			retryDelays := []time.Duration{}
			manager.retryWait = func(_ context.Context, delay time.Duration) error {
				retryDelays = append(retryDelays, delay)
				return nil
			}

			outcome := manager.notifyRouteWithRetry(context.Background(), notificationEvent{
				Kind: notificationDelivery,
				Route: notificationRoute{
					Manager: "agent-deck",
					Target:  "target",
				},
			})

			if outcome.Status != test.wantStatus || outcome.Scheme != "agent-deck" || outcome.Err != nil {
				t.Fatalf("outcome = %+v, want exhausted %s", outcome, test.wantStatus)
			}
			if probeAttempts != 4 {
				t.Fatalf("probe attempts = %d, want 4", probeAttempts)
			}
			wantDelays := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
			if !reflect.DeepEqual(retryDelays, wantDelays) {
				t.Fatalf("retry delays = %v, want %v", retryDelays, wantDelays)
			}
		})
	}
}

func TestAgentDeckSessionNotFoundDetailSuggestsNearbyBoundAddress(t *testing.T) {
	manager := newSessionManager(&fakeRunner{t: t}, &serverState{
		boundAddresses: []string{"agent-deck/reviewer", "codex/self"},
	})

	detail := manager.agentDeckSessionNotFoundDetail(context.Background(), "reveiwer")
	if !strings.Contains(detail, "agent-deck session \"reveiwer\" not found") {
		t.Fatalf("detail = %q, want missing-session warning", detail)
	}
	if !strings.Contains(detail, "Did you mean agent-deck/reviewer?") {
		t.Fatalf("detail = %q, want nearby-session suggestion", detail)
	}
}

func TestAgentDeckSessionNotFoundDetailReadsXDGStateDB(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)
	xdgDataHome := filepath.Join(home, "xdg-data")
	t.Setenv("XDG_DATA_HOME", xdgDataHome)
	isolateAutoBindEnv(t)
	writeAgentDeckStateDBRowsAt(t, filepath.Join(xdgDataHome, "agent-deck", "profiles", "default", "state.db"), []agentDeckStateDBRow{{
		ID: "reviewer-session", ProjectPath: "/tmp", CodexSessionID: "codex-reviewer", CreatedAt: 1, LastAccessed: 2,
	}})

	manager := newSessionManager(&fakeRunner{t: t}, &serverState{})
	detail := manager.agentDeckSessionNotFoundDetail(context.Background(), "reviewer-sessin")
	if !strings.Contains(detail, "Did you mean agent-deck/reviewer-session?") {
		t.Fatalf("detail = %q, want XDG session suggestion", detail)
	}
}

func TestAgentDeckStateDBPathsDefaultToXDGDataHome(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)
	t.Setenv("XDG_DATA_HOME", "")

	want := filepath.Join(home, ".local", "share", "agent-deck", "profiles", "default", "state.db")
	if !slices.Contains(agentDeckStateDBPaths(), want) {
		t.Fatalf("state DB paths = %v, want default XDG path %q", agentDeckStateDBPaths(), want)
	}
}

func TestAgentDeckNotifyRetriesStoppedAndErrorTargets(t *testing.T) {
	for _, status := range []string{"stopped", "error"} {
		t.Run(status+" recovers", func(t *testing.T) {
			probeAttempts := 0
			nudgeAttempts := 0
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch {
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "target", "--json"}):
					probeAttempts++
					if probeAttempts < 3 {
						return RunResult{ExitCode: 0, Stdout: fmt.Sprintf(`{"id":"target","status":%q}`, status)}, nil
					}
					return RunResult{ExitCode: 0, Stdout: `{"id":"target","status":"running"}`}, nil
				case reflect.DeepEqual(args, agentDeckDeferredSendArgs("target", defaultNotifyMessage)):
					nudgeAttempts++
					return RunResult{ExitCode: 0}, nil
				default:
					t.Fatalf("unexpected command args: %v", args)
					return RunResult{}, nil
				}
			}}
			manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))
			retryDelays := []time.Duration{}
			manager.retryWait = func(_ context.Context, delay time.Duration) error {
				retryDelays = append(retryDelays, delay)
				return nil
			}

			outcome := manager.notifyRouteWithRetry(context.Background(), notificationEvent{
				Kind: notificationDelivery,
				Route: notificationRoute{
					Manager: "agent-deck",
					Target:  "target",
				},
			})

			if outcome.Status != "sent" || outcome.Err != nil {
				t.Fatalf("outcome = %+v, want recovered notification", outcome)
			}
			if probeAttempts != 3 || nudgeAttempts != 1 {
				t.Fatalf("probe attempts = %d, nudge attempts = %d, want 3 and 1", probeAttempts, nudgeAttempts)
			}
			wantDelays := []time.Duration{500 * time.Millisecond, time.Second}
			if !reflect.DeepEqual(retryDelays, wantDelays) {
				t.Fatalf("retry delays = %v, want %v", retryDelays, wantDelays)
			}
		})

		t.Run(status+" persists", func(t *testing.T) {
			probeAttempts := 0
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				if !reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "target", "--json"}) {
					t.Fatalf("unexpected command args: %v", args)
				}
				probeAttempts++
				return RunResult{ExitCode: 0, Stdout: fmt.Sprintf(`{"id":"target","status":%q}`, status)}, nil
			}}
			manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))
			retryDelays := []time.Duration{}
			manager.retryWait = func(_ context.Context, delay time.Duration) error {
				retryDelays = append(retryDelays, delay)
				return nil
			}

			outcome := manager.notifyRouteWithRetry(context.Background(), notificationEvent{
				Kind: notificationDelivery,
				Route: notificationRoute{
					Manager: "agent-deck",
					Target:  "target",
				},
			})

			if outcome.Status != "target_"+status || outcome.Err == nil {
				t.Fatalf("outcome = %+v, want exhausted target_%s", outcome, status)
			}
			if probeAttempts != 4 {
				t.Fatalf("probe attempts = %d, want 4", probeAttempts)
			}
			wantDelays := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
			if !reflect.DeepEqual(retryDelays, wantDelays) {
				t.Fatalf("retry delays = %v, want %v", retryDelays, wantDelays)
			}
		})
	}
}

func TestCLINotifyWaypostSendUsesMCPNotificationPath(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	probeAttempts := 0
	nudgeAttempts := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder", "--json"}, "\x00"):
			probeAttempts++
			switch probeAttempts {
			case 1:
				return RunResult{ExitCode: 2, Stderr: "not found"}, nil
			case 2:
				return RunResult{ExitCode: 0, Stdout: `{"id":"coder","status":"queued"}`}, nil
			default:
				return RunResult{ExitCode: 0, Stdout: `{"id":"coder","status":"waiting"}`}, nil
			}
		case reflect.DeepEqual(args, agentDeckDeferredSendArgs("coder", defaultNotifyMessage)):
			nudgeAttempts++
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	outcome := notifyWaypostSendWithOptions(context.Background(), waypostService, waypost.SendNotificationRequest{
		Params: waypost.SendParams{
			ToAddress:   "agent-deck/coder",
			FromAddress: "agent-deck/supervisor",
			Subject:     "delegate",
			Body:        []byte("body"),
		},
		Result: waypost.SendResult{DeliveryID: "dlv_cli_notify"},
	}, Options{
		CommandRunner: commandRunner,
		NotifyDelay:   -1,
	})

	if outcome.Status != "sent" || outcome.Scheme != "agent-deck" || outcome.Err != nil {
		t.Fatalf("CLI notify outcome = %+v, want sent agent-deck outcome", outcome)
	}
	if probeAttempts != 3 || nudgeAttempts != 1 {
		t.Fatalf("probe attempts = %d, nudge attempts = %d, want 3 and 1", probeAttempts, nudgeAttempts)
	}
}

func TestWaypostSendDoesNotRetryUnconfirmedNudge(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	sendCount := 0
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		sendCount++
		if params.ToAddress != "agent-deck/target" || params.FromAddress != "agent-deck/self" || params.Subject != "delegate" {
			t.Fatalf("send params = %+v", params)
		}
		if string(params.Body) != "body" {
			t.Fatalf("send body = %q, want body", string(params.Body))
		}
		return waypost.SendResult{DeliveryID: "dlv_retry"}, nil
	}

	nudgeAttempts := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "target", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","title":"coder-123","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			nudgeAttempts++
			return RunResult{ExitCode: 1, Stdout: `{"success":false,"delivery":"typed","submitted":false,"error":"submission not confirmed","code":"DELIVERY_FAILED"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		NotifyDelay:           -1,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true
	retryDelays := []time.Duration{}
	service.notifications.retryWait = func(_ context.Context, delay time.Duration) error {
		retryDelays = append(retryDelays, delay)
		return nil
	}

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/target",
		"subject": "delegate",
		"body":    "body",
	})

	if got := output["status"]; got != "sent" {
		t.Fatalf("status = %v, want sent", got)
	}
	if got := output["notify_status"]; got != "unconfirmed" {
		t.Fatalf("notify_status = %v, want unconfirmed", got)
	}
	if got := output["notify_detail"]; got == nil || !strings.Contains(got.(string), "target pane") {
		t.Fatalf("notify_detail = %v, want typed delivery detail", got)
	}
	if got := output["notify_error"]; got != nil {
		t.Fatalf("notify_error = %v, want nil", got)
	}
	if sendCount != 1 {
		t.Fatalf("durable sends = %d, want 1", sendCount)
	}
	if nudgeAttempts != 1 {
		t.Fatalf("nudge attempts = %d, want 1", nudgeAttempts)
	}
	if len(retryDelays) != 0 {
		t.Fatalf("retry delays = %v, want none", retryDelays)
	}
}

func TestWaypostSendRetriesUnknownWakeProbeWithoutResendingDelivery(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	sendCount := 0
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		sendCount++
		return waypost.SendResult{DeliveryID: "dlv_probe_retry"}, nil
	}

	probeAttempts := 0
	nudgeAttempts := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "target", "--json"}, "\x00"):
			probeAttempts++
			if probeAttempts == 1 {
				return RunResult{ExitCode: 1, Stderr: "temporary lookup failure"}, nil
			}
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","title":"coder-123","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			nudgeAttempts++
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		NotifyDelay:           -1,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true
	retryDelays := []time.Duration{}
	service.notifications.retryWait = func(_ context.Context, delay time.Duration) error {
		retryDelays = append(retryDelays, delay)
		return nil
	}

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/target",
		"subject": "delegate",
		"body":    "body",
	})

	if got := output["notify_status"]; got != "sent" {
		t.Fatalf("notify_status = %v, want sent", got)
	}
	if sendCount != 1 {
		t.Fatalf("durable sends = %d, want 1", sendCount)
	}
	if probeAttempts != 2 {
		t.Fatalf("probe attempts = %d, want 2", probeAttempts)
	}
	if nudgeAttempts != 1 {
		t.Fatalf("nudge attempts = %d, want 1", nudgeAttempts)
	}
	if want := []time.Duration{500 * time.Millisecond}; !reflect.DeepEqual(retryDelays, want) {
		t.Fatalf("retry delays = %v, want %v", retryDelays, want)
	}
}

func TestWaypostSendRetriesNewAgentDeckTargetWithoutResendingDelivery(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	sendCount := 0
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		sendCount++
		if params.ToAddress != "agent-deck/target" || params.FromAddress != "agent-deck/self" {
			t.Fatalf("send params = %+v", params)
		}
		return waypost.SendResult{DeliveryID: "dlv_new_target"}, nil
	}

	probeAttempts := 0
	nudgeAttempts := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "target", "--json"}):
			probeAttempts++
			switch probeAttempts {
			case 1:
				return RunResult{ExitCode: 2, Stderr: "not found"}, nil
			case 2:
				return RunResult{ExitCode: 0, Stdout: `{"id":"target","status":"queued"}`}, nil
			default:
				return RunResult{ExitCode: 0, Stdout: `{"id":"target","status":"running"}`}, nil
			}
		case reflect.DeepEqual(args, agentDeckDeferredSendArgs("target", defaultNotifyMessage)):
			nudgeAttempts++
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		NotifyDelay:           -1,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true
	retryDelays := []time.Duration{}
	service.notifications.retryWait = func(_ context.Context, delay time.Duration) error {
		retryDelays = append(retryDelays, delay)
		return nil
	}

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/target",
		"subject": "delegate",
		"body":    "body",
	})

	if got := output["status"]; got != "sent" {
		t.Fatalf("status = %v, want sent", got)
	}
	if got := output["delivery_id"]; got != "dlv_new_target" {
		t.Fatalf("delivery_id = %v, want dlv_new_target", got)
	}
	if got := output["notify_status"]; got != "sent" {
		t.Fatalf("notify_status = %v, want sent", got)
	}
	if sendCount != 1 {
		t.Fatalf("durable sends = %d, want 1", sendCount)
	}
	if probeAttempts != 3 || nudgeAttempts != 1 {
		t.Fatalf("probe attempts = %d, nudge attempts = %d, want 3 and 1", probeAttempts, nudgeAttempts)
	}
	if want := []time.Duration{500 * time.Millisecond, time.Second}; !reflect.DeepEqual(retryDelays, want) {
		t.Fatalf("retry delays = %v, want %v", retryDelays, want)
	}
}

func TestWaypostSendStopsAfterExhaustingWakeProbeRetries(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	sendCount := 0
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		sendCount++
		if string(params.Body) != "body" {
			t.Fatalf("send body = %q, want body", string(params.Body))
		}
		return waypost.SendResult{DeliveryID: "dlv_retry_exhausted"}, nil
	}

	probeAttempts := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "target", "--json"}, "\x00"):
			probeAttempts++
			return RunResult{ExitCode: 1, Stderr: "persistent lookup failure"}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		NotifyDelay:           -1,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true
	retryDelays := []time.Duration{}
	service.notifications.retryWait = func(_ context.Context, delay time.Duration) error {
		retryDelays = append(retryDelays, delay)
		return nil
	}

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/target",
		"subject": "delegate",
		"body":    "body",
	})

	if got := output["status"]; got != "sent" {
		t.Fatalf("status = %v, want sent", got)
	}
	if got := output["delivery_id"]; got != "dlv_retry_exhausted" {
		t.Fatalf("delivery_id = %v, want dlv_retry_exhausted", got)
	}
	if got := output["notify_status"]; got != "failed" {
		t.Fatalf("notify_status = %v, want failed", got)
	}
	if got := output["notify_error"]; got == nil || !strings.Contains(got.(string), "unknown result") {
		t.Fatalf("notify_error = %v, want unknown lookup result detail", got)
	}
	if sendCount != 1 {
		t.Fatalf("durable sends = %d, want 1", sendCount)
	}
	if probeAttempts != 4 {
		t.Fatalf("probe attempts = %d, want 4", probeAttempts)
	}
	if want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}; !reflect.DeepEqual(retryDelays, want) {
		t.Fatalf("retry delays = %v, want %v", retryDelays, want)
	}
}

func TestWaypostSendDoesNotRetryAmbiguousNudgeTimeout(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		return waypost.SendResult{DeliveryID: "dlv_timeout"}, nil
	}

	probeAttempts := 0
	nudgeAttempts := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "target", "--json"}, "\x00"):
			probeAttempts++
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","title":"coder-123","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			nudgeAttempts++
			return RunResult{}, context.DeadlineExceeded
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		NotifyDelay:           -1,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true
	retryDelays := []time.Duration{}
	service.notifications.retryWait = func(_ context.Context, delay time.Duration) error {
		retryDelays = append(retryDelays, delay)
		return nil
	}

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/target",
		"subject": "delegate",
		"body":    "body",
	})

	if got := output["notify_status"]; got != "unconfirmed" {
		t.Fatalf("notify_status = %v, want unconfirmed", got)
	}
	if got := output["notify_detail"]; got == nil || !strings.Contains(got.(string), "timed out") {
		t.Fatalf("notify_detail = %v, want timeout detail", got)
	}
	if got := output["notify_error"]; got != nil {
		t.Fatalf("notify_error = %v, want nil", got)
	}
	if probeAttempts != 1 {
		t.Fatalf("probe attempts = %d, want 1", probeAttempts)
	}
	if nudgeAttempts != 1 {
		t.Fatalf("nudge attempts = %d, want 1", nudgeAttempts)
	}
	if len(retryDelays) != 0 {
		t.Fatalf("retry delays = %v, want none", retryDelays)
	}
}

func TestWaypostSendSkipsNotifyWhenDeliveryAlreadyClaimed(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	openCount := 0
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		return waypost.SendResult{DeliveryID: "dlv_claimed"}, nil
	}
	waypostService.readDeliveriesFunc = func(_ context.Context, deliveryIDs []string) ([]waypost.ReadDelivery, error) {
		if !reflect.DeepEqual(deliveryIDs, []string{"dlv_claimed"}) {
			t.Fatalf("ReadDeliveries ids = %v, want [dlv_claimed]", deliveryIDs)
		}
		return []waypost.ReadDelivery{{
			DeliveryID: "dlv_claimed",
			State:      "leased",
		}}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService, openCount: &openCount},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		NotifyDelay:          -1,
		DisableWakeScheduler: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":          "agent-deck/target",
		"subject":     "delegate",
		"body":        "body",
		"diagnostics": true,
	})

	if got := output["delivery_id"]; got != "dlv_claimed" {
		t.Fatalf("delivery_id = %v, want dlv_claimed", got)
	}
	if got := output["notify_status"]; got != "skipped_already_claimed" {
		t.Fatalf("notify_status = %v, want skipped_already_claimed", got)
	}
	if got := output["notify_scheme"]; got != "waypost" {
		t.Fatalf("notify_scheme = %v, want waypost", got)
	}
	if got := output["notify_error"]; got != nil {
		t.Fatalf("notify_error = %v, want nil", got)
	}
	if openCount != 1 {
		t.Fatalf("waypost service opens = %d, want 1", openCount)
	}
}

func TestWaypostSendNotifyIgnoresRequestCancellationAfterSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		cancel()
		return waypost.SendResult{DeliveryID: "dlv_cancelled"}, nil
	}
	waypostService.readDeliveriesFunc = func(ctx context.Context, deliveryIDs []string) ([]waypost.ReadDelivery, error) {
		if err := ctx.Err(); err != nil {
			t.Fatalf("ReadDeliveries context error = %v, want nil", err)
		}
		if !reflect.DeepEqual(deliveryIDs, []string{"dlv_cancelled"}) {
			t.Fatalf("ReadDeliveries ids = %v, want [dlv_cancelled]", deliveryIDs)
		}
		return []waypost.ReadDelivery{{
			DeliveryID: "dlv_cancelled",
			State:      "queued",
		}}, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "target", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","title":"coder-123","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		NotifyDelay:           -1,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	output, err := service.sendWaypostMessage(ctx, waypostSendInput{
		ToAddress: "agent-deck/target",
		Subject:   "delegate",
		Body:      "body",
	})
	if err != nil {
		t.Fatalf("sendWaypostMessage error = %v", err)
	}
	if got := output["notify_status"]; got != "sent" {
		t.Fatalf("notify_status = %v, want sent", got)
	}
}

func TestWaypostSendAllowsAgentDeckNotifyDisable(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		return waypost.SendResult{DeliveryID: "dlv_disabled"}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			switch {
			case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "target", "--json"}, "\x00"):
				return RunResult{ExitCode: 0, Stdout: `{"id":"target","title":"coder-123","status":"waiting"}`}, nil
			default:
				t.Fatalf("unexpected command call: %v", args)
				return RunResult{}, nil
			}
		}},
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":                     "agent-deck/target",
		"subject":                "delegate",
		"body":                   "body",
		"disable_notify_message": true,
		"diagnostics":            true,
	})

	if got := output["delivery_id"]; got != "dlv_disabled" {
		t.Fatalf("delivery_id = %v, want dlv_disabled", got)
	}
	if got := output["notify_status"]; got != "skipped_disabled" {
		t.Fatalf("notify_status = %v, want skipped_disabled", got)
	}
	if got := output["notify_scheme"]; got != "agent-deck" {
		t.Fatalf("notify_scheme = %v, want agent-deck", got)
	}
	if got := output["notify_error"]; got != nil {
		t.Fatalf("notify_error = %v, want nil", got)
	}
}

func TestWaypostSendRejectsExplicitFromAddressWithoutBoundState(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		t.Fatalf("unexpected send params = %+v", params)
		return waypost.SendResult{}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			return RunResult{}, errors.New("auto-bind should not run for explicit sender")
		}},
		DisableWakeScheduler: true,
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "waypost_send", map[string]any{
		"to":           "workflow/target",
		"from_address": "agent/sender",
		"subject":      "delegate",
		"body":         "body",
		"diagnostics":  true,
	})
	if err == nil || !strings.Contains(err.Error(), "not bound to this MCP server") {
		t.Fatalf("waypost_send error = %v, want unbound sender rejection", err)
	}
}

func TestWaypostSendRejectsExplicitFromAddressOutsideBoundState(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		t.Fatalf("unexpected send params = %+v", params)
		return waypost.SendResult{}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	setBoundTestSender(service, "agent/allowed")

	err := callServiceToolExpectError(t, service, "waypost_send", map[string]any{
		"to":           "workflow/target",
		"from_address": "agent/other",
		"subject":      "delegate",
		"body":         "body",
	})
	if err == nil || !strings.Contains(err.Error(), "not bound to this MCP server") {
		t.Fatalf("waypost_send error = %v, want sender outside bound state rejection", err)
	}
}

func TestWaypostSendBatchRejectsExplicitFromAddressOutsideBoundState(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		t.Fatalf("unexpected send params = %+v", params)
		return waypost.SendResult{}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	setBoundTestSender(service, "agent/allowed")

	err := callServiceToolExpectError(t, service, "waypost_send", map[string]any{
		"to":           []string{"workflow/one", "workflow/two"},
		"from_address": "agent/other",
		"subject":      "delegate",
		"body":         "body",
	})
	if err == nil || !strings.Contains(err.Error(), "not bound to this MCP server") {
		t.Fatalf("waypost_send batch error = %v, want sender outside bound state rejection", err)
	}
}

func TestWaypostSendGroupModeUsesGroupSendParams(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if !params.Group {
			t.Fatal("send group = false, want true")
		}
		if params.ToAddress != "group/review" || params.FromAddress != "agent/sender" || params.AsPerson != "alice" {
			t.Fatalf("send params = %+v", params)
		}
		return waypost.SendResult{
			Mode:             waypost.SendModeGroup,
			MessageID:        "msg_group",
			GroupID:          "grp_1",
			GroupAddress:     "group/review",
			EligibleCount:    2,
			MessageCreatedAt: "2026-04-18T00:00:00Z",
		}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			return RunResult{}, errors.New("auto-bind should not run for explicit sender")
		}},
		DisableWakeScheduler: true,
	})
	setBoundTestSender(service, "agent/sender")

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":           "group/review",
		"from_address": "agent/sender",
		"subject":      "group update",
		"body":         "body",
		"group":        true,
		"as_person":    " alice ",
		"diagnostics":  true,
	})

	if got := output["mode"]; got != waypost.SendModeGroup {
		t.Fatalf("mode = %v, want %q", got, waypost.SendModeGroup)
	}
	if got := output["message_id"]; got != "msg_group" {
		t.Fatalf("message_id = %v, want msg_group", got)
	}
	if got := output["group_id"]; got != "grp_1" {
		t.Fatalf("group_id = %v, want grp_1", got)
	}
	if got := output["group_address"]; got != "group/review" {
		t.Fatalf("group_address = %v, want group/review", got)
	}
	if got := output["eligible_count"]; got != float64(2) {
		t.Fatalf("eligible_count = %v, want 2", got)
	}
	if got := output["message_created_at"]; got != "2026-04-18T00:00:00Z" {
		t.Fatalf("message_created_at = %v, want timestamp", got)
	}
	if got := output["delivery_id"]; got != nil {
		t.Fatalf("delivery_id = %v, want nil", got)
	}
}

func TestWaypostSendGroupModeNotifiesSubscriber(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if !params.Group {
			t.Fatal("send group = false, want true")
		}
		return waypost.SendResult{
			Mode:                       waypost.SendModeGroup,
			MessageID:                  "msg_group",
			GroupID:                    "grp_1",
			GroupAddress:               "group/review",
			EligibleCount:              1,
			GroupNotificationAddresses: []string{"agent-deck/moderator", "agent-deck/observer"},
			MessageCreatedAt:           "2026-04-18T00:00:00Z",
		}, nil
	}
	waypostService.listGroupSubscribersFunc = func(_ context.Context, groupAddress string) ([]waypost.GroupNotificationSubscriberRecord, error) {
		t.Fatalf("ListGroupNotificationSubscribers should not be called for group send notify: %q", groupAddress)
		return nil, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "moderator", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"moderator","title":"moderator","status":"waiting"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "observer", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"observer","title":"observer","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			if args[9] != "moderator" && args[9] != "observer" {
				t.Fatalf("notify target = %q, want moderator or observer", args[9])
			}
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
	})
	setBoundTestSender(service, "agent-deck/expert")

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":           "group/review",
		"from_address": "agent-deck/expert",
		"subject":      "expert post",
		"body":         "body",
		"group":        true,
		"diagnostics":  true,
	})

	if got := output["message_id"]; got != "msg_group" {
		t.Fatalf("message_id = %v, want msg_group", got)
	}
	if got := output["notify_status"]; got != "sent" {
		t.Fatalf("notify_status = %v, want sent", got)
	}
	if got := output["notify_scheme"]; got != "agent-deck" {
		t.Fatalf("notify_scheme = %v, want agent-deck", got)
	}
	if got := output["notify_error"]; got != nil {
		t.Fatalf("notify_error = %v, want nil", got)
	}

	sentTargets := map[string]int{}
	for _, call := range commandRunner.Calls() {
		if isAgentDeckDeferredSend(call.Args) {
			sentTargets[call.Args[9]]++
		}
	}
	if sentTargets["moderator"] != 1 || sentTargets["observer"] != 1 {
		t.Fatalf("sent targets = %v, want moderator and observer once", sentTargets)
	}
}

func TestWaypostSendGroupModeReportsNoSubscribersWhenSendQueuesNoNotifications(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		return waypost.SendResult{
			Mode:             waypost.SendModeGroup,
			MessageID:        "msg_group",
			GroupID:          "grp_1",
			GroupAddress:     "group/review",
			MessageCreatedAt: "2026-04-18T00:00:00Z",
		}, nil
	}
	waypostService.listGroupSubscribersFunc = func(_ context.Context, groupAddress string) ([]waypost.GroupNotificationSubscriberRecord, error) {
		t.Fatalf("ListGroupNotificationSubscribers should not be called for group send notify: %q", groupAddress)
		return nil, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	setBoundTestSender(service, "agent-deck/moderator")

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":           "group/review",
		"from_address": "agent-deck/moderator",
		"subject":      "moderator post",
		"body":         "body",
		"group":        true,
	})
	if got := output["message_id"]; got != "msg_group" {
		t.Fatalf("message_id = %v, want msg_group", got)
	}
	if got := output["notify_status"]; got != "no_subscribers" {
		t.Fatalf("notify_status = %v, want no_subscribers", got)
	}
}

func TestWaypostSendGroupModeReportsNoSubscribersForResolvedDefaultSender(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if params.FromAddress != "agent-deck/moderator" {
			t.Fatalf("send from_address = %q, want resolved default sender", params.FromAddress)
		}
		return waypost.SendResult{
			Mode:             waypost.SendModeGroup,
			MessageID:        "msg_group",
			GroupID:          "grp_1",
			GroupAddress:     "group/review",
			MessageCreatedAt: "2026-04-18T00:00:00Z",
		}, nil
	}
	waypostService.listGroupSubscribersFunc = func(_ context.Context, groupAddress string) ([]waypost.GroupNotificationSubscriberRecord, error) {
		t.Fatalf("ListGroupNotificationSubscribers should not be called for group send notify: %q", groupAddress)
		return nil, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	service.state.boundAddresses = []string{"agent-deck/moderator"}
	service.state.defaultSender = "agent-deck/moderator"
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":          "group/review",
		"subject":     "moderator post",
		"body":        "body",
		"group":       true,
		"diagnostics": true,
	})
	if got := output["from_address"]; got != "agent-deck/moderator" {
		t.Fatalf("from_address = %v, want agent-deck/moderator", got)
	}
	if got := output["notify_status"]; got != "no_subscribers" {
		t.Fatalf("notify_status = %v, want no_subscribers", got)
	}
}

func TestWaypostSendGroupModeKeepsReceiptWhenSubscriberNotifyFails(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	openCount := 0
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		return waypost.SendResult{
			Mode:                       waypost.SendModeGroup,
			MessageID:                  "msg_group",
			GroupID:                    "grp_1",
			GroupAddress:               "group/review",
			GroupNotificationAddresses: []string{"agent-deck/moderator"},
			MessageCreatedAt:           "2026-04-18T00:00:00Z",
		}, nil
	}
	waypostService.listGroupSubscribersFunc = func(_ context.Context, groupAddress string) ([]waypost.GroupNotificationSubscriberRecord, error) {
		t.Fatalf("ListGroupNotificationSubscribers should not be called for group send notify: %q", groupAddress)
		return nil, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "moderator", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"moderator","title":"moderator","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			return RunResult{ExitCode: 1, Stderr: "notify failed"}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService, openCount: &openCount},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
	})
	setBoundTestSender(service, "agent-deck/expert")
	service.notifications.retryWait = func(context.Context, time.Duration) error { return nil }

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":           "group/review",
		"from_address": "agent-deck/expert",
		"subject":      "expert post",
		"body":         "body",
		"group":        true,
	})

	if got := output["status"]; got != "sent" {
		t.Fatalf("status = %v, want sent", got)
	}
	if got := output["message_id"]; got != "msg_group" {
		t.Fatalf("message_id = %v, want msg_group", got)
	}
	if got := output["notify_status"]; got != "failed" {
		t.Fatalf("notify_status = %v, want failed", got)
	}
	if got := output["notify_error"]; got == nil || !strings.Contains(got.(string), "notify failed") {
		t.Fatalf("notify_error = %v, want notify failure detail", got)
	}
	if openCount != 1 {
		t.Fatalf("waypost service opens = %d, want 1", openCount)
	}
}

func TestWaypostSendGroupModeReportsPartialNotifyFailureDetail(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		return waypost.SendResult{
			Mode:                       waypost.SendModeGroup,
			MessageID:                  "msg_group_partial",
			GroupID:                    "grp_1",
			GroupAddress:               "group/review",
			GroupNotificationAddresses: []string{"agent-deck/moderator", "agent-deck/observer"},
			MessageCreatedAt:           "2026-04-18T00:00:00Z",
		}, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "moderator", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"moderator","title":"moderator","status":"waiting"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "observer", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"observer","title":"observer","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args) && args[9] == "moderator":
			return RunResult{ExitCode: 0}, nil
		case isAgentDeckDeferredSend(args) && args[9] == "observer":
			return RunResult{ExitCode: 1, Stderr: "observer notify failed"}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
	})
	setBoundTestSender(service, "agent-deck/expert")
	service.notifications.retryWait = func(context.Context, time.Duration) error { return nil }

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":           "group/review",
		"from_address": "agent-deck/expert",
		"subject":      "expert post",
		"body":         "body",
		"group":        true,
	})

	if got := output["notify_status"]; got != "partial_failed" {
		t.Fatalf("notify_status = %v, want partial_failed", got)
	}
	if got := output["notify_error"]; got == nil || !strings.Contains(got.(string), "observer notify failed") {
		t.Fatalf("notify_error = %v, want observer failure detail", got)
	}
}

func TestNotifyGroupSubscribersPreservesFailureBeforeUnsupportedTarget(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "moderator", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"moderator","title":"moderator","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			return RunResult{ExitCode: 1, Stderr: "moderator notify failed"}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))
	manager.retryWait = func(context.Context, time.Duration) error { return nil }

	outcome := manager.notifyGroupSubscribers(context.Background(), waypostSendInput{
		FromAddress: "agent-deck/expert",
	}, []string{"agent-deck/moderator", "codex/observer"})

	if outcome.Status != "failed" {
		t.Fatalf("status = %q, want failed", outcome.Status)
	}
	if outcome.Scheme != "mixed" {
		t.Fatalf("scheme = %q, want mixed", outcome.Scheme)
	}
	if outcome.Err == nil || !strings.Contains(outcome.Err.Error(), "moderator notify failed") {
		t.Fatalf("error = %v, want moderator failure detail", outcome.Err)
	}
}

func TestAggregateGroupNotificationsPreservesDefiniteNonDeliveries(t *testing.T) {
	tests := []struct {
		name       string
		results    []groupSubscriberNotificationResult
		wantStatus string
	}{
		{
			name: "sent and unconfirmed",
			results: []groupSubscriberNotificationResult{
				{attempted: true, status: "sent", delivered: true},
				{attempted: true, status: "unconfirmed", detail: "submission not confirmed"},
			},
			wantStatus: "unconfirmed",
		},
		{
			name: "sent unconfirmed and unsupported",
			results: []groupSubscriberNotificationResult{
				{attempted: true, status: "sent", delivered: true},
				{attempted: true, status: "unconfirmed", detail: "submission not confirmed"},
				{attempted: true, status: "unsupported"},
			},
			wantStatus: "partial_failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := aggregateGroupSubscriberNotifications(test.results)
			if outcome.Status != test.wantStatus {
				t.Fatalf("status = %q, want %q", outcome.Status, test.wantStatus)
			}
		})
	}
}

func TestNotifyGroupSubscribersAggregatesMixedUnavailableOutcomes(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		if strings.Join(args, "\x00") != strings.Join([]string{"agent-deck", "session", "show", "missing", "--json"}, "\x00") {
			t.Fatalf("unexpected command args: %v", args)
		}
		return RunResult{ExitCode: 2, Stdout: `{"success":false}`}, nil
	}}
	manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))

	for _, addresses := range [][]string{
		{"agent-deck/missing", "codex/observer"},
		{"codex/observer", "agent-deck/missing"},
	} {
		outcome := manager.notifyGroupSubscribers(context.Background(), waypostSendInput{
			FromAddress: "agent-deck/expert",
		}, addresses)

		if outcome.Status != "unavailable" {
			t.Fatalf("addresses = %v, status = %q, want unavailable", addresses, outcome.Status)
		}
		if outcome.Scheme != "mixed" {
			t.Fatalf("addresses = %v, scheme = %q, want mixed", addresses, outcome.Scheme)
		}
		if outcome.Err != nil {
			t.Fatalf("addresses = %v, error = %v, want nil", addresses, outcome.Err)
		}
	}
}

func TestNotifyGroupSubscribersAggregatesAllFailuresWithoutSuccess(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case len(args) == 5 && args[0] == "agent-deck" && args[1] == "session" && args[2] == "show":
			target := args[3]
			return RunResult{ExitCode: 0, Stdout: fmt.Sprintf(`{"id":%q,"title":%q,"status":"waiting"}`, target, target)}, nil
		case isAgentDeckDeferredSend(args):
			return RunResult{ExitCode: 1, Stderr: args[9] + " notify failed"}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))
	manager.retryWait = func(context.Context, time.Duration) error { return nil }

	outcome := manager.notifyGroupSubscribers(context.Background(), waypostSendInput{
		FromAddress: "agent-deck/expert",
	}, []string{"agent-deck/moderator", "agent-deck/observer"})

	if outcome.Status != "failed" {
		t.Fatalf("status = %q, want failed", outcome.Status)
	}
	if outcome.Err == nil {
		t.Fatal("error = nil, want both notification failures")
	}
	for _, want := range []string{"moderator notify failed", "observer notify failed"} {
		if !strings.Contains(outcome.Err.Error(), want) {
			t.Fatalf("error = %v, want %q", outcome.Err, want)
		}
	}
}

func TestNotifyGroupSubscribersBoundsConcurrentFanoutLatency(t *testing.T) {
	const (
		subscriberCount = 10
		concurrency     = 3
		fanoutBudget    = 50 * time.Millisecond
	)

	addresses := make([]string, 0, subscriberCount)
	for index := range subscriberCount {
		addresses = append(addresses, fmt.Sprintf("agent-deck/worker-%d", index))
	}

	var activeMu sync.Mutex
	activeSends := 0
	maxActiveSends := 0
	sendAttempts := 0
	commandRunner := &fakeRunner{t: t, ctxHandler: func(ctx context.Context, args []string, input string) (RunResult, error) {
		switch {
		case len(args) == 5 && args[0] == "agent-deck" && args[1] == "session" && args[2] == "show":
			target := args[3]
			return RunResult{ExitCode: 0, Stdout: fmt.Sprintf(`{"id":%q,"status":"running"}`, target)}, nil
		case isAgentDeckDeferredSend(args):
			activeMu.Lock()
			activeSends++
			sendAttempts++
			if activeSends > maxActiveSends {
				maxActiveSends = activeSends
			}
			activeMu.Unlock()

			<-ctx.Done()

			activeMu.Lock()
			activeSends--
			activeMu.Unlock()
			return RunResult{}, ctx.Err()
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	manager := newNotificationManager(commandRunner, newSessionManager(commandRunner, &serverState{}))
	manager.groupNotifyTimeout = fanoutBudget
	manager.groupNotifyConcurrency = concurrency

	startedAt := time.Now()
	outcome := manager.notifyGroupSubscribers(context.Background(), waypostSendInput{
		FromAddress: "agent-deck/expert",
	}, addresses)
	elapsed := time.Since(startedAt)

	if outcome.Status != "failed" || !errors.Is(outcome.Err, context.DeadlineExceeded) {
		t.Fatalf("outcome = %+v, want deadline-bounded failure", outcome)
	}
	if elapsed < fanoutBudget/2 || elapsed > fanoutBudget+500*time.Millisecond {
		t.Fatalf("fanout elapsed = %v, want approximately %v independent of subscriber count", elapsed, fanoutBudget)
	}
	activeMu.Lock()
	defer activeMu.Unlock()
	if maxActiveSends != concurrency {
		t.Fatalf("max active sends = %d, want concurrency limit %d", maxActiveSends, concurrency)
	}
	if sendAttempts != concurrency {
		t.Fatalf("send attempts = %d, want only the initial %d workers before the shared deadline", sendAttempts, concurrency)
	}
	if activeSends != 0 {
		t.Fatalf("active sends after return = %d, want 0", activeSends)
	}
}

func TestWaypostForwardByMessageIDPreservesPayloadAndPrefixesSubject(t *testing.T) {
	t.Skip("waypost_forward is CLI-owned after the MCP hard cut")
	sourceSenderAddress := "agent/source"
	waypostService := &fakeWaypostService{t: t}
	openCount := 0
	waypostService.readMessagesFunc = func(_ context.Context, messageIDs []string) ([]waypost.ReadMessage, error) {
		if diff := slices.Compare(messageIDs, []string{"msg_1"}); diff != 0 {
			t.Fatalf("ReadMessages ids = %v, want [msg_1]", messageIDs)
		}
		return []waypost.ReadMessage{{
			MessageID:     "msg_1",
			SenderAddress: &sourceSenderAddress,
			Subject:       "Original subject",
			ContentType:   "text/markdown",
			SchemaVersion: "v2",
			Body:          "forward me",
		}}, nil
	}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if params.ToAddress != "workflow/target" {
			t.Fatalf("send to_address = %q, want workflow/target", params.ToAddress)
		}
		if params.FromAddress != "agent/sender" {
			t.Fatalf("send from_address = %q, want agent/sender", params.FromAddress)
		}
		if params.Subject != "Fwd: Original subject" {
			t.Fatalf("send subject = %q, want forwarded subject", params.Subject)
		}
		if params.ContentType != "text/markdown" {
			t.Fatalf("send content_type = %q, want text/markdown", params.ContentType)
		}
		if params.SchemaVersion != "v2" {
			t.Fatalf("send schema_version = %q, want v2", params.SchemaVersion)
		}
		if params.ForwardedMessageID != "msg_1" {
			t.Fatalf("send forwarded_message_id = %q, want msg_1", params.ForwardedMessageID)
		}
		if params.ForwardedFromAddress != "agent/source" {
			t.Fatalf("send forwarded_from_address = %q, want agent/source", params.ForwardedFromAddress)
		}
		if string(params.Body) != "forward me" {
			t.Fatalf("send body = %q, want forward me", string(params.Body))
		}
		return waypost.SendResult{DeliveryID: "dlv_forwarded"}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService, openCount: &openCount},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			return RunResult{}, errors.New("auto-bind should not run for explicit sender")
		}},
		DisableWakeScheduler: true,
	})

	output := callServiceTool(t, service, "waypost_forward", map[string]any{
		"message_id":   "msg_1",
		"to":           "workflow/target",
		"from_address": "agent/sender",
	})

	if got := output["status"]; got != "forwarded" {
		t.Fatalf("status = %v, want forwarded", got)
	}
	if got := output["delivery_id"]; got != "dlv_forwarded" {
		t.Fatalf("delivery_id = %v, want dlv_forwarded", got)
	}
	if got := output["source_message_id"]; got != "msg_1" {
		t.Fatalf("source_message_id = %v, want msg_1", got)
	}
	if openCount != 1 {
		t.Fatalf("waypost service opens = %d, want 1", openCount)
	}
}

func TestWaypostForwardByDeliveryIDAllowsSubjectOverride(t *testing.T) {
	t.Skip("waypost_forward is CLI-owned after the MCP hard cut")
	sourceSenderAddress := "agent/source"
	waypostService := &fakeWaypostService{t: t}
	waypostService.readDeliveriesFunc = func(_ context.Context, deliveryIDs []string) ([]waypost.ReadDelivery, error) {
		if diff := slices.Compare(deliveryIDs, []string{"dlv_1"}); diff != 0 {
			t.Fatalf("ReadDeliveries ids = %v, want [dlv_1]", deliveryIDs)
		}
		return []waypost.ReadDelivery{{
			DeliveryID:    "dlv_1",
			MessageID:     "msg_1",
			SenderAddress: &sourceSenderAddress,
			Subject:       "Original subject",
			ContentType:   "text/plain",
			SchemaVersion: "v1",
			Body:          "forward me",
		}}, nil
	}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if params.Subject != "Custom forward subject" {
			t.Fatalf("send subject = %q, want Custom forward subject", params.Subject)
		}
		if params.ForwardedMessageID != "msg_1" {
			t.Fatalf("send forwarded_message_id = %q, want msg_1", params.ForwardedMessageID)
		}
		if params.ForwardedFromAddress != "agent/source" {
			t.Fatalf("send forwarded_from_address = %q, want agent/source", params.ForwardedFromAddress)
		}
		return waypost.SendResult{DeliveryID: "dlv_forwarded"}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			return RunResult{}, errors.New("auto-bind should not run for explicit sender")
		}},
		DisableWakeScheduler: true,
	})

	output := callServiceTool(t, service, "waypost_forward", map[string]any{
		"delivery_id":  "dlv_1",
		"to":           "workflow/target",
		"from_address": "agent/sender",
		"subject":      "Custom forward subject",
	})

	if got := output["status"]; got != "forwarded" {
		t.Fatalf("status = %v, want forwarded", got)
	}
	if got := output["source_delivery_id"]; got != "dlv_1" {
		t.Fatalf("source_delivery_id = %v, want dlv_1", got)
	}
}

func TestWaypostForwardToGroupInboxPreservesGroupMode(t *testing.T) {
	t.Skip("waypost_forward is CLI-owned after the MCP hard cut")
	sourceSenderAddress := "agent/source"
	waypostService := &fakeWaypostService{t: t}
	waypostService.readMessagesFunc = func(_ context.Context, messageIDs []string) ([]waypost.ReadMessage, error) {
		if diff := slices.Compare(messageIDs, []string{"msg_1"}); diff != 0 {
			t.Fatalf("ReadMessages ids = %v, want [msg_1]", messageIDs)
		}
		return []waypost.ReadMessage{{
			MessageID:     "msg_1",
			SenderAddress: &sourceSenderAddress,
			Subject:       "Original subject",
			ContentType:   "text/plain",
			Body:          "forward me",
		}}, nil
	}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if !params.Group {
			t.Fatal("send group = false, want true")
		}
		if params.ToAddress != "group/review" {
			t.Fatalf("send to_address = %q, want group/review", params.ToAddress)
		}
		if params.ForwardedMessageID != "msg_1" {
			t.Fatalf("send forwarded_message_id = %q, want msg_1", params.ForwardedMessageID)
		}
		if params.ForwardedFromAddress != "agent/source" {
			t.Fatalf("send forwarded_from_address = %q, want agent/source", params.ForwardedFromAddress)
		}
		return waypost.SendResult{
			Mode:             waypost.SendModeGroup,
			MessageID:        "msg_forwarded",
			GroupID:          "grp_1",
			GroupAddress:     "group/review",
			EligibleCount:    1,
			MessageCreatedAt: "2026-04-18T00:00:00Z",
		}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			return RunResult{}, errors.New("auto-bind should not run for explicit sender")
		}},
		DisableWakeScheduler: true,
	})

	output := callServiceTool(t, service, "waypost_forward", map[string]any{
		"message_id":   "msg_1",
		"to":           "group/review",
		"from_address": "agent/sender",
		"group":        true,
	})

	if got := output["status"]; got != "forwarded" {
		t.Fatalf("status = %v, want forwarded", got)
	}
	if got := output["mode"]; got != waypost.SendModeGroup {
		t.Fatalf("mode = %v, want %q", got, waypost.SendModeGroup)
	}
	if got := output["message_id"]; got != "msg_forwarded" {
		t.Fatalf("message_id = %v, want msg_forwarded", got)
	}
	if got := output["group_address"]; got != "group/review" {
		t.Fatalf("group_address = %v, want group/review", got)
	}
	if got := output["delivery_id"]; got != nil {
		t.Fatalf("delivery_id = %v, want nil for group send", got)
	}
	if got := output["source_message_id"]; got != "msg_1" {
		t.Fatalf("source_message_id = %v, want msg_1", got)
	}
}

func TestWaypostForwardRequiresExactlyOneSourceID(t *testing.T) {
	t.Skip("waypost_forward is CLI-owned after the MCP hard cut")
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "waypost_forward", map[string]any{
		"message_id":  "msg_1",
		"delivery_id": "dlv_1",
		"to":          "workflow/target",
	})
	if err == nil || !strings.Contains(err.Error(), "requires exactly one of message_id or delivery_id") {
		t.Fatalf("waypost_forward error = %v, want source id validation", err)
	}
}

func TestWaypostSendUsesFixedWakeTextWhenDisableFlagUnset(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		return waypost.SendResult{DeliveryID: "dlv_custom"}, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "target", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","title":"coder-123","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			if args[9] != "target" {
				t.Fatalf("notify target = %q, want target", args[9])
			}
			if args[10] != defaultNotifyMessage {
				t.Fatalf("notify message = %q, want fixed default", args[10])
			}
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":                     "agent-deck/target",
		"subject":                "delegate",
		"body":                   "body",
		"disable_notify_message": false,
		"diagnostics":            true,
	})

	if got := output["delivery_id"]; got != "dlv_custom" {
		t.Fatalf("delivery_id = %v, want dlv_custom", got)
	}
	if got := output["notify_status"]; got != "sent" {
		t.Fatalf("notify_status = %v, want sent", got)
	}
	if got := output["notify_scheme"]; got != "agent-deck" {
		t.Fatalf("notify_scheme = %v, want agent-deck", got)
	}
	if got := output["notify_error"]; got != nil {
		t.Fatalf("notify_error = %v, want nil", got)
	}
}

func TestWaypostSendPreservesWaypostDefaultsWhenMetadataOmitted(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		if params.ContentType != "" || params.SchemaVersion != "" {
			t.Fatalf("send params unexpectedly set defaults: %+v", params)
		}
		return waypost.SendResult{DeliveryID: "dlv_2"}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":           "agent-deck/self",
		"subject":      "delegate",
		"body":         "body",
		"from_address": "agent-deck/self",
	})

	if got := output["delivery_id"]; got != "dlv_2" {
		t.Fatalf("delivery_id = %v, want dlv_2", got)
	}
	if got := output["notify_status"]; got != "skipped_local" {
		t.Fatalf("notify_status = %v, want skipped_local", got)
	}
}

func TestWaypostSendReturnsReceiptWhenNotifyFails(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
		return waypost.SendResult{DeliveryID: "dlv_3"}, nil
	}
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "target", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"target","title":"coder-123","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			return RunResult{ExitCode: 1, Stderr: "wakeup failed"}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true
	service.notifications.retryWait = func(context.Context, time.Duration) error { return nil }

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":          "agent-deck/target",
		"subject":     "delegate",
		"body":        "body",
		"diagnostics": true,
	})

	if got := output["status"]; got != "sent" {
		t.Fatalf("status = %v, want sent", got)
	}
	if got := output["delivery_id"]; got != "dlv_3" {
		t.Fatalf("delivery_id = %v, want dlv_3", got)
	}
	if got := output["notify_status"]; got != "failed" {
		t.Fatalf("notify_status = %v, want failed", got)
	}
	if got := output["notify_scheme"]; got != "agent-deck" {
		t.Fatalf("notify_scheme = %v, want agent-deck", got)
	}
	if got := output["notify_error"]; got == nil || !strings.Contains(got.(string), "wakeup failed") {
		t.Fatalf("notify_error = %v, want wakeup failure detail", got)
	}
}

func TestToolResultDoesNotOpenWaypostServiceForHint(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	status := callServiceTool(t, service, "waypost_status", nil)
	if got := status["mail_hint"]; got != nil {
		t.Fatalf("waypost_status mail_hint = %v, want nil", got)
	}
}

func TestWaypostBindDoesNotOpenWaypostServiceForHint(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	service.state.autoBindAttempted = true

	bind := callServiceTool(t, service, "waypost_bind", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if got := bind["mail_hint"]; got != nil {
		t.Fatalf("waypost_bind mail_hint = %v, want nil", got)
	}
}

func TestWaypostBindRejectsInvalidAddress(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "waypost_bind", map[string]any{
		"addresses": []string{"agent-deck"},
	})
	if err == nil || !strings.Contains(err.Error(), `invalid address`) || !strings.Contains(err.Error(), `agent-deck`) {
		t.Fatalf("waypost_bind error = %v, want invalid address", err)
	}
	if got := service.state.boundAddresses; len(got) != 0 {
		t.Fatalf("boundAddresses = %v, want unchanged empty state", got)
	}
}

func TestWaypostBindAcceptsGenericAddressCharacters(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_bind", map[string]any{
		"addresses": []string{"workflow/收件箱+tag@example.com"},
	})
	if got := output["bound_addresses"]; !reflect.DeepEqual(got, []any{"workflow/收件箱+tag@example.com"}) {
		t.Fatalf("bound_addresses = %v, want generic address preserved", got)
	}
}

func TestWaypostBindExcludesGroupAddresses(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_bind", map[string]any{
		"addresses": []string{"group/review", "agent-deck/self", "group/ops"},
	})
	if got := output["bound_addresses"]; !reflect.DeepEqual(got, []any{"agent-deck/self"}) {
		t.Fatalf("bound_addresses = %v, want only personal addresses", got)
	}
	if got := output["default_sender"]; got != "agent-deck/self" {
		t.Fatalf("default_sender = %v, want agent-deck/self", got)
	}
}

func TestWaypostBindRejectsGroupDefaultSender(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "waypost_bind", map[string]any{
		"addresses":      []string{"agent-deck/self"},
		"default_sender": "group/review",
	})
	if err == nil || !strings.Contains(err.Error(), "default_sender cannot be a group address") {
		t.Fatalf("waypost_bind error = %v, want group default sender rejection", err)
	}
}

func TestWaypostBindRejectsDefaultSenderOutsideBoundState(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "waypost_bind", map[string]any{
		"addresses":      []string{"agent/allowed"},
		"default_sender": "agent/other",
	})
	if err == nil || !strings.Contains(err.Error(), "default_sender") || !strings.Contains(err.Error(), "bound addresses") {
		t.Fatalf("waypost_bind error = %v, want default sender outside bound state rejection", err)
	}
}

func TestWaypostSendRejectsInvalidOverrideSender(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "waypost_send", map[string]any{
		"to":           "agent-deck/target",
		"from_address": "agent-deck",
		"subject":      "delegate",
		"body":         "body",
	})
	if err == nil || !strings.Contains(err.Error(), `invalid address`) || !strings.Contains(err.Error(), `agent-deck`) {
		t.Fatalf("waypost_send error = %v, want invalid address", err)
	}
}

func TestWaypostSendRejectsInvalidRecipientAddress(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck",
		"subject": "delegate",
		"body":    "body",
	})
	if err == nil || !strings.Contains(err.Error(), `invalid address`) || !strings.Contains(err.Error(), `agent-deck`) {
		t.Fatalf("waypost_send error = %v, want invalid recipient address", err)
	}
}

func TestWaypostRecvRejectsInvalidExplicitAddress(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck"},
	})
	if err == nil || !strings.Contains(err.Error(), `invalid address`) || !strings.Contains(err.Error(), `agent-deck`) {
		t.Fatalf("waypost_recv error = %v, want invalid address", err)
	}
}

func TestWaypostRecvRejectsUnboundExplicitAddress(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true
	service.state.boundAddresses = []string{"agent-deck/bound"}

	err := callServiceToolExpectError(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/unbound"},
	})
	if err == nil || !strings.Contains(err.Error(), `not bound to this MCP server`) {
		t.Fatalf("waypost_recv error = %v, want unbound-address error", err)
	}
}

func TestServiceServerReturnsStableInstance(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})

	first := service.Server()
	second := service.Server()
	if first != second {
		t.Fatal("Service.Server() returned different server instances")
	}
}

func TestWaypostOverviewResourceCapabilitiesAndNotifications(t *testing.T) {
	updateCh := make(chan struct{}, 1)
	waypostService := &fakeWaypostService{t: t}
	waypostService.listClaimableFunc = func(_ context.Context, addresses []string) ([]waypost.ClaimableAddress, error) {
		if len(addresses) != 1 || addresses[0] != "agent-deck/self" {
			t.Fatalf("claimable addresses = %v, want [agent-deck/self]", addresses)
		}
		return []waypost.ClaimableAddress{{
			Address:          "agent-deck/self",
			OldestEligibleAt: "2026-04-03T00:40:00Z",
			ClaimableCount:   1,
		}}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})

	clientSession, cleanup := connectTestClientSession(t, service.Server(), updateCh)
	defer cleanup()

	caps := clientSession.InitializeResult().Capabilities
	if caps == nil || caps.Resources == nil {
		t.Fatal("resources capability missing")
	}
	if !caps.Resources.ListChanged {
		t.Fatal("resources.listChanged = false, want true")
	}
	if !caps.Resources.Subscribe {
		t.Fatal("resources.subscribe = false, want true")
	}

	if err := clientSession.Subscribe(context.Background(), &mcp.SubscribeParams{URI: waypostOverviewURI}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	callServiceTool(t, service, "waypost_bind", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})

	select {
	case <-updateCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for waypost overview update")
	}

	resources, err := clientSession.ListResources(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListResources() error = %v", err)
	}
	if len(resources.Resources) != 1 || resources.Resources[0].URI != waypostOverviewURI {
		t.Fatalf("resources = %#v, want waypost overview resource", resources.Resources)
	}

	read, err := clientSession.ReadResource(context.Background(), &mcp.ReadResourceParams{URI: waypostOverviewURI})
	if err != nil {
		t.Fatalf("ReadResource() error = %v", err)
	}
	if len(read.Contents) != 1 {
		t.Fatalf("len(ReadResource().Contents) = %d, want 1", len(read.Contents))
	}

	var overview map[string]any
	if err := json.Unmarshal([]byte(read.Contents[0].Text), &overview); err != nil {
		t.Fatalf("unmarshal overview: %v", err)
	}
	if got := overview["default_sender"]; got != "agent-deck/self" {
		t.Fatalf("default_sender = %v, want agent-deck/self", got)
	}
	if got := overview["has_claimable_delivery"]; got != true {
		t.Fatalf("has_claimable_delivery = %v, want true", got)
	}
	if got := overview["claimable_delivery_count"]; got != float64(1) {
		t.Fatalf("claimable_delivery_count = %v, want 1", got)
	}
	if got := overview["oldest_claimable_at"]; got != "2026-04-03T00:40:00Z" {
		t.Fatalf("oldest_claimable_at = %v, want oldest claimable timestamp", got)
	}
}

func TestProcessWakeSchedulerUsesLocalHintThenAgentDeckWake(t *testing.T) {
	current := time.Date(2026, 4, 3, 6, 0, 0, 0, time.UTC)
	waypostService := &fakeWaypostService{t: t}
	waypostService.listFunc = func(_ context.Context, params waypost.ListParams) ([]waypost.ListedDelivery, error) {
		t.Fatalf("wake scheduler should not use List for claimable state: %+v", params)
		return nil, nil
	}
	waypostService.listClaimableFunc = func(_ context.Context, addresses []string) ([]waypost.ClaimableAddress, error) {
		if len(addresses) != 2 {
			t.Fatalf("claimable addresses = %v, want two bound addresses", addresses)
		}
		return []waypost.ClaimableAddress{{
			Address:          "codex/self",
			OldestEligibleAt: current.Add(-4 * time.Minute).Format(time.RFC3339Nano),
			ClaimableCount:   1,
		}}, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "worker", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"worker","title":"coder-123","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			if args[9] != "worker" {
				t.Fatalf("notify target = %q, want worker", args[9])
			}
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		Now:                   func() time.Time { return current },
		DisableWakeScheduler:  true,
	})
	service.state.boundAddresses = []string{"agent-deck/worker", "codex/self"}
	service.state.defaultSender = "agent-deck/worker"
	service.state.autoBindAttempted = true
	service.state.detectedAgentDeckSession = "worker"
	service.state.detectedToolSessions = toolSessionIDs{"codex": "self"}
	server := service.Server()
	clientSession, cleanup := connectTestClientSession(t, server, nil)
	defer cleanup()
	if err := clientSession.Subscribe(context.Background(), &mcp.SubscribeParams{URI: waypostOverviewURI}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	if err := service.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler(first) error = %v", err)
	}
	if len(commandRunner.Calls()) != 0 {
		t.Fatalf("command calls after local hint = %v, want none", commandRunner.Calls())
	}

	runtime := service.wakeSchedulerState.runtimeForScope("local/agent-deck/worker", current.Add(-4*time.Minute).Format(time.RFC3339Nano))
	if runtime.LastWakeByChannel[WakeHintMCPResourceUpdated] == "" {
		t.Fatal("mcp_resource_updated was not recorded")
	}
	if runtime.LastWakeByChannel[WakeChannelAgentDeck] != "" {
		t.Fatal("agent_deck wake recorded too early")
	}

	current = current.Add(defaultWakeInterChannelGap)
	if err := service.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler(second) error = %v", err)
	}

	calls := commandRunner.Calls()
	if len(calls) != 2 {
		t.Fatalf("command calls = %v, want probe + send", calls)
	}
	if got := calls[1].Args; !isAgentDeckDeferredSend(got) {
		t.Fatalf("second command = %v, want agent-deck send", got)
	}
}

func TestProcessWakeSchedulerRetriesStoppedAgentDeckTarget(t *testing.T) {
	current := time.Date(2026, 8, 11, 2, 0, 0, 0, time.UTC)
	waypostService := &fakeWaypostService{t: t}
	waypostService.listClaimableFunc = func(_ context.Context, addresses []string) ([]waypost.ClaimableAddress, error) {
		return []waypost.ClaimableAddress{{
			Address:          "agent-deck/worker",
			OldestEligibleAt: current.Add(-4 * time.Minute).Format(time.RFC3339Nano),
			ClaimableCount:   1,
		}}, nil
	}

	probeAttempts := 0
	nudgeAttempts := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "worker", "--json"}):
			probeAttempts++
			if probeAttempts == 1 {
				return RunResult{ExitCode: 0, Stdout: `{"id":"worker","status":"stopped"}`}, nil
			}
			return RunResult{ExitCode: 0, Stdout: `{"id":"worker","status":"running"}`}, nil
		case reflect.DeepEqual(args, agentDeckDeferredSendArgs("worker", defaultNotifyMessage)):
			nudgeAttempts++
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		Now:                   func() time.Time { return current },
		DisableWakeScheduler:  true,
	})
	service.state.boundAddresses = []string{"agent-deck/worker"}
	service.state.defaultSender = "agent-deck/worker"
	service.state.autoBindAttempted = true
	service.state.detectedAgentDeckSession = "worker"
	retryDelays := []time.Duration{}
	service.notifications.retryWait = func(_ context.Context, delay time.Duration) error {
		retryDelays = append(retryDelays, delay)
		return nil
	}

	if err := service.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler() error = %v", err)
	}
	if probeAttempts != 2 || nudgeAttempts != 1 {
		t.Fatalf("probe attempts = %d, nudge attempts = %d, want 2 and 1", probeAttempts, nudgeAttempts)
	}
	if want := []time.Duration{500 * time.Millisecond}; !reflect.DeepEqual(retryDelays, want) {
		t.Fatalf("retry delays = %v, want %v", retryDelays, want)
	}
}

func TestProcessWakeSchedulerBoundsRetriesAcrossAgentDeckTargets(t *testing.T) {
	const targetedWakeBudget = 50 * time.Millisecond

	current := time.Date(2026, 8, 11, 2, 0, 0, 0, time.UTC)
	waypostService := &fakeWaypostService{t: t}
	waypostService.listClaimableFunc = func(_ context.Context, addresses []string) ([]waypost.ClaimableAddress, error) {
		return []waypost.ClaimableAddress{{
			Address:          "agent-deck/slow-a",
			OldestEligibleAt: current.Add(-4 * time.Minute).Format(time.RFC3339Nano),
			ClaimableCount:   1,
		}}, nil
	}

	var stateMu sync.Mutex
	probeCalls := map[string]int{}
	retryWaitCalls := 0
	nudgeAttempts := 0
	invalidProbeDeadline := false
	invalidRetryDeadline := false
	commandRunner := &fakeRunner{t: t, ctxHandler: func(ctx context.Context, args []string, input string) (RunResult, error) {
		switch {
		case len(args) == 5 && args[0] == "agent-deck" && args[1] == "session" && args[2] == "show":
			target := args[3]
			deadline, ok := ctx.Deadline()
			remaining := time.Duration(0)
			if ok {
				remaining = time.Until(deadline)
			}
			stateMu.Lock()
			probeCalls[target]++
			if !ok || remaining <= 0 || remaining > targetedWakeBudget+250*time.Millisecond {
				invalidProbeDeadline = true
			}
			stateMu.Unlock()
			if target == "healthy" {
				return RunResult{ExitCode: 0, Stdout: `{"id":"healthy","status":"running"}`}, nil
			}
			return RunResult{ExitCode: 0, Stdout: fmt.Sprintf(`{"id":%q,"status":"stopped"}`, target)}, nil
		case isAgentDeckDeferredSend(args):
			stateMu.Lock()
			nudgeAttempts++
			stateMu.Unlock()
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		Now:                   func() time.Time { return current },
		DisableWakeScheduler:  true,
	})
	service.targetedWakeTimeout = targetedWakeBudget
	service.state.boundAddresses = []string{"agent-deck/slow-a", "agent-deck/slow-b", "agent-deck/healthy"}
	service.state.defaultSender = "agent-deck/slow-a"
	service.state.autoBindAttempted = true
	service.state.detectedAgentDeckSession = "slow-a"
	service.notifications.retryWait = func(ctx context.Context, _ time.Duration) error {
		deadline, ok := ctx.Deadline()
		remaining := time.Duration(0)
		if ok {
			remaining = time.Until(deadline)
		}
		stateMu.Lock()
		retryWaitCalls++
		if !ok || remaining <= 0 || remaining > targetedWakeBudget+250*time.Millisecond {
			invalidRetryDeadline = true
			stateMu.Unlock()
			return errors.New("retry wait did not receive the shared targeted-wake deadline")
		}
		stateMu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}

	startedAt := time.Now()
	if err := service.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler() error = %v", err)
	}
	elapsed := time.Since(startedAt)

	if elapsed < targetedWakeBudget/2 || elapsed > targetedWakeBudget+500*time.Millisecond {
		t.Fatalf("scheduler elapsed = %v, want approximately shared budget %v", elapsed, targetedWakeBudget)
	}
	stateMu.Lock()
	defer stateMu.Unlock()
	if invalidProbeDeadline || invalidRetryDeadline {
		t.Fatalf("shared deadline invalid: probe=%t retry=%t", invalidProbeDeadline, invalidRetryDeadline)
	}
	if probeCalls["slow-a"] != 1 || probeCalls["slow-b"] != 0 || probeCalls["healthy"] != 0 {
		t.Fatalf("probe calls = %v, want only slow-a before the shared budget expires", probeCalls)
	}
	if retryWaitCalls != 1 {
		t.Fatalf("retry wait calls = %d, want 1 bounded wait", retryWaitCalls)
	}
	if nudgeAttempts != 0 {
		t.Fatalf("nudge attempts = %d, want none after the shared budget expires", nudgeAttempts)
	}
}

func TestProcessWakeSchedulerDefersQueuedAgentDeckTargetUntilLaterTick(t *testing.T) {
	current := time.Date(2026, 8, 11, 2, 0, 0, 0, time.UTC)
	waypostService := &fakeWaypostService{t: t}
	waypostService.listClaimableFunc = func(_ context.Context, addresses []string) ([]waypost.ClaimableAddress, error) {
		return []waypost.ClaimableAddress{{
			Address:          "agent-deck/worker",
			OldestEligibleAt: current.Add(-4 * time.Minute).Format(time.RFC3339Nano),
			ClaimableCount:   1,
		}}, nil
	}

	queued := true
	probeAttempts := 0
	nudgeAttempts := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "worker", "--json"}):
			probeAttempts++
			if queued {
				return RunResult{ExitCode: 0, Stdout: `{"id":"worker","status":"queued"}`}, nil
			}
			return RunResult{ExitCode: 0, Stdout: `{"id":"worker","status":"running"}`}, nil
		case reflect.DeepEqual(args, agentDeckDeferredSendArgs("worker", defaultNotifyMessage)):
			nudgeAttempts++
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		Now:                   func() time.Time { return current },
		DisableWakeScheduler:  true,
	})
	service.state.boundAddresses = []string{"agent-deck/worker"}
	service.state.defaultSender = "agent-deck/worker"
	service.state.autoBindAttempted = true
	service.state.detectedAgentDeckSession = "worker"
	service.notifications.retryWait = func(context.Context, time.Duration) error {
		t.Fatal("queued target should not use short probe retries")
		return nil
	}

	if err := service.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler(queued) error = %v", err)
	}
	if probeAttempts != 1 || nudgeAttempts != 0 {
		t.Fatalf("queued tick probe attempts = %d, nudge attempts = %d, want 1 and 0", probeAttempts, nudgeAttempts)
	}

	queued = false
	if err := service.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler(running) error = %v", err)
	}
	if probeAttempts != 2 || nudgeAttempts != 1 {
		t.Fatalf("running tick probe attempts = %d, nudge attempts = %d, want 2 and 1", probeAttempts, nudgeAttempts)
	}
}

func TestServiceCloseStopsWakeSchedulerLoop(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	entered := make(chan struct{})
	canceled := make(chan struct{})
	var closeEntered sync.Once
	var closeCanceled sync.Once
	var mu sync.Mutex
	calls := 0
	waypostService.listClaimableFunc = func(ctx context.Context, addresses []string) ([]waypost.ClaimableAddress, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		closeEntered.Do(func() { close(entered) })
		<-ctx.Done()
		closeCanceled.Do(func() { close(canceled) })
		return nil, ctx.Err()
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		WakePollInterval: time.Hour,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true
	service.state.detectedAgentDeckSession = "self"

	service.startWakeSchedulerLoop()
	waitForTestSignal(t, entered, "wake scheduler entered")
	service.Close()
	waitForTestSignal(t, canceled, "wake scheduler canceled")

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("ListClaimableAddresses calls = %d, want 1", calls)
	}
}

func TestServiceCloseCancelsWakeSchedulerAutoBind(t *testing.T) {
	t.Setenv("AGENTDECK_INSTANCE_ID", "")
	t.Setenv("CODEX_THREAD_ID", "")

	entered := make(chan struct{})
	canceled := make(chan struct{})
	var closeEntered sync.Once
	var closeCanceled sync.Once
	commandRunner := &fakeRunner{t: t, ctxHandler: func(ctx context.Context, args []string, input string) (RunResult, error) {
		if strings.Join(args, "\x00") != strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00") {
			t.Fatalf("unexpected command args: %v", args)
		}
		closeEntered.Do(func() { close(entered) })
		<-ctx.Done()
		closeCanceled.Do(func() { close(canceled) })
		return RunResult{}, ctx.Err()
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
		WakePollInterval:      time.Hour,
	})
	service.sessions.parentPID = func() int { return 1 }

	service.startBackgroundLoop(&service.wakeSchedulerLoopOnce, service.runWakeSchedulerLoop)
	waitForTestSignal(t, entered, "wake scheduler auto-bind entered")
	service.Close()
	waitForTestSignal(t, canceled, "wake scheduler auto-bind canceled")
}

func TestProcessWakeSchedulerIgnoresDisconnectedOverviewSubscriber(t *testing.T) {
	current := time.Date(2026, 4, 3, 6, 0, 0, 0, time.UTC)
	waypostService := &fakeWaypostService{t: t}
	waypostService.listFunc = func(_ context.Context, params waypost.ListParams) ([]waypost.ListedDelivery, error) {
		t.Fatalf("wake scheduler should not use List for claimable state: %+v", params)
		return nil, nil
	}
	waypostService.listClaimableFunc = func(_ context.Context, addresses []string) ([]waypost.ClaimableAddress, error) {
		if len(addresses) != 2 {
			t.Fatalf("claimable addresses = %v, want two bound addresses", addresses)
		}
		return []waypost.ClaimableAddress{{
			Address:          "codex/self",
			OldestEligibleAt: current.Add(-4 * time.Minute).Format(time.RFC3339Nano),
			ClaimableCount:   1,
		}}, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "worker", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"worker","title":"coder-123","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		Now:                   func() time.Time { return current },
		DisableWakeScheduler:  true,
	})
	service.state.boundAddresses = []string{"agent-deck/worker", "codex/self"}
	service.state.defaultSender = "agent-deck/worker"
	service.state.autoBindAttempted = true
	service.state.detectedAgentDeckSession = "worker"
	service.state.detectedToolSessions = toolSessionIDs{"codex": "self"}

	server := service.Server()
	clientSession, cleanup := connectTestClientSession(t, server, nil)
	if err := clientSession.Subscribe(context.Background(), &mcp.SubscribeParams{URI: waypostOverviewURI}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	cleanup()

	if err := service.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler() error = %v", err)
	}

	calls := commandRunner.Calls()
	if len(calls) != 2 {
		t.Fatalf("command calls = %v, want probe + send after subscriber disconnect", calls)
	}

	runtime := service.wakeSchedulerState.runtimeForScope("local/agent-deck/worker", current.Add(-4*time.Minute).Format(time.RFC3339Nano))
	if runtime.LastWakeByChannel[WakeHintMCPResourceUpdated] != "" {
		t.Fatal("mcp_resource_updated remained deliverable after disconnect")
	}
	if runtime.LastWakeByChannel[WakeChannelAgentDeck] == "" {
		t.Fatal("agent_deck wake was not recorded after disconnected subscriber cleanup")
	}
}

func TestProcessWakeSchedulerExhaustsWakeableAgentDeckTargets(t *testing.T) {
	current := time.Date(2026, 4, 3, 6, 0, 0, 0, time.UTC)
	waypostService := &fakeWaypostService{t: t}
	waypostService.listFunc = func(_ context.Context, params waypost.ListParams) ([]waypost.ListedDelivery, error) {
		t.Fatalf("wake scheduler should not use List for claimable state: %+v", params)
		return nil, nil
	}
	waypostService.listClaimableFunc = func(_ context.Context, addresses []string) ([]waypost.ClaimableAddress, error) {
		if len(addresses) != 3 {
			t.Fatalf("claimable addresses = %v, want three bound addresses", addresses)
		}
		return []waypost.ClaimableAddress{{
			Address:          "codex/self",
			OldestEligibleAt: current.Add(-4 * time.Minute).Format(time.RFC3339Nano),
			ClaimableCount:   1,
		}}, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "show", "worker-a", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"worker-a","title":"coder-a","status":"waiting"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "worker-b", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"worker-b","title":"coder-b","status":"waiting"}`}, nil
		case strings.Join(agentDeckDeferredSendArgs("worker-a", defaultNotifyMessage), "\x00"):
			return RunResult{ExitCode: 1, Stderr: "first wake failed"}, nil
		case strings.Join(agentDeckDeferredSendArgs("worker-b", defaultNotifyMessage), "\x00"):
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		Now:                   func() time.Time { return current },
		DisableWakeScheduler:  true,
	})
	service.state.boundAddresses = []string{"agent-deck/worker-a", "agent-deck/worker-b", "codex/self"}
	service.state.defaultSender = "agent-deck/worker-a"
	service.state.autoBindAttempted = true
	service.state.detectedAgentDeckSession = "worker-a"
	service.state.detectedToolSessions = toolSessionIDs{"codex": "self"}

	if err := service.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler() error = %v", err)
	}

	calls := commandRunner.Calls()
	if len(calls) != 4 {
		t.Fatalf("command calls = %v, want probe/send for both targets", calls)
	}
	if got := calls[2].Args[3]; got != "worker-b" {
		t.Fatalf("third command target = %q, want worker-b probe", got)
	}
	if got := calls[3].Args[9]; got != "worker-b" {
		t.Fatalf("fourth command target = %q, want worker-b send", got)
	}

	runtime := service.wakeSchedulerState.runtimeForScope("local/agent-deck/worker-a", current.Add(-4*time.Minute).Format(time.RFC3339Nano))
	if runtime.LastWakeByChannel[WakeChannelAgentDeck] == "" {
		t.Fatal("agent_deck wake was not recorded after second target succeeded")
	}
}

func TestWakeSchedulerDoesNotFallThroughOrImmediatelyRetryUnconfirmedNudge(t *testing.T) {
	current := time.Date(2026, 4, 3, 6, 0, 0, 0, time.UTC)
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "show", "worker-a", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"worker-a","status":"waiting"}`}, nil
		case strings.Join(agentDeckDeferredSendArgs("worker-a", defaultNotifyMessage), "\x00"):
			return RunResult{ExitCode: 1, Stdout: `{"success":false,"delivery":"typed","submitted":false,"code":"DELIVERY_FAILED"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
	})
	snapshot := wakeSnapshot{
		ScopeID:      "local/agent-deck/worker-a",
		PendingSince: current.Add(-4 * time.Minute).Format(time.RFC3339Nano),
		WakeTargets: []WakeTarget{
			{Channel: WakeChannelAgentDeck, Target: "worker-a"},
			{Channel: WakeChannelAgentDeck, Target: "worker-b"},
		},
	}
	runtime := service.wakeSchedulerState.runtimeForScope(snapshot.ScopeID, snapshot.PendingSince)
	if attempted := service.tryWakeChannel(context.Background(), snapshot, runtime, current, WakeChannelAgentDeck); !attempted {
		t.Fatal("unconfirmed nudge should count as an attempted wake")
	}
	if calls := commandRunner.Calls(); len(calls) != 2 {
		t.Fatalf("command calls = %v, want one probe and one nudge with no second-target fallback", calls)
	}

	runtime = service.wakeSchedulerState.runtimeForScope(snapshot.ScopeID, snapshot.PendingSince)
	if attempted := service.tryWakeChannel(context.Background(), snapshot, runtime, current.Add(time.Minute), WakeChannelAgentDeck); attempted {
		t.Fatal("unconfirmed nudge should enter the ordinary channel cooldown")
	}
	if calls := commandRunner.Calls(); len(calls) != 2 {
		t.Fatalf("command calls during cooldown = %v, want no immediate retry", calls)
	}

	runtime = service.wakeSchedulerState.runtimeForScope(snapshot.ScopeID, snapshot.PendingSince)
	if attempted := service.tryWakeChannel(context.Background(), snapshot, runtime, current.Add(6*time.Minute), WakeChannelAgentDeck); !attempted {
		t.Fatal("a later scheduler reminder should remain governed by the existing cooldown policy")
	}
	if calls := commandRunner.Calls(); len(calls) != 4 {
		t.Fatalf("command calls after cooldown = %v, want one later probe and nudge", calls)
	}
}

func TestWakeSchedulerDoesNotInvokeNudgeAfterProbeExhaustsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	commandRunner := &fakeRunner{t: t, ctxHandler: func(_ context.Context, args []string, input string) (RunResult, error) {
		if !reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "worker", "--json"}) {
			t.Fatalf("unexpected command after probe cancellation: %v", args)
		}
		cancel()
		return RunResult{ExitCode: 0, Stdout: `{"id":"worker","status":"waiting"}`}, nil
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
	})
	snapshot := wakeSnapshot{
		ScopeID:      "local/agent-deck/worker",
		PendingSince: time.Date(2026, 4, 3, 5, 56, 0, 0, time.UTC).Format(time.RFC3339Nano),
		WakeTargets:  []WakeTarget{{Channel: WakeChannelAgentDeck, Target: "worker"}},
	}
	runtime := service.wakeSchedulerState.runtimeForScope(snapshot.ScopeID, snapshot.PendingSince)
	if attempted := service.tryWakeChannel(ctx, snapshot, runtime, time.Date(2026, 4, 3, 6, 0, 0, 0, time.UTC), WakeChannelAgentDeck); attempted {
		t.Fatal("scheduler reported an attempt after the probe exhausted its context")
	}
	if calls := commandRunner.Calls(); len(calls) != 1 {
		t.Fatalf("command calls = %v, want probe only", calls)
	}
	runtime = service.wakeSchedulerState.runtimeForScope(snapshot.ScopeID, snapshot.PendingSince)
	if runtime.LastWakeByChannel[WakeChannelAgentDeck] != "" {
		t.Fatal("pre-attempt cancellation must not record an agent-deck wake")
	}
}

func TestProcessWakeSchedulerFallsThroughWhenWaypostOverviewUpdateFails(t *testing.T) {
	current := time.Date(2026, 4, 3, 6, 0, 0, 0, time.UTC)
	waypostService := &fakeWaypostService{t: t}
	waypostService.listClaimableFunc = func(_ context.Context, addresses []string) ([]waypost.ClaimableAddress, error) {
		return []waypost.ClaimableAddress{{
			Address:          "codex/self",
			OldestEligibleAt: current.Add(-4 * time.Minute).Format(time.RFC3339Nano),
			ClaimableCount:   1,
		}}, nil
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "worker", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"worker","title":"coder-123","status":"waiting"}`}, nil
		case isAgentDeckDeferredSend(args):
			return RunResult{ExitCode: 0}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		Now:                   func() time.Time { return current },
		DisableWakeScheduler:  true,
	})
	service.state.boundAddresses = []string{"agent-deck/worker", "codex/self"}
	service.state.defaultSender = "agent-deck/worker"
	service.state.autoBindAttempted = true
	service.state.detectedAgentDeckSession = "worker"
	service.state.detectedToolSessions = toolSessionIDs{"codex": "self"}
	service.waypostOverviewEmitter = func(context.Context) notificationOutcome {
		return notificationOutcome{
			Status: "failed",
			Scheme: string(WakeHintMCPResourceUpdated),
			Err:    fmt.Errorf("resource update failed"),
		}
	}

	server := service.Server()
	clientSession, cleanup := connectTestClientSession(t, server, nil)
	defer cleanup()
	if err := clientSession.Subscribe(context.Background(), &mcp.SubscribeParams{URI: waypostOverviewURI}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	if err := service.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler() error = %v", err)
	}

	calls := commandRunner.Calls()
	if len(calls) != 2 {
		t.Fatalf("command calls = %v, want probe + send after local hint failure", calls)
	}

	runtime := service.wakeSchedulerState.runtimeForScope("local/agent-deck/worker", current.Add(-4*time.Minute).Format(time.RFC3339Nano))
	if runtime.LastWakeByChannel[WakeHintMCPResourceUpdated] != "" {
		t.Fatal("mcp_resource_updated should not be recorded after failed local hint")
	}
	if runtime.LastWakeByChannel[WakeChannelAgentDeck] == "" {
		t.Fatal("agent_deck wake should be recorded after local hint failure")
	}
}

func TestAgentDeckRequireSessionReturnsReadyAndNotFoundWithoutRestart(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "show", "worker-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"worker-1","title":"worker","status":"waiting","group":"team","path":"/tmp"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "missing-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	found := callServiceTool(t, service, "agent_deck_require_session", map[string]any{
		"session_ref":  "worker-ref",
		"workdir":      "/tmp",
		"auto_restart": false,
	})
	if got := found["status"]; got != "ready" {
		t.Fatalf("status = %v, want ready", got)
	}
	if got := found["session_id"]; got != "worker-1" {
		t.Fatalf("session_id = %v, want worker-1", got)
	}
	if got := found["session_ref"]; got != "worker-ref" {
		t.Fatalf("session_ref = %v, want worker-ref", got)
	}
	if _, ok := found["results"]; ok {
		t.Fatalf("single-session response unexpectedly includes results: %v", found)
	}

	notFound := callServiceTool(t, service, "agent_deck_require_session", map[string]any{
		"session_ref":  "missing-ref",
		"workdir":      "/tmp",
		"auto_restart": false,
	})
	if notFound["status"] != "not_found" || notFound["session_ref"] != "missing-ref" || notFound["started_session"] != false {
		t.Fatalf("not-found response = %v", notFound)
	}
}

func TestAgentDeckRequireSessionBatchReturnsOrderedIndependentInspectionResults(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "show", "found-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"worker-1","title":"worker","status":"waiting","path":"/tmp"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "missing-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "runner-error", "--json"}, "\x00"):
			return RunResult{}, errors.New("agent-deck is unavailable")
		case strings.Join([]string{"agent-deck", "session", "show", "invalid-json", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: "not json"}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_require_session", map[string]any{
		"sessions":     []string{"found-ref", "missing-ref", "runner-error", "invalid-json"},
		"workdir":      "/tmp",
		"auto_restart": false,
	})
	results, ok := output["results"].([]any)
	if !ok {
		t.Fatalf("results = %#v, want ordered array", output["results"])
	}
	if len(results) != 4 {
		t.Fatalf("results length = %d, want 4", len(results))
	}

	found, ok := results[0].(map[string]any)
	if !ok {
		t.Fatalf("first result = %#v, want map", results[0])
	}
	if got := found["status"]; got != "ready" {
		t.Fatalf("first result status = %v, want ready", got)
	}
	if got := found["session_ref"]; got != "found-ref" {
		t.Fatalf("first result session_ref = %v, want found-ref", got)
	}

	missing, ok := results[1].(map[string]any)
	if !ok {
		t.Fatalf("second result = %#v, want map", results[1])
	}
	if missing["status"] != "not_found" || missing["session_ref"] != "missing-ref" || missing["started_session"] != false {
		t.Fatalf("second result = %v, want structured not_found", missing)
	}

	for i, session := range []string{"runner-error", "invalid-json"} {
		result, ok := results[i+2].(map[string]any)
		if !ok {
			t.Fatalf("result %d = %#v, want map", i+3, results[i+2])
		}
		if got := result["status"]; got != "error" {
			t.Fatalf("result %d status = %v, want error", i+3, got)
		}
		if got := result["session_ref"]; got != session {
			t.Fatalf("result %d session_ref = %v, want %s", i+3, got, session)
		}
		if got, ok := result["error"].(string); !ok || got == "" {
			t.Fatalf("result %d error = %#v, want non-empty string", i+3, result["error"])
		}
	}
	if got := len(commandRunner.Calls()); got != 4 {
		t.Fatalf("command calls = %d, want 4", got)
	}
}

func TestAgentDeckRequireSessionAutoRestartFalseDoesNotStartInactiveTarget(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		if strings.Join(args, "\x00") != strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00") {
			t.Fatalf("auto_restart=false must not start the target: %v", args)
		}
		return RunResult{ExitCode: 0, Stdout: `{"id":"session-1","title":"coder-123","status":"stopped","path":"/tmp"}`}, nil
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_require_session", map[string]any{
		"session_ref":  "coder-ref",
		"workdir":      "/tmp",
		"auto_restart": false,
	})
	if output["status"] != "not_ready" || output["session_status"] != "stopped" || output["started_session"] != false {
		t.Fatalf("inspection output = %v", output)
	}
}

func TestAgentDeckRequireSessionReturnsActiveTargetWithoutStart(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-1","title":"coder-123","status":"waiting","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_require_session", map[string]any{
		"session_ref": "coder-ref",
		"workdir":     "/tmp",
	})

	if got := output["status"]; got != "ready" {
		t.Fatalf("status = %v, want ready", got)
	}
	if got := output["created_target"]; got != false {
		t.Fatalf("created_target = %v, want false", got)
	}
	if got := output["started_session"]; got != false {
		t.Fatalf("started_session = %v, want false", got)
	}
	if got := output["notify_needed"]; got != true {
		t.Fatalf("notify_needed = %v, want true", got)
	}
	if got := output["startup_instruction_status"]; got != "not_needed_existing_session" {
		t.Fatalf("startup_instruction_status = %v, want not_needed_existing_session", got)
	}
}

func TestAgentDeckRequireSessionStartsInactiveTarget(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-1","title":"coder-123","status":"stopped","path":"/tmp"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "start", "--json", "session-1"}, "\x00"):
			return RunResult{ExitCode: 0}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-1","title":"coder-123","status":"waiting","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_require_session", map[string]any{
		"session_ref": "coder-ref",
		"workdir":     "/tmp",
	})

	if got := output["status"]; got != "ready" {
		t.Fatalf("status = %v, want ready", got)
	}
	if got := output["created_target"]; got != false {
		t.Fatalf("created_target = %v, want false", got)
	}
	if got := output["started_session"]; got != true {
		t.Fatalf("started_session = %v, want true", got)
	}
	if got := output["notify_needed"]; got != false {
		t.Fatalf("notify_needed = %v, want false", got)
	}
	if got := output["startup_instruction_status"]; got != "started" {
		t.Fatalf("startup_instruction_status = %v, want started", got)
	}
	if _, found := output["recovery_required"]; found {
		t.Fatalf("created output unexpectedly includes recovery_required: %v", output)
	}
	if _, found := output["verification"]; found {
		t.Fatalf("created output unexpectedly includes verification: %v", output)
	}
}

func TestAgentDeckRequireSessionReturnsRecoveryAfterConfirmedStart(t *testing.T) {
	otherWorkdir := t.TempDir()
	tests := []struct {
		name      string
		show      RunResult
		showErr   error
		wantState string
	}{
		{
			name:      "lookup error",
			showErr:   errors.New("session show unavailable"),
			wantState: "post_start_lookup_failed",
		},
		{
			name:      "lookup missing",
			show:      RunResult{ExitCode: 2, Stderr: "not found"},
			wantState: "post_start_disappeared",
		},
		{
			name:      "invalid output",
			show:      RunResult{ExitCode: 0, Stdout: "not JSON"},
			wantState: "post_start_output_unparseable",
		},
		{
			name:      "id mismatch",
			show:      RunResult{ExitCode: 0, Stdout: `{"id":"session-other","title":"coder-123","status":"waiting","path":"/tmp"}`},
			wantState: "post_start_output_unparseable",
		},
		{
			name:      "still not ready",
			show:      RunResult{ExitCode: 0, Stdout: `{"id":"session-1","title":"coder-123","status":"unknown","path":"/tmp"}`},
			wantState: "post_start_not_ready",
		},
		{
			name:      "workdir mismatch",
			show:      RunResult{ExitCode: 0, Stdout: `{"id":"session-1","title":"coder-123","status":"waiting","path":` + jsonString(t, otherWorkdir) + `}`},
			wantState: "post_start_path_mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch {
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "coder-ref", "--json"}):
					return RunResult{ExitCode: 0, Stdout: `{"id":"session-1","title":"coder-123","status":"stopped","path":"/tmp"}`}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "start", "--json", "session-1"}):
					return RunResult{ExitCode: 0}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "session-1", "--json"}):
					return test.show, test.showErr
				default:
					t.Fatalf("unexpected command args: %v", args)
					return RunResult{}, nil
				}
			}}
			service := newService(Options{
				WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
				CommandRunner:         commandRunner,
			})
			service.state.autoBindAttempted = true

			output := callServiceTool(t, service, "agent_deck_require_session", map[string]any{
				"session_ref": "coder-ref",
				"workdir":     "/tmp",
			})
			if output["status"] != "ready_unverified" || output["started_session"] != true || output["recovery_required"] != true {
				t.Fatalf("post-start recovery output = %v", output)
			}
			if output["session_id"] != "session-1" || output["session_status"] != "stopped" || output["notify_needed"] != false || output["startup_instruction_status"] != "started" {
				t.Fatalf("post-start recovery retained state = %v", output)
			}
			verification, ok := output["verification"].(map[string]any)
			if !ok || verification["state"] != test.wantState {
				t.Fatalf("post-start verification = %v, want state %q", output["verification"], test.wantState)
			}
			if calls := commandRunner.Calls(); len(calls) != 3 {
				t.Fatalf("post-start command calls = %v, want lookup + one start + one readback", calls)
			}
		})
	}
}

func TestAgentDeckRequireSessionBatchKeepsConfirmedStartRecoveryAndContinues(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "stopped-ref", "--json"}):
			return RunResult{ExitCode: 0, Stdout: `{"id":"stopped-1","title":"stopped","status":"stopped","path":"/tmp"}`}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "start", "--json", "stopped-1"}):
			return RunResult{ExitCode: 0}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "stopped-1", "--json"}):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "active-ref", "--json"}):
			return RunResult{ExitCode: 0, Stdout: `{"id":"active-1","title":"active","status":"waiting","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_require_session", map[string]any{
		"sessions": []string{"stopped-ref", "active-ref"},
		"workdir":  "/tmp",
	})
	results, ok := output["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("batch require results = %v", output["results"])
	}
	recovery := results[0].(map[string]any)
	if recovery["status"] != "ready_unverified" || recovery["started_session"] != true || recovery["recovery_required"] != true {
		t.Fatalf("batch recovery result = %v", recovery)
	}
	if verification := recovery["verification"].(map[string]any); verification["state"] != "post_start_disappeared" {
		t.Fatalf("batch recovery verification = %v", verification)
	}
	ready := results[1].(map[string]any)
	if ready["status"] != "ready" || ready["started_session"] != false {
		t.Fatalf("continued batch result = %v", ready)
	}
	if calls := commandRunner.Calls(); len(calls) != 4 {
		t.Fatalf("batch command calls = %v, want recovery item followed by active lookup", calls)
	}
}

func TestAgentDeckRequireSessionBatchReturnsOrderedIndependentResults(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "show", "active-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"active-1","title":"active","status":"waiting","path":"/tmp"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "missing-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "stopped-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"stopped-1","title":"stopped","status":"stopped","path":"/tmp"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "start", "--json", "stopped-1"}, "\x00"):
			return RunResult{ExitCode: 0}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "stopped-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"stopped-1","title":"stopped","status":"waiting","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_require_session", map[string]any{
		"sessions": []string{"active-ref", "missing-ref", "stopped-ref"},
		"workdir":  "/tmp",
	})
	results, ok := output["results"].([]any)
	if !ok {
		t.Fatalf("results = %#v, want ordered array", output["results"])
	}
	if len(results) != 3 {
		t.Fatalf("results length = %d, want 3", len(results))
	}

	active, ok := results[0].(map[string]any)
	if !ok {
		t.Fatalf("first result = %#v, want map", results[0])
	}
	if got := active["status"]; got != "ready" {
		t.Fatalf("first result status = %v, want ready", got)
	}
	if got := active["session_ref"]; got != "active-ref" {
		t.Fatalf("first result session_ref = %v, want active-ref", got)
	}
	if got := active["started_session"]; got != false {
		t.Fatalf("first result started_session = %v, want false", got)
	}

	missing, ok := results[1].(map[string]any)
	if !ok {
		t.Fatalf("second result = %#v, want map", results[1])
	}
	if got := missing["status"]; got != "not_found" {
		t.Fatalf("second result status = %v, want not_found", got)
	}
	if got := missing["session_ref"]; got != "missing-ref" {
		t.Fatalf("second result session_ref = %v, want missing-ref", got)
	}
	if got := missing["started_session"]; got != false {
		t.Fatalf("second result started_session = %v, want false", got)
	}

	stopped, ok := results[2].(map[string]any)
	if !ok {
		t.Fatalf("third result = %#v, want map", results[2])
	}
	if got := stopped["status"]; got != "ready" {
		t.Fatalf("third result status = %v, want ready", got)
	}
	if got := stopped["started_session"]; got != true {
		t.Fatalf("third result started_session = %v, want true", got)
	}
	if got := len(commandRunner.Calls()); got != 5 {
		t.Fatalf("command calls = %d, want 5", got)
	}
}

func TestAgentDeckRequireSessionBatchValidatesInput(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	for _, test := range []struct {
		name string
		args map[string]any
		want string
	}{
		{name: "empty batch", args: map[string]any{"sessions": []string{}, "workdir": "/tmp"}, want: "sessions must contain at least one session"},
		{name: "combined inputs", args: map[string]any{"session_ref": "worker-a", "sessions": []string{"worker-b"}, "workdir": "/tmp"}, want: "sessions cannot be combined"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := callServiceToolExpectError(t, service, "agent_deck_require_session", test.args)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("agent_deck_require_session error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestAgentDeckRequireSessionRejectsStartupInstruction(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "agent_deck_require_session", map[string]any{
		"session_ref":         "coder-ref",
		"startup_instruction": "listen now",
		"workdir":             "/tmp",
	})
	if err == nil || !strings.Contains(err.Error(), "unexpected additional properties") {
		t.Fatalf("agent_deck_require_session error = %v, want schema-level startup_instruction validation", err)
	}
}

func TestAgentDeckRequireSessionRequiresExplicitWorkdir(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "agent_deck_require_session", map[string]any{
		"session_ref": "coder-ref",
	})
	if err == nil || !strings.Contains(err.Error(), "workdir") {
		t.Fatalf("agent_deck_require_session error = %v, want workdir validation", err)
	}
}

func TestAgentDeckRequireSessionValidatesTargetBeforeWorkdir(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "agent_deck_require_session", map[string]any{
		"workdir": "/path/that/does/not/exist",
	})
	if err == nil || !strings.Contains(err.Error(), "session_id or session_ref is required") {
		t.Fatalf("agent_deck_require_session error = %v, want missing target validation", err)
	}
}

func TestAgentDeckRequireSessionPropagatesOperationalLookupFailure(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "session lookup failed"}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "agent_deck_require_session", map[string]any{
		"session_ref": "coder-ref",
		"workdir":     "/tmp",
	})
	if err == nil || !strings.Contains(err.Error(), "agent-deck session show failed with exit code 1") {
		t.Fatalf("agent_deck_require_session error = %v, want operational lookup failure", err)
	}
}

func TestProbeSessionShowBestEffortUsesStructuredMissingResult(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		if strings.Join(args, "\x00") != strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00") {
			t.Fatalf("unexpected command args: %v", args)
		}
		return RunResult{ExitCode: 2, Stdout: `{"success":false}`}, nil
	}}

	manager := newSessionManager(commandRunner, &serverState{})
	probe, err := manager.probeSessionShowBestEffort(context.Background(), "coder-ref")
	if err != nil {
		t.Fatalf("probeSessionShowBestEffort() error = %v", err)
	}
	if probe.Status != sessionShowProbeNotFound {
		t.Fatalf("probe status = %v, want %v", probe.Status, sessionShowProbeNotFound)
	}
}

func TestProbeSessionShowBestEffortDoesNotSniffMissingText(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		if strings.Join(args, "\x00") != strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00") {
			t.Fatalf("unexpected command args: %v", args)
		}
		return RunResult{ExitCode: 1, Stderr: "session not found"}, nil
	}}

	manager := newSessionManager(commandRunner, &serverState{})
	probe, err := manager.probeSessionShowBestEffort(context.Background(), "coder-ref")
	if err != nil {
		t.Fatalf("probeSessionShowBestEffort() error = %v", err)
	}
	if probe.Status != sessionShowProbeUnknown {
		t.Fatalf("probe status = %v, want %v", probe.Status, sessionShowProbeUnknown)
	}
}

func TestAgentDeckRequireSessionRejectsExistingSessionWithoutPath(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-1","title":"coder-123","status":"stopped"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "agent_deck_require_session", map[string]any{
		"session_ref": "coder-ref",
		"workdir":     "/tmp",
	})
	if err == nil || !strings.Contains(err.Error(), "existing session path unavailable") {
		t.Fatalf("agent_deck_require_session error = %v, want existing session path unavailable", err)
	}
}

func TestAgentDeckRequireSessionRejectsExtraFields(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "agent_deck_require_session", map[string]any{
		"session_ref":       "coder-ref",
		"workdir":           "/tmp",
		"ensure_title":      "coder-ref",
		"parent_session_id": "planner-1",
	})
	if err == nil || !strings.Contains(err.Error(), "unexpected additional properties") {
		t.Fatalf("agent_deck_require_session error = %v, want schema-level extra field validation", err)
	}
}

func TestAgentDeckCreateSessionCreatesTargetWithoutDefaultStartupInstruction(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "planner-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"planner-1","title":"planner","status":"waiting","path":"/tmp","group":"planning"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "list", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"groups":[{"path":"planning"}]}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex --model gpt-5.4 --ask-for-approval on-request", "--group", "planning", "--parent", "planner-1", workdir}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "session-2", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","path":"/tmp","group":"planning","parent_session_id":"planner-1"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":      "coder-ref",
		"ensure_cmd":        "codex --model gpt-5.4 --ask-for-approval on-request",
		"parent_session_id": "planner-1",
		"workdir":           "/tmp",
	})

	if got := output["created_target"]; got != true {
		t.Fatalf("created_target = %v, want true", got)
	}
	if got := output["status"]; got != "created" {
		t.Fatalf("status = %v, want created", got)
	}
	if got := output["startup_instruction_status"]; got != "started" {
		t.Fatalf("startup_instruction_status = %v, want started", got)
	}
}

func TestAgentDeckCreateSessionPreservesExactTitle(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	title := " coder-ref "
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", title, "--json"}):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "launch", "--json", "--title", title, "--cmd", "codex", "--no-parent", workdir}):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2"}`}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "session-2", "--json"}):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":" coder-ref ","status":"waiting","path":` + jsonString(t, workdir) + `}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":   title,
		"ensure_cmd":     "codex",
		"no_parent_link": true,
		"workdir":        workdir,
	})
	if output["status"] != "created" || output["title"] != title {
		t.Fatalf("create output = %v, want created session with exact title %q", output, title)
	}
}

func TestAgentDeckCreateSessionReturnsRecoveryAfterConfirmedLaunch(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	otherWorkdir := t.TempDir()
	valid := `{"id":"session-2","title":"coder-ref","status":"waiting","path":` + jsonString(t, workdir) + `}`
	tests := []struct {
		name              string
		show              RunResult
		showErr           error
		wantState         string
		wantReceiptTarget bool
	}{
		{
			name:      "lookup error",
			showErr:   errors.New("session show unavailable"),
			wantState: "post_create_lookup_failed",
		},
		{
			name:      "lookup missing",
			show:      RunResult{ExitCode: 2, Stderr: "not found"},
			wantState: "post_create_lookup_failed",
		},
		{
			name:              "id mismatch",
			show:              RunResult{ExitCode: 0, Stdout: `{"id":"session-other","title":"coder-ref","status":"waiting","path":` + jsonString(t, workdir) + `}`},
			wantState:         "post_create_identity_mismatch",
			wantReceiptTarget: true,
		},
		{
			name:              "missing id",
			show:              RunResult{ExitCode: 0, Stdout: `{"title":"coder-ref","status":"waiting","path":` + jsonString(t, workdir) + `}`},
			wantState:         "post_create_identity_mismatch",
			wantReceiptTarget: true,
		},
		{
			name:      "title mismatch",
			show:      RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"other","status":"waiting","path":` + jsonString(t, workdir) + `}`},
			wantState: "post_create_identity_mismatch",
		},
		{
			name:      "title whitespace mismatch",
			show:      RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":" coder-ref ","status":"waiting","path":` + jsonString(t, workdir) + `}`},
			wantState: "post_create_identity_mismatch",
		},
		{
			name:      "parent mismatch",
			show:      RunResult{ExitCode: 0, Stdout: valid[:len(valid)-1] + `,"parent_session_id":"planner-1"}`},
			wantState: "post_create_identity_mismatch",
		},
		{
			name:      "group mismatch",
			show:      RunResult{ExitCode: 0, Stdout: valid[:len(valid)-1] + `,"group":"unexpected"}`},
			wantState: "post_create_group_mismatch",
		},
		{
			name:      "workdir mismatch",
			show:      RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","path":` + jsonString(t, otherWorkdir) + `}`},
			wantState: "path_mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			launches := 0
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch {
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "coder-ref", "--json"}):
					return RunResult{ExitCode: 2, Stderr: "not found"}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex", "--no-parent", workdir}):
					launches++
					return RunResult{ExitCode: 0, Stdout: `{"id":"session-2"}`}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "session-2", "--json"}):
					return test.show, test.showErr
				default:
					t.Fatalf("unexpected command args: %v", args)
					return RunResult{}, nil
				}
			}}

			service := newService(Options{
				WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
				CommandRunner:         commandRunner,
			})
			service.state.autoBindAttempted = true

			output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
				"ensure_title":   "coder-ref",
				"ensure_cmd":     "codex",
				"no_parent_link": true,
				"workdir":        workdir,
			})
			if output["status"] != "created_unverified" || output["created_target"] != true || output["started_session"] != true || output["recovery_required"] != true {
				t.Fatalf("create recovery output = %v", output)
			}
			verification, ok := output["verification"].(map[string]any)
			if !ok || verification["state"] != test.wantState {
				t.Fatalf("create recovery verification = %v, want state %q", output["verification"], test.wantState)
			}
			if test.wantReceiptTarget {
				if output["session_id"] != "session-2" {
					t.Fatalf("recovery session_id = %v, want launch receipt id session-2", output["session_id"])
				}
				addresses, ok := output["addresses"].([]any)
				if !ok || !slices.Equal(addresses, []any{"agent-deck/session-2"}) {
					t.Fatalf("recovery addresses = %v, want launch receipt address", output["addresses"])
				}
				if verification["observed_path"] != nil {
					t.Fatalf("recovery observed_path = %v, want nil for an untrusted refreshed identity", verification["observed_path"])
				}
			}
			if launches != 1 {
				t.Fatalf("launch count = %d, want 1", launches)
			}
		})
	}
}

func TestAgentDeckCreateSessionReturnsRecoveryForUnusableLaunchReceipt(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	launches := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "coder-ref", "--json"}):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex", "--no-parent", workdir}):
			launches++
			return RunResult{ExitCode: 0, Stdout: `{"success":true}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":   "coder-ref",
		"ensure_cmd":     "codex",
		"no_parent_link": true,
		"workdir":        workdir,
	})
	if output["status"] != "create_recovery_required" || output["created_target"] != nil || output["started_session"] != nil || output["recovery_required"] != true || output["session_id"] != nil {
		t.Fatalf("create recovery output = %v", output)
	}
	verification, ok := output["verification"].(map[string]any)
	if !ok || verification["state"] != "create_output_unparseable" {
		t.Fatalf("create recovery verification = %v", output["verification"])
	}
	if launches != 1 {
		t.Fatalf("launch count = %d, want 1", launches)
	}
}

func TestAgentDeckCreateSessionReturnsRecoveryWhenRootGroupMoveFails(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	launches := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "coder-ref", "--json"}):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "planner-1", "--json"}):
			return RunResult{ExitCode: 0, Stdout: `{"id":"planner-1","title":"planner","status":"waiting","path":` + jsonString(t, workdir) + `}`}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex", "--parent", "planner-1", workdir}):
			launches++
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2"}`}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "group", "move", "session-2", ""}):
			return RunResult{}, errors.New("group move unavailable")
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":      "coder-ref",
		"ensure_cmd":        "codex",
		"parent_session_id": "planner-1",
		"workdir":           workdir,
	})
	if output["status"] != "created_unverified" || output["recovery_required"] != true {
		t.Fatalf("group-move recovery output = %v", output)
	}
	verification, ok := output["verification"].(map[string]any)
	if !ok || verification["state"] != "post_create_group_move_failed" {
		t.Fatalf("group-move recovery verification = %v", output["verification"])
	}
	if launches != 1 {
		t.Fatalf("launch count = %d, want 1", launches)
	}
}

func TestAgentDeckCreateSessionMovesRootGroupParentChildBackToRoot(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "planner-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"planner-1","title":"planner","status":"waiting","path":"/tmp","group":""}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex --model gpt-5.4 --ask-for-approval on-request", "--parent", "planner-1", workdir}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"tmp","path":"/tmp","parent_session_id":"planner-1"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "move", "session-2", ""}, "\x00"):
			return RunResult{ExitCode: 0}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "session-2", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"","path":"/tmp","parent_session_id":"planner-1"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":      "coder-ref",
		"ensure_cmd":        "codex --model gpt-5.4 --ask-for-approval on-request",
		"parent_session_id": "planner-1",
		"workdir":           "/tmp",
	})

	if got := output["created_target"]; got != true {
		t.Fatalf("created_target = %v, want true", got)
	}
	if got := output["group"]; got != nil {
		t.Fatalf("group = %v, want nil for root group", got)
	}
}

func TestAgentDeckCreateSessionRejectsExistingTarget(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-9","title":"coder-ref","status":"waiting","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":   "coder-ref",
		"ensure_cmd":     "codex --model gpt-5.4 --ask-for-approval on-request",
		"no_parent_link": true,
		"workdir":        "/tmp",
	})
	if err == nil || !strings.Contains(err.Error(), "target session already exists") {
		t.Fatalf("agent_deck_create_session error = %v, want existing target validation", err)
	}
}

func TestAgentDeckCreateSessionRejectsExistingTargetWithMismatchedWorkdir(t *testing.T) {
	requestedWorkdir := t.TempDir()
	existingWorkdir := t.TempDir()
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			payload, err := json.Marshal(map[string]string{
				"id":     "session-9",
				"title":  "coder-ref",
				"status": "waiting",
				"path":   existingWorkdir,
			})
			if err != nil {
				t.Fatalf("marshal session show payload: %v", err)
			}
			return RunResult{ExitCode: 0, Stdout: string(payload)}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":   "coder-ref",
		"ensure_cmd":     "codex --model gpt-5.4 --ask-for-approval on-request",
		"no_parent_link": true,
		"workdir":        requestedWorkdir,
	})
	if err == nil || !strings.Contains(err.Error(), "session path mismatch") {
		t.Fatalf("agent_deck_create_session error = %v, want workdir mismatch validation", err)
	}
}

func TestAgentDeckCreateSessionAllowsDetachedCreateWithoutGroup(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex --model gpt-5.4 --ask-for-approval on-request", "--no-parent", workdir}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","path":"/tmp"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "session-2", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":   "coder-ref",
		"ensure_cmd":     "codex --model gpt-5.4 --ask-for-approval on-request",
		"no_parent_link": true,
		"workdir":        "/tmp",
	})
	if got := output["created_target"]; got != true {
		t.Fatalf("created_target = %v, want true", got)
	}
	if got := output["started_session"]; got != true {
		t.Fatalf("started_session = %v, want true", got)
	}
	if got := output["path"]; got != "/tmp" {
		t.Fatalf("path = %v, want /tmp", got)
	}
}

func TestAgentDeckCreateSessionDerivesChildGroupFromChildParentSession(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "child-planner", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"child-planner","title":"planner-child","status":"waiting","path":"/tmp","group":"planning/active","parent_session_id":"root-planner"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "list", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"groups":[{"path":"planning"},{"path":"planning/active"}]}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "create", "planner-child", "--parent", "planning/active"}, "\x00"):
			return RunResult{ExitCode: 0}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex --model gpt-5.4 --ask-for-approval on-request", "--group", "planning/active/planner-child", "--no-parent", workdir}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"planning/active/planner-child","path":"/tmp"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "session-2", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"planning/active/planner-child","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":      "coder-ref",
		"ensure_cmd":        "codex --model gpt-5.4 --ask-for-approval on-request",
		"parent_session_id": "child-planner",
		"workdir":           "/tmp",
	})
	if got := output["created_target"]; got != true {
		t.Fatalf("created_target = %v, want true", got)
	}
	if got := output["group"]; got != "planning/active/planner-child" {
		t.Fatalf("group = %v, want planning/active/planner-child", got)
	}
}

func TestAgentDeckCreateSessionDerivesTopLevelGroupFromChildParentWithoutGroup(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "child-planner", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"child-planner","title":"planner-child","status":"waiting","path":"/tmp","group":"","parent_session_id":"root-planner"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "list", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"groups":[]}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "create", "planner-child"}, "\x00"):
			return RunResult{ExitCode: 0}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex --model gpt-5.4 --ask-for-approval on-request", "--group", "planner-child", "--no-parent", workdir}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"planner-child","path":"/tmp"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "session-2", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"planner-child","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":      "coder-ref",
		"ensure_cmd":        "codex --model gpt-5.4 --ask-for-approval on-request",
		"parent_session_id": "child-planner",
		"workdir":           "/tmp",
	})
	if got := output["created_target"]; got != true {
		t.Fatalf("created_target = %v, want true", got)
	}
	if got := output["group"]; got != "planner-child" {
		t.Fatalf("group = %v, want planner-child", got)
	}
}

func TestAgentDeckCreateSessionDropsChildParentLinkForExplicitGroupPath(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "child-planner", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"child-planner","title":"planner-child","status":"waiting","path":"/tmp","group":"planning/active","parent_session_id":"root-planner"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "list", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"groups":[{"path":"planning"},{"path":"planning/active"},{"path":"reviews"},{"path":"reviews/ready"}]}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex --model gpt-5.4 --ask-for-approval on-request", "--group", "reviews/ready", "--no-parent", workdir}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"reviews/ready","path":"/tmp"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "session-2", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"reviews/ready","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":      "coder-ref",
		"ensure_cmd":        "codex --model gpt-5.4 --ask-for-approval on-request",
		"parent_session_id": "child-planner",
		"group_path":        "reviews/ready",
		"workdir":           "/tmp",
	})
	if got := output["created_target"]; got != true {
		t.Fatalf("created_target = %v, want true", got)
	}
	if got := output["group"]; got != "reviews/ready" {
		t.Fatalf("group = %v, want reviews/ready", got)
	}
}

func TestAgentDeckCreateSessionDerivesGroupFromGroupParentSession(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "planner-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"planner-1","title":"planner","status":"waiting","group":"planning","path":"/tmp"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "list", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"groups":[{"path":"planning"}]}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "create", "coder-review", "--parent", "planning"}, "\x00"):
			return RunResult{ExitCode: 0}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex --model gpt-5.4 --ask-for-approval on-request", "--group", "planning/coder-review", workdir}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"planning/coder-review","path":"/tmp"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "session-2", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"planning/coder-review","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":            "coder-ref",
		"ensure_cmd":              "codex --model gpt-5.4 --ask-for-approval on-request",
		"group_parent_session_id": "planner-1",
		"child_group_name":        "Coder Review",
		"workdir":                 "/tmp",
	})
	if got := output["group"]; got != "planning/coder-review" {
		t.Fatalf("group = %v, want planning/coder-review", got)
	}
}

func TestAgentDeckCreateSessionDoesNotCreateGroupWhenCreateValidationFails(t *testing.T) {
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		t.Fatalf("unexpected command args: %v", args)
		return RunResult{}, nil
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title": "coder-ref",
		"group_path":   "reviews/new-group",
		"workdir":      "/tmp",
	})
	if err == nil || !strings.Contains(err.Error(), "ensure_cmd is required") {
		t.Fatalf("agent_deck_create_session error = %v, want ensure_cmd is required", err)
	}
}

func TestAgentDeckRequireSessionAcceptsSymlinkedEquivalentWorkdir(t *testing.T) {
	baseDir := t.TempDir()
	realDir := filepath.Join(baseDir, "real")
	if err := os.Mkdir(realDir, 0o755); err != nil {
		t.Fatalf("Mkdir(realDir) error = %v", err)
	}
	symlinkDir := filepath.Join(baseDir, "linked")
	if err := os.Symlink(realDir, symlinkDir); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink privileges unavailable on Windows: %v", err)
		}
		t.Fatalf("Symlink() error = %v", err)
	}

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: fmt.Sprintf(`{"id":"session-1","title":"coder-123","status":"stopped","path":%q}`, realDir)}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "start", "--json", "session-1"}, "\x00"):
			return RunResult{ExitCode: 0}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: fmt.Sprintf(`{"id":"session-1","title":"coder-123","status":"waiting","path":%q}`, realDir)}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_require_session", map[string]any{
		"session_ref": "coder-ref",
		"workdir":     symlinkDir,
	})
	if got := output["status"]; got != "ready" {
		t.Fatalf("status = %v, want ready", got)
	}
	if got := output["started_session"]; got != true {
		t.Fatalf("started_session = %v, want true", got)
	}
}

func TestAgentDeckCreateSessionCreatesTargetWithGroupPathAndNoParentLink(t *testing.T) {
	workdir := canonicalTestWorkdir(t, "/tmp")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "coder-ref", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "list", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"groups":[]}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "group", "create", "reviews"}, "\x00"):
			return RunResult{ExitCode: 0}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "launch", "--json", "--title", "coder-ref", "--cmd", "codex --model gpt-5.4 --ask-for-approval on-request", "--group", "reviews", "--no-parent", workdir}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"reviews","path":"/tmp"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "session-2", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"session-2","title":"coder-ref","status":"waiting","group":"reviews","path":"/tmp"}`}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
	})
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "agent_deck_create_session", map[string]any{
		"ensure_title":   "coder-ref",
		"ensure_cmd":     "codex --model gpt-5.4 --ask-for-approval on-request",
		"group_path":     "reviews",
		"no_parent_link": true,
		"workdir":        "/tmp",
	})

	if got := output["created_target"]; got != true {
		t.Fatalf("created_target = %v, want true", got)
	}
	if got := output["group"]; got != "reviews" {
		t.Fatalf("group = %v, want reviews", got)
	}
	if got := output["path"]; got != "/tmp" {
		t.Fatalf("path = %v, want /tmp", got)
	}
}

func TestWaypostServiceUsesConfiguredStateDir(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir: stateDir,
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/self",
		"subject": "delegate",
		"body":    "body",
	})
	if got := output["delivery_id"]; got == nil || got == "" {
		t.Fatalf("delivery_id = %v, want non-empty", got)
	}

	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	deliveries, err := runtime.Store().List(context.Background(), waypost.ListParams{
		Address: "agent-deck/self",
		State:   "queued",
	})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("queued deliveries = %d, want 1", len(deliveries))
	}
	if deliveries[0].Subject != "delegate" {
		t.Fatalf("queued subject = %q, want delegate", deliveries[0].Subject)
	}
}

func TestWaypostReadSparselyReportsHasMore(t *testing.T) {
	t.Skip("waypost_read is CLI-owned after the MCP hard cut")
	t.Parallel()

	hasMore := false
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{
			t: t,
			readMessagesFunc: func(context.Context, []string) ([]waypost.ReadMessage, error) {
				return []waypost.ReadMessage{{MessageID: "msg_123", Body: "message body"}}, nil
			},
			readDeliveriesFunc: func(context.Context, []string) ([]waypost.ReadDelivery, error) {
				return []waypost.ReadDelivery{{DeliveryID: "dlv_123", Body: "delivery body"}}, nil
			},
			readLatestFunc: func(context.Context, []string, string, int) ([]waypost.ReadDelivery, bool, error) {
				return []waypost.ReadDelivery{{DeliveryID: "dlv_latest", Body: "latest body"}}, hasMore, nil
			},
		}},
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	byMessage := callServiceTool(t, service, "waypost_read", map[string]any{
		"message_ids": []string{"msg_123"},
	})
	assertMapOmitsHasMore(t, byMessage)

	byDelivery := callServiceTool(t, service, "waypost_read", map[string]any{
		"delivery_ids": []string{"dlv_123"},
	})
	assertMapOmitsHasMore(t, byDelivery)

	latest := callServiceTool(t, service, "waypost_read", map[string]any{
		"addresses": []string{"agent-deck/self"},
		"latest":    true,
		"limit":     1,
	})
	assertMapOmitsHasMore(t, latest)

	hasMore = true
	latest = callServiceTool(t, service, "waypost_read", map[string]any{
		"addresses": []string{"agent-deck/self"},
		"latest":    true,
		"limit":     1,
	})
	if got := latest["has_more"]; got != true {
		t.Fatalf("latest has_more = %v, want true", got)
	}
}

func TestWaypostLifecycleToolsUseDirectWaypostService(t *testing.T) {
	t.Skip("wait, list, and read are CLI-owned after the MCP hard cut")
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir: stateDir,
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	send := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/self",
		"subject": "delegate",
		"body":    "body",
	})
	deliveryID := send["delivery_id"].(string)
	if deliveryID == "" {
		t.Fatal("delivery_id = empty, want non-empty")
	}

	wait := callServiceTool(t, service, "waypost_wait", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if got := wait["status"]; got != "message_available" {
		t.Fatalf("wait status = %v, want message_available", got)
	}
	waitDelivery := wait["delivery"].(map[string]any)
	if waitDelivery["delivery_id"] != deliveryID {
		t.Fatalf("wait delivery_id = %v, want %s", waitDelivery["delivery_id"], deliveryID)
	}
	if _, ok := waitDelivery["body"]; ok {
		t.Fatalf("wait delivery unexpectedly contains body: %v", waitDelivery)
	}

	recv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if got := recv["status"]; got != "received" {
		t.Fatalf("recv status = %v, want received", got)
	}
	received := recv["delivery"].(map[string]any)
	messages := received["messages"].([]any)
	if len(messages) != 1 {
		t.Fatalf("recv messages = %d, want 1", len(messages))
	}
	message := messages[0].(map[string]any)
	if message["delivery_id"] != deliveryID {
		t.Fatalf("recv delivery_id = %v, want %s", message["delivery_id"], deliveryID)
	}
	if message["body"] != "body" {
		t.Fatalf("recv body = %v, want body", message["body"])
	}
	claimDetail := readMCPDeliveryEventDetail(t, stateDir, deliveryID, "delivery_leased")
	if claimDetail["claim_source"] != "mcp" {
		t.Fatalf("claim_source = %v, want mcp", claimDetail["claim_source"])
	}
	if claimDetail["claim_tool"] != "waypost_recv" {
		t.Fatalf("claim_tool = %v, want waypost_recv", claimDetail["claim_tool"])
	}
	addresses, ok := claimDetail["claim_bound_addresses"].([]any)
	if !ok || len(addresses) != 1 || addresses[0] != "agent-deck/self" {
		t.Fatalf("claim_bound_addresses = %#v, want [agent-deck/self]", claimDetail["claim_bound_addresses"])
	}

	ack := callServiceTool(t, service, "waypost_ack", map[string]any{
		"delivery_id": deliveryID,
		"lease_token": message["lease_token"],
	})
	if got := ack["status"]; got != "acked" {
		t.Fatalf("ack status = %v, want acked", got)
	}

	list := callServiceTool(t, service, "waypost_list", map[string]any{
		"address": "agent-deck/self",
		"state":   "acked",
	})
	deliveries := list["deliveries"].([]any)
	if len(deliveries) != 1 {
		t.Fatalf("list deliveries = %d, want 1", len(deliveries))
	}
	listed := deliveries[0].(map[string]any)
	if listed["delivery_id"] != deliveryID {
		t.Fatalf("listed delivery_id = %v, want %s", listed["delivery_id"], deliveryID)
	}
	if listed["state"] != "acked" {
		t.Fatalf("listed state = %v, want acked", listed["state"])
	}

	read := callServiceTool(t, service, "waypost_read", map[string]any{
		"addresses": []string{"agent-deck/self"},
		"latest":    true,
		"state":     "acked",
		"limit":     1,
	})
	if got := read["mode"]; got != "latest" {
		t.Fatalf("read mode = %v, want latest", got)
	}
	items := read["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("read items = %d, want 1", len(items))
	}
	readDelivery := items[0].(map[string]any)
	if readDelivery["delivery_id"] != deliveryID {
		t.Fatalf("read delivery_id = %v, want %s", readDelivery["delivery_id"], deliveryID)
	}
	if readDelivery["body"] != "body" {
		t.Fatalf("read body = %v, want body", readDelivery["body"])
	}
}

func TestWaypostRecvDoesNotWaitForLaterMessage(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir: stateDir,
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	recv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if got := recv["status"]; got != "no_message" {
		t.Fatalf("recv status = %v, want no_message", got)
	}
	if _, ok := recv["addresses"]; ok {
		t.Fatalf("compact recv exposed addresses: %v", recv)
	}
	if service.activeLeases.hasTrackedLeases() {
		t.Fatal("empty recv tracked active lease")
	}

	send := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/self",
		"subject": "nonblocking recv",
		"body":    "body",
	})
	deliveryID := send["delivery_id"].(string)

	secondRecv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if got := secondRecv["status"]; got != "received" {
		t.Fatalf("second recv status = %v, want received", got)
	}
	delivery := secondRecv["delivery"].(map[string]any)
	if got := delivery["delivery_id"]; got != deliveryID {
		t.Fatalf("second recv delivery_id = %v, want %s", got, deliveryID)
	}
	if got := delivery["sender_address"]; got != "agent-deck/self" {
		t.Fatalf("second recv sender_address = %v, want agent-deck/self", got)
	}
	if _, ok := secondRecv["addresses"]; ok {
		t.Fatalf("compact recv exposed addresses: %v", secondRecv)
	}
}

func TestWaypostRecvReportsActiveLeaseImmediately(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir: stateDir,
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	send := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/self",
		"subject": "active lease",
		"body":    "body",
	})
	deliveryID := send["delivery_id"].(string)
	firstRecv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	firstMessage := firstRecv["delivery"].(map[string]any)
	leaseToken := firstMessage["lease_token"].(string)

	startedAt := time.Now()
	secondRecv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("recv elapsed = %s, want immediate active lease hint", elapsed)
	}
	if got := secondRecv["status"]; got != "active_leases" {
		t.Fatalf("recv status = %v, want active_leases", got)
	}
	if _, ok := secondRecv["active_leases"]; ok {
		t.Fatalf("active_leases unexpectedly present: %v", secondRecv["active_leases"])
	}
	claimedDeliveryIDs := secondRecv["claimed_delivery_ids"].([]any)
	if len(claimedDeliveryIDs) != 1 {
		t.Fatalf("claimed_delivery_ids = %v, want one id", claimedDeliveryIDs)
	}
	if got := claimedDeliveryIDs[0]; got != deliveryID {
		t.Fatalf("claimed_delivery_ids[0] = %v, want %s", got, deliveryID)
	}
	if _, ok := secondRecv["lease_token"]; ok {
		t.Fatalf("recv hint unexpectedly included lease_token")
	}
	if _, ok := secondRecv["body"]; ok {
		t.Fatalf("recv hint unexpectedly included body")
	}

	history := callServiceTool(t, service, "waypost_claim_history", map[string]any{
		"delivery_id":         deliveryID,
		"recover_lease_token": true,
	})
	if len(history) != 2 || history["status"] != "listed" {
		t.Fatalf("targeted claim history = %v, want status and items only", history)
	}
	items := history["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("claim history items = %d, want 1", len(items))
	}
	item := items[0].(map[string]any)
	if got := item["lease_token"]; got != leaseToken {
		t.Fatalf("claim history lease_token = %v, want original token", got)
	}
	if _, ok := item["body"]; ok {
		t.Fatalf("claim history unexpectedly included body")
	}
	for _, field := range []string{"content_type", "claimed_at", "last_renewed_at"} {
		if _, ok := item[field]; ok {
			t.Fatalf("compact token recovery unexpectedly included %q: %v", field, item)
		}
	}
}

func TestWaypostRecvKnownDeliveryIDsSuppressActiveLeaseReport(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir: stateDir,
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	send := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/self",
		"subject": "known active lease",
		"body":    "body",
	})
	deliveryID := send["delivery_id"].(string)
	callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})

	secondRecv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses":          []string{"agent-deck/self"},
		"known_delivery_ids": []string{deliveryID},
	})
	if got := secondRecv["status"]; got != "no_message" {
		t.Fatalf("recv status = %v, want no_message", got)
	}
	if _, ok := secondRecv["active_leases"]; ok {
		t.Fatalf("active_leases unexpectedly present: %v", secondRecv["active_leases"])
	}
}

func TestWaypostRecvClaimsImmediateMessageWithParentContext(t *testing.T) {
	t.Parallel()

	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchWithTTLFunc = func(ctx context.Context, params waypost.ReceiveBatchParams, ttl time.Duration) (waypost.ReceiveResult, error) {
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			t.Fatal("ReceiveBatchWithLeaseTTL got timeout context, want parent call context")
		}
		if ttl <= 0 {
			t.Fatalf("ReceiveBatchWithLeaseTTL ttl = %s, want positive", ttl)
		}
		return waypost.ReceiveResult{
			Messages: []waypost.ReceivedMessage{{
				DeliveryID:       "dlv_parent_ctx",
				RecipientAddress: "agent-deck/self",
				LeaseToken:       "lease_parent_ctx",
				Subject:          "parent ctx",
				Body:             "body",
			}},
		}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if got := output["status"]; got != "received" {
		t.Fatalf("recv status = %v, want received", got)
	}
	if !service.activeLeases.hasTrackedLeases() {
		t.Fatal("recv did not track active lease")
	}
}

func TestWaypostRecvReturnsNoMessage(t *testing.T) {
	t.Parallel()

	callCount := 0
	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchFunc = func(_ context.Context, params waypost.ReceiveBatchParams) (waypost.ReceiveResult, error) {
		callCount++
		if !reflect.DeepEqual(params.Addresses, []string{"agent-deck/self"}) {
			t.Fatalf("ReceiveBatch addresses = %v, want [agent-deck/self]", params.Addresses)
		}
		return waypost.ReceiveResult{}, waypost.ErrNoMessage
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if got := output["status"]; got != "no_message" {
		t.Fatalf("recv status = %v, want no_message", got)
	}
	if callCount != 1 {
		t.Fatalf("ReceiveBatch calls = %d, want 1", callCount)
	}
	if service.activeLeases.hasTrackedLeases() {
		t.Fatal("no-message recv tracked active lease")
	}
}

func TestWaypostRecvReturnsImmediately(t *testing.T) {
	t.Parallel()

	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchFunc = func(_ context.Context, params waypost.ReceiveBatchParams) (waypost.ReceiveResult, error) {
		return waypost.ReceiveResult{}, waypost.ErrNoMessage
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	startedAt := time.Now()
	output := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if got := output["status"]; got != "no_message" {
		t.Fatalf("recv status = %v, want no_message", got)
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("recv elapsed = %s, want immediate non-blocking receive", elapsed)
	}
}

func TestWaypostRecvWithoutTimeoutRemainsImmediate(t *testing.T) {
	t.Parallel()

	callCount := 0
	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchFunc = func(_ context.Context, params waypost.ReceiveBatchParams) (waypost.ReceiveResult, error) {
		callCount++
		return waypost.ReceiveResult{}, waypost.ErrNoMessage
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	output := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if got := output["status"]; got != "no_message" {
		t.Fatalf("recv status = %v, want no_message", got)
	}
	if callCount != 1 {
		t.Fatalf("ReceiveBatch calls = %d, want 1", callCount)
	}
}

func TestWaypostGroupMCPRuntimeFlow(t *testing.T) {
	t.Skip("group control and history are CLI-owned after the MCP hard cut")
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir: stateDir,
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"agent/sender"}
	service.state.defaultSender = "agent/sender"
	service.state.autoBindAttempted = true

	created := callServiceTool(t, service, "waypost_group_create", map[string]any{
		"group_address": "group/review",
	})
	group := created["group"].(map[string]any)
	if group["address"] != "group/review" {
		t.Fatalf("created group address = %v, want group/review", group["address"])
	}

	callServiceTool(t, service, "waypost_group_add_member", map[string]any{
		"group_address": "group/review",
		"person":        "alice",
	})
	callServiceTool(t, service, "waypost_group_add_member", map[string]any{
		"group_address": "group/review",
		"person":        "bob",
	})

	send := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "group/review",
		"subject": "group update",
		"body":    "group body",
		"group":   true,
	})
	if got := send["mode"]; got != waypost.SendModeGroup {
		t.Fatalf("send mode = %v, want group", got)
	}
	if got := send["delivery_id"]; got != nil {
		t.Fatalf("group send delivery_id = %v, want nil", got)
	}

	wait := callServiceTool(t, service, "waypost_wait", map[string]any{
		"addresses": []string{"group/review"},
		"as_person": "alice",
	})
	if got := wait["status"]; got != "message_available" {
		t.Fatalf("wait status = %v, want message_available", got)
	}
	waitMessage := wait["message"].(map[string]any)
	if waitMessage["read"] != false {
		t.Fatalf("wait read = %v, want false", waitMessage["read"])
	}
	if _, ok := waitMessage["delivery_id"]; ok {
		t.Fatalf("group wait exposed delivery_id: %v", waitMessage)
	}

	recv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"group/review"},
		"as_person": "alice",
	})
	if got := recv["status"]; got != "received" {
		t.Fatalf("recv status = %v, want received", got)
	}
	recvMessage := recv["message"].(map[string]any)
	if recvMessage["body"] != "group body" {
		t.Fatalf("recv body = %v, want group body", recvMessage["body"])
	}
	if recvMessage["sender_address"] != "agent-deck/self" {
		t.Fatalf("recv sender_address = %v, want agent-deck/self", recvMessage["sender_address"])
	}
	if _, ok := recvMessage["lease_token"]; ok {
		t.Fatalf("group recv exposed lease_token: %v", recvMessage)
	}
	if service.activeLeases.hasTrackedLeases() {
		t.Fatal("group recv tracked active lease")
	}

	list := callServiceTool(t, service, "waypost_list", map[string]any{
		"address":   "group/review",
		"as_person": "alice",
	})
	deliveries := list["deliveries"].([]any)
	if len(deliveries) != 1 {
		t.Fatalf("group list deliveries = %d, want 1", len(deliveries))
	}
	listed := deliveries[0].(map[string]any)
	if listed["read"] != true {
		t.Fatalf("listed read = %v, want true after recv", listed["read"])
	}

	members := callServiceTool(t, service, "waypost_group_members", map[string]any{
		"group_address": "group/review",
	})
	if got := len(members["memberships"].([]any)); got != 2 {
		t.Fatalf("group members = %d, want 2", got)
	}

	inspect := callServiceTool(t, service, "waypost_address_inspect", map[string]any{
		"address": "group/review",
	})
	if got := inspect["inspection"].(map[string]any)["kind"]; got != waypost.AddressKindGroup {
		t.Fatalf("inspect kind = %v, want group", got)
	}
}

func TestWaypostGroupSendRuntimeKeepsMessageWhenSubscriberNotifyFails(t *testing.T) {
	t.Skip("group control is CLI-owned after the MCP hard cut")
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir: stateDir,
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			switch {
			case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "moderator", "--json"}, "\x00"):
				return RunResult{ExitCode: 0, Stdout: `{"id":"moderator","title":"moderator","status":"waiting"}`}, nil
			case isAgentDeckDeferredSend(args):
				return RunResult{ExitCode: 1, Stderr: "notify failed"}, nil
			default:
				t.Fatalf("unexpected command call: %v", args)
				return RunResult{}, nil
			}
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.autoBindAttempted = true

	callServiceTool(t, service, "waypost_group_create", map[string]any{
		"group_address": "group/review",
	})
	callServiceTool(t, service, "waypost_group_add_member", map[string]any{
		"group_address": "group/review",
		"person":        "alice",
	})
	callServiceTool(t, service, "waypost_group_add_member", map[string]any{
		"group_address": "group/review",
		"person":        "moderator",
	})
	callServiceTool(t, service, "waypost_group_add_subscriber", map[string]any{
		"group_address":  "group/review",
		"notify_address": "agent-deck/moderator",
		"person":         "moderator",
	})

	send := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":           "group/review",
		"from_address": "agent-deck/expert",
		"subject":      "expert post",
		"body":         "group body",
		"group":        true,
	})
	if got := send["status"]; got != "sent" {
		t.Fatalf("send status = %v, want sent", got)
	}
	if got := send["notify_status"]; got != "failed" {
		t.Fatalf("notify_status = %v, want failed", got)
	}

	controlRecv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/moderator"},
	})
	control := controlRecv["delivery"].(map[string]any)["messages"].([]any)[0].(map[string]any)
	if control["subject"] != "Group waypost update: group/review" {
		t.Fatalf("control subject = %v, want group update", control["subject"])
	}
	controlBody := control["body"].(string)
	for _, want := range []string{
		"Action: group_message_available",
		"Group-Address: group/review",
		"As-Person: moderator",
		"Message-ID: " + send["message_id"].(string),
	} {
		if !strings.Contains(controlBody, want) {
			t.Fatalf("control body = %q, want %q", controlBody, want)
		}
	}

	recv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"group/review"},
		"as_person": "alice",
	})
	message := recv["message"].(map[string]any)
	if message["message_id"] != send["message_id"] {
		t.Fatalf("recv message_id = %v, want %v", message["message_id"], send["message_id"])
	}
	if message["body"] != "group body" {
		t.Fatalf("recv body = %v, want group body", message["body"])
	}
}

func TestWaypostRecvExposesForwardedFromAddressInCompactPayload(t *testing.T) {
	forwardedFromAddress := "agent/source"
	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchWithTTLFunc = func(_ context.Context, params waypost.ReceiveBatchParams, ttl time.Duration) (waypost.ReceiveResult, error) {
		return waypost.ReceiveResult{
			Messages: []waypost.ReceivedMessage{{
				DeliveryID:           "dlv_forwarded",
				MessageID:            "msg_forwarded",
				ForwardedFromAddress: &forwardedFromAddress,
				LeaseToken:           "lease_forwarded",
				LeaseExpiresAt:       time.Now().UTC().Add(defaultMCPLeaseTTL).Format(time.RFC3339Nano),
				RecipientAddress:     "agent-deck/self",
				Subject:              "delegate",
				Body:                 "body",
			}},
		}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	recv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	message := recv["delivery"].(map[string]any)
	if got := message["forwarded_from_address"]; got != "agent/source" {
		t.Fatalf("forwarded_from_address = %v, want agent/source", got)
	}
	assertMCPMapOmitsForwardedMessageID(t, message)
}

func TestWaypostWaitExposesForwardedFromAddressInCompactPayload(t *testing.T) {
	t.Skip("waypost_wait is CLI-owned after the MCP hard cut")
	forwardedFromAddress := "agent/source"
	waypostService := &fakeWaypostService{t: t}
	waypostService.waitFunc = func(_ context.Context, params waypost.WaitParams) (waypost.ListedDelivery, error) {
		return waypost.ListedDelivery{
			DeliveryID:           "dlv_forwarded",
			MessageID:            "msg_forwarded",
			ForwardedFromAddress: &forwardedFromAddress,
			RecipientAddress:     "agent-deck/self",
			Subject:              "delegate",
			ContentType:          "text/plain",
		}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	wait := callServiceTool(t, service, "waypost_wait", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	delivery := wait["delivery"].(map[string]any)
	if got := delivery["forwarded_from_address"]; got != "agent/source" {
		t.Fatalf("forwarded_from_address = %v, want agent/source", got)
	}
	assertMCPMapOmitsForwardedMessageID(t, delivery)
}

func TestWaypostListAsPersonExposesForwardedFromAddress(t *testing.T) {
	t.Skip("waypost_list is CLI-owned after the MCP hard cut")
	forwardedFromAddress := "agent/source"
	waypostService := &fakeWaypostService{t: t}
	waypostService.listGroupMessagesFunc = func(_ context.Context, params waypost.GroupListParams) ([]waypost.GroupListedMessage, error) {
		if params.Address != "group/review" {
			t.Fatalf("ListGroupMessages address = %q, want group/review", params.Address)
		}
		if params.Person != "alice" {
			t.Fatalf("ListGroupMessages person = %q, want alice", params.Person)
		}
		return []waypost.GroupListedMessage{{
			MessageID:            "msg_forwarded",
			ForwardedFromAddress: &forwardedFromAddress,
			GroupID:              "grp_1",
			GroupAddress:         "group/review",
			Person:               "alice",
			MessageCreatedAt:     "2026-04-18T00:00:00Z",
			Subject:              "review",
			ContentType:          "text/plain",
			Read:                 false,
			ReadCount:            0,
			EligibleCount:        1,
		}}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	service.state.autoBindAttempted = true

	list := callServiceTool(t, service, "waypost_list", map[string]any{
		"address":   "group/review",
		"as_person": "alice",
	})
	deliveries := list["deliveries"].([]any)
	if len(deliveries) != 1 {
		t.Fatalf("len(deliveries) = %d, want 1", len(deliveries))
	}
	delivery := deliveries[0].(map[string]any)
	if got := delivery["forwarded_from_address"]; got != "agent/source" {
		t.Fatalf("forwarded_from_address = %v, want agent/source", got)
	}
	assertMCPMapOmitsForwardedMessageID(t, delivery)
}

func TestWaypostWaitAsPersonUsesGroupWaitWithoutDeliveryLease(t *testing.T) {
	t.Skip("waypost_wait is CLI-owned after the MCP hard cut")
	waypostService := &fakeWaypostService{t: t}
	waypostService.waitGroupMessageFunc = func(_ context.Context, params waypost.GroupWaitParams) (waypost.GroupListedMessage, error) {
		if params.Address != "group/review" || params.Person != "alice" || params.Timeout != 25*time.Millisecond {
			t.Fatalf("WaitGroupMessage params = %+v", params)
		}
		return waypost.GroupListedMessage{
			MessageID:        "msg_group",
			GroupID:          "grp_1",
			GroupAddress:     "group/review",
			Person:           "alice",
			MessageCreatedAt: "2026-04-18T00:00:00Z",
			Subject:          "review",
			ContentType:      "text/plain",
			Read:             false,
			ReadCount:        0,
			EligibleCount:    1,
		}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	service.state.autoBindAttempted = true

	wait := callServiceTool(t, service, "waypost_wait", map[string]any{
		"addresses": []string{"group/review"},
		"as_person": "alice",
		"timeout":   "25ms",
	})
	if got := wait["status"]; got != "message_available" {
		t.Fatalf("status = %v, want message_available", got)
	}
	message := wait["message"].(map[string]any)
	if got := message["message_id"]; got != "msg_group" {
		t.Fatalf("message_id = %v, want msg_group", got)
	}
	if _, ok := message["delivery_id"]; ok {
		t.Fatalf("group wait exposed delivery_id: %v", message)
	}
	if _, ok := message["lease_token"]; ok {
		t.Fatalf("group wait exposed lease_token: %v", message)
	}
}

func TestWaypostRecvAsPersonUsesGroupRecvWithoutTrackingLease(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveGroupMessageFunc = func(_ context.Context, params waypost.GroupReceiveParams) (waypost.GroupReceivedMessage, error) {
		if params.Address != "group/review" || params.Person != "alice" {
			t.Fatalf("ReceiveGroupMessage params = %+v", params)
		}
		return waypost.GroupReceivedMessage{
			MessageID:        "msg_group",
			GroupID:          "grp_1",
			GroupAddress:     "group/review",
			Person:           "alice",
			MessageCreatedAt: "2026-04-18T00:00:00Z",
			Subject:          "review",
			ContentType:      "text/plain",
			Body:             "body",
			ReadCount:        1,
			EligibleCount:    1,
			FirstReadAt:      "2026-04-18T00:01:00Z",
		}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableLeaseRenewLoop: true,
		DisableWakeScheduler:  true,
	})
	service.state.autoBindAttempted = true

	recv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"group/review"},
		"as_person": "alice",
	})
	if got := recv["status"]; got != "received" {
		t.Fatalf("status = %v, want received", got)
	}
	message := recv["message"].(map[string]any)
	if got := message["body"]; got != "body" {
		t.Fatalf("body = %v, want body", got)
	}
	if _, ok := message["delivery_id"]; ok {
		t.Fatalf("group recv exposed delivery_id: %v", message)
	}
	if _, ok := message["lease_token"]; ok {
		t.Fatalf("group recv exposed lease_token: %v", message)
	}
	if service.activeLeases.hasTrackedLeases() {
		t.Fatal("group recv tracked a personal delivery lease")
	}
}

func TestWaypostRecvAsPersonUsesImmediateGroupReceive(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waitCalled := false
	recvCalled := false
	waypostService.waitGroupMessageFunc = func(ctx context.Context, params waypost.GroupWaitParams) (waypost.GroupListedMessage, error) {
		waitCalled = true
		t.Fatalf("WaitGroupMessage called by non-blocking recv: %+v", params)
		return waypost.GroupListedMessage{}, nil
	}
	waypostService.receiveGroupMessageFunc = func(ctx context.Context, params waypost.GroupReceiveParams) (waypost.GroupReceivedMessage, error) {
		recvCalled = true
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			t.Fatal("ReceiveGroupMessage got timeout context, want parent context")
		}
		if params.Address != "group/review" || params.Person != "alice" {
			t.Fatalf("ReceiveGroupMessage params = %+v", params)
		}
		return waypost.GroupReceivedMessage{
			MessageID:        "msg_group",
			GroupID:          "grp_1",
			GroupAddress:     "group/review",
			Person:           "alice",
			MessageCreatedAt: "2026-04-18T00:00:00Z",
			Subject:          "review",
			ContentType:      "text/plain",
			Body:             "body",
			ReadCount:        1,
			EligibleCount:    1,
			FirstReadAt:      "2026-04-18T00:01:00Z",
		}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableLeaseRenewLoop: true,
		DisableWakeScheduler:  true,
	})
	service.state.autoBindAttempted = true

	recv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"group/review"},
		"as_person": "alice",
	})
	if got := recv["status"]; got != "received" {
		t.Fatalf("status = %v, want received", got)
	}
	message := recv["message"].(map[string]any)
	if got := message["body"]; got != "body" {
		t.Fatalf("body = %v, want body", got)
	}
	if waitCalled {
		t.Fatal("WaitGroupMessage was called by non-blocking recv")
	}
	if !recvCalled {
		t.Fatal("ReceiveGroupMessage was not called")
	}
	if service.activeLeases.hasTrackedLeases() {
		t.Fatal("group recv tracked a personal delivery lease")
	}
}

func TestWaypostGroupReadRequiresSingleGroupAddress(t *testing.T) {
	t.Skip("waypost_wait is CLI-owned after the MCP hard cut")
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	service.state.autoBindAttempted = true

	err := callServiceToolExpectError(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"group/one", "group/two"},
		"as_person": "alice",
	})
	if err == nil || !strings.Contains(err.Error(), "requires exactly one group address") {
		t.Fatalf("waypost_recv error = %v, want single group address validation", err)
	}

	err = callServiceToolExpectError(t, service, "waypost_wait", map[string]any{
		"addresses": []string{"agent/alice"},
		"as_person": "alice",
	})
	if err == nil || !strings.Contains(err.Error(), "requires a group address") {
		t.Fatalf("waypost_wait error = %v, want group address validation", err)
	}
}

func TestWaypostGroupControlToolsUseWaypostService(t *testing.T) {
	t.Skip("group control is CLI-owned after the MCP hard cut")
	waypostService := &fakeWaypostService{t: t}
	waypostService.createGroupFunc = func(_ context.Context, groupAddress string) (waypost.GroupRecord, error) {
		if groupAddress != "group/review" {
			t.Fatalf("CreateGroup address = %q, want group/review", groupAddress)
		}
		return waypost.GroupRecord{GroupID: "grp_1", Address: groupAddress, CreatedAt: "2026-04-18T00:00:00Z"}, nil
	}
	waypostService.addGroupMemberFunc = func(_ context.Context, groupAddress, person string) (waypost.GroupMembershipRecord, error) {
		if groupAddress != "group/review" || person != "alice" {
			t.Fatalf("AddGroupMember args = group=%q person=%q", groupAddress, person)
		}
		return waypost.GroupMembershipRecord{
			MembershipID: "gm_1",
			GroupID:      "grp_1",
			GroupAddress: groupAddress,
			PersonID:     "person_1",
			Person:       person,
			JoinedAt:     "2026-04-18T00:01:00Z",
			Active:       true,
		}, nil
	}
	waypostService.listGroupMembersFunc = func(_ context.Context, groupAddress string) ([]waypost.GroupMembershipRecord, error) {
		if groupAddress != "group/review" {
			t.Fatalf("ListGroupMembers address = %q, want group/review", groupAddress)
		}
		return []waypost.GroupMembershipRecord{{
			MembershipID: "gm_1",
			GroupID:      "grp_1",
			GroupAddress: groupAddress,
			PersonID:     "person_1",
			Person:       "alice",
			JoinedAt:     "2026-04-18T00:01:00Z",
			Active:       true,
		}}, nil
	}
	waypostService.removeGroupMemberFunc = func(_ context.Context, groupAddress, person string) (waypost.GroupMembershipRecord, error) {
		if groupAddress != "group/review" || person != "alice" {
			t.Fatalf("RemoveGroupMember args = group=%q person=%q", groupAddress, person)
		}
		leftAt := "2026-04-18T00:02:00Z"
		return waypost.GroupMembershipRecord{
			MembershipID: "gm_1",
			GroupID:      "grp_1",
			GroupAddress: groupAddress,
			PersonID:     "person_1",
			Person:       person,
			JoinedAt:     "2026-04-18T00:01:00Z",
			LeftAt:       &leftAt,
			Active:       false,
		}, nil
	}
	waypostService.addGroupSubscriberFunc = func(_ context.Context, groupAddress, notifyAddress, person string) (waypost.GroupNotificationSubscriberRecord, error) {
		if groupAddress != "group/review" || notifyAddress != "agent-deck/moderator" || person != "moderator" {
			t.Fatalf("AddGroupNotificationSubscriber args = group=%q notify=%q person=%q", groupAddress, notifyAddress, person)
		}
		return waypost.GroupNotificationSubscriberRecord{
			SubscriberID:  "gns_1",
			GroupID:       "grp_1",
			GroupAddress:  groupAddress,
			NotifyAddress: notifyAddress,
			Person:        person,
			CreatedAt:     "2026-04-18T00:01:30Z",
			Active:        true,
		}, nil
	}
	waypostService.listGroupSubscribersFunc = func(_ context.Context, groupAddress string) ([]waypost.GroupNotificationSubscriberRecord, error) {
		if groupAddress != "group/review" {
			t.Fatalf("ListGroupNotificationSubscribers address = %q, want group/review", groupAddress)
		}
		return []waypost.GroupNotificationSubscriberRecord{{
			SubscriberID:  "gns_1",
			GroupID:       "grp_1",
			GroupAddress:  groupAddress,
			NotifyAddress: "agent-deck/moderator",
			Person:        "moderator",
			CreatedAt:     "2026-04-18T00:01:30Z",
			Active:        true,
		}}, nil
	}
	waypostService.removeGroupSubscriberFunc = func(_ context.Context, groupAddress, notifyAddress string) (waypost.GroupNotificationSubscriberRecord, error) {
		if groupAddress != "group/review" || notifyAddress != "agent-deck/moderator" {
			t.Fatalf("RemoveGroupNotificationSubscriber args = group=%q notify=%q", groupAddress, notifyAddress)
		}
		removedAt := "2026-04-18T00:02:30Z"
		return waypost.GroupNotificationSubscriberRecord{
			SubscriberID:  "gns_1",
			GroupID:       "grp_1",
			GroupAddress:  groupAddress,
			NotifyAddress: notifyAddress,
			Person:        "moderator",
			CreatedAt:     "2026-04-18T00:01:30Z",
			RemovedAt:     &removedAt,
			Active:        false,
		}, nil
	}
	waypostService.inspectAddressFunc = func(_ context.Context, address string) (waypost.AddressInspection, error) {
		if address != "group/review" {
			t.Fatalf("InspectAddress address = %q, want group/review", address)
		}
		groupID := "grp_1"
		return waypost.AddressInspection{Address: address, Kind: waypost.AddressKindGroup, GroupID: &groupID}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler: true,
	})
	service.state.autoBindAttempted = true

	created := callServiceTool(t, service, "waypost_group_create", map[string]any{
		"group_address": "group/review",
	})
	if got := created["status"]; got != "created" {
		t.Fatalf("create status = %v, want created", got)
	}
	if got := created["group"].(map[string]any)["group_id"]; got != "grp_1" {
		t.Fatalf("created group_id = %v, want grp_1", got)
	}

	added := callServiceTool(t, service, "waypost_group_add_member", map[string]any{
		"group_address": "group/review",
		"person":        "alice",
	})
	if got := added["status"]; got != "added" {
		t.Fatalf("add status = %v, want added", got)
	}
	if got := added["membership"].(map[string]any)["person"]; got != "alice" {
		t.Fatalf("added person = %v, want alice", got)
	}

	members := callServiceTool(t, service, "waypost_group_members", map[string]any{
		"group_address": "group/review",
	})
	memberships := members["memberships"].([]any)
	if len(memberships) != 1 {
		t.Fatalf("memberships = %d, want 1", len(memberships))
	}

	removed := callServiceTool(t, service, "waypost_group_remove_member", map[string]any{
		"group_address": "group/review",
		"person":        "alice",
	})
	if got := removed["status"]; got != "removed" {
		t.Fatalf("remove status = %v, want removed", got)
	}
	if got := removed["membership"].(map[string]any)["active"]; got != false {
		t.Fatalf("removed active = %v, want false", got)
	}

	addedSubscriber := callServiceTool(t, service, "waypost_group_add_subscriber", map[string]any{
		"group_address":  "group/review",
		"notify_address": "agent-deck/moderator",
		"person":         "moderator",
	})
	if got := addedSubscriber["status"]; got != "added" {
		t.Fatalf("add subscriber status = %v, want added", got)
	}
	if got := addedSubscriber["subscriber"].(map[string]any)["notify_address"]; got != "agent-deck/moderator" {
		t.Fatalf("subscriber notify_address = %v, want agent-deck/moderator", got)
	}

	subscribers := callServiceTool(t, service, "waypost_group_subscribers", map[string]any{
		"group_address": "group/review",
	})
	subscriptions := subscribers["subscribers"].([]any)
	if len(subscriptions) != 1 {
		t.Fatalf("subscribers = %d, want 1", len(subscriptions))
	}

	removedSubscriber := callServiceTool(t, service, "waypost_group_remove_subscriber", map[string]any{
		"group_address":  "group/review",
		"notify_address": "agent-deck/moderator",
	})
	if got := removedSubscriber["status"]; got != "removed" {
		t.Fatalf("remove subscriber status = %v, want removed", got)
	}
	if got := removedSubscriber["subscriber"].(map[string]any)["active"]; got != false {
		t.Fatalf("removed subscriber active = %v, want false", got)
	}

	inspected := callServiceTool(t, service, "waypost_address_inspect", map[string]any{
		"address": "group/review",
	})
	inspection := inspected["inspection"].(map[string]any)
	if got := inspection["kind"]; got != waypost.AddressKindGroup {
		t.Fatalf("kind = %v, want group", got)
	}
	if got := inspection["group_id"]; got != "grp_1" {
		t.Fatalf("group_id = %v, want grp_1", got)
	}
}

func TestWaypostRecvStartsLeaseRenewLoopWithShortTTL(t *testing.T) {
	current := time.Date(2026, 4, 3, 6, 0, 0, 0, time.UTC)
	renewed := make(chan struct{}, 1)

	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchWithTTLFunc = func(_ context.Context, params waypost.ReceiveBatchParams, ttl time.Duration) (waypost.ReceiveResult, error) {
		if ttl != defaultMCPLeaseTTL {
			t.Fatalf("recv lease ttl = %s, want %s", ttl, defaultMCPLeaseTTL)
		}
		if params.Max != 1 || len(params.Addresses) != 1 || params.Addresses[0] != "agent-deck/self" {
			t.Fatalf("recv params = %+v, want one bound address", params)
		}
		return waypost.ReceiveResult{
			Messages: []waypost.ReceivedMessage{{
				DeliveryID:       "dlv_lease",
				LeaseToken:       "lease_1",
				LeaseExpiresAt:   current.Add(defaultMCPLeaseTTL).Format(time.RFC3339Nano),
				RecipientAddress: "agent-deck/self",
				Subject:          "delegate",
				Body:             "body",
			}},
		}, nil
	}
	waypostService.renewFunc = func(_ context.Context, deliveryID, leaseToken string, extendBy time.Duration) (waypost.LeaseRenewResult, error) {
		if deliveryID != "dlv_lease" || leaseToken != "lease_1" {
			t.Fatalf("renew args = delivery=%q lease=%q", deliveryID, leaseToken)
		}
		if extendBy != defaultMCPLeaseTTL {
			t.Fatalf("renew extendBy = %s, want %s", extendBy, defaultMCPLeaseTTL)
		}
		select {
		case renewed <- struct{}{}:
		default:
		}
		return waypost.LeaseRenewResult{
			DeliveryID:     deliveryID,
			LeaseToken:     leaseToken,
			LeaseExpiresAt: time.Now().UTC().Add(defaultMCPLeaseTTL).Format(time.RFC3339Nano),
		}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		LeaseRenewInterval: 10 * time.Millisecond,
	})
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})

	select {
	case <-renewed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for lease renew")
	}
}

func TestServiceCloseStopsLeaseRenewLoop(t *testing.T) {
	current := time.Date(2026, 4, 3, 6, 10, 0, 0, time.UTC)
	waypostService := &fakeWaypostService{t: t}
	waypostService.inspectLeaseFunc = func(_ context.Context, deliveryID string) (waypost.DeliveryLeaseState, error) {
		if deliveryID != "dlv_close" {
			t.Fatalf("InspectDeliveryLease delivery_id = %q, want dlv_close", deliveryID)
		}
		return waypost.DeliveryLeaseState{Found: true, State: "leased", LeaseToken: "lease_close"}, nil
	}
	entered := make(chan struct{})
	canceled := make(chan struct{})
	var closeEntered sync.Once
	var closeCanceled sync.Once
	var mu sync.Mutex
	calls := 0
	waypostService.renewFunc = func(ctx context.Context, deliveryID, leaseToken string, extendBy time.Duration) (waypost.LeaseRenewResult, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		closeEntered.Do(func() { close(entered) })
		<-ctx.Done()
		closeCanceled.Do(func() { close(canceled) })
		return waypost.LeaseRenewResult{}, ctx.Err()
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		Now:                func() time.Time { return current },
		LeaseRenewInterval: time.Nanosecond,
	})
	service.activeLeases.trackReceive(waypost.ReceiveResult{
		Messages: []waypost.ReceivedMessage{{
			DeliveryID:       "dlv_close",
			LeaseToken:       "lease_close",
			LeaseExpiresAt:   current.Add(defaultMCPLeaseTTL).Format(time.RFC3339Nano),
			RecipientAddress: "agent-deck/self",
			Subject:          "delegate",
			Body:             "body",
		}},
	}, current.Format(time.RFC3339Nano))

	service.startLeaseRenewLoop()
	waitForTestSignal(t, entered, "lease renew entered")
	service.Close()
	waitForTestSignal(t, canceled, "lease renew canceled")

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("Renew calls = %d, want 1", calls)
	}
}

func TestProcessLeaseRenewalsRetriesTransientFailureWithinLeaseWindow(t *testing.T) {
	current := time.Date(2026, 4, 3, 6, 15, 0, 0, time.UTC)
	renewCalls := 0
	ackCalled := false

	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchWithTTLFunc = func(_ context.Context, params waypost.ReceiveBatchParams, ttl time.Duration) (waypost.ReceiveResult, error) {
		return waypost.ReceiveResult{
			Messages: []waypost.ReceivedMessage{{
				DeliveryID:       "dlv_retry",
				LeaseToken:       "lease_retry",
				LeaseExpiresAt:   current.Add(defaultMCPLeaseTTL).Format(time.RFC3339Nano),
				RecipientAddress: "agent-deck/self",
				Subject:          "delegate",
				Body:             "body",
			}},
		}, nil
	}
	waypostService.renewFunc = func(_ context.Context, deliveryID, leaseToken string, extendBy time.Duration) (waypost.LeaseRenewResult, error) {
		renewCalls++
		if renewCalls == 1 {
			return waypost.LeaseRenewResult{}, context.DeadlineExceeded
		}
		return waypost.LeaseRenewResult{
			DeliveryID:     deliveryID,
			LeaseToken:     leaseToken,
			LeaseExpiresAt: current.Add(defaultMCPLeaseTTL).Format(time.RFC3339Nano),
		}, nil
	}
	waypostService.ackFunc = func(_ context.Context, deliveryID, leaseToken string) (waypost.DeliveryTransitionResult, error) {
		ackCalled = true
		return waypost.DeliveryTransitionResult{DeliveryID: deliveryID, State: "acked"}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		Now:                   func() time.Time { return current },
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})

	if err := service.processLeaseRenewals(context.Background()); err != nil {
		t.Fatalf("processLeaseRenewals() error = %v, want nil after transient retry", err)
	}
	if renewCalls != 2 {
		t.Fatalf("renewCalls = %d, want 2", renewCalls)
	}
	if failure := service.activeLeases.lastRenewalError(); failure != nil {
		t.Fatalf("lastRenewalError() = %v, want nil after successful retry", failure)
	}

	output := callServiceTool(t, service, "waypost_ack", map[string]any{
		"delivery_id": "dlv_retry",
		"lease_token": "lease_retry",
	})
	if got := output["status"]; got != "acked" {
		t.Fatalf("waypost_ack status = %v, want acked", got)
	}
	if !ackCalled {
		t.Fatal("Ack was not forwarded after transient renew retry")
	}
}

func TestProcessLeaseRenewalsAllowsTerminalMutationBeforeExpiryAfterTransientFailure(t *testing.T) {
	current := time.Date(2026, 4, 3, 6, 30, 0, 0, time.UTC)
	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchWithTTLFunc = func(_ context.Context, params waypost.ReceiveBatchParams, ttl time.Duration) (waypost.ReceiveResult, error) {
		return waypost.ReceiveResult{
			Messages: []waypost.ReceivedMessage{{
				DeliveryID:       "dlv_failure",
				LeaseToken:       "lease_failure",
				LeaseExpiresAt:   current.Add(defaultMCPLeaseTTL).Format(time.RFC3339Nano),
				RecipientAddress: "agent-deck/self",
				Subject:          "delegate",
				Body:             "body",
			}},
		}, nil
	}
	waypostService.renewFunc = func(_ context.Context, deliveryID, leaseToken string, extendBy time.Duration) (waypost.LeaseRenewResult, error) {
		return waypost.LeaseRenewResult{}, context.DeadlineExceeded
	}
	ackCalled := false
	waypostService.ackFunc = func(_ context.Context, deliveryID, leaseToken string) (waypost.DeliveryTransitionResult, error) {
		ackCalled = true
		return waypost.DeliveryTransitionResult{DeliveryID: deliveryID, State: "acked"}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		Now:                   func() time.Time { return current },
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})

	err := service.processLeaseRenewals(context.Background())
	if err == nil || !isLeaseRenewalFailure(err) {
		t.Fatalf("processLeaseRenewals() error = %v, want lease renewal failure", err)
	}
	if !service.activeLeases.hasTrackedLeases() {
		t.Fatal("active lease tracking removed after transient renewal failure")
	}

	output := callServiceTool(t, service, "waypost_ack", map[string]any{
		"delivery_id": "dlv_failure",
		"lease_token": "lease_failure",
	})
	if got := output["status"]; got != "acked" {
		t.Fatalf("waypost_ack status = %v, want acked", got)
	}
	if !ackCalled {
		t.Fatal("Ack was not forwarded before lease expiry")
	}
}

func TestProcessLeaseRenewalsAllowsTerminalMutationAfterExpiryFollowingTransientFailure(t *testing.T) {
	current := time.Date(2026, 4, 3, 6, 45, 0, 0, time.UTC)

	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchWithTTLFunc = func(_ context.Context, params waypost.ReceiveBatchParams, ttl time.Duration) (waypost.ReceiveResult, error) {
		return waypost.ReceiveResult{
			Messages: []waypost.ReceivedMessage{{
				DeliveryID:       "dlv_expired_failure",
				LeaseToken:       "lease_expired_failure",
				LeaseExpiresAt:   current.Add(defaultMCPLeaseTTL).Format(time.RFC3339Nano),
				RecipientAddress: "agent-deck/self",
				Subject:          "delegate",
				Body:             "body",
			}},
		}, nil
	}
	waypostService.renewFunc = func(_ context.Context, deliveryID, leaseToken string, extendBy time.Duration) (waypost.LeaseRenewResult, error) {
		return waypost.LeaseRenewResult{}, context.DeadlineExceeded
	}
	ackCalled := false
	waypostService.ackFunc = func(_ context.Context, deliveryID, leaseToken string) (waypost.DeliveryTransitionResult, error) {
		ackCalled = true
		return waypost.DeliveryTransitionResult{DeliveryID: deliveryID, State: "acked"}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		Now:                   func() time.Time { return current },
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})

	err := service.processLeaseRenewals(context.Background())
	if err == nil || !isLeaseRenewalFailure(err) {
		t.Fatalf("processLeaseRenewals() error = %v, want lease renewal failure", err)
	}

	current = current.Add(defaultMCPLeaseTTL + time.Second)
	output := callServiceTool(t, service, "waypost_ack", map[string]any{
		"delivery_id": "dlv_expired_failure",
		"lease_token": "lease_expired_failure",
	})
	if got := output["status"]; got != "acked" {
		t.Fatalf("waypost_ack status = %v, want acked", got)
	}
	if !ackCalled {
		t.Fatal("Ack was not forwarded after transient renew failure and local expiry")
	}
}

func TestRenewalFailureDefinitiveUsesLeaseSentinels(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "sentinel not found", err: fmt.Errorf("delivery not found text changed: %w", waypost.ErrLeaseNotFound), want: true},
		{name: "sentinel not leased", err: fmt.Errorf("delivery in wrong state: %w", waypost.ErrLeaseNotLeased), want: true},
		{name: "sentinel changed", err: fmt.Errorf("renew conflict: %w", waypost.ErrLeaseRenewChanged), want: true},
		{name: "legacy text not found", err: errors.New(`delivery "dlv_1" not found`), want: false},
		{name: "legacy text want leased", err: errors.New(`delivery "dlv_1" is in state "acked", want leased`), want: false},
		{name: "legacy text changed", err: errors.New(`delivery "dlv_1" changed while renewing`), want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := renewalFailureDefinitive(tc.err); got != tc.want {
				t.Fatalf("renewalFailureDefinitive(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestWaypostAckStopsTrackingActiveLease(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchWithTTLFunc = func(_ context.Context, params waypost.ReceiveBatchParams, ttl time.Duration) (waypost.ReceiveResult, error) {
		return waypost.ReceiveResult{
			Messages: []waypost.ReceivedMessage{{
				DeliveryID:       "dlv_acked",
				LeaseToken:       "lease_acked",
				LeaseExpiresAt:   time.Now().UTC().Add(defaultMCPLeaseTTL).Format(time.RFC3339Nano),
				RecipientAddress: "agent-deck/self",
				Subject:          "delegate",
				Body:             "body",
			}},
		}, nil
	}
	waypostService.ackFunc = func(_ context.Context, deliveryID, leaseToken string) (waypost.DeliveryTransitionResult, error) {
		if deliveryID != "dlv_acked" || leaseToken != "lease_acked" {
			t.Fatalf("ack args = delivery=%q lease=%q", deliveryID, leaseToken)
		}
		return waypost.DeliveryTransitionResult{DeliveryID: deliveryID, State: "acked"}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	recv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	message := recv["delivery"].(map[string]any)

	callServiceTool(t, service, "waypost_ack", map[string]any{
		"delivery_id": "dlv_acked",
		"lease_token": message["lease_token"],
	})

	if err := service.processLeaseRenewals(context.Background()); err != nil {
		t.Fatalf("processLeaseRenewals() error = %v", err)
	}
	if service.activeLeases.hasTrackedLeases() {
		t.Fatal("active leases still tracked after ack")
	}
}

func TestWaypostReleaseDeferAndFailUseDirectWaypostService(t *testing.T) {
	t.Skip("undefer and fail are CLI-owned after the MCP hard cut")
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	service := newService(Options{
		StateDir: stateDir,
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("unexpected command call: %v", args)
			return RunResult{}, nil
		}},
	})
	service.state.boundAddresses = []string{"agent-deck/self"}
	service.state.defaultSender = "agent-deck/self"
	service.state.autoBindAttempted = true

	firstSend := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/self",
		"subject": "release-defer",
		"body":    "body",
	})
	firstRecv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	firstMessage := firstRecv["delivery"].(map[string]any)["messages"].([]any)[0].(map[string]any)

	release := callServiceTool(t, service, "waypost_release", map[string]any{
		"delivery_id": firstSend["delivery_id"],
		"lease_token": firstMessage["lease_token"],
	})
	if got := release["status"]; got != "released" {
		t.Fatalf("release status = %v, want released", got)
	}

	secondRecv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	secondMessage := secondRecv["delivery"].(map[string]any)["messages"].([]any)[0].(map[string]any)
	until := time.Now().UTC().Add(10 * time.Minute).Format(time.RFC3339Nano)
	deferResult := callServiceTool(t, service, "waypost_defer", map[string]any{
		"delivery_id": firstSend["delivery_id"],
		"lease_token": secondMessage["lease_token"],
		"until":       until,
	})
	if got := deferResult["status"]; got != "deferred" {
		t.Fatalf("defer status = %v, want deferred", got)
	}

	wait := callServiceTool(t, service, "waypost_wait", map[string]any{
		"addresses": []string{"agent-deck/self"},
		"timeout":   "10ms",
	})
	if got := wait["status"]; got != "no_message" {
		t.Fatalf("wait status after defer = %v, want no_message", got)
	}

	undeferResult := callServiceTool(t, service, "waypost_undefer", map[string]any{
		"delivery_id": firstSend["delivery_id"],
	})
	if got := undeferResult["status"]; got != "undeferred" {
		t.Fatalf("undefer status = %v, want undeferred", got)
	}

	undeferWait := callServiceTool(t, service, "waypost_wait", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	if got := undeferWait["status"]; got != "message_available" {
		t.Fatalf("wait status after undefer = %v, want message_available", got)
	}
	undeferWaitDelivery := undeferWait["delivery"].(map[string]any)
	if got := undeferWaitDelivery["delivery_id"]; got != firstSend["delivery_id"] {
		t.Fatalf("wait delivery after undefer = %v, want %v", got, firstSend["delivery_id"])
	}

	undeferRecv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	undeferMessage := undeferRecv["delivery"].(map[string]any)["messages"].([]any)[0].(map[string]any)
	callServiceTool(t, service, "waypost_ack", map[string]any{
		"delivery_id": firstSend["delivery_id"],
		"lease_token": undeferMessage["lease_token"],
	})

	secondSend := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":      "agent-deck/self",
		"subject": "fail",
		"body":    "body-2",
	})
	failRecv := callServiceTool(t, service, "waypost_recv", map[string]any{
		"addresses": []string{"agent-deck/self"},
	})
	failMessage := failRecv["delivery"].(map[string]any)["messages"].([]any)[0].(map[string]any)
	failResult := callServiceTool(t, service, "waypost_fail", map[string]any{
		"delivery_id": secondSend["delivery_id"],
		"lease_token": failMessage["lease_token"],
		"reason":      "boom",
	})
	if got := failResult["status"]; got != "failed" {
		t.Fatalf("fail status = %v, want failed", got)
	}
	if got := failResult["reason"]; got != "boom" {
		t.Fatalf("fail reason = %v, want boom", got)
	}
}

func TestWriteToolRequiresWaypostStatusFirst(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	err := callServiceToolExpectErrorWithoutStatusBootstrap(t, service, "waypost_send", map[string]any{
		"from_address": "codex/source",
		"to":           "codex/target",
		"subject":      "hello",
		"body":         "body",
	})
	if err == nil || !strings.Contains(err.Error(), "waypost_status") {
		t.Fatalf("waypost_send error = %v, want waypost_status gate", err)
	}
}

func TestReadToolRequiresWaypostStatusFirst(t *testing.T) {
	t.Skip("waypost_wait is not registered after the MCP hard cut")
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	err := callServiceToolExpectErrorWithoutStatusBootstrap(t, service, "waypost_wait", map[string]any{
		"addresses": []string{"codex/source"},
		"timeout":   "0s",
	})
	if err == nil || !strings.Contains(err.Error(), "waypost_status") {
		t.Fatalf("waypost_wait error = %v, want waypost_status gate", err)
	}
}

func TestOnlyWaypostToolsRequireWaypostStatus(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	ctx := context.Background()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := service.Server().Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0.0.1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Wait()
	})

	tools, err := clientSession.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}
	registered := map[string]bool{}
	statusExempt := map[string]bool{
		"waypost_status":             true,
		"session_create":             true,
		"session_require":            true,
		"agent_deck_create_session":  true,
		"agent_deck_require_session": true,
	}
	for _, tool := range tools.Tools {
		registered[tool.Name] = true
		if statusExempt[tool.Name] {
			continue
		}
		if !requiresWaypostStatusToolName(tool.Name) {
			t.Fatalf("tool %q is registered but missing from requiresWaypostStatusToolName", tool.Name)
		}
	}
	for _, name := range requiresWaypostStatusToolNames() {
		if !registered[name] {
			t.Fatalf("requiresWaypostStatusToolName contains unregistered tool %q", name)
		}
	}
	if !registered["waypost_status"] {
		t.Fatalf("waypost_status is not registered")
	}
	if registered["waypost_debug"] {
		t.Fatal("waypost_debug is registered without IncludeDebugTool")
	}
	want := map[string]bool{
		"waypost_status":             true,
		"waypost_bind":               true,
		"waypost_send":               true,
		"waypost_recv":               true,
		"waypost_claim_history":      true,
		"waypost_ack":                true,
		"waypost_release":            true,
		"waypost_defer":              true,
		"session_create":             true,
		"session_require":            true,
		"agent_deck_create_session":  true,
		"agent_deck_require_session": true,
	}
	if !reflect.DeepEqual(registered, want) {
		t.Fatalf("registered MCP tools = %v, want exactly %v", registered, want)
	}
}

func TestWaypostDebugToolIsListedWhenEnabled(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         &fakeRunner{t: t},
		IncludeDebugTool:      true,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	defer service.Close()

	clientSession, cleanup := connectTestClientSession(t, service.Server(), nil)
	defer cleanup()
	tools, err := clientSession.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}
	registered := map[string]bool{}
	for _, tool := range tools.Tools {
		registered[tool.Name] = true
	}
	want := map[string]bool{
		"waypost_status":             true,
		"waypost_bind":               true,
		"waypost_debug":              true,
		"waypost_send":               true,
		"waypost_recv":               true,
		"waypost_claim_history":      true,
		"waypost_ack":                true,
		"waypost_release":            true,
		"waypost_defer":              true,
		"session_create":             true,
		"session_require":            true,
		"agent_deck_create_session":  true,
		"agent_deck_require_session": true,
	}
	if !reflect.DeepEqual(registered, want) {
		t.Fatalf("registered MCP tools = %v, want exactly %v", registered, want)
	}
}

func TestServerInstructionsScopeStatusGateToWaypostTools(t *testing.T) {
	for _, tt := range []struct {
		name             string
		includeDebugTool bool
		statusGate       string
	}{
		{
			name:       "default",
			statusGate: "Once after this MCP server starts, call waypost_status before the first waypost_* tool.",
		},
		{
			name:             "debug enabled",
			includeDebugTool: true,
			statusGate:       "Once after this MCP server starts, call waypost_status before the first waypost_* tool other than waypost_debug.",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			instructions := serverInstructions(tt.includeDebugTool)
			for _, want := range []string{
				tt.statusGate,
				"This server automatically renews leases for personal deliveries claimed by waypost_recv until it stops or restarts",
				"Waypost is for durable asynchronous work, not real-time communication.",
				"<executable> doc\n",
				"<executable> doc <topic>...\n",
				"Use the reported executable and resolved_state_dir for stateful CLI commands; never guess either.",
			} {
				if !strings.Contains(instructions, want) {
					t.Fatalf("serverInstructions = %q, want %q", instructions, want)
				}
			}
			if strings.Contains(instructions, "All other tools fail") {
				t.Fatalf("serverInstructions retains the global status gate: %q", instructions)
			}
		})
	}
}

func TestWaypostDebugWorksBeforeStatusAndDoesNotAutoBind(t *testing.T) {
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaaaaaaaaaaaaaa")
	t.Setenv("GEMINI_SESSION_ID", "not-a-session")
	t.Setenv("OPENCODE_SESSION_ID", "")
	t.Setenv("AGENTDECK_INSTANCE_ID", "deck-session-1")
	t.Setenv("TMUX", "/tmp/tmux-1000/default,123,0")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		t.Fatalf("waypost_debug should not run auto-bind probe command: %v", args)
		return RunResult{}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		IncludeDebugTool:      true,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	debug := callServiceToolWithoutStatusBootstrap(t, service, "waypost_debug", nil)
	if got := debug["status"]; got != "debug" {
		t.Fatalf("status = %v, want debug", got)
	}
	state, ok := debug["session_state"].(map[string]any)
	if !ok {
		t.Fatalf("session_state = %#v, want object", debug["session_state"])
	}
	if got := state["status_tool_called"]; got != false {
		t.Fatalf("status_tool_called = %v, want false", got)
	}
	if got := state["auto_bind_attempted"]; got != false {
		t.Fatalf("auto_bind_attempted = %v, want false", got)
	}
	if got := state["bound_addresses"]; got != nil && !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("bound_addresses = %#v, want empty before auto-bind", got)
	}

	debugEnv, ok := debug["debug_env"].(map[string]any)
	if !ok {
		t.Fatalf("debug_env = %#v, want object", debug["debug_env"])
	}
	tmux, ok := debugEnv["TMUX"].(map[string]any)
	if !ok {
		t.Fatalf("TMUX = %#v, want object", debugEnv["TMUX"])
	}
	if got := tmux["present"]; got != true {
		t.Fatalf("tmux present = %v, want true", got)
	}
	if got := tmux["value"]; got != "/tmp/tmux-1000/default,123,0" {
		t.Fatalf("tmux value = %v, want /tmp/tmux-1000/default,123,0", got)
	}
	agentDeck, ok := debugEnv["AGENTDECK_INSTANCE_ID"].(map[string]any)
	if !ok {
		t.Fatalf("AGENTDECK_INSTANCE_ID = %#v, want object", debugEnv["AGENTDECK_INSTANCE_ID"])
	}
	if got := agentDeck["present"]; got != true {
		t.Fatalf("agent-deck env present = %v, want true", got)
	}
	if got := agentDeck["value"]; got != "deck-session-1" {
		t.Fatalf("agent-deck env value = %v, want deck-session-1", got)
	}

	env, ok := debug["tool_session_env"].(map[string]any)
	if !ok {
		t.Fatalf("tool_session_env = %#v, want object", debug["tool_session_env"])
	}
	claude, ok := env["CLAUDE_CODE_SESSION_ID"].(map[string]any)
	if !ok {
		t.Fatalf("CLAUDE_CODE_SESSION_ID = %#v, want object", env["CLAUDE_CODE_SESSION_ID"])
	}
	if got := claude["present"]; got != true {
		t.Fatalf("claude present = %v, want true", got)
	}
	if got := claude["accepted_by_validation"]; got != true {
		t.Fatalf("claude accepted_by_validation = %v, want true", got)
	}
	if got := claude["address"]; got != "claude/aaaaaaaaaaaaaaaa" {
		t.Fatalf("claude address = %v, want claude/aaaaaaaaaaaaaaaa", got)
	}

	gemini, ok := env["GEMINI_SESSION_ID"].(map[string]any)
	if !ok {
		t.Fatalf("GEMINI_SESSION_ID = %#v, want object", env["GEMINI_SESSION_ID"])
	}
	if got := gemini["present"]; got != true {
		t.Fatalf("gemini present = %v, want true", got)
	}
	if got := gemini["accepted_by_validation"]; got != false {
		t.Fatalf("gemini accepted_by_validation = %v, want false", got)
	}
	if got := fmt.Sprint(gemini["failure_reason"]); !strings.Contains(got, "hex") {
		t.Fatalf("gemini failure_reason = %v, want hex validation reason", got)
	}
}

func TestToolSessionDescriptorsDriveOutputFields(t *testing.T) {
	sessions := toolSessionIDs{}
	for _, descriptor := range toolSessionDescriptors {
		if descriptor.StatusJSONKey == "" {
			t.Fatalf("tool session descriptor %q has no status JSON key", descriptor.Scheme)
		}
		sessions[descriptor.Scheme] = descriptor.Scheme + "-session"
	}

	status := boundStateMap(boundState{DetectedToolSessions: sessions})
	debug := debugSessionState(stateSnapshot{DetectedToolSessions: sessions})

	for _, descriptor := range toolSessionDescriptors {
		want := descriptor.Scheme + "-session"
		if got := status[descriptor.StatusJSONKey]; got != want {
			t.Fatalf("status[%q] = %v, want %v", descriptor.StatusJSONKey, got, want)
		}
		if got := debug[descriptor.StatusJSONKey]; got != want {
			t.Fatalf("debug[%q] = %v, want %v", descriptor.StatusJSONKey, got, want)
		}
	}
}

func TestWaypostStatusReportsAutoBindInvalidJSONWarningAndUnlocksManualBind(t *testing.T) {
	isolateAutoBindEnv(t)

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 1}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `not-json`}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	status := callServiceTool(t, service, "waypost_status", nil)
	warnings, ok := status["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", status["warnings"])
	}
	foundInvalidJSONWarning := false
	for _, warning := range warnings {
		if strings.Contains(fmt.Sprint(warning), "invalid JSON") {
			foundInvalidJSONWarning = true
		}
	}
	if !foundInvalidJSONWarning {
		t.Fatalf("warnings = %#v, want invalid JSON warning", warnings)
	}

	bind := callServiceTool(t, service, "waypost_bind", map[string]any{
		"addresses": []string{"codex/manual"},
	})
	if got := bind["default_sender"]; got != "codex/manual" {
		t.Fatalf("waypost_bind default_sender = %v, want codex/manual", got)
	}
	assertNoToolSessionWarning(t, bind)
}

func TestWaypostStatusReportsCodexProbeWarningAndUnlocksManualBind(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows skips Unix ps/lsof Codex session probing")
	}
	isolateAutoBindEnv(t)

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 0, Stdout: "4242 1 codex codex"}, nil
		case len(args) >= 1 && args[0] == "lsof":
			return RunResult{}, errors.New("lsof failed")
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	status := callServiceTool(t, service, "waypost_status", nil)
	warnings, ok := status["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", status["warnings"])
	}
	foundCodexProbeWarning := false
	for _, warning := range warnings {
		if strings.Contains(fmt.Sprint(warning), "codex session auto-bind probe failed") {
			foundCodexProbeWarning = true
		}
	}
	if !foundCodexProbeWarning {
		t.Fatalf("warnings = %#v, calls = %#v, want codex probe warning", warnings, runner.Calls())
	}

	bind := callServiceTool(t, service, "waypost_bind", map[string]any{
		"addresses": []string{"codex/manual"},
	})
	if got := bind["default_sender"]; got != "codex/manual" {
		t.Fatalf("waypost_bind default_sender = %v, want codex/manual", got)
	}
	assertNoToolSessionWarning(t, bind)
}

func TestWaypostStatusKeepsCodexProbeWarningWhenAgentDeckProbeDoesNotComplete(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows skips Unix ps/lsof Codex session probing")
	}
	isolateAutoBindEnv(t)

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 0, Stdout: "4242 1 codex codex"}, nil
		case len(args) >= 1 && args[0] == "lsof":
			return RunResult{}, errors.New("lsof failed")
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{}, errors.New("agent-deck not found")
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	status := callServiceTool(t, service, "waypost_status", nil)
	warnings, ok := status["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", status["warnings"])
	}
	for _, warning := range warnings {
		if strings.Contains(fmt.Sprint(warning), "codex session auto-bind probe failed") {
			return
		}
	}
	t.Fatalf("warnings = %#v, calls = %#v, want codex probe warning", warnings, runner.Calls())
}

func TestWaypostStatusRetriesAutoBindAfterEmptyAttempt(t *testing.T) {
	isolateAutoBindEnv(t)

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 1, Stderr: "no process"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	first := callServiceTool(t, service, "waypost_status", nil)
	if got := first["bound_addresses"]; got != nil && !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("first bound_addresses = %#v, want empty", got)
	}

	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaaaaaaaaaaaaaa")
	second := callServiceTool(t, service, "waypost_status", nil)
	if got := second["bound_addresses"]; !reflect.DeepEqual(got, []any{"claude/aaaaaaaaaaaaaaaa"}) {
		t.Fatalf("second bound_addresses = %#v, want claude auto-bind", got)
	}
	if got := second["default_sender"]; got != "claude/aaaaaaaaaaaaaaaa" {
		t.Fatalf("second default_sender = %v, want claude default sender", got)
	}
	assertNoToolSessionWarning(t, second)
}

func TestWaypostStatusDoesNotWarnForCodexProbeMiss(t *testing.T) {
	isolateAutoBindEnv(t)

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 1, Stderr: "no process"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	status := callServiceTool(t, service, "waypost_status", nil)
	warnings, ok := status["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", status["warnings"])
	}
	for _, warning := range warnings {
		if strings.Contains(fmt.Sprint(warning), "codex session auto-bind probe failed") {
			t.Fatalf("warnings = %#v, want no codex probe failure warning for probe miss", warnings)
		}
	}
}

func TestWaypostStatusSkipsCodexProcessProbeOnWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows-specific Codex process probe behavior")
	}
	isolateAutoBindEnv(t)

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && (args[0] == "ps" || args[0] == "lsof"):
			t.Fatalf("unexpected Unix process probe on Windows: %v", args)
			return RunResult{}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	status := callServiceTool(t, service, "waypost_status", nil)
	warnings, ok := status["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", status["warnings"])
	}
	for _, warning := range warnings {
		if strings.Contains(fmt.Sprint(warning), "codex session auto-bind probe failed") {
			t.Fatalf("warnings = %#v, want no Codex process probe failure warning on Windows", warnings)
		}
	}
}

func TestWaypostStatusReportsAgentDeckShowInvalidJSONWarning(t *testing.T) {
	isolateAutoBindEnv(t)
	t.Setenv("AGENTDECK_INSTANCE_ID", "deck-session-1")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 1}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `not-json`}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	status := callServiceTool(t, service, "waypost_status", nil)
	if got := status["default_sender"]; got != "agent-deck/deck-session-1" {
		t.Fatalf("default_sender = %v, want agent-deck/deck-session-1", got)
	}
	warnings, ok := status["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", status["warnings"])
	}
	foundInvalidJSONWarning := false
	for _, warning := range warnings {
		if strings.Contains(fmt.Sprint(warning), "invalid JSON") {
			foundInvalidJSONWarning = true
		}
	}
	if !foundInvalidJSONWarning {
		t.Fatalf("warnings = %#v, want invalid JSON warning", warnings)
	}
}

func TestWaypostStatusWarnsWhenOnlyAgentDeckAddressIsBound(t *testing.T) {
	isolateAutoBindEnv(t)
	t.Setenv("AGENTDECK_INSTANCE_ID", "deck-session-1")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 1}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	status := callServiceTool(t, service, "waypost_status", nil)
	if got := status["default_sender"]; got != "agent-deck/deck-session-1" {
		t.Fatalf("default_sender = %v, want agent-deck/deck-session-1", got)
	}
	assertHasToolSessionWarning(t, status)
}

func TestWaypostStatusIgnoresInvalidToolSessionEnvValues(t *testing.T) {
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "not-a-thread")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "claude-session")
	t.Setenv("GEMINI_SESSION_ID", "1234")
	t.Setenv("OPENCODE_SESSION_ID", "abc--def0")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 1, Stderr: "no process"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	if got := status["bound_addresses"]; got != nil && !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("bound_addresses = %#v, want no env auto-bind", got)
	}
	assertHasToolSessionWarning(t, status)

	warnings, ok := status["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", status["warnings"])
	}
	for _, envName := range []string{"CODEX_THREAD_ID", "CLAUDE_CODE_SESSION_ID", "GEMINI_SESSION_ID", "OPENCODE_SESSION_ID"} {
		found := false
		for _, warning := range warnings {
			if strings.Contains(fmt.Sprint(warning), envName) && strings.Contains(fmt.Sprint(warning), "does not look like a hex session id") {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("warnings = %#v, want invalid %s warning", warnings, envName)
		}
	}
}

func TestWaypostStatusPreservesInvalidToolSessionEnvWarningsDuringFallbackRetry(t *testing.T) {
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "claude-session")

	currentCalls := 0
	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			currentCalls++
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	first := callServiceTool(t, service, "waypost_status", nil)
	if got := first["bound_addresses"]; !reflect.DeepEqual(got, []any{"codex/0123456789abcdef"}) {
		t.Fatalf("first bound_addresses = %#v, want codex fallback", got)
	}
	assertHasInvalidToolSessionEnvWarning(t, first, "CLAUDE_CODE_SESSION_ID")

	second := callServiceTool(t, service, "waypost_status", nil)
	if currentCalls != 2 {
		t.Fatalf("agent-deck current calls = %d, want retry on fallback", currentCalls)
	}
	if got := second["bound_addresses"]; !reflect.DeepEqual(got, []any{"codex/0123456789abcdef"}) {
		t.Fatalf("second bound_addresses = %#v, want codex fallback", got)
	}
	assertHasInvalidToolSessionEnvWarning(t, second, "CLAUDE_CODE_SESSION_ID")
}

func TestWaypostStatusPreservesInvalidToolSessionEnvWarningsAfterAgentDeckUpgrade(t *testing.T) {
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "claude-session")

	currentCalls := 0
	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			currentCalls++
			if currentCalls == 1 {
				return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
			}
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-1"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	first := callServiceTool(t, service, "waypost_status", nil)
	if got := first["default_sender"]; got != "codex/0123456789abcdef" {
		t.Fatalf("first default_sender = %v, want codex fallback", got)
	}
	assertHasInvalidToolSessionEnvWarning(t, first, "CLAUDE_CODE_SESSION_ID")

	second := callServiceTool(t, service, "waypost_status", nil)
	if currentCalls != 2 {
		t.Fatalf("agent-deck current calls = %d, want retry on fallback", currentCalls)
	}
	if got := second["default_sender"]; got != "agent-deck/deck-session-1" {
		t.Fatalf("second default_sender = %v, want agent-deck/deck-session-1", got)
	}
	assertHasInvalidToolSessionEnvWarning(t, second, "CLAUDE_CODE_SESSION_ID")
}

func TestLooksLikeHexSessionID(t *testing.T) {
	tests := []struct {
		name      string
		sessionID string
		want      bool
	}{
		{name: "plain hex", sessionID: "0123456789abcdef", want: true},
		{name: "uppercase hex", sessionID: "ABCDEF1234567890", want: true},
		{name: "uuid style", sessionID: "01234567-89ab-cdef-0123-456789abcdef", want: true},
		{name: "too short", sessionID: "1234abc", want: false},
		{name: "letters outside hex", sessionID: "claude-session-123", want: false},
		{name: "consecutive hyphen", sessionID: "abc--def0", want: false},
		{name: "leading hyphen", sessionID: "-abcdef01", want: false},
		{name: "trailing hyphen", sessionID: "abcdef01-", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := looksLikeHexSessionID(tt.sessionID); got != tt.want {
				t.Fatalf("looksLikeHexSessionID(%q) = %v, want %v", tt.sessionID, got, tt.want)
			}
		})
	}
}

func TestInvalidCodexThreadEnvFallsBackToProcessProbe(t *testing.T) {
	t.Setenv("CODEX_THREAD_ID", "not-a-thread")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 0, Stdout: "4242 1 codex codex resume 01234567-89ab-cdef-0123-456789abcdef"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	sessionID, warnings := service.sessions.detectCurrentCodexSessionID(context.Background())
	if runtime.GOOS == "windows" {
		if sessionID != "" {
			t.Fatalf("sessionID = %q, want empty on Windows without process probe", sessionID)
		}
	} else if sessionID != "01234567-89ab-cdef-0123-456789abcdef" {
		t.Fatalf("sessionID = %q, want process-probed codex thread", sessionID)
	}
	if len(warnings) == 0 || !strings.Contains(warnings[0], "CODEX_THREAD_ID") {
		t.Fatalf("warnings = %#v, want invalid CODEX_THREAD_ID warning", warnings)
	}
}

func TestDevinSessionIDValidationFailure(t *testing.T) {
	t.Parallel()

	for _, sessionID := range []string{
		"truth-alarm",
		"grizzled-parakeet",
		"abc",
		"session.v2_beta-1",
		"01234567-89ab-cdef-0123-456789abcdef",
	} {
		if failure := devinSessionIDValidationFailure(sessionID); failure != "" {
			t.Errorf("devinSessionIDValidationFailure(%q) = %q, want valid", sessionID, failure)
		}
	}
	for _, sessionID := range []string{
		"",
		"ab",
		"bad--id",
		"-leading",
		"trailing-",
		"has space",
		"has/slash",
		"has:colon",
	} {
		if failure := devinSessionIDValidationFailure(sessionID); failure == "" {
			t.Errorf("devinSessionIDValidationFailure(%q) = valid, want failure", sessionID)
		}
	}
}

func TestInvalidDevinSessionEnvWarnsWithDevinHint(t *testing.T) {
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")
	t.Setenv("DEVIN_SESSION_ID", "bad--id")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 1}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	status := callServiceTool(t, service, "waypost_status", nil)
	if got := status["default_sender"]; got != "codex/0123456789abcdef" {
		t.Fatalf("default_sender = %v, want codex fallback", got)
	}
	warnings, ok := status["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", status["warnings"])
	}
	found := false
	for _, warning := range warnings {
		text := fmt.Sprint(warning)
		if strings.Contains(text, "DEVIN_SESSION_ID") && strings.Contains(text, "does not look like a devin session id") {
			found = true
		}
	}
	if !found {
		t.Fatalf("warnings = %#v, want invalid DEVIN_SESSION_ID warning", warnings)
	}
}

func TestAutoBindFindsDevinSessionFromProcessArgs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process probe requires Unix")
	}
	isolateAutoBindEnv(t)

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 0, Stdout: "4242 1 devin /Users/alice/.local/bin/devin --resume grizzled-parakeet"}, nil
		case len(args) >= 1 && args[0] == "lsof":
			t.Fatalf("unexpected lsof probe when --resume exposes the session id: %v", args)
			return RunResult{}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	ids, warnings := service.sessions.detectCurrentToolSessionIDs(context.Background())
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
	if got := ids["devin"]; got != "grizzled-parakeet" {
		t.Fatalf("ids[devin] = %q, want grizzled-parakeet", got)
	}
}

func TestAutoBindRejectsInvalidDevinSessionIDFromProcessArgs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process probe requires Unix")
	}
	isolateAutoBindEnv(t)

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 0, Stdout: "4242 1 devin /Users/alice/.local/bin/devin --resume abc-"}, nil
		case len(args) >= 1 && args[0] == "lsof":
			return RunResult{ExitCode: 0, Stdout: "devin 4242 alice 3r REG 1,4 10240 123 /Users/alice/.local/share/devin/cli/session_locks/truth-alarm.lock"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	ids, warnings := service.sessions.detectCurrentToolSessionIDs(context.Background())
	if len(warnings) != 1 || !strings.Contains(warnings[0], `"abc-"`) {
		t.Fatalf("warnings = %#v, want one invalid --resume id warning", warnings)
	}
	if got := ids["devin"]; got != "truth-alarm" {
		t.Fatalf("ids[devin] = %q, want truth-alarm from the session lock fallback", got)
	}
}

func TestAutoBindRejectsInvalidDevinSessionIDFromSessionLock(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process probe requires Unix")
	}
	isolateAutoBindEnv(t)

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 0, Stdout: "4242 1 devin /Users/alice/.local/bin/devin acp"}, nil
		case len(args) >= 1 && args[0] == "lsof":
			return RunResult{ExitCode: 0, Stdout: "devin 4242 alice 3r REG 1,4 10240 123 /Users/alice/.local/share/devin/cli/session_locks/ab.lock"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	ids, warnings := service.sessions.detectCurrentToolSessionIDs(context.Background())
	if len(warnings) != 1 || !strings.Contains(warnings[0], `"ab"`) {
		t.Fatalf("warnings = %#v, want one invalid session lock id warning", warnings)
	}
	if got := ids["devin"]; got != "" {
		t.Fatalf("ids[devin] = %q, want empty for invalid lock id", got)
	}
}

func TestAutoBindFindsDevinSessionFromSessionLock(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process probe requires Unix")
	}
	isolateAutoBindEnv(t)

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 0, Stdout: "4242 1 devin /Users/alice/.local/bin/devin acp"}, nil
		case len(args) >= 1 && args[0] == "lsof":
			return RunResult{ExitCode: 0, Stdout: "COMMAND  PID  USER   FD   TYPE DEVICE SIZE/OFF NODE NAME\ndevin   4242 alice    3r   REG    1,4    10240  123 /Users/alice/.local/share/devin/cli/session_locks/truth-alarm.lock"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	ids, warnings := service.sessions.detectCurrentToolSessionIDs(context.Background())
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
	if got := ids["devin"]; got != "truth-alarm" {
		t.Fatalf("ids[devin] = %q, want truth-alarm", got)
	}
}

func TestAutoBindSkipsDevinProcessProbeWithoutDevinEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process probe requires Unix")
	}
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		t.Fatalf("unexpected command without a Devin process signal: %v", args)
		return RunResult{}, nil
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	ids, warnings := service.sessions.detectCurrentToolSessionIDs(context.Background())
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
	if got := ids["devin"]; got != "" {
		t.Fatalf("ids[devin] = %q, want empty without a Devin signal", got)
	}
	if got := ids["codex"]; got != "0123456789abcdef" {
		t.Fatalf("ids[codex] = %q, want env session", got)
	}
}

func TestAutoBindProbesDevinWhenChiselSessionDBIsSet(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process probe requires Unix")
	}
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")
	t.Setenv("CHISEL_SESSION_DB", "/Users/alice/.local/share/devin/cli/sessions.db")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 0, Stdout: "4242 1 devin /Users/alice/.local/bin/devin acp"}, nil
		case len(args) >= 1 && args[0] == "lsof":
			return RunResult{ExitCode: 0, Stdout: "devin 4242 alice 3r REG 1,4 10240 123 /Users/alice/.local/share/devin/cli/session_locks/calm-finch.lock"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 4242 }

	ids, warnings := service.sessions.detectCurrentToolSessionIDs(context.Background())
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
	if got := ids["devin"]; got != "calm-finch" {
		t.Fatalf("ids[devin] = %q, want calm-finch", got)
	}
}

func TestAutoBindFindsClaudeCodeSessionFromEnv(t *testing.T) {
	tests := []struct {
		name              string
		envName           string
		envValue          string
		wantDefaultSender string
		wantAddresses     []any
		wantDetectedKey   string
	}{
		{
			name:              "claude code",
			envName:           "CLAUDE_CODE_SESSION_ID",
			envValue:          "aaaaaaaaaaaaaaaa",
			wantDefaultSender: "claude/aaaaaaaaaaaaaaaa",
			wantAddresses:     []any{"claude/aaaaaaaaaaaaaaaa"},
			wantDetectedKey:   "detected_claude_code_session_id",
		},
		{
			name:              "gemini",
			envName:           "GEMINI_SESSION_ID",
			envValue:          "bbbbbbbbbbbbbbbb",
			wantDefaultSender: "gemini/bbbbbbbbbbbbbbbb",
			wantAddresses:     []any{"gemini/bbbbbbbbbbbbbbbb"},
			wantDetectedKey:   "detected_gemini_session_id",
		},
		{
			name:              "opencode",
			envName:           "OPENCODE_SESSION_ID",
			envValue:          "cccccccccccccccc",
			wantDefaultSender: "opencode/cccccccccccccccc",
			wantAddresses:     []any{"opencode/cccccccccccccccc"},
			wantDetectedKey:   "detected_opencode_session_id",
		},
		{
			name:              "devin",
			envName:           "DEVIN_SESSION_ID",
			envValue:          "truth-alarm",
			wantDefaultSender: "devin/truth-alarm",
			wantAddresses:     []any{"devin/truth-alarm"},
			wantDetectedKey:   "detected_devin_session_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolateAutoBindEnv(t)
			t.Setenv(tt.envName, tt.envValue)

			runner := &fakeRunner{t: t}
			runner.handler = func(args []string, _ string) (RunResult, error) {
				switch {
				case len(args) >= 1 && args[0] == "ps":
					return RunResult{ExitCode: 1}, nil
				case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
					return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
				default:
					t.Fatalf("unexpected command: %v", args)
					return RunResult{}, nil
				}
			}

			service := newService(Options{
				WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
				CommandRunner:         runner,
				DisableWakeScheduler:  true,
				DisableLeaseRenewLoop: true,
			})
			status := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})

			if got := status["default_sender"]; got != tt.wantDefaultSender {
				t.Fatalf("default_sender = %v, want %v", got, tt.wantDefaultSender)
			}
			if !reflect.DeepEqual(status["bound_addresses"], tt.wantAddresses) {
				t.Fatalf("bound_addresses = %v, want %v", status["bound_addresses"], tt.wantAddresses)
			}
			if got := status[tt.wantDetectedKey]; got != tt.envValue {
				t.Fatalf("%s = %v, want %v", tt.wantDetectedKey, got, tt.envValue)
			}
			if got := status["detected_tool_session_addresses"]; !reflect.DeepEqual(got, tt.wantAddresses) {
				t.Fatalf("detected_tool_session_addresses = %v, want %v", got, tt.wantAddresses)
			}
		})
	}
}

func TestAutoBindFindsAgentDeckSessionFromCodexStateDB(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")
	t.Setenv("AGENTDECK_PROFILE", "bad")
	writeBrokenAgentDeckStateDB(t, home, "bad")
	writeAgentDeckStateDB(t, home, "work", "deck-session-1", "/tmp/project", "0123456789abcdef")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})

	if got := status["default_sender"]; got != "agent-deck/deck-session-1" {
		t.Fatalf("default_sender = %v, want agent-deck/deck-session-1", got)
	}
	wantAddresses := []any{"agent-deck/deck-session-1", "codex/0123456789abcdef"}
	if !reflect.DeepEqual(status["bound_addresses"], wantAddresses) {
		t.Fatalf("bound_addresses = %v, want %v", status["bound_addresses"], wantAddresses)
	}
	assertHasAgentDeckStateDBWarning(t, status)
}

func TestAutoBindPrefersCodexLinkedAgentDeckSessionOverAmbientCurrent(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")
	writeAgentDeckStateDB(t, home, "default", "deck-session-1", "/tmp/project", "0123456789abcdef")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-2"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-2", "--json"}, "\x00"):
			t.Fatalf("should not resolve ambient agent-deck current session: %v", args)
			return RunResult{}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})

	if got := status["default_sender"]; got != "agent-deck/deck-session-1" {
		t.Fatalf("default_sender = %v, want agent-deck/deck-session-1", got)
	}
	wantAddresses := []any{"agent-deck/deck-session-1", "codex/0123456789abcdef"}
	if !reflect.DeepEqual(status["bound_addresses"], wantAddresses) {
		t.Fatalf("bound_addresses = %v, want %v", status["bound_addresses"], wantAddresses)
	}
	warnings, ok := status["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", status["warnings"])
	}
	for _, warning := range warnings {
		if strings.Contains(fmt.Sprint(warning), "using codex-linked session") {
			return
		}
	}
	t.Fatalf("warnings = %#v, want ambient current mismatch warning", warnings)
}

func TestAutoBindDoesNotChooseAgentDeckSessionFromStateDBByWorkdirAlone(t *testing.T) {
	home := t.TempDir()
	workdir := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("AGENTDECK_PROFILE", "work")
	writeAgentDeckStateDB(t, home, "work", "deck-session-1", workdir, "0123456789abcdef")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 1 }
	service.state.defaultWorkdir = workdir

	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	if got := status["bound_addresses"]; got != nil && !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("bound_addresses = %v, want empty without current agent-deck session signal", got)
	}
	if got := status["default_sender"]; got != unsetValue {
		t.Fatalf("default_sender = %v, want unset without current agent-deck session signal", got)
	}
}

func TestAutoBindComplementsCurrentAgentDeckSessionFromStateDBByWorkdir(t *testing.T) {
	home := t.TempDir()
	workdir := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("AGENTDECK_PROFILE", "work")
	writeAgentDeckStateDB(t, home, "work", "deck-session-1", workdir, "0123456789abcdef")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-1"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 1 }
	service.state.defaultWorkdir = workdir

	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	wantAddresses := []any{"agent-deck/deck-session-1", "codex/0123456789abcdef"}
	if !reflect.DeepEqual(status["bound_addresses"], wantAddresses) {
		t.Fatalf("bound_addresses = %v, want %v", status["bound_addresses"], wantAddresses)
	}
	if got := status["default_sender"]; got != "agent-deck/deck-session-1" {
		t.Fatalf("default_sender = %v, want agent-deck/deck-session-1", got)
	}
}

func TestAutoBindUsesSessionShowPathBeforeStateDBWorkdirLookup(t *testing.T) {
	home := t.TempDir()
	workdir := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("AGENTDECK_PROFILE", "work")
	writeAgentDeckStateDB(t, home, "work", "deck-session-1", workdir, "0123456789abcdef")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-1"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			payload, err := json.Marshal(map[string]string{
				"id":     "deck-session-1",
				"status": "waiting",
				"path":   workdir,
			})
			if err != nil {
				t.Fatalf("marshal session show payload: %v", err)
			}
			return RunResult{ExitCode: 0, Stdout: string(payload)}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 1 }

	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	wantAddresses := []any{"agent-deck/deck-session-1", "codex/0123456789abcdef"}
	if !reflect.DeepEqual(status["bound_addresses"], wantAddresses) {
		t.Fatalf("bound_addresses = %v, want %v", status["bound_addresses"], wantAddresses)
	}
	if got := status["default_workdir"]; got != workdir {
		t.Fatalf("default_workdir = %v, want %v", got, workdir)
	}
}

func TestAutoBindFindsCurrentSessionWhenNewerCodexSessionSharesWorkdir(t *testing.T) {
	home := t.TempDir()
	workdir := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("AGENTDECK_PROFILE", "work")
	writeAgentDeckStateDBRows(t, home, "work", []agentDeckStateDBRow{
		{
			ID:             "newer-session",
			ProjectPath:    workdir,
			CodexSessionID: "fedcba9876543210",
			CreatedAt:      2,
			LastAccessed:   3,
		},
		{
			ID:             "deck-session-1",
			ProjectPath:    workdir,
			CodexSessionID: "0123456789abcdef",
			CreatedAt:      1,
			LastAccessed:   2,
		},
	})

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-1"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 1 }
	service.state.defaultWorkdir = workdir

	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	wantAddresses := []any{"agent-deck/deck-session-1", "codex/0123456789abcdef"}
	if !reflect.DeepEqual(status["bound_addresses"], wantAddresses) {
		t.Fatalf("bound_addresses = %v, want %v", status["bound_addresses"], wantAddresses)
	}
	if got := status["detected_agent_session_id"]; got != "0123456789abcdef" {
		t.Fatalf("detected_agent_session_id = %v, want 0123456789abcdef", got)
	}
}

func TestAutoBindDoesNotRetryStateDBAfterEmptyResultWithoutAgentDeckSignal(t *testing.T) {
	home := t.TempDir()
	workdir := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("AGENTDECK_PROFILE", "work")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 1 }
	service.state.defaultWorkdir = workdir

	first := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	if got := first["bound_addresses"]; got != nil && !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("first bound_addresses = %#v, want empty", got)
	}

	writeAgentDeckStateDB(t, home, "work", "deck-session-1", workdir, "0123456789abcdef")
	second := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	if got := second["bound_addresses"]; got != nil && !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("second bound_addresses = %#v, want empty without current agent-deck session signal", got)
	}
	if got := second["default_sender"]; got != unsetValue {
		t.Fatalf("second default_sender = %v, want unset without current agent-deck session signal", got)
	}
}

func TestAutoBindRetriesAgentDeckStateDBAfterAgentDeckOnlyResult(t *testing.T) {
	home := t.TempDir()
	workdir := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("AGENTDECK_PROFILE", "work")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-1"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 1 }
	service.state.defaultWorkdir = workdir

	first := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	wantFirst := []any{"agent-deck/deck-session-1"}
	if !reflect.DeepEqual(first["bound_addresses"], wantFirst) {
		t.Fatalf("first bound_addresses = %v, want %v", first["bound_addresses"], wantFirst)
	}
	if got := first["detected_agent_session_id"]; got != nil {
		t.Fatalf("first detected_agent_session_id = %v, want nil", got)
	}

	writeAgentDeckStateDB(t, home, "work", "deck-session-1", workdir, "0123456789abcdef")
	second := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	wantSecond := []any{"agent-deck/deck-session-1", "codex/0123456789abcdef"}
	if !reflect.DeepEqual(second["bound_addresses"], wantSecond) {
		t.Fatalf("second bound_addresses = %v, want %v", second["bound_addresses"], wantSecond)
	}
	if got := second["detected_agent_session_id"]; got != "0123456789abcdef" {
		t.Fatalf("second detected_agent_session_id = %v, want 0123456789abcdef", got)
	}
}

func TestAutoBindSkipsBadAgentDeckDBAndFallsBackToCodexOnly(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")
	t.Setenv("AGENTDECK_PROFILE", "bad")
	writeBrokenAgentDeckStateDB(t, home, "bad")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	status := callServiceTool(t, service, "waypost_status", nil)

	if got := status["default_sender"]; got != "codex/0123456789abcdef" {
		t.Fatalf("default_sender = %v, want codex/0123456789abcdef", got)
	}
	wantAddresses := []any{"codex/0123456789abcdef"}
	if !reflect.DeepEqual(status["bound_addresses"], wantAddresses) {
		t.Fatalf("bound_addresses = %v, want %v", status["bound_addresses"], wantAddresses)
	}
	assertHasAgentDeckStateDBWarning(t, status)
}

func TestAutoBindRetriesAgentDeckAfterCodexOnlyFallback(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")

	var currentCalls int
	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			currentCalls++
			if currentCalls == 1 {
				return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
			}
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-1"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchFunc = func(_ context.Context, params waypost.ReceiveBatchParams) (waypost.ReceiveResult, error) {
		want := []string{"agent-deck/deck-session-1", "codex/0123456789abcdef"}
		if !reflect.DeepEqual(params.Addresses, want) {
			t.Fatalf("receive addresses = %v, want %v", params.Addresses, want)
		}
		return waypost.ReceiveResult{}, waypost.ErrNoMessage
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	status := callServiceTool(t, service, "waypost_status", nil)
	if got := status["default_sender"]; got != "codex/0123456789abcdef" {
		t.Fatalf("initial default_sender = %v, want codex/0123456789abcdef", got)
	}

	recv := callServiceTool(t, service, "waypost_recv", nil)
	if got := recv["warnings"]; got != nil {
		t.Fatalf("recv warnings = %v, want nil after agent-deck retry succeeds", got)
	}
	status = callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	if got := status["default_sender"]; got != "agent-deck/deck-session-1" {
		t.Fatalf("upgraded default_sender = %v, want agent-deck/deck-session-1", got)
	}
}

func TestAutoBindRetriesAgentDeckAfterCodexFallbackWithExtraToolAddress(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaaaaaaaaaaaaaa")

	var currentCalls int
	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			currentCalls++
			if currentCalls == 1 {
				return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
			}
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-1"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchFunc = func(_ context.Context, params waypost.ReceiveBatchParams) (waypost.ReceiveResult, error) {
		want := []string{"agent-deck/deck-session-1", "codex/0123456789abcdef", "claude/aaaaaaaaaaaaaaaa"}
		if !reflect.DeepEqual(params.Addresses, want) {
			t.Fatalf("receive addresses = %v, want %v", params.Addresses, want)
		}
		return waypost.ReceiveResult{}, waypost.ErrNoMessage
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	status := callServiceTool(t, service, "waypost_status", nil)
	if got := status["default_sender"]; got != "codex/0123456789abcdef" {
		t.Fatalf("initial default_sender = %v, want codex/0123456789abcdef", got)
	}
	wantInitialAddresses := []any{"codex/0123456789abcdef", "claude/aaaaaaaaaaaaaaaa"}
	if !reflect.DeepEqual(status["bound_addresses"], wantInitialAddresses) {
		t.Fatalf("initial bound_addresses = %v, want %v", status["bound_addresses"], wantInitialAddresses)
	}

	recv := callServiceTool(t, service, "waypost_recv", nil)
	if got := recv["warnings"]; got != nil {
		t.Fatalf("recv warnings = %v, want nil after agent-deck retry succeeds", got)
	}
	status = callServiceTool(t, service, "waypost_status", nil)
	if got := status["default_sender"]; got != "agent-deck/deck-session-1" {
		t.Fatalf("upgraded default_sender = %v, want agent-deck/deck-session-1", got)
	}
	wantUpgradedAddresses := []any{"agent-deck/deck-session-1", "codex/0123456789abcdef", "claude/aaaaaaaaaaaaaaaa"}
	if !reflect.DeepEqual(status["bound_addresses"], wantUpgradedAddresses) {
		t.Fatalf("upgraded bound_addresses = %v, want %v", status["bound_addresses"], wantUpgradedAddresses)
	}
}

func TestAutoBindRetriesAgentDeckAfterClaudeOnlyFallback(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "aaaaaaaaaaaaaaaa")

	var currentCalls int
	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch {
		case len(args) >= 1 && args[0] == "ps":
			return RunResult{ExitCode: 1}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			currentCalls++
			if currentCalls == 1 {
				return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
			}
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-1"}`}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchFunc = func(_ context.Context, params waypost.ReceiveBatchParams) (waypost.ReceiveResult, error) {
		want := []string{"agent-deck/deck-session-1", "claude/aaaaaaaaaaaaaaaa"}
		if !reflect.DeepEqual(params.Addresses, want) {
			t.Fatalf("receive addresses = %v, want %v", params.Addresses, want)
		}
		return waypost.ReceiveResult{}, waypost.ErrNoMessage
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	status := callServiceTool(t, service, "waypost_status", nil)
	if got := status["default_sender"]; got != "claude/aaaaaaaaaaaaaaaa" {
		t.Fatalf("initial default_sender = %v, want claude/aaaaaaaaaaaaaaaa", got)
	}

	recv := callServiceTool(t, service, "waypost_recv", nil)
	if got := recv["warnings"]; got != nil {
		t.Fatalf("recv warnings = %v, want nil after agent-deck retry succeeds", got)
	}
	status = callServiceTool(t, service, "waypost_status", nil)
	if got := status["default_sender"]; got != "agent-deck/deck-session-1" {
		t.Fatalf("upgraded default_sender = %v, want agent-deck/deck-session-1", got)
	}
	wantAddresses := []any{"agent-deck/deck-session-1", "claude/aaaaaaaaaaaaaaaa"}
	if !reflect.DeepEqual(status["bound_addresses"], wantAddresses) {
		t.Fatalf("upgraded bound_addresses = %v, want %v", status["bound_addresses"], wantAddresses)
	}
}

func TestWaypostBindDisablesAgentDeckRetryUpgrade(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")

	var currentCalls int
	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			currentCalls++
			if currentCalls == 1 {
				return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
			}
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-1"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	status := callServiceTool(t, service, "waypost_status", nil)
	if got := status["default_sender"]; got != "codex/0123456789abcdef" {
		t.Fatalf("initial default_sender = %v, want codex/0123456789abcdef", got)
	}

	bind := callServiceTool(t, service, "waypost_bind", map[string]any{
		"addresses": []string{"codex/manual"},
	})
	if got := bind["default_sender"]; got != "codex/manual" {
		t.Fatalf("waypost_bind default_sender = %v, want codex/manual", got)
	}

	status = callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	if got := status["default_sender"]; got != "codex/manual" {
		t.Fatalf("status default_sender = %v, want codex/manual", got)
	}
	if got := status["detected_agent_session_id"]; got != nil {
		t.Fatalf("detected_agent_session_id = %v, want nil after manual bind override", got)
	}
	if got := status["detected_tool_session_addresses"]; !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("detected_tool_session_addresses = %v, want empty after manual bind override", got)
	}
	wantAddresses := []any{"codex/manual"}
	if !reflect.DeepEqual(status["bound_addresses"], wantAddresses) {
		t.Fatalf("bound_addresses = %v, want %v", status["bound_addresses"], wantAddresses)
	}
	assertNoToolSessionWarning(t, status)
	if currentCalls != 1 {
		t.Fatalf("agent-deck current calls = %d, want 1", currentCalls)
	}
}

func TestWaypostBindManualOverrideWarnsWhenNoToolAddressRemains(t *testing.T) {
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 1, Stderr: "not in an agent-deck pane"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	status := callServiceTool(t, service, "waypost_status", nil)
	if got := status["default_sender"]; got != "codex/0123456789abcdef" {
		t.Fatalf("initial default_sender = %v, want codex/0123456789abcdef", got)
	}

	bind := callServiceTool(t, service, "waypost_bind", map[string]any{
		"addresses": []string{"workflow/manual"},
	})
	if got := bind["default_sender"]; got != "workflow/manual" {
		t.Fatalf("waypost_bind default_sender = %v, want workflow/manual", got)
	}
	if got := bind["detected_agent_session_id"]; got != nil {
		t.Fatalf("detected_agent_session_id = %v, want nil after manual bind override", got)
	}
	if got := bind["detected_tool_session_addresses"]; !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("detected_tool_session_addresses = %v, want empty after manual bind override", got)
	}
	assertHasToolSessionWarning(t, bind)
}

func TestAgentDeckRetryRechecksFallbackStateBeforeUpgrade(t *testing.T) {
	home := t.TempDir()
	setTestHome(t, home)
	isolateAutoBindEnv(t)
	t.Setenv("CODEX_THREAD_ID", "0123456789abcdef")

	runner := &fakeRunner{t: t}
	runner.handler = func(args []string, _ string) (RunResult, error) {
		switch strings.Join(args, "\x00") {
		case strings.Join([]string{"agent-deck", "session", "current", "--json"}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: `{"id":"deck-session-1"}`}, nil
		case strings.Join([]string{"agent-deck", "session", "show", "deck-session-1", "--json"}, "\x00"):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		default:
			t.Fatalf("unexpected command: %v", args)
			return RunResult{}, nil
		}
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         runner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	staleFallback := stateSnapshot{
		BoundAddresses:           []string{"codex/0123456789abcdef"},
		DefaultSender:            "codex/0123456789abcdef",
		AutoBindAttempted:        true,
		AutoBoundToolFallback:    true,
		DetectedToolSessions:     toolSessionIDs{"codex": "0123456789abcdef"},
		DetectedAgentDeckSession: "",
	}
	service.state.boundAddresses = []string{"codex/manual"}
	service.state.defaultSender = "codex/manual"
	service.state.autoBindAttempted = true
	service.state.autoBoundToolFallback = false
	service.state.detectedToolSessions = toolSessionIDs{"codex": "0123456789abcdef"}

	if err := service.sessions.tryUpgradeAgentDeckBinding(context.Background(), staleFallback); err != nil {
		t.Fatalf("tryUpgradeAgentDeckBinding error = %v", err)
	}
	if !reflect.DeepEqual(service.state.boundAddresses, []string{"codex/manual"}) {
		t.Fatalf("boundAddresses = %v, want [codex/manual]", service.state.boundAddresses)
	}
	if got := service.state.defaultSender; got != "codex/manual" {
		t.Fatalf("defaultSender = %v, want codex/manual", got)
	}
	if got := service.state.detectedAgentDeckSession; got != "" {
		t.Fatalf("detectedAgentDeckSession = %v, want empty", got)
	}
}

func TestWaypostRecvWarnsWhenOnlyCodexSessionIsBound(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchFunc = func(_ context.Context, params waypost.ReceiveBatchParams) (waypost.ReceiveResult, error) {
		if !reflect.DeepEqual(params.Addresses, []string{"codex/self"}) {
			t.Fatalf("receive addresses = %v, want [codex/self]", params.Addresses)
		}
		return waypost.ReceiveResult{}, waypost.ErrNoMessage
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"codex/self"}
	service.state.defaultSender = "codex/self"
	service.state.detectedToolSessions = toolSessionIDs{"codex": "self"}
	service.state.autoBindAttempted = true

	out := callServiceTool(t, service, "waypost_recv", nil)
	warnings, ok := out["warnings"].([]any)
	if !ok || len(warnings) != 1 {
		t.Fatalf("warnings = %#v, want one warning", out["warnings"])
	}
	warning := warnings[0].(string)
	if !strings.Contains(warning, "agent-deck session current --json") || !strings.Contains(warning, "waypost_bind") {
		t.Fatalf("warning = %v, want manual agent-deck bind recovery hint", warning)
	}
}

func TestWaypostRecvWarnsWhenManualToolAddressIsBoundWithoutAgentDeck(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchFunc = func(_ context.Context, params waypost.ReceiveBatchParams) (waypost.ReceiveResult, error) {
		if !reflect.DeepEqual(params.Addresses, []string{"codex/manual"}) {
			t.Fatalf("receive addresses = %v, want [codex/manual]", params.Addresses)
		}
		return waypost.ReceiveResult{}, waypost.ErrNoMessage
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"codex/manual"}
	service.state.defaultSender = "codex/manual"
	service.state.autoBindAttempted = true

	out := callServiceTool(t, service, "waypost_recv", nil)
	warnings, ok := out["warnings"].([]any)
	if !ok || len(warnings) != 1 {
		t.Fatalf("warnings = %#v, want one warning", out["warnings"])
	}
	warning := warnings[0].(string)
	if !strings.Contains(warning, "agent-deck session current --json") || !strings.Contains(warning, "waypost_bind") {
		t.Fatalf("warning = %v, want manual agent-deck bind recovery hint", warning)
	}
}

func TestWaypostRecvWarnsWhenOnlyClaudeSessionIsBound(t *testing.T) {
	waypostService := &fakeWaypostService{t: t}
	waypostService.receiveBatchFunc = func(_ context.Context, params waypost.ReceiveBatchParams) (waypost.ReceiveResult, error) {
		if !reflect.DeepEqual(params.Addresses, []string{"claude/self"}) {
			t.Fatalf("receive addresses = %v, want [claude/self]", params.Addresses)
		}
		return waypost.ReceiveResult{}, waypost.ErrNoMessage
	}

	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         &fakeRunner{t: t},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"claude/self"}
	service.state.defaultSender = "claude/self"
	service.state.detectedToolSessions = toolSessionIDs{"claude": "self"}
	service.state.autoBindAttempted = true

	out := callServiceTool(t, service, "waypost_recv", nil)
	warnings, ok := out["warnings"].([]any)
	if !ok || len(warnings) != 1 {
		t.Fatalf("warnings = %#v, want one warning", out["warnings"])
	}
	warning, ok := warnings[0].(string)
	if !ok {
		t.Fatalf("warning = %#v, want string", warnings[0])
	}
	if !strings.Contains(warning, "agent-deck session current --json") || !strings.Contains(warning, "waypost_bind") {
		t.Fatalf("warning = %v, want manual agent-deck bind recovery hint", warning)
	}
}

func writeBrokenAgentDeckStateDB(t *testing.T, home, profile string) {
	t.Helper()
	dbPath := filepath.Join(home, ".agent-deck", "profiles", profile, "state.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		t.Fatalf("mkdir broken state db dir: %v", err)
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open broken state db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE unrelated (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("create broken state db table: %v", err)
	}
}

func setTestHome(t *testing.T, home string) {
	t.Helper()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
}

func isolateAutoBindEnv(t *testing.T) {
	t.Helper()
	for _, name := range toolSessionEnvNames() {
		t.Setenv(name, "")
	}
	t.Setenv("CHISEL_SESSION_DB", "")
	t.Setenv("AGENTDECK_INSTANCE_ID", "")
	t.Setenv("AGENTDECK_PROFILE", "")
}

type agentDeckStateDBRow struct {
	ID             string
	ProjectPath    string
	CodexSessionID string
	CreatedAt      int
	LastAccessed   int
}

func writeAgentDeckStateDB(t *testing.T, home, profile, id, projectPath, codexSessionID string) {
	t.Helper()
	writeAgentDeckStateDBRows(t, home, profile, []agentDeckStateDBRow{
		{
			ID:             id,
			ProjectPath:    projectPath,
			CodexSessionID: codexSessionID,
			CreatedAt:      1,
			LastAccessed:   2,
		},
	})
}

func writeAgentDeckStateDBRows(t *testing.T, home, profile string, rows []agentDeckStateDBRow) {
	t.Helper()
	writeAgentDeckStateDBRowsAt(t, filepath.Join(home, ".agent-deck", "profiles", profile, "state.db"), rows)
}

func writeAgentDeckStateDBRowsAt(t *testing.T, dbPath string, rows []agentDeckStateDBRow) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		t.Fatalf("mkdir state db dir: %v", err)
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open state db: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE instances (
			id TEXT PRIMARY KEY,
			project_path TEXT NOT NULL,
			tool TEXT NOT NULL,
			command TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			last_accessed INTEGER NOT NULL,
			tool_data TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create instances: %v", err)
	}
	for _, row := range rows {
		toolData, err := json.Marshal(map[string]string{"codex_session_id": row.CodexSessionID})
		if err != nil {
			t.Fatalf("marshal tool data: %v", err)
		}
		if _, err := db.Exec(`
			INSERT INTO instances (id, project_path, tool, command, created_at, last_accessed, tool_data)
			VALUES (?, ?, 'codex', 'codex', ?, ?, ?)
		`, row.ID, row.ProjectPath, row.CreatedAt, row.LastAccessed, string(toolData)); err != nil {
			t.Fatalf("insert instance: %v", err)
		}
	}
}

func callServiceTool(t *testing.T, service *Service, name string, args map[string]any) map[string]any {
	t.Helper()
	if requiresWaypostStatusToolName(name) {
		service.markWaypostStatusCalled()
	}
	return callTool(t, service.Server(), name, args)
}

func callServiceToolWithoutStatusBootstrap(t *testing.T, service *Service, name string, args map[string]any) map[string]any {
	t.Helper()
	return callTool(t, service.Server(), name, args)
}

func readMCPDeliveryEventDetail(t *testing.T, stateDir, deliveryID, eventType string) map[string]any {
	t.Helper()

	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

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

func callTool(t *testing.T, server *mcp.Server, name string, args map[string]any) map[string]any {
	t.Helper()

	ctx := context.Background()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()

	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0.0.1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Wait()
	})

	result, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name:      name,
		Arguments: args,
	})
	if err != nil {
		t.Fatalf("call tool %s: %v", name, err)
	}
	if result.IsError {
		t.Fatalf("tool %s returned error result: %#v", name, result.Content)
	}

	var output map[string]any
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatalf("marshal structured content: %v", err)
	}
	if err := json.Unmarshal(encoded, &output); err != nil {
		t.Fatalf("unmarshal structured content: %v", err)
	}
	return output
}

func assertMCPMapOmitsForwardedMessageID(t *testing.T, payload map[string]any) {
	t.Helper()

	if _, ok := payload["forwarded_message_id"]; ok {
		t.Fatalf("payload unexpectedly exposes forwarded_message_id: %v", payload)
	}
}

func assertMapOmitsHasMore(t *testing.T, payload map[string]any) {
	t.Helper()

	if _, ok := payload["has_more"]; ok {
		t.Fatalf("payload unexpectedly exposes has_more: %v", payload)
	}
}

func callServiceToolExpectError(t *testing.T, service *Service, name string, args map[string]any) error {
	t.Helper()
	if requiresWaypostStatusToolName(name) {
		service.markWaypostStatusCalled()
	}
	return callToolExpectError(t, service.Server(), name, args)
}

func callServiceToolExpectErrorWithoutStatusBootstrap(t *testing.T, service *Service, name string, args map[string]any) error {
	t.Helper()
	return callToolExpectError(t, service.Server(), name, args)
}

func callToolExpectError(t *testing.T, server *mcp.Server, name string, args map[string]any) error {
	t.Helper()

	ctx := context.Background()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()

	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0.0.1"}, nil)
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = serverSession.Wait()
	})

	result, err := clientSession.CallTool(ctx, &mcp.CallToolParams{
		Name:      name,
		Arguments: args,
	})
	if err != nil {
		return err
	}
	if result.IsError {
		encoded, marshalErr := json.Marshal(result.Content)
		if marshalErr != nil {
			return fmt.Errorf("marshal tool error content: %w", marshalErr)
		}
		return fmt.Errorf("%s", encoded)
	}
	if err == nil {
		t.Fatalf("call tool %s unexpectedly succeeded", name)
	}
	return nil
}

func assertNoToolSessionWarning(t *testing.T, output map[string]any) {
	t.Helper()
	warnings, ok := output["warnings"].([]any)
	if !ok {
		return
	}
	for _, warning := range warnings {
		if strings.Contains(fmt.Sprint(warning), "AI tool session auto-bind did not find") {
			t.Fatalf("warnings = %#v, want no tool-session auto-bind warning", warnings)
		}
	}
}

func assertHasToolSessionWarning(t *testing.T, output map[string]any) {
	t.Helper()
	warnings, ok := output["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", output["warnings"])
	}
	for _, warning := range warnings {
		if strings.Contains(fmt.Sprint(warning), "AI tool session auto-bind did not find") {
			return
		}
	}
	t.Fatalf("warnings = %#v, want tool-session auto-bind warning", warnings)
}

func assertHasAgentDeckStateDBWarning(t *testing.T, output map[string]any) {
	t.Helper()
	warnings, ok := output["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", output["warnings"])
	}
	for _, warning := range warnings {
		text := fmt.Sprint(warning)
		if strings.Contains(text, "agent-deck private state database lookup skipped") &&
			strings.Contains(text, "query private instances table") {
			return
		}
	}
	t.Fatalf("warnings = %#v, want agent-deck state DB warning", warnings)
}

func assertHasInvalidToolSessionEnvWarning(t *testing.T, output map[string]any, envName string) {
	t.Helper()
	warnings, ok := output["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want warning list", output["warnings"])
	}
	for _, warning := range warnings {
		text := fmt.Sprint(warning)
		if strings.Contains(text, envName) && strings.Contains(text, "does not look like a hex session id") {
			return
		}
	}
	t.Fatalf("warnings = %#v, want invalid %s warning", warnings, envName)
}

func requiresWaypostStatusToolName(name string) bool {
	for _, required := range requiresWaypostStatusToolNames() {
		if name == required {
			return true
		}
	}
	return false
}

func requiresWaypostStatusToolNames() []string {
	return []string{
		"waypost_bind",
		"waypost_send",
		"waypost_recv",
		"waypost_claim_history",
		"waypost_ack",
		"waypost_release",
		"waypost_defer",
	}
}

func connectTestClientSession(t *testing.T, server *mcp.Server, updateCh chan struct{}) (*mcp.ClientSession, func()) {
	t.Helper()

	ctx := context.Background()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()

	serverSession, err := server.Connect(ctx, serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0.0.1"}, &mcp.ClientOptions{
		ResourceUpdatedHandler: func(context.Context, *mcp.ResourceUpdatedNotificationRequest) {
			if updateCh == nil {
				return
			}
			select {
			case updateCh <- struct{}{}:
			default:
			}
		},
	})
	clientSession, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}

	cleanup := func() {
		_ = clientSession.Close()
		_ = serverSession.Wait()
	}
	return clientSession, cleanup
}
