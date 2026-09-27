package mcpserver

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/ruiheng/waypost/internal/waypost"
)

// These tests pin the wire contract of waypost_send's polymorphic arguments
// directly at the boundary, without booting a Service.

func TestWaypostSendTargetUnmarshalJSON(t *testing.T) {
	t.Parallel()

	t.Run("single string", func(t *testing.T) {
		var target waypostSendTarget
		if err := json.Unmarshal([]byte(`"agent-deck/reviewer"`), &target); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}
		if target.Batch || len(target.Addresses) != 1 || target.Addresses[0] != "agent-deck/reviewer" {
			t.Fatalf("target = %+v, want single non-batch address", target)
		}
	})

	t.Run("array is batch", func(t *testing.T) {
		var target waypostSendTarget
		if err := json.Unmarshal([]byte(`["a/one","b/two"]`), &target); err != nil {
			t.Fatalf("Unmarshal() error = %v", err)
		}
		if !target.Batch || len(target.Addresses) != 2 {
			t.Fatalf("target = %+v, want batch of two", target)
		}
	})

	t.Run("empty array rejected", func(t *testing.T) {
		var target waypostSendTarget
		if err := json.Unmarshal([]byte(`[]`), &target); err == nil {
			t.Fatal("Unmarshal() error = nil, want empty-array rejection")
		}
	})

	t.Run("non-string rejected", func(t *testing.T) {
		var target waypostSendTarget
		if err := json.Unmarshal([]byte(`42`), &target); err == nil {
			t.Fatal("Unmarshal() error = nil, want non-string rejection")
		}
	})
}

func sendToolRequest(t *testing.T, arguments string) *mcp.CallToolRequest {
	t.Helper()
	return &mcp.CallToolRequest{
		Params: &mcp.CallToolParamsRaw{
			Name:      "waypost_send",
			Arguments: json.RawMessage(arguments),
		},
	}
}

func TestPrepareWaypostSendTarget(t *testing.T) {
	t.Parallel()

	t.Run("single recipient", func(t *testing.T) {
		var input waypostSendInput
		req := sendToolRequest(t, `{"to":"agent-deck/reviewer","subject":"s"}`)
		if err := json.Unmarshal(req.Params.Arguments, &input); err != nil {
			t.Fatalf("decode input error = %v", err)
		}
		batch, err := prepareWaypostSendTarget(req, &input)
		if err != nil {
			t.Fatalf("prepareWaypostSendTarget() error = %v", err)
		}
		if batch || input.ToAddress != "agent-deck/reviewer" {
			t.Fatalf("batch=%v ToAddress=%q, want single recipient", batch, input.ToAddress)
		}
	})

	t.Run("batch recipients", func(t *testing.T) {
		var input waypostSendInput
		req := sendToolRequest(t, `{"to":["a/one","b/two"],"subject":"s"}`)
		if err := json.Unmarshal(req.Params.Arguments, &input); err != nil {
			t.Fatalf("decode input error = %v", err)
		}
		batch, err := prepareWaypostSendTarget(req, &input)
		if err != nil {
			t.Fatalf("prepareWaypostSendTarget() error = %v", err)
		}
		if !batch || len(input.ToAddresses) != 2 {
			t.Fatalf("batch=%v ToAddresses=%v, want batch of two", batch, input.ToAddresses)
		}
	})

	t.Run("missing to", func(t *testing.T) {
		var input waypostSendInput
		req := sendToolRequest(t, `{"subject":"s"}`)
		if _, err := prepareWaypostSendTarget(req, &input); err == nil {
			t.Fatal("prepareWaypostSendTarget() error = nil, want missing-to rejection")
		}
	})

	t.Run("no arguments", func(t *testing.T) {
		var input waypostSendInput
		if _, err := prepareWaypostSendTarget(&mcp.CallToolRequest{}, &input); err == nil {
			t.Fatal("prepareWaypostSendTarget() error = nil, want rejection")
		}
	})
}

func TestPrepareWaypostSendBodyValidation(t *testing.T) {
	t.Parallel()

	service := &Service{}

	t.Run("both body and body_file rejected", func(t *testing.T) {
		var input waypostSendInput
		req := sendToolRequest(t, `{"body":"x","body_file":"f.txt"}`)
		if err := service.prepareWaypostSendBody(req, &input); err == nil {
			t.Fatal("prepareWaypostSendBody() error = nil, want mutual-exclusion rejection")
		}
	})

	t.Run("neither body nor body_file rejected", func(t *testing.T) {
		var input waypostSendInput
		req := sendToolRequest(t, `{"subject":"s"}`)
		if err := service.prepareWaypostSendBody(req, &input); err == nil {
			t.Fatal("prepareWaypostSendBody() error = nil, want missing-body rejection")
		}
	})

	t.Run("empty body rejected as empty body", func(t *testing.T) {
		var input waypostSendInput
		req := sendToolRequest(t, `{"body":""}`)
		err := service.prepareWaypostSendBody(req, &input)
		if !errors.Is(err, waypost.ErrEmptyBody) {
			t.Fatalf("prepareWaypostSendBody() error = %v, want ErrEmptyBody", err)
		}
	})

	t.Run("body passes through", func(t *testing.T) {
		input := waypostSendInput{Body: "hello"}
		req := sendToolRequest(t, `{"body":"hello"}`)
		if err := service.prepareWaypostSendBody(req, &input); err != nil {
			t.Fatalf("prepareWaypostSendBody() error = %v", err)
		}
	})
}
