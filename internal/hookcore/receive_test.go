package hookcore

import (
	"encoding/json"
	"testing"
)

func TestReceiveOutputSucceeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		output string
		want   bool
	}{
		{`{"status":"received","delivery_id":"d1","recipient_address":"a","lease_token":"l"}`, true},
		{`{"status":"no_message"}`, true},
		{`{"status":"failed"}`, false},
		{`{"delivery_id":"d1","recipient_address":"a","lease_token":"l"}`, true},
		{`{"message_id":"m","group_address":"g","person":"p","first_read_at":"t"}`, true},
		{`{"messages":[{"delivery_id":"d1","recipient_address":"a","lease_token":"l"}]}`, true},
		{`{"messages":[{"delivery_id":"d1"}]}`, false},
		{"status=no_message", true},
		{`status: "received"`, true},
		{`status: "failed"`, false},
		{"delivery_id=d1 recipient_address=a lease_token=l", true},
		{"message_id=m group=g person=p first_read_at=t", true},
		{"delivery_id=d1", false},
		{"random output", false},
		{"", false},
	}
	for _, test := range tests {
		if got := ReceiveOutputSucceeded(test.output); got != test.want {
			t.Errorf("ReceiveOutputSucceeded(%q) = %v, want %v", test.output, got, test.want)
		}
	}
}

func TestMCPReceiveResultSucceeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		output string
		want   bool
	}{
		{`{"isError":false,"structuredContent":{"status":"received"}}`, true},
		{`{"isError":false,"structuredContent":{"status":"no_message"}}`, true},
		{`{"isError":false,"status":"received"}`, true},
		{`{"isError":true,"structuredContent":{"status":"received"}}`, false},
		{`{"isError":false,"structuredContent":{"status":"failed"}}`, false},
		{`{"status":"received","delivery_id":"d","recipient_address":"a","lease_token":"l"}`, true},
		{`{"status":"failed"}`, false},
		{"not json", false},
		{"", false},
	}
	for _, test := range tests {
		if got := MCPReceiveResultSucceeded(test.output); got != test.want {
			t.Errorf("MCPReceiveResultSucceeded(%q) = %v, want %v", test.output, got, test.want)
		}
	}
}

func TestShellReceiveOutputSucceeded(t *testing.T) {
	t.Parallel()

	if !ShellReceiveOutputSucceeded(json.RawMessage(`"status=no_message"`)) {
		t.Error("ShellReceiveOutputSucceeded(no_message) = false, want true")
	}
	if ShellReceiveOutputSucceeded(json.RawMessage(`"delivery_id=d1"`)) {
		t.Error("ShellReceiveOutputSucceeded(partial) = true, want false")
	}
	if ShellReceiveOutputSucceeded(json.RawMessage(`{}`)) {
		t.Error("ShellReceiveOutputSucceeded(non-string) = true, want false")
	}
	if ShellReceiveOutputSucceeded(nil) {
		t.Error("ShellReceiveOutputSucceeded(nil) = true, want false")
	}
}
