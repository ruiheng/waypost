package hookcore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// HookInput is the common shape harnesses send to hook commands on stdin.
type HookInput struct {
	HookEventName string          `json:"hook_event_name"`
	SessionID     string          `json:"session_id"`
	Source        string          `json:"source"`
	Prompt        string          `json:"prompt"`
	ToolName      string          `json:"tool_name"`
	ToolInput     json.RawMessage `json:"tool_input"`
	ToolResponse  json.RawMessage `json:"tool_response"`
}

// ReadHookInput decodes one hook payload. An empty stream reports hasInput
// false; label names the harness in the parse error.
func ReadHookInput(r io.Reader, label string) (HookInput, bool, error) {
	var input HookInput
	err := json.NewDecoder(r).Decode(&input)
	if errors.Is(err, io.EOF) {
		return HookInput{}, false, nil
	}
	if err != nil {
		return HookInput{}, false, fmt.Errorf("parse %s hook input: %w", label, err)
	}
	return input, true, nil
}
