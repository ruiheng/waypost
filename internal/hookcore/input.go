package hookcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
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

// inputReadDeadliner is implemented by hook input sources that can bound a
// read, such as an *os.File backed by a pipe or socket.
type inputReadDeadliner interface {
	SetReadDeadline(time.Time) error
}

// SetInputReadDeadline best-effort bounds reads on r at d. Sources able to
// enforce a deadline then fail pending and future reads with
// os.ErrDeadlineExceeded; anything else is left alone so the hook degrades to
// harness enforcement instead of failing.
func SetInputReadDeadline(r io.Reader, d time.Time) {
	if deadliner, ok := r.(inputReadDeadliner); ok {
		_ = deadliner.SetReadDeadline(d)
	}
}

// RunBudget bounds a single hook invocation below the harness deadline so a
// stalled stdin payload or downstream probe surfaces as a fast, visible error
// instead of an opaque kill. It is a variable so tests can shorten it.
var RunBudget = 4 * time.Second

// BeginRun applies RunBudget to one hook invocation: ctx is bounded for
// downstream work and reads on r are bounded to the same deadline.
func BeginRun(ctx context.Context, r io.Reader) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, RunBudget)
	if deadline, ok := ctx.Deadline(); ok {
		SetInputReadDeadline(r, deadline)
	}
	return ctx, cancel
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
