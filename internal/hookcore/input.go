package hookcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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
// false; label names the harness in the parse error. The decode runs on a
// helper goroutine so ctx still bounds the read when the source cannot
// enforce a deadline itself, such as a synchronous Windows pipe handle; on
// timeout the orphaned goroutine exits once the source is closed or the
// process ends.
func ReadHookInput(ctx context.Context, r io.Reader, label string) (HookInput, bool, error) {
	type result struct {
		input    HookInput
		hasInput bool
		err      error
	}
	done := make(chan result, 1)
	go func() {
		var input HookInput
		err := json.NewDecoder(r).Decode(&input)
		switch {
		case errors.Is(err, io.EOF):
			done <- result{}
		case err != nil:
			done <- result{err: fmt.Errorf("parse %s hook input: %w", label, err)}
		default:
			done <- result{input: input, hasInput: true}
		}
	}()
	select {
	case res := <-done:
		return res.input, res.hasInput, res.err
	case <-ctx.Done():
		err := ctx.Err()
		if errors.Is(err, context.DeadlineExceeded) {
			err = os.ErrDeadlineExceeded
		}
		return HookInput{}, false, fmt.Errorf("parse %s hook input: %w", label, err)
	}
}
