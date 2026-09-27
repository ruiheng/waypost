package hookcore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestBeginRunBoundsContextAndPipeRead(t *testing.T) {
	previous := RunBudget
	RunBudget = 50 * time.Millisecond
	defer func() { RunBudget = previous }()

	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	defer readEnd.Close()
	defer writeEnd.Close() // payload never arrives

	ctx, cancel := BeginRun(context.Background(), readEnd)
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("BeginRun() ctx has no deadline")
	}

	_, _, err = ReadHookInput(ctx, readEnd, "Test")
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("ReadHookInput() error = %v, want os.ErrDeadlineExceeded", err)
	}
}

func TestSetInputReadDeadlineBoundsPipeRead(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows pipe handles are synchronous and do not support read deadlines; BeginRun bounds them through ctx instead")
	}
	t.Parallel()

	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	defer readEnd.Close()
	defer writeEnd.Close() // payload never arrives

	SetInputReadDeadline(readEnd, time.Now().Add(20*time.Millisecond))

	start := time.Now()
	_, _, err = ReadHookInput(context.Background(), readEnd, "Test")
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("ReadHookInput() error = %v, want os.ErrDeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("ReadHookInput() blocked for %v, want prompt deadline failure", elapsed)
	}
}

func TestSetInputReadDeadlineIgnoresUnsupportedSources(t *testing.T) {
	t.Parallel()

	input := bytes.NewReader([]byte(`{"hook_event_name":"SessionStart"}`))
	SetInputReadDeadline(input, time.Now().Add(-time.Second))

	parsed, hasInput, err := ReadHookInput(context.Background(), input, "Test")
	if err != nil || !hasInput {
		t.Fatalf("ReadHookInput() = %v, %v, %v; want parsed input", parsed, hasInput, err)
	}
	if parsed.HookEventName != "SessionStart" {
		t.Fatalf("HookEventName = %q, want SessionStart", parsed.HookEventName)
	}
}
