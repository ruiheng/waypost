// Package hooktest holds shared assertions for harnesses built on hookcore,
// so lifecycle guarantees that must hold identically for every harness — and
// on every platform — are specified once instead of re-encoded per package.
package hooktest

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/ruiheng/waypost/internal/hookcore"
)

// CheckStalledInputExitsWithinBudget feeds run a stdin pipe whose payload
// never arrives. The run context's internal budget must end the wait well
// below the harness deadline — Windows pipe handles do not support read
// deadlines, so this only passes when the input decode is bound to the run
// context. verify receives the captured output and run's result so each
// harness asserts its own EmitInputTimeout contract (a harness payload such
// as a systemMessage, or the propagated os.ErrDeadlineExceeded error).
func CheckStalledInputExitsWithinBudget(t *testing.T, budget time.Duration, run func(context.Context, io.Reader, io.Writer) error, verify func(t *testing.T, output []byte, runErr error)) {
	t.Helper()

	previous := hookcore.RunBudget
	hookcore.RunBudget = budget
	t.Cleanup(func() { hookcore.RunBudget = previous })

	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}
	defer readEnd.Close()
	defer writeEnd.Close() // payload never arrives

	var output bytes.Buffer
	start := time.Now()
	runErr := run(context.Background(), readEnd, &output)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("run() took %v, want exit well below the harness deadline", elapsed)
	}
	verify(t, output.Bytes(), runErr)
}
