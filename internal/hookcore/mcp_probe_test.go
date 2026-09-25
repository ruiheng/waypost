package hookcore

import (
	"strings"
	"testing"
)

func TestBoundedProbeErrorDetailPreservesHeadAndTail(t *testing.T) {
	t.Parallel()

	detail := boundedProbeErrorDetail([]byte("error prefix: " + strings.Repeat("x", 600) + " :root cause"))
	if !strings.HasPrefix(detail, "error prefix: ") || !strings.HasSuffix(detail, " :root cause") {
		t.Fatalf("boundedProbeErrorDetail() = %q, want preserved head and tail", detail)
	}
	if got := len([]rune(detail)); got != 500 {
		t.Fatalf("boundedProbeErrorDetail() length = %d, want 500 runes", got)
	}
}
