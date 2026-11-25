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

func TestStripANSI(t *testing.T) {
	t.Parallel()

	raw := "\x1bc\x1b[32mwaypost:\x1b(B\x1b)0\x1b[m\r\n  \x1b[1;34mStatus:\x1b[0m \x1b]8;;https://example.com\x07Connected\x1b]8;;\x1b\\\n\x1b=plain text"
	want := "waypost:\r\n  Status: Connected\nplain text"
	if got := StripANSI(raw); got != want {
		t.Fatalf("StripANSI() = %q, want %q", got, want)
	}
	if got := StripANSI("plain unchanged output"); got != "plain unchanged output" {
		t.Fatalf("StripANSI(plain) = %q, want unchanged", got)
	}
	// Truncated multi-byte OSC introducer must not have its 2-byte prefix stripped cleanly while leaving URL tail.
	if got := StripANSI("\x1b]8;;https://example.com"); got != "\x1b]8;;https://example.com" {
		t.Fatalf("StripANSI(truncated OSC) = %q, want intact string", got)
	}
	// Unterminated DCS must not swallow following section text up to a later ST-terminated OSC.
	unterminatedDCS := "\x1bPbroken\nwaypost:\n  Status: ✔ Connected\n\x1b]8;;\x1b\\"
	if got := StripANSI(unterminatedDCS); !strings.Contains(got, "waypost:\n  Status: ✔ Connected\n") {
		t.Fatalf("StripANSI(unterminated DCS) = %q, must preserve waypost section", got)
	}
}
