package hookcore

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/ruiheng/waypost/internal/hideconsole"
)

var ansiEscapePattern = regexp.MustCompile("\x1b(?:\\[[0-?]*[ -/]*[@-~]|\\][^\x07\x1b]*(?:\x07|\x1b\\\\)|[PX^_][^\x1b]*\x1b\\\\|[ -/]+[0-~]|[0-OQ-WYZ\\\\`-~])")

// StripANSI removes ANSI/ECMA-48 escape sequences (CSI, OSC, string sequences,
// nF charset designations such as `\x1b(B`, and 2-byte ESC controls) so structured
// line parsers see clean text regardless of TTY colorization or terminfo resets.
func StripANSI(text string) string {
	return ansiEscapePattern.ReplaceAllString(text, "")
}

// RunMCPGetProbe runs `<binary> mcp get waypost` (plus extraArgs) with the
// given timeout and returns its standard output. A non-zero exit whose
// stderr satisfies missingServer reports an absent server entry: it yields
// (nil, true, nil) so adapters treat "not configured" as unavailable rather
// than a probe failure. Other failures are wrapped with the rendered
// command line.
func RunMCPGetProbe(
	ctx context.Context,
	timeout time.Duration,
	binary string,
	missingServer func(string) bool,
	extraArgs ...string,
) (output []byte, missing bool, err error) {
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	args := append([]string{"mcp", "get", "waypost"}, extraArgs...)
	commandName, commandArgs := mcpGetInvocation(binary, args)
	commandDesc := binary + " " + strings.Join(args, " ")
	probeCmd := exec.CommandContext(probeCtx, commandName, commandArgs...)
	hideconsole.Hide(probeCmd)
	out, runErr := probeCmd.Output()
	if runErr == nil {
		return out, false, nil
	}
	if probeErr := probeCtx.Err(); probeErr != nil {
		return nil, false, fmt.Errorf("run `%s`: %w", commandDesc, probeErr)
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		if missingServer != nil && missingServer(StripANSI(string(exitErr.Stderr))) {
			return nil, true, nil
		}
		if detail := boundedProbeErrorDetail(exitErr.Stderr); detail != "" {
			return nil, false, fmt.Errorf("run `%s`: %w: %s", commandDesc, runErr, detail)
		}
	}
	return nil, false, fmt.Errorf("run `%s`: %w", commandDesc, runErr)
}

// boundedProbeErrorDetail truncates captured stderr to a bounded prefix and
// suffix so a chatty CLI cannot flood the hook payload.
func boundedProbeErrorDetail(stderr []byte) string {
	detail := strings.TrimSpace(StripANSI(string(stderr)))
	const maxRunes = 500
	runes := []rune(detail)
	if len(runes) > maxRunes {
		const marker = "…"
		headRunes := maxRunes / 2
		tailRunes := maxRunes - headRunes - len([]rune(marker))
		detail = string(runes[:headRunes]) + marker + string(runes[len(runes)-tailRunes:])
	}
	return detail
}
