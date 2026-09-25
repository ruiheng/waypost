package hookcore

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

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
	out, runErr := exec.CommandContext(probeCtx, commandName, commandArgs...).Output()
	if runErr == nil {
		return out, false, nil
	}
	if probeErr := probeCtx.Err(); probeErr != nil {
		return nil, false, fmt.Errorf("run `%s`: %w", commandDesc, probeErr)
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		if missingServer != nil && missingServer(string(exitErr.Stderr)) {
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
	detail := strings.TrimSpace(string(stderr))
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
