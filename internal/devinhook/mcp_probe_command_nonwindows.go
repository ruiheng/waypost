//go:build !windows

package devinhook

func mcpProbeInvocation(args ...string) (string, []string) {
	return "devin", args
}
