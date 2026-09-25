//go:build !windows

package hookcore

func mcpGetInvocation(binary string, args []string) (string, []string) {
	return binary, args
}
