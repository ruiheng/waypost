package mcpserver

import (
	"os"
	"testing"

	"github.com/ruiheng/waypost/internal/testenv"
)

// TestMain floors user-directory env at a throwaway root: tests that reach
// agent-deck or agent config resolution without their own isolation must not
// observe the real user profile. Tests needing a different layout re-pin via
// t.Setenv, which wins over this floor.
func TestMain(m *testing.M) {
	os.Exit(testenv.RunMain(m))
}
