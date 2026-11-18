package hideconsole

import (
	"os/exec"
	"testing"
)

func TestHideAndHideIfConsolelessHandleNilAndCommands(t *testing.T) {
	Hide(nil)
	HideIfConsoleless(nil)

	cmd := exec.Command("go", "version")
	Hide(cmd)

	cmdConditional := exec.Command("go", "version")
	HideIfConsoleless(cmdConditional)
}
