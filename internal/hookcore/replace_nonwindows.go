//go:build !windows

package hookcore

import "os"

func replaceFile(replacementPath, destinationPath string) error {
	return os.Rename(replacementPath, destinationPath)
}
