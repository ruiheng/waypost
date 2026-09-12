//go:build windows

package hookcore

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

const replaceFileWriteThrough = 0x00000001

var replaceFileW = syscall.NewLazyDLL("kernel32.dll").NewProc("ReplaceFileW")

func replaceFile(replacementPath, destinationPath string) error {
	_, err := os.Lstat(destinationPath)
	switch {
	case err == nil:
		return replaceExistingWindowsFile(replacementPath, destinationPath)
	case errors.Is(err, os.ErrNotExist):
		return moveNewWindowsFile(replacementPath, destinationPath)
	default:
		return err
	}
}

func replaceExistingWindowsFile(replacementPath, destinationPath string) error {
	destinationPath, err := windowsAPIPath(destinationPath)
	if err != nil {
		return err
	}
	replacementPath, err = windowsAPIPath(replacementPath)
	if err != nil {
		return err
	}
	destination, err := syscall.UTF16PtrFromString(destinationPath)
	if err != nil {
		return err
	}
	replacement, err := syscall.UTF16PtrFromString(replacementPath)
	if err != nil {
		return err
	}
	result, _, callErr := replaceFileW.Call(
		uintptr(unsafe.Pointer(destination)),
		uintptr(unsafe.Pointer(replacement)),
		0,
		replaceFileWriteThrough,
		0,
		0,
	)
	if result != 0 {
		return nil
	}
	if callErr != syscall.Errno(0) {
		return callErr
	}
	return syscall.EINVAL
}

func moveNewWindowsFile(replacementPath, destinationPath string) error {
	replacementPath, err := windowsAPIPath(replacementPath)
	if err != nil {
		return err
	}
	destinationPath, err = windowsAPIPath(destinationPath)
	if err != nil {
		return err
	}
	replacement, err := syscall.UTF16PtrFromString(replacementPath)
	if err != nil {
		return err
	}
	destination, err := syscall.UTF16PtrFromString(destinationPath)
	if err != nil {
		return err
	}
	return syscall.MoveFile(replacement, destination)
}

// windowsAPIPath mirrors the extended-length path handling used by Go's os
// package before passing paths to Win32 APIs directly. Short paths keep their
// original spelling; long relative paths become absolute.
func windowsAPIPath(path string) (string, error) {
	if windowsExtendedOrDevicePath(path) {
		return path, nil
	}
	if filepath.IsAbs(path) && len(path) < 248 {
		return path, nil
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if len(absolutePath) < 248 {
		return path, nil
	}
	if windowsExtendedOrDevicePath(absolutePath) {
		return absolutePath, nil
	}
	if len(absolutePath) >= 2 && isWindowsPathSeparator(absolutePath[0]) && isWindowsPathSeparator(absolutePath[1]) {
		return `\\?\UNC\` + absolutePath[2:], nil
	}
	return `\\?\` + absolutePath, nil
}

func windowsExtendedOrDevicePath(path string) bool {
	if strings.HasPrefix(path, `\??\`) {
		return true
	}
	return len(path) >= 4 &&
		isWindowsPathSeparator(path[0]) &&
		isWindowsPathSeparator(path[1]) &&
		(path[2] == '?' || path[2] == '.') &&
		isWindowsPathSeparator(path[3])
}

func isWindowsPathSeparator(value byte) bool {
	return value == '\\' || value == '/'
}
