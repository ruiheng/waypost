package hookcore

// ReplaceFile atomically moves a completed replacement into place. On
// Windows, existing files are replaced with ReplaceFileW so their DACL is
// preserved; other platforms use the native rename operation.
func ReplaceFile(replacementPath, destinationPath string) error {
	return replaceFile(replacementPath, destinationPath)
}
