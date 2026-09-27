package waypost

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/ruiheng/waypost/internal/userdirs"
)

const (
	legacyStateDirSuffix   = "ai-agent/mailbox"
	legacyDatabaseFilename = "mailbox.db"

	migrationMarkerVersion         = 3
	legacyStagedMarkerVersion      = 2
	migrationMarkerSuffix          = ".waypost-migration"
	migrationOwnershipFilename     = ".waypost-migration-owner"
	migrationCopyingMarkerSuffix   = ".copying"
	migrationCopiedMarkerSuffix    = ".copied"
	legacyCopiedMarkerContents     = "copied\n"
	migrationCopyingMarkerContents = "copying-v2\n"
	migrationCopiedMarkerContents  = "copy-committed-v2\n"
)

type MigrationResult struct {
	Source         string
	Destination    string
	SourceRetained bool
}

type migrationMarker struct {
	Version     int            `json:"version"`
	Source      string         `json:"source"`
	Destination string         `json:"destination"`
	Stage       migrationStage `json:"stage,omitempty"`
}

type migrationOwnership struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

type migrationStage string

const (
	migrationStageMoving        migrationStage = "moving"
	migrationStageCopying       migrationStage = "copying"
	migrationStageCopyCommitted migrationStage = "copy-committed"
)

// MigrateLegacyState transfers one prior state directory into the current
// Waypost location. sourceOverride is required for a non-default legacy
// location. Windows cross-volume copies retain the source as a recovery copy.
func MigrateLegacyState(destinationOverride, sourceOverride string) (MigrationResult, error) {
	return migrateLegacyState(destinationOverride, sourceOverride, os.Rename)
}

func migrateLegacyState(destinationOverride, sourceOverride string, renameDirectory func(string, string) error) (MigrationResult, error) {
	destination, err := resolveStateDir(destinationOverride)
	if err != nil {
		return MigrationResult{}, err
	}
	source, err := resolveLegacyStateDir(sourceOverride)
	if err != nil {
		return MigrationResult{}, err
	}
	if err := validateMigrationPaths(source, destination); err != nil {
		return MigrationResult{}, err
	}

	sourceExists, err := stateDirectoryExists(source, "legacy")
	if err != nil {
		return MigrationResult{}, err
	}
	destinationExists, err := stateDirectoryExists(destination, "current")
	if err != nil {
		return MigrationResult{}, err
	}

	marker, markerExists, err := readMigrationMarker(destination)
	if err != nil {
		return MigrationResult{}, err
	}
	if markerExists {
		if err := validateMigrationMarker(marker, source, destination); err != nil {
			return MigrationResult{}, err
		}
		return resumeMigration(marker, source, destination, sourceExists, destinationExists, renameDirectory)
	}

	stageMarkersExist, err := migrationStageMarkersExist(destination)
	if err != nil {
		return MigrationResult{}, err
	}
	if stageMarkersExist {
		recovered, err := recoverOrphanedLegacyStageMarkers(destination)
		if err != nil {
			return MigrationResult{}, err
		}
		if !recovered {
			return MigrationResult{}, fmt.Errorf("migration stage markers exist for current state directory %q without a migration marker; refusing to create or use the directory", destination)
		}
		return MigrationResult{
			Source:         source,
			Destination:    destination,
			SourceRetained: sourceExists,
		}, nil
	}
	ownership, ownershipExists, err := readMigrationOwnership(destination)
	if err != nil {
		return MigrationResult{}, err
	}
	if ownershipExists {
		if ownership.Source != source || ownership.Destination != destination {
			return MigrationResult{}, fmt.Errorf("migration ownership for current state directory %q does not match requested source %q and destination %q", destination, source, destination)
		}
		if result, recovered, err := resumeFinalizedRetainedOwnedDestination(source, destination, sourceExists, destinationExists); recovered {
			return result, err
		}
		if sourceExists && destinationExists {
			return restartMigrationFromSource(source, destination, renameDirectory)
		}
		if !sourceExists && destinationExists {
			return MigrationResult{}, fmt.Errorf("migration-owned current state directory %q has no primary marker and its source %q is missing; refusing to use an unproven copied destination", destination, source)
		}
	}
	if destinationExists {
		return MigrationResult{}, fmt.Errorf("current state directory %q already exists without migration ownership evidence; refusing to remove it automatically", destination)
	}
	if !sourceExists {
		return MigrationResult{}, fmt.Errorf("legacy state directory %q does not exist", source)
	}

	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return MigrationResult{}, fmt.Errorf("create current state parent directory: %w", err)
	}
	return startMigration(source, destination, renameDirectory)
}

func startMigration(source, destination string, renameDirectory func(string, string) error) (MigrationResult, error) {
	marker := newMigrationMarker(source, destination)
	if err := writeMigrationMarker(destination, marker); err != nil {
		return MigrationResult{}, err
	}
	return continueMoveMigration(marker, source, destination, renameDirectory)
}

func resumeMigration(marker migrationMarker, source, destination string, sourceExists, destinationExists bool, renameDirectory func(string, string) error) (MigrationResult, error) {
	switch marker.Version {
	case migrationMarkerVersion:
		return resumeCurrentMigration(marker, source, destination, sourceExists, destinationExists, renameDirectory)
	case legacyStagedMarkerVersion:
		return resumeStagedMigration(marker, source, destination, sourceExists, destinationExists, renameDirectory)
	case 0:
		return resumeLegacyMigration(source, destination, sourceExists, destinationExists, renameDirectory)
	default:
		return MigrationResult{}, fmt.Errorf("migration marker for %q has unsupported version %d", destination, marker.Version)
	}
}

func resumeCurrentMigration(marker migrationMarker, source, destination string, sourceExists, destinationExists bool, renameDirectory func(string, string) error) (MigrationResult, error) {
	switch marker.Stage {
	case migrationStageMoving:
		if result, recovered, err := resumeFinalizedRetainedCopy(marker, source, destination, sourceExists, destinationExists); recovered {
			return result, err
		}
		return resumeMoveMigration(marker, source, destination, sourceExists, destinationExists, renameDirectory)
	case migrationStageCopying:
		if result, recovered, err := resumeFinalizedRetainedCopy(marker, source, destination, sourceExists, destinationExists); recovered {
			return result, err
		}
		return resumeCopyMigration(marker, source, destination, sourceExists, destinationExists)
	case migrationStageCopyCommitted:
		if result, recovered, err := resumeFinalizedRetainedCopy(marker, source, destination, sourceExists, destinationExists); recovered {
			return result, err
		}
		if sourceExists && rebuildCopiedStateOnResume() {
			return restartCopiedMigrationFromSource(marker, source, destination, destinationExists)
		}
		if !destinationExists {
			return MigrationResult{}, fmt.Errorf("copy-committed migration from %q to %q lost its destination", source, destination)
		}
		return completeCopiedMigration(source, destination, sourceExists)
	default:
		return MigrationResult{}, fmt.Errorf("migration marker for %q has invalid stage %q", destination, marker.Stage)
	}
}

// Windows retains the source because it cannot durably sync directory entries.
// If the target has already completed its database rename, it may have accepted
// new Waypost data; preserve it and repair the marker instead of recopying.
func resumeFinalizedRetainedCopy(marker migrationMarker, source, destination string, sourceExists, destinationExists bool) (MigrationResult, bool, error) {
	return resumeFinalizedRetainedDestination(marker, source, destination, sourceExists, destinationExists, replaceMigrationMarker)
}

// A surviving ownership record proves that the destination was created by the
// migration. On Windows, cleanup can lose the primary marker even though the
// copied destination has already been finalized and used. Recreate its marker
// before returning it, so future retries retain the same recovery contract.
func resumeFinalizedRetainedOwnedDestination(source, destination string, sourceExists, destinationExists bool) (MigrationResult, bool, error) {
	return resumeFinalizedRetainedDestination(newMigrationMarker(source, destination), source, destination, sourceExists, destinationExists, writeMigrationMarker)
}

func resumeFinalizedRetainedDestination(marker migrationMarker, source, destination string, sourceExists, destinationExists bool, persistMarker func(string, migrationMarker) error) (MigrationResult, bool, error) {
	if !rebuildCopiedStateOnResume() || !sourceExists || !destinationExists {
		return MigrationResult{}, false, nil
	}
	completed, err := databaseRenameCompleted(destination)
	if err != nil {
		return MigrationResult{}, true, err
	}
	if !completed {
		return MigrationResult{}, false, nil
	}
	marker.Version = migrationMarkerVersion
	marker.Stage = migrationStageCopyCommitted
	if err := persistMarker(destination, marker); err != nil {
		return MigrationResult{}, true, err
	}
	result, err := completeCopiedMigration(source, destination, true)
	return result, true, err
}

// Version 2 used separate stage files. Upgrade its stage into the primary
// marker before continuing so every later recovery decision is self-contained.
func resumeStagedMigration(marker migrationMarker, source, destination string, sourceExists, destinationExists bool, renameDirectory func(string, string) error) (MigrationResult, error) {
	stage, err := readLegacyMigrationStage(destination)
	if err != nil {
		return MigrationResult{}, err
	}

	// In version 2, cleanup renamed the database before deleting stage files.
	// That completed rename is durable evidence for the otherwise ambiguous
	// partial-cleanup states where the copy-commit file is already gone.
	if stage != migrationStageCopyCommitted && destinationExists {
		completed, err := databaseRenameCompleted(destination)
		if err != nil {
			return MigrationResult{}, err
		}
		if completed {
			stage = migrationStageCopyCommitted
		}
	}

	marker.Version = migrationMarkerVersion
	marker.Stage = stage
	if err := replaceMigrationMarker(destination, marker); err != nil {
		return MigrationResult{}, err
	}
	return resumeCurrentMigration(marker, source, destination, sourceExists, destinationExists, renameDirectory)
}

func resumeLegacyMigration(source, destination string, sourceExists, destinationExists bool, renameDirectory func(string, string) error) (MigrationResult, error) {
	if sourceExists && destinationExists {
		return restartMigrationFromSource(source, destination, renameDirectory)
	}
	if sourceExists {
		if err := removeMigrationMarkers(destination); err != nil {
			return MigrationResult{}, err
		}
		return startMigration(source, destination, renameDirectory)
	}
	if destinationExists {
		copied, err := legacyCopyCommitRecorded(destination)
		if err != nil {
			return MigrationResult{}, err
		}
		if !copied {
			return MigrationResult{}, fmt.Errorf("legacy migration from %q to %q lacks a durable copy commit and its source is missing; refusing to treat the destination as complete", source, destination)
		}
		return finishMigration(source, destination, false)
	}
	return MigrationResult{}, fmt.Errorf("incomplete legacy migration from %q to %q lost both source and destination", source, destination)
}

func resumeMoveMigration(marker migrationMarker, source, destination string, sourceExists, destinationExists bool, renameDirectory func(string, string) error) (MigrationResult, error) {
	switch {
	case sourceExists && !destinationExists:
		return continueMoveMigration(marker, source, destination, renameDirectory)
	case !sourceExists && destinationExists:
		return finishMigration(source, destination, false)
	case sourceExists && destinationExists:
		return restartMigrationFromSource(source, destination, renameDirectory)
	default:
		return MigrationResult{}, fmt.Errorf("incomplete move migration from %q to %q lost both source and destination", source, destination)
	}
}

func continueMoveMigration(marker migrationMarker, source, destination string, renameDirectory func(string, string) error) (MigrationResult, error) {
	if err := renameDirectory(source, destination); err == nil {
		return finishMigration(source, destination, false)
	} else if !isCrossDeviceRenameError(err) {
		return MigrationResult{}, fmt.Errorf("move legacy state directory from %q to %q: %w", source, destination, err)
	}

	return startCopyMigration(marker, source, destination)
}

func startCopyMigration(marker migrationMarker, source, destination string) (MigrationResult, error) {
	marker.Stage = migrationStageCopying
	if err := replaceMigrationMarker(destination, marker); err != nil {
		return MigrationResult{}, err
	}
	return copyAndCommitMigration(marker, source, destination)
}

func resumeCopyMigration(marker migrationMarker, source, destination string, sourceExists, destinationExists bool) (MigrationResult, error) {
	if !sourceExists {
		if destinationExists {
			return MigrationResult{}, fmt.Errorf("incomplete copied migration from %q to %q has no durable copy commit and its source is missing; refusing to use the destination", source, destination)
		}
		return MigrationResult{}, fmt.Errorf("incomplete copied migration from %q to %q lost both source and destination", source, destination)
	}
	return copyFromSource(marker, source, destination, destinationExists)
}

func restartMigrationFromSource(source, destination string, renameDirectory func(string, string) error) (MigrationResult, error) {
	if err := discardIncompleteCopiedState(destination); err != nil {
		return MigrationResult{}, err
	}
	if err := removeMigrationMarkers(destination); err != nil {
		return MigrationResult{}, err
	}
	return startMigration(source, destination, renameDirectory)
}

func restartCopiedMigrationFromSource(marker migrationMarker, source, destination string, destinationExists bool) (MigrationResult, error) {
	marker.Stage = migrationStageCopying
	if err := replaceMigrationMarker(destination, marker); err != nil {
		return MigrationResult{}, err
	}
	return copyFromSource(marker, source, destination, destinationExists)
}

func copyFromSource(marker migrationMarker, source, destination string, destinationExists bool) (MigrationResult, error) {
	if destinationExists {
		if err := discardIncompleteCopiedState(destination); err != nil {
			return MigrationResult{}, err
		}
	}
	return copyAndCommitMigration(marker, source, destination)
}

func copyAndCommitMigration(marker migrationMarker, source, destination string) (MigrationResult, error) {
	if err := copyStateDirectory(source, destination, marker); err != nil {
		return MigrationResult{}, fmt.Errorf("copy legacy state directory from %q to %q: %w", source, destination, err)
	}
	if err := syncCopiedStateDirectory(destination); err != nil {
		return MigrationResult{}, err
	}
	marker.Stage = migrationStageCopyCommitted
	if err := replaceMigrationMarker(destination, marker); err != nil {
		return MigrationResult{}, err
	}
	return completeCopiedMigration(source, destination, true)
}

func completeCopiedMigration(source, destination string, sourceExists bool) (MigrationResult, error) {
	sourceRetained := retainLegacySourceAfterCopy() && sourceExists
	if sourceExists && !sourceRetained {
		if err := removeMigratedLegacyState(source); err != nil {
			return MigrationResult{}, err
		}
	}
	return finishMigration(source, destination, sourceRetained)
}

func finishMigration(source, destination string, sourceRetained bool) (MigrationResult, error) {
	if err := renameLegacyDatabaseFiles(destination); err != nil {
		return MigrationResult{}, fmt.Errorf("finish legacy database rename: %w", err)
	}
	if sourceRetained && keepMigrationRecoveryMarker() {
		if err := removeLegacyMigrationStageMarkers(destination); err != nil {
			return MigrationResult{}, err
		}
	} else {
		if err := removeMigrationOwnership(destination); err != nil {
			return MigrationResult{}, err
		}
		if err := removeMigrationMarkers(destination); err != nil {
			return MigrationResult{}, err
		}
	}
	return MigrationResult{
		Source:         source,
		Destination:    destination,
		SourceRetained: sourceRetained,
	}, nil
}

func removeMigratedLegacyState(source string) error {
	if err := os.RemoveAll(source); err != nil {
		return fmt.Errorf("remove migrated legacy state %q: %w", source, err)
	}
	if err := syncMigrationDirectoryHierarchy(filepath.Dir(source)); err != nil {
		return fmt.Errorf("persist removal of migrated legacy state %q: %w", source, err)
	}
	return nil
}

func discardIncompleteCopiedState(destination string) error {
	if err := os.RemoveAll(destination); err != nil {
		return fmt.Errorf("remove incomplete copied state %q: %w", destination, err)
	}
	if err := syncMigrationDirectoryHierarchy(filepath.Dir(destination)); err != nil {
		return fmt.Errorf("persist removal of incomplete copied state %q: %w", destination, err)
	}
	return nil
}

func stateDirectoryExists(path, kind string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect %s state directory %q: %w", kind, path, err)
	}
	if !info.IsDir() {
		return false, fmt.Errorf("%s state path %q is not a directory", kind, path)
	}
	return true, nil
}

func ensureDefaultLegacyStateMigrated(overrideStateDir, stateDir string) error {
	if overrideStateDir != "" || os.Getenv("WAYPOST_STATE_DIR") != "" {
		return nil
	}

	stateExists, err := stateDirectoryExists(stateDir, "current")
	if err != nil {
		return err
	}
	if stateExists {
		return nil
	}

	legacyStateDir, err := resolveLegacyStateDir("")
	if err != nil {
		return err
	}
	legacyExists, err := stateDirectoryExists(legacyStateDir, "legacy")
	if err != nil {
		return err
	}
	if !legacyExists {
		return nil
	}

	return fmt.Errorf("legacy state directory %q exists; run waypost migrate before using the default state directory %q", legacyStateDir, stateDir)
}

func validateMigrationPaths(source, destination string) error {
	sourcePath, err := filepath.Abs(source)
	if err != nil {
		return fmt.Errorf("resolve legacy state directory %q: %w", source, err)
	}
	destinationPath, err := filepath.Abs(destination)
	if err != nil {
		return fmt.Errorf("resolve current state directory %q: %w", destination, err)
	}

	overlaps, err := migrationPathsOverlap(sourcePath, destinationPath)
	if err != nil {
		return err
	}
	if overlaps {
		return fmt.Errorf("legacy and current state directories must not overlap: %q and %q", source, destination)
	}
	return nil
}

func migrationPathsOverlap(first, second string) (bool, error) {
	firstContainsSecond, err := migrationPathContains(first, second)
	if err != nil {
		return false, err
	}
	if firstContainsSecond {
		return true, nil
	}
	return migrationPathContains(second, first)
}

func migrationPathContains(parent, child string) (bool, error) {
	if !strings.EqualFold(filepath.VolumeName(parent), filepath.VolumeName(child)) {
		return false, nil
	}
	relativePath, err := filepath.Rel(parent, child)
	if err != nil {
		return false, fmt.Errorf("compare migration paths %q and %q: %w", parent, child, err)
	}
	return filepath.IsLocal(relativePath), nil
}

func resolveLegacyStateDir(override string) (string, error) {
	if override != "" {
		return filepath.Clean(override), nil
	}
	root, err := userdirs.StateRoot(os.UserHomeDir)
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(root, legacyStateDirSuffix), nil
}

func newMigrationMarker(source, destination string) migrationMarker {
	return migrationMarker{
		Version:     migrationMarkerVersion,
		Source:      source,
		Destination: destination,
		Stage:       migrationStageMoving,
	}
}

func migrationMarkerPath(destination string) string {
	return destination + migrationMarkerSuffix
}

func migrationOwnershipPath(destination string) string {
	return filepath.Join(destination, migrationOwnershipFilename)
}

func migrationCopyingMarkerPath(destination string) string {
	return migrationMarkerPath(destination) + migrationCopyingMarkerSuffix
}

func migrationCopiedMarkerPath(destination string) string {
	return migrationMarkerPath(destination) + migrationCopiedMarkerSuffix
}

func migrationStageMarkerPaths(destination string) []string {
	return []string{
		migrationCopyingMarkerPath(destination),
		migrationCopiedMarkerPath(destination),
	}
}

func readMigrationMarker(destination string) (migrationMarker, bool, error) {
	path := migrationMarkerPath(destination)
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return migrationMarker{}, false, nil
	}
	if err != nil {
		return migrationMarker{}, false, fmt.Errorf("read migration marker %q: %w", path, err)
	}

	var marker migrationMarker
	if err := json.Unmarshal(contents, &marker); err != nil {
		return migrationMarker{}, false, fmt.Errorf("read migration marker %q: %w", path, err)
	}
	return marker, true, nil
}

func readMigrationOwnership(destination string) (migrationOwnership, bool, error) {
	path := migrationOwnershipPath(destination)
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return migrationOwnership{}, false, nil
	}
	if err != nil {
		return migrationOwnership{}, false, fmt.Errorf("read migration ownership %q: %w", path, err)
	}

	var ownership migrationOwnership
	if err := json.Unmarshal(contents, &ownership); err != nil {
		return migrationOwnership{}, false, fmt.Errorf("read migration ownership %q: %w", path, err)
	}
	if ownership.Source == "" || ownership.Destination == "" {
		return migrationOwnership{}, false, fmt.Errorf("migration ownership %q is incomplete", path)
	}
	return ownership, true, nil
}

func validateMigrationMarker(marker migrationMarker, source, destination string) error {
	if marker.Source != source || marker.Destination != destination {
		return fmt.Errorf("migration marker for %q does not match requested source %q and destination %q", migrationMarkerPath(destination), source, destination)
	}
	return nil
}

func writeMigrationMarker(destination string, marker migrationMarker) error {
	contents, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode migration marker: %w", err)
	}
	return writeMigrationFile(migrationMarkerPath(destination), contents, false)
}

func replaceMigrationMarker(destination string, marker migrationMarker) error {
	contents, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode migration marker: %w", err)
	}
	return writeMigrationFile(migrationMarkerPath(destination), contents, true)
}

func writeMigrationOwnership(destination string, marker migrationMarker) error {
	contents, err := json.Marshal(migrationOwnership{Source: marker.Source, Destination: marker.Destination})
	if err != nil {
		return fmt.Errorf("encode migration ownership: %w", err)
	}
	return writeMigrationFile(migrationOwnershipPath(destination), contents, false)
}

func writeMigrationCopyingMarker(destination string) error {
	return writeMigrationFile(migrationCopyingMarkerPath(destination), []byte(migrationCopyingMarkerContents), false)
}

func writeMigrationCopiedMarker(destination string) error {
	return writeMigrationFile(migrationCopiedMarkerPath(destination), []byte(migrationCopiedMarkerContents), false)
}

// A marker is the single owner of migration progress. On platforms with
// durable directory syncing, replacement persists an atomic state transition.
// Windows syncs the marker file but not its directory entry, so copied
// migrations retain their source and rebuild from it on retry instead of
// trusting a persisted marker replacement alone.
func writeMigrationFile(path string, contents []byte, replace bool) error {
	if _, err := os.Lstat(path); err == nil {
		if !replace {
			return fmt.Errorf("migration marker %q already exists", path)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect migration marker %q: %w", path, err)
	} else if replace {
		return fmt.Errorf("migration marker %q does not exist", path)
	}

	temporary, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create migration marker %q: %w", path, err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()

	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod migration marker %q: %w", path, err)
	}
	if _, err := temporary.Write(contents); err != nil {
		return fmt.Errorf("write migration marker %q: %w", path, err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync migration marker %q: %w", path, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close migration marker %q: %w", path, err)
	}
	if err := renameMigrationFile(temporaryPath, path, replace); err != nil {
		return fmt.Errorf("replace migration marker %q: %w", path, err)
	}
	if err := syncMigrationFile(path); err != nil {
		return fmt.Errorf("sync migration marker %q: %w", path, err)
	}
	if err := syncMigrationDirectoryHierarchy(filepath.Dir(path)); err != nil {
		return fmt.Errorf("persist migration marker %q: %w", path, err)
	}
	return nil
}

func readLegacyMigrationStage(destination string) (migrationStage, error) {
	copying, err := migrationStageMarkerMatches(migrationCopyingMarkerPath(destination), migrationCopyingMarkerContents)
	if err != nil {
		return migrationStageMoving, err
	}
	copied, err := migrationStageMarkerMatches(migrationCopiedMarkerPath(destination), migrationCopiedMarkerContents)
	if err != nil {
		return migrationStageMoving, err
	}
	if copied {
		return migrationStageCopyCommitted, nil
	}
	if copying {
		return migrationStageCopying, nil
	}
	return migrationStageMoving, nil
}

func legacyCopyCommitRecorded(destination string) (bool, error) {
	return migrationStageMarkerMatches(migrationCopiedMarkerPath(destination), legacyCopiedMarkerContents)
}

func migrationStageMarkerMatches(path, want string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect migration stage marker %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("migration stage marker %q is not a regular file", path)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("read migration stage marker %q: %w", path, err)
	}
	if string(contents) != want {
		return false, fmt.Errorf("migration stage marker %q has unexpected contents", path)
	}
	return true, nil
}

func migrationStageMarkersExist(destination string) (bool, error) {
	for _, path := range migrationStageMarkerPaths(destination) {
		if _, err := os.Lstat(path); err == nil {
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("inspect migration stage marker %q: %w", path, err)
		}
	}
	return false, nil
}

// A missing primary marker with a remaining version 2 stage marker can only
// be accepted after finishMigration has durably renamed the database. This is
// the compatibility recovery for a crash during old marker cleanup; an
// incomplete copy still has mailbox.db and remains blocked.
func recoverOrphanedLegacyStageMarkers(destination string) (bool, error) {
	if _, err := readLegacyMigrationStage(destination); err != nil {
		return false, err
	}
	completed, err := databaseRenameCompleted(destination)
	if err != nil {
		return false, err
	}
	if !completed {
		return false, nil
	}
	if err := removeLegacyMigrationStageMarkers(destination); err != nil {
		return false, err
	}
	return true, nil
}

func removeMigrationMarkers(destination string) error {
	// Version 3 does not create side markers. They only exist for a resumed
	// version 2 migration. On platforms that support directory syncing, each
	// side-marker deletion is persisted before primary-marker deletion. If an
	// older Windows migration nevertheless leaves an orphaned side marker,
	// recoverOrphanedLegacyStageMarkers accepts it only after the final database
	// rename proves that the destination is complete.
	if err := removeLegacyMigrationStageMarkers(destination); err != nil {
		return err
	}
	if err := removeMigrationMarker(migrationMarkerPath(destination)); err != nil {
		return fmt.Errorf("remove primary marker: %w", err)
	}
	return nil
}

func removeMigrationOwnership(destination string) error {
	if err := removeMigrationMarker(migrationOwnershipPath(destination)); err != nil {
		return fmt.Errorf("remove ownership marker: %w", err)
	}
	return nil
}

func removeLegacyMigrationStageMarkers(destination string) error {
	for _, path := range migrationStageMarkerPaths(destination) {
		if err := removeMigrationMarker(path); err != nil {
			return fmt.Errorf("remove stage marker %q: %w", path, err)
		}
	}
	return nil
}

func removeMigrationMarker(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := syncMigrationDirectoryHierarchy(filepath.Dir(path)); err != nil {
		return fmt.Errorf("persist removal: %w", err)
	}
	return nil
}

func ensureMigrationComplete(stateDir string) error {
	marker, markerExists, err := readMigrationMarker(stateDir)
	if err != nil {
		return fmt.Errorf("inspect incomplete migration for state directory %q: %w", stateDir, err)
	}
	if markerExists {
		usable, err := recoveryMarkerAllowsRuntime(marker, stateDir)
		if err != nil {
			return fmt.Errorf("inspect incomplete migration for state directory %q: %w", stateDir, err)
		}
		if !usable {
			return fmt.Errorf("state directory %q has an incomplete migration from %q; rerun waypost --state-dir %q migrate --from %q before using it", stateDir, marker.Source, stateDir, marker.Source)
		}
	}
	stageMarkersExist, err := migrationStageMarkersExist(stateDir)
	if err != nil {
		return fmt.Errorf("inspect incomplete migration for state directory %q: %w", stateDir, err)
	}
	if stageMarkersExist {
		recovered, err := recoverOrphanedLegacyStageMarkers(stateDir)
		if err != nil {
			return fmt.Errorf("recover incomplete migration for state directory %q: %w", stateDir, err)
		}
		if !recovered {
			return fmt.Errorf("state directory %q has incomplete migration stage markers; resolve the migration before using it", stateDir)
		}
	}
	ownership, ownershipExists, err := readMigrationOwnership(stateDir)
	if err != nil {
		return fmt.Errorf("inspect incomplete migration for state directory %q: %w", stateDir, err)
	}
	if ownershipExists {
		completed, err := databaseRenameCompleted(stateDir)
		if err != nil {
			return fmt.Errorf("inspect incomplete migration for state directory %q: %w", stateDir, err)
		}
		if !completed {
			return fmt.Errorf("state directory %q has incomplete migration ownership from %q; rerun waypost --state-dir %q migrate --from %q before using it", stateDir, ownership.Source, stateDir, ownership.Source)
		}
	}

	hasLegacyFiles, err := hasLegacyDatabaseFiles(stateDir)
	if err != nil {
		return err
	}
	if hasLegacyFiles {
		return fmt.Errorf("state directory %q still contains legacy database files; rerun waypost --state-dir %q migrate before using it", stateDir, stateDir)
	}
	return nil
}

func recoveryMarkerAllowsRuntime(marker migrationMarker, stateDir string) (bool, error) {
	if !keepMigrationRecoveryMarker() || marker.Version != migrationMarkerVersion || marker.Stage != migrationStageCopyCommitted {
		return false, nil
	}
	exists, err := stateDirectoryExists(stateDir, "current")
	if err != nil || !exists {
		return false, err
	}
	return databaseRenameCompleted(stateDir)
}

func hasLegacyDatabaseFiles(stateDir string) (bool, error) {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		path := filepath.Join(stateDir, legacyDatabaseFilename+suffix)
		if _, err := os.Lstat(path); err == nil {
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("inspect %q: %w", path, err)
		}
	}
	return false, nil
}

// databaseRenameCompleted identifies the finalization point used by the
// version 2 compatibility path. A copied legacy state still has mailbox.db;
// only finishMigration replaces it with waypost.db before marker cleanup.
func databaseRenameCompleted(stateDir string) (bool, error) {
	path := filepath.Join(stateDir, databaseFilename)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect current database %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("current database %q is not a regular file", path)
	}

	hasLegacyFiles, err := hasLegacyDatabaseFiles(stateDir)
	if err != nil {
		return false, err
	}
	return !hasLegacyFiles, nil
}

func renameLegacyDatabaseFiles(stateDir string) error {
	type rename struct {
		from string
		to   string
	}

	var renames []rename
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		from := filepath.Join(stateDir, legacyDatabaseFilename+suffix)
		_, fromErr := os.Lstat(from)
		fromExists := fromErr == nil
		if fromErr != nil && !errors.Is(fromErr, os.ErrNotExist) {
			return fmt.Errorf("inspect %q: %w", from, fromErr)
		}

		to := filepath.Join(stateDir, databaseFilename+suffix)
		_, toErr := os.Lstat(to)
		toExists := toErr == nil
		if toErr != nil && !errors.Is(toErr, os.ErrNotExist) {
			return fmt.Errorf("inspect %q: %w", to, toErr)
		}
		if fromExists && toExists {
			return fmt.Errorf("legacy and current database files both exist for suffix %q", suffix)
		}
		if fromExists {
			renames = append(renames, rename{from: from, to: to})
		}
	}

	for index, rename := range renames {
		if err := os.Rename(rename.from, rename.to); err != nil {
			for rollbackIndex := index - 1; rollbackIndex >= 0; rollbackIndex-- {
				_ = os.Rename(renames[rollbackIndex].to, renames[rollbackIndex].from)
			}
			return fmt.Errorf("rename %q to %q: %w", rename.from, rename.to, err)
		}
	}
	if len(renames) > 0 {
		if err := syncMigrationDirectory(stateDir); err != nil {
			return fmt.Errorf("sync renamed database directory %q: %w", stateDir, err)
		}
	}
	return nil
}

func copyStateDirectory(source, destination string, marker migrationMarker) error {
	info, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("inspect source directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("source path is not a directory")
	}
	if err := os.Mkdir(destination, info.Mode().Perm()); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}
	if err := os.Chmod(destination, info.Mode().Perm()); err != nil {
		return fmt.Errorf("chmod destination directory %q: %w", destination, err)
	}
	if err := writeMigrationOwnership(destination, marker); err != nil {
		return err
	}

	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relativePath, err := filepath.Rel(source, path)
		if err != nil {
			return fmt.Errorf("resolve copied path %q: %w", path, err)
		}
		if relativePath == "." {
			return nil
		}
		if relativePath == migrationOwnershipFilename {
			return fmt.Errorf("source state contains reserved migration ownership file %q", path)
		}
		target := filepath.Join(destination, relativePath)

		if entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return fmt.Errorf("inspect source directory %q: %w", path, err)
			}
			if err := os.Mkdir(target, info.Mode().Perm()); err != nil {
				return fmt.Errorf("create destination directory %q: %w", target, err)
			}
			if err := os.Chmod(target, info.Mode().Perm()); err != nil {
				return fmt.Errorf("chmod destination directory %q: %w", target, err)
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported source entry %q", path)
		}
		return copyStateFile(path, target, entry)
	})
}

func copyStateFile(source, destination string, entry fs.DirEntry) (err error) {
	info, err := entry.Info()
	if err != nil {
		return fmt.Errorf("inspect source file %q: %w", source, err)
	}
	sourceFile, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open source file %q: %w", source, err)
	}
	defer func() {
		if closeErr := sourceFile.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close source file %q: %w", source, closeErr)
		}
	}()

	destinationFile, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
	if err != nil {
		return fmt.Errorf("create destination file %q: %w", destination, err)
	}
	defer func() {
		if closeErr := destinationFile.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close destination file %q: %w", destination, closeErr)
		}
	}()

	if _, err := io.Copy(destinationFile, sourceFile); err != nil {
		return fmt.Errorf("copy source file %q: %w", source, err)
	}
	if err := destinationFile.Chmod(info.Mode().Perm()); err != nil {
		return fmt.Errorf("chmod destination file %q: %w", destination, err)
	}
	if err := destinationFile.Sync(); err != nil {
		return fmt.Errorf("sync destination file %q: %w", destination, err)
	}
	return nil
}

func syncCopiedStateDirectory(destination string) error {
	var directories []string
	if err := filepath.WalkDir(destination, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			directories = append(directories, path)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("walk copied state directory %q: %w", destination, err)
	}

	for index := len(directories) - 1; index >= 0; index-- {
		if err := syncMigrationDirectory(directories[index]); err != nil {
			return fmt.Errorf("sync copied state directory %q: %w", directories[index], err)
		}
	}
	if err := syncMigrationDirectoryHierarchy(filepath.Dir(destination)); err != nil {
		return fmt.Errorf("sync copied state parent directory %q: %w", filepath.Dir(destination), err)
	}
	return nil
}

func syncMigrationFile(path string) (err error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open file %q: %w", path, err)
	}
	defer func() {
		if closeErr := file.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close file %q: %w", path, closeErr)
		}
	}()

	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync file %q: %w", path, err)
	}
	return nil
}

func syncMigrationDirectoryHierarchy(path string) error {
	for path = filepath.Clean(path); ; path = filepath.Dir(path) {
		if err := syncMigrationDirectory(path); err != nil {
			return err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return nil
		}
	}
}
