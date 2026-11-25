package waypost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func assertFinalMigrationMarker(t *testing.T, destination string, retained bool) {
	t.Helper()
	marker, exists, err := readMigrationMarker(destination)
	if err != nil {
		t.Fatalf("readMigrationMarker() error = %v", err)
	}
	if retained {
		if !exists {
			t.Fatal("migration marker = missing, want retained recovery marker")
		}
		if marker.Version != migrationMarkerVersion || marker.Stage != migrationStageCopyCommitted {
			t.Fatalf("retained migration marker = %+v, want version=%d stage=%q", marker, migrationMarkerVersion, migrationStageCopyCommitted)
		}
		return
	}
	if exists {
		t.Fatalf("migration marker = %+v, want removed", marker)
	}
}

func crossDeviceRenameError(oldPath, newPath string) error {
	return &os.LinkError{Op: "rename", Old: oldPath, New: newPath, Err: syscall.EXDEV}
}

func TestMigrateLegacyStateMovesDirectoryAndDatabaseFiles(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")

	if err := os.MkdirAll(filepath.Join(source, blobsDirName), 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	files := map[string]string{
		legacyDatabaseFilename:              "database",
		legacyDatabaseFilename + "-wal":     "wal",
		legacyDatabaseFilename + "-shm":     "shm",
		legacyDatabaseFilename + "-journal": "journal",
		filepath.Join(blobsDirName, "blob"): "body",
	}
	for path, contents := range files {
		if err := os.WriteFile(filepath.Join(source, path), []byte(contents), 0o600); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", path, err)
		}
	}

	result, err := MigrateLegacyState(destination, source)
	if err != nil {
		t.Fatalf("MigrateLegacyState() error = %v", err)
	}
	if result.Source != source || result.Destination != destination {
		t.Fatalf("MigrateLegacyState() result = %+v, want source=%q destination=%q", result, source, destination)
	}
	if _, err := os.Lstat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy source after migration = %v, want not exist", err)
	}

	for suffix, want := range map[string]string{
		"":         "database",
		"-wal":     "wal",
		"-shm":     "shm",
		"-journal": "journal",
	} {
		got, err := os.ReadFile(filepath.Join(destination, databaseFilename+suffix))
		if err != nil {
			t.Fatalf("ReadFile(current database%q) error = %v", suffix, err)
		}
		if string(got) != want {
			t.Fatalf("current database%q = %q, want %q", suffix, got, want)
		}
	}
	if _, err := os.Lstat(filepath.Join(destination, legacyDatabaseFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy database after migration = %v, want not exist", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, blobsDirName, "blob")); err != nil || string(got) != "body" {
		t.Fatalf("migrated blob = %q, %v; want body, nil", got, err)
	}
}

func TestMigrateLegacyStateRejectsExistingDestination(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}

	_, err := MigrateLegacyState(destination, source)
	if err == nil {
		t.Fatal("MigrateLegacyState() error = nil, want existing destination error")
	}
	if _, err := os.Lstat(source); err != nil {
		t.Fatalf("legacy source after rejected migration = %v, want preserved", err)
	}
}

func TestMigrateLegacyStateRejectsNestedDestinationWithoutChangingSource(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(source, "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}

	_, err := MigrateLegacyState(destination, source)
	if err == nil {
		t.Fatal("MigrateLegacyState() error = nil, want overlapping paths error")
	}
	if !strings.Contains(err.Error(), "must not overlap") {
		t.Fatalf("MigrateLegacyState() error = %q, want overlapping paths error", err)
	}
	if got, err := os.ReadFile(filepath.Join(source, legacyDatabaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(legacy database) = %q, %v; want database, nil", got, err)
	}
	if _, err := os.Lstat(filepath.Join(source, databaseFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("current database after rejected migration = %v, want not exist", err)
	}
	if _, err := os.Lstat(migrationMarkerPath(destination)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("migration marker after rejected migration = %v, want not exist", err)
	}
}

func TestMigrateLegacyStatePreservesSourceWhenMoveFails(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}
	moveErr := errors.New("move denied")

	_, err := migrateLegacyState(destination, source, func(string, string) error {
		return moveErr
	})
	if !errors.Is(err, moveErr) {
		t.Fatalf("migrateLegacyState() error = %v, want move error", err)
	}
	if got, err := os.ReadFile(filepath.Join(source, legacyDatabaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(legacy database) = %q, %v; want database, nil", got, err)
	}
	if _, err := os.Lstat(filepath.Join(source, databaseFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("current database in source after failed move = %v, want not exist", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination after failed move = %v, want not exist", err)
	}
	if _, err := os.Lstat(migrationMarkerPath(destination)); err != nil {
		t.Fatalf("migration marker after failed move = %v, want preserved", err)
	}

	if _, err := MigrateLegacyState(destination, source); err != nil {
		t.Fatalf("MigrateLegacyState() resume error = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(current database) = %q, %v; want database, nil", got, err)
	}
}

func TestMigrateLegacyStateResumesAfterLegacyDirectoryWasMoved(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, legacyDatabaseFilename+"-wal"), []byte("wal"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database wal) error = %v", err)
	}
	if err := writeMigrationMarker(destination, newMigrationMarker(source, destination)); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	result, err := MigrateLegacyState(destination, source)
	if err != nil {
		t.Fatalf("MigrateLegacyState() error = %v", err)
	}
	if result.Source != source || result.Destination != destination {
		t.Fatalf("MigrateLegacyState() result = %+v, want source=%q destination=%q", result, source, destination)
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(current database) = %q, %v; want database, nil", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename+"-wal")); err != nil || string(got) != "wal" {
		t.Fatalf("ReadFile(current database wal) = %q, %v; want wal, nil", got, err)
	}
	if _, err := os.Lstat(filepath.Join(destination, legacyDatabaseFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy database after resumed migration = %v, want not exist", err)
	}
}

func TestOpenRuntimeRejectsIncompleteLegacyMigration(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(stateDir) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}

	_, err := OpenRuntime(context.Background(), stateDir)
	if err == nil {
		t.Fatal("OpenRuntime() error = nil, want incomplete migration error")
	}
	if !strings.Contains(err.Error(), "legacy database files") {
		t.Fatalf("OpenRuntime() error = %q, want legacy migration guidance", err)
	}
	if _, err := os.Lstat(filepath.Join(stateDir, databaseFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("current database after rejected OpenRuntime = %v, want not exist", err)
	}
}

func TestOpenRuntimeRequiresMigrationBeforeCreatingDefaultState(t *testing.T) {
	stateHome := t.TempDir()
	t.Setenv("XDG_STATE_HOME", stateHome)
	t.Setenv("WAYPOST_STATE_DIR", "")
	legacyStateDir := filepath.Join(stateHome, legacyStateDirSuffix)
	currentStateDir := filepath.Join(stateHome, defaultStateDirSuffix)
	if err := os.MkdirAll(legacyStateDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(legacyStateDir) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyStateDir, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}

	_, err := OpenRuntime(context.Background(), "")
	if err == nil {
		t.Fatal("OpenRuntime() error = nil, want migration-required error")
	}
	if !strings.Contains(err.Error(), "waypost migrate") {
		t.Fatalf("OpenRuntime() error = %q, want migration guidance", err)
	}
	if _, err := os.Lstat(currentStateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("current state after rejected OpenRuntime = %v, want not exist", err)
	}
}

func TestOpenRuntimeRejectsMigrationBeforeCreatingState(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	stateDir := filepath.Join(root, "waypost-state")
	marker := newMigrationMarker(source, stateDir)
	if err := writeMigrationMarker(stateDir, marker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	_, err := OpenRuntime(context.Background(), stateDir)
	if err == nil {
		t.Fatal("OpenRuntime() error = nil, want incomplete migration error")
	}
	if !strings.Contains(err.Error(), "incomplete migration") {
		t.Fatalf("OpenRuntime() error = %q, want incomplete migration guidance", err)
	}
	if _, err := os.Lstat(stateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state directory after rejected OpenRuntime = %v, want not exist", err)
	}
}

func TestMigrateLegacyStateResumesPreparedMigrationAfterMove(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, databaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(current database) error = %v", err)
	}
	if err := writeMigrationMarker(destination, newMigrationMarker(source, destination)); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	_, err := MigrateLegacyState(destination, source)
	if err != nil {
		t.Fatalf("MigrateLegacyState() error = %v", err)
	}
	if _, err := os.Lstat(migrationMarkerPath(destination)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("migration marker after resumed moved migration = %v, want not exist", err)
	}
}

func TestMigrateLegacyStateCopiesAcrossFilesystems(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(filepath.Join(source, blobsDirName), 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, blobsDirName, "blob"), []byte("body"), 0o600); err != nil {
		t.Fatalf("WriteFile(blob) error = %v", err)
	}

	result, err := migrateLegacyState(destination, source, func(oldPath, newPath string) error {
		return &os.LinkError{Op: "rename", Old: oldPath, New: newPath, Err: syscall.EXDEV}
	})
	if err != nil {
		t.Fatalf("migrateLegacyState() error = %v", err)
	}
	if result.Source != source || result.Destination != destination {
		t.Fatalf("migrateLegacyState() result = %+v, want source=%q destination=%q", result, source, destination)
	}
	if result.SourceRetained != retainLegacySourceAfterCopy() {
		t.Fatalf("migrateLegacyState() source retained = %t, want %t", result.SourceRetained, retainLegacySourceAfterCopy())
	}
	if retainLegacySourceAfterCopy() {
		if _, err := os.Lstat(source); err != nil {
			t.Fatalf("legacy source after cross-filesystem migration = %v, want retained recovery copy", err)
		}
	} else if _, err := os.Lstat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy source after cross-filesystem migration = %v, want not exist", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(current database) = %q, %v; want database, nil", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, blobsDirName, "blob")); err != nil || string(got) != "body" {
		t.Fatalf("ReadFile(blob) = %q, %v; want body, nil", got, err)
	}
	assertFinalMigrationMarker(t, destination, result.SourceRetained && keepMigrationRecoveryMarker())
	if _, err := os.Lstat(migrationCopiedMarkerPath(destination)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("migration copy marker after cross-filesystem migration = %v, want not exist", err)
	}
	if _, err := os.Lstat(migrationCopyingMarkerPath(destination)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("migration copying marker after cross-filesystem migration = %v, want not exist", err)
	}
}

func TestMigrateLegacyStateResumesCopiedStateCleanup(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, databaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(current database) error = %v", err)
	}
	marker := newMigrationMarker(source, destination)
	marker.Stage = migrationStageCopyCommitted
	if err := writeMigrationMarker(destination, marker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	result, err := MigrateLegacyState(destination, source)
	if err != nil {
		t.Fatalf("MigrateLegacyState() error = %v", err)
	}
	if result.SourceRetained != retainLegacySourceAfterCopy() {
		t.Fatalf("MigrateLegacyState() source retained = %t, want %t", result.SourceRetained, retainLegacySourceAfterCopy())
	}
	if retainLegacySourceAfterCopy() {
		if _, err := os.Lstat(source); err != nil {
			t.Fatalf("legacy source after resumed copied migration = %v, want retained recovery copy", err)
		}
	} else if _, err := os.Lstat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy source after resumed copied migration = %v, want not exist", err)
	}
	assertFinalMigrationMarker(t, destination, result.SourceRetained && keepMigrationRecoveryMarker())
	if _, err := os.Lstat(migrationCopiedMarkerPath(destination)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("migration copy marker after resumed copied migration = %v, want not exist", err)
	}
}

func TestMigrateLegacyStateResumesCurrentCleanupAfterSourceRemoval(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, databaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(current database) error = %v", err)
	}
	marker := newMigrationMarker(source, destination)
	marker.Stage = migrationStageCopyCommitted
	if err := writeMigrationMarker(destination, marker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	result, err := MigrateLegacyState(destination, source)
	if err != nil {
		t.Fatalf("MigrateLegacyState() error = %v", err)
	}
	if result.SourceRetained {
		t.Fatalf("MigrateLegacyState() source retained = true, want false when source is already absent")
	}
	if _, err := os.Lstat(migrationMarkerPath(destination)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("migration marker after resumed cleanup = %v, want not exist", err)
	}
}

func TestMigrateLegacyStateRestartsIncompleteCrossFilesystemCopy(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "partial"), []byte("partial"), 0o600); err != nil {
		t.Fatalf("WriteFile(partial destination) error = %v", err)
	}
	marker := newMigrationMarker(source, destination)
	marker.Stage = migrationStageCopying
	if err := writeMigrationMarker(destination, marker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	result, err := MigrateLegacyState(destination, source)
	if err != nil {
		t.Fatalf("MigrateLegacyState() error = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(current database) = %q, %v; want database, nil", got, err)
	}
	if _, err := os.Lstat(filepath.Join(destination, "partial")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial destination after resumed migration = %v, want not exist", err)
	}
	assertFinalMigrationMarker(t, destination, result.SourceRetained && keepMigrationRecoveryMarker())
}

func TestMigrateLegacyStateRejectsUncommittedCopyWithoutSource(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}
	marker := newMigrationMarker(source, destination)
	marker.Stage = migrationStageCopying
	if err := writeMigrationMarker(destination, marker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	_, err := MigrateLegacyState(destination, source)
	if err == nil {
		t.Fatal("MigrateLegacyState() error = nil, want uncommitted copy error")
	}
	if !strings.Contains(err.Error(), "no durable copy commit") {
		t.Fatalf("MigrateLegacyState() error = %q, want durable copy commit guidance", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, legacyDatabaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(legacy database) = %q, %v; want database, nil", got, err)
	}
	if _, err := os.Lstat(filepath.Join(destination, databaseFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("current database after rejected resume = %v, want not exist", err)
	}
	if _, err := os.Lstat(migrationMarkerPath(destination)); err != nil {
		t.Fatalf("migration marker after rejected resume = %v, want preserved", err)
	}
}

func TestMigrateLegacyStateFinishesCommittedCopyAfterSourceRemoval(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}
	marker := newMigrationMarker(source, destination)
	marker.Stage = migrationStageCopyCommitted
	if err := writeMigrationMarker(destination, marker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	result, err := MigrateLegacyState(destination, source)
	if err != nil {
		t.Fatalf("MigrateLegacyState() error = %v", err)
	}
	if result.SourceRetained {
		t.Fatalf("MigrateLegacyState() source retained = true, want false when source is already absent")
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(current database) = %q, %v; want database, nil", got, err)
	}
	for _, path := range []string{
		migrationMarkerPath(destination),
		migrationCopyingMarkerPath(destination),
		migrationCopiedMarkerPath(destination),
	} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("migration marker %q after resumed commit = %v, want not exist", path, err)
		}
	}
}

func TestMigrateLegacyStateResumesVersionZeroCommittedCopyAfterSourceRemoval(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}
	marker := migrationMarker{Source: source, Destination: destination}
	if err := writeMigrationMarker(destination, marker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}
	if err := writeMigrationFile(migrationCopiedMarkerPath(destination), []byte(legacyCopiedMarkerContents), false); err != nil {
		t.Fatalf("write legacy copy marker error = %v", err)
	}

	if _, err := MigrateLegacyState(destination, source); err != nil {
		t.Fatalf("MigrateLegacyState() error = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(current database) = %q, %v; want database, nil", got, err)
	}
	for _, path := range []string{
		migrationMarkerPath(destination),
		migrationCopiedMarkerPath(destination),
	} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("legacy migration marker %q after resume = %v, want not exist", path, err)
		}
	}
}

func TestMigrateLegacyStateRejectsVersionZeroMoveWithoutCommitAfterSourceRemoval(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}
	marker := migrationMarker{Source: source, Destination: destination}
	if err := writeMigrationMarker(destination, marker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	_, err := MigrateLegacyState(destination, source)
	if err == nil {
		t.Fatal("MigrateLegacyState() error = nil, want missing copy commit error")
	}
	if !strings.Contains(err.Error(), "lacks a durable copy commit") {
		t.Fatalf("MigrateLegacyState() error = %q, want missing copy commit guidance", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, legacyDatabaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(legacy database) = %q, %v; want database, nil", got, err)
	}
	if _, err := os.Lstat(migrationMarkerPath(destination)); err != nil {
		t.Fatalf("legacy migration marker after rejected resume = %v, want preserved", err)
	}
}

func TestMigrateLegacyStateRestartsVersionZeroMigrationFromSource(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("source database"), 0o600); err != nil {
		t.Fatalf("WriteFile(source database) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "partial"), []byte("partial"), 0o600); err != nil {
		t.Fatalf("WriteFile(partial destination) error = %v", err)
	}
	if err := writeMigrationMarker(destination, migrationMarker{Source: source, Destination: destination}); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	result, err := migrateLegacyState(destination, source, crossDeviceRenameError)
	if err != nil {
		t.Fatalf("migrateLegacyState() error = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "source database" {
		t.Fatalf("ReadFile(current database) = %q, %v; want source database, nil", got, err)
	}
	if _, err := os.Lstat(filepath.Join(destination, "partial")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial destination after source restart = %v, want not exist", err)
	}
	assertFinalMigrationMarker(t, destination, result.SourceRetained && keepMigrationRecoveryMarker())
}

func TestMigrateLegacyStateRestartsMovingMigrationFromSource(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("source database"), 0o600); err != nil {
		t.Fatalf("WriteFile(source database) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "partial"), []byte("partial"), 0o600); err != nil {
		t.Fatalf("WriteFile(partial destination) error = %v", err)
	}
	if err := writeMigrationMarker(destination, newMigrationMarker(source, destination)); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	result, err := migrateLegacyState(destination, source, crossDeviceRenameError)
	if err != nil {
		t.Fatalf("migrateLegacyState() error = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "source database" {
		t.Fatalf("ReadFile(current database) = %q, %v; want source database, nil", got, err)
	}
	if _, err := os.Lstat(filepath.Join(destination, "partial")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial destination after source restart = %v, want not exist", err)
	}
	assertFinalMigrationMarker(t, destination, result.SourceRetained && keepMigrationRecoveryMarker())
}

func TestMigrateLegacyStateRestartsOwnedDestinationWithoutPrimaryMarker(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("source database"), 0o600); err != nil {
		t.Fatalf("WriteFile(source database) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "partial"), []byte("partial"), 0o600); err != nil {
		t.Fatalf("WriteFile(partial destination) error = %v", err)
	}
	if err := writeMigrationOwnership(destination, newMigrationMarker(source, destination)); err != nil {
		t.Fatalf("writeMigrationOwnership() error = %v", err)
	}

	result, err := migrateLegacyState(destination, source, crossDeviceRenameError)
	if err != nil {
		t.Fatalf("migrateLegacyState() error = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "source database" {
		t.Fatalf("ReadFile(current database) = %q, %v; want source database, nil", got, err)
	}
	if _, err := os.Lstat(filepath.Join(destination, "partial")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial destination after owned recovery = %v, want not exist", err)
	}
	assertFinalMigrationMarker(t, destination, result.SourceRetained && keepMigrationRecoveryMarker())
}

func TestMigrateLegacyStateKeepsFinalizedRetainedCopy(t *testing.T) {
	if !keepMigrationRecoveryMarker() {
		t.Skip("only Windows retains a recovery marker")
	}

	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("legacy database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, databaseFilename), []byte("new waypost data"), 0o600); err != nil {
		t.Fatalf("WriteFile(current database) error = %v", err)
	}
	marker := newMigrationMarker(source, destination)
	marker.Stage = migrationStageCopyCommitted
	if err := writeMigrationMarker(destination, marker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}

	result, err := MigrateLegacyState(destination, source)
	if err != nil {
		t.Fatalf("MigrateLegacyState() error = %v", err)
	}
	if !result.SourceRetained {
		t.Fatal("MigrateLegacyState() source retained = false, want retained Windows recovery source")
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "new waypost data" {
		t.Fatalf("ReadFile(current database) = %q, %v; want preserved Waypost data, nil", got, err)
	}
	assertFinalMigrationMarker(t, destination, true)
}

func TestMigrateLegacyStateKeepsFinalizedOwnedDestinationWithoutPrimaryMarker(t *testing.T) {
	if !keepMigrationRecoveryMarker() {
		t.Skip("only Windows retains a recovery marker")
	}

	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("legacy database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, databaseFilename), []byte("new waypost data"), 0o600); err != nil {
		t.Fatalf("WriteFile(current database) error = %v", err)
	}
	if err := writeMigrationOwnership(destination, newMigrationMarker(source, destination)); err != nil {
		t.Fatalf("writeMigrationOwnership() error = %v", err)
	}

	result, err := MigrateLegacyState(destination, source)
	if err != nil {
		t.Fatalf("MigrateLegacyState() error = %v", err)
	}
	if !result.SourceRetained {
		t.Fatal("MigrateLegacyState() source retained = false, want retained Windows recovery source")
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "new waypost data" {
		t.Fatalf("ReadFile(current database) = %q, %v; want preserved Waypost data, nil", got, err)
	}
	assertFinalMigrationMarker(t, destination, true)
}

func TestMigrateLegacyStateRecoversVersion2CleanupInterruptions(t *testing.T) {
	tests := []struct {
		name          string
		copying       bool
		copied        bool
		sourcePresent bool
	}{
		{
			name:    "before stage cleanup",
			copying: true,
			copied:  true,
		},
		{
			name:    "after copy commit marker cleanup",
			copying: true,
		},
		{
			name:          "after copy commit marker cleanup with retained source",
			copying:       true,
			sourcePresent: true,
		},
		{
			name: "after stage marker cleanup",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "legacy-state")
			destination := filepath.Join(root, "waypost-state")
			if test.sourcePresent {
				if err := os.MkdirAll(source, 0o700); err != nil {
					t.Fatalf("MkdirAll(source) error = %v", err)
				}
			}
			if err := os.MkdirAll(destination, 0o700); err != nil {
				t.Fatalf("MkdirAll(destination) error = %v", err)
			}
			if err := os.WriteFile(filepath.Join(destination, databaseFilename), []byte("database"), 0o600); err != nil {
				t.Fatalf("WriteFile(current database) error = %v", err)
			}
			marker := migrationMarker{
				Version:     legacyStagedMarkerVersion,
				Source:      source,
				Destination: destination,
			}
			if err := writeMigrationMarker(destination, marker); err != nil {
				t.Fatalf("writeMigrationMarker() error = %v", err)
			}
			if test.copying {
				if err := writeMigrationCopyingMarker(destination); err != nil {
					t.Fatalf("writeMigrationCopyingMarker() error = %v", err)
				}
			}
			if test.copied {
				if err := writeMigrationCopiedMarker(destination); err != nil {
					t.Fatalf("writeMigrationCopiedMarker() error = %v", err)
				}
			}

			result, err := MigrateLegacyState(destination, source)
			if err != nil {
				t.Fatalf("MigrateLegacyState() error = %v", err)
			}
			wantSourceRetained := test.sourcePresent && retainLegacySourceAfterCopy()
			if result.SourceRetained != wantSourceRetained {
				t.Fatalf("MigrateLegacyState() source retained = %t, want %t", result.SourceRetained, wantSourceRetained)
			}
			assertFinalMigrationMarker(t, destination, result.SourceRetained && keepMigrationRecoveryMarker())
			for _, path := range []string{
				migrationCopyingMarkerPath(destination),
				migrationCopiedMarkerPath(destination),
			} {
				if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("migration marker %q after resumed cleanup = %v, want not exist", path, err)
				}
			}
		})
	}
}

func TestMigrateLegacyStateRefusesUnmarkedDestination(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, legacyDatabaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}

	_, err := MigrateLegacyState(destination, source)
	if err == nil {
		t.Fatal("MigrateLegacyState() error = nil, want existing destination error")
	}
	if got, err := os.ReadFile(filepath.Join(destination, legacyDatabaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(legacy database) = %q, %v; want database, nil", got, err)
	}
	if _, err := os.Lstat(filepath.Join(destination, databaseFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("current database after rejected unmarked destination = %v, want not exist", err)
	}
}

func TestEnsureMigrationCompleteRecoversOrphanedVersion2CleanupMarker(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(stateDir) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, databaseFilename), []byte("database"), 0o600); err != nil {
		t.Fatalf("WriteFile(current database) error = %v", err)
	}
	if err := writeMigrationCopyingMarker(stateDir); err != nil {
		t.Fatalf("writeMigrationCopyingMarker() error = %v", err)
	}

	if err := ensureMigrationComplete(stateDir); err != nil {
		t.Fatalf("ensureMigrationComplete() error = %v", err)
	}
	if _, err := os.Lstat(migrationCopyingMarkerPath(stateDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphaned stage marker after recovery = %v, want not exist", err)
	}
}

func TestOpenRuntimeRejectsOrphanedMigrationStageMarkers(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	if err := writeMigrationCopyingMarker(stateDir); err != nil {
		t.Fatalf("writeMigrationCopyingMarker() error = %v", err)
	}

	_, err := OpenRuntime(context.Background(), stateDir)
	if err == nil {
		t.Fatal("OpenRuntime() error = nil, want incomplete migration error")
	}
	if !strings.Contains(err.Error(), "stage markers") {
		t.Fatalf("OpenRuntime() error = %q, want stage-marker guidance", err)
	}
	if _, err := os.Lstat(stateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state directory after rejected OpenRuntime = %v, want not exist", err)
	}
}

func TestMigrateLegacyStateCopiesReadOnlyDatabaseAcrossFilesystems(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	t.Cleanup(func() {
		_ = os.Chmod(filepath.Join(source, legacyDatabaseFilename), 0o600)
		_ = os.Chmod(filepath.Join(destination, databaseFilename), 0o600)
	})
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("database"), 0o400); err != nil {
		t.Fatalf("WriteFile(legacy database) error = %v", err)
	}

	_, err := migrateLegacyState(destination, source, func(oldPath, newPath string) error {
		return &os.LinkError{Op: "rename", Old: oldPath, New: newPath, Err: syscall.EXDEV}
	})
	if err != nil {
		t.Fatalf("migrateLegacyState() error = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, databaseFilename)); err != nil || string(got) != "database" {
		t.Fatalf("ReadFile(current database) = %q, %v; want database, nil", got, err)
	}
}

func TestMigrateLegacyStateAllowsOpeningRetainedCopyWithoutDatabase(t *testing.T) {
	if !keepMigrationRecoveryMarker() {
		t.Skip("only platforms retaining a recovery marker hit no-database finalization")
	}

	root := t.TempDir()
	source := filepath.Join(root, "legacy-state")
	destination := filepath.Join(root, "waypost-state")
	if err := os.MkdirAll(filepath.Join(source, blobsDirName), 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, blobsDirName, "blob"), []byte("body"), 0o600); err != nil {
		t.Fatalf("WriteFile(blob) error = %v", err)
	}

	result, err := migrateLegacyState(destination, source, crossDeviceRenameError)
	if err != nil {
		t.Fatalf("migrateLegacyState() error = %v", err)
	}
	if !result.SourceRetained {
		t.Fatal("SourceRetained = false, want true")
	}
	marker, exists, err := readMigrationMarker(destination)
	if err != nil {
		t.Fatalf("readMigrationMarker() error = %v", err)
	}
	if !exists || marker.Stage != migrationStageCopyCommitted || !marker.NoDatabase {
		t.Fatalf("marker = %+v (exists=%t), want stage=%q no_database=true", marker, exists, migrationStageCopyCommitted)
	}

	if err := ensureMigrationComplete(destination); err != nil {
		t.Fatalf("ensureMigrationComplete() error = %v, want nil", err)
	}
	runtime, err := OpenRuntime(context.Background(), destination)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	_ = runtime.Close()
}

func TestEnsureMigrationCompleteAllowsSymlinkedStateDir(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	realDir := filepath.Join(base, "real-state")
	if err := os.MkdirAll(realDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(real) error = %v", err)
	}
	linkDir := filepath.Join(base, "link-state")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Skipf("os.Symlink unavailable on this environment: %v", err)
	}

	exists, err := runtimeStateDirectoryExists(linkDir, "current")
	if err != nil || !exists {
		t.Fatalf("runtimeStateDirectoryExists(symlink) = (%t, %v), want (true, nil)", exists, err)
	}
	if keepMigrationRecoveryMarker() {
		marker := migrationMarker{
			Version:     migrationMarkerVersion,
			Source:      filepath.Join(base, "legacy"),
			Destination: linkDir,
			Stage:       migrationStageCopyCommitted,
			NoDatabase:  true,
		}
		if err := writeMigrationMarker(linkDir, marker); err != nil {
			t.Fatalf("writeMigrationMarker() error = %v", err)
		}
	}
	if err := ensureMigrationComplete(linkDir); err != nil {
		t.Fatalf("ensureMigrationComplete(symlink) error = %v, want nil", err)
	}
	runtime, err := OpenRuntime(context.Background(), linkDir)
	if err != nil {
		t.Fatalf("OpenRuntime(symlink) error = %v", err)
	}
	_ = runtime.Close()
}

func TestEnsureDefaultLegacyStateMigratedAllowsSymlinkedStateDir(t *testing.T) {
	t.Setenv("WAYPOST_STATE_DIR", "")

	base := t.TempDir()
	realDir := filepath.Join(base, "real-state")
	if err := os.MkdirAll(realDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(real) error = %v", err)
	}
	linkDir := filepath.Join(base, "link-state")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Skipf("os.Symlink unavailable on this environment: %v", err)
	}
	if err := ensureDefaultLegacyStateMigrated("", linkDir); err != nil {
		t.Fatalf("ensureDefaultLegacyStateMigrated(symlink) error = %v, want nil", err)
	}
}

func TestMigrateLegacyStateIgnoresCorruptOwnershipWhenPrimaryMarkerExists(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	source := filepath.Join(base, "legacy")
	destination := filepath.Join(base, "current")
	if err := os.MkdirAll(filepath.Join(source, "blobs"), 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "blobs", "sample.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatalf("WriteFile(source blob) error = %v", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}
	marker := migrationMarker{
		Version:     migrationMarkerVersion,
		Source:      source,
		Destination: destination,
		Stage:       migrationStageCopying,
	}
	if err := writeMigrationMarker(destination, marker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}
	if err := os.WriteFile(migrationOwnershipPath(destination), []byte("{corrupt"), 0o600); err != nil {
		t.Fatalf("WriteFile(corrupt ownership) error = %v", err)
	}

	result, err := migrateLegacyState(destination, source, crossDeviceRenameError)
	if err != nil {
		t.Fatalf("migrateLegacyState() with corrupt ownership error = %v", err)
	}
	if result.Destination != destination {
		t.Fatalf("Destination = %q, want %q", result.Destination, destination)
	}
	got, err := os.ReadFile(filepath.Join(destination, "blobs", "sample.txt"))
	if err != nil || string(got) != "hello" {
		t.Fatalf("copied blob = (%q, %v), want %q", string(got), err, "hello")
	}
}

func TestRestartCopiedMigrationResetsNoDatabaseAndOwnershipDoesNotSeedIt(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	source := filepath.Join(base, "legacy")
	destination := filepath.Join(base, "current")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatalf("MkdirAll(source) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, legacyDatabaseFilename), []byte("placeholder"), 0o600); err != nil {
		t.Fatalf("WriteFile(legacy db) error = %v", err)
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatalf("MkdirAll(destination) error = %v", err)
	}

	seededMarker := migrationMarker{
		Version:     migrationMarkerVersion,
		Source:      source,
		Destination: destination,
		Stage:       migrationStageCopying,
		NoDatabase:  true,
	}
	if err := writeMigrationOwnership(destination, seededMarker); err != nil {
		t.Fatalf("writeMigrationOwnership() error = %v", err)
	}
	ownership, exists, err := readMigrationOwnership(destination)
	if err != nil || !exists || ownership.NoDatabase {
		t.Fatalf("ownership = (%+v, exists=%t, err=%v), want NoDatabase=false", ownership, exists, err)
	}

	usable, err := recoveryMarkerAllowsRuntime(seededMarker, destination)
	if err != nil || usable {
		t.Fatalf("recoveryMarkerAllowsRuntime(copying+NoDatabase) = (%t, %v), want (false, nil)", usable, err)
	}

	seededMarker.Stage = migrationStageCopyCommitted
	if err := writeMigrationMarker(destination, seededMarker); err != nil {
		t.Fatalf("writeMigrationMarker() error = %v", err)
	}
	if _, err := restartCopiedMigrationFromSource(seededMarker, source, destination, true); err != nil {
		t.Fatalf("restartCopiedMigrationFromSource() error = %v", err)
	}
	afterMarker, afterExists, err := readMigrationMarker(destination)
	if err != nil {
		t.Fatalf("readMigrationMarker(after) error = %v", err)
	}
	if afterExists && afterMarker.NoDatabase {
		t.Fatalf("after marker = %+v, want NoDatabase=false after restart from source with database", afterMarker)
	}
}
