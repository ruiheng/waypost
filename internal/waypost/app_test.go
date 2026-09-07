package waypost

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	goruntime "runtime"
	"strings"
	"testing"
	"time"
)

func TestOpenRuntimeInitializesStateAndSchema(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	runtime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	if info, err := os.Stat(runtime.StateDir()); err != nil {
		t.Fatalf("os.Stat(stateDir) error = %v", err)
	} else if goruntime.GOOS != "windows" && info.Mode().Perm() != 0o700 {
		t.Fatalf("state dir permissions = %o, want 700", info.Mode().Perm())
	}

	if info, err := os.Stat(runtime.BlobDir()); err != nil {
		t.Fatalf("os.Stat(blobDir) error = %v", err)
	} else if goruntime.GOOS != "windows" && info.Mode().Perm() != 0o700 {
		t.Fatalf("blob dir permissions = %o, want 700", info.Mode().Perm())
	}

	var journalMode string
	if err := runtime.DB().QueryRow(`PRAGMA journal_mode;`).Scan(&journalMode); err != nil {
		t.Fatalf("QueryRow(PRAGMA journal_mode) error = %v", err)
	}
	if strings.ToLower(journalMode) != "wal" {
		t.Fatalf("journal mode = %q, want wal", journalMode)
	}

	tables := []string{
		"endpoints",
		"endpoint_addresses",
		"persons",
		"groups",
		"group_memberships",
		"messages",
		"deliveries",
		"group_messages",
		"group_message_eligibility",
		"group_reads",
		"events",
	}
	for _, table := range tables {
		var name string
		if err := runtime.DB().QueryRow(`
SELECT name
FROM sqlite_master
WHERE type = 'table' AND name = ?
`, table).Scan(&name); err != nil {
			t.Fatalf("table %s missing: %v", table, err)
		}
	}

	var groupPageIndex string
	if err := runtime.DB().QueryRow(`
SELECT name
FROM sqlite_master
WHERE type = 'index' AND name = 'idx_groups_created_address'
`).Scan(&groupPageIndex); err != nil {
		t.Fatalf("group pagination index missing: %v", err)
	}

	rows, err := runtime.DB().Query(`PRAGMA table_info(endpoints)`)
	if err != nil {
		t.Fatalf("PRAGMA table_info(endpoints) error = %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan endpoints table info error = %v", err)
		}
		if name == "kind" {
			t.Fatal("endpoints schema still contains kind column")
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate endpoints table info error = %v", err)
	}
}

func TestOpenRuntimeMigratesForwardedMessageColumn(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	if err := ensureDir(stateDir); err != nil {
		t.Fatalf("ensureDir(stateDir) error = %v", err)
	}
	if err := ensureDir(filepath.Join(stateDir, blobsDirName)); err != nil {
		t.Fatalf("ensureDir(blobDir) error = %v", err)
	}

	dbPath := filepath.Join(stateDir, databaseFilename)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	_, err = db.Exec(`
PRAGMA foreign_keys = ON;
CREATE TABLE messages (
  message_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  sender_endpoint_id TEXT,
  subject TEXT NOT NULL,
  content_type TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  idempotency_key TEXT,
  body_blob_ref TEXT NOT NULL,
  body_size INTEGER NOT NULL,
  body_sha256 TEXT NOT NULL,
  reply_to_message_id TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);
`)
	if err != nil {
		db.Close()
		t.Fatalf("create legacy messages table error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	runtime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	hasColumn, err := tableHasColumn(context.Background(), runtime.DB(), "messages", "forwarded_message_id")
	if err != nil {
		t.Fatalf("tableHasColumn(messages.forwarded_message_id) error = %v", err)
	}
	if !hasColumn {
		t.Fatal("messages.forwarded_message_id missing after migration")
	}

	hasColumn, err = tableHasColumn(context.Background(), runtime.DB(), "messages", "forwarded_from_address")
	if err != nil {
		t.Fatalf("tableHasColumn(messages.forwarded_from_address) error = %v", err)
	}
	if !hasColumn {
		t.Fatal("messages.forwarded_from_address missing after migration")
	}
}

func TestOpenRuntimeBackfillsForwardedFromAddress(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	if err := ensureDir(stateDir); err != nil {
		t.Fatalf("ensureDir(stateDir) error = %v", err)
	}
	if err := ensureDir(filepath.Join(stateDir, blobsDirName)); err != nil {
		t.Fatalf("ensureDir(blobDir) error = %v", err)
	}

	dbPath := filepath.Join(stateDir, databaseFilename)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	_, err = db.Exec(`
PRAGMA foreign_keys = ON;
CREATE TABLE endpoints (
  endpoint_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE endpoint_addresses (
  address TEXT PRIMARY KEY,
  endpoint_id TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE messages (
  message_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  sender_endpoint_id TEXT,
  subject TEXT NOT NULL,
  content_type TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  idempotency_key TEXT,
  body_blob_ref TEXT NOT NULL,
  body_size INTEGER NOT NULL,
  body_sha256 TEXT NOT NULL,
  forwarded_message_id TEXT,
  reply_to_message_id TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);
INSERT INTO endpoints (endpoint_id, created_at, metadata_json)
VALUES ('ep_source', '2026-04-18T00:00:00Z', '{}');
INSERT INTO endpoint_addresses (address, endpoint_id, created_at)
VALUES ('agent/source', 'ep_source', '2026-04-18T00:00:00Z');
INSERT INTO messages (
  message_id,
  created_at,
  sender_endpoint_id,
  subject,
  content_type,
  schema_version,
  body_blob_ref,
  body_size,
  body_sha256,
  forwarded_message_id,
  reply_to_message_id,
  metadata_json
) VALUES
  ('msg_source', '2026-04-18T00:00:00Z', 'ep_source', 'source', 'text/plain', 'v1', 'blob_source', 0, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', NULL, NULL, '{}'),
  ('msg_forwarded', '2026-04-18T00:00:01Z', NULL, 'forwarded', 'text/plain', 'v1', 'blob_forwarded', 0, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 'msg_source', NULL, '{}');
`)
	if err != nil {
		db.Close()
		t.Fatalf("create legacy forwarded data error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	runtime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	var forwardedFromAddress sql.NullString
	if err := runtime.DB().QueryRow(`
SELECT forwarded_from_address
FROM messages
WHERE message_id = 'msg_forwarded'
`).Scan(&forwardedFromAddress); err != nil {
		t.Fatalf("query forwarded_from_address error = %v", err)
	}
	if !forwardedFromAddress.Valid || forwardedFromAddress.String != "agent/source" {
		t.Fatalf("forwarded_from_address = %v, want agent/source", forwardedFromAddress)
	}
}

func TestSendImplicitlyCreatesAddressesOnce(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := store.Send(context.Background(), SendParams{
			ToAddress:     "workflow/reviewer/task-123",
			FromAddress:   "agent/sender",
			Subject:       "review request",
			ContentType:   "text/plain",
			SchemaVersion: "v1",
			Body:          []byte("hello reviewer"),
		}); err != nil {
			t.Fatalf("Send(attempt %d) error = %v", attempt+1, err)
		}
	}

	var addressCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM endpoint_addresses`).Scan(&addressCount); err != nil {
		t.Fatalf("count endpoint_addresses error = %v", err)
	}
	if addressCount != 2 {
		t.Fatalf("endpoint address count = %d, want 2", addressCount)
	}

	var registrationEvents int
	if err := runtime.DB().QueryRow(`
SELECT COUNT(*)
FROM events
WHERE event_type = 'endpoint_registered'
`).Scan(&registrationEvents); err != nil {
		t.Fatalf("count endpoint_registered events error = %v", err)
	}
	if registrationEvents != 2 {
		t.Fatalf("endpoint_registered event count = %d, want 2", registrationEvents)
	}
}

func TestWriteBlobSyncsFileBeforeRenameAndDirectoryAfter(t *testing.T) {
	t.Parallel()

	blobDir := filepath.Join(t.TempDir(), "blobs")
	if err := os.MkdirAll(blobDir, 0o700); err != nil {
		t.Fatalf("os.MkdirAll(blobDir) error = %v", err)
	}

	store := NewStore(nil, nil, nil, blobDir)
	var ops []string
	store.createBlobTemp = func(dir, pattern string) (blobTempFile, error) {
		ops = append(ops, "create:"+dir)
		return &recordingBlobTempFile{
			name: filepath.Join(dir, "blob.tmp"),
			ops:  &ops,
		}, nil
	}
	store.renameFile = func(oldPath, newPath string) error {
		ops = append(ops, "rename:"+filepath.Base(oldPath)+"->"+filepath.Base(newPath))
		return nil
	}
	store.removeFile = func(path string) error {
		ops = append(ops, "remove:"+filepath.Base(path))
		return nil
	}
	store.syncDir = func(path string) error {
		ops = append(ops, "dir-sync:"+path)
		return nil
	}

	blobRef, bodySize, _, err := store.writeBlob(context.Background(), []byte("hello"))
	if err != nil {
		t.Fatalf("writeBlob() error = %v", err)
	}
	if !strings.HasPrefix(blobRef, "blob_") {
		t.Fatalf("blobRef = %q, want blob_ prefix", blobRef)
	}
	if bodySize != 5 {
		t.Fatalf("bodySize = %d, want 5", bodySize)
	}

	wantOps := []string{
		"create:" + blobDir,
		"write:hello",
		"file-sync",
		"close",
		"rename:blob.tmp->" + blobRef,
		"dir-sync:" + blobDir,
	}
	if len(ops) != len(wantOps) {
		t.Fatalf("len(ops) = %d, want %d; ops=%v", len(ops), len(wantOps), ops)
	}
	for i, want := range wantOps {
		if ops[i] != want {
			t.Fatalf("ops[%d] = %q, want %q (ops=%v)", i, ops[i], want, ops)
		}
	}
}

func TestSendAbortsBeforeMetadataCommitWhenBlobDirectorySyncFails(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	store.syncDir = func(path string) error {
		return errors.New("dir sync failed")
	}

	_, err = store.Send(context.Background(), SendParams{
		ToAddress:     "workflow/reviewer/task-123",
		FromAddress:   "agent/sender",
		Subject:       "review request",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("hello reviewer"),
	})
	if err == nil || !strings.Contains(err.Error(), "sync blob directory") {
		t.Fatalf("Send() error = %v, want sync blob directory failure", err)
	}

	var messageCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&messageCount); err != nil {
		t.Fatalf("count messages error = %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("message count = %d, want 0", messageCount)
	}

	var deliveryCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM deliveries`).Scan(&deliveryCount); err != nil {
		t.Fatalf("count deliveries error = %v", err)
	}
	if deliveryCount != 0 {
		t.Fatalf("delivery count = %d, want 0", deliveryCount)
	}

	var endpointAddressCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM endpoint_addresses`).Scan(&endpointAddressCount); err != nil {
		t.Fatalf("count endpoint_addresses error = %v", err)
	}
	if endpointAddressCount != 0 {
		t.Fatalf("endpoint address count = %d, want 0", endpointAddressCount)
	}
}

func TestSendContinuesWhenBlobDirectorySyncIsUnsupported(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	store.syncDir = func(path string) error {
		return errors.ErrUnsupported
	}

	result, err := store.Send(context.Background(), SendParams{
		ToAddress:     "workflow/reviewer/task-123",
		FromAddress:   "agent/sender",
		Subject:       "review request",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("hello reviewer"),
	})
	if err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}

	var state string
	if err := runtime.DB().QueryRow(`
SELECT state
FROM deliveries
WHERE delivery_id = ?
`, result.DeliveryID).Scan(&state); err != nil {
		t.Fatalf("QueryRow(delivery state) error = %v", err)
	}
	if state != "queued" {
		t.Fatalf("delivery state = %q, want queued", state)
	}
}

func TestSendImplicitAddressCreationIsConcurrentSafe(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()
	runtime.DB().SetMaxOpenConns(16)
	runtime.DB().SetMaxIdleConns(16)

	store := runtime.Store()

	type sendResult struct {
		result SendResult
		err    error
	}
	const workers = 16
	results := make(chan sendResult, workers)
	start := make(chan struct{})
	for range workers {
		go func() {
			<-start
			result, err := store.Send(context.Background(), SendParams{
				ToAddress:     "workflow/concurrent",
				Subject:       "race-safe send",
				ContentType:   "text/plain",
				SchemaVersion: "v1",
				Body:          []byte("hello"),
			})
			results <- sendResult{result: result, err: err}
		}()
	}
	close(start)

	var sendResults []sendResult
	for i := 0; i < workers; i++ {
		select {
		case result := <-results:
			sendResults = append(sendResults, result)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for concurrent send results")
		}
	}
	for i, result := range sendResults {
		if result.err != nil {
			t.Fatalf("Send(%d) error = %v", i, result.err)
		}
	}
	seenDeliveryIDs := make(map[string]struct{}, workers)
	for _, result := range sendResults {
		if _, exists := seenDeliveryIDs[result.result.DeliveryID]; exists {
			t.Fatalf("concurrent sends reused delivery id %q", result.result.DeliveryID)
		}
		seenDeliveryIDs[result.result.DeliveryID] = struct{}{}
	}

	deliveries, err := store.List(context.Background(), ListParams{Address: "workflow/concurrent", State: "queued"})
	if err != nil {
		t.Fatalf("List(concurrent queued) error = %v", err)
	}
	if len(deliveries) != workers {
		t.Fatalf("len(concurrent queued) = %d, want %d", len(deliveries), workers)
	}

	var endpointAddressCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM endpoint_addresses WHERE address = ?`, "workflow/concurrent").Scan(&endpointAddressCount); err != nil {
		t.Fatalf("count workflow/concurrent addresses error = %v", err)
	}
	if endpointAddressCount != 1 {
		t.Fatalf("workflow/concurrent address count = %d, want 1", endpointAddressCount)
	}

	var endpointCount int
	if err := runtime.DB().QueryRow(`
SELECT COUNT(*)
FROM endpoints
WHERE endpoint_id IN (
  SELECT endpoint_id
  FROM endpoint_addresses
  WHERE address = ?
)
`, "workflow/concurrent").Scan(&endpointCount); err != nil {
		t.Fatalf("count endpoints error = %v", err)
	}
	if endpointCount != 1 {
		t.Fatalf("endpoint count = %d, want 1", endpointCount)
	}

	for i, delivery := range deliveries {
		if delivery.RecipientAddress != "workflow/concurrent" {
			t.Fatalf("deliveries[%d] recipient address = %q, want workflow/concurrent", i, delivery.RecipientAddress)
		}
		if delivery.Subject != "race-safe send" {
			t.Fatalf("deliveries[%d] subject = %q, want race-safe send", i, delivery.Subject)
		}
	}

	if got := len(seenDeliveryIDs); got != workers {
		t.Fatalf("unique delivery id count = %d, want %d", got, workers)
	}
}

func TestSendAndListHappyPath(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		bodyFile string
		stdin    string
		body     string
	}{
		{
			name:     "stdin",
			bodyFile: "-",
			stdin:    "hello from stdin",
			body:     "hello from stdin",
		},
		{
			name: "file",
			body: "hello from file",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := filepath.Join(t.TempDir(), "waypost-state")

			bodyFile := tc.bodyFile
			if bodyFile == "" {
				path := filepath.Join(t.TempDir(), "body.txt")
				if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
					t.Fatalf("os.WriteFile(body) error = %v", err)
				}
				bodyFile = path
			}

			sendStdout := &bytes.Buffer{}
			sendApp := NewApp(strings.NewReader(tc.stdin), sendStdout, &bytes.Buffer{})
			if err := sendApp.RunWithStateDir(context.Background(), stateDir, []string{
				"send",
				"--to", "workflow/reviewer/task-123",
				"--from", "agent/sender",
				"--subject", "review request",
				"--body-file", bodyFile,
			}); err != nil {
				t.Fatalf("send error = %v", err)
			}
			if !strings.Contains(sendStdout.String(), "delivery_id=") {
				t.Fatalf("send output = %q, want delivery_id", sendStdout.String())
			}
			if strings.Contains(sendStdout.String(), "message_id=") {
				t.Fatalf("send output = %q, want compact default output", sendStdout.String())
			}

			listStdout := &bytes.Buffer{}
			listApp := NewApp(strings.NewReader(""), listStdout, &bytes.Buffer{})
			if err := listApp.RunWithStateDir(context.Background(), stateDir, []string{
				"list",
				"--for", "workflow/reviewer/task-123",
				"--json",
			}); err != nil {
				t.Fatalf("list error = %v", err)
			}

			var page Page[ListedDelivery]
			if err := json.Unmarshal(listStdout.Bytes(), &page); err != nil {
				t.Fatalf("json.Unmarshal(list output) error = %v", err)
			}
			deliveries := page.Items
			if len(deliveries) != 1 {
				t.Fatalf("len(deliveries) = %d, want 1", len(deliveries))
			}
			if deliveries[0].Subject != "review request" {
				t.Fatalf("delivery subject = %q, want review request", deliveries[0].Subject)
			}
			if deliveries[0].RecipientAddress != "workflow/reviewer/task-123" {
				t.Fatalf("recipient address = %q", deliveries[0].RecipientAddress)
			}
			if deliveries[0].SenderEndpointID == nil {
				t.Fatal("sender endpoint id = nil, want non-nil")
			}

			runtime, err := OpenRuntime(context.Background(), stateDir)
			if err != nil {
				t.Fatalf("OpenRuntime(verify) error = %v", err)
			}
			defer runtime.Close()

			var messageCount int
			if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&messageCount); err != nil {
				t.Fatalf("count messages error = %v", err)
			}
			if messageCount != 1 {
				t.Fatalf("message count = %d, want 1", messageCount)
			}

			var deliveryCount int
			if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM deliveries`).Scan(&deliveryCount); err != nil {
				t.Fatalf("count deliveries error = %v", err)
			}
			if deliveryCount != 1 {
				t.Fatalf("delivery count = %d, want 1", deliveryCount)
			}

			var eventCount int
			if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM events`).Scan(&eventCount); err != nil {
				t.Fatalf("count events error = %v", err)
			}
			if eventCount != 4 {
				t.Fatalf("event count = %d, want 4", eventCount)
			}

			var blobRef string
			if err := runtime.DB().QueryRow(`SELECT body_blob_ref FROM messages LIMIT 1`).Scan(&blobRef); err != nil {
				t.Fatalf("select body_blob_ref error = %v", err)
			}
			body, err := os.ReadFile(filepath.Join(runtime.BlobDir(), blobRef))
			if err != nil {
				t.Fatalf("os.ReadFile(blob) error = %v", err)
			}
			if string(body) != tc.body {
				t.Fatalf("blob body = %q, want %q", string(body), tc.body)
			}
		})
	}
}

func TestAppSendNotifyReportsFailureWithoutRollingBackDelivery(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	var stdout bytes.Buffer
	notifyCalls := 0
	app := NewAppWithOptions(strings.NewReader("delegate body\n"), &stdout, &bytes.Buffer{}, AppOptions{
		SendNotifier: func(_ context.Context, _ *Store, request SendNotificationRequest) SendNotificationOutcome {
			notifyCalls++
			if request.Params.ToAddress != "agent-deck/coder" || request.Params.FromAddress != "agent-deck/supervisor" {
				t.Fatalf("notify send params = %+v", request.Params)
			}
			if request.Result.DeliveryID == "" {
				t.Fatal("notify delivery ID = empty")
			}
			return SendNotificationOutcome{
				Status: "failed",
				Scheme: "agent-deck",
				Err:    errors.New("wakeup failed"),
			}
		},
	})

	err := app.RunWithStateDir(context.Background(), stateDir, []string{
		"send",
		"--to", "agent-deck/coder",
		"--from", "agent-deck/supervisor",
		"--subject", "delegate",
		"--body-file", "-",
		"--notify",
		"--json",
	})
	if err != nil {
		t.Fatalf("RunWithStateDir(send --notify) error = %v", err)
	}
	if notifyCalls != 1 {
		t.Fatalf("notify calls = %d, want 1", notifyCalls)
	}

	var output map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("json.Unmarshal(send output) error = %v; output = %q", err, stdout.String())
	}
	deliveryID, _ := output["delivery_id"].(string)
	if deliveryID == "" {
		t.Fatalf("delivery_id = %v, want non-empty", output["delivery_id"])
	}
	if output["notify_status"] != "failed" || output["notify_scheme"] != "agent-deck" || output["notify_error"] != "wakeup failed" {
		t.Fatalf("send --notify output = %v", output)
	}

	runtime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()
	deliveries, err := runtime.Store().ReadDeliveries(context.Background(), []string{deliveryID})
	if err != nil {
		t.Fatalf("ReadDeliveries() error = %v", err)
	}
	if len(deliveries) != 1 || deliveries[0].State != "queued" {
		t.Fatalf("deliveries after notify failure = %+v, want one queued delivery", deliveries)
	}
}

func TestNotificationOutputProjectsUnconfirmedDetailWithoutError(t *testing.T) {
	t.Parallel()

	outcome := SendNotificationOutcome{
		Status: "unconfirmed",
		Scheme: "agent-deck",
		Detail: "nudge reached the target pane but turn submission was not confirmed",
	}
	projected := CompactSendResultWithNotification(SendResult{DeliveryID: "dlv_1"}, outcome)
	if projected.NotifyStatus != "unconfirmed" || projected.NotifyDetail == nil || *projected.NotifyDetail != outcome.Detail {
		t.Fatalf("compact notification output = %+v, want unconfirmed detail", projected)
	}
	if projected.NotifyError != nil {
		t.Fatalf("notify_error = %v, want nil", *projected.NotifyError)
	}

	batch := SendBatchCLIOutput(SendBatchResult{
		ToAddresses: []string{"agent-deck/target"},
		SentCount:   1,
		Items: []SendBatchItem{{
			ToAddress:    "agent-deck/target",
			Result:       SendResult{DeliveryID: "dlv_1"},
			Notification: &outcome,
		}},
	}, false, true)
	if len(batch.Results) != 1 || batch.Results[0].NotifyDetail == nil || *batch.Results[0].NotifyDetail != outcome.Detail {
		t.Fatalf("batch notification output = %+v, want unconfirmed detail", batch)
	}
}

func TestAppStaleReturnsStructuredResults(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")

	runtime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}

	store := runtime.Store()
	if _, err := store.Send(context.Background(), SendParams{
		ToAddress:     "workflow/stale",
		FromAddress:   "agent/sender",
		Subject:       "stale subject",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("stale body"),
	}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	oldestEligibleAt := formatTimestamp(time.Now().UTC().Add(-10 * time.Minute))
	if _, err := runtime.DB().Exec(`
UPDATE deliveries
SET visible_at = ?
WHERE recipient_endpoint_id = (
  SELECT endpoint_id
  FROM endpoint_addresses
  WHERE address = ?
)
`, oldestEligibleAt, "workflow/stale"); err != nil {
		t.Fatalf("Exec(update stale visible_at) error = %v", err)
	}
	runtime.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	app := NewApp(strings.NewReader(""), stdout, stderr)
	if err := app.RunWithStateDir(context.Background(), stateDir, []string{
		"stale",
		"--for", "workflow/stale",
		"--older-than", "5m",
		"--json",
	}); err != nil {
		t.Fatalf("Run(stale) error = %v", err)
	}

	var stale []StaleAddress
	if err := json.Unmarshal(stdout.Bytes(), &stale); err != nil {
		t.Fatalf("json.Unmarshal(stale output) error = %v; stdout = %q", err, stdout.String())
	}
	if len(stale) != 1 {
		t.Fatalf("len(stale) = %d, want 1", len(stale))
	}
	if stale[0].Address != "workflow/stale" {
		t.Fatalf("stale[0].address = %q, want workflow/stale", stale[0].Address)
	}
	if stale[0].OldestEligibleAt != oldestEligibleAt {
		t.Fatalf("stale[0].oldest_eligible_at = %q, want %q", stale[0].OldestEligibleAt, oldestEligibleAt)
	}
	if stale[0].ClaimableCount != 1 {
		t.Fatalf("stale[0].claimable_count = %d, want 1", stale[0].ClaimableCount)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestSendRejectsEmptyBody(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	for _, body := range [][]byte{nil, []byte{}} {
		_, err := store.Send(context.Background(), SendParams{
			ToAddress:     "workflow/reviewer/task-123",
			FromAddress:   "agent/sender",
			Subject:       "review request",
			ContentType:   "text/plain",
			SchemaVersion: "v1",
			Body:          body,
		})
		if !errors.Is(err, ErrEmptyBody) {
			t.Fatalf("Send(empty body) error = %v, want ErrEmptyBody", err)
		}
	}

	var messageCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&messageCount); err != nil {
		t.Fatalf("count messages error = %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("message count = %d, want 0", messageCount)
	}

	var deliveryCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM deliveries`).Scan(&deliveryCount); err != nil {
		t.Fatalf("count deliveries error = %v", err)
	}
	if deliveryCount != 0 {
		t.Fatalf("delivery count = %d, want 0", deliveryCount)
	}

	entries, err := os.ReadDir(runtime.BlobDir())
	if err != nil {
		t.Fatalf("os.ReadDir(blob dir) error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("len(blob entries) = %d, want 0", len(entries))
	}
}

func TestAppRunStaleGroupViewJSON(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	runtime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}

	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/stale")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}
	store.now = func() time.Time {
		return time.Date(2026, 3, 31, 16, 0, 0, 0, time.UTC)
	}
	message := mustSendGroupMessage(t, store, group.Address, "agent/sender", "stale subject", "stale body")
	store.now = func() time.Time {
		return time.Date(2026, 3, 31, 16, 10, 0, 0, time.UTC)
	}
	runtime.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	app := NewApp(strings.NewReader(""), stdout, stderr)
	if err := app.RunWithStateDir(context.Background(), stateDir, []string{
		"stale",
		"--for", group.Address,
		"--as", "alice",
		"--older-than", "5m",
		"--json",
	}); err != nil {
		t.Fatalf("Run(stale group) error = %v", err)
	}

	var stale []StaleAddress
	if err := json.Unmarshal(stdout.Bytes(), &stale); err != nil {
		t.Fatalf("json.Unmarshal(stale group output) error = %v; stdout = %q", err, stdout.String())
	}
	if len(stale) != 1 {
		t.Fatalf("len(stale group) = %d, want 1", len(stale))
	}
	if stale[0].Address != group.Address {
		t.Fatalf("stale[0].address = %q, want %q", stale[0].Address, group.Address)
	}
	if stale[0].Person != "alice" {
		t.Fatalf("stale[0].person = %q, want alice", stale[0].Person)
	}
	if stale[0].OldestEligibleAt != message.MessageCreatedAt {
		t.Fatalf("stale[0].oldest_eligible_at = %q, want %q", stale[0].OldestEligibleAt, message.MessageCreatedAt)
	}
	if stale[0].ClaimableCount != 1 {
		t.Fatalf("stale[0].claimable_count = %d, want 1", stale[0].ClaimableCount)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAppSendRejectsEmptyBodyInput(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		stdin    string
		bodyFile string
	}{
		{
			name:     "empty stdin",
			bodyFile: "-",
		},
		{
			name: "empty file",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := filepath.Join(t.TempDir(), "waypost-state")

			bodyFile := tc.bodyFile
			if bodyFile == "" {
				path := filepath.Join(t.TempDir(), "body.txt")
				if err := os.WriteFile(path, []byte{}, 0o600); err != nil {
					t.Fatalf("os.WriteFile(empty body) error = %v", err)
				}
				bodyFile = path
			}

			app := NewApp(strings.NewReader(tc.stdin), &bytes.Buffer{}, &bytes.Buffer{})
			err := app.RunWithStateDir(context.Background(), stateDir, []string{
				"send",
				"--to", "workflow/reviewer/task-123",
				"--from", "agent/sender",
				"--subject", "review request",
				"--body-file", bodyFile,
			})
			if !errors.Is(err, ErrEmptyBody) {
				t.Fatalf("Run(empty body) error = %v, want ErrEmptyBody", err)
			}

			runtime, err := OpenRuntime(context.Background(), stateDir)
			if err != nil {
				t.Fatalf("OpenRuntime(verify) error = %v", err)
			}
			defer runtime.Close()

			var messageCount int
			if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&messageCount); err != nil {
				t.Fatalf("count messages error = %v", err)
			}
			if messageCount != 0 {
				t.Fatalf("message count = %d, want 0", messageCount)
			}

			entries, err := os.ReadDir(runtime.BlobDir())
			if err != nil {
				t.Fatalf("os.ReadDir(blob dir) error = %v", err)
			}
			if len(entries) != 0 {
				t.Fatalf("len(blob entries) = %d, want 0", len(entries))
			}
		})
	}
}

func TestAppSendBatchValidatesEveryRecipientBeforeAnyWrite(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name       string
		recipients []string
	}{
		{
			name:       "one later invalid recipient",
			recipients: []string{"workflow/valid", "not an address"},
		},
		{
			name:       "multiple later invalid recipients",
			recipients: []string{"workflow/first", "workflow/second", "not an address", "also not an address"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stateDir := filepath.Join(t.TempDir(), "waypost-state")
			args := []string{"send"}
			for _, recipient := range testCase.recipients {
				args = append(args, "--to", recipient)
			}
			args = append(args,
				"--from", "agent/sender",
				"--body-file", "-",
			)

			app := NewApp(strings.NewReader("body"), &bytes.Buffer{}, &bytes.Buffer{})
			err := app.RunWithStateDir(context.Background(), stateDir, args)
			if err == nil || !strings.Contains(err.Error(), "invalid address") {
				t.Fatalf("RunWithStateDir(batch invalid recipient) error = %v, want invalid address", err)
			}

			runtime, err := OpenRuntime(context.Background(), stateDir)
			if err != nil {
				t.Fatalf("OpenRuntime() error = %v", err)
			}
			defer runtime.Close()
			for _, table := range []string{"messages", "deliveries"} {
				var count int
				if err := runtime.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
					t.Fatalf("count %s error = %v", table, err)
				}
				if count != 0 {
					t.Fatalf("%s count = %d, want 0", table, count)
				}
			}
		})
	}
}

func TestAppSendBatchEnforcesRawRecipientLimitBeforeDeduplication(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name         string
		rawCount     int
		wantErr      bool
		wantMessages int
	}{
		{
			name:         "ten duplicate raw recipients execute once",
			rawCount:     MaxSendRecipients,
			wantMessages: 1,
		},
		{
			name:     "eleven duplicate raw recipients are rejected",
			rawCount: MaxSendRecipients + 1,
			wantErr:  true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			stateDir := filepath.Join(t.TempDir(), "waypost-state")
			var stdout bytes.Buffer
			args := []string{"send"}
			for range testCase.rawCount {
				args = append(args, "--to", "workflow/duplicate")
			}
			args = append(args,
				"--from", "agent/sender",
				"--body-file", "-",
			)

			app := NewApp(strings.NewReader("body"), &stdout, &bytes.Buffer{})
			err := app.RunWithStateDir(context.Background(), stateDir, args)
			if testCase.wantErr {
				if err == nil || !strings.Contains(err.Error(), "at most") {
					t.Fatalf("RunWithStateDir(%d raw recipients) error = %v, want recipient limit error", testCase.rawCount, err)
				}
			} else if err != nil {
				t.Fatalf("RunWithStateDir(%d raw recipients) error = %v", testCase.rawCount, err)
			}

			runtime, err := OpenRuntime(context.Background(), stateDir)
			if err != nil {
				t.Fatalf("OpenRuntime() error = %v", err)
			}
			defer runtime.Close()
			for _, table := range []string{"messages", "deliveries"} {
				var count int
				if err := runtime.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
					t.Fatalf("count %s error = %v", table, err)
				}
				if count != testCase.wantMessages {
					t.Fatalf("%s count = %d, want %d", table, count, testCase.wantMessages)
				}
			}

			if !testCase.wantErr && !strings.Contains(stdout.String(), "status=sent recipient_count=1 sent_count=1 failed_count=0") {
				t.Fatalf("batch stdout = %q, want one-recipient batch envelope", stdout.String())
			}
		})
	}
}

func TestAppSendBatchNotifyProjectsEveryOutcome(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	var stdout bytes.Buffer
	var notified []string
	app := NewAppWithOptions(strings.NewReader("body"), &stdout, &bytes.Buffer{}, AppOptions{
		SendNotifier: func(_ context.Context, _ *Store, request SendNotificationRequest) SendNotificationOutcome {
			notified = append(notified, request.Params.ToAddress)
			if request.Params.ToAddress == "workflow/two" {
				return SendNotificationOutcome{
					Status: "failed",
					Scheme: "agent-deck",
					Err:    errors.New("wakeup failed"),
				}
			}
			return SendNotificationOutcome{Status: "sent", Scheme: "agent-deck"}
		},
	})

	err := app.RunWithStateDir(context.Background(), stateDir, []string{
		"send",
		"--to", "workflow/one",
		"--to", "workflow/two",
		"--from", "agent/sender",
		"--body-file", "-",
		"--notify",
		"--json",
	})
	if err != nil {
		t.Fatalf("RunWithStateDir(batch --notify) error = %v", err)
	}
	if want := []string{"workflow/one", "workflow/two"}; !reflect.DeepEqual(notified, want) {
		t.Fatalf("notified recipients = %v, want %v", notified, want)
	}

	var output map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("json.Unmarshal(batch --notify output) error = %v; output = %q", err, stdout.String())
	}
	if output["status"] != "sent" || output["sent_count"] != float64(2) || output["failed_count"] != float64(0) {
		t.Fatalf("batch --notify output = %v, want two durable successes", output)
	}
	results, ok := output["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("batch --notify results = %#v, want two items", output["results"])
	}
	first := results[0].(map[string]any)
	second := results[1].(map[string]any)
	if first["notify_status"] != "sent" || first["notify_scheme"] != "agent-deck" || first["notify_error"] != nil {
		t.Fatalf("first notification result = %v, want sent agent-deck outcome", first)
	}
	if second["notify_status"] != "failed" || second["notify_scheme"] != "agent-deck" || second["notify_error"] != "wakeup failed" {
		t.Fatalf("second notification result = %v, want failed agent-deck outcome", second)
	}
}

func TestAppSendBatchCancellationStopsBeforeNextRecipient(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	ctx, cancel := context.WithCancel(context.Background())
	var stdout bytes.Buffer
	notifyCalls := 0
	app := NewAppWithOptions(strings.NewReader("body"), &stdout, &bytes.Buffer{}, AppOptions{
		SendNotifier: func(_ context.Context, _ *Store, request SendNotificationRequest) SendNotificationOutcome {
			notifyCalls++
			if request.Params.ToAddress != "workflow/one" {
				t.Fatalf("notification recipient = %q, want only first recipient", request.Params.ToAddress)
			}
			cancel()
			return SendNotificationOutcome{Status: "sent"}
		},
	})

	err := app.RunWithStateDir(ctx, stateDir, []string{
		"send",
		"--to", "workflow/one",
		"--to", "workflow/two",
		"--from", "agent/sender",
		"--body-file", "-",
		"--notify",
		"--json",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithStateDir(canceled batch) error = %v, want context.Canceled", err)
	}
	if notifyCalls != 1 {
		t.Fatalf("notification calls = %d, want one", notifyCalls)
	}
	if stdout.Len() != 0 {
		t.Fatalf("canceled batch stdout = %q, want no normal envelope", stdout.String())
	}

	runtime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()
	for _, table := range []string{"messages", "deliveries"} {
		var count int
		if err := runtime.DB().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("count %s error = %v", table, err)
		}
		if count != 1 {
			t.Fatalf("%s count = %d, want one durable first-recipient record", table, count)
		}
	}
}

func TestInvalidCLIPathsDoNotCreateRuntimeState(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		args []string
	}{
		{
			name: "unknown command",
			args: []string{"unknown"},
		},
		{
			name: "send missing body file",
			args: []string{"send", "--to", "workflow/reviewer/task-123"},
		},
		{
			name: "send invalid flag",
			args: []string{"send", "--bogus"},
		},
		{
			name: "send conflicting formats",
			args: []string{"send", "--to", "workflow/reviewer/task-123", "--body-file", "-", "--json", "--yaml"},
		},
		{
			name: "send missing to",
			args: []string{"send", "--body-file", "-"},
		},
		{
			name: "send invalid known-session target",
			args: []string{"send", "--to", "agent-deck", "--body-file", "-"},
		},
		{
			name: "forward missing to",
			args: []string{"forward", "--message", "msg_123"},
		},
		{
			name: "forward missing source selector",
			args: []string{"forward", "--to", "workflow/reviewer/task-123"},
		},
		{
			name: "forward conflicting source selectors",
			args: []string{"forward", "--message", "msg_123", "--delivery", "dlv_123", "--to", "workflow/reviewer/task-123"},
		},
		{
			name: "forward conflicting formats",
			args: []string{"forward", "--message", "msg_123", "--to", "workflow/reviewer/task-123", "--json", "--yaml"},
		},
		{
			name: "forward invalid known-session target",
			args: []string{"forward", "--message", "msg_123", "--to", "agent-deck"},
		},
		{
			name: "list missing for",
			args: []string{"list", "--json"},
		},
		{
			name: "list conflicting formats",
			args: []string{"list", "--for", "workflow/reviewer/task-123", "--json", "--yaml"},
		},
		{
			name: "list invalid state",
			args: []string{"list", "--for", "workflow/reviewer/task-123", "--state", "unknown"},
		},
		{
			name: "stale missing for",
			args: []string{"stale", "--older-than", "5m", "--json"},
		},
		{
			name: "stale empty for",
			args: []string{"stale", "--for", "   ", "--older-than", "5m", "--json"},
		},
		{
			name: "stale missing older-than",
			args: []string{"stale", "--for", "workflow/reviewer/task-123", "--json"},
		},
		{
			name: "stale zero older-than",
			args: []string{"stale", "--for", "workflow/reviewer/task-123", "--older-than", "0s", "--json"},
		},
		{
			name: "stale negative older-than",
			args: []string{"stale", "--for", "workflow/reviewer/task-123", "--older-than", "-1s", "--json"},
		},
		{
			name: "stale missing structured format",
			args: []string{"stale", "--for", "workflow/reviewer/task-123", "--older-than", "5m"},
		},
		{
			name: "stale conflicting formats",
			args: []string{"stale", "--for", "workflow/reviewer/task-123", "--older-than", "5m", "--json", "--yaml"},
		},
		{
			name: "stale group multiple for",
			args: []string{"stale", "--for", "group/ops", "--for", "group/dev", "--as", "alice", "--older-than", "5m", "--json"},
		},
		{
			name: "group missing subcommand",
			args: []string{"group"},
		},
		{
			name: "group unknown subcommand",
			args: []string{"group", "bogus"},
		},
		{
			name: "group create missing group",
			args: []string{"group", "create"},
		},
		{
			name: "group create invalid known-session nested target",
			args: []string{"group", "create", "--group", "codex/reviewer/task"},
		},
		{
			name: "address missing subcommand",
			args: []string{"address"},
		},
		{
			name: "address unknown subcommand",
			args: []string{"address", "bogus"},
		},
		{
			name: "address inspect missing address",
			args: []string{"address", "inspect"},
		},
		{
			name: "recv missing for",
			args: []string{"recv"},
		},
		{
			name: "recv empty for",
			args: []string{"recv", "--for", "   "},
		},
		{
			name: "recv zero max",
			args: []string{"recv", "--for", "workflow/reviewer/task-123", "--max", "0"},
		},
		{
			name: "recv negative max",
			args: []string{"recv", "--for", "workflow/reviewer/task-123", "--max", "-1"},
		},
		{
			name: "recv too large max",
			args: []string{"recv", "--for", "workflow/reviewer/task-123", "--max", "11"},
		},
		{
			name: "recv conflicting formats",
			args: []string{"recv", "--for", "workflow/reviewer/task-123", "--json", "--yaml"},
		},
		{
			name: "read missing selector",
			args: []string{"read"},
		},
		{
			name: "read conflicting selectors",
			args: []string{"read", "--delivery", "dlv_123", "--message", "msg_123"},
		},
		{
			name: "read state without latest",
			args: []string{"read", "--state", "acked"},
		},
		{
			name: "read for without latest",
			args: []string{"read", "--for", "workflow/reviewer/task-123"},
		},
		{
			name: "read limit without latest",
			args: []string{"read", "--limit", "2"},
		},
		{
			name: "read latest missing for",
			args: []string{"read", "--latest"},
		},
		{
			name: "read conflicting latest and message selectors",
			args: []string{"read", "--latest", "--for", "workflow/reviewer/task-123", "--message", "msg_123"},
		},
		{
			name: "watch missing for",
			args: []string{"watch", "--json"},
		},
		{
			name: "watch empty for",
			args: []string{"watch", "--for", "   "},
		},
		{
			name: "watch negative timeout",
			args: []string{"watch", "--for", "workflow/reviewer/task-123", "--timeout", "-1s"},
		},
		{
			name: "watch conflicting formats",
			args: []string{"watch", "--for", "workflow/reviewer/task-123", "--json", "--yaml"},
		},
		{
			name: "wait missing for",
			args: []string{"wait", "--json"},
		},
		{
			name: "wait empty for",
			args: []string{"wait", "--for", "   "},
		},
		{
			name: "wait negative timeout",
			args: []string{"wait", "--for", "workflow/reviewer/task-123", "--timeout", "-1s"},
		},
		{
			name: "wait conflicting formats",
			args: []string{"wait", "--for", "workflow/reviewer/task-123", "--json", "--yaml"},
		},
		{
			name: "ack missing delivery",
			args: []string{"ack", "--lease-token", "lease_token"},
		},
		{
			name: "ack missing lease token",
			args: []string{"ack", "--delivery", "dlv_123"},
		},
		{
			name: "release missing delivery",
			args: []string{"release", "--lease-token", "lease_token"},
		},
		{
			name: "release missing lease token",
			args: []string{"release", "--delivery", "dlv_123"},
		},
		{
			name: "defer missing delivery",
			args: []string{"defer", "--lease-token", "lease_token", "--until", "2026-03-18T12:00:00Z"},
		},
		{
			name: "defer missing lease token",
			args: []string{"defer", "--delivery", "dlv_123", "--until", "2026-03-18T12:00:00Z"},
		},
		{
			name: "undefer missing delivery",
			args: []string{"undefer"},
		},
		{
			name: "fail missing delivery",
			args: []string{"fail", "--lease-token", "lease_token", "--reason", "tool crashed"},
		},
		{
			name: "fail missing lease token",
			args: []string{"fail", "--delivery", "dlv_123", "--reason", "tool crashed"},
		},
		{
			name: "fail missing reason",
			args: []string{"fail", "--delivery", "dlv_123", "--lease-token", "lease_token"},
		},
		{
			name: "dead-letter missing delivery",
			args: []string{"dead-letter", "--lease-token", "lease_token", "--reason", "unsupported request"},
		},
		{
			name: "dead-letter missing lease token",
			args: []string{"dead-letter", "--delivery", "dlv_123", "--reason", "unsupported request"},
		},
		{
			name: "dead-letter missing reason",
			args: []string{"dead-letter", "--delivery", "dlv_123", "--lease-token", "lease_token"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			stateDir := filepath.Join(t.TempDir(), "waypost-state")
			app := NewApp(strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
			err := app.RunWithStateDir(context.Background(), stateDir, tc.args)
			if err == nil {
				t.Fatal("Run() error = nil, want non-nil")
			}

			assertPathMissing(t, stateDir)
			assertPathMissing(t, filepath.Join(stateDir, databaseFilename))
			assertPathMissing(t, filepath.Join(stateDir, blobsDirName))
		})
	}
}

func TestPrepareReadCommandDistinguishesAbsentAndEmptySelectorFlags(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing selector",
			args:    []string{"read"},
			wantErr: "one of ID, --delivery, --message, or --latest is required",
		},
		{
			name:    "empty direct id",
			args:    []string{"read", "   "},
			wantErr: "ID must not be empty",
		},
		{
			name:    "mixed direct ids",
			args:    []string{"read", "dlv_123", "msg_123"},
			wantErr: "direct read IDs must all be delivery IDs or all be message IDs",
		},
		{
			name:    "direct and explicit selector",
			args:    []string{"read", "dlv_123", "--message", "msg_123"},
			wantErr: "ID, --delivery, --message, and --latest are mutually exclusive",
		},
		{
			name:    "latest missing for",
			args:    []string{"read", "--latest"},
			wantErr: "--latest requires at least one --for address",
		},
		{
			name:    "empty delivery",
			args:    []string{"read", "--delivery", ""},
			wantErr: "--delivery must not be empty",
		},
		{
			name:    "empty message",
			args:    []string{"read", "--message", "   "},
			wantErr: "--message must not be empty",
		},
		{
			name:    "empty latest for",
			args:    []string{"read", "--latest", "--for", ""},
			wantErr: "--for must not be empty",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			app := NewApp(strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
			_, err := app.prepareReadCommand(tc.args[1:])
			if err == nil {
				t.Fatal("prepareReadCommand() error = nil, want non-nil")
			}
			if err.Error() != tc.wantErr {
				t.Fatalf("prepareReadCommand() error = %q, want %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestHelpCLIPathsDoNotCreateRuntimeState(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		args         []string
		wantContains string
	}{
		{
			name:         "send help",
			args:         []string{"send", "--help"},
			wantContains: "Usage:\n  waypost send --to ADDRESS [--to ADDRESS ...] --body-file PATH [options] [--json | --ndjson | --yaml] [--full] [--notify]",
		},
		{
			name:         "forward help",
			args:         []string{"forward", "--help"},
			wantContains: "Usage:\n  waypost forward (--message ID | --delivery ID) --to ADDRESS [options] [--json | --ndjson | --yaml] [--full]",
		},
		{
			name:         "stale help",
			args:         []string{"stale", "--help"},
			wantContains: "Usage:\n  waypost stale --for ADDRESS [--for ADDRESS ...] --older-than DURATION [--json | --ndjson | --yaml]\n  waypost stale --for GROUP_ADDRESS --as PERSON --older-than DURATION [--json | --ndjson | --yaml]",
		},
		{
			name:         "recv help",
			args:         []string{"recv", "--help"},
			wantContains: "Usage:\n  waypost recv --for ADDRESS [--for ADDRESS ...] [--max COUNT] [--json | --ndjson | --yaml] [--full]",
		},
		{
			name:         "recv help requires unavailable MCP tool",
			args:         []string{"recv", "--help"},
			wantContains: "Use this CLI command only after confirming that the MCP waypost_recv tool is unavailable.",
		},
		{
			name:         "read help",
			args:         []string{"read", "--help"},
			wantContains: "Usage:\n  waypost read ID [ID ...] [--json | --ndjson | --yaml]",
		},
		{
			name:         "watch help",
			args:         []string{"watch", "--help"},
			wantContains: "Usage:\n  waypost watch --for ADDRESS [--for ADDRESS ...] [--state STATE] [--timeout DURATION] [--json | --ndjson | --yaml]",
		},
		{
			name:         "list help mentions delivery states",
			args:         []string{"list", "--help"},
			wantContains: "  --state STATE      Filter by delivery state (queued, leased/claimed, acked, dead_letter)",
		},
		{
			name:         "wait help",
			args:         []string{"wait", "--help"},
			wantContains: "Usage:\n  waypost wait --for ADDRESS [--for ADDRESS ...] [--timeout DURATION] [--json | --ndjson | --yaml] [--full]",
		},
		{
			name:         "undefer help",
			args:         []string{"undefer", "--help"},
			wantContains: "Usage:\n  waypost undefer --delivery ID",
		},
		{
			name:         "dead-letter help",
			args:         []string{"dead-letter", "--help"},
			wantContains: "Usage:\n  waypost dead-letter --delivery ID --lease-token TOKEN --reason TEXT [--json | --ndjson | --yaml]",
		},
		{
			name:         "group help",
			args:         []string{"group", "--help"},
			wantContains: "  list                List group addresses",
		},
		{
			name:         "group list help",
			args:         []string{"group", "list", "--help"},
			wantContains: "Usage:\n  waypost group list [--limit N] [--cursor CURSOR] [--json | --ndjson | --yaml]",
		},
		{
			name:         "group create help",
			args:         []string{"group", "create", "--help"},
			wantContains: "Usage:\n  waypost group create --group ADDRESS [--json | --ndjson | --yaml]",
		},
		{
			name:         "address help",
			args:         []string{"address", "--help"},
			wantContains: "Usage:\n  waypost address <subcommand> [options]",
		},
		{
			name:         "address inspect help",
			args:         []string{"address", "inspect", "--help"},
			wantContains: "Usage:\n  waypost address inspect --address ADDRESS [--json | --ndjson | --yaml]",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			stateDir := filepath.Join(t.TempDir(), "waypost-state")
			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}
			app := NewApp(strings.NewReader(""), stdout, stderr)

			err := app.RunWithStateDir(context.Background(), stateDir, tc.args)
			if !errors.Is(err, ErrHelpRequested) {
				t.Fatalf("RunWithStateDir() error = %v, want ErrHelpRequested", err)
			}
			if !strings.Contains(stdout.String(), tc.wantContains) {
				t.Fatalf("stdout = %q, want substring %q", stdout.String(), tc.wantContains)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}

			assertPathMissing(t, stateDir)
			assertPathMissing(t, filepath.Join(stateDir, databaseFilename))
			assertPathMissing(t, filepath.Join(stateDir, blobsDirName))
		})
	}
}

func TestGroupHelpDoesNotAdvertiseRootOwnedWebCommand(t *testing.T) {
	t.Parallel()

	stdout := &bytes.Buffer{}
	app := NewApp(strings.NewReader(""), stdout, &bytes.Buffer{})

	err := app.RunWithStateDir(context.Background(), filepath.Join(t.TempDir(), "waypost-state"), []string{"group", "--help"})
	if !errors.Is(err, ErrHelpRequested) {
		t.Fatalf("RunWithStateDir(group --help) error = %v, want ErrHelpRequested", err)
	}
	if strings.Contains(stdout.String(), "  web") {
		t.Fatalf("group help = %q, want no web subcommand", stdout.String())
	}
}

func assertPathMissing(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("path %q exists or returned unexpected error: %v", path, err)
	}
}

type recordingBlobTempFile struct {
	name string
	ops  *[]string
}

func (f *recordingBlobTempFile) Write(data []byte) (int, error) {
	*f.ops = append(*f.ops, "write:"+string(data))
	return len(data), nil
}

func (f *recordingBlobTempFile) Sync() error {
	*f.ops = append(*f.ops, "file-sync")
	return nil
}

func (f *recordingBlobTempFile) Close() error {
	*f.ops = append(*f.ops, "close")
	return nil
}

func (f *recordingBlobTempFile) Name() string {
	return f.name
}
