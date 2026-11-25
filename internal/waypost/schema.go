package waypost

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func deliveriesTableSQL(ifNotExists bool, tableName string) string {
	prefix := "CREATE TABLE "
	if ifNotExists {
		prefix = "CREATE TABLE IF NOT EXISTS "
	}
	return fmt.Sprintf(`%s%s (
  delivery_id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL,
  recipient_endpoint_id TEXT NOT NULL,
  state TEXT NOT NULL,
  visible_at TEXT NOT NULL,
  lease_token TEXT,
  lease_expires_at TEXT,
  acked_at TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error_code TEXT,
  last_error_text TEXT,
  FOREIGN KEY (message_id) REFERENCES messages(message_id),
  FOREIGN KEY (recipient_endpoint_id) REFERENCES endpoints(endpoint_id),
  CHECK (state IN ('queued', 'leased', 'acked', 'dead_letter')),
  CHECK (attempt_count >= 0),
  CHECK (
    (state = 'leased' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR
    (state != 'leased' AND lease_token IS NULL AND lease_expires_at IS NULL)
  ),
  CHECK (
    (state = 'acked' AND acked_at IS NOT NULL)
    OR
    (state != 'acked' AND acked_at IS NULL)
  )
)`, prefix, tableName)
}

var deliveriesIndexesSQL = []string{
	`CREATE INDEX IF NOT EXISTS idx_deliveries_recipient_state_visible ON deliveries (recipient_endpoint_id, state, visible_at)`,
	`CREATE INDEX IF NOT EXISTS idx_deliveries_message_id ON deliveries (message_id)`,
}

var schemaSQL = `
CREATE TABLE IF NOT EXISTS endpoints (
  endpoint_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS endpoint_addresses (
  address TEXT PRIMARY KEY,
  endpoint_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (endpoint_id) REFERENCES endpoints(endpoint_id)
);

CREATE TABLE IF NOT EXISTS persons (
  person_id TEXT PRIMARY KEY,
  person TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS groups (
  group_id TEXT PRIMARY KEY,
  address TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS group_memberships (
  membership_id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL,
  person_id TEXT NOT NULL,
  joined_at TEXT NOT NULL,
  left_at TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (group_id) REFERENCES groups(group_id),
  FOREIGN KEY (person_id) REFERENCES persons(person_id),
  CHECK (left_at IS NULL OR left_at >= joined_at)
);

CREATE TABLE IF NOT EXISTS messages (
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
  forwarded_from_address TEXT,
  reply_to_message_id TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (sender_endpoint_id) REFERENCES endpoints(endpoint_id),
  FOREIGN KEY (forwarded_message_id) REFERENCES messages(message_id),
  FOREIGN KEY (reply_to_message_id) REFERENCES messages(message_id)
);

` + deliveriesTableSQL(true, "deliveries") + `;

CREATE TABLE IF NOT EXISTS group_messages (
  message_id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  eligible_count INTEGER NOT NULL,
  FOREIGN KEY (message_id) REFERENCES messages(message_id),
  FOREIGN KEY (group_id) REFERENCES groups(group_id)
);

CREATE TABLE IF NOT EXISTS group_message_eligibility (
  message_id TEXT NOT NULL,
  person_id TEXT NOT NULL,
  membership_id TEXT NOT NULL,
  eligible_at TEXT NOT NULL,
  PRIMARY KEY (message_id, person_id),
  FOREIGN KEY (message_id) REFERENCES group_messages(message_id),
  FOREIGN KEY (person_id) REFERENCES persons(person_id),
  FOREIGN KEY (membership_id) REFERENCES group_memberships(membership_id)
);

CREATE TABLE IF NOT EXISTS group_reads (
  message_id TEXT NOT NULL,
  person_id TEXT NOT NULL,
  first_read_at TEXT NOT NULL,
  PRIMARY KEY (message_id, person_id),
  FOREIGN KEY (message_id) REFERENCES group_messages(message_id),
  FOREIGN KEY (person_id) REFERENCES persons(person_id)
);

CREATE TABLE IF NOT EXISTS group_notification_subscribers (
  subscriber_id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL,
  notify_address TEXT NOT NULL,
  person TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  removed_at TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (group_id) REFERENCES groups(group_id),
  CHECK (removed_at IS NULL OR removed_at >= created_at)
);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  event_type TEXT NOT NULL,
  endpoint_id TEXT,
  message_id TEXT,
  delivery_id TEXT,
  detail_json TEXT NOT NULL DEFAULT '{}',
  FOREIGN KEY (endpoint_id) REFERENCES endpoints(endpoint_id),
  FOREIGN KEY (message_id) REFERENCES messages(message_id),
  FOREIGN KEY (delivery_id) REFERENCES deliveries(delivery_id)
);

CREATE INDEX IF NOT EXISTS idx_endpoint_addresses_endpoint_id
  ON endpoint_addresses (endpoint_id);

CREATE INDEX IF NOT EXISTS idx_groups_created_address
  ON groups (created_at, address);

CREATE INDEX IF NOT EXISTS idx_group_memberships_group_joined
  ON group_memberships (group_id, joined_at, membership_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_group_memberships_active
  ON group_memberships (group_id, person_id)
  WHERE left_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_group_memberships_person_joined
  ON group_memberships (person_id, joined_at, membership_id);

` + strings.Join(deliveriesIndexesSQL, ";\n\n") + `;

CREATE INDEX IF NOT EXISTS idx_group_messages_group_created
  ON group_messages (group_id, created_at, message_id);

CREATE INDEX IF NOT EXISTS idx_group_message_eligibility_person_message
  ON group_message_eligibility (person_id, message_id);

CREATE INDEX IF NOT EXISTS idx_group_reads_person_message
  ON group_reads (person_id, message_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_group_notification_subscribers_active
  ON group_notification_subscribers (group_id, notify_address)
  WHERE removed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_group_notification_subscribers_group_created
  ON group_notification_subscribers (group_id, created_at, subscriber_id);

CREATE INDEX IF NOT EXISTS idx_events_message_delivery
  ON events (message_id, delivery_id);
`

const currentSchemaVersion = 2

func schemaUserVersion(ctx context.Context, db *sql.DB) (int, error) {
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return 0, fmt.Errorf("read schema user_version: %w", err)
	}
	return version, nil
}

func ensureWALJournalMode(ctx context.Context, conn *sql.Conn) error {
	for attempt := 0; attempt < 25; attempt++ {
		var journalMode string
		err := conn.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journalMode)
		if err == nil {
			if strings.EqualFold(strings.TrimSpace(journalMode), "wal") {
				return nil
			}
			var updatedMode string
			err = conn.QueryRowContext(ctx, `PRAGMA journal_mode = WAL;`).Scan(&updatedMode)
			if err == nil {
				if strings.EqualFold(strings.TrimSpace(updatedMode), "wal") {
					return nil
				}
				return fmt.Errorf("enable WAL journal mode: sqlite remained in %q mode", strings.TrimSpace(updatedMode))
			}
		}
		if !isSQLiteBusy(err) || attempt == 24 {
			return fmt.Errorf("enable WAL journal mode: %w", err)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("enable WAL journal mode: %w", ctx.Err())
		case <-timer.C:
		}
	}
	return nil
}

func initSchema(ctx context.Context, db *sql.DB) (retErr error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire schema migration connection: %w", err)
	}
	defer conn.Close()

	var version int
	if err := conn.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read schema user_version: %w", err)
	}
	if version < 0 || version > currentSchemaVersion {
		return fmt.Errorf("unsupported database schema version %d (supported 0..%d)", version, currentSchemaVersion)
	}
	if version == currentSchemaVersion {
		return ensureWALJournalMode(ctx, conn)
	}

	if err := ensureWALJournalMode(ctx, conn); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = OFF;`); err != nil {
		return fmt.Errorf("disable foreign keys for schema migration: %w", err)
	}
	defer func() {
		if _, fkErr := conn.ExecContext(context.Background(), `PRAGMA foreign_keys = ON;`); fkErr != nil && retErr == nil {
			retErr = fmt.Errorf("re-enable foreign keys after schema migration: %w", fkErr)
		}
	}()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema migration transaction: %w", err)
	}
	defer tx.Rollback()

	var currentVersion int
	if err := tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&currentVersion); err != nil {
		return fmt.Errorf("re-read schema user_version: %w", err)
	}
	if currentVersion < 0 || currentVersion > currentSchemaVersion {
		return fmt.Errorf("unsupported database schema version %d (supported 0..%d)", currentVersion, currentSchemaVersion)
	}
	if currentVersion == currentSchemaVersion {
		if err := tx.Rollback(); err != nil {
			return fmt.Errorf("rollback completed schema check: %w", err)
		}
		return nil
	}

	if err := migrateLegacyGroupMessageSchema(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("initialize schema: %w", err)
	}
	if err := migrateForwardedMessageSchema(ctx, tx); err != nil {
		return err
	}
	if err := ensureDeliveriesCheckConstraints(ctx, tx); err != nil {
		return err
	}
	fkRows, err := tx.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return fmt.Errorf("verify schema foreign keys: %w", err)
	}
	var (
		tableName string
		rowID     sql.NullInt64
		parent    string
		fkID      int
		scanErr   error
	)
	hasFKViolation := fkRows.Next()
	if hasFKViolation {
		scanErr = fkRows.Scan(&tableName, &rowID, &parent, &fkID)
	}
	fkErr := fkRows.Err()
	_ = fkRows.Close()
	if scanErr != nil {
		return fmt.Errorf("scan foreign_key_check row: %w", scanErr)
	}
	if fkErr != nil {
		return fmt.Errorf("scan foreign_key_check: %w", fkErr)
	}
	if hasFKViolation {
		return fmt.Errorf("schema migration detected foreign key violation in table %q (rowid=%d, parent=%q, fkid=%d)", tableName, rowID.Int64, parent, fkID)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", currentSchemaVersion)); err != nil {
		return fmt.Errorf("update schema user_version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema migration: %w", err)
	}
	return nil
}
