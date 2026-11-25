package waypost

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type schemaExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func migrateLegacyGroupMessageSchema(ctx context.Context, db schemaExecutor) error {
	hasLegacyKey, err := tableHasColumn(ctx, db, "group_messages", "group_message_id")
	if err != nil {
		return fmt.Errorf("inspect legacy group_messages schema: %w", err)
	}
	if !hasLegacyKey {
		return nil
	}

	// Legacy group-message data came from the pre-v1 experimental schema and is
	// intentionally discarded instead of migrated.
	statements := []string{
		`DROP INDEX IF EXISTS idx_group_reads_person_message`,
		`DROP INDEX IF EXISTS idx_group_message_eligibility_person_message`,
		`DROP INDEX IF EXISTS idx_group_messages_group_created`,
		`DROP TABLE IF EXISTS group_reads`,
		`DROP TABLE IF EXISTS group_message_eligibility`,
		`DROP TABLE IF EXISTS group_messages`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply legacy group schema migration: %w", err)
		}
	}
	return nil
}

func migrateForwardedMessageSchema(ctx context.Context, db schemaExecutor) error {
	hasMessageIDColumn, err := tableHasColumn(ctx, db, "messages", "forwarded_message_id")
	if err != nil {
		return fmt.Errorf("inspect messages schema for forwarded_message_id: %w", err)
	}
	if !hasMessageIDColumn {
		if _, err := db.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN forwarded_message_id TEXT REFERENCES messages(message_id)`); err != nil {
			return fmt.Errorf("add messages.forwarded_message_id column: %w", err)
		}
	}
	hasFromAddressColumn, err := tableHasColumn(ctx, db, "messages", "forwarded_from_address")
	if err != nil {
		return fmt.Errorf("inspect messages schema for forwarded_from_address: %w", err)
	}
	if !hasFromAddressColumn {
		if _, err := db.ExecContext(ctx, `ALTER TABLE messages ADD COLUMN forwarded_from_address TEXT`); err != nil {
			return fmt.Errorf("add messages.forwarded_from_address column: %w", err)
		}
	}
	if err := backfillForwardedFromAddress(ctx, db); err != nil {
		return err
	}
	return nil
}

func backfillForwardedFromAddress(ctx context.Context, db schemaExecutor) error {
	if _, err := db.ExecContext(ctx, `
UPDATE messages AS forwarded
SET forwarded_from_address = (
  SELECT sender_ea.address
  FROM messages AS source
  JOIN endpoint_addresses AS sender_ea ON sender_ea.endpoint_id = source.sender_endpoint_id
  WHERE source.message_id = forwarded.forwarded_message_id
  ORDER BY sender_ea.created_at ASC, sender_ea.address ASC
  LIMIT 1
)
WHERE forwarded.forwarded_message_id IS NOT NULL
  AND (forwarded.forwarded_from_address IS NULL OR trim(forwarded.forwarded_from_address) = '')
  AND EXISTS (
    SELECT 1
    FROM messages AS source
    JOIN endpoint_addresses AS sender_ea ON sender_ea.endpoint_id = source.sender_endpoint_id
    WHERE source.message_id = forwarded.forwarded_message_id
  )
`); err != nil {
		return fmt.Errorf("backfill messages.forwarded_from_address: %w", err)
	}
	return nil
}

func ensureDeliveriesCheckConstraints(ctx context.Context, db schemaExecutor) error {
	var createSQL sql.NullString
	err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'deliveries'`).Scan(&createSQL)
	if err != nil {
		return fmt.Errorf("inspect deliveries schema: %w", err)
	}
	if !createSQL.Valid {
		return errors.New("inspect deliveries schema: missing CREATE TABLE statement")
	}
	if strings.Contains(createSQL.String, "CHECK (state IN") {
		return nil
	}

	statements := []string{
		deliveriesTableSQL(false, "deliveries_v2"),
		`INSERT INTO deliveries_v2 (
  delivery_id, message_id, recipient_endpoint_id, state, visible_at,
  lease_token, lease_expires_at, acked_at, attempt_count, last_error_code, last_error_text
)
SELECT
  delivery_id, message_id, recipient_endpoint_id, state, visible_at,
  lease_token, lease_expires_at, acked_at, attempt_count, last_error_code, last_error_text
FROM deliveries`,
		`DROP TABLE deliveries`,
		`ALTER TABLE deliveries_v2 RENAME TO deliveries`,
	}
	statements = append(statements, deliveriesIndexesSQL...)
	for _, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("upgrade deliveries constraints: %w", err)
		}
	}
	return nil
}

func tableHasColumn(ctx context.Context, db schemaExecutor, tableName, columnName string) (bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var columnType string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return false, err
		}
		if name == columnName {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}
