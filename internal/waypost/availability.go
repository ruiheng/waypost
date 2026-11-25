package waypost

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const claimableDeliveryFilter = `(
    (d.state = 'queued' AND d.visible_at <= ?)
    OR
    (d.state = 'leased' AND d.lease_expires_at IS NOT NULL AND d.lease_expires_at <= ?)
  )`

const claimableDeliveryEligibleAtExpr = `CASE
    WHEN d.state = 'queued' THEN d.visible_at
    WHEN d.state = 'leased' THEN d.lease_expires_at
  END`

type rowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type rowsQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type resolvedRecipient struct {
	Address    string
	EndpointID string
}

type personalAvailabilityScope struct {
	addressByEndpointID  map[string]string
	recipientEndpointIDs []string
}

type personalDeliveryRecord struct {
	DeliveryID           string
	MessageID            string
	ForwardedMessageID   sql.NullString
	ForwardedFromAddress sql.NullString
	RecipientAddress     string
	RecipientEndpointID  string
	SenderEndpointID     sql.NullString
	SenderAddress        sql.NullString
	State                string
	VisibleAt            string
	AckedAt              sql.NullString
	AttemptCount         int
	MessageCreatedAt     string
	Subject              string
	ContentType          string
	SchemaVersion        string
	BodyBlobRef          string
	BodySize             int64
	BodySHA256           string
}

type groupAvailabilityScope struct {
	viewer groupViewerState
}

type groupViewerState struct {
	Group            GroupRecord
	Person           string
	PersonID         string
	VisibilityCutoff *string
}

type groupMessageRecord struct {
	MessageID            string
	ForwardedMessageID   sql.NullString
	ForwardedFromAddress sql.NullString
	SenderEndpointID     sql.NullString
	SenderAddress        sql.NullString
	MessageCreatedAt     string
	Subject              string
	ContentType          string
	SchemaVersion        string
	BodyBlobRef          string
	BodySize             int64
	BodySHA256           string
	FirstReadAt          sql.NullString
	ViewerEligible       int
	ReadCount            int
	EligibleCount        int
}

func (s *Store) groupUnreadSummary(ctx context.Context, querier rowQuerier, scope groupAvailabilityScope) (string, int, error) {
	viewer := scope.viewer
	if viewer.PersonID == "" {
		return "", 0, nil
	}
	query := `
SELECT MIN(gm.created_at), COUNT(*)
FROM group_messages AS gm
LEFT JOIN group_reads AS gr
  ON gr.message_id = gm.message_id
 AND gr.person_id = ?
WHERE gm.group_id = ?
  AND gr.first_read_at IS NULL
`
	args := []any{viewer.PersonID, viewer.Group.GroupID}
	if viewer.VisibilityCutoff != nil {
		query += "  AND gm.created_at <= ?\n"
		args = append(args, *viewer.VisibilityCutoff)
	}
	var oldest sql.NullString
	var count int
	if err := querier.QueryRowContext(ctx, query, args...).Scan(&oldest, &count); err != nil {
		return "", 0, fmt.Errorf("summarize unread group messages for %q in %q: %w", viewer.Person, viewer.Group.Address, err)
	}
	if !oldest.Valid {
		return "", 0, nil
	}
	return oldest.String, count, nil
}

func (s *Store) resolvePersonal(ctx context.Context, querier rowQuerier, addresses []string) (personalAvailabilityScope, error) {
	recipients := make([]resolvedRecipient, 0, len(addresses))
	seenEndpointIDs := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		group, found, err := lookupGroupRecord(ctx, querier, address)
		if err != nil {
			return personalAvailabilityScope{}, fmt.Errorf("resolve recipient address %q: %w", address, err)
		}
		if found {
			return personalAvailabilityScope{}, fmt.Errorf("recipient address %q is reserved by group %q: %w", address, group.GroupID, ErrAddressReservedByGroup)
		}

		endpointID, found, err := s.lookupEndpointID(ctx, querier, address)
		if err != nil {
			return personalAvailabilityScope{}, fmt.Errorf("resolve recipient address %q: %w", address, err)
		}
		if !found {
			continue
		}
		if _, exists := seenEndpointIDs[endpointID]; exists {
			continue
		}
		seenEndpointIDs[endpointID] = struct{}{}
		recipients = append(recipients, resolvedRecipient{
			Address:    address,
			EndpointID: endpointID,
		})
	}

	scope := personalAvailabilityScope{
		addressByEndpointID:  make(map[string]string, len(recipients)),
		recipientEndpointIDs: make([]string, 0, len(recipients)),
	}
	for _, recipient := range recipients {
		scope.addressByEndpointID[recipient.EndpointID] = recipient.Address
		scope.recipientEndpointIDs = append(scope.recipientEndpointIDs, recipient.EndpointID)
	}
	return scope, nil
}

func (scope personalAvailabilityScope) empty() bool {
	return len(scope.recipientEndpointIDs) == 0
}

// RemainingByState returns the unfinished-delivery snapshot for exactly the
// personal address scope supplied by a receive call. It deliberately reads
// only the deliveries index: bodies and message rows are irrelevant here.
func (s *Store) RemainingByState(ctx context.Context, addresses, excludedDeliveryIDs []string) (map[string]int, error) {
	scope, err := s.resolvePersonal(ctx, s.readDB, addresses)
	if err != nil {
		return nil, err
	}
	return s.remainingByStateForScope(ctx, scope, excludedDeliveryIDs)
}

func (s *Store) remainingByStateForScope(ctx context.Context, scope personalAvailabilityScope, excludedDeliveryIDs []string) (map[string]int, error) {
	if scope.empty() {
		return nil, nil
	}

	endpointPlaceholders := strings.TrimSuffix(strings.Repeat("?,", len(scope.recipientEndpointIDs)), ",")
	args := make([]any, 0, len(scope.recipientEndpointIDs)*2+len(excludedDeliveryIDs))
	query := fmt.Sprintf(`
SELECT
  COUNT(*) FILTER (WHERE d.state = 'queued'),
  COUNT(*) FILTER (WHERE d.state = 'leased'),
  COUNT(*) FILTER (WHERE d.state = 'dead_letter')
FROM deliveries AS d
WHERE d.recipient_endpoint_id IN (%s)
  AND d.state IN ('queued', 'leased', 'dead_letter')
`, endpointPlaceholders)

	if len(excludedDeliveryIDs) > 0 {
		exclusionPlaceholders := strings.TrimSuffix(strings.Repeat("?,", len(excludedDeliveryIDs)), ",")
		// Keep the main 100k-row count covered by the recipient/state index.
		// delivery_id is not part of that index, so a NOT IN predicate here
		// forces a table lookup for every candidate. The bounded exclusion set is
		// instead resolved by delivery ID in the same SQLite snapshot and deducted
		// from the aggregate counts.
		query = fmt.Sprintf(`
WITH excluded AS (
  SELECT d.state
  FROM deliveries AS d
  WHERE d.delivery_id IN (%s)
    AND d.recipient_endpoint_id IN (%s)
    AND d.state IN ('queued', 'leased', 'dead_letter')
)
SELECT
  COUNT(*) FILTER (WHERE d.state = 'queued') - (SELECT COUNT(*) FROM excluded WHERE state = 'queued'),
  COUNT(*) FILTER (WHERE d.state = 'leased') - (SELECT COUNT(*) FROM excluded WHERE state = 'leased'),
  COUNT(*) FILTER (WHERE d.state = 'dead_letter') - (SELECT COUNT(*) FROM excluded WHERE state = 'dead_letter')
FROM deliveries AS d
WHERE d.recipient_endpoint_id IN (%s)
  AND d.state IN ('queued', 'leased', 'dead_letter')
`, exclusionPlaceholders, endpointPlaceholders, endpointPlaceholders)
		for _, deliveryID := range excludedDeliveryIDs {
			args = append(args, deliveryID)
		}
		for _, endpointID := range scope.recipientEndpointIDs {
			args = append(args, endpointID)
		}
		for _, endpointID := range scope.recipientEndpointIDs {
			args = append(args, endpointID)
		}
	} else {
		for _, endpointID := range scope.recipientEndpointIDs {
			args = append(args, endpointID)
		}
	}

	// GROUP BY state would create a temporary B-tree for a multi-address
	// receive scope, so count each state from the recipient-first covering index.
	var queued, leased, deadLetter int
	err := s.readDB.QueryRowContext(ctx, query, args...).Scan(&queued, &leased, &deadLetter)
	if err != nil {
		return nil, fmt.Errorf("count remaining deliveries: %w", err)
	}

	remaining := make(map[string]int)
	for state, count := range map[string]int{
		"queued":      queued,
		"leased":      leased,
		"dead_letter": deadLetter,
	} {
		if count > 0 {
			remaining[state] = count
		}
	}
	if len(remaining) == 0 {
		return nil, nil
	}
	return remaining, nil
}

func (s *Store) claimablePersonalDelivery(ctx context.Context, querier rowQuerier, scope personalAvailabilityScope, nowText string) (personalDeliveryRecord, error) {
	if scope.empty() {
		return personalDeliveryRecord{}, sql.ErrNoRows
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(scope.recipientEndpointIDs)), ",")
	args := make([]any, 0, len(scope.recipientEndpointIDs)+2)
	for _, recipientEndpointID := range scope.recipientEndpointIDs {
		args = append(args, recipientEndpointID)
	}
	args = append(args, nowText, nowText)

	var record personalDeliveryRecord
	err := querier.QueryRowContext(ctx, fmt.Sprintf(`
SELECT
	  d.delivery_id,
	  d.message_id,
	  m.forwarded_message_id,
	  m.forwarded_from_address,
	  d.recipient_endpoint_id,
	  m.sender_endpoint_id,
	  (
	    SELECT sender_ea.address
	    FROM endpoint_addresses AS sender_ea
	    WHERE sender_ea.endpoint_id = m.sender_endpoint_id
	    ORDER BY sender_ea.created_at ASC, sender_ea.address ASC
	    LIMIT 1
	  ) AS sender_address,
  d.state,
  d.visible_at,
  d.attempt_count,
  m.created_at,
  m.subject,
  m.content_type,
  m.schema_version,
  m.body_blob_ref,
  m.body_size,
  m.body_sha256
FROM deliveries AS d
JOIN messages AS m ON m.message_id = d.message_id
WHERE d.recipient_endpoint_id IN (%s)
  AND %s
ORDER BY d.visible_at ASC, m.created_at ASC, d.delivery_id ASC
LIMIT 1
`, placeholders, claimableDeliveryFilter), args...).Scan(
		&record.DeliveryID,
		&record.MessageID,
		&record.ForwardedMessageID,
		&record.ForwardedFromAddress,
		&record.RecipientEndpointID,
		&record.SenderEndpointID,
		&record.SenderAddress,
		&record.State,
		&record.VisibleAt,
		&record.AttemptCount,
		&record.MessageCreatedAt,
		&record.Subject,
		&record.ContentType,
		&record.SchemaVersion,
		&record.BodyBlobRef,
		&record.BodySize,
		&record.BodySHA256,
	)
	if err != nil {
		return personalDeliveryRecord{}, err
	}
	record.RecipientAddress = scope.addressByEndpointID[record.RecipientEndpointID]
	return record, nil
}

type personalDeliveryListMode int

const (
	personalDeliveryListQueued personalDeliveryListMode = iota
	personalDeliveryListClaimable
)

type personalDeliveryRow struct {
	delivery     ListedDelivery
	leaseToken   string
	attemptCount int
}

func (s *Store) listPersonalDeliveries(ctx context.Context, querier rowsQuerier, scope personalAvailabilityScope, state, nowText string) ([]ListedDelivery, error) {
	return s.listPersonalDeliveriesPage(ctx, querier, scope, state, nowText, "", nil, DefaultPageSize, false)
}

func (s *Store) listWatchDeliveriesPage(ctx context.Context, querier rowsQuerier, scope personalAvailabilityScope, state, nowText string, cursorKeys []string, limit int) ([]personalDeliveryRow, error) {
	mode := personalDeliveryListQueued
	if state == "" {
		mode = personalDeliveryListClaimable
	}
	return s.listPersonalDeliveryRowsPage(ctx, querier, scope, state, nowText, "", cursorKeys, limit, false, mode)
}

func (s *Store) listClaimablePersonalDeliveriesPage(ctx context.Context, querier rowsQuerier, scope personalAvailabilityScope, nowText string, cursorKeys []string, limit int) ([]ListedDelivery, error) {
	return s.listPersonalDeliveriesPageWithMode(ctx, querier, scope, "", nowText, "", cursorKeys, limit, false, personalDeliveryListClaimable)
}

func (s *Store) listPersonalDeliveriesPage(ctx context.Context, querier rowsQuerier, scope personalAvailabilityScope, state, nowText, senderEndpointID string, cursorKeys []string, limit int, orderByCreated bool) ([]ListedDelivery, error) {
	return s.listPersonalDeliveriesPageWithMode(ctx, querier, scope, state, nowText, senderEndpointID, cursorKeys, limit, orderByCreated, personalDeliveryListQueued)
}

func (s *Store) listPersonalDeliveriesPageWithMode(ctx context.Context, querier rowsQuerier, scope personalAvailabilityScope, state, nowText, senderEndpointID string, cursorKeys []string, limit int, orderByCreated bool, mode personalDeliveryListMode) ([]ListedDelivery, error) {
	rows, err := s.listPersonalDeliveryRowsPage(ctx, querier, scope, state, nowText, senderEndpointID, cursorKeys, limit, orderByCreated, mode)
	if err != nil {
		return nil, err
	}
	deliveries := make([]ListedDelivery, len(rows))
	for i, row := range rows {
		deliveries[i] = row.delivery
	}
	return deliveries, nil
}

func (s *Store) listPersonalDeliveryRowsPage(ctx context.Context, querier rowsQuerier, scope personalAvailabilityScope, state, nowText, senderEndpointID string, cursorKeys []string, limit int, orderByCreated bool, mode personalDeliveryListMode) ([]personalDeliveryRow, error) {
	if scope.empty() {
		return []personalDeliveryRow{}, nil
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(scope.recipientEndpointIDs)), ",")
	args := make([]any, 0, len(scope.recipientEndpointIDs)+9)
	for _, recipientEndpointID := range scope.recipientEndpointIDs {
		args = append(args, recipientEndpointID)
	}

	query := fmt.Sprintf(`
SELECT
	  d.delivery_id,
	  d.message_id,
	  m.forwarded_message_id,
	  m.forwarded_from_address,
	  d.recipient_endpoint_id,
	  m.sender_endpoint_id,
	  (
	    SELECT sender_ea.address
	    FROM endpoint_addresses AS sender_ea
	    WHERE sender_ea.endpoint_id = m.sender_endpoint_id
	    ORDER BY sender_ea.created_at ASC, sender_ea.address ASC
	    LIMIT 1
	  ) AS sender_address,
  d.state,
  d.visible_at,
  d.acked_at,
  d.lease_token,
  d.attempt_count,
  m.created_at,
  m.subject,
  m.content_type,
  m.schema_version,
  m.body_blob_ref,
  m.body_size,
  m.body_sha256
FROM deliveries AS d
JOIN messages AS m ON m.message_id = d.message_id
WHERE d.recipient_endpoint_id IN (%s)
`, placeholders)
	if state == "" {
		if mode == personalDeliveryListClaimable {
			query += "\n  AND " + claimableDeliveryFilter + "\n"
			args = append(args, nowText, nowText)
		} else {
			query += `
  AND d.state = 'queued'
  AND d.visible_at <= ?
`
			args = append(args, nowText)
		}
	} else {
		query += `
  AND d.state = ?
`
		args = append(args, state)
	}
	if senderEndpointID != "" {
		query += `
  AND m.sender_endpoint_id = ?
`
		args = append(args, senderEndpointID)
	}
	if len(cursorKeys) > 0 {
		if orderByCreated {
			query += `
  AND (
    m.created_at > ?
    OR (m.created_at = ? AND d.delivery_id > ?)
  )
`
			args = append(args, cursorKeys[0], cursorKeys[0], cursorKeys[1])
		} else {
			query += `
  AND (
    d.visible_at > ?
    OR (d.visible_at = ? AND m.created_at > ?)
    OR (d.visible_at = ? AND m.created_at = ? AND d.delivery_id > ?)
  )
`
			args = append(args, cursorKeys[0], cursorKeys[0], cursorKeys[1], cursorKeys[0], cursorKeys[1], cursorKeys[2])
		}
	}
	if orderByCreated {
		query += `
ORDER BY m.created_at ASC, d.delivery_id ASC
LIMIT ?
`
	} else {
		query += `
ORDER BY d.visible_at ASC, m.created_at ASC, d.delivery_id ASC
LIMIT ?
`
	}
	args = append(args, limit)

	rows, err := querier.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query deliveries: %w", err)
	}
	defer rows.Close()

	deliveries := make([]personalDeliveryRow, 0)
	for rows.Next() {
		var row personalDeliveryRow
		var delivery ListedDelivery
		var ackedAt sql.NullString
		var leaseToken sql.NullString
		var senderID sql.NullString
		var senderAddress sql.NullString
		var forwardedMessageID sql.NullString
		var forwardedFromAddress sql.NullString
		if err := rows.Scan(
			&delivery.DeliveryID,
			&delivery.MessageID,
			&forwardedMessageID,
			&forwardedFromAddress,
			&delivery.RecipientEndpointID,
			&senderID,
			&senderAddress,
			&delivery.State,
			&delivery.VisibleAt,
			&ackedAt,
			&leaseToken,
			&row.attemptCount,
			&delivery.MessageCreatedAt,
			&delivery.Subject,
			&delivery.ContentType,
			&delivery.SchemaVersion,
			&delivery.BodyBlobRef,
			&delivery.BodySize,
			&delivery.BodySHA256,
		); err != nil {
			return nil, fmt.Errorf("scan delivery row: %w", err)
		}
		delivery.RecipientAddress = scope.addressByEndpointID[delivery.RecipientEndpointID]
		if senderID.Valid {
			delivery.SenderEndpointID = &senderID.String
		}
		if senderAddress.Valid {
			delivery.SenderAddress = &senderAddress.String
		}
		if forwardedMessageID.Valid {
			delivery.ForwardedMessageID = &forwardedMessageID.String
		}
		if forwardedFromAddress.Valid {
			delivery.ForwardedFromAddress = &forwardedFromAddress.String
		}
		if ackedAt.Valid {
			delivery.AckedAt = &ackedAt.String
		}
		if leaseToken.Valid {
			row.leaseToken = leaseToken.String
		}
		row.delivery = delivery
		deliveries = append(deliveries, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery rows: %w", err)
	}

	return deliveries, nil
}

func (s *Store) listClaimablePersonalAddresses(ctx context.Context, querier rowsQuerier, scope personalAvailabilityScope, nowText string) ([]ClaimableAddress, error) {
	if scope.empty() {
		return []ClaimableAddress{}, nil
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(scope.recipientEndpointIDs)), ",")
	args := make([]any, 0, len(scope.recipientEndpointIDs)+2)
	for _, recipientEndpointID := range scope.recipientEndpointIDs {
		args = append(args, recipientEndpointID)
	}
	args = append(args, nowText, nowText)

	rows, err := querier.QueryContext(ctx, fmt.Sprintf(`
SELECT
  d.recipient_endpoint_id,
  MIN(%s) AS oldest_eligible_at,
  COUNT(*) AS claimable_count
FROM deliveries AS d
WHERE d.recipient_endpoint_id IN (%s)
  AND %s
GROUP BY d.recipient_endpoint_id
ORDER BY oldest_eligible_at ASC, d.recipient_endpoint_id ASC
`, claimableDeliveryEligibleAtExpr, placeholders, claimableDeliveryFilter), args...)
	if err != nil {
		return nil, fmt.Errorf("query claimable addresses: %w", err)
	}
	defer rows.Close()

	claimable := make([]ClaimableAddress, 0)
	for rows.Next() {
		var endpointID string
		var oldestEligibleAt string
		var count int
		if err := rows.Scan(&endpointID, &oldestEligibleAt, &count); err != nil {
			return nil, fmt.Errorf("scan claimable address row: %w", err)
		}
		claimable = append(claimable, ClaimableAddress{
			Address:          scope.addressByEndpointID[endpointID],
			OldestEligibleAt: oldestEligibleAt,
			ClaimableCount:   count,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimable address rows: %w", err)
	}

	return claimable, nil
}

func (s *Store) queryPersonalStaleAddresses(ctx context.Context, querier rowsQuerier, scope personalAvailabilityScope, nowText, staleBeforeText string) ([]StaleAddress, error) {
	if scope.empty() {
		return []StaleAddress{}, nil
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(scope.recipientEndpointIDs)), ",")
	args := make([]any, 0, len(scope.recipientEndpointIDs)+3)
	for _, recipientEndpointID := range scope.recipientEndpointIDs {
		args = append(args, recipientEndpointID)
	}
	args = append(args, nowText, nowText, staleBeforeText)

	rows, err := querier.QueryContext(ctx, fmt.Sprintf(`
SELECT
  d.recipient_endpoint_id,
  MIN(%s) AS oldest_eligible_at,
  COUNT(*) AS claimable_count
FROM deliveries AS d
WHERE d.recipient_endpoint_id IN (%s)
  AND %s
GROUP BY d.recipient_endpoint_id
HAVING MIN(%s) <= ?
ORDER BY oldest_eligible_at ASC, d.recipient_endpoint_id ASC
`, claimableDeliveryEligibleAtExpr, placeholders, claimableDeliveryFilter, claimableDeliveryEligibleAtExpr), args...)
	if err != nil {
		return nil, fmt.Errorf("query stale addresses: %w", err)
	}
	defer rows.Close()

	stale := make([]StaleAddress, 0)
	for rows.Next() {
		var endpointID string
		var oldestEligibleAt string
		var claimableCount int
		if err := rows.Scan(&endpointID, &oldestEligibleAt, &claimableCount); err != nil {
			return nil, fmt.Errorf("scan stale address row: %w", err)
		}
		stale = append(stale, StaleAddress{
			Address:          scope.addressByEndpointID[endpointID],
			OldestEligibleAt: oldestEligibleAt,
			ClaimableCount:   claimableCount,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stale address rows: %w", err)
	}

	return stale, nil
}

func (s *Store) resolveGroup(ctx context.Context, querier rowQuerier, groupAddress, person string) (groupAvailabilityScope, error) {
	rawGroupAddress := groupAddress
	groupAddress, err := NormalizeGroupAddress(rawGroupAddress)
	if err != nil {
		if strings.TrimSpace(rawGroupAddress) == "" {
			return groupAvailabilityScope{}, invalidArgumentError(errors.New("group address is required"))
		}
		return groupAvailabilityScope{}, err
	}
	person = strings.TrimSpace(person)
	if person == "" {
		return groupAvailabilityScope{}, invalidArgumentError(errors.New("person is required"))
	}

	group, found, err := lookupGroupRecord(ctx, querier, groupAddress)
	if err != nil {
		return groupAvailabilityScope{}, fmt.Errorf("load group %q: %w", groupAddress, err)
	}
	if !found {
		return groupAvailabilityScope{}, fmt.Errorf("group %q: %w", groupAddress, ErrGroupNotFound)
	}

	membership, found, err := lookupLatestGroupMembershipByPerson(ctx, querier, group.GroupID, person)
	if err != nil {
		return groupAvailabilityScope{}, fmt.Errorf("load group membership for %q in %q: %w", person, groupAddress, err)
	}
	if !found {
		return groupAvailabilityScope{
			viewer: groupViewerState{
				Group:  group,
				Person: person,
			},
		}, nil
	}

	viewer := groupViewerState{
		Group:    group,
		Person:   membership.Person,
		PersonID: membership.PersonID,
	}
	if !membership.Active {
		viewer.VisibilityCutoff = membership.LeftAt
	}
	return groupAvailabilityScope{viewer: viewer}, nil
}

func (s *Store) listGroupMessages(ctx context.Context, querier rowsQuerier, scope groupAvailabilityScope, unreadOnly bool, limit int) ([]groupMessageRecord, error) {
	if limit <= 0 {
		limit = DefaultPageSize
	}
	return s.listGroupMessagesPage(ctx, querier, scope, unreadOnly, "", nil, limit)
}

func (s *Store) listGroupMessagesPage(ctx context.Context, querier rowsQuerier, scope groupAvailabilityScope, unreadOnly bool, senderEndpointID string, cursorKeys []string, limit int) ([]groupMessageRecord, error) {
	viewer := scope.viewer
	if viewer.PersonID == "" {
		return []groupMessageRecord{}, nil
	}

	args := []any{viewer.PersonID, viewer.PersonID, viewer.Group.GroupID}
	query := `
SELECT
  gm.message_id,
  m.forwarded_message_id,
  m.forwarded_from_address,
  m.sender_endpoint_id,
  (
    SELECT sender_ea.address
    FROM endpoint_addresses AS sender_ea
    WHERE sender_ea.endpoint_id = m.sender_endpoint_id
    ORDER BY sender_ea.created_at ASC, sender_ea.address ASC
    LIMIT 1
  ) AS sender_address,
  gm.created_at,
  m.subject,
  m.content_type,
  m.schema_version,
  m.body_blob_ref,
  m.body_size,
  m.body_sha256,
  gr.first_read_at,
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM group_message_eligibility AS ge_viewer
      WHERE ge_viewer.message_id = gm.message_id
        AND ge_viewer.person_id = ?
    ) THEN 1
    ELSE 0
  END AS viewer_eligible,
  (
    SELECT COUNT(*)
    FROM group_message_eligibility AS ge
    JOIN group_reads AS gr_eligible
      ON gr_eligible.message_id = ge.message_id
     AND gr_eligible.person_id = ge.person_id
    WHERE ge.message_id = gm.message_id
  ) AS read_count,
  gm.eligible_count
FROM group_messages AS gm
JOIN messages AS m ON m.message_id = gm.message_id
LEFT JOIN group_reads AS gr
  ON gr.message_id = gm.message_id
 AND gr.person_id = ?
WHERE gm.group_id = ?
`
	if viewer.VisibilityCutoff != nil {
		query += `
  AND gm.created_at <= ?
`
		args = append(args, *viewer.VisibilityCutoff)
	}
	if unreadOnly {
		query += `
  AND gr.first_read_at IS NULL
`
	}
	if senderEndpointID != "" {
		query += `
  AND m.sender_endpoint_id = ?
`
		args = append(args, senderEndpointID)
	}
	if len(cursorKeys) > 0 {
		query += `
  AND (gm.created_at > ? OR (gm.created_at = ? AND gm.message_id > ?))
`
		args = append(args, cursorKeys[0], cursorKeys[0], cursorKeys[1])
	}
	query += `
ORDER BY gm.created_at ASC, gm.message_id ASC
`
	query += `
LIMIT ?
`
	args = append(args, limit)

	rows, err := querier.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query group messages for %q in %q: %w", viewer.Person, viewer.Group.Address, err)
	}
	defer rows.Close()

	records := make([]groupMessageRecord, 0)
	for rows.Next() {
		var record groupMessageRecord
		if err := rows.Scan(
			&record.MessageID,
			&record.ForwardedMessageID,
			&record.ForwardedFromAddress,
			&record.SenderEndpointID,
			&record.SenderAddress,
			&record.MessageCreatedAt,
			&record.Subject,
			&record.ContentType,
			&record.SchemaVersion,
			&record.BodyBlobRef,
			&record.BodySize,
			&record.BodySHA256,
			&record.FirstReadAt,
			&record.ViewerEligible,
			&record.ReadCount,
			&record.EligibleCount,
		); err != nil {
			return nil, fmt.Errorf("scan group message row: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate group messages: %w", err)
	}
	return records, nil
}
