package waypost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGroupMembershipLifecycle(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}

	firstMembership, err := store.AddGroupMember(context.Background(), group.Address, "alice")
	if err != nil {
		t.Fatalf("AddGroupMember(first) error = %v", err)
	}
	if !firstMembership.Active {
		t.Fatal("first membership active = false, want true")
	}

	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); !errors.Is(err, ErrActiveMembershipExists) {
		t.Fatalf("AddGroupMember(duplicate) error = %v, want ErrActiveMembershipExists", err)
	}

	removedMembership, err := store.RemoveGroupMember(context.Background(), group.Address, "alice")
	if err != nil {
		t.Fatalf("RemoveGroupMember() error = %v", err)
	}
	if removedMembership.Active {
		t.Fatal("removed membership active = true, want false")
	}
	if removedMembership.LeftAt == nil || strings.TrimSpace(*removedMembership.LeftAt) == "" {
		t.Fatalf("removed membership left_at = %v, want non-empty timestamp", removedMembership.LeftAt)
	}

	if _, err := store.RemoveGroupMember(context.Background(), group.Address, "alice"); !errors.Is(err, ErrActiveMembershipMissing) {
		t.Fatalf("RemoveGroupMember(missing) error = %v, want ErrActiveMembershipMissing", err)
	}

	secondMembership, err := store.AddGroupMember(context.Background(), group.Address, "alice")
	if err != nil {
		t.Fatalf("AddGroupMember(rejoin) error = %v", err)
	}
	if secondMembership.MembershipID == firstMembership.MembershipID {
		t.Fatalf("rejoin membership_id = %q, want a new membership id", secondMembership.MembershipID)
	}

	memberships, err := store.ListGroupMembers(context.Background(), group.Address)
	if err != nil {
		t.Fatalf("ListGroupMembers() error = %v", err)
	}
	if len(memberships) != 2 {
		t.Fatalf("len(memberships) = %d, want 2", len(memberships))
	}

	activeCount := 0
	historicalCount := 0
	for _, membership := range memberships {
		if membership.Person != "alice" {
			t.Fatalf("membership person = %q, want alice", membership.Person)
		}
		if membership.GroupID != group.GroupID {
			t.Fatalf("membership group_id = %q, want %q", membership.GroupID, group.GroupID)
		}
		if membership.Active {
			activeCount++
			if membership.LeftAt != nil {
				t.Fatalf("active membership left_at = %v, want nil", membership.LeftAt)
			}
		} else {
			historicalCount++
			if membership.LeftAt == nil {
				t.Fatal("historical membership left_at = nil, want timestamp")
			}
		}
	}
	if activeCount != 1 {
		t.Fatalf("active membership count = %d, want 1", activeCount)
	}
	if historicalCount != 1 {
		t.Fatalf("historical membership count = %d, want 1", historicalCount)
	}
}

func TestGroupTranscriptPageDoesNotHydrateLookaheadBlob(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/transcript-lookahead")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	first := mustSendGroupMessage(t, store, group.Address, "agent/sender", "first", "first body")
	second := mustSendGroupMessage(t, store, group.Address, "agent/sender", "second", "second body")
	if err := os.WriteFile(filepath.Join(runtime.BlobDir(), second.BodyBlobRef), []byte{}, 0o600); err != nil {
		t.Fatalf("corrupt lookahead blob error = %v", err)
	}

	page, err := store.ListGroupTranscriptPage(context.Background(), GroupTranscriptParams{
		Address: group.Address,
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("ListGroupTranscriptPage(first) error = %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].MessageID != first.MessageID || page.NextCursor == "" {
		t.Fatalf("ListGroupTranscriptPage(first) = %+v", page)
	}

	_, err = store.ListGroupTranscriptPage(context.Background(), GroupTranscriptParams{
		Address: group.Address,
		Limit:   1,
		Cursor:  page.NextCursor,
	})
	if !errors.Is(err, ErrBodyIntegrity) {
		t.Fatalf("ListGroupTranscriptPage(second) error = %v, want ErrBodyIntegrity", err)
	}
}

func TestGroupNotificationSubscriberLifecycle(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}

	firstSubscriber, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/moderator", "moderator")
	if err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(first) error = %v", err)
	}
	if !firstSubscriber.Active {
		t.Fatal("first subscriber active = false, want true")
	}
	if firstSubscriber.NotifyAddress != "agent-deck/moderator" {
		t.Fatalf("notify_address = %q, want agent-deck/moderator", firstSubscriber.NotifyAddress)
	}
	if firstSubscriber.Person != "moderator" {
		t.Fatalf("person = %q, want moderator", firstSubscriber.Person)
	}

	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/moderator", "moderator"); !errors.Is(err, ErrActiveSubscriberExists) {
		t.Fatalf("AddGroupNotificationSubscriber(duplicate) error = %v, want ErrActiveSubscriberExists", err)
	}

	subscribers, err := store.ListGroupNotificationSubscribers(context.Background(), group.Address)
	if err != nil {
		t.Fatalf("ListGroupNotificationSubscribers(active) error = %v", err)
	}
	if len(subscribers) != 1 {
		t.Fatalf("len(active subscribers) = %d, want 1", len(subscribers))
	}

	removedSubscriber, err := store.RemoveGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/moderator")
	if err != nil {
		t.Fatalf("RemoveGroupNotificationSubscriber() error = %v", err)
	}
	if removedSubscriber.Active {
		t.Fatal("removed subscriber active = true, want false")
	}
	if removedSubscriber.RemovedAt == nil || strings.TrimSpace(*removedSubscriber.RemovedAt) == "" {
		t.Fatalf("removed subscriber removed_at = %v, want non-empty timestamp", removedSubscriber.RemovedAt)
	}

	if _, err := store.RemoveGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/moderator"); !errors.Is(err, ErrActiveSubscriberMissing) {
		t.Fatalf("RemoveGroupNotificationSubscriber(missing) error = %v, want ErrActiveSubscriberMissing", err)
	}

	subscribers, err = store.ListGroupNotificationSubscribers(context.Background(), group.Address)
	if err != nil {
		t.Fatalf("ListGroupNotificationSubscribers(after remove) error = %v", err)
	}
	if len(subscribers) != 0 {
		t.Fatalf("len(active subscribers after remove) = %d, want 0", len(subscribers))
	}

	secondSubscriber, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/moderator", "moderator")
	if err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(readd) error = %v", err)
	}
	if secondSubscriber.SubscriberID == firstSubscriber.SubscriberID {
		t.Fatalf("readd subscriber_id = %q, want a new subscriber id", secondSubscriber.SubscriberID)
	}

	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/empty-person", " "); err == nil || !strings.Contains(err.Error(), "person is required") {
		t.Fatalf("AddGroupNotificationSubscriber(empty person) error = %v, want person required", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "group/not-a-personal-target", "moderator"); err == nil || !strings.Contains(err.Error(), "reserved group/ prefix") {
		t.Fatalf("AddGroupNotificationSubscriber(group target) error = %v, want reserved group prefix rejection", err)
	}
}

func TestGroupNotificationSubscriberRepairsLegacyEmptyPerson(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "observer"); err != nil {
		t.Fatalf("AddGroupMember(observer) error = %v", err)
	}
	if _, err := runtime.DB().Exec(`
INSERT INTO group_notification_subscribers (
  subscriber_id,
  group_id,
  notify_address,
  person,
  created_at,
  removed_at,
  metadata_json
) VALUES ('gns_legacy_empty_person', ?, 'agent-deck/observer', '', '2026-04-18T00:00:00Z', NULL, '{}')
`, group.GroupID); err != nil {
		t.Fatalf("insert legacy empty-person subscriber error = %v", err)
	}

	repaired, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer", "observer")
	if err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(repair legacy) error = %v", err)
	}
	if repaired.SubscriberID != "gns_legacy_empty_person" || repaired.Person != "observer" {
		t.Fatalf("repaired subscriber = (%q, %q), want legacy id and observer", repaired.SubscriberID, repaired.Person)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer", "observer"); !errors.Is(err, ErrActiveSubscriberExists) {
		t.Fatalf("AddGroupNotificationSubscriber(duplicate repaired) error = %v, want ErrActiveSubscriberExists", err)
	}

	sent, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent-deck/alice",
		Subject:       "update",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("group body"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}
	if len(sent.GroupNotificationAddresses) != 1 || sent.GroupNotificationAddresses[0] != "agent-deck/observer" {
		t.Fatalf("group notification addresses = %v, want [agent-deck/observer]", sent.GroupNotificationAddresses)
	}
	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"}); err != nil {
		t.Fatalf("Receive(observer subscriber) error = %v", err)
	}
}

func TestReadMessageReturnsGroupMessageBodyWithoutRecordingRead(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}

	sent := mustSendGroupMessage(t, store, group.Address, "agent/sender", "first", "first body")

	read, err := store.ReadMessage(context.Background(), sent.MessageID)
	if err != nil {
		t.Fatalf("ReadMessage(group message) error = %v", err)
	}
	if read.MessageID != sent.MessageID {
		t.Fatalf("ReadMessage(group message) message_id = %q, want %q", read.MessageID, sent.MessageID)
	}
	if read.Body != "first body" {
		t.Fatalf("ReadMessage(group message) body = %q, want first body", read.Body)
	}

	var readCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM group_reads`).Scan(&readCount); err != nil {
		t.Fatalf("count group_reads error = %v", err)
	}
	if readCount != 0 {
		t.Fatalf("group_reads count = %d, want 0", readCount)
	}
}

func TestGroupAndEndpointNamespaceCollision(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	if _, err := store.Send(context.Background(), SendParams{
		ToAddress:     "workflow/reviewer/task-123",
		Subject:       "personal message",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("hello"),
	}); err != nil {
		t.Fatalf("Send(endpoint) error = %v", err)
	}

	if _, err := store.CreateGroup(context.Background(), "workflow/reviewer/task-123"); err == nil || !strings.Contains(err.Error(), "group addresses must start with group/") {
		t.Fatalf("CreateGroup(non-group prefix) error = %v, want group prefix rejection", err)
	}
	if _, err := store.Send(context.Background(), SendParams{
		ToAddress:     "group/missing-personal",
		Subject:       "personal message",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("hello"),
	}); err == nil || !strings.Contains(err.Error(), "reserved group/ prefix") {
		t.Fatalf("Send(personal group prefix) error = %v, want reserved group prefix rejection", err)
	}
	if _, err := store.Send(context.Background(), SendParams{
		ToAddress:     "workflow/not-a-group",
		Subject:       "group message",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("hello"),
		Group:         true,
	}); err == nil || !strings.Contains(err.Error(), "group addresses must start with group/") {
		t.Fatalf("Send(group non-group prefix) error = %v, want group prefix rejection", err)
	}

	group, err := store.CreateGroup(context.Background(), "group/reviewer")
	if err != nil {
		t.Fatalf("CreateGroup(group/reviewer) error = %v", err)
	}
	if _, err := store.CreateGroup(context.Background(), group.Address); !errors.Is(err, ErrGroupExists) {
		t.Fatalf("CreateGroup(duplicate group) error = %v, want ErrGroupExists", err)
	}
	if _, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		Subject:       "group collision",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("hello group"),
	}); !errors.Is(err, ErrAddressReservedByGroup) {
		t.Fatalf("Send(group collision) error = %v, want ErrAddressReservedByGroup", err)
	}

	var endpointAddressCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM endpoint_addresses WHERE address = ?`, group.Address).Scan(&endpointAddressCount); err != nil {
		t.Fatalf("count endpoint_addresses(group) error = %v", err)
	}
	if endpointAddressCount != 0 {
		t.Fatalf("group address endpoint count = %d, want 0", endpointAddressCount)
	}

	if _, err := store.List(context.Background(), ListParams{Address: group.Address}); !errors.Is(err, ErrAddressReservedByGroup) {
		t.Fatalf("List(group collision) error = %v, want ErrAddressReservedByGroup", err)
	}

	if _, err := store.Receive(context.Background(), ReceiveParams{Address: group.Address}); !errors.Is(err, ErrAddressReservedByGroup) {
		t.Fatalf("Receive(group collision) error = %v, want ErrAddressReservedByGroup", err)
	}
}

func TestGroupControlPlaneCLI(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")

	createStdout := &bytes.Buffer{}
	createApp := NewApp(strings.NewReader(""), createStdout, &bytes.Buffer{})
	if err := createApp.RunWithStateDir(context.Background(), stateDir, []string{
		"group",
		"create",
		"--group", "group/ops",
		"--json",
	}); err != nil {
		t.Fatalf("group create error = %v", err)
	}

	var group GroupRecord
	if err := json.Unmarshal(createStdout.Bytes(), &group); err != nil {
		t.Fatalf("json.Unmarshal(group create) error = %v", err)
	}
	if group.Address != "group/ops" {
		t.Fatalf("group address = %q, want group/ops", group.Address)
	}

	listStdout := &bytes.Buffer{}
	listApp := NewApp(strings.NewReader(""), listStdout, &bytes.Buffer{})
	if err := listApp.RunWithStateDir(context.Background(), stateDir, []string{
		"group",
		"list",
		"--json",
	}); err != nil {
		t.Fatalf("group list error = %v", err)
	}

	var groupsPage Page[GroupRecord]
	if err := json.Unmarshal(listStdout.Bytes(), &groupsPage); err != nil {
		t.Fatalf("json.Unmarshal(group list) error = %v", err)
	}
	groups := groupsPage.Items
	if len(groups) != 1 {
		t.Fatalf("len(group list) = %d, want 1", len(groups))
	}
	if groups[0].Address != "group/ops" {
		t.Fatalf("group list address = %q, want group/ops", groups[0].Address)
	}

	addStdout := &bytes.Buffer{}
	addApp := NewApp(strings.NewReader(""), addStdout, &bytes.Buffer{})
	if err := addApp.RunWithStateDir(context.Background(), stateDir, []string{
		"group",
		"add-member",
		"--group", "group/ops",
		"--person", "alice",
		"--json",
	}); err != nil {
		t.Fatalf("group add-member error = %v", err)
	}

	var membership GroupMembershipRecord
	if err := json.Unmarshal(addStdout.Bytes(), &membership); err != nil {
		t.Fatalf("json.Unmarshal(group add-member) error = %v", err)
	}
	if !membership.Active {
		t.Fatal("membership active = false, want true")
	}

	membersStdout := &bytes.Buffer{}
	membersApp := NewApp(strings.NewReader(""), membersStdout, &bytes.Buffer{})
	if err := membersApp.RunWithStateDir(context.Background(), stateDir, []string{
		"group",
		"members",
		"--group", "group/ops",
		"--json",
	}); err != nil {
		t.Fatalf("group members error = %v", err)
	}

	var membershipsPage Page[GroupMembershipRecord]
	if err := json.Unmarshal(membersStdout.Bytes(), &membershipsPage); err != nil {
		t.Fatalf("json.Unmarshal(group members) error = %v", err)
	}
	memberships := membershipsPage.Items
	if len(memberships) != 1 {
		t.Fatalf("len(group members) = %d, want 1", len(memberships))
	}
	if memberships[0].Person != "alice" {
		t.Fatalf("group members person = %q, want alice", memberships[0].Person)
	}

	inspectStdout := &bytes.Buffer{}
	inspectApp := NewApp(strings.NewReader(""), inspectStdout, &bytes.Buffer{})
	if err := inspectApp.RunWithStateDir(context.Background(), stateDir, []string{
		"address",
		"inspect",
		"--address", "group/ops",
		"--json",
	}); err != nil {
		t.Fatalf("address inspect(group) error = %v", err)
	}

	var inspection AddressInspection
	if err := json.Unmarshal(inspectStdout.Bytes(), &inspection); err != nil {
		t.Fatalf("json.Unmarshal(address inspect group) error = %v", err)
	}
	if inspection.Kind != AddressKindGroup {
		t.Fatalf("group inspect kind = %q, want %q", inspection.Kind, AddressKindGroup)
	}
	if inspection.GroupID == nil || *inspection.GroupID != group.GroupID {
		t.Fatalf("group inspect group_id = %v, want %q", inspection.GroupID, group.GroupID)
	}

	sendApp := NewApp(strings.NewReader("hello endpoint"), &bytes.Buffer{}, &bytes.Buffer{})
	if err := sendApp.RunWithStateDir(context.Background(), stateDir, []string{
		"send",
		"--to", "workflow/personal",
		"--body-file", "-",
	}); err != nil {
		t.Fatalf("send endpoint error = %v", err)
	}

	endpointInspectStdout := &bytes.Buffer{}
	endpointInspectApp := NewApp(strings.NewReader(""), endpointInspectStdout, &bytes.Buffer{})
	if err := endpointInspectApp.RunWithStateDir(context.Background(), stateDir, []string{
		"address",
		"inspect",
		"--address", "workflow/personal",
		"--json",
	}); err != nil {
		t.Fatalf("address inspect(endpoint) error = %v", err)
	}

	var endpointInspection AddressInspection
	if err := json.Unmarshal(endpointInspectStdout.Bytes(), &endpointInspection); err != nil {
		t.Fatalf("json.Unmarshal(address inspect endpoint) error = %v", err)
	}
	if endpointInspection.Kind != AddressKindEndpoint {
		t.Fatalf("endpoint inspect kind = %q, want %q", endpointInspection.Kind, AddressKindEndpoint)
	}
	if endpointInspection.EndpointID == nil || strings.TrimSpace(*endpointInspection.EndpointID) == "" {
		t.Fatalf("endpoint inspect endpoint_id = %v, want non-empty", endpointInspection.EndpointID)
	}

	unboundStdout := &bytes.Buffer{}
	unboundApp := NewApp(strings.NewReader(""), unboundStdout, &bytes.Buffer{})
	if err := unboundApp.RunWithStateDir(context.Background(), stateDir, []string{
		"address",
		"inspect",
		"--address", "group/missing",
		"--json",
	}); err != nil {
		t.Fatalf("address inspect(unbound) error = %v", err)
	}

	var unboundInspection AddressInspection
	if err := json.Unmarshal(unboundStdout.Bytes(), &unboundInspection); err != nil {
		t.Fatalf("json.Unmarshal(address inspect unbound) error = %v", err)
	}
	if unboundInspection.Kind != AddressKindUnbound {
		t.Fatalf("unbound inspect kind = %q, want %q", unboundInspection.Kind, AddressKindUnbound)
	}
}

func TestExplicitGroupSendStoresMessageWithoutDelivery(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "bob"); err != nil {
		t.Fatalf("AddGroupMember(bob) error = %v", err)
	}

	result, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent/sender",
		Subject:       "group update",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("hello group"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}
	if result.Mode != SendModeGroup {
		t.Fatalf("Send(group) mode = %q, want %q", result.Mode, SendModeGroup)
	}
	if result.DeliveryID != "" {
		t.Fatalf("Send(group) delivery_id = %q, want empty", result.DeliveryID)
	}
	if result.GroupID != group.GroupID {
		t.Fatalf("Send(group) group_id = %q, want %q", result.GroupID, group.GroupID)
	}
	if result.GroupAddress != group.Address {
		t.Fatalf("Send(group) group_address = %q, want %q", result.GroupAddress, group.Address)
	}
	if result.EligibleCount != 2 {
		t.Fatalf("Send(group) eligible_count = %d, want 2", result.EligibleCount)
	}
	if strings.TrimSpace(result.MessageCreatedAt) == "" {
		t.Fatalf("Send(group) message_created_at = %q, want non-empty", result.MessageCreatedAt)
	}

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
	if deliveryCount != 0 {
		t.Fatalf("delivery count = %d, want 0", deliveryCount)
	}

	var storedGroupID string
	var eligibleCount int
	if err := runtime.DB().QueryRow(`
SELECT group_id, eligible_count
FROM group_messages
WHERE message_id = ?
	`, result.MessageID).Scan(&storedGroupID, &eligibleCount); err != nil {
		t.Fatalf("select group_messages row error = %v", err)
	}
	if storedGroupID != group.GroupID {
		t.Fatalf("group_messages group_id = %q, want %q", storedGroupID, group.GroupID)
	}
	if eligibleCount != 2 {
		t.Fatalf("group_messages eligible_count = %d, want 2", eligibleCount)
	}

	var eligibilityCount int
	if err := runtime.DB().QueryRow(`
SELECT COUNT(*)
FROM group_message_eligibility
WHERE message_id = ?
	`, result.MessageID).Scan(&eligibilityCount); err != nil {
		t.Fatalf("count group_message_eligibility error = %v", err)
	}
	if eligibilityCount != 2 {
		t.Fatalf("group_message_eligibility count = %d, want 2", eligibilityCount)
	}

	var groupEndpointCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM endpoint_addresses WHERE address = ?`, group.Address).Scan(&groupEndpointCount); err != nil {
		t.Fatalf("count endpoint_addresses(group) error = %v", err)
	}
	if groupEndpointCount != 0 {
		t.Fatalf("group address endpoint count = %d, want 0", groupEndpointCount)
	}
}

func TestExplicitGroupSendAllowsZeroMemberGroups(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/empty")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}

	result, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		Subject:       "empty group update",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("still durable"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(zero-member group) error = %v", err)
	}
	if result.EligibleCount != 0 {
		t.Fatalf("Send(zero-member group) eligible_count = %d, want 0", result.EligibleCount)
	}

	var eligibilityCount int
	if err := runtime.DB().QueryRow(`
SELECT COUNT(*)
FROM group_message_eligibility
WHERE message_id = ?
	`, result.MessageID).Scan(&eligibilityCount); err != nil {
		t.Fatalf("count group_message_eligibility error = %v", err)
	}
	if eligibilityCount != 0 {
		t.Fatalf("group_message_eligibility count = %d, want 0", eligibilityCount)
	}
}

func TestGroupSendMarksSenderReadAndQueuesSubscriberDeliveries(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "moderator"); err != nil {
		t.Fatalf("AddGroupMember(moderator) error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "observer"); err != nil {
		t.Fatalf("AddGroupMember(observer) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/moderator", "moderator"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(moderator) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer", "observer"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(observer) error = %v", err)
	}

	sent, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent-deck/moderator",
		Subject:       "roundtable turn",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("group body"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}

	moderatorMessages, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "moderator",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(moderator) error = %v", err)
	}
	if len(moderatorMessages) != 1 {
		t.Fatalf("len(moderatorMessages) = %d, want 1", len(moderatorMessages))
	}
	if !moderatorMessages[0].Read {
		t.Fatal("moderator message read = false, want true for sender")
	}
	if moderatorMessages[0].ReadCount != 1 || moderatorMessages[0].EligibleCount != 2 {
		t.Fatalf("moderator counts = (%d, %d), want (1, 2)", moderatorMessages[0].ReadCount, moderatorMessages[0].EligibleCount)
	}

	observerMessages, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "observer",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(observer) error = %v", err)
	}
	if observerMessages[0].Read {
		t.Fatal("observer message read = true, want false")
	}
	if observerMessages[0].ReadCount != 1 || observerMessages[0].EligibleCount != 2 {
		t.Fatalf("observer counts = (%d, %d), want (1, 2)", observerMessages[0].ReadCount, observerMessages[0].EligibleCount)
	}

	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/moderator"}); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("Receive(sender subscriber) error = %v, want ErrNoMessage", err)
	}

	control, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"})
	if err != nil {
		t.Fatalf("Receive(observer subscriber) error = %v", err)
	}
	if control.Subject != "Group waypost update: group/ops" {
		t.Fatalf("control subject = %q, want group update", control.Subject)
	}
	if control.ContentType != "text/plain" || control.SchemaVersion != "group-notification/v1" {
		t.Fatalf("control shape = (%q, %q), want text/plain group-notification/v1", control.ContentType, control.SchemaVersion)
	}
	for _, want := range []string{
		"Action: group_message_available",
		"Group-Address: group/ops",
		"As-Person: observer",
		"Message-ID: " + sent.MessageID,
		"Subject: roundtable turn",
	} {
		if !strings.Contains(control.Body, want) {
			t.Fatalf("control body = %q, want %q", control.Body, want)
		}
	}
	if control.ForwardedMessageID == nil || *control.ForwardedMessageID != sent.MessageID {
		t.Fatalf("control forwarded_message_id = %v, want %q", control.ForwardedMessageID, sent.MessageID)
	}
	if control.ForwardedFromAddress == nil || *control.ForwardedFromAddress != "agent-deck/moderator" {
		t.Fatalf("control forwarded_from_address = %v, want sender", control.ForwardedFromAddress)
	}
	if len(sent.GroupNotificationAddresses) != 1 || sent.GroupNotificationAddresses[0] != "agent-deck/observer" {
		t.Fatalf("group notification addresses = %v, want [agent-deck/observer]", sent.GroupNotificationAddresses)
	}
}

func TestGroupSendCoalescesQueuedSubscriberDeliveries(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/coalesce")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "observer"); err != nil {
		t.Fatalf("AddGroupMember(observer) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer", "observer"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(observer) error = %v", err)
	}

	first := mustSendGroupMessage(t, store, group.Address, "agent-deck/sender", "first", "first body")
	second := mustSendGroupMessage(t, store, group.Address, "agent-deck/sender", "second", "second body")

	if len(first.GroupNotificationAddresses) != 1 || first.GroupNotificationAddresses[0] != "agent-deck/observer" {
		t.Fatalf("first group notification addresses = %v, want observer", first.GroupNotificationAddresses)
	}
	if len(second.GroupNotificationAddresses) != 0 {
		t.Fatalf("second group notification addresses = %v, want coalesced", second.GroupNotificationAddresses)
	}
	if countGroupSubscriberNotifications(t, runtime, group.Address, "agent-deck/observer", "queued") != 1 {
		t.Fatalf("queued subscriber notifications = %d, want 1", countGroupSubscriberNotifications(t, runtime, group.Address, "agent-deck/observer", "queued"))
	}

	control, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"})
	if err != nil {
		t.Fatalf("Receive(observer subscriber) error = %v", err)
	}
	if !strings.Contains(control.Body, "Message-ID: "+first.MessageID) {
		t.Fatalf("control body = %q, want retained first message id %q", control.Body, first.MessageID)
	}
	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"}); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("Receive(observer subscriber second) error = %v, want ErrNoMessage", err)
	}
}

func TestGroupSendQueuesSubscriberDeliveryWhenExistingNoticeLeased(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/coalesce-leased")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "observer"); err != nil {
		t.Fatalf("AddGroupMember(observer) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer", "observer"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(observer) error = %v", err)
	}

	first := mustSendGroupMessage(t, store, group.Address, "agent-deck/sender", "first", "first body")
	leased, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"})
	if err != nil {
		t.Fatalf("Receive(first observer subscriber) error = %v", err)
	}
	if !strings.Contains(leased.Body, "Message-ID: "+first.MessageID) {
		t.Fatalf("leased body = %q, want first message id %q", leased.Body, first.MessageID)
	}

	second := mustSendGroupMessage(t, store, group.Address, "agent-deck/sender", "second", "second body")
	if len(second.GroupNotificationAddresses) != 1 || second.GroupNotificationAddresses[0] != "agent-deck/observer" {
		t.Fatalf("second group notification addresses = %v, want observer", second.GroupNotificationAddresses)
	}
	if countGroupSubscriberNotifications(t, runtime, group.Address, "agent-deck/observer", "leased") != 1 {
		t.Fatalf("leased subscriber notifications = %d, want 1", countGroupSubscriberNotifications(t, runtime, group.Address, "agent-deck/observer", "leased"))
	}
	if countGroupSubscriberNotifications(t, runtime, group.Address, "agent-deck/observer", "queued") != 1 {
		t.Fatalf("queued subscriber notifications = %d, want 1", countGroupSubscriberNotifications(t, runtime, group.Address, "agent-deck/observer", "queued"))
	}

	control, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"})
	if err != nil {
		t.Fatalf("Receive(second observer subscriber) error = %v", err)
	}
	if !strings.Contains(control.Body, "Message-ID: "+second.MessageID) {
		t.Fatalf("control body = %q, want second message id %q", control.Body, second.MessageID)
	}
}

func TestGroupSendMarksSenderReadOnlyThroughSubscriberBinding(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "agent/sender"); err != nil {
		t.Fatalf("AddGroupMember(collision) error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "moderator"); err != nil {
		t.Fatalf("AddGroupMember(moderator) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent/sender", "moderator"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(moderator) error = %v", err)
	}

	if _, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent/sender",
		Subject:       "roundtable turn",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("group body"),
		Group:         true,
	}); err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}

	collisionMessages, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "agent/sender",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(collision) error = %v", err)
	}
	if collisionMessages[0].Read {
		t.Fatal("collision person read = true, want false")
	}
	if collisionMessages[0].ReadCount != 1 || collisionMessages[0].EligibleCount != 2 {
		t.Fatalf("collision counts = (%d, %d), want (1, 2)", collisionMessages[0].ReadCount, collisionMessages[0].EligibleCount)
	}

	moderatorMessages, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "moderator",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(moderator) error = %v", err)
	}
	if !moderatorMessages[0].Read {
		t.Fatal("moderator message read = false, want true")
	}
	if moderatorMessages[0].ReadCount != 1 || moderatorMessages[0].EligibleCount != 2 {
		t.Fatalf("moderator counts = (%d, %d), want (1, 2)", moderatorMessages[0].ReadCount, moderatorMessages[0].EligibleCount)
	}
}

func TestGroupSendAsPersonMarksSenderReadAndSuppressesOwnNotification(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "bob"); err != nil {
		t.Fatalf("AddGroupMember(bob) error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "moderator"); err != nil {
		t.Fatalf("AddGroupMember(moderator) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/alice", "alice"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(alice) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/bob", "bob"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(bob) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/moderator", "moderator"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(moderator) error = %v", err)
	}

	sent, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent-deck/moderator",
		AsPerson:      "alice",
		Subject:       "roundtable turn",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("group body"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(group as person) error = %v", err)
	}
	wantNotifications := map[string]bool{
		"agent-deck/bob":       false,
		"agent-deck/moderator": false,
	}
	for _, address := range sent.GroupNotificationAddresses {
		if _, ok := wantNotifications[address]; ok {
			wantNotifications[address] = true
		}
	}
	for address, seen := range wantNotifications {
		if !seen {
			t.Fatalf("group notification addresses = %v, missing %s", sent.GroupNotificationAddresses, address)
		}
	}
	if len(sent.GroupNotificationAddresses) != len(wantNotifications) {
		t.Fatalf("group notification addresses = %v, want only bob and moderator", sent.GroupNotificationAddresses)
	}

	aliceMessages, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(alice) error = %v", err)
	}
	if !aliceMessages[0].Read {
		t.Fatal("alice message read = false, want true")
	}
	if aliceMessages[0].ReadCount != 1 || aliceMessages[0].EligibleCount != 3 {
		t.Fatalf("alice counts = (%d, %d), want (1, 3)", aliceMessages[0].ReadCount, aliceMessages[0].EligibleCount)
	}
	moderatorMessages, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "moderator",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(moderator) error = %v", err)
	}
	if moderatorMessages[0].Read {
		t.Fatal("moderator message read = true, want false")
	}
	if moderatorMessages[0].ReadCount != 1 || moderatorMessages[0].EligibleCount != 3 {
		t.Fatalf("moderator counts = (%d, %d), want (1, 3)", moderatorMessages[0].ReadCount, moderatorMessages[0].EligibleCount)
	}
	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/alice"}); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("Receive(alice subscriber) error = %v, want ErrNoMessage", err)
	}
	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/bob"}); err != nil {
		t.Fatalf("Receive(bob subscriber) error = %v", err)
	}
	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/moderator"}); err != nil {
		t.Fatalf("Receive(moderator subscriber) error = %v", err)
	}
}

func TestGroupSendAsPersonRequiresActiveMember(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "inactive"); err != nil {
		t.Fatalf("AddGroupMember(inactive) error = %v", err)
	}
	if _, err := store.RemoveGroupMember(context.Background(), group.Address, "inactive"); err != nil {
		t.Fatalf("RemoveGroupMember(inactive) error = %v", err)
	}

	for _, asPerson := range []string{"missing", "inactive"} {
		_, err := store.Send(context.Background(), SendParams{
			ToAddress:     group.Address,
			FromAddress:   "participant/" + asPerson,
			AsPerson:      asPerson,
			Subject:       "roundtable turn",
			ContentType:   "text/plain",
			SchemaVersion: "v1",
			Body:          []byte("group body"),
			Group:         true,
		})
		if !errors.Is(err, ErrActiveMembershipMissing) {
			t.Fatalf("Send(group as %q) error = %v, want ErrActiveMembershipMissing", asPerson, err)
		}
	}
}

func TestGroupSendWritesSubscriberBlobsBeforeSendTransaction(t *testing.T) {
	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	blobWrites := 0
	store.writeBlobHook = func(inWriteTransaction bool) error {
		blobWrites++
		if inWriteTransaction {
			return errors.New("blob write during send transaction")
		}
		return nil
	}

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "observer"); err != nil {
		t.Fatalf("AddGroupMember(observer) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer", "observer"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(observer) error = %v", err)
	}

	if _, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent-deck/moderator",
		Subject:       "roundtable turn",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("group body"),
		Group:         true,
	}); err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}
	if blobWrites != 2 {
		t.Fatalf("blob writes = %d, want group body plus subscriber notification", blobWrites)
	}
	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"}); err != nil {
		t.Fatalf("Receive(observer subscriber) error = %v", err)
	}
}

func TestGroupSendUsesPreparedSubscriberSnapshot(t *testing.T) {
	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "observer"); err != nil {
		t.Fatalf("AddGroupMember(observer) error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "late"); err != nil {
		t.Fatalf("AddGroupMember(late) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer", "observer"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(observer) error = %v", err)
	}

	blobWrites := 0
	addedLateSubscriber := false
	store.writeBlobHook = func(inWriteTransaction bool) error {
		blobWrites++
		if inWriteTransaction {
			return errors.New("blob write during send transaction")
		}
		if blobWrites == 2 && !addedLateSubscriber {
			addedLateSubscriber = true
			if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/late", "late"); err != nil {
				return err
			}
		}
		return nil
	}

	sent, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent-deck/moderator",
		Subject:       "roundtable turn",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("group body"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}
	if !addedLateSubscriber {
		t.Fatal("late subscriber was not added during prewrite window")
	}
	if blobWrites != 2 {
		t.Fatalf("blob writes = %d, want body plus prepared observer notification", blobWrites)
	}
	if len(sent.GroupNotificationAddresses) != 1 || sent.GroupNotificationAddresses[0] != "agent-deck/observer" {
		t.Fatalf("group notification addresses = %v, want observer only", sent.GroupNotificationAddresses)
	}
	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"}); err != nil {
		t.Fatalf("Receive(observer subscriber) error = %v", err)
	}
	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/late"}); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("Receive(late subscriber) error = %v, want ErrNoMessage", err)
	}
}

func TestGroupSendRemovesUnusedPreparedSubscriberBlobs(t *testing.T) {
	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "observer"); err != nil {
		t.Fatalf("AddGroupMember(observer) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer", "observer"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(observer) error = %v", err)
	}

	blobWrites := 0
	removedSubscriber := false
	store.writeBlobHook = func(inWriteTransaction bool) error {
		blobWrites++
		if inWriteTransaction {
			return errors.New("blob write during send transaction")
		}
		if blobWrites == 2 && !removedSubscriber {
			removedSubscriber = true
			if _, err := store.RemoveGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer"); err != nil {
				return err
			}
		}
		return nil
	}
	var removedBlobs []string
	removeFile := store.removeFile
	store.removeFile = func(path string) error {
		if strings.HasPrefix(filepath.Base(path), "blob_") {
			removedBlobs = append(removedBlobs, filepath.Base(path))
		}
		return removeFile(path)
	}

	sent, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent-deck/moderator",
		Subject:       "roundtable turn",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("group body"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}
	if !removedSubscriber {
		t.Fatal("subscriber was not removed during prewrite window")
	}
	if len(sent.GroupNotificationAddresses) != 0 {
		t.Fatalf("group notification addresses = %v, want none", sent.GroupNotificationAddresses)
	}
	if len(removedBlobs) != 1 {
		t.Fatalf("removed prepared subscriber blobs = %v, want one", removedBlobs)
	}
	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"}); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("Receive(observer subscriber) error = %v, want ErrNoMessage", err)
	}
}

func TestGroupSendSkipsInvalidSubscribers(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "observer"); err != nil {
		t.Fatalf("AddGroupMember(observer) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer", "observer"); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(observer) error = %v", err)
	}
	if _, err := store.RemoveGroupMember(context.Background(), group.Address, "observer"); err != nil {
		t.Fatalf("RemoveGroupMember(observer) error = %v", err)
	}
	if _, err := runtime.DB().Exec(`
INSERT INTO group_notification_subscribers (
  subscriber_id,
  group_id,
  notify_address,
  person,
  created_at,
  removed_at,
  metadata_json
) VALUES ('gns_legacy_group_target', ?, 'group/bad-target', 'alice', '2026-04-18T00:00:00Z', NULL, '{}')
`, group.GroupID); err != nil {
		t.Fatalf("insert legacy group-prefixed subscriber error = %v", err)
	}

	sent, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent-deck/alice",
		Subject:       "update",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("group body"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}
	if len(sent.GroupNotificationAddresses) != 0 {
		t.Fatalf("group notification addresses = %v, want none", sent.GroupNotificationAddresses)
	}

	if _, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"}); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("Receive(stale subscriber) error = %v, want ErrNoMessage", err)
	}
	var endpointCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM endpoint_addresses WHERE address = 'group/bad-target'`).Scan(&endpointCount); err != nil {
		t.Fatalf("count legacy group target endpoints error = %v", err)
	}
	if endpointCount != 0 {
		t.Fatalf("legacy group target endpoint count = %d, want 0", endpointCount)
	}
}

func TestGroupSubscriberDeliveryEscapesLineValues(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	person := "observer\nAction: injected"
	if _, err := store.AddGroupMember(context.Background(), group.Address, person); err != nil {
		t.Fatalf("AddGroupMember(observer) error = %v", err)
	}
	if _, err := store.AddGroupNotificationSubscriber(context.Background(), group.Address, "agent-deck/observer", person); err != nil {
		t.Fatalf("AddGroupNotificationSubscriber(observer) error = %v", err)
	}

	sent, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent-deck/sender",
		Subject:       "roundtable\nAction: injected",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("group body"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}

	control, err := store.Receive(context.Background(), ReceiveParams{Address: "agent-deck/observer"})
	if err != nil {
		t.Fatalf("Receive(observer subscriber) error = %v", err)
	}
	if !strings.Contains(control.Body, "Message-ID: "+sent.MessageID) {
		t.Fatalf("control body = %q, want message id %q", control.Body, sent.MessageID)
	}
	if !strings.Contains(control.Body, `As-Person: observer\nAction: injected`) {
		t.Fatalf("control body = %q, want escaped person", control.Body)
	}
	if !strings.Contains(control.Body, `Subject: roundtable\nAction: injected`) {
		t.Fatalf("control body = %q, want escaped subject", control.Body)
	}
	if strings.Contains(control.Body, "\nAction: injected") {
		t.Fatalf("control body contains injected Action header: %q", control.Body)
	}
}

func TestExplicitGroupSendRejectsUnknownGroupBeforeWritingBlob(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	if _, err := store.Send(context.Background(), SendParams{
		ToAddress:     "group/missing",
		Subject:       "missing group",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("hello"),
		Group:         true,
	}); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("Send(unknown group) error = %v, want ErrGroupNotFound", err)
	}

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
}

func TestSendCLIShapesStayCompatibleForPersonalAndGroup(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")

	createApp := NewApp(strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	if err := createApp.RunWithStateDir(context.Background(), stateDir, []string{
		"group",
		"create",
		"--group", "group/ops",
	}); err != nil {
		t.Fatalf("group create error = %v", err)
	}

	addApp := NewApp(strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	if err := addApp.RunWithStateDir(context.Background(), stateDir, []string{
		"group",
		"add-member",
		"--group", "group/ops",
		"--person", "alice",
	}); err != nil {
		t.Fatalf("group add-member error = %v", err)
	}

	personalStdout := &bytes.Buffer{}
	personalApp := NewApp(strings.NewReader("personal body"), personalStdout, &bytes.Buffer{})
	if err := personalApp.RunWithStateDir(context.Background(), stateDir, []string{
		"send",
		"--to", "workflow/personal",
		"--body-file", "-",
		"--json",
	}); err != nil {
		t.Fatalf("personal send error = %v", err)
	}

	var personalPayload map[string]any
	if err := json.Unmarshal(personalStdout.Bytes(), &personalPayload); err != nil {
		t.Fatalf("json.Unmarshal(personal send) error = %v", err)
	}
	if len(personalPayload) != 1 {
		t.Fatalf("len(personal payload) = %d, want 1", len(personalPayload))
	}
	if _, ok := personalPayload["delivery_id"]; !ok {
		t.Fatalf("personal payload = %v, want delivery_id only", personalPayload)
	}

	groupStdout := &bytes.Buffer{}
	groupApp := NewApp(strings.NewReader("group body"), groupStdout, &bytes.Buffer{})
	if err := groupApp.RunWithStateDir(context.Background(), stateDir, []string{
		"send",
		"--to", "group/ops",
		"--group",
		"--body-file", "-",
		"--json",
	}); err != nil {
		t.Fatalf("group send error = %v", err)
	}

	var groupPayload map[string]any
	if err := json.Unmarshal(groupStdout.Bytes(), &groupPayload); err != nil {
		t.Fatalf("json.Unmarshal(group send) error = %v", err)
	}
	if groupPayload["mode"] != SendModeGroup {
		t.Fatalf("group payload mode = %v, want %q", groupPayload["mode"], SendModeGroup)
	}
	if groupPayload["group_address"] != "group/ops" {
		t.Fatalf("group payload group_address = %v, want group/ops", groupPayload["group_address"])
	}
	if _, ok := groupPayload["delivery_id"]; ok {
		t.Fatalf("group payload = %v, want no delivery_id", groupPayload)
	}
	if _, ok := groupPayload["eligible_count"]; !ok {
		t.Fatalf("group payload = %v, want eligible_count", groupPayload)
	}
	if _, ok := groupPayload["message_created_at"]; !ok {
		t.Fatalf("group payload = %v, want message_created_at", groupPayload)
	}
}

func TestGroupListWaitAndRecvTrackPerPersonReadState(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/ops")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "bob"); err != nil {
		t.Fatalf("AddGroupMember(bob) error = %v", err)
	}

	current := time.Date(2026, 3, 29, 3, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }

	first := mustSendGroupMessage(t, store, group.Address, "agent/sender", "first", "first body")
	current = current.Add(time.Second)
	second := mustSendGroupMessage(t, store, group.Address, "agent/sender", "second", "second body")

	listed, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(alice before read) error = %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("len(ListGroupMessages(alice before read)) = %d, want 2", len(listed))
	}
	if listed[0].MessageID != first.MessageID || listed[1].MessageID != second.MessageID {
		t.Fatalf("listed message ids = [%q %q], want [%q %q]", listed[0].MessageID, listed[1].MessageID, first.MessageID, second.MessageID)
	}
	if listed[0].Read || listed[1].Read {
		t.Fatalf("listed read states before read = [%t %t], want both unread", listed[0].Read, listed[1].Read)
	}
	if listed[0].ReadCount != 0 || listed[0].EligibleCount != 2 {
		t.Fatalf("listed[0] counts = (%d, %d), want (0, 2)", listed[0].ReadCount, listed[0].EligibleCount)
	}

	waited, err := store.WaitGroupMessage(context.Background(), GroupWaitParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("WaitGroupMessage(alice) error = %v", err)
	}
	if waited.MessageID != first.MessageID {
		t.Fatalf("WaitGroupMessage(alice) message_id = %q, want %q", waited.MessageID, first.MessageID)
	}
	if waited.Read {
		t.Fatal("WaitGroupMessage(alice) returned read=true, want unread")
	}

	received, err := store.ReceiveGroupMessage(context.Background(), GroupReceiveParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ReceiveGroupMessage(alice first) error = %v", err)
	}
	if received.MessageID != first.MessageID {
		t.Fatalf("ReceiveGroupMessage(alice first) message_id = %q, want %q", received.MessageID, first.MessageID)
	}
	if received.Body != "first body" {
		t.Fatalf("ReceiveGroupMessage(alice first) body = %q, want %q", received.Body, "first body")
	}
	if received.ReadCount != 1 || received.EligibleCount != 2 {
		t.Fatalf("ReceiveGroupMessage(alice first) counts = (%d, %d), want (1, 2)", received.ReadCount, received.EligibleCount)
	}
	if strings.TrimSpace(received.FirstReadAt) == "" {
		t.Fatalf("ReceiveGroupMessage(alice first) first_read_at = %q, want non-empty", received.FirstReadAt)
	}

	aliceAfter, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(alice after read) error = %v", err)
	}
	if !aliceAfter[0].Read {
		t.Fatal("aliceAfter[0].Read = false, want true")
	}
	if aliceAfter[0].ReadCount != 1 || aliceAfter[0].EligibleCount != 2 {
		t.Fatalf("aliceAfter[0] counts = (%d, %d), want (1, 2)", aliceAfter[0].ReadCount, aliceAfter[0].EligibleCount)
	}
	if aliceAfter[1].Read {
		t.Fatal("aliceAfter[1].Read = true, want false")
	}

	bobAfter, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "bob",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(bob after alice read) error = %v", err)
	}
	if bobAfter[0].Read {
		t.Fatal("bobAfter[0].Read = true, want false")
	}
	if bobAfter[0].ReadCount != 1 || bobAfter[0].EligibleCount != 2 {
		t.Fatalf("bobAfter[0] counts = (%d, %d), want (1, 2)", bobAfter[0].ReadCount, bobAfter[0].EligibleCount)
	}
}

func TestGroupListWaitAndRecvPreserveForwardedFromAddress(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/forwarded")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}

	source, err := store.Send(context.Background(), SendParams{
		ToAddress:     "workflow/source",
		FromAddress:   "agent/sender",
		Subject:       "source",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("source body"),
	})
	if err != nil {
		t.Fatalf("Send(source) error = %v", err)
	}

	sent, err := store.Send(context.Background(), SendParams{
		ToAddress:            group.Address,
		FromAddress:          "agent/sender",
		Subject:              "forwarded",
		ContentType:          "text/plain",
		SchemaVersion:        "v1",
		ForwardedMessageID:   source.MessageID,
		ForwardedFromAddress: "agent/sender",
		Body:                 []byte("forwarded body"),
		Group:                true,
	})
	if err != nil {
		t.Fatalf("Send(group forwarded) error = %v", err)
	}

	listed, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages() error = %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("len(ListGroupMessages()) = %d, want 1", len(listed))
	}
	if listed[0].MessageID != sent.MessageID {
		t.Fatalf("listed message_id = %q, want %q", listed[0].MessageID, sent.MessageID)
	}
	if listed[0].ForwardedFromAddress == nil || *listed[0].ForwardedFromAddress != "agent/sender" {
		t.Fatalf("listed forwarded_from_address = %v, want agent/sender", listed[0].ForwardedFromAddress)
	}

	waited, err := store.WaitGroupMessage(context.Background(), GroupWaitParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("WaitGroupMessage() error = %v", err)
	}
	if waited.ForwardedFromAddress == nil || *waited.ForwardedFromAddress != "agent/sender" {
		t.Fatalf("waited forwarded_from_address = %v, want agent/sender", waited.ForwardedFromAddress)
	}

	received, err := store.ReceiveGroupMessage(context.Background(), GroupReceiveParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ReceiveGroupMessage() error = %v", err)
	}
	if received.ForwardedFromAddress == nil || *received.ForwardedFromAddress != "agent/sender" {
		t.Fatalf("received forwarded_from_address = %v, want agent/sender", received.ForwardedFromAddress)
	}
	if received.SenderAddress == nil || *received.SenderAddress != "agent/sender" {
		t.Fatalf("received sender_address = %v, want agent/sender", received.SenderAddress)
	}
}

func TestGroupListFiltersBySenderAddress(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/filter-sender")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}

	fromAlice := mustSendGroupMessage(t, store, group.Address, "agent/alice", "alice", "alice body")
	mustSendGroupMessage(t, store, group.Address, "agent/bob", "bob", "bob body")

	listed, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address:     group.Address,
		Person:      "alice",
		FromAddress: "agent/alice",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(from alice) error = %v", err)
	}
	if len(listed) != 1 || listed[0].MessageID != fromAlice.MessageID {
		t.Fatalf("ListGroupMessages(from alice) = %+v, want only %q", listed, fromAlice.MessageID)
	}
	if listed[0].SenderAddress == nil || *listed[0].SenderAddress != "agent/alice" {
		t.Fatalf("ListGroupMessages(from alice) sender_address = %v, want agent/alice", listed[0].SenderAddress)
	}
	waited, err := store.WaitGroupMessage(context.Background(), GroupWaitParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("WaitGroupMessage() error = %v", err)
	}
	if waited.SenderAddress == nil || *waited.SenderAddress != "agent/alice" {
		t.Fatalf("WaitGroupMessage() sender_address = %v, want agent/alice", waited.SenderAddress)
	}

	missing, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address:     group.Address,
		Person:      "alice",
		FromAddress: "agent/missing",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(from missing) error = %v", err)
	}
	if len(missing) != 0 || missing == nil {
		t.Fatalf("ListGroupMessages(from missing) = %+v, want empty", missing)
	}
}

func TestGroupHistoryVisibilityJoinLeaveRejoinAndStableCounts(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/history")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}

	current := time.Date(2026, 3, 29, 4, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }

	preJoin := mustSendGroupMessage(t, store, group.Address, "agent/sender", "pre-join", "pre-join body")

	current = current.Add(time.Second)
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice first) error = %v", err)
	}

	firstRead, err := store.ReceiveGroupMessage(context.Background(), GroupReceiveParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ReceiveGroupMessage(alice pre-join history) error = %v", err)
	}
	if firstRead.MessageID != preJoin.MessageID {
		t.Fatalf("ReceiveGroupMessage(alice pre-join history) message_id = %q, want %q", firstRead.MessageID, preJoin.MessageID)
	}
	if firstRead.ReadCount != 0 || firstRead.EligibleCount != 0 {
		t.Fatalf("pre-join read counts = (%d, %d), want (0, 0)", firstRead.ReadCount, firstRead.EligibleCount)
	}

	current = current.Add(time.Second)
	duringMembership := mustSendGroupMessage(t, store, group.Address, "agent/sender", "during-membership", "during body")

	secondRead, err := store.ReceiveGroupMessage(context.Background(), GroupReceiveParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ReceiveGroupMessage(alice active) error = %v", err)
	}
	if secondRead.MessageID != duringMembership.MessageID {
		t.Fatalf("ReceiveGroupMessage(alice active) message_id = %q, want %q", secondRead.MessageID, duringMembership.MessageID)
	}
	if secondRead.ReadCount != 1 || secondRead.EligibleCount != 1 {
		t.Fatalf("during-membership read counts = (%d, %d), want (1, 1)", secondRead.ReadCount, secondRead.EligibleCount)
	}

	current = current.Add(time.Second)
	if _, err := store.RemoveGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("RemoveGroupMember(alice) error = %v", err)
	}

	current = current.Add(time.Second)
	duringGap := mustSendGroupMessage(t, store, group.Address, "agent/sender", "during-gap", "gap body")

	inactiveView, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(alice inactive) error = %v", err)
	}
	if len(inactiveView) != 2 {
		t.Fatalf("len(ListGroupMessages(alice inactive)) = %d, want 2", len(inactiveView))
	}
	if inactiveView[0].MessageID != preJoin.MessageID || inactiveView[1].MessageID != duringMembership.MessageID {
		t.Fatalf("inactive visible messages = [%q %q], want [%q %q]", inactiveView[0].MessageID, inactiveView[1].MessageID, preJoin.MessageID, duringMembership.MessageID)
	}

	current = current.Add(time.Second)
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice rejoin) error = %v", err)
	}

	rejoinedView, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(alice rejoined) error = %v", err)
	}
	if len(rejoinedView) != 3 {
		t.Fatalf("len(ListGroupMessages(alice rejoined)) = %d, want 3", len(rejoinedView))
	}
	if !rejoinedView[0].Read || !rejoinedView[1].Read || rejoinedView[2].Read {
		t.Fatalf("rejoined read states = [%t %t %t], want [true true false]", rejoinedView[0].Read, rejoinedView[1].Read, rejoinedView[2].Read)
	}
	if rejoinedView[2].MessageID != duringGap.MessageID {
		t.Fatalf("rejoined newest message_id = %q, want %q", rejoinedView[2].MessageID, duringGap.MessageID)
	}

	current = current.Add(time.Second)
	if _, err := store.AddGroupMember(context.Background(), group.Address, "bob"); err != nil {
		t.Fatalf("AddGroupMember(bob) error = %v", err)
	}

	bobView, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "bob",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(bob new member) error = %v", err)
	}
	if len(bobView) != 3 {
		t.Fatalf("len(ListGroupMessages(bob new member)) = %d, want 3", len(bobView))
	}
	for i, message := range bobView {
		if message.Read {
			t.Fatalf("bobView[%d].Read = true, want false", i)
		}
	}
	if bobView[0].EligibleCount != 0 || bobView[0].ReadCount != 0 {
		t.Fatalf("bobView[0] counts = (%d, %d), want (0, 0)", bobView[0].ReadCount, bobView[0].EligibleCount)
	}
	if bobView[1].EligibleCount != 1 || bobView[1].ReadCount != 1 {
		t.Fatalf("bobView[1] counts = (%d, %d), want (1, 1)", bobView[1].ReadCount, bobView[1].EligibleCount)
	}
	if bobView[2].EligibleCount != 0 || bobView[2].ReadCount != 0 {
		t.Fatalf("bobView[2] counts = (%d, %d), want (0, 0)", bobView[2].ReadCount, bobView[2].EligibleCount)
	}

	for _, wantMessageID := range []string{preJoin.MessageID, duringMembership.MessageID, duringGap.MessageID} {
		message, err := store.ReceiveGroupMessage(context.Background(), GroupReceiveParams{
			Address: group.Address,
			Person:  "bob",
		})
		if err != nil {
			t.Fatalf("ReceiveGroupMessage(bob) error = %v", err)
		}
		if message.MessageID != wantMessageID {
			t.Fatalf("ReceiveGroupMessage(bob) message_id = %q, want %q", message.MessageID, wantMessageID)
		}
	}

	aliceAfterBobReads, err := store.ListGroupMessages(context.Background(), GroupListParams{
		Address: group.Address,
		Person:  "alice",
	})
	if err != nil {
		t.Fatalf("ListGroupMessages(alice after bob reads) error = %v", err)
	}
	if aliceAfterBobReads[0].ReadCount != 0 || aliceAfterBobReads[0].EligibleCount != 0 {
		t.Fatalf("aliceAfterBobReads[0] counts = (%d, %d), want (0, 0)", aliceAfterBobReads[0].ReadCount, aliceAfterBobReads[0].EligibleCount)
	}
	if aliceAfterBobReads[1].ReadCount != 1 || aliceAfterBobReads[1].EligibleCount != 1 {
		t.Fatalf("aliceAfterBobReads[1] counts = (%d, %d), want (1, 1)", aliceAfterBobReads[1].ReadCount, aliceAfterBobReads[1].EligibleCount)
	}
	if aliceAfterBobReads[2].ReadCount != 0 || aliceAfterBobReads[2].EligibleCount != 0 {
		t.Fatalf("aliceAfterBobReads[2] counts = (%d, %d), want (0, 0)", aliceAfterBobReads[2].ReadCount, aliceAfterBobReads[2].EligibleCount)
	}
}

func TestGroupReceiveConcurrentSamePersonMarksReadOnce(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	firstRuntime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(first) error = %v", err)
	}
	defer firstRuntime.Close()

	secondRuntime, err := OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(second) error = %v", err)
	}
	defer secondRuntime.Close()

	group, err := firstRuntime.Store().CreateGroup(context.Background(), "group/race")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := firstRuntime.Store().AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}
	sent := mustSendGroupMessage(t, firstRuntime.Store(), group.Address, "agent/sender", "race", "race body")

	type receiveResult struct {
		message GroupReceivedMessage
		err     error
	}

	start := make(chan struct{})
	results := make(chan receiveResult, 2)
	receive := func(store *Store) {
		<-start
		message, err := store.ReceiveGroupMessage(context.Background(), GroupReceiveParams{
			Address: group.Address,
			Person:  "alice",
		})
		results <- receiveResult{message: message, err: err}
	}

	go receive(firstRuntime.Store())
	go receive(secondRuntime.Store())
	close(start)

	var got []receiveResult
	for i := 0; i < 2; i++ {
		select {
		case result := <-results:
			got = append(got, result)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for concurrent group receive results")
		}
	}

	successCount := 0
	noMessageCount := 0
	for i, result := range got {
		switch {
		case result.err == nil:
			successCount++
			if result.message.MessageID != sent.MessageID {
				t.Fatalf("receive[%d] message_id = %q, want %q", i, result.message.MessageID, sent.MessageID)
			}
		case errors.Is(result.err, ErrNoMessage):
			noMessageCount++
		default:
			t.Fatalf("receive[%d] error = %v, want nil or ErrNoMessage", i, result.err)
		}
	}
	if successCount != 1 || noMessageCount != 1 {
		t.Fatalf("concurrent group receive results = %+v, want one success and one ErrNoMessage", got)
	}

	var readCount int
	if err := firstRuntime.DB().QueryRow(`
SELECT COUNT(*)
FROM group_reads
WHERE message_id = ?
`, sent.MessageID).Scan(&readCount); err != nil {
		t.Fatalf("count group_reads error = %v", err)
	}
	if readCount != 1 {
		t.Fatalf("group_reads count = %d, want 1", readCount)
	}
}

func TestGroupReadCLIShapesStayExplicitWithAs(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")

	createApp := NewApp(strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	if err := createApp.RunWithStateDir(context.Background(), stateDir, []string{
		"group",
		"create",
		"--group", "group/cli",
	}); err != nil {
		t.Fatalf("group create error = %v", err)
	}

	addApp := NewApp(strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	if err := addApp.RunWithStateDir(context.Background(), stateDir, []string{
		"group",
		"add-member",
		"--group", "group/cli",
		"--person", "alice",
	}); err != nil {
		t.Fatalf("group add-member error = %v", err)
	}

	sendApp := NewApp(strings.NewReader("group body"), &bytes.Buffer{}, &bytes.Buffer{})
	if err := sendApp.RunWithStateDir(context.Background(), stateDir, []string{
		"send",
		"--to", "group/cli",
		"--group",
		"--body-file", "-",
	}); err != nil {
		t.Fatalf("group send error = %v", err)
	}

	listStdout := &bytes.Buffer{}
	listApp := NewApp(strings.NewReader(""), listStdout, &bytes.Buffer{})
	if err := listApp.RunWithStateDir(context.Background(), stateDir, []string{
		"list",
		"--for", "group/cli",
		"--as", "alice",
		"--json",
	}); err != nil {
		t.Fatalf("group list error = %v", err)
	}

	var listPage Page[map[string]any]
	if err := json.Unmarshal(listStdout.Bytes(), &listPage); err != nil {
		t.Fatalf("json.Unmarshal(group list) error = %v", err)
	}
	listPayload := listPage.Items
	if len(listPayload) != 1 {
		t.Fatalf("len(group list payload) = %d, want 1", len(listPayload))
	}
	if listPayload[0]["group_address"] != "group/cli" || listPayload[0]["person"] != "alice" {
		t.Fatalf("group list payload = %v, want group metadata", listPayload[0])
	}
	if _, ok := listPayload[0]["read_count"]; !ok {
		t.Fatalf("group list payload = %v, want read_count", listPayload[0])
	}
	if _, ok := listPayload[0]["eligible_count"]; !ok {
		t.Fatalf("group list payload = %v, want eligible_count", listPayload[0])
	}
	if _, ok := listPayload[0]["delivery_id"]; ok {
		t.Fatalf("group list payload = %v, want no delivery_id", listPayload[0])
	}

	waitStdout := &bytes.Buffer{}
	waitApp := NewApp(strings.NewReader(""), waitStdout, &bytes.Buffer{})
	if err := waitApp.RunWithStateDir(context.Background(), stateDir, []string{
		"wait",
		"--for", "group/cli",
		"--as", "alice",
		"--json",
	}); err != nil {
		t.Fatalf("group wait error = %v", err)
	}

	var waitPayload map[string]any
	if err := json.Unmarshal(waitStdout.Bytes(), &waitPayload); err != nil {
		t.Fatalf("json.Unmarshal(group wait) error = %v", err)
	}
	if waitPayload["group_address"] != "group/cli" || waitPayload["person"] != "alice" {
		t.Fatalf("group wait payload = %v, want group metadata", waitPayload)
	}
	if _, ok := waitPayload["delivery_id"]; ok {
		t.Fatalf("group wait payload = %v, want no delivery_id", waitPayload)
	}

	recvStdout := &bytes.Buffer{}
	recvApp := NewApp(strings.NewReader(""), recvStdout, &bytes.Buffer{})
	if err := recvApp.RunWithStateDir(context.Background(), stateDir, []string{
		"recv",
		"--for", "group/cli",
		"--as", "alice",
		"--json",
	}); err != nil {
		t.Fatalf("group recv error = %v", err)
	}

	var recvPayload map[string]any
	if err := json.Unmarshal(recvStdout.Bytes(), &recvPayload); err != nil {
		t.Fatalf("json.Unmarshal(group recv) error = %v", err)
	}
	if recvPayload["status"] != "received" || recvPayload["as_person"] != "alice" {
		t.Fatalf("group recv payload = %v, want group metadata", recvPayload)
	}
	recvMessage, ok := recvPayload["message"].(map[string]any)
	if !ok || recvMessage["group_address"] != "group/cli" || recvMessage["person"] != "alice" {
		t.Fatalf("group recv payload = %v, want nested group message", recvPayload)
	}
	if _, ok := recvMessage["read_count"]; !ok {
		t.Fatalf("group recv payload = %v, want read_count", recvPayload)
	}
	if _, ok := recvMessage["eligible_count"]; !ok {
		t.Fatalf("group recv payload = %v, want eligible_count", recvPayload)
	}
	if _, ok := recvMessage["delivery_id"]; ok {
		t.Fatalf("group recv payload = %v, want no delivery_id", recvPayload)
	}
	if _, ok := recvMessage["lease_token"]; ok {
		t.Fatalf("group recv payload = %v, want no lease_token", recvPayload)
	}
}

func TestGroupTranscriptReadsBodiesWithoutMarkingRead(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/transcript")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("AddGroupMember(alice) error = %v", err)
	}
	if _, err := store.AddGroupMember(context.Background(), group.Address, "bob"); err != nil {
		t.Fatalf("AddGroupMember(bob) error = %v", err)
	}

	sent := mustSendGroupMessage(t, store, group.Address, "agent/alice", "first", "hello transcript")
	messages, err := store.ListGroupTranscript(context.Background(), GroupTranscriptParams{Address: group.Address})
	if err != nil {
		t.Fatalf("ListGroupTranscript() error = %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("len(ListGroupTranscript()) = %d, want 1", len(messages))
	}
	if messages[0].MessageID != sent.MessageID {
		t.Fatalf("transcript message_id = %q, want %q", messages[0].MessageID, sent.MessageID)
	}
	if messages[0].Body != "hello transcript" {
		t.Fatalf("transcript body = %q, want body", messages[0].Body)
	}
	if messages[0].DisplaySender != "agent/alice" {
		t.Fatalf("display_sender = %q, want agent/alice", messages[0].DisplaySender)
	}
	if messages[0].ReadCount != 0 || messages[0].EligibleCount != 2 {
		t.Fatalf("read counts = (%d, %d), want (0, 2)", messages[0].ReadCount, messages[0].EligibleCount)
	}

	var readCount int
	if err := runtime.DB().QueryRow(`SELECT COUNT(*) FROM group_reads`).Scan(&readCount); err != nil {
		t.Fatalf("count group_reads error = %v", err)
	}
	if readCount != 0 {
		t.Fatalf("group_reads count = %d, want 0", readCount)
	}
}

func TestGroupTranscriptDisplaySenderPrefersForwardedFromAddress(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/forwarded-transcript")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}

	_, err = store.Send(context.Background(), SendParams{
		ToAddress:            group.Address,
		FromAddress:          "agent/forwarder",
		Subject:              "forwarded",
		ContentType:          "text/plain",
		SchemaVersion:        "v1",
		ForwardedFromAddress: "agent/original",
		Body:                 []byte("forwarded body"),
		Group:                true,
	})
	if err != nil {
		t.Fatalf("Send(forwarded group) error = %v", err)
	}

	messages, err := store.ListGroupTranscript(context.Background(), GroupTranscriptParams{Address: group.Address})
	if err != nil {
		t.Fatalf("ListGroupTranscript() error = %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("len(ListGroupTranscript()) = %d, want 1", len(messages))
	}
	if messages[0].DisplaySender != "agent/original" {
		t.Fatalf("display_sender = %q, want forwarded source", messages[0].DisplaySender)
	}
	if messages[0].SenderAddress == nil || *messages[0].SenderAddress != "agent/forwarder" {
		t.Fatalf("sender_address = %v, want agent/forwarder", messages[0].SenderAddress)
	}
}

func countGroupSubscriberNotifications(t *testing.T, runtime *Runtime, groupAddress, notifyAddress, state string) int {
	t.Helper()

	var count int
	if err := runtime.DB().QueryRow(`
SELECT COUNT(*)
FROM deliveries AS d
JOIN endpoint_addresses AS ea
  ON ea.endpoint_id = d.recipient_endpoint_id
JOIN messages AS m
  ON m.message_id = d.message_id
JOIN group_messages AS gm
  ON gm.message_id = m.forwarded_message_id
JOIN groups AS g
  ON g.group_id = gm.group_id
WHERE d.state = ?
  AND ea.address = ?
  AND m.schema_version = 'group-notification/v1'
  AND g.address = ?
`, state, notifyAddress, groupAddress).Scan(&count); err != nil {
		t.Fatalf("count group subscriber notifications error = %v", err)
	}
	return count
}

func mustSendGroupMessage(t *testing.T, store *Store, toAddress, fromAddress, subject, body string) SendResult {
	t.Helper()

	result, err := store.Send(context.Background(), SendParams{
		ToAddress:     toAddress,
		FromAddress:   fromAddress,
		Subject:       subject,
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte(body),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}
	return result
}

func TestLookupLatestGroupMembershipPrefersActiveOverClosedSameTimestamp(t *testing.T) {
	t.Parallel()

	runtime, store := newLeaseTestStore(t)
	defer runtime.Close()

	group, err := store.CreateGroup(context.Background(), "group/rejoin-order")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	current := time.Date(2026, 4, 6, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return current }

	first, err := store.AddGroupMember(context.Background(), group.Address, "alice")
	if err != nil {
		t.Fatalf("AddGroupMember(first) error = %v", err)
	}
	if _, err := store.RemoveGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("RemoveGroupMember(first) error = %v", err)
	}
	rejoined, err := store.AddGroupMember(context.Background(), group.Address, "alice")
	if err != nil {
		t.Fatalf("AddGroupMember(rejoined) error = %v", err)
	}
	// Force lexicographically smaller membership_id on the active row while
	// keeping identical joined_at timestamps so tie-breaking depends on left_at.
	if _, err := runtime.DB().Exec(`
UPDATE group_memberships
SET membership_id = CASE
  WHEN membership_id = ? THEN 'gm_zzzz_closed'
  WHEN membership_id = ? THEN 'gm_0000_active'
  ELSE membership_id
END
`, first.MembershipID, rejoined.MembershipID); err != nil {
		t.Fatalf("rename membership_ids: %v", err)
	}

	latest, found, err := lookupLatestGroupMembershipByPerson(context.Background(), runtime.readDB, group.GroupID, "alice")
	if err != nil {
		t.Fatalf("lookupLatestGroupMembershipByPerson() error = %v", err)
	}
	if !found {
		t.Fatal("lookupLatestGroupMembershipByPerson() found = false, want true")
	}
	if !latest.Active || latest.LeftAt != nil || latest.MembershipID != "gm_0000_active" {
		t.Fatalf("latest membership = %+v, want active gm_0000_active", latest)
	}
}

func TestLookupLatestGroupMembershipClosedVsClosedTieBreaksByLeftAt(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/closed-tiebreak")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}

	fixed := time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return fixed }

	olderClosed, err := store.AddGroupMember(context.Background(), group.Address, "alice")
	if err != nil {
		t.Fatalf("AddGroupMember(older) error = %v", err)
	}
	fixed = fixed.Add(time.Minute)
	if _, err := store.RemoveGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("RemoveGroupMember(older) error = %v", err)
	}

	fixed = time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)
	newerClosed, err := store.AddGroupMember(context.Background(), group.Address, "alice")
	if err != nil {
		t.Fatalf("AddGroupMember(newer) error = %v", err)
	}
	fixed = fixed.Add(5 * time.Minute)
	if _, err := store.RemoveGroupMember(context.Background(), group.Address, "alice"); err != nil {
		t.Fatalf("RemoveGroupMember(newer) error = %v", err)
	}

	// Give the older closed row a larger membership_id while keeping identical
	// joined_at so tie-breaking must prefer newer left_at.
	if _, err := runtime.DB().Exec(`
UPDATE group_memberships
SET membership_id = CASE
  WHEN membership_id = ? THEN 'gm_zzzz_older_closed'
  WHEN membership_id = ? THEN 'gm_0000_newer_closed'
  ELSE membership_id
END
`, olderClosed.MembershipID, newerClosed.MembershipID); err != nil {
		t.Fatalf("rename membership_ids: %v", err)
	}

	latest, found, err := lookupLatestGroupMembershipByPerson(context.Background(), runtime.readDB, group.GroupID, "alice")
	if err != nil {
		t.Fatalf("lookupLatestGroupMembershipByPerson() error = %v", err)
	}
	if !found {
		t.Fatal("lookupLatestGroupMembershipByPerson() found = false, want true")
	}
	if latest.Active || latest.LeftAt == nil || latest.MembershipID != "gm_0000_newer_closed" {
		t.Fatalf("latest membership = %+v, want closed gm_0000_newer_closed", latest)
	}
}

func TestReceiveGroupMessageConcurrentReadCountAccurate(t *testing.T) {
	t.Parallel()

	runtime, err := OpenRuntime(context.Background(), filepath.Join(t.TempDir(), "waypost-state"))
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	defer runtime.Close()

	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/concurrent-reads")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	for _, person := range []string{"alice", "bob"} {
		if _, err := store.AddGroupMember(context.Background(), group.Address, person); err != nil {
			t.Fatalf("AddGroupMember(%s) error = %v", person, err)
		}
	}
	if _, err := store.Send(context.Background(), SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent/sender",
		Subject:       "sync read",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("concurrent payload"),
		Group:         true,
	}); err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}

	counts := make(chan int, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, person := range []string{"alice", "bob"} {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			<-start
			msg, err := store.ReceiveGroupMessage(context.Background(), GroupReceiveParams{
				Address: group.Address,
				Person:  p,
			})
			if err != nil {
				errs <- err
				return
			}
			counts <- msg.ReadCount
		}(person)
	}
	close(start)
	wg.Wait()
	close(errs)
	close(counts)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent ReceiveGroupMessage() error = %v", err)
		}
	}

	got := make([]int, 0, 2)
	for c := range counts {
		got = append(got, c)
	}
	if len(got) != 2 || !((got[0] == 1 && got[1] == 2) || (got[0] == 2 && got[1] == 1)) {
		t.Fatalf("concurrent ReadCount values = %v, want permutation of [1 2]", got)
	}
}
