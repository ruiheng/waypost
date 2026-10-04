package mcpserver

import (
	"context"
	"strings"
)

const (
	genericAgentDeckNestedParentDetail     = "generic agent-deck create does not support a parent that is itself a child session"
	genericAgentDeckEmptyParentGroupDetail = "generic agent-deck create requires a parent with a non-empty group"
	genericAgentDeckGroupMismatchDetail    = "refreshed agent-deck session group does not match the parent group snapshot"
)

// launchAgentDeckSession runs agent-deck launch and parses its receipt. A nil
// result with nil error means the host returned unusable output and the
// caller must report create recovery rather than a generic failure.
func (m *sessionManager) launchAgentDeckSession(ctx context.Context, name, launchValue, group, parentSessionID, workdir string, transitionNotify, assertDone bool) (*hostSessionData, error) {
	launchArgs := []string{
		"agent-deck", "launch", "--json", "--title", name, "--cmd", launchValue,
		"--group", group, "--parent", parentSessionID,
	}
	launchArgs = append(launchArgs, agentDeckNotificationArgs(transitionNotify, assertDone)...)
	launchArgs = append(launchArgs, workdir)
	result, err := runRedactedCommand(ctx, m.runner, launchArgs, runOptions{}, "generic agent-deck session create")
	if err != nil {
		return nil, err
	}
	legacyData, parseErr := parseSessionData(result.Stdout, "agent-deck launch")
	if parseErr != nil || legacyData == nil || strings.TrimSpace(legacyData.ID) == "" {
		return nil, nil
	}
	return hostSessionFromAgentDeck(legacyData), nil
}

func hostSessionFromAgentDeck(data *sessionData) *hostSessionData {
	if data == nil {
		return nil
	}
	return &hostSessionData{
		Host:            sessionHostAgentDeck,
		ID:              strings.TrimSpace(data.ID),
		Name:            strings.TrimSpace(data.Title),
		Status:          strings.TrimSpace(data.Status),
		Group:           strings.TrimSpace(data.Group),
		Path:            strings.TrimSpace(data.Path),
		ParentSessionID: strings.TrimSpace(data.ParentSessionID),
	}
}

// agentDeckNotificationArgs maps the create-tool notification switches onto
// agent-deck launch flags. Both default to suppressed: waypost is the
// notification channel for sessions this tool creates, so child-to-parent
// transition events and the completion-sentinel instruction stay off unless
// a caller explicitly opts back in.
func agentDeckNotificationArgs(transitionNotify, assertDone bool) []string {
	var args []string
	if !transitionNotify {
		args = append(args, "--no-transition-notify")
	}
	if !assertDone {
		args = append(args, "--no-assert-done")
	}
	return args
}
