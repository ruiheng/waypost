package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The generic tools intentionally expose only host-neutral session inputs,
// plus the two notification switches that map onto Agent Deck launch flags;
// transition_notify and assert_done are ignored for Thurbox.
type sessionCreateInput struct {
	Host             string `json:"host,omitempty"`
	SessionName      string `json:"session_name"`
	Workdir          string `json:"workdir"`
	ParentSessionID  string `json:"parent_session_id"`
	FullCommandLine  string `json:"full_command_line,omitempty"`
	ThurboxAgentKey  string `json:"thurbox_agent_key,omitempty"`
	TransitionNotify bool   `json:"transition_notify,omitempty"`
	AssertDone       bool   `json:"assert_done,omitempty"`
}

type sessionRequireInput struct {
	Host        string   `json:"host,omitempty"`
	SessionID   string   `json:"session_id,omitempty"`
	SessionRef  string   `json:"session_ref,omitempty"`
	Sessions    []string `json:"sessions,omitempty"`
	Workdir     string   `json:"workdir"`
	AutoRestart *bool    `json:"auto_restart,omitempty"`
}

func (s *Service) registerSessionTools(server *mcp.Server) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "session_create",
		Description: "Create a session through Agent Deck or Thurbox in an explicit workdir. Provide the selected host's launch value with full_command_line or thurbox_agent_key. Agent Deck launches suppress child-to-parent notifications by default; transition_notify and assert_done keep them (Agent Deck only).",
	}, s.sessionCreate)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "session_require",
		Description: "Find or restart existing sessions through Agent Deck or Thurbox in an explicit workdir. Never creates sessions; auto_restart defaults to true.",
	}, s.sessionRequire)
}

func (s *Service) sessionCreate(ctx context.Context, _ *mcp.CallToolRequest, input sessionCreateInput) (*mcp.CallToolResult, map[string]any, error) {
	host, err := s.sessions.selectSessionHost(ctx, input.Host)
	if err != nil {
		return nil, nil, err
	}
	out, err := s.sessions.createHostSession(ctx, host, input.SessionName, input.Workdir, input.ParentSessionID, input.FullCommandLine, input.ThurboxAgentKey, agentDeckSwitches{
		transitionNotify: input.TransitionNotify,
		assertDone:       input.AssertDone,
	})
	if err != nil {
		return nil, nil, err
	}
	return s.toolResult(ctx, out)
}

func (s *Service) sessionRequire(ctx context.Context, req *mcp.CallToolRequest, input sessionRequireInput) (*mcp.CallToolResult, map[string]any, error) {
	batch, err := validateGenericRequireSessionArgs(req, input)
	if err != nil {
		return nil, nil, err
	}
	host, err := s.sessions.selectSessionHost(ctx, input.Host)
	if err != nil {
		return nil, nil, err
	}
	if !batch {
		identifier := input.SessionRef
		selectorKind := hostSessionSelectorRef
		if strings.TrimSpace(input.SessionID) != "" {
			identifier = input.SessionID
			selectorKind = hostSessionSelectorID
		}
		out, err := s.sessions.requireHostSession(ctx, host, identifier, input.Workdir, selectorKind, autoRestartEnabled(input.AutoRestart))
		if err != nil {
			return nil, nil, err
		}
		return s.toolResult(ctx, out)
	}

	workdir, err := canonicalizeTargetWorkdir(input.Workdir, "requiring")
	if err != nil {
		return nil, nil, err
	}
	results := make([]map[string]any, 0, len(input.Sessions))
	for _, session := range input.Sessions {
		out, err := s.sessions.requireHostSessionWithCanonicalWorkdir(ctx, host, session, input.Workdir, workdir, hostSessionSelectorRef, autoRestartEnabled(input.AutoRestart))
		if err != nil {
			out = genericRequireErrorResult(host, session, err)
		}
		results = append(results, out)
	}
	return s.toolResult(ctx, map[string]any{"host": string(host), "results": results})
}

func validateGenericRequireSessionArgs(req *mcp.CallToolRequest, input sessionRequireInput) (bool, error) {
	if req == nil || len(req.Params.Arguments) == 0 {
		return false, errors.New("session_require requires exactly one of session_id, session_ref, or sessions")
	}

	var rawArgs map[string]json.RawMessage
	if err := json.Unmarshal(req.Params.Arguments, &rawArgs); err != nil {
		return false, fmt.Errorf("invalid tool arguments: %w", err)
	}
	_, hasSessionID := rawArgs["session_id"]
	_, hasSessionRef := rawArgs["session_ref"]
	_, hasSessions := rawArgs["sessions"]
	count := 0
	for _, present := range []bool{hasSessionID, hasSessionRef, hasSessions} {
		if present {
			count++
		}
	}
	if count != 1 {
		return false, errors.New("session_require requires exactly one of session_id, session_ref, or sessions")
	}
	if hasSessions && len(input.Sessions) == 0 {
		return false, errors.New("session_require sessions must contain at least one session")
	}
	if hasSessionID && strings.TrimSpace(input.SessionID) == "" {
		return false, errors.New("session_require session_id must not be empty")
	}
	if hasSessionRef && strings.TrimSpace(input.SessionRef) == "" {
		return false, errors.New("session_require session_ref must not be empty")
	}
	for _, session := range input.Sessions {
		if strings.TrimSpace(session) == "" {
			return false, errors.New("session_require sessions must not contain an empty session")
		}
	}
	return hasSessions, nil
}

func genericRequireErrorResult(host sessionHost, sessionRef string, err error) map[string]any {
	return map[string]any{
		"host":            string(host),
		"status":          "error",
		"session_ref":     sessionRef,
		"started_session": false,
		"error":           err.Error(),
	}
}

func autoRestartEnabled(value *bool) bool {
	return value == nil || *value
}
