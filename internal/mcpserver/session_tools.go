package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type agentDeckCreateSessionInput struct {
	EnsureTitle          string `json:"ensure_title,omitempty"`
	EnsureCmd            string `json:"ensure_cmd,omitempty"`
	ParentSessionID      string `json:"parent_session_id,omitempty"`
	GroupPath            string `json:"group_path,omitempty"`
	GroupParentSessionID string `json:"group_parent_session_id,omitempty"`
	ChildGroupName       string `json:"child_group_name,omitempty"`
	NoParentLink         bool   `json:"no_parent_link,omitempty"`
	Workdir              string `json:"workdir"`
	StartupInstruction   string `json:"startup_instruction,omitempty"`
}

type agentDeckRequireSessionInput struct {
	SessionID   string   `json:"session_id,omitempty"`
	SessionRef  string   `json:"session_ref,omitempty"`
	Sessions    []string `json:"sessions,omitempty"`
	Workdir     string   `json:"workdir"`
	AutoRestart *bool    `json:"auto_restart,omitempty"`
}

// The generic tools intentionally expose only host-neutral session inputs.
// Agent Deck-specific fields remain on its dedicated create/require tools.
type sessionCreateInput struct {
	Host            string `json:"host,omitempty"`
	SessionName     string `json:"session_name"`
	Workdir         string `json:"workdir"`
	ParentSessionID string `json:"parent_session_id"`
	FullCommandLine string `json:"full_command_line,omitempty"`
	ThurboxAgentKey string `json:"thurbox_agent_key,omitempty"`
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
		Description: "Create a session through Agent Deck or Thurbox in an explicit workdir. Provide the selected host's launch value with full_command_line or thurbox_agent_key.",
	}, s.sessionCreate)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "session_require",
		Description: "Find or restart existing sessions through Agent Deck or Thurbox in an explicit workdir. Never creates sessions; auto_restart defaults to true.",
	}, s.sessionRequire)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "agent_deck_create_session",
		Description: "Create a new Agent Deck session in an explicit workdir. Supports group or parent linkage, detachment, and a startup instruction.",
	}, s.agentDeckCreateSession)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "agent_deck_require_session",
		Description: "Find or restart Agent Deck sessions by ID or ref in an explicit workdir. Never creates sessions; auto_restart defaults to true.",
	}, s.agentDeckRequireSession)
}

func (s *Service) sessionCreate(ctx context.Context, _ *mcp.CallToolRequest, input sessionCreateInput) (*mcp.CallToolResult, map[string]any, error) {
	host, err := s.sessions.selectSessionHost(ctx, input.Host)
	if err != nil {
		return nil, nil, err
	}
	out, err := s.sessions.createHostSession(ctx, host, input.SessionName, input.Workdir, input.ParentSessionID, input.FullCommandLine, input.ThurboxAgentKey)
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

func (s *Service) agentDeckCreateSession(ctx context.Context, _ *mcp.CallToolRequest, input agentDeckCreateSessionInput) (*mcp.CallToolResult, map[string]any, error) {
	out, err := s.sessions.createSession(ctx, input)
	if err != nil {
		return nil, nil, err
	}
	return s.toolResult(ctx, out)
}

func (s *Service) agentDeckRequireSession(ctx context.Context, req *mcp.CallToolRequest, input agentDeckRequireSessionInput) (*mcp.CallToolResult, map[string]any, error) {
	batch, err := validateRequireSessionArgs(req, input)
	if err != nil {
		return nil, nil, err
	}
	if batch {
		workdir, err := canonicalizeTargetWorkdir(input.Workdir, "requiring")
		if err != nil {
			return nil, nil, err
		}
		results := make([]map[string]any, 0, len(input.Sessions))
		for _, session := range input.Sessions {
			out, err := s.sessions.requireSessionWithCanonicalWorkdir(ctx, agentDeckRequireSessionInput{
				SessionRef:  session,
				Workdir:     input.Workdir,
				AutoRestart: input.AutoRestart,
			}, workdir)
			if err != nil {
				out = map[string]any{
					"status":      "error",
					"session_ref": session,
					"error":       err.Error(),
				}
			}
			results = append(results, out)
		}
		return s.toolResult(ctx, map[string]any{"results": results})
	}
	out, err := s.sessions.requireSession(ctx, input)
	if err != nil {
		return nil, nil, err
	}
	return s.toolResult(ctx, out)
}

func validateRequireSessionArgs(req *mcp.CallToolRequest, input agentDeckRequireSessionInput) (bool, error) {
	if req == nil || len(req.Params.Arguments) == 0 {
		return false, nil
	}

	var rawArgs map[string]json.RawMessage
	if err := json.Unmarshal(req.Params.Arguments, &rawArgs); err != nil {
		return false, fmt.Errorf("invalid tool arguments: %w", err)
	}

	allowedFields := map[string]bool{
		"session_id":   true,
		"session_ref":  true,
		"sessions":     true,
		"workdir":      true,
		"auto_restart": true,
	}
	unexpected := make([]string, 0, len(rawArgs))
	for field := range rawArgs {
		if !allowedFields[field] {
			unexpected = append(unexpected, field)
		}
	}
	if len(unexpected) > 0 {
		slices.Sort(unexpected)
		return false, fmt.Errorf("agent_deck_require_session does not accept extra fields: %s", strings.Join(unexpected, ", "))
	}

	_, hasSessions := rawArgs["sessions"]
	_, hasSessionID := rawArgs["session_id"]
	_, hasSessionRef := rawArgs["session_ref"]
	if !hasSessions {
		return false, nil
	}
	if hasSessionID || hasSessionRef {
		return false, errors.New("agent_deck_require_session sessions cannot be combined with session_id or session_ref")
	}
	if len(input.Sessions) == 0 {
		return false, errors.New("agent_deck_require_session sessions must contain at least one session")
	}
	return true, nil
}
