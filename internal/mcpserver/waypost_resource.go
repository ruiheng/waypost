package mcpserver

import (
	"context"
	"encoding/json"
	"log"
	"slices"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type resourceSubscriptionState struct {
	mu       sync.Mutex
	sessions map[*mcp.ServerSession]bool
}

func newResourceSubscriptionState() *resourceSubscriptionState {
	return &resourceSubscriptionState{
		sessions: map[*mcp.ServerSession]bool{},
	}
}

func (s *Service) registerWaypostOverviewResource(server *mcp.Server) {
	server.AddResource(&mcp.Resource{
		URI:         waypostOverviewURI,
		Name:        "waypost-bound-overview",
		Title:       "Waypost Overview",
		Description: "Claimable Waypost work in this MCP server's bound scope.",
		MIMEType:    "application/json",
	}, s.readWaypostOverviewResource)
}

func (s *Service) readWaypostOverviewResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	if req == nil || req.Params == nil || req.Params.URI != waypostOverviewURI {
		uri := ""
		if req != nil && req.Params != nil {
			uri = req.Params.URI
		}
		return nil, mcp.ResourceNotFoundError(uri)
	}

	overview, err := s.waypostOverviewSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(overview)
	if err != nil {
		return nil, err
	}
	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{{
			URI:      waypostOverviewURI,
			MIMEType: "application/json",
			Text:     string(body),
		}},
	}, nil
}

func (s *Service) subscribeResource(_ context.Context, req *mcp.SubscribeRequest) error {
	if req == nil || req.Params == nil || req.Params.URI != waypostOverviewURI {
		uri := ""
		if req != nil && req.Params != nil {
			uri = req.Params.URI
		}
		return mcp.ResourceNotFoundError(uri)
	}
	if req.Session != nil {
		s.overviewSubscriptions.add(req.Session)
	}
	return nil
}

func (s *Service) unsubscribeResource(_ context.Context, req *mcp.UnsubscribeRequest) error {
	if req == nil || req.Params == nil || req.Params.URI != waypostOverviewURI {
		uri := ""
		if req != nil && req.Params != nil {
			uri = req.Params.URI
		}
		return mcp.ResourceNotFoundError(uri)
	}
	if req.Session != nil {
		s.overviewSubscriptions.remove(req.Session)
	}
	return nil
}

func (s *Service) waypostOverviewSnapshot(ctx context.Context) (map[string]any, error) {
	bound, err := s.sessions.boundState(ctx)
	if err != nil {
		return nil, err
	}
	visible, err := s.visibleWaypostSnapshot(ctx, bound.BoundAddresses)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"bound_addresses":          bound.BoundAddresses,
		"default_sender":           nilIfEmpty(bound.DefaultSender),
		"has_claimable_delivery":   visible.QueuedVisibleCount > 0,
		"claimable_delivery_count": visible.QueuedVisibleCount,
		"oldest_claimable_at":      nilIfEmpty(visible.OldestEligibleAt),
	}, nil
}

func (s *Service) emitWaypostOverviewUpdated(ctx context.Context) notificationOutcome {
	server := s.existingServer()
	if !s.overviewSubscriptions.hasLiveSubscribers(server) {
		return notificationOutcome{Status: "unsupported", Scheme: string(WakeHintMCPResourceUpdated)}
	}
	if err := server.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: waypostOverviewURI}); err != nil {
		log.Printf("mcpserver waypost_overview_update_failed uri=%s err=%v", waypostOverviewURI, err)
		return notificationOutcome{
			Status: "failed",
			Scheme: string(WakeHintMCPResourceUpdated),
			Err:    err,
		}
	}
	return notificationOutcome{Status: "sent", Scheme: string(WakeHintMCPResourceUpdated)}
}

func (s *Service) emitWaypostOverviewUpdatedBestEffort(ctx context.Context) {
	_ = s.emitWaypostOverviewUpdated(ctx)
}

func (s *resourceSubscriptionState) add(session *mcp.ServerSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[session] = true
}

func (s *resourceSubscriptionState) remove(session *mcp.ServerSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, session)
}

func (s *resourceSubscriptionState) hasLiveSubscribers(server *mcp.Server) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sessions) == 0 {
		return false
	}
	if server == nil {
		return false
	}
	activeSessions := slices.Collect(server.Sessions())
	if len(activeSessions) == 0 {
		s.sessions = map[*mcp.ServerSession]bool{}
		return false
	}
	activeSet := make(map[*mcp.ServerSession]bool, len(activeSessions))
	for _, session := range activeSessions {
		activeSet[session] = true
	}
	for session := range s.sessions {
		if !activeSet[session] {
			delete(s.sessions, session)
		}
	}
	return len(s.sessions) > 0
}
