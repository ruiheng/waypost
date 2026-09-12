package mcpserver

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	waypostRecvNoMessageMinInterval   = 15 * time.Second
	waypostRecvNoMessageWarningWindow = 3 * time.Minute
)

const waypostRecvPollingWarning = "The previous waypost_recv result was no_message; avoid meaningless polling and wait for a delivery notification before calling waypost_recv again."

type waypostRecvSessionState struct {
	mu            sync.Mutex
	lastNoMessage time.Time
	hasNoMessage  bool
	resultVersion atomic.Uint64
	inFlight      atomic.Int32
}

// waypostRecvGuard tracks receive results per MCP connection. A connection
// that just observed no_message must pause before asking waypost_recv again;
// this keeps independent MCP clients from throttling one another.
type waypostRecvGuard struct {
	mu       sync.Mutex
	sessions map[weak.Pointer[mcp.ServerSession]]*waypostRecvSessionState
}

type waypostRecvGuardLease struct {
	state  *waypostRecvSessionState
	active bool
}

func newWaypostRecvGuard() *waypostRecvGuard {
	return &waypostRecvGuard{
		sessions: make(map[weak.Pointer[mcp.ServerSession]]*waypostRecvSessionState),
	}
}

func (s *Service) waypostRecvActiveSessions() map[*mcp.ServerSession]struct{} {
	s.serverMu.Lock()
	server := s.server
	s.serverMu.Unlock()
	if server == nil {
		// An uninitialized Service has no live MCP sessions. Returning an empty
		// snapshot lets the guard discard states for other synthetic/old
		// sessions while preserving the current request's state.
		return map[*mcp.ServerSession]struct{}{}
	}
	active := make(map[*mcp.ServerSession]struct{})
	for session := range server.Sessions() {
		active[session] = struct{}{}
	}
	return active
}

func (g *waypostRecvGuard) begin(req *mcp.CallToolRequest, now func() time.Time, activeSessions map[*mcp.ServerSession]struct{}) (*waypostRecvGuardLease, error) {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	current := now()

	var session *mcp.ServerSession
	if req != nil {
		session = req.Session
	}
	key := weak.Make(session)
	g.prune(session, activeSessions)

	g.mu.Lock()
	state := g.sessions[key]
	if state == nil {
		state = &waypostRecvSessionState{}
		g.sessions[key] = state
	}
	// Capture the state version before this receive is admitted. If another
	// receive completes while this call is waiting for state.mu, the version
	// check below keeps that later result from throttling this already-started
	// call.
	startVersion := state.resultVersion.Load()
	// Pin the state in the map before releasing the global lock. The timing
	// mutex below is deliberately not nested under g.mu and is released before
	// the caller starts reconciliation or receive I/O.
	state.inFlight.Add(1)
	g.mu.Unlock()

	state.mu.Lock()
	if state.resultVersion.Load() == startVersion && state.hasNoMessage {
		elapsed := current.Sub(state.lastNoMessage)
		if elapsed >= 0 && elapsed < waypostRecvNoMessageMinInterval {
			state.mu.Unlock()
			state.inFlight.Add(-1)
			return nil, fmt.Errorf("the previous waypost_recv result was no_message; wait for a delivery notification instead of polling")
		}
	}
	state.mu.Unlock()
	return &waypostRecvGuardLease{
		state:  state,
		active: true,
	}, nil
}

func (g *waypostRecvGuard) prune(currentSession *mcp.ServerSession, activeSessions map[*mcp.ServerSession]struct{}) {
	// Do not acquire a session mutex while holding g.mu. An in-flight receive
	// must never delay map access for another connection.
	g.mu.Lock()
	for key, state := range g.sessions {
		session := key.Value()
		inactive := false
		if session == nil {
			inactive = true
		} else if activeSessions != nil && session != currentSession {
			_, active := activeSessions[session]
			inactive = !active
		}
		if inactive {
			if state.inFlight.Load() == 0 {
				delete(g.sessions, key)
			}
		}
	}
	g.mu.Unlock()
}

func (l *waypostRecvGuardLease) finish(status string, now time.Time) bool {
	if l == nil || !l.active {
		return false
	}
	warn := false
	l.state.mu.Lock()
	switch status {
	case "no_message":
		if l.state.hasNoMessage {
			elapsed := now.Sub(l.state.lastNoMessage)
			warn = elapsed >= 0 && elapsed < waypostRecvNoMessageWarningWindow
		}
		l.state.lastNoMessage = now
		l.state.hasNoMessage = true
		l.state.resultVersion.Add(1)
	case "":
		// An errored call has no new result, so preserve the previous result.
	default:
		l.state.lastNoMessage = time.Time{}
		l.state.hasNoMessage = false
		l.state.resultVersion.Add(1)
	}
	l.state.mu.Unlock()
	l.state.inFlight.Add(-1)
	l.active = false
	return warn
}
