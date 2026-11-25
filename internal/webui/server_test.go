package webui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ruiheng/waypost/internal/waypost"
)

type blockingReader struct {
	started chan struct{}
}

func (r blockingReader) Read([]byte) (int, error) {
	close(r.started)
	select {}
}

func TestGroupsEndpointListsGroups(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	if _, err := runtime.Store().CreateGroup(context.Background(), "group/web"); err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	runtime.Close()

	handler := NewServer(stateDir, "group/web")
	defer handler.Close()
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Get(server.URL + "/api/groups")
	if err != nil {
		t.Fatalf("GET /api/groups error = %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}

	var payload struct {
		DefaultGroup string                `json:"default_group"`
		Groups       []waypost.GroupRecord `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("Decode(groups) error = %v", err)
	}
	if payload.DefaultGroup != "group/web" {
		t.Fatalf("default_group = %q, want group/web", payload.DefaultGroup)
	}
	if len(payload.Groups) != 1 || payload.Groups[0].Address != "group/web" {
		t.Fatalf("groups = %+v, want group/web", payload.Groups)
	}
}

func TestTranscriptHTMLKeepsHistoryBeforeLiveMessages(t *testing.T) {
	t.Parallel()

	history := strings.Index(indexHTML, `id="history-messages"`)
	more := strings.Index(indexHTML, `id="more-messages"`)
	live := strings.Index(indexHTML, `id="live-messages"`)
	if history < 0 || more < 0 || live < 0 || !(history < more && more < live) {
		t.Fatalf("transcript containers are not ordered history, more, live")
	}
	if !strings.Contains(indexHTML, "appendMessage(message, historyMessagesEl, false)") {
		t.Fatal("history pages are not appended to the history container")
	}
	if !strings.Contains(indexHTML, "appendMessage(message, liveMessagesEl, true)") {
		t.Fatal("SSE messages are not appended to the live container")
	}
}

func TestLoadingMoreGroupsDoesNotResetTranscript(t *testing.T) {
	t.Parallel()

	start := strings.Index(indexHTML, "async function loadGroups(cursor)")
	if start < 0 {
		t.Fatal("loadGroups function not found")
	}
	end := strings.Index(indexHTML[start:], "function renderGroups(groups)")
	if end < 0 {
		t.Fatal("renderGroups function not found")
	}
	functionBody := indexHTML[start : start+end]
	if !strings.Contains(functionBody, "if (initialLoad) {") {
		t.Fatal("loadGroups does not isolate transcript initialization to the first page")
	}
	if strings.Contains(functionBody, "if (initialLoad && selectedGroup)") {
		t.Fatal("loadGroups still sends subsequent pages through the empty-state branch")
	}
	if strings.Index(functionBody, "if (initialLoad) {") > strings.Index(functionBody, "await loadTranscript(selectedGroup)") {
		t.Fatal("loadTranscript is not guarded by initialLoad")
	}
	for _, fragment := range []string{
		"if (groupsPageLoading) return",
		"groupsPageLoading = true",
		"moreGroupsEl.disabled = true",
		"groupsPageLoading = false",
		"moreGroupsEl.disabled = false",
	} {
		if !strings.Contains(functionBody, fragment) {
			t.Fatalf("group pagination request guard missing %q", fragment)
		}
	}
}

func TestTranscriptRequestsDiscardStaleGroupResponses(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		"const generation = ++transcriptGeneration",
		"cancelTranscriptRequests()",
		"const group = selectedGroup",
		"const generation = transcriptGeneration",
		"if (!isCurrentTranscript(group, generation)) return",
		"{signal: controller.signal}",
		"source !== eventSource || !isCurrentTranscript(group, generation)",
	} {
		if !strings.Contains(indexHTML, fragment) {
			t.Fatalf("transcript stale-response guard missing %q", fragment)
		}
	}
}

func TestGroupsEndpointEnforcesPageLimit(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	for _, address := range []string{"group/a", "group/b"} {
		if _, err := runtime.Store().CreateGroup(context.Background(), address); err != nil {
			t.Fatalf("CreateGroup(%q) error = %v", address, err)
		}
	}
	runtime.Close()

	handler := NewServer(stateDir, "")
	defer handler.Close()
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Get(server.URL + "/api/groups?limit=1")
	if err != nil {
		t.Fatalf("GET /api/groups error = %v", err)
	}
	defer response.Body.Close()
	var page struct {
		Items      []waypost.GroupRecord `json:"items"`
		NextCursor string                `json:"next_cursor"`
	}
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatalf("Decode(groups page) error = %v", err)
	}
	if len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("groups page = %+v, want one item and cursor", page)
	}

	tooLarge, err := http.Get(server.URL + "/api/groups?limit=101")
	if err != nil {
		t.Fatalf("GET /api/groups?limit=101 error = %v", err)
	}
	defer tooLarge.Body.Close()
	if tooLarge.StatusCode != http.StatusBadRequest {
		t.Fatalf("limit=101 status = %d, want 400", tooLarge.StatusCode)
	}

	zero, err := http.Get(server.URL + "/api/groups?limit=0")
	if err != nil {
		t.Fatalf("GET /api/groups?limit=0 error = %v", err)
	}
	defer zero.Body.Close()
	if zero.StatusCode != http.StatusBadRequest {
		t.Fatalf("limit=0 status = %d, want 400", zero.StatusCode)
	}
}

func TestPromptForURLActionOpensURL(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var opened string
	promptForURLAction(context.Background(), Options{
		Stdin:       strings.NewReader("o\n"),
		Stdout:      &stdout,
		Interactive: true,
		OpenURL: func(_ context.Context, webURL string) error {
			opened = webURL
			return nil
		},
	}, "http://127.0.0.1:12345")

	if opened != "http://127.0.0.1:12345" {
		t.Fatalf("opened URL = %q, want URL", opened)
	}
	if !strings.Contains(stdout.String(), "opened http://127.0.0.1:12345") {
		t.Fatalf("stdout = %q, want opened notice", stdout.String())
	}
}

func TestPromptForURLActionCopiesURL(t *testing.T) {
	t.Parallel()

	var copied string
	promptForURLAction(context.Background(), Options{
		Stdin:       strings.NewReader("c\n"),
		Stdout:      &bytes.Buffer{},
		Interactive: true,
		CopyText: func(_ context.Context, text string) error {
			copied = text
			return nil
		},
	}, "http://127.0.0.1:23456")

	if copied != "http://127.0.0.1:23456" {
		t.Fatalf("copied URL = %q, want URL", copied)
	}
}

func TestPromptForURLActionSkipsWhenNonInteractive(t *testing.T) {
	t.Parallel()

	called := false
	promptForURLAction(context.Background(), Options{
		Stdin:       strings.NewReader("o\n"),
		Stdout:      &bytes.Buffer{},
		Interactive: false,
		OpenURL: func(context.Context, string) error {
			called = true
			return nil
		},
	}, "http://127.0.0.1:34567")

	if called {
		t.Fatal("non-interactive prompt called opener")
	}
}

func TestPromptForURLActionReturnsWhenContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	reader := blockingReader{started: make(chan struct{})}
	done := make(chan struct{})

	go func() {
		defer close(done)
		promptForURLAction(ctx, Options{
			Stdin:       reader,
			Stdout:      &bytes.Buffer{},
			Interactive: true,
		}, "http://127.0.0.1:45678")
	}()

	<-reader.started
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("promptForURLAction did not return after context cancellation")
	}
}

func TestRunWarnsForNonLoopbackListenAddress(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout bytes.Buffer

	err := Run(ctx, Options{Listen: "0.0.0.0:0", Stdout: &stdout})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if !strings.Contains(stdout.String(), "warning: group web read-only UI is listening beyond loopback") {
		t.Fatalf("stdout = %q, want non-loopback warning", stdout.String())
	}
}

func TestRunDoesNotWarnForLoopbackListenAddress(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout bytes.Buffer

	err := Run(ctx, Options{Listen: "127.0.0.1:0", Stdout: &stdout})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if strings.Contains(stdout.String(), "warning:") {
		t.Fatalf("stdout = %q, want no warning", stdout.String())
	}
}

func TestCopyTextWithSystemCommandContinuesAfterCommandFailure(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux clipboard fallback order test")
	}

	binDir := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "copied.txt")
	if err := os.WriteFile(filepath.Join(binDir, "wl-copy"), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatalf("write wl-copy stub error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "xclip"), []byte("#!/bin/sh\n/bin/cat > \"$COPY_OUT\"\n"), 0o755); err != nil {
		t.Fatalf("write xclip stub error = %v", err)
	}
	t.Setenv("PATH", binDir)
	t.Setenv("COPY_OUT", outputPath)

	if err := copyTextWithSystemCommand(context.Background(), "http://127.0.0.1:12345"); err != nil {
		t.Fatalf("copyTextWithSystemCommand() error = %v", err)
	}
	copied, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read copied output error = %v", err)
	}
	if string(copied) != "http://127.0.0.1:12345" {
		t.Fatalf("copied text = %q, want URL", string(copied))
	}
}

func TestTranscriptEndpointReturnsBodies(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/web-transcript")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if _, err := store.Send(context.Background(), waypost.SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent/sender",
		Subject:       "subject",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("web body"),
		Group:         true,
	}); err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}
	runtime.Close()

	handler := NewServer(stateDir, "")
	defer handler.Close()
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Get(server.URL + "/api/groups/" + url.PathEscape(group.Address) + "/transcript")
	if err != nil {
		t.Fatalf("GET transcript error = %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}

	var payload struct {
		Messages []waypost.GroupTranscriptMessage `json:"items"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("Decode(transcript) error = %v", err)
	}
	if len(payload.Messages) != 1 {
		t.Fatalf("len(messages) = %d, want 1", len(payload.Messages))
	}
	if payload.Messages[0].Body != "web body" {
		t.Fatalf("body = %q, want web body", payload.Messages[0].Body)
	}
	if payload.Messages[0].DisplaySender != "agent/sender" {
		t.Fatalf("display_sender = %q, want agent/sender", payload.Messages[0].DisplaySender)
	}
}

func TestServerReusesWaypostRuntime(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	if _, err := runtime.Store().CreateGroup(context.Background(), "group/web-reuse"); err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	runtime.Close()

	handler := NewServer(stateDir, "")
	defer handler.Close()
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, path := range []string{
		"/api/groups",
		"/api/groups/" + url.PathEscape("group/web-reuse") + "/transcript",
	} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s error = %v", path, err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, response.StatusCode)
		}
	}

	first := handler.runtime
	if first == nil {
		t.Fatal("server did not open waypost runtime")
	}
	response, err := http.Get(server.URL + "/api/groups")
	if err != nil {
		t.Fatalf("GET /api/groups error = %v", err)
	}
	response.Body.Close()
	if handler.runtime != first {
		t.Fatal("server opened a new waypost runtime instead of reusing the existing one")
	}
}

func TestEventsEndpointStreamsNewMessages(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/web-events")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	runtime.Close()

	handler := NewServer(stateDir, "")
	defer handler.Close()
	server := httptest.NewServer(handler)
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL+"/api/groups/"+url.PathEscape(group.Address)+"/events", nil)
	if err != nil {
		t.Fatalf("NewRequest(events) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request = request.WithContext(ctx)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("GET events error = %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}

	secondRuntime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(second) error = %v", err)
	}
	if _, err := secondRuntime.Store().Send(context.Background(), waypost.SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent/sse",
		Subject:       "sse",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("streamed"),
		Group:         true,
	}); err != nil {
		t.Fatalf("Send(group) error = %v", err)
	}
	secondRuntime.Close()

	lines := make(chan string, 8)
	go func() {
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()

	deadline := time.After(3 * time.Second)
	for {
		select {
		case line := <-lines:
			if strings.HasPrefix(line, "data: ") && strings.Contains(line, `"body":"streamed"`) {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for streamed message")
		}
	}
}

func TestEventsEndpointReplaysBoundedHistoryForUnknownAfter(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/web-events-replay")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	foreignGroup, err := store.CreateGroup(context.Background(), "group/web-events-foreign")
	if err != nil {
		t.Fatalf("CreateGroup(foreign) error = %v", err)
	}
	for index := 0; index < waypost.MaxPageSize+1; index++ {
		_, err := store.Send(context.Background(), waypost.SendParams{
			ToAddress:     group.Address,
			FromAddress:   "agent/sse",
			Subject:       fmt.Sprintf("history-%03d", index),
			ContentType:   "text/plain",
			SchemaVersion: "v1",
			Body:          []byte(fmt.Sprintf("history-body-%03d", index)),
			Group:         true,
		})
		if err != nil {
			t.Fatalf("Send(history %d) error = %v", index, err)
		}
	}
	lastMessageID, err := store.LatestGroupMessageID(context.Background(), group.Address)
	if err != nil {
		t.Fatalf("LatestGroupMessageID() error = %v", err)
	}
	foreign, err := store.Send(context.Background(), waypost.SendParams{
		ToAddress:     foreignGroup.Address,
		FromAddress:   "agent/sse",
		Subject:       "foreign",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("foreign-body"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(foreign) error = %v", err)
	}
	runtime.Close()

	handler := NewServer(stateDir, "")
	defer handler.Close()
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, after := range []string{"mistyped-message-id", foreign.MessageID} {
		t.Run(after, func(t *testing.T) {
			requestURL := server.URL + "/api/groups/" + url.PathEscape(group.Address) + "/events?after=" + url.QueryEscape(after)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
			if err != nil {
				t.Fatalf("NewRequest(events) error = %v", err)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatalf("GET events error = %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", response.StatusCode)
			}

			scanner := bufio.NewScanner(response.Body)
			deadline := time.After(3 * time.Second)
			lines := make(chan string, 16)
			go func() {
				for scanner.Scan() {
					lines <- scanner.Text()
				}
			}()
			for {
				select {
				case line := <-lines:
					if strings.HasPrefix(line, "data: ") && strings.Contains(line, lastMessageID) {
						return
					}
				case <-deadline:
					t.Fatalf("timed out waiting for history replay after %q", after)
				}
			}
		})
	}
}

func TestEventsEndpointReturns404BeforeStreamingAndHonorsLastEventIDAndClose(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	store := runtime.Store()
	group, err := store.CreateGroup(context.Background(), "group/web-events-resume")
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	first, err := store.Send(context.Background(), waypost.SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent/sse",
		Subject:       "first",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("first-body"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(first) error = %v", err)
	}
	second, err := store.Send(context.Background(), waypost.SendParams{
		ToAddress:     group.Address,
		FromAddress:   "agent/sse",
		Subject:       "second",
		ContentType:   "text/plain",
		SchemaVersion: "v1",
		Body:          []byte("second-body"),
		Group:         true,
	})
	if err != nil {
		t.Fatalf("Send(second) error = %v", err)
	}
	runtime.Close()

	handler := NewServer(stateDir, "")
	defer handler.Close()
	server := httptest.NewServer(handler)
	defer server.Close()

	// Missing group returns 404 JSON before SSE headers.
	missingResp, err := http.Get(server.URL + "/api/groups/" + url.PathEscape("group/does-not-exist") + "/events")
	if err != nil {
		t.Fatalf("GET missing events error = %v", err)
	}
	missingResp.Body.Close()
	if missingResp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing group /events status = %d, want 404", missingResp.StatusCode)
	}

	// Last-Event-ID header overrides missing ?after query and emits id: lines.
	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/groups/"+url.PathEscape(group.Address)+"/events", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set("Last-Event-ID", first.MessageID)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET events with Last-Event-ID error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	lineCh := make(chan string, 16)
	eofCh := make(chan struct{})
	go func() {
		defer close(eofCh)
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			lineCh <- scanner.Text()
		}
	}()

	gotID := false
	gotSecond := false
	deadline := time.After(3 * time.Second)
	for !(gotID && gotSecond) {
		select {
		case line := <-lineCh:
			if line == "id: "+second.MessageID {
				gotID = true
			}
			if strings.HasPrefix(line, "data: ") && strings.Contains(line, `"body":"second-body"`) {
				gotSecond = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for SSE id/data after Last-Event-ID (gotID=%v, gotSecond=%v)", gotID, gotSecond)
		}
	}

	// Calling handler.Close() unblocks the active SSE handler immediately
	// (no hang waiting for poll/shutdown) and blocks reopening runtime.
	if err := handler.Close(); err != nil {
		t.Fatalf("handler.Close() error = %v", err)
	}
	select {
	case <-eofCh:
	case <-time.After(2 * time.Second):
		t.Fatal("active SSE stream did not terminate promptly after handler.Close()")
	}
	if _, err := handler.waypostRuntime(context.Background()); err == nil {
		t.Fatal("waypostRuntime() after Close() = nil, want closed error")
	}
}

func TestTranscriptHTMLStage4StateFixes(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		`transcriptCursor = "";`,
		`latestMessageID = "";`,
		`(event && event.data) || eventSource.readyState === EventSource.CLOSED`,
		`closeSource();`,
		`button.dataset.defaultLabel`,
	} {
		if !strings.Contains(indexHTML, fragment) {
			t.Fatalf("indexHTML missing Stage 4 fix fragment %q", fragment)
		}
	}
}

func TestStatusForWaypostErrorMapping(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{name: "server closed", err: fmt.Errorf("wrap: %w", errServerClosed), status: http.StatusServiceUnavailable},
		{name: "group not found", err: fmt.Errorf("wrap: %w", waypost.ErrGroupNotFound), status: http.StatusNotFound},
		{name: "invalid cursor", err: fmt.Errorf("wrap: %w", waypost.ErrInvalidPaginationCursor), status: http.StatusBadRequest},
		{name: "invalid argument", err: fmt.Errorf("wrap: %w", waypost.ErrInvalidArgument), status: http.StatusBadRequest},
		{name: "unexpected storage error", err: errors.New("sqlite busy"), status: http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := statusForWaypostError(tc.err); got != tc.status {
				t.Fatalf("statusForWaypostError(%v) = %d, want %d", tc.err, got, tc.status)
			}
		})
	}

	handler := NewServer(filepath.Join(t.TempDir(), "waypost-state"), "")
	if err := handler.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, path := range []string{
		"/api/groups",
		"/api/groups/" + url.PathEscape("group/any") + "/transcript",
		"/api/groups/" + url.PathEscape("group/any") + "/events",
	} {
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s error = %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("closed GET %s status = %d, want 503", path, resp.StatusCode)
		}
	}
}

func TestCancelStreamsUnblocksSSEWithoutClosingRuntime(t *testing.T) {
	t.Parallel()

	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime() error = %v", err)
	}
	if _, err := runtime.Store().CreateGroup(context.Background(), "group/web-cancel-streams"); err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	runtime.Close()

	handler := NewServer(stateDir, "")
	defer handler.Close()
	server := httptest.NewServer(handler)
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/groups/"+url.PathEscape("group/web-cancel-streams")+"/events", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatalf("GET events error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET events status = %d, want 200", resp.StatusCode)
	}

	lineCh := make(chan string, 8)
	eofCh := make(chan struct{})
	go func() {
		defer close(eofCh)
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			lineCh <- scanner.Text()
		}
	}()

	select {
	case line := <-lineCh:
		if line != ": connected" {
			t.Fatalf("first SSE line = %q, want : connected", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SSE connected prelude")
	}

	handler.CancelStreams()
	select {
	case <-eofCh:
	case <-time.After(2 * time.Second):
		t.Fatal("SSE stream did not close promptly after CancelStreams()")
	}

	// SQLite runtime must remain usable for in-flight non-streaming requests until Close().
	groupsResp, err := http.Get(server.URL + "/api/groups")
	if err != nil {
		t.Fatalf("GET /api/groups after CancelStreams() error = %v", err)
	}
	groupsResp.Body.Close()
	if groupsResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/groups after CancelStreams() status = %d, want 200", groupsResp.StatusCode)
	}

	// New /events connections arriving after CancelStreams() must deterministically return 503
	// instead of streaming 200 text/event-stream or failing with 500.
	postCancelEventsResp, err := http.Get(server.URL + "/api/groups/" + url.PathEscape("group/web-cancel-streams") + "/events")
	if err != nil {
		t.Fatalf("GET /events after CancelStreams() error = %v", err)
	}
	postCancelEventsResp.Body.Close()
	if postCancelEventsResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET /events after CancelStreams() status = %d, want 503", postCancelEventsResp.StatusCode)
	}
}

func TestTranscriptHTMLSurfacesPaginationErrorsWhileLive(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{
		`id="groups-error"`,
		`id="messages-error"`,
		`showInlineError(messagesErrorEl,`,
		`showInlineError(groupsErrorEl,`,
	} {
		if !strings.Contains(indexHTML, fragment) {
			t.Fatalf("indexHTML missing pagination inline error fragment %q", fragment)
		}
	}
}
