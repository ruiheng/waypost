package webui

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ruiheng/waypost/internal/hideconsole"
	"github.com/ruiheng/waypost/internal/waypost"
)

const (
	defaultListenAddress = "127.0.0.1:0"
	ssePollInterval      = time.Second
)

var errServerClosed = errors.New("web server is closed")

type Options struct {
	StateDir    string
	Listen      string
	Group       string
	Stdin       io.Reader
	Stdout      io.Writer
	Interactive bool
	OpenURL     func(context.Context, string) error
	CopyText    func(context.Context, string) error
}

type Server struct {
	stateDir       string
	group          string
	mu             sync.Mutex
	runtime        *waypost.Runtime
	closed         bool
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func Run(ctx context.Context, opts Options) error {
	listen := strings.TrimSpace(opts.Listen)
	if listen == "" {
		listen = defaultListenAddress
	}

	handler := NewServer(opts.StateDir, opts.Group)
	defer handler.Close()

	server := &http.Server{
		Addr:    listen,
		Handler: handler,
	}
	server.RegisterOnShutdown(handler.CancelStreams)

	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", listen, err)
	}

	webURL := "http://" + listener.Addr().String()
	writeLine(opts.Stdout, "waypost group web listening on "+webURL)
	if listensBeyondLoopback(listen) {
		writeLine(opts.Stdout, "warning: group web read-only UI is listening beyond loopback")
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()

	promptForURLAction(ctx, opts, webURL)

	select {
	case <-ctx.Done():
		handler.CancelStreams()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return fmt.Errorf("shutdown web server: %w", err)
		}
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func promptForURLAction(ctx context.Context, opts Options, webURL string) {
	if !opts.Interactive || opts.Stdin == nil || opts.Stdout == nil {
		return
	}

	fmt.Fprint(opts.Stdout, "Open URL [o], copy URL [c], or press Enter to continue: ")
	type promptResult struct {
		choice string
		err    error
	}
	resultCh := make(chan promptResult, 1)
	go func() {
		choice, err := bufio.NewReader(opts.Stdin).ReadString('\n')
		resultCh <- promptResult{choice: choice, err: err}
	}()

	var result promptResult
	select {
	case <-ctx.Done():
		return
	case result = <-resultCh:
	}
	if result.err != nil && !errors.Is(result.err, io.EOF) {
		writeLine(opts.Stdout, "unable to read choice: "+result.err.Error())
		return
	}
	switch strings.ToLower(strings.TrimSpace(result.choice)) {
	case "o", "open":
		openURL := opts.OpenURL
		if openURL == nil {
			openURL = openURLWithSystemCommand
		}
		if err := openURL(ctx, webURL); err != nil {
			writeLine(opts.Stdout, "unable to open URL: "+err.Error())
			return
		}
		writeLine(opts.Stdout, "opened "+webURL)
	case "c", "copy":
		copyText := opts.CopyText
		if copyText == nil {
			copyText = copyTextWithSystemCommand
		}
		if err := copyText(ctx, webURL); err != nil {
			writeLine(opts.Stdout, "unable to copy URL: "+err.Error())
			return
		}
		writeLine(opts.Stdout, "copied "+webURL)
	default:
	}
}

func openURLWithSystemCommand(ctx context.Context, webURL string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.CommandContext(ctx, "open", webURL).Start()
	case "windows":
		return exec.CommandContext(ctx, "rundll32", "url.dll,FileProtocolHandler", webURL).Start()
	default:
		path, err := exec.LookPath("xdg-open")
		if err != nil {
			return errors.New("xdg-open not found")
		}
		return exec.CommandContext(ctx, path, webURL).Start()
	}
}

func copyTextWithSystemCommand(ctx context.Context, text string) error {
	var candidates [][]string
	switch runtime.GOOS {
	case "darwin":
		candidates = [][]string{{"pbcopy"}}
	case "windows":
		candidates = [][]string{{"clip"}}
	default:
		candidates = [][]string{{"wl-copy"}, {"xclip", "-selection", "clipboard"}, {"xsel", "--clipboard", "--input"}}
	}
	failures := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		path, err := exec.LookPath(candidate[0])
		if err != nil {
			continue
		}
		cmd := exec.CommandContext(ctx, path, candidate[1:]...)
		hideconsole.Hide(cmd)
		cmd.Stdin = strings.NewReader(text)
		if err := cmd.Run(); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", candidate[0], err))
			continue
		}
		return nil
	}
	if len(failures) > 0 {
		return fmt.Errorf("clipboard commands failed: %s", strings.Join(failures, "; "))
	}
	return errors.New("no clipboard command found")
}

func writeLine(w io.Writer, text string) {
	if w == nil {
		return
	}
	fmt.Fprintln(w, text)
}

func listensBeyondLoopback(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		host = listen
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return false
	}
	ip := net.ParseIP(host)
	return ip == nil || !ip.IsLoopback()
}

func NewServer(stateDir, group string) *Server {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	return &Server{
		stateDir:       stateDir,
		group:          strings.TrimSpace(group),
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}
}

func (s *Server) CancelStreams() {
	if s.shutdownCancel != nil {
		s.shutdownCancel()
	}
}

func (s *Server) Close() error {
	s.CancelStreams()
	s.mu.Lock()
	s.closed = true
	runtime := s.runtime
	s.runtime = nil
	s.mu.Unlock()
	if runtime == nil {
		return nil
	}
	return runtime.Close()
}

func (s *Server) waypostRuntime(ctx context.Context) (*waypost.Runtime, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errServerClosed
	}
	if s.runtime != nil {
		return s.runtime, nil
	}
	runtime, err := waypost.OpenRuntime(ctx, s.stateDir)
	if err != nil {
		return nil, err
	}
	s.runtime = runtime
	return runtime, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/":
		s.handleIndex(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/api/groups":
		s.handleGroups(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/groups/"):
		s.handleGroupPath(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.WriteString(w, strings.ReplaceAll(indexHTML, "{{DEFAULT_GROUP}}", templateString(s.group)))
}

func (s *Server) handleGroups(w http.ResponseWriter, r *http.Request) {
	runtime, err := s.waypostRuntime(r.Context())
	if err != nil {
		writeError(w, statusForWaypostError(err), err)
		return
	}

	pageParams, err := pageParamsFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	page, err := runtime.Store().ListGroupsPage(r.Context(), pageParams)
	if err != nil {
		writeError(w, statusForWaypostError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":         page.Items,
		"next_cursor":   nilIfEmpty(page.NextCursor),
		"default_group": nilIfEmpty(s.group),
	})
}

func (s *Server) handleGroupPath(w http.ResponseWriter, r *http.Request) {
	groupAddress, action, ok := splitGroupAction(r.URL.EscapedPath())
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch action {
	case "transcript":
		s.handleTranscript(w, r, groupAddress)
	case "events":
		s.handleEvents(w, r, groupAddress)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleTranscript(w http.ResponseWriter, r *http.Request, groupAddress string) {
	runtime, err := s.waypostRuntime(r.Context())
	if err != nil {
		writeError(w, statusForWaypostError(err), err)
		return
	}

	pageParams, err := pageParamsFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	latestMessageID, err := runtime.Store().LatestGroupMessageID(r.Context(), groupAddress)
	if err != nil {
		writeError(w, statusForWaypostError(err), err)
		return
	}
	page, err := runtime.Store().ListGroupTranscriptPage(r.Context(), waypost.GroupTranscriptParams{
		Address: groupAddress,
		Limit:   pageParams.Limit,
		Cursor:  pageParams.Cursor,
	})
	if err != nil {
		writeError(w, statusForWaypostError(err), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"group_address":     groupAddress,
		"items":             page.Items,
		"next_cursor":       nilIfEmpty(page.NextCursor),
		"latest_message_id": nilIfEmpty(latestMessageID),
	})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request, groupAddress string) {
	if s.shutdownCtx != nil && s.shutdownCtx.Err() != nil {
		writeError(w, http.StatusServiceUnavailable, errServerClosed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("streaming is not supported"))
		return
	}

	streamCtx := r.Context()
	if s.shutdownCtx != nil {
		var streamCancel context.CancelFunc
		streamCtx, streamCancel = context.WithCancel(r.Context())
		stopShutdown := context.AfterFunc(s.shutdownCtx, streamCancel)
		defer stopShutdown()
		defer streamCancel()
	}

	afterProvided := false
	after := ""
	if lastEventID := strings.TrimSpace(r.Header.Get("Last-Event-ID")); lastEventID != "" {
		after = lastEventID
		afterProvided = true
	} else {
		afterValues, hasAfterQuery := r.URL.Query()["after"]
		afterProvided = hasAfterQuery
		if len(afterValues) > 0 {
			after = strings.TrimSpace(afterValues[0])
		}
	}

	runtime, err := s.waypostRuntime(streamCtx)
	if err != nil {
		if s.shutdownCtx != nil && s.shutdownCtx.Err() != nil {
			writeError(w, http.StatusServiceUnavailable, errServerClosed)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		writeError(w, statusForWaypostError(err), err)
		return
	}
	latestID, err := runtime.Store().LatestGroupMessageID(streamCtx, groupAddress)
	if err != nil {
		if s.shutdownCtx != nil && s.shutdownCtx.Err() != nil {
			writeError(w, http.StatusServiceUnavailable, errServerClosed)
			return
		}
		if r.Context().Err() != nil {
			return
		}
		writeError(w, statusForWaypostError(err), err)
		return
	}
	if s.shutdownCtx != nil && s.shutdownCtx.Err() != nil {
		writeError(w, http.StatusServiceUnavailable, errServerClosed)
		return
	}
	if !afterProvided {
		after = latestID
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	_, _ = io.WriteString(w, ": connected\n\n")
	flusher.Flush()

	ticker := time.NewTicker(ssePollInterval)
	defer ticker.Stop()

	streamClosed := func() bool {
		return streamCtx.Err() != nil || (s.shutdownCtx != nil && s.shutdownCtx.Err() != nil)
	}
	for {
		if streamClosed() {
			return
		}
		page, err := s.transcript(streamCtx, groupAddress, after)
		if err != nil {
			if streamClosed() || errors.Is(err, errServerClosed) {
				return
			}
			writeSSE(w, "", "error", map[string]string{"error": err.Error()})
			flusher.Flush()
			return
		}

		for _, message := range page.Items {
			writeSSE(w, message.MessageID, "message", message)
			after = message.MessageID
			flusher.Flush()
		}

		if page.NextCursor != "" {
			continue
		}
		select {
		case <-streamCtx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) transcript(ctx context.Context, groupAddress, after string) (waypost.Page[waypost.GroupTranscriptMessage], error) {
	runtime, err := s.waypostRuntime(ctx)
	if err != nil {
		return waypost.Page[waypost.GroupTranscriptMessage]{}, err
	}
	return runtime.Store().ListGroupTranscriptPage(ctx, waypost.GroupTranscriptParams{
		Address:        groupAddress,
		AfterMessageID: after,
		Limit:          waypost.MaxPageSize,
	})
}

func pageParamsFromRequest(r *http.Request) (waypost.PageParams, error) {
	params := waypost.PageParams{Limit: waypost.DefaultPageSize, Cursor: strings.TrimSpace(r.URL.Query().Get("cursor"))}
	rawLimit := strings.TrimSpace(r.URL.Query().Get("limit"))
	if rawLimit == "" {
		return params, nil
	}
	limit, err := strconv.Atoi(rawLimit)
	if err != nil || limit < 1 || limit > waypost.MaxPageSize {
		return waypost.PageParams{}, fmt.Errorf("limit must be between 1 and %d", waypost.MaxPageSize)
	}
	params.Limit = limit
	return params, nil
}

func splitGroupAction(escapedPath string) (string, string, bool) {
	const prefix = "/api/groups/"
	if !strings.HasPrefix(escapedPath, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(escapedPath, prefix)
	index := strings.LastIndex(rest, "/")
	if index < 0 {
		return "", "", false
	}
	escapedGroup := rest[:index]
	action := rest[index+1:]
	group, err := url.PathUnescape(escapedGroup)
	if err != nil {
		return "", "", false
	}
	return group, action, group != "" && action != ""
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeSSE(w io.Writer, id, event string, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		body = []byte(`{"error":"encode event"}`)
		event = "error"
		id = ""
	}
	if strings.TrimSpace(id) != "" {
		fmt.Fprintf(w, "id: %s\n", id)
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, body)
}

func statusForWaypostError(err error) int {
	switch {
	case errors.Is(err, errServerClosed):
		return http.StatusServiceUnavailable
	case errors.Is(err, waypost.ErrGroupNotFound):
		return http.StatusNotFound
	case errors.Is(err, waypost.ErrInvalidPaginationCursor), errors.Is(err, waypost.ErrInvalidArgument):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func nilIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func templateString(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return `""`
	}
	return string(encoded)
}
