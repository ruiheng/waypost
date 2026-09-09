package codexhook

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"

	"github.com/ruiheng/waypost/internal/launchpath"
)

const (
	compactManagedDescription = "Waypost Codex compact-context guard"
	promptManagedDescription  = "Waypost Codex nudge MCP hint"
	waitManagedDescription    = "Waypost Codex wait polling guard"
	receiveManagedDescription = "Waypost Codex receive completion tracker"
	cleanupManagedDescription = "Waypost Codex nudge state cleanup"
	compactStatusMessage      = "Restoring Waypost compact context"
	promptStatusMessage       = "Preparing Waypost receive hint"
	waitStatusMessage         = "Checking Waypost wait usage"
	legacyPromptStatusMessage = "Checking Waypost MCP availability"
	defaultNudgeMessage       = "NOTICE: There might be new delivery in waypost."
	receiveMCPToolName        = "mcp__waypost__waypost_recv"
	hookStateDirectoryName    = "waypost-hook-state"
)

const hookTimeoutSeconds int64 = 5
const hookTimeoutJSON json.Number = "5"
const cleanupHookTimeoutSeconds int64 = 3
const cleanupHookTimeoutJSON json.Number = "3"

const AdditionalContext = `A prior Waypost nudge in this session was already handled before compaction (either a receive completed or no message was available). Do not repeat the receive merely because of compaction. Continue from the compacted context.`

const MCPNudgeContext = `The waypost_recv MCP tool is available. Use it instead of the Waypost CLI.`

const CLINudgeContext = `The Waypost MCP tool waypost_recv is unavailable. Receive the pending delivery with ` + "`waypost recv --json`" + `.`

const MCPProbeFailedNudgeContext = `Look for the waypost_recv MCP tool. If it is unavailable, receive the pending delivery with ` + "`waypost recv --json`" + `.`

const WaitPollingContext = `Do not poll Waypost. Continue other available work; if none remains, stop completely.`

const MCPStatusDenialReason = `The Waypost MCP tool waypost_status is available. Use it instead of running waypost status.`
const MCPServerCommandDenialReason = `The Waypost MCP server is managed by Codex. Never run the Waypost CLI command ` + "`waypost mcp`" + `.`

var waypostMCPCommandBlacklist = map[string]string{
	"ack":     "waypost_ack",
	"recv":    "waypost_recv",
	"receive": "waypost_recv",
	"send":    "waypost_send",
	// The empty replacement marks a command that must always be denied.
	"mcp": "",
}

type hookInput struct {
	HookEventName string          `json:"hook_event_name"`
	SessionID     string          `json:"session_id"`
	Source        string          `json:"source"`
	Prompt        string          `json:"prompt"`
	ToolName      string          `json:"tool_name"`
	ToolInput     json.RawMessage `json:"tool_input"`
	ToolResponse  json.RawMessage `json:"tool_response"`
}

type hookOutput struct {
	SystemMessage      string             `json:"systemMessage,omitempty"`
	HookSpecificOutput hookSpecificOutput `json:"hookSpecificOutput"`
}

type hookSpecificOutput struct {
	HookEventName            string `json:"hookEventName"`
	AdditionalContext        string `json:"additionalContext,omitempty"`
	PermissionDecision       string `json:"permissionDecision,omitempty"`
	PermissionDecisionReason string `json:"permissionDecisionReason,omitempty"`
}

type InstallResult struct {
	Path    string
	Changed bool
}

type DoctorResult struct {
	Path    string
	Command string
}

func Run(ctx context.Context, r io.Reader, w io.Writer) error {
	return run(ctx, r, w)
}

func run(ctx context.Context, r io.Reader, w io.Writer) error {
	store, err := defaultNudgeStateStore()
	if err != nil {
		return err
	}
	return runWithDependencies(ctx, r, w, CurrentDirectoryWaypostMCPAvailable, store)
}

func runWithMCPProbe(ctx context.Context, r io.Reader, w io.Writer, probe waypostMCPProbe) error {
	return runWithDependencies(ctx, r, w, probe, newMemoryNudgeStateStore())
}

func runWithDependencies(
	ctx context.Context,
	r io.Reader,
	w io.Writer,
	probe waypostMCPProbe,
	store nudgeStateStore,
) error {
	input, hasInput, err := readHookInput(r)
	if err != nil {
		return err
	}
	if !hasInput {
		return writeOutput(w, "SessionStart", AdditionalContext)
	}

	switch input.HookEventName {
	case "SessionStart":
		if input.Source != "compact" {
			return nil
		}
		state, err := store.Load(input.SessionID)
		if err != nil {
			return err
		}
		if state != nudgeConsumed {
			return nil
		}
		return writeOutput(w, "SessionStart", AdditionalContext)
	case "UserPromptSubmit":
		if !LooksLikeWaypostNudge(input.Prompt) {
			return store.Clear(input.SessionID)
		}
		if err := store.Save(input.SessionID, nudgePending); err != nil {
			return err
		}
		availability, probeErr := detectWaypostMCP(ctx, probe)
		switch availability {
		case waypostMCPUnknown:
			return writeOutputWithSystemMessage(w, "UserPromptSubmit", MCPProbeFailedNudgeContext, mcpProbeFailureMessage(probeErr))
		case waypostMCPAvailable:
			return writeOutput(w, "UserPromptSubmit", MCPNudgeContext)
		default:
			return writeOutput(w, "UserPromptSubmit", CLINudgeContext)
		}
	case "PostToolUse":
		if !successfulWaypostReceive(input) {
			return nil
		}
		state, err := store.Load(input.SessionID)
		if err != nil {
			return err
		}
		if state != nudgePending {
			return nil
		}
		return store.Save(input.SessionID, nudgeConsumed)
	case "PreToolUse":
		if input.ToolName != "Bash" {
			return nil
		}
		command, err := bashCommand(input.ToolInput)
		if err != nil {
			return err
		}
		if LooksLikeWaypostWaitCommand(command) {
			return writeOutput(w, "PreToolUse", WaitPollingContext)
		}
		denialReason, guarded := waypostMCPDenialReason(command)
		if !guarded {
			return nil
		}
		if waypostMCPCommandAlwaysDenied(command) {
			return writeDenyOutput(w, denialReason)
		}
		availability, probeErr := detectWaypostMCP(ctx, probe)
		if availability == waypostMCPUnknown {
			return writeSystemMessage(w, mcpProbeFailureMessage(probeErr))
		}
		if availability == waypostMCPUnavailable {
			return nil
		}
		return writeDenyOutput(w, denialReason)
	case "SessionEnd":
		return store.Clear(input.SessionID)
	default:
		return nil
	}
}

func WriteOutput(w io.Writer) error {
	return writeOutput(w, "SessionStart", AdditionalContext)
}

func writeOutput(w io.Writer, eventName, additionalContext string) error {
	return writeOutputWithSystemMessage(w, eventName, additionalContext, "")
}

func writeOutputWithSystemMessage(w io.Writer, eventName, additionalContext, systemMessage string) error {
	return json.NewEncoder(w).Encode(hookOutput{
		SystemMessage: systemMessage,
		HookSpecificOutput: hookSpecificOutput{
			HookEventName:     eventName,
			AdditionalContext: additionalContext,
		},
	})
}

func mcpProbeFailureMessage(err error) string {
	if err == nil {
		return "Waypost MCP probe failed for an unknown reason."
	}
	return fmt.Sprintf("Waypost MCP probe failed: %v", err)
}

func writeSystemMessage(w io.Writer, systemMessage string) error {
	return json.NewEncoder(w).Encode(struct {
		SystemMessage string `json:"systemMessage"`
	}{SystemMessage: systemMessage})
}

func writeDenyOutput(w io.Writer, reason string) error {
	return json.NewEncoder(w).Encode(hookOutput{
		HookSpecificOutput: hookSpecificOutput{
			HookEventName:            "PreToolUse",
			PermissionDecision:       "deny",
			PermissionDecisionReason: reason,
		},
	})
}

func readHookInput(r io.Reader) (hookInput, bool, error) {
	var input hookInput
	err := json.NewDecoder(r).Decode(&input)
	if errors.Is(err, io.EOF) {
		return hookInput{}, false, nil
	}
	if err != nil {
		return hookInput{}, false, fmt.Errorf("parse Codex hook input: %w", err)
	}
	return input, true, nil
}

func LooksLikeWaypostNudge(prompt string) bool {
	return strings.EqualFold(strings.TrimSpace(prompt), defaultNudgeMessage)
}

type nudgeState string

const (
	nudgeNone     nudgeState = ""
	nudgePending  nudgeState = "pending"
	nudgeConsumed nudgeState = "consumed"
)

type nudgeStateStore interface {
	Load(sessionID string) (nudgeState, error)
	Save(sessionID string, state nudgeState) error
	Clear(sessionID string) error
}

type fileNudgeStateStore struct {
	dir string
}

type nudgeStateRecord struct {
	SessionID string     `json:"session_id"`
	State     nudgeState `json:"state"`
}

func defaultNudgeStateStore() (nudgeStateStore, error) {
	home, err := DefaultHome()
	if err != nil {
		return nil, err
	}
	return fileNudgeStateStore{dir: filepath.Join(home, hookStateDirectoryName)}, nil
}

func (store fileNudgeStateStore) Load(sessionID string) (nudgeState, error) {
	sessionID, err := normalizedSessionID(sessionID)
	if err != nil {
		return nudgeNone, err
	}
	path, err := store.path(sessionID)
	if err != nil {
		return nudgeNone, err
	}
	contents, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nudgeNone, nil
	}
	if err != nil {
		return nudgeNone, fmt.Errorf("read Codex Waypost nudge state %q: %w", path, err)
	}
	var record nudgeStateRecord
	if err := json.Unmarshal(contents, &record); err != nil {
		return nudgeNone, fmt.Errorf("parse Codex Waypost nudge state %q: %w", path, err)
	}
	if record.SessionID != sessionID {
		return nudgeNone, fmt.Errorf("parse Codex Waypost nudge state %q: session id mismatch", path)
	}
	if record.State != nudgePending && record.State != nudgeConsumed {
		return nudgeNone, fmt.Errorf("parse Codex Waypost nudge state %q: invalid state %q", path, record.State)
	}
	return record.State, nil
}

func (store fileNudgeStateStore) Save(sessionID string, state nudgeState) error {
	if state != nudgePending && state != nudgeConsumed {
		return fmt.Errorf("save Codex Waypost nudge state: invalid state %q", state)
	}
	sessionID, err := normalizedSessionID(sessionID)
	if err != nil {
		return err
	}
	path, err := store.path(sessionID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(store.dir, 0o700); err != nil {
		return fmt.Errorf("create Codex Waypost nudge state directory %q: %w", store.dir, err)
	}
	contents, err := json.Marshal(nudgeStateRecord{SessionID: sessionID, State: state})
	if err != nil {
		return fmt.Errorf("encode Codex Waypost nudge state: %w", err)
	}
	contents = append(contents, '\n')
	temporary, err := os.CreateTemp(store.dir, ".state.tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary Codex Waypost nudge state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("set Codex Waypost nudge state permissions: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		return fmt.Errorf("write Codex Waypost nudge state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync Codex Waypost nudge state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close Codex Waypost nudge state: %w", err)
	}
	if err := replaceHooksFile(temporaryPath, path); err != nil {
		return fmt.Errorf("replace Codex Waypost nudge state %q: %w", path, err)
	}
	return nil
}

func (store fileNudgeStateStore) Clear(sessionID string) error {
	sessionID, err := normalizedSessionID(sessionID)
	if err != nil {
		return err
	}
	path, err := store.path(sessionID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove Codex Waypost nudge state %q: %w", path, err)
	}
	return nil
}

func (store fileNudgeStateStore) path(sessionID string) (string, error) {
	sessionID, err := normalizedSessionID(sessionID)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(sessionID))
	return filepath.Join(store.dir, fmt.Sprintf("%x.json", digest)), nil
}

type memoryNudgeStateStore struct {
	states map[string]nudgeState
}

func newMemoryNudgeStateStore() *memoryNudgeStateStore {
	return &memoryNudgeStateStore{states: make(map[string]nudgeState)}
}

func (store *memoryNudgeStateStore) Load(sessionID string) (nudgeState, error) {
	sessionID, err := normalizedSessionID(sessionID)
	if err != nil {
		return nudgeNone, err
	}
	return store.states[sessionID], nil
}

func (store *memoryNudgeStateStore) Save(sessionID string, state nudgeState) error {
	sessionID, err := normalizedSessionID(sessionID)
	if err != nil {
		return err
	}
	store.states[sessionID] = state
	return nil
}

func (store *memoryNudgeStateStore) Clear(sessionID string) error {
	sessionID, err := normalizedSessionID(sessionID)
	if err != nil {
		return err
	}
	delete(store.states, sessionID)
	return nil
}

func normalizedSessionID(sessionID string) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "", errors.New("Codex hook input is missing session_id")
	}
	return sessionID, nil
}

func successfulWaypostReceive(input hookInput) bool {
	switch input.ToolName {
	case receiveMCPToolName:
		return successfulWaypostMCPResponse(input.ToolResponse)
	case "Bash":
		command, err := bashCommand(input.ToolInput)
		if err != nil {
			return false
		}
		subcommand, ok := directWaypostCommand(command)
		return ok && (subcommand == "recv" || subcommand == "receive") && successfulBashResponse(input.ToolResponse)
	default:
		return false
	}
}

func successfulWaypostMCPResponse(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var response struct {
		IsError           bool `json:"isError"`
		StructuredContent struct {
			Status string `json:"status"`
		} `json:"structuredContent"`
	}
	if err := json.Unmarshal(raw, &response); err != nil || response.IsError {
		return false
	}
	return response.StructuredContent.Status == "received" || response.StructuredContent.Status == "no_message"
}

func successfulBashResponse(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var output string
	if json.Unmarshal(raw, &output) != nil {
		return false
	}
	output = strings.TrimSpace(output)
	if output == "" {
		return false
	}

	if success, parsed := successfulJSONReceive(output); parsed {
		return success
	}
	if output == "status=no_message" {
		return true
	}

	firstLine, _, _ := strings.Cut(output, "\n")
	if strings.HasPrefix(firstLine, "status: ") {
		var status string
		if json.Unmarshal([]byte(strings.TrimPrefix(firstLine, "status: ")), &status) == nil {
			return status == "received" || status == "no_message"
		}
		return false
	}
	if successfulFullYAMLReceive(output) {
		return true
	}
	personalReceive := strings.HasPrefix(firstLine, "delivery_id=") &&
		strings.Contains(firstLine, " recipient_address=") &&
		strings.Contains(firstLine, " lease_token=")
	groupReceive := strings.HasPrefix(firstLine, "message_id=") &&
		strings.Contains(firstLine, " group=") &&
		strings.Contains(firstLine, " person=") &&
		strings.Contains(firstLine, " first_read_at=")
	return personalReceive || groupReceive
}

type cliJSONReceive struct {
	Status           string           `json:"status"`
	DeliveryID       string           `json:"delivery_id"`
	RecipientAddress string           `json:"recipient_address"`
	LeaseToken       string           `json:"lease_token"`
	MessageID        string           `json:"message_id"`
	GroupAddress     string           `json:"group_address"`
	Person           string           `json:"person"`
	FirstReadAt      string           `json:"first_read_at"`
	Messages         []cliJSONReceive `json:"messages"`
}

func successfulJSONReceive(output string) (success, parsed bool) {
	var response cliJSONReceive
	if json.Unmarshal([]byte(output), &response) != nil {
		return false, false
	}
	if response.Status != "" {
		return response.Status == "received" || response.Status == "no_message", true
	}
	if completePersonalJSONReceive(response) || completeGroupJSONReceive(response) {
		return true, true
	}
	if len(response.Messages) == 0 {
		return false, true
	}
	for _, message := range response.Messages {
		if !completePersonalJSONReceive(message) {
			return false, true
		}
	}
	return true, true
}

func completePersonalJSONReceive(response cliJSONReceive) bool {
	return response.DeliveryID != "" && response.RecipientAddress != "" && response.LeaseToken != ""
}

func completeGroupJSONReceive(response cliJSONReceive) bool {
	return response.MessageID != "" && response.GroupAddress != "" && response.Person != "" && response.FirstReadAt != ""
}

func successfulFullYAMLReceive(output string) bool {
	fields := make(map[string]bool)
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		name, value, found := strings.Cut(line, ":")
		if found && strings.TrimSpace(value) != "" {
			fields[name] = true
		}
	}
	personalReceive := fields["delivery_id"] && fields["recipient_address"] && fields["lease_token"]
	groupReceive := fields["message_id"] && fields["group_address"] && fields["person"] && fields["first_read_at"]
	return personalReceive || groupReceive
}

func LooksLikeWaypostWaitCommand(command string) bool {
	subcommand, ok := directWaypostCommand(command)
	return ok && subcommand == "wait"
}

func waypostMCPDenialReason(command string) (string, bool) {
	subcommand, ok := directWaypostCommand(command)
	if !ok {
		return "", false
	}
	if subcommand == "status" {
		return MCPStatusDenialReason, true
	}
	tool, blocked := waypostMCPCommandBlacklist[subcommand]
	if !blocked {
		return "", false
	}
	if tool == "" {
		return MCPServerCommandDenialReason, true
	}
	return fmt.Sprintf("The Waypost MCP tool %s is available. Use it instead of the Waypost CLI.", tool), true
}

func waypostMCPCommandAlwaysDenied(command string) bool {
	subcommand, ok := directWaypostCommand(command)
	if !ok {
		return false
	}
	tool, blocked := waypostMCPCommandBlacklist[subcommand]
	return blocked && tool == ""
}

func directWaypostCommand(command string) (string, bool) {
	rest := strings.TrimSpace(command)
	executable, rest, ok := consumeCommandWord(rest)
	if !ok {
		return "", false
	}
	if executable == "&" {
		executable, rest, ok = consumeCommandWord(rest)
		if !ok {
			return "", false
		}
	}
	if !isWaypostExecutable(executable) {
		return "", false
	}

	for {
		argument, remaining, ok := consumeCommandWord(rest)
		if !ok {
			return "", false
		}
		switch {
		case argument == "--state-dir":
			_, rest, ok = consumeCommandWord(remaining)
			if !ok {
				return "", false
			}
		case strings.HasPrefix(argument, "--state-dir=") && len(argument) > len("--state-dir="):
			rest = remaining
		default:
			return argument, true
		}
	}
}

func bashCommand(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var input struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return "", fmt.Errorf("parse Codex Bash tool input: %w", err)
	}
	return input.Command, nil
}

func consumeCommandWord(input string) (string, string, bool) {
	input = strings.TrimLeft(input, " \t\r")
	if input == "" || input[0] == '\n' {
		return "", input, false
	}
	if strings.ContainsRune(";&|<>()", rune(input[0])) {
		return input[:1], input[1:], true
	}

	var word strings.Builder
	var quote byte
	for index := 0; index < len(input); index++ {
		character := input[index]
		if quote != 0 {
			if character == quote {
				quote = 0
				continue
			}
			word.WriteByte(character)
			continue
		}
		switch character {
		case '\'', '"':
			quote = character
		case ' ', '\t', '\r':
			return word.String(), input[index:], word.Len() != 0
		case '\n', ';', '&', '|', '<', '>', '(', ')':
			return word.String(), input[index:], word.Len() != 0
		default:
			word.WriteByte(character)
		}
	}
	if quote != 0 || word.Len() == 0 {
		return "", input, false
	}
	return word.String(), "", true
}

func isWaypostExecutable(executable string) bool {
	normalized := strings.ReplaceAll(executable, `\`, "/")
	base := normalized
	if separator := strings.LastIndexByte(normalized, '/'); separator >= 0 {
		base = normalized[separator+1:]
	}
	return strings.EqualFold(base, "waypost") || strings.EqualFold(base, "waypost.exe")
}

func DefaultHome() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("CODEX_HOME")); configured != "" {
		return filepath.Abs(configured)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home for Codex hooks: %w", err)
	}
	return filepath.Join(home, ".codex"), nil
}

func CurrentCommand() (string, error) {
	executable, err := launchpath.CurrentExecutable()
	if err != nil {
		return "", fmt.Errorf("resolve waypost executable: %w", err)
	}
	command := quoteCommandPath(executable) + " codex-hook"
	if runtime.GOOS == "windows" {
		command = "& " + command
	}
	return command, nil
}

func Install(codexHome, command string) (InstallResult, error) {
	path := filepath.Join(codexHome, "hooks.json")
	document, mode, err := readHooksDocument(path)
	if err != nil {
		return InstallResult{}, err
	}

	hooks, err := objectField(document, "hooks")
	if err != nil {
		return InstallResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	if err := validateHooksStructure(hooks); err != nil {
		return InstallResult{}, fmt.Errorf("validate %q: %w", path, err)
	}
	groups, err := arrayField(hooks, "SessionStart")
	if err != nil {
		return InstallResult{}, fmt.Errorf("read %q: %w", path, err)
	}

	updated, compactChanged := mergeManagedHandler(groups, compactManagedGroup(command), managedHandlerSpec{
		description:    compactManagedDescription,
		statusMessages: []string{compactStatusMessage},
		command:        command,
		eligibleGroup: func(group map[string]any) bool {
			return matcherTargetsCompactOnly(group["matcher"])
		},
	})
	hooks["SessionStart"] = updated

	promptGroups, err := arrayField(hooks, "UserPromptSubmit")
	if err != nil {
		return InstallResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	updated, promptChanged := mergeManagedHandler(promptGroups, promptManagedGroup(command), managedHandlerSpec{
		description:    promptManagedDescription,
		statusMessages: []string{promptStatusMessage, legacyPromptStatusMessage},
		command:        command,
		eligibleGroup:  func(map[string]any) bool { return true },
	})
	hooks["UserPromptSubmit"] = updated

	waitGroups, err := arrayField(hooks, "PreToolUse")
	if err != nil {
		return InstallResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	updated, waitChanged := mergeManagedHandler(waitGroups, waitManagedGroup(command), managedHandlerSpec{
		description:    waitManagedDescription,
		statusMessages: []string{waitStatusMessage},
		command:        command,
		eligibleGroup: func(group map[string]any) bool {
			return matcherTargetsBashOnly(group["matcher"])
		},
	})
	hooks["PreToolUse"] = updated

	receiveGroups, err := arrayField(hooks, "PostToolUse")
	if err != nil {
		return InstallResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	updated, receiveChanged := mergeManagedHandler(receiveGroups, receiveManagedGroup(command), managedHandlerSpec{
		description:    receiveManagedDescription,
		statusMessages: nil,
		command:        command,
		eligibleGroup: func(group map[string]any) bool {
			return matcherTargetsReceiveCompletionOnly(group["matcher"])
		},
	})
	hooks["PostToolUse"] = updated

	cleanupGroups, err := arrayField(hooks, "SessionEnd")
	if err != nil {
		return InstallResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	updated, cleanupChanged := mergeManagedHandler(cleanupGroups, cleanupManagedGroup(command), managedHandlerSpec{
		description:    cleanupManagedDescription,
		statusMessages: nil,
		command:        command,
		eligibleGroup:  matcherTargetsEverySessionEnd,
	})
	hooks["SessionEnd"] = updated
	document["hooks"] = hooks
	changed := compactChanged || promptChanged || waitChanged || receiveChanged || cleanupChanged

	if !changed {
		return InstallResult{Path: path, Changed: false}, nil
	}
	if err := writeHooksDocument(path, document, mode); err != nil {
		return InstallResult{}, err
	}
	return InstallResult{Path: path, Changed: true}, nil
}

func Doctor(codexHome, command string) (DoctorResult, error) {
	path := filepath.Join(codexHome, "hooks.json")
	document, _, err := readExistingHooksDocument(path)
	if err != nil {
		return DoctorResult{}, err
	}
	hooks, err := existingObjectField(document, "hooks")
	if err != nil {
		return DoctorResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	if err := validateHooksStructure(hooks); err != nil {
		return DoctorResult{}, fmt.Errorf("validate %q: %w", path, err)
	}
	groups, err := existingArrayField(hooks, "SessionStart")
	if err != nil {
		return DoctorResult{}, fmt.Errorf("read %q: %w", path, err)
	}

	compactInstalled := false
	for _, item := range groups {
		group, ok := item.(map[string]any)
		if !ok || !matcherTargetsCompactOnly(group["matcher"]) {
			continue
		}
		if groupHasCommandWithTimeout(group, command, hookTimeoutSeconds) {
			compactInstalled = true
			break
		}
	}
	if !compactInstalled {
		return DoctorResult{}, fmt.Errorf("Codex compact hook is not installed in %q; run `waypost install codex-hook`", path)
	}

	promptGroups, err := existingArrayField(hooks, "UserPromptSubmit")
	if err != nil {
		return DoctorResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	promptInstalled := false
	for _, item := range promptGroups {
		group, ok := item.(map[string]any)
		if ok && groupHasCommandWithTimeout(group, command, hookTimeoutSeconds) {
			promptInstalled = true
			break
		}
	}
	if !promptInstalled {
		return DoctorResult{}, fmt.Errorf("Codex Waypost nudge hook is not installed in %q; run `waypost install codex-hook`", path)
	}

	waitGroups, err := arrayField(hooks, "PreToolUse")
	if err != nil {
		return DoctorResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	waitInstalled := false
	for _, item := range waitGroups {
		group, ok := item.(map[string]any)
		if !ok || !matcherTargetsBashOnly(group["matcher"]) {
			continue
		}
		if groupHasCommandWithTimeout(group, command, hookTimeoutSeconds) {
			waitInstalled = true
			break
		}
	}
	if !waitInstalled {
		return DoctorResult{}, fmt.Errorf("Codex Waypost wait polling guard is not installed in %q; run `waypost install codex-hook`", path)
	}

	receiveGroups, err := arrayField(hooks, "PostToolUse")
	if err != nil {
		return DoctorResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	receiveInstalled := false
	for _, item := range receiveGroups {
		group, ok := item.(map[string]any)
		if !ok || !matcherTargetsReceiveCompletionOnly(group["matcher"]) {
			continue
		}
		if groupHasCommandWithTimeout(group, command, hookTimeoutSeconds) {
			receiveInstalled = true
			break
		}
	}
	if !receiveInstalled {
		return DoctorResult{}, fmt.Errorf("Codex Waypost receive completion hook is not installed in %q; run `waypost install codex-hook`", path)
	}

	cleanupGroups, err := arrayField(hooks, "SessionEnd")
	if err != nil {
		return DoctorResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	for _, item := range cleanupGroups {
		group, ok := item.(map[string]any)
		if !ok || !matcherTargetsEverySessionEnd(group) {
			continue
		}
		if groupHasCommandWithTimeout(group, command, cleanupHookTimeoutSeconds) {
			return DoctorResult{Path: path, Command: command}, nil
		}
	}
	return DoctorResult{}, fmt.Errorf("Codex Waypost nudge state cleanup hook is not installed in %q; run `waypost install codex-hook`", path)
}

func compactManagedGroup(command string) map[string]any {
	return map[string]any{
		"description": compactManagedDescription,
		"matcher":     "^compact$",
		"hooks": []any{
			map[string]any{
				"type":          "command",
				"command":       command,
				"statusMessage": compactStatusMessage,
				"timeout":       hookTimeoutJSON,
			},
		},
	}
}

func promptManagedGroup(command string) map[string]any {
	return map[string]any{
		"description": promptManagedDescription,
		"hooks": []any{
			map[string]any{
				"type":          "command",
				"command":       command,
				"statusMessage": promptStatusMessage,
				"timeout":       hookTimeoutJSON,
			},
		},
	}
}

func waitManagedGroup(command string) map[string]any {
	return map[string]any{
		"description": waitManagedDescription,
		"matcher":     "^Bash$",
		"hooks": []any{
			map[string]any{
				"type":          "command",
				"command":       command,
				"statusMessage": waitStatusMessage,
				"timeout":       hookTimeoutJSON,
			},
		},
	}
}

func receiveManagedGroup(command string) map[string]any {
	return map[string]any{
		"description": receiveManagedDescription,
		"matcher":     "^(Bash|mcp__waypost__waypost_recv)$",
		"hooks": []any{
			map[string]any{
				"type":    "command",
				"command": command,
				"timeout": hookTimeoutJSON,
			},
		},
	}
}

func cleanupManagedGroup(command string) map[string]any {
	return map[string]any{
		"description": cleanupManagedDescription,
		"hooks": []any{
			map[string]any{
				"type":    "command",
				"command": command,
				"timeout": cleanupHookTimeoutJSON,
			},
		},
	}
}

type managedHandlerSpec struct {
	description    string
	statusMessages []string
	command        string
	eligibleGroup  func(map[string]any) bool
}

func mergeManagedHandler(groups []any, desired map[string]any, spec managedHandlerSpec) ([]any, bool) {
	desiredHandlers := desired["hooks"].([]any)
	desiredHandler := desiredHandlers[0]
	updated := make([]any, 0, len(groups)+1)
	installed := false
	for _, item := range groups {
		group, ok := item.(map[string]any)
		if !ok {
			updated = append(updated, item)
			continue
		}

		description, _ := group["description"].(string)
		managedGroup := description == spec.description
		if !managedGroup && !spec.eligibleGroup(group) {
			updated = append(updated, item)
			continue
		}

		handlers, ok := group["hooks"].([]any)
		if !ok {
			updated = append(updated, item)
			continue
		}
		if !installed && managedGroup && len(handlers) == 1 && managedCommandHandler(handlers[0], spec, true, 1) {
			updated = append(updated, desired)
			installed = true
			continue
		}

		kept := make([]any, 0, len(handlers))
		changed := false
		keptManagedHandler := false
		for _, handler := range handlers {
			if !managedCommandHandler(handler, spec, managedGroup, len(handlers)) {
				kept = append(kept, handler)
				continue
			}
			changed = true
			if !installed {
				kept = append(kept, desiredHandler)
				installed = true
				keptManagedHandler = true
			}
		}
		if !changed {
			updated = append(updated, item)
			continue
		}
		if len(kept) != 0 {
			preserved := cloneObject(group)
			preserved["hooks"] = kept
			if managedGroup && !keptManagedHandler {
				delete(preserved, "description")
			}
			updated = append(updated, preserved)
		}
	}
	if !installed {
		updated = append(updated, desired)
	}
	return updated, !reflect.DeepEqual(groups, updated)
}

func managedCommandHandler(value any, spec managedHandlerSpec, managedGroup bool, groupSize int) bool {
	handler, ok := value.(map[string]any)
	if !ok {
		return false
	}
	handlerType, _ := handler["type"].(string)
	if handlerType != "command" {
		return false
	}
	handlerCommand, _ := handler["command"].(string)
	if handlerCommand == spec.command {
		return true
	}
	statusMessage, _ := handler["statusMessage"].(string)
	for _, managedStatus := range spec.statusMessages {
		if statusMessage == managedStatus {
			return true
		}
	}
	return managedGroup && groupSize == 1
}

func cloneObject(value map[string]any) map[string]any {
	cloned := make(map[string]any, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

func groupHasCommand(group map[string]any, command string) bool {
	handlers, ok := group["hooks"].([]any)
	if !ok {
		return false
	}
	for _, item := range handlers {
		handler, ok := item.(map[string]any)
		if !ok {
			continue
		}
		handlerType, _ := handler["type"].(string)
		handlerCommand, _ := handler["command"].(string)
		if handlerType == "command" && handlerCommand == command {
			return true
		}
	}
	return false
}

func groupHasCommandWithTimeout(group map[string]any, command string, timeoutSeconds int64) bool {
	handlers, ok := group["hooks"].([]any)
	if !ok {
		return false
	}
	for _, item := range handlers {
		handler, ok := item.(map[string]any)
		if !ok {
			continue
		}
		handlerType, _ := handler["type"].(string)
		handlerCommand, _ := handler["command"].(string)
		timeout, ok := handler["timeout"].(json.Number)
		if handlerType != "command" || handlerCommand != command || !ok {
			continue
		}
		seconds, err := timeout.Int64()
		if err == nil && seconds == timeoutSeconds {
			return true
		}
	}
	return false
}

func matcherTargetsCompactOnly(value any) bool {
	matcher, ok := value.(string)
	if !ok {
		return false
	}
	compiled, err := regexp.Compile(matcher)
	if err != nil || !compiled.MatchString("compact") {
		return false
	}
	for _, other := range []string{"startup", "resume", "clear"} {
		if compiled.MatchString(other) {
			return false
		}
	}
	return true
}

func matcherTargetsBashOnly(value any) bool {
	matcher, ok := value.(string)
	return ok && matcher == "^Bash$"
}

func matcherTargetsReceiveCompletionOnly(value any) bool {
	matcher, ok := value.(string)
	if !ok {
		return false
	}
	compiled, err := regexp.Compile(matcher)
	if err != nil || !compiled.MatchString("Bash") || !compiled.MatchString(receiveMCPToolName) {
		return false
	}
	for _, other := range []string{"apply_patch", "mcp__waypost__waypost_send", "mcp__waypost__waypost_status"} {
		if compiled.MatchString(other) {
			return false
		}
	}
	return true
}

func matcherTargetsEverySessionEnd(group map[string]any) bool {
	value, exists := group["matcher"]
	if !exists || value == nil {
		return true
	}
	matcher, ok := value.(string)
	if !ok {
		return false
	}
	compiled, err := regexp.Compile(matcher)
	return err == nil && compiled.MatchString("other")
}

func validateHooksStructure(hooks map[string]any) error {
	for event, value := range hooks {
		groups, ok := value.([]any)
		if !ok {
			if knownHookEvent(event) {
				return fmt.Errorf("hooks.%s must be a JSON array", event)
			}
			continue
		}
		for groupIndex, value := range groups {
			group, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("hooks.%s[%d] must be a JSON object", event, groupIndex)
			}
			if matcher, exists := group["matcher"]; exists {
				if _, ok := matcher.(string); matcher != nil && !ok {
					return fmt.Errorf("hooks.%s[%d].matcher must be a string", event, groupIndex)
				}
			}
			handlers, ok := group["hooks"].([]any)
			if !ok {
				return fmt.Errorf("hooks.%s[%d].hooks must be a JSON array", event, groupIndex)
			}
			for handlerIndex, handler := range handlers {
				if _, ok := handler.(map[string]any); !ok {
					return fmt.Errorf("hooks.%s[%d].hooks[%d] must be a JSON object", event, groupIndex, handlerIndex)
				}
			}
		}
	}
	return nil
}

func knownHookEvent(name string) bool {
	switch name {
	case "PreToolUse", "PermissionRequest", "PostToolUse", "PreCompact", "PostCompact",
		"SessionStart", "SessionEnd", "UserPromptSubmit", "SubagentStart", "SubagentStop",
		"Stop", "Interrupt":
		return true
	default:
		return false
	}
}

func readHooksDocument(path string) (map[string]any, os.FileMode, error) {
	document, mode, err := readExistingHooksDocument(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]any{}, 0o600, nil
	}
	return document, mode, err
}

func readExistingHooksDocument(path string) (map[string]any, os.FileMode, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, fmt.Errorf("read Codex hooks %q: %w", path, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		return nil, 0, fmt.Errorf("parse Codex hooks %q: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return nil, 0, fmt.Errorf("parse Codex hooks %q: %w", path, err)
	}
	if document == nil {
		return nil, 0, fmt.Errorf("parse Codex hooks %q: expected a JSON object", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, fmt.Errorf("stat Codex hooks %q: %w", path, err)
	}
	return document, info.Mode().Perm(), nil
}

func objectField(document map[string]any, name string) (map[string]any, error) {
	value, ok := document[name]
	if !ok {
		return map[string]any{}, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("field %q must be a JSON object", name)
	}
	return object, nil
}

func existingObjectField(document map[string]any, name string) (map[string]any, error) {
	value, ok := document[name]
	if !ok {
		return nil, fmt.Errorf("field %q is missing", name)
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("field %q must be a JSON object", name)
	}
	return object, nil
}

func arrayField(document map[string]any, name string) ([]any, error) {
	value, ok := document[name]
	if !ok {
		return nil, nil
	}
	array, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("field %q must be a JSON array", name)
	}
	return array, nil
}

func existingArrayField(document map[string]any, name string) ([]any, error) {
	value, ok := document[name]
	if !ok {
		return nil, fmt.Errorf("field %q is missing", name)
	}
	array, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("field %q must be a JSON array", name)
	}
	return array, nil
}

func writeHooksDocument(path string, document map[string]any, mode os.FileMode) error {
	contents, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Codex hooks %q: %w", path, err)
	}
	contents = append(contents, '\n')
	writePath, err := resolveHooksWritePath(path)
	if err != nil {
		return err
	}
	writeDir := filepath.Dir(writePath)
	if err := os.MkdirAll(writeDir, 0o700); err != nil {
		return fmt.Errorf("create Codex hooks directory %q: %w", writeDir, err)
	}

	temporary, err := os.CreateTemp(writeDir, ".hooks.json.tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary Codex hooks file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(mode); err != nil {
		return fmt.Errorf("set Codex hooks permissions: %w", err)
	}
	if _, err := temporary.Write(contents); err != nil {
		return fmt.Errorf("write temporary Codex hooks: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary Codex hooks: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary Codex hooks: %w", err)
	}
	if err := replaceHooksFile(temporaryPath, writePath); err != nil {
		return fmt.Errorf("replace Codex hooks %q: %w", path, err)
	}
	return nil
}

func resolveHooksWritePath(path string) (string, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return path, nil
	}
	if err != nil {
		return "", fmt.Errorf("inspect Codex hooks %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return path, nil
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve Codex hooks symlink %q: %w", path, err)
	}
	return resolved, nil
}

func quoteCommandPath(path string) string {
	if runtime.GOOS == "windows" {
		return `"` + strings.ReplaceAll(path, `"`, `\"`) + `"`
	}
	return "'" + strings.ReplaceAll(path, "'", "'\"'\"'") + "'"
}
