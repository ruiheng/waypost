package hookcore

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
)

// HarnessSpec declares the per-harness variation of the Waypost hook
// lifecycle as data: event names, tool names, and the output envelopes the
// harness understands. The lifecycle itself lives once in HandleHookEvent,
// so a new harness is a spec declaration plus wire adapters rather than a
// copy of the state machine.
type HarnessSpec struct {
	// Label names the harness in errors ("Devin", "Codex").
	Label string

	// ShellTool is the shell-command tool whose tool_input carries
	// {"command": ...} ("exec", "Bash").
	ShellTool string

	// CompactSource is the SessionStart source marking a post-compaction
	// start ("compact"). Empty disables the direct SessionStart compact
	// emission.
	CompactSource string

	// PostCompactionEvent names the harness's post-compaction hook event, or
	// empty when the harness reports compaction only through SessionStart.
	// A consumed nudge is marked NudgeGuardPending on that event so the next
	// PostToolUse, UserPromptSubmit, or SessionStart re-injects the guard:
	// required for harnesses that drop the event's additionalContext,
	// harmless for ones that honor it.
	PostCompactionEvent string

	// FallbackEvent names the hookEventName reported when the harness sends
	// no payload (a manual invocation or an empty stdin).
	FallbackEvent string

	// ServerDenialReason explains why `waypost mcp` is always denied.
	ServerDenialReason string

	// EmitDeny writes the harness's PreToolUse block decision for reason.
	EmitDeny func(w io.Writer, reason string) error

	// EmitProbeFailure surfaces a failed MCP probe during a guarded
	// PreToolUse command check.
	EmitProbeFailure func(w io.Writer, probeErr error) error

	// EmitNudgeProbeFailure surfaces a failed MCP probe while handling the
	// nudge prompt.
	EmitNudgeProbeFailure func(w io.Writer, probeErr error) error

	// EmitInputTimeout reports a stdin read that exceeded RunBudget. Nil
	// returns the timeout as an ordinary error; set it for harnesses that
	// expect a hook output instead of a nonzero exit.
	EmitInputTimeout func(w io.Writer) error

	// ReceiveSucceeded reports whether input records a completed Waypost
	// receive: the MCP receive tool result or a shell `waypost recv` /
	// `waypost receive` invocation.
	ReceiveSucceeded func(input HookInput) bool
}

// ReceiveToolName is the bare Waypost MCP tool a nudge steers toward.
const ReceiveToolName = "waypost_recv"

// RunHook runs one hook invocation end to end: it bounds the run by
// RunBudget, decodes the harness payload, applies the spec's input-timeout
// policy, and dispatches the shared lifecycle. Harness packages keep only
// store/probe injection and the exported wrapper.
func RunHook(
	ctx context.Context,
	r io.Reader,
	w io.Writer,
	spec HarnessSpec,
	probe func(context.Context) (MCPProbeRecord, error),
	store NudgeStore,
) error {
	ctx, cancel := BeginRun(ctx, r)
	defer cancel()
	input, hasInput, err := ReadHookInput(r, spec.Label)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) && spec.EmitInputTimeout != nil {
			return spec.EmitInputTimeout(w)
		}
		return err
	}
	return HandleHookEvent(ctx, input, hasInput, w, spec, probe, store)
}

// HandleHookEvent applies the shared Waypost hook lifecycle to one decoded
// harness payload: the compact guard, the nudge prompt, receive completion,
// the PreToolUse command guards, and session cleanup. Per-harness
// differences come from spec.
func HandleHookEvent(
	ctx context.Context,
	input HookInput,
	hasInput bool,
	w io.Writer,
	spec HarnessSpec,
	probe func(context.Context) (MCPProbeRecord, error),
	store NudgeStore,
) error {
	if !hasInput {
		return WriteHookContext(w, spec.FallbackEvent, AdditionalContext)
	}

	// Harnesses guarantee a stable session_id in every hook payload; a
	// missing value means the event came from a different producer, so state
	// tracking degrades gracefully instead of failing the hook.
	sessionID := strings.TrimSpace(input.SessionID)
	switch {
	case spec.PostCompactionEvent != "" && input.HookEventName == spec.PostCompactionEvent:
		if sessionID == "" {
			return nil
		}
		state, err := store.Load(sessionID)
		if err != nil {
			return err
		}
		if state != NudgeConsumed {
			return nil
		}
		// Harnesses that drop PostCompaction additionalContext carry the
		// guard as pending state so the next PostToolUse or
		// UserPromptSubmit re-injects it. The output is still emitted for
		// harnesses that honor it.
		if err := store.Save(sessionID, NudgeGuardPending); err != nil {
			return err
		}
		return WriteHookContext(w, spec.PostCompactionEvent, AdditionalContext)
	case input.HookEventName == "SessionStart":
		if sessionID == "" {
			return nil
		}
		state, err := store.Load(sessionID)
		if err != nil {
			return err
		}
		if state == NudgeGuardPending {
			// A pending guard survives restarts; SessionStart delivers it
			// regardless of source and re-arms for the next compaction.
			if err := store.Save(sessionID, NudgeConsumed); err != nil {
				return err
			}
			return WriteHookContext(w, "SessionStart", AdditionalContext)
		}
		if spec.CompactSource == "" || input.Source != spec.CompactSource || state != NudgeConsumed {
			return nil
		}
		return WriteHookContext(w, "SessionStart", AdditionalContext)
	case input.HookEventName == "UserPromptSubmit":
		if !LooksLikeWaypostNudge(input.Prompt) {
			if sessionID == "" {
				return nil
			}
			state, err := store.Load(sessionID)
			if err != nil {
				return err
			}
			if err := store.Clear(sessionID); err != nil {
				return err
			}
			if state == NudgeGuardPending {
				// An ordinary prompt is another honored delivery channel
				// for a pending compact guard.
				return WriteHookContext(w, "UserPromptSubmit", AdditionalContext)
			}
			return nil
		}
		if sessionID != "" {
			if err := store.Save(sessionID, NudgePending); err != nil {
				return err
			}
		}
		record, err := ProbeAndCache(ctx, sessionID, probe, store)
		switch {
		case err != nil:
			return spec.EmitNudgeProbeFailure(w, err)
		case MCPToolUsable(record, ReceiveToolName):
			return WriteHookContext(w, "UserPromptSubmit", MCPNudgeContext)
		default:
			return WriteHookContext(w, "UserPromptSubmit", CLINudgeContext)
		}
	case input.HookEventName == "PostToolUse":
		if sessionID == "" {
			return nil
		}
		state, err := store.Load(sessionID)
		if err != nil {
			return err
		}
		if state == NudgeGuardPending {
			// Harnesses that drop the compact event's context install a
			// match-everything matcher so the first post-compaction tool
			// call delivers the pending guard.
			if err := store.Save(sessionID, NudgeConsumed); err != nil {
				return err
			}
			return WriteHookContext(w, "PostToolUse", AdditionalContext)
		}
		if state != NudgePending || !spec.ReceiveSucceeded(input) {
			return nil
		}
		return store.Save(sessionID, NudgeConsumed)
	case input.HookEventName == "PreToolUse":
		if input.ToolName != spec.ShellTool {
			return nil
		}
		command, err := CommandToolInput(input.ToolInput, spec.Label+" "+spec.ShellTool)
		if err != nil {
			return err
		}
		if LooksLikeWaypostWaitCommand(command) {
			return WriteHookContext(w, "PreToolUse", WaitPollingContext)
		}
		tool, guarded := WaypostMCPTool(command)
		if !guarded {
			return nil
		}
		reason := WaypostCommandDenialReason(tool, spec.ServerDenialReason)
		if tool == "" {
			// `waypost mcp` is always denied; the harness manages the server.
			return spec.EmitDeny(w, reason)
		}
		record, err := ProbeAndCache(ctx, sessionID, probe, store)
		if err != nil {
			return spec.EmitProbeFailure(w, err)
		}
		if !MCPToolUsable(record, tool) {
			// The MCP tool is absent or disabled; keep the CLI fallback
			// usable.
			return nil
		}
		return spec.EmitDeny(w, reason)
	case input.HookEventName == "SessionEnd":
		if sessionID == "" {
			return nil
		}
		if err := store.Clear(sessionID); err != nil {
			return err
		}
		return store.ClearMCPProbe(sessionID)
	default:
		return nil
	}
}

type hookContextOutput struct {
	HookSpecificOutput hookSpecificContext `json:"hookSpecificOutput"`
}

type hookSpecificContext struct {
	HookEventName     string `json:"hookEventName"`
	AdditionalContext string `json:"additionalContext,omitempty"`
}

// WriteHookContext emits the shared hookSpecificOutput additionalContext
// envelope the harnesses understand.
func WriteHookContext(w io.Writer, eventName, additionalContext string) error {
	return json.NewEncoder(w).Encode(hookContextOutput{
		HookSpecificOutput: hookSpecificContext{
			HookEventName:     eventName,
			AdditionalContext: additionalContext,
		},
	})
}

// MCPToolUsable reports whether record marks the Waypost MCP tool usable:
// the server is available and the tool is not disabled. Both names are
// canonicalized so a namespaced mcp__waypost__<tool> id matches the bare
// tool name.
func MCPToolUsable(record MCPProbeRecord, tool string) bool {
	if !record.Available {
		return false
	}
	canonical := CanonicalWaypostToolName(tool)
	for _, disabled := range record.DisabledTools {
		if CanonicalWaypostToolName(disabled) == canonical {
			return false
		}
	}
	return true
}

// CanonicalWaypostToolName maps a namespaced MCP tool id
// (mcp__waypost__<tool>) to the bare tool name used for availability checks.
// Ids belonging to other MCP servers are left untouched.
func CanonicalWaypostToolName(name string) string {
	if rest, ok := strings.CutPrefix(name, "mcp__waypost__"); ok && rest != "" {
		return rest
	}
	return name
}
