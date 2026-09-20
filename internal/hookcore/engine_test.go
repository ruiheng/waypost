package hookcore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// transitionSpec exercises every lifecycle knob in one spec: a shell tool, a
// compact-aware SessionStart, a separate PostCompaction event whose context
// the harness drops, and a fallback event.
var transitionSpec = HarnessSpec{
	Label:               "Test",
	ShellTool:           "exec",
	CompactSource:       "compact",
	PostCompactionEvent: "PostCompaction",
	FallbackEvent:       "SessionStart",
	ServerDenialReason:  "managed by harness",
	EmitDeny: func(w io.Writer, reason string) error {
		_, err := io.WriteString(w, "deny: "+reason)
		return err
	},
	EmitProbeFailure: func(w io.Writer, probeErr error) error {
		_, err := io.WriteString(w, "probe-failure: "+probeErr.Error())
		return err
	},
	EmitNudgeProbeFailure: func(w io.Writer, probeErr error) error {
		_, err := io.WriteString(w, "nudge-probe-failure: "+probeErr.Error())
		return err
	},
	ReceiveSucceeded: func(input HookInput) bool {
		return input.ToolName == "mcp__waypost__waypost_recv"
	},
}

// TestHandleHookEventTransitions pins the shared lifecycle as a state ×
// event table: every harness runs these same transitions, so a divergence
// between hosts is a spec difference, not a copied-rule difference.
func TestHandleHookEventTransitions(t *testing.T) {
	t.Parallel()

	const session = "transition-session"

	cases := []struct {
		name        string
		initial     NudgeState
		event       string
		input       HookInput
		probe       MCPProbeRecord
		wantState   NudgeState // NudgeNone means the state file is cleared
		wantContext string     // additionalContext substring expected in output
		wantOutput  string     // non-additionalContext output substring
	}{
		// Nudge prompt: pending regardless of prior state; a new nudge
		// supersedes a pending compact guard.
		{name: "nudge from none", initial: NudgeNone, event: "UserPromptSubmit",
			input: HookInput{Prompt: DefaultNudgeMessage}, probe: MCPProbeRecord{Available: true},
			wantState: NudgePending, wantContext: MCPNudgeContext},
		{name: "nudge supersedes guard", initial: NudgeGuardPending, event: "UserPromptSubmit",
			input: HookInput{Prompt: DefaultNudgeMessage}, probe: MCPProbeRecord{Available: true},
			wantState: NudgePending, wantContext: MCPNudgeContext},
		{name: "nudge without MCP falls back to CLI", initial: NudgeNone, event: "UserPromptSubmit",
			input: HookInput{Prompt: DefaultNudgeMessage}, probe: MCPProbeRecord{Available: false},
			wantState: NudgePending, wantContext: CLINudgeContext},
		{name: "nudge with disabled receive falls back to CLI", initial: NudgeNone, event: "UserPromptSubmit",
			input:     HookInput{Prompt: DefaultNudgeMessage},
			probe:     MCPProbeRecord{Available: true, DisabledTools: []string{"waypost_recv"}},
			wantState: NudgePending, wantContext: CLINudgeContext},

		// Receive completion: only a pending nudge advances, and only on a
		// successful receive.
		{name: "receive consumes pending", initial: NudgePending, event: "PostToolUse",
			input:     HookInput{ToolName: "mcp__waypost__waypost_recv"},
			wantState: NudgeConsumed},
		{name: "unrelated tool keeps pending", initial: NudgePending, event: "PostToolUse",
			input:     HookInput{ToolName: "edit"},
			wantState: NudgePending},
		{name: "consumed ignores receive", initial: NudgeConsumed, event: "PostToolUse",
			input:     HookInput{ToolName: "mcp__waypost__waypost_recv"},
			wantState: NudgeConsumed},

		// PostCompaction: only a consumed nudge arms the pending guard.
		{name: "compaction arms guard", initial: NudgeConsumed, event: "PostCompaction",
			wantState: NudgeGuardPending, wantContext: AdditionalContext},
		{name: "compaction with none stays none", initial: NudgeNone, event: "PostCompaction",
			wantState: NudgeNone},
		{name: "compaction with pending stays pending", initial: NudgePending, event: "PostCompaction",
			wantState: NudgePending},

		// Guard delivery: the next honored event emits the guard and settles.
		{name: "guard delivered by tool use", initial: NudgeGuardPending, event: "PostToolUse",
			input:     HookInput{ToolName: "edit"},
			wantState: NudgeConsumed, wantContext: AdditionalContext},
		{name: "guard delivered by ordinary prompt", initial: NudgeGuardPending, event: "UserPromptSubmit",
			input:     HookInput{Prompt: "unrelated question"},
			wantState: NudgeNone, wantContext: AdditionalContext},
		{name: "guard delivered by session start", initial: NudgeGuardPending, event: "SessionStart",
			input:     HookInput{Source: "startup"},
			wantState: NudgeConsumed, wantContext: AdditionalContext},

		// Ordinary prompts clear terminal state without output.
		{name: "ordinary prompt clears pending", initial: NudgePending, event: "UserPromptSubmit",
			input:     HookInput{Prompt: "unrelated question"},
			wantState: NudgeNone},
		{name: "ordinary prompt clears consumed", initial: NudgeConsumed, event: "UserPromptSubmit",
			input:     HookInput{Prompt: "unrelated question"},
			wantState: NudgeNone},

		// SessionStart compact emission requires a consumed nudge and the
		// declared source.
		{name: "compact session start emits for consumed", initial: NudgeConsumed, event: "SessionStart",
			input:     HookInput{Source: "compact"},
			wantState: NudgeConsumed, wantContext: AdditionalContext},
		{name: "compact session start ignores none", initial: NudgeNone, event: "SessionStart",
			input:     HookInput{Source: "compact"},
			wantState: NudgeNone},
		{name: "non-compact session start quiet", initial: NudgeConsumed, event: "SessionStart",
			input:     HookInput{Source: "startup"},
			wantState: NudgeConsumed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := NewMemoryNudgeStore()
			if tc.initial != NudgeNone {
				if err := store.Save(session, tc.initial); err != nil {
					t.Fatalf("Save() error = %v", err)
				}
			}
			input := tc.input
			input.SessionID = session
			input.HookEventName = tc.event

			var out bytes.Buffer
			err := HandleHookEvent(context.Background(), input, true, &out, transitionSpec,
				func(context.Context) (MCPProbeRecord, error) { return tc.probe, nil }, store)
			if err != nil {
				t.Fatalf("HandleHookEvent() error = %v", err)
			}

			state, err := store.Load(session)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if state != tc.wantState {
				t.Errorf("state = %q, want %q", state, tc.wantState)
			}
			if tc.wantContext != "" && !strings.Contains(out.String(), tc.wantContext) {
				t.Errorf("output = %q, want additionalContext containing %q", out.String(), tc.wantContext)
			}
			if tc.wantContext == "" && tc.wantOutput == "" && out.Len() != 0 {
				t.Errorf("output = %q, want none", out.String())
			}
			if tc.wantOutput != "" && !strings.Contains(out.String(), tc.wantOutput) {
				t.Errorf("output = %q, want %q", out.String(), tc.wantOutput)
			}
		})
	}
}

func TestHandleHookEventPreToolUseGuards(t *testing.T) {
	t.Parallel()

	const session = "guard-session"

	shellInput := func(command string) HookInput {
		toolInput, err := json.Marshal(map[string]string{"command": command})
		if err != nil {
			t.Fatal(err)
		}
		return HookInput{SessionID: session, HookEventName: "PreToolUse", ToolName: "exec", ToolInput: toolInput}
	}

	t.Run("wait command gets polling guidance", func(t *testing.T) {
		var out bytes.Buffer
		err := HandleHookEvent(context.Background(), shellInput("waypost wait"), true, &out, transitionSpec,
			func(context.Context) (MCPProbeRecord, error) {
				t.Fatal("probe called for wait command")
				return MCPProbeRecord{}, nil
			}, NewMemoryNudgeStore())
		if err != nil {
			t.Fatalf("HandleHookEvent() error = %v", err)
		}
		if !strings.Contains(out.String(), WaitPollingContext) {
			t.Fatalf("output = %q, want wait polling context", out.String())
		}
	})

	t.Run("mcp subcommand always denied", func(t *testing.T) {
		var out bytes.Buffer
		err := HandleHookEvent(context.Background(), shellInput("waypost mcp"), true, &out, transitionSpec,
			func(context.Context) (MCPProbeRecord, error) {
				t.Fatal("probe called for always-denied command")
				return MCPProbeRecord{}, nil
			}, NewMemoryNudgeStore())
		if err != nil {
			t.Fatalf("HandleHookEvent() error = %v", err)
		}
		if !strings.Contains(out.String(), "deny: managed by harness") {
			t.Fatalf("output = %q, want denial", out.String())
		}
	})

	t.Run("guarded command denied when tool usable", func(t *testing.T) {
		var out bytes.Buffer
		err := HandleHookEvent(context.Background(), shellInput("waypost recv"), true, &out, transitionSpec,
			func(context.Context) (MCPProbeRecord, error) {
				return MCPProbeRecord{Available: true}, nil
			}, NewMemoryNudgeStore())
		if err != nil {
			t.Fatalf("HandleHookEvent() error = %v", err)
		}
		if !strings.Contains(out.String(), "deny: ") {
			t.Fatalf("output = %q, want denial", out.String())
		}
	})

	t.Run("guarded command denied behind cd prefix", func(t *testing.T) {
		var out bytes.Buffer
		err := HandleHookEvent(context.Background(), shellInput("cd /tmp && waypost send --to a --body-file /b"), true, &out, transitionSpec,
			func(context.Context) (MCPProbeRecord, error) {
				return MCPProbeRecord{Available: true}, nil
			}, NewMemoryNudgeStore())
		if err != nil {
			t.Fatalf("HandleHookEvent() error = %v", err)
		}
		if !strings.Contains(out.String(), "deny: ") {
			t.Fatalf("output = %q, want denial", out.String())
		}
	})

	t.Run("mcp subcommand denied behind cd prefix without probe", func(t *testing.T) {
		var out bytes.Buffer
		err := HandleHookEvent(context.Background(), shellInput("cd /tmp && waypost mcp"), true, &out, transitionSpec,
			func(context.Context) (MCPProbeRecord, error) {
				t.Fatal("probe called for always-denied command")
				return MCPProbeRecord{}, nil
			}, NewMemoryNudgeStore())
		if err != nil {
			t.Fatalf("HandleHookEvent() error = %v", err)
		}
		if !strings.Contains(out.String(), "deny: managed by harness") {
			t.Fatalf("output = %q, want denial", out.String())
		}
	})

	t.Run("denial wins over wait guidance in compound command", func(t *testing.T) {
		var out bytes.Buffer
		err := HandleHookEvent(context.Background(), shellInput("waypost wait && waypost send --to a"), true, &out, transitionSpec,
			func(context.Context) (MCPProbeRecord, error) {
				return MCPProbeRecord{Available: true}, nil
			}, NewMemoryNudgeStore())
		if err != nil {
			t.Fatalf("HandleHookEvent() error = %v", err)
		}
		if !strings.Contains(out.String(), "deny: ") {
			t.Fatalf("output = %q, want denial", out.String())
		}
	})

	t.Run("guarded command allowed when tool disabled", func(t *testing.T) {
		var out bytes.Buffer
		err := HandleHookEvent(context.Background(), shellInput("waypost recv"), true, &out, transitionSpec,
			func(context.Context) (MCPProbeRecord, error) {
				return MCPProbeRecord{Available: true, DisabledTools: []string{"waypost_recv"}}, nil
			}, NewMemoryNudgeStore())
		if err != nil {
			t.Fatalf("HandleHookEvent() error = %v", err)
		}
		if out.Len() != 0 {
			t.Fatalf("output = %q, want CLI fallback allowed", out.String())
		}
	})

	t.Run("probe failure surfaces through spec", func(t *testing.T) {
		var out bytes.Buffer
		err := HandleHookEvent(context.Background(), shellInput("waypost recv"), true, &out, transitionSpec,
			func(context.Context) (MCPProbeRecord, error) {
				return MCPProbeRecord{}, errors.New("probe exploded")
			}, NewMemoryNudgeStore())
		if err != nil {
			t.Fatalf("HandleHookEvent() error = %v", err)
		}
		if !strings.Contains(out.String(), "probe-failure: ") {
			t.Fatalf("output = %q, want probe failure surfaced", out.String())
		}
	})

	t.Run("non-shell tool ignored", func(t *testing.T) {
		var out bytes.Buffer
		input := shellInput("waypost recv")
		input.ToolName = "edit"
		err := HandleHookEvent(context.Background(), input, true, &out, transitionSpec,
			func(context.Context) (MCPProbeRecord, error) {
				t.Fatal("probe called for non-shell tool")
				return MCPProbeRecord{}, nil
			}, NewMemoryNudgeStore())
		if err != nil {
			t.Fatalf("HandleHookEvent() error = %v", err)
		}
		if out.Len() != 0 {
			t.Fatalf("output = %q, want none", out.String())
		}
	})
}

func TestHandleHookEventSessionEndClearsStateAndProbe(t *testing.T) {
	t.Parallel()

	const session = "cleanup-session"

	store := NewMemoryNudgeStore()
	if err := store.Save(session, NudgeGuardPending); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveMCPProbe(session, MCPProbeRecord{Available: true}); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	err := HandleHookEvent(context.Background(),
		HookInput{SessionID: session, HookEventName: "SessionEnd"}, true, &out, transitionSpec,
		func(context.Context) (MCPProbeRecord, error) {
			t.Fatal("probe called for SessionEnd")
			return MCPProbeRecord{}, nil
		}, store)
	if err != nil {
		t.Fatalf("HandleHookEvent() error = %v", err)
	}
	if state, _ := store.Load(session); state != NudgeNone {
		t.Fatalf("state = %q, want cleared", state)
	}
	if _, ok, _ := store.LoadMCPProbe(session); ok {
		t.Fatal("MCP probe cache survived SessionEnd")
	}
}

func TestHandleHookEventMissingSessionDegrades(t *testing.T) {
	t.Parallel()

	for _, event := range []string{"PostCompaction", "SessionStart", "PostToolUse", "SessionEnd"} {
		t.Run(event, func(t *testing.T) {
			var out bytes.Buffer
			err := HandleHookEvent(context.Background(),
				HookInput{HookEventName: event}, true, &out, transitionSpec,
				func(context.Context) (MCPProbeRecord, error) {
					return MCPProbeRecord{Available: true}, nil
				}, NewMemoryNudgeStore())
			if err != nil {
				t.Fatalf("HandleHookEvent() error = %v", err)
			}
		})
	}
}

func TestHandleHookEventEmptyInputEmitsFallback(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	err := HandleHookEvent(context.Background(), HookInput{}, false, &out, transitionSpec,
		func(context.Context) (MCPProbeRecord, error) {
			t.Fatal("probe called without input")
			return MCPProbeRecord{}, nil
		}, NewMemoryNudgeStore())
	if err != nil {
		t.Fatalf("HandleHookEvent() error = %v", err)
	}
	var decoded struct {
		HookSpecificOutput struct {
			HookEventName     string `json:"hookEventName"`
			AdditionalContext string `json:"additionalContext"`
		} `json:"hookSpecificOutput"`
	}
	if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
		t.Fatalf("output = %q, decode error = %v", out.String(), err)
	}
	if decoded.HookSpecificOutput.HookEventName != "SessionStart" {
		t.Fatalf("hookEventName = %q, want fallback SessionStart", decoded.HookSpecificOutput.HookEventName)
	}
	if !strings.Contains(decoded.HookSpecificOutput.AdditionalContext, AdditionalContext) {
		t.Fatalf("additionalContext = %q, want compact guard", decoded.HookSpecificOutput.AdditionalContext)
	}
}
