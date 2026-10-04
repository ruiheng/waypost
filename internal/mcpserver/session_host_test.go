package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/ruiheng/waypost/internal/waypost"
)

const (
	thurboxPlannerID = "11111111-1111-4111-8111-111111111111"
	thurboxAuthorID  = "22222222-2222-4222-8222-222222222222"
	thurboxReviewID  = "33333333-3333-4333-8333-333333333333"
)

func TestGenericSessionToolSchemasAreHostNeutral(t *testing.T) {
	createSchema, err := jsonschema.For[sessionCreateInput](nil)
	if err != nil {
		t.Fatalf("jsonschema.For(sessionCreateInput): %v", err)
	}
	for _, field := range []string{"session_name", "workdir", "parent_session_id"} {
		if !containsString(createSchema.Required, field) {
			t.Fatalf("create required fields = %v, want %q", createSchema.Required, field)
		}
	}
	for _, field := range []string{"launch_profile", "ensure_cmd", "group_path", "startup_instruction", "thurbox_agent"} {
		if _, ok := createSchema.Properties[field]; ok {
			t.Fatalf("generic create schema unexpectedly exposes host-specific %q", field)
		}
	}
	for _, field := range []string{"full_command_line", "thurbox_agent_key", "transition_notify", "assert_done"} {
		if _, ok := createSchema.Properties[field]; !ok {
			t.Fatalf("generic create schema does not expose optional launch field %q", field)
		}
		if containsString(createSchema.Required, field) {
			t.Fatalf("generic create schema unexpectedly requires optional launch field %q: %v", field, createSchema.Required)
		}
	}

	requireSchema, err := jsonschema.For[sessionRequireInput](nil)
	if err != nil {
		t.Fatalf("jsonschema.For(sessionRequireInput): %v", err)
	}
	if !containsString(requireSchema.Required, "workdir") {
		t.Fatalf("require required fields = %v, want workdir", requireSchema.Required)
	}
	if _, ok := requireSchema.Properties["auto_restart"]; !ok {
		t.Fatalf("generic require schema does not expose optional auto_restart")
	}
	if containsString(requireSchema.Required, "auto_restart") {
		t.Fatalf("generic require schema unexpectedly requires auto_restart: %v", requireSchema.Required)
	}
	for _, field := range []string{"launch_profile", "full_command_line", "thurbox_agent_key", "ensure_cmd", "group_path"} {
		if _, ok := requireSchema.Properties[field]; ok {
			t.Fatalf("generic require schema unexpectedly exposes %q", field)
		}
	}
}

func TestSelectedHostLaunchValueValidatesOnlySelectedField(t *testing.T) {
	for _, test := range []struct {
		name    string
		host    sessionHost
		command string
		key     string
		want    string
		wantErr string
	}{
		{
			name:    "agent deck trims command",
			host:    sessionHostAgentDeck,
			command: "  codex --model gpt-5.6  ",
			key:     "ignored\x00key",
			want:    "codex --model gpt-5.6",
		},
		{
			name:    "thurbox trims key",
			host:    sessionHostThurbox,
			command: "ignored\x00command",
			key:     "  codex  ",
			want:    "codex",
		},
		{
			name:    "agent deck blank",
			host:    sessionHostAgentDeck,
			key:     "valid-but-irrelevant",
			wantErr: "full_command_line is required when creating an agent-deck session",
		},
		{
			name:    "thurbox blank",
			host:    sessionHostThurbox,
			command: "valid-but-irrelevant",
			wantErr: "thurbox_agent_key is required when creating a thurbox session",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := selectedHostLaunchValue(test.host, test.command, test.key)
			if test.wantErr != "" {
				if err == nil || err.Error() != test.wantErr {
					t.Fatalf("selectedHostLaunchValue() error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("selectedHostLaunchValue() = %q, err=%v; want %q", got, err, test.want)
			}
		})
	}
}

func TestGenericSessionCreateRejectsRemovedAndWrongTypeLaunchFieldsAtSchema(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("schema-invalid create must not run host command: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	base := map[string]any{
		"host":              "agent-deck",
		"session_name":      "architect-reviewer",
		"workdir":           t.TempDir(),
		"parent_session_id": "agent-parent",
		"full_command_line": "codex",
	}
	removed := cloneSessionCreateTestArgs(base)
	removed["launch_profile"] = "architect"
	if err := callServiceToolExpectError(t, service, "session_create", removed); err == nil || !strings.Contains(err.Error(), "launch_profile") {
		t.Fatalf("removed launch_profile error = %v, want schema rejection", err)
	}
	wrongType := cloneSessionCreateTestArgs(base)
	wrongType["thurbox_agent_key"] = []any{"not-a-string"}
	if err := callServiceToolExpectError(t, service, "session_create", wrongType); err == nil {
		t.Fatal("wrong-type irrelevant launch field unexpectedly passed schema validation")
	}
}

func cloneSessionCreateTestArgs(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func TestThurboxV171FixturesUseStrictPinnedGrammar(t *testing.T) {
	get := thurboxFixture(t, "session-get.json")
	record, err := parseThurboxSessionRecord(get, "thurbox v1.7.1 session get")
	if err != nil {
		t.Fatalf("parseThurboxSessionRecord(get fixture): %v", err)
	}
	if record.Host != sessionHostThurbox || record.ID != thurboxAuthorID || record.Name != "architect-author" || record.Path != "/workspace/waypost" || record.Status != "idle" || record.ParentSessionID != thurboxPlannerID {
		t.Fatalf("parsed get fixture = %+v", record)
	}

	listed, err := parseThurboxSessionList(thurboxFixture(t, "session-list.json"))
	if err != nil {
		t.Fatalf("parseThurboxSessionList(list fixture): %v", err)
	}
	if len(listed) != 2 || listed[0].ID != thurboxPlannerID || listed[0].Status != "" || listed[1].ID != thurboxAuthorID || listed[1].Status != "idle" {
		t.Fatalf("parsed list fixture = %+v", listed)
	}

	created, err := parseThurboxCreatedSession(thurboxFixture(t, "session-create.json"))
	if err != nil {
		t.Fatalf("parseThurboxCreatedSession(create fixture): %v", err)
	}
	if created.ID != thurboxReviewID || created.Name != "architect-reviewer" || created.ParentSessionID != thurboxPlannerID || created.Path != "/workspace/waypost" {
		t.Fatalf("parsed create fixture = %+v", created)
	}
	if err := parseThurboxRestartResult(thurboxFixture(t, "session-restart.json"), thurboxAuthorID); err != nil {
		t.Fatalf("parseThurboxRestartResult(restart fixture): %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{
			name: "missing effective cwd",
			mutate: func(payload map[string]any) {
				delete(payload, "cwd")
			},
			want: `no "cwd" field`,
		},
		{
			name: "lookalike path is rejected",
			mutate: func(payload map[string]any) {
				payload["repo_path"] = "/wrong"
			},
			want: "unknown fields",
		},
		{
			name: "unclassified hook state",
			mutate: func(payload map[string]any) {
				payload["hook_state"] = "error"
			},
			want: "unclassified hook_state",
		},
		{
			name: "invalid session id",
			mutate: func(payload map[string]any) {
				payload["id"] = "not-a-thurbox-uuid"
			},
			want: "invalid session id",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseThurboxSessionRecord(mutateThurboxObjectFixture(t, "session-get.json", test.mutate), "thurbox v1.7.1 session get")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("strict parser error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestThurboxResolveRejectsDuplicateExactNames(t *testing.T) {
	workdir := t.TempDir()
	first := thurboxSessionRecord(t, thurboxAuthorID, "duplicate", workdir, thurboxPlannerID, "idle")
	second := thurboxSessionRecord(t, thurboxReviewID, "duplicate", workdir, thurboxPlannerID, "idle")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		want := []string{"thurbox-cli", "session", "list", "--json"}
		if !reflect.DeepEqual(args, want) {
			t.Fatalf("command args = %v, want %v", args, want)
		}
		return RunResult{ExitCode: 0, Stdout: "[" + first + "," + second + "]"}, nil
	}}
	manager := newSessionManager(commandRunner, &serverState{})
	_, err := manager.resolveThurboxSession(context.Background(), "duplicate", syncCmdTimeout)
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("resolveThurboxSession duplicate error = %v, want exact-name ambiguity", err)
	}
}

func TestGenericSessionRequirePrefersNestedThurbox(t *testing.T) {
	t.Setenv("THURBOX_SESSION", thurboxAuthorID)
	t.Setenv("AGENTDECK_INSTANCE_ID", "outer-agent-deck")
	clearToolSessionEnvs(t)
	workdir := t.TempDir()

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		if !reflect.DeepEqual(args, []string{"thurbox-cli", "session", "list", "--json"}) {
			t.Fatalf("unexpected command; nested Thurbox must win host selection: %v", args)
		}
		return RunResult{ExitCode: 0, Stdout: "[" + thurboxSessionRecord(t, thurboxAuthorID, "architect-author", workdir, thurboxPlannerID, "idle") + "]"}, nil
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	output := callServiceTool(t, service, "session_require", map[string]any{
		"session_ref":  "architect-author",
		"workdir":      workdir,
		"auto_restart": false,
	})
	if output["host"] != "thurbox" || output["status"] != "ready" || output["session_id"] != thurboxAuthorID {
		t.Fatalf("nested require output = %v", output)
	}
	if _, ok := output["group"]; ok {
		t.Fatalf("generic require leaked Agent Deck group: %v", output)
	}
}

func TestExplicitSessionHostOverridesNestedThurbox(t *testing.T) {
	t.Setenv("THURBOX_SESSION", thurboxAuthorID)
	manager := newSessionManager(&fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		t.Fatalf("explicit host selection should not probe a host: %v", args)
		return RunResult{}, nil
	}}, &serverState{})
	host, err := manager.selectSessionHost(context.Background(), "agent-deck")
	if err != nil || host != sessionHostAgentDeck {
		t.Fatalf("explicit host selection = %q, err=%v; want agent-deck", host, err)
	}
}

func TestNestedThurboxAutoBindingAndManualBindingPrecedence(t *testing.T) {
	t.Run("auto binds immediate host first", func(t *testing.T) {
		t.Setenv("THURBOX_SESSION", thurboxAuthorID)
		t.Setenv("AGENTDECK_INSTANCE_ID", "outer-agent-deck")
		clearToolSessionEnvs(t)
		commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			if !reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "outer-agent-deck", "--json"}) {
				t.Fatalf("unexpected auto-bind command: %v", args)
			}
			return RunResult{ExitCode: 0, Stdout: `{"id":"outer-agent-deck","title":"outer","status":"waiting","path":"/tmp"}`}, nil
		}}
		service := newService(Options{
			WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
			CommandRunner:         commandRunner,
			DisableWakeScheduler:  true,
			DisableLeaseRenewLoop: true,
		})
		service.sessions.parentPID = func() int { return 0 }

		status := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
		wantAddresses := []any{"thurbox/" + thurboxAuthorID, "agent-deck/outer-agent-deck"}
		if got := status["bound_addresses"]; !reflect.DeepEqual(got, wantAddresses) {
			t.Fatalf("bound_addresses = %v, want %v", got, wantAddresses)
		}
		if status["default_sender"] != "thurbox/"+thurboxAuthorID || status["detected_thurbox_session_id"] != thurboxAuthorID || status["detected_agent_deck_session_id"] != "outer-agent-deck" {
			t.Fatalf("nested binding status = %v", status)
		}
	})

	t.Run("manual binding is authoritative", func(t *testing.T) {
		t.Setenv("THURBOX_SESSION", thurboxAuthorID)
		clearToolSessionEnvs(t)
		service := newService(Options{
			WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
			CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				t.Fatalf("manual binding must suppress auto-detection command: %v", args)
				return RunResult{}, nil
			}},
			DisableWakeScheduler:  true,
			DisableLeaseRenewLoop: true,
		})

		callServiceTool(t, service, "waypost_bind", map[string]any{"addresses": []string{"agent-deck/manual"}})
		status := callServiceTool(t, service, "waypost_status", map[string]any{})
		if got := status["bound_addresses"]; !reflect.DeepEqual(got, []any{"agent-deck/manual"}) {
			t.Fatalf("manual bound_addresses = %v, want only explicit address", got)
		}
		if status["default_sender"] != "agent-deck/manual" {
			t.Fatalf("manual default_sender = %v, want agent-deck/manual", status["default_sender"])
		}
	})
}

func TestInvalidNestedThurboxIdentityOnlyWarns(t *testing.T) {
	t.Setenv("THURBOX_SESSION", "not-a-thurbox-uuid")
	t.Setenv("AGENTDECK_INSTANCE_ID", "outer-agent-deck")
	clearToolSessionEnvs(t)
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		if !reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "outer-agent-deck", "--json"}) {
			t.Fatalf("unexpected command args: %v", args)
		}
		return RunResult{ExitCode: 0, Stdout: `{"id":"outer-agent-deck","title":"outer","status":"waiting","path":"/tmp"}`}, nil
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: &fakeWaypostService{t: t}},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.sessions.parentPID = func() int { return 0 }

	status := callServiceTool(t, service, "waypost_status", map[string]any{"include_diagnostics": true})
	if got := status["bound_addresses"]; !reflect.DeepEqual(got, []any{"agent-deck/outer-agent-deck"}) {
		t.Fatalf("invalid Thurbox binding addresses = %v", got)
	}
	if status["detected_thurbox_session_id"] != nil {
		t.Fatalf("invalid THURBOX_SESSION was treated as detected: %v", status)
	}
	warnings, ok := status["warnings"].([]any)
	if !ok || !anyStringContains(warnings, "not a valid Thurbox v1.7.1 session UUID") {
		t.Fatalf("invalid THURBOX_SESSION warnings = %v", status["warnings"])
	}
}

func TestGenericSessionCreateRequiresSelectedLaunchValueBeforeHostCommands(t *testing.T) {
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("missing selected launch value must not run a host command: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	err := callServiceToolExpectError(t, service, "session_create", map[string]any{
		"host":              "thurbox",
		"session_name":      "architect-reviewer",
		"workdir":           t.TempDir(),
		"parent_session_id": thurboxPlannerID,
	})
	if err == nil || !strings.Contains(err.Error(), "thurbox_agent_key is required") {
		t.Fatalf("missing selected launch value error = %v", err)
	}
}

func TestGenericSessionCreateUsesCallerSuppliedThurboxKeyAcrossParentWorkdir(t *testing.T) {
	workdir := t.TempDir()
	canonicalWorkdir := canonicalTestWorkdir(t, workdir)
	parentWorkdir := canonicalTestWorkdir(t, t.TempDir())
	parent := thurboxSessionRecord(t, thurboxPlannerID, "planner", parentWorkdir, "", "idle")
	created := thurboxCreatedSessionRecord(t, thurboxReviewID, "architect-reviewer", canonicalWorkdir, thurboxPlannerID)
	refreshed := thurboxSessionRecord(t, thurboxReviewID, "architect-reviewer", canonicalWorkdir, thurboxPlannerID, "idle")

	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", thurboxPlannerID}):
			return RunResult{ExitCode: 0, Stdout: parent}, nil
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "list", "--json"}):
			return RunResult{ExitCode: 0, Stdout: "[]"}, nil
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "create", "--json", "--name", "architect-reviewer", "--repo-path", canonicalWorkdir, "--agent", "codex", "--parent", thurboxPlannerID}):
			return RunResult{ExitCode: 0, Stdout: created}, nil
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", thurboxReviewID}):
			return RunResult{ExitCode: 0, Stdout: refreshed}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	output := callServiceTool(t, service, "session_create", map[string]any{
		"host":              "thurbox",
		"session_name":      "architect-reviewer",
		"workdir":           workdir,
		"parent_session_id": thurboxPlannerID,
		"full_command_line": "ignored-agent-deck-value",
		"thurbox_agent_key": "  codex  ",
	})
	if output["host"] != "thurbox" || output["status"] != "created" || output["session_id"] != thurboxReviewID || output["created_target"] != true || output["started_session"] != true || output["recovery_required"] != false {
		t.Fatalf("generic create output = %v", output)
	}
	verification, ok := output["verification"].(map[string]any)
	if !ok || verification["state"] != "verified" || verification["requested_workdir"] != canonicalWorkdir {
		t.Fatalf("create verification = %v", output["verification"])
	}
	for _, forbidden := range []string{"group", "title", "ensure_cmd", "thurbox_agent", "launch_profile", "full_command_line", "thurbox_agent_key", "transition_notify", "assert_done"} {
		if _, ok := output[forbidden]; ok {
			t.Fatalf("generic create leaked %q: %v", forbidden, output)
		}
	}
}

func TestGenericAgentDeckCreateAllowsDifferentParentWorkdirAndUsesAuthoritativeRecord(t *testing.T) {
	workdir := t.TempDir()
	canonicalWorkdir := canonicalTestWorkdir(t, workdir)
	parentWorkdir := canonicalTestWorkdir(t, t.TempDir())
	parent := `{"id":"agent-parent","title":"planner","status":"waiting","group":"waypost","path":` + jsonString(t, parentWorkdir) + `}`
	launchReceipt := `{"id":"agent-child"}`
	refreshed := `{"id":"agent-child","title":"architect-reviewer","status":"waiting","group":"waypost","path":` + jsonString(t, canonicalWorkdir) + `,"parent_session_id":"agent-parent"}`
	launchCalls := 0
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-parent", "--json"}):
			return RunResult{ExitCode: 0, Stdout: parent}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "architect-reviewer", "--json"}):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "launch", "--json", "--title", "architect-reviewer", "--cmd", "codex --model gpt-5.6", "--group", "waypost", "--parent", "agent-parent", "--no-transition-notify", "--no-assert-done", canonicalWorkdir}):
			launchCalls++
			return RunResult{ExitCode: 0, Stdout: launchReceipt}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-child", "--json"}):
			return RunResult{ExitCode: 0, Stdout: refreshed}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	output := callServiceTool(t, service, "session_create", map[string]any{
		"host":              "agent-deck",
		"session_name":      "architect-reviewer",
		"workdir":           workdir,
		"parent_session_id": "agent-parent",
		"full_command_line": "  codex --model gpt-5.6  ",
		"thurbox_agent_key": "ignored-thurbox-key",
	})
	if output["host"] != "agent-deck" || output["status"] != "created" || output["session_id"] != "agent-child" || output["parent_session_id"] != "agent-parent" {
		t.Fatalf("generic Agent Deck create output = %v", output)
	}
	if launchCalls != 1 {
		t.Fatalf("agent-deck launch calls = %d, want 1", launchCalls)
	}
	for _, forbidden := range []string{"group", "title", "ensure_cmd", "launch_profile", "full_command_line", "thurbox_agent_key", "transition_notify", "assert_done"} {
		if _, ok := output[forbidden]; ok {
			t.Fatalf("generic Agent Deck create leaked %q: %v", forbidden, output)
		}
	}
}

func TestGenericAgentDeckCreateNotificationSwitchesOptIn(t *testing.T) {
	workdir := t.TempDir()
	canonicalWorkdir := canonicalTestWorkdir(t, workdir)
	parent := `{"id":"agent-parent","title":"planner","status":"waiting","group":"waypost","path":` + jsonString(t, canonicalWorkdir) + `}`
	refreshed := `{"id":"agent-child","title":"architect-reviewer","status":"waiting","group":"waypost","path":` + jsonString(t, canonicalWorkdir) + `,"parent_session_id":"agent-parent"}`
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-parent", "--json"}):
			return RunResult{ExitCode: 0, Stdout: parent}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "architect-reviewer", "--json"}):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "launch", "--json", "--title", "architect-reviewer", "--cmd", "claude", "--group", "waypost", "--parent", "agent-parent", canonicalWorkdir}):
			return RunResult{ExitCode: 0, Stdout: `{"id":"agent-child"}`}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-child", "--json"}):
			return RunResult{ExitCode: 0, Stdout: refreshed}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	output := callServiceTool(t, service, "session_create", map[string]any{
		"host":              "agent-deck",
		"session_name":      "architect-reviewer",
		"workdir":           workdir,
		"parent_session_id": "agent-parent",
		"full_command_line": "claude",
		"transition_notify": true,
		"assert_done":       true,
	})
	if output["status"] != "created" || output["session_id"] != "agent-child" {
		t.Fatalf("generic Agent Deck opt-in create output = %v", output)
	}
}

func TestAgentDeckNotificationArgs(t *testing.T) {
	for _, test := range []struct {
		name             string
		transitionNotify bool
		assertDone       bool
		want             []string
	}{
		{name: "suppression is the default", transitionNotify: false, assertDone: false, want: []string{"--no-transition-notify", "--no-assert-done"}},
		{name: "transition notify opt-in only", transitionNotify: true, assertDone: false, want: []string{"--no-assert-done"}},
		{name: "assert done opt-in only", transitionNotify: false, assertDone: true, want: []string{"--no-transition-notify"}},
		{name: "both opt-in", transitionNotify: true, assertDone: true, want: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := agentDeckNotificationArgs(test.transitionNotify, test.assertDone)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("agentDeckNotificationArgs(%v, %v) = %v, want %v", test.transitionNotify, test.assertDone, got, test.want)
			}
		})
	}
}

func TestGenericAgentDeckCreateUsesCapturedParentGroupSnapshot(t *testing.T) {
	for _, test := range []struct {
		name        string
		parentGroup string
		wantGroup   string
	}{
		{
			name:        "trims a nested parent group",
			parentGroup: "  planning/active  ",
			wantGroup:   "planning/active",
		},
		{
			name:        "uses the preflight snapshot rather than a later parent state",
			parentGroup: "old/group",
			wantGroup:   "old/group",
		},
		{
			name:        "passes a missing registry path without a probe",
			parentGroup: "recreated/path",
			wantGroup:   "recreated/path",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			workdir := t.TempDir()
			canonicalWorkdir := canonicalTestWorkdir(t, workdir)
			parent := `{"id":"agent-parent","title":"planner","status":"waiting","group":` + jsonString(t, test.parentGroup) + `,"path":` + jsonString(t, canonicalWorkdir) + `}`
			refreshed := `{"id":"agent-child","title":"architect-reviewer","status":"waiting","group":` + jsonString(t, test.wantGroup) + `,"path":` + jsonString(t, canonicalWorkdir) + `,"parent_session_id":"agent-parent"}`
			launchCalls := 0
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch {
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-parent", "--json"}):
					return RunResult{ExitCode: 0, Stdout: parent}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "architect-reviewer", "--json"}):
					return RunResult{ExitCode: 2, Stderr: "not found"}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "launch", "--json", "--title", "architect-reviewer", "--cmd", "codex", "--group", test.wantGroup, "--parent", "agent-parent", "--no-transition-notify", "--no-assert-done", canonicalWorkdir}):
					// Agent Deck v1.10.11 launch_cmd.go creates a supplied non-empty
					// group path. Generic create must not probe or create it first.
					launchCalls++
					return RunResult{ExitCode: 0, Stdout: `{"id":"agent-child"}`}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-child", "--json"}):
					return RunResult{ExitCode: 0, Stdout: refreshed}, nil
				default:
					t.Fatalf("unexpected command args: %v", args)
					return RunResult{}, nil
				}
			}}
			service := newService(Options{
				WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
				CommandRunner:         commandRunner,
				DisableWakeScheduler:  true,
				DisableLeaseRenewLoop: true,
			})

			output := callServiceTool(t, service, "session_create", map[string]any{
				"host":              "agent-deck",
				"session_name":      "architect-reviewer",
				"workdir":           workdir,
				"parent_session_id": "agent-parent",
				"full_command_line": "codex",
			})
			if output["status"] != "created" || output["session_id"] != "agent-child" || output["path"] != canonicalWorkdir {
				t.Fatalf("generic Agent Deck snapshot create output = %v", output)
			}
			if _, ok := output["group"]; ok {
				t.Fatalf("generic Agent Deck snapshot create leaked group: %v", output)
			}
			if launchCalls != 1 {
				t.Fatalf("agent-deck launch calls = %d, want 1", launchCalls)
			}
		})
	}
}

func TestGenericAgentDeckCreateRejectsEmptyParentGroupBeforeTargetLookup(t *testing.T) {
	for _, test := range []struct {
		name      string
		group     string
		omitGroup bool
	}{
		{name: "empty group"},
		{name: "whitespace group", group: "   "},
		{name: "omitted group", omitGroup: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			workdir := t.TempDir()
			canonicalWorkdir := canonicalTestWorkdir(t, workdir)
			groupField := `,"group":` + jsonString(t, test.group)
			if test.omitGroup {
				groupField = ""
			}
			parent := `{"id":"agent-parent","title":"planner","status":"waiting"` + groupField + `,"path":` + jsonString(t, canonicalWorkdir) + `}`
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				if reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-parent", "--json"}) {
					return RunResult{ExitCode: 0, Stdout: parent}, nil
				}
				t.Fatalf("empty parent group must fail before target lookup or launch: %v", args)
				return RunResult{}, nil
			}}
			service := newService(Options{
				WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
				CommandRunner:         commandRunner,
				DisableWakeScheduler:  true,
				DisableLeaseRenewLoop: true,
			})

			err := callServiceToolExpectError(t, service, "session_create", map[string]any{
				"host":              "agent-deck",
				"session_name":      "architect-reviewer",
				"workdir":           workdir,
				"parent_session_id": "agent-parent",
				"full_command_line": "codex",
			})
			if err == nil || !strings.Contains(err.Error(), genericAgentDeckEmptyParentGroupDetail) {
				t.Fatalf("empty parent group error = %v, want %q", err, genericAgentDeckEmptyParentGroupDetail)
			}
			if calls := commandRunner.Calls(); len(calls) != 1 {
				t.Fatalf("empty parent group calls = %v, want only parent lookup", calls)
			}
		})
	}
}

func TestGenericAgentDeckCreateRejectsNestedParentBeforeTargetLookup(t *testing.T) {
	workdir := t.TempDir()
	canonicalWorkdir := canonicalTestWorkdir(t, workdir)
	parent := `{"id":"agent-parent","title":"planner","status":"waiting","group":"waypost","path":` + jsonString(t, canonicalWorkdir) + `,"parent_session_id":"grandparent"}`
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		if reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-parent", "--json"}) {
			return RunResult{ExitCode: 0, Stdout: parent}, nil
		}
		t.Fatalf("nested parent must fail before target lookup or launch: %v", args)
		return RunResult{}, nil
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	err := callServiceToolExpectError(t, service, "session_create", map[string]any{
		"host":              "agent-deck",
		"session_name":      "architect-reviewer",
		"workdir":           workdir,
		"parent_session_id": "agent-parent",
		"full_command_line": "codex",
	})
	if err == nil || !strings.Contains(err.Error(), genericAgentDeckNestedParentDetail) {
		t.Fatalf("nested parent error = %v, want %q", err, genericAgentDeckNestedParentDetail)
	}
	if calls := commandRunner.Calls(); len(calls) != 1 {
		t.Fatalf("nested parent calls = %v, want only parent lookup", calls)
	}
}

func TestGenericAgentDeckCreateReturnsRecoveryForRefreshedGroupMismatch(t *testing.T) {
	for _, test := range []struct {
		name           string
		refreshedGroup string
	}{
		{name: "empty refreshed group"},
		{name: "different refreshed group", refreshedGroup: "other/group"},
	} {
		t.Run(test.name, func(t *testing.T) {
			workdir := t.TempDir()
			canonicalWorkdir := canonicalTestWorkdir(t, workdir)
			parent := `{"id":"agent-parent","title":"planner","status":"waiting","group":"waypost","path":` + jsonString(t, canonicalWorkdir) + `}`
			refreshed := `{"id":"agent-child","title":"architect-reviewer","status":"waiting","group":` + jsonString(t, test.refreshedGroup) + `,"path":` + jsonString(t, canonicalWorkdir) + `,"parent_session_id":"agent-parent"}`
			launchCalls := 0
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch {
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-parent", "--json"}):
					return RunResult{ExitCode: 0, Stdout: parent}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "architect-reviewer", "--json"}):
					return RunResult{ExitCode: 2, Stderr: "not found"}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "launch", "--json", "--title", "architect-reviewer", "--cmd", "codex", "--group", "waypost", "--parent", "agent-parent", "--no-transition-notify", "--no-assert-done", canonicalWorkdir}):
					launchCalls++
					return RunResult{ExitCode: 0, Stdout: `{"id":"agent-child"}`}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-child", "--json"}):
					return RunResult{ExitCode: 0, Stdout: refreshed}, nil
				default:
					t.Fatalf("group mismatch must not trigger a corrective command: %v", args)
					return RunResult{}, nil
				}
			}}
			service := newService(Options{
				WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
				CommandRunner:         commandRunner,
				DisableWakeScheduler:  true,
				DisableLeaseRenewLoop: true,
			})

			output := callServiceTool(t, service, "session_create", map[string]any{
				"host":              "agent-deck",
				"session_name":      "architect-reviewer",
				"workdir":           workdir,
				"parent_session_id": "agent-parent",
				"full_command_line": "codex",
			})
			if output["status"] != "created_unverified" || output["created_target"] != true || output["recovery_required"] != true || output["session_id"] != "agent-child" || output["parent_session_id"] != "agent-parent" || output["path"] != canonicalWorkdir {
				t.Fatalf("group mismatch recovery output = %v", output)
			}
			verification, ok := output["verification"].(map[string]any)
			if !ok || verification["state"] != "post_create_group_mismatch" || verification["error"] != genericAgentDeckGroupMismatchDetail {
				t.Fatalf("group mismatch verification = %v", output["verification"])
			}
			if _, ok := output["group"]; ok {
				t.Fatalf("group mismatch recovery leaked group: %v", output)
			}
			if launchCalls != 1 {
				t.Fatalf("agent-deck launch calls = %d, want 1", launchCalls)
			}
		})
	}
}

func TestGenericSessionCreateRedactsCallerLaunchValueFromCommandErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		host sessionHost
		mode string
	}{
		{name: "agent deck runner error", host: sessionHostAgentDeck, mode: "runner"},
		{name: "agent deck nonzero exit", host: sessionHostAgentDeck, mode: "nonzero"},
		{name: "thurbox runner error", host: sessionHostThurbox, mode: "runner"},
		{name: "thurbox nonzero exit", host: sessionHostThurbox, mode: "nonzero"},
	} {
		t.Run(test.name, func(t *testing.T) {
			workdir := t.TempDir()
			canonicalWorkdir := canonicalTestWorkdir(t, workdir)
			const launchValue = "operator-launch-secret"
			parentID := "agent-parent"
			if test.host == sessionHostThurbox {
				parentID = thurboxPlannerID
			}

			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch test.host {
				case sessionHostAgentDeck:
					switch {
					case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", parentID, "--json"}):
						return RunResult{ExitCode: 0, Stdout: `{"id":"agent-parent","title":"planner","status":"waiting","group":"parent-group-secret","path":` + jsonString(t, canonicalWorkdir) + `}`}, nil
					case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "architect-reviewer", "--json"}):
						return RunResult{ExitCode: 2, Stderr: "not found"}, nil
					case reflect.DeepEqual(args, []string{"agent-deck", "launch", "--json", "--title", "architect-reviewer", "--cmd", launchValue, "--group", "parent-group-secret", "--parent", parentID, "--no-transition-notify", "--no-assert-done", canonicalWorkdir}):
						if test.mode == "runner" {
							return RunResult{}, errors.New("runner echoed " + launchValue)
						}
						return RunResult{ExitCode: 23, Stdout: "stdout " + launchValue, Stderr: "stderr " + launchValue}, nil
					}
				case sessionHostThurbox:
					parent := thurboxSessionRecord(t, thurboxPlannerID, "planner", canonicalWorkdir, "", "idle")
					switch {
					case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", parentID}):
						return RunResult{ExitCode: 0, Stdout: parent}, nil
					case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "list", "--json"}):
						return RunResult{ExitCode: 0, Stdout: "[]"}, nil
					case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "create", "--json", "--name", "architect-reviewer", "--repo-path", canonicalWorkdir, "--agent", launchValue, "--parent", parentID}):
						if test.mode == "runner" {
							return RunResult{}, errors.New("runner echoed " + launchValue)
						}
						return RunResult{ExitCode: 23, Stdout: "stdout " + launchValue, Stderr: "stderr " + launchValue}, nil
					}
				}
				t.Fatalf("unexpected command args: %v", args)
				return RunResult{}, nil
			}}

			service := newService(Options{
				WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
				CommandRunner:         commandRunner,
				DisableWakeScheduler:  true,
				DisableLeaseRenewLoop: true,
			})
			err := callServiceToolExpectError(t, service, "session_create", map[string]any{
				"host":              string(test.host),
				"session_name":      "architect-reviewer",
				"workdir":           workdir,
				"parent_session_id": parentID,
				"full_command_line": func() string {
					if test.host == sessionHostAgentDeck {
						return launchValue
					}
					return "ignored-agent-deck-value"
				}(),
				"thurbox_agent_key": func() string {
					if test.host == sessionHostThurbox {
						return launchValue
					}
					return "ignored-thurbox-key"
				}(),
			})
			if err == nil || strings.Contains(err.Error(), launchValue) {
				t.Fatalf("generic create error = %v, must redact %q", err, launchValue)
			}
			if test.host == sessionHostAgentDeck && strings.Contains(err.Error(), "parent-group-secret") {
				t.Fatalf("generic Agent Deck create error = %v, must redact parent group", err)
			}
		})
	}
}

func TestThurboxResolveOnlyTreatsPinnedMissingGetAsNotFound(t *testing.T) {
	t.Run("exact missing result", func(t *testing.T) {
		manager := newSessionManager(&fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			if !reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", thurboxAuthorID}) {
				t.Fatalf("unexpected command args: %v", args)
			}
			return RunResult{ExitCode: 1, Stderr: "error: Session not found: " + thurboxAuthorID}, nil
		}}, &serverState{})
		data, err := manager.resolveThurboxSession(context.Background(), thurboxAuthorID, syncCmdTimeout)
		if err != nil || data != nil {
			t.Fatalf("pinned missing get = data=%+v err=%v, want not found", data, err)
		}
	})

	t.Run("other get failure is operational", func(t *testing.T) {
		manager := newSessionManager(&fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			return RunResult{ExitCode: 1, Stderr: "error: database unavailable"}, nil
		}}, &serverState{})
		data, err := manager.resolveThurboxSession(context.Background(), thurboxAuthorID, syncCmdTimeout)
		if data != nil || err == nil || !strings.Contains(err.Error(), "session get failed with exit code 1") {
			t.Fatalf("operational get failure = data=%+v err=%v", data, err)
		}
	})

	t.Run("returned UUID must match requested UUID", func(t *testing.T) {
		manager := newSessionManager(&fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			return RunResult{ExitCode: 0, Stdout: thurboxSessionRecord(t, thurboxReviewID, "architect-reviewer", t.TempDir(), thurboxPlannerID, "idle")}, nil
		}}, &serverState{})
		data, err := manager.resolveThurboxSession(context.Background(), thurboxAuthorID, syncCmdTimeout)
		if data != nil || err == nil || !isHostSessionIdentityFailure(err) {
			t.Fatalf("mismatched get UUID = data=%+v err=%v", data, err)
		}
	})
}

func TestGenericThurboxCreateFailsClosedOnPreflightListFailure(t *testing.T) {
	workdir := t.TempDir()
	canonicalWorkdir := canonicalTestWorkdir(t, workdir)
	parent := thurboxSessionRecord(t, thurboxPlannerID, "planner", canonicalWorkdir, "", "idle")
	createCalled := false
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", thurboxPlannerID}):
			return RunResult{ExitCode: 0, Stdout: parent}, nil
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "list", "--json"}):
			return RunResult{ExitCode: 2, Stderr: "database unavailable"}, nil
		case len(args) >= 3 && args[0] == "thurbox-cli" && args[1] == "session" && args[2] == "create":
			createCalled = true
			t.Fatalf("create ran after a failed preflight: %v", args)
		}
		t.Fatalf("unexpected command args: %v", args)
		return RunResult{}, nil
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	err := callServiceToolExpectError(t, service, "session_create", map[string]any{
		"host":              "thurbox",
		"session_name":      "architect-reviewer",
		"workdir":           workdir,
		"parent_session_id": thurboxPlannerID,
		"thurbox_agent_key": "codex",
	})
	if err == nil || !strings.Contains(err.Error(), "session list failed with exit code 2") || createCalled {
		t.Fatalf("preflight list failure = err=%v createCalled=%v", err, createCalled)
	}
}

func TestGenericSessionCreateRecoversFromConfirmedMalformedOutput(t *testing.T) {
	workdir := t.TempDir()
	canonicalWorkdir := canonicalTestWorkdir(t, workdir)
	parent := thurboxSessionRecord(t, thurboxPlannerID, "planner", canonicalWorkdir, "", "idle")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", thurboxPlannerID}):
			return RunResult{ExitCode: 0, Stdout: parent}, nil
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "list", "--json"}):
			return RunResult{ExitCode: 0, Stdout: "[]"}, nil
		case strings.Join(args, "\x00") == strings.Join([]string{"thurbox-cli", "session", "create", "--json", "--name", "architect-reviewer", "--repo-path", canonicalWorkdir, "--agent", "codex", "--parent", thurboxPlannerID}, "\x00"):
			return RunResult{ExitCode: 0, Stdout: "not JSON"}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	output := callServiceTool(t, service, "session_create", map[string]any{
		"host":              "thurbox",
		"session_name":      "architect-reviewer",
		"workdir":           workdir,
		"parent_session_id": thurboxPlannerID,
		"thurbox_agent_key": "codex",
	})
	if output["status"] != "create_recovery_required" || output["created_target"] != nil || output["started_session"] != nil || output["recovery_required"] != true || output["session_id"] != nil {
		t.Fatalf("create recovery output = %v", output)
	}
	verification := output["verification"].(map[string]any)
	if verification["state"] != "create_output_unparseable" || verification["requested_workdir"] != canonicalWorkdir || verification["error"] != genericSessionCreateOutputUnparseableDetail {
		t.Fatalf("create recovery verification = %v", verification)
	}
}

func TestGenericAgentDeckCreateRecoveryUsesFixedRedactedDetail(t *testing.T) {
	workdir := t.TempDir()
	canonicalWorkdir := canonicalTestWorkdir(t, workdir)
	parent := `{"id":"agent-parent","title":"planner","status":"waiting","group":"waypost","path":` + jsonString(t, canonicalWorkdir) + `}`
	for _, createOutput := range []string{
		`malformed selected-secret ignored-secret`,
		`{"title":"selected-secret","path":"ignored-secret"}`,
	} {
		t.Run(createOutput, func(t *testing.T) {
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch {
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "agent-parent", "--json"}):
					return RunResult{ExitCode: 0, Stdout: parent}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "architect-reviewer", "--json"}):
					return RunResult{ExitCode: 2, Stderr: "not found"}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "launch", "--json", "--title", "architect-reviewer", "--cmd", "selected-secret", "--group", "waypost", "--parent", "agent-parent", "--no-transition-notify", "--no-assert-done", canonicalWorkdir}):
					return RunResult{ExitCode: 0, Stdout: createOutput}, nil
				default:
					t.Fatalf("unexpected command args: %v", args)
					return RunResult{}, nil
				}
			}}
			service := newService(Options{
				WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
				CommandRunner:         commandRunner,
				DisableWakeScheduler:  true,
				DisableLeaseRenewLoop: true,
			})
			output := callServiceTool(t, service, "session_create", map[string]any{
				"host":              "agent-deck",
				"session_name":      "architect-reviewer",
				"workdir":           workdir,
				"parent_session_id": "agent-parent",
				"full_command_line": " selected-secret ",
				"thurbox_agent_key": "ignored-secret",
			})
			if output["status"] != "create_recovery_required" {
				t.Fatalf("create recovery output = %v", output)
			}
			verification := output["verification"].(map[string]any)
			if verification["state"] != "create_output_unparseable" || verification["error"] != genericSessionCreateOutputUnparseableDetail {
				t.Fatalf("create recovery verification = %v", verification)
			}
			encoded, _ := json.Marshal(output)
			if strings.Contains(string(encoded), "selected-secret") || strings.Contains(string(encoded), "ignored-secret") {
				t.Fatalf("create recovery leaked launch data: %s", encoded)
			}
		})
	}
}

func TestGenericSessionCreateReturnsRecoveryRecordForPostCreatePathMismatch(t *testing.T) {
	workdir := t.TempDir()
	canonicalWorkdir := canonicalTestWorkdir(t, workdir)
	otherWorkdir := t.TempDir()
	parent := thurboxSessionRecord(t, thurboxPlannerID, "planner", canonicalWorkdir, "", "idle")
	created := thurboxCreatedSessionRecord(t, thurboxReviewID, "architect-reviewer", canonicalWorkdir, thurboxPlannerID)
	refreshed := thurboxSessionRecord(t, thurboxReviewID, "architect-reviewer", otherWorkdir, thurboxPlannerID, "idle")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", thurboxPlannerID}):
			return RunResult{ExitCode: 0, Stdout: parent}, nil
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "list", "--json"}):
			return RunResult{ExitCode: 0, Stdout: "[]"}, nil
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "create", "--json", "--name", "architect-reviewer", "--repo-path", canonicalWorkdir, "--agent", "codex", "--parent", thurboxPlannerID}):
			return RunResult{ExitCode: 0, Stdout: created}, nil
		case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", thurboxReviewID}):
			return RunResult{ExitCode: 0, Stdout: refreshed}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	output := callServiceTool(t, service, "session_create", map[string]any{
		"host":              "thurbox",
		"session_name":      "architect-reviewer",
		"workdir":           workdir,
		"parent_session_id": thurboxPlannerID,
		"thurbox_agent_key": "codex",
	})
	if output["status"] != "created_unverified" || output["created_target"] != true || output["started_session"] != true || output["recovery_required"] != true || output["session_id"] != thurboxReviewID {
		t.Fatalf("post-create recovery output = %v", output)
	}
	verification := output["verification"].(map[string]any)
	if verification["state"] != "path_mismatch" || verification["observed_path"] != otherWorkdir || verification["requested_workdir"] != canonicalWorkdir {
		t.Fatalf("post-create recovery verification = %v", verification)
	}
}

func TestGenericSessionCreateReturnsRecoveryForIdentityMismatch(t *testing.T) {
	for _, test := range []struct {
		name            string
		createdID       string
		createdName     string
		createdParent   string
		refreshedID     string
		refreshedName   string
		refreshedParent string
	}{
		{
			name:            "refreshed ID differs",
			createdID:       thurboxReviewID,
			createdName:     "architect-reviewer",
			createdParent:   thurboxPlannerID,
			refreshedID:     thurboxAuthorID,
			refreshedName:   "architect-reviewer",
			refreshedParent: thurboxPlannerID,
		},
		{
			name:            "created name differs",
			createdID:       thurboxReviewID,
			createdName:     "wrong-name",
			createdParent:   thurboxPlannerID,
			refreshedID:     thurboxReviewID,
			refreshedName:   "architect-reviewer",
			refreshedParent: thurboxPlannerID,
		},
		{
			name:            "refreshed parent differs",
			createdID:       thurboxReviewID,
			createdName:     "architect-reviewer",
			createdParent:   thurboxPlannerID,
			refreshedID:     thurboxReviewID,
			refreshedName:   "architect-reviewer",
			refreshedParent: thurboxAuthorID,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			workdir := t.TempDir()
			canonicalWorkdir := canonicalTestWorkdir(t, workdir)
			parent := thurboxSessionRecord(t, thurboxPlannerID, "planner", canonicalWorkdir, "", "idle")
			created := thurboxCreatedSessionRecord(t, test.createdID, test.createdName, canonicalWorkdir, test.createdParent)
			refreshed := thurboxSessionRecord(t, test.refreshedID, test.refreshedName, canonicalWorkdir, test.refreshedParent, "idle")
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch {
				case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", thurboxPlannerID}):
					return RunResult{ExitCode: 0, Stdout: parent}, nil
				case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "list", "--json"}):
					return RunResult{ExitCode: 0, Stdout: "[]"}, nil
				case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "create", "--json", "--name", "architect-reviewer", "--repo-path", canonicalWorkdir, "--agent", "codex", "--parent", thurboxPlannerID}):
					return RunResult{ExitCode: 0, Stdout: created}, nil
				case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", test.createdID}):
					return RunResult{ExitCode: 0, Stdout: refreshed}, nil
				default:
					t.Fatalf("unexpected command args: %v", args)
					return RunResult{}, nil
				}
			}}
			service := newService(Options{
				WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
				CommandRunner:         commandRunner,
				DisableWakeScheduler:  true,
				DisableLeaseRenewLoop: true,
			})
			output := callServiceTool(t, service, "session_create", map[string]any{
				"host":              "thurbox",
				"session_name":      "architect-reviewer",
				"workdir":           workdir,
				"parent_session_id": thurboxPlannerID,
				"thurbox_agent_key": "codex",
			})
			if output["status"] != "created_unverified" || output["created_target"] != true || output["recovery_required"] != true {
				t.Fatalf("identity recovery output = %v", output)
			}
			if verification := output["verification"].(map[string]any); verification["state"] != "post_create_identity_mismatch" {
				t.Fatalf("identity recovery verification = %v", verification)
			}
		})
	}
}

func TestGenericSessionCreateFailsBeforeHostCommandsWithoutSelectedLaunchValue(t *testing.T) {
	workdir := t.TempDir()
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
			t.Fatalf("selected launch-value validation must run before host commands: %v", args)
			return RunResult{}, nil
		}},
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	err := callServiceToolExpectError(t, service, "session_create", map[string]any{
		"host":              "thurbox",
		"session_name":      "architect-reviewer",
		"workdir":           workdir,
		"parent_session_id": thurboxPlannerID,
		"full_command_line": "   ",
	})
	if err == nil || !strings.Contains(err.Error(), "thurbox_agent_key is required") {
		t.Fatalf("missing thurbox key error = %v", err)
	}
}

func TestGenericSessionRequireInspectionResults(t *testing.T) {
	t.Run("missing target", func(t *testing.T) {
		workdir := t.TempDir()
		service := newService(Options{
			WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
			CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				if !reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "missing", "--json"}) {
					t.Fatalf("unexpected command args: %v", args)
				}
				return RunResult{ExitCode: 2, Stderr: "not found"}, nil
			}},
			DisableWakeScheduler:  true,
			DisableLeaseRenewLoop: true,
		})

		output := callServiceTool(t, service, "session_require", map[string]any{
			"host":         "agent-deck",
			"session_ref":  "missing",
			"workdir":      workdir,
			"auto_restart": false,
		})
		if output["status"] != "not_found" || output["session_ref"] != "missing" || output["started_session"] != false {
			t.Fatalf("missing inspection output = %v", output)
		}
	})

	t.Run("stopped target", func(t *testing.T) {
		workdir := t.TempDir()
		service := newService(Options{
			WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
			CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				if !reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "stopped", "--json"}) {
					t.Fatalf("auto_restart=false must not start the target: %v", args)
				}
				return RunResult{ExitCode: 0, Stdout: `{"id":"stopped-1","title":"stopped","status":"stopped","path":` + jsonString(t, workdir) + `}`}, nil
			}},
			DisableWakeScheduler:  true,
			DisableLeaseRenewLoop: true,
		})

		output := callServiceTool(t, service, "session_require", map[string]any{
			"host":         "agent-deck",
			"session_ref":  "stopped",
			"workdir":      workdir,
			"auto_restart": false,
		})
		if output["status"] != "not_ready" || output["session_status"] != "stopped" || output["started_session"] != false {
			t.Fatalf("stopped inspection output = %v", output)
		}
	})

	t.Run("operational lookup failure", func(t *testing.T) {
		workdir := t.TempDir()
		service := newService(Options{
			WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
			CommandRunner: &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				if !reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "worker", "--json"}) {
					t.Fatalf("unexpected command args: %v", args)
				}
				return RunResult{ExitCode: 1, Stderr: "database unavailable"}, nil
			}},
			DisableWakeScheduler:  true,
			DisableLeaseRenewLoop: true,
		})

		err := callServiceToolExpectError(t, service, "session_require", map[string]any{
			"host":         "agent-deck",
			"session_ref":  "worker",
			"workdir":      workdir,
			"auto_restart": false,
		})
		if err == nil || !strings.Contains(err.Error(), "agent-deck session show failed with exit code 1") {
			t.Fatalf("operational lookup error = %v", err)
		}
	})
}

func TestGenericAgentDeckCreateDoesNotTreatLookupFailureAsMissing(t *testing.T) {
	workdir := t.TempDir()
	parentWorkdir := t.TempDir()
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "planner-1", "--json"}):
			return RunResult{ExitCode: 0, Stdout: `{"id":"planner-1","title":"planner","status":"waiting","group":"planning","path":` + jsonString(t, parentWorkdir) + `}`}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "worker", "--json"}):
			return RunResult{ExitCode: 1, Stderr: "database unavailable"}, nil
		default:
			t.Fatalf("lookup failure must stop before launch: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	err := callServiceToolExpectError(t, service, "session_create", map[string]any{
		"host":              "agent-deck",
		"session_name":      "worker",
		"workdir":           workdir,
		"parent_session_id": "planner-1",
		"full_command_line": "codex",
	})
	if err == nil || !strings.Contains(err.Error(), "agent-deck session show failed with exit code 1") {
		t.Fatalf("create lookup error = %v", err)
	}
}

func TestGenericSessionRequireBatchKeepsConfirmedStartRecoveryAndContinues(t *testing.T) {
	workdir := t.TempDir()
	stopped := `{"id":"stopped-1","title":"stopped","status":"stopped","path":` + jsonString(t, workdir) + `}`
	active := `{"id":"active-1","title":"active","status":"waiting","path":` + jsonString(t, workdir) + `}`
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		switch {
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "missing", "--json"}):
			return RunResult{ExitCode: 2, Stderr: "not found"}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "stopped", "--json"}):
			return RunResult{ExitCode: 0, Stdout: stopped}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "start", "--json", "stopped-1"}):
			return RunResult{ExitCode: 0}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "stopped-1", "--json"}):
			return RunResult{ExitCode: 0, Stdout: "not JSON"}, nil
		case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "active", "--json"}):
			return RunResult{ExitCode: 0, Stdout: active}, nil
		default:
			t.Fatalf("unexpected command args: %v", args)
			return RunResult{}, nil
		}
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})

	output := callServiceTool(t, service, "session_require", map[string]any{
		"host":     "agent-deck",
		"sessions": []string{"missing", "stopped", "active"},
		"workdir":  workdir,
	})
	results, ok := output["results"].([]any)
	if !ok || len(results) != 3 {
		t.Fatalf("batch require results = %v", output["results"])
	}
	missing := results[0].(map[string]any)
	if missing["status"] != "not_found" || missing["started_session"] != false {
		t.Fatalf("missing batch result = %v", missing)
	}
	recovery := results[1].(map[string]any)
	if recovery["status"] != "ready_unverified" || recovery["started_session"] != true || recovery["recovery_required"] != true || recovery["session_id"] != "stopped-1" {
		t.Fatalf("post-start recovery result = %v", recovery)
	}
	if verification := recovery["verification"].(map[string]any); verification["state"] != "post_start_output_unparseable" {
		t.Fatalf("post-start recovery verification = %v", verification)
	}
	ready := results[2].(map[string]any)
	if ready["status"] != "ready" || ready["started_session"] != false || ready["recovery_required"] != false {
		t.Fatalf("continued batch result = %v", ready)
	}
}

func TestGenericSessionRequireSessionIDMustMatchResolvedID(t *testing.T) {
	for _, test := range []struct {
		name string
		host sessionHost
	}{
		{name: "agent deck name is not an ID", host: sessionHostAgentDeck},
		{name: "thurbox name is not an ID", host: sessionHostThurbox},
	} {
		t.Run(test.name, func(t *testing.T) {
			workdir := t.TempDir()
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch test.host {
				case sessionHostAgentDeck:
					if reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "architect-author", "--json"}) {
						return RunResult{ExitCode: 0, Stdout: `{"id":"agent-session-id","title":"architect-author","status":"stopped","path":` + jsonString(t, workdir) + `}`}, nil
					}
				case sessionHostThurbox:
					if reflect.DeepEqual(args, []string{"thurbox-cli", "session", "list", "--json"}) {
						return RunResult{ExitCode: 0, Stdout: "[" + thurboxSessionRecord(t, thurboxAuthorID, "architect-author", workdir, thurboxPlannerID, "idle") + "]"}, nil
					}
				}
				t.Fatalf("session_id must reject before any start/restart: %v", args)
				return RunResult{}, nil
			}}
			service := newService(Options{
				WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
				CommandRunner:         commandRunner,
				DisableWakeScheduler:  true,
				DisableLeaseRenewLoop: true,
			})
			err := callServiceToolExpectError(t, service, "session_require", map[string]any{
				"host":       string(test.host),
				"session_id": "architect-author",
				"workdir":    workdir,
			})
			if err == nil || !strings.Contains(err.Error(), "session_id must exactly match") {
				t.Fatalf("non-exact session_id error = %v", err)
			}
		})
	}
}

func TestReverifyStartedHostSessionUsesStructuredRecoveryStates(t *testing.T) {
	workdir := t.TempDir()
	canonicalWorkdir := canonicalTestWorkdir(t, workdir)
	otherWorkdir := t.TempDir()
	previous := &hostSessionData{
		Host:            sessionHostThurbox,
		ID:              thurboxAuthorID,
		Name:            "architect-author",
		Status:          "idle",
		Path:            canonicalWorkdir,
		ParentSessionID: thurboxPlannerID,
	}
	for _, test := range []struct {
		name      string
		output    string
		wantState string
	}{
		{
			name: "missing pinned field",
			output: mutateThurboxObjectFixture(t, "session-get.json", func(payload map[string]any) {
				delete(payload, "cwd")
			}),
			wantState: "post_start_output_unparseable",
		},
		{
			name: "unknown pinned field",
			output: mutateThurboxObjectFixture(t, "session-get.json", func(payload map[string]any) {
				payload["drift"] = true
			}),
			wantState: "post_start_output_unparseable",
		},
		{
			name:      "path mismatch",
			output:    thurboxSessionRecord(t, thurboxAuthorID, "architect-author", otherWorkdir, thurboxPlannerID, "idle"),
			wantState: "post_start_path_mismatch",
		},
		{
			name:      "path unavailable",
			output:    thurboxSessionRecord(t, thurboxAuthorID, "architect-author", "", thurboxPlannerID, "idle"),
			wantState: "post_start_path_unavailable",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := newSessionManager(&fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				if !reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", thurboxAuthorID}) {
					t.Fatalf("unexpected command args: %v", args)
				}
				return RunResult{ExitCode: 0, Stdout: test.output}, nil
			}}, &serverState{})
			output, err := manager.reverifyStartedHostSession(context.Background(), sessionHostThurbox, previous, previous.ID, workdir, canonicalWorkdir)
			if err != nil || output["status"] != "ready_unverified" || output["started_session"] != true {
				t.Fatalf("post-start recovery = output=%v err=%v", output, err)
			}
			if verification := output["verification"].(map[string]any); verification["state"] != test.wantState {
				t.Fatalf("post-start verification = %v, want %q", verification, test.wantState)
			}
		})
	}
}

func TestGenericThurboxRequireAcceptsPinnedActiveStatusWithoutRestart(t *testing.T) {
	workdir := t.TempDir()
	record := thurboxSessionRecord(t, thurboxAuthorID, "architect-author", workdir, thurboxPlannerID, "idle")
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		if !reflect.DeepEqual(args, []string{"thurbox-cli", "session", "get", "--json", thurboxAuthorID}) {
			t.Fatalf("active Thurbox require must not restart or list: %v", args)
		}
		return RunResult{ExitCode: 0, Stdout: record}, nil
	}}
	service := newService(Options{
		WaypostServiceFactory: failOpenWaypostServiceFactory{t: t},
		CommandRunner:         commandRunner,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	output := callServiceTool(t, service, "session_require", map[string]any{
		"host":       "thurbox",
		"session_id": thurboxAuthorID,
		"workdir":    workdir,
	})
	if output["status"] != "ready" || output["started_session"] != false || output["session_status"] != "idle" {
		t.Fatalf("active Thurbox require output = %v", output)
	}
}

func TestWaypostGroupSubscribersReportHostNeutralNotificationScheme(t *testing.T) {
	for _, test := range []struct {
		name       string
		addresses  []string
		wantScheme string
	}{
		{
			name:       "only Thurbox subscriber",
			addresses:  []string{"thurbox/" + thurboxAuthorID},
			wantScheme: "thurbox",
		},
		{
			name:       "mixed subscribers",
			addresses:  []string{"agent-deck/moderator", "thurbox/" + thurboxAuthorID},
			wantScheme: "mixed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			waypostService := &fakeWaypostService{t: t}
			waypostService.sendFunc = func(_ context.Context, params waypost.SendParams) (waypost.SendResult, error) {
				return waypost.SendResult{
					Mode:                       waypost.SendModeGroup,
					MessageID:                  "msg_group",
					GroupID:                    "grp_1",
					GroupAddress:               "group/review",
					GroupNotificationAddresses: test.addresses,
					MessageCreatedAt:           "2026-08-08T00:00:00Z",
				}, nil
			}
			commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
				switch {
				case reflect.DeepEqual(args, []string{"thurbox-cli", "session", "send", thurboxAuthorID, defaultNotifyMessage}):
					return RunResult{ExitCode: 0}, nil
				case reflect.DeepEqual(args, []string{"agent-deck", "session", "show", "moderator", "--json"}):
					return RunResult{ExitCode: 0, Stdout: `{"id":"moderator","title":"moderator","status":"waiting"}`}, nil
				case reflect.DeepEqual(args, agentDeckDeferredSendArgs("moderator", defaultNotifyMessage)):
					return RunResult{ExitCode: 0}, nil
				default:
					t.Fatalf("unexpected command args: %v", args)
					return RunResult{}, nil
				}
			}}
			service := newService(Options{
				WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
				CommandRunner:         commandRunner,
				DisableWakeScheduler:  true,
				DisableLeaseRenewLoop: true,
			})
			setBoundTestSender(service, "agent-deck/expert")
			output := callServiceTool(t, service, "waypost_send", map[string]any{
				"to":           "group/review",
				"from_address": "agent-deck/expert",
				"subject":      "group update",
				"body":         "body",
				"group":        true,
				"diagnostics":  true,
			})
			if output["notify_status"] != "sent" || output["notify_scheme"] != test.wantScheme {
				t.Fatalf("group notify outcome = %v, want scheme %q", output, test.wantScheme)
			}
		})
	}
}

func TestThurboxWakeFollowsDurableSendAndFailureKeepsDelivery(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "waypost-state")
	durableStored := false
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		if !durableStored {
			t.Fatal("Thurbox wake ran before durable Waypost send")
		}
		want := []string{"thurbox-cli", "session", "send", thurboxAuthorID, defaultNotifyMessage}
		if !reflect.DeepEqual(args, want) {
			t.Fatalf("Thurbox wake args = %v, want %v", args, want)
		}
		return RunResult{}, errors.New("thurbox is unreachable")
	}}
	service := newService(Options{
		StateDir:              stateDir,
		CommandRunner:         commandRunner,
		NotifyDelay:           -1,
		DisableWakeScheduler:  true,
		DisableLeaseRenewLoop: true,
	})
	service.notifications.retryWait = func(context.Context, time.Duration) error { return nil }
	defer service.Close()
	service.state.boundAddresses = []string{"agent-deck/sender"}
	service.state.defaultSender = "agent-deck/sender"
	service.state.autoBindAttempted = true

	// The real durable store is used here. The check is intentionally after the
	// send has returned, just before the wake command can run.
	originalFactory := service.waypostServices
	service.waypostServices = durableFlagFactory{delegate: originalFactory, stored: &durableStored}
	output := callServiceTool(t, service, "waypost_send", map[string]any{
		"to":          "thurbox/" + thurboxAuthorID,
		"subject":     "delegate",
		"body":        "full workflow body must stay in Waypost",
		"diagnostics": true,
	})
	if output["status"] != "sent" || output["delivery_id"] == nil || output["notify_status"] != "failed" || output["notify_scheme"] != "thurbox" || output["notify_error"] == nil {
		t.Fatalf("durable send with failed Thurbox wake = %v", output)
	}

	runtime, err := waypost.OpenRuntime(context.Background(), stateDir)
	if err != nil {
		t.Fatalf("OpenRuntime(): %v", err)
	}
	defer runtime.Close()
	deliveries, err := waypost.NewOperations(runtime.Store()).List(context.Background(), waypost.ListParams{
		Address: "thurbox/" + thurboxAuthorID,
		State:   "queued",
	})
	if err != nil || len(deliveries) != 1 || deliveries[0].DeliveryID != output["delivery_id"] {
		t.Fatalf("queued delivery after failed Thurbox wake = %+v, err=%v", deliveries, err)
	}
}

func TestNestedThurboxWakeTargetsOnlyImmediateSession(t *testing.T) {
	t.Setenv("THURBOX_SESSION", thurboxAuthorID)
	current := time.Date(2026, time.August, 8, 10, 0, 0, 0, time.UTC)
	waypostService := &fakeWaypostService{t: t}
	waypostService.listClaimableFunc = func(context.Context, []string) ([]waypost.ClaimableAddress, error) {
		return []waypost.ClaimableAddress{{
			Address:          "thurbox/" + thurboxAuthorID,
			ClaimableCount:   1,
			OldestEligibleAt: current.Add(-4 * time.Minute).Format(time.RFC3339Nano),
		}}, nil
	}
	commandRunner := &fakeRunner{t: t, handler: func(args []string, input string) (RunResult, error) {
		want := []string{"thurbox-cli", "session", "send", thurboxAuthorID, defaultNotifyMessage}
		if !reflect.DeepEqual(args, want) {
			t.Fatalf("nested scheduler woke wrong host or target: %v", args)
		}
		return RunResult{ExitCode: 0}, nil
	}}
	service := newService(Options{
		WaypostServiceFactory: fakeWaypostServiceFactory{service: waypostService},
		CommandRunner:         commandRunner,
		Now:                   func() time.Time { return current },
		DisableLeaseRenewLoop: true,
	})
	service.state.boundAddresses = []string{"thurbox/" + thurboxAuthorID, "agent-deck/outer-agent-deck"}
	service.state.defaultSender = "thurbox/" + thurboxAuthorID
	service.state.autoBindAttempted = true
	service.state.detectedThurboxSession = thurboxAuthorID
	service.state.detectedAgentDeckSession = "outer-agent-deck"

	scope, err := service.currentLocalWakeScope(context.Background())
	if err != nil {
		t.Fatalf("currentLocalWakeScope(): %v", err)
	}
	if !reflect.DeepEqual(scope.WaypostAddresses, service.state.boundAddresses) || !reflect.DeepEqual(scope.WakeTargets, []WakeTarget{{Channel: WakeChannelThurbox, Target: thurboxAuthorID}}) {
		t.Fatalf("nested wake scope = %+v", scope)
	}
	if err := service.processWakeScheduler(context.Background()); err != nil {
		t.Fatalf("processWakeScheduler(): %v", err)
	}
	if calls := commandRunner.Calls(); len(calls) != 1 {
		t.Fatalf("nested scheduler calls = %v, want one Thurbox wake", calls)
	}
}

func thurboxFixture(t *testing.T, name string) string {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join("testdata", "thurbox-v1.7.1", name))
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", name, err)
	}
	return string(contents)
}

func mutateThurboxObjectFixture(t *testing.T, name string, mutate func(map[string]any)) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(thurboxFixture(t, name)), &payload); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
	mutate(payload)
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode mutated fixture %s: %v", name, err)
	}
	return string(encoded)
}

func thurboxSessionRecord(t *testing.T, id, name, cwd, parent, hookState string) string {
	t.Helper()
	var state any
	if hookState != "" {
		state = hookState
	}
	var parentID any
	if parent != "" {
		parentID = parent
	}
	payload := map[string]any{
		"id":                id,
		"name":              name,
		"agent":             "codex",
		"backend_type":      "local-tmux",
		"agent_session_id":  nil,
		"cwd":               cwd,
		"parent_session_id": parentID,
		"display_order":     nil,
		"worktrees":         []any{},
		"hook_state":        state,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal Thurbox session record: %v", err)
	}
	return string(encoded)
}

func thurboxCreatedSessionRecord(t *testing.T, id, name, cwd, parent string) string {
	t.Helper()
	payload := map[string]any{
		"id":                id,
		"name":              name,
		"agent":             "codex",
		"agent_session_id":  nil,
		"cwd":               cwd,
		"parent_session_id": parent,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal Thurbox create record: %v", err)
	}
	return string(encoded)
}

func jsonString(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON string: %v", err)
	}
	return string(encoded)
}

func clearToolSessionEnvs(t *testing.T) {
	t.Helper()
	for _, name := range []string{"CODEX_THREAD_ID", "CLAUDE_CODE_SESSION_ID", "GEMINI_SESSION_ID", "OPENCODE_SESSION_ID"} {
		t.Setenv(name, "")
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func anyStringContains(values []any, want string) bool {
	for _, value := range values {
		text, ok := value.(string)
		if ok && strings.Contains(text, want) {
			return true
		}
	}
	return false
}

type durableFlagFactory struct {
	delegate waypostServiceFactory
	stored   *bool
}

func (f durableFlagFactory) Open(ctx context.Context) (any, func() error, error) {
	service, closeFunc, err := f.delegate.Open(ctx)
	if err != nil {
		return nil, closeFunc, err
	}
	return durableFlaggingSender{service: service, stored: f.stored}, closeFunc, nil
}

type durableFlaggingSender struct {
	service any
	stored  *bool
}

func (s durableFlaggingSender) Send(ctx context.Context, params waypost.SendParams) (waypost.SendResult, error) {
	sender, ok := s.service.(waypostSender)
	if !ok {
		return waypost.SendResult{}, fmt.Errorf("wrapped waypost service %T does not send", s.service)
	}
	result, err := sender.Send(ctx, params)
	if err == nil {
		*s.stored = true
	}
	return result, err
}

func (s durableFlaggingSender) ReadDeliveries(ctx context.Context, deliveryIDs []string) ([]waypost.ReadDelivery, error) {
	reader, ok := s.service.(waypostDeliveryReader)
	if !ok {
		return nil, fmt.Errorf("wrapped waypost service %T does not read deliveries", s.service)
	}
	return reader.ReadDeliveries(ctx, deliveryIDs)
}
