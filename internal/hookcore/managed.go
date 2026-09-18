package hookcore

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// ManagedEventSpec declares one managed hook group as data: which event it
// belongs to, the desired group envelope, and how an existing group is
// recognized as installer-owned.
type ManagedEventSpec struct {
	// Event is the harness hook event the managed group belongs to.
	Event string
	// DoctorLabel names the hook in doctor errors; empty falls back to Event.
	DoctorLabel string
	// TimeoutSeconds is the desired handler timeout.
	TimeoutSeconds int64
	// Matcher is the group's matcher value; nil leaves the key absent so the
	// group fires for every matchable input.
	Matcher any
	// Description is written on the desired group and used to recognize
	// managed groups for harnesses that label them; empty disables
	// description matching.
	Description string
	// StatusMessages recognize managed handlers written by this or older
	// installers; the first entry is written as the desired handler's
	// statusMessage.
	StatusMessages []string
	// EligibleGroup reports whether an existing group may carry the managed
	// handler.
	EligibleGroup func(group map[string]any) bool
}

// DesiredGroup renders the managed group envelope for command.
func (spec ManagedEventSpec) DesiredGroup(command string) map[string]any {
	handler := map[string]any{
		"type":    "command",
		"command": command,
		"timeout": json.Number(strconv.FormatInt(spec.TimeoutSeconds, 10)),
	}
	if len(spec.StatusMessages) > 0 {
		handler["statusMessage"] = spec.StatusMessages[0]
	}
	group := map[string]any{"hooks": []any{handler}}
	if spec.Matcher != nil {
		group["matcher"] = spec.Matcher
	}
	if spec.Description != "" {
		group["description"] = spec.Description
	}
	return group
}

// recognition describes how MergeManagedHandler recognizes installer-owned
// groups and handlers for this event.
func (spec ManagedEventSpec) recognition(command, hookSubcommand string) ManagedHandlerSpec {
	return ManagedHandlerSpec{
		Description:    spec.Description,
		StatusMessages: spec.StatusMessages,
		Command:        command,
		HookSubcommand: hookSubcommand,
		EligibleGroup:  spec.EligibleGroup,
	}
}

// ManagedHooksSpec declares a harness's managed hook set so Install and
// Doctor share one implementation: which events are managed, how older
// installations are recognized, and how the config file is read.
type ManagedHooksSpec struct {
	// Label names the harness in errors ("Devin", "Codex").
	Label string
	// HookSubcommand is the waypost subcommand installed as the hook command
	// ("devin-hook"); it recognizes handlers recorded under an older
	// executable path.
	HookSubcommand string
	// InstallHint is the command printed by doctor when a hook is missing
	// ("waypost install devin-hook").
	InstallHint string
	// AllowJSONC tolerates comments and trailing commas in the config file;
	// the rewrite emits strict JSON and reports a warning.
	AllowJSONC bool
	// KnownEvent reports whether an event name is one the harness defines.
	KnownEvent func(string) bool
	// Events lists the managed hook groups in installation order.
	Events []ManagedEventSpec
}

// InstallHooksResult reports the outcome of merging managed hooks into a
// config file.
type InstallHooksResult struct {
	Path     string
	Changed  bool
	Warnings []string
}

// DoctorHooksResult reports a successful doctor check.
type DoctorHooksResult struct {
	Path    string
	Command string
}

// InstallManagedHooks merges every managed event into the hooks object of
// the config at path, writing the document only when something changed.
func InstallManagedHooks(path string, spec ManagedHooksSpec, command string) (InstallHooksResult, error) {
	document, mode, wasJSONC, err := ReadConfigDocument(path, spec.Label, spec.AllowJSONC)
	if err != nil {
		return InstallHooksResult{}, err
	}

	hooks, err := ObjectField(document, "hooks")
	if err != nil {
		return InstallHooksResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	if err := ValidateHooksStructure(hooks, spec.KnownEvent); err != nil {
		return InstallHooksResult{}, fmt.Errorf("validate %q: %w", path, err)
	}

	changed := false
	for _, event := range spec.Events {
		groups, err := ArrayField(hooks, event.Event)
		if err != nil {
			return InstallHooksResult{}, fmt.Errorf("read %q: %w", path, err)
		}
		updated, eventChanged := MergeManagedHandler(groups, event.DesiredGroup(command), event.recognition(command, spec.HookSubcommand))
		hooks[event.Event] = updated
		changed = changed || eventChanged
	}
	document["hooks"] = hooks

	if !changed {
		return InstallHooksResult{Path: path, Changed: false}, nil
	}
	if err := WriteConfigDocument(path, document, mode, spec.Label); err != nil {
		return InstallHooksResult{}, err
	}
	result := InstallHooksResult{Path: path, Changed: true}
	if wasJSONC {
		result.Warnings = append(result.Warnings, fmt.Sprintf("%s contained JSONC comments or trailing commas; the rewrite emitted strict JSON and removed them", path))
	}
	return result, nil
}

// DoctorManagedHooks verifies every managed event carries the current
// command with the desired timeout inside an eligible group.
func DoctorManagedHooks(path string, spec ManagedHooksSpec, command string) (DoctorHooksResult, error) {
	document, _, _, err := ReadExistingConfigDocument(path, spec.Label, spec.AllowJSONC)
	if err != nil {
		return DoctorHooksResult{}, err
	}
	hooks, err := ExistingObjectField(document, "hooks")
	if err != nil {
		return DoctorHooksResult{}, fmt.Errorf("read %q: %w", path, err)
	}
	if err := ValidateHooksStructure(hooks, spec.KnownEvent); err != nil {
		return DoctorHooksResult{}, fmt.Errorf("validate %q: %w", path, err)
	}
	for _, event := range spec.Events {
		groups, err := ArrayField(hooks, event.Event)
		if err != nil {
			return DoctorHooksResult{}, fmt.Errorf("read %q: %w", path, err)
		}
		installed := false
		for _, item := range groups {
			group, ok := item.(map[string]any)
			if !ok || !event.EligibleGroup(group) {
				continue
			}
			if GroupHasCommandWithTimeout(group, command, event.TimeoutSeconds) {
				installed = true
				break
			}
		}
		if !installed {
			label := event.DoctorLabel
			if label == "" {
				label = event.Event
			}
			return DoctorHooksResult{}, fmt.Errorf("%s %s is not installed in %q; run `%s`", spec.Label, label, path, spec.InstallHint)
		}
	}
	return DoctorHooksResult{Path: path, Command: command}, nil
}
