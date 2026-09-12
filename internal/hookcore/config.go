package hookcore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"

	"github.com/ruiheng/waypost/internal/jsonc"
)

// ReadConfigDocument loads a JSON hooks config. A missing file yields an
// empty document and mode 0600. When allowJSONC is set, comments and trailing
// commas are tolerated and wasJSONC reports that a rewrite will emit strict
// JSON. label names the harness in error messages.
func ReadConfigDocument(path, label string, allowJSONC bool) (document map[string]any, mode os.FileMode, wasJSONC bool, err error) {
	document, mode, wasJSONC, err = ReadExistingConfigDocument(path, label, allowJSONC)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]any{}, 0o600, false, nil
	}
	return document, mode, wasJSONC, err
}

// ReadExistingConfigDocument loads an existing JSON hooks config, preserving
// exact JSON numbers via UseNumber.
func ReadExistingConfigDocument(path, label string, allowJSONC bool) (map[string]any, os.FileMode, bool, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, false, fmt.Errorf("read %s hooks config %q: %w", label, path, err)
	}
	wasJSONC := false
	if allowJSONC {
		contents, wasJSONC, err = jsonc.Normalize(contents)
		if err != nil {
			return nil, 0, false, fmt.Errorf("parse %s hooks config %q: %w", label, path, err)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		return nil, 0, false, fmt.Errorf("parse %s hooks config %q: %w", label, path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return nil, 0, false, fmt.Errorf("parse %s hooks config %q: %w", label, path, err)
	}
	if document == nil {
		return nil, 0, false, fmt.Errorf("parse %s hooks config %q: expected a JSON object", label, path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, 0, false, fmt.Errorf("stat %s hooks config %q: %w", label, path, err)
	}
	return document, info.Mode().Perm(), wasJSONC, nil
}

// WriteConfigDocument serializes document as indented strict JSON and
// atomically replaces path, preserving mode and writing through symlinks.
func WriteConfigDocument(path string, document map[string]any, mode os.FileMode, label string) error {
	contents, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s hooks config %q: %w", label, path, err)
	}
	contents = append(contents, '\n')
	writePath, err := ResolveConfigWritePath(path, label)
	if err != nil {
		return err
	}
	writeDir := filepath.Dir(writePath)
	if err := os.MkdirAll(writeDir, 0o700); err != nil {
		return fmt.Errorf("create %s hooks config directory %q: %w", label, writeDir, err)
	}

	temporary, err := os.CreateTemp(writeDir, ".hooks-config.tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary %s hooks config: %w", label, err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(mode); err != nil {
		return fmt.Errorf("set %s hooks config permissions: %w", label, err)
	}
	if _, err := temporary.Write(contents); err != nil {
		return fmt.Errorf("write temporary %s hooks config: %w", label, err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary %s hooks config: %w", label, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary %s hooks config: %w", label, err)
	}
	if err := ReplaceFile(temporaryPath, writePath); err != nil {
		return fmt.Errorf("replace %s hooks config %q: %w", label, path, err)
	}
	return nil
}

// ResolveConfigWritePath follows a symlink at path so the rewrite updates the
// link target rather than replacing the symlink.
func ResolveConfigWritePath(path, label string) (string, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return path, nil
	}
	if err != nil {
		return "", fmt.Errorf("inspect %s hooks config %q: %w", label, path, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return path, nil
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve %s hooks config symlink %q: %w", label, path, err)
	}
	return resolved, nil
}

// ObjectField returns the object at name, or an empty map when absent.
func ObjectField(document map[string]any, name string) (map[string]any, error) {
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

// ExistingObjectField returns the object at name, failing when absent.
func ExistingObjectField(document map[string]any, name string) (map[string]any, error) {
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

// ArrayField returns the array at name, or nil when absent.
func ArrayField(document map[string]any, name string) ([]any, error) {
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

// ExistingArrayField returns the array at name, failing when absent.
func ExistingArrayField(document map[string]any, name string) ([]any, error) {
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

func CloneObject(value map[string]any) map[string]any {
	cloned := make(map[string]any, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

// GroupHasCommand reports whether the group's hooks include a command
// handler running command.
func GroupHasCommand(group map[string]any, command string) bool {
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

// GroupHasCommandWithTimeout reports whether the group's hooks include a
// command handler running command with the given timeout in seconds.
func GroupHasCommandWithTimeout(group map[string]any, command string, timeoutSeconds int64) bool {
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

// ValidateHooksStructure checks the shape of a hooks map: known events must
// hold arrays of groups, each group an object with an optional string matcher
// and a hooks array of objects. knownEvent reports whether an event name is
// one the harness defines; unknown names may hold arbitrary values.
func ValidateHooksStructure(hooks map[string]any, knownEvent func(string) bool) error {
	for event, value := range hooks {
		groups, ok := value.([]any)
		if !ok {
			if knownEvent(event) {
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

// ManagedHandlerSpec describes one managed hook definition: the desired
// group contents, the current install command, and how groups written by
// this installer are recognized. Description and StatusMessages mark
// managed groups/handlers for harnesses that write them; HookSubcommand
// (for example "devin-hook") recognizes commands recorded under an older
// Waypost executable path.
type ManagedHandlerSpec struct {
	Description    string
	StatusMessages []string
	Command        string
	HookSubcommand string
	EligibleGroup  func(map[string]any) bool
}

// MergeManagedHandler inserts or refreshes the desired managed group inside
// groups. Unrelated groups and sibling handlers are preserved; a group the
// installer manages is rewritten in place so ordering stays stable.
func MergeManagedHandler(groups []any, desired map[string]any, spec ManagedHandlerSpec) ([]any, bool) {
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
		managedGroup := spec.Description != "" && description == spec.Description
		if !managedGroup && !spec.EligibleGroup(group) {
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
			preserved := CloneObject(group)
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

func managedCommandHandler(value any, spec ManagedHandlerSpec, managedGroup bool, groupSize int) bool {
	handler, ok := value.(map[string]any)
	if !ok {
		return false
	}
	handlerType, _ := handler["type"].(string)
	if handlerType != "command" {
		return false
	}
	handlerCommand, _ := handler["command"].(string)
	if handlerCommand == spec.Command {
		return true
	}
	if spec.HookSubcommand != "" && IsManagedHookCommand(handlerCommand, spec.HookSubcommand) {
		return true
	}
	statusMessage, _ := handler["statusMessage"].(string)
	for _, managedStatus := range spec.StatusMessages {
		if statusMessage == managedStatus {
			return true
		}
	}
	return managedGroup && groupSize == 1
}

// MatcherTargetsOnly reports whether a hook matcher is exactly ^tool$.
func MatcherTargetsOnly(value any, tool string) bool {
	matcher, ok := value.(string)
	return ok && matcher == "^"+tool+"$"
}

// MatcherTargetsCompactOnly reports whether a matcher matches the "compact"
// session-start source and none of the other known sources.
func MatcherTargetsCompactOnly(value any) bool {
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

// MatcherTargetsReceiveCompletionOnly reports whether a PostToolUse matcher
// covers the shell tool and the waypost_recv MCP tool without also matching
// other tools, so it only fires for receive completion.
func MatcherTargetsReceiveCompletionOnly(value any, shellTool, receiveTool string, reject []string) bool {
	matcher, ok := value.(string)
	if !ok {
		return false
	}
	compiled, err := regexp.Compile(matcher)
	if err != nil || !compiled.MatchString(shellTool) || !compiled.MatchString(receiveTool) {
		return false
	}
	for _, other := range reject {
		if compiled.MatchString(other) {
			return false
		}
	}
	return true
}

// MatcherFiresForNonToolEvent reports whether a group's matcher (absent,
// null, or a regex matching probe) can fire for a non-tool lifecycle event.
// probe is a sample value the matcher must accept: harnesses that match the
// event's source use a source name; harnesses whose matchers target tool
// names use the empty string.
func MatcherFiresForNonToolEvent(group map[string]any, probe string) bool {
	value, exists := group["matcher"]
	if !exists || value == nil {
		return true
	}
	matcher, ok := value.(string)
	if !ok {
		return false
	}
	compiled, err := regexp.Compile(matcher)
	return err == nil && compiled.MatchString(probe)
}
