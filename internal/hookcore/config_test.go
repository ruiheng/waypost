package hookcore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestReadConfigDocumentMissingFile(t *testing.T) {
	t.Parallel()

	document, mode, wasJSONC, err := ReadConfigDocument(filepath.Join(t.TempDir(), "hooks.json"), "Test", false)
	if err != nil {
		t.Fatalf("ReadConfigDocument() error = %v", err)
	}
	if len(document) != 0 || mode != 0o600 || wasJSONC {
		t.Fatalf("ReadConfigDocument() = %v, %o, %v; want empty doc, 600, false", document, mode, wasJSONC)
	}
}

func TestReadConfigDocumentPreservesNumbers(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "hooks.json")
	if err := os.WriteFile(path, []byte(`{"hooks":{},"timeout":5.0}`), 0o640); err != nil {
		t.Fatal(err)
	}
	document, mode, wasJSONC, err := ReadConfigDocument(path, "Test", false)
	if err != nil {
		t.Fatalf("ReadConfigDocument() error = %v", err)
	}
	if mode != 0o640 {
		t.Fatalf("mode = %o, want 640", mode)
	}
	if wasJSONC {
		t.Fatal("wasJSONC = true for plain JSON")
	}
	if number, ok := document["timeout"].(json.Number); !ok || number.String() != "5.0" {
		t.Fatalf("timeout = %#v, want json.Number 5.0", document["timeout"])
	}
}

func TestReadConfigDocumentJSONC(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.json")
	contents := "{\n  // comment\n  \"hooks\": {},\n}\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	document, _, wasJSONC, err := ReadConfigDocument(path, "Test", true)
	if err != nil {
		t.Fatalf("ReadConfigDocument(JSONC) error = %v", err)
	}
	if !wasJSONC {
		t.Fatal("wasJSONC = false for JSONC input")
	}
	if _, ok := document["hooks"]; !ok {
		t.Fatalf("document = %v, want hooks key", document)
	}

	if _, _, _, err := ReadConfigDocument(path, "Test", false); err == nil {
		t.Fatal("ReadConfigDocument(JSONC, allowJSONC=false) error = nil, want parse error")
	}
}

func TestWriteConfigDocumentPreservesModeAndSymlink(t *testing.T) {
	if testing.Short() {
		t.Skip("symlink test")
	}
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "target.json")
	link := filepath.Join(dir, "link.json")
	if err := os.WriteFile(target, []byte("{}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}

	document := map[string]any{"hooks": map[string]any{}}
	if err := WriteConfigDocument(link, document, 0o640, "Test"); err != nil {
		t.Fatalf("WriteConfigDocument() error = %v", err)
	}
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("link was replaced by a regular file, want symlink preserved")
	}
	targetInfo, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if targetInfo.Mode().Perm() != 0o640 {
		t.Fatalf("target mode = %o, want 640", targetInfo.Mode().Perm())
	}
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "{\n  \"hooks\": {}\n}\n" {
		t.Fatalf("target contents = %q, want rewritten document", contents)
	}
}

func TestMergeManagedHandler(t *testing.T) {
	t.Parallel()

	command := "/bin/waypost test-hook"
	desired := map[string]any{
		"hooks": []any{
			map[string]any{"type": "command", "command": command, "timeout": json.Number("5")},
		},
	}
	spec := ManagedHandlerSpec{
		Command:        command,
		HookSubcommand: "test-hook",
		EligibleGroup:  func(map[string]any) bool { return true },
	}

	t.Run("appends to empty", func(t *testing.T) {
		updated, changed := MergeManagedHandler(nil, desired, spec)
		if !changed || len(updated) != 1 {
			t.Fatalf("MergeManagedHandler() = %v, %v; want appended group", updated, changed)
		}
	})

	t.Run("refreshes managed command", func(t *testing.T) {
		groups := []any{
			map[string]any{"hooks": []any{
				map[string]any{"type": "command", "command": "/old/waypost test-hook"},
				map[string]any{"type": "command", "command": "keep-me"},
			}},
		}
		updated, changed := MergeManagedHandler(groups, desired, spec)
		if !changed {
			t.Fatal("MergeManagedHandler() changed = false, want true")
		}
		group := updated[0].(map[string]any)
		handlers := group["hooks"].([]any)
		if len(handlers) != 2 {
			t.Fatalf("handlers = %v, want managed refresh plus sibling", handlers)
		}
	})

	t.Run("keeps commands merely mentioning subcommand", func(t *testing.T) {
		groups := []any{
			map[string]any{"hooks": []any{
				map[string]any{"type": "command", "command": "echo test-hook"},
			}},
		}
		updated, _ := MergeManagedHandler(groups, desired, spec)
		group := updated[0].(map[string]any)
		handlers := group["hooks"].([]any)
		if len(handlers) != 1 {
			t.Fatalf("handlers = %v, want user command preserved", handlers)
		}
		if handlers[0].(map[string]any)["command"] != "echo test-hook" {
			t.Fatalf("handler = %v, want echo test-hook kept", handlers[0])
		}
	})

	t.Run("unrelated groups untouched", func(t *testing.T) {
		groups := []any{
			map[string]any{"matcher": "^Bash$", "hooks": []any{
				map[string]any{"type": "command", "command": "other"},
			}},
		}
		strictSpec := spec
		strictSpec.EligibleGroup = func(group map[string]any) bool {
			return MatcherTargetsOnly(group["matcher"], "exec")
		}
		updated, changed := MergeManagedHandler(groups, desired, strictSpec)
		if !changed || len(updated) != 2 {
			t.Fatalf("MergeManagedHandler() = %v, %v; want original plus appended", updated, changed)
		}
		if updated[0].(map[string]any)["matcher"] != "^Bash$" {
			t.Fatalf("group[0] = %v, want untouched", updated[0])
		}
	})
}

func TestValidateHooksStructure(t *testing.T) {
	t.Parallel()

	known := func(name string) bool { return name == "SessionStart" }

	if err := ValidateHooksStructure(map[string]any{
		"SessionStart": []any{
			map[string]any{"matcher": "^compact$", "hooks": []any{map[string]any{"type": "command"}}},
		},
	}, known); err != nil {
		t.Fatalf("ValidateHooksStructure(valid) error = %v", err)
	}

	// Unknown events may hold arbitrary values.
	if err := ValidateHooksStructure(map[string]any{"CustomEvent": "anything"}, known); err != nil {
		t.Fatalf("ValidateHooksStructure(unknown event) error = %v", err)
	}

	if err := ValidateHooksStructure(map[string]any{"SessionStart": "bad"}, known); err == nil {
		t.Fatal("ValidateHooksStructure(non-array known event) = nil, want error")
	}
	if err := ValidateHooksStructure(map[string]any{"SessionStart": []any{
		map[string]any{"matcher": 5, "hooks": []any{}},
	}}, known); err == nil {
		t.Fatal("ValidateHooksStructure(non-string matcher) = nil, want error")
	}
	if err := ValidateHooksStructure(map[string]any{"SessionStart": []any{
		map[string]any{"hooks": "bad"},
	}}, known); err == nil {
		t.Fatal("ValidateHooksStructure(non-array hooks) = nil, want error")
	}
}

func TestMatcherHelpers(t *testing.T) {
	t.Parallel()

	if !MatcherTargetsOnly("^exec$", "exec") {
		t.Error("MatcherTargetsOnly(^exec$) = false, want true")
	}
	if MatcherTargetsOnly("^(exec|Bash)$", "exec") {
		t.Error("MatcherTargetsOnly(multi) = true, want false")
	}
	if !MatcherTargetsCompactOnly("^compact$") {
		t.Error("MatcherTargetsCompactOnly(^compact$) = false, want true")
	}
	if MatcherTargetsCompactOnly("^(compact|resume)$") {
		t.Error("MatcherTargetsCompactOnly(compact|resume) = true, want false")
	}
	if !MatcherTargetsReceiveCompletionOnly("^(exec|mcp__waypost__waypost_recv)$", "exec", "mcp__waypost__waypost_recv", []string{"edit"}) {
		t.Error("MatcherTargetsReceiveCompletionOnly(receive) = false, want true")
	}
	if MatcherTargetsReceiveCompletionOnly("^(exec|mcp__waypost__waypost_recv|edit)$", "exec", "mcp__waypost__waypost_recv", []string{"edit"}) {
		t.Error("MatcherTargetsReceiveCompletionOnly(with edit) = true, want false")
	}
	if !MatcherFiresForNonToolEvent(map[string]any{}, "probe") {
		t.Error("MatcherFiresForNonToolEvent(no matcher) = false, want true")
	}
	if !MatcherFiresForNonToolEvent(map[string]any{"matcher": ".*"}, "probe") {
		t.Error("MatcherFiresForNonToolEvent(.*) = false, want true")
	}
	if MatcherFiresForNonToolEvent(map[string]any{"matcher": "^Bash$"}, "") {
		t.Error("MatcherFiresForNonToolEvent(^Bash$ vs empty) = true, want false")
	}
}
