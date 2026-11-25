package mcpinstall

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/BurntSushi/toml"
)

type tomlSectionKind int

const (
	tomlSectionRoot tomlSectionKind = iota
	tomlSectionMCPServers
	tomlSectionWaypost
	tomlSectionSkipTransportSubtable
	tomlSectionOther
)

type tomlStatementKind int

const (
	tomlStmtTrivia tomlStatementKind = iota
	tomlStmtHeader
	tomlStmtAssign
)

type tomlStatement struct {
	kind         tomlStatementKind
	raw          string
	headerParts  []string
	isArrayTable bool
	keyParts     []string
	rhsTrimmed   string
}

func ensureWaypostSection(contents, executable string, args ...string) (string, bool, error) {
	cleaned := strings.TrimPrefix(contents, "\xef\xbb\xbf")
	existingEnvVars, existingArgsValid, err := validateCodexTOML(cleaned)
	if err != nil {
		return "", false, err
	}

	const section = "[mcp_servers.waypost]"
	commandLine := "command = " + quoteTOMLString(executable)
	preserveExistingArgs := len(args) == 0
	if len(args) == 0 || args[0] != serverSubcommand {
		args = []string{serverSubcommand}
	}
	argsValues := make([]string, len(args))
	for i, arg := range args {
		argsValues[i] = quoteTOMLString(arg)
	}
	argsLine := "args = [" + strings.Join(argsValues, ", ") + "]"
	requiredLine := "required = true"
	envLine := formatEnvVarsLine(existingEnvVars)
	timeoutLine := fmt.Sprintf("tool_timeout_sec = %d", toolTimeoutSeconds)
	required := []struct {
		key  string
		line string
	}{
		{key: "command", line: commandLine},
		{key: "args", line: argsLine},
		{key: "required", line: requiredLine},
		{key: "env_vars", line: envLine},
		{key: "tool_timeout_sec", line: timeoutLine},
	}
	keysToRemove := map[string]bool{
		"url":                  true,
		"bearer_token_env_var": true,
		"http_headers":         true,
		"env_http_headers":     true,
		"http_headers_helper":  true,
		"oauth_resource":       true,
		"oauth":                true,
		// Codex enables MCP servers by default. Removing a persisted false
		// override also re-enables an entry that was previously disabled.
		"enabled": true,
	}

	normalized := strings.ReplaceAll(cleaned, "\r\n", "\n")
	stmts, err := scanTOMLStatements(normalized)
	if err != nil {
		return "", false, err
	}

	found := false
	sectionKind := tomlSectionRoot
	seen := make(map[string]bool, len(required))
	output := make([]string, 0, len(stmts)+len(required)+2)
	appendMissing := func() {
		for _, field := range required {
			if !seen[field.key] {
				output = append(output, field.line)
				seen[field.key] = true
			}
		}
	}

	for _, stmt := range stmts {
		switch stmt.kind {
		case tomlStmtHeader:
			if sectionKind == tomlSectionWaypost {
				appendMissing()
			}
			switch {
			case !stmt.isArrayTable && slices.Equal(stmt.headerParts, []string{"mcp_servers", serverName}):
				found = true
				sectionKind = tomlSectionWaypost
				seen = make(map[string]bool, len(required))
				output = append(output, stmt.raw)
			case isWaypostTransportSubtableParts(stmt.headerParts):
				sectionKind = tomlSectionSkipTransportSubtable
			case !stmt.isArrayTable && slices.Equal(stmt.headerParts, []string{"mcp_servers"}):
				sectionKind = tomlSectionMCPServers
				output = append(output, stmt.raw)
			default:
				sectionKind = tomlSectionOther
				output = append(output, stmt.raw)
			}
			continue

		case tomlStmtTrivia:
			if sectionKind != tomlSectionSkipTransportSubtable {
				output = append(output, stmt.raw)
			}
			continue
		}

		// tomlStmtAssign
		if sectionKind == tomlSectionSkipTransportSubtable {
			continue
		}
		if sectionKind == tomlSectionRoot && len(stmt.keyParts) >= 1 && stmt.keyParts[0] == "mcp_servers" {
			if len(stmt.keyParts) == 1 && strings.HasPrefix(stmt.rhsTrimmed, "{") {
				return "", false, errors.New("inline mcp_servers tables are unsupported; convert it to [mcp_servers.waypost] before installing")
			}
			if len(stmt.keyParts) >= 2 && stmt.keyParts[1] == serverName {
				return "", false, errors.New("inline mcp_servers.waypost tables are unsupported; convert it to [mcp_servers.waypost] before installing")
			}
		}
		if sectionKind == tomlSectionMCPServers && len(stmt.keyParts) >= 1 && stmt.keyParts[0] == serverName {
			return "", false, errors.New("inline mcp_servers.waypost tables are unsupported; convert it to [mcp_servers.waypost] before installing")
		}

		if sectionKind == tomlSectionWaypost && len(stmt.keyParts) >= 1 {
			topKey := stmt.keyParts[0]
			if keysToRemove[topKey] {
				continue
			}
			if len(stmt.keyParts) == 1 {
				replaced := false
				for _, field := range required {
					if topKey != field.key {
						continue
					}
					if topKey == "args" && preserveExistingArgs && existingArgsValid {
						if !seen[topKey] {
							output = append(output, stmt.raw)
							seen[topKey] = true
						}
					} else if !seen[topKey] {
						output = append(output, field.line)
						seen[topKey] = true
					}
					replaced = true
					break
				}
				if replaced {
					continue
				}
			}
		}
		output = append(output, stmt.raw)
	}

	if sectionKind == tomlSectionWaypost {
		appendMissing()
	}
	if !found {
		if len(output) > 0 && strings.TrimSpace(output[len(output)-1]) != "" {
			output = append(output, "")
		}
		output = append(output, section, commandLine, argsLine, requiredLine, envLine, timeoutLine)
	}

	updated := strings.Join(output, "\n") + "\n"
	if err := verifyUpdatedCodexTOML(updated, executable, keysToRemove); err != nil {
		return "", false, err
	}
	return updated, updated != contents, nil
}

func stringSliceFromTOMLArray(raw any, field, contextPrefix string) ([]string, error) {
	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("%s%s must be an array of strings", contextPrefix, field)
	}
	values := make([]string, 0, len(items))
	for _, item := range items {
		str, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s%s must contain only strings", contextPrefix, field)
		}
		values = append(values, str)
	}
	return values, nil
}

func verifyUpdatedCodexTOML(updated, executable string, keysToRemove map[string]bool) error {
	var verifyDoc map[string]any
	if _, err := toml.Decode(updated, &verifyDoc); err != nil {
		return fmt.Errorf("validate updated Codex TOML: %w", err)
	}
	servers, ok := verifyDoc["mcp_servers"].(map[string]any)
	if !ok {
		return errors.New("validate updated Codex TOML: missing mcp_servers table")
	}
	waypostTable, ok := servers[serverName].(map[string]any)
	if !ok {
		return errors.New("validate updated Codex TOML: missing [mcp_servers.waypost] table")
	}
	if cmd, _ := waypostTable["command"].(string); cmd != executable {
		return fmt.Errorf("validate updated Codex TOML: command = %q, want %q", cmd, executable)
	}
	if requiredVal, ok := waypostTable["required"].(bool); !ok || !requiredVal {
		return fmt.Errorf("validate updated Codex TOML: required = %#v, want true", waypostTable["required"])
	}
	rawArgs, exists := waypostTable["args"]
	if !exists {
		return errors.New("validate updated Codex TOML: missing required key \"args\"")
	}
	argsValues, err := stringSliceFromTOMLArray(rawArgs, "args", "validate updated Codex TOML: ")
	if err != nil {
		return err
	}
	if len(argsValues) == 0 || argsValues[0] != serverSubcommand {
		return fmt.Errorf("validate updated Codex TOML: args = %v, want non-empty starting with %q", argsValues, serverSubcommand)
	}
	rawEnvVars, exists := waypostTable["env_vars"]
	if !exists {
		return errors.New("validate updated Codex TOML: missing required key \"env_vars\"")
	}
	if _, err := stringSliceFromTOMLArray(rawEnvVars, "env_vars", "validate updated Codex TOML: "); err != nil {
		return err
	}
	if timeoutVal, ok := waypostTable["tool_timeout_sec"].(int64); !ok || timeoutVal != int64(toolTimeoutSeconds) {
		return fmt.Errorf("validate updated Codex TOML: tool_timeout_sec = %#v, want %d", waypostTable["tool_timeout_sec"], toolTimeoutSeconds)
	}
	for key := range keysToRemove {
		if _, exists := waypostTable[key]; exists {
			return fmt.Errorf("validate updated Codex TOML: unexpected stale key %q", key)
		}
	}
	return nil
}

func validateCodexTOML(contents string) ([]string, bool, error) {
	if strings.TrimSpace(contents) == "" {
		return nil, false, nil
	}
	var doc map[string]any
	if _, err := toml.Decode(contents, &doc); err != nil {
		return nil, false, fmt.Errorf("parse Codex TOML: %w", err)
	}
	rawServers, hasServers := doc["mcp_servers"]
	if !hasServers {
		return nil, false, nil
	}
	servers, ok := rawServers.(map[string]any)
	if !ok {
		return nil, false, errors.New("Codex config mcp_servers must be a TOML table")
	}
	rawWaypost, hasWaypost := servers[serverName]
	if !hasWaypost {
		return nil, false, nil
	}
	switch rawWaypost.(type) {
	case []map[string]any, []any:
		return nil, false, errors.New("array table [[mcp_servers.waypost]] is unsupported; convert it to [mcp_servers.waypost] before installing")
	}
	waypostTable, ok := rawWaypost.(map[string]any)
	if !ok {
		return nil, false, errors.New("Codex config mcp_servers.waypost must be a TOML table")
	}

	var envValues []string
	if rawEnvVars, hasEnvVars := waypostTable["env_vars"]; hasEnvVars {
		var err error
		envValues, err = stringSliceFromTOMLArray(rawEnvVars, "env_vars", "Codex Waypost ")
		if err != nil {
			return nil, false, err
		}
	}

	existingArgsValid := false
	if rawArgs, hasArgs := waypostTable["args"]; hasArgs {
		argValues, err := stringSliceFromTOMLArray(rawArgs, "args", "Codex Waypost ")
		if err != nil {
			return nil, false, err
		}
		existingArgsValid = len(argValues) > 0 && argValues[0] == serverSubcommand
	}

	return envValues, existingArgsValid, nil
}

func formatEnvVarsLine(existing []string) string {
	values := make([]string, 0, len(existing)+len(requiredEnvVars))
	seen := make(map[string]bool, len(existing)+len(requiredEnvVars))
	for _, value := range existing {
		if !seen[value] {
			values = append(values, value)
			seen[value] = true
		}
	}
	for _, required := range requiredEnvVars {
		if !seen[required] {
			values = append(values, required)
			seen[required] = true
		}
	}
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = quoteTOMLString(value)
	}
	return "env_vars = [" + strings.Join(quoted, ", ") + "]"
}

func quoteTOMLString(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 2)
	b.WriteByte('"')
	for _, r := range value {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\f':
			b.WriteString(`\f`)
		case '\r':
			b.WriteString(`\r`)
		default:
			if r < 0x20 || r == 0x7f {
				fmt.Fprintf(&b, `\u%04X`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func scanTOMLStatements(src string) ([]tomlStatement, error) {
	stmts := make([]tomlStatement, 0, 16)
	pos := 0
	for pos < len(src) {
		lineStart := pos
		for pos < len(src) && (src[pos] == ' ' || src[pos] == '\t') {
			pos++
		}
		if pos == len(src) || src[pos] == '\n' || src[pos] == '#' {
			lineEnd := indexByteOrEnd(src, pos, '\n')
			stmts = append(stmts, tomlStatement{
				kind: tomlStmtTrivia,
				raw:  src[lineStart:lineEnd],
			})
			pos = advancePastNewline(src, lineEnd)
			continue
		}
		if src[pos] == '[' {
			lineEnd := indexByteOrEnd(src, pos, '\n')
			raw := src[lineStart:lineEnd]
			headerParts, isArrayTable, ok := parseTOMLHeader(strings.TrimSpace(raw))
			if !ok {
				return nil, fmt.Errorf("invalid TOML table header %q", strings.TrimSpace(raw))
			}
			stmts = append(stmts, tomlStatement{
				kind:         tomlStmtHeader,
				raw:          raw,
				headerParts:  headerParts,
				isArrayTable: isArrayTable,
			})
			pos = advancePastNewline(src, lineEnd)
			continue
		}

		eqRel, ok := findTOMLKeyEquals(src[pos:])
		if !ok {
			lineEnd := indexByteOrEnd(src, pos, '\n')
			return nil, fmt.Errorf("invalid TOML statement %q", strings.TrimSpace(src[lineStart:lineEnd]))
		}
		eqPos := pos + eqRel
		rawKeys, ok := splitTOMLHeaderKeys(strings.TrimSpace(src[pos:eqPos]))
		if !ok || len(rawKeys) == 0 {
			return nil, fmt.Errorf("invalid TOML assignment key %q", strings.TrimSpace(src[pos:eqPos]))
		}
		keyParts := make([]string, 0, len(rawKeys))
		for _, part := range rawKeys {
			normalized, ok := normalizeTOMLKeyPart(part)
			if !ok {
				return nil, fmt.Errorf("invalid TOML assignment key part %q", part)
			}
			keyParts = append(keyParts, normalized)
		}

		stmtEnd, err := scanTOMLValueEnd(src, eqPos+1)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, tomlStatement{
			kind:       tomlStmtAssign,
			raw:        src[lineStart:stmtEnd],
			keyParts:   keyParts,
			rhsTrimmed: strings.TrimSpace(src[eqPos+1 : stmtEnd]),
		})
		pos = advancePastNewline(src, stmtEnd)
	}
	return stmts, nil
}

func indexByteOrEnd(src string, start int, target byte) int {
	if rel := strings.IndexByte(src[start:], target); rel >= 0 {
		return start + rel
	}
	return len(src)
}

func advancePastNewline(src string, end int) int {
	if end < len(src) && src[end] == '\n' {
		return end + 1
	}
	return end
}

func findTOMLKeyEquals(lineAndRest string) (int, bool) {
	var quote byte
	escaped := false
	for i := 0; i < len(lineAndRest); i++ {
		ch := lineAndRest[i]
		if quote != 0 {
			if quote == '"' {
				if escaped {
					escaped = false
				} else if ch == '\\' {
					escaped = true
				} else if ch == '"' {
					quote = 0
				}
			} else if ch == '\'' {
				quote = 0
			}
			continue
		}
		if ch == '\n' || ch == '#' {
			return -1, false
		}
		if ch == '"' || ch == '\'' {
			quote = ch
		} else if ch == '=' {
			return i, true
		}
	}
	return -1, false
}

func scanTOMLValueEnd(src string, start int) (int, error) {
	bracketDepth := 0
	braceDepth := 0
	var quote byte
	triple := false
	escaped := false
	inComment := false

	for i := start; i < len(src); {
		ch := src[i]
		if inComment {
			if ch == '\n' {
				inComment = false
				if bracketDepth == 0 && braceDepth == 0 {
					return i, nil
				}
			}
			i++
			continue
		}
		if quote != 0 {
			if triple {
				if quote == '"' && ch == '\\' {
					if i+1 < len(src) {
						i += 2
						continue
					}
					return len(src), errors.New("unterminated triple-quoted TOML string")
				}
				if ch == quote && i+2 < len(src) && src[i+1] == quote && src[i+2] == quote {
					extra := 0
					for extra < 2 && i+3+extra < len(src) && src[i+3+extra] == quote {
						extra++
					}
					i += 3 + extra
					quote = 0
					triple = false
					continue
				}
				i++
				continue
			}
			if ch == '\n' {
				return len(src), errors.New("unterminated TOML string")
			}
			if quote == '"' {
				if escaped {
					escaped = false
				} else if ch == '\\' {
					escaped = true
				} else if ch == '"' {
					quote = 0
				}
			} else if ch == '\'' {
				quote = 0
			}
			i++
			continue
		}

		switch ch {
		case '#':
			inComment = true
			i++
		case '"', '\'':
			quote = ch
			if i+2 < len(src) && src[i+1] == ch && src[i+2] == ch {
				triple = true
				i += 3
			} else {
				i++
			}
		case '[':
			bracketDepth++
			i++
		case ']':
			if bracketDepth == 0 {
				return len(src), errors.New("unexpected ']' in TOML value")
			}
			bracketDepth--
			i++
		case '{':
			braceDepth++
			i++
		case '}':
			if braceDepth == 0 {
				return len(src), errors.New("unexpected '}' in TOML value")
			}
			braceDepth--
			i++
		case '\n':
			if bracketDepth == 0 && braceDepth == 0 {
				return i, nil
			}
			i++
		default:
			i++
		}
	}
	if quote != 0 || bracketDepth != 0 || braceDepth != 0 {
		return len(src), errors.New("unterminated TOML value")
	}
	return len(src), nil
}

func isWaypostTransportSubtableParts(parts []string) bool {
	if len(parts) < 3 || parts[0] != "mcp_servers" || parts[1] != serverName {
		return false
	}
	switch parts[2] {
	case "oauth", "http_headers", "env_http_headers", "http_headers_helper":
		return true
	default:
		return false
	}
}

func parseTOMLHeader(line string) ([]string, bool, bool) {
	line = strings.TrimSpace(line)
	if len(line) < 2 || line[0] != '[' {
		return nil, false, false
	}
	isArrayTable := len(line) >= 4 && line[1] == '['
	closing := tomlHeaderEnd(line, isArrayTable)
	if closing < 0 {
		return nil, false, false
	}
	startInner := 1
	afterClosing := closing + 1
	if isArrayTable {
		startInner = 2
		afterClosing = closing + 2
	}
	trailing := strings.TrimSpace(line[afterClosing:])
	if trailing != "" && !strings.HasPrefix(trailing, "#") {
		return nil, false, false
	}
	parts, ok := splitTOMLHeaderKeys(strings.TrimSpace(line[startInner:closing]))
	if !ok || len(parts) == 0 {
		return nil, false, false
	}
	for index, part := range parts {
		part, ok = normalizeTOMLKeyPart(part)
		if !ok {
			return nil, false, false
		}
		parts[index] = part
	}
	return parts, isArrayTable, true
}

func tomlHeaderEnd(line string, isArrayTable bool) int {
	var quote byte
	escaped := false
	start := 1
	if isArrayTable {
		start = 2
	}
	for index := start; index < len(line); index++ {
		value := line[index]
		if quote != 0 {
			if quote == '"' {
				if escaped {
					escaped = false
				} else if value == '\\' {
					escaped = true
				} else if value == quote {
					quote = 0
				}
			} else if value == quote {
				quote = 0
			}
			continue
		}
		if value == '#' {
			return -1
		}
		if value == '"' || value == '\'' {
			quote = value
		} else if value == ']' {
			if isArrayTable {
				if index+1 < len(line) && line[index+1] == ']' {
					return index
				}
				return -1
			}
			return index
		}
	}
	return -1
}

func splitTOMLHeaderKeys(value string) ([]string, bool) {
	parts := make([]string, 0, 2)
	start := 0
	var quote byte
	escaped := false
	for index := 0; index < len(value); index++ {
		current := value[index]
		if quote != 0 {
			if quote == '"' {
				if escaped {
					escaped = false
				} else if current == '\\' {
					escaped = true
				} else if current == quote {
					quote = 0
				}
			} else if current == quote {
				quote = 0
			}
			continue
		}
		if current == '"' || current == '\'' {
			quote = current
		} else if current == '.' {
			parts = append(parts, strings.TrimSpace(value[start:index]))
			start = index + 1
		}
	}
	if quote != 0 {
		return nil, false
	}
	parts = append(parts, strings.TrimSpace(value[start:]))
	return parts, true
}

func normalizeTOMLKeyPart(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		var decoded map[string]bool
		if _, err := toml.Decode(value+" = true", &decoded); err != nil || len(decoded) != 1 {
			return "", false
		}
		for key := range decoded {
			return key, true
		}
		return "", false
	}
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		return value[1 : len(value)-1], true
	}
	return value, true
}
