package mcpserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ruiheng/waypost/internal/userdirs"
)

// ponytail: fragile adapter for agent-deck private state.db; replace with a public agent-deck API when one exists.
type agentDeckDBMatch struct {
	SessionID      string
	ProjectPath    string
	CodexSessionID string
}

// listAgentDeckSessionIDs returns session ids recorded by agent-deck. Missing
// or unreadable state databases are ignored because this lookup is only used
// to improve a send-time warning.
func listAgentDeckSessionIDs(ctx context.Context) []string {
	var ids []string
	for _, dbPath := range agentDeckStateDBPaths() {
		rows, err := listAgentDeckSessionIDsInDB(ctx, dbPath)
		if err != nil {
			continue
		}
		ids = append(ids, rows...)
	}
	return dedupe(ids)
}

func listAgentDeckSessionIDsInDB(ctx context.Context, dbPath string) ([]string, error) {
	if strings.TrimSpace(dbPath) == "" {
		return nil, nil
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat: %w", err)
	}
	if info.IsDir() {
		return nil, nil
	}

	db, err := sql.Open("sqlite3", "file:"+dbPath+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `
		SELECT id
		FROM instances
		ORDER BY last_accessed DESC, created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("query private instances table: %w", err)
	}
	defer rows.Close()

	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan private instances row: %w", err)
		}
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read private instances rows: %w", err)
	}
	return ids, nil
}

func lookupAgentDeckSessionByCodexID(ctx context.Context, codexSessionID string) (*agentDeckDBMatch, []string) {
	codexSessionID = strings.TrimSpace(codexSessionID)
	if codexSessionID == "" {
		return nil, nil
	}
	var warnings []string
	for _, dbPath := range agentDeckStateDBPaths() {
		match, err := lookupAgentDeckSessionByCodexIDInDB(ctx, dbPath, codexSessionID)
		if err != nil {
			warnings = append(warnings, agentDeckStateDBWarning(dbPath, err))
			continue
		}
		if match != nil {
			return match, warnings
		}
	}
	return nil, warnings
}

func lookupAgentDeckSessionByCodexIDInDB(ctx context.Context, dbPath, codexSessionID string) (*agentDeckDBMatch, error) {
	if strings.TrimSpace(dbPath) == "" {
		return nil, nil
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat: %w", err)
	}
	if info.IsDir() {
		return nil, nil
	}

	db, err := sql.Open("sqlite3", "file:"+dbPath+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `
		SELECT id, project_path, tool_data
		FROM instances
		WHERE tool = 'codex' OR command LIKE '%codex%' OR tool_data LIKE '%codex_session_id%' OR tool_data LIKE '%codex_thread_id%'
		ORDER BY last_accessed DESC, created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("query private instances table: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, projectPath, rawToolData string
		if err := rows.Scan(&id, &projectPath, &rawToolData); err != nil {
			return nil, fmt.Errorf("scan private instances row: %w", err)
		}
		toolData, ok := parseAgentDeckToolData(rawToolData)
		if !ok {
			continue
		}
		sessionID := strings.TrimSpace(toolData.sessionID())
		if sessionID == codexSessionID {
			return &agentDeckDBMatch{
				SessionID:      strings.TrimSpace(id),
				ProjectPath:    strings.TrimSpace(projectPath),
				CodexSessionID: sessionID,
			}, nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read private instances rows: %w", err)
	}
	return nil, nil
}

func lookupAgentDeckSessionByWorkdir(ctx context.Context, workdir, agentDeckSessionID string) (*agentDeckDBMatch, []string) {
	workdir = strings.TrimSpace(workdir)
	if workdir == "" {
		return nil, nil
	}
	canonicalWorkdir, err := canonicalizeExistingPath(workdir)
	if err != nil {
		return nil, nil
	}
	var warnings []string
	for _, dbPath := range agentDeckStateDBPaths() {
		match, err := lookupAgentDeckSessionByWorkdirInDB(ctx, dbPath, canonicalWorkdir, agentDeckSessionID)
		if err != nil {
			warnings = append(warnings, agentDeckStateDBWarning(dbPath, err))
			continue
		}
		if match != nil {
			return match, warnings
		}
	}
	return nil, warnings
}

func lookupAgentDeckSessionByWorkdirInDB(ctx context.Context, dbPath, canonicalWorkdir, agentDeckSessionID string) (*agentDeckDBMatch, error) {
	if strings.TrimSpace(dbPath) == "" || strings.TrimSpace(canonicalWorkdir) == "" {
		return nil, nil
	}
	agentDeckSessionID = strings.TrimSpace(agentDeckSessionID)
	info, err := os.Stat(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat: %w", err)
	}
	if info.IsDir() {
		return nil, nil
	}

	db, err := sql.Open("sqlite3", "file:"+dbPath+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `
		SELECT id, project_path, tool_data
		FROM instances
		WHERE tool = 'codex' OR command LIKE '%codex%' OR tool_data LIKE '%codex_session_id%' OR tool_data LIKE '%codex_thread_id%'
		ORDER BY last_accessed DESC, created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("query private instances table: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, projectPath, rawToolData string
		if err := rows.Scan(&id, &projectPath, &rawToolData); err != nil {
			return nil, fmt.Errorf("scan private instances row: %w", err)
		}
		canonicalProjectPath, err := canonicalizeExistingPath(projectPath)
		if err != nil || !sameCanonicalPath(canonicalProjectPath, canonicalWorkdir) {
			continue
		}
		if agentDeckSessionID != "" && strings.TrimSpace(id) != agentDeckSessionID {
			continue
		}
		toolData, ok := parseAgentDeckToolData(rawToolData)
		sessionID := strings.TrimSpace(toolData.sessionID())
		if !ok || toolSessionIDValidationFailure(sessionID) != "" {
			continue
		}
		return &agentDeckDBMatch{
			SessionID:      strings.TrimSpace(id),
			ProjectPath:    strings.TrimSpace(projectPath),
			CodexSessionID: sessionID,
		}, nil
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read private instances rows: %w", err)
	}
	return nil, nil
}

type agentDeckToolData struct {
	CodexSessionID string `json:"codex_session_id"`
	CodexThreadID  string `json:"codex_thread_id"`
}

func (d agentDeckToolData) sessionID() string {
	return firstNonEmpty(d.CodexThreadID, d.CodexSessionID)
}

func parseAgentDeckToolData(raw string) (agentDeckToolData, bool) {
	var toolData agentDeckToolData
	if err := json.Unmarshal([]byte(raw), &toolData); err != nil {
		return agentDeckToolData{}, false
	}
	return toolData, true
}

func sameCanonicalPath(left, right string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func agentDeckStateDBPaths() []string {
	homeDir, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(homeDir) == "" {
		return nil
	}

	// Agent Deck migrated its data from ~/.agent-deck to the XDG data
	// directory. Keep both roots so existing installations continue to work.
	xdgDataHome, err := userdirs.DataRoot(userdirs.AtHome(homeDir))
	if err != nil {
		return nil
	}
	baseDirs := []string{
		filepath.Join(xdgDataHome, "agent-deck"),
		filepath.Join(homeDir, ".agent-deck"),
	}

	paths := make([]string, 0, 4)
	seen := map[string]bool{}
	for _, baseDir := range baseDirs {
		for _, path := range agentDeckProfileDBPaths(baseDir) {
			if seen[path] {
				continue
			}
			seen[path] = true
			paths = append(paths, path)
		}
	}
	return paths
}

func agentDeckProfileDBPaths(baseDir string) []string {
	profilesDir := filepath.Join(baseDir, "profiles")
	profiles := []string{}
	if envProfile := strings.TrimSpace(os.Getenv("AGENTDECK_PROFILE")); envProfile != "" {
		profiles = append(profiles, filepath.Base(envProfile))
	}
	if configProfile := agentDeckDefaultProfile(filepath.Join(baseDir, "config.json")); configProfile != "" {
		profiles = append(profiles, filepath.Base(configProfile))
	}
	profiles = append(profiles, "default")
	if entries, err := os.ReadDir(profilesDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				profiles = append(profiles, entry.Name())
			}
		}
	}

	paths := make([]string, 0, len(profiles))
	for _, profile := range profiles {
		if profile == "" || profile == "." || profile == ".." {
			continue
		}
		paths = append(paths, filepath.Join(profilesDir, profile, "state.db"))
	}
	return paths
}

func agentDeckDefaultProfile(configPath string) string {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return ""
	}
	var config struct {
		DefaultProfile string `json:"default_profile"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return ""
	}
	return strings.TrimSpace(config.DefaultProfile)
}

func agentDeckStateDBWarning(dbPath string, err error) string {
	return fmt.Sprintf("agent-deck private state database lookup skipped %s during auto-bind: %v", dbPath, err)
}
