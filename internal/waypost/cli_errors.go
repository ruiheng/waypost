package waypost

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"strconv"
	"strings"
)

type cliJSONErrorOverride struct {
	err      error
	emitJSON bool
}

func (e *cliJSONErrorOverride) Error() string {
	return e.err.Error()
}

func (e *cliJSONErrorOverride) Unwrap() error {
	return e.err
}

// WithCLIJSONError attaches an authoritative JSON error-emission decision from
// a command parser (such as rootcmd) without requiring internal/waypost to
// duplicate external command flag definitions.
func WithCLIJSONError(err error, emitJSON bool) error {
	if err == nil {
		return nil
	}
	return &cliJSONErrorOverride{err: err, emitJSON: emitJSON}
}

type cliErrorDocument struct {
	Status    string `json:"status"`
	ErrorCode string `json:"error_code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	Details   any    `json:"details,omitempty"`
}

type cliReceiveRecoveryClaim struct {
	DeliveryID       string `json:"delivery_id"`
	LeaseToken       string `json:"lease_token"`
	RecipientAddress string `json:"recipient_address"`
	LeaseExpiresAt   string `json:"lease_expires_at"`
}

type cliReceiveRecoveryDetails struct {
	RemainingByStateStatus string                    `json:"remaining_by_state_status,omitempty"`
	Claims                 []cliReceiveRecoveryClaim `json:"claims"`
}

// NewGroupWebFlagSet defines the canonical flag set for `waypost group web`.
// Shared with rootcmd so CLIRequestsJSON and WriteCLIJSONError agree on
// group web without duplicating flag definitions. Although group web has no
// --json output mode, it belongs to the `group` CLI-owned error contract.
func NewGroupWebFlagSet(listen, group *string) *flag.FlagSet {
	fs := flag.NewFlagSet("waypost group web", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(listen, "listen", "127.0.0.1:0", "HTTP listen address")
	fs.StringVar(group, "group", "", "initial group address")
	return fs
}

func isKnownRootCommand(cmd string) bool {
	switch cmd {
	case "mcp", "codex-hook", "devin-hook", "claude-hook", "install", "doctor", "migrate", "doc",
		"send", "forward", "recv", "wait", "watch", "read", "show",
		"ack", "renew", "release", "defer", "undefer", "fail", "dead-letter",
		"list", "stale", "group", "address":
		return true
	default:
		return false
	}
}

func splitGlobalCLIArgs(args []string) []string {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		switch {
		case arg == "--":
			return args[index+1:]
		case arg == "--state-dir" || arg == "-state-dir":
			index++
			continue
		case strings.HasPrefix(arg, "--state-dir=") || strings.HasPrefix(arg, "-state-dir="):
			continue
		case arg == "--version" || arg == "-version" || strings.HasPrefix(arg, "--version=") || strings.HasPrefix(arg, "-version="):
			continue
		case len(arg) > 1 && arg[0] == '-':
			if !strings.Contains(arg, "=") && index+1 < len(args) {
				next := args[index+1]
				if (len(next) == 0 || next[0] != '-') && !isKnownRootCommand(next) {
					index++
				}
			}
			continue
		default:
			return args[index:]
		}
	}
	return nil
}

// flagSetRequestsJSON reports whether args select --json while skipping values
// belonging to non-boolean flags defined on fs (and likely space-separated
// values following unknown flags without '=').
func flagSetRequestsJSON(fs *flag.FlagSet, args []string, interspersed bool) bool {
	jsonRequested := false
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == "--" {
			break
		}
		if len(arg) <= 1 || arg[0] != '-' {
			if interspersed {
				continue
			}
			break
		}
		trimmed := strings.TrimPrefix(arg, "--")
		if len(trimmed) == len(arg) {
			trimmed = strings.TrimPrefix(arg, "-")
		}
		name, rawVal, hasEq := strings.Cut(trimmed, "=")
		if name == "json" {
			if !hasEq {
				jsonRequested = true
			} else if enabled, err := strconv.ParseBool(rawVal); err == nil {
				jsonRequested = enabled
			}
			continue
		}
		if !hasEq && fs != nil {
			if fs.Lookup(name) != nil {
				if readFlagTakesValue(fs, arg) {
					index++
				}
			} else if index+1 < len(args) && (len(args[index+1]) == 0 || args[index+1][0] != '-') {
				index++
			}
		}
	}
	return jsonRequested
}

// CLIRequestsJSON reports whether a canonical JSON command path was selected.
// It inspects the target command's real flag.FlagSet so boolean and value
// flags match Go's flag parser without duplicating per-command flag tables.
func CLIRequestsJSON(args []string) bool {
	cmdArgs := splitGlobalCLIArgs(args)
	if len(cmdArgs) == 0 {
		return false
	}
	cmd := cmdArgs[0]
	if cmd == "group" && len(cmdArgs) > 1 && cmdArgs[1] == "web" {
		var listen, group string
		return flagSetRequestsJSON(NewGroupWebFlagSet(&listen, &group), cmdArgs[2:], false)
	}

	var capturedFS *flag.FlagSet
	probe := NewApp(strings.NewReader(""), io.Discard, io.Discard)
	probe.inspectFlagSet = func(fs *flag.FlagSet) {
		capturedFS = fs
	}
	_, _ = probe.prepareCommand(cmdArgs)

	flagArgs := cmdArgs[1:]
	if (cmd == "group" || cmd == "address") && len(cmdArgs) > 1 && (len(cmdArgs[1]) <= 1 || !strings.HasPrefix(cmdArgs[1], "-")) {
		flagArgs = cmdArgs[2:]
	}
	if capturedFS == nil {
		if cmd != "group" && cmd != "address" {
			return false
		}
		return flagSetRequestsJSON(nil, flagArgs, true)
	}
	if capturedFS.Lookup("json") == nil {
		return false
	}

	interspersed := cmd == "read" || cmd == "show"
	return flagSetRequestsJSON(capturedFS, flagArgs, interspersed)
}

func cliOwnedCommand(args []string) bool {
	cmdArgs := splitGlobalCLIArgs(args)
	if len(cmdArgs) == 0 {
		return false
	}
	switch cmdArgs[0] {
	case "forward", "wait", "list", "stale", "read", "show", "undefer", "fail", "dead-letter", "group", "address":
		return true
	default:
		return false
	}
}

// WriteCLIJSONError writes the structured failure contract for CLI-owned
// operations and the exceptional receive-recovery result. It leaves normal
// human-oriented command failures untouched.
func WriteCLIJSONError(w io.Writer, args []string, err error) bool {
	var jsonOverride *cliJSONErrorOverride
	if errors.As(err, &jsonOverride) {
		if !jsonOverride.emitJSON {
			return false
		}
		document := cliErrorFor(err)
		_ = json.NewEncoder(w).Encode(document)
		return true
	}
	if !CLIRequestsJSON(args) || (!cliOwnedCommand(args) && !errors.Is(err, ErrReceiveRecoveryRequired)) {
		return false
	}
	document := cliErrorFor(err)
	_ = json.NewEncoder(w).Encode(document)
	return true
}

func cliErrorFor(err error) cliErrorDocument {
	document := cliErrorDocument{
		Status:    "error",
		ErrorCode: "internal",
		Message:   err.Error(),
		Retryable: false,
	}

	var recovery *ReceiveRecoveryRequiredError
	if errors.As(err, &recovery) {
		claims := make([]cliReceiveRecoveryClaim, 0, len(recovery.Claims))
		for _, claim := range recovery.Claims {
			claims = append(claims, cliReceiveRecoveryClaim{
				DeliveryID:       claim.DeliveryID,
				LeaseToken:       claim.LeaseToken,
				RecipientAddress: claim.RecipientAddress,
				LeaseExpiresAt:   claim.LeaseExpiresAt,
			})
		}
		document.ErrorCode = "receive_recovery_required"
		remainingStatus := ""
		if recovery.Phase == "" || recovery.Phase == ReceiveRecoveryPhaseRemainingState {
			remainingStatus = "unavailable"
		}
		document.Details = cliReceiveRecoveryDetails{
			RemainingByStateStatus: remainingStatus,
			Claims:                 claims,
		}
		return document
	}
	switch {
	case errors.Is(err, ErrBodyIntegrity):
		document.ErrorCode = "integrity_error"
	case isSQLiteBusy(err):
		document.ErrorCode = "busy"
		document.Retryable = true
	case errors.Is(err, ErrLeaseNotFound), errors.Is(err, ErrDeliveryNotFound), errors.Is(err, ErrMessageNotFound), errors.Is(err, ErrGroupNotFound):
		document.ErrorCode = "not_found"
	case errors.Is(err, ErrInvalidArgument), errors.Is(err, ErrInvalidPaginationCursor):
		document.ErrorCode = "invalid_argument"
	case errors.Is(err, ErrInvalidState):
		document.ErrorCode = "invalid_state"
	case errors.Is(err, ErrGroupExists), errors.Is(err, ErrActiveMembershipExists), errors.Is(err, ErrActiveSubscriberExists):
		document.ErrorCode = "already_exists"
	case errors.Is(err, ErrLeaseExpired), errors.Is(err, ErrLeaseNotLeased), errors.Is(err, ErrLeaseRenewChanged), errors.Is(err, ErrLeaseTokenMismatch), errors.Is(err, ErrActiveMembershipMissing), errors.Is(err, ErrActiveSubscriberMissing), errors.Is(err, ErrAddressReservedByEndpoint), errors.Is(err, ErrAddressReservedByGroup):
		document.ErrorCode = "invalid_state"
	}
	return document
}
