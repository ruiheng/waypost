package waypost

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
)

type preparedCommand func(context.Context, *Store) error

type stringListFlag []string

var (
	errFlagValueRequired  = errors.New("flag value required")
	errFlagSetInspected   = errors.New("flagset inspected")
)

type requiredFlagValueError struct {
	flagName string
}

func (e requiredFlagValueError) Error() string {
	return fmt.Sprintf("%s is required", e.flagName)
}

func (e requiredFlagValueError) Is(target error) bool {
	return target == errFlagValueRequired
}

func (f *stringListFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringListFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func requireFlag(value, name string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	return nil
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func normalizeAddresses(address string, addresses []string, flagName string) ([]string, error) {
	values := make([]string, 0, len(addresses)+1)
	if address != "" {
		values = append(values, address)
	}
	values = append(values, addresses...)

	if err := validateInputItemCount(flagName, len(values)); err != nil {
		return nil, err
	}
	return NormalizeRequiredAddressValues(values, flagName)
}

func normalizeFlagValues(values []string, flagName string) ([]string, error) {
	if err := validateInputItemCount(flagName, len(values)); err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, requiredFlagValueError{flagName: flagName}
	}

	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			return nil, fmt.Errorf("%s must not be empty", flagName)
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	if len(normalized) == 0 {
		return nil, requiredFlagValueError{flagName: flagName}
	}
	return normalized, nil
}

func (a *App) parseCommandFlags(fs *flag.FlagSet, args []string, writeHelp func()) error {
	if a.inspectFlagSet != nil {
		a.inspectFlagSet(fs)
		return errFlagSetInspected
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			writeHelp()
			return ErrHelpRequested
		}
		return err
	}
	return nil
}

func isHelpArg(value string) bool {
	return value == "-h" || value == "--help"
}

func writeHelp(w io.Writer, lines []string) {
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
}

func flagWasProvided(fs *flag.FlagSet, name string) bool {
	provided := false
	fs.Visit(func(current *flag.Flag) {
		if current.Name == name {
			provided = true
		}
	})
	return provided
}
