package waypost

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

var strictSessionAddressTargetPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)

var strictSessionAddressSchemes = map[string]struct{}{
	"agent-deck": {},
	"claude":     {},
	"codex":      {},
	"devin":      {},
	"gemini":     {},
	"opencode":   {},
	"thurbox":    {},
}

type ParsedAddress struct {
	Address  string
	Scheme   string
	ID       string
	Segments []string
}

func ParseAddress(address string) (ParsedAddress, error) {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return ParsedAddress{}, invalidArgumentError(fmt.Errorf("address is required"))
	}

	scheme, id, ok := strings.Cut(trimmed, "/")
	if !ok || scheme == "" || id == "" {
		return ParsedAddress{}, invalidArgumentError(fmt.Errorf("invalid address %q: expected scheme/id", address))
	}
	if err := validateAddressToken(trimmed, "scheme", scheme); err != nil {
		return ParsedAddress{}, invalidArgumentError(err)
	}

	segments := strings.Split(id, "/")
	for _, segment := range segments {
		if segment == "" {
			return ParsedAddress{}, invalidArgumentError(fmt.Errorf("invalid address %q: empty id segment", address))
		}
		if err := validateAddressToken(trimmed, "id segment", segment); err != nil {
			return ParsedAddress{}, invalidArgumentError(err)
		}
	}

	if _, ok := strictSessionAddressSchemes[scheme]; ok {
		if len(segments) != 1 {
			return ParsedAddress{}, invalidArgumentError(fmt.Errorf("invalid address %q: %s addresses require a single target segment", address, scheme))
		}
		if !strictSessionAddressTargetPattern.MatchString(segments[0]) {
			return ParsedAddress{}, invalidArgumentError(fmt.Errorf(
				"invalid address %q: %s target %q must match %s",
				address,
				scheme,
				segments[0],
				strictSessionAddressTargetPattern.String(),
			))
		}
	}

	return ParsedAddress{
		Address:  trimmed,
		Scheme:   scheme,
		ID:       id,
		Segments: segments,
	}, nil
}

func NormalizeAddress(address string) (string, error) {
	parsed, err := ParseAddress(address)
	if err != nil {
		return "", err
	}
	return parsed.Address, nil
}

func NormalizeGroupAddress(address string) (string, error) {
	parsed, err := ParseAddress(address)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != AddressKindGroup {
		return "", invalidArgumentError(fmt.Errorf("invalid group address %q: group addresses must start with group/", address))
	}
	return parsed.Address, nil
}

func IsGroupAddress(address string) bool {
	parsed, err := ParseAddress(address)
	return err == nil && parsed.Scheme == AddressKindGroup
}

func NormalizeOptionalAddress(address string) (string, error) {
	if strings.TrimSpace(address) == "" {
		return "", nil
	}
	return NormalizeAddress(address)
}

func NormalizeAddressList(values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized, err := NormalizeAddress(value)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}

func NormalizeRequiredAddressValues(values []string, flagName string) ([]string, error) {
	if len(values) == 0 {
		return nil, invalidArgumentError(fmt.Errorf("%s is required", flagName))
	}
	normalized, err := NormalizeAddressList(values)
	if err != nil {
		return nil, err
	}
	if len(normalized) == 0 {
		return nil, invalidArgumentError(fmt.Errorf("%s is required", flagName))
	}
	return normalized, nil
}

func validateAddressToken(address, label, token string) error {
	for _, r := range token {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("invalid address %q: %s %q must not contain whitespace or control characters", address, label, token)
		}
	}
	return nil
}
