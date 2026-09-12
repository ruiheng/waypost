package hookcore

import (
	"encoding/json"
	"strings"
)

// CLIJSONReceive mirrors the `waypost recv --json` document. A top-level
// status field carries the terminal outcome; without one, the record must be
// a complete personal or group delivery, or a messages array of complete
// personal deliveries.
type CLIJSONReceive struct {
	Status           string           `json:"status"`
	DeliveryID       string           `json:"delivery_id"`
	RecipientAddress string           `json:"recipient_address"`
	LeaseToken       string           `json:"lease_token"`
	MessageID        string           `json:"message_id"`
	GroupAddress     string           `json:"group_address"`
	Person           string           `json:"person"`
	FirstReadAt      string           `json:"first_read_at"`
	Messages         []CLIJSONReceive `json:"messages"`
}

// IsReceiveStatus reports whether status is a terminal receive outcome.
func IsReceiveStatus(status string) bool {
	return status == "received" || status == "no_message"
}

// SuccessfulJSONReceive parses a JSON receive document. parsed reports
// whether the output was JSON at all; when it is, success reflects the
// receive outcome.
func SuccessfulJSONReceive(output string) (success, parsed bool) {
	var response CLIJSONReceive
	if json.Unmarshal([]byte(output), &response) != nil {
		return false, false
	}
	if response.Status != "" {
		return IsReceiveStatus(response.Status), true
	}
	if CompletePersonalJSONReceive(response) || CompleteGroupJSONReceive(response) {
		return true, true
	}
	if len(response.Messages) == 0 {
		return false, true
	}
	for _, message := range response.Messages {
		if !CompletePersonalJSONReceive(message) {
			return false, true
		}
	}
	return true, true
}

func CompletePersonalJSONReceive(response CLIJSONReceive) bool {
	return response.DeliveryID != "" && response.RecipientAddress != "" && response.LeaseToken != ""
}

func CompleteGroupJSONReceive(response CLIJSONReceive) bool {
	return response.MessageID != "" && response.GroupAddress != "" && response.Person != "" && response.FirstReadAt != ""
}

// SuccessfulFullYAMLReceive recognizes a YAML receive document with the
// complete personal or group field set.
func SuccessfulFullYAMLReceive(output string) bool {
	fields := make(map[string]bool)
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		name, value, found := strings.Cut(line, ":")
		if found && strings.TrimSpace(value) != "" {
			fields[name] = true
		}
	}
	personalReceive := fields["delivery_id"] && fields["recipient_address"] && fields["lease_token"]
	groupReceive := fields["message_id"] && fields["group_address"] && fields["person"] && fields["first_read_at"]
	return personalReceive || groupReceive
}

// ShellReceiveOutputSucceeded decodes a tool_response that carries the
// command output as a JSON string and reports whether it records a
// completed receive.
func ShellReceiveOutputSucceeded(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var output string
	if json.Unmarshal(raw, &output) != nil {
		return false
	}
	return ReceiveOutputSucceeded(output)
}

// MCPReceiveResultSucceeded parses a serialized waypost_recv MCP result and
// reports whether it records a terminal receive outcome. The result may
// carry the status in structuredContent or at the top level, or be the
// receive document itself, depending on the transport.
func MCPReceiveResultSucceeded(output string) bool {
	output = strings.TrimSpace(output)
	if output == "" {
		return false
	}
	var result struct {
		IsError           bool   `json:"isError"`
		Status            string `json:"status"`
		StructuredContent struct {
			Status string `json:"status"`
		} `json:"structuredContent"`
	}
	if err := json.Unmarshal([]byte(output), &result); err == nil {
		if result.IsError {
			return false
		}
		if IsReceiveStatus(result.StructuredContent.Status) || IsReceiveStatus(result.Status) {
			return true
		}
	}
	if success, parsed := SuccessfulJSONReceive(output); parsed {
		return success
	}
	return false
}

// ReceiveOutputSucceeded reports whether the textual output of a `waypost
// recv` invocation records a completed receive, across the JSON, key=value,
// YAML-fronted, and single-line output shapes the CLI can emit.
func ReceiveOutputSucceeded(output string) bool {
	output = strings.TrimSpace(output)
	if output == "" {
		return false
	}
	if success, parsed := SuccessfulJSONReceive(output); parsed {
		return success
	}
	if output == "status=no_message" {
		return true
	}

	firstLine, _, _ := strings.Cut(output, "\n")
	if strings.HasPrefix(firstLine, "status: ") {
		var status string
		if json.Unmarshal([]byte(strings.TrimPrefix(firstLine, "status: ")), &status) == nil {
			return IsReceiveStatus(status)
		}
		return false
	}
	if SuccessfulFullYAMLReceive(output) {
		return true
	}
	personalReceive := strings.HasPrefix(firstLine, "delivery_id=") &&
		strings.Contains(firstLine, " recipient_address=") &&
		strings.Contains(firstLine, " lease_token=")
	groupReceive := strings.HasPrefix(firstLine, "message_id=") &&
		strings.Contains(firstLine, " group=") &&
		strings.Contains(firstLine, " person=") &&
		strings.Contains(firstLine, " first_read_at=")
	return personalReceive || groupReceive
}
