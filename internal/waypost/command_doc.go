package waypost

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
)

const cliDocOverview = `Waypost state is isolated by state directory; clients using different state directories do not share a mailbox.

Personal deliveries have four states:
- queued: waiting to be claimed; claimable when visible_at is reached.
- leased: claimed by one receiver; an expired lease may be reclaimed with a new lease token.
- acked: completed successfully and retained for history.
- dead_letter: reached the failure-attempt limit or was explicitly dead-lettered, and is no longer claimable.

Receiving a personal delivery returns its delivery ID and lease token. While it is leased:
- renew extends the lease without changing its state or token.
- ack moves it to acked.
- release moves it to queued immediately without recording a failure.
- defer moves it to queued with a future visible_at.
- fail increments attempt_count, then moves it to queued or dead_letter.
- dead-letter moves it directly to dead_letter without incrementing attempt_count.

Group messages track unread/read state per person and do not use personal delivery states or leases.

Persistence and recipient notification are separate outcomes.

Use waypost COMMAND --help for command syntax. Use waypost doc --list for focused topics.
`

var cliDocTopics = map[string]string{
	"addresses": `A Waypost address is a public mailbox name within one state directory. It has the form scheme/id. Waypost does not assign a current address.

For an agent session, use its actual session identity: agent-deck/<session-id> or thurbox/<session-id> for a hosted session; otherwise codex/<thread-id>, claude/<session-id>, gemini/<session-id>, opencode/<session-id>, or devin/<session-id>. Obtain the ID from the launcher or tool. Never invent it from a role, task, or display name.

Use the address as --from when sending and --for when receiving, and give it to peers as the return address. Personal addresses are created on first use. group/... is reserved for explicitly created groups.
`,
	"mcp-cli-boundary": `MCP is optional. When present, its process-local bindings, active-lease tracking, and automatic renewal are not shared with a separate CLI process.

MCP and CLI share durable messages and delivery state only when they use the same state directory. Use the executable and state directory reported by MCP status before mixing the two.

The current MCP server defines its tool surface. Use CLI for an operation it does not expose. MCP reconciles durable CLI transitions before later lease work.
`,
	"dead-letter": `Dead-lettering is an explicit terminal decision for a currently leased personal delivery. It requires the current lease token, moves the delivery directly to dead_letter, and does not increment attempt_count.

Use fail when processing failed but retry remains appropriate. Use dead-letter when the message must not be retried, such as an unsupported request or a permanently invalid payload. The reason is retained with the delivery, and the message remains readable for history or diagnosis.
`,
	"recovery": `A delivery ID alone does not prove lease ownership. Lease-bound transitions require the current lease token.

Lease expiry does not complete a delivery or immediately invalidate its token. It makes the delivery eligible for reclaim; reclaiming replaces the token.

If message context is lost, read the persisted message or delivery before settling it. Acknowledged and dead-letter deliveries remain readable.

Undefer only makes a future-visible queued delivery visible now. It does not restore a lease; receive it again to obtain a current token.
`,
	"history": `Message IDs and delivery IDs identify different records. A message ID identifies stored content and message metadata; a delivery ID identifies one recipient's mutable delivery state.

Acknowledgement and dead-lettering do not delete message content. Reading history does not claim a personal delivery or mark a group message read.

Forwarding creates a new destination message, and a new delivery for a personal target, while preserving the source identity. The sender is the forwarder; forwarded_from_address records the original source.
`,
	"groups": `A group address must be created explicitly. Active members at send time are recorded as eligible for that message.

Group receive requires a person identity, returns that person's oldest unread message, and atomically records the first read. It does not create a lease.

Membership, per-person read state, and notification subscriptions are separate durable records. Notification delivery does not determine message eligibility or read state.
`,
	"diagnostics": `An address has a durable kind in one state directory: endpoint, group, or unbound. Unbound is a valid inspection result, not an error.

Durable address kind is separate from a live MCP binding. The group/ scheme is reserved for explicitly created groups; another valid unbound address can become endpoint-owned through personal delivery.
`,
}

var cliDocTopicAliases = map[string]string{
	"ack":           "mcp-cli-boundary",
	"address":       "addresses",
	"bind":          "mcp-cli-boundary",
	"claim-history": "mcp-cli-boundary",
	"defer":         "mcp-cli-boundary",
	"fail":          "recovery",
	"forward":       "mcp-cli-boundary",
	"group":         "groups",
	"list":          "history",
	"read":          "history",
	"show":          "history",
	"receive":       "mcp-cli-boundary",
	"receiver":      "mcp-cli-boundary",
	"recv":          "mcp-cli-boundary",
	"release":       "mcp-cli-boundary",
	"send":          "mcp-cli-boundary",
	"status":        "mcp-cli-boundary",
	"undefer":       "recovery",
	"wait":          "mcp-cli-boundary",
}

func (a *App) runDocCommand(args []string) error {
	fs := flag.NewFlagSet("waypost doc", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var list bool
	fs.BoolVar(&list, "list", false, "list available topics")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			a.writeDocHelp()
			return ErrHelpRequested
		}
		return err
	}

	remaining := fs.Args()
	if err := validateInputItemCount("doc topics", len(remaining)); err != nil {
		return err
	}
	if list {
		if len(remaining) != 0 {
			return errors.New("doc --list does not accept topics")
		}
		return a.writeDocTopics()
	}
	if len(remaining) == 0 {
		_, err := fmt.Fprint(a.stdout, cliDocOverview)
		return err
	}
	if len(remaining) == 1 {
		_, topic, ok := resolveDocTopic(remaining[0])
		if !ok {
			return unknownDocTopicError(remaining[0])
		}
		_, err := fmt.Fprint(a.stdout, topic)
		return err
	}

	output, err := formatDocTopicBlocks(remaining)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(a.stdout, output)
	return err
}

func formatDocTopicBlocks(topics []string) (string, error) {
	var output strings.Builder
	seen := make(map[string]struct{}, len(topics))
	written := 0
	for _, requestedName := range topics {
		topicName, topic, ok := resolveDocTopic(requestedName)
		if !ok {
			return "", unknownDocTopicError(requestedName)
		}
		if _, exists := seen[topicName]; exists {
			continue
		}
		seen[topicName] = struct{}{}
		if written > 0 {
			output.WriteString("\n\n")
		}
		fmt.Fprintf(&output, "waypost: %s\n", topicName)
		for _, line := range strings.Split(strings.TrimSuffix(topic, "\n"), "\n") {
			output.WriteString("  ")
			output.WriteString(line)
			output.WriteByte('\n')
		}
		written++
	}
	return output.String(), nil
}

func resolveDocTopic(name string) (string, string, bool) {
	if topic, ok := cliDocTopics[name]; ok {
		return name, topic, true
	}
	canonicalName, ok := cliDocTopicAliases[name]
	if !ok {
		return "", "", false
	}
	return canonicalName, cliDocTopics[canonicalName], true
}

func unknownDocTopicError(name string) error {
	return fmt.Errorf("unknown doc topic %q; available topics: %s", name, strings.Join(docTopicNames(), ", "))
}

func docTopicNames() []string {
	topics := make([]string, 0, len(cliDocTopics))
	for topic := range cliDocTopics {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

func (a *App) writeDocTopics() error {
	for _, topic := range docTopicNames() {
		if _, err := fmt.Fprintln(a.stdout, topic); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) writeDocHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost doc",
		"  waypost doc --list",
		"  waypost doc TOPIC...",
	})
}
