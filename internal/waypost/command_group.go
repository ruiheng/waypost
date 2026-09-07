package waypost

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
)

func (a *App) prepareGroupCommand(args []string) (preparedCommand, error) {
	if len(args) == 0 {
		return nil, errors.New("expected a group subcommand: create, list, add-member, remove-member, members, add-subscriber, remove-subscriber, or subscribers")
	}
	if isHelpArg(args[0]) {
		a.writeGroupHelp()
		return nil, ErrHelpRequested
	}

	switch args[0] {
	case "create":
		return a.prepareGroupCreateCommand(args[1:])
	case "list":
		return a.prepareGroupListCommand(args[1:])
	case "add-member":
		return a.prepareGroupAddMemberCommand(args[1:])
	case "remove-member":
		return a.prepareGroupRemoveMemberCommand(args[1:])
	case "members":
		return a.prepareGroupMembersCommand(args[1:])
	case "add-subscriber":
		return a.prepareGroupAddSubscriberCommand(args[1:])
	case "remove-subscriber":
		return a.prepareGroupRemoveSubscriberCommand(args[1:])
	case "subscribers":
		return a.prepareGroupSubscribersCommand(args[1:])
	default:
		return nil, fmt.Errorf("unknown group subcommand %q", args[0])
	}
}

func (a *App) prepareGroupListCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost group list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var formats outputFlags
	var limit int
	var cursor string
	fs.IntVar(&limit, "limit", DefaultPageSize, "maximum items in this page")
	fs.StringVar(&cursor, "cursor", "", "pagination cursor")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeGroupListHelp); err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}
	if _, err := normalizePageParams(PageParams{Limit: limit, Cursor: cursor}); err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		page, err := store.ListGroupsPage(ctx, PageParams{Limit: limit, Cursor: cursor})
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, page)
		}
		for _, group := range page.Items {
			if _, err := fmt.Fprintf(
				a.stdout,
				"group_id=%s address=%s created_at=%s\n",
				group.GroupID,
				group.Address,
				group.CreatedAt,
			); err != nil {
				return err
			}
		}
		return writeNextCursor(a.stdout, page.NextCursor)
	}, nil
}

func (a *App) prepareGroupCreateCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost group create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var groupAddress string
	var formats outputFlags
	fs.StringVar(&groupAddress, "group", "", "group address")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeGroupCreateHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(groupAddress, "--group"); err != nil {
		return nil, err
	}
	groupAddress, err := NormalizeAddress(groupAddress)
	if err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		group, err := store.CreateGroup(ctx, groupAddress)
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, group)
		}
		_, err = fmt.Fprintf(a.stdout, "group_id=%s address=%s\n", group.GroupID, group.Address)
		return err
	}, nil
}

func (a *App) prepareGroupAddMemberCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost group add-member", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var groupAddress string
	var person string
	var formats outputFlags
	fs.StringVar(&groupAddress, "group", "", "group address")
	fs.StringVar(&person, "person", "", "person identity")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeGroupAddMemberHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(groupAddress, "--group"); err != nil {
		return nil, err
	}
	groupAddress, err := NormalizeAddress(groupAddress)
	if err != nil {
		return nil, err
	}
	if err := requireFlag(person, "--person"); err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		membership, err := store.AddGroupMember(ctx, groupAddress, person)
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, membership)
		}
		_, err = fmt.Fprintf(
			a.stdout,
			"membership_id=%s group=%s person=%s active=%t joined_at=%s\n",
			membership.MembershipID,
			membership.GroupAddress,
			membership.Person,
			membership.Active,
			membership.JoinedAt,
		)
		return err
	}, nil
}

func (a *App) prepareGroupRemoveMemberCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost group remove-member", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var groupAddress string
	var person string
	var formats outputFlags
	fs.StringVar(&groupAddress, "group", "", "group address")
	fs.StringVar(&person, "person", "", "person identity")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeGroupRemoveMemberHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(groupAddress, "--group"); err != nil {
		return nil, err
	}
	groupAddress, err := NormalizeAddress(groupAddress)
	if err != nil {
		return nil, err
	}
	if err := requireFlag(person, "--person"); err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		membership, err := store.RemoveGroupMember(ctx, groupAddress, person)
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, membership)
		}
		leftAt := ""
		if membership.LeftAt != nil {
			leftAt = *membership.LeftAt
		}
		_, err = fmt.Fprintf(
			a.stdout,
			"membership_id=%s group=%s person=%s active=%t joined_at=%s left_at=%s\n",
			membership.MembershipID,
			membership.GroupAddress,
			membership.Person,
			membership.Active,
			membership.JoinedAt,
			leftAt,
		)
		return err
	}, nil
}

func (a *App) prepareGroupMembersCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost group members", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var groupAddress string
	var formats outputFlags
	var limit int
	var cursor string
	fs.StringVar(&groupAddress, "group", "", "group address")
	fs.IntVar(&limit, "limit", DefaultPageSize, "maximum items in this page")
	fs.StringVar(&cursor, "cursor", "", "pagination cursor")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeGroupMembersHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(groupAddress, "--group"); err != nil {
		return nil, err
	}
	groupAddress, err := NormalizeAddress(groupAddress)
	if err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}
	if _, err := normalizePageParams(PageParams{Limit: limit, Cursor: cursor}); err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		page, err := store.ListGroupMembersPage(ctx, groupAddress, PageParams{Limit: limit, Cursor: cursor})
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, page)
		}
		for _, membership := range page.Items {
			leftAt := ""
			if membership.LeftAt != nil {
				leftAt = *membership.LeftAt
			}
			if _, err := fmt.Fprintf(
				a.stdout,
				"membership_id=%s person=%s active=%t joined_at=%s left_at=%s\n",
				membership.MembershipID,
				membership.Person,
				membership.Active,
				membership.JoinedAt,
				leftAt,
			); err != nil {
				return err
			}
		}
		return writeNextCursor(a.stdout, page.NextCursor)
	}, nil
}

func (a *App) prepareGroupAddSubscriberCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost group add-subscriber", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var groupAddress string
	var notifyAddress string
	var person string
	var formats outputFlags
	fs.StringVar(&groupAddress, "group", "", "group address")
	fs.StringVar(&notifyAddress, "notify-address", "", "notification address")
	fs.StringVar(&person, "person", "", "subscriber person identity")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeGroupAddSubscriberHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(groupAddress, "--group"); err != nil {
		return nil, err
	}
	groupAddress, err := NormalizeAddress(groupAddress)
	if err != nil {
		return nil, err
	}
	if err := requireFlag(notifyAddress, "--notify-address"); err != nil {
		return nil, err
	}
	notifyAddress, err = NormalizeAddress(notifyAddress)
	if err != nil {
		return nil, err
	}
	if err := requireFlag(person, "--person"); err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		subscriber, err := store.AddGroupNotificationSubscriber(ctx, groupAddress, notifyAddress, person)
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, subscriber)
		}
		_, err = fmt.Fprintf(a.stdout, "subscriber_id=%s group=%s notify_address=%s person=%s active=%t created_at=%s\n", subscriber.SubscriberID, subscriber.GroupAddress, subscriber.NotifyAddress, subscriber.Person, subscriber.Active, subscriber.CreatedAt)
		return err
	}, nil
}

func (a *App) prepareGroupRemoveSubscriberCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost group remove-subscriber", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var groupAddress string
	var notifyAddress string
	var formats outputFlags
	fs.StringVar(&groupAddress, "group", "", "group address")
	fs.StringVar(&notifyAddress, "notify-address", "", "notification address")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeGroupRemoveSubscriberHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(groupAddress, "--group"); err != nil {
		return nil, err
	}
	groupAddress, err := NormalizeAddress(groupAddress)
	if err != nil {
		return nil, err
	}
	if err := requireFlag(notifyAddress, "--notify-address"); err != nil {
		return nil, err
	}
	notifyAddress, err = NormalizeAddress(notifyAddress)
	if err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		subscriber, err := store.RemoveGroupNotificationSubscriber(ctx, groupAddress, notifyAddress)
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, subscriber)
		}
		removedAt := ""
		if subscriber.RemovedAt != nil {
			removedAt = *subscriber.RemovedAt
		}
		_, err = fmt.Fprintf(a.stdout, "subscriber_id=%s group=%s notify_address=%s active=%t removed_at=%s\n", subscriber.SubscriberID, subscriber.GroupAddress, subscriber.NotifyAddress, subscriber.Active, removedAt)
		return err
	}, nil
}

func (a *App) prepareGroupSubscribersCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost group subscribers", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var groupAddress string
	var formats outputFlags
	var limit int
	var cursor string
	fs.StringVar(&groupAddress, "group", "", "group address")
	fs.IntVar(&limit, "limit", DefaultPageSize, "maximum items in this page")
	fs.StringVar(&cursor, "cursor", "", "pagination cursor")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeGroupSubscribersHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(groupAddress, "--group"); err != nil {
		return nil, err
	}
	groupAddress, err := NormalizeAddress(groupAddress)
	if err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}
	if _, err := normalizePageParams(PageParams{Limit: limit, Cursor: cursor}); err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		page, err := store.ListGroupNotificationSubscribersPage(ctx, groupAddress, PageParams{Limit: limit, Cursor: cursor})
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, page)
		}
		for _, subscriber := range page.Items {
			if _, err := fmt.Fprintf(a.stdout, "subscriber_id=%s notify_address=%s person=%s active=%t created_at=%s\n", subscriber.SubscriberID, subscriber.NotifyAddress, subscriber.Person, subscriber.Active, subscriber.CreatedAt); err != nil {
				return err
			}
		}
		return writeNextCursor(a.stdout, page.NextCursor)
	}, nil
}

func (a *App) writeGroupHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost group <subcommand> [options]",
		"",
		"Subcommands:",
		"  create              Create a group address",
		"  list                List group addresses",
		"  add-member          Add a person to a group",
		"  remove-member       Remove a person from a group",
		"  members             List current and historical memberships",
		"  add-subscriber      Add a group notification subscriber",
		"  remove-subscriber   Remove a group notification subscriber",
		"  subscribers         List active group notification subscribers",
	})
}

func (a *App) writeGroupListHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost group list [--limit N] [--cursor CURSOR] [--json | --ndjson | --yaml]",
		"",
		"Options:",
		fmt.Sprintf("  --limit N          Page size (default %d, maximum %d)", DefaultPageSize, MaxPageSize),
		"  --cursor CURSOR    Continue from a prior next_cursor",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}

func (a *App) writeGroupCreateHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost group create --group ADDRESS [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --group ADDRESS     Group address",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}

func (a *App) writeGroupAddMemberHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost group add-member --group ADDRESS --person PERSON [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --group ADDRESS     Group address",
		"  --person PERSON     Person identity",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}

func (a *App) writeGroupRemoveMemberHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost group remove-member --group ADDRESS --person PERSON [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --group ADDRESS     Group address",
		"  --person PERSON     Person identity",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}

func (a *App) writeGroupMembersHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost group members --group ADDRESS [--limit N] [--cursor CURSOR] [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --group ADDRESS     Group address",
		fmt.Sprintf("  --limit N           Page size (default %d, maximum %d)", DefaultPageSize, MaxPageSize),
		"  --cursor CURSOR     Continue from a prior next_cursor",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}

func (a *App) writeGroupAddSubscriberHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost group add-subscriber --group ADDRESS --notify-address ADDRESS --person PERSON [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --group ADDRESS           Group address",
		"  --notify-address ADDRESS  Address to notify",
		"  --person PERSON           Subscriber person identity",
		"  --json                    Emit JSON",
		"  --yaml                    Emit YAML",
	})
}

func (a *App) writeGroupRemoveSubscriberHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost group remove-subscriber --group ADDRESS --notify-address ADDRESS [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --group ADDRESS           Group address",
		"  --notify-address ADDRESS  Address to remove",
		"  --json                    Emit JSON",
		"  --yaml                    Emit YAML",
	})
}

func (a *App) writeGroupSubscribersHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost group subscribers --group ADDRESS [--limit N] [--cursor CURSOR] [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --group ADDRESS     Group address",
		fmt.Sprintf("  --limit N           Page size (default %d, maximum %d)", DefaultPageSize, MaxPageSize),
		"  --cursor CURSOR     Continue from a prior next_cursor",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}
