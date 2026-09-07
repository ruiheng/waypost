package waypost

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

func (a *App) prepareListCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var address string
	var fromAddress string
	var person string
	var formats outputFlags
	var state string
	var limit int
	var cursor string
	fs.StringVar(&address, "for", "", "recipient address")
	fs.StringVar(&fromAddress, "from", "", "sender address")
	fs.StringVar(&person, "as", "", "group reader identity")
	fs.IntVar(&limit, "limit", DefaultPageSize, "maximum items in this page")
	fs.StringVar(&cursor, "cursor", "", "pagination cursor")
	formats.register(fs, "emit JSON", "emit YAML")
	fs.StringVar(&state, "state", "", "filter by delivery state")

	if err := a.parseCommandFlags(fs, args, a.writeListHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(address, "--for"); err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}
	state, err = normalizeDeliveryStateFilter(state)
	if err != nil {
		return nil, err
	}

	params := ListParams{
		Address:     address,
		FromAddress: fromAddress,
		State:       state,
		Limit:       limit,
		Cursor:      cursor,
	}
	if _, err := normalizePageParams(PageParams{Limit: limit, Cursor: cursor}); err != nil {
		return nil, err
	}
	person = strings.TrimSpace(person)
	if person != "" && state != "" {
		return nil, errors.New("--state is not supported with --as")
	}

	return func(ctx context.Context, store *Store) error {
		if person != "" {
			page, err := store.ListGroupMessagesPage(ctx, GroupListParams{
				Address:     address,
				Person:      person,
				FromAddress: fromAddress,
				Limit:       limit,
				Cursor:      cursor,
			})
			if err != nil {
				return err
			}
			if format != outputFormatText {
				summaries := make([]GroupListedMessageCompact, 0, len(page.Items))
				for _, message := range page.Items {
					summaries = append(summaries, CompactGroupListedMessage(message))
				}
				return a.writeStructuredOutput(format, Page[GroupListedMessageCompact]{Items: summaries, NextCursor: page.NextCursor})
			}
			for _, message := range page.Items {
				if err := a.writeGroupListedMessageText(message); err != nil {
					return err
				}
			}
			return writeNextCursor(a.stdout, page.NextCursor)
		}

		page, err := store.ListPage(ctx, params)
		if err != nil {
			return err
		}

		if format != outputFormatText {
			return a.writeStructuredOutput(format, page)
		}

		for _, delivery := range page.Items {
			sender := ""
			if delivery.SenderAddress != nil {
				sender = " sender_address=" + *delivery.SenderAddress
			}
			fmt.Fprintf(a.stdout, "%s %s %s %s%s\n", delivery.DeliveryID, delivery.State, delivery.VisibleAt, delivery.Subject, sender)
		}
		return writeNextCursor(a.stdout, page.NextCursor)
	}, nil
}

func (a *App) prepareStaleCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost stale", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var addresses stringListFlag
	var person string
	var olderThan time.Duration
	var formats outputFlags

	fs.Var(&addresses, "for", "recipient address (repeatable)")
	fs.StringVar(&person, "as", "", "group reader identity")
	fs.DurationVar(&olderThan, "older-than", 0, "staleness threshold")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeStaleHelp); err != nil {
		return nil, err
	}
	normalizedAddresses, err := normalizeAddresses("", []string(addresses), "--for")
	if err != nil {
		return nil, err
	}
	if olderThan <= 0 {
		return nil, errors.New("--older-than must be greater than 0")
	}
	person = strings.TrimSpace(person)
	if person != "" && len(normalizedAddresses) != 1 {
		return nil, errors.New("--as requires exactly one --for address")
	}
	format, err := formats.resolveStructured()
	if err != nil {
		return nil, err
	}

	params := StaleAddressesParams{
		OlderThan: olderThan,
	}
	if person != "" {
		params.GroupViews = []GroupStaleView{{
			Address: normalizedAddresses[0],
			Person:  person,
		}}
	} else {
		params.Addresses = normalizedAddresses
	}

	return func(ctx context.Context, store *Store) error {
		stale, err := store.ListStaleAddresses(ctx, params)
		if err != nil {
			return err
		}
		return a.writeStructuredOutput(format, stale)
	}, nil
}

func (a *App) prepareRecvCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost recv", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var addresses stringListFlag
	var person string
	var maxMessages int
	var full bool
	var formats outputFlags

	fs.Var(&addresses, "for", "recipient address (repeatable)")
	fs.StringVar(&person, "as", "", "group reader identity")
	fs.IntVar(&maxMessages, "max", 1, "maximum number of deliveries to claim")
	fs.BoolVar(&full, "full", false, "emit the full payload")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeRecvHelp); err != nil {
		return nil, err
	}
	normalizedAddresses, err := normalizeAddresses("", []string(addresses), "--for")
	if err != nil {
		return nil, err
	}
	if maxMessages < 1 || maxMessages > maxReceiveBatchSize {
		return nil, fmt.Errorf("--max must be between 1 and %d", maxReceiveBatchSize)
	}
	maxProvided := flagWasProvided(fs, "max")
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}
	person = strings.TrimSpace(person)

	params := ReceiveBatchParams{
		Addresses: normalizedAddresses,
		Max:       maxMessages,
	}

	return func(ctx context.Context, store *Store) error {
		if person != "" {
			if maxProvided {
				return errors.New("--max is not supported with --as")
			}
			if len(normalizedAddresses) != 1 {
				return errors.New("--as requires exactly one --for address")
			}

			message, err := store.ReceiveGroupMessage(ctx, GroupReceiveParams{
				Address: normalizedAddresses[0],
				Person:  person,
			})
			if errors.Is(err, ErrNoMessage) {
				if format != outputFormatText {
					if writeErr := a.writeStructuredOutput(format, groupReceiveOutput{
						Status:    "no_message",
						Addresses: []string{normalizedAddresses[0]},
						AsPerson:  person,
					}); writeErr != nil {
						return writeErr
					}
				} else if _, writeErr := fmt.Fprintln(a.stdout, "status=no_message"); writeErr != nil {
					return writeErr
				}
				return ErrNoMessage
			}
			if err != nil {
				return err
			}
			if format != outputFormatText && !full {
				compact := CompactGroupReceivedMessage(message)
				return a.writeStructuredOutput(format, groupReceiveOutput{
					Status:    "received",
					Addresses: []string{normalizedAddresses[0]},
					AsPerson:  person,
					Message:   &compact,
				})
			}
			return a.writeGroupReceiveOutput(format, full, message)
		}

		claimCtx := WithClaimMetadata(ctx, ClaimMetadata{
			Source:         "cli",
			Tool:           "waypost recv",
			BoundAddresses: normalizedAddresses,
		})
		result, err := store.ReceiveBatch(claimCtx, params)
		if errors.Is(err, ErrNoMessage) {
			if format != outputFormatText {
				if writeErr := a.writeStructuredOutput(format, personalReceiveOutput{
					Status:           "no_message",
					Addresses:        normalizedAddresses,
					RemainingByState: result.RemainingByState,
				}); writeErr != nil {
					return writeErr
				}
			} else if _, writeErr := fmt.Fprintln(a.stdout, "status=no_message"); writeErr != nil {
				return writeErr
			}
			return ErrNoMessage
		}
		if err != nil {
			return err
		}
		if !maxProvided {
			if len(result.Messages) != 1 {
				return errors.New("receive returned an unexpected delivery count")
			}
			if format != outputFormatText && !full {
				compact := CompactReceivedMessage(result.Messages[0])
				return a.writeStructuredOutput(format, personalReceiveOutput{
					Status:           "received",
					Addresses:        normalizedAddresses,
					Delivery:         &compact,
					RemainingByState: result.RemainingByState,
				})
			}
			return a.writeReceiveOutput(format, full, result.Messages[0])
		}
		if format != outputFormatText && !full {
			deliveries := make([]ReceivedMessageCompact, 0, len(result.Messages))
			for _, message := range result.Messages {
				deliveries = append(deliveries, CompactReceivedMessage(message))
			}
			return a.writeStructuredOutput(format, personalReceiveOutput{
				Status:           "received",
				Addresses:        normalizedAddresses,
				Deliveries:       deliveries,
				RemainingByState: result.RemainingByState,
			})
		}
		return a.writeReceiveBatchOutput(format, full, result)
	}, nil
}

func (a *App) prepareWatchCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost watch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var addresses stringListFlag
	var timeout time.Duration
	var formats outputFlags
	var state string

	fs.Var(&addresses, "for", "recipient address (repeatable)")
	fs.DurationVar(&timeout, "timeout", 0, "maximum idle time before watch exits")
	formats.register(fs, "emit NDJSON", "emit a YAML document stream")
	fs.StringVar(&state, "state", "", "filter by delivery state")

	if err := a.parseCommandFlags(fs, args, a.writeWatchHelp); err != nil {
		return nil, err
	}
	normalizedAddresses, err := normalizeAddresses("", []string(addresses), "--for")
	if err != nil {
		return nil, err
	}
	if timeout < 0 {
		return nil, errors.New("--timeout must be greater than or equal to 0")
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	params := WatchParams{
		Addresses: normalizedAddresses,
		State:     state,
		Timeout:   timeout,
	}

	return func(ctx context.Context, store *Store) error {
		emit, err := a.newWatchEmitter(format)
		if err != nil {
			return err
		}

		return store.Watch(ctx, params, func(delivery ListedDelivery) error {
			return emit(delivery)
		})
	}, nil
}

func (a *App) prepareWaitCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost wait", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var addresses stringListFlag
	var person string
	var timeout time.Duration
	var full bool
	var formats outputFlags

	fs.Var(&addresses, "for", "recipient address (repeatable)")
	fs.StringVar(&person, "as", "", "group reader identity")
	fs.DurationVar(&timeout, "timeout", 0, "maximum time to wait for a matching delivery")
	fs.BoolVar(&full, "full", false, "emit the full payload")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeWaitHelp); err != nil {
		return nil, err
	}
	normalizedAddresses, err := normalizeAddresses("", []string(addresses), "--for")
	if err != nil {
		return nil, err
	}
	if timeout < 0 {
		return nil, errors.New("--timeout must be greater than or equal to 0")
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}
	person = strings.TrimSpace(person)
	if person != "" && len(normalizedAddresses) != 1 {
		return nil, errors.New("--as requires exactly one --for address")
	}

	params := WaitParams{
		Addresses: normalizedAddresses,
		Timeout:   timeout,
	}

	return func(ctx context.Context, store *Store) error {
		if person != "" {
			message, err := store.WaitGroupMessage(ctx, GroupWaitParams{
				Address: normalizedAddresses[0],
				Person:  person,
				Timeout: timeout,
			})
			if err != nil {
				return err
			}
			return a.writeGroupWaitOutput(format, full, message)
		}

		delivery, err := store.Wait(ctx, params)
		if err != nil {
			return err
		}
		return a.writeWaitOutput(format, full, delivery)
	}, nil
}

func (a *App) prepareReadCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost read", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var deliveryIDs stringListFlag
	var messageIDs stringListFlag
	var addresses stringListFlag
	var fromAddress string
	var latest bool
	var state string
	var limit int
	var cursor string
	var formats outputFlags
	fs.Var(&deliveryIDs, "delivery", "delivery id (repeatable)")
	fs.Var(&messageIDs, "message", "message id (repeatable)")
	fs.Var(&addresses, "for", "recipient address (repeatable)")
	fs.StringVar(&fromAddress, "from", "", "sender address for --latest")
	fs.BoolVar(&latest, "latest", false, "read the latest deliveries for one or more queues")
	fs.StringVar(&state, "state", "", "delivery state for --latest (defaults to acked)")
	fs.IntVar(&limit, "limit", 1, "maximum number of latest deliveries to read")
	fs.StringVar(&cursor, "cursor", "", "pagination cursor for --latest")
	formats.register(fs, "emit JSON", "emit YAML")

	flagArgs, directIDs := splitReadCommandArgs(fs, args)
	if err := a.parseCommandFlags(fs, flagArgs, a.writeReadHelp); err != nil {
		return nil, err
	}
	normalizedDeliveryIDs, err := normalizeFlagValues([]string(deliveryIDs), "--delivery")
	if err != nil && !errors.Is(err, errFlagValueRequired) {
		return nil, err
	}
	normalizedMessageIDs, err := normalizeFlagValues([]string(messageIDs), "--message")
	if err != nil && !errors.Is(err, errFlagValueRequired) {
		return nil, err
	}
	normalizedAddresses, err := normalizeFlagValues([]string(addresses), "--for")
	if err != nil && !errors.Is(err, errFlagValueRequired) {
		return nil, err
	}
	directDeliveryIDs, directMessageIDs, err := normalizeDirectReadIDs(directIDs)
	if err != nil {
		return nil, err
	}
	hasDirectIDs := len(directDeliveryIDs) > 0 || len(directMessageIDs) > 0
	if hasDirectIDs && (len(normalizedDeliveryIDs) > 0 || len(normalizedMessageIDs) > 0 || latest) {
		return nil, errors.New("ID, --delivery, --message, and --latest are mutually exclusive")
	}
	if len(directDeliveryIDs) > 0 {
		normalizedDeliveryIDs = directDeliveryIDs
	}
	if len(directMessageIDs) > 0 {
		normalizedMessageIDs = directMessageIDs
	}
	selectorCount := 0
	if len(normalizedDeliveryIDs) > 0 {
		selectorCount++
	}
	if len(normalizedMessageIDs) > 0 {
		selectorCount++
	}
	if latest {
		selectorCount++
	}
	switch {
	case selectorCount == 0:
		return nil, errors.New("one of ID, --delivery, --message, or --latest is required")
	case selectorCount > 1:
		return nil, errors.New("--delivery, --message, and --latest are mutually exclusive")
	case !latest && len(normalizedAddresses) > 0:
		return nil, errors.New("--for requires --latest")
	case !latest && strings.TrimSpace(fromAddress) != "":
		return nil, errors.New("--from requires --latest")
	case !latest && strings.TrimSpace(state) != "":
		return nil, errors.New("--state requires --latest")
	case !latest && flagWasProvided(fs, "limit"):
		return nil, errors.New("--limit requires --latest")
	case !latest && strings.TrimSpace(cursor) != "":
		return nil, errors.New("--cursor requires --latest")
	case latest && len(normalizedAddresses) == 0:
		return nil, errors.New("--latest requires at least one --for address")
	case latest:
		if _, err := normalizePageParams(PageParams{Limit: limit, Cursor: cursor}); err != nil {
			return nil, err
		}
	}
	if err := validateInputItemCount("read ids", len(normalizedDeliveryIDs)+len(normalizedMessageIDs)); err != nil {
		return nil, err
	}
	if err := validateInputItemCount("--for", len(normalizedAddresses)); err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}
	state = strings.TrimSpace(state)

	return func(ctx context.Context, store *Store) error {
		if len(normalizedMessageIDs) > 0 {
			messages, err := store.ReadMessages(ctx, normalizedMessageIDs)
			if err != nil {
				return err
			}
			result := readMessageResult{Items: messages}
			if format != outputFormatText {
				return a.writeStructuredOutput(format, result)
			}
			return a.writeReadMessageResultText(result)
		}

		if latest {
			page, err := store.ReadLatestDeliveriesPage(ctx, ReadLatestParams{
				Addresses:   normalizedAddresses,
				FromAddress: fromAddress,
				State:       state,
				Limit:       limit,
				Cursor:      cursor,
			})
			if err != nil {
				return err
			}
			result := readDeliveryResult{
				Items:      page.Items,
				HasMore:    page.NextCursor != "",
				NextCursor: page.NextCursor,
			}
			if format != outputFormatText {
				return a.writeStructuredOutput(format, result)
			}
			return a.writeReadDeliveryResultText(result)
		}

		deliveries, err := store.ReadDeliveries(ctx, normalizedDeliveryIDs)
		if err != nil {
			return err
		}
		result := readDeliveryResult{Items: deliveries}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, result)
		}
		return a.writeReadDeliveryResultText(result)
	}, nil
}

// splitReadCommandArgs keeps read's positional IDs separate while preserving
// interspersed flags. The standard flag package stops parsing at the first
// positional argument, but `waypost read dlv_123 --json` should work naturally.
func splitReadCommandArgs(fs *flag.FlagSet, args []string) (flagArgs, directIDs []string) {
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if arg == "--" {
			directIDs = append(directIDs, args[index+1:]...)
			break
		}
		if len(arg) > 1 && arg[0] == '-' {
			flagArgs = append(flagArgs, arg)
			if readFlagTakesValue(fs, arg) && index+1 < len(args) {
				index++
				flagArgs = append(flagArgs, args[index])
			}
			continue
		}
		directIDs = append(directIDs, arg)
	}
	return flagArgs, directIDs
}

func readFlagTakesValue(fs *flag.FlagSet, arg string) bool {
	name, _, hasInlineValue := strings.Cut(arg, "=")
	if hasInlineValue {
		return false
	}
	if strings.HasPrefix(name, "--") {
		name = name[2:]
	} else if strings.HasPrefix(name, "-") {
		name = name[1:]
	}
	definition := fs.Lookup(name)
	if definition == nil {
		return false
	}
	if boolean, ok := definition.Value.(interface{ IsBoolFlag() bool }); ok && boolean.IsBoolFlag() {
		return false
	}
	return true
}

func normalizeDirectReadIDs(values []string) ([]string, []string, error) {
	if len(values) == 0 {
		return nil, nil, nil
	}

	normalized, err := normalizeFlagValues(values, "ID")
	if err != nil {
		return nil, nil, err
	}

	deliveryIDs := make([]string, 0, len(normalized))
	messageIDs := make([]string, 0, len(normalized))
	for _, id := range normalized {
		if strings.HasPrefix(id, "dlv_") {
			deliveryIDs = append(deliveryIDs, id)
			continue
		}
		messageIDs = append(messageIDs, id)
	}
	if len(deliveryIDs) > 0 && len(messageIDs) > 0 {
		return nil, nil, errors.New("direct read IDs must all be delivery IDs or all be message IDs")
	}
	return deliveryIDs, messageIDs, nil
}

func (a *App) writeListHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost list --for ADDRESS [--from ADDRESS] [--state STATE] [--limit N] [--cursor CURSOR] [--json | --ndjson | --yaml]",
		"  waypost list --for GROUP_ADDRESS --as PERSON [--from ADDRESS] [--limit N] [--cursor CURSOR] [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --for ADDRESS      Recipient address",
		"  --from ADDRESS     Filter by sender address",
		"  --as PERSON        Group reader identity",
		"  --state STATE      Filter by delivery state (queued, leased/claimed, acked, dead_letter)",
		fmt.Sprintf("  --limit N          Page size (default %d, maximum %d)", DefaultPageSize, MaxPageSize),
		"  --cursor CURSOR    Continue from a prior next_cursor",
		"  --json             Emit JSON",
		"  --yaml             Emit YAML",
	})
}

func (a *App) writeStaleHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost stale --for ADDRESS [--for ADDRESS ...] --older-than DURATION [--json | --ndjson | --yaml]",
		"  waypost stale --for GROUP_ADDRESS --as PERSON --older-than DURATION [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --for ADDRESS        Recipient address (repeatable)",
		"  --as PERSON          Group reader identity",
		"  --older-than DUR     Minimum receivable age before an queue is stale",
		"  --json               Emit JSON",
		"  --yaml               Emit YAML",
	})
}

func (a *App) writeRecvHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost recv --for ADDRESS [--for ADDRESS ...] [--max COUNT] [--json | --ndjson | --yaml] [--full]",
		"  waypost recv --for GROUP_ADDRESS --as PERSON [--json | --ndjson | --yaml] [--full]",
		"",
		"Important:",
		"  Use this CLI command only after confirming that the MCP waypost_recv tool is unavailable.",
		"",
		"Options:",
		"  --for ADDRESS        Recipient address (repeatable)",
		"  --as PERSON          Group reader identity",
		"  --max COUNT          Maximum number of deliveries to claim (up to 10)",
		"  --json               Emit JSON",
		"  --yaml               Emit YAML",
		"  --full               Emit the full payload",
	})
}

func (a *App) writeReadHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost read ID [ID ...] [--json | --ndjson | --yaml]",
		"  waypost read --message ID [--message ID ...] [--json | --ndjson | --yaml]",
		"  waypost read --delivery ID [--delivery ID ...] [--json | --ndjson | --yaml]",
		"  waypost read --latest --for ADDRESS [--for ADDRESS ...] [--from ADDRESS] [--state STATE] [--limit N] [--cursor CURSOR] [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  ID                  Read by id; dlv_ ids are deliveries, all others are messages (repeatable)",
		"  --message ID        Message id to read (repeatable)",
		"  --delivery ID       Delivery id to read (repeatable)",
		"  --latest            Read the latest deliveries for one or more queues",
		"  --for ADDRESS       Recipient address for --latest (repeatable)",
		"  --from ADDRESS      Filter --latest by sender address",
		"  --state STATE       Optional delivery state filter for --latest (defaults to any)",
		fmt.Sprintf("  --limit N           Page size (default 1, maximum %d)", MaxPageSize),
		"  --cursor CURSOR     Continue from a prior next_cursor",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}

func (a *App) writeWatchHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost watch --for ADDRESS [--for ADDRESS ...] [--state STATE] [--timeout DURATION] [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --for ADDRESS        Recipient address (repeatable)",
		"  --state STATE        Filter by delivery state",
		"  --timeout DURATION   Maximum idle time before watch exits (for example 30s, 5m, 120ms, 1m30s)",
		"  --json               Emit NDJSON",
		"  --yaml               Emit a YAML document stream",
	})
}

func (a *App) writeWaitHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost wait --for ADDRESS [--for ADDRESS ...] [--timeout DURATION] [--json | --ndjson | --yaml] [--full]",
		"  waypost wait --for GROUP_ADDRESS --as PERSON [--timeout DURATION] [--json | --ndjson | --yaml] [--full]",
		"",
		"Options:",
		"  --for ADDRESS        Recipient address (repeatable)",
		"  --as PERSON          Group reader identity",
		"  --timeout DURATION   Maximum time to wait for a matching delivery (for example 30s, 5m, 120ms, 1m30s)",
		"  --json               Emit JSON",
		"  --yaml               Emit YAML",
		"  --full               Emit the full payload",
	})
}
