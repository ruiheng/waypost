package waypost

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

func (a *App) prepareAckCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost ack", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var deliveryID string
	var leaseToken string
	fs.StringVar(&deliveryID, "delivery", "", "delivery id")
	fs.StringVar(&leaseToken, "lease-token", "", "lease token")

	if err := a.parseCommandFlags(fs, args, a.writeAckHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(deliveryID, "--delivery"); err != nil {
		return nil, err
	}
	if err := requireFlag(leaseToken, "--lease-token"); err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		result, err := store.Ack(ctx, deliveryID, leaseToken)
		if err != nil {
			return err
		}
		return a.writeDeliveryTransitionResultText(result)
	}, nil
}

func (a *App) prepareRenewCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost renew", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var deliveryID string
	var leaseToken string
	var extendBy time.Duration
	fs.StringVar(&deliveryID, "delivery", "", "delivery id")
	fs.StringVar(&leaseToken, "lease-token", "", "lease token")
	fs.DurationVar(&extendBy, "for", 0, "lease extension duration")

	if err := a.parseCommandFlags(fs, args, a.writeRenewHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(deliveryID, "--delivery"); err != nil {
		return nil, err
	}
	if err := requireFlag(leaseToken, "--lease-token"); err != nil {
		return nil, err
	}
	if extendBy <= 0 {
		return nil, fmt.Errorf("--for must be greater than 0")
	}

	return func(ctx context.Context, store *Store) error {
		result, err := store.Renew(ctx, deliveryID, leaseToken, extendBy)
		if err != nil {
			return err
		}
		return a.writeLeaseRenewResultText(result)
	}, nil
}

func (a *App) prepareReleaseCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost release", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var deliveryID string
	var leaseToken string
	fs.StringVar(&deliveryID, "delivery", "", "delivery id")
	fs.StringVar(&leaseToken, "lease-token", "", "lease token")

	if err := a.parseCommandFlags(fs, args, a.writeReleaseHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(deliveryID, "--delivery"); err != nil {
		return nil, err
	}
	if err := requireFlag(leaseToken, "--lease-token"); err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		result, err := store.Release(ctx, deliveryID, leaseToken)
		if err != nil {
			return err
		}
		return a.writeDeliveryTransitionResultText(result)
	}, nil
}

func (a *App) prepareDeferCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost defer", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var deliveryID string
	var leaseToken string
	var untilText string
	fs.StringVar(&deliveryID, "delivery", "", "delivery id")
	fs.StringVar(&leaseToken, "lease-token", "", "lease token")
	fs.StringVar(&untilText, "until", "", "future RFC3339 timestamp")

	if err := a.parseCommandFlags(fs, args, a.writeDeferHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(deliveryID, "--delivery"); err != nil {
		return nil, err
	}
	if err := requireFlag(leaseToken, "--lease-token"); err != nil {
		return nil, err
	}
	if strings.TrimSpace(untilText) == "" {
		return nil, fmt.Errorf("--until is required")
	}

	until, err := time.Parse(time.RFC3339Nano, untilText)
	if err != nil {
		return nil, fmt.Errorf("parse --until: %w", err)
	}

	return func(ctx context.Context, store *Store) error {
		result, err := store.Defer(ctx, deliveryID, leaseToken, until)
		if err != nil {
			return err
		}
		return a.writeDeliveryTransitionResultText(result)
	}, nil
}

func (a *App) prepareUndeferCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost undefer", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var deliveryID string
	var formats outputFlags
	fs.StringVar(&deliveryID, "delivery", "", "delivery id")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeUndeferHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(deliveryID, "--delivery"); err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		result, err := store.Undefer(ctx, deliveryID)
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, result)
		}
		return a.writeDeliveryTransitionResultText(result)
	}, nil
}

func (a *App) prepareFailCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost fail", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var deliveryID string
	var leaseToken string
	var reason string
	var formats outputFlags
	fs.StringVar(&deliveryID, "delivery", "", "delivery id")
	fs.StringVar(&leaseToken, "lease-token", "", "lease token")
	fs.StringVar(&reason, "reason", "", "failure reason")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeFailHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(deliveryID, "--delivery"); err != nil {
		return nil, err
	}
	if err := requireFlag(leaseToken, "--lease-token"); err != nil {
		return nil, err
	}
	if err := requireFlag(reason, "--reason"); err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		result, err := store.Fail(ctx, deliveryID, leaseToken, reason)
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, result)
		}
		return a.writeDeliveryTransitionResultText(result)
	}, nil
}

func (a *App) prepareDeadLetterCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost dead-letter", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var deliveryID string
	var leaseToken string
	var reason string
	var formats outputFlags
	fs.StringVar(&deliveryID, "delivery", "", "delivery id")
	fs.StringVar(&leaseToken, "lease-token", "", "lease token")
	fs.StringVar(&reason, "reason", "", "dead-letter reason")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeDeadLetterHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(deliveryID, "--delivery"); err != nil {
		return nil, err
	}
	if err := requireFlag(leaseToken, "--lease-token"); err != nil {
		return nil, err
	}
	if err := requireFlag(reason, "--reason"); err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		result, err := store.DeadLetter(ctx, deliveryID, leaseToken, reason)
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, result)
		}
		return a.writeDeliveryTransitionResultText(result)
	}, nil
}

func (a *App) writeAckHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost ack --delivery ID --lease-token TOKEN",
	})
}

func (a *App) writeRenewHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost renew --delivery ID --lease-token TOKEN --for DURATION",
	})
}

func (a *App) writeReleaseHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost release --delivery ID --lease-token TOKEN",
	})
}

func (a *App) writeDeferHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost defer --delivery ID --lease-token TOKEN --until RFC3339",
	})
}

func (a *App) writeUndeferHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost undefer --delivery ID [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}

func (a *App) writeFailHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost fail --delivery ID --lease-token TOKEN --reason TEXT [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}

func (a *App) writeDeadLetterHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost dead-letter --delivery ID --lease-token TOKEN --reason TEXT [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}
