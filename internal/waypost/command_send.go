package waypost

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func (a *App) prepareSendCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost send", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var toAddresses stringListFlag
	var fromAddress string
	var subject string
	var contentType string
	var schemaVersion string
	var bodyFile string
	var groupMode bool
	var full bool
	var notify bool
	var formats outputFlags

	fs.Var(&toAddresses, "to", "recipient address (repeatable)")
	fs.StringVar(&fromAddress, "from", "", "sender address")
	fs.StringVar(&subject, "subject", "", "message subject")
	fs.StringVar(&contentType, "content-type", "text/plain", "message content type")
	fs.StringVar(&schemaVersion, "schema-version", "v1", "sender-defined schema version")
	fs.StringVar(&bodyFile, "body-file", "", "path to message body, or - for stdin")
	fs.BoolVar(&groupMode, "group", false, "send to a known group address")
	fs.BoolVar(&full, "full", false, "emit the full payload")
	fs.BoolVar(&notify, "notify", false, "best-effort notify the recipient after sending")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeSendHelp); err != nil {
		return nil, err
	}
	if len(toAddresses) == 0 {
		return nil, requireFlag("", "--to")
	}

	if len(toAddresses) == 1 {
		return a.prepareSingleSendCommand(
			toAddresses[0],
			fromAddress,
			subject,
			contentType,
			schemaVersion,
			bodyFile,
			groupMode,
			full,
			notify,
			formats,
		)
	}

	recipients, err := NormalizeSendRecipients([]string(toAddresses), groupMode)
	if err != nil {
		return nil, err
	}
	fromAddress, err = normalizeSenderAddress(fromAddress)
	if err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}
	body, err := a.readBody(bodyFile)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, ErrEmptyBody
	}

	params := SendParams{
		FromAddress:   fromAddress,
		Subject:       subject,
		ContentType:   contentType,
		SchemaVersion: schemaVersion,
		Body:          body,
		Group:         groupMode,
	}

	return func(ctx context.Context, store *Store) error {
		var batchNotifier SendBatchNotifier
		if notify {
			batchNotifier = func(ctx context.Context, request SendNotificationRequest) SendNotificationOutcome {
				outcome := SendNotificationOutcome{
					Status: "failed",
					Err:    errors.New("send notification is not configured"),
				}
				if a.sendNotifier != nil {
					outcome = a.sendNotifier(ctx, store, request)
				}
				return outcome
			}
		}

		result, err := ExecuteSendBatch(ctx, store, params, recipients, batchNotifier)
		if err != nil {
			return err
		}
		if err := a.writeSendBatchOutput(format, full, notify, result); err != nil {
			return err
		}
		if result.FailedCount > 0 {
			return &SendBatchIncompleteError{
				FailedCount:    result.FailedCount,
				RecipientCount: len(result.ToAddresses),
			}
		}
		return nil
	}, nil
}

func (a *App) prepareSingleSendCommand(
	toAddress string,
	fromAddress string,
	subject string,
	contentType string,
	schemaVersion string,
	bodyFile string,
	groupMode bool,
	full bool,
	notify bool,
	formats outputFlags,
) (preparedCommand, error) {
	if err := requireFlag(toAddress, "--to"); err != nil {
		return nil, err
	}
	toAddress, err := NormalizeAddress(toAddress)
	if err != nil {
		return nil, err
	}
	fromAddress, err = NormalizeOptionalAddress(fromAddress)
	if err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	body, err := a.readBody(bodyFile)
	if err != nil {
		return nil, err
	}

	params := SendParams{
		ToAddress:     toAddress,
		FromAddress:   fromAddress,
		Subject:       subject,
		ContentType:   contentType,
		SchemaVersion: schemaVersion,
		Body:          body,
		Group:         groupMode,
	}

	return func(ctx context.Context, store *Store) error {
		result, err := store.Send(ctx, params)
		if err != nil {
			return err
		}
		if notify {
			// Streaming formats expose the durable receipt before the optional
			// (and potentially slow) wakeup notification completes.
			stream := format == outputFormatNDJSON || format == outputFormatYAML
			if stream {
				if err := a.writeSendOutput(format, full, result); err != nil {
					return err
				}
				if format == outputFormatYAML {
					if _, err := fmt.Fprintln(a.stdout, "---"); err != nil {
						return err
					}
				}
			}
			outcome := SendNotificationOutcome{
				Status: "failed",
				Err:    errors.New("send notification is not configured"),
			}
			if a.sendNotifier != nil {
				outcome = a.sendNotifier(ctx, store, SendNotificationRequest{
					Params: params,
					Result: result,
				})
			}
			return a.writeSendOutputWithNotification(format, full, result, outcome)
		}
		return a.writeSendOutput(format, full, result)
	}, nil
}

func (a *App) readBody(bodyFile string) ([]byte, error) {
	switch strings.TrimSpace(bodyFile) {
	case "":
		return nil, errors.New("--body-file is required")
	case "-":
		body, err := io.ReadAll(a.stdin)
		if err != nil {
			return nil, internalCLIError(fmt.Errorf("read stdin body: %w", err))
		}
		return body, nil
	default:
		body, err := os.ReadFile(bodyFile)
		if err != nil {
			return nil, internalCLIError(fmt.Errorf("read body file: %w", err))
		}
		return body, nil
	}
}

func (a *App) writeSendHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost send --to ADDRESS [--to ADDRESS ...] --body-file PATH [options] [--json | --ndjson | --yaml] [--full] [--notify]",
		"",
		"Options:",
		"  --to ADDRESS           Recipient address (repeatable)",
		"  --from ADDRESS         Sender address",
		"  --group                Send to a known group address",
		"  --subject TEXT         Message subject",
		"  --content-type TYPE    Message content type",
		"  --schema-version VER   Sender-defined schema version",
		"  --body-file PATH|-     Read body from a file or stdin",
		"  --notify               Best-effort notify the recipient after sending",
		"  --json                 Emit JSON",
		"  --ndjson               Emit newline-delimited JSON",
		"  --yaml                 Emit YAML",
		"  --full                 Emit the full payload",
	})
}
