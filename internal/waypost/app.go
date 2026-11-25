package waypost

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
)

var ErrHelpRequested = errors.New("help requested")

type App struct {
	stdin          io.Reader
	stdout         io.Writer
	stderr         io.Writer
	sendNotifier   SendNotifier
	inspectFlagSet func(*flag.FlagSet)
}

// SendNotificationRequest contains the durable send details needed by an
// optional best-effort wakeup notifier.
type SendNotificationRequest struct {
	Params SendParams
	Result SendResult
}

// SendNotificationOutcome describes the result of a post-send wakeup attempt.
// An error here is informational: the durable send has already completed and
// must remain successful.
type SendNotificationOutcome struct {
	Status string
	Scheme string
	Detail string
	Err    error
}

// SendNotifier is called only when `waypost send --notify` is requested.
type SendNotifier func(context.Context, *Store, SendNotificationRequest) SendNotificationOutcome

type AppOptions struct {
	SendNotifier SendNotifier
}

func NewApp(stdin io.Reader, stdout, stderr io.Writer) *App {
	return NewAppWithOptions(stdin, stdout, stderr, AppOptions{})
}

func NewAppWithOptions(stdin io.Reader, stdout, stderr io.Writer, options AppOptions) *App {
	return &App{
		stdin:        stdin,
		stdout:       stdout,
		stderr:       stderr,
		sendNotifier: options.SendNotifier,
	}
}

func (a *App) Run(ctx context.Context, args []string) error {
	return a.RunWithStateDir(ctx, "", args)
}

func (a *App) RunWithStateDir(ctx context.Context, stateDir string, args []string) error {
	if len(args) == 0 {
		return errors.New("expected a command: doc, send, forward, recv, wait, watch, read, show, ack, renew, release, defer, undefer, fail, dead-letter, list, stale, group, or address")
	}
	if args[0] == "doc" {
		return a.runDocCommand(args[1:])
	}

	command, err := a.prepareCommand(args)
	if err != nil {
		return err
	}

	runtime, err := OpenRuntime(ctx, stateDir)
	if err != nil {
		return err
	}
	defer runtime.Close()

	return command(ctx, runtime.Store())
}

func (a *App) prepareCommand(args []string) (preparedCommand, error) {
	if len(args) == 0 {
		return nil, invalidArgumentError(errors.New("expected a command: doc, send, forward, recv, wait, watch, read, show, ack, renew, release, defer, undefer, fail, dead-letter, list, stale, group, or address"))
	}

	switch args[0] {
	case "send":
		return classifyPreparedCommand(a.prepareSendCommand(args[1:]))
	case "forward":
		return classifyPreparedCommand(a.prepareForwardCommand(args[1:]))
	case "recv":
		return classifyPreparedCommand(a.prepareRecvCommand(args[1:]))
	case "wait":
		return classifyPreparedCommand(a.prepareWaitCommand(args[1:]))
	case "watch":
		return classifyPreparedCommand(a.prepareWatchCommand(args[1:]))
	case "read", "show":
		return classifyPreparedCommand(a.prepareReadCommand(args[1:]))
	case "ack":
		return classifyPreparedCommand(a.prepareAckCommand(args[1:]))
	case "renew":
		return classifyPreparedCommand(a.prepareRenewCommand(args[1:]))
	case "release":
		return classifyPreparedCommand(a.prepareReleaseCommand(args[1:]))
	case "defer":
		return classifyPreparedCommand(a.prepareDeferCommand(args[1:]))
	case "undefer":
		return classifyPreparedCommand(a.prepareUndeferCommand(args[1:]))
	case "fail":
		return classifyPreparedCommand(a.prepareFailCommand(args[1:]))
	case "dead-letter":
		return classifyPreparedCommand(a.prepareDeadLetterCommand(args[1:]))
	case "list":
		return classifyPreparedCommand(a.prepareListCommand(args[1:]))
	case "stale":
		return classifyPreparedCommand(a.prepareStaleCommand(args[1:]))
	case "group":
		return classifyPreparedCommand(a.prepareGroupCommand(args[1:]))
	case "address":
		return classifyPreparedCommand(a.prepareAddressCommand(args[1:]))
	default:
		return nil, invalidArgumentError(fmt.Errorf("unknown command %q", args[0]))
	}
}

func classifyPreparedCommand(command preparedCommand, err error) (preparedCommand, error) {
	if err == nil || errors.Is(err, ErrHelpRequested) {
		return command, err
	}
	if errors.Is(err, ErrInvalidArgument) || errors.Is(err, ErrInvalidState) || errors.Is(err, errInternalCLI) {
		return nil, err
	}
	return nil, invalidArgumentError(err)
}
