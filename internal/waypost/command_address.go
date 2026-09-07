package waypost

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
)

func (a *App) prepareAddressCommand(args []string) (preparedCommand, error) {
	if len(args) == 0 {
		return nil, errors.New("expected an address subcommand: inspect")
	}
	if isHelpArg(args[0]) {
		a.writeAddressHelp()
		return nil, ErrHelpRequested
	}

	switch args[0] {
	case "inspect":
		return a.prepareAddressInspectCommand(args[1:])
	default:
		return nil, fmt.Errorf("unknown address subcommand %q", args[0])
	}
}

func (a *App) prepareAddressInspectCommand(args []string) (preparedCommand, error) {
	fs := flag.NewFlagSet("waypost address inspect", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var address string
	var formats outputFlags
	fs.StringVar(&address, "address", "", "address to inspect")
	formats.register(fs, "emit JSON", "emit YAML")

	if err := a.parseCommandFlags(fs, args, a.writeAddressInspectHelp); err != nil {
		return nil, err
	}
	if err := requireFlag(address, "--address"); err != nil {
		return nil, err
	}
	address, err := NormalizeAddress(address)
	if err != nil {
		return nil, err
	}
	format, err := formats.resolve()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, store *Store) error {
		inspection, err := store.InspectAddress(ctx, address)
		if err != nil {
			return err
		}
		if format != outputFormatText {
			return a.writeStructuredOutput(format, inspection)
		}
		switch inspection.Kind {
		case AddressKindEndpoint:
			_, err = fmt.Fprintf(a.stdout, "address=%s kind=%s endpoint_id=%s\n", inspection.Address, inspection.Kind, valueOrEmpty(inspection.EndpointID))
		case AddressKindGroup:
			_, err = fmt.Fprintf(a.stdout, "address=%s kind=%s group_id=%s\n", inspection.Address, inspection.Kind, valueOrEmpty(inspection.GroupID))
		default:
			_, err = fmt.Fprintf(a.stdout, "address=%s kind=%s\n", inspection.Address, inspection.Kind)
		}
		return err
	}, nil
}

func (a *App) writeAddressHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost address <subcommand> [options]",
		"",
		"Subcommands:",
		"  inspect             Inspect endpoint/group binding for one address",
	})
}

func (a *App) writeAddressInspectHelp() {
	writeHelp(a.stdout, []string{
		"Usage:",
		"  waypost address inspect --address ADDRESS [--json | --ndjson | --yaml]",
		"",
		"Options:",
		"  --address ADDRESS   Address to inspect",
		"  --json              Emit JSON",
		"  --yaml              Emit YAML",
	})
}
