package waypost

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
)

func TestCLIRequestsJSONSkipsValueFlagsAndSeparator(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		args []string
		want bool
	}{
		{
			name: "stale with json",
			args: []string{"stale", "--for", "agent/a", "--older-than", "5m", "--json"},
			want: true,
		},
		{
			name: "reason value named json does not trigger",
			args: []string{"fail", "--delivery", "dlv_1", "--lease-token", "tok_1", "--reason", "--json"},
			want: false,
		},
		{
			name: "forward boolean --group before --json triggers",
			args: []string{"forward", "--message", "msg_1", "--to", "group/missing", "--group", "--json"},
			want: true,
		},
		{
			name: "group add-member value --group --json does not trigger",
			args: []string{"group", "add-member", "--group", "--json", "--person", "alice"},
			want: false,
		},
		{
			name: "fail lease-token value --json does not trigger",
			args: []string{"fail", "--delivery", "dlv_1", "--lease-token", "--json", "--reason", "x"},
			want: false,
		},
		{
			name: "address inspect --address --json does not trigger",
			args: []string{"address", "inspect", "--address", "--json"},
			want: false,
		},
		{
			name: "single-dash -json triggers",
			args: []string{"list", "--for", "agent/a", "-json"},
			want: true,
		},
		{
			name: "last-wins --json --json=false disables json",
			args: []string{"list", "--for", "agent/a", "--json", "--json=false"},
			want: false,
		},
		{
			name: "read interspersed positional id with --json triggers",
			args: []string{"read", "dlv_1", "--json"},
			want: true,
		},
		{
			name: "double-dash after global flags preserves command json",
			args: []string{"--", "list", "--for", "agent/a", "--json"},
			want: true,
		},
		{
			name: "double-dash after --state-dir preserves stale json",
			args: []string{"--state-dir", "/tmp/x", "--", "stale", "--json"},
			want: true,
		},
		{
			name: "bare dash stops global flags and does not misparse next token",
			args: []string{"-", "list", "--json"},
			want: false,
		},
		{
			name: "rootcmd command migrate does not trigger",
			args: []string{"migrate", "--from", "--json"},
			want: false,
		},
		{
			name: "ack without json flag does not trigger",
			args: []string{"ack", "--delivery", "dlv_1", "--lease-token", "tok_1", "--json"},
			want: false,
		},
		{
			name: "args after double-dash do not trigger",
			args: []string{"read", "--", "--json"},
			want: false,
		},
		{
			name: "group missing subcommand with --json triggers",
			args: []string{"group", "--json"},
			want: true,
		},
		{
			name: "group unknown subcommand with --json triggers",
			args: []string{"group", "bogus", "--json"},
			want: true,
		},
		{
			name: "group unknown subcommand with --json=false does not trigger",
			args: []string{"group", "bogus", "--json", "--json=false"},
			want: false,
		},
		{
			name: "group web --json emits JSON error contract",
			args: []string{"group", "web", "--json"},
			want: true,
		},
		{
			name: "group web --bogus --json agrees with WriteCLIJSONError",
			args: []string{"group", "web", "--bogus", "--json"},
			want: true,
		},
		{
			name: "group web --bogus --group --json treats --json as --group value",
			args: []string{"group", "web", "--bogus", "--group", "--json"},
			want: false,
		},
		{
			name: "group web --bogus value --json skips unknown flag value",
			args: []string{"group", "web", "--bogus", "value", "--json"},
			want: true,
		},
		{
			name: "global --bogus group web --json emits JSON error contract",
			args: []string{"--bogus", "group", "web", "--json"},
			want: true,
		},
		{
			name: "global --bogus group web --group --json emits plain text",
			args: []string{"--bogus", "group", "web", "--group", "--json"},
			want: false,
		},
		{
			name: "global unknown flag with value before group web --json skips value",
			args: []string{"--unknown-flag", "value", "group", "web", "--json"},
			want: true,
		},
		{
			name: "typoed global --statedir /tmp/x list --json still reaches list",
			args: []string{"--statedir", "/tmp/x", "list", "--json"},
			want: true,
		},
		{
			name: "unknown subcommand flag with space value before --json reaches --json",
			args: []string{"list", "--bogus", "value", "--json"},
			want: true,
		},
		{
			name: "group unknown subcommand with --group value and --json triggers",
			args: []string{"group", "bogus", "--group", "group/team", "--json"},
			want: true,
		},
		{
			name: "address unknown subcommand with --address value and --json triggers",
			args: []string{"address", "bogus", "--address", "agent/a", "--json"},
			want: true,
		},
		{
			name: "address missing subcommand with --json triggers",
			args: []string{"address", "--json"},
			want: true,
		},
		{
			name: "address unknown subcommand with --json triggers",
			args: []string{"address", "bogus", "--json"},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := CLIRequestsJSON(tc.args); got != tc.want {
				t.Fatalf("CLIRequestsJSON(%v) = %t, want %t", tc.args, got, tc.want)
			}
		})
	}
}

func TestWriteCLIJSONErrorCoversStaleCommand(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	written := WriteCLIJSONError(&buf, []string{"--state-dir", "/tmp/x", "stale", "--json"}, invalidArgumentError(errors.New("bad --older-than")))
	if !written {
		t.Fatal("WriteCLIJSONError(stale --json) = false, want true")
	}

	var doc cliErrorDocument
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("json.Unmarshal(error doc) error = %v", err)
	}
	if doc.Status != "error" || doc.ErrorCode != "invalid_argument" {
		t.Fatalf("error doc = %+v, want invalid_argument error", doc)
	}

	buf.Reset()
	if WriteCLIJSONError(&buf, []string{"fail", "--delivery", "dlv_1", "--reason", "--json"}, errors.New("boom")) {
		t.Fatalf("WriteCLIJSONError(--reason --json) unexpectedly emitted %q", buf.String())
	}
}
