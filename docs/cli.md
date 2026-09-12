# Waypost CLI

`waypost` is a local handoff CLI for one Unix user on one machine.

This guide is intentionally short. It covers what a user needs to run the CLI:

- where state lives
- the normal send/receive flow
- how immediate receive, observe-only wait, observe-only watch, and stale-address inspection work
- what each command is for

## State Directory

The waypost keeps all local state in one directory.

Resolution order:

- `$WAYPOST_STATE_DIR`
- `$XDG_STATE_HOME/ai-agent/waypost`
- `~/.local/state/ai-agent/waypost`

For demos or tests, use an isolated directory:

```bash
export WAYPOST_STATE_DIR=/tmp/waypost-demo
```

You can also override it per command:

```bash
waypost --state-dir /tmp/waypost-demo list --for workflow/reviewer/task-123
```

## Version

Report the CLI version:

```bash
waypost --version
```

The CLI version is the same value advertised by the built-in MCP server during
initialization. For the optional diagnostic `server_version` status field, call
`waypost_status` with `include_diagnostics: true`.

## MCP Server Integrations

Install or refresh the built-in Waypost MCP server in Codex's global
configuration and, when present, Claude Code, agy, and Devin configuration:

```bash
waypost install mcp-server
```

The installer uses the stable Waypost executable and preserves unrelated Codex
settings. It invokes `codex mcp add` only when the global entry is missing;
existing entries are updated in place so per-tool settings remain intact. Codex
is configured with `required = true`, the session environment passthrough
(`TMUX`, `AGENTDECK_INSTANCE_ID`, `CODEX_THREAD_ID`, `CODEX_SESSION_ID`,
`WAYPOST_STATE_DIR`, and `XDG_STATE_HOME`), and the 660-second tool timeout
needed by long Waypost waits.

If Claude Code is installed (or its `~/.claude.json` exists), the same command
also ensures its user-scoped stdio server and 660-second timeout, honoring
`$CLAUDE_CONFIG_DIR` and preserving an existing server `env` map. Claude Code
does not expose a required/enabled MCP-server switch, so no unsupported field
is written. If agy is installed (or its global MCP config exists), the command
ensures the server is enabled; agy's `disabled` flag is removed, matching
`agy mcp enable waypost`. If Devin is installed (or its
`$XDG_CONFIG_HOME/devin/mcp_config.json`, or `~/.config/devin/mcp_config.json`
when `XDG_CONFIG_HOME` is unset, exists), the command ensures the user-level
`mcpServers.waypost` entry with `command`, `args`, and `transport: "stdio"`.

The Codex target is `$CODEX_HOME/config.toml` (or `~/.codex/config.toml` when
`CODEX_HOME` is unset). Every agent is configured only when detected: the
agent CLI on `PATH` or an existing config file counts as detection, and the
command fails when no supported agent (Codex, Claude Code, agy, or Devin) is
found. For Devin, an existing user-level `config.json` also counts as
detection.

Devin resolves MCP servers project-first: `.devin/mcp_config.local.json` and
`.devin/mcp_config.json` in the session's working directory override the
user-level entry. The installer only manages the user-level file and warns
when a same-named `waypost` server exists in the current directory's
project-level config; update or remove that entry if it should use the
installed configuration. Per-tool `disabledTools` entries are preserved —
the Devin hook probes `devin mcp get waypost` and falls back to the CLI for
tools the user disabled.

## Codex Hooks

Install a Codex `SessionStart` hook that runs after compaction:

```bash
waypost install codex-hook
```

The installer merges five idempotent handlers into `$CODEX_HOME/hooks.json` (or
`~/.codex/hooks.json` when `CODEX_HOME` is unset) and preserves unrelated hooks:

- a `UserPromptSubmit` handler records the exact Waypost nudge as pending,
  clears prior nudge state for ordinary user prompts, and runs
  `codex mcp get waypost --json` from the session working directory; it injects
  one explicit receive instruction: when Waypost is enabled, it tells the agent
  that the `waypost_recv` MCP tool is available and to use it instead of the CLI
  for that pending receive; when Waypost is unavailable, it instructs use of
  `waypost recv --json`. If the probe fails, it tells the agent to look for
  `waypost_recv`, fall back to the CLI only when the tool is unavailable, and
  surfaces the probe error as a Codex UI or event-stream warning
- a `PreToolUse` `Bash` handler recognizes direct Waypost CLI invocations. A
  `waypost wait` call receives a model-visible warning not to poll, to continue
  other work, or to stop completely when no work remains; it is not blocked.
  When the MCP probe reports Waypost enabled, `waypost status` is denied in
  favor of `waypost_status`, while the maintained `recv`, `receive`, and `send`
  blacklist is denied in favor of `waypost_recv` or `waypost_send`. An
  unavailable MCP probe leaves those CLI commands untouched; a failed probe
  also leaves them untouched and surfaces the error as a warning. The
  `waypost mcp` command is always denied because the MCP server is managed by
  Codex, regardless of probe availability.
- a `PostToolUse` handler observes successful MCP or direct CLI receives and
  changes a pending nudge to consumed; `received` and `no_message` are terminal
  receive results, while active-lease and recovery-required results stay pending.
  CLI JSON and YAML output are recognized by their `status` field or, for
  `--full`, their complete personal, batch, or group receive fields; normal
  text output is recognized by the receive header fields or `status=no_message`
- a `SessionStart` `compact` handler emits the anti-repeat receive guard only
  while the current session's latest nudge is consumed; pending nudges and
  sessions without a nudge receive no compact-time context
- a `SessionEnd` handler removes the session's nudge state

The small session-scoped state lives under `$CODEX_HOME/waypost-hook-state/`
(or `~/.codex/waypost-hook-state/`). The hook does not read the Codex transcript.

Codex requires new or changed non-managed hooks to be reviewed and trusted
before they run. After installation, open `/hooks` in Codex and trust the five
Waypost handlers. Waypost does not modify Codex's private hook-trust state.

Verify the installation:

```bash
waypost doctor codex-hook
```

The doctor verifies both handler definitions and reports whether
`codex mcp get waypost --json` sees Waypost for a new Codex process started in the
current directory. This includes trusted project configuration. The MCP result
is diagnostic only: an already-running session, profile, or `-c` override may
differ. Codex does not expose hook trust through a documented noninteractive
interface, so the doctor directs you to `/hooks` instead of claiming that
configured hooks are trusted or active.

Codex invokes the machine-facing entry point automatically:

```bash
waypost codex-hook
```

It reads the Codex hook event from stdin and emits the matching
`hookSpecificOutput` JSON contract. It does not read or modify Waypost message
state.

## Devin Hooks

Install the Devin lifecycle hooks:

```bash
waypost install devin-hook
```

The installer merges six idempotent handlers into the `hooks` object of the
user-level Devin config (`$XDG_CONFIG_HOME/devin/config.json`, or
`~/.config/devin/config.json` when `XDG_CONFIG_HOME` is unset) and preserves
unrelated hooks and settings:

- a `UserPromptSubmit` handler records the exact Waypost nudge as pending,
  clears prior nudge state for ordinary user prompts, and runs
  `devin mcp get waypost` from the session working directory; it injects one
  explicit receive instruction: `waypost_recv` when the MCP server is
  configured, `waypost recv --json` otherwise. If the probe fails, the
  instruction and the probe error are emitted as additional context
- a `PreToolUse` `exec` handler recognizes direct Waypost CLI invocations. A
  `waypost wait` call receives a model-visible warning not to poll; it is not
  blocked. When the MCP probe reports Waypost configured, `waypost status`,
  `recv`, `receive`, and `send` are blocked with a `decision`/`reason` deny in
  favor of the `waypost_status`, `waypost_recv`, and `waypost_send` MCP tools.
  An unavailable probe leaves those CLI commands untouched; a failed probe
  surfaces the error as additional context. The `waypost mcp` command is always
  blocked because the MCP server is managed by Devin
- a `PostToolUse` handler matched on `exec` and
  `mcp__waypost__waypost_recv` observes successful MCP or direct CLI receives
  and changes a pending nudge to consumed; `received` and `no_message` are
  terminal receive results, while active-lease and recovery-required results
  stay pending
- a `PostCompaction` handler emits the anti-repeat receive guard only while
  the current session's latest nudge is consumed
- a `SessionStart` handler emits the same guard after a compact-source session
  start; other sources produce no context
- a `SessionEnd` handler removes the session's nudge state

The small session-scoped state lives under `waypost-hook-state/` inside the
Devin config directory.

Verify the installation:

```bash
waypost doctor devin-hook
```

The doctor verifies all six handler definitions and reports whether
`devin mcp get waypost` sees Waypost for a new Devin process started in the
current directory. The MCP result is diagnostic only: an already-running
session or an imported configuration may differ.

Devin invokes the machine-facing entry point automatically:

```bash
waypost devin-hook
```

It reads the Devin hook event from stdin and emits the matching hook JSON
contract (`decision`/`reason` for denials, `hookSpecificOutput` for additional
context). It does not read or modify Waypost message state.

### Migrate previous local state

Stop all previous-version processes, then move the previous default state
directory to the new Waypost location:

```bash
waypost migrate
```

When the previous default state exists, normal commands require this migration
before they initialize the new default state. The destination must not already
exist for a new migration. If the migration is interrupted, rerun the same
command to finish it; normal Waypost commands will refuse to initialize the
target until then. Source and destination must not overlap. They may be on
different filesystems: on Unix-like systems, Waypost durably copies the
complete state and records a copy commit before removing the old directory. On
Windows cross-volume migrations it retains the old directory as a reported
recovery copy, because Windows does not expose durable directory syncing.

If an older interrupted migration has no durable copy-commit marker and its
source no longer exists, Waypost refuses to treat the destination as complete.

For a custom previous location, pass both paths explicitly:

```bash
waypost --state-dir /new/waypost-state migrate --from /old/legacy-state
```

## Web Transcript UI

Run a local read-only UI for group messages:

```bash
waypost --state-dir /tmp/waypost-demo group web --group group/ops
```

By default it listens on `127.0.0.1:0`, so the OS chooses a free local port and
the command prints the actual URL. The UI reads group message bodies and streams
updates without using `recv`, so it does not mark any participant's group
messages as read. In an interactive terminal it offers to open or copy the URL.

## Typical Flow

Send a message:

```bash
printf 'review request body\n' | \
waypost send \
  --to workflow/reviewer/task-123 \
  --from agent/sender \
  --subject "review request" \
  --body-file -
```

Receive a message:

```bash
waypost recv \
  --for workflow/reviewer/task-123 \
  --json
```

Swap `--json` for `--yaml` when you want the same payload in YAML.
Add `--full` when you need the full legacy payload instead of the default
compact view.

Receive from multiple addresses with one command:

```bash
waypost recv \
  --for workflow/reviewer/task-123 \
  --for workflow/reviewer/task-456 \
  --json
```

Claim multiple messages in one call:

```bash
waypost recv \
  --for workflow/reviewer/task-123 \
  --max 10 \
  --json
```

Without `--max`, structured `recv` returns a `status`, `addresses`, and one
compact `delivery`. With `--max`, it returns ordered `deliveries`. Both shapes
include sparse `remaining_by_state` counts when unfinished deliveries remain;
the current call's returned deliveries are excluded. Each delivery includes its
own `delivery_id` and `lease_token`. Keep those for follow-up actions.

Observe deliveries without claiming them:

```bash
waypost wait \
  --for workflow/reviewer/task-123 \
  --timeout 30s \
  --json
```

`wait` emits one compact delivery metadata object and exits. It never returns
message bodies or lease tokens, and it does not reserve the delivery. Add
`--full` when you need the full legacy metadata object.

Observe deliveries continuously without claiming them:

```bash
waypost watch \
  --for workflow/reviewer/task-123 \
  --timeout 30s \
  --json
```

`watch` emits delivery metadata only. It never returns message bodies or lease
tokens. Use `--yaml` to emit the same metadata as a YAML document stream.

Find queues with receivable delivery older than a threshold:

```bash
waypost stale \
  --for workflow/reviewer/task-123 \
  --for workflow/reviewer/task-456 \
  --older-than 10m \
  --json
```

`stale` is personal-waypost-only and structured-output-only in v1. Use
`--json` or `--yaml`; plain-text mode is intentionally unsupported.

Ack when processing succeeds:

```bash
waypost ack \
  --delivery <delivery_id> \
  --lease-token <lease_token>
```

Renew an active lease when processing needs more time:

```bash
waypost renew \
  --delivery <delivery_id> \
  --lease-token <lease_token> \
  --for 10m
```

List already-acked deliveries later:

```bash
waypost list \
  --for workflow/reviewer/task-123 \
  --state acked \
  --json
```

Read one persisted delivery body later:

```bash
waypost read \
  <delivery_id> \
  --json
```

Read the latest delivery for one queue in one step:

```bash
waypost read \
  --latest \
  --for workflow/reviewer/task-123 \
  --json
```

Group waypost quick start:

```bash
waypost group create --group group/eng
waypost group add-member --group group/eng --person alice
waypost group add-subscriber --group group/eng --notify-address agent/eng-notice --person alice
printf 'team sync\n' | waypost send --to group/eng --group --body-file -
waypost list --for group/eng --as alice --json
waypost recv --for group/eng --as alice --json
```

## Group Waypost

Group waypost is explicit. It does not reuse lease/ack queue semantics.

Rules:

- create and reserve a group address with `group create`
- add or remove members with `group add-member` and `group remove-member`
- add or remove durable notification subscribers with `group add-subscriber`
  and `group remove-subscriber`
- use `send --to <group-address> --group` for group messages
- use `list|wait|recv --for <group-address> --as <person>` for group reads
- `watch`, `ack`, `renew`, `release`, `defer`, `undefer`, `fail`, and `dead-letter` stay personal-waypost-only
- `--as` is caller-asserted identity in the trusted local workflow environment;
  it is not an authentication boundary

Group history semantics:

- new members can see older messages immediately
- older messages start unread for the new member
- leaving a group stops future eligibility but keeps historical visibility
- `eligible_count` is based on membership snapshot at message creation time
- later joins do not rewrite old messages' `eligible_count`

## Receive

`recv` is always an immediate claim attempt.

Rules:

- `recv` returns immediately
- `--max` defaults to `1` and may not exceed `10`
- `--json` and `--yaml` are mutually exclusive
- no-message returns exit code `2`
- repeated `--for` flags search the union of the requested queues
- selection is global oldest-first by `visible_at`, then `message_created_at`,
  then `delivery_id`
- unseen addresses behave like empty queues
- `remaining_by_state` is an informational post-receive snapshot of unfinished
  `queued`, `leased`, and `dead_letter` deliveries; zero counts are omitted
- future-visible deferred deliveries remain in the `queued` count but are not
  necessarily claimable now

## Wait

`wait` is the one-shot observe-only companion to `recv`.

```bash
waypost wait --for <address> [--for <address> ...] [--timeout 30s] [--json | --yaml] [--full]
```

`--timeout` uses Go duration syntax such as `30s`, `5m`, `120ms`, or `1m30s`.

Rules:

- `wait` is observe-only; it does not claim deliveries, create lease tokens, or
  reserve the result
- repeated `--for` flags search the union of the requested queues
- duplicate `--for` values are ignored after the first occurrence
- default wait scope is currently visible queued deliveries
- `--json` emits one delivery metadata object
- `--yaml` emits the same metadata as one YAML mapping
- without `--timeout`, `wait` blocks until the first matching delivery exists
- `--timeout` is an absolute wait deadline; if no matching delivery appears
  before it expires, `wait` exits with code `2`
- selection is deterministic global oldest-first by `visible_at`, then
  `message_created_at`, then `delivery_id`
- unseen addresses behave like empty queues until a matching delivery exists

## Watch

`watch` is the observe-only companion to `recv`.

```bash
waypost watch --for <address> [--for <address> ...] [--state dead_letter] [--timeout 30s] [--json | --yaml]
```

`--timeout` uses Go duration syntax such as `30s`, `5m`, `120ms`, or `1m30s`.

Rules:

- `watch` always stays observe-only; it does not claim deliveries or create
  lease tokens
- repeated `--for` flags search the union of the requested queues
- duplicate `--for` values are ignored after the first occurrence
- default output watches currently visible queued deliveries
- `--state <state>` watches that delivery state instead
- `--json` emits one JSON object per line (NDJSON)
- `--yaml` emits one YAML document per matching delivery, each starting with `---`
- without `--timeout`, `watch` runs until interrupted
- `--timeout` is an idle timeout; if no newly matching delivery appears during
  that interval, `watch` exits successfully
- duplicate polling cycles do not reprint the same unchanged delivery snapshot
- unseen addresses behave like empty queues until matching deliveries exist

## Commands

### `mcp`

Run the built-in stdio MCP server from the main binary.

```bash
waypost mcp
```

To expose the read-only `waypost_debug` tool for a diagnostic session:

```bash
waypost mcp --include-debug-tool
```

Notes:

- this starts the waypost MCP server over stdio
- use the main `waypost` binary in MCP configs and pass `mcp` as the first argument
- `--state-dir` remains a global option on the main binary, but the MCP server manages waypost state through its own tool calls rather than per-command CLI flags
- `waypost_debug` is absent by default; `--include-debug-tool` registers it without changing the normal status gate for other Waypost tools
- `waypost_status` returns compact binding state by default. Set `include_cli_context: true` for the executable and resolved state directory, `include_diagnostics: true` for detection and version data, or `include_active_leases: true` for paginated lease details and tokens; use `limit` and `cursor` only with lease details

### `doc`

Show concise Waypost semantics that are not conveyed by command help.

```bash
waypost doc
waypost doc --list
waypost doc <topic>...
```

`waypost doc` prints a compact, client-neutral description of state-directory
isolation, personal delivery states and transitions, per-person group reads,
and the notification boundary. It does not assume MCP is available, prescribe
an output format, or duplicate the command catalog. `--list` shows the available
focused topics; initial topics are `mcp-cli-boundary`, `recovery`, `history`,
`groups`, `addresses`, `diagnostics`, and `dead-letter`.
One topic retains the plain prompt output. With multiple topics, each prompt is
emitted in argument order as a `waypost: <topic>` block whose body is indented
by two spaces. Command-shaped aliases are accepted when their routing is
unambiguous: `read`, `show`, and `list` select `history`; `group` selects `groups`;
`address` selects `addresses`; `fail` and `undefer` select `recovery`; and
common-flow names such as `send`, `recv`, `receive`, and `claim-history` select
`mcp-cli-boundary`. Unknown-topic errors include the canonical topic list so
callers do not need a separate `doc --list` call.

The `dead-letter` topic explains when to stop retrying a leased delivery and
how that differs from recording a retryable failure.

### `send`

Queue one message for a recipient address, or use repeated `--to` flags to
coordinate independent sends of the same message. With `--group`, every
recipient must be a known group address.

```bash
waypost send --to <address> [--to <address> ...] --body-file <path-or-> [--group] [--notify] [--json | --yaml] [--full]
```

Use `--json` or `--yaml` for scripts and agents.

Common options:

- `--from <address>`
- `--subject <text>`
- `--content-type <mime-type>`
- `--schema-version <version>`
- `--group`
- `--notify` requests a best-effort immediate wakeup after the durable send

Notes:

- `--body-file -` reads from stdin
- the message body must not be empty
- repeat `--to` for a batch of up to 10 raw recipient values; comma-separated
  addresses are not split
- batch recipients are normalized and deduplicated in first-seen order before
  the first send; duplicate flags therefore produce one durable send
- one `--to` keeps the existing single-recipient output and error contract
- personal send default output is a compact acknowledgement with `delivery_id`
- personal `--full` returns the legacy identifier payload with `message_id`,
  `delivery_id`, and `blob_id`
- `send` creates the recipient address automatically on first use
- `send` also creates the optional `--from` address automatically on first use
- `--from` is optional
- `send --to <address>` always uses personal queue semantics
- personal send to a known group address fails with an explicit collision error
- `send --to <group-address> --group` requires an existing group address
- group send stores one durable message record, creates no `deliveries`, and
  returns structured output with `mode`, `message_id`, `group_id`,
  `group_address`, `eligible_count`, and `message_created_at`
- group send plain-text output is
  `message_id=<id> group=<address> eligible_count=<n>`

For two or more `--to` flags, sends run in normalized order and each target has
its own durable transaction and optional notification. JSON and YAML return one
batch envelope such as:

```json
{
  "status": "partial_failed",
  "to_addresses": ["agent-deck/alpha", "agent-deck/beta"],
  "recipient_count": 2,
  "sent_count": 1,
  "failed_count": 1,
  "results": [
    {"to_address": "agent-deck/alpha", "status": "sent", "delivery_id": "dlv_..."},
    {"to_address": "agent-deck/beta", "status": "failed", "error": "..."}
  ]
}
```

The text form emits one line per result and one aggregate line. A durable
failure does not stop later recipients; the command writes the complete batch
envelope and then exits 1 with a concise stderr summary. Notification failures
remain informational. Retry only failed addresses after inspecting `results`,
because retrying the full batch can create duplicate messages for earlier
successes.

When `--notify` is set, structured output additionally includes
`notify_status`, `notify_scheme`, optional `notify_detail`, and `notify_error`.
`unconfirmed` means the wake was attempted without enough evidence to confirm
turn submission; that command is not retried or followed by another target in
the same wake attempt. Later unread-delivery reminders retain the scheduler's
normal cooldown policy. `target_not_found` means the addressed target session
does not exist. A notification failure is reported in those fields but does
not undo the durable delivery or make the command fail. For an
`agent-deck/<session-id>` target, `notify_detail` includes a warning and may
suggest a nearby known session with `Did you mean agent-deck/<other-id>?`.
Plain-text output appends the same status fields. Supported
remote targets include `agent-deck/<session-id>` and `thurbox/<session-id>`;
unsupported or local targets are reported without a notification side effect.

### `recv`

Claim one or more personal deliveries, or receive one unread group message for a
specific person.

```bash
waypost recv --for <address> [--for <address> ...] [--max 10] [--json | --yaml] [--full]
waypost recv --for <group-address> --as <person> [--json | --yaml] [--full]
```

Use `--json` or `--yaml` for scripts and agents.

Notes:

- repeat `--for` to search multiple queues with one batch claim
- `--max <n>` limits how many deliveries one invocation can lease and may not exceed `10`
- duplicate `--for` values are ignored after the first occurrence
- without `--max`, structured output returns `status`, `addresses`, and a
  compact `delivery` with `delivery_id`, `recipient_address`, `lease_token`,
  `sender_address` (when the sender supplied an address), `subject`,
  `content_type`, and `body`
- add `--full` to return the full legacy single-message payload
- with `--max`, structured output returns `status`, `addresses`, ordered
  `deliveries`, and sparse `remaining_by_state`; it never emits `has_more`
- with `--max --full`, each returned delivery uses the full payload
- unseen addresses are ignored until a matching delivery exists
- `recv` does not wait; use `wait` if you need to block until work appears
- group mode requires `--as <person>` and exactly one `--for`
- group mode does not support `--max`
- group mode returns the oldest unread visible group message for that person and
  marks it read immediately
- compact group `recv` output includes `message_id`, `group_id`,
  `group_address`, `person`, `sender_address` (when present),
  `message_created_at`, `subject`, `content_type`, `body`, `read_count`,
  `eligible_count`, and `first_read_at`
- group `recv --full` adds `sender_endpoint_id`, `schema_version`,
  `body_blob_ref`, `body_size`, and `body_sha256`

### `wait`

Observe until one matching queued delivery exists, or until one unread visible
group message exists for a specific person.

```bash
waypost wait --for <address> [--for <address> ...] [--timeout 30s] [--json | --yaml] [--full]
waypost wait --for <group-address> --as <person> [--timeout 30s] [--json | --yaml] [--full]
```

Use `--json` or `--yaml` for scripts and agents.

Notes:

- `--timeout` uses Go duration syntax such as `30s`, `5m`, `120ms`, or `1m30s`
- repeat `--for` to search multiple queues with one wait
- duplicate `--for` values are ignored after the first occurrence
- plain-text output includes `recipient_address=...`
- default `wait` output is a compact metadata view with `delivery_id`,
  `recipient_address`, `subject`, and `content_type`
- add `--full` to return the full legacy delivery metadata schema used by `list`
  and `watch`
- `wait` does not claim or reserve the returned delivery; use `recv` to claim work
- group mode requires `--as <person>` and exactly one `--for`
- group `wait` stays observe-only; it does not mark the message read
- compact group `wait` output includes `message_id`, `group_id`,
  `group_address`, `person`, `message_created_at`, `subject`, `content_type`,
  `read`, `first_read_at`, `read_count`, and `eligible_count`
- group `wait --full` adds `sender_endpoint_id` and `schema_version`

### `watch`

Observe matching deliveries without claiming them.

```bash
waypost watch --for <address> [--for <address> ...] [--state dead_letter] [--timeout 30s] [--json | --yaml]
```

Use `--json` or `--yaml` for streaming consumers.

- `--json` emits one delivery metadata object per line, not a JSON array
- `--yaml` emits one YAML document per delivery, separated by `---`

Notes:

- `--timeout` uses Go duration syntax such as `30s`, `5m`, `120ms`, or `1m30s`
- default watch scope is visible queued deliveries
- `--state` lets you watch another delivery state with the same metadata schema
- plain-text output includes `recipient_address=...`
- `watch` is for observation only; use `recv` to claim work

### `read`

Read one or more persisted messages, one or more deliveries by id, or the
latest deliveries for one or more queues.

`show` is an alias for `read`; every option and output mode works identically.

```bash
waypost read <id> [<id> ...] [--json | --yaml]
waypost read --message <id> [--message <id> ...] [--json | --yaml]
waypost read --delivery <id> [--delivery <id> ...] [--json | --yaml]
waypost read --latest --for <address> [--for <address> ...] [--from <address>] [--state <state>] [--limit <n>] [--cursor <cursor>] [--json | --yaml]
```

Use `--json` or `--yaml` for scripts and agents.

Notes:

- a positional ID beginning with `dlv_` reads a delivery; any other positional
  ID reads a message
- positional IDs can be repeated, but must all identify the same kind of record
- `--delivery` and `--message` remain available when an explicit selector is
  preferred
- `--message` reads by message identity, which matches the body-bearing object
- reading by a message ID, including a positional non-`dlv_` ID, is a raw
  trusted-environment body read and does not update group read tracking
- `--delivery` reads by one or more delivery records, regardless of whether they are
  `queued`, `leased`, `acked`, or `dead_letter`
- `--latest` requires at least one `--for`
- `--latest` defaults to no state filter (`any`) and `--limit 1`
- `--latest` searches the union of the requested queues and returns newest-first
- paginated `--latest` results use stable keyset cursors; delivery state filters
  are evaluated from current state on each page request
- `--from <address>` restricts `--latest` to messages sent by that endpoint
- forwarded messages match the current forwarder's address; the original
  source remains available separately as `forwarded_from_address`
- `--limit` may not exceed 100; `next_cursor` continues a truncated latest read
- structured output always returns an object with `items`; it retains sparse
  `has_more: true` for compatibility and emits `next_cursor` when another page exists
- returns the persisted body after verifying the blob size and sha256
- plain-text output prints one item after another, separated by `---`
- `--message` items return message metadata plus `body`
- `--delivery` and `--latest` items also include delivery metadata such as
  `state`, recipient, and `acked_at` when present
- read results include `sender_address` when the message has a sender

### `ack`

Mark a leased delivery as complete.

```bash
waypost ack --delivery <delivery_id> --lease-token <lease_token>
```

### `renew`

Extend a current lease without changing the lease token.

```bash
waypost renew --delivery <delivery_id> --lease-token <lease_token> --for 10m
```

Notes:

- `--for` uses Go duration syntax such as `30s`, `5m`, `10m`, or `1h`
- renewal requires the current lease token; expiry only allows another receiver
  to reclaim and replace that token
- renewal keeps the same `lease_token` and only updates `lease_expires_at`

### `release`

Return a leased delivery to the queue immediately.

```bash
waypost release --delivery <delivery_id> --lease-token <lease_token>
```

### `defer`

Return a leased delivery to the queue, but hide it until a future time.

```bash
waypost defer \
  --delivery <delivery_id> \
  --lease-token <lease_token> \
  --until 2026-03-18T12:00:00Z
```

### `undefer`

Make a deferred queued delivery visible immediately.

```bash
waypost undefer --delivery <delivery_id> [--json | --yaml]
```

Notes:

- `undefer` does not restore the old lease or acknowledge the delivery
- after `undefer`, call `recv` again and use the new lease token for `ack`
- if the delivery is already visible, call `recv` instead of `undefer`

### `fail`

Record a processing failure.

```bash
waypost fail \
  --delivery <delivery_id> \
  --lease-token <lease_token> \
  --reason "tool crashed" \
  --json
```

Retry behavior in v1:

- attempts 1 and 2 requeue immediately
- attempt 3 moves the delivery to `dead_letter`

### `dead-letter`

Stop retrying a currently leased delivery immediately.

```bash
waypost dead-letter \
  --delivery <delivery_id> \
  --lease-token <lease_token> \
  --reason "unsupported request" \
  --json
```

Behavior:

- requires the current lease token
- moves the delivery directly from `leased` to `dead_letter`
- does not increment `attempt_count`
- records the reason and retains the message for history and diagnosis
- use `fail` instead when another processing attempt remains appropriate

### `list`

Inspect queued personal deliveries for one recipient address, or inspect group
message metadata visible to one person.

```bash
waypost list --for <address> [--from <address>] [--state queued|leased|acked|dead_letter] [--limit <n>] [--cursor <cursor>] [--json | --yaml]
waypost list --for <group-address> --as <person> [--from <address>] [--limit <n>] [--cursor <cursor>] [--json | --yaml]
```

Notes:

- default output shows currently claimable queued deliveries
- `--state queued|leased|acked|dead_letter` filters to that personal delivery state
- `--from <address>` filters personal or group results by sender endpoint
- forwarded messages match the current forwarder, while
  `forwarded_from_address` identifies the original source
- structured and plain-text results include `sender_address` when present
- `acked` results include `acked_at` in structured output and plain text
- `list` returns the results currently visible in one request; use `wait` for
  one-shot blocking or `watch` for a stream
- personal `list` pages use immutable message creation order; delivery state
  filters are evaluated from current state on each page request
- use `--json` or `--yaml` for scripts and agents
- unseen addresses return an empty result
- group mode requires `--as <person>`
- every call returns at most 100 items (default 50); structured output is
  `{items, next_cursor}` and plain text prints `next_cursor=...` when another page exists
- pass the returned cursor back with the same address, identity, state, and sender filters
- `--state` is not supported with `--as`
- group `list` returns visible group message metadata oldest-first
- compact group `list` output includes `message_id`, `group_id`,
  `group_address`, `person`, `message_created_at`, `subject`, `content_type`,
  `read`, `first_read_at`, `read_count`, and `eligible_count`

### `stale`

List personal queues whose oldest currently receivable delivery is older than
the requested threshold.

```bash
waypost stale --for <address> [--for <address> ...] --older-than 10m [--json | --yaml]
```

Use `--json` or `--yaml`. One of them is required.

Notes:

- `--older-than` uses Go duration syntax such as `30s`, `5m`, `120ms`, or `1h`
- repeat `--for` to check multiple queues in one query
- duplicate `--for` values are ignored after the first occurrence
- plain-text mode is not supported in v1
- unseen addresses behave like empty queues
- known group addresses fail explicitly; this command is personal-waypost-only
- queued deliveries count when `visible_at <= now`
- expired leased deliveries count when `lease_expires_at <= now`
- future invisible deliveries do not count as stale
- results are ordered by `oldest_eligible_at`, then `address`
- each result object contains `address`, `oldest_eligible_at`, and `claimable_count`

### `group create`

Reserve a group address explicitly.

```bash
waypost group create --group <address> [--json | --yaml]
```

Notes:

- group addresses are explicit objects; there is no lazy create during send
- creating a group on top of an existing endpoint address fails
- structured output returns `group_id`, `address`, and `created_at`

### `group add-member`

Add one person to an existing group.

```bash
waypost group add-member --group <address> --person <person> [--json | --yaml]
```

Notes:

- the group must already exist
- the person record is created automatically on first use
- adding the same active member twice fails explicitly
- structured output returns membership metadata including `membership_id`,
  `group_id`, `group_address`, `person_id`, `person`, `joined_at`, and `active`

### `group remove-member`

Close the active membership for one person in a group.

```bash
waypost group remove-member --group <address> --person <person> [--json | --yaml]
```

Notes:

- removing a person who has no active membership fails explicitly
- historical visibility remains even after removal
- structured output returns the membership record with `left_at` and `active=false`

### `group members`

List active and historical membership records for one group.

```bash
waypost group members --group <address> [--limit <n>] [--cursor <cursor>] [--json | --yaml]
```

Notes:

- output is ordered oldest membership first
- every page contains at most 100 entries (default 50)
- each entry includes `membership_id`, `group_id`, `group_address`, `person_id`,
  `person`, `joined_at`, optional `left_at`, and `active`

### `group add-subscriber`

Add one durable notification subscriber to a group.

```bash
waypost group add-subscriber --group <address> --notify-address <address> --person <person> [--json | --yaml]
```

The group must exist. Adding the same active subscriber again fails explicitly.

### `group remove-subscriber`

Remove one active group notification subscriber.

```bash
waypost group remove-subscriber --group <address> --notify-address <address> [--json | --yaml]
```

### `group subscribers`

List active group notification subscribers in creation order.

```bash
waypost group subscribers --group <address> [--limit <n>] [--cursor <cursor>] [--json | --yaml]
```

The default page size is 50 and the hard maximum is 100. Structured output
uses `items` and optional `next_cursor`.

### `address inspect`

Inspect whether an address is currently unbound, a personal endpoint, or a
group.

```bash
waypost address inspect --address <address> [--json | --yaml]
```

Notes:

- structured output returns `address`, `kind`, and the relevant id field
- `kind` is one of `endpoint`, `group`, or `unbound`
- this is a debugging and introspection command; it does not change routing

## Exit Codes

- `0`: success
- `2`: `recv` found no message, or `wait --timeout ...` found no matching
  delivery
- `1`: CLI-owned `--json` operations emit one JSON error document on stderr
  and leave stdout empty
- other non-zero: usage error or operational failure
