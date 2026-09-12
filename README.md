# Waypost

`waypost` is a local-first handoff point for agent-managed workflows. It keeps
messages, delivery state, and audit history so one participant can persist work
immediately and another can claim it later.

The current MVP is intentionally narrow:

- one local Unix user on one machine
- direct delivery by endpoint address
- SQLite metadata plus blob-backed message bodies
- explicit `send`, `recv`, `wait`, `watch`, `list`, `stale`, `ack`, `renew`, `release`, `defer`, `undefer`, `fail`, and `dead-letter`
- no daemon, no network transport, no adapter-specific correctness dependency

## Rename note

Waypost was previously named `agent-mailbox`. New releases use the `waypost`
CLI, `waypost_*` MCP tools, and `WAYPOST_STATE_DIR`.

To move the previous default local state directory once, stop all
previous-version processes and run:

```bash
waypost migrate
```

The command moves the directory into the current Waypost state path. Across
filesystem boundaries on Unix-like systems, it durably copies the complete
state and writes a copy-commit marker before removing the old directory. On
Windows cross-volume migrations, it keeps the old directory as an explicit
recovery copy because Windows does not expose durable directory syncing; the
command reports that retained source. When the previous default state exists,
normal commands refuse to
initialize the new default state until you migrate it. A new migration refuses
to overwrite an existing destination; if an interrupted migration left its own
state behind, rerun the same command to finish it. While that migration is
incomplete, normal Waypost commands refuse to create or open a new database at
the destination. Source and destination paths must not overlap.

If an older interrupted migration lacks a durable copy-commit marker and its
source is already absent, Waypost refuses to guess that the destination is
complete; inspect or restore the source before retrying.

For a previous custom location, provide both paths explicitly:

```bash
waypost --state-dir /new/waypost-state migrate --from /old/legacy-state
```

## Requirements

To build the CLI from source you need:

- Go 1.24 or newer
- a working C toolchain, because the current SQLite driver uses CGO

## Build

Build a local executable:

```bash
make build
```

On Windows without GNU `make`, use the PowerShell wrapper with the same targets:

```powershell
./make.ps1 build
```

This project still requires CGO. If `go env CGO_ENABLED` prints `0`, install a
Windows C toolchain first and rerun with CGO enabled, for example:

```powershell
$env:CGO_ENABLED = 1
$env:CC = "C:/msys64/ucrt64/bin/gcc.exe"
$env:CXX = "C:/msys64/ucrt64/bin/g++.exe"
./make.ps1 build
```

This produces:

```text
./bin/waypost
```

On Windows, the PowerShell wrapper builds:

```text
.\bin\waypost.exe
```

Report the CLI version (the same value advertised by the built-in MCP server):

```bash
waypost --version
```

Run the full test suite:

```bash
make test
```

Windows:

```powershell
./make.ps1 test
```

Show the available build targets:

```bash
make help
```

Windows:

```powershell
./make.ps1 help
```

Run the stdio MCP server from the main binary:

```bash
make run-mcp
```

Windows:

```powershell
./make.ps1 run-mcp
```

## Install

Install into `/usr/local/bin`:

```bash
make build
sudo make install
```

On Windows, `install` writes a stable launcher into `%USERPROFILE%\.local\bin`
and stores the real CLI under `%USERPROFILE%\.local\lib\waypost\versions`:

```powershell
./make.ps1 install
```

Windows upgrades are a hard cut. Stop running Waypost processes and Codex
sessions that use Waypost before installing. If the stable launcher is locked,
the install fails without activating the new version; rerun it after those
processes exit. Successful installs route new processes through the launcher to
the active version recorded in `lib\waypost\active-version.json`.

Install into a user-local prefix without needing root:

```bash
make build
make install PREFIX="$HOME/.local"
```

Windows PowerShell equivalent:

```powershell
$env:PREFIX = "$HOME\\.local"
./make.ps1 install
```

If you use a user-local prefix, make sure `"$HOME/.local/bin"` is in your
`PATH`.

## User Guide

For day-to-day CLI usage, examples, receive semantics, exit codes, and command
reference, see [`docs/cli.md`](docs/cli.md).

## MCP Server

This repo ships an in-repo Go stdio MCP server that now runs as a built-in
subcommand of the main `waypost` binary.

Build the main binary locally:

```bash
make build
```

Run the MCP server directly:

```bash
./bin/waypost mcp
```

Register the built-in server in Codex's global MCP configuration (and in
Claude Code, agy, or Devin when those agents are installed):

```bash
waypost install mcp-server
```

Install the Devin lifecycle hooks that keep a Devin session's Waypost nudges
and compaction context in sync:

```bash
waypost install devin-hook
waypost doctor devin-hook
```

Windows:

```powershell
.\bin\waypost.exe mcp
```

Or use the convenience target:

```bash
make run-mcp
```

Example MCP config:

```toml
[mcp_servers.waypost]
command = "/absolute/path/to/waypost/bin/waypost"
args = ["mcp"]
required = true
env_vars = ["TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]
tool_timeout_sec = 660
```

Windows example:

```toml
[mcp_servers.waypost]
command = "C:\\absolute\\path\\to\\waypost\\bin\\waypost.exe"
args = ["mcp"]
required = true
env_vars = ["TMUX", "AGENTDECK_INSTANCE_ID", "CODEX_THREAD_ID", "CODEX_SESSION_ID", "WAYPOST_STATE_DIR", "XDG_STATE_HOME"]
tool_timeout_sec = 660
```

The default Go MCP entrypoint exposes these Waypost tools:

- `waypost_bind`
- `waypost_status`
- `waypost_send`
- `waypost_recv`
- `waypost_claim_history`
- `waypost_ack`
- `waypost_release`
- `waypost_defer`
- `agent_deck_create_session`
- `agent_deck_require_session`
- `session_create`
- `session_require`

`waypost_debug` is intentionally absent from that default surface. Start the
server as `waypost mcp --include-debug-tool` only when its read-only
environment diagnostics are needed.

The host-neutral `session_create` tool takes the caller-supplied optional
strings `full_command_line` (Agent Deck) and `thurbox_agent_key` (Thurbox).
After host selection it consumes only the applicable value; it does not load a
Waypost profile mapping. Existing launchers may still pass
`waypost mcp --session-host-config PATH`; that deprecated option is accepted
and ignored.

For generic Agent Deck creation, the adapter inherits the direct parent's
non-empty group from its preflight snapshot; callers do not provide a group
field. Root-group and nested-parent cases are rejected before launch.

Call `waypost_status` once after starting each MCP server process. It
auto-binds detectable session addresses from `agent-deck session current`,
tool environment variables such as `CODEX_THREAD_ID`, `CLAUDE_CODE_SESSION_ID`,
`GEMINI_SESSION_ID`, `OPENCODE_SESSION_ID`, and `DEVIN_SESSION_ID`, and on Unix
a parent-process probe that recognizes Codex and Devin sessions. When an
agent-deck session is already known, it can also use the agent-deck state
database to fill in a Codex thread synced later for that same workdir and
session.
That yields addresses such as `agent-deck/<session-id>`, `codex/<session-id>`,
`claude/<session-id>`, `gemini/<session-id>`, `opencode/<session-id>`, and
`devin/<session-id>`.
`waypost_status` returns only `status`, binding state when present, actionable
warnings, and a non-zero active-lease count by default. Set
`include_cli_context: true` when you need the authoritative executable and
resolved state directory for a CLI-owned operation. Set
`include_diagnostics: true` for detection and version fields, or
`include_active_leases: true` for paginated lease details and tokens. Use
`limit` and `cursor` only with active lease details.

All default Waypost tools other than `waypost_status` fail until it succeeds,
so callers get the
current binding state and any recovery warnings before they read, send, claim,
ack, or alter waypost state. If auto-bind cannot find a supported tool session
address, call `waypost_status` again after agent-deck has synced state for the
current session or call `waypost_bind` manually.
Tool session environment variable values must look like session ids (hex-like
for Codex, Claude, Gemini, and OpenCode; a slug such as `truth-alarm` for
Devin's `DEVIN_SESSION_ID`); invalid values are ignored and reported in the
`waypost_status` warnings.

When `waypost mcp --include-debug-tool` is in use, `waypost_debug` may run
before or after `waypost_status` when auto-bind behavior is unclear. It is
read-only, does not auto-bind, and reports only allowlisted tool session
environment diagnostics for `CODEX_THREAD_ID`,
`CLAUDE_CODE_SESSION_ID`, `GEMINI_SESSION_ID`, `OPENCODE_SESSION_ID`, and
`DEVIN_SESSION_ID`,
including whether each value is present, accepted by validation, and what
address it would produce. Its broader debug environment diagnostics also include
`AGENTDECK_INSTANCE_ID` and `TMUX`. On Linux it inspects the parent process
chain for those same allowlisted variables so callers can tell whether a tool
omitted a variable or failed to pass it into the MCP process.

`waypost_send` requires a single `to` field. A string selects the
single-recipient contract; an array of 1-10 strings selects a batch. The array
form normalizes and deduplicates recipients in first-seen order, sends them
sequentially, and always returns a batch envelope with `to_addresses`,
`recipient_count`, `sent_count`, `failed_count`, and ordered `results`.
Ordinary per-recipient durable failures appear as `failed` result items without
stopping later recipients; retry only those failed addresses to avoid duplicate
messages. The string form returns the existing single-send output.

`from_address` is optional, but when supplied it must be one of the currently
bound personal addresses in this MCP server instance. `waypost_bind` likewise
requires an explicit `default_sender` to be one of its bound addresses. Unlike
the CLI's standalone `--from`, MCP never creates a sender identity from an
arbitrary unbound address.

Supply the message content with exactly one of `body` or `body_file`.
`body` keeps the existing inline-string behavior. `body_file` is a filesystem
path read by the MCP server before sending. It requires a bound
`default_workdir`; relative paths resolve from that directory, and absolute
paths must remain inside it. Waypost resolves symlinks and Windows junctions
before enforcing the boundary; if resolution is unsupported or fails, the
send is rejected. It also rejects Windows alternate data streams and
non-regular files, and limits file-backed bodies to 10 MiB. If the workdir or
path boundary cannot be established, the send is rejected. The file is read
once and the resulting snapshot is reused for every recipient in a batch.
Inline and file-backed empty bodies are rejected.

`waypost_send` always uses the fixed wakeup text for supported remote notify
paths. Set `disable_notify_message = true` to skip only that immediate send-time
notify. Its default single-recipient result contains the durable receipt,
`status`, and `notify_status`; `notify_detail` explains an unconfirmed attempt,
while `notify_error` is added only on definite failure. An `unconfirmed` nudge
was attempted but could not be verified, so that command is not retried or
followed by another target in the same wake attempt. Later unread-delivery
reminders retain the scheduler's normal cooldown policy. Set
`diagnostics: true` for effective routing, notification scheme, and group
storage metadata. Batch result items retain their resolved sender, recipient,
subject, notification outcome, and applicable receipt fields. Input echoes such
as `subject` are not part of the singular compact contract.

`waypost_recv` defaults to the status-specific result only: `delivery` for a
new claim, a bounded `claimed_delivery_ids` hint for active leases, or just
`status = no_message`. After `no_message`, the same MCP connection must wait
at least 15 seconds before calling `waypost_recv` again. A second
`no_message` within three minutes includes a warning to avoid meaningless
polling. Actionable warnings remain sparse. Set
`diagnostics: true` for resolved addresses and `remaining_by_state`.
Repeated instructional fields, echoed known IDs, and counts derivable from the
returned ID list are intentionally omitted.

For a blocking receive, forwarding, group work, or durable inspection that is
not on the retained MCP surface, call `waypost_status` with
`include_cli_context: true`, then use the reported CLI binary and state
directory. `waypost wait --json` observes work without claiming it; after it
returns a message, call MCP `waypost_recv` to claim a personal delivery. See
[`docs/cli.md`](docs/cli.md) for the CLI forms and group behavior.

`agent_deck_create_session` is for lifecycle allocation only. It creates a new
session, errors if the target already exists, supports explicit group placement
through `group_path` or `group_parent_session_id` plus `child_group_name`, and
can launch detached sessions with `no_parent_link = true`. `startup_instruction`
is optional startup-only input passed to `agent-deck launch --message`; do not
use it for task payloads or normal wakeups.

`session_require` and `agent_deck_require_session` are the session lookup and
send-time guards. They never create a session. Each resolves `session_id` or
`session_ref`, returns `status = not_found` without an MCP error when the
target is absent, verifies an existing target belongs to the explicit
`workdir`, and starts it if needed. `auto_restart` defaults to `true`; set it
to `false` for a read-only lookup that returns `status = not_ready` instead of
starting a stopped session. Callers do not need a separate resolve preflight.
After a confirmed start, an unavailable, missing, or not-ready readback returns
`status = ready_unverified` with `started_session = true` and recovery details;
callers must not repeat the start.
`waypost_send` remains transport-only and does not create downstream sessions.
Pass `sessions` as a non-empty array of session IDs or refs to require multiple
sessions in one call. All batch items use the same explicit `workdir`, return
ordered `results`, and
report a per-session `error` without preventing the other sessions from being
required.

## Quick Start

Use `--state-dir` for demos and tests so waypost state stays isolated:

```bash
export WAYPOST_STATE_DIR=/tmp/waypost-demo
```

Address prefixes such as `workflow/...` or `agent/...` are naming conventions for
humans and tooling.

The default state directory is:

- `$WAYPOST_STATE_DIR` when set
- otherwise `$XDG_STATE_HOME/ai-agent/waypost`
- otherwise `~/.local/state/ai-agent/waypost`

You can set `WAYPOST_STATE_DIR` once, or pass `--state-dir` per command.
Structured-output commands (`send`, `forward`, `list`, `recv`, `wait`, `watch`, and `stale`) accept
either `--json` or `--yaml`, but not both together.
`send`, `forward`, `recv`, and `wait` also accept `--full` when you need the full legacy
payload instead of the default compact view.

## Minimal Example

Send a message from stdin:

```bash
printf 'review request body\n' | \
waypost --state-dir /tmp/waypost-demo \
  send --to workflow/reviewer/task-123 --from agent/sender \
  --subject "review request" --body-file -
```

`send` requires a non-empty message body. Empty stdin and empty files are
rejected. Repeat `--to` to deliver the same payload to up to 10 raw recipients;
the batch form normalizes and deduplicates targets in first-seen order while a
single `--to` retains the legacy output. A partial durable batch writes ordered
results and exits 1, so retry only its failed targets.

By default, `send` prints only `delivery_id=...`. Add `--notify` to request a
best-effort immediate wakeup of a supported remote recipient after the durable
send succeeds. Add `--full` when you also need the legacy `message_id` and
`blob_id`, or add `--json` / `--yaml` for the same compact or full payloads in
structured form.

With `--notify`, structured output includes `notify_status`, `notify_scheme`,
optional `notify_detail`, and `notify_error`. `notify_status=unconfirmed` means
the wake request may have reached the target but turn submission was not
verified; it is not reported as a wake failure, and the same command is not
immediately retried.
`notify_status=target_not_found` means the addressed target session does not
exist. For `agent-deck/<session-id>` targets, `notify_detail` also includes a
clear warning and, when a nearby known session is available, a `Did you mean
agent-deck/<other-id>?` suggestion.
The scheduler may issue a later reminder after its normal cooldown if the
delivery remains unread. Notification failure is informational and never rolls
back the durable delivery.

Group delivery is explicit. Create a group address first, then send with
`--group`:

```bash
printf 'group update\n' | \
waypost --state-dir /tmp/waypost-demo \
  send --to group/ops --group --from agent/sender --body-file - --json
```

Group send keeps personal send semantics unchanged: plain `send --to <address>`
still targets the personal queue path and fails if `<address>` is already a
known group address.

Open a local read-only transcript UI for group delivery:

```bash
waypost --state-dir /tmp/waypost-demo \
  group web --group group/ops
```

The web UI listens on `127.0.0.1:0` by default, so the OS chooses a free local
port. It prints the actual URL on startup and shows group messages without
marking them read. In an interactive terminal it also offers to open or copy the
URL. Use `--listen 127.0.0.1:8765` if you want a stable port.

Forward a stored message to a new recipient by message id or delivery id:

```bash
waypost --state-dir /tmp/waypost-demo \
  forward --message msg_123 --to workflow/reviewer/task-456 --from agent/sender --json
```

`forward` reuses the original body, `content_type`, and `schema_version`, and
defaults the subject to `Fwd: <original subject>` unless `--subject` overrides it.

Receive the next claimable message:

```bash
waypost --state-dir /tmp/waypost-demo \
  recv --for workflow/reviewer/task-123 --json
```

Receive across multiple queues with one command:

```bash
waypost --state-dir /tmp/waypost-demo \
  recv --for workflow/reviewer/task-123 --for workflow/reviewer/task-456 --json
```

Claim up to 10 messages in one call:

```bash
waypost --state-dir /tmp/waypost-demo \
  recv --for workflow/reviewer/task-123 --max 10 --json
```

Ask for the full receive payload only when you need internal metadata such as
lease expiry or blob references:

```bash
waypost --state-dir /tmp/waypost-demo \
  recv --for workflow/reviewer/task-123 --json --full
```

Observe matching queued deliveries without claiming them:

```bash
waypost --state-dir /tmp/waypost-demo \
  wait --for workflow/reviewer/task-123 --timeout 30s --json
```

The default `recv`/`wait` JSON and YAML payloads are intentionally compact.
Use `--full` to include the full legacy metadata shape.

Stream matching queued deliveries without claiming them:

```bash
waypost --state-dir /tmp/waypost-demo \
  watch --for workflow/reviewer/task-123 --timeout 30s --json
```

`--timeout` uses Go duration syntax such as `30s`, `5m`, `120ms`, or `1m30s`.

Use `--yaml` when you want the same `list`, `recv`, `wait`, `watch`, or `stale`
payloads in YAML. `wait` returns one YAML mapping. `watch` returns a YAML
document stream with one delivery per `---` document.

Find personal queues with receivable delivery older than a threshold:

```bash
waypost --state-dir /tmp/waypost-demo \
  stale --for workflow/reviewer/task-123 --older-than 10m --json
```

`stale` is structured-output-only in v1: use `--json` or `--yaml`.

Ack the leased delivery using the returned `delivery_id` and `lease_token`:

```bash
waypost --state-dir /tmp/waypost-demo \
  ack --delivery <delivery_id> --lease-token <lease_token>
```

Renew an active lease when a worker needs more time before acking:

```bash
waypost --state-dir /tmp/waypost-demo \
  renew --delivery <delivery_id> --lease-token <lease_token> --for 10m
```

Defer a leased delivery until a future time, then make it claimable early if the
blocking condition clears:

```bash
waypost --state-dir /tmp/waypost-demo \
  defer --delivery <delivery_id> --lease-token <lease_token> --until 2026-03-18T12:00:00Z

waypost --state-dir /tmp/waypost-demo \
  undefer --delivery <delivery_id>
```

After `undefer`, call `recv` again and use the new lease token before acking.

When a leased delivery must not be retried, move it directly to `dead_letter`.
This terminal action records the reason without incrementing `attempt_count`:

```bash
waypost --state-dir /tmp/waypost-demo \
  dead-letter --delivery <delivery_id> --lease-token <lease_token> --reason "unsupported request"
```

Use `waypost doc dead-letter` for the distinction between a retryable `fail`
and an explicit terminal decision.

List previously acked deliveries for one queue:

```bash
waypost --state-dir /tmp/waypost-demo \
  list --for workflow/reviewer/task-123 --state acked --json
```

Read one persisted delivery body later by `delivery_id`:

```bash
waypost --state-dir /tmp/waypost-demo \
  read <delivery_id> --json
```

Read the same body directly by `message_id`:

```bash
waypost --state-dir /tmp/waypost-demo \
  read <message_id> --json
```

`show` is an alias for `read`. Both commands infer a delivery when the ID
starts with `dlv_`; all other IDs are read as message IDs. The explicit
`--delivery` and `--message` forms remain available.

Reading by a message ID (with `--message` or a positional non-`dlv_` ID) is a
raw body read in the trusted local environment. It does not update group read
tracking; group `recv` remains the operation that records reads and advances
group unread/read state.

For the common "read the previous message from this queue" case, skip `list`
and read the latest acked delivery in one step:

```bash
waypost --state-dir /tmp/waypost-demo \
  read --latest --for workflow/reviewer/task-123 --json
```

Filter personal history by sender with `--from`; the same filter works for
group history visible to a person:

```bash
waypost --state-dir /tmp/waypost-demo \
  read --latest --for workflow/reviewer/task-123 --from agent/sender --json
waypost --state-dir /tmp/waypost-demo \
  list --for group/eng --as alice --from agent/sender --json
```

Sender filtering matches the current sender or forwarder. Forwarded messages
report the original source separately in `forwarded_from_address`.

All potentially unbounded list surfaces are cursor-paginated. `list`,
`read --latest`, `group list`, `group members`, and `group subscribers` accept
`--limit` and `--cursor`; the default page is 50 items and the hard maximum is
100. Structured results use `items` plus optional `next_cursor`. Reuse a cursor
only with the exact same query scope and filters.

For the full command reference, see [`docs/cli.md`](docs/cli.md).

`recv` v1 contract for multiple `--for` flags:

- repeated `--for` searches the union of the requested queues
- `--max` limits how many deliveries one command will claim, up to `10`
- default output returns a compact leased message view
- `--full` returns the full legacy leased-message payload
- when `--max` is provided, structured output returns `messages` plus sparse
  `remaining_by_state`; receive output does not use pagination cursors
- unseen addresses behave like empty queues
- selection is deterministic global oldest-first by `visible_at`, then
  `message_created_at`, then `delivery_id`

Use `list` for a one-shot snapshot, `wait` for a one-shot observe-only block,
`watch` for observe-only streaming metadata, and `recv` when the consumer is
ready to claim work and receive a lease token.

## Cron Wake Helper

For cron-style wakeups of live `agent-deck` sessions, use:

```bash
scripts/wake-stale-agent-deck-sessions.sh \
  --older-than 10m \
  --confirm-delay 2 \
  --state-dir /tmp/waypost-demo
```

The script:

- lists `waiting` and `idle` sessions
- runs `waypost stale` for their `agent-deck/<session-id>` queues
- waits briefly, rechecks both session status and staleness, then sends
  `agent-deck session send --no-wait` only if the session is still idle/waiting

Install it into a bin directory:

```bash
scripts/wake-stale-agent-deck-sessions.sh install --prefix "$HOME/.local"
```

`--all-delivery-states` tells the script to pass `--all` through to `agent-deck list`,
so it includes sessions that the default list view may hide.

## Local State Layout

The waypost state directory contains:

- `waypost.db`: authoritative SQLite state for endpoints, messages, deliveries,
  and events
- `blobs/`: immutable message body files referenced by `messages.body_blob_ref`

The event log is append-only audit history. Current-state tables remain the
source of truth for delivery behavior.

## Durability Boundaries

`send` now makes the blob durable before it starts the SQLite write transaction:
it writes a temp file in `blobs/`, fsyncs that file, renames it into place, and
fsyncs the `blobs/` directory. That narrows the success window so committed
metadata does not rely on an unflushed blob filename or payload.

The remaining v1 trade-off is explicit: there is still no cross-store atomic
commit between the filesystem blob and the SQLite transaction. A crash after the
blob is durable but before the SQLite transaction commit can still leave an
orphaned blob. That is acceptable for the MVP and should be handled by a later
GC command rather than hidden behind implicit cleanup.
