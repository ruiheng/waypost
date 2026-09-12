# Waypost MCP And CLI Surface

## Scope

This design changes only the Waypost repository and the `waypost` binary.

It does not add or modify:

- Agent Deck CLI commands or session behavior
- host-side adapters or process execution protocols
- external skills or prompt repositories
- cross-repository configuration

The goal is one small MCP surface for frequent agent work, with the complete
durable-state Waypost capability surface available through CLI.

## Hard-Cut Decision

Waypost MCP exposes exactly twelve tools by default. `waypost_debug` is a
thirteenth, explicitly opt-in diagnostic tool enabled only by `waypost mcp
--include-debug-tool`; there is no legacy tool set, capability manifest, or
runtime capability registry.

CLI completeness is the functional replacement. Existing MCP response shapes
and removed MCP tool names are not preserved.

## MCP Surface

The default retained tools are:

- `waypost_status`
- `waypost_bind`
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

`waypost_debug` is registered only with `waypost mcp --include-debug-tool`.

### Why they remain

`status` and `bind` own live state in the long-running MCP process. A separate
CLI process cannot repair that MCP instance. The optional `debug` tool exposes
read-only diagnostics for an explicitly requested diagnostic session.

`send`, `recv`, claim history, and `ack`/`release`/`defer` form the common
message path. Receive and these common lease transitions also interact with
the MCP active-lease tracker and renewal loop.

The two Agent Deck session tools remain because they are frequent structured
operations. The two host-neutral session tools cover the fixed Agent Deck and
Thurbox host set; they do not expose a generic lifecycle or command surface.
`session_require` owns both lookup and readiness enforcement: it returns
`not_found` for an absent target and accepts `auto_restart=false` for read-only
inspection. There is no separate resolve tool. `session_create` accepts the
optional caller-supplied opaque launch values `full_command_line` and
`thurbox_agent_key`; after host selection it consumes only the applicable
value and does not resolve roles or profiles.

Lease lifecycle operations stay separate. There is no synthetic `settle`
operation.

## CLI-Owned Operations

These MCP tools are deleted:

- `waypost_forward`
- `waypost_wait`
- `waypost_list`
- `waypost_read`
- `waypost_undefer`
- `waypost_fail`
- `waypost_group_create`
- `waypost_group_add_member`
- `waypost_group_remove_member`
- `waypost_group_members`
- `waypost_group_add_subscriber`
- `waypost_group_remove_subscriber`
- `waypost_group_subscribers`
- `waypost_address_inspect`

They operate on durable Waypost state and do not need live MCP binding or lease
tracker mutation when address, group, person, and state directory are explicit.

The required `forward` behavior is durable forwarding. The MCP-only target
notification side effect is not part of the replacement contract, and this
design adds no notification compatibility layer. `fail` is an exceptional
lease transition rather than a common agent operation. CLI owns it once its
structured result is complete; MCP lease tracking is reconciled from durable
delivery state rather than requiring `waypost_fail` to mutate the tracker.

`dead-letter` is likewise CLI-only. It is an explicit terminal lease
transition with no former MCP tool: it requires the current token, moves the
delivery directly to `dead_letter`, preserves `attempt_count`, and returns the
same structured transition shape as `fail`.

Before deletion:

- add group subscriber add, remove, and list CLI commands
- add structured JSON output to `undefer` and `fail`
- preserve acknowledged-message recovery through `read --latest --state acked`
- make `waypost_status` report, when `include_cli_context` is requested, the
  exact executable and resolved state directory used by the MCP process
- prove an agent can call status and use those reported values to execute a
  removed operation through CLI
- freeze success, empty-result, ordering, error, exit-code, and stream behavior
  with integration tests
- provide a relevant `waypost doc` topic

## Agent-Facing CLI Contract

Agent guidance uses one canonical `--json` path. Text and YAML remain
human-facing formats and are not part of the MCP replacement contract.

For CLI-owned operations:

- success: exit `0`, one JSON document on stdout, empty stderr
- empty collection: exit `0` with `[]` or `{"items":[]}`
- `wait` with no matching message: exit `2`, both streams empty
- failure: exit `1`, empty stdout, one JSON error document on stderr

The error document is:

```json
{
  "status": "error",
  "error_code": "not_found",
  "message": "delivery \"dlv_...\" not found",
  "retryable": false
}
```

Stable error codes are:

- `invalid_argument`: missing, malformed, or conflicting input
- `not_found`: an explicitly named object does not exist
- `already_exists`: create or add duplicates active state
- `invalid_state`: the object exists but the transition is illegal
- `integrity_error`: body metadata does not match its blob
- `busy`: transient SQLite contention
- `receive_recovery_required`: post-claim bookkeeping failed and at least one
  new lease could not be rolled back
- `internal`: unexpected storage or I/O failure

Only `busy` is retryable. An unknown code is treated as non-retryable
`internal`; the agent stops instead of guessing.

## CLI Operation Matrix

Full record names below refer to the existing exported Waypost JSON records.

| Removed MCP tool | Canonical CLI route | Success and ordering | Empty/error behavior | Doc topic |
| --- | --- | --- | --- | --- |
| `waypost_forward` | `forward (--message ID | --delivery ID) --to ADDRESS [--from ADDRESS] [--group] [--subject TEXT] --json` | Compact `ForwardResult`; exactly one source id | Missing source: `not_found`. Unknown explicit group: `not_found`. Conflicting source ids: `invalid_argument` | `mcp-cli-boundary` |
| `waypost_wait` | `wait --for ADDRESS [--as PERSON] --timeout D --json` | Personal compact delivery or group compact message; oldest eligible first | No match: exit `2`, silent. Unknown group: `not_found` | `mcp-cli-boundary` |
| `waypost_list` | `list --for ADDRESS [--state STATE | --as PERSON] --json` | Personal `ListedDelivery[]`, visible/created/id ascending; group summaries created/id ascending | Unknown personal address: `[]`. Unknown group: `not_found` | `history` |
| `waypost_read` | `read (ID... | --delivery ID... | --message ID... | --latest --for ADDRESS... [--state STATE] [--limit N]) --json`; `dlv_` IDs select deliveries | `{"items":[...]}`; direct reads preserve input order and latest reads are newest first | Missing direct id: atomic `not_found`. Unknown latest address: empty items. Blob mismatch: `integrity_error` | `recovery`, `history` |
| `waypost_undefer` | `undefer --delivery ID --json` | `delivery_id`, `state`, `visible_at`, `attempt_count` | Missing: `not_found`. Non-deferred state: `invalid_state` | `recovery` |
| `waypost_fail` | `fail --delivery ID --lease-token TOKEN --reason TEXT --json` | `delivery_id`, resulting `queued` or `dead_letter` state, `visible_at`, `attempt_count` | Missing delivery: `not_found`. Non-leased state or token mismatch: `invalid_state` | `recovery` |
| `waypost_group_create` | `group create --group ADDRESS --json` | `GroupRecord` | Existing group: `already_exists`; endpoint-owned address: `invalid_state` | `groups` |
| `waypost_group_add_member` | `group add-member --group ADDRESS --person PERSON --json` | `GroupMembershipRecord` | Missing group: `not_found`; active membership: `already_exists` | `groups` |
| `waypost_group_remove_member` | `group remove-member --group ADDRESS --person PERSON --json` | Updated `GroupMembershipRecord` | Missing group: `not_found`; no active membership: `invalid_state` | `groups` |
| `waypost_group_members` | `group members --group ADDRESS --json` | Memberships joined/id ascending, including history | Missing group: `not_found` | `groups` |
| `waypost_group_add_subscriber` | `group add-subscriber --group ADDRESS --notify-address ADDRESS --person PERSON --json` | `GroupNotificationSubscriberRecord` | Missing group: `not_found`; active subscriber: `already_exists` | `groups` |
| `waypost_group_remove_subscriber` | `group remove-subscriber --group ADDRESS --notify-address ADDRESS --json` | Updated subscriber record | Missing group: `not_found`; no active subscriber: `invalid_state` | `groups` |
| `waypost_group_subscribers` | `group subscribers --group ADDRESS --json` | Active subscribers created/id ascending | Missing group: `not_found` | `groups` |
| `waypost_address_inspect` | `address inspect --address ADDRESS --json` | `AddressInspection`; unbound is a successful `kind: "unbound"` result | Malformed address: `invalid_argument` | `diagnostics` |

The additive CLI-only route is `dead-letter --delivery ID --lease-token TOKEN
--reason TEXT --json`. It returns the delivery in `dead_letter` with its
unchanged `attempt_count`; missing deliveries report `not_found`, while a
non-leased delivery or token mismatch reports `invalid_state`. Its doc topic is
`dead-letter`.

## MCP Registration

Registration remains explicit and typed. There is no metadata-driven
dispatcher or capability table.

Conceptually:

```go
func registerWaypostTools(server *mcp.Server) {
    registerStatusBind(server)
    if includeDebugTool {
        registerDebug(server)
    }
    registerMessagePath(server)
    registerLeaseLifecycle(server)
    registerGenericSessionTools(server)
    registerAgentDeckSessionTools(server)
}
```

Tests assert the exact twelve default tool names and the thirteen names when
the debug flag is enabled. A removed tool appearing in either MCP list is a
test failure.

Default `waypost_status` returns compact operational and binding state:

```json
{
  "status": "ready",
  "bound_addresses": ["ADDRESS"],
  "default_sender": "ADDRESS"
}
```

`active_lease_count` is added only when non-zero (or when active lease detail
is requested). `include_cli_context: true` adds `executable` and
`resolved_state_dir` for CLI-owned operations.
`include_diagnostics: true` adds version and session-detection fields.
`include_active_leases: true` adds paginated lease details and tokens; `limit`
and `cursor` apply only to that opt-in detail:

```json
{
  "active_leases": [
    {
      "delivery_id": "dlv_...",
      "recipient_address": "ADDRESS",
      "lease_token": "lease_...",
      "last_renewed_at": "RFC3339 or null"
    }
  ]
}
```

An agent invoking CLI uses that executable and state directory and passes
address, group, and person explicitly. It does not guess from `PATH` or an
unrelated Waypost installation. How an MCP host executes a local process is
outside this design.

The MCP server instructions are themselves a concise agent prompt:

```text
Once after this MCP server starts, call waypost_status before the first
waypost_* tool. It auto-binds detectable session addresses and reports
warnings.
This server automatically renews leases for personal deliveries claimed by
waypost_recv until it stops or restarts.
Waypost is for durable asynchronous work, not real-time communication. MCP
covers common operations. For complete Waypost guidance:
  <executable> doc
  <executable> doc --list
  <executable> doc <topic>...
Use the reported executable and resolved_state_dir for stateful CLI commands;
never guess either.
```

When `--include-debug-tool` is enabled, the first line instead exempts
`waypost_debug`, which remains callable without the status bootstrap.

This adds no command catalog to the MCP instructions. `waypost doc` owns the
complete workflow prompt, while MCP instructions only identify that entry
point and the authoritative binary and state directory.

MCP sender identity is session-scoped: an explicit `from_address` for
`waypost_send` must be one of the personal addresses currently bound to that
MCP server, and an explicit `default_sender` in `waypost_bind` must be in the
same bound set. This differs from the standalone CLI, where `--from` may create
an address on first use.

## Receive Contract

`recv.has_more` is deleted from MCP and CLI results. No legacy serializer,
alias, schema selector, or fallback remains.

Waypost continues to support concurrent receive and CLI batch receive. MCP
receive claims at most one delivery per call; atomic claim behavior decides
which concurrent caller receives work.

### MCP personal receive

Received:

```json
{
  "status": "received",
  "delivery": {
    "delivery_id": "dlv_...",
    "recipient_address": "ADDRESS",
    "lease_token": "lease_...",
    "subject": "subject",
    "content_type": "text/plain",
    "body": "body"
  }
}
```

When the post-receive snapshot contains one or more queued deliveries, the
default MCP result also includes `notice: "more_messages_available"`. This is
an informational cue for agents to call `waypost_recv` again; the next call
still determines whether a queued delivery is currently visible and claimable.

No message:

```json
{
  "status": "no_message"
}
```

The same `notice` cue is included on `no_message` when queued deliveries
remain in the mailbox snapshot.

After a `no_message` result, the same MCP connection must wait at least 15
seconds before calling `waypost_recv` again. A call inside that window is
rejected with an instruction to wait for a delivery notification rather than
a retry time, so callers do not treat it as a polling schedule. If the next
result is also `no_message` within three minutes, the response includes a
warning to avoid meaningless polling.

Active leases:

```json
{
  "status": "active_leases",
  "active_lease_count": 1,
  "claimed_delivery_ids": ["dlv_..."]
}
```

Set `diagnostics: true` to add resolved `addresses` and sparse
`remaining_by_state`. Warnings remain present only when actionable. Returned
input echoes, derivable counts, tool-name pointers, and repeated usage prose
are not part of the response contract.

### CLI personal receive

Single JSON receive uses the same `status`, `addresses`, `delivery`, and
`remaining_by_state` fields as MCP.

Batch receive replaces `delivery` with ordered `deliveries`. Each element
uses the same compact delivery fields as single receive.

```json
{
  "status": "received",
  "addresses": ["ADDRESS_A", "ADDRESS_B"],
  "deliveries": [
    {"delivery_id": "dlv_1", "recipient_address": "ADDRESS_A"},
    {"delivery_id": "dlv_2", "recipient_address": "ADDRESS_B"}
  ],
  "remaining_by_state": {"queued": 3}
}
```

Batch order is `visible_at`, message creation time, then delivery id,
ascending.

CLI no-message returns exit `2`, empty stderr, and:

```json
{
  "status": "no_message",
  "addresses": ["ADDRESS"],
  "remaining_by_state": {"queued": 2}
}
```

### Remaining-state semantics

- use the same resolved personal address scope as the receive call
- exclude every delivery returned by the current call
- exclude `acked`
- possible keys are `queued`, `leased`, and `dead_letter`
- future-visible deferred deliveries remain `queued`
- omit zero-valued keys
- omit the whole map when all counts are zero
- include the map on MCP `received`, `no_message`, and `active_leases` only
  when `diagnostics` is requested
- include the map on CLI `received` and `no_message`
- never interpret `queued` as claimable-now work; only `recv` or `wait`
  answers availability

The count is an informational snapshot immediately after the receive-path
decision and any claims. Concurrent sends and state transitions may change it
immediately.

Use one grouped `COUNT(*)` query over resolved endpoint ids and unfinished
states, excluding returned ids. It must not read message rows or body blobs.

### Active-lease reconciliation

The in-memory active-lease tracker is a cache; durable delivery state is
authoritative.

One internal `reconcileTrackedLeases` operation owns this rule. Renewal, the
`recv` active-lease gate, and `waypost_claim_history` all call it instead of
re-implementing durable-state checks.

For each tracked lease it reads the current delivery state and lease token. The
lease remains active only when the delivery is still `leased` with the same
token. A missing delivery, any non-leased state, or a changed token removes the
entry from the active set and clears the tracked token before any response is
serialized. Renewal calls `Renew` only for entries that pass reconciliation.
The atomic predicates in `Renew` remain necessary to close the race between
inspection and update.

Reconciliation updates the history entry as follows:

- a non-leased delivery uses its exact durable state, such as `queued`, `acked`,
  or `dead_letter`, as the terminal history status for that prior claim
- `terminal_at` uses the durable state-transition event time when available;
  otherwise it is the reconciliation observation time
- a missing delivery uses `missing`; a still-leased delivery with a different
  token uses `lease_replaced`
- terminal and replaced entries never return the old lease token, including a
  targeted `recover_lease_token` request

The reconciliation result and history snapshot are applied under the tracker
lock, so a response cannot race with another local tracker update. An
inspection error keeps the entry for a later retry but skips renewal. `recv` and
claim history return the inspection error instead of presenting stale memory as
authoritative.

This is the root-cause fix that allows `waypost_fail` and the CLI-only
`dead-letter` command to operate without an MCP-specific tracker mutation path.
A durable terminal transition observed immediately by recv, claim history, or
the renewal loop ends MCP ownership and is never renewed.

### Post-claim count failure

A receive call must not hide a newly claimed lease behind an ordinary error.

After successful claims:

1. run the remaining-state query
2. if it fails, release every new claim back to `queued` at its pre-claim
   `visible_at`
3. if every release succeeds, return the original count error
4. if any release fails, return only the unreleased claims in an explicit
   recovery result

Each unreleased claim contains `delivery_id`, `lease_token`,
`recipient_address`, and `lease_expires_at`.

MCP tracks and renews every unreleased claim before returning:

```json
{
  "status": "receive_recovery_required",
  "message": "remaining-state query failed and claim rollback was incomplete",
  "claims": [
    {
      "delivery_id": "dlv_...",
      "lease_token": "lease_...",
      "recipient_address": "ADDRESS",
      "lease_expires_at": "RFC3339"
    }
  ]
}
```

With `diagnostics`, MCP also returns `addresses` and
`remaining_by_state_status: "unavailable"`. Recovery instructions live in the
tool description rather than being repeated as tool-name fields in every
response.

CLI returns exit `1`, empty stdout, and the normal JSON error document with:

```json
{
  "status": "error",
  "error_code": "receive_recovery_required",
  "message": "remaining-state query failed and claim rollback was incomplete",
  "retryable": false,
  "details": {
    "remaining_by_state_status": "unavailable",
    "claims": [
      {
        "delivery_id": "dlv_...",
        "lease_token": "lease_...",
        "recipient_address": "ADDRESS",
        "lease_expires_at": "RFC3339"
      }
    ]
  }
}
```

The caller releases every listed claim before another receive. It must not ack,
defer, or fail a claim without the missing message context.

When details are requested, `remaining_by_state` omission means zero only for
normal receive statuses. Detailed recovery results explicitly report
`remaining_by_state_status: "unavailable"`.

### Group receive

Group receive marks one group message read for one person and does not use
personal delivery states.

MCP returns `status` plus `message` when received; `diagnostics` adds the
resolved `addresses` and `as_person`. CLI retains its `status`, `addresses`,
`as_person`, and `message` contract. CLI no-message returns exit `2` with a
`status: "no_message"` JSON document.

The default MCP group message keeps `message_id`, sender/forwarding identity
when present, `subject`, `content_type`, and `body`. `diagnostics` restores
group identity, creation/read timestamps, and eligibility/read counts.

Group receive never returns `remaining_by_state`, `has_more`, or another
remaining-count field.

### Count-query performance

Two checks prevent an unbounded receive-path regression:

1. `EXPLAIN QUERY PLAN` must use
   `idx_deliveries_recipient_state_visible` and must not scan deliveries.
2. A dedicated Linux amd64 benchmark seeds 100 addresses and 100,000
   deliveries: 50,000 visible queued, 10,000 future-visible queued, 20,000
   leased, 10,000 dead-letter, and 10,000 acked. It runs 30 interleaved
   count-free/count-enabled pairs on independent fixture copies. Regression
   fails when count-enabled p95 is both more than 25% and more than 2 ms slower.

The benchmark records SQLite version, pragmas, CPU, storage class, and raw
samples. The performance threshold is blocking only on the declared benchmark
runner; the query-plan assertion is blocking everywhere.

## Read Pagination

Structured `read` always returns `items`.

- latest read emits `has_more: true` only when another matching item exists
  beyond the requested limit
- false is omitted
- direct message-id and delivery-id reads omit the field

## `waypost doc` Agent Prompts

`docs/cli.md` remains a human/operator manual and is never loaded wholesale
as an agent prompt.

Add:

```text
waypost doc
waypost doc --list
waypost doc <topic>...
```

Initial topics:

- `mcp-cli-boundary`
- `recovery`
- `history`
- `groups`
- `addresses`
- `diagnostics`
- `dead-letter`

The doc command accepts explicit command-shaped aliases for these canonical
topics and reports the canonical topic list with an unknown-topic error. It
does not use edit-distance or other open-ended fuzzy matching.

Bare `waypost doc` prints a minimal, client-neutral semantic overview. It
explains durable state, personal leases, per-person group reads, and the
separation between persistence and notification. It does not assume MCP is
available, prescribe an output format, or duplicate the command catalog. It is
the starting prompt, not another listed topic.

Do not create one topic per command. Topics exist only when workflow guidance
beyond `--help` is needed.

### Content rules

The prompt is client-neutral. It explains Waypost semantics that cannot be
recovered from command syntax alone:

- state-directory isolation
- personal delivery states, lease ownership, and state transitions
- message identity versus delivery identity
- group eligibility, per-person reads, and notification subscriptions
- process-local MCP state, but only in the explicitly selected MCP/CLI topic

It does not:

- assume MCP exists or prescribe MCP as the normal path
- prescribe JSON, YAML, or another output format
- copy the command or flag catalog from `--help`
- include role policy, repository workflow, or unrelated host configuration

### Prompt shape

Topics use short paragraphs without repeating the selected topic as a heading.
Each topic is at most 100 words and contains no command examples unless syntax
itself carries semantics unavailable from `--help`.

Topic responsibilities:

- `mcp-cli-boundary`: shared durable state versus process-local MCP state
- `recovery`: lease expiry, token ownership, persisted context, and undefer
- `history`: message identity, delivery identity, non-mutating reads, forwarding
- `groups`: eligibility, per-person reads, membership, and notifications
- `diagnostics`: durable address kind versus live MCP binding

## Risks

- An MCP tool could be removed before its CLI path is complete. Registration
  and CLI replacement tests guard that boundary.
- Prompt text can drift from CLI mechanics. Embedding prompts in the binary and
  testing their commands keeps them version-matched.
- Remaining-state counting adds receive-path work. The index, query-plan test,
  rollback invariant, and benchmark bound the risk.
- Thirteen tools are not the theoretical minimum, but each retained tool is
  justified by frequency or live MCP state.

## Rejected Alternatives

### Runtime profiles

Rejected. There is one intended MCP surface, so `full|hybrid` creates a legacy
mode and rollout machinery with no product value.

### Public capabilities command or internal capability registry

Rejected. Tool registration is a small explicit typed list. Tests, not runtime
metadata, prove CLI completeness and the exact MCP surface.

### Keep every CLI command as MCP

Rejected because uncommon schemas and choices consume agent context on every
turn.

### Move every lease completion to CLI

Rejected because `ack`, `release`, and `defer` are common message-path
operations. Exceptional `fail` and `dead-letter` are CLI-owned; durable-state
reconciliation keeps the MCP tracker and renewal loop correct without exposing
them as tools.

### Move Agent Deck session tools to CLI

Rejected because they are frequent structured operations and are explicitly
retained.

### Generic `waypost_command` MCP tool

Rejected because it hides the same broad surface behind weaker validation.

### Use `docs/cli.md` as the prompt

Rejected because it mixes operator setup, every command, multiple output modes,
and human-oriented detail.
