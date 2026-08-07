# Tether Architecture

This document describes Tether `0.3.0-beta.1`, binding protocol 3, database
schema 15, and broker protocol 6. The implementation is authoritative:
[`runtime/bridge_runtime.py`](../runtime/bridge_runtime.py),
[`runtime/plugin/__init__.py`](../runtime/plugin/__init__.py), and
[`runtime/routing.py`](../runtime/routing.py).

## Scope

Tether connects one Slack thread to one continuation endpoint:

- a verified Zellij pane;
- a verified Herdr agent;
- a detached Codex or Claude Code session;
- a Hermes session; or
- an explicit headless run.

Tether owns admission, routing, durable delivery state, and Slack writes. It
does not own agent reasoning, tool authorization, or host sandboxing.

## Components

| Component | Authority |
| --- | --- |
| Pure router | Chooses `SILENT`, `HERMES`, or `NATIVE` from normalized identity, mention, authorization, thread, binding, and participation facts. |
| Hermes plugin | Receives Slack Events API callbacks through Socket Mode, normalizes events, resolves Slack identity, persists ingress, and dispatches the selected writer. |
| SQLite Store | Owns schema, bindings, generations, ingress leases, event queues, attempts, outboxes, reconciliation, polling cursors, and retention. |
| Local broker | Accepts same-UID Unix-socket requests, performs Slack writes, advances reconciliation, and exposes diagnostics and operator recovery. |
| Native runtime | Verifies working directories and processes, resumes Codex or Claude Code, and delivers to Herdr or Zellij. |
| Installer | Stages and snapshots managed files, records plugin state, and performs crash-recoverable install, upgrade, rollback, and uninstall. |

## Runtime topology

```text
Slack Events API through Hermes Socket Mode
  -> normalize workspace, channel, message, actor, and mentions
  -> pure routing decision
  -> durable thread_ingress claim
       -> HERMES: original adapter -> gateway
       -> NATIVE: atomic transfer -> bridge_events
                                  -> generation-bound attempt
                                  -> Herdr, Zellij, or detached CLI

Local CLI or continued agent
  -> owner-only Unix socket
  -> broker
  -> durable root/reply/text-post/text-edit outbox
  -> Slack API

Broker recovery
  -> reconcile uncertain Slack writes
  -> retry safe pending writes
  -> retention maintenance
```

The broker holds a singleton lock for one database until its recovery and
request-delivery work has stopped. This prevents two local broker processes
from writing the same outbox concurrently.

Local broker requests are limited to 1 MiB. Responses are limited to 8 MiB so
the documented 100-message thread read fits without leaving the protocol
unbounded. Invalid or oversized response values fail closed as a typed broker
error.

## Binding lifecycle

```text
create
  -> pending, generation 1
       -> root accepted or explicit attach
       -> active
            -> rebind: active, generation + 1
            -> close: closed, generation + 1
```

`Store.create` is idempotent only when an idempotency key is reused with the
same source, owner, workspace, channel, and requested thread. A logical native
endpoint may have only one pending or active bridge in one database.

### Activation

A normal notification first reserves a root outbox. The bridge becomes active
only after Slack's accepted root timestamp is recorded. `tether attach` binds
an existing thread directly.

The distinction affects ambient routing:

- **Tether-created root:** the Store records that the root outbox created the
  accepted thread. That durable fact establishes ambient ownership without a
  Slack history read.
- **Existing-thread attachment:** the bridge did not create the root and does
  not gain ambient ownership. An unmentioned reply fails closed unless other
  routing evidence establishes ownership.

See `Store.owns_thread_root` and
[`tests/test_routing_plugin.py`](../tests/test_routing_plugin.py).

### Generation fencing

The binding generation is copied into admitted ingress, native events, and
delivery attempts. Rebind and close compare the caller's expected generation
with the active generation and reject stale callers.

Rebind is blocked by active attempts, queued or active delivery work, and
claimed or uncertain ingress. A successful rebind changes the endpoint,
increments the generation, and moves still-queued events to the new
generation.

Close applies the same fence. It also rejects an incomplete root outbox and
releases thread participation and endpoint ownership only after the bridge is
safe to close.

Primary evidence:
[`tests/test_bridge_lifecycle.py`](../tests/test_bridge_lifecycle.py) and
[`tests/test_attempt_recovery.py`](../tests/test_attempt_recovery.py).

## One-writer routing

The router returns one action and one writer ID. Its precedence is:

1. A mention of another bot without this bot makes this instance silent.
2. Each explicitly named bot may handle a multi-bot mention in its own app.
3. A trusted peer bot must explicitly mention this bot.
4. An authorized human may explicitly address this bot.
5. An ambient human reply requires a direct message, an exact active owned
   binding, or one fresh and unambiguous participation owner.
6. Unresolved or competing identity fails closed.

The plugin persists the selected action, writer ID, bridge ID, generation, and
normalized payload in `thread_ingress` before dispatch. The composite Slack
workspace/channel/message identity is the deduplication key. A lease ID plus a
monotonic fence epoch prevents an expired ingress worker from completing a
newer claim.

Native admission and insertion into `bridge_events` occur in one SQLite
transaction. Hermes admission marks the boundary between safe pre-dispatch
replay and uncertain post-dispatch outcome.

One-writer semantics are local to one Slack app, one Tether database, and one
Unix account. Separate installations do not run distributed consensus.

## Ingress and attempts

### Hermes ingress

```text
processing
  -> pending       failure before gateway dispatch; locally replayable
  -> dispatched
       -> completed
       -> uncertain  dispatch may have run; never replay automatically
  -> transferred   native queue insert committed
  -> cancelled
```

The plugin stores enough normalized data to reconstruct pre-dispatch Hermes
events. It renews ingress leases and replays only `pending` or expired
`processing` records after backoff.

### Native attempts

The normal path is:

```text
queued -> processing -> prepared -> submitting
       -> awaiting_ack -> replying -> delivered
```

One bridge may have only one open attempt. Its deterministic attempt/reply key
binds the bridge, ordered event IDs, and generation.

- A proven pre-I/O failure requeues the event.
- A submission that may have started becomes `uncertain`.
- The exact reply key acknowledges the attempt or stages one immutable reply.
- `NO_REPLY` acknowledges without posting.
- A verified Herdr or Zellij cancel targets and closes the exact attempt.

Unknown, wrong-bridge, or stale-generation reply keys cannot post or suppress a
reply.

## Durable Slack outboxes

Tether records outbound state before network I/O:

| Outbox | Durable identity |
| --- | --- |
| Root | Bridge ID, immutable redacted payload, deterministic `client_msg_id`, optional requested thread |
| Native reply | Exact attempt/reply key, immutable payload and hash, deterministic `client_msg_id` |
| Generic thread reply | Caller idempotency key bound to workspace, channel, thread, payload, hash, and deterministic `client_msg_id` |
| Hermes text post | Ingress identity, per-turn sequence, immutable chunks, and deterministic `client_msg_id` |
| Hermes text edit | Ingress identity, per-turn sequence, immutable payload, and exact target message |

A lease selects one network writer. Reusing a key with a changed payload or
destination fails closed.

If Slack may have accepted a write before the local commit, Tether records the
outcome as uncertain. Durable reconciliation searches paginated history for the
exact Tether metadata and client identity before retrying. Reconciliation
persists its cursor, seen cursors, page count, lower time bound, pacing, and
terminal result.

Each reconciliation read is limited to 15 messages. The Store coordinates one
history page per workspace and Slack method every 60 seconds and stops after
1,000 pages or an invalid cursor. These are recovery bounds, not throughput
promises.

Slack remains an external system. Tether reduces duplicates but provides
at-least-once recovery, not formal exactly-once delivery.

Ephemeral messages and Hermes native media APIs are outside the durable
outbox. Tether redacts their text and stages local files through the same
attachment guard, but delivery remains best-effort. Root file uploads sent by
the Tether CLI use a separate durable, restart-safe upload state machine.

## Slack ingress and recovery

Slack Events API callbacks delivered by Hermes Socket Mode are the
authoritative ingress path. They enter the same normalization, authorization,
routing, and durable ingress ledger used by recovery.

The `conversations.replies` poller is best-effort recovery for recent active or
participating threads. It:

- persists target rotation and page cursors;
- retains normalized replies durably until all pages establish complete
  thread-ownership evidence;
- reads one 15-message page at a time;
- shares the Store's per-workspace/method read budget with reconciliation;
- advances a cursor only after fetched messages are handled; and
- deduplicates recovered messages through `thread_ingress`.

Polling is not an authoritative or guaranteed ingress service. Slack may
rate-limit it, and bot tokens may not be able to read channel threads depending
on token type, scopes, and channel membership. Tether-created root ownership
does not require polling. Existing-thread attachments and participation-based
ambient routing fail closed when complete ownership evidence is unavailable.

Reference:
[Slack `conversations.replies`](https://docs.slack.dev/reference/methods/conversations.replies/).

## Native continuation

### Working directory

A native binding stores the real path, device, inode, and owner UID. Delivery
reopens and revalidates that directory, then uses a pinned descriptor as the
child working directory. Replacing the path does not redirect execution.

### Zellij

The captured process identity includes the host boot ID, PID and start ticks,
executable identity and path hash, controlling terminal, Zellij session and
pane, and agent adapter.

Delivery:

1. verifies the current foreground process;
2. writes the request to an owner-only inbox file;
3. stages an instruction with `zellij write-chars`;
4. verifies the marker is visible;
5. rechecks process identity immediately before Enter;
6. sends Enter; and
7. checks the marker and process identity again.

This narrows but cannot remove the terminal time-of-check/time-of-use window.
The process can change between the final check and Enter, and screen state
cannot prove semantic consumption. Any such ambiguous outcome becomes
`uncertain`; Tether does not retry it blindly.

### Herdr

The captured endpoint includes a private mode-`0600` same-user Unix socket,
protocol 19, terminal and pane IDs, an occupant-bound agent name, Herdr's
official Codex or Claude session reference, and the foreground process
incarnation. The native session reference must equal the Tether source session.

Delivery revalidates those fields, submits one prompt through `agent.prompt`
over NDJSON, checks the returned agent, and revalidates the endpoint afterward.
The prompt is transported in the socket body rather than process arguments.
The occupant-bound name disappears when the terminal's agent is replaced, so a
replacement cannot inherit the old delivery capability.

Herdr does not currently expose an expected-revision precondition or a durable
per-prompt turn ID. Therefore a missing response, unknown server error, or
failed post-submit verification is `uncertain` and is never retried
automatically. An explicit `agent_not_found` response proves submission did not
start and is safe to requeue.

The `parcha.tether` Herdr plugin is outside this authority boundary. It
provides manifest actions, a link handler, and a terminal popup, then calls the
local Tether CLI. Its invocation context is revalidated against Herdr and the
broker. Slack credentials, routing decisions, durable queues, and delivery
attempts remain in Hermes and Tether.

### Detached native process

Detached Codex or Claude Code continuation uses the configured binary and
resume arguments, a pinned working directory, a new process group, a timeout,
bounded output, and an allowlisted child environment. Its response is persisted
before the worker returns.

## Text transport

CLI message text should use `--text-stdin` or `--text-fd FD`. The JavaScript CLI
forwards text to the Python notifier over standard input. Deprecated `--text`
places content in process arguments and should not be used.

The configured word, character, and sentence values are soft writing targets.
The runtime does not reject a useful reply for exceeding them. The hard text
transport limit is `MAX_TEXT`, currently 35,000 characters.

## Schema and upgrades

The current SQLite schema is 15. Store startup:

1. rejects a database with a newer schema;
2. opens an immediate transaction;
3. applies additive migrations and binding backfills;
4. writes `PRAGMA user_version=15`; and
5. recovers safe pre-I/O attempts.

Legacy or incomplete native bindings become `rebind_required`. Migration also
closes older duplicate owners before enforcing unique active endpoint
ownership.

The installer snapshots managed files and plugin state, not the database. A
code rollback does not downgrade schema 15. Back up `bridges.db` and its WAL/SHM
sidecars before crossing a schema boundary.

## Operator recovery

List uncertain ingress and native attempts:

```bash
tether unresolved --team T12345678
```

After inspecting the exact Slack thread and bound session, apply one explicit
resolution:

```bash
tether resolve \
  --team T12345678 \
  --kind ingress \
  --id slack:T12345678:C12345678:1234567890.123456 \
  --action complete
```

Actions:

| Action | Ingress | Native attempt |
| --- | --- | --- |
| `retry` | Returns uncertain Hermes ingress to `pending`; native ingress must be resolved through its attempt | Requeues its events |
| `complete` | Marks ingress completed | Marks attempt acknowledged and events delivered |
| `abandon` | Marks ingress cancelled | Cancels the attempt and fails its events |

Resolution is workspace-bound and idempotent for the same terminal choice.
Rebind and close remain blocked until related uncertain work is resolved.

## Install and rollback

Install and upgrade validate managed roots, take an owner-only lifecycle lock,
stage a complete payload, snapshot managed files and Hermes plugin state, and
commit through atomic renames. A durable transaction journal lets the next
lifecycle command recover an interrupted commit. A requested gateway restart
failure triggers restoration of the previous managed state.

Rollback restores the immediately previous managed files and recorded plugin
state. It does not downgrade the database or restore Slack app settings.
Uninstall preserves config, database state, snapshots, and locally modified
managed files.

See [Operations](OPERATIONS.md).

## Known limitations

- Slack is the only chat adapter.
- Native continuation and peer credential checks are Linux-specific.
- Same-UID processes share the broker authority boundary.
- Endpoint uniqueness and writer election are not distributed across hosts.
- Slack and terminal I/O retain ambiguity windows.
- Polling and reconciliation may recover slowly under Slack limits.
- State is owner-only but not encrypted by Tether.
- Secret redaction and upload scanning are pattern-based, not semantic DLP.
- Slack ephemeral notices and native media uploads are best-effort.
- File rollback does not reverse database migrations or Slack configuration.

## Verification map

| Property | Tests |
| --- | --- |
| Binding ownership and generations | [`tests/test_bridge_lifecycle.py`](../tests/test_bridge_lifecycle.py) |
| Attempts, ambiguity, and cancellation | [`tests/test_attempt_recovery.py`](../tests/test_attempt_recovery.py), [`tests/test_herdr_endpoint.py`](../tests/test_herdr_endpoint.py), [`tests/test_zellij_cancellation.py`](../tests/test_zellij_cancellation.py) |
| Routing and thread ownership | [`tests/test_routing.py`](../tests/test_routing.py), [`tests/test_routing_plugin.py`](../tests/test_routing_plugin.py) |
| Outboxes and reconciliation | [`tests/test_outbox_recovery.py`](../tests/test_outbox_recovery.py), [`tests/test_outbox_process_safety.py`](../tests/test_outbox_process_safety.py), [`tests/test_generic_outbox.py`](../tests/test_generic_outbox.py), [`tests/test_slack_plugin_protocol.py`](../tests/test_slack_plugin_protocol.py) |
| Poll cursor and rate coordination | [`tests/test_poll_state_recovery.py`](../tests/test_poll_state_recovery.py), [`tests/test_reconciliation_recovery.py`](../tests/test_reconciliation_recovery.py) |
| Operator resolution | [`tests/test_operator_recovery.py`](../tests/test_operator_recovery.py), [`tests/test_cli.py`](../tests/test_cli.py) |
| Process and cwd verification | [`tests/test_process_identity_hardening.py`](../tests/test_process_identity_hardening.py), [`tests/test_cwd_identity.py`](../tests/test_cwd_identity.py), [`tests/test_native_delivery_safety.py`](../tests/test_native_delivery_safety.py) |
| Installer lifecycle | [`tests/test_release_install.sh`](../tests/test_release_install.sh) |
