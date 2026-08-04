# Tether Security Model

This document describes the security boundary of Tether `0.2.0-beta.1`,
binding protocol 2, and database schema 15. The implementation and tests are
authoritative.

## Scope

Tether connects an authorized Slack message to one Hermes or native
continuation and returns the result to Slack. It protects:

- Slack admission and writer selection;
- binding and endpoint identity;
- local broker access;
- durable ingress, delivery attempts, and Slack outboxes;
- native process and working-directory selection; and
- file installation and rollback.

Tether does not sandbox the agent, grant tool permissions, isolate processes
that share a Unix UID, or provide formal exactly-once delivery across Slack or
a terminal.

## Trust boundaries

```text
Slack user or peer bot
  -> Slack Events API / Hermes Socket Mode
  -> allowlists, identity resolution, pure routing
  -> durable ingress and selected writer
       -> Hermes gateway
       -> native attempt and verified endpoint

Local CLI
  -> owner-only Unix socket with peer-UID check
  -> broker
  -> durable Slack outbox
  -> Slack API
```

The intended deployment has one dedicated, non-root Unix account for Hermes
and Tether. Processes under that UID are mutually trusted. Put mutually
untrusted agents in separate accounts or hosts.

The local SQLite database is the coordination boundary. One-writer routing and
endpoint uniqueness are not distributed across databases, hosts, or unrelated
Slack integrations.

## Principals

| Principal | Accepted authority | Not implied |
| --- | --- | --- |
| Authorized human | Address an allowed bot and continue an eligible thread | Channel membership alone |
| Trusted peer bot | Address this bot when explicitly mentioned | Ambient bot messages |
| Hermes gateway | Hold Slack credentials and dispatch admitted Hermes turns | Root or Docker authority |
| Broker peer | Request operations as the broker's Unix UID | Isolation from other same-UID processes |
| Bound native process | Receive one generation-bound attempt | Any process with the same command name or pane label |
| Operator | Inspect and explicitly resolve uncertain work | Permission to replay blindly |

Unknown actors, unresolved identities, competing owners, and stale generations
fail closed.

## Slack admission and ownership

Slack Events API callbacks delivered through Hermes Socket Mode are the
authoritative ingress path. Every event is normalized, authorized, routed, and
claimed in `thread_ingress` before execution.

The `conversations.replies` poller is best-effort recovery for recent active
threads. It is not an authoritative ingress service. Slack can rate-limit it,
and bot tokens may be unable to read channel threads depending on token type,
scopes, and channel membership. A healthy deployment therefore requires
working Socket Mode; polling only reduces some missed-event windows.

Thread-root ownership is deliberately narrow:

- A root posted through Tether's durable root outbox gives that bridge durable
  ownership of the accepted thread. Ambient replies can use that local proof
  without reading Slack history.
- `tether attach` binds an existing thread but does not claim that it created
  the root. Ambient replies fail closed unless another routing rule supplies
  current, unambiguous ownership evidence.

Human and peer-bot allowlists are explicit. A peer bot must explicitly mention
this bot. If another bot is mentioned and this bot is not, this instance stays
silent. Each named bot may independently handle a message that explicitly
mentions multiple bots.

Evidence:
[`runtime/routing.py`](../runtime/routing.py),
[`runtime/plugin/__init__.py`](../runtime/plugin/__init__.py), and
[`tests/test_routing_plugin.py`](../tests/test_routing_plugin.py).

## Binding and writer integrity

A binding identifies one source, one continuation endpoint, one Slack thread,
and a monotonic generation. It begins pending at generation 1 and becomes
active after a Tether-created root is accepted or an existing thread is
explicitly attached.

Rebind and close increment the generation. Admitted ingress, native events,
and attempts carry the generation they were created under, so stale workers
cannot complete work against a newer binding. Active or uncertain work blocks
rebind and close until it reaches a safe terminal state.

The pure router selects one of `SILENT`, `HERMES`, or `NATIVE` and one writer
ID. Tether persists that choice before dispatch. Native admission and queue
insertion are one SQLite transaction. A lease ID and monotonic fence epoch
prevent an expired ingress worker from committing over a newer claim.

These controls prevent two Tether paths in one installation from executing the
same admitted message. They cannot prevent an independent Slack app or a second
Tether database from acting on the same message.

Evidence:
[`tests/test_bridge_lifecycle.py`](../tests/test_bridge_lifecycle.py),
[`tests/test_attempt_recovery.py`](../tests/test_attempt_recovery.py), and
[`tests/test_routing.py`](../tests/test_routing.py).

## Durable delivery

Tether writes state before external I/O:

| Operation | Durable controls |
| --- | --- |
| Hermes ingress | Slack message deduplication key, selected writer, lease, fence epoch, dispatch state |
| Native attempt | Binding generation, ordered event IDs, exact attempt/reply key, one open attempt per bridge |
| Slack root | Immutable payload, bridge ID, deterministic `client_msg_id`, lease |
| Native reply | Exact reply key, immutable payload and hash, deterministic `client_msg_id`, lease |
| Generic reply | Caller idempotency key bound to destination and immutable payload, deterministic `client_msg_id`, lease |
| Hermes text post | Ingress identity, per-turn sequence, immutable chunks, deterministic `client_msg_id`, lease |
| Hermes text edit | Ingress identity, per-turn sequence, immutable payload, exact target message, lease |

Slack outboxes reconcile uncertain writes against paginated history using
persisted metadata, client identity, cursors, pacing, and terminal state.
Reusing an idempotency or reply key with changed text or destination fails
closed.

Slack remains external. Tether provides durable at-least-once recovery and
duplicate reduction, not formal exactly-once delivery.

Ephemeral messages and native media uploads are not in the durable text
outbox. Tether redacts their text and applies the attachment guard to local
files, but Slack offers no equivalent recoverable idempotency boundary for
those operations.

Evidence:
[`tests/test_outbox_recovery.py`](../tests/test_outbox_recovery.py),
[`tests/test_outbox_process_safety.py`](../tests/test_outbox_process_safety.py),
[`tests/test_generic_outbox.py`](../tests/test_generic_outbox.py), and
[`tests/test_reconciliation_recovery.py`](../tests/test_reconciliation_recovery.py).

## Native continuation

### Process and directory identity

A native binding records the real working-directory path, device, inode, and
owner. Delivery reopens and validates it, then starts the child from a pinned
descriptor. Replacing a path cannot redirect execution.

Zellij identity includes host boot ID, PID and start ticks, executable identity
and path hash, terminal, session, pane, and adapter. Tether re-resolves the
foreground process instead of trusting a pane label or stale PID.

Herdr identity includes a private same-user socket, protocol, terminal and pane
IDs, an occupant-bound agent name, the official native session reference, and
the same process-incarnation evidence. Tether does not trust Herdr display
titles or a reusable pane ID as authorization.

### Verified Zellij delivery

Tether writes the request to a private inbox, stages an instruction, verifies
its marker on screen, rechecks process identity immediately before Enter,
sends Enter, and verifies screen and process state afterward. Cancellation
also revalidates the exact process and attempt.

This narrows but cannot eliminate a terminal time-of-check/time-of-use window.
The foreground process can change between the last check and Enter, and a
screen marker cannot prove semantic consumption. An ambiguous submission
becomes `uncertain` and is not retried automatically.

### Verified Herdr delivery

Tether revalidates the named agent, official native session, terminal, adapter,
and process before calling Herdr `agent.prompt`, then validates the returned
agent and process again. Prompt text stays in the private socket request body.
Cancellation uses `agent.send_keys` only after the same endpoint check.

Herdr has no conditional expected-revision prompt or durable Tether turn ID.
The occupant-bound name prevents a replacement agent from inheriting the old
target, while ambiguous acceptance remains durable `uncertain` state requiring
operator resolution.

### Detached continuation

Detached Codex and Claude Code continuations use configured binaries, pinned
working directories, new process groups, bounded output, timeouts, and an
allowlisted child environment. Tether does not pass its Slack credential to
the child.

Evidence:
[`tests/test_process_identity_hardening.py`](../tests/test_process_identity_hardening.py),
[`tests/test_cwd_identity.py`](../tests/test_cwd_identity.py),
[`tests/test_native_delivery_safety.py`](../tests/test_native_delivery_safety.py),
[`tests/test_herdr_endpoint.py`](../tests/test_herdr_endpoint.py), and
[`tests/test_zellij_cancellation.py`](../tests/test_zellij_cancellation.py).

## Credentials and sensitive data

The Hermes process owns the Slack token. Local clients call an owner-only
mode-`0600` Unix socket; the broker rejects UID 0 and peers whose UID differs
from its own.

Native child environments are built from an allowlist. An optional credential
helper must be an absolute, owner-private, regular, singly linked executable;
its bounded JSON output is restricted to approved keys. The helper itself is
trusted code.

CLI text should use `--text-stdin` or `--text-fd FD`. Deprecated `--text`
places message content in process arguments, where other local diagnostics may
observe it.

The SQLite database contains Slack text and identifiers, native session
identifiers, working-directory and process metadata, outbox payloads, and
bounded errors. Files and sockets are owner-only, but Tether does not encrypt
the database. Protect backups as sensitive operational data.

Redaction and upload scanning match known credential patterns. They are not
semantic DLP. Attachments additionally require approved private roots,
owner-matching regular files, stable inode and hash checks, size limits, and
private staging. Operators remain responsible for deciding whether content is
appropriate for Slack.

Tether supports Hermes only from the exact audited Git commit and requires the
checkout to have no tracked changes, non-ignored untracked files, or ignored
Python overlays in source trees. This prevents local source overlays from
weakening the verified adapter and gateway behavior.

Evidence:
[`runtime/security.py`](../runtime/security.py),
[`tests/test_security_integration.py`](../tests/test_security_integration.py),
and [`tests/test_file_upload_protocol.py`](../tests/test_file_upload_protocol.py).

## Failure semantics

| Boundary | Proven not started | May have started |
| --- | --- | --- |
| Hermes dispatch | Return ingress to pending after backoff | Mark ingress uncertain |
| Native submission | Requeue event or attempt | Mark attempt uncertain |
| Slack write | Retain immutable pending outbox | Mark uncertain and reconcile before retry |
| Ephemeral or native media send | No durable acceptance record | Best-effort only; do not infer delivery |
| Install or upgrade | Stop before commit | Recover journal or restore snapshot |

Tether never silently routes failed native work to Hermes or another session.
Unknown outcomes remain durable and can block rebind or close.

## Operator recovery

List uncertain Hermes ingress and native attempts:

```bash
tether unresolved --team T12345678
```

Inspect the exact Slack thread and bound native session. Then record one
workspace-bound decision:

```bash
tether resolve \
  --team T12345678 \
  --kind attempt \
  --id att_example \
  --action complete
```

Actions are:

| Action | Ingress | Attempt |
| --- | --- | --- |
| `retry` | Requeues uncertain Hermes ingress; native ingress must be resolved through its attempt | Requeues its events |
| `complete` | Records dispatch as completed | Acknowledges the attempt and records its events delivered |
| `abandon` | Cancels ingress | Cancels the attempt and fails its events |

Use `retry` only after proving the original operation did not run. Use
`complete` only after proving it did. Otherwise use `abandon` and start a new
explicit operation. Repeating the same terminal resolution is idempotent;
conflicting resolution fails closed.

Evidence:
[`tests/test_operator_recovery.py`](../tests/test_operator_recovery.py) and
[`tests/test_cli.py`](../tests/test_cli.py).

## Install and schema boundary

Install and upgrade use an owner-only lifecycle lock, trusted managed roots,
complete staging, checksums, snapshots, atomic renames, and a durable
transaction journal. A failed requested gateway restart restores the prior
managed files and plugin state.

The current database schema is 15. Startup rejects a newer schema and upgrades
older supported state in one immediate transaction. Legacy or incomplete
native bindings become `rebind_required`; Tether does not guess a replacement
endpoint.

File rollback does not downgrade the database or restore Slack app settings.
Back up the database before crossing a schema boundary.

Evidence:
[`install.sh`](../install.sh),
[`tests/test_bridge.py`](../tests/test_bridge.py), and
[`tests/test_release_install.sh`](../tests/test_release_install.sh).

## Residual risks and non-goals

- Slack and terminal operations retain unavoidable ambiguity windows.
- Slack ephemeral notices and native media uploads are best-effort.
- Same-UID code can access Tether's local authority and owner-only state.
- SQLite availability and integrity are required for coordination.
- One-writer and endpoint uniqueness do not span hosts or databases.
- Polling may recover slowly or not at all under Slack limits and token
  restrictions.
- State is private by filesystem permissions, not encrypted by Tether.
- Secret detection is pattern-based and may miss sensitive business data.
- Tether does not sandbox agent tools, containers, the host, or model
  providers.
- File rollback does not reverse database migrations or external Slack
  configuration.

Report vulnerabilities through the repository
[Security Policy](../../.github/SECURITY.md), not a public issue.
