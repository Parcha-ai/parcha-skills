# Binding and delivery contract

This contract describes the persisted routing boundary. Security, installation,
retention, and operator recovery are documented in the README and Operations.

## BindingV2

Each active bridge records:

- one source: Codex session, Claude Code session, verified Zellij pane, Hermes
  session, or explicit headless run;
- one endpoint: detached native resume, verified Zellij process, or Hermes
  continuation;
- one Slack workspace, channel, and thread;
- one operator policy and idempotency key; and
- one binding generation.

Codex and Claude Code require a concrete native session ID and working
directory. A native session running in Zellij also records the session, pane,
adapter, host boot ID, PID, kernel start time, TTY ancestry, and executable
identity. A Zellij-only bridge requires equivalent process evidence.

A headless bridge exists only when the caller supplies `--run-id`. It continues
as a durable Hermes conversation after the original process exits. Ambient
agent or Zellij variables never create a headless source.

Legacy or incomplete native records become `rebind_required`. Tether never
infers missing process identity, converts a native binding to Hermes, or chooses
a replacement session.

## Routing

An inbound Slack event is normalized with its workspace, channel, thread,
message identity, actor identity, and mentions. Tether then applies:

1. configured workspace and channel policy;
2. the explicit human or trusted-peer-bot allowlist;
3. direct-message, exact-mention, or locally owned-thread routing;
4. the persisted bridge and binding generation; and
5. durable ingress deduplication and one-writer serialization.

A bot may participate only when its Slack member ID or bot ID is explicitly
trusted. Another bot's presence in a thread does not transfer ownership.
Hermes is not a second writer for a thread bound to a native session.

## Delivery

Each admitted event receives an attempt identity derived from the event, not
its text. Repeated text is therefore separate work. An attempt records the
binding generation before submission and checks it again before acknowledgment.
A rebind increments the generation and fences older work.

For a Zellij endpoint, Tether verifies the same process immediately before and
after submission. A pane ID, pane title, command string, or visible keypress is
not proof that the intended agent received the turn.

Native work returns either one useful Slack reply or exactly `NO_REPLY`.
`NO_REPLY` acknowledges the attempt without posting. Cancellation terminates
the active continuation process group when possible and records the terminal
state.

Slack posting is at least once. Stable client message IDs and a durable outbox
reduce duplicates, but ambiguous network acceptance remains possible.

## Failure behavior

Tether fails closed when:

- a workspace, channel, actor, or bot is unauthorized;
- the source or process identity is missing, changed, or ambiguous;
- the binding generation changes during delivery;
- the exact native session cannot resume;
- Hermes is incompatible;
- the local peer UID differs from the broker UID; or
- delivery acceptance cannot be determined safely.

Failures contain a typed code and next action but exclude raw session IDs,
commands, prompts, credentials, and absolute paths. Common codes include:

| Code | Required response |
| --- | --- |
| `binding_rebind_required` | Rebind from the intended live session. |
| `binding_generation_changed` | Retry against the latest verified generation. |
| `process_identity_missing` | Start the agent in the intended pane, then rebind. |
| `process_identity_changed` | Rebind; do not reuse the stale endpoint. |
| `process_identity_ambiguous` | Select one process, then rebind. |
| `adapter_pane_mismatch` | Rebind from a pane running the intended adapter. |
| `ack_timeout` | Inspect the bound session before retrying. |
| `terminal_submit_uncertain` | Inspect the session; do not submit blindly. |
| `peer_uid_mismatch` | Run the client under the broker's dedicated non-root user. |
| `broker_busy` | Retry after current local broker work finishes. |

Unknown or malformed state must not trigger a weaker fallback.
