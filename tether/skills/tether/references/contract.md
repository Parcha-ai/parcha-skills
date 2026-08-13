# Binding and delivery contract

This contract describes the persisted routing boundary. Security, installation,
retention, and operator recovery are documented in the README and Operations.

## BindingV3

Each active bridge records:

- one source: Codex session, Claude Code session, verified Zellij pane, Hermes
  session, or explicit headless run;
- one endpoint: detached native resume, verified Herdr agent, verified Zellij process, or Hermes
  continuation;
- one Slack workspace, channel, and thread;
- one operator policy and idempotency key; and
- one binding generation.

Codex and Claude Code require a concrete native session ID and working
directory. A native session running in Zellij also records the session, pane,
adapter, host boot ID, PID, kernel start time, TTY ancestry, and executable
identity. A Zellij-only bridge requires equivalent process evidence.

A native session running in Herdr records the private socket path, protocol,
terminal and pane IDs, occupant-bound agent name, Herdr's official native
session reference, and the same class of Linux process-incarnation evidence.
The official reference must equal the Tether source session ID.

A headless bridge exists only when the caller supplies `--run-id`. It continues
as a durable Hermes conversation after the original process exits. Ambient
agent, Herdr, or Zellij variables never create a headless source.

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

A Tether-created root proves local thread ownership. A trusted owner-UID
`attach` or `rebind` also records a durable thread claim fenced to the current
binding generation. That claim admits unmentioned replies only from allowlisted
humans; peer bots must still mention this bot unless an administrator granted
their exact identity and channel as an ambient automation source. Close or a
later rebind fences the old claim together with the rest of the binding
generation.

## Delivery

Each admitted event receives an attempt identity derived from the event, not
its text. Repeated text is therefore separate work. An attempt records the
binding generation before submission and checks it again before acknowledgment.
A rebind increments the generation and fences older work.

For a Zellij endpoint, Tether verifies the same process immediately before and
after submission. A pane ID, pane title, command string, or visible keypress is
not proof that the intended agent received the turn.

For a Herdr endpoint, Tether targets an occupant-bound agent name with
`agent.prompt`, not terminal keystroke staging. It verifies the private socket,
protocol, official native session, terminal, adapter, and process incarnation
before submission and rechecks them afterward. Herdr does not expose a
conditional expected-revision prompt, so a lost response or unknown server
error remains `uncertain` and is never blindly replayed.

Herdr plugin context is a targeting hint, not authority. Tether re-resolves the
focused pane through Herdr before create, attach, rebind, delivery,
cancellation, or recovery. The plugin owns no Slack credential and cannot
bypass the broker's allowlist, generation fence, or durable attempt ledger.

Native work returns either one useful Slack reply or ends with a standalone
`NO_REPLY` line. That terminal control line acknowledges the attempt without
posting the output or any preceding routing rationale. Cancellation terminates
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
