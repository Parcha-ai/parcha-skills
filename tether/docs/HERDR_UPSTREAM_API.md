# Conditional Herdr agent prompt proposal

Status: proposed additive API. Tether does not require this method to operate.

## Problem

Tether verifies a Herdr pane, its terminal, the detected agent, the official
native agent-session reference, and the foreground process before calling
`agent.prompt`. It verifies the same identity again afterward. A target can
still change between the verification and the mutation, and a lost response
cannot prove whether Herdr accepted the prompt.

Tether handles that gap by recording the delivery as `uncertain` and requiring
an explicit operator decision. It never retries an ambiguous prompt
automatically. A conditional prompt operation could make the common path both
safer and easier to reconcile.

## Proposed request

Add a new socket method without changing `agent.prompt`:

```json
{
  "id": "opaque-caller-request-id",
  "method": "agent.prompt_if_current",
  "params": {
    "pane_id": "workspace:tab:pane",
    "expected_terminal_id": "terminal-id",
    "expected_agent": "codex",
    "expected_session_kind": "codex_thread_id",
    "expected_session_value": "native-session-reference",
    "expected_process_incarnation": "opaque-host-value",
    "prompt": "message body"
  }
}
```

All `expected_*` values are preconditions. Herdr must compare them against one
atomic view of the current pane occupant before submitting the prompt. A
precondition failure must not mutate the terminal or agent.

`expected_process_incarnation` should be an opaque Herdr-issued value. It can
initially encode the operating-system process identity, but callers should not
have to understand `/proc` or platform-specific process metadata.

## Proposed receipt

```json
{
  "id": "opaque-caller-request-id",
  "ok": true,
  "result": {
    "accepted": true,
    "receipt_id": "durable-herdr-receipt-id",
    "terminal_id": "terminal-id",
    "agent_session_value": "native-session-reference"
  }
}
```

Herdr should persist the mapping from caller request ID to result for a bounded
retention window. Repeating the same request ID with the same parameters returns
the original receipt. Reusing it with different parameters returns a typed
conflict. That lets a caller reconcile a response loss without resubmitting the
prompt.

Recommended typed failures are `precondition_failed`, `request_conflict`,
`unsupported_agent`, and `receipt_expired`. Error responses should identify
which precondition class failed without returning prompt content.

## Feature discovery

Expose the method and receipt version through the existing server capability or
protocol metadata. Tether will feature-detect it at runtime:

- when available, use the conditional operation and persist its receipt;
- when absent, keep the existing verify/prompt/verify flow and `uncertain`
  handling;
- never silently downgrade during one delivery attempt after selecting the
  conditional operation.

## Security and logging

The method remains on Herdr's private same-user socket. Prompt bodies belong in
the framed request body, never command arguments or plugin logs. Receipts and
errors must not echo the prompt. Existing Herdr authorization and socket-owner
checks remain authoritative.

## Tether compatibility

This proposal is additive. It does not alter the Tether binding schema, remove
the Zellij endpoint, require the companion Herdr plugin, or move Tether's Slack
authorization and durable queues into Herdr. Tether's current fail-closed path
remains the compatibility fallback until an upstream capability is available.
