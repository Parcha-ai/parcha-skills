# Upstream proposal 1 — record plugin-path sends in the delivery ledger and return a receipt

**Target:** NousResearch/hermes-agent · one PR · branch `feat/plugin-send-delivery-ledger`
**Consumer (stated, per AGENTS.md anti-speculation rule):** Tether, a Slack-continuation plugin
(Parcha-ai/parcha-skills) that binds Slack threads to long-running local agent sessions and must
prove to an operator that a reply was durably handed to the platform.

## Problem (current main, `6575507`)

`gateway/delivery_ledger.py` gives the gateway's **final agent response** honest at-least-once
delivery: obligation row before I/O, `attempting`/`delivered`/`failed` transitions, restart
sweep with a visible recovered marker. But the **plugin egress path is completely outside it**:

- `ctx.dispatch_tool("send_message", ...)` → `tools/send_message_tool.py::_send_via_adapter`
  (`:830-922`) → `SlackAdapter.send` → `chat_postMessage` — zero `delivery_ledger` imports on
  this path (only call sites are `gateway/platforms/base.py:6578,6616` and
  `gateway/run.py:11820`).
- The tool result does not return the platform message id, so a plugin cannot even correlate
  its send with a later edit/delete/repair.

Any plugin that must not silently lose a message is forced to build a private outbox — exactly
the duplication `delivery_obligations` exists to prevent.

## Proposal (smallest change, no new subsystem)

1. In `_handle_send` (`tools/send_message_tool.py`), when the target resolves to a gateway-
   ledgered platform adapter and `ledger_enabled()`: `record_obligation()` before I/O,
   `mark_attempting()`, then `mark_delivered()`/`mark_failed()` around the existing send —
   the same three calls `base.py:6573-6632` already makes, no new states, no new table.
2. Add `obligation_id` to the send_message success result when ledgered (additive; the
   platform message id is already returned as `message_id`, so no external-id change is
   needed — verified against current main).
3. Restart sweep behavior is inherited unchanged, including the visible RECOVERED_MARKER —
   this deliberately does NOT reintroduce a silent outbox (#61790); plugin sends get the same
   honest at-least-once contract final responses already have.

Out of scope, deliberately: idempotency keys on the wire, watch/reconcile APIs, media paths,
read-state — nothing beyond parity between the two egress paths.

## Tests

- E2E against a temp HERMES_HOME with a fake adapter: obligation row exists before the fake
  send runs; `delivered` after success; `failed` + sweep redelivery with marker after injected
  failure; result carries `external_id`/`obligation_id`.
- Behavior-contract test: a frozen plugin calling `dispatch_tool("send_message", ...)` through
  the real discovery path observes an additive result (old callers unaffected).
- No change-detector tests; no reading source in tests.

## Compatibility

Additive only: no signature changes, result dict gains optional keys, ledger participation is
behind the existing `gateway.delivery_ledger` gate (default on). Conventional commit:
`feat(gateway,tools): record plugin-path sends in the delivery ledger and return receipts`.

## Status (2026-08-22)

**Implemented and tested** on `feat/plugin-send-delivery-ledger` from hermes-agent main
`261a4efb9`. Patch: `0001-plugin-send-delivery-ledger.patch` (this directory). Local evidence:
6 new tests (obligation durably `attempting` before adapter I/O, `delivered`/`failed`
transitions with error text, config gate honored, ledger trouble never blocks the send,
identical-resend idempotency by obligation id) plus 68 send_message tests and 24 delivery-ledger
tests green (pre-existing environmental collection error in test_clipboard.py excluded,
reproduced on pristine main). Filing blocked on credentials: the claudio-michel GitHub App
cannot fork or PR outside its installations — apply the patch on a fork from any user account,
push branch `feat/plugin-send-delivery-ledger`, and open the PR with this document's Problem /
Proposal / Tests sections as the body.
