# Upstream proposal 2 — Slack fire-sites for `gateway_platform_event`

**Target:** NousResearch/hermes-agent · one PR · branch `feat/slack-gateway-platform-events`
**Consumer (stated):** Tether (Parcha-ai/parcha-skills), which must observe edits and deletions
of the Slack messages that drive long-running agent turns, and thread lifecycle for bound
threads.

## Problem (current main, `6575507`)

The normalized observer hook `gateway_platform_event` exists (`hermes_cli/plugins.py:348-362`,
dispatched `gateway/run.py:15441-15453`, post-authorization, envelopes only, never raw SDK
objects) — but only Discord (`plugins/platforms/discord/adapter.py:1641-1711`: message_edited,
message_deleted, thread_created, thread_renamed) and Telegram
(`plugins/platforms/telegram/adapter.py:4022-4085`: reaction, message_edited) emit it. Slack,
the adapter with the largest event vocabulary, fires none: `message_changed` and
`message_deleted` subtypes are consumed internally by `_handle_slack_message` and are invisible
to plugins.

## Proposal (pure parity)

In `plugins/platforms/slack/adapter.py`, at the points where `message_changed` /
`message_deleted` subtypes and thread-broadcast events are already classified, emit the same
normalized envelopes Discord emits: `message_edited`, `message_deleted`, plus `reaction`
parity with Telegram. (`thread_created` is dropped from scope: Slack has no native thread
lifecycle event and deriving "first threaded reply" needs adapter state — scope creep for a
parity PR.) Same post-auth gating and
`has_hook()` short-circuit as the existing fire-sites; payload fields mirror Discord's envelope
shape (platform, channel, thread, message id/ts, actor, edited text where applicable). No new
hook, no schema change, no raw Bolt objects across the boundary.

## Tests

- Adapter-level tests injecting synthetic Bolt payloads for each subtype and asserting one
  normalized `gateway_platform_event` per event with the documented fields, gated off when no
  hook is registered and when the actor fails `_is_user_authorized`.
- Behavior-contract test through the real plugin discovery path with a frozen listener plugin.

## Compatibility

Additive observer emissions only; zero behavior change for installations with no registered
hook (existing `has_hook()` short-circuit). Conventional commit:
`feat(slack): emit normalized gateway_platform_event for edits, deletions, threads, reactions`.

## Status (2026-08-22)

**Implemented and tested** on `feat/slack-gateway-platform-events` from hermes-agent main
`657550716`. Patch: `0001-slack-gateway-platform-events.patch` (this directory). Local
evidence: 11 new tests mirroring upstream's Discord fire-site suite (normalized envelopes,
edits visible even when dispatch drops them as already processed, fail-closed on missing
identities, has_hook fast-path skips normalization, raising observers never break the
listener), plus the full Slack + platform-event gateway selection green: 563 passed
(pre-existing environmental collection error in test_teams.py excluded, reproduced on pristine
main). Filing blocked on credentials, same as proposal 1.

## Prior art (updated 2026-08-26, rebased on main d9e4b234a)

Searched open and merged PRs/issues per CONTRIBUTING. No PR adds Slack fire-sites; related:

- **#89908** (open) suppresses Slack's phantom `message_changed` re-emits (unfurl metadata).
  Composes cleanly: that PR reduces noise into the branch where our observer fires; whichever
  lands second rebases trivially. Cite it in the PR body.
- **#73450 / #83501** (open) fix edit-replay dispatch bugs in the same branch — same relationship.

Verified on main d9e4b234a: 62/62 across slack/discord/hook platform-event suites.
