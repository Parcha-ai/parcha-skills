# Upstream proposal 3 — carry the sender bot flag into the Slack early-reject check

**Target:** NousResearch/hermes-agent · one PR · branch `fix/slack-early-reject-drops-bot-flag`
**Type:** bug fix (not a feature request)
**Found by:** debugging why one Hermes agent could not answer another in Slack. The
answer was that no configuration could make it work.

## Problem (current main)

`plugins/platforms/slack/adapter.py` rejects unauthorized senders early — before
thread lookups, name resolution, and media downloads — by calling the gateway
runner's `_is_user_authorized` with a `SessionSource` the adapter builds itself.

That source is built **without `is_bot`**:

```python
_source = self.build_source(
    chat_id=channel_id,
    chat_name="",
    chat_type="dm" if is_dm else "group",
    user_id=user_id,
    user_name="",
)
```

`gateway/authz_mixin.py::_is_user_authorized` branches on exactly that field:

```python
if getattr(source, "is_bot", False):
    allow_bots_var = platform_allow_bots_map.get(source.platform)
    if allow_bots_var and os.getenv(allow_bots_var, "none").lower().strip() in {"mentions", "all"}:
        return True
```

Because the field is always absent here, the authorizer never reaches its bot
branch and **`SLACK_ALLOW_BOTS` is unreachable on this path**. An operator who sets
`SLACK_ALLOW_BOTS=all` still has every bot-authored Slack message rejected, and the
only trace is:

```
WARNING [Slack] Early reject of unauthorized user U09450ZLS81 in channel C095VU95XQR
```

which names the sender but not the reason, and is identical to the message a
genuinely unlisted human produces. The setting looks broken and the remedy is
unguessable.

## Fix

`sender_is_bot` is already resolved ~200 lines earlier in the same function
(`_event_declares_bot_sender`, plus the `users.info` probe for unlabeled events), so
the fix is to pass it through:

```python
    user_name="",
    is_bot=sender_is_bot,
)
```

No behavior change for human senders. For bot senders it makes the documented
`SLACK_ALLOW_BOTS` contract actually apply at this gate, matching what the later
gateway-runner check already does with a fully-populated source.

## Tests

`tests/gateway/test_slack_early_reject_bot_flag.py` — three behavior assertions: the
guard's source carries the flag, the flag is resolved before the guard runs, and the
authorizer's bot branch depends on exactly that field.

## Verification

- 3 new tests pass.
- `pytest tests/gateway/ -k slack` → **626 passed, 2 skipped, zero regressions**
  (`test_teams.py` excluded: pre-existing collection error on pristine main).
- Applied to a live deployment: an agent that had been silently rejected for seven
  attempts began receiving and answering mentions immediately.

## Platforms tested

Linux (Ubuntu 24.04), Python 3.11.
