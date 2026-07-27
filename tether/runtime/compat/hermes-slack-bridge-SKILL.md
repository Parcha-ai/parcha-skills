---
name: hermes-slack-bridge
description: Compatibility entrypoint for Tether resumable Slack threads. Use when existing instructions require hermes_notify.py.
---

# Hermes Slack Bridge

This is the compatibility name for Tether. Route all Slack notifications and
replies through `scripts/hermes_notify.py`; it uses the canonical Tether broker
and never reads a Slack token.

For a shared Slack channel, omit `--owner`. Every member of the explicit Hermes
allowlist may then continue the bound session. Use `--owner U...` only when the
thread must intentionally be restricted to one operator, normally in a DM or a
sensitive workflow. The allowlist remains mandatory in both cases.

```bash
python ~/.codex/skills/hermes-slack-bridge/scripts/hermes_notify.py notify \
  --text "Done: <outcome and evidence>" \
  --idempotency-key "<stable-key>"
```

Use `--run-id` for a durable headless run. Use the exact `reply` command supplied
to a resumed native session. Do not call Slack APIs directly, expose credentials,
or invent a replacement session when the bound source is stale.

For a user-requested DM, use `users --query` and `dm --user U...`; the broker
permits only authorized Hermes operators and keeps the Slack token private.
