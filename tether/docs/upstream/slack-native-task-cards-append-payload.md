# Upstream: Slack native task cards send `markdown_text` and `chunks` together

Observed on Hermes v0.21.0 (main 245e4800, 2026-09-05) with
`platforms.slack.extra.native_task_cards: true` and `streaming.enabled: true`:

```
ERROR [Slack] Native task-card progress error: chat.appendStream ->
{'ok': False, 'error': 'cannot_provide_both_markdown_text_and_chunks'}
WARNING Slack native task-card progress failed; falling back to an editable text update
```

`plugins/platforms/slack/adapter.py`, `send_native_task_card_progress`: the append payload is
built with `chunks` (plan_update + task_update) and then, when a fallback text exists,
`append_payload["markdown_text"] = fallback_text` is added to the same call. Slack's
`chat.appendStream` accepts one or the other, never both (see the method reference:
"Cannot mix markdown_text and chunks"). Every card update fails, and the fallback posts a
"Hermes is working" text message into the thread, which is exactly the noise the cards were
meant to replace.

Fix: send `chunks` alone on `appendStream`; keep `fallback_text` for the non-native path only.

```diff
-                if fallback_text:
-                    append_payload["markdown_text"] = fallback_text
```

Until this lands, `native_task_cards` should stay `false` on gateways that stream.
Filed from Parcha's Tether canary (claudio, 2026-09-06). Hold external PR for Miguel's nod.
