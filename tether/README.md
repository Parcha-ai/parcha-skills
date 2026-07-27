# Tether

> Slack threads that stay attached to your agents.

Your coding agent says “done” in Slack. You reply “one more thing.” The same agent, in the same session, picks it back up.

That is Tether.

It connects a Slack thread to the exact Codex, Claude Code, Zellij, Hermes, or headless run that created it. No mystery relay agent. No pasted context. No Slack token in your coding session.

## Level 1: make it work

You need [Hermes Agent](https://github.com/NousResearch/hermes-agent) and Node 18+.

```bash
npx --yes --package=github:miguelrios/unc-skills#main tether setup --harness=both
```

Tether installs its Codex and Claude Code skill, adds an external Hermes plugin, opens Hermes's Slack setup, restarts the gateway, and checks the live connection.

Slack's website is the one unavoidable pit stop: create or update the app from the generated manifest, install it to the workspace, and create the Socket Mode app token. Tether joins public destinations before posting so their replies remain readable; invite the bot to private channels explicitly.

When setup finishes:

```bash
tether doctor
```

Every line should be `ok` (a missing default channel is only a warning).

## Level 2: let the magic happen

Inside Codex or Claude Code, say:

> Let me know in Slack when this is done.

The agent uses Tether to publish the result. The root message includes a compact origin such as:

```text
Origin: Codex a1b2c3d4 in Zellij api-work / pane 7 · my-project
```

Reply in that thread without mentioning the bot. Hermes verifies the workspace, channel, thread, and user allowlist, then continues the captured session. Shared channels accept every explicitly allowlisted operator by default; private workflows can set one owner. When the notification came from a live Zellij pane, Tether verifies and steers that pane instead of launching a competing background resume. The answer returns to the same thread.

Tether uses Socket Mode for immediate delivery and a bounded, persistent-dedupe poller for recent active threads. If Slack's websocket drops during a reply, the recovery path picks up the missed message and sends it through the same authorization and session routing exactly once.

Say `cancel`, `stop`, `nvm`, or `never mind` to stop an active native continuation.

## Level 3: wire a cron

A process that may exit needs a durable run identity:

```bash
python3 ~/.local/share/tether/tether_notify.py notify \
  --run-id "security-sweep-$RUN_ID" \
  --idempotency-key "security-sweep-$RUN_ID" \
  --text "Sweep complete: 0 critical findings."
```

Replies continue as a durable Hermes operator conversation, even after the original process is gone. Reusing the idempotency key for a retry reuses the same Slack thread.

You can also add `--channel C…`, `--owner U…`, or `--file /absolute/path/report.png`. Keep destinations in operator config and secrets out of message text.

For an explicitly requested direct or group message, resolve only authorized
operators and open the conversation through the broker:

```bash
tether users --query "Alex"
tether dm --user U12345678 --user U87654321 \
  --run-id onboarding --idempotency-key onboarding-2026-01 \
  --text "The machine is ready. Please test SSH."
```

The Slack token never enters the caller, and Tether refuses recipients outside
Hermes's configured human allowlist.

## Level 4: understand the trick

```text
Codex / Claude / Zellij / cron
              │ local 0600 Unix socket
              ▼
        Hermes + Tether broker
              │ owns Slack credentials
              ▼
       one exact Slack thread
              │ authorized reply
              ▼
      the original bound session
```

Each bridge persists one source capability, one Slack workspace/channel/thread, one owner policy, and one idempotency key. Replies are deduplicated and serialized. Prompts go to native agents on stdin, not command arguments. Child processes get a scrubbed environment and never inherit Hermes's Slack credentials.

A Zellij-only bridge fingerprints an allowlisted agent command when the message is created and checks it again before every reply. If that pane has returned to a shell or started a different process, Tether refuses to type into it.

Tether fails closed. If the original session is stale, the sender is unauthorized, or the adapter is incompatible, it will not guess another session.

## Level 5: run it your way

Tether automatically reuses Hermes's `SLACK_ALLOWED_USERS`, `GATEWAY_ALLOWED_USERS`, and `SLACK_HOME_CHANNEL`. Optional overrides live at `${XDG_CONFIG_HOME:-~/.config}/tether/config.toml` with mode `0600`.

For scoped native-agent credentials, configure `credential_command` and `credential_env_allowlist`. The helper reads non-secret bridge metadata on stdin and returns short-lived environment values as JSON. Tether rejects undeclared keys and all Slack credentials.

For headless installation:

```bash
npx --yes --package=github:miguelrios/unc-skills#main tether setup \
  --harness=both \
  --non-interactive
```

Then finish `hermes gateway setup`, start the gateway, and run `tether doctor`.

## Updates without drama

Hermes and Tether update independently. After a Hermes update, rerun the setup command. Tether replaces only its own code, preserves bridge state and config, restarts Hermes, and checks the Slack adapter compatibility surface.

Use `#main` for transparent latest updates or pin a release tag for reproducible production installs.

`tether setup` enables the Tether Hermes plugin and disables the legacy pre-release `session-bridge` plugin when present. `tether doctor` verifies the live broker protocol and reply-ingress health; a plugin file merely existing on disk is not considered ready.

## Skill-only install

```bash
npx skills add miguelrios/unc-skills --skill tether
```

This teaches an agent how to use Tether but does not install the external Hermes runtime. Use the setup command for end-to-end routing. Browse the skill at [skills.sh/miguelrios/unc-skills/tether](https://skills.sh/miguelrios/unc-skills/tether).

Stock pi can publish with `--run-id`; native session resume currently targets Codex and Claude Code.

## Build it, break it, prove it

```bash
npm test
npm run pack:check
```

The suite covers exact-thread routing, unauthorized replies, deduplication, queue serialization, cancellation, restart recovery, credential isolation, secret redaction, private socket permissions, installation, and package leak checks.

The precise security and routing rules live in [`skills/tether/references/contract.md`](skills/tether/references/contract.md). Setup details live in [`skills/tether/references/setup.md`](skills/tether/references/setup.md).
