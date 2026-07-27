# Bridge contract

## Boundary

The Hermes gateway owns the Slack credential and a mode-`0600` Unix socket. Local publishers send newline-delimited JSON to that socket. They never load a Slack token.

One bridge binds:

- one source capability: `codex_session`, `claude_session`, `zellij_pane`, `hermes_session`, or `headless_run`;
- one Slack workspace/channel/thread tuple;
- one owner (`*` means any allowlisted operator and is the shared-channel default);
- one idempotency key.

Direct and group DMs use the same contract. The broker may open a conversation
only when every requested recipient is already in the effective Hermes
allowlist. Opening the conversation and posting its root message occur inside
the credential-owning broker; local publishers receive only the channel/thread
result.

A trusted local launcher may silently attach an existing Slack thread only after it has captured a concrete native session ID. When that session already runs in Zellij, attach must also capture and fingerprint the exact pane; otherwise Tether would create a parallel resume process. Attach is idempotent and refuses to replace an active binding; it is not a semantic router or stale-session fallback.

A wrong Zellij binding may be replaced only by `tether rebind` running inside the intended live pane. The CLI does not accept a caller-supplied pane ID, and rebind posts nothing.

An explicit run ID always creates `headless_run`, even inside Codex or Claude Code. Native sessions remain the source when those agents run inside Zellij, but the captured and fingerprinted live pane is their delivery endpoint.

## Inbound routing

Resolve the exact persisted thread before bypassing Slack's mention gate. Then apply the global allowlist and any explicit per-bridge owner restriction. Shared channels accept every allowlisted operator by default; use one owner only for an intentionally private bridge. Fail closed at every missing field. Deduplicate by Slack message timestamp.

By default, owner restrictions are accepted only for DMs. A public/private
shared channel rejects an owner-restricted bridge unless the deployment opts in
with `allow_channel_owner_restrictions = true`. A rejected handoff must not keep
Hermes's processing or success reaction.

Socket Mode is the primary inbound transport. Tether also polls a bounded batch of recently active threads through Hermes's existing Slack client. Polled events re-enter the normal adapter and gateway pipeline; a persistent ingress ledger prevents duplicate execution across live delivery, polling, and gateway restarts. Polling never weakens workspace, channel, allowlist, or bridge-owner checks.

Peer-agent collaboration is agentic. Hermes may admit peer messages in threads where the agent is participating; Tether applies the same configured transport policy during reply recovery and routes trusted turns to the exact bound session when one exists. Hermes is not a second writer for a bound thread. Code handles identity, self-echo prevention, and deduplication. The agent decides from conversation context whether a response is warranted and returns exactly `NO_REPLY` when it is not; Hermes suppresses that marker before Slack delivery.

Queue and serialize native replies per bridge. Strip synthetic Slack thread history before native resume because prior bridge turns already exist in the bound agent session. Terminate the whole continuation process group on cancellation or timeout.

Never forward the gateway's Slack credential environment to a native agent process. Pass prompts on stdin rather than command arguments. Accept credential-helper output only for explicitly allowlisted environment keys. Treat resume flags as administrator configuration, never Slack-controlled data.

## Outcomes

Post native output back to the same Slack thread. When Claude or Codex has a captured Zellij pane, capture its allowlisted agent command and process fingerprint, recheck both before every delivery, inject the operator instruction into that exact pane, verify the text is visible, press Enter, and verify the same agent process remains active. Detached native resume is allowed only when no live pane was captured. Never write into a shell or a pane whose process changed. Continue headless work as a durable Hermes conversation using the root report and thread history as context.

Display compact origin metadata in the root message without exposing full session IDs or absolute paths. Apply high-confidence credential redaction at Slack egress, sanitize stored and posted errors, and require agents to omit sensitive content that cannot be detected mechanically. Never route to a replacement session after a stale-source failure.

## Automation checklist

Before migrating a cron or automation, verify all of the following:

- the direct Slack call and token access are removed;
- `--run-id` is unique per scheduled occurrence;
- the idempotency key is stable across retries of that occurrence;
- destination channel, owner, and workspace come from explicit config;
- message text contains no credential or sensitive raw payload;
- a real thread reply reaches the intended durable Hermes context;
- unauthorized and duplicate replies produce no execution.
