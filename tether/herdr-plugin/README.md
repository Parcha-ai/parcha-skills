# Tether for Herdr

Tether keeps a Slack thread attached to the exact Codex or Claude Code process
running in a Herdr pane. The Herdr plugin is a control surface; the installed
Tether broker remains the sole owner of Slack credentials, routing, durable
queues, and delivery recovery.

## Install

For the coordinated install:

```bash
npx --yes --package=@parcha/tether@0.3.0-beta.1 \
  tether setup --harness=both --herdr
```

When Tether core is already installed, pin the plugin to the full commit from
the matching release:

```bash
herdr plugin install Parcha-ai/parcha-skills/tether/herdr-plugin \
  --ref <verified-release-commit-sha>
```

Open `Tether: Open cockpit` from Herdr's plugin action menu. The cockpit can
create a thread, attach a selected or Ctrl-clicked Slack thread link, rebind a
stale agent, detach, run doctor, and review unresolved delivery.

## Security and beta boundary

- Linux, Herdr 0.8.x protocol 19, Codex, and Claude Code are supported.
- No Slack token enters this plugin or a coding-agent process.
- Prompts are submitted through Herdr's private socket and message text uses
  stdin or a private Tether inbox, never process arguments.
- A lost or ambiguous Herdr prompt response becomes `uncertain` and is never
  replayed automatically. Inspect the agent before resolving it.
- Herdr plugins execute as your user and are not sandboxed. Review the manifest
  and source before installation.
