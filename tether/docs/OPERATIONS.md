# Tether Operations

This runbook covers installation, upgrade, rollback, recovery, and diagnosis
for Tether `0.3.0-beta.1`.

## Managed state

Tether installs managed files under:

- `${XDG_DATA_HOME:-~/.local/share}/tether/`;
- `${HERMES_HOME:-~/.hermes}/plugins/tether/`;
- `${CODEX_HOME:-~/.codex}/skills/tether/`, when selected;
- `${CLAUDE_HOME:-~/.claude}/skills/tether/`, when selected; and
- `~/.local/bin/tether`.

Installer manifests, transaction journals, and rollback snapshots live under:

```text
${XDG_STATE_HOME:-~/.local/state}/tether-installer/
```

Runtime config and `bridges.db` are retained across upgrade, rollback, and
uninstall. The database contains Slack text and identifiers, continuation
identifiers, local paths and process metadata, outbox payloads, and bounded
errors. Treat it and its backups as sensitive.

## Install

Use an immutable published version:

```bash
npx --yes --package=@parcha/tether@0.3.0-beta.1 \
  tether install --harness=both --dry-run

npx --yes --package=@parcha/tether@0.3.0-beta.1 \
  tether setup --harness=both --herdr
```

Use `--harness=codex` or `--harness=claude-code` when only one harness is
needed. `setup` installs the payload, enables the Tether Hermes plugin,
disables the legacy `session-bridge` plugin when present, opens Hermes Slack
setup, and runs readiness checks.

The installer:

1. refuses root and unsupported hosts;
2. validates managed roots;
3. takes an exclusive owner-only lifecycle lock;
4. stages and validates a complete payload;
5. snapshots managed files and Hermes plugin state;
6. commits with atomic renames and a durable transaction journal; and
7. restores the snapshot if commit or a requested gateway restart fails.

Do not delete this lock while another lifecycle operation is running:

```text
${XDG_STATE_HOME:-~/.local/state}/tether-installer/install.lock
```

After setup:

```bash
export PATH="$HOME/.local/bin:$PATH"
tether version
tether doctor
```

A production deployment should report:

- the expected Tether and Hermes versions;
- a private broker socket owned by the Hermes user;
- an explicit Slack operator allowlist;
- valid Hermes plugin wiring; and
- connected Slack Events API ingress through Socket Mode.

The `conversations.replies` poller is only best-effort recovery. It may be
rate-limited or unavailable to a bot token for channel threads. Do not accept a
poll-only deployment as healthy.

## Upgrade

Before crossing a database schema boundary:

1. Stop Hermes.
2. Back up `bridges.db` with SQLite's backup command, or copy the database and
   any `-wal` and `-shm` sidecars as one set.
3. Record the installed Tether version and schema with the backup.
4. Run the immutable upgrade.
5. Start Hermes and run `tether doctor`.
6. Test one outbound root and one Slack reply through Socket Mode.

Upgrade the managed payload:

```bash
npx --yes --package=@parcha/tether@0.3.0-beta.1 \
  tether upgrade --harness=both --restart --herdr
```

On Linux, `--restart` detects an active system-level
`hermes-gateway.service` and uses Hermes's documented `gateway restart
--system` command through non-interactive `/usr/bin/sudo`. If that privilege is
unavailable or the restart fails, the lifecycle command restores the snapshot
and exits nonzero.

The current schema is 15. Runtime startup rejects a database with a newer
schema, upgrades supported older schemas in one immediate transaction, and
marks legacy or incomplete native bindings `rebind_required`. Rebind those
sessions explicitly; Tether does not infer a replacement endpoint.

If the process is killed during commit, the next lifecycle command reads the
transaction journal and completes recovery before starting new work.

## Rollback

Restore the immediately previous managed payload and recorded plugin state:

```bash
tether rollback --restart --herdr
```

Pass `--herdr` when the companion plugin is installed. Tether reconciles the
global Herdr link with the restored payload: it relinks a restored plugin or
unlinks it when the restored version predates the plugin.

Rollback does not:

- downgrade `bridges.db`;
- restore Slack app configuration;
- restore deleted runtime data; or
- reverse changes made by an agent or external tool.

If the older runtime cannot read schema 15, stop Hermes and restore the
database backup created with that runtime. Do not point old code at a newer
database and keep retrying.

## Uninstall

```bash
tether uninstall --herdr
```

`--herdr` unlinks `parcha.tether` before removing its managed files. Omit it on
a host where the companion plugin was never linked.

Uninstall disables the plugin and removes only files whose checksums still
match the installer manifest. It preserves locally modified managed files,
config, `bridges.db`, and installer snapshots. The command prints the snapshot
that can restore the removed payload.

Because uninstall removes `~/.local/bin/tether`, restore through the same
immutable package:

```bash
npx --yes --package=@parcha/tether@0.3.0-beta.1 \
  tether rollback --restart
```

## Normal diagnostics

Run these as the Hermes/Tether Unix user:

```bash
tether status
tether doctor
tether unresolved --team T12345678
tether maintenance
```

| Command | Use |
| --- | --- |
| `status` | Show local plugin, broker, binding, and queue state |
| `doctor` | Validate compatibility, broker privacy, allowlists, plugin protocol, and ingress readiness |
| `unresolved` | List uncertain Hermes ingress and native attempts that require an operator decision |
| `maintenance` | Run bounded retention and recovery maintenance |

For a public support issue, include command names, exit codes, typed Tether
errors, versions, operating system, architecture, and whether Socket Mode was
connected. Do not publish `bridges.db`, Slack text, environment dumps,
transcripts, credentials, or raw Hermes logs.

## Resolve uncertain work

An external operation may have succeeded even when Tether could not record its
result. Do not retry it until the outcome is known.

1. List unresolved work:

   ```bash
   tether unresolved --team T12345678
   ```

2. Inspect the exact Slack thread and, for native work, the exact bound
   session.
3. Record one decision:

   ```bash
   tether resolve \
     --team T12345678 \
     --kind attempt \
     --id att_example \
     --action complete
   ```

Actions:

| Action | Use |
| --- | --- |
| `retry` | The original Hermes dispatch or native attempt is proven not to have run |
| `complete` | The original operation is proven to have completed |
| `abandon` | The outcome cannot be accepted safely; cancel it and begin a new explicit operation |

Native ingress transferred to an attempt cannot be retried directly; resolve
the attempt. Rebind and close remain blocked while related work is uncertain.
Repeating the same terminal decision is safe; a conflicting decision fails
closed.

## Message input

Pass Slack text through standard input:

```bash
printf '%s\n' 'Deployment verified.' |
  tether notify \
    --run-id "deploy-$RUN_ID" \
    --idempotency-key "deploy-$RUN_ID" \
    --text-stdin
```

Use `--text-fd FD` for another inherited private file descriptor. Avoid
deprecated `--text`, which exposes message content in process arguments.

Configured word, character, and sentence values are soft writing targets. The
default is 50 words and three sentences; complete or safety-critical replies
may exceed it. The enforced transport limit is 35,000 characters.

## Recovery cases

| Symptom | Recovery |
| --- | --- |
| Broker unavailable | Restart Hermes, then run `tether doctor`. |
| Socket Mode disconnected | Restore Socket Mode. Polling is not a complete substitute. |
| Native binding stale | Rebind from the intended live session. Never route to another agent as fallback. |
| Live terminal submission uncertain | Inspect that Herdr agent or Zellij pane, then use `tether resolve`. |
| Slack write uncertain | Keep one broker running and allow reconciliation; do not post the same text manually. |
| Install interrupted | Run the same lifecycle command; it recovers the transaction journal first. |
| Upgrade causes gateway failure | Inspect automatic rollback, then use `tether rollback --restart` if needed. |
| Schema migration fails | Stop Hermes and restore the matching database backup. |

## Polling and reconciliation

Slack Events API through Socket Mode is authoritative ingress. The recent-reply
poller and Slack-write reconciler share a durable history-read budget:

- one 15-message page at a time;
- at most one page per workspace and Slack method every 60 seconds;
- persisted cursors and target rotation; and
- rejection of repeated, malformed, or non-advancing cursors.

This favors duplicate avoidance and Slack compliance over recovery latency.
Tether-created root ownership does not depend on polling. An explicitly
attached existing thread does not gain root ownership, so ambient routing there
may fail closed when ownership evidence is unavailable.

## Retention and state removal

Completed delivery and deduplication records use `retention_days` from
`config.toml`, default 30 days. Active bindings and unresolved delivery state
are retained. The gateway runs bounded maintenance daily while reply recovery
is active; `tether maintenance` runs it explicitly.

To remove all state:

1. Disable the plugin and stop Hermes.
2. Make any required compliance backup.
3. Verify no other component uses `~/.hermes/bridges.db`.
4. Delete the database and its `-wal` and `-shm` sidecars together.
5. Delete config and installer state only when their recovery value has ended.

This is irreversible and intentionally separate from uninstall.

## Attachments

Attachments remain disabled until
`TETHER_UPLOAD_APPROVED_ROOTS` contains colon-separated absolute private
directories owned by the Hermes user and mode `0700`. Do not approve `/`,
`/tmp`, shared checkouts, or credential stores.

`TETHER_UPLOAD_MAX_BYTES` may lower the size ceiling.
`TETHER_UPLOAD_STAGING_DIRECTORY` may select another private staging
directory. Both are read at gateway start.

The upload guard checks location, owner, regular-file type, link count, size,
stable identity, and known secret patterns. It is not semantic DLP.

Hermes local media sends use this guard, but those native Slack upload calls
are best-effort. Only a root upload submitted with `tether notify --file` uses
Tether's durable restart-safe file state machine.
