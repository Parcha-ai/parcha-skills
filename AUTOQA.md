# Tether AutoQA Baseline

This baseline covers `tether/` and the repository metadata that publishes it.
The current implementation supports Linux, Python 3.11–3.14, Node.js 22 or 24,
and Hermes Agent 0.19.0 at the commit pinned in `tether/docs/COMPATIBILITY.md`.

## Target and isolation

Run local QA from the repository root. Use an isolated temporary `HOME`,
`HERMES_HOME`, XDG directories, fake Slack clients, and fake Hermes commands.
The packaged shell tests already create and clean those fixtures. They must not
read the operator's real Slack credential or mutate the live gateway.

The local target instance is the broker and installed CLI created by the test
fixtures. A real gateway cutover, database migration, Slack post, or Slack reply
is a separate live phase that requires immediate operator confirmation.

## Stable commands

From `tether/`:

```bash
npm ci --ignore-scripts --no-audit --no-fund
npm test
npm run pack:check
```

Match CI from the repository root:

```bash
python3 -m unittest discover -s tests -p 'test_portability.py' -v
ruff check tether/runtime tether/skills/tether/scripts tether/tests
bandit -q -r tether/runtime tether/skills/tether/scripts
shellcheck tether/install.sh tether/tests/*.sh
```

The authoritative platform matrix is `.github/workflows/tether-ci.yml`. Where
the host lacks a matrix runtime or architecture, use isolated containers or
record that cell as untested. Do not infer a platform pass from another
interpreter or CPU architecture.

## Authentication

Local QA uses synthetic Slack workspace, channel, user, event, and thread IDs.
Broker tests use private Unix sockets and same-UID fixtures. Release tests use
fake Hermes commands and temporary XDG roots.

Live QA requires:

- the dedicated non-root Hermes Unix user;
- Hermes 0.19.0 from the exact clean, pinned checkout;
- an explicit Slack operator allowlist; and
- connected Socket Mode ingress.

Never load a Slack bot token into an agent process. Tether and Hermes own that
credential boundary.

## Baseline feature catalog

1. CLI discovery and diagnostics: help, version, status, doctor, and protocol
   compatibility.
2. Transactional lifecycle: setup, install, upgrade, rollback, uninstall,
   locking, snapshots, journals, and crash recovery.
3. Binding lifecycle: create, activate, attach, rebind, close, generation
   fencing, and uniqueness.
4. Slack admission and one-writer routing: identity, authorization, mentions,
   root ownership, participation, and `SILENT`/`HERMES`/`NATIVE` selection.
5. Durable ingress and delivery: leases, attempts, queues, outboxes,
   reconciliation, uncertain states, polling recovery, and retention.
6. Native continuation: Codex, Claude Code, Zellij, headless runs, process
   identity, working-directory identity, cancellation, and bounded I/O.
7. Outbound CLI operations: notify, reply, post, thread/history, attachment,
   stdin/file-descriptor text transport, and idempotency.
8. Operator recovery: unresolved inspection, retry, complete, abandon, and
   maintenance.
9. Security boundary: root refusal, same-UID socket peers, private paths,
   credential helpers, environment allowlists, upload staging, secret
   rejection, and redaction.
10. Release integrity: exact package inventory, install-from-tarball smoke,
    version agreement, immutable marketplace refs, workflow validation, and
    repository portability.
11. Compatibility matrix: Linux x86-64/arm64, Python 3.11–3.14, Node.js 22/24,
    exact Hermes compatibility, schema migration, and fail-closed newer
    protocols or schemas.

## Live acceptance checks

After local QA passes and the operator approves the interruptive cutover:

1. Back up the database and record the installed versions.
2. Upgrade the dedicated gateway and run `tether version`, `tether status`, and
   `tether doctor`.
3. Post one outbound root to a controlled Slack destination.
4. Reply in that thread without mentioning the bot and witness exactly one
   continuation in the bound session.
5. Rebind or close the test bridge, verify stale delivery is fenced, and leave
   the gateway healthy.
