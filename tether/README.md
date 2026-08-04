# Tether

Tether binds a Slack thread to the Codex, Claude Code, Herdr, Zellij, Hermes, or
headless run that created it. Hermes owns the Slack credential. Local clients
use an owner-only Unix socket and do not receive that credential.

`0.2.0-beta.1` is a pre-release. This source tree uses BindingV3 and database schema 15.
Read [Compatibility](docs/COMPATIBILITY.md) before upgrading an existing host.

## Supported boundary

| Component | Supported |
| --- | --- |
| Operating system | Linux on x86-64 or arm64 |
| Python | 3.11 through 3.14 |
| Node.js | Maintained LTS 22 or 24 |
| Hermes Agent | Exactly 0.19.0; tested commit `b9ba7c78e41b5d187e2c8fb446655c4b71c42aa5` |
| Native continuation | Codex and Claude Code |
| Terminal continuation | Herdr protocol 19 (tested with 0.8.0), or Zellij on Linux; both require `/proc` process identity |
| Slack ingress | Slack Events API through Hermes Socket Mode |
| Headless publication | Explicit `--run-id`; no native-session resume |

macOS and Windows are unsupported.

## How it works

```text
Slack Events API / Socket Mode
  -> identity, authorization, and one-writer routing
  -> durable ingress record
  -> Hermes or one exact native binding

Local CLI
  -> owner-only Unix socket
  -> durable Slack outbox
  -> Slack API
```

Each bridge stores one source, one continuation endpoint, one Slack thread, an
operator policy, and a monotonic binding generation. Rebind and close increment
the generation, which fences stale ingress and delivery attempts.

A Tether-created thread root is durable local proof that the bridge owns that
thread for ambient replies. `tether attach` binds an existing thread but does
not claim its root; unmentioned replies there fail closed unless ownership can
be established by the routing rules.

Slack Events API delivery is authoritative ingress. The
`conversations.replies` poller is bounded, best-effort recovery for events
missed during a disconnect. It can be rate-limited or unavailable to bot tokens
for channel threads, so a healthy poller is not a replacement for healthy
Socket Mode.

See [Architecture](docs/ARCHITECTURE.md) and
[Security Model](docs/SECURITY_MODEL.md).

## Install

Install an immutable published version:

```bash
npx --yes --package=@parcha/tether@0.2.0-beta.1 \
  tether setup --harness=both
```

Use `--harness=codex` or `--harness=claude-code` for one harness. Setup requires
Hermes 0.19.0, an explicit Slack operator allowlist, and a Slack app configured
for Socket Mode.

For a source install, use the full 40-character commit from the matching
release:

```bash
TETHER_COMMIT="<verified-release-commit-sha>"
npx --yes \
  --package="github:Parcha-ai/parcha-skills#$TETHER_COMMIT" \
  tether setup --harness=both
```

Do not install from a moving branch.

To install only the portable instruction skill:

```bash
npx skills add miguelrios/unc-skills --skill tether
```

Browse the skill on
[skills.sh](https://skills.sh/miguelrios/unc-skills/tether).

The skill-only command does not install the Hermes plugin or local broker.

Verify the live installation:

```bash
export PATH="$HOME/.local/bin:$PATH"
tether version
tether doctor
```

For production operation, `doctor` should report a private broker, compatible
Hermes, an explicit operator allowlist, and connected Socket Mode ingress.

## Use

From Codex or Claude Code:

> Let me know in Slack when this is done.

For a process that may exit, provide a durable run identity and pass text over
standard input:

```bash
printf '%s\n' 'Sweep complete: 0 critical findings.' |
  tether notify \
    --run-id "security-sweep-$RUN_ID" \
    --idempotency-key "security-sweep-$RUN_ID" \
    --text-stdin
```

Use `--text-fd FD` when another inherited private file descriptor is more
appropriate. `--text` remains deprecated because it exposes content in process
arguments.

Inspect a thread without loading a Slack token:

```bash
tether thread --channel C12345678 --thread-ts 1234567890.123456
```

Tether asks agents to default to 50 words and three sentences. Those values,
including configurable word, character, and sentence targets, are writing
guidance rather than delivery gates. A complete or safety-critical answer may
exceed them. The enforced text transport limit is 35,000 characters.

### Attachments

`--file` is disabled until the gateway receives
`TETHER_UPLOAD_APPROVED_ROOTS`, a colon-separated list of absolute private
directories owned by the Hermes user and mode `0700`. Optional controls are
`TETHER_UPLOAD_STAGING_DIRECTORY` and `TETHER_UPLOAD_MAX_BYTES`.

Tether accepts an owner-matching regular file beneath an approved root. It
rejects symlinks, hard links, oversized or changed files, and content matching
its known-secret policy. Hermes local media paths use the same private staging
guard. This is not general data classification or DLP.

## Delivery and recovery

One routing decision selects `SILENT`, `HERMES`, or `NATIVE`, and the selected
writer is persisted before execution. Native events are transferred to their
queue in the same SQLite transaction that completes ingress.

Slack roots, native replies, generic thread replies, and Hermes text posts and
edits use durable immutable outboxes and leases. Posts use stable client IDs
and paginated reconciliation; edits retry the same payload against the same
message. This reduces duplicates but does not make Slack exactly once.

Slack ephemeral notices and native media APIs are guarded and redacted but
remain best-effort because Slack does not expose a recoverable idempotency
boundary for those operations. A `tether notify --file` root upload uses
Tether's durable staged upload protocol.

Ambiguous Hermes ingress or native delivery is retained as `uncertain` and is
not blindly replayed. Inspect it and resolve it explicitly:

```bash
tether unresolved --team T12345678
tether resolve \
  --team T12345678 \
  --kind attempt \
  --id att_example \
  --action retry
```

Actions are `retry`, `complete`, or `abandon`. Choose one only after checking
whether the original operation ran.

## Lifecycle

Upgrade:

```bash
npx --yes --package=@parcha/tether@0.2.0-beta.1 \
  tether upgrade --harness=both --restart
```

Restore the immediately previous managed payload:

```bash
tether rollback --restart
```

Install and upgrade take a lifecycle lock, stage a complete payload, snapshot
managed files and plugin state, and maintain a crash-recovery journal. A failed
commit or requested gateway restart restores the previous managed state.

Rollback does not downgrade `bridges.db` or undo Slack settings. Uninstall
retains config, bridge state, snapshots, and locally modified managed files:

```bash
tether uninstall
```

See [Operations](docs/OPERATIONS.md) for backup, rollback, diagnostics,
retention, and irreversible state removal.

## Security boundary

Run Hermes and Tether as one dedicated non-root Unix user. The broker refuses
UID 0, creates a mode-`0600` socket, and accepts only peers with the broker UID.
Native child environments are allowlisted and do not inherit Slack
credentials.

The Unix account is the local authority boundary. Tether does not isolate
processes that share that UID and does not sandbox agent tools. Put mutually
untrusted agents in separate accounts or hosts.

Slack humans and trusted peer bots require explicit allowlists. Channel
membership alone is not authorization.

See the repository [Security Policy](../.github/SECURITY.md) for private
reporting.

## Diagnostics

```bash
tether status
tether doctor
tether unresolved --team T12345678
tether maintenance
```

| Symptom | Action |
| --- | --- |
| Broker unavailable | Restart Hermes, then run `tether doctor`. |
| Socket Mode disconnected | Restore Socket Mode; do not rely on polling alone. |
| Native binding stale | Rebind from the intended live session. |
| Live terminal delivery uncertain | Inspect that exact Herdr agent or Zellij pane, then use `tether resolve`. |
| Upgrade failed | Review automatic rollback; run `tether rollback --restart` if needed. |

## Development

```bash
npm ci
npm test
npm run pack:check
```

Release controls are documented in [RELEASE.md](docs/RELEASE.md).
