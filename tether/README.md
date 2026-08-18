# Tether

Tether binds a Slack thread to the Codex, Claude Code, Herdr, Zellij, Hermes, or
headless run that created it. Hermes owns the Slack credential. Local clients
use an owner-only Unix socket and do not receive that credential.

`0.3.0-beta.1` is a pre-release. This source tree uses BindingV3 and database schema 17.
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

One continuation endpoint may own multiple bridges and therefore multiple
independent Slack threads. Tether routes ingress by exact workspace, channel,
and thread, and serializes agent turns across every bridge sharing the endpoint.

A Tether-created thread root is durable local proof that the bridge owns that
thread for ambient replies. An explicit local `tether attach` or `tether
rebind` durably claims an existing thread for its exact binding generation.
Allowlisted humans can then reply without a mention; peer bots remain
mention-gated, and replacing the writer still requires an explicit rebind.

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
npx --yes --package=@parcha/tether@0.3.0-beta.1 \
  tether setup --harness=both --herdr
```

Use `--harness=codex` or `--harness=claude-code` for one harness. Setup requires
Hermes 0.19.0, an explicit Slack operator allowlist, and a Slack app configured
for Socket Mode. Omit `--herdr` on a host that uses only Zellij or detached
native sessions.

For a source install, use the full 40-character commit from the matching
release:

```bash
TETHER_COMMIT="<verified-release-commit-sha>"
npx --yes \
  --package="github:Parcha-ai/parcha-skills#$TETHER_COMMIT" \
  tether setup --harness=both --herdr
```

Do not install from a moving branch.

When Tether core is already installed, install the Herdr-native package from
the same reviewed commit:

```bash
herdr plugin install Parcha-ai/parcha-skills/tether/herdr-plugin \
  --ref "$TETHER_COMMIT"
```

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

Inside Herdr, invoke `Tether: Open cockpit` from the plugin action menu. It can
create a thread for the focused Codex or Claude agent, attach a selected or
Ctrl-clicked Slack thread URL, rebind a stale agent, detach, run doctor, and
inspect unresolved work. Create, attach, and rebind disclose and then assign a
visible occupant-bound `tether_…` name when the agent is unnamed.

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
not blindly replayed. Inspect it with:

```bash
tether unresolved --team T12345678
```

The legacy same-UID `tether resolve` mutation is disabled. It cannot safely
distinguish an endpoint/model process from an operator. Resolution remains
unavailable until the service-writer isolation and separate operator authority
channel are attested; do not retry or manually duplicate ambiguous work.

## Lifecycle

Upgrade:

```bash
npx --yes --package=@parcha/tether@0.3.0-beta.1 \
  tether upgrade --harness=both --restart --herdr
```

On Linux, `--restart` detects an active system-level Hermes gateway and uses
Hermes's documented non-interactive system restart path. A restart failure
restores the previous managed state and returns nonzero.

Restore the immediately previous managed payload:

```bash
tether rollback --restart --herdr
```

Install and upgrade take a lifecycle lock, stage a complete payload, snapshot
managed files and plugin state, and maintain a crash-recovery journal. A failed
commit or requested gateway restart restores the previous managed state.

Rollback does not downgrade `bridges.db` or undo Slack settings. Uninstall
retains config, bridge state, snapshots, and locally modified managed files:

```bash
tether uninstall --herdr
```

Inspect database/runtime compatibility without contacting the broker:

```bash
tether schema status --json
```

The command reads only owner-private installed state. It reports the database
schema, runtime capability, logical-manifest digest, explicit security-domain
configuration, incomplete schema receipts, and schema-18 domain blockers. The
schema-18 model is packaged but not activated in this release, so migration
readiness remains false until the schema-18 runtime and coupled lifecycle
orchestrator are installed. There is intentionally no manual or force migrate
command.

Use `--herdr` only when the companion plugin is linked. Rollback reconciles the
link with the restored payload; uninstall removes the link before deleting
unchanged managed plugin files.

See [Operations](docs/OPERATIONS.md) for backup, rollback, diagnostics,
retention, and irreversible state removal.

## Security boundary

Run Hermes and Tether as one dedicated non-root Unix user. The broker refuses
UID 0, creates a mode-`0600` socket, and accepts only peers with the broker UID.
Native child environments are allowlisted and do not inherit Slack
credentials.

The deployed schema-17 runtime still treats the Unix account as its local
authority boundary. Mode `0600` and `SO_PEERCRED` do not isolate processes that
share a UID. Schema-18 native routing and privileged operator resolution must
remain disabled until the Tether state writer is OS-isolated from endpoint and
model processes. Put mutually untrusted agents in separate accounts or hosts.

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
| Live terminal delivery uncertain | Inspect the exact endpoint and keep it blocked; operator mutation remains disabled until the isolated authority channel ships. |
| Upgrade failed | Review automatic rollback; run `tether rollback --restart` if needed. |

## Development

```bash
npm ci
npm test
npm run pack:check
```

Release controls are documented in [RELEASE.md](docs/RELEASE.md).
