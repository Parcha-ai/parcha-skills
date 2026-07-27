# Tether Compatibility

## Supported release boundary

Tether `0.2.0-beta.1` supports:

- Linux on x86-64 or arm64;
- Python 3.11, 3.12, 3.13, or 3.14;
- a maintained Node.js LTS release: 22 or 24; and
- Hermes Agent exactly 0.19.0, tested at commit
  `b9ba7c78e41b5d187e2c8fb446655c4b71c42aa5`.

Hermes is not bundled. Tether validates the exact Hermes version and required
Slack adapter call signatures at startup and in `tether doctor`. Unsupported
versions fail closed.

Hermes must be loaded from the exact tested Git commit in a clean checkout.
Tracked changes, non-ignored untracked files, and ignored Python overlays in
Hermes source trees fail closed, even when they are outside the Slack adapter.

The installer refuses non-Linux hosts. Native Zellij continuation also depends
on Linux `/proc` identity and Unix-domain sockets. Headless runs can publish
with an explicit `--run-id`; stock pi sessions cannot be resumed natively.

## Binding and database upgrade

This release uses BindingV2 and SQLite schema 15.

A BindingV2 record contains a concrete source, one delivery endpoint, and a
monotonic generation. Rebind and close increment the generation. Legacy or
incomplete native bindings become `rebind_required`; Tether never guesses a
replacement process or session.

At startup, the Store:

1. rejects a database whose `PRAGMA user_version` is newer than 15;
2. opens an immediate migration transaction;
3. applies additive schema migrations and binding backfills;
4. closes older duplicate endpoint owners before enforcing uniqueness;
5. writes schema version 15; and
6. recovers attempts that are proven not to have started external I/O.

The installer snapshots managed code and Hermes plugin state, not the
database. Before upgrading an important host:

1. Stop Hermes.
2. Back up `bridges.db` with SQLite's backup command, or copy it with any
   `-wal` and `-shm` sidecars as one set.
3. Record the Tether version with the backup.
4. Upgrade and start Hermes.
5. Run `tether doctor`, then test one outbound root and one Socket Mode reply.

Code rollback does not downgrade schema 15. If an older runtime cannot read the
database, restore the backup created for that runtime.

## Slack compatibility

Slack Events API delivery through Hermes Socket Mode is the authoritative
ingress path. Tether's `conversations.replies` poller is best-effort recovery,
not a substitute. It may be rate-limited or unavailable to bot tokens for
channel threads depending on token type, scopes, and channel membership.

Tether distinguishes two thread types:

- a thread whose root was posted by Tether has durable local root ownership;
- an existing thread attached with `tether attach` is bound but does not gain
  root ownership.

This distinction can make ambient replies on attached threads fail closed when
current ownership evidence is unavailable.

## CLI compatibility

The CLI and local broker must both use broker protocol 5. Tether rejects older
and unknown newer protocols; upgrade the package and installed runtime
together.

Use `--text-stdin` or `--text-fd FD` for notification and reply text.
Deprecated `--text` remains accepted for compatibility but exposes text in
process arguments and emits a warning.

Configured word, character, and sentence limits are soft writing targets. They
do not reject a complete response. The hard transport limit is 35,000
characters.

The operator recovery commands in this release are:

```bash
tether unresolved [--team T12345678]
tether resolve \
  --team T12345678 \
  --kind ingress|attempt \
  --id ID \
  --action retry|complete|abandon
```

## Credential compatibility

Hermes and Tether must run as the same dedicated non-root Unix user. The broker
rejects UID 0 and local peers with another UID.

Native Codex and Claude Code authentication may use normal user configuration
or an administrator-controlled `credential_command`. Tether validates the
helper path, non-writable ancestors, ownership, mode, file type, and bounded
allowlisted output before use.
