# Portable collectors

The collector discovers only Claude Code `*.jsonl` or Codex `rollout-*.jsonl` files under one
explicit root. Complete JSONL records enter a mode-0600 SQLite outbox before network I/O. The
scan offset may move once the spool transaction is durable; the separate committed offset moves
only after BrainStore returns a commit acknowledgement. A lost acknowledgement retries the same
idempotency key.

One process and source-scoped credential serve each harness on Linux or macOS:

```bash
python -m collector.cli run \
  --harness claude --root ~/.claude/projects \
  --source-id claude:linux:host \
  --spool ~/.local/state/recall-brain/collector-claude.db \
  --endpoint https://host.example.ts.net:9443 \
  --token-file ~/.config/recall-brain/collector-claude-token.json \
  --visibility private
```

Each scan invocation is resumably bounded to 1,000 complete JSONL records or 20
seconds by default. Override these closed deployment bounds with
`--max-scan-records` and `--max-scan-seconds`. A bounded slice retains its exact
byte checkpoint and scan membership, never infers deletion from unvisited files,
and resumes before it advances to later work. Newest-modified sessions are
visited first so a fresh installation becomes useful while older history
backfills. An unchanged rerun reads no record bodies and queues no versions.

Canonical archive writes use two concurrent workers by default. Operators can
set `RECALL_ARCHIVE_WORKERS` or pass `--archive-workers` from 1 through 16 for a
temporary backfill, subject to the archive and ingest service capacity.

For a large historical text backfill, `--bulk-manifest-archive` (or
`RECALL_BULK_MANIFEST_ARCHIVE=1`) replaces one archive object per record with one
content-free manifest per bounded group. Full privacy-transformed record content
still enters the canonical event/document store; R2 holds only stable native IDs,
digests, offsets, and format metadata for that group. This keeps exact text
forget-capable without duplicating transcript prose into a shared object. Configure
`RECALL_BULK_BUNDLE_RECORDS` from 1 through 10,000, plus the existing scan-record,
scan-seconds, and interval environment variables. Open/live files may continue on
the normal path, but both paths apply privacy before archive or spool network I/O.

The canonical writer batches up to 500 events under the same 8 MB request bound.
Bundle and event identities are deterministic: a crash or replay converges on the
same artifact and receipts, while a shared manifest is collected only after its
last event reference is forgotten.

`doctor` reports file coverage, parse/dead-letter rate, pending/acked records,
committed files, acknowledgement latency p95, last successful Brain ACK, bounded
scan completion, and stable runtime error state. `locate --receipt ...` maps a central receipt back to its
original local file and exact byte window without retaining acknowledged envelope bodies.
Transport batches are capped at 8 MB under the server's authenticated 12 MB request ceiling.
An oversized row retains its source coordinates and is losslessly recoverable with `recover` after
a limit upgrade; it is never counted as acknowledged while dead.

Large backfills may run non-overlapping `flush --shard-count N --shard-index I` workers. Sharding
is a stable hash of the source path modulo `N`, so one transcript/session is owned by exactly one
worker and cannot deadlock its projection rows; the steady-state watcher remains a single worker.

Structured values whose keys name credentials—including `LITELLM_MASTER_KEY`—are replaced before
spooling. Non-JSONL files and paths outside the configured root are never discovered.

Use `--privacy-mode scrub` to retain safe context with category-labelled
redactions or `--privacy-mode drop` to omit a classified record before it enters
the outbox. The default is explicit `off`. Scan and doctor output show the mode and
content-free action/category counts. A one-time secure compaction protects pending
rows when privacy is enabled on an older spool. See `client/README.md` for preview,
agentic-judge consent and routing, deletion limits, and rollback.

The reproducible macOS bundle in `client/` uses this exact collector class and
spool. Its LaunchAgents resolve source-scoped credentials from Keychain at run
time; the Linux token-file path remains mode-0600 and is never packaged.
