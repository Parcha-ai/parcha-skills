# Tailnet-private pilot deployment

## Public MCP deployment profile

`RenderPublicMcpAdapter` creates one digest-pinned Render `web_service` with managed HTTPS,
`/readyz` health checks, bearer authentication, and `RECALL_HTTP_PROFILE=public-mcp`. By
default the application exposes only `/mcp`, its OAuth protected-resource metadata,
`/healthz`, and `/readyz`; no Tailscale gateway,
REST ingest route, metrics route, or doctor route is part of this profile. The separately
gated `RECALL_ADMIN_WEB_ENABLED=1` setting adds only `/admin` assets, authenticated
`/admin/api/v1` routes, and the one-time OAuth callback described below.

PlanetScale IP restrictions require stable egress. `RenderDedicatedIpAdapter` models Render's
separate dedicated-IP resource and refuses to create it unless `purchase_approved` is explicitly
true. The resource is asynchronous and is not ready for database allowlisting until Render
reports `RUNNING` with exactly three IPv4 addresses. It is workspace-scoped to one region, so
database credentials remain the second independent boundary. As of July 2026, Render requires
a Pro-or-higher workspace and bills one dedicated IP set at $100/month:

- <https://render.com/docs/dedicated-ips>
- <https://api-docs.render.com/reference/create-dedicated-ip>

Keep the prior database egress allowlist in place during cutover. Add all three dedicated
addresses as `/32` entries, prove the hosted service can reach PlanetScale, then remove obsolete
egress entries. Dedicated outbound IPs do not restrict inbound MCP traffic; bearer capabilities
and the MCP-only application surface remain mandatory.

`RECALL_HTTP_PROFILE=public-edge` is the opt-in superset for custom incoming evidence. It adds only
`POST /webhooks/v1/events` to the public-MCP route set. The endpoint requires a separate
webhook-capability bearer bound to one source, principal, and `scrub` or `drop` policy; it never
exposes the generic batch-ingest, credential, migration, metrics, doctor, debug, or administrative
surfaces. Use `public-mcp` when no incoming webhook is required.

## Legacy v1 plane retirement (H1-T6)

The v1 tables (`sources`, `source_grants`, `source_events`, `items`, `chunks`, `entities`,
`item_embeddings`, `sessions`, `turn_embedding*`, `projection_watermarks`, `projection_backfills`,
`ingest_batches`, `embedding_projection_watermarks`) no longer receive writes. Webhooks and
`POST /v1/ingest/batches` archive the raw body into `raw_artifacts` and commit through the
canonical plane, so any profile that accepts them needs `RECALL_CANONICAL_V2_ENABLED=1` and the
`RECALL_ARCHIVE_*` settings; without a canonical plane those routes answer `503` and write
nothing. `POST /v1/search`, `/v1/show`, `/v1/related`, and `/v1/session-export` answer
`410 Gone` with `{"error":"gone","code":"legacy_plane_retired","replacement":"<MCP tool>"}`.
`GET /v1/receipts/resolve` stays and resolves canonical receipts.

```text
RECALL_LEGACY_WRITES=0              # 1 = rollback: dual-write the v1 tables again
RECALL_LEGACY_READS=0               # 1 = restore the four v1 read routes
RECALL_LEGACY_INGEST_TENANT_ID=tenant:personal   # tenant for non-tenant-bound v1 callers
```

The tables are dropped thirty days after both flags have been `0` in production; the drop is a
separate change. Flags, rollback steps, the drop order, and the pre-drop checklist live in
`docs/architecture/storage-runbook.md`.

Create one webhook-only credential through an administrative process and write the one-time value
directly to a new private file:

```bash
recall-server token-create source-webhook \
  --source webhook:service:instance \
  --principal owner \
  --scopes webhook \
  --webhook-privacy-mode scrub \
  --output /approved/private/webhook-credential.json
```

The sending service loads that value through its secret manager. It sends only the closed
`WebhookEventV1` body from `openapi.yaml`; source, principal, visibility, provenance, and privacy
mode are not request fields. Rotate by creating a replacement credential, updating the sender's
secret reference, proving one synthetic event, and revoking the prior credential.

## Managed Core preview

`recall-core` is the existing API, projection, and retrieval runtime packaged as one
non-root container. The deployment preview is intentionally offline and non-mutating:

```bash
python -m recall_server.cli deployment-preview \
  --manifest server/deploy/recall-core.plan.example.json
```

It emits only a content-free plan hash, resource kinds, and the five approvals still
required. It does not contact a provider, read a source, render a reference, or apply
infrastructure. The example is synthetic; a live manifest belongs in a private mode-0600
location and contains references, never credential values.

### Parquet scan fragments

The scan plane (`{documents,passages,records,actors}-part-NNNNN.parquet` per
tenant/source/month) is a set of fragments. `canonical_parquet_scan_fragment_documents`
records which logical documents each live part holds; `canonical_parquet_scan_dirty_documents`
records which documents a queued month must reconsider. A queued month rewrites only the
fragments that hold a changed, forgotten, or new document; unchanged siblings keep their
immutable objects. New parts are uploaded first, under indexes above every surviving part,
and the catalog flips in one transaction; replaced objects go through
`canonical_evidence_cleanup_queue`, so a reader holding an object list is never cut off.
Gaps in `shard_index` are normal.

- **Compaction**: a month with more than `RECALL_PARQUET_COMPACTION_FRAGMENTS` (default 16)
  live parts in any dataset, or more than half its recorded documents dead, is rebuilt fully.
  The worker does this through `scan.project_pending(compaction_budget=1)`: at most one
  month per cycle. Raise the cap or lower the budget if the cycle log shows `parquet_rows`
  dominated by compactions; a `reason='backfill'` queue row plus a `*` dirty row forces a
  full rebuild of one month.
- **Cycle log**: `parquet_fragments_rewritten` (parts uploaded), `parquet_fragments_total`
  (live parts), `parquet_documents_dirty`. Expect `parquet_rows` per day to fall by an order
  of magnitude against the pre-fragment plane; `parquet_shards` still counts committed
  source-months.
- **First deploy**: months that have parts but no membership rows are rebuilt fully the next
  time they are queued (no migration-time requeue). Migration 061 only creates two tables
  and an index; migration 051 no longer re-keys the shards table on every run.
- **Manual full rebuild**: `python -m recall_server.cli backfill-parquet-scan --tenant T
  [--source S]` queues every month of the scope with the `*` sentinel; a content-identical
  month is a no-op (`mode=reuse`) and keeps its objects.

### Search projection outbox (H3-a)

`search_projection_outbox` is the queue for the Lance-on-S3 search plane (H3-b), separate
from the parquet queue so the two cadences never couple. Every passage-plane write leaves
its rows in the same transaction: the differential passage commit queues the months of
the passages it inserted or deleted (`logical-update`) and tombstones the deleted ids in
`search_projection_tombstones`; forget tombstones every passage of the forgotten group
and queues its months (`forget`); a header fill queues the months whose header actually
changed (`header-change`). `generation` increments on every re-enqueue, `backfill` is
sticky, `first_queued_at` never moves. `search_projection_shards` is the catalog the
Lance writer fills; it stays empty until H3-b ships.

- **Seed**: `python -m recall_server.cli search-outbox-seed --tenant T [--source S]`
  queues one `backfill` row per existing parquet shard month. Idempotent: a repeat run
  prints `{"pending": N, "seeded": 0}`. Run it once per tenant when the Lance writer is
  deployed; nothing drains the outbox before then.
- **Cycle log**: `search_outbox_pending` on the `projection-worker` line is the number of
  queued source-months for the tenant.

### Search plane (turbopuffer)

The search plane is one turbopuffer namespace per tenant (`recall-<sha256(tenant)[:20]>`)
holding one document per live passage. turbopuffer embeds `embed_text` (contextual header
plus verbatim text) natively and indexes `text` for BM25, so neither the worker nor the
web service calls an embedding provider for the plane. Everything in a namespace is a
projection of the Postgres catalog and can be rebuilt with a seed.

The writer (`recall_server.turbopuffer_projection`) drains `search_projection_outbox`
oldest-first, one (tenant, source, month) at a time: it applies the month's tombstones as
deletes, upserts the month's live passages in batches of `RECALL_TPUF_WRITE_BATCH_ROWS`,
and only after the writes succeed retires the outbox row with a compare-and-delete on the
claimed `generation` (a month re-queued during the write stays queued) and upserts
`search_projection_shards` (`dataset_uri` = `turbopuffer://<region>/<namespace>`,
`row_count` = rows written by that pass, `built_at` = the read watermark). `backfill`
sends every live passage of the month; `logical-update`, `forget` and `header-change`
send only passages created after the shard's `built_at`. A turbopuffer failure on a month
leaves its outbox row, is counted as failed, and is logged by error class only. A 429
(`RateLimitError`: 1024 requests and 2M embedding tokens per minute per organisation)
backs off 1 s, 2 s, 4 s ... capped at 60 s and retries the same batch for up to five
minutes per month, counted as `rate_limited`; a batch is never dropped. Writes are clamped
to 32 MB of row payload as well as `RECALL_TPUF_WRITE_BATCH_ROWS` (the per-namespace
ingest limit is 32 MB/s).

Environment (the worker never logs the key):

| Variable | Meaning |
| --- | --- |
| `RECALL_TPUF_API_KEY` or `RECALL_TPUF_KEY_FILE` | API key inline, or a 0600 file holding it (set one, not both). Unset: the plane is off. |
| `RECALL_TPUF_REGION` | turbopuffer region, default `aws-us-west-2`. |
| `RECALL_TPUF_EMBED_MODEL` / `RECALL_TPUF_EMBED_DIMS` | Native embedding model and dimensions, default `voyage/voyage-4` at 512. Changing either needs a full re-seed. |
| `RECALL_TPUF_NAMESPACE_PREFIX` | Namespace prefix, default `recall`. |
| `RECALL_TPUF_WRITE_BATCH_ROWS` | Rows per write call, default 200. |
| `RECALL_SEARCH_PLANE` | `postgres` (default) or `turbopuffer`: which plane the read path queries. The writer runs whenever a key is configured, regardless of this value, so a namespace can be filled before the read path is switched. On `turbopuffer` no process reads or writes the Postgres vector plane (see "Retire the Postgres vector plane"); once migration 067 is applied `postgres` refuses to start. |
| `RECALL_TPUF_CLIENT_FACTORY` / `RECALL_TPUF_FAKE_STATE` | Test hooks only: `tests.central_brain.fake_turbopuffer:factory` swaps in the in-process fake, file-backed at the state path so a worker and a server share one fake plane. Never set in production. |

Runbook, first deployment for a tenant:

```bash
# 1. Queue one backfill row per existing parquet shard month (idempotent).
python -m recall_server.cli search-outbox-seed --tenant tenant:company:example

# 2. Either let the projection worker drain it (default on when a key is set;
#    --search-plane off disables the phase, --search-plane on requires the key)
python -m recall_server.cli projection-worker --tenant tenant:company:example \
  --skip-embedding --search-plane auto --search-plane-months-per-cycle 4

#    or drain it in the foreground until the outbox is empty.
python -m recall_server.cli search-plane-project --tenant tenant:company:example \
  --max-months 4          # add --once for a single cycle
# {"cycles": 12, "deleted": 0, "failed": 0, "months": 48, "pending": 0, "rate_limited": 3, "requeued": 0, "rows": 812345, "status": "complete"}
```

- **Cycle log**: `search_plane_months`, `search_plane_rows`, `search_plane_deleted`,
  `search_plane_failed` and `search_plane_rate_limited` on the `projection-worker` line,
  next to `search_outbox_pending`; `search_plane_elapsed_ms` attributes the phase's wall
  clock. A cycle claims at most `--search-plane-months-per-cycle` months, so the other
  phases keep running between drains during the hours a backfill takes.
- **Metrics**: `recall_search_plane_pending` (outbox rows, all tenants) and
  `recall_search_plane_shards` (built source-months) on `/metrics`;
  `recall_projection_search_plane_rows_written_total` counts rows since process start.
- **Repair**: a month that keeps failing stays in the outbox with its `generation`; fix the
  cause and the next cycle retries it. A namespace that must be rebuilt (model or
  dimension change) is re-seeded with `search-outbox-seed`, which promotes every month to
  `backfill`.
- **Reconciliation**: `search-plane-reconcile --tenant ... --apply` repairs observed
  missing passages through the projector's current-authority lookup. Its independently
  paged reads are not a shared snapshot: a concurrent insert can appear stale. It never
  deletes inferred-stale rows; `unresolved_stale` reports their count and makes the
  report's `status` `unresolved`. Authoritative outbox tombstones still own deletion.
  This command walks the full namespace and catalog; page size is not a duration or
  temporary-disk bound. Run it explicitly with operator supervision, not as an
  unattended recurring job or a proof of exact coverage.
- **Cost**: native embeddings bill at about $0.06 per million tokens of `embed_text`; the
  organisation rate limit is about 2M embedding tokens per minute, so a full backfill of
  the production corpus takes roughly 8 hours regardless of `--max-months`. Incremental
  drains embed only the passages that changed.

#### Retire the Postgres vector plane (H3-e')

Once the read path runs on turbopuffer, the Postgres vector/tsvector plane is dead
weight: `canonical_passage_embeddings` (halfvec + HNSW), `canonical_embedding_ledger`,
and the `canonical_passages.search_vector` generated column with its GIN index (the
largest remaining tsvector; `canonical_chunks.search_vector` stays because `show` and
the legacy paths still read it). Migration `067_retire_postgres_vector_plane.sql` drops
them. It is destructive, so `migrate` never applies it on its own:

- `python -m recall_server.cli migrate` runs every file through 066 as before (all
  idempotent) and reports `"deferred": [67]` on either plane. Once 067 is recorded the
  files below it are skipped (041 and 063 reference the dropped objects), so the
  delete-a-version-row-to-replay-a-repair trick ends with the retirement;
  `*_concurrent.sql` companions still run every time.
- `python -m recall_server.cli migrate --retire-postgres-plane` applies 067, and only from
  a process with `RECALL_SEARCH_PLANE=turbopuffer` in its environment. From a postgres-plane
  process it exits 2 with `refusing to apply migration 067 ...` and changes nothing.
- After 067 every process must run with `RECALL_SEARCH_PLANE=turbopuffer`: a store started
  with `RECALL_SEARCH_PLANE=postgres` (web, workers, `cli migrate`) refuses to open its pool
  with `migration 067 retired the Postgres vector plane ...`. `capability-check` accepts a
  database with all mandatory migrations, including 068. The embeddings table must
  exist when optional migration 067 is absent and be absent when 067 is recorded;
  disagreement is `schema_drift`.

On the turbopuffer plane the writers already leave the retired objects alone, before and
after 067: the differential passage commit neither captures nor re-attaches embeddings,
`embed_pending`, `passage-embed-plan` and the contract coverage report
`not-applicable` with nothing pending, the ledger counters read 0, `projection-worker`
skips the embedding phase, and `embedding-worker` exits 2 with
`embedding-worker is not applicable on the turbopuffer search plane: suspend this
service`. `/metrics` exports `recall_passages_unembedded 0` and
`recall_embedding_daily_total 0`, so the card's `freshness.projection_churn`,
`freshness.embedding_lag` and `cost.storage` probes stay green without the table.

Prerequisites, per tenant:

1. The drain is complete. `search-plane-status` compares the live passages under the
   current policy fingerprint with the namespace's `approx_row_count`; `drift` is the
   difference and must be about 0 (turbopuffer's count is approximate), with
   `outbox_pending` 0:

   ```bash
   RECALL_SEARCH_PLANE=turbopuffer python -m recall_server.cli search-plane-status \
     --tenant tenant:company:example
   # {"drift": 0, "namespace": "recall-...", "outbox_pending": 0, "passages": 812345, "policy_fingerprint": "...", "rows": 812345, "search_plane": "turbopuffer", "shards": 48, "status": "ok", "tenant_id": "tenant:company:example"}
   ```

2. The card is green on the turbopuffer plane for the nightly run (the read path flipped
   with `RECALL_SEARCH_PLANE=turbopuffer`, accuracy and latency probes passing).
3. The `embedding-worker` service is suspended (it exits on its own on the turbopuffer
   plane, but suspend it so the platform stops restarting it).

Then, from a shell with the turbopuffer plane configured:

```bash
RECALL_SEARCH_PLANE=turbopuffer python -m recall_server.cli migrate --retire-postgres-plane
# {"applied": [67], "current_schema_version": 67, "deferred": [], "postgres_vector_plane": "retired", "schema_version": 67, "skipped": 66, "status": "ok"}
```

A second run reports `"applied": []` and `"skipped": 67`. Refresh runtime grants
afterwards as after every migration.

Rollback boundary: **none after 067**. Before 067, rolling back is one environment flip,
`RECALL_SEARCH_PLANE=postgres` on every service (the Postgres arms, embeddings and
ledger are all still there, and the embedding worker resumes where it stopped). After
067 the vectors are gone; the turbopuffer namespace is the only search plane and is
rebuilt, if ever needed, with `search-outbox-seed` plus a drain.

The production database gate requires a standard PostgreSQL URL with
`sslmode=verify-full` and an explicit trust root. Schema migrations 1 through 70
are supported: versions through 69 are required except optional migration 67,
which retires the Postgres vector plane and is applied explicitly from the
turbopuffer plane. Migration 70 adds only a reconciliation performance index;
serving accepts both schema 69 and 70 and reports the actual recorded version.
Optional [search authority indexes](../operations/README.md) are applied explicitly
without changing those schema versions or restarting workers.
The gate also requires
pgvector 0.8.0 or newer, and a runtime role without superuser, database/role creation,
replication, or RLS-bypass privilege:

```bash
python -m recall_server.cli capability-check
```

For a rolling 69-to-70 upgrade, first deploy a runtime that accepts both versions
to every service while the database remains at 69. Then explicitly build and
verify the concurrent reconciliation index and record migration 70. Keep that
compatible runtime as the rollback floor: older exact-69 capability checks reject
a database recording 70. Serving never applies migrations. The generic `migrate`
command still applies 70 and its concurrent companion; a recorded marker alone
does not prove that a concurrently built index finished successfully.

`--profile local-fixture` is a visibly non-production exception restricted to a
loopback PostgreSQL fixture. It never reports production readiness.

The separate approval document is owner-only, bound to the exact preview hash, and
contains only explicit booleans plus the approved billing and region slugs. Validate it
without applying anything:

```bash
python -m recall_server.cli deployment-approval-check \
  --manifest server/deploy/recall-core.plan.example.json \
  --approvals /private/approvals.json
```

Infrastructure reconciliation remains impossible until billing, region, provider
authorization, and the Tailnet route are all approved. Writer cutover is a separate
approval and is never inferred from infrastructure approval. Provider adapters receive
only the closed desired state and return content-free receipts; repeated reconciliation
must converge to `unchanged` without duplicate resources.

The managed pilot profile is deliberately one stack:

```text
Grep/Codex/Claude/Mac collectors
              |
       Tailnet HTTPS :9443
              |
  Render private Tailscale gateway
              |
      Render private network
              |
      Recall Core :8788 -------- HTTPS --------> managed embeddings
              |
 PlanetScale Postgres (Virginia, HA, bounded autoscaling)
```

There is no Render public web service and no Tailscale Funnel. Core requires a
revocable bearer credential even after the Tailnet boundary. Grep agents use the
stable MCP Streamable HTTP endpoint at `https://<tailnet-host>:9443/mcp`; the
same bearer token and source scope apply to both MCP tools and REST reads.
`recall_search`, `recall_related`, and `recall_show` are read-only. Browser
clients must match `RECALL_MCP_ALLOWED_ORIGINS`; server-side agents omit
`Origin`.

The live adapter profile pins PlanetScale `PS_80` with two replicas, 50 GiB
initial storage, a 1 TiB autoscaling ceiling, PostgreSQL 17, and the current
PlanetScale Virginia slug `us-east`. It creates only two digest-pinned Render
private services: Starter Core and a Starter Tailscale gateway with a 1 GiB
identity disk. The manifest selects a managed `voyage` or OpenAI-compatible
embedding endpoint over exact-match HTTPS; no dedicated embedding service is
created. Existing service environment variables, secret files, image digests,
commands, plans, disks, and regions must match exactly or reconciliation fails
without mutation.

Hosted embeddings receive the redacted text projection selected for semantic
indexing. The example uses `voyage-4` at 512 dimensions; operators who cannot
send that projection to a provider should use the self-hosted TEI profile in
the existing-host section instead. The managed profile uses the validated
2,000ms database-work ceiling because a remote database round trip cannot meet
the 300ms loopback default reliably at multi-million-item scale.

Inject credentials from the approved 1Password Environment at runtime. Never
put their values in arguments, a manifest, an approval file, shell history, or
the repository:

```text
PLANETSCALE_SERVICE_TOKEN_ID
PLANETSCALE_SERVICE_TOKEN
RENDER_API_KEY
RECALL_DATABASE_URL
RECALL_EMBEDDING_API_KEY
RECALL_ARCHIVE_ACCESS_KEY_ID
RECALL_ARCHIVE_SECRET_ACCESS_KEY
RECALL_ARCHIVE_NAMESPACE_KEY
TAILSCALE_OAUTH_CLIENT_ID
TAILSCALE_OAUTH_CLIENT_SECRET
```

For a managed Cloudflare R2 raw archive, also set the non-secret configuration:

```text
RECALL_ARCHIVE_BACKEND=r2
RECALL_ARCHIVE_BUCKET=recall-raw-owner
RECALL_ARCHIVE_ENDPOINT_URL=https://ACCOUNT_ID.r2.cloudflarestorage.com
RECALL_ARCHIVE_REGION=auto
```

The R2 credential must have Object Read & Write permission for that bucket only.
`RECALL_ARCHIVE_NAMESPACE_KEY` is a separate base64-encoded 32-byte random key;
do not derive it from either R2 credential. Recall uses immutable opaque object
keys and conditional writes because R2 does not expose S3 object version IDs.
R2 rejects S3 SSE headers and applies provider-managed encryption automatically.
Recall does not read a Cloudflare management API token. Validate the configured
archive with `python -m recall_server.cli archive-check`; the probe writes,
replays, reads, deletes, and verifies absence using synthetic bytes, then emits
only a content-free status.

Operational raw-object and bulk-ingest manifests intentionally contain digests,
counts, cursors, and status only. They prove replay and integrity without
copying transcripts, secrets, or PII into logs and deployment evidence. Full
deep-search content lives in a different projection: the canonical
privacy-processed chunk text plus exact Recall receipts, stored in a separate
private evidence bucket. Never mount the raw archive into an external compute
provider.

Deep inspection is hard-bound to exactly one brain tenant per service instance,
evidence bucket credential, and Archil disk:

```text
RECALL_EVIDENCE_ENABLED=1
RECALL_EVIDENCE_TENANT_ID=tenant:company:example
RECALL_EVIDENCE_ARCHIVE_BACKEND=r2
RECALL_EVIDENCE_ARCHIVE_BUCKET=recall-evidence-company-example
RECALL_EVIDENCE_ARCHIVE_ENDPOINT_URL=https://ACCOUNT_ID.r2.cloudflarestorage.com
RECALL_EVIDENCE_ARCHIVE_REGION=auto
RECALL_EVIDENCE_ARCHIVE_ACCESS_KEY_ID=<injected secret>
RECALL_EVIDENCE_ARCHIVE_SECRET_ACCESS_KEY=<injected secret>
RECALL_EVIDENCE_ARCHIVE_NAMESPACE_KEY=<independent base64 32-byte secret>
RECALL_DEEP_INSPECTOR=archil
ARCHIL_API_KEY=<injected secret>
RECALL_ARCHIL_DISK_ID=dsk_replace_me
RECALL_ARCHIL_REGION=aws-us-west-2
```

Give the evidence R2 credential Object Read & Write access to that evidence
bucket only, then attach only that bucket to the tenant's Archil disk.
Do not reuse a bucket or disk across personal and company brains. Validate and
populate it before enabling the MCP tool:

AWS S3 is also supported. Use `RECALL_EVIDENCE_ARCHIVE_BACKEND=s3` for a
versioned bucket or `s3-unversioned` for a bucket without versioning, an exact
regional endpoint such as `https://s3.us-west-2.amazonaws.com`, and the same
bucket-scoped access-key variables. The unversioned profile preserves
immutability with content-addressed keys and conditional writes. Archil
serverless execution requires an Archil disk in a supported AWS region; that
disk may mount the same S3 bucket.

```bash
python -m recall_server.cli evidence-archive-check
python -m recall_server.cli backfill-canonical-evidence \
  --tenant tenant:company:example --batch-size 100 --max-batches 10
python -m recall_server.cli canonical-evidence-worker \
  --tenant tenant:company:example
```

The source-native evidence projection stores one logical session, thread, or
document revision rather than one object per retrieval chunk. Each JSONL record
contains the complete privacy-processed source content, structural role and
time metadata, and every canonical receipt for that source record. Documents
larger than one object are represented by ordered immutable parts behind one
opaque manifest. Oversized collector records are restored from the separately
credentialed raw archive and verified against their declared size and digest
before projection.

Migration 39 adds the logical-document catalog, dirty-session queue, and
durable object-cleanup queue. Migration 40 materializes source-record order so
large-session projection does not repeatedly parse archived JSON in PostgreSQL.
Populate it without removing the prior projection:

```bash
python -m recall_server.cli backfill-logical-evidence \
  --tenant tenant:company:example --source source:google.gmail:example \
  --batch-size 100 --max-batches 100 \
  --upload-concurrency 2
python -m recall_server.cli logical-evidence-worker \
  --tenant tenant:company:example --upload-concurrency 2
```

After a retention-policy or projection-format change, intentionally replace
every current logical document through the same tenant-scoped path:

```bash
python -m recall_server.cli backfill-logical-evidence \
  --tenant tenant:company:example --rebuild-existing \
  --batch-size 2000 --max-batches 100 --upload-concurrency 16
```

Ingest and forget transactions enqueue only affected logical documents.
Revision replacement queues superseded S3 references in the same database
transaction; transient delete failures remain durable and are retried by the
worker. The operational default of 25 documents over two upload streams keeps
memory and database load boring. One-time backfills may use up to 2,000
documents per batch and raise `--upload-concurrency` as high as 32 only when
`RECALL_DATABASE_POOL_MAX_SIZE` is at least the same value and a live
throughput/memory measurement justifies it. The worker pre-warms that bounded
pool before starting the measured projection. Keep the prior
`canonical-evidence-worker` running until the
logical-document integrity, rollback, and consumer cutover gates pass.

Migration 41 adds the disposable, document-linked lossless passage index. It
packs exact visible user and assistant message bytes into overlapping passages
without crossing logical documents. Tool text remains available through the
canonical sparse index; no completion model summarizes, classifies, or segments
ingestion.

```bash
python -m recall_server.cli backfill-lossless-passages \
  --tenant tenant:company:example --target-tokens 1024 \
  --overlap-tokens 128 --batch-size 100 --max-batches 100 \
  --concurrency 8
python -m recall_server.cli lossless-passage-worker \
  --tenant tenant:company:example --target-tokens 1024 \
  --overlap-tokens 128
```

The passage policy is versioned by fingerprint. Evaluate alternative target and
overlap values on a shadow database; production keeps one policy and deletes
losing variants after cutover.

Migration 42 adds fingerprinted shadow passage representations for retrieval
experiments. Each arm remains tenant- and source-scoped, points back to the
same lossless canonical passage, and can be removed by fingerprint. Contextual
text and provider vectors are derived projections; they do not replace or
truncate canonical evidence. Provider inputs use a fingerprinted 7,000-byte
head/tail excerpt so one unusual code token cannot exceed a managed model's
context window; a retrieval hit still resolves to the complete canonical
passage and S3 document. Do not route production retrieval to a shadow arm
until its private optimize and validation gates pass.
Large resumable backfills may be split into deterministic, non-overlapping
passage-ID shards without changing representation fingerprints.

Migration 45 adds actor attribution independently of authentication principals.
It links stable actors and explicit relations to sources, events, logical
documents, and passages. Existing raw artifacts do not change: after binding an
employee's enrolled sources, run the existing logical-document rebuild, passage
projection, and actor-aware contextual-representation backfill. Person filters
remain inside the tenant and authorized-source boundary. See
[`../../docs/actor-attribution.md`](../../docs/actor-attribution.md) for relation
semantics, the retrofit sequence, and the cutover gate.

Migration 46 adds the employee display name to invitations. Acceptance now
creates and links the employee actor, and source-local route enrollment binds
that actor as the source contributor. Apply it before inviting employees; mixed
schema/application deployment is intentionally unsupported.

Migration 49 repairs actor bindings for legacy `coding_history` sources whose
owner principal already maps to a brain actor. It deliberately does not infer an
author for Slack, email, or any other shared source. Migration 50 adds a derived
Parquet scan plane; migration 53 adds its compact `passages` planning dataset.

Migration 60 re-keys every child of `canonical_evidence_documents` (parts,
passage documents, passages, actor links, the passage queue) to the stable
`(tenant_id, source_id, logical_document_id)` identity, so a logical
document revision is an in-place update and a session append no longer
cascades through passages, embeddings, contexts, and actors. Forget still
deletes the catalog row and cascades. Runbook: stop the projection worker,
then run `migrate` once as usual. `060_stable_projection_keys.sql` runs in
one transaction and holds only brief `ACCESS EXCLUSIVE` locks while it drops
the revision-keyed constraints and adds the replacements as `NOT VALID`
(metadata only; no table scan). Its companion
`060b_stable_projection_keys_concurrent.sql` runs afterwards, statement by
statement in autocommit: each `VALIDATE CONSTRAINT` scans its table once
under a `SHARE UPDATE EXCLUSIVE` lock (the `canonical_passages` scan is the
long one; reads and writes continue), then `CREATE UNIQUE INDEX
CONCURRENTLY` builds the `(…, policy_fingerprint, ordinal)` key and drops the
revision-keyed document index. Between the two files passages briefly lack a
document-ordinal uniqueness guarantee, which is why the worker stays stopped
until `migrate` returns. Every statement is idempotent, so a rerun is safe.

Passage ids are stable (H1-T3): a passage is identified by its tenant, source,
logical document, policy fingerprint, text hash and canonical span JSON, never
by revision or ordinal, and the passage projector commits differentially
(delete the windows that disappeared, insert the new ones, keep the rest in
place with their embeddings). Before the first deploy that carries the new id
function, run the read-only parity gate on a sample of production sessions;
it recomputes the passages of up to `--limit` current passage documents of one
source and reports counts only (no text):

```bash
python -m recall_server.cli passage-shadow-diff   --tenant tenant:company:example --source codex:example --limit 50
```

`receipt_parity` must be `true` (every compared document covers the identical
receipt multiset). `ids_shared` is 0 for rows written by the old id function
and equals `passages_existing` after the first re-projection; the first
re-projection of each session after the deploy is therefore a one-time full
rewrite (all old ids are deleted, embeddings are re-attached by content hash),
after which appends only insert the new tail windows. The worker log reports
`passages_inserted`, `passages_deleted` and `passages_retained` per cycle;
`passages` stays equal to `passages_inserted`.
Each authorized source and UTC month has `documents`, `passages`, `records`, and
`actors` shards. Passages contain bounded visible-message text, time, attribution,
and receipt pointers; records retain complete projected JSON for exact inspection.
During the one-time passage upgrade, existing immutable `documents`, `records`, and
`actors` objects are retained and only `passages` is materialized. Later source changes
continue to take the ordinary full replacement path.
Canonical JSONL documents remain the complete evidence and only source of truth.
Ingest, replacement, passage projection, and forget transactions invalidate only
the affected source-months. First application seeds existing source-months for a
rebuild after upstream logical and passage projection has drained.

Populate or rebuild it independently of ingestion:

```bash
python -m recall_server.cli backfill-parquet-scan \
  --tenant tenant:company:example --batch-size 4 --max-batches 100
```

The managed projection worker continuously drains later invalidations. Before
enabling `recall_scan`, publish the official DuckDB CLI into the private
evidence archive for **every architecture Archil may execute on**. Archil has
run sandboxes on both aarch64 and x86_64; a build for the wrong machine stages
successfully but fails with `Exec format error`. The sandbox selects the build
matching `uname -m` at execution time and, when no build exists for that
machine, `duckdb` exits 69 with an explicit message instead. Pin both the
release zip digest (GitHub's published asset digest) and the extracted
binary's digest; the command rechecks both before upload:

```bash
python -m recall_server.cli publish-archil-duckdb \
  --release-zip-url https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-linux-arm64.zip \
  --zip-sha256 <asset digest> --version 1.5.5 --sha256 <binary digest> --arch linux-arm64
python -m recall_server.cli publish-archil-duckdb \
  --release-zip-url https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-linux-amd64.zip \
  --zip-sha256 <asset digest> --version 1.5.5 --sha256 <binary digest> --arch linux-x86_64
```

`--path /private/duckdb` remains available for an already-verified local file.

Configure the returned opaque object identities as
`RECALL_ARCHIL_DUCKDB_OBJECT_KEY` / `RECALL_ARCHIL_DUCKDB_SHA256` (arm64) and
`RECALL_ARCHIL_DUCKDB_X86_64_OBJECT_KEY` / `RECALL_ARCHIL_DUCKDB_X86_64_SHA256`
(x86_64). Either pair alone is accepted, but only sandboxes of that
architecture can then run DuckDB.

The execution host must also provide util-linux `setpriv`: after mounting the authorized
view, both exec and scan drop all capability sets and enable `no_new_privs`
before running the caller's shell. Missing or failing `setpriv` fails closed.
The Linux kernel regression can be run explicitly as root with
`python server/tests/e2e_agent_namespace.py`; it uses only synthetic local
objects and checks mount escapes, normal subprocess/thread behavior, and
network namespace routes. It does not prove every possible sandbox escape or
replace a real DuckDB/provider compatibility check.

At execution time Recall selects only shards inside the caller's tenant/source grants.
Before network isolation, it downloads selected document, passage, and actor
objects through signed archive URLs with 16 concurrent workers, verifies their
sizes and SHA-256 hashes,
and binds the local files read-only into the authorized view. Existing record
objects retain lazy Archil reads so a records count need not copy raw bodies;
records missing from Archil use the verified downloader. A missing archive
object can still be read from Archil; an object absent in both stores is reported
unavailable. The writable download aliases and signed inventory are removed
before the caller's program starts. This avoids repeated remote filesystem
metadata and Parquet footer reads without limiting the selected inventory.

The outer Archil request uses its default mount mode because `readOnly: true`
fails delegation check-in on teardown; the inner authorized mounts and capability
drop enforce the caller's read-only boundary. Recall stages the checksum-pinned
DuckDB binary into that networkless sandbox and verifies every
emitted `recall://` receipt against current canonical evidence before returning
it in `opened_receipts`. The caller's agent writes one shell/DuckDB program; no
second retrieval agent or model credential runs inside Recall.
`recall_scan` defaults to 60 seconds and permits a caller-selected ceiling up to
240 seconds, below Archil's documented five-minute response limit. The MCP
client may cancel its own request sooner; Recall does not impose the 30-second
interactive-document limit on these broad scans. Scan stdout is capped at 16 KiB
with explicit truncation and `complete=false`; callers should return compact
passage-derived candidate IDs and open selected full documents separately.

Size the database pool for ingest fan-in. `RECALL_DATABASE_POOL_MAX_SIZE`
(default 8, range 4 to 32) bounds pooled PostgreSQL connections per process.
Every collector host posts archive objects and canonical batches through the
same web service that serves MCP, so a pool sized for a single client will
saturate under a fleet: `/readyz` then answers `busy` (200) for up to two
minutes after its last successful probe rather than `not_ready`, and requests
that cannot obtain a connection within five seconds receive `503 brain_busy`
with a `Retry-After` header that collectors honor with jitter. Persistent
`recall_http_pool_busy_total` growth in `/metrics` means the pool, the
database tier, or the number of collector hosts needs attention.

Recall is write-dominated (roughly 1,100 collector writes for every 50 MCP
reads per day), so two hot write paths are cached or batched in process:

- `RECALL_IDENTITY_CACHE_TTL_SECONDS` (default 600; `0` disables) and
  `RECALL_IDENTITY_CACHE_MAX` (default 10000) bound the identity-write cache.
  Every collector write registers its tenant, principal, source, owner grant,
  and derived read grants; the cache remembers identity tuples whose source
  row was already committed so a repeat write runs zero registration
  statements. Only positive results are cached, a brand-new source is never
  cached inside its own transaction, and membership changes (brain
  provisioning, invitation acceptance, member revocation) invalidate the
  tenant. The cache is per process; the TTL bounds staleness across replicas.
- `RECALL_AUDIT_BATCH_ROWS` (default 500; `0` writes every row synchronously)
  and `RECALL_AUDIT_BATCH_SECONDS` (default 2) batch the authorization audit
  rows for allowed MCP decisions. Denied decisions are always written
  synchronously so the row is durable before the 403 returns. Allowed rows
  wait in a bounded queue (four batches deep) that a daemon thread flushes at
  the row or time threshold, whichever comes first; on overflow the caller
  writes its row synchronously, so no audit row is dropped. Shutdown flushes
  the queue, and `BrainStore.flush_authorization_audit()` forces a flush for
  tests and operators. The `authorization_audit_events` schema is unchanged.

Enable the canonical v2 write plane only after the archive probe and database
migrations pass:

```text
RECALL_CANONICAL_V2_ENABLED=1
RECALL_CANONICAL_INGEST_PUBLIC=1
RECALL_TENANT_ID=tenant:personal
RECALL_PRINCIPAL_ID=principal:owner
```

`RECALL_CANONICAL_INGEST_PUBLIC=1` adds only the authenticated
`POST /v2/archive/objects` and `POST /v2/ingest/canonical` routes to a public
profile. Both require a write credential bound to the exact tenant, principal,
and source. Create one such credential per source with `token-create --tenant
TENANT --principal PRINCIPAL --source SOURCE --scopes write`. The connector
runner archives raw bytes through the fenced archive route, applies privacy,
writes only the redacted envelope to its private spool, and advances its cursor
only after the canonical ACK. Do not enable this flag on an MCP-only service
that has no collector ingress.

After canonical ingestion is live, provision each personal or company brain and
mint a separate audience-bound MCP credential:

```bash
python -m recall_server.cli brain-provision \
  --organization org:owner --kind personal --display-name "Personal" \
  --tenant tenant:personal --slug personal --owner-principal principal:owner
python -m recall_server.cli mcp-token-create owner-personal \
  --tenant tenant:personal --principal principal:owner \
  --principal-kind workload \
  --scopes read,forget \
  --output /approved/private/owner-personal-mcp.json
```

Set `RECALL_CANONICAL_MCP_ENABLED=1` only after migration 28 and canonical
embedding backfill pass. In this mode, public MCP accepts only unexpired
`recall-mcp` credentials bound to one tenant. Retrieval intersects brain access
with explicit canonical source grants and never reads the legacy evidence
projection. A single principal can hold separate personal and company
credentials without gaining an implicit cross-brain view. Omit the optional
`forget` scope for read-only agents; canonical forget also requires an owner
grant on the exact source.

The hosted MCP is model-free. The caller's own agent interprets the question,
uses `recall_search` for high-recall pointers, optionally uses `recall_show`
for exact context, and runs bounded read-only shell or Python over selected full
documents with `recall_exec`. Search results are hints; only host-verified
`opened_receipts` from show or exec authorize citations. Recall owns tenant and
source grants, exact target resolution, read-only mounting, network isolation,
resource bounds, and receipt verification. No model key or nested agent runtime
belongs in the Recall service.

### Human OAuth and company-brain invitations

Static MCP tokens remain the simplest machine-to-machine option. For people,
configure one OAuth resource instead of issuing long-lived bearer tokens:

```text
RECALL_MCP_RESOURCE_URI=https://<public-host>/mcp
RECALL_AUTHORIZATION_SERVERS=https://<authorization-server>
RECALL_MCP_AUTH_PROVIDER=oidc
RECALL_OIDC_ISSUER=https://<authorization-server>
RECALL_OIDC_JWKS_URI=https://<authorization-server>/.well-known/jwks.json
```

Set the provider to `descope` for Descope and use the issuer and JWKS URLs shown
for its MCP resource. Recall needs no Descope management key: the provider owns
login, MFA, and account recovery; Recall owns invitations, tenant mapping,
roles, source grants, and revocation. The access token must have the exact
`RECALL_MCP_RESOURCE_URI` audience, a `read` scope, and a provider-verified email
for first-time invitation acceptance.

To turn Descope's standard MCP consent flow into the branded Recall experience,
name the flow `recall-mcp-user-consent` and run the setup-only operator script:

```bash
RECALL_DESCOPE_PROJECT_ID=P_replace_me \
RECALL_DESCOPE_MGMT_KEY='<setup-only management key>' \
node server/scripts/configure_descope_brand.js
```

The script discovers the welcome, OTP, verified-consent, and unverified-consent
screens by their component contracts rather than project-specific screen IDs. It
preserves every flow interaction, applies the Recall palette and copy, and updates
inbound apps already assigned to that flow to use the managed dark host. It uses
only Node's built-in APIs and is safe to rerun. The management key is not an
application runtime dependency; remove it from the operator environment after
setup unless invitation email delivery below also uses Descope.

Recall follows the company-wide Grep visual contract: JetBrains Mono, square
corners, and literal product copy. The operator script pins both Descope font
slots to JetBrains Mono and every global and component border-radius token to
`0px`. Do not add slogans, trust strips, or rounded surfaces.

For the OAuth-first browser experience, create one confidential Descope Inbound
App with the callback below and the `openid email recall.identity` scopes. The
app-local `recall.identity` permission has no role mapping and authorizes only
identity confirmation; Recall remains the authorization system for brains:

```text
RECALL_IDENTITY_OAUTH_CLIENT_ID=<confidential Inbound App client ID>
RECALL_IDENTITY_OAUTH_CLIENT_SECRET=<server-side Inbound App secret>
RECALL_IDENTITY_OAUTH_REDIRECT_URI=https://<public-host>/admin/oauth/callback/identity
RECALL_BOOTSTRAP_OWNER_EMAILS=owner@example.com
```

PKCE and one-use, ten-minute state are enforced even for the confidential app.
Recall calls UserInfo, requires a verified email, discards the provider tokens,
and creates only a 12-hour Recall browser session. On a fresh installation the
allow-listed email may bind the single unclaimed owner principal once. Afterwards
the durable Descope subject—not email—is authoritative. If multiple owner
principals exist or another identity is already bound, claiming fails closed.

In `/admin`, create a company invitation. By default Recall displays the
brain-specific MCP URL for manual sharing. To send an onboarding email with a
brain-specific setup page, enable one server-side delivery provider:

```text
# Use the same isolated Descope project that authenticates Recall users.
RECALL_INVITATION_EMAIL_PROVIDER=descope
RECALL_DESCOPE_PROJECT_ID=P_replace_me
RECALL_DESCOPE_MGMT_KEY=<server-side management key>

# Or use a provider-neutral transactional email account.
RECALL_INVITATION_EMAIL_PROVIDER=resend
RECALL_INVITATION_EMAIL_FROM=Recall <recall@example.com>
RECALL_INVITATION_EMAIL_API_KEY=<server-side API key>
```

Configuration is explicit and all-or-none. Recall never renders or logs either
secret. A delivery failure leaves the email-bound authorization pending and is
shown in the admin UI; re-inviting the same address safely replaces the pending
invitation and retries delivery. The setup page presents the current Codex and
Claude Code setup blocks immediately. The client's Descope login activates the
exact email-bound invitation on its first MCP request, so the primary path has
only one login. Browser identity also offers an optional pre-activation path. See
[`docs/authorization-v1.md`](../../docs/authorization-v1.md) for the policy,
generic OIDC contract, and revocation semantics.

If the authorization server does not support dynamic client registration,
pre-register a public PKCE client for Codex and configure both values below.
The callback URL must match Codex's localhost callback for the brain-specific
MCP URL. Recall then renders a single `codex mcp add` command with the public
client ID, resource indicator, and fixed callback port; no client secret or
bearer token is distributed.

```text
RECALL_CODEX_OAUTH_CLIENT_ID=<public OAuth client ID>
RECALL_CODEX_OAUTH_CALLBACK_PORT=8765
```

## Unified connector administration

Schemas 029–031 add one tenant-aware connector control plane for the web
switchboard and native utilities. A connector installation is always bound to one principal,
one destination brain, and one opaque source ID. Connecting the same provider to
personal and company memory creates separate installations; it never implies a
cross-brain grant.

Enable the browser and native control surface only after injecting the required
owner boundary through the runtime secret manager:

```text
RECALL_ADMIN_WEB_ENABLED=1
RECALL_CONTROL_ENCRYPTION_KEY=<base64url-encoded random 32 bytes>
```

Google Workspace is optional. It becomes available only when all three values
below are configured; a partial set fails startup closed:

```text
RECALL_GOOGLE_CLIENT_ID=<Google web application client ID>
RECALL_GOOGLE_CLIENT_SECRET=<Google web application client secret>
RECALL_GOOGLE_REDIRECT_URI=https://<public-host>/admin/oauth/callback/google
```

Composio is the hosted connection option. It is also optional and can coexist
with direct Google OAuth; its API key and callback are an all-or-none pair:

```text
RECALL_COMPOSIO_API_KEY=<scoped server-side project key>
RECALL_COMPOSIO_REDIRECT_URI=https://<public-host>/admin/oauth/callback/composio
```

Slack is a direct managed source. Configure all four values or none; partial
configuration fails startup closed:

```text
RECALL_SLACK_CLIENT_ID=<Slack app client ID>
RECALL_SLACK_CLIENT_SECRET=<Slack app client secret>
RECALL_SLACK_SIGNING_SECRET=<Slack app signing secret>
RECALL_SLACK_REDIRECT_URI=https://<public-host>/admin/oauth/callback/slack
```

Generate a paste-ready Slack app manifest from the exact public origin:

```bash
python -m connectors.slack_manifest https://<public-host> > /tmp/recall-slack-app.json
```

Create the Slack app **from a manifest**, paste that JSON, then copy the app's
client ID, client secret, and signing secret into the deployment secret manager.
After the deployment is healthy, generate the final manifest with
`python -m connectors.slack_manifest --events https://<public-host>` and update
the app from that manifest; Slack can now verify the signed Events URL. The owner
completes installation from the Recall switchboard; Recall exchanges the OAuth
code and encrypts both bot and user tokens server-side.

Register `https://<public-host>/webhooks/v1/slack` as the Events API request URL
and subscribe only to `message.channels`, as the generated manifest specifies.
The user grant requests `channels:history`, `channels:read`, and `files:read`.
Polling discovers active and archived public channels and reads their history
and thread replies through a fixed cycle upper bound. Private channels and DMs
are outside this connector's scope. Existing bot joins for discovered active
public channels preserve Events membership; signed Events capture subsequent
edits and deletions. An inventory count alone does not prove full history or
Events coverage. See [channel coverage and polling limits](../../connectors/README.md#work-apis).

To upgrade a legacy bot-only connection, open `https://<public-host>/admin` as
the same Recall principal that owns the existing Slack installation. In the
Slack card, explicitly select the existing company brain, leave Slack enabled,
and click **Authorize Slack**. The destination selector defaults to a personal
brain when one exists; it does not preserve the existing Slack route. Select the
same Slack workspace and approve the requested user scopes. Do not disconnect
first. Reauthorizing the same principal, brain, and connector preserves the
source ID and spool; another principal or destination creates another source.
Pending pages finish their acknowledgement before the expanded public scope
starts an epoch replay with the same native message IDs. Verify the resulting
coverage report says `public_channels` and then observe its per-channel baseline
progress; reconnecting alone is not a completeness claim.

Files served directly from Slack's private file host are downloaded with the
user authority, bounded at 64 MiB, archived as raw artifacts, and projected
as stable `document.v1` records; supported text/PDF/Office bodies become
searchable. A rejected redirect, oversized file, or unavailable binary remains
an explicit `attachment_bytes` omission on the parent message and is repaired
by reconciliation when Slack makes it directly available.

Use a stable Recall principal ID as the Composio user ID. The switchboard opens
one Google toolkit per hosted authorization trip and binds the returned exact
`ca_...` account to that principal, connector, and brain route. Recall re-reads
the account, verifies active status/owner/toolkit/scopes, and executes a minimal
read capability probe before enabling ingestion. Only the opaque user, toolkit,
and connected-account references are encrypted in Recall; provider tokens stay
in Composio. Set the optional `RECALL_COMPOSIO_AUTH_CONFIG_*` variables from
`service.env.example` for production-owned OAuth branding and scopes. Omit them
only for Composio-managed development auth.

The encryption key is independent of database, archive, Google, and MCP
credentials. Keep it stable for the lifetime of encrypted provider connections;
rotate it with an explicit decrypt/re-encrypt migration, never by silently
replacing the variable. Google must register the redirect URI exactly. Recall
requests offline access, incremental authorization, PKCE, one-time server-side
state, `openid`, and only the read-only scopes for the source toggles selected by
the owner. Workspace administrators may still need to trust the OAuth client,
and public distributions must complete Google's applicable sensitive-scope
verification.

Mint an audience-specific bootstrap key into a new owner-private file:

```bash
python -m recall_server.cli admin-token-create owner-web \
  --principal principal:owner --expires-in-days 30 \
  --output /approved/private/recall-admin.json
```

Open `/admin`, paste the one-time key into the access dialog, then choose a brain
for each Google service before authorization. The browser exchanges the key for
a twelve-hour Secure, HttpOnly, SameSite session and a CSRF-bound companion
cookie. OAuth refresh and access tokens are encrypted with AES-256-GCM in the
database, never returned by the state API, and cryptographically wiped after
provider disconnection. Pause preserves the installation checkpoint; revoking
one installation disables only that routed source, while disconnecting Google
revokes provider authority for every dependent route. Uninstall removes the
route from the active map.

If the provider reports an expired or forbidden connected account, the managed
worker marks the provider `degraded`, returns each enabled dependent route to
`configured`, and stops retrying. `/admin` then reports that Google must be
authorized again. A successful authorization binds the replacement connection,
re-enables the selected route, and resumes its retained ACK checkpoint. Generic
transport failures remain bounded retries and never masquerade as a reconnect.

Native clients use the same versioned `/admin/api/v1` session, state, OAuth, and
lifecycle contract. They must store the bootstrap or browser-session authority
in the operating-system credential store and must not copy provider tokens out
of Recall.

Run one or more managed workers from the same immutable Recall Core image:

```bash
python -m recall_server.cli managed-worker \
  --state-root /var/lib/recall --interval-seconds 60
```

The worker claims only due, enabled `remote_worker` installations with a
database lease. It decrypts one provider capability in memory, materializes any
short-lived CLI authority beneath an owner-private worker directory, archives
raw records before projection, commits through the tenant-scoped canonical
plane, advances the connector cursor only after acknowledgement, and updates
the same installation row shown in the web UI. Pause, revoke, tenant selection,
and provider disconnect therefore affect execution without a second config
surface. Mount `/var/lib/recall` on persistent encrypted storage so ACK-gated
spools survive image restarts. Every worker replica must receive the same
database, R2, embedding, and control-encryption settings as the API service; it
does not listen on a network port.

The same process can own the existing full-document and lossless-passage queues
without adding another service. Set `RECALL_MANAGED_PROJECTIONS_ENABLED=1` and
give the worker the evidence-archive settings already used by deep inspection.
Each cycle drains at most 20 logical documents and 20 passage documents across
four streams, then embeds at most 100 passages. Keep the worker database pool at
eight or higher so those fixed bounds retain headroom. Leave the flag unset when
an operator runs dedicated logical-evidence and passage workers instead; never
run both owners for steady-state scheduling.

Treat a one-time high-concurrency backfill as maintenance, not free background
capacity. Do not run it beside managed projections unless the database session
budget leaves both services headroom and health/readiness stay green for a
complete canary batch. Stop on the first readiness failure; the durable queues
make resumption safe. On a 50-session database with an eight-connection API pool
and eight-connection managed-worker pool, prefer the managed worker itself over
a sustained external drain.

Run canonical embeddings as a separate worker from that same immutable image:

```bash
python -m recall_server.cli canonical-embedding-worker \
  --tenant tenant:company:example \
  --batch-size 2000 --max-batches-per-cycle 10 --interval-seconds 5
```

Canonical ingest commits do not call an embedding provider. The worker drains
unembedded current chunks in bounded, idempotent cycles, so provider latency or
an outage cannot delay or fail source ingestion. Restarting the worker is safe:
the durable canonical chunk table is the queue, the unique embedding key is the
acknowledgement, and a runtime-scoped keyset watermark avoids rescanning the
already-embedded prefix. The watermark wraps at the end so late-arriving chunks
that sort before it are still repaired. Give this worker the same database and
embedding settings as the API service, but no collector or MCP credentials.

Managed providers can parallelize document batches with
`RECALL_EMBEDDING_WORKERS=2` through `8`; the default is `1`, and the local TEI
sidecar remains deliberately single-request. Results are reassembled in input
order before their idempotent database write. Keep query embedding on the
ordinary bounded path and raise worker concurrency only within the provider's
document rate limits.

### Projection admission and early publication

The projection worker admits source-fair batches, with forget work first, while
previous parents are still preparing. The executor holds at most one additional
batch beyond its active owners. `--upload-concurrency` bounds active parent work;
`--logical-batch-size` times `--max-batches-per-cycle` bounds total admissions.
One coordinator publishes completed parents through passages and search while
other parents continue. It also keeps the existing cleanup budget per completed
admission batch. Failed or raced parents wait for the next cycle.

With one batch per cycle, a giant still bounds that cycle's total work. Increasing
the existing batch budget lets small parents continue through later batches;
measure backlog change and acknowledged-record search/open freshness before
claiming recovery. `logical_source_races` reports discarded stale preparations
separately from `logical_failed`. These settings do not resolve a parent that
continually changes during its own preparation, or capacity exhausted by giant
parents in every executor slot.

Logical admission alternates oldest-first and recently changed-first rounds,
retaining forget priority, debounce/max-wait gates and source round-robin in both.
The next mode belongs to the projector, survives cycle boundaries and changes
only after nonempty admission; restart begins oldest-first. Even one-slot rounds
therefore share capacity between history and recent changes. Recent means the
queue's canonical change time, not event time: historical replay competes for
those slots too. This does not guarantee freshness when arrivals exceed capacity.

Passage projection also commits each ready document independently. Only active
owners retain prepared passage bodies, and commits retain the existing cap of
8 or one fewer than the database pool size. A progress callback invokes only the
same coordinator's search writer, so a slow passage preparation cannot hold a
ready sibling's search publication behind the batch. Missing/unavailable archives
still yield to logical recovery after the current batch.

While waiting on busy logical or passage owners, the same coordinator wakes
every five seconds to publish already committed work, including external repair
outbox entries. Active publication can take longer than that interval; it is a
bound on idle waiting, not a search-latency guarantee or another writer.
Logical idle callbacks drain search only: retrying missing passages there would
invalidate their already running archive rebuilds. Completed logical parents
still trigger passage projection followed by search.

`passage_elapsed_ms` remains coordinator wall time excluding search callbacks;
`search_plane_elapsed_ms` measures those callbacks separately. Passage
`prepare_ms` and `commit_ms` (also logged with the `passage_` prefix) are summed
owner durations and can overlap. Commit time includes cap/connection waits;
they are not additive partitions of total cycle wall time.

### Dedicated passage embedding worker with a daily cap (H5-2/H5-3)

Measured in production (voyage-4, `RECALL_EMBEDDING_WORKERS=4`), the embedding
phase of `projection-worker` takes 40 to 125 s of every cycle for 64 passages,
the largest steady-state phase, and it delays logical, passage, and parquet
freshness behind the provider. Run embedding as its own service with its own
database pool, and cap the total volume per day so an accidental full
re-embed (September 2026: about $800) stops at a known budget.

Two services from the same immutable image, same database, same embedding
settings:

```bash
# Service 1: projection worker, embedding phase handed off.
python -m recall_server.cli projection-worker \
  --tenant tenant:company:example \
  --skip-embedding \
  --logical-batch-size 25 --passage-batch-size 100 \
  --max-batches-per-cycle 10 --interval-seconds 5

# Service 2: embedding worker with a rolling daily cap.
RECALL_EMBEDDING_DAILY_CAP=200000 \
python -m recall_server.cli embedding-worker \
  --tenant tenant:company:example \
  --batch-size 128 --max-batches-per-cycle 10 --interval-seconds 5
```

- `--skip-embedding` is off by default; without it the projection worker
  behaves exactly as before. With it the cycle log still reports
  `embedded=0 embed_elapsed_ms=0`, the idle check ignores embedding, and the
  advisory embedding lock is never taken by that process.
- `embedding-worker` needs the database and embedding settings only (no
  evidence-archive or collector credentials). Give it the same
  `--target-tokens` / `--overlap-tokens` as the projection worker (defaults
  match) because only passages of that policy fingerprint are embedded.
- `--daily-cap` (or `RECALL_EMBEDDING_DAILY_CAP`, default `200000`) is the
  number of passages sent to the provider per rolling day, read every cycle
  from `canonical_embedding_ledger (tenant_id, day date, embedded int)`
  (schema 065). The window is the UTC day buckets that intersect the last
  24 hours, so it never under-counts. The ledger is upserted after every
  cycle, so the cap survives restarts and covers every replica. When the
  budget is exhausted the worker logs `embedding cap reached ...`, stops
  calling the provider, and polls the ledger every `--interval-seconds`
  until the window rolls. Raise the cap and restart the service to resume
  sooner.
- Each cycle logs `embedding cycle status=... embedded=N pending=0|1 lag=L
  embedded_24h=T cap=C cap_remaining=R elapsed_ms=...`. `lag` is a bounded
  count (up to 10000) of passages still without a vector; it is `0` after a
  complete drain and `-1` when no embedding runtime is configured.
- `/metrics` on the web service exports `recall_embedding_daily_total`
  (ledger window, all tenants) and `recall_embedding_daily_cap` next to
  `recall_passages_unembedded`; the systems card probe
  `freshness.embedding_lag` gates `passages_unembedded <= 5000` and
  `cap_remaining > 0`.

Rollback: remove `--skip-embedding` from the projection worker and stop the
embedding worker. Both processes share the same advisory lock per tenant, so
running them together during the switch is safe; the ledger table stays and
is harmless without a reader.

`RECALL_DATABASE_URL` must be a PlanetScale application role URL with
`sslmode=verify-full` and an explicit trust root. Prefer
`sslrootcert=/etc/ssl/certs/ca-certificates.crt` in the pinned Linux container;
`sslrootcert=system` is also accepted where the runtime's libpq/OpenSSL build
resolves the OS trust store correctly. Bootstrap and migrate the database with
a separate administrative credential, then retain only this least-privilege
runtime URL in the injected environment. The provider token needs only database
read/create permissions; the Tailscale OAuth client must be restricted to the
dedicated gateway tag.

When migration and runtime use separate PostgreSQL roles, refresh runtime grants
after every migration and before deleting a short-lived migration role. Replace
`recall_runtime` with the actual runtime role identifier:

```sql
GRANT USAGE ON SCHEMA public TO recall_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE
  ON ALL TABLES IN SCHEMA public TO recall_runtime;
GRANT USAGE, SELECT
  ON ALL SEQUENCES IN SCHEMA public TO recall_runtime;
REVOKE ALL PRIVILEGES
  ON TABLE public.schema_migrations FROM recall_runtime;
GRANT SELECT
  ON TABLE public.schema_migrations TO recall_runtime;
```

The final two statements are mandatory after the broad table refresh: the runtime
capability gate requires migration history to remain read-only. Apply only the grants
the enabled runtime operations need. Reassign objects to the durable owner before
deleting a temporary migration role.

After reviewing the zero-network preview and mode-0600 approval document, run
the exact approved apply under 1Password injection:

```bash
OP_CACHE=false op run --environment "$APPROVED_ENV_ID" -- \
  python -m recall_server.cli deployment-apply \
  --manifest /private/recall-core.plan.json \
  --approvals /private/approvals.json \
  --planetscale-organization ORGANIZATION \
  --database-name DATABASE \
  --render-owner-id WORKSPACE_ID \
  --core-name RECALL_CORE \
  --gateway-name RECALL_GATEWAY \
  --tailnet-hostname RECALL \
  --tailnet-tag tag:recall
```

The command checks infrastructure approvals before reading any credential. Its
stdout is content-free: actions, plan hash, and non-reversible resource
receipts only. Writer cutover remains a separate approval.

## Existing host pilot

The existing host pilot listens on a Unix socket, not TCP. Tailscale Serve is its only network proxy.
On Linux, the server verifies `SO_PEERCRED` and trusts Tailscale identity headers only when the
Unix-socket peer UID is explicitly allowlisted. Ubuntu's sandboxed `tailscaled` uses UID 65534;
root is UID 0. Neither identity is assumable by the interactive user, so a same-user process that
connects directly and forges `Tailscale-User-Login` is rejected. Narrow
`RECALL_TRUSTED_PROXY_UIDS` to the UIDs observed for the local `tailscaled` service.

Install from an immutable, reviewed checkout at `~/services/recall-brain`. The service unit never
points at a contributor's active checkout.

```bash
git worktree add --detach ~/services/recall-brain <reviewed-merged-sha>
python3 -m venv ~/.config/recall-brain/venv
~/.config/recall-brain/venv/bin/pip install -r ~/services/recall-brain/recall/server/requirements.txt
docker pull ghcr.io/huggingface/text-embeddings-inference@sha256:ad950d30878eceb72aaf32024d26fa2b1d04a75304fa0b4776b49aa1941fea07
install -m 0600 ~/services/recall-brain/recall/server/deploy/service.env.example ~/.config/recall-brain/service.env
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-brain.service ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-brain-backup.service ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-brain-backup.timer ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-embedding.service ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-embedding-backfill-sidecar.service ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-embedding-backfill.service ~/.config/systemd/user/
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-embedding-backfill.timer ~/.config/systemd/user/
# Fill in service.env, then apply every schema before starting services or timers.
set -a; source ~/.config/recall-brain/service.env; set +a
cd ~/services/recall-brain/recall/server
~/.config/recall-brain/venv/bin/python -m recall_server.cli migrate
systemctl --user daemon-reload
systemctl --user enable --now recall-embedding
systemctl --user enable --now recall-brain
systemctl --user enable --now recall-brain-backup.timer
systemctl --user enable --now recall-embedding-backfill.timer
tailscale serve --bg --https=9443 unix:/run/user/$(id -u)/recall-brain.sock
```

Do not use Funnel. Preserve unrelated Serve listeners by configuring only the dedicated 9443
listener. Collectors receive revocable tokens from `recall_server.cli token-create`; plaintext
is emitted once and only its SHA-256 is stored. Use `--output /secure/mode-0600-file.json` so the
plaintext never enters terminal or session logs; the command refuses to overwrite an existing file.
A read token may use `--principal OWNER` to read exactly the sources granted to that principal.
Any token with `write` scope must also use `--source SOURCE_ID`; write authority is never
principal-wide. Add `--capture-origin ORIGIN` to a principal-aware read/write token to expose
`recall_capture` and `recall_forget` over MCP. The host binds that origin and source; tool
arguments cannot override either. Hosted capture structurally scrubs title and body before the
canonical event is stored.

The pilot timer starts its first logical backup after 15 minutes and schedules another six hours
after the previous run finishes. This deliberately prevents overlapping full dumps. The interval
is a conservative example, not an RPO guarantee: measure a complete backup on the real corpus and
set the schedule from its duration, available disk, and provider-native recovery guarantees.
Before multi-user scale or C10 production cutover, use provider point-in-time recovery or
continuous WAL archival plus daily base backups; the same blank-database restore/fingerprint
contract remains the gate.

Searches have a 300ms database-work budget by default. Override it only within the validated
10–30000ms range with `RECALL_SEARCH_DEADLINE_MS`; the response and service log expose only
content-free per-leg timings, result counts, and the deadline outcome.

Passage search fuses its dense, passage-lexical, and sparse-exact arms with convex min-max
fusion by default: each arm's best score per document is min-max normalised inside the arm and
the arms are combined as `Σ alpha × normalised` (`RECALL_SEARCH_FUSION_ALPHAS`, default
`dense:0.15,lexical:0.30,sparse:0.55`, must sum to 1). Arms with fewer than three documents, or
a recent-first fallback whose scores are all `0.0`, contribute rank scores instead. Set
`RECALL_SEARCH_FUSION=rrf` to restore reciprocal-rank fusion. Both values are validated at
startup; a malformed value stops the service. `diagnostics.fusion` reports the mode, alphas, and
per-arm candidate counts on every search, and each result carries content-free `arm_scores`
(raw best score, arm rank, normalised value per arm) so the offline tuner in
`recall/evals/fusion_tuning.py` can replay fusion without re-querying.

A date phrase in the question is a soft temporal hint (H2-h), consulted only when the caller
supplied neither `since` nor `until`. `recall_server/temporal_hints.py` recognises explicit
dates and day ranges (`2026-05-03`, `May 2-4`, `5/3`), month names with an optional year and
part (`in May`, `early May 2026`), relative phrases (`yesterday`, `last week`, `two weeks
ago`), quarters (`Q2`, `second quarter of 2025`) and years with a preposition. Day-level
hints are `exact`; everything wider, or hedged with `around`/`about`/`roughly`, is `loose`
(a hedge also pads the window by three days). Documents whose
`[first_occurred_at, last_occurred_at]` intersects the window have their fused score
multiplied by `1 + RECALL_TEMPORAL_BOOST` (`exact`, default 0.5) or
`1 + RECALL_TEMPORAL_BOOST_LOOSE` (default 0.25) before the reranker and before the collapse
truncation, floored first at the pool's smallest positive fused score so a document at an
arm's min-max floor still moves. A day-level hint (hedged or not) additionally runs the dense
arm once more inside the window (`RECALL_TEMPORAL_WINDOW_BUDGET_MS`, default 150, sequential
on the dense worker so no fourth pooled connection) and unions the pools, so a document at
the bottom of the global dense pool is still guaranteed into the collapse. `RECALL_TEMPORAL_HINTS=off`
disables all of it; queries without a hint are unchanged. Diagnostics:
`temporal_hint={since,until,confidence,boost}`, `temporal_boosted`, `dense_window_status`,
`dense_window_strategy`, `dense_window_candidates`, `dense_window_added`,
`arm_elapsed_ms.dense_window`; boosted results carry `temporal_boost`.

Semantic retrieval requires PostgreSQL with pgvector and one explicitly selected embedding
profile. Cosine score distributions vary by model, so
`RECALL_SEMANTIC_MINIMUM_SIMILARITY` is an explicit validated deployment setting in the
0–1 range and defaults to `0.35`. Calibrate it with a private retrieval eval when changing
models; the bounded top-K candidate pool prevents a lower floor from creating an unbounded
scan. Recall supports three protocols:

- `voyage` is the recommended hosted profile. `voyage-4` supports 512-dimensional output and
  distinct `document`/`query` retrieval modes.
- `openai` calls the standard `/v1/embeddings` contract and works with OpenAI-compatible hosted or
  local services. Optional document/query prefixes support asymmetric open models such as Nomic.
- `tei` preserves the pinned local Qwen profile below for operators who prioritize private
  inference and accept its compute footprint.

Leaving `RECALL_EMBEDDING_URL` unset is a supported zero-dependency lexical-only profile. This is
degraded retrieval, not failed startup. Hosted profiles send projected memory text to the selected
provider; operators must make that privacy choice deliberately.

Every non-loopback endpoint must use HTTPS, exactly match
`RECALL_EMBEDDING_APPROVED_URL`, and read its bearer from exactly one protected source:
an owner-only, non-symlink `RECALL_EMBEDDING_KEY_FILE`, or the deployment secret variable named
by `RECALL_EMBEDDING_KEY_ENV`. The latter is the normal container pattern; the variable's value
must be injected by the secret manager and never written in the config file. Recall rejects
redirects, rereads the selected key source, validates response
indices, dimensions, and finite values, and fingerprints the protocol, model version, dimensions,
and query/document transformation. A profile change therefore makes old vectors stale until the
online backfill converges.

### Optional reranker (`RECALL_RERANK_*`)

Recall can rerank the top fused passage candidates with a hosted cross-encoder before
collapsing them into documents. It is off by default and fail-open: any failure (network,
timeout, HTTP status, malformed body, out-of-range index, unreadable key) raises
`RerankUnavailable` and search keeps the fused ranking it already has. The runtime never
retries on the query path and never logs query text, passage text, or the bearer.

| Variable | Default | Meaning |
| --- | --- | --- |
| `RECALL_RERANK_PROTOCOL` | `off` | `voyage`, `cohere`, or `off`. |
| `RECALL_RERANK_MODEL` | `rerank-2.5` (voyage) / `rerank-v3.5` (cohere) | Provider model label. |
| `RECALL_RERANK_URL` | provider endpoint | `https://api.voyageai.com/v1/rerank` or `https://api.cohere.com/v2/rerank`. |
| `RECALL_RERANK_APPROVED_URL` | unset | Required when `RECALL_RERANK_URL` is not the provider default; must match exactly. |
| `RECALL_RERANK_KEY_FILE` | unset | Owner-only (`0600`), non-symlink bearer file. Mutually exclusive with the variable below. |
| `RECALL_RERANK_KEY_ENV` | unset | Name of the secret-manager variable holding the bearer. |
| `RECALL_RERANK_TIMEOUT_SECONDS` | `2.5` | Per-request timeout (0.1–30). Search may pass a shorter remaining budget. |
| `RECALL_RERANK_MAX_CANDIDATES` | `50` | Passages sent per query; extra candidates are dropped, not reranked. |
| `RECALL_RERANK_MAX_DOC_CHARS` | `2000` | Each passage is truncated to this many characters before sending. |
| `RECALL_RERANK_MIN_BUDGET_SECONDS` | `1.0` | Search reranks only when at least this much of the search deadline remains after the arms (0.05–30); otherwise it keeps the fused order with `rerank_status=skipped-budget`. |

Every non-loopback endpoint must use HTTPS. The provider's canonical endpoint is approved by
construction; any other host needs `RECALL_RERANK_APPROVED_URL` set to the exact same value, the
same pattern as `RECALL_EMBEDDING_APPROVED_URL`. Exactly one key source is required. Redirects are
refused, response bodies above 2 MiB are refused, and every returned index must be unique and
inside the submitted batch. The `fingerprint` (protocol, model, truncation width) is reported in
search diagnostics so eval runs can attribute a ranking change to a reranker change. Enabling a
hosted reranker sends redacted passage text to that provider; make the privacy choice deliberately.

Where it runs: `passage_retrieval.search()` fuses the arms, collapses the pool to documents (widened
to at least `RECALL_RERANK_MAX_CANDIDATES` documents when a reranker is configured), selects up to
that many passages round-robin over the fused document order (each document's strongest range
first), sends the query plus each passage's redacted text once, and re-orders documents by their
best reranked passage. The time clip runs after this. Diagnostics carry `rerank_status`
(`ok` | `skipped-budget` | `skipped-disabled` | `unavailable`), `rerank_elapsed_ms` (also under
`arm_elapsed_ms.rerank` when the provider was called, so `latency.search_stages` reports
`arm.rerank.p95_ms`), `rerank_candidates`, `rerank_model` (the runtime fingerprint) and, on
`unavailable`, the content-free `rerank_error` code. Each reranked result row and matching range
carries `rerank_score`; the fused `rank` is left as-is. With `RECALL_RERANK_PROTOCOL=off` the only
trace is `rerank_status=skipped-disabled`; results are byte-identical to a build without the stage.

The optional packaged self-hosted unit pins TEI 1.9 and Qwen3-Embedding-0.6B, binds only
`127.0.0.1:8089`, and never exposes an embedding route through Tailscale Serve. Keep
`RECALL_EMBEDDING_BATCH_SIZE=1`: the derivation
fingerprint and backfill deliberately trade background throughput for reproducible Qwen document
vectors. The sidecar admits only one single-input request at a time because this Qwen/TEI CPU path is
not reproducible across batched requests. An overlapping request is rejected and retrieval safely
falls back to exact and lexical legs; a backfill retry converges later. Oversized
documents use a fingerprinted 4,096-character
head-and-tail projection so a giant tool result cannot stall a complete backfill batch. The runtime
verifies the exact model commit and float32 dtype against TEI `/info` before sending text. Search
ignores stale fingerprints, dimensions, projector versions, and content hashes.

Schema 021 adds a second, rebuildable semantic projection for conversational sources. It embeds a
user request together with every assistant message before the next user turn, preserving the
request at the head and the final response at the tail. Search still returns the final canonical
assistant item and its normal `recall://` receipt; the combined turn is only a retrieval vector.
Every contributing item is linked so a soft deletion excludes the vector immediately, and search
also verifies that the cited response is still the current final response for the turn. This
closes the common failure where a short answer is meaningless without its preceding question
without creating uncited synthetic memory.

Production backfill uses an identical second sidecar on `127.0.0.1:8090`. Keeping historical CPU
inference separate prevents convergence work from rejecting live query embeddings. Neither port is
served through Tailscale, and both runtimes enforce the same pinned single-input contract. The worker
sets its endpoint at `ExecStart`, after `EnvironmentFile` loading, so the live-query URL cannot override
the dedicated backfill route through systemd environment precedence. Its 120-second transport timeout
allows long CPU inference to finish under contention without relaxing the live-query timeout.

Query planning is separate from embeddings and optional. When enabled, it must use the staging
LiteLLM HTTPS router plus a
short-lived model-scoped virtual key in a non-symlink owner-only file. A separate secret-manager
timer must atomically replace that file before expiry. Never place a LiteLLM master key in
`service.env`, pass it to Recall, or call a model provider directly. Recall rereads the key on every
uncached plan and keeps only bounded hash-keyed in-memory caches; it does not persist query text or
planner output. Set `RECALL_LITELLM_APPROVED_URL` to the exact same approved staging-router base URL;
startup fails if the planner points anywhere else.

After schema 011, converge the derived embedding projection online. The timer holds a dedicated
advisory lock, processes bounded batches, and is safe to replay. It never rewrites canonical events,
items, or receipts:

```bash
RECALL_DATABASE_URL=... RECALL_EMBEDDING_URL=http://127.0.0.1:8089 \
  python -m recall_server.cli backfill-embeddings --batch-size 128
RECALL_DATABASE_URL=... RECALL_EMBEDDING_URL=http://127.0.0.1:8089 \
  python -m recall_server.cli backfill-turn-embeddings --batch-size 128
```

On a large existing brain, converge high-value sources first without changing global correctness:

```bash
python -m recall_server.cli backfill-embeddings --source-id SOURCE_ID --batch-size 128
python -m recall_server.cli backfill-embeddings --source-id SOURCE_ID --surface user --batch-size 128
python -m recall_server.cli backfill-turn-embeddings --source-id SOURCE_ID --batch-size 128
```

The optional source and surface selectors change scheduling only. Metrics and search compatibility remain global, and an
unscoped replay finishes every remaining source. The packaged oneshot has no start timeout because a
bounded batch on CPU can legitimately exceed the systemd manager default; batch and timer bounds still
provide resumable checkpoints.

`recall_embedding_lag` must reach zero before semantic retrieval is considered ready. The service
continues exact and lexical retrieval if the local sidecar or scoped planner is unavailable; stale
vectors are never searched.

Federated ranking uses explicit host-owned source profiles. Ingest envelopes and model tools
cannot set family, quality, or freshness policy. After a source has ingested at least one event,
an operator may configure it through the database-local admin CLI:

```bash
RECALL_DATABASE_URL=... python -m recall_server.cli source-profile-set SOURCE_ID \
  --family coding_history --quality trusted --freshness-half-life-days 180
RECALL_DATABASE_URL=... python -m recall_server.cli federation-scoreboard
```

Families and quality levels are closed enums. Search results add a content-free source profile
receipt plus bounded lexical, freshness, quality, and cross-family corroboration components.
The scoreboard reports aggregates only: it never returns source IDs, query text, or item text.
Unprofiled sources remain explicitly `unclassified`/`unrated`; the server never guesses a profile
from source-name patterns.

After applying schema 005 to an existing brain, backfill the rebuildable entity projection
online. The command commits bounded batches, holds a dedicated advisory lock, resumes from its
watermark after interruption, and does not rewrite canonical events or items:

```bash
RECALL_DATABASE_URL=... python -m recall_server.cli backfill-entities --batch-size 5000
```

Do not substitute the full `rebuild` command for this live migration; rebuild intentionally
truncates all derived projections inside one transaction and is reserved for offline recovery.

After upgrading to projector version 3, converge existing derived text on the current privacy
contract with the resumable redaction backfill. It snapshots the current item high-water mark,
rewrites only derived items/chunks/entities whose redacted form changes, and never mutates canonical
source events or receipts:

```bash
RECALL_DATABASE_URL=... python -m recall_server.cli backfill-redaction --batch-size 5000
```

The default is single-process. On a dedicated multi-core maintenance host, `--workers N` (maximum
32) parallelizes only the pure redaction computation; database reads, writes, watermarks, and the
advisory lock remain single-owner and ordered.

After schema 009, repair legacy Cowork messages that were projected as one session per message.
This migration only moves derived item/session relationships; canonical events, revisions, content
digests, and item receipts remain unchanged. It is high-water bounded, resumable, and idempotent:

```bash
RECALL_DATABASE_URL=... python -m recall_server.cli backfill-cowork-sessions --batch-size 5000
```

Configure owner-controlled aliases only after their exact source exists. Search routing by source
ID, source family, or alias always intersects with a source-scoped credential:

```bash
RECALL_DATABASE_URL=... python -m recall_server.cli source-alias-set cowork cowork:mac:owner
```

Linux history collectors use `recall-collector@.service` with separate `claude` and `codex`
environment/token files. Issue one source-scoped credential per unit, install the two example
environment files with mode 0600 after replacing every value.

The Codex example configures both the active and archived rollout roots; both feed one
stable session ledger and one source-scoped credential. Create both directories before starting
the unit; an unavailable configured archive root fails closed. Claude remains single-root.

Then enable the instances:

```bash
install -m 0644 ~/services/recall-brain/recall/server/deploy/recall-collector@.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now recall-collector@claude recall-collector@codex
```

### Verify team session collection and MCP access

Track each person's device, selected harnesses, latest Brain ACK, latest searchable
session, and MCP login. An accepted invitation enables access; installing a
collector enables uploads. Verify both before marking a person ready.

1. **Invite and connect.** The owner invites the exact verified email through
   `/admin`. The employee accepts and follows the Codex or Claude commands on
   `/join/<invitation-id>`, which use the company-specific `/mcp/brains/<tenant>`
   endpoint and the configured OAuth settings. Use each person's own identity.
2. **Enroll selected session sources.** In the web or Mac switchboard, the employee
   creates a device route for `local.claude-code` and/or `local.codex`. This uses
   `POST /admin/api/v1/device/installations` to bind the source to their principal
   and contributor actor and issue a source-scoped write-only credential. Keep
   existing source IDs and spools on upgrades. On Linux, fill in every example
   path and the enrolled tenant/principal/source tuple; the unit explicitly needs
   `RECALL_PRINCIPAL_ID` and `RECALL_INTERVAL_SECONDS`. On Mac, follow the
   [client installation instructions](../../client/README.md). Codex collection
   includes both active and archived rollout roots.
3. **Verify upload progress.** On Linux, check
   `systemctl --user status recall-collector@claude recall-collector@codex` and run
   `python -m collector.cli doctor` with that unit's environment and all arguments
   from its `watch` invocation, changing only the command. Replace `doctor` with
   `status` for central source parity; retain the principal and credential arguments.
   On Mac, use `recall-brain mac-status`. Require advancing ACKs when new sessions
   exist, complete or progressing scan coverage, draining pending records, and
   no unexplained dead records. `coverage_percent` counts discovered files in the
   ledger; neither 100% coverage nor a `ready` label alone proves uploads drained.
4. **Verify the serving pipeline.** The owner's authenticated
   `GET /admin/api/v1/state` includes invitations and the source fleet: owner,
   device, heartbeat, pending/dead counts, last transfer and 24-hour activity.
   Compare it with the intended roster; people with no source will not appear in
   fleet rows. Heartbeats older than two minutes are stale. The collector ingress
   service needs canonical v2/archive configuration and
   `RECALL_CANONICAL_INGEST_PUBLIC=1` for its archive, ingest, status and health
   routes. Check logical and passage queues, then `search_projection_outbox` and
   `python -m recall_server.cli search-plane-status --tenant <tenant>` for search
   publication. The managed
   worker runs remote connectors, not local session collectors; its optional
   projection cycle does not drain turbopuffer. Verify the existing dedicated
   `projection-worker` or `search-plane-project` owns that drain.
5. **Prove it from the employee's client.** Search for a recent known session from
   each selected harness using the employee's authenticated MCP, then open its
   returned receipt. Record the session timestamp and successful open alongside
   the upload ACK. Namespace counts and green service health do not replace this
   check. For live sessions, observe a later upload becoming searchable as well.

Back up and run a blank-database restore proof:

```bash
RECALL_DATABASE_URL=... recall/server/scripts/backup_restore.sh backup /secure/backup/dir
RECALL_RESTORE_DATABASE_URL=... recall/server/scripts/backup_restore.sh restore-test /secure/backup/dir
```

The database fingerprint covers both legacy events and every v2 canonical truth, projection,
redirect, and forget-fence table. The laptop OSS profile backs up its exact raw archive separately
and proves it into an empty root:

```bash
python -m recall_server.archive_snapshot backup /private/archive /secure/archive-snapshot
python -m recall_server.archive_snapshot restore-test /secure/archive-snapshot /private/empty-restore
```

Both commands emit aggregate counts and fingerprints only. The restore refuses a symlink, a
non-owner-only tree, tampered bytes or metadata, and any nonempty destination.

### Exact body record positions (068)

Apply migration 068 before deploying the worker or enabling archive body reads;
application startup does not apply it. It adds two nullable integers on existing
canonical documents and does not clear any prose. Logical publication fills only
changed record positions in the same transaction as the current parent catalog.
Unchanged append prefixes are not rewritten. A document-lock conflict aborts that
publication attempt immediately and leaves the queue for retry.

Located reads fetch intersecting existing parts and verify current receipts, complete
event text and every canonical chunk hash. Parent growth beyond 64 MiB cannot strand
a located body. NULL positions retain the transitional reader; deployment alone does
not backfill existing parents. Context callers can retain only requested chunks after
full-event verification, and excess retained output fails explicitly.

This migration does not authorize body thinning. Historical receipt recovery, measured
locator coverage, and both ingest writer paths remain separate retirement gates.
