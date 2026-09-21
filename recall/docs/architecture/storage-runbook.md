# Storage runbook: legacy v1 plane retirement (H1-T6)

Decision (Miguel, 2026-09-11): the canonical v2 plane is the only authority.
The legacy v1 plane stops receiving writes immediately, its read routes answer
`410 Gone`, and its tables are dropped thirty days after the retirement ships.
This runbook covers the flags, the write-path change, the read-path change,
and the drop.

## What changed

Every v1 write entry point now lands on the canonical plane:

| Entry point | Before | After (default) |
| --- | --- | --- |
| `POST /webhooks/v1/events` | `BrainStore.ingest` → `source_events`, `items`, `chunks`, ... | raw body → `raw_artifacts` via the fenced archive gateway, envelope → `CanonicalPlane.ingest_document` |
| `POST /webhooks/v1/slack` | same, one legacy batch per installation route | same canonical path, one artifact per installation route (tenant from `connector_installations.tenant_id`) |
| `POST /v1/ingest/batches` | same, keyed by `Idempotency-Key` in `ingest_batches` | one artifact per envelope (its canonical JSON), then `ingest_document`; dedupe is content-addressed |
| MCP `recall_capture` / `recall_forget` (`BrainStore.capture`, `forget_capture`) | `BrainStore.ingest` | same bridge, connector `mcp.capture`; `forget_capture` looks the captured revision up in `canonical_events` first |

The bridge lives in `server/recall_server/legacy_plane.py`
(`LegacyIngestBridge`). `BrainStore.ingest` itself stays callable as a library
function: the rollback path and the legacy e2e fixtures use it directly, but no
HTTP or MCP entry point reaches it while `RECALL_LEGACY_WRITES=0`. Acknowledgements keep the v1 shape
(`status`, `inserted`, `duplicate_events`, `receipts`, `replay`); a replay of
the same event returns `duplicate_events=1`, `replay=true`, and the same
receipt. Webhook responses now carry `duplicate_events` too. `batch_id` is no
longer returned in the default mode because nothing is written to
`ingest_batches`.

Receipts minted by the canonical writer look like
`recall://<source>/<native>?rev=<n>#item=<ordinal>`.
`GET /v1/receipts/resolve` resolves them from `canonical_events` and
`canonical_chunks` with the authenticated tenant and source grants. HTTP misses
do not fall through to tenantless `source_events`; trusted unscoped library callers
retain that legacy fallback until retirement. Explicit unauthenticated local
development rollback (`RECALL_LEGACY_READS=1`) also retains v1 resolution; the
flag never broadens MCP tenant or source grants. The v1 response shape remains.

The four legacy read routes answer `410 Gone` before authentication:

```json
{"error": "gone", "code": "legacy_plane_retired", "replacement": "recall_search"}
```

| Route | Replacement |
| --- | --- |
| `POST /v1/search` | `recall_search` (MCP) |
| `POST /v1/show` | `recall_show` (MCP) |
| `POST /v1/related` | `recall_related` (MCP) |
| `POST /v1/session-export` | `recall_session_context` (MCP) |

`GET /v1/receipts/resolve` and `GET /v1/doctor` stay.

## Flags

| Variable | Default | Meaning |
| --- | --- | --- |
| `RECALL_LEGACY_WRITES` | `0` | `1` restores the old dual-write path for rollback: the canonical write still happens when a canonical plane is configured, then `BrainStore.ingest` writes the v1 tables and its acknowledgement is what callers see. |
| `RECALL_LEGACY_READS` | `0` | `1` restores `/v1/search`, `/v1/show`, `/v1/related`, `/v1/session-export` exactly as before. |
| `RECALL_LEGACY_INGEST_TENANT_ID` | `tenant:personal` | Tenant that owns canonical rows written for v1 callers whose credential is not tenant-bound (existing webhook tokens, development mode). A credential created with `token-create --tenant ...` wins over this value; webhook-scoped credentials may now carry a tenant. |

Accepted spellings are `0/1`, `true/false`, `yes/no`, `on/off`. Anything
else fails startup (`validate_http_profile`) so a typo cannot silently flip the
plane.

The canonical write path needs `RECALL_CANONICAL_V2_ENABLED=1` and a
configured archive (`RECALL_ARCHIVE_*`). A deployment that receives webhooks
or v1 batches without a canonical plane returns
`503 {"error": "canonical plane unavailable"}` and writes nothing. Either
enable the canonical plane there or set `RECALL_LEGACY_WRITES=1` until it is.

### Rollback

1. Set `RECALL_LEGACY_WRITES=1` and `RECALL_LEGACY_READS=1`, restart.
2. Rows written while the flags were `0` exist only on the canonical plane;
   the legacy projection does not backfill from canonical. If the rollback
   window matters, replay the affected webhooks or batches: canonical dedupe
   makes the replay a no-op on the canonical side while the v1 tables fill in.
3. Reverse the flags to `0` when done. Do not drop tables while either flag is `1`.

## Thirty-day drop plan

Target: 2026-10-11 or later, thirty days after this change ships with both
flags at `0` in production. The drop is a separate change; nothing in H1-T6
removes a table.

Tables to drop, in dependency order (children first):

1. `turn_embedding_items`
2. `turn_embeddings`
3. `turn_embedding_projection_watermarks`
4. `item_embeddings`
5. `embedding_projection_watermarks`
6. `entities`
7. `chunks`
8. `items`
9. `sessions`
10. `projection_backfills`
11. `projection_watermarks`
12. `source_events`
13. `ingest_batches`
14. `source_grants`
15. `sources`

The same list is `LEGACY_DROPPABLE_TABLES` in
`server/recall_server/legacy_plane.py`; the e2e prints it as evidence.

Pre-drop checklist:

- Both flags have been `0` in production for the full thirty days
  (deploy history, not memory).
- `recall_ingest_commits_total` kept increasing and `source_events` row count
  did not change during that window (`SELECT count(*) FROM source_events`).
- `storage-discard-covered-legacy` (see `recall_server.cli`) reports every
  legacy identity as canonical-covered or archived to the legacy gap archive.
- No collector or MCP client still calls the four retired routes: the
  `recall_http_errors_total` counter split by status shows 410s trending to
  zero.
- `GET /v1/receipts/resolve` fallback to `source_events` is removed in the
  same change that drops the tables.
- Take a database snapshot first.

Code that still references the legacy tables and goes away with the drop:
`BrainStore.ingest`, `_project_one`/`_project_batch`, `search`, `show`,
`related`, `session_export`, the embedding and turn-embedding projection
workers in `db.py`, the legacy fallback in `BrainStore.resolve`, and the
`RECALL_LEGACY_*` flags themselves.

## Evidence

`server/tests/e2e_webhook_ingest.py` on a fresh PostgreSQL proves, in one run:
canonical rows and raw artifacts for the generic webhook, the Slack webhook,
and `/v1/ingest/batches`; zero rows in `source_events`, `items`, `chunks`,
`ingest_batches`, `sources`; receipts resolving; replays returning
`duplicate_events=1`; tombstoned receipts answering 404; the four read routes
answering 410 by default and 200 with `RECALL_LEGACY_READS=1`; and
`RECALL_LEGACY_WRITES=1` dual-writing. Unit coverage is
`tests/central_brain/test_legacy_plane.py`.


## Chunk search index retirement (2026-09-21)

The turbopuffer deployment now resolves a parent to its exact logical document,
filters to that document before BM25 ranking, then verifies current passage pins
and live authorized receipts in Postgres. It no longer uses the chunk GIN index.
Deploy this reader on every serving instance before running the retirement command:

```sh
python -m recall_server.cli storage-retire-chunk-search-index
python -m recall_server.cli storage-retire-chunk-search-index --apply
```

The first command reports the existing index size without writing. Apply requires
`RECALL_SEARCH_PLANE=turbopuffer`, drops only
`public.canonical_chunks_search_idx` concurrently, waits at most two seconds for
locks, and has a 60-second statement timeout. A timeout is not completion: inspect
the index state before retrying. Bodies, receipts and the generated search vector
remain unchanged. The separate archive reader is opt-in via
`RECALL_CHUNK_BODY_READS=archive`; its default is `postgres`. It reads existing
logical parts, verifies exact text/chunk hashes and liveness, and retains verified
SQL fallback for unprojected, oversized, structural and historical-layout records.
Its source-reader rollout and live acceptance have since passed. Body retirement
still requires exact per-target archive proof, compatible writers and the separate
bounded retirement procedure; enabling a reader alone does not authorize a clear.

The index can be restored, if needed, without restoring source data:

```sql
CREATE INDEX CONCURRENTLY canonical_chunks_search_idx
ON public.canonical_chunks USING gin(search_vector)
WHERE deleted_at IS NULL;
```

Rebuilding needs time and free space. Record actual database/index bytes before
and after removal; lower the provider disk floor only after verifying physical
usage and live health. Neither source-reader parity nor removal of this index
completes the remaining ingest/body/catalog migration.

### Verified retirement and provider follow-through

On 2026-09-21, PR #622/main `7bf7f8c` was deployed to the MCP service. The
independent 43-case availability/accuracy card passed 14/14 gates before and
after removal of the chunk index. The operation reclaimed 19,381,166,080 bytes
(18.05 GiB), reducing the database to 183,432,861,363 bytes (170.84 GiB).

The application role correctly refused DDL. A 15-minute PlanetScale role
inheriting `postgres` ran the bounded operation through a Render one-off; its
credential traveled through one temporary encrypted environment entry, never a
command argument or log. The role and entry were deleted afterward. No worker
deploy or source-body deletion was part of that operation. Verify migration 067
is recorded before retirement so old idempotent migrations cannot recreate
the index.

The disk floor changed from 300 to 275 GiB with `confirm_shrink: true`, retaining
PS80, two replicas and the 300 GiB cap. A completed branch change is insufficient
proof of physical shrink: query `/infrastructure` and check each node's
`volume_capacity_bytes` and `disk_replacement`. At 00:46 UTC the floor was
275 GiB while all three actual disks remained 300 GiB with no replacement
scheduled. Billing reduction was not verified. Private operation/card artifacts:
`~/.recall/storage-retirement-20260920/`.

Before clearing any body, require the append/reprojection proof: project older
turns, clear only verified synthetic bodies, append/revise/forget, then reproject
and reopen every retained receipt. A correct reader alone cannot prove that a
future projection preserves archived evidence. PR #624 prevents publishing bad
source bytes; archive-backed reprojection is a separate prerequisite.

Receipt resolution retains the exact requested revision. Current documents use
the shared archive hydration when enabled; historical revisions remain in
PostgreSQL and are hash checked. Do not thin historical bodies using current
logical evidence. The public MCP/edge profiles continue to hide this HTTP route.

Current public reads use verified record locators into existing immutable logical
parts; NULL positions retain hash-checked PostgreSQL fallback without fetching an
entire parent. Located reads bound each part at 64 MiB, each canonical body at 8 MB,
and retained results at 64 MiB. A successful read alone is not permanent retirement
coverage: locator identity, revision transitions and reprojection must remain
safe. Asynchronous passage or Parquet pointers alone do not cover logical-publication
lag. See [streaming locator publication](2026-09-21-streaming-locator-publication.md)
and [bounded parent retirement](2026-09-21-recall-parent-chunk-retirement.md).

### Current storage checkpoint — 2026-09-21, after documents compaction

MCP and projection run #648; managed ingestion runs #650. Archive body reads
and turbopuffer search are live. The managed cycle succeeded at 09:15:08 UTC;
its new record volume was not measured. Acknowledged logical retirement totals
are 80,343 documents, 87,914 chunks and 371,565,367 UTF-8 bytes. These lifetime
work counters are not current compressed storage or physical savings.

Managed pg_squeeze rehearsal proved concurrent-data preservation, exact external
cancellation and cleanup. The first documents rewrite was canceled after an
observer timeout; the second failed on lock acquisition. Neither reclaimed
documents storage. **Attempt 3 succeeded at 09:35:12 UTC**, reducing allocation
from **20,985,446,400 to 14,030,356,480 bytes**: **6,955,089,920 bytes (6.477 GiB)
physically reclaimed**. Catalog identity and five indexes were preserved, the
storage file changed, and worker, slot, role-default and credential cleanup passed.

The post-rewrite card passed 26/26 gates with all 16 original evaluator hashes
unchanged: recall@20 .9625, MRR .6579, server search p95 752.9 ms and show p95
511 ms. Each deployed service matched six preserved archived response hashes,
for 18 passing comparisons. Context p95 was 4,821.7 ms from three calls to one
variable receipt, versus 9,927.6 ms during the rewrite; this small, ungated sample
leaves the context tail unresolved.

One observed table-exclusive wait cleared under the temporary role's 1 s lock
acquisition timeout. That allowance bounds each wait separately. The provider's
100 ms final-replay setting excludes index acquisition and final swaps; it is not
a hard total lock-hold guarantee.

PS80 and two replicas remain; the configured disk floor is 275 GiB and cap 300 GiB,
while **all three actual volumes remain 300 GiB**. The fresh 275 GiB request
was accepted at 09:41:17 UTC and completed by 09:42:40 UTC, with no disk
replacement pending. Documents physical reclaim is proved; billed disk reduction
is not. Continue bounded source retirement with explicit remaining-coverage
accounting; a completed provider request does not satisfy the capacity exit. The living phase state is in
[the storage Cascade](../../../.cascade/recall-rewrite.md#storage-retirement-resumed--2026-09-20).
