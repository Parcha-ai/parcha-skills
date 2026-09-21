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

### Current company-brain checkpoint — 2026-09-21, after bulk archive correction

MCP runs #656 (`3161214`); projection remains #648 and managed ingestion #650.
Schema 069, archive body reads and turbopuffer search are live. MCP deployed at
13:33 UTC without restarting either worker. Installed archive/publisher checksums,
database readiness and six preserved archive response hashes passed. The managed
cycle succeeded at 09:15:08 UTC; its new record volume was not measured.

| Component | Production responsibility | Boundary still retained |
|---|---|---|
| turbopuffer | Dense, lexical and identifier retrieval over passages | PostgreSQL verifies current identity, scope and receipt authority |
| S3 logical evidence and Parquet | Exact redacted evidence and analytical projections | Unsupported/current-unlocated records and historical chunks retain PostgreSQL bodies |
| Archil and DuckDB | Mount authorized evidence and query analytical files in execution/analysis | They do not replace transactional grants, tombstones, replay or historical receipt lookup |
| PostgreSQL | Current authority, revision history and operational catalog | Duplicate bodies, metadata and indexes still prevent the small-catalog goal |

**Logical progress:** the 10:29:48 UTC source audit found **183 proof-complete
parents out of 2,645**, with 2,462 unrepresented and zero orphan progress rows.
This is one source, not full-corpus coverage. Acknowledged lifetime clears remain
**121,269 documents / 132,018 chunks / 535,436,711 UTF-8 bytes** at this checkpoint.
Restores and revisions do not subtract from those counters; neither remaining
stored-body bytes nor physical savings can be inferred from them.

The smaller archive outlier has made separately acknowledged locator progress:
11,008 NULL-only publications in an earlier attempt whose batch failure cause
remains unknown, then **51,200 more in 200 transactions** on #656. The latter
completed the 52-part / 220,194,071-byte archive proof, then stopped on a known
PostgreSQL lock conflict (`55P03`) at **13:42:49 UTC**. It enrolled no parent,
cleared no body and reported no uncertain commit. Its authenticated recovery
and source-reservation release passed; only a fresh plan and full proof may resume.
A fresh follow-up then committed **2,304 more locators in nine transactions**
before another known `55P03` at **13:52:49 UTC**, again with no body clear or
uncertain commit. It was recovered and released; the contended parent remains
an explicit residual while normal discovery advances. The larger archive outlier
also remains excluded. These are publication checkpoints, not permission to
clear from a saved proof.

Two small changes now support the existing retirement path. #654 replaces
per-document locator UPDATE/deadline calls with one guarded batch UPDATE. #656
lets maintenance explicitly select a five-second archive socket inactivity
allowance: the preceding attempt had failed on the serving 500 ms allowance.
Serving remains at **500 ms**, SDK retries remain off, and overall deadlines,
byte/hash proof and stream closure are unchanged. The #656 production archive
proof passed; the subsequent lock conflict is a separate cause.

**Physical progress:** removing the redundant chunk GIN reclaimed
**19,381,166,080 bytes (18.05 GiB)**. Documents rewrite attempt 3 completed at
09:35:12 UTC, reducing allocation **20,985,446,400 → 14,030,356,480 bytes**:
**6,955,089,920 bytes (6.477 GiB) reclaimed**. Catalog identity, five indexes and
exact receipts survived; worker, slot and temporary credential cleanup passed.
Its first two attempts reclaimed nothing. The unchanged post-rewrite card and
18 archived-response comparisons passed.

The chunk vector column was removed at 10:49:44 UTC after 18 pre / 18 post exact
archive checks. The column DROP reclaimed **zero physical bytes**. Three chunk
rewrite attempts stopped respectively on provider-observation freshness, a
16 GiB retained-WAL allowance, and the 64 GiB total-growth limit. Attempt 3 allowed
48 GiB WAL within that unchanged total limit, copied 14,969,299 rows, then canceled
at **64.804 GiB observed growth**. Cancellation started in 188 ms; the job finished
at 12:17:24 UTC and all worker/slot/role/environment/login cleanup passed.
Allocation **96,242,982,912 → 96,267,567,104 bytes**, original file/four indexes:
**zero chunk reclaim**. Neither the copied-row count nor the estimated removed
vector payload proves the remaining index, WAL, replay or swap workspace fits.

A finite row-rewrite pilot completed at **13:26:16 UTC** after the earlier 11-row
canary. It traversed 256 original heap pages and committed **1,726 rows in 27
transactions**, preserving every retained value and native prose TOAST pointer.
All 55 journal records were recovered with no pending or uncertain commit;
connection, journal lease and temporary-file cleanup passed. Summed pre-COMMIT
batch time was 5,020 ms, maximum 338 ms. Allocation **96,403,865,600 → 96,405,315,584
bytes** grew by 1,449,984 bytes; cluster-WAL growth was at most 27,828,144 bytes.
The unchanged post-pilot card passed 26/26. This proves finite traversal safety,
not physical reclaim or a reason to rewrite every row.

The scale investigation changes the next step: a small-prose local fixture grew
after both a no-op sweep and ordinary VACUUM. Surviving tail rows can prevent file
truncation, and internal holes cannot fund a separate destination relation.
pg_squeeze already omitted the dropped vector while making its failed copy, so
sweeping first does not inherently reduce its surviving prose/index payload.
**Continue archive-backed body retirement before another full sweep or copy.**
Measure remaining stored payload and any actual tail reclaim before admitting a
new total-workspace budget. Pausing ingestion alone is not proof: the copy's own
writes also generate WAL. No expansion or repeated whole-table attempt is implied.

**Quality and speed:** the original post-#656 card passed 26/26 at 13:36:32 UTC.
The post-publication card also passed 26/26 at **13:49:26 UTC**, with all 16 original
evaluator hashes unchanged: recall@20 **.9625**, MRR **.6811**, backend errors **0**.
Its server p95 was **3,012.7 ms**, show **645.9 ms**, context **5,338.5 ms**, scan
**11,887.5 ms**. Latency was higher than the prior run; passing gates does not
close the speed objective or establish a causal quality gain. Context uses only
three calls to a variable target. The separate fixed-receipt diagnostic's first
neighbor query took 4,526 ms; optional index operation #653 remains unbuilt.

**Provider capacity:** the PS80 primary remains writable, with two replicas. The floor is
275 GiB and cap 300 GiB, but **all three actual volumes remain 300 GiB**, with no
replacement pending and no billed disk reduction proved. At **13:44:21 UTC**,
node usage was approximately **169.6–169.8 GiB**. The earlier completed 275 GiB
configuration request did not shrink the disks. Usage changes, reusable pages
and accepted configuration are separate from the physical-capacity exit.

### Next boundaries and acceptance

1. **Finish finite source coverage.** Resume the known locator prefix only through
   fresh proof. Normal pages use the existing 50-parent discovery/publication and
   100-parent drain limits; the one-parent limit is for the isolated outlier.
   Resume normal discovery from its furthest 185-parent frontier, not the earlier
   outlier page. Preserve the pending projection and archive-budget residuals.
   Keep the publication/clear boundary, disabled markers and 60-second grace;
   reconcile current catalog keys again after cursor exhaustion.
2. **Measure and reclaim physical space.** Distinguish remaining current-located,
   unlocated and historical bodies before choosing more work. Preserve retained
   history. Require measured duration, allocation, WAL and cleanup before scaling
   a sweep or copy; ordinary VACUUM holes are not file shrink. Then verify each
   provider volume's actual capacity separately.
3. **Close the latency gap.** Prove the reviewed query/index boundary with exact
   receipt parity and representative cold/warm/load measurements. A passing
   overall card does not close an ungated tool tail.
4. **Shrink the catalog without losing history.** Preserve audit/job consumers,
   cold metadata, event/job lineage, old receipts, grants, replay and forget
   before retiring hot rows. Current-only logical archives do not justify
   deleting history. The catalog/index target below 10 GB remains unproved.

After every boundary: **simplicity** means fewer redundant responsibilities and
hot copies; **power** means exact authorized current and historical evidence;
**breakthrough** requires measured storage or latency improvement; **doing the
job fast** requires fresh ingestion and measured tool performance alongside the
unchanged correctness checks. The finite pilot and timeout correction are useful
prerequisites, not a completed downsize. Jev/W5 retains its separate acceptance
and does not substitute for this storage work.

See [the living storage Cascade](../../../.cascade/recall-rewrite.md#storage-retirement-resumed--2026-09-20).
Private operational artifacts retain source identities, original failed/passing
cards, exact jobs, authenticated progress and unchanged verifier pins.

### Optional chunk search-vector retirement

After every serving process uses turbopuffer, the generated
`canonical_chunks.search_vector` is redundant. Retiring its GIN index alone
preserves that stored column. The explicit operation previews by default:

```sh
RECALL_SEARCH_PLANE=turbopuffer python -m recall_server.cli \
  storage-retire-chunk-search-vector
```

Use the existing database configuration and turbopuffer credentials. Add
`--apply` only for the reviewed column retirement. The operation requires
recorded migration 067, the old chunk GIN already absent, the expected stored
`to_tsvector('simple', text_redacted)` expression, and no other column
dependencies. It takes the table lock with `NOWAIT`, rechecks the catalog, and
performs one transactional `DROP COLUMN ... RESTRICT`. Local lock and statement
limits prevent a queued maintenance operation from lingering. An error rolls
back the DDL; an ambiguous connection loss at commit requires checking the
column state before retrying.

This operation preserves every body, receipt, hash and authority/history row.
It adds no migration version, so existing readiness expectations remain valid;
067 already prevents ordinary migration from recreating the old vector.
PostgreSQL-mode installations retain their vector until explicitly retired.
The private legacy PostgreSQL evaluator refuses turbopuffer mode; public
retrieval continues through turbopuffer and the verified archive reader.

**A column drop is not physical disk reclamation.** The result reports zero
physical bytes reclaimed. Existing row/TOAST storage needs a separately admitted
rewrite, with fresh live-size, index-build, WAL, replica and free-space budgets.
A sampled vector payload estimate is not a rewrite workspace upper bound.
Do not combine this command with an automatic rewrite or provider resize.

Rollback, if separately required, rebuilds only derived search data:
`ALTER TABLE public.canonical_chunks ADD COLUMN search_vector tsvector
GENERATED ALWAYS AS (to_tsvector('simple', text_redacted)) STORED`.
That rebuild can rewrite the table and consume substantial time and space;
plan it explicitly. Recreating the retired GIN would be a second separately
budgeted operation. Neither action restores the retired PostgreSQL search plane.

Validation retains the existing archive reads, parent routing, chunk clear and
restore, historical revision, authorization and forget tests on fresh databases
with the column absent. It also checks PostgreSQL behavior before retirement,
least-privilege readiness afterward, migration replay, dependency refusal,
lock contention and rollback after an injected post-DDL failure.
