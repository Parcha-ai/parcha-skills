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

### Current company-brain checkpoint — 2026-09-21, events validated; disk replacements in progress

MCP now runs #658 (`78f6ba0`); projection remains #648 and managed ingestion
#650. Schema 069, archive body reads and turbopuffer search remain live. The
MCP-only deployment is live without restarting either worker. Runtime verification
succeeded at **14:53:40 UTC** in **5,055 ms**: installed publisher/archive checksums,
schema69/readiness, six exact preserved response pairs, eight archive reads and
two documents still empty in PostgreSQL. The unchanged post-#658 original card
also passed 26/26 at 15:02:32 UTC; detailed latency and limitations are below.
The earlier #656 runtime proof also passed. The managed cycle succeeded at 09:15:08 UTC;
its new record volume was not measured. Documentation checkpoints #657 and #659 are merged.
#660 (`88f1ca2`) is also merged but **not deployed**: it removes redundant SQL
pings while preserving deadline setup and proof behavior. Local PostgreSQL
equivalence and timeout tests passed; no production speed benefit has been measured.
PR #661 (`8b7e5c2`) merged at **16:39:02 UTC** and is **not deployed**. It gives both
collector-health writes one five-second SQL budget, preserving atomic rollback
and generic HTTP 503 on failure. Tests pass, but the budget is not a hard
network/COMMIT deadline and does not establish the cause of the Insights latency.
PR #662 merged as `c10e6d3` at **17:39:41 UTC**, after full CI and zero unresolved
review threads, and is **not deployed**. Its single guard skips document-
manifest discovery when scan aliases are empty; admitted-object mounts, dataset
links, tool hash checks and nonempty-alias behavior remain intact. Six generated-
stage tests and the full **1,835 Python / 8 Node** suite passed; independent
review scored **4.5/5**. This is a tested simplification, not a measured live speed gain.

| Component | Production responsibility | Boundary still retained |
|---|---|---|
| turbopuffer | Dense, lexical and identifier retrieval over passages | PostgreSQL verifies current identity, scope and receipt authority |
| S3 logical evidence and Parquet | Exact redacted evidence and analytical projections | Unsupported/current-unlocated records and historical chunks retain PostgreSQL bodies |
| Archil and DuckDB | Mount authorized evidence and query analytical files in execution/analysis | They do not replace transactional grants, tombstones, replay or historical receipt lookup |
| PostgreSQL | Current authority, revision history and operational catalog | Duplicate bodies, metadata and indexes still prevent the small-catalog goal |

**Logical progress:** the last whole-source audit, at 10:29:48 UTC, found
**183 proof-complete parents out of 2,645**, with 2,462 unrepresented and zero
orphan progress rows. This is one source, not full-corpus coverage. New discovery
has reached **235 parents**, and **233 are enabled**; neither count is an updated
proof-complete count. The three earlier pending/outlier residuals remain explicit.

Normal source13 published **30,861 locators in 159 transactions** and enrolled
50 parents. It then acknowledged **23,676 documents / 28,041 chunks /
166,360,038 UTF-8 bytes** cleared in 124 batches before one retirement deadline
stop. Forty parent proofs completed; one failed. Three exact archived read pairs
passed, no uncertain commit was reported, and authenticated recovery preceded
reservation release. Enrollment did not imply all 50 parents were cleared.

Gentler source15 completed **10 parent proofs / 17 batches** in **31.086 seconds**,
clearing **2,289 documents / 2,433 chunks / 9,496,691 UTF-8 bytes**. It reported
zero errors, partial parents or unknown commits, with three exact read pairs.
Its result is **`bounded`**, not `no_ready` or source completion; it does not
individually establish that source13's failed parent has been reconciled.
After the additional priority-source work below, acknowledged lifetime totals
are **151,863 documents / 167,244 chunks / 723,382,521 UTF-8 bytes**. Restores and revisions do not subtract from those
counters; neither remaining stored-body bytes nor physical savings follows from them.

The next priority source now has **40 enabled parents**, separate from the original
235-parent discovery frontier. Its bounded passes were verified and their
reservations released:

| Pass | Proven logical work | Timing and limits |
|---|---|---|
| Initial ten parents | 10 proofs; 447 documents / 476 chunks / 1,878,423 UTF-8 bytes cleared; seven structural exclusions | 73.027 s; three exact read pairs; bounded result |
| Next 20-parent page | 3,596 locators in 28 batches; 20 parents enrolled; one proof cleared 12 documents / 15 chunks / 134,049 bytes | 316.082 s, including 251.781 s publication; grace/readiness limited same-job draining |
| Separate drain 04 | 19 proofs; 3,584 documents / 3,645 chunks / 7,747,339 bytes cleared in 27 batches; 45 structural exclusions | 22.606 s; three exact pairs; no errors, partial parents or unknown commits |
| Execute 05 timing pass | 586 locators and 10 enrollments; 10 proofs cleared 586 documents / 616 chunks / 2,329,270 bytes; eight structural exclusions | 71.451 s, publication 6.943 s; three exact pairs; no errors, partial parents or unknown commits |

Drain 04's `no_ready_parents` was a momentary scheduling result, not proof of all
source bodies being removed. Structural exclusions survive completed proofs.
The driver stopped on a reported lag-metric gate after the 20-parent page; the
metric's meaning and the job's effects are not established by that coincidence.
A fresh reviewed session, current frontier and quality gate remain required
before another bounded page. Never replay consumed intents or resume that stopped
session; reconcile its known result and reservation first.

Insights for the **15:24–15:30 UTC** window reported **76 collector-health upserts /
350,434 ms total / 39,450 ms p99**, while **28 canonical-document UPDATE calls
totaled 573 ms**. These are window aggregates, not job attribution; **COMMIT is
excluded**. The original field-selected zero values were unusable and are not
retained as evidence of absent work. The execute 05 timing pass measured
6.943 s of publication, including 2.150 s in metadata capture and 1.987 s in
publication batches; nested timing categories overlap and must not be summed.
The earlier slow publication did not recur. The upsert is a diagnostic lead,
not an established cause or permission to change writer behavior.

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

Three small changes now support the existing retirement path. #654 replaces
per-document locator UPDATE/deadline calls with one guarded batch UPDATE. #656
lets maintenance explicitly select a five-second archive socket inactivity
allowance: the preceding attempt had failed on the serving 500 ms allowance.
Serving remains at **500 ms**, SDK retries remain off, and overall deadlines,
byte/hash proof and stream closure are unchanged. The #656 production archive
proof passed; the subsequent lock conflict is a separate cause. #658 adds closed
statement-stage labels to publication failures while preserving SQL, locks,
deadlines, committed prefixes and unknown-COMMIT behavior. It does not retroactively
identify the statement or writer responsible for the earlier conflicts.

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

**Remaining bodies:** the read-only sample completed at 14:26:06 UTC, covering
**256 heap pages / 1,150 rows**, with four of five explicitly authorized sources
observed. Current unlocated rows accounted for **1,360,117 of 1,668,728 sampled
nonempty stored bytes (81.5%)**. This supports prioritizing locator publication
and exact body retirement over another whole-table copy. Sample extrapolations
are estimates, not physical savings or guaranteed clear eligibility. Structural
eligibility was not inspected; zero observations do not prove absence. Historical
bodies still require their own authority and retention review. The next priority
source has since advanced through the bounded passes above; that does not expand
the sample into a full-corpus coverage claim.

**Quality and speed:** the original card after source13 was degraded at **22/26**,
with availability/search failures during observed database load and replica lag.
These observations do not establish the precise cause. Its recovery card reached **25/26**,
leaving an isolated scope-latency failure whose exact cause was not established.
Both results remain preserved. After gentler source15, the unchanged original
card passed **26/26 at 14:49:44 UTC**, with all 16 evaluator hashes unchanged:
recall@20 **.9625**, MRR **.6579**, zero backend errors; p95 scope **342.8 ms**,
search **1,706.7 ms**, server **813.0 ms**, show **578.1 ms**, context **3,843.2 ms**,
scan **8,314.5 ms**. This is the pre-#658-deployment recovery card.

The post-#658 original card then passed **26/26**, terminal at **15:02:32 UTC**
after 163.6 seconds, with all 16 verifier hashes unchanged: recall@20 **.9625**,
MRR **.6628**, zero backend/auth/tool errors; p95 scope **351.3 ms**, search
**1,464.2 ms**, server **1,009.2 ms**, show **655.3 ms**, context **5,140.3 ms**,
scan **7,578.7 ms**. The initial priority-source ten-parent card, page20 card and
drain04 card each also passed **26/26**. After execute 05, the unchanged original
card passed **26/26 at 16:04:26 UTC** in **184.0 seconds**, over the same 43 validation
cases and three repetitions with all 16 verifier files and truth pins unchanged:
recall@20 **.9625**, MRR **.6617**, backend/auth/tool errors **0**; p95 scope
**233.3 ms**, search **1,465.4 ms**, server **1,060.8 ms**, show **453.0 ms**,
context **5,068.3 ms**, scan **8,342.7 ms**. All passing cards and the earlier
failures remain preserved.
Passing gates does not close the speed objective or establish a causal gain. Context still uses only three calls to a
variable target and its tail remains unfinished; optional index operation #653
remains unbuilt. The stopped driver is not resumable: continuation requires a
fresh reviewed session and frontier. No consumed intent is replayed and no causal
speed claim is implied.

**Provider capacity:** the **18:41:35 UTC** observation still showed six
nodes. The old primary and two replicas continued serving on
**322,122,547,200-byte (300 GiB)** disks, with replacement in progress. Three new
nodes remained in `restore`, each with **295,279,001,600-byte (275 GiB)** capacity.
Replacement-target metadata has fluctuated: all new restore fields were null at
18:32, but the new primary again listed an unscheduled 300 GiB target at 18:41,
with no reason given. Its actual disk remained 275 GiB. These hints do not
establish a capacity change; no actual capacity growth was observed.
The primary replacement was scheduled at 17:28:51, following the replica schedules
at 17:08:00 and 17:09:12 UTC. This follows the earlier 275 GiB minimum request;
**no new PATCH was submitted** and the cap remains 300 GiB.

The separate **18:23:25 UTC** fetch reported 100% backup fetch and restore active
for all three new nodes. Intrinsic metric measurement time was unavailable;
backup fetch is not restoration or fleet-switchover completion. The observer
ending is also not provider completion. Completed resize, final three-node
capacities, post-resize validation and billed savings remain unproved. Ordinary
maintenance remains held. A smaller compute tier is an optional proposal only:
no compute change or compute savings have been achieved, and measurements during
restore do not establish normal workload headroom.

Measured physical reclaim is **36,751,712,256 bytes (34.23 GiB)** from the chunk
GIN, documents and events rewrites; chunk-body rewrites have reclaimed zero.
Relation allocation, provider usage, completed capacity replacement and billing
are distinct measurements. Earlier observations of three unchanged 300 GiB
volumes remain valid historical evidence, superseded by this in-progress roll.

The fresh **events-only read-only sample completed at 16:07:47 UTC** in
**1,760.929 ms**: 256 heap pages / 2,185 visible rows / 58 external values.
Exact allocations were **41.304 GiB total**, including **14.745 GiB heap /
17.492 GiB main indexes / 9.063 GiB TOAST**. Estimated live external stored data
was **1.029 GiB**, standard error **0.373 GiB**. This supports investigating the
TOAST allocation gap; it does not prove reclaim, a replacement size or a safe
workspace peak. The sampler activation was reset to disabled after its verified result.

Inspection preserved the exact events target and seven-index catalog identity,
observed zero replica byte gaps and completed temporary credential cleanup.
All **18 baseline response comparisons** passed across MCP #658, projection #648
and managed #650. The first events rewrite **completed at 17:02:45 UTC**, with
**2,373.092 seconds** reported elapsed time. Allocation fell from
**44,352,831,488 to 33,937,375,232 bytes**, reclaiming
**10,415,456,256 bytes (9.700 GiB)**. The same table OID, catalog signature and
seven indexes were preserved; the storage file changed from 47742688 to 65206644.
Worker and slot disappearance, temporary-role defaults, role removal and
credential removal were verified. Apply modes were reset to disabled.

The operation retained the reviewed **64 GiB growth limit / 51 GiB free floor /
48 GiB retained-WAL limit / 3,600-second work budget**. The controller-observed peak node growth
was **57.356 GiB**. Later readings were higher: infrastructure reported primary
usage **249,003,704,320 bytes** at 17:03:53 UTC, and the provider
17:03 sample reported **255,210,110,976 bytes**. These differently timed readings
do not establish an instantaneous maximum or prove a hard 64 GiB growth bound
held. The allocation reclaim is exact; settled net volume usage and the timing
of temporary-file/WAL release remain unproved.
The unchanged original card during the rewrite passed **26/26 at 16:26:28 UTC**,
with recall@20 **.9625**, MRR **.6622** and zero backend/auth/tool errors. Its
three-call tool samples are not a causal performance comparison.

**The 9.700 GiB events reclaim and its serving checks passed.** All **18 fresh
post-rewrite response comparisons** matched across the exact deployed fleet.
The original card passed **26/26 at 17:08:45 UTC** in **161.6 seconds**, with
all verifier/truth pins unchanged: recall@20 **.9625**, MRR **.6559**, zero
backend/auth/tool errors; p95 server **808.6 ms**, search **1,354.6 ms**, show
**600.2 ms**, context **5,314.6 ms**, scan **8,961.5 ms**, scope **302.0 ms**.
Most tool probes have three calls; these results preserve quality and access
without proving a causal latency improvement. Earlier degraded cards remain
preserved. Provider primary and replica replacements are in progress; completion,
actual final capacities and post-resize verification remain separate gates.
The canceled chunks copy
lacks a measured temporary heap/index/sort breakdown or exact remaining-work
figure, so a blind retry is unjustified; sweeping first does not reduce data
the failed copy already omitted.

### Next boundaries and acceptance

The immediate dependency order is **finish provider replacement → 18 exact
response pairs and the original card → MCP #662 → six exact response pairs and
the original card → three serial 20-parent qualification pages → measured
50-parent / 55-claim graduation**. The larger settings are a proposal, not active
configuration. Graduation requires three clean pages with matched archive-byte
and record distributions, no residual/unknown outcome, mature drains without a
bounded remainder, and unchanged quality gates. Projected 50-parent publication
must fit 250 seconds and total work 450 seconds within the existing 600-second
job, retaining grace, readback and all proof/byte limits. Otherwise use the timing
evidence to address the measured cost rather than widening budgets.

1. **Continue bounded source coverage with fresh serving gates.** Keep the original
   source's 235-parent frontier and three pending/outlier residuals distinct from
   the priority source's 40 enabled parents. Retain source13's deadline follow-up.
   Reuse fresh proof and the measured gentler limits; preserve disabled markers,
   the 60-second publication grace, exact readback and unknown-commit stops.
   The slow 20-parent publication and faster ten-parent timing pass do not yet
   establish sustainable throughput. Continue only with a fresh reviewed session
   and current frontier after reconciling prior results and reservations; never
   replay a consumed intent or resume the stopped session. Reconcile current
   catalog keys again after cursor exhaustion.

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

Completing current-body retirement alone cannot meet the **<10 GiB** target:
`canonical_events` still allocates **31.61 GiB** after compaction. The five-source
sample's stored-byte shares—**81.5% current-unlocated, 3.3% current-located,
0.8% noncurrent and 14.4% deleted**—are sample composition, not whole-table
proportions, proof of historical dominance or clear eligibility. Revision
replacement currently restores superseded cleared bodies into PostgreSQL through
[`CanonicalPlane.prepare_history`](../../server/recall_server/canonical.py) and
[`restore_outgoing`](../../server/recall_server/canonical_history.py). Deleting
historical bodies requires an immutable revision envelope and preserved historical
receipts, replay and forget semantics first. The immediate order remains finishing
this resize, verifying the release and continuing proven body retirement.

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
