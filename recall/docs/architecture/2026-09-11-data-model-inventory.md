I have a complete picture. Report follows.

---

# Recall Data Model and Flow Inventory

Worktree `/home/ubuntu/worktrees/recall-search-latency-20260910/recall`, HEAD `a23fd3c`. 57 forward-only migrations in `server/schema/*.sql`, applied by version with `schema_migrations(version)` rows appended at the end of each file.

## 1. Schema inventory by plane

### (a) Legacy plane — `001`–`026`
| Table | PK | Notable constraints/indexes | Body storage |
|---|---|---|---|
| `sources` | `id text` | — | pointer |
| `source_grants` | `(source_id,principal_id,permission)` | `source_grants_principal_permission_source_idx` (`015:6`) | — |
| `ingest_batches` | `id uuid` | — | — |
| `source_events` | `id bigserial` | `source_events_parent_idx`, `source_events_batch_idx` (`001:52,54`), `source_events_original_path_idx` (`004:10`) | full JSON body |
| `sessions` | `(source_id,native_id)` (`001:66`) | — | metadata |
| `items` | `id bigserial` | `items_session_idx`, `items_search_vector_idx`+`items_source_session_time_idx` (`004:3,6`), `items_live_*` partials (`013`,`017`,`022`) | full text + `search_vector` |
| `chunks` | `id bigserial` | `chunks_item_idx` | full text |
| `entities` | `(item_id,kind,value)` (`005:9`) | `entities_identity_hash_idx` UNIQUE (`006:6`) | — |
| `item_embeddings` | `item_id` | HNSW `item_embeddings_hnsw_idx` (`010:19`) | vector |
| `turn_embeddings` / `turn_embedding_items` / `turn_embedding_dirty_sessions` | `anchor_item_id` / `(anchor_item_id,item_id)` / `(source_id,session_native_id)` | HNSW (`021:81`) | vector |
| `dead_letters`, `audit_events`, `projection_watermarks`, `projection_backfills`, `embedding_projection_watermarks`, `turn_embedding_projection_watermarks`, `source_profiles`, `source_aliases`, `session_export_cursors` | various | — | — |

No generated columns other than `items.search_vector` (implied by `004:3`).

### (b) Canonical v2 plane — `019_v2_canonical_plane.sql`
| Table | PK | Unique/index | Body |
|---|---|---|---|
| `raw_artifacts` | `(tenant_id,source_id,artifact_id)` `019:43` | `UNIQUE(storage_backend,object_key,version_id)` `019:44`; key must match `^objects/xx/<sha256>$` `019:48`; `raw_artifacts_fleet_transfer_idx` `055:44` | **pointer to S3/filesystem object** |
| `canonical_ingest_jobs` | `(tenant,source,job_id)` `019:62` | `canonical_jobs_status_idx` `019:195` | — |
| `canonical_events` | `(tenant,source,event_id)` `019:83` | `UNIQUE(...native_id,revision)` and `UNIQUE(...native_id,content_sha256)` `019:84-85`; `canonical_events_native_idx` `019:190`, `canonical_events_session_order_idx` `040:16`, `canonical_events_recent_search_idx` `037:5`, `canonical_events_fleet_activity_idx` `055:41` | **`canonical_redacted jsonb` full body** unless `body_location='raw'` (`056:14-21`) |
| — `source_ordinal bigint` | was GENERATED from `canonical_redacted#>'{provenance,byte_start}'` (`040:4-14`), **`DROP EXPRESSION` in `057:7`** → now a plain ingestion-time scalar |
| `canonical_documents` | `(tenant,source,document_id)` `019:108` | `UNIQUE(...native_id,revision)`; partial UNIQUE `canonical_documents_one_current_idx` `019:119`; `canonical_documents_event_lookup_idx` `033:4`; `canonical_documents_inline_body_idx` `056:23` | **`text_redacted text` full doc text** unless `body_location='chunks'` (`056:3-11`) |
| `canonical_chunks` | `(tenant,source,chunk_id)` `019:134` | `UNIQUE(tenant_id,receipt)`, `UNIQUE(tenant,source,document_id,ordinal)` `019:135-136`; `canonical_chunks_document_idx` `019:192`; GIN `canonical_chunks_search_idx` `028:84` | **`text_redacted` + GENERATED `search_vector tsvector` `028:81-82`** |
| `receipt_redirects` | `(tenant_id,old_receipt)` `019:149` | reasons limited to `v2_migration`/`canonical_rewrite` | — |
| `forget_tombstones` | `(tenant,source,target_identity_sha256)` `019:163` | `forget_tombstones_idempotency_idx` UNIQUE `020:58` | — |
| `canonical_audit_events` | `(tenant,source,audit_id)` `019:184` | — | counters only, no text |
| `canonical_chunk_embeddings` | `(tenant,source,chunk_id)` `028:98` | HNSW `028:108`, scope idx `028:105` | vector |
| `canonical_embedding_projection_watermarks` | `(runtime_fingerprint,tenant_scope)` `036:10` | — | — |

### (c) Logical evidence — `039`, `035`, `052`
- `canonical_evidence_documents` PK `(tenant,source,logical_document_id)` `039:33`, with **four** uniques (`native_parent_id`; `(ldoc,revision)`; `evidence_id`; manifest object triple) `039:34-37`; `..._current_source_idx` `039:52`, `..._time_scope_idx` `048:5`. Stores **pointers only** (manifest artifact ref + `document_content_sha256`, counts, time bounds).
- `canonical_evidence_document_parts` PK `(tenant,source,ldoc,revision,part_ordinal)` `039:81`, `UNIQUE(storage_backend,object_key,version_id)` `039:84`, `ON DELETE CASCADE` from the document, media type pinned to `...logical-document-part+jsonl` `039:92`; time bounds added `052:4`, `..._parts_time_idx` `052:20`. **Pointer to a JSONL part in S3 that contains the full record text.**
- `canonical_evidence_document_queue` PK `(tenant,source,native_parent_id)` `039:103` with `generation`, `reason ∈ {backfill,ingest,forget}`, work index `039:109`.
- `canonical_evidence_cleanup_queue` PK `(tenant,source,artifact_id)` `039:135` — orphaned-object GC.
- `canonical_evidence_document_actors` PK `(tenant,source,ldoc,revision,actor_id,relation)` `045:112`.
- **`canonical_evidence_objects` (`035`) is the superseded v1 per-`document_id` evidence plane** (PK `(tenant,source,document_id)` `035:20`, media type `...evidence+json`); still written by `evidence_projection.py:514`.

### (d) Passages — `041`, `042`, `043`, `045`
- `canonical_passage_documents` PK `(tenant,source,logical_document_id)` `041:17`, UNIQUE `(…,revision,policy_fingerprint)` `041:18`, FK cascade on `canonical_evidence_documents(…,revision)` `041:22`.
- `canonical_passages` PK `(tenant,source,passage_id)` `041:54`, UNIQUE `(…ldoc,revision,policy_fingerprint,ordinal)` `041:55`. Columns: `roles text[]`, **`receipts text[]`** `041:47`, **`spans jsonb`** `041:48`, **`text_redacted`** `041:49`, and **GENERATED `search_vector tsvector` `041:51-52`**; GIN `041:85`, time idx `041:80`, doc idx `041:75`.
- `canonical_passage_embeddings` PK `(tenant,source,passage_id)` `041:98`, `halfvec(512)`, HNSW m=16/ef=64 `041:108`; `043` adds **unused-by-default `embedding_1536`/`embedding_3072` halfvec columns** (`043:7-8`).
- `canonical_passage_projection_queue` PK `(tenant,source,ldoc)` `041:121`, `reason ∈ {backfill,logical-update}`, work idx `041:129`.
- `canonical_passage_actors` PK `(tenant,source,passage_id,actor_id,relation)` `045:137`, lookup idx `045:145`.
- Representation tables (`042`): `canonical_passage_contexts` PK `(tenant,source,passage_id,context_fingerprint)` `042:15`, **a third copy of passage prose (`context_text_redacted`) plus its own GENERATED `search_vector` `042:9-13` and GIN `042:25`**; `canonical_passage_embedding_representations` PK `(…,representation_fingerprint)` `042:39`. Written only from `passage_representations.py` and `cli.py` (experimental arm).

### (e) Parquet scan plane — `050`, `051`, `053`, `054`
- `canonical_parquet_scan_queue` PK `(tenant,source,bucket_start)` `050:10`, `CHECK EXTRACT(DAY…)=1` (monthly buckets) `050:13`, work idx `050:16`.
- `canonical_parquet_scan_shards` PK originally `(tenant,source,bucket_start,dataset)` `050:41`, **re-keyed to include `shard_index` in `051:11`**; `UNIQUE(storage_backend,object_key,version_id)` `050:42`; `dataset` widened from `{documents,records,actors}` to include `passages` in `053:6-8`. Pointer-only (Parquet object in S3).
- `050:64-85` and `053:14-25` / `054` are **full-corpus re-seed statements** that enqueue every source-month.

### (f) Identity / authz / control
`brain_tenants` (`019:4`), `brain_principals` (`019:13`), `canonical_sources` (`019:22`, FK to tenant + owner principal), `brain_organizations`/`brain_spaces`/`brain_memberships`/`brain_access_grants` (`028:4,14,30,38`), `canonical_source_grants` PK `(tenant,principal,source)` + lookup idx (`028:49,56`), `mcp_credentials` (`028:60`), `collector_credentials` (`002:4`, later `+tenant_id` `027:4`, `+installation_id`/`device_id` `030`), `admin_credentials`/`admin_sessions`/`provider_connections`/`connector_installations`/`oauth_sessions`/`control_audit_events` (`029`), `connector_installations` worker lease columns (`031:4-8`), `external_identity_bindings` (`032:7`), `brain_invitations` (`032:26`), **`authorization_audit_events` `id bigserial` (`032:58`, lookup idx `032:72`)**, `identity_oauth_states` (`044:28`), `brain_actors`/`brain_actor_aliases`/`brain_actor_principals`/`brain_actor_external_identities`/`canonical_source_actor_bindings`/`canonical_event_actors` (`045:6,19,35,51,69,82`), `collector_health_reports` PK `(tenant,source)` `055:33` (content-free heartbeat, latest-wins).

### (g) Memory / capture
No dedicated tables. Capture is a normal canonical event: `capture.py:67 build_capture_event` → `/mcp` `recall_capture` → the same ingest path; provenance is `source_profiles.family='deliberate_capture'` (`007:6`, `012:8`) and `collector_credentials.capture_origin` (`016:4`).

### (h) Other
`agent_runs` PK `(tenant_id,run_id)` `038:28` (+`status_message` `047:4`), `schema_migrations`, `canonical_evidence_cleanup_queue` (above). `capabilities.py:28-30` enumerates the v2 table set for the capability probe.

## 2. Duplication map

One ingested record's text exists in up to **seven** places, each with its own index:

1. **S3 raw object** — `raw_artifacts.object_key` (`019:35`), content-addressed by `content_sha256`.
2. **`canonical_events.canonical_redacted` jsonb** (`019:81`) — full canonical body inline in Postgres.
3. **`canonical_documents.text_redacted`** (`019:104`) — full flattened document text.
4. **`canonical_chunks.text_redacted` + GENERATED `search_vector`** (`019:130`, `028:81`) — third text copy plus a TOASTed tsvector per chunk. This is the 79 GB object; `passage_retrieval.py:25-30` explicitly notes the sparse arm "scans `canonical_chunks` (every record, tool output included) through one global GIN index."
5. **Evidence parts in S3** — `canonical_evidence_document_parts` JSONL parts (`039:57`), full record text again; plus the v1 `canonical_evidence_objects` `...evidence+json` blob (`035:3`).
6. **`canonical_passages.text_redacted` + GENERATED `search_vector` + `receipts text[]` + `spans jsonb`** (`041:47-52`) — fourth Postgres text copy, with receipts duplicated from `canonical_chunks.receipt`.
7. **Parquet `passages` and `records` datasets** — `canonical_parquet_scan_shards` objects, passage text re-serialized per source-month (`parquet_scan.py:240` schema, `:962 _passage_row`, `:1025`).

Plus embeddings duplicated across `item_embeddings`/`turn_embeddings` (legacy), `canonical_chunk_embeddings` (`028:88`), `canonical_passage_embeddings` (`041:88`), and `canonical_passage_embedding_representations` (`042:28`) — four vector stores over overlapping text. `canonical_passage_contexts.context_text_redacted` (`042:7`) is a fifth Postgres prose copy with a fifth GIN index.

Mitigation exists but is opt-in: `canonical_thinning.py:68 thin_canonical_bodies` compacts `canonical_redacted` to routing metadata and flips `body_location` (`056`), but it is only reachable from `cli.py:2041` and `cli.py:2301` — no worker calls it.

## 3. Write path per ingested record

**Per record (collector → server):**
1. `POST /v2/archive/objects` (`app.py:1210`) → `CanonicalArchiveGateway.put_raw` → one `raw_artifacts` row + one S3 PUT. Content-addressed: identical payloads collapse (`archive.py:113-122`).
2. `POST /v2/ingest/canonical` (`app.py:1260`) → `canonical_plane.ingest_batch` (`canonical.py:647`). Per event, inside one transaction (`canonical.py:392-660`): `register_source` upserts (§5), forget-tombstone check `canonical.py:401`, advisory lock on `v2\x1f{tenant}\x1f{source}\x1f{native_id}` `canonical.py:412`, `INSERT raw_artifacts … ON CONFLICT DO NOTHING` `canonical.py:417`, insert `canonical_events`, actor links, **`UPDATE canonical_documents SET is_current=false`** `canonical.py:552`, insert the new `canonical_documents` row `canonical.py:573`, `executemany` N `canonical_chunks` rows `canonical.py:591`, then `mark_logical_evidence_dirty` `canonical.py:619` and one `canonical_audit_events` row `canonical.py:631`.

**Per logical document (async):**
3. `mark_logical_evidence_dirty` (`logical_evidence_projection.py:50`) upserts `canonical_evidence_document_queue` keyed by `native_parent_id` with `generation = generation+1` — many records collapse to one queue row per session/thread.
4. `CanonicalLogicalEvidenceProjector.project_pending` (`:1485`) re-reads the whole group from `canonical_chunks` (`:675`, `:1720`), builds new JSONL parts, uploads them, then in `_commit` (`:1067`) performs a **full replacement**: `DELETE FROM canonical_evidence_documents` `:1193` → `INSERT` `:1203` → `INSERT … parts` `:1244` → `INSERT … actors` `:1285` → enqueue `canonical_passage_projection_queue` `:1322` → enqueue `canonical_parquet_scan_queue` `:869` → `DELETE … queue` `:1161/:1361`. Superseded objects go to `canonical_evidence_cleanup_queue` (`:801/:817`), drained by `drain_cleanup` `:912`. Reuse: `logical_evidence.py:779 reusable_part` / `:825` keeps byte-identical parts instead of re-uploading.
5. `CanonicalPassageProjector._commit` (`passage_index.py:350`) is likewise **full replacement per logical document**: advisory lock `:355`, staleness guard comparing queue `revision/generation/changed_at` and `document_content_sha256` `:371-390`, then `CREATE TEMP TABLE recall_reusable_passage_embeddings … ON COMMIT DROP` `:391-409` snapshotting existing embeddings, `DELETE FROM canonical_passage_documents` `:411` (cascades to all passages, actors, embeddings), `COPY canonical_passages` `:440`, `COPY canonical_passage_actors` `:477`, then re-attach embeddings by `content_sha256 = passage.text_sha256` `:493-528`. **Unchanged passage text keeps its vector; changed text loses it and must be re-embedded.** Finally enqueue `canonical_parquet_scan_queue` for every month the document spans `:544+`.
6. Embedding workers fill `canonical_passage_embeddings` / `canonical_chunk_embeddings` under watermarks keyed by `(runtime_fingerprint, tenant_scope)` (`036:10`; `passage_index.py:725`, `canonical_retrieval.py:247-460`).
7. `CanonicalParquetScanProjector._build` (`parquet_scan.py:985`) rebuilds **all four datasets for an entire source-month** whenever any document in that month changes — a per-record edit re-serializes the month. `_legacy_upload` `:875` can retain immutable v1 datasets and add only `passages` (`legacy_datasets = SCAN_DATASETS - {'passages'}` `:889`).

**Re-projection triggers:** any ingest of a record whose `native_parent_id` matches (step 3); tombstones, which fan out via `_linked_native_ids` to every linked native id (`canonical.py:542`); `reason='forget'`; and one-shot migration re-seeds (`050:64`, `053:14`, `054`).

**Idempotent/reused:** content-addressed artifact keys; `canonical_events` `UNIQUE(native_id,content_sha256)` making replays no-ops (the endpoint returns 200 + `replay` `app.py:1358`); `reusable_part`; `recall_reusable_passage_embeddings`; `generation_sha256` on parquet shards (`parquet_scan.py:920`).

## 4. Read path per MCP tool

MCP tool table is `mcp.py:110 ALL_READ_TOOLS`; dispatch is `mcp.py:785 _call_tool`. With `RECALL_CANONICAL_MCP_ENABLED=1`, the store is `canonical_retrieval.bind(principal)` (`app.py:1557-1560`); otherwise the legacy `BrainStore` methods (`db.py:3017/3443/3730`).

| Tool | Entry | Tables/planes |
|---|---|---|
| `recall_search` | `canonical_retrieval.py:811 search` → `:1706 passage_hints` → `passage_retrieval.py:981` | **`canonical_passages.search_vector` (GIN) + `canonical_passage_embeddings` (HNSW)** as the primary arms; an identifier-only sparse arm over **`canonical_chunks`** (`passage_retrieval.py:557,589`); `LEFT JOIN canonical_chunks live_chunk` liveness checks at `:469,:927,:1245,:1295,:1397`; `canonical_evidence_documents` for `logical_document_id`. |
| `recall_scope` | `canonical_retrieval.py:1150` | `canonical_evidence_documents` (+`..._time_scope_idx` `048:5`), `canonical_evidence_document_actors`, `brain_actors`/`brain_actor_aliases` `:1219`, `source_profiles`. No chunks, no S3. |
| `recall_people` | `canonical_retrieval.py:709` | `canonical_source_actor_bindings` + `brain_actors` + `source_profiles` only. |
| `recall_show` | `canonical_retrieval.py:3989` | `canonical_events` + `canonical_documents` via `_receipt_event`, then **full `canonical_chunks` read for the document** `:4014`. `around`/`tail`/`prompts` are rejected in the v2 store — legacy-only features. |
| `recall_session_context` | `canonical_retrieval.py:2936` | `canonical_events` (session order idx `040:16`) joined to `canonical_documents` and **`canonical_chunks` LATERAL, 2 chunks/neighbour** `:2991`, plus 3 anchor chunks `:3030`. |
| `recall_exec` / `recall_exec_map` | `:2021` / `:2208` → `deep_inspection.py:837 ArchilDeepInspector` | Postgres only to resolve `ldoc_*` → manifest/part object keys; the bodies are read **from S3 via read-only Archil mounts at `/mnt/archil/evidence`** (`deep_inspection.py:521,574,678`). |
| `recall_scan` | `:1560 execute_parquet_scan` | `canonical_parquet_scan_shards` for the shard list (`:1622 _parquet_shards`), optional `brain_actors` for `person`; then DuckDB over **Parquet objects on the Archil `/datasets` mount** (`deep_inspection.py:684`). Hard cap 511 shards `:1636`. |
| `recall_related` | `:4033` | `canonical_chunks` ⋈ `canonical_documents` ⋈ `canonical_events` with `canonical_redacted #>> '{provenance,cwd}'` filters — **unindexed JSON predicates over the chunk table.** |

So the 79 GB `canonical_chunks` table is still on the hot path for `show`, `session_context`, `related`, search-liveness joins, and the identifier arm; only `scope`, `people`, `exec*`, and `scan` avoid it.

## 5. Per-request control writes

- **Every collector write request** (`/v2/archive/objects`, `/v2/ingest/canonical`, `/v2/ingest/status`, `/v2/collector/health`) calls `canonical_authority` (`app.py:561`) and then, inside the ingest transaction, `CanonicalPlane.register_source` (`canonical.py:266`), which issues **four write statements unconditionally**: `INSERT brain_tenants … ON CONFLICT DO NOTHING` `:274`, `INSERT brain_principals …` `:278`, `INSERT canonical_sources …` `:283`, and `INSERT canonical_source_grants … DO UPDATE SET permission='owner'` `:296` — the last is an unconditional row update on every single ingest, not a no-op.
- **Every ingested event** writes one `canonical_audit_events` row (`canonical.py:631`; also `:1363`, `:1641`, `:1701` for batch/forget/status paths).
- **Every MCP tool call** writes one `authorization_audit_events` row: `authorize_mcp` (`app.py:539`) unconditionally calls `store.record_authorization_event` `app.py:550` → `db.py:962 INSERT INTO authorization_audit_events`, on its **own connection** (`db.py:965 with self.connect()`), allowed or denied. This is one extra connection acquisition + insert per tool call before any read work.
- `/v2/collector/health` writes/overwrites one `collector_health_reports` row per request (`app.py:1352`).
- `canonical_ingest_jobs` rows are written per batch inside `ingest_batch`; `agent_runs`/`agent_run_progress` per `recall_scan`-style agent run (`038`, `047`).
- Identity flows write `brain_principals`, `brain_memberships`, `brain_access_grants`, `external_identity_bindings` (`db.py:1126-1140`), and `control_audit_events` for admin actions (`029:108`).

## 6. Tenancy

Every v2 table leads with `tenant_id` in its primary key, and every retrieval query is a row filter, never a session/role setting: `BoundCanonicalRetrieval` captures `self.tenant_id` and `self.authorized_sources` at bind time (`canonical_retrieval.py:512-514`), and each SQL statement passes `tenant_id=%s AND source_id=ANY(%s)` (e.g. `:743`, `:888`, `:1440`, `:2991`, `:4014`). `authorized_sources` comes from `store.authorized_canonical_source_ids(tenant_id, principal_id)` for MCP credentials (`app.py:404`), i.e. `canonical_source_grants`; the legacy branch uses `authorized_source_ids(principal_id)` over `source_grants` (`app.py:410`). Requests may pin a tenant via the brain path, and a credential whose `tenant_id` mismatches is rejected (`app.py:394-398`).

**There is no table partitioning anywhere** — no `PARTITION BY` in any of the 57 migrations. All indexes are global, which is exactly why the shared GIN index on `canonical_chunks` is described as "one global GIN index" (`passage_retrieval.py:25`). Referential integrity is per-tenant via composite FKs (e.g. `019:86-89`, `041:22`).

The code is structurally multi-tenant, but the operational assumptions are small-N: embedding watermarks are keyed `(runtime_fingerprint, tenant_scope)` where `tenant_scope = tenant_id or ''` (`canonical_retrieval.py:247`, `passage_index.py:725`), the embedding worker refuses to mix "one tenant with parallel tenants" (`embedding_worker.py:96`), `recall_scan` caps at 511 parquet shards per call (`canonical_retrieval.py:1636`), and the deploy profile is a single PlanetScale instance with two replicas and 50 GiB initial storage (`deploy/README.md:118-119`).

## 7. Legacy vs v2

Legacy tables are referenced in **86 places in `db.py`**, 14 in `cli.py`, 3 in `capabilities.py`, and nowhere else — the legacy plane is entirely contained in `BrainStore`.

Still live in production paths:
- `BrainStore` is always constructed (`app.py:1827`) and remains the store for auth, `/v1/receipts/resolve`, `/metrics`, readiness, admin, and — critically — the **Slack/generic webhook ingest paths, which call `self.store.ingest(...)`, the legacy writer** (`app.py:1449`, `app.py:1492`, `app.py:1701`). So `source_events`/`items`/`chunks`/`sessions`/`ingest_batches` are still written for webhook-sourced records.
- `/v1/search`, `/v1/show`, `/v1/related`, `/v1/sessions/export` (`app.py:1630-1668`) read the legacy plane unconditionally.
- MCP reads go to v2 only when `RECALL_CANONICAL_MCP_ENABLED=1` (`app.py:1557`); the flag is commented out in `deploy/service.env.example:41` and gated in `deploy/README.md:414`, so the legacy read path is the fallback, not dead code.

Deletable once webhook ingest and `/v1/*` are cut over to v2: `BrainStore.search`/`show`/`related`/`session_export` (`db.py:3017,3443,3730`), the legacy embedding stack (`item_embeddings`, `turn_embeddings`, `turn_embedding_items`, `turn_embedding_dirty_sessions` and their watermark tables — `010`, `014`, `021`), `entities`/`projection_backfills` (`005`), `projectors.py`, `projection_worker.py`, `semantic.py`'s legacy arms, and `canonical_retrieval.py:822 _legacy_chunk_search_for_eval` (named for evals, not called by any tool). Independently deletable in the v2 plane: `canonical_evidence_objects` + the whole of `evidence_projection.py`'s object writer (`:514`), superseded by `canonical_evidence_documents`/`_parts` but still wired at `app.py:1849`; and `canonical_passage_contexts`/`canonical_passage_embedding_representations` (`042`) plus `043`'s unused `embedding_1536`/`embedding_3072` columns, which only `passage_representations.py` and `cli.py` touch.

## 8. Known TODOs and limitations in code/docs

Recent search-latency work left the sharpest notes, all in `passage_retrieval.py:20-54`:
- "The sparse-exact arm scans `canonical_chunks` (every record, tool output included) through one global GIN index… for prose it scores millions of chunks by rank and runs to the deadline while dense and passage-lexical already cover the same words" — hence `IDENTIFIER_TOKEN_RE` gating and `SPARSE_ARM_BUDGET_FRACTION = 0.5`.
- "Ranking every full-text match reads each passage's TOASTed `search_vector`: **~3 random disk reads per match on the managed instance**" — hence `RANKED_PHASE_BUDGET_FRACTION = 0.7` and a recency-ordered fallback.
- "HNSW cost grows with the requested neighbour count; **200 neighbours cost ~1.7 s cold on the managed instance versus 3+ s for 400**" — `DENSE_NEAREST_LIMIT = 400`, `LIVENESS_OVERSAMPLE = 2`, `MAX_EXACT_DENSE_SCOPE_PASSAGES = 20_000`.
- Commits `#479`–`#485` are all latency mitigations against this shape; `#484` reverted HNSW scan settings, indicating the tuning is not settled.

Elsewhere:
- `057_materialized_source_ordinal.sql:3-5`: the generated `source_ordinal` was dematerialized specifically to allow "canonical event JSON to be rewritten without a second full-table generated-column pass" — an explicit acknowledgement that full-table generated-column rewrites are unaffordable.
- `deploy/README.md:820`: "Searches have a 300ms database-work budget by default"; `:133` the 300 ms loopback default does not hold "reliably at multi-million-item scale".
- `deploy/README.md:375`: connection exhaustion returns `503 brain_busy` after 5 s.
- `deploy/README.md:363`: `recall_scan` is capped at 240 s, "below Archil's documented five-minute response limit"; `:333` requires the evidence archive to work "for every architecture Archil may execute on" (see `#466`).
- `deploy/README.md:816`: "Before multi-user scale or C10 production cutover, use provider point-in-time recovery" — multi-user scale is still ahead of the current deployment.
- `deploy/README.md:309`/`:957`/`:995` and `049`, `024`, `025`, `026` are one-off repair/dedupe migrations; `050:64`, `053:14`, `054` are full-corpus re-seeds, so at least three migrations have already required re-projecting the entire passage/parquet corpus.
- `docs/CODEX_ARCHIVE_IDENTITY.md:20`: `canonical_rewrite` receipt redirects are "reserved for declared legacy" cases.
- `docs/actor-attribution.md:71`: legacy `coding_history` sources registered outside employee device enrollment need repair (what `049` does).

No `TODO`/`FIXME` markers exist in the Python or SQL — the limitations are recorded as prose comments and migration commentary instead.