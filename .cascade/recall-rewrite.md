# Recall rewrite: one cascade, five hills, every loop verified

_Living plan. Status legend: todo / doing / done / blocked. Evidence = PR + card history row timestamp._

## Status board

_Ops note 2026-09-11: root disk hit 100% on greppy3 during H0; back to 95% (44 GB free) by 06:15 UTC without deleting anything of ours. Worker finding 06:40 UTC: recall-logical-evidence-worker cycles take ~20 min at 5 docs/cycle while logical_pending grows 212 → 278 in 80 min; freshness 64 h behind. Root cause under investigation (thinning 1000 bodies/cycle + embedding 64/cycle share the cycle)._
| task | status | owner | evidence |
|---|---|---|---|
| H0-0 plan + briefings committed | done | lead | PR #489 merged (eef1216) |
| H0-1 churn probe | done | agent churn | PR #492 merged (main 1889db0) after rebase + FakeSemanticRuntime fix |
| H0-2 forget-latency probe | done | agent forget | PR #490 merged (main 6e5f1ce); live row still pending behind --forget-probe |
| H0-3 storage cost probe | done (code) | agent storage | PR #491 merged (main f28ad8e), MCP deployed dep-dahq42h594qs738434jg; card 06:5x UTC: cost.storage skipped in ship_and_measure (no PS/S3 creds passed) — nightly script has them; verify at 06:00 UTC run |
| H0-4 shadow-projection tool | folded into H1-T3 (`passage-shadow-diff`) | lead | |
| H0-5 ship_and_measure in repo | done | lead | recall/scripts/ship_and_measure.sh in #489 |
| H0-6 nightly card + Slack | done (dry run) | lead | ~/bin/recall-nightly-card.sh, cron 06:00 UTC; dry run 2026-09-11 05:43 wrote the docs page; first Slack post at the next run |
| H1-T1 debounce | done (code) / flag off in prod | lead | PR #493 merged (main d750446), MCP deployed dep-dahpts1594qs73838ov0; e2e quiet=300 → 0 docs/1 waiting, forget immediate; 1102 unit OK. Card row 2026-09-11 ~06:40 UTC: server p95 8.1 s cold after restart, recall@20 0.5417, MRR 0.346. Migrations 058+059 applied to prod 07:0x UTC via temp PlanetScale role (CIDR + role deleted, file shredded; 058 built CONCURRENTLY, schema=59, runtime grants refreshed). Worker deploy to f28ad8e triggered 07:1x UTC. Miguel approved 07:05 UTC: worker dockerCommand += `--quiet-seconds 90 --max-wait-seconds 600` (Render PATCH 200), redeploy triggered; verify `logical_waiting` in cycle logs and logical_pending trend over the next 24 h |
| H1-T5 identity cache + audit batching | done (code) | agent h1-t5 | PR #497 merged (main 81e35c5), MCP + worker deployed 08:59 UTC; card 17/17 ok, authorization leaks 0. Live check: PlanetScale insights brain_tenants/canonical_sources inserts per day should fall from ~122k → ~0 (check tomorrow) |
| H1-T6 legacy write retirement | deployed | agent h1-t6 + lead | PR #494 merged (main 087a5de); RECALL_LEGACY_INGEST_TENANT_ID=tenant:company:parcha set on MCP (approved); MCP deploy + card running | PR #494 rebased twice on main, CI green (1181 unit; e2e_connector_host, e2e_core_container fixed by pinning legacy flags). Before deploy: MCP env RECALL_LEGACY_INGEST_TENANT_ID=tenant:company:parcha (default tenant:personal) — prod-config write, ask Miguel |
| H0-7 per-phase cycle timing | done | agent h0-7 | PR #496 merged (main 518e802); MCP + worker deployed 08:4x UTC; card 16/16 gates ok on availability+latency (server p95 139 ms); recall_exec/scan p95 ~11 s on this run (cold sandbox after deploy, watch) |
| H1-T2 in-place logical revisions | deployed | agent h1-t2 | PR #498 merged (main a946873). Cutover approved by Miguel ~17:15 UTC: worker suspended 17:2x, 060 + 060b applied via psql with lock_timeout (cli migrate deadlocked twice: it runs all files in one transaction — fix in H5), schema=60, 5 FKs valid, unique index built, no invalid indexes; worker resumed + deployed 17:41 (087a5de); first cycles: inserted 898/deleted 719/retained 0 = expected one-time id rewrite; 18:33 UTC cycle: inserted 78 / deleted 27 / retained 612 — differential commit confirmed in prod. Transient: cards at 17:5x and 18:2x show lexical p95 13-15 s during the rewrite wave (GIN churn + cache eviction); re-check on the 06:00 UTC nightly card, then T7 REINDEX |
| H1-T3 stable passage ids + differential commit | deployed | agent h1-t3 | PR #501 merged (main 35bedbc). Prod shadow-diff (Render job, MCP image): 6 sources × 50 docs, receipt_set_equal 300/300, recomputed == existing, ids_shared 0 (expected). Deployed with T2 in the same window |
| H1-T4 parquet delta fragments | deployed | agent h1-t4 | PR #502 merged (main 8b2dfa4); 1158 unit, 40/40 e2e; MCP deployed, card 16/16 ok; migration 061 applied 18:1x UTC (lock_timeout retry once); worker live 8b2dfa4 18:2x, first cycle shows parquet_fragments_* counters |
| H1-T7a thinning yields to freshness | done | lead | PR #499 merged (main 7d46879), worker live 08:55 UTC; cycles 916 s → 89-270 s, thin 713 s → 33 s (100 bodies), logical_pending 283 → 274 in 10 min and falling | H0-7 first timed cycle 08:41 UTC: cycle 916 s = thin 713 s + embed 101 s + logical 51 s + passage 51 s; thinner now takes a 100-body batch while work is queued (`--thin-busy-batch-size`), 1000 when idle |
| H1-T7 thinning steady state + REINDEX | todo | | after ≥ 2 quiet days post-cutover: REINDEX passages GIN/HNSW CONCURRENTLY, VACUUM ANALYZE, storage runbook |
| H1-T8 (new) cleanup + embedding throughput | todo | | 18:22 UTC cycle: 200 S3 deletes took 933 s in the logical phase (≈4.6 s/object, upload_concurrency 1) → batch DeleteObjects + concurrency; embed 64 passages = 100-150 s every cycle (voyage, RECALL_EMBEDDING_WORKERS=4) → embedding is the steady-state bottleneck, size per H5-2/H5-3 |
| H2 a..e | todo | | |
| H3 a..f | todo | | |
| H4 1..4 | todo | | |
| H5 1..5 | todo | | |


## Context

The 2026-09-11 architecture assessment (https://docs.greppy3.parcha.dev/2026-09-11-recall-architecture-assessment.html)
found Recall to be a write-dominated system built like a read-heavy one: ~1,100 collector writes vs ~50 MCP calls a day,
each record's text stored in seven places and vectors in four, six of them in a managed Postgres whose cache cannot hold
its own derived indexes, and full-replacement re-projection of whole sessions on every change (~150k passages rewritten
per day for ~320 documents). Consequences measured this week: search at the 20 s deadline until PRs #479–#485 and a
PS-160 resize, freshness 58 h behind with 13 pending projections, an $800 one-off re-embed in September, 108 GB in
Postgres of which 79 GB is `canonical_chunks`. The read path the 2026 literature favors (agent running code over raw
evidence in S3, LongMemEval-V2 72.5% vs 48.5% for RAG) is the one Recall already has and under-invests in.

Miguel's ask: turn the entire rewrite into a Cascade with strong verification in each loop, build all of it, plan the
whole thing. This document is the plan. The living Cascade plan (per `~/.claude/skills/cascade/SKILL.md`, 297-word
version, verified equal to origin/main 989295e) lives at
`~/worktrees/recall-rewrite-20260911/.cascade/recall-rewrite.md` and is updated every loop.

## Outcome and final acceptance

**Outcome.** Postgres is a catalog (< 10 GB, no bodies/tsvectors/vectors); S3 holds evidence and a Lance search plane
(vector + BM25 + SQL) built incrementally at turn granularity from an outbox; the MCP exposes hybrid search with
convex fusion and a reranker, open/session_context from S3 parts, exec/scan in the Archil sandbox, plus recap and
decision tools; ingest and MCP are separate services; the systems card runs nightly and is green.

**Final acceptance, measured by `python -m evals.systems_card run` (all dimensions) against the live brain:**
| gate | target |
|---|---|
| search server p95, warm | ≤ 1 s |
| search p95, cold new query | ≤ 2 s |
| deadline exceeded | ≤ 1% |
| boundary recall@20 / MRR (validation split) | ≥ 0.75 / ≥ 0.50; test split ≥ 0.70 / ≥ 0.45 checked once at the end |
| freshness: newest passage age, any active source | ≤ 15 min; projection_pending 0 |
| passages rewritten per day | ≤ 20k (from ~150k) at the same ingest rate |
| Postgres total size | ≤ 10 GB |
| authorization leaks / secret hits | 0 / 0 |
| forget: tombstoned passage absent from search | ≤ 10 min |
| cost | PlanetScale ≤ $150/mo tier; embeddings inside free tier at 7 seats; card cost probe green |
| ops | card green 14 consecutive nights; zero Render restarts from health checks |

## Principles that every loop honors

1. One authority (S3 raw + evidence parts); everything else is a rebuildable projection.
2. Append-only at the turn; nothing already indexed is rewritten by a session growing.
3. Verbatim text stays primary; recaps/decisions are supplements with receipts.
4. The caller is the retriever; the server gives it candidates, receipts, and code execution.
5. No loop is done until the exit check passes on the deployed candidate and the card row is appended.
6. Nothing rewrites the corpus (re-embed, re-project) without a dry-run reuse report and a cost estimate first.

## Verification machinery (built in H0, used by every hill)

- **Unit + e2e in CI** (`.github/workflows/recall-ci.yml`): fresh pgvector Postgres e2e suite; new e2e scripts added per hill.
- **Systems card** (`recall/evals/systems_card/`): the ruler. Every hill adds probes/gates it needs (churn, embedding lag,
  forget latency, Lance parity). History rows are the evidence of record; the plan file cites the row timestamp.
- **Shadow comparison**: before any projection or search-plane cutover, a read-only job runs old and new side by side on a
  sample (sessions for projection; truth set for search) and emits a content-free parity report.
- **Live-DB EXPLAIN** only through a temporary read-only PlanetScale role + role-scoped CIDR, both deleted after (procedure
  from 2026-09-10; never print the password; shred the file).
- **Ship loop** per PR: CI green → squash merge → Render deploy → card run → compare to previous row → plan updated.
  (`ship_and_measure.sh` pattern from the latency hill, promoted into `recall/scripts/`.)

## Hills

(Task tables with files, approach, exit checks, and sizes follow. Dependencies are explicit; independent tasks run in
parallel via subagents with owned files.)

### H0: Instruments first (1–2 days)

Exit checks for later hills need probes that do not exist yet. Build them first so every loop is measured the same way.

| ID | Deliverable | Files | Exit check |
|---|---|---|---|
| H0-1 | Card probe `freshness.projection_churn`: passages rewritten/day, docs re-projected/day, embedded/day, embedding lag (unembedded passages), from worker cycle logs + `/metrics` | `evals/systems_card/corpus.py`, `projection_worker.py` (emit counters), `app.py` metrics | unit test with fake logs; card shows the ~150k/day baseline row |
| H0-2 | Card probe `integrity.forget_latency`: capture a synthetic memory via `recall_capture`, forget it, poll `recall_search` for its unique phrase; report minutes to absence | `evals/systems_card/corpus.py` | unit test with fake brain; live row (baseline measured) |
| H0-3 | Card probe `cost.storage`: Postgres size by table (via PlanetScale insights/metrics API), S3 bytes by prefix, embed calls/day | `evals/systems_card/cost.py` | baseline row: 108 GB, 79 GB chunks |
| H0-4 | Shadow-projection tool: `recall_server.cli shadow-projection --sample N` projects sampled sessions with old and new projectors into temp schemas and reports receipt/passage-set parity (content-free) | `server/recall_server/cli.py`, new `shadow_projection.py` | unit test; used as H1 gate |
| H0-5 | `recall/scripts/ship_and_measure.sh`: CI wait → merge → deploy → card → diff vs previous row (promoted from the latency hill) | `recall/scripts/` | script used by every later loop |
| H0-6 | Nightly card cron on greppy3 + Slack summary through the tether skill; card history committed content-free to `~/docs` | cron, tether | first nightly post |

### H1: Stop the churn (2–3 weeks)

Root causes in the code: the logical commit is delete-then-insert with `revision+1`
(`logical_evidence_projection.py:1192-1241`) and the passage FK cascades on `revision` (`schema/041:22-27`), so every
session change destroys its passages; passage ids hash `revision` and `len(passages)` (`passage_projection.py:384-409`),
so even identical prefix windows get new ids; there is no quiet period (`mark_logical_evidence_dirty:50-84`,
`_pending:397-437`); Parquet rebuilds the whole source-month (`parquet_scan.py:985-1167`). S3 parts are already reused
by `(ordinal, content_sha256)`; only the database churns.

Order: T1 → T2 → T3 → T4; T5 and T6 in parallel with T2–T4; T7 last. Deploy T1 alone first, then T2+T3 in one
low-traffic release (they cause one final full rewrite per session as new ids land).

| ID | Deliverable | Files / approach | Exit check |
|---|---|---|---|
| T1 | Debounce the logical queue: `first_queued_at` column (`058`), `_pending(quiet_seconds, max_wait_seconds)`; forget/backfill never wait; worker logs `logical_ready` vs `logical_waiting`; CLI `--quiet-seconds 90 --max-wait-seconds 600`, Render start cmd updated | `logical_evidence_projection.py:50-84,397-437,1485`; `projection_worker.py`; `cli.py:1692-1704,2262-2308`; `server/schema/058_projection_debounce.sql` | `test_worker_reports_waiting_and_ready_pending`; e2e_logical_evidence_projection: quiet=300 → 0 docs / 1 waiting, quiet=0 → 1 doc, forget projects immediately; live: docs re-projected/day falls toward distinct quiet sessions |
| T2 | In-place logical revisions: FKs re-keyed to `(tenant, source, logical_document_id)` with ON DELETE CASCADE (forget still cascades), `UNIQUE(…, policy_fingerprint, ordinal)`; `_commit` does `INSERT … ON CONFLICT DO UPDATE` on `canonical_evidence_documents`; parts/actors replaced per revision; six retrieval join sites updated; constraints dropped by `pg_constraint` lookup, added `NOT VALID` + `VALIDATE`, indexes `CONCURRENTLY` in a separate autocommit file | `logical_evidence_projection.py:1067-1373,1375-1467`; `passage_retrieval.py:455-463,716-720,808,912,1280,1377`; `parquet_scan.py:505-579`; `server/schema/059_stable_projection_keys.sql` (+`059b` concurrent indexes) | e2e_logical_evidence_projection: second ingest keeps the passage_documents row (no cascade), evidence `created_at` unchanged, cleanup queue holds only the replaced tail part; e2e_v2_lifecycle `:219` still passes; staging lock-hold time measured on a restore; card recall@20 equal before/after |
| T3 | Stable passage identity + differential commit: id = sha256(tenant, source, ldoc, policy fingerprint, text_sha256, spans) without revision/count, so a record-window is a stable "turn" for any later append; `_commit` computes to_delete / to_insert / retained, deletes only shifted windows, updates the pointer row in place, COPYs only new passages, re-attaches embeddings for inserts from the temp table; retained rows untouched (no GIN/HNSW churn); worker logs inserted/deleted/retained; CLI `passage-shadow-diff --tenant --source --limit 50` | `passage_projection.py:319-449`; `passage_index.py:276-349,351-575,577`; `projection_worker.py`; `cli.py` | `test_passage_ids_are_stable_under_append`, `test_passage_ids_change_only_after_mid_document_edit`, `test_passage_id_excludes_revision`; e2e_lossless_passages: second ingest → deleted ≤ 1, retained = n−1, retained `embedded_at` unchanged, no new embedding calls; e2e_v2_lifecycle: no passage overlaps forgotten receipts; shadow diff on 50 staging sessions `receipt_set_equal` 100%; live: passages rewritten/day 150k → ≤ 15k, embedded/day unchanged, card `newest_age` falls toward the debounce window |
| T4 | Parquet delta fragments: `canonical_parquet_scan_dirty_documents` + `…_fragment_documents` (`060`); `_build` rewrites only fragments intersecting dirty documents into new `shard_index` parts (32 MiB cap), full rebuild only for `backfill`; periodic compaction; scan contract unchanged (`{dataset}-part-{shard_index:05d}`) | `parquet_scan.py:456,920,985,1194`; `logical_evidence_projection.py:860-890`; `passage_index.py:549-574`; `server/schema/060_parquet_scan_fragments.sql` | `test_delta_rebuild_rewrites_only_intersecting_fragments`, `test_backfill_reason_forces_full_rebuild`, `test_fragment_rows_never_duplicate_a_document`; e2e: sibling session's shard artifact_id unchanged and both parts listed; live: parquet rows/day ≥ 10× lower; card `scope_scan_agreement` 1.0 |
| T5 | Identity-write cache (`_ensure_source_registered`, TTL 600 s, cap 10k, positive results only, tenant invalidation on membership change) so `register_source` runs zero statements on hit; authorization audit: denied synchronous, allowed batched via a daemon thread (2 s / 500 rows, bounded, synchronous fallback, flush on close) | `canonical.py:266-339` + call sites `:210,:395,:1005`; `db.py:940-967`; `app.py:539-556` | `test_denied_audit_is_written_synchronously`, `test_allowed_audit_is_batched_and_flushed`, register_source zero-statement test; e2e_v2_isolation foreign principal still forbidden; e2e_v2_multitenant_mcp flushes before asserting; live: `brain_tenants` inserts and audit inserts/min → ~0 in PlanetScale insights |
| T6 | Retire legacy writes: `CanonicalPlane.ingest_provider_event` for Slack/generic webhooks and `/v1/ingest/batches` (archive raw body + `ingest_document`); `RECALL_LEGACY_WRITES=0` default; `/v1/search|show|related|session-export` → 410 Gone unless `RECALL_LEGACY_READS=1`, tables dropped 30 days later (decided by Miguel 2026-09-11); `/v1/receipts/resolve` stays; droppable tables listed (`sources, source_grants, source_events, items, chunks, entities, item_embeddings, sessions, turn_embedding*, projection_watermarks, projection_backfills, ingest_batches, embedding_projection_watermarks`) | `app.py:1402-1515,1620-1730`; `db.py:1316,1352,2340+`; `webhooks.py`; `canonical.py` | `e2e_webhook_ingest.py` rewritten: rows in canonical_events/raw_artifacts, receipts resolve, replay `duplicate_events=1`, `source_events` count 0; live: `max(created_at)` on `source_events` frozen 7 days → `storage-discard-empty-legacy` |
| T7 | Thinning as steady state: relax the queue gate to exclude only `backfill` (logical projection reads chunks + structural fields, which compaction preserves), batch 5000×2; chunk text NOT thinned in H1 (projection source + liveness predicate; H3 target); after ≥ 2 quiet days: `REINDEX INDEX CONCURRENTLY` passages GIN (2.9 GB), HNSW, pkey, time idx; `VACUUM ANALYZE`; storage runbook | `canonical_thinning.py:116-150`; `cli.py:2301-2306`; `projection_worker.py:109-119`; `docs/architecture/storage-runbook.md` | e2e_canonical_body_thinning: queued `ingest` session still thinned, `backfill` refused; live: `canonical_bodies_refused` 0, inline documents → debounce window only, Postgres total < 40 GB, GIN size after reindex recorded |

Hill gates on the card: passages rewritten/day ≤ 20k; `projection_pending` 0; newest age ≤ 15 min at steady state;
Postgres ≤ 40 GB; cold search p50 ≤ 1 s; forget latency probe ≤ 10 min; shadow parity 100% receipts; recall@20 / MRR
unchanged. Size ≈ 10–12 engineer-days.

Migration numbering across hills: H1 `058–060`, H2-a `061`, H3 `062–069`.

### H2: Retrieval quality (2 weeks) — order H2-d → H2-b → H2-c → H2-a → H2-e

| ID | Deliverable | Files / approach | Exit check |
|---|---|---|---|
| H2-d | `recall_search` limit 50; `candidate_depth`/`result_limit` in diagnostics; sparse-exact arm reads `canonical_passages` (phrase query per identifier token) instead of `canonical_chunks`; note tool-output identifiers now reachable only via `recall_scan` | `mcp.py:122-183,827`; `canonical_retrieval.py:1722`; `passage_retrieval.py:545-668,997,1057-1080`; `evals/systems_card/accuracy.py` `CANDIDATE_LIMIT=50` | `test_sparse_arm_never_reads_canonical_chunks`, `test_search_diagnostics_report_candidate_depth`, conformance accepts 50 rejects 51; card `boundary_recall@50` real; identifier-stratum recall not lower |
| H2-b | Convex min-max fusion per arm (legs with < 3 candidates or recency-mode scores fall back to rank), α from env `RECALL_SEARCH_FUSION_ALPHAS`, RRF behind `RECALL_SEARCH_FUSION=rrf`; tuning script `evals/fusion_tuning.py` grid-searches α on the optimize split, reports validation, content-free private report | `passage_retrieval.py:106-190,1049-1054`; `db.py:122-171`; new `evals/fusion_tuning.py` | 4 fusion unit tests; `tests/test_fusion_tuning.py` rejects in-repo paths and leaks nothing; card: validation MRR ≥ +0.05 with recall@20 not lower; `diagnostics.fusion` captured |
| H2-c | `RerankRuntime` (`rerank.py`, voyage rerank-2.5 / cohere rerank-4, key-file + approved-URL hygiene from `semantic.py`, transport injection); rerank top 50 passages → re-collapse before time clip; skip on low budget; diagnostics `rerank_status/elapsed/candidates/model` and `arm_elapsed_ms.rerank`; scorer adds recall@5/@10 | new `server/recall_server/rerank.py`; `passage_retrieval.py` search(); `db.py:171`, `app.py:1827`, `cli.py:1971`; `evals/agentic_truth.py:508-517,675-684` | `test_rerank_runtime.py` (5 tests: https+approved URL, owner-only key, redirects/oversize, both payload shapes, index bounds); 4 retrieval tests (reorder, skip on budget, failure preserves results, elapsed reported); `test_scores_recall_at_five_and_ten`; card: recall@5 ≥ 0.6, MRR ≥ 0.5, `arm.rerank.p95_ms ≤ 400`, `rerank_skipped_rate ≤ 0.1` |
| H2-a | Deterministic contextual header per passage (source family, aliases, harness, workspace, branch, people, passage times; ≤ 512 B) reusing `passage_representations._metadata_lines`; header is embedding input only (text/spans/receipts/text_sha256 untouched); `header_redacted`, `embed_sha256` columns (`061`); reuse key becomes `embed_sha256`; new `PASSAGE_EMBEDDING_CONTRACT` v2 rolled out with two fingerprints (worker writes v2, server flips at ≥ 99% coverage); one deliberate re-embed with a cost estimate first | `passage_projection.py` (`render_passage_header`), `passage_index.py:444-479,516-517,739-795`, `semantic.py:29,312-324`, `projection_worker.py`, `server/schema/061_passage_embedding_headers.sql` | 3 projection tests (deterministic + excluded from spans/sha; bounded; embed_sha changes, passage_id does not); `test_passage_fingerprint_includes_header_contract`; e2e lossless_passages asserts headers + full coverage after flip; card: no regression, gains in person/repo/time strata; embed calls/day back to baseline after the one pass |
| H2-e | Card gates: recall@20 ≥ 0.75, MRR ≥ 0.5, recall@5 ≥ 0.6, warm search p95 ≤ 1 s, server p95 ≤ 1 s; card records fusion mode/α, rerank model, candidate depth, passage fingerprint from first-search diagnostics | `evals/systems_card/accuracy.py:132-137`, `latency.py:134,225`, `evals/README.md` | one green card with the new gates; test split scored once, recorded, untouched otherwise |

### H3: Search plane on S3 (4–6 weeks) — Lance, in-process reads with a local cache (decided by Miguel 2026-09-11; turbopuffer stays the fallback behind the same outbox)

| ID | Deliverable | Files / approach | Exit check |
|---|---|---|---|
| H3-a | Outbox: `search_projection_outbox` (tenant, source, month, generation, reason ∈ backfill/logical-update/forget/header-change), `search_projection_tombstones` (per passage), `search_projection_shards` catalog; written by `passage_index._commit` and `canonical.forget`; seeded from parquet shard months | `server/schema/062_search_projection_outbox.sql`; `passage_index.py:549-574`; `canonical.py:1584-1626` | `test_search_outbox.py`: commit enqueues every spanned month; forget writes tombstones + outbox row; generation increments |
| H3-b | Lance writer `lance_projection.py` (`CanonicalLanceProjector`) in the projection worker: one dataset per tenant-month on S3 (`RECALL_SEARCH_PLANE_PREFIX`, same archive credentials); schema = parquet passages + header, spans, content_sha256, embedding_fingerprint, vector float16[512], deleted, generation; append fragments, delete tombstoned/superseded rows, hash reuse skips existing rows; IVF_PQ vector index + INVERTED (BM25) text index + btree on source/time; re-index when unindexed > 10%; nightly `compact_files` + `cleanup_old_versions(7d)`; advisory-locked writer per tenant-month; `pylance` added to requirements | new `server/recall_server/lance_projection.py`; `projection_worker.py`; `server/requirements.txt` | `e2e_lance_projection.py` (MinIO): project → readable; re-run → zero new fragments; tombstone → absent under `deleted=false`; compaction reduces fragments. Unit: Arrow schema equality; halfvec→float16 round-trip |
| H3-c | `LanceHintRetrieval` with the same `search()` signature as `PassageHintRetrieval`: catalog pre-filter from Postgres (sources, actors, months), per-month concurrent vector k=400 + FTS + identifier-phrase arms with `deleted=false`/source/time predicates, rows mapped to today's leg row dicts, then the same convex fusion, reranker, and time clip; document fields joined from `canonical_evidence_documents` in one query; `RECALL_SEARCH_PLANE=postgres|lance|shadow`; shadow logs content-free rank agreement + elapsed; warm-on-boot last 3 months per active tenant, background touch every 10 min; `RECALL_LANCE_CACHE_DIR` size-capped | new `server/recall_server/lance_retrieval.py`; `canonical_retrieval.py:1807` selection; `db.py` | `test_lance_retrieval.py` on a temp-dir dataset: identical result/matching_ranges keys, tombstone absent, predicates applied, deadline skips months; conformance output schema unchanged |
| H3-d | Postgres → catalog, each step after ≥ 14 days green in `lance` mode: `063` drop passage embeddings; `064` drop passages `search_vector`/GIN and `text_redacted` (keep ids, hashes, receipts, spans, times, roles, actors); `065` drop representation tables + `search_representation`; `066` chunks → `canonical_receipts` (drop bodies + GIN); `067` legacy plane + `_legacy_chunk_search_for_eval` + `_entity_leg` | `server/schema/063–067_*.sql`; deletions across `db.py`, `passage_retrieval.py:1179`, `canonical_retrieval.py` | `e2e_storage_legacy_cut.py` extended; CI grep gate `test_storage_cut.py` (no `text_redacted` reads outside allowed modules after `064`/`066`); card `cost.storage` Postgres ≤ 10 GB |
| H3-e | `show`, `session_context`, `related`, time clip served from the receipt map + evidence parts (`logical_evidence.read_part` + `decode_logical_record`, LRU part cache `RECALL_EVIDENCE_PART_CACHE_MB`) and Lance for text; no `canonical_chunks` on any read path | `canonical_retrieval.py:1887-1919,2879-2907,2969-3044,4012-4086` | `TimeClipWindowTests` assert no chunk SQL; `e2e_logical_evidence_projection.py` byte-identical `show` text before/after; `recall_show.p95_ms ≤ 2000` |
| H3-f | Hill gates: shadow comparison on validation (Lance recall@20 ≥ Postgres, MRR ≥ −0.02); new `latency.search_cold` probe (cache cleared via admin endpoint) p95 ≤ 2 s; warm ≤ 1 s; Postgres ≤ 10 GB; `e2e_forget_lance.py` (forget → absent within 2 worker intervals; `show` null); cost per seat reported | `evals/systems_card/*`, `server/tests/e2e_forget_lance.py` | all listed gates green on one card; history row cites `engine=lossless-passages-lance-v1` |

### H4: Memory layer (3 weeks)

| ID | Deliverable | Files / approach | Exit check |
|---|---|---|---|
| H4-1 | Per-session recap at session close (quiet ≥ 30 min or explicit end): what, why, outcome, files touched, actors, time span, key receipts; produced by a cheap model through the machine's LiteLLM broker (never a provider key); stored as a Lance `recaps` dataset + catalog row; indexed (vector + FTS) beside turns; `recall_search` results carry `recap_id` | new `recap_projection.py`, outbox reason `recap`, `lance_projection.py`, `mcp.py` | unit: recap schema + receipt validation (every cited receipt resolves); e2e: closed session → recap present within 2 cycles; card: recap coverage ≥ 95% of closed sessions; content-free |
| H4-2 | Dated decision facts: extractor emits `{decision, rationale, valid_at, receipts, superseded_by}` from recaps; verbatim text stays primary; `recall_decisions` MCP tool (query + time window) | new `decision_projection.py`, Lance `decisions` dataset, `mcp.py` | unit: supersession chain; truth strata added for "what did we decide about X"; judge-scored answer quality on validation ≥ baseline |
| H4-3 | Identity wiring: Slack and git identities into `brain_actors`/`brain_actor_external_identities`; recaps and decisions carry `actor_ids` | `actor_attribution.py`, identity CLI | `e2e_employee_attribution.py` extended; `recall_people` shows merged identities |
| H4-4 | Card: new truth strata (decision, who-worked-on, how-fixed) + LLM-judge probe through the broker; gates set after one baseline run | `evals/systems_card/accuracy.py`, `evals/agentic_truth.py` | baseline recorded; no raw-recall regression |

### H5: Operate it (starts in H0, ongoing)

| ID | Deliverable | Exit check |
|---|---|---|
| H5-1 | Split Render services: `recall-ingest` (collector routes) and `recall-mcp` (MCP + admin); shared image, separate pools; collectors pointed at the ingest host | zero MCP health-check restarts under ingest load; card availability green during a collector backfill |
| H5-2 | Worker sizing: evidence, passage, lance, embedding as separate workers with their own pools; `RECALL_DATABASE_POOL_MAX_SIZE` ≥ 8 each | projection_pending 0 at steady state |
| H5-3 | Embedding guardrails: daily embed cap + alert; reuse dry-run report required before any corpus rewrite (`cli reembed-plan`) | probe `freshness.embedding_lag` green; cap alert tested |
| H5-4 | SLOs on the card: warm search p95 1 s, cold 2 s, freshness 15 min, leaks 0, cost/seat; nightly Slack summary | 14 consecutive green nights before H3 cutover and again before closing the cascade |
| H5-5 | PlanetScale: budget alert set; back to PS-80 or Metal after H3-d; pg_prewarm/autoprewarm on | `cost.planetscale` gates green |

## Cascade mechanics

- Living plan: `~/worktrees/recall-rewrite-20260911/.cascade/recall-rewrite.md` mirrors this document with status/evidence columns (`todo/doing/done/blocked`), updated every loop with the card row and PR that proved it.
- Branch per task (`feat|perf|fix/recall-<hill>-<task>-<date>`), PR per task, squash merge, `ship_and_measure` after each deploy. Migrations forward-only, numbered from `058`.
- Parallelism: independent tasks (e.g. H0-1..H0-3; H2-b and H2-c; H3-b and H3-c after H3-a) go to subagents with owned files and a stated exit check; the lead integrates and owns the plan.
- Failure policy: a red gate leaves the task unfinished; two attempts without new information means change approach; a production regression (as with #483) is reverted first, understood second.
- Cutovers (H1 projector, H3 search plane) always run shadow → flag → 14 green days → drop.
- Money and prod-config writes (PlanetScale tier/alerts, Render service creation, embedding re-runs) get an AskUserQuestion at the moment of the write.

## Verification summary

Per loop: unit + fresh-Postgres e2e in CI; shadow parity where a projection or plane changes; live card run after deploy, diffed against the previous history row; plan updated with the evidence. Per hill: the hill's gate table green on one card. End: the final-acceptance table green, 14 nights in a row, test split scored once.
