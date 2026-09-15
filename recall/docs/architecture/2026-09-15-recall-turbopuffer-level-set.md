# Recall architecture level-set: what turbopuffer enables, and what is actually left

**Date:** 2026-09-15 (written during the turbopuffer backfill, before cutover). **For:** Miguel. **Purpose:** decide what the simplest Recall looks like now that the search plane lives on turbopuffer, and where the PlanetScale bill really comes from. Nothing in this document has been changed in production; it is research and a proposal.

**Provenance.** Postgres sizes: `pg_total_relation_size` on the live `recall-brain` database via a temporary read-only role, 2026-09-15 16:2x UTC. turbopuffer capabilities: turbopuffer.com/docs (write, query, fts, embedding, limits, performance) fetched 2026-09-15. Prices: turbopuffer.com/pricing and planetscale.com/pricing fetched 2026-09-15. Card numbers: `~/.recall/systems-card/out/history.jsonl`.

## 1. Where the bytes are (measured)

| table | total | heap | indexes | rows |
|---|---|---|---|---|
| canonical_chunks | 92 GB | 18 GB | 25 GB (+49 GB TOAST bodies) | 13.6 M |
| canonical_events | 33 GB | 11 GB | 14 GB | 14.3 M |
| canonical_documents | 18 GB | 6.8 GB | 7.6 GB | 13.0 M |
| canonical_passages | 7.3 GB | 0.6 GB | 0.9 GB (+TOAST text and tsvector) | 409 k |
| canonical_audit_events | 5.2 GB | 3.2 GB | 2.0 GB | 13.7 M |
| canonical_ingest_jobs | 4.6 GB | 2.4 GB | 2.2 GB | 13.8 M |
| raw_artifacts | 1.5 GB | | | 1.7 M |
| canonical_passage_embeddings | 1.2 GB | 0.5 GB | 0.7 GB HNSW | 408 k |
| everything else | < 2 GB | | | |
| **database** | **164 GB** | | | |

Two facts follow.

- **The vector plane is 5 GB of 164.** Migration 067 (embeddings + HNSW + the passages tsvector and its GIN) is right to do, but it does not move the PlanetScale tier. The tier is bound by RAM and IO for the working set (PS-160 = 16 GB RAM in front of 164 GB), not by the vector plane.
- **143 GB is the canonical ledger in Postgres**: chunk bodies with their tsvector (92), the event ledger (33), the document ledger (18). Every byte of that content already exists on S3 twice: as the raw artifact and inside the evidence parts the logical projection writes. Postgres holds a third copy plus indexes on it.

Add 10 GB of audit and ingest-job rows that nothing reads after a few days.

## 2. What turbopuffer actually is, for us

We adopted it as "the vector store". It is more than that, and several of its features map onto things we built by hand this week.

| capability | what it replaces or enables |
|---|---|
| Native embeddings (`voyage/voyage-4`, $0.06/M tokens) | The embedding worker, the ledger and daily cap, the v1/v2 contract flip, the query-time Voyage call, and every per-clause embedding call in H2-m. Done. |
| BM25 over the verbatim text (word_v4, no stemming) | The Postgres tsvector GIN and the min-should-match lexical plan (H2-k). BM25 is OR-scored by nature: a passage that has more of the query's terms ranks higher without any of the conjunction machinery. |
| `ContainsAllTokens` / `ContainsTokenSequence` filters | The identifier arm (`#6076`, `expert_skills`) and exact phrases, as filters on the same query. |
| `compute_attributes` on a query (BM25 score, vector distance, `Highlight`) | One round trip can return ANN rows *with* their BM25 score and dense rows' lexical signal. Our three-arm, three-connection search becomes one `multi_query` (up to 16 sub-queries: arms, temporal window, clause passes) with fusion still ours. |
| `Highlight` (beta): query-ranked fragments by sentence or paragraph, with offsets | Exactly the problem that cost us two regressions (H2-i, H2-o): which 2,000 characters of a 12 kB passage the reranker should read. The service can hand back the best fragments for the query; the reranker reads those, agents get snippets that match. |
| `branch_from_namespace` ($0.032, instant copy-on-write clone) | Eval branches: try a different embedding model, header, or tokenizer on a branch, run the card against it, delete it. Today an experiment means a re-embed of the corpus. |
| `patch_rows` / `patch_by_filter` (non-vector attributes, no re-embedding) | Actor attribution repairs, header changes, retention flags: patched in place without touching the text or paying for embeddings. |
| `delete_by_filter` | Forget a whole source or session in one call instead of per-passage tombstones. |
| Export by id-pagination (10k per page), `copy_from_namespace` | Rebuild or migrate a namespace without Postgres; a namespace is a projection and is disposable. |
| Pinning / eventual consistency | Warm cache for the tenant namespace; cheaper reads for probes and the card. |

What it is not: it holds no receipts, no authorization, no evidence bodies beyond the passage text, and it does not rerank (client-side reranker recommended, which is what we do with Voyage rerank-2.5). It is a projection, rebuildable from the outbox seed in a few hours for about $50.

## 3. The simplest shape

Today Recall runs seven stores and five processes for one job. The shape below keeps three stores and three processes.

**Stores**

1. **S3 is the authority.** Raw artifacts, evidence parts (verbatim records with receipts), parquet fragments for code mode. Unchanged.
2. **turbopuffer is the whole search plane.** One namespace per tenant, one document per passage with text, BM25, native vector, scope attributes. Later, recaps and decisions (H4) are more documents in the same namespace with a `kind` attribute. No second index anywhere.
3. **Postgres is a catalog, target under 15 GB:** tenants, principals, grants and audit of denials; sources and profiles; actors and identities; the evidence document catalog (30 k rows) and part index; a slim passage catalog (ids, hashes, spans, receipts, times, no text, no tsvector, no vectors) for the differential projection; the outbox and shard catalog; a compact ingest ledger for idempotency (tenant, source, native id, digest, pointer). No bodies.

**Processes**

1. **Ingest** (collectors → `/v2/archive/objects` → `/v2/ingest/canonical`): writes the raw artifact and the ledger row. Bodies stop landing in `canonical_chunks`; the logical projection reads the raw artifact from S3 (it already verifies the digest on read).
2. **Projection worker**: logical evidence → passages → turbopuffer (outbox drain) and parquet fragments, from the same passage rows. One writer, two sinks. The embedding worker is gone.
3. **MCP** (plus the managed connector worker for pull connectors): `recall_search` = one turbopuffer `multi_query` → our fusion, nomination, temporal and source hints → Voyage rerank over `Highlight` fragments → results. `recall_show` and `recall_session_context` read evidence parts from S3. `recall_exec` / `recall_scan` run DuckDB over parquet in the Archil sandbox. That last part is the product's edge (the caller is the retriever) and stays.

What leaves: the embedding worker service, the Postgres vector and tsvector planes, chunk bodies in Postgres, the audit and job rows beyond a retention window, the three-connection search, and the per-clause embedding calls.

## 4. What that does to the PlanetScale bill

PlanetScale Postgres (HA, ARM): PS-160 $286, PS-80 $148, PS-40 $83, PS-20 $50 per month; storage billed separately. September month-to-date at PS-160 was $318.

| step | Postgres after | tier that fits | note |
|---|---|---|---|
| today | 164 GB | PS-160 | 16 GB RAM cannot cache a 164 GB working set; that is why searches needed the ef_search and index work |
| migration 067 (vector plane) | ~159 GB | PS-160 | already coded (#562); do it, but it is not the lever |
| audit + ingest-job retention (30 days) | ~149 GB | PS-160 | one cron, zero risk |
| chunk bodies out of Postgres (show/session_context from S3 parts; passage projection reads raw artifacts) | ~60 GB | PS-80 | the old H3-e task; biggest single cut |
| event and document ledger compaction (drop the unused indexes, keep dedupe keys and pointers) | ~20–25 GB | PS-40 | needs a measured index-usage pass first |

Realistic landing: **PS-40 at ~$83–99/month, from $286–349**, plus the embedding worker service gone and no Voyage embedding bill. turbopuffer adds ~$16–75/month. Net saving is in the $200/month range and, more importantly, the search path no longer depends on Postgres cache behaviour at all.

## 5. What changes in the search path itself (after cutover, measured by the card)

1. One `multi_query` per search: ANN with `Embed(query)`, BM25(lexical), BM25(identifiers) filtered by `ContainsAllTokens`, the temporal window pass, and the clause passes as sub-queries; `compute_attributes` adds BM25 to dense rows. Same fusion and nomination on top. Expect server p95 to fall from ~650 ms to the low hundreds, and the pool-admission code to become unnecessary.
2. `Highlight` fragments as reranker input and as the snippets agents see. This is the experiment that H2-i/H2-o could not run safely because our window heuristics were blind to the query; the service ranks fragments by the query itself.
3. Namespace branches for every card experiment (embedding model, header, tokenizer). One branch, one card, delete.

## 6. Risks and their bounds

- **Vendor dependency.** The namespace is a projection: `search-outbox-seed` + drain rebuilds it (about 9 h at the org rate limit, about $50). Keep that runbook exercised.
- **Privacy.** turbopuffer stores passage text (Voyage already saw it; S3 already holds it). Same trust boundary as the S3 bucket, one more vendor with a copy. Scale plan adds BAA/SSO/audit logs if that ever matters.
- **Rate limits.** 2M tokens/min per org for native embeddings; the drain paces itself at 1.8M. Query-time embedding is ~50 tokens per search.
- **Relevance semantics.** BM25 OR-scoring replaces our AND/min-should-match; measured at cutover by the card, with `RECALL_SEARCH_PLANE=postgres` as the rollback until 067 runs.
- **Beta features.** `Highlight` is marked beta; use it behind the card like everything else.

## 7. Recommended order (proposal, nothing started)

1. Cutover and card (in flight tonight). 2. Migration 067 + suspend the embedding worker. 3. Retention cron for audit and ingest-job rows. 4. Chunk bodies out of Postgres: `show`/`session_context` from S3 parts, passage projection from raw artifacts, drop `canonical_chunks` bodies and GIN. 5. Ledger compaction after an index-usage measurement. 6. PS-40. 7. Single `multi_query` search + `Highlight` reranker input, measured. 8. H4 recaps as documents in the same namespace.

Steps 2–6 are the PlanetScale cut; steps 7–8 are the "simpler and better" search.
