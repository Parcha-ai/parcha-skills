# Retrieval over agent transcripts / chat logs / engineering history — state of the art, 2025–26

Scope note: almost nothing in the literature evaluates on *agent transcripts* specifically. The closest proxies are long-conversation memory benchmarks (LongMemEval, LoCoMo, LongMemEval-V2), code retrieval, and "tables + text" enterprise corpora. I flag where I am extrapolating.

## 1. Hybrid retrieval: BM25 + dense + reranking

- **Hybrid beats either alone; the reranker is the biggest single lever.** On a 2026 text-and-table RAG benchmark: Recall@5 BM25 0.644, dense (text-embedding-3-large) 0.587, hybrid RRF 0.695, hybrid + Cohere Rerank v4.0 Pro (50→10) 0.816; nDCG@10 0.515 / 0.466 / 0.551 / 0.683. BM25 beat dense on nearly every metric on that corpus. https://arxiv.org/html/2604.01733v1
- **Anthropic's contextual-retrieval study** (codebases, fiction, arXiv, science): top-20 retrieval failure rate 5.7% → 3.7% (contextual embeddings), → 2.9% (+ contextual BM25), → 1.9% (+ reranking) — i.e. hybrid + rerank cuts misses 67%. Top-20 beat top-10/top-5 for the downstream model. https://www.anthropic.com/news/contextual-retrieval
- **RRF vs learned fusion.** The TOIS analysis of fusion functions found RRF is sensitive to its k parameter, while a convex combination of normalized scores (tune one α) beats RRF in- and out-of-domain and is insensitive to the normalization choice; ~40 labeled query-doc pairs suffice to tune α. https://dl.acm.org/doi/10.1145/3596512 — corroborated on a 2025 benchmark where CC α=0.5 Recall@5 0.726 vs RRF 0.716. https://ceur-ws.org/Vol-4173/T3-7.pdf. Weaviate defaulted to relative-score (min-max) fusion over RRF for the same reason. https://weaviate.io/blog/hybrid-search-explained
- **Rerankers, 2025–26:**
  - Voyage rerank-2.5 / 2.5-lite (Aug 2025): +7.94% / +7.16% over Cohere Rerank 3.5 across 93 datasets, +12.7% on MAIR instruction-following; 32K context; first-stage evaluated on BM25, OpenAI v3-large, voyage-3.x. https://blog.voyageai.com/2025/08/11/rerank-2-5/
  - Cohere Rerank 4 (Dec 2025): Pro and Fast variants, 32K context (8× 3.5), claims to beat Voyage and Jina on BEIR + internal long-doc/PDF/semi-structured sets. Cohere publishes no nDCG figures on the launch page or docs I could reach; pricing $0.0025/search Pro, $0.002 Fast, $0.001 v3.5 — billed per search, and over-length docs are chunked and billed per chunk. https://cohere.com/blog/rerank-4 https://openrouter.ai/cohere/rerank-4-pro
  - jina-reranker-v3 (0.6B, listwise "last but not late", 131K context, 64 docs per pass): BEIR nDCG@10 61.94 vs Qwen3-Reranker-4B 61.16, mxbai-rerank-large-v2 61.44, bge-reranker-v2-m3 56.51. License CC BY-NC. https://jina.ai/news/jina-reranker-v3-0-6b-listwise-reranker-for-sota-multilingual-retrieval/
  - bge-reranker-v2-m3 remains the permissive-license default; Qwen3-Reranker-4B is ~1 s/query. https://aimultiple.com/rerankers
  - Late interaction: GTE-ModernColBERT handles 8K-token docs and beat ColBERT-small on BEIR; LateOn beats Jina-ColBERT-v2 at 1/4 the size. https://lighton.ai/lighton-blogs/lighton-releases-gte-moderncolbert-first-state-of-the-art-late-interaction-model-trained-on-pylate https://huggingface.co/lightonai/LateOn
  - Reasoning rerankers (Rank1, ReasonRank): ReasonRank-32B reaches 40.6 on BRIGHT (SOTA), 7B runs 0.25–0.5 s per 100 passages, 2–2.7× faster than pointwise Rank1. Only worthwhile on reasoning-heavy queries; LLM reranking generally adds 4–6 s vs cross-encoders. https://arxiv.org/abs/2508.07050 https://zeroentropy.dev/articles/ultimate-guide-to-choosing-the-best-reranking-model-in-2025/
- **Conversational/long-document recall numbers specifically:** none of the reranker vendors break out "conversation" as a domain with figures; Voyage lists it as one of eight eval domains without per-domain numbers. Uncertain.

## 2. Learned sparse vs BM25 for identifier-heavy text

- **General corpora:** ELSER beat BM25 on 10/12 BEIR sets, avg +17% nDCG@10; OpenSearch neural sparse +12.7% (doc-only) to +20% (bi-encoder) over BM25. https://www.elastic.co/search-labs/blog/articles/may-2023-launch-information-retrieval-elasticsearch-ai-model https://opensearch.org/blog/improving-document-retrieval-with-sparse-semantic-encoders/
- **Code:** "On the Challenges and Opportunities of LSR for Code" (2026) introduces SPLADE-Code: 75.4 on MTEB-Code (<1B params, SOTA at that size), 79.0 at 8B; sub-ms retrieval on 1M passages. It names the exact failure mode for your corpus: sub-word fragmentation of identifiers and query/code vocabulary misalignment — learned expansion tokens are what closes the gap. https://arxiv.org/abs/2603.22008
- **Tokenization is the weak spot.** LSR is "particularly sensitive to tokenization in specialized domains" where WordPiece over-fragments technical terms; ModernBERT lags BERT in sparse retrieval for this reason. https://arxiv.org/html/2607.00004v1
- BGE-M3 sparse beats BM25 on MIRACL in all languages and beats its own dense head by ~10 points on long docs (MLDR), with 8192-token input. https://arxiv.org/html/2402.03216v3
- **Log/transcript evidence:** none peer-reviewed. Practitioner reports say BM25 still wins on error strings, IPs, IDs, and that hybrid gains are 10–30% on such data. https://tianpan.co/blog/2026/04/12/hybrid-search-production-bm25-dense-embeddings. Treat as anecdotal. For paths, hashes, PR numbers and stack frames, keep BM25 with a tokenizer that preserves the identifier; add learned sparse only if you can fine-tune (SPLADE-Code style).

## 3. Chunking / passage strategies for long conversations

- **Round (turn) granularity beats session granularity** for chat memory: LongMemEval found decomposing sessions into rounds "significantly enhances" QA with GPT-4o; fact-augmented key expansion (index extracted facts alongside the raw text) gave +9.4% Recall@k and +5.4% accuracy; time-aware query expansion +11.3% (round-level) on temporal questions. https://arxiv.org/html/2410.10813
- **Keep verbatim text; don't replace it with extracted facts.** Controlled ablation (2026): verbatim chunks 43.9% vs extracted artifacts 28.0% on LoCoMo; 67.4% vs 45.4% on LongMemEval-S. Structured artifacts help only as a supplement. https://arxiv.org/abs/2601.00821
- **Contextual retrieval** (prepend an LLM-written 50–100 token chunk context): the 35–67% failure-rate reductions above; one-time cost ~$1.02 per 1M doc tokens with prompt caching. https://www.anthropic.com/news/contextual-retrieval
- **Late chunking** (embed whole doc, pool per chunk): ~2.7–3.6% relative nDCG gain over naive chunking; cheaper than contextual retrieval but smaller effect and model-dependent. https://arxiv.org/pdf/2409.04701 https://arxiv.org/abs/2504.19754
- **Vendor-side contextualized embeddings:** voyage-context-4 (Jun 2026) auto-chunks, handles >32K docs, +2.08% chunk-level over context-3, +8.4 nDCG points over cohere-embed-v4 on chunk retrieval, $0.12/M. https://blog.voyageai.com/2026/06/29/voyage-context-4/
- **Hierarchical / parent-document:** HiChunk + auto-merge lifted evidence recall 74.06 → 81.03 over fixed 200-token chunks on evidence-dense QA. https://arxiv.org/html/2509.11552v2. Proposition indexing showed no consistent recall win over semantic chunking in a 2025 clinical comparison (0.71 vs 0.75). https://www.ncbi.nlm.nih.gov/pmc/articles/PMC12649634/

## 4. Query side and "the agent is the retriever"

- **Multi-query / HyDE:** Multi-HyDE (several non-equivalent hypothetical docs) +11.2% accuracy, −15% hallucination in a financial RAG agent. https://arxiv.org/html/2509.16369v1. Gains are real but modest relative to reranking.
- **Agentic keyword search ≈ RAG:** Amazon (AAAI 2026) — a ReAct agent with `rga`/`pdfgrep`/metadata tools reached >90% of vector-RAG metrics with no vector store, same LLM and datasets. https://arxiv.org/abs/2602.23368
- **Coding-agent-as-memory-module beats vector RAG on agent trajectories:** LongMemEval-V2's AgentRunbook-C stores raw trajectories as files and lets a coding agent search/inspect them with a manifest and helper scripts: 74.9% / 70.1% (small/medium) vs RAG-pipeline AgentRunbook-R 58.6% / 57.0%, vs naive slice RAG 42.8% / 38.1%; an unscaffolded Codex agent got 69.9%. https://arxiv.org/html/2605.12493
- **Claude Code dropped its vector index in 2025** for grep/glob/read agentic search ("outperformed everything, by a lot" — Cherny). https://vadim.blog/claude-code-no-indexing/ (secondary source; uncertain on details.)
- **Code mode:** Anthropic's code-execution-with-MCP (filesystem of tool stubs, filter data in the sandbox) 150K → 2K tokens (98.7%) on one workflow; Cloudflare Code Mode: two tools (`search`, `execute`) expose 2,500 endpoints in ~1K tokens vs 1.17M. Independent replications 58–92% depending on tool count, ~7% latency cost. https://www.anthropic.com/engineering/code-execution-with-mcp https://blog.cloudflare.com/code-mode-mcp/ https://particula.tech/blog/code-execution-mcp-token-reduction-pattern
- **Deep-research lessons:** Anthropic's orchestrator + 3–5 parallel subagents beat single Opus 4 by 90.2% at ~15× tokens; token spend explains 80% of BrowseComp variance; "start broad, then narrow." https://www.anthropic.com/engineering/multi-agent-research-system. LRAT (SIGIR 2026) mines 26,482 agent trajectories for retriever training signal (browse/reject/reasoning), improving evidence recall across agents. https://arxiv.org/abs/2604.04949

## 5. Embedding landscape 2026

| Model | Dims (MRL) | Context | $/1M tok | Notes |
|---|---|---|---|---|
| voyage-4-large / 4 / 4-lite / 4-nano (open) | 2048/1024/512/256; fp32/int8/binary | 32K | 0.12 / 0.06 / 0.02 / free | Shared space across sizes (embed docs with lite, queries with large); +14.05% vs OpenAI v3-large, +8.2% vs gemini-001, +3.87% vs Cohere v4 on RTEB. https://blog.voyageai.com/2026/01/15/voyage-4/ https://docs.voyageai.com/docs/pricing |
| voyage-code-4 (Aug 2026) | same | 32K | 0.12 | +27.5% vs code-3, +48.6% vs OpenAI on an "agentic code retrieval" benchmark; explicitly targets symptom-style queries from coding agents. https://blog.voyageai.com/2026/08/13/voyage-code-4/ |
| voyage-context-4 | same | >32K (auto-split) | 0.12 | Contextualized chunk embeddings; see §3. |
| OpenAI text-embedding-3-large / small | 3072 / 1536 (MRL) | 8K | 0.13 / 0.02 | No refresh since Jan 2024; now trails open models by 7–10 MTEB points. https://developers.openai.com/api/docs/guides/embeddings |
| Gemini Embedding 2 (preview, Mar 2026) | 3072→768 (MRL) | 8K text (multimodal) | 0.20 | MTEB-English 68.32; 128K claim in some blogs conflicts with 8,192 in others — verify. https://developers.googleblog.com/gemini-embedding-available-gemini-api/ |
| Cohere embed-v4 | 1536/1024/512/256 | 128K | 0.12 | Multimodal; useful for whole-doc embedding without chunking. https://docs.cohere.com/changelog/embed-multimodal-v4 |
| Qwen3-Embedding 0.6B/4B/8B (Apache-2) | 1024/2560/4096, MRL 32–4096 | 32K | self-host | MTEB-multilingual 70.58 (8B). https://huggingface.co/Qwen/Qwen3-Embedding-8B |
| jina-embeddings-v4 (3.8B) | 2048→128 | 32K | self-host/API | Code LoRA: CoIR 71.59. https://jina.ai/news/jina-embeddings-v4-universal-embeddings-for-multimodal-multilingual-retrieval/ |
| NV-Embed-v2 | 4096 | 32K | self-host, non-commercial | Older (2024) leaderboard leader. https://arxiv.org/pdf/2405.17428 |

No vendor publishes results on chat-log or agent-transcript text; code numbers are the nearest proxy.

## Implications for a transcript brain on Postgres + pgvector

1. **Index rounds (one user turn + one assistant turn, with tool calls attached), not whole sessions**, and keep the verbatim text; store LLM-extracted facts/decisions as *additional* indexed keys, never as replacements (LongMemEval +9.4% recall; verbatim-vs-artifacts −16 to −22 points when substituted).
2. **Hybrid is mandatory**: BM25 (pg_search/ParadeDB, VectorChord-BM25, or pg_textsearch) with an identifier-preserving tokenizer for paths/IDs/hashes, plus pgvector. Fuse with min-max convex combination and tune α on ~40–100 labeled queries rather than defaulting to RRF.
3. **Rerank the top 50 → 20** with a 32K-context cross-encoder (rerank-2.5, Rerank 4, or self-hosted bge/jina); it is the largest measured gain (+12pp Recall@5). Reserve reasoning rerankers for hard queries only.
4. **Prepend contextual headers** (session goal, repo, date, who) to each chunk before embedding — cheap (~$1/M tokens) and it fixes the "this chunk means nothing alone" problem transcripts have.
5. **Embedding**: voyage-4 family (shared space lets you embed at lite and query at large) or self-hosted Qwen3-Embedding-4B at 1024 dims to keep pgvector HNSW small; voyage-code-4 if turns are mostly code/diffs. Hold OpenAI v3.
6. **Expose search + exec (SQL/DuckDB over Parquet) as the two primary MCP tools** and let the coding agent iterate: the LongMemEval-V2 and Amazon results say a scaffolded agent over raw files/keyword search matches or beats a fixed RAG pipeline, at higher token cost. Budget for that spend; it explained most of the variance in Anthropic's data.