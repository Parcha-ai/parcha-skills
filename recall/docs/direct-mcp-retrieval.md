# Direct MCP retrieval

Recall is an evidence service for an agent, not an agent hidden behind an agent.
The Codex, Claude Code, or other MCP client already interpreting the user's
question owns planning, query reformulation, evidence gathering, and synthesis.
The hosted Recall service owns authorization and access to private data.

## Interface

- `recall_search` returns authorized high-recall pointers, exact receipts, and
  logical document IDs. Embeddings help the caller find where an answer could be;
  a hit is not proof. Lexical and semantic search share one wall-clock budget and
  run concurrently; a successful leg is preserved when the other leg times out.
- `recall_scope` enumerates exact, content-free document boundaries for person,
  source, and time constraints. It does not spend the search budget embedding a
  question whose universe is already known. Page until `complete` is true.
- `recall_scan` runs one caller-authored DuckDB/shell program over authorized
  source/month Parquet. Its compact passage rows are the default planning surface:
  bounded visible-message text, time, attribution, logical document IDs, and
  receipt pointers. The output is capped at 16 KiB and passage pointers are hints,
  not opened evidence.
- `recall_show` and `recall_session_context` open exact receipt-backed context.
- `recall_exec` mounts only explicitly selected full documents into a bounded,
  read-only, networkless sandbox. The caller can use `recall-scan`, shell, Python,
  `jq`, and ordinary text tools to inspect them.
- `recall_exec_map` applies one caller-authored program concurrently to bounded
  shards of as many as 80 authorized full documents. Each shard is independent;
  the MCP client agent reads the outputs, changes its search program if needed,
  and performs the semantic reduction itself.
- `opened_receipts` is the citation boundary. Recall reauthorizes every requested
  document and verifies every receipt returned by execution.

The caller can search narrowly, broaden, decompose a time range, sample, or inspect
whole documents according to the question. Recall does not impose a deterministic
retrieval plan and does not need a model-provider credential.

## Broad-question workflow

For a question such as “What did Alice work on yesterday?”, the client should:

1. Resolve the people or source boundary when the question supplies one, then call
   `recall_scan` once with exact UTC bounds and a compact DuckDB program over
   `passages-part-*.parquet`.
2. Deduplicate passage IDs across adjacent month buckets and return a bounded set
   of candidate logical document IDs. Use raw record shards only when required
   tool output or exact low-level details cannot be present in visible passages.
3. Treat `complete=false` or `projection_pending>0` as partial planning, never as
   proof that the missing work does not exist.
4. Call `recall_exec_map` on the selected document IDs. Use small shards for large
   documents or heterogeneous sessions, and more parallel workers when breadth is
   the bottleneck.
5. Inspect the shard outputs, run a second targeted program when evidence is thin,
   and synthesize only from verified `opened_receipts`.

This is agentic map/reduce without a nested model: exact metadata supplies recall,
embeddings prioritize ambiguous concepts, Archil supplies parallel full-document
inspection, and the already-present MCP client agent owns judgment.

## Trust boundary

The MCP credential determines the tenant, principal, roles, and allowed sources.
Tool arguments can only narrow that authority. Storage credentials never cross the
service boundary; the execution sandbox has no network and cannot mutate evidence.
Search snippets remain hints until the caller opens supporting records.

## Why the nested agent was removed

A hosted Pi worker duplicated the reasoning agent already present in every target
client. It added another prompt, model failure mode, queue, lifecycle API, model
credential, and roughly half the container image while making the outer agent wait
for an opaque investigation. The direct interface keeps one semantic agent and one
small evidence plane.
