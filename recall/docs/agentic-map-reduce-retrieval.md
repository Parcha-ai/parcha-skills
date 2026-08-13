# Agentic map-reduce retrieval

> Historical research note. The hosted nested-agent prototype described below
> was retired in August 2026. Production Recall is model-free: the MCP caller's
> agent plans and reduces with `recall_search`, `recall_show`, and `recall_exec`.

## Objective

Answer natural questions from a large private archive without treating vector
similarity as evidence or giving an agent an unbounded corpus.

Recall uses three layers:

1. **Scope:** the answer agent classifies the question and proposes source,
   occurred-at, session, and topic constraints. The host intersects them with
   the authenticated source grant. Explicit caller constraints cannot be
   widened.
2. **Map:** hybrid retrieval routes each subquestion to a bounded list of
   evidence objects. Independent maps run concurrently against read-only
   Archil mounts. Each map returns exact Recall receipts, coverage, and
   uncertainty.
3. **Reduce:** the answer agent checks whether every subquestion has sufficient
   evidence. It may issue one narrower second wave, then synthesizes claims.
   Recall accepts only claims whose receipts were opened through an authorized
   evidence tool.

Embeddings and lexical search are routing hints. Source objects and their exact
receipts are the evidence.

## Query modes

| Question shape | Retrieval path |
| --- | --- |
| Exact receipt or session | Open the exact session; do not fan out. |
| Source-specific or bounded timeline | Apply the hard filters, then run one hybrid/deep map. |
| Cross-session, cross-source, or multi-part synthesis | Decompose into at most five independent maps, run them concurrently, check coverage, then reduce. |
| Materially ambiguous scope | Ask the user instead of silently searching everything. |

The existing source/session/time structure is Recall's first retrieval
hierarchy. A generated knowledge graph is not required for the prototype.

## Retired prototype contract

`recall.map_reduce` accepts:

- the natural question;
- one to five agent-authored maps;
- for each map: an ID, objective, rewritten query, and hard filters;
- exact seed receipts returned by a prior hybrid investigation;
- one shared bounded depth.

The host:

- rejects duplicate or malformed maps;
- rejects any map that widens explicit time or source-family constraints;
- executes maps concurrently;
- requires the agent to route once with the tenant-bound hybrid investigator,
  then revalidates every seed receipt against the map's hard scope (the
  already-decomposed map never re-enters the broad investigator);
- returns per-map findings, coverage, uncertainty, and aggregate diagnostics;
- records the full bounded evidence result for authorization and trace metrics,
  but gives the reducer a model view of at most six byte-bounded findings per
  map, selecting the highest-ranked finding from each distinct source/session
  before filling by rank, so one giant session cannot drown the synthesis step;
- distinguishes a completed corpus scan and a nonempty result from the agent's
  semantic judgment that the evidence is sufficient for the map objective;
- never puts questions, answers, source bodies, or credentials in the trace.

The caller's agent is both planner and reducer. Recall's closed evidence tools
keep all storage credentials and authorization inside the service boundary.

## Why this shape

- GraphRAG's global search uses map-reduce over hierarchical community reports,
  while local and DRIFT modes route narrower questions differently. Recall
  adopts the query-mode router and map-reduce pattern without requiring an
  expensive graph build:
  <https://microsoft.github.io/graphrag/query/overview/> and
  <https://microsoft.github.io/graphrag/query/global_search/>.
- Anthropic's multi-agent research system uses an orchestrator to decompose a
  query and parallel workers to gather independent evidence. Its contextual
  retrieval work also supports combining lexical and semantic retrieval rather
  than using embeddings alone:
  <https://www.anthropic.com/engineering/multi-agent-research-system> and
  <https://www.anthropic.com/engineering/contextual-retrieval>.
- Google's Agentic RAG and sufficient-context work support iterative retrieval:
  ask whether the current evidence can answer the question, reformulate only
  when it cannot, and abstain when the evidence remains insufficient:
  <https://research.google/blog/unlocking-dependable-responses-with-gemini-enterprise-agent-platforms-agentic-rag/>
  and
  <https://research.google/blog/deeper-insights-into-retrieval-augmented-generation-the-role-of-sufficient-context/>.
- Archil serverless execution gives each concurrent invocation its own
  ephemeral container over the same mounted data, making bounded parallel maps
  a native fit:
  <https://docs.archil.com/compute/serverless-sandboxes> and
  <https://docs.archil.com/guides/ai/bash-tool>.

RAPTOR-style recursive summaries remain a later option for genuinely global
questions, after evaluations show that the natural source/session/time
hierarchy is insufficient: <https://arxiv.org/abs/2401.18059>.

## Hard prototype exit

The prototype passes only when:

1. two differently scoped maps overlap in execution;
2. explicit source and occurred-at constraints cannot be widened;
3. maps return exact receipts and per-map coverage;
4. the reducer cannot cite a receipt that no evidence tool opened;
5. an incomplete map can trigger one targeted second wave;
6. a trace reads `authorize → plan → inspect → synthesize → verify → complete`
   without containing private bodies, questions, answers, or credentials.

The next live gate is an evaluation set containing exact-session, bounded
timeline, source-specific, and cross-session synthesis questions against a
private brain. Private questions and evidence stay outside the repository.
