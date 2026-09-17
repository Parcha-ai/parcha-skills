# Recall × Jev: replace the regex rot with typed judgments — handoff

Written 2026-09-17 by the recall-architecture-rewrite session (Claude), for the next
developer. Everything you need is here or linked; nothing here depends on that session.

## 0. The objective, in one paragraph

Recall's search and ingest paths carry ~3,000 lines of hand-written guessing: regex date
parsers, harness-name sniffing, clause splitting, "is this an identifier" rules, 5-gram
Jaccard duplicate detection, rerank plumbing that argues about which passage represents
a document, and content-triage rules (secrets, runaway sessions, thinning). Each was
patched again the day new phrasing broke it; today alone we shipped six PRs to that
middle layer. The objective is to move every **semantic judgment** into Jev (TypeSafe's
System One model: typed Choice / Score / Noul answers with calibrated probabilities) and
keep only **execution invariants** in code: arms, fusion math, deadlines, storage,
migrations, authorization. Built the harness way (`~/ati-harness/AGENTS.md`): one
falsifiable feature at a time, baseline + owning boundary + acceptance test + retained
use cases + rollback, and superseded machinery deleted only after replacement evidence
passes on the ruler.

The ruler is the systems card (`recall/evals/systems_card`, validation split, 15 cases,
12 answerable). Nothing lands without a card row.

## 1. Where the system is (2026-09-17 01:00 UTC)

- Search plane: turbopuffer (namespace per tenant, native voyage-4 embeddings, BM25).
  Postgres vector plane retired (migration 067 applied). Worker and MCP both run
  `RECALL_SEARCH_PLANE=turbopuffer`. Embedding worker suspended. No rollback to Postgres
  search exists.
- Card, main 64de873 → 0.875 recall@20 / 0.5702 MRR / 761 ms server p95 / 0 errors.
  Best on record. Nightly card 06:00 UTC posts into the Slack thread in
  `#engineering-updates` (root ts in `~/.recall/systems-card/slack-thread.json`).
- Parquet scan plane converging after #589–#607 (fragmentation contract, spanning
  documents own their parts). Freshness gate expected green after the 09-17 nightly.
- Cascade plan of record: `.cascade/recall-rewrite.md` (status board, every PR and card
  row cited). Memory: `~/.claude/projects/-home-ubuntu-unc-skills/memory/recall-company-brain-state.md`.

Read those two files first. They explain every scar in the code you are about to delete.

## 2. The rot inventory (what Jev replaces), with anchors

All paths under `recall/server/recall_server/`.

| # | what it guesses today | where | lines | known failure modes |
|---|---|---|---|---|
| R1 | Dates and windows in the question ("around May 2-4", "on July 8", "last quarter"), day-level vs loose, boost factor | `temporal_hints.py` (`parse_temporal_hint` :583, `TemporalHintSettings`) and the window pass in `passage_retrieval.py` (`dense_arms`, `window_pass`) | 639 + ~120 | hedged ranges, relative phrases, years without months; the window pass budget saga (#576) |
| R2 | Harness / source family named in the question ("in the Codex work") | `passage_retrieval.py` `parse_source_hint` :554, `source_hints_enabled` :567 | ~60 | only recognises literal harness names |
| R3 | Compound questions → clauses, each run as its own dense + lexical pass | `query_clauses` :501, `_content_words` :494, `clause_passes` / `lexical_arms` inside `_search_admitted` | ~250 | splits on conjunctions; "across both docs, what caused X or Y" misfires |
| R4 | Exact-identifier queries (PR numbers, hashes, error strings) and the min-should-match lexical plan | `identifier_tokens` :116, `lexical_match_plan` :159, `lexical_plan_is_all_common` :202, `_sparse_candidates` (turbopuffer_retrieval.py) | ~300 | common-lexeme table, hash prefixes (#533), person-name skips |
| R5 | Which passage represents a document for the reranker; rerank input framing; blend | `focus_terms` :831, `rerank_context` :917, `rerank_document` :940, `select_rerank_candidates` :960, `apply_rerank_scores` :1007, range ordering in `collapse_document_candidates` (`ranges.sort`), `RERANK_FOCUS_WINDOW`, `RERANK_NOMINATE_PER_ARM` | ~400 | #534/#537/#538 (focus window), #577/#579/#580 (range order, composite) — three regressions in two days |
| R6 | Near-duplicate sessions (Codex copies) and which copy is the primary | `group_near_duplicates` :764, `text_shingles`, `shingle_similarity`, `similar_document_record` | ~110 | 5-gram Jaccard ≥ 0.6; primary = latest `last_occurred_at` (#582/#584) |
| R7 | Who authored a record (native identities, display names, local-user heuristics) | `actor_attribution.py` (`attribute_canonical_events` :215, `is_local_user_authored` :175, `claimable_actor_for_display_name` :56) | 383 | regex on display names; assistant-vs-user authorship false positives (see `e2e_employee_attribution`) |
| R8 | Content triage: secrets (regex families in `redaction`/`secret_scan` paths, `evals/systems_card/corpus.py` patterns), runaway sessions (#530), oversized records (`e2e_oversized_records`), body thinning policy (`canonical_thinning.py`) | several | ~600 | every rule is a table that grew per incident |
| R9 | The card's answer judgment: gold boundaries only (12 answerable), no per-candidate relevance | `evals/agentic_truth.py` `score_boundary_candidates` :572, truth set `~/.recall-cascade/agentic-map-reduce-20260727/employee-truth-v2-approved.jsonl` | — | ±0.05 MRR run-to-run noise from 12 cases |

Keep in code, untouched by this work: fusion (`fusion.py`, convex alphas), deadlines and
admission, hydration, outbox/drain (`turbopuffer_projection.py`), parquet layout
(`parquet_scan.py`), migrations, authorization, receipts.

## 3. Target architecture

### 3.1 One module owns every judgment: `recall_server/judgments.py`

```
JudgmentClient            # typesafe_sdk TypeSafeClient/AsyncTypeSafeClient behind our
                          # own interface; model pinned to a versioned id (jev-1.13.0),
                          # alias never; logs the response model id; content-free logs.
QueryReading              # dataclass: window(since,until,confidence,day_level),
                          # harness(Choice), identifier_lookup(bool,p), sub_questions(list),
                          # person(Choice over recall_people), answer_kind(Choice)
read_query(text, *, people, now) -> QueryReading         # ONE fan-out request
DocumentJudgment          # dataclass: answers(Score levels), p, confidence
judge_documents(question, docs[]) -> list[DocumentJudgment]   # ONE fan-out request
SameWork                  # continuation/copy/unrelated + which member is most complete
judge_same_work(a, b) ...
TurnReading               # kind(Choice), keep(Score), secret(Noul), actor(Choice)
read_turn(record, *, actors) -> TurnReading             # ONE fan-out per projected turn
```

Rules (from `AGENTS.md`, adapted):

- **Judgment in the model, invariants in code.** `judgments.py` returns typed values and
  probabilities. Thresholds, weights, deadlines, and what to do with a low-confidence
  answer live in the caller and are named constants with an env override, exactly like
  `RECALL_SEARCH_FUSION_ALPHAS` today.
- **Whole rubric in the question.** Each question carries its full instructions and
  criteria (JSON structure where contrasts matter). No rule restated in code that the
  question does not also carry. Question ids are for code only.
- **Fan-out, not chains.** Everything answerable from the same state goes in one request
  (query reading: all six questions; document judgment: one Score per document). A
  second request only when the first answer is needed to fetch new state.
- **Fallback = today's code, demoted.** Every wave keeps the regex path as the fallback
  when Jev is unavailable, times out, or confidence is below the wave's floor, and the
  diagnostics say which path answered (`diagnostics.judgments = {path: "jev"|"fallback",
  model: "jev-1.13.0", elapsed_ms, confidence}`). Delete the fallback in the wave's last
  PR, after the card has held for 3 nightlies with the fallback rate < 1%.
- **Judgments are data.** Ingest-time answers are stored as filterable attributes on the
  turbopuffer row (`kind`, `keep`, `actor_ids` already exists) and in the passage catalog,
  so ranking can filter on them and the card can stratify by them. Search-time answers go
  into `diagnostics` (content-free: no question text, no snippets).
- **Versioned candidates.** A change to a question's instructions/criteria is a new
  `judgments.py` version string (`JUDGMENTS_CONTRACT = "q-v3"`) recorded in
  diagnostics and in the card row, like `policy_fingerprint` today. Never let a wave
  rewrite its own verifier: the card gates and the truth set do not change in the same
  PR as a judgment.

### 3.2 Client, key, budgets

- SDK: `typesafe-sdk` (Python; async client in the MCP, sync in the worker). Read
  `https://docs.typesafe.ai/sdk/python.md` and `.../sdk/python/usage.md` before writing
  code; the skill at `~/.claude/skills/typesafe-ai/SKILL.md` (also linked for Codex at
  `~/.codex/skills/typesafe-ai`) is the map.
- Key: `TYPESAFE_API_KEY` in the greppy interactive 1Password environment (Miguel
  creates it), then `RECALL_TYPESAFE_API_KEY` on the MCP (`srv-d9o4vf6417fc73ei24ag`) and
  worker (`srv-da16tfou01pc739jvntg`) via the scratchpad `render.sh` pattern. Same hygiene
  as `RECALL_TPUF_API_KEY`: 0600 key file locally (`RECALL_TYPESAFE_KEY_FILE`), never
  printed, never in `~/docs`.
- Price: $0.042 per million input tokens, output free (docs.typesafe.ai/models, 09-17).
  Search: ~2 k tokens per query reading + ~20 k per document judgment (20 docs × 1 k)
  ≈ $0.001 per search. Ingest: ~150 k passages/day × ~1.5 k tokens ≈ 225 Mtok/day ≈
  $9.50/day at full volume; gate it behind `keep`-first ordering and measure.
- Rate limits: 1,200 requests/min, 250 k tokens/s (adjusting dynamically; SDK retries
  with backoff). The worker's ingest fan-out must go through a `TokenPacer` like the
  turbopuffer drain (`turbopuffer_projection.TokenPacer`).
- Latency budget on the hot path: the query reading runs **in parallel with the arms**
  (submit it to the same executor as the window pass; the arms do not need it). Only the
  collapse needs it, so the effective cost is `max(arms, jev) − arms`. Hard timeout
  `RECALL_JUDGMENT_TIMEOUT_MS` (start 400 ms, measure in W0); on timeout the fallback
  answers and the diagnostics say so. Document judgment replaces the Voyage call it sits
  where (`_rerank_fused`), same `rerank_min_budget_seconds` rule.
- SDK retries: **off** inside a search deadline (`RetryPolicy(max_retries=0)`), exactly
  the lesson of #576 with the turbopuffer SDK. On for the worker.

### 3.3 The waves (one falsifiable feature each)

Each wave: baseline row cited → owning boundary → acceptance test on the card → retained
use cases (tests that must still pass) → rollback (env switch) → deletion PR.

**W0 — spike (½ day).** No production code. `evals/judgments_spike.py` runs the 15
validation questions through `read_query` and the 50-candidate `judge_documents` from
saved probe rows (`~/.recall/systems-card/systems-card-boundaries-validation-*.jsonl`
carry `candidates`, `arm_scores`, `rerank_evidence`). Report, content-free: p50/p95
latency per request, tokens, cost, and for R1: agreement of Jev's window with the regex
hint and with the gold document's dates. Exit: numbers in the plan; go/no-go on the
latency budget.

**W1 — query reading (R1, R2, R3, R4 detection).** `read_query` feeds `temporal_hint`,
`source_boost`, `clauses`, and the sparse-arm decision. Owning boundary:
`PassageHintRetrieval._search_admitted` inputs; the arms and fusion are untouched.
Acceptance: card recall@20 ≥ 0.875 and MRR ≥ 0.57 (both ≥ today) with
`diagnostics.judgments.path == "jev"` on ≥ 95% of probe searches; per-case: the four
dated questions (f50d, b0f4, 1878, 672e) keep or improve rank. Retained: every test in
`test_temporal_hints.py`, `test_query_clauses*.py`, `test_source_hints*.py` still passes
with `RECALL_JUDGMENTS=off`. Rollback: `RECALL_JUDGMENTS=off`. Deletion PR: `temporal_hints.py`
parser, `parse_source_hint`, `query_clauses`, `_content_words`, `focus_terms`, the
common-lexeme table; keep `TemporalHint` as the typed value the reading fills.

**W2 — document judgment (R5).** After fusion, hydrate the top 20 and ask one Score per
document ("how well does this session answer the question": levels *not at all / touches
the topic / partially answers / answers it / is the definitive record*), state = header +
two strongest passages + question. Blend by probability-weighted level with the fused
score (start 0.6 like today), then delete the range-order sort, the composite experiment's
remains, `focus_window`, and the nomination plumbing once the card holds. Try both: Jev
on top of Voyage's 50→20, and Jev alone over 50. Acceptance: MRR ≥ 0.57, recall@5 up
(add the `boundary_recall@5` gate that H2-e promised), rerank arm p95 ≤ 500 ms.

**W3 — same-work grouping (R6).** For each pair (candidate, higher-ranked candidate)
whose headers share source and overlap in time, one Choice: *copy / continuation /
unrelated*, and for a group one Choice: *most complete member*. Replaces shingles and the
`last_occurred_at` rule. Acceptance: recall@20 ≥ 0.875 with the strict scorer; the
ce5f/11ce pattern (gold is a later copy) resolved by the judgment, not by a date rule.

**W4 — ingest triage as attributes (R8, plus the H4 lever).** In `passage_index._commit`
(or a worker phase after it) one fan-out per new passage: `kind` (Choice: decision /
error / fix / plan / discussion / tool-output / noise), `keep` (Score), `secret` (Noul,
asked only on regex hits → verify-and-escalate). Stored on the turbopuffer row and in
`canonical_passages` (migration 068: two columns). Then: (a) the thinning policy becomes
"thin bodies where keep ≤ level 1" (delete `canonical_thinning` rule table), (b) runaway
and oversized detection become `noise` kind, (c) `recall_search` gains a `kind` filter and
the H4 "what did we decide" stratum is a filter, not an extractor. Acceptance: no card
regression; churn probe shows tool-noise passages not projected; a new truth stratum for
decisions scored ≥ baseline. Cost measured against the estimate above.

**W5 — the ruler grows (R9).** A Noul "does this document answer the question" over all
50 candidates per validation case, run offline, produces candidate labels a human
approves in bulk (owner review packet exists: `agentic_truth.build_owner_review_packet`).
Grow the validation split from 12 answerable to ≥ 40 before trusting any MRR delta under
0.05. This wave does not touch production; it is what makes W1–W4 measurable.

**W6 — actor attribution (R7).** Choice over the tenant's known actors with the record's
native references and display names as state; regex display-name matching becomes the
fallback. Acceptance: `e2e_employee_attribution` counts unchanged or better;
`assistant_authorship_false_positives == 0`.

Order: W0 → W5 (cheap, unblocks measurement) → W1 → W2 → W4 → W3 → W6. W1 and W4 are the
two that delete the most code.

## 4. TDD: the tests to write before the code

Fixture first: `tests/central_brain/fake_judgments.py`, a `FakeJudgmentClient` that
answers from a dict keyed by question id (default answers are neutral: no window, no
harness, not an identifier, one sub-question, `discussion`, keep = middle level), records
every request (state, questions) so tests assert **what was asked**, and can be told to
time out or raise. Same pattern as `fake_turbopuffer.py`, same reason: the SDK's real
row/answer objects bit us once (#572); the fake must mirror the SDK's answer shapes
(`result.choices[id].choice / .probabilities / .confidence`, `result.nouls[id].noul`,
`result.scores[id].score / .probabilities`).

For each wave, in this order:

1. **Contract tests** (`test_judgments.py`): every question carries complete
   instructions and criteria (no empty strings, criteria cover a no-match outcome, Score
   levels are concrete sentences); the versioned model id is pinned; `JUDGMENTS_CONTRACT`
   changes when any instruction text changes (hash test, like
   `test_passage_fingerprint_includes_header_contract`).
2. **Reading tests**: `read_query` maps SDK answers into `QueryReading` including
   confidence gating (below floor → field is None and `path` records the fallback);
   timeouts and exceptions never raise into `search()`.
3. **Wiring tests** (in `test_canonical_retrieval.py` / `test_temporal_hints.py` style):
   with the fake, a dated question runs the window pass from Jev's window; with
   `RECALL_JUDGMENTS=off` the response is byte-identical to today (there is an existing
   byte-identity test for temporal hints; extend it).
4. **Replay eval** (`evals/judgments_replay.py`, sibling of `rerank_blend_replay.py`):
   offline, from saved probe rows, reports per-case rank deltas for W2/W3 before any
   deploy. Content-free output, private results dir, rejects in-repo paths (copy the
   `test_fusion_tuning` hygiene tests).
5. **e2e** (`server/tests/e2e_judgments.py`, CI's fresh Postgres): file-backed fake
   client (env `RECALL_JUDGMENT_CLIENT_FACTORY=module:callable`, like
   `RECALL_TPUF_CLIENT_FACTORY`) so worker and server share one fake; asserts stored
   attributes round-trip (W4) and the kind filter works end to end.
6. **Card**: the accuracy probe records `judgments.path`, `judgments.contract`, and
   `judgments.fallback_rate` in metrics; a gate `fallback_rate ≤ 0.01` is added in the
   wave's deletion PR, never before.

Deletion PRs carry a **retention test**: the retained use cases from the wave table run
against the deleted-path-free code and pass.

## 5. Guardrails and gotchas the next dev will hit

- Any SDK with retries inside an arm deadline: `max_retries=0` (turbopuffer lesson, #576).
- Render one-off jobs: a start command with spaces inside quotes runs but logs nothing;
  use whitespace-free forms (`python -u -c exec(...gzip+base64...)`) or the CLI module.
  Job logs: `logs?ownerId=…&resource=<job-id>&type=app&direction=backward`.
- Every worker deploy restarts an in-flight parquet build; batch worker deploys.
- Ship loop: scratchpad `shiplite.sh <pr> <services…>` (3-min CI poll; GitHub API quota
  is 5 k/h). Card: `card-now.sh` (accuracy+latency, ~10 min). Both under
  `/tmp/claude-1000/-home-ubuntu-unc-skills/3429078a-d2cd-4810-923c-d3c66618c59c/scratchpad/`
  and worth copying into `recall/scripts/` (H0-5 never finished that promotion).
- Status board edits only in `~/worktrees/recall-status` on a fresh branch, merged
  immediately (`.cascade` is gitignored: `git add -f`).
- Card noise: ±0.05 MRR between identical runs on 12 cases. Do W5 early.
- The truth set and the validation split are the verifier; a wave never edits them in
  the same PR (AGENTS.md: a candidate never rewrites its verifier).
- Content-free everywhere: judgments state carries customer text to TypeSafe (that is
  the point), but logs, diagnostics, cards, `~/docs`, and PR bodies carry ids and numbers
  only.
- Calibration is a claim to verify on our data: W0 and W5 measure it (agreement with
  gold, Brier on the Noul labels) before any threshold is trusted.

## 6. Decisions for Miguel (each blocks one wave)

1. TypeSafe key in the interactive 1Password environment (blocks W0).
2. Ingest-time judgments send passage text to TypeSafe: confirm that is acceptable under
   the same terms as sending it to Voyage/turbopuffer today (blocks W4).
3. W5 owner review: someone approves the bulk labels the Noul proposes (blocks growing
   the truth set).

## 7. Pointers

- Plan of record: `.cascade/recall-rewrite.md` (add the waves as H6 rows; H4 recaps and
  H5-3 embedding guardrails are subsumed by W4).
- Skill: `~/.claude/skills/typesafe-ai/SKILL.md`; docs index `https://docs.typesafe.ai/llms.txt`;
  rerank cookbook `.../cookbooks/rerank_typesafe.md`; fan-out `.../patterns/fan-out.md`;
  confidence `.../confidence.md`.
- Harness principles: `~/ati-harness/AGENTS.md`.
- Card history: `~/.recall/systems-card/out/history.jsonl`; probe rows
  `~/.recall/systems-card/systems-card-boundaries-validation-*.jsonl`.
- Services: MCP `srv-d9o4vf6417fc73ei24ag`, worker `srv-da16tfou01pc739jvntg`, embedding
  worker (suspended) `srv-dak6dt5g1s2s738dgnbg`; PlanetScale `miguel-miguelrios/recall-brain`
  (PS-160, downsize to PS-80 after a clean nightly).
