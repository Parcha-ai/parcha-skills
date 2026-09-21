# Recall retrieval evaluation

This directory contains the content-free scoring machinery for Recall's frozen synthetic
retrieval suites. It measures Hit/Recall/Precision at k, MRR, nDCG, negative false hits,
authorization violations, latency, session reconstruction, deletion, and ingest deduplication.

## Live synthetic baseline

Use only an empty disposable database. The runner refuses a database that already contains source
events.

```bash
PYTHONPATH=recall python -m evals.runner live \
  --dsn postgresql://localhost/recall_eval \
  --corpus recall/tests/central_brain/retrieval_eval_v2/corpus.jsonl \
  --queries recall/tests/central_brain/retrieval_eval_v2/queries-dev.jsonl \
  --output /tmp/recall-eval-dev.json \
  --repo-root "$(git rev-parse --show-toplevel)"
```

Holdout filenames can emit aggregate output only; pass `--aggregate-only`. The central E2E test
also exercises the real HTTP search/show boundary and verifies two-run ranking determinism.

## Private directional scoring

Private queries and rankings must live outside the git top level in mode-`0600` regular files,
under a directory that grants no group/world access. The output must be a new file. The private
report contains only aggregate metrics, content hashes, runtime pins, and an opaque run ID.

```bash
PYTHONPATH=recall python -m evals.runner score \
  --private --aggregate-only --run-id opaque-run-id \
  --queries "$RECALL_PRIVATE_EVAL_DIR/queries.jsonl" \
  --rankings "$RECALL_PRIVATE_EVAL_DIR/rankings.jsonl" \
  --output "$RECALL_PRIVATE_EVAL_DIR/aggregate.json" \
  --repo-root "$(git rev-parse --show-toplevel)"
```

The runner never queries a private Brain itself. A separately authorized process produces the
private ranking file so credentials and raw responses stay outside the evaluator and repository.

## Systems card (whole-system evaluation)

`evals/systems_card/` measures the deployed brain the way a client agent uses it: every probe
goes through the public MCP with an owner-scoped read token. It writes a content-free
`card.json` (schema `recall.systems-card.v1`), a self-contained `card.html`, and appends one
line to `history.jsonl` so runs can be compared over time.

| Dimension | Probe | What it measures |
|---|---|---|
| availability | `availability.endpoints` | healthz / readyz / MCP ping success and latency; readiness reporting `busy` |
| latency | `latency.tools` | wall-clock p50/p95/max per tool (search, scope, people, session_context, show, exec, scan), first call kept separately |
| latency | `latency.search_stages` | server `diagnostics`: elapsed, deadline-exceeded rate, dense arm health, candidates per arm |
| latency | `latency.archil_phases` | scan `timing`: queue, execute, and every sandbox phase |
| accuracy | `accuracy.truth_boundary` | the owner-approved 60-question truth set replayed through `recall_search`, scored by `evals.agentic_truth` (boundary recall@20, MRR, case hit rate, false hits on insufficient questions, pointer integrity, per stratum) |
| accuracy | `accuracy.synthetic_suite` | surfaces a `recall.retrieval-eval.v1` report from the frozen synthetic suite that CI runs |
| freshness | `freshness.source_age` | age of the newest visible passage per source (sources hashed), `projection_pending` |
| integrity | `integrity.scan_consistency` | `recall_scope` enumeration vs `recall_scan` distinct documents, `objects_unavailable` |
| integrity | `integrity.forget_latency` | capture one synthetic memory, wait until search sees it (`capture_visible_after_s`), forget it, wait until search and receipt resolution no longer return it (`forgotten_after_s`, `receipt_unresolvable`); off unless `--forget-probe` because it writes to the brain |
| authorization | `authorization.negative_scope` | unauthorized source / unknown person / foreign tenant must return nothing |
| privacy | `privacy.secret_scan` | secret-shaped strings that survived redaction, counted inside the sandbox; emails/phones reported |
| cost | `cost.planetscale` | cluster tier, IOPS/throughput, storage bounds, month-to-date invoice, budget alert (optional) |
| cost | `cost.storage` | where the bytes are: Postgres database size and the 12 largest tables (from the brain's `/metrics`, needs a metrics-scoped token), PlanetScale storage min/max bounds, evidence-bucket bytes under `objects/` (page-capped); gate `postgres_database_gib <= 40` |

`cost.storage` reads three optional sources and notes each one it skips: the brain's `/metrics`
(`--metrics-token-file` or `RECALL_METRICS_TOKEN_FILE`, a mode-0600 JSON `{"token": ...}` with the
`metrics` scope; the brain exports `recall_database_bytes` and `recall_table_bytes{table=...}`),
the PlanetScale branch (same credentials as `cost.planetscale`; the API publishes no per-table
sizes), and the evidence bucket (`RECALL_EVIDENCE_ARCHIVE_BUCKET` plus either the archive keys or
ambient AWS credentials; listing stops after 200 pages of 1000 keys and reports
`s3_listing_complete`). The 40 GiB gate is the H1 target and fails at the 108 GB baseline on
purpose; H3 lowers it to 10.

Gates are initial thresholds, recorded in the card next to the observed value. A failed gate marks
the dimension `degraded`; a probe that cannot run marks it `failed`; a probe without inputs is
`skipped` with the reason.

```bash
cd recall
python -m evals.systems_card run \
  --output-dir "$HOME/.recall/systems-card/out" \
  --private-dir "$HOME/.recall/systems-card" \
  --truth "$RECALL_PRIVATE_EVAL_DIR/employee-truth-v2-approved.jsonl" --truth-split validation \
  --since 2026-09-01 --repetitions 3
```

The MCP URL and token come from `~/.config/recall-brain/client.json` (or `--url` / `--token-file`,
`RECALL_URL` / `RECALL_TOKEN_FILE`). `--dimensions availability,latency` restricts a run;
`--queries FILE` replaces the built-in generic latency queries; `--planetscale-org` and
`--planetscale-database` plus `PLANETSCALE_SERVICE_ACCOUNT_ID` / `PLANETSCALE_SERVICE_TOKEN` enable
the cost probe; `--forget-probe` enables the write-then-forget latency probe (needs a token with
`write` scope; the receipt and body never reach the card). Per-case rankings are written mode-0600 under `--private-dir`, never into the
card. `python -m evals.systems_card render --output-dir DIR` re-renders HTML from an existing
`card.json`.

`python -m evals.systems_card summary --output-dir DIR --git-sha SHA --date YYYY-MM-DD
[--reconcile LINE]` prints the nightly summary text for an existing `card.json` plus its
`history.jsonl`. The nightly cron reads it from here rather than formatting its own, so the
percentile labels stay tied to the metric keys they are read from.

An approved validation expansion uses `--truth-expansion /private/bundle/manifest.json`
instead of `--truth`. Keep the manifest and three JSONL files mode-0600 in an owner-only
directory outside git. The closed manifest schema is:

```json
{
  "schema_version": "recall.systems-card.truth-expansion.v1",
  "base_canonical_sha256": "<frozen base canonical SHA256>",
  "base": {"path": "base.jsonl", "sha256": "<raw file SHA256>"},
  "additions": {"path": "additions.jsonl", "sha256": "<raw file SHA256>"},
  "families": {"path": "families.jsonl", "sha256": "<raw file SHA256>"}
}
```

Artifact paths are relative to the manifest, with no `..` or symlink traversal. The base
canonical digest comes from `evals.expanded_truth.canonical_sha256`; freeze it before reviewing
additions. All pins, case schemas, approvals and native-family separation are checked before
any probe makes a network call. This option requires `--truth-split validation` (the default).
The expanded validation set retains every original negative and failed search. It uses the
existing metric math and gates, while `original.*` independently scores the original validation
panel from the same calls and must pass the same five gates. Both panels and the manifest pin
appear in card history. Pointer/authorization fields retain the existing candidate adapter's
semantics; the separate sampled receipt-resolution check is not duplicated as a panel audit.
The original `--truth` behavior is unchanged. A larger passing set does not establish a
candidate improvement, calibrated labels, or the required consecutive nightly acceptance.

Unit coverage runs against a fake brain (`tests/test_systems_card.py`); the live run is an
operator action, not a CI step, because it needs the owner token and takes several minutes.

### Agentic truth scorer

`evals/agentic_truth.py` (restored) validates the 60-case truth contract and scores boundary
rankings. The ranking producer and candidate-matrix tools that drove a server-side agent were
retired when the calling agent became the retrieval agent; the systems card's accuracy probe is
the replacement producer, ranking through `recall_search` exactly as clients do.

## Agentic boundary truth

The agentic evaluator freezes 60 owner-approved questions: 12 each for exact-document,
bounded-timeline, source-specific, cross-source, and insufficient retrieval. It enforces a
25/15/20 optimize/validation/test split and rejects any stable logical document shared across
splits, even when projection revisions differ. Discovery is scored by source plus logical
document; revision freshness and exact revision agreement are reported separately. Gold facts
and receipts remain in an owner-only file outside Git.

```bash
PYTHONPATH=recall python -m evals.agentic_truth validate \
  --input "$RECALL_PRIVATE_EVAL_DIR/truth.jsonl" \
  --repo-root "$(git rev-parse --show-toplevel)"

PYTHONPATH=recall python -m evals.agentic_truth score \
  --truth "$RECALL_PRIVATE_EVAL_DIR/truth.jsonl" \
  --results "$RECALL_PRIVATE_EVAL_DIR/boundaries.jsonl" \
  --output "$RECALL_PRIVATE_EVAL_DIR/aggregate.json" \
  --run-id frozen-baseline-1 \
  --split validation \
  --repo-root "$(git rev-parse --show-toplevel)"
```

The result contains aggregate Boundary Recall@20 and @50, case hit rate at 50,
Boundary MRR, pointer integrity, authorization violations, backend errors, and
latency only. `--split` scores one frozen partition while still validating the
complete truth contract. Per-question rankings, questions, facts, receipts,
source bodies, and traces are never copied into Git output.

**Retired:** the commands below (`evals.agentic_rankings`, `evals.agentic_candidate_matrix`)
were removed with the server-side agent; use the systems card accuracy probe instead.
They are kept here as the historical contract:

```bash
PYTHONPATH=recall:recall/server python -m evals.agentic_rankings \
  --input "$RECALL_PRIVATE_EVAL_DIR/truth.jsonl" \
  --output "$RECALL_PRIVATE_EVAL_DIR/boundaries-run-1.jsonl" \
  --repo-root "$(git rev-parse --show-toplevel)" \
  --run-id frozen-baseline-1 \
  --tenant tenant:company:example \
  --source claude:linux:example \
  --source codex:linux:example
```

Select the lossless passage hint index without changing the private output
contract:

```bash
PYTHONPATH=recall:recall/server python -m evals.agentic_rankings \
  --input "$RECALL_PRIVATE_EVAL_DIR/truth.jsonl" \
  --output "$RECALL_PRIVATE_EVAL_DIR/passage-boundaries.jsonl" \
  --repo-root "$(git rev-parse --show-toplevel)" \
  --run-id passage-validation-1 \
  --tenant tenant:company:example \
  --source claude:linux:example \
  --source codex:linux:example \
  --retrieval-mode passage \
  --candidate-depth 50 \
  --expected-cases 15 \
  --query-bundle "$RECALL_PRIVATE_EVAL_DIR/query-bundle.json" \
  --arm fused
```

Candidate-generation evaluation accepts owner-private, exact-coverage query
bundles and can report `dense`, `passage-lexical`, `sparse-exact`, and `fused`
arms independently. The public MCP hint limit is unchanged.

## Tuning the fusion alphas offline

`recall_search` fuses its arms with convex min-max fusion (`RECALL_SEARCH_FUSION=convex`,
alphas from `RECALL_SEARCH_FUSION_ALPHAS`, default `dense:0.15,lexical:0.30,sparse:0.55`).
The systems-card accuracy probe saves each case's ranked candidates privately as
`systems-card-boundaries-<split>-<stamp>.jsonl` together with every candidate's per-arm
`arm_scores`. The tuner replays fusion over those saved rows for every alpha on a simplex
grid (step 0.05), picks the alpha that maximises MRR on the `optimize` split without lowering
recall@20 below the recorded ordering, and reports the `validation` split for that alpha.
Only documents that reached the saved candidate list can move, so capture with the deepest
`CANDIDATE_LIMIT` the MCP allows. Run the probe once per split you need
(`--truth-split optimize`, `--truth-split validation`), then:

```bash
PYTHONPATH=recall:recall/server python -m evals.fusion_tuning \
  --truth ~/.recall/eval/agentic-truth.jsonl \
  --results ~/.recall/systems-card/systems-card-boundaries-optimize-<stamp>.jsonl \
            ~/.recall/systems-card/systems-card-boundaries-validation-<stamp>.jsonl \
  --output ~/.recall/systems-card/fusion-tuning-<stamp>.json
```

Inputs and the output must live outside the repository under an owner-only directory; the
report is content-free (alphas, aggregate metrics per split, digests) and prints the
`RECALL_SEARCH_FUSION_ALPHAS` value to deploy. `--baseline-alphas` names the alphas the rows
were captured with when the recorded ordering should not be the recall floor; `--step`,
`--k`, `--tune-split`, and `--report-split` override the defaults.

When a Recall@50 miss could be either retrieval absence or fusion loss, freeze
one depth-100 matrix before changing either subsystem. Live matrix generation
does not read truth and writes no query text or source content:

```bash
PYTHONPATH=recall:recall/server python -m evals.agentic_candidate_matrix live \
  --input "$RECALL_PRIVATE_EVAL_DIR/validation-questions.jsonl" \
  --query-bundle "$RECALL_PRIVATE_EVAL_DIR/validation-query-bundle.json" \
  --output "$RECALL_PRIVATE_EVAL_DIR/candidate-matrix.jsonl" \
  --repo-root "$(git rev-parse --show-toplevel)" \
  --run-id candidate-availability-1 \
  --tenant tenant:company:example \
  --source claude:linux:example \
  --source codex:linux:example \
  --expected-cases 15

PYTHONPATH=recall:recall/server python -m evals.agentic_candidate_matrix score \
  --truth "$RECALL_PRIVATE_EVAL_DIR/truth.jsonl" \
  --matrix "$RECALL_PRIVATE_EVAL_DIR/candidate-matrix.jsonl" \
  --output "$RECALL_PRIVATE_EVAL_DIR/candidate-attribution.json" \
  --repo-root "$(git rev-parse --show-toplevel)" \
  --run-id candidate-attribution-1 \
  --split validation
```

The aggregate scorer exhaustively classifies each gold document as already in
fused top 50, available somewhere in the retriever top-100 union but dropped,
or absent from every retriever. A read-only evaluator deadline may be longer
than the production search deadline so operational timeouts cannot masquerade
as semantic absence; it does not alter service or MCP configuration.

After a passage projection, audit exact S3 reconstruction, full dense-span
coverage, embedding convergence, and vector compression. The report is
aggregate-only; source bodies, identifiers, object keys, and receipts never
leave process memory.

```bash
PYTHONPATH=recall:recall/server python -m evals.passage_index_audit \
  --tenant tenant:company:example \
  --target-tokens 1024 \
  --overlap-tokens 128 \
  --sample-size 500 \
  --repo-root "$(git rev-parse --show-toplevel)"
```

## Logical corpus reconstruction

The logical-corpus audit deterministically samples across every source and
document-size quartile. It recomputes each sample from current canonical rows
through the production projector, reads every persisted S3 part, validates the
manifest and record contracts, and compares exact encoded-byte digests. Its
stdout is aggregate-only.

```bash
PYTHONPATH=recall:recall/server python -m evals.logical_corpus_audit \
  --tenant tenant:company:example \
  --sample-size 200 \
  --concurrency 8 \
  --repo-root "$(git rev-parse --show-toplevel)"
```

## Source-reviewed candidate diagnostics

`evals.candidate_review` compares one frozen prediction pass with approved labels on
selected passages. It is separate from boundary truth and never dispatches models,
approves labels, updates gold, or selects a production threshold.

Keep all four JSONL inputs in mode-0600 files under an owner-only directory outside git:

```bash
PYTHONPATH=recall python -m evals.candidate_review \
  --evidence "$RECALL_PRIVATE_EVAL_DIR/evidence.jsonl" \
  --reviews "$RECALL_PRIVATE_EVAL_DIR/reviews.jsonl" \
  --predictions "$RECALL_PRIVATE_EVAL_DIR/predictions.jsonl" \
  --protected-families "$RECALL_PRIVATE_EVAL_DIR/protected-families.jsonl" \
  --expected-evidence-sha256 "$RECALL_CANDIDATE_POOL_SHA256"
```

The schemas are closed; every listed field is required:

| Input | Row fields |
| --- | --- |
| Evidence | `id`, `case_id`, `question`, `source_id`, `logical_document_id`, `text`, `context` (object), `receipts` (nonempty list), `families` (list), `complete` (boolean) |
| Review | `id`, `evidence_sha256`, `reviewer`, `label`, `rationale`, `witnesses` |
| Prediction | `id`, `evidence_sha256`, `probability`, `error` |
| Protected families | `family_id` |

Freeze the ordered evidence list with `evidence_digest(evidence)` before review. Each
review and prediction also pins its full individual evidence row with that function,
including the question, source metadata in `context`, and provenance. Evidence text
and context must match what the model saw. IDs identify unique candidates within a
case; duplicate boundaries, unknown IDs, and stale pins are rejected.

Labels are `answers_query`, `does_not_answer`, or `insufficient_evidence`. Each witness
contains `start`, `end` (zero-based Unicode character offsets, end exclusive), `quote`,
and `receipt`. Positive labels require a witness. The scorer checks exact text and
receipt membership; the reviewer must attest that the receipt actually supports the
quote and that source-family attribution is correct. A negative label applies only
to the supplied passages, not an unseen full document.

A prediction has a finite probability in [0, 1] and null error, or null probability
and a nonempty error. Missing reviews and predictions stay in coverage. Protected
families, unresolved lineage, incomplete evidence, ambiguous labels, and prediction
failures are excluded from binary arithmetic and counted separately; coverage counts
can overlap. Standard output contains only aggregate counts, hashes, Brier score,
and a fixed 0.5 confusion matrix. Related candidate documents and selected questions
are correlated: this diagnostic does not establish population calibration, retrieval
improvement, or a deployment threshold.

### Capture selected evidence when freezing a search pool

The accuracy probe can opt in to private evidence capture immediately after each
search, before later projection changes make the pinned source unavailable:

```bash
PYTHONPATH=recall python -m evals.systems_card run \
  --dimensions accuracy --truth-expansion "$RECALL_PRIVATE_EVAL_DIR/expansion.json" \
  --private-dir "$RECALL_PRIVATE_EVAL_DIR" --capture-candidate-evidence \
  --output-dir "$RECALL_CARD_DIR"
```

The private directory must already exist, be owner-only and stay outside Git.
The expansion must use canonical hashed native families. Capture uses at most four
workers, 30 seconds per source call, no retries, and a run budget of 250 source
calls per selected validation question. It makes no model calls. Capture duration
is separate from measured search latency; interleaved reads can still affect the
live service, so this is an explicit diagnostic run, not a normal latency sample.

Every actual returned slot remains in the new `candidate-evidence-*` directory.
For the first two matching ranges, missing spans or truncated receipts are recovered
through `recall_passage_metadata` for the exact passage IDs, revision and manifest
hash. This bounded, paged read returns only stored metadata hints, never prose or
opened citation authority. A manifest-pinned source metadata pass then establishes
native family identity before a separate visible-text pass. Protected, unresolved,
unavailable metadata, advanced manifests, over-budget and failed reads retain their
status without substituting newer text. Complete evidence requires verified source
parts, exact spans and receipts, closed pagination, agreement with the original
search prefix and, when metadata was recovered, the stored passage text hash. The passages are joined by one newline with Unicode character offsets,
UTF-8 source spans and text hashes. Completeness applies only to those selected
passages, never the complete document or session.

Search receipts pin the raw result hash and preserve selected-range metadata;
they omit snippet prose until its source family is eligible. Per-call and
per-candidate receipts survive interruptions. `manifest.json` is written only by
successful finalization; its presence means capture finished, not that every slot
was available. See `summary.json` and candidate statuses for actual coverage.

For a separately frozen bounded batch, `CandidateCapture(output, client_factory=...,
protected_families=set_of_hashes, max_calls=...)` provides the same
`capture_case(case_id, query, search_result, search_error=...)` and `finish()` API.
It does not approve labels, change truth, or change systems-card scoring.
