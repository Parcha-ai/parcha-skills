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

## Agentic boundary truth

The agentic evaluator freezes 60 owner-approved questions: 12 each for exact-document,
bounded-timeline, source-specific, cross-source, and insufficient retrieval. It enforces a
25/15/20 optimize/validation/test split and rejects any logical-document revision shared across
splits. Gold facts and receipts remain in an owner-only file outside Git.

```bash
PYTHONPATH=recall python -m evals.agentic_truth validate \
  --input "$RECALL_PRIVATE_EVAL_DIR/truth.jsonl" \
  --repo-root "$(git rev-parse --show-toplevel)"

PYTHONPATH=recall python -m evals.agentic_truth score \
  --truth "$RECALL_PRIVATE_EVAL_DIR/truth.jsonl" \
  --results "$RECALL_PRIVATE_EVAL_DIR/boundaries.jsonl" \
  --output "$RECALL_PRIVATE_EVAL_DIR/aggregate.json" \
  --run-id frozen-baseline-1 \
  --repo-root "$(git rev-parse --show-toplevel)"
```

The result contains aggregate Boundary Recall@20, Boundary MRR, pointer integrity,
authorization violations, backend errors, and latency only. Per-question rankings, questions,
facts, receipts, source bodies, and traces are never copied into Git output.

Generate the private boundary ranking file through an explicitly tenant- and
source-bound runtime. This step does not inspect gold labels or imply owner
approval:

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
  --retrieval-mode passage
```

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
