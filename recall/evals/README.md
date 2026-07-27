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
