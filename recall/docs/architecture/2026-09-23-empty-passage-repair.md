# Repair current empty passage documents

Typed communication messages can be visible without a resolved employee actor. Older projections may have recorded these logical documents as successfully processed with zero passages. Normal `backfill-lossless-passages` skips a document whose revision and policy are already current, even when it is empty.

`repair-empty-passages` inspects one source through the existing canonical manifest/part reader and passage projector. It defaults to a dry run. It neither reingests logical evidence nor changes the passage policy, and it makes no embedding/model calls. Optional application adds work to the existing passage queue; the normal projection worker performs the commit and updates the Parquet/search outboxes.

From `recall/`, using the operator's existing database/archive environment:

```bash
PYTHONPATH=server python -m recall_server.cli repair-empty-passages \
  --tenant "$TENANT" --source "$SOURCE" --limit 25
```

The report contains document IDs, revisions/hashes, counts and byte estimates, not passage text or receipts. Selection is restricted to empty documents on the current policy without existing queue entries. Each inspected archive is hash-verified. A stale evidence revision or mismatched policy is not eligible to queue. Metadata-only documents can still recompute to zero and are not queued.

Review the report before applying a batch. Add `--price-per-mtoken` with the operator's current input-token price to obtain an estimated indexing cost; otherwise cost is `null`. Tokens are estimated from UTF-8 text bytes plus the maximum context header divided by four. This is an advisory **batch estimate**, not exact provider billing, a corpus extrapolation, or a resource ceiling. Archive bytes include the inspected manifests and parts. Archive reads, indexing and worker time have separate operational costs.

```bash
PYTHONPATH=server python -m recall_server.cli repair-empty-passages \
  --tenant "$TENANT" --source "$SOURCE" --limit 25 --apply
```

Apply recomputes the current batch; it does not consume a previously printed plan. It rechecks exact source, revision, source-content hash, policy and empty count at queue insertion. Existing queue generations are never reset (`ON CONFLICT DO NOTHING`). Concurrent changes can make `queued` smaller than `eligible_documents`. No passage is changed by this command itself.

Use `--after "$LAST_DOCUMENT"` with the report's `next_after` to inspect subsequent batches in document-ID order. The per-call limit is a work boundary, not a total corpus ceiling. Retain each report and monitor worker/outbox progress between batches; newly changed documents before the cursor need a later pass. A terminal empty batch means no selected candidates in that pass, not that the whole source is healthy.

After the first batch, verify exact expected passages/receipts, unchanged logical evidence and no invented actor links. Follow the item through search, show/context and scan after their normal outboxes drain. Check other sources still progress. Run the original acceptance checks unchanged. Do not repeatedly requeue when an outcome is unknown; inspect the existing queue and projection first.

This repairs only previously empty current documents. Roleless messages omitted inside already-nonempty mixed documents require a separate measured inventory and are not covered by this command. Do not bump the global passage fingerprint, rebuild all logical evidence, or infer full source recovery from a sample.

`--concurrency 4` opts into four concurrent read-only document comparisons. The
CLI and API default remains `1`. Results retain document-ID order and identical
eligibility/cost estimates. No database connection is held during archive reads;
any comparison failure aborts the batch before queue insertion. Concurrency
bounds active document preparations, not total corpus coverage. Measure archive
latency, memory and worker drainage before increasing concurrent work.
