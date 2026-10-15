# Canonical ingestion metadata

New canonical events reuse one connector-lineage row in `canonical_ingest_jobs` for each tenant, source and connector. Previously each event created a separate committed job and an `ingest.commit` audit row, even in a batch. Neither row owned delivery retries or receipt deduplication.

The lineage job ID uses the existing opaque hash with `connector-lineage-v1` as its final input. Legacy IDs used an `evt_*` identifier there, so the input namespaces are distinct. `attempt=0` denotes lineage registration rather than an execution attempt. The existing schema's `incremental`/`committed` values identify registered ingestion lineage; they are not per-event scheduling or completion records. `created_at` and `updated_at` remain equal to the registration time. Ingestion never updates that shared row.

The writer checks every reused row's connector, mode, status, attempt and immutable timestamps under a shared row lock. Unexpected metadata fails the canonical transaction with `canonical_job_conflict`. Concurrent admissions can share the row; a conflicting update cannot change it before the event commits. Canonical events retain their job foreign key, including the connector identity used to authorize forgetting a Slack delete-before-original receipt.

Ordinary success no longer creates a separate `ingest.commit` audit. Canonical events, raw artifacts and receipts retain the committed record. Slack suppression and successful or failed forget operations still produce their existing audit records. Suppression ownership and permanent forget fences remain independent of audit retention.

Native identities, event/document/chunk IDs, revision assignment, content-hash deduplication, receipt order and acknowledgement counts are unchanged. Existing event-to-job associations and historical audit rows are not rewritten or deleted. Older binaries can still read both forms of job and replay canonical identities; rolling back resumes the old growth pattern without a data migration.

This change stops ordinary ingestion from adding per-event job and audit rows. It does not reclaim existing tables, remove an index, move historical records, or establish a database size target. Historical metadata reclamation requires separate ownership and archive proofs.
