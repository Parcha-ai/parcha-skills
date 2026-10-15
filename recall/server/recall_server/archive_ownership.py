"""Coordinate archive admission with the existing artifact deletion guard."""

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time

from .archive import S3_DEADLINE_CONNECT_TIMEOUT

logger = logging.getLogger(__name__)

REFERENCE_FIELDS = (
    "tenant_id",
    "source_id",
    "artifact_id",
    "storage_backend",
    "object_key",
    "content_sha256",
    "size_bytes",
    "media_type",
    "encryption",
    "version_id",
)


def reference_key(reference):
    return tuple(reference[name] for name in REFERENCE_FIELDS)


def delete_persisted_reference(archive, reference):
    """Bound the HEAD and DELETE while the caller holds the artifact guard."""
    seconds = 2 * (
        S3_DEADLINE_CONNECT_TIMEOUT
        + getattr(archive, "deadline_read_timeout_seconds", 0.5)
    ) + 0.25
    return archive.delete_internal_raw_bounded(
        reference, deadline_at=time.monotonic() + seconds
    )


def live_references(connection, references, *, excluding_evidence_documents=()):
    """Return exact live descriptors; a deleted revision cannot prove existence."""
    if not references:
        return set()
    rows = connection.execute(
        """WITH targets AS (
        SELECT * FROM unnest(%s::text[],%s::text[],%s::text[])
            AS input(tenant_id,source_id,artifact_id))
        SELECT owner.tenant_id,owner.source_id,owner.artifact_id,
            owner.storage_backend,owner.object_key,owner.content_sha256,
            owner.size_bytes,owner.media_type,owner.encryption,owner.version_id
        FROM raw_artifacts owner JOIN targets USING(tenant_id,source_id,artifact_id)
        WHERE owner.state='live'
        UNION ALL
        SELECT owner.tenant_id,owner.source_id,owner.manifest_artifact_id,
            owner.manifest_storage_backend,owner.manifest_object_key,owner.manifest_content_sha256,
            owner.manifest_size_bytes,owner.manifest_media_type,owner.manifest_encryption,owner.manifest_version_id
        FROM canonical_evidence_documents owner JOIN targets target
            ON target.tenant_id=owner.tenant_id AND target.source_id=owner.source_id
            AND target.artifact_id=owner.manifest_artifact_id
        UNION ALL
        SELECT owner.tenant_id,owner.source_id,owner.artifact_id,
            owner.storage_backend,owner.object_key,owner.content_sha256,
            owner.size_bytes,owner.media_type,owner.encryption,owner.version_id
        FROM canonical_evidence_document_parts owner JOIN targets USING(tenant_id,source_id,artifact_id)
        UNION ALL
        SELECT owner.tenant_id,owner.source_id,owner.artifact_id,
            owner.storage_backend,owner.object_key,owner.content_sha256,
            owner.size_bytes,owner.media_type,owner.encryption,owner.version_id
        FROM canonical_parquet_scan_shards owner JOIN targets USING(tenant_id,source_id,artifact_id)
        UNION ALL
        SELECT owner.tenant_id,owner.source_id,owner.artifact_id,
            owner.storage_backend,owner.object_key,owner.content_sha256,
            owner.size_bytes,owner.media_type,owner.encryption,owner.version_id
        FROM canonical_evidence_objects owner JOIN targets USING(tenant_id,source_id,artifact_id)
        JOIN canonical_documents document USING(tenant_id,source_id,document_id)
        WHERE document.deleted_at IS NULL
          AND NOT (owner.document_id=ANY(%s::text[]))""",
        (
            [r["tenant_id"] for r in references],
            [r["source_id"] for r in references],
            [r["artifact_id"] for r in references],
            list(excluding_evidence_documents),
        ),
    ).fetchall()
    return {reference_key(row) for row in rows}


def admit_references(connection, archive, references):
    """Keep one transaction lease while checking only new, deduplicated objects.

    Existing callers retain source/native authorization and their final catalogue
    checks. Every physical deletion owner must use the same artifact guard.
    """
    from .canonical import CanonicalLifecycleError, _guard_artifact_admission

    started = time.monotonic()
    occurrences = live_reused = new_references = 0
    head_wall_ms = 0.0
    outcome = "error"
    unique = {}
    try:
        for reference in references:
            occurrences += 1
            key = tuple(
                reference[name] for name in ("tenant_id", "source_id", "artifact_id")
            )
            if key in unique and reference_key(unique[key]) != reference_key(reference):
                raise CanonicalLifecycleError("canonical_artifact_conflict")
            unique[key] = reference
        groups = {}
        for tenant, source, artifact in sorted(unique):
            groups.setdefault((tenant, source), []).append(artifact)
        for (tenant, source), artifacts in groups.items():
            _guard_artifact_admission(connection, tenant, source, artifacts)
        references = list(unique.values())
        live = live_references(connection, references)
        pending = [
            reference for reference in references if reference_key(reference) not in live
        ]

        live_reused = len(references) - len(pending)
        new_references = len(pending)

        def verify(reference):
            # Per-request SDK timeouts bound failed checks. Total successful work
            # scales with new objects, not events; no batch-size rejection is added.
            seconds = (
                S3_DEADLINE_CONNECT_TIMEOUT
                + getattr(archive, "deadline_read_timeout_seconds", 0.5)
                + 0.25
            )
            archive.verify_raw_bounded(reference, deadline_at=time.monotonic() + seconds)

        head_started = time.monotonic()
        try:
            if len(pending) == 1:
                verify(pending[0])
            elif pending:
                with ThreadPoolExecutor(
                    max_workers=4, thread_name_prefix="recall-admit-head"
                ) as pool:
                    futures = [pool.submit(verify, reference) for reference in pending]
                    try:
                        for future in as_completed(futures):
                            future.result()
                    except BaseException:
                        for future in futures:
                            future.cancel()
                        raise
        finally:
            head_wall_ms = (time.monotonic() - head_started) * 1000 if pending else 0.0
        outcome = "ok"
        return len(pending)
    finally:
        logger.info(
            "archive_admission occurrences=%d unique=%d live_reused=%d "
            "new_references=%d head_wall_ms=%.3f helper_elapsed_ms=%.3f outcome=%s",
            occurrences,
            len(unique),
            live_reused,
            new_references,
            head_wall_ms,
            (time.monotonic() - started) * 1000,
            outcome,
        )
