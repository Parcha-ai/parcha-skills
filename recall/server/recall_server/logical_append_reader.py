"""Attempt-local assembly of an exactly bound committed prefix."""

from __future__ import annotations

import hashlib
from datetime import datetime

from .logical_evidence import (
    LogicalEvidenceManifest,
    LogicalEvidenceUpload,
    ManifestPart,
    PreparedPart,
    StreamedPreparedLogicalDocument,
)


def eligible(candidate, manifest, queued):
    if not manifest or not queued:
        return False
    frontier = manifest.get("append_source_ordinal")
    changed = queued.get("changed_at")
    return (
        type(frontier) is int
        and isinstance(changed, str)
        and (
            manifest.get("tenant_id"),
            manifest.get("source_id"),
            manifest.get("native_parent_id"),
        )
        == (candidate.tenant_id, candidate.source_id, candidate.native_parent_id)
        and queued.get("reason") == "ingest"
        and queued.get("generation") == candidate.generation
        and datetime.fromisoformat(changed) == candidate.source_updated_at
        and queued.get("append_generation") == candidate.generation
        and queued.get("append_base_manifest_id")
        == manifest.get("manifest_artifact_id")
        and manifest.get("append_manifest_id") == manifest.get("manifest_artifact_id")
        and manifest.get("revision") == candidate.revision - 1
    )


def from_catalog(
    candidate, manifest, parts, *, reference, timestamp, retention_profile
):
    def stamp(value):
        return timestamp(
            datetime.fromisoformat(value) if isinstance(value, str) else value
        )

    references = tuple(reference(part) for part in parts)
    prepared_parts = tuple(
        PreparedPart(
            ordinal=part["part_ordinal"],
            first_record_ordinal=part["first_record_ordinal"],
            last_record_ordinal=part["last_record_ordinal"],
            first_occurred_at=stamp(part["first_occurred_at"]),
            last_occurred_at=stamp(part["last_occurred_at"]),
            receipt_count=part["receipt_count"],
            payload=b"",
            content_sha256=part["content_sha256"],
        )
        for part in parts
    )
    values = dict(
        logical_document_id=manifest["logical_document_id"],
        evidence_id=manifest["evidence_id"],
        revision=manifest["revision"],
        document_content_sha256=manifest["document_content_sha256"],
        first_occurred_at=stamp(manifest["first_occurred_at"]),
        last_occurred_at=stamp(manifest["last_occurred_at"]),
        record_count=manifest["record_count"],
        receipt_count=manifest["receipt_count"],
    )
    archived_manifest = LogicalEvidenceManifest(
        **values,
        native_parent_sha256=hashlib.sha256(
            candidate.native_parent_id.encode()
        ).hexdigest(),
        retention_profile=retention_profile,
        parts=tuple(
            ManifestPart(
                ordinal=part["part_ordinal"],
                artifact_id=part["artifact_id"],
                object_key=part["object_key"],
                content_sha256=part["content_sha256"],
                size_bytes=part["size_bytes"],
                media_type=part["media_type"],
                version_id=part["version_id"],
                first_record_ordinal=part["first_record_ordinal"],
                last_record_ordinal=part["last_record_ordinal"],
                receipt_count=part["receipt_count"],
            )
            for part in parts
        ),
    )
    prepared = StreamedPreparedLogicalDocument(
        **values,
        tenant_id=candidate.tenant_id,
        source_id=candidate.source_id,
        native_parent_id=candidate.native_parent_id,
        parts=prepared_parts,
    )
    manifest_reference = reference(
        dict(manifest, manifest_created_at=values["first_occurred_at"]),
        prefix="manifest_",
    )
    return LogicalEvidenceUpload(
        prepared=prepared,
        manifest=archived_manifest,
        manifest_reference=manifest_reference,
        part_references=references,
        created_part_references=(),
    )
