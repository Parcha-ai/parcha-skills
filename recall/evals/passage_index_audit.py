"""Aggregate-only integrity audit for the lossless passage hint index.

The auditor may read private passage bodies and S3 evidence in memory, but its
report contains only counts, timings, hashes, and pass/fail invariants.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from recall_server.archive_runtime import build_evidence_archive_store
from recall_server.db import BrainStore
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.passage_index import (
    CanonicalPassageProjector,
    PassageCandidate,
)
from recall_server.passage_projection import PassagePolicy
from recall_server.semantic import SemanticRuntime

from .runner import git_dirty, git_sha


SCHEMA_VERSION = "recall.lossless-passage-audit.v1"


class PassageIndexAuditError(RuntimeError):
    """A passage index invariant failed without exposing private content."""


def _choose_sample(
    rows: list[dict[str, Any]],
    *,
    sample_size: int,
    seed: str,
) -> list[dict[str, Any]]:
    if not 1 <= sample_size <= len(rows):
        raise PassageIndexAuditError("passage sample size is invalid")
    cells: list[list[dict[str, Any]]] = []
    sources = sorted({row["source_id"] for row in rows})
    for source_id in sources:
        source_rows = sorted(
            (row for row in rows if row["source_id"] == source_id),
            key=lambda row: (int(row["token_count"]), row["passage_id"]),
        )
        buckets: list[list[dict[str, Any]]] = [[], [], [], []]
        for index, row in enumerate(source_rows):
            buckets[min(3, index * 4 // len(source_rows))].append(row)
        cells.extend(bucket for bucket in buckets if bucket)
    for cell in cells:
        cell.sort(
            key=lambda row: hashlib.sha256(
                f"{seed}\0{row['passage_id']}".encode()
            ).digest()
        )
    selected: list[dict[str, Any]] = []
    cursor = 0
    while len(selected) < sample_size and cells:
        cell = cells[cursor % len(cells)]
        cursor += 1
        if not cell:
            cells.remove(cell)
            cursor = 0
            continue
        selected.append(cell.pop(0))
    if len(selected) != sample_size:
        raise PassageIndexAuditError("passage sample is incomplete")
    return selected


def _span_coverage(
    documents: list[dict[str, Any]],
    passages: list[dict[str, Any]],
) -> dict[str, int]:
    """Prove passage spans cover every byte declared by each dense message."""

    expected = {
        (
            row["tenant_id"],
            row["source_id"],
            row["logical_document_id"],
        ): {
            "messages": int(row["dense_message_count"]),
            "bytes": int(row["dense_message_bytes"]),
            "passages": int(row["passage_count"]),
        }
        for row in documents
    }
    actual_passages: dict[tuple[str, str, str], int] = {}
    intervals: dict[
        tuple[str, str, str, int, int],
        list[tuple[int, int]],
    ] = {}
    for passage in passages:
        document = (
            passage["tenant_id"],
            passage["source_id"],
            passage["logical_document_id"],
        )
        if document not in expected:
            raise PassageIndexAuditError("passage has no projected document")
        actual_passages[document] = actual_passages.get(document, 0) + 1
        spans = passage["spans"]
        if not isinstance(spans, list) or not spans:
            raise PassageIndexAuditError("passage spans are invalid")
        for span in spans:
            try:
                ordinal = int(span["record_ordinal"])
                record_count = int(span["record_count"])
                start = int(span["source_byte_start"])
                end = int(span["source_byte_end"])
            except (KeyError, TypeError, ValueError) as error:
                raise PassageIndexAuditError(
                    "passage span contract is invalid"
                ) from error
            if (
                ordinal < 0
                or record_count < 1
                or start < 0
                or end <= start
            ):
                raise PassageIndexAuditError(
                    "passage span boundary is invalid"
                )
            intervals.setdefault(
                (*document, ordinal, record_count),
                [],
            ).append((start, end))

    messages_by_document: dict[tuple[str, str, str], int] = {}
    bytes_by_document: dict[tuple[str, str, str], int] = {}
    for message, values in intervals.items():
        document = message[:3]
        ordered = sorted(values)
        cursor = 0
        for start, end in ordered:
            if start > cursor:
                raise PassageIndexAuditError(
                    "dense message contains an uncovered byte range"
                )
            cursor = max(cursor, end)
        messages_by_document[document] = (
            messages_by_document.get(document, 0) + 1
        )
        bytes_by_document[document] = (
            bytes_by_document.get(document, 0) + cursor
        )

    for document, counts in expected.items():
        if (
            actual_passages.get(document, 0) != counts["passages"]
            or messages_by_document.get(document, 0) != counts["messages"]
            or bytes_by_document.get(document, 0) != counts["bytes"]
        ):
            raise PassageIndexAuditError(
                "passage coverage disagrees with projected document"
            )
    return {
        "documents": len(expected),
        "messages": sum(messages_by_document.values()),
        "message_bytes": sum(bytes_by_document.values()),
        "passages": sum(actual_passages.values()),
        "uncovered_bytes": 0,
    }


def _artifact_reference(
    row: dict[str, Any],
    *,
    prefix: str,
) -> dict[str, Any]:
    created_at = row[prefix + "created_at"]
    if isinstance(created_at, datetime):
        created_at = created_at.isoformat()
    return {
        "contract": "recall.artifact-ref.v1",
        "schema_version": 1,
        "tenant_id": row["tenant_id"],
        "source_id": row["source_id"],
        "artifact_id": row[prefix + "artifact_id"],
        "storage_backend": row[prefix + "storage_backend"],
        "object_key": row[prefix + "object_key"],
        "content_sha256": row[prefix + "content_sha256"],
        "size_bytes": row[prefix + "size_bytes"],
        "media_type": row[prefix + "media_type"],
        "encryption": row[prefix + "encryption"],
        "version_id": row[prefix + "version_id"],
        "created_at": created_at,
    }


def _candidates(
    store: BrainStore,
    sample: list[dict[str, Any]],
) -> dict[tuple[str, str, str], PassageCandidate]:
    identities = sorted({
        (
            row["tenant_id"],
            row["source_id"],
            row["logical_document_id"],
        )
        for row in sample
    })
    with store.connect() as connection:
        rows = connection.execute(
            """WITH selected(tenant_id,source_id,logical_document_id) AS (
                   SELECT * FROM unnest(%s::text[],%s::text[],%s::text[])
               )
               SELECT evidence.tenant_id,evidence.source_id,
                      evidence.logical_document_id,evidence.revision,
                      evidence.document_content_sha256,
                      evidence.manifest_artifact_id,
                      evidence.manifest_storage_backend,
                      evidence.manifest_object_key,
                      evidence.manifest_content_sha256,
                      evidence.manifest_size_bytes,
                      evidence.manifest_media_type,
                      evidence.manifest_encryption,
                      evidence.manifest_version_id,
                      evidence.created_at AS manifest_created_at,
                      part.part_ordinal,
                      part.artifact_id AS part_artifact_id,
                      part.storage_backend AS part_storage_backend,
                      part.object_key AS part_object_key,
                      part.content_sha256 AS part_content_sha256,
                      part.size_bytes AS part_size_bytes,
                      part.media_type AS part_media_type,
                      part.encryption AS part_encryption,
                      part.version_id AS part_version_id,
                      part.created_at AS part_created_at
                 FROM selected
                 JOIN canonical_evidence_documents evidence
                   USING(tenant_id,source_id,logical_document_id)
                 JOIN canonical_evidence_document_parts part
                   USING(tenant_id,source_id,logical_document_id,revision)
                ORDER BY evidence.tenant_id,evidence.source_id,
                         evidence.logical_document_id,part.part_ordinal""",
            (
                [value[0] for value in identities],
                [value[1] for value in identities],
                [value[2] for value in identities],
            ),
        ).fetchall()
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        identity = (
            row["tenant_id"],
            row["source_id"],
            row["logical_document_id"],
        )
        grouped.setdefault(identity, []).append(row)
    if set(grouped) != set(identities):
        raise PassageIndexAuditError("sample evidence catalog is incomplete")
    now = datetime.now(timezone.utc)
    return {
        identity: PassageCandidate(
            tenant_id=values[0]["tenant_id"],
            source_id=values[0]["source_id"],
            logical_document_id=values[0]["logical_document_id"],
            revision=int(values[0]["revision"]),
            generation=1,
            changed_at=now,
            source_document_sha256=values[0]["document_content_sha256"],
            manifest_reference=_artifact_reference(
                values[0],
                prefix="manifest_",
            ),
            part_references=tuple(
                _artifact_reference(value, prefix="part_")
                for value in values
            ),
        )
        for identity, values in grouped.items()
    }


def _sample_details(
    store: BrainStore,
    sample: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    with store.connect() as connection:
        rows = connection.execute(
            """WITH selected(tenant_id,source_id,passage_id) AS (
                   SELECT * FROM unnest(%s::text[],%s::text[],%s::text[])
               )
               SELECT passage.tenant_id,passage.source_id,
                      passage.logical_document_id,passage.revision,
                      passage.passage_id,passage.ordinal,
                      passage.policy_fingerprint,passage.token_count,
                      passage.first_occurred_at,passage.last_occurred_at,
                      passage.roles,passage.receipts,passage.spans,
                      passage.text_redacted,passage.text_sha256
                 FROM selected
                 JOIN canonical_passages passage
                   USING(tenant_id,source_id,passage_id)""",
            (
                [row["tenant_id"] for row in sample],
                [row["source_id"] for row in sample],
                [row["passage_id"] for row in sample],
            ),
        ).fetchall()
    catalog = {
        (row["tenant_id"], row["source_id"], row["passage_id"]): row
        for row in rows
    }
    ordered = [
        catalog.get(
            (row["tenant_id"], row["source_id"], row["passage_id"])
        )
        for row in sample
    ]
    if any(row is None for row in ordered):
        raise PassageIndexAuditError("sample passage catalog is incomplete")
    return ordered  # type: ignore[return-value]


def _exact_sample(
    store: BrainStore,
    projection: LogicalEvidenceProjectionStore,
    sample: list[dict[str, Any]],
    *,
    policy: PassagePolicy,
    tenant_id: str,
) -> int:
    projector = CanonicalPassageProjector(
        store,
        projection,
        policy=policy,
        bound_tenant_id=tenant_id,
    )
    candidates = _candidates(store, sample)
    expected_by_document = {
        identity: {
            passage.passage_id: passage
            for passage in projector._prepare(candidate).passages
        }
        for identity, candidate in candidates.items()
    }
    exact = 0
    for stored in sample:
        identity = (
            stored["tenant_id"],
            stored["source_id"],
            stored["logical_document_id"],
        )
        expected = expected_by_document[identity].get(stored["passage_id"])
        if expected is None:
            raise PassageIndexAuditError(
                "sample passage does not reconstruct from S3"
            )
        stored_time = (
            stored["first_occurred_at"].isoformat(),
            stored["last_occurred_at"].isoformat(),
        )
        expected_time = (
            datetime.fromisoformat(
                expected.first_occurred_at.replace("Z", "+00:00")
            ).isoformat(),
            datetime.fromisoformat(
                expected.last_occurred_at.replace("Z", "+00:00")
            ).isoformat(),
        )
        if (
            int(stored["revision"]) != expected.revision
            or int(stored["ordinal"]) != expected.ordinal
            or stored["policy_fingerprint"] != expected.policy_fingerprint
            or int(stored["token_count"]) != expected.token_count
            or stored_time != expected_time
            or tuple(stored["roles"]) != expected.roles
            or tuple(stored["receipts"]) != expected.receipts
            or stored["spans"]
            != [asdict(span) for span in expected.spans]
            or stored["text_redacted"] != expected.text
            or stored["text_sha256"] != expected.text_sha256
        ):
            raise PassageIndexAuditError(
                "sample passage differs from S3 reconstruction"
            )
        exact += 1
    return exact


def audit_passage_index(
    store: BrainStore,
    projection: LogicalEvidenceProjectionStore,
    *,
    tenant_id: str,
    policy: PassagePolicy,
    sample_size: int = 500,
    seed: str = "recall-lossless-passages-v1",
    baseline_vectors: int = 4_963_863,
) -> dict[str, Any]:
    if not tenant_id or baseline_vectors < 1:
        raise PassageIndexAuditError("passage audit arguments are invalid")
    with store.connect() as connection:
        documents = connection.execute(
            """SELECT tenant_id,source_id,logical_document_id,
                      dense_message_count,dense_message_bytes,passage_count
                 FROM canonical_passage_documents
                WHERE tenant_id=%s AND policy_fingerprint=%s
                ORDER BY source_id,logical_document_id""",
            (tenant_id, policy.fingerprint),
        ).fetchall()
        passage_rows = connection.execute(
            """SELECT tenant_id,source_id,logical_document_id,
                      passage_id,token_count,spans
                 FROM canonical_passages
                WHERE tenant_id=%s AND policy_fingerprint=%s
                ORDER BY source_id,logical_document_id,ordinal""",
            (tenant_id, policy.fingerprint),
        ).fetchall()
        runtime = store.semantic_runtime
        embedding_count = connection.execute(
            """SELECT count(*) AS count
                 FROM canonical_passage_embeddings embedding
                 JOIN canonical_passages passage
                   USING(tenant_id,source_id,passage_id)
                WHERE passage.tenant_id=%s
                  AND passage.policy_fingerprint=%s
                  AND embedding.runtime_fingerprint=%s
                  AND embedding.content_sha256=passage.text_sha256""",
            (
                tenant_id,
                policy.fingerprint,
                runtime.passage_fingerprint if runtime else "",
            ),
        ).fetchone()["count"]
    if not documents or not passage_rows:
        raise PassageIndexAuditError("passage index is empty")

    started = time.monotonic()
    coverage = _span_coverage(documents, passage_rows)
    sample = _choose_sample(
        passage_rows,
        sample_size=sample_size,
        seed=seed,
    )
    sample = _sample_details(store, sample)
    exact = _exact_sample(
        store,
        projection,
        sample,
        policy=policy,
        tenant_id=tenant_id,
    )
    passage_count = len(passage_rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pass",
        "policy": {
            "fingerprint": policy.fingerprint,
            "target_tokens": policy.target_tokens,
            "overlap_tokens": policy.overlap_tokens,
        },
        "integrity": {
            "requested_passages": sample_size,
            "exact_s3_reconstructions": exact,
            "source_count": len({row["source_id"] for row in sample}),
        },
        "coverage": coverage,
        "embeddings": {
            "expected": passage_count,
            "current": int(embedding_count),
            "complete": int(embedding_count) == passage_count,
        },
        "compression": {
            "baseline_vectors": baseline_vectors,
            "passage_vectors": passage_count,
            "ratio": round(baseline_vectors / passage_count, 6),
        },
        "completion_model_calls": 0,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "seed_sha256": hashlib.sha256(seed.encode()).hexdigest(),
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(prog="recall-passage-index-audit")
    value.add_argument("--dsn", default=os.environ.get("RECALL_DATABASE_URL"))
    value.add_argument("--tenant", required=True)
    value.add_argument("--target-tokens", type=int, default=1024)
    value.add_argument("--overlap-tokens", type=int, default=128)
    value.add_argument("--sample-size", type=int, default=500)
    value.add_argument("--seed", default="recall-lossless-passages-v1")
    value.add_argument("--baseline-vectors", type=int, default=4_963_863)
    value.add_argument("--repo-root", required=True)
    return value


def main() -> None:
    args = parser().parse_args()
    if not args.dsn:
        raise SystemExit("RECALL_DATABASE_URL or --dsn is required")
    store = BrainStore(
        args.dsn,
        semantic_runtime=SemanticRuntime.from_env(),
        pool_max_size=8,
    )
    try:
        report = audit_passage_index(
            store,
            LogicalEvidenceProjectionStore(
                build_evidence_archive_store()
            ),
            tenant_id=args.tenant,
            policy=PassagePolicy(
                target_tokens=args.target_tokens,
                overlap_tokens=args.overlap_tokens,
            ),
            sample_size=args.sample_size,
            seed=args.seed,
            baseline_vectors=args.baseline_vectors,
        )
    finally:
        store.close()
    root = Path(args.repo_root).resolve(strict=True)
    report["pins"] = {
        "git_sha": git_sha(root),
        "git_dirty": git_dirty(root),
        "auditor_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
