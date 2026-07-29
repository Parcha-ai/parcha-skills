from __future__ import annotations

import hashlib
import json
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from urllib.parse import urlsplit

EVIDENCE_ID_RE = re.compile(r"evd_[0-9a-f]{32}\Z")
DOCUMENT_ID_RE = re.compile(r"doc_[0-9a-f]{32}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
MAX_EVIDENCE_CHUNKS = 10_000
MAX_EVIDENCE_TEXT_BYTES = 8_000_000


class EvidenceProjectionError(ValueError):
    """Stable failure that never renders source content."""


class EvidenceArchive(Protocol):
    def put_raw(
        self,
        *,
        tenant_id: str,
        source_id: str,
        native_id: str,
        payload: bytes,
        media_type: str,
        created_at: str,
    ) -> dict[str, Any]: ...

    def read_raw(self, value: dict[str, Any]) -> bytes: ...
    def delete_raw(self, value: dict[str, Any]) -> bool: ...


def _timestamp(value: str) -> str:
    if not isinstance(value, str) or len(value) > 64:
        raise EvidenceProjectionError("evidence_timestamp_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise EvidenceProjectionError("evidence_timestamp_invalid") from None
    if parsed.tzinfo is None:
        raise EvidenceProjectionError("evidence_timestamp_invalid")
    return value


def _receipt(value: str) -> str:
    if not isinstance(value, str) or len(value) > 2048:
        raise EvidenceProjectionError("evidence_receipt_invalid")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "recall"
        or not parsed.netloc
        or not parsed.path
        or parsed.query == ""
        or not parsed.fragment.startswith("item=")
    ):
        raise EvidenceProjectionError("evidence_receipt_invalid")
    return value


@dataclass(frozen=True)
class EvidenceChunk:
    ordinal: int
    receipt: str
    text: str

    def validate(self) -> None:
        if (
            isinstance(self.ordinal, bool)
            or not isinstance(self.ordinal, int)
            or self.ordinal < 0
        ):
            raise EvidenceProjectionError("evidence_chunk_invalid")
        _receipt(self.receipt)
        if (
            not isinstance(self.text, str)
            or len(self.text.encode()) > MAX_EVIDENCE_TEXT_BYTES
        ):
            raise EvidenceProjectionError("evidence_chunk_invalid")


@dataclass(frozen=True)
class EvidenceBundle:
    evidence_id: str
    revision: int
    occurred_at: str
    session_sha256: str
    text_sha256: str
    chunks: tuple[EvidenceChunk, ...]

    def validate(self) -> None:
        if not isinstance(self.evidence_id, str) or not EVIDENCE_ID_RE.fullmatch(
            self.evidence_id
        ):
            raise EvidenceProjectionError("evidence_identity_invalid")
        if (
            isinstance(self.revision, bool)
            or not isinstance(self.revision, int)
            or self.revision < 1
        ):
            raise EvidenceProjectionError("evidence_revision_invalid")
        _timestamp(self.occurred_at)
        if (
            not isinstance(self.session_sha256, str)
            or not SHA256_RE.fullmatch(self.session_sha256)
            or not isinstance(self.text_sha256, str)
            or not SHA256_RE.fullmatch(self.text_sha256)
        ):
            raise EvidenceProjectionError("evidence_digest_invalid")
        if (
            not isinstance(self.chunks, tuple)
            or not 1 <= len(self.chunks) <= MAX_EVIDENCE_CHUNKS
        ):
            raise EvidenceProjectionError("evidence_chunks_invalid")
        expected = list(range(len(self.chunks)))
        actual = [chunk.ordinal for chunk in self.chunks]
        if actual != expected:
            raise EvidenceProjectionError("evidence_chunks_invalid")
        for chunk in self.chunks:
            chunk.validate()
        full_text = "".join(chunk.text for chunk in self.chunks)
        if len(full_text.encode()) > MAX_EVIDENCE_TEXT_BYTES:
            raise EvidenceProjectionError("evidence_text_invalid")
        if not __import__("hmac").compare_digest(
            hashlib.sha256(full_text.encode()).hexdigest(),
            self.text_sha256,
        ):
            raise EvidenceProjectionError("evidence_digest_invalid")

    def encode(self) -> bytes:
        self.validate()
        return json.dumps(
            {
                "contract": "recall.evidence-bundle.v1",
                "schema_version": 1,
                "evidence_id": self.evidence_id,
                "revision": self.revision,
                "occurred_at": self.occurred_at,
                "session_sha256": self.session_sha256,
                "text_sha256": self.text_sha256,
                "chunks": [
                    {
                        "ordinal": chunk.ordinal,
                        "receipt": chunk.receipt,
                        "text": chunk.text,
                    }
                    for chunk in self.chunks
                ],
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()

    @classmethod
    def decode(cls, payload: bytes) -> EvidenceBundle:
        if (
            not isinstance(payload, bytes)
            or len(payload) > MAX_EVIDENCE_TEXT_BYTES + 1_000_000
        ):
            raise EvidenceProjectionError("evidence_payload_invalid")
        try:
            value = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise EvidenceProjectionError("evidence_payload_invalid") from None
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "contract",
                "schema_version",
                "evidence_id",
                "revision",
                "occurred_at",
                "session_sha256",
                "text_sha256",
                "chunks",
            }
            or value.get("contract") != "recall.evidence-bundle.v1"
            or value.get("schema_version") != 1
            or not isinstance(value.get("chunks"), list)
        ):
            raise EvidenceProjectionError("evidence_payload_invalid")
        try:
            bundle = cls(
                evidence_id=value["evidence_id"],
                revision=value["revision"],
                occurred_at=value["occurred_at"],
                session_sha256=value["session_sha256"],
                text_sha256=value["text_sha256"],
                chunks=tuple(
                    EvidenceChunk(
                        ordinal=chunk["ordinal"],
                        receipt=chunk["receipt"],
                        text=chunk["text"],
                    )
                    for chunk in value["chunks"]
                    if isinstance(chunk, dict)
                    and set(chunk) == {"ordinal", "receipt", "text"}
                ),
            )
        except (KeyError, TypeError):
            raise EvidenceProjectionError("evidence_payload_invalid") from None
        if len(bundle.chunks) != len(value["chunks"]):
            raise EvidenceProjectionError("evidence_payload_invalid")
        bundle.validate()
        return bundle


class EvidenceProjectionStore:
    """Full, privacy-processed evidence objects in a separate private archive."""

    def __init__(self, archive: EvidenceArchive):
        self.archive = archive

    def put(
        self,
        *,
        tenant_id: str,
        source_id: str,
        document_id: str,
        bundle: EvidenceBundle,
    ) -> dict[str, Any]:
        if not isinstance(document_id, str) or not DOCUMENT_ID_RE.fullmatch(
            document_id
        ):
            raise EvidenceProjectionError("evidence_document_invalid")
        bundle.validate()
        return self.archive.put_raw(
            tenant_id=tenant_id,
            source_id=source_id,
            native_id=(
                f"evidence:{document_id}:{bundle.revision}:{bundle.text_sha256[:16]}"
            ),
            payload=bundle.encode(),
            media_type="application/vnd.recall.evidence+json",
            created_at=bundle.occurred_at,
        )

    def read(
        self,
        reference: dict[str, Any],
        *,
        tenant_id: str,
        source_id: str,
    ) -> EvidenceBundle:
        if (
            not isinstance(reference, dict)
            or reference.get("tenant_id") != tenant_id
            or reference.get("source_id") != source_id
        ):
            raise EvidenceProjectionError("evidence object not found")
        try:
            payload = self.archive.read_raw(reference)
        except Exception as error:
            if error.__class__.__name__ == "ArchiveNotFound":
                raise EvidenceProjectionError("evidence object not found") from None
            raise
        return EvidenceBundle.decode(payload)

    def delete(self, reference: dict[str, Any]) -> bool:
        return self.archive.delete_raw(reference)


class CanonicalEvidenceProjector:
    """Idempotently materialize current canonical documents for deep inspection."""

    def __init__(
        self,
        store: Any,
        projection: EvidenceProjectionStore,
        *,
        bound_tenant_id: str | None = None,
    ):
        if bound_tenant_id is not None and (
            not isinstance(bound_tenant_id, str)
            or not bound_tenant_id.strip()
            or len(bound_tenant_id) > 255
        ):
            raise EvidenceProjectionError("evidence_tenant_invalid")
        self.store = store
        self.projection = projection
        self.bound_tenant_id = bound_tenant_id

    def _tenant(self, tenant_id: str | None) -> str | None:
        if self.bound_tenant_id is None:
            return tenant_id
        if tenant_id is not None and tenant_id != self.bound_tenant_id:
            raise EvidenceProjectionError("evidence_tenant_not_configured")
        return self.bound_tenant_id

    @staticmethod
    def _reference(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "contract": "recall.artifact-ref.v1",
            "schema_version": 1,
            **{
                key: row[key]
                for key in (
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
            },
            "created_at": (
                row["created_at"].isoformat()
                if isinstance(row["created_at"], datetime)
                else str(row["created_at"])
            ),
        }

    @staticmethod
    def _bundle(row: dict[str, Any], chunks: list[dict[str, Any]]) -> EvidenceBundle:
        session = row["native_parent_id"] or row["native_id"]
        bundle = EvidenceBundle(
            evidence_id="evd_"
            + hashlib.sha256(
                "\x1f".join(
                    (
                        row["tenant_id"],
                        row["source_id"],
                        row["document_id"],
                        row["text_sha256"],
                    )
                ).encode()
            ).hexdigest()[:32],
            revision=row["revision"],
            occurred_at=(
                row["occurred_at"].isoformat()
                if isinstance(row["occurred_at"], datetime)
                else str(row["occurred_at"])
            ),
            session_sha256=hashlib.sha256(
                "\x1f".join((row["tenant_id"], row["source_id"], session)).encode()
            ).hexdigest(),
            text_sha256=row["text_sha256"],
            chunks=tuple(
                EvidenceChunk(
                    ordinal=chunk["ordinal"],
                    receipt=chunk["receipt"],
                    text=chunk["text_redacted"],
                )
                for chunk in chunks
            ),
        )
        bundle.validate()
        return bundle

    def project_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 100,
        max_batches: int = 10,
        upload_concurrency: int = 16,
    ) -> dict[str, int | str]:
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= 1000
            or isinstance(max_batches, bool)
            or not isinstance(max_batches, int)
            or not 1 <= max_batches <= 100
            or isinstance(upload_concurrency, bool)
            or not isinstance(upload_concurrency, int)
            or not 1 <= upload_concurrency <= 64
        ):
            raise EvidenceProjectionError("evidence_projection_budget_invalid")
        tenant_id = self._tenant(tenant_id)
        processed = batches = 0
        for _ in range(max_batches):
            with self.store.connect() as connection:
                rows = connection.execute(
                    """SELECT document.tenant_id,document.source_id,
                              document.document_id,document.native_id,
                              document.revision,document.text_sha256,
                              event.native_parent_id,event.occurred_at
                       FROM canonical_documents document
                       JOIN canonical_events event
                         USING(tenant_id,source_id,event_id)
                       WHERE document.is_current
                         AND document.deleted_at IS NULL
                         AND (%s::text IS NULL OR document.tenant_id=%s)
                         AND EXISTS (
                             SELECT 1 FROM canonical_chunks chunk
                              WHERE chunk.tenant_id=document.tenant_id
                                AND chunk.source_id=document.source_id
                                AND chunk.document_id=document.document_id
                                AND chunk.deleted_at IS NULL
                         )
                         AND NOT EXISTS (
                             SELECT 1 FROM canonical_evidence_objects evidence
                              WHERE evidence.tenant_id=document.tenant_id
                                AND evidence.source_id=document.source_id
                                AND evidence.document_id=document.document_id
                         )
                       ORDER BY document.tenant_id,document.source_id,
                                document.document_id
                       LIMIT %s""",
                    (tenant_id, tenant_id, batch_size),
                ).fetchall()
            if not rows:
                break
            tenant_ids = [row["tenant_id"] for row in rows]
            source_ids = [row["source_id"] for row in rows]
            document_ids = [row["document_id"] for row in rows]
            with self.store.connect() as connection:
                chunk_rows = connection.execute(
                    """SELECT chunk.tenant_id,chunk.source_id,chunk.document_id,
                              chunk.ordinal,chunk.receipt,chunk.text_redacted
                         FROM canonical_chunks chunk
                         JOIN unnest(%s::text[],%s::text[],%s::text[])
                              AS target(tenant_id,source_id,document_id)
                           ON target.tenant_id=chunk.tenant_id
                          AND target.source_id=chunk.source_id
                          AND target.document_id=chunk.document_id
                        WHERE chunk.deleted_at IS NULL
                        ORDER BY chunk.tenant_id,chunk.source_id,
                                 chunk.document_id,chunk.ordinal""",
                    (tenant_ids, source_ids, document_ids),
                ).fetchall()
            chunks_by_document: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
            for chunk in chunk_rows:
                key = (
                    chunk["tenant_id"],
                    chunk["source_id"],
                    chunk["document_id"],
                )
                chunks_by_document.setdefault(key, []).append(chunk)
            entries: list[tuple[dict[str, Any], EvidenceBundle]] = []
            for row in rows:
                chunks = chunks_by_document.get(
                    (row["tenant_id"], row["source_id"], row["document_id"]),
                    [],
                )
                if chunks:
                    entries.append((row, self._bundle(row, chunks)))
            if not entries:
                break

            def upload(entry: tuple[dict[str, Any], EvidenceBundle]) -> dict[str, Any]:
                row, bundle = entry
                return self.projection.put(
                    tenant_id=row["tenant_id"],
                    source_id=row["source_id"],
                    document_id=row["document_id"],
                    bundle=bundle,
                )

            references: list[dict[str, Any]] = []
            futures = []
            try:
                with ThreadPoolExecutor(
                    max_workers=min(upload_concurrency, len(entries)),
                    thread_name_prefix="recall-evidence",
                ) as executor:
                    futures = [executor.submit(upload, entry) for entry in entries]
                    references = [future.result() for future in futures]
            except Exception:
                for future in futures:
                    if not future.done():
                        continue
                    try:
                        reference = future.result()
                    except Exception:
                        continue
                    if reference not in references:
                        references.append(reference)
                for reference in references:
                    try:
                        self.projection.delete(reference)
                    except Exception:
                        pass
                raise

            values = []
            for (row, bundle), reference in zip(entries, references, strict=True):
                values.append(
                    (
                        row["tenant_id"],
                        row["source_id"],
                        row["document_id"],
                        bundle.evidence_id,
                        reference["artifact_id"],
                        reference["storage_backend"],
                        reference["object_key"],
                        reference["content_sha256"],
                        reference["size_bytes"],
                        reference["media_type"],
                        reference["encryption"],
                        reference["version_id"],
                        bundle.text_sha256,
                        bundle.revision,
                        len(bundle.chunks),
                        reference["created_at"],
                    )
                )
            try:
                with self.store.connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.executemany(
                            """INSERT INTO canonical_evidence_objects(
                                   tenant_id,source_id,document_id,evidence_id,
                                   artifact_id,storage_backend,object_key,
                                   content_sha256,size_bytes,media_type,encryption,
                                   version_id,text_sha256,revision,receipt_count,
                                   created_at
                               ) VALUES (
                                   %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                   %s,%s,%s,%s
                               )
                               ON CONFLICT(tenant_id,source_id,document_id)
                               DO NOTHING""",
                            values,
                        )
                    stored_rows = connection.execute(
                        """SELECT evidence.*
                             FROM canonical_evidence_objects evidence
                             JOIN unnest(%s::text[],%s::text[],%s::text[])
                                  AS target(tenant_id,source_id,document_id)
                               ON target.tenant_id=evidence.tenant_id
                              AND target.source_id=evidence.source_id
                              AND target.document_id=evidence.document_id""",
                        (
                            [row["tenant_id"] for row, _bundle in entries],
                            [row["source_id"] for row, _bundle in entries],
                            [row["document_id"] for row, _bundle in entries],
                        ),
                    ).fetchall()
                    stored_by_document = {
                        (
                            stored["tenant_id"],
                            stored["source_id"],
                            stored["document_id"],
                        ): stored
                        for stored in stored_rows
                    }
                    for (row, bundle), reference in zip(
                        entries, references, strict=True
                    ):
                        stored = stored_by_document.get(
                            (
                                row["tenant_id"],
                                row["source_id"],
                                row["document_id"],
                            )
                        )
                        if (
                            stored is None
                            or stored["artifact_id"] != reference["artifact_id"]
                            or stored["text_sha256"] != bundle.text_sha256
                        ):
                            raise EvidenceProjectionError(
                                "evidence_projection_conflict"
                            )
            except Exception:
                for reference in references:
                    try:
                        self.projection.delete(reference)
                    except Exception:
                        pass
                raise
            processed += len(entries)
            batches += 1
        return {
            "status": "complete",
            "processed": processed,
            "batches": batches,
        }

    def references_for_receipts(
        self,
        *,
        tenant_id: str,
        source_ids: tuple[str, ...],
        receipts: tuple[str, ...],
        limit: int,
    ) -> list[dict[str, Any]]:
        tenant_id = self._tenant(tenant_id)
        assert tenant_id is not None
        if not source_ids or not receipts:
            return []
        if not 1 <= limit <= 100:
            raise EvidenceProjectionError("evidence_projection_budget_invalid")
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT DISTINCT evidence.*
                   FROM canonical_chunks chunk
                   JOIN canonical_documents document
                     USING(tenant_id,source_id,document_id)
                   JOIN canonical_evidence_objects evidence
                     USING(tenant_id,source_id,document_id)
                   WHERE chunk.tenant_id=%s
                     AND chunk.source_id=ANY(%s)
                     AND chunk.receipt=ANY(%s)
                     AND chunk.deleted_at IS NULL
                     AND document.is_current
                     AND document.deleted_at IS NULL
                   ORDER BY evidence.created_at,evidence.document_id
                   LIMIT %s""",
                (tenant_id, list(source_ids), list(receipts), limit),
            ).fetchall()
        return [self._reference(row) for row in rows]

    def targets_for_receipts(
        self,
        *,
        tenant_id: str,
        source_ids: tuple[str, ...],
        receipts: tuple[str, ...],
        limit: int,
    ) -> list[dict[str, Any]]:
        tenant_id = self._tenant(tenant_id)
        assert tenant_id is not None
        references = self.references_for_receipts(
            tenant_id=tenant_id,
            source_ids=source_ids,
            receipts=receipts,
            limit=limit,
        )
        targets: list[dict[str, Any]] = []
        with self.store.connect() as connection:
            for reference in references:
                rows = connection.execute(
                    """SELECT chunk.receipt
                       FROM canonical_evidence_objects evidence
                       JOIN canonical_chunks chunk
                         USING(tenant_id,source_id,document_id)
                       JOIN canonical_documents document
                         USING(tenant_id,source_id,document_id)
                       WHERE evidence.tenant_id=%s AND evidence.source_id=%s
                         AND evidence.artifact_id=%s
                         AND chunk.deleted_at IS NULL
                         AND document.is_current
                         AND document.deleted_at IS NULL
                       ORDER BY chunk.ordinal""",
                    (
                        tenant_id,
                        reference["source_id"],
                        reference["artifact_id"],
                    ),
                ).fetchall()
                targets.append(
                    {
                        "reference": reference,
                        "receipts": tuple(row["receipt"] for row in rows),
                    }
                )
        return targets

    def delete_native_ids(
        self,
        *,
        tenant_id: str,
        source_id: str,
        native_ids: list[str],
    ) -> int:
        tenant_id = self._tenant(tenant_id)
        assert tenant_id is not None
        if not native_ids:
            return 0
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT evidence.*
                   FROM canonical_evidence_objects evidence
                   JOIN canonical_documents document
                     USING(tenant_id,source_id,document_id)
                   WHERE document.tenant_id=%s AND document.source_id=%s
                     AND document.native_id=ANY(%s)
                   ORDER BY evidence.document_id""",
                (tenant_id, source_id, native_ids),
            ).fetchall()
        references = [self._reference(row) for row in rows]
        for reference in references:
            self.projection.delete(reference)
        with self.store.connect() as connection:
            connection.execute(
                """DELETE FROM canonical_evidence_objects evidence
                   USING canonical_documents document
                   WHERE evidence.tenant_id=document.tenant_id
                     AND evidence.source_id=document.source_id
                     AND evidence.document_id=document.document_id
                     AND document.tenant_id=%s AND document.source_id=%s
                     AND document.native_id=ANY(%s)""",
                (tenant_id, source_id, native_ids),
            )
        return len(references)
