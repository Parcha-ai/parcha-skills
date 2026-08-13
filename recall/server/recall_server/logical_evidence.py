from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Protocol
from urllib.parse import urlsplit

import orjson

from .actor_attribution import ActorLink, actor_links, canonical_actor_links

LOGICAL_DOCUMENT_ID_RE = re.compile(r"ldoc_[0-9a-f]{32}\Z")
EVIDENCE_ID_RE = re.compile(r"evd_[0-9a-f]{32}\Z")
ARTIFACT_ID_RE = re.compile(r"art_[0-9a-f]{32}\Z")
OBJECT_KEY_RE = re.compile(r"objects/[0-9a-f]{2}/[0-9a-f]{64}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+=-]{0,511}\Z")
ROLE_RE = re.compile(r"[A-Za-z0-9_.:-]{1,64}\Z")
EVENT_KIND_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}\Z")

PART_MEDIA_TYPE = "application/vnd.recall.logical-document-part+jsonl"
MANIFEST_MEDIA_TYPE = "application/vnd.recall.logical-document-manifest+json"
DEFAULT_PART_BYTES = 4 * 1024 * 1024
MAX_PART_BYTES = 64 * 1024 * 1024
MAX_RECORD_BYTES = 16 * 1024 * 1024
MAX_DOCUMENT_RECORDS = 5_000_000
MAX_DOCUMENT_RECEIPTS = 5_000_000
MAX_RECORD_RECEIPTS = 512
MIN_PART_BYTES = 1_024
RETENTION_PROFILES = {"lossless-v1", "conversation-useful-v1"}


class LogicalEvidenceError(ValueError):
    """Stable logical-document failure that never renders source content."""


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


def _parsed_timestamp(value: str) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise LogicalEvidenceError("logical_evidence_timestamp_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise LogicalEvidenceError("logical_evidence_timestamp_invalid") from None
    if parsed.tzinfo is None:
        raise LogicalEvidenceError("logical_evidence_timestamp_invalid")
    return parsed


def _receipt(value: str, source_id: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 2048
        or any(ord(character) < 0x20 for character in value)
    ):
        raise LogicalEvidenceError("logical_evidence_receipt_invalid")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "recall"
        or parsed.netloc != source_id
        or not parsed.path
        or not parsed.query
        or not parsed.fragment.startswith("item=")
    ):
        raise LogicalEvidenceError("logical_evidence_receipt_invalid")
    return value


def _opaque(prefix: str, *values: str) -> str:
    digest = hashlib.sha256("\x1f".join(values).encode()).hexdigest()[:32]
    return prefix + digest


def logical_document_id(tenant_id: str, source_id: str, native_parent_id: str) -> str:
    for value in (tenant_id, source_id, native_parent_id):
        if not isinstance(value, str) or not IDENTITY_RE.fullmatch(value):
            raise LogicalEvidenceError("logical_evidence_identity_invalid")
    return _opaque("ldoc_", tenant_id, source_id, native_parent_id)


@dataclass(frozen=True)
class LogicalEvidenceRecord:
    ordinal: int
    event_native_id: str
    event_kind: str
    occurred_at: str
    roles: tuple[str, ...]
    receipts: tuple[str, ...]
    segment_ordinal: int
    segment_count: int
    text: str
    canonical_content_bytes: bytes | None = None
    actor_links: tuple[ActorLink, ...] = ()

    def validate(self, *, source_id: str) -> None:
        if (
            isinstance(self.ordinal, bool)
            or not isinstance(self.ordinal, int)
            or self.ordinal < 0
            or not isinstance(self.event_native_id, str)
            or not IDENTITY_RE.fullmatch(self.event_native_id)
            or not isinstance(self.event_kind, str)
            or not EVENT_KIND_RE.fullmatch(self.event_kind)
            or not isinstance(self.roles, tuple)
            or len(self.roles) > 16
            or any(
                not isinstance(role, str) or not ROLE_RE.fullmatch(role)
                for role in self.roles
            )
            or tuple(sorted(set(self.roles))) != self.roles
            or not isinstance(self.receipts, tuple)
            or len(self.receipts) > MAX_RECORD_RECEIPTS
            or len(set(self.receipts)) != len(self.receipts)
            or isinstance(self.segment_ordinal, bool)
            or not isinstance(self.segment_ordinal, int)
            or isinstance(self.segment_count, bool)
            or not isinstance(self.segment_count, int)
            or self.segment_count < 1
            or not 0 <= self.segment_ordinal < self.segment_count
            or (self.segment_ordinal == 0 and not self.receipts)
            or (self.segment_ordinal > 0 and self.receipts)
            or not isinstance(self.text, str)
            or len(self.text.encode()) > MAX_RECORD_BYTES
            or (
                self.canonical_content_bytes is not None
                and not isinstance(self.canonical_content_bytes, bytes)
            )
            or (
                self.canonical_content_bytes is not None
                and self.segment_count != 1
            )
            or actor_links(self.actor_links) != self.actor_links
        ):
            raise LogicalEvidenceError("logical_evidence_record_invalid")
        _parsed_timestamp(self.occurred_at)
        for receipt in self.receipts:
            _receipt(receipt, source_id)

    def encode(self, *, source_id: str) -> bytes:
        self.validate(source_id=source_id)
        value: dict[str, Any] = {
            "event_kind": self.event_kind,
            "event_native_id": self.event_native_id,
            "occurred_at": self.occurred_at,
            "ordinal": self.ordinal,
            "receipts": list(self.receipts),
            "roles": list(self.roles),
            "segment_count": self.segment_count,
            "segment_ordinal": self.segment_ordinal,
        }
        if self.actor_links:
            value["actor_links"] = canonical_actor_links(self.actor_links)
        if self.segment_count > 1:
            value["content_fragment"] = self.text
        else:
            canonical = self.canonical_content_bytes
            try:
                if canonical is None:
                    canonical = orjson.dumps(
                        orjson.loads(self.text),
                        option=orjson.OPT_SORT_KEYS,
                    )
            except (TypeError, ValueError, orjson.JSONDecodeError):
                canonical = None
            if canonical is not None and canonical == self.text.encode():
                encoded = orjson.dumps(value, option=orjson.OPT_SORT_KEYS)
                return b'{"content":' + canonical + b"," + encoded[1:] + b"\n"
            value["text"] = self.text
        return orjson.dumps(value, option=orjson.OPT_SORT_KEYS) + b"\n"


@dataclass(frozen=True)
class PreparedPart:
    ordinal: int
    first_record_ordinal: int
    last_record_ordinal: int
    first_occurred_at: str
    last_occurred_at: str
    receipt_count: int
    payload: bytes
    content_sha256: str


@dataclass(frozen=True)
class PreparedRecordLocation:
    ordinal: int
    receipt: str
    part_ordinal: int
    line_ordinal: int


@dataclass(frozen=True)
class PreparedLogicalDocument:
    tenant_id: str
    source_id: str
    native_parent_id: str
    logical_document_id: str
    revision: int
    evidence_id: str
    first_occurred_at: str
    last_occurred_at: str
    document_content_sha256: str
    record_count: int
    receipt_count: int
    record_locations: tuple[PreparedRecordLocation, ...]
    parts: tuple[PreparedPart, ...]


@dataclass(frozen=True)
class StreamedPreparedLogicalDocument:
    tenant_id: str
    source_id: str
    native_parent_id: str
    logical_document_id: str
    revision: int
    evidence_id: str
    first_occurred_at: str
    last_occurred_at: str
    document_content_sha256: str
    record_count: int
    receipt_count: int
    parts: tuple[PreparedPart, ...]


@dataclass(frozen=True)
class ManifestPart:
    ordinal: int
    artifact_id: str
    object_key: str
    content_sha256: str
    size_bytes: int
    media_type: str
    version_id: str
    first_record_ordinal: int
    last_record_ordinal: int
    receipt_count: int

    @classmethod
    def from_reference(
        cls,
        *,
        prepared: PreparedPart,
        reference: dict[str, Any],
        tenant_id: str,
        source_id: str,
    ) -> ManifestPart:
        try:
            value = cls(
                ordinal=prepared.ordinal,
                artifact_id=reference["artifact_id"],
                object_key=reference["object_key"],
                content_sha256=reference["content_sha256"],
                size_bytes=reference["size_bytes"],
                media_type=reference["media_type"],
                version_id=reference["version_id"],
                first_record_ordinal=prepared.first_record_ordinal,
                last_record_ordinal=prepared.last_record_ordinal,
                receipt_count=prepared.receipt_count,
            )
        except KeyError:
            raise LogicalEvidenceError("logical_evidence_reference_invalid") from None
        if (
            reference.get("tenant_id") != tenant_id
            or reference.get("source_id") != source_id
            or value.content_sha256 != prepared.content_sha256
            or value.size_bytes != len(prepared.payload)
            or value.media_type != PART_MEDIA_TYPE
        ):
            raise LogicalEvidenceError("logical_evidence_reference_invalid")
        value.validate()
        return value

    def validate(self) -> None:
        if (
            isinstance(self.ordinal, bool)
            or not isinstance(self.ordinal, int)
            or self.ordinal < 0
            or not isinstance(self.artifact_id, str)
            or not ARTIFACT_ID_RE.fullmatch(self.artifact_id)
            or not isinstance(self.object_key, str)
            or not OBJECT_KEY_RE.fullmatch(self.object_key)
            or not isinstance(self.content_sha256, str)
            or not SHA256_RE.fullmatch(self.content_sha256)
            or isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or not 1 <= self.size_bytes <= MAX_PART_BYTES
            or self.media_type != PART_MEDIA_TYPE
            or not isinstance(self.version_id, str)
            or not self.version_id
            or isinstance(self.first_record_ordinal, bool)
            or not isinstance(self.first_record_ordinal, int)
            or isinstance(self.last_record_ordinal, bool)
            or not isinstance(self.last_record_ordinal, int)
            or not 0 <= self.first_record_ordinal <= self.last_record_ordinal
            or isinstance(self.receipt_count, bool)
            or not isinstance(self.receipt_count, int)
            or self.receipt_count < 0
        ):
            raise LogicalEvidenceError("logical_evidence_part_invalid")


@dataclass(frozen=True)
class LogicalEvidenceManifest:
    logical_document_id: str
    evidence_id: str
    revision: int
    native_parent_sha256: str
    document_content_sha256: str
    first_occurred_at: str
    last_occurred_at: str
    record_count: int
    receipt_count: int
    retention_profile: str
    parts: tuple[ManifestPart, ...]

    def validate(self) -> None:
        if (
            not isinstance(self.logical_document_id, str)
            or not LOGICAL_DOCUMENT_ID_RE.fullmatch(self.logical_document_id)
            or not isinstance(self.evidence_id, str)
            or not EVIDENCE_ID_RE.fullmatch(self.evidence_id)
            or isinstance(self.revision, bool)
            or not isinstance(self.revision, int)
            or self.revision < 1
            or not isinstance(self.native_parent_sha256, str)
            or not SHA256_RE.fullmatch(self.native_parent_sha256)
            or not isinstance(self.document_content_sha256, str)
            or not SHA256_RE.fullmatch(self.document_content_sha256)
            or isinstance(self.record_count, bool)
            or not isinstance(self.record_count, int)
            or not 1 <= self.record_count <= MAX_DOCUMENT_RECORDS
            or isinstance(self.receipt_count, bool)
            or not isinstance(self.receipt_count, int)
            or not 1 <= self.receipt_count <= MAX_DOCUMENT_RECEIPTS
            or self.retention_profile not in RETENTION_PROFILES
            or not isinstance(self.parts, tuple)
            or not self.parts
        ):
            raise LogicalEvidenceError("logical_evidence_manifest_invalid")
        first = _parsed_timestamp(self.first_occurred_at)
        last = _parsed_timestamp(self.last_occurred_at)
        if first > last:
            raise LogicalEvidenceError("logical_evidence_manifest_invalid")
        expected_parts = list(range(len(self.parts)))
        if [part.ordinal for part in self.parts] != expected_parts:
            raise LogicalEvidenceError("logical_evidence_manifest_invalid")
        expected_record = 0
        for part in self.parts:
            part.validate()
            if part.first_record_ordinal != expected_record:
                raise LogicalEvidenceError("logical_evidence_manifest_invalid")
            expected_record = part.last_record_ordinal + 1
        if (
            expected_record != self.record_count
            or sum(part.receipt_count for part in self.parts) != self.receipt_count
        ):
            raise LogicalEvidenceError("logical_evidence_manifest_invalid")

    def encode(self) -> bytes:
        self.validate()
        return json.dumps(
            {
                "contract": "recall.logical-document-manifest.v2",
                "document_content_sha256": self.document_content_sha256,
                "evidence_id": self.evidence_id,
                "first_occurred_at": self.first_occurred_at,
                "last_occurred_at": self.last_occurred_at,
                "logical_document_id": self.logical_document_id,
                "native_parent_sha256": self.native_parent_sha256,
                "parts": [
                    {
                        "artifact_id": part.artifact_id,
                        "content_sha256": part.content_sha256,
                        "first_record_ordinal": part.first_record_ordinal,
                        "last_record_ordinal": part.last_record_ordinal,
                        "media_type": part.media_type,
                        "object_key": part.object_key,
                        "ordinal": part.ordinal,
                        "receipt_count": part.receipt_count,
                        "size_bytes": part.size_bytes,
                        "version_id": part.version_id,
                    }
                    for part in self.parts
                ],
                "receipt_count": self.receipt_count,
                "record_count": self.record_count,
                "retention_profile": self.retention_profile,
                "revision": self.revision,
                "schema_version": 2,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()


@dataclass(frozen=True)
class LogicalEvidenceUpload:
    prepared: PreparedLogicalDocument | StreamedPreparedLogicalDocument
    manifest: LogicalEvidenceManifest
    manifest_reference: dict[str, Any]
    part_references: tuple[dict[str, Any], ...]

    @property
    def all_references(self) -> tuple[dict[str, Any], ...]:
        return (self.manifest_reference, *self.part_references)


def prepare_logical_document(
    *,
    tenant_id: str,
    source_id: str,
    native_parent_id: str,
    revision: int,
    records: Iterable[LogicalEvidenceRecord],
    part_bytes: int = DEFAULT_PART_BYTES,
) -> PreparedLogicalDocument:
    document_id = logical_document_id(tenant_id, source_id, native_parent_id)
    if (
        isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 1
        or isinstance(part_bytes, bool)
        or not isinstance(part_bytes, int)
        or not MIN_PART_BYTES <= part_bytes <= MAX_PART_BYTES
    ):
        raise LogicalEvidenceError("logical_evidence_document_invalid")

    document_digest = hashlib.sha256()
    part_payload = bytearray()
    part_start = 0
    part_record_count = 0
    part_receipt_count = 0
    part_first_occurred = ""
    part_last_occurred = ""
    part_first_time: datetime | None = None
    part_last_time: datetime | None = None
    parts: list[PreparedPart] = []
    locations: list[PreparedRecordLocation] = []
    first_occurred: str | None = None
    last_occurred: str | None = None
    first_time: datetime | None = None
    last_time: datetime | None = None
    expected_ordinal = 0

    def close_part(last_record: int) -> None:
        nonlocal part_payload, part_start
        nonlocal part_record_count, part_receipt_count
        nonlocal part_first_occurred, part_last_occurred
        nonlocal part_first_time, part_last_time
        payload = bytes(part_payload)
        if not payload:
            return
        if part_first_time is None or part_last_time is None:
            raise LogicalEvidenceError("logical_evidence_state_invalid")
        parts.append(
            PreparedPart(
                ordinal=len(parts),
                first_record_ordinal=part_start,
                last_record_ordinal=last_record,
                first_occurred_at=part_first_occurred,
                last_occurred_at=part_last_occurred,
                receipt_count=part_receipt_count,
                payload=payload,
                content_sha256=hashlib.sha256(payload).hexdigest(),
            )
        )
        part_payload = bytearray()
        part_start = last_record + 1
        part_record_count = 0
        part_receipt_count = 0
        part_first_occurred = ""
        part_last_occurred = ""
        part_first_time = None
        part_last_time = None

    for record in records:
        if (
            expected_ordinal >= MAX_DOCUMENT_RECORDS
            or record.ordinal != expected_ordinal
        ):
            raise LogicalEvidenceError("logical_evidence_document_invalid")
        encoded = record.encode(source_id=source_id)
        if len(encoded) > MAX_RECORD_BYTES:
            raise LogicalEvidenceError("logical_evidence_record_too_large")
        if part_payload and len(part_payload) + len(encoded) > part_bytes:
            close_part(record.ordinal - 1)
        locations.extend(
            PreparedRecordLocation(
                ordinal=record.ordinal,
                receipt=receipt,
                part_ordinal=len(parts),
                line_ordinal=part_record_count,
            )
            for receipt in record.receipts
        )
        part_payload.extend(encoded)
        part_record_count += 1
        part_receipt_count += len(record.receipts)
        document_digest.update(encoded)
        occurred = _parsed_timestamp(record.occurred_at)
        if part_first_time is None or occurred < part_first_time:
            part_first_time = occurred
            part_first_occurred = record.occurred_at
        if part_last_time is None or occurred > part_last_time:
            part_last_time = occurred
            part_last_occurred = record.occurred_at
        if first_time is None or occurred < first_time:
            first_time = occurred
            first_occurred = record.occurred_at
        if last_time is None or occurred > last_time:
            last_time = occurred
            last_occurred = record.occurred_at
        if len(encoded) > part_bytes:
            close_part(record.ordinal)
        expected_ordinal += 1
    if expected_ordinal == 0 or not locations:
        raise LogicalEvidenceError("logical_evidence_document_invalid")
    close_part(expected_ordinal - 1)

    digest = document_digest.hexdigest()
    evidence_id = _opaque(
        "evd_", tenant_id, source_id, document_id, str(revision), digest
    )
    return PreparedLogicalDocument(
        tenant_id=tenant_id,
        source_id=source_id,
        native_parent_id=native_parent_id,
        logical_document_id=document_id,
        revision=revision,
        evidence_id=evidence_id,
        first_occurred_at=first_occurred or "",
        last_occurred_at=last_occurred or "",
        document_content_sha256=digest,
        record_count=expected_ordinal,
        receipt_count=len(locations),
        record_locations=tuple(locations),
        parts=tuple(parts),
    )


class LogicalEvidenceProjectionStore:
    """Write immutable logical-document parts and their opaque manifest."""

    def __init__(
        self,
        archive: EvidenceArchive,
        *,
        part_upload_concurrency: int = 1,
    ):
        if (
            isinstance(part_upload_concurrency, bool)
            or not isinstance(part_upload_concurrency, int)
            or not 1 <= part_upload_concurrency <= 8
        ):
            raise LogicalEvidenceError("logical_evidence_budget_invalid")
        self.archive = archive
        self.part_upload_concurrency = part_upload_concurrency

    def put(
        self,
        prepared: PreparedLogicalDocument,
        *,
        retention_profile: str = "lossless-v1",
    ) -> LogicalEvidenceUpload:
        part_references: list[dict[str, Any]] = []
        try:
            for part in prepared.parts:
                part_references.append(
                    self.archive.put_raw(
                        tenant_id=prepared.tenant_id,
                        source_id=prepared.source_id,
                        native_id=(
                            f"logical-part:{prepared.logical_document_id}:"
                            f"{part.ordinal}"
                        ),
                        payload=part.payload,
                        media_type=PART_MEDIA_TYPE,
                        created_at=prepared.first_occurred_at,
                    )
                )
            manifest_parts = tuple(
                ManifestPart.from_reference(
                    prepared=part,
                    reference=reference,
                    tenant_id=prepared.tenant_id,
                    source_id=prepared.source_id,
                )
                for part, reference in zip(
                    prepared.parts, part_references, strict=True
                )
            )
            manifest = LogicalEvidenceManifest(
                logical_document_id=prepared.logical_document_id,
                evidence_id=prepared.evidence_id,
                revision=prepared.revision,
                native_parent_sha256=hashlib.sha256(
                    prepared.native_parent_id.encode()
                ).hexdigest(),
                document_content_sha256=prepared.document_content_sha256,
                first_occurred_at=prepared.first_occurred_at,
                last_occurred_at=prepared.last_occurred_at,
                record_count=prepared.record_count,
                receipt_count=prepared.receipt_count,
                retention_profile=retention_profile,
                parts=manifest_parts,
            )
            manifest_reference = self.archive.put_raw(
                tenant_id=prepared.tenant_id,
                source_id=prepared.source_id,
                native_id=(
                    f"logical-manifest:{prepared.logical_document_id}:"
                    f"{prepared.document_content_sha256[:16]}"
                ),
                payload=manifest.encode(),
                media_type=MANIFEST_MEDIA_TYPE,
                created_at=prepared.first_occurred_at,
            )
        except Exception:
            for reference in reversed(part_references):
                try:
                    self.archive.delete_raw(reference)
                except Exception:
                    pass
            raise
        return LogicalEvidenceUpload(
            prepared=prepared,
            manifest=manifest,
            manifest_reference=manifest_reference,
            part_references=tuple(part_references),
        )

    def put_records(
        self,
        *,
        tenant_id: str,
        source_id: str,
        native_parent_id: str,
        revision: int,
        records: Iterable[LogicalEvidenceRecord],
        part_bytes: int = DEFAULT_PART_BYTES,
        retention_profile: str = "lossless-v1",
        existing_part_references: tuple[dict[str, Any], ...] = (),
    ) -> LogicalEvidenceUpload:
        """Upload a logical document with memory bounded to one object part."""

        document_id = logical_document_id(
            tenant_id,
            source_id,
            native_parent_id,
        )
        if (
            isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision < 1
            or isinstance(part_bytes, bool)
            or not isinstance(part_bytes, int)
            or not MIN_PART_BYTES <= part_bytes <= MAX_PART_BYTES
            or retention_profile not in RETENTION_PROFILES
            or not isinstance(existing_part_references, tuple)
        ):
            raise LogicalEvidenceError("logical_evidence_document_invalid")

        document_digest = hashlib.sha256()
        part_payload = bytearray()
        part_start = 0
        part_receipt_count = 0
        record_count = 0
        receipt_count = 0
        part_created_at = ""
        part_first_occurred = ""
        part_last_occurred = ""
        part_first_time: datetime | None = None
        part_last_time: datetime | None = None
        first_occurred = ""
        last_occurred = ""
        first_time: datetime | None = None
        last_time: datetime | None = None
        parts: list[PreparedPart] = []
        manifest_parts: list[ManifestPart] = []
        part_references: list[dict[str, Any]] = []
        pending_parts: deque[
            tuple[PreparedPart, Future[dict[str, Any]]]
        ] = deque()
        part_executor = (
            ThreadPoolExecutor(
                max_workers=self.part_upload_concurrency,
                thread_name_prefix="recall-logical-part",
            )
            if self.part_upload_concurrency > 1
            else None
        )

        def publish_part(
            prepared_part: PreparedPart,
            created_at: str,
        ) -> dict[str, Any]:
            return self.archive.put_raw(
                tenant_id=tenant_id,
                source_id=source_id,
                native_id=(
                    f"logical-part:{document_id}:"
                    f"{prepared_part.ordinal}"
                ),
                payload=prepared_part.payload,
                media_type=PART_MEDIA_TYPE,
                created_at=created_at,
            )

        def accept_part(
            prepared_part: PreparedPart,
            reference: dict[str, Any],
        ) -> None:
            part_references.append(reference)
            manifest_parts.append(
                ManifestPart.from_reference(
                    prepared=prepared_part,
                    reference=reference,
                    tenant_id=tenant_id,
                    source_id=source_id,
                )
            )
            parts.append(
                PreparedPart(
                    ordinal=prepared_part.ordinal,
                    first_record_ordinal=prepared_part.first_record_ordinal,
                    last_record_ordinal=prepared_part.last_record_ordinal,
                    first_occurred_at=prepared_part.first_occurred_at,
                    last_occurred_at=prepared_part.last_occurred_at,
                    receipt_count=prepared_part.receipt_count,
                    payload=b"",
                    content_sha256=prepared_part.content_sha256,
                )
            )

        def finish_oldest_part() -> None:
            prepared_part, future = pending_parts.popleft()
            accept_part(prepared_part, future.result())

        def reusable_part(
            prepared_part: PreparedPart,
        ) -> dict[str, Any] | None:
            if prepared_part.ordinal >= len(existing_part_references):
                return None
            reference = existing_part_references[prepared_part.ordinal]
            if (
                not isinstance(reference, dict)
                or reference.get("tenant_id") != tenant_id
                or reference.get("source_id") != source_id
                or reference.get("content_sha256")
                    != prepared_part.content_sha256
                or reference.get("size_bytes") != len(prepared_part.payload)
                or reference.get("media_type") != PART_MEDIA_TYPE
            ):
                return None
            return reference

        def close_part(last_record: int) -> None:
            nonlocal part_payload, part_start
            nonlocal part_receipt_count, part_created_at
            nonlocal part_first_occurred, part_last_occurred
            nonlocal part_first_time, part_last_time
            payload = bytes(part_payload)
            if not payload:
                return
            if part_first_time is None or part_last_time is None:
                raise LogicalEvidenceError("logical_evidence_state_invalid")
            prepared_part = PreparedPart(
                ordinal=len(parts) + len(pending_parts),
                first_record_ordinal=part_start,
                last_record_ordinal=last_record,
                first_occurred_at=part_first_occurred,
                last_occurred_at=part_last_occurred,
                receipt_count=part_receipt_count,
                payload=payload,
                content_sha256=hashlib.sha256(payload).hexdigest(),
            )
            existing_reference = reusable_part(prepared_part)
            if existing_reference is not None:
                accept_part(prepared_part, existing_reference)
            elif part_executor is None:
                accept_part(
                    prepared_part,
                    publish_part(prepared_part, part_created_at),
                )
            else:
                if len(pending_parts) >= self.part_upload_concurrency:
                    finish_oldest_part()
                pending_parts.append(
                    (
                        prepared_part,
                        part_executor.submit(
                            publish_part,
                            prepared_part,
                            part_created_at,
                        ),
                    )
                )
            part_payload = bytearray()
            part_start = last_record + 1
            part_receipt_count = 0
            part_created_at = ""
            part_first_occurred = ""
            part_last_occurred = ""
            part_first_time = None
            part_last_time = None

        try:
            for record in records:
                if (
                    record_count >= MAX_DOCUMENT_RECORDS
                    or record.ordinal != record_count
                ):
                    raise LogicalEvidenceError(
                        "logical_evidence_document_invalid"
                    )
                encoded = record.encode(source_id=source_id)
                if len(encoded) > MAX_RECORD_BYTES:
                    raise LogicalEvidenceError(
                        "logical_evidence_record_too_large"
                    )
                if part_payload and len(part_payload) + len(encoded) > part_bytes:
                    close_part(record.ordinal - 1)
                if not part_payload:
                    part_created_at = record.occurred_at
                part_payload.extend(encoded)
                part_receipt_count += len(record.receipts)
                record_count += 1
                receipt_count += len(record.receipts)
                if receipt_count > MAX_DOCUMENT_RECEIPTS:
                    raise LogicalEvidenceError(
                        "logical_evidence_document_invalid"
                    )
                document_digest.update(encoded)
                occurred = _parsed_timestamp(record.occurred_at)
                if part_first_time is None or occurred < part_first_time:
                    part_first_time = occurred
                    part_first_occurred = record.occurred_at
                if part_last_time is None or occurred > part_last_time:
                    part_last_time = occurred
                    part_last_occurred = record.occurred_at
                if first_time is None or occurred < first_time:
                    first_time = occurred
                    first_occurred = record.occurred_at
                if last_time is None or occurred > last_time:
                    last_time = occurred
                    last_occurred = record.occurred_at
                if len(encoded) > part_bytes:
                    close_part(record.ordinal)
            if record_count == 0:
                raise LogicalEvidenceError("logical_evidence_document_empty")
            if receipt_count == 0:
                raise LogicalEvidenceError("logical_evidence_document_invalid")
            close_part(record_count - 1)
            while pending_parts:
                finish_oldest_part()
            digest = document_digest.hexdigest()
            evidence_id = _opaque(
                "evd_",
                tenant_id,
                source_id,
                document_id,
                str(revision),
                digest,
            )
            prepared = StreamedPreparedLogicalDocument(
                tenant_id=tenant_id,
                source_id=source_id,
                native_parent_id=native_parent_id,
                logical_document_id=document_id,
                revision=revision,
                evidence_id=evidence_id,
                first_occurred_at=first_occurred,
                last_occurred_at=last_occurred,
                document_content_sha256=digest,
                record_count=record_count,
                receipt_count=receipt_count,
                parts=tuple(parts),
            )
            manifest = LogicalEvidenceManifest(
                logical_document_id=document_id,
                evidence_id=evidence_id,
                revision=revision,
                native_parent_sha256=hashlib.sha256(
                    native_parent_id.encode()
                ).hexdigest(),
                document_content_sha256=digest,
                first_occurred_at=first_occurred,
                last_occurred_at=last_occurred,
                record_count=record_count,
                receipt_count=receipt_count,
                retention_profile=retention_profile,
                parts=tuple(manifest_parts),
            )
            manifest_reference = self.archive.put_raw(
                tenant_id=tenant_id,
                source_id=source_id,
                native_id=(
                    f"logical-manifest:{document_id}:"
                    f"{digest[:16]}"
                ),
                payload=manifest.encode(),
                media_type=MANIFEST_MEDIA_TYPE,
                created_at=first_occurred,
            )
        except BaseException:
            while pending_parts:
                prepared_part, future = pending_parts.popleft()
                try:
                    accept_part(prepared_part, future.result())
                except Exception:
                    pass
            for reference in reversed(part_references):
                try:
                    self.archive.delete_raw(reference)
                except Exception:
                    pass
            raise
        finally:
            if part_executor is not None:
                part_executor.shutdown(wait=True, cancel_futures=True)
        return LogicalEvidenceUpload(
            prepared=prepared,
            manifest=manifest,
            manifest_reference=manifest_reference,
            part_references=tuple(part_references),
        )

    def read_manifest(
        self,
        reference: dict[str, Any],
        *,
        tenant_id: str,
        source_id: str,
    ) -> dict[str, Any]:
        if (
            not isinstance(reference, dict)
            or reference.get("tenant_id") != tenant_id
            or reference.get("source_id") != source_id
        ):
            raise LogicalEvidenceError("logical_evidence_not_found")
        try:
            value = json.loads(self.archive.read_raw(reference))
        except Exception as error:
            if error.__class__.__name__ == "ArchiveCorruption":
                raise LogicalEvidenceError("logical_evidence_corrupt") from None
            raise LogicalEvidenceError("logical_evidence_not_found") from error
        if not isinstance(value, dict):
            raise LogicalEvidenceError("logical_evidence_manifest_invalid")
        version = (value.get("contract"), value.get("schema_version"))
        if version == ("recall.logical-document-manifest.v1", 1):
            return value
        if (
            version != ("recall.logical-document-manifest.v2", 2)
            or value.get("retention_profile") not in RETENTION_PROFILES
        ):
            raise LogicalEvidenceError("logical_evidence_manifest_invalid")
        return value

    def read_part(
        self,
        reference: dict[str, Any],
        *,
        tenant_id: str,
        source_id: str,
    ) -> bytes:
        if (
            not isinstance(reference, dict)
            or reference.get("tenant_id") != tenant_id
            or reference.get("source_id") != source_id
            or reference.get("media_type") != PART_MEDIA_TYPE
        ):
            raise LogicalEvidenceError("logical_evidence_not_found")
        try:
            payload = self.archive.read_raw(reference)
        except Exception as error:
            if error.__class__.__name__ == "ArchiveCorruption":
                raise LogicalEvidenceError("logical_evidence_corrupt") from None
            raise LogicalEvidenceError("logical_evidence_not_found") from error
        if not hmac.compare_digest(
            hashlib.sha256(payload).hexdigest(),
            reference.get("content_sha256", ""),
        ):
            raise LogicalEvidenceError("logical_evidence_corrupt")
        return payload

    def delete(self, upload: LogicalEvidenceUpload) -> int:
        deleted = 0
        for reference in upload.all_references:
            deleted += int(self.archive.delete_raw(reference))
        return deleted

    def delete_reference(self, reference: dict[str, Any]) -> bool:
        delete_internal = getattr(self.archive, "delete_internal_raw", None)
        if callable(delete_internal):
            return bool(delete_internal(reference))
        return self.archive.delete_raw(reference)
