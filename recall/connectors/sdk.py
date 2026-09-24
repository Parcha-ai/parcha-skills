from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import sqlite3
import stat
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

from client.mac import canonical_envelope, canonical_json
from contracts.v2 import ContractError, validate_contract
from privacy.policy import PrivacyPolicy, summarize_receipts
from connectors.record_contract import TYPED_RECORD_FIELDS, validate_content_fidelity


CONNECTOR_SCHEMA_VERSION = 1
CONNECTOR_SCHEMA_VERSION_V2 = 2
MAX_PAGE_RECORDS = 500
MAX_RECORD_BYTES = 1_000_000
MAX_PAGE_BYTES = 8_000_000
MAX_ARCHIVE_OVERRIDE_BYTES = 64 * 1024 * 1024
IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@/=-]{1,255}\Z")
CONNECTOR_ID = re.compile(r"[a-z][a-z0-9_.-]{2,63}\Z")
SOURCE_ID = re.compile(r"[A-Za-z0-9_.:@-]{3,160}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
MEDIA_TYPE = re.compile(
    r"[a-z0-9][a-z0-9.+-]{0,63}/[a-z0-9][a-z0-9.+-]{0,127}\Z"
)
ALLOWED_PROVENANCE_SCHEMES = {"https", "export", "connector", "manual"}
MAX_ACK_SEED_BYTES = 16_000_000
MAX_ACK_SEED_RECORDS = 250_000


class ConnectorContractError(ValueError):
    pass


class ConnectorPageCapacityError(ConnectorContractError):
    """A provider response exceeds bounded ACK work-unit capacity."""


class ConnectorRateLimited(Exception):
    def __init__(self, *, retry_after_seconds: int | float):
        if not isinstance(retry_after_seconds, (int, float)) or retry_after_seconds <= 0:
            raise ConnectorContractError("retry-after must be positive")
        self.retry_after_seconds = float(retry_after_seconds)
        super().__init__("connector rate limited")


class ConnectorRunError(RuntimeError):
    def __init__(self, error_code: str):
        self.error_code = error_code
        super().__init__(error_code)


class ConnectorUpstreamError(RuntimeError):
    """A connector-owned, stable, content-free upstream condition."""

    def __init__(self, error_code: str):
        if not isinstance(error_code, str) or not re.fullmatch(r"[a-z][a-z0-9_]{2,63}", error_code):
            raise ConnectorContractError("upstream error code is invalid")
        self.error_code = error_code
        super().__init__(error_code)


def _create_acknowledged_records_table(db: sqlite3.Connection) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS acknowledged_records(
          native_sha256 TEXT NOT NULL
            CHECK(length(native_sha256)=64 AND native_sha256 NOT GLOB '*[^0-9a-f]*'),
          content_sha256 TEXT NOT NULL
            CHECK(length(content_sha256)=64 AND content_sha256 NOT GLOB '*[^0-9a-f]*'),
          acknowledged_at REAL NOT NULL,
          PRIMARY KEY(native_sha256,content_sha256)
        ) WITHOUT ROWID
    """)
    columns = [
        (row[1], row[2], row[3], row[5])
        for row in db.execute("PRAGMA table_info(acknowledged_records)")
    ]
    expected = [
        ("native_sha256", "TEXT", 1, 1),
        ("content_sha256", "TEXT", 1, 2),
        ("acknowledged_at", "REAL", 1, 0),
    ]
    if columns != expected:
        raise ConnectorContractError("acknowledged-record ledger schema is invalid")
    table_sql = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='acknowledged_records'"
    ).fetchone()[0]
    if "WITHOUT ROWID" not in table_sql.upper():
        raise ConnectorContractError("acknowledged-record ledger schema is invalid")


def _native_sha256(source_id: str, native_id: str) -> str:
    return hashlib.sha256(f"{source_id}\0{native_id}".encode()).hexdigest()


def _validate_acknowledgement(value: Any, events: list[dict[str, Any]]) -> None:
    if not isinstance(value, dict) or value.get("status") != "committed":
        raise ConnectorRunError("brain_invalid_acknowledgement")
    inserted = value.get("inserted")
    duplicates = value.get("duplicate_events")
    replay = value.get("replay")
    receipts = value.get("receipts")
    if (
        type(inserted) is not int
        or type(duplicates) is not int
        or inserted < 0
        or duplicates < 0
        or inserted + duplicates != len(events)
        or type(replay) is not bool
        or not isinstance(receipts, list)
        or len(receipts) != len(events)
    ):
        raise ConnectorRunError("brain_invalid_acknowledgement")
    for receipt, event in zip(receipts, events, strict=True):
        prefix = f"recall://{event['source_id']}/{event['native_id']}?rev="
        if (
            not isinstance(receipt, str)
            or not receipt.startswith(prefix)
            or not re.fullmatch(
                r"[1-9][0-9]*(?:#item=0)?",
                receipt.removeprefix(prefix),
            )
        ):
            raise ConnectorRunError("brain_invalid_acknowledgement")


def _strict_private_file(path: Path, *, label: str, max_bytes: int | None = None,
                         read_data: bool = True) -> bytes:
    path = Path(path)
    if not path.is_absolute():
        raise ConnectorContractError(f"{label} path must be absolute")
    try:
        parent = path.parent.lstat()
        details = path.lstat()
    except OSError as error:
        raise ConnectorContractError(f"{label} path is unavailable") from error
    if not stat.S_ISDIR(parent.st_mode) or stat.S_ISLNK(parent.st_mode):
        raise ConnectorContractError(f"{label} parent must be a non-symlink directory")
    if stat.S_IMODE(parent.st_mode) != 0o700:
        raise ConnectorContractError(f"{label} parent must have mode 0700")
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise ConnectorContractError(f"{label} must be a regular non-symlink file")
    if stat.S_IMODE(details.st_mode) != 0o600:
        raise ConnectorContractError(f"{label} must have mode 0600")
    if max_bytes is not None and details.st_size > max_bytes:
        raise ConnectorContractError(f"{label} exceeds maximum byte count")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (details.st_dev, details.st_ino):
                raise ConnectorContractError(f"{label} changed during validation")
            if not read_data:
                return b""
            chunks = []
            remaining = details.st_size + 1
            while remaining:
                chunk = os.read(descriptor, min(1_048_576, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            return b"".join(chunks)
        finally:
            os.close(descriptor)
    except ConnectorContractError:
        raise
    except OSError as error:
        raise ConnectorContractError(f"{label} could not be read safely") from error


def seed_acknowledged_records(*, spool_path: Path, seed_path: Path) -> dict[str, int]:
    """Seed an existing spool from a strict, hash-only acknowledged-record manifest."""
    raw = _strict_private_file(Path(seed_path), label="seed", max_bytes=MAX_ACK_SEED_BYTES)
    try:
        manifest = json.loads(raw, parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ConnectorContractError("seed must be finite JSON") from error
    if not isinstance(manifest, dict) or set(manifest) != {"schema_version", "records"}:
        raise ConnectorContractError("seed must be a closed versioned object")
    records = manifest["records"]
    if manifest["schema_version"] != 1 or not isinstance(records, list):
        raise ConnectorContractError("seed schema is invalid")
    if len(records) > MAX_ACK_SEED_RECORDS:
        raise ConnectorContractError("seed exceeds maximum record count")
    pairs: list[tuple[str, str]] = []
    for record in records:
        if not isinstance(record, dict) or set(record) != {"native_sha256", "content_sha256"}:
            raise ConnectorContractError("seed record must be a closed object")
        native_sha256 = record["native_sha256"]
        content_sha256 = record["content_sha256"]
        if not isinstance(native_sha256, str) or not SHA256.fullmatch(native_sha256):
            raise ConnectorContractError("seed native hash is invalid")
        if not isinstance(content_sha256, str) or not SHA256.fullmatch(content_sha256):
            raise ConnectorContractError("seed content hash is invalid")
        pairs.append((native_sha256, content_sha256))
    if len(pairs) != len(set(pairs)):
        raise ConnectorContractError("seed contains duplicate records")

    spool_path = Path(spool_path)
    _strict_private_file(spool_path, label="spool", read_data=False)
    try:
        db = sqlite3.connect(f"file:{spool_path}?mode=rw", uri=True)
        db.row_factory = sqlite3.Row
        tables = {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if not {"meta", "pages", "outbox"}.issubset(tables):
            raise ConnectorContractError("spool schema is invalid")
        identity = dict(db.execute(
            "SELECT key,value FROM meta WHERE key IN ('connector_id','source_id')"
        ))
        if set(identity) != {"connector_id", "source_id"}:
            raise ConnectorContractError("spool identity is incomplete")
        db.execute("BEGIN IMMEDIATE")
        try:
            pending = db.execute("SELECT count(*) FROM pages").fetchone()[0]
            pending += db.execute("SELECT count(*) FROM outbox").fetchone()[0]
            if pending:
                raise ConnectorContractError("spool must have no pending work before seeding")
            _create_acknowledged_records_table(db)
            before = db.execute("SELECT count(*) FROM acknowledged_records").fetchone()[0]
            acknowledged_at = time.time()
            db.executemany(
                "INSERT OR IGNORE INTO acknowledged_records"
                "(native_sha256,content_sha256,acknowledged_at) VALUES (?,?,?)",
                [(native, content, acknowledged_at) for native, content in pairs],
            )
            after = db.execute("SELECT count(*) FROM acknowledged_records").fetchone()[0]
            db.commit()
        except Exception:
            db.rollback()
            raise
    except ConnectorContractError:
        raise
    except sqlite3.Error as error:
        raise ConnectorContractError("spool could not be seeded") from error
    finally:
        if "db" in locals():
            db.close()
    seeded = after - before
    return {"schema_version": 1, "seeded": seeded, "already_acknowledged": len(pairs) - seeded}


def _json_copy(value: Any, label: str) -> Any:
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError) as error:
        raise ConnectorContractError(f"{label} must be finite JSON") from error


def _timestamp(value: str) -> str:
    if not isinstance(value, str):
        raise ConnectorContractError("occurred_at must be an RFC3339 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ConnectorContractError("occurred_at must be RFC3339") from error
    if parsed.tzinfo is None:
        raise ConnectorContractError("occurred_at must include a timezone")
    return value


def _validate_typed_value(value: Any, specification: dict[str, Any]) -> bool:
    value_type = specification.get("type")
    valid = (
        (value_type == "string" and isinstance(value, str))
        or (value_type == "boolean" and type(value) is bool)
        or (value_type == "number" and type(value) in {int, float})
        or (value_type == "object" and isinstance(value, dict))
        or (
            value_type == "string_list" and isinstance(value, list)
            and all(isinstance(item, str) for item in value)
        )
        or (
            value_type == "object_list" and isinstance(value, list)
            and all(isinstance(item, dict) for item in value)
        )
    )
    return (
        valid
        and ("const" not in specification or value == specification["const"])
        and ("enum" not in specification or value in specification["enum"])
        and (
            "pattern" not in specification
            or isinstance(value, str)
            and re.fullmatch(specification["pattern"], value) is not None
        )
    )


def _validate_record_common(record: Any) -> None:
    if not isinstance(record.native_id, str) or not IDENTITY.fullmatch(record.native_id):
        raise ConnectorContractError("native_id is invalid")
    if record.native_parent_id is not None and (
        not isinstance(record.native_parent_id, str)
        or not IDENTITY.fullmatch(record.native_parent_id)
    ):
        raise ConnectorContractError("native_parent_id is invalid")
    _timestamp(record.occurred_at)
    if not isinstance(record.content, dict):
        raise ConnectorContractError("content must be an object")
    if not isinstance(record.provenance, dict) or set(record.provenance) == set():
        raise ConnectorContractError("provenance must be a non-empty object")
    if not all(isinstance(key, str) and key for key in record.provenance):
        raise ConnectorContractError("provenance keys must be strings")
    uri = record.provenance.get("uri")
    parsed = urlparse(uri) if isinstance(uri, str) else None
    if not parsed or parsed.scheme not in ALLOWED_PROVENANCE_SCHEMES:
        raise ConnectorContractError("provenance uri scheme is not allowed")
    if parsed.scheme == "https" and not parsed.hostname:
        raise ConnectorContractError("HTTPS provenance must include a host")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ConnectorContractError("provenance uri must not contain credentials, query, or fragment")
    if not isinstance(record.deleted, bool):
        raise ConnectorContractError("deleted must be boolean")
    if (record.archive_payload is None) != (record.archive_media_type is None):
        raise ConnectorContractError("archive payload and media type must be configured together")
    if record.archive_payload is not None and (
        record.deleted
        or not isinstance(record.archive_payload, bytes)
        or not 1 <= len(record.archive_payload) <= MAX_ARCHIVE_OVERRIDE_BYTES
    ):
        raise ConnectorContractError("archive payload is invalid")
    if record.archive_media_type is not None and (
        not isinstance(record.archive_media_type, str)
        or not MEDIA_TYPE.fullmatch(record.archive_media_type)
    ):
        raise ConnectorContractError("archive media type is invalid")
    object.__setattr__(record, "content", _json_copy(record.content, "content"))
    object.__setattr__(record, "provenance", _json_copy(record.provenance, "provenance"))
    if len(json.dumps({"content": record.content, "provenance": record.provenance}).encode()) > MAX_RECORD_BYTES:
        raise ConnectorContractError("record exceeds maximum byte count")


@dataclass(frozen=True)
class ConnectorRecord:
    schema_version: int
    native_id: str
    occurred_at: str
    content: dict[str, Any]
    provenance: dict[str, Any]
    native_parent_id: str | None = None
    deleted: bool = False
    archive_payload: bytes | None = None
    archive_media_type: str | None = None

    def __post_init__(self) -> None:
        if self.schema_version != CONNECTOR_SCHEMA_VERSION:
            raise ConnectorContractError("unsupported connector record schema_version")
        _validate_record_common(self)

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "ConnectorRecord":
        required = {"schema_version", "native_id", "occurred_at", "content", "provenance", "deleted"}
        expected = required | {"native_parent_id"}
        if not isinstance(value, dict):
            raise ConnectorContractError("record must be an object")
        unknown = set(value) - expected
        missing = required - set(value)
        if unknown:
            raise ConnectorContractError("record has unknown fields")
        if missing:
            raise ConnectorContractError("record is missing fields")
        return cls(**value)

    def to_mapping(self) -> dict[str, Any]:
        value = {
            "schema_version": self.schema_version,
            "native_id": self.native_id,
            "occurred_at": self.occurred_at,
            "content": _json_copy(self.content, "content"),
            "provenance": _json_copy(self.provenance, "provenance"),
            "deleted": self.deleted,
        }
        if self.native_parent_id is not None:
            value["native_parent_id"] = self.native_parent_id
        return value


@dataclass(frozen=True)
class ConnectorRecordV2(ConnectorRecord):
    """Typed v2 record; v1 remains a separate strict compatibility contract."""

    def __post_init__(self) -> None:
        if self.schema_version != CONNECTOR_SCHEMA_VERSION_V2:
            raise ConnectorContractError("unsupported connector record schema_version")
        _validate_record_common(self)
        kind = self.content.get("kind")
        schema = TYPED_RECORD_FIELDS.get(kind)
        if schema is None:
            raise ConnectorContractError("record kind is unsupported")
        fields = set(self.content)
        required = {"kind"} if self.deleted else schema["required"]
        optional = set() if self.deleted else schema["optional"]
        missing = required - fields
        unknown = fields - required - optional
        if missing:
            raise ConnectorContractError("record content is missing fields")
        if unknown:
            raise ConnectorContractError("record has unknown content fields")
        if any(
            not _validate_typed_value(self.content[field], schema["properties"][field])
            for field in fields
        ):
            raise ConnectorContractError("record content has invalid field values")
        try:
            validate_content_fidelity(self.content, deleted=self.deleted)
        except ValueError:
            raise ConnectorContractError("record content fidelity is invalid") from None

    @property
    def record_kind(self) -> str:
        return self.content["kind"]


@dataclass(frozen=True)
class ConnectorPage:
    records: tuple[ConnectorRecord, ...]
    next_cursor: str
    has_more: bool

    def __post_init__(self) -> None:
        if not isinstance(self.records, tuple) or not all(isinstance(item, ConnectorRecord) for item in self.records):
            raise ConnectorContractError("page records must be a tuple of ConnectorRecord")
        if len(self.records) > MAX_PAGE_RECORDS:
            raise ConnectorPageCapacityError("page exceeds maximum record count")
        if not isinstance(self.next_cursor, str) or not self.next_cursor or len(self.next_cursor) > 4096:
            raise ConnectorContractError("next_cursor is invalid")
        if not isinstance(self.has_more, bool):
            raise ConnectorContractError("has_more must be boolean")
        identities = [item.native_id for item in self.records]
        if len(identities) != len(set(identities)):
            raise ConnectorContractError("page contains duplicate native_id")
        page_bytes = sum(len(json.dumps({"content": item.content, "provenance": item.provenance}).encode()) for item in self.records)
        if page_bytes > MAX_PAGE_BYTES:
            raise ConnectorPageCapacityError("page exceeds maximum byte count")


class PullConnector(Protocol):
    connector_id: str
    source_id: str

    def pull(self, cursor: str | None) -> ConnectorPage: ...


class BrainWriter(Protocol):
    def ingest(self, events: list[dict[str, Any]]) -> dict[str, Any]: ...


class RawArchiveWriter(Protocol):
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


class ConnectorRunner:
    """ACK-gated runtime with raw archive before private spool and Brain."""

    def __init__(self, *, connector: PullConnector, brain: BrainWriter, spool_path: Path,
                 privacy: PrivacyPolicy | None = None, enabled: bool = True,
                 archive: RawArchiveWriter | None = None, tenant_id: str | None = None,
                 principal_id: str = "owner"):
        connector_id = getattr(connector, "connector_id", None)
        source_id = getattr(connector, "source_id", None)
        if not isinstance(connector_id, str) or not CONNECTOR_ID.fullmatch(connector_id):
            raise ConnectorContractError("connector_id is invalid")
        if not isinstance(source_id, str) or not SOURCE_ID.fullmatch(source_id):
            raise ConnectorContractError("source_id is invalid")
        if not isinstance(enabled, bool):
            raise ConnectorContractError("enabled must be boolean")
        if (archive is None) != (tenant_id is None):
            raise ConnectorContractError("archive and tenant_id must be configured together")
        if tenant_id is not None and (
            not isinstance(tenant_id, str) or not IDENTITY.fullmatch(tenant_id)
        ):
            raise ConnectorContractError("tenant_id is invalid")
        if not isinstance(principal_id, str) or not IDENTITY.fullmatch(principal_id):
            raise ConnectorContractError("principal_id is invalid")
        self.connector = connector
        self.brain = brain
        self.archive = archive
        self.tenant_id = tenant_id
        self.principal_id = principal_id
        self.connector_id = connector_id
        self.source_id = source_id
        self.privacy = privacy or PrivacyPolicy(mode="off")
        self.enabled = enabled
        self.spool_path = Path(spool_path)
        self.spool_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(self.spool_path)
        os.chmod(self.spool_path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA secure_delete=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS pages(
              id INTEGER PRIMARY KEY, cursor_before TEXT NOT NULL,
              cursor_after TEXT NOT NULL, has_more INTEGER NOT NULL,
              created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbox(
              id INTEGER PRIMARY KEY, page_id INTEGER NOT NULL REFERENCES pages(id),
              envelope_json TEXT NOT NULL, state TEXT NOT NULL CHECK(state='pending')
            );
        """)
        _create_acknowledged_records_table(self.db)
        self._pin_identity()
        # Trusted in-process connectors may keep content-free checkpoint state
        # in this spool. Its updates share the page ACK transaction; the closed
        # third-party page wire gains no SQL or executable extension surface.
        self.checkpoints = connector if callable(getattr(connector, "bind_checkpoint_store", None)) else None
        if self.checkpoints is not None:
            self.checkpoints.bind_checkpoint_store(self.db)
            self.db.commit()

    def _pin_identity(self) -> None:
        current = {row["key"]: row["value"] for row in self.db.execute(
            "SELECT key,value FROM meta WHERE key IN ('connector_id','source_id')"
        )}
        expected = {"connector_id": self.connector_id, "source_id": self.source_id}
        if current and current != expected:
            raise ConnectorContractError("spool identity does not match connector")
        for key, value in expected.items():
            self.db.execute("INSERT OR IGNORE INTO meta(key,value) VALUES (?,?)", (key, value))
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def _get_meta(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def _set_meta(self, key: str, value: str) -> None:
        self.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)", (key, value))

    def _cursor(self) -> str | None:
        raw = self._get_meta("committed_cursor")
        return json.loads(raw) if raw is not None else None

    def _record_error(self, code: str) -> None:
        self._set_meta("last_error_code", code)
        self.db.commit()

    def _clear_error(self) -> None:
        self.db.execute("DELETE FROM meta WHERE key='last_error_code'")

    @staticmethod
    def _raw_payload(record: ConnectorRecord) -> bytes:
        if record.archive_payload is not None:
            return record.archive_payload
        return json.dumps(
            {
                "schema_version": record.schema_version,
                "native_id": record.native_id,
                "native_parent_id": record.native_parent_id,
                "occurred_at": record.occurred_at,
                "content": record.content,
                "provenance": record.provenance,
                "deleted": record.deleted,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()

    def _archive_raw(self, record: ConnectorRecord) -> dict[str, Any] | None:
        if self.archive is None:
            return None
        payload = self._raw_payload(record)
        media_type = record.archive_media_type or "application/json"
        try:
            candidate = self.archive.put_raw(
                tenant_id=self.tenant_id,
                source_id=self.source_id,
                native_id=record.native_id,
                payload=payload,
                media_type=media_type,
                created_at=record.occurred_at,
            )
        except Exception as error:
            if getattr(error, "error_code", None) == "archive_identity_forgotten":
                raise ConnectorRunError("archive_identity_forgotten") from None
            raise ConnectorRunError("archive_unavailable") from None
        try:
            reference = validate_contract(candidate, expected="recall.artifact-ref.v1")
        except ContractError:
            raise ConnectorRunError("archive_invalid_reference") from None
        expected = {
            "tenant_id": self.tenant_id,
            "source_id": self.source_id,
            "content_sha256": hashlib.sha256(payload).hexdigest(),
            "size_bytes": len(payload),
            "media_type": media_type,
            "created_at": record.occurred_at,
        }
        if any(reference.get(key) != value for key, value in expected.items()):
            raise ConnectorRunError("archive_invalid_reference")
        return reference

    def _event(
        self,
        record: ConnectorRecord,
        content: dict[str, Any],
        provenance: dict[str, Any],
        artifact_ref: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        provenance = {
            **provenance,
            "connector_id": self.connector_id,
            "connector_schema_version": record.schema_version,
        }
        if artifact_ref is not None:
            provenance["artifact_ref"] = artifact_ref
        if record.deleted:
            return canonical_envelope(
                source_id=self.source_id, native_id=record.native_id, kind="tombstone",
                content={"target_native_id": record.native_id}, principal_id=self.principal_id,
                visibility="private", occurred_at=record.occurred_at,
                provenance=provenance, parent=record.native_parent_id,
            )
        return canonical_envelope(
            source_id=self.source_id, native_id=record.native_id, kind="connector_record",
            content=content, principal_id=self.principal_id, visibility="private",
            occurred_at=record.occurred_at, provenance=provenance,
            parent=record.native_parent_id,
        )

    def _acknowledged(self, event: dict[str, Any]) -> bool:
        return self.db.execute(
            "SELECT 1 FROM acknowledged_records WHERE native_sha256=? AND content_sha256=?",
            (_native_sha256(self.source_id, event["native_id"]), event["content_sha256"]),
        ).fetchone() is not None

    def _stage(self, page: ConnectorPage, cursor: str | None) -> dict[str, Any]:
        if page.next_cursor == cursor and (page.records or page.has_more):
            raise ConnectorContractError("connector cursor did not advance")
        receipts = []
        events = []
        dropped = 0
        deduplicated = 0
        archived = 0
        forgotten = 0
        for record in page.records:
            was_forgotten = False
            was_deduplicated = False
            provenance_decision = PrivacyPolicy(mode="scrub").apply(record.provenance)
            safe_provenance = provenance_decision.value
            if record.deleted:
                decision = PrivacyPolicy(mode="off").apply(
                    {"content": {}, "provenance": safe_provenance}
                )
            else:
                privacy_content = record.content
                artifact_digest = record.content.get("artifact_content_sha256")
                proven_digest = (
                    record.schema_version == CONNECTOR_SCHEMA_VERSION_V2
                    and record.content.get("kind") == "document.v1"
                    and isinstance(artifact_digest, str)
                    and SHA256.fullmatch(artifact_digest) is not None
                    and record.archive_payload is not None
                    and artifact_digest == hashlib.sha256(record.archive_payload).hexdigest()
                )
                if proven_digest:
                    # An exact raw-byte checksum is identity metadata, not prose.
                    # Keep all other fields under the configured privacy policy.
                    privacy_content = dict(record.content)
                    del privacy_content["artifact_content_sha256"]
                decision = self.privacy.apply(
                    {"content": privacy_content, "provenance": safe_provenance}
                )
                if proven_digest and decision.action != "drop":
                    decision.value["content"]["artifact_content_sha256"] = artifact_digest
            if decision.action == "drop":
                event = None
            else:
                content = {} if record.deleted else decision.value["content"]
                preliminary = self._event(
                    record,
                    content,
                    decision.value["provenance"],
                )
                if self._acknowledged(preliminary):
                    event = None
                    deduplicated += 1
                    was_deduplicated = True
                else:
                    try:
                        artifact_ref = self._archive_raw(record)
                    except ConnectorRunError as error:
                        if error.error_code != "archive_identity_forgotten":
                            raise
                        artifact_ref = None
                        event = None
                        forgotten += 1
                        was_forgotten = True
                    else:
                        archived += int(artifact_ref is not None)
                        event = self._event(
                            record,
                            content,
                            decision.value["provenance"],
                            artifact_ref,
                        )
            receipts.append(decision.receipt())
            if event is None:
                dropped += int(not was_forgotten and not was_deduplicated)
            else:
                events.append(event)
        privacy = summarize_receipts(receipts, self.privacy.mode)
        with self.db:
            page_id = self.db.execute(
                "INSERT INTO pages(cursor_before,cursor_after,has_more,created_at) VALUES (?,?,?,?)",
                (json.dumps(cursor), json.dumps(page.next_cursor), int(page.has_more), time.time()),
            ).lastrowid
            self.db.executemany(
                "INSERT INTO outbox(page_id,envelope_json,state) VALUES (?,?,'pending')",
                [(page_id, json.dumps(event, sort_keys=True, separators=(",", ":"))) for event in events],
            )
            if not events:
                self._commit_page(page_id, page.next_cursor)
        return {
            "privacy": privacy, "staged": len(events), "dropped": dropped,
            "deduplicated": deduplicated, "archived": archived,
            "forgotten": forgotten, "has_more": page.has_more,
        }

    def _commit_page(self, page_id: int, cursor: str | None) -> None:
        if self.checkpoints is not None:
            self.checkpoints.commit_checkpoint(self._cursor(), cursor)
        self.db.execute("DELETE FROM outbox WHERE page_id=?", (page_id,))
        self.db.execute("DELETE FROM pages WHERE id=?", (page_id,))
        self._set_meta("committed_cursor", json.dumps(cursor))
        self._set_meta("last_success_epoch", str(int(time.time())))
        self._clear_error()

    def _purge_acknowledged_bytes(self) -> None:
        checkpoint = self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint is None or checkpoint[0] != 0:
            raise ConnectorRunError("connector_spool_purge_failed")

    def _repair_pending_artifact_digests(
        self, rows: list[sqlite3.Row], events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Repair only the known checksum scrub defect in our private spool.

        The reference is the persisted witness checked by _archive_raw before
        staging. This does not fetch raw bytes or establish new remote authority.
        Prove every candidate before atomically replacing any queued identity.
        """
        repairs = []
        repaired_events = list(events)
        for index, (row, event) in enumerate(zip(rows, events)):
            content = event.get("content")
            provenance = event.get("provenance")
            if not (
                isinstance(content, dict) and content.get("kind") == "document.v1"
                and "artifact_content_sha256" in content
                and isinstance(provenance, dict)
                and provenance.get("connector_schema_version") == 2
            ):
                continue
            damaged = content["artifact_content_sha256"]
            if isinstance(damaged, str) and SHA256.fullmatch(damaged):
                continue
            refusal_code = "archive_invalid_reference"
            try:
                reference = validate_contract(
                    provenance.get("artifact_ref"), expected="recall.artifact-ref.v1",
                )
                if not (
                    reference.get("tenant_id") == self.tenant_id
                    and reference.get("source_id") == self.source_id
                    and reference.get("created_at") == event.get("occurred_at")
                    and reference.get("media_type") == content.get("mime_type")
                    and 0 <= reference["size_bytes"] <= MAX_ARCHIVE_OVERRIDE_BYTES
                ):
                    raise ValueError
                refusal_code = "connector_invalid_page"
                digest = reference["content_sha256"]
                old_digest = hashlib.sha256(canonical_json(content)).hexdigest()
                legacy = PrivacyPolicy(mode="scrub").apply(digest).value
                native_id = event["native_id"]
                known_attachment = (
                    self.connector_id == "slack.messages"
                    and native_id.startswith("slack-file:")
                    and content.get("surface") == "slack"
                ) or (
                    self.connector_id == "google.gmail"
                    and native_id.startswith("gmail-attachment:")
                    and content.get("surface") == "gmail_attachment"
                )
                if not (
                    known_attachment
                    and type(damaged) is str
                    and "[REDACTED:financial_id]" in damaged
                    and damaged == legacy and legacy != digest
                    and type(event.get("schema_version")) is int
                    and event["schema_version"] == 1
                    and type(provenance.get("connector_schema_version")) is int
                    and event.get("kind") == "connector_record"
                    and event.get("content_type") == "application/json"
                    and event.get("visibility") == "private"
                    and event.get("source_id") == self.source_id
                    and event.get("principal_id") == self.principal_id
                    and event.get("content_sha256") == old_digest
                    and provenance.get("connector_id") == self.connector_id
                    and content.get("document_id") == native_id
                    and content.get("parent_id") == event.get("native_parent_id")
                    and isinstance(event.get("native_parent_id"), str)
                ):
                    raise ValueError
                fixed_content = {**content, "artifact_content_sha256": digest}
                # Apply the existing SDK contract, including cross-field fidelity.
                ConnectorRecordV2(
                    schema_version=2, native_id=native_id,
                    native_parent_id=event["native_parent_id"],
                    occurred_at=event["occurred_at"], content=fixed_content,
                    provenance=provenance,
                )
                fixed = {**event, "content": fixed_content,
                         "content_sha256": hashlib.sha256(canonical_json(fixed_content)).hexdigest()}
                expected = canonical_envelope(
                    source_id=self.source_id, native_id=native_id, kind="connector_record",
                    content=fixed_content, principal_id=self.principal_id, visibility="private",
                    occurred_at=event["occurred_at"], parent=event["native_parent_id"],
                    provenance=provenance,
                )
                if fixed != expected:
                    raise ValueError
                rendered = json.dumps(fixed, sort_keys=True, separators=(",", ":"), allow_nan=False)
            except (ContractError, ValueError, TypeError, KeyError, AttributeError):
                raise ConnectorRunError(refusal_code) from None
            repairs.append((rendered, row["id"], row["envelope_json"]))
            repaired_events[index] = fixed
        if repairs:
            try:
                with self.db:
                    for rendered, row_id, original in repairs:
                        changed = self.db.execute(
                            "UPDATE outbox SET envelope_json=? WHERE id=? AND envelope_json=? AND state='pending'",
                            (rendered, row_id, original),
                        ).rowcount
                        if changed != 1:
                            raise ConnectorRunError("connector_spool_error")
            except sqlite3.Error:
                raise ConnectorRunError("connector_spool_error") from None
        return repaired_events

    def flush(self) -> dict[str, int]:
        page = self.db.execute("SELECT * FROM pages ORDER BY id LIMIT 1").fetchone()
        if page is None:
            return {"acked": 0, "replayed": 0}
        rows = list(self.db.execute("SELECT * FROM outbox WHERE page_id=? ORDER BY id", (page["id"],)))
        if not rows:
            with self.db:
                self._commit_page(page["id"], json.loads(page["cursor_after"]))
            return {"acked": 0, "replayed": 0}
        events = [json.loads(row["envelope_json"]) for row in rows]
        events = self._repair_pending_artifact_digests(rows, events)
        try:
            acknowledgement = self.brain.ingest(events)
        except PermissionError:
            self._record_error("brain_unauthorized")
            raise ConnectorRunError("brain_unauthorized") from None
        except Exception:
            self._record_error("brain_unavailable")
            raise ConnectorRunError("brain_unavailable") from None
        try:
            _validate_acknowledgement(acknowledgement, events)
        except ConnectorRunError:
            self._record_error("brain_invalid_acknowledgement")
            raise
        replayed = int(bool(self._get_meta("last_error_code") == "brain_unavailable"))
        with self.db:
            acknowledged_at = time.time()
            self.db.executemany(
                "INSERT OR IGNORE INTO acknowledged_records"
                "(native_sha256,content_sha256,acknowledged_at) VALUES (?,?,?)",
                [
                    (_native_sha256(self.source_id, event["native_id"]), event["content_sha256"], acknowledged_at)
                    for event in events
                ],
            )
            self._commit_page(page["id"], json.loads(page["cursor_after"]))
        self._purge_acknowledged_bytes()
        return {"acked": len(events), "replayed": replayed}

    def run_once(self) -> dict[str, Any]:
        if not self.enabled:
            return {"status": "disabled", "error_code": "connector_disabled"}
        pending = self.db.execute(
            "SELECT has_more FROM pages ORDER BY id LIMIT 1"
        ).fetchone()
        if pending:
            flushed = self.flush()
            return {
                "status": "committed",
                "has_more": bool(pending["has_more"]),
                **flushed,
            }
        cursor = self._cursor()
        try:
            page = self.connector.pull(cursor)
        except ConnectorRateLimited as error:
            self._record_error("connector_rate_limited")
            base = min(3600, max(1, int(error.retry_after_seconds)))
            jitter = 0.9 + secrets.randbelow(21) / 100
            return {
                "status": "backoff", "error_code": "connector_rate_limited",
                "retry_after_seconds": min(3600, max(1, int(base * jitter))),
            }
        except ConnectorUpstreamError as error:
            self._record_error(error.error_code)
            raise ConnectorRunError(error.error_code) from None
        except ConnectorContractError:
            self._record_error("connector_invalid_page")
            raise
        except Exception:
            self._record_error("connector_unavailable")
            raise ConnectorRunError("connector_unavailable") from None
        if not isinstance(page, ConnectorPage):
            self._record_error("connector_invalid_page")
            raise ConnectorContractError("pull must return ConnectorPage")
        try:
            staged = self._stage(page, cursor)
        except ConnectorRunError as error:
            self._record_error(error.error_code)
            raise
        except ConnectorContractError:
            self._record_error("connector_invalid_page")
            raise
        except Exception:
            self._record_error("connector_spool_error")
            raise ConnectorRunError("connector_spool_error") from None
        flushed = self.flush()
        return {"status": "committed", **staged, **flushed}

    def doctor(self) -> dict[str, Any]:
        return {
            "connector_id": self.connector_id,
            "source_id": self.source_id,
            "enabled": self.enabled,
            "checkpointed": self._get_meta("committed_cursor") is not None,
            "pending": self.db.execute("SELECT count(*) FROM outbox").fetchone()[0],
            "pending_pages": self.db.execute("SELECT count(*) FROM pages").fetchone()[0],
            "privacy_mode": self.privacy.mode,
            "privacy_policy_version": self.privacy.apply({}).policy_version,
            "last_success_epoch": int(self._get_meta("last_success_epoch") or 0),
            "last_error_code": self._get_meta("last_error_code"),
            **({"coverage": self.checkpoints.checkpoint_status()}
               if self.checkpoints is not None else {}),
        }
