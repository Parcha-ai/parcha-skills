from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.request
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from privacy.policy import PrivacyPolicy, summarize_receipts
from privacy.transport import open_no_redirect

COLLECTOR_VERSION = 1
MAX_BATCH_BYTES = 8_000_000
MAX_CANONICAL_BATCH_EVENTS = 1_000
DEFAULT_MAX_SCAN_RECORDS = 1_000
DEFAULT_MAX_SCAN_SECONDS = 20.0
OVERSIZED_PROJECTION_TEXT_CHARS = 250_000
SENSITIVE_KEY = re.compile(r"(?:litellm.*master.*key|api[_-]?key|password|secret|authorization|bearer|access[_-]?token|refresh[_-]?token|token)$", re.I)
SENSITIVE_LINE = re.compile(
    r"(?i)\b(LITELLM_MASTER_KEY|api[_-]?key|access[_-]?key|private[_-]?key|client[_-]?secret|password|secret|authorization|bearer|access[_-]?token|refresh[_-]?token|token|key)"
    r"\s*[=:]\s*\S{12,}|sk-[A-Za-z0-9_-]{20,}|xox[baprs]-[A-Za-z0-9-]{10,}|"
    r"(?:gh[pousr]|github_pat)_[A-Za-z0-9_]{20,}|AKIA[A-Z0-9]{16}|AIza[A-Za-z0-9_-]{30,}"
)
PRIVATE_KEY_BLOCK = re.compile(
    r"-----BEGIN (?P<label>[A-Z0-9 ]*PRIVATE KEY)-----.*?-----END (?P=label)-----",
    re.DOTALL,
)


class CollectorRuntimeError(RuntimeError):
    """A stable, content-free collector failure for local health surfaces."""

    def __init__(self, error_code: str):
        self.error_code = error_code
        super().__init__(error_code)


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: "[REDACTED]" if SENSITIVE_KEY.search(str(key)) else sanitize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    if isinstance(value, str):
        without_nul = PRIVATE_KEY_BLOCK.sub("[REDACTED-PRIVATE-KEY]", value.replace("\x00", "[NUL]"))
        return "\n".join("[REDACTED]" if SENSITIVE_LINE.search(line) else line for line in without_nul.splitlines())
    return value


def fingerprint(path: Path, size: int | None = None) -> str:
    size = path.stat().st_size if size is None else size
    with path.open("rb") as source:
        first = source.read(min(4096, size))
        source.seek(max(0, size - 4096))
        last = source.read(min(4096, size))
    return hashlib.sha256(first + last + str(size).encode()).hexdigest()


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalized_timestamp(value: Any, fallback_epoch: float) -> str:
    try:
        if isinstance(value, (int, float)):
            parsed = datetime.fromtimestamp(float(value), timezone.utc)
        elif isinstance(value, str) and value:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            raise ValueError
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except (ValueError, TypeError, OverflowError):
        return datetime.fromtimestamp(fallback_epoch, timezone.utc).isoformat().replace("+00:00", "Z")


class Collector:
    def __init__(self, *, root: Path, harness: str, source_id: str, spool_path: Path,
                 endpoint: str, token: str, principal_id: str = "owner",
                 visibility: str = "private", batch_size: int = 500,
                 privacy: PrivacyPolicy | None = None, brain_writer: Any = None,
                 archive: Any = None, tenant_id: str | None = None,
                 archive_workers: int = 2,
                 max_scan_records: int = DEFAULT_MAX_SCAN_RECORDS,
                 max_scan_seconds: float = DEFAULT_MAX_SCAN_SECONDS,
                 bulk_manifest_archive: bool = False,
                 bulk_bundle_records: int = 500):
        if harness not in {"claude", "codex"}:
            raise ValueError("harness must be claude or codex")
        if visibility not in {"private", "shared"}:
            raise ValueError("visibility must be private or shared")
        if (brain_writer is None) != (archive is None) or (
            archive is not None and not tenant_id
        ):
            raise ValueError("canonical collector runtime is incomplete")
        if (
            type(archive_workers) is not int
            or not 1 <= archive_workers <= 16
        ):
            raise ValueError("archive_workers must be between 1 and 16")
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        if type(max_scan_records) is not int or not 1 <= max_scan_records <= 100_000:
            raise ValueError("max_scan_records must be between 1 and 100000")
        if (
            isinstance(max_scan_seconds, bool)
            or not isinstance(max_scan_seconds, (int, float))
            or not 0.1 <= float(max_scan_seconds) <= 300.0
        ):
            raise ValueError("max_scan_seconds must be between 0.1 and 300")
        if type(bulk_manifest_archive) is not bool:
            raise ValueError("bulk_manifest_archive must be a boolean")
        if bulk_manifest_archive and archive is None:
            raise ValueError("bulk manifest archive requires canonical runtime")
        if (
            type(bulk_bundle_records) is not int
            or not 1 <= bulk_bundle_records <= 10_000
        ):
            raise ValueError("bulk_bundle_records must be between 1 and 10000")
        self.root = Path(root).expanduser().resolve()
        self.harness = harness
        self.source_id = source_id
        self.spool_path = Path(spool_path).expanduser()
        self.endpoint = endpoint.rstrip("/")
        self.token = token
        self.principal_id = principal_id
        self.visibility = visibility
        self.batch_size = (
            min(batch_size, MAX_CANONICAL_BATCH_EVENTS)
            if brain_writer is not None
            else batch_size
        )
        self.privacy = privacy or PrivacyPolicy(mode="off")
        self.brain_writer = brain_writer
        self.archive = archive
        self.tenant_id = tenant_id
        self.archive_workers = archive_workers
        self.max_scan_records = max_scan_records
        self.max_scan_seconds = float(max_scan_seconds)
        self.bulk_manifest_archive = bulk_manifest_archive
        self.bulk_bundle_records = bulk_bundle_records
        self.shard_count = 1
        self.shard_index = 0
        self.spool_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.spool_path.parent, 0o700)
        self.db = sqlite3.connect(self.spool_path)
        self.db.row_factory = sqlite3.Row
        os.chmod(self.spool_path, 0o600)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=30000")
        self.db.execute("PRAGMA synchronous=FULL")
        self._migrate()
        self._migrate_privacy_state()

    def _migrate(self) -> None:
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS files(
          path TEXT PRIMARY KEY, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,
          fingerprint TEXT NOT NULL, scanned_offset INTEGER NOT NULL DEFAULT 0,
          committed_offset INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL,
          last_scan_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS active_records(
          path TEXT NOT NULL, native_id TEXT NOT NULL, content_sha256 TEXT NOT NULL,
          start_offset INTEGER NOT NULL, end_offset INTEGER NOT NULL, receipt TEXT,
          PRIMARY KEY(path,native_id));
        CREATE TABLE IF NOT EXISTS record_generations(
          native_id TEXT PRIMARY KEY, generation INTEGER NOT NULL,
          base_content_sha256 TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS scan_members(
          path TEXT NOT NULL, native_id TEXT NOT NULL,
          PRIMARY KEY(path,native_id));
        CREATE TABLE IF NOT EXISTS outbox(
          id INTEGER PRIMARY KEY, path TEXT NOT NULL, native_id TEXT NOT NULL,
          content_sha256 TEXT NOT NULL, start_offset INTEGER NOT NULL, end_offset INTEGER NOT NULL,
          shard_key INTEGER NOT NULL DEFAULT 0,
          envelope_json TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
          queued_at REAL NOT NULL, acked_at REAL, receipt TEXT,
          UNIQUE(native_id,content_sha256));
        CREATE INDEX IF NOT EXISTS outbox_state_id ON outbox(state,id);
        CREATE TABLE IF NOT EXISTS dead_letters(
          id INTEGER PRIMARY KEY, path TEXT NOT NULL, byte_offset INTEGER NOT NULL,
          error_code TEXT NOT NULL, error_summary TEXT NOT NULL, created_at REAL NOT NULL,
          UNIQUE(path,byte_offset,error_code));
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        """)
        columns = {row["name"] for row in self.db.execute("PRAGMA table_info(outbox)")}
        if "start_offset" not in columns:
            self.db.execute("ALTER TABLE outbox ADD COLUMN start_offset INTEGER NOT NULL DEFAULT 0")
        if "shard_key" not in columns:
            self.db.execute("ALTER TABLE outbox ADD COLUMN shard_key INTEGER NOT NULL DEFAULT 0")
            self.db.execute("CREATE INDEX IF NOT EXISTS outbox_path_idx ON outbox(path)")
            for row in self.db.execute("SELECT DISTINCT path FROM outbox"):
                self.db.execute("UPDATE outbox SET shard_key=? WHERE path=?", (self._path_shard(row["path"]), row["path"]))
        self.db.execute("CREATE INDEX IF NOT EXISTS outbox_shard_state_id ON outbox(shard_key,state,id)")
        self.db.execute(
            "DELETE FROM dead_letters "
            "WHERE error_code='RecoveryError' AND EXISTS ("
            "SELECT 1 FROM outbox "
            "WHERE outbox.path=dead_letters.path "
            "AND outbox.start_offset=dead_letters.byte_offset "
            "AND outbox.state='acked')"
        )
        last_ack = self.db.execute(
            "SELECT max(acked_at) FROM outbox WHERE state='acked' AND acked_at IS NOT NULL"
        ).fetchone()[0]
        if last_ack is not None:
            self.db.execute(
                "INSERT OR IGNORE INTO meta(key,value) VALUES ('last_success_epoch',?)",
                (str(int(last_ack)),),
            )
        self.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('collector_version',?)", (str(COLLECTOR_VERSION),))
        self.db.commit()

    def _migrate_privacy_state(self) -> None:
        state = f"{self.privacy.mode}:{self.privacy.apply({}).policy_version}"
        previous = self.db.execute("SELECT value FROM meta WHERE key='privacy_policy_state'").fetchone()
        if previous is not None and previous["value"] == state:
            return
        if self.privacy.mode != "off":
            self.db.execute("PRAGMA secure_delete=ON")
            for row in list(self.db.execute("SELECT * FROM outbox WHERE state='pending' ORDER BY id")):
                # A privacy-policy migration must not turn startup into an
                # unbounded archive backfill. Re-scrub the durable envelope
                # here; the ordinary bounded flush repairs missing artifact
                # references immediately before their batch is committed.
                self._repair_pending_envelope(row, repair_artifact=False)
            self.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('privacy_policy_state',?)", (state,))
            self.db.commit()
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self.db.execute("VACUUM")
            self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        else:
            self.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('privacy_policy_state',?)", (state,))
            self.db.commit()

    def close(self) -> None:
        self.db.close()

    def _set_meta(self, key: str, value: str) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)",
            (key, value),
        )

    def _record_error(self, error_code: str) -> None:
        self._set_meta("last_error_code", error_code)
        self.db.commit()

    def _clear_error(self) -> None:
        self.db.execute("DELETE FROM meta WHERE key='last_error_code'")

    def _clear_running(self) -> None:
        self.db.execute("DELETE FROM meta WHERE key='running_started_epoch'")

    def discover(self) -> list[Path]:
        if not self.root.exists():
            return []
        pattern = "rollout-*.jsonl" if self.harness == "codex" else "*.jsonl"
        paths = [path for path in self.root.rglob(pattern) if path.is_file()]
        return sorted(
            paths,
            key=lambda path: (path.stat().st_mtime_ns, str(path)),
            reverse=True,
        )

    def _file_key(self, path: Path) -> str:
        relative = str(path.relative_to(self.root))
        return hashlib.sha256((self.harness + "\x1f" + relative).encode()).hexdigest()[:24]

    @staticmethod
    def _path_shard(path: str) -> int:
        return int.from_bytes(hashlib.sha256(path.encode()).digest()[:8], "big") & ((1 << 63) - 1)

    def _envelope(self, path: Path, native_id: str, kind: str, content: Any,
                  occurred_at: str, start: int, end: int,
                  artifact_ref: dict[str, Any] | None = None,
                  artifact_member: dict[str, Any] | None = None) -> dict:
        clean = sanitize(content)
        provenance = {
            "harness": self.harness,
            "connector_id": f"{self.harness}.jsonl",
            "connector_schema_version": COLLECTOR_VERSION,
            "collector_version": COLLECTOR_VERSION,
            "privacy_policy_version": self.privacy.apply({}).policy_version,
            "original_path": str(path),
            "byte_start": start,
            "byte_end": end,
        }
        if artifact_ref is not None:
            provenance["artifact_ref"] = artifact_ref
        if artifact_member is not None:
            provenance["artifact_member"] = artifact_member
        return {
            "schema_version": 1,
            "source_id": self.source_id,
            "native_id": native_id,
            "native_parent_id": f"{self.harness}-session-{self._file_key(path)}",
            "kind": kind,
            "occurred_at": occurred_at,
            "observed_at": iso_now(),
            "principal_id": self.principal_id,
            "visibility": self.visibility,
            "content_type": "application/json",
            "content": clean,
            "content_sha256": hashlib.sha256(canonical_json(clean)).hexdigest(),
            "provenance": provenance,
        }

    def _archive_raw(
        self,
        *,
        native_id: str,
        payload: bytes,
        occurred_at: str,
        media_type: str = "application/x-ndjson",
    ) -> dict[str, Any] | None:
        if self.archive is None:
            return None
        try:
            return self.archive.put_raw(
                tenant_id=self.tenant_id,
                source_id=self.source_id,
                native_id=native_id,
                payload=payload,
                media_type=media_type,
                created_at=occurred_at,
            )
        except Exception:
            raise CollectorRuntimeError("archive_unavailable") from None

    def _archive_manifest(
        self,
        pending: list[tuple[
            str, dict[str, Any], str, int, int,
            Future[dict[str, Any] | None] | None,
            dict[str, Any] | None,
        ]],
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        records = [
            {
                "ordinal": ordinal,
                "native_id": item[0],
                "content_sha256": hashlib.sha256(
                    canonical_json(sanitize(item[1]))
                ).hexdigest(),
                "byte_start": item[3],
                "byte_end": item[4],
            }
            for ordinal, item in enumerate(pending)
        ]
        payload = canonical_json({
            "contract": "recall.bulk-manifest.v1",
            "schema_version": 1,
            "privacy_policy_version": self.privacy.apply({}).policy_version,
            "record_count": len(records),
            "records": records,
        })
        digest = hashlib.sha256(payload).hexdigest()
        reference = self._archive_raw(
            native_id="bulk-" + digest,
            payload=payload,
            occurred_at=min(item[2] for item in pending),
            media_type="application/vnd.recall.bulk-manifest+json",
        )
        if reference is None:
            raise CollectorRuntimeError("archive_unavailable")
        return reference, [
            {
                "contract": "recall.artifact-member.v1",
                "schema_version": 1,
                **record,
                "manifest_sha256": digest,
            }
            for record in records
        ]

    def _bounded_record_envelope(
        self,
        *,
        path: Path,
        native_id: str,
        content: dict[str, Any],
        occurred_at: str,
        start: int,
        end: int,
        artifact_ref: dict[str, Any] | None = None,
        artifact_member: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        envelope = self._envelope(
            path,
            native_id,
            "transcript_record",
            content,
            occurred_at,
            start,
            end,
            artifact_ref,
            artifact_member,
        )
        target_bytes = max(1_024, int(self._max_batch_bytes() * 0.9))
        if len(canonical_json(envelope)) <= target_bytes or self.archive is None:
            return envelope

        full_payload = canonical_json(sanitize(content))
        full_digest = hashlib.sha256(full_payload).hexdigest()
        compressed_payload = gzip.compress(
            full_payload,
            compresslevel=6,
            mtime=0,
        )
        full_artifact = self._archive_raw(
            native_id=native_id + ":full",
            payload=compressed_payload,
            occurred_at=occurred_at,
            media_type="application/vnd.recall.oversized-record+gzip",
        )
        if full_artifact is None:
            return envelope

        rendered = full_payload.decode("utf-8")
        text_chars = min(
            OVERSIZED_PROJECTION_TEXT_CHARS,
            max(0, target_bytes // 4),
        )
        while True:
            projection = {
                "contract": "recall.oversized-projection.v1",
                "schema_version": 1,
                "_recall_collector_generation": content.get(
                    "_recall_collector_generation",
                    0,
                ),
                "content_fidelity": "head_tail",
                "full_record_available": True,
                "full_content_sha256": full_digest,
                "full_size_bytes": len(full_payload),
                "archive_encoding": "gzip",
                "archive_size_bytes": len(compressed_payload),
                "head": rendered[:text_chars],
                "tail": rendered[-text_chars:] if text_chars else "",
            }
            candidate = self._envelope(
                path,
                native_id,
                "transcript_record",
                projection,
                occurred_at,
                start,
                end,
                full_artifact,
            )
            if len(canonical_json(candidate)) <= target_bytes or text_chars == 0:
                return candidate
            text_chars //= 2

    def _versioned_record_content(self, native_id: str, content: dict, *, was_active: bool) -> dict:
        clean = sanitize(content)
        base_sha = hashlib.sha256(canonical_json(clean)).hexdigest()
        row = self.db.execute("SELECT generation,base_content_sha256 FROM record_generations WHERE native_id=?", (native_id,)).fetchone()
        generation = 0 if row is None else int(row["generation"]) + int(not was_active or row["base_content_sha256"] != base_sha)
        self.db.execute(
            "INSERT INTO record_generations(native_id,generation,base_content_sha256) VALUES (?,?,?) "
            "ON CONFLICT(native_id) DO UPDATE SET generation=excluded.generation,base_content_sha256=excluded.base_content_sha256",
            (native_id, generation, base_sha),
        )
        return {**clean, "_recall_collector_generation": generation}

    def _queue(self, path: Path, envelope: dict, end_offset: int) -> bool:
        cursor = self.db.execute(
            "INSERT OR IGNORE INTO outbox(path,native_id,content_sha256,start_offset,end_offset,shard_key,envelope_json,queued_at) VALUES (?,?,?,?,?,?,?,?)",
            (str(path), envelope["native_id"], envelope["content_sha256"], envelope["provenance"]["byte_start"], end_offset, self._path_shard(str(path)),
             canonical_json(envelope).decode(), time.time()),
        )
        return cursor.rowcount == 1

    def _save_file_progress(self, path: str, stat, current_fingerprint: str, offset: int,
                            status: str, scan_id: str) -> None:
        self.db.execute(
            """INSERT INTO files(path,size,mtime_ns,fingerprint,scanned_offset,committed_offset,status,last_scan_id)
               VALUES (?,?,?,?,?,0,?,?)
               ON CONFLICT(path) DO UPDATE SET size=excluded.size,mtime_ns=excluded.mtime_ns,
               fingerprint=excluded.fingerprint,scanned_offset=excluded.scanned_offset,
               status=excluded.status,last_scan_id=excluded.last_scan_id""",
            (path, stat.st_size, stat.st_mtime_ns, current_fingerprint, offset, status, scan_id),
        )

    def scan(self) -> dict:
        """Run one bounded, resumable scan slice and publish content-free health."""

        self._set_meta("running_started_epoch", str(time.time()))
        self.db.commit()
        try:
            summary = self._scan()
        except CollectorRuntimeError as error:
            self._clear_running()
            self._record_error(error.error_code)
            raise
        except Exception:
            self._clear_running()
            self._record_error("scan_failed")
            raise
        self._clear_running()
        self._set_meta("last_scan_complete", "1" if summary["scan_complete"] else "0")
        self.db.execute(
            "DELETE FROM meta WHERE key='last_error_code' "
            "AND value IN ('archive_unavailable','scan_failed')"
        )
        self.db.commit()
        return summary

    def _scan(self) -> dict:
        scan_id = hashlib.sha256(f"{time.time_ns()}:{os.getpid()}".encode()).hexdigest()[:16]
        summary = {"files_seen": 0, "records_queued": 0, "tombstones_queued": 0,
                   "parse_errors": 0, "partial_files": 0, "scan_complete": True}
        scan_started = time.monotonic()
        records_seen = 0
        bounded = False
        privacy_receipts = []
        if self.brain_writer is not None:
            self.flush()
        executor = (
            ThreadPoolExecutor(max_workers=self.archive_workers)
            if (
                self.archive is not None
                and self.archive_workers > 1
                and not self.bulk_manifest_archive
            )
            else None
        )
        try:
            paths = self.discover()
            for path in paths:
                if (
                    records_seen >= self.max_scan_records
                    or time.monotonic() - scan_started >= self.max_scan_seconds
                ):
                    bounded = True
                    break
                if summary["files_seen"] and self.brain_writer is not None:
                    self.flush()
                summary["files_seen"] += 1
                stat = path.stat()
                path_text = str(path)
                row = self.db.execute("SELECT * FROM files WHERE path=?", (path_text,)).fetchone()
                current_fingerprint = fingerprint(path, stat.st_size)
                if row and not row["status"].startswith("scanning-") and row["size"] == stat.st_size and row["mtime_ns"] == stat.st_mtime_ns and row["fingerprint"] == current_fingerprint:
                    self.db.execute("UPDATE files SET last_scan_id=? WHERE path=?", (scan_id, path_text))
                    continue
                resume_mode = row["status"].removeprefix("scanning-") if row and row["status"].startswith("scanning-") else None
                if resume_mode:
                    mode = resume_mode
                    file_scan_id = row["last_scan_id"]
                else:
                    mode = "append" if row and stat.st_size > row["size"] and fingerprint(path, row["size"]) == row["fingerprint"] else ("full" if row else "new")
                    file_scan_id = scan_id
                    if mode != "append":
                        self.db.execute("DELETE FROM scan_members WHERE path=?", (path_text,))
                append = mode == "append"
                start_offset = int(row["scanned_offset"]) if append or resume_mode else 0
                old_active = {item["native_id"]: item for item in self.db.execute("SELECT * FROM active_records WHERE path=?", (path_text,))}
                seen_native: set[str] = set(old_active) if append else {item["native_id"] for item in self.db.execute("SELECT native_id FROM scan_members WHERE path=?", (path_text,))}
                complete_end = start_offset
                complete_records = 0
                pending: list[tuple[
                    str, dict[str, Any], str, int, int,
                    Future[dict[str, Any] | None] | None,
                    dict[str, Any] | None,
                ]] = []

                def commit_item(
                    item: tuple[
                        str, dict[str, Any], str, int, int,
                        Future[dict[str, Any] | None] | None,
                        dict[str, Any] | None,
                    ],
                    *,
                    shared_artifact: dict[str, Any] | None = None,
                    artifact_member: dict[str, Any] | None = None,
                ) -> None:
                    (
                        native_id, content, occurred_at, line_start, line_end,
                        future, artifact_ref,
                    ) = item
                    if future is not None:
                        artifact_ref = future.result()
                    if shared_artifact is not None:
                        artifact_ref = shared_artifact
                    versioned_content = self._versioned_record_content(
                        native_id,
                        content,
                        was_active=native_id in old_active,
                    )
                    envelope = self._bounded_record_envelope(
                        path=path,
                        native_id=native_id,
                        content=versioned_content,
                        occurred_at=occurred_at,
                        start=line_start,
                        end=line_end,
                        artifact_ref=artifact_ref,
                        artifact_member=artifact_member,
                    )
                    if self._queue(path, envelope, line_end):
                        summary["records_queued"] += 1
                    self.db.execute(
                        "INSERT INTO active_records(path,native_id,content_sha256,start_offset,end_offset) VALUES (?,?,?,?,?) "
                        "ON CONFLICT(path,native_id) DO UPDATE SET content_sha256=excluded.content_sha256,start_offset=excluded.start_offset,end_offset=excluded.end_offset",
                        (
                            path_text,
                            native_id,
                            envelope["content_sha256"],
                            line_start,
                            line_end,
                        ),
                    )
                    seen_native.add(native_id)
                    if not append:
                        self.db.execute(
                            "INSERT OR IGNORE INTO scan_members(path,native_id) VALUES (?,?)",
                            (path_text, native_id),
                        )

                def commit_pending() -> None:
                    commit_item(pending.pop(0))

                def commit_bulk() -> None:
                    if not pending:
                        return
                    artifact_ref, members = self._archive_manifest(pending)
                    items = list(pending)
                    pending.clear()
                    for item, member in zip(items, members, strict=True):
                        commit_item(
                            item,
                            shared_artifact=artifact_ref,
                            artifact_member=member,
                        )

                with path.open("rb") as source:
                    source.seek(start_offset)
                    while source.tell() < stat.st_size:
                        if (
                            records_seen >= self.max_scan_records
                            or time.monotonic() - scan_started >= self.max_scan_seconds
                        ):
                            bounded = True
                            break
                        line_start = source.tell()
                        line = source.readline(stat.st_size - line_start)
                        if not line:
                            break
                        if not line.endswith(b"\n"):
                            summary["partial_files"] += 1
                            break
                        complete_end = source.tell()
                        complete_records += 1
                        records_seen += 1
                        native_id = f"{self._file_key(path)}-{line_start:016x}"
                        try:
                            content = json.loads(line)
                            if not isinstance(content, dict):
                                raise ValueError("record is not an object")
                        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                            summary["parse_errors"] += 1
                            self.db.execute(
                                "INSERT OR IGNORE INTO dead_letters(path,byte_offset,error_code,error_summary,created_at) VALUES (?,?,?,?,?)",
                                (path_text, line_start, type(exc).__name__, "record rejected", time.time()),
                            )
                            continue
                        occurred_at = normalized_timestamp(content.get("timestamp"), stat.st_mtime)
                        privacy = self.privacy.apply(content)
                        privacy_receipts.append(privacy.receipt())
                        if privacy.action == "drop":
                            seen_native.add(native_id)
                            if not append:
                                self.db.execute("INSERT OR IGNORE INTO scan_members(path,native_id) VALUES (?,?)", (path_text, native_id))
                            continue
                        content = privacy.value
                        if self.bulk_manifest_archive:
                            future = None
                            artifact_ref = None
                        elif executor is None:
                            future = None
                            artifact_ref = self._archive_raw(
                                native_id=native_id,
                                payload=canonical_json(content),
                                occurred_at=occurred_at,
                            )
                        else:
                            future = executor.submit(
                                self._archive_raw,
                                native_id=native_id,
                                payload=canonical_json(content),
                                occurred_at=occurred_at,
                            )
                            artifact_ref = None
                        pending.append(
                            (
                                native_id,
                                content,
                                occurred_at,
                                line_start,
                                complete_end,
                                future,
                                artifact_ref,
                            )
                        )
                        if (
                            self.bulk_manifest_archive
                            and len(pending) >= self.bulk_bundle_records
                        ):
                            commit_bulk()
                        elif (
                            not self.bulk_manifest_archive
                            and len(pending) >= self.archive_workers * 2
                        ):
                            commit_pending()
                        if complete_records % 1000 == 0:
                            if self.bulk_manifest_archive:
                                commit_bulk()
                            else:
                                while pending:
                                    commit_pending()
                            self._save_file_progress(path_text, stat, current_fingerprint, complete_end, "scanning-" + mode, file_scan_id)
                            self.db.commit()
                            if self.brain_writer is not None:
                                self.flush()
                if self.bulk_manifest_archive:
                    commit_bulk()
                else:
                    while pending:
                        commit_pending()
                if bounded and complete_end < stat.st_size:
                    self._save_file_progress(
                        path_text,
                        stat,
                        current_fingerprint,
                        complete_end,
                        "scanning-" + mode,
                        file_scan_id,
                    )
                    if not self.db.execute(
                        "SELECT 1 FROM outbox WHERE path=? AND state='pending' LIMIT 1",
                        (path_text,),
                    ).fetchone():
                        self.db.execute(
                            "UPDATE files SET committed_offset=scanned_offset WHERE path=?",
                            (path_text,),
                        )
                    self.db.commit()
                    break
                if not append:
                    for native_id, old in old_active.items():
                        if native_id in seen_native:
                            continue
                        content = {"target_native_id": native_id, "deletion_id": scan_id}
                        occurred_at = iso_now()
                        artifact_ref = self._archive_raw(
                            native_id=native_id,
                            payload=canonical_json({
                                "deleted": True,
                                "native_id": native_id,
                            }),
                            occurred_at=occurred_at,
                        )
                        envelope = self._envelope(
                            path, native_id, "tombstone", content, occurred_at,
                            old["start_offset"], old["end_offset"], artifact_ref,
                        )
                        if self._queue(path, envelope, complete_end):
                            summary["tombstones_queued"] += 1
                        self.db.execute("DELETE FROM active_records WHERE path=? AND native_id=?", (path_text, native_id))
                status = "partial" if complete_end < stat.st_size else "ok"
                self._save_file_progress(path_text, stat, current_fingerprint, complete_end, status, scan_id)
                if not self.db.execute("SELECT 1 FROM outbox WHERE path=? AND state='pending' LIMIT 1", (path_text,)).fetchone():
                    self.db.execute("UPDATE files SET committed_offset=scanned_offset WHERE path=?", (path_text,))
                self.db.execute("DELETE FROM scan_members WHERE path=?", (path_text,))
                # Bound crash recovery to one source file; acknowledged offsets still move only in flush().
                self.db.commit()
        finally:
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)
        if not bounded:
            missing = list(self.db.execute(
                "SELECT path FROM files WHERE last_scan_id != ? AND status != 'tombstone'",
                (scan_id,),
            ))
            for item in missing:
                for old in self.db.execute(
                    "SELECT * FROM active_records WHERE path=?",
                    (item["path"],),
                ):
                    path = Path(item["path"])
                    occurred_at = iso_now()
                    artifact_ref = self._archive_raw(
                        native_id=old["native_id"],
                        payload=canonical_json({
                            "deleted": True,
                            "native_id": old["native_id"],
                        }),
                        occurred_at=occurred_at,
                    )
                    envelope = self._envelope(
                        path, old["native_id"], "tombstone",
                        {"target_native_id": old["native_id"], "deletion_id": scan_id},
                        occurred_at, old["start_offset"], old["end_offset"],
                        artifact_ref,
                    )
                    if self._queue(path, envelope, old["end_offset"]):
                        summary["tombstones_queued"] += 1
                self.db.execute(
                    "DELETE FROM active_records WHERE path=?",
                    (item["path"],),
                )
                self.db.execute(
                    "UPDATE files SET status='tombstone',last_scan_id=? WHERE path=?",
                    (scan_id, item["path"]),
                )
        self.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('last_scan_at',?)", (str(time.time()),))
        self.db.commit()
        summary["scan_complete"] = not bounded
        summary["privacy"] = summarize_receipts(privacy_receipts, self.privacy.mode)
        return summary

    def pending_envelopes(self) -> list[dict]:
        return [json.loads(row["envelope_json"]) for row in self.db.execute("SELECT envelope_json FROM outbox WHERE state='pending' ORDER BY id")]

    def _after_remote_commit(self, acknowledgement: dict[str, Any]) -> None:
        """Fault-injection boundary after a durable remote commit and before local ACK."""

    def recover_dead_payloads(self) -> dict:
        result = {"recovered": 0, "unrecoverable": 0}
        rows = list(self.db.execute("SELECT * FROM outbox WHERE state='dead' ORDER BY id"))
        for row in rows:
            try:
                path = Path(row["path"])
                with path.open("rb") as source:
                    source.seek(row["start_offset"])
                    raw = source.read(row["end_offset"] - row["start_offset"])
                if not raw.endswith(b"\n"):
                    raise ValueError("source byte window is no longer a complete record")
                content = json.loads(raw)
                if not isinstance(content, dict):
                    raise ValueError("record is not an object")
                privacy = self.privacy.apply(content)
                if privacy.action == "drop":
                    self.db.execute("DELETE FROM outbox WHERE id=?", (row["id"],))
                    result["recovered"] += 1
                    continue
                versioned = self._versioned_record_content(row["native_id"], privacy.value, was_active=True)
                occurred_at = normalized_timestamp(
                    content.get("timestamp"),
                    path.stat().st_mtime,
                )
                envelope = self._bounded_record_envelope(
                    path=path,
                    native_id=row["native_id"],
                    content=versioned,
                    occurred_at=occurred_at,
                    start=row["start_offset"],
                    end=row["end_offset"],
                )
                self.db.execute(
                    "UPDATE outbox SET state='pending',content_sha256=?,envelope_json=?,queued_at=?,acked_at=NULL,receipt=NULL WHERE id=?",
                    (envelope["content_sha256"], canonical_json(envelope).decode(), time.time(), row["id"]),
                )
                self.db.execute(
                    """UPDATE active_records
                       SET content_sha256=?,receipt=NULL
                       WHERE path=? AND native_id=? AND content_sha256=?""",
                    (
                        envelope["content_sha256"],
                        row["path"],
                        row["native_id"],
                        row["content_sha256"],
                    ),
                )
                self.db.execute(
                    "DELETE FROM dead_letters WHERE path=? AND error_code='PayloadTooLarge'",
                    (row["path"],),
                )
                result["recovered"] += 1
            except (
                OSError,
                TypeError,
                ValueError,
                json.JSONDecodeError,
                UnicodeDecodeError,
            ):
                self.db.execute(
                    "INSERT OR IGNORE INTO dead_letters(path,byte_offset,error_code,error_summary,created_at) VALUES (?,?,?,?,?)",
                    (row["path"], row["start_offset"], "RecoveryError", "record recovery rejected", time.time()),
                )
                result["unrecoverable"] += 1
        self.db.commit()
        return result

    def _repair_pending_envelope(
        self,
        row: sqlite3.Row,
        *,
        repair_artifact: bool = True,
    ) -> dict | None:
        envelope = json.loads(row["envelope_json"])
        if (
            repair_artifact
            and self.archive is not None
            and "artifact_ref" not in envelope.get("provenance", {})
        ):
            try:
                if envelope.get("kind") == "tombstone":
                    raw = canonical_json({
                        "deleted": True,
                        "native_id": row["native_id"],
                    })
                    content = envelope["content"]
                else:
                    path = Path(row["path"])
                    with path.open("rb") as source:
                        source.seek(row["start_offset"])
                        raw = source.read(row["end_offset"] - row["start_offset"])
                    if not raw.endswith(b"\n"):
                        raise ValueError("source byte window is incomplete")
                    original = json.loads(raw)
                    if not isinstance(original, dict):
                        raise ValueError("record is not an object")
                    privacy = self.privacy.apply(original)
                    if privacy.action == "drop":
                        self.db.execute("DELETE FROM outbox WHERE id=?", (row["id"],))
                        return None
                    raw = canonical_json(privacy.value)
                    content = envelope["content"]
                    expected_content = dict(content)
                    generation = expected_content.pop(
                        "_recall_collector_generation",
                        None,
                    )
                    if (
                        not isinstance(generation, int)
                        or sanitize(privacy.value) != expected_content
                    ):
                        raise ValueError("source byte window changed")
                artifact_ref = self._archive_raw(
                    native_id=row["native_id"],
                    payload=raw,
                    occurred_at=envelope["occurred_at"],
                )
                envelope = self._envelope(
                    Path(row["path"]),
                    row["native_id"],
                    envelope["kind"],
                    content,
                    envelope["occurred_at"],
                    row["start_offset"],
                    row["end_offset"],
                    artifact_ref,
                )
                rendered = canonical_json(envelope).decode()
                self.db.execute(
                    "UPDATE outbox SET envelope_json=?,queued_at=? WHERE id=?",
                    (rendered, time.time(), row["id"]),
                )
                repaired = dict(row)
                repaired["envelope_json"] = rendered
                row = repaired
            except (
                OSError,
                TypeError,
                ValueError,
                json.JSONDecodeError,
                UnicodeDecodeError,
            ):
                self.db.execute(
                    "UPDATE outbox SET state='dead' WHERE id=?",
                    (row["id"],),
                )
                self.db.execute(
                    "INSERT OR IGNORE INTO dead_letters(path,byte_offset,error_code,error_summary,created_at) VALUES (?,?,?,?,?)",
                    (
                        row["path"],
                        row["start_offset"],
                        "RecoveryError",
                        "record recovery rejected",
                        time.time(),
                    ),
                )
                return None
        if envelope.get("kind") == "tombstone":
            clean = sanitize(envelope["content"])
        else:
            privacy = self.privacy.apply(envelope["content"])
            if privacy.action == "drop":
                self.db.execute("DELETE FROM outbox WHERE id=?", (row["id"],))
                return None
            clean = sanitize(privacy.value)
        if clean == envelope["content"]:
            return dict(row)
        old_sha = envelope["content_sha256"]
        envelope["content"] = clean
        envelope["content_sha256"] = hashlib.sha256(canonical_json(clean)).hexdigest()
        rendered = canonical_json(envelope).decode()
        duplicate = self.db.execute(
            "SELECT id,state,receipt FROM outbox WHERE native_id=? AND content_sha256=? AND id<>?",
            (row["native_id"], envelope["content_sha256"], row["id"]),
        ).fetchone()
        if duplicate is not None and duplicate["state"] in {"pending", "acked"}:
            receipt = duplicate["receipt"] if duplicate["state"] == "acked" else None
            self.db.execute("DELETE FROM outbox WHERE id=?", (row["id"],))
            self.db.execute(
                "UPDATE active_records SET content_sha256=?,receipt=? WHERE path=? AND native_id=?",
                (envelope["content_sha256"], receipt, row["path"], row["native_id"]),
            )
            if duplicate["state"] == "acked":
                pending = self.db.execute(
                    "SELECT 1 FROM outbox WHERE path=? AND state='pending' LIMIT 1", (row["path"],)
                ).fetchone()
                if not pending:
                    self.db.execute("UPDATE files SET committed_offset=scanned_offset WHERE path=?", (row["path"],))
            return None
        self.db.execute(
            "UPDATE outbox SET content_sha256=?,envelope_json=? WHERE id=?",
            (envelope["content_sha256"], rendered, row["id"]),
        )
        self.db.execute(
            "UPDATE active_records SET content_sha256=? WHERE path=? AND native_id=? AND content_sha256=?",
            (envelope["content_sha256"], row["path"], row["native_id"], old_sha),
        )
        repaired = dict(row)
        repaired["content_sha256"] = envelope["content_sha256"]
        repaired["envelope_json"] = rendered
        return repaired

    def flush(self) -> dict:
        recovery = self.recover_dead_payloads() if self.shard_index == 0 else {"recovered": 0, "unrecoverable": 0}
        result = {"batches": 0, "acked": 0, "replayed_batches": 0, "errors": 0, **recovery}
        max_batch_bytes = self._max_batch_bytes()
        while True:
            raw_candidates = list(self.db.execute(
                "SELECT * FROM outbox WHERE state='pending' AND (shard_key % ?) = ? ORDER BY id LIMIT ?",
                (self.shard_count, self.shard_index, self.batch_size),
            ))
            if not raw_candidates:
                break
            candidates = []
            for row in raw_candidates:
                candidate = self._repair_pending_envelope(row)
                if candidate is not None:
                    candidates.append(candidate)
            self.db.commit()
            rows: list[dict] = []
            body_size = len(b'{"events":[]}')
            for candidate in candidates:
                event_size = len(candidate["envelope_json"].encode()) + 1
                if not rows and event_size > max_batch_bytes:
                    with self.db:
                        self.db.execute("UPDATE outbox SET state='dead',envelope_json='{}' WHERE id=?", (candidate["id"],))
                        self.db.execute(
                            "INSERT OR IGNORE INTO dead_letters(path,byte_offset,error_code,error_summary,created_at) VALUES (?,?,?,?,?)",
                            (candidate["path"], candidate["start_offset"], "PayloadTooLarge", f"sanitized envelope exceeds {max_batch_bytes} bytes", time.time()),
                        )
                    result["errors"] += 1
                    continue
                if rows and body_size + event_size > max_batch_bytes:
                    break
                rows.append(candidate)
                body_size += event_size
            if not rows:
                continue
            events = [json.loads(row["envelope_json"]) for row in rows]
            key_material = self.source_id + ":" + ",".join(f"{row['id']}:{row['content_sha256']}" for row in rows)
            key = "collector-v1-" + hashlib.sha256(key_material.encode()).hexdigest()
            body = canonical_json({"events": events})
            request = urllib.request.Request(
                self.endpoint + "/v1/ingest/batches", data=body, method="POST",
                headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json", "Idempotency-Key": key},
            )
            acknowledgement = None
            for attempt in range(5):
                try:
                    if self.brain_writer is not None:
                        acknowledgement = self.brain_writer.ingest(events)
                    else:
                        with open_no_redirect(request, timeout=60) as response:
                            acknowledgement = json.loads(response.read())
                            if response.status not in {200, 201}:
                                raise RuntimeError(
                                    "server did not return a commit acknowledgement"
                                )
                    if acknowledgement.get("status") != "committed":
                        raise RuntimeError(
                            "server did not return a commit acknowledgement"
                        )
                    break
                except urllib.error.HTTPError as exc:
                    result["errors"] += 1
                    if exc.code < 500:
                        self._record_error(
                            "brain_unauthorized"
                            if exc.code in {401, 403}
                            else "brain_rejected"
                        )
                        return result
                    self._record_error("brain_unavailable")
                except PermissionError:
                    result["errors"] += 1
                    self._record_error("brain_unauthorized")
                    return result
                except ValueError:
                    result["errors"] += 1
                    self._record_error("brain_rejected")
                    return result
                except (OSError, urllib.error.URLError):
                    result["errors"] += 1
                    self._record_error("brain_unavailable")
                except (json.JSONDecodeError, RuntimeError):
                    result["errors"] += 1
                    self._record_error("brain_invalid_acknowledgement")
                    return result
                except Exception:
                    result["errors"] += 1
                    self._record_error("brain_unavailable")
                if attempt < 4:
                    time.sleep(min(2 ** attempt, 10))
            if acknowledgement is None:
                return result
            receipts = acknowledgement.get("receipts", [])
            if len(receipts) != len(rows):
                result["errors"] += 1
                self._record_error("brain_invalid_acknowledgement")
                break
            self._after_remote_commit(acknowledgement)
            acked_at = time.time()
            with self.db:
                acknowledgements = list(zip(rows, receipts, strict=True))
                self.db.executemany(
                    "UPDATE outbox SET state='acked',acked_at=?,receipt=?,envelope_json='{}' WHERE id=?",
                    [(acked_at, receipt, row["id"]) for row, receipt in acknowledgements],
                )
                self.db.executemany(
                    "UPDATE active_records SET receipt=? WHERE path=? AND native_id=?",
                    [(receipt, row["path"], row["native_id"]) for row, receipt in acknowledgements],
                )
                self.db.executemany(
                    "DELETE FROM dead_letters "
                    "WHERE path=? AND byte_offset=? AND error_code='RecoveryError'",
                    [(row["path"], row["start_offset"]) for row in rows],
                )
                for path in {row["path"] for row in rows}:
                    pending = self.db.execute("SELECT 1 FROM outbox WHERE path=? AND state='pending' LIMIT 1", (path,)).fetchone()
                    if not pending:
                        self.db.execute("UPDATE files SET committed_offset=scanned_offset WHERE path=?", (path,))
                self._set_meta("last_success_epoch", str(int(acked_at)))
                self._clear_error()
            result["batches"] += 1
            result["acked"] += len(rows)
            result["replayed_batches"] += int(bool(acknowledgement.get("replay")))
        return result

    def _max_batch_bytes(self) -> int:
        maximum = MAX_BATCH_BYTES
        provider = getattr(self.brain_writer, "max_events_payload_bytes", None)
        if callable(provider):
            advertised = provider()
            if type(advertised) is not int or advertised < 1:
                raise ValueError("brain writer returned an invalid batch byte limit")
            maximum = min(maximum, advertised)
        return maximum

    def doctor(self, *, include_dead_letters: bool = True) -> dict:
        disk = {str(path) for path in self.discover()}
        ledger = {row["path"] for row in self.db.execute("SELECT path FROM files WHERE status != 'tombstone'")}
        total_lines = self.db.execute("SELECT count(*) AS n FROM active_records").fetchone()["n"]
        parse_errors = self.db.execute("SELECT count(*) AS n FROM dead_letters").fetchone()["n"]
        latencies = [row["acked_at"] - row["queued_at"] for row in self.db.execute("SELECT queued_at,acked_at FROM outbox WHERE acked_at IS NOT NULL ORDER BY acked_at-queued_at")]
        p95 = latencies[max(0, int(len(latencies) * 0.95 + 0.999999) - 1)] if latencies else None
        metadata = dict(self.db.execute(
            "SELECT key,value FROM meta WHERE key IN "
            "('last_success_epoch','last_error_code','running_started_epoch',"
            "'last_scan_complete')"
        ))
        result = {
            "harness": self.harness,
            "source_id": self.source_id,
            "disk_files": len(disk),
            "ledger_files": len(ledger),
            "coverage_percent": 100.0 if not disk else 100.0 * len(disk & ledger) / len(disk),
            "records": total_lines,
            "parse_errors": parse_errors,
            "parse_error_percent": 100.0 * parse_errors / max(1, total_lines + parse_errors),
            "pending": self.db.execute("SELECT count(*) AS n FROM outbox WHERE state='pending'").fetchone()["n"],
            "acked": self.db.execute("SELECT count(*) AS n FROM outbox WHERE state='acked'").fetchone()["n"],
            "dead": self.db.execute("SELECT count(*) AS n FROM outbox WHERE state='dead'").fetchone()["n"],
            "committed_files": self.db.execute("SELECT count(*) AS n FROM files WHERE status != 'tombstone' AND committed_offset=scanned_offset").fetchone()["n"],
            "ack_latency_p95_seconds": p95,
            "dead_letter_count": parse_errors,
            "privacy_mode": self.privacy.mode,
            "privacy_policy_version": self.privacy.apply({}).policy_version,
            "last_success_epoch": int(metadata.get("last_success_epoch", "0")),
            "last_error_code": metadata.get("last_error_code"),
            "running": "running_started_epoch" in metadata,
            "scan_complete": metadata.get("last_scan_complete") == "1",
        }
        if include_dead_letters:
            result["dead_letters"] = [dict(row) for row in self.db.execute("SELECT path,byte_offset,error_code,error_summary FROM dead_letters ORDER BY id")]
        return result

    def locate_receipt(self, receipt: str) -> dict | None:
        row = self.db.execute(
            "SELECT path,start_offset,end_offset,native_id FROM outbox WHERE receipt=? AND state='acked'", (receipt,)
        ).fetchone()
        return dict(row) if row else None
