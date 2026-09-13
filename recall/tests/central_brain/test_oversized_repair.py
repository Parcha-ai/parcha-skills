from __future__ import annotations

import contextlib
import gzip
import hashlib
import io
import json
import sys
import unittest
from unittest import mock
from datetime import datetime, timezone
from pathlib import Path

RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path.insert(0, str(RECALL))
sys.path.insert(0, str(SERVER))

from recall_server import cli  # noqa: E402
from recall_server.canonical import (  # noqa: E402
    CanonicalLifecycleError,
    _validate_oversized_pointer,
)
from recall_server.oversized_repair import (  # noqa: E402
    OversizedRepairError,
    classify,
    repair_oversized_records,
    repaired_content,
)

OVERSIZED = "application/vnd.recall.oversized-record+gzip"


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class FakeArchive:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.reads: list[str] = []

    def read_raw(self, reference: dict) -> bytes:
        self.reads.append(reference["artifact_id"])
        try:
            return self.objects[reference["artifact_id"]]
        except KeyError:
            raise RuntimeError("archive object not found") from None


class FakeResult:
    def __init__(self, rows=None, rowcount=0) -> None:
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeConnection:
    def __init__(self, store: "FakeStore") -> None:
        self.store = store

    @contextlib.contextmanager
    def transaction(self):
        self.store.transactions += 1
        yield

    def execute(self, sql: str, params=()):
        self.store.statements.append((" ".join(sql.split()), params))
        if "FROM canonical_events event" in sql and "JOIN raw_artifacts" in sql:
            return FakeResult(rows=self.store.rows)
        if sql.lstrip().startswith("UPDATE canonical_events"):
            self.store.updated.append(params)
            return FakeResult(rowcount=1)
        if "INSERT INTO canonical_evidence_document_queue" in sql:
            self.store.queued.append(params)
            return FakeResult(rowcount=1)
        if sql.lstrip().startswith("UPDATE canonical_evidence_document_queue"):
            self.store.reset.append(params)
            return FakeResult(rowcount=1)
        raise AssertionError("unexpected statement: " + sql[:60])


class FakeStore:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.statements: list = []
        self.updated: list = []
        self.queued: list = []
        self.reset: list = []
        self.transactions = 0

    @contextlib.contextmanager
    def connect(self):
        yield FakeConnection(self)


def make_row(
    *,
    native_id: str,
    artifact_id: str,
    occurred_at: str,
    content: dict,
    size_bytes: int = 10,
) -> dict:
    return {
        "tenant_id": "tenant:test",
        "source_id": "codex:test",
        "event_id": "evt_" + native_id,
        "native_id": native_id,
        "occurred_at": datetime.fromisoformat(
            occurred_at.replace("Z", "+00:00")
        ),
        "content": content,
        "raw_artifact_id": artifact_id,
        "raw_storage_backend": "filesystem",
        "raw_object_key": "objects/aa/" + "a" * 64,
        "raw_content_sha256": "b" * 64,
        "raw_size_bytes": size_bytes,
        "raw_media_type": OVERSIZED,
        "raw_encryption": "filesystem-owner-only",
        "raw_version_id": "fs-test",
        "raw_created_at": datetime(2026, 9, 11, 6, 0, tzinfo=timezone.utc),
    }


def pointer(payload: bytes, **overrides) -> dict:
    return {
        "contract": "recall.oversized-projection.v1",
        "schema_version": 1,
        "full_record_available": True,
        "archive_encoding": "gzip",
        "full_size_bytes": len(payload),
        "full_content_sha256": hashlib.sha256(payload).hexdigest(),
        **overrides,
    }


class OversizedRepairTest(unittest.TestCase):
    def setUp(self) -> None:
        self.record = {"type": "user", "timestamp": "2026-09-11T06:00:05.087Z", "message": {"content": "x" * 500}}
        self.payload = canonical(self.record)
        self.compressed = gzip.compress(self.payload, compresslevel=6, mtime=0)

    def test_classify_names_every_outcome(self) -> None:
        row = make_row(
            native_id="rec-1",
            artifact_id="art_1",
            occurred_at="2026-09-11T06:00:05.087Z",
            content=pointer(self.payload),
        )
        self.assertEqual(classify(row, self.payload), "consistent")
        stale = pointer(self.payload, full_size_bytes=len(self.payload) + 7)
        self.assertEqual(classify({**row, "content": stale}, self.payload), "repaired")
        wrong_digest = pointer(self.payload, full_content_sha256="0" * 64)
        self.assertEqual(classify({**row, "content": wrong_digest}, self.payload), "repaired")
        self.assertEqual(classify(row, None), "unreadable")
        other = make_row(
            native_id="rec-1",
            artifact_id="art_1",
            occurred_at="2026-09-11T07:00:00Z",
            content=pointer(self.payload),
        )
        self.assertEqual(classify(other, self.payload), "record_mismatch")
        untimed = canonical({"type": "user", "message": {"content": "no timestamp"}})
        self.assertEqual(
            classify({**other, "content": pointer(untimed, full_size_bytes=1)}, untimed),
            "repaired",
        )

    def test_repaired_content_derives_only_archive_facts(self) -> None:
        content = pointer(self.payload, full_size_bytes=1, full_content_sha256="0" * 64)
        content["_recall_collector_generation"] = 3
        repaired = repaired_content(content, self.payload)
        self.assertEqual(repaired["full_size_bytes"], len(self.payload))
        self.assertEqual(repaired["full_content_sha256"], hashlib.sha256(self.payload).hexdigest())
        self.assertEqual(repaired["_recall_collector_generation"], 3)
        self.assertEqual(repaired["schema_version"], 1)

    def test_dry_run_reads_but_never_writes(self) -> None:
        rows = [
            make_row(
                native_id="rec-1",
                artifact_id="art_1",
                occurred_at="2026-09-11T06:00:05.087Z",
                content=pointer(self.payload, full_size_bytes=1),
            ),
        ]
        store = FakeStore(rows)
        archive = FakeArchive({"art_1": self.compressed})
        result = repair_oversized_records(
            store,
            archive,
            tenant_id="tenant:test",
            source_id="codex:test",
            native_parent_id="codex-session-test",
            dry_run=True,
        )
        self.assertEqual(result["events"], 1)
        self.assertEqual(result["repaired"], 1)
        self.assertEqual(result["written"], 0)
        self.assertFalse(result["requeued"])
        self.assertEqual(archive.reads, ["art_1"])
        self.assertEqual(store.updated, [])
        self.assertEqual(store.queued, [])
        self.assertEqual(store.transactions, 0)

    def test_repair_rewrites_pointer_and_requeues_group(self) -> None:
        consistent_payload = canonical({"type": "assistant", "timestamp": "2026-09-11T06:57:14.980Z", "message": {"content": "y" * 400}})
        rows = [
            make_row(
                native_id="rec-1",
                artifact_id="art_1",
                occurred_at="2026-09-11T06:00:05.087Z",
                content=pointer(self.payload, full_size_bytes=1, full_content_sha256="0" * 64),
            ),
            make_row(
                native_id="rec-2",
                artifact_id="art_2",
                occurred_at="2026-09-11T06:57:14.980Z",
                content=pointer(consistent_payload),
            ),
        ]
        store = FakeStore(rows)
        archive = FakeArchive({
            "art_1": self.compressed,
            "art_2": gzip.compress(consistent_payload, compresslevel=6, mtime=0),
        })
        result = repair_oversized_records(
            store,
            archive,
            tenant_id="tenant:test",
            source_id="codex:test",
            native_parent_id="codex-session-test",
            dry_run=False,
        )
        self.assertEqual(
            {key: result[key] for key in ("events", "consistent", "repaired", "record_mismatch", "unreadable", "written")},
            {"events": 2, "consistent": 1, "repaired": 1, "record_mismatch": 0, "unreadable": 0, "written": 1},
        )
        self.assertTrue(result["requeued"])
        self.assertEqual(len(store.updated), 1)
        written = json.loads(store.updated[0][0])
        self.assertEqual(written["full_size_bytes"], len(self.payload))
        self.assertEqual(written["full_content_sha256"], hashlib.sha256(self.payload).hexdigest())
        self.assertEqual(store.updated[0][1:], ("tenant:test", "codex:test", "evt_rec-1"))
        self.assertEqual(store.queued[0][3], ["rec-1"])
        self.assertEqual(store.reset, [("tenant:test", "codex:test", "codex-session-test")])
        self.assertEqual(store.transactions, 1)
        # Counts only: no content, digests or identifiers leak through the result.
        self.assertTrue(all(isinstance(value, (int, bool)) for value in result.values()))

    def test_blocked_group_is_reported_and_left_alone(self) -> None:
        rows = [
            make_row(
                native_id="rec-1",
                artifact_id="art_1",
                occurred_at="2026-09-11T06:00:05.087Z",
                content=pointer(self.payload, full_size_bytes=1),
            ),
            make_row(
                native_id="rec-2",
                artifact_id="art_missing",
                occurred_at="2026-09-11T06:57:14.980Z",
                content=pointer(self.payload),
            ),
            make_row(
                native_id="rec-3",
                artifact_id="art_1",
                occurred_at="2026-09-11T09:00:00Z",
                content=pointer(self.payload),
            ),
        ]
        store = FakeStore(rows)
        result = repair_oversized_records(
            store,
            FakeArchive({"art_1": self.compressed}),
            tenant_id="tenant:test",
            source_id="codex:test",
            native_parent_id="codex-session-test",
            dry_run=False,
        )
        self.assertEqual(result["repaired"], 1)
        self.assertEqual(result["unreadable"], 1)
        self.assertEqual(result["record_mismatch"], 1)
        self.assertEqual(result["written"], 0)
        self.assertFalse(result["requeued"])
        self.assertEqual(store.updated, [])
        self.assertEqual(store.reset, [])

    def test_corrupt_gzip_counts_as_unreadable(self) -> None:
        rows = [
            make_row(
                native_id="rec-1",
                artifact_id="art_1",
                occurred_at="2026-09-11T06:00:05.087Z",
                content=pointer(self.payload),
            ),
        ]
        result = repair_oversized_records(
            FakeStore(rows),
            FakeArchive({"art_1": b"not a gzip member"}),
            tenant_id="tenant:test",
            source_id="codex:test",
            native_parent_id="codex-session-test",
            dry_run=False,
        )
        self.assertEqual(result["unreadable"], 1)
        self.assertFalse(result["requeued"])

    def test_invalid_scope_fails_closed(self) -> None:
        with self.assertRaises(OversizedRepairError):
            repair_oversized_records(
                FakeStore([]),
                FakeArchive({}),
                tenant_id="",
                source_id="codex:test",
                native_parent_id="parent",
            )
        with self.assertRaises(OversizedRepairError):
            repair_oversized_records(
                FakeStore([]),
                None,
                tenant_id="tenant:test",
                source_id="codex:test",
                native_parent_id="parent",
            )


class OversizedPointerIngestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.payload = canonical({"type": "user", "message": {"content": "z" * 100}})
        self.compressed = gzip.compress(self.payload, compresslevel=6, mtime=0)
        self.artifact = {
            "media_type": OVERSIZED,
            "size_bytes": len(self.compressed),
            "content_sha256": hashlib.sha256(self.compressed).hexdigest(),
        }

    def event(self, **overrides) -> dict:
        content = {
            **pointer(self.payload),
            "archive_size_bytes": len(self.compressed),
            "head": "{",
            "tail": "}",
        }
        content.update(overrides)
        return {"kind": "transcript_record", "content": content}

    def test_consistent_pointer_is_accepted(self) -> None:
        _validate_oversized_pointer(self.event(), self.artifact)
        _validate_oversized_pointer({"kind": "transcript_record", "content": {"type": "user"}}, {"media_type": "application/json"})
        _validate_oversized_pointer({"kind": "tombstone", "content": {}}, self.artifact)

    def test_pointer_disagreeing_with_artifact_is_rejected(self) -> None:
        cases = [
            self.event(archive_size_bytes=len(self.compressed) + 1),
            self.event(archive_size_bytes=True),
            self.event(full_size_bytes=0),
            self.event(full_size_bytes="10"),
            self.event(full_content_sha256="xyz"),
            self.event(full_content_sha256="A" * 64),
            self.event(archive_encoding="zstd"),
            self.event(full_record_available=False),
        ]
        for event in cases:
            with self.assertRaises(CanonicalLifecycleError) as caught:
                _validate_oversized_pointer(event, self.artifact)
            self.assertEqual(caught.exception.error_code, "canonical_oversized_pointer_invalid")
        with self.assertRaises(CanonicalLifecycleError):
            _validate_oversized_pointer(self.event(), {**self.artifact, "media_type": "application/x-ndjson"})


class RepairCliTest(unittest.TestCase):
    def test_command_prints_counts_only(self) -> None:
        calls: list[dict] = []

        def fake_repair(store, archive, **kwargs):
            calls.append(kwargs)
            return {"events": 14, "repaired": 14, "dry_run": kwargs["dry_run"]}

        output = io.StringIO()
        with (
            mock.patch.object(cli, "BrainStore", return_value=object()),
            mock.patch.object(cli, "build_archive_store", return_value=object()),
            mock.patch.object(cli, "repair_oversized_records", fake_repair),
            mock.patch.object(cli.SemanticRuntime, "from_env", return_value=None),
            mock.patch.object(
                sys,
                "argv",
                [
                    "recall-server",
                    "--dsn",
                    "postgresql://synthetic",
                    "repair-oversized-records",
                    "--tenant",
                    "tenant:company:parcha",
                    "--source",
                    "codex:linux:greppy3",
                    "--native-parent-id",
                    "codex-session-3638830eb64b85fba0740c4b",
                    "--dry-run",
                ],
            ),
            contextlib.redirect_stdout(output),
        ):
            cli.main()
        self.assertEqual(
            calls,
            [
                {
                    "tenant_id": "tenant:company:parcha",
                    "source_id": "codex:linux:greppy3",
                    "native_parent_id": "codex-session-3638830eb64b85fba0740c4b",
                    "dry_run": True,
                }
            ],
        )
        self.assertEqual(
            json.loads(output.getvalue()),
            {"dry_run": True, "events": 14, "repaired": 14},
        )


if __name__ == "__main__":
    unittest.main()
