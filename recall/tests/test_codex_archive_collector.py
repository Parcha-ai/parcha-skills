from __future__ import annotations

import hashlib
import json
import os
import random
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from collector.collector import Collector, CollectorRuntimeError


def _line(value: dict) -> str:
    return json.dumps(value, sort_keys=True) + "\n"


def _rollout(path: Path, session_id: str, marker: str = "work") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        _line({
            "timestamp": "2026-08-10T00:00:00Z",
            "type": "session_meta",
            "payload": {"id": session_id},
        })
        + _line({
            "timestamp": "2026-08-10T00:00:01Z",
            "type": "response_item",
            "payload": {"type": "message", "marker": marker},
        })
    )


class _AckServer(BaseHTTPRequestHandler):
    batches: dict[str, dict] = {}
    requests = 0

    def log_message(self, *_args) -> None:
        pass

    def do_POST(self) -> None:
        length = int(self.headers["Content-Length"])
        body = json.loads(self.rfile.read(length))
        key = self.headers["Idempotency-Key"]
        type(self).requests += 1
        acknowledgement = type(self).batches.get(key)
        replay = acknowledgement is not None
        if acknowledgement is None:
            acknowledgement = {
                "batch_id": "synthetic-" + str(len(type(self).batches) + 1),
                "status": "committed",
                "inserted": len(body["events"]),
                "duplicate_events": 0,
                "receipts": [
                    f"recall://{item['source_id']}/{item['native_id']}?rev=1"
                    for item in body["events"]
                ],
            }
            type(self).batches[key] = acknowledgement
        rendered = json.dumps({**acknowledgement, "replay": replay}).encode()
        self.send_response(200 if replay else 201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(rendered)))
        self.end_headers()
        self.wfile.write(rendered)


class CodexArchiveCollectorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.active = root / "sessions"
        self.archived = root / "archived_sessions"
        self.active.mkdir()
        self.archived.mkdir()
        self.spool = root / "spool.db"
        _AckServer.batches = {}
        _AckServer.requests = 0
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _AckServer)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.temporary.cleanup()

    def collector(self, **kwargs) -> Collector:
        return Collector(
            root=self.active,
            archive_root=self.archived,
            harness="codex",
            source_id="codex:linux:synthetic",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="synthetic-test-token",
            max_scan_records=kwargs.pop("max_scan_records", 100_000),
            max_scan_seconds=kwargs.pop("max_scan_seconds", 60),
            **kwargs,
        )

    @staticmethod
    def _rows(collector: Collector) -> list[tuple[str, str | None]]:
        return [
            (row["native_id"], row["receipt"])
            for row in collector.db.execute(
                "SELECT native_id,receipt FROM outbox ORDER BY native_id"
            )
        ]

    def test_one_hundred_random_moves_preserve_ids_receipts_and_content(self) -> None:
        randomizer = random.Random(20260811)
        paths: list[Path] = []
        for index in range(100):
            session_id = f"019f{index:04x}-1111-7222-8333-{index:012x}"
            name = f"rollout-2026-08-10T00-00-00-{session_id}.jsonl"
            path = self.active / str(index % 7) / name
            _rollout(path, session_id, marker=f"work-{index}")
            paths.append(path)

        collector = self.collector()
        self.assertEqual(collector.scan()["records_queued"], 200)
        parents_before = {
            json.loads(row[0])["native_parent_id"]
            for row in collector.db.execute("SELECT envelope_json FROM outbox")
        }
        self.assertEqual(len(parents_before), 100)
        self.assertEqual(collector.flush()["acked"], 200)
        before = self._rows(collector)
        content_before = sorted(
            row[0] for row in collector.db.execute(
                "SELECT content_sha256 FROM active_records"
            )
        )

        for index, path in enumerate(paths):
            target = (
                self.archived
                / str(randomizer.randrange(12))
                / str(randomizer.randrange(31))
                / path.name
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            path.replace(target)

        moved = collector.scan()
        self.assertEqual(moved["records_queued"], 0)
        self.assertEqual(moved["tombstones_queued"], 0)
        self.assertEqual(moved["identity_conflicts"], 0)
        self.assertEqual(self._rows(collector), before)
        self.assertEqual(
            {
                "codex-session-" + row[0]
                for row in collector.db.execute(
                    "SELECT record_key FROM codex_sessions"
                )
            },
            parents_before,
        )
        self.assertEqual(
            sorted(
                row[0] for row in collector.db.execute(
                    "SELECT content_sha256 FROM active_records"
                )
            ),
            content_before,
        )
        self.assertEqual(
            collector.db.execute(
                "SELECT count(*) FROM codex_sessions"
            ).fetchone()[0],
            100,
        )
        self.assertEqual(collector.doctor()["archive_coverage_percent"], 100.0)
        collector.close()

    def test_restart_after_remote_commit_replays_once_after_move(self) -> None:
        session_id = "019f1111-2222-7333-8444-555555555555"
        source = self.active / f"rollout-{session_id}.jsonl"
        _rollout(source, session_id)
        collector = self.collector()
        collector.scan()
        before = self._rows(collector)

        def stop_after_commit(_acknowledgement) -> None:
            raise RuntimeError("synthetic process death")

        collector._after_remote_commit = stop_after_commit
        with self.assertRaisesRegex(RuntimeError, "process death"):
            collector.flush()
        collector.close()

        target = self.archived / source.name
        source.replace(target)
        resumed = self.collector()
        moved = resumed.scan()
        self.assertEqual(moved["records_queued"], 0)
        self.assertEqual(moved["tombstones_queued"], 0)
        self.assertEqual(
            [item[0] for item in self._rows(resumed)],
            [item[0] for item in before],
        )
        flushed = resumed.flush()
        self.assertEqual(flushed["acked"], 2)
        self.assertEqual(flushed["replayed_batches"], 1)
        self.assertEqual(_AckServer.requests, 2)
        resumed.close()

    def test_acknowledged_legacy_path_ids_are_adopted_after_archival(self) -> None:
        session_id = "019f1111-2222-7333-8444-555555555555"
        source = self.active / "2026" / "08" / f"rollout-{session_id}.jsonl"
        _rollout(source, session_id)
        legacy = Collector(
            root=self.active,
            harness="codex",
            source_id="codex:linux:synthetic",
            spool_path=self.spool,
            endpoint=self.endpoint,
            token="synthetic-test-token",
            max_scan_records=100_000,
            max_scan_seconds=60,
        )
        legacy.scan()
        stable_key = legacy.db.execute(
            "SELECT record_key FROM codex_sessions"
        ).fetchone()[0]
        legacy_key = hashlib.sha256(
            (
                "codex\x1f"
                + str(source.relative_to(self.active))
            ).encode()
        ).hexdigest()[:24]
        self.assertNotEqual(stable_key, legacy_key)
        rows = list(legacy.db.execute(
            "SELECT id,native_id,envelope_json,start_offset FROM outbox"
        ))
        for row in rows:
            replacement = f"{legacy_key}-{int(row['start_offset']):016x}"
            envelope = json.loads(row["envelope_json"])
            envelope["native_id"] = replacement
            envelope["native_parent_id"] = "codex-session-" + legacy_key
            legacy.db.execute(
                "UPDATE outbox SET native_id=?,envelope_json=? WHERE id=?",
                (replacement, json.dumps(envelope), row["id"]),
            )
            legacy.db.execute(
                "UPDATE active_records SET native_id=? WHERE native_id=?",
                (replacement, row["native_id"]),
            )
            legacy.db.execute(
                "UPDATE record_generations SET native_id=? WHERE native_id=?",
                (replacement, row["native_id"]),
            )
        legacy.db.execute(
            "UPDATE codex_sessions SET record_key=?",
            (legacy_key,),
        )
        legacy._codex_path_keys[str(source)] = legacy_key
        legacy.db.commit()
        self.assertEqual(legacy.flush()["acked"], 2)

        target = self.archived / source.name
        target.parent.mkdir(parents=True, exist_ok=True)
        source.replace(target)
        removed = legacy.scan()
        self.assertEqual(removed["tombstones_queued"], 2)
        self.assertEqual(legacy.flush()["acked"], 2)
        receipts = self._rows(legacy)
        legacy.db.execute("DROP TABLE codex_session_locations")
        legacy.db.execute("DROP TABLE codex_sessions")
        legacy.db.commit()
        legacy.close()

        migrated = self.collector()
        result = migrated.scan()
        self.assertEqual(result["records_queued"], 0)
        self.assertEqual(result["tombstones_queued"], 0)
        self.assertEqual(self._rows(migrated), receipts)
        self.assertEqual(
            migrated.db.execute(
                "SELECT record_key FROM codex_sessions"
            ).fetchone()[0],
            legacy_key,
        )
        self.assertEqual(migrated.doctor()["archive_backlog"], 1)
        migrated.close()

    def test_identical_duplicate_prefers_active_and_divergence_fails_closed(self) -> None:
        session_id = "019f1111-2222-7333-8444-555555555555"
        name = f"rollout-{session_id}.jsonl"
        active = self.active / name
        archived = self.archived / name
        _rollout(active, session_id, "same")
        archived.write_bytes(active.read_bytes())

        collector = self.collector()
        first = collector.scan()
        self.assertEqual(first["records_queued"], 2)
        self.assertEqual(first["duplicate_sessions"], 1)
        self.assertEqual(first["identity_conflicts"], 0)
        canonical = collector.db.execute(
            "SELECT canonical_path FROM codex_sessions"
        ).fetchone()[0]
        self.assertEqual(canonical, str(active))
        collector.flush()

        _rollout(archived, session_id, "different")
        conflict = collector.scan()
        self.assertEqual(conflict["identity_conflicts"], 1)
        self.assertEqual(conflict["records_queued"], 0)
        self.assertEqual(conflict["tombstones_queued"], 0)
        self.assertEqual(collector.doctor()["identity_conflicts"], 1)
        self.assertEqual(
            collector.db.execute(
                "SELECT count(*) FROM active_records"
            ).fetchone()[0],
            2,
        )
        collector.close()

    def test_active_record_is_serviced_before_archive_backfill(self) -> None:
        active_id = "019f1111-2222-7333-8444-555555555555"
        _rollout(self.active / f"rollout-{active_id}.jsonl", active_id)
        for index in range(12):
            session_id = f"019f{index + 1:04x}-aaaa-7bbb-8ccc-{index:012x}"
            path = self.archived / f"rollout-{session_id}.jsonl"
            _rollout(path, session_id, marker=f"archive-{index}")
            os.utime(path, ns=(10_000_000_000, 10_000_000_000))

        collector = self.collector(max_scan_records=1)
        result = collector.scan()
        self.assertFalse(result["scan_complete"])
        pending = collector.pending_envelopes()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["content"]["payload"]["id"], active_id)
        self.assertEqual(result["active_files_seen"], 1)
        self.assertEqual(result["archive_files_seen"], 12)
        collector.close()

    def test_new_append_after_move_keeps_record_prefix(self) -> None:
        session_id = "019f1111-2222-7333-8444-555555555555"
        source = self.active / f"rollout-{session_id}.jsonl"
        _rollout(source, session_id)
        collector = self.collector()
        collector.scan()
        prefix = collector.pending_envelopes()[0]["native_id"].split("-", 1)[0]
        collector.flush()

        target = self.archived / source.name
        source.replace(target)
        with target.open("a") as output:
            output.write(_line({
                "timestamp": "2026-08-10T00:00:02Z",
                "type": "response_item",
                "payload": {"type": "message", "marker": "after-move"},
            }))
        result = collector.scan()
        self.assertEqual(result["records_queued"], 1)
        self.assertEqual(result["tombstones_queued"], 0)
        self.assertTrue(
            collector.pending_envelopes()[0]["native_id"].startswith(prefix + "-")
        )
        collector.close()

    def test_unavailable_archive_root_fails_closed_without_tombstones(self) -> None:
        session_id = "019f1111-2222-7333-8444-555555555555"
        source = self.active / f"rollout-{session_id}.jsonl"
        _rollout(source, session_id)
        collector = self.collector()
        collector.scan()
        collector.flush()
        source.unlink()
        self.archived.rmdir()

        with self.assertRaisesRegex(CollectorRuntimeError, "archive_unavailable"):
            collector.scan()
        self.assertEqual(
            collector.db.execute(
                "SELECT count(*) FROM outbox WHERE state='pending'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            collector.doctor(include_dead_letters=False)["last_error_code"],
            "archive_unavailable",
        )
        collector.close()

    def test_claude_schema_and_identity_remain_unchanged(self) -> None:
        root = Path(self.temporary.name) / "claude"
        root.mkdir()
        source = root / "session.jsonl"
        source.write_text(_line({
            "type": "user",
            "timestamp": "2026-08-10T00:00:00Z",
            "message": {"content": "synthetic"},
        }))
        spool = Path(self.temporary.name) / "claude.db"
        collector = Collector(
            root=root,
            harness="claude",
            source_id="claude:linux:synthetic",
            spool_path=spool,
            endpoint=self.endpoint,
            token="synthetic-test-token",
        )
        collector.scan()
        expected_key = hashlib.sha256(
            "claude\x1fsession.jsonl".encode()
        ).hexdigest()[:24]
        envelope = collector.pending_envelopes()[0]
        self.assertEqual(
            envelope["native_parent_id"],
            "claude-session-" + expected_key,
        )
        tables = {
            row[0] for row in collector.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertFalse(any(name.startswith("codex_") for name in tables))
        collector.close()
        with self.assertRaisesRegex(ValueError, "only for codex"):
            Collector(
                root=root,
                archive_root=self.archived,
                harness="claude",
                source_id="claude:linux:synthetic",
                spool_path=Path(self.temporary.name) / "bad.db",
                endpoint=self.endpoint,
                token="synthetic-test-token",
            )


if __name__ == "__main__":
    unittest.main()
