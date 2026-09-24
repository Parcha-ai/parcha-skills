"""Paginated physical histories retain separate stable parents and lineage."""

import json
import shutil
import unittest
from unittest.mock import patch

from tests import test_codex_archive_collector as fixture

from skills.recall.scripts.codex_identity import resolve_codex_session_identity
from skills.recall.scripts.recall import session_file_key
from scripts.audit_codex_roots import audit_roots

_rollout, _line = fixture._rollout, fixture._line

SESSION = "019f1111-2222-7333-8444-555555555555"
SEGMENT = "019f6666-7777-7888-8999-aaaaaaaaaaaa"
NEXT = "019fbbbb-cccc-7ddd-8eee-ffffffffffff"


class CodexPaginationTest(unittest.TestCase):
    setUp = fixture.CodexArchiveCollectorTest.setUp
    tearDown = fixture.CodexArchiveCollectorTest.tearDown
    collector = fixture.CodexArchiveCollectorTest.collector

    def root_file(self, session=SESSION):
        path = self.active / f"rollout-2026-08-10T00-00-00-{session}.jsonl"
        _rollout(path, session)
        return path

    def segment(
        self, base, *, session=SESSION, segment=SEGMENT, base_id=None, offset=None
    ):
        path = self.active / f"rollout-2026-08-11T00-00-00-{session}_{segment}.jsonl"
        path.write_text(
            _line(
                {
                    "type": "session_meta",
                    "payload": {
                        "id": session,
                        "session_id": session,
                        "history_mode": "paginated",
                        "history_base": {
                            "thread_id": base_id or SESSION,
                            "end_ordinal_exclusive": 2,
                            "end_byte_offset": base.stat().st_size
                            if offset is None
                            else offset,
                        },
                    },
                }
            )
            + _line(
                {
                    "type": "response_item",
                    "payload": {"type": "message", "marker": segment},
                }
            )
        )
        return path

    def test_native_identity_and_legacy_records_survive_separate_continuations(self):
        root = self.root_file()
        collector = self.collector()
        collector.scan()
        collector.flush()
        parent = (
            "codex-session-"
            + collector.db.execute("SELECT record_key FROM codex_sessions").fetchone()[
                0
            ]
        )
        before = list(
            collector.db.execute(
                "SELECT native_id,receipt,content_sha256 FROM outbox ORDER BY native_id"
            )
        )
        first = self.segment(root)
        second = self.segment(first, segment=NEXT, base_id=SEGMENT)
        self.assertEqual(
            resolve_codex_session_identity(second).native_session_id, SESSION
        )
        scanned = collector.scan()
        self.assertEqual(scanned["records_queued"], 4)
        self.assertEqual(scanned["identity_conflicts"], 0)
        self.assertEqual(scanned["tombstones_queued"], 0)
        rows = [
            json.loads(row[0])
            for row in collector.db.execute(
                "SELECT envelope_json FROM outbox WHERE state='pending'"
            )
        ]
        self.assertEqual(
            collector.db.execute(
                "SELECT count(DISTINCT native_id) FROM outbox"
            ).fetchone()[0],
            6,
        )
        self.assertEqual(len({row["native_parent_id"] for row in rows}), 2)
        self.assertNotIn(parent, {row["native_parent_id"] for row in rows})
        self.assertEqual(
            {row["provenance"]["codex_session_id"] for row in rows}, {SESSION}
        )
        self.assertEqual(
            {row["provenance"]["codex_history_base"]["thread_id"] for row in rows},
            {SESSION, SEGMENT},
        )
        self.assertEqual(
            collector.db.execute("SELECT count(*) FROM codex_sessions").fetchone()[0], 1
        )
        for old in before:
            current = collector.db.execute(
                "SELECT native_id,receipt,content_sha256 FROM outbox WHERE native_id=?",
                (old[0],),
            ).fetchone()
            self.assertEqual(tuple(current), tuple(old))
        collector.flush()
        self.assertEqual(collector.scan()["records_queued"], 0)
        self.assertEqual(collector.doctor()["duplicate_sessions"], 0)
        collector.close()

    def test_segment_archive_move_restart_and_duplicate_copy_preserve_ids(self):
        root = self.root_file()
        segment = self.segment(root)
        collector = self.collector()
        collector.scan()
        collector.flush()
        before = [
            tuple(row)
            for row in collector.db.execute(
                "SELECT native_id,receipt FROM outbox ORDER BY native_id"
            )
        ]
        self.assertEqual(len(before), 4)
        collector.close()
        archived = self.archived / segment.name
        segment.replace(archived)
        resumed = self.collector()
        moved = resumed.scan()
        self.assertEqual((moved["records_queued"], moved["tombstones_queued"]), (0, 0))
        self.assertEqual(
            [
                tuple(row)
                for row in resumed.db.execute(
                    "SELECT native_id,receipt FROM outbox ORDER BY native_id"
                )
            ],
            before,
        )
        shutil.copyfile(archived, segment)
        duplicate = resumed.scan()
        self.assertEqual(
            (duplicate["records_queued"], duplicate["identity_conflicts"]), (0, 0)
        )
        self.assertEqual(
            resumed.db.execute(
                "SELECT count(*) FROM codex_session_locations WHERE status='duplicate'"
            ).fetchone()[0],
            1,
        )
        resumed.close()

    def test_actual_fork_retains_separate_parent(self):
        root = self.root_file()
        self.segment(root)
        fork = self.root_file(NEXT)
        self.segment(
            fork,
            session=NEXT,
            segment="019f0000-1111-7222-8333-444444444444",
            base_id=NEXT,
        )
        collector = self.collector()
        scanned = collector.scan()
        self.assertEqual(scanned["records_queued"], 8)
        rows = [
            json.loads(row[0])
            for row in collector.db.execute("SELECT envelope_json FROM outbox")
        ]
        self.assertEqual(len({row["native_parent_id"] for row in rows}), 4)
        self.assertEqual(len({row["native_id"] for row in rows}), 8)
        collector.close()

    def test_bad_base_never_ingests_or_tombstones_existing_root(self):
        for problem in ("missing", "foreign", "offset", "boundary", "cycle"):
            with self.subTest(problem=problem):
                root = self.root_file()
                collector = self.collector()
                collector.scan()
                collector.flush()
                before = collector.db.execute("SELECT count(*) FROM outbox").fetchone()[
                    0
                ]
                base_id = {"missing": NEXT, "foreign": NEXT, "cycle": SEGMENT}.get(
                    problem, SESSION
                )
                if problem == "foreign":
                    self.root_file(NEXT)
                offset = (
                    root.stat().st_size + 1
                    if problem == "offset"
                    else 1
                    if problem == "boundary"
                    else None
                )
                part = self.segment(root, base_id=base_id, offset=offset)
                scanned = collector.scan()
                self.assertGreater(scanned["identity_conflicts"], 0)
                self.assertEqual(scanned["tombstones_queued"], 0)
                self.assertEqual(
                    collector.db.execute(
                        "SELECT count(*) FROM outbox WHERE path=?", (str(part),)
                    ).fetchone()[0],
                    0,
                )
                self.assertGreaterEqual(
                    collector.db.execute("SELECT count(*) FROM outbox").fetchone()[0],
                    before,
                )
                collector.close()
                part.unlink()
                if problem == "foreign":
                    (self.active / f"rollout-2026-08-10T00-00-00-{NEXT}.jsonl").unlink()

    def test_bounded_incremental_scans_resume_each_physical_history(self):
        root = self.root_file()
        first = self.segment(root)
        self.segment(first, segment=NEXT, base_id=SEGMENT)
        for _ in range(12):
            collector = self.collector(max_scan_records=1)
            result = collector.scan()
            self.assertLessEqual(result["records_queued"], 1)
            self.assertEqual(result["tombstones_queued"], 0)
            collector.flush()
            complete = result["scan_complete"]
            collector.close()
            if complete:
                break
        self.assertTrue(complete)
        collector = self.collector()
        self.assertEqual(
            collector.db.execute("SELECT count(*) FROM outbox").fetchone()[0], 6
        )
        self.assertEqual(
            collector.db.execute(
                "SELECT count(DISTINCT native_id) FROM outbox"
            ).fetchone()[0],
            6,
        )
        self.assertEqual(collector.scan()["records_queued"], 0)
        collector.close()

    def test_deleted_segment_after_restart_queues_tombstones_without_reading_file(self):
        root = self.root_file()
        segment = self.segment(root)
        collector = self.collector()
        collector.scan()
        collector.flush()
        before = {
            row[0]
            for row in collector.db.execute(
                "SELECT native_id FROM outbox WHERE path=?", (str(segment),)
            )
        }
        collector.close()
        segment.unlink()
        resumed = self.collector()
        scanned = resumed.scan()
        self.assertEqual(scanned["tombstones_queued"], 2)
        pending = [
            json.loads(row[0])
            for row in resumed.db.execute(
                "SELECT envelope_json FROM outbox WHERE state='pending'"
            )
        ]
        self.assertEqual({row["native_id"] for row in pending}, before)
        self.assertEqual({row["kind"] for row in pending}, {"tombstone"})
        resumed.close()

    def test_root_audit_does_not_call_distinct_segments_conflicting_copies(self):
        root = self.root_file()
        segment = self.segment(root)
        result = audit_roots(
            source_id="synthetic", active_root=self.active, archive_root=self.archived
        )
        self.assertEqual(result["duplicates"]["session_ids"], 0)
        self.assertEqual(result["duplicates"]["divergent"], 0)
        shutil.copyfile(segment, self.archived / segment.name)
        result = audit_roots(
            source_id="synthetic", active_root=self.active, archive_root=self.archived
        )
        self.assertEqual(result["duplicates"]["session_ids"], 1)
        self.assertEqual(result["duplicates"]["byte_identical"], 1)
        self.assertEqual(result["duplicates"]["divergent"], 0)

    def test_local_export_and_collector_segment_keys_agree_without_collisions(self):
        root = self.root_file()
        first = self.segment(root)
        second = self.segment(first, segment=NEXT, base_id=SEGMENT)
        collector = self.collector()
        collector.scan()
        keys = [
            session_file_key(path, self.active, "codex")
            for path in (root, first, second)
        ]
        self.assertEqual(len(set(keys)), 3)
        for path, key in zip((root, first, second), keys):
            rows = collector.db.execute(
                "SELECT native_id,envelope_json FROM outbox WHERE path=?", (str(path),)
            )
            for row in rows:
                self.assertTrue(row["native_id"].startswith(key + "-"))
                self.assertEqual(
                    json.loads(row["envelope_json"])["native_parent_id"],
                    "codex-session-" + key,
                )
        collector.close()

    def test_adopted_legacy_root_key_is_not_changed_by_segment(self):
        root = self.root_file()
        collector = self.collector()
        collector.scan()
        legacy_key = collector._legacy_file_key(root)
        for row in list(
            collector.db.execute(
                "SELECT id,native_id,start_offset,envelope_json FROM outbox"
            )
        ):
            native_id = f"{legacy_key}-{row['start_offset']:016x}"
            envelope = json.loads(row["envelope_json"])
            envelope.update(
                native_id=native_id, native_parent_id="codex-session-" + legacy_key
            )
            collector.db.execute(
                "UPDATE outbox SET native_id=?,envelope_json=? WHERE id=?",
                (native_id, json.dumps(envelope), row["id"]),
            )
            collector.db.execute(
                "UPDATE active_records SET native_id=? WHERE native_id=?",
                (native_id, row["native_id"]),
            )
            collector.db.execute(
                "UPDATE record_generations SET native_id=? WHERE native_id=?",
                (native_id, row["native_id"]),
            )
        collector.db.execute("UPDATE codex_sessions SET record_key=?", (legacy_key,))
        collector.db.commit()
        collector.flush()
        collector.close()
        self.segment(root)
        resumed = self.collector()
        scanned = resumed.scan()
        self.assertEqual(scanned["records_queued"], 2)
        self.assertEqual(scanned["tombstones_queued"], 0)
        self.assertEqual(
            resumed.db.execute("SELECT record_key FROM codex_sessions").fetchone()[0],
            legacy_key,
        )
        self.assertEqual(
            resumed.db.execute(
                "SELECT count(*) FROM outbox WHERE state='acked' AND native_id LIKE ?",
                (legacy_key + "-%",),
            ).fetchone()[0],
            2,
        )
        resumed.close()

    def test_divergent_segment_copy_does_not_tombstone_existing_history(self):
        root = self.root_file()
        segment = self.segment(root)
        collector = self.collector()
        collector.scan()
        collector.flush()
        copied = self.archived / segment.name
        shutil.copyfile(segment, copied)
        with copied.open("a") as target:
            target.write(
                _line({"type": "response_item", "payload": {"marker": "divergence"}})
            )
        scanned = collector.scan()
        self.assertEqual(scanned["identity_conflicts"], 1)
        self.assertEqual(scanned["records_queued"], 0)
        self.assertEqual(scanned["tombstones_queued"], 0)
        self.assertEqual(
            collector.db.execute("SELECT count(*) FROM active_records").fetchone()[0], 4
        )
        collector.close()

    def test_base_tail_and_continuation_stay_in_separate_documents(self):
        root = self.root_file()
        segment = self.segment(root)
        with root.open("a") as target:
            target.write(
                _line(
                    {
                        "type": "response_item",
                        "payload": {"marker": "retained-base-tail"},
                    }
                )
            )
        collector = self.collector()
        scanned = collector.scan()
        self.assertEqual(scanned["records_queued"], 5)
        records = [
            json.loads(row[0])
            for row in collector.db.execute("SELECT envelope_json FROM outbox")
        ]
        root_records = [
            row for row in records if row["provenance"]["original_path"] == str(root)
        ]
        segment_records = [
            row for row in records if row["provenance"]["original_path"] == str(segment)
        ]
        self.assertEqual(len(root_records), 3)
        self.assertEqual(len(segment_records), 2)
        self.assertNotEqual(
            root_records[0]["native_parent_id"], segment_records[0]["native_parent_id"]
        )
        for rows in (root_records, segment_records):
            self.assertEqual(rows[0]["provenance"]["byte_start"], 0)
            self.assertEqual(
                [row["provenance"]["byte_start"] for row in rows],
                sorted(row["provenance"]["byte_start"] for row in rows),
            )
        collector.close()

    def test_divergent_root_preserves_previously_ingested_segment(self):
        root = self.root_file()
        self.segment(root)
        collector = self.collector()
        collector.scan()
        collector.flush()
        copied = self.archived / root.name
        shutil.copyfile(root, copied)
        with copied.open("a") as target:
            target.write(
                _line({"type": "response_item", "payload": {"marker": "divergence"}})
            )
        scanned = collector.scan()
        self.assertEqual(scanned["identity_conflicts"], 1)
        self.assertEqual(scanned["tombstones_queued"], 0)
        self.assertEqual(
            collector.db.execute("SELECT count(*) FROM active_records").fetchone()[0], 4
        )
        collector.close()

    def test_root_and_segment_relocations_rollback_together(self):
        root = self.root_file()
        segment = self.segment(root)
        collector = self.collector()
        collector.scan()
        collector.flush()
        previous = [
            tuple(row)
            for row in collector.db.execute(
                "SELECT path,scanned_offset FROM files ORDER BY path"
            )
        ]
        root.replace(self.archived / root.name)
        segment.replace(self.archived / segment.name)
        with patch.object(collector, "_rebind_codex_segments", return_value=False):
            scanned = collector.scan()
        self.assertEqual(scanned["identity_conflicts"], 1)
        self.assertEqual(scanned["tombstones_queued"], 0)
        self.assertEqual(
            [
                tuple(row)
                for row in collector.db.execute(
                    "SELECT path,scanned_offset FROM files ORDER BY path"
                )
            ],
            previous,
        )
        collector.close()

    def test_unproven_two_uuid_filename_stays_quarantined_and_reported(self):
        path = self.active / f"rollout-2026-08-11T00-00-00-{SESSION}_{SEGMENT}.jsonl"
        _rollout(path, SESSION)
        self.assertEqual(
            resolve_codex_session_identity(path).status, "identity_conflict"
        )
        collector = self.collector()
        scanned = collector.scan()
        self.assertEqual(scanned["records_queued"], 0)
        self.assertEqual(collector.doctor()["identity_conflicts"], 1)
        self.assertEqual(collector.doctor()["quarantined_files"], 1)
        collector.close()


if __name__ == "__main__":
    unittest.main()
