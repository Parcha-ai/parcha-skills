"""Compressed physical copies preserve native identities and decoded cursors."""

import json
import os
import time
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from compression import zstd
except ImportError:
    from backports import zstd

from tests import test_codex_archive_collector as fixture
from tests import test_collector as collector_fixture
from skills.recall.scripts.codex_identity import resolve_codex_session_identity
from skills.recall.scripts.codex_rollout import IdentityReadView
from skills.recall.scripts.codex_rollout import DecodedSourceDeadline
from collector.collector import CollectorRuntimeError

SESSION = "019f1111-2222-7333-8444-555555555555"
FIXTURE = Path(__file__).parent / "fixtures" / "codex-rollout.jsonl.zst"


class CompressedCollectorTest(unittest.TestCase):
    setUp = fixture.CodexArchiveCollectorTest.setUp
    tearDown = fixture.CodexArchiveCollectorTest.tearDown
    collector = fixture.CodexArchiveCollectorTest.collector
    _rows = staticmethod(fixture.CodexArchiveCollectorTest._rows)

    def test_active_root_and_native_identity_read_the_same_decoded_bytes(self):
        path = self.active / f"rollout-{SESSION}.jsonl.zst"
        path.write_bytes(FIXTURE.read_bytes())
        identity = resolve_codex_session_identity(
            IdentityReadView(path, time.monotonic() + 5),
        )
        self.assertEqual(identity.native_session_id, SESSION)
        self.assertEqual(identity.basis, "metadata")
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(collector.scan()["records_queued"], 2)
        for envelope in collector.pending_envelopes():
            self.assertEqual(envelope["provenance"]["original_path"], str(path))
        self.assertEqual(collector.flush()["acked"], 2)
        row = collector.db.execute("SELECT * FROM files").fetchone()
        self.assertEqual(row["size"], 255)
        self.assertEqual(json.loads(row["snapshot_stat"])[2], path.stat().st_size)
        self.assertEqual(json.loads(row["snapshot_stat"])[1], path.stat().st_ino)
        self.assertEqual(collector.doctor()["disk_files"], 1)

    def test_equal_plain_compressed_copies_collapse_but_divergent_copy_refuses(self):
        plain = self.active / f"rollout-{SESSION}.jsonl"
        compressed = self.archived / (plain.name + ".zst")
        plain.write_bytes(zstd.decompress(FIXTURE.read_bytes()))
        compressed.write_bytes(FIXTURE.read_bytes())
        collector = self.collector()
        self.addCleanup(collector.close)
        first = collector.scan()
        self.assertEqual(first["records_queued"], 2)
        self.assertEqual(first["duplicate_sessions"], 1)
        self.assertEqual(first["identity_conflicts"], 0)
        self.assertEqual(collector.flush()["acked"], 2)
        before = self._rows(collector)
        compressed.write_bytes(zstd.compress(plain.read_bytes().replace(b"synthetic work", b"different work")))
        conflict = collector.scan()
        self.assertEqual(conflict["identity_conflicts"], 1)
        self.assertEqual(conflict["records_queued"], 0)
        self.assertEqual(conflict["tombstones_queued"], 0)
        self.assertEqual(self._rows(collector), before)

    def test_lost_ack_replays_same_batch_after_plain_to_compressed_move(self):
        plain = self.active / f"rollout-{SESSION}.jsonl"
        fixture._rollout(plain, SESSION)
        collector = self.collector()
        collector.scan()
        before = self._rows(collector)
        decoded_size = plain.stat().st_size

        def lost_ack(_ack):
            raise RuntimeError("synthetic lost acknowledgement")

        collector._after_remote_commit = lost_ack
        with self.assertRaisesRegex(RuntimeError, "lost acknowledgement"):
            collector.flush()
        collector.close()
        compressed = self.archived / (plain.name + ".zst")
        compressed.write_bytes(zstd.compress(plain.read_bytes()))
        plain.unlink()
        resumed = self.collector()
        self.addCleanup(resumed.close)
        result = resumed.scan()
        self.assertEqual(result["records_queued"], 0)
        self.assertEqual(result["tombstones_queued"], 0)
        flushed = resumed.flush()
        self.assertEqual(flushed["acked"], 2)
        self.assertEqual(flushed["replayed_batches"], 1)
        self.assertEqual([x[0] for x in self._rows(resumed)], [x[0] for x in before])
        self.assertEqual(tuple(resumed.db.execute(
            "SELECT scanned_offset,committed_offset FROM files WHERE path=?", (str(compressed),),
        ).fetchone()), (decoded_size, decoded_size))

    def test_partial_scan_restart_and_incomplete_line_retain_decoded_offsets(self):
        path = self.archived / f"rollout-{SESSION}.jsonl.zst"
        raw = zstd.decompress(FIXTURE.read_bytes())
        path.write_bytes(FIXTURE.read_bytes())
        first = self.collector(max_scan_records=1)
        self.assertEqual(first.scan()["records_queued"], 1)
        first.close()
        resumed = self.collector()
        self.addCleanup(resumed.close)
        self.assertEqual(resumed.scan()["records_queued"], 1)
        self.assertEqual(resumed.flush()["acked"], 2)
        prefix = self._rows(resumed)
        path.write_bytes(zstd.compress(raw + b'{"partial":'))
        partial = resumed.scan()
        self.assertEqual(partial["partial_files"], 1)
        self.assertEqual(partial["records_queued"], 0)
        self.assertEqual(resumed.db.execute("SELECT scanned_offset FROM files").fetchone()[0], len(raw))
        path.write_bytes(zstd.compress(raw + b'{"partial":true}\n'))
        self.assertEqual(resumed.scan()["records_queued"], 1)
        self.assertEqual(resumed.flush()["acked"], 1)
        self.assertTrue(set(prefix) <= set(self._rows(resumed)))

    def test_compressed_disappearance_retains_captured_history(self):
        path = self.archived / f"rollout-{SESSION}.jsonl.zst"
        path.write_bytes(FIXTURE.read_bytes())
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(collector.scan()["records_queued"], 2)
        self.assertEqual(collector.flush()["acked"], 2)
        before = self._rows(collector)
        path.unlink()
        self.assertEqual(collector.scan()["tombstones_queued"], 0)
        self.assertEqual(collector.flush()["acked"], 0)
        self.assertEqual(self._rows(collector), before)

    def test_corrupt_frame_refuses_then_malformed_json_keeps_native_disposition(self):
        path = self.archived / f"rollout-{SESSION}.jsonl.zst"
        path.write_bytes(FIXTURE.read_bytes()[:-1])
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(collector.scan()["quarantined_files"], 1)
        self.assertEqual(collector.db.execute(
            "SELECT status FROM codex_session_locations WHERE path=?", (str(path),),
        ).fetchone()[0], "identity_unavailable")
        self.assertEqual(collector.db.execute(
            "SELECT error_code FROM dead_letters WHERE path=?", (str(path),),
        ).fetchone()[0], "decoded_source_invalid")
        self.assertEqual(self._rows(collector), [])
        raw = zstd.decompress(FIXTURE.read_bytes()) + b'{bad}\n{"partial":'
        path.write_bytes(zstd.compress(raw))
        result = collector.scan()
        self.assertEqual(result["records_queued"], 2)
        self.assertEqual(result["parse_errors"], 1)
        self.assertEqual(result["partial_files"], 1)
        self.assertEqual(collector.flush()["acked"], 2)
        self.assertEqual(collector.db.execute("SELECT count(*) FROM dead_letters").fetchone()[0], 2)
        self.assertEqual(collector.db.execute(
            "SELECT count(*) FROM dead_letters WHERE error_code='decoded_source_invalid'",
        ).fetchone()[0], 1)
        self.assertEqual(collector.db.execute("SELECT scanned_offset FROM files").fetchone()[0], 261)
        self.assertEqual(zstd.decompress(path.read_bytes()), raw)

    def test_acknowledged_cold_snapshot_needs_no_decode(self):
        path = self.archived / f"rollout-{SESSION}.jsonl.zst"
        path.write_bytes(FIXTURE.read_bytes())
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(collector.scan()["records_queued"], 2)
        self.assertEqual(collector.flush()["acked"], 2)
        before = self._rows(collector)
        with patch("collector.collector.open_decoded", side_effect=AssertionError("unexpected decode")):
            with patch("collector.collector.decoded_snapshot", side_effect=AssertionError("unexpected snapshot")):
                self.assertEqual(collector.scan()["records_queued"], 0)
        self.assertEqual(self._rows(collector), before)

    def test_partial_resume_binds_stream_to_observed_physical_signature(self):
        path = self.archived / f"rollout-{SESSION}.jsonl.zst"
        path.write_bytes(FIXTURE.read_bytes())
        first = self.collector(max_scan_records=1)
        self.assertEqual(first.scan()["records_queued"], 1)
        first.close()
        resumed = self.collector()
        self.addCleanup(resumed.close)
        before = self._rows(resumed)
        original = resumed._open_source

        def replace_before_open(input_path, **kwargs):
            self.assertIsNotNone(kwargs.get("expected_signature"))
            input_path.write_bytes(zstd.compress(zstd.decompress(FIXTURE.read_bytes()) + b'{}\n'))
            return original(input_path, **kwargs)

        resumed._open_source = replace_before_open
        with self.assertRaises(CollectorRuntimeError) as caught:
            resumed.scan()
        self.assertEqual(caught.exception.error_code, "source_snapshot_changed")
        self.assertEqual(self._rows(resumed), before)

    def test_partial_compression_move_retains_original_repeat_budget(self):
        plain = self.active / f"rollout-{SESSION}.jsonl"
        raw = zstd.decompress(FIXTURE.read_bytes()).splitlines(keepends=True)
        plain.write_bytes(raw[0] + raw[1] * 10)
        first = self.collector(max_scan_records=3)
        self.assertEqual(first.scan()["records_queued"], 3)
        self.assertEqual(first.flush()["acked"], 3)
        first.close()
        compressed = self.archived / (plain.name + ".zst")
        compressed.write_bytes(zstd.compress(plain.read_bytes()))
        plain.unlink()
        resumed = self.collector()
        self.addCleanup(resumed.close)
        self.assertEqual(resumed.scan()["records_queued"], 1)
        self.assertEqual(resumed.flush()["acked"], 1)
        self.assertEqual(len(self._rows(resumed)), 4)

    def test_corrupt_first_copy_does_not_starve_valid_history(self):
        bad = self.active / f"rollout-{SESSION}.jsonl.zst"
        good = self.active / "rollout-019f9999-2222-7333-8444-555555555555.jsonl"
        bad.write_bytes(b"not a zstd frame")
        fixture._rollout(good, "019f9999-2222-7333-8444-555555555555")
        collector = self.collector()
        self.addCleanup(collector.close)
        for _ in range(3):
            try:
                collector.scan()
            except CollectorRuntimeError:
                pass
            collector.flush()
        self.assertEqual(len(self._rows(collector)), 2)
        self.assertEqual(collector.db.execute(
            "SELECT count(*) FROM files WHERE path=?", (str(bad),),
        ).fetchone()[0], 0)

    def test_corrupt_tail_after_identity_does_not_starve_valid_history(self):
        bad = self.active / f"rollout-{SESSION}.jsonl.zst"
        good = self.active / "rollout-019f9999-2222-7333-8444-555555555555.jsonl"
        bad.write_bytes(FIXTURE.read_bytes())
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(collector.scan()["records_queued"], 2)
        self.assertEqual(collector.flush()["acked"], 2)
        prefix = self._rows(collector)
        checkpoint = dict(collector.db.execute(
            "SELECT * FROM files WHERE path=?", (str(bad),),
        ).fetchone())
        fixture._rollout(good, "019f9999-2222-7333-8444-555555555555")
        bad.write_bytes(zstd.compress(zstd.decompress(FIXTURE.read_bytes()) + b"{}\n" * 50_000) + b"bad")
        os.utime(bad, (time.time() + 1, time.time() + 1))
        failures = 0
        for _ in range(3):
            try:
                collector.scan()
            except CollectorRuntimeError as error:
                self.assertEqual(error.error_code, "decoded_source_invalid")
                failures += 1
            collector.flush()
        self.assertGreater(failures, 0)
        self.assertEqual(len(self._rows(collector)), 4)
        self.assertTrue(set(prefix) <= set(self._rows(collector)))
        self.assertEqual(dict(collector.db.execute(
            "SELECT * FROM files WHERE path=?", (str(bad),),
        ).fetchone()), checkpoint)

    def test_corrupt_duplicate_group_retains_history_and_allows_other_session(self):
        plain = self.active / f"rollout-{SESSION}.jsonl"
        plain.write_bytes(zstd.decompress(FIXTURE.read_bytes()))
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(collector.scan()["records_queued"], 2)
        self.assertEqual(collector.flush()["acked"], 2)
        prefix = self._rows(collector)
        corrupt = self.archived / (plain.name + ".zst")
        corrupt.write_bytes(zstd.compress(plain.read_bytes() + b"{}\n" * 50_000) + b"bad")
        other_id = "019f9999-2222-7333-8444-555555555555"
        fixture._rollout(self.active / f"rollout-{other_id}.jsonl", other_id)
        result = collector.scan()
        self.assertEqual(result["identity_conflicts"], 1)
        self.assertEqual(result["records_queued"], 2)
        self.assertEqual(result["tombstones_queued"], 0)
        self.assertEqual(collector.flush()["acked"], 2)
        self.assertTrue(set(prefix) <= set(self._rows(collector)))
        self.assertEqual(len(self._rows(collector)), 4)

    def test_corrupt_segment_duplicate_protects_segment_and_allows_other_session(self):
        segment_id = "019f6666-7777-7888-8999-aaaaaaaaaaaa"
        plain = self.active / f"rollout-2026-08-11T00-00-00-{SESSION}_{segment_id}.jsonl"
        plain.write_text(
            fixture._line({"type": "session_meta", "payload": {
                "id": SESSION, "session_id": SESSION, "history_mode": "paginated",
                "history_base": {"thread_id": SESSION, "end_ordinal_exclusive": 2, "end_byte_offset": 255},
            }})
            + fixture._line({"type": "response_item", "payload": {"type": "message", "marker": "segment"}}),
        )
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(collector.scan()["records_queued"], 2)
        self.assertEqual(collector.flush()["acked"], 2)
        prefix = self._rows(collector)
        corrupt = self.archived / (plain.name + ".zst")
        corrupt.write_bytes(zstd.compress(plain.read_bytes() + b"{}\n" * 50_000) + b"bad")
        other_id = "019f9999-2222-7333-8444-555555555555"
        fixture._rollout(self.active / f"rollout-{other_id}.jsonl", other_id)
        result = collector.scan()
        self.assertEqual(result["identity_conflicts"], 1)
        self.assertEqual(result["records_queued"], 2)
        self.assertEqual(result["tombstones_queued"], 0)
        self.assertEqual(collector.flush()["acked"], 2)
        self.assertTrue(set(prefix) <= set(self._rows(collector)))
        self.assertEqual(len(self._rows(collector)), 4)

    def test_flush_failure_clears_scan_decode_deadline(self):
        collector = self.collector(brain_writer=object(), archive=object(), tenant_id="tenant:test")
        self.addCleanup(collector.close)
        with patch.object(collector, "flush", side_effect=RuntimeError("synthetic flush failure")):
            with self.assertRaisesRegex(RuntimeError, "synthetic flush failure"):
                collector.scan()
        self.assertIsNone(getattr(collector, "_scan_source_deadline", None))

    def test_identity_deadline_reuses_completed_discovery_on_next_scan(self):
        first = self.active / f"rollout-{SESSION}.jsonl.zst"
        second_id = "019f9999-2222-7333-8444-555555555555"
        second = self.active / f"rollout-{second_id}.jsonl.zst"
        first.write_bytes(FIXTURE.read_bytes())
        second.write_bytes(zstd.compress(
            zstd.decompress(FIXTURE.read_bytes()).replace(SESSION.encode(), second_id.encode()),
        ))
        collector = self.collector()
        self.addCleanup(collector.close)
        completed = []

        def first_allowance(view):
            if completed:
                raise DecodedSourceDeadline("decoded_source_deadline")
            result = resolve_codex_session_identity(view)
            completed.append(view.source_path)
            return result

        with patch("collector.collector.resolve_codex_session_identity", side_effect=first_allowance):
            with self.assertRaises(CollectorRuntimeError) as caught:
                collector.scan()
        self.assertEqual(caught.exception.error_code, "source_snapshot_deadline")
        self.assertIsNone(collector.db.execute(
            "SELECT value FROM meta WHERE key='scan_resume_after_path'",
        ).fetchone())
        self.assertEqual(self._rows(collector), [])

        def next_allowance(view):
            self.assertNotEqual(view.source_path, completed[0])
            return resolve_codex_session_identity(view)

        with patch("collector.collector.resolve_codex_session_identity", side_effect=next_allowance):
            self.assertEqual(collector.scan()["records_queued"], 4)
        self.assertEqual(collector.flush()["acked"], 4)

    def test_dead_payload_recovery_uses_exact_decoded_windows(self):
        path = self.archived / f"rollout-{SESSION}.jsonl.zst"
        path.write_bytes(FIXTURE.read_bytes())
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(collector.scan()["records_queued"], 2)
        before = collector.pending_envelopes()
        with patch("collector.collector.MAX_BATCH_BYTES", 300):
            self.assertEqual(collector.flush()["acked"], 0)
        self.assertEqual(collector.doctor()["dead"], 2)
        with patch("collector.collector.MAX_BATCH_BYTES", 10_000):
            recovered = collector.flush()
        self.assertEqual(recovered["recovered"], 2)
        self.assertEqual(recovered["acked"], 2)
        self.assertEqual({row[0] for row in self._rows(collector)},
                         {event["native_id"] for event in before})

    def test_pending_archive_repair_uses_exact_decoded_windows(self):
        path = self.archived / f"rollout-{SESSION}.jsonl.zst"
        path.write_bytes(FIXTURE.read_bytes())
        legacy = self.collector()
        self.assertEqual(legacy.scan()["records_queued"], 2)
        before = legacy.pending_envelopes()
        legacy.close()
        archived = []
        ingested = []

        class Archive:
            def put_raw(inner, **kwargs):
                archived.append(json.loads(kwargs["payload"]))
                return collector_fixture.CollectorTest._reference_for(self, kwargs, kwargs["payload"])

        class Writer:
            def ingest(inner, events):
                ingested.extend(events)
                return {
                    "status": "committed", "inserted": len(events),
                    "duplicate_events": 0, "replay": False,
                    "receipts": [
                        f"recall://{event['source_id']}/{event['native_id']}?rev=1"
                        for event in events
                    ],
                }

        canonical = self.collector(
            brain_writer=Writer(), archive=Archive(), tenant_id="tenant:test",
        )
        self.addCleanup(canonical.close)
        self.assertEqual(canonical.flush()["acked"], 2)
        self.assertEqual(archived, [json.loads(line) for line in
                                   zstd.decompress(FIXTURE.read_bytes()).splitlines()])
        self.assertEqual([event["content_sha256"] for event in ingested],
                         [event["content_sha256"] for event in before])
        self.assertTrue(all(event["provenance"]["original_path"] == str(path) for event in ingested))

    def test_compressed_paginated_segment_keeps_native_parent_and_lineage(self):
        base = self.archived / f"rollout-{SESSION}.jsonl.zst"
        base.write_bytes(FIXTURE.read_bytes())
        segment_id = "019f6666-7777-7888-8999-aaaaaaaaaaaa"
        segment = self.active / f"rollout-2026-08-11T00-00-00-{SESSION}_{segment_id}.jsonl.zst"
        lineage = {"thread_id": SESSION, "end_ordinal_exclusive": 2, "end_byte_offset": 255}
        records = (
            fixture._line({"type": "session_meta", "payload": {
                "id": SESSION, "session_id": SESSION,
                "history_mode": "paginated", "history_base": lineage,
            }})
            + fixture._line({"type": "response_item", "payload": {
                "type": "message", "marker": "separate segment",
            }})
        ).encode()
        segment.write_bytes(zstd.compress(records))
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(collector.scan()["records_queued"], 4)
        envelopes = collector.pending_envelopes()
        self.assertEqual(len({item["native_parent_id"] for item in envelopes}), 2)
        for item in envelopes:
            if item["provenance"]["original_path"] == str(segment):
                self.assertEqual(item["provenance"]["codex_segment_id"], segment_id)
                self.assertEqual(item["provenance"]["codex_history_base"], lineage)
        self.assertEqual(collector.flush()["acked"], 4)
        before = self._rows(collector)
        segment.with_suffix("").write_bytes(records)
        segment.unlink()
        self.assertEqual(collector.scan()["records_queued"], 0)
        self.assertEqual(collector.flush()["acked"], 0)
        self.assertEqual(self._rows(collector), before)


if __name__ == "__main__":
    unittest.main()
