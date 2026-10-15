from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from collector import cli as collector_cli
from collector.collector import Collector
from collector.health import build_health_report


class CollectorHealthLatencyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "sessions"
        self.root.mkdir()
        self.spool = Path(self.temp.name) / "spool.sqlite3"
        self.collector = Collector(
            root=self.root, harness="claude", source_id="source:latency-test",
            spool_path=self.spool, endpoint="http://127.0.0.1:1", token="fixture",
        )
        self.addCleanup(self.collector.close)

    def seed_latencies(self, values):
        self.collector.db.executemany(
            "INSERT INTO outbox(path,native_id,content_sha256,start_offset,end_offset,"
            "envelope_json,state,queued_at,acked_at) VALUES(?,?,?,0,1,'{}','acked',100,?)",
            [("fixture", str(i), str(i), 100 + value)
             for i, value in enumerate(values)],
        )
        self.collector.db.commit()

    def test_explicit_doctor_keeps_exact_cumulative_nearest_rank(self):
        self.assertIsNone(self.collector.doctor()["ack_latency_p95_seconds"])
        # The nineteenth of twenty sorted values is 8, including duplicates.
        self.seed_latencies([8] * 18 + [100, 2])
        self.assertEqual(self.collector.doctor()["ack_latency_p95_seconds"], 8)
        # Preserve the existing non-null timestamp predicate, regardless of state.
        self.collector.db.execute(
            "UPDATE outbox SET state='dead',acked_at=300 WHERE native_id='19'"
        )
        self.assertEqual(self.collector.doctor()["ack_latency_p95_seconds"], 100)

    def test_light_doctor_preserves_health_and_backlog_without_latency_query(self):
        self.seed_latencies([1, 2, 100])
        self.collector.db.execute("UPDATE outbox SET state='pending',acked_at=NULL WHERE native_id='0'")
        self.collector.db.execute("UPDATE outbox SET state='dead' WHERE native_id='1'")
        self.collector.db.execute("INSERT INTO meta VALUES('last_success_epoch','123')")
        full = self.collector.doctor(include_dead_letters=False)
        queries = []
        self.collector.db.set_trace_callback(queries.append)
        light = self.collector.doctor(include_dead_letters=False, include_ack_latency=False)
        self.assertEqual(light, {key: value for key, value in full.items()
                                 if key != "ack_latency_p95_seconds"})
        self.assertEqual(build_health_report("claude", light), build_health_report("claude", full))
        self.assertEqual((light["pending"], light["dead"], light["last_success_epoch"]), (1, 1, 123))
        self.assertFalse(any("acked_at" in sql.lower() for sql in queries), queries)

    def test_watch_iteration_omits_sort_but_reports_same_health(self):
        self.seed_latencies([1, 100])
        queries = []
        self.collector.db.set_trace_callback(queries.append)
        writer = mock.Mock()
        writer.report_health.return_value = {"status": "accepted"}
        result = collector_cli._run_once(self.collector, writer, "claude")
        self.assertNotIn("ack_latency_p95_seconds", result["doctor"])
        self.assertFalse(any("order by acked_at-queued_at" in sql.lower() for sql in queries))
        writer.report_health.assert_called_once_with(
            build_health_report("claude", self.collector.doctor())
        )

    def test_collect_cli_omits_sort(self):
        from client import cli as client_cli

        self.seed_latencies([1, 100])
        queries = []
        self.collector.db.set_trace_callback(queries.append)
        argv = [
            "recall", "collect", "--harness", "claude", "--root", str(self.root),
            "--spool", str(self.spool), "--source-id", "source:latency-test",
            "--endpoint", "http://127.0.0.1:1", "--visibility", "private",
            "--token-file", "unused", "--privacy-mode", "off",
        ]
        output = io.StringIO()
        with mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch("sys.argv", argv), \
                mock.patch.object(client_cli, "_token", return_value="fixture"), \
                mock.patch.object(client_cli, "_canonical_clients", return_value=None), \
                mock.patch.object(client_cli, "Collector", return_value=self.collector), \
                contextlib.redirect_stdout(output):
            client_cli.main()
        result = json.loads(output.getvalue())
        self.assertNotIn("ack_latency_p95_seconds", result["doctor"])
        self.assertEqual(result["doctor"]["acked"], 2)
        self.assertFalse(any("order by acked_at-queued_at" in sql.lower() for sql in queries))

    def test_explicit_doctor_cli_still_reports_exact_latency(self):
        self.seed_latencies([8] * 18 + [100, 2])
        output = io.StringIO()
        argv = [
            "collector", "doctor", "--harness", "claude", "--root", str(self.root),
            "--spool", str(self.spool), "--source-id", "source:latency-test",
        ]
        with mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch("sys.argv", argv), contextlib.redirect_stdout(output):
            collector_cli.main()
        self.assertEqual(json.loads(output.getvalue())["ack_latency_p95_seconds"], 8)


if __name__ == "__main__":
    unittest.main()
