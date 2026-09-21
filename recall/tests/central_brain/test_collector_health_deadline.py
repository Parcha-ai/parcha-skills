"""A heartbeat shares one SQL deadline, including time spent acquiring its connection."""
from contextlib import contextmanager
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))
from recall_server.app import Handler
from recall_server.db import BrainStore, SearchDeadlineExceeded


def report():
    return dict(schema_version=1, collector_kind="codex", collector_version=1,
                status="ready", scan_complete=True, pending_records=0, dead_records=0,
                coverage_percent=100, archive_coverage_percent=100, archive_backlog=0,
                last_success_epoch=None, last_error_code=None)


class CollectorHealthDeadlineTests(unittest.TestCase):
    def setUp(self):
        self.store = BrainStore("postgresql://synthetic.invalid/recall")
        self.now = 100.0
        self.commands = []
        self.acquire_seconds = 0
        self.first_seconds = 0
        self.connection = Mock()
        self.connection.execute.side_effect = self.execute
        self.reported_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

        @contextmanager
        def connect():
            self.now += self.acquire_seconds
            yield self.connection

        self.store.connect = connect

    def execute(self, sql, values):
        self.commands.append((sql, values))
        if sql.lstrip().startswith("INSERT"):
            self.now += self.first_seconds
        return Mock(fetchone=Mock(return_value={"reported_at": self.reported_at}))

    def heartbeat(self, installation=True):
        with patch("recall_server.db.time.monotonic", side_effect=lambda: self.now):
            return self.store.record_collector_health(
                tenant_id="tenant", source_id="source",
                installation_id=uuid.UUID(int=1) if installation else None,
                report=report(),
            )

    def test_acquisition_and_first_write_consume_one_budget(self):
        self.acquire_seconds = 2
        self.first_seconds = 2
        result = self.heartbeat()
        timeouts = [values[0] for sql, values in self.commands if "set_config" in sql]
        self.assertEqual(timeouts, ["3000ms", "1000ms"])
        writes = [sql.lstrip().split()[0] for sql, _ in self.commands if "set_config" not in sql]
        self.assertEqual(writes, ["INSERT", "UPDATE"])
        self.assertEqual(result, dict(schema_version=1, status="accepted", reported_at=self.reported_at))

    def test_expired_acquisition_admits_no_sql(self):
        self.acquire_seconds = 5.01
        with self.assertRaises(SearchDeadlineExceeded):
            self.heartbeat()
        self.assertEqual(self.commands, [])

    def test_first_statement_exhaustion_never_admits_second_write(self):
        self.first_seconds = 5.01
        with self.assertRaises(SearchDeadlineExceeded):
            self.heartbeat()
        self.assertFalse(any(sql.lstrip().startswith("UPDATE") for sql, _ in self.commands))

    def test_without_installation_only_health_is_written(self):
        self.assertEqual(self.heartbeat(False)["status"], "accepted")
        self.assertEqual(sum(sql.lstrip().startswith("INSERT") for sql, _ in self.commands), 1)
        self.assertFalse(any(sql.lstrip().startswith("UPDATE") for sql, _ in self.commands))

    def test_actual_handler_keeps_accepted_and_generic_unavailable(self):
        for error, expected in ((None, 202), (SearchDeadlineExceeded("private details"), 503)):
            with self.subTest(expected=expected):
                handler = object.__new__(Handler)
                body = json.dumps(dict(tenant_id="tenant", principal_id="owner",
                                       source_id="source", report=report())).encode()
                handler.path = "/v2/collector/health"
                handler.rfile = io.BytesIO(body)
                handler.hide_non_public_route = Mock(return_value=False)
                handler.admin_web_enabled = Mock(return_value=False)
                handler.require = Mock(return_value={"installation_id": uuid.UUID(int=1)})
                handler.body_length = Mock(return_value=len(body))
                handler.canonical_authority = Mock(return_value=("tenant", "owner", "source"))
                accepted = dict(schema_version=1, status="accepted", reported_at="synthetic")
                handler.store = Mock()
                handler.store.record_collector_health = Mock(return_value=accepted, side_effect=error)
                handler.send_json = Mock()
                Handler.do_POST(handler)
                handler.send_json.assert_called_once_with(
                    expected, accepted if expected == 202 else {"error": "collector health unavailable"}
                )
                self.assertEqual(handler.store.record_collector_health.call_count, 1)


if __name__ == "__main__":
    unittest.main()
