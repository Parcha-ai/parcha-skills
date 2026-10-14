#!/usr/bin/env python3
"""Retirement deadline/query exchanges preserve PostgreSQL transaction semantics."""

# ruff: noqa: E402
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import psycopg
from psycopg.rows import dict_row

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
from recall_server.db import BrainStore, SearchDeadlineExceeded
from recall_server.retirement_process import RetirementStore


class RetirementQueryPipelineTests(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg.connect(
            os.environ["RECALL_DATABASE_URL"], row_factory=dict_row
        )
        self.addCleanup(self.conn.close)
        self.conn.execute("CREATE TEMP TABLE pipeline_fixture (id integer PRIMARY KEY)")
        self.conn.commit()

    def traced_query(self, execute):
        # Open the owner transaction before tracing, excluding BEGIN's exchange.
        with self.conn.transaction():
            with tempfile.TemporaryFile() as trace:
                self.conn.pgconn.trace(trace.fileno())
                try:
                    cursor = execute(
                        self.conn,
                        "SELECT %s::integer AS value, current_setting('statement_timeout') AS timeout",
                        [42],
                        time.monotonic() + 5,
                    )
                    rows = cursor.fetchall()
                finally:
                    self.conn.pgconn.untrace()
                trace.seek(0)
                lines = trace.read().decode().splitlines()
        return rows, [line.split("\t")[3] for line in lines if "\tF\t" in line]

    def test_deadline_then_query_use_one_protocol_sync(self):
        original_rows, original_messages = self.traced_query(
            BrainStore._execute_bounded
        )
        rows, messages = self.traced_query(RetirementStore._execute_bounded)
        self.assertEqual(rows[0]["value"], original_rows[0]["value"])
        self.assertNotEqual(rows[0]["timeout"], "0")
        self.assertEqual(original_messages.count("Sync"), 2)
        self.assertEqual(messages.count("Execute"), 2)
        self.assertEqual(messages.count("Sync"), 1)
        self.assertEqual(
            [kind for kind in messages if kind in {"Execute", "Sync"}],
            ["Execute", "Execute", "Sync"],
        )

    def execute(self, sql, values=(), deadline=None):
        return RetirementStore._execute_bounded(
            self.conn,
            sql,
            values,
            time.monotonic() + 5 if deadline is None else deadline,
        )

    def test_cancelled_target_rolls_back_owner_transaction(self):
        with self.assertRaises(SearchDeadlineExceeded) as caught:
            with self.conn.transaction():
                self.execute("INSERT INTO pipeline_fixture VALUES (%s)", [1])
                self.execute("SELECT pg_sleep(%s)", [1], time.monotonic() + 0.1)
        self.assertIsInstance(caught.exception.__cause__, psycopg.errors.QueryCanceled)
        self.assertEqual(caught.exception.__cause__.sqlstate, "57014")
        self.assertEqual(
            self.conn.execute("SELECT count(*) AS n FROM pipeline_fixture").fetchone()[
                "n"
            ],
            0,
        )
        self.assertEqual(self.execute("SELECT 42 AS value").fetchone(), {"value": 42})

    def test_cancelled_target_without_transaction_block_rolls_back(self):
        self.execute("INSERT INTO pipeline_fixture VALUES (%s)", [1])
        with self.assertRaises(SearchDeadlineExceeded):
            self.execute("SELECT pg_sleep(%s)", [1], time.monotonic() + 0.1)
        self.assertEqual(
            self.conn.execute("SELECT count(*) AS n FROM pipeline_fixture").fetchone()[
                "n"
            ],
            0,
        )

    def test_expired_deadline_never_sends_target(self):
        with self.assertRaises(SearchDeadlineExceeded):
            with self.conn.transaction():
                self.execute(
                    "INSERT INTO pipeline_fixture VALUES (%s)",
                    [1],
                    time.monotonic() - 1,
                )
        self.assertEqual(
            self.conn.execute("SELECT count(*) AS n FROM pipeline_fixture").fetchone()[
                "n"
            ],
            0,
        )

    def test_original_sql_error_survives_pipeline_exit(self):
        with self.assertRaises(psycopg.errors.UniqueViolation) as caught:
            with self.conn.transaction():
                self.execute("INSERT INTO pipeline_fixture VALUES (%s)", [1])
                self.execute("INSERT INTO pipeline_fixture VALUES (%s)", [1])
        self.assertEqual(caught.exception.sqlstate, "23505")
        self.assertEqual(
            self.conn.execute("SELECT count(*) AS n FROM pipeline_fixture").fetchone()[
                "n"
            ],
            0,
        )

    def test_cursor_ready_after_pipeline_then_copy_and_server_cursor(self):
        with self.conn.transaction():
            cursor = self.execute(
                "INSERT INTO pipeline_fixture VALUES (%s), (%s) RETURNING id", [1, 2]
            )
            self.assertEqual(cursor.rowcount, 2)
            self.assertEqual(cursor.fetchall(), [{"id": 1}, {"id": 2}])
            self.assertEqual(
                self.conn.info.pipeline_status, psycopg.pq.PipelineStatus.OFF
            )
            with self.conn.cursor().copy("COPY pipeline_fixture FROM STDIN") as copy:
                copy.write_row((3,))
            with self.conn.cursor(name="proof_metadata") as server_cursor:
                server_cursor.execute("SELECT id FROM pipeline_fixture ORDER BY id")
                self.assertEqual(server_cursor.fetchmany(2), [{"id": 1}, {"id": 2}])
                self.assertEqual(server_cursor.fetchall(), [{"id": 3}])

    def test_no_deadline_keeps_unbounded_execution(self):
        with patch.object(
            self.conn, "pipeline", side_effect=AssertionError("unexpected pipeline")
        ):
            cursor = RetirementStore._execute_bounded(
                self.conn, "SELECT 42 AS value", (), None
            )
        self.assertEqual(cursor.fetchone(), {"value": 42})

    def test_connection_failure_is_not_reclassified_as_deadline(self):
        with self.assertRaises(psycopg.OperationalError):
            self.execute("SELECT pg_terminate_backend(pg_backend_pid())")
        self.assertTrue(self.conn.broken)

    def test_original_full_parent_proof_and_lost_ack_suite_with_adapter(self):
        # Reuse original owner invariants, including committed/lost-ACK recovery,
        # hash checks, stale epochs and restore/disable races, with this adapter.
        import e2e_retirement_runner as fixtures

        with patch.object(
            fixtures.TrackedStore,
            "_execute_bounded",
            staticmethod(RetirementStore._execute_bounded),
        ):
            fixtures.main()


if __name__ == "__main__":
    unittest.main()
