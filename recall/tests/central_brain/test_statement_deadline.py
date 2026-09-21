"""Shared SQL deadline setup preserves the existing execution/error boundary."""

from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, call, patch

import psycopg

sys.path[:0] = [str(Path(__file__).resolve().parents[2] / "server")]
from recall_server.db import BrainStore, SearchDeadlineExceeded


class StatementDeadlineTests(unittest.TestCase):
    def test_setup_only_sets_remaining_timeout_and_never_pings(self):
        connection = Mock()
        with patch("recall_server.db.time.monotonic", return_value=10):
            BrainStore._set_statement_deadline(connection, 10.125)
        connection.execute.assert_called_once_with(
            "SELECT set_config('statement_timeout', %s, true)", ("125ms",)
        )
        connection.rollback.assert_not_called()

    def test_absent_deadline_setup_performs_no_io(self):
        connection = Mock()
        BrainStore._set_statement_deadline(connection, None)
        connection.execute.assert_not_called()

    def test_expired_deadline_refuses_without_sql(self):
        connection = Mock()
        with patch("recall_server.db.time.monotonic", return_value=10):
            with self.assertRaises(SearchDeadlineExceeded):
                BrainStore._set_statement_deadline(connection, 10)
            with self.assertRaises(SearchDeadlineExceeded):
                BrainStore._execute_bounded(connection, "SELECT synthetic", (), 9)
        connection.execute.assert_not_called()
        connection.rollback.assert_not_called()

    def test_bounded_execute_keeps_order_result_and_values(self):
        connection = Mock()
        expected = object()
        connection.execute.side_effect = [None, expected]
        with patch("recall_server.db.time.monotonic", return_value=10):
            result = BrainStore._execute_bounded(connection, "SELECT %s", (7,), 10.125)
        self.assertIs(result, expected)
        self.assertEqual(
            connection.execute.call_args_list,
            [
                call("SELECT set_config('statement_timeout', %s, true)", ("125ms",)),
                call("SELECT %s", (7,)),
            ],
        )

    def test_no_deadline_preserves_raw_query_cancel_without_rollback(self):
        connection = Mock()
        error = psycopg.errors.QueryCanceled("synthetic")
        connection.execute.side_effect = error
        with self.assertRaises(psycopg.errors.QueryCanceled) as caught:
            BrainStore._execute_bounded(connection, "SELECT synthetic", (), None)
        self.assertIs(caught.exception, error)
        connection.rollback.assert_not_called()
        connection.execute.assert_called_once_with("SELECT synthetic", ())

    def test_timeout_setup_failure_stays_outside_execute_handler(self):
        connection = Mock()
        error = psycopg.errors.QueryCanceled("synthetic")
        connection.execute.side_effect = error
        with patch("recall_server.db.time.monotonic", return_value=10):
            with self.assertRaises(psycopg.errors.QueryCanceled) as caught:
                BrainStore._execute_bounded(connection, "SELECT synthetic", (), 11)
        self.assertIs(caught.exception, error)
        connection.rollback.assert_not_called()
        self.assertEqual(connection.execute.call_count, 1)

    def test_bounded_query_cancel_retains_cause_and_rollback_policy(self):
        for rollback_error in (None, psycopg.ProgrammingError("transaction context")):
            with self.subTest(nested=rollback_error is not None):
                connection = Mock()
                error = psycopg.errors.QueryCanceled("synthetic")
                connection.execute.side_effect = [None, error]
                connection.rollback.side_effect = rollback_error
                with patch("recall_server.db.time.monotonic", return_value=10):
                    with self.assertRaises(SearchDeadlineExceeded) as caught:
                        BrainStore._execute_bounded(
                            connection, "SELECT synthetic", (), 11
                        )
                self.assertIs(caught.exception.__cause__, error)
                connection.rollback.assert_called_once_with()
