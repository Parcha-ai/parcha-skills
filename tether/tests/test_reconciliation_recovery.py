import io
import pathlib
import tempfile
import unittest
from unittest import mock

from test_bridge import load_runtime


TEAM = "T12345678"
CHANNEL = "C12345678"
THREAD = "1790000000.000001"


class ReconciliationRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.database_path = self.home / "bridges.db"
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.database_path)
        self.broker = self.runtime.Broker(
            "test-token",
            self.store,
            verified_workspace_team_id=TEAM,
        )

    def tearDown(self):
        self.temp.cleanup()

    def reserve(self, target_id: str):
        key = self.store.reconciliation_key(
            "message",
            TEAM,
            CHANNEL,
            THREAD,
            target_id,
        )
        self.store.ensure_reconciliation(
            reconciliation_key=key,
            team_id=TEAM,
            method="conversations.replies",
            channel_id=CHANNEL,
            thread_ts=THREAD,
            target_kind="message",
            target_id=target_id,
        )
        return key

    def force_due(self):
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE slack_reconciliations
                SET next_attempt_at=datetime('now','-1 second')
                """
            )
            database.execute(
                """
                UPDATE slack_reconciliation_limits
                SET next_allowed_at=datetime('now','-1 second')
                """
            )

    def row(self, key):
        with self.store.connect() as database:
            saved = database.execute(
                """
                SELECT * FROM slack_reconciliations
                WHERE reconciliation_key=?
                """,
                (key,),
            ).fetchone()
        return dict(saved)

    def restart(self):
        runtime = load_runtime(self.home)
        store = runtime.Store(self.database_path)
        broker = runtime.Broker(
            "test-token",
            store,
            verified_workspace_team_id=TEAM,
        )
        return runtime, store, broker

    def test_recovery_warning_is_rate_limited_and_does_not_log_exception_text(self):
        self.runtime._RECOVERY_WARNING_TIMES.clear()
        output = io.StringIO()
        with mock.patch.object(
            self.runtime.time,
            "monotonic",
            side_effect=(100.0, 101.0, 500.0),
        ), mock.patch.object(self.runtime.sys, "stderr", output):
            self.runtime._warn_recovery_failure(
                "reply",
                RuntimeError("credential-value-must-not-leak"),
            )
            self.runtime._warn_recovery_failure(
                "reply",
                RuntimeError("credential-value-must-not-leak"),
            )
            self.runtime._warn_recovery_failure(
                "reply",
                RuntimeError("credential-value-must-not-leak"),
            )

        self.assertEqual(output.getvalue().count("Tether reply recovery failed"), 2)
        self.assertNotIn("credential-value-must-not-leak", output.getvalue())

    def test_one_page_is_persisted_and_restart_resumes_exact_cursor(self):
        client_msg_id = "11111111-1111-5111-8111-111111111111"
        key = self.reserve(client_msg_id)
        calls = []

        def first_page(_token, method, payload):
            calls.append((method, dict(payload)))
            return {
                "messages": [],
                "response_metadata": {"next_cursor": "cursor-1"},
            }

        with mock.patch.object(
            self.runtime,
            "_slack_call",
            side_effect=first_page,
        ), self.assertRaisesRegex(
            self.runtime.NativeContinuationError,
            "pending",
        ):
            self.broker._process_reconciliation(key)

        self.assertEqual(len(calls), 1)
        self.assertNotIn("cursor", calls[0][1])
        self.assertEqual(calls[0][1]["limit"], 15)
        self.assertEqual(
            (self.row(key)["next_cursor"], self.row(key)["pages_seen"]),
            ("cursor-1", 1),
        )

        runtime, store, broker = self.restart()
        with store.connect() as database:
            database.execute(
                """
                UPDATE slack_reconciliations
                SET next_attempt_at=datetime('now','-1 second')
                WHERE reconciliation_key=?
                """,
                (key,),
            )
            database.execute(
                """
                UPDATE slack_reconciliation_limits
                SET next_allowed_at=datetime('now','-1 second')
                """
            )

        def second_page(_token, method, payload):
            self.assertEqual(method, "conversations.replies")
            self.assertEqual(payload["cursor"], "cursor-1")
            return {
                "messages": [
                    {
                        "ts": "1790000001.000001",
                        "metadata": {
                            "event_type": "tether_message",
                            "event_payload": {
                                "client_msg_id": client_msg_id,
                            },
                        },
                    }
                ],
                "response_metadata": {"next_cursor": "cursor-2"},
            }

        with mock.patch.object(
            runtime,
            "_slack_call",
            side_effect=second_page,
        ):
            self.assertEqual(
                broker._process_reconciliation(key),
                "1790000001.000001",
            )
        with store.connect() as database:
            state = database.execute(
                """
                SELECT state,result_ts,pages_seen
                FROM slack_reconciliations
                WHERE reconciliation_key=?
                """,
                (key,),
            ).fetchone()
        self.assertEqual(
            (state["state"], state["result_ts"], state["pages_seen"]),
            ("found", "1790000001.000001", 2),
        )

    def test_workspace_method_budget_allows_only_one_page_per_interval(self):
        first = self.reserve("22222222-2222-5222-8222-222222222222")
        second = self.reserve("33333333-3333-5333-8333-333333333333")
        calls = []

        def page(_token, _method, _payload):
            calls.append(1)
            return {
                "messages": [],
                "response_metadata": {"next_cursor": "more"},
            }

        with mock.patch.object(self.runtime, "_slack_call", side_effect=page):
            with self.assertRaises(self.runtime.NativeContinuationError):
                self.broker._process_reconciliation(first)
            with self.assertRaises(self.runtime.NativeContinuationError):
                self.broker._process_reconciliation(second)
        self.assertEqual(calls, [1])
        self.assertEqual(
            self.store.pending_reconciliation_keys()[0],
            second,
            "the unserved target must rotate ahead of the paginated target",
        )

    def test_repeated_cursor_fails_closed_and_survives_restart(self):
        key = self.reserve("44444444-4444-5444-8444-444444444444")
        with mock.patch.object(
            self.runtime,
            "_slack_call",
            return_value={
                "messages": [],
                "response_metadata": {"next_cursor": "repeat-me"},
            },
        ), self.assertRaises(self.runtime.NativeContinuationError):
            self.broker._process_reconciliation(key)
        self.force_due()
        with mock.patch.object(
            self.runtime,
            "_slack_call",
            return_value={
                "messages": [],
                "response_metadata": {"next_cursor": "repeat-me"},
            },
        ), self.assertRaisesRegex(
            self.runtime.NativeContinuationError,
            "failed closed",
        ):
            self.broker._process_reconciliation(key)
        self.assertEqual(
            (self.row(key)["state"], self.row(key)["error"]),
            ("failed", "slack_reconciliation_invalid_cursor"),
        )

        runtime, _store, broker = self.restart()
        with mock.patch.object(runtime, "_slack_call") as network, self.assertRaises(
            runtime.NativeContinuationError
        ):
            broker._process_reconciliation(key)
        network.assert_not_called()

    def test_invalid_cursor_type_and_page_limit_fail_closed(self):
        invalid = self.reserve("55555555-5555-5555-8555-555555555555")
        with mock.patch.object(
            self.runtime,
            "_slack_call",
            return_value={
                "messages": [],
                "response_metadata": {"next_cursor": 123},
            },
        ), self.assertRaises(self.runtime.NativeContinuationError):
            self.broker._process_reconciliation(invalid)
        self.assertEqual(self.row(invalid)["state"], "failed")

        limited = self.reserve("66666666-6666-5666-8666-666666666666")
        self.force_due()
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE slack_reconciliations SET pages_seen=999
                WHERE reconciliation_key=?
                """,
                (limited,),
            )
        with mock.patch.object(
            self.runtime,
            "_slack_call",
            return_value={
                "messages": [],
                "response_metadata": {"next_cursor": "page-1001"},
            },
        ), self.assertRaises(self.runtime.NativeContinuationError):
            self.broker._process_reconciliation(limited)
        self.assertEqual(
            self.row(limited)["error"],
            "slack_reconciliation_page_limit",
        )

    def test_network_failure_is_deferred_before_any_reconciliation_read(self):
        request = {
            "op": "thread_reply",
            "team_id": TEAM,
            "channel_id": CHANNEL,
            "thread_ts": THREAD,
            "text": "Outcome uncertain",
            "idempotency_key": "defer-after-network-failure",
        }
        with mock.patch.object(
            self.broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=TimeoutError("response lost"),
        ), self.assertRaises(TimeoutError):
            self.broker.handle(request)

        with mock.patch.object(self.runtime, "_slack_call") as history:
            self.assertEqual(self.broker.recover_reconciliations(), 0)
        history.assert_not_called()


if __name__ == "__main__":
    unittest.main()
