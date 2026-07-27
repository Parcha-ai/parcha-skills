import concurrent.futures
import pathlib
import tempfile
import threading
import unittest
from unittest import mock

from test_bridge import load_runtime


class OutboxRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.database_path = self.home / "bridges.db"
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.database_path)
        self.thread_counter = 0

    def tearDown(self):
        self.temp.cleanup()

    def _bound_bridge(self, key: str):
        bridge = self.store.create(
            {
                "source_kind": "headless_run",
                "source": {"run_id": key, "cwd": "/tmp/project"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "idempotency_key": key,
            }
        )
        self.thread_counter += 1
        return self.store.bind(
            bridge.bridge_id,
            f"1788000000.{self.thread_counter:06d}",
        )

    def _stage_reply(
        self,
        key: str,
        *,
        event_id: str,
        text: str = "verified result",
    ):
        bridge = self._bound_bridge(key)
        self.assertTrue(
            self.store.enqueue_event(
                event_id,
                bridge.bridge_id,
                "continue",
            )
        )
        items = self.store.claim_event_batch(bridge.bridge_id)
        self.assertEqual([item["event_id"] for item in items], [event_id])
        reply_key = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [event_id],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [event_id],
                bridge.bridge_id,
                bridge.binding_generation,
                reply_key,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                reply_key,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        self.assertEqual(
            self.runtime.stage_reply_payload(
                self.store,
                bridge.bridge_id,
                reply_key,
                text,
            ),
            ("pending", ""),
        )
        return bridge, reply_key, text

    def _restart(self):
        runtime = load_runtime(self.home)
        store = runtime.Store(self.database_path)
        return runtime, store, runtime.Broker("test-token", store, verified_workspace_team_id="T12345678")

    @staticmethod
    def _notify_request(key: str, *, file_path: str = ""):
        request = {
            "op": "notify",
            "text": "verified result",
            "source_kind": "headless_run",
            "source": {"run_id": key, "cwd": "/tmp/project"},
            "owner_user_id": "*",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": key,
        }
        if file_path:
            request["file_path"] = file_path
        return request

    @staticmethod
    def _outbox_row(store, reply_key: str):
        with store.connect() as database:
            row = database.execute(
                """
                SELECT state,message_ts,payload_text,client_msg_id,lease_id,
                       lease_expires_at,retry_count
                FROM bridge_replies WHERE reply_key=?
                """,
                (reply_key,),
            ).fetchone()
        return dict(row) if row is not None else None

    def test_pending_staged_reply_survives_restart_and_posts_once(self):
        bridge, reply_key, text = self._stage_reply(
            "pending-restart",
            event_id="1788000001.000001",
        )
        self.assertEqual(
            self._outbox_row(self.store, reply_key)["payload_text"],
            text,
        )

        runtime, store, broker = self._restart()
        posts = []

        def post(*_args, **kwargs):
            posts.append(kwargs.get("client_msg_id"))
            return "1788000001.000002"

        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            runtime,
            "slack_post",
            side_effect=post,
        ):
            self.assertEqual(broker.recover_replies(), 1)

        self.assertEqual(len(posts), 1)
        self.assertTrue(posts[0])
        self.assertEqual(
            self._outbox_row(store, reply_key)["message_ts"],
            "1788000001.000002",
        )
        self.assertEqual(
            store.attempt_state(reply_key, bridge.bridge_id),
            "acknowledged",
        )

        restarted_runtime, restarted_store, restarted_broker = self._restart()
        with mock.patch.object(
            restarted_broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            restarted_runtime,
            "slack_post",
        ) as repost:
            self.assertEqual(restarted_broker.recover_replies(), 0)
        repost.assert_not_called()
        self.assertEqual(
            self._outbox_row(restarted_store, reply_key)["state"],
            "sent",
        )

    def test_accepted_reply_is_reconciled_after_local_complete_crash(self):
        bridge = self._bound_bridge("accepted-before-complete")
        event_id = "1788000002.000001"
        self.assertTrue(
            self.store.enqueue_event(event_id, bridge.bridge_id, "continue")
        )
        self.assertEqual(
            [item["event_id"] for item in self.store.claim_event_batch(bridge.bridge_id)],
            [event_id],
        )
        reply_key = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [event_id],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [event_id],
                bridge.bridge_id,
                bridge.binding_generation,
                reply_key,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                reply_key,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )

        accepted_client_ids = []
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")

        def accepted_post(*_args, **kwargs):
            accepted_client_ids.append(kwargs.get("client_msg_id"))
            return "1788000002.000002"

        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=accepted_post,
        ), mock.patch.object(
            self.store,
            "complete_reply",
            side_effect=RuntimeError("simulated crash before local commit"),
        ), self.assertRaisesRegex(
            RuntimeError,
            "simulated crash",
        ):
            broker.handle(
                {
                    "op": "reply",
                    "bridge_id": bridge.bridge_id,
                    "reply_key": reply_key,
                    "text": "accepted exactly once",
                }
            )

        self.assertEqual(len(accepted_client_ids), 1)
        client_msg_id = accepted_client_ids[0]
        self.assertTrue(client_msg_id)
        self.assertEqual(
            self._outbox_row(self.store, reply_key)["state"],
            "delivering",
        )
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE bridge_replies
                SET lease_expires_at=datetime('now','-1 second')
                WHERE reply_key=?
                """,
                (reply_key,),
            )
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

        runtime, store, restarted_broker = self._restart()
        history_calls = []

        def slack_call(_token, method, payload):
            history_calls.append((method, payload))
            self.assertEqual(method, "conversations.replies")
            return {
                "messages": [
                    {
                        "ts": "1788000002.000002",
                        "metadata": {
                            "event_type": "tether_reply",
                            "event_payload": {
                                "client_msg_id": client_msg_id,
                            },
                        },
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }

        with mock.patch.object(
            restarted_broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            runtime,
            "_slack_call",
            side_effect=slack_call,
        ), mock.patch.object(
            runtime,
            "slack_post",
        ) as repost:
            self.assertEqual(restarted_broker.recover_reconciliations(), 1)
            self.assertEqual(restarted_broker.recover_replies(), 1)

        repost.assert_not_called()
        self.assertEqual(len(history_calls), 1)
        row = self._outbox_row(store, reply_key)
        self.assertEqual((row["state"], row["message_ts"]), (
            "sent",
            "1788000002.000002",
        ))
        self.assertEqual(
            store.attempt_state(reply_key, bridge.bridge_id),
            "acknowledged",
        )

    def test_restart_recovers_lease_claimed_before_network(self):
        bridge, reply_key, _text = self._stage_reply(
            "claimed-before-network",
            event_id="1788000003.000001",
        )
        claimed = self.store.claim_reply(reply_key, bridge.bridge_id)
        self.assertEqual(claimed["status"], "claimed")
        self.assertEqual(
            self._outbox_row(self.store, reply_key)["state"],
            "delivering",
        )
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE bridge_replies
                SET lease_expires_at=datetime('now','-1 second')
                WHERE reply_key=?
                """,
                (reply_key,),
            )

        runtime, store, broker = self._restart()
        posts = []

        def post(*_args, **kwargs):
            posts.append(kwargs.get("client_msg_id"))
            return "1788000003.000002"

        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            runtime,
            "_slack_call",
            return_value={
                "messages": [],
                "response_metadata": {"next_cursor": ""},
            },
        ), mock.patch.object(
            runtime,
            "slack_post",
            side_effect=post,
        ):
            self.assertEqual(broker.recover_replies(), 1)

        self.assertEqual(len(posts), 1)
        self.assertEqual(
            self._outbox_row(store, reply_key)["message_ts"],
            "1788000003.000002",
        )
        self.assertEqual(
            store.attempt_state(reply_key, bridge.bridge_id),
            "acknowledged",
        )

    def test_concurrent_startup_recovery_does_not_steal_active_lease(self):
        bridge, reply_key, _text = self._stage_reply(
            "concurrent-startup",
            event_id="1788000004.000001",
        )
        runtime = load_runtime(self.home)
        stores = (
            runtime.Store(self.database_path),
            runtime.Store(self.database_path),
        )
        brokers = (
            runtime.Broker("test-token", stores[0], verified_workspace_team_id="T12345678"),
            runtime.Broker("test-token", stores[1], verified_workspace_team_id="T12345678"),
        )
        first_post_entered = threading.Event()
        release_first_post = threading.Event()
        calls = []
        calls_lock = threading.Lock()

        def post(*_args, **kwargs):
            with calls_lock:
                calls.append(kwargs.get("client_msg_id"))
                call_number = len(calls)
            if call_number == 1:
                first_post_entered.set()
                if not release_first_post.wait(timeout=5):
                    raise TimeoutError("first recovery post was not released")
                return "1788000004.000002"
            return "1788000004.000003"

        with mock.patch.object(
            brokers[0],
            "_ensure_channel_membership",
        ), mock.patch.object(
            brokers[1],
            "_ensure_channel_membership",
        ), mock.patch.object(
            runtime,
            "_slack_call",
            return_value={
                "messages": [],
                "response_metadata": {"next_cursor": ""},
            },
        ), mock.patch.object(
            runtime,
            "slack_post",
            side_effect=post,
        ), concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(brokers[0].recover_replies)
            self.assertTrue(first_post_entered.wait(timeout=5))
            second = executor.submit(brokers[1].recover_replies)
            try:
                second.result(timeout=5)
            finally:
                release_first_post.set()
            first.result(timeout=5)

        row = self._outbox_row(stores[0], reply_key)
        self.assertEqual(
            calls,
            [row["client_msg_id"]],
            "a second startup worker must not steal an unexpired delivery lease",
        )
        self.assertEqual((row["state"], row["message_ts"]), (
            "sent",
            "1788000004.000002",
        ))
        self.assertEqual(
            stores[0].attempt_state(reply_key, bridge.bridge_id),
            "acknowledged",
        )

    def test_accepted_root_is_reconciled_after_crash_before_local_bind(self):
        request = self._notify_request("root-accepted-before-bind")
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        accepted_ts = "1788000005.000001"

        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value=accepted_ts,
        ), mock.patch.object(
            self.store,
            "record_root_post",
            side_effect=RuntimeError("simulated crash before root commit"),
        ), self.assertRaisesRegex(
            RuntimeError,
            "simulated crash",
        ):
            broker.handle(request)

        bridge = self.store.create(request)
        self.assertEqual(bridge.status, "pending")
        root = self.store.root_record(bridge.bridge_id)
        self.assertEqual(root["state"], "uncertain")
        client_msg_id = root["client_msg_id"]

        runtime, store, restarted = self._restart()
        with store.connect() as database:
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

        def slack_call(_token, method, _payload):
            self.assertEqual(method, "conversations.history")
            return {
                "ok": True,
                "messages": [{
                    "ts": accepted_ts,
                    "metadata": {
                        "event_type": "tether_root",
                        "event_payload": {
                            "bridge_id": bridge.bridge_id,
                            "client_msg_id": client_msg_id,
                        },
                    },
                }],
                "response_metadata": {"next_cursor": ""},
            }

        with mock.patch.object(
            restarted,
            "_ensure_channel_membership",
        ), mock.patch.object(
            runtime,
            "_slack_call",
            side_effect=slack_call,
        ), mock.patch.object(
            runtime,
            "slack_post",
        ) as repost:
            self.assertEqual(restarted.recover_reconciliations(), 1)
            result = restarted.handle(request)

        repost.assert_not_called()
        self.assertEqual(result["thread_ts"], accepted_ts)
        self.assertTrue(result["deduplicated"])
        self.assertEqual(store.root_record(bridge.bridge_id)["state"], "complete")
        self.assertEqual(store.get(bridge.bridge_id).status, "active")

    def test_accepted_root_file_is_reconciled_after_completion_crash(self):
        uploads = self.home / "uploads"
        uploads.mkdir()
        uploads.chmod(0o700)
        source = uploads / "report.txt"
        source.write_text("synthetic report", encoding="utf-8")
        source.chmod(0o600)
        self.runtime.UPLOAD_APPROVED_ROOTS = (str(uploads),)
        request = self._notify_request(
            "root-file-accepted-before-complete",
            file_path=str(source),
        )
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        root_ts = "1788000006.000001"

        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value=root_ts,
        ), mock.patch.object(
            self.runtime,
            "_allocate_slack_upload",
            return_value=(
                "F12345678",
                "https://files.slack.com/upload/v1/test-signed-url",
            ),
        ), mock.patch.object(
            self.runtime,
            "_upload_slack_bytes",
        ), mock.patch.object(
            self.runtime,
            "_complete_slack_upload",
            return_value={"ok": True, "files": [{"id": "F12345678"}]},
        ) as complete_upload, mock.patch.object(
            self.store,
            "complete_root_file",
            side_effect=RuntimeError("simulated crash after file acceptance"),
        ), self.assertRaisesRegex(
            RuntimeError,
            "simulated crash",
        ):
            broker.handle(request)

        bridge = self.store.create(request)
        self.assertEqual(bridge.status, "active")
        self.assertEqual(bridge.thread_ts, root_ts)
        self.assertEqual(
            complete_upload.call_args.kwargs["thread_ts"],
            root_ts,
        )
        root = self.store.root_record(bridge.bridge_id)
        self.assertEqual(root["state"], "root_posted")
        self.assertTrue(pathlib.Path(root["staged_path"]).is_file())
        self.assertEqual(root["slack_file_id"], "F12345678")
        self.assertEqual(root["upload_phase"], "completion_confirmed")

        runtime, store, restarted = self._restart()

        with mock.patch.object(
            restarted,
            "_ensure_channel_membership",
        ), mock.patch.object(
            runtime,
            "_allocate_slack_upload",
        ) as allocate, mock.patch.object(
            runtime,
            "_upload_slack_bytes",
        ) as upload, mock.patch.object(
            runtime,
            "_complete_slack_upload",
        ) as complete, mock.patch.object(
            restarted,
            "_find_staged_root_file",
        ) as reconcile:
            result = restarted.handle(request)

        allocate.assert_not_called()
        upload.assert_not_called()
        complete.assert_not_called()
        reconcile.assert_not_called()
        self.assertEqual(result["thread_ts"], root_ts)
        self.assertTrue(result["deduplicated"])
        completed = store.root_record(bridge.bridge_id)
        self.assertEqual(completed["state"], "complete")
        self.assertFalse(pathlib.Path(completed["staged_path"]).exists())


if __name__ == "__main__":
    unittest.main()
