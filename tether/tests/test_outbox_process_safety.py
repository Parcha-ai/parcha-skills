import pathlib
import tempfile
import threading
import unittest
from unittest import mock

from test_bridge import load_runtime


class OutboxProcessSafetyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.database_path = self.home / ".hermes" / "bridges.db"
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.database_path)
        self.thread_counter = 0

    def tearDown(self):
        self.temp.cleanup()

    def _runtime_epoch(self):
        runtime = load_runtime(self.home)
        self.assertEqual(runtime.DB_PATH, self.database_path)
        self.assertNotEqual(runtime.PROCESS_EPOCH, self.runtime.PROCESS_EPOCH)
        return runtime

    def _prepare_reply(self, runtime, store, key: str, event_id: str):
        bridge = store.create(
            {
                "source_kind": "headless_run",
                "source": {"run_id": key, "cwd": "/tmp/synthetic-project"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "idempotency_key": key,
            }
        )
        self.thread_counter += 1
        bridge = store.bind(
            bridge.bridge_id,
            f"1790000000.{self.thread_counter:06d}",
        )
        self.assertTrue(store.enqueue_event(event_id, bridge.bridge_id, "continue"))
        events = store.claim_event_batch(bridge.bridge_id)
        self.assertEqual([event["event_id"] for event in events], [event_id])
        reply_key = runtime.delivery_attempt_id(
            bridge.bridge_id,
            [event_id],
            bridge.binding_generation,
        )
        self.assertTrue(
            store.prepare_delivery_attempt(
                [event_id],
                bridge.bridge_id,
                bridge.binding_generation,
                reply_key,
            )
        )
        self.assertTrue(
            store.mark_attempt_awaiting_ack(
                reply_key,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        return bridge, reply_key

    def _stage_reply(self, runtime, store, key: str, event_id: str):
        bridge, reply_key = self._prepare_reply(runtime, store, key, event_id)
        self.assertEqual(
            runtime.stage_reply_payload(
                store,
                bridge.bridge_id,
                reply_key,
                "synthetic completed result",
            ),
            ("pending", ""),
        )
        return bridge, reply_key

    def _accepted_then_crashed(
        self,
        runtime,
        store,
        key: str,
        event_id: str,
    ):
        bridge, reply_key = self._prepare_reply(runtime, store, key, event_id)
        broker = runtime.Broker("synthetic-token", store, verified_workspace_team_id="T12345678")
        accepted_client_ids = []

        def accept(*_args, **kwargs):
            accepted_client_ids.append(kwargs.get("client_msg_id"))
            return f"1791000000.{self.thread_counter:06d}"

        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            runtime,
            "slack_post",
            side_effect=accept,
        ), mock.patch.object(
            store,
            "complete_reply",
            side_effect=RuntimeError("synthetic crash before local completion"),
        ), self.assertRaisesRegex(
            RuntimeError,
            "synthetic crash before local completion",
        ):
            broker.handle(
                {
                    "op": "reply",
                    "bridge_id": bridge.bridge_id,
                    "reply_key": reply_key,
                    "text": "synthetic completed result",
                }
            )

        self.assertEqual(len(accepted_client_ids), 1)
        self.assertTrue(accepted_client_ids[0])
        self.assertEqual(self._reply_row(store, reply_key)["state"], "delivering")
        return bridge, reply_key, accepted_client_ids[0]

    @staticmethod
    def _reply_row(store, reply_key: str):
        with store.connect() as database:
            row = database.execute(
                """
                SELECT state,message_ts,client_msg_id,lease_id,lease_owner,
                       lease_expires_at,retry_count,error
                FROM bridge_replies
                WHERE reply_key=?
                """,
                (reply_key,),
            ).fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def _expire_reply_lease(store, reply_key: str) -> None:
        with store.connect() as database:
            database.execute(
                """
                UPDATE bridge_replies
                SET lease_expires_at=datetime('now','-1 second')
                WHERE reply_key=? AND state='delivering'
                """,
                (reply_key,),
            )

    @staticmethod
    def _idle_recovery(_broker, stop_event, interval_seconds=10.0):
        del interval_seconds
        stop_event.wait()

    @staticmethod
    def _stop_server(server, socket_path: pathlib.Path) -> None:
        server.shutdown()
        server.server_close()
        socket_path.unlink(missing_ok=True)

    def _competing_broker_is_denied(self, runtime, socket_path: pathlib.Path):
        competitor = None
        try:
            competitor = runtime.start_broker("synthetic-token", socket_path)
        except RuntimeError as exc:
            self.assertRegex(
                str(exc),
                r"(?i)(?:(?:database|outbox).*(?:owns|singleton)|"
                r"(?:owns|singleton).*(?:database|outbox))",
            )
            return True
        finally:
            if competitor is not None:
                self._stop_server(competitor, socket_path)
        return False

    def _assert_successor_starts(self, runtime, socket_path: pathlib.Path) -> None:
        successor = runtime.start_broker("synthetic-token", socket_path)
        self._stop_server(successor, socket_path)

    def test_different_sockets_cannot_bypass_database_singleton(self):
        runtime_b = self._runtime_epoch()
        socket_a = self.home / "sockets" / "epoch-a.sock"
        socket_b = self.home / "sockets" / "epoch-b.sock"
        server_a = None
        server_b = None
        with mock.patch.object(
            self.runtime.Broker,
            "run_reply_recovery",
            new=self._idle_recovery,
        ), mock.patch.object(
            runtime_b.Broker,
            "run_reply_recovery",
            new=self._idle_recovery,
        ):
            try:
                server_a = self.runtime.start_broker(
                    "synthetic-token",
                    socket_a,
                )
                with self.assertRaisesRegex(
                    RuntimeError,
                    r"(?i)(?:(?:database|outbox).*(?:owns|singleton)|"
                    r"(?:owns|singleton).*(?:database|outbox))",
                ):
                    server_b = runtime_b.start_broker(
                        "synthetic-token",
                        socket_b,
                    )
            finally:
                if server_b is not None:
                    self._stop_server(server_b, socket_b)
                if server_a is not None:
                    self._stop_server(server_a, socket_a)

    def test_database_singleton_is_held_until_recovery_delivery_stops(self):
        self._stage_reply(
            self.runtime,
            self.store,
            "blocked-recovery",
            "1792000000.000001",
        )
        runtime_b = self._runtime_epoch()
        socket_a = self.home / "sockets" / "recovery-a.sock"
        socket_b = self.home / "sockets" / "recovery-b.sock"
        socket_successor = self.home / "sockets" / "recovery-successor.sock"
        delivery_entered = threading.Event()
        release_delivery = threading.Event()
        close_entered = threading.Event()
        close_finished = threading.Event()
        close_errors = []

        def blocked_post(*_args, **_kwargs):
            delivery_entered.set()
            if not release_delivery.wait(timeout=5):
                raise TimeoutError("synthetic recovery delivery was not released")
            return "1792000000.000002"

        server = None
        with mock.patch.object(
            self.runtime.Broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "_slack_call",
            return_value={"ok": True, "team_id": "T12345678"},
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=blocked_post,
        ), mock.patch.object(
            runtime_b.Broker,
            "run_reply_recovery",
            new=self._idle_recovery,
        ):
            server = self.runtime.start_broker("synthetic-token", socket_a)
            self.assertTrue(delivery_entered.wait(timeout=2))
            original_join = server.recovery_thread.join

            def close_server():
                try:
                    server.shutdown()
                    close_entered.set()
                    server.server_close()
                except BaseException as exc:
                    close_errors.append(exc)
                finally:
                    close_finished.set()

            with mock.patch.object(
                server.recovery_thread,
                "join",
                side_effect=lambda timeout=None: original_join(timeout=0.02),
            ):
                closer = threading.Thread(target=close_server)
                closer.start()
                self.assertTrue(close_entered.wait(timeout=2))
                close_finished.wait(timeout=0.2)
                singleton_held = self._competing_broker_is_denied(
                    runtime_b,
                    socket_b,
                )
                release_delivery.set()
                closer.join(timeout=3)

            self.assertFalse(closer.is_alive())
            self.assertEqual(close_errors, [])
            self.assertTrue(
                singleton_held,
                "server_close released the database singleton while recovery "
                "delivery was still running",
            )
            self._assert_successor_starts(runtime_b, socket_successor)

    def test_database_singleton_is_held_until_request_delivery_stops(self):
        runtime_b = self._runtime_epoch()
        socket_a = self.home / "sockets" / "handler-a.sock"
        socket_b = self.home / "sockets" / "handler-b.sock"
        socket_successor = self.home / "sockets" / "handler-successor.sock"
        delivery_entered = threading.Event()
        release_delivery = threading.Event()
        close_entered = threading.Event()
        close_finished = threading.Event()
        close_errors = []
        client_results = []
        client_errors = []

        def blocked_post(*_args, **_kwargs):
            delivery_entered.set()
            if not release_delivery.wait(timeout=5):
                raise TimeoutError("synthetic request delivery was not released")
            return "1793000000.000002"

        with mock.patch.object(
            self.runtime.Broker,
            "run_reply_recovery",
            new=self._idle_recovery,
        ), mock.patch.object(
            runtime_b.Broker,
            "run_reply_recovery",
            new=self._idle_recovery,
        ):
            server = self.runtime.start_broker("synthetic-token", socket_a)
            bridge, reply_key = self._prepare_reply(
                self.runtime,
                server.broker.store,
                "blocked-handler",
                "1793000000.000001",
            )

            def call_broker():
                try:
                    client_results.append(
                        self.runtime.broker_call(
                            {
                                "op": "reply",
                                "bridge_id": bridge.bridge_id,
                                "reply_key": reply_key,
                                "text": "synthetic completed result",
                            },
                            socket_a,
                        )
                    )
                except BaseException as exc:
                    client_errors.append(exc)

            def close_server():
                try:
                    server.shutdown()
                    close_entered.set()
                    server.server_close()
                except BaseException as exc:
                    close_errors.append(exc)
                finally:
                    close_finished.set()

            with mock.patch.object(
                server.broker,
                "_ensure_channel_membership",
            ), mock.patch.object(
                self.runtime,
                "_slack_call",
                return_value={"ok": True, "team_id": "T12345678"},
            ), mock.patch.object(
                self.runtime,
                "slack_post",
                side_effect=blocked_post,
            ):
                client = threading.Thread(target=call_broker)
                client.start()
                self.assertTrue(delivery_entered.wait(timeout=2))
                closer = threading.Thread(target=close_server)
                closer.start()
                self.assertTrue(close_entered.wait(timeout=2))
                close_finished.wait(timeout=0.2)
                singleton_held = self._competing_broker_is_denied(
                    runtime_b,
                    socket_b,
                )
                release_delivery.set()
                client.join(timeout=3)
                closer.join(timeout=3)

            self.assertFalse(client.is_alive())
            self.assertFalse(closer.is_alive())
            self.assertEqual(client_errors, [])
            self.assertEqual(close_errors, [])
            self.assertEqual(len(client_results), 1)
            self.assertTrue(
                singleton_held,
                "server_close released the database singleton while a request "
                "delivery handler was still running",
            )
            self._assert_successor_starts(runtime_b, socket_successor)

    def test_uncertain_reconciliation_never_reposts_after_acceptance_crash(self):
        errors = (
            ("timeout", TimeoutError("synthetic Slack reconciliation timeout")),
            (
                "rate-limit",
                RuntimeError(
                    "synthetic Slack conversations.replies HTTP 429 ratelimited"
                ),
            ),
        )
        for index, (label, reconciliation_error) in enumerate(errors, start=1):
            with self.subTest(reconciliation=label):
                bridge, reply_key, _client_msg_id = self._accepted_then_crashed(
                    self.runtime,
                    self.store,
                    f"accepted-{label}",
                    f"1794000000.{index:06d}",
                )
                self._expire_reply_lease(self.store, reply_key)
                runtime_b = load_runtime(self.home)
                store_b = runtime_b.Store(self.database_path)
                broker_b = runtime_b.Broker("synthetic-token", store_b, verified_workspace_team_id="T12345678")
                slack_methods = []
                with store_b.connect() as database:
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

                def reconcile(_token, method, _payload):
                    slack_methods.append(method)
                    if method == "conversations.replies":
                        raise reconciliation_error
                    raise AssertionError(f"unexpected Slack method: {method}")

                with mock.patch.object(
                    broker_b,
                    "_ensure_channel_membership",
                ), mock.patch.object(
                    runtime_b,
                    "_slack_call",
                    side_effect=reconcile,
                ), mock.patch.object(
                    runtime_b,
                    "slack_post",
                ) as post:
                    self.assertEqual(broker_b.recover_reconciliations(), 0)
                    recovered = broker_b.recover_replies()

                row = self._reply_row(store_b, reply_key)
                self.assertEqual(recovered, 0)
                self.assertGreaterEqual(len(slack_methods), 1)
                self.assertEqual(
                    set(slack_methods),
                    {"conversations.replies"},
                )
                post.assert_not_called()
                self.assertEqual(
                    (
                        row["state"],
                        row["message_ts"],
                        row["lease_id"],
                        row["lease_owner"],
                        row["lease_expires_at"],
                        store_b.attempt_state(reply_key, bridge.bridge_id),
                    ),
                    ("uncertain", None, None, None, None, "replying"),
                )

    def test_reconciliation_finds_accepted_reply_after_page_ten(self):
        bridge, reply_key, client_msg_id = self._accepted_then_crashed(
            self.runtime,
            self.store,
            "accepted-page-eleven",
            "1795000000.000001",
        )
        self._expire_reply_lease(self.store, reply_key)
        runtime_b = self._runtime_epoch()
        store_b = runtime_b.Store(self.database_path)
        broker_b = runtime_b.Broker("synthetic-token", store_b, verified_workspace_team_id="T12345678")
        cursors = []
        accepted_ts = "1795000000.000002"

        def page(_token, method, payload):
            self.assertEqual(method, "conversations.replies")
            cursors.append(str(payload.get("cursor") or ""))
            page_number = len(cursors)
            if page_number == 11:
                return {
                    "messages": [
                        {
                            "ts": accepted_ts,
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
            return {
                "messages": [],
                "response_metadata": {
                    "next_cursor": f"synthetic-cursor-{page_number}"
                },
            }

        with mock.patch.object(
            broker_b,
            "_ensure_channel_membership",
        ), mock.patch.object(
            runtime_b,
            "_slack_call",
            side_effect=page,
        ), mock.patch.object(
            runtime_b,
            "slack_post",
        ) as post:
            recovered_reconciliations = 0
            for _page in range(11):
                with store_b.connect() as database:
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
                recovered_reconciliations += broker_b.recover_reconciliations()
            recovered = broker_b.recover_replies()

        expected_cursors = [""] + [
            f"synthetic-cursor-{page}" for page in range(1, 11)
        ]
        row = self._reply_row(store_b, reply_key)
        self.assertEqual(cursors, expected_cursors)
        post.assert_not_called()
        self.assertEqual(recovered_reconciliations, 1)
        self.assertEqual(recovered, 1)
        self.assertEqual((row["state"], row["message_ts"]), ("sent", accepted_ts))
        self.assertEqual(
            store_b.attempt_state(reply_key, bridge.bridge_id),
            "acknowledged",
        )

    def test_independent_epoch_cannot_steal_unexpired_delivery_lease(self):
        bridge, reply_key = self._stage_reply(
            self.runtime,
            self.store,
            "unexpired-lease",
            "1796000000.000001",
        )
        claimed = self.store.claim_reply(
            reply_key,
            bridge.bridge_id,
            lease_seconds=300,
            lease_owner=self.runtime.PROCESS_EPOCH,
        )
        self.assertEqual(claimed["status"], "claimed")
        original = self._reply_row(self.store, reply_key)

        runtime_b = self._runtime_epoch()
        store_b = runtime_b.Store(self.database_path)
        broker_b = runtime_b.Broker("synthetic-token", store_b, verified_workspace_team_id="T12345678")
        with mock.patch.object(
            runtime_b,
            "_slack_call",
        ) as slack_call, mock.patch.object(
            runtime_b,
            "slack_post",
        ) as post:
            recovered = broker_b.recover_replies()
            competing_claim = store_b.claim_reply(
                reply_key,
                bridge.bridge_id,
                lease_seconds=300,
                lease_owner=runtime_b.PROCESS_EPOCH,
            )

        current = self._reply_row(store_b, reply_key)
        self.assertEqual(recovered, 0)
        self.assertEqual(competing_claim, {"status": "busy"})
        slack_call.assert_not_called()
        post.assert_not_called()
        self.assertEqual(
            (
                current["state"],
                current["lease_id"],
                current["lease_owner"],
                current["lease_expires_at"],
                current["retry_count"],
            ),
            (
                "delivering",
                original["lease_id"],
                original["lease_owner"],
                original["lease_expires_at"],
                1,
            ),
        )


if __name__ == "__main__":
    unittest.main()
