import asyncio
import concurrent.futures
import hashlib
import importlib.util
import json
import pathlib
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock

from test_bridge import PLUGIN_PATH, load_runtime, process_identity


class AttemptRecoveryTest(unittest.TestCase):
    """Fault-injection tests for delivery ownership and recovery."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")
        self.thread_counter = 0

    def tearDown(self):
        self.temp.cleanup()

    def _request(
        self,
        key: str,
        *,
        source_kind: str = "headless_run",
        source: dict[str, str] | None = None,
        channel_id: str = "C12345678",
        owner_user_id: str = "U12345678",
    ) -> dict[str, object]:
        return {
            "source_kind": source_kind,
            "source": source or {"run_id": key, "cwd": "/tmp/project"},
            "owner_user_id": owner_user_id,
            "team_id": "T12345678",
            "channel_id": channel_id,
            "idempotency_key": key,
        }

    def _bound_bridge(self, key: str, **overrides):
        bridge = self.store.create(self._request(key, **overrides))
        self.thread_counter += 1
        return self.store.bind(
            bridge.bridge_id,
            f"1785000000.{self.thread_counter:06d}",
        )

    def _attempt(
        self,
        bridge,
        *,
        event_id: str,
        text: str = "continue",
        awaiting_ack: bool = True,
    ) -> str:
        self.assertTrue(self.store.enqueue_event(event_id, bridge.bridge_id, text))
        items = self.store.claim_event_batch(bridge.bridge_id)
        self.assertEqual([item["event_id"] for item in items], [event_id])
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [event_id],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [event_id],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
            )
        )
        if awaiting_ack:
            self.assertTrue(
                self.store.mark_attempt_awaiting_ack(
                    attempt_id,
                    bridge.bridge_id,
                    bridge.binding_generation,
                )
            )
        return attempt_id

    def _assert_reply_rejected_without_side_effects(
        self,
        *,
        bridge,
        reply_key: str,
        text: str,
        expected_attempt_bridge_id: str | None = None,
    ) -> None:
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        with mock.patch.object(
            broker, "_ensure_channel_membership"
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value="1785000099.000001",
        ) as post:
            with self.assertRaises(
                (ValueError, self.runtime.NativeContinuationError),
                msg="an unattached reply key must be rejected",
            ):
                broker.handle(
                    {
                        "op": "reply",
                        "bridge_id": bridge.bridge_id,
                        "reply_key": reply_key,
                        "text": text,
                    }
                )
        post.assert_not_called()
        with self.store.connect() as database:
            reservation = database.execute(
                "SELECT bridge_id,message_ts FROM bridge_replies WHERE reply_key=?",
                (reply_key,),
            ).fetchone()
        self.assertIsNone(
            reservation,
            "a rejected reply must not reserve or poison its reply key",
        )
        if expected_attempt_bridge_id:
            self.assertEqual(
                self.store.attempt_state(reply_key, expected_attempt_bridge_id),
                "awaiting_ack",
                "a rejected reply must not mutate the legitimate attempt",
            )

    def test_unknown_reply_key_cannot_post_or_suppress(self):
        for text in ("completed", "NO_REPLY"):
            with self.subTest(text=text):
                bridge = self._bound_bridge(f"unknown-{text.lower()}")
                self._assert_reply_rejected_without_side_effects(
                    bridge=bridge,
                    reply_key="att_000000000000000000000001",
                    text=text,
                )

    def test_delivery_health_reports_head_of_line_blocking(self):
        bridge = self._bound_bridge("delivery-health")
        attempt_id = self._attempt(
            bridge,
            event_id="1785000000.000901",
            awaiting_ack=False,
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_uncertain(
                attempt_id,
                bridge.bridge_id,
                "terminal_submit_uncertain",
            )
        )
        # Schema 16 preserves older active records whose endpoint identity
        # cannot be reconstructed. Their own blocked queue must remain visible.
        with self.store.connect() as database:
            database.execute(
                "UPDATE bridges SET endpoint_key='' WHERE bridge_id=?",
                (bridge.bridge_id,),
            )
        self.assertTrue(
            self.store.enqueue_event(
                "1785000000.000902",
                bridge.bridge_id,
                "later follow-up",
            )
        )

        self.assertEqual(
            self.store.delivery_health(),
            {
                "queued_delivery_count": 1,
                "uncertain_delivery_count": 1,
                "blocked_bridge_count": 1,
            },
        )

    def test_shared_endpoint_serializes_threads_and_wakes_next(self):
        shared_source = {"run_id": "shared-native-session", "cwd": "/tmp/project"}
        first = self._bound_bridge(
            "shared-thread-first",
            source=shared_source,
        )
        second = self._bound_bridge(
            "shared-thread-second",
            source=shared_source,
            channel_id="C87654321",
        )
        first_attempt = self._attempt(
            first,
            event_id="1785000000.000911",
            awaiting_ack=True,
        )
        self.assertTrue(
            self.store.enqueue_event(
                "1785000000.000912",
                second.bridge_id,
                "independent thread follow-up",
            )
        )
        second_items = self.store.claim_event_batch(second.bridge_id)
        second_attempt = self.runtime.delivery_attempt_id(
            second.bridge_id,
            [item["event_id"] for item in second_items],
            second.binding_generation,
        )

        with self.assertRaises(self.runtime.NativeContinuationError) as raised:
            self.store.prepare_delivery_attempt(
                [item["event_id"] for item in second_items],
                second.bridge_id,
                second.binding_generation,
                second_attempt,
                delivery_kind="detached_native",
            )

        self.assertEqual(raised.exception.code, "endpoint_busy")
        self.assertEqual(
            self.store.endpoint_queued_bridge_ids(first.bridge_id),
            [second.bridge_id],
        )
        self.assertEqual(
            self.store.delivery_health()["blocked_bridge_count"],
            1,
        )
        self.assertEqual(
            self.store.acknowledge_attempt(
                first_attempt,
                first.bridge_id,
                ack_kind="no_reply",
            ),
            1,
        )
        retried_items = self.store.claim_event_batch(second.bridge_id)
        self.assertEqual(
            [item["event_id"] for item in retried_items],
            ["1785000000.000912"],
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                ["1785000000.000912"],
                second.bridge_id,
                second.binding_generation,
                second_attempt,
                delivery_kind="detached_native",
            )
        )

    def test_concurrent_shared_endpoint_prepares_exactly_one_turn(self):
        shared_source = {"run_id": "concurrent-native-session", "cwd": "/tmp/project"}
        bridges = [
            self._bound_bridge(
                f"concurrent-thread-{index}",
                source=shared_source,
                channel_id=("C12345678" if index == 1 else "C87654321"),
            )
            for index in (1, 2)
        ]
        batches = []
        for index, bridge in enumerate(bridges, start=1):
            event_id = f"1785000000.00092{index}"
            self.assertTrue(
                self.store.enqueue_event(event_id, bridge.bridge_id, "follow-up")
            )
            self.store.claim_event_batch(bridge.bridge_id)
            attempt_id = self.runtime.delivery_attempt_id(
                bridge.bridge_id,
                [event_id],
                bridge.binding_generation,
            )
            batches.append((bridge, event_id, attempt_id))
        barrier = threading.Barrier(2)

        def prepare(batch):
            bridge, event_id, attempt_id = batch
            barrier.wait()
            try:
                prepared = self.store.prepare_delivery_attempt(
                    [event_id],
                    bridge.bridge_id,
                    bridge.binding_generation,
                    attempt_id,
                    delivery_kind="detached_native",
                )
                return "prepared" if prepared else "rejected"
            except self.runtime.NativeContinuationError as exc:
                return exc.code

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(prepare, batches))

        self.assertEqual(sorted(outcomes), ["endpoint_busy", "prepared"])
        with self.store.connect() as database:
            states = dict(
                database.execute(
                    "SELECT state,count(*) FROM bridge_events "
                    "WHERE bridge_id IN (?,?) GROUP BY state",
                    (bridges[0].bridge_id, bridges[1].bridge_id),
                ).fetchall()
            )
        self.assertEqual(states, {"prepared": 1, "queued": 1})

    def test_wrong_bridge_reply_key_cannot_post_or_suppress(self):
        for index, text in enumerate(("completed", "NO_REPLY"), start=1):
            with self.subTest(text=text):
                legitimate = self._bound_bridge(f"legitimate-{index}")
                wrong = self._bound_bridge(f"wrong-{index}")
                reply_key = self._attempt(
                    legitimate,
                    event_id=f"1785000001.{index:06d}",
                )
                self._assert_reply_rejected_without_side_effects(
                    bridge=wrong,
                    reply_key=reply_key,
                    text=text,
                    expected_attempt_bridge_id=legitimate.bridge_id,
                )

    def test_stale_generation_reply_key_cannot_post_or_suppress(self):
        for index, text in enumerate(("completed", "NO_REPLY"), start=1):
            with self.subTest(text=text):
                bridge = self._bound_bridge(f"stale-{index}")
                reply_key = self._attempt(
                    bridge,
                    event_id=f"1785000002.{index:06d}",
                )
                with self.store.connect() as database:
                    database.execute(
                        """
                        UPDATE bridges
                        SET binding_generation=binding_generation+1
                        WHERE bridge_id=?
                        """,
                        (bridge.bridge_id,),
                    )
                current = self.store.get(bridge.bridge_id)
                self.assertIsNotNone(current)
                self._assert_reply_rejected_without_side_effects(
                    bridge=current,
                    reply_key=reply_key,
                    text=text,
                    expected_attempt_bridge_id=bridge.bridge_id,
                )

    def test_valid_reply_key_posts_once_and_acknowledges_exact_attempt(self):
        bridge = self._bound_bridge("valid-reply")
        reply_key = self._attempt(bridge, event_id="1785000003.000001")
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        with mock.patch.object(
            broker, "_ensure_channel_membership"
        ), mock.patch.object(
            self.runtime, "slack_post", return_value="1785000003.000002"
        ) as post:
            result = broker.handle(
                {
                    "op": "reply",
                    "bridge_id": bridge.bridge_id,
                    "reply_key": reply_key,
                    "text": "completed",
                }
            )
        post.assert_called_once()
        self.assertEqual(result["acknowledged_events"], 1)
        self.assertEqual(
            self.store.attempt_state(reply_key, bridge.bridge_id),
            "acknowledged",
        )

    def test_concurrent_identical_replies_post_once(self):
        bridge = self._bound_bridge("concurrent-reply")
        reply_key = self._attempt(bridge, event_id="1785000003.000010")
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        first_post_entered = threading.Event()
        release_first_post = threading.Event()
        post_count = 0
        post_count_lock = threading.Lock()

        def post(_token, _channel, _text, _thread_ts, *, client_msg_id=None):
            nonlocal post_count
            with post_count_lock:
                post_count += 1
                current = post_count
            if current == 1:
                first_post_entered.set()
                self.assertTrue(release_first_post.wait(timeout=5))
            return "1785000003.000011"

        request = {
            "op": "reply",
            "bridge_id": bridge.bridge_id,
            "reply_key": reply_key,
            "text": "completed",
        }
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            broker,
            "_find_staged_reply",
            return_value="",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=post,
        ):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(broker.handle, request)
                self.assertTrue(first_post_entered.wait(timeout=5))
                second = executor.submit(broker.handle, request)
                threading.Event().wait(0.1)
                release_first_post.set()
                results = (first.result(timeout=5), second.result(timeout=5))

        self.assertEqual(post_count, 1)
        self.assertEqual(
            sorted(result["deduplicated"] for result in results),
            [False, True],
        )
        self.assertEqual(
            self.store.attempt_state(reply_key, bridge.bridge_id),
            "acknowledged",
        )

    def test_uncertain_slack_post_retries_immutable_payload_with_same_client_id(self):
        bridge = self._bound_bridge("slack-uncertain")
        reply_key = self._attempt(
            bridge,
            event_id="1785000003.000003",
        )
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        client_ids: list[str] = []

        def post(_token, _channel, _text, _thread_ts, *, client_msg_id=None):
            client_ids.append(client_msg_id)
            if len(client_ids) == 1:
                raise TimeoutError("response lost after remote acceptance")
            return "1785000003.000004"

        request = {
            "op": "reply",
            "bridge_id": bridge.bridge_id,
            "reply_key": reply_key,
            "text": "completed",
        }
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            broker,
            "_find_staged_reply",
            return_value="",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=post,
        ):
            with self.assertRaises(TimeoutError):
                broker.handle(request)
            result = broker.handle(request)

        self.assertEqual(len(client_ids), 2)
        self.assertTrue(client_ids[0])
        self.assertEqual(client_ids[0], client_ids[1])
        self.assertEqual(result["acknowledged_events"], 1)
        self.assertEqual(
            self.store.attempt_state(reply_key, bridge.bridge_id),
            "acknowledged",
        )

    def test_reserved_reply_rejects_changed_retry_content(self):
        bridge = self._bound_bridge("slack-content-conflict")
        reply_key = self._attempt(
            bridge,
            event_id="1785000003.000005",
        )
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            broker,
            "_find_staged_reply",
            return_value="",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=TimeoutError("ambiguous delivery"),
        ):
            with self.assertRaises(TimeoutError):
                broker.handle({
                    "op": "reply",
                    "bridge_id": bridge.bridge_id,
                    "reply_key": reply_key,
                    "text": "first answer",
                })
            with self.assertRaisesRegex(ValueError, "different content"):
                broker.handle({
                    "op": "reply",
                    "bridge_id": bridge.bridge_id,
                    "reply_key": reply_key,
                    "text": "different answer",
                })

    def test_wake_callback_failure_cannot_reverse_a_successful_reply(self):
        bridge = self._bound_bridge("wake-failure")
        reply_key = self._attempt(
            bridge,
            event_id="1785000003.000006",
        )
        broker = self.runtime.Broker(
            "test-token",
            self.store,
            attempt_closed=mock.Mock(side_effect=RuntimeError("loop closed")),
            verified_workspace_team_id="T12345678",
        )
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value="1785000003.000007",
        ):
            result = broker.handle({
                "op": "reply",
                "bridge_id": bridge.bridge_id,
                "reply_key": reply_key,
                "text": "completed",
            })
        self.assertEqual(result["acknowledged_events"], 1)
        self.assertEqual(
            self.store.attempt_state(reply_key, bridge.bridge_id),
            "acknowledged",
        )

    def test_acknowledgment_unblocks_queued_followup(self):
        bridge = self._bound_bridge("followup")
        reply_key = self._attempt(bridge, event_id="1785000004.000001")
        self.assertTrue(
            self.store.enqueue_event(
                "1785000004.000002",
                bridge.bridge_id,
                "second request",
            )
        )
        self.assertIsNone(self.store.claim_next_event(bridge.bridge_id))
        self.assertEqual(
            self.store.acknowledge_attempt(
                reply_key,
                bridge.bridge_id,
                ack_kind="no_reply",
            ),
            1,
        )
        followup = self.store.claim_next_event(bridge.bridge_id)
        self.assertIsNotNone(
            followup,
            "acknowledgment must make the next queued event claimable",
        )
        self.assertEqual(followup["event_id"], "1785000004.000002")

    def _insert_attempt_state(self, state: str, index: int):
        bridge = self._bound_bridge(f"restart-{state}-{index}")
        event_id = f"1785000010.{index:06d}"
        self.assertTrue(self.store.enqueue_event(event_id, bridge.bridge_id, state))
        self.assertEqual(
            self.store.claim_next_event(bridge.bridge_id)["event_id"],
            event_id,
        )
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [event_id],
            bridge.binding_generation,
        )
        with self.store.connect() as database:
            database.execute(
                """
                INSERT INTO bridge_attempts(
                  attempt_id,reply_key,bridge_id,binding_generation,
                  delivery_kind,state,submitted_at
                ) VALUES(?,?,?,?,?,?,CURRENT_TIMESTAMP)
                """,
                (
                    attempt_id,
                    attempt_id,
                    bridge.bridge_id,
                    bridge.binding_generation,
                    "zellij",
                    state,
                ),
            )
            database.execute(
                """
                UPDATE bridge_events
                SET state=?,attempt_id=?,binding_generation=?
                WHERE event_id=?
                """,
                (state, attempt_id, bridge.binding_generation, event_id),
            )
        return bridge, event_id, attempt_id

    def test_restart_recovers_pre_io_and_preserves_ambiguous_attempts(self):
        fixtures = {
            state: self._insert_attempt_state(state, index)
            for index, state in enumerate(
                ("prepared", "submitting", "awaiting_ack"),
                start=1,
            )
        }

        restarted = self.runtime.Store(self.store.path)

        prepared_bridge, prepared_event, _ = fixtures["prepared"]
        with self.subTest(state="prepared"):
            claimed = restarted.claim_next_event(prepared_bridge.bridge_id)
            self.assertIsNotNone(
                claimed,
                "prepared means no external I/O occurred and must be safely replayable",
            )
            if claimed is not None:
                self.assertEqual(claimed["event_id"], prepared_event)

        submitting_bridge, _, submitting_attempt = fixtures["submitting"]
        with self.subTest(state="submitting"):
            self.assertEqual(
                restarted.attempt_state(
                    submitting_attempt,
                    submitting_bridge.bridge_id,
                ),
                "uncertain",
                "a restart during submission must preserve ambiguity, not replay or fail",
            )
            self.assertIsNone(
                restarted.claim_next_event(submitting_bridge.bridge_id)
            )

        awaiting_bridge, _, awaiting_attempt = fixtures["awaiting_ack"]
        with self.subTest(state="awaiting_ack"):
            self.assertEqual(
                restarted.attempt_state(
                    awaiting_attempt,
                    awaiting_bridge.bridge_id,
                ),
                "awaiting_ack",
            )
            self.assertIsNone(
                restarted.claim_next_event(awaiting_bridge.bridge_id)
            )

    def test_requeued_pre_io_attempt_can_be_prepared_again(self):
        bridge, event_id, attempt_id = self._insert_attempt_state(
            "prepared",
            10,
        )
        restarted = self.runtime.Store(self.store.path)
        item = restarted.claim_next_event(bridge.bridge_id)
        self.assertIsNotNone(item)
        self.assertEqual(item["event_id"], event_id)
        self.assertTrue(
            restarted.prepare_delivery_attempt(
                [event_id],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
            ),
            "a deterministic pre-I/O attempt ID must be reusable after safe recovery",
        )
        self.assertEqual(
            restarted.attempt_state(attempt_id, bridge.bridge_id),
            "prepared",
        )

    def test_stale_awaiting_ack_remains_blocked_until_reconciled(self):
        bridge, event_id, attempt_id = self._insert_attempt_state(
            "awaiting_ack",
            20,
        )
        self.assertTrue(
            self.store.enqueue_event(
                "1785000020.000002",
                bridge.bridge_id,
                "new follow-up",
            )
        )
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE bridge_attempts
                SET updated_at=datetime('now','-2 days')
                WHERE attempt_id=?
                """,
                (attempt_id,),
            )
            database.execute(
                """
                UPDATE bridge_events
                SET updated_at=datetime('now','-2 days')
                WHERE event_id=?
                """,
                (event_id,),
            )

        restarted = self.runtime.Store(self.store.path)
        self.assertEqual(
            restarted.attempt_state(attempt_id, bridge.bridge_id),
            "awaiting_ack",
            "time alone cannot prove whether a submitted pane task ran",
        )
        with restarted.connect() as database:
            attempt = database.execute(
                "SELECT error_code FROM bridge_attempts WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()
        self.assertIsNone(attempt["error_code"])
        self.assertIsNone(
            restarted.claim_next_event(bridge.bridge_id),
            "later work must stay blocked until the ambiguous attempt is reconciled",
        )

    def test_rebind_is_blocked_for_zellij_in_flight_delivery(self):
        bridge = self._bound_bridge("zellij-rebind")
        self._attempt(bridge, event_id="1785000030.000001")
        with self.assertRaisesRegex(ValueError, "active delivery"):
            self.store.rebind(
                bridge.bridge_id,
                "headless_run",
                {"run_id": "replacement", "cwd": "/tmp/project"},
                expected_generation=bridge.binding_generation,
            )

    def test_idempotency_collision_rejects_changed_source_or_destination(self):
        original = self._request("same-idempotency-key")
        self.store.create(original)
        variants = {
            "source": {
                **original,
                "source": {"run_id": "different-run", "cwd": "/tmp/project"},
            },
            "channel": {**original, "channel_id": "C87654321"},
            "owner": {**original, "owner_user_id": "U87654321"},
            "workspace": {**original, "team_id": "T87654321"},
        }
        for field, request in variants.items():
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "idempotency"):
                    self.store.create(request)


class PluginAttemptRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.previous_bridge_runtime = sys.modules.get("bridge_runtime")
        sys.modules["bridge_runtime"] = self.runtime
        spec = importlib.util.spec_from_file_location(
            f"attempt_recovery_plugin_{id(self)}",
            PLUGIN_PATH,
        )
        self.plugin = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.plugin
        spec.loader.exec_module(self.plugin)
        self.plugin.store = self.runtime.Store()
        self.plugin.state.store = self.plugin.store
        self.plugin.state.ready = True
        self.plugin_module_name = spec.name
        self.thread_counter = 0

    def tearDown(self):
        sys.modules.pop(self.plugin_module_name, None)
        if self.previous_bridge_runtime is None:
            sys.modules.pop("bridge_runtime", None)
        else:
            sys.modules["bridge_runtime"] = self.previous_bridge_runtime
        self.temp.cleanup()

    def _bridge(self, key: str, source_kind: str, source: dict[str, str]):
        bridge = self.plugin.store.create(
            {
                "source_kind": source_kind,
                "source": source,
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "idempotency_key": key,
            }
        )
        self.thread_counter += 1
        return self.plugin.store.bind(
            bridge.bridge_id,
            f"1785000100.{self.thread_counter:06d}",
        )

    def _zellij_bridge(self, key: str):
        pane = str(51 + self.thread_counter)
        identity = process_identity(
            agent="codex",
            session="didactic-jellyfish",
            pane=pane,
        )
        return self._bridge(
            key,
            "codex_session",
            {
                "session_id": f"{key}-session",
                "cwd": "/tmp/project",
                "zellij_session": "didactic-jellyfish",
                "zellij_pane_id": pane,
                "pane_agent": "codex",
                "pane_command_hash": "expected",
                "process_identity": identity,
            },
        )

    def _herdr_bridge(self, key: str):
        terminal_id = "term_6583153c2a1b81"
        identity = "herdr-proc-v1:" + json.dumps(
            {
                "agent": "codex",
                "boot": "00000000-0000-4000-8000-000000000001",
                "exe": "1:2",
                "exe_path": hashlib.sha256(
                    b"/opt/codex/bin/codex"
                ).hexdigest()[:16],
                "pid": 200,
                "start": "20000",
                "terminal": terminal_id,
                "tty": "34823",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        session_id = f"{key}-session"
        return self._bridge(
            key,
            "codex_session",
            {
                "session_id": session_id,
                "cwd": "/tmp/project",
                "pane_agent": "codex",
                "process_identity": identity,
                "herdr_session": "pilot",
                "herdr_socket_path": "/tmp/herdr-pilot.sock",
                "herdr_terminal_id": terminal_id,
                "herdr_pane_id": "w1:p1",
                "herdr_agent_name": "tether_0123456789abcdef",
                "herdr_agent_session_source": "codex_notify",
                "herdr_agent_session_kind": "thread_id",
                "herdr_agent_session_value": session_id,
                "herdr_protocol": "19",
            },
        )

    def _claimed_item(self, bridge, event_id: str):
        self.assertTrue(
            self.plugin.store.enqueue_event(
                event_id,
                bridge.bridge_id,
                "continue",
            )
        )
        items = self.plugin.store.claim_event_batch(bridge.bridge_id)
        self.assertEqual([item["event_id"] for item in items], [event_id])
        return items

    def test_attempt_close_schedules_queued_sibling_thread(self):
        source = {"run_id": "shared-plugin-session", "cwd": "/tmp/project"}
        first = self._bridge("shared-plugin-first", "headless_run", source)
        second = self._bridge("shared-plugin-second", "headless_run", source)
        self.assertTrue(
            self.plugin.store.enqueue_event(
                "1785000100.000050",
                second.bridge_id,
                "second thread",
            )
        )

        with mock.patch.object(
            self.plugin,
            "_schedule_one_bridge_drain",
            return_value=True,
        ) as schedule:
            self.plugin._schedule_bridge_drain(first.bridge_id)

        schedule.assert_called_once_with(second.bridge_id)

    def test_bound_zellij_cancel_interrupts_and_closes_exact_attempt(self):
        bridge = self._zellij_bridge("operator-cancel")
        event_id = "1785000100.000099"
        self._claimed_item(bridge, event_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [event_id],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.plugin.store.prepare_delivery_attempt(
                [event_id],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
            )
        )
        self.assertTrue(
            self.plugin.store.mark_attempt_awaiting_ack(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        with mock.patch.object(
            self.plugin,
            "interrupt_zellij",
        ) as interrupt:
            cancelled = self.plugin._interrupt_active_zellij_attempt(bridge)
        interrupt.assert_called_once_with(bridge)
        self.assertEqual(cancelled, 1)
        self.assertEqual(
            self.plugin.store.attempt_state(
                attempt_id,
                bridge.bridge_id,
            ),
            "cancelled",
        )
        with self.plugin.store.connect() as database:
            state = database.execute(
                "SELECT state FROM bridge_events WHERE event_id=?",
                (event_id,),
            ).fetchone()[0]
        self.assertEqual(state, "failed")

    def test_bound_herdr_submission_uses_herdr_attempt_ledger(self):
        bridge = self._herdr_bridge("herdr-submit")
        event_id = "1785000100.000199"
        items = self._claimed_item(bridge, event_id)
        with mock.patch.object(
            self.plugin,
            "deliver_herdr",
            return_value="att_marker",
        ) as deliver, mock.patch.object(
            self.plugin,
            "deliver_zellij",
        ) as zellij:
            attempt_id = self.plugin._submit_live_attempt(
                bridge,
                items,
                "continue",
            )
        deliver.assert_called_once_with(bridge, "continue", attempt_id)
        zellij.assert_not_called()
        active = self.plugin.store.active_live_attempt(
            bridge.bridge_id,
            "herdr",
        )
        self.assertEqual(active["attempt_id"], attempt_id)
        self.assertEqual(active["delivery_kind"], "herdr")
        with self.plugin.store.connect() as database:
            delivery_kind = database.execute(
                "SELECT delivery_kind FROM bridge_attempts WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()[0]
        self.assertEqual(delivery_kind, "herdr")

    def test_restart_requeues_legacy_safe_herdr_preflight_failure(self):
        bridge = self._herdr_bridge("herdr-safe-preflight")
        event_id = "1785000100.000249"
        items = self._claimed_item(bridge, event_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [item["event_id"] for item in items],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.plugin.store.prepare_delivery_attempt(
                [event_id],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="herdr",
            )
        )
        self.assertTrue(
            self.plugin.store.mark_attempt_submitting(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        self.assertTrue(
            self.plugin.store.mark_attempt_uncertain(
                attempt_id,
                bridge.bridge_id,
                "native_continuation_failed",
            )
        )

        restarted = self.runtime.Store(self.plugin.store.path)
        claimed = restarted.claim_event_batch(bridge.bridge_id)

        self.assertEqual([item["event_id"] for item in claimed], [event_id])
        self.assertEqual(
            restarted.attempt_state(attempt_id, bridge.bridge_id),
            "requeued",
        )

    def test_bound_herdr_cancel_interrupts_and_closes_exact_attempt(self):
        bridge = self._herdr_bridge("herdr-cancel")
        event_id = "1785000100.000299"
        items = self._claimed_item(bridge, event_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [item["event_id"] for item in items],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.plugin.store.prepare_delivery_attempt(
                [event_id],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="herdr",
            )
        )
        self.assertTrue(
            self.plugin.store.mark_attempt_awaiting_ack(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        with mock.patch.object(
            self.plugin,
            "interrupt_herdr",
        ) as interrupt:
            cancelled = self.plugin._interrupt_active_live_attempt(bridge)
        interrupt.assert_called_once_with(bridge)
        self.assertEqual(cancelled, 1)
        self.assertIsNone(
            self.plugin.store.active_live_attempt(bridge.bridge_id, "herdr")
        )
        self.assertEqual(
            self.plugin.store.attempt_state(attempt_id, bridge.bridge_id),
            "cancelled",
        )

    def test_crash_before_zellij_injection_is_recoverable(self):
        bridge = self._zellij_bridge("pre-injection")
        event_id = "1785000101.000001"
        items = self._claimed_item(bridge, event_id)
        error = self.runtime.NativeContinuationError(
            "terminal write did not start",
            code="terminal_submit_not_started",
        )
        with mock.patch.object(
            self.plugin,
            "deliver_zellij",
            side_effect=error,
        ), self.assertRaises(self.runtime.NativeContinuationError):
            self.plugin._submit_zellij_attempt(
                bridge,
                items,
                "continue",
            )

        restarted = self.runtime.Store(self.plugin.store.path)
        claimed = restarted.claim_next_event(bridge.bridge_id)
        self.assertIsNotNone(
            claimed,
            "a confirmed pre-I/O crash must be requeued instead of failed",
        )
        self.assertEqual(claimed["event_id"], event_id)

    def test_post_enter_verification_failure_stays_uncertain(self):
        bridge = self._zellij_bridge("post-enter")
        event_id = "1785000102.000001"
        items = self._claimed_item(bridge, event_id)
        error = self.runtime.NativeContinuationError(
            "Enter was sent but verification was interrupted",
            code="terminal_submit_uncertain",
        )
        with mock.patch.object(
            self.plugin,
            "deliver_zellij",
            side_effect=error,
        ), self.assertRaises(self.runtime.NativeContinuationError):
            self.plugin._submit_zellij_attempt(
                bridge,
                items,
                "continue",
            )

        with self.plugin.store.connect() as database:
            event = database.execute(
                """
                SELECT state,error,attempt_id
                FROM bridge_events WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            attempt = database.execute(
                """
                SELECT state,error_code
                FROM bridge_attempts WHERE attempt_id=?
                """,
                (event["attempt_id"],),
            ).fetchone()
        self.assertIn(event["state"], {"submitting", "uncertain", "awaiting_ack"})
        self.assertNotEqual(event["state"], "failed")
        self.assertIn(attempt["state"], {"submitting", "uncertain", "awaiting_ack"})
        self.assertNotEqual(attempt["state"], "failed")

    def test_drain_does_not_overwrite_requeued_or_uncertain_attempts(self):
        class Platform:
            value = "slack"

        platform = Platform()
        gateway = types.SimpleNamespace(
            adapters={platform: types.SimpleNamespace()}
        )
        cases = (
            ("pre-io-drain", "terminal_submit_not_started", "queued", "requeued"),
            ("post-enter-drain", "terminal_submit_uncertain", "uncertain", "uncertain"),
        )
        for index, (key, code, event_state, attempt_state) in enumerate(
            cases,
            start=1,
        ):
            with self.subTest(code=code):
                bridge = self._zellij_bridge(key)
                event_id = f"1785000104.{index:06d}"
                self.assertTrue(
                    self.plugin.store.enqueue_event(
                        event_id,
                        bridge.bridge_id,
                        "continue",
                    )
                )
                error = self.runtime.NativeContinuationError(
                    "fault injection",
                    code=code,
                )
                with mock.patch.object(
                    self.plugin,
                    "deliver_zellij",
                    side_effect=error,
                ):
                    asyncio.run(
                        self.plugin._drain_bridge(
                            bridge.bridge_id,
                            gateway,
                            platform,
                        )
                    )
                attempt_id = self.runtime.delivery_attempt_id(
                    bridge.bridge_id,
                    [event_id],
                    bridge.binding_generation,
                )
                with self.plugin.store.connect() as database:
                    persisted_event = database.execute(
                        "SELECT state FROM bridge_events WHERE event_id=?",
                        (event_id,),
                    ).fetchone()
                self.assertEqual(persisted_event["state"], event_state)
                self.assertEqual(
                    self.plugin.store.attempt_state(
                        attempt_id,
                        bridge.bridge_id,
                    ),
                    attempt_state,
                )

    def test_failed_native_drain_never_posts_failure_noise_to_slack(self):
        bridge = self._zellij_bridge("silent-failure")
        event_id = "1785000104.900001"
        self.assertTrue(
            self.plugin.store.enqueue_event(
                event_id,
                bridge.bridge_id,
                "continue",
            )
        )

        class Platform:
            value = "slack"

        class Adapter:
            def __init__(self):
                self.sent = []

            async def send(self, *args, **kwargs):
                self.sent.append((args, kwargs))

        platform = Platform()
        adapter = Adapter()
        gateway = types.SimpleNamespace(adapters={platform: adapter})
        error = self.runtime.NativeContinuationError(
            "captured pane is unavailable",
            code="process_identity_changed",
        )

        with mock.patch.object(
            self.plugin,
            "deliver_zellij",
            side_effect=error,
        ):
            asyncio.run(
                self.plugin._drain_bridge(
                    bridge.bridge_id,
                    gateway,
                    platform,
                )
            )

        self.assertEqual(adapter.sent, [])

    def test_rebind_is_blocked_while_detached_continuation_runs(self):
        bridge = self._bridge(
            "detached-rebind",
            "claude_session",
            {"session_id": "claude-1", "cwd": "/tmp/project"},
        )
        self.assertTrue(
            self.plugin.store.enqueue_event(
                "1785000103.000001",
                bridge.bridge_id,
                "continue",
            )
        )
        entered = threading.Event()
        release = threading.Event()
        errors: list[BaseException] = []

        def continue_native(*_args, **_kwargs):
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("test continuation was not released")
            _args[3]("NO_REPLY")
            return "NO_REPLY"

        class Platform:
            value = "slack"

        class Adapter:
            async def send(self, *_args, **_kwargs):
                return None

        platform = Platform()
        gateway = types.SimpleNamespace(adapters={platform: Adapter()})

        def drain():
            try:
                asyncio.run(
                    self.plugin._drain_bridge(
                        bridge.bridge_id,
                        gateway,
                        platform,
                    )
                )
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=drain, daemon=True)
        with mock.patch.object(
            self.plugin,
            "continue_native",
            side_effect=continue_native,
        ):
            worker.start()
            self.assertTrue(
                entered.wait(timeout=5),
                "detached continuation did not start",
            )
            try:
                with self.assertRaisesRegex(ValueError, "active delivery"):
                    self.plugin.store.rebind(
                        bridge.bridge_id,
                        "claude_session",
                        {"session_id": "claude-2", "cwd": "/tmp/project"},
                        expected_generation=bridge.binding_generation,
                    )
            finally:
                release.set()
                worker.join(timeout=5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
