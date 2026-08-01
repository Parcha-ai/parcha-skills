import concurrent.futures
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from test_bridge import load_runtime


TEAM = "T12345678"
CHANNEL = "C12345678"
THREAD = "1789000000.000001"


class GenericOutboxTest(unittest.TestCase):
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

    @staticmethod
    def request(key="post-progress", text="Progress verified."):
        return {
            "op": "thread_reply",
            "team_id": TEAM,
            "channel_id": CHANNEL,
            "thread_ts": THREAD,
            "text": text,
            "idempotency_key": key,
        }

    def restart(self):
        runtime = load_runtime(self.home)
        store = runtime.Store(self.database_path)
        broker = runtime.Broker(
            "test-token",
            store,
            verified_workspace_team_id=TEAM,
        )
        return runtime, store, broker

    def test_same_key_and_payload_posts_once(self):
        with mock.patch.object(
            self.broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value="1789000001.000001",
        ) as post:
            first = self.broker.handle(self.request())
            second = self.broker.handle(self.request())

        self.assertEqual(first["message_ts"], "1789000001.000001")
        self.assertEqual(second["message_ts"], "1789000001.000001")
        self.assertTrue(second["deduplicated"])
        post.assert_called_once()
        kwargs = post.call_args.kwargs
        self.assertTrue(kwargs["client_msg_id"])
        self.assertEqual(kwargs["metadata_event_type"], "tether_message")
        self.assertEqual(
            kwargs["metadata_event_payload"],
            {"client_msg_id": kwargs["client_msg_id"]},
        )
        self.assertTrue(self.store.participates(TEAM, CHANNEL, THREAD))

    def test_same_key_rejects_changed_payload_or_destination(self):
        self.store.reserve_message(
            "immutable-message",
            TEAM,
            CHANNEL,
            THREAD,
            "Original",
        )
        with self.assertRaisesRegex(ValueError, "different Slack message"):
            self.store.reserve_message(
                "immutable-message",
                TEAM,
                CHANNEL,
                THREAD,
                "Changed",
            )
        with self.assertRaisesRegex(ValueError, "different Slack message"):
            self.store.reserve_message(
                "immutable-message",
                TEAM,
                "C87654321",
                THREAD,
                "Original",
            )

    def test_message_options_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "options must be an object"):
            self.store.reserve_message(
                "invalid-options",
                TEAM,
                CHANNEL,
                THREAD,
                "Original",
                options=["mrkdwn"],
            )
        with self.assertRaisesRegex(ValueError, "options must be an object"):
            self.store.reserve_message_group(
                "hsg_" + "f" * 32,
                TEAM,
                CHANNEL,
                THREAD,
                [{"text": "Original", "options": ["mrkdwn"]}],
            )

    def test_message_options_redact_nested_secrets_before_persistence(self):
        raw_secret = "sk-proj-" + "a" * 32
        self.store.reserve_message(
            "redacted-blocks",
            TEAM,
            CHANNEL,
            THREAD,
            "Safe fallback",
            options={
                "mrkdwn": True,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"provider={raw_secret}",
                        },
                        "api_key": "unprefixed-sensitive-value",
                    }
                ],
            },
        )

        with self.store.connect() as database:
            row = database.execute(
                """
                SELECT payload_options_json FROM slack_messages
                WHERE idempotency_key='redacted-blocks'
                """
            ).fetchone()
        serialized = str(row["payload_options_json"])
        options = json.loads(serialized)
        self.assertNotIn(raw_secret, serialized)
        self.assertNotIn("unprefixed-sensitive-value", serialized)
        self.assertEqual(
            options["blocks"][0]["text"]["text"],
            "provider=[REDACTED_PROVIDER_KEY]",
        )
        self.assertEqual(options["blocks"][0]["api_key"], "[REDACTED]")

    def test_message_options_reject_cycles_and_nonfinite_numbers(self):
        cyclic_blocks = []
        cyclic_blocks.append(cyclic_blocks)
        with self.assertRaisesRegex(ValueError, "valid bounded JSON"):
            self.store.reserve_message(
                "cyclic-blocks",
                TEAM,
                CHANNEL,
                THREAD,
                "Safe fallback",
                options={"blocks": cyclic_blocks},
            )
        with self.assertRaisesRegex(ValueError, "valid bounded JSON"):
            self.store.reserve_message(
                "nonfinite-blocks",
                TEAM,
                CHANNEL,
                THREAD,
                "Safe fallback",
                options={
                    "blocks": [
                        {"type": "section", "unsafe_number": float("nan")}
                    ]
                },
            )

    def test_expired_same_process_message_lease_is_recoverable(self):
        self.store.reserve_message(
            "expired-same-owner",
            TEAM,
            CHANNEL,
            THREAD,
            "Recover the delivery",
        )
        first = self.store.claim_message(
            "expired-same-owner",
            lease_owner=self.broker.lease_owner,
        )
        self.assertEqual(first["status"], "claimed")
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE slack_messages
                SET lease_expires_at=datetime('now','-1 second')
                WHERE idempotency_key='expired-same-owner'
                """
            )

        second = self.store.claim_message(
            "expired-same-owner",
            lease_owner=self.broker.lease_owner,
        )

        self.assertEqual(second["status"], "claimed")
        self.assertEqual(second["previous_state"], "delivering")
        self.assertNotEqual(second["lease_id"], first["lease_id"])

    def test_cancelled_message_cannot_be_reclaimed(self):
        self.store.reserve_message(
            "cancelled-message",
            TEAM,
            CHANNEL,
            THREAD,
            "Do not deliver",
        )
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE slack_messages SET state='cancelled'
                WHERE idempotency_key='cancelled-message'
                """
            )

        claimed = self.store.claim_message(
            "cancelled-message",
            lease_owner=self.broker.lease_owner,
        )

        self.assertEqual(
            claimed,
            {
                "status": "terminal",
                "state": "cancelled",
                "thread_ts": THREAD,
            },
        )

    def test_concurrent_delivery_has_one_network_writer(self):
        posts = []

        def post(*_args, **_kwargs):
            posts.append(1)
            return "1789000002.000001"

        with mock.patch.object(
            self.broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=post,
        ):
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                results = list(
                    pool.map(
                        lambda _index: self.broker.handle(
                            self.request("concurrent-message")
                        ),
                        range(8),
                    )
                )

        self.assertEqual(posts, [1])
        self.assertEqual(
            {result["message_ts"] for result in results},
            {"1789000002.000001"},
        )

    def test_pending_message_survives_restart(self):
        self.store.reserve_message(
            "pending-restart",
            TEAM,
            CHANNEL,
            THREAD,
            "Resume after restart",
        )
        runtime, _store, broker = self.restart()
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            runtime,
            "slack_post",
            return_value="1789000003.000001",
        ) as post:
            self.assertEqual(broker.recover_messages(), 1)
        post.assert_called_once()

    def test_invalid_blocks_falls_back_to_durable_plain_text(self):
        self.store.reserve_message(
            "invalid-blocks",
            TEAM,
            CHANNEL,
            THREAD,
            "Durable fallback",
            options={
                "mrkdwn": True,
                "blocks": [{"type": "invalid_test_block"}],
            },
        )
        with mock.patch.object(
            self.broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=[
                self.runtime.SlackAPIError("invalid_blocks"),
                "1789000003.000002",
            ],
        ) as post:
            result = self.broker._deliver_staged_message(
                "invalid-blocks"
            )

        self.assertEqual(result["message_ts"], "1789000003.000002")
        self.assertEqual(post.call_count, 2)
        first = post.call_args_list[0].kwargs
        second = post.call_args_list[1].kwargs
        self.assertIn("blocks", first["options"])
        self.assertEqual(second["options"], {"mrkdwn": True})
        self.assertEqual(first["client_msg_id"], second["client_msg_id"])
        with self.store.connect() as database:
            state = database.execute(
                """
                SELECT state,message_ts FROM slack_messages
                WHERE idempotency_key='invalid-blocks'
                """
            ).fetchone()
        self.assertEqual(tuple(state), ("sent", "1789000003.000002"))

    def test_bookkeeping_failure_does_not_turn_delivery_into_failure(self):
        with mock.patch.object(
            self.broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value="1789000003.000003",
        ) as post, mock.patch.object(
            self.store,
            "mark_participation",
            side_effect=RuntimeError("database unavailable"),
        ):
            first = self.broker.handle(
                self.request("bookkeeping-after-delivery")
            )
            second = self.broker.handle(
                self.request("bookkeeping-after-delivery")
            )

        self.assertEqual(first["message_ts"], "1789000003.000003")
        self.assertTrue(second["deduplicated"])
        post.assert_called_once()

    def test_durable_update_retries_the_same_target_after_uncertainty(self):
        target = "1789000003.000010"
        row = self.store.reserve_message_update(
            "hsg_" + "b" * 32,
            TEAM,
            CHANNEL,
            THREAD,
            target,
            "Final answer",
            {"blocks": [{"type": "section"}]},
        )
        with mock.patch.object(
            self.broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_update",
            side_effect=TimeoutError("unknown outcome"),
        ), self.assertRaises(TimeoutError):
            self.broker._deliver_staged_message(
                row["idempotency_key"]
            )

        runtime, store, broker = self.restart()
        with mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            runtime,
            "slack_update",
            return_value=target,
        ) as update:
            self.assertEqual(broker.recover_messages(), 1)

        update.assert_called_once()
        self.assertEqual(update.call_args.args[2], target)
        with store.connect() as database:
            state = database.execute(
                """
                SELECT operation,target_message_ts,state,message_ts
                FROM slack_messages
                WHERE idempotency_key=?
                """,
                (row["idempotency_key"],),
            ).fetchone()
        self.assertEqual(
            tuple(state),
            ("update", target, "sent", target),
        )

    def test_invalid_update_blocks_fall_back_to_plain_text(self):
        target = "1789000003.000020"
        row = self.store.reserve_message_update(
            "hsg_" + "c" * 32,
            TEAM,
            CHANNEL,
            THREAD,
            target,
            "Final answer",
            {"blocks": [{"type": "invalid_test_block"}]},
        )
        with mock.patch.object(
            self.broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_update",
            side_effect=[
                self.runtime.SlackAPIError("invalid_blocks"),
                target,
            ],
        ) as update:
            result = self.broker._deliver_staged_message(
                row["idempotency_key"]
            )

        self.assertEqual(result["message_ts"], target)
        self.assertEqual(update.call_count, 2)
        self.assertIn("blocks", update.call_args_list[0].kwargs["options"])
        self.assertEqual(update.call_args_list[1].kwargs["options"], {})

    def test_hermes_group_is_atomic_immutable_and_completes_ingress(self):
        event_id = f"slack:{TEAM}:{CHANNEL}:1789000005.000001"
        claim = self.store.claim_thread_ingress(
            event_id,
            TEAM,
            CHANNEL,
            THREAD,
            writer_id="hermes:test",
        )
        self.assertEqual(claim["status"], "claimed")
        self.assertTrue(
            self.store.mark_thread_ingress_dispatched(
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
            )
        )
        messages = [
            {"text": "First chunk", "options": {"mrkdwn": True}},
            {"text": "Second chunk", "options": {"mrkdwn": True}},
        ]
        group_id = "hsg_" + "a" * 32
        rows = self.store.reserve_message_group(
            group_id,
            TEAM,
            CHANNEL,
            THREAD,
            messages,
            ingress_event_id=event_id,
            ingress_lease_id=claim["lease_id"],
            ingress_fence_epoch=claim["fence_epoch"],
        )
        duplicate = self.store.reserve_message_group(
            group_id,
            TEAM,
            CHANNEL,
            THREAD,
            messages,
            ingress_event_id=event_id,
            ingress_lease_id=claim["lease_id"],
            ingress_fence_epoch=claim["fence_epoch"],
        )
        self.assertEqual(rows, duplicate)
        with self.assertRaisesRegex(RuntimeError, "changed after reservation"):
            self.store.reserve_message_group(
                group_id,
                TEAM,
                CHANNEL,
                THREAD,
                [
                    {"text": "Changed", "options": {"mrkdwn": True}},
                    {"text": "Second chunk", "options": {"mrkdwn": True}},
                ],
                ingress_event_id=event_id,
                ingress_lease_id=claim["lease_id"],
                ingress_fence_epoch=claim["fence_epoch"],
            )
        self.assertEqual(
            self.store.seal_thread_ingress_egress(
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
                allow_empty=False,
            ),
            "pending",
        )
        with mock.patch.object(
            self.broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=[
                "1789000005.000002",
                "1789000005.000003",
            ],
        ):
            self.broker._deliver_staged_message(rows[0]["idempotency_key"])
            with self.store.connect() as database:
                first_state = database.execute(
                    "SELECT state FROM thread_ingress WHERE event_id=?",
                    (event_id,),
                ).fetchone()["state"]
            self.assertEqual(first_state, "uncertain")
            self.broker._deliver_staged_message(rows[1]["idempotency_key"])
        with self.store.connect() as database:
            ingress = database.execute(
                """
                SELECT state,egress_sealed,error_code
                FROM thread_ingress WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            chunks = database.execute(
                """
                SELECT egress_chunk_index,egress_chunk_count,state
                FROM slack_messages WHERE egress_group_id=?
                ORDER BY egress_chunk_index
                """,
                (group_id,),
            ).fetchall()
        self.assertEqual(
            (ingress["state"], ingress["egress_sealed"], ingress["error_code"]),
            ("completed", 1, None),
        )
        self.assertEqual(
            [
                (row["egress_chunk_index"], row["egress_chunk_count"], row["state"])
                for row in chunks
            ],
            [(0, 2, "sent"), (1, 2, "sent")],
        )

    def test_crash_after_hermes_send_before_seal_requires_resolution(self):
        event_id = f"slack:{TEAM}:{CHANNEL}:1789000006.000001"
        claim = self.store.claim_thread_ingress(
            event_id,
            TEAM,
            CHANNEL,
            THREAD,
            writer_id="hermes:test",
        )
        self.assertTrue(
            self.store.mark_thread_ingress_dispatched(
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
            )
        )
        rows = self.store.reserve_message_group(
            "hsg_" + "e" * 32,
            TEAM,
            CHANNEL,
            THREAD,
            [{"text": "Accepted before process crash"}],
            ingress_event_id=event_id,
            ingress_lease_id=claim["lease_id"],
            ingress_fence_epoch=claim["fence_epoch"],
        )
        with mock.patch.object(
            self.broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value="1789000006.000002",
        ):
            self.broker._deliver_staged_message(rows[0]["idempotency_key"])
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE thread_ingress
                SET lease_expires_at=datetime('now','-1 second')
                WHERE event_id=?
                """,
                (event_id,),
            )

        unresolved = self.store.unresolved_operations(TEAM)

        self.assertEqual(
            [
                (item["kind"], item["id"], item["error_code"])
                for item in unresolved
            ],
            [
                (
                    "ingress",
                    event_id,
                    "hermes_dispatch_lease_expired",
                )
            ],
        )
        with self.store.connect() as database:
            ingress = database.execute(
                """
                SELECT state,egress_sealed FROM thread_ingress
                WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()
            outbox = database.execute(
                """
                SELECT state FROM slack_messages
                WHERE ingress_event_id=?
                """,
                (event_id,),
            ).fetchone()
        self.assertEqual(tuple(ingress), ("uncertain", 0))
        self.assertEqual(outbox["state"], "sent")

    def test_accepted_message_reconciles_after_local_commit_crash(self):
        with mock.patch.object(
            self.broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            return_value="1789000004.000001",
        ), mock.patch.object(
            self.store,
            "complete_message",
            side_effect=RuntimeError("simulated local commit crash"),
        ), self.assertRaisesRegex(RuntimeError, "simulated local commit crash"):
            self.broker.handle(self.request("accepted-before-commit"))

        with self.store.connect() as database:
            row = database.execute(
                """
                SELECT client_msg_id FROM slack_messages
                WHERE idempotency_key='accepted-before-commit'
                """
            ).fetchone()
            client_msg_id = str(row["client_msg_id"])
            database.execute(
                """
                UPDATE slack_messages
                SET lease_expires_at=datetime('now','-1 second')
                WHERE idempotency_key='accepted-before-commit'
                """
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

        runtime, store, broker = self.restart()

        def slack_call(_token, method, payload):
            self.assertEqual(method, "conversations.replies")
            self.assertEqual(payload["limit"], 15)
            return {
                "messages": [
                    {
                        "ts": "1789000004.000001",
                        "metadata": {
                            "event_type": "tether_message",
                            "event_payload": {
                                "client_msg_id": client_msg_id,
                            },
                        },
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            }

        with mock.patch.object(
            runtime,
            "_slack_call",
            side_effect=slack_call,
        ), mock.patch.object(
            runtime,
            "slack_post",
        ) as repost:
            self.assertEqual(broker.recover_reconciliations(), 1)
            self.assertEqual(broker.recover_messages(), 1)
        repost.assert_not_called()
        with store.connect() as database:
            saved = database.execute(
                """
                SELECT state,message_ts FROM slack_messages
                WHERE idempotency_key='accepted-before-commit'
                """
            ).fetchone()
        self.assertEqual(
            (saved["state"], saved["message_ts"]),
            ("sent", "1789000004.000001"),
        )

    def test_raw_broker_rejects_missing_idempotency_keys(self):
        request = self.request()
        request.pop("idempotency_key")
        with self.assertRaisesRegex(ValueError, "idempotency key is required"):
            self.broker.handle(request)

    def test_raw_broker_rejects_reply_without_attempt_key(self):
        bridge = self.store.bind(
            self.store.create(
                {
                    "source_kind": "headless_run",
                    "source": {
                        "run_id": "reply-key-required",
                        "cwd": "/tmp/project",
                    },
                    "owner_user_id": "U12345678",
                    "team_id": TEAM,
                    "channel_id": CHANNEL,
                    "idempotency_key": "reply-key-required",
                }
            ).bridge_id,
            THREAD,
        )
        with self.assertRaisesRegex(ValueError, "reply key is required"):
            self.broker.handle(
                {
                    "op": "reply",
                    "bridge_id": bridge.bridge_id,
                    "text": "Unsafe direct reply",
                }
            )


if __name__ == "__main__":
    unittest.main()
