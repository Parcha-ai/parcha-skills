import hashlib
import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"


def load_runtime(home: pathlib.Path):
    env = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
    }
    with mock.patch.dict(os.environ, env, clear=False):
        name = f"bridge_lifecycle_test_{id(home)}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


def process_identity(
    *,
    agent: str = "codex",
    pid: int = 200,
    start: str = "20000",
    session: str = "work",
    pane: str = "7",
) -> str:
    payload = {
        "agent": agent,
        "boot": "00000000-0000-4000-8000-000000000001",
        "exe": "1:2",
        "exe_path": hashlib.sha256(
            f"/opt/{agent}/bin/{agent}".encode()
        ).hexdigest()[:16],
        "pane": pane,
        "pid": pid,
        "session": session,
        "start": start,
        "tty": "34823",
    }
    return "linux-proc-v2:" + json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )


class BridgeLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")

    def tearDown(self):
        self.temp.cleanup()

    def request(
        self,
        key: str,
        *,
        pane: str = "7",
        session: str = "work",
        session_id: str | None = None,
    ):
        identity = process_identity(
            agent="codex",
            pid=200 + int(pane),
            start=str(20_000 + int(pane)),
            session=session,
            pane=pane,
        )
        return {
            "source_kind": "codex_session",
            "source": {
                "session_id": session_id or f"codex-{key}",
                "cwd": "/tmp/project",
                "zellij_session": session,
                "zellij_pane_id": pane,
                "pane_agent": "codex",
                "pane_command_hash": hashlib.sha256(
                    identity.encode()
                ).hexdigest(),
                "process_identity": identity,
            },
            "owner_user_id": "U12345678",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": key,
        }

    def bind(self, key: str, *, pane: str = "7", thread: str = "123.456"):
        bridge = self.store.create(self.request(key, pane=pane))
        return self.store.bind(bridge.bridge_id, thread)

    def test_exact_native_endpoint_can_own_multiple_active_threads(self):
        first = self.bind("first", pane="7", thread="123.001")
        duplicate = self.request(
            "second",
            pane="7",
            session_id="codex-different-session-id",
        )
        duplicate["channel_id"] = "C87654321"
        second = self.store.create(duplicate)
        second = self.store.bind(second.bridge_id, "123.002")

        other = self.bind("third", pane="8", thread="123.003")
        self.assertNotEqual(first.bridge_id, second.bridge_id)
        self.assertNotEqual(first.bridge_id, other.bridge_id)
        with self.store.connect() as database:
            keys = {
                row[0]
                for row in database.execute(
                    "SELECT endpoint_key FROM bridges WHERE bridge_id IN (?,?)",
                    (first.bridge_id, second.bridge_id),
                )
            }
        self.assertEqual(len(keys), 1)

    def test_migration_preserves_duplicate_endpoint_threads(self):
        older = self.bind("older", pane="7", thread="123.010")
        newer = self.bind("newer", pane="8", thread="123.011")
        with self.store.connect() as database:
            database.execute("DROP INDEX bridge_endpoint_lookup")
            database.execute(
                """
                CREATE UNIQUE INDEX bridge_endpoint_owner
                ON bridges(endpoint_key)
                WHERE endpoint_key!='' AND status IN ('pending','active')
                """
            )
            older_source = database.execute(
                "SELECT source_kind,source_json,endpoint_key FROM bridges "
                "WHERE bridge_id=?",
                (older.bridge_id,),
            ).fetchone()
            database.execute(
                """
                UPDATE bridges
                SET source_kind=?,source_json=?,endpoint_key=?,
                    updated_at='2026-07-26 00:00:00'
                WHERE bridge_id=?
                """,
                (
                    older_source["source_kind"],
                    older_source["source_json"],
                    older_source["endpoint_key"],
                    older.bridge_id,
                ),
            )
            database.execute(
                """
                UPDATE bridges
                SET source_kind=?,source_json=?,
                    updated_at='2026-07-27 00:00:00'
                WHERE bridge_id=?
                """,
                (
                    older_source["source_kind"],
                    older_source["source_json"],
                    newer.bridge_id,
                ),
            )

        reopened = self.runtime.Store(self.store.path)
        self.assertEqual(reopened.get(newer.bridge_id).status, "active")
        self.assertEqual(reopened.get(older.bridge_id).status, "active")
        with reopened.connect() as database:
            indexes = {
                row[1]: bool(row[2])
                for row in database.execute("PRAGMA index_list(bridges)")
            }
        self.assertNotIn("bridge_endpoint_owner", indexes)
        self.assertFalse(indexes["bridge_endpoint_lookup"])

    def test_rebind_moves_queued_work_to_new_generation(self):
        bridge = self.bind("queued-rebind")
        self.assertTrue(
            self.store.enqueue_event("1785000000.000001", bridge.bridge_id, "continue")
        )
        replacement = self.request("replacement", pane="8")["source"]
        rebound = self.store.rebind(
            bridge.bridge_id,
            "codex_session",
            replacement,
            expected_generation=bridge.binding_generation,
        )
        self.assertEqual(
            rebound.binding_generation,
            bridge.binding_generation + 1,
        )
        claimed = self.store.claim_next_event(bridge.bridge_id)
        self.assertEqual(claimed["event_id"], "1785000000.000001")
        with self.store.connect() as database:
            generation = database.execute(
                "SELECT binding_generation FROM bridge_events WHERE event_id=?",
                ("1785000000.000001",),
            ).fetchone()[0]
        self.assertEqual(generation, rebound.binding_generation)

    def test_rebind_blocks_claimed_thread_ingress(self):
        bridge = self.bind("claimed-ingress")
        claim = self.store.claim_thread_ingress(
            "1785000001.000001",
            bridge.team_id,
            bridge.channel_id,
            bridge.thread_ts,
            route_action="native",
            writer_id="native",
            bridge_id=bridge.bridge_id,
            binding_generation=bridge.binding_generation,
            payload={"text": "continue"},
        )
        self.assertEqual(claim["status"], "claimed")
        with self.assertRaisesRegex(
            ValueError,
            "claimed or pending Slack ingress",
        ):
            self.store.rebind(
                bridge.bridge_id,
                "codex_session",
                self.request("replacement", pane="8")["source"],
                expected_generation=bridge.binding_generation,
            )

    def test_only_pre_dispatch_hermes_ingress_is_recoverable(self):
        bridge = self.bind("hermes-recovery")
        event_id = "slack:T12345678:C12345678:1785000001.000002"
        payload = {
            "text": "continue",
            "user": "U12345678",
            "message_ts": "1785000001.000002",
            "channel_type": "channel",
        }
        claim = self.store.claim_thread_ingress(
            event_id,
            bridge.team_id,
            bridge.channel_id,
            bridge.thread_ts,
            route_action="hermes",
            writer_id="hermes",
            bridge_id=bridge.bridge_id,
            binding_generation=bridge.binding_generation,
            payload=payload,
        )
        self.assertTrue(
            self.store.release_thread_ingress(
                event_id,
                claim["lease_id"],
                "gateway_not_started",
            )
        )
        future = (
            self.runtime.datetime.datetime.now(
                self.runtime.datetime.timezone.utc
            )
            + self.runtime.datetime.timedelta(minutes=10)
        )
        recovered = self.store.recoverable_hermes_ingress(now=future)
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0]["event_id"], event_id)
        self.assertEqual(recovered[0]["payload"], payload)

        claimed_again = self.store.claim_thread_ingress(
            event_id,
            bridge.team_id,
            bridge.channel_id,
            bridge.thread_ts,
            route_action="hermes",
            writer_id="hermes",
            bridge_id=bridge.bridge_id,
            binding_generation=bridge.binding_generation,
            payload=payload,
        )
        self.assertTrue(
            self.store.mark_thread_ingress_dispatched(
                event_id,
                claimed_again["lease_id"],
                claimed_again["fence_epoch"],
            )
        )
        self.assertEqual(
            self.store.recoverable_hermes_ingress(now=future),
            [],
        )

    def test_expired_dispatched_hermes_ingress_becomes_uncertain(self):
        bridge = self.bind("hermes-crash-recovery")
        event_id = "slack:T12345678:C12345678:1785000001.000003"
        claim = self.store.claim_thread_ingress(
            event_id,
            bridge.team_id,
            bridge.channel_id,
            bridge.thread_ts,
            route_action="hermes",
            writer_id="hermes",
            bridge_id=bridge.bridge_id,
            binding_generation=bridge.binding_generation,
            payload={
                "text": "continue",
                "user": "U12345678",
                "message_ts": "1785000001.000003",
            },
        )
        self.assertTrue(
            self.store.mark_thread_ingress_dispatched(
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
            )
        )
        self.assertTrue(
            self.store.renew_thread_ingress(
                event_id,
                claim["lease_id"],
            )
        )
        with self.store.connect() as db:
            db.execute(
                """
                UPDATE thread_ingress
                SET lease_expires_at=datetime('now','-1 second')
                WHERE event_id=?
                """,
                (event_id,),
            )

        self.assertEqual(self.store.recoverable_hermes_ingress(), [])
        self.assertEqual(
            [
                (item["kind"], item["id"], item["error_code"])
                for item in self.store.unresolved_operations(bridge.team_id)
            ],
            [
                (
                    "ingress",
                    event_id,
                    "hermes_dispatch_lease_expired",
                )
            ],
        )

    def test_close_is_idempotent_and_releases_endpoint(self):
        bridge = self.bind("close-me")
        self.store.mark_participation(
            bridge.team_id,
            bridge.channel_id,
            bridge.thread_ts,
        )
        closed = self.store.close(
            bridge.bridge_id,
            expected_generation=bridge.binding_generation,
        )
        self.assertEqual(closed.status, "closed")
        self.assertEqual(
            closed.binding_generation,
            bridge.binding_generation + 1,
        )
        self.assertEqual(
            self.store.close(
                bridge.bridge_id,
                expected_generation=bridge.binding_generation,
            ),
            closed,
        )
        replacement = self.bind(
            "replacement-owner",
            pane="7",
            thread="123.999",
        )
        self.assertEqual(replacement.status, "active")
        with self.store.connect() as database:
            participation = database.execute(
                """
                SELECT count(*) FROM thread_participation
                WHERE team_id=? AND channel_id=? AND thread_ts=?
                """,
                (bridge.team_id, bridge.channel_id, bridge.thread_ts),
            ).fetchone()[0]
        self.assertEqual(participation, 0)

    def test_close_rejects_queued_or_active_work(self):
        bridge = self.bind("busy-close")
        self.store.enqueue_event("1785000002.000001", bridge.bridge_id, "continue")
        with self.assertRaisesRegex(ValueError, "queued or active delivery work"):
            self.store.close(
                bridge.bridge_id,
                expected_generation=bridge.binding_generation,
            )

    def test_broker_close_enforces_workspace_and_generation(self):
        bridge = self.bind("broker-close")
        broker = self.runtime.Broker(
            "test-token",
            self.store,
            verified_workspace_team_id="T12345678",
        )
        with self.assertRaisesRegex(
            self.runtime.NativeContinuationError,
            "different workspace",
        ):
            broker.handle(
                {
                    "op": "close",
                    "bridge_id": bridge.bridge_id,
                    "team_id": "T87654321",
                }
            )
        result = broker.handle(
            {
                "op": "close",
                "bridge_id": bridge.bridge_id,
                "expected_generation": bridge.binding_generation,
            }
        )
        self.assertEqual(result["status"], "closed")


if __name__ == "__main__":
    unittest.main()
