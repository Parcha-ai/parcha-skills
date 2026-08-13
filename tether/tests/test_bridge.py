import asyncio
import concurrent.futures
import datetime
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import shutil
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"
SECURITY_PATH = ROOT / "runtime" / "security.py"
HERMES_COMPAT_PATH = ROOT / "runtime" / "hermes_compat.py"
ROUTING_PATH = ROOT / "runtime" / "routing.py"
SLACK_PROTOCOL_PATH = ROOT / "runtime" / "slack_protocol.py"
PLUGIN_PATH = ROOT / "runtime" / "plugin" / "__init__.py"
NOTIFIER_PATH = ROOT / "skills" / "tether" / "scripts" / "tether_notify.py"
INSTALL_PATH = ROOT / "install.sh"


def load_runtime(home: pathlib.Path):
    env = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
    }
    with mock.patch.dict(os.environ, env, clear=False):
        name = f"bridge_runtime_test_{id(home)}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


def process_identity(
    *,
    agent="codex",
    pid=200,
    start="20000",
    session="work",
    pane="7",
    tty="34823",
):
    payload = {
        "agent": agent,
        "boot": "00000000-0000-4000-8000-000000000001",
        "exe": "1:2",
        "exe_path": hashlib.sha256(f"/opt/{agent}/bin/{agent}".encode()).hexdigest()[:16],
        "pane": pane,
        "pid": pid,
        "session": session,
        "start": start,
        "tty": tty,
    }
    return "linux-proc-v2:" + json.dumps(
        payload, sort_keys=True, separators=(",", ":")
    )


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")

    def tearDown(self):
        self.temp.cleanup()

    def request(self, key="run-1"):
        return {
            "source_kind": "headless_run",
            "source": {"run_id": "run-1", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": key,
        }

    def test_idempotency_and_exact_thread_lookup(self):
        first = self.store.create(self.request())
        second = self.store.create(self.request())
        self.assertEqual(first.bridge_id, second.bridge_id)
        active = self.store.bind(first.bridge_id, "123.456")
        self.assertEqual(self.store.find("T12345678", "C12345678", "123.456"), active)
        self.assertIsNone(self.store.find("T12345678", "C99999999", "123.456"))

    def test_events_are_deduplicated_and_serialized(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        self.assertTrue(self.store.enqueue_event("111.1", bridge.bridge_id, "first"))
        self.assertFalse(self.store.enqueue_event("111.1", bridge.bridge_id, "duplicate"))
        self.assertTrue(self.store.enqueue_event("111.2", bridge.bridge_id, "second"))
        first = self.store.claim_next_event(bridge.bridge_id)
        self.assertEqual(first["text"], "first")
        self.assertIsNone(self.store.claim_next_event(bridge.bridge_id), "a processing event blocks the next claim")
        self.store.finish_event(first["event_id"])
        self.assertEqual(self.store.claim_next_event(bridge.bridge_id)["text"], "second")

    def test_queued_followups_are_claimed_as_one_batch(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        self.assertTrue(self.store.enqueue_event("111.1", bridge.bridge_id, "first follow-up"))
        self.assertTrue(self.store.enqueue_event("111.2", bridge.bridge_id, "second follow-up"))
        batch = self.store.claim_event_batch(bridge.bridge_id)
        self.assertEqual(
            [(item["event_id"], item["text"]) for item in batch],
            [("111.1", "first follow-up"), ("111.2", "second follow-up")],
        )
        self.assertEqual(self.store.claim_event_batch(bridge.bridge_id), [])

    def test_processing_events_are_requeued_after_restart(self):
        bridge = self.store.bind(
            self.store.create(self.request()).bridge_id,
            "123.456",
        )
        self.assertTrue(self.store.enqueue_event("111.1", bridge.bridge_id, "resume me"))
        self.assertEqual(self.store.claim_next_event(bridge.bridge_id)["text"], "resume me")
        self.store.requeue_processing()
        self.assertEqual(self.store.claim_next_event(bridge.bridge_id)["text"], "resume me")

    def test_ingress_is_persistent_in_the_unified_event_ledger(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        self.assertTrue(self.store.enqueue_event("111.1", bridge.bridge_id, "event"))
        self.assertTrue(self.store.has_ingress("111.1"))
        self.assertFalse(self.store.enqueue_event("111.1", bridge.bridge_id, "duplicate"))

    def test_recent_active_bridges_include_native_and_headless_sources(self):
        native_request = self.request("native")
        native_request["source_kind"] = "claude_session"
        native_request["source"] = {"session_id": "claude-1", "cwd": "/tmp/project"}
        native = self.store.bind(self.store.create(native_request).bridge_id, "123.456")
        headless_request = self.request("headless")
        headless_request["source_kind"] = "headless_run"
        headless = self.store.bind(self.store.create(headless_request).bridge_id, "456.789")
        self.assertEqual(
            {bridge.bridge_id for bridge in self.store.recent_active_bridges()},
            {native.bridge_id, headless.bridge_id},
        )

    def test_recovery_keeps_old_active_bindings(self):
        bridge = self.store.bind(
            self.store.create(self.request("old-active")).bridge_id,
            "123.456",
        )
        with self.store.connect() as db:
            db.execute(
                """
                UPDATE bridges
                SET created_at='2020-01-01 00:00:00',
                    updated_at='2020-01-01 00:00:00'
                WHERE bridge_id=?
                """,
                (bridge.bridge_id,),
            )
        self.assertEqual(self.store.recent_active_bridges(), [])
        self.assertEqual(
            [item.bridge_id for item in self.store.active_bridges()],
            [bridge.bridge_id],
        )

    def test_legacy_owner_database_permissions_are_tightened_before_migration(self):
        legacy_path = self.home / "legacy.db"
        database = sqlite3.connect(legacy_path)
        database.execute("CREATE TABLE legacy(value TEXT)")
        database.commit()
        database.close()
        legacy_path.chmod(0o644)
        self.runtime.Store(legacy_path)
        self.assertEqual(stat.S_IMODE(legacy_path.stat().st_mode), 0o600)

    def test_stored_errors_are_truncated(self):
        bridge = self.store.bind(
            self.store.create(self.request()).bridge_id,
            "123.456",
        )
        self.store.claim_event("111.1", bridge.bridge_id)
        self.store.finish_event("111.1", "sensitive" * 500)
        with self.store.connect() as db:
            error = db.execute("SELECT error FROM bridge_events WHERE event_id='111.1'").fetchone()[0]
        self.assertEqual(len(error), 1000)

    def test_invalid_ids_fail_closed(self):
        request = self.request()
        request["owner_user_id"] = "not-a-slack-id"
        with self.assertRaises(ValueError):
            self.store.create(request)

    def test_user_id_cannot_be_stored_as_a_channel(self):
        with self.assertRaisesRegex(ValueError, "invalid Slack channel"):
            self.store.create({
                "source_kind": "headless_run",
                "source": {"run_id": "run-1"},
                "owner_user_id": "U12345678",
                "channel_id": "U12345678",
                "idempotency_key": "bad-user-destination",
            })

    def test_source_metadata_rejects_unknown_or_oversized_values(self):
        request = self.request()
        request["source"]["prompt"] = "should never be persisted"
        with self.assertRaisesRegex(ValueError, "invalid bridge source"):
            self.store.create(request)

        request = self.request()
        request["source"]["cwd"] = "x" * (self.runtime.MAX_SOURCE_VALUE + 1)
        with self.assertRaisesRegex(ValueError, "source value is too large"):
            self.store.create(request)

    def test_source_contract_rejects_incomplete_contradictory_and_option_like_records(self):
        invalid_sources = (
            ("claude_session", {"cwd": "/tmp/project"}, "missing native session"),
            (
                "claude_session",
                {"session_id": "--resume", "cwd": "/tmp/project"},
                "option-like Claude session",
            ),
            (
                "claude_session",
                {
                    "session_id": "claude-1",
                    "cwd": "/tmp/project",
                    "zellij_session": "work",
                    "zellij_pane_id": "7",
                    "pane_agent": "codex",
                    "pane_command_hash": "fingerprint",
                },
                "Claude session bound to Codex pane",
            ),
            ("codex_session", {"cwd": "/tmp/project"}, "missing Codex session"),
            (
                "codex_session",
                {"session_id": "-danger-full-access", "cwd": "/tmp/project"},
                "option-like Codex session",
            ),
            (
                "codex_session",
                {
                    "session_id": "codex-1",
                    "cwd": "/tmp/project",
                    "zellij_session": "work",
                    "zellij_pane_id": "7",
                    "pane_agent": "claude",
                    "pane_command_hash": "fingerprint",
                },
                "Codex session bound to Claude pane",
            ),
            (
                "zellij_pane",
                {"session_name": "work", "pane_agent": "codex"},
                "incomplete Zellij endpoint",
            ),
            (
                "zellij_pane",
                {
                    "session_name": "work",
                    "zellij_session": "other-work",
                    "pane_id": "7",
                    "zellij_pane_id": "8",
                    "pane_agent": "codex",
                    "pane_command_hash": "fingerprint",
                },
                "contradictory Zellij aliases",
            ),
            ("headless_run", {"cwd": "/tmp/project"}, "missing headless run ID"),
            (
                "headless_run",
                {"run_id": "run-1", "queue_id": "run-2", "cwd": "/tmp/project"},
                "contradictory headless IDs",
            ),
        )
        for kind, source, reason in invalid_sources:
            with self.subTest(reason=reason):
                with self.assertRaises(ValueError):
                    self.store.validate_source(kind, source)

        valid_sources = (
            ("claude_session", {"session_id": "claude-1", "cwd": "/tmp/project"}),
            (
                "codex_session",
                {
                    "session_id": "codex-1",
                    "cwd": "/tmp/project",
                    "zellij_session": "work",
                    "zellij_pane_id": "7",
                    "pane_agent": "codex",
                    "pane_command_hash": "fingerprint",
                    "process_identity": process_identity(),
                },
            ),
            (
                "zellij_pane",
                {
                    "session_name": "work",
                    "pane_id": "7",
                    "cwd": "/tmp/project",
                    "pane_agent": "codex",
                    "pane_command_hash": "fingerprint",
                    "process_identity": process_identity(),
                },
            ),
            (
                "headless_run",
                {"run_id": "run-1", "queue_id": "run-1", "cwd": "/tmp/project"},
            ),
            (
                "headless_run",
                {"queue_id": "legacy-queue-1", "cwd": "/tmp/project"},
            ),
        )
        for kind, source in valid_sources:
            with self.subTest(valid_kind=kind):
                canonical = self.store.validate_source(kind, source)
                self.assertEqual(
                    {key: canonical[key] for key in source},
                    source,
                    "canonical BindingV3 metadata may be added, but source identity must be preserved",
                )
                self.assertEqual(canonical["binding_version"], "3")
                self.assertEqual(canonical["binding_state"], "verified")
        _, legacy_headless = self.runtime._canonical_source(
            "headless_run",
            {"queue_id": "legacy-queue-1", "cwd": "/tmp/project"},
        )
        self.assertEqual(legacy_headless.run_id, "legacy-queue-1")

    def test_schema_v2_migrates_bindings_and_removes_dead_routes(self):
        path = self.home / "legacy.db"
        database = sqlite3.connect(path)
        database.executescript(
            """
            CREATE TABLE bridges (
              bridge_id TEXT PRIMARY KEY, source_kind TEXT NOT NULL,
              source_json TEXT NOT NULL, owner_user_id TEXT NOT NULL,
              team_id TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL,
              thread_ts TEXT, idempotency_key TEXT NOT NULL UNIQUE,
              status TEXT NOT NULL DEFAULT 'pending',
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE thread_routes (
              team_id TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL,
              thread_ts TEXT NOT NULL, route TEXT NOT NULL,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (team_id, channel_id, thread_ts)
            );
            """
        )
        rows = (
            (
                "brg_detached",
                "codex_session",
                json.dumps({"session_id": "codex-1", "cwd": "/tmp/project"}),
            ),
            (
                "brg_legacy_pane",
                "claude_session",
                json.dumps({
                    "session_id": "claude-1",
                    "cwd": "/tmp/project",
                    "zellij_session": "work",
                    "zellij_pane_id": "7",
                    "pane_agent": "claude",
                    "pane_command_hash": "command-only",
                }),
            ),
            ("brg_invalid", "headless_run", "{not-json"),
        )
        for index, (bridge_id, source_kind, source_json) in enumerate(rows):
            database.execute(
                """
                INSERT INTO bridges(
                  bridge_id,source_kind,source_json,owner_user_id,team_id,
                  channel_id,thread_ts,idempotency_key,status
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    bridge_id,
                    source_kind,
                    source_json,
                    "U12345678",
                    "T12345678",
                    "C12345678",
                    f"123.45{index}",
                    f"legacy-{index}",
                    "active",
                ),
            )
        database.execute(
            """
            INSERT INTO thread_routes(team_id,channel_id,thread_ts,route)
            VALUES('T12345678','C12345678','123.450','self')
            """
        )
        database.commit()
        database.close()
        path.chmod(0o600)

        migrated = self.runtime.Store(path)
        detached = migrated.get("brg_detached")
        self.assertEqual(detached.binding_state, "verified")
        self.assertEqual(
            detached.thread_claim_generation,
            detached.binding_generation,
        )
        legacy = migrated.get("brg_legacy_pane")
        self.assertEqual(legacy.binding_state, "rebind_required")
        self.assertEqual(legacy.binding_error_code, "process_identity_missing")
        self.assertIsNone(legacy.thread_claim_generation)
        invalid = migrated.get("brg_invalid")
        self.assertEqual(invalid.binding_state, "rebind_required")
        self.assertEqual(invalid.binding_error_code, "binding_invalid")
        self.assertIsNone(invalid.thread_claim_generation)
        with migrated.connect() as database:
            self.assertEqual(
                database.execute("PRAGMA user_version").fetchone()[0],
                self.runtime.SCHEMA_VERSION,
            )
            self.assertIsNone(
                database.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type='table' AND name='thread_routes'
                    """
                ).fetchone()
            )
            self.assertEqual(database.execute("SELECT count(*) FROM bridges").fetchone()[0], 3)

    def test_schema_rejects_future_version_before_mutation(self):
        path = self.home / "future.db"
        database = sqlite3.connect(path)
        database.execute("PRAGMA user_version=99")
        database.close()
        path.chmod(0o600)
        with self.assertRaisesRegex(RuntimeError, "newer than this runtime"):
            self.runtime.Store(path)
        database = sqlite3.connect(path)
        self.assertEqual(database.execute("PRAGMA user_version").fetchone()[0], 99)
        self.assertEqual(
            database.execute(
                "SELECT count(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0],
            0,
        )
        database.close()

    def test_concurrent_schema_open_is_serialized_and_idempotent(self):
        path = self.home / "concurrent-migration.db"
        database = sqlite3.connect(path)
        database.execute("PRAGMA user_version=1")
        database.close()
        path.chmod(0o600)
        barrier = threading.Barrier(8)

        def open_store(_index):
            barrier.wait()
            store = self.runtime.Store(path)
            return store.get("missing")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(open_store, range(8)))

        self.assertEqual(results, [None] * 8)
        database = sqlite3.connect(path)
        self.assertEqual(
            database.execute("PRAGMA user_version").fetchone()[0],
            self.runtime.SCHEMA_VERSION,
        )
        self.assertEqual(
            database.execute(
                "SELECT count(*) FROM sqlite_master WHERE name='bridge_attempts'"
            ).fetchone()[0],
            1,
        )
        database.close()

    def test_failed_schema_migration_rolls_back_as_one_transaction(self):
        path = self.home / "failed-migration.db"
        database = sqlite3.connect(path)
        database.execute("PRAGMA user_version=1")
        database.close()
        path.chmod(0o600)

        with mock.patch.object(
            self.runtime.Store,
            "_backfill_bindings",
            side_effect=RuntimeError("fault injection"),
        ), self.assertRaisesRegex(RuntimeError, "fault injection"):
            self.runtime.Store(path)

        database = sqlite3.connect(path)
        self.assertEqual(database.execute("PRAGMA user_version").fetchone()[0], 1)
        self.assertEqual(
            database.execute(
                """
                SELECT count(*) FROM sqlite_master
                WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%'
                """
            ).fetchone()[0],
            0,
        )
        database.close()

    def test_tampered_verified_binding_is_downgraded_on_reopen(self):
        bridge = self.store.create(self.request("tampered-v2"))
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE bridges
                SET source_json='{}',binding_version=2,binding_state='verified'
                WHERE bridge_id=?
                """,
                (bridge.bridge_id,),
            )
        reopened = self.runtime.Store(self.store.path)
        loaded = reopened.get(bridge.bridge_id)
        self.assertEqual(loaded.binding_state, "rebind_required")
        self.assertEqual(loaded.binding_error_code, "binding_invalid")

    def test_rebind_is_generation_safe_and_blocks_open_attempt(self):
        exact = process_identity(agent="codex", session="work", pane="7")
        request = self.request("generation-safe")
        request["source_kind"] = "codex_session"
        request["source"] = {
            "session_id": "codex-1",
            "cwd": "/tmp/project",
            "zellij_session": "work",
            "zellij_pane_id": "7",
            "pane_agent": "codex",
            "pane_command_hash": hashlib.sha256(exact.encode()).hexdigest(),
            "process_identity": exact,
        }
        bridge = self.store.bind(self.store.create(request).bridge_id, "123.456")
        self.store.enqueue_event("111.1", bridge.bridge_id, "continue")
        self.store.claim_event_batch(bridge.bridge_id)
        attempt = self.runtime.delivery_attempt_id(
            bridge.bridge_id, ("111.1",), bridge.binding_generation
        )
        self.assertTrue(self.store.prepare_delivery_attempt(
            ["111.1"], bridge.bridge_id, bridge.binding_generation, attempt
        ))
        replacement = {
            **request["source"],
            "process_identity": process_identity(
                agent="codex", pid=300, start="30000", session="work", pane="7"
            ),
        }
        with self.assertRaisesRegex(ValueError, "active delivery attempt"):
            self.store.rebind(
                bridge.bridge_id,
                "codex_session",
                replacement,
                expected_generation=bridge.binding_generation,
            )
        self.store.fail_attempt(attempt, bridge.bridge_id, "operator retry")
        rebound = self.store.rebind(
            bridge.bridge_id,
            "codex_session",
            replacement,
            expected_generation=bridge.binding_generation,
        )
        self.assertEqual(rebound.binding_generation, bridge.binding_generation + 1)
        with self.assertRaisesRegex(ValueError, "binding changed"):
            self.store.rebind(
                bridge.bridge_id,
                "codex_session",
                replacement,
                expected_generation=bridge.binding_generation,
            )

    def test_attempt_reply_and_no_reply_acknowledge_exact_events(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        for event_id, text, response, expected_ack in (
            ("111.1", "status?", "Fixed.", "reply"),
            ("111.2", "anything else?", "NO_REPLY", "no_reply"),
        ):
            self.assertTrue(self.store.enqueue_event(event_id, bridge.bridge_id, text))
            self.store.claim_event_batch(bridge.bridge_id)
            attempt = self.runtime.delivery_attempt_id(
                bridge.bridge_id, (event_id,), bridge.binding_generation
            )
            self.assertTrue(self.store.prepare_delivery_attempt(
                [event_id], bridge.bridge_id, bridge.binding_generation, attempt
            ))
            self.assertTrue(self.store.mark_attempt_awaiting_ack(
                attempt, bridge.bridge_id, bridge.binding_generation
            ))
            with mock.patch.object(
                broker, "_ensure_channel_membership"
            ), mock.patch.object(
                self.runtime, "slack_post", return_value="123.999"
            ) as post:
                result = broker.handle({
                    "op": "reply",
                    "bridge_id": bridge.bridge_id,
                    "reply_key": attempt,
                    "text": response,
                })
            self.assertEqual(result["acknowledged_events"], 1)
            self.assertEqual(post.call_count, 0 if response == "NO_REPLY" else 1)
            with self.store.connect() as database:
                event_state = database.execute(
                    "SELECT state FROM bridge_events WHERE event_id=?", (event_id,)
                ).fetchone()[0]
                attempt_row = database.execute(
                    "SELECT state,ack_kind FROM bridge_attempts WHERE attempt_id=?",
                    (attempt,),
                ).fetchone()
            self.assertEqual(event_state, "delivered")
            self.assertEqual(tuple(attempt_row), ("acknowledged", expected_ack))

    def test_fast_attempt_ack_wins_over_late_awaiting_transition(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        self.store.enqueue_event("111.1", bridge.bridge_id, "status?")
        self.store.claim_event_batch(bridge.bridge_id)
        attempt = self.runtime.delivery_attempt_id(
            bridge.bridge_id, ("111.1",), bridge.binding_generation
        )
        self.assertTrue(self.store.prepare_delivery_attempt(
            ["111.1"], bridge.bridge_id, bridge.binding_generation, attempt
        ))
        self.assertTrue(self.store.mark_attempt_submitting(
            attempt, bridge.bridge_id, bridge.binding_generation
        ))
        self.assertEqual(
            self.store.acknowledge_attempt(
                attempt, bridge.bridge_id, ack_kind="no_reply"
            ),
            1,
        )
        self.assertTrue(self.store.mark_attempt_awaiting_ack(
            attempt, bridge.bridge_id, bridge.binding_generation
        ))
        self.assertEqual(
            self.store.attempt_state(attempt, bridge.bridge_id),
            "acknowledged",
        )
        with self.store.connect() as database:
            self.assertEqual(
                database.execute(
                    "SELECT state FROM bridge_events WHERE event_id='111.1'"
                ).fetchone()[0],
                "delivered",
            )

    def test_broker_socket_is_private(self):
        socket_path = self.home / "hermes" / "bridge.sock"
        server = self.runtime.start_broker("test-token", socket_path)
        try:
            self.assertTrue(socket_path.is_socket())
            self.assertEqual(socket_path.stat().st_mode & 0o777, 0o600)
            with self.assertRaisesRegex(
                self.runtime.NativeContinuationError, "unsupported operation"
            ) as rejected:
                self.runtime.broker_call({"op": "unsupported"}, socket_path)
            self.assertEqual(rejected.exception.code, "invalid_request")
            self.assertEqual(rejected.exception.status, "rejected")
            self.assertFalse(rejected.exception.retryable)
            self.assertEqual(
                rejected.exception.next_action,
                "Correct the request and retry.",
            )
            with self.assertRaises(
                ValueError
            ) as invalid_request:
                self.runtime.broker_call([], socket_path)
            self.assertIn("JSON object", str(invalid_request.exception))
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(str(socket_path))
                client.sendall(b"[]\n")
                raw_response = client.recv(4096)
            malformed = json.loads(raw_response)
            self.assertFalse(malformed["ok"])
            self.assertEqual(malformed["code"], "invalid_request")

            server.broker.handle = mock.Mock(
                side_effect=RuntimeError(
                    "credential secret-value leaked from /private/operator/path"
                )
            )
            with self.assertRaises(
                self.runtime.NativeContinuationError
            ) as internal:
                self.runtime.broker_call({"op": "status"}, socket_path)
            self.assertEqual(internal.exception.code, "broker_internal_error")
            self.assertTrue(internal.exception.retryable)
            self.assertNotIn("secret-value", str(internal.exception))
            self.assertNotIn("/private/operator/path", str(internal.exception))
        finally:
            server.shutdown()
            server.server_close()
            socket_path.unlink(missing_ok=True)

    def test_broker_response_budget_supports_history_but_fails_closed(self):
        socket_path = self.home / "hermes" / "bridge.sock"
        server = self.runtime.start_broker("test-token", socket_path)
        try:
            server.broker.handle = mock.Mock(
                return_value={"ok": True, "padding": "x" * (2 * 1024 * 1024)}
            )
            accepted = self.runtime.broker_call({"op": "status"}, socket_path)
            self.assertEqual(len(accepted["padding"]), 2 * 1024 * 1024)

            server.broker.handle = mock.Mock(
                return_value={
                    "ok": True,
                    "padding": "x" * self.runtime.MAX_BROKER_RESPONSE_BYTES,
                }
            )
            with self.assertRaises(
                self.runtime.NativeContinuationError
            ) as oversized:
                self.runtime.broker_call({"op": "status"}, socket_path)
            self.assertEqual(
                oversized.exception.code,
                "broker_internal_error",
            )

            server.broker.handle = mock.Mock(
                return_value={"ok": True, "not_json": object()}
            )
            with self.assertRaises(
                self.runtime.NativeContinuationError
            ) as invalid:
                self.runtime.broker_call({"op": "status"}, socket_path)
            self.assertEqual(
                invalid.exception.code,
                "broker_internal_error",
            )

            server.broker.handle = mock.Mock(return_value=[])
            with self.assertRaises(
                self.runtime.NativeContinuationError
            ) as wrong_contract:
                self.runtime.broker_call({"op": "status"}, socket_path)
            self.assertEqual(
                wrong_contract.exception.code,
                "broker_internal_error",
            )
        finally:
            server.shutdown()
            server.server_close()
            socket_path.unlink(missing_ok=True)

    def test_store_connections_close_after_each_transaction(self):
        context = self.store.connect()
        with context as db:
            self.assertEqual(db.execute("SELECT 1").fetchone()[0], 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")

    def test_broker_reuses_hermes_channel_and_allowlist_without_copying_ids(self):
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        request = {
            "op": "notify", "text": "finished", "source_kind": "headless_run",
            "source": {"run_id": "run-2", "cwd": "/tmp/project"},
            "idempotency_key": "run-2",
        }
        with mock.patch.dict(os.environ, {
            "SLACK_HOME_CHANNEL": "C12345678",
            "SLACK_ALLOWED_USERS": "U12345678,U87654321",
        }, clear=False), mock.patch.object(
            broker, "_ensure_channel_membership"
        ), mock.patch.object(self.runtime, "slack_post", return_value="123.456"):
            result = broker.handle(request)
            status = broker.handle({"op": "status"})
        bridge = self.store.get(result["bridge_id"])
        self.assertEqual(bridge.channel_id, "C12345678")
        self.assertEqual(bridge.owner_user_id, "*", "Hermes's explicit allowlist is shared by default")
        self.assertEqual(status["allowed_user_count"], 2)
        self.assertEqual(status["implementation"], "tether")
        self.assertEqual(status["protocol_version"], 6)
        self.assertNotIn("allowed_users", status, "status reports readiness, never identities")

    def test_shared_channel_rejects_accidental_owner_restriction(self):
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        request = {
            "op": "notify", "text": "finished", "source_kind": "headless_run",
            "source": {"run_id": "run-owner", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678",
            "channel_id": "C12345678",
            "idempotency_key": "run-owner",
        }
        with mock.patch.dict(
            os.environ, {"SLACK_ALLOWED_USERS": "U12345678,U87654321"}, clear=False
        ), self.assertRaisesRegex(ValueError, "owner-restricted shared-channel"):
            broker.handle(request)

    def test_bound_reply_is_brief_and_idempotent_per_agent_turn(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        self.store.enqueue_event("111.1", bridge.bridge_id, "status?")
        self.store.claim_event_batch(bridge.bridge_id)
        reply_key = self.runtime.delivery_attempt_id(
            bridge.bridge_id, ("111.1",), bridge.binding_generation
        )
        self.assertTrue(self.store.prepare_delivery_attempt(
            ["111.1"], bridge.bridge_id, bridge.binding_generation, reply_key
        ))
        self.assertTrue(self.store.mark_attempt_awaiting_ack(
            reply_key, bridge.bridge_id, bridge.binding_generation
        ))
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        request = {
            "op": "reply", "bridge_id": bridge.bridge_id,
            "reply_key": reply_key, "text": "Fixed and verified.",
        }
        with mock.patch.object(broker, "_ensure_channel_membership"), mock.patch.object(
            self.runtime, "slack_post", return_value="123.457",
        ) as post:
            first = broker.handle(request)
            second = broker.handle(request)
        self.assertFalse(first["deduplicated"])
        self.assertTrue(second["deduplicated"])
        self.assertEqual(post.call_count, 1)

        detailed = " ".join(["detail"] * 51)
        self.assertEqual(self.runtime.validate_reply_text(detailed), detailed)

        too_long = "x" * (self.runtime.MAX_TEXT + 1)
        with self.assertRaisesRegex(ValueError, "transport limit"):
            broker.handle({
                "op": "reply", "bridge_id": bridge.bridge_id,
                "reply_key": "tether-" + "abcdef123458", "text": too_long,
            })

    def test_explicit_rebind_updates_only_the_matching_active_thread(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        result = broker.handle({
            "op": "rebind",
            "team_id": bridge.team_id,
            "channel_id": bridge.channel_id,
            "thread_ts": bridge.thread_ts,
            "source_kind": "zellij_pane",
            "source": {
                "session_name": "work", "pane_id": "7", "cwd": "/tmp/project",
                "pane_agent": "claude", "pane_command_hash": "new-fingerprint",
                "process_identity": process_identity(agent="claude"),
            },
        })
        rebound = self.store.get(bridge.bridge_id)
        self.assertEqual(result["bridge_id"], bridge.bridge_id)
        self.assertEqual(rebound.source_kind, "zellij_pane")
        self.assertEqual(rebound.source["pane_command_hash"], "new-fingerprint")
        with self.assertRaisesRegex(ValueError, "active bridge not found"):
            broker.handle({
                "op": "rebind",
                "team_id": bridge.team_id,
                "channel_id": "C99999999",
                "thread_ts": bridge.thread_ts,
                "source_kind": "zellij_pane",
                "source": rebound.source,
            })

    def test_no_reply_control_token_is_suppressed(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        self.store.enqueue_event("111.1", bridge.bridge_id, "anything else?")
        self.store.claim_event_batch(bridge.bridge_id)
        reply_key = self.runtime.delivery_attempt_id(
            bridge.bridge_id, ("111.1",), bridge.binding_generation
        )
        self.assertTrue(self.store.prepare_delivery_attempt(
            ["111.1"], bridge.bridge_id, bridge.binding_generation, reply_key
        ))
        self.assertTrue(self.store.mark_attempt_awaiting_ack(
            reply_key, bridge.bridge_id, bridge.binding_generation
        ))
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        with mock.patch.object(self.runtime, "slack_post") as post:
            result = broker.handle({
                "op": "reply", "bridge_id": bridge.bridge_id,
                "reply_key": reply_key, "text": "NO_REPLY",
            })
        self.assertTrue(result["suppressed"])
        post.assert_not_called()

    def test_trailing_no_reply_suppresses_the_entire_agent_output(self):
        bridge = self.store.bind(self.store.create(self.request()).bridge_id, "123.456")
        self.store.enqueue_event("111.2", bridge.bridge_id, "anything else?")
        self.store.claim_event_batch(bridge.bridge_id)
        reply_key = self.runtime.delivery_attempt_id(
            bridge.bridge_id, ("111.2",), bridge.binding_generation
        )
        self.assertTrue(self.store.prepare_delivery_attempt(
            ["111.2"], bridge.bridge_id, bridge.binding_generation, reply_key
        ))
        self.assertTrue(self.store.mark_attempt_awaiting_ack(
            reply_key, bridge.bridge_id, bridge.binding_generation
        ))
        broker = self.runtime.Broker(
            "test-token",
            self.store,
            verified_workspace_team_id="T12345678",
        )
        with mock.patch.object(self.runtime, "slack_post") as post:
            result = broker.handle({
                "op": "reply",
                "bridge_id": bridge.bridge_id,
                "reply_key": reply_key,
                "text": "This is directed at another person.\n\nNO_REPLY",
            })
        self.assertTrue(result["suppressed"])
        post.assert_not_called()

    def test_no_reply_in_prose_remains_a_normal_reply(self):
        self.assertEqual(
            self.runtime.validate_reply_text(
                "NO_REPLY is a control token, not user-facing prose."
            ),
            "NO_REPLY is a control token, not user-facing prose.",
        )

    def test_thread_history_stays_behind_broker_and_returns_sanitized_messages(self):
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        response = {
            "messages": [{
                "ts": "123.456", "thread_ts": "100.000", "text": "reply",
                "user": "U12345678", "blocks": [{"private": "detail"}],
            }]
        }
        with mock.patch.object(self.runtime, "_slack_call", side_effect=[
            {"ok": True, "messages": []}, response,
        ]) as call:
            result = broker.handle({
                "op": "thread_history", "channel_id": "C12345678",
                "thread_ts": "100.000", "limit": 10,
            })
        self.assertEqual(result["messages"], [{
            "ts": "123.456", "thread_ts": "100.000", "text": "reply", "user": "U12345678",
        }])
        self.assertEqual(call.call_args_list, [
            mock.call(
                "test-token", "conversations.history",
                {"channel": "C12345678", "limit": 1},
            ),
            mock.call(
                "test-token", "conversations.replies",
                {"channel": "C12345678", "ts": "100.000", "limit": 10},
            ),
        ])

    def test_public_destination_is_joined_only_once_per_broker(self):
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        with mock.patch.object(
            self.runtime,
            "_slack_call",
            side_effect=[
                RuntimeError("Slack API error: not_in_channel"),
                {"ok": True},
            ],
        ) as call:
            broker._ensure_channel_membership("C12345678")
            broker._ensure_channel_membership("C12345678")
            broker._ensure_channel_membership("D12345678")
        self.assertEqual(call.call_args_list, [
            mock.call(
                "test-token", "conversations.history",
                {"channel": "C12345678", "limit": 1},
            ),
            mock.call("test-token", "conversations.join", {"channel": "C12345678"}),
        ])

    def test_dm_with_c_prefixed_id_is_not_joined(self):
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        with mock.patch.object(
            self.runtime,
            "_slack_call",
            return_value={"ok": True, "messages": []},
        ) as call:
            broker._ensure_channel_membership("C87654321")
        call.assert_called_once_with(
            "test-token",
            "conversations.history",
            {"channel": "C87654321", "limit": 1},
        )

    def test_identity_returns_only_nonsecret_bot_metadata(self):
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        with mock.patch.object(self.runtime, "_slack_call", return_value={
            "ok": True, "team_id": "T12345678", "user_id": "U12345678",
            "user": "agent", "url": "https://example.slack.com/",
        }):
            result = broker.handle({"op": "identity"})
        self.assertEqual(result, {
            "ok": True, "team_id": "T12345678", "user_id": "U12345678", "user": "agent",
        })

    def test_brokered_thread_post_does_not_create_a_second_bridge(self):
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        with mock.patch.object(broker, "_ensure_channel_membership"), mock.patch.object(
            self.runtime, "slack_post", return_value="123.457",
        ) as post:
            result = broker.handle({
                "op": "thread_reply", "channel_id": "C12345678",
                "thread_ts": "123.456", "text": "progress",
                "idempotency_key": "progress-123.456",
            })
        post.assert_called_once()
        self.assertEqual(
            post.call_args.args,
            ("test-token", "C12345678", "progress", "123.456"),
        )
        self.assertTrue(post.call_args.kwargs["client_msg_id"])
        self.assertEqual(
            post.call_args.kwargs["metadata_event_type"],
            "tether_message",
        )
        self.assertEqual(result["thread_ts"], "123.456")
        self.assertEqual(self.store.recent_active_bridges(), [])
        self.assertTrue(
            self.store.participates("T12345678", "C12345678", "123.456")
        )

    def test_attach_binds_existing_thread_without_posting(self):
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        with mock.patch.object(self.runtime, "slack_post") as post:
            result = broker.handle({
                "op": "attach",
                "source_kind": "claude_session",
                "source": {"session_id": "claude-1", "cwd": "/tmp/project"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "thread_ts": "123.456",
                "idempotency_key": "review-123.456",
            })
        post.assert_not_called()
        self.assertEqual(result["thread_ts"], "123.456")
        bridge = self.store.find("T12345678", "C12345678", "123.456")
        self.assertEqual(bridge.source_kind, "claude_session")
        self.assertEqual(bridge.source["session_id"], "claude-1")
        self.assertEqual(
            bridge.thread_claim_generation,
            bridge.binding_generation,
        )

    def test_deduplicated_attach_repairs_missing_thread_claim(self):
        broker = self.runtime.Broker(
            "test-token",
            self.store,
            verified_workspace_team_id="T12345678",
        )
        request = {
            "op": "attach",
            "source_kind": "claude_session",
            "source": {"session_id": "claude-1", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "thread_ts": "123.456",
            "idempotency_key": "review-123.456",
        }
        first = broker.handle(request)
        with self.store.connect() as database:
            database.execute(
                "UPDATE bridges SET thread_claim_generation=NULL "
                "WHERE bridge_id=?",
                (first["bridge_id"],),
            )

        repeated = broker.handle(request)

        self.assertTrue(repeated["deduplicated"])
        bridge = self.store.get(first["bridge_id"])
        self.assertEqual(
            bridge.thread_claim_generation,
            bridge.binding_generation,
        )

    def test_attach_refuses_to_replace_active_binding(self):
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        request = {
            "op": "attach", "source_kind": "claude_session",
            "source": {"session_id": "claude-1", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678", "team_id": "T12345678",
            "channel_id": "C12345678", "thread_ts": "123.456",
            "idempotency_key": "review-one",
        }
        broker.handle(request)
        request["idempotency_key"] = "review-two"
        request["source"] = {"session_id": "claude-2", "cwd": "/tmp/project"}
        with self.assertRaisesRegex(ValueError, "already has an active"):
            broker.handle(request)

    def test_thread_participation_survives_store_reopen_and_team_lookup(self):
        self.store.mark_participation("T12345678", "C12345678", "123.456")
        reopened = self.runtime.Store(self.store.path)
        self.assertTrue(reopened.participates("T12345678", "C12345678", "123.456"))
        self.assertFalse(reopened.participates("T99999999", "C12345678", "123.456"))

    def test_participating_thread_ingress_is_deduplicated_and_kept_recent(self):
        self.store.mark_participation("T12345678", "C12345678", "123.456")
        claimed = self.store.claim_thread_ingress(
            "123.457",
            "T12345678",
            "C12345678",
            "123.456",
        )
        self.assertEqual(claimed["status"], "claimed")
        self.assertTrue(
            self.store.complete_thread_ingress(
                "123.457",
                claimed["lease_id"],
                claimed["fence_epoch"],
            )
        )
        self.assertEqual(
            self.store.claim_thread_ingress(
                "123.457",
                "T12345678",
                "C12345678",
                "123.456",
            )["status"],
            "completed",
        )
        self.assertTrue(self.store.has_ingress("123.457"))
        recent = self.store.recent_participating_threads()
        self.assertIn(
            ("T12345678", "C12345678", "123.456"),
            [item[:3] for item in recent],
        )
        self.assertIsInstance(recent[0][3], float)

    def test_thread_ingress_lease_recovers_failures_without_parallel_dispatch(self):
        identity = (
            "slack:T12345678:C12345678:123.457",
            "T12345678",
            "C12345678",
            "123.456",
        )
        first = self.store.claim_thread_ingress(*identity)
        self.assertEqual(first["status"], "claimed")
        self.assertEqual(
            self.store.claim_thread_ingress(*identity)["status"],
            "busy",
        )
        self.assertTrue(
            self.store.renew_thread_ingress(
                identity[0],
                first["lease_id"],
            )
        )
        self.assertTrue(
            self.store.release_thread_ingress(
                identity[0],
                first["lease_id"],
                "dispatch_failed",
            )
        )
        second = self.store.claim_thread_ingress(*identity)
        self.assertEqual(second["status"], "claimed")
        self.assertNotEqual(second["lease_id"], first["lease_id"])
        self.assertTrue(
            self.store.complete_thread_ingress(
                identity[0],
                second["lease_id"],
            )
        )
        self.assertEqual(
            self.store.claim_thread_ingress(*identity)["status"],
            "completed",
        )

    def test_retention_prunes_only_terminal_records_and_keeps_active_binding(self):
        bridge = self.store.bind(
            self.store.create(self.request("retention")).bridge_id,
            "123.456",
        )
        with self.store.connect() as db:
            db.execute(
                """
                INSERT INTO bridge_events(
                  event_id,bridge_id,state,payload_json,created_at,updated_at
                ) VALUES(
                  'old-done',?,'delivered','{"text":"private"}',
                  '2020-01-01','2020-01-01'
                )
                """,
                (bridge.bridge_id,),
            )
            db.execute(
                """
                INSERT INTO bridge_events(
                  event_id,bridge_id,state,payload_json,created_at,updated_at
                ) VALUES(
                  'old-open',?,'uncertain','{"text":"retain"}',
                  '2020-01-01','2020-01-01'
                )
                """,
                (bridge.bridge_id,),
            )
            db.execute(
                """
                INSERT INTO thread_ingress(
                  event_id,team_id,channel_id,thread_ts,state,
                  created_at,updated_at
                ) VALUES(
                  'old-thread','T12345678','C12345678','123.456',
                  'completed','2020-01-01','2020-01-01'
                )
                """
            )
        counts = self.store.prune(retention_days=30)
        self.assertEqual(counts["bridge_events"], 1)
        self.assertEqual(counts["thread_ingress"], 1)
        self.assertIsNotNone(self.store.get(bridge.bridge_id))
        with self.store.connect() as db:
            rows = db.execute(
                "SELECT event_id FROM bridge_events ORDER BY event_id"
            ).fetchall()
        self.assertEqual([row[0] for row in rows], ["old-open"])

    def test_concurrent_idempotent_notifications_post_one_root_message(self):
        broker = self.runtime.Broker("test-token", self.store, verified_workspace_team_id="T12345678")
        request = {
            "op": "notify", "text": "finished", "source_kind": "headless_run",
            "source": {"run_id": "run-concurrent", "cwd": "/tmp/project"},
            "idempotency_key": "run-concurrent",
        }
        barrier = threading.Barrier(2)

        def notify():
            barrier.wait()
            return broker.handle(dict(request))

        with mock.patch.dict(os.environ, {
            "SLACK_HOME_CHANNEL": "C12345678",
            "SLACK_ALLOWED_USERS": "U12345678",
        }, clear=False), mock.patch.object(
            broker, "_ensure_channel_membership"
        ), mock.patch.object(self.runtime, "slack_post", return_value="123.456") as post:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda _: notify(), range(2)))

        self.assertEqual(post.call_count, 1)
        self.assertEqual({result["thread_ts"] for result in results}, {"123.456"})
        self.assertEqual(sorted(result["deduplicated"] for result in results), [False, True])

    def test_same_session_can_create_and_route_multiple_root_threads(self):
        broker = self.runtime.Broker(
            "test-token",
            self.store,
            verified_workspace_team_id="T12345678",
        )
        base = {
            "op": "notify",
            "source_kind": "headless_run",
            "source": {"run_id": "shared-root-session", "cwd": "/tmp/project"},
            "channel_id": "C12345678",
            "owner_user_id": "*",
        }
        timestamps = iter(("123.456", "123.789"))
        with mock.patch.dict(
            os.environ,
            {"SLACK_ALLOWED_USERS": "U12345678"},
            clear=False,
        ), mock.patch.object(
            broker,
            "_ensure_channel_membership",
        ), mock.patch.object(
            self.runtime,
            "slack_post",
            side_effect=lambda *_args, **_kwargs: next(timestamps),
        ) as post:
            first = broker.handle({
                **base,
                "text": "first independent update",
                "idempotency_key": "shared-root-first",
            })
            second = broker.handle({
                **base,
                "text": "second independent update",
                "idempotency_key": "shared-root-second",
            })

        self.assertEqual(post.call_count, 2)
        self.assertNotEqual(first["bridge_id"], second["bridge_id"])
        self.assertEqual(
            self.store.find_thread("T12345678", "C12345678", "123.456").bridge_id,
            first["bridge_id"],
        )
        self.assertEqual(
            self.store.find_thread("T12345678", "C12345678", "123.789").bridge_id,
            second["bridge_id"],
        )


class CredentialBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        cwd_identity = self.runtime.working_directory_identity(str(self.home))
        self.bridge = self.runtime.Bridge(
            "brg_test", "codex_session",
            {"session_id": "session-1", **cwd_identity},
            "U12345678", "T12345678", "C12345678", "123.456", "key", "active",
            1, 2, "verified", "",
        )

    def tearDown(self):
        self.temp.cleanup()

    def test_gateway_secrets_are_absent_from_native_environment(self):
        config = self.runtime.Config()
        captured = {}

        class Process:
            returncode = 0
            pid = 12345

            def __init__(self, command, **kwargs):
                captured["command"] = command
                captured["env"] = kwargs["env"]

        def collect(_process, prompt, _deadline, _cancel_event):
            captured["input"] = prompt
            return b"native answer", b"", False

        with mock.patch.dict(os.environ, {
            "SLACK_BOT_TOKEN": "not-forwarded",
            "OP_SERVICE_ACCOUNT_TOKEN": "not-forwarded",
            "HOME": str(self.home),
            "PATH": os.environ.get("PATH", ""),
        }, clear=False), mock.patch.object(self.runtime, "load_config", return_value=config), mock.patch.object(
            self.runtime, "_resolve_executable", return_value="/usr/bin/codex"
        ), mock.patch.object(
            self.runtime.subprocess, "Popen", Process
        ), mock.patch.object(
            self.runtime, "_collect_native_output", side_effect=collect
        ):
            output = self.runtime.continue_native(self.bridge, "private operator prompt")

        self.assertEqual(output, "native answer")
        self.assertNotIn("private operator prompt", captured["command"])
        self.assertEqual(captured["command"][-1], "-")
        self.assertEqual(captured["input"], "private operator prompt")
        self.assertNotIn("SLACK_BOT_TOKEN", captured["env"])
        self.assertNotIn("OP_SERVICE_ACCOUNT_TOKEN", captured["env"])

    def test_credential_helper_is_allowlisted_and_silent(self):
        config = self.runtime.Config(
            credential_command=("/usr/bin/credential-helper",),
            credential_env_allowlist=("OPENAI_API_KEY", "OPENAI_BASE_URL"),
        )
        result = types.SimpleNamespace(returncode=0, stdout=json.dumps({"OPENAI_API_KEY": "short-lived"}))
        with mock.patch.object(
            self.runtime,
            "_resolve_credential_helper",
            return_value="/usr/bin/credential-helper",
        ), mock.patch.object(self.runtime.subprocess, "run", return_value=result) as run:
            values = self.runtime._credential_env(self.bridge, config)
        self.assertEqual(values, {"OPENAI_API_KEY": "short-lived"})
        self.assertEqual(run.call_args.kwargs["stderr"], subprocess.DEVNULL)
        self.assertNotIn("SLACK_BOT_TOKEN", run.call_args.kwargs["env"])

    def test_credential_helper_rejects_unlisted_or_slack_keys(self):
        for values, allowlist in (
            ({"AWS_SECRET_ACCESS_KEY": "x"}, ("OPENAI_API_KEY",)),
            ({"SLACK_BOT_TOKEN": "x"}, ("SLACK_BOT_TOKEN",)),
            ({"LD_PRELOAD": "/tmp/x.so"}, ("LD_PRELOAD",)),
        ):
            config = self.runtime.Config(
                credential_command=("/usr/bin/credential-helper",),
                credential_env_allowlist=allowlist,
            )
            result = types.SimpleNamespace(returncode=0, stdout=json.dumps(values))
            with mock.patch.object(
                self.runtime,
                "_resolve_credential_helper",
                return_value="/usr/bin/credential-helper",
            ), mock.patch.object(self.runtime.subprocess, "run", return_value=result):
                with self.assertRaises(self.runtime.NativeContinuationError):
                    self.runtime._credential_env(self.bridge, config)

    def test_credential_helper_requires_private_absolute_executable(self):
        helper = self.home / "credential-helper"
        helper.write_text("#!/bin/sh\nprintf '{}'\n", encoding="utf-8")
        helper.chmod(0o700)
        self.assertEqual(
            self.runtime._resolve_credential_helper(str(helper)),
            str(helper),
        )

        helper.chmod(0o722)
        with self.assertRaises(self.runtime.NativeContinuationError):
            self.runtime._resolve_credential_helper(str(helper))
        with self.assertRaises(self.runtime.NativeContinuationError):
            self.runtime._resolve_credential_helper("credential-helper")

    def test_doctor_executes_the_native_credential_helper(self):
        self.runtime.CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.runtime.CONFIG_PATH.write_text("config_version = 1\n", encoding="utf-8")
        self.runtime.CONFIG_PATH.chmod(0o600)
        helper = self.home / "credential-helper"
        helper.write_text("#!/bin/sh\nprintf '{}'\n", encoding="utf-8")
        helper.chmod(0o700)
        config = self.runtime.Config(credential_command=(str(helper),))

        with mock.patch.object(self.runtime, "load_config", return_value=config):
            _, checks = self.runtime.doctor()

        self.assertIn(
            "ok native continuation credential helper is executable and valid",
            checks,
        )

    def test_doctor_rejects_a_symlinked_native_credential_helper(self):
        self.runtime.CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.runtime.CONFIG_PATH.write_text("config_version = 1\n", encoding="utf-8")
        self.runtime.CONFIG_PATH.chmod(0o600)
        helper = self.home / "credential-helper"
        helper.write_text("#!/bin/sh\nprintf '{}'\n", encoding="utf-8")
        helper.chmod(0o700)
        linked_helper = self.home / "credential-helper-link"
        linked_helper.symlink_to(helper)
        config = self.runtime.Config(credential_command=(str(linked_helper),))

        with mock.patch.object(self.runtime, "load_config", return_value=config):
            ok, checks = self.runtime.doctor()

        self.assertFalse(ok)
        self.assertTrue(any(
            line.startswith("FAIL native continuation credential helper:")
            for line in checks
        ))

    def test_broker_refuses_invalid_credentials_before_opening_traffic(self):
        helper = self.home / "credential-helper"
        helper.write_text("#!/bin/sh\nprintf '{}'\n", encoding="utf-8")
        helper.chmod(0o700)
        linked_helper = self.home / "credential-helper-link"
        linked_helper.symlink_to(helper)
        config = self.runtime.Config(credential_command=(str(linked_helper),))
        socket_path = self.home / "broker" / "bridge.sock"

        with mock.patch.object(self.runtime, "load_config", return_value=config):
            with self.assertRaises(self.runtime.NativeContinuationError):
                self.runtime.start_broker("token", socket_path)

        self.assertFalse(socket_path.exists())

    def test_native_child_does_not_inherit_ambient_proxy_credentials(self):
        with mock.patch.dict(
            os.environ,
            {
                "HTTP_PROXY": "http://user:secret@proxy.example",
                "HTTPS_PROXY": "https://user:secret@proxy.example",
                "NO_PROXY": "localhost",
            },
            clear=False,
        ):
            child = self.runtime._base_child_env()
        self.assertNotIn("HTTP_PROXY", child)
        self.assertNotIn("HTTPS_PROXY", child)
        self.assertNotIn("NO_PROXY", child)

    def test_missing_configured_executable_fails_closed(self):
        with mock.patch.object(self.runtime.shutil, "which", return_value=None):
            with self.assertRaisesRegex(self.runtime.NativeContinuationError, "executable is unavailable"):
                self.runtime._resolve_executable("missing-agent-cli")

    def test_slack_egress_redacts_high_confidence_credentials(self):
        samples = (
            "xox" + "b-1234567890-abcdefghijklmnop",
            "xap" + "p-1-A1234567890-abcdefghijklmnop",
            "gh" + "p_abcdefghijklmnopqrstuvwxyz",
            "sk-abcdefghijklmnopqrstuvwxyz123456",
            "AKIAIOSFODNN7EXAMPLE",
            "-----BEGIN PRIVATE KEY-----\nsecret material\n-----END PRIVATE KEY-----",
        )
        redacted = self.runtime.redact_text("\n".join(samples))
        for sample in samples:
            self.assertNotIn(sample, redacted)
        self.assertGreaterEqual(redacted.count("[REDACTED_"), len(samples))

    def test_origin_is_sanitized_and_retained_at_maximum_message_size(self):
        bridge = self.runtime.Bridge(
            "brg_test", "zellij_pane",
            {"session_name": "work`\nspoof", "pane_id": "7", "cwd": "/tmp/project`\nspoof"},
            "U12345678", "T12345678", "C12345678", None, "key", "pending",
        )
        rendered = self.runtime.with_origin("x" * self.runtime.MAX_TEXT, bridge)
        self.assertLessEqual(len(rendered), self.runtime.MAX_TEXT)
        self.assertTrue(rendered.endswith("_"))
        self.assertIn("_Origin: Zellij `workspoof` / pane `7` · `projectspoof`_", rendered)

    def test_slack_transport_rejects_unapproved_api_methods(self):
        with self.assertRaisesRegex(ValueError, "unsupported Slack API method"):
            self.runtime._slack_call("hidden-token", "admin.users.list", {})

    def test_zellij_identity_requires_allowlisted_live_agent_and_returns_only_hash(self):
        panes = [{
            "id": 7,
            "is_plugin": False,
            "exited": False,
            "terminal_command": "node /opt/agents/codex --resume session-secret",
        }]
        completed = types.SimpleNamespace(stdout=json.dumps(panes))
        with mock.patch.object(self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"), mock.patch.object(
            self.runtime.subprocess, "run", return_value=completed
        ), mock.patch.object(
            self.runtime,
            "_trusted_agent_paths",
            return_value={"codex": {"/opt/agents/codex"}},
        ), mock.patch.object(
            self.runtime,
            "_zellij_agent_process",
            return_value=("codex", process_identity()),
        ):
            identity = self.runtime.zellij_pane_identity("work", "7", "/tmp/project")
        self.assertEqual(identity["pane_agent"], "codex")
        self.assertEqual(len(identity["pane_command_hash"]), 64)
        self.assertEqual(identity["process_identity"], process_identity())
        self.assertNotIn("session-secret", json.dumps(identity))

        panes[0]["terminal_command"] = "bash"
        completed = types.SimpleNamespace(stdout=json.dumps(panes))
        with mock.patch.object(self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"), mock.patch.object(
            self.runtime.subprocess, "run", return_value=completed
        ), mock.patch.object(
            self.runtime,
            "_trusted_agent_paths",
            return_value={"codex": {"/opt/agents/codex"}},
        ), mock.patch.object(
            self.runtime,
            "_zellij_agent_process",
            side_effect=self.runtime.NativeContinuationError(
                "captured Zellij pane is not running an allowlisted agent"
            ),
        ):
            with self.assertRaisesRegex(self.runtime.NativeContinuationError, "not running an allowlisted agent"):
                self.runtime.zellij_pane_identity("work", "7")

    def test_zellij_identity_resolves_old_pane_with_null_command(self):
        panes = [{
            "id": 31,
            "is_plugin": False,
            "exited": False,
            "terminal_command": None,
        }]
        completed = types.SimpleNamespace(stdout=json.dumps(panes))
        with mock.patch.object(
            self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"
        ), mock.patch.object(
            self.runtime.subprocess, "run", return_value=completed
        ), mock.patch.object(
            self.runtime,
            "_trusted_agent_paths",
            return_value={"claude": {"/opt/agents/claude"}},
        ), mock.patch.object(
            self.runtime,
            "_zellij_agent_process",
            return_value=("claude", "proc:3381024:39575982:command-hash"),
        ) as process_identity:
            identity = self.runtime.zellij_pane_identity(
                "didactic-jellyfish", "31", "/tmp/project"
            )
        process_identity.assert_called_once_with(
            "didactic-jellyfish",
            "31",
            {"claude", "codex", "gemini", "hermes", "pi"},
            metadata_agent="",
            trusted_paths={"claude": {"/opt/agents/claude"}},
        )
        self.assertEqual(identity["pane_agent"], "claude")
        self.assertEqual(len(identity["pane_command_hash"]), 64)

    def test_zellij_delivery_fails_closed_when_pane_process_changes(self):
        bridge = self.runtime.Bridge(
            "brg_test", "zellij_pane",
            {
                "session_name": "work", "pane_id": "7", "cwd": "/tmp/project",
                "pane_agent": "codex", "pane_command_hash": "expected",
                "process_identity": process_identity(),
            },
            "U12345678", "T12345678", "C12345678", "123.456", "key", "active",
            1, 2, "verified", "",
        )
        with mock.patch.object(
            self.runtime,
            "zellij_pane_identity",
            return_value={
                "pane_command_hash": "different",
                "process_identity": process_identity(pid=300, start="30000"),
            },
        ), mock.patch.object(self.runtime.subprocess, "run") as run:
            with self.assertRaisesRegex(
                self.runtime.NativeContinuationError, "different process"
            ) as changed:
                self.runtime.deliver_zellij(bridge, "continue")
        self.assertEqual(changed.exception.code, "process_identity_changed")
        self.assertEqual(changed.exception.binding_id, "brg_test")
        self.assertEqual(changed.exception.status, "stale")
        run.assert_not_called()

    def test_native_zellij_delivery_verifies_visible_input_and_live_agent_after_enter(self):
        bridge = self.runtime.Bridge(
            "brg_test", "claude_session",
            {
                "session_id": "session-1", "zellij_session": "work",
                "zellij_pane_id": "7", "cwd": "/tmp/project",
                "pane_agent": "claude", "pane_command_hash": "expected",
                "process_identity": process_identity(agent="claude"),
            },
            "*", "T12345678", "C12345678", "123.456", "key", "active",
            1, 2, "verified", "",
        )
        text = "review AJ's correction"
        marker = "att_legacytest1234567890"
        inbox_dir = self.home / ".local" / "share" / "tether" / "inbox"
        inbox_dir.mkdir(parents=True)
        stale = inbox_dir / "att_stalehandoff1234567890.txt"
        stale.write_text("stale request")
        stale.chmod(0o600)
        os.utime(stale, (0, 0))

        staged_instruction = ""
        dump_snapshots = []

        def run(command, **_kwargs):
            nonlocal staged_instruction
            if "write-chars" in command:
                staged_instruction += command[-1]
            if "dump-screen" in command:
                dump_snapshots.append(staged_instruction)
                return types.SimpleNamespace(
                    stdout=staged_instruction,
                    stderr="",
                    returncode=0,
                )
            return types.SimpleNamespace(stdout="", stderr="", returncode=0)

        with mock.patch.object(
            self.runtime, "zellij_pane_identity",
            return_value={
                "pane_command_hash": "expected",
                "process_identity": process_identity(agent="claude"),
            },
        ) as identity, mock.patch.object(self.runtime.subprocess, "run", side_effect=run) as invoked, mock.patch.object(
            self.runtime.time, "sleep"
        ), mock.patch.object(self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"):
            self.runtime.deliver_zellij(bridge, text, marker)

        commands = [call.args[0] for call in invoked.call_args_list]
        self.assertTrue(any("write-chars" in command for command in commands))
        self.assertTrue(any("send-keys" in command and "Enter" in command for command in commands))
        dump_commands = [
            command for command in commands if "dump-screen" in command
        ]
        self.assertEqual(len(dump_commands), 2)
        self.assertNotIn("--full", dump_commands[0])
        self.assertIn("--full", dump_commands[1])
        self.assertGreaterEqual(identity.call_count, 2)
        written = "".join(
            command[-1] for command in commands
            if "write-chars" in command
        )
        self.assertGreater(len([
            command for command in commands if "write-chars" in command
        ]), 1)
        self.assertTrue(all(
            command[-2] == "--"
            for command in commands if "write-chars" in command
        ))
        self.assertEqual(len(dump_snapshots), 2)
        self.assertLessEqual(
            len(dump_snapshots[0]),
            self.runtime.ZELLIJ_WRITE_CHUNK_CHARS,
        )
        self.assertIn(marker, dump_snapshots[0])
        self.assertEqual(dump_snapshots[1], written)
        self.assertIn("--reply-key " + marker, written)
        self.assertIn("bridge brg_test", written)
        self.assertIn("thread C12345678/123.456", written)
        self.assertIn(" thread --channel C12345678 --thread-ts 123.456", written)
        self.assertIn("only to that exact Slack thread", written)
        self.assertIn("at most one Slack message", written)
        self.assertIn("Default to 50 words", written)
        self.assertIn("--text-stdin", written)
        self.assertNotIn("--text '", written)
        self.assertGreaterEqual(identity.call_count, 3)
        inbox = self.home / ".local" / "share" / "tether" / "inbox" / f"{marker}.txt"
        self.assertEqual(inbox.read_text().strip(), text)
        self.assertEqual(inbox.stat().st_mode & 0o777, 0o600)
        self.assertFalse(stale.exists())

    def test_zellij_delivery_rechecks_process_before_enter(self):
        bridge = self.runtime.Bridge(
            "brg_test", "claude_session",
            {
                "session_id": "session-1", "zellij_session": "work",
                "zellij_pane_id": "7", "cwd": "/tmp/project",
                "pane_agent": "claude", "pane_command_hash": "expected",
                "process_identity": process_identity(agent="claude"),
            },
            "*", "T12345678", "C12345678", "123.456", "key", "active",
            1, 2, "verified", "",
        )
        expected = {
            "process_identity": process_identity(agent="claude"),
        }
        changed = {
            "process_identity": process_identity(
                agent="claude",
                pid=301,
                start="30100",
            ),
        }
        marker = "att_preenterrace123456789"

        def run(command, **_kwargs):
            if "dump-screen" in command:
                return types.SimpleNamespace(
                    stdout=f"prompt contains {marker}",
                    stderr="",
                    returncode=0,
                )
            return types.SimpleNamespace(stdout="", stderr="", returncode=0)

        with mock.patch.object(
            self.runtime,
            "zellij_pane_identity",
            side_effect=(expected, changed),
        ), mock.patch.object(
            self.runtime.subprocess,
            "run",
            side_effect=run,
        ) as invoked, mock.patch.object(
            self.runtime.time,
            "sleep",
        ), mock.patch.object(
            self.runtime,
            "_resolve_executable",
            return_value="/usr/bin/zellij",
        ):
            with self.assertRaises(
                self.runtime.NativeContinuationError
            ) as raised:
                self.runtime.deliver_zellij(bridge, "continue", marker)

        self.assertEqual(raised.exception.code, "terminal_submit_uncertain")
        commands = [call.args[0] for call in invoked.call_args_list]
        self.assertTrue(any("write-chars" in command for command in commands))
        self.assertFalse(
            any(
                "send-keys" in command and "Enter" in command
                for command in commands
            )
        )

    def test_zellij_delivery_refuses_existing_symlink_without_truncating_target(self):
        marker = "att_symlinktest123456789"
        target = self.home / "target.txt"
        target.write_text("preserve me", encoding="utf-8")
        target.chmod(0o600)
        inbox_dir = self.home / ".local" / "share" / "tether" / "inbox"
        inbox_dir.mkdir(mode=0o700, parents=True)
        (inbox_dir / f"{marker}.txt").symlink_to(target)
        bridge = self.runtime.Bridge(
            "brg_test",
            "claude_session",
            {
                "session_id": "session-1",
                "zellij_session": "work",
                "zellij_pane_id": "7",
                "cwd": "/tmp/project",
                "pane_agent": "claude",
                "pane_command_hash": "expected",
                "process_identity": process_identity(agent="claude"),
            },
            "*",
            "T12345678",
            "C12345678",
            "123.456",
            "key",
            "active",
            1,
            2,
            "verified",
            "",
        )
        with mock.patch.object(
            self.runtime,
            "zellij_pane_identity",
            return_value={
                "pane_command_hash": "expected",
                "process_identity": process_identity(agent="claude"),
            },
        ), mock.patch.object(self.runtime.subprocess, "run") as invoked:
            with self.assertRaises(self.runtime.security.StatePathError):
                self.runtime.deliver_zellij(bridge, "replacement", marker)
        invoked.assert_not_called()
        self.assertEqual(target.read_text(encoding="utf-8"), "preserve me")


class NativeBindingContractTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")

    def tearDown(self):
        self.temp.cleanup()

    def _write_process(
        self,
        proc_root,
        *,
        pid,
        parent_pid,
        start_time,
        executable,
        argv,
        session="didactic-jellyfish",
        pane="51",
    ):
        executable_path = pathlib.Path(executable)
        if not executable_path.exists():
            executable_path.parent.mkdir(parents=True, exist_ok=True)
            executable_path.write_text("#!/bin/sh\n")
            executable_path.chmod(0o700)
        process = proc_root / str(pid)
        process.mkdir(parents=True)
        (process / "environ").write_bytes(
            f"ZELLIJ_SESSION_NAME={session}\0ZELLIJ_PANE_ID={pane}\0".encode()
        )
        (process / "cmdline").write_bytes(
            b"\0".join(part.encode() for part in argv) + b"\0"
        )
        # Both processes are foreground leaders of nested PTYs. Executable
        # identity, not argv token matching, must distinguish the real agent.
        stat_fields = [
            "S",
            str(parent_pid),
            str(pid),
            str(pid),
            "34851",
            str(pid),
            *(["0"] * 13),
            str(start_time),
        ]
        (process / "stat").write_text(
            f"{pid} ({pathlib.Path(executable).name}) " + " ".join(stat_fields)
        )
        (process / "exe").symlink_to(executable_path)

    def _zellij_bridge(
        self,
        pane_hash="expected",
        exact_process_identity=None,
    ):
        exact_process_identity = exact_process_identity or process_identity(
            session="didactic-jellyfish", pane="51"
        )
        return self.runtime.Bridge(
            "brg_test",
            "codex_session",
            {
                "session_id": "codex-session",
                "zellij_session": "didactic-jellyfish",
                "zellij_pane_id": "51",
                "cwd": "/tmp/project",
                "pane_agent": "codex",
                "pane_command_hash": pane_hash,
                "process_identity": exact_process_identity,
                "binding_version": "2",
                "binding_state": "verified",
                "endpoint_kind": "zellij_pane",
                "delivery_policy": "native_required",
            },
            "*",
            "T12345678",
            "C12345678",
            "123.456",
            "key",
            "active",
            1,
            2,
            "verified",
            "",
        )

    def test_null_command_resolver_selects_nested_agent_executable(self):
        proc_root = self.home / "proc"
        proc_root.mkdir()
        broker_python = str(self.home / "usr" / "bin" / "python3")
        self._write_process(
            proc_root,
            pid=100,
            parent_pid=1,
            start_time=10_000,
            executable=broker_python,
            argv=(
                broker_python,
                "-m",
                "agent_observatory.broker_launch",
                "--runtime",
                "codex",
                "--executable",
                "/opt/codex/bin/codex",
            ),
        )
        self._write_process(
            proc_root,
            pid=200,
            parent_pid=100,
            start_time=20_000,
            executable=str(self.home / "opt" / "codex" / "bin" / "codex"),
            argv=(
                str(self.home / "opt" / "codex" / "bin" / "codex"),
                "exec",
                "resume",
                "session-1",
            ),
        )
        panes = [{
            "id": 51,
            "is_plugin": False,
            "exited": False,
            "terminal_command": None,
        }]
        completed = types.SimpleNamespace(stdout=json.dumps(panes))
        original_resolver = self.runtime._zellij_agent_process
        resolved = {}

        codex = str(self.home / "opt" / "codex" / "bin" / "codex")

        def resolve(
            session,
            pane,
            allowed,
            metadata_agent="",
            trusted_paths=None,
        ):
            result = original_resolver(
                session,
                pane,
                allowed,
                proc_root,
                metadata_agent=metadata_agent or "codex",
                trusted_paths=trusted_paths,
            )
            resolved["agent"], resolved["descriptor"] = result
            return result

        with mock.patch.object(
            self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"
        ), mock.patch.object(
            self.runtime.subprocess, "run", return_value=completed
        ), mock.patch.object(
            self.runtime,
            "_trusted_agent_paths",
            return_value={"codex": {codex}},
        ), mock.patch.object(
            self.runtime, "_zellij_agent_process", side_effect=resolve
        ):
            identity = self.runtime.zellij_pane_identity(
                "didactic-jellyfish", "51", "/tmp/project"
            )

        self.assertEqual(identity["pane_agent"], "codex")
        self.assertEqual(resolved["agent"], "codex")
        self.assertTrue(
            resolved["descriptor"].startswith(self.runtime.PROCESS_IDENTITY_PREFIX)
        )
        descriptor = json.loads(
            resolved["descriptor"].removeprefix(
                self.runtime.PROCESS_IDENTITY_PREFIX
            )
        )
        self.assertEqual(descriptor["pid"], 200)
        self.assertEqual(descriptor["start"], "20000")
        self.assertEqual(
            descriptor["exe_path"],
            hashlib.sha256(
                str(self.home / "opt" / "codex" / "bin" / "codex").encode()
            ).hexdigest()[:16],
        )
        self.assertEqual(identity["process_identity"], resolved["descriptor"])
        self.assertEqual(
            identity["pane_command_hash"],
            hashlib.sha256(resolved["descriptor"].encode()).hexdigest(),
        )

    def test_process_resolver_uses_non_agent_ancestors_to_select_nested_agent(self):
        proc_root = self.home / "proc"
        proc_root.mkdir()
        codex = str(self.home / "bin" / "codex")
        self._write_process(
            proc_root,
            pid=100,
            parent_pid=1,
            start_time=10_000,
            executable=codex,
            argv=(codex, "exec", "resume", "outer-session"),
        )
        self._write_process(
            proc_root,
            pid=150,
            parent_pid=100,
            start_time=15_000,
            executable="/usr/bin/bash",
            argv=("/usr/bin/bash", "-l"),
        )
        self._write_process(
            proc_root,
            pid=200,
            parent_pid=150,
            start_time=20_000,
            executable=codex,
            argv=(codex, "exec", "resume", "inner-session"),
        )

        agent, descriptor = self.runtime._zellij_agent_process(
            "didactic-jellyfish",
            "51",
            {"codex"},
            proc_root,
            metadata_agent="codex",
            trusted_paths={"codex": {str(pathlib.Path(codex).resolve())}},
        )

        self.assertEqual(agent, "codex")
        payload = json.loads(
            descriptor.removeprefix(self.runtime.PROCESS_IDENTITY_PREFIX)
        )
        self.assertEqual(payload["pid"], 200)
        self.assertEqual(payload["start"], "20000")

    def test_same_command_with_new_process_identity_is_stale(self):
        panes = [{
            "id": 51,
            "is_plugin": False,
            "exited": False,
            "terminal_command": "/opt/codex/bin/codex exec resume session-1",
        }]
        completed = types.SimpleNamespace(stdout=json.dumps(panes))
        original_identity = process_identity(
            pid=200,
            start="20000",
            session="didactic-jellyfish",
            pane="51",
        )
        replacement_identity = process_identity(
            pid=300,
            start="30000",
            session="didactic-jellyfish",
            pane="51",
        )
        with mock.patch.object(
            self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"
        ), mock.patch.object(
            self.runtime.subprocess, "run", return_value=completed
        ), mock.patch.object(
            self.runtime,
            "_trusted_agent_paths",
            return_value={"codex": {"/opt/codex/bin/codex"}},
        ), mock.patch.object(
            self.runtime,
            "_zellij_agent_process",
            side_effect=(
                ("codex", original_identity),
                ("codex", replacement_identity),
            ),
        ) as resolver_mock:
            original = self.runtime.zellij_pane_identity(
                "didactic-jellyfish", "51", "/tmp/project"
            )
            replacement = self.runtime.zellij_pane_identity(
                "didactic-jellyfish", "51", "/tmp/project"
            )

        self.assertEqual(resolver_mock.call_count, 2)
        self.assertNotEqual(
            original["process_identity"],
            replacement["process_identity"],
            "same command text must not make a replacement process look current",
        )
        self.assertNotEqual(
            original["pane_command_hash"], replacement["pane_command_hash"]
        )
        bridge = self._zellij_bridge(
            original["pane_command_hash"], original["process_identity"]
        )
        with mock.patch.object(
            self.runtime, "zellij_pane_identity", return_value=replacement
        ), mock.patch.object(self.runtime.subprocess, "run") as run:
            with self.assertRaisesRegex(
                self.runtime.NativeContinuationError, "different process|stale"
            ):
                self.runtime.deliver_zellij(bridge, "continue")
        run.assert_not_called()

    def test_delivery_waits_for_attempt_specific_acknowledgement(self):
        exact_process_identity = process_identity(
            session="didactic-jellyfish", pane="51"
        )
        request = {
            "source_kind": "codex_session",
            "source": {
                "session_id": "codex-session",
                "zellij_session": "didactic-jellyfish",
                "zellij_pane_id": "51",
                "cwd": "/tmp/project",
                "pane_agent": "codex",
                "pane_command_hash": "expected",
                "process_identity": exact_process_identity,
                "binding_version": "2",
                "binding_state": "verified",
                "endpoint_kind": "zellij_pane",
                "delivery_policy": "native_required",
            },
            "owner_user_id": "*",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "ack-contract",
        }
        bridge = self.store.bind(self.store.create(request).bridge_id, "123.456")
        self.assertEqual(
            bridge.source.get("process_identity"),
            exact_process_identity,
            "Store must persist the process identity required to acknowledge this binding",
        )
        self.assertTrue(self.store.enqueue_event("111.1", bridge.bridge_id, "status?"))
        self.assertEqual(self.store.claim_event_batch(bridge.bridge_id)[0]["event_id"], "111.1")

        first_key = self.runtime.delivery_attempt_id(
            bridge.bridge_id, ("111.1",), bridge.binding_generation
        )
        second_key = self.runtime.delivery_attempt_id(
            bridge.bridge_id, ("111.2",), bridge.binding_generation
        )
        self.assertNotEqual(
            first_key,
            second_key,
            "identical message text in separate events must receive separate acknowledgements",
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                ["111.1"],
                bridge.bridge_id,
                bridge.binding_generation,
                first_key,
            )
        )
        self.assertEqual(
            self.store.attempt_state(first_key, bridge.bridge_id),
            "prepared",
            "prepare the generation-bound attempt before terminal I/O starts",
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                first_key,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )

        staged_instruction = ""

        def run(command, **_kwargs):
            nonlocal staged_instruction
            if "write-chars" in command:
                staged_instruction += command[-1]
            if "dump-screen" in command:
                return types.SimpleNamespace(
                    stdout=staged_instruction, stderr="", returncode=0
                )
            return types.SimpleNamespace(stdout="", stderr="", returncode=0)

        with mock.patch.object(
            self.runtime,
            "zellij_pane_identity",
            return_value={
                "pane_command_hash": "expected",
                "process_identity": exact_process_identity,
            },
        ), mock.patch.object(
            self.runtime.subprocess, "run", side_effect=run
        ), mock.patch.object(
            self.runtime.time, "sleep"
        ), mock.patch.object(
            self.runtime, "_resolve_executable", return_value="/usr/bin/zellij"
        ):
            self.runtime.deliver_zellij(
                bridge, "status?", attempt_id=first_key
            )

        self.assertEqual(
            self.store.attempt_state(first_key, bridge.bridge_id),
            "submitting",
            "terminal I/O must not complete or acknowledge the durable attempt",
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                first_key,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        with self.store.connect() as db:
            state = db.execute(
                "SELECT state FROM bridge_events WHERE event_id='111.1'"
            ).fetchone()[0]
        self.assertEqual(
            state,
            "awaiting_ack",
            "write-chars and Enter prove submission, not agent acceptance",
        )
        self.assertFalse(
            self.store.acknowledge_attempt("wrong-attempt", bridge.bridge_id)
        )
        with self.store.connect() as db:
            state = db.execute(
                "SELECT state FROM bridge_events WHERE event_id='111.1'"
            ).fetchone()[0]
        self.assertEqual(state, "awaiting_ack")
        self.assertTrue(
            self.store.acknowledge_attempt(first_key, bridge.bridge_id)
        )
        with self.store.connect() as db:
            state = db.execute(
                "SELECT state FROM bridge_events WHERE event_id='111.1'"
            ).fetchone()[0]
        self.assertEqual(state, "delivered")


class NotifierTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        data = self.home / "data" / "tether"
        data.mkdir(parents=True)
        shutil.copy2(RUNTIME_PATH, data / "bridge_runtime.py")
        shutil.copy2(SECURITY_PATH, data / "security.py")
        shutil.copy2(HERMES_COMPAT_PATH, data / "hermes_compat.py")
        shutil.copy2(ROUTING_PATH, data / "routing.py")
        shutil.copy2(SLACK_PROTOCOL_PATH, data / "slack_protocol.py")
        env = {
            "HOME": str(self.home),
            "HERMES_HOME": str(self.home / ".hermes"),
            "XDG_DATA_HOME": str(self.home / "data"),
            "XDG_CONFIG_HOME": str(self.home / "config"),
            "HERDR_ENV": "",
            "HERDR_SESSION": "",
            "HERDR_SOCKET_PATH": "",
            "HERDR_PANE_ID": "",
            "HERDR_TAB_ID": "",
            "HERDR_WORKSPACE_ID": "",
        }
        self.env_patch = mock.patch.dict(os.environ, env, clear=False)
        self.env_patch.start()
        sys.modules.pop("bridge_runtime", None)
        spec = importlib.util.spec_from_file_location(f"notifier_test_{id(self)}", NOTIFIER_PATH)
        self.notifier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.notifier)

    def tearDown(self):
        self.env_patch.stop()
        sys.modules.pop("bridge_runtime", None)
        self.temp.cleanup()

    def _setup_runner(
        self,
        *,
        tether: str = "disabled",
        legacy: str = "absent",
        config: dict[str, str] | None = None,
        fail_on: tuple[str, ...] | None = None,
        fail_code: int = 74,
        raise_on: tuple[str, ...] | None = None,
    ):
        plugins = {"tether": tether, "session-bridge": legacy}
        values = dict(config or {})

        def completed(returncode=0, stdout="", stderr=""):
            return types.SimpleNamespace(
                returncode=returncode,
                stdout=stdout,
                stderr=stderr,
            )

        def run(command, **_kwargs):
            operation = tuple(command[1:])
            if raise_on == operation:
                raise subprocess.TimeoutExpired(command, 60)
            if fail_on == operation:
                return completed(fail_code)
            if operation == ("plugins", "list", "--plain"):
                lines = []
                for name in ("tether", "session-bridge"):
                    state = plugins[name]
                    if state == "enabled":
                        lines.append(f"enabled user 0.2.0 {name}")
                    elif state == "disabled":
                        lines.append(f"not enabled user 0.2.0 {name}")
                return completed(stdout="\n".join(lines) + ("\n" if lines else ""))
            if operation[:2] == ("plugins", "enable"):
                plugins[operation[2]] = "enabled"
                return completed()
            if operation[:2] == ("plugins", "disable"):
                if plugins.get(operation[2]) != "absent":
                    plugins[operation[2]] = "disabled"
                return completed()
            if operation[:2] == ("config", "get"):
                key = operation[2]
                if key in values:
                    return completed(stdout=f"{values[key]}\n")
                return completed(1, stderr=f"Config key not set: {key}\n")
            if operation[:2] == ("config", "set"):
                values[operation[2]] = operation[3]
                return completed()
            if operation[:2] == ("config", "unset"):
                values.pop(operation[2], None)
                return completed()
            return completed()

        return run, plugins, values

    def test_explicit_run_id_cannot_replace_ambient_native_session(self):
        args = types.SimpleNamespace(run_id="cron-2026", hermes_session_id=None)
        with mock.patch.dict(os.environ, {
            "CLAUDE_CODE_SESSION_ID": "claude-session",
            "CODEX_THREAD_ID": "codex-session",
        }, clear=False):
            with self.assertRaisesRegex(
                SystemExit,
                "cannot replace an active",
            ):
                self.notifier.detected_source(args)

    def test_explicit_hermes_id_cannot_replace_ambient_native_session(self):
        args = types.SimpleNamespace(
            run_id=None,
            hermes_session_id="hermes-session",
        )
        with mock.patch.dict(
            os.environ,
            {"CODEX_THREAD_ID": "codex-session"},
            clear=False,
        ):
            with self.assertRaisesRegex(
                SystemExit,
                "cannot replace an active",
            ):
                self.notifier.detected_source(args)

    def test_explicit_run_id_is_accepted_without_native_context(self):
        args = types.SimpleNamespace(run_id="cron-2026", hermes_session_id=None)
        with mock.patch.dict(
            os.environ,
            {
                "CLAUDE_CODE_SESSION_ID": "",
                "CODEX_THREAD_ID": "",
                "ZELLIJ_SESSION_NAME": "",
                "ZELLIJ_PANE_ID": "",
            },
            clear=False,
        ):
            kind, source = self.notifier.detected_source(args)
        self.assertEqual(kind, "headless_run")
        self.assertEqual(source["run_id"], "cron-2026")

    def test_both_ambient_native_ids_use_the_bound_pane_agent(self):
        args = types.SimpleNamespace(run_id=None, hermes_session_id=None)
        for pane_agent, expected_kind, expected_session in (
            ("claude", "claude_session", "claude-session"),
            ("codex", "codex_session", "codex-session"),
        ):
            identity = {
                "session_name": "work",
                "pane_id": "7",
                "cwd": str(pathlib.Path.cwd()),
                "pane_agent": pane_agent,
                "pane_command_hash": "process-fingerprint",
                "process_identity": process_identity(
                    agent=pane_agent,
                    session="work",
                    pane="7",
                ),
            }
            with self.subTest(pane_agent=pane_agent):
                with mock.patch.dict(
                    os.environ,
                    {
                        "CLAUDE_CODE_SESSION_ID": "claude-session",
                        "CODEX_THREAD_ID": "codex-session",
                        "ZELLIJ_SESSION_NAME": "work",
                        "ZELLIJ_PANE_ID": "7",
                    },
                    clear=True,
                ), mock.patch.object(
                    self.notifier, "zellij_pane_identity", return_value=identity
                ):
                    kind, source = self.notifier.detected_source(args)
                self.assertEqual(kind, expected_kind)
                self.assertEqual(source["session_id"], expected_session)
                self.assertEqual(source["pane_agent"], pane_agent)

    def test_both_ambient_native_ids_without_a_matching_pane_fail(self):
        args = types.SimpleNamespace(run_id=None, hermes_session_id=None)
        scenarios = (
            ({}, None),
            (
                {
                    "ZELLIJ_SESSION_NAME": "work",
                    "ZELLIJ_PANE_ID": "7",
                },
                {
                    "session_name": "work",
                    "pane_id": "7",
                    "cwd": str(pathlib.Path.cwd()),
                    "pane_agent": "gemini",
                    "pane_command_hash": "process-fingerprint",
                    "process_identity": process_identity(
                        agent="gemini",
                        session="work",
                        pane="7",
                    ),
                },
            ),
        )
        for terminal_env, identity in scenarios:
            environment = {
                "CLAUDE_CODE_SESSION_ID": "claude-session",
                "CODEX_THREAD_ID": "codex-session",
                **terminal_env,
            }
            with self.subTest(terminal=bool(terminal_env)), mock.patch.dict(
                os.environ, environment, clear=True
            ), mock.patch.object(
                self.notifier,
                "zellij_pane_identity",
                return_value=identity,
            ):
                with self.assertRaisesRegex(
                    SystemExit,
                    "(?i)(ambiguous|both|does not match|ambient native session)",
                ):
                    self.notifier.detected_source(args)

    def test_native_session_keeps_zellij_metadata(self):
        args = types.SimpleNamespace(run_id=None, hermes_session_id=None)
        identity = {
            "session_name": "work", "pane_id": "7", "cwd": str(pathlib.Path.cwd()),
            "pane_agent": "claude", "pane_command_hash": "abc123",
            "process_identity": process_identity(agent="claude"),
        }
        with mock.patch.dict(os.environ, {
            "CLAUDE_CODE_SESSION_ID": "claude-session",
            "ZELLIJ_SESSION_NAME": "work",
            "ZELLIJ_PANE_ID": "7",
        }, clear=True), mock.patch.object(self.notifier, "zellij_pane_identity", return_value=identity):
            kind, source = self.notifier.detected_source(args)
        self.assertEqual(kind, "claude_session")
        self.assertEqual(source["zellij_session"], "work")
        self.assertEqual(source["zellij_pane_id"], "7")
        self.assertEqual(source["pane_command_hash"], "abc123")
        self.assertEqual(source["process_identity"], process_identity(agent="claude"))

    def test_herdr_official_session_is_sufficient_for_native_capture(self):
        args = types.SimpleNamespace(run_id=None, hermes_session_id=None)
        identity = {
            "herdr_session": "pilot",
            "herdr_socket_path": str(self.home / "herdr.sock"),
            "herdr_terminal_id": "term_6583153c2a1b81",
            "herdr_pane_id": "w1:p1",
            "herdr_agent_name": "tether_0123456789abcdef",
            "herdr_agent_session_source": "codex_notify",
            "herdr_agent_session_kind": "thread_id",
            "herdr_agent_session_value": "codex-session",
            "herdr_protocol": "19",
            "native_session_id": "codex-session",
            "pane_agent": "codex",
            "process_identity": "herdr-proc-v1:exact",
        }
        with mock.patch.dict(
            os.environ,
            {
                "HERDR_ENV": "1",
                "HERDR_SESSION": "pilot",
                "HERDR_SOCKET_PATH": str(self.home / "herdr.sock"),
                "HERDR_PANE_ID": "w1:p1",
            },
            clear=True,
        ), mock.patch.object(
            self.notifier,
            "herdr_agent_identity",
            return_value=identity,
        ) as capture:
            kind, source = self.notifier.detected_source(args)
        self.assertEqual(kind, "codex_session")
        self.assertEqual(source["session_id"], "codex-session")
        self.assertEqual(source["herdr_terminal_id"], "term_6583153c2a1b81")
        self.assertNotIn("native_session_id", source)
        capture.assert_called_once_with(
            str(self.home / "herdr.sock"),
            "w1:p1",
            "pilot",
            str(pathlib.Path.cwd()),
        )

    def test_default_herdr_session_does_not_require_session_environment(self):
        args = types.SimpleNamespace(run_id=None, hermes_session_id=None)
        identity = {
            "herdr_session": "default",
            "herdr_socket_path": str(self.home / "herdr.sock"),
            "herdr_terminal_id": "term_6583153c2a1b81",
            "herdr_pane_id": "w1:p1",
            "herdr_agent_name": "tether_0123456789abcdef",
            "herdr_agent_session_source": "codex_notify",
            "herdr_agent_session_kind": "thread_id",
            "herdr_agent_session_value": "codex-session",
            "herdr_protocol": "19",
            "native_session_id": "codex-session",
            "pane_agent": "codex",
            "process_identity": "herdr-proc-v1:exact",
        }
        with mock.patch.dict(
            os.environ,
            {
                "HERDR_ENV": "1",
                "HERDR_SOCKET_PATH": str(self.home / "herdr.sock"),
                "HERDR_PANE_ID": "w1:p1",
            },
            clear=True,
        ), mock.patch.object(
            self.notifier,
            "herdr_agent_identity",
            return_value=identity,
        ) as capture:
            kind, source = self.notifier.detected_source(args)

        self.assertEqual(kind, "codex_session")
        self.assertEqual(source["herdr_session"], "default")
        capture.assert_called_once_with(
            str(self.home / "herdr.sock"),
            "w1:p1",
            "default",
            str(pathlib.Path.cwd()),
        )

    def test_slack_thread_url_parser_is_strict_and_canonical(self):
        channel, thread_ts = self.notifier.parse_slack_thread_url(
            "https://workspace.slack.com/archives/C12345678/p1234567890123456"
        )
        self.assertEqual(channel, "C12345678")
        self.assertEqual(thread_ts, "1234567890.123456")
        for invalid in (
            "http://workspace.slack.com/archives/C12345678/p1234567890123456",
            "https://workspace.slack.com/archives/C12345678/p1234567890123456?token=x",
            "https://evil.example/archives/C12345678/p1234567890123456",
        ):
            with self.subTest(invalid=invalid), self.assertRaises(SystemExit):
                self.notifier.parse_slack_thread_url(invalid)

    def test_herdr_attach_reads_slack_url_from_bounded_stdin(self):
        expected = "https://workspace.slack.com/archives/C12345678/p1234567890123456"
        stream = io.TextIOWrapper(io.BytesIO((expected + "\n").encode("utf-8")))
        with mock.patch.object(self.notifier.sys, "stdin", stream):
            actual = self.notifier.slack_thread_url(
                types.SimpleNamespace(slack_url=None)
            )
        self.assertEqual(actual, expected)

        oversized = io.TextIOWrapper(
            io.BytesIO(b"x" * (self.notifier.MAX_SLACK_URL_BYTES + 1))
        )
        with mock.patch.object(
            self.notifier.sys, "stdin", oversized
        ), self.assertRaises(SystemExit):
            self.notifier.slack_thread_url(types.SimpleNamespace(slack_url=None))

    def test_herdr_detach_does_not_require_pane_arguments(self):
        args = types.SimpleNamespace(
            herdr_command="detach",
            bridge_id="brg_0123456789abcdef0123456789abcdef",
            expected_generation=2,
            team="T12345678",
            json=True,
        )
        result = {
            "ok": True,
            "bridge_id": args.bridge_id,
            "status": "closed",
        }
        with mock.patch.object(
            self.notifier,
            "broker_call",
            return_value=result,
        ) as broker, mock.patch("builtins.print"):
            self.assertEqual(self.notifier.run_herdr_command(args), 0)

        broker.assert_called_once_with(
            {
                "op": "close",
                "bridge_id": args.bridge_id,
                "expected_generation": 2,
                "team_id": "T12345678",
            }
        )

    def test_herdr_session_mismatch_fails_closed(self):
        args = types.SimpleNamespace(run_id=None, hermes_session_id=None)
        identity = {
            "herdr_terminal_id": "term_6583153c2a1b81",
            "native_session_id": "official-session",
            "pane_agent": "codex",
        }
        with mock.patch.dict(
            os.environ,
            {
                "HERDR_ENV": "1",
                "HERDR_SESSION": "pilot",
                "HERDR_SOCKET_PATH": str(self.home / "herdr.sock"),
                "HERDR_PANE_ID": "w1:p1",
                "CODEX_THREAD_ID": "different-session",
            },
            clear=True,
        ), mock.patch.object(
            self.notifier,
            "herdr_agent_identity",
            return_value=identity,
        ), self.assertRaisesRegex(SystemExit, "does not match"):
            self.notifier.detected_source(args)

    def test_zellij_only_source_captures_process_identity(self):
        args = types.SimpleNamespace(run_id=None, hermes_session_id=None)
        identity = {
            "session_name": "work", "pane_id": "7", "cwd": "/tmp/project",
            "pane_agent": "codex", "pane_command_hash": "abc123",
            "process_identity": process_identity(),
        }
        with mock.patch.dict(os.environ, {
            "ZELLIJ_SESSION_NAME": "work",
            "ZELLIJ_PANE_ID": "7",
        }, clear=True), mock.patch.object(self.notifier, "zellij_pane_identity", return_value=identity) as capture:
            kind, source = self.notifier.detected_source(args)
        self.assertEqual(kind, "zellij_pane")
        self.assertEqual(source["pane_command_hash"], "abc123")
        self.assertEqual(source["process_identity"], process_identity())
        capture.assert_called_once_with("work", "7", str(pathlib.Path.cwd()))

    def test_rebind_captures_the_current_exact_pane(self):
        identity = {
            "session_name": "work", "pane_id": "7", "cwd": "/tmp/project",
            "pane_agent": "claude", "pane_command_hash": "new-fingerprint",
            "process_identity": process_identity(agent="claude"),
        }
        with mock.patch.dict(os.environ, {
            "ZELLIJ_SESSION_NAME": "work",
            "ZELLIJ_PANE_ID": "7",
        }, clear=True), mock.patch.object(
            self.notifier, "zellij_pane_identity", return_value=identity,
        ), mock.patch.object(
            self.notifier, "broker_call",
            return_value={"ok": True, "thread_ts": "123.456"},
        ) as broker:
            result = self.notifier.main([
                "rebind", "--channel", "C12345678", "--thread-ts", "123.456",
            ])
        self.assertEqual(result, 0)
        request = broker.call_args.args[0]
        self.assertEqual(request["op"], "rebind")
        self.assertEqual(request["source"]["pane_command_hash"], "new-fingerprint")
        self.assertEqual(
            request["source"]["process_identity"], process_identity(agent="claude")
        )

    def test_attach_captures_an_explicit_existing_native_pane(self):
        project = self.home / "project"
        project.mkdir()
        identity = {
            "session_name": "didactic-jellyfish",
            "pane_id": "31",
            "cwd": str(project),
            "pane_agent": "claude",
            "pane_command_hash": "exact-fingerprint",
            "process_identity": process_identity(
                agent="claude",
                session="didactic-jellyfish",
                pane="31",
            ),
        }
        with mock.patch.object(
            self.notifier, "zellij_pane_identity", return_value=identity,
        ) as capture, mock.patch.object(
            self.notifier, "broker_call",
            return_value={
                "ok": True,
                "bridge_id": "brg_test",
                "thread_ts": "123.456",
            },
        ) as broker:
            result = self.notifier.main([
                "attach",
                "--channel", "C12345678",
                "--thread-ts", "123.456",
                "--claude-session-id", "claude-session",
                "--zellij-session", "didactic-jellyfish",
                "--zellij-pane-id", "31",
                "--cwd", str(project),
                "--idempotency-key", "attach-" + "existing-123",
                "--json",
            ])
        self.assertEqual(result, 0)
        capture.assert_called_once_with(
            "didactic-jellyfish", "31", str(project)
        )
        request = broker.call_args.args[0]
        self.assertEqual(request["source_kind"], "claude_session")
        self.assertEqual(request["source"]["session_id"], "claude-session")
        self.assertEqual(
            request["source"]["pane_command_hash"], "exact-fingerprint"
        )

    def test_attach_requires_a_complete_explicit_pane(self):
        with self.assertRaisesRegex(
            SystemExit, "--zellij-session and --zellij-pane-id"
        ):
            self.notifier.main([
                "attach",
                "--channel", "C12345678",
                "--thread-ts", "123.456",
                "--claude-session-id", "claude-session",
                "--zellij-session", "work",
                "--idempotency-key", "attach-" + "existing-123",
            ])

    def test_noninteractive_setup_delegates_manifest_to_hermes(self):
        args = types.SimpleNamespace(non_interactive=True, no_restart=False)
        runner, _, _ = self._setup_runner()
        with mock.patch.object(self.notifier, "_find_hermes", return_value="/usr/bin/hermes"), mock.patch.object(
            self.notifier.subprocess, "run", side_effect=runner
        ) as run:
            self.assertEqual(self.notifier.run_setup(args), 0)
        self.assertEqual(
            [call.args[0] for call in run.call_args_list],
            [
                ["/usr/bin/hermes", "plugins", "list", "--plain"],
                ["/usr/bin/hermes", "config", "get", "slack.allow_bots"],
                ["/usr/bin/hermes", "config", "get", "display.busy_ack_enabled"],
                ["/usr/bin/hermes", "plugins", "enable", "tether"],
                ["/usr/bin/hermes", "config", "set", "slack.allow_bots", "mentions"],
                ["/usr/bin/hermes", "config", "set", "display.busy_ack_enabled", "false"],
                ["/usr/bin/hermes", "slack", "manifest", "--write"],
            ],
        )

    def test_interactive_setup_runs_hermes_onboarding_restart_and_live_doctor(self):
        args = types.SimpleNamespace(non_interactive=False, no_restart=False)
        runner, _, _ = self._setup_runner()
        with mock.patch.object(self.notifier, "_find_hermes", return_value="/usr/bin/hermes"), mock.patch.object(
            self.notifier.subprocess, "run", side_effect=runner
        ) as run, mock.patch.object(self.notifier, "doctor", return_value=(True, ["ok live broker"])):
            self.assertEqual(self.notifier.run_setup(args), 0)
        self.assertEqual(
            [call.args[0] for call in run.call_args_list],
            [
                ["/usr/bin/hermes", "plugins", "list", "--plain"],
                ["/usr/bin/hermes", "config", "get", "slack.allow_bots"],
                ["/usr/bin/hermes", "config", "get", "display.busy_ack_enabled"],
                ["/usr/bin/hermes", "plugins", "enable", "tether"],
                ["/usr/bin/hermes", "config", "set", "slack.allow_bots", "mentions"],
                ["/usr/bin/hermes", "config", "set", "display.busy_ack_enabled", "false"],
                ["/usr/bin/hermes", "gateway", "setup"],
                ["/usr/bin/hermes", "gateway", "restart"],
            ],
        )
        self.assertEqual(
            [call.kwargs["timeout"] for call in run.call_args_list],
            [
                self.notifier.SERVICE_TIMEOUT_SECONDS,
                self.notifier.SERVICE_TIMEOUT_SECONDS,
                self.notifier.SERVICE_TIMEOUT_SECONDS,
                self.notifier.SERVICE_TIMEOUT_SECONDS,
                self.notifier.SERVICE_TIMEOUT_SECONDS,
                self.notifier.SERVICE_TIMEOUT_SECONDS,
                self.notifier.SETUP_TIMEOUT_SECONDS,
                self.notifier.SERVICE_TIMEOUT_SECONDS,
            ],
        )

    def test_setup_discovers_and_disables_legacy_before_enabling_tether(self):
        args = types.SimpleNamespace(non_interactive=True, no_restart=False)
        runner, plugins, _ = self._setup_runner(legacy="enabled")
        with mock.patch.object(self.notifier, "_find_hermes", return_value="/usr/bin/hermes"), mock.patch.object(
            self.notifier.subprocess, "run", side_effect=runner
        ) as run:
            result = self.notifier.run_setup(args)
        self.assertEqual(result, 0)
        commands = [call.args[0] for call in run.call_args_list]
        self.assertLess(
            commands.index(["/usr/bin/hermes", "plugins", "disable", "session-bridge"]),
            commands.index(["/usr/bin/hermes", "plugins", "enable", "tether"]),
        )
        self.assertEqual(plugins, {"tether": "enabled", "session-bridge": "disabled"})

    def test_setup_failure_restores_plugin_and_config_state(self):
        args = types.SimpleNamespace(non_interactive=True, no_restart=False)
        original = {
            "slack.allow_bots": "none",
            "display.busy_ack_enabled": "true",
        }
        runner, plugins, values = self._setup_runner(
            legacy="enabled",
            config=original,
            fail_on=("slack", "manifest", "--write"),
        )
        with mock.patch.object(
            self.notifier, "_find_hermes", return_value="/usr/bin/hermes"
        ), mock.patch.object(
            self.notifier.subprocess, "run", side_effect=runner
        ) as run:
            result = self.notifier.run_setup(args)
        self.assertEqual(result, 74)
        self.assertEqual(plugins, {"tether": "disabled", "session-bridge": "enabled"})
        self.assertEqual(values, original)
        self.assertEqual(
            sum(
                call.args[0][1:] == ["plugins", "list", "--plain"]
                for call in run.call_args_list
            ),
            1,
        )

    def test_setup_rollback_never_restores_two_active_bridges(self):
        args = types.SimpleNamespace(non_interactive=True, no_restart=False)
        runner, plugins, _ = self._setup_runner(
            tether="enabled",
            legacy="enabled",
            fail_on=("slack", "manifest", "--write"),
        )
        with mock.patch.object(
            self.notifier, "_find_hermes", return_value="/usr/bin/hermes"
        ), mock.patch.object(self.notifier.subprocess, "run", side_effect=runner):
            result = self.notifier.run_setup(args)
        self.assertEqual(result, 74)
        self.assertEqual(plugins, {"tether": "enabled", "session-bridge": "disabled"})

    def test_setup_timeout_rolls_back_completed_mutations(self):
        args = types.SimpleNamespace(non_interactive=True, no_restart=False)
        runner, plugins, values = self._setup_runner(
            legacy="enabled",
            raise_on=("slack", "manifest", "--write"),
        )
        with mock.patch.object(
            self.notifier, "_find_hermes", return_value="/usr/bin/hermes"
        ), mock.patch.object(self.notifier.subprocess, "run", side_effect=runner):
            result = self.notifier.run_setup(args)
        self.assertEqual(result, 1)
        self.assertEqual(plugins, {"tether": "disabled", "session-bridge": "enabled"})
        self.assertEqual(values, {})

    def test_setup_redacts_exception_details_before_printing(self):
        args = types.SimpleNamespace(non_interactive=True, no_restart=False)
        captured = io.StringIO()
        with mock.patch.object(
            self.notifier, "_find_hermes", return_value="/usr/bin/hermes"
        ), mock.patch.object(
            self.notifier,
            "_snapshot_setup",
            side_effect=RuntimeError(
                "OPENAI_API_KEY=sk-proj-abcdefghijklmnopqrstuvwxyz0123456789"
            ),
        ), mock.patch("sys.stderr", captured):
            result = self.notifier.run_setup(args)
        self.assertEqual(result, 2)
        self.assertIn("[REDACTED_PROVIDER_KEY]", captured.getvalue())
        self.assertNotIn("sk-proj-", captured.getvalue())


class PluginRoutingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        sys.modules["bridge_runtime"] = self.runtime
        spec = importlib.util.spec_from_file_location(f"session_bridge_test_{id(self)}", PLUGIN_PATH)
        self.plugin = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = self.plugin
        spec.loader.exec_module(self.plugin)
        self.plugin.store = self.runtime.Store()
        self.plugin.state.store = self.plugin.store
        self.plugin.state.ready = True
        self.plugin_module_name = spec.name
        self.config = self.home / ".config" / "tether" / "config.toml"
        self.config.parent.mkdir(parents=True)
        self.config.write_text('allowed_users = ["U12345678"]\n')
        self.env_patch = mock.patch.dict(os.environ, {
            "SLACK_ALLOWED_USERS": "", "GATEWAY_ALLOWED_USERS": "",
        }, clear=False)
        self.env_patch.start()

    def tearDown(self):
        self.env_patch.stop()
        sys.modules.pop("bridge_runtime", None)
        sys.modules.pop(self.plugin_module_name, None)
        self.temp.cleanup()

    def make_bridge(self, owner="U12345678"):
        bridge = self.plugin.store.create({
            "source_kind": "headless_run",
            "source": {"run_id": "cron-1", "cwd": "/tmp/project"},
            "owner_user_id": owner,
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "cron-1",
        })
        return self.plugin.store.bind(bridge.bridge_id, "123.456")

    def gateway_turn(
        self,
        *,
        platform,
        thread_ts,
        message_ts,
        text,
        user_id,
        action,
        reason,
        bridge_id=None,
        is_bot=False,
    ):
        source = types.SimpleNamespace(
            platform=platform,
            thread_id=thread_ts,
            guild_id="T12345678",
            chat_id="C12345678",
            user_id=user_id,
            message_id=message_ts,
            is_bot=is_bot,
        )
        bridge = self.plugin.store.get(bridge_id) if bridge_id else None
        decision = self.plugin.routing.RoutingDecision(
            action=action,
            reason=reason,
            message_identity=self.plugin.routing.MessageIdentity(
                "T12345678", "C12345678", message_ts,
            ),
            writer_id="writer:test" if action is not self.plugin.routing.RouteAction.SILENT else None,
            bridge_id=bridge_id,
            binding_generation=(
                bridge.binding_generation if bridge is not None else None
            ),
        )
        raw_message = {self.plugin.ROUTING_DECISION_KEY: decision}
        if (
            action is self.plugin.routing.RouteAction.HERMES
            and bridge is not None
        ):
            event_id = f"slack:T12345678:C12345678:{message_ts}"
            claim = self.plugin.store.claim_thread_ingress(
                event_id,
                "T12345678",
                "C12345678",
                thread_ts,
                route_action="hermes",
                writer_id="writer:test",
                bridge_id=bridge.bridge_id,
                binding_generation=bridge.binding_generation,
                payload={"text": text, "subtype": ""},
            )
            raw_message["_tether_ingress_claim"] = (
                event_id,
                claim["lease_id"],
                claim["fence_epoch"],
            )
        return types.SimpleNamespace(
            source=source,
            message_id=message_ts,
            text=text,
            raw_message=raw_message,
        )

    def simulate_polled_hermes(self, event, adapter):
        decision = event[self.plugin.ROUTING_DECISION_KEY]
        event_id = self.plugin._composite_event_id(decision)
        claim = self.plugin.store.claim_thread_ingress(
            event_id,
            decision.message_identity.team_id,
            decision.message_identity.channel_id,
            str(event.get("thread_ts") or decision.message_identity.message_ts),
            route_action="hermes",
            writer_id=str(decision.writer_id or ""),
            bridge_id=str(decision.bridge_id or ""),
            binding_generation=decision.binding_generation,
            payload={"text": event["text"], "subtype": str(event.get("subtype") or "")},
        )
        if claim["status"] != "claimed":
            return False, None
        event["_tether_ingress_claim"] = (
            event_id,
            claim["lease_id"],
            claim["fence_epoch"],
        )
        result = None
        if decision.bridge_id:
            class Platform:
                value = "slack"

            platform = Platform()
            source = types.SimpleNamespace(
                platform=platform,
                thread_id=event["thread_ts"],
                guild_id=decision.message_identity.team_id,
                chat_id=decision.message_identity.channel_id,
                user_id=event.get("user"),
                message_id=event["ts"],
                is_bot=bool(event.get("bot_id")),
            )
            gateway_event = types.SimpleNamespace(
                source=source,
                message_id=event["ts"],
                text=event["text"],
                raw_message=event,
            )
            result = self.plugin._pre_gateway_dispatch(
                event=gateway_event,
                gateway=types.SimpleNamespace(adapters={platform: adapter}),
            )
        self.plugin.store.complete_thread_ingress(
            event_id,
            claim["lease_id"],
            claim["fence_epoch"],
        )
        event["_tether_ingress_dispatched"] = True
        return True, result

    def test_imports_recent_native_slack_sessions_for_restart_recovery(self):
        sessions = self.runtime.HERMES_HOME / "sessions" / "sessions.json"
        sessions.parent.mkdir(parents=True)
        sessions.write_text(json.dumps({
            "agent:main:slack:dm:C87654321:1785000000.000001": {
                "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "origin": {
                    "platform": "slack",
                    "chat_id": "C87654321",
                    "thread_id": "1785000000.000001",
                },
            },
            "old": {
                "updated_at": "2020-01-01T00:00:00+00:00",
                "origin": {
                    "platform": "slack",
                    "chat_id": "COLD00000",
                    "thread_id": "100.000",
                },
            },
        }))
        adapter = types.SimpleNamespace(
            _channel_team={"C87654321": "T12345678"},
        )
        imported = self.plugin._import_native_slack_participation(adapter)
        self.assertEqual(imported, 1)
        self.assertTrue(self.plugin.store.participates(
            "T12345678", "C87654321", "1785000000.000001",
        ))
        participation = self.plugin.store.recent_participating_threads(
            hours=24 * 365, limit=10,
        )
        self.assertLess(participation[0][3], datetime.datetime.now().timestamp() + 1)

    def test_import_uses_newest_sessions_when_store_exceeds_limit(self):
        sessions = self.runtime.HERMES_HOME / "sessions" / "sessions.json"
        sessions.parent.mkdir(parents=True)
        now = datetime.datetime.now(datetime.timezone.utc)
        payload = {
            f"old-{index}": {
                "updated_at": (now - datetime.timedelta(days=30)).isoformat(),
                "origin": {
                    "platform": "slack",
                    "chat_id": "COLD00000",
                    "thread_id": f"{index}.000",
                },
            }
            for index in range(2000)
        }
        payload["recent"] = {
            "updated_at": now.isoformat(),
            "origin": {
                "platform": "slack",
                "chat_id": "C87654321",
                "thread_id": "1785000000.000001",
            },
        }
        sessions.write_text(json.dumps(payload))
        adapter = types.SimpleNamespace(
            _channel_team={"C87654321": "T12345678"},
        )

        imported = self.plugin._import_native_slack_participation(adapter)

        self.assertEqual(imported, 1)
        self.assertTrue(self.plugin.store.participates(
            "T12345678", "C87654321", "1785000000.000001",
        ))

    def test_authorization_fails_closed_and_honors_owner(self):
        bridge = self.make_bridge()
        self.assertTrue(self.plugin._authorized(bridge, "U12345678"))
        self.assertFalse(self.plugin._authorized(bridge, "U99999999"))
        self.config.write_text("allowed_users = []\n")
        self.assertFalse(self.plugin._authorized(bridge, "U12345678"))

    def test_authorization_reuses_hermes_allowlist(self):
        self.config.write_text("allowed_users = []\n")
        bridge = self.make_bridge(owner="*")
        with mock.patch.dict(os.environ, {"SLACK_ALLOWED_USERS": "U12345678"}, clear=False):
            self.assertTrue(self.plugin._authorized(bridge, "U12345678"))

    def test_unauthorized_bridge_reply_does_not_keep_success_reaction(self):
        self.make_bridge(owner="U12345678")

        class Platform:
            value = "slack"

        platform = Platform()
        event = self.gateway_turn(
            platform=platform,
            thread_ts="123.456",
            message_ts="111.1",
            text="continue",
            user_id="U99999999",
            action=self.plugin.routing.RouteAction.SILENT,
            reason="human_not_authorized",
        )

        class Adapter:
            _reacting_message_ids = {("T12345678", "111.1")}

            def __init__(self):
                self.removed = []

            async def _remove_reaction(
                self, channel, event_id, reaction, team_id=""
            ):
                self.removed.append(
                    (channel, event_id, reaction, team_id)
                )

        adapter = Adapter()
        gateway = types.SimpleNamespace(adapters={platform: adapter})

        async def exercise():
            result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
            await asyncio.sleep(0)
            return result

        result = asyncio.run(exercise())
        self.assertEqual(result["reason"], "bridge-user-not-authorized")
        self.assertNotIn(
            ("T12345678", "111.1"),
            adapter._reacting_message_ids,
        )
        self.assertEqual(
            adapter.removed,
            [("C12345678", "111.1", "eyes", "T12345678")],
        )

    def test_native_send_routes_thread_reply_through_durable_broker(self):
        class SlackAdapter:
            _tether_prefilter = False

            def __init__(self):
                self._bot_message_ts = set()
                self._channel_team = {"C12345678": "T12345678"}

            async def connect(self):
                return True

            async def send(self, channel, content, reply_to=None, metadata=None):
                return {"ok": True}

            async def _handle_slack_message(self, event):
                return event

        modules = {
            "plugins": types.ModuleType("plugins"),
            "plugins.platforms": types.ModuleType("plugins.platforms"),
            "plugins.platforms.slack": types.ModuleType("plugins.platforms.slack"),
            "plugins.platforms.slack.adapter": types.ModuleType("plugins.platforms.slack.adapter"),
        }
        modules["plugins.platforms.slack.adapter"].SlackAdapter = SlackAdapter
        delivery = mock.AsyncMock(
            return_value={"message_ts": "123.457"},
        )
        with mock.patch.dict(
            sys.modules,
            modules,
        ), mock.patch.object(
            self.plugin,
            "_ensure_reply_poller",
        ), mock.patch.object(
            self.plugin,
            "_deliver_hermes_message_group",
            new=delivery,
        ):
            self.plugin._install_slack_bridge_prefilter()
            adapter = SlackAdapter()
            asyncio.run(adapter.send("C12345678", "done", "123.456", None))
        delivery.assert_awaited_once()
        self.assertEqual(delivery.await_args.kwargs["team_id"], "T12345678")
        self.assertEqual(delivery.await_args.kwargs["channel_id"], "C12345678")
        self.assertEqual(delivery.await_args.kwargs["thread_ts"], "123.456")

    def test_prefilter_admits_unmentioned_reply_before_hermes_mention_gate(self):
        bridge = self.make_bridge()

        class SlackAdapter:
            _tether_prefilter = False

            def __init__(self):
                self._bot_message_ts = set()
                self._bot_user_id = "UBOT00001"
                self._team_bot_user_ids = {"T12345678": "UBOT00001"}
                self._channel_team = {"C12345678": "T12345678"}
                self.config = types.SimpleNamespace(extra={})
                self.sent = []

            async def connect(self):
                return True

            async def send(self, channel, content, metadata=None):
                self.sent.append((channel, content, metadata))
                return {"ok": True}

            def _get_client(self, _channel, team_id=None):
                return types.SimpleNamespace(
                    users_info=mock.AsyncMock(
                        return_value={
                            "user": {
                                "id": "U12345678",
                                "is_bot": False,
                            }
                        }
                    ),
                    conversations_replies=mock.AsyncMock(
                        return_value={
                            "messages": [{
                                "ts": "123.456",
                                "user": "UBOT00001",
                                "bot_id": "BBOT00001",
                                "metadata": {
                                    "event_type": "tether_root",
                                    "event_payload": {
                                        "bridge_id": bridge.bridge_id,
                                    },
                                },
                            }],
                            "response_metadata": {"next_cursor": ""},
                        }
                    ),
                )

            async def _handle_slack_message(self, event, payload=None):
                return event.get("thread_ts") in self._bot_message_ts, payload

        modules = {
            "plugins": types.ModuleType("plugins"),
            "plugins.platforms": types.ModuleType("plugins.platforms"),
            "plugins.platforms.slack": types.ModuleType("plugins.platforms.slack"),
            "plugins.platforms.slack.adapter": types.ModuleType("plugins.platforms.slack.adapter"),
        }
        modules["plugins.platforms.slack.adapter"].SlackAdapter = SlackAdapter
        with mock.patch.dict(sys.modules, modules), mock.patch.object(self.plugin, "_ensure_reply_poller"):
            self.plugin._install_slack_bridge_prefilter()
            adapter = SlackAdapter()
            admitted, payload = asyncio.run(adapter._handle_slack_message({
                "ts": "111.1", "thread_ts": "123.456", "channel": "C12345678",
                "text": "continue", "user": "U12345678",
            }, {"team_id": "T12345678"}))
            ignored = asyncio.run(adapter._handle_slack_message({
                "ts": "111.2", "thread_ts": "999.999", "channel": "C12345678",
                "team": "T12345678", "text": "ambient", "user": "U12345678",
            }))
        self.assertTrue(admitted)
        self.assertEqual(payload, {"team_id": "T12345678"})
        self.assertIsNone(ignored)

    def test_live_hermes_adapter_alias_precedes_source_tree_fallback(self):
        class LiveSlackAdapter:
            async def _handle_slack_message(self, event):
                return event

        class SourceTreeSlackAdapter(LiveSlackAdapter):
            pass

        modules = {
            "hermes_plugins": types.ModuleType("hermes_plugins"),
            "hermes_plugins.slack_platform": types.ModuleType("hermes_plugins.slack_platform"),
            "hermes_plugins.slack_platform.adapter": types.ModuleType("hermes_plugins.slack_platform.adapter"),
            "plugins": types.ModuleType("plugins"),
            "plugins.platforms": types.ModuleType("plugins.platforms"),
            "plugins.platforms.slack": types.ModuleType("plugins.platforms.slack"),
            "plugins.platforms.slack.adapter": types.ModuleType("plugins.platforms.slack.adapter"),
        }
        modules["hermes_plugins.slack_platform.adapter"].SlackAdapter = LiveSlackAdapter
        modules["plugins.platforms.slack.adapter"].SlackAdapter = SourceTreeSlackAdapter
        with mock.patch.dict(sys.modules, modules):
            self.assertIs(self.plugin._resolve_slack_adapter(), LiveSlackAdapter)

    def test_reply_poller_recovers_only_authorized_unseen_human_reply(self):
        bridge = self.make_bridge()
        messages = [
            {
                "ts": bridge.thread_ts,
                "text": "root",
                "bot_id": "B12345678",
                "user": "ULOCAL",
                "metadata": {
                    "event_type": "tether_root",
                    "event_payload": {"bridge_id": bridge.bridge_id},
                },
            },
            {"ts": "111.1", "thread_ts": bridge.thread_ts, "text": "continue", "user": "U12345678"},
            {"ts": "111.2", "thread_ts": bridge.thread_ts, "text": "no", "user": "U99999999"},
            {
                "ts": "111.3",
                "thread_ts": bridge.thread_ts,
                "text": "bot",
                "bot_id": "B12345678",
                "user": "ULOCAL",
            },
        ]

        class Client:
            async def users_info(self, *, user):
                return {"user": {"id": user, "is_bot": False}}

            async def conversations_history(self, **_kwargs):
                return {"ok": True, "messages": []}

            async def conversations_join(self, **_kwargs):
                raise AssertionError("already-accessible DM must not be joined")

            async def conversations_replies(self, **_kwargs):
                return {"messages": messages}

        class Adapter:
            _bot_user_id = "ULOCAL"
            _team_bot_user_ids = {"T12345678": "ULOCAL"}
            _channel_team = {"C12345678": "T12345678"}
            config = types.SimpleNamespace(extra={})

            def __init__(self):
                self.events = []

            def _get_client(self, _channel, team_id=None):
                return Client()

            async def _handle_slack_message(self, event):
                decision = await test_case.plugin._route_slack_event(
                    self,
                    event,
                )
                if (
                    decision is None
                    or decision.action
                    is test_case.plugin.routing.RouteAction.SILENT
                ):
                    return
                event[test_case.plugin.ROUTING_DECISION_KEY] = decision
                dispatched, result = test_case.simulate_polled_hermes(
                    event,
                    self,
                )
                if dispatched:
                    self.events.append(event)
                    self.events[-1]["dispatch_result"] = result

        test_case = self
        adapter = Adapter()
        recovered = asyncio.run(self.plugin._poll_recent_replies(adapter))
        recovered_again = asyncio.run(self.plugin._poll_recent_replies(adapter))
        self.assertEqual(recovered, 1)
        self.assertEqual(recovered_again, 0)
        self.assertEqual(adapter.events[0]["text"], "continue")
        self.assertTrue(adapter.events[0]["_tether_polled"])
        self.assertEqual(adapter.events[0]["dispatch_result"]["action"], "rewrite")

    def test_reply_poller_joins_only_after_not_in_channel(self):
        self.make_bridge()
        calls = []

        class Client:
            async def conversations_history(self, **_kwargs):
                calls.append("history")
                raise RuntimeError("Slack API error: not_in_channel")

            async def conversations_join(self, **_kwargs):
                calls.append("join")
                return {"ok": True}

            async def conversations_replies(self, **_kwargs):
                return {"messages": []}

        class Adapter:
            def _get_client(self, _channel, team_id=None):
                return Client()

            async def _handle_slack_message(self, _event):
                raise AssertionError("no messages should be dispatched")

        recovered = asyncio.run(self.plugin._poll_recent_replies(Adapter()))
        self.assertEqual(recovered, 0)
        self.assertEqual(calls, ["history", "join"])

    def test_reply_poller_recovers_unmentioned_participating_thread_reply(self):
        self.plugin.store.mark_participation(
            "T12345678", "C12345678", "123.456",
        )
        messages = [
            {"ts": "123.456", "text": "root", "user": "U12345678"},
            {
                "ts": "123.457", "thread_ts": "123.456",
                "text": "did you see this?", "user": "U12345678",
            },
        ]

        class Client:
            async def users_info(self, *, user):
                return {"user": {"id": user, "is_bot": False}}

            async def conversations_history(self, **_kwargs):
                return {"ok": True, "messages": []}

            async def conversations_join(self, **_kwargs):
                raise AssertionError("already-accessible DM must not be joined")

            async def conversations_replies(self, **_kwargs):
                return {"messages": messages}

        class Adapter:
            _bot_user_id = "ULOCAL"
            _team_bot_user_ids = {"T12345678": "ULOCAL"}
            _channel_team = {"C12345678": "T12345678"}
            config = types.SimpleNamespace(extra={})

            def __init__(self):
                self.events = []

            def _get_client(self, _channel, team_id=None):
                return Client()

            async def _handle_slack_message(self, event):
                decision = await test_case.plugin._route_slack_event(
                    self,
                    event,
                )
                if (
                    decision is None
                    or decision.action
                    is test_case.plugin.routing.RouteAction.SILENT
                ):
                    return
                event[test_case.plugin.ROUTING_DECISION_KEY] = decision
                dispatched, _result = test_case.simulate_polled_hermes(
                    event,
                    self,
                )
                if dispatched:
                    self.events.append(event)
        test_case = self
        adapter = Adapter()
        recovered = asyncio.run(self.plugin._poll_recent_replies(adapter))
        recovered_again = asyncio.run(self.plugin._poll_recent_replies(adapter))
        self.assertEqual(recovered, 1)
        self.assertEqual(recovered_again, 0)
        self.assertEqual(adapter.events[0]["text"], "did you see this?")
        self.assertTrue(adapter.events[0]["_tether_polled"])

    def test_reply_poller_skips_participation_without_workspace(self):
        self.plugin.store.mark_participation(
            "", "C12345678", "123.456",
        )

        class Adapter:
            def _get_client(self, _channel, team_id=None):
                raise AssertionError("incomplete participation must not be polled")

        recovered = asyncio.run(self.plugin._poll_recent_replies(Adapter()))
        self.assertEqual(recovered, 0)

    def test_reply_poller_recovers_peer_bot_thread_turns_when_enabled(self):
        bridge = self.make_bridge(owner="*")
        messages = [
            {
                "ts": bridge.thread_ts,
                "text": "root",
                "bot_id": "BLOCAL",
                "user": "ULOCAL",
                "metadata": {
                    "event_type": "tether_root",
                    "event_payload": {"bridge_id": bridge.bridge_id},
                },
            },
            {
                "ts": "111.1", "thread_ts": bridge.thread_ts,
                "text": "<@ULOCAL> challenge this premise", "bot_id": "BPEER", "user": "UPEER",
                "subtype": "bot_message",
            },
            {
                "ts": "111.2", "thread_ts": bridge.thread_ts,
                "text": "general bot chatter", "bot_id": "BPEER", "user": "UPEER",
                "subtype": "bot_message",
            },
        ]

        class Client:
            async def conversations_history(self, **_kwargs):
                return {"ok": True, "messages": []}

            async def conversations_join(self, **_kwargs):
                raise AssertionError("already-accessible thread must not be joined")

            async def conversations_replies(self, **_kwargs):
                return {"messages": messages}

        class Adapter:
            _bot_user_id = "ULOCAL"
            _team_bot_user_ids = {"T12345678": "ULOCAL"}
            config = types.SimpleNamespace(extra={"allow_bots": "all"})

            def __init__(self):
                self.events = []

            def _get_client(self, _channel, team_id=None):
                return Client()

            async def _handle_slack_message(self, event):
                decision = await test_case.plugin._route_slack_event(
                    self,
                    event,
                )
                if (
                    decision is None
                    or decision.action
                    is test_case.plugin.routing.RouteAction.SILENT
                ):
                    return
                event[test_case.plugin.ROUTING_DECISION_KEY] = decision
                dispatched, result = test_case.simulate_polled_hermes(
                    event,
                    self,
                )
                if dispatched:
                    self.events.append(event)
                    self.events[-1]["dispatch_result"] = result

        test_case = self
        adapter = Adapter()
        with mock.patch.dict(os.environ, {"TETHER_ALLOWED_BOT_USERS": "UPEER"}, clear=False):
            recovered = asyncio.run(self.plugin._poll_recent_replies(adapter))
            recovered_again = asyncio.run(self.plugin._poll_recent_replies(adapter))
        self.assertEqual(recovered, 1)
        self.assertEqual(recovered_again, 0)
        self.assertEqual(len(adapter.events), 1)
        self.assertEqual(adapter.events[0]["ts"], "111.1")
        self.assertEqual(adapter.events[0]["dispatch_result"]["action"], "rewrite")

    def test_trusted_peer_bot_in_bound_thread_routes_to_bound_session(self):
        bridge = self.make_bridge()

        class Platform:
            value = "slack"

        platform = Platform()
        event = self.gateway_turn(
            platform=platform,
            thread_ts="123.456",
            message_ts="111.1",
            text="<@ULOCAL> challenge this",
            user_id="UPEER",
            action=self.plugin.routing.RouteAction.HERMES,
            reason="active_hermes_binding",
            bridge_id=bridge.bridge_id,
            is_bot=True,
        )
        gateway = types.SimpleNamespace(adapters={platform: types.SimpleNamespace()})
        with mock.patch.dict(os.environ, {"TETHER_ALLOWED_BOT_USERS": "UPEER"}, clear=False):
            result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
        self.assertEqual(result["action"], "rewrite")
        self.assertIn("challenge this", result["text"])

    def test_untrusted_peer_bot_cannot_enter_bound_thread(self):
        self.make_bridge()

        class Platform:
            value = "slack"

        platform = Platform()
        event = self.gateway_turn(
            platform=platform,
            thread_ts="123.456",
            message_ts="111.1",
            text="run this",
            user_id="UUNTRUSTED",
            action=self.plugin.routing.RouteAction.SILENT,
            reason="untrusted_peer_bot",
            is_bot=True,
        )
        gateway = types.SimpleNamespace(adapters={platform: types.SimpleNamespace()})
        with mock.patch.dict(os.environ, {"TETHER_ALLOWED_BOT_USERS": "UPEER"}, clear=False):
            result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
        self.assertEqual(result["reason"], "bridge-bot-not-authorized")

    def test_native_delta_drops_synthetic_thread_history(self):
        text = "old transcript\n[End of thread context]\nplease continue"
        self.assertEqual(self.plugin._reply_delta(text), "please continue")

    def test_authorized_headless_reply_rewrites_into_durable_hermes_context(self):
        bridge = self.make_bridge()

        class Platform:
            value = "slack"

        platform = Platform()
        event = self.gateway_turn(
            platform=platform,
            thread_ts="123.456",
            message_ts="111.1",
            text="continue the run",
            user_id="U12345678",
            action=self.plugin.routing.RouteAction.HERMES,
            reason="active_hermes_binding",
            bridge_id=bridge.bridge_id,
        )
        adapter = types.SimpleNamespace(_reacting_message_ids=set())
        gateway = types.SimpleNamespace(adapters={platform: adapter})
        result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
        self.assertEqual(result["action"], "rewrite")
        self.assertIn("Durable Hermes continuation", result["text"])
        self.assertIn("continue the run", result["text"])

    def test_restart_recovery_delivers_queued_reply_and_marks_it_complete(self):
        bridge = self.plugin.store.create({
            "source_kind": "claude_session",
            "source": {"session_id": "claude-restart", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "claude-restart",
        })
        bridge = self.plugin.store.bind(bridge.bridge_id, "123.456")
        self.plugin.store.enqueue_event("111.1", bridge.bridge_id, "continue after restart")
        replies = []

        def broker_call(request):
            replies.append(request["text"])
            acknowledged = self.plugin.store.acknowledge_attempt(
                request["reply_key"],
                request["bridge_id"],
                ack_kind="reply",
                message_ts="123.457",
            )
            return {"ok": True, "acknowledged_events": acknowledged}

        def continue_after_restart(_bridge, _prompt, _cancellation, persist):
            persist("finished after restart")
            return "finished after restart"

        with mock.patch.object(
            self.plugin, "broker_call", side_effect=broker_call
        ), mock.patch.object(
            self.plugin,
            "continue_native",
            side_effect=continue_after_restart,
        ):
            self.plugin._recover_queued_events()

        with self.plugin.store.connect() as database:
            state = database.execute("SELECT state FROM bridge_events WHERE event_id='111.1'").fetchone()[0]
        self.assertEqual(state, "delivered")
        self.assertIn("finished after restart", replies)

    def test_zellij_dispatch_waits_for_attempt_ack_before_completion(self):
        exact = process_identity(
            agent="codex", session="didactic-jellyfish", pane="51"
        )
        bridge = self.plugin.store.create({
            "source_kind": "codex_session",
            "source": {
                "session_id": "codex-1",
                "cwd": "/tmp/project",
                "zellij_session": "didactic-jellyfish",
                "zellij_pane_id": "51",
                "pane_agent": "codex",
                "pane_command_hash": hashlib.sha256(exact.encode()).hexdigest(),
                "process_identity": exact,
            },
            "owner_user_id": "*",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "zellij-awaiting-ack",
        })
        bridge = self.plugin.store.bind(bridge.bridge_id, "456.789")
        self.plugin.store.enqueue_event("111.1", bridge.bridge_id, "status?")

        class Platform:
            value = "slack"

        platform = Platform()
        adapter = types.SimpleNamespace(sent=[])
        gateway = types.SimpleNamespace(adapters={platform: adapter})
        with mock.patch.object(
            self.plugin, "deliver_zellij", return_value="unused"
        ) as deliver:
            asyncio.run(self.plugin._drain_bridge(bridge.bridge_id, gateway, platform))

        deliver.assert_called_once()
        attempt_id = deliver.call_args.args[2]
        self.assertEqual(
            attempt_id,
            self.runtime.delivery_attempt_id(
                bridge.bridge_id, ("111.1",), bridge.binding_generation
            ),
        )
        with self.plugin.store.connect() as database:
            self.assertEqual(
                database.execute(
                    "SELECT state FROM bridge_events WHERE event_id='111.1'"
                ).fetchone()[0],
                "awaiting_ack",
            )
            self.assertEqual(
                database.execute(
                    "SELECT state FROM bridge_attempts WHERE attempt_id=?",
                    (attempt_id,),
                ).fetchone()[0],
                "awaiting_ack",
            )

    def test_busy_native_followups_share_one_agent_turn_and_one_slack_reply(self):
        bridge = self.plugin.store.create({
            "source_kind": "claude_session",
            "source": {"session_id": "claude-1", "cwd": "/tmp/project"},
            "owner_user_id": "*",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "claude-batch",
        })
        bridge = self.plugin.store.bind(bridge.bridge_id, "456.789")
        self.plugin.store.enqueue_event("111.1", bridge.bridge_id, "first follow-up")
        self.plugin.store.enqueue_event("111.2", bridge.bridge_id, "latest follow-up")

        class Platform:
            value = "slack"

        platform = Platform()

        class Adapter:
            def __init__(self):
                self.sent = []

            async def send(self, channel, text, metadata):
                self.sent.append((channel, text, metadata))

        adapter = Adapter()
        gateway = types.SimpleNamespace(adapters={platform: adapter})
        prompts = []

        def continue_native(_bridge, prompt, _cancellation, persist):
            prompts.append(prompt)
            response = "Fixed and verified."
            persist(response)
            return response

        broker_requests = []

        def broker_call(request):
            broker_requests.append(request)
            acknowledged = self.plugin.store.acknowledge_attempt(
                request["reply_key"],
                request["bridge_id"],
                ack_kind="reply",
                message_ts="456.790",
            )
            return {"ok": True, "acknowledged_events": acknowledged}

        with mock.patch.object(
            self.plugin, "continue_native", side_effect=continue_native
        ), mock.patch.object(
            self.plugin, "broker_call", side_effect=broker_call
        ):
            asyncio.run(self.plugin._drain_bridge(bridge.bridge_id, gateway, platform))

        self.assertEqual(len(prompts), 1)
        self.assertIn("first follow-up", prompts[0])
        self.assertIn("latest follow-up", prompts[0])
        self.assertEqual(len(adapter.sent), 0)
        self.assertEqual(len(broker_requests), 1)
        self.assertEqual(broker_requests[0]["text"], "Fixed and verified.")
        with self.plugin.store.connect() as database:
            states = [
                row[0] for row in database.execute(
                    "SELECT state FROM bridge_events ORDER BY event_id"
                ).fetchall()
            ]
        self.assertEqual(states, ["delivered", "delivered"])

    def test_cancel_reply_discards_queued_work(self):
        bridge = self.plugin.store.create({
            "source_kind": "codex_session",
            "source": {"session_id": "codex-1", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "codex-1",
        })
        bridge = self.plugin.store.bind(bridge.bridge_id, "456.789")
        self.plugin.store.enqueue_event("111.1", bridge.bridge_id, "queued work")

        class Platform:
            value = "slack"

        platform = Platform()
        event = self.gateway_turn(
            platform=platform,
            thread_ts="456.789",
            message_ts="111.2",
            text="stop",
            user_id="U12345678",
            action=self.plugin.routing.RouteAction.NATIVE,
            reason="active_native_binding",
            bridge_id=bridge.bridge_id,
        )
        notices = []

        class Adapter:
            _reacting_message_ids = set()

        gateway = types.SimpleNamespace(adapters={platform: Adapter()})

        async def record_notice(_bridge, **kwargs):
            notices.append(kwargs)

        async def exercise():
            with mock.patch.object(
                self.plugin,
                "_post_control_notice",
                side_effect=record_notice,
            ):
                result = self.plugin._pre_gateway_dispatch(event=event, gateway=gateway)
                await asyncio.sleep(0)
            await asyncio.sleep(0)
            return result

        result = asyncio.run(exercise())
        self.assertEqual(result["reason"], "tether-cancel")
        self.assertEqual(len(notices), 1)
        self.assertIn("Cancelled 1 queued reply", notices[0]["text"])
        self.assertTrue(notices[0]["idempotency_key"].startswith("control:cancel:"))
        with self.plugin.store.connect() as database:
            state = database.execute("SELECT state FROM bridge_events WHERE event_id='111.1'").fetchone()[0]
        self.assertEqual(state, "failed")


class InstallerAndPackageTest(unittest.TestCase):
    def run_installer(self, home, harness="both"):
        env = {
            **os.environ,
            "HOME": str(home),
            "CODEX_HOME": str(home / "codex"),
            "CLAUDE_HOME": str(home / "claude"),
            "HERMES_HOME": str(home / "hermes"),
            "XDG_DATA_HOME": str(home / "data"),
            "XDG_CONFIG_HOME": str(home / "config"),
        }
        return subprocess.run(
            [str(INSTALL_PATH), f"--harness={harness}"], env=env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

    def test_installer_supports_both_harnesses_and_preserves_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = pathlib.Path(tmp)
            for harness in ("codex", "claude"):
                legacy = home / harness / "skills" / "hermes-slack-bridge" / "scripts"
                legacy.mkdir(parents=True)
                compatibility_client = legacy / "hermes_notify.py"
                compatibility_client.write_text(
                    'notify.add_argument("--owner", default="U12345678")\n'
                )
                compatibility_client.chmod(0o600)
            for directory in (path for path in home.rglob("*") if path.is_dir()):
                directory.chmod(0o700)
            first = self.run_installer(home)
            self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
            for harness in ("codex", "claude"):
                self.assertTrue((home / harness / "skills" / "tether" / "SKILL.md").is_file())
                compatibility_client = (
                    home / harness / "skills" / "hermes-slack-bridge" / "scripts" / "hermes_notify.py"
                ).read_text()
                self.assertIn('notify.add_argument("--owner")', compatibility_client)
                self.assertNotIn('default="U12345678"', compatibility_client)
                compatibility_skill = (
                    home / harness / "skills" / "hermes-slack-bridge" / "SKILL.md"
                ).read_text()
                self.assertIn("For a shared Slack channel, omit `--owner`", compatibility_skill)
            self.assertTrue((home / "hermes" / "plugins" / "tether" / "__init__.py").is_file())
            manifest = home / "hermes" / "plugins" / "tether" / "plugin.yaml"
            self.assertTrue(manifest.is_file())
            self.assertIn("name: tether", manifest.read_text())
            config = home / "config" / "tether" / "config.toml"
            config.write_text('default_channel = "C12345678"\nallowed_users = ["U12345678"]\n')
            config.chmod(0o600)
            second = self.run_installer(home)
            self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
            self.assertIn("C12345678", config.read_text(), "upgrades must preserve operator config")

    def test_one_command_setup_installs_then_uses_hermes_onboarding(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = pathlib.Path(tmp)
            fake_bin = home / "fake-bin"
            fake_bin.mkdir()
            hermes = fake_bin / "hermes"
            hermes.write_text("#!/bin/sh\nexit 0\n")
            hermes.chmod(0o700)
            env = {
                **os.environ,
                "HOME": str(home),
                "CODEX_HOME": str(home / "codex"),
                "CLAUDE_HOME": str(home / "claude"),
                "HERMES_HOME": str(home / "hermes"),
                "XDG_DATA_HOME": str(home / "data"),
                "XDG_CONFIG_HOME": str(home / "config"),
                "PATH": str(fake_bin) + os.pathsep + os.environ.get("PATH", ""),
            }
            result = subprocess.run(
                ["node", str(ROOT / "bin" / "tether.js"), "setup", "--harness=both", "--non-interactive"],
                env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertTrue((home / "codex" / "skills" / "tether" / "SKILL.md").is_file())
            self.assertTrue((home / "claude" / "skills" / "tether" / "SKILL.md").is_file())
            self.assertIn("Slack manifest generated", result.stdout)

    def test_public_tree_contains_no_known_private_identifiers_or_token_values(self):
        private_digests = {
            (12, "4e3c100b7e146ea64d5774c9fdddad6a9a3ec84bfd1c7b2ac8920f70f9f8ac64"),
            (12, "d1f84d64ec000ce0626824ba6112ac1443d20362bfe9f628e445e2c5f1577ce7"),
            (12, "a401343ad071c39758b906a27b1edbb3d4857b8ad4eaa6f1a37ae7ca3c7a8b83"),
            (26, "2d276b2dbe1b68717dcc126768295b9e85562c397f2165ee44c1c7861871d6e3"),
            (22, "3b627447db98828cf775837753f511df98a4c1717274f5d51815106189da5437"),
        }
        text = "\n".join(
            path.read_text(errors="ignore")
            for path in ROOT.rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        )
        for length, digest in private_digests:
            windows = (text[index:index + length] for index in range(max(0, len(text) - length + 1)))
            self.assertFalse(any(hashlib.sha256(value.encode()).hexdigest() == digest for value in windows))
        self.assertNotIn("xox" + "b-", text)
        self.assertNotIn("xap" + "p-", text)

    def test_skill_references_and_manifests_are_complete(self):
        skill = ROOT / "skills" / "tether"
        text = (skill / "SKILL.md").read_text()
        self.assertIn("name: tether", text)
        self.assertTrue((skill / "references" / "setup.md").is_file())
        self.assertTrue((skill / "references" / "contract.md").is_file())
        package = json.loads((ROOT / "package.json").read_text())
        self.assertEqual(package["pi"]["skills"], ["./skills"])
        for manifest in (ROOT / ".claude-plugin" / "plugin.json", ROOT / ".codex-plugin" / "plugin.json"):
            payload = json.loads(manifest.read_text())
            self.assertEqual(payload["name"], "tether")
            self.assertEqual(payload["version"], package["version"])
        plugin_manifest = (ROOT / "runtime" / "plugin" / "plugin.yaml").read_text()
        self.assertIn(f"version: {package['version']}", plugin_manifest)


if __name__ == "__main__":
    unittest.main()
