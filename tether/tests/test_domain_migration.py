import hashlib
import importlib.util
import json
import os
import pathlib
import sqlite3
import stat
import sys
import tempfile
import unittest
import uuid

from test_bridge import load_runtime


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "runtime" / "domain_schema.py"


def load_schema():
    name = "tether_domain_migration_test"
    spec = importlib.util.spec_from_file_location(name, SCHEMA_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class DomainMigrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.schema = load_schema()
        self.store = self.runtime.Store(self.home / "bridges.db")
        self.descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U12345678",),
            policy_generation=1,
        )
        raw_rollback = self.schema.rollback_v18_to_v17

        def rollback_with_predecessor(connection, fault_inject=None):
            return raw_rollback(
                connection,
                fault_inject,
                legacy_source_validator=self.validate_legacy_source,
            )

        self.schema.rollback_v18_to_v17 = rollback_with_predecessor

    def tearDown(self):
        self.temp.cleanup()

    def bridge(self, key, source=None, channel="C12345678"):
        bridge = self.store.create(
            {
                "source_kind": "headless_run",
                "source": source or {"run_id": "shared-run", "cwd": "/tmp/project"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": channel,
                "idempotency_key": key,
            }
        )
        return self.store.bind(bridge.bridge_id, f"1786000000.{len(key):06d}")

    def resolve_endpoint(self, row):
        raw = json.loads(str(row["source_json"]))
        source, binding = self.runtime._canonical_source(
            str(row["source_kind"]),
            raw,
            allow_legacy=True,
        )
        try:
            endpoint_key = self.runtime.endpoint_identity_key(binding)
        except ValueError:
            endpoint_key = None
        return self.schema.LegacyEndpointRef(
            endpoint_key=endpoint_key,
            candidate_endpoint_key=endpoint_key,
            endpoint_kind=binding.endpoint_kind,
            source_kind=str(row["source_kind"]),
            source_json=json.dumps(source, sort_keys=True, separators=(",", ":")),
            ref_version=binding.version,
            ready=str(row["binding_state"]) == "verified" and endpoint_key is not None,
            error_code=str(row["binding_error_code"] or "") or None,
        )

    def validate_legacy_source(self, source_kind, source_json, ref_version):
        source = json.loads(source_json)
        _validated, binding = self.runtime._canonical_source(
            source_kind,
            source,
            allow_legacy=True,
        )
        if binding.version != ref_version:
            raise ValueError("legacy source reference version mismatch")

    def migrate(self, fault_inject=None):
        connection = sqlite3.connect(self.store.path)
        try:
            self.schema.migrate_legacy_v17(
                connection,
                self.descriptor,
                self.resolve_endpoint,
                fault_inject,
            )
        finally:
            connection.close()

    def insert_terminal_response_v18(
        self,
        connection,
        *,
        binding_id,
        event_key,
        attempt_id,
        response_inline=None,
        response_ref=None,
    ):
        binding = connection.execute(
            """
            SELECT binding.endpoint_id,binding.generation,endpoint.incarnation,
                   endpoint.next_lease_fence
            FROM thread_bindings AS binding
            JOIN endpoints AS endpoint ON endpoint.endpoint_id=binding.endpoint_id
            WHERE binding.binding_id=?
            """,
            (binding_id,),
        ).fetchone()
        fence = int(binding["next_lease_fence"]) + 1
        body = response_inline if response_inline is not None else "referenced response"
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("PRAGMA defer_foreign_keys=ON")
        connection.execute(
            "UPDATE endpoints SET next_lease_fence=? WHERE endpoint_id=?",
            (fence, binding["endpoint_id"]),
        )
        connection.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES(?,?,?,?,datetime('now','+30 minutes'))
            """,
            (
                attempt_id,
                binding["endpoint_id"],
                binding["incarnation"],
                fence,
            ),
        )
        connection.execute(
            """
            INSERT INTO native_attempts(
              attempt_id,endpoint_id,binding_id,binding_generation,driver_kind,
              driver_request_id,driver_request_hash,reply_token_hash,state
            ) VALUES(?,?,?,?, 'detached_native',?,?,?,'prepared')
            """,
            (
                attempt_id,
                binding["endpoint_id"],
                binding_id,
                binding["generation"],
                f"submit:{attempt_id}",
                hashlib.sha256(f"submit:{attempt_id}".encode()).hexdigest(),
                hashlib.sha256(f"token:{attempt_id}".encode()).hexdigest(),
            ),
        )
        connection.execute(
            """
            INSERT INTO native_attempt_turns(
              attempt_id,ordinal,event_key,binding_id,turn_binding_generation
            ) VALUES(?,0,?,?,?)
            """,
            (attempt_id, event_key, binding_id, binding["generation"]),
        )
        connection.execute(
            """
            UPDATE native_attempts SET state='submitting',submitted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        connection.execute(
            """
            UPDATE native_attempts SET state='accepted',accepted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        response_hash = hashlib.sha256(body.encode()).hexdigest()
        connection.execute(
            """
            INSERT INTO driver_receipts(
              receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
              driver_kind,driver_incarnation,operation,request_id,request_hash,
              watch_cursor,state,response_ref,response_sha256,observed_at
            ) VALUES(?,?,?, ?,1,'detached_native','process-fixture','submit',?,?,?,
                     'completed_with_response',?,?,CURRENT_TIMESTAMP)
            """,
            (
                f"receipt:{attempt_id}",
                attempt_id,
                binding["endpoint_id"],
                fence,
                f"submit:{attempt_id}",
                hashlib.sha256(f"submit:{attempt_id}".encode()).hexdigest(),
                f"cursor:{attempt_id}",
                response_ref or f"inline-sha256:{response_hash}",
                response_hash,
            ),
        )
        connection.execute(
            """
            UPDATE native_attempts SET state='completed_with_response',
              response_inline=?,response_ref=?,response_sha256=?,response_bytes=?,
              last_driver_receipt_id=?,last_driver_sequence=1,receipt_cursor=?,
              terminal_at=CURRENT_TIMESTAMP WHERE attempt_id=?
            """,
            (
                response_inline,
                response_ref,
                response_hash,
                len(body.encode()),
                f"receipt:{attempt_id}",
                f"cursor:{attempt_id}",
                attempt_id,
            ),
        )
        connection.execute(
            """
            UPDATE queued_turns SET state='completed',terminal_at=CURRENT_TIMESTAMP,
              error_code=NULL WHERE event_key=?
            """,
            (event_key,),
        )
        connection.execute(
            """
            UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
              release_reason='completed_with_response' WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        connection.commit()

    def insert_terminal_signal_v18(
        self,
        connection,
        *,
        binding_id,
        event_key,
        attempt_id,
        receipt_state,
        submit_attempted=False,
    ):
        binding = connection.execute(
            """
            SELECT binding.endpoint_id,binding.generation,endpoint.incarnation,
                   endpoint.next_lease_fence,turn.binding_generation AS turn_generation
            FROM thread_bindings AS binding
            JOIN endpoints AS endpoint ON endpoint.endpoint_id=binding.endpoint_id
            JOIN queued_turns AS turn ON turn.binding_id=binding.binding_id
            WHERE binding.binding_id=? AND turn.event_key=?
            """,
            (binding_id, event_key),
        ).fetchone()
        fence = int(binding["next_lease_fence"]) + 1
        terminal_state = (
            "failed_before_start" if receipt_state == "not_started" else receipt_state
        )
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("PRAGMA defer_foreign_keys=ON")
        connection.execute(
            "UPDATE endpoints SET next_lease_fence=? WHERE endpoint_id=?",
            (fence, binding["endpoint_id"]),
        )
        connection.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES(?,?,?,?,datetime('now','+30 minutes'))
            """,
            (
                attempt_id,
                binding["endpoint_id"],
                binding["incarnation"],
                fence,
            ),
        )
        connection.execute(
            """
            INSERT INTO native_attempts(
              attempt_id,endpoint_id,binding_id,binding_generation,driver_kind,
              driver_request_id,driver_request_hash,reply_token_hash,state
            ) VALUES(?,?,?,?, 'detached_native',?,?,?,'prepared')
            """,
            (
                attempt_id,
                binding["endpoint_id"],
                binding_id,
                binding["generation"],
                f"submit:{attempt_id}",
                hashlib.sha256(f"submit:{attempt_id}".encode()).hexdigest(),
                hashlib.sha256(f"token:{attempt_id}".encode()).hexdigest(),
            ),
        )
        connection.execute(
            """
            INSERT INTO native_attempt_turns(
              attempt_id,ordinal,event_key,binding_id,turn_binding_generation
            ) VALUES(?,0,?,?,?)
            """,
            (attempt_id, event_key, binding_id, binding["turn_generation"]),
        )
        if receipt_state != "not_started" or submit_attempted:
            connection.execute(
                """
                UPDATE native_attempts SET state='submitting',
                  submitted_at=CURRENT_TIMESTAMP WHERE attempt_id=?
                """,
                (attempt_id,),
            )
            if receipt_state != "not_started":
                connection.execute(
                    """
                    UPDATE native_attempts SET state='accepted',
                      accepted_at=CURRENT_TIMESTAMP WHERE attempt_id=?
                    """,
                    (attempt_id,),
                )
        receipt_id = f"receipt:{attempt_id}"
        cursor = f"cursor:{attempt_id}"
        connection.execute(
            """
            INSERT INTO driver_receipts(
              receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
              driver_kind,driver_incarnation,operation,request_id,request_hash,
              watch_cursor,state,observed_at
            ) VALUES(?,?,?,?,1,'detached_native','process-fixture','submit',?,?,?,?,
                     CURRENT_TIMESTAMP)
            """,
            (
                receipt_id,
                attempt_id,
                binding["endpoint_id"],
                fence,
                f"submit:{attempt_id}",
                hashlib.sha256(f"submit:{attempt_id}".encode()).hexdigest(),
                cursor,
                receipt_state,
            ),
        )
        connection.execute(
            """
            UPDATE native_attempts SET state=?,last_driver_receipt_id=?,
              last_driver_sequence=1,receipt_cursor=?,terminal_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (terminal_state, receipt_id, cursor, attempt_id),
        )
        if receipt_state == "no_reply":
            connection.execute(
                """
                UPDATE queued_turns SET state='completed',terminal_at=CURRENT_TIMESTAMP
                WHERE event_key=?
                """,
                (event_key,),
            )
        elif receipt_state in {"failed", "cancelled"}:
            connection.execute(
                """
                UPDATE queued_turns SET state='cancelled',terminal_at=CURRENT_TIMESTAMP
                WHERE event_key=?
                """,
                (event_key,),
            )
        connection.execute(
            """
            UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
              release_reason=? WHERE attempt_id=?
            """,
            (terminal_state, attempt_id),
        )
        connection.commit()

    def test_one_endpoint_many_bindings_and_open_attempt_migrate_losslessly(self):
        first = self.bridge("first", channel="C12345678")
        second = self.bridge("second", channel="C87654321")
        self.assertTrue(self.store.enqueue_event("event-1", first.bridge_id, "run first"))
        items = self.store.claim_event_batch(first.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            first.bridge_id,
            ["event-1"],
            first.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [item["event_id"] for item in items],
                first.bridge_id,
                first.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id,
                first.bridge_id,
                first.binding_generation,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                attempt_id,
                first.bridge_id,
                first.binding_generation,
            )
        )
        self.assertTrue(self.store.enqueue_event("event-2", second.bridge_id, "run second"))

        before_connection = sqlite3.connect(self.store.path)
        try:
            before = self.schema.logical_manifest_v17(before_connection)
        finally:
            before_connection.close()
        backup_path = self.home / "bridges.schema17.backup.db"
        self.schema.backup_database(self.store.path, backup_path)
        self.assertEqual(stat.S_IMODE(backup_path.stat().st_mode), 0o600)
        backup_connection = sqlite3.connect(backup_path)
        try:
            self.assertEqual(
                self.schema.logical_manifest_v17(backup_connection),
                before,
            )
        finally:
            backup_connection.close()

        self.migrate()

        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        try:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 18)
            self.assertEqual(connection.execute("SELECT count(*) FROM endpoints").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM thread_bindings").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT count(*) FROM queued_turns").fetchone()[0], 2)
            attempt = connection.execute(
                "SELECT state FROM native_attempts WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()
            self.assertEqual(attempt["state"], "uncertain")
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM endpoint_leases WHERE released_at IS NULL"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(self.schema.invariant_violations(connection), [])
            after = self.schema.logical_manifest_v18(connection)
            for key in (
                "binding_count",
                "binding_ids",
                "turn_count",
                "turn_payloads",
                "attempt_count",
                "attempt_ids",
            ):
                self.assertEqual(after[key], before[key], key)
            legacy = connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name IN ('bridges','bridge_events','bridge_attempts')
                """
            ).fetchall()
            self.assertEqual(legacy, [])
        finally:
            connection.close()

    def test_active_binding_without_turns_migrates(self):
        bridge = self.bridge("idle-binding", channel="C12345678")

        self.migrate()

        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        try:
            binding = connection.execute(
                "SELECT state FROM thread_bindings WHERE binding_id=?",
                (bridge.bridge_id,),
            ).fetchone()
            self.assertEqual(binding["state"], "active")
            self.assertEqual(
                connection.execute("SELECT count(*) FROM queued_turns").fetchone()[0],
                0,
            )
            self.schema.require_valid(connection)
        finally:
            connection.close()

    def test_turn_order_survives_rollback_and_reupgrade(self):
        bridge = self.bridge("turn-order", channel="C12345678")
        self.migrate()

        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        rows = (
            ("event-a", "2026-08-18 05:00:02", "2026-08-18 05:00:01", "a"),
            ("event-b", "2026-08-18 05:00:01", "2026-08-18 05:00:02", "b"),
        )
        for event_key, ordered_at, created_at, body in rows:
            connection.execute(
                """
                INSERT INTO queued_turns(
                  event_key,binding_id,binding_generation,ordered_at,mutation_kind,
                  payload_inline,payload_sha256,payload_bytes,state,created_at,updated_at
                ) VALUES(?,?,1,?,'create',?,?,?,'ready',?,?)
                """,
                (
                    event_key,
                    bridge.bridge_id,
                    ordered_at,
                    body,
                    hashlib.sha256(body.encode()).hexdigest(),
                    len(body.encode()),
                    created_at,
                    created_at,
                ),
            )
        connection.commit()
        before = connection.execute(
            "SELECT event_key FROM queued_turns ORDER BY ordered_at,event_key"
        ).fetchall()
        before_manifest = self.schema.logical_manifest_v18(connection)

        self.schema.rollback_v18_to_v17(connection)
        rollback_copy = self.home / "ordered-rollback.db"
        copy_connection = sqlite3.connect(rollback_copy)
        connection.backup(copy_connection)
        copy_connection.close()
        rollback_store = self.runtime.Store(rollback_copy)
        claimed = rollback_store.claim_event_batch(bridge.bridge_id)
        self.assertEqual(
            [item["event_id"] for item in claimed],
            ["event-b", "event-a"],
        )
        connection.close()
        self.migrate()

        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        after = restored.execute(
            "SELECT event_key FROM queued_turns ORDER BY ordered_at,event_key"
        ).fetchall()
        after_manifest = self.schema.logical_manifest_v18(restored)
        self.assertEqual([row[0] for row in before], ["event-b", "event-a"])
        self.assertEqual(after, before)
        self.assertEqual(after_manifest["turn_order"], before_manifest["turn_order"])
        self.schema.require_valid(restored)
        restored.close()

    def test_fresh_empty_v18_database_rolls_back_and_boots_v17(self):
        fresh_path = self.home / "fresh-v18.db"
        connection = sqlite3.connect(fresh_path)
        self.schema.install_schema(connection)
        connection.execute("PRAGMA user_version=18")
        connection.commit()
        self.schema.require_valid(connection)

        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        legacy = self.runtime.Store(fresh_path)
        with legacy.connect() as reopened:
            self.assertEqual(reopened.execute("PRAGMA user_version").fetchone()[0], 17)
            self.assertEqual(
                reopened.execute("SELECT count(*) FROM bridges").fetchone()[0],
                0,
            )

    def test_rollback_rejects_source_the_pinned_predecessor_cannot_open(self):
        bridge = self.bridge("invalid-predecessor-source", channel="C12345678")
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        endpoint = connection.execute(
            """
            SELECT endpoint.endpoint_id,endpoint.security_domain_id
            FROM endpoints AS endpoint
            JOIN thread_bindings AS binding ON binding.endpoint_id=endpoint.endpoint_id
            WHERE binding.binding_id=?
            """,
            (bridge.bridge_id,),
        ).fetchone()
        connection.execute(
            """
            INSERT INTO thread_bindings(
              binding_id,endpoint_id,security_domain_id,team_id,channel_id,
              thread_ts,owner_user_id,idempotency_key,request_hash,generation,state
            ) VALUES('brg-postcutover-invalid',?,?,'T12345678','C87654321',
                     '1786000999.000001','U12345678','postcutover-invalid',?,1,'active')
            """,
            (
                endpoint["endpoint_id"],
                endpoint["security_domain_id"],
                hashlib.sha256(b"postcutover-invalid").hexdigest(),
            ),
        )
        connection.execute(
            """
            UPDATE endpoints SET source_json='{"run_id":""}',incarnation=incarnation+1
            WHERE endpoint_id=(
              SELECT endpoint_id FROM thread_bindings WHERE binding_id=?
            )
            """,
            (bridge.bridge_id,),
        )
        connection.commit()

        with self.assertRaisesRegex(
            RuntimeError,
            "rollback_source_incompatible:brg-postcutover-invalid",
        ):
            self.schema.rollback_v18_to_v17(connection)
        self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 18)
        self.assertEqual(
            connection.execute("SELECT count(*) FROM endpoints").fetchone()[0],
            1,
        )
        connection.close()

    def test_legacy_manifest_covers_routing_turn_and_attempt_semantics(self):
        bridge = self.bridge("manifest", channel="C12345678")
        self.assertTrue(self.store.enqueue_event("event-manifest", bridge.bridge_id, "run"))
        items = self.store.claim_event_batch(bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [item["event_id"] for item in items],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                ["event-manifest"],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        connection = sqlite3.connect(self.store.path)
        try:
            before = self.schema.logical_manifest_v17(connection)
            connection.execute(
                """
                UPDATE bridges SET channel_id='C87654321',
                  binding_error_code='synthetic_binding_error'
                WHERE bridge_id=?
                """,
                (bridge.bridge_id,),
            )
            after_binding = self.schema.logical_manifest_v17(connection)
            self.assertNotEqual(
                before["binding_records"], after_binding["binding_records"]
            )
            connection.execute(
                """
                UPDATE bridge_events SET state='failed',error='synthetic_turn_error'
                WHERE event_id='event-manifest'
                """
            )
            after_turn = self.schema.logical_manifest_v17(connection)
            self.assertNotEqual(
                after_binding["turn_records"], after_turn["turn_records"]
            )
            connection.execute(
                """
                UPDATE bridge_attempts SET delivery_kind='zellij',
                  state='failed',error_code='synthetic_attempt_error'
                WHERE attempt_id=?
                """,
                (attempt_id,),
            )
            after_attempt = self.schema.logical_manifest_v17(connection)
            self.assertNotEqual(
                after_turn["attempt_records"], after_attempt["attempt_records"]
            )
        finally:
            connection.close()

    def test_replying_attempt_with_durable_payload_cannot_replay_after_migration(self):
        bridge = self.bridge("replying", channel="C12345678")
        self.assertTrue(self.store.enqueue_event("event-replying", bridge.bridge_id, "run"))
        items = self.store.claim_event_batch(bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [item["event_id"] for item in items],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                ["event-replying"],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.runtime.stage_reply_payload(
            self.store,
            bridge.bridge_id,
            attempt_id,
            "finished response",
        )

        self.migrate()

        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        try:
            attempt = connection.execute(
                """
                SELECT attempt.state,lease.released_at
                FROM native_attempts AS attempt
                JOIN endpoint_leases AS lease USING(attempt_id)
                WHERE attempt.attempt_id=?
                """,
                (attempt_id,),
            ).fetchone()
            turn = connection.execute(
                "SELECT state FROM queued_turns WHERE event_key='event-replying'"
            ).fetchone()
            self.assertEqual(attempt["state"], "completed_with_response")
            self.assertIsNotNone(attempt["released_at"])
            self.assertEqual(turn["state"], "completed")
            self.assertEqual(self.schema.invariant_violations(connection), [])
        finally:
            connection.close()

    def test_orphan_event_attempt_reference_aborts_atomically(self):
        bridge = self.bridge("orphan", channel="C12345678")
        self.assertTrue(self.store.enqueue_event("event-orphan", bridge.bridge_id, "run"))
        with self.store.connect() as connection:
            connection.execute(
                """
                UPDATE bridge_events SET attempt_id='att_missing000000000000000',
                  binding_generation=? WHERE event_id='event-orphan'
                """,
                (bridge.binding_generation,),
            )

        with self.assertRaisesRegex(RuntimeError, "migration preflight"):
            self.migrate()

        connection = sqlite3.connect(self.store.path)
        try:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 17)
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM sqlite_master WHERE name='endpoints'"
                ).fetchone()[0],
                0,
            )
        finally:
            connection.close()

    def test_legacy_driver_endpoint_mismatch_aborts_atomically(self):
        bridge = self.bridge("driver-endpoint-mismatch", channel="C12345678")
        event_key = "event-driver-endpoint-mismatch"
        self.assertTrue(self.store.enqueue_event(event_key, bridge.bridge_id, "run"))
        self.store.claim_event_batch(bridge.bridge_id)
        attempt_id = "att-driver-endpoint-mismatch"
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [event_key], bridge.bridge_id, bridge.binding_generation,
                attempt_id, delivery_kind="herdr",
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                attempt_id, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.assertEqual(
            self.store.acknowledge_attempt(
                attempt_id, bridge.bridge_id, ack_kind="no_reply"
            ),
            1,
        )
        connection = sqlite3.connect(self.store.path)
        before = self.schema.logical_manifest_v17(connection)
        connection.close()

        with self.assertRaisesRegex(RuntimeError, "attempt_driver_endpoint_mismatch"):
            self.migrate()

        connection = sqlite3.connect(self.store.path)
        self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 17)
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM sqlite_master WHERE name='endpoints'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM bridge_attempts WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()[0],
            1,
        )
        self.assertEqual(self.schema.logical_manifest_v17(connection), before)
        connection.close()

    def test_every_forward_migration_phase_rolls_back_atomically(self):
        bridge = self.bridge("faults", channel="C12345678")
        self.assertTrue(self.store.enqueue_event("event-faults", bridge.bridge_id, "run"))
        baseline_connection = sqlite3.connect(self.store.path)
        try:
            baseline = self.schema.logical_manifest_v17(baseline_connection)
        finally:
            baseline_connection.close()

        checkpoints = (
            "after_preflight",
            "after_schema",
            "after_endpoints",
            "after_bindings",
            "after_turns",
            "after_attempts",
            "after_validation",
            "after_legacy_drop",
            "after_version",
        )
        for checkpoint in checkpoints:
            with self.subTest(checkpoint=checkpoint):
                def inject(current, *, expected=checkpoint):
                    if current == expected:
                        raise RuntimeError(f"synthetic fault at {current}")

                with self.assertRaisesRegex(RuntimeError, "synthetic fault"):
                    self.migrate(inject)
                connection = sqlite3.connect(self.store.path)
                try:
                    self.assertEqual(
                        self.schema.logical_manifest_v17(connection), baseline
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT count(*) FROM sqlite_master WHERE name='endpoints'"
                        ).fetchone()[0],
                        0,
                    )
                finally:
                    connection.close()

    def test_every_schema_rollback_phase_restores_schema18_atomically(self):
        bridge = self.bridge("rollback-faults", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-rollback-faults", bridge.bridge_id, "run")
        )
        self.migrate()
        baseline_connection = sqlite3.connect(self.store.path)
        try:
            baseline = self.schema.logical_manifest_v18(baseline_connection)
        finally:
            baseline_connection.close()

        checkpoints = (
            "after_preflight",
            "after_legacy_schema",
            "after_bindings",
            "after_turns",
            "after_attempts",
            "after_validation",
            "after_v18_drop",
            "after_version",
        )
        for checkpoint in checkpoints:
            with self.subTest(checkpoint=checkpoint):
                def inject(current, *, expected=checkpoint):
                    if current == expected:
                        raise RuntimeError(f"synthetic rollback fault at {current}")

                connection = sqlite3.connect(self.store.path)
                try:
                    with self.assertRaisesRegex(
                        RuntimeError, "synthetic rollback fault"
                    ):
                        self.schema.rollback_v18_to_v17(connection, inject)
                    self.assertEqual(
                        self.schema.logical_manifest_v18(connection), baseline
                    )
                    self.assertEqual(
                        connection.execute(
                            "SELECT count(*) FROM sqlite_master WHERE name='bridges'"
                        ).fetchone()[0],
                        0,
                    )
                finally:
                    connection.close()

    def test_pending_binding_with_thread_is_quarantined_not_promoted(self):
        bridge = self.bridge("pending-thread", channel="C12345678")
        with self.store.connect() as connection:
            connection.execute(
                "UPDATE bridges SET status='pending' WHERE bridge_id=?",
                (bridge.bridge_id,),
            )

        self.migrate()

        connection = sqlite3.connect(self.store.path)
        try:
            state = connection.execute(
                "SELECT state FROM thread_bindings WHERE binding_id=?",
                (bridge.bridge_id,),
            ).fetchone()[0]
            self.assertEqual(state, "rebind_required")
        finally:
            connection.close()

    def test_unauthorized_legacy_owner_aborts_before_schema_change(self):
        self.bridge("unauthorized-owner", channel="C12345678")
        descriptor = self.schema.SecurityDomainDescriptor(
            instance_uid=os.geteuid(),
            workspace_id="T12345678",
            persona_id="primary",
            authorized_owner_ids=("U99999999",),
            policy_generation=1,
        )
        connection = sqlite3.connect(self.store.path)
        try:
            with self.assertRaisesRegex(RuntimeError, "outside the security domain"):
                self.schema.migrate_legacy_v17(
                    connection,
                    descriptor,
                    self.resolve_endpoint,
                )
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 17)
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM sqlite_master WHERE name='endpoints'"
                ).fetchone()[0],
                0,
            )
        finally:
            connection.close()

    def test_null_legacy_turn_generation_uses_current_binding_generation(self):
        bridge = self.bridge("null-generation", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-null-generation", bridge.bridge_id, "run")
        )
        with self.store.connect() as connection:
            connection.execute(
                "UPDATE bridges SET binding_generation=2 WHERE bridge_id=?",
                (bridge.bridge_id,),
            )
            connection.execute(
                """
                UPDATE bridge_events SET binding_generation=NULL
                WHERE event_id='event-null-generation'
                """
            )

        self.migrate()

        connection = sqlite3.connect(self.store.path)
        try:
            self.assertEqual(
                connection.execute(
                    """
                    SELECT binding_generation FROM queued_turns
                    WHERE event_key='event-null-generation'
                    """
                ).fetchone()[0],
                2,
            )
        finally:
            connection.close()

    def test_submitted_legacy_failure_is_terminal_and_releases_endpoint(self):
        bridge = self.bridge("submitted-failure", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-submitted-failure", bridge.bridge_id, "run")
        )
        items = self.store.claim_event_batch(bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [item["event_id"] for item in items],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                ["event-submitted-failure"],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.assertTrue(
            self.store.fail_attempt(
                attempt_id,
                bridge.bridge_id,
                "known terminal failure",
            )
        )

        self.migrate()

        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        try:
            attempt = connection.execute(
                """
                SELECT attempt.state,lease.released_at
                FROM native_attempts AS attempt
                JOIN endpoint_leases AS lease USING(attempt_id)
                WHERE attempt.attempt_id=?
                """,
                (attempt_id,),
            ).fetchone()
            self.assertEqual(attempt["state"], "failed")
            self.assertIsNotNone(attempt["released_at"])
            self.assertEqual(
                connection.execute(
                    "SELECT state FROM queued_turns WHERE event_key='event-submitted-failure'"
                ).fetchone()[0],
                "cancelled",
            )
            self.assertEqual(self.schema.invariant_violations(connection), [])
        finally:
            connection.close()

    def test_pre_submit_legacy_failure_is_terminal_without_claiming_execution(self):
        bridge = self.bridge("pre-submit-failure", channel="C12345678")
        event_key = "event-pre-submit-failure"
        self.assertTrue(self.store.enqueue_event(event_key, bridge.bridge_id, "run"))
        self.assertEqual(
            [item["event_id"] for item in self.store.claim_event_batch(bridge.bridge_id)],
            [event_key],
        )
        attempt_id = "att-pre-submit-failure-0001"
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [event_key],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertEqual(
            self.store.fail_attempt(attempt_id, bridge.bridge_id, "local preflight failed"),
            1,
        )

        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        attempt = connection.execute(
            """
            SELECT attempt.state,attempt.submitted_at,attempt.error_code,
                   lease.released_at
            FROM native_attempts AS attempt
            JOIN endpoint_leases AS lease ON lease.attempt_id=attempt.attempt_id
            WHERE attempt.attempt_id=?
            """,
            (attempt_id,),
        ).fetchone()
        self.assertEqual(attempt["state"], "cancelled")
        self.assertIsNone(attempt["submitted_at"])
        self.assertEqual(attempt["error_code"], "local preflight failed")
        self.assertIsNotNone(attempt["released_at"])
        self.assertEqual(
            connection.execute(
                "SELECT state FROM queued_turns WHERE event_key=?", (event_key,)
            ).fetchone()[0],
            "cancelled",
        )
        self.schema.require_valid(connection)
        connection.close()

    def test_submitted_requeue_keeps_submit_history_on_first_migration(self):
        bridge = self.bridge("submitted-requeue", channel="C12345678")
        event_key = "event-submitted-requeue"
        self.assertTrue(self.store.enqueue_event(event_key, bridge.bridge_id, "run"))
        self.store.claim_event_batch(bridge.bridge_id)
        attempt_id = "att-submitted-requeue-00001"
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [event_key],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.assertTrue(
            self.store.requeue_prepared_attempt(
                attempt_id, bridge.bridge_id, "terminal_submit_not_started"
            )
        )

        self.migrate()
        connection = sqlite3.connect(self.store.path)
        attempt = connection.execute(
            """
            SELECT state,submitted_at FROM native_attempts WHERE attempt_id=?
            """,
            (attempt_id,),
        ).fetchone()
        self.assertEqual(attempt[0], "failed_before_start")
        self.assertIsNotNone(attempt[1])
        self.assertEqual(self.schema.invariant_violations(connection), [])
        connection.close()

    def test_terminal_attempts_migrate_before_one_open_endpoint_attempt(self):
        terminal_bridge = self.bridge("terminal-order-a", channel="C12345678")
        open_bridge = self.bridge("terminal-order-b", channel="C87654321")
        terminal_event = "event-terminal-order"
        open_event = "event-open-order"
        self.assertTrue(
            self.store.enqueue_event(terminal_event, terminal_bridge.bridge_id, "terminal")
        )
        self.assertTrue(
            self.store.enqueue_event(open_event, open_bridge.bridge_id, "open")
        )
        self.store.claim_event_batch(terminal_bridge.bridge_id)
        terminal_attempt = "zzzzzzzz-terminal-attempt"
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [terminal_event], terminal_bridge.bridge_id,
                terminal_bridge.binding_generation, terminal_attempt,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                terminal_attempt,
                terminal_bridge.bridge_id,
                terminal_bridge.binding_generation,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                terminal_attempt,
                terminal_bridge.bridge_id,
                terminal_bridge.binding_generation,
            )
        )
        self.assertEqual(
            self.store.acknowledge_attempt(
                terminal_attempt, terminal_bridge.bridge_id, ack_kind="no_reply"
            ),
            1,
        )

        self.store.claim_event_batch(open_bridge.bridge_id)
        open_attempt = "aaaaaaaa-open-attempt"
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [open_event], open_bridge.bridge_id, open_bridge.binding_generation,
                open_attempt, delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                open_attempt, open_bridge.bridge_id, open_bridge.binding_generation
            )
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                open_attempt, open_bridge.bridge_id, open_bridge.binding_generation
            )
        )

        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        leases = {
            row["attempt_id"]: row
            for row in connection.execute(
                """
                SELECT lease.attempt_id,lease.fence,lease.released_at,
                       endpoint.next_lease_fence
                FROM endpoint_leases AS lease
                JOIN endpoints AS endpoint ON endpoint.endpoint_id=lease.endpoint_id
                """
            )
        }
        self.assertIsNotNone(leases[terminal_attempt]["released_at"])
        self.assertIsNone(leases[open_attempt]["released_at"])
        self.assertGreater(
            leases[open_attempt]["fence"], leases[terminal_attempt]["fence"]
        )
        self.assertEqual(
            leases[open_attempt]["fence"], leases[open_attempt]["next_lease_fence"]
        )
        self.schema.require_valid(connection)
        connection.close()

    def test_conflicting_sibling_snapshots_quarantine_the_whole_endpoint(self):
        first = self.bridge(
            "first",
            source={"run_id": "shared-run", "cwd": "/tmp/project-a"},
            channel="C12345678",
        )
        second = self.bridge(
            "second",
            source={"run_id": "shared-run", "cwd": "/tmp/project-b"},
            channel="C87654321",
        )

        self.migrate()

        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        try:
            endpoint = connection.execute(
                "SELECT endpoint_key,candidate_endpoint_key,state,error_code FROM endpoints"
            ).fetchone()
            self.assertIsNone(endpoint[0])
            self.assertIsNotNone(endpoint[1])
            self.assertEqual(endpoint[2:], ("rebind_required", "legacy_endpoint_conflict"))
            states = connection.execute(
                "SELECT DISTINCT state FROM thread_bindings"
            ).fetchall()
            self.assertEqual([tuple(row) for row in states], [("rebind_required",)])
            self.schema.rollback_v18_to_v17(connection)
        finally:
            connection.close()

        legacy = self.runtime.Store(self.store.path)
        with legacy.connect() as reopened:
            sources = {
                str(row["bridge_id"]): json.loads(str(row["source_json"]))
                for row in reopened.execute(
                    "SELECT bridge_id,source_json FROM bridges ORDER BY bridge_id"
                )
            }
        self.assertEqual(sources[first.bridge_id]["cwd"], "/tmp/project-a")
        self.assertEqual(sources[second.bridge_id]["cwd"], "/tmp/project-b")
        self.migrate()
        restored = sqlite3.connect(self.store.path)
        self.assertEqual(
            restored.execute("SELECT count(*) FROM endpoints").fetchone()[0],
            1,
        )
        self.assertEqual(self.schema.invariant_violations(restored), [])
        restored.close()

    def test_quarantined_endpoint_keeps_released_terminal_history(self):
        first = self.bridge(
            "terminal-first",
            source={"run_id": "shared-run", "cwd": "/tmp/project-a"},
            channel="C12345678",
        )
        self.bridge(
            "terminal-second",
            source={"run_id": "shared-run", "cwd": "/tmp/project-b"},
            channel="C87654321",
        )
        attempt_id = "att_terminalhistory000000000"
        with self.store.connect() as connection:
            connection.execute(
                """
                INSERT INTO bridge_events(
                  event_id,bridge_id,state,payload_json,attempt_id,
                  binding_generation
                ) VALUES('event-terminal-history',?,'delivered','{"text":"run"}',?,?)
                """,
                (first.bridge_id, attempt_id, first.binding_generation),
            )
            connection.execute(
                """
                INSERT INTO bridge_attempts(
                  attempt_id,reply_key,bridge_id,binding_generation,
                  delivery_kind,state,ack_kind,acknowledged_at
                ) VALUES(?,?,?,?,?,'acknowledged','no_reply',CURRENT_TIMESTAMP)
                """,
                (
                    attempt_id,
                    attempt_id,
                    first.bridge_id,
                    first.binding_generation,
                    "detached_native",
                ),
            )

        self.migrate()

        connection = sqlite3.connect(self.store.path)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT state FROM native_attempts WHERE attempt_id=?",
                    (attempt_id,),
                ).fetchone()[0],
                "no_reply",
            )
            self.assertIsNotNone(
                connection.execute(
                    "SELECT released_at FROM endpoint_leases WHERE attempt_id=?",
                    (attempt_id,),
                ).fetchone()[0]
            )
            self.assertEqual(self.schema.invariant_violations(connection), [])
        finally:
            connection.close()

    def test_two_open_sibling_attempts_abort_without_partial_schema(self):
        first = self.bridge("first", channel="C12345678")
        second = self.bridge("second", channel="C87654321")
        with self.store.connect() as connection:
            for index, bridge in enumerate((first, second), start=1):
                event_id = f"collision-{index}"
                attempt_id = f"att_collision{index:0>14}"
                connection.execute(
                    """
                    INSERT INTO bridge_events(
                      event_id,bridge_id,state,payload_json,attempt_id,
                      binding_generation,updated_at
                    ) VALUES(?,?,'awaiting_ack','{"text":"run"}',?,?,CURRENT_TIMESTAMP)
                    """,
                    (event_id, bridge.bridge_id, attempt_id, bridge.binding_generation),
                )
                connection.execute(
                    """
                    INSERT INTO bridge_attempts(
                      attempt_id,reply_key,bridge_id,binding_generation,
                      delivery_kind,state,submitted_at
                    ) VALUES(?,?,?,?,?,'awaiting_ack',CURRENT_TIMESTAMP)
                    """,
                    (
                        attempt_id,
                        attempt_id,
                        bridge.bridge_id,
                        bridge.binding_generation,
                        "detached_native",
                    ),
                )

        with self.assertRaisesRegex(RuntimeError, "multiple potentially-started"):
            self.migrate()

        connection = sqlite3.connect(self.store.path)
        try:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 17)
            self.assertEqual(
                connection.execute(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='endpoints'"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(connection.execute("SELECT count(*) FROM bridges").fetchone()[0], 2)
        finally:
            connection.close()

    def test_rollback_preserves_post_migration_binding_turn_and_terminal(self):
        first = self.bridge("first", channel="C12345678")
        self.assertTrue(self.store.enqueue_event("event-before", first.bridge_id, "before"))
        self.migrate()

        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        endpoint = connection.execute("SELECT * FROM endpoints").fetchone()
        binding_id = "brg_postmigration000000000000"
        event_key = "event-after"
        attempt_id = "att_postmigration00000000000"
        request_hash = hashlib.sha256(b"post-migration-binding").hexdigest()
        response_token = hashlib.sha256(b"unusable-reply-token").hexdigest()
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("PRAGMA defer_foreign_keys=ON")
        connection.execute(
            """
            INSERT INTO thread_bindings(
              binding_id,endpoint_id,security_domain_id,team_id,channel_id,
              thread_ts,owner_user_id,idempotency_key,request_hash,
              generation,state
            ) VALUES(?,?,?,'T12345678','C87654321','1786000001.000001',
                     'U12345678','post-migration',?,1,'active')
            """,
            (
                binding_id,
                endpoint["endpoint_id"],
                endpoint["security_domain_id"],
                request_hash,
            ),
        )
        connection.execute(
            """
            INSERT INTO queued_turns(
              event_key,binding_id,binding_generation,ordered_at,mutation_kind,
              payload_inline,payload_sha256,payload_bytes,state
            ) VALUES(?,?,1,CURRENT_TIMESTAMP,'create','after',?,5,'ready')
            """,
            (event_key, binding_id, hashlib.sha256(b"after").hexdigest()),
        )
        connection.execute(
            "UPDATE endpoints SET next_lease_fence=1 WHERE endpoint_id=?",
            (endpoint["endpoint_id"],),
        )
        connection.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES(?,?,1,1,datetime('now','+30 minutes'))
            """,
            (attempt_id, endpoint["endpoint_id"]),
        )
        connection.execute(
            """
            INSERT INTO native_attempts(
              attempt_id,endpoint_id,binding_id,binding_generation,driver_kind,
              driver_request_id,driver_request_hash,reply_token_hash,state
            ) VALUES(?,?,?,1,'detached_native',?,?,?,'prepared')
            """,
            (
                attempt_id,
                endpoint["endpoint_id"],
                binding_id,
                f"submit:{attempt_id}",
                hashlib.sha256(f"submit:{attempt_id}".encode()).hexdigest(),
                response_token,
            ),
        )
        connection.execute(
            """
            INSERT INTO native_attempt_turns(
              attempt_id,ordinal,event_key,binding_id,turn_binding_generation
            ) VALUES(?,0,?,?,1)
            """,
            (attempt_id, event_key, binding_id),
        )
        connection.execute(
            """
            UPDATE native_attempts SET state='submitting',submitted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        connection.execute(
            """
            UPDATE native_attempts SET state='accepted',accepted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        connection.execute(
            """
            INSERT INTO driver_receipts(
              receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
              driver_kind,driver_incarnation,operation,request_id,request_hash,
              watch_cursor,state,observed_at
            ) VALUES('receipt-postmigration',?,?,1,1,'detached_native',
                     'process-fixture','submit',?,?,
                     'cursor-postmigration','no_reply',CURRENT_TIMESTAMP)
            """,
            (
                attempt_id,
                endpoint["endpoint_id"],
                f"submit:{attempt_id}",
                hashlib.sha256(f"submit:{attempt_id}".encode()).hexdigest(),
            ),
        )
        connection.execute(
            """
            UPDATE native_attempts SET state='no_reply',terminal_at=CURRENT_TIMESTAMP,
              last_driver_receipt_id='receipt-postmigration',
              last_driver_sequence=1,receipt_cursor='cursor-postmigration'
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        connection.execute(
            """
            UPDATE queued_turns SET state='completed',terminal_at=CURRENT_TIMESTAMP
            WHERE event_key=?
            """,
            (event_key,),
        )
        connection.execute(
            """
            UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
              release_reason='no_reply' WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        connection.commit()

        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        reopened = self.runtime.Store(self.store.path)
        binding = reopened.find(
            "T12345678",
            "C87654321",
            "1786000001.000001",
        )
        self.assertIsNotNone(binding)
        self.assertEqual(
            reopened.attempt_state(attempt_id, binding_id),
            "acknowledged",
        )
        with reopened.connect() as legacy:
            event = legacy.execute(
                "SELECT state FROM bridge_events WHERE event_id=?",
                (event_key,),
            ).fetchone()
        self.assertEqual(event["state"], "delivered")

    def test_rollback_requeues_undelivered_response_through_v17_outbox(self):
        bridge = self.bridge("response-rollback", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-response-rollback", bridge.bridge_id, "run")
        )
        self.migrate()

        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        attempt_id = "att_responserollback00000000"
        self.insert_terminal_response_v18(
            connection,
            binding_id=bridge.bridge_id,
            event_key="event-response-rollback",
            attempt_id=attempt_id,
            response_inline="response to deliver",
        )
        before = self.schema.logical_manifest_v18(connection)

        self.schema.rollback_v18_to_v17(connection)
        attempt = connection.execute(
            "SELECT state,ack_kind FROM bridge_attempts WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()
        reply = connection.execute(
            """
            SELECT state,payload_text,client_msg_id,message_ts
            FROM bridge_replies WHERE reply_key=?
            """,
            (attempt_id,),
        ).fetchone()
        event = connection.execute(
            "SELECT state FROM bridge_events WHERE event_id='event-response-rollback'"
        ).fetchone()
        self.assertEqual(tuple(attempt), ("replying", None))
        self.assertEqual(reply["state"], "pending")
        self.assertEqual(reply["payload_text"], "response to deliver")
        self.assertEqual(
            reply["client_msg_id"],
            str(uuid.uuid5(uuid.NAMESPACE_URL, f"tether:{attempt_id}")),
        )
        self.assertIsNone(reply["message_ts"])
        self.assertEqual(event["state"], "replying")
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM tether_domain_rollback_activity"
            ).fetchone()[0],
            0,
        )
        connection.close()
        legacy = self.runtime.Store(self.store.path)
        self.runtime.stage_reply_payload(
            legacy,
            bridge.bridge_id,
            attempt_id,
            "response to deliver",
        )
        with legacy.connect() as legacy_connection:
            self.assertEqual(
                legacy_connection.execute(
                    "SELECT count(*) FROM tether_domain_rollback_activity"
                ).fetchone()[0],
                0,
            )
        self.migrate()
        restored = sqlite3.connect(self.store.path)
        after = self.schema.logical_manifest_v18(restored)
        self.assertEqual(
            {key: before[key] for key in self.schema.PRESERVED_MANIFEST_KEYS},
            {key: after[key] for key in self.schema.PRESERVED_MANIFEST_KEYS},
        )
        self.assertEqual(
            restored.execute("SELECT count(*) FROM driver_receipts").fetchone()[0],
            1,
        )
        restored.close()

    def test_schema17_delivery_after_rollback_preserves_native_receipt(self):
        bridge = self.bridge("response-delivery", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-response-delivery", bridge.bridge_id, "run")
        )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        attempt_id = "att_response_delivery_000001"
        self.insert_terminal_response_v18(
            connection,
            binding_id=bridge.bridge_id,
            event_key="event-response-delivery",
            attempt_id=attempt_id,
            response_inline="response to deliver",
        )
        before_receipt = connection.execute(
            "SELECT * FROM driver_receipts WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()
        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        legacy = self.runtime.Store(self.store.path)
        claim = legacy.claim_reply(
            attempt_id,
            bridge.bridge_id,
            lease_owner="a" * 32,
        )
        self.assertEqual(claim["status"], "claimed")
        self.assertEqual(
            legacy.complete_reply(
                attempt_id,
                bridge.bridge_id,
                claim["lease_id"],
                "1786000999.000001",
            ),
            1,
        )
        self.migrate()

        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        after_receipt = restored.execute(
            "SELECT * FROM driver_receipts WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()
        self.assertEqual(dict(after_receipt), dict(before_receipt))
        self.assertEqual(
            restored.execute(
                "SELECT state FROM native_attempts WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()[0],
            "completed_with_response",
        )
        self.schema.require_valid(restored)
        restored.close()

    def test_rollback_refuses_ambiguous_slack_reply_without_demoting_it(self):
        bridge = self.bridge("ambiguous-reply", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-ambiguous-reply", bridge.bridge_id, "run")
        )
        items = self.store.claim_event_batch(bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [item["event_id"] for item in items],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                ["event-ambiguous-reply"],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.runtime.stage_reply_payload(
            self.store,
            bridge.bridge_id,
            attempt_id,
            "possibly delivered",
        )
        with self.store.connect() as legacy:
            legacy.execute(
                "UPDATE bridge_replies SET state='uncertain' WHERE reply_key=?",
                (attempt_id,),
            )
        self.migrate()

        connection = sqlite3.connect(self.store.path)
        for state in ("uncertain", "delivering"):
            with self.subTest(state=state):
                connection.execute(
                    "UPDATE bridge_replies SET state=? WHERE reply_key=?",
                    (state, attempt_id),
                )
                connection.commit()
                with self.assertRaisesRegex(RuntimeError, "ambiguous Slack reply"):
                    self.schema.rollback_v18_to_v17(connection)
                self.assertEqual(
                    connection.execute(
                        "SELECT state FROM bridge_replies WHERE reply_key=?",
                        (attempt_id,),
                    ).fetchone()[0],
                    state,
                )
                self.assertEqual(
                    connection.execute("PRAGMA user_version").fetchone()[0], 18
                )
        connection.close()

    def test_rollback_keeps_ready_pre_rebind_turn_runnable(self):
        bridge = self.bridge("rebind-ready", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-pre-rebind", bridge.bridge_id, "run")
        )
        self.migrate()

        connection = sqlite3.connect(self.store.path)
        connection.execute(
            """
            UPDATE thread_bindings SET state='rebind_required'
            WHERE binding_id=?
            """,
            (bridge.bridge_id,),
        )
        connection.execute(
            """
            UPDATE thread_bindings SET state='active',generation=generation+1,
              error_code=NULL WHERE binding_id=?
            """,
            (bridge.bridge_id,),
        )
        connection.commit()

        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        reopened = self.runtime.Store(self.store.path)
        claimed = reopened.claim_event_batch(bridge.bridge_id)
        self.assertEqual([item["event_id"] for item in claimed], ["event-pre-rebind"])

    def test_rollback_refuses_unmaterialized_response_without_partial_projection(self):
        bridge = self.bridge("response-ref", channel="C12345678")
        self.assertTrue(self.store.enqueue_event("event-response-ref", bridge.bridge_id, "run"))
        self.migrate()

        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        self.insert_terminal_response_v18(
            connection,
            binding_id=bridge.bridge_id,
            event_key="event-response-ref",
            attempt_id="att_responseref0000000000000",
            response_ref="sha256:synthetic-reference",
        )

        with self.assertRaisesRegex(RuntimeError, "non-materialized response"):
            self.schema.rollback_v18_to_v17(connection)
        self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 18)
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM sqlite_master WHERE name='bridges'"
            ).fetchone()[0],
            0,
        )
        connection.close()

    def test_rollback_refuses_open_lease_without_partial_projection(self):
        bridge = self.bridge("first", channel="C12345678")
        self.assertTrue(self.store.enqueue_event("event-open", bridge.bridge_id, "run"))
        items = self.store.claim_event_batch(bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            bridge.bridge_id,
            [item["event_id"] for item in items],
            bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                ["event-open"],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        self.migrate()

        connection = sqlite3.connect(self.store.path)
        with self.assertRaisesRegex(RuntimeError, "open endpoint lease"):
            self.schema.rollback_v18_to_v17(connection)
        self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 18)
        self.assertEqual(connection.execute("SELECT count(*) FROM endpoints").fetchone()[0], 1)
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM sqlite_master WHERE name='bridges'"
            ).fetchone()[0],
            0,
        )
        connection.close()

    def test_multi_generation_retry_history_roundtrips_without_reexecution(self):
        bridge = self.bridge("multi-generation", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-multi-generation", bridge.bridge_id, "run")
        )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        for index in (1, 2):
            self.insert_terminal_signal_v18(
                connection,
                binding_id=bridge.bridge_id,
                event_key="event-multi-generation",
                attempt_id=f"att-multi-not-started-{index:08d}",
                receipt_state="not_started",
            )
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE thread_bindings SET state='rebind_required'
                WHERE binding_id=?
                """,
                (bridge.bridge_id,),
            )
            connection.execute(
                """
                UPDATE thread_bindings SET state='active',generation=generation+1,
                  error_code=NULL WHERE binding_id=?
                """,
                (bridge.bridge_id,),
            )
            connection.commit()
        self.insert_terminal_signal_v18(
            connection,
            binding_id=bridge.bridge_id,
            event_key="event-multi-generation",
            attempt_id="att-multi-no-reply-00000001",
            receipt_state="no_reply",
        )
        before = self.schema.logical_manifest_v18(connection)
        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        self.migrate()
        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        after = self.schema.logical_manifest_v18(restored)
        self.assertEqual(
            {key: before[key] for key in self.schema.PRESERVED_MANIFEST_KEYS},
            {key: after[key] for key in self.schema.PRESERVED_MANIFEST_KEYS},
        )
        self.assertEqual(
            restored.execute(
                """
                SELECT count(*) FROM native_attempt_turns
                WHERE event_key='event-multi-generation'
                """
            ).fetchone()[0],
            3,
        )
        self.assertEqual(
            restored.execute(
                "SELECT state FROM queued_turns WHERE event_key='event-multi-generation'"
            ).fetchone()[0],
            "completed",
        )
        self.schema.require_valid(restored)
        restored.close()

    def test_endpoint_repoint_preserves_binding_and_attempt_identity_roundtrip(self):
        first = self.bridge(
            "endpoint-a",
            source={"run_id": "endpoint-a", "cwd": "/tmp/project"},
            channel="C12345678",
        )
        second = self.bridge(
            "endpoint-b",
            source={"run_id": "endpoint-b", "cwd": "/tmp/project"},
            channel="C87654321",
        )
        self.assertTrue(
            self.store.enqueue_event("event-endpoint-repoint", first.bridge_id, "run")
        )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        endpoint_a = connection.execute(
            "SELECT endpoint_id FROM thread_bindings WHERE binding_id=?",
            (first.bridge_id,),
        ).fetchone()[0]
        endpoint_b = connection.execute(
            "SELECT endpoint_id FROM thread_bindings WHERE binding_id=?",
            (second.bridge_id,),
        ).fetchone()[0]
        self.assertNotEqual(endpoint_a, endpoint_b)
        self.insert_terminal_signal_v18(
            connection,
            binding_id=first.bridge_id,
            event_key="event-endpoint-repoint",
            attempt_id="att-endpoint-a-not-started",
            receipt_state="not_started",
        )
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE thread_bindings SET state='rebind_required' WHERE binding_id=?",
            (first.bridge_id,),
        )
        connection.execute(
            """
            UPDATE thread_bindings SET endpoint_id=?,state='active',
              generation=generation+1,error_code=NULL WHERE binding_id=?
            """,
            (endpoint_b, first.bridge_id),
        )
        connection.commit()
        self.insert_terminal_signal_v18(
            connection,
            binding_id=first.bridge_id,
            event_key="event-endpoint-repoint",
            attempt_id="att-endpoint-b-no-reply",
            receipt_state="no_reply",
        )
        before = self.schema.logical_manifest_v18(connection)
        before_attempts = connection.execute(
            "SELECT attempt_id,endpoint_id FROM native_attempts ORDER BY attempt_id"
        ).fetchall()
        before_binding = connection.execute(
            """
            SELECT endpoint_id,request_hash,generation FROM thread_bindings
            WHERE binding_id=?
            """,
            (first.bridge_id,),
        ).fetchone()

        self.schema.rollback_v18_to_v17(connection)
        connection.close()
        self.migrate()

        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        after = self.schema.logical_manifest_v18(restored)
        self.assertEqual(
            {key: before[key] for key in self.schema.PRESERVED_MANIFEST_KEYS},
            {key: after[key] for key in self.schema.PRESERVED_MANIFEST_KEYS},
        )
        self.assertEqual(
            restored.execute(
                "SELECT attempt_id,endpoint_id FROM native_attempts ORDER BY attempt_id"
            ).fetchall(),
            before_attempts,
        )
        self.assertEqual(
            restored.execute(
                """
                SELECT endpoint_id,request_hash,generation FROM thread_bindings
                WHERE binding_id=?
                """,
                (first.bridge_id,),
            ).fetchone(),
            before_binding,
        )
        self.schema.require_valid(restored)
        restored.close()

    def test_schema17_rebind_after_rollback_becomes_new_endpoint(self):
        bridge = self.bridge(
            "rollback-rebind",
            source={"run_id": "before-rollback", "cwd": "/tmp/project"},
        )
        self.assertTrue(
            self.store.enqueue_event(
                "event-rollback-rebind", bridge.bridge_id, "queued before rollback"
            )
        )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        old_endpoint = connection.execute(
            "SELECT endpoint_id FROM thread_bindings WHERE binding_id=?",
            (bridge.bridge_id,),
        ).fetchone()[0]
        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        legacy = self.runtime.Store(self.store.path)
        rebound = legacy.rebind(
            bridge.bridge_id,
            "headless_run",
            {"run_id": "after-rollback", "cwd": "/tmp/project"},
            expected_generation=bridge.binding_generation,
        )
        self.assertEqual(rebound.binding_generation, bridge.binding_generation + 1)
        self.migrate()

        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        binding = restored.execute(
            """
            SELECT endpoint_id,generation,request_hash FROM thread_bindings
            WHERE binding_id=?
            """,
            (bridge.bridge_id,),
        ).fetchone()
        self.assertNotEqual(binding["endpoint_id"], old_endpoint)
        self.assertEqual(binding["generation"], bridge.binding_generation + 1)
        expected_hash = self.schema._request_hash(
            {
                "bridge_id": bridge.bridge_id,
                "source_kind": "headless_run",
                "source_json": json.dumps(
                    {"run_id": "after-rollback", "cwd": "/tmp/project"},
                    separators=(",", ":"),
                ),
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "thread_ts": bridge.thread_ts,
                "idempotency_key": "rollback-rebind",
                "binding_generation": bridge.binding_generation + 1,
            },
            binding["endpoint_id"],
            self.descriptor.security_domain_id,
        )
        self.assertEqual(binding["request_hash"], expected_hash)
        turn = restored.execute(
            """
            SELECT binding_generation,state FROM queued_turns
            WHERE event_key='event-rollback-rebind'
            """
        ).fetchone()
        self.assertEqual(turn["binding_generation"], bridge.binding_generation + 1)
        self.assertEqual(turn["state"], "ready")
        self.schema.require_valid(restored)
        restored.close()

    def test_schema17_retry_after_rollback_supersedes_stale_driver_proof(self):
        bridge = self.bridge("rollback-retry", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-rollback-retry", bridge.bridge_id, "run")
        )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        attempt_id = "att-rollback-retry-00000001"
        self.insert_terminal_signal_v18(
            connection,
            binding_id=bridge.bridge_id,
            event_key="event-rollback-retry",
            attempt_id=attempt_id,
            receipt_state="not_started",
        )
        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        legacy = self.runtime.Store(self.store.path)
        claimed = legacy.claim_event_batch(bridge.bridge_id)
        self.assertEqual([item["event_id"] for item in claimed], ["event-rollback-retry"])
        self.assertTrue(
            legacy.prepare_delivery_attempt(
                ["event-rollback-retry"],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            legacy.mark_attempt_submitting(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        self.assertTrue(
            legacy.mark_attempt_awaiting_ack(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        self.assertEqual(
            legacy.acknowledge_attempt(
                attempt_id,
                bridge.bridge_id,
                ack_kind="no_reply",
            ),
            1,
        )
        terminal_time = "2099-01-01 00:00:00"
        with legacy.connect() as legacy_connection:
            legacy_connection.execute(
                """
                UPDATE bridge_events SET updated_at=?
                WHERE event_id='event-rollback-retry'
                """,
                (terminal_time,),
            )

        self.migrate()
        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        attempt = restored.execute(
            """
            SELECT state,last_driver_sequence,last_driver_receipt_id
            FROM native_attempts WHERE attempt_id=?
            """,
            (attempt_id,),
        ).fetchone()
        self.assertEqual(attempt["state"], "no_reply")
        self.assertEqual(attempt["last_driver_sequence"], 0)
        self.assertIsNone(attempt["last_driver_receipt_id"])
        turn = restored.execute(
            """
            SELECT state,terminal_at,updated_at FROM queued_turns
            WHERE event_key='event-rollback-retry'
            """
        ).fetchone()
        self.assertEqual(tuple(turn), ("completed", terminal_time, terminal_time))
        self.assertEqual(
            restored.execute(
                "SELECT state FROM queued_turns WHERE event_key='event-rollback-retry'"
            ).fetchone()[0],
            "completed",
        )
        self.schema.require_valid(restored)
        restored.close()

    def test_schema17_multiple_retries_preserve_every_attempt_membership(self):
        bridge = self.bridge("rollback-new-attempt", channel="C12345678")
        event_key = "event-rollback-new-attempt"
        self.assertTrue(self.store.enqueue_event(event_key, bridge.bridge_id, "run"))
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        old_attempt = "att-rollback-old-not-started"
        self.insert_terminal_signal_v18(
            connection,
            binding_id=bridge.bridge_id,
            event_key=event_key,
            attempt_id=old_attempt,
            receipt_state="not_started",
        )
        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        legacy = self.runtime.Store(self.store.path)
        self.assertEqual(
            [item["event_id"] for item in legacy.claim_event_batch(bridge.bridge_id)],
            [event_key],
        )
        middle_attempt = "att-rollback-middle-not-started"
        self.assertTrue(
            legacy.prepare_delivery_attempt(
                [event_key], bridge.bridge_id, bridge.binding_generation,
                middle_attempt, delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            legacy.mark_attempt_submitting(
                middle_attempt, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.assertTrue(
            legacy.requeue_prepared_attempt(
                middle_attempt,
                bridge.bridge_id,
                "terminal_submit_not_started",
            )
        )
        self.assertEqual(
            [item["event_id"] for item in legacy.claim_event_batch(bridge.bridge_id)],
            [event_key],
        )
        new_attempt = "att-rollback-new-no-reply"
        self.assertTrue(
            legacy.prepare_delivery_attempt(
                [event_key], bridge.bridge_id, bridge.binding_generation,
                new_attempt, delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            legacy.mark_attempt_submitting(
                new_attempt, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.assertTrue(
            legacy.mark_attempt_awaiting_ack(
                new_attempt, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.assertEqual(
            legacy.acknowledge_attempt(
                new_attempt, bridge.bridge_id, ack_kind="no_reply"
            ),
            1,
        )

        self.migrate()
        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        memberships = restored.execute(
            """
            SELECT attempt_id,ordinal,event_key,turn_binding_generation
            FROM native_attempt_turns WHERE event_key=? ORDER BY attempt_id
            """,
            (event_key,),
        ).fetchall()
        self.assertEqual(
            [(row["attempt_id"], row["ordinal"], row["event_key"])
             for row in memberships],
            [
                (middle_attempt, 0, event_key),
                (new_attempt, 0, event_key),
                (old_attempt, 0, event_key),
            ],
        )
        old = restored.execute(
            """
            SELECT state,last_driver_sequence,last_driver_receipt_id
            FROM native_attempts WHERE attempt_id=?
            """,
            (old_attempt,),
        ).fetchone()
        self.assertEqual(old["state"], "failed_before_start")
        self.assertEqual(old["last_driver_sequence"], 1)
        self.assertIsNotNone(old["last_driver_receipt_id"])
        self.schema.require_valid(restored)
        restored.close()

    def test_schema17_batch_journal_uses_claim_order_not_trigger_order(self):
        bridge = self.bridge("rollback-batch-order", channel="C12345678")
        first = "event-inserted-first-but-ordered-last"
        second = "event-inserted-second-but-ordered-first"
        self.assertTrue(self.store.enqueue_event(first, bridge.bridge_id, "first"))
        self.assertTrue(self.store.enqueue_event(second, bridge.bridge_id, "second"))
        with self.store.connect() as legacy:
            legacy.execute(
                "UPDATE bridge_events SET created_at=? WHERE event_id=?",
                ("2026-08-18 05:00:02", first),
            )
            legacy.execute(
                "UPDATE bridge_events SET created_at=? WHERE event_id=?",
                ("2026-08-18 05:00:01", second),
            )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        legacy = self.runtime.Store(self.store.path)
        claimed = legacy.claim_event_batch(bridge.bridge_id)
        self.assertEqual([item["event_id"] for item in claimed], [second, first])
        attempt_id = "att-rollback-batch-order"
        self.assertTrue(
            legacy.prepare_delivery_attempt(
                [second, first],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            legacy.mark_attempt_submitting(
                attempt_id, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.assertTrue(
            legacy.mark_attempt_awaiting_ack(
                attempt_id, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.assertEqual(
            legacy.acknowledge_attempt(
                attempt_id, bridge.bridge_id, ack_kind="no_reply"
            ),
            2,
        )

        self.migrate()
        restored = sqlite3.connect(self.store.path)
        actual = restored.execute(
            """
            SELECT event_key FROM native_attempt_turns
            WHERE attempt_id=? ORDER BY ordinal
            """,
            (attempt_id,),
        ).fetchall()
        self.assertEqual([row[0] for row in actual], [second, first])
        self.schema.require_valid(restored)
        restored.close()

    def test_schema17_same_state_retry_cycle_marks_archive_superseded(self):
        bridge = self.bridge("rollback-same-state", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-rollback-same-state", bridge.bridge_id, "run")
        )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        attempt_id = "att-rollback-same-state-0001"
        self.insert_terminal_signal_v18(
            connection,
            binding_id=bridge.bridge_id,
            event_key="event-rollback-same-state",
            attempt_id=attempt_id,
            receipt_state="not_started",
        )
        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        legacy = self.runtime.Store(self.store.path)
        self.assertEqual(
            [item["event_id"] for item in legacy.claim_event_batch(bridge.bridge_id)],
            ["event-rollback-same-state"],
        )
        self.assertTrue(
            legacy.prepare_delivery_attempt(
                ["event-rollback-same-state"],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            legacy.mark_attempt_submitting(
                attempt_id,
                bridge.bridge_id,
                bridge.binding_generation,
            )
        )
        self.assertTrue(
            legacy.requeue_prepared_attempt(
                attempt_id,
                bridge.bridge_id,
                "terminal_submit_not_started",
            )
        )
        self.migrate()

        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        attempt = restored.execute(
            """
            SELECT state,submitted_at,last_driver_sequence
            FROM native_attempts WHERE attempt_id=?
            """,
            (attempt_id,),
        ).fetchone()
        self.assertEqual(attempt["state"], "failed_before_start")
        self.assertIsNotNone(attempt["submitted_at"])
        self.assertEqual(attempt["last_driver_sequence"], 0)
        self.assertEqual(
            restored.execute(
                "SELECT state FROM queued_turns WHERE event_key='event-rollback-same-state'"
            ).fetchone()[0],
            "ready",
        )
        self.schema.require_valid(restored)
        restored.close()

    def test_terminal_lifecycle_timestamps_survive_roundtrip(self):
        bridge = self.bridge("terminal-timestamps", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-terminal-timestamps", bridge.bridge_id, "run")
        )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        attempt_id = "att-terminal-timestamps-00001"
        self.insert_terminal_signal_v18(
            connection,
            binding_id=bridge.bridge_id,
            event_key="event-terminal-timestamps",
            attempt_id=attempt_id,
            receipt_state="failed",
        )
        connection.execute(
            "UPDATE native_attempts SET updated_at='2099-01-01 01:00:00' WHERE attempt_id=?",
            (attempt_id,),
        )
        connection.execute(
            """
            UPDATE queued_turns SET updated_at='2099-01-01 02:00:00'
            WHERE event_key='event-terminal-timestamps'
            """
        )
        connection.commit()
        before_attempt = connection.execute(
            """
            SELECT submitted_at,accepted_at,terminal_at,updated_at
            FROM native_attempts WHERE attempt_id=?
            """,
            (attempt_id,),
        ).fetchone()
        before_turn = connection.execute(
            """
            SELECT terminal_at,updated_at FROM queued_turns
            WHERE event_key='event-terminal-timestamps'
            """
        ).fetchone()

        self.schema.rollback_v18_to_v17(connection)
        connection.close()
        self.migrate()
        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        self.assertEqual(
            restored.execute(
                """
                SELECT submitted_at,accepted_at,terminal_at,updated_at
                FROM native_attempts WHERE attempt_id=?
                """,
                (attempt_id,),
            ).fetchone(),
            before_attempt,
        )
        self.assertEqual(
            restored.execute(
                """
                SELECT terminal_at,updated_at FROM queued_turns
                WHERE event_key='event-terminal-timestamps'
                """
            ).fetchone(),
            before_turn,
        )
        self.schema.require_valid(restored)
        restored.close()

    def test_not_started_proof_preserves_submit_attempt_timestamp(self):
        bridge = self.bridge("not-started-timestamp", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-not-started-timestamp", bridge.bridge_id, "run")
        )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        attempt_id = "att-not-started-timestamp-001"
        self.insert_terminal_signal_v18(
            connection,
            binding_id=bridge.bridge_id,
            event_key="event-not-started-timestamp",
            attempt_id=attempt_id,
            receipt_state="not_started",
            submit_attempted=True,
        )
        before = connection.execute(
            """
            SELECT state,submitted_at,accepted_at,terminal_at,updated_at
            FROM native_attempts WHERE attempt_id=?
            """,
            (attempt_id,),
        ).fetchone()
        self.assertIsNotNone(before["submitted_at"])
        self.assertIsNone(before["accepted_at"])
        self.schema.rollback_v18_to_v17(connection)
        connection.close()
        self.migrate()
        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        after = restored.execute(
            """
            SELECT state,submitted_at,accepted_at,terminal_at,updated_at
            FROM native_attempts WHERE attempt_id=?
            """,
            (attempt_id,),
        ).fetchone()
        self.assertEqual(after, before)
        self.schema.require_valid(restored)
        restored.close()

    def test_schema17_prune_cannot_delete_rollback_horizon_records(self):
        bridge = self.bridge("rollback-prune", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-rollback-prune", bridge.bridge_id, "run")
        )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        attempt_id = "att-rollback-prune-00000001"
        self.insert_terminal_signal_v18(
            connection,
            binding_id=bridge.bridge_id,
            event_key="event-rollback-prune",
            attempt_id=attempt_id,
            receipt_state="no_reply",
        )
        before_receipt = connection.execute(
            "SELECT * FROM driver_receipts WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()
        self.schema.rollback_v18_to_v17(connection)
        connection.execute(
            "UPDATE bridge_attempts SET updated_at='2000-01-01 00:00:00'"
        )
        connection.execute(
            "UPDATE bridge_events SET updated_at='2000-01-01 00:00:00'"
        )
        connection.commit()
        connection.close()

        legacy = self.runtime.Store(self.store.path)
        counts = legacy.prune(30)
        self.assertEqual(counts["bridge_attempts"], 0)
        self.assertEqual(counts["bridge_events"], 0)
        self.migrate()
        restored = sqlite3.connect(self.store.path)
        self.assertEqual(
            restored.execute(
                "SELECT state FROM native_attempts WHERE attempt_id=?",
                (attempt_id,),
            ).fetchone()[0],
            "no_reply",
        )
        after_receipt = restored.execute(
            "SELECT * FROM driver_receipts WHERE attempt_id=?",
            (attempt_id,),
        ).fetchone()
        self.assertEqual(after_receipt, tuple(before_receipt))
        self.assertEqual(
            restored.execute(
                """
                SELECT count(*) FROM sqlite_master
                WHERE name LIKE 'tether%rollback%'
                """
            ).fetchone()[0],
            0,
        )
        restored.execute("DELETE FROM bridge_replies")
        self.schema.require_valid(restored)
        restored.close()

    def test_schema17_prune_preserves_fallback_response_and_delivery_proof(self):
        bridge = self.bridge("rollback-prune-response", channel="C12345678")
        event_key = "event-rollback-prune-response"
        self.assertTrue(self.store.enqueue_event(event_key, bridge.bridge_id, "run"))
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        legacy = self.runtime.Store(self.store.path)
        self.assertEqual(
            [item["event_id"] for item in legacy.claim_event_batch(bridge.bridge_id)],
            [event_key],
        )
        attempt_id = "att-rollback-prune-response"
        self.assertTrue(
            legacy.prepare_delivery_attempt(
                [event_key],
                bridge.bridge_id,
                bridge.binding_generation,
                attempt_id,
                delivery_kind="detached_native",
            )
        )
        self.assertTrue(
            legacy.mark_attempt_submitting(
                attempt_id, bridge.bridge_id, bridge.binding_generation
            )
        )
        self.runtime.stage_reply_payload(
            legacy, bridge.bridge_id, attempt_id, "fallback response"
        )
        claim = legacy.claim_reply(
            attempt_id, bridge.bridge_id, lease_owner="a" * 32
        )
        self.assertEqual(claim["status"], "claimed")
        self.assertEqual(
            legacy.complete_reply(
                attempt_id,
                bridge.bridge_id,
                claim["lease_id"],
                "1786000999.000001",
            ),
            1,
        )
        with legacy.connect() as fallback:
            before_reply = fallback.execute(
                """
                SELECT reply_key,bridge_id,text_hash,payload_text,
                       client_msg_id,state,message_ts
                FROM bridge_replies WHERE reply_key=?
                """,
                (attempt_id,),
            ).fetchone()
            fallback.execute(
                """
                UPDATE bridge_attempts
                SET created_at='2000-01-01 00:00:00',
                    submitted_at='2000-01-01 00:01:00',
                    acknowledged_at='2000-01-01 00:02:00',
                    updated_at='2000-01-01 00:03:00'
                """
            )
            fallback.execute(
                "UPDATE bridge_events SET updated_at='2000-01-01 00:00:00'"
            )
            fallback.execute(
                """
                UPDATE bridge_replies
                SET created_at='2000-01-01 00:00:00',
                    updated_at='2000-01-01 00:03:00'
                """
            )

        counts = legacy.prune(30)
        self.assertEqual(counts["bridge_attempts"], 0)
        self.assertEqual(counts["bridge_events"], 0)
        self.assertEqual(counts["bridge_replies"], 0)
        with legacy.connect() as fallback:
            after_reply = fallback.execute(
                """
                SELECT reply_key,bridge_id,text_hash,payload_text,
                       client_msg_id,state,message_ts
                FROM bridge_replies WHERE reply_key=?
                """,
                (attempt_id,),
            ).fetchone()
        self.assertEqual(after_reply, before_reply)

        self.migrate()
        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        attempt = restored.execute(
            """
            SELECT state,response_inline,hermes_egress_receipt_id
            FROM native_attempts WHERE attempt_id=?
            """,
            (attempt_id,),
        ).fetchone()
        self.assertEqual(
            tuple(attempt), ("completed_with_response", "fallback response", None)
        )
        self.assertEqual(
            restored.execute(
                "SELECT count(*) FROM sqlite_master WHERE name LIKE 'tether%rollback%'"
            ).fetchone()[0],
            0,
        )
        self.schema.require_valid(restored)
        restored.close()

    def test_schema17_prune_preserves_unattempted_fallback_terminal_turn(self):
        bridge = self.bridge("rollback-prune-turn", channel="C12345678")
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        self.schema.rollback_v18_to_v17(connection)
        connection.close()

        legacy = self.runtime.Store(self.store.path)
        event_key = "event-fallback-terminal-without-attempt"
        self.assertTrue(legacy.enqueue_event(event_key, bridge.bridge_id, "payload"))
        self.assertEqual(
            [item["event_id"] for item in legacy.claim_event_batch(bridge.bridge_id)],
            [event_key],
        )
        legacy.finish_event(event_key, "fallback terminal failure")
        with legacy.connect() as fallback:
            before = fallback.execute(
                """
                SELECT event_id,bridge_id,payload_json,state,error,attempt_id,
                       binding_generation
                FROM bridge_events WHERE event_id=?
                """,
                (event_key,),
            ).fetchone()
            fallback.execute(
                """
                UPDATE bridge_events SET updated_at='2000-01-01 00:00:00'
                WHERE event_id=?
                """,
                (event_key,),
            )
        counts = legacy.prune(30)
        self.assertEqual(counts["bridge_events"], 0)
        with legacy.connect() as fallback:
            after = fallback.execute(
                """
                SELECT event_id,bridge_id,payload_json,state,error,attempt_id,
                       binding_generation
                FROM bridge_events WHERE event_id=?
                """,
                (event_key,),
            ).fetchone()
        self.assertEqual(after, before)

        self.migrate()
        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        turn = restored.execute(
            """
            SELECT state,payload_inline,error_code FROM queued_turns
            WHERE event_key=?
            """,
            (event_key,),
        ).fetchone()
        self.assertEqual(
            tuple(turn), ("cancelled", "payload", "fallback terminal failure")
        )
        self.assertEqual(
            restored.execute(
                "SELECT count(*) FROM sqlite_master WHERE name LIKE 'tether%rollback%'"
            ).fetchone()[0],
            0,
        )
        self.schema.require_valid(restored)
        restored.close()

    def test_authority_resolution_survives_rollback_and_reupgrade(self):
        bridge = self.bridge("operator-roundtrip", channel="C12345678")
        self.assertTrue(
            self.store.enqueue_event("event-operator-roundtrip", bridge.bridge_id, "run")
        )
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        binding = connection.execute(
            """
            SELECT binding.endpoint_id,binding.generation,endpoint.incarnation
            FROM thread_bindings AS binding
            JOIN endpoints AS endpoint ON endpoint.endpoint_id=binding.endpoint_id
            WHERE binding.binding_id=?
            """,
            (bridge.bridge_id,),
        ).fetchone()
        attempt_id = "att-operator-roundtrip-000001"
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("PRAGMA defer_foreign_keys=ON")
        connection.execute(
            "UPDATE endpoints SET next_lease_fence=1 WHERE endpoint_id=?",
            (binding["endpoint_id"],),
        )
        connection.execute(
            """
            INSERT INTO endpoint_leases(
              attempt_id,endpoint_id,endpoint_incarnation,fence,expires_at
            ) VALUES(?,?,?,1,datetime('now','+30 minutes'))
            """,
            (attempt_id, binding["endpoint_id"], binding["incarnation"]),
        )
        connection.execute(
            """
            INSERT INTO native_attempts(
              attempt_id,endpoint_id,binding_id,binding_generation,driver_kind,
              driver_request_id,driver_request_hash,reply_token_hash,state
            ) VALUES(?,?,?,?, 'detached_native',?,?,?,'prepared')
            """,
            (
                attempt_id,
                binding["endpoint_id"],
                bridge.bridge_id,
                binding["generation"],
                f"submit:{attempt_id}",
                hashlib.sha256(f"submit:{attempt_id}".encode()).hexdigest(),
                hashlib.sha256(f"token:{attempt_id}".encode()).hexdigest(),
            ),
        )
        connection.execute(
            """
            INSERT INTO native_attempt_turns(
              attempt_id,ordinal,event_key,binding_id,turn_binding_generation
            ) VALUES(?,0,'event-operator-roundtrip',?,1)
            """,
            (attempt_id, bridge.bridge_id),
        )
        connection.execute(
            """
            UPDATE native_attempts SET state='submitting',submitted_at=CURRENT_TIMESTAMP
            WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        connection.execute(
            "UPDATE native_attempts SET state='uncertain' WHERE attempt_id=?",
            (attempt_id,),
        )
        connection.execute(
            """
            INSERT INTO operator_resolutions(
              attempt_id,endpoint_id,lease_fence,action,source_kind,
              authority_receipt_id,operator_principal_hash,evidence_ref,
              evidence_sha256,resolved_at,created_at
            ) VALUES(?,?,1,'abandon','authority','authority-roundtrip',?,?,?,
                     '2026-08-18 04:00:00','2026-08-18 04:00:00')
            """,
            (
                attempt_id,
                binding["endpoint_id"],
                hashlib.sha256(b"operator-roundtrip").hexdigest(),
                "authority://roundtrip",
                hashlib.sha256(b"operator-evidence").hexdigest(),
            ),
        )
        connection.execute(
            """
            UPDATE native_attempts SET state='operator_abandoned',
              terminal_at=CURRENT_TIMESTAMP WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        connection.execute(
            """
            UPDATE queued_turns SET state='cancelled',terminal_at=CURRENT_TIMESTAMP
            WHERE event_key='event-operator-roundtrip'
            """
        )
        connection.execute(
            """
            UPDATE endpoint_leases SET released_at=CURRENT_TIMESTAMP,
              release_reason='operator_abandoned' WHERE attempt_id=?
            """,
            (attempt_id,),
        )
        connection.commit()
        before = self.schema.logical_manifest_v18(connection)
        self.schema.rollback_v18_to_v17(connection)
        connection.close()
        self.migrate()
        restored = sqlite3.connect(self.store.path)
        restored.row_factory = sqlite3.Row
        after = self.schema.logical_manifest_v18(restored)
        self.assertEqual(
            {key: before[key] for key in self.schema.PRESERVED_MANIFEST_KEYS},
            {key: after[key] for key in self.schema.PRESERVED_MANIFEST_KEYS},
        )
        resolution = restored.execute(
            """
            SELECT source_kind,authority_receipt_id FROM operator_resolutions
            WHERE attempt_id=?
            """,
            (attempt_id,),
        ).fetchone()
        self.assertEqual(tuple(resolution), ("authority", "authority-roundtrip"))
        self.schema.require_valid(restored)
        restored.close()


if __name__ == "__main__":
    unittest.main()


class BindingStatusPreservationTest(DomainMigrationTest):
    """Preservation must see binding status, not just identity and routes."""

    def test_corrupting_binding_status_changes_the_preserved_digest(self):
        self.bridge("status-key")
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        try:
            before = self.schema.preserved_manifest_digest(
                self.schema.logical_manifest_v17(connection)
            )
            # A lossy round trip that returns every binding closed/unverified
            # must NOT pass the preservation check.
            connection.execute(
                "UPDATE bridges SET status='closed',binding_state='pending'"
            )
            connection.commit()
            after = self.schema.preserved_manifest_digest(
                self.schema.logical_manifest_v17(connection)
            )
        finally:
            connection.close()
        self.assertNotEqual(before, after)

    def test_lossless_round_trip_preserves_the_digest(self):
        self.bridge("round-trip-key")
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        try:
            before = self.schema.preserved_manifest_digest(
                self.schema.logical_manifest_v17(connection)
            )
        finally:
            connection.close()
        self.migrate()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        try:
            self.schema.rollback_v18_to_v17(connection)
        finally:
            connection.close()
        connection = sqlite3.connect(self.store.path)
        connection.row_factory = sqlite3.Row
        try:
            after = self.schema.preserved_manifest_digest(
                self.schema.logical_manifest_v17(connection)
            )
        finally:
            connection.close()
        self.assertEqual(before, after)
