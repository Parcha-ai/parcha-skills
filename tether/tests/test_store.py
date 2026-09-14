"""Store: the four-table model that replaces the schema-18 core for the session driver."""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runtime.plugin_next.store import Store, StoreError, is_no_reply  # noqa: E402


def _payload(user: str, text: str, ts: str) -> str:
    return json.dumps({"user": user, "text": text, "ts": ts}, sort_keys=True)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / "tether.db")

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def endpoint(self, session_id: str = "sess-1") -> dict:
        return self.store.register_endpoint(
            endpoint_key=f"detached_native:claude_session:{session_id}", endpoint_kind="detached_native",
            source_kind="claude_session", source_json=json.dumps({"session_id": session_id, "cwd": "/w"}),
        )

    def bind(self, thread: str = "100.1", session_id: str = "sess-1") -> dict:
        endpoint = self.endpoint(session_id)
        return self.store.bind_thread(
            endpoint_id=endpoint["endpoint_id"], team_id="T1", channel_id="C1", owner_user_id="U1",
            idempotency_key=f"bind:T1:C1:{thread}", thread_ts=thread,
        )

    def test_register_is_idempotent_and_repoints_the_source(self):
        first = self.endpoint()
        again = self.store.register_endpoint(
            endpoint_key=first["endpoint_key"], endpoint_kind="detached_native", source_kind="claude_session",
            source_json=json.dumps({"session_id": "sess-1", "cwd": "/elsewhere"}),
        )
        self.assertEqual(first["endpoint_id"], again["endpoint_id"])
        self.assertEqual(again["source"]["cwd"], "/elsewhere")
        with self.assertRaises(StoreError) as caught:
            self.store.register_endpoint(
                endpoint_key=first["endpoint_key"], endpoint_kind="detached_native", source_kind="codex_session",
                source_json="{}",
            )
        self.assertEqual(caught.exception.code, "endpoint_identity_conflict")

    def test_bind_is_idempotent_and_one_live_binding_per_thread(self):
        binding = self.bind()
        self.assertTrue(binding["active"])
        self.assertEqual(self.bind()["binding_id"], binding["binding_id"], "same key, same binding")
        other = self.endpoint("sess-2")
        with self.assertRaises(StoreError) as caught:
            self.store.bind_thread(
                endpoint_id=other["endpoint_id"], team_id="T1", channel_id="C1", owner_user_id="U1",
                idempotency_key="other-key", thread_ts="100.1",
            )
        self.assertEqual(caught.exception.code, "thread_claim_conflict")
        found = self.store.find_active_binding(team_id="T1", channel_id="C1", thread_ts="100.1")
        self.assertEqual(found["binding_id"], binding["binding_id"])
        self.assertIsNone(self.store.find_active_binding(team_id="T1", channel_id="C1", thread_ts="999.9"))

    def test_pending_root_then_activate(self):
        endpoint = self.endpoint()
        pending = self.store.bind_thread(
            endpoint_id=endpoint["endpoint_id"], team_id="T1", channel_id="C1", owner_user_id="U1",
            idempotency_key="notify-1",
        )
        self.assertTrue(pending["pending_root"])
        self.assertIsNone(self.store.binding_thread(pending["binding_id"]))
        active = self.store.activate_binding(pending["binding_id"], "200.5")
        self.assertTrue(active["active"])
        self.assertEqual(self.store.binding_thread(active["binding_id"])["thread_ts"], "200.5")
        with self.assertRaises(StoreError) as caught:
            self.store.activate_binding(active["binding_id"], "201.0")
        self.assertEqual(caught.exception.code, "thread_claim_conflict")

    def test_turns_schedule_finish_and_close(self):
        binding = self.bind()
        first = self.store.admit_turn(
            binding_id=binding["binding_id"], event_key="slack:T1:C1:100.2", ordered_at="100.2",
            payload_inline=_payload("U1", "ship it", "100.2"),
        )
        self.assertEqual(first["state"], "ready")
        dup = self.store.admit_turn(
            binding_id=binding["binding_id"], event_key="slack:T1:C1:100.2", ordered_at="100.2",
            payload_inline=_payload("U1", "ship it", "100.2"),
        )
        self.assertEqual(dup["event_key"], first["event_key"], "duplicate delivery is a no-op")
        self.store.admit_turn(
            binding_id=binding["binding_id"], event_key="slack:T1:C1:100.3", ordered_at="100.3",
            payload_inline=_payload("U2", "and tests", "100.3"),
        )
        with self.assertRaises(StoreError) as caught:
            self.store.close_binding(binding["binding_id"])
        self.assertEqual(caught.exception.code, "binding_has_ready_turns")

        self.assertEqual(self.store.endpoints_with_ready_turns(), [binding["endpoint_id"]])
        attempt = self.store.schedule_next(binding["endpoint_id"])
        self.assertEqual(attempt["state"], "accepted")
        self.assertIsNone(self.store.schedule_next(binding["endpoint_id"]), "one attempt at a time per endpoint")
        self.assertEqual(self.store.endpoints_with_ready_turns(), [])
        context = self.store.attempt_context(attempt["attempt_id"])
        self.assertEqual([json.loads(t["payload_inline"])["text"] for t in context["turns"]], ["ship it", "and tests"])
        self.assertEqual((context["channel_id"], context["thread_ts"], context["source"]["session_id"]),
                         ("C1", "100.1", "sess-1"))

        done = self.store.finish_attempt(attempt["attempt_id"], state="completed_with_response", response_ref="/r/1")
        self.assertEqual(done["state"], "completed_with_response")
        self.assertEqual(self.store.attempt_context(attempt["attempt_id"])["response_ref"], "/r/1")
        self.assertEqual(self.store.counts()["ready_turns"], 0)
        closed = self.store.close_binding(binding["binding_id"])
        self.assertEqual((closed["state"], closed["generation"]), ("closed", 2))
        with self.assertRaises(StoreError) as caught:
            self.store.admit_turn(binding_id=binding["binding_id"], event_key="slack:T1:C1:100.9", ordered_at="100.9")
        self.assertEqual(caught.exception.code, "binding_not_admitting")
        # a closed thread can be bound again under the same idempotency key
        rebound = self.bind()
        self.assertNotEqual(rebound["binding_id"], binding["binding_id"])

    def test_failed_attempt_cancels_turns_and_orphans_are_failed_on_restart(self):
        binding = self.bind()
        self.store.admit_turn(binding_id=binding["binding_id"], event_key="e1", ordered_at="1",
                              payload_inline=_payload("U1", "go", "1"))
        attempt = self.store.schedule_next(binding["endpoint_id"])
        self.store.finish_attempt(attempt["attempt_id"], state="failed", error_code="exit_7")
        ctx = self.store.attempt_context(attempt["attempt_id"])
        self.assertEqual((ctx["state"], ctx["error_code"], ctx["turns"][0]["state"]), ("failed", "exit_7", "cancelled"))
        self.store.admit_turn(binding_id=binding["binding_id"], event_key="e2", ordered_at="2",
                              payload_inline=_payload("U1", "again", "2"))
        self.store.schedule_next(binding["endpoint_id"])
        self.assertEqual(len(self.store.uncertain_attempts()), 1)
        self.assertEqual(self.store.fail_orphans(), 1)
        self.assertEqual(self.store.uncertain_attempts(), [])

    def test_recent_turn_actors_newest_first(self):
        binding = self.bind()
        for i, who in enumerate(["UPEER1", "UPEER2", "U1"], start=2):
            self.store.admit_turn(binding_id=binding["binding_id"], event_key=f"k{i}", ordered_at=f"100.{i}",
                                  payload_inline=_payload(who, "x", f"100.{i}"))
        self.assertEqual(self.store.recent_turn_actors(binding["binding_id"], 2), ["U1", "UPEER2"])

    def test_store_is_usable_from_another_thread(self):
        binding = self.bind()
        errors: list[str] = []

        def worker():
            try:
                self.store.admit_turn(binding_id=binding["binding_id"], event_key="thread-key", ordered_at="1")
            except Exception as exc:  # pragma: no cover
                errors.append(repr(exc))

        t = threading.Thread(target=worker)
        t.start()
        t.join(5)
        self.assertEqual(errors, [])
        self.assertEqual(self.store.counts()["ready_turns"], 1)

    def test_import_legacy_bindings_is_idempotent(self):
        legacy = Path(self.temp.name) / "domain.db"
        db = sqlite3.connect(legacy)
        db.executescript(
            "CREATE TABLE endpoints(endpoint_id TEXT, endpoint_key TEXT, endpoint_kind TEXT, source_kind TEXT, source_json TEXT);"
            "CREATE TABLE thread_bindings(binding_id TEXT, endpoint_id TEXT, team_id TEXT, channel_id TEXT, thread_ts TEXT, "
            "owner_user_id TEXT, state TEXT, created_at TEXT);"
            "INSERT INTO endpoints VALUES('e1','detached_native:claude_session:old-1','detached_native','claude_session','{\"session_id\":\"old-1\",\"cwd\":\"/w\"}');"
            "INSERT INTO thread_bindings VALUES('b1','e1','T1','C9','500.1','U1','active','2026-09-01');"
            "INSERT INTO thread_bindings VALUES('b2','e1','T1','C9','500.2','U1','closed','2026-09-01');"
        )
        db.commit()
        db.close()
        self.assertEqual(self.store.import_legacy_bindings(legacy), 1)
        self.assertEqual(self.store.import_legacy_bindings(legacy), 0, "second import adds nothing")
        found = self.store.find_active_binding(team_id="T1", channel_id="C9", thread_ts="500.1")
        self.assertIsNotNone(found)
        ctx_source = self.store.register_endpoint(
            endpoint_key="detached_native:claude_session:old-1", endpoint_kind="detached_native",
            source_kind="claude_session", source_json='{"session_id":"old-1","cwd":"/w"}')["source"]
        self.assertEqual(ctx_source["session_id"], "old-1")
        self.assertIsNone(self.store.find_active_binding(team_id="T1", channel_id="C9", thread_ts="500.2"))
        self.assertEqual(self.store.import_legacy_bindings(Path(self.temp.name) / "missing.db"), 0)

    def test_binding_index_reads_the_store(self):
        from runtime.plugin_next import BindingIndex
        self.bind("700.1")
        index = BindingIndex(self.store.path, ttl_seconds=0.0)
        self.assertEqual(index.bound_threads(), frozenset({("C1", "700.1")}))
        self.store.close_binding(self.store.find_active_binding(team_id="T1", channel_id="C1", thread_ts="700.1")["binding_id"])
        self.assertEqual(index.bound_threads(), frozenset())

    def test_no_reply_marker(self):
        from runtime.plugin_next.store import strip_no_reply
        self.assertTrue(is_no_reply("NO_REPLY"))
        self.assertTrue(is_no_reply("NO_REPLY\n\nNO_REPLY"))
        # marker plus content is content: the work is posted, the marker is not
        self.assertFalse(is_no_reply("done here\n\nNO_REPLY"))
        self.assertFalse(is_no_reply("NO_REPLY\n\nDelivering my contract-aligned metrics fragment."))
        self.assertEqual(strip_no_reply("NO_REPLY\n\nDelivering my fragment.\nNO_REPLY"), "Delivering my fragment.")
        self.assertFalse(is_no_reply("NO_REPLY unless you need me"))
        self.assertFalse(is_no_reply(""))


if __name__ == "__main__":
    unittest.main()


class DeferredTurnTests(StoreTests):
    def test_deferred_attempt_requeues_turns_with_a_wait(self):
        binding = self.bind()
        self.store.admit_turn(binding_id=binding["binding_id"], event_key="d1", ordered_at="1",
                              payload_inline=_payload("U1", "first", "1"))
        self.store.admit_turn(binding_id=binding["binding_id"], event_key="d2", ordered_at="2",
                              payload_inline=_payload("U1", "second", "2"))
        attempt = self.store.schedule_next(binding["endpoint_id"])
        self.store.finish_attempt(attempt["attempt_id"], state="deferred", error_code="codex_thread_busy",
                                  retry_after_seconds=3600)
        self.assertEqual(self.store.counts()["ready_turns"], 2, "both turns are ready again, nothing cancelled")
        self.assertEqual(self.store.endpoints_with_ready_turns(), [], "but not before the retry time")
        self.assertIsNone(self.store.schedule_next(binding["endpoint_id"]))
        self.assertEqual(self.store.deferral_count(binding["binding_id"], "codex_thread_busy"), 1)
        # a second deferral with no wait: schedulable at once, counted twice
        self.store.admit_turn(binding_id=binding["binding_id"], event_key="d3", ordered_at="3",
                              payload_inline=_payload("U1", "third", "3"))
        with self.store._lock:
            self.store._db.execute("UPDATE turns SET not_before=NULL")
            self.store._db.commit()
        again = self.store.schedule_next(binding["endpoint_id"])
        self.assertEqual(len(self.store.attempt_context(again["attempt_id"])["turns"]), 3, "all three coalesce")
        self.store.finish_attempt(again["attempt_id"], state="deferred", error_code="codex_thread_busy")
        self.assertEqual(self.store.deferral_count(binding["binding_id"], "codex_thread_busy"), 2)
        self.assertEqual(self.store.endpoints_with_ready_turns(), [binding["endpoint_id"]])

    def test_not_before_column_is_added_to_an_old_database(self):
        import sqlite3 as _sq
        old = Path(self.temp.name) / "old.db"
        db = _sq.connect(old)
        db.executescript(
            "CREATE TABLE turns(event_key TEXT PRIMARY KEY, binding_id TEXT NOT NULL, binding_generation INTEGER NOT NULL, "
            "ordered_at TEXT NOT NULL, payload_inline TEXT, state TEXT NOT NULL, attempt_id TEXT, error_code TEXT, "
            "created_at TEXT NOT NULL, updated_at TEXT NOT NULL);")
        db.commit()
        db.close()
        store = Store(old)
        cols = {r[1] for r in store._db.execute("PRAGMA table_info(turns)")}
        self.assertIn("not_before", cols)
        store.close()
