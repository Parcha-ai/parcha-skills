"""Cross-connection task fences exercised against temporary SQLite databases."""
from __future__ import annotations

import importlib
from pathlib import Path
import sys
import tempfile
import threading
from types import ModuleType
import unittest


ROOT = Path(__file__).resolve().parents[1] / "runtime/plugin_next"
PACKAGE = "offline_task_races_fixture"
package = ModuleType(PACKAGE)
package.__path__ = [str(ROOT)]
sys.modules[PACKAGE] = package
store_module = importlib.import_module(PACKAGE + ".store")
model = importlib.import_module(PACKAGE + ".task_context")


class TaskRaceTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="tether-task-races-")
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / "tether.sqlite"
        self.first = store_module.Store(path)
        self.second = store_module.Store(path)
        self.addCleanup(self.first.close)
        self.addCleanup(self.second.close)
        endpoint = self.first.register_endpoint(endpoint_key="native-exact", endpoint_kind="native",
            source_kind="claude_session", source_json='{"session_id":"exact-session"}')
        self.endpoint_id = endpoint["endpoint_id"]
        self.binding = self.first.bind_thread(endpoint_id=self.endpoint_id, team_id="T", channel_id="C",
            thread_ts="1.1", owner_user_id="U", idempotency_key="test-binding")
        self.binding_id = self.binding["binding_id"]

    def admit(self, store=None, key="event"):
        return (store or self.first).admit_turn(binding_id=self.binding_id,
            event_key=key, ordered_at="2.1", payload_inline='{"text":"Continue this task"}')

    def contend(self, pause_select, first_operation, second_operation):
        """Pause a reader inside its real SQLite transaction and contend a writer.

        The second trace confirms BEGIN IMMEDIATE reached SQLite, rather than
        inferring concurrency merely from starting a Python thread.
        """
        first_paused, release_first = threading.Event(), threading.Event()
        second_begin, second_finished = threading.Event(), threading.Event()
        results = {}

        def trace_first(sql):
            if pause_select in sql:
                first_paused.set()
                if not release_first.wait(5):
                    results["barrier_timeout"] = True

        def trace_second(sql):
            if sql == "BEGIN IMMEDIATE":
                second_begin.set()

        self.first._db.set_trace_callback(trace_first)
        self.second._db.set_trace_callback(trace_second)

        def run(name, operation, finished=None):
            try:
                results[name] = operation()
            except Exception as error:
                results[name] = error
            finally:
                if finished is not None:
                    finished.set()

        first_thread = threading.Thread(target=run, args=("first", first_operation))
        second_thread = threading.Thread(target=run, args=("second", second_operation, second_finished))
        first_thread.start()
        try:
            self.assertTrue(first_paused.wait(3), "First connection did not reach the guarded read")
            second_thread.start()
            self.assertTrue(second_begin.wait(3), "Second connection did not attempt its SQLite write transaction")
            self.assertFalse(second_finished.wait(0.05), "Competing writer passed the transaction fence")
        finally:
            release_first.set()
            first_thread.join(5)
            if second_thread.ident is not None:
                second_thread.join(5)
            self.first._db.set_trace_callback(None)
            self.second._db.set_trace_callback(None)
        self.assertFalse(first_thread.is_alive())
        self.assertFalse(second_thread.is_alive())
        self.assertNotIn("barrier_timeout", results)
        return results

    def test_admission_serializes_with_unlink_on_other_connection(self):
        self.first.set_task_ref(self.binding_id, model.TaskRef("project", "t_current"), expected_generation=1)
        results = self.contend("SELECT * FROM bindings WHERE binding_id=",
            lambda: self.admit(),
            lambda: self.second.set_task_ref(self.binding_id, None, expected_generation=2))
        self.assertEqual(results["first"]["binding_generation"], 2)
        self.assertIsInstance(results["second"], store_module.StoreError)
        self.assertEqual(results["second"].code, "task_binding_busy")
        current = self.first.task_binding(self.binding_id)
        self.assertEqual(current["generation"], 2)
        self.assertEqual(current["task_ref"]["task_id"], "t_current")

    def test_two_schedulers_create_exactly_one_attempt(self):
        self.admit()
        results = self.contend("SELECT attempt_id FROM attempts WHERE endpoint_id=",
            lambda: self.first.schedule_next(self.endpoint_id),
            lambda: self.second.schedule_next(self.endpoint_id))
        self.assertEqual(results["first"]["state"], "accepted")
        self.assertIsNone(results["second"])
        self.assertEqual(self.first._db.execute("SELECT count(*) FROM attempts").fetchone()[0], 1)
        self.assertEqual(self.first._db.execute("SELECT count(*) FROM turns WHERE state='running'").fetchone()[0], 1)

    def test_stale_unlinked_turn_is_failed_without_attempt(self):
        self.admit()
        # A persisted stale turn models an old writer or a prior buggy gateway.
        self.first._db.execute("UPDATE bindings SET generation=generation+1")
        self.first._db.commit()
        self.assertIsNone(self.first.schedule_next(self.endpoint_id))
        row = self.first._db.execute("SELECT state,error_code FROM turns").fetchone()
        self.assertEqual((row["state"], row["error_code"]), ("failed", "binding_generation_conflict"))
        self.assertEqual(self.first._db.execute("SELECT count(*) FROM attempts").fetchone()[0], 0)

    def test_stale_turn_does_not_block_current_generation(self):
        self.admit(key="old")
        self.first._db.execute("UPDATE bindings SET generation=generation+1")
        self.first._db.commit()
        self.admit(key="current")
        attempt = self.first.schedule_next(self.endpoint_id)
        context = self.first.attempt_context(attempt["attempt_id"])
        self.assertEqual([turn["event_key"] for turn in context["turns"]], ["current"])
        stale = self.first._db.execute("SELECT state FROM turns WHERE event_key='old'").fetchone()
        self.assertEqual(stale["state"], "failed")

    def test_refusals_release_write_transaction(self):
        with self.assertRaises(store_module.StoreError):
            self.first.admit_turn(binding_id="missing", event_key="new", ordered_at="1")
        self.assertFalse(self.first._db.in_transaction)
        self.admit()
        self.first.schedule_next(self.endpoint_id)
        self.assertIsNone(self.first.schedule_next(self.endpoint_id))
        self.assertFalse(self.first._db.in_transaction)


if __name__ == "__main__":
    unittest.main()
