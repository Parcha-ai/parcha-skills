"""Read-only task snapshots: portable fixtures and an optional real Hermes contract.

Every database is newly created under TemporaryDirectory. No board discovery,
worker, network, native computer, or provider operation belongs to this suite.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1] / "runtime/plugin_next"
PACKAGE = "offline_hermes_tasks_fixture"
package = ModuleType(PACKAGE)
package.__path__ = [str(ROOT)]
sys.modules[PACKAGE] = package
reader_module = importlib.import_module(PACKAGE + ".hermes_tasks")
model = importlib.import_module(PACKAGE + ".task_context")

TASK = "t_0123456789ab"
PARENT = "t_1123456789ab"
CHILD = "t_2123456789ab"
OTHER = "t_3123456789ab"
SCHEMA = """
CREATE TABLE tasks(id TEXT PRIMARY KEY,title TEXT,body TEXT,assignee TEXT,status TEXT,
 tenant TEXT,result TEXT,completion_contract TEXT,current_run_id INTEGER);
CREATE TABLE task_comments(id INTEGER PRIMARY KEY,task_id TEXT,author TEXT,body TEXT,created_at INTEGER);
CREATE TABLE task_events(id INTEGER PRIMARY KEY,task_id TEXT,kind TEXT,payload TEXT,created_at INTEGER,run_id INTEGER);
CREATE TABLE task_runs(id INTEGER PRIMARY KEY,task_id TEXT,profile TEXT,status TEXT,
 started_at INTEGER,ended_at INTEGER,outcome TEXT,summary TEXT,metadata TEXT,error TEXT);
CREATE TABLE task_attachments(id INTEGER PRIMARY KEY,task_id TEXT,filename TEXT,stored_path TEXT,
 content_type TEXT,size INTEGER,uploaded_by TEXT,created_at INTEGER);
CREATE TABLE task_links(parent_id TEXT,child_id TEXT);
"""


def rows(connection, table, task_id):
    values = []
    for row in connection.execute(f"SELECT * FROM {table} WHERE task_id=? ORDER BY id", (task_id,)):
        raw = dict(row)
        for field in ("payload", "metadata"):
            if raw.get(field):
                raw[field] = json.loads(raw[field])
        values.append(SimpleNamespace(**raw))
    return values


def fake_api():
    def get_task(connection, task_id):
        row = connection.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return SimpleNamespace(**dict(row)) if row else None
    return SimpleNamespace(
        get_task=get_task,
        list_comments=lambda c, t: rows(c, "task_comments", t),
        list_events=lambda c, t: rows(c, "task_events", t),
        list_runs=lambda c, t: rows(c, "task_runs", t),
        list_attachments=lambda c, t: rows(c, "task_attachments", t),
        parent_ids=lambda c, t: [r[0] for r in c.execute("SELECT parent_id FROM task_links WHERE child_id=?", (t,))],
        child_ids=lambda c, t: [r[0] for r in c.execute("SELECT child_id FROM task_links WHERE parent_id=?", (t,))],
    )


class HermesTaskReaderTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tether-hermes-tasks-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "board.sqlite"
        self.api = fake_api()
        self.ref = model.TaskRef("project", TASK)
        with sqlite3.connect(self.path) as connection:
            connection.executescript(SCHEMA)
            self.seed(connection)
        self.reader = reader_module.HermesTaskReader({"project": self.path}, tenant="tenant-a")
        self.host = patch.object(reader_module, "_canonical", return_value=self.api)
        self.host.start()
        self.addCleanup(self.host.stop)

    def seed(self, connection):
        for task_id, title, status in ((TASK, "Implement median", "running"),
                                       (PARENT, "Specify edges", "done"),
                                       (CHILD, "Review result", "todo"), (OTHER, "Unrelated", "ready")):
            connection.execute("INSERT INTO tasks(id,title,body,assignee,status,tenant,result) VALUES(?,?,?,?,?,?,?)",
                               (task_id, title, "Required edge cases", "builder", status, "tenant-a", "handoff"))
        connection.executemany("INSERT INTO task_links VALUES(?,?)", [(PARENT, TASK), (TASK, CHILD)])
        connection.execute("INSERT INTO task_comments VALUES(1,?,'reviewer','Handle even lengths',1)", (TASK,))
        connection.execute("INSERT INTO task_comments VALUES(2,?,'other','UNRELATED_PRIVATE',2)", (OTHER,))
        connection.execute("INSERT INTO task_events VALUES(1,?,'review_requested',?,1,1)",
                           (TASK, json.dumps({"summary": "Please review", "credential": "PRIVATE_SENTINEL"})))
        connection.execute("INSERT INTO task_runs(id,task_id,status,started_at,summary,metadata) VALUES(1,?,'running',1,'Prior evidence',?)",
                           (TASK, json.dumps({"artifact_ref": "https://example.invalid/result", "workspace_path": "PRIVATE_SENTINEL"})))
        connection.execute("INSERT INTO task_attachments VALUES(1,?,'result.txt','PRIVATE_SENTINEL','text/plain',12,'builder',1)", (TASK,))

    def error(self, code, function):
        with self.assertRaises(model.TaskContextError) as captured:
            function()
        self.assertEqual(captured.exception.code, code)
        self.assertNotIn(str(self.path), str(captured.exception))

    def test_read_projects_only_linked_task_without_mutation(self):
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        files = sorted(p.name for p in self.path.parent.iterdir())
        raw = self.reader.read(self.ref)
        self.assertEqual(raw["reference"], {"board": "project", "task_id": TASK})
        self.assertEqual(raw["task"]["id"], TASK)
        self.assertEqual(raw["comments"][0]["body"], "Handle even lengths")
        self.assertEqual(raw["runs"][0]["metadata"], {"artifact_ref": "https://example.invalid/result"})
        self.assertEqual([(r["task_id"], r["relationship"]) for r in raw["dependencies"]], [(PARENT, "parent"), (CHILD, "child")])
        self.assertNotIn("PRIVATE_SENTINEL", json.dumps(raw))
        self.assertNotIn("UNRELATED_PRIVATE", json.dumps(raw))
        self.assertNotIn("stored_path", raw["attachments"][0])
        context = model.build_task_context(self.ref, raw)
        self.assertEqual(context.ref, self.ref)
        self.assertIn("Handle even lengths", context.render())
        self.assertNotIn("PRIVATE_SENTINEL", context.render())
        self.assertEqual(before, hashlib.sha256(self.path.read_bytes()).hexdigest())
        self.assertEqual(files, sorted(p.name for p in self.path.parent.iterdir()))

    def test_unconfigured_board_never_imports_or_falls_back_to_environment(self):
        with patch.dict(os.environ, {"HERMES_KANBAN_DB": str(self.path), "HERMES_KANBAN_BOARD": "project"}), \
             patch.object(reader_module, "_canonical", side_effect=AssertionError("unconfigured must not import")):
            self.error("task_board_unconfigured", lambda: reader_module.HermesTaskReader({}).read(self.ref))

    def test_unknown_task_and_tenant_mismatch_are_rejected(self):
        self.error("task_not_found", lambda: self.reader.read(model.TaskRef("project", "t_ffffffffffff")))
        self.error("task_scope_mismatch", lambda: reader_module.HermesTaskReader({"project": self.path}, tenant="other").read(self.ref))

    def test_dependency_tenant_and_task_identity_are_checked(self):
        with sqlite3.connect(self.path) as connection:
            connection.execute("UPDATE tasks SET tenant='other' WHERE id=?", (PARENT,))
        self.error("task_scope_mismatch", lambda: self.reader.read(self.ref))
        self.error("task_scope_mismatch", lambda: reader_module.HermesTaskReader({"project": self.path}).read(self.ref))
        original = self.api.get_task
        self.api.get_task = lambda c, t: SimpleNamespace(**{**vars(original(c, t)), "id": OTHER})
        self.error("task_scope_mismatch", lambda: self.reader.read(self.ref))

    def test_each_owned_row_rejects_cross_task_data(self):
        for bucket in ("comments", "events", "runs", "attachments"):
            with self.subTest(bucket=bucket), patch.object(self.api, "list_" + bucket,
                    return_value=[SimpleNamespace(task_id=OTHER)]):
                self.error("task_scope_mismatch", lambda: self.reader.read(self.ref))

    def test_missing_invalid_and_directory_source_never_create_database(self):
        missing = self.path.parent / "missing.sqlite"
        for path in (missing, self.path.parent):
            self.error("task_source_unavailable", lambda: reader_module.HermesTaskReader({"project": path}).read(self.ref))
        self.assertFalse(missing.exists())
        self.path.write_bytes(b"not sqlite")
        self.error("task_source_unavailable", lambda: self.reader.read(self.ref))

    def test_relative_path_and_invalid_board_are_rejected(self):
        for mapping in ({"project": Path("relative.db")}, {"../elsewhere": self.path}, {"project": None}, []):
            self.error("task_ref_invalid", lambda: reader_module.HermesTaskReader(mapping))

    def test_even_a_host_reader_cannot_write(self):
        def malicious(connection, _task_id):
            self.assertEqual(connection.execute("PRAGMA query_only").fetchone()[0], 1)
            connection.execute("DELETE FROM tasks")
        self.api.get_task = malicious
        self.error("task_source_unavailable", lambda: self.reader.read(self.ref))
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM tasks").fetchone()[0], 4)

    def test_database_replacement_mid_read_returns_no_context(self):
        old = self.api.list_comments
        def replace(connection, task_id):
            self.path.rename(self.path.with_suffix(".original"))
            self.path.write_bytes(b"replacement")
            return old(connection, task_id)
        self.api.list_comments = replace
        self.error("task_source_changed", lambda: self.reader.read(self.ref))

    def test_one_snapshot_then_next_read_observes_committed_update(self):
        with sqlite3.connect(self.path) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
        old = self.api.list_comments
        def concurrent_write(connection, task_id):
            with sqlite3.connect(self.path) as writer:
                writer.execute("UPDATE tasks SET status='review' WHERE id=?", (TASK,))
                writer.execute("INSERT INTO task_comments VALUES(3,?,'reviewer','Fresh correction',3)", (TASK,))
            return old(connection, task_id)
        with patch.object(self.api, "list_comments", side_effect=concurrent_write):
            first = self.reader.read(self.ref)
        second = self.reader.read(self.ref)
        self.assertEqual((first["task"]["status"], len(first["comments"])), ("running", 1))
        self.assertEqual((second["task"]["status"], len(second["comments"])), ("review", 2))

    def test_equal_timestamp_rows_are_stable_even_if_host_order_changes(self):
        first = SimpleNamespace(id=1, task_id=TASK, created_at=1, body="First")
        second = SimpleNamespace(id=2, task_id=TASK, created_at=1, body="Second")
        with patch.object(self.api, "list_comments", return_value=[second, first]):
            backwards = model.build_task_context(self.ref, self.reader.read(self.ref))
        with patch.object(self.api, "list_comments", return_value=[first, second]):
            forwards = model.build_task_context(self.ref, self.reader.read(self.ref))
        self.assertEqual(backwards.digest, forwards.digest)
        self.assertEqual([row["body"] for row in forwards.data["comments"]], ["First", "Second"])

    def test_missing_host_or_read_capability_fails_explicitly(self):
        self.host.stop()
        for module in (None, SimpleNamespace(get_task=lambda *_: None)):
            with self.subTest(module=module), patch.object(reader_module.importlib, "import_module",
                    **({"side_effect": ImportError("missing")} if module is None else {"return_value": module})):
                self.error("task_capability_unavailable", lambda: self.reader.read(self.ref))


class InstalledHermesContractTest(unittest.TestCase):
    def test_actual_schema_and_canonical_readers_against_temporary_database(self):
        try:
            canonical = importlib.import_module("hermes_cli.kanban_db")
            writer_api = importlib.import_module("hermes_cli.kanban_db_connect")
            acceptance_api = importlib.import_module("hermes_cli.kanban_pr_acceptance")
        except ImportError:
            self.skipTest("Hermes is optional; run this contract check in the installed Hermes interpreter")
        sha = "a" * 40
        pr_url = "https://github.com/example/repo/pull/1"
        # Exercise Hermes' actual receipt producer with an offline GitHub
        # fixture. Its emitted shape, not a Tether-invented receipt, is stored.
        def github_fixture(endpoint, **_kwargs):
            if endpoint == "graphql":
                return {"data": {"repository": {"pullRequest": {"headRefOid": sha,
                    "baseRefName": "main", "state": "OPEN", "baseRef": {"branchProtectionRule": {
                        "requiredStatusChecks": [{"context": "tests", "app": {"databaseId": 1}}]}}}}}}
            if "/rules/branches/" in endpoint:
                return [[]]
            if "/check-runs?" in endpoint:
                return [{"total_count": 1, "check_runs": [{"id": 1, "name": "tests", "app": {"id": 1},
                    "status": "completed", "conclusion": "success", "head_sha": sha,
                    "html_url": "https://github.com/example/repo/actions/runs/1"}]}]
            if "/statuses?" in endpoint:
                return []
            if endpoint == "repos/example/repo/pulls/1":
                return {"head": {"sha": sha}, "base": {"ref": "main"}, "state": "open"}
            raise AssertionError("Unexpected offline GitHub fixture request")
        with patch.object(acceptance_api, "_api", side_effect=github_fixture):
            acceptance = acceptance_api.collect_acceptance("example/repo", pr_url)
        self.assertTrue(acceptance["ok"])
        with tempfile.TemporaryDirectory(prefix="tether-real-hermes-schema-") as root:
            path = Path(root) / "actual-schema.sqlite"
            with sqlite3.connect(path) as connection:
                connection.executescript(canonical.SCHEMA_SQL)
                connection.execute("INSERT INTO tasks(id,title,status,tenant,created_at) VALUES(?, 'Actual schema task','running','tenant-a',1)", (TASK,))
                connection.execute("INSERT INTO tasks(id,title,status,tenant,created_at,result) VALUES(?, 'Actual parent','done','tenant-a',1,'Edge cases specified')", (PARENT,))
                connection.execute("INSERT INTO tasks(id,title,status,tenant,created_at) VALUES(?, 'Actual child','ready','tenant-a',1)", (CHILD,))
                connection.executemany("INSERT INTO task_links(parent_id,child_id) VALUES(?,?)", [(PARENT, TASK), (TASK, CHILD)])
                connection.execute("INSERT INTO task_comments(task_id,author,body,created_at) VALUES(?,'reviewer','Actual schema comment',1)", (TASK,))
                connection.execute("INSERT INTO task_runs(task_id,status,started_at,summary) VALUES(?,'done',1,'Actual schema handoff')", (TASK,))
                connection.execute("UPDATE task_runs SET metadata=? WHERE task_id=?",
                                   (json.dumps({"published_pr": pr_url, "workspace_path": "PRIVATE_LOCAL_PATH"}), TASK))
                connection.execute("INSERT INTO task_events(task_id,kind,created_at) VALUES(?,'created',1)", (TASK,))
                connection.execute("INSERT INTO task_events(task_id,kind,payload,created_at) VALUES(?,'pr_acceptance',?,2)",
                                   (TASK, json.dumps(acceptance)))
                connection.execute("INSERT INTO task_attachments(task_id,filename,stored_path,created_at) VALUES(?,'artifact.txt','PRIVATE_LOCAL_PATH',1)", (TASK,))
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            files = sorted(p.name for p in path.parent.iterdir())
            with patch.object(writer_api, "connect", side_effect=AssertionError("writer connection forbidden")), \
                 patch.object(writer_api, "init_db", side_effect=AssertionError("migration forbidden")), \
                 patch.object(canonical, "connect", side_effect=AssertionError("writer facade forbidden")), \
                 patch.object(canonical, "init_db", side_effect=AssertionError("migration facade forbidden")), \
                 patch.object(canonical, "get_current_board", side_effect=AssertionError("ambient board forbidden")), \
                 patch.object(canonical, "build_worker_context", side_effect=AssertionError("role history forbidden")), \
                 patch.dict(os.environ, {"HERMES_KANBAN_DB": str(path.parent / "must-not-exist.sqlite"),
                                         "HERMES_KANBAN_BOARD": "unconfigured"}):
                raw = reader_module.HermesTaskReader({"project": path}, tenant="tenant-a").read(model.TaskRef("project", TASK))
            self.assertEqual(raw["task"]["title"], "Actual schema task")
            self.assertEqual(raw["comments"][0]["body"], "Actual schema comment")
            self.assertEqual(raw["runs"][0]["summary"], "Actual schema handoff")
            self.assertEqual(len(raw["attachments"]), 1)
            self.assertEqual([(r["task_id"], r["relationship"]) for r in raw["dependencies"]], [(PARENT, "parent"), (CHILD, "child")])
            context = model.build_task_context(model.TaskRef("project", TASK), raw)
            self.assertIn("Actual schema handoff", context.render())
            evidence = context.as_dict()["data"]["events"][1]["payload"]
            self.assertEqual(evidence["head_sha"], sha)
            self.assertEqual(evidence["classification"], "success")
            self.assertTrue(evidence["ok"])
            self.assertEqual(evidence["checks"][0]["head_sha"], sha)
            self.assertEqual(evidence["required"], [{"context": "tests", "app_id": 1}])
            self.assertIn(pr_url, context.as_dict()["data"]["runs"][0]["metadata"]["published_pr"])
            self.assertIn(sha, context.render())
            changed = json.loads(json.dumps(raw))
            changed["events"][1]["payload"]["classification"] = "pending"
            self.assertNotEqual(context.digest, model.build_task_context(model.TaskRef("project", TASK), changed).digest)
            self.assertNotIn("PRIVATE_LOCAL_PATH", json.dumps(raw))
            self.assertEqual(digest, hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(files, sorted(p.name for p in path.parent.iterdir()))


if __name__ == "__main__":
    unittest.main()
