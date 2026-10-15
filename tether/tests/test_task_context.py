"""Task-only prompt evidence, exercised without a host, database or computer."""
from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import sys
import unittest


PATH = Path(__file__).resolve().parents[1] / "runtime/plugin_next/task_context.py"
SPEC = importlib.util.spec_from_file_location("offline_task_context_fixture", PATH)
model = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = model
SPEC.loader.exec_module(model)


class TaskContextTest(unittest.TestCase):
    def setUp(self):
        self.ref = model.TaskRef("project", "t_example")
        self.raw = {
            "reference": self.ref.as_dict(),
            "task": {"id": "t_example", "title": "Implement median", "body": "Cover even lengths.",
                     "assignee": "builder", "status": "running", "tenant": "example",
                     "result": "Awaiting review", "completion_contract": "All edge cases pass",
                     "current_run_id": 1},
            "comments": [{"id": 1, "task_id": "t_example", "author": "reviewer",
                          "body": "Even lengths currently fail", "created_at": 1}],
            "events": [{"id": 1, "task_id": "t_example", "kind": "review_requested", "created_at": 2,
                        "payload": {"artifact_ref": {"uri": "https://example.invalid/pr/1", "revision": "abc"},
                                    "review_findings": [{"id": "f1", "summary": "Fix even case", "resolved": False}]}}],
            "runs": [{"id": 1, "task_id": "t_example", "status": "done", "summary": "Candidate ready",
                      "metadata": {"verification": ["unit cases passed"], "revision": "abc"}}],
            "attachments": [{"id": 1, "task_id": "t_example", "filename": "results.txt", "size": 20}],
            "dependencies": [{"board": "project", "task_id": "t_spec", "relationship": "parent",
                              "title": "Specify edges", "status": "done", "result": "Include even lengths"}],
        }

    def build(self):
        return model.build_task_context(self.ref, self.raw)

    def assertCode(self, code, operation):
        with self.assertRaises(model.TaskContextError) as caught:
            operation()
        self.assertEqual(caught.exception.code, code)

    def test_reference_roundtrip_and_path_rejection(self):
        self.assertEqual(model.TaskRef.from_dict(self.ref.as_dict()), self.ref)
        for value in ({"board": "project", "task_id": "../secret"},
                      {"board": "/tmp/board.db", "task_id": "t1"},
                      {"board": "Project", "task_id": "t1"},
                      {"board": "project", "task_id": "t1", "path": "board.db"},
                      {"board": "project"}, None):
            with self.subTest(value=value):
                self.assertCode("task_ref_invalid", lambda: model.TaskRef.from_dict(value))

    def test_snapshot_retains_task_review_artifacts_and_dependency(self):
        context = self.build()
        snapshot = context.as_dict()
        self.assertEqual(set(snapshot), {"task_ref", "digest", "data"})
        self.assertEqual(snapshot["task_ref"], self.ref.as_dict())
        self.assertEqual(snapshot["data"]["task"], self.raw["task"])
        self.assertEqual(snapshot["data"]["events"][0]["payload"]["review_findings"][0]["resolved"], False)
        self.assertEqual(snapshot["data"]["dependencies"], self.raw["dependencies"])
        self.assertIn("does not complete this task", context.render())

    def test_unknown_host_and_worker_fields_are_not_prompt_evidence(self):
        self.raw["worker_context"] = {"role_history": "PRIVATE_SENTINEL"}
        self.raw["role_history"] = "PRIVATE_SENTINEL"
        self.raw["task"]["env"] = "PRIVATE_SENTINEL"
        self.raw["runs"][0]["metadata"]["workspace_path"] = "PRIVATE_SENTINEL"
        self.raw["events"][0]["payload"]["token"] = "PRIVATE_SENTINEL"
        self.raw["attachments"][0]["stored_path"] = "PRIVATE_SENTINEL"
        self.raw["dependencies"][0]["worker_context"] = "PRIVATE_SENTINEL"
        context = self.build()
        self.assertNotIn("PRIVATE_SENTINEL", context.render())
        self.assertNotIn("PRIVATE_SENTINEL", json.dumps(context.as_dict()))

    def test_digest_is_deterministic_without_clock_or_mapping_order(self):
        expected = self.build().digest
        self.raw = dict(reversed(list(self.raw.items())))
        self.raw["task"] = dict(reversed(list(self.raw["task"].items())))
        self.assertEqual(self.build().digest, expected)
        self.raw["worker_context"] = {"attempt": 100}
        self.assertEqual(self.build().digest, expected)

    def test_human_correction_changes_snapshot_digest(self):
        before = self.build()
        self.raw["comments"].append({"id": 2, "task_id": "t_example", "author": "human",
                                     "body": "Also cover empty input", "created_at": 3})
        after = self.build()
        self.assertNotEqual(after.digest, before.digest)
        self.assertIn("Also cover empty input", after.render())

    def test_canonical_pr_acceptance_fields_are_typed_and_bounded(self):
        payload = {"pr_url": "https://github.com/example/repo/pull/1", "head_sha": "a" * 40,
                   "classification": "success", "ok": True,
                   "required": [{"context": "tests", "app_id": -1}],
                   "checks": [{"name": "tests", "id": i, "head_sha": "a" * 40,
                               "classification": "success", "conclusion": "success",
                               "worker_env": "PRIVATE_SENTINEL"} for i in range(55)]}
        self.raw["events"][0]["payload"] = payload
        self.raw["runs"][0]["metadata"] = {"published_pr": payload["pr_url"]}
        first = self.build()
        evidence = first.as_dict()["data"]["events"][0]["payload"]
        self.assertEqual(evidence["head_sha"], "a" * 40)
        self.assertTrue(evidence["ok"])
        self.assertEqual(len(evidence["checks"]), 50)
        self.assertEqual(evidence["omitted_checks"], 5)
        self.assertEqual(evidence["required"][0]["app_id"], -1)
        self.assertNotIn("PRIVATE_SENTINEL", first.render())
        payload["head_sha"] = "b" * 40
        self.assertNotEqual(first.digest, self.build().digest)
        for key, value in (("ok", "yes"), ("checks", {}),
                           ("required", [{"context": "tests", "app_id": True}])):
            old = payload[key]
            payload[key] = value
            self.assertCode("task_context_invalid", self.build)
            payload[key] = old

    def test_input_and_receipt_mutation_cannot_change_context(self):
        context = self.build()
        original = context.render()
        self.raw["task"]["body"] = "mutated input"
        receipt = context.as_dict()
        receipt["data"]["task"]["body"] = "mutated receipt"
        self.assertEqual(context.render(), original)
        with self.assertRaises(TypeError):
            context.data["task"]["body"] = "mutated context"

    def test_history_tail_limits_report_each_omission(self):
        for name, limit in model.TAIL_LIMITS.items():
            prototype = self.raw[name][0]
            self.raw[name] = [dict(prototype, id=i) for i in range(limit + 5)]
        data = self.build().as_dict()["data"]
        for name, limit in model.TAIL_LIMITS.items():
            self.assertEqual(len(data[name]), limit)
            self.assertEqual(data[name][0]["id"], 5)
            self.assertEqual(data["omitted"][name], 5)
        self.assertEqual(data["task"]["body"], "Cover even lengths.")

    def test_unrelated_row_is_rejected_even_if_tail_would_omit_it(self):
        self.raw["comments"] = [dict(self.raw["comments"][0], id=i) for i in range(30)]
        self.raw["comments"][0]["task_id"] = "t_unrelated"
        self.assertCode("task_scope_mismatch", self.build)

    def test_top_level_reference_and_task_scope_must_match(self):
        original = copy.deepcopy(self.raw)
        for mutate in (lambda r: r["reference"].update(board="other"),
                       lambda r: r["task"].update(id="t_other"),
                       lambda r: r["task"].update(board="other"),
                       lambda r: r["task"].update(task_id="t_other")):
            self.raw = copy.deepcopy(original)
            mutate(self.raw)
            self.assertCode("task_scope_mismatch", self.build)

    def test_source_reference_is_required(self):
        del self.raw["reference"]
        self.assertCode("task_context_invalid", self.build)

    def test_every_history_bucket_is_scope_checked(self):
        for name in ("comments", "events", "runs", "attachments"):
            with self.subTest(bucket=name):
                original = self.raw[name][0]["task_id"]
                self.raw[name][0]["task_id"] = "t_other"
                self.assertCode("task_scope_mismatch", self.build)
                self.raw[name][0]["task_id"] = original

    def test_dependency_scope_and_relationship_are_checked(self):
        self.raw["dependencies"][0]["board"] = "other"
        self.assertCode("task_scope_mismatch", self.build)
        self.raw["dependencies"][0]["board"] = "project"
        self.raw["dependencies"][0]["relationship"] = "unrelated"
        self.assertCode("task_context_invalid", self.build)

    def test_invalid_handoff_types_are_rejected(self):
        for value in (42, {"findings": "all good"}, {"verification": ["ok", 2]},
                      {"artifact_ref": {"label": "Missing location"}},
                      {"findings": [{"resolved": "yes"}]}):
            with self.subTest(value=value):
                self.raw["events"][0]["payload"] = value
                self.assertCode("task_context_invalid", self.build)

    def test_utf8_byte_budget_rejects_instead_of_truncating_task_body(self):
        self.raw["task"]["body"] = "界" * (model.MAX_CONTEXT_BYTES // 3)
        self.assertCode("task_context_too_large", self.build)
        self.assertEqual(len(self.raw["task"]["body"]), model.MAX_CONTEXT_BYTES // 3)

    def test_exact_render_budget_is_inclusive(self):
        self.raw["task"]["body"] = ""
        overhead = len(self.build().render().encode("utf-8"))
        self.raw["task"]["body"] = "x" * (model.MAX_CONTEXT_BYTES - overhead)
        self.assertEqual(len(self.build().render().encode("utf-8")), model.MAX_CONTEXT_BYTES)
        self.raw["task"]["body"] += "x"
        self.assertCode("task_context_too_large", self.build)

    def test_invalid_unicode_and_digest_are_rejected(self):
        self.raw["task"]["body"] = "\ud800"
        self.assertCode("task_context_invalid", self.build)
        self.assertCode("task_context_invalid", lambda: model.TaskContext(self.ref, {}, "bad-digest"))

    def test_wrong_scalar_types_are_rejected(self):
        for field, value in (("body", ["text"]), ("current_run_id", True), ("current_run_id", -1)):
            with self.subTest(field=field, value=value):
                original = self.raw["task"][field]
                self.raw["task"][field] = value
                self.assertCode("task_context_invalid", self.build)
                self.raw["task"][field] = original


if __name__ == "__main__":
    unittest.main()
