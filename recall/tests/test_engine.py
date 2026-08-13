"""Synthetic, fixture-scoped tests for the local recall engine."""
from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import os
import shutil
import tempfile
import threading
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "skills/recall/scripts/recall.py"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
spec = importlib.util.spec_from_file_location("recall_engine", SCRIPT)
engine = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(engine)


class RecallEngineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        # macOS exposes /var through the /private/var filesystem alias. The
        # engine intentionally stores canonical paths, so expected paths must
        # use that same cross-platform contract.
        self.root = Path(self.tmp.name).resolve()
        self.claude = self.root / "claude"
        self.codex = self.root / "codex"
        self.claude.mkdir(); self.codex.mkdir()
        self.db = self.root / "state/index.db"
        self.old_env = {key: os.environ.get(key) for key in (
            "RECALL_CLAUDE_ROOT", "RECALL_CODEX_ROOT", "RECALL_DB", "RECALL_URL",
            "RECALL_MODE", "RECALL_TOKEN_FILE", "RECALL_CONFIG_FILE",
            "RECALL_SESSION_CURSOR_DB", "RECALL_EXPORT_SOURCE_ID", "CODEX_THREAD_ID",
            "CLAUDE_SESSION_ID", "CLAUDE_CODE_SESSION_ID", "CLAUDECODE",
            "CLAUDE_CODE_ENTRYPOINT",
        )}
        os.environ.update(
            RECALL_CLAUDE_ROOT=str(self.claude),
            RECALL_CODEX_ROOT=str(self.codex),
            RECALL_DB=str(self.db),
            RECALL_MODE="local",
            RECALL_CONFIG_FILE=str(self.root / "absent-client.json"),
            RECALL_SESSION_CURSOR_DB=str(self.root / "state/session-cursors.db"),
            RECALL_EXPORT_SOURCE_ID="claude:linux:test",
        )
        for key in (
            "RECALL_URL", "RECALL_TOKEN_FILE", "CODEX_THREAD_ID", "CLAUDE_SESSION_ID",
            "CLAUDE_CODE_SESSION_ID", "CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT",
        ):
            os.environ.pop(key, None)

    def tearDown(self):
        for key, value in self.old_env.items():
            if value is None: os.environ.pop(key, None)
            else: os.environ[key] = value
        self.tmp.cleanup()

    def cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = engine.main(list(argv))
        self.assertEqual(code, 0, err.getvalue())
        return out.getvalue()

    def write_claude(self, name="session.jsonl", extra=""):
        target = self.claude / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES / "claude_sample.jsonl", target)
        if extra:
            with target.open("a") as fh: fh.write(extra)
        return target

    def copy_codex(self):
        target = self.codex / "2026/02/03/rollout-fixture.jsonl"
        target.parent.mkdir(parents=True)
        shutil.copy(FIXTURES / "codex_rollout.jsonl", target)
        return target

    def rows(self, sql):
        conn = engine.connect(self.db)
        result = conn.execute(sql).fetchall(); conn.close()
        return result

    def test_append_resume_grows_without_duplicates(self):
        p = self.write_claude()
        self.cli("index")
        before = self.rows("select count(*) from chunks")[0][0]
        line = json.dumps({"type":"user", "timestamp":"2026-01-02T03:05:00Z", "message":{"content":"appended uniquely"}}) + "\n"
        with p.open("a") as fh: fh.write(line)
        self.assertIn("appended=1", self.cli("index"))
        self.assertEqual(self.rows("select count(*) from chunks")[0][0], before + 1)
        self.cli("index")
        self.assertEqual(self.rows("select count(*) from chunks")[0][0], before + 1)

    def test_truncation_reparses_and_rename_tombstones(self):
        p = self.write_claude(); self.cli("index")
        p.write_text(json.dumps({"type":"user", "timestamp":"2026-01-03T00:00:00Z", "message":{"content":"short replacement"}}) + "\n")
        self.cli("index")
        self.assertEqual(self.rows("select count(*) from chunks")[0][0], 1)
        newer = self.claude / "renamed.jsonl"; p.rename(newer)
        self.cli("index")
        statuses = {Path(r[0]).name: r[1] for r in self.rows("select path,status from files")}
        self.assertEqual(statuses["session.jsonl"], "tombstone")
        self.assertEqual(statuses["renamed.jsonl"], "ok")

    def test_final_partial_line_is_safe_and_resumed(self):
        p = self.write_claude()
        partial = '{"type":"user","timestamp":"2026-01-04T00:00:00Z","message":{"content":"later"}}'
        with p.open("a") as fh: fh.write(partial)
        self.cli("index")
        self.assertEqual(self.rows("select status from files")[0][0], "partial")
        self.assertNotIn("later", self.cli("search", "later"))
        with p.open("a") as fh: fh.write("\n")
        self.cli("index")
        self.assertIn("later", self.cli("search", "later"))

    def test_session_export_pages_1001_items_and_redacts_cursor_surface(self):
        session = self.claude / "long.jsonl"
        secret = "sk-" + "A" * 32
        with session.open("w") as output:
            for index in range(1001):
                text = f"event {index}"
                if index == 500:
                    text = "api_key=" + secret
                output.write(json.dumps({
                    "type": "user", "timestamp": f"2026-01-01T00:{(index // 60) % 60:02d}:{index % 60:02d}Z",
                    "message": {"content": text},
                }) + "\n")
        first = json.loads(self.cli("session-export", "--target", str(session), "--limit", "1000"))
        self.assertFalse(first["page"]["complete"])
        self.assertEqual(first["page"]["count"], 1000)
        self.assertTrue(first["page"]["next_cursor"].startswith("rsl_"))
        self.assertNotIn(secret, json.dumps(first))
        self.assertNotIn(str(session), first["page"]["next_cursor"])
        with session.open("a") as output:
            output.write(json.dumps({
                "type": "user", "timestamp": "2026-01-02T00:00:00Z",
                "message": {"content": "appended after snapshot"},
            }) + "\n")
        second = json.loads(self.cli("session-export", "--cursor", first["page"]["next_cursor"], "--limit", "1000"))
        replay = json.loads(self.cli("session-export", "--cursor", first["page"]["next_cursor"], "--limit", "1000"))
        self.assertTrue(second["page"]["complete"])
        self.assertFalse(second["session"]["source_snapshot_stable"])
        self.assertEqual(second["page"]["count"], 1)
        self.assertEqual(
            [item["evidence_id"] for item in second["items"]],
            [item["evidence_id"] for item in replay["items"]],
        )
        items = first["items"] + second["items"]
        self.assertEqual([item["sequence"] for item in items], list(range(1001)))
        self.assertEqual(len({item["evidence_id"] for item in items}), 1001)
        self.assertEqual(first["session"]["boundary_receipt"], second["session"]["boundary_receipt"])
        self.assertEqual((self.root / "state/session-cursors.db").stat().st_mode & 0o777, 0o600)

    def test_session_export_cursor_store_rejects_symlink(self):
        state = self.root / "state"
        state.mkdir(mode=0o700, exist_ok=True)
        target = state / "target.db"
        target.write_text("unchanged")
        target.chmod(0o600)
        link = state / "cursor-link.db"
        link.symlink_to(target)
        os.environ["RECALL_SESSION_CURSOR_DB"] = str(link)
        with self.assertRaisesRegex(ValueError, "symlink"):
            engine.export_cursor_connection()
        self.assertEqual(target.read_text(), "unchanged")

    def test_session_export_cursor_store_never_chmods_shared_parent(self):
        shared = self.root / "shared"
        shared.mkdir(mode=0o755)
        shared.chmod(0o755)
        before = shared.stat().st_mode & 0o777
        os.environ["RECALL_SESSION_CURSOR_DB"] = str(shared / "cursor.db")
        with self.assertRaisesRegex(ValueError, "0700"):
            engine.export_cursor_connection()
        self.assertEqual(shared.stat().st_mode & 0o777, before)
        self.assertFalse((shared / "cursor.db").exists())

    def test_session_export_marks_partial_record_and_current_codex_is_exact(self):
        os.environ["RECALL_EXPORT_SOURCE_ID"] = "codex:linux:test"
        thread = "12345678-1234-1234-1234-123456789abc"
        session = self.codex / f"2026/01/01/rollout-2026-01-01T00-00-00-{thread}.jsonl"
        session.parent.mkdir(parents=True)
        session.write_text(
            json.dumps({"timestamp": "2026-01-01T00:00:00Z", "type": "response_item", "payload": {
                "role": "user", "content": [{"type": "input_text", "text": "exact current"}]}}) + "\n"
            + '{"type":"response_item"'
        )
        os.environ["CODEX_THREAD_ID"] = thread
        page = json.loads(self.cli("session-export", "--current"))
        self.assertTrue(page["page"]["complete"])
        self.assertTrue(page["session"]["source_partial_record"])
        self.assertEqual(page["items"][0]["text"], "exact current")

    def test_session_export_redacts_private_key_blocks_and_generic_key_assignments(self):
        private_value = "Z" * 50
        pem = "-----BEGIN " + "PRIVATE KEY-----\n" + ("Q" * 256) + "\n-----END " + "PRIVATE KEY-----"
        session = self.claude / "sensitive.jsonl"
        session.write_text(json.dumps({
            "type": "user", "timestamp": "2026-01-01T00:00:00Z",
            "message": {"content": "key=" + private_value + "\n" + pem + "\nsafe"},
        }) + "\n")
        page = json.loads(self.cli("session-export", "--target", str(session)))
        rendered = json.dumps(page)
        self.assertNotIn(private_value, rendered)
        self.assertNotIn("Q" * 64, rendered)
        self.assertIn("redacted", rendered.lower())
        self.assertIn("safe", rendered)

    def test_session_export_redacts_secret_shaped_native_tool_entity(self):
        secret = "Z" * 40
        session = self.claude / "sensitive-tool.jsonl"
        session.write_text(json.dumps({
            "type": "assistant", "timestamp": "2026-01-01T00:00:00Z",
            "message": {"content": [{
                "type": "tool_use", "name": "api_key=" + secret, "input": {"path": "safe.txt"},
            }]},
        }) + "\n")
        page = json.loads(self.cli("session-export", "--target", str(session)))
        rendered = json.dumps(page)
        self.assertNotIn(secret, rendered)
        self.assertIn("redacted", rendered.lower())

    def test_session_redaction_covers_deep_sweep_provider_formats(self):
        values = (
            "sk-" + "O" * 48,
            "sk-ant-api03-" + "A" * 90 + "AA",
            "AIza" + "G" * 35,
            "sk-or-v1-" + "R" * 40,
            "gsk_" + "Q" * 40,
            "xai-" + "X" * 40,
            "pplx-" + "P" * 40,
            "csk-" + "C" * 40,
            "github_pat_" + "J" * 60,
            "xoxb-" + "S" * 40,
            "ops_" + "W" * 40,
            "AKIA" + "Z" * 16,
            "sk_live_" + "T" * 32,
            "hf_" + "F" * 40,
            "pcsk_" + "N" * 40,
            "lsv2_" + "L" * 40,
        )
        for value in values:
            with self.subTest(prefix=value[:10]):
                self.assertNotIn(value, engine.clean_text("prefix " + value + " suffix"))
        fireworks = "FireworksSynthetic" + "8" * 40
        self.assertNotIn(
            fireworks,
            engine.clean_text("FIREWORKS_API_KEY=" + fireworks),
        )
        nested = "NestedSynthetic" + "7" * 40
        self.assertNotIn(
            nested,
            engine.clean_text('{"service":{"fireworks_api_key_value":"' + nested + '"}}'),
        )
        multiline = "MultilineSynthetic" + "6" * 40
        self.assertNotIn(
            multiline,
            engine.clean_text("| OP_SERVICE_ACCOUNT_TOKEN\n=\n" + multiline),
        )
        self.assertNotIn(multiline, engine.clean_text("| deployment_key = " + multiline))
        proximity = "a" * 63 + "7"
        self.assertNotIn(proximity, engine.clean_text("_sentry token " + proximity))
        self.assertNotIn(proximity, engine.clean_text("other_access_token " + proximity))

    def test_session_redaction_is_idempotent_after_nested_secret_markers(self):
        secret = "SyntheticSecret" + "9" * 40
        value = (
            "safe prefix\n"
            "access_token:\n"
            f", Authorization={secret}\n"
            "safe suffix"
        )

        once = engine.clean_text(value)

        self.assertNotIn(secret, once)
        self.assertEqual(engine.clean_text(once), once)

    def test_ambiguous_current_error_uses_content_free_ranked_receipts(self):
        session = self.claude / "candidate-secret-token-value.jsonl"
        session.write_text(json.dumps({"type": "user", "message": {"content": "private"}}) + "\n")
        with self.assertRaises(ValueError) as raised:
            engine.resolve_current_session()
        message = str(raised.exception)
        self.assertIn("ranked_candidate_receipts=", message)
        self.assertNotIn(str(session), message)
        self.assertNotIn("private", message)

    def test_current_claude_identity_requires_exact_native_id(self):
        session_id = "87654321-4321-4321-4321-cba987654321"
        session = self.claude / "project" / f"{session_id}.jsonl"
        session.parent.mkdir()
        session.write_text(json.dumps({"type": "user", "message": {"content": "exact"}}) + "\n")
        os.environ["CLAUDE_SESSION_ID"] = session_id
        self.assertEqual(engine.resolve_current_session(), session.resolve())
        duplicate = self.claude / f"duplicate-{session_id}.jsonl"
        duplicate.write_text(session.read_text())
        with self.assertRaisesRegex(ValueError, "resolved to 2"):
            engine.resolve_current_session()

    def test_current_identity_fails_when_codex_and_claude_are_both_present(self):
        os.environ["CODEX_THREAD_ID"] = "codex-id"
        os.environ["CLAUDE_SESSION_ID"] = "claude-id"
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            engine.resolve_current_session()

    def test_active_claude_refuses_an_inherited_codex_identity(self):
        os.environ["CLAUDECODE"] = "1"
        os.environ["CODEX_THREAD_ID"] = "parent-codex-id"
        with self.assertRaisesRegex(ValueError, "inherited Codex identity was ignored"):
            engine.resolve_current_session()

    def test_active_claude_prefers_its_exact_identity_over_inherited_codex(self):
        session_id = "12345678-4321-4321-4321-cba987654321"
        session = self.claude / "project" / f"{session_id}.jsonl"
        session.parent.mkdir()
        session.write_text(json.dumps({"type": "user", "message": {"content": "exact"}}) + "\n")
        os.environ["CLAUDE_CODE_ENTRYPOINT"] = "cli"
        os.environ["CLAUDE_CODE_SESSION_ID"] = session_id
        os.environ["CODEX_THREAD_ID"] = "parent-codex-id"
        self.assertEqual(engine.resolve_current_session(), session.resolve())

    def test_session_relations_selects_codex_children_and_fork_chain_exactly(self):
        def write_codex(name, node_id, *, parent=None, forked=None):
            target = self.codex / "2026/07/14" / f"rollout-{name}.jsonl"
            target.parent.mkdir(parents=True, exist_ok=True)
            payload = {"id": node_id, "cwd": "/workspace"}
            if parent:
                payload["source"] = {"subagent": {"thread_spawn": {"parent_thread_id": parent}}}
            if forked:
                payload["forked_from_id"] = forked
            target.write_text(json.dumps({"type": "session_meta", "payload": payload}) + "\n")
            return target

        main = write_codex("main", "main-id")
        write_codex("child", "child-id", parent="main-id")
        write_codex("nested", "nested-id", parent="child-id")
        write_codex("fork", "fork-id", forked="main-id")
        write_codex("unrelated", "other-id")
        graph = json.loads(self.cli(
            "session-relations", "--target", str(main), "--include-children", "--chain",
        ))
        self.assertTrue(graph["graph_complete"])
        self.assertEqual({node["node_id"] for node in graph["nodes"]},
                         {"main-id", "child-id", "nested-id", "fork-id"})
        self.assertNotIn("other-id", {node["node_id"] for node in graph["nodes"]})
        self.assertEqual({edge["type"] for edge in graph["edges"]}, {"child", "continuation"})

    def test_session_relations_maps_claude_sidechain_without_filename_guessing(self):
        session_id = "claude-main-id"
        main = self.claude / "project" / "opaque-main.jsonl"
        child = self.claude / "project" / "subagents" / "opaque-child.jsonl"
        main.parent.mkdir(parents=True)
        child.parent.mkdir(parents=True)
        main.write_text(json.dumps({"sessionId": session_id, "type": "user", "message": {"content": "x"}}) + "\n")
        child.write_text(json.dumps({
            "sessionId": session_id, "agentId": "claude-child-id", "isSidechain": True,
            "type": "assistant", "message": {"content": "y"},
        }) + "\n")
        graph = json.loads(self.cli(
            "session-relations", "--target", str(main), "--include-children",
        ))
        self.assertEqual([node["node_id"] for node in graph["nodes"]],
                         [session_id, "claude-child-id"])
        self.assertEqual(graph["edges"], [{"from": session_id, "to": "claude-child-id", "type": "child"}])

    def test_session_relations_fails_closed_for_missing_fork_ancestor(self):
        target = self.codex / "rollout-fork.jsonl"
        target.write_text(json.dumps({
            "type": "session_meta", "payload": {"id": "fork-id", "forked_from_id": "missing-id"},
        }) + "\n")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = engine.main(["session-relations", "--target", str(target), "--chain"])
        self.assertEqual(code, 2)
        graph = json.loads(out.getvalue())
        self.assertFalse(graph["graph_complete"])
        self.assertEqual(graph["incomplete_relations"][0]["reason"], "missing")

    def test_session_relations_fails_closed_for_claude_child_without_agent_id(self):
        main = self.claude / "project" / "main.jsonl"
        child = self.claude / "project" / "subagents" / "broken.jsonl"
        main.parent.mkdir(parents=True)
        child.parent.mkdir(parents=True)
        main.write_text(json.dumps({"sessionId": "main-id", "type": "user"}) + "\n")
        child.write_text(json.dumps({
            "sessionId": "main-id", "isSidechain": True, "type": "assistant",
        }) + "\n")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = engine.main([
                "session-relations", "--target", str(main), "--include-children",
            ])
        self.assertEqual(code, 2)
        graph = json.loads(out.getvalue())
        self.assertEqual(graph["incomplete_relations"][0]["reason"], "missing_agent_id")
        self.assertNotIn(str(child), out.getvalue())

    def test_session_relations_remote_only_mode_fails_without_local_fallback(self):
        target = self.codex / "rollout-remote.jsonl"
        target.write_text(json.dumps({
            "type": "session_meta", "payload": {"id": "remote-id"},
        }) + "\n")
        previous_mode = os.environ.get("RECALL_MODE")
        os.environ["RECALL_MODE"] = "remote"
        out, err = io.StringIO(), io.StringIO()
        try:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                with self.assertRaises(SystemExit) as raised:
                    engine.main(["session-relations", "--target", str(target), "--chain"])
        finally:
            if previous_mode is None:
                os.environ.pop("RECALL_MODE", None)
            else:
                os.environ["RECALL_MODE"] = previous_mode
        self.assertEqual(raised.exception.code, 2)
        self.assertEqual(out.getvalue(), "")
        self.assertIn("requires local native transcript metadata", err.getvalue())

    def test_session_relations_rejects_credential_shaped_native_identity(self):
        secret = "xai-" + "X" * 40
        target = self.codex / "rollout-unsafe-id.jsonl"
        target.write_text(json.dumps({
            "type": "session_meta", "payload": {"id": secret},
        }) + "\n")
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            with self.assertRaises(SystemExit) as raised:
                engine.main(["session-relations", "--target", str(target)])
        self.assertEqual(raised.exception.code, 2)
        self.assertNotIn(secret, out.getvalue() + err.getvalue())

    def test_secret_redaction_tool_cap_and_fts_injection(self):
        secret = "sk-ABCDEFGHIJKLMNOPQRSTUVWX"
        huge = "z" * 5000
        p = self.claude / "secret.jsonl"
        p.write_text("\n".join([
            json.dumps({"type":"user","timestamp":"2026-01-01T00:00:00Z","message":{"content":"token=" + secret + "\nnormal safe words"}}),
            json.dumps({"type":"assistant","timestamp":"2026-01-01T00:00:01Z","message":{"content":[{"type":"tool_result","content":huge}]}}),
        ]) + "\n")
        self.cli("index")
        self.assertNotIn(secret, self.cli("search", secret))
        self.assertEqual(self.rows("select length(text) from chunks where surface='tool_output'")[0][0], 4096)
        self.cli("search", '"foo OR bar)(')

    def test_entity_extraction_is_pure_normalized_and_shared(self):
        uuid = "DEADBEEF-1234-1234-1234-123456789ABC"
        entities = engine.extract_entities(
            f"see /workspace/pkg/file.py and {uuid} after ConnectTimeout",
            [("tool", "Read")],
        )
        self.assertIn(("file_path", "/workspace/pkg/file.py"), entities)
        self.assertIn(("uuid", uuid.lower()), entities)
        self.assertIn(("uuid", "deadbeef"), entities)
        self.assertIn(("error", "ConnectTimeout"), entities)
        self.assertIn(("tool", "Read"), entities)
        self.assertEqual(entities, sorted(set(entities)))

    def test_quoted_secret_and_codex_metadata_are_redacted(self):
        secret = "AbCdEfGhIjKlMnOpQrStUvWxYz123456"
        rollout = self.codex / "2026/01/01/rollout-secret.jsonl"
        rollout.parent.mkdir(parents=True)
        rollout.write_text("\n".join([
            json.dumps({"timestamp":"2026-01-01T00:00:00Z", "type":"session_meta", "payload": {
                "title": '"api_key": "' + secret, "first_user_prompt": 'authorization: "' + secret,
                "cwd":"/safe"}}),
            json.dumps({"timestamp":"2026-01-01T00:00:01Z", "type":"response_item", "payload": {
                "role":"user", "content":[{"type":"input_text", "text":'"token": "' + secret}]}}),
        ]) + "\n")
        self.cli("index")
        self.assertNotIn(secret, self.cli("search", secret))
        stored = self.rows("select title,first_user_prompt from sessions")[0]
        self.assertNotIn(secret, stored[0]); self.assertNotIn(secret, stored[1])
        self.assertEqual(stored[0], "[redacted-secret-line]")

    def test_current_codex_record_shapes_project_user_agent_and_tool_surfaces(self):
        marker = "c6a-current-codex-projection-4f18"
        cases = [
            (
                {
                    "timestamp": "2026-07-13T00:00:00Z",
                    "type": "event_msg",
                    "payload": {
                        "type": "user_message", "message": marker + " question",
                        "images": [], "local_images": [], "text_elements": [],
                    },
                },
                ("user", marker + " question"),
            ),
            (
                {
                    "timestamp": "2026-07-13T00:00:00Z",
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_message", "message": marker,
                        "phase": "final_answer", "memory_citation": None,
                    },
                },
                ("assistant", marker),
            ),
            (
                {
                    "timestamp": "2026-07-13T00:00:01Z",
                    "type": "response_item",
                    "payload": {
                        "type": "agent_message", "author": "assistant",
                        "recipient": "user", "content": [
                            {"type": "text", "text": marker + " response"},
                            {"type": "encrypted_content", "encrypted_content": "opaque"},
                        ],
                    },
                },
                ("assistant", marker + " response"),
            ),
            (
                {
                    "timestamp": "2026-07-13T00:00:02Z",
                    "type": "response_item",
                    "payload": {
                        "type": "custom_tool_call", "name": "synthetic_tool",
                        "call_id": "call-1", "input": marker + " input",
                    },
                },
                ("tool_input", marker + " input"),
            ),
            (
                {
                    "timestamp": "2026-07-13T00:00:03Z",
                    "type": "response_item",
                    "payload": {
                        "type": "custom_tool_call_output", "call_id": "call-1",
                        "output": [{"type": "text", "text": marker + " output"}],
                    },
                },
                ("tool_output", marker + " output"),
            ),
        ]
        for record, expected in cases:
            with self.subTest(record_type=record["payload"]["type"]):
                parsed, _metadata = engine.codex_record(record)
                self.assertEqual([(surface, text) for _, surface, text, _ in parsed], [expected])

        ignored, _metadata = engine.codex_record({
            "timestamp": "2026-07-13T00:00:04Z", "type": "event_msg",
            "payload": {"type": "token_count", "info": {"total": 42}},
        })
        self.assertEqual(ignored, [])

    def test_partial_fts_or_fallback_returns_natural_partial_matches(self):
        (self.claude / "alpha.jsonl").write_text(json.dumps({"type":"user", "timestamp":"2026-01-01T00:00:00Z", "message":{"content":"alpha bravo only"}}) + "\n")
        (self.claude / "beta.jsonl").write_text(json.dumps({"type":"user", "timestamp":"2026-01-01T00:00:01Z", "message":{"content":"alpha charlie only"}}) + "\n")
        self.cli("index")
        paths = self.cli("search", "alpha bravo charlie", "--paths")
        self.assertIn("alpha.jsonl", paths)
        self.assertIn("beta.jsonl", paths)

    def test_phrase_leg_finds_exact_error_among_common_decoys(self):
        error = "TypeError expected str got None"
        for number in range(8):
            (self.claude / f"decoy-{number}.jsonl").write_text(json.dumps({
                "type":"assistant", "timestamp":"2026-01-01T00:00:00Z",
                "message":{"content":"TypeError expected another value"}}) + "\n")
        target = self.claude / "error-target.jsonl"
        target.write_text(json.dumps({"type":"assistant", "timestamp":"2026-01-01T00:00:01Z",
                                      "message":{"content":"prefix " + error + " suffix"}}) + "\n")
        self.cli("index")
        self.assertEqual(Path(self.cli("search", error, "--paths").splitlines()[0]), target)

    def test_entity_direct_survives_fts_leg_cutoff(self):
        uuid = "deadbeef-1234-1234-1234-123456789abc"
        target = self.claude / "entity-target.jsonl"
        target.write_text(json.dumps({"type":"assistant", "timestamp":"2026-01-01T00:00:00Z",
                                      "message":{"content":[{"type":"tool_result", "content":uuid}]}}) + "\n")
        decoy = self.claude / "entity-decoys.jsonl"
        decoy.write_text(json.dumps({"type":"user", "timestamp":"2026-01-01T00:00:00Z",
                                     "message":{"content":"seed"}}) + "\n")
        self.cli("index")
        conn = engine.connect(self.db)
        decoy_session = conn.execute("select s.id from sessions s join files f on f.id=s.file_id where f.path like '%entity-decoys.jsonl'").fetchone()[0]
        for _ in range(4):
            chunk_id = conn.execute("insert into chunks(session_id,ts,surface,text) values (?,?,?,?)", (decoy_session, 1, "assistant", "common " + uuid + " " + uuid)).lastrowid
            conn.execute("insert into chunks_fts(rowid,text) values (?,?)", (chunk_id, "common " + uuid + " " + uuid))
        conn.commit(); conn.close()
        old_limit = engine.FTS_LEG_LIMIT
        engine.FTS_LEG_LIMIT = 2
        try:
            self.assertEqual(Path(self.cli("search", "common " + uuid, "--paths").splitlines()[0]), target)
        finally:
            engine.FTS_LEG_LIMIT = old_limit

    def test_identifier_token_leg_finds_raw_tool_output_without_entity(self):
        identifier = "identifier-prod-6fcdc84dd4-mmjpj"
        target = self.claude / "identifier-tool.jsonl"
        target.write_text(json.dumps({"type":"user", "timestamp":"2026-01-01T00:00:00Z",
                                      "message":{"content":"seed"}}) + "\n")
        self.cli("index")
        conn = engine.connect(self.db)
        session_id = conn.execute("select s.id from sessions s join files f on f.id=s.file_id where f.path like '%identifier-tool.jsonl'").fetchone()[0]
        chunk_id = conn.execute("insert into chunks(session_id,ts,surface,text) values (?,?,?,?)", (session_id, 1, "tool_output", identifier)).lastrowid
        conn.execute("insert into chunks_fts(rowid,text) values (?,?)", (chunk_id, identifier))
        conn.commit(); conn.close()
        self.assertEqual(Path(self.cli("search", "which pod was failing " + identifier, "--paths").splitlines()[0]), target)

    def test_gate_keeps_fuzzy_best_with_two_long_terms(self):
        target = self.claude / "fuzzy-best.jsonl"
        target.write_text(json.dumps({"type":"user", "timestamp":"2026-01-01T00:00:00Z",
                                      "message":{"content":"aurora beacon remediation"}}) + "\n")
        self.cli("index")
        paths = self.cli("search", "aurora unrelated beacon", "--paths")
        self.assertIn("fuzzy-best.jsonl", paths)

    def test_vocab_pruning_omits_noisy_or_term(self):
        target = self.claude / "signal.jsonl"
        target.write_text(json.dumps({"type":"user", "timestamp":"2026-01-01T00:00:00Z",
                                      "message":{"content":"signalterm anotherterm"}}) + "\n")
        self.cli("index")
        original = engine.vocab_doc_counts
        engine.vocab_doc_counts = lambda conn, terms: {"noisyterm": 100001, "signalterm": 1, "anotherterm": 1}
        try:
            self.assertEqual(Path(self.cli("search", "noisyterm signalterm anotherterm", "--paths").splitlines()[0]), target)
        finally:
            engine.vocab_doc_counts = original

    def test_negative_and_stopword_queries_are_empty(self):
        self.write_claude(); self.copy_codex(); self.cli("index")
        self.assertEqual(self.cli("search", "kafka consumer rebalancing tuning", "--paths"), "")
        self.assertEqual(self.cli("search", "the and of to", "--paths"), "")

    def test_fts_limit_keeps_bm25_best_candidate(self):
        filler = self.claude / "filler.jsonl"
        best = self.claude / "best.jsonl"
        filler.write_text(json.dumps({"type":"user", "timestamp":"2026-01-01T00:00:00Z", "message":{"content":"seed"}}) + "\n")
        best.write_text(json.dumps({"type":"user", "timestamp":"2026-01-01T00:00:00Z", "message":{"content":"seed"}}) + "\n")
        self.cli("index")
        conn = engine.connect(self.db)
        ids = {Path(r[1]).name: r[0] for r in conn.execute("select s.id,f.path from sessions s join files f on f.id=s.file_id")}
        for _ in range(1001):
            chunk_id = conn.execute("insert into chunks(session_id,ts,surface,text) values (?,?,?,?)", (ids["filler.jsonl"], 1, "user", "rankingword")).lastrowid
            conn.execute("insert into chunks_fts(rowid,text) values (?,?)", (chunk_id, "rankingword"))
        chunk_id = conn.execute("insert into chunks(session_id,ts,surface,text) values (?,?,?,?)", (ids["best.jsonl"], 1, "user", " ".join(["rankingword"] * 40))).lastrowid
        conn.execute("insert into chunks_fts(rowid,text) values (?,?)", (chunk_id, " ".join(["rankingword"] * 40)))
        conn.commit(); conn.close()
        self.assertIn("best.jsonl", self.cli("search", "rankingword", "--paths"))

    def test_append_fingerprint_detects_tail_edit_with_preserved_stat(self):
        p = self.claude / "large.jsonl"
        old = "a" * 12000
        p.write_text(json.dumps({"type":"user", "timestamp":"2026-01-01T00:00:00Z", "message":{"content":old}}) + "\n")
        self.cli("index")
        stat = p.stat()
        changed = p.read_text()
        # Change a region within the final 4 KiB while retaining byte length and mtime.
        changed = changed[:-100] + changed[-100:].replace("a", "b", 1)
        p.write_text(changed); os.utime(p, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        self.cli("index")
        self.assertIn("b", self.rows("select text from chunks")[0][0][-200:])

    def test_tombstoned_file_resurrects_without_reparse(self):
        p = self.write_claude(); parked = self.root / "parked.jsonl"
        self.cli("index")
        p.rename(parked); self.cli("index")
        self.assertEqual(self.rows("select status from files")[0][0], "tombstone")
        parked.rename(p); self.cli("index")
        self.assertEqual(self.rows("select status from files")[0][0], "ok")
        self.assertIn("session.jsonl", self.cli("search", "PAR-123"))

    def test_required_indexes_exist(self):
        self.write_claude(); self.cli("index")
        conn = engine.connect_ro(self.db)
        names = {row[1] for table in ("chunks", "entities", "sessions") for row in conn.execute(f"pragma index_list({table})")}
        conn.close()
        self.assertTrue({"chunks_session_idx", "entities_chunk_idx", "sessions_file_idx"} <= names)

    def test_entity_boost_filters_and_codex_reader(self):
        c = self.write_claude("old.jsonl")
        # A generic recent session supplies a less-specific ordinary match.
        (self.claude / "generic.jsonl").write_text(json.dumps({"type":"user","timestamp":"2026-07-01T00:00:00Z","cwd":"/other","message":{"content":"mention an unrelated identifier"}}) + "\n")
        codex = self.copy_codex(); self.cli("index")
        # Seed a matching legacy-style chunk with no entities: the exact UUID
        # prefix entity in the Codex transcript must outrank this newer match.
        conn = engine.connect(self.db)
        session_id = conn.execute("select s.id from sessions s join files f on f.id=s.file_id where f.path like '%generic.jsonl'").fetchone()[0]
        chunk_id = conn.execute("insert into chunks(session_id,ts,surface,text) values (?,?,?,?)", (session_id, 1782864000, "assistant", "legacy mention 12345678")).lastrowid
        conn.execute("insert into chunks_fts(rowid,text) values (?,?)", (chunk_id, "legacy mention 12345678"))
        conn.commit(); conn.close()
        result = self.cli("search", "12345678", "--paths")
        self.assertEqual(Path(result.splitlines()[0]), codex)
        self.assertIn("rollout-fixture", self.cli("search", "found", "--harness", "codex"))
        self.assertNotIn("rollout-fixture", self.cli("search", "found", "--since", "2026-03-01T00:00:00Z"))
        self.assertIn("old.jsonl", self.cli("search", "PAR-123", "--harness", "claude"))
        self.assertTrue(c.exists())

    def test_related_show_and_doctor(self):
        self.write_claude("one.jsonl")
        self.write_claude("two.jsonl")
        self.cli("index")
        related = self.cli("related", "--cwd", "/work/grep123/project")
        self.assertIn("one.jsonl", related)
        shown = self.cli("show", str(self.claude / "one.jsonl"), "--prompts")
        self.assertIn("Please inspect", shown)
        doctor = self.cli("doctor")
        self.assertIn("OK FTS5 available", doctor)
        self.assertIn("mode=local", doctor)

    def test_doctor_missing_db_is_read_only(self):
        self.assertFalse(self.db.exists())
        doctor = self.cli("doctor")
        self.assertIn("WARN db exists=False", doctor)
        self.assertFalse(self.db.exists())

    def test_search_without_index_suggests_raw_transcript_scan(self):
        self.assertFalse(self.db.exists())
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = engine.main(["search", "anything"])
        self.assertEqual(code, 0)
        self.assertEqual(out.getvalue(), "")
        self.assertIn("scanning transcripts directly", err.getvalue())
        self.assertIn("rg -l -i", err.getvalue())


class RemoteHandler(BaseHTTPRequestHandler):
    requests: list[dict] = []
    target_path = "/source/session.jsonl"
    fail_search = False

    def log_message(self, *_args):
        pass

    def send_json(self, status: int, body: dict) -> None:
        rendered = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(rendered)))
        self.end_headers()
        self.wfile.write(rendered)

    def do_GET(self):
        type(self).requests.append({"method": "GET", "path": self.path, "authorization": self.headers.get("Authorization")})
        if self.path == "/v1/doctor":
            self.send_json(200, {"status": "ok", "source_events": 12, "projection_lag": 0})
        else:
            self.send_json(404, {"error": "not found"})

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        type(self).requests.append({"method": "POST", "path": self.path, "body": body, "authorization": self.headers.get("Authorization"), "idempotency_key": self.headers.get("Idempotency-Key")})
        if self.path == "/v1/search":
            if type(self).fail_search:
                self.send_json(503, {"error": "unavailable"})
                return
            self.send_json(200, {"results": [{
                "path": type(self).target_path,
                "occurred_at": "2026-01-01T00:00:00Z",
                "cwd": "/work/grep123/project",
                "slot": "grep123",
                "branch": "feature/remote",
                "surface": "tool_output",
                "text": "remote exact deadbeef evidence",
                "matched_terms": ["deadbeef"],
                "legs": ["exact"],
                "tier": 2,
                "evidence": {
                    "evidence_class": "identifier", "class_priority": 4,
                    "origin_priority": 0, "matched_count": 1,
                    "informative_count": 1, "coverage": 1.0,
                    "lexical_score": 1.0, "rank_key": [4, 0, 1.0, 1.0],
                },
                "receipt": "recall://claude:linux/session:1?rev=1#item=0"
            }], "abstention_reason": None, "diagnostics": {
                "deadline_ms": 300, "elapsed_ms": 12.5, "deadline_exceeded": False,
                "legs": [{"leg": "exact", "elapsed_ms": 10.0, "n_results": 1, "timed_out": False}],
            }})
        elif self.path == "/v1/show":
            self.send_json(200, {"chunks": [{
                "occurred_at": "2026-01-01T00:00:00Z", "surface": "user",
                "text": "remote prompt", "receipt": "recall://claude:linux/session:1?rev=1#item=0"
            }]})
        elif self.path == "/v1/session-export":
            self.send_json(200, {
                "schema_version": "recall.session-export.v1",
                "session": {
                    "source_id": "claude:linux:test", "native_session_id": "claude-session-test",
                    "harness": "claude", "boundary_receipt": "boundary-test",
                    "projector_version": 1, "privacy_policy_version": "privacy-v1",
                },
                "items": [{
                    "sequence": 0, "evidence_id": "rse_test", "event_native_id": "event-1",
                    "item_ordinal": 0, "surface": "user", "text": "remote prompt",
                    "text_sha256": hashlib.sha256(b"remote prompt").hexdigest(),
                    "receipt": "recall://claude:linux:test/event-1?rev=1#item=0",
                }],
                "page": {"count": 1, "complete": True, "next_cursor": None, "page_receipt": "page-test"},
            })
        elif self.path == "/v1/related":
            self.send_json(200, {"results": [{
                "path": type(self).target_path, "overlap": 3,
                "cwd": "/work/grep123/project", "branch": "feature/remote",
                "receipt": "recall://claude:linux/session:1?rev=1#item=0"
            }]})
        elif self.path == "/v1/ingest/batches":
            event = body["events"][0]
            revision = 2 if event["kind"] == "tombstone" else 1
            self.send_json(201, {
                "status": "committed", "inserted": 1, "duplicate_events": 0,
                "receipts": [f"recall://{event['source_id']}/{event['native_id']}?rev={revision}"],
            })
        else:
            self.send_json(404, {"error": "not found"})


class RedirectOnlyHandler(BaseHTTPRequestHandler):
    destination = ""

    def log_message(self, *_args):
        pass

    def do_GET(self):
        self.send_response(302)
        self.send_header("Location", type(self).destination)
        self.end_headers()


class McpRemoteHandler(BaseHTTPRequestHandler):
    requests: list[dict] = []
    target_path = "/source/mcp-session.jsonl"
    fail_tool = False
    omit_search_path = False
    passage_search = False

    def log_message(self, *_args):
        pass

    def send_json(self, status: int, body: dict) -> None:
        rendered = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(rendered)))
        self.end_headers()
        self.wfile.write(rendered)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        type(self).requests.append({
            "method": "POST",
            "path": self.path,
            "body": body,
            "authorization": self.headers.get("Authorization"),
            "accept": self.headers.get("Accept"),
            "protocol": self.headers.get("MCP-Protocol-Version"),
            "idempotency_key": self.headers.get("Idempotency-Key"),
        })
        if self.path != "/mcp":
            self.send_json(404, {"error": "not found"})
            return
        if type(self).fail_tool:
            self.send_json(200, {
                "jsonrpc": "2.0", "id": body.get("id"),
                "error": {"code": -32603, "message": "synthetic tool failure"},
            })
            return
        if body["method"] == "ping":
            value = {}
        else:
            self.assert_tool_call(body)
            name = body["params"]["name"]
            arguments = body["params"]["arguments"]
            if name == "recall_search":
                receipt = "recall://claude:linux/mcp-session:1?rev=1#item=0"
                if type(self).passage_search:
                    result = {
                        "logical_document_id": "ldoc_" + "1" * 32,
                        "source_id": "claude:linux",
                        "last_occurred_at": "2026-01-01T00:00:00Z",
                        "reasons": ["dense", "passage-lexical"],
                        "matching_ranges": [{
                            "kind": "dense",
                            "receipts": [receipt],
                            "text": "remote MCP passage evidence",
                        }],
                    }
                else:
                    result = {
                        "occurred_at": "2026-01-01T00:00:00Z",
                        "cwd": "/work/grep123/project",
                        "slot": "grep123",
                        "branch": "feature/mcp",
                        "surface": "tool_output",
                        "text": "remote MCP exact deadbeef evidence",
                        "matched_terms": ["deadbeef"],
                        "legs": ["exact"],
                        "receipt": receipt,
                    }
                    if not type(self).omit_search_path:
                        result["path"] = type(self).target_path
                value = {
                    "results": [result],
                    "diagnostics": {"deadline_ms": 300, "elapsed_ms": 11.0},
                }
            elif name == "recall_show":
                value = {"chunks": [{
                    "occurred_at": "2026-01-01T00:00:00Z",
                    "surface": "user",
                    "text": "remote MCP prompt",
                    "receipt": arguments["target"],
                }]}
            elif name == "recall_related":
                value = {"results": [{
                    "path": type(self).target_path,
                    "overlap": 2,
                    "cwd": arguments.get("cwd"),
                    "branch": arguments.get("branch"),
                    "receipt": "recall://claude:linux/mcp-session:1?rev=1#item=0",
                }]}
            elif name == "recall_capture":
                value = {
                    "status": "committed",
                    "receipt": "recall://memory:mcp/capture-1?rev=1",
                    "duplicate": False,
                }
            elif name == "recall_forget":
                value = {
                    "status": "forgotten",
                    "receipt": arguments["receipt"].split("#", 1)[0],
                }
            else:
                raise AssertionError(name)
        self.send_json(200, {
            "jsonrpc": "2.0", "id": body["id"],
            "result": (
                value if body["method"] == "ping" else {
                    "content": [{"type": "text", "text": json.dumps(value)}],
                    "structuredContent": value,
                    "isError": False,
                }
            ),
        })

    @staticmethod
    def assert_tool_call(body):
        if (
            body.get("jsonrpc") != "2.0"
            or body.get("method") != "tools/call"
            or set(body.get("params", {})) != {"name", "arguments"}
        ):
            raise AssertionError(body)


class RemoteTransportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.claude = self.root / "claude"; self.claude.mkdir()
        self.codex = self.root / "codex"; self.codex.mkdir()
        self.db = self.root / "state/index.db"
        self.shadow = self.root / "shadow.jsonl"
        self.old_env = {key: os.environ.get(key) for key in (
            "RECALL_CLAUDE_ROOT", "RECALL_CODEX_ROOT", "RECALL_DB", "RECALL_URL",
            "RECALL_MODE", "RECALL_TOKEN_FILE", "RECALL_SHADOW_LOG", "RECALL_REMOTE_TRACE",
            "RECALL_CONFIG_FILE",
        )}
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), RemoteHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
        RemoteHandler.requests = []; RemoteHandler.fail_search = False
        RemoteHandler.target_path = str(self.claude / "session.jsonl")
        os.environ.update(
            RECALL_CLAUDE_ROOT=str(self.claude), RECALL_CODEX_ROOT=str(self.codex), RECALL_DB=str(self.db),
            RECALL_URL=f"http://127.0.0.1:{self.server.server_port}", RECALL_SHADOW_LOG=str(self.shadow),
            RECALL_CONFIG_FILE=str(self.root / "absent-client.json"),
        )

    def tearDown(self):
        self.server.shutdown(); self.server.server_close()
        for key, value in self.old_env.items():
            if value is None: os.environ.pop(key, None)
            else: os.environ[key] = value
        self.tmp.cleanup()

    def call(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = engine.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def seed_local(self):
        (self.claude / "session.jsonl").write_text(json.dumps({
            "type": "user", "timestamp": "2026-01-01T00:00:00Z",
            "cwd": "/work/grep123/project", "message": {"content": "local deadbeef evidence"},
        }) + "\n")
        old_mode = os.environ.pop("RECALL_MODE", None)
        old_url = os.environ.pop("RECALL_URL", None)
        try:
            self.assertEqual(self.call("index")[0], 0)
        finally:
            if old_mode is not None: os.environ["RECALL_MODE"] = old_mode
            if old_url is not None: os.environ["RECALL_URL"] = old_url

    def test_url_selects_remote_search_and_preserves_filters(self):
        code, out, err = self.call(
            "search", "deadbeef evidence", "--since", "2026-01-01", "--until", "2026-02-01",
            "--cwd", "grep123", "--branch", "feature", "--harness", "claude", "--limit", "7",
            "--source-id", "cowork:mac:owner", "--source-family", "coding_history",
            "--source-alias", "cowork",
        )
        self.assertEqual((code, err), (0, ""))
        self.assertIn(RemoteHandler.target_path, out)
        self.assertIn("WHY: terms=deadbeef; legs=exact", out)
        request = RemoteHandler.requests[-1]
        self.assertEqual(request["path"], "/v1/search")
        self.assertEqual(request["body"]["filters"], {
            "since": "2026-01-01", "until": "2026-02-01", "cwd": "grep123",
            "branch": "feature", "harness": "claude",
            "source_id": "cowork:mac:owner", "source_family": "coding_history",
            "source_alias": "cowork",
        })
        self.assertEqual(request["body"]["limit"], 7)

    def test_explicit_mcp_url_uses_tools_call_for_read_surface(self):
        mcp = ThreadingHTTPServer(("127.0.0.1", 0), McpRemoteHandler)
        thread = threading.Thread(target=mcp.serve_forever, daemon=True)
        thread.start()
        McpRemoteHandler.requests = []
        os.environ["RECALL_URL"] = f"http://127.0.0.1:{mcp.server_port}/mcp"
        try:
            code, output, error = self.call(
                "search", "deadbeef", "--limit", "5",
                "--since", "2026-01-01", "--until", "2026-02-01",
                "--source-family", "coding_history",
            )
            self.assertEqual((code, error), (0, ""))
            self.assertIn(McpRemoteHandler.target_path, output)
            request = McpRemoteHandler.requests[-1]
            self.assertEqual(request["path"], "/mcp")
            self.assertEqual(request["protocol"], "2025-11-25")
            self.assertEqual(request["accept"], "application/json, text/event-stream")
            self.assertIsNone(request["idempotency_key"])
            self.assertEqual(request["body"]["method"], "tools/call")
            self.assertEqual(request["body"]["params"], {
                "name": "recall_search",
                "arguments": {
                    "query": "deadbeef",
                    "filters": {
                        "since": "2026-01-01T00:00:00Z",
                        "until": "2026-02-01T00:00:00Z",
                        "source_family": "coding_history",
                    },
                    "limit": 5,
                },
            })

            target = "recall://claude:linux/mcp-session:1?rev=1#item=0"
            self.assertIn("remote MCP prompt", self.call("show", target, "--prompts")[1])
            self.assertEqual(
                McpRemoteHandler.requests[-1]["body"]["params"]["arguments"],
                {"target": target, "prompts": True, "tail": 0},
            )
            self.assertIn(
                "overlap=2",
                self.call("related", "--cwd", "/work/grep123/project", "--branch", "feature/mcp")[1],
            )
            self.assertIn(
                "overlap=2",
                self.call("related", "--cwd", "/work/grep123/project")[1],
            )
            self.assertEqual(
                McpRemoteHandler.requests[-1]["body"]["params"]["arguments"],
                {
                    "cwd": "/work/grep123/project",
                    "limit": 10,
                    "mains_only": False,
                    "fast": False,
                },
            )
            self.assertIn("OK remote", self.call("doctor")[1])
            self.assertEqual(McpRemoteHandler.requests[-1]["body"]["method"], "ping")
        finally:
            mcp.shutdown()
            mcp.server_close()

    def test_mcp_search_uses_receipt_when_managed_result_has_no_local_path(self):
        mcp = ThreadingHTTPServer(("127.0.0.1", 0), McpRemoteHandler)
        thread = threading.Thread(target=mcp.serve_forever, daemon=True)
        thread.start()
        McpRemoteHandler.requests = []
        McpRemoteHandler.omit_search_path = True
        os.environ["RECALL_URL"] = f"http://127.0.0.1:{mcp.server_port}/mcp"
        receipt = "recall://claude:linux/mcp-session:1?rev=1#item=0"
        try:
            code, output, error = self.call("search", "deadbeef")
            self.assertEqual((code, error), (0, ""))
            self.assertIn(f"1. {receipt}", output)
            self.assertEqual(self.call("search", "deadbeef", "--paths")[1].strip(), receipt)
        finally:
            McpRemoteHandler.omit_search_path = False
            mcp.shutdown()
            mcp.server_close()

    def test_mcp_search_resolves_lossless_passage_receipts(self):
        mcp = ThreadingHTTPServer(("127.0.0.1", 0), McpRemoteHandler)
        thread = threading.Thread(target=mcp.serve_forever, daemon=True)
        thread.start()
        McpRemoteHandler.requests = []
        McpRemoteHandler.passage_search = True
        os.environ["RECALL_URL"] = f"http://127.0.0.1:{mcp.server_port}/mcp"
        receipt = "recall://claude:linux/mcp-session:1?rev=1#item=0"
        try:
            code, output, error = self.call("search", "passage evidence")
            self.assertEqual((code, error), (0, ""))
            self.assertIn(f"1. {receipt}", output)
            self.assertIn("remote MCP passage evidence", output)
            self.assertIn("legs=dense,passage-lexical", output)
            self.assertEqual(
                self.call("search", "passage evidence", "--paths")[1].strip(),
                receipt,
            )
        finally:
            McpRemoteHandler.passage_search = False
            mcp.shutdown()
            mcp.server_close()

    def test_explicit_mcp_url_maps_deliberate_write_and_fails_closed_for_export(self):
        mcp = ThreadingHTTPServer(("127.0.0.1", 0), McpRemoteHandler)
        thread = threading.Thread(target=mcp.serve_forever, daemon=True)
        thread.start()
        McpRemoteHandler.requests = []
        os.environ["RECALL_URL"] = f"http://127.0.0.1:{mcp.server_port}/mcp"
        os.environ["RECALL_WRITE_SOURCE_ID"] = "memory:mac:test"
        self.addCleanup(os.environ.pop, "RECALL_WRITE_SOURCE_ID", None)
        try:
            code, output, error = self.call(
                "put", "A deliberate MCP memory\nwith bounded body",
                "--provenance-uri", "manual://unit-test",
            )
            self.assertEqual((code, error), (0, ""))
            captured = json.loads(output)
            self.assertEqual(captured["receipt"], "recall://memory:mcp/capture-1?rev=1")
            request = McpRemoteHandler.requests[-1]
            self.assertEqual(request["body"]["params"]["name"], "recall_capture")
            self.assertEqual(request["body"]["params"]["arguments"]["title"], "A deliberate MCP memory")
            self.assertEqual(
                request["body"]["params"]["arguments"]["body"],
                "A deliberate MCP memory\nwith bounded body",
            )
            self.assertNotIn("source_id", request["body"]["params"]["arguments"])

            code, output, error = self.call(
                "delete", captured["receipt"], "--source-id", "memory:mac:test",
            )
            self.assertEqual((code, error), (0, ""))
            self.assertEqual(
                McpRemoteHandler.requests[-1]["body"]["params"],
                {"name": "recall_forget", "arguments": {"receipt": captured["receipt"]}},
            )

            before = len(McpRemoteHandler.requests)
            code, output, error = self.call(
                "session-export", "--target", McpRemoteHandler.target_path,
            )
            self.assertNotEqual(code, 0)
            self.assertEqual(output, "")
            self.assertIn("not available over MCP", error)
            self.assertEqual(len(McpRemoteHandler.requests), before)
        finally:
            mcp.shutdown()
            mcp.server_close()

    def test_mcp_error_is_bounded_and_does_not_echo_request_or_token(self):
        mcp = ThreadingHTTPServer(("127.0.0.1", 0), McpRemoteHandler)
        thread = threading.Thread(target=mcp.serve_forever, daemon=True)
        thread.start()
        McpRemoteHandler.requests = []
        McpRemoteHandler.fail_tool = True
        token_file = self.root / "mcp-token.json"
        token_file.write_text(json.dumps({"token": "mcp-synthetic-secret-token"}))
        token_file.chmod(0o600)
        os.environ["RECALL_TOKEN_FILE"] = str(token_file)
        os.environ["RECALL_URL"] = f"http://127.0.0.1:{mcp.server_port}/mcp"
        canary = "private-query-canary-must-not-echo"
        try:
            code, output, error = self.call("search", canary)
            self.assertNotEqual(code, 0)
            self.assertEqual(output, "")
            self.assertIn("MCP -32603", error)
            self.assertNotIn(canary, error)
            self.assertNotIn("mcp-synthetic-secret-token", error)
        finally:
            McpRemoteHandler.fail_tool = False
            mcp.shutdown()
            mcp.server_close()

    def test_private_client_config_accepts_explicit_mcp_endpoint(self):
        token_file = self.root / "mcp-read-token.json"
        token_file.write_text(json.dumps({"token": "configured-mcp-read-token"}))
        token_file.chmod(0o600)
        config = self.root / "mcp-client.json"
        config.write_text(json.dumps({
            "schema_version": 1,
            "url": f"http://127.0.0.1:{self.server.server_port}/mcp",
            "token_file": str(token_file),
        }))
        config.chmod(0o600)
        os.environ.pop("RECALL_URL", None)
        os.environ.pop("RECALL_TOKEN_FILE", None)
        os.environ["RECALL_CONFIG_FILE"] = str(config)
        loaded = engine.load_client_config()
        self.assertEqual(loaded["url"], f"http://127.0.0.1:{self.server.server_port}/mcp")
        self.assertEqual(loaded["token_file"], str(token_file))

    def test_remote_paths_show_related_and_doctor_keep_cli_surface(self):
        remote_trace = self.root / "remote-trace.jsonl"
        os.environ["RECALL_REMOTE_TRACE"] = str(remote_trace)
        self.assertEqual(self.call("search", "deadbeef", "--paths")[1].strip(), RemoteHandler.target_path)
        trace = json.loads(remote_trace.read_text())
        self.assertEqual(trace["remote_results"], [{
            "path": RemoteHandler.target_path,
            "receipt": "recall://claude:linux/session:1?rev=1#item=0",
            "legs": ["exact"],
            "evidence": {
                "evidence_class": "identifier", "class_priority": 4,
                "origin_priority": 0, "matched_count": 1,
                "informative_count": 1, "coverage": 1.0,
                "lexical_score": 1.0, "rank_key": [4, 0, 1.0, 1.0],
            },
        }])
        self.assertNotIn("deadbeef", json.dumps(trace))
        self.assertEqual(remote_trace.stat().st_mode & 0o777, 0o600)
        shown = self.call("show", RemoteHandler.target_path, "--prompts", "--tail", "5")[1]
        self.assertIn("user: remote prompt", shown)
        related = self.call("related", "--cwd", "/work/grep123/project", "--branch", "feature/remote")[1]
        self.assertIn("overlap=3", related)
        doctor = self.call("doctor")[1]
        self.assertIn("OK remote", doctor)
        self.assertIn("projection_lag=0", doctor)

    def test_remote_session_export_preserves_machine_contract(self):
        code, output, error = self.call(
            "session-export", "--target", RemoteHandler.target_path, "--limit", "500",
        )
        self.assertEqual((code, error), (0, ""))
        page = json.loads(output)
        self.assertEqual(page["items"][0]["evidence_id"], "rse_test")
        request = RemoteHandler.requests[-1]
        self.assertEqual(request["path"], "/v1/session-export")
        self.assertEqual(request["body"], {"target": RemoteHandler.target_path, "limit": 500})

    def test_explicit_local_mode_is_config_only_rollback(self):
        self.seed_local()
        before = hashlib.sha256(self.db.read_bytes()).hexdigest()
        RemoteHandler.requests = []
        os.environ["RECALL_MODE"] = "local"
        code, out, _ = self.call("search", "deadbeef", "--paths")
        self.assertEqual(code, 0)
        self.assertEqual(Path(out.strip()), self.claude / "session.jsonl")
        self.assertEqual(RemoteHandler.requests, [])
        self.assertEqual(hashlib.sha256(self.db.read_bytes()).hexdigest(), before)

    def test_central_profile_blocks_accidental_local_rebuild_even_in_local_mode(self):
        os.environ["RECALL_MODE"] = "local"
        code, out, error = self.call("index", "--rebuild")
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn("local index disabled", error)
        self.assertIn("cannot repair or refresh the central brain", error)
        self.assertFalse(self.db.exists())
        self.assertEqual(RemoteHandler.requests, [])

    def test_central_profile_allows_explicit_local_index_maintenance(self):
        os.environ["RECALL_MODE"] = "local"
        code, _out, error = self.call("index", "--allow-local-index")
        self.assertEqual((code, error), (0, ""))
        self.assertTrue(self.db.exists())
        self.assertEqual(RemoteHandler.requests, [])

    def test_remote_failure_does_not_silently_fallback(self):
        self.seed_local(); RemoteHandler.fail_search = True
        code, out, err = self.call("search", "deadbeef", "--paths")
        self.assertNotEqual(code, 0)
        self.assertEqual(out, "")
        self.assertIn("remote recall unavailable", err)

    def test_remote_redirect_never_forwards_bearer_to_destination(self):
        token_file = self.root / "redirect-token.json"
        token_file.write_text(json.dumps({"token": "redirect-secret-token"}))
        token_file.chmod(0o600)
        os.environ["RECALL_TOKEN_FILE"] = str(token_file)
        redirect = ThreadingHTTPServer(("127.0.0.1", 0), RedirectOnlyHandler)
        RedirectOnlyHandler.destination = f"http://127.0.0.1:{self.server.server_port}/v1/doctor"
        thread = threading.Thread(target=redirect.serve_forever, daemon=True)
        thread.start()
        RemoteHandler.requests = []
        os.environ["RECALL_URL"] = f"http://127.0.0.1:{redirect.server_port}"
        try:
            code, output, error = self.call("doctor")
            self.assertNotEqual(code, 0)
            self.assertEqual(output, "")
            self.assertIn("HTTP 302", error)
            self.assertEqual(RemoteHandler.requests, [])
        finally:
            redirect.shutdown()
            redirect.server_close()

    def test_environment_url_is_validated_before_authority_or_network(self):
        os.environ["RECALL_URL"] = "file:///tmp/not-http"
        with mock.patch.object(engine, "_open_remote") as opened:
            code, output, error = self.call("doctor")
        self.assertNotEqual(code, 0)
        self.assertEqual(output, "")
        self.assertIn("remote URL is invalid", error)
        opened.assert_not_called()

    def test_shadow_returns_local_and_records_receipt_level_comparison(self):
        self.seed_local(); os.environ["RECALL_MODE"] = "shadow"
        RemoteHandler.target_path = str(self.claude / "remote-other.jsonl")
        code, out, err = self.call("search", "deadbeef", "--paths")
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(Path(out.strip()), self.claude / "session.jsonl")
        entry = json.loads(self.shadow.read_text().splitlines()[-1])
        self.assertEqual(entry["command"], "search")
        self.assertEqual(entry["local_paths"], [str(self.claude / "session.jsonl")])
        self.assertEqual(entry["remote_results"][0]["receipt"], "recall://claude:linux/session:1?rev=1#item=0")
        self.assertEqual(entry["remote_diagnostics"]["deadline_ms"], 300)
        self.assertNotIn("deadbeef", json.dumps(entry["remote_diagnostics"]))
        self.assertTrue(entry["diverged"])

    def test_shadow_log_does_not_chmod_an_existing_shared_parent(self):
        self.seed_local(); os.environ["RECALL_MODE"] = "shadow"
        shared_parent = Path(tempfile.gettempdir())
        before_mode = shared_parent.stat().st_mode & 0o7777
        shared_log = shared_parent / f"recall-shadow-{os.getpid()}.jsonl"
        shared_log.unlink(missing_ok=True)
        os.environ["RECALL_SHADOW_LOG"] = str(shared_log)
        try:
            code, _, err = self.call("search", "deadbeef", "--paths")
            self.assertEqual((code, err), (0, ""))
            self.assertTrue(shared_log.is_file())
            self.assertEqual(shared_parent.stat().st_mode & 0o7777, before_mode)
        finally:
            shared_log.unlink(missing_ok=True)

    def test_token_file_must_be_private_and_is_sent_as_bearer(self):
        token_file = self.root / "token.json"
        token_file.write_text(json.dumps({"token": "scoped-test-token"})); token_file.chmod(0o644)
        os.environ["RECALL_TOKEN_FILE"] = str(token_file)
        code, _, err = self.call("doctor")
        self.assertNotEqual(code, 0); self.assertIn("0600", err)
        token_file.chmod(0o600)
        code, _, err = self.call("doctor")
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(RemoteHandler.requests[-1]["authorization"], "Bearer scoped-test-token")

    def test_token_file_rejects_symlinks_and_oversized_content_without_network(self):
        target = self.root / "actual-token.json"
        target.write_text(json.dumps({"token": "private-synthetic-token"}))
        target.chmod(0o600)
        linked = self.root / "linked-token.json"
        linked.symlink_to(target)
        os.environ["RECALL_TOKEN_FILE"] = str(linked)
        with mock.patch.object(engine, "_open_remote") as opened:
            code, output, error = self.call("doctor")
        self.assertNotEqual(code, 0)
        self.assertEqual(output, "")
        self.assertIn("regular file", error)
        opened.assert_not_called()

        oversized = self.root / "oversized-token.json"
        oversized.write_bytes(b"{" + b" " * engine.MAX_TOKEN_FILE_BYTES + b"}")
        oversized.chmod(0o600)
        os.environ["RECALL_TOKEN_FILE"] = str(oversized)
        with mock.patch.object(engine, "_open_remote") as opened:
            code, output, error = self.call("doctor")
        self.assertNotEqual(code, 0)
        self.assertEqual(output, "")
        self.assertIn("too large", error)
        opened.assert_not_called()

    def test_private_default_client_config_enables_remote_and_env_overrides_it(self):
        token_file = self.root / "read-token.json"
        token_file.write_text(json.dumps({"token": "configured-read-token"}))
        token_file.chmod(0o600)
        config = self.root / "client.json"
        config.write_text(json.dumps({
            "schema_version": 1,
            "url": f"http://127.0.0.1:{self.server.server_port}",
            "token_file": str(token_file),
        }))
        config.chmod(0o600)
        os.environ.pop("RECALL_URL", None)
        os.environ.pop("RECALL_TOKEN_FILE", None)
        os.environ["RECALL_CONFIG_FILE"] = str(config)

        code, output, error = self.call("doctor")

        self.assertEqual((code, error), (0, ""))
        self.assertIn("OK remote", output)
        self.assertEqual(RemoteHandler.requests[-1]["authorization"], "Bearer configured-read-token")

        override = self.root / "override-token.json"
        override.write_text(json.dumps({"token": "environment-read-token"}))
        override.chmod(0o600)
        os.environ["RECALL_URL"] = f"http://127.0.0.1:{self.server.server_port}"
        os.environ["RECALL_TOKEN_FILE"] = str(override)
        self.assertEqual(self.call("doctor")[0], 0)
        self.assertEqual(RemoteHandler.requests[-1]["authorization"], "Bearer environment-read-token")

    def test_client_config_rejects_open_mode_symlink_and_unknown_fields_without_path_echo(self):
        token_file = self.root / "read-token.json"
        token_file.write_text(json.dumps({"token": "configured-read-token"}))
        token_file.chmod(0o600)
        config = self.root / "private-config-canary.json"
        config.write_text(json.dumps({
            "schema_version": 1,
            "url": f"http://127.0.0.1:{self.server.server_port}",
            "token_file": str(token_file),
            "secret_extra": "private-config-content-canary",
        }))
        config.chmod(0o644)
        os.environ.pop("RECALL_URL", None)
        os.environ.pop("RECALL_TOKEN_FILE", None)
        os.environ["RECALL_CONFIG_FILE"] = str(config)

        code, output, error = self.call("doctor")

        self.assertNotEqual(code, 0)
        self.assertEqual(output, "")
        self.assertNotIn(str(config), error)
        self.assertNotIn("private-config-content-canary", error)

        config.chmod(0o600)
        target = self.root / "target.json"
        config.rename(target)
        config.symlink_to(target)
        code, output, error = self.call("doctor")
        self.assertNotEqual(code, 0)
        self.assertEqual(output, "")
        self.assertNotIn(str(target), error)

    def test_explicit_memory_put_and_delete_are_remote_scoped_and_receipted(self):
        os.environ["RECALL_WRITE_SOURCE_ID"] = "memory:mac:test"
        self.addCleanup(os.environ.pop, "RECALL_WRITE_SOURCE_ID", None)
        code, output, error = self.call(
            "put", "remember the c5 exact marker", "--visibility", "private",
            "--provenance-uri", "manual://unit-test",
        )
        self.assertEqual((code, error), (0, ""))
        put = json.loads(output)
        self.assertEqual(put["kind"], "memory")
        request = RemoteHandler.requests[-1]
        event = request["body"]["events"][0]
        self.assertEqual(event["content"], {"text": "remember the c5 exact marker"})
        self.assertEqual(event["source_id"], "memory:mac:test")
        self.assertEqual(event["visibility"], "private")
        self.assertTrue(request["idempotency_key"].startswith("recall-skill-v1-"))

        code, output, error = self.call("delete", put["receipt"], "--source-id", "memory:mac:test")
        self.assertEqual((code, error), (0, ""))
        deleted = json.loads(output)
        self.assertEqual(deleted["kind"], "tombstone")
        tombstone = RemoteHandler.requests[-1]["body"]["events"][0]
        self.assertEqual(tombstone["native_id"], put["native_id"])
        self.assertEqual(tombstone["content"]["target_native_id"], put["native_id"])


if __name__ == "__main__":
    unittest.main()
