"""CLI requests must satisfy the published canonical MCP contract."""

from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))

from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server.mcp import CANONICAL_SHOW_TOOL, READ_TOOLS
from tests.test_engine import engine


class SchemaMcp:
    def __init__(self, *, canonical=True):
        self.tools = {tool["name"]: tool for tool in READ_TOOLS}
        if canonical:
            self.tools["recall_show"] = CANONICAL_SHOW_TOOL
        self.requests = []
        self.view = BoundCanonicalRetrieval(
            None,
            tenant_id="tenant:synthetic",
            principal_id="owner",
            authorized_sources=("claude:linux:allowed", "codex:linux:allowed"),
        )

    def _closed(self, value, schema):
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            if set(value) - set(properties):
                raise AssertionError("request violates published MCP properties")
        if set(schema.get("required", ())) - set(value):
            raise AssertionError("request omits required MCP properties")
        for key, item in value.items():
            if isinstance(item, dict) and key in properties:
                self._closed(item, properties[key])

    def open(self, request, *, timeout):
        message = json.loads(request.data)
        self.requests.append(message)
        if message["method"] == "tools/list":
            result = {"tools": list(self.tools.values())}
        else:
            assert message["method"] == "tools/call"
            name, arguments = message["params"]["name"], message["params"]["arguments"]
            self._closed(arguments, self.tools[name]["inputSchema"])
            if name == "recall_search":
                source, family, alias, connector, _since, _until = self.view._filters(
                    arguments.get("filters", {})
                )
                selected = self.view._sources(
                    source_id=source,
                    source_family=family,
                    source_alias=alias,
                    source_connector=connector,
                )
                value = {"results": [{"source_id": source} for source in selected]}
            else:
                assert name == "recall_show"
                value = {"chunks": [{"text": "synthetic evidence", "surface": "user"}]}
            result = {"structuredContent": value, "isError": False}
        return io.BytesIO(
            json.dumps(
                {"jsonrpc": "2.0", "id": message["id"], "result": result}
            ).encode()
        )


class EngineMcpContractTests(unittest.TestCase):
    def setUp(self):
        self.headers = mock.patch.object(engine, "remote_headers", return_value={})
        self.headers.start()
        self.addCleanup(self.headers.stop)

    def call(self, server, path, body):
        with mock.patch.object(engine, "_open_remote", side_effect=server.open):
            return engine._mcp_call("http://127.0.0.1/mcp", "POST", path, body)

    def test_harness_maps_to_connector_and_intersects_existing_source_scope(self):
        for harness in ("claude", "codex"):
            for selected in (
                "claude:linux:allowed",
                "codex:linux:allowed",
                "claude:linux:denied",
            ):
                with self.subTest(harness=harness, selected=selected):
                    server = SchemaMcp()
                    result = self.call(
                        server,
                        "/v1/search",
                        {
                            "query": "synthetic question",
                            "filters": {
                                "harness": harness,
                                "source_id": selected,
                                "since": "2026-09-01",
                            },
                            "limit": 1,
                        },
                    )
                    arguments = server.requests[-1]["params"]["arguments"]
                    self.assertEqual(
                        arguments["filters"],
                        {
                            "source_connector": harness,
                            "source_id": selected,
                            "since": "2026-09-01T00:00:00Z",
                        },
                    )
                    expected = (
                        [{"source_id": selected}]
                        if selected.startswith(harness + ":")
                        and selected.endswith(":allowed")
                        else []
                    )
                    self.assertEqual(result["results"], expected)
                    self.assertEqual(len(server.requests), 1)

    def test_unsupported_worktree_filters_fail_before_network(self):
        for name in ("cwd", "branch"):
            server = SchemaMcp()
            with self.assertRaisesRegex(engine.RemoteRecallError, "--" + name):
                self.call(
                    server,
                    "/v1/search",
                    {"query": "synthetic", "filters": {name: "private constraint"}},
                )
            self.assertEqual(server.requests, [])

    def test_default_show_sends_only_target_without_discovery(self):
        server = SchemaMcp()
        target = "recall://claude:linux:allowed/event?rev=1#item=0"
        result = self.call(
            server,
            "/v1/show",
            {
                "target": target,
                "around": None,
                "tail": 0,
                "prompts": False,
            },
        )
        self.assertEqual(result["chunks"][0]["text"], "synthetic evidence")
        self.assertEqual(len(server.requests), 1)
        self.assertEqual(server.requests[0]["params"]["arguments"], {"target": target})

    def test_canonical_show_rejects_explicit_windows_after_schema_discovery(self):
        for modifier in (
            {"tail": 5},
            {"around": "2026-09-17T12:00:00Z"},
            {"prompts": True},
        ):
            server = SchemaMcp()
            with self.assertRaisesRegex(
                engine.RemoteRecallError, "recall_session_context|recall_exec"
            ):
                self.call(
                    server,
                    "/v1/show",
                    {"target": "recall://synthetic/event?rev=1", **modifier},
                )
            self.assertEqual([r["method"] for r in server.requests], ["tools/list"])

    def test_advertised_legacy_show_modifiers_are_preserved(self):
        for modifier in (
            {"tail": 5},
            {"around": "2026-09-17T12:00:00Z"},
            {"prompts": True},
        ):
            server = SchemaMcp(canonical=False)
            target = "recall://synthetic/event?rev=1"
            result = self.call(server, "/v1/show", {"target": target, **modifier})
            self.assertEqual(result["chunks"][0]["text"], "synthetic evidence")
            self.assertEqual(
                [r["method"] for r in server.requests], ["tools/list", "tools/call"]
            )
            self.assertEqual(
                server.requests[-1]["params"]["arguments"],
                {"target": target, **modifier},
            )

    def test_failed_or_incomplete_discovery_does_not_send_show(self):
        for tools in ({}, {"recall_show": {"name": "recall_show"}}):
            server = SchemaMcp()
            server.tools = tools
            with self.assertRaises(engine.RemoteRecallError):
                self.call(
                    server,
                    "/v1/show",
                    {"target": "recall://synthetic/event?rev=1", "tail": 5},
                )
            self.assertEqual([r["method"] for r in server.requests], ["tools/list"])


if __name__ == "__main__":
    unittest.main()
