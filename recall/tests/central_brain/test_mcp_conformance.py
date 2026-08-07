from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.mcp import (  # noqa: E402
    READ_TOOLS,
    SUPPORTED_PROTOCOL_VERSIONS,
    WRITE_TOOLS,
)
from recall_server.app import Handler  # noqa: E402
from recall_server.mcp_conformance import (  # noqa: E402
    MUTANTS,
    ConformanceError,
    McpConformanceConfig,
    coverage_manifest,
    mutate_report,
    run_conformance,
    validate_report,
)


def private_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value))
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)


class McpConformanceConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.owner = self.root / "owner.json"
        self.read_only = self.root / "read.json"
        self.isolated = self.root / "isolated.json"
        self.queries = self.root / "queries.json"
        self.output = self.root / "result.json"
        private_json(self.owner, {"token": "synthetic-owner-token"})
        private_json(self.read_only, {"token": "synthetic-read-token"})
        private_json(self.isolated, {"token": "synthetic-isolated-token"})
        private_json(self.queries, {"queries": ["synthetic bounded question"]})
        private_json(self.output, {})
        self.config = self.root / "config.json"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_config(self, **overrides: object) -> None:
        value = {
            "schema_version": 1,
            "url": "https://recall.synthetic.invalid/mcp",
            "owner_token_file": str(self.owner),
            "read_only_token_file": str(self.read_only),
            "isolated_token_file": str(self.isolated),
            "private_queries_file": str(self.queries),
            "output_file": str(self.output),
            **overrides,
        }
        private_json(self.config, value)

    def test_loads_one_closed_config_with_separate_private_files(self) -> None:
        self.write_config()
        loaded = McpConformanceConfig.load(self.config)
        self.assertEqual(loaded.url, "https://recall.synthetic.invalid/mcp")
        self.assertEqual(loaded.private_queries, ("synthetic bounded question",))
        self.assertNotEqual(loaded.owner_token, loaded.read_only_token)
        self.assertNotEqual(loaded.owner_token, loaded.isolated_token)
        self.assertNotEqual(loaded.read_only_token, loaded.isolated_token)

    def test_rejects_plaintext_credentials_unknown_keys_and_unsafe_files(self) -> None:
        cases = (
            {"token": "plaintext"},
            {"follow_redirects": True},
            {"owner_token_file": str(self.read_only)},
        )
        for override in cases:
            with self.subTest(override=override):
                self.write_config(**override)
                with self.assertRaises(ConformanceError):
                    McpConformanceConfig.load(self.config)

        self.write_config()
        self.owner.chmod(0o644)
        with self.assertRaises(ConformanceError):
            McpConformanceConfig.load(self.config)

    def test_rejects_symlinks_and_non_https_non_loopback_urls(self) -> None:
        self.write_config()
        real = self.owner
        linked = self.root / "owner-link.json"
        linked.symlink_to(real)
        self.write_config(owner_token_file=str(linked))
        with self.assertRaises(ConformanceError):
            McpConformanceConfig.load(self.config)

        self.write_config(url="http://recall.synthetic.invalid/mcp")
        with self.assertRaises(ConformanceError):
            McpConformanceConfig.load(self.config)

    def test_rejects_output_inside_a_git_repository(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory(dir=repository) as tracked_parent:
            unsafe_output = Path(tracked_parent) / "output.json"
            private_json(unsafe_output, {})
            self.write_config(output_file=str(unsafe_output))
            with self.assertRaises(ConformanceError):
                McpConformanceConfig.load(self.config)


class McpConformanceCoverageTest(unittest.TestCase):
    def test_manifest_is_derived_from_every_shipped_definition(self) -> None:
        manifest = coverage_manifest()
        expected_tools = {
            tool["name"]: set(tool["inputSchema"]["properties"])
            for tool in READ_TOOLS + WRITE_TOOLS
        }
        self.assertEqual(
            set(manifest["protocol_versions"]),
            set(SUPPORTED_PROTOCOL_VERSIONS),
        )
        self.assertEqual(set(manifest["tools"]), set(expected_tools))
        self.assertEqual(
            set(manifest["declared_arguments"]),
            {
                f"{tool}.{argument}"
                for tool, arguments in expected_tools.items()
                for argument in arguments
            },
        )
        self.assertEqual(
            set(manifest["methods"]),
            {
                "initialize",
                "server/discover",
                "notifications/initialized",
                "ping",
                "tools/list",
                "tools/call",
                "tasks/get",
                "tasks/update",
                "tasks/cancel",
                "subscriptions/listen",
            },
        )
        self.assertEqual(
            set(manifest["capability_classes"]),
            {"owner", "read_only", "isolated"},
        )
        self.assertGreater(len(manifest["lifecycle_paths"]), 5)
        self.assertGreater(len(manifest["abuse_cells"]), 5)

    def test_every_named_fake_success_mutant_fails_validation(self) -> None:
        manifest = coverage_manifest()
        expected = sum(len(values) for values in manifest.values())
        report = {
            "schema_version": 1,
            "status": "pass",
            "coverage": {
                "expected": expected,
                "executed": expected,
                "missing": 0,
                "ratio": 1.0,
                "protocol_versions": len(manifest["protocol_versions"]),
                "methods": len(manifest["methods"]),
                "tools": len(manifest["tools"]),
                "capability_classes": len(manifest["capability_classes"]),
                "declared_arguments": len(manifest["declared_arguments"]),
                "lifecycle_paths": len(manifest["lifecycle_paths"]),
                "abuse_cells": len(manifest["abuse_cells"]),
            },
            "checks": {"passed": expected, "failed": 0},
            "private_queries": {"total": 1, "with_evidence": 1},
            "assertions": {
                "expected_tool_errors": 6,
                "tool_errors_observed": 6,
                "receipts_found": 2,
                "receipts_resolved": 2,
                "receipt_resolution_rate": 1.0,
                "capture_event_delta": 1,
                "capture_replay_event_delta": 0,
                "capture_same_receipt": True,
                "live_hits_after_forget": 0,
                "forget_replay": True,
                "credential_values_emitted": 0,
                "response_bodies_emitted": 0,
            },
        }
        validate_report(report)
        self.assertEqual(
            set(MUTANTS),
            {
                "omitted_case",
                "swallowed_tool_error",
                "unresolved_receipt",
                "duplicate_capture",
                "ineffective_forget",
                "leaked_response_body",
            },
        )
        for mutant in MUTANTS:
            with self.subTest(mutant=mutant):
                with self.assertRaises(ConformanceError):
                    validate_report(mutate_report(report, mutant))


class LoopbackStore:
    def __init__(self) -> None:
        self.capture_arguments: dict | None = None
        self.capture_forgotten = False

    def authenticate_bearer(self, token: str, scope: str) -> dict | None:
        if scope != "read":
            return None
        principals = {
            "synthetic-owner-token": {
                "name": "owner",
                "source_id": "synthetic:capture",
                "principal_id": "synthetic-owner",
                "capture_origin": "synthetic-conformance",
                "scopes": ["read", "write"],
            },
            "synthetic-read-token": {
                "name": "read",
                "source_id": None,
                "principal_id": "synthetic-owner",
                "scopes": ["read"],
            },
            "synthetic-isolated-token": {
                "name": "isolated",
                "source_id": None,
                "principal_id": "synthetic-isolated",
                "scopes": ["read"],
            },
        }
        return principals.get(token)

    def authorized_source_ids(self, principal_id: str) -> list[str]:
        if principal_id == "synthetic-owner":
            return ["synthetic:history"]
        return []

    def search(self, query, filters, limit, authorized_source):
        if authorized_source == []:
            return {"results": []}
        if query.startswith("synthetic-conformance-"):
            if self.capture_arguments is not None and not self.capture_forgotten:
                return {
                    "results": [
                        {
                            "receipt": (
                                "recall://synthetic:capture/capture-1?rev=1#item=0"
                            ),
                            "source_id": "synthetic:capture",
                            "text": "synthetic captured evidence",
                        }
                    ]
                }
            return {"results": []}
        return {
            "results": [
                {
                    "receipt": "recall://synthetic:history/item-1?rev=1",
                    "source_id": "synthetic:history",
                    "text": "synthetic evidence",
                }
            ]
        }

    def show(self, target, *, around, tail, prompts, authorized_source):
        if around is None and tail == 0:
            raise RuntimeError("unbounded synthetic show")
        if target == "recall://synthetic:history/item-1?rev=1":
            return {"chunks": [{"text": "synthetic evidence"}]}
        return None

    def related(self, *, cwd, branch, limit, mains_only, fast, authorized_source):
        return {"results": []}

    def capture(self, principal, arguments):
        replay = self.capture_arguments is not None
        self.capture_arguments = arguments
        return {
            "status": "committed",
            "receipt": "recall://synthetic:capture/capture-1?rev=1#item=0",
            "replay": replay,
            "privacy": {"mode": "scrub", "changed_fields": 0},
        }

    def forget_capture(self, principal, receipt):
        replay = self.capture_forgotten
        self.capture_forgotten = True
        return {
            "status": "committed",
            "receipt": "recall://synthetic:capture/capture-1?rev=2",
            "replay": replay,
        }


class McpConformanceLoopbackTest(unittest.TestCase):
    def test_complete_loopback_matrix_emits_aggregate_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            owner = root / "owner.json"
            read_only = root / "read.json"
            isolated = root / "isolated.json"
            queries = root / "queries.json"
            output = root / "output.json"
            config_path = root / "config.json"
            private_json(owner, {"token": "synthetic-owner-token"})
            private_json(read_only, {"token": "synthetic-read-token"})
            private_json(isolated, {"token": "synthetic-isolated-token"})
            private_json(queries, {"queries": ["synthetic bounded question"]})
            private_json(output, {})

            Handler.store = LoopbackStore()
            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            environment = mock.patch.dict(
                os.environ,
                {
                    "RECALL_AUTH_REQUIRED": "1",
                    "RECALL_HTTP_PROFILE": "public-mcp",
                    "RECALL_TRUST_TAILSCALE_HEADERS": "0",
                    "RECALL_MCP_ALLOWED_ORIGINS": (
                        "https://conformance.synthetic.invalid"
                    ),
                },
                clear=False,
            )
            environment.start()
            thread.start()
            try:
                private_json(
                    config_path,
                    {
                        "schema_version": 1,
                        "url": f"http://127.0.0.1:{server.server_port}/mcp",
                        "origin": "https://conformance.synthetic.invalid",
                        "owner_token_file": str(owner),
                        "read_only_token_file": str(read_only),
                        "isolated_token_file": str(isolated),
                        "private_queries_file": str(queries),
                        "output_file": str(output),
                    },
                )
                config = McpConformanceConfig.load(config_path)
                report = run_conformance(config)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)
                environment.stop()

            rendered = output.read_text()
            self.assertEqual(json.loads(rendered), report)
            self.assertEqual(report["status"], "pass")
            self.assertEqual(report["coverage"]["ratio"], 1.0)
            self.assertNotIn("synthetic-owner-token", rendered)
            self.assertNotIn("synthetic bounded question", rendered)
            self.assertNotIn("structuredContent", rendered)

    def test_redirect_is_rejected_without_following_it(self) -> None:
        class RedirectHandler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(302)
                self.send_header("Location", "https://redirect.synthetic.invalid/mcp")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, _format, *_args):
                return

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = {
                name: root / f"{name}.json"
                for name in ("owner", "read", "isolated", "queries", "output")
            }
            private_json(paths["owner"], {"token": "synthetic-owner-token"})
            private_json(paths["read"], {"token": "synthetic-read-token"})
            private_json(paths["isolated"], {"token": "synthetic-isolated-token"})
            private_json(paths["queries"], {"queries": ["synthetic question"]})
            private_json(paths["output"], {})
            config_path = root / "config.json"
            server = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                private_json(
                    config_path,
                    {
                        "schema_version": 1,
                        "url": f"http://127.0.0.1:{server.server_port}/mcp",
                        "owner_token_file": str(paths["owner"]),
                        "read_only_token_file": str(paths["read"]),
                        "isolated_token_file": str(paths["isolated"]),
                        "private_queries_file": str(paths["queries"]),
                        "output_file": str(paths["output"]),
                    },
                )
                with self.assertRaisesRegex(ConformanceError, "redirect"):
                    run_conformance(McpConformanceConfig.load(config_path))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
