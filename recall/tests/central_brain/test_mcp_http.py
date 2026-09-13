from __future__ import annotations

import http.client
import json
import os
import sys
import threading
import types
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

try:
    import psycopg  # noqa: F401
except ModuleNotFoundError:
    psycopg = types.ModuleType("psycopg")
    psycopg_rows = types.ModuleType("psycopg.rows")
    psycopg_rows.dict_row = object()
    psycopg.rows = psycopg_rows
    sys.modules["psycopg"] = psycopg
    sys.modules["psycopg.rows"] = psycopg_rows

from recall_server.app import Handler  # noqa: E402
from recall_server.deep_inspection import DeepInspectionError  # noqa: E402


class FakeStore:
    def __init__(self, *, write: bool = True) -> None:
        self.calls: list[tuple] = []
        self.write = write

    def authenticate_bearer(self, token: str, scope: str) -> dict | None:
        self.calls.append(("authenticate", token, scope))
        if token != "synthetic-read-token" or scope != "read":
            return None
        return {
            "name": "synthetic-grep-agent",
            "source_id": "synthetic:grep-capture",
            "principal_id": "synthetic-owner",
            "capture_origin": "grep-agent",
            "scopes": ["read", "write"] if self.write else ["read"],
        }

    def authorized_source_ids(self, principal_id: str) -> list[str]:
        self.calls.append(("authorized_sources", principal_id))
        return ["synthetic:codex", "synthetic:cowork"]

    def search(self, query, filters, limit, authorized_source):
        self.calls.append(("search", query, filters, limit, authorized_source))
        return {
            "query": query,
            "results": [
                {
                    "receipt": "recall://synthetic:codex/item-1?rev=1",
                    "text": "Synthetic retrieval result",
                }
            ],
        }

    def execute_agent_program(self, program, **arguments):
        self.calls.append(("exec", program, arguments))
        return {
            "provider": "synthetic-archil",
            "stdout": "synthetic evidence",
            "stderr": "",
            "exit_code": 0,
            "complete": True,
            "stopped_reason": "completed",
            "output_truncated": False,
            "opened_receipts": [
                "recall://source:synthetic:company/item-1?rev=1"
            ],
            "documents_available": len(arguments["logical_document_ids"]),
            "objects_available": len(arguments["logical_document_ids"]),
        }

    def scope_documents(self, *, filters, limit, offset):
        self.calls.append(("scope", filters, limit, offset))
        return {
            "documents": [{
                "source_id": "source:synthetic:company",
                "logical_document_id": (
                    "ldoc_0123456789abcdef0123456789abcdef"
                ),
                "revision": 1,
                "first_occurred_at": "2026-08-08T00:00:00Z",
                "last_occurred_at": "2026-08-08T01:00:00Z",
                "record_count": 2,
                "part_count": 1,
            }],
            "total_documents": 1,
            "offset": offset,
            "complete": True,
            "diagnostics": {"engine": "canonical-scope-v1"},
        }

    def list_people(self):
        self.calls.append(("people",))
        return {
            "people": [{
                "actor_id": "actor_0123456789abcdef0123456789abcdef",
                "display_name": "Synthetic Employee",
                "sources": [{
                    "source_id": "source:synthetic:company",
                    "relation": "owner",
                }],
            }],
            "complete": True,
            "diagnostics": {"engine": "canonical-people-v1"},
        }

    def execute_agent_program_parallel(self, program, **arguments):
        self.calls.append(("exec_map", program, arguments))
        return {
            "provider": "synthetic-archil",
            "complete": True,
            "opened_receipts": [],
            "shards": [{"shard": 0, "complete": True, "stdout": ""}],
            "timing": {"elapsed_ms": 100.0},
        }

    def execute_parquet_scan(self, program, **arguments):
        self.calls.append(("scan", program, arguments))
        return {
            "provider": "synthetic-archil",
            "stdout": "synthetic scan",
            "stderr": "",
            "exit_code": 0,
            "complete": True,
            "stopped_reason": "completed",
            "output_truncated": False,
            "opened_receipts": [],
            "datasets_available": 4,
            "sources_available": 1,
            "buckets_available": 1,
            "projection_pending": 0,
        }

    def show(self, target, *, around, tail, prompts, authorized_source):
        self.calls.append(("show", target, around, tail, prompts, authorized_source))
        return {
            "receipt": target,
            "text": "Synthetic detail",
        }

    def related(self, *, cwd, branch, limit, mains_only, fast, authorized_source):
        self.calls.append(
            (
                "related",
                cwd,
                branch,
                limit,
                mains_only,
                fast,
                authorized_source,
            )
        )
        return {"results": []}

    def capture(self, principal, arguments):
        self.calls.append(("capture", principal, arguments))
        return {
            "status": "committed",
            "receipt": "recall://synthetic:grep-capture/capture_abc?rev=1",
            "replay": False,
            "privacy": {"mode": "scrub", "changed_fields": 0},
        }

    def forget_capture(self, principal, receipt):
        self.calls.append(("forget", principal, receipt))
        return {
            "status": "committed",
            "receipt": "recall://synthetic:grep-capture/capture_abc?rev=2",
            "replay": False,
        }


class PolicyStore(FakeStore):
    def __init__(self) -> None:
        super().__init__()
        self.audit: list[dict] = []

    def authenticate_bearer(self, token: str, scope: str) -> dict | None:
        self.calls.append(("authenticate", "redacted", scope))
        identities = {
            "synthetic-human-read": {
                "principal_kind": "human",
                "role": "admin",
                "scopes": ["read"],
                "audience": "recall-mcp",
            },
            "synthetic-workload-admin": {
                "principal_kind": "workload",
                "role": "admin",
                "scopes": ["read", "forget"],
                "audience": "recall-mcp",
            },
            "synthetic-wrong-audience": {
                "principal_kind": "human",
                "role": "member",
                "scopes": ["read"],
                "audience": "other-resource",
            },
        }
        identity = identities.get(token)
        if identity is None or scope not in identity["scopes"]:
            return None
        return {
            "credential_kind": "mcp",
            "name": "synthetic-policy-principal",
            "tenant_id": "tenant:synthetic:company",
            "principal_id": "principal:synthetic:member",
            "source_id": None,
            "capture_origin": None,
            "webhook_privacy_mode": None,
            **identity,
        }

    def authorized_canonical_source_ids(self, tenant_id, principal_id):
        self.calls.append(("canonical_sources", tenant_id, principal_id))
        return ["source:synthetic:company"]

    def record_authorization_event(
        self, principal, *, action, allowed, reason, policy_version
    ):
        self.audit.append({
            "principal_kind": principal["principal_kind"],
            "principal_id": principal["principal_id"],
            "tenant_id": principal["tenant_id"],
            "action": action,
            "allowed": allowed,
            "reason": reason,
            "policy_version": policy_version,
        })


class FailingStore(FakeStore):
    def search(self, query, filters, limit, authorized_source):
        raise RuntimeError("private payload must not escape")


class OversizedShowStore(FakeStore):
    def show(self, target, *, around, tail, prompts, authorized_source):
        return {
            "receipt": target,
            "text": "private-oversized-canary-" + ("x" * (1024 * 1024)),
        }


class FailingExecStore(PolicyStore):
    def execute_agent_program(self, program, **arguments):
        del program, arguments
        raise DeepInspectionError("private-provider-payload-must-not-escape")


class NoDeepInspectionStore(PolicyStore):
    unavailable_mcp_tools = frozenset({
        "recall_exec", "recall_exec_map", "recall_scan"
    })


class McpHttpServer:
    def __init__(self, store: FakeStore, verifier=None) -> None:
        Handler.store = store
        Handler.external_identity_verifier = verifier
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        Handler.external_identity_verifier = None

    def request(
        self,
        method: str,
        body: dict | None = None,
        *,
        path: str = "/mcp",
        token: str | None = "synthetic-read-token",
        origin: str | None = None,
        protocol: str | None = None,
        accept: str | None = "application/json, text/event-stream",
    ) -> tuple[int, dict[str, str], bytes]:
        headers: dict[str, str] = {}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if origin is not None:
            headers["Origin"] = origin
        if protocol is not None:
            headers["MCP-Protocol-Version"] = protocol
        if accept is not None:
            headers["Accept"] = accept
        payload = None
        if body is not None:
            payload = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(payload))
        connection = http.client.HTTPConnection(
            "127.0.0.1", self.server.server_port, timeout=2
        )
        connection.request(method, path, body=payload, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        result_headers = {key.casefold(): value for key, value in response.getheaders()}
        connection.close()
        return response.status, result_headers, raw


def request(method: str, request_id: int = 1, params: dict | None = None) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": method,
        "params": params or {},
    }


class RemoteMcpContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = FakeStore()
        self.environment = mock.patch.dict(
            os.environ,
            {
                "RECALL_AUTH_REQUIRED": "1",
                "RECALL_MCP_ALLOWED_ORIGINS": "https://grep.synthetic.invalid",
            },
            clear=False,
        )
        self.environment.start()

    def tearDown(self) -> None:
        self.environment.stop()

    def test_initialize_and_closed_deterministic_tool_list(self) -> None:
        with McpHttpServer(self.store) as server:
            status, headers, raw = server.request(
                "POST",
                request(
                    "initialize",
                    params={
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "synthetic-grep", "version": "1"},
                    },
                ),
            )
            self.assertEqual(status, 200)
            self.assertEqual(headers["content-type"], "application/json")
            initialized = json.loads(raw)
            self.assertEqual(initialized["result"]["protocolVersion"], "2025-11-25")
            self.assertEqual(
                initialized["result"]["capabilities"],
                {"tools": {"listChanged": False}},
            )

            status, _, raw = server.request(
                "POST",
                request("tools/list", request_id=2),
                protocol="2025-11-25",
            )
            self.assertEqual(status, 200)
            names = [tool["name"] for tool in json.loads(raw)["result"]["tools"]]
            self.assertEqual(
                names,
                [
                    "recall_related",
                    "recall_search",
                    "recall_show",
                    "recall_capture",
                    "recall_forget",
                ],
            )

    def test_oauth_resource_challenge_and_default_deny_policy(self) -> None:
        resource = "https://recall.synthetic.invalid/mcp"
        with mock.patch.dict(
            os.environ,
            {
                "RECALL_MCP_RESOURCE_URI": resource,
                "RECALL_AUTHORIZATION_SERVERS":
                    "https://identity.synthetic.invalid/oauth",
                "RECALL_HTTP_PROFILE": "public-mcp",
            },
            clear=False,
        ):
            store = PolicyStore()
            with McpHttpServer(store) as server:
                for metadata_path in (
                    "/.well-known/oauth-protected-resource",
                    "/.well-known/oauth-protected-resource/mcp",
                    (
                        "/.well-known/oauth-protected-resource/mcp/brains/"
                        "tenant:company:parcha"
                    ),
                ):
                    with self.subTest(metadata_path=metadata_path):
                        status, _, raw = server.request(
                            "GET", path=metadata_path, token=None
                        )
                        self.assertEqual(status, 200)
                        metadata = json.loads(raw)
                        expected_resource = (
                            f"{resource}/brains/tenant:company:parcha"
                            if metadata_path.endswith(
                                "/mcp/brains/tenant:company:parcha"
                            )
                            else resource
                        )
                        self.assertEqual(metadata["resource"], expected_resource)
                        self.assertEqual(metadata["scopes_supported"], ["read"])
                        self.assertEqual(
                            metadata["authorization_servers"],
                            ["https://identity.synthetic.invalid/oauth"],
                        )

                status, _, _ = server.request(
                    "GET",
                    path=(
                        "/.well-known/oauth-protected-resource/mcp/brains/"
                        "invalid/extra"
                    ),
                    token=None,
                )
                self.assertEqual(status, 404)

                status, headers, _ = server.request(
                    "POST", request("ping"), token=None
                )
                self.assertEqual(status, 401)
                self.assertEqual(
                    headers["www-authenticate"],
                    "Bearer resource_metadata=\""
                    "https://recall.synthetic.invalid/"
                    ".well-known/oauth-protected-resource/mcp\", scope=\"read\"",
                )

                status, headers, _ = server.request(
                    "POST",
                    request("ping"),
                    path="/mcp/brains/tenant:company:parcha",
                    token=None,
                )
                self.assertEqual(status, 401)
                self.assertEqual(
                    headers["www-authenticate"],
                    "Bearer resource_metadata=\""
                    "https://recall.synthetic.invalid/"
                    ".well-known/oauth-protected-resource/mcp/brains/"
                    "tenant:company:parcha\", scope=\"read\"",
                )

                status, _, raw = server.request(
                    "POST",
                    request("tools/list"),
                    token="synthetic-human-read",
                    protocol="2025-11-25",
                )
                self.assertEqual(status, 200)
                names = {
                    tool["name"] for tool in json.loads(raw)["result"]["tools"]
                }
                self.assertEqual(names, {
                    "recall_search",
                    "recall_people",
                    "recall_scope",
                    "recall_exec",
                    "recall_exec_map",
                    "recall_scan",
                    "recall_session_context",
                    "recall_show",
                    "recall_related",
                })

                status, _, raw = server.request(
                    "POST",
                    request("tools/call", params={
                        "name": "recall_people",
                        "arguments": {},
                    }),
                    token="synthetic-human-read",
                    protocol="2025-11-25",
                )
                self.assertEqual(status, 200)
                people = json.loads(raw)["result"]["structuredContent"]
                self.assertEqual(
                    people["people"][0]["display_name"],
                    "Synthetic Employee",
                )
                self.assertNotIn("email", json.dumps(people).casefold())

                status, _, raw = server.request(
                    "POST",
                    request("tools/call", params={
                        "name": "recall_forget",
                        "arguments": {"receipt": "recall://synthetic/hidden"},
                    }),
                    token="synthetic-human-read",
                    protocol="2025-11-25",
                )
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(raw)["error"]["message"], "unknown tool")

                status, _, raw = server.request(
                    "POST",
                    request("tools/call", params={
                        "name": "../../private-object",
                        "arguments": {},
                    }),
                    token="synthetic-human-read",
                    protocol="2025-11-25",
                )
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(raw)["error"]["message"], "unknown tool")
                self.assertEqual(store.audit[-1]["action"], "mcp.unknown_tool")

                status, _, _ = server.request(
                    "POST",
                    request("ping"),
                    token="synthetic-wrong-audience",
                )
                self.assertEqual(status, 401)

            forget_audit = next(
                item for item in store.audit
                if item["action"] == "mcp.recall_forget"
            )
            self.assertFalse(forget_audit["allowed"])
            self.assertEqual(forget_audit["reason"], "scope_denied")
            rendered = json.dumps(store.audit)
            self.assertNotIn("synthetic-human-read", rendered)
            self.assertNotIn("synthetic-wrong-audience", rendered)

    def test_capture_and_forget_are_capability_gated_and_host_bound(self) -> None:
        body = "Synthetic bounded memory selected by the user"
        capture = {
            "schema_version": 1,
            "title": "Synthetic decision",
            "body": body,
            "occurred_at": "2026-07-17T20:00:00Z",
            "tags": ["synthetic"],
            "provenance": {"uri": "manual://grep-agent"},
        }
        with McpHttpServer(self.store) as server:
            status, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={"name": "recall_capture", "arguments": capture},
                ),
                protocol="2025-11-25",
            )
            self.assertEqual(status, 200)
            response = json.loads(raw)
            self.assertNotIn(body, json.dumps(response))
            capture_call = next(call for call in self.store.calls if call[0] == "capture")
            self.assertEqual(capture_call[1]["source_id"], "synthetic:grep-capture")
            self.assertEqual(capture_call[1]["capture_origin"], "grep-agent")
            self.assertEqual(capture_call[2], capture)

            receipt = response["result"]["structuredContent"]["receipt"]
            _, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_forget",
                        "arguments": {"receipt": receipt},
                    },
                ),
                protocol="2025-11-25",
            )
            self.assertEqual(
                json.loads(raw)["result"]["structuredContent"]["receipt"],
                "recall://synthetic:grep-capture/capture_abc?rev=2",
            )

    def test_capture_origin_cannot_be_supplied_by_the_model(self) -> None:
        with McpHttpServer(self.store) as server:
            _, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_capture",
                        "arguments": {
                            "schema_version": 1,
                            "title": "Synthetic",
                            "body": "Synthetic",
                            "origin": "spoofed",
                            "occurred_at": "2026-07-17T20:00:00Z",
                            "provenance": {"uri": "manual://synthetic"},
                        },
                    },
                ),
                protocol="2025-11-25",
            )
        self.assertEqual(json.loads(raw)["error"]["code"], -32602)
        self.assertNotIn("capture", {call[0] for call in self.store.calls})

    def test_read_only_principal_never_discovers_or_calls_write_tools(self) -> None:
        store = FakeStore(write=False)
        with McpHttpServer(store) as server:
            _, _, listed = server.request(
                "POST",
                request("tools/list"),
                protocol="2025-11-25",
            )
            names = {
                tool["name"]
                for tool in json.loads(listed)["result"]["tools"]
            }
            self.assertNotIn("recall_capture", names)
            _, _, called = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_capture",
                        "arguments": {},
                    },
                ),
                protocol="2025-11-25",
            )
        self.assertEqual(json.loads(called)["error"]["code"], -32602)
        self.assertNotIn("capture", {call[0] for call in store.calls})

    def test_search_is_natural_language_and_source_scoped(self) -> None:
        with self.assertLogs("recall.mcp", level="INFO") as captured:
            with McpHttpServer(self.store) as server:
                status, _, raw = server.request(
                    "POST",
                    request(
                        "tools/call",
                        params={
                            "name": "recall_search",
                            "arguments": {
                                "query": "What did we decide about the synthetic rollout?",
                                "limit": 5,
                            },
                        },
                    ),
                    protocol="2025-11-25",
                )
        self.assertEqual(status, 200)
        result = json.loads(raw)["result"]
        self.assertFalse(result.get("isError", False))
        self.assertEqual(
            result["structuredContent"]["results"][0]["receipt"],
            "recall://synthetic:codex/item-1?rev=1",
        )
        self.assertIn(
            (
                "search",
                "What did we decide about the synthetic rollout?",
                {},
                5,
                ["synthetic:codex", "synthetic:cowork"],
            ),
            self.store.calls,
        )
        rendered_logs = "\n".join(captured.output)
        self.assertIn("tool=recall_search outcome=ok", rendered_logs)
        self.assertIn("elapsed_ms=", rendered_logs)
        self.assertNotIn("synthetic rollout", rendered_logs)

    def test_declared_tool_bounds_match_the_runtime_contract(self) -> None:
        with McpHttpServer(self.store) as server:
            _, _, raw = server.request(
                "POST",
                request("tools/list"),
                protocol="2025-11-25",
            )
        tools = {
            tool["name"]: tool["inputSchema"]
            for tool in json.loads(raw)["result"]["tools"]
        }
        self.assertEqual(
            tools["recall_search"]["properties"]["query"]["maxLength"],
            8192,
        )
        self.assertEqual(
            tools["recall_search"]["properties"]["limit"]["maximum"],
            20,
        )
        self.assertEqual(
            tools["recall_search"]["properties"]["filters"]["properties"]["since"],
            {
                "oneOf": [
                    {"type": "string", "format": "date"},
                    {"type": "string", "format": "date-time"},
                ]
            },
        )
        with McpHttpServer(PolicyStore()) as server:
            _, _, canonical_raw = server.request(
                "POST",
                request("tools/list"),
                token="synthetic-human-read",
                protocol="2025-11-25",
            )
        canonical_catalog = {
            tool["name"]: tool
            for tool in json.loads(canonical_raw)["result"]["tools"]
        }
        canonical_tools = {
            name: tool["inputSchema"]
            for name, tool in canonical_catalog.items()
        }
        self.assertEqual(
            canonical_tools["recall_exec"]["properties"]["targets"]["maxItems"],
            20,
        )
        self.assertEqual(
            canonical_tools["recall_scope"]["properties"]["limit"]["maximum"],
            80,
        )
        self.assertEqual(
            canonical_tools["recall_exec_map"]["properties"]["targets"]["maxItems"],
            80,
        )
        self.assertEqual(
            canonical_tools["recall_exec"]["properties"]["timeout_seconds"][
                "maximum"
            ],
            30,
        )
        self.assertEqual(
            canonical_tools["recall_scan"]["properties"]["timeout_seconds"],
            {"type": "integer", "minimum": 1, "maximum": 240, "default": 60},
        )
        self.assertIn(
            "/docs/d1",
            canonical_catalog["recall_exec"]["description"],
        )
        self.assertIn(
            "month granularity",
            canonical_catalog["recall_scan"]["description"],
        )
        self.assertIn(
            "actors-part-*.parquet",
            canonical_catalog["recall_scan"]["description"],
        )
        self.assertIn(
            "passages-part-*.parquet",
            canonical_catalog["recall_scan"]["description"],
        )
        self.assertIn(
            "16 KiB",
            canonical_catalog["recall_scan"]["description"],
        )
        self.assertIn(
            "recall-scan --broad --fixed",
            canonical_catalog["recall_exec"]["description"],
        )
        self.assertIn(
            "recall_exec_map",
            canonical_catalog["recall_exec"]["description"],
        )
        self.assertIn(
            "rg -L",
            canonical_catalog["recall_exec"]["description"],
        )
        self.assertIn(
            "event_native_id",
            canonical_catalog["recall_exec"]["description"],
        )
        self.assertIn(
            "shard_size=1",
            canonical_catalog["recall_exec_map"]["description"],
        )
        self.assertIn(
            "rg -L",
            canonical_catalog["recall_exec_map"]["description"],
        )
        self.assertIn(
            "content_fragment",
            canonical_catalog["recall_exec_map"]["description"],
        )
        self.assertEqual(
            tools["recall_search"]["properties"]["filters"]["properties"]["person"],
            {"type": "string", "maxLength": 256},
        )
        self.assertEqual(
            tools["recall_search"]["properties"]["filters"]["properties"]
            ["person_relation"]["enum"],
            [
                "author",
                "contributor",
                "owner",
                "organizer",
                "participant",
                "attendee",
            ],
        )
        self.assertEqual(
            tools["recall_related"]["properties"]["limit"]["maximum"],
            20,
        )
        self.assertEqual(
            tools["recall_show"]["properties"]["around"],
            {"type": "string", "format": "date-time"},
        )
        capture = tools["recall_capture"]["properties"]
        self.assertEqual(capture["body"]["maxLength"], 32_000)
        self.assertEqual(capture["tags"]["items"]["maxLength"], 64)
        self.assertIn("pattern", capture["provenance"]["properties"]["uri"])

    def test_initialize_teaches_evidence_first_retrieval(self) -> None:
        with McpHttpServer(self.store) as server:
            _, _, raw = server.request(
                "POST",
                request(
                    "initialize",
                    params={"protocolVersion": "2025-06-18"},
                ),
                protocol="2025-06-18",
            )
        instructions = json.loads(raw)["result"]["instructions"]
        self.assertIn("recall_search", instructions)
        self.assertIn("recall_exec", instructions)
        self.assertIn("opened_receipts", instructions)
        self.assertIn("Never infer", instructions)
        self.assertIn("each named part", instructions)
        self.assertIn("passages-part-*.parquet", instructions)
        self.assertIn("planning hints", instructions)

    def test_show_uses_timestamp_and_rejects_conflicting_tail_before_store(self) -> None:
        target = "recall://synthetic:codex/item-1?rev=1"
        timestamp = "2026-07-18T02:00:00Z"
        with McpHttpServer(self.store) as server:
            status, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_show",
                        "arguments": {
                            "target": target,
                            "around": timestamp,
                            "prompts": True,
                        },
                    },
                ),
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        self.assertIn("result", json.loads(raw))
        self.assertIn(
            (
                "show",
                target,
                timestamp,
                0,
                True,
                ["synthetic:codex", "synthetic:cowork"],
            ),
            self.store.calls,
        )

        invalid = (
            {"around": 1},
            {"around": "not-a-time"},
            {"around": "2026-07-18T02:00:00"},
            {"around": timestamp, "tail": 1},
        )
        for arguments in invalid:
            with self.subTest(arguments=arguments):
                self.store.calls.clear()
                with McpHttpServer(self.store) as server:
                    status, _, raw = server.request(
                        "POST",
                        request(
                            "tools/call",
                            params={
                                "name": "recall_show",
                                "arguments": {"target": target, **arguments},
                            },
                        ),
                        protocol="2025-11-25",
                    )
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(raw)["error"]["code"], -32602)
                self.assertNotIn("show", {call[0] for call in self.store.calls})

    def test_exec_passes_only_bounded_exact_targets_to_canonical_store(self) -> None:
        first = "ldoc_0123456789abcdef0123456789abcdef"
        second = "ldoc_fedcba9876543210fedcba9876543210"
        store = PolicyStore()
        with McpHttpServer(store) as server:
            status, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_exec",
                        "arguments": {
                            "targets": [
                                {"logical_document_id": first, "alias": "d1"},
                                {"logical_document_id": second, "alias": "d2"},
                            ],
                            "program": "rg -n --fixed-strings decision /docs",
                            "timeout_seconds": 17,
                        },
                    },
                ),
                token="synthetic-human-read",
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        result = json.loads(raw)["result"]["structuredContent"]
        self.assertEqual(result["documents_available"], 2)
        self.assertEqual(result["opened_receipts"], [
            "recall://source:synthetic:company/item-1?rev=1"
        ])
        self.assertIn(
            (
                "exec",
                "rg -n --fixed-strings decision /docs",
                {
                    "logical_document_ids": (first, second),
                    "document_aliases": {first: "d1", second: "d2"},
                    "record_spans": {first: (), second: ()},
                    "routing_receipts": {first: (), second: ()},
                    "timeout_seconds": 17,
                },
            ),
            store.calls,
        )

    def test_exec_rejects_duplicate_targets_before_store(self) -> None:
        document_id = "ldoc_0123456789abcdef0123456789abcdef"
        store = PolicyStore()
        with McpHttpServer(store) as server:
            status, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_exec",
                        "arguments": {
                            "targets": [
                                {"logical_document_id": document_id, "alias": "d1"},
                                {"logical_document_id": document_id, "alias": "d2"},
                            ],
                            "program": "true",
                        },
                    },
                ),
                token="synthetic-human-read",
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(raw)["error"]["code"], -32602)
        self.assertNotIn("exec", {call[0] for call in store.calls})

    def test_scope_and_parallel_exec_are_bounded_and_principal_scoped(self) -> None:
        document_id = "ldoc_0123456789abcdef0123456789abcdef"
        store = PolicyStore()
        with McpHttpServer(store) as server:
            status, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_scope",
                        "arguments": {
                            "filters": {
                                "person": "Synthetic Person",
                                "person_relation": "contributor",
                                "since": "2026-08-08T00:00:00Z",
                                "until": "2026-08-09T00:00:00Z",
                            },
                            "limit": 40,
                            "offset": 0,
                        },
                    },
                ),
                token="synthetic-human-read",
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        self.assertIn("result", json.loads(raw))
        self.assertIn(
            (
                "scope",
                {
                    "person": "Synthetic Person",
                    "person_relation": "contributor",
                    "since": "2026-08-08T00:00:00Z",
                    "until": "2026-08-09T00:00:00Z",
                },
                40,
                0,
            ),
            store.calls,
        )

        store.calls.clear()
        with McpHttpServer(store) as server:
            status, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_exec_map",
                        "arguments": {
                            "targets": [{
                                "logical_document_id": document_id,
                                "alias": "d1",
                            }],
                            "program": "rg -n --fixed-strings decision /docs",
                            "max_parallel": 4,
                            "shard_size": 20,
                            "timeout_seconds": 17,
                        },
                    },
                ),
                token="synthetic-human-read",
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        self.assertIn("result", json.loads(raw))
        self.assertIn("exec_map", {call[0] for call in store.calls})

    def test_scan_is_one_code_mode_call_with_hard_scope(self) -> None:
        store = PolicyStore()
        program = (
            "duckdb -json -c \"select record_json from "
            "read_parquet('/datasets/*/*/records-part-*.parquet') limit 5\""
        )
        filters = {
            "person": "Synthetic Person",
            "source_family": "coding_history",
            "since": "2026-08-01T00:00:00Z",
            "until": "2026-08-08T00:00:00Z",
        }
        with McpHttpServer(store) as server:
            status, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_scan",
                        "arguments": {
                            "filters": filters,
                            "program": program,
                            "timeout_seconds": 120,
                        },
                    },
                ),
                token="synthetic-human-read",
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        result = json.loads(raw)["result"]["structuredContent"]
        self.assertEqual(result["datasets_available"], 4)
        self.assertIn(
            (
                "scan",
                program,
                {"filters": filters, "timeout_seconds": 120},
            ),
            store.calls,
        )

    def test_exec_provider_failure_is_content_free(self) -> None:
        document_id = "ldoc_0123456789abcdef0123456789abcdef"
        with self.assertLogs("recall.mcp", level="ERROR") as captured:
            with McpHttpServer(FailingExecStore()) as server:
                status, _, raw = server.request(
                    "POST",
                    request(
                        "tools/call",
                        params={
                            "name": "recall_exec",
                            "arguments": {
                                "targets": [
                                    {"logical_document_id": document_id, "alias": "d1"}
                                ],
                                "program": "true",
                            },
                        },
                    ),
                    token="synthetic-human-read",
                    protocol="2025-11-25",
                )
        self.assertEqual(status, 200)
        self.assertEqual(
            json.loads(raw)["error"],
            {"code": -32603, "message": "recall_exec_failed"},
        )
        self.assertNotIn(b"private-provider-payload", raw)
        rendered_logs = "\n".join(captured.output)
        self.assertIn("tool=recall_exec outcome=error", rendered_logs)
        self.assertIn("code=deep_inspection_failed", rendered_logs)
        self.assertNotIn("private-provider-payload", rendered_logs)

    def test_unavailable_deep_inspection_tools_are_not_advertised_or_callable(self):
        store = NoDeepInspectionStore()
        with McpHttpServer(store) as server:
            _, _, listed = server.request(
                "POST",
                request("tools/list"),
                token="synthetic-human-read",
                protocol="2025-11-25",
            )
            names = {
                tool["name"] for tool in json.loads(listed)["result"]["tools"]
            }
            self.assertTrue(
                {"recall_exec", "recall_exec_map", "recall_scan"}.isdisjoint(names)
            )
            _, _, called = server.request(
                "POST",
                request("tools/call", params={
                    "name": "recall_exec",
                    "arguments": {},
                }),
                token="synthetic-human-read",
                protocol="2025-11-25",
            )
        self.assertEqual(json.loads(called)["error"]["message"], "unknown tool")
        self.assertNotIn("exec", {call[0] for call in store.calls})

    def test_search_and_related_bounds_reject_before_store(self) -> None:
        cases = (
            (
                "recall_search",
                {"query": "synthetic", "limit": 21},
                "search",
            ),
            (
                "recall_search",
                {"query": "x" * 8193},
                "search",
            ),
            (
                "recall_related",
                {"cwd": "/synthetic", "limit": 21},
                "related",
            ),
        )
        for tool_name, arguments, store_call in cases:
            with self.subTest(tool=tool_name):
                self.store.calls.clear()
                with McpHttpServer(self.store) as server:
                    status, _, raw = server.request(
                        "POST",
                        request(
                            "tools/call",
                            params={
                                "name": tool_name,
                                "arguments": arguments,
                            },
                        ),
                        protocol="2025-11-25",
                    )
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(raw)["error"]["code"], -32602)
                self.assertNotIn(
                    store_call,
                    {call[0] for call in self.store.calls},
                )

    def test_public_profile_hides_every_non_mcp_route_before_store_io(self) -> None:
        self.environment.stop()
        self.environment = mock.patch.dict(
            os.environ,
            {
                "RECALL_AUTH_REQUIRED": "1",
                "RECALL_HTTP_PROFILE": "public-mcp",
                "RECALL_TRUST_TAILSCALE_HEADERS": "0",
            },
            clear=False,
        )
        self.environment.start()
        with McpHttpServer(self.store) as server:
            for method, path in (
                ("GET", "/metrics"),
                ("GET", "/v1/doctor"),
                ("GET", "/v1/receipts/resolve?receipt=synthetic"),
                ("POST", "/v1/search"),
                ("POST", "/v1/ingest/batches"),
                ("POST", "/webhooks/v1/events"),
                ("DELETE", "/mcp"),
            ):
                with self.subTest(method=method, path=path):
                    self.store.calls.clear()
                    status, _, _ = server.request(method, path=path)
                    self.assertEqual(status, 404)
                    self.assertEqual(self.store.calls, [])

    def test_principal_without_grants_is_never_treated_as_unrestricted(self) -> None:
        self.store.authorized_source_ids = mock.Mock(return_value=[])
        with McpHttpServer(self.store) as server:
            status, _, _ = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_search",
                        "arguments": {"query": "synthetic denied"},
                    },
                ),
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        search_call = next(call for call in self.store.calls if call[0] == "search")
        self.assertEqual(search_call[-1], [])

    def test_remote_mcp_is_read_only_and_unknown_tools_do_not_touch_store(self) -> None:
        with McpHttpServer(self.store) as server:
            status, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={"name": "recall_ingest", "arguments": {}},
                ),
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        response = json.loads(raw)
        self.assertEqual(response["error"]["code"], -32602)
        self.assertNotIn("ingest", {call[0] for call in self.store.calls})

    def test_auth_origin_protocol_and_accept_fail_closed_before_tool_io(self) -> None:
        cases = [
            {"token": None, "expected": 401},
            {
                "origin": "https://attacker.synthetic.invalid",
                "expected": 403,
            },
            {"protocol": "2024-11-05", "expected": 400},
            {"accept": "application/json", "expected": 406},
        ]
        for case in cases:
            with self.subTest(case=case):
                self.store.calls.clear()
                options = {
                    key: value for key, value in case.items() if key not in {"expected"}
                }
                with McpHttpServer(self.store) as server:
                    status, _, _ = server.request(
                        "POST",
                        request(
                            "tools/call",
                            params={
                                "name": "recall_search",
                                "arguments": {"query": "synthetic"},
                            },
                        ),
                        **options,
                    )
                self.assertEqual(status, case["expected"])
                self.assertNotIn("search", {call[0] for call in self.store.calls})

    def test_get_explicitly_declines_sse_and_requires_auth(self) -> None:
        with McpHttpServer(self.store) as server:
            status, headers, raw = server.request("GET", accept="text/event-stream")
            self.assertEqual(status, 405)
            self.assertEqual(headers["allow"], "POST")
            self.assertEqual(raw, b"")

            status, _, _ = server.request("GET", token=None, accept="text/event-stream")
            self.assertEqual(status, 401)

    def test_stateless_call_survives_server_restart_without_session_affinity(
        self,
    ) -> None:
        with McpHttpServer(self.store) as first:
            initialized, headers, _ = first.request(
                "POST",
                request(
                    "initialize",
                    params={
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "synthetic-grep", "version": "1"},
                    },
                ),
            )
            self.assertEqual(initialized, 200)
            self.assertNotIn("mcp-session-id", headers)

        with McpHttpServer(self.store) as restarted:
            status, _, raw = restarted.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_search",
                        "arguments": {"query": "synthetic restart"},
                    },
                ),
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        self.assertIn("result", json.loads(raw))

    def test_notifications_are_accepted_without_response_body(self) -> None:
        notification = {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {},
        }
        with McpHttpServer(self.store) as server:
            status, _, raw = server.request("POST", notification, protocol="2025-11-25")
        self.assertEqual(status, 202)
        self.assertEqual(raw, b"")

    def test_backend_failures_are_content_free_json_rpc_errors(self) -> None:
        with McpHttpServer(FailingStore()) as server:
            status, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_search",
                        "arguments": {"query": "synthetic failure"},
                    },
                ),
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        self.assertEqual(
            json.loads(raw)["error"],
            {"code": -32603, "message": "tool execution failed"},
        )
        self.assertNotIn(b"private payload", raw)

    def test_oversized_tool_result_becomes_a_bounded_content_free_error(self) -> None:
        target = "recall://synthetic:codex/item-1?rev=1"
        with McpHttpServer(OversizedShowStore()) as server:
            status, _, raw = server.request(
                "POST",
                request(
                    "tools/call",
                    params={
                        "name": "recall_show",
                        "arguments": {"target": target},
                    },
                ),
                protocol="2025-11-25",
            )
        self.assertEqual(status, 200)
        self.assertEqual(
            json.loads(raw)["error"],
            {"code": -32603, "message": "tool result exceeds limit"},
        )
        self.assertLess(len(raw), 1024)
        self.assertNotIn(b"private-oversized-canary", raw)


if __name__ == "__main__":
    unittest.main()
