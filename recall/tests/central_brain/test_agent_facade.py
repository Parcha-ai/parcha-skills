from __future__ import annotations

import dataclasses
import http.client
import json
import os
import sys
import threading
import unittest
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))
sys.path.insert(0, str(SERVER / "tests"))

from recall_server.agent import (  # noqa: E402
    AgentBudget,
    AgentExecutionError,
    ConstrainedAgentTools,
    DelegationContext,
    RecallAgentService,
    service_from_env,
)
from recall_server.app import Handler  # noqa: E402
from agent_fakes import ScriptedAgentRunner  # noqa: E402


TENANT = "tenant:synthetic:company"
PERSONAL_TENANT = "tenant:synthetic:personal"
PRINCIPAL = "principal:synthetic:member"
SOURCE = "source:synthetic:company"
RECEIPT = f"recall://{SOURCE}/item-1?rev=1#item=0"
REQUEST = {
    "contract": "recall.agent-request.v1",
    "schema_version": 1,
    "request_id": "req_0123456789abcdef",
    "idempotency_key": "synthetic-retry-1",
    "question": "What changed in the synthetic project?",
    "depth": "normal",
}


def principal(**extra) -> dict:
    return {
        "credential_kind": "mcp",
        "kind": "mcp",
        "name": "synthetic-member",
        "tenant_id": TENANT,
        "principal_id": PRINCIPAL,
        "principal_kind": "human",
        "role": "member",
        "audience": "recall-mcp",
        "source_id": None,
        "scopes": ["read"],
        "authorized_sources": [SOURCE],
        **extra,
    }


class FakeBoundRetrieval:
    calls: list[tuple]

    def __init__(self) -> None:
        self.calls = []

    def passage_hints(self, query, *, filters, limit):
        self.calls.append(("hints", query, filters, limit))
        return {
            "results": [{
                "source_id": SOURCE,
                "logical_document_id": (
                    "ldoc_0123456789abcdef0123456789abcdef"
                ),
                "matching_ranges": [{
                    "receipts": [RECEIPT],
                    "spans": [{
                        "record_ordinal": 11,
                        "record_count": 3,
                    }],
                }],
            }],
            "diagnostics": {"engine": "synthetic"},
        }

    def execute_agent_program(
        self,
        program,
        *,
        logical_document_ids,
        record_spans,
        routing_receipts,
        timeout_seconds,
        document_aliases=None,
    ):
        self.calls.append((
            "exec",
            program,
            logical_document_ids,
            record_spans,
            routing_receipts,
            timeout_seconds,
        ))
        return {
            "stdout": RECEIPT,
            "opened_receipts": [RECEIPT],
            "complete": True,
        }

    def find_documents(self, **arguments):
        self.calls.append(("find", arguments))
        return {
            "provider": "synthetic",
            "matches": [{
                "document_alias": next(iter(
                    arguments["document_aliases"].values()
                )),
                "record_ordinal": 11,
                "occurred_at": "2026-07-23T00:00:00Z",
                "content": '{"message":"synthetic evidence"}',
                "receipts": [RECEIPT],
            }],
            "opened_receipts": [RECEIPT],
            "complete": True,
        }

    def open_document(self, **arguments):
        self.calls.append(("open", arguments))
        return {
            "provider": "synthetic",
            "document_alias": arguments["document_alias"],
            "records": [{
                "document_alias": arguments["document_alias"],
                "record_ordinal": 11,
                "occurred_at": "2026-07-23T00:00:00Z",
                "content": '{"message":"synthetic evidence"}',
                "content_start": 0,
                "content_end": 32,
                "content_length": 32,
                "content_byte_start": 0,
                "content_byte_end": 32,
                "content_length_bytes": 32,
                "content_complete": True,
                "receipts": [RECEIPT],
            }],
            "opened_receipts": [RECEIPT],
            "next_cursor": None,
            "complete": True,
        }

    def investigate(self, question, *, filters, depth):
        self.calls.append(("investigate", question, filters, depth))
        return {
            "question_interpretation": {"time_basis": "occurred_at"},
            "investigations": [{
                "match": {
                    "source_id": SOURCE,
                    "receipt": RECEIPT,
                },
                "context": {"events": []},
            }],
            "coverage": {
                "sessions": 2,
                "sources": [SOURCE],
            },
        }

    def deep_search(self, question, *, filters, depth):
        self.calls.append(("deep_search", question, filters, depth))
        return {"findings": []}

    def session_context(self, target, *, before, after):
        self.calls.append(("session_context", target, before, after))
        return {"anchor_receipt": target, "events": []}

    def show(self, target):
        self.calls.append(("show", target))
        return {"resolved_receipt": target}


class FakeCanonicalRetrieval:
    def __init__(self) -> None:
        self.bound = FakeBoundRetrieval()
        self.principals: list[dict] = []

    def bind(self, value):
        self.principals.append(value)
        if value["tenant_id"] != TENANT:
            raise PermissionError("wrong tenant")
        return self.bound


class FakeStore:
    def __init__(self) -> None:
        self.audit: list[dict] = []

    def authenticate_bearer(self, token, scope):
        if token != "synthetic-agent-token" or scope != "read":
            return None
        value = principal()
        value.pop("authorized_sources")
        return value

    def authorized_canonical_source_ids(self, tenant_id, principal_id):
        if (tenant_id, principal_id) != (TENANT, PRINCIPAL):
            return []
        return [SOURCE]

    def record_authorization_event(
        self,
        principal_value,
        *,
        action,
        allowed,
        reason,
        policy_version,
    ):
        self.audit.append({
            "tenant_id": principal_value["tenant_id"],
            "action": action,
            "allowed": allowed,
            "reason": reason,
            "policy_version": policy_version,
        })


class MutatingRunner:
    def run(self, request, context, tools, *, clock, monotonic):
        bundle = ScriptedAgentRunner().run(
            request,
            context,
            tools,
            clock=clock,
            monotonic=monotonic,
        )
        personal = "recall://source:synthetic:personal/item-1?rev=1#item=0"
        bundle["result"]["citations"] = [personal]
        bundle["result"]["claims"][0]["receipts"] = [personal]
        for event in bundle["trace"][1:]:
            event["receipts"] = [personal]
        return bundle


class CapturingRunner:
    def __init__(self) -> None:
        self.context = None

    def run(self, request, context, tools, *, clock, monotonic):
        self.context = context
        return ScriptedAgentRunner().run(
            request,
            context,
            tools,
            clock=clock,
            monotonic=monotonic,
        )


def service(runner=None) -> RecallAgentService:
    fixed = datetime(2026, 7, 25, 10, 0, tzinfo=timezone.utc)
    return RecallAgentService(
        runner or ScriptedAgentRunner(),
        clock=lambda: fixed,
        monotonic=lambda: 10.0,
    )


class AgentFacadeUnitTest(unittest.TestCase):
    def test_default_agent_deadline_allows_bounded_multi_step_retrieval(self) -> None:
        self.assertEqual(AgentBudget().deadline_seconds, 120)

    def test_runner_configuration_is_explicit_and_fail_closed(self) -> None:
        self.assertIsNone(service_from_env({}))
        with self.assertRaisesRegex(RuntimeError, "unsupported"):
            service_from_env({"RECALL_AGENT_RUNNER": "scripted"})
        with self.assertRaisesRegex(RuntimeError, "unsupported"):
            service_from_env({"RECALL_AGENT_RUNNER": "unknown"})

    def test_delegation_context_is_host_owned_frozen_and_credential_free(self) -> None:
        runner = CapturingRunner()
        value = service(runner).use_recall(
            principal(
                token="forbidden-credential-canary",
                brain_id=PERSONAL_TENANT,
            ),
            REQUEST,
            FakeBoundRetrieval(),
        )
        self.assertEqual(value["result"]["citations"], [RECEIPT])
        self.assertEqual(runner.context.tenant_id, TENANT)
        self.assertEqual(runner.context.authorized_sources, (SOURCE,))
        rendered = repr(runner.context)
        self.assertNotIn("forbidden-credential-canary", rendered)
        self.assertNotIn("brain_id", rendered)
        with self.assertRaisesRegex((
            AttributeError,
            dataclasses.FrozenInstanceError,
        ), ""):
            runner.context.tenant_id = PERSONAL_TENANT

    def test_child_catalog_excludes_recursion_credentials_and_infrastructure(self) -> None:
        context = DelegationContext.from_principal(principal())
        tools = ConstrainedAgentTools(FakeBoundRetrieval(), context)
        names = {item["name"] for item in tools.catalog}
        self.assertEqual(names, set(context.allowed_tools))
        for forbidden in (
            "use_recall",
            "recall.capture",
            "recall.credentials",
            "archil.execute",
            "shell",
        ):
            self.assertNotIn(forbidden, names)
            with self.assertRaisesRegex(AgentExecutionError, "not authorized"):
                tools.call(forbidden, {})

    def test_cross_brain_receipt_invention_fails_closed(self) -> None:
        with self.assertRaisesRegex(
            AgentExecutionError,
            "output failed validation",
        ):
            service(MutatingRunner()).use_recall(
                principal(),
                REQUEST,
                FakeBoundRetrieval(),
            )

    def test_host_owned_tool_budget_is_enforced(self) -> None:
        context = dataclasses.replace(
            DelegationContext.from_principal(principal()),
            budget=AgentBudget(max_tool_calls=2),
        )
        tools = ConstrainedAgentTools(FakeBoundRetrieval(), context)
        for _ in range(context.budget.max_tool_calls):
            tools.call("recall.hints", {
                "query": REQUEST["question"],
                "filters": {},
                "limit": 1,
            })
        with self.assertRaisesRegex(AgentExecutionError, "budget is exhausted"):
            tools.call("recall.hints", {
                "query": REQUEST["question"],
                "filters": {},
                "limit": 1,
            })

    def test_expensive_tool_budgets_are_enforced_per_tool(self) -> None:
        context = DelegationContext.from_principal(principal())
        tools = ConstrainedAgentTools(FakeBoundRetrieval(), context)
        arguments = {
            "query": REQUEST["question"],
            "filters": {},
            "limit": 1,
        }
        for _ in range(6):
            tools.call("recall.hints", arguments)
        with self.assertRaises(AgentExecutionError) as caught:
            tools.call("recall.hints", arguments)
        self.assertEqual(
            caught.exception.code,
            "agent_tool_budget_exhausted",
        )
        self.assertEqual(tools.observations[-1]["outcome"], "failed")

    def test_exec_requires_an_admitted_hint_document(self) -> None:
        tools = ConstrainedAgentTools(
            FakeBoundRetrieval(),
            DelegationContext.from_principal(principal()),
        )
        with self.assertRaises(AgentExecutionError) as caught:
            tools.call("recall.exec", {
                "program": "rg synthetic /docs/d1",
                "timeout_seconds": 10,
            })
        self.assertEqual(caught.exception.code, "agent_exec_without_hints")

    def test_find_uses_stable_alias_and_full_document_scope(self) -> None:
        retrieval = FakeBoundRetrieval()
        tools = ConstrainedAgentTools(
            retrieval,
            DelegationContext.from_principal(principal()),
        )
        document_id = "ldoc_0123456789abcdef0123456789abcdef"
        tools.call("recall.hints", {
            "query": REQUEST["question"],
            "filters": {},
            "limit": 1,
        })
        result = tools.call("recall.find", {
            "aliases": ["d1"],
            "patterns": ["synthetic evidence"],
            "context_chars": 800,
            "limit": 6,
        })
        self.assertEqual(result["opened_receipts"], [RECEIPT])
        self.assertEqual(tools.citable_receipts, (RECEIPT,))
        call = retrieval.calls[-1]
        self.assertEqual(call[0], "find")
        self.assertEqual(
            call[1]["record_spans"],
            {document_id: ((11, 3),)},
        )
        self.assertEqual(
            call[1]["routing_receipts"],
            {document_id: (RECEIPT,)},
        )
        self.assertEqual(
            call[1]["document_aliases"],
            {document_id: "d1"},
        )

    def test_open_can_address_any_hinted_record_ordinal(self) -> None:
        retrieval = FakeBoundRetrieval()
        tools = ConstrainedAgentTools(
            retrieval,
            DelegationContext.from_principal(principal()),
        )
        tools.call("recall.hints", {
            "query": REQUEST["question"],
            "filters": {},
            "limit": 1,
        })
        result = tools.call("recall.open", {
            "alias": "d1",
            "cursor": None,
            "record_ordinal": 13,
            "page_bytes": 32_768,
        })
        self.assertEqual(result["opened_receipts"], [RECEIPT])
        call = retrieval.calls[-1]
        self.assertEqual(call[0], "open")
        self.assertEqual(call[1]["record_ordinal"], 13)
        self.assertEqual(call[1]["page_bytes"], 32_768)
        self.assertIsNone(call[1]["cursor"])

    def test_open_rejects_cursor_with_record_ordinal(self) -> None:
        tools = ConstrainedAgentTools(
            FakeBoundRetrieval(),
            DelegationContext.from_principal(principal()),
        )
        tools.call("recall.hints", {
            "query": REQUEST["question"],
            "filters": {},
            "limit": 1,
        })
        with self.assertRaises(AgentExecutionError) as caught:
            tools.call("recall.open", {
                "alias": "d1",
                "cursor": "0:0:0",
                "record_ordinal": 13,
                "page_bytes": 4_000,
            })
        self.assertEqual(caught.exception.code, "agent_open_invalid")

    def test_find_rejects_two_hundred_unadmitted_aliases(self) -> None:
        for index in range(200):
            with self.subTest(index=index):
                tools = ConstrainedAgentTools(
                    FakeBoundRetrieval(),
                    DelegationContext.from_principal(principal()),
                )
                tools.call("recall.hints", {
                    "query": REQUEST["question"],
                    "filters": {},
                    "limit": 1,
                })
                invented = f"d{index + 2}"
                with self.assertRaises(AgentExecutionError) as caught:
                    tools.call("recall.find", {
                        "aliases": [invented],
                        "patterns": ["synthetic"],
                        "context_chars": 800,
                        "limit": 1,
                    })
                self.assertEqual(
                    caught.exception.code,
                    "agent_find_invalid",
                )

    def test_find_rejects_two_hundred_cross_source_receipts(self) -> None:
        class CrossSourceFind(FakeBoundRetrieval):
            def __init__(self, index):
                super().__init__()
                self.index = index

            def find_documents(self, **arguments):
                receipt = (
                    "recall://source:foreign:"
                    f"{self.index}/private?rev=1#item=0"
                )
                return {
                    "matches": [],
                    "opened_receipts": [receipt],
                    "complete": True,
                }

        for index in range(200):
            with self.subTest(index=index):
                tools = ConstrainedAgentTools(
                    CrossSourceFind(index),
                    DelegationContext.from_principal(principal()),
                )
                tools.call("recall.hints", {
                    "query": REQUEST["question"],
                    "filters": {},
                    "limit": 1,
                })
                with self.assertRaises(AgentExecutionError) as caught:
                    tools.call("recall.find", {
                        "aliases": ["d1"],
                        "patterns": ["synthetic"],
                        "context_chars": 800,
                        "limit": 1,
                    })
                self.assertEqual(
                    caught.exception.code,
                    "agent_evidence_scope_violation",
                )

    def test_exec_timeout_is_clamped_to_the_remaining_turn_budget(self) -> None:
        context = dataclasses.replace(
            DelegationContext.from_principal(principal()),
            budget=AgentBudget(deadline_seconds=25),
        )
        ticks = iter([100.0, 101.0, 101.0, 102.0, 108.5, 108.5, 109.0])
        retrieval = FakeBoundRetrieval()
        tools = ConstrainedAgentTools(
            retrieval,
            context,
            monotonic=lambda: next(ticks),
        )
        tools.call("recall.hints", {
            "query": REQUEST["question"],
            "filters": {},
            "limit": 1,
        })
        tools.call("recall.exec", {
            "program": "rg synthetic /docs/d1",
            "timeout_seconds": 30,
        })
        exec_call = retrieval.calls[-1]
        self.assertEqual(exec_call[0], "exec")
        self.assertEqual(
            exec_call[-3],
            {
                "ldoc_0123456789abcdef0123456789abcdef": (
                    (11, 3),
                ),
            },
        )
        self.assertEqual(
            exec_call[-2],
            {
                "ldoc_0123456789abcdef0123456789abcdef": (
                    RECEIPT,
                ),
            },
        )
        self.assertEqual(exec_call[-1], 10)

    def test_exec_fails_closed_when_only_finish_reserve_remains(self) -> None:
        context = dataclasses.replace(
            DelegationContext.from_principal(principal()),
            budget=AgentBudget(deadline_seconds=10),
        )
        ticks = iter([100.0, 101.0, 101.0, 102.0, 105.0, 105.0])
        tools = ConstrainedAgentTools(
            FakeBoundRetrieval(),
            context,
            monotonic=lambda: next(ticks),
        )
        tools.call("recall.hints", {
            "query": REQUEST["question"],
            "filters": {},
            "limit": 1,
        })
        with self.assertRaises(AgentExecutionError) as caught:
            tools.call("recall.exec", {
                "program": "rg synthetic /docs/d1",
                "timeout_seconds": 30,
            })
        self.assertEqual(
            caught.exception.code,
            "agent_tool_deadline_exhausted",
        )

    def test_host_owned_receipt_budget_is_enforced(self) -> None:
        class ManyReceipts(FakeBoundRetrieval):
            def execute_agent_program(self, *args, **kwargs):
                return {
                    "opened_receipts": [
                        RECEIPT,
                        f"recall://{SOURCE}/item-2?rev=1#item=0",
                    ]
                }

        context = dataclasses.replace(
            DelegationContext.from_principal(principal()),
            budget=AgentBudget(max_receipts=1),
        )
        tools = ConstrainedAgentTools(ManyReceipts(), context)
        tools.call("recall.hints", {
            "query": REQUEST["question"],
            "filters": {},
            "limit": 1,
        })
        with self.assertRaises(AgentExecutionError) as caught:
            tools.call("recall.exec", {
                "program": "rg synthetic /docs/d1",
                "timeout_seconds": 10,
            })
        self.assertEqual(
            caught.exception.code,
            "agent_receipt_budget_exhausted",
        )

    def test_cumulative_tool_output_budget_is_enforced(self) -> None:
        class LargeResult(FakeBoundRetrieval):
            def execute_agent_program(self, *args, **kwargs):
                return {
                    "opened_receipts": [RECEIPT],
                    "text": "x" * 200,
                }

        context = dataclasses.replace(
            DelegationContext.from_principal(principal()),
            budget=AgentBudget(max_tool_output_bytes=700),
        )
        tools = ConstrainedAgentTools(LargeResult(), context)
        tools.call("recall.hints", {
            "query": REQUEST["question"],
            "filters": {},
            "limit": 1,
        })
        arguments = {
            "program": "rg synthetic /docs/d1",
            "timeout_seconds": 10,
        }
        tools.call("recall.exec", arguments)
        with self.assertRaises(AgentExecutionError) as caught:
            tools.call("recall.exec", arguments)
        self.assertEqual(
            caught.exception.code,
            "agent_tool_output_budget_exhausted",
        )
        self.assertEqual(tools.observations[-1]["outcome"], "failed")

    def test_scripted_result_is_receipt_backed_and_trace_is_content_free(self) -> None:
        result = service().use_recall(
            principal(),
            REQUEST,
            FakeBoundRetrieval(),
        )
        self.assertEqual(result["result"]["status"], "partial")
        self.assertEqual(result["result"]["citations"], [RECEIPT])
        self.assertEqual(
            [event["stage"] for event in result["trace"]],
            ["authorize", "retrieve", "verify", "complete"],
        )
        rendered = json.dumps(result["trace"])
        self.assertNotIn(REQUEST["question"], rendered)
        for forbidden in ("prompt", "answer", "payload", "token", "transcript"):
            self.assertNotIn(f'"{forbidden}"', rendered)


class AgentHttpServer:
    def __init__(self) -> None:
        self.store = FakeStore()
        self.retrieval = FakeCanonicalRetrieval()
        Handler.store = self.store
        Handler.canonical_retrieval = self.retrieval
        Handler.agent_service = service()
        Handler.agent_coordinator = None
        Handler.external_identity_verifier = None
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        Handler.canonical_retrieval = None
        Handler.agent_service = None
        Handler.agent_coordinator = None

    def request(self, path, body, *, protocol=None):
        payload = json.dumps(body).encode()
        headers = {
            "Authorization": "Bearer synthetic-agent-token",
            "Content-Type": "application/json",
            "Content-Length": str(len(payload)),
            "Accept": "application/json, text/event-stream",
        }
        if protocol is not None:
            headers["MCP-Protocol-Version"] = protocol
        connection = http.client.HTTPConnection(
            "127.0.0.1",
            self.server.server_port,
            timeout=3,
        )
        connection.request("POST", path, body=payload, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        connection.close()
        return response.status, json.loads(raw)


class AgentTransportParityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.environment = mock.patch.dict(
            os.environ,
            {
                "RECALL_AUTH_REQUIRED": "1",
                "RECALL_CANONICAL_MCP_ENABLED": "1",
                "RECALL_HTTP_PROFILE": "public-mcp",
            },
            clear=False,
        )
        self.environment.start()

    def tearDown(self) -> None:
        self.environment.stop()

    def test_http_and_mcp_execute_the_same_domain_operation(self) -> None:
        with AgentHttpServer() as server:
            http_status, http_result = server.request(
                f"/v1/agent/brains/{TENANT}/use-recall",
                REQUEST,
            )
            mcp_status, mcp_result = server.request(
                f"/mcp/brains/{TENANT}",
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "use_recall",
                        "arguments": REQUEST,
                    },
                },
                protocol="2025-11-25",
            )
            self.assertEqual((http_status, mcp_status), (200, 200))
            self.assertEqual(
                http_result,
                mcp_result["result"]["structuredContent"],
            )
            self.assertEqual(
                server.store.audit[-2:],
                [
                    {
                        "tenant_id": TENANT,
                        "action": "mcp.use_recall",
                        "allowed": True,
                        "reason": "allowed",
                        "policy_version": "recall.authorization.v1",
                    },
                    {
                        "tenant_id": TENANT,
                        "action": "mcp.use_recall",
                        "allowed": True,
                        "reason": "allowed",
                        "policy_version": "recall.authorization.v1",
                    },
                ],
            )

    def test_tools_list_exposes_facade_only_when_agent_is_configured(self) -> None:
        with AgentHttpServer() as server:
            status, result = server.request(
                f"/mcp/brains/{TENANT}",
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/list",
                    "params": {},
                },
                protocol="2025-11-25",
            )
            self.assertEqual(status, 200)
            names = {
                tool["name"]
                for tool in result["result"]["tools"]
            }
            self.assertIn("use_recall", names)
            self.assertIn("recall_investigate", names)

    def test_request_tenant_escape_and_cross_brain_url_fail_closed(self) -> None:
        with AgentHttpServer() as server:
            escaped = {**REQUEST, "tenant_id": PERSONAL_TENANT}
            status, result = server.request(
                f"/v1/agent/brains/{TENANT}/use-recall",
                escaped,
            )
            self.assertEqual(status, 400)
            self.assertEqual(result, {"error": "agent request invalid"})

            status, result = server.request(
                f"/v1/agent/brains/{PERSONAL_TENANT}/use-recall",
                REQUEST,
            )
            self.assertEqual(status, 401)
            self.assertEqual(result, {"error": "unauthorized"})


if __name__ == "__main__":
    unittest.main()
