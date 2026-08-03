from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.agent import (  # noqa: E402
    AgentExecutionError,
    ConstrainedAgentTools,
    DelegationContext,
    RecallAgentService,
    service_from_env,
)
from recall_server.agent_pi import (  # noqa: E402
    MODEL_PROXY_PLACEHOLDER_KEY,
    PROTOCOL,
    PiRunner,
    ProviderKey,
    SubprocessPiTransport,
    _load_provider_key,
    _tool_definitions,
)
from recall_server.federation import SOURCE_FAMILIES  # noqa: E402


TENANT = "tenant:synthetic:company"
PRINCIPAL = "principal:synthetic:member"
SOURCE = "source:synthetic:company"
HINT = f"recall://{SOURCE}/hint?rev=1#item=0"
DECISION = f"recall://{SOURCE}/decision?rev=1#item=0"
IMPLEMENTATION = f"recall://{SOURCE}/implementation?rev=1#item=0"
REQUEST = {
    "contract": "recall.agent-request.v1",
    "schema_version": 1,
    "request_id": "req_0123456789abcdef",
    "idempotency_key": "synthetic-pi-1",
    "question": "What changed in Project Aurora during July 23?",
    "depth": "deep",
    "since": "2026-07-23T00:00:00Z",
    "until": "2026-07-24T00:00:00Z",
}


def principal() -> dict:
    return {
        "credential_kind": "mcp",
        "tenant_id": TENANT,
        "principal_id": PRINCIPAL,
        "role": "member",
        "audience": "recall-mcp",
        "authorized_sources": [SOURCE],
    }


class SyntheticRetrieval:
    """Multi-document corpus with an ingest-time decoy."""

    def __init__(self, *, fail_deep: bool = False):
        self.calls: list[str] = []
        self.filters: list[dict] = []
        self.fail_deep = fail_deep

    def passage_hints(self, query, *, filters, limit):
        self.calls.append("recall_hints")
        self.filters.append(dict(filters))
        return {
            "results": [
                {
                    "source_id": SOURCE,
                    "logical_document_id": (
                        "ldoc_0123456789abcdef0123456789abcdef"
                    ),
                    "matching_ranges": [
                        {
                            "text": "Aurora bridge decision",
                            "receipts": [HINT],
                            "passage_ordinal": 12,
                            "spans": [{
                                "record_ordinal": 80,
                                "record_count": 4,
                                "source_byte_start": 10,
                            }],
                        }
                    ],
                },
                {
                    "source_id": SOURCE,
                    "logical_document_id": (
                        "ldoc_fedcba9876543210fedcba9876543210"
                    ),
                    "matching_ranges": [
                        {
                            "text": "Grounding verification",
                            "receipts": [IMPLEMENTATION],
                        }
                    ],
                },
            ][:limit],
            "diagnostics": {"engine": "synthetic-voyage-hybrid"},
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
        self.calls.append("recall_exec")
        if self.fail_deep:
            raise RuntimeError("synthetic provider body must not escape")
        return {
            "provider": "synthetic-archil",
            "stdout": (
                "Decision: selected the bounded agent bridge "
                f"{DECISION}\n"
                "Verification: grounding check passed "
                f"{IMPLEMENTATION}\n"
            ),
            "stderr": "",
            "exit_code": 0,
            "complete": True,
            "stopped_reason": "completed",
            "opened_receipts": [DECISION, IMPLEMENTATION],
            "timing": {"totalMs": 80, "queueMs": 10, "executeMs": 70},
        }

    def find_documents(self, **arguments):
        self.calls.append("recall_find")
        return {
            "provider": "synthetic-archil",
            "matches": [{
                "document_alias": next(iter(
                    arguments["document_aliases"].values()
                )),
                "record_ordinal": 80,
                "occurred_at": "2026-07-23T00:00:00Z",
                "content": '{"message":"bounded agent bridge selected"}',
                "receipts": [DECISION],
            }],
            "opened_receipts": [DECISION],
            "complete": True,
        }

    def open_document(self, **arguments):
        self.calls.append("recall_open")
        return {
            "provider": "synthetic-archil",
            "document_alias": arguments["document_alias"],
            "records": [{
                "document_alias": arguments["document_alias"],
                "record_ordinal": 80,
                "occurred_at": "2026-07-23T00:00:00Z",
                "content": '{"message":"bounded agent bridge selected"}',
                "content_start": 0,
                "content_end": 43,
                "content_length": 43,
                "content_byte_start": 0,
                "content_byte_end": 43,
                "content_length_bytes": 43,
                "content_complete": True,
                "receipts": [DECISION],
            }],
            "opened_receipts": [DECISION],
            "next_cursor": None,
            "complete": True,
        }


class ScriptedTransport:
    def __init__(self, script):
        self.script = script
        self.start = None

    def run(self, start, invoke, *, timeout_seconds):
        self.start = start
        for name, arguments in self.script:
            invoke(name, arguments)
        return {
            "terminal": {
                "status": "complete",
                "model_attestation": {
                    "model_alias": "gemma-4-31b",
                    "route_kind": "private_broker",
                    "provider": "broker",
                    "route_identity": "10.23.45.67",
                },
            },
            "usage": {},
        }



def success_script():
    filters = {
        "since": REQUEST["since"],
        "until": REQUEST["until"],
        "source_family": None,
        "source_connector": None,
    }
    return [
        (
            "search",
            {
                "query": "Project Aurora bridge decision",
                "filters": filters,
                "limit": 8,
            },
        ),
        (
            "search",
            {
                "query": "Project Aurora grounding verification",
                "filters": filters,
                "limit": 8,
            },
        ),
        (
            "exec",
            {
                "program": (
                    "rg -n 'bounded agent bridge|grounding check' "
                    "/docs/d1 /docs/d2"
                ),
                "timeout_seconds": 20,
            },
        ),
        (
            "finish",
            {
                "status": "complete",
                "answer": (
                    "On July 23, Aurora selected the bounded agent bridge and "
                    "then passed its receipt-grounding check."
                ),
                "claims": [
                    {
                        "statement": "The bounded bridge was selected on July 23.",
                        "receipts": [DECISION],
                    },
                    {
                        "statement": "Its grounding check passed later that day.",
                        "receipts": [IMPLEMENTATION],
                    },
                ],
                "citations": [DECISION, IMPLEMENTATION],
                "gaps": [],
            },
        ),
    ]



def service(transport) -> RecallAgentService:
    fixed = datetime(2026, 7, 25, 10, 0, tzinfo=timezone.utc)
    ticks = iter([10.0, 10.05, 10.25])
    return RecallAgentService(
        PiRunner(transport),
        clock=lambda: fixed,
        monotonic=lambda: next(ticks),
    )


class SimpleAgentKernelTest(unittest.TestCase):
    def test_blank_optional_hint_filters_are_normalized_to_absent(self):
        self.assertEqual(
            PiRunner._authorize_hint_arguments(
                {
                    "query": "project context",
                    "filters": {
                        "since": "",
                        "until": " ",
                        "source_family": "",
                        "source_connector": "",
                    },
                    "limit": 10,
                },
                REQUEST,
            ),
            {
                "query": "project context",
                "filters": {
                    "since": REQUEST["since"],
                    "until": REQUEST["until"],
                },
                "limit": 10,
            },
        )

    def test_two_agent_chosen_queries_exec_and_grounded_finish(self):
        transport = ScriptedTransport(success_script())
        retrieval = SyntheticRetrieval()
        bundle = service(transport).use_recall(
            principal(),
            REQUEST,
            retrieval,
        )
        self.assertEqual(
            retrieval.calls,
            [
                "recall_hints",
                "recall_hints",
                "recall_hints",
                "recall_exec",
            ],
        )
        self.assertEqual(bundle["result"]["status"], "complete")
        self.assertEqual(
            bundle["result"]["citations"],
            [DECISION, IMPLEMENTATION],
        )
        self.assertEqual(
            [tool["name"] for tool in transport.start["data"]["tools"]],
            ["search", "find", "open", "exec", "finish"],
        )
        hint_tool = next(
            tool
            for tool in transport.start["data"]["tools"]
            if tool["name"] == "search"
        )
        find_tool = next(
            tool
            for tool in transport.start["data"]["tools"]
            if tool["name"] == "find"
        )
        open_tool = next(
            tool
            for tool in transport.start["data"]["tools"]
            if tool["name"] == "open"
        )
        self.assertIn(
            "literal",
            find_tool["description"],
        )
        self.assertIn("actual match", find_tool["description"])
        self.assertIn("record_ordinal", open_tool["description"])
        self.assertIn(
            "record_ordinal",
            open_tool["input_schema"]["properties"],
        )
        self.assertEqual(
            open_tool["input_schema"]["properties"]["page_bytes"]["maximum"],
            32_768,
        )
        family_schema = hint_tool["input_schema"]["properties"]["filters"][
            "properties"
        ]["source_family"]["anyOf"][0]
        self.assertEqual(
            set(family_schema["enum"]),
            set(SOURCE_FAMILIES),
        )
        connector_schema = hint_tool["input_schema"]["properties"]["filters"][
            "properties"
        ]["source_connector"]["anyOf"][0]
        self.assertIn("pattern", connector_schema)
        self.assertEqual(
            [event["stage"] for event in bundle["trace"]][2:6],
            ["retrieve", "retrieve", "retrieve", "inspect"],
        )
        system = transport.start["data"]["prompt_sections"][0]["content"]
        self.assertIn("/docs/dN", system)
        self.assertIn("literal match-centered search", system)
        self.assertNotIn("classify the question", system)
        self.assertNotIn("map_reduce", system)
        seed_packet = json.loads(
            transport.start["data"]["prompt_sections"][2]["content"]
        )
        self.assertEqual(seed_packet["query_basis"], "verbatim_user_question")
        self.assertFalse(seed_packet["evidence"])
        self.assertEqual(len(seed_packet["results"]), 2)
        self.assertEqual(seed_packet["results"][0]["alias"], "d1")
        self.assertNotIn(
            "logical_document_id",
            seed_packet["results"][0],
        )
        self.assertEqual(
            seed_packet["results"][0]["matching_ranges"][0]["spans"],
            [{"record_ordinal": 80, "record_count": 4}],
        )
        self.assertEqual(
            seed_packet["results"][0]["matching_ranges"][0][
                "routing_receipts"
            ],
            [HINT],
        )
        self.assertNotIn(
            "manifest_object_key",
            json.dumps(seed_packet),
        )
        for tool in transport.start["data"]["tools"]:
            stack = [tool["input_schema"]]
            while stack:
                schema = stack.pop()
                if not isinstance(schema, dict):
                    continue
                if schema.get("type") == "object":
                    self.assertIs(schema.get("additionalProperties"), False)
                    self.assertEqual(
                        set(schema["properties"]),
                        set(schema["required"]),
                    )
                    stack.extend(schema["properties"].values())
                if schema.get("type") == "array":
                    stack.append(schema["items"])
                stack.extend(schema.get("anyOf", []))

    def test_explicit_scope_is_a_host_ceiling(self):
        script = success_script()
        script[0][1]["filters"] = {
            "since": "2020-01-01T00:00:00Z",
            "until": "2030-01-01T00:00:00Z",
            "source_family": None,
            "source_connector": None,
        }
        retrieval = SyntheticRetrieval()
        service(ScriptedTransport(script)).use_recall(
            principal(),
            REQUEST,
            retrieval,
        )
        self.assertEqual(
            retrieval.filters[0],
            {"since": REQUEST["since"], "until": REQUEST["until"]},
        )

    def test_exec_can_use_host_seed_hints(self):
        script = success_script()[2:]
        retrieval = SyntheticRetrieval()
        service(ScriptedTransport(script)).use_recall(
            principal(),
            REQUEST,
            retrieval,
        )
        self.assertEqual(retrieval.calls, ["recall_hints", "recall_exec"])

    def test_find_can_use_host_seed_hints_and_ground_finish(self):
        script = [
            (
                "find",
                {
                    "aliases": ["d1"],
                    "patterns": ["bounded agent bridge"],
                    "context_chars": 800,
                    "limit": 6,
                },
            ),
            (
                "finish",
                {
                    "status": "complete",
                    "answer": "Aurora selected the bounded bridge.",
                    "claims": [{
                        "statement": "Aurora selected the bounded bridge.",
                        "receipts": [DECISION],
                    }],
                    "citations": [DECISION],
                    "gaps": [],
                },
            ),
        ]
        retrieval = SyntheticRetrieval()
        result = service(ScriptedTransport(script)).use_recall(
            principal(),
            REQUEST,
            retrieval,
        )
        self.assertEqual(
            retrieval.calls,
            ["recall_hints", "recall_find"],
        )
        self.assertEqual(result["result"]["citations"], [DECISION])

    def test_hints_are_not_citable(self):
        script = success_script()
        script.pop(2)
        script[-1] = (
            "finish",
            {
                "status": "complete",
                "answer": "A hint is not proof.",
                "claims": [{"statement": "Hint claim", "receipts": [HINT]}],
                "citations": [HINT],
                "gaps": [],
            },
        )
        with self.assertRaises(AgentExecutionError) as caught:
            service(ScriptedTransport(script)).use_recall(
                principal(),
                REQUEST,
                SyntheticRetrieval(),
            )
        self.assertEqual(caught.exception.code, "agent_citation_not_opened")

    def test_provider_failure_is_content_free(self):
        with self.assertRaises(AgentExecutionError) as caught:
            service(ScriptedTransport(success_script())).use_recall(
                principal(),
                REQUEST,
                SyntheticRetrieval(fail_deep=True),
            )
        self.assertEqual(caught.exception.code, "agent_evidence_tool_failed")
        self.assertNotIn("synthetic provider body", str(caught.exception))

    def test_cross_source_exec_receipts_fail_closed_two_hundred_out_of_two_hundred(
        self,
    ):
        class CrossSourceRetrieval(SyntheticRetrieval):
            def __init__(self, index):
                super().__init__()
                self.index = index

            def execute_agent_program(self, *args, **kwargs):
                receipt = (
                    "recall://source:foreign:"
                    f"{self.index}/private?rev=1#item=0"
                )
                return {
                    "stdout": receipt,
                    "opened_receipts": [receipt],
                    "complete": True,
                }

        for index in range(200):
            with self.subTest(index=index):
                with self.assertRaises(AgentExecutionError) as caught:
                    service(ScriptedTransport(success_script())).use_recall(
                        principal(),
                        {
                            **REQUEST,
                            "request_id": f"req_{index:016x}",
                            "idempotency_key": f"cross-source-{index}",
                        },
                        CrossSourceRetrieval(index),
                    )
                self.assertEqual(
                    caught.exception.code,
                    "agent_evidence_scope_violation",
                )

    def test_missing_finish_and_post_finish_calls_fail_closed(self):
        with self.assertRaises(AgentExecutionError) as missing:
            service(ScriptedTransport(success_script()[:-1])).use_recall(
                principal(),
                REQUEST,
                SyntheticRetrieval(),
            )
        self.assertEqual(missing.exception.code, "agent_finish_missing")
        with self.assertRaises(AgentExecutionError) as post:
            service(
                ScriptedTransport(
                    success_script()
                    + [(
                        "exec",
                        {"program": "true", "timeout_seconds": 1},
                    )]
                )
            ).use_recall(principal(), REQUEST, SyntheticRetrieval())
        self.assertEqual(post.exception.code, "agent_post_finish_tool_call")


class PiSubprocessBoundaryTest(unittest.TestCase):
    @unittest.skipUnless(
        (SERVER / "pi-agent" / "dist" / "worker.js").is_file(),
        "build server/pi-agent before the direct worker integration test",
    )
    def test_direct_pi_worker_runs_open_then_grounded_finish(self):
        calls = []

        class ModelHandler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                return

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                json.loads(self.rfile.read(length))
                index = len(calls)
                calls.append(self.path)
                if index == 0:
                    name = "open"
                    arguments = {
                        "alias": "d1",
                        "cursor": None,
                        "record_ordinal": 80,
                        "page_bytes": 32768,
                    }
                else:
                    name = "finish"
                    arguments = {
                        "status": "complete",
                        "answer": "Aurora selected the bounded bridge.",
                        "claims": [{
                            "statement": "Aurora selected the bounded bridge.",
                            "receipts": [DECISION],
                        }],
                        "citations": [DECISION],
                        "gaps": [],
                    }
                chunks = [
                    {
                        "id": f"chatcmpl-{index}",
                        "object": "chat.completion.chunk",
                        "created": 1,
                        "model": "gemma-4-31b",
                        "choices": [{
                            "index": 0,
                            "delta": {
                                "role": "assistant",
                                "tool_calls": [{
                                    "index": 0,
                                    "id": f"call-{index}",
                                    "type": "function",
                                    "function": {
                                        "name": name,
                                        "arguments": json.dumps(arguments),
                                    },
                                }],
                            },
                            "finish_reason": None,
                        }],
                    },
                    {
                        "id": f"chatcmpl-{index}",
                        "object": "chat.completion.chunk",
                        "created": 1,
                        "model": "gemma-4-31b",
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": "tool_calls",
                        }],
                    },
                ]
                body = "".join(
                    f"data: {json.dumps(chunk)}\n\n" for chunk in chunks
                ) + "data: [DONE]\n\n"
                encoded = body.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        server = ThreadingHTTPServer(("127.0.0.1", 0), ModelHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            worker = SERVER / "pi-agent" / "dist" / "worker.js"
            transport = SubprocessPiTransport(
                ("node", str(worker)),
                model_base_url=f"http://127.0.0.1:{server.server_port}",
                route_kind="private_broker",
                provider="broker",
                expected_route_identity="127.0.0.1",
                environment={"PATH": os.environ["PATH"]},
            )
            result = RecallAgentService(PiRunner(transport)).use_recall(
                principal(),
                REQUEST,
                SyntheticRetrieval(),
            )
        finally:
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()
        self.assertEqual(result["result"]["status"], "complete")
        self.assertEqual(result["result"]["citations"], [DECISION])
        self.assertEqual(calls, ["/v1/chat/completions", "/v1/chat/completions"])

    def test_render_managed_secret_symlink_is_narrowly_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "managed-secrets"
            root.mkdir(mode=0o700)
            target = Path(directory) / "runtime-secret"
            target.write_text("synthetic-provider-key-value\n")
            target.chmod(0o640)
            link = root / "cerebras-api-key"
            link.symlink_to(target)

            with patch(
                "recall_server.agent_pi.os.getuid",
                return_value=os.getuid() + 10_000,
            ):
                key = _load_provider_key(
                    str(link),
                    _managed_secret_root=root,
                    _managed_secret_group=os.getgid(),
                )
            self.assertNotIn("synthetic-provider-key-value", repr(key))

            with self.assertRaisesRegex(RuntimeError, "not private"):
                _load_provider_key(
                    str(link),
                    _managed_secret_root=root,
                    _managed_secret_group=os.getgid(),
                )

            root.chmod(0o777)
            with patch(
                "recall_server.agent_pi.os.getuid",
                return_value=os.getuid() + 10_000,
            ):
                key = _load_provider_key(
                    str(link),
                    _managed_secret_root=root,
                    _managed_secret_group=os.getgid(),
                )
            self.assertNotIn("synthetic-provider-key-value", repr(key))

    def test_unmanaged_provider_key_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            managed_root = Path(directory) / "managed-secrets"
            managed_root.mkdir(mode=0o700)
            target = Path(directory) / "runtime-secret"
            target.write_text("synthetic-provider-key-value\n")
            target.chmod(0o600)
            link = Path(directory) / "cerebras-api-key"
            link.symlink_to(target)
            with self.assertRaisesRegex(RuntimeError, "not private"):
                _load_provider_key(
                    str(link),
                    _managed_secret_root=managed_root,
                )

    def _terminal_child(self, *, route_kind, provider, identity):
        return f'''\
import json,os,sys
assert os.environ["RECALL_PI_API_KEY"]
assert os.environ["RECALL_PI_ROUTE_KIND"]=="{route_kind}"
assert os.environ["RECALL_PI_PROVIDER"]=="{provider}"
start=json.loads(sys.stdin.readline())
print(json.dumps({{"v":"{PROTOCOL}","turn_id":start["turn_id"],"seq":0,"type":"terminal.complete","at":"2026-07-25T10:00:00Z","data":{{"status":"complete","unresolved_call_ids":[],"model_attestation":{{"model_alias":"gemma-4-31b","route_kind":"{route_kind}","provider":"{provider}","route_identity":"{identity}"}}}}}}),flush=True)
'''

    def test_private_broker_passes_only_placeholder_and_no_ambient_secret(self):
        transport = SubprocessPiTransport(
            (sys.executable, "-c", self._terminal_child(
                route_kind="private_broker",
                provider="broker",
                identity="10.23.45.67",
            )),
            model_base_url="http://10.23.45.67:9420",
            route_kind="private_broker",
            provider="broker",
            expected_route_identity="10.23.45.67",
            environment={"PATH": os.environ["PATH"], "FORBIDDEN_SECRET": "no"},
        )
        outcome = transport.run(
            {"turn_id": "turn_broker", "data": {"model": {"alias": "gemma-4-31b"}}},
            lambda *_args: {},
            timeout_seconds=3,
        )
        self.assertEqual(outcome["terminal"]["model_attestation"]["route_kind"], "private_broker")
        self.assertNotIn("FORBIDDEN_SECRET", transport.child_environment)
        self.assertEqual(MODEL_PROXY_PLACEHOLDER_KEY, "not-a-secret")

    def test_direct_openai_compatible_route_is_attested(self):
        key = ProviderKey(value="synthetic-provider-key-value")
        transport = SubprocessPiTransport(
            (sys.executable, "-c", self._terminal_child(
                route_kind="direct_provider",
                provider="openai-compatible",
                identity="api.cerebras.ai",
            )),
            model_base_url="https://api.cerebras.ai/v1",
            route_kind="direct_provider",
            provider="openai-compatible",
            provider_key=key,
            expected_route_identity="api.cerebras.ai",
            environment={"PATH": os.environ["PATH"], "FORBIDDEN_SECRET": "no"},
        )
        outcome = transport.run(
            {"turn_id": "turn_direct", "data": {"model": {"alias": "gemma-4-31b"}}},
            lambda *_args: {},
            timeout_seconds=3,
        )
        self.assertEqual(outcome["terminal"]["model_attestation"]["provider"], "openai-compatible")
        self.assertNotIn("synthetic-provider-key-value", repr(key))

    def test_transport_rejects_public_keyless_and_malformed_children(self):
        with self.assertRaisesRegex(RuntimeError, "must be private"):
            SubprocessPiTransport(
                (sys.executable, "-c", "pass"),
                model_base_url="https://litellm.example/v1",
                route_kind="private_broker",
                provider="broker",
                expected_route_identity="litellm.example",
            )
        transport = SubprocessPiTransport(
            (sys.executable, "-c", "print('not-json',flush=True)"),
            model_base_url="https://api.cerebras.ai/v1",
            route_kind="direct_provider",
            provider="openai-compatible",
            provider_key=ProviderKey(value="synthetic-provider-key-value"),
            expected_route_identity="api.cerebras.ai",
            environment={"PATH": os.environ["PATH"]},
        )
        with self.assertRaises(AgentExecutionError) as caught:
            transport.run(
                {"turn_id": "turn_bad", "data": {"model": {"alias": "gemma-4-31b"}}},
                lambda *_args: {},
                timeout_seconds=1,
            )
        self.assertEqual(caught.exception.code, "agent_transport_frame_invalid")

    def test_configuration_has_one_pi_runner_and_private_secret_file(self):
        with tempfile.TemporaryDirectory() as directory:
            key_path = Path(directory) / "model.key"
            key_path.write_text("synthetic-provider-key-value\n")
            key_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
            direct = {
                "RECALL_AGENT_RUNNER": "pi",
                "RECALL_AGENT_MODEL_BASE_URL": "https://api.cerebras.ai/v1",
                "RECALL_AGENT_MODEL_KEY_FILE": str(key_path),
                "RECALL_AGENT_MODEL_ALIAS": "gpt-oss-120b",
            }
            self.assertIsInstance(service_from_env(direct).runner, PiRunner)
            key_path.write_text("contains whitespace")
            with self.assertRaisesRegex(RuntimeError, "invalid"):
                service_from_env(direct)
            key_path.write_text("synthetic-provider-key-value\n")
            key_path.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "not private"):
                service_from_env(direct)

            broker = {
                "RECALL_AGENT_RUNNER": "pi",
                "RECALL_AGENT_MODEL_BASE_URL": "http://10.23.45.67:9420",
                "RECALL_AGENT_MODEL_ALIAS": "gemma-4-31b",
            }
            self.assertIsInstance(service_from_env(broker).runner, PiRunner)
            broker["RECALL_AGENT_MODEL_BASE_URL"] = "https://litellm.example/v1"
            with self.assertRaisesRegex(RuntimeError, "must be private"):
                service_from_env(broker)


if __name__ == "__main__":
    unittest.main()
