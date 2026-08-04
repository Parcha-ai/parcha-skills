from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.agent import (  # noqa: E402
    AgentExecutionError,
    DelegationContext,
)
from recall_server.agent_runs import (  # noqa: E402
    AgentRunCoordinator,
    backend_from_env,
)
from recall_server.mcp import McpProtocolError, dispatch  # noqa: E402


RUN_ID = "run_0123456789abcdef0123456789abcdef"
TASK_ID = "tsk_0123456789abcdef0123456789abcdef"
CREATED = "2026-07-25T10:00:00Z"


def principal() -> dict:
    return {
        "credential_kind": "mcp",
        "tenant_id": "tenant:synthetic:company",
        "principal_id": "principal:synthetic:member",
        "principal_kind": "human",
        "role": "member",
        "audience": "recall-mcp",
        "scopes": ["read"],
        "authorized_sources": ["source:synthetic:company"],
        "agent_enabled": True,
    }


def run(status: str = "running") -> dict:
    value = {
        "contract": "recall.agent-run.v1",
        "schema_version": 1,
        "run_id": RUN_ID,
        "request_id": "req_0123456789abcdef",
        "tenant_id": "tenant:synthetic:company",
        "principal_id": "principal:synthetic:member",
        "trace_id": "trc_0123456789abcdef0123456789abcdef",
        "status": status,
        "attempt": 1,
        "created_at": CREATED,
        "updated_at": CREATED,
    }
    if status not in {"queued", "running"}:
        value["completed_at"] = CREATED
    if status == "failed":
        value["error_code"] = "worker_lost_retryable"
    if status == "cancelled":
        value["error_code"] = "cancelled_by_caller"
    return value


REQUEST = {
    "contract": "recall.agent-request.v1",
    "schema_version": 1,
    "request_id": "req_0123456789abcdef",
    "idempotency_key": "synthetic-lifecycle",
    "question": "What changed in the synthetic project?",
    "depth": "normal",
}


class FakeLifecycle:
    def __init__(self) -> None:
        self.status = "running"
        self.started = 0

    def callbacks(self) -> dict:
        return {
            "start": self.start,
            "status": lambda _run_id: {"run": run(self.status)},
            "result": lambda _run_id: {"answer": "synthetic"},
            "cancel": self.cancel_run,
            "task_status": lambda _task_id: {
                "run": run(self.status),
                "task_id": TASK_ID,
                "ttl_ms": 60_000,
            },
            "task_result": lambda _task_id: {"answer": "synthetic"},
            "task_cancel": self.cancel_task,
        }

    def start(self, _arguments):
        self.started += 1
        return {
            "run": run(self.status),
            "task_id": TASK_ID,
            "ttl_ms": 60_000,
        }

    def cancel_run(self, _run_id):
        self.status = "cancelled"
        return {"run": run(self.status)}

    def cancel_task(self, _task_id):
        self.status = "cancelled"
        return {
            "run": run(self.status),
            "task_id": TASK_ID,
            "ttl_ms": 60_000,
        }


def message(method: str, params: dict) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }


class McpTaskNegotiationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.lifecycle = FakeLifecycle()

    def call(self, payload, *, protocol="2026-06-30", task_name=None):
        return dispatch(
            object(),
            principal(),
            payload,
            authorize=lambda _action: True,
            agent=lambda _arguments: {"mode": "sync"},
            agent_lifecycle=self.lifecycle.callbacks(),
            protocol_version=protocol,
            task_name=task_name,
        )

    def test_current_protocol_advertises_extension_only_when_available(self) -> None:
        current = self.call(message("initialize", {
            "protocolVersion": "2026-06-30",
            "capabilities": {},
            "clientInfo": {"name": "synthetic", "version": "1"},
        }))
        self.assertEqual(
            current["result"]["capabilities"]["extensions"],
            {"io.modelcontextprotocol/tasks": {}},
        )
        legacy = self.call(
            message("initialize", {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "synthetic", "version": "1"},
            }),
            protocol="2025-11-25",
        )
        self.assertNotIn("extensions", legacy["result"]["capabilities"])

    def test_task_opt_in_is_per_request_and_durably_started(self) -> None:
        created = self.call(message("tools/call", {
            "name": "use_recall",
            "arguments": REQUEST,
            "_meta": {
                "io.modelcontextprotocol/clientCapabilities": {
                    "extensions": {"io.modelcontextprotocol/tasks": {}},
                },
            },
        }))
        self.assertEqual(created["result"]["resultType"], "task")
        self.assertEqual(created["result"]["taskId"], TASK_ID)
        self.assertEqual(created["result"]["status"], "working")
        self.assertEqual(self.lifecycle.started, 1)

    def test_unnegotiated_and_legacy_calls_fall_back_to_sync(self) -> None:
        ordinary = self.call(message("tools/call", {
            "name": "use_recall",
            "arguments": REQUEST,
        }))
        self.assertEqual(
            ordinary["result"]["structuredContent"],
            {"mode": "sync"},
        )
        legacy = self.call(
            message("tools/call", {
                "name": "use_recall",
                "arguments": REQUEST,
                "_meta": {
                    "io.modelcontextprotocol/clientCapabilities": {
                        "extensions": {"io.modelcontextprotocol/tasks": {}},
                    },
                },
            }),
            protocol="2025-11-25",
        )
        self.assertEqual(
            legacy["result"]["structuredContent"],
            {"mode": "sync"},
        )
        self.assertEqual(self.lifecycle.started, 0)

    def test_task_get_terminal_result_and_routing_header(self) -> None:
        self.lifecycle.status = "partial"
        result = self.call(
            message("tasks/get", {"taskId": TASK_ID}),
            task_name=TASK_ID,
        )
        self.assertEqual(result["result"]["status"], "completed")
        self.assertEqual(
            result["result"]["result"]["structuredContent"],
            {"answer": "synthetic"},
        )
        with self.assertRaisesRegex(McpProtocolError, "routing header"):
            self.call(
                message("tasks/get", {"taskId": TASK_ID}),
                task_name="tsk_ffffffffffffffffffffffffffffffff",
            )

    def test_task_cancel_and_explicit_compatibility_tools(self) -> None:
        cancelled = self.call(
            message("tasks/cancel", {"taskId": TASK_ID}),
            task_name=TASK_ID,
        )
        self.assertEqual(cancelled["result"], {"resultType": "complete"})
        status = self.call(message("tools/call", {
            "name": "recall_agent_status",
            "arguments": {"run_id": RUN_ID},
        }))
        self.assertEqual(
            status["result"]["structuredContent"]["run"]["status"],
            "cancelled",
        )

    def test_task_update_is_explicitly_unsupported_without_input_state(self) -> None:
        with self.assertRaisesRegex(McpProtocolError, "input is not supported"):
            self.call(
                message(
                    "tasks/update",
                    {"taskId": TASK_ID, "inputResponses": {}},
                ),
                task_name=TASK_ID,
            )


class DurableConfigurationTest(unittest.TestCase):
    class Store:
        def connect(self):
            raise AssertionError("configuration must not connect")

    def test_bounds_are_validated_before_database_io(self) -> None:
        backend = backend_from_env({}, self.Store())
        self.assertEqual(backend.max_active_per_principal, 4)
        self.assertEqual(backend.lease_seconds, 600)
        self.assertEqual(backend.retention_seconds, 604800)
        for environment in (
            {"RECALL_AGENT_MAX_ACTIVE_PER_PRINCIPAL": "0"},
            {"RECALL_AGENT_LEASE_SECONDS": "14"},
            {"RECALL_AGENT_RETENTION_SECONDS": "59"},
            {"RECALL_AGENT_RETENTION_SECONDS": "not-an-integer"},
        ):
            with self.subTest(environment=tuple(environment)):
                with self.assertRaises((RuntimeError, ValueError)):
                    backend_from_env(environment, self.Store())

    def test_schema_stores_hash_scope_state_and_bounded_outputs_not_question(self) -> None:
        schema = (
            SERVER / "schema" / "038_agent_runs.sql"
        ).read_text().casefold()
        self.assertIn("request_sha256", schema)
        self.assertIn("source_ids", schema)
        self.assertIn("lease_expires_at", schema)
        self.assertIn("pg_column_size(trace_events) <= 1048576", schema)
        self.assertNotIn("question", schema)
        self.assertNotIn("credential", schema)

    def test_coordinator_persists_safe_typed_failure_code(self) -> None:
        class Service:
            def execute(self, *_args):
                raise AgentExecutionError(
                    "provider body is not durable",
                    code="agent_model_timeout",
                    trace=[{
                        "contract": "recall.agent-trace-event.v1",
                        "schema_version": 1,
                        "trace_id": (
                            "trc_0123456789abcdef0123456789abcdef"
                        ),
                        "run_id": RUN_ID,
                        "sequence": 0,
                        "occurred_at": CREATED,
                        "stage": "complete",
                        "outcome": "failed",
                        "elapsed_ms": 100.0,
                        "receipts": [],
                        "receipt_count": 0,
                        "source_count": 0,
                        "session_count": 0,
                        "tool": "recall.agent",
                        "error_code": "agent_model_timeout",
                    }],
                )

        class Backend:
            retention_seconds = 60

            def __init__(self):
                self.failure = None

            def claim(self, *_args, **_kwargs):
                return {
                    "created_at": "2026-07-25T10:00:00Z",
                    "attempt": 1,
                }

            def fail(self, *_args, **kwargs):
                self.failure = kwargs

        backend = Backend()
        coordinator = AgentRunCoordinator(
            Service(),
            backend,
            clock=lambda: datetime(
                2026, 7, 25, 10, 0, tzinfo=timezone.utc
            ),
        )
        try:
            coordinator._execute(
                DelegationContext.from_principal(principal()),
                REQUEST,
                object(),
                RUN_ID,
            )
        finally:
            coordinator.close()
        self.assertEqual(
            backend.failure["error_code"],
            "agent_model_timeout",
        )
        self.assertEqual(
            backend.failure["trace"][0]["error_code"],
            "agent_model_timeout",
        )
        self.assertEqual(backend.failure["trace"][0]["receipts"], [])

    def test_execution_error_normalizes_unsafe_failure_code(self) -> None:
        error = AgentExecutionError(
            "provider body is not durable",
            code="unsafe value from provider",
        )
        self.assertEqual(error.code, "agent_execution_failed")


if __name__ == "__main__":
    unittest.main()
