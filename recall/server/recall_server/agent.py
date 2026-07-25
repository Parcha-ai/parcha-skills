"""Scoped Recall answer-agent façade.

This module owns the portable domain boundary shared by HTTP and MCP. It does
not authenticate callers and it does not know provider credentials. The host
derives an immutable delegation context, binds canonical retrieval to it, and
hands the runner only a closed evidence-tool catalog.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Protocol
from urllib.parse import urlsplit

from contracts.agent_v1 import (
    derive_run_id,
    validate_agent_contract,
    validate_agent_exchange,
)
from contracts.v2 import ContractError


class AgentRequestError(ValueError):
    """The public request is invalid."""


class AgentExecutionError(RuntimeError):
    """The internal runner or evidence boundary failed closed."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "agent_execution_failed",
        trace: list[dict[str, Any]] | None = None,
    ):
        super().__init__(message)
        self.code = (
            code
            if isinstance(code, str)
            and re.fullmatch(r"[a-z][a-z0-9_.-]{1,63}", code)
            else "agent_execution_failed"
        )
        self.trace = list(trace or [])


@dataclass(frozen=True)
class AgentBudget:
    max_tool_calls: int = 12
    max_receipts: int = 256
    max_tool_output_bytes: int = 2_000_000
    max_trace_events: int = 64
    deadline_seconds: int = 60


@dataclass(frozen=True)
class TracePolicy:
    include_receipts: bool = True
    include_source_bodies: bool = False
    include_credentials: bool = False


@dataclass(frozen=True)
class DelegationContext:
    tenant_id: str
    principal_id: str
    role: str
    authorized_sources: tuple[str, ...]
    allowed_tools: tuple[str, ...]
    budget: AgentBudget
    trace_policy: TracePolicy

    @classmethod
    def from_principal(cls, principal: dict[str, Any]) -> DelegationContext:
        tenant_id = principal.get("tenant_id")
        principal_id = principal.get("principal_id")
        role = principal.get("role")
        source_values = principal.get("authorized_sources")
        if (
            principal.get("credential_kind") != "mcp"
            or principal.get("audience") != "recall-mcp"
            or not isinstance(tenant_id, str)
            or not tenant_id
            or not isinstance(principal_id, str)
            or not principal_id
            or role not in {"owner", "admin", "member"}
            or not isinstance(source_values, (list, tuple))
            or any(not isinstance(source, str) or not source for source in source_values)
        ):
            raise AgentRequestError("authenticated agent context is invalid")
        return cls(
            tenant_id=tenant_id,
            principal_id=principal_id,
            role=role,
            authorized_sources=tuple(sorted(set(source_values))),
            allowed_tools=ConstrainedAgentTools.TOOL_NAMES,
            budget=AgentBudget(),
            trace_policy=TracePolicy(),
        )


class AgentRunner(Protocol):
    def run(
        self,
        request: dict[str, Any],
        context: DelegationContext,
        tools: ConstrainedAgentTools,
        *,
        clock: Callable[[], datetime],
        monotonic: Callable[[], float],
    ) -> dict[str, Any]: ...


class ConstrainedAgentTools:
    """Closed host-owned wrappers over an already tenant-bound retrieval view."""

    TOOL_NAMES = (
        "recall.deep_search",
        "recall.investigate",
        "recall.session_context",
        "recall.show",
    )

    def __init__(self, retrieval: Any, context: DelegationContext):
        self._retrieval = retrieval
        self._context = context
        self._calls = 0
        self._opened_receipts: list[str] = []
        self._citable_receipts: list[str] = []
        self._observations: list[dict[str, Any]] = []
        self._output_bytes = 0

    @property
    def opened_receipts(self) -> tuple[str, ...]:
        return tuple(self._opened_receipts)

    @property
    def citable_receipts(self) -> tuple[str, ...]:
        return tuple(self._citable_receipts)

    @property
    def observations(self) -> tuple[dict[str, Any], ...]:
        return tuple(dict(item) for item in self._observations)

    @property
    def catalog(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            {
                "name": name,
                "read_only": True,
                "tenant_bound": True,
                "credential_access": False,
            }
            for name in self.TOOL_NAMES
        )

    def call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if name not in self._context.allowed_tools:
            raise AgentExecutionError("agent tool is not authorized")
        if self._calls >= self._context.budget.max_tool_calls:
            raise AgentExecutionError("agent tool-call budget is exhausted")
        if not isinstance(arguments, dict):
            raise AgentExecutionError("agent tool arguments are invalid")
        self._calls += 1
        started_at = time.monotonic()
        try:
            if name == "recall.investigate":
                if set(arguments) != {"question", "filters", "depth"}:
                    raise AgentExecutionError("agent tool arguments are invalid")
                result = self._retrieval.investigate(
                    arguments["question"],
                    filters=arguments["filters"],
                    depth=arguments["depth"],
                )
            elif name == "recall.deep_search":
                if set(arguments) != {"question", "filters", "depth"}:
                    raise AgentExecutionError("agent tool arguments are invalid")
                result = self._retrieval.deep_search(
                    arguments["question"],
                    filters=arguments["filters"],
                    depth=arguments["depth"],
                )
            elif name == "recall.session_context":
                if set(arguments) != {"target", "before", "after"}:
                    raise AgentExecutionError("agent tool arguments are invalid")
                result = self._retrieval.session_context(
                    arguments["target"],
                    before=arguments["before"],
                    after=arguments["after"],
                )
                if result is None:
                    raise AgentExecutionError("agent receipt was not found")
            elif name == "recall.show":
                if set(arguments) != {"target"}:
                    raise AgentExecutionError("agent tool arguments are invalid")
                result = self._retrieval.show(arguments["target"])
                if result is None:
                    raise AgentExecutionError("agent receipt was not found")
            else:
                raise AgentExecutionError("agent tool is not authorized")
            encoded = json.dumps(
                result,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode()
            if len(encoded) > 512_000:
                raise AgentExecutionError(
                    "agent tool result exceeds its bound",
                    code="agent_tool_result_too_large",
                )
            if (
                self._output_bytes + len(encoded)
                > self._context.budget.max_tool_output_bytes
            ):
                raise AgentExecutionError(
                    "agent cumulative tool output exceeds its bound",
                    code="agent_tool_output_budget_exhausted",
                )
            receipts = _receipts(result)
            granted = set(self._context.authorized_sources)
            if any(urlsplit(receipt).netloc not in granted for receipt in receipts):
                raise AgentExecutionError(
                    "agent evidence escaped its source grant",
                    code="agent_evidence_scope_violation",
                )
            new_receipts = [
                receipt
                for receipt in receipts
                if receipt not in self._opened_receipts
            ]
            if (
                len(self._opened_receipts) + len(new_receipts)
                > self._context.budget.max_receipts
            ):
                raise AgentExecutionError(
                    "agent receipt budget is exhausted",
                    code="agent_receipt_budget_exhausted",
                )
            for receipt in receipts:
                if receipt not in self._opened_receipts:
                    self._opened_receipts.append(receipt)
                if (
                    name != "recall.investigate"
                    and receipt not in self._citable_receipts
                ):
                    self._citable_receipts.append(receipt)
            self._output_bytes += len(encoded)
            coverage = result.get("coverage", {}) if isinstance(result, dict) else {}
            self._observations.append({
                "tool": name,
                "outcome": "ok",
                "elapsed_ms": round(
                    max(0.0, time.monotonic() - started_at) * 1000,
                    3,
                ),
                "receipts": receipts,
                "source_count": len({
                    urlsplit(receipt).netloc for receipt in receipts
                }),
                "session_count": (
                    int(coverage.get("sessions", 0))
                    if isinstance(coverage, dict)
                    and isinstance(coverage.get("sessions", 0), int)
                    else 0
                ),
            })
            return result
        except AgentExecutionError:
            self._record_failed_observation(name, started_at)
            raise
        except (TypeError, ValueError) as error:
            self._record_failed_observation(name, started_at)
            raise AgentExecutionError(
                "agent evidence tool rejected the call",
                code="agent_evidence_tool_rejected",
            ) from error
        except Exception as error:
            self._record_failed_observation(name, started_at)
            raise AgentExecutionError(
                "agent evidence tool failed",
                code="agent_evidence_tool_failed",
            ) from error
        raise AgentExecutionError("agent tool is not authorized")

    def _record_failed_observation(self, name: str, started_at: float) -> None:
        self._observations.append({
            "tool": name,
            "outcome": "failed",
            "elapsed_ms": round(
                max(0.0, time.monotonic() - started_at) * 1000,
                3,
            ),
            "receipts": [],
            "source_count": 0,
            "session_count": 0,
        })


def _stable_id(prefix: str, value: str) -> str:
    return f"{prefix}_{hashlib.sha256(value.encode()).hexdigest()[:32]}"


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _receipts(value: Any) -> list[str]:
    found: list[str] = []

    def visit(child: Any) -> None:
        if isinstance(child, dict):
            for key, nested in child.items():
                if key in {"receipt", "anchor_receipt", "resolved_receipt"}:
                    if isinstance(nested, str) and nested.startswith("recall://"):
                        found.append(nested)
                else:
                    visit(nested)
        elif isinstance(child, list):
            for nested in child:
                visit(nested)

    visit(value)
    return list(dict.fromkeys(found))


class ScriptedAgentRunner:
    """Deterministic transport/grounding proof; semantic synthesis arrives in L3."""

    def run(
        self,
        request: dict[str, Any],
        context: DelegationContext,
        tools: ConstrainedAgentTools,
        *,
        clock: Callable[[], datetime],
        monotonic: Callable[[], float],
    ) -> dict[str, Any]:
        started = monotonic()
        now = clock()
        run_id = derive_run_id(context.principal_id, request["idempotency_key"])
        trace_id = _stable_id("trc", run_id)
        filters = {
            key: request[key]
            for key in ("since", "until")
            if key in request
        }
        families = request.get("source_families") or [None]
        packets = []
        for family in families:
            routed_filters = dict(filters)
            if family is not None:
                routed_filters["source_family"] = family
            packets.append(
                tools.call(
                    "recall.investigate",
                    {
                        "question": request["question"],
                        "filters": routed_filters,
                        "depth": request["depth"],
                    },
                )
            )
        receipts = list(dict.fromkeys(
            receipt
            for packet in packets
            for receipt in _receipts(packet)
        ))[: context.budget.max_receipts]
        granted = set(context.authorized_sources)
        if any(urlsplit(receipt).netloc not in granted for receipt in receipts):
            raise AgentExecutionError("agent evidence escaped its source grant")
        sessions = sum(
            int(packet.get("coverage", {}).get("sessions", 0))
            for packet in packets
        )
        sources = {
            source
            for packet in packets
            for source in packet.get("coverage", {}).get("sources", [])
            if isinstance(source, str)
        }
        elapsed_ms = round(max(0.0, monotonic() - started) * 1000, 3)
        if receipts:
            status = "partial"
            answer = (
                f"Recall opened {len(receipts)} exact evidence receipt(s) across "
                f"{sessions} session(s) and {len(sources)} source(s)."
            )
            claims = [
                {
                    "statement": (
                        f"Evidence receipt batch {batch} was opened by Recall."
                    ),
                    "receipts": receipts[offset:offset + 32],
                }
                for batch, offset in enumerate(
                    range(0, len(receipts), 32),
                    start=1,
                )
            ]
            gaps = [
                "Semantic answer synthesis is not enabled in the scripted runner."
            ]
        else:
            status = "no_answer"
            answer = ""
            claims = []
            gaps = ["No authorized evidence matched the question."]
        run = {
            "contract": "recall.agent-run.v1",
            "schema_version": 1,
            "run_id": run_id,
            "request_id": request["request_id"],
            "tenant_id": context.tenant_id,
            "principal_id": context.principal_id,
            "trace_id": trace_id,
            "status": status,
            "attempt": 1,
            "created_at": _timestamp(now),
            "updated_at": _timestamp(now),
            "completed_at": _timestamp(now),
        }
        trace = [
            self._trace_event(
                trace_id,
                run_id,
                sequence=0,
                now=now,
                stage="authorize",
                tool="recall.authorization",
                elapsed_ms=0,
                receipts=[],
                sources=0,
                sessions=0,
            ),
            self._trace_event(
                trace_id,
                run_id,
                sequence=1,
                now=now,
                stage="retrieve",
                tool="recall.investigate",
                elapsed_ms=elapsed_ms,
                receipts=receipts,
                sources=len(sources),
                sessions=sessions,
            ),
            self._trace_event(
                trace_id,
                run_id,
                sequence=2,
                now=now,
                stage="verify",
                tool="recall.grounding",
                elapsed_ms=elapsed_ms,
                receipts=receipts,
                sources=len(sources),
                sessions=sessions,
            ),
            self._trace_event(
                trace_id,
                run_id,
                sequence=3,
                now=now,
                stage="complete",
                tool="recall.agent",
                elapsed_ms=elapsed_ms,
                receipts=receipts,
                sources=len(sources),
                sessions=sessions,
                outcome="degraded" if status == "partial" else "ok",
            ),
        ]
        result = {
            "contract": "recall.agent-result.v1",
            "schema_version": 1,
            "run_id": run_id,
            "request_id": request["request_id"],
            "tenant_id": context.tenant_id,
            "principal_id": context.principal_id,
            "trace_id": trace_id,
            "status": status,
            "answer": answer,
            "claims": claims,
            "citations": receipts,
            "gaps": gaps,
            "completed_at": _timestamp(now),
        }
        return {"run": run, "trace": trace, "result": result}

    @staticmethod
    def _trace_event(
        trace_id: str,
        run_id: str,
        *,
        sequence: int,
        now: datetime,
        stage: str,
        tool: str,
        elapsed_ms: float,
        receipts: list[str],
        sources: int,
        sessions: int,
        outcome: str = "ok",
    ) -> dict[str, Any]:
        return {
            "contract": "recall.agent-trace-event.v1",
            "schema_version": 1,
            "trace_id": trace_id,
            "run_id": run_id,
            "sequence": sequence,
            "occurred_at": _timestamp(now),
            "stage": stage,
            "outcome": outcome,
            "elapsed_ms": elapsed_ms,
            "receipts": receipts,
            "receipt_count": len(receipts),
            "source_count": sources,
            "session_count": sessions,
            "tool": tool,
        }


class RecallAgentService:
    """Validate one shared HTTP/MCP operation around a constrained runner."""

    def __init__(
        self,
        runner: AgentRunner,
        *,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
    ):
        self.runner = runner
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic or time.monotonic

    def use_recall(
        self,
        principal: dict[str, Any],
        request: Any,
        retrieval: Any,
    ) -> dict[str, Any]:
        query, context = self.prepare(principal, request)
        return self.execute(query, context, retrieval)

    def prepare(
        self,
        principal: dict[str, Any],
        request: Any,
    ) -> tuple[dict[str, Any], DelegationContext]:
        try:
            query = validate_agent_contract(
                request,
                expected="recall.agent-request.v1",
            )
        except ContractError as error:
            raise AgentRequestError("agent request is invalid") from error
        context = DelegationContext.from_principal(principal)
        return query, context

    def execute(
        self,
        query: dict[str, Any],
        context: DelegationContext,
        retrieval: Any,
    ) -> dict[str, Any]:
        tools = ConstrainedAgentTools(retrieval, context)
        try:
            bundle = self.runner.run(
                query,
                context,
                tools,
                clock=self.clock,
                monotonic=self.monotonic,
            )
            if not isinstance(bundle, dict) or set(bundle) != {"run", "trace", "result"}:
                raise AgentExecutionError("agent runner result is invalid")
            now = self.clock()
            authority = {
                "contract": "recall.principal-authority.v1",
                "schema_version": 1,
                "tenant_id": context.tenant_id,
                "principal_id": context.principal_id,
                "subject": context.principal_id,
                "audience": "recall-agent",
                "scopes": ["recall:answer"],
                "source_ids": list(context.authorized_sources),
                "expires_at": _timestamp(now + timedelta(minutes=5)),
            }
            validate_agent_exchange(
                authority,
                query,
                bundle["run"],
                bundle["trace"],
                bundle["result"],
            )
            if len(bundle["trace"]) > context.budget.max_trace_events:
                raise AgentExecutionError("agent trace exceeds its bound")
            return bundle
        except AgentExecutionError:
            raise
        except (ContractError, TypeError, ValueError) as error:
            raise AgentExecutionError("agent output failed validation") from error


def service_from_env(environment: dict[str, str]) -> RecallAgentService | None:
    runner = environment.get("RECALL_AGENT_RUNNER", "").strip().casefold()
    if not runner:
        return None
    if runner == "scripted":
        return RecallAgentService(ScriptedAgentRunner())
    if runner == "pi-ati":
        from .agent_pi_ati import runner_from_env

        return RecallAgentService(runner_from_env(environment))
    raise RuntimeError("unsupported Recall agent runner")
