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
    deadline_seconds: int = 120


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
    """Small host-owned tool boundary over one tenant-bound retrieval view.

    The model gets semantic hints and a general read-only execution primitive.
    Hints only authorize candidate documents; evidence becomes citable only
    after the execution sandbox actually opens it.
    """

    TOOL_NAMES = (
        "recall.hints",
        "recall.find",
        "recall.open",
        "recall.exec",
    )
    TOOL_CALL_LIMITS = {
        "recall.hints": 6,
        "recall.find": 6,
        "recall.open": 10,
        "recall.exec": 6,
    }

    def __init__(
        self,
        retrieval: Any,
        context: DelegationContext,
        *,
        monotonic: Callable[[], float] | None = None,
    ):
        self._retrieval = retrieval
        self._context = context
        self._monotonic = monotonic or time.monotonic
        self._deadline_at = (
            self._monotonic() + context.budget.deadline_seconds
        )
        self._calls = 0
        self._calls_by_tool: dict[str, int] = {}
        self._opened_receipts: list[str] = []
        self._citable_receipts: list[str] = []
        self._hinted_documents: list[str] = []
        self._document_ids_by_alias: dict[str, str] = {}
        self._aliases_by_document: dict[str, str] = {}
        self._hinted_record_spans: dict[str, list[tuple[int, int]]] = {}
        self._hinted_routing_receipts: dict[str, list[str]] = {}
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
        started_at = self._monotonic()
        try:
            tool_calls = self._calls_by_tool.get(name, 0)
            tool_limit = self.TOOL_CALL_LIMITS.get(name)
            if tool_limit is not None and tool_calls >= tool_limit:
                raise AgentExecutionError(
                    f"{name} per-turn budget is exhausted",
                    code="agent_tool_budget_exhausted",
                )
            self._calls_by_tool[name] = tool_calls + 1
            if name == "recall.hints":
                if (
                    not {"query", "filters", "limit"} == set(arguments)
                    or not isinstance(arguments["query"], str)
                    or not arguments["query"].strip()
                    or len(arguments["query"]) > 8192
                    or not isinstance(arguments["filters"], dict)
                    or isinstance(arguments["limit"], bool)
                    or not isinstance(arguments["limit"], int)
                    or not 1 <= arguments["limit"] <= 20
                ):
                    raise AgentExecutionError("agent tool arguments are invalid")
                result = self._retrieval.passage_hints(
                    arguments["query"],
                    filters=arguments["filters"],
                    limit=arguments["limit"],
                )
                results = result.get("results", [])
                if not isinstance(results, list):
                    raise AgentExecutionError("agent hint result is invalid")
                for item in results:
                    document_id = (
                        item.get("logical_document_id")
                        if isinstance(item, dict)
                        else None
                    )
                    if (
                        isinstance(document_id, str)
                        and document_id not in self._hinted_documents
                    ):
                        self._hinted_documents.append(document_id)
                        alias = f"d{len(self._hinted_documents)}"
                        self._document_ids_by_alias[alias] = document_id
                        self._aliases_by_document[document_id] = alias
                    if isinstance(document_id, str):
                        spans = self._hinted_record_spans.setdefault(
                            document_id,
                            [],
                        )
                        routing_receipts = (
                            self._hinted_routing_receipts.setdefault(
                                document_id,
                                [],
                            )
                        )
                        for matching_range in item.get(
                            "matching_ranges",
                            [],
                        )[:2]:
                            if not isinstance(matching_range, dict):
                                continue
                            for receipt in matching_range.get("receipts", []):
                                if (
                                    isinstance(receipt, str)
                                    and receipt.startswith("recall://")
                                    and len(receipt) <= 2048
                                    and receipt not in routing_receipts
                                    and len(routing_receipts) < 256
                                ):
                                    routing_receipts.append(receipt)
                            for span in matching_range.get("spans", []):
                                if not isinstance(span, dict):
                                    continue
                                start = span.get("record_ordinal")
                                count = span.get("record_count")
                                candidate = (start, count)
                                if (
                                    isinstance(start, int)
                                    and not isinstance(start, bool)
                                    and start >= 0
                                    and isinstance(count, int)
                                    and not isinstance(count, bool)
                                    and 1 <= count <= 10_000
                                    and candidate not in spans
                                    and len(spans) < 64
                                ):
                                    spans.append(candidate)
                if len(self._hinted_documents) > 80:
                    self._hinted_documents = self._hinted_documents[:80]
                    admitted = set(self._hinted_documents)
                    self._hinted_record_spans = {
                        key: value
                        for key, value in self._hinted_record_spans.items()
                        if key in admitted
                    }
                    self._hinted_routing_receipts = {
                        key: value
                        for key, value in self._hinted_routing_receipts.items()
                        if key in admitted
                    }
                    self._document_ids_by_alias = {
                        alias: document_id
                        for alias, document_id
                        in self._document_ids_by_alias.items()
                        if document_id in admitted
                    }
                    self._aliases_by_document = {
                        document_id: alias
                        for document_id, alias
                        in self._aliases_by_document.items()
                        if document_id in admitted
                    }
                result = {
                    **result,
                    "results": [
                        {
                            **{
                                key: value
                                for key, value in item.items()
                                if key != "logical_document_id"
                            },
                            "alias": self._aliases_by_document[document_id],
                        }
                        for item in results
                        if isinstance(item, dict)
                        and isinstance(
                            document_id := item.get("logical_document_id"),
                            str,
                        )
                        and document_id in self._aliases_by_document
                    ],
                }
            elif name == "recall.find":
                aliases = arguments.get("aliases")
                patterns = arguments.get("patterns")
                if (
                    set(arguments)
                    != {
                        "aliases",
                        "patterns",
                        "context_chars",
                        "limit",
                    }
                    or not isinstance(aliases, list)
                    or not 1 <= len(aliases) <= 20
                    or len(aliases) != len(set(aliases))
                    or any(
                        not isinstance(alias, str)
                        or alias not in self._document_ids_by_alias
                        for alias in aliases
                    )
                    or not isinstance(patterns, list)
                    or not 1 <= len(patterns) <= 5
                    or any(
                        not isinstance(pattern, str)
                        or not pattern.strip()
                        or len(pattern) > 512
                        for pattern in patterns
                    )
                    or sum(len(pattern) for pattern in patterns) > 2_000
                    or isinstance(arguments["context_chars"], bool)
                    or not isinstance(arguments["context_chars"], int)
                    or not 200 <= arguments["context_chars"] <= 4_000
                    or isinstance(arguments["limit"], bool)
                    or not isinstance(arguments["limit"], int)
                    or not 1 <= arguments["limit"] <= 20
                ):
                    raise AgentExecutionError(
                        "agent find arguments are invalid",
                        code="agent_find_invalid",
                    )
                remaining = self._deadline_at - self._monotonic()
                executable_seconds = int(remaining) - 6
                if executable_seconds < 1:
                    raise AgentExecutionError(
                        "agent turn has no time remaining for evidence search",
                        code="agent_tool_deadline_exhausted",
                    )
                document_ids = tuple(
                    self._document_ids_by_alias[alias]
                    for alias in aliases
                )
                result = self._retrieval.find_documents(
                    logical_document_ids=tuple(document_ids),
                    document_aliases={
                        document_id: self._aliases_by_document[document_id]
                        for document_id in document_ids
                    },
                    patterns=tuple(patterns),
                    context_chars=arguments["context_chars"],
                    limit=arguments["limit"],
                    record_spans={
                        document_id: tuple(
                            self._hinted_record_spans.get(document_id, ())
                        )
                        for document_id in document_ids
                    },
                    routing_receipts={
                        document_id: tuple(
                            self._hinted_routing_receipts.get(
                                document_id,
                                (),
                            )
                        )
                        for document_id in document_ids
                    },
                    timeout_seconds=min(20, executable_seconds),
                )
            elif name == "recall.open":
                if (
                    set(arguments)
                    != {
                        "alias",
                        "cursor",
                        "record_ordinal",
                        "page_bytes",
                    }
                    or not isinstance(arguments["alias"], str)
                    or arguments["alias"]
                    not in self._document_ids_by_alias
                    or (
                        arguments["cursor"] is not None
                        and (
                            not isinstance(arguments["cursor"], str)
                            or re.fullmatch(
                                r"\d{1,6}:\d{1,12}:\d{1,12}",
                                arguments["cursor"],
                            )
                            is None
                        )
                    )
                    or isinstance(arguments["page_bytes"], bool)
                    or not isinstance(arguments["page_bytes"], int)
                    or not 1_024 <= arguments["page_bytes"] <= 32_768
                    or (
                        arguments["record_ordinal"] is not None
                        and (
                            isinstance(arguments["record_ordinal"], bool)
                            or not isinstance(
                                arguments["record_ordinal"],
                                int,
                            )
                            or arguments["record_ordinal"] < 0
                        )
                    )
                    or (
                        arguments["cursor"] is not None
                        and arguments["record_ordinal"] is not None
                    )
                ):
                    raise AgentExecutionError(
                        "agent open arguments are invalid",
                        code="agent_open_invalid",
                    )
                remaining = self._deadline_at - self._monotonic()
                executable_seconds = int(remaining) - 6
                if executable_seconds < 1:
                    raise AgentExecutionError(
                        "agent turn has no time remaining for evidence open",
                        code="agent_tool_deadline_exhausted",
                    )
                alias = arguments["alias"]
                document_id = self._document_ids_by_alias[alias]
                result = self._retrieval.open_document(
                    logical_document_id=document_id,
                    document_alias=alias,
                    cursor=arguments["cursor"],
                    record_ordinal=arguments["record_ordinal"],
                    page_bytes=arguments["page_bytes"],
                    record_spans={
                        document_id: tuple(
                            self._hinted_record_spans.get(document_id, ())
                        )
                    },
                    routing_receipts={
                        document_id: tuple(
                            self._hinted_routing_receipts.get(
                                document_id,
                                (),
                            )
                        )
                    },
                    timeout_seconds=min(20, executable_seconds),
                )
            elif name == "recall.exec":
                if (
                    set(arguments) != {"program", "timeout_seconds"}
                    or not isinstance(arguments["program"], str)
                    or not arguments["program"].strip()
                    or len(arguments["program"]) > 16_000
                    or isinstance(arguments["timeout_seconds"], bool)
                    or not isinstance(arguments["timeout_seconds"], int)
                    or not 1 <= arguments["timeout_seconds"] <= 30
                ):
                    raise AgentExecutionError("agent tool arguments are invalid")
                if not self._hinted_documents:
                    raise AgentExecutionError(
                        "agent execution requires at least one prior hint",
                        code="agent_exec_without_hints",
                    )
                # Archil's HTTP boundary adds up to four seconds around the
                # sandbox timeout. Reserve that transport allowance plus two
                # seconds for the model's grounded finish, so one synchronous
                # exec can never consume more than the turn's remaining wall
                # budget.
                remaining = self._deadline_at - self._monotonic()
                executable_seconds = int(remaining) - 6
                if executable_seconds < 1:
                    raise AgentExecutionError(
                        "agent turn has no time remaining for evidence execution",
                        code="agent_tool_deadline_exhausted",
                    )
                result = self._retrieval.execute_agent_program(
                    arguments["program"],
                    logical_document_ids=tuple(self._hinted_documents),
                    document_aliases={
                        document_id: self._aliases_by_document[document_id]
                        for document_id in self._hinted_documents
                    },
                    record_spans={
                        document_id: tuple(
                            self._hinted_record_spans.get(document_id, ())
                        )
                        for document_id in self._hinted_documents
                    },
                    routing_receipts={
                        document_id: tuple(
                            self._hinted_routing_receipts.get(
                                document_id,
                                (),
                            )
                        )
                        for document_id in self._hinted_documents
                    },
                    timeout_seconds=min(
                        arguments["timeout_seconds"],
                        executable_seconds,
                    ),
                )
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
            receipts = (
                _receipts(result)
                if name
                in {"recall.find", "recall.open", "recall.exec"}
                else []
            )
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
                if receipt not in self._citable_receipts:
                    self._citable_receipts.append(receipt)
            self._output_bytes += len(encoded)
            coverage = result.get("coverage", {}) if isinstance(result, dict) else {}
            self._observations.append({
                "tool": name,
                "outcome": "ok",
                "elapsed_ms": round(
                    max(0.0, self._monotonic() - started_at) * 1000,
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
                max(0.0, self._monotonic() - started_at) * 1000,
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
                elif key in {"receipts", "opened_receipts"}:
                    if isinstance(nested, list):
                        found.extend(
                            item
                            for item in nested
                            if isinstance(item, str)
                            and item.startswith("recall://")
                        )
                else:
                    visit(nested)
        elif isinstance(child, list):
            for nested in child:
                visit(nested)

    visit(value)
    return list(dict.fromkeys(found))


class ScriptedAgentRunner:
    """Transport/grounding smoke test over the same small agent tool boundary."""

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
            packets.append(tools.call(
                "recall.hints",
                {
                    "query": request["question"],
                    "filters": routed_filters,
                    "limit": 10,
                },
            ))
        has_hints = any(
            isinstance(packet.get("results"), list)
            and packet["results"]
            for packet in packets
        )
        opened = (
            tools.call(
                "recall.exec",
                {
                    "program": (
                        "find /mnt/archil/evidence -type f -print0 | "
                        "xargs -0 rg -n --fixed-strings ''"
                    ),
                    "timeout_seconds": min(
                        30,
                        context.budget.deadline_seconds,
                    ),
                },
            )
            if has_hints
            else {}
        )
        receipts = list(dict.fromkeys(_receipts(opened)))[
            : context.budget.max_receipts
        ]
        granted = set(context.authorized_sources)
        if any(urlsplit(receipt).netloc not in granted for receipt in receipts):
            raise AgentExecutionError("agent evidence escaped its source grant")
        sessions = 0
        sources = {urlsplit(receipt).netloc for receipt in receipts}
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
                tool="recall.hints",
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
