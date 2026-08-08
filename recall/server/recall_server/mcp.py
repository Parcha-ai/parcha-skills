from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .authorization import allowed_tools, decide
from .deep_inspection import DeepInspectionError

LATEST_PROTOCOL_VERSION = "2026-07-28"
LATEST_LEGACY_PROTOCOL_VERSION = "2026-06-30"
SUPPORTED_PROTOCOL_VERSIONS = frozenset(
    {
        "2025-03-26",
        "2025-06-18",
        "2025-11-25",
        LATEST_LEGACY_PROTOCOL_VERSION,
        LATEST_PROTOCOL_VERSION,
    }
)
REQUEST_METHODS = (
    "initialize",
    "server/discover",
    "ping",
    "tools/list",
    "tools/call",
    "tasks/get",
    "tasks/update",
    "tasks/cancel",
    "subscriptions/listen",
)
NOTIFICATION_METHODS = ("notifications/initialized",)
MAX_MCP_RESPONSE_BYTES = 1024 * 1024
TIME_BOUND_SCHEMA = {
    "oneOf": [
        {"type": "string", "format": "date"},
        {"type": "string", "format": "date-time"},
    ]
}


@dataclass(frozen=True)
class McpProtocolError(Exception):
    code: int
    message: str
    data: dict[str, Any] | None = None


def _object(value: Any, name: str) -> dict:
    if not isinstance(value, dict):
        raise McpProtocolError(-32602, f"{name} must be an object")
    return value


def _string(value: Any, name: str, *, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or (required and not value.strip()):
        raise McpProtocolError(-32602, f"{name} must be a non-empty string")
    return value


def _integer(
    value: Any,
    name: str,
    *,
    default: int,
    minimum: int = 0,
    maximum: int = 100,
) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise McpProtocolError(-32602, f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise McpProtocolError(
            -32602, f"{name} must be between {minimum} and {maximum}"
        )
    return value


def _boolean(value: Any, name: str, *, default: bool = False) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise McpProtocolError(-32602, f"{name} must be a boolean")
    return value


def _date_time(value: Any, name: str) -> str:
    text = _string(value, name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        raise McpProtocolError(
            -32602,
            f"{name} must be a timezone-aware ISO-8601 timestamp",
        ) from None
    if parsed.tzinfo is None:
        raise McpProtocolError(
            -32602,
            f"{name} must be a timezone-aware ISO-8601 timestamp",
        )
    return text


ALL_READ_TOOLS = (
    {
        "name": "use_recall",
        "description": (
            "Ask the authenticated Recall brain one natural-language question. "
            "Recall owns investigation, grounding, citations, and the redacted trace. "
            "For a broad matrix asking about several people across several dates, "
            "the client agent should decompose it into parallel narrow calls, usually "
            "one person/time slice, then synthesize their grounded results. Start "
            "those cells at quick depth and deepen only ambiguous cells. Never use "
            "one broad call as proof of absence for a requested cell. "
            "Long investigations start asynchronously: MCP Tasks clients receive a "
            "native task, and compatibility clients receive a durable run handle with "
            "run_id and trace_id for status, result, or cancellation calls."
        ),
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "contract",
                "schema_version",
                "request_id",
                "idempotency_key",
                "question",
                "depth",
            ],
            "properties": {
                "contract": {"const": "recall.agent-request.v1"},
                "schema_version": {"const": 1},
                "request_id": {
                    "type": "string",
                    "pattern": "^req_[A-Za-z0-9_-]{16,128}$",
                },
                "idempotency_key": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 200,
                },
                "question": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 8192,
                },
                "depth": {
                    "type": "string",
                    "enum": ["quick", "normal", "deep"],
                    "default": "normal",
                },
                "since": {"type": "string", "format": "date-time"},
                "until": {"type": "string", "format": "date-time"},
                "source_families": {
                    "type": "array",
                    "maxItems": 32,
                    "uniqueItems": True,
                    "items": {
                        "type": "string",
                        "minLength": 2,
                        "maxLength": 160,
                    },
                },
            },
        },
        "outputSchema": {"type": "object"},
        "annotations": { "readOnlyHint": True },
    },
    {
        "name": "recall_agent_start",
        "description": (
            "Durably start one Recall investigation and return immediately with "
            "an authorization-bound run handle."
        ),
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "contract",
                "schema_version",
                "request_id",
                "idempotency_key",
                "question",
                "depth",
            ],
            "properties": {
                "contract": {"const": "recall.agent-request.v1"},
                "schema_version": {"const": 1},
                "request_id": {
                    "type": "string",
                    "pattern": "^req_[A-Za-z0-9_-]{16,128}$",
                },
                "idempotency_key": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 200,
                },
                "question": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 8192,
                },
                "depth": {
                    "type": "string",
                    "enum": ["quick", "normal", "deep"],
                },
                "since": {"type": "string", "format": "date-time"},
                "until": {"type": "string", "format": "date-time"},
                "source_families": {
                    "type": "array",
                    "maxItems": 32,
                    "uniqueItems": True,
                    "items": {
                        "type": "string",
                        "minLength": 2,
                        "maxLength": 160,
                    },
                },
            },
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "recall_agent_status",
        "description": "Read one authenticated Recall agent run status.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["run_id"],
            "properties": {
                "run_id": {
                    "type": "string",
                    "pattern": "^run_[0-9a-f]{32}$",
                },
            },
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "recall_agent_result",
        "description": "Read one terminal Recall agent result and redacted trace.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["run_id"],
            "properties": {
                "run_id": {
                    "type": "string",
                    "pattern": "^run_[0-9a-f]{32}$",
                },
            },
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "recall_agent_cancel",
        "description": "Cancel one queued or running authenticated Recall agent run.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["run_id"],
            "properties": {
                "run_id": {
                    "type": "string",
                    "pattern": "^run_[0-9a-f]{32}$",
                },
            },
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": False, "destructiveHint": True},
    },
    {
        "name": "recall_related",
        "description": (
            "Find Recall evidence related to a working directory or branch. "
            "Use this to recover nearby work context when exact terms are unknown."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "cwd": {"type": "string"},
                "branch": {"type": "string"},
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 20,
                    "default": 10,
                },
                "mains_only": {"type": "boolean", "default": False},
                "fast": {"type": "boolean", "default": False},
            },
            "additionalProperties": False,
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "recall_search",
        "description": (
            "Search the authorized Recall brain using a natural-language "
            "question. Results include stable recall:// receipts for follow-up."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "maxLength": 8192,
                    "description": "A natural-language question or search phrase.",
                },
                "filters": {
                    "type": "object",
                    "default": {},
                    "properties": {
                        "since": TIME_BOUND_SCHEMA,
                        "until": TIME_BOUND_SCHEMA,
                        "source_id": {"type": "string"},
                        "source_family": {"type": "string"},
                        "source_alias": {"type": "string"},
                        "person": {"type": "string", "maxLength": 256},
                        "person_relation": {
                            "type": "string",
                            "enum": [
                                "author",
                                "contributor",
                                "owner",
                                "organizer",
                                "participant",
                                "attendee",
                            ],
                        },
                    },
                    "additionalProperties": False,
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 20,
                    "default": 10,
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "recall_investigate",
        "description": (
            "Investigate a natural-language question in one bounded call. "
            "Uses source occurrence time, diversifies across sessions and sources, "
            "and returns exact recall:// receipts with surrounding evidence."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "maxLength": 8192,
                },
                "filters": {
                    "type": "object",
                    "default": {},
                    "properties": {
                        "since": TIME_BOUND_SCHEMA,
                        "until": TIME_BOUND_SCHEMA,
                        "source_id": {"type": "string"},
                        "source_family": {"type": "string"},
                        "source_alias": {"type": "string"},
                        "person": {"type": "string", "maxLength": 256},
                        "person_relation": {
                            "type": "string",
                            "enum": [
                                "author",
                                "contributor",
                                "owner",
                                "organizer",
                                "participant",
                                "attendee",
                            ],
                        },
                    },
                    "additionalProperties": False,
                },
                "depth": {
                    "type": "string",
                    "enum": ["quick", "normal", "deep"],
                    "default": "normal",
                },
            },
            "required": ["question"],
            "additionalProperties": False,
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "recall_deep_search",
        "description": (
            "Deep-search full privacy-processed evidence files in one bounded "
            "call. Recall selects and authorizes files; optional serverless "
            "compute returns exact recall:// receipts and completeness."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "question": {"type": "string", "maxLength": 8192},
                "filters": {
                    "type": "object",
                    "default": {},
                    "properties": {
                        "since": TIME_BOUND_SCHEMA,
                        "until": TIME_BOUND_SCHEMA,
                        "source_id": {"type": "string"},
                        "source_family": {"type": "string"},
                        "source_alias": {"type": "string"},
                        "person": {"type": "string", "maxLength": 256},
                        "person_relation": {
                            "type": "string",
                            "enum": [
                                "author",
                                "contributor",
                                "owner",
                                "organizer",
                                "participant",
                                "attendee",
                            ],
                        },
                    },
                    "additionalProperties": False,
                },
                "depth": {
                    "type": "string",
                    "enum": ["quick", "normal", "deep"],
                    "default": "normal",
                },
            },
            "required": ["question"],
            "additionalProperties": False,
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "recall_exec",
        "description": (
            "Run bounded read-only shell over exact full Recall documents selected "
            "by recall_search. Pass only logical_document_id values returned by "
            "search; ordered aliases are mounted at /docs/d1 through /docs/d20. "
            "Use rg, jq, awk, sed, sort, or Python to inspect the files. The sandbox "
            "has no network and cannot mutate evidence. Search hits are hints, not "
            "proof: only recall:// values in opened_receipts are citation authority. "
            "To cite a match, print its JSONL record. An optional exact "
            "`RECALL_EVIDENCE <receipt>` line may accompany, never replace, that record."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "targets": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 20,
                    "items": {
                        "type": "object",
                        "properties": {
                            "logical_document_id": {
                                "type": "string",
                                "pattern": r"^ldoc_[0-9a-f]{32}$",
                            },
                            "alias": {
                                "type": "string",
                                "pattern": r"^d(?:[1-9]|1[0-9]|20)$",
                            },
                        },
                        "required": ["logical_document_id", "alias"],
                        "additionalProperties": False,
                    },
                },
                "program": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 16000,
                },
                "timeout_seconds": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 30,
                    "default": 20,
                },
            },
            "required": ["targets", "program"],
            "additionalProperties": False,
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "recall_session_context",
        "description": (
            "Expand one recall:// receipt inside its authorized source session "
            "in occurrence-time order."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "target": {"type": "string"},
                "before": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 20,
                    "default": 4,
                },
                "after": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 20,
                    "default": 4,
                },
            },
            "required": ["target"],
            "additionalProperties": False,
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "recall_show",
        "description": (
            "Resolve a recall:// receipt and return its authorized surrounding context."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "target": {"type": "string"},
                "around": {
                    "type": "string",
                    "format": "date-time",
                },
                "tail": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 100,
                    "default": 0,
                },
                "prompts": {"type": "boolean", "default": False},
            },
            "required": ["target"],
            "additionalProperties": False,
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
)
CANONICAL_ONLY_READ_TOOLS = frozenset({
    "use_recall",
    "recall_agent_start",
    "recall_agent_status",
    "recall_agent_result",
    "recall_agent_cancel",
    "recall_deep_search",
    "recall_exec",
    "recall_investigate",
    "recall_session_context",
})
CANONICAL_READ_TOOLS = tuple(
    tool for tool in ALL_READ_TOOLS
    if tool["name"] in CANONICAL_ONLY_READ_TOOLS
)
READ_TOOLS = tuple(
    tool for tool in ALL_READ_TOOLS
    if tool["name"] not in CANONICAL_ONLY_READ_TOOLS
)
CANONICAL_SHOW_TOOL = {
    **next(tool for tool in READ_TOOLS if tool["name"] == "recall_show"),
    "inputSchema": {
        "type": "object",
        "properties": {"target": {"type": "string"}},
        "required": ["target"],
        "additionalProperties": False,
    },
}
WRITE_TOOLS = (
    {
        "name": "recall_capture",
        "description": (
            "Deliberately save one user-selected memory. Its source and origin "
            "are bound by the host credential, not by model arguments."
        ),
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "schema_version",
                "title",
                "body",
                "occurred_at",
                "provenance",
            ],
            "properties": {
                "schema_version": {"const": 1},
                "title": {"type": "string", "minLength": 1, "maxLength": 500},
                "body": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 32000,
                },
                "occurred_at": {"type": "string", "format": "date-time"},
                "tags": {
                    "type": "array",
                    "maxItems": 20,
                    "uniqueItems": True,
                    "items": {
                        "type": "string",
                        "maxLength": 64,
                        "pattern": "^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$",
                    },
                },
                "provenance": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["uri"],
                    "properties": {
                        "uri": {
                            "type": "string",
                            "format": "uri",
                            "pattern": "^(?:https|manual|connector|export):",
                        }
                    },
                },
            },
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": False, "destructiveHint": False},
    },
    {
        "name": "recall_forget",
        "description": (
            "Forget one prior deliberate capture from this credential's exact "
            "capture source."
        ),
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["receipt"],
            "properties": {
                "receipt": {"type": "string", "minLength": 1},
            },
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": False, "destructiveHint": True},
    },
)


def _write_enabled(principal: dict) -> bool:
    return (
        "write" in principal.get("scopes", ())
        and isinstance(principal.get("source_id"), str)
        and isinstance(principal.get("principal_id"), str)
        and isinstance(principal.get("capture_origin"), str)
    )


def _canonical_forget_enabled(principal: dict) -> bool:
    return (
        principal.get("credential_kind") == "mcp"
        and principal.get("audience") == "recall-mcp"
        and "forget" in principal.get("scopes", ())
    )


def _tools_for(principal: dict) -> tuple[dict, ...]:
    if principal.get("credential_kind") == "mcp":
        permitted = allowed_tools(principal)
        return tuple(
            CANONICAL_SHOW_TOOL if tool["name"] == "recall_show" else tool
            for tool in READ_TOOLS + CANONICAL_READ_TOOLS + WRITE_TOOLS
            if tool["name"] in permitted
            and (
                tool["name"] not in {
                    "use_recall",
                    "recall_agent_start",
                    "recall_agent_status",
                    "recall_agent_result",
                    "recall_agent_cancel",
                }
                or principal.get("agent_enabled") is True
            )
        )
    if _write_enabled(principal):
        return READ_TOOLS + WRITE_TOOLS
    if _canonical_forget_enabled(principal):
        return READ_TOOLS + (WRITE_TOOLS[1],)
    return READ_TOOLS


def _reject_extra(arguments: dict, allowed: frozenset[str]) -> None:
    unknown = sorted(set(arguments) - allowed)
    if unknown:
        raise McpProtocolError(-32602, f"unknown tool arguments: {', '.join(unknown)}")


def _call_tool(
    store,
    principal: dict,
    name: str,
    arguments: dict,
    *,
    agent=None,
    agent_lifecycle=None,
) -> dict:
    authorized_source = principal.get(
        "authorized_sources",
        principal.get("source_id"),
    )
    if name == "use_recall":
        if agent is None:
            raise McpProtocolError(-32602, "unknown tool")
        return agent(arguments)
    if name in {
        "recall_agent_start",
        "recall_agent_status",
        "recall_agent_result",
        "recall_agent_cancel",
    }:
        if agent_lifecycle is None:
            raise McpProtocolError(-32602, "unknown tool")
        operation = name.removeprefix("recall_agent_")
        callback = agent_lifecycle.get(operation)
        if callback is None:
            raise McpProtocolError(-32602, "unknown tool")
        if operation == "start":
            return callback(arguments)
        _reject_extra(arguments, frozenset({"run_id"}))
        run_id = _string(arguments.get("run_id"), "run_id")
        return callback(run_id)
    if name == "recall_search":
        _reject_extra(arguments, frozenset({"query", "filters", "limit"}))
        query = _string(arguments.get("query"), "query")
        if len(query) > 8192:
            raise McpProtocolError(-32602, "query must be at most 8192 characters")
        filters = _object(arguments.get("filters", {}), "filters")
        limit = _integer(
            arguments.get("limit"),
            "limit",
            default=10,
            minimum=1,
            maximum=20,
        )
        return store.search(query, filters, limit, authorized_source)
    if name == "recall_investigate":
        _reject_extra(arguments, frozenset({"question", "filters", "depth"}))
        question = _string(arguments.get("question"), "question")
        if len(question) > 8192:
            raise McpProtocolError(-32602, "question must be at most 8192 characters")
        filters = _object(arguments.get("filters", {}), "filters")
        depth = _string(arguments.get("depth", "normal"), "depth")
        if depth not in {"quick", "normal", "deep"}:
            raise McpProtocolError(
                -32602,
                "depth must be quick, normal, or deep",
            )
        return store.investigate(
            question,
            filters=filters,
            depth=depth,
            authorized_source=authorized_source,
        )
    if name == "recall_deep_search":
        _reject_extra(arguments, frozenset({"question", "filters", "depth"}))
        question = _string(arguments.get("question"), "question")
        if len(question) > 8192:
            raise McpProtocolError(-32602, "question must be at most 8192 characters")
        filters = _object(arguments.get("filters", {}), "filters")
        depth = _string(arguments.get("depth", "normal"), "depth")
        if depth not in {"quick", "normal", "deep"}:
            raise McpProtocolError(
                -32602,
                "depth must be quick, normal, or deep",
            )
        return store.deep_search(
            question,
            filters=filters,
            depth=depth,
            authorized_source=authorized_source,
        )
    if name == "recall_exec":
        _reject_extra(arguments, frozenset({"targets", "program", "timeout_seconds"}))
        targets = arguments.get("targets")
        if not isinstance(targets, list) or not 1 <= len(targets) <= 20:
            raise McpProtocolError(-32602, "targets must contain 1 to 20 documents")
        logical_document_ids: list[str] = []
        document_aliases: dict[str, str] = {}
        for target in targets:
            if not isinstance(target, dict):
                raise McpProtocolError(-32602, "each target must be an object")
            _reject_extra(target, frozenset({"logical_document_id", "alias"}))
            document_id = _string(
                target.get("logical_document_id"),
                "logical_document_id",
            )
            alias = _string(target.get("alias"), "alias")
            if re.fullmatch(r"ldoc_[0-9a-f]{32}", document_id) is None:
                raise McpProtocolError(-32602, "logical_document_id is invalid")
            if re.fullmatch(r"d(?:[1-9]|1[0-9]|20)", alias) is None:
                raise McpProtocolError(-32602, "alias must be d1 through d20")
            if document_id in document_aliases or alias in document_aliases.values():
                raise McpProtocolError(-32602, "targets must be unique")
            logical_document_ids.append(document_id)
            document_aliases[document_id] = alias
        program = _string(arguments.get("program"), "program")
        if len(program.encode()) > 16_000:
            raise McpProtocolError(-32602, "program must be at most 16000 bytes")
        timeout_seconds = _integer(
            arguments.get("timeout_seconds"),
            "timeout_seconds",
            default=20,
            minimum=1,
            maximum=30,
        )
        try:
            return store.execute_agent_program(
                program,
                logical_document_ids=tuple(logical_document_ids),
                document_aliases=document_aliases,
                record_spans={document_id: () for document_id in logical_document_ids},
                routing_receipts={
                    document_id: () for document_id in logical_document_ids
                },
                timeout_seconds=timeout_seconds,
            )
        except DeepInspectionError:
            raise McpProtocolError(-32603, "recall_exec_failed") from None
    if name == "recall_session_context":
        _reject_extra(arguments, frozenset({"target", "before", "after"}))
        target = _string(arguments.get("target"), "target")
        before = _integer(
            arguments.get("before"),
            "before",
            default=4,
            minimum=0,
            maximum=20,
        )
        after = _integer(
            arguments.get("after"),
            "after",
            default=4,
            minimum=0,
            maximum=20,
        )
        result = store.session_context(
            target,
            before=before,
            after=after,
            authorized_source=authorized_source,
        )
        if result is None:
            raise McpProtocolError(-32602, "receipt not found")
        return result
    if name == "recall_show":
        _reject_extra(arguments, frozenset({"target", "around", "tail", "prompts"}))
        target = _string(arguments.get("target"), "target")
        around = (
            _date_time(arguments["around"], "around")
            if "around" in arguments
            else None
        )
        tail = _integer(arguments.get("tail"), "tail", default=0, minimum=0)
        if around is not None and tail > 0:
            raise McpProtocolError(
                -32602,
                "around and positive tail are mutually exclusive",
            )
        prompts = _boolean(arguments.get("prompts"), "prompts")
        result = store.show(
            target,
            around=around,
            tail=tail,
            prompts=prompts,
            authorized_source=authorized_source,
        )
        if result is None:
            raise McpProtocolError(-32602, "receipt not found")
        return result
    if name == "recall_related":
        _reject_extra(
            arguments,
            frozenset({"cwd", "branch", "limit", "mains_only", "fast"}),
        )
        cwd = _string(arguments.get("cwd"), "cwd", required=False)
        branch = _string(arguments.get("branch"), "branch", required=False)
        limit = _integer(
            arguments.get("limit"),
            "limit",
            default=10,
            minimum=1,
            maximum=20,
        )
        mains_only = _boolean(arguments.get("mains_only"), "mains_only")
        fast = _boolean(arguments.get("fast"), "fast")
        return store.related(
            cwd=cwd,
            branch=branch,
            limit=limit,
            mains_only=mains_only,
            fast=fast,
            authorized_source=authorized_source,
        )
    if name == "recall_capture":
        if not _write_enabled(principal):
            raise McpProtocolError(-32602, "unknown tool")
        _reject_extra(
            arguments,
            frozenset({
                "schema_version",
                "title",
                "body",
                "occurred_at",
                "tags",
                "provenance",
            }),
        )
        return store.capture(principal, arguments)
    if name == "recall_forget":
        _reject_extra(arguments, frozenset({"receipt"}))
        receipt = _string(arguments.get("receipt"), "receipt")
        if _canonical_forget_enabled(principal):
            return store.forget(receipt)
        if _write_enabled(principal):
            return store.forget_capture(principal, receipt)
        raise McpProtocolError(-32602, "unknown tool")
    raise McpProtocolError(-32602, "unknown tool")


def _tool_result(value: dict) -> dict:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(value, default=str, sort_keys=True),
            }
        ],
        "structuredContent": value,
        "isError": False,
    }


def bound_response(response: dict, request_id: Any) -> dict:
    encoded = json.dumps(response, default=str, sort_keys=True).encode()
    if len(encoded) <= MAX_MCP_RESPONSE_BYTES:
        return response
    return error_response(
        McpProtocolError(-32603, "tool result exceeds limit"),
        request_id,
    )


def _tasks_extension_enabled(params: dict, protocol_version: str) -> bool:
    if protocol_version not in {
        LATEST_LEGACY_PROTOCOL_VERSION,
        LATEST_PROTOCOL_VERSION,
    }:
        return False
    metadata = params.get("_meta")
    if not isinstance(metadata, dict):
        return False
    capabilities = metadata.get("io.modelcontextprotocol/clientCapabilities")
    if not isinstance(capabilities, dict):
        return False
    extensions = capabilities.get("extensions")
    return (
        isinstance(extensions, dict)
        and isinstance(extensions.get("io.modelcontextprotocol/tasks"), dict)
    )


def _validate_modern_metadata(params: dict, protocol_version: str) -> None:
    if protocol_version != LATEST_PROTOCOL_VERSION:
        return
    metadata = params.get("_meta")
    if not isinstance(metadata, dict):
        raise McpProtocolError(-32602, "modern MCP request metadata is required")
    if metadata.get("io.modelcontextprotocol/protocolVersion") != protocol_version:
        raise McpProtocolError(-32020, "MCP protocol metadata does not match header")
    client = metadata.get("io.modelcontextprotocol/clientInfo")
    capabilities = metadata.get("io.modelcontextprotocol/clientCapabilities")
    if (
        not isinstance(client, dict)
        or not isinstance(client.get("name"), str)
        or not client["name"]
        or not isinstance(client.get("version"), str)
        or not client["version"]
        or not isinstance(capabilities, dict)
    ):
        raise McpProtocolError(-32602, "modern MCP client metadata is invalid")


def task_subscription(
    message: Any,
    *,
    protocol_version: str,
) -> tuple[Any, tuple[str, ...]] | None:
    request = _object(message, "request")
    if request.get("method") != "subscriptions/listen":
        return None
    if protocol_version != LATEST_PROTOCOL_VERSION:
        raise McpProtocolError(-32601, "method not found")
    if request.get("jsonrpc") != "2.0" or "id" not in request:
        raise McpProtocolError(-32600, "invalid subscription request")
    params = _object(request.get("params", {}), "params")
    _validate_modern_metadata(params, protocol_version)
    if not _tasks_extension_enabled(params, protocol_version):
        raise McpProtocolError(
            -32003,
            "missing Tasks client capability",
            {
                "requiredCapabilities": {
                    "extensions": {"io.modelcontextprotocol/tasks": {}}
                }
            },
        )
    notifications = _object(params.get("notifications"), "notifications")
    if set(notifications) != {"taskIds"}:
        raise McpProtocolError(-32602, "only task subscriptions are supported")
    raw_ids = notifications["taskIds"]
    if (
        not isinstance(raw_ids, list)
        or not 1 <= len(raw_ids) <= 32
        or any(
            not isinstance(task_id, str)
            or re.fullmatch(r"tsk_[0-9a-f]{32}", task_id) is None
            for task_id in raw_ids
        )
        or len(raw_ids) != len(set(raw_ids))
    ):
        raise McpProtocolError(-32602, "task subscription identifiers are invalid")
    return request["id"], tuple(raw_ids)


def _task_result(
    run: dict,
    task_id: str,
    *,
    creation: bool,
    result: dict | None = None,
    ttl_ms: int = 604_800_000,
) -> dict:
    status = {
        "queued": "working",
        "running": "working",
        "complete": "completed",
        "partial": "completed",
        "no_answer": "completed",
        "failed": "failed",
        "cancelled": "cancelled",
    }[run["status"]]
    value = {
        "resultType": "task" if creation else "complete",
        "taskId": task_id,
        "status": status,
        "createdAt": run["created_at"],
        "lastUpdatedAt": run["updated_at"],
        "ttlMs": ttl_ms,
        "pollIntervalMs": 1000,
    }
    value["statusMessage"] = {
        "queued": "Queued",
        "planning": "Planning the investigation",
        "searching": "Searching candidate evidence",
        "inspecting": "Inspecting full evidence",
        "synthesizing": "Synthesizing the answer",
        "verifying": "Verifying citations",
        "completed": "Completed",
        "failed": "Failed",
        "cancelled": "Cancelled",
    }.get(run.get("status_message"), "Working")
    if status == "completed" and result is not None:
        value["result"] = _tool_result(result)
    elif status == "failed":
        value["error"] = {
            "code": -32603,
            "message": run.get("error_code", "agent run failed"),
        }
    return value


def task_notification(
    state: dict,
    task_id: str,
    *,
    subscription_id: Any,
    result: dict | None = None,
    ttl_ms: int = 604_800_000,
) -> dict:
    params = _task_result(
        state,
        task_id,
        creation=False,
        result=result,
        ttl_ms=ttl_ms,
    )
    params.pop("resultType", None)
    params["_meta"] = {
        "io.modelcontextprotocol/subscriptionId": subscription_id,
    }
    return {
        "jsonrpc": "2.0",
        "method": "notifications/tasks",
        "params": params,
    }


def dispatch(
    store,
    principal: dict,
    message: Any,
    *,
    authorize=None,
    agent=None,
    agent_lifecycle=None,
    protocol_version: str = LATEST_PROTOCOL_VERSION,
    task_name: str | None = None,
) -> dict | None:
    request = _object(message, "request")
    request_id = request.get("id")
    if request.get("jsonrpc") != "2.0":
        raise McpProtocolError(-32600, "invalid JSON-RPC version")
    method = _string(request.get("method"), "method")
    params = _object(request.get("params", {}), "params")
    if method not in {"initialize", "notifications/initialized"}:
        _validate_modern_metadata(params, protocol_version)

    def require_action(action: str, *, hide: bool = False) -> None:
        if principal.get("credential_kind") != "mcp":
            return
        allowed = (
            bool(authorize(action))
            if authorize is not None
            else decide(principal, action).allowed
        )
        if not allowed:
            raise McpProtocolError(
                -32602 if hide else -32600,
                "unknown tool" if hide else "operation not authorized",
            )

    if "id" not in request:
        if method == "notifications/initialized":
            return None
        raise McpProtocolError(-32600, "unsupported notification")

    if method == "server/discover":
        if protocol_version != LATEST_PROTOCOL_VERSION:
            raise McpProtocolError(-32601, "method not found")
        require_action("mcp.initialize")
        result = {
            "resultType": "complete",
            "supportedVersions": [LATEST_PROTOCOL_VERSION],
            "capabilities": {
                "tools": {"listChanged": False},
                **(
                    {"extensions": {"io.modelcontextprotocol/tasks": {}}}
                    if agent_lifecycle is not None
                    else {}
                ),
            },
            "_meta": {
                "io.modelcontextprotocol/serverInfo": {
                    "name": "recall",
                    "version": "1",
                },
            },
            "instructions": "Private, tenant- and source-scoped evidence retrieval.",
            "ttlMs": 300_000,
            "cacheScope": "private",
        }
    elif method == "initialize":
        if protocol_version == LATEST_PROTOCOL_VERSION:
            raise McpProtocolError(-32601, "method not found")
        require_action("mcp.initialize")
        requested = params.get("protocolVersion")
        selected = (
            requested
            if requested in SUPPORTED_PROTOCOL_VERSIONS
            and requested != LATEST_PROTOCOL_VERSION
            else LATEST_LEGACY_PROTOCOL_VERSION
        )
        result = {
            "protocolVersion": selected,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {
                "name": "recall",
                "version": "1",
                "description": "Private, tenant- and source-scoped evidence retrieval.",
            },
        }
        if selected == LATEST_LEGACY_PROTOCOL_VERSION and agent_lifecycle is not None:
            result["capabilities"]["extensions"] = {
                "io.modelcontextprotocol/tasks": {},
            }
    elif method == "ping":
        if protocol_version == LATEST_PROTOCOL_VERSION:
            raise McpProtocolError(-32601, "method not found")
        require_action("mcp.ping")
        result = {}
    elif method == "tools/list":
        require_action("mcp.tools.list")
        result = {"tools": list(_tools_for(principal))}
        if protocol_version == LATEST_PROTOCOL_VERSION:
            result.update({
                "resultType": "complete",
                "ttlMs": 300_000,
                "cacheScope": "private",
            })
    elif method == "tools/call":
        name = _string(params.get("name"), "name")
        if name not in {tool["name"] for tool in _tools_for(principal)}:
            if principal.get("credential_kind") == "mcp":
                require_action(f"mcp.{name}", hide=True)
            raise McpProtocolError(-32602, "unknown tool")
        require_action(f"mcp.{name}", hide=True)
        arguments = _object(params.get("arguments", {}), "arguments")
        if (
            name == "use_recall"
            and agent_lifecycle is not None
            and _tasks_extension_enabled(params, protocol_version)
        ):
            started = agent_lifecycle["start"](arguments)
            final = None
            if started["run"]["status"] in {"complete", "partial", "no_answer"}:
                final = agent_lifecycle["task_result"](started["task_id"])
            result = _task_result(
                started["run"],
                started["task_id"],
                creation=True,
                result=final,
                ttl_ms=started["ttl_ms"],
            )
        else:
            result = _tool_result(
                _call_tool(
                    store,
                    principal,
                    name,
                    arguments,
                    agent=agent,
                    agent_lifecycle=agent_lifecycle,
                )
            )
            if protocol_version == LATEST_PROTOCOL_VERSION:
                result["resultType"] = "complete"
    elif method in {"tasks/get", "tasks/update", "tasks/cancel"}:
        if protocol_version not in {
            LATEST_LEGACY_PROTOCOL_VERSION,
            LATEST_PROTOCOL_VERSION,
        } or agent_lifecycle is None:
            raise McpProtocolError(-32601, "method not found")
        task_id = _string(params.get("taskId"), "taskId")
        expected_params = (
            {"taskId", "inputResponses", "_meta"}
            if method == "tasks/update"
            else {"taskId", "_meta"}
        ) if protocol_version == LATEST_PROTOCOL_VERSION else (
            {"taskId", "inputResponses"}
            if method == "tasks/update"
            else {"taskId"}
        )
        if set(params) != expected_params or task_name != task_id:
            raise McpProtocolError(-32602, "task routing header is invalid")
        action = (
            "mcp.recall_agent_cancel"
            if method == "tasks/cancel"
            else "mcp.recall_agent_status"
        )
        require_action(action, hide=True)
        if method == "tasks/update":
            _object(params["inputResponses"], "inputResponses")
            agent_lifecycle["task_status"](task_id)
            raise McpProtocolError(-32602, "task input is not supported")
        if method == "tasks/cancel":
            agent_lifecycle["task_cancel"](task_id)
            result = {"resultType": "complete"}
        else:
            state = agent_lifecycle["task_status"](task_id)
            final = None
            if state["run"]["status"] in {"complete", "partial", "no_answer"}:
                final = agent_lifecycle["task_result"](task_id)
            result = _task_result(
                state["run"],
                task_id,
                creation=False,
                result=final,
                ttl_ms=state["ttl_ms"],
            )
    else:
        raise McpProtocolError(-32601, "method not found")

    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def error_response(error: McpProtocolError, request_id: Any = None) -> dict:
    value = {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": error.code, "message": error.message},
    }
    if error.data is not None:
        value["error"]["data"] = error.data
    return value
