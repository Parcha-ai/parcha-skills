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
            "Search the authorized Recall brain for high-recall evidence pointers. "
            "Use natural language, identifiers, people, source, and time filters as "
            "the question warrants; reformulate or split the question when useful. "
            "Results include logical_document_id values for recall_exec and stable "
            "recall:// receipts for exact follow-up. Hits are hints, not proof."
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
                        "source_connector": {"type": "string"},
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
        "name": "recall_people",
        "description": (
            "List active people explicitly bound to the caller's authorized "
            "sources. Returns only actor IDs, display names, source IDs, source "
            "families, and ownership relations—never message content or provider "
            "identifiers. Use this before recall_scan for team-wide questions."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
        "outputSchema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "recall_scope",
        "description": (
            "Enumerate the complete authorized full-document scope for exact "
            "person, source, and time constraints without semantic ranking or "
            "document prose. Use this for broad coverage questions, then pass "
            "the returned logical_document_id values to recall_exec_map. Page "
            "until complete is true."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "filters": {
                    "type": "object",
                    "default": {},
                    "properties": {
                        "since": TIME_BOUND_SCHEMA,
                        "until": TIME_BOUND_SCHEMA,
                        "source_id": {"type": "string"},
                        "source_family": {"type": "string"},
                        "source_alias": {"type": "string"},
                        "source_connector": {"type": "string"},
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
                    "maximum": 80,
                    "default": 40,
                },
                "offset": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 10000,
                    "default": 0,
                },
            },
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
            "Output is shared across targets, so use recall_exec_map with "
            "shard_size=1 when the same inspection must cover several candidates. "
            "For portable verified search, run `recall-scan --broad --fixed "
            "--pattern TERM --limit N`; ordinary shell and Python are also available. "
            "The sandbox has no network and cannot mutate evidence. Search hits are "
            "hints, not proof: only recall:// values in opened_receipts are citation "
            "authority. To cite a match, print its JSONL record. An optional exact "
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
        "name": "recall_exec_map",
        "description": (
            "Fan one agent-authored bounded read-only shell program across up "
            "to 80 exact full documents from recall_scope or recall_search. "
            "Use shard_size=1 to inspect several independent search candidates "
            "without a large earlier document consuming another's output budget. "
            "Recall splits the admitted targets into bounded shards and runs Archil "
            "sandboxes concurrently; aliases remain stable at /docs/d1 through "
            "/docs/d80. The sandbox is read-only and networkless. Each shard "
            "returns bounded stdout and only verified opened_receipts may be cited."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "targets": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 80,
                    "items": {
                        "type": "object",
                        "properties": {
                            "logical_document_id": {
                                "type": "string",
                                "pattern": r"^ldoc_[0-9a-f]{32}$",
                            },
                            "alias": {
                                "type": "string",
                                "pattern": r"^d(?:[1-9]|[1-7][0-9]|80)$",
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
                "max_parallel": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 8,
                    "default": 4,
                },
                "shard_size": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 20,
                    "default": 20,
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
        "name": "recall_scan",
        "description": (
            "Run one caller-authored DuckDB/shell program over authorized Parquet "
            "source/month datasets. Use this code-mode tool for broad person, team, "
            "source, project, or time analysis instead of issuing many searches. "
            "Files are mounted under /datasets/sN/YYYY-MM/ as "
            "{documents,records,actors}-part-NNNNN.parquet "
            "and the pinned `duckdb` CLI is on PATH. Use DuckDB globs, SQL filters, "
            "grouping, sampling, and joins; use rg/jq over exact documents only when "
            "the scan points to evidence needing deeper inspection. The sandbox is "
            "read-only and networkless. Source filters restrict mounted sources, but "
            "time is mounted at month granularity: repeat exact occurred_at bounds in "
            "SQL. Repeat person predicates too; join actors-part-*.parquet when relation must "
            "be exact. "
            "To open evidence, emit raw complete JSONL with "
            "`duckdb -noheader -list -c \"SELECT record_json FROM "
            "read_parquet('/datasets/*/*/records-part-*.parquet') ...\"`; "
            "only verified recall:// values in opened_receipts may be cited."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "filters": {
                    "type": "object",
                    "default": {},
                    "properties": {
                        "since": TIME_BOUND_SCHEMA,
                        "until": TIME_BOUND_SCHEMA,
                        "source_id": {"type": "string"},
                        "source_family": {"type": "string"},
                        "source_alias": {"type": "string"},
                        "source_connector": {"type": "string"},
                        "person": {"type": "string", "maxLength": 256},
                        "person_relation": {
                            "type": "string",
                            "enum": [
                                "author", "contributor", "owner", "organizer",
                                "participant", "attendee",
                            ],
                        },
                    },
                    "additionalProperties": False,
                },
                "program": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 16000,
                    "description": (
                        "One shell program. Prefer one DuckDB query that filters and "
                        "aggregates first, then emits only supporting record_json lines."
                    ),
                },
                "timeout_seconds": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 240,
                    "default": 60,
                },
            },
            "required": ["program"],
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

RETRIEVAL_INSTRUCTIONS = (
    "Use recall_search to find likely full documents, then inspect exact records "
    "with recall_show or recall_exec before answering. Search hits are hints, not "
    "proof. Treat each named part of the question as an evidence gap and inspect "
    "enough distinct candidates to support each part; the best evidence may rank "
    "below the first result. Never infer that an unfamiliar project or person name "
    "is an alias: material claims require opened evidence using that name or an "
    "explicit alias, otherwise report insufficient evidence. For one named topic, "
    "make no more than three focused searches and stop once the evidence supports "
    "the answer. Use recall_exec_map with shard_size=1 when the same focused check "
    "must cover several independent candidates. For team-wide questions, call "
    "recall_people once, then prefer one recall_scan code-mode call; use DuckDB to "
    "filter and aggregate the mounted Parquet datasets. Cite only receipts "
    "returned in opened_receipts."
)
CANONICAL_ONLY_READ_TOOLS = frozenset({
    "recall_exec",
    "recall_exec_map",
    "recall_people",
    "recall_scan",
    "recall_scope",
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


def _exec_targets(
    value: Any,
    *,
    maximum: int,
) -> tuple[tuple[str, ...], dict[str, str]]:
    if not isinstance(value, list) or not 1 <= len(value) <= maximum:
        raise McpProtocolError(
            -32602,
            f"targets must contain 1 to {maximum} documents",
        )
    logical_document_ids: list[str] = []
    document_aliases: dict[str, str] = {}
    for target in value:
        if not isinstance(target, dict):
            raise McpProtocolError(-32602, "each target must be an object")
        _reject_extra(target, frozenset({"logical_document_id", "alias"}))
        document_id = _string(target.get("logical_document_id"), "logical_document_id")
        alias = _string(target.get("alias"), "alias")
        if re.fullmatch(r"ldoc_[0-9a-f]{32}", document_id) is None:
            raise McpProtocolError(-32602, "logical_document_id is invalid")
        if (
            re.fullmatch(r"d[1-9][0-9]?", alias) is None
            or int(alias[1:]) > maximum
        ):
            raise McpProtocolError(
                -32602,
                f"alias must be d1 through d{maximum}",
            )
        if document_id in document_aliases or alias in document_aliases.values():
            raise McpProtocolError(-32602, "targets must be unique")
        logical_document_ids.append(document_id)
        document_aliases[document_id] = alias
    return tuple(logical_document_ids), document_aliases


def _call_tool(
    store,
    principal: dict,
    name: str,
    arguments: dict,
) -> dict:
    authorized_source = principal.get(
        "authorized_sources",
        principal.get("source_id"),
    )
    if name == "recall_people":
        _reject_extra(arguments, frozenset())
        return store.list_people()
    if name == "recall_scope":
        _reject_extra(arguments, frozenset({"filters", "limit", "offset"}))
        filters = _object(arguments.get("filters", {}), "filters")
        limit = _integer(
            arguments.get("limit"),
            "limit",
            default=40,
            minimum=1,
            maximum=80,
        )
        offset = _integer(
            arguments.get("offset"),
            "offset",
            default=0,
            minimum=0,
            maximum=10_000,
        )
        return store.scope_documents(filters=filters, limit=limit, offset=offset)
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
    if name == "recall_exec":
        _reject_extra(arguments, frozenset({"targets", "program", "timeout_seconds"}))
        logical_document_ids, document_aliases = _exec_targets(
            arguments.get("targets"),
            maximum=20,
        )
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
                logical_document_ids=logical_document_ids,
                document_aliases=document_aliases,
                record_spans={document_id: () for document_id in logical_document_ids},
                routing_receipts={
                    document_id: () for document_id in logical_document_ids
                },
                timeout_seconds=timeout_seconds,
            )
        except DeepInspectionError:
            raise McpProtocolError(-32603, "recall_exec_failed") from None
    if name == "recall_exec_map":
        _reject_extra(
            arguments,
            frozenset({
                "targets",
                "program",
                "max_parallel",
                "shard_size",
                "timeout_seconds",
            }),
        )
        logical_document_ids, document_aliases = _exec_targets(
            arguments.get("targets"),
            maximum=80,
        )
        program = _string(arguments.get("program"), "program")
        if len(program.encode()) > 16_000:
            raise McpProtocolError(-32602, "program must be at most 16000 bytes")
        max_parallel = _integer(
            arguments.get("max_parallel"),
            "max_parallel",
            default=4,
            minimum=1,
            maximum=8,
        )
        shard_size = _integer(
            arguments.get("shard_size"),
            "shard_size",
            default=20,
            minimum=1,
            maximum=20,
        )
        timeout_seconds = _integer(
            arguments.get("timeout_seconds"),
            "timeout_seconds",
            default=20,
            minimum=1,
            maximum=30,
        )
        try:
            return store.execute_agent_program_parallel(
                program,
                logical_document_ids=logical_document_ids,
                document_aliases=document_aliases,
                timeout_seconds=timeout_seconds,
                max_parallel=max_parallel,
                shard_size=shard_size,
            )
        except DeepInspectionError:
            raise McpProtocolError(-32603, "recall_exec_map_failed") from None
    if name == "recall_scan":
        _reject_extra(
            arguments,
            frozenset({"filters", "program", "timeout_seconds"}),
        )
        filters = _object(arguments.get("filters", {}), "filters")
        program = _string(arguments.get("program"), "program")
        if len(program.encode()) > 16_000:
            raise McpProtocolError(-32602, "program must be at most 16000 bytes")
        timeout_seconds = _integer(
            arguments.get("timeout_seconds"),
            "timeout_seconds",
            default=60,
            minimum=1,
            maximum=240,
        )
        try:
            return store.execute_parquet_scan(
                program,
                filters=filters,
                timeout_seconds=timeout_seconds,
            )
        except DeepInspectionError:
            raise McpProtocolError(-32603, "recall_scan_failed") from None
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


def dispatch(
    store,
    principal: dict,
    message: Any,
    *,
    authorize=None,
    protocol_version: str = LATEST_PROTOCOL_VERSION,
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
            "capabilities": {"tools": {"listChanged": False}},
            "_meta": {
                "io.modelcontextprotocol/serverInfo": {
                    "name": "recall",
                    "version": "1",
                },
            },
            "instructions": RETRIEVAL_INSTRUCTIONS,
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
            "instructions": RETRIEVAL_INSTRUCTIONS,
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
        result = _tool_result(
            _call_tool(
                store,
                principal,
                name,
                arguments,
            )
        )
        if protocol_version == LATEST_PROTOCOL_VERSION:
            result["resultType"] = "complete"
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
