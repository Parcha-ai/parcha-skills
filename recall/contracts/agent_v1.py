"""Dependency-free contracts for the Recall answer-agent boundary.

The caller asks a question. Authentication and the URL bind the brain, tenant,
principal, and source grants; those fields are intentionally absent from the
request. The server returns a durable run, a receipt-backed answer, and a
content-free execution trace.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

from .v2 import ContractError, validate_contract


AGENT_SCHEMA_VERSION = 1
AGENT_CONTRACTS = frozenset({
    "recall.agent-request.v1",
    "recall.agent-run.v1",
    "recall.agent-trace-event.v1",
    "recall.agent-result.v1",
})
OPAQUE_RE = re.compile(r"[a-z][a-z0-9_]{2,31}_[A-Za-z0-9_-]{16,128}\Z")
IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@/-]{1,159}\Z")
SAFE_ERROR_RE = re.compile(r"[a-z][a-z0-9_.-]{1,63}\Z")
MAX_CONTRACT_BYTES = 1_000_000


def _copy(value: Any) -> dict[str, Any]:
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
    except (TypeError, ValueError) as error:
        raise ContractError("agent contract must be finite JSON") from error
    if len(encoded) > MAX_CONTRACT_BYTES:
        raise ContractError("agent contract exceeds byte bound")
    decoded = json.loads(encoded)
    if not isinstance(decoded, dict):
        raise ContractError("agent contract must be an object")
    return decoded


def _closed(
    value: Any,
    *,
    required: set[str],
    optional: set[str] = set(),
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ContractError("agent contract value must be an object")
    missing = required - set(value)
    unknown = set(value) - required - optional
    if missing or unknown:
        raise ContractError("agent contract fields are incomplete or unknown")
    return value


def _string(value: Any, *, maximum: int, minimum: int = 1) -> str:
    if not isinstance(value, str) or not minimum <= len(value) <= maximum:
        raise ContractError("agent string field is invalid")
    return value


def _enum(value: Any, allowed: set[str]) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ContractError("agent enum field is invalid")
    return value


def _opaque(value: Any, prefix: str) -> str:
    text = _string(value, maximum=160)
    if not OPAQUE_RE.fullmatch(text) or not text.startswith(prefix + "_"):
        raise ContractError("agent opaque identity field is invalid")
    return text


def _identity(value: Any) -> str:
    text = _string(value, maximum=160, minimum=2)
    if not IDENTITY_RE.fullmatch(text):
        raise ContractError("agent identity field is invalid")
    return text


def _timestamp(value: Any) -> str:
    text = _string(value, maximum=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ContractError("agent timestamp field is invalid") from error
    if parsed.tzinfo is None:
        raise ContractError("agent timestamp field is invalid")
    return text


def _integer(value: Any, *, minimum: int = 0, maximum: int = 2**63 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError("agent integer field is invalid")
    if not minimum <= value <= maximum:
        raise ContractError("agent integer field is invalid")
    return value


def _number(value: Any, *, minimum: float = 0, maximum: float = 120_000) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not minimum <= value <= maximum
    ):
        raise ContractError("agent number field is invalid")
    return float(value)


def _receipt(value: Any) -> str:
    text = _string(value, maximum=2048)
    parsed = urlsplit(text)
    if (
        parsed.scheme != "recall"
        or not parsed.netloc
        or not parsed.path.strip("/")
        or parsed.username
        or parsed.password
    ):
        raise ContractError("agent receipt field is invalid")
    return text


def _unique_strings(
    value: Any,
    *,
    maximum: int,
    item_maximum: int,
    empty: bool = True,
) -> list[str]:
    if not isinstance(value, list) or len(value) > maximum or (not empty and not value):
        raise ContractError("agent string list field is invalid")
    result = [_string(item, maximum=item_maximum) for item in value]
    if len(result) != len(set(result)):
        raise ContractError("agent string list contains duplicates")
    return result


def derive_run_id(principal_id: str, idempotency_key: str) -> str:
    """Return the stable run identity for one principal-scoped retry key."""
    _identity(principal_id)
    key = _string(idempotency_key, maximum=200)
    digest = hashlib.sha256(f"{principal_id}\0{key}".encode()).hexdigest()[:32]
    return f"run_{digest}"


def _request(value: dict[str, Any]) -> None:
    _closed(
        value,
        required={
            "contract",
            "schema_version",
            "request_id",
            "idempotency_key",
            "question",
            "depth",
        },
        optional={"since", "until", "source_families"},
    )
    _opaque(value["request_id"], "req")
    _string(value["idempotency_key"], maximum=200)
    _string(value["question"], maximum=8192)
    _enum(value["depth"], {"quick", "normal", "deep"})
    since = _timestamp(value["since"]) if "since" in value else None
    until = _timestamp(value["until"]) if "until" in value else None
    if (
        since is not None
        and until is not None
        and datetime.fromisoformat(since.replace("Z", "+00:00"))
        > datetime.fromisoformat(until.replace("Z", "+00:00"))
    ):
        raise ContractError("agent time window is inverted")
    if "source_families" in value:
        _unique_strings(
            value["source_families"],
            maximum=32,
            item_maximum=160,
        )


def _run(value: dict[str, Any]) -> None:
    _closed(
        value,
        required={
            "contract",
            "schema_version",
            "run_id",
            "request_id",
            "tenant_id",
            "principal_id",
            "trace_id",
            "status",
            "attempt",
            "created_at",
            "updated_at",
        },
        optional={"completed_at", "error_code"},
    )
    _opaque(value["run_id"], "run")
    _opaque(value["request_id"], "req")
    _identity(value["tenant_id"])
    _identity(value["principal_id"])
    _opaque(value["trace_id"], "trc")
    status = _enum(
        value["status"],
        {"queued", "running", "complete", "partial", "no_answer", "failed"},
    )
    _integer(value["attempt"], minimum=1, maximum=100)
    _timestamp(value["created_at"])
    _timestamp(value["updated_at"])
    if "completed_at" in value:
        _timestamp(value["completed_at"])
        if status not in {"complete", "partial", "no_answer", "failed"}:
            raise ContractError("unfinished agent run has completion time")
    elif status in {"complete", "partial", "no_answer", "failed"}:
        raise ContractError("finished agent run has no completion time")
    if "error_code" in value:
        code = _string(value["error_code"], maximum=64)
        if not SAFE_ERROR_RE.fullmatch(code) or status != "failed":
            raise ContractError("agent run error code is invalid")


def _trace_event(value: dict[str, Any]) -> None:
    _closed(
        value,
        required={
            "contract",
            "schema_version",
            "trace_id",
            "run_id",
            "sequence",
            "occurred_at",
            "stage",
            "outcome",
            "elapsed_ms",
            "receipt_count",
            "source_count",
            "session_count",
        },
        optional={"tool", "error_code"},
    )
    _opaque(value["trace_id"], "trc")
    _opaque(value["run_id"], "run")
    _integer(value["sequence"], maximum=10_000)
    _timestamp(value["occurred_at"])
    _enum(
        value["stage"],
        {
            "authorize",
            "plan",
            "retrieve",
            "inspect",
            "synthesize",
            "verify",
            "complete",
        },
    )
    _enum(value["outcome"], {"started", "ok", "degraded", "denied", "failed"})
    _number(value["elapsed_ms"])
    _integer(value["receipt_count"], maximum=100_000)
    _integer(value["source_count"], maximum=100_000)
    _integer(value["session_count"], maximum=100_000)
    if "tool" in value:
        _identity(value["tool"])
    if "error_code" in value:
        code = _string(value["error_code"], maximum=64)
        if not SAFE_ERROR_RE.fullmatch(code):
            raise ContractError("agent trace error code is invalid")


def _result(value: dict[str, Any]) -> None:
    _closed(
        value,
        required={
            "contract",
            "schema_version",
            "run_id",
            "request_id",
            "tenant_id",
            "principal_id",
            "trace_id",
            "status",
            "answer",
            "claims",
            "citations",
            "gaps",
            "completed_at",
        },
    )
    _opaque(value["run_id"], "run")
    _opaque(value["request_id"], "req")
    _identity(value["tenant_id"])
    _identity(value["principal_id"])
    _opaque(value["trace_id"], "trc")
    status = _enum(value["status"], {"complete", "partial", "no_answer"})
    answer = _string(value["answer"], maximum=64_000, minimum=0)
    citations = value["citations"]
    if not isinstance(citations, list) or len(citations) > 256:
        raise ContractError("agent citations are invalid")
    citation_values = [_receipt(item) for item in citations]
    if len(citation_values) != len(set(citation_values)):
        raise ContractError("agent citations contain duplicates")
    claims = value["claims"]
    if not isinstance(claims, list) or len(claims) > 128:
        raise ContractError("agent claims are invalid")
    for claim in claims:
        _closed(claim, required={"statement", "receipts"})
        _string(claim["statement"], maximum=4096)
        receipts = claim["receipts"]
        if not isinstance(receipts, list) or not receipts or len(receipts) > 32:
            raise ContractError("agent claim has no supporting receipts")
        claim_receipts = [_receipt(item) for item in receipts]
        if len(claim_receipts) != len(set(claim_receipts)):
            raise ContractError("agent claim receipts contain duplicates")
        if not set(claim_receipts) <= set(citation_values):
            raise ContractError("agent claim cites an undeclared receipt")
    gaps = _unique_strings(
        value["gaps"],
        maximum=64,
        item_maximum=1024,
    )
    _timestamp(value["completed_at"])
    if status in {"complete", "partial"} and (not answer or not claims or not citations):
        raise ContractError("answering agent result is not receipt-backed")
    if status == "no_answer" and (answer or claims or citations or not gaps):
        raise ContractError("no-answer result must be empty and explain its gap")


VALIDATORS = {
    "recall.agent-request.v1": _request,
    "recall.agent-run.v1": _run,
    "recall.agent-trace-event.v1": _trace_event,
    "recall.agent-result.v1": _result,
}


def validate_agent_contract(value: Any, *, expected: str | None = None) -> dict[str, Any]:
    copied = _copy(value)
    contract = copied.get("contract")
    if contract not in AGENT_CONTRACTS or (expected is not None and contract != expected):
        raise ContractError("agent contract discriminator is invalid")
    if copied.get("schema_version") != AGENT_SCHEMA_VERSION:
        raise ContractError("agent contract schema version is invalid")
    VALIDATORS[contract](copied)
    return copied


def validate_agent_exchange(
    authority: Any,
    request: Any,
    run: Any,
    trace_events: Any,
    result: Any,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Prove an answer stayed inside one authenticated brain and receipt grant."""
    auth = validate_contract(authority, expected="recall.principal-authority.v1")
    query = validate_agent_contract(request, expected="recall.agent-request.v1")
    execution = validate_agent_contract(run, expected="recall.agent-run.v1")
    answer = validate_agent_contract(result, expected="recall.agent-result.v1")
    if not isinstance(trace_events, list) or not trace_events or len(trace_events) > 10_000:
        raise ContractError("agent trace is invalid")
    trace = [
        validate_agent_contract(item, expected="recall.agent-trace-event.v1")
        for item in trace_events
    ]
    identity = (auth["tenant_id"], auth["principal_id"])
    if identity != (execution["tenant_id"], execution["principal_id"]):
        raise ContractError("agent run authority mismatch")
    if identity != (answer["tenant_id"], answer["principal_id"]):
        raise ContractError("agent result authority mismatch")
    if "recall:answer" not in auth["scopes"]:
        raise ContractError("agent answer scope is missing")
    if query["request_id"] != execution["request_id"] or query["request_id"] != answer["request_id"]:
        raise ContractError("agent request identity mismatch")
    if execution["run_id"] != answer["run_id"]:
        raise ContractError("agent run identity mismatch")
    if execution["trace_id"] != answer["trace_id"]:
        raise ContractError("agent trace identity mismatch")
    if execution["run_id"] != derive_run_id(auth["principal_id"], query["idempotency_key"]):
        raise ContractError("agent idempotency identity mismatch")
    expected_sequence = list(range(len(trace)))
    if [item["sequence"] for item in trace] != expected_sequence:
        raise ContractError("agent trace sequence is not contiguous")
    if any(
        item["run_id"] != execution["run_id"]
        or item["trace_id"] != execution["trace_id"]
        for item in trace
    ):
        raise ContractError("agent trace lineage mismatch")
    if trace[0]["stage"] != "authorize" or trace[-1]["stage"] != "complete":
        raise ContractError("agent trace has no closed lifecycle")
    expected_run_status = {
        "complete": "complete",
        "partial": "partial",
        "no_answer": "no_answer",
    }[answer["status"]]
    if execution["status"] != expected_run_status:
        raise ContractError("agent run and result status mismatch")
    if max(item["receipt_count"] for item in trace) < len(answer["citations"]):
        raise ContractError("agent trace does not account for answer citations")
    granted_sources = set(auth["source_ids"])
    cited_sources = {urlsplit(receipt).netloc for receipt in answer["citations"]}
    if not cited_sources <= granted_sources:
        raise ContractError("agent citation source scope mismatch")
    return auth, query, execution, trace, answer
