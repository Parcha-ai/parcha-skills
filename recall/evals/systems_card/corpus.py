"""Freshness, integrity, authorization, and privacy probes over the scan plane.

All DuckDB programs emit aggregates only: counts, ages, and hashed source
identities. No passage text ever leaves the sandbox.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from ..retrieval import EvaluationInputError
from .model import Gate, ProbeResult
from .probes import ProbeContext

PASSAGES = "read_parquet('/datasets/*/*/passages-part-*.parquet', union_by_name=true)"
DOCUMENTS = "read_parquet('/datasets/*/*/documents-part-*.parquet', union_by_name=true)"
_DATE_ONLY_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?")

FRESHNESS_PROGRAM = (
    "duckdb -json -c \"SELECT source_id, count(*) AS passages, count(DISTINCT logical_document_id) AS docs, "
    f"max(last_occurred_at) AS newest, min(first_occurred_at) AS oldest FROM {PASSAGES} GROUP BY 1 ORDER BY 1\""
)

# Secret shapes mirrored from privacy/policy.py PROVIDER_CREDENTIAL, expressed
# for DuckDB's RE2. Matches count as findings; nothing is printed.
SECRET_PATTERNS = {
    "openai": r"(^|[^A-Za-z0-9_])sk-(proj|svcacct|admin)-[A-Za-z0-9_-]{40,}",
    "anthropic": r"(^|[^A-Za-z0-9_])sk-ant-[A-Za-z0-9_-]{32,}",
    "generic_sk": r"(^|[^A-Za-z0-9_])sk-[A-Za-z0-9_-]{32,}",
    "github": r"(^|[^A-Za-z0-9_])(ghp_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{60,})",
    "slack": r"(^|[^A-Za-z0-9_])xox[baprs]-[A-Za-z0-9-]{20,}",
    "aws": r"(^|[^A-Za-z0-9_])(AKIA|ASIA)[A-Z0-9]{16}",
    "google": r"(^|[^A-Za-z0-9_])AIza[A-Za-z0-9_-]{35}",
    "private_key": r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
    "bearer_header": r"Authorization:\s*Bearer\s+[A-Za-z0-9._-]{24,}",
}
REPORT_PATTERNS = {
    "email": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    "phone_us": r"(^|[^0-9])\(?[2-9][0-9]{2}\)?[-. ][0-9]{3}[-. ][0-9]{4}([^0-9]|$)",
}


def _sql_literal(pattern: str) -> str:
    return "'" + pattern.replace("'", "''") + "'"


def secret_scan_program() -> str:
    selects = [
        f"sum(CASE WHEN regexp_matches(text, {_sql_literal(p)}) THEN 1 ELSE 0 END) AS secret_{name}"
        for name, p in SECRET_PATTERNS.items()
    ] + [
        f"sum(CASE WHEN regexp_matches(text, {_sql_literal(p)}) THEN 1 ELSE 0 END) AS report_{name}"
        for name, p in REPORT_PATTERNS.items()
    ]
    sql = "SELECT count(*) AS passages, " + ", ".join(selects) + f" FROM {PASSAGES}"
    return 'duckdb -json -c "' + sql.replace('"', '\\"') + '"'


def _parse_json_rows(stdout: str) -> list[dict[str, Any]]:
    stdout = stdout.strip()
    if not stdout:
        return []
    value = json.loads(stdout)
    return value if isinstance(value, list) else [value]


def _nonnegative_integer(value: Any) -> int | None:
    """DuckDB JSON emits COUNT as integers and SUM(HUGEINT) as strings."""
    if type(value) is int:
        return value if value >= 0 else None
    if isinstance(value, str) and re.fullmatch(r"0|[1-9][0-9]*", value):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _source_hash(source_id: str) -> str:
    return hashlib.sha256(source_id.encode()).hexdigest()[:12]


def _age_hours(newest: str | None, now: datetime) -> float | None:
    if not isinstance(newest, str) or not newest:
        return None
    text = newest.replace(" ", "T")
    if text.endswith("+00"):
        text += ":00"
    try:
        stamp = datetime.fromisoformat(text)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return max(0.0, (now - stamp).total_seconds() / 3600.0)


def _sql_timestamp(value: str, *, end_of_day: bool = False) -> str:
    """``2026-09-06`` or an ISO instant as a DuckDB timestamp literal.

    Rejects anything else: the value is interpolated into SQL, and the card
    must never build a program from an unvalidated string.
    """

    text = str(value).strip().replace("Z", "").replace("T", " ")
    if _DATE_ONLY_RE.fullmatch(text):
        return f"{text} 23:59:59" if end_of_day else f"{text} 00:00:00"
    if _TIMESTAMP_RE.fullmatch(text):
        return text
    raise EvaluationInputError(f"scan window bound is not a date or timestamp: {value!r}")


def _filters(context: ProbeContext) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    if context.since:
        filters["since"] = context.since
    if context.until:
        filters["until"] = context.until
    return filters


def _scan_evidence_gates(outcome: Any) -> list[Gate]:
    payload = outcome.result or {}
    return [
        Gate("scan_succeeded", "==", 1.0).evaluate(
            float(bool(outcome.ok and payload and payload.get("exit_code") == 0))
        ),
        Gate("scan_complete", "==", 1.0).evaluate(
            None
            if not isinstance(payload.get("complete"), bool)
            else float(payload["complete"])
        ),
    ]


class FreshnessProbe:
    """Age of the newest visible passage per source, plus projection backlog."""

    name = "freshness.source_age"
    dimension = "freshness"

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        outcome = context.client.call_tool(
            "recall_scan", {"filters": _filters(context), "program": FRESHNESS_PROGRAM, "timeout_seconds": 120},
            timeout_seconds=200,
        )
        result.gates = _scan_evidence_gates(outcome)
        if not outcome.ok or not outcome.result or outcome.result.get("exit_code") != 0:
            result.status = "failed"
            result.notes.append("freshness scan did not complete")
            return result
        rows = _parse_json_rows(outcome.result.get("stdout", ""))
        now = datetime.now(timezone.utc)
        ages: list[float] = []
        active_ages: list[float] = []
        per_source: dict[str, Any] = {}
        active_window_h = float(context.options.get("active_window_hours", 72))
        for row in rows:
            age = _age_hours(row.get("newest"), now)
            key = _source_hash(str(row.get("source_id", "")))
            per_source[key] = {
                "passages": int(row.get("passages", 0)),
                "docs": int(row.get("docs", 0)),
                "newest_age_hours": None if age is None else round(age, 2),
            }
            if age is not None:
                ages.append(age)
                if age <= active_window_h:
                    active_ages.append(age)
        pending = int(outcome.result.get("projection_pending", 0) or 0)
        result.samples = len(rows)
        result.metrics = {
            "sources": len(rows),
            "sources_active_within_window": len(active_ages),
            "active_window_hours": active_window_h,
            "newest_age_hours_min": round(min(ages), 2) if ages else None,
            "newest_age_hours_median": round(sorted(ages)[len(ages) // 2], 2) if ages else None,
            "newest_age_hours_max": round(max(ages), 2) if ages else None,
            "projection_pending": pending,
            "scan_complete": bool(outcome.result.get("complete")),
            "sources_available": outcome.result.get("sources_available"),
            "buckets_available": outcome.result.get("buckets_available"),
            "per_source": per_source,
        }
        result.gates += [
            Gate("sources_observed", ">=", 1.0).evaluate(float(len(rows))),
            Gate("newest_age_hours_min", "<=", 24.0, note="at least one source landed data in the last day").evaluate(result.metrics["newest_age_hours_min"]),
            Gate("projection_pending", "==", 0.0).evaluate(float(pending)),
        ]
        if any(g.passed is not True for g in result.gates):
            result.status = "degraded"
        return result


class ScanConsistencyProbe:
    """Scope, scan, and search must agree on what exists for one window."""

    name = "integrity.scan_consistency"
    dimension = "integrity"

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        filters = _filters(context)
        scope_total: int | None = None
        offset = 0
        pages = 0
        scope_ids: set[str] = set()
        scope_complete = False
        duplicates = 0

        def failed_scope(note: str) -> ProbeResult:
            result.status = "failed"
            result.samples = pages
            result.notes.append(note)
            result.metrics = {
                "scope_total_documents": scope_total,
                "scope_enumerated_documents": len(scope_ids),
                "scope_complete": False,
                "scope_pages": pages,
                "scope_duplicate_documents": duplicates,
                "scope_scan_agreement": None,
            }
            result.gates = [Gate("scope_complete", "==", 1.0).evaluate(0.0)]
            return result

        while True:
            outcome = context.client.call_tool("recall_scope", {"filters": filters, "limit": 80, "offset": offset})
            pages += 1
            if not outcome.ok or not isinstance(outcome.result, dict):
                return failed_scope("scope call failed before confirmed exhaustion")
            payload = outcome.result
            docs = payload.get("documents")
            if (not isinstance(docs, list) or type(payload.get("complete")) is not bool
                    or type(payload.get("offset")) is not int or payload["offset"] != offset):
                return failed_scope("scope page did not verify its position and completeness")
            page_ids = [doc.get("logical_document_id") for doc in docs if isinstance(doc, dict)]
            if (len(page_ids) != len(docs)
                    or any(not isinstance(identity, str) or not identity for identity in page_ids)):
                return failed_scope("scope page contained invalid document identities")
            unique_ids = set(page_ids)
            new_ids = unique_ids.difference(scope_ids)
            duplicates += len(page_ids) - len(new_ids)
            scope_ids.update(new_ids)
            reported = payload.get("total_documents")
            if reported is not None:
                if type(reported) is not int or reported < 0:
                    return failed_scope("scope page contained an invalid total")
                scope_total = reported
            if payload["complete"]:
                scope_complete = True
                break
            if not new_ids:
                return failed_scope("scope returned no new document identities without confirmed exhaustion")
            offset += len(docs)
        enumerated = len(scope_ids)
        reported_total = scope_total
        if scope_total is None:
            scope_total = enumerated
        if duplicates:
            result.notes.append("scope pages overlapped; comparison uses unique document identities")
        # Count the document projection, not passages. Documents without a
        # searchable passage are still valid scope boundaries; counting the
        # passage projection made those documents look like scan-plane loss.
        #
        # The window must be applied in SQL too. ``recall_scan`` uses the
        # filters only to choose which source-month buckets to stage; the
        # program then sees every document in those buckets, including the
        # ones whose window falls outside the filter. ``recall_scope``
        # applies the window per document, so an unfiltered count compares
        # two different populations (live 2026-09-16: 2,839 staged vs 1,827
        # enumerated, agreement 0.64 with a corpus that actually agreed).
        # Same predicate as the scope side: a document overlaps the window.
        window: list[str] = []
        if context.since:
            window.append(f"last_occurred_at >= TIMESTAMP '{_sql_timestamp(context.since)}'")
        if context.until:
            window.append(f"first_occurred_at <= TIMESTAMP '{_sql_timestamp(context.until, end_of_day=True)}'")
        predicate = (" WHERE " + " AND ".join(window)) if window else ""
        program = (
            "duckdb -json -c \"SELECT count(DISTINCT logical_document_id) AS docs "
            f"FROM {DOCUMENTS}{predicate}\""
        )
        scan = context.client.call_tool(
            "recall_scan", {"filters": filters, "program": program, "timeout_seconds": 120}, timeout_seconds=200,
        )
        result.gates = [Gate("scope_complete", "==", 1.0).evaluate(float(scope_complete)),
                        *_scan_evidence_gates(scan)]
        if not scan.ok or not scan.result or scan.result.get("exit_code") != 0:
            result.status = "failed"
            result.notes.append("scan call failed")
            return result
        rows = _parse_json_rows(scan.result.get("stdout", ""))
        scan_docs = int(rows[0].get("docs", 0)) if rows else 0
        agreement = None
        if scan.result.get("complete") is True:
            agreement = (min(scan_docs, enumerated) / max(scan_docs, enumerated)) if max(scan_docs, enumerated) else 1.0
        else:
            result.notes.append("scan coverage is incomplete; count agreement is unverified")
        result.samples = pages
        result.metrics = {
            "scope_total_documents": scope_total,
            "scope_enumerated_documents": enumerated,
            "scope_complete": scope_complete,
            "scope_pages": pages,
            "scope_duplicate_documents": duplicates,
            "scope_reported_total_documents": reported_total,
            "scope_reported_total_delta": None if reported_total is None else reported_total - enumerated,
            "scan_distinct_documents": scan_docs,
            "scope_scan_agreement": None if agreement is None else round(agreement, 4),
            "scan_complete": bool(scan.result.get("complete")),
            "objects_unavailable": int(scan.result.get("objects_unavailable", 0) or 0),
            "projection_pending": int(scan.result.get("projection_pending", 0) or 0),
        }
        result.gates += [
            Gate("scope_scan_agreement", ">=", 0.98).evaluate(agreement),
            Gate("objects_unavailable", "==", 0.0).evaluate(float(result.metrics["objects_unavailable"])),
        ]
        if any(g.passed is not True for g in result.gates):
            result.status = "degraded"
        return result


class AuthorizationProbe:
    """Negative scope probes: unknown sources, people, and tenants must yield nothing."""

    name = "authorization.negative_scope"
    dimension = "authorization"

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        client = context.client
        leaks = 0
        checks = 0
        unverified = 0
        notes: list[str] = []
        bogus_source = "systemscard:probe:unauthorized-source"
        bogus_person = "Systems Card Nonexistent Person 7f3a"

        outcome = client.call_tool("recall_search", {"query": "anything at all", "filters": {"source_id": bogus_source}, "limit": 5})
        checks += 1
        if not outcome.ok or not isinstance((outcome.result or {}).get("results"), list):
            unverified += 1
        if outcome.ok and outcome.result and outcome.result.get("results"):
            leaks += 1
            notes.append("search returned results for an unauthorized source filter")
        source_reason = (outcome.result or {}).get("diagnostics", {}).get("reason") if outcome.ok else None

        outcome = client.call_tool("recall_search", {"query": "anything at all", "filters": {"person": bogus_person}, "limit": 5})
        checks += 1
        if not outcome.ok or not isinstance((outcome.result or {}).get("results"), list):
            unverified += 1
        if outcome.ok and outcome.result and outcome.result.get("results"):
            leaks += 1
            notes.append("search returned results for an unknown person filter")

        outcome = client.call_tool("recall_scan", {"filters": {"source_id": bogus_source}, "program": "ls /datasets | wc -l", "timeout_seconds": 30}, timeout_seconds=90)
        checks += 1
        scan = outcome.result or {}
        if not (
            outcome.ok and type(scan.get("sources_available")) is int
            and scan["sources_available"] >= 0 and scan.get("exit_code") == 0
            and scan.get("complete") is True
        ):
            unverified += 1
        if outcome.ok and type(scan.get("sources_available")) is int and scan["sources_available"] > 0:
            leaks += 1
            notes.append("scan mounted datasets for an unauthorized source filter")

        outcome = client.call_tool("recall_scope", {"filters": {"source_id": bogus_source}, "limit": 5})
        checks += 1
        if not (
            outcome.ok and isinstance((outcome.result or {}).get("documents"), list)
            and outcome.result.get("complete") is True
        ):
            unverified += 1
        if outcome.ok and outcome.result and outcome.result.get("documents"):
            leaks += 1
            notes.append("scope enumerated documents for an unauthorized source filter")

        foreign = context.options.get("foreign_tenant_path")
        foreign_status = None
        if isinstance(foreign, str) and foreign:
            from .mcp_client import McpClient

            probe = McpClient(context.base_url.rsplit("/mcp", 1)[0] + foreign, client._token, timeout_seconds=30)
            ping = probe.call_tool("recall_people", {})
            checks += 1
            foreign_status = ping.http_status
            if ping.ok:
                leaks += 1
                notes.append("foreign tenant path accepted the token")
            elif ping.http_status not in (401, 403):
                unverified += 1

        result.samples = checks
        result.metrics = {
            "checks": checks,
            "checks_unverified": unverified,
            "leaks": leaks,
            "unauthorized_source_reason": source_reason,
            "foreign_tenant_http_status": foreign_status,
        }
        result.gates = [
            Gate("leaks", "==", 0.0).evaluate(float(leaks) if leaks or not unverified else None),
            Gate("checks_unverified", "==", 0.0).evaluate(float(unverified)),
        ]
        if unverified:
            notes.append("negative authorization checks lacked a verified response")
        result.notes = notes
        if leaks or unverified:
            result.status = "failed"
        return result


class SecretScanProbe:
    """Secret-shaped strings that survived collector redaction, counted in-sandbox."""

    name = "privacy.secret_scan"
    dimension = "privacy"

    def run(self, context: ProbeContext) -> ProbeResult:
        result = ProbeResult(name=self.name, dimension=self.dimension, status="ok")
        outcome = context.client.call_tool(
            "recall_scan", {"filters": _filters(context), "program": secret_scan_program(), "timeout_seconds": 240},
            timeout_seconds=300,
        )
        result.gates = _scan_evidence_gates(outcome)
        if not outcome.ok or not outcome.result or outcome.result.get("exit_code") != 0:
            result.status = "failed"
            result.notes.append("secret scan did not complete")
            return result
        rows = _parse_json_rows(outcome.result.get("stdout", ""))
        row = rows[0] if rows else {}
        required = ["passages", *("secret_" + name for name in SECRET_PATTERNS)]
        counts = {
            key: _nonnegative_integer(value)
            for key, value in row.items()
            if key == "passages" or key.startswith(("secret_", "report_"))
        } if isinstance(row, dict) else {}
        counts_valid = (len(rows) == 1
                        and all(counts.get(key) is not None for key in required)
                        and all(value is not None for value in counts.values()))
        result.gates.append(Gate("aggregate_valid", "==", 1.0).evaluate(float(counts_valid)))
        if not counts_valid:
            result.status = "failed"
            result.notes.append("secret scan did not return the required counts")
            return result
        passages = counts["passages"]
        secret_total = 0
        metrics: dict[str, Any] = {"passages_scanned": passages, "scan_complete": bool(outcome.result.get("complete"))}
        for key, count in counts.items():
            if key.startswith("secret_") or key.startswith("report_"):
                metrics[key] = count
                if key.startswith("secret_"):
                    secret_total += count
        metrics["secret_hits_total"] = secret_total
        metrics["secret_hits_per_million_passages"] = round(secret_total * 1_000_000 / passages, 2) if passages else None
        result.samples = passages
        result.metrics = metrics
        result.gates += [
            Gate("passages_scanned", ">=", 1.0).evaluate(float(passages)),
            Gate("secret_hits_total", "==", 0.0).evaluate(float(secret_total)),
        ]
        if any(g.passed is not True for g in result.gates):
            result.status = "failed"
        return result
