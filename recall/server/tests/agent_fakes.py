"""Test-only agent runners. This module is not copied into production images."""

from __future__ import annotations

import json
from urllib.parse import urlsplit

from contracts.agent_v1 import derive_run_id
from recall_server.agent import _receipts, _stable_id, _timestamp


class ScriptedAgentRunner:
    """Deterministic receipt-backed runner for transport and policy tests."""

    def run(self, request, context, tools, *, clock, monotonic):
        started = monotonic()
        now = clock()
        run_id = derive_run_id(context.principal_id, request["idempotency_key"])
        trace_id = _stable_id("trc", run_id)
        filters = {key: request[key] for key in ("since", "until") if key in request}
        packets = []
        for family in request.get("source_families") or [None]:
            routed = dict(filters)
            if family is not None:
                routed["source_family"] = family
            packets.append(tools.call("recall.hints", {
                "query": request["question"],
                "filters": routed,
                "limit": 10,
            }))
        opened = tools.call("recall.exec", {
            "program": "find /mnt/archil/evidence -type f -print0 | xargs -0 rg -n --fixed-strings ''",
            "timeout_seconds": context.budget.max_exec_seconds,
        }) if any(packet.get("results") for packet in packets) else {}
        receipts = list(dict.fromkeys(_receipts(opened)))[:context.budget.max_receipts]
        if any(urlsplit(receipt).netloc not in set(context.authorized_sources) for receipt in receipts):
            raise RuntimeError("test runner escaped its source grant")
        sources = {urlsplit(receipt).netloc for receipt in receipts}
        elapsed = round(max(0.0, monotonic() - started) * 1000, 3)
        status = "partial" if receipts else "no_answer"
        answer = f"Recall opened {len(receipts)} exact evidence receipt(s) across 0 session(s) and {len(sources)} source(s)." if receipts else ""
        claims = [{
            "statement": f"Evidence receipt batch {batch} was opened by Recall.",
            "receipts": receipts[offset:offset + 32],
        } for batch, offset in enumerate(range(0, len(receipts), 32), start=1)]
        gaps = [
            "Semantic answer synthesis is not enabled in the test runner."
            if receipts else "No authorized evidence matched the question."
        ]
        run = {
            "contract": "recall.agent-run.v1", "schema_version": 1,
            "run_id": run_id, "request_id": request["request_id"],
            "tenant_id": context.tenant_id, "principal_id": context.principal_id,
            "trace_id": trace_id, "status": status, "attempt": 1,
            "created_at": _timestamp(now), "updated_at": _timestamp(now),
            "completed_at": _timestamp(now),
        }
        events = [
            ("authorize", "recall.authorization", [], 0, "ok"),
            ("retrieve", "recall.hints", receipts, len(sources), "ok"),
            ("verify", "recall.grounding", receipts, len(sources), "ok"),
            ("complete", "recall.agent", receipts, len(sources), "degraded" if receipts else "ok"),
        ]
        trace = [{
            "contract": "recall.agent-trace-event.v1", "schema_version": 1,
            "trace_id": trace_id, "run_id": run_id, "sequence": sequence,
            "occurred_at": _timestamp(now), "stage": stage, "outcome": outcome,
            "elapsed_ms": 0 if sequence == 0 else elapsed,
            "receipts": event_receipts, "receipt_count": len(event_receipts),
            "source_count": source_count, "session_count": 0, "tool": tool,
        } for sequence, (stage, tool, event_receipts, source_count, outcome) in enumerate(events)]
        result = {
            "contract": "recall.agent-result.v1", "schema_version": 1,
            "run_id": run_id, "request_id": request["request_id"],
            "tenant_id": context.tenant_id, "principal_id": context.principal_id,
            "trace_id": trace_id, "status": status, "answer": answer,
            "claims": claims, "citations": receipts, "gaps": gaps,
            "completed_at": _timestamp(now),
        }
        return {"run": run, "trace": trace, "result": result}


class ScriptedExecInspector:
    """Keep transport E2Es synthetic while exercising receipt verification."""

    def __init__(self, inspector):
        self.inspector = inspector

    def inspect(self, **arguments):
        return self.inspector.inspect(**arguments)

    def execute(self, **arguments):
        records = []
        receipts = []
        for document_id, values in arguments["routing_receipts"].items():
            candidates = list(dict.fromkeys(values))
            if not candidates:
                continue
            # Model the agent selecting exact supporting evidence after opening
            # a broader routed document instead of citing every routing hint.
            selected = candidates[-1:]
            receipts.extend(selected)
            records.append({
                "logical_document_id": document_id,
                "content": "synthetic transport evidence",
                "event_native_id": "synthetic-event",
                "occurred_at": "2026-07-23T00:00:00Z",
                "ordinal": 0,
                "receipts": selected,
            })
        receipts = list(dict.fromkeys(receipts))
        lines = [json.dumps(record) for record in records]
        lines.extend(f"RECALL_EVIDENCE {receipt}" for receipt in receipts)
        return {
            "provider": "synthetic-exec",
            "stdout": "\n".join(lines),
            "stderr": "",
            "exit_code": 0,
            "complete": True,
            "stopped_reason": "completed",
            "timing": None,
        }
