"""Compare Pi tool policy across OpenAI-compatible models on a fixed case.

The corpus is synthetic and public-safe. The harness exercises the real Pi
worker, Recall tool contracts, authorization, evidence opening, and grounding.
Only the model route changes between arms.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

from recall_server.agent import AgentExecutionError, RecallAgentService
from recall_server.agent_pi import PiRunner, SubprocessPiTransport


TENANT = "tenant:synthetic:company"
PRINCIPAL = "principal:synthetic:member"
SOURCE = "source:synthetic:company"
DOCS = {
    "ldoc_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": {
        "day": "2026-08-05",
        "content": "Jordan implemented the Atlas ingestion retry queue.",
        "receipt": f"recall://{SOURCE}/atlas-aug-05?rev=1#item=0",
    },
    "ldoc_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": {
        "day": "2026-08-06",
        "content": "Jordan verified Atlas retry recovery in the end-to-end suite.",
        "receipt": f"recall://{SOURCE}/atlas-aug-06?rev=1#item=0",
    },
}


def principal() -> dict[str, Any]:
    return {
        "credential_kind": "mcp",
        "tenant_id": TENANT,
        "principal_id": PRINCIPAL,
        "role": "member",
        "audience": "recall-mcp",
        "authorized_sources": [SOURCE],
    }


def request(model: str) -> dict[str, Any]:
    suffix = "".join(character for character in model if character.isalnum())[-12:]
    return {
        "contract": "recall.agent-request.v1",
        "schema_version": 1,
        "request_id": f"req_modelmatrix{suffix:0>12}",
        "idempotency_key": f"pi-model-matrix-{model}",
        "question": (
            "What did Jordan work on each day from August 5 through August 6, "
            "2026? Give the concrete change and verification for each day."
        ),
        "depth": "normal",
        "since": "2026-08-05T00:00:00Z",
        "until": "2026-08-07T00:00:00Z",
    }


class SyntheticDailyRetrieval:
    def __init__(self) -> None:
        self.hint_queries: list[dict[str, Any]] = []
        self.inspections: list[str] = []

    def passage_hints(self, query, *, filters, limit):
        self.hint_queries.append({"query": query, "filters": dict(filters)})
        since = str(filters.get("since") or "")
        selected = list(DOCS.items())
        if since.startswith("2026-08-05"):
            selected = [selected[0]]
        elif since.startswith("2026-08-06"):
            selected = [selected[1]]
        return {
            "results": [
                {
                    "source_id": SOURCE,
                    "logical_document_id": document_id,
                    "matching_ranges": [{
                        "text": data["content"],
                        "receipts": [data["receipt"]],
                        "passage_ordinal": 0,
                        "spans": [{
                            "record_ordinal": 0,
                            "record_count": 1,
                            "source_byte_start": 0,
                        }],
                    }],
                }
                for document_id, data in selected[:limit]
            ],
            "diagnostics": {"engine": "synthetic-hybrid"},
        }

    def open_document(self, **arguments):
        document_id = arguments["logical_document_id"]
        data = DOCS[document_id]
        self.inspections.append(document_id)
        return {
            "provider": "synthetic-reader",
            "document_alias": arguments["document_alias"],
            "records": [{
                "document_alias": arguments["document_alias"],
                "record_ordinal": 0,
                "occurred_at": f'{data["day"]}T16:00:00Z',
                "content": data["content"],
                "content_start": 0,
                "content_end": len(data["content"]),
                "content_length": len(data["content"]),
                "content_byte_start": 0,
                "content_byte_end": len(data["content"].encode()),
                "content_length_bytes": len(data["content"].encode()),
                "content_complete": True,
                "receipts": [data["receipt"]],
            }],
            "opened_receipts": [data["receipt"]],
            "next_cursor": None,
            "complete": True,
        }

    def find_documents(self, **arguments):
        matches = []
        opened = []
        for document_id in arguments["logical_document_ids"]:
            data = DOCS[document_id]
            if any(
                pattern.casefold() in data["content"].casefold()
                for pattern in arguments["patterns"]
            ):
                self.inspections.append(document_id)
                receipt = data["receipt"]
                opened.append(receipt)
                matches.append({
                    "document_alias": arguments["document_aliases"][document_id],
                    "record_ordinal": 0,
                    "occurred_at": f'{data["day"]}T16:00:00Z',
                    "content": data["content"],
                    "receipts": [receipt],
                })
        return {
            "provider": "synthetic-reader",
            "matches": matches,
            "opened_receipts": opened,
            "complete": True,
        }

    def execute_agent_program(self, _program, *, logical_document_ids, **_kwargs):
        opened = []
        lines = []
        for document_id in logical_document_ids:
            data = DOCS[document_id]
            self.inspections.append(document_id)
            opened.append(data["receipt"])
            lines.extend([
                f'RECALL_EVIDENCE {data["receipt"]}',
                json.dumps({
                    "content": data["content"],
                    "occurred_at": f'{data["day"]}T16:00:00Z',
                    "receipts": [data["receipt"]],
                }, separators=(",", ":")),
            ])
        return {
            "provider": "synthetic-exec",
            "stdout": "\n".join(lines),
            "stderr": "",
            "exit_code": 0,
            "complete": True,
            "stopped_reason": "completed",
            "opened_receipts": opened,
            "timing": {"totalMs": 10, "queueMs": 1, "executeMs": 9},
        }


def evaluate_model(model: str, *, base_url: str, worker: Path) -> dict[str, Any]:
    retrieval = SyntheticDailyRetrieval()
    transport = SubprocessPiTransport(
        ("node", str(worker)),
        model_base_url=base_url,
        route_kind="private_broker",
        provider="broker",
        expected_route_identity="10.255.254.1",
        environment={"PATH": os.environ["PATH"]},
    )
    started = time.monotonic()
    try:
        result = RecallAgentService(
            PiRunner(transport, model_alias=model, thinking="low"),
        ).use_recall(principal(), request(model), retrieval)
    except AgentExecutionError as error:
        return {
            "model": model,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "passed": False,
            "error_code": error.code,
            "tool_sequence": [event.get("tool") for event in error.trace],
            "hint_calls": len(retrieval.hint_queries),
            "inspected_documents": len(set(retrieval.inspections)),
        }
    tool_sequence = [
        event.get("tool")
        for event in result["trace"]
        if event.get("tool")
    ]
    citations = set(result["result"]["citations"])
    expected = {data["receipt"] for data in DOCS.values()}
    gates = {
        "grounded_complete": result["result"]["status"] == "complete",
        "both_days_cited": citations == expected,
        "both_documents_inspected": len(set(retrieval.inspections)) == 2,
        "no_budget_exhaustion": all(
            event.get("error_code") != "agent_tool_budget_exhausted"
            for event in result["trace"]
        ),
    }
    return {
        "model": model,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "passed": all(gates.values()),
        "gates": gates,
        "status": result["result"]["status"],
        "tool_sequence": tool_sequence,
        "hint_calls": len(retrieval.hint_queries),
        "inspected_documents": len(set(retrieval.inspections)),
        "citation_count": len(citations),
        "used_coverage_map": "recall.map" in tool_sequence,
    }


def main() -> None:
    parser = argparse.ArgumentParser(prog="recall-pi-model-matrix")
    parser.add_argument(
        "--models",
        nargs="+",
        default=[
            "gemma-4-31b",
            "deepseek-v4-flash-0731",
            "gpt-5.6-luna",
        ],
    )
    parser.add_argument(
        "--base-url",
        default="http://10.255.254.1:9400",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    worker = (
        Path(__file__).resolve().parents[1]
        / "server" / "pi-agent" / "dist" / "worker.js"
    )
    if not worker.is_file():
        raise SystemExit("build recall/server/pi-agent before running the matrix")
    rows = [
        evaluate_model(model, base_url=args.base_url, worker=worker)
        for model in args.models
    ]
    report = {
        "schema": "recall.pi-model-matrix.v1",
        "case": "synthetic-person-two-day-coverage",
        "rows": rows,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
