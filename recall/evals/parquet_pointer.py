"""Synthetic, content-free gate for the Parquet passage planning plane."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

import pyarrow as pa

from recall_server.parquet_scan import _parquet_bytes, _schemas


PEOPLE = tuple(f"Employee {ordinal}" for ordinal in range(1, 7))
TOPICS = (
    "archive identity",
    "oauth callback",
    "parquet projection",
    "embedding recall",
    "collector cadence",
    "tenant isolation",
)


def _pointer_schema() -> Any:
    utc = pa.timestamp("us", tz="UTC")
    strings = pa.list_(pa.string())
    return pa.schema([
        ("schema_version", pa.int16()),
        ("tenant_id", pa.string()),
        ("source_id", pa.string()),
        ("logical_document_id", pa.string()),
        ("revision", pa.int32()),
        ("passage_id", pa.string()),
        ("ordinal", pa.int32()),
        ("first_occurred_at", utc),
        ("last_occurred_at", utc),
        ("token_count", pa.int32()),
        ("roles", strings),
        ("receipts", strings),
        ("actor_ids", strings),
        ("actor_names", strings),
        ("actor_relations", strings),
        ("text", pa.large_string()),
    ])


def _noise(seed: str, size: int) -> str:
    blocks = []
    ordinal = 0
    while sum(map(len, blocks)) < size:
        blocks.append(hashlib.sha256(f"{seed}:{ordinal}".encode()).hexdigest())
        ordinal += 1
    return "".join(blocks)[:size]


def fixture() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict]]:
    """Return raw rows, thin passage rows, and closed expected cases."""

    raw: list[dict[str, Any]] = []
    passages: list[dict[str, Any]] = []
    cases = []
    start = datetime(2026, 8, 10, tzinfo=timezone.utc)
    for person_index, (person, topic) in enumerate(zip(PEOPLE, TOPICS, strict=True)):
        actor_id = f"actor:{person_index + 1}"
        for document_index in range(2):
            document_id = f"ldoc_{person_index + 1:02x}{document_index:02x}" + "a" * 28
            identifier = f"SYN-{person_index + 1:02d}-{document_index + 1:02d}-A9F3"
            occurred_at = start + timedelta(hours=person_index * 4 + document_index)
            receipts = [
                f"recall://source:synthetic/{document_id}?rev=1#item=0"
            ]
            visible = (
                f"{person} worked on {topic}. Exact marker {identifier}. "
                "The source document remains authoritative."
            )
            for record_index in range(12):
                content = visible if record_index in {0, 11} else _noise(
                    f"{document_id}:{record_index}", 4_096
                )
                raw.append({
                    "schema_version": 2,
                    "tenant_id": "tenant:synthetic",
                    "source_id": f"source:synthetic:{person_index + 1}",
                    "logical_document_id": document_id,
                    "revision": 1,
                    "ordinal": record_index,
                    "occurred_at": occurred_at + timedelta(minutes=record_index),
                    "event_kind": "transcript_record",
                    "roles": ["user"] if record_index == 0 else ["tool"],
                    "receipts": receipts,
                    "actor_ids": [actor_id] if record_index == 0 else [],
                    "actor_names": [person] if record_index == 0 else [],
                    "actor_relations": ["contributor"] if record_index == 0 else [],
                    "search_text": content,
                    "record_json": '{"content":"' + content + '"}',
                })
            passages.append({
                "schema_version": 2,
                "tenant_id": "tenant:synthetic",
                "source_id": f"source:synthetic:{person_index + 1}",
                "logical_document_id": document_id,
                "revision": 1,
                "passage_id": "psg_" + hashlib.sha256(
                    document_id.encode()
                ).hexdigest()[:32],
                "ordinal": 0,
                "first_occurred_at": occurred_at,
                "last_occurred_at": occurred_at + timedelta(minutes=11),
                "token_count": len(visible.split()),
                "roles": ["assistant", "user"],
                "receipts": receipts,
                "actor_ids": [actor_id],
                "actor_names": [person],
                "actor_relations": ["contributor"],
                "text": visible,
            })
            cases.extend((
                {
                    "stratum": "person_time",
                    "person": person,
                    "expected": document_id,
                },
                {
                    "stratum": "project_topic",
                    "query": topic,
                    "expected": document_id,
                },
                {
                    "stratum": "exact_identifier",
                    "query": identifier,
                    "expected": document_id,
                },
            ))
    cases.extend((
        {"stratum": "fleet_inventory", "expected_count": len(PEOPLE)},
        {"stratum": "team_time", "expected_count": len(passages)},
        {"stratum": "cold_negative", "query": "ZZZ-NOT-PRESENT", "expected_count": 0},
    ))
    return raw, passages, cases


def evaluate(
    passages: list[dict[str, Any]],
    cases: list[dict],
    *,
    complete: bool = True,
) -> dict[str, Any]:
    """Score structural candidate recall and evidence-pointer integrity."""

    successes = 0
    exact_successes = 0
    exact_total = 0
    positives = 0
    supported = 0
    by_stratum: dict[str, dict[str, int]] = {}
    for case in cases:
        stratum = case["stratum"]
        if stratum == "fleet_inventory":
            candidates = {
                name for row in passages for name in row["actor_names"]
            }
            passed = len(candidates) == case["expected_count"]
        elif stratum == "team_time":
            candidates = {row["logical_document_id"] for row in passages}
            passed = len(candidates) == case["expected_count"]
        elif stratum == "person_time":
            candidates = {
                row["logical_document_id"]
                for row in passages
                if case["person"] in row["actor_names"]
            }
            passed = case["expected"] in candidates
        else:
            needle = case["query"].casefold()
            candidates = {
                row["logical_document_id"]
                for row in passages
                if needle in row["text"].casefold()
            }
            passed = (
                len(candidates) == case.get("expected_count")
                if "expected_count" in case
                else case["expected"] in candidates
            )
        if stratum == "exact_identifier":
            exact_total += 1
            exact_successes += int(passed)
        if stratum != "cold_negative":
            positives += 1
            supported += int(
                passed
                and all(
                    row["receipts"]
                    for row in passages
                    if row["logical_document_id"] in candidates
                )
            )
        successes += int(passed)
        cell = by_stratum.setdefault(stratum, {"passed": 0, "total": 0})
        cell["passed"] += int(passed)
        cell["total"] += 1
    return {
        "complete": complete,
        "cases_passed": successes,
        "cases_total": len(cases),
        "candidate_recall": successes / len(cases),
        "exact_identifier_recall": exact_successes / exact_total,
        "positive_receipt_support": supported / positives,
        "by_stratum": by_stratum,
        "passed": (
            complete
            and successes == len(cases)
            and supported == positives
        ),
    }


def run() -> dict[str, Any]:
    raw, passages, cases = fixture()
    raw_bytes = len(_parquet_bytes(raw, _schemas()["records"]))
    pointer_bytes = len(_parquet_bytes(passages, _pointer_schema()))
    score = evaluate(passages, cases)
    return {
        "schema_version": "recall.parquet-pointer-eval.v1",
        **score,
        "raw_parquet_bytes": raw_bytes,
        "pointer_parquet_bytes": pointer_bytes,
        "physical_reduction": raw_bytes / pointer_bytes,
    }
