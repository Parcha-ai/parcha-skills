from __future__ import annotations

import json
import unittest

from evals.agentic_reader_oracle import (
    OracleDocumentRetrieval,
    validate_oracle_documents,
)
from evals.retrieval import EvaluationInputError


SOURCE = "claude:linux:test"
DOCUMENT = {
    "source_id": SOURCE,
    "logical_document_id": "ldoc_" + "1" * 32,
    "revision": 3,
    "first_occurred_at": "2026-07-01T00:00:00+00:00",
    "last_occurred_at": "2026-07-01T01:00:00+00:00",
}


class FakeExecution:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def execute_agent_program(self, program: str, **kwargs):
        self.calls.append({"program": program, **kwargs})
        return {"stdout": "", "opened_receipts": []}

    def inspect_documents(self, **kwargs):
        self.calls.append({"operation": "inspect", **kwargs})
        return {"matches": [], "opened_receipts": []}

    def passage_hints(self, query, *, filters, limit):
        self.calls.append({
            "operation": "pointers",
            "query": query,
            "filters": filters,
            "limit": limit,
        })
        return {
            "results": [
                {
                    "logical_document_id": DOCUMENT[
                        "logical_document_id"
                    ],
                    "matching_ranges": [{
                        "kind": "dense",
                        "text": "normal production pointer",
                        "spans": [{
                            "record_ordinal": 9,
                            "record_count": 2,
                        }],
                        "receipts": [
                            f"recall://{SOURCE}/pointer?rev=1#item=0"
                        ],
                    }],
                },
                {
                    "logical_document_id": "ldoc_" + "f" * 32,
                    "matching_ranges": [{
                        "text": "foreign document pointer",
                    }],
                },
            ],
            "diagnostics": {
                "engine": "lossless-passages-v1",
                "dense_status": "ok",
            },
        }


class ReaderOracleTests(unittest.TestCase):
    def test_hint_packet_contains_identity_metadata_only(self):
        retrieval = OracleDocumentRetrieval(
            FakeExecution(),
            documents=[DOCUMENT],
            authorized_sources=(SOURCE,),
        )

        result = retrieval.passage_hints(
            "What changed?",
            filters={},
            limit=20,
        )

        self.assertEqual(
            set(result),
            {"results", "diagnostics"},
        )
        self.assertEqual(len(result["results"]), 1)
        candidate = result["results"][0]
        self.assertEqual(candidate["logical_document_id"], DOCUMENT["logical_document_id"])
        self.assertEqual(candidate["matching_ranges"], [])
        rendered = json.dumps(result, sort_keys=True).casefold()
        for forbidden in (
            "gold_fact",
            "gold_receipt",
            "expected_answer",
            "recall://",
            "content",
            "text",
        ):
            self.assertNotIn(forbidden, rendered)

    def test_rejects_truth_evidence_and_unknown_fields(self):
        for field, value in (
            ("gold_facts", ["secret"]),
            ("receipts", ["recall://source/item"]),
            ("content", "source body"),
            ("expected_answer", "answer"),
        ):
            with self.subTest(field=field):
                with self.assertRaises(EvaluationInputError):
                    validate_oracle_documents(
                        [{**DOCUMENT, field: value}],
                        authorized_sources=(SOURCE,),
                    )

    def test_live_embedding_pointers_are_soft_and_gold_filtered(self):
        retrieval = OracleDocumentRetrieval(
            FakeExecution(),
            documents=[DOCUMENT],
            authorized_sources=(SOURCE,),
            include_live_pointers=True,
        )

        result = retrieval.passage_hints(
            "What changed?",
            filters={},
            limit=20,
        )

        self.assertEqual(len(result["results"]), 1)
        candidate = result["results"][0]
        self.assertEqual(
            candidate["matching_ranges"][0]["kind"],
            "dense",
        )
        self.assertEqual(
            candidate["matching_ranges"][0]["spans"],
            [{"record_ordinal": 9, "record_count": 2}],
        )
        self.assertEqual(
            result["diagnostics"]["engine"],
            "reader-oracle-soft-pointers-v1",
        )
        self.assertNotIn(
            "foreign document pointer",
            json.dumps(result),
        )

    def test_rejects_duplicates_and_unauthorized_sources(self):
        with self.assertRaises(EvaluationInputError):
            validate_oracle_documents(
                [DOCUMENT, DOCUMENT],
                authorized_sources=(SOURCE,),
            )
        with self.assertRaises(EvaluationInputError):
            validate_oracle_documents(
                [DOCUMENT],
                authorized_sources=("codex:linux:test",),
            )

    def test_rejects_invalid_identity_revision_and_time(self):
        invalid = (
            {**DOCUMENT, "logical_document_id": "ldoc_wrong"},
            {**DOCUMENT, "revision": True},
            {
                **DOCUMENT,
                "first_occurred_at": "2026-07-02T00:00:00+00:00",
                "last_occurred_at": "2026-07-01T00:00:00+00:00",
            },
        )
        for document in invalid:
            with self.subTest(document=document):
                with self.assertRaises(EvaluationInputError):
                    validate_oracle_documents(
                        [document],
                        authorized_sources=(SOURCE,),
                    )

    def test_execution_cannot_introduce_another_document(self):
        inner = FakeExecution()
        retrieval = OracleDocumentRetrieval(
            inner,
            documents=[DOCUMENT],
            authorized_sources=(SOURCE,),
        )

        with self.assertRaises(EvaluationInputError):
            retrieval.execute_agent_program(
                "true",
                logical_document_ids=("ldoc_" + "2" * 32,),
                record_spans={},
                routing_receipts={},
                timeout_seconds=10,
            )
        self.assertEqual(inner.calls, [])

    def test_execution_delegates_supplied_document_only(self):
        inner = FakeExecution()
        retrieval = OracleDocumentRetrieval(
            inner,
            documents=[DOCUMENT],
            authorized_sources=(SOURCE,),
        )

        result = retrieval.execute_agent_program(
            "true",
            logical_document_ids=(DOCUMENT["logical_document_id"],),
            record_spans={DOCUMENT["logical_document_id"]: ()},
            routing_receipts={DOCUMENT["logical_document_id"]: ()},
            timeout_seconds=10,
        )

        self.assertEqual(result["opened_receipts"], [])
        self.assertEqual(
            inner.calls[0]["logical_document_ids"],
            (DOCUMENT["logical_document_id"],),
        )

    def test_native_inspection_delegates_supplied_document_only(self):
        inner = FakeExecution()
        retrieval = OracleDocumentRetrieval(
            inner,
            documents=[DOCUMENT],
            authorized_sources=(SOURCE,),
        )

        result = retrieval.inspect_documents(
            logical_document_ids=(DOCUMENT["logical_document_id"],),
            query="synthetic",
            scope="full_documents",
            literal=True,
            context=1,
            limit=6,
            record_spans={},
            routing_receipts={},
            timeout_seconds=10,
        )

        self.assertEqual(result["matches"], [])
        self.assertEqual(inner.calls[0]["operation"], "inspect")
        self.assertEqual(
            inner.calls[0]["logical_document_ids"],
            (DOCUMENT["logical_document_id"],),
        )

    def test_native_inspection_cannot_introduce_another_document(self):
        inner = FakeExecution()
        retrieval = OracleDocumentRetrieval(
            inner,
            documents=[DOCUMENT],
            authorized_sources=(SOURCE,),
        )

        with self.assertRaises(EvaluationInputError):
            retrieval.inspect_documents(
                logical_document_ids=("ldoc_" + "2" * 32,),
                query="synthetic",
                scope="full_documents",
                literal=True,
                context=1,
                limit=6,
                record_spans={},
                routing_receipts={},
                timeout_seconds=10,
            )
        self.assertEqual(inner.calls, [])


if __name__ == "__main__":
    unittest.main()
