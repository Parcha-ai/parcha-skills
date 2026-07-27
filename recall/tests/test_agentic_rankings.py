from __future__ import annotations

import json
import stat
import tempfile
import unittest
from pathlib import Path

from evals.agentic_rankings import (
    _retrieval_error,
    rank_private_questions,
    resolve_logical_boundaries,
    resolve_passage_boundaries,
)
from recall_server.logical_evidence import logical_document_id


REPO_ROOT = Path(__file__).resolve().parents[2]


def private_directory(root: Path) -> Path:
    directory = root / "private"
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    return directory


def private_write(path: Path, rows: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
    )
    path.chmod(0o600)


class AgenticRankingsTest(unittest.TestCase):
    def test_classifies_partial_retrieval_as_a_backend_error(self) -> None:
        self.assertEqual(
            _retrieval_error(
                {
                    "diagnostics": {
                        "lexical_mode": "deadline-exceeded",
                        "semantic_status": "ok",
                    }
                }
            ),
            "RetrievalLexicalDeadlineExceeded",
        )
        self.assertEqual(
            _retrieval_error(
                {
                    "diagnostics": {
                        "lexical_mode": "strict",
                        "semantic_status": "deadline-exceeded",
                    }
                }
            ),
            "RetrievalSemanticDeadlineExceeded",
        )
        self.assertEqual(
            _retrieval_error(
                {
                    "diagnostics": {
                        "lexical_mode": "strict",
                        "semantic_status": "unavailable",
                    }
                }
            ),
            "RetrievalBackendUnavailable",
        )
        self.assertEqual(
            _retrieval_error(
                {
                    "diagnostics": {
                        "lexical_mode": "strict",
                        "semantic_status": "ok",
                    }
                }
            ),
            "",
        )
        self.assertEqual(
            _retrieval_error(
                {
                    "diagnostics": {
                        "dense_status": "ok",
                        "passage_lexical_status": "deadline-exceeded",
                        "sparse_status": "ok",
                    }
                }
            ),
            "RetrievalLexicalDeadlineExceeded",
        )

    def test_resolves_ranked_hits_to_deduplicated_current_boundaries(self) -> None:
        tenant = "tenant:company:synthetic"
        source = "codex:linux:synthetic"
        missing_source = "claude:linux:synthetic"
        first_parent = "session-one"
        missing_parent = "session-two"
        first_id = logical_document_id(tenant, source, first_parent)

        candidates = resolve_logical_boundaries(
            [
                {
                    "source_id": source,
                    "native_id": "event-one",
                    "native_parent_id": first_parent,
                },
                {
                    "source_id": source,
                    "native_id": "event-two",
                    "native_parent_id": first_parent,
                },
                {
                    "source_id": missing_source,
                    "native_id": missing_parent,
                    "native_parent_id": None,
                },
            ],
            tenant_id=tenant,
            authorized_sources=(source,),
            lookup=lambda _keys: {
                (source, first_parent): {
                    "logical_document_id": first_id,
                    "revision": 7,
                }
            },
        )

        self.assertEqual(len(candidates), 2)
        self.assertEqual(candidates[0], {
            "logical_document_id": first_id,
            "source_id": source,
            "revision": 7,
            "pointer_valid": True,
            "authorized": True,
        })
        self.assertEqual(candidates[1]["revision"], 1)
        self.assertFalse(candidates[1]["pointer_valid"])
        self.assertFalse(candidates[1]["authorized"])

    def test_validates_passage_hits_against_current_boundaries(self) -> None:
        tenant = "tenant:company:synthetic"
        source = "codex:linux:synthetic"
        parent = "session-one"
        document_id = logical_document_id(tenant, source, parent)

        candidates = resolve_passage_boundaries(
            [
                {
                    "source_id": source,
                    "native_parent_id": parent,
                    "logical_document_id": document_id,
                    "revision": 7,
                },
                {
                    "source_id": source,
                    "native_parent_id": parent,
                    "logical_document_id": document_id,
                    "revision": 7,
                },
            ],
            tenant_id=tenant,
            authorized_sources=(source,),
            lookup=lambda _keys: {
                (source, parent): {
                    "logical_document_id": document_id,
                    "revision": 7,
                }
            },
        )

        self.assertEqual(candidates, [{
            "logical_document_id": document_id,
            "source_id": source,
            "revision": 7,
            "pointer_valid": True,
            "authorized": True,
        }])

    def test_writes_private_rankings_and_returns_aggregate_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            private = private_directory(root)
            questions = private / "questions.jsonl"
            output = private / "rankings.jsonl"
            marker = "PRIVATE SYNTHETIC QUESTION MARKER"
            private_write(
                questions,
                [
                    {
                        "id": f"case_{ordinal:032x}",
                        "question": f"{marker} {ordinal}",
                    }
                    for ordinal in range(2)
                ],
            )
            calls: list[str] = []

            def search(question: str) -> dict:
                calls.append(question)
                return {
                    "results": [
                        {
                            "source_id": "codex:linux:synthetic",
                            "native_id": "event-one",
                            "native_parent_id": "session-one",
                        }
                    ]
                }

            def resolve(_results: list[dict]) -> list[dict]:
                return [{
                    "logical_document_id": "ldoc_" + "1" * 32,
                    "source_id": "codex:linux:synthetic",
                    "revision": 1,
                    "pointer_valid": True,
                    "authorized": True,
                }]

            report = rank_private_questions(
                questions,
                output,
                search=search,
                resolve=resolve,
                repo_root=REPO_ROOT,
                run_id="synthetic-run",
                workers=2,
                expected_cases=2,
            )

            self.assertEqual(len(calls), 2)
            self.assertEqual(report["case_count"], 2)
            self.assertEqual(report["candidate_count"], 2)
            self.assertEqual(report["backend_error_rate"], 0)
            self.assertEqual(report["pointer_integrity"], 1)
            self.assertEqual(report["authorization_violation_rate"], 0)
            self.assertNotIn(marker, json.dumps(report))
            self.assertEqual(
                stat.S_IMODE(output.stat().st_mode),
                0o600,
            )
            rendered = output.read_text()
            self.assertNotIn(marker, rendered)
            self.assertNotIn("question", rendered)

    def test_records_only_error_class_when_backend_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            private = private_directory(root)
            questions = private / "questions.jsonl"
            output = private / "rankings.jsonl"
            private_write(
                questions,
                [{
                    "id": "case_" + "2" * 32,
                    "question": "Private failure question",
                }],
            )

            def fail(_question: str) -> dict:
                raise RuntimeError("secret backend detail")

            report = rank_private_questions(
                questions,
                output,
                search=fail,
                resolve=lambda _results: [],
                repo_root=REPO_ROOT,
                run_id="failure-run",
                expected_cases=1,
            )

            self.assertEqual(report["backend_error_count"], 1)
            rendered = output.read_text()
            self.assertIn("RuntimeError", rendered)
            self.assertNotIn("secret backend detail", rendered)

    def test_rejects_resolver_payload_outside_closed_candidate_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            private = private_directory(root)
            questions = private / "questions.jsonl"
            output = private / "rankings.jsonl"
            private_write(
                questions,
                [{
                    "id": "case_" + "3" * 32,
                    "question": "Private schema question",
                }],
            )

            report = rank_private_questions(
                questions,
                output,
                search=lambda _question: {"results": []},
                resolve=lambda _results: [{
                    "logical_document_id": "ldoc_" + "1" * 32,
                    "source_id": "codex:linux:synthetic",
                    "revision": 1,
                    "pointer_valid": True,
                    "authorized": True,
                    "text": "must never reach the private ranking file",
                }],
                repo_root=REPO_ROOT,
                run_id="closed-schema-run",
                expected_cases=1,
            )

            self.assertEqual(report["backend_error_count"], 1)
            self.assertNotIn("must never", output.read_text())


if __name__ == "__main__":
    unittest.main()
