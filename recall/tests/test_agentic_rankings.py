from __future__ import annotations

import json
import stat
import tempfile
import unittest
from pathlib import Path

from evals.agentic_candidate_matrix import (
    _EvaluationDeadlineStore,
    _validate_matrix,
    write_candidate_matrix,
)
from evals.agentic_representation_matrix import (
    repair_representation_matrix,
    write_representation_matrix,
)
from evals.agentic_rankings import (
    _query_bundles,
    _retrieval_error,
    _select_arm,
    parser,
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
    def test_representation_matrix_repairs_only_failed_query_cells(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            private = private_directory(Path(temporary))
            questions = private / "questions.jsonl"
            bundle = private / "bundle.json"
            initial = private / "initial.jsonl"
            repaired = private / "repaired.jsonl"
            case_id = "case_" + "9" * 32
            question = "PRIVATE QUESTION"
            expansion = "PRIVATE EXPANSION"
            private_write(
                questions,
                [{"id": case_id, "question": question}],
            )
            bundle.write_text(json.dumps({
                "private_rows": [{
                    "id": case_id,
                    "queries": [expansion],
                }]
            }))
            bundle.chmod(0o600)

            def candidate(value: str) -> dict:
                return {
                    "logical_document_id": "ldoc_" + value * 32,
                    "source_id": "codex:linux:synthetic",
                    "revision": 1,
                    "pointer_valid": True,
                    "authorized": True,
                }

            def initial_search(query: str):
                if query == question:
                    return {
                        "arm-a": ([], "deadline-exceeded"),
                        "arm-b": ([candidate("1")], "ok"),
                    }
                return {
                    "arm-a": ([candidate("2")], "ok"),
                    "arm-b": ([candidate("2")], "ok"),
                }

            first = write_representation_matrix(
                questions,
                bundle,
                initial,
                arm_names=("arm-a", "arm-b"),
                search=initial_search,
                resolve=lambda values: values,
                repo_root=REPO_ROOT,
                run_id="synthetic-initial",
                expected_cases=1,
            )
            repaired_queries = []

            def repair_search(query: str):
                repaired_queries.append(query)
                return {
                    "arm-a": ([candidate("1")], "ok"),
                    "arm-b": ([candidate("1")], "ok"),
                }

            final = repair_representation_matrix(
                questions,
                bundle,
                initial,
                repaired,
                arm_names=("arm-a", "arm-b"),
                search=repair_search,
                resolve=lambda values: values,
                repo_root=REPO_ROOT,
                run_id="synthetic-repair",
                expected_cases=1,
            )

            self.assertEqual(first["backend_error_count"], 1)
            self.assertEqual(final["repaired_query_count"], 1)
            self.assertEqual(final["backend_error_count"], 0)
            self.assertEqual(repaired_queries, [question])
            rendered = repaired.read_text()
            self.assertNotIn(question, rendered)
            self.assertNotIn(expansion, rendered)

    def test_representation_matrix_is_dynamic_grounded_and_private(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            private = private_directory(Path(temporary))
            questions = private / "questions.jsonl"
            bundle = private / "bundle.json"
            output = private / "representations.jsonl"
            case_id = "case_" + "1" * 32
            marker = "PRIVATE REPRESENTATION QUESTION"
            private_write(
                questions,
                [{"id": case_id, "question": marker}],
            )
            bundle.write_text(json.dumps({
                "private_rows": [{
                    "id": case_id,
                    "queries": ["private representation expansion"],
                }]
            }))
            bundle.chmod(0o600)
            candidate = {
                "logical_document_id": "ldoc_" + "1" * 32,
                "source_id": "codex:linux:synthetic",
                "revision": 1,
                "pointer_valid": True,
                "authorized": True,
            }

            report = write_representation_matrix(
                questions,
                bundle,
                output,
                arm_names=("openai-small-plain", "openai-small-context"),
                search=lambda _query: {
                    "openai-small-plain": ([candidate], "ok"),
                    "openai-small-context": ([candidate], "ok"),
                },
                resolve=lambda values: values,
                repo_root=REPO_ROOT,
                run_id="synthetic-representations",
                expected_cases=1,
            )

            self.assertEqual(report["backend_error_count"], 0)
            self.assertEqual(
                report["arms"],
                ["openai-small-plain", "openai-small-context"],
            )
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
            rendered = output.read_text()
            self.assertNotIn(marker, rendered)
            self.assertNotIn("private representation expansion", rendered)

    def test_candidate_matrix_rejects_ungrounded_bundle_candidates(self) -> None:
        candidate = {
            "logical_document_id": "ldoc_" + "1" * 32,
            "source_id": "codex:linux:synthetic",
            "revision": 1,
            "pointer_valid": True,
            "authorized": True,
        }
        empty_arms = {
            arm: []
            for arm in (
                "dense",
                "passage-lexical",
                "sparse-exact",
                "fused",
            )
        }
        bundle_rankings = {
            **empty_arms,
            "fused": [candidate],
        }

        with self.assertRaisesRegex(ValueError, "not grounded"):
            _validate_matrix([{
                "id": "case_" + "1" * 32,
                "queries": [{
                    "ordinal": 0,
                    "arms": empty_arms,
                    "statuses": {arm: "ok" for arm in empty_arms},
                    "latency_ms": 1.0,
                }],
                "bundle_rankings": bundle_rankings,
            }])

    def test_candidate_matrix_deadline_proxy_is_evaluator_local(self) -> None:
        class Store:
            search_deadline_ms = 5000
            semantic_runtime = object()

            def connect(self):
                return "connection"

            def _execute_bounded(
                self,
                connection,
                sql,
                values,
                deadline_at,
            ):
                return connection, sql, values, deadline_at

        production = Store()
        evaluation = _EvaluationDeadlineStore(
            production,
            search_deadline_ms=30_000,
        )

        self.assertEqual(production.search_deadline_ms, 5000)
        self.assertEqual(evaluation.search_deadline_ms, 30_000)
        self.assertEqual(evaluation.connect(), "connection")
        self.assertEqual(
            evaluation._execute_bounded(
                "connection",
                "SELECT 1",
                (),
                3.0,
            ),
            ("connection", "SELECT 1", (), 3.0),
        )

    def test_candidate_matrix_is_complete_private_and_content_free(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            private = private_directory(Path(temporary))
            questions = private / "questions.jsonl"
            bundle = private / "bundle.json"
            output = private / "matrix.jsonl"
            case_ids = [
                "case_" + "1" * 32,
                "case_" + "2" * 32,
            ]
            marker = "PRIVATE MATRIX QUESTION MARKER"
            private_write(
                questions,
                [
                    {
                        "id": case_id,
                        "question": f"{marker} {ordinal}",
                    }
                    for ordinal, case_id in enumerate(case_ids)
                ],
            )
            bundle.write_text(json.dumps({
                "private_rows": [
                    {"id": case_id, "queries": ["private focused query"]}
                    for case_id in case_ids
                ]
            }))
            bundle.chmod(0o600)
            candidate = {
                "logical_document_id": "ldoc_" + "1" * 32,
                "source_id": "codex:linux:synthetic",
                "revision": 1,
                "pointer_valid": True,
                "authorized": True,
            }

            def search(_query: str) -> dict:
                return {
                    "results": [candidate],
                    "arms": {
                        "dense": [candidate],
                        "passage-lexical": [candidate],
                        "sparse-exact": [candidate],
                    },
                    "diagnostics": {
                        "engine": "synthetic",
                        "dense_status": "ok",
                        "passage_lexical_status": "ok",
                        "sparse_status": "ok",
                    },
                }

            report = write_candidate_matrix(
                questions,
                bundle,
                output,
                search=search,
                resolve=lambda values: values,
                fuse=lambda rankings, _limit: (
                    rankings[0] if rankings else []
                ),
                repo_root=REPO_ROOT,
                run_id="synthetic-matrix",
                expected_cases=2,
            )

            self.assertEqual(report["case_count"], 2)
            self.assertEqual(report["query_count"], 4)
            self.assertEqual(report["backend_error_count"], 0)
            self.assertEqual(report["pointer_integrity"], 1)
            self.assertEqual(report["authorization_violation_rate"], 0)
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
            rendered = output.read_text()
            self.assertNotIn(marker, rendered)
            self.assertNotIn("private focused query", rendered)
            self.assertNotIn("question", rendered)

    def test_accepts_an_explicit_passage_policy_for_live_rankings(self) -> None:
        args = parser().parse_args([
            "--input", "/private/questions.jsonl",
            "--output", "/private/rankings.jsonl",
            "--repo-root", str(REPO_ROOT),
            "--run-id", "passage-arm",
            "--tenant", "tenant:company:synthetic",
            "--source", "codex:linux:synthetic",
            "--retrieval-mode", "passage",
            "--target-tokens", "512",
            "--overlap-tokens", "64",
            "--candidate-depth", "50",
            "--expected-cases", "15",
            "--arm", "dense",
            "--query-bundle", "/private/query-bundle.json",
        ])

        self.assertEqual(args.target_tokens, 512)
        self.assertEqual(args.overlap_tokens, 64)
        self.assertEqual(args.candidate_depth, 50)
        self.assertEqual(args.expected_cases, 15)
        self.assertEqual(args.arm, "dense")
        self.assertEqual(args.query_bundle, "/private/query-bundle.json")

    def test_query_bundle_requires_exact_private_case_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            private = private_directory(Path(temporary))
            bundle = private / "queries.json"
            case_ids = {
                "case_" + "1" * 32,
                "case_" + "2" * 32,
            }
            bundle.write_text(json.dumps({
                "private_rows": [
                    {"id": case_id, "queries": ["focused query"]}
                    for case_id in sorted(case_ids)
                ]
            }))
            bundle.chmod(0o600)

            queries, payload = _query_bundles(
                bundle,
                repo_root=REPO_ROOT,
                case_ids=case_ids,
            )

            self.assertEqual(set(queries), case_ids)
            self.assertEqual(
                set(json.loads(payload)["private_rows"][0]),
                {"id", "queries"},
            )

            bundle.write_text(json.dumps({
                "private_rows": [{
                    "id": next(iter(case_ids)),
                    "queries": ["focused query"],
                }]
            }))
            bundle.chmod(0o600)
            with self.assertRaisesRegex(
                ValueError,
                "coverage",
            ):
                _query_bundles(
                    bundle,
                    repo_root=REPO_ROOT,
                    case_ids=case_ids,
                )

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

    def test_arm_metrics_ignore_unrelated_arm_failures(self) -> None:
        response = {
            "results": [{"logical_document_id": "fused"}],
            "arms": {
                "dense": [{"logical_document_id": "dense"}],
                "passage-lexical": [{"logical_document_id": "lexical"}],
                "sparse-exact": [{"logical_document_id": "sparse"}],
            },
            "diagnostics": {
                "engine": "synthetic",
                "dense_status": "ok",
                "passage_lexical_status": "deadline-exceeded",
                "sparse_status": "ok",
            },
        }

        dense = _select_arm(response, "dense")
        lexical = _select_arm(response, "passage-lexical")

        self.assertEqual(dense["results"], response["arms"]["dense"])
        self.assertEqual(_retrieval_error(dense), "")
        self.assertEqual(
            _retrieval_error(lexical),
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
