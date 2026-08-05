from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / "server"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SERVER))

from evals.agentic_query_plans import write_query_bundle  # noqa: E402
from recall_server.candidate_planning import (  # noqa: E402
    CandidatePlanningError,
    CandidateQueryPlan,
    CandidateScope,
    plan_candidate_queries,
)


class CandidatePlanningTest(unittest.TestCase):
    def test_agent_owns_queries_while_host_preserves_scope(self) -> None:
        scope = CandidateScope(
            source_ids=("codex:linux:synthetic",),
            since="2026-07-01T00:00:00Z",
            until="2026-07-02T00:00:00Z",
        )
        seen = {}

        def generate(messages):
            seen["messages"] = messages
            return json.dumps(
                {
                    "queries": [
                        "project status decision",
                        "implementation rollout outcome",
                        "remaining blockers next steps",
                    ]
                }
            )

        plan = plan_candidate_queries(
            "What happened to Project Alpha?",
            scope=scope,
            generate=generate,
        )
        self.assertEqual(plan.scope, scope)
        self.assertEqual(len(plan.queries), 3)
        model_input = json.loads(seen["messages"][1]["content"])
        self.assertEqual(model_input["scope"]["source_ids"], list(scope.source_ids))
        self.assertNotIn("truth", model_input)
        self.assertNotIn("gold", model_input)

    def test_model_cannot_widen_scope_or_add_contract_fields(self) -> None:
        scope = CandidateScope(source_ids=("codex:linux:synthetic",))
        with self.assertRaisesRegex(
            CandidatePlanningError,
            "response is invalid",
        ):
            plan_candidate_queries(
                "What changed?",
                scope=scope,
                generate=lambda _messages: json.dumps(
                    {
                        "queries": ["change", "result"],
                        "source_ids": ["gmail:synthetic"],
                    }
                ),
            )

    def test_custom_instruction_is_the_only_semantic_optimization_surface(
        self,
    ) -> None:
        seen = {}

        def generate(messages):
            seen["messages"] = messages
            return json.dumps({"queries": ["first query", "second query"]})

        plan_candidate_queries(
            "What changed?",
            scope=CandidateScope(),
            instruction="Synthetic optimized planner guidance.",
            generate=generate,
        )

        self.assertEqual(
            seen["messages"][0]["content"],
            "Synthetic optimized planner guidance.",
        )

    def test_malformed_duplicate_and_unbounded_plans_fail_closed(self) -> None:
        scope = CandidateScope()
        for response in (
            "not json",
            json.dumps({"queries": ["same", "same"]}),
            json.dumps({"queries": [str(index) for index in range(6)]}),
            json.dumps({"queries": ["x" * 513, "valid"]}),
        ):
            with self.subTest(response=response[:20]):
                with self.assertRaises(CandidatePlanningError):
                    plan_candidate_queries(
                        "Original question",
                        scope=scope,
                        generate=lambda _messages, value=response: value,
                    )

    def test_private_bundle_has_complete_bounded_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            private = root / "private"
            private.mkdir(mode=0o700)
            questions = private / "questions.jsonl"
            output = private / "bundle.json"
            questions.write_text(
                "\n".join(
                    json.dumps(
                        {
                            "id": f"case_{index:032x}",
                            "question": f"Question {index}",
                        }
                    )
                    for index in range(2)
                )
                + "\n"
            )
            os.chmod(questions, 0o600)
            scope = CandidateScope()

            report = write_query_bundle(
                questions,
                output,
                plan=lambda question, actual_scope: CandidateQueryPlan(
                    queries=(question + " decision", question + " outcome"),
                    scope=actual_scope,
                ),
                scope=scope,
                repo_root=ROOT.parent,
                expected_cases=2,
            )

            self.assertEqual(report["case_count"], 2)
            self.assertEqual(report["planner_failure_count"], 0)
            self.assertEqual(report["query_count"], 4)
            self.assertEqual(os.stat(output).st_mode & 0o777, 0o600)

    def test_private_bundle_rejects_scope_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            private = root / "private"
            private.mkdir(mode=0o700)
            questions = private / "questions.jsonl"
            questions.write_text(
                json.dumps(
                    {"id": "case_" + "1" * 32, "question": "Question"}
                )
                + "\n"
            )
            os.chmod(questions, 0o600)
            scope = CandidateScope(
                source_ids=("codex:linux:synthetic",)
            )
            with self.assertRaisesRegex(
                ValueError,
                "planning failed for 1 cases",
            ):
                write_query_bundle(
                    questions,
                    private / "bundle.json",
                    plan=lambda _question, _scope: CandidateQueryPlan(
                        queries=("query one", "query two"),
                        scope=CandidateScope(),
                    ),
                    scope=scope,
                    repo_root=ROOT.parent,
                    expected_cases=1,
                )


if __name__ == "__main__":
    unittest.main()
