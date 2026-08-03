from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / "server"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SERVER))

from recall_server.candidate_admission import (  # noqa: E402
    AdmissionError,
    AdmissionScope,
    admit_candidate_documents,
)


def card(
    ordinal: int,
    *,
    family: str = "coding_history",
    authorized: bool = True,
    pointer_valid: bool = True,
    occurred_at: str = "2026-07-10T00:00:00Z",
) -> dict:
    return {
        "logical_document_id": f"ldoc_{ordinal:032x}",
        "source_id": "codex:linux:synthetic",
        "source_family": family,
        "first_occurred_at": occurred_at,
        "last_occurred_at": occurred_at,
        "snippets": [f"Synthetic candidate {ordinal}"],
        "provenance": [{
            "query_ordinal": 0,
            "arm": "dense",
            "rank": ordinal % 50 + 1,
        }],
        "pointer_valid": pointer_valid,
        "authorized": authorized,
    }


def valid_response(messages, *, empty: bool = False) -> str:
    payload = json.loads(messages[1]["content"])
    limit = payload["selection_limit"]
    selected = [] if empty else payload["candidates"][:limit]
    return json.dumps({
        "selected": [
            {
                "id": item["id"],
                "reason": "Synthetic evidence match.",
                "needs": ["Synthetic evidence need."],
            }
            for item in selected
        ]
    })


class CandidateAdmissionTest(unittest.TestCase):
    def test_agentic_map_reduce_is_bounded_and_attributable(self) -> None:
        result = admit_candidate_documents(
            "What happened to the synthetic project?",
            scope=AdmissionScope(),
            cards=[card(index) for index in range(70)],
            generate=valid_response,
            map_workers=2,
        )

        self.assertLessEqual(len(result["selected"]), 8)
        self.assertEqual(
            [stage["stage"] for stage in result["stages"]],
            ["map", "map", "map", "final"],
        )
        self.assertTrue(all(
            set(item) == {"source_id", "logical_document_id"}
            for item in result["selected"]
        ))

    def test_unauthorized_or_invalid_card_never_reaches_model(self) -> None:
        for invalid in (
            card(1, authorized=False),
            card(1, pointer_valid=False),
        ):
            calls = []
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(
                    AdmissionError,
                    "cannot reach admission",
                ):
                    admit_candidate_documents(
                        "Question",
                        scope=AdmissionScope(),
                        cards=[invalid],
                        generate=lambda messages: calls.append(messages) or "",
                    )
                self.assertEqual(calls, [])

    def test_explicit_source_and_time_scope_filter_before_model(self) -> None:
        seen = []

        def generate(messages):
            payload = json.loads(messages[1]["content"])
            seen.append(payload["candidates"])
            return valid_response(messages)

        result = admit_candidate_documents(
            "Question",
            scope=AdmissionScope(
                source_families=("coding_history",),
                since="2026-07-05T00:00:00Z",
                until="2026-07-15T00:00:00Z",
            ),
            cards=[
                card(1),
                card(2, family="email"),
                card(3, occurred_at="2026-06-01T00:00:00Z"),
            ],
            generate=generate,
        )

        self.assertTrue(seen)
        self.assertTrue(all(len(values) == 1 for values in seen))
        self.assertTrue(all(
            values[0]["source_family"] == "coding_history"
            for values in seen
        ))
        self.assertEqual(len(result["selected"]), 1)
        self.assertEqual(
            result["selected"][0]["logical_document_id"],
            "ldoc_" + f"{1:032x}",
        )

    def test_map_cannot_select_an_id_outside_its_shard(self) -> None:
        invented = "ldoc_" + "f" * 32
        with self.assertRaisesRegex(AdmissionError, "unknown document"):
            admit_candidate_documents(
                "Question",
                scope=AdmissionScope(),
                cards=[card(1)],
                generate=lambda _messages: json.dumps({
                    "selected": [{
                        "id": invented,
                        "reason": "Invented",
                        "needs": [],
                    }]
                }),
            )

    def test_reducer_cannot_reintroduce_a_rejected_card(self) -> None:
        rejected = card(2)["logical_document_id"]

        def generate(messages):
            payload = json.loads(messages[1]["content"])
            if payload["stage"] == "map":
                first = payload["candidates"][0]
                return json.dumps({
                    "selected": [{
                        "id": first["id"],
                        "reason": "Map selection",
                        "needs": [],
                    }]
                })
            return json.dumps({
                "selected": [{
                    "id": rejected,
                    "reason": "Invalid reducer selection",
                    "needs": [],
                }]
            })

        with self.assertRaisesRegex(AdmissionError, "unknown document"):
            admit_candidate_documents(
                "Question",
                scope=AdmissionScope(),
                cards=[card(1), card(2)],
                generate=generate,
            )

    def test_malformed_duplicate_and_over_limit_outputs_fail_closed(self) -> None:
        candidates = [card(index) for index in range(10)]
        first = candidates[0]["logical_document_id"]
        invalid = (
            "not-json",
            json.dumps({
                "selected": [
                    {"id": first, "reason": "x", "needs": []},
                    {"id": first, "reason": "x", "needs": []},
                ]
            }),
            json.dumps({
                "selected": [
                    {
                        "id": item["logical_document_id"],
                        "reason": "x",
                        "needs": [],
                    }
                    for item in candidates[:7]
                ]
            }),
        )
        for response in invalid:
            with self.subTest(response=response[:20]):
                with self.assertRaises(AdmissionError):
                    admit_candidate_documents(
                        "Question",
                        scope=AdmissionScope(),
                        cards=candidates,
                        generate=lambda _messages, value=response: value,
                    )

    def test_empty_valid_selection_is_not_a_model_failure(self) -> None:
        result = admit_candidate_documents(
            "Question",
            scope=AdmissionScope(),
            cards=[card(1)],
            generate=lambda messages: valid_response(messages, empty=True),
        )
        self.assertEqual(result["selected"], [])
        self.assertEqual(result["stages"][-1]["selected_count"], 0)

    def test_truth_or_full_document_fields_fail_closed(self) -> None:
        for field in ("gold", "truth", "full_document"):
            invalid = {**card(1), field: "forbidden"}
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    AdmissionError,
                    "schema is invalid",
                ):
                    admit_candidate_documents(
                        "Question",
                        scope=AdmissionScope(),
                        cards=[invalid],
                        generate=valid_response,
                    )


if __name__ == "__main__":
    unittest.main()
