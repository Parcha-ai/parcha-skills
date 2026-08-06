from __future__ import annotations

import sys
import threading
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.db import SearchDeadlineExceeded  # noqa: E402
from recall_server.mcp import CANONICAL_SHOW_TOOL  # noqa: E402


class DeadlineStore:
    def __init__(self) -> None:
        self.deadlines: list[float] = []

    @contextmanager
    def connect(self):
        yield object()

    def _execute_bounded(self, _connection, _sql, _values, deadline_at):
        self.deadlines.append(deadline_at)
        raise SearchDeadlineExceeded("synthetic investigation deadline")


class DeadlineRetrieval(BoundCanonicalRetrieval):
    def passage_hints(self, _query, _filters=None, _limit=10):
        return {"results": [], "diagnostics": {"engine": "synthetic"}}


class ParallelMapRetrieval(BoundCanonicalRetrieval):
    def __init__(self) -> None:
        super().__init__(
            object(),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )
        self.barrier = threading.Barrier(2, timeout=1)
        self.calls: list[tuple[str, dict]] = []

    def deep_search(self, question, *, filters=None, depth="normal", **_kwargs):
        self.calls.append((question, dict(filters or {})))
        self.barrier.wait()
        receipt = (
            "recall://codex.jsonl:test/"
            f"{question.replace(' ', '-')}?rev=1#item=0"
        )
        return {
            "status": "complete",
            "findings": [{"receipt": receipt, "text": question}],
            "coverage": {"complete": True},
            "uncertainty": [],
        }


class CanonicalInvestigatorContractTest(unittest.TestCase):
    def test_investigate_seeds_from_lossless_passages(self) -> None:
        receipts = tuple(
            f"recall://codex.jsonl:test/event-{index}?rev=1#item=0"
            for index in range(3)
        )

        class Store:
            @contextmanager
            def connect(self):
                yield object()

            @staticmethod
            def _execute_bounded(_connection, sql, _values, _deadline_at):
                if "FROM canonical_sources" in sql:
                    return type("Rows", (), {"fetchall": lambda self: [
                        {"source_id": "codex.jsonl:test"}
                    ]})()
                return type(
                    "Rows",
                    (),
                    {"fetchall": lambda self: []},
                )()

        class Retrieval(BoundCanonicalRetrieval):
            def search(self, *_args, **_kwargs):
                raise AssertionError("investigate used the legacy chunk index")

            def passage_hints(self, query, filters=None, limit=10):
                self.passage_call = (query, filters, limit)
                return {
                    "results": [{
                        "source_id": "codex.jsonl:test",
                        "logical_document_id": "ldoc_" + "1" * 32,
                        "revision": 1,
                        "native_parent_id": "session-1",
                        "first_occurred_at": "2026-08-05T10:00:00+00:00",
                        "last_occurred_at": "2026-08-05T12:00:00+00:00",
                        "rank": 0.02,
                        "reasons": ["dense"],
                        "matching_ranges": [{
                            "kind": "dense",
                            "score": 0.8,
                            "text": "implemented the snapshot rollout",
                            "text_clipped": False,
                            "receipts": list(receipts),
                        }],
                    }],
                    "diagnostics": {"engine": "lossless-passages-v1"},
                }

            def session_context(self, target, **_kwargs):
                self.context_target = target
                return {
                    "session": {
                        "source_id": "codex.jsonl:test",
                        "native_parent_id": "session-1",
                        "time_basis": "occurred_at",
                    },
                    "events": [{
                        "source_id": "codex.jsonl:test",
                        "native_id": "event-1",
                        "native_parent_id": "session-1",
                        "revision": 1,
                        "kind": "message",
                        "occurred_at": "2026-08-05T11:00:00+00:00",
                        "observed_at": "2026-08-05T11:00:01+00:00",
                        "ingested_at": "2026-08-05T11:00:02+00:00",
                        "time_basis": "occurred_at",
                        "chunks": [],
                    }],
                    "anchor_receipt": target,
                    "bounds": {"before": 2, "after": 2},
                }

        retrieval = Retrieval(
            Store(),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )
        filters = {
            "source_id": "codex.jsonl:test",
            "since": "2026-08-05T00:00:00Z",
            "until": "2026-08-06T00:00:00Z",
        }

        result = retrieval.investigate(
            "What did Chris implement?",
            filters=filters,
            depth="quick",
        )

        self.assertEqual(
            retrieval.passage_call,
            ("What did Chris implement?", filters, 20),
        )
        self.assertEqual(retrieval.context_target, receipts[1])
        self.assertEqual(result["coverage"]["sessions"], 1)
        self.assertEqual(result["diagnostics"]["unique_candidates"], 1)
        self.assertEqual(
            result["diagnostics"]["candidate_engines"],
            ["lossless-passages-v1"],
        )
        self.assertEqual(
            result["investigations"][0]["match"]["receipt"],
            receipts[1],
        )

    def test_map_seed_expands_to_ranked_session_evidence(self) -> None:
        seed = "recall://codex.jsonl:test/seed?rev=1#item=0"
        expanded = (
            "recall://codex.jsonl:test/decision?rev=1#item=0"
        )

        class Store:
            @contextmanager
            def connect(self):
                yield object()

        class Projector:
            bound_tenant_id = "tenant:test"

            def targets_for_receipts(self, *, receipts, **_kwargs):
                self.receipts = receipts
                return []

        class Inspector:
            @staticmethod
            def inspect(*, targets, **_kwargs):
                return {
                    "findings": [],
                    "complete": True,
                    "files_scanned": len(targets),
                    "stopped_reason": "completed",
                    "provider": "synthetic",
                    "timing": None,
                }

        class Retrieval(BoundCanonicalRetrieval):
            def _receipt_event(self, *_args, **_kwargs):
                return {
                    "resolved_receipt": seed,
                    "source_id": "codex.jsonl:test",
                    "native_id": "event-1",
                    "native_parent_id": "session-1",
                    "occurred_at": datetime(
                        2026,
                        7,
                        23,
                        tzinfo=timezone.utc,
                    ),
                }

            def _parent_scoped_receipts(self, **kwargs):
                self.parent_call = kwargs
                return (expanded,)

        projector = Projector()
        retrieval = Retrieval(
            Store(),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
            evidence_projector=projector,
            deep_inspector=Inspector(),
        )

        result = retrieval.deep_search(
            "ATI harness decision",
            filters={
                "since": "2026-07-22T00:00:00Z",
                "until": "2026-07-24T23:59:59Z",
            },
            depth="quick",
            _seed_receipts=(seed,),
        )

        self.assertEqual(projector.receipts, (expanded, seed))
        self.assertEqual(
            retrieval.parent_call["parent_id"],
            "session-1",
        )
        self.assertEqual(
            result["coverage"]["recall"]["seed_sessions"],
            1,
        )

    def test_deep_candidate_file_bound_is_reported_as_partial(self) -> None:
        class Store:
            @contextmanager
            def connect(self):
                yield object()

        class Projector:
            bound_tenant_id = "tenant:test"

            def targets_for_receipts(
                self,
                *,
                tenant_id,
                source_ids,
                receipts,
                limit,
            ):
                self.limit = limit
                return [
                    {
                        "reference": {
                            "tenant_id": tenant_id,
                            "source_id": source_ids[0],
                            "object_key": (
                                f"objects/{index:02x}/"
                                + f"{index:064x}"
                            ),
                            "content_sha256": f"{index:064x}",
                        },
                        "receipts": receipts,
                    }
                    for index in range(7)
                ][:limit]

        class Inspector:
            def inspect(self, *, targets, **_kwargs):
                self.targets = targets
                return {
                    "findings": [{
                        "receipt": targets[0].receipts[0],
                        "text": "bounded proof",
                        "line": 1,
                        "object_key": targets[0].object_key,
                    }],
                    "complete": True,
                    "files_scanned": len(targets),
                    "stopped_reason": "completed",
                    "provider": "synthetic",
                    "timing": None,
                }

        class Retrieval(BoundCanonicalRetrieval):
            def _receipt_event(self, *_args, **_kwargs):
                return {
                    "source_id": "codex.jsonl:test",
                    "native_id": "event-1",
                    "native_parent_id": "session-1",
                    "occurred_at": datetime(
                        2026,
                        7,
                        23,
                        tzinfo=timezone.utc,
                    ),
                }

            def investigate(self, *_args, **_kwargs):
                return {
                    "investigations": [{
                        "context": {
                            "events": [{
                                "chunks": [{
                                    "receipt": (
                                        "recall://codex.jsonl:test/"
                                        "seed?rev=1#item=0"
                                    )
                                }]
                            }]
                        }
                    }],
                    "coverage": {},
                    "uncertainty": [],
                }

        projector = Projector()
        inspector = Inspector()
        retrieval = Retrieval(
            Store(),
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
            evidence_projector=projector,
            deep_inspector=inspector,
        )
        result = retrieval.deep_search("bounded corpus", depth="quick")
        self.assertEqual(projector.limit, 7)
        self.assertEqual(len(inspector.targets), 6)
        self.assertEqual(result["coverage"]["candidate_files"], 7)
        self.assertIs(result["coverage"]["candidate_files_truncated"], True)
        self.assertIs(result["coverage"]["complete"], False)
        self.assertEqual(result["coverage"]["stopped_reason"], "max_files")
        self.assertEqual(
            result["findings"][0]["occurred_at"],
            "2026-07-23T00:00:00+00:00",
        )
        self.assertEqual(result["findings"][0]["time_basis"], "occurred_at")

    def test_agentic_maps_run_concurrently_and_preserve_hard_filters(self) -> None:
        retrieval = ParallelMapRetrieval()
        time_filter = {"since": "2026-07-20T00:00:00Z"}
        result = retrieval.map_reduce_search(
            "What did we decide and implement?",
            maps=[
                {
                    "map_id": "decision",
                    "objective": "Find the decision.",
                    "query": "project decision",
                    "filters": time_filter,
                    "seed_receipts": [
                        "recall://codex.jsonl:test/decision?rev=1#item=0"
                    ],
                },
                {
                    "map_id": "implementation",
                    "objective": "Find implementation proof.",
                    "query": "project implementation",
                    "filters": time_filter,
                    "seed_receipts": [
                        "recall://codex.jsonl:test/implementation?rev=1#item=0"
                    ],
                },
            ],
            depth="deep",
        )
        self.assertEqual(result["contract"], "recall.agentic-map-reduce.v1")
        self.assertEqual(result["coverage"]["complete_maps"], 2)
        self.assertEqual(result["coverage"]["maps_with_evidence"], 2)
        self.assertIs(
            result["coverage"]["evidence_found_for_every_map"],
            True,
        )
        self.assertEqual(result["coverage"]["unique_receipts"], 2)
        self.assertEqual(result["diagnostics"]["parallelism"], 2)
        self.assertEqual(
            [item["map_id"] for item in result["maps"]],
            ["decision", "implementation"],
        )
        self.assertEqual(
            [filters for _question, filters in retrieval.calls],
            [time_filter, time_filter],
        )

    def test_agentic_map_output_is_bounded_and_coverage_stays_truthful(
        self,
    ) -> None:
        class OversizedMapRetrieval(ParallelMapRetrieval):
            def __init__(self) -> None:
                BoundCanonicalRetrieval.__init__(
                    self,
                    object(),
                    tenant_id="tenant:test",
                    principal_id="principal:test",
                    authorized_sources=("codex.jsonl:test",),
                )

            def deep_search(
                self,
                question,
                *,
                filters=None,
                depth="normal",
                **_kwargs,
            ):
                return {
                    "status": "complete",
                    "findings": [
                        {
                            "receipt": (
                                "recall://codex.jsonl:test/"
                                f"item-{index}?rev=1#item=0"
                            ),
                            "text": "x" * 8_000,
                        }
                        for index in range(50)
                    ],
                    "coverage": {"complete": True},
                    "uncertainty": [],
                }

        retrieval = OversizedMapRetrieval()
        result = retrieval.map_reduce_search(
            "Find everything.",
            maps=[{
                "map_id": "bounded",
                "objective": "Prove the bound.",
                "query": "large map",
                "filters": {},
                "seed_receipts": [
                    "recall://codex.jsonl:test/bounded?rev=1#item=0"
                ],
            }],
            depth="deep",
        )
        mapped = result["maps"][0]
        self.assertLess(len(mapped["findings"]), 50)
        self.assertIs(mapped["coverage"]["complete"], False)
        self.assertEqual(
            mapped["coverage"]["stopped_reason"],
            "map_output_bound",
        )
        self.assertIs(result["coverage"]["complete"], False)
        self.assertIs(
            result["coverage"]["evidence_found_for_every_map"],
            True,
        )

    def test_empty_complete_map_is_not_misreported_as_sufficient(self) -> None:
        class EmptyMapRetrieval(ParallelMapRetrieval):
            def __init__(self) -> None:
                BoundCanonicalRetrieval.__init__(
                    self,
                    object(),
                    tenant_id="tenant:test",
                    principal_id="principal:test",
                    authorized_sources=("codex.jsonl:test",),
                )

            def deep_search(self, *_args, **_kwargs):
                return {
                    "status": "complete",
                    "findings": [],
                    "coverage": {"complete": True},
                    "uncertainty": [],
                }

        result = EmptyMapRetrieval().map_reduce_search(
            "Find a missing decision.",
            maps=[{
                "map_id": "missing",
                "objective": "Find the decision.",
                "query": "missing decision",
                "filters": {},
                "seed_receipts": [
                    "recall://codex.jsonl:test/missing?rev=1#item=0"
                ],
            }],
        )
        self.assertIs(result["coverage"]["complete"], True)
        self.assertIs(
            result["coverage"]["evidence_found_for_every_map"],
            False,
        )
        self.assertIs(
            result["maps"][0]["coverage"]["evidence_found"],
            False,
        )
        self.assertEqual(len(result["maps"][0]["uncertainty"]), 1)

    def test_relative_windows_are_utc_and_source_time_based(self) -> None:
        now = datetime(2026, 7, 24, 12, 30, tzinfo=timezone.utc)
        cases = {
            "what happened today?": (
                "2026-07-24T00:00:00+00:00",
                "question:today",
            ),
            "what happened yesterday?": (
                "2026-07-23T00:00:00+00:00",
                "question:yesterday",
            ),
            "what changed in the past 2 days?": (
                "2026-07-22T12:30:00+00:00",
                "question:last-2-days",
            ),
        }
        for question, (expected_since, expected_reason) in cases.items():
            with self.subTest(question=question):
                since, until, reason = (
                    BoundCanonicalRetrieval._question_time_window(
                        question,
                        now=now,
                    )
                )
                self.assertEqual(since, expected_since)
                self.assertEqual(reason, expected_reason)
                self.assertIsNotNone(until)

    def test_canonical_show_does_not_advertise_rejected_arguments(self) -> None:
        self.assertEqual(
            set(CANONICAL_SHOW_TOOL["inputSchema"]["properties"]),
            {"target"},
        )

    def test_investigation_metadata_queries_degrade_at_one_shared_deadline(
        self,
    ) -> None:
        store = DeadlineStore()
        retrieval = DeadlineRetrieval(
            store,
            tenant_id="tenant:test",
            principal_id="principal:test",
            authorized_sources=("codex.jsonl:test",),
        )

        result = retrieval.investigate("What changed?", depth="quick")

        self.assertEqual(result["investigations"], [])
        self.assertEqual(len(store.deadlines), 2)
        self.assertEqual(len(set(store.deadlines)), 1)


if __name__ == "__main__":
    unittest.main()
