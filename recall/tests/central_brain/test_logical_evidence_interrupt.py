from __future__ import annotations

import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[2]
SERVER = ROOT / "server"
for candidate in (str(ROOT), str(SERVER)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
    LogicalGroupCandidate,
    _structural_values,
)


class FakeStore:
    pool_max_size = 2

    def prepare_pool(self, _connections: int) -> None:
        return None


class InterruptingProjector(CanonicalLogicalEvidenceProjector):
    def __init__(self) -> None:
        self.store = FakeStore()
        self.projection = None
        self.bound_tenant_id = "tenant:company:synthetic"
        self.raw_archive = None
        self.excluded_structural_types = (
            "file-history-snapshot",
            "queue-operation",
            "token_count",
            "turn_context",
        )
        self.retention_profile = "conversation-useful-v1"
        self._prefer_recent_admission = False
        self.upload = SimpleNamespace(
            all_references=(
                {
                    "tenant_id": self.bound_tenant_id,
                    "source_id": "source:synthetic",
                    "artifact_id": "art_" + "a" * 32,
                },
            )
        )
        self.cleanup_batches: list[list[object]] = []
        self.drain_calls = 0

    def drain_cleanup(self, **_values):
        self.drain_calls += 1
        return {
            "status": "complete",
            "completed": 0,
            "deleted": 0,
            "failures": 0,
            "pending": 0,
        }

    def _pending(self, **_values):
        return [
            LogicalGroupCandidate(
                tenant_id=self.bound_tenant_id,
                source_id="source:synthetic",
                native_parent_id=native_parent_id,
                source_updated_at=SimpleNamespace(),
                generation=1,
                revision=1,
            )
            for native_parent_id in ("complete", "interrupt")
        ]

    def _prepare_batch_and_upload(self, candidates, **kwargs):
        if candidates[0].native_parent_id == "interrupt":
            raise KeyboardInterrupt
        return [self.upload]

    def _commit_upload(self, candidate, upload):
        # Interrupt before the transaction commits: this owner must release
        # the completed upload even when its sibling also interrupts.
        raise KeyboardInterrupt

    def _schedule_upload_cleanup(self, uploads):
        self.cleanup_batches.append(list(uploads))
        return len(uploads)


class LogicalEvidenceInterruptTests(TestCase):
    def test_pending_reuses_document_weight_without_rescanning_events(self) -> None:
        captured: dict[str, object] = {}

        class Connection:
            def execute(self, query, values):
                captured["query"] = query
                captured["values"] = values
                return SimpleNamespace(
                    fetchall=lambda: [
                        {
                            "tenant_id": "tenant:company:synthetic",
                            "source_id": "source:synthetic",
                            "native_parent_id": "session-1",
                            "source_updated_at": "2026-08-05T00:00:00Z",
                            "generation": 2,
                            "revision": 3,
                            "estimated_records": 42,
                            "estimated_bytes": 84,
                        }
                    ]
                )

        class Store:
            def connect(self):
                return nullcontext(Connection())

        projector = CanonicalLogicalEvidenceProjector(
            Store(),
            None,
            bound_tenant_id="tenant:company:synthetic",
        )

        pending = projector._pending(
            tenant_id="tenant:company:synthetic",
            limit=10,
        )

        self.assertEqual(pending[0].estimated_records, 42)
        self.assertEqual(pending[0].estimated_bytes, 84)
        self.assertNotIn("canonical_events", captured["query"])

    def test_structural_values_reads_only_bounded_remembering_fields(self) -> None:
        parsed, types, roles = _structural_values(
            """{
                "type":"response_item",
                "payload":{
                    "type":"turn_context",
                    "message":{"type":"message","role":"assistant"},
                    "nested":{"type":"file-history-snapshot","role":"user"}
                }
            }"""
        )

        self.assertTrue(parsed)
        self.assertEqual(
            types,
            ("response_item", "turn_context", "message"),
        )
        self.assertEqual(
            roles,
            ("response_item", "turn_context", "assistant", "message"),
        )

    def test_structural_values_leaves_non_json_to_legacy_fallback(self) -> None:
        self.assertEqual(
            _structural_values("useful plain-text connector record"),
            (False, (), ()),
        )

    def test_interrupt_cleans_every_completed_uncommitted_upload(self) -> None:
        projector = InterruptingProjector()

        with self.assertRaises(KeyboardInterrupt):
            projector.project_pending(
                tenant_id="tenant:company:synthetic",
                batch_size=2,
                max_batches=1,
                upload_concurrency=2,
            )

        self.assertEqual(projector.cleanup_batches, [[projector.upload]])
        self.assertEqual(projector.drain_calls, 2)


class PoisonedGroupIsolationTests(TestCase):
    """One failing group is backed off; the rest of the batch still commits."""

    def _projector(self):
        from recall_server.logical_evidence_projection import LogicalEvidenceError

        class Connection:
            def execute(self, query, values=()):
                return SimpleNamespace(
                    fetchone=lambda: {"queued": 3, "waiting": 0, "quarantined": 0, "backoff": 1},
                    fetchall=lambda: [],
                )

        class Store:
            pool_max_size = 2

            def prepare_pool(self, _n):
                return None

            def connect(self):
                return nullcontext(Connection())

        class Projector(InterruptingProjector):
            def __init__(self):
                super().__init__()
                self.store = Store()
                self.upload = SimpleNamespace(
                    all_references=({"tenant_id": self.bound_tenant_id, "source_id": "source:synthetic", "artifact_id": "art_" + "a" * 32, "size_bytes": 1},),
                    prepared=SimpleNamespace(record_count=1, receipt_count=1),
                )
                self.marked: list[tuple[str, str]] = []
                self.committed: list[str] = []
                self.prepare_calls: list[tuple[str, ...]] = []

            def _pending(self, **_values):
                return [
                    LogicalGroupCandidate(
                        tenant_id=self.bound_tenant_id,
                        source_id="source:synthetic",
                        native_parent_id=parent,
                        source_updated_at=SimpleNamespace(),
                        generation=1,
                        revision=1,
                    )
                    for parent in ("good-a", "poison", "good-b")
                ]

            def _prepare_batch_and_upload(self, candidates, **kwargs):
                self.prepare_calls.append(tuple(c.native_parent_id for c in candidates))
                if any(c.native_parent_id == "poison" for c in candidates):
                    raise LogicalEvidenceError("logical_evidence_full_record_corrupt")
                return [self.upload for _ in candidates]

            def _mark_failed(self, candidate, error):
                self.marked.append((candidate.native_parent_id, str(error)))

            def _commit_upload(self, candidate, upload):
                self.committed.append(candidate.native_parent_id)
                return "committed"

        return Projector()

    def test_poisoned_group_is_isolated_and_others_commit(self) -> None:
        projector = self._projector()
        result = projector.project_pending(
            tenant_id=projector.bound_tenant_id,
            batch_size=3,
            max_batches=1,
            upload_concurrency=1,
        )
        # Each parent is attempted once; a poisoned parent cannot force its
        # healthy siblings to repeat archive work.
        self.assertEqual(projector.prepare_calls, [("good-a",), ("poison",), ("good-b",)])
        self.assertEqual(projector.marked, [("poison", "logical_evidence_full_record_corrupt")])
        self.assertEqual(sorted(projector.committed), ["good-a", "good-b"])
        self.assertEqual(result["documents"], 2)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["backoff"], 1)
        self.assertEqual(result["pending"], 2)
