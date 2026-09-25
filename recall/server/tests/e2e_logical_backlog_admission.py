#!/usr/bin/env python3
"""Real production admission against synthetic queue skew; no runtime patch."""

from collections import Counter
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

import psycopg
from psycopg.rows import dict_row

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER), str(SERVER.parent)]
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
    MAX_LOGICAL_ATTEMPTS,
)


class BacklogAdmission(unittest.TestCase):
    def setUp(self):
        self.connection = psycopg.connect(
            os.environ["RECALL_DATABASE_URL"], row_factory=dict_row
        )
        self.addCleanup(self.connection.close)
        self.connection.execute("""CREATE TEMP TABLE canonical_evidence_document_queue (
            tenant_id text, source_id text, native_parent_id text,
            generation bigint DEFAULT 1, reason text DEFAULT 'ingest',
            changed_at timestamptz, first_queued_at timestamptz,
            attempts integer DEFAULT 0, next_attempt_at timestamptz,
            PRIMARY KEY(tenant_id,source_id,native_parent_id))""")
        self.connection.execute("""CREATE TEMP TABLE canonical_evidence_documents (
            tenant_id text, source_id text, native_parent_id text,
            logical_document_id text, revision bigint, record_count bigint)""")
        self.connection.execute("""CREATE TEMP TABLE canonical_evidence_document_parts (
            tenant_id text, source_id text, logical_document_id text,
            revision bigint, size_bytes bigint)""")
        self.projector = CanonicalLogicalEvidenceProjector(
            SimpleNamespace(connect=lambda: nullcontext(self.connection)), None
        )
        self.now = datetime.now(timezone.utc)

    def add(
        self,
        source,
        count,
        *,
        tenant="tenant:test",
        reason="ingest",
        attempts=0,
        next_attempt=None,
        prefix="parent",
        quiet=True,
    ):
        # Increasing ordinal means increasing queue age and changed timestamp.
        self.connection.execute(
            """INSERT INTO canonical_evidence_document_queue
            (tenant_id,source_id,native_parent_id,reason,changed_at,first_queued_at,attempts,next_attempt_at)
            SELECT %s,%s,%s||'-'||lpad(n::text,6,'0'),%s,
                   CASE WHEN %s THEN %s::timestamptz-interval '1 day'+n*interval '1 second'
                        ELSE %s::timestamptz END,
                   %s::timestamptz-interval '1 day'+n*interval '1 second',%s,%s
            FROM generate_series(1,%s)n""",
            (
                tenant,
                source,
                prefix,
                reason,
                quiet,
                self.now,
                self.now,
                self.now,
                attempts,
                next_attempt,
                count,
            ),
        )

    def pending(self, limit, *, recent=False, tenant="tenant:test", quiet=0, wait=0):
        return self.projector._pending(
            tenant_id=tenant,
            limit=limit,
            prefer_recent=recent,
            quiet_seconds=quiet,
            max_wait_seconds=wait,
        )

    def skew(self):
        self.add("source:dominant", 5000)
        for index in range(7):
            self.add(f"source:peer-{index}", 20)

    def test_residual_capacity_tracks_backlog_after_every_source_head(self):
        self.skew()
        for recent in (False, True):
            with self.subTest(recent=recent):
                rows = self.pending(50, recent=recent)
                counts = Counter(row.source_id for row in rows)
                self.assertEqual(len(rows), 50)
                self.assertEqual(
                    len({(r.tenant_id, r.source_id, r.native_parent_id) for r in rows}),
                    50,
                )
                self.assertEqual(len({row.source_id for row in rows[:8]}), 8)
                self.assertEqual(
                    counts["source:dominant"],
                    43,
                    "equal residual source quotas under-serve the dominant ready backlog",
                )
                self.assertEqual({counts[f"source:peer-{n}"] for n in range(7)}, {1})
                dominant = [
                    r.native_parent_id for r in rows if r.source_id == "source:dominant"
                ]
                expected = range(5000, 4957, -1) if recent else range(1, 44)
                self.assertEqual(dominant, [f"parent-{n:06d}" for n in expected])

    def test_floor_holds_when_limit_has_no_residual_capacity(self):
        self.skew()
        for recent in (False, True):
            for limit in (1, 3, 8):
                with self.subTest(recent=recent, limit=limit):
                    rows = self.pending(limit, recent=recent)
                    self.assertEqual(len(rows), limit)
                    self.assertEqual(len({r.source_id for r in rows}), limit)

    def test_forget_precedence_and_its_existing_order_are_unchanged(self):
        self.skew()
        self.add("source:urgent-a", 3, reason="forget", quiet=False)
        self.add("source:urgent-b", 1, reason="forget", quiet=False)
        for recent in (False, True):
            rows = self.pending(4, recent=recent, quiet=60)
            self.assertEqual(
                [(r.source_id, r.native_parent_id) for r in rows],
                [
                    ("source:urgent-a", "parent-000001"),
                    ("source:urgent-b", "parent-000001"),
                    ("source:urgent-a", "parent-000002"),
                    ("source:urgent-a", "parent-000003"),
                ],
            )
            self.assertTrue(all(row.admission_priority == 0 for row in rows))

    def test_ineligible_rows_do_not_inflate_source_weight(self):
        self.add("source:dominant", 500)
        self.add("source:peer", 2)
        self.add(
            "source:peer", 5000, prefix="quarantined", attempts=MAX_LOGICAL_ATTEMPTS
        )
        self.add(
            "source:peer",
            5000,
            prefix="backoff",
            next_attempt=self.now + timedelta(hours=1),
        )
        self.add("source:peer", 5000, prefix="debounced", quiet=False)
        rows = self.pending(10, quiet=60)
        self.assertEqual(
            Counter(r.source_id for r in rows), {"source:dominant": 9, "source:peer": 1}
        )
        self.assertTrue(all(r.native_parent_id.startswith("parent-") for r in rows))

    def test_tenant_scoping_precedes_counts_and_shared_source_names_stay_separate(self):
        self.add("source:dominant", 100)
        self.add("source:peer", 2)
        self.add("source:peer", 10000, tenant="tenant:other")
        rows = self.pending(10)
        self.assertEqual({r.tenant_id for r in rows}, {"tenant:test"})
        self.assertEqual(
            Counter(r.source_id for r in rows), {"source:dominant": 9, "source:peer": 1}
        )
        all_tenants = self.pending(3, tenant=None)
        self.assertEqual(
            {(r.tenant_id, r.source_id) for r in all_tenants},
            {
                ("tenant:test", "source:dominant"),
                ("tenant:test", "source:peer"),
                ("tenant:other", "source:peer"),
            },
        )

    def test_balanced_backlogs_keep_stable_interleaving(self):
        for source in ("source:a", "source:b", "source:c"):
            self.add(source, 10)
        for recent in (False, True):
            rows = self.pending(9, recent=recent)
            ordinals = (10, 9, 8) if recent else (1, 2, 3)
            self.assertEqual(
                [(r.source_id, r.native_parent_id) for r in rows],
                [
                    (source, f"parent-{number:06d}")
                    for number in ordinals
                    for source in ("source:a", "source:b", "source:c")
                ],
            )

    def test_inflight_head_exclusion_preserves_bound_and_peer_drain(self):
        self.add("source:dominant", 31)
        self.add("source:peer", 3)
        first = self.pending(2)
        excluded = {(r.tenant_id, r.source_id, r.native_parent_id) for r in first}
        # Match the coordinator's existing overscan -> exclusion -> slice seam.
        # The floor protects eligible SQL heads; an already-running peer head
        # can be excluded and does not imply a new peer in this actual batch.
        rows = [
            r
            for r in self.pending(5 + len(excluded))
            if (r.tenant_id, r.source_id, r.native_parent_id) not in excluded
        ][:5]
        self.assertEqual(len(rows), 5)
        selected = {(r.tenant_id, r.source_id, r.native_parent_id) for r in rows}
        self.assertTrue(selected.isdisjoint(excluded))
        self.assertEqual(len(selected), 5)
        seen = excluded | selected
        for identity in seen:
            self.connection.execute(
                """DELETE FROM canonical_evidence_document_queue
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                identity,
            )
        while rows := self.pending(5):
            for row in rows:
                identity = (row.tenant_id, row.source_id, row.native_parent_id)
                self.assertNotIn(identity, seen)
                seen.add(identity)
                self.connection.execute(
                    """DELETE FROM canonical_evidence_document_queue
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                    identity,
                )
        self.assertEqual(len(seen), 34)
        self.assertEqual(sum(source == "source:peer" for _, source, _ in seen), 3)

    def test_finite_work_drains_and_small_sources_receive_their_floor(self):
        self.add("source:dominant", 31)
        self.add("source:peer", 3)
        seen = set()
        rounds = 0
        while rows := self.pending(5, recent=bool(rounds % 2)):
            rounds += 1
            ready_sources = {
                r["source_id"]
                for r in self.connection.execute(
                    "SELECT DISTINCT source_id FROM canonical_evidence_document_queue"
                ).fetchall()
            }
            self.assertEqual(
                {r.source_id for r in rows[: len(ready_sources)]}, ready_sources
            )
            for row in rows:
                identity = (row.tenant_id, row.source_id, row.native_parent_id)
                self.assertNotIn(identity, seen)
                seen.add(identity)
                self.connection.execute(
                    """DELETE FROM canonical_evidence_document_queue
                    WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                    identity,
                )
            self.assertLessEqual(rounds, 7)
        self.assertEqual(len(seen), 34)


if __name__ == "__main__":
    unittest.main(verbosity=2)
