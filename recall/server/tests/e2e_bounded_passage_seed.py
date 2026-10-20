#!/usr/bin/env python3
"""Finite migration admission retains examined cursors and native publication."""

import time
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from e2e_legacy_tool_migration import LegacyToolMigration
from recall_server import passage_append
from recall_server.db import SearchDeadlineExceeded


class BoundedPassageSeed(LegacyToolMigration):
    def project(self):
        self.logical.project_pending(
            tenant_id=self.tenant, batch_size=32, max_batches=1, upload_concurrency=1
        )
        return self.passages.project_pending(
            tenant_id=self.tenant, batch_size=32, max_batches=1, concurrency=1
        )

    def history(self, count=6):
        for number in range(count):
            self.ingest_blocks(
                f"event-{number}",
                [{"type": "text", "text": f"Historical finding number {number}."}],
                parent=f"parent-{number}",
            )
        with patch.object(passage_append, "CHECKPOINT_VERSION", 2):
            self.assertEqual(self.project()["documents"], count)
        with self.store.connect() as connection:
            return connection.execute(
                """SELECT logical_document_id FROM canonical_evidence_documents
                WHERE tenant_id=%s AND source_id=%s ORDER BY logical_document_id""",
                (self.tenant, self.source),
            ).fetchall()

    def page(
        self, *, upper, after=None, limit=2,
        max_source_bytes=1_000_000, deadline_at=None,
    ):
        return self.passages.seed_backfill_page(
            tenant_id=self.tenant,
            source_id=self.source,
            after=after,
            upper=upper,
            limit=limit,
            max_source_bytes=max_source_bytes,
            deadline_at=deadline_at or time.monotonic() + 3,
        )

    def test_page_bounds_actual_writes_and_native_rebuild_terminates(self):
        keys = [row["logical_document_id"] for row in self.history()]
        # The existing CLI remains intentionally unchanged: its batch_size
        # belongs to processing, and does not make this seeding call finite.
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 6)
        with self.store.connect() as connection:
            connection.execute(
                "DELETE FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                (self.tenant,),
            )
        old = self.published_rows()
        first = self.page(upper=keys[-1])
        self.assertEqual(first["queued"], 2)
        self.assertEqual(first["examined"], 2)
        self.assertEqual(first["after"], keys[1])
        self.assertEqual(self.pending(), 2)
        self.assertEqual(self.project()["documents"], 2)
        self.assertEqual(self.published_rows(), old)
        second = self.page(upper=keys[-1], after=first["after"])
        third = self.page(upper=keys[-1], after=second["after"])
        self.assertEqual(second["queued"], 2)
        self.assertEqual(third["queued"], 2)
        self.assertEqual(self.pending(), 4)
        self.assertEqual(self.project()["documents"], 4)
        self.assertEqual(self.published_rows(), old)
        done = self.page(upper=keys[-1], after=third["after"])
        self.assertEqual(done["examined"], 0)
        self.assertEqual(done["after"], keys[-1])
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)

    def test_cursor_covers_current_and_conflict_with_certification_cas(self):
        keys = [row["logical_document_id"] for row in self.history()]
        with self.store.connect() as connection:
            connection.execute(
                """UPDATE canonical_passage_documents
                SET append_checkpoint=jsonb_set(append_checkpoint,'{version}','3')
                WHERE tenant_id=%s AND logical_document_id=%s""",
                (self.tenant, keys[0]),
            )
            connection.execute(
                """INSERT INTO canonical_passage_projection_queue(
                tenant_id,source_id,logical_document_id,revision,generation,reason,changed_at)
                SELECT tenant_id,source_id,logical_document_id,revision,7,
                       CASE WHEN logical_document_id=%s THEN 'logical-update'
                            ELSE 'certification' END,clock_timestamp()
                FROM canonical_evidence_documents
                WHERE tenant_id=%s AND logical_document_id=ANY(%s)""",
                (keys[1], self.tenant, keys[1:3]),
            )
        result = self.page(upper=keys[3], limit=3)
        self.assertEqual(
            [row["disposition"] for row in result["rows"]],
            ["current", "conflict", "queued"],
        )
        self.assertEqual(result["after"], keys[2])
        self.assertEqual(result["queued"], 1)
        self.assertEqual(result["rows"][2]["generation"], 8)
        with self.store.connect() as connection:
            protected = connection.execute(
                """SELECT reason,generation FROM canonical_passage_projection_queue
                WHERE tenant_id=%s AND logical_document_id=%s""",
                (self.tenant, keys[1]),
            ).fetchone()
        self.assertEqual(protected, {"reason": "logical-update", "generation": 7})
        tail = self.page(upper=keys[3], after=result["after"], limit=3)
        self.assertEqual(tail["examined"], 1)
        self.assertEqual(tail["after"], keys[3])
        self.assertEqual(tail["queued"], 1)
        # Rows beyond the frozen upper boundary remain outside this campaign.
        with self.store.connect() as connection:
            beyond = connection.execute(
                """SELECT count(*) AS n FROM canonical_passage_projection_queue
                WHERE tenant_id=%s AND logical_document_id=ANY(%s)""",
                (self.tenant, keys[4:]),
            ).fetchone()["n"]
        self.assertEqual(beyond, 0)

    def test_giant_is_deferred_without_blocking_later_small_parents(self):
        keys = [row["logical_document_id"] for row in self.history(4)]
        with self.store.connect() as connection:
            # Only metadata changes: this control must not read a synthetic
            # giant body or imply that its separate extraction has succeeded.
            connection.execute(
                """UPDATE canonical_evidence_document_parts SET size_bytes=2070685843
                WHERE tenant_id=%s AND logical_document_id=%s""",
                (self.tenant, keys[0]),
            )
            costs = connection.execute(
                """SELECT logical_document_id,sum(size_bytes)::bigint AS size
                FROM canonical_evidence_document_parts
                WHERE tenant_id=%s GROUP BY logical_document_id
                ORDER BY logical_document_id""",
                (self.tenant,),
            ).fetchall()
        budget = costs[1]["size"] + costs[2]["size"]
        result = self.page(upper=keys[-1], limit=4, max_source_bytes=budget)
        self.assertEqual(
            [row["disposition"] for row in result["rows"]],
            ["byte_deferred", "queued", "queued", "byte_deferred"],
        )
        self.assertEqual(result["queued_source_bytes"], budget)
        self.assertEqual(result["after"], keys[-1])
        self.assertEqual(result["rows"][0]["source_bytes"], 2070685843)
        self.assertEqual(self.pending(), 2)
        self.assertEqual(self.project()["documents"], 2)
        self.assertEqual(self.pending(), 0)

    def test_actual_sql_cancellation_after_writes_rolls_back_without_cursor(self):
        keys = [row["logical_document_id"] for row in self.history(2)]
        execute = self.store._execute_bounded
        seen = []

        def cancel(connection, sql, values, deadline):
            result = execute(connection, sql, values, deadline)
            seen.append(connection.execute(
                """SELECT count(*) AS n FROM canonical_passage_projection_queue
                WHERE tenant_id=%s""", (self.tenant,),
            ).fetchone()["n"])
            execute(connection, "SELECT pg_sleep(1)", (), deadline)
            return result

        with (
            patch.object(self.store, "_execute_bounded", side_effect=cancel),
            self.assertRaises(SearchDeadlineExceeded),
        ):
            self.page(upper=keys[-1], deadline_at=time.monotonic() + 0.2)
        self.assertEqual(seen, [2])
        self.assertEqual(self.pending(), 0)
        self.assertEqual(self.page(upper=keys[-1])["queued"], 2)

    def test_lost_commit_response_has_no_ack_and_repeat_preserves_generation(self):
        keys = [row["logical_document_id"] for row in self.history(2)]
        connect = self.store.connect

        @contextmanager
        def lose_ack(*args, **kwargs):
            with connect(*args, **kwargs) as connection:
                yield connection
            raise OSError("lost response after actual native COMMIT")

        with (
            patch.object(self.store, "connect", side_effect=lose_ack),
            self.assertRaises(OSError),
        ):
            self.page(upper=keys[-1])
        self.assertEqual(self.pending(), 2)
        repeated = self.page(upper=keys[-1])
        self.assertEqual(repeated["queued"], 0)
        self.assertEqual(repeated["after"], keys[-1])
        self.assertEqual(
            [row["disposition"] for row in repeated["rows"]],
            ["conflict", "conflict"],
        )
        with self.store.connect() as connection:
            generations = connection.execute(
                """SELECT generation FROM canonical_passage_projection_queue
                WHERE tenant_id=%s""", (self.tenant,),
            ).fetchall()
        self.assertEqual(generations, [{"generation": 1}, {"generation": 1}])

    def test_source_scope_and_non_claude_compatibility_are_unchanged(self):
        claude_source = self.source
        claude_keys = [row["logical_document_id"] for row in self.history(2)]
        self.source = "codex:" + claude_source
        other_keys = [row["logical_document_id"] for row in self.history(2)]
        result = self.page(upper=other_keys[-1])
        self.assertEqual(result["queued"], 0)
        self.assertEqual(
            [row["disposition"] for row in result["rows"]], ["current", "current"]
        )
        self.source = claude_source
        result = self.page(upper=claude_keys[-1])
        self.assertEqual(result["queued"], 2)
        with self.store.connect() as connection:
            sources = connection.execute(
                """SELECT DISTINCT source_id FROM canonical_passage_projection_queue
                WHERE tenant_id=%s""", (self.tenant,),
            ).fetchall()
        self.assertEqual(sources, [{"source_id": claude_source}])


if __name__ == "__main__":
    suite = unittest.TestSuite(
        BoundedPassageSeed(name)
        for name in BoundedPassageSeed.__dict__
        if name.startswith("test_")
    )
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
