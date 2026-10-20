#!/usr/bin/env python3
"""Native receipt deadlines reduce only their unfinished month's page width."""
import unittest
from unittest.mock import patch

from e2e_turbopuffer_quantum import CooperativePublication
from recall_server.db import SearchDeadlineExceeded


class AdaptivePublicationPage(CooperativePublication):
    def configure(self):
        self.assertEqual(self.store.search_deadline_ms, 300)
        self.writer.page_rows = 3
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        with self.store.connect() as connection:
            claim = next(row for row in self.writer.claim_months(
                connection, tenant_id=self.tenant, limit=10)
                if row["source_id"] == self.sources["large"])
            expected = {row["passage_id"] for row in connection.execute(
                "SELECT passage_id FROM canonical_passages WHERE tenant_id=%s",
                (self.tenant,))}
        self.assertEqual(len(expected), 4)
        self.addCleanup(self.close_steps)
        return namespace, claim, expected

    def close_steps(self):
        while self.writer._quantum_months:
            _, steps, _ = self.writer._quantum_months.popleft()
            steps.close()

    def state(self, claim):
        with self.store.connect() as connection:
            return dict(connection.execute(
                """SELECT generation,page_checkpoint FROM search_projection_outbox
                   WHERE tenant_id=%s AND source_id=%s AND month=%s""",
                (self.tenant, claim["source_id"], claim["month"])).fetchone())

    def test_authority_timeout_recovers_without_replaying_deletes_or_skipping_rows(self):
        namespace, claim, expected = self.configure()
        deleted_id = "psg_" + "d" * 32
        namespace.write(upsert_rows=[{"id": deleted_id}])
        with self.store.connect() as connection:
            connection.execute(
                """INSERT INTO search_projection_tombstones(
                   tenant_id,source_id,month,passage_id,deleted_at)
                   VALUES (%s,%s,%s,%s,clock_timestamp())""",
                (self.tenant, claim["source_id"], claim["month"], deleted_id))
        before = self.state(claim)
        phases = []
        original_page = self.writer.passage_page
        original_authorize = self.writer._authorized_publication_rows

        def page(*args, **kwargs):
            rows = original_page(*args, **kwargs)
            phases.append(dict(phase="selection", rows=len(rows),
                width=kwargs.get("page_rows", self.writer.page_rows)))
            return rows

        def authorize(connection, tenant, rows, *, deadline_at):
            receipts = tuple(dict.fromkeys(
                receipt for row in rows for receipt in row["receipts"]))
            timed_out = False
            try:
                if rows and rows[0]["source_id"] == self.sources["large"]:
                    # The existing real cancellation seam, with cost proportional
                    # to the exact selected receipt set. Never fabricate authority.
                    self.store._execute_bounded(connection,
                        "SELECT pg_sleep(0.12) FROM unnest(%s::text[]) AS requested(receipt)",
                        (list(receipts),), deadline_at).fetchall()
                return original_authorize(connection, tenant, rows, deadline_at=deadline_at)
            except SearchDeadlineExceeded:
                timed_out = True
                raise
            finally:
                phases.append(dict(phase="authority", rows=len(rows),
                    receipt_count=len(receipts), timed_out=timed_out,
                    passage_ids=[row["passage_id"] for row in rows]))

        with patch.object(self.store, "verified_singleton_receipts", True), \
                patch.object(self.writer, "passage_page", side_effect=page), \
                patch.object(self.writer, "_authorized_publication_rows", side_effect=authorize), \
                patch.object(namespace, "write", wraps=namespace.write) as writes:
            deletion = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertEqual(deletion["deleted"], 1)
            self.assertNotIn(deleted_id, namespace.rows)
            with self.assertLogs("recall_server.turbopuffer_projection", level="WARNING") as logs:
                result = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
            self.assertIn("phase=authority page_rows=3 next_page_rows=1", logs.output[-1])
            self.assertEqual(result["failed"], 1)
            self.assertEqual(result["rows"], 0)
            self.assertEqual(self.state(claim), before)
            self.assertEqual(namespace.rows, {})
            failed = [row for row in phases if row["phase"] == "authority"]
            self.assertEqual([row["receipt_count"] for row in failed], [3])
            self.assertTrue(all(row["timed_out"] for row in failed))

            # The owner must recover without a caller/global configuration change.
            for _ in range(10):
                result = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                self.assertEqual(result["failed"], 0)
                if not result["pending"]:
                    break
            self.assertEqual(result["pending"], 0)
            self.assertEqual(set(namespace.rows), expected)
            self.assertEqual(sum(len(call.kwargs.get("deletes", ()))
                for call in writes.call_args_list), 1)
            sent = [row["id"] for call in writes.call_args_list
                    for row in call.kwargs.get("upsert_rows", ())]
            self.assertEqual(len(sent), len(set(sent)))
            self.assertEqual(set(sent), expected)
            self.assertEqual(self.writer.page_rows, 3)
            selections = [row for row in phases if row["phase"] == "selection"]
            self.assertEqual([row["width"] for row in selections[:4]], [3, 1, 1, 1])
            self.assertEqual(selections[-1]["width"], 3,
                "the next month inherited another month's reduced width")
            with self.store.connect() as connection:
                count = connection.execute(
                    "SELECT sum(row_count) AS n FROM search_projection_shards WHERE tenant_id=%s",
                    (self.tenant,)).fetchone()["n"]
            self.assertEqual(count, len(expected))

    def test_key_selection_timeout_does_not_reduce_page_width(self):
        namespace, claim, _ = self.configure()
        before = self.state(claim)
        original_page = self.writer.passage_page
        phases = []

        def blocked_page(connection, scope, *, deadline_at, **kwargs):
            try:
                self.store._execute_bounded(
                    connection, "SELECT pg_sleep(0.4)", (), deadline_at).fetchall()
                return original_page(connection, scope, deadline_at=deadline_at, **kwargs)
            finally:
                phases.append(dict(width=kwargs.get("page_rows", self.writer.page_rows)))

        with patch.object(self.store, "verified_singleton_receipts", True), \
                patch.object(self.writer, "passage_page", side_effect=blocked_page), \
                patch.object(self.writer, "_authorized_publication_rows",
                             wraps=self.writer._authorized_publication_rows) as authority:
            for _ in range(2):
                with self.assertLogs("recall_server.turbopuffer_projection", level="WARNING") as logs:
                    result = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                self.assertIn("phase=selection page_rows=3 next_page_rows=3", logs.output[-1])
                self.assertEqual(result["failed"], 1)
                self.assertEqual(result["rows"], 0)
                self.assertEqual(self.state(claim), before)
                self.assertEqual(namespace.rows, {})
            self.assertEqual(authority.call_count, 0)
            self.assertEqual([row["width"] for row in phases], [3, 3])
            self.assertEqual(self.writer.page_rows, 3)
    def test_one_row_authority_timeout_preserves_queue_and_width_floor(self):
        namespace, claim, _ = self.configure()
        before = self.state(claim)
        widths = []
        original_page = self.writer.passage_page

        def page(*args, **kwargs):
            widths.append(kwargs.get("page_rows", self.writer.page_rows))
            return original_page(*args, **kwargs)

        def timeout(connection, tenant, rows, *, deadline_at):
            self.assertTrue(rows)
            self.store._execute_bounded(
                connection, "SELECT pg_sleep(0.4)", (), deadline_at).fetchall()
            self.fail("native cancellation did not occur")

        with patch.object(self.store, "verified_singleton_receipts", True), \
                patch.object(self.writer, "passage_page", side_effect=page), \
                patch.object(self.writer, "_authorized_publication_rows", side_effect=timeout):
            for _ in range(3):
                result = self.writer.drain_quantum(tenant_id=self.tenant, max_months=1)
                self.assertEqual(result["failed"], 1)
                self.assertEqual(result["rows"], 0)
                self.assertEqual(self.state(claim), before)
                self.assertEqual(namespace.rows, {})
            self.assertEqual(widths, [3, 1, 1])
            self.assertEqual(self.writer.page_rows, 3)


if __name__ == "__main__":
    names = [name for name in AdaptivePublicationPage.__dict__ if name.startswith("test_")]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(
        AdaptivePublicationPage(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
