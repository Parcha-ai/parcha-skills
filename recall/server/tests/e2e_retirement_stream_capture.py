"""Native EOF capture keeps full proof while avoiding per-page network fetches."""

from dataclasses import replace
import os
from pathlib import Path
import time
import unittest
from unittest.mock import patch

import psycopg

import e2e_parent_retirement_window as fixture
from recall_server import parent_chunk_proof, receipt_membership
from recall_server.db import SearchDeadlineExceeded
from recall_server.retirement_process import RetirementStore


class StreamCapture(unittest.TestCase):
    def setUp(self):
        fixture.WindowTests.setUp(self)
        flag = patch.object(self.store, "verified_singleton_receipts", True)
        flag.start()
        self.addCleanup(flag.stop)

    def capture(self, limits=None):
        limits = limits or self.limits
        with parent_chunk_proof.ParentMetadataSpool(
            max_bytes=limits.max_spool_bytes
        ) as spool:
            result = parent_chunk_proof._capture(
                self.store, spool, self.scope, limits, time.monotonic() + 10
            )
            rows = list(
                spool.index.execute("SELECT payload FROM documents ORDER BY document")
            )
            return result, rows

    def test_full_proof_uses_one_stream_with_native_retirement_pipeline(self):
        with patch.dict(
            os.environ,
            RECALL_VERIFIED_SINGLETON_RECEIPTS="1",
            RECALL_SEARCH_PLANE="turbopuffer",
            RECALL_CHUNK_BODY_READS="archive",
        ):
            worker = RetirementStore(self.store.dsn)
        self.addCleanup(worker.close)
        stream, execute = psycopg.Cursor.stream, psycopg.Cursor.execute
        calls = {"stream": 0, "fetch": 0}

        def observed_stream(cursor, query, *args, **kwargs):
            if "SELECT selected.*,items.chunks FROM (" in query:
                calls["stream"] += 1
            yield from stream(cursor, query, *args, **kwargs)

        def observed_execute(cursor, query, *args, **kwargs):
            rendered = (
                query.as_string(cursor.connection)
                if hasattr(query, "as_string")
                else str(query)
            )
            if "FETCH FORWARD" in rendered and "retirement_metadata" in rendered:
                calls["fetch"] += 1
            return execute(cursor, query, *args, **kwargs)

        metrics = {}
        with (
            patch.object(psycopg.Cursor, "stream", observed_stream),
            patch.object(psycopg.Cursor, "execute", observed_execute),
        ):
            with parent_chunk_proof.prove_parent_chunks(
                worker,
                self.archive,
                scope=self.scope,
                limits=self.limits,
                deadline_at=time.monotonic() + 10,
                capture_metrics=metrics,
            ) as proof:
                self.assertEqual(proof["current_documents"], 12)
                self.assertEqual(proof["current_chunks"], 12)
                self.assertEqual(proof["eligible_documents"], 12)
                self.assertEqual(proof["excluded"], {})
                self.assertEqual(
                    proof["eligible_utf8_bytes"],
                    sum(len(t.encode()) for t in self.texts.values()),
                )
                self.assertEqual(len(list(proof["spool"].verified())), 12)
        self.assertEqual(calls, {"stream": 1, "fetch": 0})
        self.assertEqual(metrics["metadata_fetches"], 1)
        self.assertEqual(metrics["metadata_membership_calls"], 0)

    def test_same_snapshot_survives_physical_update_before_native_membership(self):
        before = self.capture()
        original_stream = psycopg.Cursor.stream
        changed = False

        def change_after_snapshot(cursor, query, *args, **kwargs):
            nonlocal changed
            stream = original_stream(cursor, query, *args, **kwargs)
            try:
                for row in stream:
                    if not changed and "SELECT selected.*,items.chunks FROM (" in query:
                        with self.store.connect() as connection:
                            connection.execute(
                                "UPDATE canonical_chunks SET text_sha256=repeat('0',64) WHERE tenant_id=%s",
                                (self.tenant,),
                            )
                        changed = True
                    yield row
            finally:
                stream.close()

        with patch.object(psycopg.Cursor, "stream", change_after_snapshot):
            self.assertEqual(self.capture(), before)
        self.assertTrue(changed)

    def test_native_membership_refusal_discards_all_staged_metadata(self):
        from e2e_logical_evidence_projection import legacy_canonical_state
        with self.store.connect() as connection, legacy_canonical_state(connection):
            connection.execute(
                "UPDATE canonical_chunks SET receipt=receipt||'invalid' WHERE tenant_id=%s",
                (self.tenant,),
            )
        directory = None
        with self.assertRaises(receipt_membership.ReceiptMembershipError):
            with parent_chunk_proof.ParentMetadataSpool(
                max_bytes=self.limits.max_spool_bytes
            ) as spool:
                directory = spool.directory.name
                parent_chunk_proof._capture(
                    self.store, spool, self.scope, self.limits, time.monotonic() + 10
                )
        self.assertFalse(Path(directory).exists())
        self.assertEqual(self.store.active_connections, 0)

    def test_physical_chunk_cap_refuses_over_budget_row_before_validation(self):
        with patch.object(
            receipt_membership,
            "receipt_metadata_from_rows",
            wraps=receipt_membership.receipt_metadata_from_rows,
        ) as verify:
            with self.assertRaisesRegex(Exception, "metadata_budget"):
                self.capture(replace(self.limits, max_chunks=2))
        self.assertEqual(verify.call_count, 2)
        self.assertEqual(self.store.active_connections, 0)

    def test_document_row_and_total_spool_caps_remain_fail_closed(self):
        with self.assertRaisesRegex(Exception, "metadata_budget"):
            self.capture(replace(self.limits, max_documents=2))
        for size, cap in (
            (200 * 1024, 128 * 1024),
            (1024 * 1024, self.limits.max_spool_bytes),
        ):
            with self.store.connect() as connection:
                connection.execute(
                    "UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{type}',to_jsonb(repeat('w',%s))) WHERE tenant_id=%s",
                    (size, self.tenant),
                )
            with self.subTest(size=size):
                with self.assertRaisesRegex(Exception, "metadata_(invalid|budget)"):
                    self.capture(replace(self.limits, max_spool_bytes=cap))
        self.assertEqual(self.store.active_connections, 0)

    def test_midstream_deadline_closes_stream_and_connection(self):
        check = parent_chunk_proof._check_deadline
        calls = 0

        def expire(deadline):
            nonlocal calls
            calls += 1
            if calls == 5:
                raise SearchDeadlineExceeded("search deadline exceeded")
            return check(deadline)

        directory = None
        with self.assertRaises(SearchDeadlineExceeded):
            with parent_chunk_proof.ParentMetadataSpool(
                max_bytes=self.limits.max_spool_bytes
            ) as spool:
                directory = spool.directory.name
                with patch.object(parent_chunk_proof, "_check_deadline", expire):
                    parent_chunk_proof._capture(
                        self.store,
                        spool,
                        self.scope,
                        self.limits,
                        time.monotonic() + 10,
                    )
        self.assertFalse(Path(directory).exists())
        self.assertEqual(self.store.active_connections, 0)
        self.assertEqual(self.capture()[0][2], 12)

    def test_real_server_cancel_returns_connection_and_cleans_spool(self):
        directory = None
        with self.store.connect() as blocker, blocker.transaction():
            blocker.execute("LOCK TABLE canonical_documents IN ACCESS EXCLUSIVE MODE")
            with self.assertRaises(
                (psycopg.errors.QueryCanceled, SearchDeadlineExceeded)
            ):
                with parent_chunk_proof.ParentMetadataSpool(
                    max_bytes=self.limits.max_spool_bytes
                ) as spool:
                    directory = spool.directory.name
                    parent_chunk_proof._capture(
                        self.store,
                        spool,
                        self.scope,
                        self.limits,
                        time.monotonic() + 0.1,
                    )
        self.assertFalse(Path(directory).exists())
        self.assertEqual(self.store.active_connections, 0)
        self.assertEqual(self.capture()[0][2], 12)

    def test_many_native_singletons_cannot_grow_beyond_spool_cap(self):
        from e2e_singleton_metadata_admission import SingletonMetadataAdmission
        from recall_server.chunk_retirement import (
            retire_current_chunks,
            retire_singleton_metadata,
        )

        SingletonMetadataAdmission.setUpClass()
        f = SingletonMetadataAdmission()
        f.setUp()
        try:
            for i in range(128):
                f.ingest(f"turn/cap/{i}", f"Exact admitted singleton number {i}.")
            report = f.projector.project_pending(
                tenant_id=f.tenant, batch_size=10, max_batches=1, upload_concurrency=1
            )
            self.assertEqual(report["failed"], 0)
            self.assertEqual(report["documents"], 1)
            with f.store.connect() as c:
                docs = c.execute(
                    "SELECT document_id FROM canonical_documents WHERE tenant_id=%s ORDER BY document_id",
                    (f.tenant,),
                ).fetchall()
            retired = 0
            with (
                patch.dict(os.environ, RECALL_CHUNK_BODY_READS="archive"),
                patch.object(f.store, "search_plane", "turbopuffer"),
                patch.object(f.store, "verified_singleton_receipts", True),
            ):
                for offset in range(0, 128, 8):
                    scope = dict(
                        tenant_id=f.tenant,
                        source_id=f.source,
                        document_ids=tuple(
                            d["document_id"] for d in docs[offset : offset + 8]
                        ),
                    )
                    body = retire_current_chunks(f.store, f.archive, **scope)
                    retire_current_chunks(
                        f.store,
                        f.archive,
                        **scope,
                        apply=True,
                        reviewed_plan=body["plan"],
                    )
                    metadata = retire_singleton_metadata(
                        f.store, f.archive, principal_id=f.principal, **scope
                    )
                    result = retire_singleton_metadata(
                        f.store,
                        f.archive,
                        principal_id=f.principal,
                        **scope,
                        apply=True,
                        reviewed_plan=metadata["plan"],
                    )
                    retired += result["retired_chunks"]
                self.assertEqual(retired, 128)
                # Accepted-width metadata makes derived singleton growth cross SQLite pages.
                with f.store.connect() as c:
                    c.execute(
                        "UPDATE canonical_events SET canonical_redacted=jsonb_set(canonical_redacted,'{type}',to_jsonb(repeat('w',600))) WHERE tenant_id=%s",
                        (f.tenant,),
                    )
                with parent_chunk_proof.ParentMetadataSpool(max_bytes=1024**3) as spool:
                    with patch.object(f.store, "verified_singleton_receipts", False):
                        parent_chunk_proof._capture(
                            f.store,
                            spool,
                            (f.tenant, f.source, "session"),
                            self.limits,
                            time.monotonic() + 10,
                        )
                    staged_bytes = (
                        (Path(spool.directory.name) / "metadata.sqlite").stat().st_size
                    )
                self.assertGreaterEqual(staged_bytes, 128 * 1024)
                for adapter in (parent_chunk_proof._capture,):
                    with self.subTest(adapter=adapter.__module__):
                        folder = None
                        with self.assertRaisesRegex(Exception, "metadata_invalid"):
                            with parent_chunk_proof.ParentMetadataSpool(
                                max_bytes=staged_bytes
                            ) as spool:
                                folder = spool.directory.name
                                adapter(
                                    f.store,
                                    spool,
                                    (f.tenant, f.source, "session"),
                                    replace(self.limits, max_spool_bytes=staged_bytes),
                                    time.monotonic() + 10,
                                )
                        self.assertFalse(Path(folder).exists())
        finally:
            f.doCleanups()
            SingletonMetadataAdmission.tearDownClass()


if __name__ == "__main__":
    unittest.main(verbosity=2)
