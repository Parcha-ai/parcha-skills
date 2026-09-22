#!/usr/bin/env python3
"""Committed hints preserve the exact historical mutation and its authority."""

from contextlib import contextmanager
from pathlib import Path
import tempfile
import time
import unittest

import e2e_bounded_canonical_thinning as retained
from recall_server.canonical_thinning import CanonicalBodyThinner
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector,
    mark_logical_evidence_dirty,
)
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.archive import FilesystemArchiveStore
from e2e_logical_evidence_projection import insert_record, insert_source

BULK = (
    "INSERT INTO canonical_events(tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,kind,content_sha256,revision,occurred_at,observed_at,canonical_redacted)\n   SELECT tenant_id,source_id,'evt_'||md5('bulk:'||i),'native:'||i,'parent:'||i,artifact_id,job_id,kind,content_sha256,revision,occurred_at,observed_at,canonical_redacted FROM canonical_events CROSS JOIN generate_series(1,%s)i WHERE tenant_id=%s AND source_id=%s",
    "INSERT INTO canonical_documents(tenant_id,source_id,document_id,event_id,artifact_id,native_id,content_sha256,revision,is_current,text_redacted,text_sha256)\n   SELECT tenant_id,source_id,'doc_'||lpad(i::text,32,'0'),'evt_'||md5('bulk:'||i),artifact_id,'native:'||i,content_sha256,revision,is_current,text_redacted,text_sha256 FROM canonical_documents CROSS JOIN generate_series(1,%s)i WHERE tenant_id=%s AND source_id=%s",
    "INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)\n   SELECT tenant_id,source_id,'chk_'||md5('bulk:'||i),'doc_'||lpad(i::text,32,'0'),ordinal,'recall://synthetic/'||i,text_redacted,text_sha256 FROM canonical_chunks CROSS JOIN generate_series(1,%s)i WHERE tenant_id=%s AND source_id=%s",
    "INSERT INTO canonical_evidence_documents(tenant_id,source_id,logical_document_id,native_parent_id,revision,evidence_id,manifest_artifact_id,manifest_storage_backend,manifest_object_key,manifest_content_sha256,manifest_size_bytes,manifest_media_type,manifest_encryption,manifest_version_id,document_content_sha256,record_count,receipt_count,part_count,first_occurred_at,last_occurred_at,source_updated_at)\n   SELECT tenant_id,source_id,'ldoc_'||md5('bulk:'||i),'parent:'||i,revision,'evd_'||md5('bulk:'||i),manifest_artifact_id,manifest_storage_backend,'objects/aa/'||repeat(md5('bulk:'||i),2),manifest_content_sha256,manifest_size_bytes,manifest_media_type,manifest_encryption,'version:'||i,document_content_sha256,record_count,receipt_count,part_count,first_occurred_at,last_occurred_at,source_updated_at FROM canonical_evidence_documents CROSS JOIN generate_series(1,%s)i WHERE tenant_id=%s AND source_id=%s",
)


class CommittedKeys(retained.Thinning):
    def test_z_40k_queued_prefix_hint_reaches_tail_without_skipping_history(self):
        self.insert("seed")
        with self.store.connect() as c:
            for query in BULK:
                c.execute(query, (40025, self.tenant, self.source))
                c.execute("ANALYZE " + query.split("(")[0].split()[-1])
            c.execute(
                "INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,generation,reason) SELECT %s,%s,'parent:'||i,1,'ingest' FROM generate_series(1,40000)i",
                (self.tenant, self.source),
            )

            def digest(table):
                return c.execute(
                    "SELECT md5(string_agg(row_to_json(t)::text,'' ORDER BY row_to_json(t)::text)) AS h FROM "
                    + table
                    + " t WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["h"]

            tables = (
                "canonical_chunks",
                "canonical_evidence_documents",
                "raw_artifacts",
                "canonical_evidence_document_queue",
            )
            before = {t: digest(t) for t in tables}
        started = time.monotonic()
        baseline = CanonicalBodyThinner(self.store, tenant_id=self.tenant)
        self.assertEqual(baseline.thin(batch_size=10)["documents"], 0)
        baseline_ms = (time.monotonic() - started) * 1000
        hints = tuple(
            (self.tenant, self.source, "doc_" + str(i).zfill(32))
            for i in range(40001, 40026)
        )
        started = time.monotonic()
        result = self.thinner.thin(batch_size=10, committed_keys=hints)
        hint_ms = (time.monotonic() - started) * 1000
        self.assertEqual(result["documents"], 10)
        self.assertEqual(result["scanned_keys"], 1024)
        self.assertEqual(self.thinner._after, baseline._after)
        with self.store.connect() as c:
            self.assertEqual(before, {t: digest(t) for t in tables})
        reports = [self.thinner.thin(batch_size=10) for _ in range(2)]
        self.assertEqual([r["documents"] for r in reports], [10, 5])
        self.assertFalse(self.thinner._committed_keys)
        stale = CanonicalBodyThinner(self.store, tenant_id=self.tenant)
        started = time.monotonic()
        stale_result = stale.thin(
            batch_size=10,
            committed_keys=tuple(
                (self.tenant, self.source, "doc_" + str(i).zfill(32))
                for i in range(1, 257)
            ),
        )
        stale_ms = (time.monotonic() - started) * 1000
        self.assertEqual(stale_result["documents"], 0)
        self.assertEqual(len(stale._committed_keys), 256)
        started = time.monotonic()
        self.assertEqual(stale.thin(batch_size=10)["documents"], 0)
        repeated_stale_ms = (time.monotonic() - started) * 1000
        print(
            {
                "synthetic_prefix": 40000,
                "baseline_first_call_documents": 0,
                "hint_first_call_documents": 10,
                "baseline_ms": baseline_ms,
                "hint_ms": hint_ms,
                "queued_stale_hints": 256,
                "stale_first_ms": stale_ms,
                "stale_repeat_ms": repeated_stale_ms,
            },
            flush=True,
        )

    def test_hint_guard_race_locks_and_unknown_ack(self):
        _, doc = self.insert("hint")
        owner = self

        class Wrapped:
            mode = None

            @contextmanager
            def connect(wrapper):
                class Connection:
                    def execute(_, query, params):
                        if query.lstrip().startswith("SELECT source_id,document_id"):
                            # Isolate hint selection from historical enumeration.
                            from types import SimpleNamespace

                            return SimpleNamespace(
                                fetchone=lambda: None, fetchall=lambda: []
                            )
                        if "updated_documents AS" in query and wrapper.mode == "queue":
                            owner.queue("hint")
                        return c.execute(query, params)

                with owner.store.connect() as c:
                    yield Connection()
                    if wrapper.mode == "rollback":
                        raise RuntimeError("unknown ACK")
                if wrapper.mode == "lost":
                    raise RuntimeError("unknown ACK")

        wrapped = Wrapped()
        thinner = CanonicalBodyThinner(wrapped, tenant_id=self.tenant)
        hints = ((self.tenant, self.source, doc),)
        with self.store.connect() as held:
            held.execute(
                "SELECT 1 FROM canonical_documents WHERE tenant_id=%s AND document_id=%s FOR UPDATE",
                (self.tenant, doc),
            )
            self.assertEqual(
                thinner.thin(batch_size=1, committed_keys=hints)["documents"], 0
            )
        wrapped.mode = "queue"
        self.assertEqual(
            thinner.thin(batch_size=1, committed_keys=hints)["documents"], 0
        )
        with self.store.connect() as c:
            c.execute(
                "DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s",
                (self.tenant,),
            )
        for mode, remaining in (("rollback", 1), ("lost", 0)):
            wrapped.mode = mode
            with self.assertRaisesRegex(RuntimeError, "unknown ACK"):
                thinner.thin(batch_size=1, committed_keys=hints)
            self.assertEqual(self.remaining(), remaining)
            self.assertIn((self.source, doc), thinner._committed_keys)
        wrapped.mode = None
        self.assertEqual(thinner.thin(batch_size=1)["documents"], 0)

    def test_worker_timeout_rolls_back_retains_hints_and_reuses_pool(self):
        import os
        import psycopg
        from psycopg.rows import dict_row
        from psycopg_pool import ConnectionPool

        _, doc = self.insert("timeout")
        with self.store.connect() as c:
            c.execute(
                "CREATE FUNCTION committed_hint_test_wait() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_sleep(3); RETURN NEW; END$$"
            )
            c.execute(
                "CREATE TRIGGER committed_hint_wait BEFORE UPDATE ON canonical_events FOR EACH ROW EXECUTE FUNCTION committed_hint_test_wait()"
            )
        pool = ConnectionPool(
            os.environ["RECALL_DATABASE_URL"],
            min_size=1,
            max_size=1,
            kwargs={"row_factory": dict_row},
        )

        class OneConnection:
            def connect(self):
                return pool.connection()

        thinner = CanonicalBodyThinner(OneConnection(), tenant_id=self.tenant)
        try:
            try:
                with pool.connection() as c:
                    pid = c.info.backend_pid
                    original_timeout = c.execute("SHOW statement_timeout").fetchone()[
                        "statement_timeout"
                    ]
                started = time.monotonic()
                with self.assertRaises(psycopg.errors.QueryCanceled):
                    thinner.thin(
                        batch_size=1, committed_keys=((self.tenant, self.source, doc),)
                    )
                elapsed = time.monotonic() - started
                self.assertGreater(elapsed, 1.7)
                self.assertLess(elapsed, 4)
                # The document UPDATE preceded the blocked event UPDATE: this
                # proves rollback of already-written rows, not just no writes.
                self.assertEqual(self.remaining(), 1)
                self.assertIsNone(thinner._after)
                self.assertIsNone(thinner._through)
                self.assertIn((self.source, doc), thinner._committed_keys)
                with pool.connection() as c:
                    self.assertEqual(c.info.backend_pid, pid)
                    self.assertEqual(
                        c.info.transaction_status, psycopg.pq.TransactionStatus.IDLE
                    )
                    self.assertEqual(
                        c.execute("SHOW statement_timeout").fetchone()[
                            "statement_timeout"
                        ],
                        original_timeout,
                    )
                    self.assertEqual(c.execute("SELECT 1 AS n").fetchone()["n"], 1)
            finally:
                with self.store.connect() as c:
                    c.execute("DROP TRIGGER committed_hint_wait ON canonical_events")
                    c.execute("DROP FUNCTION committed_hint_test_wait()")
            self.assertEqual(thinner.thin(batch_size=1)["documents"], 1)
            self.assertEqual(self.remaining(), 0)
            self.assertFalse(thinner._committed_keys)
            with pool.connection() as c:
                self.assertEqual(c.info.backend_pid, pid)
                self.assertEqual(
                    c.execute("SHOW statement_timeout").fetchone()["statement_timeout"],
                    original_timeout,
                )
        finally:
            pool.close()

    def test_real_projection_handoff_only_after_committed_upload(self):
        with self.store.connect() as c:
            insert_source(c, self.tenant, "principal:test", self.source)
            insert_record(
                c,
                tenant=self.tenant,
                source=self.source,
                parent="parent:hint",
                native="record:hint",
                text="retained source",
                role="assistant",
                byte_start=0,
            )
            mark_logical_evidence_dirty(
                c,
                tenant_id=self.tenant,
                source_id=self.source,
                native_ids=["record:hint"],
                reason="ingest",
            )
            doc = c.execute(
                "SELECT document_id FROM canonical_documents WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["document_id"]
        with tempfile.TemporaryDirectory() as tmp:
            p = CanonicalLogicalEvidenceProjector(
                self.store,
                LogicalEvidenceProjectionStore(
                    FilesystemArchiveStore(
                        Path(tmp),
                        namespace_key=b"synthetic-committed-body-keys-namespace",
                    )
                ),
                bound_tenant_id=self.tenant,
            )
            self.assertEqual(p._take_committed_body_keys(), ())
            result = p.project_pending(batch_size=1, max_batches=1, quiet_seconds=0)
            self.assertEqual(result["documents"], 1)
            self.assertEqual(
                p._take_committed_body_keys(), ((self.tenant, self.source, doc),)
            )
            self.assertEqual(p._take_committed_body_keys(), ())


if __name__ == "__main__":
    unittest.main(verbosity=2)
