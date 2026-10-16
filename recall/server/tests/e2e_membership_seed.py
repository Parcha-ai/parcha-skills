#!/usr/bin/env python3
"""Historical admission uses the existing metadata phase and native proof."""

import time
import io
import json
import os
from contextlib import contextmanager, redirect_stdout
import unittest
from unittest.mock import patch
from e2e_parent_membership_publication import ParentMembershipPublicationTests
from recall_server import membership_remainder as remainder
from recall_server import parent_membership_enrollment as enrollment


class HistoricalMembershipSeedTests(ParentMembershipPublicationTests):
    def seed(self, **kwargs):
        return remainder.seed_history(
            self.store,
            tenant_id=self.tenant,
            settings=self.settings(),
            deadline_at=time.monotonic() + 5,
            **kwargs,
        )

    def test_historical_seed_worker_proves_without_archive_republication(self):
        self.scope["binding_epoch"] = "synthetic-epoch-a"
        self.assertEqual(self.compare()["certified_passages"], 0)
        seeded = self.seed(limit=10)
        self.assertEqual(seeded["queued"], 1)
        self.assertFalse(seeded["coverage_measured"])
        publisher = self.publisher()
        with patch.object(
            publisher, "_prepare", side_effect=AssertionError("no source preparation")
        ):
            result = publisher.project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1
            )
        self.assertEqual(result["documents"], 0)
        self.assertEqual(result["membership_certified_passages"], len(self.ids))
        compared = self.compare()
        self.assertEqual(compared["certified_passages"], len(self.ids))
        self.assertEqual(compared["native_allowed_passages"], len(self.ids))
        self.assertEqual(compared["disagreements"], 0)

    def queued(self):
        return remainder.pending(
            self.store,
            tenant_id=self.tenant,
            limit=20,
            deadline_at=time.monotonic() + 3,
        )

    def test_repeat_seed_preserves_partial_and_normal_publication_work(self):
        self.seed()
        row = self.queued()[0]
        self.assertTrue(
            remainder.change_cursor(
                self.store,
                row,
                row["checkpoint"],
                after=min(self.ids),
                deadline_at=time.monotonic() + 3,
            )
        )
        before = self.queued()
        repeated = self.seed()
        self.assertEqual(repeated["queued"], 0)
        self.assertEqual(self.queued(), before)
        self.queue()
        with self.store.connect() as c:
            before = c.execute(
                "SELECT * FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchall()
        self.assertEqual(self.seed()["queued"], 0)
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT * FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchall(),
                before,
            )
        self.assertEqual(before[0]["reason"], "logical-update")

    def test_normal_publication_wins_during_seed_admission(self):
        original = enrollment._query
        interleaved = []

        def query(store, connection, statement, parameters, deadline):
            if (
                "INSERT INTO canonical_passage_projection_queue" in statement
                and not interleaved
            ):
                interleaved.append(True)
                self.queue()
            return original(store, connection, statement, parameters, deadline)

        with patch.object(enrollment, "_query", side_effect=query):
            seeded = self.seed()
        self.assertEqual(seeded["queued"], 0)
        with self.store.connect() as c:
            reason = c.execute(
                "SELECT reason FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()["reason"]
        self.assertEqual(reason, "logical-update")
        self.assertEqual(
            self.publisher().project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1
            )["documents"],
            1,
        )

    def test_commit_lost_response_repeat_does_not_reset_work(self):
        original = enrollment._connect

        @contextmanager
        def lost_response(*args):
            with original(*args) as connection:
                yield connection
            raise OSError("synthetic lost response after actual commit")

        with (
            patch.object(enrollment, "_connect", side_effect=lost_response),
            self.assertRaises(OSError),
        ):
            self.seed()
        self.assertEqual(len(self.queued()), 1)
        before = self.queued()
        self.assertEqual(self.seed()["queued"], 0)
        self.assertEqual(self.queued(), before)
        result = self.publisher().project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1
        )
        self.assertEqual(result["documents"], 0)
        self.assertEqual(self.compare()["certified_passages"], len(self.ids))

    def test_failure_before_commit_rolls_back_admission(self):
        original = enrollment._query

        def interrupted(store, connection, statement, parameters, deadline):
            result = original(store, connection, statement, parameters, deadline)
            if "INSERT INTO canonical_passage_projection_queue" in statement:
                raise OSError("synthetic interruption before transaction commit")
            return result

        with (
            patch.object(enrollment, "_query", side_effect=interrupted),
            self.assertRaises(OSError),
        ):
            self.seed()
        self.assertEqual(self.queued(), [])
        self.assertEqual(self.seed()["queued"], 1)

    def test_disabled_and_expired_seed_do_not_checkout(self):
        with patch.object(
            self.store, "connect", side_effect=AssertionError("must not checkout")
        ):
            with self.assertRaisesRegex(ValueError, "disabled"):
                remainder.seed_history(
                    self.store,
                    tenant_id=self.tenant,
                    settings=self.settings(enabled=False),
                    deadline_at=time.monotonic() + 3,
                )
            with self.assertRaises(enrollment.SearchDeadlineExceeded):
                remainder.seed_history(
                    self.store,
                    tenant_id=self.tenant,
                    settings=self.settings(),
                    deadline_at=time.monotonic() - 1,
                )
            for kwargs in (
                {"limit": 0},
                {"limit": True},
                {"limit": 1001},
                {"after": ["bad"]},
                {"after": ["z", "z"], "through": ["a", "a"]},
            ):
                with self.assertRaises(ValueError):
                    self.seed(**kwargs)

    def test_busy_parent_deferred_with_exact_cursor(self):
        with self.store.connect() as c:
            c.execute(
                """INSERT INTO canonical_chunk_retirement_progress(
              tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
              SELECT tenant_id,source_id,native_parent_id,logical_document_id,true,'pending'
              FROM canonical_evidence_documents WHERE tenant_id=%s""",
                (self.tenant,),
            )
        result = self.seed()
        self.assertEqual(result["scanned"], 1)
        self.assertEqual(result["queued"], 0)
        self.assertEqual(
            result["deferred"],
            [dict(key=result["after"], reasons=["retirement_pending"])],
        )
        self.assertEqual(self.queued(), [])

    def test_grant_revocation_after_seed_remains_authoritative(self):
        self.seed()
        with self.store.connect() as c:
            c.execute(
                "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND principal_id=%s",
                (self.tenant, self.principal),
            )
        result = self.publisher().project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1
        )
        self.assertEqual(result["membership_certified_passages"], 0)
        self.assertEqual(self.compare()["status"], "unauthorized")

    def test_forget_after_seed_does_not_certify_deleted_evidence(self):
        from recall_server.canonical import CanonicalLifecycleError

        self.seed()
        request = dict(
            contract="recall.forget-request.v1",
            schema_version=1,
            tenant_id=self.tenant,
            principal_id=self.principal,
            source_id=self.source,
            target_receipt=self.receipt,
            mode="explicit_forget",
            reason="owner_requested",
            requested_at="2026-10-14T00:01:00Z",
            idempotency_key="seed-forget",
        )
        with patch.object(
            self.archive,
            "delete_internal_raw_bounded",
            side_effect=OSError("archive unavailable"),
        ):
            with self.assertRaisesRegex(
                CanonicalLifecycleError, "archive_delete_failed"
            ):
                self.plane.forget(request)
        result = self.publisher().project_pending(
            tenant_id=self.tenant, batch_size=10, max_batches=1
        )
        self.assertEqual(result["membership_certified_passages"], 0)
        self.assertEqual(self.compare()["certified_passages"], 0)
        self.assertEqual(self.compare()["disagreements"], 0)

    def test_keyset_resume_preserves_initial_upper_bound(self):
        original = self.plane.ingest_document

        def ingest_parent(parent):
            def ingest(**kwargs):
                kwargs["envelope"]["native_parent_id"] = parent
                return original(**kwargs)

            with patch.object(self.plane, "ingest_document", side_effect=ingest):
                self.ingest("turn/" + parent, "Synthetic stable historical evidence")
            self.projector.project_pending(
                tenant_id=self.tenant,
                batch_size=10,
                max_batches=1,
                upload_concurrency=1,
            )
            self.publisher(self.settings(enabled=False)).project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1
            )

        ingest_parent("second")
        first = self.seed(limit=1)
        self.assertEqual(first["status"], "bounded")
        self.assertEqual(first["scanned"], 1)
        original_source = self.source
        self.source += ":later"
        try:
            ingest_parent("after-frozen-upper")
        finally:
            self.source = original_source
        second = self.seed(limit=1, after=first["after"], through=first["through"])
        self.assertEqual(second["status"], "completed")
        self.assertEqual(second["scanned"], 1)
        self.assertEqual(second["through"], first["through"])
        self.assertNotEqual(first["after"], second["after"])
        final = self.seed(limit=1, after=second["after"], through=first["through"])
        self.assertEqual(final["scanned"], 0)
        self.assertEqual(len(self.queued()), 2)
        self.assertEqual(self.seed()["queued"], 1)

    def test_default_page_progress_with_twenty_ms_query_latency(self):
        original_ingest = self.plane.ingest_document
        for ordinal in range(127):

            def ingest(**kwargs):
                kwargs["envelope"]["native_parent_id"] = "history-" + str(ordinal)
                return original_ingest(**kwargs)

            with patch.object(self.plane, "ingest_document", side_effect=ingest):
                self.ingest(
                    "turn/history-" + str(ordinal), "Synthetic historical publication"
                )
        logical = self.projector.project_pending(
            tenant_id=self.tenant, batch_size=200, max_batches=1, upload_concurrency=4
        )
        self.assertEqual(logical["failed"], 0)
        published = self.publisher(self.settings(enabled=False)).project_pending(
            tenant_id=self.tenant, batch_size=200, max_batches=1, concurrency=4
        )
        self.assertEqual(published["documents"], 127)
        native = enrollment._query
        calls = []

        def delayed(*args):
            calls.append(True)
            time.sleep(0.02)
            return native(*args)

        started = time.monotonic()
        with patch.object(enrollment, "_query", side_effect=delayed):
            result = self.seed()
        print(
            json.dumps(
                dict(
                    seed_parents=result["queued"],
                    query_calls=len(calls),
                    injected_query_ms=20,
                    elapsed_ms=round((time.monotonic() - started) * 1000),
                )
            )
        )
        self.assertEqual(result["scanned"], 128)
        self.assertEqual(result["queued"], 128)
        self.assertLess(len(calls), 10)

    def test_cli_dispatch_uses_native_metadata_seed_only(self):
        from recall_server import cli

        output = io.StringIO()
        with (
            patch.object(cli, "BrainStore", return_value=self.store),
            patch.object(
                cli,
                "build_evidence_archive_store",
                side_effect=AssertionError("archive construction"),
            ),
            patch.dict(
                os.environ,
                RECALL_PARENT_MEMBERSHIP_CERTIFICATION="1",
                RECALL_PARENT_MEMBERSHIP_EPOCH="synthetic-epoch-a",
            ),
            patch(
                "sys.argv",
                [
                    "recall-server",
                    "membership-certification-seed",
                    "--tenant",
                    self.tenant,
                    "--limit",
                    "1",
                ],
            ),
            redirect_stdout(output),
        ):
            cli.main()
        result = json.loads(output.getvalue())
        self.assertEqual(result["queued"], 1)
        self.assertFalse(result["coverage_measured"])


if __name__ == "__main__":
    suite = unittest.TestSuite(
        HistoricalMembershipSeedTests(name)
        for name in HistoricalMembershipSeedTests.__dict__
        if name.startswith("test_")
    )
    raise SystemExit(
        not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    )
