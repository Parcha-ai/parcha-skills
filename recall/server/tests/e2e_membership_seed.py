#!/usr/bin/env python3
"""Historical admission uses the existing metadata phase and native proof."""

from concurrent.futures import ThreadPoolExecutor
import threading
import hashlib
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

    def test_historical_missing_generation_seed_reaches_native_proof(self):
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s', (self.tenant,))
        self.assertEqual(self.compare()['certified_passages'], 0)
        seeded = self.seed(limit=10)
        self.assertEqual(seeded['queued'], 1)
        publisher = self.publisher()
        with patch.object(publisher, '_prepare', side_effect=AssertionError('no archive preparation')):
            result = publisher.project_pending(tenant_id=self.tenant, batch_size=10, max_batches=1)
        self.assertEqual(result['documents'], 0)
        self.assertEqual(result['membership_certified_passages'], len(self.ids))
        compared = self.compare()
        self.assertEqual(compared['certified_passages'], len(self.ids))
        self.assertEqual(compared['native_allowed_passages'], len(self.ids))
        self.assertEqual(compared['disagreements'], 0)

    def test_lost_authority_with_old_certificate_is_not_reinitialized(self):
        self.assertEqual(self.enroll()['status'], 'enrolled')
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s', (self.tenant,))
        result = self.seed()
        self.assertEqual(result['queued'], 0)
        self.assertIn('authority_generation_missing', result['deferred'][0]['reasons'])
        self.assertEqual(self.compare()['certified_passages'], 0)
        self.assertEqual(self.queued(), [])
        with self.store.connect() as c:
            self.assertIsNone(c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s',
                                       (self.tenant,)).fetchone())

    def test_initializer_lost_commit_response_does_not_admit_or_reset_generation(self):
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s', (self.tenant,))
        connect = enrollment._connect
        query = enrollment._query
        initialized = []
        def track(*args):
            result = query(*args)
            if 'INSERT INTO canonical_parent_authority' in args[2]:
                initialized.append(True)
            return result
        @contextmanager
        def lose_response(*args):
            with connect(*args) as connection:
                yield connection
            if initialized:
                raise OSError('synthetic lost initializer COMMIT response')
        with patch.object(enrollment, '_query', side_effect=track), \
                patch.object(enrollment, '_connect', side_effect=lose_response), self.assertRaises(OSError):
            self.seed()
        self.assertEqual(self.queued(), [])
        with self.store.connect() as c:
            generation = c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s',
                                   (self.tenant,)).fetchone()['generation']
        self.assertEqual(generation, 1)
        self.assertEqual(self.seed()['queued'], 1)
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s',
                                      (self.tenant,)).fetchone()['generation'], generation)

    def test_initializer_releases_authority_before_queue_admission(self):
        self.queue()
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s', (self.tenant,))
        queue_locked = threading.Event()
        blocked_pid = []
        original = enrollment._query
        futures = []
        def canonical_writer():
            with self.store.connect() as c:
                c.execute("SET LOCAL lock_timeout='2s'")
                c.execute('SELECT generation FROM canonical_passage_projection_queue WHERE tenant_id=%s FOR UPDATE',
                          (self.tenant,)).fetchall()
                blocked_pid.append(c.info.backend_pid)
                queue_locked.set()
                c.execute('INSERT INTO canonical_parent_authority(tenant_id,source_id,native_parent_id,generation) '
                          'VALUES(%s,%s,%s,1) ON CONFLICT(tenant_id,source_id,native_parent_id) '
                          'DO UPDATE SET generation=canonical_parent_authority.generation+1',
                          (self.tenant,self.source,'session'))
        with ThreadPoolExecutor(max_workers=1) as pool:
            def after_initialize(*args):
                result = original(*args)
                if 'INSERT INTO canonical_parent_authority' in args[2] and not futures:
                    futures.append(pool.submit(canonical_writer))
                    self.assertTrue(queue_locked.wait(1), 'concurrent writer must own queue first')
                    waiting = False
                    until = time.monotonic() + 1
                    while time.monotonic() < until:
                        with self.store.connect() as c:
                            waiting = c.execute("SELECT wait_event_type='Lock' AS waiting FROM pg_stat_activity "
                                                "WHERE pid=%s", (blocked_pid[0],)).fetchone()['waiting']
                        if waiting:
                            break
                        time.sleep(.01)
                    self.assertTrue(waiting, 'native invalidation must wait on the uncommitted initializer')
                return result
            with patch.object(enrollment, '_query', side_effect=after_initialize):
                result = self.seed()
            futures[0].result(timeout=3)
        self.assertEqual(result['queued'], 0)
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s',
                                      (self.tenant,)).fetchone()['generation'], 2)
            self.assertEqual(c.execute('SELECT reason FROM canonical_passage_projection_queue WHERE tenant_id=%s',
                                      (self.tenant,)).fetchone()['reason'], 'logical-update')

    def test_concurrent_generation_creation_wins_without_reset(self):
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s', (self.tenant,))
        original = enrollment._query
        created = []
        def create_first(*args):
            if 'INSERT INTO canonical_parent_authority' in args[2] and not created:
                created.append(True)
                with self.store.connect() as c:
                    c.execute('INSERT INTO canonical_parent_authority(tenant_id,source_id,native_parent_id,generation) '
                              'VALUES(%s,%s,%s,7)', (self.tenant,self.source,'session'))
            return original(*args)
        with patch.object(enrollment, '_query', side_effect=create_first):
            self.assertEqual(self.seed()['queued'], 1)
        self.assertEqual(self.queued()[0]['checkpoint']['authority_generation'], 7)
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s',
                                      (self.tenant,)).fetchone()['generation'], 7)

    def test_missing_generation_with_revoked_owner_is_not_initialized(self):
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s', (self.tenant,))
            c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s', (self.tenant,))
        self.assertEqual(self.seed()['queued'], 0)
        with self.store.connect() as c:
            self.assertIsNone(c.execute('SELECT generation FROM canonical_parent_authority WHERE tenant_id=%s',
                                       (self.tenant,)).fetchone())

    def passive_retirement(self):
        with self.store.connect() as c:
            c.execute("""INSERT INTO canonical_chunk_retirement_progress(
                tenant_id,source_id,native_parent_id,logical_document_id,enabled,status)
                SELECT tenant_id,source_id,native_parent_id,logical_document_id,true,'pending'
                FROM canonical_evidence_documents WHERE tenant_id=%s""", (self.tenant,))

    def active_retirement(self):
        with self.store.connect() as c:
            c.execute("""INSERT INTO canonical_retirement_source_policy(
                tenant_id,source_id,principal_id,maintenance_intent)
                VALUES(%s,%s,%s,'{"parent":"session"}'::jsonb)""",
                (self.tenant,self.source,self.principal))

    def test_passive_retirement_seed_reaches_native_proof_on092(self):
        if not self.additive_membership_generation:
            self.skipTest('schema092-specific admission')
        self.passive_retirement()
        result = self.seed()
        self.assertEqual(result['queued'],1)
        self.assertEqual(result['deferred'],[])
        publisher = self.publisher()
        with patch.object(publisher,'_prepare',side_effect=AssertionError('no archive republication')):
            outcome = publisher.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1)
        self.assertEqual(outcome['documents'],0)
        self.assertEqual(outcome['membership_certified_passages'],len(self.ids))
        compared = self.compare()
        self.assertEqual(compared['certified_passages'],len(self.ids))
        self.assertEqual(compared['native_allowed_passages'],len(self.ids))
        self.assertEqual(compared['disagreements'],0)
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT status FROM canonical_chunk_retirement_progress WHERE tenant_id=%s',(self.tenant,)).fetchone()['status'],'pending')

    def test_seed_recertifies_nonnull_v2_binding_with_matching_generation(self):
        legacy_fields = (
            'tenant_id', 'source_id', 'native_parent_id', 'logical_document_id',
            'passage_id', 'policy_fingerprint', 'text_sha256', 'receipts',
            'manifest_content_sha256', 'manifest_revision', 'projected_revision',
            'passage_revision', 'source_document_sha256',
        )
        def legacy(row, scope):
            payload = ['recall.passage-membership-binding.v2', scope['binding_epoch'],
                       *[row[field] for field in legacy_fields]]
            return hashlib.sha256(json.dumps(payload, ensure_ascii=False,
                separators=(',', ':'), allow_nan=False).encode()).hexdigest()
        with patch.object(enrollment, '_binding', legacy):
            self.assertEqual(self.enroll()['status'], 'enrolled')
            self.assertEqual(self.compare()['certified_passages'], len(self.ids))
        self.assertEqual(self.compare()['certified_passages'],0)
        self.assertEqual(self.compare()['native_allowed_passages'],len(self.ids))
        self.assertEqual(self.queued(),[])
        with self.store.connect() as c:
            before = c.execute('SELECT passage_id,membership_generation,membership_binding_sha256 '
                               'FROM canonical_passages WHERE tenant_id=%s ORDER BY passage_id',(self.tenant,)).fetchall()
        self.assertTrue(all(r['membership_generation'] is not None and r['membership_binding_sha256'] for r in before))
        self.assertEqual(self.seed()['queued'],1)
        publisher = self.publisher()
        with patch.object(publisher,'_prepare',side_effect=AssertionError('no source re-publication')):
            result = publisher.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1)
        self.assertEqual(result['documents'],0)
        self.assertEqual(result['membership_certified_passages'],len(self.ids))
        compared = self.compare()
        self.assertEqual(compared['certified_passages'],len(self.ids))
        self.assertEqual(compared['native_allowed_passages'],len(self.ids))
        self.assertEqual(compared['disagreements'],0)
        with self.store.connect() as c:
            after = c.execute('SELECT passage_id,membership_generation,membership_binding_sha256 '
                              'FROM canonical_passages WHERE tenant_id=%s ORDER BY passage_id',(self.tenant,)).fetchall()
        self.assertEqual([(r['passage_id'],r['membership_generation']) for r in before],
                         [(r['passage_id'],r['membership_generation']) for r in after])
        self.assertTrue(all(a['membership_binding_sha256'] != b['membership_binding_sha256']
                            for a,b in zip(before,after,strict=True)))

    def test_active_retirement_still_defers_seed(self):
        self.passive_retirement()
        self.active_retirement()
        result = self.seed()
        self.assertEqual(result['queued'],0)
        self.assertIn('retirement_intent',result['deferred'][0]['reasons'])
        self.assertEqual(self.queued(),[])
        self.assertEqual(self.compare()['certified_passages'],0)

    def test_active_intent_after_passive_admission_is_rechecked(self):
        if not self.additive_membership_generation:
            self.skipTest('schema092-specific admission')
        self.passive_retirement()
        self.assertEqual(self.seed()['queued'],1)
        self.active_retirement()
        publisher = self.publisher()
        result = publisher.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1)
        self.assertEqual(result['membership_certified_passages'],0)
        self.assertEqual(len(self.queued()),1)
        self.assertEqual(self.compare()['certified_passages'],0)
        with self.store.connect() as c:
            c.execute('UPDATE canonical_retirement_source_policy SET maintenance_intent=NULL WHERE tenant_id=%s',(self.tenant,))
        result = publisher.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1)
        self.assertEqual(result['membership_certified_passages'],len(self.ids))
        self.assertEqual(self.compare()['disagreements'],0)

    def test_edit_during_passive_retirement_proof_refuses_stale_stamp(self):
        if not self.additive_membership_generation:
            self.skipTest('schema092-specific admission')
        self.passive_retirement()
        self.assertEqual(self.seed()['queued'],1)
        original = enrollment._capture
        def edited(*args,**kwargs):
            captured = original(*args,**kwargs)
            self.edit()
            return captured
        with patch.object(enrollment,'_capture',side_effect=edited):
            result = self.publisher().project_pending(tenant_id=self.tenant,batch_size=10,max_batches=1)
        self.assertEqual(result['membership_certified_passages'],0)
        compared=self.compare()
        self.assertEqual(compared['certified_passages'],0)
        self.assertEqual(compared['disagreements'],0)

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
        query = enrollment._query
        admitted = []
        def track(*args):
            result = query(*args)
            if 'INSERT INTO canonical_passage_projection_queue' in args[2]:
                admitted.append(True)
            return result

        @contextmanager
        def lost_response(*args):
            with original(*args) as connection:
                yield connection
            if admitted:
                raise OSError("synthetic lost response after actual queue commit")

        with (
            patch.object(enrollment, "_connect", side_effect=lost_response),
            patch.object(enrollment, "_query", side_effect=track),
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
        if self.additive_membership_generation:
            self.assertEqual(result["queued"], 1)
            self.assertEqual(result["deferred"], [])
            self.assertEqual(len(self.queued()), 1)
            return
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
        with self.store.connect() as c:
            c.execute('DELETE FROM canonical_parent_authority WHERE tenant_id=%s', (self.tenant,))
            # Observe actual insertion order, which must match the existing
            # deferred invalidator when several authority rows share a commit.
            c.execute('CREATE TABLE seed_authority_order(ordinal bigserial,native_parent_id text)')
            c.execute("""CREATE FUNCTION record_seed_authority_order() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN
                INSERT INTO seed_authority_order(native_parent_id) VALUES(NEW.native_parent_id);
                RETURN NEW; END $$""")
            c.execute('CREATE TRIGGER record_seed_authority_order AFTER INSERT ON canonical_parent_authority '
                      'FOR EACH ROW EXECUTE FUNCTION record_seed_authority_order()')
        native = enrollment._query
        calls = []

        def delayed(*args):
            calls.append(True)
            time.sleep(0.02)
            return native(*args)

        started = time.monotonic()
        try:
            with patch.object(enrollment, "_query", side_effect=delayed):
                result = self.seed()
            elapsed = time.monotonic() - started
            with self.store.connect() as c:
                actual = [row['native_parent_id'] for row in c.execute(
                    'SELECT native_parent_id FROM seed_authority_order ORDER BY ordinal').fetchall()]
            self.assertEqual(len(actual), 128)
            self.assertEqual(actual, sorted(actual))
        finally:
            with self.store.connect() as c:
                c.execute('DROP TRIGGER record_seed_authority_order ON canonical_parent_authority')
                c.execute('DROP FUNCTION record_seed_authority_order()')
                c.execute('DROP TABLE seed_authority_order')
        print(
            json.dumps(
                dict(
                    seed_parents=result["queued"],
                    query_calls=len(calls),
                    injected_query_ms=20,
                    elapsed_ms=round(elapsed * 1000),
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
