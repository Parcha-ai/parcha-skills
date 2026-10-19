#!/usr/bin/env python3
"""Existing historical Slack page/ACK owner converges native actor links."""
import json
import sqlite3
import sys
from pathlib import Path
import unittest
import uuid
from decimal import Decimal
from unittest.mock import patch

from e2e_managed_slack_identity import ManagedSlackIdentity
from connectors.slack_workspace import SlackWorkspaceConnector
from connectors.sdk import ConnectorRunner
from recall_server.canonical_retrieval import BoundCanonicalRetrieval
from recall_server import logical_evidence_projection
# Reuse the SDK's real historical rail fixture, including its sibling imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tests'))
from test_slack_historical_reconciliation import HistoricalRail


class ManagedSlackHistoryActors(ManagedSlackIdentity):
    def setup_history(self, before_enroll=None):
        worker = self.managed()
        owner = self
        class Rail(HistoricalRail):
            def request(self, operation, **arguments):
                if operation == 'users.list':
                    self.calls.append((operation,arguments))
                    return {'ok':True,'members':[{'id':'U123','profile':{'email':owner.email}}]}
                result = super().request(operation, **arguments)
                if operation == 'messages.history' and arguments['query'].get('inclusive') is False:
                    if owner.before_history: owner.before_history()
                return result
        self.rail = Rail()
        self.rail.now = Decimal('1790000000')
        self.rail.root = {'ts':'1789981200.000001','user':'U123','text':'Synthetic question for reader@example.invalid'}
        self.rail.messages = [self.rail.root]
        self.rail.channel_messages = {'C123':self.rail.messages}
        self.spool = worker.spool_root/'historical.db'
        self.before_history = None
        def build(*_):
            connector = SlackWorkspaceConnector(rail=self.rail,source_id=self.source,
                workspace_id='T123',channel_ids=('C123',))
            connector.reconciliation_epoch = 'a'*64
            return connector,self.spool
        worker.connector_factory = build
        self.worker = worker
        for _ in range(2): self.assertEqual(self.real_step()['failed'],0)
        self.assertEqual(self.counts(),(0,0))
        if before_enroll: before_enroll()
        self.enroll()
        # Drive the original eight-live/one-history schedule, never edit its cursor.
        while self.state()['live_pages'] < 8:
            self.assertEqual(self.real_step()['failed'],0)
        self.assertEqual(self.counts(),(1,0))
        self.project()
        self.assertEqual(self.scoped(),[])

    def real_step(self):
        original = ConnectorRunner.run_once
        self.sdk_results = []
        def observed_run(runner):
            self.last_runner = runner
            result = original(runner)
            self.sdk_results.append(result)
            return result
        # These controls isolate the original provider-history lane. The new
        # captured-history entrypoint tests production run_once composition.
        with patch('recall_server.slack_captured_attribution.repair_slice', return_value=None), patch.object(ConnectorRunner, 'run_once', observed_run), patch(
            'connectors.slack_workspace._now', side_effect=self.rail.timestamp
        ):
            return self.step(self.worker)

    def state(self):
        with sqlite3.connect(self.spool) as c:
            return json.loads(c.execute('SELECT state_json FROM slack_reconciliation_state').fetchone()[0])

    def ack_rows(self):
        with sqlite3.connect(self.spool) as c:
            return c.execute('SELECT * FROM acknowledged_records ORDER BY native_sha256,content_sha256').fetchall()

    def immutable(self):
        with self.store.connect() as c:
            return {table:c.execute(f'SELECT * FROM {table} WHERE tenant_id=%s AND source_id=%s ORDER BY 1,2,3',
                (self.tenant,self.source)).fetchall() for table in (
                    'canonical_events','canonical_documents','canonical_chunks','canonical_source_grants','forget_tombstones')}

    def project(self):
        report=self.projector.project_pending(tenant_id=self.tenant,batch_size=10,max_batches=2,upload_concurrency=1)
        self.assertEqual(report['failed'],0)
        return report

    def scoped(self):
        result=BoundCanonicalRetrieval(self.store,tenant_id=self.tenant,principal_id=self.principal,
            authorized_sources=(self.source,)).scope_documents(filters={'person':'Synthetic','person_relation':'author'},limit=10)
        self.assertTrue(result['complete'])
        return result['documents']

    def test_historical_ack_replay_repairs_links_without_reingest(self):
        self.setup_history(); before=self.immutable(); acks=self.ack_rows(); state=self.state()
        result=self.real_step()
        self.assertEqual(result['failed'],0)
        self.assertEqual(self.counts(),(1,1))
        self.assertEqual(self.immutable(),before)
        self.assertNotIn('reader@example.invalid',str(before['canonical_events']))
        self.assertEqual(self.ack_rows(),acks)
        self.assertEqual(self.state()['historical_pages'],state['historical_pages']+1)
        self.assertEqual(self.sdk_results[-1]['deduplicated'],1)
        self.assertEqual(result['acked'],0)
        self.project();self.assertEqual(len(self.scoped()),1)
        self.assertEqual(self.immutable(),before)
        # Every step constructs a fresh connector/runner over the original spool.
        while self.state()['live_pages']<8:self.assertEqual(self.real_step()['failed'],0)
        self.assertEqual(self.real_step()['failed'],0)
        self.assertEqual(self.counts(),(1,1));self.assertEqual(self.project()['documents'],0)

    def test_failed_native_queue_rolls_back_and_preserves_history_cursor(self):
        self.setup_history();before=self.immutable();state=self.state();acks=self.ack_rows()
        with patch.object(logical_evidence_projection,'mark_logical_evidence_dirty',side_effect=RuntimeError('synthetic queue failure')):
            self.assertEqual(self.real_step()['failed'],1)
        self.assertEqual(self.state(),state);self.assertEqual(self.ack_rows(),acks)
        self.assertEqual(self.immutable(),before);self.assertEqual(self.counts(),(1,0))
        self.assertEqual(self.project()['documents'],0)
        self.assertEqual(self.real_step()['failed'],0)
        self.assertEqual(self.counts(),(1,1))
        self.project();self.assertEqual(len(self.scoped()),1)

    def test_revoked_source_during_historical_pull_cannot_attribute_or_advance(self):
        self.setup_history();state=self.state()
        def revoke():
            with self.store.connect() as c:c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',(self.tenant,self.source,self.principal))
        self.before_history=revoke
        self.assertEqual(self.real_step()['failed'],1)
        self.assertEqual(self.state(),state);self.assertEqual(self.counts(),(1,0))

    def test_native_forget_is_not_undone_by_acknowledged_historical_page(self):
        self.setup_history()
        with self.store.connect() as c:
            receipt=c.execute('''SELECT chunk.receipt FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id)
                WHERE document.tenant_id=%s AND document.source_id=%s AND document.native_id=%s AND document.is_current ORDER BY chunk.ordinal LIMIT 1''',
                (self.tenant,self.source,self.message.native_id)).fetchone()['receipt']
        self.plane.forget(dict(contract='recall.forget-request.v1',schema_version=1,
            tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,target_receipt=receipt,
            mode='explicit_forget',reason='owner_requested',requested_at='2026-10-19T09:00:00Z',idempotency_key=uuid.uuid4().hex))
        before=self.immutable()
        self.assertEqual(self.real_step()['failed'],0)
        self.assertEqual(self.counts(),(1,0));self.assertEqual(self.immutable(),before)
        self.project();self.assertEqual(self.scoped(),[])

    def test_committed_repair_survives_sdk_stage_failure_without_duplicate_dirty_work(self):
        self.setup_history();state=self.state();acks=self.ack_rows()
        with patch.object(ConnectorRunner,'_stage',side_effect=RuntimeError('synthetic stage failure')):
            self.assertEqual(self.real_step()['failed'],1)
        self.assertEqual(self.counts(),(1,1));self.assertEqual(self.state(),state)
        self.assertEqual(self.ack_rows(),acks)
        with self.store.connect() as c:
            queued=c.execute('SELECT native_parent_id,generation FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s ORDER BY native_parent_id',(self.tenant,self.source)).fetchall()
        self.assertEqual(len(queued),1)
        self.assertEqual(self.real_step()['failed'],0)
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT native_parent_id,generation FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s ORDER BY native_parent_id',(self.tenant,self.source)).fetchall(),queued)
        self.project();self.assertEqual(len(self.scoped()),1)

    def test_privacy_drop_on_historical_page_has_no_identity_side_effect(self):
        self.setup_history();before=self.immutable();acks=self.ack_rows()
        with self.store.connect() as c:
            c.execute("UPDATE connector_installations SET privacy_mode='drop' WHERE id=%s",(self.installation,))
        result=self.real_step()
        self.assertEqual(result['failed'],0);self.assertEqual(self.sdk_results[-1]['dropped'],1)
        self.assertEqual(self.counts(),(1,0));self.assertEqual(self.immutable(),before)
        self.assertEqual(self.ack_rows(),acks)

    def test_exact_page_source_does_not_reconcile_same_native_in_other_source(self):
        from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane
        from recall_server.projectors import canonical_json
        other=self.source+':other'
        def seed_other():
            with self.store.connect() as c:
                event=c.execute('SELECT canonical_redacted FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s',(self.tenant,self.source,self.message.native_id)).fetchone()['canonical_redacted']
            event={**event,'source_id':other}
            artifact=CanonicalArchiveGateway(self.store,self.archive,tenant_id=self.tenant,principal_id=self.principal).put_raw(
                tenant_id=self.tenant,source_id=other,native_id=event['native_id'],payload=canonical_json(event['content']),media_type='application/json',created_at=event['occurred_at'])
            event['provenance']={**event['provenance'],'artifact_ref':artifact}
            CanonicalPlane(self.store,self.archive).ingest_document(tenant_id=self.tenant,principal_id=self.principal,
                connector_id='slack.messages',artifact_ref=artifact,envelope=event,text_redacted='Synthetic other source')
        self.setup_history(before_enroll=seed_other)
        self.assertEqual(self.real_step()['failed'],0);self.assertEqual(self.counts(),(1,1))
        with self.store.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s',(self.tenant,other)).fetchone()['n'],0)

    def thin_history(self):
        from recall_server.canonical_thinning import thin_canonical_bodies
        self.project()
        # Model the existing durable-archive admission with the exact local
        # objects retained; only storage-backend metadata differs in this fixture.
        with self.store.connect() as c:
            c.execute("UPDATE raw_artifacts SET storage_backend='s3' WHERE tenant_id=%s",(self.tenant,))
            c.execute("UPDATE canonical_evidence_documents SET manifest_storage_backend='s3' WHERE tenant_id=%s",(self.tenant,))
        try:
            report=thin_canonical_bodies(self.store,tenant_id=self.tenant,batch_size=10,max_batches=1)
            self.assertGreater(report['events'],0)
        finally:
            with self.store.connect() as c:
                c.execute("UPDATE raw_artifacts SET storage_backend='filesystem' WHERE tenant_id=%s",(self.tenant,))
                c.execute("UPDATE canonical_evidence_documents SET manifest_storage_backend='filesystem' WHERE tenant_id=%s",(self.tenant,))
        with self.store.connect() as c:
            row=c.execute("SELECT body_location,canonical_redacted FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s",(self.tenant,self.source,self.message.native_id)).fetchone()
        self.assertEqual(row['body_location'],'raw')
        self.assertNotIn('author_id',row['canonical_redacted']['content'])

    def test_actual_thinning_before_enrollment_still_converges_on_history_replay(self):
        self.setup_history(before_enroll=self.thin_history)
        before=self.immutable();acks=self.ack_rows()
        self.assertEqual(self.real_step()['failed'],0)
        self.assertEqual(self.counts(),(1,1))
        self.assertEqual(self.immutable(),before);self.assertEqual(self.ack_rows(),acks)
        self.project();self.assertEqual(len(self.scoped()),1)

    def test_old_provider_author_cannot_attribute_current_different_author_revision(self):
        from connectors.slack_source import normalize_slack_message
        from privacy.policy import PrivacyPolicy
        def change_author():
            record=normalize_slack_message(workspace_id='T123',channel_id='C123',
                value={**self.rail.root,'user':'U999'})
            decision=PrivacyPolicy(mode='scrub').apply({'content':record.content,
                'provenance':PrivacyPolicy(mode='scrub').apply(record.provenance).value})
            artifact=self.gateway.put_raw(tenant_id=self.tenant,source_id=self.source,
                native_id=record.native_id,payload=ConnectorRunner._raw_payload(record),
                media_type='application/json',created_at=record.occurred_at)
            event=self.last_runner._event(record,decision.value['content'],decision.value['provenance'],artifact)
            self.plane.ingest_batch(tenant_id=self.tenant,principal_id=self.principal,events=[event])
        self.setup_history(before_enroll=change_author)
        before=self.immutable();acks=self.ack_rows()
        with self.store.connect() as c:
            self.assertEqual(c.execute("SELECT max(revision) AS rev FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id=%s",(self.tenant,self.source,self.message.native_id)).fetchone()['rev'],2)
        self.assertEqual(self.real_step()['failed'],0)
        self.assertEqual(self.counts(),(1,0))
        self.assertEqual(self.immutable(),before);self.assertEqual(self.ack_rows(),acks)
        self.project();self.assertEqual(self.scoped(),[])

    def test_current_tombstone_is_not_attributed_from_stale_historical_page(self):
        self.setup_history()
        self.ingest(self.message.native_id, '', tombstone=True)
        before=self.immutable();acks=self.ack_rows()
        self.assertEqual(self.real_step()['failed'],0)
        self.assertEqual(self.counts(),(1,0));self.assertEqual(self.immutable(),before)
        self.assertEqual(self.ack_rows(),acks)
        self.project();self.assertEqual(self.scoped(),[])

    def test_revoked_connection_during_historical_pull_refuses_repair(self):
        self.setup_history();state=self.state()
        def revoke():
            with self.store.connect() as c:c.execute("UPDATE provider_connections SET status='revoked',revoked_at=now() WHERE id=%s",(self.connection_id,))
        self.before_history=revoke
        self.assertEqual(self.real_step()['failed'],1)
        self.assertEqual(self.state(),state);self.assertEqual(self.counts(),(1,0))


if __name__=='__main__':
    suite=unittest.TestSuite(ManagedSlackHistoryActors(name) for name in ManagedSlackHistoryActors.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
