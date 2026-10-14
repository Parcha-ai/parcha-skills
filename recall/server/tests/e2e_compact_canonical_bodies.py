#!/usr/bin/env python3
"""Atomic ingest retains one SQL body copy with durable raw recovery."""
from __future__ import annotations

import hashlib
import unittest

import e2e_slack_deletion_ordering as fixture
from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical_retrieval import BoundCanonicalRetrieval


class DurableArchiveFixture(FilesystemArchiveStore):
    """Real local IO exposing the production durable-reference contract."""
    storage_backend = 's3'
    encryption = 'sse-s3'


class CompactCanonicalBodies(unittest.TestCase):
    setUpClass = classmethod(fixture.SlackDeletionOrdering.setUpClass.__func__)
    tearDownClass = classmethod(fixture.SlackDeletionOrdering.tearDownClass.__func__)
    ingest = fixture.SlackDeletionOrdering.ingest
    project = fixture.SlackDeletionOrdering.project

    def setUp(self):
        fixture.SlackDeletionOrdering.setUp(self)
        self.archive = DurableArchiveFixture(self.archive.root, namespace_key=b's' * 32)
        self.plane = fixture.CanonicalPlane(self.store, self.archive)

    def scan_records(self):
        from recall_server.parquet_scan import CanonicalParquetScanProjector
        from e2e_parquet_scan_fragments import live_parts, read_dataset
        projection = fixture.LogicalEvidenceProjectionStore(self.archive)
        scan = CanonicalParquetScanProjector(self.store, projection)
        scan.project_pending(tenant_id=self.tenant, batch_size=4, max_batches=4,
                             compaction_budget=0)
        return read_dataset(self.archive, live_parts(self.store,self.tenant,self.source), 'records')

    def test_ingest_retains_chunks_and_raw_without_duplicate_bodies(self):
        for path in ('document', 'batch'):
            with self.subTest(path=path):
                self.source += ':' + path
                prepared = self.ingest(self.root, path='prepare')
                raw_before = self.archive.read_raw(prepared['artifact_ref'])
                result = (self.plane.ingest_document(**prepared) if path == 'document'
                          else self.plane.ingest_batch(tenant_id=self.tenant,
                              principal_id=self.principal, events=[prepared['envelope']]))
                receipt = result['receipt'] if path == 'document' else result['receipts'][0]
                with self.store.connect() as c:
                    row = c.execute('''SELECT d.text_redacted,d.text_sha256,
                        d.body_location,e.body_location AS event_location,e.canonical_redacted
                        FROM canonical_documents d JOIN canonical_events e
                        USING(tenant_id,source_id,event_id)
                        WHERE d.tenant_id=%s AND d.source_id=%s''', (self.tenant,self.source)).fetchone()
                    chunk_body = ''.join(r['text_redacted'] for r in c.execute('''SELECT text_redacted
                        FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s ORDER BY ordinal''',
                        (self.tenant,self.source)).fetchall())
                    self.assertEqual(c.execute('''SELECT count(*) AS n FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s''',(self.tenant,self.source)).fetchone()['n'],0)
                self.assertEqual(row['text_redacted'],'', 'document body duplicates the original chunks')
                self.assertEqual(row['body_location'],'chunks')
                self.assertEqual(row['event_location'],'raw')
                self.assertNotIn('text', row['canonical_redacted']['content'])
                self.assertEqual(chunk_body, prepared['text_redacted'])
                self.assertEqual(row['text_sha256'], hashlib.sha256(chunk_body.encode()).hexdigest())
                self.assertEqual(self.archive.read_raw(prepared['artifact_ref']),raw_before)
                retrieval = BoundCanonicalRetrieval(self.store, tenant_id=self.tenant, principal_id=self.principal,
                    authorized_sources=(self.source,))
                shown = retrieval.show(receipt)
                self.assertEqual(''.join(r['text'] for r in shown['chunks']),chunk_body)
                self.assertIn('Root payload', self.project())
                records = self.scan_records()
                self.assertEqual(len(records),1)
                self.assertIn('Root payload',records[0]['search_text'])

    def test_local_archive_and_legacy_rows_keep_prior_representation(self):
        for path in ('document', 'batch'):
            self.source += ':' + path
            self.archive = FilesystemArchiveStore(self.archive.root, namespace_key=b's' * 32)
            self.plane = fixture.CanonicalPlane(self.store, self.archive)
            prepared = self.ingest(self.root, path='prepare')
            first = self.ingest(self.root, path=path)
            with self.store.connect() as c:
                before = c.execute('''SELECT d.*,e.canonical_redacted,e.body_location AS event_location
                    FROM canonical_documents d JOIN canonical_events e USING(tenant_id,source_id,event_id)
                    WHERE d.tenant_id=%s AND d.source_id=%s''',(self.tenant,self.source)).fetchone()
            self.assertEqual(before['text_redacted'],prepared['text_redacted'])
            self.assertEqual(before['body_location'],'inline')
            self.assertEqual(before['event_location'],'inline')
            # A replay keeps an already-written row unchanged.
            self.assertEqual(self.ingest(self.root,path=path)['inserted'],0)
            with self.store.connect() as c:
                after = c.execute('''SELECT d.*,e.canonical_redacted,e.body_location AS event_location
                    FROM canonical_documents d JOIN canonical_events e USING(tenant_id,source_id,event_id)
                    WHERE d.tenant_id=%s AND d.source_id=%s''',(self.tenant,self.source)).fetchone()
            self.assertEqual(before,after)
            self.assertEqual(first['inserted'],1)

    def test_chunk_failure_rolls_back_event_document_and_queue(self):
        from psycopg import errors
        with self.store.connect() as c:
            c.execute("""CREATE FUNCTION reject_compact_chunk() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'synthetic chunk failure'; END $$""")
            c.execute('''CREATE TRIGGER reject_compact_chunk BEFORE INSERT ON canonical_chunks
                FOR EACH ROW EXECUTE FUNCTION reject_compact_chunk()''')
        try:
            for path in ('document','batch'):
                with self.assertRaises(errors.RaiseException):
                    self.ingest(self.root,path=path)
                with self.store.connect() as c:
                    for table in ('canonical_events','canonical_documents','canonical_chunks',
                                  'canonical_evidence_document_queue'):
                        self.assertEqual(c.execute('SELECT count(*) AS n FROM '+table+' WHERE tenant_id=%s',
                            (self.tenant,)).fetchone()['n'],0)
        finally:
            with self.store.connect() as c:
                c.execute('DROP TRIGGER reject_compact_chunk ON canonical_chunks')
                c.execute('DROP FUNCTION reject_compact_chunk()')

    def test_structural_roles_and_provenance_survive_before_publication(self):
        for path in ('document','batch'):
            self.source += ':' + path
            content={'type':'assistant','message':{'role':'assistant','content':'Exact structural answer α 🧠'}}
            event=fixture.canonical_envelope(source_id=self.source,native_id='structural',
                kind='transcript_record',content=content,principal_id=self.principal,
                visibility='private',occurred_at='2026-10-14T00:00:00Z',parent='structural-session',
                provenance={'connector_id':'claude-code.jsonl','cwd':'/synthetic/project',
                            'branch':'synthetic','byte_start':0,'byte_end':123})
            gateway=fixture.CanonicalArchiveGateway(self.store,self.archive,
                tenant_id=self.tenant,principal_id=self.principal)
            artifact=gateway.put_raw(tenant_id=self.tenant,source_id=self.source,native_id='structural',
                payload=fixture.canonical_json(event),media_type='application/json',
                created_at='2026-10-14T00:00:00Z')
            event['provenance']['artifact_ref']=artifact
            if path=='batch':
                result=self.plane.ingest_batch(tenant_id=self.tenant,principal_id=self.principal,events=[event])
                receipt=result['receipts'][0]
            else:
                result=self.plane.ingest_document(tenant_id=self.tenant,principal_id=self.principal,
                    connector_id='claude-code.jsonl',artifact_ref=artifact,envelope=event,
                    text_redacted=fixture.canonical_json(content).decode())
                receipt=result['receipt']
            view=BoundCanonicalRetrieval(self.store,tenant_id=self.tenant,principal_id=self.principal,
                authorized_sources=(self.source,))
            shown=view.show(receipt)
            metadata=shown['event']['canonical_redacted']
            self.assertEqual(metadata['content']['type'],'assistant')
            self.assertEqual(metadata['content']['message']['role'],'assistant')
            self.assertNotIn('content',metadata['content']['message'])
            self.assertEqual(metadata['provenance']['cwd'],'/synthetic/project')
            self.assertEqual(metadata['provenance']['branch'],'synthetic')
            self.assertEqual(metadata['provenance']['byte_start'],0)
            self.assertIn('Exact structural answer',self.project())

    def test_oversized_raw_pointer_restores_full_source_after_compaction(self):
        import gzip
        from e2e_oversized_records import oversized_envelope, OVERSIZED
        for path in ('document','batch'):
            self.source += ':' + path
            payload=fixture.canonical_json({'type':'assistant','message':{'role':'assistant',
                'content':'Full oversized answer '+('α🧠 ' * 14000)}})
            compressed=gzip.compress(payload,mtime=0)
            gateway=fixture.CanonicalArchiveGateway(self.store,self.archive,
                tenant_id=self.tenant,principal_id=self.principal)
            artifact=gateway.put_raw(tenant_id=self.tenant,source_id=self.source,native_id='oversized:full',
                payload=compressed,media_type=OVERSIZED,created_at='2026-10-14T00:00:00Z')
            event=oversized_envelope(source_id=self.source,principal_id=self.principal,parent='oversized-session',
                native_id='oversized',occurred_at='2026-10-14T00:00:00Z',artifact=artifact,
                full_payload=payload,compressed=compressed,byte_start=0)
            if path=='batch':
                self.plane.ingest_batch(tenant_id=self.tenant,principal_id=self.principal,events=[event])
            else:
                self.plane.ingest_document(tenant_id=self.tenant,principal_id=self.principal,
                    connector_id='codex.jsonl',artifact_ref=artifact,envelope=event,
                    text_redacted=fixture.canonical_json(event['content']).decode())
            with self.store.connect() as c:
                row=c.execute('SELECT canonical_redacted FROM canonical_events WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant,self.source)).fetchone()['canonical_redacted']['content']
            self.assertNotIn('head',row)
            self.assertNotIn('tail',row)
            self.assertEqual(row['full_content_sha256'],hashlib.sha256(payload).hexdigest())
            self.assertEqual(row['full_size_bytes'],len(payload))
            self.assertEqual(row['archive_encoding'],'gzip')
            self.project()
            records = self.scan_records()
            import json
            self.assertEqual(len(records),1)
            restored = json.loads(records[0]['record_json'])['content']
            self.assertEqual(fixture.canonical_json(restored),payload)


    def test_original_actor_inputs_reach_event_and_passage_before_compaction(self):
        from recall_server.actor_attribution import ActorIdentityIndex
        index=ActorIdentityIndex(lambda subject, *, purpose: hashlib.sha256((purpose+subject).encode()).hexdigest())
        actor='actor_'+'1'*32
        for path in ('document','batch'):
            self.source += ':' + path
            prepared=self.ingest(self.root,path='prepare')
            connector,namespace,digest=index.lookup(self.tenant,'slack.messages','author_id','slack:T123:U111')
            with self.store.connect() as c:
                c.execute("INSERT INTO brain_actors(tenant_id,actor_id,actor_kind,display_name) VALUES (%s,%s,'human','Synthetic author') ON CONFLICT DO NOTHING",(self.tenant,actor))
                c.execute('''INSERT INTO brain_actor_external_identities(tenant_id,actor_id,connector_id,namespace,subject_hmac_sha256)
                    VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',(self.tenant,actor,connector,namespace,digest))
            self.plane=fixture.CanonicalPlane(self.store,self.archive,actor_identity_index=index)
            self.ingest(self.root,path=path)
            with self.store.connect() as c:
                event_links=c.execute('SELECT actor_id,relation FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s ORDER BY relation',
                    (self.tenant,self.source)).fetchall()
                self.assertEqual(event_links,[{'actor_id':actor,'relation':'author'},{'actor_id':actor,'relation':'participant'}])
                self.assertEqual(c.execute('SELECT permission FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',
                    (self.tenant,self.source,self.principal)).fetchone()['permission'],'owner')
            self.project()
            with self.store.connect() as c:
                passage_links=c.execute('SELECT DISTINCT actor_id,relation FROM canonical_passage_actors WHERE tenant_id=%s AND source_id=%s ORDER BY relation',
                    (self.tenant,self.source)).fetchall()
            self.assertEqual(passage_links,event_links)
            self.assertIn('author_id',prepared['envelope']['content'])

    def test_conflicting_existing_artifact_ref_is_refused_before_any_new_body(self):
        import copy
        for path in ('document','batch'):
            self.source += ':' + path
            prepared=self.ingest(self.root,path='prepare')
            self.ingest(self.root,path=path)
            wrong=copy.deepcopy(prepared)
            wrong['artifact_ref']['version_id']='synthetic-conflicting-version'
            wrong['envelope']['provenance']['artifact_ref']=wrong['artifact_ref']
            with self.assertRaises(fixture.CanonicalLifecycleError) as error:
                if path=='document':
                    self.plane.ingest_document(**wrong)
                else:
                    self.plane.ingest_batch(tenant_id=self.tenant,principal_id=self.principal,
                        events=[wrong['envelope']])
            self.assertEqual(error.exception.error_code,'canonical_artifact_conflict')
            with self.store.connect() as c:
                self.assertEqual(c.execute('SELECT count(*) AS n FROM canonical_events WHERE tenant_id=%s AND source_id=%s',
                    (self.tenant,self.source)).fetchone()['n'],1)


    def test_capture_identity_metadata_and_owner_forget_survive_compaction(self):
        previous=self.store.legacy_ingest_bridge
        self.store.legacy_ingest_bridge=fixture.LegacyIngestBridge(self.store,self.plane,self.archive,
            environment={'RECALL_LEGACY_WRITES':'0'})
        self.addCleanup(setattr,self.store,'legacy_ingest_bridge',previous)
        principal={'tenant_id':self.tenant,'principal_id':self.principal,
                   'source_id':self.source,'capture_origin':'grep-agent'}
        captured=self.store.capture(principal,{'schema_version':1,'title':'Synthetic capture',
            'body':'Keep this exact memory','occurred_at':'2026-10-14T00:00:00Z',
            'provenance':{'uri':'manual://synthetic'},'tags':['synthetic']})
        resolved=self.store.resolve(captured['receipt'],tenant_id=self.tenant,
            authorized_sources=(self.source,))
        self.assertEqual(resolved['event']['principal_id'],self.principal)
        self.assertEqual(resolved['event']['provenance']['uri'],'manual://synthetic')
        with self.assertRaises(ValueError):
            self.store.forget_capture({**principal,'principal_id':'principal:intruder'},captured['receipt'])
        forgotten=self.store.forget_capture(principal,captured['receipt'])
        self.assertEqual(forgotten['status'],'committed')
        self.assertIsNone(self.store.resolve(captured['receipt'],tenant_id=self.tenant,
            authorized_sources=(self.source,)))


    def test_durable_compact_rows_keep_historical_revisions_after_retirement(self):
        from unittest.mock import patch
        import e2e_revision_history
        # Retain the existing full retirement/revision/forget owner exercise,
        # with durable references so both new atomic write paths are exercised.
        with patch.object(e2e_revision_history, 'FilesystemArchiveStore', DurableArchiveFixture):
            e2e_revision_history.main()


if __name__ == '__main__':
    unittest.main()
