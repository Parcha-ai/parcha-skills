#!/usr/bin/env python3
"""Canonical final-message units survive publication, authority and source opening."""
import hashlib
import time
import unittest
from unittest.mock import patch

import e2e_handback_projection as fixture
import e2e_report_unit_authority as report_fixture
from recall_server.projectors import canonical_json
from recall_server.rerank import RerankRuntime
from tests.central_brain.test_rerank_matched_ranges import RecordingProvider

KIND = 'assistant_final.complete.v1'


class AssistantFinalAuthority(unittest.TestCase):
    setUp = fixture.HandbackPublication.setUp
    project = fixture.HandbackPublication.project
    projected = fixture.HandbackPublication.projected
    grant_and_open = fixture.HandbackPublication.grant_and_open
    hydrate = report_fixture.ReportUnitAuthority.hydrate

    def ingest(self, native, text, phase):
        occurred = '2026-10-15T00:00:00Z'
        value = {'type': 'response_item', 'payload': {'type': 'message',
            'role': 'assistant', 'phase': phase,
            'content': [{'type': 'output_text', 'text': text}]}}
        payload = canonical_json(value)
        artifact = self.gateway.put_raw(tenant_id=self.tenant, source_id=self.source,
            native_id=native, payload=payload, media_type='application/json', created_at=occurred)
        envelope = dict(schema_version=1, source_id=self.source, native_id=native,
            native_parent_id='session-one', kind='transcript_record', occurred_at=occurred,
            observed_at=occurred, principal_id=self.principal, visibility='private',
            content_type='application/json', content=value,
            provenance=dict(connector_id='synthetic.final-message', connector_schema_version=1,
                            artifact_ref=artifact), content_sha256=hashlib.sha256(payload).hexdigest())
        self.plane.ingest_document(tenant_id=self.tenant, principal_id=self.principal,
            connector_id='synthetic.final-message', artifact_ref=artifact,
            envelope=envelope, text_redacted=payload.decode())
        return payload.decode()

    def prepare(self):
        self.ingest('event-before', 'Earlier context ' * 300, 'commentary')
        final = 'Observed finding: café. ' * 130 + '\nDeployment remains untested.'
        canonical = self.ingest('event-final', final, 'final_answer')
        self.ingest('event-after', 'Unrelated later investigation.', 'commentary')
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        rows = self.projected()
        unit = next(r for r in rows if r['text_redacted'] == final)
        self.assertEqual(len(unit['spans']), 1)
        self.assertEqual(unit['spans'][0]['source_unit'], KIND)
        self.assertEqual(len(unit['receipts']), 1)
        opened = self.grant_and_open(unit)
        self.assertIn(canonical, [c['text'] for c in opened['chunks']])
        return final, rows, unit

    def test_final_canonical_marker_overrides_forged_vendor_and_preserves_whole_view(self):
        final, rows, unit = self.prepare()
        for enabled in (False, True):
            self.store.verified_singleton_receipts = enabled
            pool, legs, diagnostics = self.hydrate(rows)
            self.assertEqual(diagnostics['hydrate_status'], 'ok')
            for row in legs[0][2]:
                self.assertEqual(row.get('source_unit'), KIND if row['passage_id'] == unit['passage_id'] else None)
            provider = RecordingProvider()
            runtime = RerankRuntime(protocol='voyage', key_env='FINAL_TEST_KEY', max_doc_chars=2000, transport=provider)
            with patch.dict('os.environ', {'FINAL_TEST_KEY': 'synthetic-only'}):
                _, d = self.retrieval._rerank_fused('Which finding and limitation?', pool, legs,
                    runtime=runtime, deadline_at=time.monotonic()+5, arm_elapsed_ms={})
            self.assertEqual(d['rerank_status'], 'ok')
            docs = provider.calls[0][0]['documents']
            self.assertEqual(sum(final in doc for doc in docs), 1)
            self.assertTrue(all(len(doc) <= 2000 for doc in docs if final not in doc))
            self.assertEqual(runtime.max_doc_chars, 2000)

    def test_changed_vendor_body_and_revoked_receipt_do_not_gain_unit_authority(self):
        _, rows, unit = self.prepare()
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        original = namespace.rows[unit['passage_id']]['text']
        namespace.rows[unit['passage_id']]['text'] = 'forged final answer'
        _, legs, diagnostics = self.hydrate(rows)
        self.assertEqual(diagnostics['hydrate_status'], 'partial')
        self.assertFalse(next(r for r in legs[0][2] if r['passage_id'] == unit['passage_id']).get('text_redacted'))
        namespace.rows[unit['passage_id']]['text'] = original
        with self.store.connect() as connection:
            connection.execute('DELETE FROM canonical_chunks WHERE tenant_id=%s', (self.tenant,))
        for enabled in (False, True):
            self.store.verified_singleton_receipts = enabled
            _, legs, d = self.hydrate(rows)
            self.assertGreater(d['authority_rejected'], 0)
            self.assertTrue(all(not r.get('text_redacted') for r in legs[0][2]))


if __name__ == '__main__':
    unittest.main()
