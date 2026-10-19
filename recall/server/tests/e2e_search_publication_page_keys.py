#!/usr/bin/env python3
"""The page limit precedes wide catalog hydration, without changing eligibility."""
import json
import time
import unittest
from datetime import datetime,timezone
from unittest.mock import patch
from e2e_search_publication_certificates import PublicationCertificates
from recall_server import turbopuffer_projection as publication


# Original455 query retained as a differential output oracle.
BASELINE_SQL = "\n    SELECT passage.passage_id,passage.source_id,passage.logical_document_id,\n           passage.policy_fingerprint,passage.ordinal,\n           passage.first_occurred_at,passage.last_occurred_at,\n           passage.roles,passage.receipts,passage.spans,\n           passage.text_redacted,passage.text_sha256,passage.header_redacted,\n           evidence.native_parent_id,evidence.revision,\n           evidence.manifest_object_key,evidence.manifest_content_sha256,\n           evidence.first_occurred_at AS doc_first_occurred_at,\n           evidence.last_occurred_at AS doc_last_occurred_at,\n           COALESCE(\n               (SELECT jsonb_agg(\n                           jsonb_build_array(actor.relation,actor.actor_id)\n                           ORDER BY actor.relation,actor.actor_id\n                       )\n                  FROM canonical_passage_actors actor\n                 WHERE actor.tenant_id=passage.tenant_id\n                   AND actor.source_id=passage.source_id\n                   AND actor.passage_id=passage.passage_id),\n               '[]'::jsonb\n           ) AS actors\n      FROM canonical_passages passage\n      JOIN canonical_passage_documents projected\n        USING(tenant_id,source_id,logical_document_id,revision,policy_fingerprint)\n      JOIN canonical_evidence_documents evidence\n        USING(tenant_id,source_id,logical_document_id)\n     WHERE passage.tenant_id=%s\n       AND passage.source_id=%s\n       AND passage.first_occurred_at>=%s\n       AND passage.first_occurred_at<%s\n       AND (%s::timestamptz IS NULL OR passage.created_at>=%s::timestamptz)\n       AND (passage.first_occurred_at,passage.passage_id)>(%s,%s)\n       AND \n    NOT EXISTS (\n        SELECT 1\n          FROM unnest(passage.receipts) AS passage_receipt(receipt)\n          LEFT JOIN canonical_chunks live_chunk\n            ON live_chunk.tenant_id=passage.tenant_id\n           AND live_chunk.source_id=passage.source_id\n           AND live_chunk.receipt=passage_receipt.receipt\n           AND live_chunk.deleted_at IS NULL\n         WHERE live_chunk.receipt IS NULL\n    )\n\n     ORDER BY passage.first_occurred_at,passage.passage_id\n     LIMIT %s\n"


def nodes(plan):
    yield plan
    for child in plan.get('Plans',[]):yield from nodes(child)


class PublicationPageKeys(PublicationCertificates):
    def tied_fixture(self):
        with self.store.connect()as c:
            c.execute('''INSERT INTO canonical_passages(
                tenant_id,source_id,logical_document_id,revision,passage_id,ordinal,policy_fingerprint,
                target_tokens,overlap_tokens,token_count,first_occurred_at,last_occurred_at,
                roles,receipts,spans,text_redacted,text_sha256,created_at)
                SELECT p.tenant_id,p.source_id,p.logical_document_id,p.revision,
                'psg_'||md5('page-key-fixture-'||n::text),10000+n,p.policy_fingerprint,
                p.target_tokens,p.overlap_tokens,p.token_count,p.first_occurred_at,p.last_occurred_at,
                p.roles,p.receipts,p.spans,p.text_redacted,p.text_sha256,
                CASE WHEN mod(n,7)=0 THEN '2026-01-01'::timestamptz ELSE '2026-10-19'::timestamptz END
                FROM canonical_passages p CROSS JOIN generate_series(1,10000)n
                WHERE p.tenant_id=%s AND p.passage_id=%s''',(self.tenant,self.ids[0]))
            c.execute('ANALYZE canonical_passages');c.execute('ANALYZE canonical_evidence_documents');c.execute('ANALYZE canonical_passage_documents')
        owner=self.publisher();owner.page_rows=128
        with self.store.connect()as c:
            first=c.execute('SELECT min(first_occurred_at)AS t FROM canonical_passages WHERE tenant_id=%s',(self.tenant,)).fetchone()['t']
        claim=dict(tenant_id=self.tenant,source_id=self.source,month=first.date().replace(day=1))
        return owner,claim

    def test_tied_time_limit_bounds_full_row_hydration(self):
        owner,claim=self.tied_fixture();original=BASELINE_SQL
        candidate=publication._PASSAGE_PAGE_SQL
        captured=[];execute=self.store._execute_bounded
        def record(c,sql,args,deadline):
            captured.append((sql,args));return execute(c,sql,args,deadline)
        with patch.object(self.store,'verified_singleton_receipts',True),self.store.connect()as c,c.transaction():
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            cases=[]
            for sql in (original,candidate):
                with patch.object(publication,'_PASSAGE_PAGE_SQL',sql),patch.object(self.store,'_execute_bounded',side_effect=record):
                    rows=owner.passage_page(c,claim,since=datetime(2026,10,1,tzinfo=timezone.utc),after=None,deadline_at=time.monotonic()+5)
                statement,args=captured[-1]
                plan=execute(c,'EXPLAIN (ANALYZE,BUFFERS,VERBOSE,FORMAT JSON) '+statement,args,time.monotonic()+5).fetchone()['QUERY PLAN'][0]
                def outer_nodes(n):
                    # The CTE carries only selected keys. Count later full-row hydration.
                    if n.get('Subplan Name')=='CTE eligible_page':return
                    yield n
                    for child in n.get('Plans',[]):yield from outer_nodes(child)
                wide=[n for n in outer_nodes(plan['Plan'])if n.get('Relation Name')=='canonical_passages']
                work=sum(n['Actual Rows']*n['Actual Loops']for n in wide)
                cases.append(dict(rows=rows,work=work,ms=plan['Execution Time'],plan=plan))
            self.assertEqual(cases[0]['rows'],cases[1]['rows']);self.assertEqual(len(cases[0]['rows']),128)
            print(json.dumps(dict(original_ms=cases[0]['ms'],candidate_ms=cases[1]['ms'],original_wide_rows=cases[0]['work'],candidate_wide_rows=cases[1]['work'],full_rows_equal=True),sort_keys=True))
            self.assertLessEqual(cases[1]['work'],128,'candidate hydrates wide rows beyond the examined page')
            self.assertGreater(cases[0]['work'],128,'fixture must exercise the original pre-limit hydration')

    def test_current_revision_watermark_keyset_and_native_authority_equal(self):
        owner,claim=self.tied_fixture()
        with self.store.connect()as c:
            c.execute('UPDATE canonical_passages SET revision=revision+1 WHERE tenant_id=%s AND mod(ordinal,11)=0',(self.tenant,))
        for verified in (True,False):
            for since in (None,datetime(2026,10,1,tzinfo=timezone.utc)):
                with self.subTest(verified=verified,since=since),patch.object(self.store,'verified_singleton_receipts',verified),self.store.connect()as c,c.transaction():
                    c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                    after=None;seen=set()
                    for _ in range(2):
                        with patch.object(publication,'_PASSAGE_PAGE_SQL',BASELINE_SQL):
                            expected=owner.passage_page(c,claim,since=since,after=after,deadline_at=time.monotonic()+5)
                        actual=owner.passage_page(c,claim,since=since,after=after,deadline_at=time.monotonic()+5)
                        self.assertEqual(actual,expected)
                        self.assertFalse(seen.intersection(r['passage_id']for r in actual))
                        seen.update(r['passage_id']for r in actual)
                        self.assertTrue(all(r['ordinal']%11!=0 for r in actual))
                        if verified:
                            expected_allowed=owner._authorized_publication_rows(c,self.tenant,expected,deadline_at=time.monotonic()+5)
                            actual_allowed=owner._authorized_publication_rows(c,self.tenant,actual,deadline_at=time.monotonic()+5)
                            self.assertEqual(actual_allowed,expected_allowed)
                        if not actual:break
                        after=(actual[-1]['first_occurred_at'],actual[-1]['passage_id'])

if __name__=='__main__':
    suite=unittest.TestSuite(PublicationPageKeys(n)for n in PublicationPageKeys.__dict__ if n.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
