#!/usr/bin/env python3
"""Time clipping keeps verified source text across metadata retirement."""
import unittest
from unittest.mock import patch
from e2e_singleton_passage_reads import SingletonPassageReads


class SingletonTimeClip(SingletonPassageReads):
    def clip(self, receipts=None, *, since='2026-10-14T00:00:00Z'):
        response=dict(results=[dict(logical_document_id=self.args['logical_document_id'],
            matching_ranges=[dict(text='Unverified broad hint',receipts=receipts or [self.receipt],
                passage_window=['2026-10-13T00:00:00Z','2026-10-15T00:00:00Z'])])])
        return self.bound._clip_passage_hints_to_time_window(response,sources=[self.source],since=since,until='2026-10-14T23:59:59Z')

    def test_time_clip_returns_same_exact_text_and_receipt_after_admission(self):
        self.prepare_passages()
        baseline=self.clip()
        self.assertEqual(baseline['results'][0]['matching_ranges'][0]['receipts'],[self.receipt])
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            actual=self.clip()
        self.assertEqual(actual['results'],baseline['results'])
        self.assertEqual(actual['diagnostics']['time_clip_status'],'ok')

    def test_time_clip_mixed_population_keeps_remaining_live_receipt(self):
        self.prepare_passages(mixed=True)
        self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            self.ingest('turn/physical','',tombstone=True)
            actual=self.clip([self.receipt,self.physical_receipt])
        self.assertEqual(actual['diagnostics']['time_clip_status'],'ok')
        self.assertEqual(actual['results'][0]['matching_ranges'][0]['receipts'],[self.receipt])

    def test_time_clip_missing_archive_keeps_pointer_without_unverified_text(self):
        self.prepare_passages();self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True),patch.object(self.bound,'chunk_body_archive',None):
            actual=self.clip()
        self.assertEqual(actual['diagnostics']['time_clip_status'],'evidence-unavailable')
        self.assertEqual(actual['results'][0]['matching_ranges'],[])

    def test_time_clip_enabled_retained_inline_body_needs_no_archive(self):
        self.prepare_passages();baseline=self.clip()
        with patch.object(self.store,'verified_singleton_receipts',True),patch.object(self.bound,'chunk_body_archive',None):
            actual=self.clip()
        self.assertEqual(actual['results'],baseline['results'])

    def test_time_clip_deletion_during_archive_read_never_returns_body(self):
        self.prepare_passages();self.admit_fixture(self.baseline(self.receipt)['document_id'])
        read=self.archive.read_raw;deleted=[]
        def delete_during_read(reference,*args,**kwargs):
            data=read(reference,*args,**kwargs)
            if not deleted:
                deleted.append(True)
                self.ingest('turn/passage','',tombstone=True)
            return data
        with patch.object(self.store,'verified_singleton_receipts',True),patch.object(self.archive,'read_raw',delete_during_read):
            actual=self.clip()
        self.assertTrue(deleted)
        self.assertEqual(actual['diagnostics']['time_clip_status'],'evidence-unavailable')
        self.assertEqual(actual['results'][0]['matching_ranges'],[])

    def test_time_clip_time_scope_still_excludes_outside_event(self):
        self.prepare_passages();self.admit_fixture(self.baseline(self.receipt)['document_id'])
        with patch.object(self.store,'verified_singleton_receipts',True):
            actual=self.clip(since='2026-10-14T01:00:00Z')
        self.assertEqual(actual['diagnostics']['time_clip_status'],'ok')
        self.assertEqual(actual['results'],[])


if __name__=='__main__':
    suite=unittest.TestSuite(SingletonTimeClip(name) for name in SingletonTimeClip.__dict__ if name.startswith('test_time_clip_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
