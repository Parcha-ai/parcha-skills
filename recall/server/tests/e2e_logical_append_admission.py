#!/usr/bin/env python3
"""Append publication can become ready without waiting for an active session to stop."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parents[2])]
import e2e_logical_append_capability as fixtures  # noqa: E402


class AppendAdmission(unittest.TestCase):
    setUp = fixtures.AppendCapability.setUp
    event = fixtures.AppendCapability.event
    ingest = fixtures.AppendCapability.ingest
    publish = fixtures.AppendCapability.publish

    def active_append(self):
        self.ingest('old', 1)
        self.publish()
        self.ingest('new', 2)
        self.logical.append_enabled = True
        with self.store.connect() as c:
            c.execute("""UPDATE canonical_evidence_document_queue
                SET first_queued_at=clock_timestamp()-interval '100 seconds'
                WHERE tenant_id=%s AND source_id=%s""", (self.tenant, self.source))

    def scheduled_publish(self):
        return self.logical.project_pending(tenant_id=self.tenant,
            batch_size=2, max_batches=1, upload_concurrency=1,
            quiet_seconds=90, max_wait_seconds=600)

    def test_active_append_publishes_after_one_quiet_window_with_only_suffix_input(self):
        self.active_append()
        rows = []
        records = self.logical._record_stream
        def capture(inputs, **kwargs):
            def observed():
                for row in inputs:
                    rows.append(row['native_id'])
                    yield row
            return records(observed(), **kwargs)
        with patch.object(self.logical, '_record_stream', capture):
            report = self.scheduled_publish()
        self.assertEqual(report['documents'], 1, 'eligible append waited for the full-rebuild max wait')
        self.assertEqual(rows, ['new'])
        self.assertEqual(report['receipts'], 2)

    def test_disabled_reader_retains_full_rebuild_wait(self):
        self.active_append()
        self.logical.append_enabled = False
        self.assertEqual(self.scheduled_publish()['documents'], 0)
        with self.store.connect() as c:
            c.execute("UPDATE canonical_evidence_document_queue SET first_queued_at=clock_timestamp()-interval '601 seconds' WHERE tenant_id=%s", (self.tenant,))
        self.assertEqual(self.scheduled_publish()['documents'], 1)

    def test_young_append_still_coalesces(self):
        self.active_append()
        with self.store.connect() as c:
            c.execute('UPDATE canonical_evidence_document_queue SET first_queued_at=clock_timestamp() WHERE tenant_id=%s', (self.tenant,))
        self.assertEqual(self.scheduled_publish()['documents'], 0)

    def test_custom_retention_retains_full_rebuild_wait(self):
        self.active_append()
        self.logical.excluded_structural_types = ('custom-only',)
        self.assertEqual(self.scheduled_publish()['documents'], 0)

    def test_poisoned_or_unbound_metadata_does_not_get_append_admission(self):
        self.active_append()
        with self.store.connect() as c:
            base = c.execute('SELECT append_source_ordinal,append_manifest_id FROM canonical_evidence_documents WHERE tenant_id=%s', (self.tenant,)).fetchone()
            queued = c.execute('SELECT append_generation,append_base_manifest_id FROM canonical_evidence_document_queue WHERE tenant_id=%s', (self.tenant,)).fetchone()
        for target, field, value in (
            ('canonical_evidence_document_queue', 'append_generation', None),
            ('canonical_evidence_document_queue', 'append_base_manifest_id', 'wrong-manifest'),
            ('canonical_evidence_documents', 'append_source_ordinal', None),
            ('canonical_evidence_documents', 'append_manifest_id', 'wrong-manifest'),
        ):
            with self.subTest(field=field):
                with self.store.connect() as c:
                    c.execute('UPDATE canonical_evidence_documents SET append_source_ordinal=%s,append_manifest_id=%s WHERE tenant_id=%s', (base['append_source_ordinal'], base['append_manifest_id'], self.tenant))
                    c.execute('UPDATE canonical_evidence_document_queue SET append_generation=%s,append_base_manifest_id=%s WHERE tenant_id=%s', (queued['append_generation'], queued['append_base_manifest_id'], self.tenant))
                    c.execute(f'UPDATE {target} SET {field}=%s WHERE tenant_id=%s', (value, self.tenant))
                self.assertEqual(self.scheduled_publish()['documents'], 0)

    def test_append_admission_preserves_backoff_and_quarantine(self):
        self.active_append()
        for assignments in ("next_attempt_at=clock_timestamp()+interval '1 hour'", "next_attempt_at=NULL,attempts=8"):
            with self.store.connect() as c:
                c.execute(f"UPDATE canonical_evidence_document_queue SET {assignments} WHERE tenant_id=%s", (self.tenant,))
            self.assertEqual(self.scheduled_publish()['documents'], 0)

    def test_forget_and_backfill_still_bypass_wait_without_append_bindings(self):
        self.active_append()
        for reason in ('forget', 'backfill'):
            with self.store.connect() as c:
                c.execute("""UPDATE canonical_evidence_document_queue
                    SET reason=%s,first_queued_at=clock_timestamp(),append_generation=NULL
                    WHERE tenant_id=%s""", (reason, self.tenant))
            candidates = self.logical._pending(tenant_id=self.tenant, limit=2,
                quiet_seconds=90, max_wait_seconds=600)
            self.assertEqual(len(candidates), 1)

    def test_schema_absent_does_not_reference_optional_columns(self):
        self.active_append()
        with self.store.connect() as c:
            c.execute('DELETE FROM schema_migrations WHERE version=79')
            c.execute('ALTER TABLE canonical_evidence_document_queue DROP COLUMN append_generation, DROP COLUMN append_base_manifest_id')
            c.execute('ALTER TABLE canonical_evidence_documents DROP COLUMN append_source_ordinal, DROP COLUMN append_manifest_id')
        try:
            self.assertEqual(self.scheduled_publish()['documents'], 0)
        finally:
            with self.store.connect() as c:
                c.execute((Path(__file__).resolve().parents[1] / 'schema/079_logical_append_capability.sql').read_text())

    def test_new_generation_after_admission_keeps_existing_stale_fence(self):
        self.active_append()
        prepare = self.logical._prepare_batch_and_upload
        def changed(*args, **kwargs):
            self.ingest('racing', 3)
            return prepare(*args, **kwargs)
        with patch.object(self.logical, '_prepare_batch_and_upload', changed):
            report = self.scheduled_publish()
        self.assertEqual(report['documents'], 0)
        self.assertEqual(report['source_races'], 1)
        self.assertEqual(self.scheduled_publish()['documents'], 1)


if __name__ == '__main__':
    unittest.main()
