"""Forgetting a logical parent retains its old month scope until invalidation."""
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector


class Store:
    def __init__(self, documents):
        self.documents = documents
        self.in_transaction = False
        self.invalidated = []
        self.deleted = False

    def connect(self):
        return nullcontext(self)

    @contextmanager
    def transaction(self):
        self.in_transaction = True
        try:
            yield self
        finally:
            self.in_transaction = False

    def execute(self, query, parameters=()):
        rows = []
        if "SELECT DISTINCT COALESCE(native_parent_id,native_id)" in query:
            rows = [{"native_parent_id": "parent:gone"}]
        elif "FOR UPDATE NOWAIT" in query:
            rows = self.documents
        elif "SELECT 1 FROM canonical_documents document" in query:
            pass
        elif "SELECT passage.passage_id" in query:
            pass
        elif "DELETE FROM canonical_evidence_documents" in query:
            assert self.in_transaction
            self.deleted = True
            self.documents = []
        elif "pg_advisory_xact_lock" not in query:
            raise AssertionError("unexpected SQL")
        return SimpleNamespace(fetchall=lambda: rows, fetchone=lambda: rows[0] if rows else None)


class Projector(CanonicalLogicalEvidenceProjector):
    def _old_references(self, connection, candidate):
        return None, []

    def _queue_parquet_scan(self, connection, **values):
        assert connection.in_transaction and not connection.deleted
        connection.invalidated.append(values)
        return 1

    def drain_cleanup(self, **kwargs):
        return {"pending": 0}


class ForgetParquetInvalidationTest(TestCase):
    def forget(self, store):
        projector = Projector(store, None, bound_tenant_id="tenant:test")
        with patch("recall_server.logical_evidence_projection.mark_logical_evidence_dirty"), patch(
            "recall_server.logical_evidence_projection.record_passage_deletions"
        ):
            self.assertEqual(projector.delete_native_ids(
                tenant_id="tenant:test", source_id="source:test", native_ids=["native:gone"]
            ), 0)
        self.assertTrue(store.deleted)

    def test_forget_invalidates_old_document_range_in_same_transaction(self):
        first = datetime(2025, 12, 31, tzinfo=timezone.utc)
        last = datetime(2026, 2, 1, tzinfo=timezone.utc)
        store = Store([dict(logical_document_id="document:gone",
                            first_occurred_at=first, last_occurred_at=last)])
        self.forget(store)
        self.assertEqual(store.invalidated, [dict(
            tenant_id="tenant:test", source_id="source:test",
            logical_document_id="document:gone", ranges=((first, last),), reason="forget",
        )])

    def test_already_absent_parent_does_not_invalidate_unrelated_months(self):
        store = Store([])
        self.forget(store)
        self.assertEqual(store.invalidated, [])
