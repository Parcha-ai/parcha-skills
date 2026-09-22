"""Committed hints cannot displace historical work or bypass its authority."""

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from recall_server.canonical_thinning import CanonicalBodyThinner
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector,
    LogicalGroupCandidate,
    LogicalEvidenceError,
    _close_body_locators,
)
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.archive import FilesystemArchiveStore


class Store:
    def __init__(self, history=(), eligible=(), fail=False):
        self.history = list(history)
        self.eligible = set(eligible)
        self.fail = fail
        self.calls = []
        self.mutations = []

    @contextmanager
    def connect(self):
        yield self
        if self.fail:
            raise RuntimeError("unknown commit")

    def execute(self, q, p):
        self.calls.append((q, p))
        if q.startswith("SET LOCAL statement_timeout"):
            return SimpleNamespace()
        if "ORDER BY source_id DESC" in q:
            return SimpleNamespace(
                fetchone=lambda: dict(source_id="s", document_id="z")
            )
        if q.lstrip().startswith("SELECT source_id,document_id"):
            return SimpleNamespace(
                fetchall=lambda: [
                    dict(source_id=s, document_id=d) for s, d in self.history
                ]
            )
        if "updated_documents AS" not in q:
            keys = list(zip(p[0], p[1]))
            rows = [
                dict(source_id=s, document_id=d)
                for s, d in keys
                if (s, d) in self.eligible
            ][: p[-1]]
            return SimpleNamespace(fetchall=lambda: rows)
        keys = list(zip(p[1], p[2]))
        self.mutations.append(keys)
        n = len(keys)
        return SimpleNamespace(
            fetchone=lambda: dict(
                candidates=n, documents=n, events=n, document_bytes=n, event_bytes=n
            )
        )


class SelectionTests(unittest.TestCase):
    def test_history_first_hints_fill_one_bounded_mutation(self):
        s = Store([("s", "a"), ("s", "b")], [("s", "a"), ("s", "x"), ("s", "y")])
        t = CanonicalBodyThinner(s, tenant_id="t")
        r = t.thin(
            batch_size=2,
            committed_keys=(("t", "s", "x"), ("t", "s", "x"), ("t", "s", "y")),
        )
        self.assertEqual(r["documents"], 2)
        self.assertEqual(s.mutations, [[("s", "a"), ("s", "x")]])
        self.assertEqual(t._after, ("s", "b"))
        self.assertEqual(r["scanned_keys"], 2)

    def test_full_history_does_not_spend_or_displace_hints(self):
        s = Store([("s", "a")], [("s", "a"), ("s", "x")])
        t = CanonicalBodyThinner(s, tenant_id="t")
        t.thin(batch_size=1, committed_keys=(("t", "s", "x"),))
        self.assertEqual(s.mutations, [[("s", "a")]])
        self.assertEqual(len(s.calls), 5)
        s.history = [("s", "b")]
        t.thin(batch_size=1)
        self.assertEqual(s.mutations[-1], [("s", "x")])

    def test_worker_statement_budget_precedes_data_queries(self):
        store = Store([], [])
        CanonicalBodyThinner(store, tenant_id="t").thin(batch_size=1)
        self.assertEqual(store.calls[0], ("SET LOCAL statement_timeout='2s'", ()))

    def test_ready_hint_suffix_drains_without_new_ingest(self):
        keys = [("s", str(i).zfill(3)) for i in range(25)]
        store = Store([("s", "prefix")], keys)
        thinner = CanonicalBodyThinner(store, tenant_id="t")
        reports = [
            thinner.thin(batch_size=10, committed_keys=tuple(("t", *k) for k in keys))
        ]
        reports.extend(thinner.thin(batch_size=10) for _ in range(2))
        self.assertEqual([r["documents"] for r in reports], [10, 10, 5])
        self.assertEqual([k for batch in store.mutations for k in batch], keys)
        self.assertEqual(thinner._committed_keys, {})

    def test_unknown_ack_retains_hints_and_history_position(self):
        s = Store([("s", "a")], [("s", "x")], True)
        t = CanonicalBodyThinner(s, tenant_id="t")
        with self.assertRaisesRegex(RuntimeError, "unknown commit"):
            t.thin(batch_size=1, committed_keys=(("t", "s", "x"),))
        self.assertIsNone(t._after)
        self.assertIsNone(t._through)
        s.fail = False
        t.thin(batch_size=1)
        self.assertEqual(s.mutations[-1], [("s", "x")])

    def test_wrong_tenant_and_source_pairs_never_expand(self):
        s = Store([("s", "a")], [("other", "same"), ("s", "same")])
        t = CanonicalBodyThinner(s, tenant_id="t")
        t.thin(
            batch_size=10,
            committed_keys=(("wrong", "s", "same"), ("t", "other", "same")),
        )
        self.assertEqual(s.mutations, [[("other", "same")]])

    def test_overflow_bounded_and_history_still_runs(self):
        s = Store([("s", "a")], [("s", "a")])
        t = CanonicalBodyThinner(s, tenant_id="t")
        for k in range(8):
            t.thin(
                batch_size=1,
                committed_keys=tuple(("t", "s", str(k * 100 + i)) for i in range(100)),
            )
        self.assertLessEqual(len(t._committed_keys), 256)
        self.assertEqual(len(s.mutations), 8)
        self.assertNotIn(("s", "0"), t._committed_keys)


class InputStore:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, q, p):
        self.queries.append(q)
        return SimpleNamespace(fetchall=lambda: [])

    def cursor(self, **kwargs):
        return self

    def __iter__(self):
        return iter(self.rows)


def source_rows(count):
    for i in range(count):
        text = "body " + str(i)
        digest = hashlib.sha256(text.encode()).hexdigest()
        yield dict(
            candidate_ordinal=0,
            tenant_id="tenant",
            source_id="source",
            document_id="doc_" + str(i).zfill(32),
            body_location="inline",
            event_text=text,
            document_revision=1,
            document_text_sha256=digest,
            source_chunks=[dict(ordinal=0, size_bytes=len(text), text_sha256=digest)],
            chunk_count=1,
            chunk_receipts=[f"recall://source/native{i}?rev=1#item=0"],
            fallback_type_values=[],
            fallback_role_values=["assistant"],
            raw_media_type="application/json",
            native_id="native" + str(i),
            kind="transcript_record",
            occurred_at="2026-09-22T00:00:00Z",
        )


class StreamTests(unittest.TestCase):
    def prepare(self, rows, root):
        store = InputStore(rows)
        projector = CanonicalLogicalEvidenceProjector(
            store,
            LogicalEvidenceProjectionStore(
                FilesystemArchiveStore(
                    root, namespace_key=b"committed-keys-local-fixture-key"
                )
            ),
            bound_tenant_id="tenant",
        )
        candidate = LogicalGroupCandidate(
            "tenant",
            "source",
            "parent",
            datetime(2026, 9, 22, tzinfo=timezone.utc),
            1,
            1,
        )
        return projector, store, candidate

    def test_cap_does_not_cut_stream_and_archive_bytes_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            p, s, c = self.prepare(list(source_rows(100)), Path(tmp) / "one")
            uploads = p._prepare_batch_and_upload((c,))
            u = uploads[0]
            try:
                self.assertEqual(u.prepared.record_count, 100)
                self.assertEqual(len(u.inline_document_ids), 32)
                self.assertEqual(p._take_committed_body_keys(), ())
                rows = list(source_rows(100))
                for r in rows:
                    r["body_location"] = "chunks"
                q, _, d = self.prepare(rows, Path(tmp) / "two")
                v = q._prepare_batch_and_upload((d,))[0]
                try:
                    self.assertEqual(v.inline_document_ids, ())
                    self.assertEqual(
                        [r["content_sha256"] for r in u.all_references],
                        [r["content_sha256"] for r in v.all_references],
                    )
                    self.assertEqual(list(u.body_locators), list(v.body_locators))
                finally:
                    _close_body_locators(v)
                self.assertIn("document.body_location", s.queries[1])
            finally:
                _close_body_locators(u)

    def test_late_invalid_after_cap_still_refuses_before_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = list(source_rows(100))
            rows[-1]["document_revision"] = 0
            p, _, c = self.prepare(rows, Path(tmp))
            with (
                patch.object(
                    p.projection,
                    "put_records",
                    side_effect=AssertionError("upload started"),
                ),
                patch.object(p, "drain_cleanup", return_value={}),
                self.assertRaises(LogicalEvidenceError),
            ):
                p._prepare_batch_and_upload((c,))
            self.assertEqual(p._take_committed_body_keys(), ())


class PublicationTests(unittest.TestCase):
    def test_only_committed_status_publishes_source_keys(self):
        from tests.central_brain.test_logical_cleanup_concurrency import (
            Store as CountsStore,
        )

        for status in (
            "committed",
            "adopted",
            "repaired",
            "pruned",
            "stale",
            "unknown",
        ):
            with self.subTest(status=status):
                p = CanonicalLogicalEvidenceProjector(
                    CountsStore(), object(), bound_tenant_id="tenant"
                )
                c = LogicalGroupCandidate(
                    "tenant", "source", "parent", datetime.now(timezone.utc), 1, 1
                )
                u = SimpleNamespace(
                    prepared=SimpleNamespace(record_count=1, receipt_count=1),
                    all_references=(),
                    inline_document_ids=("document",),
                )
                cleanup = dict(deleted=0, failures=0, completed=0, pending=0)
                with (
                    patch.object(p, "_pending", return_value=[c]),
                    patch.object(p, "_prepare_batch_and_upload", return_value=[u]),
                    patch.object(p, "drain_cleanup", return_value=cleanup),
                    patch.object(
                        p,
                        "_commit_upload",
                        side_effect=RuntimeError("unknown commit")
                        if status == "unknown"
                        else None,
                        return_value=status,
                    ),
                ):
                    if status == "unknown":
                        with self.assertRaisesRegex(RuntimeError, "unknown commit"):
                            p.project_pending(batch_size=1, max_batches=1)
                    else:
                        p.project_pending(batch_size=1, max_batches=1)
                self.assertEqual(
                    p._take_committed_body_keys(),
                    (("tenant", "source", "document"),)
                    if status == "committed"
                    else (),
                )
                self.assertEqual(p._take_committed_body_keys(), ())

    def test_long_source_identity_keeps_projection_but_omits_hint(self):
        from tests.central_brain.test_logical_cleanup_concurrency import (
            Store as CountsStore,
        )

        p = CanonicalLogicalEvidenceProjector(
            CountsStore(), object(), bound_tenant_id="tenant"
        )
        candidate = LogicalGroupCandidate(
            "tenant", "s" * 256, "parent", datetime.now(timezone.utc), 1, 1
        )
        upload = SimpleNamespace(
            prepared=SimpleNamespace(record_count=1, receipt_count=1),
            all_references=(),
            inline_document_ids=("document",),
        )
        with (
            patch.object(p, "_pending", return_value=[candidate]),
            patch.object(p, "_prepare_batch_and_upload", return_value=[upload]),
            patch.object(p, "_commit_upload", return_value="committed"),
            patch.object(
                p,
                "drain_cleanup",
                return_value=dict(deleted=0, failures=0, completed=0, pending=0),
            ),
        ):
            self.assertEqual(
                p.project_pending(batch_size=1, max_batches=1)["documents"], 1
            )
        self.assertEqual(p._take_committed_body_keys(), ())

    def test_large_batch_allocates_finite_hint_budget_including_failed_shard_retry(
        self,
    ):
        from tests.central_brain.test_logical_cleanup_concurrency import (
            Store as CountsStore,
        )

        p = CanonicalLogicalEvidenceProjector(
            CountsStore(), object(), bound_tenant_id="tenant"
        )
        rows = [
            LogicalGroupCandidate(
                "tenant", "source", str(i), datetime.now(timezone.utc), 1, 1
            )
            for i in range(260)
        ]
        budgets = []

        def prepare(candidates, *, hint_limit):
            budgets.append((len(candidates), hint_limit))
            if len(candidates) > 1:
                raise RuntimeError("synthetic shard error")
            return [
                SimpleNamespace(
                    prepared=SimpleNamespace(record_count=1, receipt_count=1),
                    all_references=(),
                    inline_document_ids=(),
                )
            ]

        with (
            patch.object(p, "_pending", return_value=rows),
            patch.object(p, "_prepare_batch_and_upload", side_effect=prepare),
            patch.object(p, "_commit_upload", return_value="committed"),
            patch.object(
                p,
                "drain_cleanup",
                return_value=dict(deleted=0, failures=0, completed=0, pending=0),
            ),
        ):
            p.project_pending(batch_size=260, max_batches=1, upload_concurrency=2)
        self.assertEqual(len(budgets), 262)
        self.assertTrue(all(limit == 0 for _, limit in budgets))


if __name__ == "__main__":
    unittest.main()
