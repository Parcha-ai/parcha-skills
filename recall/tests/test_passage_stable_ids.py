"""H1-T3: stable passage identity, differential commit, shadow parity gate."""

from __future__ import annotations

import inspect
import re
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any
from unittest import mock

from recall_server.logical_evidence import LogicalEvidenceError
from recall_server.passage_index import (
    PASSAGE_ORDINAL_PARK_OFFSET,
    CanonicalPassageProjector,
    PassageCandidate,
    PassageDiff,
    PreparedPassageDocument,
    classify_passages,
)
from recall_server.passage_projection import (
    PassageMessage,
    PassagePolicy,
    build_passages,
    canonical_spans_json,
    passage_identity,
)

TENANT = "tenant:company:test"
SOURCE = "source:test"
LDOC = "ldoc_0123456789abcdef0123456789abcdef"
POLICY = PassagePolicy(target_tokens=8, overlap_tokens=2)
PASSAGE_ID_RE = re.compile(r"psg_[0-9a-f]{32}\Z")


def messages(count: int, *, edit: dict[int, str] | None = None) -> tuple[PassageMessage, ...]:
    edit = edit or {}
    return tuple(
        PassageMessage(
            record_ordinal=index,
            occurred_at=f"2026-07-27T00:00:{index:02d}Z",
            roles=("user",) if index % 2 == 0 else ("assistant",),
            receipts=(f"recall://source:test/record?rev=1#item={index}",),
            text=edit.get(index, f"message {index} alpha beta gamma delta"),
        )
        for index in range(count)
    )


def build(count: int, *, revision: int = 1, edit: dict[int, str] | None = None):
    return build_passages(
        tenant_id=TENANT,
        source_id=SOURCE,
        logical_document_id=LDOC,
        revision=revision,
        messages=messages(count, edit=edit),
        policy=POLICY,
    )


class StablePassageIdentityTests(unittest.TestCase):
    def test_passage_id_shape_and_formula(self) -> None:
        passages = build(3)
        for passage in passages:
            self.assertRegex(passage.passage_id, PASSAGE_ID_RE)
            self.assertEqual(
                passage.passage_id,
                passage_identity(
                    tenant_id=TENANT,
                    source_id=SOURCE,
                    logical_document_id=LDOC,
                    policy_fingerprint=POLICY.fingerprint,
                    text_sha256=passage.text_sha256,
                    spans=passage.spans,
                ),
            )
        # Canonical JSON: sorted keys, no whitespace, one object per span.
        encoded = canonical_spans_json(passages[0].spans)
        self.assertNotIn(" ", encoded)
        self.assertTrue(encoded.startswith('[{"message_index":'))

    def test_passage_ids_are_stable_under_append(self) -> None:
        for base, extra in ((3, 1), (3, 5), (6, 2), (10, 7)):
            before = build(base, revision=1)
            after = build(base + extra, revision=2)
            self.assertGreater(len(after), len(before))
            # Every window except possibly the last (short) one of the first
            # build is the same passage in the second build: same id, text,
            # spans and ordinal.
            for index, passage in enumerate(before[:-1]):
                grown = after[index]
                self.assertEqual(passage.passage_id, grown.passage_id, (base, extra, index))
                self.assertEqual(passage.text, grown.text)
                self.assertEqual(passage.spans, grown.spans)
                self.assertEqual(passage.ordinal, grown.ordinal)
            shared = {p.passage_id for p in before} & {p.passage_id for p in after}
            self.assertGreaterEqual(len(shared), len(before) - 1)

    def test_passage_ids_change_only_after_mid_document_edit(self) -> None:
        original = build(8)
        edited = build(8, edit={4: "message 4 rewritten with other words entirely"})
        first_changed = next(
            index
            for index, (a, b) in enumerate(zip(original, edited, strict=True))
            if a.passage_id != b.passage_id
        )
        # Windows before the edit keep their ids and are byte-identical (a
        # window may end inside the unchanged prefix of the edited record) …
        for a, b in zip(original[:first_changed], edited[:first_changed], strict=True):
            self.assertEqual(a.passage_id, b.passage_id)
            self.assertEqual(a.text, b.text)
            self.assertEqual(a.spans, b.spans)
        # … the first changed window covers the edited record …
        self.assertTrue(
            any(span.message_index == 4 for span in edited[first_changed].spans)
        )
        # … and everything from the edit onward is a different passage.
        original_tail = {p.passage_id for p in original[first_changed:]}
        edited_tail = {p.passage_id for p in edited[first_changed:]}
        self.assertFalse(original_tail & edited_tail)

    def test_passage_id_excludes_revision(self) -> None:
        first = build(5, revision=1)
        second = build(5, revision=9)
        self.assertEqual(
            [p.passage_id for p in first], [p.passage_id for p in second]
        )
        self.assertEqual([p.revision for p in second], [9] * len(second))
        # …but a different document, tenant, source or policy changes the id.
        other_policy = build_passages(
            tenant_id=TENANT,
            source_id=SOURCE,
            logical_document_id=LDOC,
            revision=1,
            messages=messages(5),
            policy=PassagePolicy(target_tokens=8, overlap_tokens=3),
        )
        self.assertNotEqual(first[0].passage_id, other_policy[0].passage_id)
        other_document = build_passages(
            tenant_id=TENANT,
            source_id=SOURCE,
            logical_document_id="ldoc_ffffffffffffffffffffffffffffffff",
            revision=1,
            messages=messages(5),
            policy=POLICY,
        )
        self.assertNotEqual(first[0].passage_id, other_document[0].passage_id)


class ClassifyPassagesTests(unittest.TestCase):
    def test_classification_of_append(self) -> None:
        before = build(3, revision=1)
        after = build(6, revision=2)
        existing = [
            {"passage_id": p.passage_id, "ordinal": p.ordinal, "revision": 1}
            for p in before
        ]
        diff = classify_passages(existing, after, revision=2)
        self.assertIsInstance(diff, PassageDiff)
        self.assertEqual(
            set(diff.retained), {p.passage_id for p in before[:-1]} | (
                {before[-1].passage_id} if before[-1].passage_id in {p.passage_id for p in after} else set()
            ),
        )
        self.assertLessEqual(len(diff.to_delete), 1)
        self.assertEqual(len(diff.retained) + len(diff.to_delete), len(before))
        self.assertEqual(
            len(diff.to_insert), len(after) - len(diff.retained)
        )
        self.assertEqual(diff.moved, ())
        # Every retained row must have its revision bumped to 2.
        self.assertEqual(set(diff.revision_stale), set(diff.retained))
        self.assertEqual(
            diff.counters,
            {
                "inserted": len(diff.to_insert),
                "deleted": len(diff.to_delete),
                "retained": len(diff.retained),
            },
        )

    def test_classification_marks_moved_and_superseded_policy_rows(self) -> None:
        passages = build(4, revision=3)
        existing = [
            # Same ids, rotated ordinals: every row moved.
            {
                "passage_id": p.passage_id,
                "ordinal": (p.ordinal + 1) % len(passages),
                "revision": 3,
            }
            for p in passages
        ] + [
            {"passage_id": "psg_" + "0" * 32, "ordinal": 99, "revision": 1},
        ]
        diff = classify_passages(existing, passages, revision=3)
        self.assertEqual(diff.to_insert, ())
        self.assertEqual(diff.to_delete, ("psg_" + "0" * 32,))
        self.assertEqual(len(diff.retained), len(passages))
        self.assertEqual(
            diff.moved,
            tuple((p.passage_id, p.ordinal) for p in passages),
        )
        self.assertEqual(diff.revision_stale, ())

    def test_classification_rejects_duplicate_rows(self) -> None:
        passages = build(2)
        row = {"passage_id": passages[0].passage_id, "ordinal": 0, "revision": 1}
        with self.assertRaises(LogicalEvidenceError):
            classify_passages([row, row], passages, revision=1)


class FakeResult:
    def __init__(self, rows: list[dict[str, Any]] | None = None, rowcount: int = 0):
        self.rows = rows or []
        self.rowcount = rowcount

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class FakeCopy:
    def __init__(self, sql: str, sink: list[tuple[str, tuple]]):
        self.sql = sql
        self.sink = sink

    def write_row(self, row) -> None:
        self.sink.append((self.sql, tuple(row)))


class FakeCursor:
    def __init__(self, connection: "FakeConnection"):
        self.connection = connection

    @contextmanager
    def copy(self, sql: str):
        self.connection.statements.append(("COPY", sql, None))
        yield FakeCopy(sql, self.connection.copied)


class FakeConnection:
    def __init__(self, *, candidate: PassageCandidate, existing: list[dict[str, Any]]):
        self.candidate = candidate
        self.existing = existing
        self.statements: list[tuple[str, str, Any]] = []
        self.copied: list[tuple[str, tuple]] = []

    @contextmanager
    def transaction(self):
        yield

    @contextmanager
    def cursor(self):
        yield FakeCursor(self)

    def execute(self, sql: str, params: Any = None) -> FakeResult:
        self.statements.append(("SQL", sql, params))
        if "FROM canonical_passage_projection_queue" in sql and "SELECT" in sql:
            return FakeResult([
                {
                    "revision": self.candidate.revision,
                    "generation": self.candidate.generation,
                    "changed_at": self.candidate.changed_at,
                }
            ])
        if "FROM canonical_evidence_documents" in sql and "document_content_sha256" in sql:
            return FakeResult([
                {
                    "revision": self.candidate.revision,
                    "document_content_sha256": self.candidate.source_document_sha256,
                }
            ])
        if sql.lstrip().startswith("SELECT passage_id,ordinal,revision"):
            return FakeResult(list(self.existing))
        if sql.lstrip().startswith("DELETE FROM canonical_passage_projection_queue"):
            return FakeResult(rowcount=1)
        return FakeResult()


class FakeStore:
    pool_max_size = 4

    def __init__(self, connection: FakeConnection):
        self.connection = connection

    @contextmanager
    def connect(self):
        yield self.connection


def candidate_for(revision: int) -> PassageCandidate:
    return PassageCandidate(
        tenant_id=TENANT,
        source_id=SOURCE,
        logical_document_id=LDOC,
        revision=revision,
        generation=1,
        changed_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
        source_document_sha256="ab" * 32,
        manifest_reference={},
        part_references=(),
    )


def prepared_for(passages, revision: int) -> PreparedPassageDocument:
    return PreparedPassageDocument(
        candidate=candidate_for(revision),
        dense_message_count=len({s.message_index for p in passages for s in p.spans}),
        dense_message_bytes=sum(len(p.text.encode()) for p in passages),
        passages=passages,
    )


def statement_kinds(connection: FakeConnection) -> list[str]:
    kinds = []
    for kind, sql, _ in connection.statements:
        head = " ".join(sql.split())
        if kind == "COPY":
            kinds.append("COPY " + head.split("(")[0].split()[-1])
        elif head.startswith("DELETE FROM canonical_passages "):
            kinds.append("DELETE passages")
        elif head.startswith("INSERT INTO canonical_passage_documents"):
            kinds.append("UPSERT pointer")
        elif head.startswith("UPDATE canonical_passages SET ordinal=ordinal+"):
            kinds.append("PARK moved")
        elif head.startswith("UPDATE canonical_passages passage SET ordinal=moved.ordinal"):
            kinds.append("PLACE moved")
        elif head.startswith("UPDATE canonical_passages SET revision="):
            kinds.append("BUMP revision")
        elif head.startswith("INSERT INTO canonical_passage_embeddings"):
            kinds.append("REATTACH embeddings")
        elif head.startswith("CREATE TEMP TABLE"):
            kinds.append("CAPTURE embeddings")
    return kinds


class DifferentialCommitTests(unittest.TestCase):
    def projector(self, connection: FakeConnection) -> CanonicalPassageProjector:
        return CanonicalPassageProjector(
            FakeStore(connection),
            mock.Mock(),
            policy=POLICY,
        )

    def test_append_inserts_new_windows_and_keeps_the_prefix_in_place(self) -> None:
        before = build(3, revision=1)
        after = build(6, revision=2)
        existing = [
            {"passage_id": p.passage_id, "ordinal": p.ordinal, "revision": 1}
            for p in before
        ]
        connection = FakeConnection(candidate=candidate_for(2), existing=existing)

        result = self.projector(connection)._commit(prepared_for(after, 2))

        self.assertEqual(result["status"], "committed")
        self.assertEqual(result["retained"] + result["deleted"], len(before))
        self.assertLessEqual(result["deleted"], 1)
        self.assertEqual(result["inserted"], len(after) - result["retained"])
        kinds = statement_kinds(connection)
        # Embedding capture precedes the delete, the delete precedes the
        # pointer upsert, retained rows are bumped before COPY, COPY precedes
        # the embedding re-attach.
        self.assertEqual(kinds[0], "CAPTURE embeddings")
        if result["deleted"]:
            self.assertLess(kinds.index("DELETE passages"), kinds.index("UPSERT pointer"))
        self.assertLess(kinds.index("UPSERT pointer"), kinds.index("BUMP revision"))
        self.assertLess(kinds.index("BUMP revision"), kinds.index("COPY canonical_passages"))
        self.assertLess(kinds.index("COPY canonical_passages"), kinds.index("REATTACH embeddings"))
        self.assertNotIn("PARK moved", kinds)
        # Only new passages hit COPY; text of retained rows is never written.
        copied_ids = {row[4] for sql, row in connection.copied if "canonical_passages(" in sql}
        self.assertEqual(copied_ids, {p.passage_id for p in after} - {p.passage_id for p in before})
        for kind, sql, params in connection.statements:
            if kind == "SQL" and "UPDATE canonical_passages" in sql:
                self.assertNotIn("text_redacted", sql)
        bump = next(
            params for kind, sql, params in connection.statements
            if kind == "SQL" and " ".join(sql.split()).startswith("UPDATE canonical_passages SET revision=")
        )
        self.assertEqual(bump[0], 2)
        self.assertEqual(set(bump[3]), set(result and {p.passage_id for p in before} & {p.passage_id for p in after}))

    def test_shifted_windows_are_deleted_then_moved_rows_are_parked_before_placement(self) -> None:
        passages = build(4, revision=3)
        # Stored ordinals are rotated by one (every retained row moves) and a
        # stale row sits on ordinal 0, which retained row 3 must take over.
        existing = [
            {"passage_id": p.passage_id, "ordinal": p.ordinal + 1, "revision": 2}
            for p in passages[:-1]
        ] + [{"passage_id": "psg_" + "f" * 32, "ordinal": 0, "revision": 2}]
        connection = FakeConnection(candidate=candidate_for(3), existing=existing)

        result = self.projector(connection)._commit(prepared_for(passages, 3))

        self.assertEqual(result, {"status": "committed", "inserted": 1, "deleted": 1, "retained": 3})
        kinds = statement_kinds(connection)
        self.assertEqual(
            [k for k in kinds if k not in {"CAPTURE embeddings", "COPY canonical_passage_actors"}],
            [
                "DELETE passages",
                "UPSERT pointer",
                "PARK moved",
                "PLACE moved",
                "COPY canonical_passages",
                "REATTACH embeddings",
            ],
        )
        park = next(p for k, s, p in connection.statements if k == "SQL" and "ordinal=ordinal+" in s)
        self.assertEqual(park[0], PASSAGE_ORDINAL_PARK_OFFSET)
        self.assertEqual(set(park[3]), {p.passage_id for p in passages[:-1]})
        place = next(p for k, s, p in connection.statements if k == "SQL" and "moved.ordinal" in s)
        self.assertEqual(place[0], 3)
        self.assertEqual(
            dict(zip(place[1], place[2], strict=True)),
            {p.passage_id: p.ordinal for p in passages[:-1]},
        )
        delete = next(p for k, s, p in connection.statements if k == "SQL" and s.lstrip().startswith("DELETE FROM canonical_passages\n"))
        self.assertEqual(delete[3], ["psg_" + "f" * 32])
        # The moved rows take the revision in the placement statement; no
        # separate revision bump is issued for them.
        self.assertNotIn("BUMP revision", kinds)

    def test_unchanged_document_touches_nothing_but_the_pointer(self) -> None:
        passages = build(4, revision=2)
        existing = [
            {"passage_id": p.passage_id, "ordinal": p.ordinal, "revision": 2}
            for p in passages
        ]
        connection = FakeConnection(candidate=candidate_for(2), existing=existing)

        result = self.projector(connection)._commit(prepared_for(passages, 2))

        self.assertEqual(result, {"status": "committed", "inserted": 0, "deleted": 0, "retained": 4})
        self.assertEqual(statement_kinds(connection), ["UPSERT pointer"])
        self.assertEqual(connection.copied, [])

    def test_stale_candidate_returns_stale_without_writes(self) -> None:
        passages = build(2, revision=2)
        connection = FakeConnection(candidate=candidate_for(3), existing=[])

        result = self.projector(connection)._commit(prepared_for(passages, 2))

        self.assertEqual(result, {"status": "stale"})
        self.assertEqual(statement_kinds(connection), [])

    def test_project_pending_sums_differential_counters(self) -> None:
        projector = CanonicalPassageProjector(
            FakeStore(FakeConnection(candidate=candidate_for(1), existing=[])),
            mock.Mock(),
            policy=POLICY,
        )
        passages = build(3)
        prepared = prepared_for(passages, 1)
        projector._pending = mock.Mock(side_effect=[(prepared.candidate, prepared.candidate), ()])
        projector._prepare = mock.Mock(return_value=prepared)
        projector._commit = mock.Mock(side_effect=[
            {"status": "committed", "inserted": 2, "deleted": 1, "retained": 5},
            {"status": "stale"},
        ])
        with mock.patch.object(
            type(projector.store.connection), "execute",
            return_value=FakeResult([{"count": 0}]),
        ):
            result = projector.project_pending(max_batches=2)
        self.assertEqual(result["documents"], 1)
        self.assertEqual(result["stale"], 1)
        self.assertEqual(result["passages"], 2)
        self.assertEqual(result["inserted"], 2)
        self.assertEqual(result["deleted"], 1)
        self.assertEqual(result["retained"], 5)

    def test_commit_source_never_rewrites_retained_text(self) -> None:
        source = inspect.getsource(CanonicalPassageProjector._commit)
        for statement in re.findall(r'UPDATE canonical_passages.*?"""', source, flags=re.S):
            self.assertNotIn("text_redacted", statement)
            self.assertNotIn("spans", statement)
        self.assertIn("FOR UPDATE", source)


class ShadowDiffTests(unittest.TestCase):
    def test_shadow_diff_reports_parity_content_free(self) -> None:
        current = build(4, revision=2)
        # Stored rows: two keep the new id, two carry old-formula ids, and the
        # receipt multiset is identical (same passage set, different ids).
        stored = [
            {
                "logical_document_id": LDOC,
                "passage_id": p.passage_id if index < 2 else "psg_" + f"{index:032x}",
                "receipts": list(p.receipts),
            }
            for index, p in enumerate(current)
        ]
        base = {
            "tenant_id": TENANT,
            "source_id": SOURCE,
            "logical_document_id": LDOC,
            "revision": 2,
            "policy_fingerprint": POLICY.fingerprint,
            "target_tokens": POLICY.target_tokens,
            "overlap_tokens": POLICY.overlap_tokens,
            "source_document_sha256": "ab" * 32,
            "passage_count": 4,
            "evidence_revision": 2,
            "document_content_sha256": "ab" * 32,
            "manifest_created_at": datetime(2026, 9, 11, tzinfo=timezone.utc),
            "part_ordinal": 0,
        }
        for prefix in ("manifest_", "part_"):
            base.update({
                prefix + "artifact_id": "art_" + "0" * 32,
                prefix + "storage_backend": "filesystem",
                prefix + "object_key": "objects/00/x",
                prefix + "content_sha256": "cd" * 32,
                prefix + "size_bytes": 1,
                prefix + "media_type": "application/x-ndjson",
                prefix + "encryption": "none",
                prefix + "version_id": "v1",
            })
        base["part_created_at"] = base["manifest_created_at"]
        stale = dict(base, logical_document_id="ldoc_" + "e" * 32, evidence_revision=3)

        class Connection(FakeConnection):
            def execute(self, sql, params=None):
                self.statements.append(("SQL", sql, params))
                if "FROM canonical_passage_documents sampled" in sql:
                    return FakeResult([base, stale])
                if "SELECT logical_document_id,passage_id,receipts" in sql:
                    return FakeResult(stored)
                raise AssertionError(sql)

        connection = Connection(candidate=candidate_for(2), existing=[])
        projector = CanonicalPassageProjector(FakeStore(connection), mock.Mock(), policy=POLICY)
        projector._prepare = mock.Mock(return_value=prepared_for(current, 2))

        report = projector.shadow_diff(tenant_id=TENANT, source_id=SOURCE, limit=50)

        self.assertTrue(report["read_only"])
        self.assertEqual(report["totals"], {
            "documents": 1,
            "documents_stale": 1,
            "documents_policy_mismatch": 0,
            "passages_existing": 4,
            "passages_recomputed": 4,
            "ids_shared": 2,
            "receipt_set_equal": 1,
        })
        self.assertTrue(report["receipt_parity"])
        compared = next(d for d in report["documents"] if d["status"] == "compared")
        self.assertEqual(compared["ids_shared"], 2)
        self.assertTrue(compared["receipt_set_equal"])
        self.assertEqual(
            next(d for d in report["documents"] if d["status"] == "stale")["logical_document_id"],
            "ldoc_" + "e" * 32,
        )
        projector._prepare.assert_called_once()
        self.assertEqual(projector._prepare.call_args.kwargs["policy"], POLICY)
        # Content-free: no passage text or receipts leave the report.
        flat = repr(report)
        self.assertNotIn("alpha beta", flat)
        self.assertNotIn("recall://", flat)
        # Nothing but SELECTs was issued.
        for _, sql, _ in connection.statements:
            self.assertTrue(sql.lstrip().startswith("SELECT"), sql)

    def test_shadow_diff_rejects_bad_scope(self) -> None:
        projector = CanonicalPassageProjector(
            FakeStore(FakeConnection(candidate=candidate_for(1), existing=[])),
            mock.Mock(),
            policy=POLICY,
        )
        with self.assertRaises(ValueError):
            projector.shadow_diff(tenant_id=TENANT, source_id=SOURCE, limit=0)
        with self.assertRaises(ValueError):
            projector.shadow_diff(tenant_id=TENANT, source_id="", limit=5)


if __name__ == "__main__":
    unittest.main()
