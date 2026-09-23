#!/usr/bin/env python3
"""Fresh-PostgreSQL proof that the Parquet scan plane rebuilds only dirty fragments.

Three sessions share one source-month. Changing one session must rewrite only
the fragments that hold it: the siblings keep their immutable objects, the scan listing
still enumerates every live part (with a gap in ``shard_index``), the replaced
objects are queued for cleanup, and reading every live part returns each
document exactly once. Compaction then folds the month back into one fragment
per dataset under the same never-overwrite rule.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timezone
import hashlib

from psycopg import sql
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq


RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path[:0] = [str(RECALL), str(SERVER)]

from e2e_logical_evidence_projection import (  # noqa: E402
    insert_record,
    insert_source,
)
from recall_server import parquet_scan  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import (  # noqa: E402
    LogicalEvidenceProjectionStore,
)
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
    mark_logical_evidence_dirty,
)
from recall_server.parquet_scan import CanonicalParquetScanProjector  # noqa: E402
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import PassagePolicy  # noqa: E402


def reference(row: dict) -> dict:
    return {
        "contract": "recall.artifact-ref.v1",
        "schema_version": 1,
        "tenant_id": row["tenant_id"],
        "source_id": row["source_id"],
        **{
            key: row[key]
            for key in (
                "artifact_id",
                "storage_backend",
                "object_key",
                "content_sha256",
                "size_bytes",
                "media_type",
                "encryption",
                "version_id",
            )
        },
        "created_at": row["created_at"].isoformat(),
    }


def live_parts(store: BrainStore, tenant: str, source: str) -> list[dict]:
    with store.connect() as connection:
        return connection.execute(
            """SELECT * FROM canonical_parquet_scan_shards
                WHERE tenant_id=%s AND source_id=%s
                ORDER BY dataset,shard_index""",
            (tenant, source),
        ).fetchall()


def members(store: BrainStore, tenant: str, source: str) -> dict[tuple[str, int], set[str]]:
    with store.connect() as connection:
        rows = connection.execute(
            """SELECT dataset,shard_index,logical_document_id
                 FROM canonical_parquet_scan_fragment_documents
                WHERE tenant_id=%s AND source_id=%s""",
            (tenant, source),
        ).fetchall()
    grouped: dict[tuple[str, int], set[str]] = {}
    for row in rows:
        grouped.setdefault((row["dataset"], row["shard_index"]), set()).add(
            row["logical_document_id"]
        )
    return grouped


def read_dataset(archive, parts: list[dict], dataset: str) -> list[dict]:
    rows: list[dict] = []
    for row in parts:
        if row["dataset"] != dataset:
            continue
        rows.extend(
            pq.read_table(pa.BufferReader(archive.read_raw(reference(row)))).to_pylist()
        )
    return rows


def assert_each_document_once(archive, parts: list[dict], expected: set[str]) -> dict:
    documents = Counter(
        row["logical_document_id"] for row in read_dataset(archive, parts, "documents")
    )
    assert set(documents) == expected, (set(documents), expected)
    assert set(documents.values()) == {1}, documents
    records = Counter(
        (row["logical_document_id"], row["ordinal"])
        for row in read_dataset(archive, parts, "records")
    )
    assert set(records.values()) == {1}, records
    assert {document for document, _ in records} == expected
    passages = Counter(
        row["passage_id"] for row in read_dataset(archive, parts, "passages")
    )
    assert set(passages.values()) <= {1}, passages
    return {
        "documents": sum(documents.values()),
        "records": sum(records.values()),
        "passages": sum(passages.values()),
    }


def cleanup_queue(store: BrainStore, tenant: str, source: str) -> set[str]:
    with store.connect() as connection:
        return {
            row["artifact_id"]
            for row in connection.execute(
                """SELECT artifact_id FROM canonical_evidence_cleanup_queue
                    WHERE tenant_id=%s AND source_id=%s
                      AND media_type='application/vnd.apache.parquet'""",
                (tenant, source),
            ).fetchall()
        }


def assert_compaction_owner_equivalence(store: BrainStore) -> int:
    """Exercise the exact owning sweep against actual schema/PKs, isolated in temp tables."""
    from tests.central_brain.test_parquet_compaction_owner import cases

    with store.connect() as connection:
        with connection.transaction():
            for table in (
                "canonical_parquet_scan_shards", "canonical_parquet_scan_fragment_documents",
                "canonical_parquet_scan_queue", "canonical_parquet_scan_dirty_documents",
            ):
                connection.execute(sql.SQL(
                    "CREATE TEMP TABLE {} (LIKE public.{} INCLUDING ALL) ON COMMIT DROP"
                ).format(sql.Identifier(table), sql.Identifier(table)))

            class BoundStore:
                @contextmanager
                def connect(self):
                    yield connection

            for index, (name, catalog, cap, expected) in enumerate(cases()):
                tenant, source = f"tenant:owner-{index}", "source:owner-proof"
                bucket = date(2026, 8, 1)
                for (dataset, shard_index), row in catalog.shards.items():
                    digest = hashlib.sha256(f"{name}/{dataset}/{shard_index}".encode()).hexdigest()
                    connection.execute(
                        """INSERT INTO canonical_parquet_scan_shards(
                               tenant_id,source_id,bucket_start,dataset,shard_index,
                               generation_sha256,artifact_id,storage_backend,object_key,
                               content_sha256,size_bytes,media_type,encryption,version_id,
                               row_count,created_at
                           ) VALUES (%s,%s,%s,%s,%s,%s,%s,'filesystem',%s,%s,%s,
                                     'application/vnd.apache.parquet','filesystem-owner-only',
                                     'v1',1,%s)""",
                        (tenant, source, bucket, dataset, shard_index, row["generation_sha256"],
                         "art_"+digest[:32], "objects/"+digest[:2]+"/"+digest, digest,
                         row["size_bytes"], datetime(2026, 8, 1, tzinfo=timezone.utc)),
                    )
                    for member in catalog.members.get((dataset, shard_index), ()):
                        connection.execute(
                            """INSERT INTO canonical_parquet_scan_fragment_documents(
                                   tenant_id,source_id,bucket_start,dataset,shard_index,
                                   logical_document_id,revision,generation_sha256
                               ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (tenant, source, bucket, dataset, shard_index,
                             member.logical_document_id, member.revision, member.generation_sha256),
                        )
                projector = CanonicalParquetScanProjector(
                    BoundStore(), SimpleNamespace(archive=None), compaction_fragments=cap
                )
                queued = projector._over_fragmented(tenant_id=tenant, limit=1)
                assert bool(queued) == catalog.fragmented(cap) == expected, name
                if expected:
                    assert len(queued) == 1 and queued[0].tenant_id == tenant, name
                    assert projector._over_fragmented(tenant_id=tenant, limit=1) == [], name
                    dirty = connection.execute(
                        """SELECT logical_document_id,reason FROM canonical_parquet_scan_dirty_documents
                            WHERE tenant_id=%s""", (tenant,)
                    ).fetchall()
                    assert dirty == [{"logical_document_id": "*", "reason": "compaction"}], name
            # The unscoped sweep returns none: all eligible rows were consumed,
            # while large replacements and empty catalogs remain unqueued.
            projector.compaction_fragments = 16
            assert projector._over_fragmented(tenant_id=None, limit=10) == []
            connection.execute("DELETE FROM canonical_parquet_scan_queue")
            connection.execute("DELETE FROM canonical_parquet_scan_dirty_documents")
            projector.compaction_fragments = 2
            eligible = []
            for index, (_, catalog, _, _) in enumerate(cases()):
                if catalog.fragmented(2):
                    by_dataset = {}
                    for (dataset, _), row in catalog.shards.items():
                        count, size = by_dataset.get(dataset, (0, 0))
                        by_dataset[dataset] = (count+1, size+row["size_bytes"])
                    dominant = max(by_dataset, key=lambda name: (
                        by_dataset[name][1], by_dataset[name][0], name
                    ))
                    eligible.append((-by_dataset[dominant][0], f"tenant:owner-{index}"))
            queued = projector._over_fragmented(tenant_id=None, limit=2)
            assert [row.tenant_id for row in queued] == [
                tenant for _, tenant in sorted(eligible)[:2]
            ]
    return len(cases())


def assert_dirty_fence(store, tenant, source):
    with store.connect() as connection:
        rows = connection.execute(
            """SELECT dirty.queued_at,queue.changed_at
                 FROM canonical_parquet_scan_dirty_documents dirty
                 LEFT JOIN canonical_parquet_scan_queue queue
                   USING(tenant_id,source_id,bucket_start)
                WHERE dirty.tenant_id=%s AND dirty.source_id=%s""",
            (tenant, source),
        ).fetchall()
    assert rows and all(row['changed_at'] is not None and
                        row['queued_at'] <= row['changed_at'] for row in rows), rows


def assert_forget_read_fence(store, logical, scan, tenant, principal, source, parent):
    """Exercise the actual catalog SQL while old immutable bytes still exist."""
    retrieval = BoundCanonicalRetrieval(
        store, tenant_id=tenant, principal_id=principal, authorized_sources=(source,),
    )
    def selected():
        return retrieval._parquet_shards([source], since=None, until=None)
    empty_ids = {row['artifact_id'] for row in live_parts(store, tenant, source)
                 if row['row_count'] == 0}
    before, pending = selected()
    assert len(before) == 4 and pending == 0, (before, pending)
    with store.connect() as connection:
        document = connection.execute(
            "SELECT * FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
            (tenant, source, parent),
        ).fetchone()
        connection.execute(
            "INSERT INTO canonical_evidence_document_queue(tenant_id,source_id,native_parent_id,generation,reason) VALUES (%s,%s,%s,1,'backfill')",
            (tenant, source, parent),
        )
    # Ordinary projection lag does not hide the corpus.
    ordinary, pending = selected()
    assert len(ordinary) == 4 and pending == 1, (ordinary, pending)
    with store.connect() as connection:
        connection.execute(
            "UPDATE canonical_evidence_document_queue SET reason='forget' WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
            (tenant, source, parent),
        )
    # Existing packed objects contain this parent. All are withheld, and a
    # new worker build must not read its old logical archive either.
    hidden, pending = selected()
    assert {r['artifact_id'] for r in hidden} == empty_ids and pending >= 4-len(empty_ids), (hidden, pending)
    candidate = parquet_scan.ScanCandidate(tenant, source, before[0]['bucket_start'],
        generation=1, changed_at=datetime.now(timezone.utc), reason='forget')
    assert document['logical_document_id'] not in {
        row['logical_document_id'] for row in scan._documents(candidate)
    }
    scan._requeue_missing_document(document)
    with store.connect() as connection:
        assert connection.execute(
            "SELECT reason FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
            (tenant, source, parent),
        ).fetchone()['reason'] == 'forget'
    logical.seed_backfill(tenant_id=tenant, source_id=source, include_existing=True)
    with store.connect() as connection:
        reason = connection.execute(
            "SELECT reason FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
            (tenant, source, parent),
        ).fetchone()['reason']
        assert reason == 'forget', reason
        connection.execute(
            "DELETE FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s", (tenant, source),
        )
        # Models the sanitized logical generation advancing while old shards
        # remain. The old catalog cannot become visible when the queue clears.
        connection.execute(
            "UPDATE canonical_evidence_documents SET revision=revision+1 WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
            (tenant, source, parent),
        )
    hidden, pending = selected()
    assert {r['artifact_id'] for r in hidden} == empty_ids and pending == 4-len(empty_ids), (hidden, pending)
    with store.connect() as connection:
        connection.execute(
            "UPDATE canonical_evidence_documents SET revision=revision-1 WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
            (tenant, source, parent),
        )
        memberships = connection.execute(
            "DELETE FROM canonical_parquet_scan_fragment_documents WHERE tenant_id=%s AND source_id=%s AND dataset='records' RETURNING *",
            (tenant, source),
        ).fetchall()
    without_members, pending = selected()
    assert len(without_members) == 3 and pending == 1, (without_members, pending)
    with store.connect() as connection:
        for row in memberships:
            connection.execute(
                "INSERT INTO canonical_parquet_scan_fragment_documents(tenant_id,source_id,bucket_start,dataset,shard_index,logical_document_id,revision,generation_sha256) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                tuple(row[key] for key in ('tenant_id','source_id','bucket_start','dataset','shard_index','logical_document_id','revision','generation_sha256')),
            )
    restored, pending = selected()
    assert len(restored) == 4 and pending == 0


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant = f"tenant:fragments-e2e:{nonce}"
    principal = f"principal:fragments-e2e:{nonce}"
    source = f"codex:fragments-e2e:{nonce}"
    session_a = f"session-a-{nonce}"
    session_b = f"session-b-{nonce}"
    session_c = f"session-c-{nonce}"
    with store.connect() as connection:
        insert_source(connection, tenant, principal, source)
        for session, marker in (
            (session_a, "alpha"), (session_b, "beta"), (session_c, "gamma")
        ):
            for ordinal, role in enumerate(("user", "assistant")):
                insert_record(
                    connection,
                    tenant=tenant,
                    source=source,
                    parent=session,
                    native=f"{session}:{role}",
                    text=f"{marker} fragment marker {role} {nonce}",
                    role=role,
                    byte_start=ordinal * 10,
                )

    # Production fragments hold many documents (up to FRAGMENT_TARGET_BYTES of
    # rows). Close a fragment at every document boundary here so each session
    # gets its own part and the delta is observable with two documents.
    fragment_target = parquet_scan.FRAGMENT_TARGET_BYTES
    parquet_scan.FRAGMENT_TARGET_BYTES = 1

    with tempfile.TemporaryDirectory(prefix="recall-fragments-e2e-") as value:
        archive = FilesystemArchiveStore(Path(value) / "archive", namespace_key=b"f" * 32)
        logical_store = LogicalEvidenceProjectionStore(archive)
        logical = CanonicalLogicalEvidenceProjector(
            store, logical_store, bound_tenant_id=tenant, raw_archive=archive
        )
        assert logical.seed_backfill(tenant_id=tenant) == 3
        assert logical.project_pending(
            tenant_id=tenant, batch_size=10, max_batches=1, upload_concurrency=1
        )["documents"] == 3
        passages = CanonicalPassageProjector(
            store,
            logical_store,
            policy=PassagePolicy(target_tokens=4, overlap_tokens=1),
            bound_tenant_id=tenant,
        )
        assert passages.project_pending(
            tenant_id=tenant, batch_size=10, max_batches=1, concurrency=2
        )["documents"] == 3
        with store.connect() as connection:
            documents = {
                row["native_parent_id"]: row["logical_document_id"]
                for row in connection.execute(
                    """SELECT native_parent_id,logical_document_id
                         FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s""",
                    (tenant, source),
                ).fetchall()
            }
            dirty_before = connection.execute(
                """SELECT logical_document_id,reason
                     FROM canonical_parquet_scan_dirty_documents
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            ).fetchall()
        every = set(documents.values())
        # Documents are written in document-id order, so change the session
        # with the lowest logical id: its parts sit at index 0 of every dataset
        # and the rewrite leaves a real gap for the listing to serve.
        changed_session, ldoc_a = min(documents.items(), key=lambda item: item[1])
        ldoc_b, ldoc_c = sorted(every - {ldoc_a})
        # The logical projector named every document dirty for the month.
        assert {row["logical_document_id"] for row in dirty_before} == every

        scan = CanonicalParquetScanProjector(store, logical_store)
        initial = scan.project_pending(tenant_id=tenant, batch_size=4, max_batches=1)
        assert initial["shards"] == 1, initial
        assert initial["documents_dirty"] == 3, initial
        parts_v1 = live_parts(store, tenant, source)
        per_dataset = Counter(row["dataset"] for row in parts_v1)
        assert per_dataset == {"documents": 3, "records": 3, "passages": 3, "actors": 1}, per_dataset
        assert initial["fragments_rewritten"] == len(parts_v1), initial
        assert initial["fragments_total"] == len(parts_v1), initial
        members_v1 = members(store, tenant, source)
        assert all(len(owners) == 1 for owners in members_v1.values()), members_v1

        def owned_by(parts: list[dict], owners: dict, document: str) -> dict:
            return {
                (row["dataset"], row["shard_index"]): (row["artifact_id"], row["object_key"])
                for row in parts
                if owners.get((row["dataset"], row["shard_index"])) == {document}
            }

        changed_v1 = owned_by(parts_v1, members_v1, ldoc_a)
        siblings_v1 = {
            **owned_by(parts_v1, members_v1, ldoc_b),
            **owned_by(parts_v1, members_v1, ldoc_c),
        }
        assert len(changed_v1) == 3 and len(siblings_v1) == 6, (changed_v1, siblings_v1)
        assert all(index == 0 for (_, index) in changed_v1), changed_v1
        counts_v1 = assert_each_document_once(archive, parts_v1, every)
        assert cleanup_queue(store, tenant, source) == set()
        with store.connect() as connection:
            assert connection.execute(
                """SELECT count(*) AS n FROM canonical_parquet_scan_dirty_documents
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            ).fetchone()["n"] == 0

        # Change one session only: a new record, re-projected in place.
        with store.connect() as connection:
            insert_record(
                connection,
                tenant=tenant,
                source=source,
                parent=changed_session,
                native=f"{changed_session}:tool",
                text=f"changed fragment marker tool {nonce}",
                role="tool",
                byte_start=30,
            )
            connection.execute(
                """INSERT INTO canonical_evidence_document_queue(
                       tenant_id,source_id,native_parent_id,generation,reason,changed_at
                   ) VALUES (%s,%s,%s,1,'ingest',clock_timestamp())
                   ON CONFLICT(tenant_id,source_id,native_parent_id)
                   DO UPDATE SET
                       generation=canonical_evidence_document_queue.generation+1,
                       reason='ingest',changed_at=clock_timestamp()""",
                (tenant, source, changed_session),
            )
        assert logical.project_pending(
            tenant_id=tenant, batch_size=10, max_batches=1, upload_concurrency=1
        )["documents"] == 1
        assert passages.project_pending(
            tenant_id=tenant, batch_size=10, max_batches=1, concurrency=2
        )["documents"] == 1
        with store.connect() as connection:
            dirty = connection.execute(
                """SELECT logical_document_id,reason
                     FROM canonical_parquet_scan_dirty_documents
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            ).fetchall()
            queued = connection.execute(
                """SELECT reason FROM canonical_parquet_scan_queue
                    WHERE tenant_id=%s AND source_id=%s""",
                (tenant, source),
            ).fetchall()
        assert [(row["logical_document_id"], row["reason"]) for row in dirty] == [
            (ldoc_a, "logical-update")
        ], dirty
        assert [row["reason"] for row in queued] == ["logical-update"], queued

        delta = scan.project_pending(tenant_id=tenant, batch_size=4, max_batches=1)
        assert delta["shards"] == 1, delta
        assert delta["documents_dirty"] == 1, delta
        assert delta["fragments_rewritten"] == 3, delta
        assert delta["compacted"] == 0, delta
        parts_v2 = live_parts(store, tenant, source)
        assert delta["fragments_total"] == len(parts_v2), delta
        members_v2 = members(store, tenant, source)
        changed_v2 = owned_by(parts_v2, members_v2, ldoc_a)
        siblings_v2 = {
            **owned_by(parts_v2, members_v2, ldoc_b),
            **owned_by(parts_v2, members_v2, ldoc_c),
        }
        # The siblings' parts are untouched: same index, artifact, and object.
        assert siblings_v2 == siblings_v1, (siblings_v1, siblings_v2)
        # The changed session moved to fresh parts above every survivor; its old objects
        # are queued for cleanup and index 0 is now a gap in every dataset.
        assert len(changed_v2) == 3, changed_v2
        replaced = {artifact for artifact, _ in changed_v1.values()}
        assert {artifact for artifact, _ in changed_v2.values()} & replaced == set()
        assert {key for _, key in changed_v2.values()} & {
            key for _, key in changed_v1.values()
        } == set()
        assert cleanup_queue(store, tenant, source) == replaced
        for dataset in ("documents", "records", "passages"):
            indexes = sorted(
                row["shard_index"] for row in parts_v2 if row["dataset"] == dataset
            )
            assert indexes == [1, 2, 3], indexes
            assert changed_v2[(dataset, 3)]
        counts_v2 = assert_each_document_once(archive, parts_v2, every)
        assert counts_v2["records"] == counts_v1["records"] + 1, (counts_v1, counts_v2)
        # The scan listing enumerates every live part, gaps included.
        listing, pending = BoundCanonicalRetrieval(
            store,
            tenant_id=tenant,
            principal_id=principal,
            authorized_sources=(source,),
        )._parquet_shards([source], since=None, until=None)
        assert pending == 0, (pending, listing)
        listed = {(row["dataset"], row["shard_index"]) for row in listing}
        assert listed == {(row["dataset"], row["shard_index"]) for row in parts_v2}
        aliases = {
            f"{row['dataset']}-part-{int(row['shard_index']):05d}.parquet"
            for row in listing
        }
        assert len(aliases) == len(listing)
        assert {row["object_key"] for row in listing} == {row["object_key"] for row in parts_v2}
        non_contiguous = sorted(
            row["shard_index"] for row in listing if row["dataset"] == "records"
        )

        # Re-queueing the unchanged month is a no-op: objects and cleanup untouched.
        assert scan.seed_backfill(tenant_id=tenant) == 1
        reuse = scan.project_pending(tenant_id=tenant, batch_size=4, max_batches=1)
        assert reuse["shards"] == 1 and reuse["fragments_rewritten"] == 0, reuse
        assert [
            (row["dataset"], row["shard_index"], row["artifact_id"])
            for row in live_parts(store, tenant, source)
        ] == [(row["dataset"], row["shard_index"], row["artifact_id"]) for row in parts_v2]
        assert cleanup_queue(store, tenant, source) == replaced

        # Compaction: the month has 2 fragments per dataset; a cap of 1 folds it.
        # Back at the production target both documents fit one fragment.
        parquet_scan.FRAGMENT_TARGET_BYTES = fragment_target
        compactor = CanonicalParquetScanProjector(
            store, logical_store, compaction_fragments=1
        )
        # A prior idle sweep can leave a queued compaction sentinel. Busy
        # processing must not turn that hint into a mandatory whole-month build.
        queued = compactor._over_fragmented(tenant_id=tenant, limit=1)
        assert len(queued) == 1, queued
        assert_dirty_fence(store, tenant, source)
        assert compactor._catalog(queued[0]).compaction
        before_rows = {
            dataset: sorted(json.dumps(row, sort_keys=True, default=str)
                            for row in read_dataset(archive, parts_v2, dataset))
            for dataset in parquet_scan.SCAN_DATASETS
        }
        busy = compactor.project_pending(
            tenant_id=tenant, batch_size=4, max_batches=1, compaction_budget=0
        )
        assert busy["shards"] == 1 and busy["compacted"] == 0, busy
        assert busy["fragments_rewritten"] == 0, busy
        after_busy = live_parts(store, tenant, source)
        assert [(r["dataset"], r["shard_index"], r["artifact_id"]) for r in after_busy] == [
            (r["dataset"], r["shard_index"], r["artifact_id"]) for r in parts_v2
        ]
        assert {
            dataset: sorted(json.dumps(row, sort_keys=True, default=str)
                            for row in read_dataset(archive, after_busy, dataset))
            for dataset in parquet_scan.SCAN_DATASETS
        } == before_rows
        assert not compactor._pending(tenant_id=tenant, limit=1)
        # Idle maintenance rediscovers the same fragmentation autonomously,
        # despite successful busy queue processing consuming the old hint.
        compacted = compactor.project_pending(
            tenant_id=tenant, batch_size=4, max_batches=1, compaction_budget=1
        )
        assert compacted["compacted"] == 1, compacted
        parts_v3 = live_parts(store, tenant, source)
        assert Counter(row["dataset"] for row in parts_v3) == {
            "documents": 1, "records": 1, "passages": 1, "actors": 1
        }, parts_v3
        assert {row["shard_index"] for row in parts_v3} == {0}
        assert {row["artifact_id"] for row in parts_v3} & {
            row["artifact_id"] for row in parts_v2
        } == set()
        assert cleanup_queue(store, tenant, source) >= {row["artifact_id"] for row in parts_v2}
        counts_v3 = assert_each_document_once(archive, parts_v3, every)
        assert counts_v3 == counts_v2, (counts_v2, counts_v3)
        members_v3 = members(store, tenant, source)
        assert members_v3[("documents", 0)] == every
        # A second sweep finds nothing to compact.
        assert compactor.project_pending(
            tenant_id=tenant, batch_size=4, max_batches=1
        )["compacted"] == 0

        # Draining cleanup deletes only the replaced objects; live parts stay readable.
        drained = logical.drain_cleanup(tenant_id=tenant, limit=100)
        assert drained["completed"] >= len(parts_v2), drained
        assert cleanup_queue(store, tenant, source) == set()
        assert assert_each_document_once(archive, parts_v3, every) == counts_v3

        # A compaction hint can outlive its consumed queue row. Re-queue the
        # unchanged month through the owning API, then let reuse consume that
        # older hint so the deletion regression starts with no unrelated dirt.
        assert scan.seed_backfill(tenant_id=tenant, source_id=source) == 1
        assert_dirty_fence(store, tenant, source)
        clean_before_forget = scan.project_pending(
            tenant_id=tenant, batch_size=4, max_batches=1, compaction_budget=0,
        )
        assert clean_before_forget["fragments_rewritten"] == 0, clean_before_forget
        with store.connect() as connection:
            assert connection.execute(
                """SELECT count(*) AS count FROM canonical_parquet_scan_dirty_documents
                   WHERE tenant_id=%s AND source_id=%s""", (tenant, source),
            ).fetchone()["count"] == 0
        assert not scan._pending(tenant_id=tenant, limit=1)

        assert_forget_read_fence(store, logical, scan, tenant, principal, source, session_b)

        # Forget a whole parent after compaction packed it with siblings. The
        # empty logical rebuild has no old document left from which to recover
        # month bounds, so delete_native_ids must have persisted them already.
        forgotten_parent = session_b
        forgotten_document = documents[forgotten_parent]
        with store.connect() as connection:
            forgotten_ids = [row["native_id"] for row in connection.execute(
                """SELECT native_id FROM canonical_events
                   WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
                (tenant, source, forgotten_parent),
            ).fetchall()]
            connection.execute(
                """UPDATE canonical_chunks chunk SET deleted_at=now()
                   FROM canonical_documents document
                   WHERE chunk.tenant_id=document.tenant_id
                     AND chunk.source_id=document.source_id
                     AND chunk.document_id=document.document_id
                     AND document.tenant_id=%s AND document.source_id=%s
                     AND document.native_id=ANY(%s)""",
                (tenant, source, forgotten_ids),
            )
            connection.execute(
                """UPDATE canonical_documents SET is_current=false,deleted_at=now()
                   WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)""",
                (tenant, source, forgotten_ids),
            )
        assert logical.delete_native_ids(
            tenant_id=tenant, source_id=source, native_ids=forgotten_ids,
        ) > 0
        logical.project_pending(tenant_id=tenant, batch_size=10, max_batches=1)
        with store.connect() as connection:
            forgotten_dirty = connection.execute(
                """SELECT logical_document_id,reason FROM canonical_parquet_scan_dirty_documents
                   WHERE tenant_id=%s AND source_id=%s""", (tenant, source),
            ).fetchall()
        assert forgotten_dirty == [dict(logical_document_id=forgotten_document, reason="forget")], forgotten_dirty
        visible_after_forget, pending_after_forget = BoundCanonicalRetrieval(
            store, tenant_id=tenant, principal_id=principal, authorized_sources=(source,),
        )._parquet_shards([source], since=None, until=None)
        assert all(row['dataset'] == 'actors' for row in visible_after_forget)
        assert pending_after_forget >= 1, pending_after_forget
        forgotten_scan = scan.project_pending(
            tenant_id=tenant, batch_size=4, max_batches=1, compaction_budget=0,
        )
        assert forgotten_scan["shards"] == 1, forgotten_scan
        parts_after_forget = live_parts(store, tenant, source)
        remaining = every - {forgotten_document}
        assert_each_document_once(archive, parts_after_forget, remaining)
        for dataset in parquet_scan.SCAN_DATASETS:
            assert all(row["logical_document_id"] != forgotten_document
                       for row in read_dataset(archive, parts_after_forget, dataset)), dataset
        assert not scan._pending(tenant_id=tenant, limit=1)

        # The public path queues logical forget asynchronously. Unlike the
        # direct deletion above, _commit_empty must capture TP IDs itself.
        with store.connect() as connection:
            async_ids = [row['native_id'] for row in connection.execute(
                "SELECT native_id FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
                (tenant, source, session_c),
            ).fetchall()]
            doomed = {row['passage_id'] for row in connection.execute(
                "SELECT passage_id FROM canonical_passages WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s",
                (tenant, source, documents[session_c]),
            ).fetchall()}
            assert doomed
            surviving_ids = {row['passage_id'] for row in connection.execute(
                "SELECT passage_id FROM canonical_passages WHERE tenant_id=%s AND source_id=%s AND logical_document_id<>%s",
                (tenant, source, documents[session_c]),
            ).fetchall()}
            connection.execute(
                "UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND source_id=%s AND document_id IN (SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s))",
                (tenant, source, tenant, source, async_ids),
            )
            connection.execute(
                "UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND source_id=%s AND native_id=ANY(%s)",
                (tenant, source, async_ids),
            )
            mark_logical_evidence_dirty(connection, tenant_id=tenant,
                source_id=source, native_ids=async_ids, reason='forget')
        async_result = logical.project_pending(tenant_id=tenant, batch_size=10, max_batches=1)
        assert async_result['pruned'] == 1, async_result
        assert_dirty_fence(store, tenant, source)
        scan.project_pending(tenant_id=tenant, batch_size=4, max_batches=1, compaction_budget=0)
        with store.connect() as connection:
            assert connection.execute(
                "SELECT count(*) AS count FROM canonical_parquet_scan_dirty_documents WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            ).fetchone()['count'] == 0
        with store.connect() as connection:
            tombstones = {row['passage_id'] for row in connection.execute(
                "SELECT passage_id FROM search_projection_tombstones WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            ).fetchall()}
            assert doomed <= tombstones and not (surviving_ids & tombstones)
            actual_survivors = {row['passage_id'] for row in connection.execute(
                "SELECT passage_id FROM canonical_passages WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            ).fetchall()}
            assert actual_survivors == surviving_ids
            assert connection.execute(
                "SELECT count(*) AS count FROM search_projection_outbox WHERE tenant_id=%s AND source_id=%s",
                (tenant, source),
            ).fetchone()['count'] > 0

        # Partial forget sanitizes logical records before the passage worker.
        # A scan build in that gap must not republish the old passage bytes.
        native = session_a + ':user'
        with store.connect() as connection:
            forgotten_receipts = {row['receipt'] for row in connection.execute(
                "SELECT chunk.receipt FROM canonical_chunks chunk JOIN canonical_documents document USING(tenant_id,source_id,document_id) WHERE document.tenant_id=%s AND document.source_id=%s AND document.native_id=%s",
                (tenant, source, native),
            ).fetchall()}
            assert forgotten_receipts
            connection.execute(
                "UPDATE canonical_chunks SET deleted_at=now() WHERE tenant_id=%s AND source_id=%s AND receipt=ANY(%s)",
                (tenant, source, list(forgotten_receipts)),
            )
            connection.execute(
                "UPDATE canonical_documents SET is_current=false,deleted_at=now() WHERE tenant_id=%s AND source_id=%s AND native_id=%s",
                (tenant, source, native),
            )
            mark_logical_evidence_dirty(connection, tenant_id=tenant,
                source_id=source, native_ids=[native], reason='forget')
        assert logical.project_pending(tenant_id=tenant, batch_size=10, max_batches=1)['documents'] == 1
        with store.connect() as connection:
            old_passages = connection.execute(
                "SELECT receipts FROM canonical_passages WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s",
                (tenant, source, documents[session_a]),
            ).fetchall()
            assert any(forgotten_receipts & set(row['receipts']) for row in old_passages)
        scan.project_pending(tenant_id=tenant, batch_size=4, max_batches=1, compaction_budget=0)
        partial_parts = live_parts(store, tenant, source)
        for dataset in ('records', 'passages'):
            projected_rows = read_dataset(archive, partial_parts, dataset)
            assert all(not (forgotten_receipts & set(row['receipts'])) for row in projected_rows), dataset
        assert read_dataset(archive, partial_parts, 'records')
        _, partial_pending = BoundCanonicalRetrieval(store, tenant_id=tenant,
            principal_id=principal, authorized_sources=(source,))._parquet_shards([source], since=None, until=None)
        assert partial_pending > 0
        passages.project_pending(tenant_id=tenant, batch_size=10, max_batches=1, concurrency=2)
        scan.project_pending(tenant_id=tenant, batch_size=4, max_batches=1, compaction_budget=0)
        final_parts = live_parts(store, tenant, source)
        assert read_dataset(archive, final_parts, 'passages')
        assert all(not (forgotten_receipts & set(row['receipts']))
                   for row in read_dataset(archive, final_parts, 'passages'))

    owner_cases = assert_compaction_owner_equivalence(store)
    result = {
        "status": "pass",
        "summary": {
            "sessions": 3,
            "compaction_owner_sql_python_cases": owner_cases,
            "initial_fragments": len(parts_v1),
            "delta_fragments_rewritten": delta["fragments_rewritten"],
            "delta_documents_dirty": delta["documents_dirty"],
            "delta_fragments_total": delta["fragments_total"],
            "sibling_parts_unchanged": len(siblings_v1),
            "replaced_objects_queued": len(replaced),
            "records_shard_indexes_after_delta": non_contiguous,
            "documents_read_once": counts_v2["documents"],
            "records_read_once": counts_v2["records"],
            "compacted_months": compacted["compacted"],
            "fragments_after_compaction": len(parts_v3),
            "cleanup_completed": drained["completed"],
            "forget_scan_pending_before_rebuild": pending_after_forget,
            "forgotten_document_absent_from_all_datasets": True,
            "async_empty_commit_tombstones": len(doomed),
        },
    }
    rendered = json.dumps(result, sort_keys=True)
    if output := os.environ.get("RECALL_E2E_OUT"):
        Path(output).write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
