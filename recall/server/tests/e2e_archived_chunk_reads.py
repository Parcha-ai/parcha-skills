#!/usr/bin/env python3
"""Fresh PostgreSQL proof that logical archives preserve exact chunk reads."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))
from e2e_logical_evidence_projection import insert_record, insert_source  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical_retrieval import BoundCanonicalRetrieval  # noqa: E402
from recall_server.chunk_bodies import ChunkBodyError, read_archived_chunks  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceProjectionStore  # noqa: E402
from recall_server.logical_evidence_projection import CanonicalLogicalEvidenceProjector  # noqa: E402


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def unavailable(callback):
    try:
        callback()
    except ChunkBodyError as error:
        assert str(error) == "archived_chunk_body_unavailable"
    else:
        raise AssertionError("unverified archived body was returned or silently replaced")


def unsupported_records_keep_inline(store, archive, projection, tenant, source, kwargs):
    """Unsupported representations cannot become eligible for body thinning."""
    historical_text = "historical chunk boundary α " * 100
    oversized_text = json.dumps({
        "type": "assistant", "message": {"role": "assistant", "content": "full oversized " + "z" * 40_000},
    }, separators=(",", ":"))
    reference = archive.put_raw(
        tenant_id=tenant, source_id=source, native_id="oversized:full",
        payload=gzip.compress(oversized_text.encode()),
        media_type="application/vnd.recall.oversized-record+gzip",
        created_at="2026-09-20T12:00:00Z",
    )
    with store.connect() as connection:
        historical_receipt = insert_record(
            connection, tenant=tenant, source=source, parent="session:historical",
            native="historical", text=historical_text, role="assistant", byte_start=0,
        )
        # Two historical chunks differ from today's one-chunk layout.
        first, second = historical_text[:51], historical_text[51:]
        connection.execute(
            """UPDATE canonical_chunks SET text_redacted=%s,text_sha256=%s
               WHERE tenant_id=%s AND source_id=%s AND receipt=%s""",
            (first, digest(first), tenant, source, historical_receipt),
        )
        connection.execute(
            """INSERT INTO canonical_chunks(tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256)
               SELECT tenant_id,source_id,%s,document_id,1,%s,%s,%s FROM canonical_chunks
               WHERE tenant_id=%s AND source_id=%s AND receipt=%s""",
            ("chk_" + uuid.uuid4().hex, historical_receipt.replace("#item=0", "#item=1"),
             second, digest(second), tenant, source, historical_receipt),
        )
        same_count_receipt = insert_record(
            connection, tenant=tenant, source=source, parent="session:historical-same-count",
            native="historical-same-count", text=historical_text * 30, role="assistant", byte_start=0,
        )
        chunks = connection.execute(
            """SELECT chunk.receipt,chunk.text_redacted FROM canonical_chunks chunk
               JOIN canonical_documents document USING(tenant_id,source_id,document_id)
               WHERE chunk.tenant_id=%s AND chunk.source_id=%s
                 AND document.native_id='historical-same-count' ORDER BY chunk.ordinal""",
            (tenant, source),
        ).fetchall()
        assert len(chunks) > 2
        # Preserve the count and whole text, while moving a historical boundary.
        moved = chunks[0]["text_redacted"][-1]
        chunks[0]["text_redacted"] = chunks[0]["text_redacted"][:-1]
        chunks[1]["text_redacted"] = moved + chunks[1]["text_redacted"]
        for chunk in chunks[:2]:
            connection.execute(
                """UPDATE canonical_chunks SET text_redacted=%s,text_sha256=%s
                   WHERE tenant_id=%s AND source_id=%s AND receipt=%s""",
                (chunk["text_redacted"], digest(chunk["text_redacted"]), tenant, source, chunk["receipt"]),
            )
        oversized_receipt = insert_record(
            connection, tenant=tenant, source=source, parent="session:oversized", native="oversized",
            text="bounded oversized projection", role="assistant", byte_start=0,
            raw_reference=reference,
            canonical_content={
                "contract": "recall.oversized-projection.v1", "archive_encoding": "gzip",
                "full_record_available": True, "full_size_bytes": len(oversized_text.encode()),
                "full_content_sha256": digest(oversized_text),
            },
        )
        rows = connection.execute(
            """SELECT document_id FROM canonical_documents WHERE tenant_id=%s AND source_id=%s
               AND native_id=ANY(%s) ORDER BY native_id""",
            (tenant, source, ["historical", "historical-same-count", "oversized"]),
        ).fetchall()
    postgres = BoundCanonicalRetrieval(store, **kwargs)
    baseline = {r: postgres.show(r) for r in (historical_receipt, same_count_receipt, oversized_receipt)}
    assert projection.seed_backfill(tenant_id=tenant) == 3
    report = projection.project_pending(tenant_id=tenant, batch_size=10, max_batches=1, upload_concurrency=1)
    assert report["documents"] == 3, report
    assert read_archived_chunks(
        store, archive, tenant_id=tenant, source_ids=(source,),
        document_ids=tuple(row["document_id"] for row in rows),
    ) == {}, "unsupported documents must not be eligible to thin"
    archived = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **kwargs)
    for receipt, expected in baseline.items():
        assert archived.show(receipt) == expected, "retain exact inline fallback for unsupported layouts"
    with store.connect() as connection:
        assert connection.execute("SELECT count(*) AS n FROM canonical_evidence_objects").fetchone()["n"] == 0


def main():
    admin_dsn = os.environ["RECALL_DATABASE_URL"]
    database = "recall_archived_reads_" + uuid.uuid4().hex
    settings = conninfo_to_dict(admin_dsn)
    settings["dbname"] = database
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{database}"')
    store = BrainStore(make_conninfo(**settings))
    try:
        store.migrate()
        nonce = uuid.uuid4().hex
        tenant, source, principal = f"tenant:archive:{nonce}", f"codex:archive:{nonce}", f"principal:archive:{nonce}"
        parent = "session:archive:one"
        start = datetime(2026, 9, 20, 10, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory(prefix="recall-archived-read-e2e-") as temporary:
            archive = FilesystemArchiveStore(Path(temporary) / "archive", namespace_key=b"a" * 32)
            with store.connect() as connection:
                insert_source(connection, tenant, principal, source)
                for index, (native, text, session) in enumerate([
                    ("before", "Before α and café", parent),
                    ("anchor", ("Long exact β 🧠 line\n" * 7000) + "tail", parent),
                    ("after", "After β", parent),
                    ("other-parent", "A different session must not enter context", "session:archive:other"),
                ]):
                    insert_record(connection, tenant=tenant, source=source, parent=session,
                                  native=native, text=text, role="assistant", byte_start=index * 10)
                    connection.execute(
                        """UPDATE canonical_events SET occurred_at=%s,observed_at=%s
                           WHERE tenant_id=%s AND source_id=%s AND native_id=%s""",
                        (start + timedelta(minutes=index), start + timedelta(minutes=index, seconds=1), tenant, source, native),
                    )
                anchor = connection.execute(
                    """SELECT chunk.receipt,chunk.document_id,chunk.ordinal
                       FROM canonical_chunks chunk JOIN canonical_documents document
                       USING(tenant_id,source_id,document_id)
                       WHERE chunk.tenant_id=%s AND chunk.source_id=%s AND document.native_id='anchor'
                       ORDER BY chunk.ordinal OFFSET 2 LIMIT 1""", (tenant, source),
                ).fetchone()
            assert anchor is not None, "fixture must contain a nonzero chunk anchor"
            target = anchor["receipt"]
            kwargs = dict(tenant_id=tenant, principal_id=principal, authorized_sources=(source,))
            postgres = BoundCanonicalRetrieval(store, **kwargs)
            archived = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **kwargs)
            baseline_show = postgres.show(target)
            baseline_context = postgres.session_context(target, before=2, after=2)
            baseline_related = postgres.related(limit=20)
            assert len(baseline_show["chunks"]) > 3
            assert [e["native_id"] for e in baseline_context["events"]] == ["before", "anchor", "after"]
            assert any(c["text_clipped"] for e in baseline_context["events"] for c in e["chunks"])
            assert archived.show(target) == baseline_show, "unprojected catalog must retain PG fallback"
            projection = CanonicalLogicalEvidenceProjector(
                store, LogicalEvidenceProjectionStore(archive), bound_tenant_id=tenant, raw_archive=archive,
            )
            assert projection.seed_backfill(tenant_id=tenant) == 2
            report = projection.project_pending(tenant_id=tenant, batch_size=10, max_batches=1, upload_concurrency=1)
            assert report["documents"] == 2 and report["records"] == 4, report
            with store.connect() as connection:
                connection.execute("UPDATE canonical_chunks SET text_redacted='' WHERE tenant_id=%s AND source_id=%s", (tenant, source))
                assert connection.execute(
                    "SELECT sum(octet_length(text_redacted)) AS n FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s", (tenant, source),
                ).fetchone()["n"] == 0
                catalog = connection.execute(
                    """SELECT part.* FROM canonical_evidence_document_parts part
                       JOIN canonical_evidence_documents document USING(tenant_id,source_id,logical_document_id)
                       WHERE part.tenant_id=%s AND part.source_id=%s AND document.native_parent_id=%s
                       ORDER BY part.part_ordinal LIMIT 1""", (tenant, source, parent),
                ).fetchone()
                assert connection.execute("SELECT count(*) AS n FROM canonical_evidence_objects").fetchone()["n"] == 0
            assert archived.show(target) == baseline_show
            assert archived.session_context(target, before=2, after=2) == baseline_context
            assert archived.related(limit=20) == baseline_related
            assert all(c["text"] == "" for c in postgres.show(target)["chunks"])
            revoked = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **{**kwargs, "authorized_sources": ()})
            assert revoked.show(target) is None
            assert revoked.session_context(target) is None
            outsider = BoundCanonicalRetrieval(store, chunk_body_archive=archive, **{**kwargs, "tenant_id": "tenant:other"})
            assert outsider.show(target) is None
            data_path = archive.root / catalog["object_key"] / "data"
            payload = data_path.read_bytes()
            try:
                data_path.write_bytes(b"corrupt archived body")
                unavailable(lambda: archived.show(target))
                unavailable(lambda: archived.session_context(target))
                data_path.unlink()
                unavailable(lambda: archived.show(target))
            finally:
                data_path.write_bytes(payload)
                data_path.chmod(0o600)
            assert archived.show(target) == baseline_show
            unsupported_records_keep_inline(store, archive, projection, tenant, source, kwargs)
            with store.connect() as connection:
                original_hash = connection.execute(
                    "SELECT text_sha256 FROM canonical_chunks WHERE tenant_id=%s AND source_id=%s AND receipt=%s", (tenant, source, target),
                ).fetchone()["text_sha256"]
                connection.execute(
                    "UPDATE canonical_chunks SET text_sha256=%s WHERE tenant_id=%s AND source_id=%s AND receipt=%s", ("0" * 64, tenant, source, target),
                )
            unavailable(lambda: archived.show(target))
            with store.connect() as connection:
                connection.execute(
                    "UPDATE canonical_chunks SET text_sha256=%s WHERE tenant_id=%s AND source_id=%s AND receipt=%s", (original_hash, tenant, source, target),
                )
                connection.execute(
                    """INSERT INTO canonical_events(
                        tenant_id,source_id,event_id,native_id,native_parent_id,artifact_id,job_id,
                        kind,content_sha256,revision,occurred_at,observed_at,is_tombstone,canonical_redacted)
                       SELECT tenant_id,source_id,%s,native_id,native_parent_id,artifact_id,job_id,
                              kind,%s,revision+1,occurred_at,observed_at,true,'{}'::jsonb
                       FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id='anchor'""",
                    ("evt_" + uuid.uuid4().hex, "f" * 64, tenant, source),
                )
            assert archived.show(target) is None
            assert archived.session_context(target) is None
        print(json.dumps({
            "status": "pass", "eligible_fixture_inline_chunk_text_bytes": 0,
            "show_context_related_exact": True, "nonzero_anchor_preserved": True,
            "unprojected_fallback": True, "legacy_bundle_catalog_empty": True,
            "unsupported_oversized_and_historical_chunker_keep_inline": True,
            "corrupt_missing_and_hash_mismatch_refused": True,
            "revoked_cross_tenant_and_tombstoned_denied": True,
        }, sort_keys=True))
    finally:
        store.close()
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{database}"')


if __name__ == "__main__":
    main()
