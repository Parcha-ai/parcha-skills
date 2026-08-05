#!/usr/bin/env python3
"""Fresh-PostgreSQL proof for the production canonical connector path."""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "recall"))
sys.path.insert(0, str(ROOT / "recall/server"))

from client.mac import (
    CanonicalArchiveClient,
    CanonicalBrainWriter,
    CanonicalClientError,
)
from connectors.sdk import ConnectorPage, ConnectorRecord, ConnectorRunner
from privacy.policy import PrivacyPolicy
from recall_server.app import Handler
from recall_server.archive import FilesystemArchiveStore
from recall_server.archive_snapshot import (
    backup_filesystem_archive,
    restore_filesystem_archive,
    verify_filesystem_archive,
)
from recall_server.canonical import CanonicalPlane
from recall_server.db import BrainStore


TENANT = "tenant:personal:e2e"
PRINCIPAL = "principal:owner:e2e"
SOURCE = "source:v2:e2e"
NATIVE = "native:v2:e2e"
CANARY = "synthetic-v2-private-canary-8741"
OCCURRED = "2026-07-20T00:00:00Z"


class CountingArchive:
    def __init__(self, archive: FilesystemArchiveStore):
        self.archive = archive
        self.puts = 0

    def put_raw(self, **kwargs):
        self.puts += 1
        return self.archive.put_raw(**kwargs)

    def delete_raw(self, reference):
        return self.archive.delete_raw(reference)


class RevisionConnector:
    connector_id = "synthetic.v2"
    source_id = SOURCE

    @staticmethod
    def record(text: str, *, deleted: bool = False) -> ConnectorRecord:
        return ConnectorRecord(
            schema_version=1,
            native_id=NATIVE,
            native_parent_id=NATIVE,
            occurred_at=OCCURRED,
            content={} if deleted else {
                "text": text,
                "password": CANARY,
            },
            provenance={"uri": "connector://synthetic-v2/e2e"},
            deleted=deleted,
        )

    def pull(self, cursor: str | None) -> ConnectorPage:
        pages = {
            None: (self.record("safe revision one"), "cursor:1"),
            "cursor:1": (self.record("safe revision two"), "cursor:2"),
            "cursor:2": (self.record("", deleted=True), "cursor:3"),
            "cursor:3": (self.record("safe revision one"), "cursor:4"),
            "cursor:4": (self.record("safe resurrection attempt"), "cursor:5"),
        }
        record, next_cursor = pages[cursor]
        return ConnectorPage(records=(record,), next_cursor=next_cursor, has_more=True)


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    credential = store.create_collector_token(
        "canonical-v2-e2e",
        SOURCE,
        ["write"],
        tenant_id=TENANT,
        principal_id=PRINCIPAL,
    )
    previous = {
        name: os.environ.get(name)
        for name in (
            "RECALL_AUTH_REQUIRED",
            "RECALL_HTTP_PROFILE",
            "RECALL_CANONICAL_INGEST_PUBLIC",
        )
    }
    os.environ.update({
        "RECALL_AUTH_REQUIRED": "1",
        "RECALL_HTTP_PROFILE": "public-mcp",
        "RECALL_CANONICAL_INGEST_PUBLIC": "1",
    })
    logs = io.StringIO()
    handler = logging.StreamHandler(logs)
    logger = logging.getLogger("recall.brainstore")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        archive = FilesystemArchiveStore(
            root / "archive",
            namespace_key=b"e" * 32,
        )
        counting = CountingArchive(archive)
        plane = CanonicalPlane(store, counting)
        Handler.store = store
        Handler.archive_store = counting
        Handler.canonical_plane = plane
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        endpoint = f"http://127.0.0.1:{server.server_port}"
        common = {
            "endpoint": endpoint,
            "token": credential["token"],
            "source_id": SOURCE,
            "tenant_id": TENANT,
            "principal_id": PRINCIPAL,
        }
        for forged in (
            {**common, "tenant_id": "tenant:forged:e2e"},
            {**common, "source_id": "source:forged:e2e"},
        ):
            try:
                CanonicalArchiveClient(**forged).put_raw(
                    tenant_id=forged["tenant_id"],
                    source_id=forged["source_id"],
                    native_id=NATIVE,
                    payload=b'{"forged":true}',
                    media_type="application/json",
                    created_at=OCCURRED,
                )
            except CanonicalClientError:
                pass
            else:
                raise RuntimeError("cross-scope archive write was accepted")
        if counting.puts:
            raise RuntimeError("cross-scope write reached private archive")
        spool_root = root / "spool"
        spool_root.mkdir(mode=0o700)
        spool = spool_root / "connector.db"
        runner = ConnectorRunner(
            connector=RevisionConnector(),
            brain=CanonicalBrainWriter(**common),
            archive=CanonicalArchiveClient(**common),
            tenant_id=TENANT,
            principal_id=PRINCIPAL,
            spool_path=spool,
            privacy=PrivacyPolicy(mode="scrub"),
        )
        try:
            results = [runner.run_once() for _ in range(3)]
            if [result["acked"] for result in results] != [1, 1, 1]:
                raise RuntimeError("canonical revisions did not ACK")
            if counting.puts != 3:
                raise RuntimeError("raw archive ordering failed")
            replay = runner.run_once()
            if (
                replay["acked"] != 0
                or replay["deduplicated"] != 1
                or counting.puts != 3
            ):
                raise RuntimeError("acknowledged replay rearchived or duplicated")

            before = verify_filesystem_archive(archive.root)
            snapshot = root / "snapshot"
            restored = root / "restored"
            backup_filesystem_archive(archive.root, snapshot)
            restore_filesystem_archive(snapshot, restored)
            after = verify_filesystem_archive(restored)
            if before != after or before["object_count"] != 3:
                raise RuntimeError("archive restore fingerprint failed")

            with store.connect() as connection:
                state = connection.execute(
                    """SELECT
                         (SELECT count(*) FROM canonical_events
                           WHERE tenant_id=%s AND source_id=%s) AS events,
                         (SELECT count(*) FROM canonical_documents
                           WHERE tenant_id=%s AND source_id=%s) AS documents,
                         (SELECT count(*) FROM canonical_chunks
                           WHERE tenant_id=%s AND source_id=%s) AS chunks,
                         (SELECT count(*) FROM canonical_documents
                           WHERE tenant_id=%s AND source_id=%s
                             AND is_current) AS current_documents,
                         (SELECT count(*) FROM canonical_events
                           WHERE tenant_id=%s AND source_id=%s
                             AND canonical_redacted::text LIKE %s) AS leaks,
                         (SELECT receipt FROM canonical_chunks
                           WHERE tenant_id=%s AND source_id=%s
                           ORDER BY created_at LIMIT 1) AS receipt""",
                    (
                        TENANT, SOURCE, TENANT, SOURCE, TENANT, SOURCE,
                        TENANT, SOURCE, TENANT, SOURCE, f"%{CANARY}%",
                        TENANT, SOURCE,
                    ),
                ).fetchone()
            if tuple(state[key] for key in (
                "events", "documents", "chunks", "current_documents", "leaks",
            )) != (3, 2, 2, 0, 0):
                raise RuntimeError("canonical revision or tombstone state failed")
            status = CanonicalBrainWriter(**common).status()
            if (
                status["status"] != "ok"
                or status["live_events"] != 0
                or status["current_documents"] != 0
                or status["missing_current_documents"] != 0
                or status["tombstoned_identities"] != 1
            ):
                raise RuntimeError("canonical status endpoint lost lifecycle parity")
            private_spool = b"".join(
                path.read_bytes()
                for path in spool.parent.glob(spool.name + "*")
                if path.is_file()
            )
            if CANARY.encode() in private_spool or CANARY in logs.getvalue():
                raise RuntimeError("privacy canary escaped archive")

            forget = plane.forget({
                "contract": "recall.forget-request.v1",
                "schema_version": 1,
                "tenant_id": TENANT,
                "principal_id": PRINCIPAL,
                "source_id": SOURCE,
                "target_receipt": state["receipt"],
                "mode": "explicit_forget",
                "reason": "owner_requested",
                "requested_at": "2026-07-20T00:05:00Z",
                "idempotency_key": "-".join(("canonical", "v2", "e2e", "forget")),
            })
            resurrection = runner.run_once()
            if (
                forget["raw_deleted"] != 3
                or resurrection["forgotten"] != 1
                or resurrection["acked"] != 0
                or counting.puts != 3
            ):
                raise RuntimeError("forget fence accepted resurrection")
            with store.connect() as connection:
                remaining = connection.execute(
                    """SELECT
                         (SELECT count(*) FROM canonical_events
                           WHERE tenant_id=%s AND source_id=%s)
                       + (SELECT count(*) FROM canonical_documents
                           WHERE tenant_id=%s AND source_id=%s)
                       + (SELECT count(*) FROM canonical_chunks
                           WHERE tenant_id=%s AND source_id=%s) AS count""",
                    (TENANT, SOURCE, TENANT, SOURCE, TENANT, SOURCE),
                ).fetchone()["count"]
            if remaining or verify_filesystem_archive(archive.root)["object_count"]:
                raise RuntimeError("forget left canonical or raw evidence")
        finally:
            runner.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            logger.removeHandler(handler)
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
    store.close()
    print(json.dumps({
        "status": "pass",
        "canonical_revisions": 2,
        "canonical_tombstones": 1,
        "exact_replay_duplicates": 0,
        "exact_replay_archive_writes": 0,
        "privacy_leaks": 0,
        "archive_restore_fingerprint": True,
        "cross_scope_writes": 0,
        "resurrections": 0,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
