#!/usr/bin/env python3
"""Locator spooling stays bounded and disk failure precedes all object uploads."""
from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import sys
import tempfile
import tracemalloc
import uuid
from unittest.mock import patch

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from e2e_archive_reprojection import fixture, mark_dirty  # noqa: E402
from e2e_logical_source_integrity import TrackedStore, snapshot  # noqa: E402
from recall_server.logical_evidence import LogicalEvidenceError  # noqa: E402
from recall_server.logical_evidence_projection import _LocatorSpool  # noqa: E402


def disk_failure(store, root, fail_at):
    tenant, source, archive, projection, projector, _ = fixture(store, root)
    before = snapshot(store, tenant, source)
    mark_dirty(store, tenant, source)
    created = []

    class FailingSpool(_LocatorSpool):
        def __init__(self):
            super().__init__()
            self.writes = 0
            created.append(self)

        def append(self, row):
            self.writes += 1
            if self.writes == fail_at:
                raise OSError(errno.ENOSPC, 'synthetic locator disk full')
            return super().append(row)

    with patch('recall_server.logical_evidence_projection._LocatorSpool', FailingSpool):
        report = projector.project_pending(tenant_id=tenant, batch_size=1, max_batches=1, upload_concurrency=1)
    assert report['failed'] == 1 and archive.uploads == projection.upload_calls == 0, report
    assert created and all(spool.closed for spool in created)
    assert snapshot(store, tenant, source) == before
    with store.connect() as connection:
        queue = connection.execute('SELECT attempts FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s', (tenant, source)).fetchone()
        assert queue['attempts'] == 1


def publication(store, root):
    tenant, source, _archive, _projection, projector, _ = fixture(store, root)
    mark_dirty(store, tenant, source)
    candidate = projector._pending(tenant_id=tenant, limit=1)[0]
    spool = _LocatorSpool()
    try:
        for index in range(100_000):
            spool.append((f'doc_{index:032x}', index, 1))
        with store.connect() as connection:
            try:
                with connection.transaction():
                    # A publisher must not retain either its input or the
                    # changed-row result in Python. Keep the test's large
                    # metadata stream synthetic; no 100k source bodies needed.
                    tracemalloc.start()
                    try:
                        projector._publish_body_locators(connection, candidate, spool)
                        _current, peak = tracemalloc.get_traced_memory()
                    finally:
                        tracemalloc.stop()
                    assert peak < 4 * 1024 * 1024, peak
                    assert connection.execute('SELECT count(*) AS n FROM pg_temp.recall_body_locator_desired').fetchone()['n'] == 100_000
                    raise RuntimeError('rollback synthetic publication')
            except RuntimeError as error:
                assert str(error) == 'rollback synthetic publication'
            assert connection.execute("SELECT to_regclass('pg_temp.recall_body_locator_desired') AS relation").fetchone()['relation'] is None
        # A duplicate document must abort the transaction, not silently select
        # an arbitrary position. The original current locators stay intact.
        spool.append(('doc_' + '0' * 32, 100_000, 1))
        with store.connect() as connection:
            try:
                with connection.transaction():
                    projector._publish_body_locators(connection, candidate, spool)
            except LogicalEvidenceError as error:
                assert str(error) == 'logical_evidence_state_invalid'
            else:
                raise AssertionError('duplicate locator accepted')
            assert connection.execute("SELECT to_regclass('pg_temp.recall_body_locator_desired') AS relation").fetchone()['relation'] is None
        return peak
    finally:
        spool.close()


def main():
    admin = os.environ['RECALL_DATABASE_URL']
    database = 'recall_locator_spool_' + uuid.uuid4().hex
    settings = conninfo_to_dict(admin)
    settings['dbname'] = database
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(f'CREATE DATABASE "{database}"')
    store = TrackedStore(make_conninfo(**settings))
    try:
        store.migrate()
        with tempfile.TemporaryDirectory(prefix='recall-locator-spool-') as directory:
            root = Path(directory)
            disk_failure(store, root, 1)
            disk_failure(store, root, 3)
            peak = publication(store, root)
        print(json.dumps({'status': 'pass', 'streamed_locators': 100_000,
                          'publication_python_peak_bytes': peak,
                          'early_and_late_enospc_zero_uploads': True,
                          'duplicate_rollback_temp_cleanup': True}, sort_keys=True))
    finally:
        store.close()
        with psycopg.connect(admin, autocommit=True) as connection:
            connection.execute(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == '__main__':
    main()
