"""Explicit retirement of redundant search indexes after the reader cutover."""
from __future__ import annotations

from typing import Any

_INDEX = 'public.canonical_chunks_search_idx'
_SIZE_SQL = """SELECT to_regclass('public.canonical_chunks_search_idx') IS NOT NULL AS present,
                      coalesce(pg_relation_size(to_regclass(
                          'public.canonical_chunks_search_idx')),0)::bigint AS bytes"""


def retire_chunk_search_index(store: Any, *, apply: bool = False) -> dict[str, Any]:
    """Preview by default; concurrently remove only the retired chunk GIN.

    Operators must deploy the session-scoped turbopuffer reader on all serving
    instances before applying. This operation preserves every row and generated
    search vector; restoring the index requires a separate concurrent build.
    """
    if store.search_plane != 'turbopuffer':
        raise ValueError('chunk index retirement requires turbopuffer')
    with store.connect() as connection:
        before = connection.execute(_SIZE_SQL).fetchone()
    if not apply or not before['present']:
        return {'status': 'preview' if not apply else 'already_absent',
                'index': _INDEX, 'bytes_before': int(before['bytes']),
                'bytes_reclaimed': 0}
    with store.connect() as connection:
        connection.autocommit = True
        try:
            connection.execute("SET lock_timeout='2s'")
            connection.execute("SET statement_timeout='60s'")
            connection.execute('DROP INDEX CONCURRENTLY IF EXISTS ' + _INDEX)
            after = connection.execute(_SIZE_SQL).fetchone()
        finally:
            # Session settings must not leak into the shared connection pool.
            connection.execute('RESET lock_timeout')
            connection.execute('RESET statement_timeout')
            connection.autocommit = False
    if after['present']:
        raise RuntimeError('chunk index retirement incomplete')
    return {'status': 'retired', 'index': _INDEX,
            'bytes_before': int(before['bytes']), 'bytes_reclaimed': int(before['bytes'])}


_VECTOR_SQL = """SELECT relation.oid::bigint AS relation_oid,
    EXISTS(SELECT 1 FROM schema_migrations WHERE version=67) AS plane_retired,
    to_regclass('public.canonical_chunks_search_idx') IS NOT NULL AS gin_present,
    attribute.attnum IS NOT NULL AS present,
    attribute.atttypid::integer AS type_oid,attribute.attgenerated AS generated,
    pg_get_expr(definition.adbin,definition.adrelid) AS expression,
    (SELECT count(*) FROM pg_depend dependency
      WHERE dependency.refclassid='pg_class'::regclass
        AND dependency.refobjid=relation.oid
        AND dependency.refobjsubid=attribute.attnum
        AND NOT (dependency.classid='pg_attrdef'::regclass
                 AND dependency.objid=definition.oid))::integer AS dependents
    FROM pg_class relation JOIN pg_namespace namespace
      ON namespace.oid=relation.relnamespace
    LEFT JOIN pg_attribute attribute ON attribute.attrelid=relation.oid
      AND attribute.attname='search_vector' AND NOT attribute.attisdropped
    LEFT JOIN pg_attrdef definition ON definition.adrelid=relation.oid
      AND definition.adnum=attribute.attnum
    WHERE namespace.nspname='public' AND relation.relname='canonical_chunks'
      AND relation.relkind='r'"""
_VECTOR_EXPRESSION = "to_tsvector('simple'::regconfig, text_redacted)"


def _vector_snapshot(connection: Any) -> dict[str, Any]:
    row = connection.execute(_VECTOR_SQL).fetchone()
    if row is None or not row['plane_retired']:
        raise ValueError('chunk vector retirement requires recorded migration 067')
    if row['gin_present']:
        raise ValueError('retire the chunk search index before its vector column')
    if row['present'] and (
        row['type_oid'] != 3614 or row['generated'] != 's'
        or row['expression'] != _VECTOR_EXPRESSION or row['dependents'] != 0
    ):
        raise ValueError('chunk vector definition or dependencies changed')
    return row


def retire_chunk_search_vector(store: Any, *, apply: bool = False) -> dict[str, Any]:
    """Explicitly remove only the unused derived column on the retired PG plane.

    No migration version is added: 067 already prevents migrate from replaying
    028 and recreating the column. Deploy compatible turbopuffer readers before
    apply. Row bodies, receipts and hashes are preserved. Dropping the column
    does not immediately reclaim its physical storage; a separately budgeted
    rewrite owns that operation. Rollback requires rebuilding derived data.
    """
    if store.search_plane != 'turbopuffer':
        raise ValueError('chunk vector retirement requires turbopuffer')
    if type(apply) is not bool:
        raise ValueError('chunk vector apply must be boolean')
    with store.connect() as connection, connection.transaction():
        if not apply:
            connection.execute('SET TRANSACTION READ ONLY')
        connection.execute("SET LOCAL lock_timeout='1s'")
        connection.execute("SET LOCAL statement_timeout='10s'")
        before = _vector_snapshot(connection)
        status = 'preview' if before['present'] else 'already_absent'
        if apply and before['present']:
            # RESTRICT alone still permits automatic deletion of same-table
            # indexes/checks/statistics. Freeze DDL and recheck every dependency.
            connection.execute('LOCK TABLE ONLY public.canonical_chunks IN ACCESS EXCLUSIVE MODE NOWAIT')
            if _vector_snapshot(connection) != before:
                raise ValueError('chunk vector catalog changed before retirement')
            connection.execute('ALTER TABLE ONLY public.canonical_chunks DROP COLUMN search_vector RESTRICT')
            if _vector_snapshot(connection)['present']:
                raise RuntimeError('chunk vector retirement incomplete')
            status = 'retired'
    return {'status': status, 'column': 'public.canonical_chunks.search_vector',
            'present_before': bool(before['present']),
            'physical_bytes_reclaimed': 0,
            'physical_reclaim_requires_separate_rewrite': True}
