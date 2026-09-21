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
