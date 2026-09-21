"""Explicit optional access path for timestamp-ordered session context.

This operation is outside automatic migrations: index absence never changes
serving readiness or schema versions. Call with a dedicated operator connection.
"""
from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

INDEX_NAME = 'canonical_events_context_time_idx'
EXPECTED_KEYS = ('tenant_id', 'source_id', 'COALESCE(native_parent_id, native_id)',
                 'occurred_at', 'native_id')
EXPECTED_CLASSES = ('pg_catalog.text_ops', 'pg_catalog.text_ops', 'pg_catalog.text_ops',
                    'pg_catalog.timestamptz_ops', 'pg_catalog.text_ops')
CREATE_SQL = '''CREATE INDEX CONCURRENTLY canonical_events_context_time_idx
    ON public.canonical_events
       (tenant_id,source_id,(COALESCE(native_parent_id,native_id)),occurred_at,native_id)'''
INSPECT_SQL = '''SELECT index.oid AS index_oid,pg_relation_size(index.oid) AS index_bytes,
       index.relkind AS index_kind,table_ns.nspname AS table_schema,
       tab.relname AS table_name,am.amname AS method,ind.indisunique AS unique,
       ind.indpred IS NOT NULL AS partial,ind.indisvalid AS valid,ind.indisready AS ready,
       ind.indnkeyatts AS key_count,ind.indnatts AS attribute_count,
       ARRAY(SELECT pg_get_indexdef(index.oid,n,true)
             FROM generate_series(1,ind.indnkeyatts) AS n) AS keys,
       ind.indoption::int2[] AS options,ind.indcollation::oid[] AS collations,
       ARRAY(SELECT attr.attcollation
               FROM unnest(ARRAY['tenant_id','source_id','native_parent_id','occurred_at','native_id'])
                    WITH ORDINALITY AS wanted(name,n)
               JOIN pg_attribute attr ON attr.attrelid=tab.oid AND attr.attname=wanted.name
              ORDER BY wanted.n) AS declared_collations,
       ARRAY(SELECT opc_ns.nspname || '.' || opc.opcname
               FROM unnest(ind.indclass::oid[]) WITH ORDINALITY AS classes(oid,n)
               JOIN pg_opclass opc ON opc.oid=classes.oid
               JOIN pg_namespace opc_ns ON opc_ns.oid=opc.opcnamespace
              ORDER BY classes.n) AS operator_classes
  FROM pg_class index JOIN pg_namespace ns ON ns.oid=index.relnamespace
  LEFT JOIN pg_index ind ON ind.indexrelid=index.oid
  LEFT JOIN pg_class tab ON tab.oid=ind.indrelid
  LEFT JOIN pg_namespace table_ns ON table_ns.oid=tab.relnamespace
  LEFT JOIN pg_am am ON am.oid=index.relam
 WHERE ns.nspname='public' AND index.relname='canonical_events_context_time_idx' '''


def inspect_context_time_index(connection) -> dict:
    row = connection.execute(INSPECT_SQL).fetchone()
    if row is None:
        return {'status': 'absent'}
    compatible = (
        row['index_kind'] == 'i' and row['table_schema'] == 'public'
        and row['table_name'] == 'canonical_events' and row['method'] == 'btree'
        and row['unique'] is False and row['partial'] is False
        and row['key_count'] == row['attribute_count'] == 5
        and tuple(row['keys']) == EXPECTED_KEYS and list(row['options']) == [0]*5
        # pg_get_indexdef(index, column, true) omits collation/opclass detail.
        # Compare catalog identities too; a same-name but differently ordered
        # text index must never be accepted as this access path.
        and tuple(row['operator_classes']) == EXPECTED_CLASSES
        and len(row['declared_collations']) == 5
        and list(row['collations']) == list(row['declared_collations'])
        and row['declared_collations'][2] == row['declared_collations'][4]
    )
    return dict(status=('incompatible' if not compatible else
                        'ready' if row['valid'] and row['ready'] else 'invalid'),
                index_oid=row['index_oid'], index_bytes=row['index_bytes'],
                valid=bool(row['valid']), ready=bool(row['ready']))


def ensure_context_time_index(dsn: str, *, apply: bool = False,
                              timeout_seconds: int = 900) -> dict:
    """Inspect by default; optionally attempt one bounded concurrent build.

    An invalid or differently defined same-name index is an explicit refusal.
    No retry, replacement, cancellation of other sessions, or cleanup is
    implicit. A failure after the CREATE starts requires fresh inspection,
    even when no successful DDL acknowledgement reached this process.
    """
    if (not isinstance(dsn, str) or not dsn.strip() or type(apply) is not bool
            or type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 1800):
        raise ValueError('invalid context index operation limits')
    result = dict(action='inspect', ddl_attempted=False, ddl_acknowledged=False)
    phase = 'connect'
    # The small maintenance allocation bounds sort memory, not total index/WAL
    # scratch. The operator must separately admit disk/replica headroom.
    options = (f'-c default_transaction_read_only={"off" if apply else "on"} '
               f'-c statement_timeout={timeout_seconds*1000 if apply else 5000} '
               '-c lock_timeout=1000 -c maintenance_work_mem=64MB '
               '-c max_parallel_maintenance_workers=0 -c search_path=pg_catalog')
    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5,
                             row_factory=dict_row, options=options,
                             application_name='recall-context-time-index') as connection:
            phase = 'inspect'
            state = inspect_context_time_index(connection)
            if not apply:
                return dict(result, **state)
            if state['status'] == 'ready':
                return dict(result, **state, action='already_ready')
            if state['status'] != 'absent':
                return dict(result, **state, action='refused')
            phase = 'create'
            result.update(action='create', ddl_attempted=True)
            connection.execute(CREATE_SQL)
            result['ddl_acknowledged'] = True
            phase = 'verify'
            state = inspect_context_time_index(connection)
            if state['status'] != 'ready':
                return dict(result, **state, action='inspect_required')
            return dict(result, **state, action='created')
    except (Exception, KeyboardInterrupt) as error:
        return dict(result, status='inspect_required' if result['ddl_attempted'] else 'unavailable',
                    phase=phase, outcome='unknown' if result['ddl_attempted'] else 'not_attempted',
                    error_class=type(error).__name__)
