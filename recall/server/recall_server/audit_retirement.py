"""Bounded, recoverable retirement of ordinary diagnostic ingest-success rows.

This owner is operator-only. It never alters canonical identity or ACK authority,
creates permanent tables, runs VACUUM, or removes its immutable recovery objects.
"""
from contextlib import contextmanager
import csv
from dataclasses import asdict
from datetime import datetime, timezone
import gzip
import hashlib
import io

from psycopg import sql

from .archive import ArchiveRequest, ArtifactReference
from .retirement_campaign import _check, digest

COLUMNS = ('tenant_id', 'source_id', 'audit_id', 'operation', 'status',
           'subject_sha256', 'item_count', 'byte_count', 'duration_ms', 'created_at')
COLUMN_SQL = sql.SQL(',').join(map(sql.Identifier, COLUMNS))
MAX_ROWS = 50_000
MAX_BYTES = 32 * 1024 * 1024
CONTRACT = 'recall.ingest-audit-retirement.v1'
COLUMN_TYPES = ('text', 'text', 'text', 'text', 'text', 'character(64)',
                'integer', 'bigint', 'double precision', 'timestamp with time zone')


def _cutoff(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    _check(parsed.tzinfo is not None, 'cutoff_timezone_required')
    return parsed.astimezone(timezone.utc)


def _hash(data):
    return hashlib.sha256(data).hexdigest()


def _copy(connection, query, params=()):
    output = bytearray()
    command = sql.SQL('COPY ({}) TO STDOUT WITH (FORMAT CSV)').format(query)
    with connection.cursor().copy(command, params) as stream:
        for block in stream:
            output.extend(block)
            _check(len(output) <= MAX_BYTES, 'page_byte_budget')
    return bytes(output)


def _select(tenant, source, keys):
    return (sql.SQL('SELECT {} FROM canonical_audit_events WHERE tenant_id=%s '
                    'AND source_id=%s AND audit_id=ANY(%s) ORDER BY audit_id').format(COLUMN_SQL),
            (tenant, source, keys))


class AuditRetirement:
    def __init__(self, store, archive):
        self.store, self.archive = store, archive

    @contextmanager
    def _connection(self, *, snapshot=False):
        with self.store.connect() as connection:
            if snapshot:
                connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            connection.execute("SET LOCAL timezone='UTC'")
            connection.execute("SET LOCAL datestyle='ISO, YMD'")
            connection.execute('SET LOCAL extra_float_digits=3')
            connection.execute("SET LOCAL lock_timeout='1s'")
            connection.execute("SET LOCAL statement_timeout='15s'")
            # Hold a relation lock before inspecting the catalog. Concurrent DDL
            # cannot add deletion side effects after this check and before mutation.
            connection.execute('LOCK TABLE canonical_audit_events IN ' +
                               ('ACCESS SHARE' if snapshot else 'ROW EXCLUSIVE') + ' MODE')
            columns = connection.execute("""SELECT attname,format_type(atttypid,atttypmod) AS type
                FROM pg_attribute WHERE attrelid='canonical_audit_events'::regclass
                AND attnum>0 AND NOT attisdropped ORDER BY attnum""").fetchall()
            _check([(r['attname'], r['type']) for r in columns] == list(zip(COLUMNS, COLUMN_TYPES)),
                   'audit_schema_changed')
            side_effects = connection.execute("""SELECT
                EXISTS(SELECT 1 FROM pg_constraint WHERE contype='f'
                       AND confrelid='canonical_audit_events'::regclass) OR
                EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='canonical_audit_events'::regclass
                       AND NOT tgisinternal AND tgenabled<>'D') OR
                EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class='canonical_audit_events'::regclass)
                AS present""").fetchone()
            _check(not side_effects['present'], 'audit_delete_side_effects')
            yield connection

    def archive_page(self, *, tenant_id, source_id, cutoff, after=None, limit=1000):
        _check(type(limit) is int and 1 <= limit <= MAX_ROWS, 'page_row_budget')
        cutoff = _cutoff(cutoff).isoformat()
        with self._connection(snapshot=True) as connection:
            _check(after is None or isinstance(after, str), 'page_cursor')
            predicate = '' if after is None else ' AND audit_id>%s'
            params = (tenant_id, source_id) + (() if after is None else (after,)) + (limit,)
            rows = connection.execute('SELECT audit_id FROM canonical_audit_events '
                'WHERE tenant_id=%s AND source_id=%s' + predicate +
                ' ORDER BY audit_id LIMIT %s', params).fetchall()
            keys = [row['audit_id'] for row in rows]
            # Bound discovery first. Filtering before LIMIT can scan an entire sparse source.
            # Filter while copying, avoiding a second heap pass over the same rows.
            query = sql.SQL('''SELECT {} FROM canonical_audit_events
                WHERE tenant_id=%s AND source_id=%s AND audit_id=ANY(%s)
                AND operation='ingest.commit' AND status='success' AND created_at<%s
                ORDER BY audit_id''').format(COLUMN_SQL)
            raw = _copy(connection, query, (tenant_id, source_id, keys, cutoff))
        row_count = sum(1 for _ in csv.reader(io.StringIO(raw.decode('utf-8'), newline='')))
        # No database transaction remains open across object I/O.
        receipt = dict(contract=CONTRACT, tenant_id=tenant_id, source_id=source_id,
                       cutoff=cutoff, after=after, next_after=keys[-1] if keys else after,
                       scanned=len(keys), exhausted=len(keys) < limit, row_count=row_count,
                       raw_bytes=len(raw), raw_sha256=_hash(raw), artifact=None)
        if row_count:
            request = ArchiveRequest(tenant_id, source_id, 'ingest-audits:' + _hash(raw),
                                     'application/gzip', gzip.compress(raw, mtime=0))
            reference = self.archive.put(request)
            _check(self.archive.read(reference, tenant_id=tenant_id, source_id=source_id)
                   == request.payload, 'archive_readback')
            receipt['artifact'] = asdict(reference)
        return dict(receipt, sha256=digest(receipt))

    def _load(self, receipt):
        unsigned = {k: v for k, v in receipt.items() if k != 'sha256'}
        _check(receipt.get('sha256') == digest(unsigned) and receipt.get('contract') == CONTRACT,
               'receipt_digest')
        _check(type(receipt['row_count']) is int and 0 <= receipt['row_count'] <= MAX_ROWS,
               'page_row_budget')
        _check(0 <= receipt['raw_bytes'] <= MAX_BYTES, 'page_byte_budget')
        raw = b''
        if receipt['row_count']:
            reference = ArtifactReference(**receipt['artifact'])
            _check(reference.size_bytes <= MAX_BYTES + 65536, 'archive_byte_budget')
            packed = self.archive.read(reference, tenant_id=receipt['tenant_id'],
                                       source_id=receipt['source_id'])
            with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:
                raw = stream.read(MAX_BYTES + 1)
                _check(len(raw) <= MAX_BYTES and not stream.read(1), 'page_byte_budget')
        _check(len(raw) == receipt['raw_bytes'] and _hash(raw) == receipt['raw_sha256'],
               'archive_page_digest')
        rows = list(csv.reader(io.StringIO(raw.decode('utf-8'), newline='')))
        _check(len(rows) == receipt['row_count'], 'archive_row_count')
        cutoff = _cutoff(receipt['cutoff'])
        keys = []
        for row in rows:
            _check(len(row) == len(COLUMNS) and row[0] == receipt['tenant_id']
                   and row[1] == receipt['source_id'] and row[3:5] == ['ingest.commit', 'success']
                   and _cutoff(row[9]) < cutoff, 'archive_row_scope_or_policy')
            keys.append(row[2])
        _check(len(set(keys)) == len(keys), 'archive_duplicate_keys')
        return raw, keys

    def inspect(self, receipt):
        raw, keys = self._load(receipt)
        with self._connection(snapshot=True) as connection:
            query, params = _select(receipt['tenant_id'], receipt['source_id'], keys)
            current = _copy(connection, query, params)
        count = sum(1 for _ in csv.reader(io.StringIO(current.decode(), newline='')))
        status = 'present' if current == raw else 'absent' if not current else 'changed_or_partial'
        return dict(status=status, rows=count, expected=len(keys))

    def clear(self, receipt):
        raw, keys = self._load(receipt)
        with self._connection() as connection:
            tenant, source = receipt['tenant_id'], receipt['source_id']
            locked = connection.execute('''SELECT audit_id FROM canonical_audit_events
                WHERE tenant_id=%s AND source_id=%s AND audit_id=ANY(%s)
                ORDER BY audit_id FOR UPDATE''', (tenant, source, keys)).fetchall()
            if not locked:
                return dict(status='absent', rows=0)
            query, params = _select(tenant, source, keys)
            _check(_copy(connection, query, params) == raw, 'audit_rows_changed_or_partial')
            removed = connection.execute('''DELETE FROM canonical_audit_events
                WHERE tenant_id=%s AND source_id=%s AND audit_id=ANY(%s)
                AND operation='ingest.commit' AND status='success' AND created_at<%s
                RETURNING audit_id''', (tenant, source, keys, receipt['cutoff'])).fetchall()
            _check({r['audit_id'] for r in removed} == set(keys), 'audit_clear_membership')
        return dict(status='cleared', rows=len(keys))

    def restore(self, receipt):
        raw, keys = self._load(receipt)
        with self._connection() as connection:
            connection.execute('CREATE TEMP TABLE _recall_audit_restore '
                               '(LIKE canonical_audit_events INCLUDING CONSTRAINTS) ON COMMIT DROP')
            with connection.cursor().copy(sql.SQL('COPY pg_temp._recall_audit_restore ({}) '
                                                   'FROM STDIN WITH (FORMAT CSV)').format(COLUMN_SQL)) as stream:
                stream.write(raw)
            restored = connection.execute(sql.SQL('''INSERT INTO canonical_audit_events ({})
                SELECT {} FROM pg_temp._recall_audit_restore ORDER BY audit_id
                ON CONFLICT (tenant_id,source_id,audit_id) DO NOTHING RETURNING audit_id''').format(
                    COLUMN_SQL, COLUMN_SQL)).fetchall()
            connection.execute('''SELECT audit_id FROM canonical_audit_events
                WHERE tenant_id=%s AND source_id=%s AND audit_id=ANY(%s)
                ORDER BY audit_id FOR UPDATE''', (receipt['tenant_id'], receipt['source_id'], keys)).fetchall()
            query, params = _select(receipt['tenant_id'], receipt['source_id'], keys)
            _check(_copy(connection, query, params) == raw, 'audit_restore_conflict')
        return dict(status='restored', rows=len(restored))
