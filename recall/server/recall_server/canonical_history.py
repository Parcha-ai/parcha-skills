"""Restore outgoing revisions before their current-only archive proof can expire.

Preparation owns no connection during object reads. The writer must hold its
existing native advisory locks before restore_outgoing; a current-body clearer
must use those same locks and recheck is_current. Nothing here restores deletes.
"""
from __future__ import annotations

import hashlib
import os
import tempfile
import time
from contextlib import contextmanager
from itertools import groupby
from typing import Any

import orjson

from .canonical_text import MAX_CANONICAL_TEXT_BYTES
from .chunk_bodies import read_archived_chunks

EMPTY_SHA256 = hashlib.sha256(b'').hexdigest()
# Accommodates 1,000 maximum-size outgoing documents plus receipt metadata.
# Bodies are private disk staging, never a batch-sized in-memory dictionary.
MAX_STAGE_BYTES = 2 * 1_000 * MAX_CANONICAL_TEXT_BYTES
MIN_FREE_STAGE_BYTES = 64 * 1024 * 1024
READ_SECONDS = 20
READ_DOCUMENTS = 8  # 8 * maximum 8,000,000-byte document stays below 64MiB.


class HistoryUnavailable(RuntimeError):
    """Retry the entire write; no receipt or partial batch may be acknowledged."""


class HistoryAuthorityError(RuntimeError):
    pass


class StagedHistory:
    def __init__(self):
        self.file = None
        self.offsets = {}
        self.size = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if self.file is not None:
            self.file.close()

    @staticmethod
    def key(row):
        return row['tenant_id'], row['source_id'], row['document_id']

    def add(self, row, chunks):
        bodies = [chunk['text_redacted'].encode() for chunk in chunks]
        size = sum(map(len, bodies))
        digest = hashlib.sha256()
        for body in bodies:
            digest.update(body)
        if size > MAX_CANONICAL_TEXT_BYTES or digest.hexdigest() != row['text_sha256']:
            raise HistoryUnavailable()
        header = {key: row[key] for key in ('tenant_id', 'source_id', 'document_id', 'revision', 'text_sha256')}
        header['chunks'] = [dict(ordinal=chunk['ordinal'], receipt=chunk['receipt'], size=len(body),
                                 text_sha256=hashlib.sha256(body).hexdigest())
                            for chunk, body in zip(chunks, bodies, strict=True)]
        encoded = orjson.dumps(header)
        if len(encoded) > MAX_CANONICAL_TEXT_BYTES or self.size + len(encoded) + size > MAX_STAGE_BYTES:
            raise HistoryUnavailable()
        if self.file is None:
            self.file = tempfile.TemporaryFile(mode='w+b', prefix='recall-history-')
        available = os.fstatvfs(self.file.fileno())
        if available.f_bavail * available.f_frsize < len(encoded) + size + MIN_FREE_STAGE_BYTES:
            raise HistoryUnavailable()
        offset = self.file.tell()
        self.file.write(encoded)
        for body in bodies:
            self.file.write(body)
        self.offsets[self.key(row)] = (offset, len(encoded))
        self.size += len(encoded) + size

    def read(self, row):
        try:
            offset, length = self.offsets[self.key(row)]
            self.file.seek(offset)
            header = orjson.loads(self.file.read(length))
            if any(header[key] != row[key] for key in ('tenant_id', 'source_id', 'document_id', 'revision', 'text_sha256')):
                raise HistoryUnavailable()
            chunks = []
            digest = hashlib.sha256()
            for chunk in header['chunks']:
                body = self.file.read(chunk['size'])
                if len(body) != chunk['size'] or hashlib.sha256(body).hexdigest() != chunk['text_sha256']:
                    raise HistoryUnavailable()
                digest.update(body)
                chunks.append(dict(ordinal=chunk['ordinal'], receipt=chunk['receipt'], text_redacted=body.decode()))
            if digest.hexdigest() != row['text_sha256']:
                raise HistoryUnavailable()
            return chunks
        except (KeyError, OSError, ValueError, AttributeError):
            raise HistoryUnavailable() from None


def _stage_history_rows(store, archive, staged, *, tenant_id, rows, deadline_at=None):
    # Share immutable parts within each parent and retain only eight documents
    # in memory; the complete request stays on the existing private spool.
    for _, group in groupby(rows, lambda row: (row['source_id'], row['native_parent_id'])):
        grouped = list(group)
        for offset in range(0, len(grouped), READ_DOCUMENTS):
            batch = grouped[offset:offset + READ_DOCUMENTS]
            read_deadline = time.monotonic() + READ_SECONDS
            if deadline_at is not None:
                read_deadline = min(read_deadline, deadline_at)
            restored = read_archived_chunks(store, archive, tenant_id=tenant_id,
                source_ids=(batch[0]['source_id'],),
                document_ids=tuple(row['document_id'] for row in batch), deadline_at=read_deadline)
            for row in batch:
                chunks = restored.get((row['source_id'], row['document_id']))
                if chunks is None:
                    raise HistoryUnavailable()
                staged.add(row, chunks)
            del chunks, restored


@contextmanager
def stage_history_rows(store, archive, *, tenant_id, rows, deadline_at):
    """Stage already-authorized exact documents without retaining a SQL lease."""
    with StagedHistory() as staged:
        try:
            _stage_history_rows(store, archive, staged, tenant_id=tenant_id,
                                rows=rows, deadline_at=deadline_at)
        except Exception:
            raise HistoryUnavailable() from None
        yield staged


@contextmanager
def prepare_history(store, archive, *, tenant_id: str, candidates: list[dict[str, Any]]):
    """Stage only missing current bodies that a genuinely new revision replaces."""
    with StagedHistory() as staged:
        if not candidates:
            yield staged
            return
        encoded = orjson.dumps(candidates).decode()
        profile = ('document.chunk_receipt_profile' if getattr(store, 'verified_singleton_receipts', False)
                   else 'NULL::smallint')
        try:
            with store.connect() as connection:
                owners = connection.execute('''
                    SELECT source.source_id,source.owner_principal_id
                      FROM canonical_sources source
                     WHERE source.tenant_id=%s AND source.source_id=ANY(%s)
                ''', (tenant_id, sorted({item['source_id'] for item in candidates}))).fetchall()
                owners = {row['source_id']: row['owner_principal_id'] for row in owners}
                if any(item['source_id'] in owners and owners[item['source_id']] != item['principal_id']
                       for item in candidates):
                    raise HistoryAuthorityError()
                rows = connection.execute(f'''
                    SELECT DISTINCT document.tenant_id,document.source_id,document.document_id,
                           document.revision,document.text_sha256,
                           COALESCE(event.native_parent_id,event.native_id) AS native_parent_id
                      FROM jsonb_to_recordset(%s::jsonb) incoming(
                          source_id text,native_id text,content_sha256 text,principal_id text)
                      JOIN canonical_documents document ON document.tenant_id=%s
                       AND document.source_id=incoming.source_id AND document.native_id=incoming.native_id
                      JOIN canonical_events event ON event.tenant_id=document.tenant_id
                       AND event.source_id=document.source_id AND event.event_id=document.event_id
                     WHERE document.is_current AND document.deleted_at IS NULL
                       AND NOT EXISTS (SELECT 1 FROM canonical_events replay
                            WHERE replay.tenant_id=document.tenant_id AND replay.source_id=document.source_id
                              AND replay.native_id=document.native_id AND replay.content_sha256=incoming.content_sha256)
                       AND ({profile} IS NOT NULL OR EXISTS (SELECT 1 FROM canonical_chunks chunk
                            WHERE chunk.tenant_id=document.tenant_id AND chunk.source_id=document.source_id
                              AND chunk.document_id=document.document_id AND chunk.deleted_at IS NULL
                              AND chunk.text_redacted='' AND chunk.text_sha256<>%s))
                     ORDER BY document.source_id,native_parent_id,document.document_id
                ''', (encoded, tenant_id, EMPTY_SHA256)).fetchall()
            if rows and archive is None:
                raise HistoryUnavailable()
            _stage_history_rows(store, archive, staged, tenant_id=tenant_id, rows=rows)
        except HistoryAuthorityError:
            raise
        except Exception:
            raise HistoryUnavailable() from None
        yield staged


def restore_outgoing(connection, staged, *, tenant_id, source_id, native_ids, store=None, deadline_at=None):
    """Called after replay detection, under the writer's native advisory locks."""
    def execute(sql, parameters):
        if deadline_at is not None:
            return store._execute_bounded(connection, sql, parameters, deadline_at)
        return connection.execute(sql, parameters)

    profile = ('document.chunk_receipt_profile' if getattr(store, 'verified_singleton_receipts', False)
               else 'NULL::smallint')
    rows = execute(f'''
        SELECT document.tenant_id,document.source_id,document.document_id,
               document.revision,document.text_sha256,document.native_id,document.created_at,
               {profile} AS chunk_receipt_profile
          FROM canonical_documents document
         WHERE document.tenant_id=%s AND document.source_id=%s AND document.native_id=ANY(%s)
           AND document.is_current AND document.deleted_at IS NULL
           AND ({profile} IS NOT NULL OR EXISTS (SELECT 1 FROM canonical_chunks chunk
                WHERE chunk.tenant_id=document.tenant_id AND chunk.source_id=document.source_id
                  AND chunk.document_id=document.document_id AND chunk.deleted_at IS NULL
                  AND chunk.text_redacted='' AND chunk.text_sha256<>%s))
         ORDER BY document.document_id FOR UPDATE
    ''', (tenant_id, source_id, native_ids, EMPTY_SHA256)).fetchall()
    for row in rows:
        if row['chunk_receipt_profile'] is not None:
            from .receipt_membership import lookup_receipt_metadata
            from .projectors import item_receipt

            receipt = item_receipt(source_id, row['native_id'], row['revision'], 0)
            member = lookup_receipt_metadata(store, tenant_id=tenant_id,
                authorized_sources=(source_id,), receipts=(receipt,),
                allow_verified_singletons=True, connection=connection, deadline_at=deadline_at).get(receipt)
            if member is None or member['document_id'] != row['document_id'] or staged is None:
                raise HistoryUnavailable()
            restored = staged.read(row)
            if (len(restored) != 1 or restored[0]['ordinal'] != 0
                    or restored[0]['receipt'] != receipt
                    or hashlib.sha256(restored[0]['text_redacted'].encode()).hexdigest() != member['text_sha256']):
                raise HistoryUnavailable()
            execute('''INSERT INTO canonical_chunks(
                tenant_id,source_id,chunk_id,document_id,ordinal,receipt,text_redacted,text_sha256,created_at)
                VALUES (%s,%s,%s,%s,0,%s,%s,%s,%s)''',
                (tenant_id, source_id, member['chunk_id'], row['document_id'], receipt,
                 restored[0]['text_redacted'], member['text_sha256'], row['created_at']))
            changed = execute('''UPDATE canonical_documents SET chunk_receipt_profile=NULL
                WHERE tenant_id=%s AND source_id=%s AND document_id=%s AND chunk_receipt_profile=1''',
                (tenant_id, source_id, row['document_id']))
            if changed.rowcount != 1:
                raise HistoryUnavailable()
        chunks = execute('''
            SELECT ordinal,receipt,text_sha256,
                   (text_redacted='' AND text_sha256<>%s) AS missing
              FROM canonical_chunks
             WHERE tenant_id=%s AND source_id=%s AND document_id=%s AND deleted_at IS NULL
             ORDER BY ordinal FOR UPDATE
        ''', (EMPTY_SHA256, tenant_id, source_id, row['document_id'])).fetchall()
        if not any(chunk['missing'] for chunk in chunks):
            continue
        if staged is None:
            raise HistoryUnavailable()
        restored = staged.read(row)
        if len(restored) != len(chunks) or any(
            candidate['ordinal'] != chunk['ordinal'] or candidate['receipt'] != chunk['receipt']
            or hashlib.sha256(candidate['text_redacted'].encode()).hexdigest() != chunk['text_sha256']
            for candidate, chunk in zip(restored, chunks, strict=True)
        ):
            raise HistoryUnavailable()
        payload = [{**candidate, 'text_sha256': chunk['text_sha256']}
                   for candidate, chunk in zip(restored, chunks, strict=True) if chunk['missing']]
        updated = execute('''
            UPDATE canonical_chunks chunk SET text_redacted=restored.text_redacted
              FROM jsonb_to_recordset(%s::jsonb) restored(
                  ordinal integer,receipt text,text_redacted text,text_sha256 text)
             WHERE chunk.tenant_id=%s AND chunk.source_id=%s AND chunk.document_id=%s
               AND chunk.ordinal=restored.ordinal AND chunk.receipt=restored.receipt
               AND chunk.text_sha256=restored.text_sha256 AND chunk.deleted_at IS NULL
        ''', (orjson.dumps(payload).decode(), tenant_id, source_id, row['document_id']))
        if updated.rowcount != len(payload):
            raise HistoryUnavailable()
