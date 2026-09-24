"""Explicit, source-scoped metadata repair for already projected native sessions.

Read existing immutable records and update only nullable catalog identity. No
archive writes, ingest, passage projection, search reindex, or schema migration.
The proof is the first verified native header and its current canonical receipts,
not a whole-session audit. Each part read is hash-verified; unread tails are not.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os

from contracts.native_conversation import NativeConversationConflict, projected_conversation
from .logical_evidence import LogicalEvidenceError
from .logical_evidence_projection import CanonicalLogicalEvidenceProjector, MAX_RESTORED_RECORD_BYTES
from .passage_projection import decode_logical_record
from .projectors import SOURCE_ID_RE


class NativeConversationRepair:
    def __init__(self, store, projection):
        self.store = store
        self.projection = projection

    def _records(self, candidate, metrics):
        with self.store.connect() as connection:
            parts = connection.execute('''SELECT * FROM canonical_evidence_document_parts
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s AND revision=%s
                ORDER BY part_ordinal''', (candidate['tenant_id'], candidate['source_id'],
                    candidate['logical_document_id'], candidate['revision'])).fetchall()
        expected = 0
        for ordinal, part in enumerate(parts):
            if part['part_ordinal'] != ordinal or part['first_record_ordinal'] != expected:
                raise LogicalEvidenceError('logical_evidence_part_invalid')
            payload = self.projection.read_part(
                CanonicalLogicalEvidenceProjector._reference(part),
                tenant_id=candidate['tenant_id'], source_id=candidate['source_id'])
            metrics['archive_bytes_read'] += len(payload)
            if len(payload) != part['size_bytes'] or not payload.endswith(b'\n'):
                raise LogicalEvidenceError('logical_evidence_corrupt')
            for line in io.BytesIO(payload):
                record = decode_logical_record(line, source_id=candidate['source_id'])
                if record.ordinal != expected:
                    raise LogicalEvidenceError('logical_evidence_record_order_invalid')
                expected += 1
                yield record
        if expected != candidate['record_count'] or len(parts) != candidate['part_count']:
            raise LogicalEvidenceError('logical_evidence_part_invalid')

    def _identity(self, candidate, harness, metrics):
        records = iter(self._records(candidate, metrics))
        for first in records:
            if first.segment_ordinal != 0:
                raise LogicalEvidenceError('logical_evidence_record_order_invalid')
            pieces = [first.text]
            size = len(first.text.encode())
            for index in range(1, first.segment_count):
                following = next(records, None)
                if (following is None or following.event_native_id != first.event_native_id
                        or following.segment_ordinal != index
                        or following.segment_count != first.segment_count or following.receipts):
                    raise LogicalEvidenceError('logical_evidence_record_order_invalid')
                size += len(following.text.encode())
                if size > MAX_RESTORED_RECORD_BYTES:
                    raise LogicalEvidenceError('logical_evidence_source_integrity_invalid')
                pieces.append(following.text)
            text = ''.join(pieces)
            try:
                content = json.loads(text)
            except (ValueError, TypeError):
                continue
            with self.store.connect() as connection:
                row = connection.execute('''SELECT document.document_id,document.text_sha256,
                        event.canonical_redacted->'provenance' AS provenance
                    FROM canonical_events event JOIN canonical_documents document
                      USING(tenant_id,source_id,event_id)
                    WHERE event.tenant_id=%s AND event.source_id=%s AND event.native_id=%s
                      AND coalesce(event.native_parent_id,event.native_id)=%s
                      AND document.is_current AND document.deleted_at IS NULL''',
                    (candidate['tenant_id'], candidate['source_id'], first.event_native_id,
                     candidate['native_parent_id'])).fetchone()
            if row is None or row['text_sha256'] != hashlib.sha256(text.encode()).hexdigest():
                return None, None
            provenance = row['provenance'] or {}
            if provenance.get('harness') != harness:
                return None, None
            try:
                identity = projected_conversation(content, provenance)
            except NativeConversationConflict:
                return None, None
            if identity is not None:
                return identity, dict(document_id=row['document_id'], text_sha256=row['text_sha256'],
                                      receipts=list(first.receipts))
            if isinstance(content, dict) and (
                    (harness == 'codex' and content.get('type') == 'session_meta')
                    or (harness == 'claude' and 'sessionId' in content)):
                # A malformed original identity stays unknown; later records
                # must not manufacture a replacement session for this header.
                return None, None
        return None, None

    def _apply(self, candidate, identity, proof):
        # Lock the current header and its exact live receipts before updating
        # metadata. Forget waits for this short transaction or wins first and
        # makes this proof unavailable. No archive access holds these locks.
        with self.store.connect() as connection:
            with connection.transaction():
                rows = connection.execute('''SELECT chunk.receipt
                    FROM canonical_chunks chunk JOIN canonical_documents document
                      USING(tenant_id,source_id,document_id)
                    JOIN canonical_events event USING(tenant_id,source_id,event_id)
                    WHERE chunk.tenant_id=%s AND chunk.source_id=%s
                      AND document.document_id=%s AND document.text_sha256=%s
                      AND document.is_current AND document.deleted_at IS NULL
                      AND coalesce(event.native_parent_id,event.native_id)=%s
                      AND chunk.receipt=ANY(%s) AND chunk.deleted_at IS NULL
                    ORDER BY chunk.chunk_id FOR SHARE OF chunk,document,event''',
                    (candidate['tenant_id'], candidate['source_id'], proof['document_id'],
                     proof['text_sha256'], candidate['native_parent_id'], proof['receipts'])).fetchall()
                if not proof['receipts'] or {row['receipt'] for row in rows} != set(proof['receipts']):
                    return False
                result = connection.execute('''UPDATE canonical_evidence_documents evidence
                    SET conversation_id=%s,conversation_strand_id=%s
                    WHERE evidence.tenant_id=%s AND evidence.source_id=%s
                      AND evidence.logical_document_id=%s AND evidence.revision=%s
                      AND evidence.native_parent_id=%s
                      AND (evidence.manifest_artifact_id,evidence.manifest_storage_backend,
                           evidence.manifest_object_key,evidence.manifest_content_sha256,
                           evidence.manifest_size_bytes,evidence.manifest_media_type,
                           evidence.manifest_encryption,evidence.manifest_version_id,
                           evidence.document_content_sha256)
                          IS NOT DISTINCT FROM (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                      AND evidence.conversation_id IS NULL
                      AND NOT EXISTS (SELECT 1 FROM canonical_evidence_document_queue queue
                        WHERE queue.tenant_id=evidence.tenant_id AND queue.source_id=evidence.source_id
                          AND queue.native_parent_id=evidence.native_parent_id AND queue.reason='forget')''',
                    (identity.conversation_id, identity.strand_id, candidate['tenant_id'],
                     candidate['source_id'], candidate['logical_document_id'], candidate['revision'],
                     candidate['native_parent_id'], candidate['manifest_artifact_id'], candidate['manifest_storage_backend'],
                     candidate['manifest_object_key'], candidate['manifest_content_sha256'],
                     candidate['manifest_size_bytes'], candidate['manifest_media_type'],
                     candidate['manifest_encryption'], candidate['manifest_version_id'],
                     candidate['document_content_sha256']))
                return result.rowcount == 1

    def run(self, *, tenant_id, source_ids, harness, apply=False, limit=100, after=None):
        if (not isinstance(tenant_id, str) or not tenant_id or not source_ids
                or any(not isinstance(source, str) or not SOURCE_ID_RE.fullmatch(source) for source in source_ids)
                or harness not in {'claude', 'codex'} or type(apply) is not bool
                or type(limit) is not int or limit < 1
                or (after is not None and (not isinstance(after, (list, tuple)) or len(after) != 2
                    or any(not isinstance(value, str) or not value for value in after)))):
            raise ValueError('invalid native metadata repair scope')
        after_source, after_document = after or ('', '')
        with self.store.connect() as connection:
            rows = connection.execute('''SELECT * FROM canonical_evidence_documents
                WHERE tenant_id=%s AND source_id=ANY(%s) AND conversation_id IS NULL
                  AND (source_id,logical_document_id)>(%s,%s)
                ORDER BY source_id,logical_document_id LIMIT %s''',
                (tenant_id, sorted(set(source_ids)), after_source, after_document, limit + 1)).fetchall()
        selected = rows[:limit]
        result = dict(selected=len(selected), would_update=0, updated=0, unknown=0,
                      unavailable=0, raced=0, archive_bytes_read=0, scope_exhausted=len(rows) <= limit,
                      next_after=[selected[-1]['source_id'], selected[-1]['logical_document_id']] if selected else after,
                      apply=apply)
        for candidate in selected:
            try:
                identity, proof = self._identity(candidate, harness, result)
            except LogicalEvidenceError:
                result['unavailable'] += 1
                continue
            if identity is None:
                result['unknown'] += 1
                continue
            result['would_update'] += 1
            if apply:
                # Commit failures propagate: an unknown commit is not a refusal
                # and must not be reported as an acknowledged zero-write result.
                result['updated' if self._apply(candidate, identity, proof) else 'raced'] += 1
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--source', action='append', required=True)
    parser.add_argument('--harness', choices=('claude', 'codex'), required=True)
    parser.add_argument('--limit', type=int, default=100, help='Documents per resumable batch; no corpus cap')
    parser.add_argument('--after', nargs=2, metavar=('SOURCE_ID', 'LOGICAL_DOCUMENT_ID'))
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    from .archive_runtime import build_evidence_archive_store
    from .db import BrainStore
    from .logical_evidence import LogicalEvidenceProjectionStore
    store = None
    try:
        store = BrainStore(os.environ['RECALL_DATABASE_URL'])
        result = NativeConversationRepair(store, LogicalEvidenceProjectionStore(build_evidence_archive_store())).run(
            tenant_id=args.tenant, source_ids=args.source, harness=args.harness, apply=args.apply,
            limit=args.limit, after=args.after)
        print(json.dumps(dict(status='ok', **result), sort_keys=True))
    except Exception as error:
        print(json.dumps(dict(status='failed', error_class=type(error).__name__,
                              write_outcome='unknown' if args.apply else 'not_attempted')))
        raise SystemExit(1) from None
    finally:
        if store is not None:
            store._pool.close()


if __name__ == '__main__':
    main()
