"""One bounded full-parent record walker for locator and retirement proofs.

Consumers must exhaust it (including the final whole-parent hash) before using
any yielded metadata as write authority. It never opens a database connection.
"""
from __future__ import annotations

from dataclasses import replace
import hashlib
from io import BytesIO

from .canonical_text import MAX_CANONICAL_TEXT_BYTES
from .chunk_bodies import _VerifiedArchive, _check_deadline
from .evidence_projection import CanonicalEvidenceProjector
from .logical_evidence import LogicalEvidenceProjectionStore, MAX_PART_BYTES
from .passage_projection import decode_logical_record


class LogicalBodyProofError(ValueError):
    pass


def iter_parent_bodies(archive, *, tenant_id, source_id, native_parent_id,
                       manifest, parts, max_records, max_bytes, deadline_at=None):
    """Yield first record and bounded segments; None means an oversized body.

    Duplicate native identities are checked by each consumer's metadata index:
    the bounded locator planner uses a set, the bulk job uses a private SQLite
    unique key. Both use chunk_bodies._verified_body for exact current bodies.
    """
    if (manifest['tenant_id'] != tenant_id or manifest['source_id'] != source_id
            or manifest['native_parent_id'] != native_parent_id or not parts
            or len(parts) != manifest['part_count'] or type(manifest['record_count']) is not int
            or not 1 <= manifest['record_count'] <= max_records):
        raise LogicalBodyProofError('logical_body_catalog_invalid')
    if any(type(part['size_bytes']) is not int or not 0 < part['size_bytes'] <= MAX_PART_BYTES for part in parts):
        raise LogicalBodyProofError('logical_body_catalog_invalid')
    if sum(part['size_bytes'] for part in parts) > max_bytes:
        raise LogicalBodyProofError('logical_body_archive_budget_exceeded')
    projection = LogicalEvidenceProjectionStore(_VerifiedArchive(archive, deadline_at))
    digest = hashlib.sha256()
    ordinal, receipts, first, segments = 0, 0, None, []
    for part_number, part in enumerate(parts):
        if (part['tenant_id'] != tenant_id or part['source_id'] != source_id
                or part['logical_document_id'] != manifest['logical_document_id']
                or part['revision'] != manifest['revision'] or part['part_ordinal'] != part_number
                or part['first_record_ordinal'] != ordinal):
            raise LogicalBodyProofError('logical_body_catalog_invalid')
        payload = projection.read_part(CanonicalEvidenceProjector._reference(part), tenant_id=tenant_id, source_id=source_id)
        digest.update(payload)
        if not payload.endswith(b'\n'):
            raise LogicalBodyProofError('logical_body_part_invalid')
        part_receipts = 0
        for line in BytesIO(payload):
            _check_deadline(deadline_at)
            record = decode_logical_record(line, source_id=source_id)
            if record.ordinal != ordinal or ordinal >= max_records:
                raise LogicalBodyProofError('logical_body_part_invalid')
            ordinal += 1
            if first is None:
                if record.segment_ordinal != 0 or not record.receipts:
                    raise LogicalBodyProofError('logical_body_part_invalid')
                first, segment_index, body_bytes, oversized = record, 0, 0, False
            elif (record.event_native_id != first.event_native_id or record.event_kind != first.event_kind
                    or record.occurred_at != first.occurred_at or record.roles != first.roles
                    or record.actor_links != first.actor_links or record.receipts):
                raise LogicalBodyProofError('logical_body_part_invalid')
            if record.segment_ordinal != segment_index or record.segment_count != first.segment_count:
                raise LogicalBodyProofError('logical_body_part_invalid')
            segment_index += 1
            part_receipts += len(record.receipts)
            body_bytes += len(record.text.encode())
            if body_bytes > MAX_CANONICAL_TEXT_BYTES:
                oversized = True
                segments.clear()
                first = replace(first, text='')
            if not oversized:
                segments.append(record)
            if segment_index == first.segment_count:
                yield first, None if oversized else segments
                first, segments = None, []
            del record
        if ordinal - 1 != part['last_record_ordinal'] or part_receipts != part['receipt_count']:
            raise LogicalBodyProofError('logical_body_part_invalid')
        receipts += part_receipts
        del payload, line
    if (first is not None or ordinal != manifest['record_count'] or receipts != manifest['receipt_count']
            or digest.hexdigest() != manifest['document_content_sha256']):
        raise LogicalBodyProofError('logical_body_part_invalid')
    _check_deadline(deadline_at)
