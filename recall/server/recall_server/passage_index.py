"""Projection of complete logical documents into lossless retrieval passages."""

from __future__ import annotations

import hashlib
import logging
import time
import json
import math
import pickle
import tempfile
import threading
from array import array
from collections import Counter
from collections.abc import Sequence
from contextlib import ExitStack, contextmanager, nullcontext
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from datetime import datetime
from io import BytesIO
from itertools import islice
from typing import Any, Callable, ClassVar, Iterable, Iterator
from types import SimpleNamespace
from weakref import finalize

import psycopg

from . import parent_membership_enrollment, passage_append, membership_remainder
from .tool_passage_projection import extract_codex_tool_event_groups, extract_tool_event_groups, tool_evidence_context
from .logical_evidence import (
    LogicalEvidenceError,
    LogicalEvidenceProjectionStore,
)
from .passage_projection import (
    MAX_PASSAGE_HEADER_BYTES,
    COMPLETE_SOURCE_UNITS,
    COMPLETE_REPORT_UNIT,
    LOGICAL_DOCUMENT_ID_RE,
    PASSAGE_EMBEDDING_SEPARATOR,
    LosslessPassage,
    PassagePolicy,
    PassageSpan,
    reconstruct_passage,
    visible_message_text,
    canonical_spans_json,
    decode_logical_record,
    passage_embed_sha256,
    passage_embedding_input,
    render_passage_header,
    iter_visible_messages,
    iter_passages,
    _logical_record_groups,
    _text_blocks,
)
from .passage_representations import ActorContext, DocumentContext
from .search_outbox import (
    enqueue_search_outbox,
    outbox_months,
    write_search_tombstones,
)


LOG = logging.getLogger(__name__)


class _PassageRecordReplay:
    """Replay only this attempt's validated records from its private file."""

    def __init__(self, stream):
        self.stream = stream
        self.end = stream.tell()

    def __iter__(self):
        self.stream.seek(0)
        while self.stream.tell() < self.end:
            yield pickle.load(self.stream)


class _DiskSequence(Sequence):
    """Attempt-local payloads with compact offsets, never external pickle input.

    Payloads are replayed one at a time. Offsets and selected-row indexes still
    scale with row count; a single decoded event/window is not size bounded.
    """

    def __init__(self):
        self.stream = tempfile.TemporaryFile(mode="w+b")
        self._finalizer = finalize(self, self.stream.close)
        self.offsets = array('Q')

    def extend(self, values):
        for value in values:
            self.stream.seek(0, 2)
            offset = self.stream.tell()
            pickle.dump(value, self.stream, protocol=5)
            self.offsets.append(offset)

    def __len__(self):
        return len(self.offsets)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return _DiskView(self, range(len(self))[index])
        self.stream.seek(self.offsets[index])
        return pickle.load(self.stream)

    def __eq__(self, other):
        return isinstance(other, Sequence) and len(self) == len(other) and all(
            left == right for left, right in zip(self, other))

    def close(self):
        self._finalizer()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class _DiskView(Sequence):
    __eq__ = _DiskSequence.__eq__

    def __init__(self, source, indexes):
        self.source, self.indexes = source, indexes

    def __len__(self):
        return len(self.indexes)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return _DiskView(self.source, self.indexes[index])
        return self.source[self.indexes[index]]


def _handback_owned_by_visible_message(group, records):
    """Check whether the complete native report already owns this exact field."""
    if group.evidence_kind != 'tool_input' or len(group.messages) != 1:
        return False
    message = group.messages[0]
    if message.source_unit != 'claude.tool-input.string.v1#' + group.enclosing_pointer + '/input/message':
        return False
    # The group and pointer were resolved by the unchanged typed extractor.
    event = json.loads(group.full_event_text)
    block = event['message']['content'][int(group.enclosing_pointer.rsplit('/', 1)[1])]
    arguments = block.get('input')
    if (block.get('name') != 'SubagentHandback' or not isinstance(arguments, dict)
        or set(arguments) != {'message'} or arguments['message'] != message.text):
        return False
    visible = tuple(iter_visible_messages(records, first_record_ordinal=records[0].ordinal))
    return len(visible) == 1 and (
        visible[0].source_unit == COMPLETE_REPORT_UNIT
        and visible[0].record_ordinal == message.record_ordinal
        and visible[0].record_count == message.record_count
        and visible[0].receipts == message.receipts
        and visible[0].text == message.text
    )


def _spool_tool_groups(records, extractor, output, *, first_record_ordinal=0):
    # Call the unchanged extractor on each complete validated event. Both native
    # adapters' coverage counters are additive; no provider block is split.
    _, coverage = extractor(())
    dispositions = Counter()
    for records_group in _logical_record_groups(records, first_record_ordinal=first_record_ordinal):
        groups, counts = extractor(records_group, first_record_ordinal=records_group[0].ordinal)
        retained = []
        for group in groups:
            if _handback_owned_by_visible_message(group, records_group):
                counts['groups'] -= 1
                counts['included_string_leaves'] -= 1
                dispositions['already_projected_handback'] += 1
            else:
                retained.append(group)
        output.extend(retained)
        for key in ('events', 'tool_blocks', 'groups', 'string_leaves', 'included_string_leaves'):
            coverage[key] += counts[key]
        dispositions.update(counts['dispositions'])
    coverage['dispositions'] = dict(sorted(dispositions.items()))
    return coverage


class CertificationBudget:
    """At most three seconds of optional proof work per coordinator allowance."""

    def __init__(self):
        self._lock = threading.Lock()
        self._remaining = 3.0

    def renew(self):
        """The coordinator may renew only after an active proof has settled."""
        if not self._lock.acquire(blocking=False):
            return False
        try:
            self._remaining = 3.0
            return True
        finally:
            self._lock.release()

    @contextmanager
    def lease(self):
        # Publication has already committed. A competing optional proof must
        # defer, never hold up source progress or multiply the shared allowance.
        if not self._lock.acquire(blocking=False):
            yield None
            return
        started = time.monotonic()
        try:
            yield started + self._remaining if self._remaining > 0 else None
        finally:
            self._remaining = max(0.0, self._remaining - (time.monotonic() - started))
            self._lock.release()


PROJECTION_PROGRESS_INTERVAL_SECONDS = 5.0
MAX_PASSAGE_PROJECTION_BATCH = 1_000
MAX_PASSAGE_EMBEDDING_BATCH = 5_000
MAX_PASSAGE_HEADER_BACKFILL_BATCH = 5_000
# Content-free token estimate for the embed plan: UTF-8 bytes per token.
PASSAGE_PLAN_BYTES_PER_TOKEN = 4
PASSAGE_POOL_WARM_SIZE = 4
PASSAGE_COMMIT_WORKERS = 8
# Retained passages whose ordinal moved are parked above every real ordinal
# before they take their final position, so the (document, policy, ordinal)
# unique index never sees a transient duplicate. ``ordinal`` is a CHECK >= 0
# integer; no document has 2**30 passages.
PASSAGE_ORDINAL_PARK_OFFSET = 1 << 30


def _document_extraction_version(source_id: str, requested: str | None) -> str:
    """Keep recorded requests; first provider projections include typed tools."""
    if requested is not None:
        return requested
    if source_id.startswith("codex:"):
        return "codex_tool_groups_v1"
    return "tool_groups_v1" if source_id.startswith("claude:") else "legacy"


@dataclass(frozen=True)
class PassageCandidate:
    supported_extraction_versions: ClassVar[frozenset[str]] = frozenset(
        {"legacy", "tool_groups_v1", "codex_tool_groups_v1"})
    tenant_id: str
    source_id: str
    logical_document_id: str
    revision: int
    generation: int
    changed_at: datetime
    source_document_sha256: str
    manifest_reference: dict[str, Any]
    part_references: tuple[dict[str, Any], ...]
    extraction_version: str = "legacy"


@dataclass(frozen=True)
class PreparedPassageDocument:
    candidate: PassageCandidate
    dense_message_count: int
    dense_message_bytes: int
    passages: Sequence[LosslessPassage | passage_append.RetainedPassage]
    append_checkpoint: dict[str, Any] | None = None
    previous_checkpoint: dict[str, Any] | None = None
    append_checkpoint_available: bool = False
    archive_bytes_read: int = 0
    archive_parts_read: int = 0
    extraction_coverage: dict[str, Any] | None = None
    legacy_migration: dict[str, Any] | None = None
    _resources: ExitStack | None = field(default=None, repr=False, compare=False)

    def close(self):
        if self._resources is not None:
            self._resources.close()


_LEGACY_PASSAGES = """SELECT passage_id,ordinal,revision,text_sha256,receipts,spans
    FROM canonical_passages WHERE tenant_id=%s AND source_id=%s
      AND logical_document_id=%s ORDER BY ordinal"""

# Scheduling only: epoch 5 changes only the v4 resolver-refusal branch. Keep the
# old checkpoint; an actual source update still uses the unchanged append proof.
_CODEX_BACKFILL_CURRENT = """
    (projected.append_checkpoint->'version'=to_jsonb(%s::integer)
     OR (%s::integer=5 AND projected.append_checkpoint->'version'='4'::jsonb
         AND projected.applied_extraction_version='codex_tool_groups_v1'
         AND projected.source_document_sha256=evidence.document_content_sha256
         AND projected.append_checkpoint->'revision'=to_jsonb(evidence.revision)
         AND projected.append_checkpoint->>'source_document_sha256'=evidence.document_content_sha256
         AND projected.append_checkpoint->>'manifest_sha256'=evidence.manifest_content_sha256
         AND NOT EXISTS (SELECT 1 FROM canonical_evidence_document_queue pending
             WHERE pending.tenant_id=evidence.tenant_id AND pending.source_id=evidence.source_id
               AND pending.native_parent_id=evidence.native_parent_id)
         AND projected.append_checkpoint->'tool_groups'->'version'='1'::jsonb
         AND projected.append_checkpoint->'tool_groups'->>'extraction_version'='codex_tool_groups_v1'
         AND projected.append_checkpoint->'tool_groups'->'record_count'=to_jsonb(evidence.record_count)
         AND projected.append_checkpoint->'tool_groups'->'coverage'=projected.extraction_coverage
         AND projected.append_checkpoint->'tool_groups'->'messages'=
             projected.extraction_coverage->'included_string_leaves'
         AND projected.extraction_coverage->>'contract'='recall.codex-tool-event-projection.v1'
         AND projected.extraction_coverage ?& ARRAY['contract','events','tool_blocks','groups',
             'string_leaves','included_string_leaves','dispositions']
         AND CASE WHEN jsonb_typeof(projected.extraction_coverage)='object'
             THEN projected.extraction_coverage - ARRAY['contract','events','tool_blocks','groups',
                 'string_leaves','included_string_leaves','dispositions']
             END='{}'::jsonb
         AND NOT EXISTS (
             SELECT 1 FROM jsonb_each(CASE
                 WHEN jsonb_typeof(projected.extraction_coverage)='object'
                 THEN projected.extraction_coverage ELSE '{}'::jsonb END) counter
             WHERE counter.key NOT IN ('contract','dispositions')
               AND (jsonb_typeof(counter.value)<>'number' OR counter.value::text !~ '^(0|[1-9][0-9]*)$'))
         AND projected.extraction_coverage->'events'<=to_jsonb(evidence.record_count)
         AND projected.extraction_coverage->'events'>'0'::jsonb
         AND projected.extraction_coverage->'tool_blocks'<=projected.extraction_coverage->'events'
         AND projected.extraction_coverage->'string_leaves'<=projected.extraction_coverage->'tool_blocks'
         AND projected.extraction_coverage->'included_string_leaves'<=projected.extraction_coverage->'string_leaves'
         AND projected.extraction_coverage->'groups'=projected.extraction_coverage->'included_string_leaves'
         AND jsonb_typeof(projected.extraction_coverage->'dispositions')='object'
         AND NOT EXISTS (
             SELECT 1 FROM jsonb_each(CASE
                 WHEN jsonb_typeof(projected.extraction_coverage->'dispositions')='object'
                 THEN projected.extraction_coverage->'dispositions' ELSE '{}'::jsonb END) disposition
             WHERE disposition.key NOT IN ('invalid_json','unsupported_provider_event',
                 'source_unit_refused','provider_role_or_receipts_mismatch','empty_string','no_token_content')
                OR jsonb_typeof(disposition.value)<>'number'
                OR disposition.value::text !~ '^(0|[1-9][0-9]*)$')
         AND coalesce(projected.extraction_coverage->'dispositions'->'source_unit_refused',
                      '0'::jsonb)='0'::jsonb))
"""
_MIGRATION_FIELDS = ('revision', 'source_document_sha256', 'policy_fingerprint',
                     'requested_extraction_version', 'applied_extraction_version')


def _legacy_block_intervals(message, span, normal, normal_units, leaves, raw):
    """Translate an exact historical concatenation into native emitted byte domains.

    Only the newline separators inserted by the historical visible-message join
    have no source-field interval. Every intersected field byte must be covered
    by the unchanged final typed/normal passage coverage check.
    """
    key = (message.record_ordinal, message.record_count)
    content = json.loads(raw)['message']['content']
    if not isinstance(content, list):
        raise ValueError('historical_domain_unsupported')
    historical = tuple(dict.fromkeys(_text_blocks(content)))
    if '\n'.join(historical) != message.text:
        raise ValueError('historical_commitment_mismatch')
    visible = tuple(dict.fromkeys(_text_blocks(content, include_tool_results=False)))
    offsets = {}
    position = 0
    for text in visible:
        offsets[text] = position
        position += len(text.encode()) + 1
    if visible and (key not in normal or normal[key].text != '\n'.join(visible)
                    or normal[key].receipts != message.receipts
                    or len(normal_units.get(key, ())) != 1):
        raise ValueError('historical_domain_unsupported')
    seen = set()
    position = 0
    obligations = []
    for index, block in enumerate(content):
        for text in _text_blocks(block):
            if text in seen:
                continue
            seen.add(text)
            end = position + len(text.encode())
            left, right = max(position, span.source_byte_start), min(end, span.source_byte_end)
            if left < right:
                if isinstance(block, dict) and block.get('type') == 'tool_result':
                    matching = [leaf for leaf in leaves
                                if leaf.source_unit.startswith(
                                    f'claude.tool-result.string.v1#/message/content/{index}/')
                                and leaf.text == text and leaf.receipts == message.receipts]
                    if not matching:
                        raise ValueError('historical_domain_unsupported')
                    obligations.extend((*key, leaf.source_unit, left-position, right-position)
                                       for leaf in matching)
                else:
                    unit = next(iter(normal_units[key]))
                    obligations.append((*key, unit, offsets[text]+left-position,
                                        offsets[text]+right-position))
            position = end + 1
    if not obligations:
        raise ValueError('historical_domain_unsupported')
    return obligations


def _legacy_result_intervals(old, messages, passages, groups, current_receipts):
    """Prove old visible bytes are covered by native normal and typed result domains."""
    tool_receipts = {r for group in groups if group.evidence_kind == 'tool_result'
                     for message in group.messages for r in message.receipts}
    if not any(tool_receipts.intersection(row['receipts']) for row in old):
        return None
    normal = {(m.record_ordinal, m.record_count): m for m in messages}
    # Use the native emitted unit: an oversized complete final can be split
    # into ordinary windows instead of carrying its complete-message marker.
    normal_units = {}
    for passage in passages:
        for span in passage.spans:
            normal_units.setdefault((span.record_ordinal, span.record_count), set()).add(span.source_unit)
    historical = dict(normal)
    leaves = {}
    historical_group_indexes = {}
    for group_index, group in enumerate(groups):
        if group.evidence_kind != 'tool_result':
            continue
        for message in group.messages:
            key = (message.record_ordinal, message.record_count)
            leaves.setdefault(key, []).append(message)
            historical_group_indexes.setdefault(key, group_index)
            historical[key] = replace(message, text=visible_message_text(group.full_event_text),
                                      source_unit=None, origin=None)
    obligations = []
    lost_tool = unsupported = False

    def reconstruct(row, source):
        spans = tuple(PassageSpan(**span) for span in row['spans'])
        mapped = {}
        for span in spans:
            message = source.get((span.record_ordinal, span.record_count))
            # A single old span can be located by its exact immutable receipts
            # even when a same-time arrival changes the record ordinal.
            if len(spans) == 1 and (message is None or list(message.receipts) != row['receipts']):
                matches = [m for m in source.values() if m.record_count == span.record_count
                           and list(m.receipts) == row['receipts']]
                if len(matches) != 1:
                    raise ValueError('historical_domain_unsupported')
                message = matches[0]
            if message is None:
                raise ValueError('historical_domain_unsupported')
            mapped[span.message_index] = message
        indexed = {span.message_index: replace(mapped[span.message_index], record_ordinal=span.record_ordinal)
                   for span in spans}
        receipts = list(dict.fromkeys(r for span in spans for r in indexed[span.message_index].receipts))
        text = reconstruct_passage(SimpleNamespace(spans=spans), indexed)
        if receipts != row['receipts'] or hashlib.sha256(text.encode()).hexdigest() != row['text_sha256']:
            raise ValueError('historical_commitment_mismatch')
        for span in spans:
            message = indexed[span.message_index]
            length = len(message.text.encode())
            if not 0 <= span.source_byte_start < span.source_byte_end <= length:
                raise ValueError('historical_domain_unsupported')
            if span.source_unit is not None and not (
                span.source_unit in COMPLETE_SOURCE_UNITS and span.source_unit == message.source_unit
                and span.source_byte_start == 0 and span.source_byte_end == length
            ):
                raise ValueError('historical_domain_unsupported')
        return spans, mapped

    for row in old:
        live = current_receipts.intersection(row['receipts'])
        if not live:
            continue  # Actual source removal/revision is not a preservation obligation.
        if live != set(row['receipts']):
            unsupported = True
            if tool_receipts.intersection(row['receipts']):
                lost_tool = True
            continue
        try:
            spans, source = reconstruct(row, normal)
        except (KeyError, TypeError, ValueError, AttributeError):
            try:
                spans, source = reconstruct(row, historical)
            except (KeyError, TypeError, ValueError, AttributeError):
                unsupported = True
                lost_tool |= bool(tool_receipts.intersection(row['receipts']))
                continue
        for span in spans:
            message = source[span.message_index]
            key = (message.record_ordinal, message.record_count)
            unit = None
            if key not in normal or normal[key].text != message.text:
                lost_tool = True
                candidates = leaves.get(key, ())
                if len(candidates) != 1 or candidates[0].text != message.text or candidates[0].receipts != message.receipts:
                    try:
                        obligations.extend(_legacy_block_intervals(
                            message, span, normal, normal_units, candidates,
                            groups[historical_group_indexes[key]].full_event_text))
                    except (KeyError, TypeError, ValueError, AttributeError):
                        unsupported = True
                    continue
                unit = candidates[0].source_unit
            else:
                units = normal_units.get(key, set())
                if len(units) != 1:
                    unsupported = True
                    continue
                unit = next(iter(units))
            obligations.append((*key, unit, span.source_byte_start, span.source_byte_end))
    if not lost_tool:
        return None
    if unsupported:
        raise LogicalEvidenceError('legacy_tool_migration_required')
    return obligations


@dataclass(frozen=True)
class PassageDiff:
    """Differential plan for one document's passage rows.

    ``to_insert`` are new passages (ids absent from the table), ``to_delete``
    are existing ids the new build no longer produces (shifted or edited
    windows, or a superseded policy), ``retained`` are ids present on both
    sides. ``moved`` is the subset of retained ids whose ordinal changed, with
    their new ordinal; ``revision_stale`` are retained ids whose stored
    revision differs from the candidate revision.
    """

    to_insert: Sequence[LosslessPassage]
    to_delete: tuple[str, ...]
    retained: tuple[str, ...]
    moved: tuple[tuple[str, int], ...]
    revision_stale: tuple[str, ...]

    @property
    def counters(self) -> dict[str, int]:
        return {
            "inserted": len(self.to_insert),
            "deleted": len(self.to_delete),
            "retained": len(self.retained),
        }


def classify_passages(
    existing: Iterable[dict[str, Any]],
    passages: Sequence[LosslessPassage | passage_append.RetainedPassage],
    *,
    revision: int,
) -> PassageDiff:
    """Split the new passage set against the stored rows of one document.

    ``existing`` rows carry ``passage_id``, ``ordinal`` and ``revision`` (any
    policy fingerprint: rows of a superseded policy have different ids and are
    deleted). Identity is the stable passage id, so a retained row has the
    same text and spans by construction and its ``text_redacted`` is never
    rewritten.
    """

    stored: dict[str, dict[str, Any]] = {}
    for row in existing:
        passage_id = row["passage_id"]
        if passage_id in stored:
            raise LogicalEvidenceError("passage_rows_duplicate")
        stored[passage_id] = row
    new_ids = set()
    disk_backed = isinstance(passages, _DiskSequence)
    inserted = array('Q') if disk_backed else []
    retained = []
    moved = []
    missing_prefix = False
    # Replay each payload once for the diff; keep only identities and indexes.
    for index, passage in enumerate(passages):
        new_ids.add(passage.passage_id)
        prior = stored.get(passage.passage_id)
        if prior is None:
            if isinstance(passage, passage_append.RetainedPassage):
                missing_prefix = True
            elif isinstance(passage, LosslessPassage):
                inserted.append(index if disk_backed else passage)
        else:
            retained.append(passage.passage_id)
            if int(prior['ordinal']) != passage.ordinal:
                moved.append((passage.passage_id, passage.ordinal))
    if len(new_ids) != len(passages):
        raise LogicalEvidenceError("passage_identity_collision")
    if missing_prefix:
        raise LogicalEvidenceError("passage_retained_prefix_missing")
    to_insert = _DiskView(passages, inserted) if disk_backed else tuple(inserted)
    revision_stale = tuple(
        passage_id
        for passage_id in retained
        if int(stored[passage_id]["revision"]) != revision
    )
    to_delete = tuple(
        passage_id for passage_id in stored if passage_id not in new_ids
    )
    return PassageDiff(
        to_insert=to_insert,
        to_delete=to_delete,
        retained=tuple(retained),
        moved=tuple(moved),
        revision_stale=revision_stale,
    )


DOCUMENT_CONTEXT_SQL = """
    SELECT profile.family AS source_family,
           coalesce(aliases.values,ARRAY[]::text[]) AS source_aliases,
           session.harness,session.metadata,
           coalesce(attributed.actors,'[]'::jsonb) AS actors
      FROM canonical_evidence_documents evidence
      LEFT JOIN sessions session
        ON session.source_id=evidence.source_id
       AND session.native_id=evidence.native_parent_id
      LEFT JOIN source_profiles profile
        ON profile.source_id=evidence.source_id
      LEFT JOIN LATERAL (
          SELECT array_agg(alias ORDER BY alias) AS values
            FROM source_aliases
           WHERE source_id=evidence.source_id
      ) aliases ON true
      LEFT JOIN LATERAL (
          SELECT jsonb_agg(
                     jsonb_build_object(
                         'actor_id',person.actor_id,
                         'display_name',person.display_name,
                         'relations',person.relations,
                         'aliases',person.aliases
                     )
                     ORDER BY lower(person.display_name),person.actor_id
                 ) AS actors
            FROM (
                  SELECT actor.actor_id,actor.display_name,
                         array_agg(DISTINCT link.relation
                                   ORDER BY link.relation) AS relations,
                         coalesce(
                             array_agg(DISTINCT alias.alias
                                       ORDER BY alias.alias)
                             FILTER (WHERE alias.searchable),
                             ARRAY[]::text[]
                         ) AS aliases
                    FROM canonical_evidence_document_actors link
                    JOIN brain_actors actor
                      ON actor.tenant_id=link.tenant_id
                     AND actor.actor_id=link.actor_id
                     AND actor.active
                    LEFT JOIN brain_actor_aliases alias
                      ON alias.tenant_id=actor.tenant_id
                     AND alias.actor_id=actor.actor_id
                   WHERE link.tenant_id=evidence.tenant_id
                     AND link.source_id=evidence.source_id
                     AND link.logical_document_id=evidence.logical_document_id
                     AND link.revision=evidence.revision
                   GROUP BY actor.actor_id,actor.display_name
            ) person
      ) attributed ON true
     WHERE evidence.tenant_id=%s AND evidence.source_id=%s
       AND evidence.logical_document_id=%s
"""


def document_context_from_row(row: dict[str, Any] | None) -> DocumentContext:
    """Header inputs for one logical document from its catalog row."""

    if row is None:
        return DocumentContext()
    session_metadata = row.get("metadata") or {}
    if not isinstance(session_metadata, dict):
        session_metadata = {}
    return DocumentContext(
        source_family=row.get("source_family"),
        source_aliases=tuple(row.get("source_aliases") or ()),
        harness=row.get("harness") or session_metadata.get("harness"),
        workspace=session_metadata.get("cwd"),
        branch=session_metadata.get("branch"),
        actors=tuple(
            ActorContext(
                actor_id=value["actor_id"],
                display_name=value["display_name"],
                relations=tuple(value.get("relations") or ()),
                aliases=tuple(value.get("aliases") or ()),
            )
            for value in row.get("actors") or ()
        ),
    )


def document_context(
    connection: Any,
    *,
    tenant_id: str,
    source_id: str,
    logical_document_id: str,
) -> DocumentContext:
    row = connection.execute(
        DOCUMENT_CONTEXT_SQL,
        (tenant_id, source_id, logical_document_id),
    ).fetchone()
    return document_context_from_row(row)


def postgres_vector_plane(store: Any) -> bool:
    """True unless ``store`` reads the turbopuffer search plane (H3-e').

    On the turbopuffer plane no process reads or writes
    ``canonical_passage_embeddings`` or the embedding ledger: turbopuffer
    embeds natively, and migration 067 drops the tables. Stores without the
    attribute (unit-test fakes) are the postgres plane.
    """

    return getattr(store, "search_plane", "postgres") != "turbopuffer"


NOT_APPLICABLE_COVERAGE: dict[str, Any] = {
    "total": 0,
    "covered": 0,
    "coverage": 1.0,
    "status": "not-applicable",
    "plane": "turbopuffer",
}


def passage_contract_coverage(
    connection: Any,
    *,
    fingerprint: str,
    tenant_id: str | None = None,
    search_plane: str = "postgres",
) -> dict[str, Any]:
    """Share of live passages whose vector carries ``fingerprint`` (v2 key).

    A passage is covered when its embedding row has the given runtime
    fingerprint and its content hash equals the passage's ``embed_sha256``.
    An empty table counts as fully covered: nothing would be lost by reading
    the new contract. On the turbopuffer plane the question does not arise
    and the embeddings table is never read.
    """

    if search_plane == "turbopuffer":
        return dict(NOT_APPLICABLE_COVERAGE)
    row = connection.execute(
        """SELECT count(*) AS total,
                  count(*) FILTER (
                      WHERE embedding.runtime_fingerprint=%s
                        AND embedding.content_sha256=passage.embed_sha256
                  ) AS covered
             FROM canonical_passages passage
             JOIN canonical_passage_documents document
               USING(
                   tenant_id,source_id,logical_document_id,
                   policy_fingerprint
               )
             LEFT JOIN canonical_passage_embeddings embedding
               USING(tenant_id,source_id,passage_id)
            WHERE (%s::text IS NULL OR passage.tenant_id=%s)""",
        (fingerprint, tenant_id, tenant_id),
    ).fetchone()
    total = int(row["total"])
    covered = int(row["covered"])
    return {
        "total": total,
        "covered": covered,
        "coverage": 1.0 if total == 0 else covered / total,
    }


def passage_embed_plan(
    connection: Any,
    *,
    tenant_id: str,
    runtime: Any,
    price_per_mtoken: float = 0.0,
    search_plane: str = "postgres",
) -> dict[str, Any]:
    """Content-free, read-only report of the v2 re-embed for one tenant.

    Counts live passages, headers present/missing, vectors already under the
    v2 and v1 fingerprints, the rows a v2 pass would embed, and a byte-based
    token estimate (headers still missing are budgeted at the header cap).
    Nothing is written and no passage text leaves the database. On the
    turbopuffer plane nothing is pending and nothing is read.
    """

    if (
        not isinstance(tenant_id, str)
        or not tenant_id
        or isinstance(price_per_mtoken, bool)
        or not isinstance(price_per_mtoken, (int, float))
        or not 0 <= price_per_mtoken <= 1_000
    ):
        raise ValueError("passage embed plan scope is invalid")
    if search_plane == "turbopuffer":
        return {
            "status": "not-applicable",
            "read_only": True,
            "tenant_id": tenant_id,
            "plane": "turbopuffer",
            "contract": "v2",
            "runtime_configured": runtime is not None,
            "needs_embedding": 0,
            "estimated_tokens": 0,
            "price_per_mtoken": float(price_per_mtoken),
            "estimated_cost": 0.0,
        }
    fingerprint_v2 = getattr(runtime, "passage_fingerprint_v2", None)
    fingerprint_v1 = getattr(runtime, "passage_fingerprint_v1", None)
    if runtime is not None and (fingerprint_v2 is None or fingerprint_v1 is None):
        fingerprint_v2 = fingerprint_v1 = getattr(
            runtime, "passage_fingerprint", None
        )
    row = connection.execute(
        """SELECT count(*) AS passages,
                  count(passage.header_redacted) AS headers_present,
                  count(*) FILTER (
                      WHERE %s::text IS NOT NULL
                        AND embedding.runtime_fingerprint=%s
                        AND embedding.content_sha256=passage.embed_sha256
                  ) AS embedded_v2,
                  count(*) FILTER (
                      WHERE %s::text IS NOT NULL
                        AND embedding.runtime_fingerprint=%s
                        AND embedding.content_sha256=passage.text_sha256
                  ) AS embedded_v1,
                  coalesce(sum(
                      octet_length(passage.text_redacted)
                      +coalesce(octet_length(passage.header_redacted),%s)
                      +2
                  ) FILTER (
                      WHERE %s::text IS NULL
                         OR embedding.runtime_fingerprint IS DISTINCT FROM %s
                         OR embedding.content_sha256
                            IS DISTINCT FROM passage.embed_sha256
                  ),0) AS pending_bytes
             FROM canonical_passages passage
             JOIN canonical_passage_documents document
               USING(
                   tenant_id,source_id,logical_document_id,
                   policy_fingerprint
               )
             LEFT JOIN canonical_passage_embeddings embedding
               USING(tenant_id,source_id,passage_id)
            WHERE passage.tenant_id=%s""",
        (
            fingerprint_v2,
            fingerprint_v2,
            fingerprint_v1,
            fingerprint_v1,
            MAX_PASSAGE_HEADER_BYTES,
            fingerprint_v2,
            fingerprint_v2,
            tenant_id,
        ),
    ).fetchone()
    passages = int(row["passages"])
    embedded_v2 = int(row["embedded_v2"])
    pending_bytes = int(row["pending_bytes"])
    estimated_tokens = -(-pending_bytes // PASSAGE_PLAN_BYTES_PER_TOKEN)
    return {
        "status": "ok",
        "read_only": True,
        "tenant_id": tenant_id,
        "contract": "v2",
        "runtime_configured": runtime is not None,
        "passages": passages,
        "headers_present": int(row["headers_present"]),
        "headers_missing": passages - int(row["headers_present"]),
        "embedded_v2": embedded_v2,
        "embedded_v1": int(row["embedded_v1"]),
        "needs_embedding": passages - embedded_v2,
        "coverage_v2": 1.0 if passages == 0 else round(embedded_v2 / passages, 6),
        "estimated_tokens": estimated_tokens,
        "bytes_per_token": PASSAGE_PLAN_BYTES_PER_TOKEN,
        "price_per_mtoken": float(price_per_mtoken),
        "estimated_cost": round(
            estimated_tokens / 1_000_000 * float(price_per_mtoken),
            4,
        ),
    }


class CanonicalPassageProjector:
    """Build one disposable pointer index from authoritative logical documents."""

    def __init__(
        self,
        store: Any,
        logical_projection: LogicalEvidenceProjectionStore,
        *,
        policy: PassagePolicy,
        bound_tenant_id: str | None = None,
        membership_certification: parent_membership_enrollment.CertificationSettings | None = None,
    ) -> None:
        if bound_tenant_id is not None and (
            not isinstance(bound_tenant_id, str)
            or not bound_tenant_id
            or len(bound_tenant_id) > 256
        ):
            raise ValueError("passage projector tenant is invalid")
        if not isinstance(policy, PassagePolicy):
            raise ValueError("passage projector policy is invalid")
        self.store = store
        self.logical_projection = logical_projection
        self.policy = policy
        self.bound_tenant_id = bound_tenant_id
        self.append_reuse_enabled = passage_append.reuse_enabled()
        self.membership_certification = (membership_certification
            if membership_certification is not None
            else parent_membership_enrollment.CertificationSettings.from_env())
        self._prefer_notification_admission = False
        # Separate history/recent turns; fixed-size process-local state. Advance
        # on admission, so an unavailable source cannot retain the first turn.
        self._ordinary_source_cursor: dict[bool, tuple[str, str] | None] = {
            False: None, True: None,
        }
        # Optional proof has a shared deadline. Rotate only sources actually
        # attempted, since selected siblings can outlive that allowance.
        # A fresh worker honors durable queue age before rotating source turns.
        self._certification_source_cursor = None
        # Known refusals yield to siblings without changing authoritative queue
        # age or generation. At most one position per encountered source/mode;
        # restart resets these admission hints, never publication state.
        self._ordinary_parent_cursor: dict[
            bool, dict[tuple[str, str], tuple[datetime, str]]
        ] = {False: {}, True: {}}
        runtime = getattr(store, "semantic_runtime", None)
        bind = getattr(runtime, "bind_passage_coverage_probe", None)
        if callable(bind) and not getattr(
            runtime, "has_passage_coverage_probe", True
        ):
            bind(self._coverage_probe)

    @property
    def search_plane(self) -> str:
        return "postgres" if postgres_vector_plane(self.store) else "turbopuffer"

    def _coverage_probe(self) -> float | None:
        runtime = getattr(self.store, "semantic_runtime", None)
        fingerprint = getattr(runtime, "passage_fingerprint_v2", None)
        if not isinstance(fingerprint, str) or not fingerprint:
            return None
        if not postgres_vector_plane(self.store):
            return NOT_APPLICABLE_COVERAGE["coverage"]
        with self.store.connect() as connection:
            return passage_contract_coverage(
                connection,
                fingerprint=fingerprint,
            )["coverage"]

    def contract_coverage(
        self,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Read-only v2 coverage for the rollout flip (see ``semantic``)."""

        tenant_id = self._tenant(tenant_id)
        runtime = self.store.semantic_runtime
        fingerprint = getattr(runtime, "passage_fingerprint_v2", None)
        if not isinstance(fingerprint, str) or not fingerprint:
            fingerprint = getattr(runtime, "passage_fingerprint", "")
        if not postgres_vector_plane(self.store):
            return dict(NOT_APPLICABLE_COVERAGE)
        with self.store.connect() as connection:
            return passage_contract_coverage(
                connection,
                fingerprint=fingerprint,
                tenant_id=tenant_id,
            )

    def _tenant(self, tenant_id: str | None) -> str | None:
        if self.bound_tenant_id is None:
            return tenant_id
        if tenant_id is not None and tenant_id != self.bound_tenant_id:
            raise PermissionError("passage projector tenant is not authorized")
        return self.bound_tenant_id

    @staticmethod
    def _reference(
        row: dict[str, Any],
        *,
        prefix: str,
    ) -> dict[str, Any]:
        created_at = row[prefix + "created_at"]
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat()
        return {
            "contract": "recall.artifact-ref.v1",
            "schema_version": 1,
            "tenant_id": row["tenant_id"],
            "source_id": row["source_id"],
            "artifact_id": row[prefix + "artifact_id"],
            "storage_backend": row[prefix + "storage_backend"],
            "object_key": row[prefix + "object_key"],
            "content_sha256": row[prefix + "content_sha256"],
            "size_bytes": row[prefix + "size_bytes"],
            "media_type": row[prefix + "media_type"],
            "encryption": row[prefix + "encryption"],
            "version_id": row[prefix + "version_id"],
            "created_at": created_at,
        }

    def request_extraction_version(
        self, *, source_id: str, logical_document_id: str, revision: int,
        source_document_sha256: str, extraction_version: str,
        principal_id: str, tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Enroll one current projected document and queue its rebuild atomically.

        This maintenance operation uses the existing source-owner/brain-admin
        authority. No caller can enroll by wildcard, infer a revision, or downgrade.
        """
        from .control import ControlPlane

        tenant_id = self._tenant(tenant_id)
        if (tenant_id is None or extraction_version not in {"tool_groups_v1", "codex_tool_groups_v1"}
            or (extraction_version == "codex_tool_groups_v1" and not source_id.startswith("codex:"))):
            raise ValueError("unsupported_document_extraction_request")
        key = (tenant_id, source_id, logical_document_id)
        with self.store.connect() as connection, connection.transaction():
            # Same lock order as logical publication and passage commit.
            for owner in ("logical-evidence", "lossless-passages"):
                connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                                   (owner + "\x1f" + logical_document_id,))
            row = connection.execute("""SELECT evidence.revision,
                    evidence.document_content_sha256, evidence.native_parent_id,
                    projected.revision AS projected_revision,
                    projected.source_document_sha256, projected.policy_fingerprint,
                    projected.requested_extraction_version, projected.applied_extraction_version,
                    source.owner_principal_id, grant_row.permission
                FROM canonical_evidence_documents evidence
                JOIN canonical_passage_documents projected USING(tenant_id,source_id,logical_document_id)
                JOIN canonical_sources source USING(tenant_id,source_id)
                JOIN canonical_source_grants grant_row USING(tenant_id,source_id)
                WHERE evidence.tenant_id=%s AND evidence.source_id=%s
                  AND evidence.logical_document_id=%s AND grant_row.principal_id=%s
                FOR UPDATE OF projected FOR SHARE OF evidence,source,grant_row""",
                (*key, principal_id)).fetchone()
            if row is None:
                raise ValueError("source_or_projected_document_unavailable")
            if row['owner_principal_id'] != principal_id or row['permission'] != 'owner':
                ControlPlane._brain_admin(connection, principal_id=principal_id,
                                          tenant_id=tenant_id, lock=True)
            if (row['revision'] != revision or row['projected_revision'] != revision
                or row['document_content_sha256'] != source_document_sha256
                or row['source_document_sha256'] != source_document_sha256
                or row['policy_fingerprint'] != self.policy.fingerprint):
                raise ValueError("document_extraction_catalog_changed")
            forgetting = connection.execute("""SELECT 1 FROM canonical_evidence_document_queue
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s AND reason='forget'""",
                (tenant_id, source_id, row['native_parent_id'])).fetchone()
            if forgetting:
                raise ValueError("document_extraction_forget_pending")
            if (extraction_version != "codex_tool_groups_v1"
                and "codex_tool_groups_v1" in (row['requested_extraction_version'], row['applied_extraction_version'])):
                raise ValueError("document_extraction_downgrade_refused")
            if row['requested_extraction_version'] == row['applied_extraction_version'] == extraction_version:
                return {"status": "current", "extraction_version": extraction_version}
            connection.execute("""UPDATE canonical_passage_documents
                SET requested_extraction_version=%s
                WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s""",
                (extraction_version, *key))
            connection.execute("""INSERT INTO canonical_passage_projection_queue(
                    tenant_id,source_id,logical_document_id,revision,generation,reason,changed_at)
                VALUES (%s,%s,%s,%s,1,'backfill',clock_timestamp())
                ON CONFLICT(tenant_id,source_id,logical_document_id) DO UPDATE SET
                    revision=excluded.revision,
                    generation=canonical_passage_projection_queue.generation+1,
                    reason='backfill',changed_at=clock_timestamp()""", (*key, revision))
            return {"status": "queued", "extraction_version": extraction_version}

    def seed_backfill(self, *, tenant_id: str | None = None) -> int:
        tenant_id = self._tenant(tenant_id)
        with self.store.connect() as connection:
            result = connection.execute(
                f"""INSERT INTO canonical_passage_projection_queue(
                       tenant_id,source_id,logical_document_id,revision,
                       generation,reason,changed_at
                   )
                   SELECT evidence.tenant_id,evidence.source_id,
                          evidence.logical_document_id,evidence.revision,
                          1,'backfill',clock_timestamp()
                     FROM canonical_evidence_documents evidence
                     LEFT JOIN canonical_passage_documents projected
                       ON projected.tenant_id=evidence.tenant_id
                      AND projected.source_id=evidence.source_id
                      AND projected.logical_document_id
                          =evidence.logical_document_id
                      AND projected.revision=evidence.revision
                      AND projected.policy_fingerprint=%s
                      AND projected.requested_extraction_version=projected.applied_extraction_version
                      AND (evidence.source_id NOT LIKE 'claude:%%'
                           OR projected.append_checkpoint->'version'=to_jsonb(%s::integer))
                      AND (evidence.source_id NOT LIKE 'codex:%%'
                           OR {_CODEX_BACKFILL_CURRENT})
                   WHERE (%s::text IS NULL OR evidence.tenant_id=%s)
                      AND projected.logical_document_id IS NULL
                   ON CONFLICT(tenant_id,source_id,logical_document_id)
                   DO UPDATE SET revision=excluded.revision,
                       generation=canonical_passage_projection_queue.generation+1,
                       reason='backfill',changed_at=clock_timestamp()
                   WHERE canonical_passage_projection_queue.reason='certification'""",
                (self.policy.fingerprint, passage_append.CHECKPOINT_VERSION,
                 passage_append.CODEX_CHECKPOINT_VERSION,
                 passage_append.CODEX_CHECKPOINT_VERSION, tenant_id, tenant_id),
            )
        return max(0, result.rowcount)

    def seed_backfill_page(
        self, *, tenant_id: str, source_id: str, after: str | None,
        upper: str, limit: int, max_source_bytes: int, deadline_at: float,
    ) -> dict[str, Any]:
        """Queue one finite metadata page; deferred rows remain caller obligations.

        The cursor covers examined keys, not successful writes. Persist the
        returned dispositions with it before resuming. Source bytes bound queue
        admission, not decoded memory or embedding tokens. Only the existing
        projector can validate and publish the queued work.
        """
        tenant_id = self._tenant(tenant_id)
        if not tenant_id or not source_id:
            raise ValueError("backfill page requires a tenant and source")
        if not LOGICAL_DOCUMENT_ID_RE.fullmatch(upper) or (
            after is not None and not LOGICAL_DOCUMENT_ID_RE.fullmatch(after)
        ):
            raise ValueError("backfill page requires exact logical document bounds")
        if type(limit) is not int or not 1 <= limit <= MAX_PASSAGE_PROJECTION_BATCH:
            raise ValueError("invalid backfill page limit")
        if type(max_source_bytes) is not int or max_source_bytes <= 0:
            raise ValueError("backfill page requires a positive source byte budget")
        if not math.isfinite(deadline_at):
            raise ValueError("backfill page requires a finite deadline")
        with self.store.connect(timeout=max(0.001, deadline_at - time.monotonic())) as connection:
            rows = self.store._execute_bounded(
                connection,
                f"""WITH page AS MATERIALIZED (
                    SELECT tenant_id,source_id,logical_document_id,revision,
                           document_content_sha256,manifest_content_sha256,record_count,native_parent_id
                    FROM canonical_evidence_documents
                    WHERE tenant_id=%s AND source_id=%s
                      AND logical_document_id>%s
                      AND logical_document_id<=%s
                    ORDER BY logical_document_id LIMIT %s
                ), eligible AS MATERIALIZED (
                    SELECT evidence.*,
                           projected.logical_document_id IS NOT NULL AS current,
                           coalesce(queue.reason<>'certification',false) AS conflict
                    FROM page evidence
                    LEFT JOIN canonical_passage_documents projected
                      ON projected.tenant_id=evidence.tenant_id
                     AND projected.source_id=evidence.source_id
                     AND projected.logical_document_id=evidence.logical_document_id
                     AND projected.revision=evidence.revision
                     AND projected.policy_fingerprint=%s
                     AND projected.requested_extraction_version=projected.applied_extraction_version
                     AND (evidence.source_id NOT LIKE 'claude:%%'
                          OR projected.append_checkpoint->'version'=to_jsonb(%s::integer))
                     AND (evidence.source_id NOT LIKE 'codex:%%'
                          OR {_CODEX_BACKFILL_CURRENT})
                    LEFT JOIN canonical_passage_projection_queue queue
                      ON queue.tenant_id=evidence.tenant_id
                     AND queue.source_id=evidence.source_id
                     AND queue.logical_document_id=evidence.logical_document_id
                ), costs AS MATERIALIZED (
                    SELECT eligible.*,parts.source_bytes
                    FROM eligible
                    LEFT JOIN LATERAL (
                        SELECT sum(size_bytes)::bigint AS source_bytes
                        FROM canonical_evidence_document_parts part
                        WHERE part.tenant_id=eligible.tenant_id
                          AND part.source_id=eligible.source_id
                          AND part.logical_document_id=eligible.logical_document_id
                          AND part.revision=eligible.revision
                          AND NOT eligible.current AND NOT eligible.conflict
                    ) parts ON true
                ), budgeted AS MATERIALIZED (
                    SELECT costs.*,
                           sum(CASE WHEN source_bytes<=%s THEN source_bytes ELSE 0 END)
                             OVER (ORDER BY logical_document_id) AS admitted_bytes
                    FROM costs
                ), queued AS (
                    INSERT INTO canonical_passage_projection_queue(
                        tenant_id,source_id,logical_document_id,revision,
                        generation,reason,changed_at)
                    SELECT tenant_id,source_id,logical_document_id,revision,
                           1,'backfill',clock_timestamp()
                    FROM budgeted
                    WHERE NOT current AND NOT conflict
                      AND source_bytes<=%s AND admitted_bytes<=%s
                    ON CONFLICT(tenant_id,source_id,logical_document_id)
                    DO UPDATE SET revision=excluded.revision,
                        generation=canonical_passage_projection_queue.generation+1,
                        reason='backfill',changed_at=clock_timestamp()
                    WHERE canonical_passage_projection_queue.reason='certification'
                    RETURNING logical_document_id,generation
                )
                SELECT budgeted.logical_document_id,budgeted.revision,
                       budgeted.source_bytes,queued.generation,
                       CASE WHEN budgeted.current THEN 'current'
                            WHEN budgeted.conflict THEN 'conflict'
                            WHEN budgeted.source_bytes IS NULL THEN 'parts_unavailable'
                            WHEN budgeted.source_bytes>%s OR budgeted.admitted_bytes>%s
                              THEN 'byte_deferred'
                            WHEN queued.logical_document_id IS NOT NULL THEN 'queued'
                            ELSE 'conflict' END AS disposition
                FROM budgeted LEFT JOIN queued USING(logical_document_id)
                ORDER BY budgeted.logical_document_id""",
                (tenant_id, source_id, after or "", upper, limit,
                 self.policy.fingerprint, passage_append.CHECKPOINT_VERSION,
                 passage_append.CODEX_CHECKPOINT_VERSION,
                 passage_append.CODEX_CHECKPOINT_VERSION,
                 max_source_bytes, max_source_bytes, max_source_bytes,
                 max_source_bytes, max_source_bytes),
                deadline_at,
            ).fetchall()
        # No cursor or queue ACK escapes a failed/uncertain COMMIT.
        return {
            "source_id": source_id, "upper": upper,
            "after": rows[-1]["logical_document_id"] if rows else after,
            "examined": len(rows),
            "queued": sum(row["disposition"] == "queued" for row in rows),
            "queued_source_bytes": sum(
                row["source_bytes"] for row in rows if row["disposition"] == "queued"
            ),
            "rows": rows,
        }

    def _yield_unavailable_parents(self, keys, *, recent: bool, previous=None) -> None:
        grouped = {}
        for tenant, source, changed_at, document in keys:
            grouped.setdefault((tenant, source), []).append((changed_at, document))
        cursors = self._ordinary_parent_cursor[recent]
        for source, positions in grouped.items():
            cursor = (cursors if previous is None else previous).get(source)
            if previous is not None and source in cursors and cursors[source] != cursor:
                # Metadata refusals were already recorded by _pending. Combine
                # both failure phases against the original admission position.
                positions.append(cursors[source])
            def order(position):
                stamp, document = position
                if cursor is None:
                    after = True
                elif recent:
                    after = stamp < cursor[0] or (stamp == cursor[0] and document > cursor[1])
                else:
                    after = position > cursor
                return (not after, -stamp.timestamp() if recent else stamp.timestamp(), document)
            # Use admission order, not future completion order, when one source
            # supplied several failed siblings in the same bounded batch.
            cursors[source] = max(positions, key=order)

    def _pending(
        self,
        *,
        tenant_id: str | None,
        limit: int,
    ) -> tuple[PassageCandidate, ...]:
        recent = self._prefer_notification_admission
        cursor = self._ordinary_source_cursor[recent]
        after_tenant, after_source = cursor if cursor is not None else (None, None)
        parent_cursors = json.dumps([
            dict(tenant_id=tenant, source_id=source, changed_at=position[0].isoformat(),
                 logical_document_id=position[1])
            for (tenant, source), position in self._ordinary_parent_cursor[recent].items()
        ])
        with self.store.connect() as connection:
            rows = connection.execute(
                """WITH parent_cursors AS (
                       SELECT * FROM jsonb_to_recordset(%s::jsonb) AS cursor(
                           tenant_id text, source_id text, changed_at timestamptz,
                           logical_document_id text)
                   ), eligible AS MATERIALIZED (
                       SELECT candidate_queue.*,
                              CASE WHEN %s AND notification_queued_at IS NOT NULL
                                   THEN 0 ELSE 1 END AS admission_priority,
                              CASE WHEN %s::text IS NULL OR (candidate_queue.tenant_id,candidate_queue.source_id)>
                                        (%s::text,%s::text) THEN 0 ELSE 1 END AS source_wrap,
                              CASE WHEN cursor.source_id IS NULL OR
                                   (CASE WHEN %s THEN
                                       candidate_queue.changed_at<cursor.changed_at OR
                                       (candidate_queue.changed_at=cursor.changed_at AND
                                        candidate_queue.logical_document_id>cursor.logical_document_id)
                                    ELSE (candidate_queue.changed_at,candidate_queue.logical_document_id)>
                                         (cursor.changed_at,cursor.logical_document_id) END)
                                   THEN 0 ELSE 1 END AS parent_wrap
                         FROM canonical_passage_projection_queue candidate_queue
                         LEFT JOIN parent_cursors cursor USING (tenant_id,source_id)
                        WHERE (%s::text IS NULL OR candidate_queue.tenant_id=%s)
                          AND reason<>'certification'
                   ), ranked AS (
                       SELECT eligible.*,
                              row_number() OVER (
                                  PARTITION BY tenant_id,source_id,admission_priority
                                  ORDER BY CASE WHEN admission_priority=1 THEN parent_wrap ELSE 0 END,
                                           CASE WHEN %s THEN changed_at END DESC,
                                           changed_at,logical_document_id
                              ) AS source_position,
                              count(*) OVER (
                                  PARTITION BY tenant_id,source_id,admission_priority
                              ) AS source_backlog
                         FROM eligible
                   ), admitted AS MATERIALIZED (
                       SELECT * FROM ranked
                        ORDER BY admission_priority,
                                 CASE WHEN admission_priority=0
                                      THEN notification_queued_at END,
                                 CASE WHEN source_position=1 THEN 0 ELSE 1 END,
                                 CASE WHEN source_position=1 THEN source_wrap END,
                                 CASE WHEN source_position=1 THEN tenant_id END,
                                 CASE WHEN source_position=1 THEN source_id END,
                                 CASE WHEN source_position>1
                                      THEN source_position::numeric/source_backlog END,
                                 changed_at,tenant_id,source_id,logical_document_id
                        LIMIT %s
                   ), queue AS (
                       SELECT admitted.*,
                              dense_rank() OVER (
                                  ORDER BY source_wrap,tenant_id,source_id
                              ) AS source_turn
                         FROM admitted
                   )
                   SELECT queue.tenant_id,queue.source_id,
                          queue.logical_document_id,queue.revision,
                          queue.generation,queue.changed_at,
                          queue.admission_priority,queue.source_turn,
                          CASE WHEN queue.admission_priority=0
                               THEN queue.notification_queued_at END AS notification_priority,
                          (queue.changed_at<clock_timestamp()-interval '5 minutes') AS aged_priority,
                          coalesce(sum(part.size_bytes) OVER (
                              PARTITION BY queue.tenant_id,queue.source_id,queue.logical_document_id
                          ),0) AS estimated_bytes,
                          evidence.document_content_sha256,
                          projected.requested_extraction_version AS extraction_version,
                          evidence.manifest_artifact_id,
                          evidence.manifest_storage_backend,
                          evidence.manifest_object_key,
                          evidence.manifest_content_sha256,
                          evidence.manifest_size_bytes,
                          evidence.manifest_media_type,
                          evidence.manifest_encryption,
                          evidence.manifest_version_id,
                          evidence.created_at AS manifest_created_at,
                          part.part_ordinal,
                          part.artifact_id AS part_artifact_id,
                          part.storage_backend AS part_storage_backend,
                          part.object_key AS part_object_key,
                          part.content_sha256 AS part_content_sha256,
                          part.size_bytes AS part_size_bytes,
                          part.media_type AS part_media_type,
                          part.encryption AS part_encryption,
                          part.version_id AS part_version_id,
                          part.created_at AS part_created_at
                     FROM queue
                     LEFT JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=queue.tenant_id
                      AND evidence.source_id=queue.source_id
                      AND evidence.logical_document_id=queue.logical_document_id
                      AND evidence.revision=queue.revision
                     LEFT JOIN canonical_evidence_document_parts part
                       ON part.tenant_id=evidence.tenant_id
                      AND part.source_id=evidence.source_id
                      AND part.logical_document_id=evidence.logical_document_id
                      AND part.revision=evidence.revision
                     LEFT JOIN canonical_passage_documents projected
                       ON projected.tenant_id=queue.tenant_id
                      AND projected.source_id=queue.source_id
                      AND projected.logical_document_id=queue.logical_document_id
                    ORDER BY notification_priority ASC NULLS LAST,
                             aged_priority DESC,estimated_bytes,
                             queue.changed_at,queue.tenant_id,
                             queue.source_id,queue.logical_document_id,
                             part.part_ordinal""",
                (parent_cursors, recent, after_tenant, after_tenant, after_source,
                 recent, tenant_id, tenant_id, recent, limit),
            ).fetchall()
        # The turn belongs to admission, not successful metadata hydration.
        # A truly empty queue spends no turn; an invalid head must not suppress
        # the next recent/history mode indefinitely.
        if rows:
            self._prefer_notification_admission = not recent
        # Hydration can fail for an admitted key. Keep its source turn moving,
        # but retain that queue row for repair; missing metadata is never ACKed.
        ordinary = [row for row in rows if row["admission_priority"] == 1]
        if ordinary:
            last = max(ordinary, key=lambda row: row["source_turn"])
            self._ordinary_source_cursor[recent] = (last["tenant_id"], last["source_id"])
        self._yield_unavailable_parents(
            ((row["tenant_id"], row["source_id"], row["changed_at"], row["logical_document_id"])
             for row in rows if row["admission_priority"] == 1 and
             (row["manifest_artifact_id"] is None or row["part_ordinal"] is None)), recent=recent)
        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in rows:
            if row["manifest_artifact_id"] is None or row["part_ordinal"] is None:
                continue
            key = (
                row["tenant_id"],
                row["source_id"],
                row["logical_document_id"],
            )
            grouped.setdefault(key, []).append(row)
        candidates = []
        for values in grouped.values():
            first = values[0]
            candidates.append(
                PassageCandidate(
                    tenant_id=first["tenant_id"],
                    source_id=first["source_id"],
                    logical_document_id=first["logical_document_id"],
                    revision=int(first["revision"]),
                    generation=int(first["generation"]),
                    changed_at=first["changed_at"],
                    source_document_sha256=first[
                        "document_content_sha256"
                    ],
                    manifest_reference=self._reference(
                        first,
                        prefix="manifest_",
                    ),
                    part_references=tuple(
                        self._reference(row, prefix="part_")
                        for row in values
                    ),
                    extraction_version=_document_extraction_version(
                        first["source_id"], first["extraction_version"]),
                )
            )
        return tuple(candidates)

    def _requeue_missing(self, candidate: PassageCandidate, *, connection=None) -> int:
        """Ask the logical owner to rebuild this exact current catalog revision.

        Receipt repair can share its admission transaction; missing-part callers
        retain the original independently committed queue operation.
        """
        with (self.store.connect() if connection is None else nullcontext(connection)) as connection:
            result = connection.execute(
                """INSERT INTO canonical_evidence_document_queue(
                       tenant_id,source_id,native_parent_id,generation,
                       reason,changed_at
                   )
                   SELECT evidence.tenant_id,evidence.source_id,
                          evidence.native_parent_id,1,'backfill',
                          clock_timestamp()
                     FROM canonical_evidence_documents evidence
                    WHERE evidence.tenant_id=%s
                      AND evidence.source_id=%s
                      AND evidence.logical_document_id=%s
                      AND evidence.revision=%s
                   ON CONFLICT(tenant_id,source_id,native_parent_id)
                   DO UPDATE SET
                       generation=
                           canonical_evidence_document_queue.generation+1,
                       reason=CASE WHEN canonical_evidence_document_queue.reason='forget'
                                   THEN 'forget' ELSE 'backfill' END,
                       changed_at=clock_timestamp()""",
                (
                    candidate.tenant_id,
                    candidate.source_id,
                    candidate.logical_document_id,
                    candidate.revision,
                ),
            )
        return max(0, result.rowcount)

    def _append_state(self, candidate):
        # to_jsonb keeps schema083 readers/writers compatible during rollout.
        # Capability is read per attempt, never cached across migrations.
        try:
            with self.store.connect() as connection:
                return connection.execute("""SELECT EXISTS(
                    SELECT 1 FROM pg_attribute
                    WHERE attrelid=to_regclass('canonical_passage_documents')
                      AND attname='append_checkpoint' AND NOT attisdropped
                      AND atttypid='jsonb'::regtype) AS available,
                    (SELECT to_jsonb(projected) FROM canonical_passage_documents projected
                     WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s) AS projected""",
                    (candidate.tenant_id, candidate.source_id, candidate.logical_document_id)).fetchone()
        except psycopg.Error:
            return None

    def _append_prefix(self, candidate, checkpoint, connection=None):
        if connection is None:
            with self.store.connect() as current:
                return self._append_prefix(candidate, checkpoint, current)
        if 'tool_groups' in checkpoint:
            visible = self._append_prefix(candidate,
                {key: value for key, value in checkpoint.items() if key != 'tool_groups'}, connection)
            if visible is None:
                return None
            groups = checkpoint['tool_groups']
            rows = connection.execute("""SELECT passage_id,ordinal,receipts
                FROM canonical_passages WHERE tenant_id=%s AND source_id=%s
                  AND logical_document_id=%s AND ordinal>=%s ORDER BY ordinal""",
                (candidate.tenant_id, candidate.source_id, candidate.logical_document_id,
                 groups['visible_passages'])).fetchall()
            tools = tuple(passage_append.RetainedPassage(row['passage_id'], row['ordinal'],
                tuple(row['receipts'])) for row in rows)
            if (len(tools) != groups['tool_passages']
                or passage_append.prefix_digest(tools) != groups['prefix_sha256']):
                return None
            return visible + tools
        rows = connection.execute("""SELECT passage_id,ordinal,receipts
            FROM canonical_passages WHERE tenant_id=%s AND source_id=%s
              AND logical_document_id=%s AND ordinal<%s ORDER BY ordinal""",
            (candidate.tenant_id, candidate.source_id, candidate.logical_document_id,
             checkpoint['passage_ordinal'])).fetchall()
        prefix = tuple(passage_append.RetainedPassage(row['passage_id'], row['ordinal'],
                                                     tuple(row['receipts'])) for row in rows)
        if (len(prefix) != checkpoint['passage_ordinal']
            or passage_append.prefix_digest(prefix) != checkpoint['retained_prefix_sha256']):
            return None
        return prefix

    def _prepare(
        self,
        candidate: PassageCandidate,
        *,
        policy: PassagePolicy | None = None,
        incremental: bool = False,
        _migrate_legacy: bool = False,
    ) -> PreparedPassageDocument:
        policy = self.policy if policy is None else policy
        if (candidate.extraction_version not in PassageCandidate.supported_extraction_versions
            or (candidate.extraction_version == "codex_tool_groups_v1" and not candidate.source_id.startswith("codex:"))):
            raise LogicalEvidenceError("unsupported_document_extraction_version")
        grouped_tools = candidate.extraction_version != "legacy"
        manifest = self.logical_projection.read_manifest(
            candidate.manifest_reference,
            tenant_id=candidate.tenant_id,
            source_id=candidate.source_id,
        )
        if (
            manifest.get("logical_document_id")
            != candidate.logical_document_id
            or manifest.get("revision") != candidate.revision
            or manifest.get("document_content_sha256")
            != candidate.source_document_sha256
            or len(manifest.get("parts") or ())
            != len(candidate.part_references)
        ):
            raise LogicalEvidenceError("passage_manifest_catalog_mismatch")
        state = (self._append_state(candidate) if policy == self.policy
                 or (grouped_tools and candidate.source_id.startswith('claude:')) else None)
        if (candidate.source_id.startswith('claude:') and state is None
            and (grouped_tools or incremental or _migrate_legacy)):
            # Append reuse is optional; the prior publication needed to preserve
            # historical tool coverage is not. Retry through the existing queue.
            raise LogicalEvidenceError('legacy_tool_migration_required')
        checkpoint = (state.get('projected') or {}).get('append_checkpoint') if state else None
        bodies = {}
        archive_bytes_read = archive_parts_read = 0

        def full_fallback():
            full = self._prepare(candidate, policy=policy, incremental=False,
                                 _migrate_legacy=_migrate_legacy or incremental)
            return replace(full, archive_bytes_read=full.archive_bytes_read + archive_bytes_read,
                           archive_parts_read=full.archive_parts_read + archive_parts_read)

        def read_part(ordinal, *, remember=False):
            nonlocal archive_bytes_read, archive_parts_read
            if ordinal in bodies:
                return bodies.pop(ordinal)
            body = self.logical_projection.read_part(
                candidate.part_references[ordinal], tenant_id=candidate.tenant_id,
                source_id=candidate.source_id)
            archive_bytes_read += len(body)
            archive_parts_read += 1
            if remember:
                bodies[ordinal] = body
            return body

        resume = None
        prefix = ()
        compatible_groups = (grouped_tools and state and isinstance(checkpoint, dict)
            and passage_append.grouped_checkpoint_matches(checkpoint, state['projected'], candidate))
        if ((not grouped_tools and isinstance(checkpoint, dict) and 'tool_groups' not in checkpoint) or compatible_groups) and incremental and self.append_reuse_enabled and state and isinstance(checkpoint, dict) and checkpoint.get('policy_fingerprint') == policy.fingerprint:
            try:
                if passage_append.proves_append(checkpoint, state['projected'], candidate, manifest, lambda ordinal: read_part(ordinal, remember=True)):
                    prefix = self._append_prefix(candidate, checkpoint)
                    if prefix is not None:
                        resume = checkpoint
            except (LogicalEvidenceError, KeyError, TypeError, ValueError, psycopg.Error):
                # Optional prefix proof cannot make an otherwise valid current
                # full publication unavailable. Current reads still verify hashes.
                bodies.clear()
        first_record = resume['record_ordinal'] if resume else 0

        def records() -> Iterator[Any]:
            for ordinal, reference in enumerate(candidate.part_references):
                manifest_part = manifest["parts"][ordinal]
                if (
                    manifest_part.get("ordinal") != ordinal
                    or any(
                        manifest_part.get(field) != reference[field]
                        for field in (
                            "artifact_id",
                            "object_key",
                            "content_sha256",
                            "size_bytes",
                            "media_type",
                            "version_id",
                        )
                    )
                ):
                    raise LogicalEvidenceError(
                        "passage_manifest_part_mismatch"
                    )
                if manifest_part['last_record_ordinal'] < first_record:
                    continue
                payload = read_part(ordinal)
                if not payload.endswith(b"\n"):
                    raise LogicalEvidenceError(
                        "passage_logical_part_invalid"
                    )
                for record_ordinal, line in enumerate(BytesIO(payload), manifest_part['first_record_ordinal']):
                    if record_ordinal < first_record:
                        continue
                    # read_part verifies the immutable object hash. Validate
                    # each record contract without serializing trusted bytes
                    # back into canonical JSON a second time.
                    yield decode_logical_record(
                        line,
                        source_id=candidate.source_id,
                        verify_canonical=False,
                    )

        with ExitStack() as resources:
            payload_resources = resources.enter_context(ExitStack())
            passages = payload_resources.enter_context(_DiskSequence())
            messages = resources.enter_context(_DiskSequence())
            try:
                # Explicit Claude enrollment must preserve the same historical
                # tool bytes as automatic migration, including full rebuilds.
                migration_check = (not resume and candidate.source_id.startswith('claude:')
                    and state and state.get('projected')
                    and state['projected'].get('applied_extraction_version') == 'legacy'
                    and ((grouped_tools
                          and state['projected'].get('requested_extraction_version') == 'tool_groups_v1')
                         or ((incremental or _migrate_legacy) and not grouped_tools
                             and state['projected'].get('requested_extraction_version') == 'legacy')))
                source_records = records()
                if grouped_tools or migration_check:
                    # Never unpickle external input: this anonymous file contains
                    # only records decoded and validated above in this attempt.
                    spool = resources.enter_context(tempfile.TemporaryFile(mode="w+b"))
                    for record in source_records:
                        pickle.dump(record, spool, protocol=5)
                    source_records = _PassageRecordReplay(spool)
                messages.extend(iter_visible_messages(source_records, first_record_ordinal=first_record))
            except LogicalEvidenceError:
                if resume:
                    return full_fallback()
                raise
            passages.extend(
                iter_passages(
                    tenant_id=candidate.tenant_id,
                    source_id=candidate.source_id,
                    logical_document_id=candidate.logical_document_id,
                    revision=candidate.revision,
                    messages=messages,
                    policy=policy,
                )
                if messages and not resume
                else ()
            )
            if resume:
                if not messages or messages[0].record_ordinal != resume['record_ordinal']:
                    return full_fallback()
                arguments = dict(tenant_id=candidate.tenant_id, source_id=candidate.source_id,
                    logical_document_id=candidate.logical_document_id, revision=candidate.revision, policy=policy)
                try:
                    if resume['source_byte_start'] >= len(messages[0].text.encode()):
                        raise ValueError("invalid passage restart offset")
                    # Keep the existing append proof and tail geometry unchanged.
                    # The append helper still materializes its tail, not the full rebuild.
                    passages.extend(prefix[:resume['passage_ordinal']])
                    passages.extend(passage_append.resume_passages(tuple(messages), resume, arguments))
                except (ValueError, IndexError, UnicodeDecodeError):
                    return full_fallback()
            coverage = None
            groups = None
            migration = None
            obligations = None
            if migration_check:
                with self.store.connect() as connection:
                    old = connection.execute(_LEGACY_PASSAGES, (candidate.tenant_id, candidate.source_id,
                        candidate.logical_document_id)).fetchall()
                groups = resources.enter_context(_DiskSequence())
                coverage = _spool_tool_groups(source_records, extract_tool_event_groups, groups)
                obligations = _legacy_result_intervals(old, messages, passages, groups,
                    {r for record in source_records for r in record.receipts})
                if obligations is not None or grouped_tools:
                    migration = dict(projected={k: state['projected'][k] for k in _MIGRATION_FIELDS},
                                     passages_sha256=passage_append.digest(old))
                    candidate = replace(candidate, extraction_version='tool_groups_v1')
                    grouped_tools = True
                else:
                    coverage = None
            tool_message_count = tool_message_bytes = 0
            visible_passages = len(passages)
            visible_checkpoint = (passage_append.make_checkpoint(candidate, manifest, messages, passages,
                policy, previous=resume) if grouped_tools and state and state['available'] else None)
            if grouped_tools:
                previous_groups = resume['tool_groups'] if resume else None
                extractor = (extract_codex_tool_event_groups
                             if candidate.extraction_version == "codex_tool_groups_v1" else extract_tool_event_groups)
                if groups is None:
                    groups = resources.enter_context(_DiskSequence())
                    start = previous_groups['record_count'] if previous_groups else 0
                    coverage = _spool_tool_groups(
                        (record for record in source_records if record.ordinal >= start),
                        extractor, groups, first_record_ordinal=start)
                if previous_groups:
                    try:
                        coverage = passage_append.add_group_coverage(previous_groups['coverage'], coverage)
                    except (KeyError, TypeError, ValueError):
                        return full_fallback()
                    tool_message_count = previous_groups['messages']
                    tool_message_bytes = previous_groups['bytes']
                    passages.extend(replace(passage, ordinal=visible_passages + index)
                        for index, passage in enumerate(prefix[resume['passage_ordinal']:]))
                # Build each provider block independently. Its message indexes are
                # group-local, so later visible arrivals cannot rekey old tool spans.
                for group in groups:
                    tool_message_count += len(group.messages)
                    tool_message_bytes += sum(len(message.text.encode()) for message in group.messages)
                    for passage in iter_passages(tenant_id=candidate.tenant_id,
                        source_id=candidate.source_id, logical_document_id=candidate.logical_document_id,
                        revision=candidate.revision, messages=group.messages, policy=policy):
                        passages.extend((replace(passage, ordinal=len(passages)),))
            if obligations is not None:
                intervals = {}
                for passage in passages:
                    for span in passage.spans:
                        intervals.setdefault((span.record_ordinal, span.record_count, span.source_unit), []).append(
                            (span.source_byte_start, span.source_byte_end))
                for ordinal, count, unit, start, end in obligations:
                    for left, right in sorted(intervals.get((ordinal, count, unit), ())):
                        if left > start:
                            break
                        start = max(start, right)
                        if start >= end:
                            break
                    if start < end:
                        raise LogicalEvidenceError('legacy_tool_migration_required')
            checkpoint = (passage_append.make_checkpoint(candidate, manifest, messages, passages,
                policy, previous=resume) if not grouped_tools and state and state['available'] else None)
            if grouped_tools and visible_checkpoint is not None:
                checkpoint = dict(visible_checkpoint, tool_groups=dict(
                    version=1, extraction_version=candidate.extraction_version,
                    visible_passages=visible_passages, tool_passages=len(passages)-visible_passages,
                    record_count=manifest['record_count'], messages=tool_message_count,
                    bytes=tool_message_bytes, coverage=coverage,
                    prefix_sha256=passage_append.prefix_digest(passages[visible_passages:])))
            return PreparedPassageDocument(
                candidate=candidate,
                dense_message_count=len(messages) + tool_message_count + (resume["message_index"] if resume else 0),
                dense_message_bytes=sum(len(message.text.encode()) for message in messages) + tool_message_bytes + (resume["prefix_message_bytes"] if resume else 0),
                passages=passages,
                append_checkpoint=checkpoint,
                previous_checkpoint=resume,
                append_checkpoint_available=bool(state and state["available"]),
                archive_bytes_read=archive_bytes_read,
                archive_parts_read=archive_parts_read,
                extraction_coverage=coverage,
                legacy_migration=migration,
                _resources=payload_resources.pop_all(),
            )

    def _commit(self, prepared: PreparedPassageDocument) -> dict[str, Any]:
        candidate = prepared.candidate
        with self.store.connect() as connection:
            with connection.transaction():
                # Logical publication takes the parent catalog before this
                # queue. Serialize commit before taking the opposite FK order.
                # These ordered locks/reads depend only on the candidate.
                # Synchronize before consuming results or changing publication.
                with connection.pipeline():
                    connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        ("logical-evidence\x1f" + candidate.logical_document_id,),
                    )
                    connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        ("lossless-passages\x1f" + candidate.logical_document_id,),
                    )
                    queued_cursor = connection.execute(
                        """SELECT revision,generation,changed_at
                         FROM canonical_passage_projection_queue
                        WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s AND reason<>'certification'
                        FOR UPDATE""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.logical_document_id,
                        ),
                    )
                    current_cursor = connection.execute(
                        """SELECT revision,document_content_sha256,first_occurred_at,last_occurred_at
                         FROM canonical_evidence_documents
                        WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.logical_document_id,
                        ),
                    )
                    extraction_cursor = connection.execute("""SELECT requested_extraction_version,applied_extraction_version,
                    revision,source_document_sha256,policy_fingerprint
                    FROM canonical_passage_documents
                    WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s FOR UPDATE""",
                        (candidate.tenant_id, candidate.source_id, candidate.logical_document_id))
                    forgetting_cursor = (connection.execute("""SELECT 1
                    FROM canonical_evidence_document_queue queued
                    JOIN canonical_evidence_documents evidence USING(tenant_id,source_id,native_parent_id)
                    WHERE evidence.tenant_id=%s AND evidence.source_id=%s
                      AND evidence.logical_document_id=%s AND queued.reason='forget'""",
                        (candidate.tenant_id, candidate.source_id, candidate.logical_document_id))
                        if candidate.extraction_version != 'legacy' else None)
                queued = queued_cursor.fetchone()
                current = current_cursor.fetchone()
                extraction = extraction_cursor.fetchone()
                forgetting = forgetting_cursor.fetchone() if forgetting_cursor is not None else None
                if (
                    queued is None
                    or current is None
                    or forgetting is not None
                    or int(queued["revision"]) != candidate.revision
                    or int(queued["generation"]) != candidate.generation
                    or queued["changed_at"] != candidate.changed_at
                    or int(current["revision"]) != candidate.revision
                    or current["document_content_sha256"]
                    != candidate.source_document_sha256
                    or _document_extraction_version(candidate.source_id,
                        extraction['requested_extraction_version'] if extraction else None)
                    != (prepared.legacy_migration['projected']['requested_extraction_version']
                        if prepared.legacy_migration else candidate.extraction_version)
                ):
                    return {"status": "stale"}
                if prepared.legacy_migration:
                    old = connection.execute(_LEGACY_PASSAGES + ' FOR UPDATE',
                        (candidate.tenant_id, candidate.source_id, candidate.logical_document_id)).fetchall()
                    if (candidate.extraction_version != 'tool_groups_v1'
                        or not candidate.source_id.startswith('claude:')
                        or extraction != prepared.legacy_migration['projected']
                        or passage_append.digest(old) != prepared.legacy_migration['passages_sha256']):
                        return {"status": "stale"}
                    connection.execute("""UPDATE canonical_passage_documents
                        SET requested_extraction_version='tool_groups_v1'
                        WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s""",
                        (candidate.tenant_id, candidate.source_id, candidate.logical_document_id))
                existing = connection.execute(
                    """SELECT passage_id,ordinal,revision,
                              first_occurred_at,last_occurred_at,
                              header_redacted IS NULL AS header_missing
                         FROM canonical_passages
                        WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s
                        FOR UPDATE""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.logical_document_id,
                    ),
                ).fetchall()
                if prepared.previous_checkpoint is not None:
                    prior = connection.execute("""SELECT append_checkpoint
                        FROM canonical_passage_documents WHERE tenant_id=%s AND source_id=%s
                          AND logical_document_id=%s FOR UPDATE""",
                        (candidate.tenant_id,candidate.source_id,candidate.logical_document_id)).fetchone()
                    if (prior is None or prior['append_checkpoint'] != prepared.previous_checkpoint
                        or self._append_prefix(candidate, prepared.previous_checkpoint, connection) is None):
                        return {"status": "stale"}
                diff = classify_passages(
                    existing,
                    prepared.passages,
                    revision=candidate.revision,
                )
                # H2-a: one catalog read per document renders the contextual
                # header of every inserted passage and of retained rows that
                # predate headers. The header is embedding input only.
                retained_ids = set(diff.retained)
                retained_unheaded = [
                    row
                    for row in existing
                    if row["passage_id"] in retained_ids
                    and bool(row.get("header_missing"))
                ]
                context = (
                    document_context(
                        connection,
                        tenant_id=candidate.tenant_id,
                        source_id=candidate.source_id,
                        logical_document_id=candidate.logical_document_id,
                    )
                    if diff.to_insert or retained_unheaded
                    else None
                )
                headers = {
                    passage.passage_id: render_passage_header(
                        context,
                        first_occurred_at=passage.first_occurred_at,
                        last_occurred_at=passage.last_occurred_at,
                    )
                    for passage in diff.to_insert
                }
                for passage in diff.to_insert:
                    attribution = tool_evidence_context(passage.spans)
                    if attribution:
                        # Keep the existing header bound. Attribution must survive
                        # truncation; catalog context fills the remaining bytes.
                        headers[passage.passage_id] = (attribution + '\n' + headers[passage.passage_id]).encode()[:MAX_PASSAGE_HEADER_BYTES].decode(errors='ignore')
                embed_hashes = {
                    passage.passage_id: passage_embed_sha256(
                        headers[passage.passage_id],
                        passage.text,
                    )
                    for passage in diff.to_insert
                }
                # Write order (the (document, policy, ordinal) unique index
                # is not deferrable):
                #   1. capture reusable embeddings for the texts about to be
                #      inserted, before the rows that hold them are deleted;
                #   2. DELETE to_delete (frees their ordinals; cascades their
                #      actors and embeddings only);
                #   3. UPSERT the pointer row in place;
                #   4. park moved retained rows above every real ordinal,
                #      then set their final ordinal + revision (two phases,
                #      so a moved row never lands on an ordinal another
                #      retained row still holds);
                #   5. refresh retained revisions and receipts together;
                #      text_redacted and embedding input stay untouched;
                #   6. COPY to_insert passages + actors into the freed
                #      ordinals;
                #   7. re-attach embeddings for to_insert by content hash.
                # Steps 1 and 7 exist only on the postgres plane (H3-e'):
                # turbopuffer embeds natively and migration 067 drops the
                # embeddings table, so the turbopuffer plane never touches it.
                vector_plane = postgres_vector_plane(self.store)
                if diff.to_insert and vector_plane:
                    connection.execute(
                        """CREATE TEMP TABLE
                               recall_reusable_passage_embeddings
                               ON COMMIT DROP AS
                           SELECT embedding.model,embedding.dimensions,
                                  embedding.content_sha256,
                                  embedding.runtime_fingerprint,
                                  embedding.embedding,embedding.embedded_at
                             FROM canonical_passage_embeddings embedding
                             JOIN canonical_passages passage
                               USING(tenant_id,source_id,passage_id)
                            WHERE passage.tenant_id=%s
                              AND passage.source_id=%s
                              AND passage.logical_document_id=%s
                              AND (
                                  passage.embed_sha256=ANY(%s::text[])
                                  OR passage.text_sha256=ANY(%s::text[])
                              )""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.logical_document_id,
                            sorted(set(embed_hashes.values())),
                            sorted({
                                passage.text_sha256
                                for passage in diff.to_insert
                            }),
                        ),
                    )
                if diff.to_delete:
                    connection.execute(
                        """DELETE FROM canonical_passages
                            WHERE tenant_id=%s AND source_id=%s
                              AND logical_document_id=%s
                              AND passage_id=ANY(%s::text[])""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.logical_document_id,
                            list(diff.to_delete),
                        ),
                    )
                connection.execute(
                    """INSERT INTO canonical_passage_documents(
                           tenant_id,source_id,logical_document_id,revision,
                           policy_fingerprint,target_tokens,overlap_tokens,
                           source_document_sha256,dense_message_count,
                           dense_message_bytes,passage_count
                           ,requested_extraction_version,applied_extraction_version,extraction_coverage
                       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                       ON CONFLICT(tenant_id,source_id,logical_document_id)
                       DO UPDATE SET
                           revision=excluded.revision,
                           policy_fingerprint=excluded.policy_fingerprint,
                           target_tokens=excluded.target_tokens,
                           overlap_tokens=excluded.overlap_tokens,
                           source_document_sha256=
                               excluded.source_document_sha256,
                           dense_message_count=excluded.dense_message_count,
                           dense_message_bytes=excluded.dense_message_bytes,
                           passage_count=excluded.passage_count,
                           applied_extraction_version=excluded.applied_extraction_version,
                           extraction_coverage=excluded.extraction_coverage""",
                    (
                        candidate.tenant_id,
                        candidate.source_id,
                        candidate.logical_document_id,
                        candidate.revision,
                        self.policy.fingerprint,
                        self.policy.target_tokens,
                        self.policy.overlap_tokens,
                        candidate.source_document_sha256,
                        prepared.dense_message_count,
                        prepared.dense_message_bytes,
                        len(prepared.passages),
                        candidate.extraction_version,
                        candidate.extraction_version,
                        json.dumps(prepared.extraction_coverage) if prepared.extraction_coverage is not None else None,
                    ),
                )
                if prepared.append_checkpoint_available:
                    connection.execute("""UPDATE canonical_passage_documents SET append_checkpoint=%s::jsonb
                        WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s""",
                        (json.dumps(prepared.append_checkpoint) if prepared.append_checkpoint is not None else None,candidate.tenant_id,
                         candidate.source_id,candidate.logical_document_id))
                elif prepared.previous_checkpoint is not None:
                    raise LogicalEvidenceError("passage_append_checkpoint_missing")
                if diff.moved:
                    moved_ids = [passage_id for passage_id, _ in diff.moved]
                    connection.execute(
                        """UPDATE canonical_passages
                              SET ordinal=ordinal+%s
                            WHERE tenant_id=%s AND source_id=%s
                              AND passage_id=ANY(%s::text[])""",
                        (
                            PASSAGE_ORDINAL_PARK_OFFSET,
                            candidate.tenant_id,
                            candidate.source_id,
                            moved_ids,
                        ),
                    )
                    connection.execute(
                        """UPDATE canonical_passages passage
                              SET ordinal=moved.ordinal,revision=%s
                             FROM unnest(%s::text[],%s::int[])
                                  AS moved(passage_id,ordinal)
                            WHERE passage.tenant_id=%s
                              AND passage.source_id=%s
                              AND passage.passage_id=moved.passage_id""",
                        (
                            candidate.revision,
                            moved_ids,
                            [ordinal for _, ordinal in diff.moved],
                            candidate.tenant_id,
                            candidate.source_id,
                        ),
                    )
                # Stable IDs cover text and window geometry, not receipts or
                # optional origin annotations. Refresh full prepared metadata;
                # immutable append-prefix records carry no replacement spans.
                retained_receipts = (
                    {"passage_id": passage.passage_id,
                     "receipts": list(passage.receipts),
                     "spans": (json.loads(canonical_spans_json(passage.spans))
                               if isinstance(passage, LosslessPassage) else None)}
                    for passage in prepared.passages
                    if passage.passage_id in retained_ids
                )
                while receipt_batch := list(islice(retained_receipts, MAX_PASSAGE_PROJECTION_BATCH)):
                    connection.execute(
                        """UPDATE canonical_passages passage
                              SET revision=%s,receipts=current.receipts,
                                  spans=coalesce(current.spans,passage.spans)
                             FROM jsonb_to_recordset(%s::jsonb)
                                  AS current(passage_id text,receipts text[],spans jsonb)
                            WHERE passage.tenant_id=%s AND passage.source_id=%s
                              AND passage.logical_document_id=%s
                              AND passage.passage_id=current.passage_id
                              AND (passage.revision IS DISTINCT FROM %s
                                   OR passage.receipts IS DISTINCT FROM current.receipts
                                   OR (current.spans IS NOT NULL
                                       AND passage.spans IS DISTINCT FROM current.spans))""",
                        (candidate.revision,json.dumps(receipt_batch), candidate.tenant_id,
                         candidate.source_id, candidate.logical_document_id,candidate.revision),
                    )
                if retained_unheaded:
                    self._write_headers(
                        connection,
                        tenant_id=candidate.tenant_id,
                        source_id=candidate.source_id,
                        rows=retained_unheaded,
                        contexts={
                            row["passage_id"]: context
                            for row in retained_unheaded
                        },
                        enqueue=False,
                    )
                if diff.to_insert:
                    with connection.cursor() as cursor:
                        with cursor.copy(
                            """COPY canonical_passages(
                                   tenant_id,source_id,logical_document_id,
                                   revision,passage_id,ordinal,
                                   policy_fingerprint,target_tokens,
                                   overlap_tokens,token_count,
                                   first_occurred_at,last_occurred_at,
                                   roles,receipts,spans,text_redacted,
                                   text_sha256,header_redacted,embed_sha256
                               ) FROM STDIN"""
                        ) as copy:
                            for passage in diff.to_insert:
                                copy.write_row((
                                    passage.tenant_id,
                                    passage.source_id,
                                    passage.logical_document_id,
                                    passage.revision,
                                    passage.passage_id,
                                    passage.ordinal,
                                    passage.policy_fingerprint,
                                    self.policy.target_tokens,
                                    self.policy.overlap_tokens,
                                    passage.token_count,
                                    passage.first_occurred_at,
                                    passage.last_occurred_at,
                                    list(passage.roles),
                                    list(passage.receipts),
                                    canonical_spans_json(passage.spans),
                                    passage.text,
                                    passage.text_sha256,
                                    headers[passage.passage_id],
                                    embed_hashes[passage.passage_id],
                                ))
                        with cursor.copy(
                            """COPY canonical_passage_actors(
                                   tenant_id,source_id,passage_id,
                                   actor_id,relation
                               ) FROM STDIN"""
                        ) as copy:
                            for passage in diff.to_insert:
                                for link in passage.actor_links:
                                    copy.write_row((
                                        passage.tenant_id,
                                        passage.source_id,
                                        passage.passage_id,
                                        link.actor_id,
                                        link.relation,
                                    ))
                if diff.to_insert and vector_plane:
                    connection.execute(
                        """INSERT INTO canonical_passage_embeddings(
                               tenant_id,source_id,passage_id,model,
                               dimensions,content_sha256,
                               runtime_fingerprint,embedding,embedded_at
                           )
                           SELECT passage.tenant_id,passage.source_id,
                                  passage.passage_id,reusable.model,
                                  reusable.dimensions,
                                  reusable.content_sha256,
                                  reusable.runtime_fingerprint,
                                  reusable.embedding,reusable.embedded_at
                             FROM canonical_passages passage
                             JOIN LATERAL (
                                   SELECT cached.model,cached.dimensions,
                                          cached.content_sha256,
                                          cached.runtime_fingerprint,
                                          cached.embedding,cached.embedded_at
                                     FROM
                                          recall_reusable_passage_embeddings
                                          cached
                                    WHERE cached.content_sha256=
                                          passage.embed_sha256
                                       OR cached.content_sha256=
                                          passage.text_sha256
                                    ORDER BY (
                                        cached.content_sha256=
                                        passage.embed_sha256
                                    ) DESC,cached.runtime_fingerprint
                                    LIMIT 1
                             ) reusable ON true
                            WHERE passage.tenant_id=%s
                              AND passage.source_id=%s
                              AND passage.passage_id=ANY(%s::text[])
                           ON CONFLICT(tenant_id,source_id,passage_id)
                           DO NOTHING""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            [passage.passage_id for passage in diff.to_insert],
                        ),
                    )
                durable_certification = membership_remainder.retain(
                    connection, candidate, self.membership_certification)
                if not durable_certification:
                    deleted = connection.execute(
                        """DELETE FROM canonical_passage_projection_queue
                            WHERE tenant_id=%s AND source_id=%s
                              AND logical_document_id=%s AND generation=%s""",
                        (
                            candidate.tenant_id,
                            candidate.source_id,
                            candidate.logical_document_id,
                            candidate.generation,
                        ),
                    )
                    if deleted.rowcount != 1:
                        raise LogicalEvidenceError("passage_queue_conflict")
                from .logical_evidence_projection import CanonicalLogicalEvidenceProjector
                CanonicalLogicalEvidenceProjector._queue_parquet_scan(
                    connection, tenant_id=candidate.tenant_id,
                    source_id=candidate.source_id,
                    ranges=((current["first_occurred_at"], current["last_occurred_at"]),),
                    reason="logical-update", logical_document_id=candidate.logical_document_id,
                )
                # H3-a: the search outbox sees only what this commit changed.
                # Deleted rows become tombstones; every month an inserted or
                # deleted passage touched is queued for the Lance writer.
                deleted_ids = set(diff.to_delete)
                deleted_rows = [
                    row for row in existing if row["passage_id"] in deleted_ids
                ]
                if deleted_rows:
                    write_search_tombstones(
                        connection,
                        tenant_id=candidate.tenant_id,
                        source_id=candidate.source_id,
                        passages=deleted_rows,
                    )
                enqueue_search_outbox(
                    connection,
                    tenant_id=candidate.tenant_id,
                    source_id=candidate.source_id,
                    months=outbox_months(
                        [
                            (passage.first_occurred_at, passage.last_occurred_at)
                            for passage in diff.to_insert
                        ]
                        + [
                            (row["first_occurred_at"], row["last_occurred_at"])
                            for row in (*deleted_rows, *retained_unheaded)
                        ]
                    ),
                    reason="logical-update",
                )
        result = {"status": "committed", **diff.counters}
        if durable_certification is None:
            result["membership_legacy"] = True
        if durable_certification:
            result["membership_durable"] = True
        return result

    @staticmethod
    def _write_headers(
        connection: Any,
        *,
        tenant_id: str,
        source_id: str,
        rows: list[dict[str, Any]],
        contexts: dict[str, DocumentContext],
        enqueue: bool = True,
    ) -> int:
        """Fill header_redacted/embed_sha256 on rows that still lack them.

        Only NULL headers are written: an existing header is never rewritten
        here, so a retained passage keeps its embedding reuse key. With
        ``enqueue`` (H3-a) the months of the rows whose header actually
        changed are queued in the search outbox as ``header-change``; the
        differential commit passes ``False`` because it queues those months
        as ``logical-update`` itself.
        """

        if not rows:
            return 0
        ids: list[str] = []
        rendered: list[str] = []
        for row in rows:
            ids.append(row["passage_id"])
            rendered.append(render_passage_header(
                contexts[row["passage_id"]],
                first_occurred_at=row["first_occurred_at"],
                last_occurred_at=row["last_occurred_at"],
            ))
        # embed_sha256 = sha256(header || "\n\n" || text_redacted), hashed in
        # the database so passage text never round-trips for a header fill.
        # Byte-identical to ``passage_embed_sha256``.
        result = connection.execute(
            """UPDATE canonical_passages passage
                  SET header_redacted=headed.header_redacted,
                      embed_sha256=encode(sha256(convert_to(
                          headed.header_redacted||%s||passage.text_redacted,
                          'UTF8'
                      )),'hex')
                 FROM unnest(%s::text[],%s::text[])
                      AS headed(passage_id,header_redacted)
                WHERE passage.tenant_id=%s AND passage.source_id=%s
                  AND passage.passage_id=headed.passage_id
                  AND passage.header_redacted IS NULL
            RETURNING passage.first_occurred_at,passage.last_occurred_at""",
            (PASSAGE_EMBEDDING_SEPARATOR, ids, rendered, tenant_id, source_id),
        )
        changed = result.fetchall()
        if enqueue and changed:
            enqueue_search_outbox(
                connection,
                tenant_id=tenant_id,
                source_id=source_id,
                months=outbox_months(
                    (row["first_occurred_at"], row["last_occurred_at"])
                    for row in changed
                ),
                reason="header-change",
            )
        return len(changed)

    def backfill_headers(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 500,
        max_batches: int = 10,
    ) -> dict[str, int | str]:
        """Render headers for passages projected before schema 064.

        Reads catalog rows only (never the archive): the header is a pure
        function of the document's catalog context and the passage's own
        times. Runs ahead of ``embed_pending`` under contract v2 so every
        passage has an ``embed_sha256`` before it is embedded.
        """

        tenant_id = self._tenant(tenant_id)
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= MAX_PASSAGE_HEADER_BACKFILL_BATCH
            or isinstance(max_batches, bool)
            or not isinstance(max_batches, int)
            or not 1 <= max_batches <= 100
        ):
            raise ValueError("passage header backfill budget is invalid")
        updated = batches = 0
        tenant_scope = tenant_id or ""
        with self.store.connect() as connection:
            while batches < max_batches:
                rows = connection.execute(
                    """SELECT passage.tenant_id,passage.source_id,
                              passage.logical_document_id,passage.passage_id,
                              passage.first_occurred_at,
                              passage.last_occurred_at
                         FROM canonical_passages passage
                        WHERE passage.header_redacted IS NULL
                          AND (%s::text='' OR passage.tenant_id=%s)
                        ORDER BY passage.tenant_id,passage.source_id,
                                 passage.logical_document_id,passage.ordinal
                        LIMIT %s""",
                    (tenant_scope, tenant_scope, batch_size),
                ).fetchall()
                if not rows:
                    break
                grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
                for row in rows:
                    grouped.setdefault(
                        (
                            row["tenant_id"],
                            row["source_id"],
                            row["logical_document_id"],
                        ),
                        [],
                    ).append(row)
                written = 0
                with connection.transaction():
                    for (scope_tenant, scope_source, document_id), values in (
                        grouped.items()
                    ):
                        context = document_context(
                            connection,
                            tenant_id=scope_tenant,
                            source_id=scope_source,
                            logical_document_id=document_id,
                        )
                        written += self._write_headers(
                            connection,
                            tenant_id=scope_tenant,
                            source_id=scope_source,
                            rows=values,
                            contexts={
                                row["passage_id"]: context for row in values
                            },
                        )
                updated += written
                batches += 1
                if written == 0:
                    # A concurrent writer filled these rows; do not spin.
                    break
            remaining = connection.execute(
                """SELECT EXISTS(
                       SELECT 1 FROM canonical_passages
                        WHERE header_redacted IS NULL
                          AND (%s::text='' OR tenant_id=%s)
                   ) AS value""",
                (tenant_scope, tenant_scope),
            ).fetchone()["value"]
            connection.commit()
        return {
            "status": "pending" if remaining else "complete",
            "updated": updated,
            "batches": batches,
        }

    def shadow_diff(
        self,
        *,
        tenant_id: str,
        source_id: str,
        limit: int = 50,
        _empty_only: bool = False,
        _after: str | None = None,
        _concurrency: int = 1,
    ) -> dict[str, Any]:
        """Read-only parity gate: recompute passages and compare with the rows.

        For up to ``limit`` passage documents of one source whose projection
        is current (pointer revision and source hash match the evidence
        catalog), rebuild the passage set from the archived logical document
        with the stored policy and the current id function. Reports, per
        document and in total, how many stored rows exist, how many were
        recomputed, how many ids are shared, and whether the multiset of
        receipts covered is identical. Nothing is written. The output carries
        ids and counts only, never passage text.
        """

        tenant_id = self._tenant(tenant_id)
        if (
            not isinstance(tenant_id, str)
            or not tenant_id
            or not isinstance(source_id, str)
            or not source_id
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_PASSAGE_PROJECTION_BATCH
            or type(_concurrency) is not int or not 1 <= _concurrency <= 32
        ):
            raise ValueError("passage shadow diff scope is invalid")
        with self.store.connect() as connection:
            rows = connection.execute(
                """SELECT projected.tenant_id,projected.source_id,
                          projected.logical_document_id,projected.revision,
                          projected.policy_fingerprint,projected.target_tokens,
                          projected.applied_extraction_version,
                          projected.requested_extraction_version,
                          projected.overlap_tokens,
                          projected.source_document_sha256,
                          projected.passage_count,
                          evidence.revision AS evidence_revision,
                          evidence.document_content_sha256,
                          evidence.manifest_artifact_id,
                          evidence.manifest_storage_backend,
                          evidence.manifest_object_key,
                          evidence.manifest_content_sha256,
                          evidence.manifest_size_bytes,
                          evidence.manifest_media_type,
                          evidence.manifest_encryption,
                          evidence.manifest_version_id,
                          evidence.created_at AS manifest_created_at,
                          part.part_ordinal,
                          part.artifact_id AS part_artifact_id,
                          part.storage_backend AS part_storage_backend,
                          part.object_key AS part_object_key,
                          part.content_sha256 AS part_content_sha256,
                          part.size_bytes AS part_size_bytes,
                          part.media_type AS part_media_type,
                          part.encryption AS part_encryption,
                          part.version_id AS part_version_id,
                          part.created_at AS part_created_at
                     FROM (
                           SELECT *
                             FROM canonical_passage_documents sampled
                            WHERE sampled.tenant_id=%s AND sampled.source_id=%s
                              AND (NOT %s OR (
                                  sampled.passage_count=0
                                  AND sampled.policy_fingerprint=%s
                                  AND NOT EXISTS (
                                      SELECT 1 FROM canonical_passage_projection_queue queued
                                       WHERE queued.tenant_id=sampled.tenant_id
                                         AND queued.source_id=sampled.source_id
                                         AND queued.logical_document_id=sampled.logical_document_id
                                         AND queued.reason<>'certification'
                                  )
                              ))
                              AND (%s::text IS NULL OR sampled.logical_document_id>%s)
                            ORDER BY CASE WHEN %s THEN sampled.logical_document_id END,
                                     sampled.created_at DESC,
                                     sampled.logical_document_id
                            LIMIT %s
                     ) projected
                     JOIN canonical_evidence_documents evidence
                       ON evidence.tenant_id=projected.tenant_id
                      AND evidence.source_id=projected.source_id
                      AND evidence.logical_document_id
                          =projected.logical_document_id
                     JOIN canonical_evidence_document_parts part
                       ON part.tenant_id=evidence.tenant_id
                      AND part.source_id=evidence.source_id
                      AND part.logical_document_id
                          =evidence.logical_document_id
                      AND part.revision=evidence.revision
                    ORDER BY projected.logical_document_id,part.part_ordinal""",
                (tenant_id, source_id, _empty_only, self.policy.fingerprint,
                 _after, _after, _empty_only, limit),
            ).fetchall()
            grouped: dict[str, list[dict[str, Any]]] = {}
            for row in rows:
                grouped.setdefault(row["logical_document_id"], []).append(row)
            existing_rows = connection.execute(
                """SELECT logical_document_id,passage_id,receipts
                     FROM canonical_passages
                    WHERE tenant_id=%s AND source_id=%s
                      AND logical_document_id=ANY(%s::text[])""",
                (tenant_id, source_id, sorted(grouped)),
            ).fetchall()
        existing: dict[str, dict[str, tuple[str, ...]]] = {}
        for row in existing_rows:
            existing.setdefault(row["logical_document_id"], {})[
                row["passage_id"]
            ] = tuple(row["receipts"])
        def compare_document(logical_document_id: str) -> dict[str, Any]:
            values = grouped[logical_document_id]
            first = values[0]
            stored = existing.get(logical_document_id, {})
            report: dict[str, Any] = {
                "logical_document_id": logical_document_id,
                "revision": int(first["revision"]),
                "passages_existing": len(stored),
            }
            if (
                int(first["evidence_revision"]) != int(first["revision"])
                or first["document_content_sha256"]
                != first["source_document_sha256"]
                or first["requested_extraction_version"] != first["applied_extraction_version"]
            ):
                # The stored projection lags the evidence catalog; the queue
                # will catch it up. Comparing would measure the append, not
                # the id function.
                report["status"] = "stale"
                return report
            policy = PassagePolicy(
                target_tokens=int(first["target_tokens"]),
                overlap_tokens=int(first["overlap_tokens"]),
            )
            if policy.fingerprint != str(first["policy_fingerprint"]).strip():
                report["status"] = "policy_mismatch"
                return report
            candidate = PassageCandidate(
                tenant_id=first["tenant_id"],
                source_id=first["source_id"],
                logical_document_id=logical_document_id,
                revision=int(first["revision"]),
                generation=0,
                changed_at=first["manifest_created_at"],
                source_document_sha256=first["document_content_sha256"],
                manifest_reference=self._reference(first, prefix="manifest_"),
                part_references=tuple(
                    self._reference(row, prefix="part_") for row in values
                ),
                extraction_version=first["applied_extraction_version"],
            )
            prepared = self._prepare(candidate, policy=policy)
            if _empty_only:
                embedding_bytes = sum(
                    len(passage.text.encode()) + MAX_PASSAGE_HEADER_BYTES
                    + len(PASSAGE_EMBEDDING_SEPARATOR.encode())
                    for passage in prepared.passages
                )
                report.update({
                    "source_document_sha256": candidate.source_document_sha256,
                    "policy_fingerprint": policy.fingerprint,
                    "archive_bytes": candidate.manifest_reference["size_bytes"]
                    + sum(ref["size_bytes"] for ref in candidate.part_references),
                    "embedding_bytes_estimate": embedding_bytes,
                    "embedding_tokens_estimate": math.ceil(
                        embedding_bytes / PASSAGE_PLAN_BYTES_PER_TOKEN
                    ),
                })
            recomputed = {
                passage.passage_id: passage.receipts
                for passage in prepared.passages
            }
            shared = stored.keys() & recomputed.keys()
            existing_receipts = Counter(
                receipt
                for receipts in stored.values()
                for receipt in receipts
            )
            recomputed_receipts = Counter(
                receipt
                for receipts in recomputed.values()
                for receipt in receipts
            )
            receipt_set_equal = existing_receipts == recomputed_receipts
            report.update({
                "status": "compared",
                "passages_recomputed": len(recomputed),
                "ids_shared": len(shared),
                "receipt_set_equal": receipt_set_equal,
            })
            return report

        # Each task releases its prepared bodies before returning metadata.
        # Catalog leases are already closed; no queue write occurs until every
        # comparison succeeds. map preserves document-ID report order.
        document_ids = sorted(grouped)
        if _concurrency == 1 or len(document_ids) < 2:
            documents = [compare_document(doc) for doc in document_ids]
        else:
            with ThreadPoolExecutor(
                max_workers=min(_concurrency, len(document_ids)),
                thread_name_prefix="recall-passage-shadow",
            ) as executor:
                documents = list(executor.map(compare_document, document_ids))
        totals = {
            "documents": 0,
            "documents_stale": 0,
            "documents_policy_mismatch": 0,
            "passages_existing": 0,
            "passages_recomputed": 0,
            "ids_shared": 0,
            "receipt_set_equal": 0,
        }
        for report in documents:
            if report["status"] == "stale":
                totals["documents_stale"] += 1
            elif report["status"] == "policy_mismatch":
                totals["documents_policy_mismatch"] += 1
            else:
                totals["documents"] += 1
                for key in ("passages_existing", "passages_recomputed", "ids_shared", "receipt_set_equal"):
                    totals[key] += int(report[key])
        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "source_id": source_id,
            "limit": limit,
            "read_only": True,
            "totals": totals,
            "receipt_parity": (
                totals["receipt_set_equal"] == totals["documents"]
            ),
            "documents": documents,
        }

    def requeue_receipt_repair(
        self,
        *,
        plan: dict[str, Any],
        principal_id: str,
        deadline_at: float | None = None,
    ) -> dict[str, Any]:
        """Queue an exact current projection with a proven stale receipt witness.

        No receipt/body writes or provider calls. The regular publisher owns repair.
        Existing pending work is never overwritten or inferred to be our outcome.
        """
        from .control import ControlPlane
        from . import passage_receipt_repair as receipt_repair

        unsigned = {key: value for key, value in plan.items() if key != "proof_sha256"}
        proof = hashlib.sha256(
            json.dumps(
                unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()
        ).hexdigest()
        if plan.get("proof_sha256") != proof:
            raise ValueError("plan_digest")
        tenant = self._tenant(plan["tenant_id"])
        if (
            plan["contract"] != "recall.passage-receipt-repair.v1"
            or plan["principal_id"] != principal_id
        ):
            raise ValueError("plan_scope")
        target = plan["target"]
        deadline = min(deadline_at or time.monotonic() + 15, time.monotonic() + 15)
        membership_enabled = getattr(self.store, "verified_singleton_receipts", False)
        with self.store.connect() as connection, connection.transaction():
            if membership_enabled:
                connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")

            def query(sql, args):
                return self.store._execute_bounded(
                    connection, sql, args, min(deadline, time.monotonic() + 5)
                )

            row = query(
                receipt_repair.CATALOG,
                (
                    tenant,
                    target["source_id"],
                    target["logical_document_id"],
                    target["passage_id"],
                    principal_id,
                ),
            ).fetchone()
            if row is None:
                raise ValueError("source_or_catalog_unavailable")
            source_owner = row.pop("owner_principal_id")
            source_permission = row.pop("source_permission")
            if source_owner != principal_id or source_permission != "owner":
                # A source read grant alone never permits maintenance. Reuse the
                # company's existing admin predicate, locked until queue commit.
                self.store._set_statement_deadline(
                    connection, min(deadline, time.monotonic() + 5)
                )
                ControlPlane._brain_admin(
                    connection, principal_id=principal_id, tenant_id=tenant, lock=True
                )
            receipts = row.pop("receipts")
            if dict(row) != plan["catalog"]:
                raise ValueError("catalog_changed")
            if (
                row["policy_fingerprint"] != self.policy.fingerprint
                or row["revision"] != row["projected_revision"]
                or row["revision"] != row["passage_revision"]
                or row["document_content_sha256"] != row["source_document_sha256"]
            ):
                raise ValueError("projection_not_current")
            receipt_digest = hashlib.sha256(
                json.dumps(
                    receipts, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                ).encode()
            ).hexdigest()
            if (
                not receipts
                or receipt_digest != plan["witness_receipt_digest"]
            ):
                raise ValueError("witness_changed")
            pending = query(
                """SELECT
                EXISTS(SELECT 1 FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s) OR
                EXISTS(SELECT 1 FROM canonical_passage_projection_queue WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s AND reason<>'certification') AS pending""",
                (
                    tenant,
                    target["source_id"],
                    row["native_parent_id"],
                    tenant,
                    target["source_id"],
                    target["logical_document_id"],
                ),
            ).fetchone()["pending"]
            if pending:
                raise ValueError("projection_pending")
            if membership_enabled:
                from .receipt_membership import lookup_current_search_membership

                members = lookup_current_search_membership(
                    self.store, tenant_id=tenant,
                    authorized_sources=(target["source_id"],), receipts=tuple(receipts),
                    connection=connection, deadline_at=min(deadline, time.monotonic() + 5),
                )
                stale = sum((target["source_id"], receipt) not in members for receipt in receipts)
            else:
                stale = query(
                    """SELECT count(*) AS count FROM unnest(%s::text[]) receipt(value)
                    WHERE NOT EXISTS(SELECT 1 FROM canonical_chunks chunk JOIN canonical_documents document
                         ON document.tenant_id=chunk.tenant_id AND document.source_id=chunk.source_id
                        AND document.document_id=chunk.document_id
                        WHERE chunk.tenant_id=%s AND chunk.source_id=%s AND chunk.receipt=receipt.value
                          AND chunk.deleted_at IS NULL AND document.is_current AND document.deleted_at IS NULL)""",
                    (receipts, tenant, target["source_id"]),
                ).fetchone()["count"]
            if stale != plan["stale_receipts"] or stale <= 0:
                raise ValueError("witness_changed")
            if "supersession" in plan:
                receipt_repair.validate_admission(
                    self.store, connection, plan, deadline
                )
            inserted = query(
                """INSERT INTO canonical_passage_projection_queue(
                tenant_id,source_id,logical_document_id,revision,generation,reason,changed_at)
                VALUES(%s,%s,%s,%s,1,'backfill',clock_timestamp())
                ON CONFLICT(tenant_id,source_id,logical_document_id) DO UPDATE SET
                    revision=excluded.revision,
                    generation=canonical_passage_projection_queue.generation+1,
                    reason='backfill',changed_at=clock_timestamp()
                WHERE canonical_passage_projection_queue.reason='certification'""",
                (
                    tenant,
                    target["source_id"],
                    target["logical_document_id"],
                    row["revision"],
                ),
            )
            if inserted.rowcount != 1:
                raise ValueError("projection_pending")
            if "supersession" in plan:
                # A current replacement is not proof that the archived logical
                # manifest contains it. Reuse the native logical rebuild owner
                # atomically, so either lane order eventually publishes current
                # source records. An unchanged manifest takes its existing noop.
                self.store._set_statement_deadline(connection, deadline)
                logical = self._requeue_missing(SimpleNamespace(
                    tenant_id=tenant, source_id=target["source_id"],
                    logical_document_id=target["logical_document_id"],
                    revision=row["revision"],
                ), connection=connection)
                if logical != 1:
                    raise ValueError("catalog_changed")
        return {"status": "queued", "proof_sha256": proof, "repair_complete": False}

    def repair_empty(
        self,
        *,
        tenant_id: str,
        source_id: str,
        limit: int = 25,
        after: str | None = None,
        apply: bool = False,
        price_per_mtoken: float | None = None,
        concurrency: int = 1,
    ) -> dict[str, Any]:
        """Plan one source's empty-projection repair; optionally queue that batch.

        Reads archived evidence through the existing shadow projector. Never
        embeds or executes projection work. The existing worker owns commits.
        The cursor allows finite batches without a permanent corpus ceiling.
        """
        tenant_id = self._tenant(tenant_id)
        if (
            not isinstance(tenant_id, str) or not tenant_id
            or not isinstance(source_id, str) or not source_id
            or type(limit) is not int or not 1 <= limit <= MAX_PASSAGE_PROJECTION_BATCH
            or type(apply) is not bool
            or type(concurrency) is not int or not 1 <= concurrency <= 32
            or (after is not None and (
                not isinstance(after, str) or LOGICAL_DOCUMENT_ID_RE.fullmatch(after) is None
            ))
            or (price_per_mtoken is not None and (
                type(price_per_mtoken) not in (int, float)
                or not math.isfinite(price_per_mtoken) or price_per_mtoken < 0
            ))
        ):
            raise ValueError("empty passage repair scope is invalid")
        report = self.shadow_diff(
            tenant_id=tenant_id, source_id=source_id, limit=limit,
            _empty_only=True, _after=after, _concurrency=concurrency,
        )
        documents = report["documents"]
        eligible = [doc for doc in documents
                    if doc["status"] == "compared" and doc["passages_existing"] == 0
                    and doc["passages_recomputed"] > 0]
        queued = 0
        if apply and eligible:
            with self.store.connect() as connection:
                with connection.transaction():
                    result = connection.execute(
                        """INSERT INTO canonical_passage_projection_queue(
                               tenant_id,source_id,logical_document_id,revision,
                               generation,reason,changed_at
                           )
                           SELECT evidence.tenant_id,evidence.source_id,
                                  evidence.logical_document_id,evidence.revision,
                                  1,'backfill',clock_timestamp()
                             FROM canonical_evidence_documents evidence
                             JOIN canonical_passage_documents projected
                               USING(tenant_id,source_id,logical_document_id)
                             JOIN jsonb_to_recordset(%s::jsonb) selected(
                                 logical_document_id text,revision integer,
                                 source_document_sha256 text,policy_fingerprint text
                             ) ON selected.logical_document_id=evidence.logical_document_id
                            WHERE evidence.tenant_id=%s AND evidence.source_id=%s
                              AND evidence.revision=selected.revision
                              AND evidence.document_content_sha256=selected.source_document_sha256
                              AND projected.revision=evidence.revision
                              AND projected.source_document_sha256=evidence.document_content_sha256
                              AND projected.policy_fingerprint=selected.policy_fingerprint
                              AND projected.passage_count=0
                           ON CONFLICT(tenant_id,source_id,logical_document_id)
                           DO UPDATE SET revision=excluded.revision,
                               generation=canonical_passage_projection_queue.generation+1,
                               reason='backfill',changed_at=clock_timestamp()
                           WHERE canonical_passage_projection_queue.reason='certification'""",
                        (json.dumps(eligible), tenant_id, source_id),
                    )
                    queued = max(0, result.rowcount)
        tokens = sum(doc.get("embedding_tokens_estimate", 0) for doc in eligible)
        return {
            "status": "queued" if apply else "planned",
            "read_only": not apply, "tenant_id": tenant_id, "source_id": source_id,
            "limit": limit, "after": after,
            "next_after": max((doc["logical_document_id"] for doc in documents), default=None),
            "documents_examined": len(documents), "eligible_documents": len(eligible),
            "queued": queued,
            "archive_bytes": sum(doc.get("archive_bytes", 0) for doc in documents),
            "embedding_bytes_estimate": sum(doc.get("embedding_bytes_estimate", 0) for doc in eligible),
            "embedding_tokens_estimate": tokens,
            "price_per_mtoken": price_per_mtoken,
            "embedding_cost_usd_estimate": (
                None if price_per_mtoken is None else tokens / 1_000_000 * price_per_mtoken
            ),
            "estimate_basis": "UTF-8 bytes / 4; maximum context header per new passage; advisory batch estimate only",
            "documents": documents,
        }

    def project_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 25,
        max_batches: int = 10,
        concurrency: int = 2,
        on_progress: Callable[[], None] | None = None,
        certification_budget: CertificationBudget | None = None,
        certification_batch_size: int | None = None,
        defer_count: bool = False,
    ) -> dict[str, int | str | None]:
        """Publish bounded waves; deferred depth stays unknown until counted."""
        tenant_id = self._tenant(tenant_id)
        certification_budget = certification_budget or CertificationBudget()
        if certification_batch_size is None:
            certification_batch_size = batch_size
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= MAX_PASSAGE_PROJECTION_BATCH
            or isinstance(max_batches, bool)
            or not isinstance(max_batches, int)
            or not 1 <= max_batches <= 100
            or isinstance(concurrency, bool)
            or not isinstance(concurrency, int)
            or not 1 <= concurrency <= 32
            or isinstance(certification_batch_size, bool)
            or not isinstance(certification_batch_size, int)
            or certification_batch_size < 1
        ):
            raise ValueError("passage projection budget is invalid")
        phase_started = time.monotonic()
        prepare_pool = getattr(self.store, "prepare_pool", None)
        if callable(prepare_pool):
            prepare_pool(min(PASSAGE_POOL_WARM_SIZE, concurrency))
        warmup_seconds = time.monotonic() - phase_started
        pending_seconds = prepare_seconds = commit_seconds = 0.0
        started = time.monotonic()
        documents = passages = stale = requeued = unavailable = batches = 0
        deleted = retained = 0
        membership_certified = membership_failed = membership_deferred = 0
        membership_failures = []
        membership_elapsed_ms = 0
        preparation_counts = Counter()
        while batches < max_batches:
            phase_started = time.monotonic()
            recent = self._prefer_notification_admission
            previous_parent_cursors = self._ordinary_parent_cursor[recent].copy()
            candidates = self._pending(
                tenant_id=tenant_id,
                limit=batch_size,
            )
            pending_seconds += time.monotonic() - phase_started
            if not candidates:
                break
            stopping = threading.Event()
            commit_slots = threading.BoundedSemaphore(min(
                concurrency, len(candidates),
                max(1, self.store.pool_max_size - 1), PASSAGE_COMMIT_WORKERS,
            ))

            def project_document(candidate):
                # The owner releases the prepared body after its own commit;
                # futures retain scalar results, never a batch of passage text.
                preparation_started = time.monotonic()
                try:
                    prepared = self._prepare(candidate, incremental=True)
                except LogicalEvidenceError as error:
                    if str(error) not in {"logical_evidence_not_found", "logical_evidence_unavailable", "legacy_tool_migration_required"}:
                        raise
                    return {"status": str(error)}, time.monotonic() - preparation_started, 0.0
                try:
                    preparation_elapsed = time.monotonic() - preparation_started
                    commit_started = time.monotonic()
                    with commit_slots:
                        if stopping.is_set():
                            return {"status": "cancelled"}, preparation_elapsed, 0.0
                        try:
                            status = self._commit(prepared)
                        except psycopg.errors.LockNotAvailable:
                            # _commit has rolled back and released its locks. Keep
                            # the unchanged queue for retry without aborting siblings.
                            status = {"status": "stale"}
                    commit_elapsed = time.monotonic() - commit_started
                    should_certify = (status['status'] == 'committed'
                                      and self.membership_certification.enabled
                                      and status.get('membership_legacy', False))
                    passage_ids = (tuple(passage.passage_id for passage in prepared.passages)
                                   if should_certify else ())
                    status['archive_bytes_read'] = prepared.archive_bytes_read
                    status['archive_parts_read'] = prepared.archive_parts_read
                    status['prepare_reused_documents'] = int(prepared.previous_checkpoint is not None)
                    status['prepare_full_documents'] = int(prepared.previous_checkpoint is None)
                finally:
                    prepared.close()
                if should_certify:
                    with certification_budget.lease() as certification_deadline:
                        if certification_deadline is None:
                            status['membership_deferred'] = 1
                            return status, preparation_elapsed, commit_elapsed
                        certification_started = time.monotonic()
                        try:
                            certification = parent_membership_enrollment.certify_published_passages(
                                self.store, tenant_id=candidate.tenant_id, source_id=candidate.source_id,
                                logical_document_id=candidate.logical_document_id, passage_ids=passage_ids,
                                settings=self.membership_certification, deadline_at=certification_deadline)
                            status['membership_certified'] = certification['certified_passages']
                            status['membership_failed'] = int(certification['status'] == 'failed')
                            if 'failure' in certification:
                                status['membership_failure'] = certification['failure']
                            status['membership_deferred'] = int(
                                certification['status'] not in {'enrolled', 'failed'})
                        except Exception as error:
                            # Publication and its queue ACK have committed. Optional
                            # proof failure must never replay publication or ingest.
                            status['membership_failed'] = 1
                            status['membership_failure'] = parent_membership_enrollment.certification_failure(
                                error, source_id=candidate.source_id,
                                logical_document_id=candidate.logical_document_id)
                        finally:
                            status['membership_elapsed_ms'] = max(
                                0, round((time.monotonic() - certification_started) * 1000))
                return status, preparation_elapsed, commit_elapsed

            requeued_in_batch = unavailable_in_batch = 0
            yielded_parents = []
            with ThreadPoolExecutor(
                max_workers=min(concurrency, len(candidates)),
                thread_name_prefix="recall-passage-projector",
            ) as executor:
                futures = {executor.submit(project_document, candidate): candidate for candidate in candidates}
                next_progress = time.monotonic() + PROJECTION_PROGRESS_INTERVAL_SECONDS
                try:
                    while futures:
                        completed, _ = wait(
                            futures,
                            timeout=max(0.0, next_progress - time.monotonic())
                            if on_progress is not None else None,
                            return_when=FIRST_COMPLETED,
                        )
                        publish = False
                        for future in completed:
                            candidate = futures.pop(future)
                            status, preparation_elapsed, commit_elapsed = future.result()
                            for key in ('archive_bytes_read', 'archive_parts_read',
                                        'prepare_reused_documents', 'prepare_full_documents'):
                                preparation_counts[key] += int(status.get(key, 0))
                            prepare_seconds += preparation_elapsed
                            commit_seconds += commit_elapsed
                            if status["status"] in {
                                "logical_evidence_not_found", "logical_evidence_unavailable",
                                "legacy_tool_migration_required", "stale",
                            }:
                                yielded_parents.append((candidate.tenant_id, candidate.source_id,
                                                        candidate.changed_at, candidate.logical_document_id))
                            if status["status"] in {
                                "logical_evidence_not_found", "logical_evidence_unavailable",
                                "legacy_tool_migration_required",
                            }:
                                LOG.warning("passage preparation refused source_id=%r logical_document_id=%r status=%s",
                                            candidate.source_id, candidate.logical_document_id, status["status"])
                            if status["status"] == "logical_evidence_not_found":
                                # Keep logical requeue on the coordinator's
                                # reserved pool connection, outside commit owners.
                                requeued_in_batch += self._requeue_missing(candidate)
                                continue
                            if status["status"] in {"logical_evidence_unavailable", "legacy_tool_migration_required"}:
                                unavailable_in_batch += 1
                                continue
                            if status["status"] == "stale":
                                stale += 1
                                continue
                            membership_certified += int(status.get('membership_certified', 0))
                            membership_failed += int(status.get('membership_failed', 0))
                            if 'membership_failure' in status:
                                membership_failures.append(status['membership_failure'])
                            membership_deferred += int(status.get('membership_deferred', 0))
                            membership_elapsed_ms += int(status.get('membership_elapsed_ms', 0))
                            documents += 1
                            passages += int(status["inserted"])
                            deleted += int(status["deleted"])
                            retained += int(status["retained"])
                            publish = True
                        # Search publication has one coordinator; preparation
                        # and commits continue in the bounded executor meanwhile.
                        if on_progress is not None and (publish or time.monotonic() >= next_progress):
                            on_progress()
                            next_progress = time.monotonic() + PROJECTION_PROGRESS_INTERVAL_SECONDS
                except BaseException:
                    stopping.set()
                    for future in futures:
                        future.cancel()
                    raise
            self._yield_unavailable_parents(yielded_parents, recent=recent, previous=previous_parent_cursors)
            requeued += requeued_in_batch
            unavailable += unavailable_in_batch
            batches += 1
            if requeued_in_batch or unavailable_in_batch:
                # Yield to the logical projector in the unified worker. The
                # passage queue remains authoritative and retries next cycle.
                # A transient archive failure must never become a destructive
                # logical-document rebuild.
                break
        optional = self.certify_pending(tenant_id=tenant_id,
            batch_size=certification_batch_size, certification_budget=certification_budget)
        membership_certified += optional['membership_certified_passages']
        membership_failed += optional['membership_failed_documents']
        membership_deferred += optional['membership_deferred_documents']
        membership_elapsed_ms += optional['membership_elapsed_ms']
        membership_failures.extend(optional['membership_failures'])
        pending_summary = (
            {"status": "unknown", "pending": None, "count_ms": 0}
            if defer_count else self.count_pending(tenant_id=tenant_id)
        )
        elapsed_seconds = max(0.001, time.monotonic() - started)
        return {
            **pending_summary,
            "documents": documents,
            "membership_certified_passages": membership_certified,
            "membership_failed_documents": membership_failed,
            "membership_failures": membership_failures,
            "membership_deferred_documents": membership_deferred,
            # Sum of optional owner durations; concurrent hooks may overlap.
            "membership_elapsed_ms": membership_elapsed_ms,
            "passages": passages,
            "inserted": passages,
            "deleted": deleted,
            "retained": retained,
            "stale": stale,
            "requeued": requeued,
            "unavailable": unavailable,
            "batches": batches,
            # Prepare/commit are accumulated owner durations and can overlap;
            # commit includes cap/connection waits. Other phases are coordinator
            # wall time. Failure diagnostics contain identities but no passage text.
            "warmup_ms": max(0, round(warmup_seconds * 1000)),
            "pending_ms": max(0, round(pending_seconds * 1000)),
            "prepare_ms": max(0, round(prepare_seconds * 1000)),
            # Successful archive reads, including an abandoned prefix attempt
            # before a full fallback. These measure work, not total history size.
            "archive_bytes_read": preparation_counts['archive_bytes_read'],
            "archive_parts_read": preparation_counts['archive_parts_read'],
            "prepare_reused_documents": preparation_counts['prepare_reused_documents'],
            "prepare_full_documents": preparation_counts['prepare_full_documents'],
            "commit_ms": max(0, round(commit_seconds * 1000)),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "documents_per_second": round(
                documents / elapsed_seconds,
                3,
            ),
            "passages_per_second": round(
                passages / elapsed_seconds,
                3,
            ),
        }

    def count_pending(self, *, tenant_id: str | None = None) -> dict[str, int | str]:
        """Read exact ordinary queue depth at a caller's scheduling boundary."""
        tenant_id = self._tenant(tenant_id)
        phase_started = time.monotonic()
        with self.store.connect() as connection:
            pending = connection.execute(
                """SELECT count(*) AS count
                     FROM canonical_passage_projection_queue
                    WHERE (%s::text IS NULL OR tenant_id=%s) AND reason<>'certification'""",
                (tenant_id, tenant_id),
            ).fetchone()["count"]
        count_seconds = time.monotonic() - phase_started
        return {
            "status": "complete" if int(pending) == 0 else "pending",
            "pending": int(pending),
            "count_ms": max(0, round(count_seconds * 1000)),
        }

    def certify_pending(self, *, tenant_id, batch_size, certification_budget):
        """Drain existing optional work without preparing or publishing sources."""
        membership_certified = membership_failed = membership_deferred = membership_elapsed_ms = 0
        membership_failures = []
        if self.membership_certification.enabled:
            with certification_budget.lease() as certification_deadline:
                if certification_deadline is not None:
                    certification_started = time.monotonic()
                    try:
                        row = None
                        certification_rows = membership_remainder.pending(self.store,
                            tenant_id=tenant_id, limit=batch_size,
                            deadline_at=certification_deadline,
                            after_source=self._certification_source_cursor)
                        for row in certification_rows:
                            if time.monotonic() >= certification_deadline:
                                break
                            self._certification_source_cursor = (row["tenant_id"], row["source_id"])
                            outcome = membership_remainder.resume(self.store, row,
                                settings=self.membership_certification, deadline_at=certification_deadline)
                            membership_certified += outcome['certified_passages']
                            membership_failed += int(outcome['status'] == 'failed')
                            if 'failure' in outcome:
                                membership_failures.append(outcome['failure'])
                            membership_deferred += int(outcome['status'] not in {'enrolled', 'failed'})
                    except Exception as error:
                        membership_failed += 1
                        membership_failures.append(parent_membership_enrollment.certification_failure(
                            error, source_id=row["source_id"] if row is not None else None,
                            logical_document_id=row["logical_document_id"] if row is not None else None))
                    finally:
                        membership_elapsed_ms += max(0, round((time.monotonic()-certification_started)*1000))
        return dict(membership_certified_passages=membership_certified,
                    membership_failed_documents=membership_failed,
                    membership_deferred_documents=membership_deferred,
                    membership_elapsed_ms=membership_elapsed_ms,
                    membership_failures=membership_failures)

    def embed_pending(
        self,
        *,
        tenant_id: str | None = None,
        batch_size: int = 100,
        max_batches: int = 10,
        shard_count: int = 1,
        shard_index: int = 0,
        max_passages: int | None = None,
    ) -> dict[str, int | str]:
        """Embed only lossless passages missing the selected runtime fingerprint.

        ``max_passages`` (H5-3) is the caller's remaining daily budget: the call
        never sends more passages than that to the provider, shrinking the last
        batch as needed.
        """

        tenant_id = self._tenant(tenant_id)
        if not postgres_vector_plane(self.store):
            # H3-e': turbopuffer embeds ``embed_text`` natively on write; no
            # passage is pending here and the embeddings table is not read.
            return {
                "status": "not-applicable",
                "processed": 0,
                "batches": 0,
                "plane": "turbopuffer",
            }
        runtime = self.store.semantic_runtime
        if runtime is None:
            return {"status": "disabled", "processed": 0, "batches": 0}
        if runtime.dimensions != 512:
            raise ValueError("passage embeddings require 512 dimensions")
        if max_passages is not None and (
            isinstance(max_passages, bool)
            or not isinstance(max_passages, int)
            or max_passages < 1
        ):
            raise ValueError("passage embedding budget is invalid")
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= MAX_PASSAGE_EMBEDDING_BATCH
            or isinstance(max_batches, bool)
            or not isinstance(max_batches, int)
            or not 1 <= max_batches <= 100
            or isinstance(shard_count, bool)
            or not isinstance(shard_count, int)
            or not 1 <= shard_count <= 64
            or isinstance(shard_index, bool)
            or not isinstance(shard_index, int)
            or not 0 <= shard_index < shard_count
        ):
            raise ValueError("passage embedding budget is invalid")
        # H2-a: the worker writes the contract the runtime declares (v2 =
        # header + text keyed by embed_sha256; v1 = text keyed by
        # text_sha256). Runtimes without the v2 surface (synthetic test
        # runtimes) behave as v2 under their single fingerprint.
        write_contract = getattr(runtime, "passage_write_contract", "v2")
        write_fingerprint = getattr(
            runtime,
            "passage_write_fingerprint",
            runtime.passage_fingerprint,
        )
        headed = write_contract == "v2"
        processed = batches = 0
        tenant_scope = tenant_id or ""
        headers_backfilled = 0
        if headed:
            # Rows projected before schema 064 get their header from the
            # catalog first, so the v2 key exists for every candidate.
            headers_backfilled = int(self.backfill_headers(
                tenant_id=tenant_id,
                batch_size=min(MAX_PASSAGE_HEADER_BACKFILL_BATCH, batch_size * 5),
                max_batches=max_batches,
            )["updated"])
        lock_name = f"recall:lossless-passage-embeddings:{tenant_scope}"
        if shard_count > 1:
            lock_name += f":shard:{shard_index}:{shard_count}"
        with self.store.connect() as connection:
            locked = connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS value",
                (lock_name,),
            ).fetchone()["value"]
            connection.commit()
            if not locked:
                return {"status": "busy", "processed": 0, "batches": 0}
            try:
                while batches < max_batches:
                    # H5-3 budget: the last batch shrinks to what the daily cap
                    # still allows; when nothing is allowed the loop stops.
                    batch_limit = (
                        batch_size
                        if max_passages is None
                        else min(batch_size, max_passages - processed)
                    )
                    if batch_limit <= 0:
                        break
                    rows = connection.execute(
                        f"""SELECT passage.tenant_id,passage.source_id,
                                  passage.passage_id,passage.text_redacted,
                                  passage.text_sha256,passage.header_redacted,
                                  passage.embed_sha256
                             FROM canonical_passages passage
                             JOIN canonical_passage_documents document
                               USING(
                                   tenant_id,source_id,logical_document_id,
                                   policy_fingerprint
                               )
                             LEFT JOIN canonical_passage_embeddings embedding
                               ON embedding.tenant_id=passage.tenant_id
                              AND embedding.source_id=passage.source_id
                              AND embedding.passage_id=passage.passage_id
                              AND embedding.runtime_fingerprint=%s
                              AND embedding.content_sha256=passage.{
                                  "embed_sha256" if headed else "text_sha256"
                              }
                            WHERE document.policy_fingerprint=%s
                              AND (%s::text='' OR passage.tenant_id=%s)
                              AND (
                                  (
                                      hashtextextended(passage.passage_id,0)
                                      & 9223372036854775807
                                  ) %% %s
                              )=%s
                              AND embedding.passage_id IS NULL
                              {
                                  "AND passage.header_redacted IS NOT NULL"
                                  if headed else ""
                              }
                            ORDER BY passage.tenant_id,passage.source_id,
                                     passage.passage_id
                            LIMIT %s""",
                        (
                            write_fingerprint,
                            self.policy.fingerprint,
                            tenant_scope,
                            tenant_scope,
                            shard_count,
                            shard_index,
                            batch_limit,
                        ),
                    ).fetchall()
                    connection.commit()
                    if not rows:
                        break
                    vectors = runtime.embed_passages(
                        [
                            passage_embedding_input(
                                row["header_redacted"] if headed else None,
                                row["text_redacted"],
                            )
                            for row in rows
                        ]
                    )
                    with connection.transaction():
                        with connection.cursor() as cursor:
                            cursor.executemany(
                                """INSERT INTO canonical_passage_embeddings(
                                       tenant_id,source_id,passage_id,model,
                                       dimensions,content_sha256,
                                       runtime_fingerprint,embedding
                                   ) VALUES (%s,%s,%s,%s,512,%s,%s,%s::halfvec)
                                   ON CONFLICT(
                                       tenant_id,source_id,passage_id
                                   )
                                   DO UPDATE SET
                                       model=excluded.model,
                                       dimensions=excluded.dimensions,
                                       content_sha256=excluded.content_sha256,
                                       runtime_fingerprint=
                                           excluded.runtime_fingerprint,
                                       embedding=excluded.embedding,
                                       embedded_at=now()""",
                                [
                                    (
                                        row["tenant_id"],
                                        row["source_id"],
                                        row["passage_id"],
                                        runtime.model,
                                        row["embed_sha256"]
                                        if headed
                                        else row["text_sha256"],
                                        write_fingerprint,
                                        vector,
                                    )
                                    for row, vector in zip(
                                        rows,
                                        vectors,
                                        strict=True,
                                    )
                                ],
                            )
                    processed += len(rows)
                    batches += 1
                pending = connection.execute(
                    f"""SELECT EXISTS(
                           SELECT 1
                             FROM canonical_passages passage
                             JOIN canonical_passage_documents document
                               USING(
                                   tenant_id,source_id,logical_document_id,
                                   policy_fingerprint
                               )
                             LEFT JOIN canonical_passage_embeddings embedding
                               ON embedding.tenant_id=passage.tenant_id
                              AND embedding.source_id=passage.source_id
                              AND embedding.passage_id=passage.passage_id
                              AND embedding.runtime_fingerprint=%s
                              AND embedding.content_sha256=passage.{
                                  "embed_sha256" if headed else "text_sha256"
                              }
                            WHERE document.policy_fingerprint=%s
                              AND (%s::text='' OR passage.tenant_id=%s)
                              AND (
                                  (
                                      hashtextextended(passage.passage_id,0)
                                      & 9223372036854775807
                                  ) %% %s
                              )=%s
                              AND embedding.passage_id IS NULL
                       ) AS value""",
                    (
                        write_fingerprint,
                        self.policy.fingerprint,
                        tenant_scope,
                        tenant_scope,
                        shard_count,
                        shard_index,
                    ),
                ).fetchone()["value"]
                connection.commit()
                return {
                    "status": "pending" if pending else "complete",
                    "processed": processed,
                    "batches": batches,
                    "contract": write_contract,
                    "headers_backfilled": headers_backfilled,
                }
            finally:
                connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended(%s,0))",
                    (lock_name,),
                )
                connection.commit()
