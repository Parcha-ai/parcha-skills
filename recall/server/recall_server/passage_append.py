"""Versioned append checkpoints for the native passage projector."""
from dataclasses import dataclass, replace
import hashlib
import json
import os

from contracts.source_units import codex_history_source_unit_index
from .passage_projection import COMPLETE_SOURCE_UNITS, build_passages, passage_identity

# Change whenever visible extraction, tokenization, or source-unit grouping changes.
# Version 1 can describe historical visible tool-result concatenations. Rebuild
# it once before resuming with the current normal/typed extraction domains.
# Version 3 includes Claude request-replay tool leaves and single-owner handbacks,
# both introduced after version 2. Other source extraction remains compatible.
CHECKPOINT_VERSION = 3
# Codex epoch 6 includes visible messages in captured replacement history.
# Other source contracts retain their existing epoch.
CODEX_CHECKPOINT_VERSION = 6


def checkpoint_version(source_id):
    return CODEX_CHECKPOINT_VERSION if source_id.startswith('codex:') else CHECKPOINT_VERSION


def reuse_enabled():
    value = os.environ.get('RECALL_PASSAGE_APPEND_REUSE', '1')
    if value not in {'0', '1'}:
        raise ValueError('RECALL_PASSAGE_APPEND_REUSE must be 0 or 1')
    return value == '1'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=False).encode()).hexdigest()


@dataclass(frozen=True)
class RetainedPassage:
    """Exact existing metadata; never an inserted passage or replacement body."""
    passage_id: str
    ordinal: int
    receipts: tuple[str, ...]


def prefix_digest(passages):
    # Hash the same canonical JSON array without retaining the whole prefix.
    result = hashlib.sha256(b'[')
    separator = b''
    for passage in passages:
        result.update(separator)
        result.update(json.dumps((passage.passage_id, passage.ordinal, list(passage.receipts)),
            sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode())
        separator = b','
    result.update(b']')
    return result.hexdigest()


def resume_passages(messages, checkpoint, arguments):
    start, byte = checkpoint['message_index'], checkpoint['source_byte_start']
    tail = (replace(messages[0], text=messages[0].text.encode()[byte:].decode()),) + messages[1:]
    result = []
    for passage in build_passages(messages=tail, **arguments):
        spans = tuple(replace(span, message_index=span.message_index + start,
            source_byte_start=span.source_byte_start + (byte if span.message_index == 0 else 0),
            source_byte_end=span.source_byte_end + (byte if span.message_index == 0 else 0))
            for span in passage.spans)
        identity = passage_identity(tenant_id=passage.tenant_id, source_id=passage.source_id,
            logical_document_id=passage.logical_document_id, policy_fingerprint=passage.policy_fingerprint,
            text_sha256=passage.text_sha256, spans=spans)
        result.append(replace(passage, passage_id=identity, spans=spans,
                              ordinal=passage.ordinal + checkpoint['passage_ordinal']))
    return tuple(result)


def make_checkpoint(candidate, manifest, messages, passages, policy, *, previous=None):
    version = checkpoint_version(getattr(candidate, 'source_id', ''))
    if not passages or not messages:
        if not candidate.source_id.startswith(('claude:', 'codex:')):
            return None
        # Record completed extraction even when no reusable visible prefix exists.
        # Missing prefix fields deliberately cannot satisfy proves_append.
        return dict(version=version, full_rebuild_only=True,
            source_document_sha256=candidate.source_document_sha256,
            revision=candidate.revision,
            manifest_sha256=candidate.manifest_reference['content_sha256'])
    message_base = previous['message_index'] if previous else 0
    prefix_bytes = previous['prefix_message_bytes'] if previous else 0
    last = passages[-1]
    first = last.spans[0]
    # Native reads restart at a record boundary. Walk the existing sequence
    # backwards once, including an earlier history record if a window overlaps
    # it. This keeps a copied history message from becoming the wrong restart.
    previous_index = len(passages) - 2
    while codex_history_source_unit_index(first.source_unit) is not None and previous_index >= 0:
        earlier = passages[previous_index]
        if not any(span.record_ordinal == first.record_ordinal
                   and codex_history_source_unit_index(span.source_unit) is not None
                   for span in getattr(earlier, 'spans', ())):
            break
        last = earlier
        first = last.spans[0]
        previous_index -= 1
    index, byte, ordinal = first.message_index, first.source_byte_start, last.ordinal
    local_index = index - message_base
    if messages[local_index].source_unit in COMPLETE_SOURCE_UNITS:
        byte = 0
        ordinal = next(p.ordinal for p in passages if not isinstance(p, RetainedPassage)
                       and any(s.message_index == index for s in p.spans))
    return dict(version=version, policy_fingerprint=policy.fingerprint,
        source_document_sha256=candidate.source_document_sha256, revision=candidate.revision,
        manifest_sha256=candidate.manifest_reference['content_sha256'],
        part_count=len(manifest['parts']), sealed_parts_sha256=digest(manifest['parts'][:-1]),
        last_part=manifest['parts'][-1], message_index=index, source_byte_start=byte,
        passage_ordinal=ordinal, record_ordinal=messages[local_index].record_ordinal,
        prefix_message_bytes=prefix_bytes + sum(len(m.text.encode()) for m in messages[:local_index]),
        retained_prefix_sha256=prefix_digest(passages[:ordinal]))


def proves_append(checkpoint, projected, candidate, manifest, read_part):
    """Prove the old record stream is an exact prefix without reading old objects."""
    try:
        # Changed source contracts rebuild their old extraction once. Unchanged
        # sources retain the original v1/v2 prefix proof.
        source_id = getattr(candidate, 'source_id', '')
        compatible = isinstance(checkpoint, dict) and (
            checkpoint.get('version') == checkpoint_version(source_id)
            or (checkpoint.get('version') in (1, 2)
                and not source_id.startswith(('claude:', 'codex:'))))
        if (not compatible
            or checkpoint['policy_fingerprint'] != projected['policy_fingerprint']
            or checkpoint['revision'] != projected['revision']
            or checkpoint['source_document_sha256'] != projected['source_document_sha256']
            or candidate.revision <= projected['revision']):
            return False
        for name in ('part_count','message_index','source_byte_start','passage_ordinal',
                     'record_ordinal','prefix_message_bytes'):
            if type(checkpoint.get(name)) is not int or checkpoint[name] < 0:
                return False
        if not isinstance(checkpoint.get('retained_prefix_sha256'), str):
            return False
        count = checkpoint['part_count']
        if type(count) is not int or not 1 <= count <= len(manifest['parts']):
            return False
        if digest(manifest['parts'][:count-1]) != checkpoint['sealed_parts_sha256']:
            return False
        old, new = checkpoint['last_part'], manifest['parts'][count-1]
        if old['first_record_ordinal'] != new['first_record_ordinal']:
            return False
        if old != new:
            body = read_part(count-1)
            size = old['size_bytes']
            if (type(size) is not int or not 0 < size <= len(body)
                or body[size-1:size] != b'\n'
                or hashlib.sha256(body[:size]).hexdigest() != old['content_sha256']):
                return False
        return (0 <= checkpoint['record_ordinal'] <= old['last_record_ordinal']
                and 0 <= checkpoint['message_index'] < projected['dense_message_count']
                and 0 <= checkpoint['passage_ordinal'] < projected['passage_count']
                and checkpoint['prefix_message_bytes'] <= projected['dense_message_bytes']
                and checkpoint['source_byte_start'] >= 0)
    except (KeyError, TypeError, ValueError):
        return False


def grouped_checkpoint_matches(checkpoint, projected, candidate):
    """Typed groups have a second retained range after the visible overlap."""
    try:
        groups = checkpoint['tool_groups']
        if (type(groups['version']) is not int or groups['version'] != 1
            or groups['extraction_version'] != candidate.extraction_version
            or projected['requested_extraction_version'] != candidate.extraction_version
            or projected['applied_extraction_version'] != candidate.extraction_version
            or not isinstance(groups['coverage'], dict)
            or groups['coverage'] != projected['extraction_coverage']):
            return False
        for key in ('visible_passages', 'tool_passages', 'record_count', 'messages', 'bytes'):
            if type(groups[key]) is not int or groups[key] < 0:
                return False
        return (checkpoint['passage_ordinal'] < groups['visible_passages']
            and groups['visible_passages'] + groups['tool_passages'] == projected['passage_count']
            and groups['record_count'] == checkpoint['last_part']['last_record_ordinal'] + 1
            and groups['messages'] <= projected['dense_message_count']
            and groups['bytes'] <= projected['dense_message_bytes']
            and isinstance(groups['prefix_sha256'], str))
    except (KeyError, TypeError, ValueError):
        return False


def add_group_coverage(previous, current):
    from collections import Counter
    if previous['contract'] != current['contract']:
        raise ValueError('tool coverage contract changed')
    result = dict(current)
    for key in ('events', 'tool_blocks', 'groups', 'string_leaves', 'included_string_leaves'):
        if any(type(value[key]) is not int or value[key] < 0 for value in (previous, current)):
            raise ValueError('invalid tool coverage count')
        result[key] += previous[key]
    dispositions = Counter()
    if any(not isinstance(value['dispositions'], dict) for value in (previous, current)):
        raise ValueError('invalid tool coverage dispositions')
    for value in (previous, current):
        for key, count in value['dispositions'].items():
            if not isinstance(key, str) or type(count) is not int or count < 0:
                raise ValueError('invalid tool coverage disposition')
            dispositions[key] += count
    result['dispositions'] = dict(sorted(dispositions.items()))
    return result
