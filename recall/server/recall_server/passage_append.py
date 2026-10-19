"""Versioned append checkpoints for the native passage projector."""
from dataclasses import dataclass, replace
import hashlib
import json
import os

from .passage_projection import COMPLETE_SOURCE_UNITS, build_passages, passage_identity

# Change whenever visible extraction, tokenization, or source-unit grouping changes.
# Version 1 can describe historical visible tool-result concatenations. Rebuild
# it once before resuming with the current normal/typed extraction domains.
CHECKPOINT_VERSION = 2


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
    if not passages or not messages:
        return None
    message_base = previous['message_index'] if previous else 0
    prefix_bytes = previous['prefix_message_bytes'] if previous else 0
    last = passages[-1]
    first = last.spans[0]
    index, byte, ordinal = first.message_index, first.source_byte_start, last.ordinal
    local_index = index - message_base
    if messages[local_index].source_unit in COMPLETE_SOURCE_UNITS:
        byte = 0
        ordinal = next(p.ordinal for p in passages if not isinstance(p, RetainedPassage)
                       and any(s.message_index == index for s in p.spans))
    return dict(version=CHECKPOINT_VERSION, policy_fingerprint=policy.fingerprint,
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
        # Only Claude's historical visible tool-result extraction changed.
        # Unchanged source contracts can keep their exact v1 prefix proof.
        compatible = isinstance(checkpoint, dict) and (
            checkpoint.get('version') == CHECKPOINT_VERSION
            or (checkpoint.get('version') == 1 and not candidate.source_id.startswith('claude:')))
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
