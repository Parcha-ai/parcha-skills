"""Native conversation metadata is identity evidence, never an access grant."""
from __future__ import annotations

import re
from dataclasses import dataclass

_UUID = re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z', re.I)
_AGENT = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,255}\Z')


def native_uuid(value):
    return value.lower() if isinstance(value, str) and _UUID.fullmatch(value) else None


def conversation_key(value):
    if not isinstance(value, str):
        return None
    harness, separator, native_id = value.partition(':')
    native_id = native_uuid(native_id)
    return f'{harness}:{native_id}' if separator and harness in {'claude', 'codex'} and native_id else None


@dataclass(frozen=True)
class NativeConversation:
    conversation_id: str
    strand_id: str | None

    def provenance(self):
        return {'conversation_id': self.conversation_id, 'strand_id': self.strand_id}


def native_conversation(record, *, harness, segment_id=None):
    """Read explicit native IDs; never infer an import from text or a path."""
    if not isinstance(record, dict):
        return None
    if harness == 'codex':
        payload = record.get('payload')
        if record.get('type') != 'session_meta' or not isinstance(payload, dict):
            return None
        native_id = native_uuid(payload.get('id'))
        if native_id is None or (payload.get('session_id') is not None
                                and native_uuid(payload['session_id']) != native_id):
            return None
        if segment_id is not None:
            segment = native_uuid(segment_id)
            if segment is None:
                return None
            strand = 'segment:' + segment
        else:
            # A paginated header alone may not identify its physical segment.
            # Preserve the conversation but do not claim copied-range identity.
            strand = None if payload.get('history_base') else 'root'
    elif harness == 'claude':
        native_id = native_uuid(record.get('sessionId'))
        if native_id is None:
            return None
        agent = record.get('agentId')
        if agent is not None:
            if not isinstance(agent, str) or not _AGENT.fullmatch(agent):
                return None
            strand = 'agent:' + agent
        elif record.get('isSidechain'):
            strand = None
        else:
            strand = 'root'
    else:
        return None
    return NativeConversation(f'{harness}:{native_id}', strand)


def conversation_from_provenance(value):
    if not isinstance(value, dict):
        return None
    key = conversation_key(value.get('conversation_id'))
    strand = value.get('strand_id')
    if key is None:
        return None
    if strand is not None and strand != 'root':
        if not isinstance(strand, str):
            return None
        kind, separator, identifier = strand.partition(':')
        if not separator or not ((kind == 'segment' and native_uuid(identifier))
                                 or (kind == 'agent' and _AGENT.fullmatch(identifier))):
            return None
    return NativeConversation(key, strand)


class NativeConversationConflict(ValueError):
    """Explicit native header and retained metadata contradict one another."""


def projected_conversation(record, provenance):
    """Use original native metadata or its explicit retained collector identity."""
    if not isinstance(provenance, dict):
        return None
    harness = provenance.get('harness')
    if harness not in {'claude', 'codex'}:
        return None
    original = native_conversation(record, harness=harness)
    retained = conversation_from_provenance(provenance.get('native_conversation'))
    if retained is not None and not retained.conversation_id.startswith(harness + ':'):
        raise NativeConversationConflict('native_conversation_conflict')
    if original is not None and retained is not None:
        if original.conversation_id != retained.conversation_id:
            raise NativeConversationConflict('native_conversation_conflict')
        if original.strand_id is not None and original.strand_id != retained.strand_id:
            raise NativeConversationConflict('native_conversation_conflict')
        return retained
    return original or retained
