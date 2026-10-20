"""Document-enrolled tool-event extraction for the existing passage publisher.

Groups preserve provider-block boundaries and expose exact input/output evidence.
The resolver establishes coordinates, never execution, truth, or human assent.
"""
from dataclasses import dataclass
from collections import Counter
import json
from typing import Iterable

from contracts.source_units import resolve_claude_tool_source_unit, resolve_codex_tool_source_unit
from .logical_evidence import LogicalEvidenceRecord
from .passage_projection import PassageMessage, _logical_record_groups

TOOL_PROJECTION_CONTRACT = "recall.tool-event-projection.v1"


def tool_evidence_context(spans) -> str:
    """Attribution only; source bytes and provider envelope roles stay unchanged."""
    kinds = set()
    for span in spans:
        unit = span.get('source_unit') if isinstance(span, dict) else span.source_unit
        if isinstance(unit, str) and unit.startswith(('claude.tool-input.string.v1#', 'codex.tool-input.string.v1#')):
            kinds.add('tool_input')
        elif isinstance(unit, str) and unit.startswith(('claude.tool-result.string.v1#',
                'codex.tool-result.string.v1#', 'codex.tool-result.block-text.v1#')):
            kinds.add('tool_result')
    if not kinds:
        return ''
    descriptions = []
    if 'tool_input' in kinds:
        descriptions.append('Tool input: requested action or content; does not prove execution, selection, or human approval.')
    if 'tool_result' in kinds:
        descriptions.append('Tool result: reported output, not verified fact or human speech.')
    return ' '.join(descriptions) + ' Open source for context.'


@dataclass(frozen=True)
class ToolEventGroup:
    messages: tuple[PassageMessage, ...]
    full_event_text: str
    enclosing_pointer: str
    evidence_kind: str
    provider_role: str
    tool_call_id: str | None

    @property
    def context_description(self) -> str:
        # Context only: not inserted into source text or its byte coordinates.
        return (f"{self.evidence_kind}; provider envelope role={self.provider_role}. "
                "Literal fields from one provider block; field semantics unknown. "
                "Tool input does not establish execution, selection, or human assent; "
                "tool output records an observation, not verified truth.")


def _strings(value, pointer):
    pending = [(value, pointer)]
    while pending:
        node, path = pending.pop()
        if isinstance(node, str):
            yield path, node
        elif isinstance(node, dict):
            pending.extend((v, path+'/'+k.replace('~','~0').replace('/','~1'))
                           for k,v in reversed(list(node.items())))
        elif isinstance(node, list):
            pending.extend((v,path+'/'+str(i))for i,v in reversed(list(enumerate(node))))


def _unique_pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate_json_key')
        value[key] = item
    return value


def tool_event_groups(
    records: Iterable[LogicalEvidenceRecord], *, first_record_ordinal: int = 0,
) -> tuple[ToolEventGroup, ...]:
    return extract_tool_event_groups(records, first_record_ordinal=first_record_ordinal)[0]


def extract_tool_event_groups(
    records: Iterable[LogicalEvidenceRecord], *, first_record_ordinal: int = 0,
) -> tuple[tuple[ToolEventGroup, ...], dict]:
    """Resolve every declared string within each explicitly supplied provider block.

    No size/word thresholds or tool-name special cases. Empty strings have no span;
    their original event remains intact. Existing passage geometry bounds windows.
    Coverage counts describe this adapter, not complete provider capture. Unsupported
    events and leaves stay in the unchanged archive and are counted as raw-only.
    """
    output = []
    dispositions = Counter()
    events = leaves = blocks = 0
    for records_group in _logical_record_groups(records, first_record_ordinal=first_record_ordinal):
        events += 1
        first = records_group[0]
        raw = ''.join(record.text for record in records_group)
        try:
            event = json.loads(raw, object_pairs_hook=_unique_pairs)
        except (ValueError, RecursionError):
            dispositions['invalid_json'] += 1
            continue
        if not isinstance(event, dict) or not isinstance(event.get('message'), dict):
            dispositions['unsupported_provider_event'] += 1
            continue
        content = event['message'].get('content')
        if not isinstance(content, list) or not first.receipts:
            dispositions['unsupported_event_content_or_receipts'] += 1
            continue
        for index, block in enumerate(content):
            if not isinstance(block, dict):
                continue
            path = f'/message/content/{index}'
            kind = block.get('type')
            if kind == 'tool_use':
                marker, role = 'claude.tool-input.string.v1#', 'assistant'
                candidates = _strings(block.get('input'), path+'/input')
                call_id = block.get('id')
            elif kind == 'tool_result':
                marker, role = 'claude.tool-result.string.v1#', 'user'
                value = block.get('content')
                candidates = ([(path+'/content',value)] if isinstance(value,str) else
                    [(path+f'/content/{j}/text',v['text']) for j,v in enumerate(value)
                     if isinstance(v,dict) and v.get('type')=='text' and isinstance(v.get('text'),str)]
                    if isinstance(value,list) else [])
                call_id = block.get('tool_use_id')
                if not isinstance(value, (str, list)):
                    dispositions['unsupported_result_content'] += 1
                elif isinstance(value, list):
                    dispositions['unsupported_result_content'] += sum(
                        not (isinstance(v, dict) and v.get('type') == 'text'
                             and isinstance(v.get('text'), str)) for v in value)
            else:
                continue
            blocks += 1
            if first.roles != (role,):
                dispositions['provider_role_mismatch'] += 1
                continue
            messages = []
            block_leaves = 0
            origin_kind = 'tool_input' if kind=='tool_use' else 'tool_result'
            origin = dict(evidence_kind=origin_kind, provider_role=role,
                          enclosing_pointer=path, semantic_role='unknown')
            for pointer, candidate in candidates:
                leaves += 1
                block_leaves += 1
                unit = marker+pointer
                try:
                    text, source_origin = resolve_claude_tool_source_unit(raw,unit)
                except (ValueError, RecursionError):
                    dispositions['source_unit_refused'] += 1
                    continue
                assert text == candidate and source_origin == origin_kind
                if not text:
                    dispositions['empty_string'] += 1
                    continue
                if not text.strip():
                    # Existing token geometry has no window for whitespace.
                    # Retain the raw field and account for it explicitly.
                    dispositions['no_token_content'] += 1
                    continue
                message = PassageMessage(record_ordinal=first.ordinal,
                    record_count=len(records_group), occurred_at=first.occurred_at,
                    roles=first.roles, receipts=first.receipts, text=text,
                    actor_links=first.actor_links, source_unit=unit, origin=origin)
                message.validate()
                messages.append(message)
            if messages:
                output.append(ToolEventGroup(tuple(messages),raw,path,origin_kind,role,
                                              call_id if isinstance(call_id,str)else None))
            elif not block_leaves:
                dispositions['no_supported_string_leaves'] += 1
    coverage = dict(contract=TOOL_PROJECTION_CONTRACT, events=events,
                    tool_blocks=blocks, groups=len(output), string_leaves=leaves,
                    included_string_leaves=sum(len(group.messages) for group in output),
                    dispositions=dict(sorted(dispositions.items())))
    return tuple(output), coverage


def extract_codex_tool_event_groups(
    records: Iterable[LogicalEvidenceRecord], *, first_record_ordinal: int = 0,
) -> tuple[tuple[ToolEventGroup, ...], dict]:
    """Publish literal strings from supported Codex provider tool items.

    Each item is one group. JSON-looking arguments remain one literal string;
    the shared source resolver alone authorizes its provider form and coordinates.
    Unsupported items remain raw and receive an explicit coverage disposition.
    """
    output = []
    dispositions = Counter()
    events = blocks = leaves = 0
    for records_group in _logical_record_groups(records, first_record_ordinal=first_record_ordinal):
        events += 1
        first = records_group[0]
        raw = ''.join(record.text for record in records_group)
        try:
            event = json.loads(raw, object_pairs_hook=_unique_pairs)
        except (ValueError, RecursionError):
            dispositions['invalid_json'] += 1
            continue
        payload = event.get('payload') if isinstance(event, dict) else None
        form = payload.get('type') if isinstance(payload, dict) else None
        fields = {
            'function_call': ('arguments', 'input'),
            'custom_tool_call': ('input', 'input'),
            'function_call_output': ('output', 'result'),
            'custom_tool_call_output': ('output', 'result'),
        }
        if not isinstance(form, str) or form not in fields:
            dispositions['unsupported_provider_event'] += 1
            continue
        blocks += 1
        field, kind = fields[form]
        unit = f'codex.tool-{kind}.string.v1#/payload/{field}'
        try:
            text, evidence_kind, provenance = resolve_codex_tool_source_unit(raw, unit)
            resolved = [(unit, text, evidence_kind, provenance)]
        except (ValueError, RecursionError):
            # Keep successful whole-output coordinates unchanged. Mixed custom
            # outputs use the additive contract for each exact text leaf.
            resolved = []
            refused = 0
            if form == 'custom_tool_call_output' and isinstance(payload.get('output'), list):
                for index in range(len(payload['output'])):
                    block_unit = f'codex.tool-result.block-text.v1#/payload/output/{index}/text'
                    try:
                        text, evidence_kind, provenance = resolve_codex_tool_source_unit(raw, block_unit)
                        resolved.append((block_unit, text, evidence_kind, provenance))
                    except (ValueError, RecursionError):
                        refused += 1
            if not resolved:
                dispositions['source_unit_refused'] += 1
                continue
            dispositions['unsupported_output_block'] += refused
        _, _, evidence_kind, provenance = resolved[0]
        role = provenance['role']
        if first.roles not in ((), (role,)) or not first.receipts:
            dispositions['provider_role_or_receipts_mismatch'] += 1
            continue
        messages = []
        for unit, text, evidence_kind, provenance in resolved:
            leaves += 1
            if not text or not text.strip():
                dispositions['empty_string' if not text else 'no_token_content'] += 1
                continue
            origin = dict(evidence_kind=evidence_kind, provider_role=role,
                          enclosing_pointer='/payload', semantic_role='unknown',
                          source_tool=provenance)
            message = PassageMessage(record_ordinal=first.ordinal,
                record_count=len(records_group), occurred_at=first.occurred_at,
                roles=(role,), receipts=first.receipts, text=text,
                actor_links=first.actor_links, source_unit=unit, origin=origin)
            message.validate()
            messages.append(message)
        if messages:
            output.append(ToolEventGroup(tuple(messages), raw, '/payload', evidence_kind,
                                         role, provenance.get('call_id')))
    coverage = dict(contract='recall.codex-tool-event-projection.v1', events=events,
                    tool_blocks=blocks, groups=len(output), string_leaves=leaves,
                    included_string_leaves=sum(len(group.messages) for group in output),
                    dispositions=dict(sorted(dispositions.items())))
    return tuple(output), coverage
