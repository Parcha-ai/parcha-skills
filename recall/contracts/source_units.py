"""Resolve versioned tool text inside an already authorized source record."""


def codex_tool_source_unit_role(source_unit):
    """Declared provider role for a supported discriminator, not source proof."""
    if not isinstance(source_unit, str):
        return None
    return {
        'codex.tool-input.string.v1#/payload/arguments': 'assistant',
        'codex.tool-input.string.v1#/payload/input': 'assistant',
        'codex.tool-result.string.v1#/payload/output': 'tool',
    }.get(source_unit)


def resolve_claude_tool_source_unit(record_text, source_unit):
    """Return (exact decoded string, input/result origin) for one typed pointer.

    This pure function is embedded unchanged in the existing recall_exec reader.
    It does not authorize sources, issue receipts, or enumerate unrelated leaves.
    Claude input is arbitrary JSON. Claude result content is a literal string
    or an array of provider blocks whose declared text fields can be addressed.
    JSON-looking strings are never parsed a second time.
    api-request-blob is a replay wrapper carrying the inner provider message;
    it does not establish a new utterance or tool execution.
    """
    import json

    def refuse():
        raise ValueError('tool_source_unit_invalid')

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                refuse()
            result[key] = value
        return result

    def index(part):
        if not part.isascii() or not part.isdigit():
            refuse()
        number = int(part)
        if str(number) != part:
            refuse()
        return number

    try:
        if not isinstance(record_text, str) or not isinstance(source_unit, str):
            refuse()
        marker, pointer = source_unit.split('#', 1)
        kinds = {'claude.tool-input.string.v1': 'input', 'claude.tool-result.string.v1': 'result'}
        kind = kinds[marker]
        if not pointer.startswith('/'):
            refuse()
        parts = []
        for part in pointer[1:].split('/'):
            decoded = part.replace('~1', '/').replace('~0', '~')
            if decoded.replace('~', '~0').replace('/', '~1') != part:
                refuse()
            parts.append(decoded)
        field = 'input' if kind == 'input' else 'content'
        if len(parts) < 4 or parts[:2] != ['message', 'content'] or parts[3] != field:
            refuse()
        value = json.loads(record_text, object_pairs_hook=unique, parse_constant=lambda _: refuse())
        message = value['message']
        role = 'assistant' if kind == 'input' else 'user'
        if (value.get('type') not in (role, 'api-request-blob') or message.get('role') != role
                or value.get('channel') == 'analysis' or message.get('channel') == 'analysis'
                or not isinstance(message.get('content'), list)):
            refuse()
        block = message['content'][index(parts[2])]
        if block.get('type') != ('tool_use' if kind == 'input' else 'tool_result'):
            refuse()
        leaf = block[field]
        tail = parts[4:]
        if kind == 'result':
            if isinstance(leaf, str):
                if tail:
                    refuse()
            else:
                if not isinstance(leaf, list) or len(tail) != 2 or tail[1] != 'text':
                    refuse()
                text_block = leaf[index(tail[0])]
                if text_block.get('type') != 'text' or text_block.get('channel') == 'analysis':
                    refuse()
                leaf = text_block['text']
        else:
            for part in tail:
                leaf = leaf[index(part)] if isinstance(leaf, list) else leaf[part] if isinstance(leaf, dict) else None
        if not isinstance(leaf, str):
            refuse()
        leaf.encode('utf-8')
        return leaf, 'tool_' + kind
    except (ValueError, KeyError, IndexError, TypeError, AttributeError, RecursionError, UnicodeError):
        raise ValueError('tool_source_unit_invalid') from None


def resolve_codex_tool_source_unit(record_text, source_unit):
    """Resolve one literal provider tool string, with its structural provenance.

    Input is an authored request; output is reported tool output. Neither the
    item type nor optional call identity establishes execution or human assent.
    JSON-looking strings remain literal, with decoded UTF-8 byte coordinates.
    A custom output's all-input_text block list resolves to its ordered strings
    joined by newlines, including empty blocks. Coordinates refer to that
    normalized string. Mixed or unknown blocks refuse the complete output.
    """
    import json

    def refuse():
        raise ValueError('tool_source_unit_invalid')

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                refuse()
            result[key] = value
        return result

    try:
        if not isinstance(record_text, str) or not isinstance(source_unit, str):
            refuse()
        value = json.loads(record_text, object_pairs_hook=unique, parse_constant=lambda _: refuse())
        if value.get('type') != 'response_item' or value.get('channel') == 'analysis':
            refuse()
        payload = value['payload']
        field, kind, role = {
            'function_call': ('arguments', 'input', 'assistant'),
            'custom_tool_call': ('input', 'input', 'assistant'),
            'function_call_output': ('output', 'result', 'tool'),
            'custom_tool_call_output': ('output', 'result', 'tool'),
        }[payload['type']]
        if (source_unit != 'codex.tool-' + kind + '.string.v1#/payload/' + field
                or payload.get('channel') == 'analysis'
                or any('role' in item and item['role'] != role for item in (value, payload))):
            refuse()
        text = payload[field]
        if payload['type'] == 'custom_tool_call_output' and isinstance(text, list):
            if not text or any(
                not isinstance(block, dict) or set(block) != {'type', 'text'}
                or block['type'] != 'input_text' or not isinstance(block['text'], str)
                for block in text
            ):
                refuse()
            text = '\n'.join(block['text'] for block in text)
        if not isinstance(text, str):
            refuse()
        text.encode('utf-8')
        provenance = {'type': payload['type'], 'role': role}
        for key in ('call_id', 'name'):
            if key in payload:
                if not isinstance(payload[key], str):
                    refuse()
                provenance[key] = payload[key]
        return text, 'tool_' + kind, provenance
    except (ValueError, KeyError, IndexError, TypeError, AttributeError, RecursionError, UnicodeError):
        raise ValueError('tool_source_unit_invalid') from None
