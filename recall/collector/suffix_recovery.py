"""Explicit recovery of reviewed Claude suffixes; never a permissive scan parser."""
from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from .collector import canonical_json, normalized_timestamp, sanitize


def inspect_suffix(root: Path, spec: dict, *, max_bytes: int = 256 * 1024) -> dict:
    """Read two exact pinned windows without opening or changing the spool."""
    path = Path(spec['path']).resolve()
    if not path.is_relative_to(Path(root).resolve()):
        raise ValueError('recovery_path_outside_root')
    offsets = [spec[key] for key in ('line_start', 'suffix_start', 'line_end', 'next_end')]
    if (any(type(n) is not int for n in offsets) or not 0 <= offsets[0] < offsets[1] < offsets[2] < offsets[3]
            or offsets[3] - offsets[0] > max_bytes):
        raise ValueError('recovery_window_invalid')
    start, suffix_start, end, next_end = offsets
    with path.open('rb') as source:
        source.seek(start)
        raw = source.read(next_end - start)
    line, following = raw[:end-start], raw[end-start:]
    suffix = line[suffix_start-start:]
    for name, value in (('line', line), ('suffix', suffix), ('next', following)):
        if not value.endswith(b'\n') or hashlib.sha256(value).hexdigest() != spec[name+'_sha256']:
            raise ValueError('recovery_source_changed')
    try:
        json.loads(line)
    except (ValueError, UnicodeDecodeError):
        pass
    else:
        raise ValueError('recovery_line_not_malformed')
    content, adjacent = json.loads(suffix), json.loads(following)
    if not isinstance(content, dict) or not isinstance(adjacent, dict):
        raise ValueError('recovery_native_object_required')
    role = content.get('type')
    message = content.get('message')
    if role not in ('user', 'assistant') or not isinstance(message, dict) or message.get('role') != role:
        raise ValueError('recovery_visible_message_required')
    blocks = message.get('content')
    visible = isinstance(blocks, str) and bool(blocks.strip())
    if isinstance(blocks, list):
        visible = any(isinstance(block, dict) and block.get('type') == 'text'
                      and isinstance(block.get('text'), str) and block['text'].strip() for block in blocks)
    if not visible:
        raise ValueError('recovery_visible_message_required')
    for name in ('uuid', 'sessionId'):
        if not isinstance(content.get(name), str):
            raise ValueError('recovery_native_identity_required')
        uuid.UUID(content[name])
    if (not isinstance(content.get('timestamp'), str) or not content['timestamp']
            or adjacent.get('sessionId') != content['sessionId']
            or adjacent.get('parentUuid') != content['uuid']
            or adjacent.get('type') not in ('user', 'assistant', 'system', 'attachment')):
        raise ValueError('recovery_chain_mismatch')
    return dict(path=path, content=content, start=suffix_start, end=end, provenance={
        'original_line_start': start, 'original_line_end': end,
        'original_line_sha256': spec['line_sha256'], 'suffix_start': suffix_start,
        'suffix_sha256': spec['suffix_sha256'], 'following_record_sha256': spec['next_sha256'],
    })


def recover_suffix(collector, spec: dict) -> dict:
    """Enqueue one reviewed suffix through the existing archive and ACK owners.

    This does not scan, flush, reset cursors, remove errors or rewrite source
    files. The caller supplies an explicitly reviewed, hash-pinned manifest.
    """
    if collector.harness != 'claude' or collector.db.in_transaction:
        raise ValueError('recovery_requires_idle_claude_spool')
    verified = inspect_suffix(collector.root, spec)
    path, start, end = verified['path'], verified['start'], verified['end']
    native_id = f'{collector._file_key(path)}-{start:016x}'
    privacy = collector.privacy.apply(verified['content'])
    if privacy.action == 'drop':
        return dict(status='privacy_dropped', native_id=native_id)
    base_hash = hashlib.sha256(canonical_json(sanitize(privacy.value))).hexdigest()

    def existing():
        if not collector.db.execute(
            'SELECT 1 FROM dead_letters WHERE path=? AND byte_offset=? AND error_code=?',
            (str(path), spec['line_start'], 'JSONDecodeError'),
        ).fetchone():
            raise ValueError('recovery_dead_letter_missing')
        row = collector.db.execute(
            'SELECT state,start_offset,end_offset FROM outbox WHERE path=? AND native_id=? ORDER BY id DESC LIMIT 1',
            (str(path), native_id),
        ).fetchone()
        if row is None:
            return None
        generation = collector.db.execute(
            'SELECT base_content_sha256 FROM record_generations WHERE native_id=?', (native_id,),
        ).fetchone()
        if (row['start_offset'] != start or row['end_offset'] != end or generation is None
                or generation['base_content_sha256'] != base_hash or row['state'] not in ('pending', 'acked')):
            raise ValueError('recovery_existing_identity_mismatch')
        return dict(status='already_'+row['state'], native_id=native_id)

    prior = existing()
    if prior is not None:
        return prior
    occurred = normalized_timestamp(verified['content']['timestamp'], path.stat().st_mtime)
    artifact = collector._archive_raw(native_id=native_id, payload=canonical_json(privacy.value), occurred_at=occurred)
    inspect_suffix(collector.root, spec)  # Archive latency must not hide a changed source window.
    with collector.db:
        collector.db.execute('BEGIN IMMEDIATE')
        prior = existing()
        if prior is not None:
            return prior
        content = collector._versioned_record_content(native_id, privacy.value, was_active=False)
        envelope = collector._bounded_record_envelope(path=path, native_id=native_id,
            content=content, occurred_at=occurred, start=start, end=end, artifact_ref=artifact)
        envelope['provenance']['recovered_suffix'] = verified['provenance']
        queued = collector._queue(path, envelope, end)
        collector.db.execute(
            'INSERT INTO active_records(path,native_id,content_sha256,start_offset,end_offset) VALUES (?,?,?,?,?)',
            (str(path), native_id, envelope['content_sha256'], start, end),
        )
    return dict(status='queued' if queued else 'already_pending', native_id=native_id)
