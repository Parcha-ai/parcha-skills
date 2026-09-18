"""Opt-in, private selected-passage capture beside the accuracy probe.

No labels, models, scoring or current-source substitution. A complete capture
means the selected passages only. Every returned candidate keeps its slot.
"""
from __future__ import annotations

import base64
import gzip
import hashlib
import inspect
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable

from .boundary_identity import native_family_id, protected_family_ids
from .private_holdout import _private_path
from .retrieval import EvaluationInputError, receipt_source


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _require(ok: bool, code: str = 'capture_invalid') -> None:
    if not ok:
        raise EvaluationInputError(code)


def _read_page(root, spec):
    """Executed unchanged inside recall_exec; returns only the requested page."""
    import hashlib
    import json

    def require(ok, code):
        if not ok:
            raise ValueError(code)

    def body(record):
        return json.dumps(record['content'], ensure_ascii=False, sort_keys=True, separators=(',', ':')) if 'content' in record else record.get('text', record.get('content_fragment', ''))

    def blocks(value):
        if isinstance(value, str):
            return [value] if value.strip() else []
        if isinstance(value, list):
            return [text for item in value for text in blocks(item)]
        if not isinstance(value, dict):
            return []
        require(value.get('type') not in ('thinking', 'reasoning', 'redacted_thinking') and value.get('channel') != 'analysis', 'nonvisible_content')
        text = value.get('text')
        return [text] if isinstance(text, str) and text.strip() else blocks(value.get('content'))

    def visible(text):
        try:
            value = json.loads(text)
        except ValueError:
            return text
        if isinstance(value, str):
            return value
        require(isinstance(value, dict), 'visible_projection_missing')
        require(value.get('type') not in ('thinking', 'reasoning', 'redacted_thinking') and value.get('channel') != 'analysis', 'nonvisible_content')
        candidates = []
        payload = value.get('payload')
        if isinstance(payload, dict):
            require(payload.get('type') not in ('reasoning', 'agent_reasoning') and payload.get('channel') != 'analysis', 'nonvisible_content')
            candidates.extend([payload.get('message'), payload.get('content')])
        message = value.get('message')
        require(not isinstance(message, dict) or message.get('channel') != 'analysis', 'nonvisible_content')
        candidates.extend([message.get('content') if isinstance(message, dict) else message, value.get('content'), value.get('text')])
        return '\n'.join(dict.fromkeys(text for candidate in candidates for text in blocks(candidate)))

    try:
        raw = (root / 'manifest.json').read_bytes()
        require(hashlib.sha256(raw).hexdigest() == spec['manifest_sha256'], 'manifest_advanced_or_mismatch')
        manifest = json.loads(raw)
        require(manifest['logical_document_id'] == spec['logical_document_id'] and manifest['revision'] == spec['revision'], 'logical_identity_mismatch')
        require(manifest['native_parent_sha256'] == hashlib.sha256(spec['native_parent_id'].encode()).hexdigest(), 'native_parent_mismatch')
        needed = {o for p in spec['passages'] for s in p['spans'] for o in range(s['record_ordinal'], s['record_ordinal'] + s['record_count'])}
        require(0 < len(needed) <= 2000, 'selected_record_bound')
        records = {}
        for part in manifest['parts']:
            if not any(part['first_record_ordinal'] <= o <= part['last_record_ordinal'] for o in needed):
                continue
            raw = (root / ('part-%05d.jsonl' % part['ordinal'])).read_bytes()
            require(hashlib.sha256(raw).hexdigest() == part['content_sha256'], 'part_hash_mismatch')
            for i, line in enumerate(raw.splitlines()):
                ordinal = part['first_record_ordinal'] + i
                if ordinal in needed:
                    record = json.loads(line)
                    require(record['ordinal'] == ordinal and ordinal not in records, 'record_ordinal_mismatch')
                    records[ordinal] = record
        require(set(records) == needed, 'required_record_missing')
        groups, identities = {}, set()
        for passage in spec['passages']:
            for span in passage['spans']:
                ordinal, count = span['record_ordinal'], span['record_count']
                group = [records[o] for o in range(ordinal, ordinal + count)]
                first = group[0]
                require(first['segment_ordinal'] == 0 and first['segment_count'] == count, 'segment_count_mismatch')
                require(first['roles'] and set(first['roles']) <= {'user', 'assistant'}, 'nonvisible_role')
                require(first['receipts'] and all(r.startswith('recall://' + spec['source_id'] + '/') for r in first['receipts']), 'source_receipt_mismatch')
                require(all(r['segment_ordinal'] == i and r['segment_count'] == count and r['event_native_id'] == first['event_native_id'] for i, r in enumerate(group)), 'segment_identity_mismatch')
                text = ''.join(body(r) for r in group)
                groups[ordinal] = (first, text)
                if spec['source_id'].startswith('claude:'):
                    native = json.loads(text)
                    require(isinstance(native, dict) and isinstance(native.get('sessionId'), str), 'native_identity_unavailable')
                    identities.add(('claude-parent', native['sessionId']))
                elif spec['source_id'].startswith('codex:'):
                    identities.add(('codex-native', spec['native_parent_id']))
                else:
                    require(False, 'native_identity_unavailable')
        require(len(identities) == 1, 'native_identity_ambiguous')
        kind, native = next(iter(identities))
        identity = {'kind': kind, 'native_id': native}
        base = {'manifest_sha256': spec['manifest_sha256'], 'revision': spec['revision'], 'family': identity}
        if spec['phase'] == 'metadata':
            return base
        require(identity == spec['expected_family'], 'native_identity_changed')
        fragments, summaries = [], []
        for passage in spec['passages']:
            parts, receipts = [], []
            for si, span in enumerate(passage['spans']):
                first, original = groups[span['record_ordinal']]
                raw = visible(original).encode()
                start, end = span['source_byte_start'], span['source_byte_end']
                require(0 <= start < end <= len(raw), 'source_span_out_of_bounds')
                text = raw[start:end].decode(); parts.append(text); receipts.extend(first['receipts'])
                for offset in range(0, len(text), 900):
                    fragments.append({'passage_id': passage['passage_id'], 'span_index': si,
                        'start': offset, 'end': min(offset + 900, len(text)), 'length': len(text),
                        'text': text[offset:offset + 900], 'event_native_id': first['event_native_id'],
                        'occurred_at': first['occurred_at'], 'ordinal': first['ordinal'], 'receipts': first['receipts']})
            text = '\n'.join(parts)
            for span, part in zip(passage['spans'], parts):
                require(text.encode()[span['passage_byte_start']:span['passage_byte_end']].decode() == part, 'passage_coordinate_mismatch')
            require(set(receipts) == set(passage['receipts']), 'selected_receipt_mismatch')
            summaries.append({'passage_id': passage['passage_id'], 'bytes': len(text.encode()), 'sha256': hashlib.sha256(text.encode()).hexdigest()})
        cursor = spec['cursor']
        require(type(cursor) is int and 0 <= cursor < len(fragments), 'pagination_invalid')
        result = {**base, 'cursor': cursor, 'next_cursor': cursor, 'total_fragments': len(fragments), 'fragments': [], 'summaries': summaries}
        for position, fragment in enumerate(fragments[cursor:], start=cursor + 1):
            result['fragments'].append(fragment)
            result['next_cursor'] = None if position == len(fragments) else position
            if len(_page_stdout(result).encode()) > 11000:
                result['fragments'].pop(); result['next_cursor'] = position - 1; break
        require(bool(result['fragments']), 'single_fragment_exceeds_bound')
        return result
    except (ValueError, KeyError, TypeError, OSError, IndexError, UnicodeError) as error:
        # Do not echo provider text, file contents or JSON decoder details.
        codes = {'manifest_advanced_or_mismatch', 'logical_identity_mismatch', 'native_parent_mismatch', 'part_hash_mismatch', 'nonvisible_role', 'nonvisible_content', 'native_identity_ambiguous', 'native_identity_changed', 'native_identity_unavailable', 'source_span_out_of_bounds', 'passage_coordinate_mismatch', 'selected_receipt_mismatch'}
        return {'error': str(error) if type(error) is ValueError and str(error) in codes else 'source_verification_failed'}


def _page_stdout(page):
    """Recall verifies receipts on top-level canonical evidence JSONL records."""
    import json

    metadata = {k: v for k, v in page.items() if k != 'fragments'}
    records = [*page.get('fragments', []), {**metadata, 'capture_meta': True}]
    return ''.join(json.dumps(row, ensure_ascii=False, separators=(',', ':')) + '\n' for row in records)


class CandidateCapture:
    """One bounded capture run; its manifest is written only by finish()."""

    def __init__(self, output: Path, *, client_factory: Callable, protected_families: set[str], max_calls: int = 250, workers: int = 4):
        self.output = _private_path(output, exists=False)
        _require(not any((p / '.git').exists() for p in self.output.parents), 'capture_output_inside_git')
        _require(type(max_calls) is int and 1 <= max_calls <= 20000 and type(workers) is int and 1 <= workers <= 4, 'capture_budget_invalid')
        self.protected = protected_family_ids({f: ['test'] for f in protected_families})
        self.factory, self.max_calls, self.workers = client_factory, max_calls, workers
        self.calls = 0; self.lock = threading.Lock(); self.cases = []; self.finished = False
        self.output.mkdir(mode=0o700)
        self._write('plan.json', {'schema': 'recall.candidate-capture.v1', 'max_calls': max_calls, 'workers': workers,
            'timeout_seconds': 30, 'retries': 0, 'selected_passages': 2, 'scope': 'supplied_selected_passages',
            'protected_family_ids': sorted(self.protected), 'capture_code_sha256': _sha(Path(__file__).read_bytes())})

    def _write(self, name, value):
        raw = (json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False) + '\n').encode()
        fd = os.open(self.output / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)

    def _call(self, client, spec, prefix):
        encoded = base64.b64encode(gzip.compress(json.dumps(spec, separators=(',', ':')).encode(), mtime=0)).decode()
        program = "python3 - <<'RECALL_CAPTURE'\nfrom pathlib import Path\nimport json, base64, gzip\n" + inspect.getsource(_read_page) + '\n' + inspect.getsource(_page_stdout) + f"\nSPEC = '{encoded}'\nprint(_page_stdout(_read_page(Path('/docs/d1'), json.loads(gzip.decompress(base64.b64decode(SPEC))))), end='')\nRECALL_CAPTURE"
        _require(len(program.encode()) <= 16000, 'capture_program_bound')
        args = {'targets': [{'logical_document_id': spec['logical_document_id'], 'alias': 'd1'}], 'program': program, 'timeout_seconds': 30}
        with self.lock:
            _require(self.calls < self.max_calls, 'capture_call_budget')
            number = self.calls; self.calls += 1
        receipt = {'request': args, 'ok': False, 'error': 'capture_transport_exception', 'phase': spec['phase']}
        try:
            outcome = client.call_tool('recall_exec', args, timeout_seconds=30)
            response = outcome.result or {}
            receipt.update(ok=outcome.ok, error=outcome.error, elapsed_ms=outcome.elapsed_ms,
                response_sha256=_sha(json.dumps(response, sort_keys=True).encode()))
            _require(isinstance(response, dict), 'capture_payload_invalid')
            _require(outcome.ok and response.get('complete') is True and response.get('output_truncated') is False and response.get('exit_code') == 0, 'capture_exec_unavailable')
            records = [json.loads(line) for line in response['stdout'].splitlines()]
            _require(records and all(isinstance(r, dict) for r in records), 'capture_payload_invalid')
            _require(records[-1].get('capture_meta') is True and not any(r.get('capture_meta') for r in records[:-1]), 'capture_payload_invalid')
            payload = {k: v for k, v in records[-1].items() if k != 'capture_meta'}
            if spec['phase'] == 'text' and not payload.get('error'):
                payload['fragments'] = records[:-1]
            else:
                _require(len(records) == 1, 'capture_payload_invalid')
            if payload.get('error'):
                _require(payload['error'] in {'manifest_advanced_or_mismatch', 'logical_identity_mismatch', 'native_parent_mismatch', 'part_hash_mismatch', 'nonvisible_role', 'nonvisible_content', 'native_identity_ambiguous', 'native_identity_changed', 'native_identity_unavailable', 'source_span_out_of_bounds', 'passage_coordinate_mismatch', 'selected_receipt_mismatch', 'source_verification_failed'}, 'capture_payload_invalid')
                receipt['payload'] = payload
                raise EvaluationInputError(payload['error'])
            fields = {'manifest_sha256', 'revision', 'family'}
            if spec['phase'] == 'text':
                fields.update({'cursor', 'next_cursor', 'total_fragments', 'fragments', 'summaries'})
            _require(set(payload) == fields and set(payload['family']) == {'kind', 'native_id'}, 'capture_payload_invalid')
            _require(payload['manifest_sha256'] == spec['manifest_sha256'] and payload['revision'] == spec['revision'], 'capture_response_pin_mismatch')
            family = payload['family']; fid = native_family_id(family['kind'], family['native_id'])
            if spec['phase'] == 'metadata':
                receipt['payload'] = {k: payload[k] for k in ('manifest_sha256', 'revision', 'family')}
            else:
                _require(fid not in self.protected and family == spec['expected_family'], 'capture_family_changed')
                opened = set(response.get('opened_receipts', []))
                _require(all(set(f['receipts']) <= opened for f in payload['fragments']), 'capture_receipt_unopened')
                receipt.update(payload=payload, stdout=response['stdout'], opened_receipts=sorted(opened))
            return payload
        finally:
            self._write(f'{prefix}-call-{number:05}.json', receipt)

    def _recover_metadata(self, client, hit, prefix):
        """Recover exact stored hints; never persist prose before family checks."""
        initial = self._spec(hit, allow_missing=True)
        ids = [p['passage_id'] for p in initial['passages']]
        args = {k: hit[k] for k in ('source_id', 'logical_document_id', 'revision', 'manifest_content_sha256')}
        args['passage_ids'] = ids
        cursor, selection, pi, rows = None, None, 0, []
        current = None
        for _ in range(64):
            request = {**args, **({'cursor': cursor} if cursor else {})}
            with self.lock:
                _require(self.calls < self.max_calls, 'capture_call_budget')
                number = self.calls; self.calls += 1
            receipt = {'request': request, 'phase': 'passage_metadata', 'ok': False, 'error': 'capture_transport_exception'}
            try:
                outcome = client.call_tool('recall_passage_metadata', request, timeout_seconds=30)
                response = outcome.result
                receipt.update(ok=outcome.ok, error=outcome.error, elapsed_ms=outcome.elapsed_ms,
                    response_sha256=_sha(json.dumps(response, sort_keys=True).encode()))
                _require(outcome.ok and isinstance(response, dict), 'capture_metadata_unavailable')
                _require(set(response) == set(args) | {'contract', 'selection_sha256', 'passage', 'next_cursor', 'complete'}, 'capture_metadata_invalid')
                _require(all(response[k] == v for k, v in args.items()) and response['contract'] == 'recall.passage-metadata.v1', 'capture_response_pin_mismatch')
                token = response['selection_sha256']
                _require(isinstance(token, str) and re.fullmatch(r'[0-9a-f]{64}', token) is not None, 'capture_metadata_invalid')
                _require(selection is None or selection == token, 'capture_response_pin_mismatch'); selection = token
                part = response['passage']
                headers = {'passage_id', 'ordinal', 'policy_fingerprint', 'text_sha256', 'metadata_sha256', 'total_spans', 'total_receipts'}
                _require(isinstance(part, dict) and set(part) == headers | {'span_offset', 'receipt_offset', 'spans', 'receipts'}, 'capture_metadata_invalid')
                _require(pi < len(ids) and part['passage_id'] == ids[pi], 'capture_metadata_invalid')
                for key in ('policy_fingerprint', 'text_sha256', 'metadata_sha256'):
                    _require(isinstance(part[key], str) and re.fullmatch(r'[0-9a-f]{64}', part[key]) is not None, 'capture_metadata_invalid')
                for key in ('ordinal', 'span_offset', 'receipt_offset', 'total_spans', 'total_receipts'):
                    _require(type(part[key]) is int and 0 <= part[key] <= 1_000_000, 'capture_metadata_invalid')
                _require(0 < part['total_spans'] <= 128 and 0 < part['total_receipts'] <= 8192, 'capture_metadata_bound')
                _require(isinstance(part['spans'], list) and isinstance(part['receipts'], list) and bool(part['spans'] or part['receipts']), 'capture_metadata_invalid')
                span_fields = {'record_ordinal', 'record_count', 'source_byte_start', 'source_byte_end', 'passage_byte_start', 'passage_byte_end'}
                for span in part['spans']:
                    _require(isinstance(span, dict) and span_fields <= set(span) <= span_fields | {'message_index'} and all(type(v) is int and v >= 0 for v in span.values()), 'capture_metadata_invalid')
                _require(all(isinstance(r, str) and len(r) <= 2048 and receipt_source(r) == args['source_id'] for r in part['receipts']), 'capture_source_mismatch')
                if current is None:
                    current = {k: part[k] for k in headers}; current.update(spans=[], receipts=[])
                _require(all(current[k] == part[k] for k in headers), 'capture_response_pin_mismatch')
                _require(part['span_offset'] == len(current['spans']) and part['receipt_offset'] == len(current['receipts']), 'capture_metadata_pagination_invalid')
                for key in ('spans', 'receipts'):
                    current[key].extend(part[key])
                    _require(len(current[key]) <= part['total_' + key], 'capture_metadata_pagination_invalid')
                done = all(len(current[k]) == part['total_' + k] for k in ('spans', 'receipts'))
                if done:
                    stored = {k: current[k] for k in ('passage_id', 'ordinal', 'policy_fingerprint', 'text_sha256', 'spans', 'receipts')}
                    _require(_sha(json.dumps(stored, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()) == part['metadata_sha256'], 'capture_metadata_hash_mismatch')
                    rows.append(stored); pi += 1; current = None
                expected = None if pi == len(ids) else f'{selection}:{pi}:{len(current["spans"]) if current else 0}:{len(current["receipts"]) if current else 0}'
                _require(response['next_cursor'] == expected and type(response['complete']) is bool and response['complete'] == (expected is None), 'capture_metadata_pagination_invalid')
                receipt['payload'] = response
                cursor = expected
                if cursor is None:
                    break
            finally:
                self._write(f'{prefix}-call-{number:05}.json', receipt)
        else:
            raise EvaluationInputError('capture_metadata_pagination_bound')
        recovered = []
        for original, stored in zip(hit['matching_ranges'][:2], rows, strict=True):
            old = original.get('receipts') or []
            _require(stored['receipts'][:len(old)] == old, 'capture_metadata_receipt_mismatch')
            if original.get('spans'):
                _require(original['spans'] == stored['spans'], 'capture_metadata_span_mismatch')
            recovered.append({**original, **stored, 'spans_omitted': False, 'receipts_truncated': 0})
        return {**hit, 'matching_ranges': recovered}

    @staticmethod
    def _spec(hit, *, allow_missing=False):
        keys = ('source_id', 'logical_document_id', 'native_parent_id', 'revision')
        spec = {k: hit[k] for k in keys}
        _require(isinstance(spec['source_id'], str) and spec['source_id'].startswith(('claude:', 'codex:')))
        _require(re.fullmatch(r'ldoc_[0-9a-f]{30,32}', spec['logical_document_id']) is not None)
        _require(re.fullmatch(r'(?:claude|codex)-session-[0-9a-f]{24}', spec['native_parent_id']) is not None)
        _require(type(spec['revision']) is int and spec['revision'] >= 1)
        _require(all(isinstance(hit.get(k), str) and hit[k] for k in ('first_occurred_at', 'last_occurred_at')), 'capture_missing_metadata')
        spec['manifest_sha256'] = hit['manifest_content_sha256']
        _require(re.fullmatch(r'[0-9a-f]{64}', spec['manifest_sha256']) is not None)
        ranges = hit['matching_ranges'][:2]; _require(0 < len(ranges) <= 2, 'capture_missing_metadata')
        spec['passages'] = []
        for passage in ranges:
            _require(isinstance(passage.get('text'), str) and bool(passage['text']), 'capture_missing_metadata')
            _require(isinstance(passage.get('passage_id'), str) and re.fullmatch(r'psg_[0-9a-f]{30,32}', passage['passage_id']) is not None)
            available = passage.get('spans') and passage.get('receipts') and not passage.get('spans_omitted') and not passage.get('receipts_truncated')
            if not available and allow_missing:
                spec['passages'].append({'passage_id': passage['passage_id']})
                continue
            _require(available, 'capture_missing_metadata')
            _require(all(receipt_source(r) == spec['source_id'] for r in passage['receipts']), 'capture_source_mismatch')
            fields = ('record_ordinal', 'record_count', 'source_byte_start', 'source_byte_end', 'passage_byte_start', 'passage_byte_end')
            spans = [{k: s[k] for k in fields} for s in passage['spans']]
            _require(len(spans) <= 128 and all(type(v) is int and v >= 0 for s in spans for v in s.values()))
            _require(all(0 < s['record_count'] <= 2000 and s['source_byte_start'] < s['source_byte_end'] and s['passage_byte_start'] < s['passage_byte_end'] for s in spans))
            _require(isinstance(passage['passage_id'], str) and re.fullmatch(r'psg_[0-9a-f]{30,32}', passage['passage_id']) is not None)
            spec['passages'].append({'passage_id': passage['passage_id'], 'spans': spans, 'receipts': passage['receipts']})
        _require(len({p['passage_id'] for p in spec['passages']}) == len(spec['passages']))
        return spec

    def _candidate(self, hit, index, case_index):
        prefix = f'case-{case_index:03}-candidate-{index:02}'
        row = {'candidate_index': index, **{k: hit.get(k) for k in ('source_id', 'logical_document_id', 'revision', 'native_parent_id', 'first_occurred_at', 'last_occurred_at')},
            'complete_selected_passages': False, 'complete_document': False, 'capture_status': 'unavailable', 'family_ids': [], 'error': None}
        try:
            initial = self._spec(hit, allow_missing=True)
            client = self.factory()
            recovered = self._recover_metadata(client, hit, prefix) if any('spans' not in p for p in initial['passages']) else hit
            spec = self._spec(recovered); row['manifest_sha256'] = spec['manifest_sha256']
            metadata = self._call(client, {**spec, 'phase': 'metadata'}, prefix)
            fid = native_family_id(**metadata['family']); row['family_ids'] = [fid]
            if fid in self.protected:
                row.update(capture_status='withheld_protected', error='protected_family')
                self._write(prefix + '.json', row)
                return row
            cursor, fragments, summaries = 0, [], None
            for _ in range(64):
                payload = self._call(client, {**spec, 'phase': 'text', 'expected_family': metadata['family'], 'cursor': cursor}, prefix)
                _require(payload['cursor'] == cursor and payload['fragments'], 'capture_pagination_invalid')
                if summaries is not None:
                    _require(summaries == payload['summaries'], 'capture_summary_changed')
                summaries = payload['summaries']; fragments.extend(payload['fragments'])
                next_cursor = payload['next_cursor']
                _require(cursor + len(payload['fragments']) <= payload['total_fragments'], 'capture_pagination_invalid')
                if next_cursor is None:
                    _require(len(fragments) == payload['total_fragments'], 'capture_pagination_invalid'); break
                _require(type(next_cursor) is int and next_cursor == cursor + len(payload['fragments']), 'capture_pagination_invalid')
                cursor = next_cursor
            else:
                raise EvaluationInputError('capture_pagination_bound')
            text, evidence = self._assemble(spec, fragments, summaries)
            for original, recovered_range, verified in zip(hit['matching_ranges'][:2], recovered['matching_ranges'][:2], evidence):
                selected = text[verified['combined_char_start']:verified['combined_char_end']]
                _require(selected.startswith(original['text']), 'capture_search_prefix_mismatch')
                if 'text_sha256' in recovered_range:
                    _require(verified['sha256'] == recovered_range['text_sha256'], 'capture_passage_hash_mismatch')
            row.update(capture_status='complete', complete_selected_passages=True, text=text,
                text_sha256=_sha(text.encode()), text_bytes=len(text.encode()), source_evidence=evidence,
                selected_passage_ids=[p['passage_id'] for p in spec['passages']])
        except Exception as error:  # One malformed/failed capture never removes other slots.
            row['error'] = str(error) if type(error) is EvaluationInputError and re.fullmatch(r'[a-z_]+', str(error)) else 'capture_verification_failed'
        self._write(prefix + '.json', row)
        return row

    @staticmethod
    def _assemble(spec, fragments, summaries):
        texts, evidence = [], []
        _require(len(summaries) == len(spec['passages']), 'capture_summary_invalid')
        for passage, summary in zip(spec['passages'], summaries):
            parts, spans = [], []
            for si, span in enumerate(passage['spans']):
                chunks = [f for f in fragments if f['passage_id'] == passage['passage_id'] and f['span_index'] == si]
                _require(bool(chunks), 'capture_fragment_missing')
                cursor = 0
                for chunk in chunks:
                    _require(chunk['start'] == cursor and chunk['end'] == cursor + len(chunk['text']) and chunk['length'] == chunks[0]['length'], 'capture_fragment_gap')
                    _require(type(chunk['ordinal']) is int and chunk['ordinal'] == span['record_ordinal'], 'capture_fragment_ordinal')
                    _require(all(chunk[k] == chunks[0][k] for k in ('event_native_id', 'occurred_at', 'receipts')), 'capture_fragment_identity')
                    cursor = chunk['end']
                _require(cursor == chunks[0]['length'], 'capture_fragment_incomplete')
                parts.append(''.join(c['text'] for c in chunks))
                spans.append({**span, **{k: chunks[0][k] for k in ('event_native_id', 'occurred_at', 'receipts')}})
            text = '\n'.join(parts)
            _require(summary == {'passage_id': passage['passage_id'], 'bytes': len(text.encode()), 'sha256': _sha(text.encode())}, 'capture_passage_hash_mismatch')
            _require(set(passage['receipts']) == {r for span in spans for r in span['receipts']}, 'capture_passage_receipt_mismatch')
            char_start = sum(len(t) + 1 for t in texts); byte_start = sum(len(t.encode()) + 1 for t in texts)
            for span, part in zip(spans, parts):
                _require(text.encode()[span['passage_byte_start']:span['passage_byte_end']].decode() == part, 'capture_span_mismatch')
                span.update(combined_char_start=char_start + len(text.encode()[:span['passage_byte_start']].decode()), combined_char_end=char_start + len(text.encode()[:span['passage_byte_end']].decode()), receipt_granularity='Containing source-event receipts; not per-chunk byte ownership.')
            evidence.append({**summary, 'receipts': passage['receipts'], 'spans': spans, 'combined_char_start': char_start, 'combined_char_end': char_start + len(text), 'combined_byte_start': byte_start, 'combined_byte_end': byte_start + len(text.encode())})
            texts.append(text)
        _require(len(fragments) == sum(len([f for f in fragments if f['passage_id'] == p['passage_id'] and f['span_index'] in range(len(p['spans']))]) for p in spec['passages']), 'capture_unexpected_fragment')
        return '\n'.join(texts), evidence

    def capture_case(self, case_id, query, search_result, *, search_error=None):
        _require(not self.finished and isinstance(case_id, str) and case_id and isinstance(query, str) and query, 'capture_case_invalid')
        _require(case_id not in {c['case_id'] for c in self.cases}, 'capture_case_duplicate')
        hits = search_result.get('results', []) if isinstance(search_result, dict) else []
        _require(isinstance(hits, list) and len(hits) <= 50 and all(isinstance(h, dict) for h in hits), 'capture_search_invalid')
        index = len(self.cases)
        # Search text may include protected prose; pin raw bytes but persist metadata only.
        slots = []
        for h in hits:
            meta = {k: h.get(k) for k in ('source_id', 'logical_document_id', 'revision', 'native_parent_id', 'manifest_content_sha256', 'first_occurred_at', 'last_occurred_at')}
            ranges = h.get('matching_ranges', [])
            meta['selected_passages'] = [
                {**{k: p.get(k) for k in ('passage_id', 'spans', 'receipts', 'passage_window', 'receipts_truncated', 'spans_omitted')},
                 'search_prefix_sha256': _sha(p['text'].encode()) if isinstance(p.get('text'), str) else None}
                for p in ranges[:2] if isinstance(p, dict)
            ] if isinstance(ranges, list) else []
            slots.append(meta)
        self._write(f'case-{index:03}-search.json', {'case_id': case_id, 'query': query, 'search_sha256': _sha(json.dumps(search_result, sort_keys=True).encode()), 'slots': slots})
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            candidates = list(pool.map(lambda pair: self._candidate(pair[1], pair[0], index), enumerate(hits)))
        case = {'case_id': case_id, 'query': query, 'scope': 'supplied_selected_passages', 'search_status': 'unavailable' if search_error or search_result is None else 'ok', 'candidates': candidates,
            'complete_candidate_pool': bool(candidates) and all(c['complete_selected_passages'] for c in candidates)}
        self._write(f'case-{index:03}.json', case); self.cases.append(case)
        return case

    def finish(self):
        _require(not self.finished, 'capture_already_finished')
        candidates = [r for c in self.cases for r in c['candidates']]
        summary = {'cases': len(self.cases), 'candidate_slots': len(candidates), 'complete_candidates': sum(c['complete_selected_passages'] for c in candidates), 'protected_candidates': sum(c['capture_status'] == 'withheld_protected' for c in candidates), 'unavailable_candidates': sum(c['capture_status'] == 'unavailable' for c in candidates), 'source_calls': self.calls, 'model_calls': 0, 'scope': 'supplied_selected_passages'}
        self._write('summary.json', summary)
        self._write('manifest.json', {'schema': 'recall.candidate-capture.v1', 'summary': summary, 'files': [{'path': p.name, 'sha256': _sha(p.read_bytes())} for p in sorted(self.output.iterdir()) if p.is_file()]})
        self.finished = True
        return summary
