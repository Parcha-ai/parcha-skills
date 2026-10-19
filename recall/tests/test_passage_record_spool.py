"""Native preparation must not retain discarded archive records across passes."""
import contextlib
from datetime import datetime, timezone
import gc
import json
from types import SimpleNamespace
import tracemalloc
import tempfile
import unittest
from unittest.mock import patch

from recall_server import passage_index
from recall_server.logical_evidence import LogicalEvidenceError, LogicalEvidenceRecord
from recall_server.passage_index import CanonicalPassageProjector, PassageCandidate
from recall_server.passage_projection import DEFAULT_PASSAGE_POLICY


class SyntheticArchive:
    """Generate bounded verified-part inputs; never retain the whole source."""
    def __init__(self, count=10000, per_part=100):
        self.count = count
        self.per_part = per_part
        self.calls = []
        self.fail_part = None
        self.parts = tuple(dict(ordinal=i, artifact_id=f'part-{i}', object_key=f'part-{i}',
            content_sha256='a' * 64, size_bytes=1, media_type='application/jsonl', version_id=None,
            first_record_ordinal=i * per_part,
            last_record_ordinal=min(count, (i + 1) * per_part) - 1)
            for i in range((count + per_part - 1) // per_part))

    def read_manifest(self, reference, **kwargs):
        return dict(logical_document_id='ldoc_' + 'a' * 32, revision=2,
                    document_content_sha256='b' * 64, parts=self.parts)

    def read_part(self, reference, **kwargs):
        ordinal = reference['ordinal']
        self.calls.append(ordinal)
        if ordinal == self.fail_part:
            raise LogicalEvidenceError('synthetic_archive_failure')
        return b''.join(LogicalEvidenceRecord(ordinal=i, event_native_id=f'event-{i}',
            event_kind='transcript_record', occurred_at='2026-01-01T00:00:00Z',
            roles=('system',), receipts=(f'recall://claude:synthetic/event-{i}?rev=1#item=0',),
            segment_ordinal=0, segment_count=1,
            text=json.dumps(dict(type='progress', padding=str(i) + 'x' * 4096)))
            .encode(source_id='claude:synthetic')
            for i in range(reference['first_record_ordinal'], reference['last_record_ordinal'] + 1))


def fixture(version='tool_groups_v1', count=10000):
    archive = SyntheticArchive(count)
    connection = SimpleNamespace(execute=lambda *a, **k: SimpleNamespace(fetchall=lambda: []))
    store = SimpleNamespace(connect=lambda: contextlib.nullcontext(connection))
    projector = CanonicalPassageProjector(store, archive, policy=DEFAULT_PASSAGE_POLICY)
    candidate = PassageCandidate('tenant:test', 'claude:synthetic', 'ldoc_' + 'a' * 32,
        2, 1, datetime(2026, 1, 1, tzinfo=timezone.utc), 'b' * 64, {}, archive.parts,
        extraction_version=version)
    state = dict(available=False, projected=dict(requested_extraction_version='legacy',
        applied_extraction_version='legacy')) if version == 'legacy' else None
    return projector, candidate, archive, state


class PassageRecordSpoolTests(unittest.TestCase):
    def test_nonvisible_parent_does_not_retain_raw_record_text(self):
        for version in ('tool_groups_v1', 'legacy'):
            with self.subTest(version=version):
                projector, candidate, archive, state = fixture(version)
                gc.collect()
                tracemalloc.start()
                try:
                    with patch.object(projector, '_append_state', return_value=state):
                        prepared = projector._prepare(candidate, incremental=True)
                    peak = tracemalloc.get_traced_memory()[1]
                finally:
                    tracemalloc.stop()
                print(json.dumps(dict(version=version, records=archive.count,
                    peak_python_bytes=peak, parts_read=len(archive.calls))))
                self.assertEqual(prepared.passages, ())
                self.assertEqual(prepared.dense_message_count, 0)
                self.assertEqual(archive.calls, list(range(len(archive.parts))))
                if version != 'legacy':
                    self.assertEqual(prepared.extraction_coverage['events'], archive.count)
                    self.assertEqual(prepared.extraction_coverage['dispositions'],
                        {'unsupported_provider_event': archive.count})
                # The fixture has over40MiB of discarded text; preparation should
                # retain only current part/record plus migration receipt metadata.
                self.assertLess(peak, 16 * 1024 * 1024)


    def test_replay_file_closes_on_success_archive_failure_and_extraction_interrupt(self):
        real_file = tempfile.TemporaryFile
        for failure in (None, 'archive', 'extraction'):
            with self.subTest(failure=failure):
                projector, candidate, archive, state = fixture(count=201)
                opened = []

                def tracked_file(*args, **kwargs):
                    stream = real_file(*args, **kwargs)
                    opened.append(stream)
                    return stream

                with contextlib.ExitStack() as patches:
                    patches.enter_context(patch.object(projector, '_append_state', return_value=state))
                    patches.enter_context(patch.object(passage_index.tempfile, 'TemporaryFile', side_effect=tracked_file))
                    if failure == 'archive':
                        archive.fail_part = 1
                    elif failure == 'extraction':
                        patches.enter_context(patch.object(passage_index, 'extract_tool_event_groups',
                            side_effect=KeyboardInterrupt('synthetic extraction stop')))
                    if failure:
                        error = LogicalEvidenceError if failure == 'archive' else KeyboardInterrupt
                        with self.assertRaises(error):
                            projector._prepare(candidate, incremental=True)
                    else:
                        self.assertEqual(projector._prepare(candidate, incremental=True).passages, ())
                self.assertEqual(len(opened), 3 if failure == 'archive' else 4)
                self.assertTrue(all(stream.closed for stream in opened))
                expected = [0, 1] if failure == 'archive' else [0, 1, 2]
                self.assertEqual(archive.calls, expected)


if __name__ == '__main__':
    unittest.main()
