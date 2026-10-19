"""Exact pre-streaming native outputs and bounded-consumption API controls."""
from dataclasses import asdict, replace
import hashlib
import json
import unittest
import weakref
import random

from recall_server import passage_projection as projection
from recall_server.actor_attribution import ActorLink
from recall_server.logical_evidence import LogicalEvidenceRecord


ARGUMENTS = dict(tenant_id='tenant:test', source_id='claude:test',
                logical_document_id='ldoc_' + 'a' * 32, revision=3)


def message(index, text, **kwargs):
    return projection.PassageMessage(record_ordinal=index, occurred_at='2026-10-01T00:00:00Z',
        roles=('assistant',), receipts=(f'recall://claude:test/event-{index}?rev=1#item=0',),
        text=text, **kwargs)


def cases():
    small = projection.PassagePolicy(4, 1)
    yield 'ordinary', small, tuple(message(i, 'one two three four five') for i in range(4))
    yield 'unicode-empty-actors', small, (message(0, ''), message(1, 'é🙂\t你好\nword   ',
        actor_links=(ActorLink('actor_' + 'a' * 32, 'author'),), origin={'message_id': 'm'}),
        message(2, '\n '), message(3, 'x' * 160 + '🙂' * 50))
    yield 'complete-final', small, (message(0, 'before report'),
        message(1, 'short final', source_unit=projection.COMPLETE_ASSISTANT_FINAL_UNIT),
        message(2, 'after report words are here'),
        message(3, 'long ' * 12, source_unit=projection.COMPLETE_REPORT_UNIT),
        message(4, 'tail'))
    yield 'empty-final', small, (message(0, 'before'), message(1, '',
        source_unit=projection.COMPLETE_ASSISTANT_FINAL_UNIT), message(2, 'after'))
    yield 'default-final-cutoff', projection.PassagePolicy(2048, 128), (
        message(0, 'word ' * 1100, source_unit=projection.COMPLETE_ASSISTANT_FINAL_UNIT),)
    yield 'reader-spans', projection.PassagePolicy(256, 16), tuple(
        replace(message(0, f'leaf{i}', source_unit=f'claude.tool-result.string.v1#/message/content/0/content/{i}',
                        origin={'message_id': 'tool'}), record_ordinal=0)
        for i in range(140))
    yield 'zero-overlap', projection.PassagePolicy(4, 0), tuple(message(i, str(i)) for i in range(9))
    yield 'maximum-overlap', projection.PassagePolicy(4, 3), tuple(message(i, str(i)) for i in range(9))


def fingerprint(rows):
    return hashlib.sha256(json.dumps([asdict(row) for row in rows], sort_keys=True,
        ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


# Filled once from unchanged a8ffe11d before runtime changes. Covers every field,
# including passage IDs, ordinal, receipts, spans, actors, timestamps and text.
ORACLE = {'ordinary': '5012361ca24cdfe5a4d720969b2fa96411f0da9749fa1139a3f743f1798e2078', 'unicode-empty-actors': '62bd5ecc926947f50e6dff054aa61bdd05c914d6ed1237eec7ddef83e8c9d5ab', 'complete-final': '60f8a7fb626d0ba81bbbe4e12f15c057441e77cc102196c34b0e166b016ecc88', 'empty-final': '1adb625872a6f8ff31a71465e1a66849c747b22128b9f2dd44d2b1a235463b09', 'default-final-cutoff': 'c04220654232ef984347a5cae47d5ed1f9c4db7feaa256eea65a7951561925c2', 'reader-spans': '68f9ba9a8be5fb7f75c0d8bca228020cb4ac9c0fea4a896c347664f1986ade97', 'zero-overlap': '25768b129e7078e815bfb95b7bb7dbf1dd2e4f56787e473fb4de6a1b60e0f431', 'maximum-overlap': '1df3849a5d21dced6d9d87783d2d23c548e255155d92a9692a20c21c3beb557a'}


class PassageStreamingTests(unittest.TestCase):
    def test_prechange_exact_all_field_oracle(self):
        for name, policy, messages in cases():
            with self.subTest(name=name):
                result = projection.build_passages(**ARGUMENTS, messages=messages, policy=policy)
                self.assertEqual(fingerprint(result), ORACLE[name])

    def test_iterator_emits_before_consuming_whole_parent(self):
        consumed = []
        def inputs():
            for index in range(1000):
                consumed.append(index)
                yield message(index, 'one')
        iterator = projection.iter_passages(**ARGUMENTS, messages=inputs(),
            policy=projection.PassagePolicy(4, 1))
        first = next(iterator)
        self.assertEqual(first.text, 'one\none\none\none')
        self.assertLessEqual(len(consumed), 6)
        iterator.close()


def segmented_records():
    text = json.dumps(dict(type='assistant', message=dict(role='assistant', id='same-api',
        stop_reason='end_turn', content=[dict(type='text', text='Final é🙂 report with exact finding.')])) , ensure_ascii=False)
    # Split source text inside a JSON field; native reassembly must preserve it.
    pieces = (text[:31], text[31:85], text[85:])
    first = LogicalEvidenceRecord(ordinal=0, event_native_id='first', event_kind='transcript_record',
        occurred_at='2026-10-01T00:00:00Z', roles=('assistant',),
        receipts=('recall://claude:test/first?rev=1#item=0',), segment_ordinal=0,
        segment_count=3, text=pieces[0])
    result = [first]
    result.extend(replace(first, ordinal=i, segment_ordinal=i, receipts=(), text=piece)
                  for i, piece in enumerate(pieces[1:], 1))
    result.append(replace(first, ordinal=3, event_native_id='second', segment_count=1,
        text=text, receipts=('recall://claude:test/second?rev=1#item=0',)))
    return tuple(result)


def generated_cases():
    rng = random.Random(20261018)
    for case in range(60):
        target = rng.choice((4, 8, 16, 32))
        policy = projection.PassagePolicy(target, rng.randrange(target))
        messages = tuple(message(i, rng.choice(('', '  ', 'é🙂 ', 'a\n', 'word ', 'x' * 130)) * rng.randrange(15),
            source_unit=rng.choice((None, None, projection.COMPLETE_ASSISTANT_FINAL_UNIT)))
            for i in range(rng.randrange(1, 30)))
        yield policy, messages


EXTRA_ORACLE = {'generated': 'b8ff1dd113284ba37f46e3abc0571c207872ab10ad480d3fb0f26a932d85cb93', 'visible': '11517222d5bcbd68e20ba392e1e79d812024d1eb39bf0cef1224a35ae62bfb28', 'segmented_passages': '7cc6f3345f84ad219a95de1c1b77286e1e463b33320041f4f0a5a6b15d67bf59'}


class StreamingControls(unittest.TestCase):
    def test_seeded_geometry_matches_frozen_original_all_fields(self):
        hashes = [fingerprint(tuple(projection.iter_passages(**ARGUMENTS, messages=iter(messages), policy=policy)))
                  for policy, messages in generated_cases()]
        self.assertEqual(hashlib.sha256(json.dumps(hashes).encode()).hexdigest(), EXTRA_ORACLE['generated'])

    def test_segmented_records_and_complete_final_visible_iterator_exact(self):
        records = segmented_records()
        consumed = []
        def source():
            for record in records:
                consumed.append(record.ordinal)
                yield record
        iterator = projection.iter_visible_messages(source())
        first = next(iterator)
        self.assertEqual(consumed, [0, 1, 2])
        self.assertEqual(first.record_count, 3)
        rows = (first, *iterator)
        self.assertEqual(fingerprint(rows), EXTRA_ORACLE['visible'])
        self.assertEqual(projection.visible_messages(records), rows)
        output = tuple(projection.iter_passages(**ARGUMENTS, messages=iter(rows), policy=projection.PassagePolicy(8, 2)))
        self.assertEqual(fingerprint(output), EXTRA_ORACLE['segmented_passages'])
        with self.assertRaises(projection.LogicalEvidenceError):
            tuple(projection.iter_visible_messages(records[:2]))
        with self.assertRaises(projection.LogicalEvidenceError):
            tuple(projection.iter_visible_messages((records[0], replace(records[1], receipts=records[0].receipts), records[2])))

    def test_stream_drops_message_objects_outside_active_window(self):
        refs = []
        def source():
            for i in range(2000):
                item = message(i, 'word')
                refs.append(weakref.ref(item))
                yield item
        count = 0
        for passage in projection.iter_passages(**ARGUMENTS, messages=source(), policy=projection.PassagePolicy(8, 2)):
            self.assertLessEqual(sum(ref() is not None for ref in refs), 10)
            count += 1
        self.assertGreater(count, 300)
        self.assertTrue(all(ref() is None for ref in refs))

    def test_duplicate_tool_leaf_nonadjacent_and_backward_or_invalid_messages_refuse(self):
        tool = message(0, 'a', source_unit='claude.tool-result.string.v1#/message/content/0/a')
        other = replace(tool, source_unit='claude.tool-result.string.v1#/message/content/0/b')
        fixtures = ((tool, other, tool), (message(1, 'a'), message(0, 'b')),
                    (message(0, 'a', record_count=2), message(1, 'b')),
                    (message(0, 'a'), replace(message(1, 'b'), receipts=())),
                    (message(0, 'a'), object()))
        for messages in fixtures:
            with self.subTest(messages=messages):
                with self.assertRaises(ValueError):
                    tuple(projection.iter_passages(**ARGUMENTS, messages=iter(messages), policy=projection.PassagePolicy(4, 1)))
                with self.assertRaises(ValueError):
                    projection.build_passages(**ARGUMENTS, messages=messages, policy=projection.PassagePolicy(4, 1))

    def test_tuple_wrapper_keeps_empty_and_list_refusal(self):
        for messages in ((), [], [message(0, 'a')]):
            with self.assertRaises(ValueError):
                projection.build_passages(**ARGUMENTS, messages=messages, policy=projection.PassagePolicy(4, 1))
        self.assertEqual(tuple(projection.iter_passages(**ARGUMENTS, messages=iter(()), policy=projection.PassagePolicy(4, 1))), ())
