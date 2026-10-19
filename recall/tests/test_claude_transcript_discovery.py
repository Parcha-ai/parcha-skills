"""Native transcript discovery excludes only producer-owned workflow replay journals."""
from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest import mock

import test_collector as fixtures


class ClaudeTranscriptDiscoveryTests(unittest.TestCase):
    setUp = fixtures.CollectorTest.setUp
    tearDown = fixtures.CollectorTest.tearDown
    collector = fixtures.CollectorTest.collector

    def write(self, relative: str, value: str) -> Path:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
        return path

    def test_workflow_journal_exclusion_preserves_transcript_paths(self):
        transcripts = {
            self.write(name, fixtures.claude_line('synthetic native message ' + str(index)))
            for index, name in enumerate((
                'session.jsonl',
                'project/session.jsonl',
                'project/session/subagents/agent-native.jsonl',
                'project/session/subagents/workflows/wf_example/agent-workflow.jsonl',
                'project/session/subagents/agent-parent/subagents/agent-child.jsonl',
                'project/attic/renamed-session.jsonl',
                'custom/deep/moved/transcript.jsonl',
                'project/benchmarks/run/judge_trace.jsonl',
                'project/benchmarks/run/judge_spend.jsonl',
                'custom/workflows/wf_example/journal.jsonl',
                'project/session/subagents/workflows/not-a-run/journal.jsonl',
                'project/session/subagents/workflows/wf_/journal.jsonl',
            ))
        }
        for name in (
            'project/session/subagents/workflows/wf_example/journal.jsonl',
        ):
            self.write(name, json.dumps({'type': 'result', 'result': {'count': 1}}) + '\n')
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(set(collector.discover()), transcripts)
        report = collector.scan()
        collector.flush()
        self.assertEqual(report['records_queued'], len(transcripts))
        self.assertEqual(collector.doctor()['records'], len(transcripts))
        self.assertEqual(sum(len(request['events']) for request in fixtures.AckServer.received), len(transcripts))
        self.assertTrue(all(event['kind'] == 'transcript_record' for request in fixtures.AckServer.received for event in request['events']))

    def test_custom_workflow_root_preserves_agent_and_excludes_journal(self):
        journal = self.write('project/session/subagents/workflows/wf_example/journal.jsonl',
                             json.dumps({'type': 'result', 'result': {'count': 1}}) + '\n')
        agent = self.write('project/session/subagents/workflows/wf_example/agent-native.jsonl',
                           fixtures.claude_line('synthetic agent'))
        self.root = journal.parent
        collector = self.collector()
        self.addCleanup(collector.close)
        self.assertEqual(collector.discover(), [agent])
        self.assertEqual(collector.scan()['records_queued'], 1)
        collector.flush()
        self.assertEqual(collector.doctor()['records'], 1)

    def test_upgrade_preserves_pending_and_acked_journal_history(self):
        journal = self.write('project/session/subagents/workflows/wf_example/journal.jsonl',
                             json.dumps({'type': 'result', 'result': {'count': 1}}) + '\n')
        collector = self.collector()
        self.addCleanup(collector.close)
        # Simulate prior discovery; retain the native scan and outbox operations.
        with mock.patch.object(collector, 'discover', return_value=[journal]):
            self.assertEqual(collector.scan()['records_queued'], 1)
        self.assertEqual(len(collector.pending_envelopes()), 1)
        self.assertEqual(collector.discover(), [])
        with journal.open('a') as handle:
            handle.write(json.dumps({'type': 'result', 'result': {'count': 2}}) + '\n')
        self.assertEqual(collector.scan()['records_queued'], 0)
        collector.flush()
        self.assertEqual(collector.pending_envelopes(), [])
        self.assertEqual(collector.doctor()['records'], 1)
        journal.unlink()
        transcript = self.write('project/session.jsonl', fixtures.claude_line('new transcript'))
        self.assertEqual(collector.discover(), [transcript])
        self.assertEqual(collector.scan()['records_queued'], 1)
        collector.flush()
        self.assertEqual(collector.doctor()['records'], 2)
        events = [event for request in fixtures.AckServer.received for event in request['events']]
        self.assertEqual(len(events), 2)
        self.assertTrue(all(event['kind'] == 'transcript_record' for event in events))


if __name__ == '__main__':
    unittest.main()
