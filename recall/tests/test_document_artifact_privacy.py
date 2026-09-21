import hashlib
import json
from dataclasses import replace
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from connectors.sdk import ConnectorPage, ConnectorRecord, ConnectorRunner
from connectors.slack_source import normalize_slack_message
from connectors.slack_workspace import SlackWorkspaceConnector
from privacy.policy import PrivacyPolicy
from server.recall_server.projectors import validate_envelope
from tests.test_connector_sdk import FakeArchive, FakeBrain, SyntheticConnector

PAYLOAD = b'synthetic attachment 5'
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()


def attachment():
    rail = SimpleNamespace(request=Mock(), download_binary=lambda url: (PAYLOAD, 'text/plain'))
    slack = SlackWorkspaceConnector(rail=rail, source_id='slack:test', workspace_id='T123')
    parent = normalize_slack_message(workspace_id='T123', channel_id='C123', value={
        'ts': '1789981200.000001', 'text': 'synthetic', 'user': 'U123',
    })
    return slack._file_records({'files': [{'id': 'F123', 'name': 'synthetic.txt', 'url_private': 'https://synthetic.invalid/file'}]}, parent)[0]


class DocumentArtifactPrivacyTests(unittest.TestCase):
    def stage(self, mode, record=None):
        record = record or attachment()
        tmp = tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        archive = FakeArchive()
        runner = ConnectorRunner(connector=SyntheticConnector({}), brain=FakeBrain(), spool_path=Path(tmp.name)/'spool.sqlite',
            privacy=PrivacyPolicy(mode=mode), archive=archive, tenant_id='tenant:test', principal_id='owner-test')
        self.addCleanup(runner.close)
        result=runner._stage(ConnectorPage((record,), 'next', False), None)
        rows=runner.db.execute('SELECT envelope_json FROM outbox').fetchall()
        return runner, result, [json.loads(row['envelope_json']) for row in rows], archive

    def test_real_attachment_hash_survives_scrub_and_server_validation(self):
        record=attachment()
        self.assertEqual(record.content['artifact_content_sha256'], DIGEST)
        self.assertNotEqual(PrivacyPolicy(mode='scrub').apply(DIGEST).value,DIGEST)
        _, result, events, archive=self.stage('scrub',record)
        self.assertEqual(result['staged'],1)
        self.assertEqual(events[0]['content']['artifact_content_sha256'],DIGEST)
        self.assertEqual(events[0]['provenance']['artifact_ref']['content_sha256'],DIGEST)
        self.assertEqual(archive.objects[DIGEST],PAYLOAD)
        validate_envelope(events[0])

    def test_digest_alone_does_not_drop_safe_attachment(self):
        _,result,events,_=self.stage('drop')
        self.assertEqual(result['staged'],1)
        self.assertEqual(result['dropped'],0)
        validate_envelope(events[0])

    def test_prose_still_scrubbed_or_dropped(self):
        record=attachment();record=replace(record,content={**record.content,'text':'card 4111111111111111'})
        _,_,events,_=self.stage('scrub',record)
        self.assertNotIn('4111111111111111',events[0]['content']['text'])
        self.assertEqual(events[0]['content']['artifact_content_sha256'],DIGEST)
        _,result,events,_=self.stage('drop',record)
        self.assertEqual(result['dropped'],1);self.assertFalse(events)

    def test_v1_and_digest_in_prose_remain_under_privacy_policy(self):
        typed=attachment()
        legacy=ConnectorRecord(schema_version=1, native_id=typed.native_id,
            native_parent_id=typed.native_parent_id, occurred_at=typed.occurred_at,
            content=typed.content, provenance=typed.provenance,
            archive_payload=typed.archive_payload, archive_media_type=typed.archive_media_type)
        _,_,events,_=self.stage('scrub',legacy)
        self.assertNotEqual(events[0]['content']['artifact_content_sha256'],DIGEST)
        _,_,events,_=self.stage('scrub',replace(typed,content={**typed.content,'text':DIGEST}))
        self.assertEqual(events[0]['content']['artifact_content_sha256'],DIGEST)
        self.assertNotEqual(events[0]['content']['text'],DIGEST)

    def test_unproven_or_mismatched_digest_gets_no_privacy_exemption(self):
        for payload in (None,b'different bytes'):
            with self.subTest(payload=payload):
                record=replace(attachment(),archive_payload=payload, archive_media_type=None if payload is None else "text/plain")
                _,_,events,_=self.stage('scrub',record)
                self.assertNotEqual(events[0]['content']['artifact_content_sha256'],DIGEST)

if __name__=='__main__':unittest.main()
