import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from connectors.sdk import ConnectorPage, ConnectorRunner, ConnectorRunError
from connectors.slack_source import normalize_slack_message, normalize_slack_user
from privacy.policy import PrivacyPolicy
from server.recall_server import managed_worker, projectors
from server.recall_server.canonical import CanonicalPlane
from tests.test_connector_sdk import SyntheticConnector, FakeArchive


def message_record():
    return normalize_slack_message(workspace_id="T123", channel_id="C123", value={
        "ts": "1789981200.000001", "user": "U123", "text": "synthetic",
    })


def envelope(record=None):
    record = record or message_record()
    runner = object.__new__(ConnectorRunner)
    runner.connector_id = "slack.messages"
    runner.source_id = "slack:test"
    runner.principal_id = "owner-test"
    return runner._event(record, record.content, record.provenance)


class EnvelopeValidationDiagnosticTests(unittest.TestCase):
    def refusal(self, value, message, rule, field="unrecognized", kind="unrecognized"):
        with self.assertRaises(ValueError) as caught:
            projectors.validate_envelope(value)
        error = caught.exception
        self.assertEqual(str(error), message)
        self.assertIsInstance(error, projectors.StructuredEnvelopeValidationError)
        self.assertEqual((error.validation_rule, error.validation_field, error.validation_kind), (rule, field, kind))
        return error

    def test_envelope_messages_order_and_closed_rules(self):
        base = envelope()
        cases = [
            (None, "envelope must be an object", "envelope_object", "unrecognized"),
            ({**base, "source_profile": {}}, "source profile is host-controlled", "host_controlled", "unrecognized"),
            ({k: v for k, v in base.items() if k != "native_id"}, "missing fields: native_id", "required_missing", "native_id"),
            ({**base, "secret-unknown-field": 1}, "unknown envelope fields", "unknown_fields", "unrecognized"),
            ({**base, "schema_version": True}, "unsupported schema_version", "schema_version", "schema_version"),
            ({**base, "source_id": "?"}, "invalid source_id", "identifier", "source_id"),
            ({**base, "native_id": "?"}, "invalid native_id", "identifier", "native_id"),
            ({**base, "occurred_at": "no-time"}, "invalid occurred_at", "timestamp", "occurred_at"),
            ({**base, "principal_id": "x" * 161}, "invalid principal_id", "identifier", "principal_id"),
            ({**base, "visibility": "secret"}, "unsupported visibility", "visibility", "visibility"),
            ({**base, "content_type": "secret"}, "unsupported content_type", "content_type", "content_type"),
            ({**base, "provenance": []}, "provenance must be an object", "provenance_object", "provenance"),
            ({**base, "content_sha256": "x"}, "invalid content_sha256", "digest_shape", "content_sha256"),
            ({**base, "content_sha256": "0" * 64}, "content_sha256 mismatch", "digest_mismatch", "content_sha256"),
        ]
        for value, message, rule, field in cases:
            with self.subTest(rule=rule, field=field):self.refusal(value, message, rule, field)

    def test_other_refusals_preserve_literal_messages_and_invariants(self):
        base = envelope()
        cases = [
            ({**base, "native_parent_id": "?"}, "invalid native_parent_id", "identifier", "native_parent_id"),
            ({**base, "kind": "?"}, "invalid kind", "kind", "kind"),
            ({**base, "observed_at": "2026-09-21T00:00:00"}, "invalid observed_at", "timestamp", "observed_at"),
            ({**base, "occurred_at": 7}, "invalid occurred_at", "timestamp", "occurred_at"),
            ({**base, "observed_at": "x" * 65}, "invalid observed_at", "timestamp", "observed_at"),
            ({**base, "provenance": {"connector_schema_version": True}}, "unsupported connector schema_version", "connector_schema_version", "provenance"),
            ({**base, "kind": "other_kind"}, "invalid typed connector record", "typed_kind", "kind"),
            ({**base, "content": None}, "invalid typed connector record", "typed_object", "content"),
            ({**base, "content": {"kind": "secret-kind"}}, "invalid typed connector record", "typed_kind", "kind"),
            ({**base, "content": {"number": float("nan")}, "provenance": {}}, "content and provenance must be finite JSON values", "finite_json", "unrecognized"),
            ({**base, "provenance": {"bad": object()}}, "content and provenance must be finite JSON values", "finite_json", "unrecognized"),
        ]
        tombstone = {**base, "kind": "tombstone", "content": {"target_native_id": "wrong"}}
        tombstone['content_sha256'] = hashlib.sha256(projectors.canonical_json(tombstone['content'])).hexdigest()
        cases.append((tombstone, "tombstone target must match native_id", "tombstone_target", "content"))
        for value, message, rule, field in cases:
            with self.subTest(rule=rule, field=field):self.refusal(value, message, rule, field)

    def test_refusal_precedence_and_non_value_error_behavior_are_unchanged(self):
        value = envelope();value.pop('schema_version');value['secret-field'] = 1
        self.refusal(value, 'missing fields: schema_version', 'required_missing', 'schema_version')
        value['source_profile'] = {}
        self.refusal(value, 'source profile is host-controlled', 'host_controlled')
        value = envelope();value['content'].pop('content_fidelity');value['content']['secret-field'] = 1
        self.refusal(value, 'invalid typed connector record', 'typed_required_missing', 'content_fidelity', 'communication_message.v1')
        # Existing validators use hash lookup/membership: keep their TypeError
        # behavior on unhashable values; this diagnostic does not normalize input.
        for field, replacement in (('visibility', []), ('content', {'kind': []})):
            value = envelope();value[field] = replacement
            with self.assertRaises(TypeError):projectors.validate_envelope(value)

    def test_typed_missing_unknown_invalid_and_fidelity_are_distinct(self):
        for mutation, rule, field in (
            (lambda c: c.pop("content_fidelity"), "typed_required_missing", "content_fidelity"),
            (lambda c: c.update({"secret-unknown-field": "private"}), "typed_unknown_fields", "unrecognized"),
            (lambda c: c.update(text=[]), "typed_invalid_known_field", "text"),
            (lambda c: c.update(content_omissions=["attachment_bytes"]), "typed_fidelity", "content_fidelity"),
        ):
            value = envelope();mutation(value["content"])
            error = self.refusal(value, "invalid typed connector record", rule, field, "communication_message.v1")
            self.assertNotIn("secret-unknown-field", repr(error.__dict__))

    def test_valid_slack_pipeline_ten_cases_remain_accepted(self):
        records = [message_record(), normalize_slack_message(
            workspace_id="T123", channel_id="C123", value={"ts": "1789981200.000002", "text": "Unicode café secret=synthetic-private-value", "files": [{"id": "F123", "name": "synthetic.txt"}]},
        ), normalize_slack_message(workspace_id="T123", channel_id="C123", value={"subtype": "message_deleted", "deleted_ts": "1789981200.000001"})]
        records.extend(normalize_slack_user(workspace_id="T123", value={"id": "U123", "profile": {"email": "synthetic@example.invalid", "real_name": "Synthetic"}}))
        self.assertEqual(len(records), 5)
        for record in records:
            for mode in ("off", "scrub"):
                decision = PrivacyPolicy(mode=mode).apply({"content": record.content, "provenance": record.provenance})
                value = envelope(record)
                if not record.deleted:
                    runner=object.__new__(ConnectorRunner);runner.connector_id="slack.messages";runner.source_id="slack:test";runner.principal_id="owner-test"
                    value=runner._event(record,decision.value['content'],decision.value['provenance'])
                value=json.loads(json.dumps(value))
                self.assertIs(projectors.validate_envelope(value), value)

    def test_malformed_pending_page_retries_without_pull_sql_or_cursor_advance(self):
        with tempfile.TemporaryDirectory() as directory:
            record=message_record();connector=SyntheticConnector({None:ConnectorPage(records=(record,),next_cursor="next",has_more=False)})
            archive=FakeArchive();store=Mock();store.connect.side_effect=AssertionError("no SQL")
            writer=managed_worker._DirectCanonicalWriter(CanonicalPlane(store,archive),tenant_id="tenant-test",principal_id="owner-test")
            runner=ConnectorRunner(connector=connector,brain=writer,spool_path=Path(directory)/"spool.db",privacy=PrivacyPolicy(mode="off"),archive=archive,tenant_id="tenant-test",principal_id="owner-test")
            try:
                runner._stage(connector.pages[None],None)
                value=json.loads(runner.db.execute("SELECT envelope_json FROM outbox").fetchone()[0])
                value['content'].pop('content_fidelity');value['content_sha256']=hashlib.sha256(projectors.canonical_json(value['content'])).hexdigest()
                runner.db.execute("UPDATE outbox SET envelope_json=?",(json.dumps(value),));runner.db.commit()
                before=runner.db.execute("SELECT envelope_json FROM outbox").fetchone()[0]
                for _ in range(2):
                    with self.assertLogs(managed_worker.LOG,level="ERROR") as logs,self.assertRaisesRegex(ConnectorRunError,'brain_unavailable'):runner.run_once()
                    self.assertTrue(any('rule=typed_required_missing kind=communication_message.v1 field=content_fidelity' in record.getMessage() for record in logs.records))
                    self.assertLessEqual(len(logs.records),3)
                self.assertEqual(runner.db.execute("SELECT envelope_json FROM outbox").fetchone()[0],before)
                self.assertFalse(runner.doctor()['checkpointed']);self.assertEqual(runner.doctor()['pending'],1)
                self.assertEqual(connector.pulls,[]);store.connect.assert_not_called()
            finally:runner.close()

    def test_typed_field_must_belong_to_the_reported_record_kind(self):
        error = projectors.StructuredEnvelopeValidationError(
            "invalid typed connector record", rule="typed_invalid_known_field",
            kind="communication_message.v1", field="identifier_type",
        )
        self.assertEqual(error.validation_field, "unrecognized")
        # Revalidate at the log boundary even if a producer mutates an attribute.
        error.validation_field = "identifier_type"
        plane = Mock();plane.ingest_batch.side_effect = error
        writer = managed_worker._DirectCanonicalWriter(plane, tenant_id="private", principal_id="secret")
        with self.assertLogs(managed_worker.LOG, level="ERROR") as logs, self.assertRaises(ValueError):writer.ingest([])
        self.assertIn("rule=typed_invalid_known_field kind=communication_message.v1 field=unrecognized", logs.records[0].getMessage())

    def test_hostile_validation_attributes_are_never_logged(self):
        error=self.refusal(None,"envelope must be an object","envelope_object")
        error.validation_rule='secret-rule';error.validation_kind={'private':1};error.validation_field=['secret']
        plane=Mock();plane.ingest_batch.side_effect=error
        writer=managed_worker._DirectCanonicalWriter(plane,tenant_id='private',principal_id='secret')
        with self.assertLogs(managed_worker.LOG,level='ERROR') as logs,self.assertRaises(ValueError) as caught:writer.ingest([{'secret':'private'}])
        self.assertIs(caught.exception,error)
        self.assertIn('rule=unrecognized kind=unrecognized field=unrecognized',logs.records[0].getMessage())
        for record in logs.records:
            self.assertNotIn('secret',repr(record.__dict__));self.assertNotIn('private',repr(record.__dict__))
            self.assertIsNone(record.exc_info);self.assertIsNone(record.stack_info)


if __name__=='__main__':unittest.main()
