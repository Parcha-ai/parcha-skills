"""Recover the exact #424 collector shape with its immutable member binding."""
import copy
import functools
import hashlib
import json
import unittest
from unittest.mock import patch

import e2e_collector_history_compacted as legacy
from collector.collector import Collector
from recall_server.projectors import canonical_json

# Exact #424 SQL oracle from3f01427c. Keep independent of the recovery predicate.
def _compact_event_expression(alias: str = "event") -> str:
    """Retain only routing metadata used after the full body moves to raw storage."""

    def structural(path: str) -> str:
        return f"jsonb_strip_nulls(jsonb_build_object('role',{alias}.canonical_redacted #> '{{{path},role}}','type',{alias}.canonical_redacted #> '{{{path},type}}'))"

    content_metadata = (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'role',{alias}.canonical_redacted #> '{{content,role}}',"
        f"'type',{alias}.canonical_redacted #> '{{content,type}}',"
        f"'message',{structural('content,message')},"
        f"'payload',{structural('content,payload')}))"
    )
    content = (
        f"CASE WHEN {alias}.canonical_redacted #>> '{{content,contract}}'="
        "'recall.oversized-projection.v1' "
        f"THEN {alias}.canonical_redacted->'content' ELSE {content_metadata} END"
    )
    return (
        "jsonb_strip_nulls(jsonb_build_object("
        f"'role',{alias}.canonical_redacted->'role',"
        f"'type',{alias}.canonical_redacted->'type',"
        f"'provenance',{alias}.canonical_redacted->'provenance',"
        f"'content',{content},"
        f"'message',{structural('message')},"
        f"'payload',{structural('payload')}))"
    )

class FullProvenanceRecovery(legacy.LegacyCompactedRecovery):
    def setUp(self):
        with patch.object(legacy, 'legacy_compact', _compact_event_expression), \
             patch.object(legacy, 'Collector', functools.partial(Collector, bulk_manifest_archive=True)):
            super().setUp()
        self.original_metadata = self.metadata()
        self.assertEqual(len(self.original_metadata['provenance']), 10)
        member = self.original_metadata['provenance']['artifact_member']
        base = {key: value for key, value in json.loads(self.original_text).items()
                if key != '_recall_collector_generation'}
        self.assertEqual(member['content_sha256'], hashlib.sha256(canonical_json(base)).hexdigest())
        self.assertNotEqual(member['content_sha256'], self.expected['text_sha256'])

    def metadata(self):
        with self.store.connect() as connection:
            return connection.execute('SELECT canonical_redacted FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND revision=1',
                                      (self.tenant, self.source)).fetchone()['canonical_redacted']

    def change_metadata(self, value):
        with self.store.connect() as connection:
            connection.execute('UPDATE canonical_events SET canonical_redacted=%s::jsonb WHERE tenant_id=%s AND source_id=%s AND revision=1',
                               (json.dumps(value), self.tenant, self.source))

    def test_exact_historical_bulk_collector_restores_through_public_reader(self):
        self.test_old_compacted_collector_recovers_private_new_generation()
        with self.store.connect() as connection:
            event = connection.execute('SELECT canonical_redacted FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND revision=3',
                                       (self.tenant, self.source)).fetchone()['canonical_redacted']
        self.assertEqual(event['provenance']['artifact_member'], self.original_metadata['provenance']['artifact_member'])
        self.assertEqual(event['provenance']['original_path'], self.original_metadata['provenance']['original_path'])

    def test_member_identity_or_normalized_hash_mismatch_refuses(self):
        changes = [('native_id', 'different-native'), ('content_sha256', '0' * 64),
                   ('content_sha256', self.expected['text_sha256']),
                   ('manifest_sha256', '0' * 64), ('byte_start', 1), ('byte_end', 1),
                   ('ordinal', -1), ('schema_version', True), ('contract', 'other')]
        for key, value in changes:
            with self.subTest(key=key, value=value):
                metadata = copy.deepcopy(self.original_metadata)
                metadata['provenance']['artifact_member'][key] = value
                self.change_metadata(metadata)
                with self.assertRaisesRegex(RuntimeError, 'lineage_changed'):
                    self.recover()
                self.assert_no_correction()

    def test_member_extra_fields_and_unknown_provenance_refuse(self):
        for location in ('provenance', 'member'):
            with self.subTest(location=location):
                metadata = copy.deepcopy(self.original_metadata)
                target = metadata['provenance'] if location == 'provenance' else metadata['provenance']['artifact_member']
                target['unknown'] = 'synthetic'
                self.change_metadata(metadata)
                with self.assertRaisesRegex(RuntimeError, 'lineage_changed'):
                    self.recover()
                self.assert_no_correction()

    def test_missing_provenance_or_member_keys_refuse(self):
        for location, key in [('provenance', 'original_path'), ('provenance', 'artifact_ref'),
                              ('provenance', 'privacy_policy_version'), ('member', 'ordinal')]:
            with self.subTest(location=location, key=key):
                metadata = copy.deepcopy(self.original_metadata)
                target = metadata['provenance'] if location == 'provenance' else metadata['provenance']['artifact_member']
                del target[key]
                self.change_metadata(metadata)
                with self.assertRaisesRegex(RuntimeError, 'lineage_changed'):
                    self.recover()
                self.assert_no_correction()

    def test_generation_type_and_presence_are_required(self):
        from recall_server.collector_history_recovery import _validate_full_provenance
        provenance = self.original_metadata['provenance']
        for generation in (None, True, -1, 1.0, '0'):
            with self.subTest(generation=generation):
                content = json.loads(self.original_text)
                if generation is None:
                    del content['_recall_collector_generation']
                else:
                    content['_recall_collector_generation'] = generation
                # Removing generation still gives the genuine member digest;
                # reject malformed generation independently of that equality.
                with self.assertRaisesRegex(RuntimeError, 'lineage_changed'):
                    _validate_full_provenance(provenance, content, self.expected, provenance['artifact_ref'])
                self.assert_no_correction()

    def test_oversized_pointer_is_outside_this_historical_variant(self):
        from recall_server.collector_history_recovery import _validate_full_provenance
        provenance = copy.deepcopy(self.original_metadata['provenance'])
        content = json.loads(self.original_text)
        content['contract'] = 'recall.oversized-projection.v1'
        base = {key: value for key, value in content.items() if key != '_recall_collector_generation'}
        provenance['artifact_member']['content_sha256'] = hashlib.sha256(canonical_json(base)).hexdigest()
        with self.assertRaisesRegex(RuntimeError, 'lineage_changed'):
            _validate_full_provenance(provenance, content, self.expected, provenance['artifact_ref'])
        self.assert_no_correction()

    def test_artifact_reference_must_match_original_raw_row(self):
        for key, value in [('tenant_id', 'tenant:other'), ('source_id', 'source:other'),
                           ('artifact_id', 'raw_other'), ('object_key', 'objects/00/' + 'a' * 64),
                           ('content_sha256', '0' * 64), ('size_bytes', 1), ('version_id', 'other'),
                           ('created_at', 'not-a-timestamp'), ('created_at', None),
                           ('created_at', '2025-01-01T00:00:00Z')]:
            with self.subTest(key=key):
                metadata = copy.deepcopy(self.original_metadata)
                metadata['provenance']['artifact_ref'][key] = value
                self.change_metadata(metadata)
                with self.assertRaisesRegex(RuntimeError, 'lineage_changed'):
                    self.recover()
                self.assert_no_correction()

    test_present_mismatching_principal_still_refuses = legacy.LegacyCompactedRecovery.test_present_mismatching_principal_still_refuses
    test_partially_missing_or_null_attribution_is_not_legacy_shape = legacy.LegacyCompactedRecovery.test_partially_missing_or_null_attribution_is_not_legacy_shape
    test_same_length_body_corruption_refused = legacy.LegacyCompactedRecovery.test_same_length_body_corruption_refused
    test_read_grant_without_company_admin_refused = legacy.LegacyCompactedRecovery.test_read_grant_without_company_admin_refused
    test_source_revoked_during_archive_metadata_check_refused = legacy.LegacyCompactedRecovery.test_source_revoked_during_archive_metadata_check_refused
    test_explicit_forget_always_vetoes_correction = legacy.LegacyCompactedRecovery.test_explicit_forget_always_vetoes_correction
    test_competing_native_lock_refuses_without_waiting = legacy.LegacyCompactedRecovery.test_competing_native_lock_refuses_without_waiting
    test_failure_after_canonical_write_rolls_back_event_queue_and_audit = legacy.LegacyCompactedRecovery.test_failure_after_canonical_write_rolls_back_event_queue_and_audit
    test_actual_commit_lost_ack_is_observed_without_replay = legacy.LegacyCompactedRecovery.test_actual_commit_lost_ack_is_observed_without_replay

if __name__ == '__main__':
    names = [name for name in FullProvenanceRecovery.__dict__ if name.startswith('test_')]
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(FullProvenanceRecovery(name) for name in names))
    raise SystemExit(not result.wasSuccessful())
