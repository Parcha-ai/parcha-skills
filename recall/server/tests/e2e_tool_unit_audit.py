#!/usr/bin/env python3
"""Old-schema audit binding and refusal of unsupported extraction contracts."""
import unittest

import e2e_passage_origin as fixture
from evals.passage_index_audit import _candidates, _exact_sample, PassageIndexAuditError


class ToolUnitAudit(unittest.TestCase):
    setUp = fixture.PassageOrigin.setUp
    ingest = fixture.PassageOrigin.ingest
    project = fixture.PassageOrigin.project
    projected = fixture.PassageOrigin.projected

    def test_legacy_schema_binding_and_nonlegacy_capability_refusal(self):
        self.ingest('legacy-event'); self.project()
        rows = self.projected()
        candidates = _candidates(self.store, rows)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(_exact_sample(self.store, self.passages.logical_projection, rows,
            policy=self.passages.policy, tenant_id=self.tenant), 1)
        with self.assertRaisesRegex(PassageIndexAuditError, 'differs from S3'):
            _exact_sample(self.store, self.passages.logical_projection,
                [{**rows[0], 'text_redacted': rows[0]['text_redacted'][:-1]}],
                policy=self.passages.policy, tenant_id=self.tenant)
        # Simulate a future publisher's metadata without adding its extraction
        # implementation or migration to this contract-only change.
        with self.store.connect() as connection:
            connection.execute("""ALTER TABLE canonical_passage_documents
                ADD COLUMN IF NOT EXISTS applied_extraction_version text NOT NULL DEFAULT 'legacy'""")
            connection.execute("UPDATE canonical_passage_documents SET applied_extraction_version='tool_groups_v1' WHERE tenant_id=%s", (self.tenant,))
        if hasattr(next(iter(candidates.values())), 'extraction_version'):
            self.assertEqual(next(iter(_candidates(self.store, rows).values())).extraction_version,
                             'tool_groups_v1')
        else:
            with self.assertRaisesRegex(PassageIndexAuditError, 'version-aware projector'):
                _candidates(self.store, rows)


if __name__ == '__main__':
    unittest.main(verbosity=2)
