#!/usr/bin/env python3
"""A committed publication retains optional native certification work across restart."""
import unittest
from unittest.mock import patch
from e2e_parent_membership_publication import ParentMembershipPublicationTests
from recall_server import parent_membership_enrollment as enrollment


class MembershipRemainderTests(ParentMembershipPublicationTests):
    def test_restart_certifies_committed_publication_without_archive_republication(self):
        self.queue()
        with patch.object(enrollment, 'enroll_parent_passages',
                          side_effect=OSError('synthetic interruption after publication')):
            published = self.publisher().project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1)
        self.assertEqual(published['documents'], 1)
        self.assertEqual(published['membership_failed_documents'], 1)
        self.refresh_scope()
        self.assertEqual(self.compare()['certified_passages'], 0)
        restarted = self.publisher()
        with patch.object(restarted.logical_projection, 'read_part',
                          side_effect=AssertionError('certification must not reread archives')):
            resumed = restarted.project_pending(
                tenant_id=self.tenant, batch_size=10, max_batches=1, concurrency=1)
        self.assertEqual(resumed['documents'], 0)
        self.assertEqual(resumed['membership_certified_passages'], len(self.scope['passage_ids']))
        compared = self.compare()
        self.assertEqual(compared['certified_passages'], len(self.scope['passage_ids']))
        self.assertEqual(compared['native_allowed_passages'], len(self.scope['passage_ids']))
        self.assertEqual(compared['disagreements'], 0)
        self.assertEqual(resumed['pending'], 0)


if __name__ == '__main__':
    suite = unittest.TestSuite(MembershipRemainderTests(name)
                              for name in MembershipRemainderTests.__dict__
                              if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
