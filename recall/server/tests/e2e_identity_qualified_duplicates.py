#!/usr/bin/env python3
"""Public MCP keeps an exact receipt ahead of a similar independent message."""
import unittest
from unittest.mock import patch

from e2e_notification_priority import NotificationPriority, TENANT, OWNER, SLACK_SOURCE, Handler
from e2e_public_mcp_principal import tool
from recall_server.canonical_retrieval import CanonicalRetrieval


class IdentityQualifiedDuplicates(NotificationPriority):
    def test_public_search_preserves_rank_one_receipt_and_independent_parent(self):
        shared = ('The sapphire deployment is ready for the production review. '
                  'We have checked the workers and verified the current receipts '
                  'before asking the team to approve the release today.')
        events = self.history(0, 1, text=shared + ' quartzanchor')
        later = self.history(1, 1, text=shared)
        self.assertNotEqual(events[0]['native_parent_id'], later[0]['native_parent_id'])
        self.assertEqual(self.logical.project_pending(batch_size=2, max_batches=1,
            upload_concurrency=1)['documents'], 2)
        self.publish()
        with self.store.connect() as connection:
            receipt = connection.execute('''SELECT chunk.receipt FROM canonical_chunks chunk
                JOIN canonical_documents document USING(tenant_id,source_id,document_id)
                WHERE document.tenant_id=%s AND document.source_id=%s AND document.native_id=%s
                  AND document.is_current AND document.deleted_at IS NULL AND chunk.deleted_at IS NULL''',
                (TENANT, SLACK_SOURCE, events[0]['native_id'])).fetchone()['receipt']
        self.store.search_plane = 'turbopuffer'
        self.store.turbopuffer = self.retrieval.settings
        self.store.turbopuffer_client = self.retrieval.client
        with self.store.connect() as connection:
            for principal in (OWNER, 'principal:identity:outsider'):
                connection.execute('INSERT INTO brain_principals(tenant_id,principal_id) VALUES (%s,%s) ON CONFLICT DO NOTHING',
                                   (TENANT, principal))
                connection.execute("INSERT INTO brain_memberships(organization_id,principal_id,role) VALUES ('org:e2e:webhook',%s,'member')",
                                   (principal,))
                connection.execute("INSERT INTO brain_access_grants(tenant_id,principal_id,permission) VALUES (%s,%s,'read') ON CONFLICT DO NOTHING",
                                   (TENANT, principal))
        reader = self.store.create_mcp_token('identity-reader', tenant_id=TENANT,
            principal_id=OWNER, principal_kind='workload')['token']
        outsider = self.store.create_mcp_token('identity-outsider', tenant_id=TENANT,
            principal_id='principal:identity:outsider', principal_kind='workload')['token']
        arguments = {'query': 'sapphire quartzanchor', 'limit': 10}
        with patch.object(Handler, 'canonical_retrieval', CanonicalRetrieval(self.store, self.archive)), \
                patch.dict('os.environ', {'RECALL_HTTP_PROFILE': 'public-mcp',
                    'RECALL_CANONICAL_MCP_ENABLED': '1', 'RECALL_SEARCH_NEAR_DUPLICATES': 'off'}):
            control = tool(self.server, reader, 'recall_search', arguments)['result']['structuredContent']
            self.assertEqual(len(control['results']), 2)
            self.assertTrue(any(receipt in item['receipts']
                for item in control['results'][0]['matching_ranges']))
            with patch.dict('os.environ', {'RECALL_SEARCH_NEAR_DUPLICATES': 'on'}):
                response = tool(self.server, reader, 'recall_search', arguments)['result']['structuredContent']
                denied = tool(self.server, outsider, 'recall_search', arguments)['result']['structuredContent']
        self.assertEqual(denied['results'], [])
        self.assertEqual([row['native_parent_id'] for row in response['results']],
                         [row['native_parent_id'] for row in control['results']])
        self.assertTrue(any(receipt in item['receipts']
            for item in response['results'][0]['matching_ranges']))
        self.assertEqual(response['diagnostics']['near_duplicates_folded'], 0)


def load_tests(loader, tests, pattern):
    # Reuse fixture ownership without rerunning inherited scheduling tests.
    return unittest.TestSuite([IdentityQualifiedDuplicates(
        'test_public_search_preserves_rank_one_receipt_and_independent_parent')])


if __name__ == '__main__':
    unittest.main(verbosity=2)
