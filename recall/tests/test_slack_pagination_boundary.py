"""Diagnose refused Slack pages without recording provider content or identifiers."""

import unittest

from connectors.sdk import ConnectorContractError
from connectors.slack_reconciliation import time_page


class SlackPaginationBoundary(unittest.TestCase):
    def test_refusal_reports_only_fixed_boundary_labels(self):
        for descending in (True, False):
            for timestamp, relation in (
                ('100.000001', 'before_oldest'),
                ('200.000002', 'equal_oldest'),
                ('400.000004', 'equal_latest'),
                ('500.000005', 'after_latest'),
            ):
                with self.subTest(descending=descending, relation=relation):
                    query = {'oldest': '200.000002', 'latest': '400.000004',
                             'channel': 'private-channel', 'ts': timestamp}
                    response = {'has_more': True,
                                'response_metadata': {'next_cursor': 'private-cursor'}}
                    with self.assertLogs('connectors.slack_reconciliation', level='WARNING') as logs:
                        with self.assertRaisesRegex(ConnectorContractError, 'did not advance'):
                            time_page(response, [{'ts': timestamp, 'text': 'private-body'}],
                                      query, descending=descending)
                    self.assertEqual([record.getMessage() for record in logs.records], [
                        'slack historical boundary refused direction='
                        + ('descending' if descending else 'ascending')
                        + f' relation={relation} root=True more=True cursor=True count=1'
                    ])

    def test_parent_echo_cannot_hide_more_pages_or_nonparent_boundary_errors(self):
        query = {'oldest': '200.000002', 'latest': '400.000004', 'ts': '100.000001'}
        parent = {'ts': query['ts']}
        terminal = {'has_more': False, 'response_metadata': {'next_cursor': ''}}
        cases = [
            (terminal, [{'ts': '150.000001'}], query, False),
            ({'has_more': True}, [parent], query, False),
            ({'has_more': False, 'response_metadata': {'next_cursor': 'opaque'}},
             [parent], query, False),
            ({}, [parent], query, False),
            (terminal, [parent, parent], query, False),
            (terminal, [parent, {'ts': '250.000001'}], query, False),
            (terminal, [parent], query, True),
            (terminal, [{'ts': '400.000004'}], dict(query, ts='400.000004'), False),
            (terminal, [{'ts': '500.000005'}], dict(query, ts='500.000005'), False),
        ]
        for response, messages, params, descending in cases:
            with self.subTest(response=response, messages=messages, descending=descending):
                with self.assertLogs('connectors.slack_reconciliation', level='WARNING'):
                    with self.assertRaisesRegex(ConnectorContractError, 'did not advance'):
                        time_page(response, messages, params, descending=descending)

    def test_valid_pages_keep_their_original_boundary_and_emit_no_warning(self):
        messages = [{'ts': '250.000001'}, {'ts': '350.000001'}]
        query = {'oldest': '200.000002', 'latest': '400.000004'}
        with self.assertNoLogs('connectors.slack_reconciliation', level='WARNING'):
            self.assertEqual(time_page({'has_more': True}, messages, query, descending=True),
                             '250.000001')
            self.assertEqual(time_page({'has_more': True}, messages, query, descending=False),
                             '350.000001')
            self.assertIsNone(time_page({'has_more': False}, messages, query, descending=True))


if __name__ == '__main__':
    unittest.main()
