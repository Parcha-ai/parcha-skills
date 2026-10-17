import unittest
import sys
from pathlib import Path

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]

from recall_server import mcp
from recall_server.db import SearchDeadlineExceeded
from recall_server.logical_evidence import LogicalEvidenceError


class ReceiptErrors(unittest.TestCase):
    def invoke(self, error, canonical=True):
        class Store:
            def show(self, *args, **kwargs):
                raise error
        store = Store()
        if canonical:
            store._show_passage = object()
        return mcp._call_tool(store, {}, 'recall_show', {'target': 'recall://source/native?rev=1#item=0'})

    def test_current_receipt_refusal_is_safe_protocol_error(self):
        with self.assertRaises(mcp.McpProtocolError) as caught:
            self.invoke(ValueError('private implementation detail'))
        self.assertEqual(caught.exception.code, -32602)
        self.assertEqual(caught.exception.message, 'source record unavailable for current receipt')

    def test_corruption_and_deadline_are_not_input_errors(self):
        for error in (LogicalEvidenceError('logical_evidence_full_record_corrupt'), SearchDeadlineExceeded()):
            with self.assertRaises(type(error)):
                self.invoke(error)

    def test_legacy_error_behavior_is_unchanged(self):
        with self.assertRaises(ValueError):
            self.invoke(ValueError('legacy'), canonical=False)


if __name__ == '__main__':
    unittest.main()
