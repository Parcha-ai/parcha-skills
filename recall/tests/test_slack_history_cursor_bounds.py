"""Keep the requested live history window across continuation pages."""

import json
import unittest
from decimal import Decimal

from connectors.slack_workspace import EPOCH, SlackWorkspaceConnector, _cursor, _initial_state


class WindowRail:
    public_history = True

    def __init__(self):
        self.calls = []
        self.rows = [
            {"type": "message", "user": "U111", "ts": str(timestamp),
             "text": "synthetic window record"}
            for timestamp in (1784333100, 1784333000, 1784332900, 1784332800)
        ]

    def request(self, operation, *, query):
        if operation != "messages.history":
            raise AssertionError("unexpected provider operation")
        self.calls.append(dict(query))
        lower = Decimal(query.get("oldest", "0"))
        upper = Decimal(query["latest"])
        rows = [row for row in self.rows if lower <= Decimal(row["ts"]) <= upper]
        # This fake models a position cursor, not an implicit filter snapshot.
        offset = int(query.get("cursor", "0"))
        end = offset + query["limit"]
        return {"ok": True, "messages": rows[offset:end],
                "has_more": end < len(rows),
                "response_metadata": {"next_cursor": str(end) if end < len(rows) else ""}}


class SlackHistoryCursorBoundsTests(unittest.TestCase):
    def connector(self, rail):
        return SlackWorkspaceConnector(
            rail=rail, source_id="synthetic:slack:window", workspace_id="T111", page_size=2,
        )

    def state(self, lower):
        return {**_initial_state(public_history=True), "phase": "history",
                "channels": ["C111:1"], "channel_lower": lower,
                "upper": "2026-07-18T00:10:00Z"}

    def test_live_continuation_preserves_window_after_connector_restart(self):
        rail = WindowRail()
        first = self.connector(rail).pull(_cursor(self.state("2026-07-18T00:01:40Z")))
        self.assertTrue(first.has_more)
        self.assertEqual(json.loads(first.next_cursor)["page"], "2")
        second = self.connector(rail).pull(first.next_cursor)
        timestamps = [record.native_id.rsplit(":", 1)[-1]
                      for page in (first, second) for record in page.records]
        self.assertEqual(timestamps, ["1784333100", "1784333000", "1784332900"])
        self.assertFalse(second.has_more)
        self.assertEqual({k: v for k, v in rail.calls[1].items() if k != "cursor"},
                         rail.calls[0])
        self.assertEqual(rail.calls[1]["oldest"], "1784332900.000000")
        self.assertEqual(rail.calls[1]["latest"], "1784333400.000000")
        self.assertTrue(rail.calls[1]["inclusive"])

    def test_epoch_continuation_keeps_full_history(self):
        rail = WindowRail()
        connector = self.connector(rail)
        first = connector.pull(_cursor(self.state(EPOCH)))
        second = connector.pull(first.next_cursor)
        self.assertEqual(len(first.records) + len(second.records), 4)
        self.assertFalse(second.has_more)
        self.assertTrue(all("oldest" not in query for query in rail.calls))


if __name__ == "__main__":
    unittest.main()
