"""Read-only diagnostics for an attribution backlog that is still being scanned."""
import hashlib
import json
import sqlite3
import unittest
from types import SimpleNamespace

from recall_server.slack_captured_attribution import (
    DEBT,
    KEY,
    RETRY_TURN,
    progress_report,
)


class CapturedProgressTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        self.row = {"id": "installation", "source_id": "source:slack"}
        self.runner = SimpleNamespace(db=self.db)
        self.state = dict(version=1, epoch="a" * 64, phase="contacts",
                          after="private-forward-id", upper="private-upper-id",
                          examined=500, unresolved=0, retry_after="private-retry-id")
        self.save()

    def save(self):
        self.db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",
                        (KEY, json.dumps(self.state)))
        self.db.commit()

    def report(self):
        return progress_report(self.row, self.runner)

    def test_cursor_fingerprints_distinguish_forward_from_retry_progress(self):
        before = self.report()
        self.state["after"] = "next-private-forward-id"
        self.save()
        after = self.report()
        self.assertNotEqual(before["forward_cursor_sha256"], after["forward_cursor_sha256"])
        self.assertEqual(before["retry_cursor_sha256"], after["retry_cursor_sha256"])
        self.assertIsNone(before["retry_turn"])
        self.db.execute("INSERT INTO meta VALUES (?,?)", (RETRY_TURN, "1"))
        self.assertTrue(self.report()["retry_turn"])
        self.db.execute("UPDATE meta SET value='0' WHERE key=?", (RETRY_TURN,))
        self.assertFalse(self.report()["retry_turn"])
        encoded = json.dumps(after)
        for private in (self.state["after"], self.state["retry_after"], self.state["upper"]):
            self.assertNotIn(private, encoded)

    def test_large_backlog_uses_a_bounded_sample_without_mutating_progress(self):
        self.state["unresolved"] = 10_000
        self.save()
        self.db.executemany("INSERT INTO meta VALUES (?,?)", (
            (DEBT + f"{index:064x}", json.dumps(
                dict(native_id=f"private-{index}", reason="ArchiveNotFound")))
            for index in range(10_000)))
        self.db.commit()
        changes = self.db.total_changes
        # Reading the entire backlog would exhaust this SQLite VM budget.
        self.db.set_progress_handler(lambda: 1, 2_000)
        try:
            report = self.report()
        finally:
            self.db.set_progress_handler(None, 0)
        sample = report["debt_reason_sample"]
        self.assertEqual(sample["count"], 32)
        self.assertEqual(sample["limit"], 32)
        self.assertEqual(sample["reasons"], {"ArchiveNotFound": 32})
        self.assertEqual(report["unresolved"], 10_000)
        self.assertEqual(report["examined"], 500)
        self.assertEqual(self.db.total_changes, changes)
        self.assertEqual(json.loads(self.db.execute(
            "SELECT value FROM meta WHERE key=?", (KEY,)).fetchone()[0]), self.state)

    def test_debt_sample_never_logs_native_ids_or_unrecognized_reason_text(self):
        self.state["unresolved"] = 3
        self.save()
        for index, value in enumerate((
            json.dumps(dict(native_id="private@example.com", reason="ValueError")),
            json.dumps(dict(native_id="private@example.com", reason="private@example.com")),
            "malformed-private@example.com",
        )):
            self.db.execute("INSERT INTO meta VALUES (?,?)", (DEBT + str(index), value))
        report = self.report()
        self.assertEqual(report["debt_reason_sample"]["reasons"],
                         {"ValueError": 1, "other": 1, "invalid_debt_record": 1})
        self.assertNotIn("private@example.com", json.dumps(report))
        self.assertEqual(report["status"], "unresolved")

    def test_fixed_capture_codes_are_reported_without_exception_text(self):
        codes = ("captured_archive_unavailable", "captured_archive_corrupt",
                 "captured_record_invalid", "captured_commitment_changed")
        self.state["unresolved"] = len(codes)
        self.save()
        for index, code in enumerate(codes):
            self.db.execute("INSERT INTO meta VALUES (?,?)",
                            (DEBT + str(index), json.dumps(dict(native_id="private", reason=code))))
        self.assertEqual(self.report()["debt_reason_sample"]["reasons"],
                         {code: 1 for code in codes})

    def test_existing_counter_debt_mismatch_still_fails(self):
        self.db.execute("INSERT INTO meta VALUES (?,?)",
                        (DEBT + "a", json.dumps(dict(native_id="private", reason="ValueError"))))
        with self.assertRaisesRegex(ValueError, "captured_progress_invalid"):
            self.report()

    def test_complete_report_keeps_existing_frontier_and_empty_sample(self):
        self.state["phase"] = "complete"
        self.save()
        report = self.report()
        self.assertEqual(report["status"], "complete")
        self.assertFalse(report["unresolved_entries_present"])
        self.assertEqual(report["frontier_sha256"],
                         hashlib.sha256(json.dumps(self.state["upper"]).encode()).hexdigest())
        self.assertEqual(report["debt_reason_sample"],
                         {"count": 0, "limit": 32, "reasons": {}})


if __name__ == "__main__":
    unittest.main()
