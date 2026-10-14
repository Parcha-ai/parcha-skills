#!/usr/bin/env python3
"""Completion logs distinguish actual input from estimates and failed attempts."""

import unittest
from unittest.mock import patch

import e2e_logical_append_capability as fixtures
from recall_server.logical_evidence_projection import LOG


class AppendObservability(unittest.TestCase):
    setUp = fixtures.AppendCapability.setUp
    event = fixtures.AppendCapability.event
    ingest = fixtures.AppendCapability.ingest
    publish = fixtures.AppendCapability.publish

    def completion(self, operation):
        with self.assertLogs(LOG.name, level="INFO") as captured:
            result = operation()
        lines = [line for line in captured.output if "logical owner completed " in line]
        self.assertEqual(len(lines), 1, lines)
        self.assertNotIn("An exact archived body", lines[0])
        return result, dict(
            item.split("=", 1) for item in lines[0].split() if "=" in item
        )

    def test_full_bootstrap_then_append_report_actual_input_including_exclusions(self):
        self.ingest("old-1", 1)
        self.ingest("old-2", 2)
        self.ingest("excluded-old", 3, record_type="token_count")
        result, log = self.completion(self.publish)
        self.assertEqual(result["documents"], 1)
        self.assertEqual(log["status"], "committed")
        self.assertEqual(log.get("append_used"), "False")
        self.assertEqual(log.get("input_sql_rows"), "3")
        self.ingest("new", 4)
        self.ingest("excluded-new", 5, record_type="token_count")
        self.logical.append_enabled = True
        result, log = self.completion(self.publish)
        self.assertEqual(result["documents"], 1)
        self.assertEqual(log["status"], "committed")
        self.assertEqual(log.get("append_used"), "True")
        self.assertEqual(log.get("input_sql_rows"), "2")
        self.assertEqual(result["records"], 3)

    def test_prepared_append_that_loses_generation_is_not_committed_adoption(self):
        self.ingest("old", 1)
        self.publish()
        self.ingest("new", 2)
        self.logical.append_enabled = True
        original = self.logical._commit_upload

        def supersede(candidate, upload):
            self.ingest("later", 3)
            return original(candidate, upload)

        with patch.object(self.logical, "_commit_upload", supersede):
            result, log = self.completion(self.publish)
        self.assertEqual(result["documents"], 0)
        self.assertEqual(log["status"], "stale")
        self.assertEqual(log.get("append_used"), "True")
        self.assertEqual(log.get("input_sql_rows"), "1")

    def test_preparation_failure_does_not_report_zero_or_success(self):
        self.ingest("old", 1)
        with patch.object(
            self.logical.projection, "put_records", side_effect=ConnectionError
        ):
            result, log = self.completion(self.publish)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(log["status"], "failed")
        self.assertEqual(log.get("append_used"), "None")
        self.assertEqual(log.get("input_sql_rows"), "None")

    def test_commit_unknown_does_not_report_committed_adoption(self):
        self.ingest("old", 1)
        original = self.logical._commit_upload

        def lost_ack(*args, **kwargs):
            original(*args, **kwargs)
            raise ConnectionError("synthetic lost acknowledgment")

        with (
            patch.object(self.logical, "_commit_upload", lost_ack),
            self.assertLogs(LOG.name, level="INFO") as captured,
        ):
            with self.assertRaises(ConnectionError):
                self.publish()
        lines = [line for line in captured.output if "logical owner completed " in line]
        self.assertEqual(len(lines), 1)
        log = dict(item.split("=", 1) for item in lines[0].split() if "=" in item)
        self.assertEqual(log["status"], "error")
        self.assertEqual(log.get("input_sql_rows"), "None")

    def test_logging_failure_does_not_change_publication(self):
        self.ingest("old", 1)
        with patch.object(
            LOG, "info", side_effect=RuntimeError("synthetic log failure")
        ):
            self.assertEqual(self.publish()["documents"], 1)


if __name__ == "__main__":
    unittest.main()
