# ruff: noqa: E402  # Direct scripts add the repository modules to sys.path.
"""Preserve owner certainty; metadata timeouts are not unknown commits."""

from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path[:0] = [
    str(Path(__file__).resolve().parents[1]),
    str(Path(__file__).resolve().parents[2]),
    str(Path(__file__).resolve().parents[2] / "server"),
]
from recall_server import retirement_enrollment as enrollment
from recall_server.db import SearchDeadlineExceeded
from central_brain.test_retirement_campaign import plan


class EnrollmentTests(unittest.TestCase):
    def call(self, operator):
        with patch.object(enrollment.operator, "execute", side_effect=operator):
            return enrollment.EnrollmentOwner(Mock(), Mock())(plan("a", "1"), Mock())

    def test_read_only_timeout_or_busy_is_known_deferral(self):
        class Busy(Exception):
            sqlstate = "55P03"

        for error, status in (
            (TimeoutError(), "metadata_timeout"),
            (SearchDeadlineExceeded("search deadline exceeded"), "metadata_timeout"),
            (Busy(), "metadata_lock_busy"),
        ):

            def owner(*args, **kwargs):
                raise error

            result = self.call(owner)
            self.assertEqual((result["certainty"], result["status"]), ("known", status))

    def test_publication_or_enrollment_timeout_is_unknown(self):
        for status in (
            "publication_inflight",
            "enrollment_inflight",
            "publication_unknown",
            "enrollment_unknown",
        ):
            for error in (TimeoutError("private details"), SearchDeadlineExceeded()):
                with self.subTest(status=status, error=type(error).__name__):

                    def owner(*args, **kwargs):
                        kwargs["emit"]("parent", dict(status=status))
                        raise error

                    result = self.call(owner)
                    self.assertEqual(result["certainty"], "unknown")
                    self.assertNotIn("private details", str(result))

    def test_known_publication_failure_retains_committed_report(self):
        event = dict(status="publication_failed", committed=dict(published_documents=2))

        def owner(*args, **kwargs):
            kwargs["emit"]("parent", event)
            raise TimeoutError()

        result = self.call(owner)
        self.assertEqual(result["certainty"], "known")
        self.assertEqual(result["owner_result"], event)

    def test_unclassified_error_never_becomes_known_success(self):
        def owner(*args, **kwargs):
            raise OSError("unclassified")

        with self.assertRaises(OSError):
            self.call(owner)

    def test_durable_sink_failure_does_not_continue_owner(self):
        events = []

        def owner(*args, **kwargs):
            kwargs["emit"]("parent", dict(status="enrollment_inflight"))
            events.append("mutation")

        with patch.object(enrollment.operator, "execute", side_effect=owner):
            with self.assertRaises(OSError):
                enrollment.EnrollmentOwner(Mock(), Mock())(
                    plan("a", "1"), Mock(side_effect=OSError())
                )
        self.assertEqual(events, [])

    def test_quantum_reuses_one_owner_budget_and_identifies_later_read_failure(self):
        plans = [plan("a", "1"), plan("a", "2")]

        def owner(*args, **kwargs):
            self.assertEqual(kwargs["cfg"]["plans"], plans)
            kwargs["emit"]("parent", dict(index=0, status="enrolled"))
            kwargs["emit"]("parent", dict(index=1, status="metadata_started"))
            raise TimeoutError()

        with patch.object(enrollment.operator, "execute", side_effect=owner) as execute:
            result = enrollment.EnrollmentOwner(Mock(), Mock()).quantum(plans, Mock())
        self.assertEqual(execute.call_count, 1)
        self.assertEqual(
            [row["status"] for row in result], ["enrolled", "metadata_timeout"]
        )
        self.assertTrue(all(row["certainty"] == "known" for row in result))


if __name__ == "__main__":
    unittest.main()
