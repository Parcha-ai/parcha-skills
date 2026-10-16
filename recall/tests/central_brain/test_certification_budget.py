"""Optional proof allowance counts active work, including failure and cancellation."""
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from recall_server.passage_index import CertificationBudget


class CertificationBudgetTest(unittest.TestCase):
    def test_only_active_work_spends_finite_allowance(self):
        now = [0.0]
        budget = CertificationBudget()
        with patch('recall_server.passage_index.time', SimpleNamespace(monotonic=lambda: now[0])):
            with budget.lease() as deadline:
                self.assertEqual(deadline, 3)
                now[0] = .25
            now[0] = 10.25
            with budget.lease() as deadline:
                self.assertEqual(deadline, 13)
                now[0] = 13
            now[0] = 100
            with budget.lease() as deadline:
                self.assertIsNone(deadline)
            with CertificationBudget().lease() as deadline:
                self.assertEqual(deadline, 103)

    def test_competing_thread_defers_without_wait_or_renewal(self):
        budget = CertificationBudget()
        observed = []
        def competitor():
            with budget.lease() as deadline:
                observed.append(deadline)
        with budget.lease() as deadline:
            self.assertIsNotNone(deadline)
            thread = threading.Thread(target=competitor)
            thread.start()
            thread.join(timeout=1)
            self.assertFalse(thread.is_alive(), 'optional proof blocked source progress')
            self.assertEqual(observed, [None])

    def test_errors_and_cancellation_charge_and_release_allowance(self):
        now = [0.0]
        budget = CertificationBudget()
        with patch('recall_server.passage_index.time', SimpleNamespace(monotonic=lambda: now[0])):
            for error in (OSError, KeyboardInterrupt):
                with self.assertRaises(error):
                    with budget.lease() as deadline:
                        self.assertEqual(deadline, 3)
                        now[0] += 1
                        raise error()
            with budget.lease() as deadline:
                self.assertEqual(deadline, 3)
                now[0] += 1
            with budget.lease() as deadline:
                self.assertIsNone(deadline)
