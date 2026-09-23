"""Only a rolled-back read probe may yield to fresh committed-key hints."""

from contextlib import contextmanager
import unittest
from types import SimpleNamespace
from psycopg.pq import TransactionStatus

from psycopg.errors import QueryCanceled
from recall_server.canonical_thinning import CanonicalBodyThinner


class Store:
    def __init__(
        self,
        *,
        failures=1,
        fail_hint=False,
        fail_write=False,
        fail_rollback=False,
        fail_commit=False,
        other_error=False,
        fail_rollback_cancel=False,
        fail_release_cancel=False,
    ):
        self.failures = failures
        self.fail_hint, self.fail_write = fail_hint, fail_write
        self.fail_rollback, self.fail_commit = fail_rollback, fail_commit
        self.other_error = other_error
        self.fail_rollback_cancel = fail_rollback_cancel
        self.fail_release_cancel = fail_release_cancel
        self.closed = self.broken = False
        self.info = SimpleNamespace(transaction_status=TransactionStatus.INTRANS)
        self.calls = []
        self.in_savepoint = False
        self.rollbacks = 0

    @contextmanager
    def connect(self):
        yield self
        if self.fail_commit:
            raise ConnectionError("unknown commit")

    @contextmanager
    def transaction(self):
        self.in_savepoint = True
        try:
            yield self
            if self.fail_release_cancel:
                raise QueryCanceled("savepoint release cancelled")
        except QueryCanceled:
            self.rollbacks += 1
            if self.fail_rollback_cancel:
                raise QueryCanceled("rollback cancelled") from None
            if self.fail_rollback:
                raise RuntimeError("rollback unavailable") from None
            raise
        finally:
            self.in_savepoint = False

    def execute(self, sql, params):
        self.calls.append((sql, params))
        if sql.startswith("SET LOCAL"):
            pass
        elif sql.lstrip().startswith("SELECT source_id,document_id"):
            self.rows = [dict(source_id="s", document_id="a")]
        elif "updated_documents AS" in sql:
            if self.fail_write:
                raise QueryCanceled("mutation cancelled")
            self.row = dict(
                candidates=1, documents=1, events=1, document_bytes=10, event_bytes=20
            )
        elif "WITH candidates AS MATERIALIZED" in sql:
            is_hint = params[1] == ["recent"]
            if is_hint:
                assert not self.in_savepoint
                if self.fail_hint:
                    raise QueryCanceled("hint cancelled")
                self.rows = [dict(source_id="s", document_id="recent")]
            else:
                if self.other_error:
                    raise ValueError("probe invalid")
                if self.failures:
                    self.failures -= 1
                    raise QueryCanceled("historical cancelled")
                self.rows = [dict(source_id="s", document_id="a")]
        else:
            raise AssertionError(sql)
        return self

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.row


class ProbeTimeoutTests(unittest.TestCase):
    def thinner(self, store):
        thinner = CanonicalBodyThinner(store, tenant_id="tenant")
        thinner._after, thinner._through = ("s", "0"), ("s", "z")
        return thinner

    def test_cancelled_history_yields_to_hints_then_revisits_smaller_window(self):
        store = Store()
        thinner = self.thinner(store)
        first = thinner.thin(
            batch_size=1,
            committed_keys=(("tenant", "s", "recent"), ("foreign", "s", "foreign")),
        )
        self.assertEqual(store.rollbacks, 1)
        self.assertEqual(first["historical_probe_timeouts"], 1)
        self.assertEqual(first["historical_window_size"], 512)
        self.assertEqual(first["committed_hints_pending"], 0)
        self.assertEqual(first["documents"], 1)
        self.assertEqual(first["status"], "pending")
        self.assertFalse(first["pass_complete"])
        self.assertEqual(thinner._after, ("s", "0"))
        second = thinner.thin(batch_size=1)
        self.assertEqual(second["historical_probe_timeouts"], 0)
        self.assertEqual(thinner._after, ("s", "a"))
        windows = [
            params
            for sql, params in store.calls
            if sql.lstrip().startswith("SELECT source_id,document_id")
        ]
        self.assertEqual([p[-1] for p in windows], [1024, 512])
        self.assertEqual(windows[0][:-1], windows[1][:-1])

    def test_repeated_cancel_halves_only_future_window_and_never_skips(self):
        store = Store(failures=20)
        thinner = self.thinner(store)
        sizes = [
            thinner.thin(batch_size=1)["historical_window_size"] for _ in range(12)
        ]
        self.assertEqual(sizes, [512, 256, 128, 64, 32, 16, 8, 4, 2, 1, 1, 1])
        self.assertEqual(thinner._after, ("s", "0"))
        self.assertFalse(any("updated_documents AS" in sql for sql, _ in store.calls))

    def test_rollback_commit_hint_and_mutation_failures_never_publish_progress(self):
        for option, error in [
            ("fail_rollback", RuntimeError),
            ("fail_commit", ConnectionError),
            ("fail_hint", QueryCanceled),
            ("fail_write", QueryCanceled),
            ("other_error", ValueError),
            ("fail_rollback_cancel", QueryCanceled),
        ]:
            with self.subTest(option=option):
                store = Store(**{option: True})
                thinner = self.thinner(store)
                with self.assertRaises(error):
                    thinner.thin(
                        batch_size=1, committed_keys=(("tenant", "s", "recent"),)
                    )
                self.assertEqual(thinner._after, ("s", "0"))
                self.assertEqual(thinner._through, ("s", "z"))
                self.assertEqual(thinner._window_size, 1024)
                self.assertEqual(list(thinner._committed_keys), [("s", "recent")])

    def test_suppressed_rollback_failure_never_allows_hints_or_progress(self):
        for state in ("aborted", "idle", "closed", "broken"):
            with self.subTest(state=state):

                class RollbackFailure(Store):
                    @contextmanager
                    def transaction(self):
                        try:
                            yield self
                        except QueryCanceled:
                            # psycopg's _exit_gen can warn about rollback error
                            # then rethrow this identical SELECT exception.
                            if state == "aborted":
                                self.info.transaction_status = TransactionStatus.INERROR
                            elif state == "idle":
                                self.info.transaction_status = TransactionStatus.IDLE
                            else:
                                setattr(self, state, True)
                            raise

                store = RollbackFailure()
                thinner = self.thinner(store)
                with self.assertRaises(QueryCanceled):
                    thinner.thin(
                        batch_size=1, committed_keys=(("tenant", "s", "recent"),)
                    )
                self.assertEqual(thinner._after, ("s", "0"))
                self.assertEqual(thinner._window_size, 1024)
                self.assertEqual(list(thinner._committed_keys), [("s", "recent")])
                self.assertEqual(
                    sum(
                        "WITH candidates AS MATERIALIZED" in sql
                        for sql, _ in store.calls
                    ),
                    1,
                )

    def test_original_window_cap_still_bounds_existing_instance(self):
        store = Store(failures=0)
        thinner = self.thinner(store)
        thinner.WINDOW_SIZE = 1
        thinner.thin(batch_size=1)
        window = next(
            params
            for sql, params in store.calls
            if sql.lstrip().startswith("SELECT source_id,document_id")
        )
        self.assertEqual(window[-1], 1)

    def test_savepoint_release_cancellation_is_not_a_probe_timeout(self):
        store = Store(failures=0, fail_release_cancel=True)
        thinner = self.thinner(store)
        with self.assertRaises(QueryCanceled):
            thinner.thin(batch_size=1)
        self.assertEqual(thinner._after, ("s", "0"))
        self.assertEqual(thinner._window_size, 1024)


if __name__ == "__main__":
    unittest.main()
