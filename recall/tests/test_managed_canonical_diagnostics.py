from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import psycopg
from psycopg.errors import (
    CardinalityViolation, InvalidColumnReference, UndefinedFunction, UniqueViolation,
)
from connectors.sdk import ConnectorPage, ConnectorRunError, ConnectorRunner
from privacy.policy import PrivacyPolicy
from tests.test_connector_sdk import SyntheticConnector, record
from server.recall_server import managed_worker
from server.recall_server.canonical import CanonicalLifecycleError
from server.recall_server.canonical_history import HistoryAuthorityError, HistoryUnavailable


class ManagedCanonicalDiagnosticTests(unittest.TestCase):
    def writer(self, failure=None):
        plane = Mock()
        plane.ingest_batch.side_effect = failure
        writer = managed_worker._DirectCanonicalWriter(
            plane, tenant_id="private-tenant", principal_id="private-principal"
        )
        return writer, plane

    def test_success_identity_arguments_and_no_log(self):
        writer, plane = self.writer()
        events = [{"private-payload": "secret"}]
        with patch.object(managed_worker.LOG, "error") as log:
            self.assertIs(writer.ingest(events), plane.ingest_batch.return_value)
        plane.ingest_batch.assert_called_once_with(
            tenant_id="private-tenant", principal_id="private-principal", events=events
        )
        log.assert_not_called()

    def assert_failure(self, failure, expected):
        writer, plane = self.writer(failure)
        with self.assertLogs(managed_worker.LOG, level="ERROR") as logs:
            with self.assertRaises(type(failure)) as caught:
                writer.ingest([{"private-payload": "secret"}])
        self.assertIs(caught.exception, failure)
        plane.ingest_batch.assert_called_once()
        self.assertEqual(len(logs.records), 1)
        record = logs.records[0]
        self.assertEqual(record.getMessage(), expected + " depth=0")
        self.assertIsNone(record.exc_info)
        self.assertIsNone(record.stack_info)
        for secret in ("secret", "private", "Traceback"):
            self.assertNotIn(secret, str(record.__dict__))

    def test_known_canonical_code(self):
        self.assert_failure(
            CanonicalLifecycleError("canonical_history_unavailable"),
            "managed canonical ingest failed type=CanonicalLifecycleError "
            "code=canonical_history_unavailable sqlstate=unrecognized",
        )

    def test_real_psycopg_class_and_sqlstate_without_detail(self):
        self.assert_failure(
            UniqueViolation("secret private query and IDs"),
            "managed canonical ingest failed type=UniqueViolation "
            "code=unrecognized sqlstate=23505",
        )

    def test_registered_sql_errors_are_not_limited_to_handpicked_states(self):
        for kind, state in (
            (UndefinedFunction, "42883"),
            (CardinalityViolation, "21000"),
            (InvalidColumnReference, "42P10"),
        ):
            self.assert_failure(
                kind("secret query and payload"),
                f"managed canonical ingest failed type={kind.__name__} "
                f"code=unrecognized sqlstate={state}",
            )

    def test_unknown_psycopg_states_and_constant_aliases_are_masked(self):
        for state in ("ZZZZZ", "UNIQUE_VIOLATION", "secret", {"private": 1}, "42p10"):
            kind = type("private_secret_class", (psycopg.Error,), {"sqlstate": state})
            self.assert_failure(
                kind("secret payload"),
                "managed canonical ingest failed type=unrecognized "
                "code=unrecognized sqlstate=unrecognized",
            )

    def test_registered_class_overrides_hostile_psycopg_subclass_name(self):
        kind = type("private_secret_class", (psycopg.Error,), {"sqlstate": "42883"})
        self.assert_failure(
            kind("secret"),
            "managed canonical ingest failed type=UndefinedFunction "
            "code=unrecognized sqlstate=42883",
        )

    def test_unknown_class_and_hostile_attributes_are_closed(self):
        hostile = type("private_secret_class", (Exception,), {})
        for code, state in (("private-secret", "SECRET"), ({"secret": 1}, ["secret"])):
            error = hostile("private payload")
            error.error_code, error.sqlstate = code, state
            self.assert_failure(
                error, "managed canonical ingest failed type=unrecognized "
                "code=unrecognized sqlstate=unrecognized",
            )

    def test_unknown_canonical_code_is_not_logged(self):
        self.assert_failure(
            CanonicalLifecycleError("private-secret"),
            "managed canonical ingest failed type=CanonicalLifecycleError "
            "code=unrecognized sqlstate=unrecognized",
        )

    def test_attribute_failure_cannot_replace_original(self):
        class Hostile(Exception):
            @property
            def error_code(self):
                raise ValueError("secret")
        error = Hostile("private")
        writer, plane = self.writer(error)
        with self.assertRaises(Hostile) as caught:
            writer.ingest([])
        self.assertIs(caught.exception, error)
        plane.ingest_batch.assert_called_once()

    def test_logging_failure_cannot_replace_original(self):
        error = ValueError("secret")
        writer, _ = self.writer(error)
        with patch.object(managed_worker.LOG, "error", side_effect=RuntimeError("private")):
            with self.assertRaises(ValueError) as caught:
                writer.ingest([])
        self.assertIs(caught.exception, error)

    def chain_logs(self, error):
        writer, plane = self.writer(error)
        with self.assertLogs(managed_worker.LOG, level="ERROR") as logs:
            with self.assertRaises(type(error)) as caught:
                writer.ingest([{"private-payload": "secret"}])
        self.assertIs(caught.exception, error)
        plane.ingest_batch.assert_called_once()
        for entry in logs.records:
            self.assertIsNone(entry.exc_info)
            self.assertIsNone(entry.stack_info)
            for secret in ("private", "secret", "Traceback"):
                self.assertNotIn(secret, str(entry.__dict__))
        return [entry.getMessage() for entry in logs.records]

    def test_suppressed_history_chain_preserves_real_sqlstate_at_depth_two(self):
        try:
            try:
                try:
                    raise UndefinedFunction("private SQL secret")
                except Exception:
                    raise HistoryUnavailable() from None
            except HistoryUnavailable:
                raise CanonicalLifecycleError("canonical_history_unavailable") from None
        except CanonicalLifecycleError as error:
            self.assertTrue(error.__suppress_context__)
            self.assertTrue(error.__context__.__suppress_context__)
            messages = self.chain_logs(error)
        self.assertEqual(messages, [
            "managed canonical ingest failed type=CanonicalLifecycleError "
            "code=canonical_history_unavailable sqlstate=unrecognized depth=0",
            "managed canonical ingest failed type=HistoryUnavailable "
            "code=unrecognized sqlstate=unrecognized depth=1",
            "managed canonical ingest failed type=UndefinedFunction "
            "code=unrecognized sqlstate=42883 depth=2",
        ])

    def test_chain_cycle_and_three_node_bound(self):
        errors = [ValueError("secret") for _ in range(4)]
        for first, second in zip(errors, errors[1:]):
            first.__context__ = second
        self.assertEqual(len(self.chain_logs(errors[0])), 3)
        errors[1].__context__ = errors[0]
        self.assertEqual(len(self.chain_logs(errors[0])), 2)
        errors[0].__context__ = errors[0]
        self.assertEqual(len(self.chain_logs(errors[0])), 1)

    def test_explicit_cause_preferred_and_history_authority_name_closed(self):
        error = RuntimeError("secret")
        error.__cause__ = HistoryAuthorityError("private")
        error.__context__ = UniqueViolation("secret")
        messages = self.chain_logs(error)
        self.assertEqual(len(messages), 2)
        self.assertTrue(messages[1].endswith(
            "type=HistoryAuthorityError code=unrecognized sqlstate=unrecognized depth=1"))

    def test_chain_attribute_failure_preserves_original(self):
        class Hostile(Exception):
            def __getattribute__(self, name):
                if name == "__cause__":
                    raise ValueError("secret")
                return super().__getattribute__(name)
        error = Hostile("private")
        self.assertEqual(len(self.chain_logs(error)), 1)

    def test_inner_logging_failure_preserves_outer(self):
        error = ValueError("secret")
        error.__context__ = HistoryUnavailable()
        writer, _ = self.writer(error)
        with patch.object(managed_worker.LOG, "error", side_effect=[None, RuntimeError("private")]):
            with self.assertRaises(ValueError) as caught:
                writer.ingest([])
        self.assertIs(caught.exception, error)

    def test_actual_sdk_keeps_error_mapping_and_pending_cursor(self):
        for failure, code in (
            (CanonicalLifecycleError("canonical_history_unavailable"), "brain_unavailable"),
            (PermissionError("private credentials"), "brain_unauthorized"),
        ):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                writer, plane = self.writer(failure)
                connector = SyntheticConnector({
                    None: ConnectorPage(
                        records=(record("one", "private content"),),
                        next_cursor="next", has_more=False,
                    ),
                })
                runner = ConnectorRunner(
                    connector=connector, brain=writer,
                    spool_path=Path(directory) / "spool.db",
                    privacy=PrivacyPolicy(mode="off"),
                )
                try:
                    with self.assertLogs(managed_worker.LOG, level="ERROR"):
                        with self.assertRaisesRegex(ConnectorRunError, code):
                            runner.run_once()
                    self.assertEqual(runner.doctor()["pending"], 1)
                    self.assertFalse(runner.doctor()["checkpointed"])
                    self.assertEqual(runner._get_meta("last_error_code"), code)
                    self.assertEqual(connector.pulls, [None])
                    plane.ingest_batch.assert_called_once()
                finally:
                    runner.close()

    def test_termination_exceptions_are_untouched(self):
        for error in (KeyboardInterrupt(), SystemExit(2)):
            writer, _ = self.writer(error)
            with patch.object(managed_worker.LOG, "error") as log:
                with self.assertRaises(type(error)) as caught:
                    writer.ingest([])
            self.assertIs(caught.exception, error)
            log.assert_not_called()


if __name__ == "__main__":
    unittest.main()
