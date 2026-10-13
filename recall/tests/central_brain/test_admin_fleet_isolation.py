"""Fleet statistics cannot prevent the authenticated control state from loading."""
from contextlib import contextmanager
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))
from recall_server.app import Handler
from recall_server.control import ControlError, ControlPlane
from recall_server.db import BrainStore, SearchDeadlineExceeded


class AdminFleetIsolationTests(unittest.TestCase):
    def setUp(self):
        self.connection = Mock()
        self.connection.execute.return_value.fetchall.return_value = []
        self.store = Mock()

        @contextmanager
        def connect():
            yield self.connection

        self.store.connect = connect
        self.plane = ControlPlane(self.store, object(), {})
        self.plane._brain_rows = Mock(return_value=[{"tenant_id": "tenant:company"}])
        self.plane._brain_invitations = Mock(return_value=[])

    def test_core_state_never_queries_fleet_or_its_membership(self):
        self.store.fleet_status.side_effect = psycopg.errors.QueryCanceled("private SQL")
        result = self.plane.state("principal:owner")
        self.assertEqual(result["brains"], [{"tenant_id": "tenant:company"}])
        self.assertNotIn("fleet", result)
        self.store.fleet_status.assert_not_called()
        self.assertFalse(any("brain_memberships" in call.args[0]
                             for call in self.connection.execute.call_args_list))

    def test_fleet_uses_both_existing_admin_authority_predicates(self):
        self.connection.execute.return_value.fetchall.return_value = [
            {"tenant_id": "tenant:company"}]
        self.store.fleet_status.return_value = [{"source_id": "source:one"}]
        self.assertEqual(self.plane.fleet("principal:owner"),
                         {"fleet": [{"source_id": "source:one"}]})
        sql, params = self.connection.execute.call_args.args
        self.assertEqual(params, ("principal:owner",))
        for predicate in ("access.permission IN ('owner','admin')",
                          "membership.role IN ('owner','admin')",
                          "membership.principal_id=access.principal_id",
                          "membership.organization_id=space.organization_id"):
            self.assertIn(predicate, sql)
        self.store.fleet_status.assert_called_once_with(["tenant:company"])

    def test_outsider_has_no_fleet_scope(self):
        self.store.fleet_status.return_value = []
        self.assertEqual(self.plane.fleet("principal:outsider"), {"fleet": []})
        self.store.fleet_status.assert_called_once_with([])

    def handler(self, path):
        handler = object.__new__(Handler)
        handler.path = path
        handler.headers = {}
        handler.hide_non_public_route = Mock(return_value=False)
        handler.admin_web_enabled = Mock(return_value=True)
        handler.control_plane = self.plane
        handler.send_json = Mock()
        return handler

    def test_fleet_timeout_is_generic_and_state_still_loads(self):
        self.plane.authenticate_session = Mock(return_value={"principal_id": "principal:owner"})
        self.store.fleet_status.side_effect = SearchDeadlineExceeded("private SQL")
        handler = self.handler("/admin/api/v1/fleet")
        Handler.do_GET(handler)
        handler.send_json.assert_called_once_with(503, {"error": "fleet_unavailable"})
        handler = self.handler("/admin/api/v1/state")
        Handler.do_GET(handler)
        self.assertEqual(handler.send_json.call_args.args[0], 200)
        self.assertNotIn("fleet", handler.send_json.call_args.args[1])

    def test_fleet_requires_existing_session_authentication(self):
        self.plane.authenticate_session = Mock(side_effect=ControlError("admin_session_invalid", 401))
        handler = self.handler("/admin/api/v1/fleet")
        Handler.do_GET(handler)
        handler.send_json.assert_called_once_with(401, {"error": "admin_session_invalid"})
        self.store.fleet_status.assert_not_called()

    def test_unexpected_fleet_errors_are_not_disguised_as_timeout(self):
        self.plane.authenticate_session = Mock(return_value={"principal_id": "principal:owner"})
        self.store.fleet_status.side_effect = RuntimeError("unexpected")
        with self.assertRaisesRegex(RuntimeError, "unexpected"):
            Handler.do_GET(self.handler("/admin/api/v1/fleet"))


class FleetQueryBudgetTests(unittest.TestCase):
    def setUp(self):
        self.store = BrainStore("postgresql://synthetic.invalid/recall")
        self.connection = Mock()
        self.connection.execute.return_value.fetchall.return_value = []
        self.now = 100.0
        self.acquire_seconds = 0

        @contextmanager
        def connect():
            self.now += self.acquire_seconds
            yield self.connection

        self.store.connect = connect

    def fleet(self):
        with patch("recall_server.db.time.monotonic", side_effect=lambda: self.now):
            return self.store.fleet_status(["tenant:company"])

    def test_transaction_local_budget_accounts_for_connection_acquisition(self):
        self.acquire_seconds = 2
        self.assertEqual(self.fleet(), [])
        calls = self.connection.execute.call_args_list
        self.assertEqual(calls[0].args,
                         ("SELECT set_config('statement_timeout', %s, true)", ("3000ms",)))
        self.assertEqual(calls[1].args[1], (["tenant:company"],))

    def test_expired_acquisition_does_not_start_statistics_query(self):
        self.acquire_seconds = 5.01
        with self.assertRaises(SearchDeadlineExceeded):
            self.fleet()
        self.connection.execute.assert_not_called()

    def test_cancelled_query_rolls_back_before_returning_controlled_failure(self):
        def execute(sql, params):
            if "set_config" in sql:
                return Mock()
            raise psycopg.errors.QueryCanceled("private SQL")
        self.connection.execute.side_effect = execute
        with self.assertRaises(SearchDeadlineExceeded):
            self.fleet()
        self.connection.rollback.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
