import importlib.util
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"


def load_runtime(home: pathlib.Path):
    env = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
    }
    with mock.patch.dict(os.environ, env, clear=False):
        name = f"retention_safety_test_{id(home)}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


class RetentionSafetyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")
        request = {
            "source_kind": "headless_run",
            "source": {"run_id": "retention", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "retention",
        }
        created = self.store.create(request)
        self.bridge = self.store.bind(created.bridge_id, "123.456")

    def tearDown(self):
        self.temp.cleanup()

    def _ingress(self, suffix: str, state: str) -> str:
        event_id = f"slack:T12345678:C12345678:{suffix}"
        claim = self.store.claim_thread_ingress(
            event_id,
            self.bridge.team_id,
            self.bridge.channel_id,
            self.bridge.thread_ts,
            route_action="hermes",
            writer_id="hermes",
            bridge_id=self.bridge.bridge_id,
            binding_generation=self.bridge.binding_generation,
            payload={"text": suffix},
        )
        self.assertEqual(claim["status"], "claimed")
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE thread_ingress
                SET state=?,lease_id=NULL,lease_owner=NULL,
                    lease_expires_at=NULL,
                    updated_at=datetime('now','-10 days')
                WHERE event_id=?
                """,
                (state, event_id),
            )
        return event_id

    def test_prune_keeps_every_unresolved_ingress_state(self):
        unresolved = {
            self._ingress("1785000000.000001", "pending"),
            self._ingress("1785000000.000002", "processing"),
            self._ingress("1785000000.000003", "dispatched"),
            self._ingress("1785000000.000004", "uncertain"),
        }
        terminal = {
            self._ingress("1785000000.000005", "completed"),
            self._ingress("1785000000.000006", "transferred"),
            self._ingress("1785000000.000007", "cancelled"),
        }
        counts = self.store.prune(retention_days=1)
        self.assertEqual(counts["thread_ingress"], len(terminal))
        with self.store.connect() as database:
            remaining = {
                str(row[0])
                for row in database.execute(
                    "SELECT event_id FROM thread_ingress"
                )
            }
        self.assertEqual(remaining, unresolved)

    def test_active_binding_participation_is_not_pruned(self):
        self.store.mark_participation(
            self.bridge.team_id,
            self.bridge.channel_id,
            self.bridge.thread_ts,
        )
        with self.store.connect() as database:
            database.execute(
                """
                UPDATE thread_participation
                SET updated_at=datetime('now','-10 days')
                """
            )
        counts = self.store.prune(retention_days=1)
        self.assertEqual(counts["thread_participation"], 0)
        self.assertTrue(
            self.store.participates(
                self.bridge.team_id,
                self.bridge.channel_id,
                self.bridge.thread_ts,
            )
        )

    def test_retention_must_exceed_slack_recovery_horizon(self):
        config = self.home / "config.toml"
        config.write_text("retention_days = 7\n", encoding="utf-8")
        config.chmod(0o600)
        with mock.patch.dict(
            os.environ,
            {"TETHER_REPLY_RECOVERY_HOURS": "168"},
            clear=False,
        ), self.assertRaisesRegex(
            ValueError,
            "between 8 and 3650",
        ):
            self.runtime.load_config(config)


if __name__ == "__main__":
    unittest.main()
