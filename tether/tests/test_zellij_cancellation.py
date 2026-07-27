import hashlib
import importlib.util
import json
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
        name = f"zellij_cancellation_test_{id(home)}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


def identity() -> str:
    payload = {
        "agent": "codex",
        "boot": "00000000-0000-4000-8000-000000000001",
        "exe": "1:2",
        "exe_path": hashlib.sha256(
            b"/opt/codex/bin/codex"
        ).hexdigest()[:16],
        "pane": "7",
        "pid": 200,
        "session": "work",
        "start": "20000",
        "tty": "34823",
    }
    return "linux-proc-v2:" + json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )


class ZellijCancellationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")
        exact = identity()
        request = {
            "source_kind": "codex_session",
            "source": {
                "session_id": "codex-session",
                "cwd": "/tmp/project",
                "zellij_session": "work",
                "zellij_pane_id": "7",
                "pane_agent": "codex",
                "pane_command_hash": hashlib.sha256(
                    exact.encode()
                ).hexdigest(),
                "process_identity": exact,
            },
            "owner_user_id": "U12345678",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "zellij-cancel",
        }
        created = self.store.create(request)
        self.bridge = self.store.bind(created.bridge_id, "123.456")
        self.attempt_id = self._attempt("1785000000.000001")

    def tearDown(self):
        self.temp.cleanup()

    def _attempt(self, event_id: str) -> str:
        self.assertTrue(
            self.store.enqueue_event(event_id, self.bridge.bridge_id, "continue")
        )
        items = self.store.claim_event_batch(self.bridge.bridge_id)
        attempt_id = self.runtime.delivery_attempt_id(
            self.bridge.bridge_id,
            [item["event_id"] for item in items],
            self.bridge.binding_generation,
        )
        self.assertTrue(
            self.store.prepare_delivery_attempt(
                [item["event_id"] for item in items],
                self.bridge.bridge_id,
                self.bridge.binding_generation,
                attempt_id,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_submitting(
                attempt_id,
                self.bridge.bridge_id,
                self.bridge.binding_generation,
            )
        )
        self.assertTrue(
            self.store.mark_attempt_awaiting_ack(
                attempt_id,
                self.bridge.bridge_id,
                self.bridge.binding_generation,
            )
        )
        return attempt_id

    def test_verified_interrupt_cancels_exact_attempt(self):
        active = self.store.active_zellij_attempt(self.bridge.bridge_id)
        self.assertEqual(active["attempt_id"], self.attempt_id)
        current = {
            "process_identity": identity(),
            "session_name": "work",
            "pane_id": "7",
            "cwd": "/tmp/project",
            "pane_agent": "codex",
            "pane_command_hash": hashlib.sha256(identity().encode()).hexdigest(),
        }
        with mock.patch.object(
            self.runtime,
            "zellij_pane_identity",
            return_value=current,
        ), mock.patch.object(
            self.runtime,
            "_resolve_executable",
            return_value="/usr/bin/zellij",
        ), mock.patch.object(
            self.runtime.subprocess,
            "run",
        ) as run:
            self.runtime.interrupt_zellij(self.bridge)
        run.assert_called_once_with(
            [
                "/usr/bin/zellij",
                "--session",
                "work",
                "action",
                "send-keys",
                "--pane-id",
                "terminal_7",
                "Ctrl c",
            ],
            check=True,
            timeout=10,
        )

        cancelled = self.store.cancel_zellij_attempt(
            self.attempt_id,
            self.bridge.bridge_id,
            self.bridge.binding_generation,
        )
        self.assertEqual(cancelled, 1)
        self.assertIsNone(
            self.store.active_zellij_attempt(self.bridge.bridge_id)
        )
        with self.store.connect() as database:
            attempt = database.execute(
                "SELECT state,error_code FROM bridge_attempts WHERE attempt_id=?",
                (self.attempt_id,),
            ).fetchone()
            event = database.execute(
                "SELECT state,error FROM bridge_events WHERE attempt_id=?",
                (self.attempt_id,),
            ).fetchone()
        self.assertEqual(tuple(attempt), ("cancelled", "operator_cancelled"))
        self.assertEqual(tuple(event), ("failed", "operator_cancelled"))

        next_attempt = self._attempt("1785000000.000002")
        self.assertNotEqual(next_attempt, self.attempt_id)

    def test_identity_change_blocks_interrupt_before_io(self):
        changed = {
            "process_identity": identity().replace('"pid":200', '"pid":201'),
        }
        with mock.patch.object(
            self.runtime,
            "zellij_pane_identity",
            return_value=changed,
        ), mock.patch.object(
            self.runtime.subprocess,
            "run",
        ) as run, self.assertRaisesRegex(
            self.runtime.NativeContinuationError,
            "different process",
        ):
            self.runtime.interrupt_zellij(self.bridge)
        run.assert_not_called()
        self.assertEqual(
            self.store.attempt_state(
                self.attempt_id,
                self.bridge.bridge_id,
            ),
            "awaiting_ack",
        )

    def test_completed_reply_wins_cancellation_race(self):
        self.assertEqual(
            self.store.acknowledge_attempt(
                self.attempt_id,
                self.bridge.bridge_id,
                ack_kind="reply",
            ),
            1,
        )
        self.assertIsNone(
            self.store.active_zellij_attempt(self.bridge.bridge_id)
        )
        self.assertEqual(
            self.store.cancel_zellij_attempt(
                self.attempt_id,
                self.bridge.bridge_id,
                self.bridge.binding_generation,
            ),
            0,
        )


if __name__ == "__main__":
    unittest.main()
