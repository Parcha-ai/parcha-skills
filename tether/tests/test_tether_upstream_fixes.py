from __future__ import annotations

import asyncio
import importlib.util
import logging
import os
import pathlib
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"
PLUGIN_PATH = ROOT / "runtime" / "plugin" / "__init__.py"

TEAM = "T11111111"
CHANNEL = "D11111111"


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_runtime(home: pathlib.Path):
    env = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
    }
    with mock.patch.dict(os.environ, env, clear=False):
        name = f"bridge_runtime_upstream_{id(home)}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


class TeamIdBackfillTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.db_path = self.home / ".hermes" / "bridges.db"
        self.store = self.runtime.Store(self.db_path)
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)
        self.temp.cleanup()

    def _insert_bridge(self, bridge_id, team_id, channel_id, thread_ts):
        with self.store.connect() as db:
            db.execute(
                """
                INSERT INTO bridges(
                  bridge_id, source_kind, source_json, owner_user_id,
                  team_id, channel_id, thread_ts, idempotency_key, status
                )
                VALUES (?, 'headless_run', ?,
                        'U12345678', ?, ?, ?, ?, 'active')
                ON CONFLICT(bridge_id) DO UPDATE SET
                  team_id=excluded.team_id,
                  channel_id=excluded.channel_id,
                  thread_ts=excluded.thread_ts
                """,
                (
                    bridge_id,
                    f'{{"run_id":"{bridge_id}"}}',
                    team_id, channel_id, thread_ts, bridge_id,
                ),
            )

    def _insert_participation(self, team_id, channel_id, thread_ts):
        with self.store.connect() as db:
            db.execute(
                """
                INSERT INTO thread_participation(team_id, channel_id, thread_ts)
                VALUES (?, ?, ?)
                ON CONFLICT(team_id, channel_id, thread_ts) DO NOTHING
                """,
                (team_id, channel_id, thread_ts),
            )

    def _insert_ingress(self, event_id, team_id, channel_id, thread_ts):
        with self.store.connect() as db:
            db.execute(
                """
                INSERT INTO thread_ingress(
                  event_id, team_id, channel_id, thread_ts
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                  team_id=excluded.team_id
                """,
                (event_id, team_id, channel_id, thread_ts),
            )

    def _insert_poll_state(self, team_id, channel_id, thread_ts, cursor):
        with self.store.connect() as db:
            db.execute(
                """
                INSERT INTO slack_reply_poll_state(
                  team_id, channel_id, thread_ts, page_oldest
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(team_id, channel_id, thread_ts) DO UPDATE SET
                  page_oldest=excluded.page_oldest
                """,
                (team_id, channel_id, thread_ts, cursor),
            )

    def _bridge_status(self, bridge_id):
        with sqlite3.connect(self.db_path) as db:
            row = db.execute(
                "SELECT status, binding_error_code FROM bridges WHERE bridge_id=?",
                (bridge_id,),
            ).fetchone()
        return row[0], row[1]

    def test_bridge_collision_on_backfill_closes_legacy_bridge(self):
        self._insert_bridge("legacy", "", CHANNEL, "1.1")
        self._insert_bridge("real", TEAM, CHANNEL, "1.1")

        reopened = self.runtime.Store(self.db_path)
        del reopened

        legacy_status, legacy_error = self._bridge_status("legacy")
        self.assertEqual(legacy_status, "closed")
        self.assertEqual(legacy_error, "team_backfill_conflict")

        real_status, real_error = self._bridge_status("real")
        self.assertEqual(real_status, "active")
        self.assertEqual(real_error, None)

    def _team_ids(self, table):
        with sqlite3.connect(self.db_path) as db:
            return {
                row[0]
                for row in db.execute(f"SELECT DISTINCT team_id FROM {table}").fetchall()
            }

    def _row_count(self, table):
        with sqlite3.connect(self.db_path) as db:
            return db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def test_single_team_backfills_all_three_tables(self):
        self._insert_bridge("b1", "", CHANNEL, "1.1")
        self._insert_bridge("b2", TEAM, CHANNEL, "1.2")
        self._insert_participation("", CHANNEL, "1.1")
        self._insert_participation(TEAM, CHANNEL, "1.1")
        self._insert_participation("", CHANNEL, "1.3")
        self._insert_ingress("e1", "", CHANNEL, "1.1")
        self._insert_ingress("e2", "", CHANNEL, "1.2")
        self._insert_poll_state("", CHANNEL, "1.1", "old-cursor")
        self._insert_poll_state(TEAM, CHANNEL, "1.1", "new-cursor")
        self._insert_poll_state("", CHANNEL, "1.4", "orphan-cursor")

        with sqlite3.connect(self.db_path) as db:
            db.execute(
                """
                UPDATE thread_participation
                SET updated_at='2026-07-01 00:00:00'
                WHERE team_id='' AND channel_id=? AND thread_ts='1.1'
                """,
                (CHANNEL,),
            )
            db.execute(
                """
                UPDATE thread_participation
                SET updated_at='2026-08-01 00:00:00'
                WHERE team_id=? AND channel_id=? AND thread_ts='1.1'
                """,
                (TEAM, CHANNEL),
            )

        reopened = self.runtime.Store(self.db_path)
        del reopened

        for table in ("bridges", "thread_ingress"):
            self.assertEqual(self._team_ids(table), {TEAM}, f"{table}")

        self.assertEqual(self._team_ids("thread_participation"), {TEAM})
        self.assertEqual(self._row_count("thread_participation"), 2)

        with sqlite3.connect(self.db_path) as db:
            updated_at = db.execute(
                """
                SELECT updated_at FROM thread_participation
                WHERE team_id=? AND channel_id=? AND thread_ts='1.1'
                """,
                (TEAM, CHANNEL),
            ).fetchone()[0]
        self.assertEqual(updated_at, "2026-08-01 00:00:00")

        self.assertEqual(self._team_ids("slack_reply_poll_state"), {TEAM})
        self.assertEqual(self._row_count("slack_reply_poll_state"), 2)
        with sqlite3.connect(self.db_path) as db:
            page_oldest = db.execute(
                """
                SELECT page_oldest FROM slack_reply_poll_state
                WHERE team_id=? AND channel_id=? AND thread_ts='1.1'
                """,
                (TEAM, CHANNEL),
            ).fetchone()[0]
        self.assertEqual(page_oldest, "new-cursor")

    def test_multiple_teams_leaves_empty_team_id_unchanged(self):
        self._insert_bridge("b1", "", CHANNEL, "1.1")
        self._insert_bridge("b2", "T2", CHANNEL, "1.2")
        self._insert_bridge("b3", "T3", CHANNEL, "1.3")
        self._insert_participation("", CHANNEL, "1.1")
        self._insert_ingress("e1", "", CHANNEL, "1.1")

        reopened = self.runtime.Store(self.db_path)
        del reopened

        self.assertIn("", self._team_ids("bridges"))
        self.assertIn("", self._team_ids("thread_participation"))
        self.assertIn("", self._team_ids("thread_ingress"))

    def test_zero_teams_leaves_empty_team_id_unchanged(self):
        self._insert_bridge("b1", "", CHANNEL, "1.1")
        self._insert_participation("", CHANNEL, "1.1")
        self._insert_ingress("e1", "", CHANNEL, "1.1")

        reopened = self.runtime.Store(self.db_path)
        del reopened

        self.assertEqual(self._team_ids("bridges"), {""})
        self.assertEqual(self._team_ids("thread_participation"), {""})
        self.assertEqual(self._team_ids("thread_ingress"), {""})


class FailureReasonTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.env_patch = mock.patch.dict(
            os.environ,
            {
                "HOME": str(self.home),
                "HERMES_HOME": str(self.home / ".hermes"),
                "XDG_DATA_HOME": str(self.home / ".local" / "share"),
                "XDG_CONFIG_HOME": str(self.home / ".config"),
            },
            clear=False,
        )
        self.env_patch.start()
        self.runtime = load_runtime(self.home)
        sys.modules["bridge_runtime"] = self.runtime
        for name in ("tether_routing", "tether_hermes_compat", "tether_slack_protocol"):
            sys.modules.pop(name, None)
        self.plugin = load_module(
            f"tether_plugin_upstream_{id(self)}", PLUGIN_PATH,
        )
        self.plugin.store = self.runtime.Store()
        self.plugin.state.store = self.plugin.store
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)
        self.env_patch.stop()
        self.temp.cleanup()

    def test_credential_helper_misconfiguration(self):
        exc = Exception(
            "credential helper executable failed ownership or mode validation"
        )
        self.assertIn(
            "credential helper is misconfigured",
            self.plugin._failure_reason(exc),
        )

    def test_authentication_failure_unchanged(self):
        exc = Exception("model authentication failed")
        self.assertEqual(
            self.plugin._failure_reason(exc),
            "the native session credential could not be obtained or authenticated",
        )

    def test_401_still_maps_to_auth(self):
        exc = Exception("received 401 from provider")
        self.assertEqual(
            self.plugin._failure_reason(exc),
            "the native session credential could not be obtained or authenticated",
        )


class PollEmptyTeamIdTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.env_patch = mock.patch.dict(
            os.environ,
            {
                "HOME": str(self.home),
                "HERMES_HOME": str(self.home / ".hermes"),
                "XDG_DATA_HOME": str(self.home / ".local" / "share"),
                "XDG_CONFIG_HOME": str(self.home / ".config"),
                "TETHER_REPLY_RECOVERY_HOURS": "24",
                "TETHER_REPLY_POLL_BATCH": "10",
                "TETHER_REPLY_POLL_MAX_PAGES": "25",
            },
            clear=False,
        )
        self.env_patch.start()
        self.runtime = load_runtime(self.home)
        self.db_path = self.home / ".hermes" / "bridges.db"
        self.store = self.runtime.Store(self.db_path)
        sys.modules["bridge_runtime"] = self.runtime
        for name in ("tether_routing", "tether_hermes_compat", "tether_slack_protocol"):
            sys.modules.pop(name, None)
        self.plugin = load_module(
            f"tether_poll_empty_team_{id(self)}", PLUGIN_PATH,
        )
        self.plugin.store = self.store
        self.plugin.state.store = self.store

    def tearDown(self):
        self.env_patch.stop()
        self.temp.cleanup()

    def _make_bridge(self, team_id):
        bridge = self.store.create({
            "source_kind": "headless_run",
            "source": {"run_id": f"r-{team_id or 'empty'}-{id(self)}", "cwd": "/tmp"},
            "owner_user_id": "U12345678",
            "team_id": team_id,
            "channel_id": CHANNEL,
            "idempotency_key": f"bridge-{team_id or 'empty'}-{id(self)}-{id(self)}",
        })
        return self.store.bind(bridge.bridge_id, "1785000000.000001")

    def test_poll_skips_threads_without_team_id(self):
        self._make_bridge("")
        with self.store.connect() as db:
            db.execute(
                """
                UPDATE slack_reconciliation_limits
                SET next_allowed_at=CURRENT_TIMESTAMP
                """
            )

        class Client:
            calls = []

            async def conversations_replies(self, **_kwargs):
                Client.calls.append("replies")
                return {"messages": [], "response_metadata": {"next_cursor": ""}}

            async def conversations_history(self, **_kwargs):
                return {"ok": True, "messages": []}

            async def conversations_join(self, **_kwargs):
                return {"ok": True}

        class Adapter:
            def __init__(self):
                self.client = Client()

            def _get_client(self, _channel, team_id=None):
                return self.client

            async def _handle_slack_message(self, _event):
                pass

        with self.assertLogs(self.plugin.log.name, level="WARNING") as log_ctx:
            result = asyncio.run(
                self.plugin._poll_recent_replies(Adapter())
            )

        self.assertEqual(result, 0)
        self.assertEqual(Client.calls, [])
        self.assertTrue(
            any(
                "skipped 1 thread(s) without a workspace identity" in rec
                for rec in log_ctx.output
            ),
            f"expected warning in {log_ctx.output}",
        )

    def test_poll_uses_explicit_configured_team_for_legacy_thread(self):
        self._make_bridge("")
        config = self.home / ".config" / "tether" / "config.toml"
        config.parent.mkdir(parents=True)
        config.write_text(f'team_id = "{TEAM}"\n')
        with self.store.connect() as db:
            db.execute(
                """
                UPDATE slack_reconciliation_limits
                SET next_allowed_at=CURRENT_TIMESTAMP
                """
            )

        class Client:
            calls = []

            async def conversations_replies(self, **kwargs):
                Client.calls.append(kwargs)
                return {"messages": [], "response_metadata": {"next_cursor": ""}}

            async def conversations_history(self, **_kwargs):
                return {"ok": True, "messages": []}

            async def conversations_join(self, **_kwargs):
                return {"ok": True}

        class Adapter:
            def __init__(self):
                self.client = Client()

            def _get_client(self, _channel, team_id=None):
                self.requested_team_id = team_id
                return self.client

            async def _handle_slack_message(self, _event):
                pass

        adapter = Adapter()
        result = asyncio.run(self.plugin._poll_recent_replies(adapter))

        self.assertEqual(result, 0)
        self.assertEqual(adapter.requested_team_id, TEAM)
        self.assertEqual(len(Client.calls), 1)

    def test_hermes_egress_uses_explicit_configured_team(self):
        config = self.home / ".config" / "tether" / "config.toml"
        config.parent.mkdir(parents=True)
        config.write_text(f'team_id = "{TEAM}"\n')
        adapter = types.SimpleNamespace(_channel_team={})

        self.assertEqual(
            self.plugin._hermes_workspace_id(adapter, CHANNEL, {}),
            TEAM,
        )

    def test_poll_includes_hermes_compat_error_detail(self):
        self._make_bridge(TEAM)
        with self.store.connect() as db:
            db.execute(
                """
                UPDATE slack_reconciliation_limits
                SET next_allowed_at=CURRENT_TIMESTAMP
                """
            )

        compat_error = self.plugin.hermes_compat.HermesCompatibilityError(
            "team_id is empty"
        )

        class Client:
            pass

        class Adapter:
            def __init__(self):
                self.client = Client()

            def _get_client(self, _channel, team_id=None):
                raise compat_error

            async def _handle_slack_message(self, _event):
                pass

        # The detail must reach the log; a total-failure cycle no longer
        # raises, because the caller's queue drain must still run.
        with self.assertLogs(self.plugin.log.name, level="WARNING") as log_ctx:
            asyncio.run(self.plugin._poll_recent_replies(Adapter()))

        self.assertTrue(
            any(
                "team_id is empty" in rec
                for rec in log_ctx.output
            ),
            f"expected detail in {log_ctx.output}",
        )


class DrainBridgeNoticeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.env_patch = mock.patch.dict(
            os.environ,
            {
                "HOME": str(self.home),
                "HERMES_HOME": str(self.home / ".hermes"),
                "XDG_DATA_HOME": str(self.home / ".local" / "share"),
                "XDG_CONFIG_HOME": str(self.home / ".config"),
            },
            clear=False,
        )
        self.env_patch.start()
        self.runtime = load_runtime(self.home)
        self.store = self.runtime.Store(self.home / "bridges.db")
        sys.modules["bridge_runtime"] = self.runtime
        for name in ("tether_routing", "tether_hermes_compat", "tether_slack_protocol"):
            sys.modules.pop(name, None)
        self.plugin = load_module(
            f"tether_drain_notice_{id(self)}", PLUGIN_PATH,
        )
        self.plugin.store = self.store
        self.plugin.state.store = self.store
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)
        self.env_patch.stop()
        self.temp.cleanup()

    def _make_bridge(self):
        bridge = self.store.create({
            "source_kind": "claude_session",
            "source": {"session_id": f"claude-{id(self)}", "cwd": "/tmp/project"},
            "owner_user_id": "U12345678",
            "team_id": TEAM,
            "channel_id": CHANNEL,
            "idempotency_key": f"drain-notice-{id(self)}-{id(self)}",
        })
        return self.store.bind(bridge.bridge_id, "1785000000.000001")

    def test_drain_bridge_posts_control_notice_on_failure(self):
        bridge = self._make_bridge()
        event_id = "1785000001.000001"
        self.assertTrue(
            self.store.enqueue_event(event_id, bridge.bridge_id, "continue")
        )

        class Platform:
            value = "slack"

        platform = Platform()
        gateway = types.SimpleNamespace(
            adapters={platform: types.SimpleNamespace()}
        )

        notices = []

        async def capture_notice(bridge_ref, *, idempotency_key, text):
            notices.append({
                "team_id": bridge_ref.team_id,
                "idempotency_key": idempotency_key,
                "text": text,
            })

        error = self.runtime.NativeContinuationError(
            "credential helper executable failed ownership or mode validation",
            code="credential_helper_rejected",
        )

        with mock.patch.object(
            self.plugin, "continue_native", side_effect=error
        ), mock.patch.object(
            self.plugin, "_post_control_notice", side_effect=capture_notice
        ):
            asyncio.run(
                self.plugin._drain_bridge(bridge.bridge_id, gateway, platform)
            )

        self.assertEqual(len(notices), 1)
        self.assertIn("control:reply-failed:", notices[0]["idempotency_key"])
        self.assertIn("credential helper is misconfigured", notices[0]["text"])
        self.assertIn("Send another reply to retry.", notices[0]["text"])

    def test_cancelled_drain_does_not_post_notice(self):
        bridge = self._make_bridge()
        event_id = "1785000001.000001"
        self.assertTrue(
            self.store.enqueue_event(event_id, bridge.bridge_id, "continue")
        )

        class Platform:
            value = "slack"

        platform = Platform()
        gateway = types.SimpleNamespace(
            adapters={platform: types.SimpleNamespace()}
        )

        notices = []

        async def capture_notice(bridge_ref, *, idempotency_key, text):
            notices.append({
                "team_id": bridge_ref.team_id,
                "idempotency_key": idempotency_key,
                "text": text,
            })

        error = self.runtime.NativeContinuationError(
            "cancelled by operator",
            code="operator_cancelled",
        )

        with mock.patch.object(
            self.plugin, "continue_native", side_effect=error
        ), mock.patch.object(
            self.plugin, "_post_control_notice", side_effect=capture_notice
        ):
            asyncio.run(
                self.plugin._drain_bridge(bridge.bridge_id, gateway, platform)
            )

        self.assertEqual(len(notices), 0)

    def test_set_cancellation_event_does_not_post_notice(self):
        bridge = self._make_bridge()
        event_id = "1785000001.000001"
        self.assertTrue(
            self.store.enqueue_event(event_id, bridge.bridge_id, "continue")
        )

        class Platform:
            value = "slack"

        platform = Platform()
        gateway = types.SimpleNamespace(
            adapters={platform: types.SimpleNamespace()}
        )

        notices = []

        async def capture_notice(bridge_ref, *, idempotency_key, text):
            notices.append({
                "team_id": bridge_ref.team_id,
                "idempotency_key": idempotency_key,
                "text": text,
            })

        def continue_native(*_args, **_kwargs):
            self.plugin.state.active_cancellations[
                bridge.bridge_id
            ].set()
            raise RuntimeError("something else")

        with mock.patch.object(
            self.plugin, "continue_native", side_effect=continue_native
        ), mock.patch.object(
            self.plugin, "_post_control_notice", side_effect=capture_notice
        ):
            asyncio.run(
                self.plugin._drain_bridge(bridge.bridge_id, gateway, platform)
            )

        self.assertEqual(len(notices), 0)


if __name__ == "__main__":
    unittest.main()
