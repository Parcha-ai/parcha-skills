from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import sqlite3
import sys
import tempfile
import types
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
PLUGIN_ROOT = ROOT / "runtime" / "plugin_next"


def load_plugin(name: str):
    # Mirror Hermes's directory-plugin loader exactly: spec_from_file_location
    # on __init__.py with submodule_search_locations.
    for stale in [key for key in sys.modules if key.startswith(name)]:
        sys.modules.pop(stale, None)
    spec = importlib.util.spec_from_file_location(
        name,
        PLUGIN_ROOT / "__init__.py",
        submodule_search_locations=[str(PLUGIN_ROOT)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class FakeSource:
    def __init__(self, **kwargs):
        self.platform = types.SimpleNamespace(value=kwargs.pop("platform", "slack"))
        self.chat_id = kwargs.pop("chat_id", "C1")
        self.chat_type = kwargs.pop("chat_type", "thread")
        self.thread_id = kwargs.pop("thread_id", "100.1")
        self.parent_chat_id = kwargs.pop("parent_chat_id", "C1")
        self.user_id = kwargs.pop("user_id", "U12345678")
        self.is_bot = kwargs.pop("is_bot", False)
        self.scope_id = kwargs.pop("scope_id", "T12345678")
        self.guild_id = None
        self.message_id = kwargs.pop("message_id", "170.500")
        assert not kwargs, kwargs


class FakeEvent:
    def __init__(self, **kwargs):
        self.message_id = kwargs.pop("event_message_id", "170.500")
        self.source = FakeSource(**kwargs)
        self.text = "hola"


class FloorCtx:
    """The Hermes v2026.7.20 context surface: hooks + CLI, nothing newer."""

    def __init__(self):
        self.hooks: dict[str, object] = {}
        self.cli_commands: dict[str, object] = {}

    def register_hook(self, name, callback):
        self.hooks[name] = callback

    def register_cli_command(self, name, help, setup_fn, handler_fn=None, description=""):
        self.cli_commands[name] = (setup_fn, handler_fn)


class RichCtx(FloorCtx):
    def __init__(self, config=None):
        super().__init__()
        self.config = config or {}
        self.unload_callbacks = []

    def get_config(self, key, default=None):
        return self.config.get(key, default)

    def on_unload(self, callback):
        self.unload_callbacks.append(callback)


class PluginEnvironment(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tether-plugin-next-")
        self.home = pathlib.Path(self.temp.name)
        os.chmod(self.home, 0o700)
        (self.home / ".hermes").mkdir(mode=0o700)
        (self.home / ".config" / "tether").mkdir(parents=True, mode=0o700)
        self.env = mock.patch.dict(os.environ, {
            "HOME": str(self.home),
            "HERMES_HOME": str(self.home / ".hermes"),
            "XDG_CONFIG_HOME": str(self.home / ".config"),
        }, clear=False)
        self.env.start()
        self.write_config()
        self.module = load_plugin(f"tether_plugin_next_{id(self)}")

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def write_config(self, **overrides):
        values = {
            "team_id": "T12345678",
            "allowed_users": ["U12345678"],
        }
        values.update(overrides)
        lines = []
        for key, value in values.items():
            if isinstance(value, list):
                rendered = "[" + ",".join(f'"{item}"' for item in value) + "]"
            elif isinstance(value, str):
                rendered = f'"{value}"'
            else:
                rendered = str(value)
            lines.append(f"{key} = {rendered}")
        path = self.home / ".config" / "tether" / "config.toml"
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        path.chmod(0o600)

    def seed_v17_binding(self, channel="C1", thread="100.1"):
        db = self.home / ".hermes" / "bridges.db"
        connection = sqlite3.connect(db)
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS bridges("
                "bridge_id TEXT PRIMARY KEY,channel_id TEXT,thread_ts TEXT)"
            )
            connection.execute(
                "INSERT OR IGNORE INTO bridges VALUES(?,?,?)",
                (f"brg-{channel}-{thread}", channel, thread),
            )
            connection.commit()
        finally:
            connection.close()
        os.chmod(db, 0o600)

    def journal_rows(self):
        db = self.home / ".hermes" / "plugin-data" / "tether" / "shadow.db"
        connection = sqlite3.connect(db)
        connection.row_factory = sqlite3.Row
        try:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM shadow_events ORDER BY event_key"
            )]
        finally:
            connection.close()


class AdmissionPolicyTest(PluginEnvironment):
    def decide(self, **overrides):
        arguments = dict(
            platform="slack",
            workspace="T12345678",
            channel="C1",
            thread="100.1",
            actor="U12345678",
            actor_is_bot=False,
            message_id="170.5",
            settings=self.module.load_settings(),
            bound_threads={("C1", "100.1")},
        )
        arguments.update(overrides)
        return self.module.admission.evaluate(**arguments)

    def test_authorized_owner_on_bound_thread_is_admitted(self):
        decision = self.decide()
        self.assertEqual(decision["verdict"], "admit")
        self.assertIsNotNone(decision["binding_ref"])

    def test_every_provenance_failure_on_a_bound_thread_denies(self):
        cases = {
            "wrong_workspace": {"workspace": "T99999999"},
            "workspace_unknown": {"workspace": None},
            "untrusted_bot": {"actor_is_bot": True},
            "unauthorized_user": {"actor": "U_ATTACKER"},
            "unauthorized_user_empty": {"actor": None},
            "event_identity_missing": {"message_id": None},
        }
        for name, overrides in cases.items():
            with self.subTest(name):
                decision = self.decide(**overrides)
                self.assertEqual(decision["verdict"], "deny", decision)

    def test_unbound_thread_and_foreign_platform_are_not_ours(self):
        self.assertEqual(
            self.decide(thread="999.9")["verdict"], "not_ours"
        )
        self.assertEqual(
            self.decide(platform="discord")["verdict"], "not_ours"
        )

    def test_incomplete_security_domain_is_unconfigured_not_admit(self):
        self.write_config(allowed_users=[])
        decision = self.decide(settings=self.module.load_settings())
        self.assertEqual(decision["verdict"], "unconfigured")


class ShadowHookTest(PluginEnvironment):
    def register(self, ctx=None):
        context = ctx or FloorCtx()
        self.module.register(context)
        return context

    def dispatch(self, ctx, **event_kwargs):
        hook = ctx.hooks["pre_gateway_dispatch"]
        return hook(event=FakeEvent(**event_kwargs))

    def test_floor_context_registers_and_shadow_never_directs(self):
        self.seed_v17_binding()
        ctx = self.register()
        self.assertIn("pre_gateway_dispatch", ctx.hooks)
        self.assertIn("tether", ctx.cli_commands)
        result = self.dispatch(ctx)
        self.assertIsNone(result)
        rows = self.journal_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["verdict"], "admit")
        self.assertEqual(rows[0]["actor"], "U12345678")

    def test_replayed_event_key_is_journaled_once(self):
        self.seed_v17_binding()
        ctx = self.register()
        self.assertIsNone(self.dispatch(ctx))
        self.assertIsNone(self.dispatch(ctx))
        self.assertEqual(len(self.journal_rows()), 1)

    def test_denials_are_recorded_but_never_influence_dispatch(self):
        self.seed_v17_binding()
        ctx = self.register()
        result = self.dispatch(ctx, user_id="U_ATTACKER", event_message_id="171.0")
        self.assertIsNone(result)
        rows = self.journal_rows()
        self.assertEqual(rows[0]["verdict"], "deny")
        self.assertEqual(rows[0]["reason"], "unauthorized_user")

    def test_non_slack_events_are_untouched_and_unjournaled(self):
        ctx = self.register()
        self.assertIsNone(self.dispatch(ctx, platform="telegram"))
        self.assertEqual(self.journal_rows(), [])

    def test_shadow_mode_false_is_refused_loudly_and_stays_shadow(self):
        self.seed_v17_binding()
        ctx = RichCtx(config={"shadow_mode": False})
        with self.assertLogs("tether.plugin", level="WARNING") as logs:
            self.register(ctx)
        self.assertTrue(any("staying in shadow" in line for line in logs.output))
        self.assertIsNone(self.dispatch(ctx))

    def test_unconfigured_domain_claims_nothing_and_records_why(self):
        self.write_config(team_id="")
        module = load_plugin(f"tether_plugin_next_unconf_{id(self)}")
        self.seed_v17_binding()
        ctx = FloorCtx()
        module.register(ctx)
        self.assertIsNone(ctx.hooks["pre_gateway_dispatch"](event=FakeEvent()))
        rows = self.journal_rows()
        self.assertEqual(rows[0]["verdict"], "unconfigured")

    def test_hook_failure_is_swallowed_and_gateway_flow_untouched(self):
        ctx = self.register()
        broken = types.SimpleNamespace(source=None, message_id=None)
        self.assertIsNone(ctx.hooks["pre_gateway_dispatch"](event=broken))

    def test_missing_bridges_database_fails_closed_to_not_ours(self):
        ctx = self.register()
        self.assertIsNone(self.dispatch(ctx))
        rows = self.journal_rows()
        self.assertEqual(rows[0]["verdict"], "not_ours")
        self.assertEqual(rows[0]["reason"], "thread_not_bound")

    def test_v18_thread_bindings_are_read_when_schema_is_migrated(self):
        db = self.home / ".hermes" / "bridges.db"
        connection = sqlite3.connect(db)
        try:
            connection.execute(
                "CREATE TABLE thread_bindings("
                "binding_id TEXT PRIMARY KEY,channel_id TEXT,thread_ts TEXT,"
                "state TEXT)"
            )
            connection.execute(
                "INSERT INTO thread_bindings VALUES('bnd-1','C1','100.1','active')"
            )
            connection.execute("PRAGMA user_version=18")
            connection.commit()
        finally:
            connection.close()
        os.chmod(db, 0o600)
        ctx = self.register()
        self.assertIsNone(self.dispatch(ctx))
        self.assertEqual(self.journal_rows()[0]["verdict"], "admit")

    def test_journal_and_directory_permissions_are_private(self):
        self.seed_v17_binding()
        ctx = self.register()
        self.dispatch(ctx)
        data_dir = self.home / ".hermes" / "plugin-data" / "tether"
        self.assertEqual(oct(data_dir.stat().st_mode & 0o777), "0o700")
        self.assertEqual(
            oct((data_dir / "shadow.db").stat().st_mode & 0o777), "0o600"
        )

    def test_cli_status_reports_verdict_counts(self):
        self.seed_v17_binding()
        ctx = self.register()
        self.dispatch(ctx)
        self.dispatch(ctx, user_id="U_ATTACKER", event_message_id="172.0")
        _setup, handler = ctx.cli_commands["tether"]
        with mock.patch("builtins.print") as printed:
            code = handler(types.SimpleNamespace(subcommand="status", json=True))
        self.assertEqual(code, 0)
        summary = json.loads(printed.call_args[0][0])
        self.assertEqual(summary["events"], 2)
        self.assertEqual(summary["verdicts"], {"admit": 1, "deny": 1})

    def test_unload_closes_the_journal_when_the_host_offers_it(self):
        ctx = RichCtx()
        self.register(ctx)
        self.assertEqual(len(ctx.unload_callbacks), 1)
        ctx.unload_callbacks[0]()  # must not raise


if __name__ == "__main__":
    unittest.main()


class InvalidatedBindingTest(PluginEnvironment):
    """An invalidated binding is not a live one."""

    def seed_v18_binding(self, state):
        db = self.home / ".hermes" / "bridges.db"
        connection = sqlite3.connect(db)
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS thread_bindings("
                "binding_id TEXT PRIMARY KEY,channel_id TEXT,thread_ts TEXT,"
                "state TEXT)"
            )
            connection.execute(
                "INSERT OR REPLACE INTO thread_bindings VALUES(?,?,?,?)",
                (f"bnd-{state}", "C1", "100.1", state),
            )
            connection.execute("PRAGMA user_version=18")
            connection.commit()
        finally:
            connection.close()
        os.chmod(db, 0o600)

    def dispatch_verdict(self):
        module = load_plugin(f"tether_plugin_next_states_{id(self)}")
        ctx = FloorCtx()
        module.register(ctx)
        ctx.hooks["pre_gateway_dispatch"](event=FakeEvent())
        rows = self.journal_rows()
        return rows[-1]["verdict"] if rows else None

    def test_rebind_required_binding_is_not_claimed(self):
        # The endpoint's incarnation moved; the domain will refuse turns on
        # this binding, so Tether must not claim its traffic.
        self.seed_v18_binding("rebind_required")
        self.assertEqual(self.dispatch_verdict(), "not_ours")

    def test_closed_binding_is_not_claimed(self):
        self.seed_v18_binding("closed")
        self.assertEqual(self.dispatch_verdict(), "not_ours")

    def test_active_binding_is_still_claimed(self):
        self.seed_v18_binding("active")
        self.assertEqual(self.dispatch_verdict(), "admit")


class AllowlistResolutionTest(PluginEnvironment):
    """Owners resolve the way the broker resolves them, not config-only."""

    def test_hermes_allowlist_env_authorizes_an_owner(self):
        self.write_config(allowed_users=[])
        with mock.patch.dict(
            os.environ, {"SLACK_ALLOWED_USERS": "U12345678,UOTHER01"}, clear=False
        ):
            settings = self.module.load_settings()
        self.assertIn("U12345678", settings.allowed_users)
        self.assertIn("UOTHER01", settings.allowed_users)
        self.assertTrue(settings.configured)

    def test_config_and_env_owners_merge(self):
        with mock.patch.dict(
            os.environ, {"GATEWAY_ALLOWED_USERS": "UENVONLY"}, clear=False
        ):
            settings = self.module.load_settings()
        self.assertEqual(
            {"U12345678", "UENVONLY"}, set(settings.allowed_users)
        )

    def test_wildcard_and_malformed_entries_are_refused(self):
        self.write_config(allowed_users=[])
        with mock.patch.dict(
            os.environ,
            {"SLACK_ALLOWED_USERS": "*, ,not a user id,U12345678"},
            clear=False,
        ):
            settings = self.module.load_settings()
        self.assertEqual({"U12345678"}, set(settings.allowed_users))
