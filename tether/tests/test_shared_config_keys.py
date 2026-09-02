"""The legacy broker and the schema-18 plugin share one config.toml.

Keys owned by plugin_next must not make the broker refuse to start.
On 2026-09-02 enabling ``active = true`` did exactly that: load_config raised
``unknown Tether config keys`` and the gateway ran with no broker socket.
"""

from __future__ import annotations

import importlib
import os
import pathlib
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


class SharedConfigKeysTest(unittest.TestCase):
    def test_plugin_next_keys_are_accepted_by_the_broker(self):
        previous = list(sys.path)
        sys.path.insert(0, str(RUNTIME))
        try:
            for name in ("bridge_runtime", "plugin_next", "plugin_next.active"):
                sys.modules.pop(name, None)
            active = importlib.import_module("plugin_next.active")
            bridge_runtime = importlib.import_module("bridge_runtime")
        finally:
            sys.path[:] = previous
        for key in ("active", "trusted_bot_users"):
            self.assertIn(key, bridge_runtime.CONFIG_KEYS)
        with tempfile.TemporaryDirectory() as temp:
            config_dir = pathlib.Path(temp) / "tether"
            config_dir.mkdir()
            config = config_dir / "config.toml"
            config.write_text(
                'active = true\ntrusted_bot_users = ["U0PEER0001"]\n'
                'team_id = "T12345678"\nallowed_users = ["U12345678"]\n',
                encoding="utf-8",
            )
            saved = os.environ.get("XDG_CONFIG_HOME")
            os.environ["XDG_CONFIG_HOME"] = temp
            try:
                loaded = bridge_runtime.load_config()
            finally:
                if saved is None:
                    os.environ.pop("XDG_CONFIG_HOME", None)
                else:
                    os.environ["XDG_CONFIG_HOME"] = saved
            # The broker may resolve team/allowlist from the gateway environment;
            # what matters here is that load_config accepted the shared keys.
            self.assertIsNotNone(loaded)
            settings = active.load_active_settings(config)
            self.assertTrue(settings.enabled)


if __name__ == "__main__":
    unittest.main()
