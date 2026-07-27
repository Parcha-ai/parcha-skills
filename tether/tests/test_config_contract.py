from __future__ import annotations

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
    environment = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_CONFIG_HOME": str(home / ".config"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
    }
    with mock.patch.dict(os.environ, environment, clear=False):
        name = f"tether_config_contract_{os.urandom(4).hex()}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        if spec is None or spec.loader is None:
            raise RuntimeError("runtime module could not be loaded")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


class ConfigContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="tether-config-")
        self.home = pathlib.Path(self.temporary.name)
        self.runtime = load_runtime(self.home)
        self.config = self.home / ".config" / "tether" / "config.toml"
        self.config.parent.mkdir(parents=True, mode=0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write(self, text: str) -> None:
        self.config.write_text(text, encoding="utf-8")
        self.config.chmod(0o600)

    def test_current_version_and_legacy_omission_are_accepted(self) -> None:
        self.write("config_version = 1\nretention_days = 30\n")
        self.assertEqual(self.runtime.load_config(self.config).retention_days, 30)

        self.write("retention_days = 30\n")
        self.assertEqual(self.runtime.load_config(self.config).retention_days, 30)

    def test_future_version_and_unknown_keys_fail_closed(self) -> None:
        self.write("config_version = 2\n")
        with self.assertRaisesRegex(ValueError, "config_version"):
            self.runtime.load_config(self.config)

        self.write("config_version = 1\nslakc_allowed_users = []\n")
        with self.assertRaisesRegex(ValueError, "unknown"):
            self.runtime.load_config(self.config)

    def test_scalar_types_and_control_characters_are_rejected(self) -> None:
        invalid_documents = (
            "config_version = true\n",
            'codex_binary = ["codex"]\n',
            'claude_binary = ""\n',
            'team_id = ["T12345678"]\n',
            'codex_resume_args = ["safe", "bad\\u0000argument"]\n',
        )
        for document in invalid_documents:
            with self.subTest(document=document):
                self.write(document)
                with self.assertRaises(ValueError):
                    self.runtime.load_config(self.config)


if __name__ == "__main__":
    unittest.main()
