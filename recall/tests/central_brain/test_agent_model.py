from __future__ import annotations

import sys
import unittest
from pathlib import Path


SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.agent_model import PiModelRuntime  # noqa: E402
from recall_server.agent_prompt import (  # noqa: E402
    AGENT_INVESTIGATOR_GUIDANCE,
    build_investigator_system_prompt,
)


class PiModelRuntimeTest(unittest.TestCase):
    def test_private_broker_model_is_explicit_and_provider_neutral(self):
        runtime = PiModelRuntime.from_environment({
            "RECALL_AGENT_MODEL_BASE_URL": "http://10.255.254.1:9400/v1/",
            "RECALL_AGENT_MODEL_ALIAS": "gpt-5.6-luna",
            "RECALL_AGENT_THINKING": "medium",
        })
        self.assertEqual(runtime.alias, "gpt-5.6-luna")
        self.assertEqual(runtime.base_url, "http://10.255.254.1:9400/v1")
        self.assertEqual(runtime.route_kind, "private_broker")
        self.assertEqual(runtime.provider, "broker")
        self.assertIsNone(runtime.provider_key_file)
        self.assertEqual(runtime.route_identity, "10.255.254.1")

    def test_model_alias_has_no_implicit_gemma_fallback(self):
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            PiModelRuntime.from_environment({
                "RECALL_AGENT_MODEL_BASE_URL": "http://10.255.254.1:9400",
            })

    def test_direct_provider_selects_the_same_runtime_interface(self):
        runtime = PiModelRuntime.from_environment({
            "RECALL_AGENT_MODEL_BASE_URL": "https://models.example/v1",
            "RECALL_AGENT_MODEL_ALIAS": "open-model",
            "RECALL_AGENT_MODEL_KEY_FILE": "/run/secrets/model-key",
        })
        self.assertEqual(runtime.route_kind, "direct_provider")
        self.assertEqual(runtime.provider, "openai-compatible")
        self.assertEqual(runtime.provider_key_file, "/run/secrets/model-key")


class AgentPromptTest(unittest.TestCase):
    def test_authoritative_prompt_is_renderable_without_runtime_or_secrets(self):
        prompt = build_investigator_system_prompt("2026-08-07T12:00:00Z")
        self.assertIn("Recall's evidence investigator", prompt)
        self.assertIn(AGENT_INVESTIGATOR_GUIDANCE, prompt)
        self.assertIn("2026-08-07T12:00:00Z", prompt)
        self.assertIn("Always end by calling finish exactly once", prompt)
        self.assertNotIn("tenant:", prompt)
        self.assertNotIn("api key", prompt.lower())


if __name__ == "__main__":
    unittest.main()
