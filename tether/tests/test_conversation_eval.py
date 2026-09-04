"""Structural conversation metrics are deterministic over the shipped fixture."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load():
    spec = importlib.util.spec_from_file_location("conversation_eval", ROOT / "evals" / "conversation" / "conversation_eval.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ConversationEvalTest(unittest.TestCase):
    def test_measure_pins_the_baseline(self):
        module = load()
        threads = json.loads((ROOT / "evals" / "conversation" / "fixtures" / "agent-hub-2026-09-03.json").read_text())
        metrics = module.measure(threads)
        self.assertEqual(metrics["threads"], 10)
        self.assertEqual(metrics["answered_by_addressee"], 8)
        self.assertEqual(metrics["marker_leaks"], 1)
        self.assertEqual(metrics["bare_mentions"], 1)
        self.assertEqual(metrics["status_lines_in_threads"], 3)
        self.assertEqual(metrics["threads_with_unasked_extra_voices"], 5)
        self.assertEqual(metrics["median_first_reply_s"], 21)

    def test_redaction_masks_emails_and_urls(self):
        module = load()
        self.assertEqual(module.redact("mail me@x.io see https://a.b/c now"), "mail <email> see <url> now")

    def test_fixture_carries_no_raw_human_identities(self):
        text = (ROOT / "evals" / "conversation" / "fixtures" / "agent-hub-2026-09-03.json").read_text()
        self.assertNotIn("@", text.replace("<email>", "").replace("<@", ""))


if __name__ == "__main__":
    unittest.main()
