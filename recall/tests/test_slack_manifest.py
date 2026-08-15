from __future__ import annotations

import unittest

from connectors.registry import definition
from connectors.slack_manifest import slack_app_manifest
from connectors.slack_source import SLACK_PUBLIC_HISTORY_USER_SCOPES


class SlackManifestTest(unittest.TestCase):
    def test_manifest_is_exactly_derived_from_connector_scopes_and_origin(self):
        value = slack_app_manifest("https://recall.example", events=True)
        self.assertEqual(
            value["oauth_config"]["scopes"]["bot"],
            list(definition("slack.messages").auth.minimum_scopes),
        )
        self.assertEqual(
            value["oauth_config"]["scopes"]["user"],
            list(SLACK_PUBLIC_HISTORY_USER_SCOPES),
        )
        self.assertEqual(
            value["oauth_config"]["redirect_urls"],
            ["https://recall.example/admin/oauth/callback/slack"],
        )
        self.assertEqual(
            value["settings"]["event_subscriptions"]["request_url"],
            "https://recall.example/webhooks/v1/slack",
        )
        self.assertEqual(
            value["settings"]["event_subscriptions"]["bot_events"],
            ["message.channels"],
        )
        self.assertNotIn(
            "event_subscriptions", slack_app_manifest("https://recall.example")["settings"],
        )

    def test_manifest_rejects_paths_credentials_queries_and_plain_http(self):
        for value in (
            "http://recall.example", "https://user@recall.example",
            "https://recall.example/path", "https://recall.example?x=1",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                slack_app_manifest(value)


if __name__ == "__main__":
    unittest.main()
