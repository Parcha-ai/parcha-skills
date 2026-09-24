"""Provider-visible Slack prose survives normalization without duplicate fallbacks."""

import unittest
from connectors.slack_source import normalize_slack_message


class SlackVisibleText(unittest.TestCase):
    def normalize(self, **changes):
        value = {"ts": "1784332800.000100", "user": "U111", "text": "", **changes}
        return normalize_slack_message(
            workspace_id="T123", channel_id="C123", value=value
        )

    def test_attachment_only_pretext_and_fields_are_visible(self):
        record = self.normalize(
            attachments=[
                {
                    "pretext": "Build needs review",
                    "fields": [
                        {"title": "Owner", "value": "Release team"},
                        {"title": "Status", "value": "Blocked"},
                    ],
                    "fallback": "Build needs review. Owner Release team. Status Blocked.",
                }
            ]
        )
        self.assertEqual(
            record.content["text"],
            "Build needs review\nOwner: Release team\nStatus: Blocked",
        )
        self.assertEqual(record.content["content_fidelity"], "complete")

    def test_attachment_title_body_footer_and_plain_message_keep_order(self):
        record = self.normalize(
            text="New report",
            attachments=[
                {
                    "pretext": "Preview",
                    "title": "Report title",
                    "text": "Report body",
                    "footer": "Report source",
                }
            ],
        )
        self.assertEqual(
            record.content["text"],
            "New report\nPreview\nReport title\nReport body\nReport source",
        )

    def test_notification_fallback_not_duplicated_over_visible_blocks(self):
        record = self.normalize(
            text="notification summary",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "Actual visible message"},
                }
            ],
        )
        self.assertEqual(record.content["text"], "Actual visible message")

    def test_attachment_fallback_used_only_when_no_visible_body(self):
        self.assertEqual(
            self.normalize(attachments=[{"fallback": "Legacy fallback"}]).content[
                "text"
            ],
            "Legacy fallback",
        )
        self.assertEqual(
            self.normalize(
                text="Same fallback", attachments=[{"fallback": "Same fallback"}]
            ).content["text"],
            "Same fallback",
        )

    def test_rich_text_inline_elements_preserve_order_and_mentions(self):
        record = self.normalize(
            blocks=[
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {"type": "text", "text": "Hello "},
                                {"type": "user", "user_id": "U111"},
                                {"type": "text", "text": " "},
                                {
                                    "type": "link",
                                    "url": "https://example.com",
                                    "text": "runbook",
                                },
                                {"type": "text", "text": " "},
                                {"type": "emoji", "name": "wave"},
                            ],
                        }
                    ],
                }
            ]
        )
        self.assertEqual(
            record.content["text"], "Hello <@U111> <https://example.com|runbook> :wave:"
        )

    def test_repeated_visible_occurrences_are_not_deduplicated(self):
        record = self.normalize(
            blocks=[
                {"type": "section", "text": {"type": "plain_text", "text": "Again"}}
            ]
            * 2
        )
        self.assertEqual(record.content["text"], "Again\nAgain")

    def test_attachment_blocks_and_context_are_visible_without_private_action_values(
        self,
    ):
        record = self.normalize(
            attachments=[
                {
                    "blocks": [
                        {
                            "type": "header",
                            "text": {"type": "plain_text", "text": "Header"},
                        },
                        {
                            "type": "context",
                            "elements": [{"type": "mrkdwn", "text": "Context"}],
                        },
                        {
                            "type": "actions",
                            "elements": [
                                {
                                    "type": "button",
                                    "text": {"type": "plain_text", "text": "Open"},
                                    "action_id": "private-action-id",
                                    "value": "private hidden value",
                                }
                            ],
                        },
                    ],
                    "fallback": "Header Context Open",
                }
            ]
        )
        self.assertEqual(record.content["text"], "Header\nContext\nOpen")
        self.assertNotIn("private", record.content["text"])

    def test_edited_nested_payload_keeps_native_identity(self):
        plain = self.normalize(text="Before")
        changed = self.normalize(
            subtype="message_changed",
            message={
                "ts": "1784332800.000100",
                "user": "U111",
                "thread_ts": "1784332799.000100",
                "text": "",
                "edited": {"ts": "1784332801.000100"},
                "attachments": [{"text": "After"}],
            },
        )
        self.assertEqual(changed.native_id, plain.native_id)
        self.assertEqual(changed.content["text"], "After")
        self.assertIn("edited_at", changed.content)
        self.assertEqual(
            changed.content["reply_to_id"], "slack:T123:C123:1784332799.000100"
        )

    def test_plain_text_is_byte_preserved_and_not_silently_clipped(self):
        text = "x" * 500001
        self.assertEqual(self.normalize(text=text).content["text"], text)
        self.assertEqual(
            self.normalize(text="  first\nsecond  ").content["text"],
            "  first\nsecond  ",
        )

    def test_unknown_visible_structure_is_explicitly_partial(self):
        record = self.normalize(
            text="Fallback",
            blocks=[{"type": "future_block", "opaque": "private hidden value"}],
        )
        self.assertEqual(record.content["text"], "Fallback")
        self.assertEqual(record.content["content_fidelity"], "partial")
        self.assertIn("unsupported_slack_blocks", record.content["content_omissions"])

    def test_file_metadata_and_capture_version_remain_owned_here(self):
        from connectors.slack_source import SLACK_MESSAGE_CAPTURE_VERSION

        self.assertEqual(SLACK_MESSAGE_CAPTURE_VERSION, 2)
        record = self.normalize(
            files=[{"id": "F123", "title": "File title"}],
            attachments=[{"text": "Card text"}],
        )
        self.assertEqual(record.content["text"], "Card text")
        self.assertEqual(record.content["attachments"][0]["file_id"], "F123")
        self.assertEqual(record.content["content_fidelity"], "partial")
        self.assertIn("attachment_bytes", record.content["content_omissions"])

    def test_legacy_attachment_images_report_missing_bytes_and_title_keeps_link(self):
        record = self.normalize(
            attachments=[
                {
                    "title": "Report",
                    "title_link": "https://example.com/report",
                    "image_url": "https://example.com/chart.png",
                    "thumb_url": "https://example.com/thumb.png",
                }
            ]
        )
        self.assertEqual(record.content["text"], "<https://example.com/report|Report>")
        self.assertEqual(record.content["content_fidelity"], "partial")
        self.assertIn("image_bytes", record.content["content_omissions"])

    def test_date_preserves_timestamp_and_unsupported_date_is_partial(self):
        def date(**data):
            return self.normalize(
                blocks=[
                    {
                        "type": "rich_text",
                        "elements": [
                            {
                                "type": "rich_text_section",
                                "elements": [{"type": "date", **data}],
                            }
                        ],
                    }
                ]
            )

        self.assertEqual(
            date(timestamp=1784332800, format="{date_num}").content["text"],
            "<!date^1784332800^{date_num}>",
        )
        missing = date(format="{date_num}", fallback="Date unavailable")
        self.assertEqual(missing.content["text"], "Date unavailable")
        self.assertEqual(missing.content["content_fidelity"], "partial")
