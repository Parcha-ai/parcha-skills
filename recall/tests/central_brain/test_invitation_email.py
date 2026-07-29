from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import unittest
from unittest import mock


SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.invitation_email import (  # noqa: E402
    DescopeInvitationEmailSender,
    InvitationEmail,
    InvitationEmailError,
    ResendInvitationEmailSender,
    installation_commands,
    invitation_message,
    invitation_urls,
    onboarding_page,
    sender_from_env,
)


class Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class Opener:
    def __init__(self, response=None, error=None):
        self.response = response or Response()
        self.error = error
        self.calls = []

    def open(self, request, *, timeout):
        self.calls.append((request, timeout))
        if self.error is not None:
            raise self.error
        return self.response


def invitation(**updates) -> InvitationEmail:
    values = {
        "invitation_id": "11111111-2222-4333-8444-555555555555",
        "recipient": "invitee@example.invalid",
        "organization_name": "Synthetic & Company",
        "brain_slug": "Engineering Brain",
        "role": "member",
        "expires_at": datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
        "brain_url": (
            "https://recall.synthetic.invalid/mcp/brains/tenant:company:synthetic"
        ),
        "onboarding_url": (
            "https://recall.synthetic.invalid/join/11111111-2222-4333-8444-555555555555"
        ),
    }
    values.update(updates)
    return InvitationEmail(**values)


class InvitationTemplateTest(unittest.TestCase):
    def test_urls_and_current_cli_commands_are_brain_specific(self) -> None:
        brain_url, onboarding_url = invitation_urls(
            "https://recall.synthetic.invalid/mcp",
            tenant_id="tenant:company:synthetic",
            invitation_id="11111111-2222-4333-8444-555555555555",
        )
        item = invitation(brain_url=brain_url, onboarding_url=onboarding_url)

        commands = installation_commands(item)

        self.assertEqual(commands["codex"][0], "npm install -g @openai/codex")
        self.assertEqual(
            commands["codex"][1],
            "codex mcp add recall-engineering-brain --url " + brain_url,
        )
        self.assertEqual(
            commands["codex"][2],
            "codex mcp login recall-engineering-brain",
        )
        self.assertEqual(
            commands["claude"][0],
            "npm install -g @anthropic-ai/claude-code",
        )
        self.assertEqual(
            commands["claude"][1],
            "claude mcp add --scope user --transport http "
            "recall-engineering-brain " + brain_url,
        )
        self.assertEqual(
            commands["claude"][2],
            "claude mcp login recall-engineering-brain",
        )
        self.assertEqual(
            onboarding_url,
            "https://recall.synthetic.invalid/join/"
            "11111111-2222-4333-8444-555555555555",
        )

    def test_message_and_page_escape_dynamic_values_and_never_render_recipient(
        self,
    ) -> None:
        item = invitation(organization_name='<script>alert("x")</script>')

        subject, text, html = invitation_message(item)
        page = onboarding_page(item).decode()

        self.assertIn('<script>alert("x")</script>', subject)
        self.assertNotIn("invitee@example.invalid", text)
        self.assertNotIn('<script>alert("x")</script>', html)
        self.assertIn("&lt;script&gt;", html)
        self.assertNotIn('<script>alert("x")</script>', page)
        self.assertIn("&lt;script&gt;", page)
        self.assertNotIn("invitee@example.invalid", page)


class InvitationSenderTest(unittest.TestCase):
    def test_descope_sends_only_the_email_and_opaque_onboarding_url(self) -> None:
        opener = Opener()
        sender = DescopeInvitationEmailSender(
            project_id="P12345678901",
            management_key="synthetic-management-secret",
            opener=opener,
        )
        sender.send(invitation())

        request, timeout = opener.calls[0]
        body = json.loads(request.data)
        self.assertEqual(body["loginId"], "invitee@example.invalid")
        self.assertEqual(body["email"], "invitee@example.invalid")
        self.assertTrue(body["invite"])
        self.assertTrue(body["sendMail"])
        self.assertFalse(body["sendSMS"])
        self.assertNotIn("brain_url", body)
        self.assertNotIn("synthetic-management-secret", request.data.decode())
        self.assertEqual(request.headers["User-agent"], "recall-core/1")
        self.assertEqual(timeout, 10.0)

    def test_resend_email_contains_both_clients_and_idempotency_key(self) -> None:
        opener = Opener()
        sender = ResendInvitationEmailSender(
            api_key="synthetic-api-secret",
            from_address="Recall <recall@example.invalid>",
            opener=opener,
        )
        sender.send(invitation())

        request, _ = opener.calls[0]
        body = json.loads(request.data)
        self.assertIn("npm install -g @openai/codex", body["text"])
        self.assertIn("npm install -g @anthropic-ai/claude-code", body["text"])
        self.assertNotIn("synthetic-api-secret", request.data.decode())
        self.assertEqual(request.headers["User-agent"], "recall-core/1")
        self.assertEqual(
            request.headers["Idempotency-key"],
            "recall-invitation/11111111-2222-4333-8444-555555555555",
        )

    def test_sender_configuration_is_explicit_and_closed(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(sender_from_env())
        with mock.patch.dict(
            os.environ,
            {
                "RECALL_INVITATION_EMAIL_PROVIDER": "unexpected",
                "RECALL_MCP_RESOURCE_URI": "https://recall.synthetic.invalid/mcp",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(
                InvitationEmailError, "invitation_email_provider_invalid"
            ):
                sender_from_env()
        with mock.patch.dict(
            os.environ,
            {
                "RECALL_INVITATION_EMAIL_PROVIDER": "descope",
                "RECALL_DESCOPE_PROJECT_ID": "P12345678901",
                "RECALL_MCP_RESOURCE_URI": "https://recall.synthetic.invalid/mcp",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(InvitationEmailError, "configuration_invalid"):
                sender_from_env()


if __name__ == "__main__":
    unittest.main()
