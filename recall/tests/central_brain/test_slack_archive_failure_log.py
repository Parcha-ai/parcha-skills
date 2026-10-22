"""Slack failures remain retryable without exposing archive request contents."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import unittest
from unittest import mock

from botocore.exceptions import ClientError, ReadTimeoutError

from recall_server import app
from recall_server.archive import ArchiveError
from central_brain.test_webhook_http import WebhookServer


CANARY = "synthetic-private-message-and-credential"


def archive_failure(cause=None):
    error = ArchiveError(CANARY)
    error.__cause__ = cause
    return error


class SlackArchiveFailureLogTest(unittest.TestCase):
    def request_failure(self, error):
        secret = "synthetic-signing-secret"
        payload = {
            "type": "event_callback", "team_id": "T111", "event_id": "Ev111",
            "event": {
                "type": "message", "channel": "C111", "channel_type": "channel",
                "user": "U111", "ts": "1720000000.000001", "text": CANARY,
            },
        }
        body = json.dumps(payload).encode()
        timestamp = str(int(time.time()))
        signature = "v0=" + hmac.new(
            secret.encode(), f"v0:{timestamp}:".encode() + body, hashlib.sha256,
        ).hexdigest()
        store = mock.Mock()
        store.connect = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value
        connection.execute.return_value.fetchall.return_value = [{
            "source_id": "slack:synthetic", "principal_id": "synthetic-owner",
            "privacy_mode": "scrub", "tenant_id": "synthetic-tenant", "selectors": {},
        }]
        bridge = mock.Mock()
        bridge.ingest.side_effect = error
        environment = {
            "RECALL_HTTP_PROFILE": "public-edge", "RECALL_AUTH_REQUIRED": "1",
            "RECALL_TRUST_TAILSCALE_HEADERS": "0",
            "RECALL_SLACK_SIGNING_SECRET": secret,
            "RECALL_SLACK_CLIENT_ID": "synthetic-client",
            "RECALL_SLACK_CLIENT_SECRET": "synthetic-client-secret",
            "RECALL_SLACK_REDIRECT_URI": "https://recall.example/admin/oauth/callback/slack",
        }
        with mock.patch.dict(os.environ, environment), \
                mock.patch.object(app.Handler, "store", store, create=True), \
                mock.patch.object(app.Handler, "legacy_ingest", return_value=bridge), \
                self.assertLogs(app.LOG, level="ERROR") as captured:
            with WebhookServer(store) as server:
                status, raw = server.request(
                    "POST", "/webhooks/v1/slack", body=payload, token=None,
                    extra_headers={
                        "X-Slack-Request-Timestamp": timestamp,
                        "X-Slack-Signature": signature,
                    },
                )
        bridge.ingest.assert_called_once()
        self.assertEqual(status, 500)
        self.assertEqual(json.loads(raw), {"error": "slack webhook failed"})
        logged = "\n".join(captured.output)
        self.assertNotIn(CANARY, logged + raw.decode())
        return logged

    def test_signed_webhook_records_provider_code_without_request_data(self):
        cause = ClientError({
            "Error": {"Code": "SlowDown", "Message": CANARY},
            "ResponseMetadata": {
                "HTTPStatusCode": 503, "RequestId": CANARY,
                "HTTPHeaders": {"Authorization": CANARY},
            },
        }, CANARY)
        logged = self.request_failure(archive_failure(cause))
        self.assertIn("type=ArchiveError cause=ClientError status=503 code=SlowDown", logged)

    def test_timeout_records_only_class(self):
        cause = ReadTimeoutError(endpoint_url=f"https://{CANARY}/object")
        logged = self.request_failure(archive_failure(cause))
        self.assertIn("cause=ReadTimeoutError status=none code=none", logged)

    def test_missing_cause_does_not_log_exception_message(self):
        logged = self.request_failure(archive_failure())
        self.assertIn("cause=none status=none code=none", logged)

    def test_unknown_exception_name_is_not_logged(self):
        cause = type(CANARY, (Exception,), {})(CANARY)
        logged = self.request_failure(archive_failure(cause))
        self.assertIn("cause=other status=none code=none", logged)

    def test_unknown_code_cannot_inject_private_text(self):
        cause = ClientError({
            "Error": {"Code": CANARY, "Message": CANARY},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        }, CANARY)
        logged = self.request_failure(archive_failure(cause))
        self.assertIn("cause=ClientError status=403 code=other", logged)

    def test_malformed_response_does_not_break_failure_response(self):
        for response in (None, [], {"Error": []}, {
            "Error": {"Code": [CANARY]},
            "ResponseMetadata": {"HTTPStatusCode": True},
        }, {"ResponseMetadata": {"HTTPStatusCode": CANARY}}):
            with self.subTest(response=response):
                cause = ClientError({"Error": {"Code": "SlowDown"}}, "PutObject")
                cause.response = response
                logged = self.request_failure(archive_failure(cause))
                self.assertIn("cause=ClientError status=none code=none", logged)


if __name__ == "__main__":
    unittest.main()
