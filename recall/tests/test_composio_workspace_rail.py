from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import unittest

from connectors.composio_workspace_rail import (
    ComposioWorkspaceRail,
    _fetch_binary,
)
from connectors.google_workspace import (
    GmailConnector,
    GoogleCalendarConnector,
    GoogleContactsConnector,
    GoogleDriveConnector,
)
from connectors.workspace_rail import WorkspaceRailError
from tests.test_google_workspace_connectors import gmail_message


@dataclass
class ProxyResponse:
    status: int
    data: object | None = None
    binary_data: object | None = None
    headers: dict[str, str] | None = None


class FakeSession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def proxy_execute(self, **request):
        self.calls.append(request)
        key = (request["toolkit"], request["endpoint"])
        values = self.responses[key]
        if not values:
            raise AssertionError(f"unexpected synthetic proxy request: {key}")
        value = values.popleft()
        if isinstance(value, Exception):
            raise value
        return value


class FakeClient:
    def __init__(self, session):
        self.session = session
        self.create_calls = []

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        return self.session


class FakeSDKError(RuntimeError):
    def __init__(self, *, status_code, provider_code):
        self.status_code = status_code
        self.body = {"error": {"code": provider_code}}
        super().__init__("synthetic provider detail that must not escape")


class DirectRail:
    def __init__(self, operations):
        self.operations = operations

    def run(self, operation, _params):
        values = self.operations[operation]
        if not values:
            raise AssertionError(f"unexpected synthetic direct request: {operation}")
        return values.popleft()

    def export_document(self, *, file_id, mime_type="text/plain"):
        raise AssertionError((file_id, mime_type))


def queue(values):
    return defaultdict(deque, {key: deque(items) for key, items in values.items()})


class ComposioWorkspaceRailTests(unittest.TestCase):
    def rail(self, connector_id, responses, **kwargs):
        session = FakeSession(queue(responses))
        client = FakeClient(session)
        rail = ComposioWorkspaceRail(
            api_key="synthetic-project-authority",
            user_id="principal_synthetic_owner",
            connected_account_id="ca_synthetic_account_123",
            connector_id=connector_id,
            client_factory=lambda _api_key: client,
            **kwargs,
        )
        return rail, client, session

    def test_session_is_pinned_to_exact_user_toolkit_and_account(self):
        rail, client, session = self.rail(
            "google.gmail",
            {
                ("gmail", "/gmail/v1/users/me/profile"): [
                    ProxyResponse(200, {"emailAddress": "owner@example.invalid"})
                ]
            },
        )
        self.assertEqual(
            rail.run("gmail.users.getProfile", {"userId": "me"}),
            {"emailAddress": "owner@example.invalid"},
        )
        self.assertEqual(
            client.create_calls,
            [
                {
                    "user_id": "principal_synthetic_owner",
                    "toolkits": ["gmail"],
                    "connected_accounts": {"gmail": ["ca_synthetic_account_123"]},
                }
            ],
        )
        self.assertEqual(session.calls[0]["method"], "GET")
        with self.assertRaisesRegex(WorkspaceRailError, "operation_not_allowed"):
            rail.run(
                "calendar.events.list",
                {"calendarId": "primary", "maxResults": 1},
            )

    def test_expired_connected_account_maps_to_stable_revoked_authority(self):
        rail, _client, _session = self.rail(
            "google.gmail",
            {
                ("gmail", "/gmail/v1/users/me/profile"): [
                    FakeSDKError(status_code=400, provider_code=4302)
                ]
            },
        )
        with self.assertRaisesRegex(WorkspaceRailError, "authority_revoked"):
            rail.run("gmail.users.getProfile", {"userId": "me"})

    def test_generic_bad_request_does_not_impersonate_revoked_authority(self):
        rail, _client, _session = self.rail(
            "google.gmail",
            {
                ("gmail", "/gmail/v1/users/me/profile"): [
                    FakeSDKError(status_code=400, provider_code=9999)
                ]
            },
        )
        with self.assertRaisesRegex(
            WorkspaceRailError,
            "upstream_invalid_request",
        ):
            rail.run("gmail.users.getProfile", {"userId": "me"})

    def test_only_closed_read_operations_and_parameters_reach_proxy(self):
        rail, _client, session = self.rail(
            "google.gmail",
            {
                ("gmail", "/gmail/v1/users/me/history"): [
                    ProxyResponse(200, {"history": [], "historyId": "101"})
                ]
            },
        )
        value = rail.run(
            "gmail.history.list",
            {
                "userId": "me",
                "startHistoryId": "100",
                "maxResults": 50,
                "historyTypes": ["messageAdded", "messageDeleted"],
            },
        )
        self.assertEqual(value["historyId"], "101")
        self.assertEqual(
            session.calls[0]["parameters"],
            [
                {"name": "historyTypes", "value": "messageAdded", "in": "query"},
                {"name": "historyTypes", "value": "messageDeleted", "in": "query"},
                {"name": "maxResults", "value": "50", "in": "query"},
                {"name": "startHistoryId", "value": "100", "in": "query"},
            ],
        )
        for operation, params in (
            ("gmail.messages.send", {"userId": "me"}),
            ("gmail.history.list", {"userId": "me", "url": "https://bad.invalid"}),
        ):
            with (
                self.subTest(operation=operation),
                self.assertRaises(WorkspaceRailError),
            ):
                rail.run(operation, params)

    def test_gmail_body_part_fetch_is_bound_to_message_and_attachment(self):
        rail, _client, session = self.rail(
            "google.gmail",
            {
                (
                    "gmail",
                    "/gmail/v1/users/me/messages/message-1/attachments/body-part-1",
                ): [ProxyResponse(200, {"size": 12, "data": "c3ludGhldGlj"})]
            },
        )

        value = rail.run(
            "gmail.messages.attachments.get",
            {"userId": "me", "messageId": "message-1", "id": "body-part-1"},
        )

        self.assertEqual(value["size"], 12)
        self.assertEqual(session.calls[0]["parameters"], [])

    def test_status_shape_and_output_failures_are_content_free(self):
        expected = {
            401: "authority_revoked",
            403: "authority_forbidden",
            404: "not_found",
            410: "cursor_expired",
            429: "rate_limited",
        }
        for status, code in expected.items():
            rail, _client, _session = self.rail(
                "google.gmail",
                {
                    ("gmail", "/gmail/v1/users/me/history"): [
                        ProxyResponse(
                            status,
                            {"error": {"message": "synthetic-private-upstream-detail"}},
                        )
                    ]
                },
            )
            with (
                self.subTest(status=status),
                self.assertRaisesRegex(WorkspaceRailError, code) as raised,
            ):
                rail.run(
                    "gmail.history.list",
                    {"userId": "me", "startHistoryId": "100"},
                )
            self.assertNotIn("private-upstream-detail", str(raised.exception))

        malformed, _client, _session = self.rail(
            "google.gmail",
            {
                ("gmail", "/gmail/v1/users/me/history"): [
                    ProxyResponse(200, ["not", "an", "object"])
                ]
            },
        )
        with self.assertRaisesRegex(WorkspaceRailError, "invalid_response_shape"):
            malformed.run(
                "gmail.history.list",
                {"userId": "me", "startHistoryId": "100"},
            )

        oversized, _client, _session = self.rail(
            "google.gmail",
            {
                ("gmail", "/gmail/v1/users/me/history"): [
                    ProxyResponse(200, {"history": ["x" * 512], "historyId": "101"})
                ]
            },
            max_output_bytes=128,
        )
        with self.assertRaisesRegex(WorkspaceRailError, "output_too_large"):
            oversized.run(
                "gmail.history.list",
                {"userId": "me", "startHistoryId": "100"},
            )

    def test_document_export_requires_an_exact_download_host(self):
        binary = type(
            "Binary",
            (),
            {
                "url": "https://download.composio.invalid/object",
                "size": 18,
            },
        )()
        seen = []

        def fetch(url, **kwargs):
            seen.append((url, kwargs))
            return b"Synthetic document"

        rail, _client, session = self.rail(
            "google.drive",
            {
                (
                    "googledrive",
                    "/drive/v3/files/synthetic-file/export",
                ): [ProxyResponse(200, binary_data=binary)]
            },
            binary_hosts=("download.composio.invalid",),
            binary_fetcher=fetch,
        )
        self.assertEqual(
            rail.export_document(file_id="synthetic-file"),
            b"Synthetic document",
        )
        self.assertEqual(
            session.calls[0]["parameters"],
            [{"name": "mimeType", "value": "text/plain", "in": "query"}],
        )
        self.assertEqual(
            seen[0][1]["allowed_hosts"],
            ("download.composio.invalid",),
        )
        with self.assertRaisesRegex(WorkspaceRailError, "export_invalid"):
            _fetch_binary(
                "https://metadata.invalid/private",
                maximum=128,
                allowed_hosts=("download.composio.invalid",),
            )

    def test_google_connectors_have_direct_and_composio_page_parity(self):
        gmail_list = {"messages": [{"id": "m1"}]}
        gmail_get = gmail_message("m1")
        calendar = {
            "items": [
                {
                    "id": "event-1",
                    "status": "confirmed",
                    "summary": "Synthetic meeting",
                    "updated": "2026-07-20T01:00:00Z",
                    "start": {"dateTime": "2026-07-20T10:00:00Z"},
                    "end": {"dateTime": "2026-07-20T11:00:00Z"},
                }
            ],
            "nextSyncToken": "calendar-sync-1",
        }
        contacts = {
            "connections": [
                {
                    "resourceName": "people/c1",
                    "metadata": {"sources": [{"updateTime": "2026-07-20T02:00:00Z"}]},
                    "names": [{"displayName": "Synthetic Person"}],
                }
            ],
            "nextSyncToken": "contacts-sync-1",
        }
        drive_token = {"startPageToken": "drive-token-1"}
        drive_files = {
            "files": [
                {
                    "id": "file-1",
                    "name": "Synthetic File",
                    "mimeType": "application/pdf",
                    "modifiedTime": "2026-07-20T03:00:00Z",
                }
            ]
        }
        cases = [
            (
                "google.gmail",
                lambda rail: GmailConnector(
                    rail=rail,
                    source_id="synthetic:google:gmail",
                    own_addresses=("owner@example.invalid",),
                ),
                {
                    "gmail.messages.list": [gmail_list],
                    "gmail.messages.get": [gmail_get],
                },
                {
                    ("gmail", "/gmail/v1/users/me/messages"): [
                        ProxyResponse(200, gmail_list)
                    ],
                    ("gmail", "/gmail/v1/users/me/messages/m1"): [
                        ProxyResponse(200, gmail_get)
                    ],
                },
            ),
            (
                "google.calendar",
                lambda rail: GoogleCalendarConnector(
                    rail=rail, source_id="synthetic:google:calendar"
                ),
                {"calendar.events.list": [calendar]},
                {
                    ("googlecalendar", "/calendar/v3/calendars/primary/events"): [
                        ProxyResponse(200, calendar)
                    ]
                },
            ),
            (
                "google.contacts",
                lambda rail: GoogleContactsConnector(
                    rail=rail, source_id="synthetic:google:contacts"
                ),
                {"people.people.connections.list": [contacts]},
                {
                    ("googlecontacts", "/v1/people/me/connections"): [
                        ProxyResponse(200, contacts)
                    ]
                },
            ),
            (
                "google.drive",
                lambda rail: GoogleDriveConnector(
                    rail=rail,
                    source_id="synthetic:google:drive",
                    include_document_text=False,
                ),
                {
                    "drive.changes.getStartPageToken": [drive_token],
                    "drive.files.list": [drive_files],
                },
                {
                    ("googledrive", "/drive/v3/changes/startPageToken"): [
                        ProxyResponse(200, drive_token)
                    ],
                    ("googledrive", "/drive/v3/files"): [
                        ProxyResponse(200, drive_files)
                    ],
                },
            ),
        ]
        for connector_id, factory, direct_values, proxy_values in cases:
            with self.subTest(connector_id=connector_id):
                direct = factory(DirectRail(queue(direct_values))).pull(None)
                rail, client, _session = self.rail(connector_id, proxy_values)
                proxied = factory(rail).pull(None)
                self.assertEqual(proxied, direct)
                self.assertEqual(len(client.create_calls), 1)


if __name__ == "__main__":
    unittest.main()
