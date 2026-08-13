from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
import urllib.error
from pathlib import Path

from connectors.registry import ConnectorDefinitionV3, definition, preview
from connectors.kit import (
    BoundedJsonRail as PublicBoundedJsonRail,
    RemoteApiError as PublicRemoteApiError,
    RemoteOperation as PublicRemoteOperation,
)
from connectors.remote_api import (
    BoundedJsonRail,
    RemoteApiError,
    RemoteOperation,
)
from connectors.sdk import ConnectorRateLimited


REMOTE_CONNECTORS = {
    "google.gmail": {
        "family": "communications",
        "kinds": ("communication_message.v1", "document.v1"),
        "auth": "oauth2",
        "acquisition": ("poll",),
    },
    "google.calendar": {
        "family": "schedule",
        "kinds": ("calendar_event.v1",),
        "auth": "oauth2",
        "acquisition": ("poll",),
    },
    "google.contacts": {
        "family": "contacts",
        "kinds": ("contact_identity.v1",),
        "auth": "oauth2",
        "acquisition": ("poll",),
    },
    "google.drive": {
        "family": "documents",
        "kinds": ("document.v1",),
        "auth": "oauth2",
        "acquisition": ("poll",),
    },
    "github.activity": {
        "family": "work_activity",
        "kinds": ("document.v1",),
        "auth": "api_token",
        "acquisition": ("poll",),
    },
    "linear.activity": {
        "family": "work_activity",
        "kinds": ("document.v1",),
        "auth": "oauth2",
        "acquisition": ("poll", "webhook"),
    },
    "slack.messages": {
        "family": "communications",
        "kinds": ("communication_message.v1", "contact_identity.v1", "document.v1"),
        "auth": "oauth2",
        "acquisition": ("poll", "webhook"),
    },
    "notion.workspace": {
        "family": "documents",
        "kinds": ("document.v1",),
        "auth": "oauth2",
        "acquisition": ("poll",),
    },
    "x.activity": {
        "family": "social",
        "kinds": ("social_post.v1",),
        "auth": "oauth2",
        "acquisition": ("poll",),
    },
}


class RemoteRegistryTest(unittest.TestCase):
    def test_transport_contract_is_on_the_public_kit_surface(self):
        self.assertIs(PublicBoundedJsonRail, BoundedJsonRail)
        self.assertIs(PublicRemoteApiError, RemoteApiError)
        self.assertIs(PublicRemoteOperation, RemoteOperation)

    def test_nine_remote_definitions_are_closed_v3_manifests(self):
        for connector_id, expected in REMOTE_CONNECTORS.items():
            with self.subTest(connector_id=connector_id):
                item = definition(connector_id)
                self.assertIsInstance(item, ConnectorDefinitionV3)
                self.assertEqual(item.source_family, expected["family"])
                self.assertEqual(item.record_kinds, expected["kinds"])
                self.assertEqual(item.placement.execution, "remote_worker")
                self.assertEqual(item.placement.acquisition, expected["acquisition"])
                self.assertEqual(item.auth.kind, expected["auth"])
                self.assertTrue(item.auth.minimum_scopes)
                self.assertEqual(item.authority_slots, ("brain", "source"))
                self.assertEqual(item.policy.default_privacy_mode, "scrub")
                self.assertEqual(
                    item.policy.attachment_capability,
                    connector_id in {"google.gmail", "slack.messages"},
                )
                self.assertEqual(
                    ConnectorDefinitionV3.from_mapping(item.to_public()),
                    item,
                )

    def test_preview_is_lazy_content_free_and_has_no_recipe_surface(self):
        value = preview()
        self.assertEqual(value["credential_reads"], 0)
        self.assertEqual(value["source_reads"], 0)
        self.assertEqual(value["network_requests"], 0)
        self.assertEqual(value["writes"], 0)
        self.assertEqual(
            {item["connector_id"] for item in value["connectors"]} & set(REMOTE_CONNECTORS),
            set(REMOTE_CONNECTORS),
        )
        rendered = json.dumps(value, sort_keys=True)
        for forbidden in (
            "credential_path",
            "token",
            "secret",
            "base_url",
            "endpoint",
            "recipe",
            "executable",
        ):
            self.assertNotIn(f'"{forbidden}"', rendered)

    def test_x_home_timeline_is_selectable_but_not_implicitly_enabled(self):
        item = definition("x.activity")
        self.assertIn("streams", item.selection_fields)
        self.assertNotIn("home", item.to_public())
        self.assertNotIn("defaults", item.to_public())
        self.assertNotIn("enabled", item.to_public())


class _Response:
    def __init__(
        self,
        body: bytes,
        *,
        status: int = 200,
        content_type: str = "application/json",
    ):
        self._body = io.BytesIO(body)
        self.status = status
        self.headers = {
            "Content-Type": content_type,
            "Content-Length": str(len(body)),
        }

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class RemoteApiRailTest(unittest.TestCase):
    def private_secret(self, root: Path) -> Path:
        os.chmod(root, 0o700)
        path = root / "authority"
        path.write_text("synthetic-authority")
        os.chmod(path, 0o600)
        return path

    def test_exact_code_owned_get_is_bounded_and_lazy(self):
        captured = []

        def opener(request, *, timeout):
            captured.append((request, timeout))
            return _Response(b'{"items":[{"id":"synthetic"}],"next":"page-2"}')

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            secret = self.private_secret(root)
            operation = RemoteOperation(
                method="GET",
                path_template="/repos/{owner}/{repo}/issues",
                path_fields=("owner", "repo"),
                query_fields=("page", "per_page"),
            )
            rail = BoundedJsonRail(
                origin="https://api.github.com",
                authority_path=secret,
                authorization_scheme="Bearer",
                operations={"issues.list": operation},
                opener=opener,
            )
            self.assertEqual(captured, [])
            result = rail.request(
                "issues.list",
                path={"owner": "synthetic-org", "repo": "synthetic-repo"},
                query={"page": 2, "per_page": 100},
            )
        self.assertEqual(result["next"], "page-2")
        self.assertEqual(len(captured), 1)
        request, timeout = captured[0]
        self.assertEqual(timeout, 30)
        self.assertEqual(
            request.full_url,
            "https://api.github.com/repos/synthetic-org/synthetic-repo/issues?page=2&per_page=100",
        )
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(
            request.get_header("Authorization"),
            "Bearer synthetic-authority",
        )

    def test_binary_download_is_exact_host_bounded_and_authorized(self):
        captured = []

        def opener(request, *, timeout):
            captured.append(request)
            return _Response(b"full attachment", content_type="text/plain")

        with tempfile.TemporaryDirectory() as directory:
            secret = self.private_secret(Path(directory))
            rail = BoundedJsonRail(
                origin="https://slack.com",
                authority_path=secret,
                authorization_scheme="Bearer",
                operations={
                    "users.list": RemoteOperation(
                        method="GET", path_template="/api/users.list",
                        path_fields=(), query_fields=(),
                    ),
                },
                binary_hosts=("files.slack.com",),
                opener=opener,
            )
            payload, media_type = rail.download_binary(
                "https://files.slack.com/files-pri/T123-F123/document.txt"
            )
            self.assertEqual(payload, b"full attachment")
            self.assertEqual(media_type, "text/plain")
            self.assertEqual(captured[0].get_header("Authorization"),
                             "Bearer synthetic-authority")
            with self.assertRaisesRegex(RemoteApiError, "binary_url_invalid"):
                rail.download_binary("https://example.invalid/files-pri/private")

    def test_code_owned_provider_headers_are_closed_and_cannot_override_authority(self):
        captured = []

        def opener(request, *, timeout):
            captured.append(request)
            return _Response(b"{}")

        with tempfile.TemporaryDirectory() as directory:
            secret = self.private_secret(Path(directory))
            rail = BoundedJsonRail(
                origin="https://api.notion.com",
                authority_path=secret,
                authorization_scheme="Bearer",
                operations={
                    "search.list": RemoteOperation(
                        method="POST",
                        path_template="/v1/search",
                        path_fields=(),
                        query_fields=(),
                        json_fields=("page_size",),
                    ),
                },
                fixed_headers={"Notion-Version": "2026-03-11"},
                opener=opener,
            )
            rail.request("search.list", json_body={"page_size": 100})
        self.assertEqual(captured[0].get_header("Notion-version"), "2026-03-11")
        for forbidden in ("Authorization", "Accept", "Content-Type", "User-Agent"):
            with self.assertRaisesRegex(ValueError, "invalid fixed headers"):
                BoundedJsonRail(
                    origin="https://api.notion.com",
                    authority_path=Path("/synthetic/not-read"),
                    authorization_scheme="Bearer",
                    operations={
                        "search.list": RemoteOperation(
                            method="GET",
                            path_template="/v1/search",
                            path_fields=(),
                            query_fields=(),
                        ),
                    },
                    fixed_headers={forbidden: "synthetic"},
                )

    def test_unknown_operation_extra_fields_and_bad_authority_fail_before_io(self):
        calls = []

        def opener(*_args, **_kwargs):
            calls.append(True)
            return _Response(b"{}")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            secret = self.private_secret(root)
            operation = RemoteOperation(
                method="GET",
                path_template="/users/{user}",
                path_fields=("user",),
                query_fields=("page",),
            )
            rail = BoundedJsonRail(
                origin="https://api.example.com",
                authority_path=secret,
                authorization_scheme="Bearer",
                operations={"users.get": operation},
                opener=opener,
            )
            with self.assertRaisesRegex(RemoteApiError, "operation_not_allowed"):
                rail.request("users.delete", path={"user": "synthetic"})
            with self.assertRaisesRegex(RemoteApiError, "parameter_not_allowed"):
                rail.request(
                    "users.get",
                    path={"user": "synthetic"},
                    query={"page": 1, "url": "https://elsewhere.invalid"},
                )
            os.chmod(secret, 0o644)
            with self.assertRaisesRegex(RemoteApiError, "authority_invalid"):
                rail.request("users.get", path={"user": "synthetic"})
        self.assertEqual(calls, [])

    def test_post_body_keeps_provider_query_fixed_in_code(self):
        captured = []

        def opener(request, *, timeout):
            captured.append(request)
            return _Response(b'{"data":{"issues":{"nodes":[]}}}')

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            secret = self.private_secret(root)
            operation = RemoteOperation(
                method="POST",
                path_template="/graphql",
                path_fields=(),
                query_fields=(),
                json_fields=("variables",),
                fixed_json={
                    "query": "query RecallIssues($after:String){issues(after:$after){nodes{id}}}",
                },
            )
            rail = BoundedJsonRail(
                origin="https://api.linear.app",
                authority_path=secret,
                authorization_scheme="Bearer",
                operations={"issues.list": operation},
                opener=opener,
            )
            value = rail.request(
                "issues.list",
                json_body={"variables": {"after": "synthetic-cursor"}},
            )
            self.assertIn("data", value)
            with self.assertRaisesRegex(RemoteApiError, "parameter_not_allowed"):
                rail.request(
                    "issues.list",
                    json_body={"query": "mutation Forbidden { issueDelete(id:\"x\") }"},
                )
        self.assertEqual(len(captured), 1)
        request = captured[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Content-type"), "application/json")
        body = json.loads(request.data)
        self.assertTrue(body["query"].startswith("query RecallIssues"))
        self.assertEqual(body["variables"], {"after": "synthetic-cursor"})

    def test_fixed_query_is_code_owned_and_disjoint_from_runtime_query(self):
        captured = []

        def opener(request, *, timeout):
            captured.append(request)
            return _Response(b'{"data":[],"meta":{}}')

        with tempfile.TemporaryDirectory() as directory:
            secret = self.private_secret(Path(directory))
            rail = BoundedJsonRail(
                origin="https://api.x.com",
                authority_path=secret,
                authorization_scheme="Bearer",
                operations={
                    "posts.list": RemoteOperation(
                        method="GET",
                        path_template="/2/users/{user_id}/tweets",
                        path_fields=("user_id",),
                        query_fields=("pagination_token", "since_id"),
                        fixed_query={
                            "tweet.fields": "author_id,conversation_id,created_at",
                        },
                    ),
                },
                opener=opener,
            )
            rail.request(
                "posts.list",
                path={"user_id": "123"},
                query={"since_id": "456"},
            )
        self.assertEqual(
            captured[0].full_url,
            "https://api.x.com/2/users/123/tweets?"
            "tweet.fields=author_id%2Cconversation_id%2Ccreated_at&since_id=456",
        )
        with self.assertRaisesRegex(ValueError, "invalid fixed query"):
            RemoteOperation(
                method="GET",
                path_template="/2/users/{user_id}/tweets",
                path_fields=("user_id",),
                query_fields=("since_id",),
                fixed_query={"since_id": "not-runtime-owned"},
            )

    def test_redirect_rate_limit_content_type_and_response_size_fail_closed(self):
        operation = RemoteOperation(
            method="GET",
            path_template="/items",
            path_fields=(),
            query_fields=(),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            secret = self.private_secret(root)

            def make(opener, **kwargs):
                return BoundedJsonRail(
                    origin="https://api.example.com",
                    authority_path=secret,
                    authorization_scheme="Bearer",
                    operations={"items.list": operation},
                    opener=opener,
                    **kwargs,
                )

            def redirect(_request, *, timeout):
                raise urllib.error.HTTPError(
                    "https://api.example.com/items",
                    302,
                    "redirect",
                    {"Location": "https://elsewhere.invalid"},
                    None,
                )

            with self.assertRaisesRegex(RemoteApiError, "redirect_rejected"):
                make(redirect).request("items.list")

            def limited(_request, *, timeout):
                raise urllib.error.HTTPError(
                    "https://api.example.com/items",
                    429,
                    "limited",
                    {"Retry-After": "75"},
                    None,
                )

            with self.assertRaises(ConnectorRateLimited) as raised:
                make(limited).request("items.list")
            self.assertEqual(raised.exception.retry_after_seconds, 75)

            with self.assertRaisesRegex(RemoteApiError, "content_type_invalid"):
                make(
                    lambda *_args, **_kwargs: _Response(
                        b"<html>synthetic</html>",
                        content_type="text/html",
                    )
                ).request("items.list")

            with self.assertRaisesRegex(RemoteApiError, "response_too_large"):
                make(
                    lambda *_args, **_kwargs: _Response(b'{"large":"0123456789"}'),
                    max_response_bytes=16,
                ).request("items.list")

    def test_origin_operation_and_json_contracts_are_closed(self):
        with self.assertRaises(ValueError):
            RemoteOperation(
                method="DELETE",
                path_template="/items",
                path_fields=(),
                query_fields=(),
            )
        with self.assertRaises(ValueError):
            RemoteOperation(
                method="GET",
                path_template="https://elsewhere.invalid/items",
                path_fields=(),
                query_fields=(),
            )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            secret = self.private_secret(root)
            with self.assertRaises(ValueError):
                BoundedJsonRail(
                    origin="http://api.example.com",
                    authority_path=secret,
                    authorization_scheme="Bearer",
                    operations={},
                )


if __name__ == "__main__":
    unittest.main()
