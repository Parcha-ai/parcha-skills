from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest
from urllib.parse import parse_qs, urlsplit


SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.identity_oauth import (  # noqa: E402
    DescopeIdentityOAuth,
    IdentityOAuthError,
)


class Response:
    status = 200

    def __init__(self, body):
        self.body = json.dumps(body).encode()

    def read(self, _limit):
        return self.body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class Opener:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def open(self, request, *, timeout):
        self.calls.append((request, timeout))
        return self.responses.pop(0)


def provider(opener):
    return DescopeIdentityOAuth(
        client_id="synthetic-browser-client",
        client_secret="synthetic-browser-secret",
        redirect_uri="https://recall.synthetic.invalid/admin/oauth/callback/identity",
        canonical_issuer="https://api.descope.com/v1/apps/synthetic-project",
        opener=opener,
    )


class IdentityOAuthTest(unittest.TestCase):
    def test_authorization_uses_pkce_state_and_identity_only_scopes(self):
        url = provider(Opener()).authorization_url(
            state="s" * 43, code_challenge="c" * 43
        )
        query = parse_qs(urlsplit(url).query)

        self.assertEqual(query["response_type"], ["code"])
        self.assertEqual(query["scope"], ["openid email recall.identity"])
        self.assertEqual(query["state"], ["s" * 43])
        self.assertEqual(query["code_challenge"], ["c" * 43])
        self.assertEqual(query["code_challenge_method"], ["S256"])
        self.assertNotIn("resource", query)

    def test_exchange_returns_verified_identity_without_retaining_tokens(self):
        opener = Opener(
            Response(
                {
                    "access_token": "synthetic-access-token",
                    "token_type": "Bearer",
                    "refresh_token": "synthetic-refresh-token",
                }
            ),
            Response(
                {
                    "sub": "descope-user-one",
                    "email": "Owner@Example.com",
                    "email_verified": True,
                }
            ),
        )
        identity = provider(opener).exchange(
            code="synthetic-code", code_verifier="v" * 64
        )

        self.assertEqual(identity.subject, "descope-user-one")
        self.assertEqual(identity.email, "Owner@Example.com")
        token_request, _ = opener.calls[0]
        self.assertIn(b"code_verifier=", token_request.data)
        self.assertNotIn(b"synthetic-access-token", token_request.data)
        user_request, _ = opener.calls[1]
        self.assertEqual(
            user_request.headers["Authorization"],
            "Bearer synthetic-access-token",
        )
        self.assertFalse(hasattr(provider(Opener()), "access_token"))

    def test_unverified_email_fails_closed(self):
        opener = Opener(
            Response({"access_token": "synthetic-access-token"}),
            Response(
                {
                    "sub": "descope-user-one",
                    "email": "owner@example.com",
                    "email_verified": False,
                }
            ),
        )
        with self.assertRaisesRegex(
            IdentityOAuthError, "identity_oauth_identity_invalid"
        ):
            provider(opener).exchange(
                code="synthetic-code", code_verifier="v" * 64
            )

    def test_configuration_rejects_non_https_callback(self):
        with self.assertRaisesRegex(
            IdentityOAuthError, "identity_oauth_configuration_invalid"
        ):
            DescopeIdentityOAuth(
                client_id="synthetic-browser-client",
                client_secret="synthetic-browser-secret",
                redirect_uri="http://localhost/admin/oauth/callback/identity",
                canonical_issuer="https://identity.synthetic.invalid",
            )


if __name__ == "__main__":
    unittest.main()
