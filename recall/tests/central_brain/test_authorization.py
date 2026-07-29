from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import sys
import time
import unittest
from unittest import mock

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.authorization import OidcJwtVerifier  # noqa: E402


ISSUER = "https://identity.synthetic.invalid/oauth"
AUDIENCE = "https://recall.synthetic.invalid/mcp"
JWKS_URI = "https://identity.synthetic.invalid/jwks.json"


def b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


class StaticJwks:
    def __init__(self, value: dict) -> None:
        self.value = value
        self.calls = 0

    def load(self) -> dict:
        self.calls += 1
        return self.value


class OidcVerifierTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public = cls.private_key.public_key().public_numbers()
        cls.jwks = {
            "keys": [{
                "kty": "RSA",
                "kid": "synthetic-key-1",
                "alg": "RS256",
                "use": "sig",
                "n": b64(public.n.to_bytes((public.n.bit_length() + 7) // 8, "big")),
                "e": b64(public.e.to_bytes((public.e.bit_length() + 7) // 8, "big")),
            }]
        }

    def token(self, **overrides) -> str:
        now = int(time.time())
        claims = {
            "iss": ISSUER,
            "sub": "subject-synthetic-owner",
            "aud": AUDIENCE,
            "scope": "openid email read",
            "iat": now - 10,
            "nbf": now - 10,
            "exp": now + 300,
            "email": "owner@synthetic.invalid",
            "email_verified": True,
        }
        claims.update(overrides)
        header = {"alg": "RS256", "kid": "synthetic-key-1", "typ": "at+jwt"}
        encoded = ".".join((
            b64(json.dumps(header, separators=(",", ":")).encode()),
            b64(json.dumps(claims, separators=(",", ":")).encode()),
        ))
        signature = self.private_key.sign(
            encoded.encode(), padding.PKCS1v15(), hashes.SHA256()
        )
        return f"{encoded}.{b64(signature)}"

    def verifier(self, loader: StaticJwks | None = None) -> OidcJwtVerifier:
        return OidcJwtVerifier(
            issuer=ISSUER,
            audience=AUDIENCE,
            jwks_uri=JWKS_URI,
            jwks_loader=loader or StaticJwks(self.jwks),
        )

    def test_verified_identity_is_exact_resource_scoped_and_email_verified(self) -> None:
        loader = StaticJwks(self.jwks)
        identity = self.verifier(loader).verify(self.token())

        self.assertIsNotNone(identity)
        self.assertEqual(identity.issuer, ISSUER)
        self.assertEqual(identity.audience, AUDIENCE)
        self.assertEqual(identity.subject, "subject-synthetic-owner")
        self.assertEqual(identity.scopes, ("email", "openid", "read"))
        self.assertEqual(identity.email, "owner@synthetic.invalid")
        self.assertTrue(identity.email_verified)
        self.assertEqual(loader.calls, 1)

    def test_wrong_issuer_audience_time_scope_key_and_signature_fail_closed(self) -> None:
        now = int(time.time())
        cases = (
            self.token(iss="https://other.synthetic.invalid"),
            self.token(aud="https://other.synthetic.invalid/mcp"),
            self.token(exp=now - 1),
            self.token(nbf=now + 300),
            self.token(scope="openid email"),
        )
        verifier = self.verifier()
        for token in cases:
            with self.subTest(token=token[:16]):
                self.assertIsNone(verifier.verify(token))

        other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pieces = self.token().split(".")
        bad_signature = other.sign(
            f"{pieces[0]}.{pieces[1]}".encode(),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        self.assertIsNone(
            verifier.verify(f"{pieces[0]}.{pieces[1]}.{b64(bad_signature)}")
        )
        self.assertIsNone(
            OidcJwtVerifier(
                issuer=ISSUER,
                audience=AUDIENCE,
                jwks_uri=JWKS_URI,
                jwks_loader=StaticJwks({"keys": []}),
            ).verify(self.token())
        )

    def test_descope_is_a_preset_over_the_same_generic_verifier(self) -> None:
        environment = {
            "RECALL_MCP_AUTH_PROVIDER": "descope",
            "RECALL_OIDC_ISSUER": ISSUER,
            "RECALL_OIDC_JWKS_URI": JWKS_URI,
            "RECALL_MCP_RESOURCE_URI": AUDIENCE,
            "RECALL_AUTHORIZATION_SERVERS": ISSUER,
            "DESCOPE_MANAGEMENT_KEY": "must-not-be-read-by-resource-server",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            verifier = OidcJwtVerifier.from_env()
        self.assertIsInstance(verifier, OidcJwtVerifier)
        self.assertEqual(verifier.provider, "descope")
        self.assertFalse(hasattr(verifier, "management_key"))

    def test_configuration_requires_exact_https_and_declared_issuer(self) -> None:
        with self.assertRaises(ValueError):
            OidcJwtVerifier(
                issuer="http://identity.invalid",
                audience=AUDIENCE,
                jwks_uri=JWKS_URI,
            )
        with mock.patch.dict(os.environ, {
            "RECALL_MCP_AUTH_PROVIDER": "oidc",
            "RECALL_OIDC_ISSUER": ISSUER,
            "RECALL_OIDC_JWKS_URI": JWKS_URI,
            "RECALL_MCP_RESOURCE_URI": AUDIENCE,
            "RECALL_AUTHORIZATION_SERVERS": "https://other.synthetic.invalid",
        }, clear=True):
            with self.assertRaisesRegex(
                RuntimeError, "OIDC issuer must be an authorization server"
            ):
                OidcJwtVerifier.from_env()


if __name__ == "__main__":
    unittest.main()
