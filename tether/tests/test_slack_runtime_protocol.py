import importlib.util
import json
import os
import pathlib
import ssl
import sys
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME_PATH = ROOT / "runtime" / "bridge_runtime.py"


def load_runtime(home: pathlib.Path):
    env = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
    }
    with mock.patch.dict(os.environ, env, clear=False):
        name = f"slack_runtime_protocol_test_{id(home)}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


class Response:
    def __init__(self, status: int, payload: dict, headers=()):
        self.status = status
        self.payload = payload
        self.headers = list(headers)

    def getheaders(self):
        return self.headers

    def read(self, amount=None):
        encoded = json.dumps(self.payload).encode()
        return encoded if amount is None else encoded[:amount]


class Connection:
    def __init__(self, response: Response):
        self.response = response
        self.requests = []

    def request(self, *args, **kwargs):
        self.requests.append((args, kwargs))

    def getresponse(self):
        return self.response

    def close(self):
        return None


class SlackRuntimeProtocolTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)

    def tearDown(self):
        self.temp.cleanup()

    def test_sync_call_honors_retry_after_before_one_retry(self):
        clock = [100.0]
        sleeps = []

        def sleep(delay):
            sleeps.append(delay)
            clock[0] += delay

        coordinator = self.runtime.slack_protocol.RetryAfterCoordinator(
            clock=lambda: clock[0],
            sleep=sleep,
        )
        connections = [
            Connection(
                Response(
                    429,
                    {"ok": False, "error": "ratelimited"},
                    [("Retry-After", "3")],
                )
            ),
            Connection(
                Response(
                    200,
                    {"ok": True, "team_id": "T12345678"},
                )
            ),
        ]
        with mock.patch.object(
            self.runtime,
            "_SLACK_RETRY_COORDINATOR",
            coordinator,
        ), mock.patch.object(
            self.runtime.http.client,
            "HTTPSConnection",
            side_effect=connections,
        ):
            result = self.runtime._slack_call(
                "token",
                "auth.test",
                {},
            )
        self.assertEqual(result["team_id"], "T12345678")
        self.assertEqual(sleeps, [3.0])
        self.assertEqual(
            self.runtime._SLACK_TOKEN_WORKSPACES[
                self.runtime.hashlib.sha256(b"token").hexdigest()
            ],
            "T12345678",
        )
        self.assertEqual(
            sum(len(connection.requests) for connection in connections),
            2,
        )

    def test_slack_tls_context_verifies_hostname_and_certificate(self):
        context = self.runtime._SLACK_TLS_CONTEXT
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertGreaterEqual(
            context.minimum_version,
            ssl.TLSVersion.TLSv1_2,
        )

    def test_second_rate_limit_fails_without_unbounded_retry(self):
        clock = [100.0]
        coordinator = self.runtime.slack_protocol.RetryAfterCoordinator(
            clock=lambda: clock[0],
            sleep=lambda delay: clock.__setitem__(0, clock[0] + delay),
        )
        connections = [
            Connection(
                Response(
                    429,
                    {"ok": False, "error": "ratelimited"},
                    [("Retry-After", "1")],
                )
            ),
            Connection(
                Response(
                    429,
                    {"ok": False, "error": "ratelimited"},
                    [("Retry-After", "1")],
                )
            ),
        ]
        with mock.patch.object(
            self.runtime,
            "_SLACK_RETRY_COORDINATOR",
            coordinator,
        ), mock.patch.object(
            self.runtime.http.client,
            "HTTPSConnection",
            side_effect=connections,
        ), self.assertRaisesRegex(
            RuntimeError,
            "remained active",
        ):
            self.runtime._slack_call(
                "token",
                "conversations.replies",
                {"channel": "C12345678", "ts": "123.456"},
            )

    def test_oversized_response_is_rejected_before_json_decode(self):
        class OversizedResponse(Response):
            def read(self, amount=None):
                return b"x" * (int(amount or 65))

        connection = Connection(OversizedResponse(200, {}))
        with mock.patch.object(
            self.runtime,
            "MAX_SLACK_API_RESPONSE_BYTES",
            64,
        ), mock.patch.object(
            self.runtime.http.client,
            "HTTPSConnection",
            return_value=connection,
        ), self.assertRaisesRegex(
            RuntimeError,
            "exceeds the size limit",
        ):
            self.runtime._slack_call("token", "auth.test", {})

        self.assertEqual(len(connection.requests), 1)


if __name__ == "__main__":
    unittest.main()
