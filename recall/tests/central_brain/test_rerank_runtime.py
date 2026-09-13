from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import time
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.rerank import (  # noqa: E402
    DEFAULT_MAX_CANDIDATES,
    DEFAULT_MAX_DOC_CHARS,
    DEFAULT_TIMEOUT_SECONDS,
    MAX_RERANK_RESPONSE_BYTES,
    PROVIDERS,
    RerankRuntime,
    RerankUnavailable,
    UrllibRerankTransport,
    build_rerank_runtime,
)


class FakeTransport:
    """Records the request and returns a canned payload (or raises)."""

    def __init__(self, payload=None, *, error=None, delay: float = 0.0):
        self.payload = payload
        self.error = error
        self.delay = delay
        self.calls: list[dict] = []

    def post(self, *, url, headers, body, timeout):
        self.calls.append(
            {"url": url, "headers": dict(headers), "body": body, "timeout": timeout}
        )
        if self.delay:
            time.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return self.payload


def voyage_payload(*rows):
    return {
        "object": "list",
        "data": [{"index": i, "relevance_score": s} for i, s in rows],
        "model": "rerank-2.5",
        "usage": {"total_tokens": 1},
    }


def cohere_payload(*rows):
    return {
        "id": "abc",
        "results": [{"index": i, "relevance_score": s} for i, s in rows],
        "meta": {"api_version": {"version": "2"}},
    }


class RerankRuntimeContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.env = mock.patch.dict(os.environ, {"RERANK_TEST_KEY": "sk-test"}, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)

    def runtime(self, protocol: str = "voyage", transport=None, **overrides) -> RerankRuntime:
        kwargs = {"protocol": protocol, "key_env": "RERANK_TEST_KEY", "transport": transport}
        kwargs.update(overrides)
        return RerankRuntime(**kwargs)

    # -- construction --------------------------------------------------------

    def test_rerank_requires_https_and_exact_approved_url(self) -> None:
        with self.assertRaisesRegex(ValueError, "HTTPS"):
            self.runtime(
                url="http://rerank.example/v1/rerank",
                approved_url="http://rerank.example/v1/rerank",
            )
        with self.assertRaisesRegex(ValueError, "requires an approved rerank endpoint"):
            self.runtime(url="https://rerank.example/v1/rerank")
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.runtime(
                url="https://rerank.example/v1/rerank",
                approved_url="https://other.example/v1/rerank",
            )
        with self.assertRaisesRegex(ValueError, "plain URL"):
            self.runtime(
                url="https://user:pw@rerank.example/v1/rerank",
                approved_url="https://user:pw@rerank.example/v1/rerank",
            )
        approved = self.runtime(
            url="https://rerank.example/v1/rerank/",
            approved_url="https://rerank.example/v1/rerank",
        )
        self.assertEqual(approved.url, "https://rerank.example/v1/rerank")
        # Provider defaults are approved by construction.
        self.assertEqual(self.runtime().url, PROVIDERS["voyage"]["url"])
        self.assertEqual(self.runtime("cohere").url, PROVIDERS["cohere"]["url"])
        # Loopback may use plain HTTP without approval (local proxy / tests).
        self.assertEqual(
            self.runtime(url="http://127.0.0.1:8099/rerank").url,
            "http://127.0.0.1:8099/rerank",
        )

    def test_rerank_rejects_unknown_protocol_and_bad_settings(self) -> None:
        with self.assertRaisesRegex(ValueError, "voyage or cohere"):
            RerankRuntime(protocol="bogus", key_env="RERANK_TEST_KEY")
        with self.assertRaisesRegex(ValueError, "requires a key source"):
            RerankRuntime(protocol="voyage")
        with self.assertRaisesRegex(ValueError, "mutually exclusive"):
            RerankRuntime(protocol="voyage", key_env="RERANK_TEST_KEY", key_file="/x")
        with self.assertRaisesRegex(ValueError, "variable name is invalid"):
            RerankRuntime(protocol="voyage", key_env="lowercase")
        with self.assertRaisesRegex(ValueError, "timeout"):
            self.runtime(timeout_seconds=0)
        with self.assertRaisesRegex(ValueError, "max candidates"):
            self.runtime(max_candidates=0)
        with self.assertRaisesRegex(ValueError, "document chars"):
            self.runtime(max_doc_chars=10)
        with self.assertRaisesRegex(ValueError, "model"):
            self.runtime(model="bad model!")

    def test_rerank_key_file_must_be_owner_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rerank.key"
            path.write_text("sk-file\n")
            os.chmod(path, 0o644)
            transport = FakeTransport(voyage_payload((0, 0.9)))
            runtime = RerankRuntime(protocol="voyage", key_file=str(path), transport=transport)
            with self.assertRaises(RerankUnavailable) as ctx:
                runtime.rerank("q", ["doc"])
            self.assertEqual(ctx.exception.code, "rerank_key_unavailable")
            self.assertEqual(transport.calls, [])  # no request without a valid key
            os.chmod(path, 0o600)
            self.assertEqual(runtime.rerank("q", ["doc"]), [(0, 0.9)])
            self.assertEqual(transport.calls[0]["headers"]["Authorization"], "Bearer sk-file")
            # Symlinks are refused even when the target is owner-only.
            link = Path(tmp) / "link.key"
            link.symlink_to(path)
            linked = RerankRuntime(protocol="voyage", key_file=str(link), transport=transport)
            with self.assertRaises(RerankUnavailable):
                linked.rerank("q", ["doc"])

    def test_rerank_key_env_missing_is_unavailable_not_crash(self) -> None:
        transport = FakeTransport(voyage_payload((0, 0.9)))
        runtime = RerankRuntime(protocol="voyage", key_env="RERANK_ABSENT", transport=transport)
        with self.assertRaises(RerankUnavailable) as ctx:
            runtime.rerank("q", ["doc"])
        self.assertEqual(ctx.exception.code, "rerank_key_unavailable")
        self.assertEqual(transport.calls, [])

    # -- transport hygiene ---------------------------------------------------

    def test_rerank_rejects_redirects_and_oversized_responses(self) -> None:
        transport = UrllibRerankTransport()

        class Redirecting:
            status = 302
            headers = {"Location": "https://evil.example/"}

        def redirect_open(request, timeout):
            raise urllib.error.HTTPError(
                request.full_url, 302, "Found", {"Location": "https://evil.example/"}, io.BytesIO(b"")
            )

        with mock.patch("urllib.request.OpenerDirector.open", side_effect=redirect_open):
            with self.assertRaises(RerankUnavailable) as ctx:
                transport.post(url="https://api.voyageai.com/v1/rerank", headers={}, body={}, timeout=1)
        self.assertEqual(ctx.exception.code, "rerank_redirect_refused")

        class BigResponse:
            status = 200
            headers = {"Content-Length": str(MAX_RERANK_RESPONSE_BYTES + 1)}

            def read(self, n=-1):
                return b"{}"

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        with mock.patch("urllib.request.OpenerDirector.open", return_value=BigResponse()):
            with self.assertRaises(RerankUnavailable) as ctx:
                transport.post(url="https://api.voyageai.com/v1/rerank", headers={}, body={}, timeout=1)
        self.assertEqual(ctx.exception.code, "rerank_response_too_large")

        class ChunkedBig(BigResponse):
            headers = {}

            def read(self, n=-1):
                return b"x" * n

        with mock.patch("urllib.request.OpenerDirector.open", return_value=ChunkedBig()):
            with self.assertRaises(RerankUnavailable) as ctx:
                transport.post(url="https://api.voyageai.com/v1/rerank", headers={}, body={}, timeout=1)
        self.assertEqual(ctx.exception.code, "rerank_response_too_large")

        class NotJson(BigResponse):
            headers = {}

            def read(self, n=-1):
                return b"<html>"

        with mock.patch("urllib.request.OpenerDirector.open", return_value=NotJson()):
            with self.assertRaises(RerankUnavailable) as ctx:
                transport.post(url="https://api.voyageai.com/v1/rerank", headers={}, body={}, timeout=1)
        self.assertEqual(ctx.exception.code, "rerank_response_invalid")

        with mock.patch(
            "urllib.request.OpenerDirector.open",
            side_effect=urllib.error.URLError("refused"),
        ):
            with self.assertRaises(RerankUnavailable) as ctx:
                transport.post(url="https://api.voyageai.com/v1/rerank", headers={}, body={}, timeout=1)
        self.assertEqual(ctx.exception.code, "rerank_transport_error")

    def test_rerank_default_transport_refuses_redirect_handler(self) -> None:
        from recall_server.rerank import _RejectRedirect

        self.assertIsNone(
            _RejectRedirect().redirect_request(None, None, 302, "Found", {}, "https://x")
        )

    # -- payload shapes ------------------------------------------------------

    def test_rerank_voyage_and_cohere_payload_shapes(self) -> None:
        docs = ["alpha", "beta", "gamma"]
        voyage = FakeTransport(voyage_payload((2, 0.1), (0, 0.9), (1, 0.5)))
        result = self.runtime("voyage", voyage).rerank("query", docs)
        self.assertEqual(result, [(0, 0.9), (1, 0.5), (2, 0.1)])
        body = voyage.calls[0]["body"]
        self.assertEqual(body["model"], "rerank-2.5")
        self.assertEqual(body["documents"], docs)
        self.assertEqual(body["top_k"], 3)
        self.assertTrue(body["truncation"])
        self.assertEqual(voyage.calls[0]["url"], "https://api.voyageai.com/v1/rerank")
        self.assertEqual(voyage.calls[0]["headers"]["Authorization"], "Bearer sk-test")

        cohere = FakeTransport(cohere_payload((1, 0.7), (0, 0.2)))
        result = self.runtime("cohere", cohere).rerank("query", docs, top_k=2)
        self.assertEqual(result, [(1, 0.7), (0, 0.2)])
        body = cohere.calls[0]["body"]
        self.assertEqual(body["model"], "rerank-v3.5")
        self.assertEqual(body["top_n"], 2)
        self.assertNotIn("top_k", body)
        self.assertEqual(cohere.calls[0]["url"], "https://api.cohere.com/v2/rerank")

        # Cross-shape responses are rejected, not silently accepted.
        wrong = FakeTransport(cohere_payload((0, 0.9)))
        with self.assertRaises(RerankUnavailable) as ctx:
            self.runtime("voyage", wrong).rerank("query", docs)
        self.assertEqual(ctx.exception.code, "rerank_response_invalid")

    def test_rerank_returns_indices_sorted_and_rejects_out_of_range(self) -> None:
        docs = ["a", "b"]
        for bad in (
            voyage_payload((2, 0.5)),
            voyage_payload((-1, 0.5)),
            voyage_payload((True, 0.5)),
            voyage_payload(("0", 0.5)),
        ):
            with self.assertRaises(RerankUnavailable) as ctx:
                self.runtime("voyage", FakeTransport(bad)).rerank("q", docs)
            self.assertEqual(ctx.exception.code, "rerank_index_out_of_range")
        for bad in (
            voyage_payload((0, 0.5), (0, 0.4)),  # duplicate index
            voyage_payload((0, "high")),
            voyage_payload((0, float("nan"))),
            voyage_payload((0, 0.5), (1, 0.4), (1, 0.3)),  # more rows than docs
            {"data": "nope"},
            [],
            {"data": [{"index": 0}]},
        ):
            with self.assertRaises(RerankUnavailable) as ctx:
                self.runtime("voyage", FakeTransport(bad)).rerank("q", docs)
            self.assertIn(ctx.exception.code, {"rerank_response_invalid", "rerank_index_out_of_range"})
        # Provider order is not trusted: scores decide, ties break by index.
        result = self.runtime("voyage", FakeTransport(voyage_payload((1, 0.5), (0, 0.5)))).rerank("q", docs)
        self.assertEqual(result, [(0, 0.5), (1, 0.5)])
        # A partial (top_k) response is fine.
        result = self.runtime("voyage", FakeTransport(voyage_payload((1, 0.5)))).rerank("q", docs, top_k=1)
        self.assertEqual(result, [(1, 0.5)])

    def test_rerank_truncates_documents_and_caps_candidates(self) -> None:
        transport = FakeTransport(voyage_payload((0, 0.9)))
        runtime = self.runtime("voyage", transport, max_candidates=2, max_doc_chars=64)
        long_doc = "x" * 500
        result = runtime.rerank("q", [long_doc, "short", "dropped"])
        self.assertEqual(result, [(0, 0.9)])
        sent = transport.calls[0]["body"]["documents"]
        self.assertEqual(len(sent), 2)
        self.assertEqual(sent[0], "x" * 64)
        self.assertEqual(sent[1], "short")
        # Indexes above the sent batch are out of range even if the caller passed more docs.
        with self.assertRaises(RerankUnavailable):
            self.runtime("voyage", FakeTransport(voyage_payload((2, 0.1))), max_candidates=2).rerank(
                "q", ["a", "b", "c"]
            )
        self.assertEqual(runtime.rerank("q", []), [])
        self.assertEqual(self.runtime().max_candidates, DEFAULT_MAX_CANDIDATES)
        self.assertEqual(self.runtime().max_doc_chars, DEFAULT_MAX_DOC_CHARS)

    def test_rerank_rejects_bad_query_documents_and_top_k(self) -> None:
        runtime = self.runtime("voyage", FakeTransport(voyage_payload((0, 0.9))))
        for query in ("", "   ", None):
            with self.assertRaises(RerankUnavailable):
                runtime.rerank(query, ["doc"])
        with self.assertRaises(RerankUnavailable):
            runtime.rerank("q", ["doc", 3])
        with self.assertRaises(RerankUnavailable):
            runtime.rerank("q", ["doc"], top_k=0)

    # -- timing --------------------------------------------------------------

    def test_rerank_timeout_and_deadline_via_injected_transport(self) -> None:
        transport = FakeTransport(voyage_payload((0, 0.9)))
        runtime = self.runtime("voyage", transport, timeout_seconds=2.5)
        runtime.rerank("q", ["doc"])
        self.assertEqual(transport.calls[0]["timeout"], 2.5)
        runtime.rerank("q", ["doc"], deadline_seconds=0.4)
        self.assertEqual(transport.calls[1]["timeout"], 0.4)
        runtime.rerank("q", ["doc"], deadline_seconds=9.0)
        self.assertEqual(transport.calls[2]["timeout"], 2.5)
        with self.assertRaises(RerankUnavailable) as ctx:
            runtime.rerank("q", ["doc"], deadline_seconds=0)
        self.assertEqual(ctx.exception.code, "rerank_deadline_exhausted")
        self.assertEqual(len(transport.calls), 3)

        timing_out = FakeTransport(error=TimeoutError("read timed out"))
        with self.assertRaises(RerankUnavailable) as ctx:
            self.runtime("voyage", timing_out).rerank("q", ["doc"])
        self.assertEqual(ctx.exception.code, "rerank_transport_error")

        slow = FakeTransport(voyage_payload((0, 0.9)), delay=0.15)
        with self.assertRaises(RerankUnavailable) as ctx:
            self.runtime("voyage", slow, timeout_seconds=0.1).rerank("q", ["doc"])
        self.assertEqual(ctx.exception.code, "rerank_deadline_exhausted")

        http_error = FakeTransport(
            error=urllib.error.HTTPError("https://api.voyageai.com/v1/rerank", 429, "Too Many", {}, None)
        )
        with self.assertRaises(RerankUnavailable):
            self.runtime("voyage", http_error).rerank("q", ["doc"])
        self.assertEqual(len(http_error.calls), 1)  # no retry on the query path

        exploding = FakeTransport(error=RuntimeError("provider said: secret sk-test"))
        with self.assertRaises(RerankUnavailable) as ctx:
            self.runtime("voyage", exploding).rerank("q", ["doc"])
        self.assertNotIn("sk-test", str(ctx.exception))

    # -- identity ------------------------------------------------------------

    def test_rerank_fingerprint_tracks_protocol_model_and_truncation(self) -> None:
        base = self.runtime("voyage").fingerprint
        self.assertEqual(len(base), 64)
        self.assertEqual(base, self.runtime("voyage").fingerprint)
        self.assertNotEqual(base, self.runtime("cohere").fingerprint)
        self.assertNotEqual(base, self.runtime("voyage", model="rerank-2").fingerprint)
        self.assertNotEqual(base, self.runtime("voyage", max_doc_chars=1000).fingerprint)
        self.assertEqual(base, self.runtime("voyage", timeout_seconds=1.0).fingerprint)

    # -- factory -------------------------------------------------------------

    def test_factory_returns_none_when_off_and_runtime_when_configured(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(build_rerank_runtime())
        with mock.patch.dict(os.environ, {"RECALL_RERANK_PROTOCOL": "off"}, clear=True):
            self.assertIsNone(build_rerank_runtime())
        with mock.patch.dict(os.environ, {"RECALL_RERANK_PROTOCOL": "OFF"}, clear=True):
            self.assertIsNone(build_rerank_runtime())
        with mock.patch.dict(os.environ, {"RECALL_RERANK_PROTOCOL": "bogus"}, clear=True):
            with self.assertRaisesRegex(ValueError, "RECALL_RERANK_PROTOCOL"):
                build_rerank_runtime()
        with mock.patch.dict(os.environ, {"RECALL_RERANK_PROTOCOL": "voyage"}, clear=True):
            with self.assertRaisesRegex(ValueError, "key source"):
                build_rerank_runtime()
        env = {
            "RECALL_RERANK_PROTOCOL": "voyage",
            "RECALL_RERANK_KEY_ENV": "VOYAGE_API_KEY",
            "VOYAGE_API_KEY": "sk-live",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            runtime = build_rerank_runtime()
        self.assertIsInstance(runtime, RerankRuntime)
        self.assertEqual(runtime.protocol, "voyage")
        self.assertEqual(runtime.model, "rerank-2.5")
        self.assertEqual(runtime.url, PROVIDERS["voyage"]["url"])
        self.assertEqual(runtime.timeout_seconds, DEFAULT_TIMEOUT_SECONDS)
        self.assertEqual(runtime.max_candidates, DEFAULT_MAX_CANDIDATES)
        self.assertEqual(runtime.max_doc_chars, DEFAULT_MAX_DOC_CHARS)
        env = {
            "RECALL_RERANK_PROTOCOL": "cohere",
            "RECALL_RERANK_MODEL": "rerank-english-v3.0",
            "RECALL_RERANK_URL": "https://gateway.example/v2/rerank",
            "RECALL_RERANK_APPROVED_URL": "https://gateway.example/v2/rerank",
            "RECALL_RERANK_KEY_ENV": "COHERE_API_KEY",
            "RECALL_RERANK_TIMEOUT_SECONDS": "1.2",
            "RECALL_RERANK_MAX_CANDIDATES": "20",
            "RECALL_RERANK_MAX_DOC_CHARS": "800",
        }
        with mock.patch.dict(os.environ, env, clear=True):
            runtime = build_rerank_runtime()
        self.assertEqual(runtime.protocol, "cohere")
        self.assertEqual(runtime.model, "rerank-english-v3.0")
        self.assertEqual(runtime.url, "https://gateway.example/v2/rerank")
        self.assertEqual(runtime.timeout_seconds, 1.2)
        self.assertEqual(runtime.max_candidates, 20)
        self.assertEqual(runtime.max_doc_chars, 800)
        env["RECALL_RERANK_APPROVED_URL"] = ""
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(ValueError, "approved"):
                build_rerank_runtime()
        env["RECALL_RERANK_APPROVED_URL"] = env["RECALL_RERANK_URL"]
        env["RECALL_RERANK_MAX_CANDIDATES"] = "many"
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(ValueError, "RECALL_RERANK_MAX_CANDIDATES"):
                build_rerank_runtime()

    def test_brainstore_carries_optional_rerank_runtime(self) -> None:
        from recall_server.db import BrainStore

        store = BrainStore("postgresql://localhost/recall_test")
        self.assertIsNone(store.rerank_runtime)
        runtime = self.runtime()
        store = BrainStore("postgresql://localhost/recall_test", rerank_runtime=runtime)
        self.assertIs(store.rerank_runtime, runtime)

    def test_transport_body_is_compact_json(self) -> None:
        # Shape check on the wire format without a network: the default
        # transport serialises with the same compact separators as semantic.py.
        captured = {}

        class Capture:
            status = 200
            headers = {}

            def read(self, n=-1):
                return json.dumps(voyage_payload((0, 0.5))).encode()

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        def fake_open(self_, request, timeout=None):
            captured["data"] = request.data
            captured["headers"] = dict(request.header_items())
            captured["timeout"] = timeout
            return Capture()

        with mock.patch("urllib.request.OpenerDirector.open", fake_open):
            payload = UrllibRerankTransport().post(
                url="https://api.voyageai.com/v1/rerank",
                headers={"Authorization": "Bearer sk-test"},
                body={"query": "q", "documents": ["d"]},
                timeout=0.7,
            )
        self.assertEqual(payload["data"][0]["index"], 0)
        self.assertEqual(captured["data"], b'{"query":"q","documents":["d"]}')
        self.assertEqual(captured["timeout"], 0.7)
        self.assertEqual(captured["headers"]["Authorization"], "Bearer sk-test")
        self.assertEqual(captured["headers"]["Content-type"], "application/json")


if __name__ == "__main__":
    unittest.main()


class RerankMinBudgetEnvTests(unittest.TestCase):
    def test_min_budget_defaults_and_validates(self) -> None:
        from recall_server.rerank import (
            DEFAULT_RERANK_MIN_BUDGET_SECONDS,
            rerank_min_budget_seconds_from_env,
        )

        with mock.patch.dict(os.environ, {"RECALL_RERANK_MIN_BUDGET_SECONDS": ""}):
            self.assertEqual(rerank_min_budget_seconds_from_env(), DEFAULT_RERANK_MIN_BUDGET_SECONDS)
        with mock.patch.dict(os.environ, {"RECALL_RERANK_MIN_BUDGET_SECONDS": "0.4"}):
            self.assertEqual(rerank_min_budget_seconds_from_env(), 0.4)
        for bad in ("0", "31", "nan", "soon"):
            with mock.patch.dict(os.environ, {"RECALL_RERANK_MIN_BUDGET_SECONDS": bad}):
                with self.assertRaises(ValueError):
                    rerank_min_budget_seconds_from_env()
