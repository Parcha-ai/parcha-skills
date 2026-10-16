"""Real loopback transport checks; no provider or corpus access."""
from __future__ import annotations

import copy
import gzip
import json
import os
import socket
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))
from recall_server.rerank import MAX_RERANK_RESPONSE_BYTES, PooledRerankTransport, RerankRuntime, RerankUnavailable


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        self.connections = 0
        self.requests = []
        self.cookies = []
        self.guard = threading.Lock()
        super().__init__(("127.0.0.1", 0), Handler)

    def get_request(self):
        accepted = super().get_request()
        with self.guard:
            self.connections += 1
        return accepted


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except ConnectionResetError:
            # Refused/oversized responses deliberately discard the connection.
            pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        with self.server.guard:
            self.server.requests.append((self.path, body, self.headers.get("Authorization")))
            self.server.cookies.append(self.headers.get("Cookie"))
        if self.path == "/disconnect":
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            self.close_connection = True
            return
        if self.path == "/slow-headers":
            time.sleep(.15)
        payload = json.dumps({"data": [{"index": 0, "relevance_score": .75}]}).encode()
        if self.path == "/invalid":
            payload = b"not json"
        if self.path == "/oversized-body":
            payload = b"x" * (MAX_RERANK_RESPONSE_BYTES + 1)
        if self.path == "/compressed":
            payload = gzip.compress(b"x" * (MAX_RERANK_RESPONSE_BYTES * 10))
        status = 302 if self.path == "/redirect" else 429 if self.path == "/rate-limit" else 503 if self.path == "/unavailable" else 200
        self.send_response(status)
        if self.path == "/cookie":
            self.send_header("Set-Cookie", "session=synthetic; Path=/")
        if self.path == "/compressed":
            self.send_header("Content-Encoding", "gzip")
        if status == 302:
            self.send_header("Location", "/target")
        if status in (429, 503):
            self.send_header("Retry-After", "0")
        if self.path == "/oversized-header":
            self.send_header("Content-Length", str(MAX_RERANK_RESPONSE_BYTES + 1))
        elif self.path == "/oversized-body":
            self.send_header("Connection", "close")
            self.close_connection = True
        else:
            self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.path == "/slow-body":
            time.sleep(.15)
        try:
            if self.path == "/trickle":
                for byte in payload:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(.02)
                return
            self.wfile.write(payload)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_CONNECT(self):
        with self.server.guard:
            self.server.requests.append((self.path, b"", self.headers.get("Authorization")))
        self.send_response(502)
        self.send_header("Content-Length", "0")
        self.end_headers()


class KeepaliveTest(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.env = mock.patch.dict(os.environ, {"KEEPALIVE_TEST_KEY": "synthetic-first"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def runtime(self, path="/ok"):
        runtime = RerankRuntime(protocol="voyage", url=f"http://127.0.0.1:{self.server.server_port}{path}", key_env="KEEPALIVE_TEST_KEY")
        close = getattr(runtime.transport, "close", None)
        if close:
            self.addCleanup(close)
        return runtime

    def test_copied_runtime_reuses_connection_without_reusing_credentials_or_body(self):
        first = self.runtime()
        self.assertEqual(first.rerank("one", ["first"]), [(0, .75)])
        os.environ["KEEPALIVE_TEST_KEY"] = "synthetic-second"
        second = copy.copy(first)  # The actual passage owner copies the runtime.
        self.assertEqual(second.rerank("two", ["second"]), [(0, .75)])
        self.assertEqual(self.server.connections, 1)
        self.assertEqual([row[2] for row in self.server.requests], ["Bearer synthetic-first", "Bearer synthetic-second"])
        self.assertEqual([json.loads(row[1])["documents"] for row in self.server.requests], [["first"], ["second"]])

    def test_refusals_are_one_attempt_and_keep_safe_codes(self):
        for path, code in (("/redirect", "rerank_redirect_refused"), ("/rate-limit", "rerank_http_status"), ("/unavailable", "rerank_http_status"), ("/disconnect", "rerank_transport_error"), ("/oversized-header", "rerank_response_too_large"), ("/oversized-body", "rerank_response_too_large"), ("/invalid", "rerank_response_invalid")):
            with self.subTest(path=path):
                before = len(self.server.requests)
                with self.assertRaises(RerankUnavailable) as caught:
                    self.runtime(path).rerank("private-test-query", ["synthetic-body"], deadline_seconds=.5)
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(str(caught.exception), code)
                self.assertEqual(len(self.server.requests), before + 1)
        self.assertNotIn("/target", [row[0] for row in self.server.requests])

    def test_header_and_body_stalls_obey_per_call_timeout_without_retry(self):
        for path in ("/slow-headers", "/slow-body"):
            with self.subTest(path=path):
                before = len(self.server.requests)
                runtime = self.runtime(path)
                started = time.monotonic()
                with self.assertRaises(RerankUnavailable):
                    runtime.rerank("q", ["d"], deadline_seconds=.05)
                self.assertLess(time.monotonic() - started, .14)
                self.assertEqual(len(self.server.requests), before + 1)

    def test_concurrent_copies_do_not_mix_request_bodies(self):
        runtime = self.runtime()
        with ThreadPoolExecutor(max_workers=4) as executor:
            replies = list(executor.map(lambda i: copy.copy(runtime).rerank(f"q{i}", [f"d{i}"]), range(8)))
        self.assertEqual(replies, [[(0, .75)]] * 8)
        bodies = [json.loads(row[1]) for row in self.server.requests]
        self.assertEqual(sorted((body["query"], body["documents"]) for body in bodies), [(f"q{i}", [f"d{i}"]) for i in range(8)])
        self.assertLessEqual(self.server.connections, 4)

    def test_failed_response_does_not_poison_subsequent_request(self):
        runtime = self.runtime("/oversized-header")
        with self.assertRaises(RerankUnavailable):
            runtime.rerank("bad", ["d"])
        runtime.url = f"http://127.0.0.1:{self.server.server_port}/ok"
        self.assertEqual(runtime.rerank("good", ["d"]), [(0, .75)])
        self.assertEqual(len(self.server.requests), 2)

    def test_expired_deadline_never_dispatches(self):
        with self.assertRaises(RerankUnavailable) as caught:
            self.runtime().rerank("q", ["d"], deadline_seconds=0)
        self.assertEqual(caught.exception.code, "rerank_deadline_exhausted")
        self.assertEqual(self.server.requests, [])

    def test_environment_proxy_routing_and_no_proxy(self):
        proxy = Server()
        thread = threading.Thread(target=proxy.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
        thread.start()
        self.addCleanup(proxy.server_close)
        self.addCleanup(proxy.shutdown)
        proxy_url = f"http://127.0.0.1:{proxy.server_port}"
        url = f"http://127.0.0.1:{self.server.server_port}/ok"
        for bypass in (False, True):
            with self.subTest(bypass=bypass), mock.patch.dict(os.environ, {"HTTP_PROXY": proxy_url, "HTTPS_PROXY": proxy_url, "NO_PROXY": "127.0.0.1" if bypass else ""}, clear=True):
                transport = PooledRerankTransport()
                try:
                    self.assertEqual(transport.post(url=url, headers={}, body={}, timeout=.5)["data"][0]["index"], 0)
                finally:
                    transport.close()
        self.assertEqual(len(proxy.requests), 1)
        self.assertEqual(proxy.requests[0][0], url)
        self.assertEqual(len(self.server.requests), 1)
        with mock.patch.dict(os.environ, {"HTTPS_PROXY": proxy_url}, clear=True):
            transport = PooledRerankTransport()
            try:
                with self.assertRaises(RerankUnavailable):
                    transport.post(url="https://origin.invalid/v1/rerank", headers={}, body={}, timeout=.5)
            finally:
                transport.close()
        self.assertEqual([row[0] for row in proxy.requests], [url, "origin.invalid:443"])

    def test_stream_total_deadline_and_compression_remain_bounded(self):
        runtime = self.runtime("/trickle")
        started = time.monotonic()
        with self.assertRaises(RerankUnavailable) as caught:
            runtime.rerank("q", ["d"], deadline_seconds=.05)
        self.assertEqual(caught.exception.code, "rerank_deadline_exhausted")
        self.assertLess(time.monotonic() - started, .14)
        with self.assertRaises(RerankUnavailable) as caught:
            self.runtime("/compressed").rerank("q", ["d"])
        # Raw bytes are never decompressed into the advertised bomb.
        self.assertEqual(caught.exception.code, "rerank_response_invalid")

    def test_pool_wait_uses_the_call_timeout(self):
        import httpx
        transport = PooledRerankTransport()
        transport.close()
        transport._client = httpx.Client(limits=httpx.Limits(max_connections=1))
        self.addCleanup(transport.close)
        url = f"http://127.0.0.1:{self.server.server_port}/slow-body"
        with transport._client.stream("POST", url, content=b"{}"):
            started = time.monotonic()
            with self.assertRaises(RerankUnavailable) as caught:
                transport.post(url=url, headers={}, body={}, timeout=.05)
            self.assertEqual(caught.exception.code, "rerank_transport_error")
            self.assertLess(time.monotonic() - started, .14)
        self.assertEqual(len(self.server.requests), 1)

    def test_store_shutdown_closes_runtime_pool_even_after_audit_failure(self):
        from recall_server.db import BrainStore
        runtime = self.runtime()
        store = BrainStore("postgresql://localhost/recall_test", rerank_runtime=runtime)
        with mock.patch.object(store.audit_batcher, "close", side_effect=RuntimeError("synthetic")):
            with self.assertRaises(RuntimeError):
                store.close()
        self.assertTrue(runtime.transport._client.is_closed)

    def test_provider_cookies_are_not_retained_between_calls(self):
        runtime = self.runtime("/cookie")
        runtime.rerank("first", ["d"])
        runtime.rerank("second", ["d"])
        self.assertEqual(self.server.cookies, [None, None])
        self.assertEqual(len(runtime.transport._client.cookies), 0)


if __name__ == "__main__":
    unittest.main()
