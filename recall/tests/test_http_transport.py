from __future__ import annotations

import threading
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from privacy.transport import open_no_redirect


class _SinkHandler(BaseHTTPRequestHandler):
    requests = 0

    def do_GET(self) -> None:
        type(self).requests += 1
        self.send_response(204)
        self.end_headers()

    def log_message(self, _format: str, *_args) -> None:
        return


class _RedirectHandler(BaseHTTPRequestHandler):
    destination = ""

    def do_GET(self) -> None:
        self.send_response(302)
        self.send_header("Location", type(self).destination)
        self.end_headers()

    def log_message(self, _format: str, *_args) -> None:
        return


class AuthenticatedTransportTest(unittest.TestCase):
    def test_redirect_is_rejected_before_authorization_can_reach_destination(self) -> None:
        sink = ThreadingHTTPServer(("127.0.0.1", 0), _SinkHandler)
        redirect = ThreadingHTTPServer(("127.0.0.1", 0), _RedirectHandler)
        _SinkHandler.requests = 0
        _RedirectHandler.destination = f"http://127.0.0.1:{sink.server_port}/capture"
        threads = [
            threading.Thread(target=server.serve_forever, daemon=True)
            for server in (sink, redirect)
        ]
        for thread in threads:
            thread.start()
        try:
            request = urllib.request.Request(
                f"http://127.0.0.1:{redirect.server_port}/start",
                headers={"Authorization": "Bearer synthetic-secret"},
            )
            with self.assertRaises(urllib.error.HTTPError) as raised:
                open_no_redirect(request, timeout=2)
            self.assertEqual(raised.exception.code, 302)
            self.assertEqual(_SinkHandler.requests, 0)
        finally:
            redirect.shutdown()
            sink.shutdown()
            redirect.server_close()
            sink.server_close()


if __name__ == "__main__":
    unittest.main()


class RetryDelayTest(unittest.TestCase):
    def _error(self, retry_after: str | None) -> urllib.error.HTTPError:
        headers = {} if retry_after is None else {"Retry-After": retry_after}
        return urllib.error.HTTPError("https://brain.invalid/", 503, "busy", headers, None)

    def test_exponential_backoff_is_capped_and_jittered(self) -> None:
        from privacy.transport import retry_delay_seconds

        for attempt in range(6):
            delay = retry_delay_seconds(self._error(None), attempt, base_cap=10)
            expected = min(2.0 ** attempt, 10)
            self.assertTrue(expected * 0.75 <= delay <= expected * 1.25, (attempt, delay))

    def test_retry_after_header_extends_the_delay(self) -> None:
        from privacy.transport import retry_delay_seconds

        delay = retry_delay_seconds(self._error("45"), 0, base_cap=10)
        self.assertTrue(45 * 0.75 <= delay <= 45 * 1.25, delay)

    def test_retry_after_never_exceeds_the_ceiling(self) -> None:
        from privacy.transport import retry_delay_seconds

        delay = retry_delay_seconds(self._error("100000"), 0, base_cap=10)
        self.assertLessEqual(delay, 120 * 1.25)

    def test_malformed_retry_after_and_non_http_errors_fall_back(self) -> None:
        from privacy.transport import retry_delay_seconds

        for error in (self._error("soon"), OSError("down"), None):
            delay = retry_delay_seconds(error, 2, base_cap=10)
            self.assertTrue(4 * 0.75 <= delay <= 4 * 1.25, (error, delay))
