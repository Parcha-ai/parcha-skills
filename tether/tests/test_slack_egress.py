"""SlackEgress.upload: the three-call external upload flow, driven through a fake opener."""

from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from runtime.plugin_next.slack_egress import SlackEgress, SlackError  # noqa: E402


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class UploadTests(unittest.TestCase):
    def test_upload_posts_bytes_and_returns_the_share_ts(self):
        seen = []

        def opener(request, timeout=0):
            url = request.full_url
            seen.append((url, request.get_method(), request.data))
            if "files.getUploadURLExternal" in url:
                return _Response(json.dumps({"ok": True, "upload_url": "https://files.slack.com/u/abc", "file_id": "F9"}).encode())
            if url == "https://files.slack.com/u/abc":
                return _Response(b"OK - 12")
            if "files.completeUploadExternal" in url:
                body = json.loads(request.data)
                assert body["channel_id"] == "C1" and body["thread_ts"] == "100.1" and body["initial_comment"] == "demo"
                return _Response(json.dumps({"ok": True, "files": [{"id": "F9", "shares": None}]}).encode())
            if "conversations.replies" in url:
                return _Response(json.dumps({"ok": True, "messages": [{"ts": "100.1"}, {"ts": "170.5", "files": [{"id": "F9"}]}]}).encode())
            raise AssertionError(url)

        with tempfile.TemporaryDirectory() as tmp:
            clip = Path(tmp) / "demo.mp4"
            clip.write_bytes(b"x" * 12)
            egress = SlackEgress(token="xoxb-test", opener=opener)
            result = egress.upload("C1", str(clip), thread_ts="100.1", initial_comment="demo")
        self.assertEqual(result, {"file_id": "F9", "ts": "170.5"})
        self.assertEqual(seen[1][1:], ("POST", b"x" * 12))
        self.assertIn("length=12", seen[0][0])

    def test_unreadable_or_empty_files_are_refused_before_any_call(self):
        egress = SlackEgress(token="xoxb-test", opener=lambda *a, **k: (_ for _ in ()).throw(AssertionError("no call")))
        with self.assertRaises(SlackError) as caught:
            egress.upload("C1", "/nonexistent/clip.mp4")
        self.assertEqual(caught.exception.code, "file_unreadable")
        with tempfile.TemporaryDirectory() as tmp:
            empty = Path(tmp) / "empty.bin"
            empty.write_bytes(b"")
            with self.assertRaises(SlackError) as caught:
                egress.upload("C1", str(empty))
        self.assertEqual(caught.exception.code, "file_empty")




class ThreadRepliesTests(unittest.TestCase):
    def test_thread_replies_follow_the_cursor_and_keep_file_names(self):
        pages = {
            "": {"ok": True, "has_more": True, "response_metadata": {"next_cursor": "c2"},
                 "messages": [{"ts": "100.1", "text": "root", "user": "U1"},
                              {"ts": "100.2", "text": "", "user": "U2",
                               "files": [{"id": "F1", "name": "frag.json", "permalink": "https://x/F1", "size": 3, "url_private": "secret"}]}]},
            "c2": {"ok": True, "has_more": False,
                   "messages": [{"ts": "100.2", "text": "", "user": "U2"}, {"ts": "100.3", "text": "last", "user": "U3"}]},
        }
        calls = []

        def opener(request, timeout=0):
            from urllib.parse import parse_qs, urlparse
            query = parse_qs(urlparse(request.full_url).query)
            cursor = query.get("cursor", [""])[0]
            calls.append(cursor)
            return _Response(json.dumps(pages[cursor]).encode())

        egress = SlackEgress(token="xoxb-test", opener=opener)
        messages = egress.thread_replies("C1", "100.1")
        self.assertEqual(calls, ["", "c2"])
        self.assertEqual([m["ts"] for m in messages], ["100.1", "100.2", "100.3"], "oldest first, no duplicates")
        self.assertEqual(messages[1]["files"], [{"id": "F1", "name": "frag.json", "permalink": "https://x/F1", "size": 3}])
        self.assertNotIn("url_private", json.dumps(messages))
        self.assertEqual(len(egress.thread_replies("C1", "100.1", limit=2)), 2)


if __name__ == "__main__":
    unittest.main()
