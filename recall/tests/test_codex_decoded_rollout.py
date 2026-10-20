from __future__ import annotations

import hashlib
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from compression import zstd
except ImportError:
    from backports import zstd

from skills.recall.scripts.codex_rollout import (
    DecodedSourceChanged,
    decoded_snapshot,
    open_decoded,
)


FIXTURE = Path(__file__).parent / "fixtures" / "codex-rollout.jsonl.zst"
RAW = (
    b'{"payload": {"id": "019f1111-2222-7333-8444-555555555555"}, '
    b'"timestamp": "2026-08-10T00:00:00Z", "type": "session_meta"}\n'
    b'{"payload": {"marker": "compressed synthetic work", "type": "message"}, '
    b'"timestamp": "2026-08-10T00:00:01Z", "type": "response_item"}\n'
)


class DecodedRolloutTest(unittest.TestCase):
    def test_native_stream_has_decoded_offsets_and_private_verified_snapshot(self):
        deadline = time.monotonic() + 5
        with open_decoded(FIXTURE, deadline=deadline) as stream:
            first = stream.readline(1024)
            self.assertEqual(first, RAW.splitlines(keepends=True)[0])
            self.assertEqual(stream.tell(), len(first))
            stream.seek(0)
            self.assertEqual(stream.read(1024), RAW)
            stream.seek(0)
            self.assertEqual(stream.read1(3), RAW[:3])
            target = bytearray(4)
            self.assertEqual(stream.readinto(target), 4)
            self.assertEqual(target, RAW[3:7])
            for operation in (stream.read, stream.read1, stream.readline):
                with self.assertRaises(ValueError):
                    operation(-1)
        with tempfile.TemporaryDirectory() as directory:
            with decoded_snapshot(FIXTURE, deadline=deadline, directory=Path(directory)) as snapshot:
                self.assertEqual(snapshot.source_path, FIXTURE)
                self.assertEqual(snapshot.path.read_bytes(), RAW)
                self.assertEqual(snapshot.size, 255)
                self.assertEqual(snapshot.sha256, hashlib.sha256(RAW).hexdigest())
                self.assertEqual(snapshot.path.stat().st_mode & 0o777, 0o600)
                self.assertNotEqual(snapshot.physical_signature[1], snapshot.path.stat().st_ino)
                snapshot.verify_current()
                temporary = snapshot.path
            self.assertFalse(temporary.exists())

    def test_truncated_or_trailing_corruption_never_yields_complete_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollout.jsonl.zst"
            for data in (b"", FIXTURE.read_bytes()[:-1], FIXTURE.read_bytes() + b"bad"):
                with self.subTest(length=len(data)):
                    path.write_bytes(data)
                    with self.assertRaises((EOFError, zstd.ZstdError)):
                        with decoded_snapshot(path, deadline=time.monotonic() + 5):
                            self.fail("invalid frame cannot yield a snapshot")

    def test_concatenated_frames_and_partial_final_record_keep_exact_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollout.jsonl.zst"
            path.write_bytes(FIXTURE.read_bytes() + zstd.compress(b'{"partial":'))
            with decoded_snapshot(path, deadline=time.monotonic() + 5) as snapshot:
                self.assertEqual(snapshot.path.read_bytes(), RAW + b'{"partial":')
                # Valid compression does not turn an incomplete JSONL record
                # into an ACKable record. The existing scanner owns that rule.
                self.assertFalse(snapshot.path.read_bytes().endswith(b"\n"))

    def test_read_seek_and_snapshot_expiry_release_private_files(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("skills.recall.scripts.codex_rollout.time.monotonic", return_value=1):
                with open_decoded(FIXTURE, deadline=2) as stream:
                    with patch("skills.recall.scripts.codex_rollout.time.monotonic", return_value=3):
                        with self.assertRaises(TimeoutError):
                            stream.seek(200)
                with self.assertRaises(TimeoutError):
                    with decoded_snapshot(FIXTURE, deadline=0, directory=Path(directory)):
                        self.fail("expired decode cannot yield a snapshot")
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_expired_deadline_also_refuses_already_buffered_bytes(self):
        for method, argument in (
            ("read", 1), ("read1", 1), ("readline", 1),
            ("readinto", bytearray(1)), ("seek", 0),
        ):
            with self.subTest(method=method):
                with patch("skills.recall.scripts.codex_rollout.time.monotonic", return_value=1):
                    with open_decoded(FIXTURE, deadline=2) as stream:
                        self.assertEqual(stream.read(1), RAW[:1])
                        with patch("skills.recall.scripts.codex_rollout.time.monotonic", return_value=3):
                            with self.assertRaises(TimeoutError):
                                getattr(stream, method)(argument)

    def test_snapshot_checkpoint_refuses_expired_deadline(self):
        with patch("skills.recall.scripts.codex_rollout.time.monotonic", return_value=1):
            with decoded_snapshot(FIXTURE, deadline=2) as snapshot:
                with patch("skills.recall.scripts.codex_rollout.time.monotonic", return_value=3):
                    with self.assertRaises(TimeoutError):
                        snapshot.verify_current()

    def test_physical_change_after_decode_refuses_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollout.jsonl.zst"
            path.write_bytes(FIXTURE.read_bytes())
            with self.assertRaises(DecodedSourceChanged):
                with decoded_snapshot(path, deadline=time.monotonic() + 5) as snapshot:
                    path.write_bytes(path.read_bytes() + b"changed")
                    snapshot.verify_current()

    def test_plain_bytes_use_same_offsets_and_changed_source_fence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollout.jsonl"
            path.write_bytes(RAW)
            with open_decoded(path, deadline=time.monotonic() + 5) as stream:
                stream.seek(123)
                self.assertEqual(stream.read(10), RAW[123:133])
                stream.seek(-5, 1)
                self.assertEqual(stream.read(5), RAW[128:133])
            with self.assertRaises(DecodedSourceChanged):
                with open_decoded(path, deadline=time.monotonic() + 5) as stream:
                    self.assertEqual(stream.read(5), RAW[:5])
                    path.write_bytes(b"changed")

    def test_large_expansion_is_consumed_in_small_blocks(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollout.jsonl.zst"
            # Multiple small native frames avoid allocating the whole decoded
            # fixture in the test as well as in the reader.
            frame = zstd.compress(b"x" * 65536)
            path.write_bytes(frame * 512)
            count = 0
            with open_decoded(path, deadline=time.monotonic() + 5) as stream:
                while block := stream.read(65536):
                    self.assertEqual(block, b"x" * len(block))
                    self.assertLessEqual(len(block), 65536)
                    count += len(block)
            self.assertEqual(count, 32 * 1024 * 1024)
