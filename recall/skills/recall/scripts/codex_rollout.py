"""Decoded Codex bytes, without changing source identity or capture authority.

Discovery is deliberately owned by the collector. A completed temporary snapshot
proves decoded bytes at one physical source signature, not a delivered record.
"""

from __future__ import annotations

import hashlib
import io
import os
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path


BLOCK_BYTES = 128 * 1024
# Bounds decoder memory, not total transcript size. Native level-3 rollouts use
# a much smaller window; reject unsupported resource demands explicitly.
WINDOW_LOG_MAX = 27


class DecodedSourceChanged(OSError):
    pass


class DecodedSourceDeadline(TimeoutError):
    pass


class DecodedSourceInvalid(ValueError, EOFError):
    """Invalid compression is not a proved empty or complete transcript."""


def identity_name(path: Path) -> Path:
    """Name presented to the unchanged native filename parser, never an IO path."""
    path = Path(path)
    return path.with_suffix("") if path.name.endswith(".jsonl.zst") else path


@dataclass(frozen=True)
class IdentityReadView:
    """Supply decoded bytes to the native identity parser without changing it."""

    source_path: Path
    deadline: float

    @property
    def stem(self) -> str:
        return identity_name(self.source_path).stem

    def open(self, mode: str):
        if mode != "rb":
            raise ValueError("identity view only supports binary reads")
        return open_decoded(self.source_path, deadline=self.deadline)


def physical_signature(value) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _check_deadline(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise DecodedSourceDeadline("decoded_source_deadline")


def _zstd():
    try:
        from compression import zstd
    except ImportError:
        try:
            from backports import zstd
        except ImportError:
            raise RuntimeError("decoded_source_dependency_unavailable") from None
    return zstd


class _DecodedReader(io.RawIOBase):
    def __init__(self, source, *, compressed: bool, deadline: float):
        super().__init__()
        self._source = source
        self._deadline = deadline
        self._position = 0
        self._decoder = None
        self._pending = b""
        self._at_frame_end = False
        if compressed:
            self._zstd_module = _zstd()
            self._decoder = self._new_decoder()

    def _new_decoder(self):
        zstd = self._zstd_module
        return zstd.ZstdDecompressor(
            options={zstd.DecompressionParameter.window_log_max: WINDOW_LOG_MAX},
        )

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._position

    def readinto(self, target):
        _check_deadline(self._deadline)
        maximum = min(len(target), BLOCK_BYTES)
        if not maximum:
            return 0
        if self._decoder is None:
            block = self._source.read(maximum)
        else:
            block = b""
            while not block:
                _check_deadline(self._deadline)
                if self._decoder.eof:
                    self._pending = self._decoder.unused_data
                    self._decoder = self._new_decoder()
                    self._at_frame_end = True
                data = b""
                if self._decoder.needs_input:
                    data, self._pending = self._pending, b""
                    if not data:
                        data = self._source.read(BLOCK_BYTES)
                    if not data:
                        if not self._at_frame_end:
                            raise DecodedSourceInvalid("decoded_source_truncated")
                        return 0
                    self._at_frame_end = False
                try:
                    block = self._decoder.decompress(data, max_length=maximum)
                except self._zstd_module.ZstdError as error:
                    raise DecodedSourceInvalid("decoded_source_invalid") from error
        _check_deadline(self._deadline)
        target[:len(block)] = block
        self._position += len(block)
        return len(block)

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_CUR:
            offset += self._position
        elif whence != os.SEEK_SET:
            raise io.UnsupportedOperation("decoded seek requires an explicit offset")
        if offset < 0:
            raise ValueError("negative decoded offset")
        _check_deadline(self._deadline)
        if offset < self._position:
            self._source.seek(0)
            self._position = 0
            self._pending = b""
            self._at_frame_end = False
            if self._decoder is not None:
                self._decoder = self._new_decoder()
        while self._position < offset:
            if not self.read(min(BLOCK_BYTES, offset - self._position)):
                break
        return self._position


class _DecodedStream:
    """Expose only checked operations, including reads served from the buffer."""

    def __init__(self, buffered, deadline):
        self._buffered = buffered
        self._deadline = deadline

    def _perform(self, operation, *args):
        _check_deadline(self._deadline)
        result = operation(*args)
        _check_deadline(self._deadline)
        return result

    def read(self, size):
        if size < 0:
            raise ValueError("decoded reads require an explicit byte bound")
        return self._perform(self._buffered.read, size)

    def read1(self, size):
        if size < 0:
            raise ValueError("decoded reads require an explicit byte bound")
        return self._perform(self._buffered.read1, size)

    def readline(self, size):
        if size < 0:
            raise ValueError("decoded reads require an explicit byte bound")
        return self._perform(self._buffered.readline, size)

    def readinto(self, target):
        return self._perform(self._buffered.readinto, target)

    def seek(self, offset, whence=os.SEEK_SET):
        return self._perform(self._buffered.seek, offset, whence)

    def tell(self):
        return self._perform(self._buffered.tell)


@contextmanager
def open_decoded(path: Path, *, deadline: float, expected_signature: tuple[int, ...] | None = None):
    """Read bounded decoded blocks; offsets never refer to compressed bytes.

    A bounded prefix read does not validate an unread frame tail. Consumers
    claiming complete content must read to EOF, as decoded_snapshot does.
    """
    path = Path(path)
    _check_deadline(deadline)
    before = physical_signature(path.stat())
    if expected_signature is not None and before != expected_signature:
        raise DecodedSourceChanged("decoded_source_changed")
    with path.open("rb") as source:
        if physical_signature(os.fstat(source.fileno())) != before:
            raise DecodedSourceChanged("decoded_source_changed")
        with io.BufferedReader(_DecodedReader(
            source, compressed=path.name.endswith(".jsonl.zst"), deadline=deadline,
        ), buffer_size=BLOCK_BYTES) as reader:
            yield _DecodedStream(reader, deadline)
            _check_deadline(deadline)
            if (physical_signature(os.fstat(source.fileno())) != before
                    or physical_signature(path.stat()) != before):
                raise DecodedSourceChanged("decoded_source_changed")


def verify_source(path: Path, expected_signature: tuple[int, ...], *, deadline: float) -> None:
    """Recheck the physical input and the same deadline before a checkpoint."""
    _check_deadline(deadline)
    if physical_signature(path.stat()) != expected_signature:
        raise DecodedSourceChanged("decoded_source_changed")
    _check_deadline(deadline)


@dataclass(frozen=True)
class DecodedSnapshot:
    source_path: Path
    physical_signature: tuple[int, ...]
    path: Path
    size: int
    sha256: str
    deadline: float

    def verify_current(self) -> None:
        """Call again immediately before persisting a source checkpoint."""
        verify_source(self.source_path, self.physical_signature, deadline=self.deadline)


@contextmanager
def decoded_snapshot(path: Path, *, deadline: float, directory: Path | None = None):
    """Provide a private seekable file for existing plain-file hash owners."""
    path = Path(path)
    before = physical_signature(path.stat())
    with tempfile.TemporaryDirectory(prefix="recall-decoded-", dir=directory) as temporary:
        decoded = Path(temporary) / "snapshot.jsonl"
        digest = hashlib.sha256()
        size = 0
        with decoded.open("xb") as output:
            decoded.chmod(0o600)
            with open_decoded(path, deadline=deadline, expected_signature=before) as source:
                while block := source.read(BLOCK_BYTES):
                    output.write(block)
                    digest.update(block)
                    size += len(block)
        snapshot = DecodedSnapshot(path, before, decoded, size, digest.hexdigest(), deadline)
        snapshot.verify_current()
        yield snapshot
        snapshot.verify_current()
