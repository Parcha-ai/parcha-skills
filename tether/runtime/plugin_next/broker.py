"""Local Unix-socket broker: the one door for the CLI and scripts.

Protocol (unchanged from the legacy broker so every caller keeps working):
one JSON object per line in, one JSON object per line out, ``{"ok": true, ...}``
or ``{"ok": false, "code": ..., "error": ...}``. The peer must be the same Unix
user as the gateway; root is refused. Everything the broker does is delegated
to :class:`ActiveSlice` ops -- this module owns framing and the socket only.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import stat
import struct
import threading
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger("hermes_plugins.tether_next.broker")

PROTOCOL_VERSION = 6
MAX_FRAME = 1_000_000
Handler = Callable[[dict[str, Any]], dict[str, Any]]


class BrokerRefused(Exception):
    def __init__(self, code: str, message: str | None = None, *, retryable: bool = False):
        super().__init__(message or code)
        self.code = code
        self.retryable = retryable


class BrokerServer:
    def __init__(self, socket_path: Path, handler: Handler):
        self.socket_path = Path(socket_path)
        self.handler = handler
        self._stop = threading.Event()
        self._server: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        if self.socket_path.exists() or self.socket_path.is_symlink():
            self.socket_path.unlink()
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        previous = os.umask(0o177)
        try:
            server.bind(str(self.socket_path))
        finally:
            os.umask(previous)
        os.chmod(self.socket_path, 0o600)
        server.listen(16)
        server.settimeout(0.5)
        self._server = server
        self._thread = threading.Thread(target=self._serve, name="tether-broker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._server is not None:
            try:
                self._server.close()
            except OSError:
                pass
        try:
            if self.socket_path.exists() and stat.S_ISSOCK(self.socket_path.lstat().st_mode):
                self.socket_path.unlink()
        except OSError:
            pass

    def _serve(self) -> None:
        if self._server is None:
            return
        while not self._stop.is_set():
            try:
                connection, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._session, args=(connection,), daemon=True).start()

    def _session(self, connection: socket.socket) -> None:
        with connection:
            try:
                if not self._peer_allowed(connection):
                    _write(connection, {"ok": False, "code": "peer_refused",
                                        "error": "broker peer is not the gateway user"})
                    return
                connection.settimeout(30)
                frame = _read_line(connection)
                if frame is None:
                    return
                try:
                    request = json.loads(frame)
                except ValueError:
                    _write(connection, {"ok": False, "code": "bad_request", "error": "malformed JSON"})
                    return
                if not isinstance(request, dict):
                    _write(connection, {"ok": False, "code": "bad_request", "error": "request must be an object"})
                    return
                _write(connection, self._dispatch(request))
            except Exception:  # never let one client kill the server
                logger.exception("tether: broker session failed")

    def _dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self.handler(request)
        except BrokerRefused as refused:
            return {"ok": False, "code": refused.code, "error": str(refused),
                    "retryable": refused.retryable}
        except Exception as exc:
            logger.exception("tether: broker op failed")
            return {"ok": False, "code": "internal_error", "error": type(exc).__name__}
        if "ok" not in response:
            response = dict(response, ok=True)
        return response

    @staticmethod
    def _peer_allowed(connection: socket.socket) -> bool:
        try:
            creds = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
            _pid, uid, _gid = struct.unpack("3i", creds)
        except (OSError, struct.error):
            return False
        return uid != 0 and uid == os.geteuid()


def _read_line(connection: socket.socket) -> str | None:
    chunks = bytearray()
    while True:
        piece = connection.recv(65536)
        if not piece:
            return bytes(chunks).decode("utf-8", errors="replace") if chunks else None
        chunks.extend(piece)
        if b"\n" in piece:
            break
        if len(chunks) > MAX_FRAME:
            raise ValueError("frame too large")
    return bytes(chunks).split(b"\n", 1)[0].decode("utf-8", errors="replace")


def _write(connection: socket.socket, payload: dict[str, Any]) -> None:
    connection.sendall((json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"))


def call(socket_path: Path, request: dict[str, Any], *, timeout: float = 30.0) -> dict[str, Any]:
    """Client half, used by tests and the Python CLI."""
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(str(socket_path))
        _write(client, request)
        line = _read_line(client)
    if line is None:
        raise BrokerRefused("broker_unavailable", "broker closed without a response")
    payload = json.loads(line)
    if not isinstance(payload, dict) or not isinstance(payload.get("ok"), bool):
        raise BrokerRefused("protocol", "invalid response contract")
    return payload
