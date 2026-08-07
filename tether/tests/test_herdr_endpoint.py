import hashlib
import json
import os
import pathlib
import socket
import tempfile
import threading
import types
import unittest
from unittest import mock

from test_bridge import load_runtime


BOOT_ID = "00000000-0000-4000-8000-000000000001"


class OneShotHerdrServer:
    def __init__(self, path: pathlib.Path, handler):
        self.path = path
        self.handler = handler
        self.request = None
        self.error = None
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._serve, daemon=True)

    def __enter__(self):
        self.thread.start()
        if not self.ready.wait(5):
            raise RuntimeError("fake Herdr server did not start")
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.thread.join(5)
        if self.thread.is_alive():
            raise RuntimeError("fake Herdr server did not stop")
        if self.error is not None and exc is None:
            raise self.error

    def _serve(self):
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server.bind(str(self.path))
            os.chmod(self.path, 0o600)
            server.listen(1)
            self.ready.set()
            connection, _address = server.accept()
            with connection:
                payload = bytearray()
                while b"\n" not in payload:
                    chunk = connection.recv(65_536)
                    if not chunk:
                        return
                    payload.extend(chunk)
                self.request = json.loads(bytes(payload).split(b"\n", 1)[0])
                response = self.handler(self.request)
                if response is not None:
                    connection.sendall(
                        json.dumps(response, separators=(",", ":")).encode()
                        + b"\n"
                    )
        except BaseException as exc:
            self.error = exc
            self.ready.set()
        finally:
            server.close()


class HerdrEndpointTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name)
        self.runtime = load_runtime(self.home)
        self.socket_index = 0

    def tearDown(self):
        self.temp.cleanup()

    def socket_path(self):
        self.socket_index += 1
        return self.home / f"herdr-{self.socket_index}.sock"

    def process_identity(self, **overrides):
        payload = {
            "agent": "codex",
            "boot": BOOT_ID,
            "exe": "1:2",
            "exe_path": hashlib.sha256(b"/opt/codex/bin/codex").hexdigest()[:16],
            "pid": 200,
            "start": "20000",
            "terminal": "term_6583153c2a1b81",
            "tty": "34823",
        }
        payload.update(overrides)
        return self.runtime.HERDR_PROCESS_IDENTITY_PREFIX + json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        )

    def source(self, **overrides):
        source = {
            "session_id": "codex-session-1",
            "cwd": str(self.home),
            "pane_agent": "codex",
            "process_identity": self.process_identity(),
            "herdr_session": "pilot",
            "herdr_socket_path": str(self.home / "pilot" / "herdr.sock"),
            "herdr_terminal_id": "term_6583153c2a1b81",
            "herdr_pane_id": "w1:p1",
            "herdr_agent_name": "tether_0123456789abcdef",
            "herdr_agent_session_source": "codex_notify",
            "herdr_agent_session_kind": "thread_id",
            "herdr_agent_session_value": "codex-session-1",
            "herdr_protocol": "19",
        }
        source.update(overrides)
        return source

    def test_raw_api_keeps_prompt_out_of_argv_and_returns_exact_result(self):
        path = self.socket_path()

        def handler(request):
            return {
                "id": request["id"],
                "result": {"type": "agent_prompted", "agent": {"name": "bound"}},
            }

        with OneShotHerdrServer(path, handler) as server:
            result = self.runtime._herdr_call(
                str(path),
                "agent.prompt",
                {"target": "tether_bound", "text": "secret follow-up"},
                mutation=True,
            )

        self.assertEqual(result["type"], "agent_prompted")
        self.assertEqual(server.request["method"], "agent.prompt")
        self.assertEqual(server.request["params"]["text"], "secret follow-up")

    def test_server_rejection_is_proven_not_started(self):
        path = self.socket_path()

        def handler(request):
            return {
                "id": request["id"],
                "error": {"code": "agent_not_found", "message": "gone"},
            }

        with OneShotHerdrServer(path, handler), self.assertRaises(
            self.runtime.NativeContinuationError
        ) as raised:
            self.runtime._herdr_call(
                str(path),
                "agent.prompt",
                {"target": "tether_bound", "text": "follow-up"},
                mutation=True,
            )
        self.assertEqual(raised.exception.code, "terminal_submit_not_started")

    def test_unknown_server_error_after_mutation_request_is_uncertain(self):
        path = self.socket_path()

        def handler(request):
            return {
                "id": request["id"],
                "error": {"code": "internal_error", "message": "unknown outcome"},
            }

        with OneShotHerdrServer(path, handler), self.assertRaises(
            self.runtime.NativeContinuationError
        ) as raised:
            self.runtime._herdr_call(
                str(path),
                "agent.prompt",
                {"target": "tether_bound", "text": "follow-up"},
                mutation=True,
            )
        self.assertEqual(raised.exception.code, "terminal_submit_uncertain")

    def test_lost_mutation_response_is_uncertain(self):
        path = self.socket_path()
        with OneShotHerdrServer(path, lambda _request: None), self.assertRaises(
            self.runtime.NativeContinuationError
        ) as raised:
            self.runtime._herdr_call(
                str(path),
                "agent.prompt",
                {"target": "tether_bound", "text": "follow-up"},
                mutation=True,
            )
        self.assertEqual(raised.exception.code, "terminal_submit_uncertain")

    def test_socket_must_be_private_and_owned(self):
        path = self.socket_path()
        endpoint = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            endpoint.bind(str(path))
            os.chmod(path, 0o660)
            with self.assertRaises(self.runtime.NativeContinuationError):
                self.runtime._validate_herdr_socket(str(path))
        finally:
            endpoint.close()

    def test_binding_canonicalizes_exact_herdr_endpoint(self):
        canonical = self.runtime.Store.validate_source(
            "codex_session", self.source()
        )
        self.assertEqual(canonical["binding_version"], "3")
        self.assertEqual(canonical["endpoint_kind"], "herdr_agent")
        self.assertEqual(canonical["delivery_policy"], "native_required")
        bridge = types.SimpleNamespace(
            source_kind="codex_session",
            source=canonical,
            binding_version=3,
            binding_state="verified",
            delivery_policy="native_required",
            endpoint_kind="herdr_agent",
        )
        binding = self.runtime.source_binding(bridge)
        self.assertTrue(binding.uses_herdr)
        self.assertEqual(binding.herdr_agent_name, "tether_0123456789abcdef")
        self.assertEqual(
            self.runtime.endpoint_identity_key(binding),
            self.runtime.endpoint_identity_key(binding),
        )
        rotated = types.SimpleNamespace(
            **{
                **binding.__dict__,
                "herdr_terminal_id": "term_7654321fedcba98",
            }
        )
        self.assertEqual(
            self.runtime.endpoint_identity_key(binding),
            self.runtime.endpoint_identity_key(rotated),
        )

    def test_binding_rejects_cross_endpoint_and_session_confusion(self):
        cases = (
            self.source(herdr_agent_session_value="different-session"),
            self.source(herdr_terminal_id="term_different"),
            self.source(herdr_agent_name="UPPERCASE"),
            self.source(herdr_protocol="18"),
            self.source(
                zellij_session="work",
                zellij_pane_id="7",
            ),
            {
                "session_id": "codex-session-1",
                "cwd": str(self.home),
                "pane_agent": "codex",
                "process_identity": self.process_identity(),
            },
        )
        for source in cases:
            with self.subTest(source=source), self.assertRaises(ValueError):
                self.runtime.Store.validate_source("codex_session", source)

    def test_binding_requires_all_herdr_capability_fields(self):
        for field in self.runtime.HERDR_ENDPOINT_FIELDS:
            source = self.source()
            source.pop(field)
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.runtime.Store.validate_source("codex_session", source)

    def test_capture_uses_official_session_and_assigns_occupant_name(self):
        pane = {
            "pane_id": "w1:p1",
            "terminal_id": "term_6583153c2a1b81",
        }
        unnamed = {
            **pane,
            "agent": "codex",
            "name": None,
            "launch_pending": False,
            "agent_session": {
                "source": "codex_notify",
                "agent": "codex",
                "kind": "thread_id",
                "value": "codex-session-1",
            },
        }
        named = {**unnamed, "name": "tether_0123456789abcdef"}
        responses = (
            {"type": "pong", "protocol": 19},
            {"type": "pane_info", "pane": pane},
            {"type": "agent_info", "agent": unnamed},
            {"type": "agent_info", "agent": named},
            {"type": "pane_process_info", "process_info": {"pane_id": "w1:p1"}},
            {"type": "agent_info", "agent": named},
        )
        with mock.patch.object(
            self.runtime, "_herdr_call", side_effect=responses
        ) as call, mock.patch.object(
            self.runtime,
            "_herdr_agent_name",
            return_value="tether_0123456789abcdef",
        ), mock.patch.object(
            self.runtime,
            "_herdr_process_identity",
            return_value=self.process_identity(),
        ), mock.patch.object(
            self.runtime,
            "_validate_herdr_socket",
            return_value=pathlib.Path("/tmp/herdr.sock"),
        ):
            identity = self.runtime.herdr_agent_identity(
                "/tmp/herdr.sock", "w1:p1", "pilot", str(self.home)
            )

        self.assertEqual(identity["native_session_id"], "codex-session-1")
        self.assertEqual(identity["herdr_agent_name"], "tether_0123456789abcdef")
        self.assertEqual(call.call_args_list[3].args[1], "agent.rename")
        self.assertEqual(
            call.call_args_list[3].args[2],
            {"target": "w1:p1", "name": "tether_0123456789abcdef"},
        )

    def test_read_only_capture_never_assigns_an_agent_name(self):
        pane = {"pane_id": "w1:p1", "terminal_id": "term_6583153c2a1b81"}
        unnamed = {
            **pane,
            "agent": "codex",
            "name": None,
            "launch_pending": False,
            "agent_session": {
                "source": "codex_notify",
                "agent": "codex",
                "kind": "thread_id",
                "value": "codex-session-1",
            },
        }
        responses = (
            {"type": "pong", "protocol": 19},
            {"type": "pane_info", "pane": pane},
            {"type": "agent_info", "agent": unnamed},
            {"type": "pane_process_info", "process_info": {"pane_id": "w1:p1"}},
            {"type": "agent_info", "agent": unnamed},
        )
        with mock.patch.object(
            self.runtime, "_herdr_call", side_effect=responses
        ) as call, mock.patch.object(
            self.runtime,
            "_herdr_process_identity",
            return_value=self.process_identity(),
        ), mock.patch.object(
            self.runtime,
            "_validate_herdr_socket",
            return_value=pathlib.Path("/tmp/herdr.sock"),
        ):
            identity = self.runtime.herdr_agent_identity(
                "/tmp/herdr.sock",
                "w1:p1",
                "default",
                str(self.home),
                assign_name=False,
            )

        self.assertEqual(identity["herdr_session"], "default")
        self.assertEqual(identity["herdr_agent_name"], "")
        self.assertNotIn("agent.rename", [entry.args[1] for entry in call.call_args_list])

    def test_moved_pane_keeps_binding_when_terminal_and_occupant_match(self):
        canonical = self.runtime.Store.validate_source("codex_session", self.source())
        binding = types.SimpleNamespace(**{
            "herdr_socket_path": canonical["herdr_socket_path"],
            "herdr_agent_name": canonical["herdr_agent_name"],
            "herdr_terminal_id": canonical["herdr_terminal_id"],
            "herdr_pane_id": canonical["herdr_pane_id"],
            "herdr_agent_session_source": canonical["herdr_agent_session_source"],
            "herdr_agent_session_kind": canonical["herdr_agent_session_kind"],
            "herdr_agent_session_value": canonical["herdr_agent_session_value"],
            "pane_agent": canonical["pane_agent"],
            "process_identity": canonical["process_identity"],
        })
        moved = {
            "name": canonical["herdr_agent_name"],
            "terminal_id": canonical["herdr_terminal_id"],
            "pane_id": "w2:p9",
            "agent": "codex",
            "launch_pending": False,
            "agent_session": {
                "source": canonical["herdr_agent_session_source"],
                "agent": "codex",
                "kind": canonical["herdr_agent_session_kind"],
                "value": canonical["herdr_agent_session_value"],
            },
        }
        responses = (
            {"type": "pong", "protocol": 19},
            {"type": "agent_info", "agent": moved},
            {"type": "pane_process_info", "process_info": {"pane_id": "w2:p9"}},
        )
        with mock.patch.object(
            self.runtime, "_herdr_call", side_effect=responses
        ), mock.patch.object(
            self.runtime,
            "_herdr_process_identity",
            return_value=canonical["process_identity"],
        ):
            agent, pane_id = self.runtime._current_herdr_agent(binding)

        self.assertEqual(agent["terminal_id"], canonical["herdr_terminal_id"])
        self.assertEqual(pane_id, "w2:p9")

    def test_live_handoff_keeps_binding_when_only_terminal_id_rotates(self):
        canonical = self.runtime.Store.validate_source("codex_session", self.source())
        binding = types.SimpleNamespace(**{
            "herdr_socket_path": canonical["herdr_socket_path"],
            "herdr_agent_name": canonical["herdr_agent_name"],
            "herdr_terminal_id": canonical["herdr_terminal_id"],
            "herdr_pane_id": canonical["herdr_pane_id"],
            "herdr_agent_session_source": canonical["herdr_agent_session_source"],
            "herdr_agent_session_kind": canonical["herdr_agent_session_kind"],
            "herdr_agent_session_value": canonical["herdr_agent_session_value"],
            "pane_agent": canonical["pane_agent"],
            "process_identity": canonical["process_identity"],
        })
        rotated_terminal = "term_7654321fedcba98"
        moved = {
            "name": canonical["herdr_agent_name"],
            "terminal_id": rotated_terminal,
            "pane_id": "w1:p1",
            "agent": "codex",
            "launch_pending": False,
            "agent_session": {
                "source": canonical["herdr_agent_session_source"],
                "agent": "codex",
                "kind": canonical["herdr_agent_session_kind"],
                "value": canonical["herdr_agent_session_value"],
            },
        }
        responses = (
            {"type": "pong", "protocol": 19},
            {"type": "agent_info", "agent": moved},
            {"type": "pane_process_info", "process_info": {"pane_id": "w1:p1"}},
        )
        with mock.patch.object(
            self.runtime, "_herdr_call", side_effect=responses
        ), mock.patch.object(
            self.runtime,
            "_herdr_process_identity",
            return_value=self.process_identity(terminal=rotated_terminal),
        ):
            agent, pane_id = self.runtime._current_herdr_agent(binding)

        self.assertEqual(agent["terminal_id"], rotated_terminal)
        self.assertEqual(pane_id, "w1:p1")

    def test_live_handoff_still_rejects_replaced_process(self):
        self.assertFalse(
            self.runtime._same_herdr_process_identity(
                self.process_identity(terminal="term_rotated"),
                self.process_identity(pid=201, start="20001"),
            )
        )

    def test_context_lookup_returns_sanitized_binding_and_counts(self):
        store = self.runtime.Store(self.home / "bridges.db")
        bridge = store.create({
            "source_kind": "codex_session",
            "source": self.source(),
            "owner_user_id": "*",
            "team_id": "T12345678",
            "channel_id": "C12345678",
            "idempotency_key": "herdr-context-test",
        })
        store.bind(bridge.bridge_id, "1234567890.123456")
        broker = self.runtime.Broker(
            "unused",
            store=store,
            verified_workspace_team_id="T12345678",
        )
        result = broker._herdr_context({
            "herdr_terminal_id": "term_7654321fedcba98",
            "herdr_agent_name": "tether_0123456789abcdef",
            "herdr_agent_session_value": "codex-session-1",
            "herdr_agent": "codex",
        })

        self.assertTrue(result["bound"])
        self.assertEqual(result["bridge"]["channel_id"], "C12345678")
        serialized = json.dumps(result)
        self.assertNotIn("codex-session-1", serialized)
        self.assertNotIn("herdr.sock", serialized)

    def test_delivery_revalidates_before_and_after_atomic_prompt(self):
        canonical = self.runtime.Store.validate_source(
            "codex_session", self.source()
        )
        bridge = types.SimpleNamespace(
            bridge_id="brg_0123456789abcdef01234567",
            source_kind="codex_session",
            source=canonical,
            binding_version=3,
            binding_state="verified",
            binding_error_code="",
            delivery_policy="native_required",
            endpoint_kind="herdr_agent",
        )
        prompted = {
            "type": "agent_prompted",
            "agent": {
                "name": "tether_0123456789abcdef",
                "terminal_id": "term_7654321fedcba98",
                "agent": "codex",
            },
        }
        with mock.patch.object(
            self.runtime,
            "_current_herdr_agent",
            return_value=(
                {"terminal_id": "term_7654321fedcba98"},
                "w1:p1",
            ),
        ) as current, mock.patch.object(
            self.runtime,
            "_live_attempt_instruction",
            return_value=("att_0123456789abcdef", "private instruction"),
        ), mock.patch.object(
            self.runtime,
            "_herdr_call",
            return_value=prompted,
        ) as call:
            marker = self.runtime.deliver_herdr(
                bridge, "follow-up", "att_0123456789abcdef"
            )

        self.assertEqual(marker, "att_0123456789abcdef")
        self.assertEqual(current.call_count, 2)
        self.assertEqual(call.call_args.args[1], "agent.prompt")
        self.assertEqual(
            call.call_args.args[2],
            {
                "target": "tether_0123456789abcdef",
                "text": "private instruction",
            },
        )
        self.assertTrue(call.call_args.kwargs["mutation"])


if __name__ == "__main__":
    unittest.main()
