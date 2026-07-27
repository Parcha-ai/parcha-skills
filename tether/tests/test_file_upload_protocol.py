import importlib.util
import os
import pathlib
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock


RUNTIME_PATH = pathlib.Path(__file__).resolve().parents[1] / "runtime" / "bridge_runtime.py"


def load_runtime(home):
    environment = {
        "HOME": str(home),
        "HERMES_HOME": str(home / ".hermes"),
        "XDG_DATA_HOME": str(home / ".local" / "share"),
        "XDG_CONFIG_HOME": str(home / ".config"),
    }
    with mock.patch.dict(os.environ, environment, clear=False):
        name = f"file_upload_runtime_{id(home)}_{os.urandom(4).hex()}"
        spec = importlib.util.spec_from_file_location(name, RUNTIME_PATH)
        if spec is None or spec.loader is None:
            raise RuntimeError("runtime module could not be loaded")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module


class FileUploadProtocolTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = pathlib.Path(self.temp.name) / "home"
        self.home.mkdir(mode=0o700)
        self.approved = self.home / "approved"
        self.approved.mkdir(mode=0o700)
        self.staging = self.home / "upload-staging"
        self.runtime = load_runtime(self.home)
        self.runtime.UPLOAD_APPROVED_ROOTS = (str(self.approved),)
        self.runtime.UPLOAD_STAGING_DIRECTORY = str(self.staging)
        self.runtime.UPLOAD_MAX_BYTES = "4096"
        self.store = self.runtime.Store(self.home / "bridges.db")
        self.broker = self.runtime.Broker(
            "test-token",
            self.store,
            verified_workspace_team_id="T12345678",
        )
        self.counter = 0

    def tearDown(self):
        self.temp.cleanup()

    def _prepared_attachment(self, key):
        self.counter += 1
        source = self.approved / f"{key}.txt"
        source.write_bytes(f"evidence for {key}".encode())
        source.chmod(0o600)
        bridge = self.store.create(
            {
                "source_kind": "headless_run",
                "source": {"run_id": key, "cwd": "/tmp/project"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "idempotency_key": key,
            }
        )
        thread_ts = f"1789000000.{self.counter:06d}"
        bridge = self.store.bind(bridge.bridge_id, thread_ts)
        staged = self.runtime.stage_safe_upload(str(source))
        self.store.reserve_root(
            bridge.bridge_id,
            "verified result",
            thread_ts,
            staged_upload=staged,
            upload_filename=f"tether-{bridge.bridge_id}-report.txt",
        )
        claimed = self.store.claim_root(bridge.bridge_id)
        self.assertEqual(claimed["status"], "claimed")
        lease_id = claimed["lease_id"]
        self.assertTrue(
            self.store.record_root_post(
                bridge.bridge_id,
                lease_id,
                thread_ts,
            )
        )
        self.assertTrue(
            self.store.release_root(
                bridge.bridge_id,
                lease_id,
                "prepared by test",
            )
        )
        return self.store.get(bridge.bridge_id)

    def test_root_delivery_error_is_redacted_before_persistence(self):
        bridge = self.store.create(
            {
                "source_kind": "headless_run",
                "source": {"run_id": "redacted-root", "cwd": "/tmp/project"},
                "owner_user_id": "U12345678",
                "team_id": "T12345678",
                "channel_id": "C12345678",
                "idempotency_key": "redacted-root",
            }
        )
        self.store.reserve_root(
            bridge.bridge_id,
            "verified result",
            "1789000000.999999",
        )
        claimed = self.store.claim_root(bridge.bridge_id)

        self.assertTrue(
            self.store.release_root(
                bridge.bridge_id,
                claimed["lease_id"],
                "delivery failed with api_key='synthetic-secret-value'",
            )
        )

        with self.store.connect() as database:
            error = database.execute(
                "SELECT error FROM bridge_roots WHERE bridge_id=?",
                (bridge.bridge_id,),
            ).fetchone()[0]
        self.assertIn("[REDACTED]", error)
        self.assertNotIn("synthetic-secret-value", error)

    def _deliver(
        self,
        bridge,
        *,
        allocation=None,
        byte_upload=None,
        completion=None,
        reconciliation=None,
    ):
        allocation = allocation or mock.Mock(
            return_value=(
                "F12345678",
                "https://files.slack.com/upload/v1/signed-value",
            )
        )
        byte_upload = byte_upload or mock.Mock(return_value=None)
        completion = completion or mock.Mock(
            return_value={"ok": True, "files": [{"id": "F12345678"}]}
        )
        reconciliation = reconciliation or mock.Mock(return_value="")
        with mock.patch.object(
            self.runtime,
            "_allocate_slack_upload",
            allocation,
        ), mock.patch.object(
            self.runtime,
            "_upload_slack_bytes",
            byte_upload,
        ), mock.patch.object(
            self.runtime,
            "_complete_slack_upload",
            completion,
        ), mock.patch.object(
            self.broker,
            "_find_staged_root_file",
            reconciliation,
        ):
            result = self.broker._deliver_staged_root(bridge)
        return result, allocation, byte_upload, completion, reconciliation

    def test_schema_persists_file_id_and_phase_but_never_upload_url(self):
        bridge = self._prepared_attachment("schema")
        with self.store.connect() as database:
            columns = {
                row["name"]
                for row in database.execute(
                    "PRAGMA table_info(bridge_roots)"
                )
            }
            version = int(
                database.execute("PRAGMA user_version").fetchone()[0]
            )
        self.assertIn("slack_file_id", columns)
        self.assertIn("upload_phase", columns)
        self.assertNotIn("upload_url", columns)
        self.assertEqual(version, 15)
        self.assertEqual(
            self.store.root_record(bridge.bridge_id)["upload_phase"],
            "reserved",
        )

    def test_v9_schema_migrates_and_backfills_pending_attachment(self):
        legacy_path = self.home / "legacy-v9.db"
        legacy = self.runtime.Store(legacy_path)
        with legacy.connect() as database:
            database.execute(
                """
                INSERT INTO bridge_roots(
                  bridge_id,payload_text,client_msg_id,state,staged_path
                ) VALUES('brg_legacy','result','client','root_posted','/tmp/staged')
                """
            )
        with sqlite3.connect(legacy_path) as database:
            database.execute(
                "ALTER TABLE bridge_roots DROP COLUMN slack_file_id"
            )
            database.execute(
                "ALTER TABLE bridge_roots DROP COLUMN upload_phase"
            )
            database.execute("PRAGMA user_version=9")

        migrated = self.runtime.Store(legacy_path)
        record = migrated.root_record("brg_legacy")
        self.assertEqual(record["upload_phase"], "reserved")
        self.assertIsNone(record["slack_file_id"])
        with migrated.connect() as database:
            self.assertEqual(
                int(database.execute("PRAGMA user_version").fetchone()[0]),
                15,
            )

    def test_allocation_failure_is_durable_and_retry_allocates_once(self):
        bridge = self._prepared_attachment("allocation-failure")

        def fail_allocation(*_args):
            current = self.store.root_record(bridge.bridge_id)
            self.assertEqual(current["upload_phase"], "allocating")
            self.assertIsNone(current["slack_file_id"])
            raise TimeoutError("allocation")

        failed_allocation = mock.Mock(side_effect=fail_allocation)
        with mock.patch.object(
            self.runtime,
            "_allocate_slack_upload",
            failed_allocation,
        ), self.assertRaises(TimeoutError):
            self.broker._deliver_staged_root(bridge)

        failed = self.store.root_record(bridge.bridge_id)
        self.assertEqual(failed["state"], "root_posted")
        self.assertEqual(failed["upload_phase"], "allocation_uncertain")
        self.assertIsNone(failed["slack_file_id"])

        result, allocation, byte_upload, completion, _ = self._deliver(
            self.store.get(bridge.bridge_id)
        )
        self.assertTrue(result["ok"])
        allocation.assert_called_once()
        byte_upload.assert_called_once()
        completion.assert_called_once()
        completed = self.store.root_record(bridge.bridge_id)
        self.assertEqual(completed["upload_phase"], "completed")
        self.assertEqual(completed["slack_file_id"], "F12345678")

    def test_byte_upload_failure_abandons_only_invisible_file_id(self):
        bridge = self._prepared_attachment("byte-failure")
        first_allocation = mock.Mock(
            return_value=(
                "F11111111",
                "https://files.slack.com/upload/v1/first",
            )
        )

        def fail_byte_upload(*_args):
            current = self.store.root_record(bridge.bridge_id)
            self.assertEqual(current["upload_phase"], "uploading_bytes")
            self.assertEqual(current["slack_file_id"], "F11111111")
            raise ConnectionError("bytes")

        failed_upload = mock.Mock(side_effect=fail_byte_upload)
        with mock.patch.object(
            self.runtime,
            "_allocate_slack_upload",
            first_allocation,
        ), mock.patch.object(
            self.runtime,
            "_upload_slack_bytes",
            failed_upload,
        ), self.assertRaises(ConnectionError):
            self.broker._deliver_staged_root(bridge)

        failed = self.store.root_record(bridge.bridge_id)
        self.assertEqual(failed["upload_phase"], "bytes_uncertain")
        self.assertEqual(failed["slack_file_id"], "F11111111")

        second_allocation = mock.Mock(
            return_value=(
                "F22222222",
                "https://files.slack.com/upload/v1/second",
            )
        )
        completed_ids = []

        def complete(_token, _channel, file_id, **_kwargs):
            completed_ids.append(file_id)
            return {"ok": True, "files": [{"id": file_id}]}

        result, _, _, _, _ = self._deliver(
            self.store.get(bridge.bridge_id),
            allocation=second_allocation,
            completion=mock.Mock(side_effect=complete),
        )
        self.assertTrue(result["ok"])
        self.assertEqual(completed_ids, ["F22222222"])
        self.assertEqual(
            self.store.root_record(bridge.bridge_id)["slack_file_id"],
            "F22222222",
        )

    def test_uncertain_completion_reconciles_exact_id_without_retry(self):
        bridge = self._prepared_attachment("completion-timeout")
        visible_ids = set()
        completion_calls = []

        def completion(_token, _channel, file_id, **_kwargs):
            current = self.store.root_record(bridge.bridge_id)
            self.assertEqual(current["upload_phase"], "completing")
            self.assertEqual(current["slack_file_id"], file_id)
            completion_calls.append(file_id)
            visible_ids.add(file_id)
            raise TimeoutError("response lost")

        def reconcile(_bridge, file_id):
            if file_id in visible_ids:
                return bridge.thread_ts
            return ""

        with self.assertRaises(TimeoutError):
            self._deliver(
                bridge,
                completion=mock.Mock(side_effect=completion),
                reconciliation=mock.Mock(side_effect=reconcile),
            )
        result, _, _, no_retry, reconciliation = self._deliver(
            self.store.get(bridge.bridge_id),
            allocation=mock.Mock(
                side_effect=AssertionError("must not allocate a second file")
            ),
            byte_upload=mock.Mock(
                side_effect=AssertionError("must not upload bytes twice")
            ),
            completion=mock.Mock(
                side_effect=AssertionError("must not complete twice")
            ),
            reconciliation=mock.Mock(side_effect=reconcile),
        )
        self.assertTrue(result["ok"])
        self.assertEqual(completion_calls, ["F12345678"])
        reconciliation.assert_called_once_with(bridge, "F12345678")
        no_retry.assert_not_called()
        self.assertEqual(
            self.store.root_record(bridge.bridge_id)["upload_phase"],
            "completed",
        )

    def test_crash_after_completion_recovers_without_external_io(self):
        bridge = self._prepared_attachment("local-commit-crash")
        original_complete = self.store.complete_root_file
        local_calls = 0

        def fail_first_local_commit(*args, **kwargs):
            nonlocal local_calls
            local_calls += 1
            if local_calls == 1:
                raise RuntimeError("local commit crash")
            return original_complete(*args, **kwargs)

        allocation = mock.Mock(
            return_value=(
                "F33333333",
                "https://files.slack.com/upload/v1/third",
            )
        )
        byte_upload = mock.Mock(return_value=None)
        completion = mock.Mock(
            return_value={"ok": True, "files": [{"id": "F33333333"}]}
        )
        with mock.patch.object(
            self.runtime,
            "_allocate_slack_upload",
            allocation,
        ), mock.patch.object(
            self.runtime,
            "_upload_slack_bytes",
            byte_upload,
        ), mock.patch.object(
            self.runtime,
            "_complete_slack_upload",
            completion,
        ), mock.patch.object(
            self.store,
            "complete_root_file",
            side_effect=fail_first_local_commit,
        ), self.assertRaisesRegex(RuntimeError, "local commit crash"):
            self.broker._deliver_staged_root(bridge)

        root = self.store.root_record(bridge.bridge_id)
        self.assertEqual(root["upload_phase"], "completion_confirmed")
        self.assertEqual(root["state"], "root_posted")

        with mock.patch.object(
            self.runtime,
            "_allocate_slack_upload",
        ) as no_allocate, mock.patch.object(
            self.runtime,
            "_upload_slack_bytes",
        ) as no_upload, mock.patch.object(
            self.runtime,
            "_complete_slack_upload",
        ) as no_complete, mock.patch.object(
            self.broker,
            "_find_staged_root_file",
        ) as no_reconcile:
            result = self.broker._deliver_staged_root(
                self.store.get(bridge.bridge_id)
            )

        self.assertTrue(result["ok"])
        no_allocate.assert_not_called()
        no_upload.assert_not_called()
        no_complete.assert_not_called()
        no_reconcile.assert_not_called()
        self.assertEqual(completion.call_count, 1)

    def test_retry_after_unresolved_completion_reuses_same_file_id(self):
        bridge = self._prepared_attachment("completion-retry")
        completion_ids = []

        def first_completion(_token, _channel, file_id, **_kwargs):
            completion_ids.append(file_id)
            raise TimeoutError("unknown completion")

        with mock.patch.object(
            self.runtime,
            "_allocate_slack_upload",
            return_value=(
                "F44444444",
                "https://files.slack.com/upload/v1/fourth",
            ),
        ), mock.patch.object(
            self.runtime,
            "_upload_slack_bytes",
        ), mock.patch.object(
            self.runtime,
            "_complete_slack_upload",
            side_effect=first_completion,
        ), mock.patch.object(
            self.broker,
            "_find_staged_root_file",
            return_value="",
        ), self.assertRaises(TimeoutError):
            self.broker._deliver_staged_root(bridge)

        self.assertEqual(
            self.store.root_record(bridge.bridge_id)["upload_phase"],
            "completion_uncertain",
        )

        def second_completion(_token, _channel, file_id, **_kwargs):
            completion_ids.append(file_id)
            return {"ok": True, "files": [{"id": file_id}]}

        with mock.patch.object(
            self.runtime,
            "_allocate_slack_upload",
        ) as no_allocate, mock.patch.object(
            self.runtime,
            "_upload_slack_bytes",
        ) as no_upload, mock.patch.object(
            self.runtime,
            "_complete_slack_upload",
            side_effect=second_completion,
        ), mock.patch.object(
            self.broker,
            "_find_staged_root_file",
            return_value="",
        ):
            result = self.broker._deliver_staged_root(
                self.store.get(bridge.bridge_id)
            )

        self.assertTrue(result["ok"])
        no_allocate.assert_not_called()
        no_upload.assert_not_called()
        self.assertEqual(completion_ids, ["F44444444", "F44444444"])

    def test_filename_spoof_cannot_reconcile_a_different_file(self):
        self._prepared_attachment("spoof")
        messages = [
            {
                "ts": "1789000000.900001",
                "files": [{
                    "id": "F99999999",
                    "name": "tether-spoof-report.txt",
                    "title": "tether-spoof-report.txt",
                }],
            },
            {
                "ts": "1789000000.900002",
                "files": [{
                    "id": "F55555555",
                    "name": "different-name.txt",
                }],
            },
        ]
        self.assertEqual(
            self.broker._reconciliation_match(
                "file",
                "F55555555",
                messages,
            ),
            "1789000000.900002",
        )
        self.assertEqual(
            self.broker._reconciliation_match(
                "file",
                "F77777777",
                messages,
            ),
            "",
        )

    def test_upload_url_validation_and_byte_transport_are_strict(self):
        for unsafe in (
            "http://files.slack.com/upload/v1/value",
            "https://slack.com/upload/v1/value",
            "https://files.slack.com.evil.test/upload/v1/value",
            "https://user@files.slack.com/upload/v1/value",
            "https://files.slack.com:444/upload/v1/value",
            "https://files.slack.com/not-upload/value",
        ):
            with self.subTest(unsafe=unsafe), self.assertRaises(RuntimeError):
                self.runtime._validate_slack_upload_url(unsafe)

        source = self.approved / "transport.txt"
        source.write_bytes(b"immutable bytes")
        source.chmod(0o600)
        staged = self.runtime.stage_safe_upload(str(source))
        requests = []

        class Response:
            status = 200

            @staticmethod
            def getheaders():
                return []

            @staticmethod
            def read(_limit):
                return b"OK - 15"

        class Connection:
            def __init__(self, host, port, timeout, context):
                self.host = host
                self.port = port
                self.timeout = timeout
                self.context = context

            def request(self, method, target, body, headers):
                requests.append(
                    {
                        "method": method,
                        "target": target,
                        "body": body.read(),
                        "headers": dict(headers),
                        "host": self.host,
                    }
                )

            @staticmethod
            def getresponse():
                return Response()

            @staticmethod
            def close():
                return None

        coordinator = mock.Mock()
        with mock.patch.object(
            self.runtime.http.client,
            "HTTPSConnection",
            Connection,
        ), mock.patch.object(
            self.runtime,
            "_SLACK_RETRY_COORDINATOR",
            coordinator,
        ):
            self.runtime._upload_slack_bytes(
                "test-token",
                "https://files.slack.com/upload/v1/signed?part=1",
                staged,
            )

        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]["host"], "files.slack.com")
        self.assertEqual(requests[0]["body"], b"immutable bytes")
        self.assertNotIn("Authorization", requests[0]["headers"])
        self.assertEqual(
            requests[0]["target"],
            "/upload/v1/signed?part=1",
        )
        method_key = coordinator.wait.call_args.args[0]
        self.assertEqual(method_key.method, "files.externalUpload")
        self.assertTrue(method_key.workspace_id)


if __name__ == "__main__":
    unittest.main()
