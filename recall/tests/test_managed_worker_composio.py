from __future__ import annotations

import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from connectors.composio_workspace_rail import ComposioWorkspaceRail
from connectors.work_apis import SlackPublicHistoryRail
from server.recall_server.control import ControlError
from server.recall_server.managed_worker import (
    ManagedConnectorWorker,
    _private_root,
    _run_projection_cycle,
)


class FakeLogicalProjector:
    def __init__(self):
        self.calls = []

    def project_pending(self, **kwargs):
        self.calls.append(kwargs)
        return {"documents": 2, "records": 8}


class FakePassageProjector:
    def __init__(self):
        self.project_calls = []
        self.embed_calls = []

    def project_pending(self, **kwargs):
        self.project_calls.append(kwargs)
        return {"documents": 2, "passages": 5}

    def embed_pending(self, **kwargs):
        self.embed_calls.append(kwargs)
        return {"processed": 5}


class FakeScanProjector:
    def __init__(self):
        self.calls = []

    def project_pending(self, **kwargs):
        self.calls.append(kwargs)
        return {"shards": 1, "rows": 13, "stale": 0}


class ManagedWorkerComposioTests(unittest.TestCase):
    def test_managed_projection_cycle_drains_logical_passages_and_embeddings(self):
        logical = FakeLogicalProjector()
        passages = FakePassageProjector()
        scan = FakeScanProjector()
        result = _run_projection_cycle(
            logical,
            passages,
            scan,
        )
        self.assertEqual(
            result,
            {
                "status": "complete",
                "logical_documents": 2,
                "logical_records": 8,
                "passage_documents": 2,
                "passages": 5,
                "passage_embeddings": 5,
                "parquet_shards": 1,
                "parquet_rows": 13,
                "parquet_stale": 0,
            },
        )
        self.assertEqual(
            logical.calls,
            [
                {
                    "tenant_id": None,
                    "batch_size": 20,
                    "max_batches": 1,
                    "upload_concurrency": 4,
                }
            ],
        )
        self.assertEqual(passages.project_calls[0]["tenant_id"], None)
        self.assertEqual(passages.project_calls[0]["batch_size"], 20)
        self.assertEqual(passages.project_calls[0]["concurrency"], 4)
        self.assertEqual(passages.embed_calls[0]["batch_size"], 100)
        self.assertEqual(scan.calls[0]["tenant_id"], None)

    def test_private_root_normalizes_provider_mount_mode_without_following_links(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            provider_mount = root / "worker"
            provider_mount.mkdir(mode=0o755)

            self.assertEqual(_private_root(provider_mount), provider_mount)
            self.assertEqual(stat.S_IMODE(provider_mount.stat().st_mode), 0o700)

            target = root / "target"
            target.mkdir(mode=0o700)
            linked = root / "linked"
            linked.symlink_to(target, target_is_directory=True)
            with self.assertRaisesRegex(
                ValueError,
                "managed worker state root is not private",
            ):
                _private_root(linked)

    def worker(self, root: Path) -> ManagedConnectorWorker:
        worker = object.__new__(ManagedConnectorWorker)
        worker.spool_root = root / "spools"
        worker.spool_root.mkdir(mode=0o700)
        worker.remote_rails = {}
        return worker

    def row(self):
        return {
            "id": "synthetic-installation",
            "connector_id": "google.gmail",
            "provider": "composio",
            "source_id": "synthetic:google:gmail",
            "privacy_mode": "scrub",
            "selectors": {"own_addresses": [], "label_ids": []},
        }

    def credentials(self):
        return {
            "user_id": "principal_synthetic_owner",
            "connected_account_id": "ca_synthetic_account_123",
            "toolkit": "gmail",
        }

    def test_composio_connection_builds_without_materializing_provider_credentials(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "authority"
            private.mkdir(mode=0o700)
            worker = self.worker(root)
            with patch.dict(
                os.environ,
                {"RECALL_COMPOSIO_API_KEY": "synthetic-project-authority"},
                clear=True,
            ):
                connector, spool = worker._build_default(
                    self.row(), self.credentials(), private
                )
            self.assertIsInstance(connector.rail, ComposioWorkspaceRail)
            self.assertEqual(connector.rail.user_id, "principal_synthetic_owner")
            self.assertEqual(
                connector.rail.connected_account_id,
                "ca_synthetic_account_123",
            )
            self.assertEqual(connector.rail.toolkit, "gmail")
            self.assertEqual(connector.page_size, 10)
            self.assertEqual(list(private.iterdir()), [])
            self.assertEqual(spool, worker.spool_root / "synthetic-installation.db")

    def test_toolkit_mismatch_and_missing_project_authority_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "authority"
            private.mkdir(mode=0o700)
            worker = self.worker(root)
            wrong = {**self.credentials(), "toolkit": "googlecalendar"}
            with (
                patch.dict(
                    os.environ,
                    {"RECALL_COMPOSIO_API_KEY": "synthetic-project-authority"},
                    clear=True,
                ),
                self.assertRaisesRegex(ControlError, "connector_authority_revoked"),
            ):
                worker._build_default(self.row(), wrong, private)
            with (
                patch.dict(os.environ, {}, clear=True),
                self.assertRaisesRegex(ControlError, "connector_authority_revoked"),
            ):
                worker._build_default(self.row(), self.credentials(), private)

    def test_slack_dual_authority_is_ephemeral_split_and_public_history_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / "authority"
            private.mkdir(mode=0o700)
            worker = self.worker(root)
            stale_bot_only_rail = object()
            worker.remote_rails = {"slack.messages": stale_bot_only_rail}
            row = {
                "id": "synthetic-slack-installation",
                "connector_id": "slack.messages",
                "provider": "slack",
                "source_id": "synthetic:slack:public-history",
                "privacy_mode": "scrub",
                "selectors": {
                    "workspace_id": "T123",
                    "channel_ids": [],
                    "owner_user_ids": ["U111"],
                },
            }
            connector, spool = worker._build_default(row, {
                "bot_access_token": "synthetic-bot-authority",
                "user_access_token": "synthetic-user-authority",
            }, private)

            self.assertIsInstance(connector.rail, SlackPublicHistoryRail)
            self.assertIsNot(connector.rail, stale_bot_only_rail)
            self.assertTrue(connector.public_history)
            self.assertEqual(
                {path.name for path in private.iterdir()},
                {"slack-bot-authority", "slack-user-authority"},
            )
            self.assertTrue(all(
                stat.S_IMODE(path.stat().st_mode) == 0o600
                for path in private.iterdir()
            ))
            self.assertEqual(
                spool, worker.spool_root / "synthetic-slack-installation.db",
            )

            second_private = root / "incomplete-authority"
            second_private.mkdir(mode=0o700)
            with self.assertRaisesRegex(ControlError, "connector_authority_revoked"):
                worker._build_default(row, {
                    "bot_access_token": "synthetic-bot-authority",
                }, second_private)


if __name__ == "__main__":
    unittest.main()
