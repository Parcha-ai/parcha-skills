from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from connectors.host import (
    ConnectorHostConfig,
    ConnectorHostError,
    HOSTED_FACTORIES,
    REMOTE_SELECTOR_FIELDS,
    build_host,
)
from connectors.registry import definition
from connectors.remote_worker import (
    REMOTE_WORKER_CONNECTORS,
    preview_remote_worker_config,
    validate_remote_worker_config,
)


def schedule(index: int, connector_id: str) -> dict:
    return {
        "schema_version": 1,
        "job_key": f"{index:064x}",
        "connector_id": connector_id,
        "generation": 1,
        "enabled": False,
        "interval_seconds": 300,
        "jitter_seconds": 0,
        "transient_base_seconds": 5,
        "max_backoff_seconds": 300,
        "lease_seconds": 60,
        "max_rate_limit_seconds": 3600,
    }


SELECTORS = {
    "google.gmail": {
        "include_attachments": False,
        "own_addresses": ["owner@example.invalid"],
        "label_ids": ["INBOX"],
        "query": None,
        "include_spam_trash": False,
    },
    "google.calendar": {
        "calendar_id": "primary",
        "time_min": None,
        "time_max": None,
    },
    "google.contacts": {},
    "google.drive": {
        "drive_id": None,
        "mime_types": [],
        "include_document_text": False,
    },
    "github.activity": {
        "owner": "synthetic-org",
        "repository": "synthetic-repo",
    },
    "linear.activity": {"team_id": "team-synthetic"},
    "slack.messages": {
        "workspace_id": "T123", "channel_ids": ["C123"],
        "owner_user_ids": ["U111"],
    },
    "notion.workspace": {},
    "x.activity": {"user_id": "999", "streams": ["bookmark", "mention", "own"]},
}


def config(root: Path) -> dict:
    jobs = []
    for index, connector_id in enumerate(REMOTE_WORKER_CONNECTORS, 1):
        jobs.append({
            "schedule": schedule(index, connector_id),
            "source_id": f"synthetic:remote:{index}",
            "endpoint": "https://brain.example.invalid",
            "brain_authority": {
                "kind": "file",
                "path": str(root / f"brain-{index}.token"),
            },
            "privacy_mode": "scrub",
            "connector": {
                "source_authority": {
                    "kind": "file",
                    "path": str(root / f"source-{index}.token"),
                },
                "spool": str(root / f"spool-{index}.db"),
                "page_size": 10,
                "timeout_seconds": 10,
                "selectors": copy.deepcopy(SELECTORS[connector_id]),
            },
        })
    return {"schema_version": 1, "jobs": jobs}


class RemoteWorkerConfigTest(unittest.TestCase):
    def test_registry_selectors_exactly_match_the_shipped_worker_config(self):
        self.assertEqual(set(REMOTE_WORKER_CONNECTORS), set(REMOTE_SELECTOR_FIELDS))
        for connector_id in REMOTE_WORKER_CONNECTORS:
            self.assertEqual(
                set(definition(connector_id).selection_fields),
                REMOTE_SELECTOR_FIELDS[connector_id],
            )

    def test_nine_remote_jobs_preview_without_authority_source_network_or_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            value = ConnectorHostConfig.from_mapping(config(Path(directory)))
            with mock.patch("connectors.host.load_file_token") as brain_read, \
                 mock.patch("pathlib.Path.read_bytes") as source_read, \
                 mock.patch("urllib.request.urlopen") as network:
                validate_remote_worker_config(value)
                preview = preview_remote_worker_config(value)
            brain_read.assert_not_called()
            source_read.assert_not_called()
            network.assert_not_called()
        self.assertEqual(preview["profile"], "remote_worker")
        self.assertEqual(preview["jobs"], 9)
        self.assertEqual(preview["enabled"], 0)
        self.assertEqual(preview["credential_reads"], 0)
        self.assertEqual(preview["source_reads"], 0)
        self.assertEqual(preview["network_requests"], 0)
        self.assertEqual(preview["writes"], 0)
        rendered = json.dumps(preview, sort_keys=True)
        self.assertNotIn("synthetic:", rendered)
        self.assertNotIn("/tmp/", rendered)

    def test_selectors_are_closed_typed_and_cannot_define_transport_or_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            original = config(Path(directory))
            for field, value in (
                ("base_url", "https://elsewhere.invalid"),
                ("query_recipe", {"method": "DELETE"}),
                ("token", "not-a-real-token"),
            ):
                changed = copy.deepcopy(original)
                changed["jobs"][4]["connector"]["selectors"][field] = value
                with self.subTest(field=field), self.assertRaises(ConnectorHostError):
                    ConnectorHostConfig.from_mapping(changed)
            changed = copy.deepcopy(original)
            changed["jobs"][8]["connector"]["selectors"]["streams"].append("home")
            with self.assertRaisesRegex(ConnectorHostError, "invalid_remote_selectors"):
                ConnectorHostConfig.from_mapping(changed)
            for invalid in (["INBOX", 7], [{"label": "INBOX"}]):
                changed = copy.deepcopy(original)
                changed["jobs"][0]["connector"]["selectors"]["label_ids"] = invalid
                with self.subTest(invalid=invalid), self.assertRaisesRegex(
                    ConnectorHostError, "invalid_remote_selectors"
                ):
                    ConnectorHostConfig.from_mapping(changed)

    def test_writable_state_cannot_alias_an_authority_file(self):
        with tempfile.TemporaryDirectory() as directory:
            original = config(Path(directory))
            for authority in ("brain_authority", "source_authority"):
                changed = copy.deepcopy(original)
                reference = (
                    changed["jobs"][0][authority]
                    if authority == "brain_authority"
                    else changed["jobs"][0]["connector"][authority]
                )
                changed["jobs"][0]["connector"]["spool"] = reference["path"]
                with self.subTest(authority=authority), self.assertRaisesRegex(
                    ConnectorHostError, "authority_durable_path_alias"
                ):
                    ConnectorHostConfig.from_mapping(changed)

    def test_remote_profile_rejects_local_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            value = config(Path(directory))
            value["jobs"] = [{
                "schedule": schedule(1, "openai.export-inbox"),
                "source_id": "synthetic:local:export",
                "endpoint": "https://brain.example.invalid",
                "brain_authority": {
                    "kind": "file",
                    "path": str(Path(directory) / "brain.token"),
                },
                "privacy_mode": "scrub",
                "connector": {
                    "inbox": str(Path(directory) / "inbox"),
                    "catalog": str(Path(directory) / "catalog.db"),
                    "spool": str(Path(directory) / "spool.db"),
                    "page_size": 10,
                },
            }]
            parsed = ConnectorHostConfig.from_mapping(value)
            with self.assertRaisesRegex(ConnectorHostError, "non_remote_connector"):
                validate_remote_worker_config(parsed)


class RemoteWorkerFactoryTest(unittest.TestCase):
    def test_every_remote_definition_is_bundled_without_discovery(self):
        self.assertEqual(
            set(REMOTE_WORKER_CONNECTORS),
            set(HOSTED_FACTORIES) - {"openai.export-inbox", "grep.ai"},
        )
        self.assertNotIn("entrypoint", HOSTED_FACTORIES)
        with self.assertRaises(TypeError):
            HOSTED_FACTORIES["runtime.plugin"] = object()

    def test_build_uses_explicit_injected_rails_and_separate_authorities(self):
        with tempfile.TemporaryDirectory() as directory:
            value = config(Path(directory))
            value["jobs"] = value["jobs"][4:8]
            parsed = ConnectorHostConfig.from_mapping(value)
            rails = {
                connector_id: mock.Mock(request=mock.Mock())
                for connector_id in (
                    "github.activity",
                    "linear.activity",
                    "slack.messages",
                    "notion.workspace",
                )
            }
            with mock.patch("connectors.host.load_file_token", return_value="brain-authority") as brain, \
                 mock.patch("connectors.host.BrainClient") as brain_type, \
                 mock.patch("connectors.host.ConnectorRunner") as runner_type:
                runner_type.side_effect = lambda **_kwargs: mock.Mock(
                    run_once=mock.Mock(return_value={"status": "committed"}),
                    close=mock.Mock(),
                )
                worker = build_host(
                    parsed,
                    state_path=Path(directory) / "supervisor.db",
                    remote_rails=rails,
                )
            self.assertEqual(brain.call_count, 4)
            self.assertEqual(brain_type.call_count, 4)
            self.assertEqual(runner_type.call_count, 4)
            self.assertEqual(
                [connector.connector_id for connector in worker.connectors],
                [
                    "github.activity",
                    "linear.activity",
                    "slack.messages",
                    "notion.workspace",
                ],
            )
            worker.close()


if __name__ == "__main__":
    unittest.main()
