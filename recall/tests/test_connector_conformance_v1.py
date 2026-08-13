from __future__ import annotations

import json
import unittest

from connectors.conformance import (
    CONNECTOR_CONFORMANCE_CELLS,
    ConformanceReport,
    run_connector_conformance,
)
from connectors.registry import ConnectorDefinitionV3
from connectors.sdk import (
    ConnectorPage,
    ConnectorRateLimited,
    ConnectorRecordV2,
)
from connectors.source_plugin import (
    SourcePluginManifest,
    run_source_plugin_conformance,
)


PRIVATE_CANARY = "conformance-private-canary-77"


def record(native_id: str, text: str, *, deleted: bool = False) -> ConnectorRecordV2:
    return ConnectorRecordV2.from_mapping({
        "schema_version": 2,
        "native_id": native_id,
        "native_parent_id": "conversation-1",
        "occurred_at": "2026-07-18T00:00:00Z",
        "content": (
            {"kind": "communication_message.v1"}
            if deleted
            else {
                "kind": "communication_message.v1",
                "content_fidelity": "complete",
                "conversation_id": "conversation-1",
                "message_id": native_id,
                "direction": "inbound",
                "text": text,
            }
        ),
        "provenance": {"uri": "connector://conformance"},
        "deleted": deleted,
    })


def manifest(shape: str) -> ConnectorDefinitionV3:
    placements = {
        "remote": {
            "placement": {"execution": "remote_worker", "acquisition": ["poll"]},
            "auth": {
                "kind": "oauth2",
                "minimum_scopes": ["https://example.invalid/read"],
            },
            "command": "remote-sync",
        },
        "local": {
            "placement": {"execution": "source_local", "acquisition": ["snapshot"]},
            "auth": {
                "kind": "os_permission",
                "minimum_scopes": ["macos.synthetic_read"],
            },
            "command": "bridge-sync",
        },
        "import": {
            "placement": {"execution": "either", "acquisition": ["import"]},
            "auth": {"kind": "selected_export", "minimum_scopes": []},
            "command": "import-sync",
        },
    }
    selected = placements[shape]
    return ConnectorDefinitionV3.from_mapping({
        "schema_version": 3,
        "connector_id": f"synthetic.{shape}",
        "command": selected["command"],
        "mode": "pull",
        "authority_slots": ["brain", "source"],
        "source_family": "communications",
        "record_kinds": ["communication_message.v1"],
        "placement": selected["placement"],
        "auth": selected["auth"],
        "sync": {
            "backfill_modes": ["full", "incremental"],
            "checkpoint": "ack_cursor",
            "edit_semantics": "content_revision",
            "deletion_semantics": "explicit_upstream",
            "reconciliation": True,
        },
        "policy": {
            "visibility_modes": ["private"],
            "privacy_modes": ["drop", "scrub"],
            "default_privacy_mode": "scrub",
            "retention_modes": ["source_controlled"],
            "attachment_capability": False,
        },
        "selection_fields": ["account"],
    })


class FixtureConnector:
    def __init__(
        self,
        *,
        connector_id: str,
        source_id: str,
        pages: dict[str | None, ConnectorPage | object] | None = None,
        rate_limited: bool = False,
    ):
        self.connector_id = connector_id
        self.source_id = source_id
        self.pages = pages or {}
        self.rate_limited = rate_limited

    def pull(self, cursor: str | None):
        if self.rate_limited:
            raise ConnectorRateLimited(retry_after_seconds=120)
        return self.pages[cursor]


class ReferenceFactory:
    def __init__(self, shape: str, mutant: str | None = None):
        self.manifest = manifest(shape)
        self.source_id = f"synthetic:{shape}:conformance"
        self.mutant = mutant

    def build(self, scenario: str) -> FixtureConnector:
        connector_id = self.manifest.connector_id
        if self.mutant == "wrong_identity":
            connector_id = "synthetic.other"
        one = record("message-1", "synthetic first")
        if scenario == "content_fidelity":
            if self.mutant == "missing_fidelity":
                one.content.pop("content_fidelity")
            elif self.mutant == "empty_partial_omissions":
                one.content["content_fidelity"] = "partial"
                one.content["content_omissions"] = []
            elif self.mutant == "complete_with_omissions":
                one.content["content_omissions"] = ["body_truncated"]
            elif self.mutant == "unstable_omissions":
                one.content["content_fidelity"] = "partial"
                one.content["content_omissions"] = [
                    "snippet_fallback", "body_unavailable",
                ]
            elif self.mutant == "snippet_complete":
                one.content["format"] = "snippet"
        pages: dict[str | None, ConnectorPage | object]
        rate_limited = False
        if scenario in {"content_fidelity", "first_page", "lost_ack", "wire"}:
            pages = {None: ConnectorPage(records=(one,), next_cursor="done", has_more=False)}
        elif scenario == "pagination":
            pages = {
                None: ConnectorPage(records=(one,), next_cursor="page-1", has_more=True),
                "page-1": ConnectorPage(
                    records=(record("message-2", "synthetic second"),),
                    next_cursor=("page-1" if self.mutant == "cursor_stall" else "done"),
                    has_more=False,
                ),
            }
        elif scenario == "empty_page":
            pages = {None: ConnectorPage(records=(), next_cursor="done", has_more=False)}
        elif scenario == "revision":
            second = (
                one
                if self.mutant == "missing_revision"
                else record("message-1", "synthetic changed")
            )
            pages = {
                None: ConnectorPage(records=(one,), next_cursor="page-1", has_more=True),
                "page-1": ConnectorPage(records=(second,), next_cursor="done", has_more=False),
            }
        elif scenario == "tombstone":
            deleted = (
                ()
                if self.mutant == "missing_tombstone"
                else (record("message-1", "", deleted=True),)
            )
            pages = {
                None: ConnectorPage(records=(one,), next_cursor="page-1", has_more=True),
                "page-1": ConnectorPage(records=deleted, next_cursor="done", has_more=False),
            }
        elif scenario == "replay":
            second = (
                record("message-1", "synthetic changed")
                if self.mutant == "changed_replay"
                else one
            )
            pages = {
                None: ConnectorPage(records=(one,), next_cursor="page-1", has_more=True),
                "page-1": ConnectorPage(records=(second,), next_cursor="done", has_more=False),
            }
        elif scenario == "privacy_drop":
            pages = {None: ConnectorPage(
                records=(record("message-private", f"api_key={PRIVATE_CANARY}"),),
                next_cursor="done",
                has_more=False,
            )}
        elif scenario == "rate_limit":
            if self.mutant == "rate_limit_as_success":
                pages = {None: ConnectorPage(records=(), next_cursor="done", has_more=False)}
            else:
                pages = {}
                rate_limited = True
        elif scenario == "invalid_page":
            pages = {
                None: (
                    ConnectorPage(records=(), next_cursor="done", has_more=False)
                    if self.mutant == "invalid_as_success"
                    else {"status": "ok"}
                ),
            }
        else:
            raise AssertionError("unknown synthetic scenario")
        return FixtureConnector(
            connector_id=connector_id,
            source_id=self.source_id,
            pages=pages,
            rate_limited=rate_limited,
        )


class ConnectorConformanceTest(unittest.TestCase):
    def test_remote_local_and_import_shapes_pass_the_same_matrix(self):
        self.assertGreaterEqual(len(CONNECTOR_CONFORMANCE_CELLS), 12)
        for shape in ("remote", "local", "import"):
            with self.subTest(shape=shape):
                report = run_connector_conformance(ReferenceFactory(shape))
                self.assertIsInstance(report, ConformanceReport)
                self.assertEqual(report.passed, report.total)
                self.assertEqual(report.total, len(CONNECTOR_CONFORMANCE_CELLS))
                self.assertEqual(report.failures, ())
                public = report.to_public()
                self.assertEqual(public["status"], "pass")
                self.assertNotIn("source_id", public)
                self.assertNotIn(PRIVATE_CANARY, json.dumps(public))

    def test_external_source_plugin_runs_the_identical_conformance_matrix(self):
        reference = ReferenceFactory("remote")

        class PluginFactory:
            manifest = SourcePluginManifest(
                definition=reference.manifest,
                operations=("backfill", "reconcile"),
            )
            source_id = reference.source_id

            @staticmethod
            def build(scenario):
                connector = reference.build(scenario)
                connector.manifest = PluginFactory.manifest
                return connector

        report = run_source_plugin_conformance(PluginFactory())
        self.assertEqual(report.passed, report.total)

    def test_each_fake_success_mutant_fails_a_named_cell(self):
        mutants = (
            "wrong_identity",
            "cursor_stall",
            "missing_revision",
            "missing_tombstone",
            "changed_replay",
            "rate_limit_as_success",
            "invalid_as_success",
            "missing_fidelity",
            "empty_partial_omissions",
            "complete_with_omissions",
            "unstable_omissions",
            "snippet_complete",
        )
        for mutant in mutants:
            with self.subTest(mutant=mutant):
                report = run_connector_conformance(ReferenceFactory("remote", mutant))
                self.assertLess(report.passed, report.total)
                self.assertTrue(report.failures)
                self.assertTrue(set(report.failures).issubset(CONNECTOR_CONFORMANCE_CELLS))
                self.assertNotIn(PRIVATE_CANARY, json.dumps(report.to_public()))

    def test_report_rejects_empty_duplicate_and_unknown_cells(self):
        with self.assertRaises(ValueError):
            ConformanceReport(
                connector_id="synthetic.remote",
                passed=0,
                total=0,
                failures=(),
            )
        with self.assertRaises(ValueError):
            ConformanceReport(
                connector_id="synthetic.remote",
                passed=1,
                total=2,
                failures=("wire_round_trip", "wire_round_trip"),
            )
        with self.assertRaises(ValueError):
            ConformanceReport(
                connector_id="synthetic.remote",
                passed=1,
                total=2,
                failures=("made_up_success",),
            )


if __name__ == "__main__":
    unittest.main()
