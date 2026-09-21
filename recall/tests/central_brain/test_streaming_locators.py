"""Streaming locator outputs are private and cannot become saved write proof."""

from pathlib import Path
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from unittest.mock import Mock, patch

RECALL = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(RECALL), str(RECALL / "server")]
from recall_server.streaming_locators import (
    LocatorPublicationError,
    publish_parent_locators,
)


class StreamingLocatorTests(unittest.TestCase):
    def cli(self):
        spec = importlib.util.spec_from_file_location(
            "locator_cli", RECALL / "scripts/publish_parent_locators.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def args(self):
        return [
            "--tenant-id",
            "tenant:private",
            "--source-id",
            "source:private",
            "--native-parent-id",
            "parent:private",
            "--owner-principal-id",
            "principal:private",
        ]

    def test_dry_default_and_private_scope_report(self):
        module = self.cli()
        result = dict(
            status="dry_run",
            complete=True,
            proposed_documents=20_003,
            plan={"tenant_id": "tenant:private", "proof_sha256": "f" * 64},
        )
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "report.json"
            output = StringIO()
            with (
                patch.dict(os.environ, RECALL_DATABASE_URL="synthetic"),
                patch.object(module, "BrainStore"),
                patch.object(module, "build_evidence_archive_store"),
                patch.object(
                    module, "publish_parent_locators", return_value=result
                ) as publish,
                redirect_stdout(output),
            ):
                self.assertEqual(
                    module.main(self.args() + ["--report-file", str(target)]), 0
                )
            self.assertFalse(publish.call_args.kwargs["apply"])
            self.assertNotIn("private", output.getvalue())
            self.assertEqual(
                json.loads(output.getvalue())["proposed_documents"], 20_003
            )
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            self.assertEqual(
                json.loads(target.read_text())["plan"]["tenant_id"], "tenant:private"
            )

    def test_partial_and_unknown_commit_are_not_success(self):
        module = self.cli()
        failure = LocatorPublicationError("locator_publication_batch_unavailable")
        failure.committed = dict(batches=2, published_documents=128)
        failure.commit_unknown = True
        with (
            patch.dict(os.environ, RECALL_DATABASE_URL="synthetic"),
            patch.object(module, "BrainStore"),
            patch.object(module, "build_evidence_archive_store"),
            patch.object(module, "publish_parent_locators", side_effect=failure),
            redirect_stderr(StringIO()) as output,
        ):
            self.assertEqual(module.main(self.args() + ["--apply"]), 1)
            self.assertTrue(json.loads(output.getvalue())["commit_unknown"])
            self.assertEqual(
                json.loads(output.getvalue())["committed"]["published_documents"], 128
            )
        result = dict(status="partial", complete=False, plan={"proof_sha256": "f" * 64})
        with (
            patch.dict(os.environ, RECALL_DATABASE_URL="synthetic"),
            patch.object(module, "BrainStore"),
            patch.object(module, "build_evidence_archive_store"),
            patch.object(module, "publish_parent_locators", return_value=result),
            redirect_stdout(StringIO()),
        ):
            self.assertEqual(module.main(self.args() + ["--apply"]), 2)

    def test_invalid_scope_and_apply_type_do_no_io(self):
        store, archive = Mock(), Mock()
        for values in ({"tenant_id": "*"}, {"owner_principal_id": ""}, {"apply": 1}):
            scope = (
                dict(
                    tenant_id="tenant",
                    source_id="source",
                    native_parent_id="session",
                    owner_principal_id="principal",
                )
                | values
            )
            with self.assertRaises(LocatorPublicationError):
                publish_parent_locators(store, archive, **scope)
        store.connect.assert_not_called()
        archive.read_raw.assert_not_called()


if __name__ == "__main__":
    unittest.main()
