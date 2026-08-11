from __future__ import annotations

import contextlib
import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path.insert(0, str(SERVER))

from recall_server.cli import main  # noqa: E402


class _Archive:
    def __init__(self) -> None:
        self.values: dict | None = None

    def put_raw(self, **values):
        self.values = values
        return {
            "object_key": "objects/aa/" + "a" * 64,
            "content_sha256": hashlib.sha256(values["payload"]).hexdigest(),
            "size_bytes": len(values["payload"]),
        }


class ArchilToolPublishTest(unittest.TestCase):
    def test_published_tool_identity_is_archils_arm64_runtime(self) -> None:
        archive = _Archive()
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "duckdb"
            artifact.write_bytes(b"x" * 1_000_000)
            digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            output = io.StringIO()
            with (
                mock.patch.object(
                    sys,
                    "argv",
                    [
                        "recall-server",
                        "publish-archil-duckdb",
                        "--path",
                        str(artifact),
                        "--version",
                        "1.5.5",
                        "--sha256",
                        digest,
                    ],
                ),
                mock.patch(
                    "recall_server.cli.build_evidence_archive_store",
                    return_value=archive,
                ),
                contextlib.redirect_stdout(output),
            ):
                main()

        self.assertIsNotNone(archive.values)
        self.assertEqual(
            archive.values["native_id"],
            "archil-duckdb:1.5.5:linux-arm64",
        )
        result = json.loads(output.getvalue())
        self.assertEqual(result["status"], "published")
        self.assertNotIn(str(artifact), output.getvalue())


if __name__ == "__main__":
    unittest.main()
