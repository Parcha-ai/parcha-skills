"""Explicit opt-in and epoch are required before sparse metadata reads."""

import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from recall_server.parquet_scan import CanonicalParquetScanProjector, ParquetScanError


class SparseSettingsTests(unittest.TestCase):
    def settings(self, flag, epoch=""):
        with patch.dict(
            os.environ,
            {"RECALL_PARQUET_SPARSE_PLAN": flag, "RECALL_PARQUET_DIRTY_EPOCH": epoch},
        ):
            return CanonicalParquetScanProjector(None, SimpleNamespace(archive=None))

    def test_disabled_requires_no_epoch_or_database(self):
        self.assertIsNone(self.settings("0").sparse_plan_epoch)

    def test_enabled_requires_explicit_nonempty_epoch(self):
        for epoch in ("", "   "):
            with (
                self.subTest(epoch=epoch),
                self.assertRaisesRegex(ParquetScanError, "epoch_required"),
            ):
                self.settings("1", epoch)
        self.assertTrue(
            self.settings("1", "synthetic-complete-writers").sparse_plan_epoch.endswith(
                ":synthetic-complete-writers"
            )
        )

    def test_invalid_flag_does_not_silently_select_a_mode(self):
        for flag in ("true", "false", "2", "", " 1"):
            with (
                self.subTest(flag=flag),
                self.assertRaisesRegex(ParquetScanError, "flag_invalid"),
            ):
                self.settings(flag, "synthetic-complete-writers")
