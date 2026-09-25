"""The fixture contract accepts only the explicitly reserved optional gap."""
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import schema_versions


class ExpectedSchemaVersionsTests(unittest.TestCase):
    def versions(self, latest, native_file):
        with patch.object(schema_versions, "SCHEMA_VERSION", latest), patch.object(
            Path, "glob", return_value=[Path("071_native.sql")] if native_file else [],
        ):
            return schema_versions.expected_schema_versions()

    def test_core_is_exact_and_reserved_071_may_be_absent(self):
        self.assertEqual(self.versions(70, False), list(range(1, 71)))
        self.assertEqual(self.versions(72, False), [*range(1, 71), 72])
        self.assertEqual(self.versions(73, False), [*range(1, 71), 72, 73])

    def test_known_071_can_ship_without_creating_a_gap(self):
        self.assertEqual(self.versions(71, True), list(range(1, 72)))
        self.assertEqual(self.versions(72, True), list(range(1, 73)))
        self.assertEqual(self.versions(73, True), list(range(1, 74)))

    def test_declared_latest_must_equal_last_shipped_version(self):
        for latest, native_file in ((71, False), (70, True)):
            with self.subTest(latest=latest, native_file=native_file):
                with self.assertRaisesRegex(AssertionError, "latest shipped"):
                    self.versions(latest, native_file)

    def test_unknown_latest_is_not_implicitly_allowed(self):
        for latest in (69, 74):
            with self.subTest(latest=latest):
                with self.assertRaisesRegex(AssertionError, "unreviewed latest"):
                    self.versions(latest, False)
