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
        self.assertEqual(self.versions(74, False), [*range(1, 71), 72, 73, 74])
        self.assertEqual(self.versions(75, False), [*range(1, 71), 72, 73, 74, 75])
        self.assertEqual(self.versions(76, False), [*range(1, 71), 72, 73, 74, 75, 76])
        self.assertEqual(self.versions(77, False), [*range(1, 71), 72, 73, 74, 75, 76, 77])
        self.assertEqual(self.versions(78, False), [*range(1, 71), 72, 73, 74, 75, 76, 77, 78])
        self.assertEqual(self.versions(79, False), [*range(1, 71), 72, 73, 74, 75, 76, 77, 78, 79])
        self.assertEqual(self.versions(80, False), [*range(1, 71), 72, 73, 74, 75, 76, 77, 78, 79, 80])

        self.assertEqual(self.versions(81, False), [*range(1, 71), *range(72, 82)])
        self.assertEqual(self.versions(82, False), [*range(1, 71), *range(72, 83)])

    def test_certificate_schema_is_explicitly_supported(self):
        self.assertEqual(self.versions(83,False),[*range(1,71),*range(72,84)])
        self.assertEqual(self.versions(83,True),list(range(1,84)))

    def test_append_checkpoint_schema_is_explicitly_supported(self):
        self.assertEqual(self.versions(84,False),[*range(1,71),*range(72,85)])
        self.assertEqual(self.versions(84,True),list(range(1,85)))

    def test_remainder_schema_is_explicitly_supported(self):
        self.assertEqual(self.versions(85,False),[*range(1,71),*range(72,86)])
        self.assertEqual(self.versions(85,True),list(range(1,86)))

    def test_audit_checkpoint_schema_is_explicitly_supported(self):
        self.assertEqual(self.versions(86, False), [*range(1, 71), *range(72, 87)])
        self.assertEqual(self.versions(86, True), list(range(1, 87)))

    def test_actor_checkpoint_schema_is_explicitly_supported(self):
        self.assertEqual(self.versions(87, False), [*range(1, 71), *range(72, 88)])
        self.assertEqual(self.versions(87, True), list(range(1, 88)))

    def test_known_071_can_ship_without_creating_a_gap(self):
        self.assertEqual(self.versions(71, True), list(range(1, 72)))
        self.assertEqual(self.versions(72, True), list(range(1, 73)))
        self.assertEqual(self.versions(73, True), list(range(1, 74)))
        self.assertEqual(self.versions(74, True), list(range(1, 75)))
        self.assertEqual(self.versions(75, True), list(range(1, 76)))
        self.assertEqual(self.versions(76, True), list(range(1, 77)))
        self.assertEqual(self.versions(77, True), list(range(1, 78)))
        self.assertEqual(self.versions(78, True), list(range(1, 79)))
        self.assertEqual(self.versions(79, True), list(range(1, 80)))
        self.assertEqual(self.versions(80, True), list(range(1, 81)))

        self.assertEqual(self.versions(81, True), list(range(1, 82)))
        self.assertEqual(self.versions(82, True), list(range(1, 83)))

    def test_declared_latest_must_equal_last_shipped_version(self):
        for latest, native_file in ((71, False), (70, True)):
            with self.subTest(latest=latest, native_file=native_file):
                with self.assertRaisesRegex(AssertionError, "latest shipped"):
                    self.versions(latest, native_file)

    def test_unknown_latest_is_not_implicitly_allowed(self):
        for latest in (69, 88):
            with self.subTest(latest=latest):
                with self.assertRaisesRegex(AssertionError, "unreviewed latest"):
                    self.versions(latest, False)
