"""Explicit schema-72 compatibility; no DDL or notification feature behavior."""

import itertools
import unittest

from tests.central_brain.test_core_deployment import healthy_snapshot
from recall_server.capabilities import CapabilityError, assess_snapshot


class NotificationSchemaCompatibility(unittest.TestCase):
    def snapshot(
        self,
        *,
        retired=True,
        reconciliation=True,
        conversation=False,
        notification=True,
    ):
        snapshot = healthy_snapshot()
        snapshot["migration_versions"] = [n for n in range(1, 70) if retired or n != 67]
        for enabled, version in (
            (reconciliation, 70),
            (conversation, 71),
            (notification, 72),
        ):
            if enabled:
                snapshot["migration_versions"].append(version)
        snapshot["postgres_vector_plane_present"] = not retired
        return snapshot

    def test_notification_marker_is_optional_and_independent_of_other_optionals(self):
        for retired, reconciliation, conversation, notification in itertools.product(
            (False, True), repeat=4
        ):
            with self.subTest(
                retired=retired,
                reconciliation=reconciliation,
                conversation=conversation,
                notification=notification,
            ):
                snapshot = self.snapshot(
                    retired=retired,
                    reconciliation=reconciliation,
                    conversation=conversation,
                    notification=notification,
                )
                try:
                    result = assess_snapshot(snapshot)
                except CapabilityError as error:
                    self.fail(f"known optional marker rejected: {error.code}")
                self.assertEqual(result["status"], "ready")
                self.assertEqual(
                    result["schema_version"], max(snapshot["migration_versions"])
                )
                self.assertEqual(
                    result["postgres_vector_plane"], "retired" if retired else "present"
                )

    def test_marker_cannot_hide_missing_mandatory_or_unknown_versions(self):
        complete = list(range(1, 73))
        for versions in (
            [n for n in complete if n != 68],
            [n for n in complete if n != 69],
            complete + [73],
            list(range(1, 71)) + [73],
            complete + [72],
            complete[:-2] + [72, 71],
            [0] + complete,
        ):
            with self.subTest(versions=versions):
                snapshot = self.snapshot()
                snapshot["migration_versions"] = versions
                with self.assertRaises(CapabilityError) as raised:
                    assess_snapshot(snapshot)
                self.assertEqual(raised.exception.code, "schema_drift")

    def test_retirement_consistency_remains_required_with_72(self):
        for retired in (False, True):
            with self.subTest(retired=retired):
                snapshot = self.snapshot(retired=retired)
                snapshot["postgres_vector_plane_present"] = retired
                with self.assertRaises(CapabilityError) as raised:
                    assess_snapshot(snapshot)
                self.assertEqual(raised.exception.code, "schema_drift")

    def test_known_marker_does_not_relax_tls_role_or_privileges(self):
        for field, change, code in (
            ("ssl_in_use", False, "tls_not_active"),
            ("role", {"superuser": True}, "role_privilege_excessive"),
            (
                "privileges",
                {"schema_migrations_readonly": False},
                "role_privilege_insufficient",
            ),
        ):
            with self.subTest(field=field):
                snapshot = self.snapshot()
                if isinstance(change, dict):
                    snapshot[field].update(change)
                else:
                    snapshot[field] = change
                with self.assertRaises(CapabilityError) as raised:
                    assess_snapshot(snapshot)
                self.assertEqual(raised.exception.code, code)


if __name__ == "__main__":
    unittest.main()
