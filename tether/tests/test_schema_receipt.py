from __future__ import annotations

import importlib
import json
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime"


def load_module(home: pathlib.Path):
    environment = {
        "HOME": str(home),
        "XDG_STATE_HOME": str(home / ".local" / "state"),
    }
    previous = list(sys.path)
    try:
        sys.path.insert(0, str(RUNTIME))
        for name in ("schema_receipt", "security"):
            sys.modules.pop(name, None)
        with mock.patch.dict(os.environ, environment, clear=False):
            return importlib.import_module("schema_receipt")
    finally:
        sys.path[:] = previous


class SchemaReceiptTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tether-schema-receipt-")
        self.home = pathlib.Path(self.temp.name)
        self.module = load_module(self.home)
        self.path = (
            self.home / ".local" / "state" / "tether-installer" / "schema" / "active.json"
        )
        self.path.parent.mkdir(parents=True, mode=0o700)
        self.env = mock.patch.dict(os.environ, {
            "HOME": str(self.home),
            "XDG_STATE_HOME": str(self.home / ".local" / "state"),
        }, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def create(self, **overrides):
        arguments = dict(
            operation="rehearse",
            from_schema=17,
            to_schema=18,
            database_device=11,
            database_inode=22,
            security_domain_id="sec_000000000000000000000000",
            predecessor_build_sha256="a" * 64,
            target_build_sha256="b" * 64,
            installed_manifest_sha256="c" * 64,
        )
        arguments.update(overrides)
        return self.module.create(self.path, **arguments)

    def test_round_trip_and_public_view_redaction(self):
        receipt = self.create()
        loaded, error = self.module.load(self.path)
        self.assertIsNone(error)
        self.assertEqual(loaded, receipt)
        public = self.module.public_view(loaded)
        self.assertEqual(
            set(public),
            {
                "version",
                "receipt_id",
                "operation",
                "phase",
                "from_schema",
                "to_schema",
                "error_code",
            },
        )
        self.assertNotIn(str(self.home), json.dumps(public))

    def test_second_operation_is_refused(self):
        self.create()
        with self.assertRaises(self.module.ReceiptError):
            self.create()

    def test_corrupt_and_unsafe_receipts_fail_closed(self):
        self.path.write_text("{not json", encoding="utf-8")
        self.path.chmod(0o600)
        loaded, error = self.module.load(self.path)
        self.assertIsNone(loaded)
        self.assertEqual(error, "invalid")

        self.path.write_text(json.dumps({"version": 1, "phase": "planned"}))
        self.path.chmod(0o600)
        loaded, error = self.module.load(self.path)
        self.assertIsNone(loaded)
        self.assertEqual(error, "invalid")

        self.path.unlink()
        receipt = self.create()
        del receipt
        self.path.chmod(0o640)
        loaded, error = self.module.load(self.path)
        self.assertIsNone(loaded)
        self.assertEqual(error, "unsafe")

    def test_advance_is_compare_and_swap(self):
        receipt = self.create()
        advanced = self.module.advance(self.path, expect=receipt, to_phase="quiesced")
        self.assertEqual(advanced["phase"], "quiesced")
        self.assertEqual(advanced["phase_seq"], 1)
        with self.assertRaises(self.module.ReceiptError) as caught:
            self.module.advance(self.path, expect=receipt, to_phase="singleton_acquired")
        self.assertEqual(caught.exception.code, "receipt_phase_conflict")

        drifted = dict(advanced)
        drifted["database_inode"] = 999
        with self.assertRaises(self.module.ReceiptError) as caught:
            self.module.advance(self.path, expect=drifted, to_phase="singleton_acquired")
        self.assertEqual(caught.exception.code, "receipt_identity_changed")

    def test_phases_only_advance_forward_and_terminals_are_final(self):
        receipt = self.create()
        quiesced = self.module.advance(self.path, expect=receipt, to_phase="quiesced")
        with self.assertRaises(self.module.ReceiptError):
            self.module.advance(self.path, expect=quiesced, to_phase="planned")
        with self.assertRaises(self.module.ReceiptError):
            self.module.advance(self.path, expect=quiesced, to_phase="failed_safe")
        held = self.module.advance(
            self.path,
            expect=quiesced,
            to_phase="failed_safe",
            error_code="test_failure",
        )
        self.assertEqual(held["error_code"], "test_failure")
        with self.assertRaises(self.module.ReceiptError) as caught:
            self.module.advance(self.path, expect=held, to_phase="complete")
        self.assertEqual(caught.exception.code, "receipt_terminal")

    def gate(self):
        return self.module.runtime_gate_error(
            runtime_schema_version=17,
            path=self.path,
        )

    def test_runtime_gate_blocks_every_incomplete_phase(self):
        # A live-mutating operation gates the runtime at every incomplete
        # phase. (A rehearsal that never reached db_committed does not —
        # see SafeHoldRecoveryTest.)
        self.assertIsNone(self.gate())
        receipt = self.create(operation="migrate")
        self.assertEqual(self.gate(), "schema_operation_incomplete")
        for phase in ("quiesced", "singleton_acquired", "backup_verified", "db_committed", "runtime_verified"):
            receipt = self.module.advance(self.path, expect=receipt, to_phase=phase)
            self.assertEqual(self.gate(), "schema_operation_incomplete", phase)
        receipt = self.module.advance(self.path, expect=receipt, to_phase="resumed")
        self.assertIsNone(self.gate())
        receipt = self.module.advance(self.path, expect=receipt, to_phase="complete")
        self.assertEqual(self.gate(), "schema_receipt_runtime_conflict")
        self.assertIsNone(
            self.module.runtime_gate_error(runtime_schema_version=18, path=self.path)
        )

    def test_runtime_gate_blocks_corrupt_and_safe_hold_receipts(self):
        self.path.write_text("junk", encoding="utf-8")
        self.path.chmod(0o600)
        self.assertEqual(self.gate(), "schema_receipt_invalid")
        self.path.unlink()
        receipt = self.create(operation="migrate")
        self.module.advance(
            self.path,
            expect=receipt,
            to_phase="needs_operator",
            error_code="ambiguous",
        )
        self.assertEqual(self.gate(), "schema_operation_incomplete")

    def test_maintenance_flag_lifecycle(self):
        flag = self.path.parent / "maintenance"
        self.assertFalse(self.module.maintenance_armed(flag))
        self.module.arm_maintenance(flag)
        self.assertTrue(self.module.maintenance_armed(flag))
        with self.assertRaises(OSError):
            self.module.arm_maintenance(flag)
        self.module.disarm_maintenance(flag)
        self.assertFalse(self.module.maintenance_armed(flag))
        self.module.disarm_maintenance(flag)

    def test_classify_is_single_valued(self):
        self.assertEqual(self.module.classify(None, None), "no_operation")
        self.assertEqual(self.module.classify(None, "invalid"), "invalid_receipt")
        receipt = self.create()
        self.assertEqual(self.module.classify(receipt, None), "incomplete_planned")


if __name__ == "__main__":
    unittest.main()


class SafeHoldRecoveryTest(SchemaReceiptTest):
    """A read-only rehearsal must never be able to brick the broker."""

    def held_rehearsal(self, *, committed: bool):
        receipt = self.create(operation="rehearse")
        phase = "quiesced"
        receipt = self.module.advance(self.path, expect=receipt, to_phase=phase)
        if committed:
            receipt = self.module.advance(
                self.path, expect=receipt, to_phase="db_committed"
            )
        return self.module.advance(
            self.path,
            expect=receipt,
            to_phase="failed_safe",
            error_code="gateway_still_active",
        )

    def test_failed_rehearsal_that_never_touched_the_database_still_boots(self):
        self.held_rehearsal(committed=False)
        self.assertIsNone(self.gate())

    def test_failed_operation_past_db_commit_still_blocks_boot(self):
        self.held_rehearsal(committed=True)
        self.assertEqual(self.gate(), "schema_operation_incomplete")

    def test_live_migration_hold_always_blocks_boot(self):
        receipt = self.create(operation="migrate")
        self.module.advance(
            self.path,
            expect=receipt,
            to_phase="needs_operator",
            error_code="ambiguous",
        )
        self.assertEqual(self.gate(), "schema_operation_incomplete")

    def test_operator_can_resolve_a_safe_hold_and_plan_again(self):
        held = self.held_rehearsal(committed=True)
        self.assertEqual(self.gate(), "schema_operation_incomplete")

        archived = self.module.resolve_safe_hold(
            self.path, expect=held, resolution="abandoned"
        )

        self.assertEqual(archived["resolution"], "abandoned")
        # Slot is clear: the runtime boots and a fresh operation can start.
        self.assertIsNone(self.gate())
        self.create(operation="migrate")
        # The held receipt's evidence is preserved, not destroyed.
        archive = self.path.with_name(f"resolved-{held['receipt_id']}.json")
        self.assertTrue(archive.exists())

    def test_resolution_refuses_non_held_and_drifted_receipts(self):
        receipt = self.create()
        with self.assertRaises(self.module.ReceiptError) as caught:
            self.module.resolve_safe_hold(
                self.path, expect=receipt, resolution="abandoned"
            )
        self.assertEqual(caught.exception.code, "receipt_not_held")

        held = self.module.advance(
            self.path, expect=receipt, to_phase="failed_safe", error_code="x"
        )
        drifted = dict(held)
        drifted["database_inode"] = 999
        with self.assertRaises(self.module.ReceiptError) as caught:
            self.module.resolve_safe_hold(
                self.path, expect=drifted, resolution="abandoned"
            )
        self.assertEqual(caught.exception.code, "receipt_identity_changed")
