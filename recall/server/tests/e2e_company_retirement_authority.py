#!/usr/bin/env python3
"""Company storage maintenance uses actual admin authority, never source impersonation."""

# ruff: noqa: E402
from contextlib import contextmanager
import os

import psycopg
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

SERVER = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(SERVER.parent), str(SERVER)]
import e2e_retirement_detachment as fixture
from recall_server import retirement_runner as runner
from recall_server import retirement_policy as policy
from recall_server.chunk_retirement import (
    ChunkRetirementError,
    ParentRetirementLimits,
    set_parent_retirement_enabled,
)


class CompanyAuthorityTests(fixture.DetachmentTests):
    def setUp(self):
        super().setUp()
        self.admin_principal = "principal:company-admin"
        self.organization = "organization:" + self.tenant
        self.admin_scope = runner.RetirementScope(
            self.tenant, self.admin_principal, (self.source,)
        )
        with self.store.connect() as c:
            c.execute(
                "INSERT INTO brain_principals VALUES(%s,%s,now())",
                (self.tenant, self.admin_principal),
            )
            c.execute(
                "INSERT INTO brain_organizations VALUES(%s,'company','Company maintenance test',now())",
                (self.organization,),
            )
            c.execute(
                "INSERT INTO brain_spaces(tenant_id,organization_id,brain_kind,slug) VALUES(%s,%s,'company','maintenance')",
                (self.tenant, self.organization),
            )
            c.execute(
                "INSERT INTO brain_memberships VALUES(%s,%s,'admin',now())",
                (self.organization, self.admin_principal),
            )
            c.execute(
                "INSERT INTO brain_access_grants VALUES(%s,%s,'admin',now())",
                (self.tenant, self.admin_principal),
            )
            c.execute(
                "INSERT INTO canonical_source_grants VALUES(%s,%s,%s,'read',now())",
                (self.tenant, self.admin_principal, self.source),
            )
            c.execute(
                "UPDATE canonical_chunk_retirement_progress SET updated_at=now()-interval '2 minutes' WHERE tenant_id=%s AND source_id=%s",
                self.key[:2],
            )
        p = patch.dict(os.environ, RECALL_CHUNK_BODY_READS="archive")
        p.start()
        self.addCleanup(p.stop)

    def acl(self):
        with self.store.connect() as c:
            owner = c.execute(
                "SELECT owner_principal_id FROM canonical_sources WHERE tenant_id=%s AND source_id=%s",
                self.key[:2],
            ).fetchone()
            grants = c.execute(
                "SELECT principal_id,permission FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s ORDER BY principal_id",
                self.key[:2],
            ).fetchall()
            return owner, grants

    def test_company_admin_clears_proved_body_without_source_owner_or_grant_changes(
        self,
    ):
        before = self.acl()
        result = runner.run_retirement(
            self.store, self.archive, scope=self.admin_scope, apply=True
        )
        self.assertEqual(result["cleared_documents"], 2)
        self.assertEqual(result["failed_parents"], 0)
        self.assertEqual(self.state()["status"], "complete")
        self.assertGreater(sum(self.archive.reads.values()), 0)
        self.assertEqual(self.acl(), before)

    def run_admin(self, **kwargs):
        return runner.run_retirement(
            self.store, self.archive, scope=self.admin_scope, apply=True, **kwargs
        )

    def update(self, sql, args):
        with self.store.connect() as c:
            c.execute(sql, args)

    def assert_rejected_before_archive(self):
        before = self.state()
        self.archive.reads.clear()
        with self.assertRaisesRegex(ChunkRetirementError, "retirement_owner_required"):
            self.run_admin()
        self.assertEqual(self.state(), before)
        self.assertFalse(self.archive.reads)

    def test_exact_source_owner_path_remains_valid(self):
        result = runner.run_retirement(
            self.store, self.archive, scope=self.scope, apply=True
        )
        self.assertEqual(result["cleared_documents"], 2)

    def test_company_admin_without_exact_source_grant_is_rejected(self):
        self.update(
            "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s",
            (*self.key[:2], self.admin_principal),
        )
        self.assert_rejected_before_archive()

    def test_org_member_with_brain_admin_access_is_rejected(self):
        self.update(
            "UPDATE brain_memberships SET role='member' WHERE organization_id=%s",
            (self.organization,),
        )
        self.assert_rejected_before_archive()

    def test_org_admin_with_brain_read_access_is_rejected(self):
        self.update(
            "UPDATE brain_access_grants SET permission='read' WHERE tenant_id=%s",
            (self.tenant,),
        )
        self.assert_rejected_before_archive()

    def test_personal_brain_is_rejected(self):
        self.update(
            "UPDATE brain_spaces SET brain_kind='personal' WHERE tenant_id=%s",
            (self.tenant,),
        )
        self.assert_rejected_before_archive()

    def test_another_tenant_admin_cannot_use_source_scope(self):
        self.admin_scope = runner.RetirementScope(
            self.tenant + "-different", self.admin_principal, (self.source,)
        )
        self.assert_rejected_before_archive()

    def revoke(self, dimension):
        choices = {
            "source": (
                "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s",
                (*self.key[:2], self.admin_principal),
            ),
            "membership": (
                "UPDATE brain_memberships SET role='member' WHERE organization_id=%s",
                (self.organization,),
            ),
            "access": (
                "UPDATE brain_access_grants SET permission='read' WHERE tenant_id=%s",
                (self.tenant,),
            ),
        }
        return choices[dimension]

    def revoke_during_proof(self, dimension):
        self.archive.on_read = lambda: self.update(*self.revoke(dimension))
        report = self.run_admin()
        self.assertEqual(report["cleared_documents"], 0)
        self.assertEqual(report["failed_parents"], 1)
        self.assertIn("retirement_owner_required", report["errors"])
        self.assertEqual(self.state()["cumulative_cleared_documents"], 0)
        self.assertGreater(sum(self.archive.reads.values()), 0)

    def test_source_revoked_during_archive_proof_blocks_clear(self):
        self.revoke_during_proof("source")

    def test_org_admin_revoked_during_archive_proof_blocks_clear(self):
        self.revoke_during_proof("membership")

    def test_brain_access_revoked_during_archive_proof_blocks_clear(self):
        self.revoke_during_proof("access")

    def test_policy_records_actual_admin_and_preserves_disabled_hold(self):
        set_parent_retirement_enabled(self.store, **self.target, enabled=False)
        before = self.state()
        policy.start_policy(self.store, scope=self.admin_scope)
        policy.capture_baseline(self.store, scope=self.admin_scope)
        reviewed = policy.policy_plan(self.store, scope=self.admin_scope)
        policy.set_policy_enabled(
            self.store, scope=self.admin_scope, reviewed_plan=reviewed, enabled=True
        )
        self.assertEqual(reviewed["scope"]["principal_id"], self.admin_principal)
        self.assertEqual(self.state(), before)
        self.assertEqual(self.run_admin()["attempted_parents"], 0)
        self.assertEqual(self.acl()[0]["owner_principal_id"], self.scope.principal_id)

    def test_revocation_wins_lock_before_clear_then_commits(self):
        blocker = self.store.connect()
        connection = blocker.__enter__()
        transaction = connection.transaction()
        transaction.__enter__()
        try:
            connection.execute(*self.revoke("membership"))
            # MVCC still sees the old admin role, but NOWAIT must reject its lock.
            self.archive.on_read = lambda: None
            with self.assertRaises(psycopg.errors.LockNotAvailable):
                self.run_admin()
            self.assertEqual(self.state()["cumulative_cleared_documents"], 0)
        finally:
            transaction.__exit__(None, None, None)
            blocker.__exit__(None, None, None)
        self.assert_rejected_before_archive()

    def test_clear_holds_admin_locks_and_next_batch_observes_revocation(self):
        original_connect = self.store.connect
        original_execute = self.store._execute_bounded
        seen = {"clearing": False, "blocked": False, "revoked": False}

        def execute(connection, sql, args, deadline):
            if (
                "UPDATE canonical_chunks SET text_redacted" in sql
                and not seen["blocked"]
            ):
                with original_connect() as other:
                    other.execute("SET lock_timeout='100ms'")
                    with self.assertRaises(psycopg.errors.LockNotAvailable):
                        with other.transaction():
                            other.execute(*self.revoke("membership"))
                seen["blocked"] = True
                seen["clearing"] = True
            return original_execute(connection, sql, args, deadline)

        @contextmanager
        def connect():
            with original_connect() as connection:
                yield connection
            if seen["clearing"] and not seen["revoked"]:
                seen["revoked"] = True
                with original_connect() as other:
                    other.execute(*self.revoke("membership"))

        with (
            patch.object(self.store, "connect", connect),
            patch.object(self.store, "_execute_bounded", execute),
        ):
            report = self.run_admin(
                parent_limits=ParentRetirementLimits(batch_documents=1)
            )
        self.assertTrue(seen["blocked"])
        self.assertTrue(seen["revoked"])
        self.assertEqual(report["cleared_documents"], 1)
        self.assertEqual(report["failed_parents"], 1)
        self.assertEqual(self.state()["cumulative_cleared_documents"], 1)
        self.assertIn("retirement_owner_required", report["errors"])

    def assert_failed_without_clear(self):
        report = self.run_admin()
        self.assertEqual(report["cleared_documents"], 0)
        self.assertEqual(report["failed_parents"], 1)
        self.assertEqual(self.state()["cumulative_cleared_documents"], 0)

    def test_corrupt_archive_cannot_be_cleared_by_company_admin(self):
        with self.store.connect() as c:
            part = c.execute(
                "SELECT object_key FROM canonical_evidence_document_parts WHERE tenant_id=%s AND source_id=%s ORDER BY part_ordinal LIMIT 1",
                self.key[:2],
            ).fetchone()
        (self.archive.delegate.root / part["object_key"] / "data").write_bytes(
            b"corrupt"
        )
        self.assert_failed_without_clear()

    def test_catalog_replacement_during_proof_blocks_admin_clear(self):
        self.archive.on_read = self.replace
        self.assert_failed_without_clear()
        self.assertFalse(self.state()["enabled"])

    def test_epoch_change_during_proof_blocks_admin_clear(self):
        self.archive.on_read = lambda: self.update(
            "UPDATE canonical_chunk_retirement_progress SET scope_epoch=scope_epoch+1 WHERE tenant_id=%s AND source_id=%s",
            self.key[:2],
        )
        self.assert_failed_without_clear()

    def test_forget_during_proof_blocks_admin_clear(self):
        from recall_server.canonical import CanonicalPlane

        plane = CanonicalPlane(
            self.store,
            self.archive,
            evidence_projector=self.projector,
            chunk_body_archive=self.archive,
        )

        def forget():
            value = plane.forget(
                dict(
                    contract="recall.forget-request.v1",
                    schema_version=1,
                    tenant_id=self.tenant,
                    principal_id=self.scope.principal_id,
                    source_id=self.source,
                    target_receipt=f"recall://{self.source}/event-0000?rev=1#item=0",
                    mode="explicit_forget",
                    reason="owner_requested",
                    requested_at="2026-10-14T00:00:00Z",
                    idempotency_key="admin-race-forget",
                )
            )
            self.assertEqual(value["status"], "deleted")

        self.archive.on_read = forget
        self.assert_failed_without_clear()
        self.assertFalse(self.state()["enabled"])

    def test_future_enrollment_rechecks_admin_revocation(self):
        policy.start_policy(self.store, scope=self.admin_scope)
        policy.capture_baseline(self.store, scope=self.admin_scope)
        policy.set_policy_enabled(
            self.store,
            scope=self.admin_scope,
            reviewed_plan=policy.policy_plan(self.store, scope=self.admin_scope),
            enabled=True,
        )
        self.update(*self.revoke("membership"))
        before = self.state()
        with self.store.connect() as connection:
            self.assertEqual(
                policy.enroll_published_parent(self.store, connection, self.key),
                "deferred",
            )
        self.assertEqual(self.state(), before)


for name in dir(fixture.DetachmentTests):
    if name.startswith("test_") and name not in CompanyAuthorityTests.__dict__:
        setattr(CompanyAuthorityTests, name, None)

if __name__ == "__main__":
    unittest.main(verbosity=2)
