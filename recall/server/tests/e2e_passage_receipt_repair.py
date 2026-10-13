"""Exact same-revision admission over a proven stale receipt, on disposable PG."""

import hashlib
import json
import unittest
from unittest.mock import patch

import psycopg
from e2e_retained_passage_receipts import RetainedPassageReceipts
from recall_server.control import ControlError, ControlPlane


class Repair(RetainedPassageReceipts):
    def corrupt_old_receipt(self):
        self.ingest(1)
        self.project()
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        old = self.rows()[0]["receipts"]
        self.ingest(2)
        self.project()
        self.correct = self.rows()
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_passages SET receipts=%s WHERE tenant_id=%s",
                (old, self.tenant),
            )
            row = c.execute(
                "SELECT source_id,logical_document_id,passage_id FROM canonical_passages WHERE tenant_id=%s",
                (self.tenant,),
            ).fetchone()
        return row

    @staticmethod
    def digest(value):
        return hashlib.sha256(
            json.dumps(
                value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode()
        ).hexdigest()

    def admit(self, plan):
        return self.passages.requeue_receipt_repair(
            plan=plan, principal_id=self.principal
        )

    def plan(self, target):
        # Freeze the exact stale fixture before invoking the owning admission method.
        with self.store.connect() as c:
            row = c.execute(
                """SELECT evidence.native_parent_id,evidence.revision,
                evidence.created_at::text AS catalog_created_at,evidence.document_content_sha256,
                evidence.manifest_content_sha256,evidence.manifest_object_key,evidence.manifest_version_id,
                projected.revision AS projected_revision,projected.policy_fingerprint,
                projected.source_document_sha256,passage.revision AS passage_revision,
                passage.text_sha256,passage.receipts
                FROM canonical_evidence_documents evidence
                JOIN canonical_passage_documents projected USING(tenant_id,source_id,logical_document_id)
                JOIN canonical_passages passage USING(tenant_id,source_id,logical_document_id)
                WHERE evidence.tenant_id=%s AND evidence.source_id=%s
                  AND evidence.logical_document_id=%s AND passage.passage_id=%s""",
                (
                    self.tenant,
                    target["source_id"],
                    target["logical_document_id"],
                    target["passage_id"],
                ),
            ).fetchone()
        receipts = row.pop("receipts")
        plan = dict(
            contract="recall.passage-receipt-repair.v1",
            tenant_id=self.tenant,
            principal_id=self.principal,
            target=target,
            catalog=row,
            witness_receipt_digest=self.digest(receipts),
            stale_receipts=len(receipts),
        )
        return dict(plan, proof_sha256=self.digest(plan))

    def company_admin_plan(self):
        target = self.corrupt_old_receipt()
        plan = self.plan(target)
        self.admin = self.principal + ":admin"
        self.organization = self.tenant + ":org"
        self.store.provision_brain(
            organization_id=self.organization,
            organization_kind="company",
            display_name="Receipt fixture",
            tenant_id=self.tenant,
            brain_kind="company",
            slug="company",
            owner_principal_id=self.admin,
        )
        with self.store.connect() as c:
            c.execute(
                "UPDATE brain_memberships SET role='admin' WHERE organization_id=%s AND principal_id=%s",
                (self.organization, self.admin),
            )
            c.execute(
                "UPDATE brain_access_grants SET permission='admin' WHERE tenant_id=%s AND principal_id=%s",
                (self.tenant, self.admin),
            )
            c.execute(
                "INSERT INTO canonical_source_grants(tenant_id,principal_id,source_id,permission) VALUES(%s,%s,%s,'read') ON CONFLICT DO NOTHING",
                (self.tenant, self.admin, self.source),
            )
        plan["principal_id"] = self.admin
        plan["proof_sha256"] = self.digest(
            {k: v for k, v in plan.items() if k != "proof_sha256"}
        )
        return plan

    def test_company_admin_with_exact_source_grant_can_queue(self):
        plan = self.company_admin_plan()
        result = self.passages.requeue_receipt_repair(
            plan=plan, principal_id=self.admin
        )
        self.assertEqual(result["status"], "queued")

    def test_read_grant_does_not_replace_current_company_admin_authority(self):
        plan = self.company_admin_plan()
        for table, column, key, value in [
            ("brain_memberships", "role", "organization_id", self.organization),
            ("brain_access_grants", "permission", "tenant_id", self.tenant),
            ("canonical_source_grants", None, "tenant_id", self.tenant),
        ]:
            with self.subTest(table=table), self.store.connect() as c, c.transaction():
                # Separate connection commits the revocation before admission.
                if column:
                    downgrade = "member" if column == "role" else "read"
                    c.execute(
                        f"UPDATE {table} SET {column}=%s WHERE {key}=%s AND principal_id=%s",
                        (downgrade, value, self.admin),
                    )
                else:
                    c.execute(
                        "DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s",
                        (self.tenant, self.source, self.admin),
                    )
            with self.assertRaises((ControlError, ValueError)):
                self.passages.requeue_receipt_repair(plan=plan, principal_id=self.admin)
            with self.store.connect() as c:
                if column:
                    c.execute(
                        f"UPDATE {table} SET {column}='admin' WHERE {key}=%s AND principal_id=%s",
                        (value, self.admin),
                    )
                else:
                    c.execute(
                        "INSERT INTO canonical_source_grants(tenant_id,principal_id,source_id,permission) VALUES(%s,%s,%s,'read')",
                        (self.tenant, self.admin, self.source),
                    )
        with self.store.connect() as c:
            self.assertEqual(
                c.execute(
                    "SELECT count(*) AS n FROM canonical_passage_projection_queue WHERE tenant_id=%s",
                    (self.tenant,),
                ).fetchone()["n"],
                0,
            )

    def test_admin_membership_and_grants_are_locked_through_queue_commit(self):
        plan = self.company_admin_plan()
        original = ControlPlane._brain_admin
        checked = []

        def authorize(*args, **kwargs):
            result = original(*args, **kwargs)
            for table, key, value in [
                ("brain_memberships", "organization_id", self.organization),
                ("brain_access_grants", "tenant_id", self.tenant),
                ("canonical_source_grants", "tenant_id", self.tenant),
            ]:
                with self.store.connect() as revoker:
                    revoker.execute("SET lock_timeout='100ms'")
                    with self.assertRaises(psycopg.errors.LockNotAvailable):
                        revoker.execute(
                            f"DELETE FROM {table} WHERE {key}=%s AND principal_id=%s",
                            (value, self.admin),
                        )
                    revoker.rollback()
                checked.append(table)
            return result

        with patch.object(ControlPlane, "_brain_admin", side_effect=authorize):
            result = self.passages.requeue_receipt_repair(
                plan=plan, principal_id=self.admin
            )
        self.assertEqual(result["status"], "queued")
        self.assertEqual(len(checked), 3)

    def test_same_revision_repair_uses_publisher_without_vendor_writes(self):
        target = self.corrupt_old_receipt()
        plan = self.plan(target)
        namespace = self.client.namespace(self.settings.namespace(self.tenant))
        writes = len(namespace.writes)
        self.assertEqual(self.passages.seed_backfill(tenant_id=self.tenant), 0)
        self.assertEqual(self.admit(plan)["status"], "queued")
        result = self.passages.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1, concurrency=1
        )
        self.assertEqual(
            (result["retained"], result["passages"], result["deleted"]), (1, 0, 0)
        )
        self.assertEqual(self.rows(), self.correct)
        self.writer.drain(tenant_id=self.tenant, max_months=2)
        self.assertEqual(len(namespace.writes), writes)

    def test_receipt_count_has_no_new_repair_eligibility_limit(self):
        target = self.corrupt_old_receipt()
        with self.store.connect() as c:
            c.execute(
                "UPDATE canonical_passages SET receipts=%s WHERE tenant_id=%s",
                ([f"stale-receipt-{n}" for n in range(1001)], self.tenant),
            )
        plan = self.plan(target)
        self.assertEqual(self.admit(plan)["status"], "queued")

    def test_existing_queue_is_preserved_and_digest_is_checked(self):
        target = self.corrupt_old_receipt()
        plan = self.plan(target)
        self.admit(plan)
        with self.assertRaisesRegex(ValueError, "projection_pending"):
            self.admit(plan)
        with self.assertRaisesRegex(ValueError, "plan_digest"):
            self.admit({**plan, "principal_id": "someone-else"})

    def test_catalog_revision_drift_is_not_repaired_or_reported_as_success(self):
        target = self.corrupt_old_receipt()
        plan = self.plan(target)
        self.ingest(3)
        self.logical.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1, upload_concurrency=1
        )
        with self.assertRaisesRegex(ValueError, "catalog_changed"):
            self.admit(plan)

    def test_clean_witness_and_wrong_owner_are_refused(self):
        target = self.corrupt_old_receipt()
        plan = self.plan(target)
        self.admit(plan)
        self.passages.project_pending(
            tenant_id=self.tenant, batch_size=1, max_batches=1, concurrency=1
        )
        with self.assertRaisesRegex(ValueError, "witness_changed"):
            self.admit(plan)
        wrong = {**plan, "principal_id": "different-owner"}
        wrong["proof_sha256"] = self.digest(
            {k: v for k, v in wrong.items() if k != "proof_sha256"}
        )
        with self.assertRaisesRegex(ValueError, "source_or_catalog_unavailable"):
            self.passages.requeue_receipt_repair(
                plan=wrong, principal_id="different-owner"
            )


if __name__ == "__main__":
    suite = unittest.TestSuite(
        Repair(name) for name in Repair.__dict__ if name.startswith("test_")
    )
    raise SystemExit(
        not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
    )
