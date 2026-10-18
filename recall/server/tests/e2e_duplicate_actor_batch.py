#!/usr/bin/env python3
"""Only events gaining actor links should dirty duplicate logical parents."""
import unittest
import hashlib
from unittest.mock import patch

from e2e_logical_actor_checkpoint import ActorCheckpoint
from recall_server import canonical
from recall_server.actor_attribution import ActorIdentityIndex
from recall_server.projectors import canonical_json


class DuplicateActorBatch(ActorCheckpoint):
    def test_new_other_parent_actor_does_not_dirty_unchanged_duplicate(self):
        old = self.attributed_event("old", 1)
        self.bind_actor_fixture()
        self.ingest_attributed(old)
        self.publish()
        with self.store.connect() as connection:
            before = connection.execute(
                "SELECT revision,manifest_artifact_id FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s AND native_parent_id='session-one'",
                (self.tenant, self.source)).fetchone()
        self.parent = "session-two"
        new = self.attributed_event("new", 2)
        result = self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=[old, new])
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["duplicate_events"], 1)
        with self.store.connect() as connection:
            queued = connection.execute(
                "SELECT native_parent_id,generation,append_generation FROM canonical_evidence_document_queue WHERE tenant_id=%s AND source_id=%s ORDER BY native_parent_id",
                (self.tenant, self.source)).fetchall()
            old_catalog = connection.execute(
                "SELECT revision,manifest_artifact_id FROM canonical_evidence_documents WHERE tenant_id=%s AND source_id=%s AND native_parent_id='session-one'",
                (self.tenant, self.source)).fetchone()
        self.assertEqual(old_catalog["revision"], before["revision"])
        self.assertEqual(old_catalog["manifest_artifact_id"], before["manifest_artifact_id"])
        self.assertEqual([r["native_parent_id"] for r in queued], ["session-two"])

    def test_same_parent_new_actor_does_not_poison_valid_append(self):
        old = self.attributed_event("old", 1)
        self.bind_actor_fixture()
        self.ingest_attributed(old)
        self.publish()
        before = self.catalog()
        new = self.attributed_event("new", 2)
        self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=[old, new])
        queued = self.queue()
        self.assertEqual(queued["generation"], 1)
        self.assertEqual(queued["append_generation"], 1)
        self.assertEqual(queued["append_base_manifest_id"], before["manifest_artifact_id"])
        self.assertEqual(self.publish()["documents"], 1)
        self.assertEqual(self.catalog()["append_source_ordinal"], 2)

    def duplicate_fixture(self):
        changed = self.attributed_event("changed", 1)
        self.ingest_attributed(changed)
        self.bind_actor_fixture()
        unchanged = self.attributed_event("unchanged", 2)
        self.ingest_attributed(unchanged)
        self.publish()
        return unchanged, changed

    def test_same_parent_actual_actor_add_requires_full_work_and_replay_is_noop(self):
        unchanged, changed = self.duplicate_fixture()
        for attempt in range(2):
            result = self.plane.ingest_batch(
                tenant_id=self.tenant, principal_id=self.principal, events=[unchanged, changed])
            self.assertEqual(result["inserted"], 0)
            self.assertEqual(result["duplicate_events"], 2)
            queued = self.queue()
            self.assertEqual(queued["generation"], 1, f"duplicate replay {attempt} dirtied again")
            self.assertIsNone(queued["append_generation"])
            self.assertIsNone(queued["append_base_manifest_id"])
        with self.store.connect() as connection:
            links = connection.execute(
                "SELECT count(*) AS n FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s",
                (self.tenant, self.source)).fetchone()["n"]
        self.assertEqual(links, 2)
        self.publish()
        self.assertIsNone(self.queue())
        self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=[unchanged, changed])
        self.assertIsNone(self.queue())

    def test_actor_link_and_dirty_queue_roll_back_together(self):
        unchanged, changed = self.duplicate_fixture()
        failure = RuntimeError("synthetic queue write failure")
        with patch.object(canonical, "mark_logical_evidence_dirty", side_effect=failure):
            with self.assertRaisesRegex(RuntimeError, "synthetic queue write failure"):
                self.plane.ingest_batch(
                    tenant_id=self.tenant, principal_id=self.principal, events=[unchanged, changed])
        self.assertIsNone(self.queue())
        with self.store.connect() as connection:
            links = connection.execute(
                "SELECT event.native_id FROM canonical_event_actors actor JOIN canonical_events event USING(tenant_id,source_id,event_id) WHERE actor.tenant_id=%s AND actor.source_id=%s",
                (self.tenant, self.source)).fetchall()
        self.assertEqual([row["native_id"] for row in links], ["unchanged"])
        self.plane.ingest_batch(
            tenant_id=self.tenant, principal_id=self.principal, events=[unchanged, changed])
        self.assertEqual(self.queue()["generation"], 1)

    def test_external_identity_link_dirties_duplicate_once(self):
        event = self.event("external", 1)
        content = {"text": "Synthetic external author", "author_id": "reader@example.com"}
        payload = canonical_json(content)
        event.update(content=content, content_sha256=hashlib.sha256(payload).hexdigest())
        event["provenance"]["artifact_ref"] = self.gateway.put_raw(
            tenant_id=self.tenant, source_id=self.source, native_id="external",
            payload=payload, media_type="application/json", created_at=event["occurred_at"])
        index = ActorIdentityIndex(lambda value, *, purpose:
                                   hashlib.sha256((purpose + value).encode()).hexdigest())
        self.plane.actor_identity_index = index
        self.plane.ingest_batch(tenant_id=self.tenant, principal_id=self.principal, events=[event])
        self.publish()
        actor = self.actor("a")
        connector, namespace, digest = index.lookup(
            self.tenant, event["provenance"]["connector_id"], "author_id", content["author_id"])
        with self.store.connect() as connection:
            connection.execute(
                "INSERT INTO brain_actor_external_identities(tenant_id,actor_id,connector_id,namespace,subject_hmac_sha256) VALUES(%s,%s,%s,%s,%s)",
                (self.tenant, actor, connector, namespace, digest))
        for _ in range(2):
            result = self.plane.ingest_batch(
                tenant_id=self.tenant, principal_id=self.principal, events=[event])
            self.assertEqual(result["duplicate_events"], 1)
            self.assertEqual(self.queue()["generation"], 1)
            self.assertIsNone(self.queue()["append_generation"])
        self.publish()
        self.assertIn((actor, "author"), self.published())


if __name__ == '__main__':
    suite = unittest.TestSuite(DuplicateActorBatch(name) for name in DuplicateActorBatch.__dict__
                               if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
