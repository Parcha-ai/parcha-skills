#!/usr/bin/env python3
"""Managed Slack directory identities survive privacy and ACK dedup safely."""
from pathlib import Path
import tempfile
import unittest
from e2e_receipt_membership import ReceiptMembershipTests
from recall_server.actor_attribution import ActorIdentityIndex
from recall_server.control import SecretBox
from connectors.sdk import ConnectorPage
from connectors.slack_source import normalize_slack_message, normalize_slack_user


class SlackIdentityFixture(ReceiptMembershipTests):
    def setUp(self):
        super().setUp()
        self.box = SecretBox(b'i' * 32)
        self.index = ActorIdentityIndex(self.box.blind_index)
        self.plane.actor_identity_index = self.index
        self.ingest('fixture/bootstrap', 'Synthetic fixture bootstrap')
        self.actor = 'actor_' + '1' * 32
        self.email = 'synthetic@example.invalid'
        self.contacts = normalize_slack_user(workspace_id='T123', value={
            'id': 'U123', 'profile': {'email': self.email, 'real_name': 'Synthetic'},
        })
        self.message = normalize_slack_message(workspace_id='T123', channel_id='C123',
            value={'ts': '1789981200.000001', 'user': 'U123', 'text': 'Synthetic question'})

    def enroll(self):
        connector, namespace, digest = self.index.lookup(self.tenant, 'identity', 'email', self.email)
        with self.store.connect() as c:
            c.execute("INSERT INTO brain_actors(tenant_id,actor_id,actor_kind,display_name) VALUES(%s,%s,'human','Synthetic')", (self.tenant, self.actor))
            c.execute('INSERT INTO brain_actor_external_identities(tenant_id,actor_id,connector_id,namespace,subject_hmac_sha256) VALUES(%s,%s,%s,%s,%s)', (self.tenant,self.actor,connector,namespace,digest))

    def counts(self):
        with self.store.connect() as c:
            return (
                c.execute("SELECT count(*) AS n FROM brain_actor_external_identities WHERE tenant_id=%s AND connector_id='identity' AND namespace='slack_user'", (self.tenant,)).fetchone()['n'],
                c.execute("SELECT count(*) AS n FROM canonical_event_actors WHERE tenant_id=%s AND source_id=%s AND relation='author'", (self.tenant,self.source)).fetchone()['n'],
            )


class ManagedSlackIdentity(SlackIdentityFixture):
    def managed(self, *, privacy='scrub', before_pull=None):
        import uuid
        from recall_server.managed_worker import ManagedConnectorWorker
        state=tempfile.TemporaryDirectory();self.addCleanup(state.cleanup)
        self.store.provision_brain(organization_id='org:'+uuid.uuid4().hex,
            organization_kind='company',display_name='Synthetic',tenant_id=self.tenant,
            brain_kind='company',slug='synthetic',owner_principal_id=self.principal)
        self.installation=str(uuid.uuid4());self.connection_id=str(uuid.uuid4())
        encrypted=self.box.seal({'token':'synthetic'},purpose='provider-connection:'+self.connection_id)
        with self.store.connect()as c:
            c.execute("INSERT INTO provider_connections(id,principal_id,provider,subject_id,status,granted_scopes,encrypted_credentials,encryption_key_id)VALUES(%s,%s,'slack','T123','connected',ARRAY['users:read','users:read.email'],%s,%s)",(self.connection_id,self.principal,encrypted,self.box.key_id))
            c.execute("INSERT INTO connector_installations(id,tenant_id,principal_id,connector_id,source_id,connection_id,execution,state,privacy_mode)VALUES(%s,%s,%s,'slack.messages',%s,%s,'remote_worker','enabled',%s)",(self.installation,self.tenant,self.principal,self.source,self.connection_id,privacy))
        def disable():
            with self.store.connect() as c:
                c.execute("UPDATE connector_installations SET state='paused' WHERE id=%s",(self.installation,))
        self.addCleanup(disable)
        source=self.source;contacts=self.contacts;message=self.message
        class Connector:
            connector_id='slack.messages';source_id=source
            pages={None:ConnectorPage(contacts,'message',True),
                   'message':ConnectorPage((message,),'contacts-again',True),
                   'contacts-again':ConnectorPage(contacts,'done',False)}
            def pull(self,cursor):
                if before_pull:before_pull()
                return self.pages[cursor]
        worker=ManagedConnectorWorker(self.store,self.archive,self.box,state_root=Path(state.name),
            worker_id='synthetic-slack-identity',connector_factory=lambda *_:(Connector(),Path(state.name)/'spool.db'))
        # Synthetic semantic backend: this test concerns native acquisition, not models.
        worker.retrieval.embed_pending=lambda **_:{}
        return worker

    def step(self, worker):
        with self.store.connect()as c:c.execute('UPDATE connector_installations SET run_after=now() WHERE id=%s',(self.installation,))
        return worker.run_once()

    def test_managed_pre_enrolled_scrubbed_contact_attributes_next_message(self):
        self.enroll();worker=self.managed()
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.counts(),(1,0))
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.counts(),(1,1))
        with self.store.connect()as c:
            content=c.execute("SELECT canonical_redacted->'content' AS c FROM canonical_events WHERE tenant_id=%s AND source_id=%s AND native_id='slack-email:T123:U123'",(self.tenant,self.source)).fetchone()['c']
        self.assertEqual(content['identifier'],'[REDACTED:email]')

    def test_managed_later_enrollment_maps_acknowledged_contact_without_reingest(self):
        worker=self.managed()
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.counts(),(0,0))
        with self.store.connect()as c:
            before=c.execute('SELECT event_id,content_sha256,revision FROM canonical_events WHERE tenant_id=%s AND source_id=%s ORDER BY event_id',(self.tenant,self.source)).fetchall()
        self.enroll()
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.counts(),(1,0)) # Forward identity only; no historical author claim.
        with self.store.connect()as c:
            after=c.execute('SELECT event_id,content_sha256,revision FROM canonical_events WHERE tenant_id=%s AND source_id=%s ORDER BY event_id',(self.tenant,self.source)).fetchall()
        self.assertEqual(before,after)

    def test_managed_unmatched_email_does_not_create_actor(self):
        worker=self.managed()
        with self.store.connect()as c:
            before=c.execute('SELECT actor_id FROM brain_actors WHERE tenant_id=%s ORDER BY actor_id',(self.tenant,)).fetchall()
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.counts(),(0,0))
        with self.store.connect()as c:
            self.assertEqual(c.execute('SELECT actor_id FROM brain_actors WHERE tenant_id=%s ORDER BY actor_id',(self.tenant,)).fetchall(),before)

    def test_managed_privacy_drop_does_not_enroll(self):
        self.enroll();worker=self.managed(privacy='drop')
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.counts(),(0,0))

    def test_managed_forgotten_email_contact_is_not_enrolled_on_repeat(self):
        import uuid
        worker=self.managed()
        self.assertEqual(self.step(worker)['failed'],0)
        self.plane.forget(dict(contract='recall.forget-request.v1',schema_version=1,
            tenant_id=self.tenant,principal_id=self.principal,source_id=self.source,
            target_receipt=f'recall://{self.source}/slack-email:T123:U123?rev=1#item=0',
            mode='explicit_forget',reason='owner_requested',requested_at='2026-10-19T09:00:00Z',
            idempotency_key=uuid.uuid4().hex))
        self.enroll()
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.counts(),(0,0))

    def test_managed_connection_revoked_during_pull_refuses_mapping(self):
        def revoke():
            with self.store.connect()as c:c.execute("UPDATE provider_connections SET status='revoked',revoked_at=now() WHERE id=%s",(self.connection_id,))
        self.enroll();worker=self.managed(before_pull=revoke)
        self.assertEqual(self.step(worker)['failed'],1)
        self.assertEqual(self.counts(),(0,0))

    def test_managed_source_grant_revoked_during_pull_is_not_recreated(self):
        def revoke():
            with self.store.connect()as c:c.execute('DELETE FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',(self.tenant,self.source,self.principal))
        self.enroll();worker=self.managed(before_pull=revoke)
        self.assertEqual(self.step(worker)['failed'],1)
        self.assertEqual(self.counts(),(0,0))
        with self.store.connect()as c:
            self.assertFalse(c.execute('SELECT 1 FROM canonical_source_grants WHERE tenant_id=%s AND source_id=%s AND principal_id=%s',(self.tenant,self.source,self.principal)).fetchone())

    def test_managed_installation_paused_during_pull_refuses_mapping(self):
        def revoke():
            with self.store.connect()as c:c.execute("UPDATE connector_installations SET state='paused' WHERE id=%s",(self.installation,))
        self.enroll();worker=self.managed(before_pull=revoke)
        self.assertEqual(self.step(worker)['failed'],1)
        self.assertEqual(self.counts(),(0,0))

    def test_managed_first_source_uses_existing_installation_authority(self):
        self.source += ':first'
        self.enroll();worker=self.managed()
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.counts(),(1,0))

    def test_managed_brain_access_revoked_during_pull_refuses_mapping(self):
        def revoke():
            with self.store.connect()as c:c.execute("UPDATE brain_access_grants SET permission='read' WHERE tenant_id=%s AND principal_id=%s",(self.tenant,self.principal))
        self.enroll();worker=self.managed(before_pull=revoke)
        self.assertEqual(self.step(worker)['failed'],1)
        self.assertEqual(self.counts(),(0,0))

    def test_managed_contact_privacy_input_matches_actual_sdk_input(self):
        import copy
        from dataclasses import replace
        from unittest.mock import patch
        from privacy.policy import PrivacyPolicy
        self.contacts=tuple(replace(record,provenance={**record.provenance,
            'metadata':{'secret':'synthetic-provenance-secret'}})for record in self.contacts)
        self.enroll();worker=self.managed();seen=[];original=PrivacyPolicy.apply
        def record_apply(policy,value):
            if isinstance(value,dict) and isinstance(value.get('content'),dict) and value['content'].get('identifier_type')=='email':
                seen.append(copy.deepcopy(value))
            return original(policy,value)
        with patch.object(PrivacyPolicy,'apply',record_apply):
            self.assertEqual(self.step(worker)['failed'],0)
        self.assertIn(len(seen),(2,4))
        self.assertEqual(seen[0],seen[1])
        self.assertEqual(seen[0]['provenance']['metadata']['secret'],'[REDACTED:credential]')
        if len(seen)==4:
            # A post-capture repair verifies and enrolls the same archived input.
            self.assertEqual(seen[2],seen[3])
            self.assertEqual(seen[2],seen[0])
            self.assertEqual(seen[2]['provenance']['metadata']['secret'],'[REDACTED:credential]')
        self.assertEqual(self.counts(),(1,0))

    def test_managed_expired_claim_during_pull_refuses_identity_write(self):
        def expire():
            with self.store.connect()as c:c.execute("UPDATE connector_installations SET lease_expires_at=now()-interval '1 second' WHERE id=%s",(self.installation,))
        self.enroll();worker=self.managed(before_pull=expire)
        self.assertEqual(self.step(worker)['failed'],1)
        self.assertEqual(self.counts(),(0,0))

    def test_managed_reassigned_connection_during_pull_refuses_identity_write(self):
        import uuid
        def reassign():
            replacement=str(uuid.uuid4())
            with self.store.connect()as c:
                c.execute("""INSERT INTO provider_connections(id,principal_id,provider,subject_id,status,granted_scopes,encrypted_credentials,encryption_key_id)
                    SELECT %s,principal_id,provider,'replacement',status,granted_scopes,encrypted_credentials,encryption_key_id FROM provider_connections WHERE id=%s""",(replacement,self.connection_id))
                c.execute('UPDATE connector_installations SET connection_id=%s WHERE id=%s',(replacement,self.installation))
        self.enroll();worker=self.managed(before_pull=reassign)
        self.assertEqual(self.step(worker)['failed'],1)
        self.assertEqual(self.counts(),(0,0))

    def test_managed_provider_expiry_metadata_keeps_existing_refresh_semantics(self):
        def expire():
            with self.store.connect()as c:c.execute("UPDATE provider_connections SET expires_at=now()-interval '1 second' WHERE id=%s",(self.connection_id,))
        self.enroll();worker=self.managed(before_pull=expire)
        self.assertEqual(self.step(worker)['failed'],0)
        self.assertEqual(self.counts(),(1,0))


if __name__ == '__main__':
    suite=unittest.TestSuite(ManagedSlackIdentity(name)for name in ManagedSlackIdentity.__dict__ if name.startswith('test_'))
    raise SystemExit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
