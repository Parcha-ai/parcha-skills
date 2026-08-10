BEGIN;

-- Local coding-history sources created before actor enrollment have an exact
-- owner principal but no source-to-actor binding. Repair only this source
-- family: a shared communications source (for example Slack) must never be
-- attributed wholesale to the principal that installed its connector.
WITH inserted AS (
    INSERT INTO canonical_source_actor_bindings(
        tenant_id, source_id, actor_id, relation
    )
    SELECT source.tenant_id, source.source_id, principal.actor_id,
           'contributor'
      FROM canonical_sources source
      JOIN brain_actor_principals principal
        ON principal.tenant_id=source.tenant_id
       AND principal.principal_id=source.owner_principal_id
      JOIN source_profiles profile
        ON profile.source_id=source.source_id
       AND profile.family='coding_history'
    ON CONFLICT DO NOTHING
    RETURNING tenant_id, source_id
)
INSERT INTO canonical_evidence_document_queue(
    tenant_id, source_id, native_parent_id,
    generation, reason, changed_at
)
SELECT event.tenant_id, event.source_id,
       coalesce(event.native_parent_id,event.native_id),
       1, 'backfill', clock_timestamp()
  FROM inserted
  JOIN canonical_events event
    ON event.tenant_id=inserted.tenant_id
   AND event.source_id=inserted.source_id
  JOIN canonical_documents document
    ON document.tenant_id=event.tenant_id
   AND document.source_id=event.source_id
   AND document.event_id=event.event_id
   AND document.is_current
   AND document.deleted_at IS NULL
 GROUP BY event.tenant_id,event.source_id,
          coalesce(event.native_parent_id,event.native_id)
ON CONFLICT(tenant_id,source_id,native_parent_id)
DO UPDATE SET
    generation=canonical_evidence_document_queue.generation+1,
    reason='backfill',
    changed_at=clock_timestamp();

INSERT INTO schema_migrations(version) VALUES (49) ON CONFLICT DO NOTHING;

COMMIT;
