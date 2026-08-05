BEGIN;

-- A principal answers "who may access this brain?". An actor answers "which
-- person does this content describe?". Never use one as a substitute for the
-- other.
CREATE TABLE IF NOT EXISTS brain_actors (
    tenant_id text NOT NULL REFERENCES brain_tenants(tenant_id),
    actor_id text NOT NULL,
    actor_kind text NOT NULL CHECK (actor_kind IN ('human','service','agent')),
    display_name text NOT NULL,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, actor_id),
    CHECK (actor_id ~ '^actor_[0-9a-f]{32}$'),
    CHECK (length(display_name) BETWEEN 1 AND 200)
);

CREATE TABLE IF NOT EXISTS brain_actor_aliases (
    tenant_id text NOT NULL,
    actor_id text NOT NULL,
    alias text NOT NULL,
    searchable boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, actor_id, alias),
    FOREIGN KEY(tenant_id, actor_id)
        REFERENCES brain_actors(tenant_id, actor_id) ON DELETE CASCADE,
    CHECK (length(alias) BETWEEN 1 AND 256)
);

CREATE INDEX IF NOT EXISTS brain_actor_aliases_lookup_idx
    ON brain_actor_aliases(tenant_id, lower(alias), actor_id)
    WHERE searchable;

CREATE TABLE IF NOT EXISTS brain_actor_principals (
    tenant_id text NOT NULL,
    actor_id text NOT NULL,
    principal_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, actor_id, principal_id),
    UNIQUE(tenant_id, principal_id),
    FOREIGN KEY(tenant_id, actor_id)
        REFERENCES brain_actors(tenant_id, actor_id) ON DELETE CASCADE,
    FOREIGN KEY(tenant_id, principal_id)
        REFERENCES brain_principals(tenant_id, principal_id) ON DELETE CASCADE
);

-- Provider subjects are HMAC-SHA-256 fingerprinted with a tenant-scoped secret
-- at the application boundary. Display names and searchable aliases belong in
-- the actor directory, not in provider IDs.
CREATE TABLE IF NOT EXISTS brain_actor_external_identities (
    tenant_id text NOT NULL,
    actor_id text NOT NULL,
    connector_id text NOT NULL,
    namespace text NOT NULL,
    subject_hmac_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, connector_id, namespace, subject_hmac_sha256),
    FOREIGN KEY(tenant_id, actor_id)
        REFERENCES brain_actors(tenant_id, actor_id) ON DELETE CASCADE,
    CHECK (length(connector_id) BETWEEN 1 AND 128),
    CHECK (length(namespace) BETWEEN 1 AND 128),
    CHECK (subject_hmac_sha256 ~ '^[0-9a-f]{64}$')
);

-- Local Codex/Claude captures usually identify a machine/source owner rather
-- than an author on each event. This explicit binding makes that provenance a
-- contributor relationship without pretending the employee wrote model text.
CREATE TABLE IF NOT EXISTS canonical_source_actor_bindings (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    actor_id text NOT NULL,
    relation text NOT NULL CHECK (relation IN ('contributor','owner')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, source_id, actor_id, relation),
    FOREIGN KEY(tenant_id, source_id)
        REFERENCES canonical_sources(tenant_id, source_id) ON DELETE CASCADE,
    FOREIGN KEY(tenant_id, actor_id)
        REFERENCES brain_actors(tenant_id, actor_id)
);

CREATE TABLE IF NOT EXISTS canonical_event_actors (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    event_id text NOT NULL,
    actor_id text NOT NULL,
    relation text NOT NULL CHECK (
        relation IN ('author','contributor','owner','organizer','participant','attendee')
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, source_id, event_id, actor_id, relation),
    FOREIGN KEY(tenant_id, source_id, event_id)
        REFERENCES canonical_events(tenant_id, source_id, event_id)
        ON DELETE CASCADE,
    FOREIGN KEY(tenant_id, actor_id)
        REFERENCES brain_actors(tenant_id, actor_id)
);

CREATE INDEX IF NOT EXISTS canonical_event_actors_lookup_idx
    ON canonical_event_actors(tenant_id, actor_id, relation, source_id, event_id);

CREATE TABLE IF NOT EXISTS canonical_evidence_document_actors (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    logical_document_id text NOT NULL,
    revision integer NOT NULL,
    actor_id text NOT NULL,
    relation text NOT NULL CHECK (
        relation IN ('author','contributor','owner','organizer','participant','attendee')
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(
        tenant_id, source_id, logical_document_id, revision, actor_id, relation
    ),
    FOREIGN KEY(tenant_id, source_id, logical_document_id, revision)
        REFERENCES canonical_evidence_documents(
            tenant_id, source_id, logical_document_id, revision
        ) ON DELETE CASCADE,
    FOREIGN KEY(tenant_id, actor_id)
        REFERENCES brain_actors(tenant_id, actor_id)
);

CREATE INDEX IF NOT EXISTS canonical_evidence_document_actors_lookup_idx
    ON canonical_evidence_document_actors(
        tenant_id, actor_id, relation, source_id, logical_document_id
    );

CREATE TABLE IF NOT EXISTS canonical_passage_actors (
    tenant_id text NOT NULL,
    source_id text NOT NULL,
    passage_id text NOT NULL,
    actor_id text NOT NULL,
    relation text NOT NULL CHECK (
        relation IN ('author','contributor','owner','organizer','participant','attendee')
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, source_id, passage_id, actor_id, relation),
    FOREIGN KEY(tenant_id, source_id, passage_id)
        REFERENCES canonical_passages(tenant_id, source_id, passage_id)
        ON DELETE CASCADE,
    FOREIGN KEY(tenant_id, actor_id)
        REFERENCES brain_actors(tenant_id, actor_id)
);

CREATE INDEX IF NOT EXISTS canonical_passage_actors_lookup_idx
    ON canonical_passage_actors(tenant_id, actor_id, relation, source_id, passage_id);

INSERT INTO schema_migrations(version) VALUES (45) ON CONFLICT DO NOTHING;

COMMIT;
