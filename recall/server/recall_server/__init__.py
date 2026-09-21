"""Recall central BrainStore service."""

SCHEMA_VERSION = 69
# H3-e': migration 067 drops the Postgres vector/tsvector plane (passage
# embeddings, the embedding ledger, canonical_passages.search_vector). It is
# destructive, so ``migrate`` applies it only when asked to retire the plane
# and only from a process on the turbopuffer search plane; every version
# except that explicit retirement is mandatory on both planes.
RETIRE_POSTGRES_PLANE_VERSION = 67
MANDATORY_SCHEMA_VERSION = 69
PROJECTOR_VERSION = 3
