"""Recall central BrainStore service."""

SCHEMA_VERSION = 72
# H3-e': migration 067 drops the Postgres vector/tsvector plane (passage
# embeddings, the embedding ledger, canonical_passages.search_vector). It is
# destructive, so ``migrate`` applies it only when asked to retire the plane
# and only from a process on the turbopuffer search plane.
RETIRE_POSTGRES_PLANE_VERSION = 67
# 070 adds only an optional reconciliation index, not a serving requirement.
RECONCILIATION_INDEX_VERSION = 70
# Compatibility is deployed before the additive conversation metadata migration.
# It remains optional until the feature starts reading the new columns.
NATIVE_CONVERSATION_SCHEMA_VERSION = 71
MANDATORY_SCHEMA_VERSION = 69
PROJECTOR_VERSION = 3
