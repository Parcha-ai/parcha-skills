"""Recall central BrainStore service."""

SCHEMA_VERSION = 81
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
# Additive notification-priority queue column, required by its projection reader.
# Capability checks also accept the previous schema during ordered rollout.
NOTIFICATION_PRIORITY_SCHEMA_VERSION = 72
PASSAGE_NOTIFICATION_SCHEMA_VERSION = 73
# Optional indexes accelerate existing cleanup authority checks.
CLEANUP_REFERENCE_INDEX_VERSION = 74
# Additive retirement lifecycle state; serving remains compatible with schema069.
RETIREMENT_DETACHMENT_VERSION = 75
RETIREMENT_POLICY_VERSION = 76
RETIREMENT_MAINTENANCE_VERSION = 78
# Advisory append metadata; no fast reader is enabled by this migration.
LOGICAL_APPEND_CAPABILITY_VERSION = 79
# Optional exact singleton profile; no row is admitted by its migration.
SINGLETON_RECEIPT_PROFILE_VERSION = 80
# Optional type correction for the existing forgotten-identity trigger.
FORGOTTEN_EVENT_DIGEST_TYPE_VERSION = 81
# Optional until a delayed Slack body needs a terminal-identity ACK.
SUPPRESSED_ARTIFACT_SCHEMA_VERSION = 77
MANDATORY_SCHEMA_VERSION = 69
PROJECTOR_VERSION = 3
