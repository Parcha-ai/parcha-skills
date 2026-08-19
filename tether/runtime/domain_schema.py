from __future__ import annotations

import contextlib
import hashlib
import json
import os
import secrets
import sqlite3
import stat
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from pathlib import Path


SCHEMA_VERSION = 18
PRESERVED_MANIFEST_KEYS = (
    "binding_count",
    "binding_ids",
    "binding_routes",
    "binding_sources",
    "turn_count",
    "turn_payloads",
    "turn_order",
    "turn_routes",
    "turn_outcomes",
    "attempt_count",
    "attempt_ids",
    "attempt_routes",
    "attempt_outcomes",
    "attempt_memberships",
    "response_payloads",
    "driver_receipts",
    "authority_resolutions",
    "egress_receipts",
)


def preserved_manifest_digest(manifest: Mapping[str, Any]) -> str:
    """Digest over exactly the keys the migration contract preserves.

    Whole-manifest digests differ across a lossless 17->18->17 round trip
    because rollback intentionally retains the archived endpoint inventory;
    this subset is the preservation contract itself.
    """
    return _sha256_text(
        json.dumps(
            {key: manifest[key] for key in PRESERVED_MANIFEST_KEYS},
            sort_keys=True,
            separators=(",", ":"),
        )
    )


@dataclass(frozen=True)
class SecurityDomainDescriptor:
    instance_uid: int
    workspace_id: str
    persona_id: str
    authorized_owner_ids: tuple[str, ...]
    policy_generation: int

    def validate(self) -> None:
        owners = self.canonical_owner_ids
        if (
            self.instance_uid < 0
            or not self.workspace_id
            or not self.persona_id
            or not owners
            or self.policy_generation < 1
        ):
            raise ValueError("security domain descriptor is incomplete")
        if any(
            not owner
            or len(owner) > 128
            or any(ord(character) < 32 for character in owner)
            for owner in owners
        ):
            raise ValueError("security domain owner set is invalid")

    @property
    def canonical_owner_ids(self) -> tuple[str, ...]:
        return tuple(sorted(set(self.authorized_owner_ids)))

    @property
    def authorized_owners_hash(self) -> str:
        self.validate()
        return _sha256_text(
            json.dumps(
                self.canonical_owner_ids,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )

    @property
    def security_domain_id(self) -> str:
        self.validate()
        material = json.dumps(
            {
                "instance_uid": self.instance_uid,
                "workspace_id": self.workspace_id,
                "persona_id": self.persona_id,
                "authorized_owners_hash": self.authorized_owners_hash,
                "policy_generation": self.policy_generation,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return "sec_" + hashlib.sha256(material.encode()).hexdigest()[:24]


@dataclass(frozen=True)
class LegacyEndpointRef:
    endpoint_key: str | None
    candidate_endpoint_key: str | None
    endpoint_kind: str
    source_kind: str
    source_json: str
    ref_version: int
    ready: bool
    error_code: str | None = None


DDL: tuple[str, ...] = (
    # Kept as the schema-17-compatible egress projection during L1a. Native
    # execution state is authoritative below; this table is only the Slack
    # delivery handoff and makes a fresh schema-18 database rollbackable.
    """
    CREATE TABLE IF NOT EXISTS bridge_replies (
      reply_key TEXT PRIMARY KEY, bridge_id TEXT NOT NULL,
      message_ts TEXT, text_hash TEXT,
      payload_text TEXT, client_msg_id TEXT,
      lease_id TEXT, lease_owner TEXT, lease_expires_at TEXT,
      retry_count INTEGER NOT NULL DEFAULT 0,
      state TEXT NOT NULL DEFAULT 'reserved', error TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE endpoints (
      endpoint_id TEXT PRIMARY KEY,
      endpoint_key TEXT,
      candidate_endpoint_key TEXT,
      endpoint_kind TEXT NOT NULL CHECK(endpoint_kind IN (
        'zellij_pane','herdr_agent','detached_native',
        'hermes_continuation','unknown'
      )),
      source_kind TEXT NOT NULL CHECK(source_kind IN (
        'zellij_pane','claude_session','codex_session','hermes_session',
        'headless_run','quarantined_legacy'
      )),
      source_json TEXT NOT NULL
        CHECK(json_valid(source_json) AND json_type(source_json)='object'),
      ref_version INTEGER NOT NULL CHECK(ref_version>=1),
      incarnation INTEGER NOT NULL CHECK(incarnation>=1),
      security_domain_id TEXT NOT NULL,
      instance_uid INTEGER NOT NULL CHECK(instance_uid>=0),
      workspace_id TEXT NOT NULL,
      persona_id TEXT NOT NULL,
      authorized_owners_json TEXT NOT NULL
        CHECK(json_valid(authorized_owners_json)
          AND json_type(authorized_owners_json)='array'),
      authorized_owners_hash TEXT NOT NULL CHECK(length(authorized_owners_hash)=64),
      policy_generation INTEGER NOT NULL CHECK(policy_generation>=1),
      capabilities_json TEXT NOT NULL DEFAULT '[]'
        CHECK(json_valid(capabilities_json) AND json_type(capabilities_json)='array'),
      state TEXT NOT NULL CHECK(state IN ('ready','rebind_required','retired')),
      error_code TEXT,
      next_lease_fence INTEGER NOT NULL DEFAULT 0 CHECK(next_lease_fence>=0),
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(endpoint_id,security_domain_id),
      UNIQUE(endpoint_id,security_domain_id,workspace_id),
      CHECK(
        (state='ready' AND endpoint_key IS NOT NULL AND error_code IS NULL)
        OR (state='rebind_required' AND endpoint_key IS NULL AND error_code IS NOT NULL)
        OR state='retired'
      )
    )
    """,
    """
    CREATE UNIQUE INDEX endpoint_one_verified_native_ref
    ON endpoints(endpoint_key) WHERE endpoint_key IS NOT NULL
    """,
    """
    CREATE INDEX endpoint_candidate_lookup ON endpoints(candidate_endpoint_key)
    WHERE candidate_endpoint_key IS NOT NULL
    """,
    """
    CREATE TABLE endpoint_authorized_owners (
      endpoint_id TEXT NOT NULL,
      security_domain_id TEXT NOT NULL,
      owner_user_id TEXT NOT NULL,
      PRIMARY KEY(endpoint_id,security_domain_id,owner_user_id),
      FOREIGN KEY(endpoint_id,security_domain_id)
        REFERENCES endpoints(endpoint_id,security_domain_id) ON DELETE RESTRICT
    )
    """,
    """
    CREATE TRIGGER endpoint_authorized_owner_insert_guard
    BEFORE INSERT ON endpoint_authorized_owners
    WHEN NOT EXISTS(
      SELECT 1 FROM endpoints AS endpoint,json_each(endpoint.authorized_owners_json)
      WHERE endpoint.endpoint_id=NEW.endpoint_id
        AND endpoint.security_domain_id=NEW.security_domain_id
        AND json_each.type='text'
        AND json_each.value=NEW.owner_user_id
    )
    BEGIN
      SELECT RAISE(ABORT,'owner is outside the endpoint security domain');
    END
    """,
    """
    CREATE TRIGGER endpoint_authorized_owner_immutable
    BEFORE UPDATE ON endpoint_authorized_owners
    BEGIN
      SELECT RAISE(ABORT,'endpoint authorized owner is immutable');
    END
    """,
    """
    CREATE TRIGGER endpoint_authorized_owner_delete_forbidden
    BEFORE DELETE ON endpoint_authorized_owners
    BEGIN
      SELECT RAISE(ABORT,'endpoint authorized owner cannot be deleted');
    END
    """,
    """
    CREATE TRIGGER endpoint_delete_forbidden
    BEFORE DELETE ON endpoints
    BEGIN
      SELECT RAISE(ABORT,'endpoint cannot be deleted');
    END
    """,
    """
    CREATE TABLE thread_bindings (
      binding_id TEXT PRIMARY KEY,
      endpoint_id TEXT NOT NULL,
      security_domain_id TEXT NOT NULL,
      team_id TEXT NOT NULL,
      channel_id TEXT NOT NULL,
      thread_ts TEXT,
      owner_user_id TEXT NOT NULL,
      idempotency_key TEXT NOT NULL UNIQUE,
      request_hash TEXT NOT NULL CHECK(length(request_hash)=64),
      generation INTEGER NOT NULL CHECK(generation>=1),
      state TEXT NOT NULL CHECK(state IN (
        'pending_root','active','rebind_required','closed'
      )),
      thread_claim_generation INTEGER,
      error_code TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(endpoint_id,security_domain_id)
        REFERENCES endpoints(endpoint_id,security_domain_id) ON DELETE RESTRICT,
      FOREIGN KEY(endpoint_id,security_domain_id,team_id)
        REFERENCES endpoints(endpoint_id,security_domain_id,workspace_id)
        ON DELETE RESTRICT,
      FOREIGN KEY(endpoint_id,security_domain_id,owner_user_id)
        REFERENCES endpoint_authorized_owners(
          endpoint_id,security_domain_id,owner_user_id
        ) ON DELETE RESTRICT,
      CHECK(
        thread_claim_generation IS NULL
        OR (thread_claim_generation>=1 AND thread_claim_generation<=generation)
      ),
      CHECK(
        (state='pending_root' AND thread_ts IS NULL)
        OR (state='active' AND thread_ts IS NOT NULL AND team_id!='')
        OR state IN ('rebind_required','closed')
      )
    )
    """,
    """
    CREATE UNIQUE INDEX binding_one_live_thread
    ON thread_bindings(team_id,channel_id,thread_ts)
    WHERE thread_ts IS NOT NULL AND state!='closed'
    """,
    """
    CREATE INDEX binding_endpoint_live
    ON thread_bindings(endpoint_id,state,binding_id) WHERE state!='closed'
    """,
    """
    CREATE TABLE legacy_binding_sources (
      binding_id TEXT PRIMARY KEY REFERENCES thread_bindings(binding_id)
        ON DELETE RESTRICT,
      binding_generation INTEGER NOT NULL CHECK(binding_generation>=1),
      source_kind TEXT NOT NULL,
      source_json TEXT NOT NULL
        CHECK(json_valid(source_json) AND json_type(source_json)='object'),
      ref_version INTEGER NOT NULL CHECK(ref_version>=1),
      imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TRIGGER legacy_binding_source_insert_guard
    BEFORE INSERT ON legacy_binding_sources
    WHEN (SELECT user_version FROM pragma_user_version)!=17
    BEGIN
      SELECT RAISE(ABORT,'legacy binding source provenance is migration-only');
    END
    """,
    """
    CREATE TRIGGER legacy_binding_source_immutable
    BEFORE UPDATE ON legacy_binding_sources
    BEGIN
      SELECT RAISE(ABORT,'legacy binding source provenance is immutable');
    END
    """,
    """
    CREATE TRIGGER legacy_binding_source_delete_forbidden
    BEFORE DELETE ON legacy_binding_sources
    BEGIN
      SELECT RAISE(ABORT,'legacy binding source provenance cannot be deleted');
    END
    """,
    """
    CREATE TABLE queued_turns (
      event_key TEXT PRIMARY KEY,
      binding_id TEXT NOT NULL REFERENCES thread_bindings(binding_id)
        ON DELETE RESTRICT,
      binding_generation INTEGER NOT NULL CHECK(binding_generation>=1),
      ordered_at TEXT NOT NULL,
      mutation_kind TEXT NOT NULL CHECK(mutation_kind IN ('create','edit','delete')),
      mutation_target_key TEXT,
      payload_inline TEXT,
      payload_ref TEXT,
      payload_sha256 TEXT,
      payload_bytes INTEGER CHECK(payload_bytes IS NULL OR payload_bytes>=0),
      state TEXT NOT NULL CHECK(state IN ('ready','completed','cancelled')),
      terminal_at TEXT,
      error_code TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(event_key,binding_id,binding_generation),
      CHECK(
        (mutation_kind='create' AND mutation_target_key IS NULL)
        OR (mutation_kind IN ('edit','delete') AND mutation_target_key IS NOT NULL)
      ),
      CHECK(
        (mutation_kind IN ('create','edit')
          AND ((payload_inline IS NOT NULL) <> (payload_ref IS NOT NULL))
          AND payload_sha256 IS NOT NULL AND length(payload_sha256)=64
          AND payload_bytes IS NOT NULL)
        OR (mutation_kind='delete' AND payload_inline IS NULL
          AND payload_ref IS NULL AND payload_sha256 IS NULL
          AND payload_bytes IS NULL)
      ),
      CHECK((state='ready') = (terminal_at IS NULL))
    )
    """,
    """
    CREATE INDEX queued_turn_ready_order
    ON queued_turns(binding_id,ordered_at,event_key) WHERE state='ready'
    """,
    """
    CREATE TABLE legacy_terminal_imports (
      event_key TEXT PRIMARY KEY REFERENCES queued_turns(event_key)
        ON DELETE RESTRICT,
      source_schema INTEGER NOT NULL CHECK(source_schema=17),
      source_digest TEXT NOT NULL CHECK(length(source_digest)=64),
      imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TRIGGER legacy_terminal_import_insert_guard
    BEFORE INSERT ON legacy_terminal_imports
    WHEN (SELECT user_version FROM pragma_user_version)!=17
    BEGIN
      SELECT RAISE(ABORT,'legacy provenance is migration-only');
    END
    """,
    """
    CREATE TRIGGER legacy_terminal_import_immutable
    BEFORE UPDATE ON legacy_terminal_imports
    BEGIN
      SELECT RAISE(ABORT,'legacy terminal provenance is immutable');
    END
    """,
    """
    CREATE TRIGGER legacy_terminal_import_delete_forbidden
    BEFORE DELETE ON legacy_terminal_imports
    BEGIN
      SELECT RAISE(ABORT,'legacy terminal provenance cannot be deleted');
    END
    """,
    """
    CREATE TABLE legacy_attempt_imports (
      attempt_id TEXT PRIMARY KEY REFERENCES native_attempts(attempt_id)
        ON DELETE RESTRICT,
      source_schema INTEGER NOT NULL CHECK(source_schema=17),
      source_digest TEXT NOT NULL CHECK(length(source_digest)=64),
      imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TRIGGER legacy_attempt_import_insert_guard
    BEFORE INSERT ON legacy_attempt_imports
    WHEN (SELECT user_version FROM pragma_user_version)!=17
    BEGIN
      SELECT RAISE(ABORT,'legacy attempt provenance is migration-only');
    END
    """,
    """
    CREATE TRIGGER legacy_attempt_import_immutable
    BEFORE UPDATE ON legacy_attempt_imports
    BEGIN
      SELECT RAISE(ABORT,'legacy attempt provenance is immutable');
    END
    """,
    """
    CREATE TRIGGER legacy_attempt_import_delete_forbidden
    BEFORE DELETE ON legacy_attempt_imports
    BEGIN
      SELECT RAISE(ABORT,'legacy attempt provenance cannot be deleted');
    END
    """,
    """
    CREATE TABLE endpoint_leases (
      attempt_id TEXT PRIMARY KEY,
      endpoint_id TEXT NOT NULL REFERENCES endpoints(endpoint_id) ON DELETE RESTRICT,
      endpoint_incarnation INTEGER NOT NULL CHECK(endpoint_incarnation>=1),
      fence INTEGER NOT NULL CHECK(fence>=1),
      acquired_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      expires_at TEXT NOT NULL,
      released_at TEXT,
      release_reason TEXT,
      UNIQUE(endpoint_id,fence),
      UNIQUE(attempt_id,endpoint_id),
      FOREIGN KEY(attempt_id) REFERENCES native_attempts(attempt_id)
        DEFERRABLE INITIALLY DEFERRED,
      CHECK(expires_at>acquired_at),
      CHECK(
        (released_at IS NULL AND release_reason IS NULL)
        OR (released_at IS NOT NULL AND release_reason IS NOT NULL)
      )
    )
    """,
    """
    CREATE UNIQUE INDEX endpoint_one_open_lease
    ON endpoint_leases(endpoint_id) WHERE released_at IS NULL
    """,
    """
    CREATE TABLE native_attempts (
      attempt_id TEXT PRIMARY KEY,
      endpoint_id TEXT NOT NULL,
      binding_id TEXT NOT NULL REFERENCES thread_bindings(binding_id) ON DELETE RESTRICT,
      binding_generation INTEGER NOT NULL CHECK(binding_generation>=1),
      driver_kind TEXT NOT NULL CHECK(driver_kind IN (
        'zellij','herdr','detached_native'
      )),
      driver_request_id TEXT NOT NULL UNIQUE,
      driver_request_hash TEXT NOT NULL CHECK(length(driver_request_hash)=64),
      cancel_request_id TEXT UNIQUE,
      cancel_request_hash TEXT,
      reply_token_hash TEXT NOT NULL UNIQUE CHECK(length(reply_token_hash)=64),
      receipt_cursor TEXT,
      last_driver_receipt_id TEXT,
      last_driver_sequence INTEGER NOT NULL DEFAULT 0 CHECK(last_driver_sequence>=0),
      state TEXT NOT NULL CHECK(state IN (
        'prepared','submitting','accepted','uncertain','completed_with_response',
        'no_reply','cancelled','failed_before_start','failed',
        'operator_completed','operator_abandoned'
      )),
      response_inline TEXT,
      response_ref TEXT,
      response_sha256 TEXT,
      response_bytes INTEGER CHECK(response_bytes IS NULL OR response_bytes>=0),
      hermes_egress_receipt_id TEXT UNIQUE,
      error_code TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      submitted_at TEXT,
      accepted_at TEXT,
      terminal_at TEXT,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(attempt_id,binding_id),
      UNIQUE(attempt_id,binding_id,binding_generation),
      FOREIGN KEY(attempt_id,endpoint_id)
        REFERENCES endpoint_leases(attempt_id,endpoint_id)
        DEFERRABLE INITIALLY DEFERRED,
      CHECK(
        (state IN (
          'completed_with_response','no_reply','cancelled','failed_before_start',
          'failed','operator_completed','operator_abandoned'
        )) = (terminal_at IS NOT NULL)
      ),
      CHECK(
        (state='completed_with_response'
          AND ((response_inline IS NOT NULL) <> (response_ref IS NOT NULL))
          AND response_sha256 IS NOT NULL AND length(response_sha256)=64
          AND response_bytes IS NOT NULL)
        OR (state!='completed_with_response' AND response_inline IS NULL
          AND response_ref IS NULL AND response_sha256 IS NULL
          AND response_bytes IS NULL)
      ),
      CHECK(
        hermes_egress_receipt_id IS NULL OR state='completed_with_response'
      ),
      CHECK(
        (cancel_request_id IS NULL AND cancel_request_hash IS NULL)
        OR (cancel_request_id IS NOT NULL AND length(cancel_request_hash)=64)
      ),
      CHECK(
        (last_driver_sequence=0 AND receipt_cursor IS NULL
          AND last_driver_receipt_id IS NULL)
        OR (last_driver_sequence>0 AND receipt_cursor IS NOT NULL
          AND last_driver_receipt_id IS NOT NULL)
      ),
      CHECK(
        state!='prepared' OR (
          submitted_at IS NULL AND accepted_at IS NULL AND terminal_at IS NULL
        )
      )
    )
    """,
    """
    CREATE UNIQUE INDEX attempt_last_driver_receipt
    ON native_attempts(last_driver_receipt_id)
    WHERE last_driver_receipt_id IS NOT NULL
    """,
    """
    CREATE TABLE operator_resolutions (
      attempt_id TEXT PRIMARY KEY REFERENCES native_attempts(attempt_id)
        ON DELETE RESTRICT,
      endpoint_id TEXT NOT NULL,
      lease_fence INTEGER NOT NULL CHECK(lease_fence>=1),
      action TEXT NOT NULL CHECK(action IN ('complete','abandon')),
      source_kind TEXT NOT NULL CHECK(source_kind IN ('authority','legacy_import')),
      authority_receipt_id TEXT NOT NULL UNIQUE,
      operator_principal_hash TEXT NOT NULL
        CHECK(length(operator_principal_hash)=64),
      evidence_ref TEXT NOT NULL CHECK(length(evidence_ref)>0),
      evidence_sha256 TEXT NOT NULL CHECK(length(evidence_sha256)=64),
      resolved_at TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(attempt_id,endpoint_id)
        REFERENCES endpoint_leases(attempt_id,endpoint_id) ON DELETE RESTRICT
    )
    """,
    """
    CREATE TRIGGER operator_resolution_insert_guard
    BEFORE INSERT ON operator_resolutions
    WHEN NOT EXISTS(
      SELECT 1 FROM native_attempts AS attempt
      JOIN endpoint_leases AS lease ON lease.attempt_id=attempt.attempt_id
      WHERE attempt.attempt_id=NEW.attempt_id
        AND attempt.endpoint_id=NEW.endpoint_id
        AND attempt.state='uncertain'
        AND attempt.terminal_at IS NULL
        AND lease.fence=NEW.lease_fence
        AND (
          (NEW.source_kind='authority' AND lease.released_at IS NULL)
          OR (
            NEW.source_kind='legacy_import'
            AND (SELECT user_version FROM pragma_user_version)=17
          )
          OR (
            NEW.source_kind='authority'
            AND (SELECT user_version FROM pragma_user_version)=17
            AND EXISTS(
              SELECT 1 FROM legacy_attempt_imports AS imported
              WHERE imported.attempt_id=attempt.attempt_id
            )
          )
        )
    )
    BEGIN
      SELECT RAISE(ABORT,'operator resolution is not authorized for this attempt');
    END
    """,
    """
    CREATE TRIGGER operator_resolution_immutable
    BEFORE UPDATE ON operator_resolutions
    BEGIN
      SELECT RAISE(ABORT,'operator resolution is immutable');
    END
    """,
    """
    CREATE TRIGGER operator_resolution_delete_forbidden
    BEFORE DELETE ON operator_resolutions
    BEGIN
      SELECT RAISE(ABORT,'operator resolution cannot be deleted');
    END
    """,
    """
    CREATE TABLE driver_receipts (
      receipt_id TEXT PRIMARY KEY,
      attempt_id TEXT NOT NULL,
      endpoint_id TEXT NOT NULL,
      lease_fence INTEGER NOT NULL CHECK(lease_fence>=1),
      sequence INTEGER NOT NULL CHECK(sequence>=1),
      driver_kind TEXT NOT NULL,
      driver_incarnation TEXT NOT NULL,
      operation TEXT NOT NULL DEFAULT 'submit'
        CHECK(operation IN ('submit','cancel')),
      request_id TEXT NOT NULL,
      request_hash TEXT NOT NULL CHECK(length(request_hash)=64),
      watch_cursor TEXT NOT NULL CHECK(length(watch_cursor)>0),
      state TEXT NOT NULL CHECK(state IN (
        'not_started','accepted','running','completed_with_response',
        'no_reply','failed','cancelled','uncertain'
      )),
      response_ref TEXT,
      response_sha256 TEXT,
      error_code TEXT,
      observed_at TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(attempt_id,lease_fence,sequence),
      FOREIGN KEY(attempt_id,endpoint_id)
        REFERENCES endpoint_leases(attempt_id,endpoint_id) ON DELETE RESTRICT,
      CHECK(
        operation='submit'
        OR state IN ('not_started','cancelled','uncertain')
      ),
      CHECK(
        (state='completed_with_response' AND response_ref IS NOT NULL
          AND length(response_sha256)=64)
        OR (state!='completed_with_response' AND response_ref IS NULL
          AND response_sha256 IS NULL)
      )
    )
    """,
    """
    CREATE INDEX driver_receipt_watch
    ON driver_receipts(attempt_id,lease_fence,sequence)
    """,
    """
    CREATE TABLE native_attempt_turns (
      attempt_id TEXT NOT NULL,
      ordinal INTEGER NOT NULL CHECK(ordinal>=0),
      event_key TEXT NOT NULL,
      binding_id TEXT NOT NULL,
      turn_binding_generation INTEGER NOT NULL,
      PRIMARY KEY(attempt_id,ordinal),
      UNIQUE(attempt_id,event_key),
      FOREIGN KEY(attempt_id,binding_id)
        REFERENCES native_attempts(attempt_id,binding_id)
        ON DELETE RESTRICT,
      FOREIGN KEY(event_key,binding_id,turn_binding_generation)
        REFERENCES queued_turns(event_key,binding_id,binding_generation)
        ON DELETE RESTRICT
    )
    """,
    """
    CREATE INDEX attempt_turn_event ON native_attempt_turns(event_key,attempt_id)
    """,
    """
    CREATE TRIGGER endpoint_identity_immutable
    BEFORE UPDATE OF endpoint_key,security_domain_id,instance_uid,workspace_id,
      persona_id,authorized_owners_json,authorized_owners_hash,
      policy_generation ON endpoints
    WHEN (SELECT user_version FROM pragma_user_version)!=17 AND (
      NEW.endpoint_key IS NOT OLD.endpoint_key
      OR NEW.security_domain_id IS NOT OLD.security_domain_id
      OR NEW.instance_uid IS NOT OLD.instance_uid
      OR NEW.workspace_id IS NOT OLD.workspace_id
      OR NEW.persona_id IS NOT OLD.persona_id
      OR NEW.authorized_owners_json IS NOT OLD.authorized_owners_json
      OR NEW.authorized_owners_hash IS NOT OLD.authorized_owners_hash
      OR NEW.policy_generation IS NOT OLD.policy_generation
    )
    BEGIN
      SELECT RAISE(ABORT,'endpoint identity is immutable');
    END
    """,
    """
    CREATE TRIGGER endpoint_ref_change_requires_incarnation
    BEFORE UPDATE OF endpoint_kind,source_kind,source_json,ref_version,
      capabilities_json ON endpoints
    WHEN (SELECT user_version FROM pragma_user_version)!=17 AND (
      NEW.endpoint_kind IS NOT OLD.endpoint_kind
      OR NEW.source_kind IS NOT OLD.source_kind
      OR NEW.source_json IS NOT OLD.source_json
      OR NEW.ref_version IS NOT OLD.ref_version
      OR NEW.capabilities_json IS NOT OLD.capabilities_json
    ) AND NEW.incarnation!=OLD.incarnation+1
    BEGIN
      SELECT RAISE(ABORT,'endpoint ref change requires next incarnation');
    END
    """,
    """
    CREATE TRIGGER endpoint_incarnation_guard
    BEFORE UPDATE OF incarnation ON endpoints
    WHEN (SELECT user_version FROM pragma_user_version)!=17
      AND NEW.incarnation!=OLD.incarnation AND (
      NEW.incarnation!=OLD.incarnation+1 OR EXISTS(
        SELECT 1 FROM endpoint_leases AS lease
        WHERE lease.endpoint_id=OLD.endpoint_id AND lease.released_at IS NULL
      )
    )
    BEGIN
      SELECT RAISE(ABORT,'endpoint incarnation change is not safe');
    END
    """,
    """
    CREATE TRIGGER endpoint_incarnation_invalidates_bindings
    AFTER UPDATE OF incarnation ON endpoints
    WHEN (SELECT user_version FROM pragma_user_version)!=17
      AND NEW.incarnation=OLD.incarnation+1
    BEGIN
      UPDATE thread_bindings
      SET state='rebind_required',error_code='endpoint_incarnation_changed',
          updated_at=CURRENT_TIMESTAMP
      WHERE endpoint_id=NEW.endpoint_id AND state!='closed';
    END
    """,
    """
    CREATE TRIGGER endpoint_state_forward_only
    BEFORE UPDATE OF state ON endpoints
    WHEN (SELECT user_version FROM pragma_user_version)!=17
      AND NEW.state!=OLD.state AND NOT (
      (OLD.state='ready' AND NEW.state='retired')
      OR (OLD.state='rebind_required' AND NEW.state='retired')
    )
    BEGIN
      SELECT RAISE(ABORT,'endpoint state transition is invalid');
    END
    """,
    """
    CREATE TRIGGER endpoint_retire_guard
    BEFORE UPDATE OF state ON endpoints
    WHEN NEW.state='retired' AND EXISTS(
      SELECT 1 FROM endpoint_leases AS lease
      WHERE lease.endpoint_id=OLD.endpoint_id AND lease.released_at IS NULL
    )
    BEGIN
      SELECT RAISE(ABORT,'endpoint with open lease cannot retire');
    END
    """,
    """
    CREATE TRIGGER endpoint_retire_invalidates_bindings
    AFTER UPDATE OF state ON endpoints
    WHEN NEW.state='retired' AND OLD.state!='retired'
    BEGIN
      UPDATE thread_bindings
      SET state='rebind_required',error_code='endpoint_retired',
          updated_at=CURRENT_TIMESTAMP
      WHERE endpoint_id=NEW.endpoint_id AND state!='closed';
    END
    """,
    """
    CREATE TRIGGER endpoint_lease_guard
    BEFORE INSERT ON endpoint_leases
    WHEN NEW.released_at IS NULL
      AND (SELECT user_version FROM pragma_user_version)!=17 AND NOT EXISTS(
      SELECT 1 FROM endpoints AS endpoint
      WHERE endpoint.endpoint_id=NEW.endpoint_id AND endpoint.state='ready'
        AND endpoint.incarnation=NEW.endpoint_incarnation
        AND endpoint.next_lease_fence=NEW.fence
    )
    BEGIN
      SELECT RAISE(ABORT,'stale endpoint lease');
    END
    """,
    """
    CREATE TRIGGER endpoint_lease_released_insert_guard
    BEFORE INSERT ON endpoint_leases
    WHEN NEW.released_at IS NOT NULL AND NOT (
      (SELECT user_version FROM pragma_user_version)=17
    )
    BEGIN
      SELECT RAISE(ABORT,'released endpoint lease is migration-only');
    END
    """,
    """
    CREATE TRIGGER endpoint_lease_fence_monotonic
    BEFORE UPDATE OF next_lease_fence ON endpoints
    WHEN (SELECT user_version FROM pragma_user_version)!=17
      AND NEW.next_lease_fence!=OLD.next_lease_fence+1
    BEGIN
      SELECT RAISE(ABORT,'endpoint lease fence must advance by one');
    END
    """,
    """
    CREATE TRIGGER binding_repoint_guard
    BEFORE UPDATE OF endpoint_id ON thread_bindings
    WHEN NEW.endpoint_id IS NOT OLD.endpoint_id AND NOT (
      OLD.state IN ('active','rebind_required')
      AND NEW.state='active'
      AND NEW.generation=OLD.generation+1
      AND EXISTS(
        SELECT 1 FROM endpoints AS endpoint
        WHERE endpoint.endpoint_id=NEW.endpoint_id
          AND endpoint.security_domain_id=NEW.security_domain_id
          AND endpoint.workspace_id=NEW.team_id
          AND endpoint.state='ready'
      )
    )
    BEGIN
      SELECT RAISE(ABORT,'binding repoint requires next generation');
    END
    """,
    """
    CREATE TRIGGER binding_open_lease_guard
    BEFORE UPDATE OF endpoint_id,generation,state ON thread_bindings
    WHEN (
      NEW.endpoint_id IS NOT OLD.endpoint_id
      OR NEW.generation!=OLD.generation
      OR NEW.state IS NOT OLD.state
    ) AND EXISTS(
      SELECT 1 FROM native_attempts AS attempt
      JOIN endpoint_leases AS lease ON lease.attempt_id=attempt.attempt_id
      WHERE attempt.binding_id=OLD.binding_id
        AND lease.released_at IS NULL
    )
    BEGIN
      SELECT RAISE(ABORT,'binding with open endpoint lease cannot change');
    END
    """,
    """
    CREATE TRIGGER binding_close_requires_empty_queue
    BEFORE UPDATE OF state ON thread_bindings
    WHEN NEW.state='closed' AND OLD.state!='closed' AND EXISTS(
      SELECT 1 FROM queued_turns AS turn
      WHERE turn.binding_id=OLD.binding_id AND turn.state='ready'
    )
    BEGIN
      SELECT RAISE(ABORT,'binding queue must be terminal before close');
    END
    """,
    """
    CREATE TRIGGER binding_identity_immutable
    BEFORE UPDATE OF security_domain_id,team_id,channel_id,owner_user_id,
      idempotency_key,request_hash,created_at ON thread_bindings
    WHEN NEW.security_domain_id IS NOT OLD.security_domain_id
      OR NEW.team_id IS NOT OLD.team_id
      OR NEW.channel_id IS NOT OLD.channel_id
      OR NEW.owner_user_id IS NOT OLD.owner_user_id
      OR NEW.idempotency_key IS NOT OLD.idempotency_key
      OR NEW.request_hash IS NOT OLD.request_hash
      OR NEW.created_at IS NOT OLD.created_at
    BEGIN
      SELECT RAISE(ABORT,'thread binding identity is immutable');
    END
    """,
    """
    CREATE TRIGGER binding_ready_endpoint_on_insert
    BEFORE INSERT ON thread_bindings
    WHEN NEW.state IN ('pending_root','active') AND NOT EXISTS(
      SELECT 1 FROM endpoints AS endpoint
      WHERE endpoint.endpoint_id=NEW.endpoint_id
        AND endpoint.security_domain_id=NEW.security_domain_id
        AND endpoint.state='ready'
    )
    BEGIN
      SELECT RAISE(ABORT,'live binding requires a ready endpoint');
    END
    """,
    """
    CREATE TRIGGER binding_ready_endpoint_on_activation
    BEFORE UPDATE OF state ON thread_bindings
    WHEN NEW.state='active' AND OLD.state!='active' AND NOT EXISTS(
      SELECT 1 FROM endpoints AS endpoint
      WHERE endpoint.endpoint_id=NEW.endpoint_id
        AND endpoint.security_domain_id=NEW.security_domain_id
        AND endpoint.state='ready'
    )
    BEGIN
      SELECT RAISE(ABORT,'binding activation requires a ready endpoint');
    END
    """,
    """
    CREATE TRIGGER binding_thread_identity_guard
    BEFORE UPDATE OF thread_ts ON thread_bindings
    WHEN NOT (
      OLD.state='pending_root' AND OLD.thread_ts IS NULL
      AND NEW.state='active' AND NEW.thread_ts IS NOT NULL
      AND NEW.generation=OLD.generation
    )
    BEGIN
      SELECT RAISE(ABORT,'thread identity change is invalid');
    END
    """,
    """
    CREATE TRIGGER binding_generation_guard
    BEFORE UPDATE OF generation ON thread_bindings
    WHEN NEW.generation!=OLD.generation+1 OR NOT (
      (OLD.state='rebind_required' AND NEW.state='active')
      OR (OLD.state='active' AND NEW.state='active'
        AND NEW.endpoint_id IS NOT OLD.endpoint_id)
      OR (OLD.state IN ('pending_root','active','rebind_required')
        AND NEW.state='closed')
    )
    BEGIN
      SELECT RAISE(ABORT,'binding generation must advance by one');
    END
    """,
    """
    CREATE TRIGGER binding_state_forward_only
    BEFORE UPDATE OF state ON thread_bindings
    WHEN NEW.state!=OLD.state AND NOT (
      (OLD.state='pending_root' AND NEW.state='active'
        AND NEW.generation=OLD.generation)
      OR (OLD.state='pending_root' AND NEW.state='rebind_required'
        AND NEW.generation=OLD.generation)
      OR (OLD.state='active' AND NEW.state='rebind_required'
        AND NEW.generation=OLD.generation)
      OR (OLD.state='rebind_required' AND NEW.state='active'
        AND NEW.generation=OLD.generation+1)
      OR (OLD.state IN ('pending_root','active','rebind_required')
        AND NEW.state='closed' AND NEW.generation=OLD.generation+1)
    )
    BEGIN
      SELECT RAISE(ABORT,'thread binding state transition is invalid');
    END
    """,
    """
    CREATE TRIGGER queued_turn_identity_immutable
    BEFORE UPDATE OF event_key,binding_id,binding_generation,ordered_at,
      mutation_kind,mutation_target_key,payload_inline,payload_ref,
      payload_sha256,payload_bytes,created_at ON queued_turns
    WHEN NEW.event_key IS NOT OLD.event_key
      OR NEW.binding_id IS NOT OLD.binding_id
      OR NEW.binding_generation IS NOT OLD.binding_generation
      OR NEW.ordered_at IS NOT OLD.ordered_at
      OR NEW.mutation_kind IS NOT OLD.mutation_kind
      OR NEW.mutation_target_key IS NOT OLD.mutation_target_key
      OR NEW.payload_inline IS NOT OLD.payload_inline
      OR NEW.payload_ref IS NOT OLD.payload_ref
      OR NEW.payload_sha256 IS NOT OLD.payload_sha256
      OR NEW.payload_bytes IS NOT OLD.payload_bytes
      OR NEW.created_at IS NOT OLD.created_at
    BEGIN
      SELECT RAISE(ABORT,'queued turn identity is immutable');
    END
    """,
    """
    CREATE TRIGGER queued_turn_admission_guard
    BEFORE INSERT ON queued_turns
    WHEN NOT EXISTS(
      SELECT 1 FROM thread_bindings AS binding
      WHERE binding.binding_id=NEW.binding_id
        AND NEW.binding_generation<=binding.generation
        AND (
          (SELECT user_version FROM pragma_user_version)=17
          OR (
            NEW.binding_generation=binding.generation
            AND binding.state='active'
          )
        )
    )
    BEGIN
      SELECT RAISE(ABORT,'queued turn binding generation is not admissible');
    END
    """,
    """
    CREATE TRIGGER queued_turn_initial_state_guard
    BEFORE INSERT ON queued_turns
    WHEN NEW.state!='ready'
      AND (SELECT user_version FROM pragma_user_version)!=17
    BEGIN
      SELECT RAISE(ABORT,'queued turn must be admitted ready');
    END
    """,
    """
    CREATE TRIGGER queued_turn_terminal_monotonic
    BEFORE UPDATE OF state,terminal_at,error_code ON queued_turns
    WHEN OLD.state!='ready' AND (
      NEW.state IS NOT OLD.state
      OR NEW.terminal_at IS NOT OLD.terminal_at
      OR NEW.error_code IS NOT OLD.error_code
    )
    BEGIN
      SELECT RAISE(ABORT,'queued turn terminal is immutable');
    END
    """,
    """
    CREATE TRIGGER queued_turn_state_forward_only
    BEFORE UPDATE OF state ON queued_turns
    WHEN NEW.state!=OLD.state
      AND NOT (OLD.state='ready' AND NEW.state IN ('completed','cancelled'))
    BEGIN
      SELECT RAISE(ABORT,'queued turn state transition is invalid');
    END
    """,
    """
    CREATE TRIGGER thread_binding_delete_forbidden
    BEFORE DELETE ON thread_bindings
    BEGIN
      SELECT RAISE(ABORT,'thread binding cannot be deleted');
    END
    """,
    """
    CREATE TRIGGER queued_turn_delete_forbidden
    BEFORE DELETE ON queued_turns
    BEGIN
      SELECT RAISE(ABORT,'queued turn cannot be deleted');
    END
    """,
    """
    CREATE TRIGGER endpoint_lease_identity_immutable
    BEFORE UPDATE OF attempt_id,endpoint_id,endpoint_incarnation,fence,
      acquired_at,expires_at ON endpoint_leases
    WHEN NEW.attempt_id IS NOT OLD.attempt_id
      OR NEW.endpoint_id IS NOT OLD.endpoint_id
      OR NEW.endpoint_incarnation IS NOT OLD.endpoint_incarnation
      OR NEW.fence IS NOT OLD.fence
      OR NEW.acquired_at IS NOT OLD.acquired_at
      OR NEW.expires_at IS NOT OLD.expires_at
    BEGIN
      SELECT RAISE(ABORT,'endpoint lease identity is immutable');
    END
    """,
    """
    CREATE TRIGGER endpoint_lease_release_monotonic
    BEFORE UPDATE OF released_at,release_reason ON endpoint_leases
    WHEN OLD.released_at IS NOT NULL AND (
      NEW.released_at IS NOT OLD.released_at
      OR NEW.release_reason IS NOT OLD.release_reason
    )
    BEGIN
      SELECT RAISE(ABORT,'endpoint lease release is terminal');
    END
    """,
    """
    CREATE TRIGGER endpoint_lease_first_release_guard
    BEFORE UPDATE OF released_at,release_reason ON endpoint_leases
    WHEN OLD.released_at IS NULL AND NEW.released_at IS NOT NULL AND NOT EXISTS(
      SELECT 1 FROM native_attempts AS attempt
      WHERE attempt.attempt_id=OLD.attempt_id
        AND attempt.endpoint_id=OLD.endpoint_id
        AND attempt.terminal_at IS NOT NULL
        AND attempt.state IN (
          'completed_with_response','no_reply','cancelled',
          'failed_before_start','failed','operator_completed',
          'operator_abandoned'
        )
    )
    BEGIN
      SELECT RAISE(ABORT,'endpoint lease release requires terminal execution');
    END
    """,
    """
    CREATE TRIGGER endpoint_lease_delete_forbidden
    BEFORE DELETE ON endpoint_leases
    BEGIN
      SELECT RAISE(ABORT,'endpoint lease cannot be deleted');
    END
    """,
    """
    CREATE TRIGGER native_attempt_identity_immutable
    BEFORE UPDATE OF attempt_id,endpoint_id,binding_id,binding_generation,
      driver_kind,driver_request_id,driver_request_hash,reply_token_hash,
      created_at
      ON native_attempts
    WHEN NEW.attempt_id IS NOT OLD.attempt_id
      OR NEW.endpoint_id IS NOT OLD.endpoint_id
      OR NEW.binding_id IS NOT OLD.binding_id
      OR NEW.binding_generation IS NOT OLD.binding_generation
      OR NEW.driver_kind IS NOT OLD.driver_kind
      OR NEW.driver_request_id IS NOT OLD.driver_request_id
      OR NEW.driver_request_hash IS NOT OLD.driver_request_hash
      OR NEW.reply_token_hash IS NOT OLD.reply_token_hash
      OR NEW.created_at IS NOT OLD.created_at
    BEGIN
      SELECT RAISE(ABORT,'native attempt identity is immutable');
    END
    """,
    """
    CREATE TRIGGER native_attempt_initial_state_guard
    BEFORE INSERT ON native_attempts
    WHEN NEW.state!='prepared'
    BEGIN
      SELECT RAISE(ABORT,'native attempt must start prepared');
    END
    """,
    """
    CREATE TRIGGER native_attempt_binding_guard
    BEFORE INSERT ON native_attempts
    WHEN NOT EXISTS(
      SELECT 1 FROM endpoint_leases AS lease
      JOIN thread_bindings AS binding ON binding.binding_id=NEW.binding_id
      JOIN endpoints AS endpoint ON endpoint.endpoint_id=NEW.endpoint_id
      WHERE lease.attempt_id=NEW.attempt_id
        AND lease.endpoint_id=NEW.endpoint_id
        AND binding.security_domain_id=endpoint.security_domain_id
        AND (
          (endpoint.endpoint_kind='zellij_pane' AND NEW.driver_kind='zellij')
          OR (endpoint.endpoint_kind='herdr_agent' AND NEW.driver_kind='herdr')
          OR (endpoint.endpoint_kind IN ('detached_native','hermes_continuation')
            AND NEW.driver_kind='detached_native')
          OR (SELECT user_version FROM pragma_user_version)=17
        )
        AND (
          (
            (SELECT user_version FROM pragma_user_version)=17
            AND NEW.binding_generation<=binding.generation
          ) OR (
            binding.state='active'
            AND binding.endpoint_id=NEW.endpoint_id
            AND binding.generation=NEW.binding_generation
            AND endpoint.state='ready'
            AND endpoint.incarnation=lease.endpoint_incarnation
          )
        )
    )
    BEGIN
      SELECT RAISE(ABORT,'native attempt binding is not runnable');
    END
    """,
    """
    CREATE TRIGGER native_attempt_membership_seal_guard
    BEFORE UPDATE OF state ON native_attempts
    WHEN OLD.state='prepared' AND NEW.state!='prepared' AND (
      (
        NOT EXISTS(
          SELECT 1 FROM legacy_attempt_imports AS imported
          WHERE imported.attempt_id=OLD.attempt_id
        ) AND NOT EXISTS(
        SELECT 1 FROM native_attempt_turns AS membership
        WHERE membership.attempt_id=OLD.attempt_id
        )
      ) OR EXISTS(
        SELECT 1 FROM native_attempt_turns AS membership
        WHERE membership.attempt_id=OLD.attempt_id
        GROUP BY membership.attempt_id
        HAVING MIN(membership.ordinal)!=0
          OR COUNT(*)!=MAX(membership.ordinal)+1
      )
    )
    BEGIN
      SELECT RAISE(ABORT,'native attempt membership is incomplete');
    END
    """,
    """
    CREATE TRIGGER native_attempt_cancel_identity_monotonic
    BEFORE UPDATE OF cancel_request_id,cancel_request_hash ON native_attempts
    WHEN OLD.cancel_request_id IS NOT NULL AND (
      NEW.cancel_request_id IS NOT OLD.cancel_request_id
      OR NEW.cancel_request_hash IS NOT OLD.cancel_request_hash
    )
    BEGIN
      SELECT RAISE(ABORT,'native attempt cancel identity is immutable');
    END
    """,
    """
    CREATE TRIGGER native_attempt_egress_receipt_monotonic
    BEFORE UPDATE OF hermes_egress_receipt_id ON native_attempts
    WHEN OLD.hermes_egress_receipt_id IS NOT NULL
      AND NEW.hermes_egress_receipt_id IS NOT OLD.hermes_egress_receipt_id
    BEGIN
      SELECT RAISE(ABORT,'native attempt egress receipt is immutable');
    END
    """,
    """
    CREATE TRIGGER native_attempt_forward_state
    BEFORE UPDATE OF state ON native_attempts
    WHEN NEW.state!=OLD.state AND NOT (
      (OLD.state='prepared' AND NEW.state IN (
        'submitting','failed_before_start','cancelled'
      ))
      OR (OLD.state='submitting' AND NEW.state IN (
        'accepted','uncertain','failed_before_start','failed','cancelled'
      ))
      OR (OLD.state='accepted' AND NEW.state IN (
        'uncertain','completed_with_response','no_reply','failed','cancelled'
      ))
      OR (OLD.state='uncertain' AND NEW.state IN (
        'accepted','completed_with_response','no_reply','failed_before_start',
        'failed','cancelled',
        'operator_completed','operator_abandoned'
      ))
    )
    BEGIN
      SELECT RAISE(ABORT,'native attempt state transition is invalid');
    END
    """,
    """
    CREATE TRIGGER native_attempt_state_timestamp_guard
    BEFORE UPDATE OF state,submitted_at,accepted_at ON native_attempts
    WHEN (
      (NEW.state IN (
        'submitting','accepted','uncertain','completed_with_response','no_reply',
        'failed','operator_completed','operator_abandoned'
      ) AND NEW.submitted_at IS NULL)
      OR (NEW.state IN ('accepted','completed_with_response','no_reply')
        AND NEW.accepted_at IS NULL)
      OR (OLD.submitted_at IS NOT NULL
        AND NEW.submitted_at IS NOT OLD.submitted_at)
      OR (OLD.accepted_at IS NOT NULL
        AND NEW.accepted_at IS NOT OLD.accepted_at)
    )
    BEGIN
      SELECT RAISE(ABORT,'native attempt timestamps do not match state');
    END
    """,
    """
    CREATE TRIGGER native_attempt_reconcile_transition_guard
    BEFORE UPDATE OF state ON native_attempts
    WHEN OLD.state='uncertain'
      AND NEW.state IN ('accepted','failed_before_start')
      AND (
        (NEW.state='failed_before_start' AND OLD.accepted_at IS NOT NULL)
        OR NOT EXISTS(
        SELECT 1 FROM driver_receipts AS receipt
        WHERE receipt.receipt_id=NEW.last_driver_receipt_id
          AND receipt.attempt_id=OLD.attempt_id
          AND receipt.sequence=NEW.last_driver_sequence
          AND (
            (NEW.state='accepted' AND receipt.state IN ('accepted','running')
              AND receipt.operation='submit'
              AND receipt.request_id=OLD.driver_request_id
              AND receipt.request_hash=OLD.driver_request_hash)
            OR (NEW.state='failed_before_start' AND receipt.state='not_started'
              AND receipt.operation='submit'
              AND receipt.request_id=OLD.driver_request_id
              AND receipt.request_hash=OLD.driver_request_hash)
          )
        )
      )
    BEGIN
      SELECT RAISE(ABORT,'uncertain recovery requires a fenced driver receipt');
    END
    """,
    """
    CREATE TRIGGER native_attempt_terminal_proof_guard
    BEFORE UPDATE OF state ON native_attempts
    WHEN NEW.state IN (
      'completed_with_response','no_reply','cancelled','failed_before_start',
      'failed','operator_completed','operator_abandoned'
    ) AND OLD.state!=NEW.state AND NOT (
      EXISTS(
        SELECT 1 FROM legacy_attempt_imports AS imported
        WHERE imported.attempt_id=OLD.attempt_id
      )
      OR (
        NEW.state IN ('operator_completed','operator_abandoned')
        AND EXISTS(
          SELECT 1 FROM operator_resolutions AS resolution
          WHERE resolution.attempt_id=OLD.attempt_id
        )
      )
      OR EXISTS(
        SELECT 1 FROM driver_receipts AS receipt
        WHERE receipt.receipt_id=NEW.last_driver_receipt_id
          AND receipt.attempt_id=OLD.attempt_id
          AND receipt.sequence=NEW.last_driver_sequence
          AND (
            (
              receipt.operation='submit'
              AND receipt.request_id=OLD.driver_request_id
              AND receipt.request_hash=OLD.driver_request_hash
              AND receipt.state=CASE NEW.state
              WHEN 'completed_with_response' THEN 'completed_with_response'
              WHEN 'no_reply' THEN 'no_reply'
              WHEN 'failed_before_start' THEN 'not_started'
              WHEN 'failed' THEN 'failed'
              END
            )
            OR (
              NEW.state='cancelled'
              AND receipt.operation='cancel'
              AND receipt.request_id=OLD.cancel_request_id
              AND receipt.request_hash=OLD.cancel_request_hash
              AND receipt.state IN ('not_started','cancelled')
              AND (receipt.state!='not_started' OR OLD.accepted_at IS NULL)
            )
          )
      )
    )
    BEGIN
      SELECT RAISE(ABORT,'native terminal requires durable execution proof');
    END
    """,
    """
    CREATE TRIGGER native_attempt_operator_resolution_guard
    BEFORE UPDATE OF state ON native_attempts
    WHEN NEW.state IN ('operator_completed','operator_abandoned')
      AND OLD.state!=NEW.state
      AND NOT EXISTS(
        SELECT 1 FROM operator_resolutions AS resolution
        WHERE resolution.attempt_id=OLD.attempt_id
          AND resolution.endpoint_id=OLD.endpoint_id
          AND resolution.action=CASE NEW.state
            WHEN 'operator_completed' THEN 'complete' ELSE 'abandon' END
      )
    BEGIN
      SELECT RAISE(ABORT,'operator terminal requires an authority receipt');
    END
    """,
    """
    CREATE TRIGGER native_attempt_terminal_monotonic
    BEFORE UPDATE OF state,response_inline,response_ref,response_sha256,
      response_bytes,error_code,terminal_at,cancel_request_id,
      cancel_request_hash,receipt_cursor,last_driver_receipt_id,
      last_driver_sequence,created_at,submitted_at,accepted_at ON native_attempts
    WHEN OLD.terminal_at IS NOT NULL
      AND (SELECT user_version FROM pragma_user_version)!=17 AND (
      NEW.state IS NOT OLD.state
      OR NEW.response_inline IS NOT OLD.response_inline
      OR NEW.response_ref IS NOT OLD.response_ref
      OR NEW.response_sha256 IS NOT OLD.response_sha256
      OR NEW.response_bytes IS NOT OLD.response_bytes
      OR NEW.error_code IS NOT OLD.error_code
      OR NEW.terminal_at IS NOT OLD.terminal_at
      OR NEW.cancel_request_id IS NOT OLD.cancel_request_id
      OR NEW.cancel_request_hash IS NOT OLD.cancel_request_hash
      OR NEW.receipt_cursor IS NOT OLD.receipt_cursor
      OR NEW.last_driver_receipt_id IS NOT OLD.last_driver_receipt_id
      OR NEW.last_driver_sequence IS NOT OLD.last_driver_sequence
      OR NEW.created_at IS NOT OLD.created_at
      OR NEW.submitted_at IS NOT OLD.submitted_at
      OR NEW.accepted_at IS NOT OLD.accepted_at
    )
    BEGIN
      SELECT RAISE(ABORT,'native attempt terminal is immutable');
    END
    """,
    """
    CREATE TRIGGER native_attempt_delete_forbidden
    BEFORE DELETE ON native_attempts
    BEGIN
      SELECT RAISE(ABORT,'native attempt cannot be deleted');
    END
    """,
    """
    CREATE TRIGGER driver_receipt_append_guard
    BEFORE INSERT ON driver_receipts
    WHEN NOT EXISTS(
      SELECT 1 FROM native_attempts AS attempt
      JOIN endpoint_leases AS lease ON lease.attempt_id=attempt.attempt_id
      WHERE attempt.attempt_id=NEW.attempt_id
        AND attempt.endpoint_id=NEW.endpoint_id
        AND attempt.driver_kind=NEW.driver_kind
        AND (
          (NEW.operation='submit'
            AND NEW.request_id=attempt.driver_request_id
            AND NEW.request_hash=attempt.driver_request_hash)
          OR (NEW.operation='cancel'
            AND NEW.request_id=attempt.cancel_request_id
            AND NEW.request_hash=attempt.cancel_request_hash)
        )
        AND attempt.terminal_at IS NULL
        AND lease.fence=NEW.lease_fence
        AND lease.released_at IS NULL
        AND NEW.sequence=attempt.last_driver_sequence+1
    ) AND NOT EXISTS(
      SELECT 1 FROM native_attempts AS attempt
      JOIN endpoint_leases AS lease ON lease.attempt_id=attempt.attempt_id
      JOIN legacy_attempt_imports AS imported
        ON imported.attempt_id=attempt.attempt_id
      WHERE (SELECT user_version FROM pragma_user_version)=17
        AND attempt.attempt_id=NEW.attempt_id
        AND attempt.endpoint_id=NEW.endpoint_id
        AND attempt.driver_kind=NEW.driver_kind
        AND (
          (NEW.operation='submit'
            AND NEW.request_id=attempt.driver_request_id
            AND NEW.request_hash=attempt.driver_request_hash)
          OR (NEW.operation='cancel'
            AND NEW.request_id=attempt.cancel_request_id
            AND NEW.request_hash=attempt.cancel_request_hash)
        )
        AND attempt.state='prepared'
        AND lease.fence=NEW.lease_fence
        AND NEW.sequence=attempt.last_driver_sequence+1
    )
    BEGIN
      SELECT RAISE(ABORT,'driver receipt is stale or out of order');
    END
    """,
    """
    CREATE TRIGGER native_attempt_receipt_cursor_guard
    BEFORE UPDATE OF receipt_cursor,last_driver_receipt_id,last_driver_sequence
      ON native_attempts
    WHEN NEW.last_driver_sequence!=OLD.last_driver_sequence
      OR NEW.last_driver_receipt_id IS NOT OLD.last_driver_receipt_id
      OR NEW.receipt_cursor IS NOT OLD.receipt_cursor
    BEGIN
      SELECT CASE WHEN NOT (
        NEW.last_driver_sequence=OLD.last_driver_sequence+1
        AND EXISTS(
          SELECT 1 FROM driver_receipts AS receipt
          WHERE receipt.receipt_id=NEW.last_driver_receipt_id
            AND receipt.attempt_id=NEW.attempt_id
            AND receipt.sequence=NEW.last_driver_sequence
            AND receipt.watch_cursor=NEW.receipt_cursor
        )
      ) THEN RAISE(ABORT,'driver receipt cursor is stale or out of order') END;
    END
    """,
    """
    CREATE TRIGGER driver_receipt_immutable
    BEFORE UPDATE ON driver_receipts
    BEGIN
      SELECT RAISE(ABORT,'driver receipt is immutable');
    END
    """,
    """
    CREATE TRIGGER driver_receipt_delete_forbidden
    BEFORE DELETE ON driver_receipts
    BEGIN
      SELECT RAISE(ABORT,'driver receipt cannot be deleted');
    END
    """,
    """
    CREATE TRIGGER native_attempt_turn_immutable
    BEFORE UPDATE ON native_attempt_turns
    BEGIN
      SELECT RAISE(ABORT,'native attempt turn is immutable');
    END
    """,
    """
    CREATE TRIGGER native_attempt_turn_insert_guard
    BEFORE INSERT ON native_attempt_turns
    WHEN NOT EXISTS(
      SELECT 1 FROM native_attempts AS attempt
      JOIN queued_turns AS turn
        ON turn.event_key=NEW.event_key
       AND turn.binding_id=NEW.binding_id
       AND turn.binding_generation=NEW.turn_binding_generation
      WHERE attempt.attempt_id=NEW.attempt_id
        AND attempt.state='prepared'
        AND (
          turn.state='ready'
          OR (SELECT user_version FROM pragma_user_version)=17
        )
    ) OR (
      (SELECT user_version FROM pragma_user_version)!=17
      AND EXISTS(
        SELECT 1 FROM native_attempt_turns AS prior_membership
        JOIN native_attempts AS prior_attempt
          ON prior_attempt.attempt_id=prior_membership.attempt_id
        WHERE prior_membership.event_key=NEW.event_key
          AND prior_attempt.state!='failed_before_start'
      )
    )
    BEGIN
      SELECT RAISE(ABORT,'native attempt membership is sealed or not retryable');
    END
    """,
    """
    CREATE TRIGGER native_attempt_turn_delete_forbidden
    BEFORE DELETE ON native_attempt_turns
    BEGIN
      SELECT RAISE(ABORT,'native attempt turn cannot be deleted');
    END
    """,
)


def install_schema(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA defer_foreign_keys=ON")
    for statement in DDL:
        connection.execute(statement)


def invariant_violations(connection: sqlite3.Connection) -> list[str]:
    checks: tuple[tuple[str, str], ...] = (
        (
            "foreign_key",
            "SELECT 1 FROM pragma_foreign_key_check LIMIT 1",
        ),
        (
            "endpoint_owner_projection_mismatch",
            """
            SELECT 1 FROM endpoints AS endpoint
            WHERE EXISTS(
              SELECT 1 FROM json_each(endpoint.authorized_owners_json) AS owner
              WHERE owner.type!='text' OR NOT EXISTS(
                SELECT 1 FROM endpoint_authorized_owners AS projected
                WHERE projected.endpoint_id=endpoint.endpoint_id
                  AND projected.security_domain_id=endpoint.security_domain_id
                  AND projected.owner_user_id=owner.value
              )
            ) OR EXISTS(
              SELECT 1 FROM endpoint_authorized_owners AS projected
              WHERE projected.endpoint_id=endpoint.endpoint_id
                AND projected.security_domain_id=endpoint.security_domain_id
                AND NOT EXISTS(
                  SELECT 1 FROM json_each(endpoint.authorized_owners_json) AS owner
                  WHERE owner.type='text'
                    AND owner.value=projected.owner_user_id
                )
            )
            LIMIT 1
            """,
        ),
        (
            "nonterminal_attempt_without_open_lease",
            """
            SELECT 1 FROM native_attempts AS attempt
            LEFT JOIN endpoint_leases AS lease
              ON lease.attempt_id=attempt.attempt_id
             AND lease.endpoint_id=attempt.endpoint_id
             AND lease.released_at IS NULL
            WHERE attempt.state IN ('prepared','submitting','accepted','uncertain')
              AND lease.attempt_id IS NULL LIMIT 1
            """,
        ),
        (
            "open_lease_not_latest_fence",
            """
            SELECT 1 FROM endpoint_leases AS lease
            JOIN endpoints AS endpoint ON endpoint.endpoint_id=lease.endpoint_id
            WHERE lease.released_at IS NULL
              AND lease.fence!=endpoint.next_lease_fence
            LIMIT 1
            """,
        ),
        (
            "attempt_without_dense_membership",
            """
            SELECT 1 FROM native_attempts AS attempt
            LEFT JOIN native_attempt_turns AS membership
              ON membership.attempt_id=attempt.attempt_id
            LEFT JOIN legacy_attempt_imports AS imported
              ON imported.attempt_id=attempt.attempt_id
            GROUP BY attempt.attempt_id
            HAVING (COUNT(membership.event_key)=0
                AND imported.attempt_id IS NULL)
              OR MIN(membership.ordinal)!=0
              OR COUNT(*)!=MAX(membership.ordinal)+1
            LIMIT 1
            """,
        ),
        (
            "attempt_security_domain_mismatch",
            """
            SELECT 1 FROM native_attempts AS attempt
            JOIN endpoints AS endpoint ON endpoint.endpoint_id=attempt.endpoint_id
            JOIN thread_bindings AS binding ON binding.binding_id=attempt.binding_id
            WHERE endpoint.security_domain_id!=binding.security_domain_id
            LIMIT 1
            """,
        ),
        (
            "attempt_driver_endpoint_mismatch",
            """
            SELECT 1 FROM native_attempts AS attempt
            JOIN endpoints AS endpoint ON endpoint.endpoint_id=attempt.endpoint_id
            WHERE NOT (
              (endpoint.endpoint_kind='zellij_pane'
                AND attempt.driver_kind='zellij')
              OR (endpoint.endpoint_kind='herdr_agent'
                AND attempt.driver_kind='herdr')
              OR (endpoint.endpoint_kind IN (
                    'detached_native','hermes_continuation'
                  ) AND attempt.driver_kind='detached_native')
            )
            LIMIT 1
            """,
        ),
        (
            "attempt_state_timestamp_mismatch",
            """
            SELECT 1 FROM native_attempts
            WHERE (
              state='prepared' AND (
                submitted_at IS NOT NULL OR accepted_at IS NOT NULL
                OR terminal_at IS NOT NULL
              )
            ) OR (
              state IN (
                'submitting','accepted','uncertain','completed_with_response',
                'no_reply','failed','operator_completed','operator_abandoned'
              ) AND submitted_at IS NULL
            ) OR (
              state IN ('accepted','completed_with_response','no_reply')
              AND accepted_at IS NULL
            )
            LIMIT 1
            """,
        ),
        (
            "terminal_attempt_with_open_lease",
            """
            SELECT 1 FROM native_attempts AS attempt
            JOIN endpoint_leases AS lease ON lease.attempt_id=attempt.attempt_id
            WHERE attempt.terminal_at IS NOT NULL AND lease.released_at IS NULL
            LIMIT 1
            """,
        ),
        (
            "nonterminal_attempt_with_released_lease",
            """
            SELECT 1 FROM native_attempts AS attempt
            JOIN endpoint_leases AS lease ON lease.attempt_id=attempt.attempt_id
            WHERE attempt.terminal_at IS NULL AND lease.released_at IS NOT NULL
            LIMIT 1
            """,
        ),
        (
            "stale_open_lease",
            """
            SELECT 1 FROM endpoint_leases AS lease
            JOIN endpoints AS endpoint ON endpoint.endpoint_id=lease.endpoint_id
            JOIN native_attempts AS attempt ON attempt.attempt_id=lease.attempt_id
            JOIN thread_bindings AS binding ON binding.binding_id=attempt.binding_id
            WHERE lease.released_at IS NULL AND (
              endpoint.state!='ready'
              OR endpoint.incarnation!=lease.endpoint_incarnation
              OR binding.endpoint_id!=lease.endpoint_id
              OR binding.generation!=attempt.binding_generation
              OR binding.state!='active'
              OR binding.security_domain_id!=endpoint.security_domain_id
            ) LIMIT 1
            """,
        ),
        (
            "ready_turn_on_closed_binding",
            """
            SELECT 1 FROM queued_turns AS turn
            JOIN thread_bindings AS binding ON binding.binding_id=turn.binding_id
            WHERE turn.state='ready' AND binding.state='closed'
            LIMIT 1
            """,
        ),
        (
            "turn_binding_generation_ahead",
            """
            SELECT 1 FROM queued_turns AS turn
            JOIN thread_bindings AS binding ON binding.binding_id=turn.binding_id
            WHERE turn.binding_generation>binding.generation
            LIMIT 1
            """,
        ),
        (
            "lease_fence_regression",
            """
            SELECT 1 FROM endpoints AS endpoint
            WHERE endpoint.next_lease_fence < COALESCE((
              SELECT MAX(lease.fence) FROM endpoint_leases AS lease
              WHERE lease.endpoint_id=endpoint.endpoint_id
            ),0) LIMIT 1
            """,
        ),
        (
            "completed_turn_without_terminal_attempt",
            """
            SELECT 1 FROM queued_turns AS turn
            WHERE turn.state='completed'
              AND NOT EXISTS(
                SELECT 1 FROM legacy_terminal_imports AS imported
                WHERE imported.event_key=turn.event_key
              )
              AND NOT EXISTS(
                SELECT 1 FROM native_attempt_turns AS membership
                JOIN native_attempts AS attempt
                  ON attempt.attempt_id=membership.attempt_id
                WHERE membership.event_key=turn.event_key
                  AND attempt.terminal_at IS NOT NULL
              )
            LIMIT 1
            """,
        ),
        (
            "cancelled_turn_without_terminal_attempt",
            """
            SELECT 1 FROM queued_turns AS turn
            WHERE turn.state='cancelled'
              AND NOT EXISTS(
                SELECT 1 FROM legacy_terminal_imports AS imported
                WHERE imported.event_key=turn.event_key
              )
              AND NOT EXISTS(
                SELECT 1 FROM native_attempt_turns AS membership
                JOIN native_attempts AS attempt
                  ON attempt.attempt_id=membership.attempt_id
                WHERE membership.event_key=turn.event_key
                  AND attempt.state IN (
                    'cancelled','failed','operator_abandoned'
                  )
              )
            LIMIT 1
            """,
        ),
        (
            "ready_turn_with_executed_terminal",
            """
            SELECT 1 FROM queued_turns AS turn
            JOIN native_attempt_turns AS membership
              ON membership.event_key=turn.event_key
            JOIN native_attempts AS attempt
              ON attempt.attempt_id=membership.attempt_id
            WHERE turn.state='ready' AND attempt.state IN (
              'completed_with_response','no_reply','failed','cancelled',
              'operator_completed','operator_abandoned'
            )
            LIMIT 1
            """,
        ),
        (
            "attempt_turn_state_mismatch",
            """
            WITH ranked AS (
              SELECT membership.event_key,membership.attempt_id,
                     ROW_NUMBER() OVER (
                       PARTITION BY membership.event_key
                       ORDER BY attempt.binding_generation DESC,
                                lease.fence DESC,attempt.attempt_id DESC
                     ) AS position
              FROM native_attempt_turns AS membership
              JOIN native_attempts AS attempt
                ON attempt.attempt_id=membership.attempt_id
              JOIN endpoint_leases AS lease
                ON lease.attempt_id=membership.attempt_id
            )
            SELECT 1 FROM ranked
            JOIN native_attempt_turns AS membership
              ON membership.event_key=ranked.event_key
             AND membership.attempt_id=ranked.attempt_id
            JOIN native_attempts AS attempt
              ON attempt.attempt_id=membership.attempt_id
            JOIN queued_turns AS turn ON turn.event_key=membership.event_key
            WHERE ranked.position=1 AND ((
              attempt.state IN (
                'completed_with_response','no_reply','operator_completed'
              ) AND turn.state!='completed'
            ) OR (
              attempt.state IN (
                'cancelled','failed','operator_abandoned'
              ) AND turn.state!='cancelled'
            ) OR (
              attempt.state IN (
                'prepared','submitting','accepted','uncertain',
                'failed_before_start'
              ) AND turn.state!='ready'
            ))
            LIMIT 1
            """,
        ),
        (
            "attempt_turn_generation_regression",
            """
            SELECT 1 FROM native_attempt_turns AS membership
            JOIN native_attempts AS attempt
              ON attempt.attempt_id=membership.attempt_id
            WHERE membership.turn_binding_generation>attempt.binding_generation
            LIMIT 1
            """,
        ),
        (
            "attempt_turn_illegal_retry",
            """
            SELECT 1 FROM native_attempt_turns AS later_membership
            JOIN native_attempts AS later_attempt
              ON later_attempt.attempt_id=later_membership.attempt_id
            JOIN endpoint_leases AS later_lease
              ON later_lease.attempt_id=later_attempt.attempt_id
            WHERE EXISTS(
              SELECT 1 FROM native_attempt_turns AS prior_membership
              JOIN native_attempts AS prior_attempt
                ON prior_attempt.attempt_id=prior_membership.attempt_id
              JOIN endpoint_leases AS prior_lease
                ON prior_lease.attempt_id=prior_attempt.attempt_id
              WHERE prior_membership.event_key=later_membership.event_key
                AND (
                  prior_attempt.binding_generation<later_attempt.binding_generation
                  OR (
                    prior_attempt.binding_generation=later_attempt.binding_generation
                    AND prior_lease.fence<later_lease.fence
                  ) OR (
                    prior_attempt.binding_generation=later_attempt.binding_generation
                    AND prior_lease.fence=later_lease.fence
                    AND prior_attempt.attempt_id<later_attempt.attempt_id
                  )
                )
                AND prior_attempt.state!='failed_before_start'
            )
            LIMIT 1
            """,
        ),
        (
            "operator_resolution_projection_mismatch",
            """
            SELECT 1 FROM native_attempts AS attempt
            LEFT JOIN operator_resolutions AS resolution
              ON resolution.attempt_id=attempt.attempt_id
            LEFT JOIN endpoint_leases AS lease
              ON lease.attempt_id=attempt.attempt_id
            WHERE (
              attempt.state='operator_completed' AND (
                resolution.action IS NOT 'complete'
                OR resolution.endpoint_id IS NOT attempt.endpoint_id
                OR resolution.lease_fence IS NOT lease.fence
              )
            ) OR (
              attempt.state='operator_abandoned' AND (
                resolution.action IS NOT 'abandon'
                OR resolution.endpoint_id IS NOT attempt.endpoint_id
                OR resolution.lease_fence IS NOT lease.fence
              )
            ) OR (
              resolution.attempt_id IS NOT NULL
              AND attempt.state NOT IN ('operator_completed','operator_abandoned')
            )
            LIMIT 1
            """,
        ),
        (
            "driver_receipt_fence_mismatch",
            """
            SELECT 1 FROM driver_receipts AS receipt
            JOIN endpoint_leases AS lease ON lease.attempt_id=receipt.attempt_id
            JOIN native_attempts AS attempt
              ON attempt.attempt_id=receipt.attempt_id
            WHERE receipt.endpoint_id!=lease.endpoint_id
              OR receipt.lease_fence!=lease.fence
              OR receipt.driver_kind!=attempt.driver_kind
              OR NOT (
                (receipt.operation='submit'
                  AND receipt.request_id=attempt.driver_request_id
                  AND receipt.request_hash=attempt.driver_request_hash)
                OR (receipt.operation='cancel'
                  AND receipt.request_id=attempt.cancel_request_id
                  AND receipt.request_hash=attempt.cancel_request_hash)
              )
            LIMIT 1
            """,
        ),
        (
            "driver_receipt_sequence_gap",
            """
            SELECT 1 FROM driver_receipts
            GROUP BY attempt_id,lease_fence
            HAVING MIN(sequence)!=1 OR COUNT(*)!=MAX(sequence)
            LIMIT 1
            """,
        ),
        (
            "unapplied_driver_receipt",
            """
            SELECT 1 FROM native_attempts AS attempt
            WHERE attempt.last_driver_sequence!=COALESCE((
              SELECT MAX(receipt.sequence) FROM driver_receipts AS receipt
              JOIN endpoint_leases AS lease
                ON lease.attempt_id=receipt.attempt_id
              WHERE receipt.attempt_id=attempt.attempt_id
                AND receipt.lease_fence=lease.fence
            ),0)
            LIMIT 1
            """,
        ),
        (
            "last_driver_receipt_mismatch",
            """
            SELECT 1 FROM native_attempts AS attempt
            WHERE (
              attempt.last_driver_receipt_id IS NULL
              AND attempt.last_driver_sequence!=0
            ) OR (
              attempt.last_driver_receipt_id IS NOT NULL AND NOT EXISTS(
                SELECT 1 FROM driver_receipts AS receipt
                WHERE receipt.receipt_id=attempt.last_driver_receipt_id
                  AND receipt.attempt_id=attempt.attempt_id
                  AND receipt.sequence=attempt.last_driver_sequence
                  AND receipt.watch_cursor=attempt.receipt_cursor
                  AND receipt.sequence=(
                    SELECT MAX(latest.sequence) FROM driver_receipts AS latest
                    JOIN endpoint_leases AS lease
                      ON lease.attempt_id=latest.attempt_id
                    WHERE latest.attempt_id=attempt.attempt_id
                      AND latest.lease_fence=lease.fence
                  )
              )
            ) LIMIT 1
            """,
        ),
        (
            "driver_receipt_projection_mismatch",
            """
            SELECT 1 FROM native_attempts AS attempt
            JOIN driver_receipts AS receipt
              ON receipt.receipt_id=attempt.last_driver_receipt_id
            WHERE (
              receipt.state='completed_with_response' AND (
                attempt.state!='completed_with_response'
                OR attempt.response_sha256 IS NOT receipt.response_sha256
              )
            ) OR (
              receipt.state='no_reply' AND attempt.state!='no_reply'
            ) OR (
              receipt.state='failed' AND attempt.state!='failed'
            ) OR (
              receipt.state='not_started'
              AND NOT (
                (attempt.state='failed_before_start'
                  AND receipt.operation='submit')
                OR (
                  attempt.state='cancelled' AND receipt.operation='cancel'
                  AND receipt.request_id=attempt.cancel_request_id
                  AND receipt.request_hash=attempt.cancel_request_hash
                  AND attempt.accepted_at IS NULL
                )
              )
            ) OR (
              receipt.state='cancelled' AND (
                attempt.state!='cancelled' OR receipt.operation!='cancel'
              )
            ) OR (
              receipt.state='uncertain' AND attempt.state NOT IN (
                'uncertain','operator_completed','operator_abandoned'
              )
            ) OR (
              receipt.state IN ('accepted','running')
              AND attempt.state!='accepted'
            )
            LIMIT 1
            """,
        ),
    )
    violations = [
        name for name, query in checks if connection.execute(query).fetchone()
    ]
    for row in connection.execute(
        """
        SELECT security_domain_id,instance_uid,workspace_id,persona_id,
               authorized_owners_json,authorized_owners_hash,policy_generation
        FROM endpoints
        """
    ):
        try:
            owners = json.loads(str(row[4]))
            if not isinstance(owners, list) or not all(
                isinstance(owner, str) for owner in owners
            ):
                raise ValueError("invalid owner list")
            descriptor = SecurityDomainDescriptor(
                instance_uid=int(row[1]),
                workspace_id=str(row[2]),
                persona_id=str(row[3]),
                authorized_owner_ids=tuple(owners),
                policy_generation=int(row[6]),
            )
            if (
                tuple(owners) != descriptor.canonical_owner_ids
                or str(row[5]) != descriptor.authorized_owners_hash
                or str(row[0]) != descriptor.security_domain_id
            ):
                raise ValueError("descriptor mismatch")
        except (TypeError, ValueError, json.JSONDecodeError):
            violations.append("endpoint_security_descriptor_mismatch")
            break
    for row in connection.execute(
        """
        SELECT payload_inline,payload_sha256,payload_bytes FROM queued_turns
        WHERE payload_inline IS NOT NULL
        """
    ):
        payload = str(row[0])
        if (
            str(row[1]) != _sha256_text(payload)
            or int(row[2]) != len(payload.encode())
        ):
            violations.append("queued_turn_inline_content_mismatch")
            break
    for row in connection.execute(
        """
        SELECT response_inline,response_sha256,response_bytes
        FROM native_attempts WHERE response_inline IS NOT NULL
        """
    ):
        response = str(row[0])
        if (
            str(row[1]) != _sha256_text(response)
            or int(row[2]) != len(response.encode())
        ):
            violations.append("attempt_inline_content_mismatch")
            break
    return violations


def require_valid(connection: sqlite3.Connection) -> None:
    violations = invariant_violations(connection)
    if violations:
        raise RuntimeError("invalid Tether domain state: " + ", ".join(violations))


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _digest_rows(rows: Iterable[tuple[Any, ...]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        values = tuple(row)
        digest.update(
            json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
        )
        digest.update(b"\n")
    return digest.hexdigest()


ROLLBACK_ARCHIVE_TABLE = "tether_domain_rollback_archive"


def _rollback_archive(connection: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (ROLLBACK_ARCHIVE_TABLE,),
    ).fetchone()
    if not exists:
        return {}
    records: dict[tuple[str, str], dict[str, Any]] = {}
    for row in connection.execute(
        """
        SELECT record_kind,record_key,payload_json,payload_sha256
        FROM tether_domain_rollback_archive ORDER BY record_kind,record_key
        """
    ):
        payload_json = str(row[2])
        if _sha256_text(payload_json) != str(row[3]):
            raise RuntimeError("rollback archive payload digest mismatch")
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError as exc:
            raise RuntimeError("rollback archive payload is invalid") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("rollback archive payload is not an object")
        records[(str(row[0]), str(row[1]))] = payload
    return records


def _create_rollback_archive(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE tether_domain_rollback_archive (
          record_kind TEXT NOT NULL CHECK(record_kind IN (
            'attempt_membership','turn_origin','binding_identity','endpoint_snapshot',
            'attempt_identity','driver_receipt','operator_resolution',
            'egress_receipt'
          )),
          record_key TEXT NOT NULL,
          payload_json TEXT NOT NULL
            CHECK(json_valid(payload_json) AND json_type(payload_json)='object'),
          payload_sha256 TEXT NOT NULL CHECK(length(payload_sha256)=64),
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(record_kind,record_key)
        )
        """
    )


def _install_rollback_horizon_guards(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE tether_domain_rollback_activity (
          record_kind TEXT NOT NULL CHECK(record_kind IN ('attempt','turn')),
          record_key TEXT NOT NULL,
          first_mutated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(record_kind,record_key)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE tether_domain_rollback_memberships (
          attempt_id TEXT NOT NULL,
          event_key TEXT NOT NULL,
          binding_id TEXT NOT NULL,
          turn_binding_generation INTEGER NOT NULL CHECK(turn_binding_generation>=1),
          attempt_binding_generation INTEGER NOT NULL CHECK(attempt_binding_generation>=1),
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(attempt_id,event_key)
        )
        """
    )
    connection.execute(
        """
        INSERT INTO tether_domain_rollback_memberships(
          attempt_id,event_key,binding_id,
          turn_binding_generation,attempt_binding_generation
        )
        SELECT membership.attempt_id,membership.event_key,membership.binding_id,
               membership.turn_binding_generation,attempt.binding_generation
        FROM native_attempt_turns AS membership
        JOIN native_attempts AS attempt
          ON attempt.attempt_id=membership.attempt_id
        ORDER BY membership.attempt_id,membership.ordinal,membership.event_key
        """
    )
    for table, key_column, record_kind in (
        ("bridges", "bridge_id", "binding_identity"),
        ("bridge_events", "event_id", "turn_origin"),
        ("bridge_attempts", "attempt_id", "attempt_identity"),
        ("bridge_replies", "reply_key", "attempt_identity"),
    ):
        membership_guard = (
            " OR EXISTS(SELECT 1 FROM tether_domain_rollback_archive LIMIT 1)"
            if table == "bridge_events"
            else
            " OR EXISTS(SELECT 1 FROM tether_domain_rollback_memberships "
            "WHERE attempt_id="
            f"OLD.{key_column})"  # nosec B608 - fixed identifiers.
            if table in {"bridge_attempts", "bridge_replies"}
            else ""
        )
        connection.execute(
            f"""
            CREATE TRIGGER tether_rollback_preserve_{table}
            BEFORE DELETE ON {table}
            WHEN EXISTS(
              SELECT 1 FROM tether_domain_rollback_archive
              WHERE record_kind='{record_kind}' AND record_key=OLD.{key_column}
            ){membership_guard}
            BEGIN SELECT RAISE(IGNORE); END
            """  # nosec - table/column/kind come only from fixed literals above.
        )
    connection.execute(
        """
        CREATE TRIGGER tether_rollback_journal_attempt_membership
        AFTER UPDATE OF attempt_id ON bridge_events
        WHEN NEW.attempt_id IS NOT NULL
          AND NEW.attempt_id IS NOT OLD.attempt_id
        BEGIN
          INSERT OR IGNORE INTO tether_domain_rollback_memberships(
            attempt_id,event_key,binding_id,
            turn_binding_generation,attempt_binding_generation
          ) VALUES(
            NEW.attempt_id,
            NEW.event_id,
            NEW.bridge_id,
            COALESCE(NEW.binding_generation,(
              SELECT binding_generation FROM bridges
              WHERE bridge_id=NEW.bridge_id
            )),
            (SELECT binding_generation FROM bridge_attempts
             WHERE attempt_id=NEW.attempt_id AND bridge_id=NEW.bridge_id)
          );
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER tether_rollback_track_attempt_update
        AFTER UPDATE OF state,ack_kind,message_ts,error_code,submitted_at,
          acknowledged_at,delivery_kind ON bridge_attempts
        WHEN (
          NEW.state IS NOT OLD.state
          OR NEW.ack_kind IS NOT OLD.ack_kind
          OR NEW.message_ts IS NOT OLD.message_ts
          OR NEW.error_code IS NOT OLD.error_code
          OR NEW.submitted_at IS NOT OLD.submitted_at
          OR NEW.acknowledged_at IS NOT OLD.acknowledged_at
          OR NEW.delivery_kind IS NOT OLD.delivery_kind
        ) AND EXISTS(
          SELECT 1 FROM tether_domain_rollback_archive
          WHERE record_kind='attempt_identity' AND record_key=NEW.attempt_id
        )
        BEGIN
          INSERT OR IGNORE INTO tether_domain_rollback_activity(
            record_kind,record_key
          ) VALUES('attempt',NEW.attempt_id);
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER tether_rollback_track_event_update
        AFTER UPDATE OF state,error,payload_json,attempt_id,binding_generation
          ON bridge_events
        WHEN (
          NEW.state IS NOT OLD.state
          OR NEW.error IS NOT OLD.error
          OR NEW.payload_json IS NOT OLD.payload_json
          OR NEW.attempt_id IS NOT OLD.attempt_id
          OR NEW.binding_generation IS NOT OLD.binding_generation
        ) AND EXISTS(
          SELECT 1 FROM tether_domain_rollback_archive
          WHERE record_kind='turn_origin' AND record_key=NEW.event_id
        )
        BEGIN
          INSERT OR IGNORE INTO tether_domain_rollback_activity(
            record_kind,record_key
          ) VALUES('turn',NEW.event_id);
        END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER tether_domain_rollback_archive_immutable
        BEFORE UPDATE ON tether_domain_rollback_archive
        BEGIN SELECT RAISE(ABORT,'rollback archive is immutable'); END
        """
    )
    connection.execute(
        """
        CREATE TRIGGER tether_domain_rollback_archive_delete_forbidden
        BEFORE DELETE ON tether_domain_rollback_archive
        BEGIN SELECT RAISE(ABORT,'rollback archive cannot be deleted'); END
        """
    )


def _remove_rollback_horizon_guards(connection: sqlite3.Connection) -> None:
    connection.execute("DROP TRIGGER tether_rollback_journal_attempt_membership")
    connection.execute("DROP TRIGGER tether_rollback_track_attempt_update")
    connection.execute("DROP TRIGGER tether_rollback_track_event_update")
    for table in ("bridges", "bridge_events", "bridge_attempts", "bridge_replies"):
        connection.execute(f"DROP TRIGGER tether_rollback_preserve_{table}")  # nosec B608


def _archive_record(
    connection: sqlite3.Connection,
    record_kind: str,
    record_key: str,
    payload: Mapping[str, Any],
) -> None:
    payload_json = json.dumps(
        dict(payload), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    connection.execute(
        """
        INSERT INTO tether_domain_rollback_archive(
          record_kind,record_key,payload_json,payload_sha256
        ) VALUES(?,?,?,?)
        """,
        (record_kind, record_key, payload_json, _sha256_text(payload_json)),
    )


def _legacy_attempt_membership_rows(
    connection: sqlite3.Connection,
) -> Iterable[tuple[Any, ...]]:
    archive = _rollback_archive(connection)
    rows: dict[tuple[str, str], tuple[str, str, str, int, int]] = {}
    for row in connection.execute(
            """
            SELECT event.attempt_id,event.event_id,event.bridge_id,
                   COALESCE(event.binding_generation,binding.binding_generation),
                   ROW_NUMBER() OVER (
                     PARTITION BY event.attempt_id
                     ORDER BY event.created_at,event.event_id
                   )-1 AS ordinal
            FROM bridge_events AS event
            JOIN bridges AS binding ON binding.bridge_id=event.bridge_id
            WHERE event.attempt_id IS NOT NULL
            """
        ):
        value = (str(row[0]), str(row[1]), str(row[2]), int(row[3]), int(row[4]))
        rows[(value[0], value[1])] = value
    journal_exists = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='tether_domain_rollback_memberships'
        """
    ).fetchone()
    if journal_exists:
        for row in connection.execute(
            """
            SELECT membership.attempt_id,membership.event_key,
                   membership.binding_id,
                   membership.attempt_binding_generation,
                   ROW_NUMBER() OVER (
                     PARTITION BY membership.attempt_id
                     ORDER BY event.created_at,event.event_id
                   )-1 AS ordinal
            FROM tether_domain_rollback_memberships AS membership
            JOIN bridge_events AS event ON event.event_id=membership.event_key
            ORDER BY membership.attempt_id,ordinal,membership.event_key
            """
        ):
            journaled = (
                str(row[0]), str(row[1]), str(row[2]), int(row[3]), int(row[4])
            )
            current = rows.get((journaled[0], journaled[1]))
            if current is not None and current[2] != journaled[2]:
                raise RuntimeError("rollback journal attempt membership conflicts")
            rows[(journaled[0], journaled[1])] = journaled
    for (kind, _), payload in archive.items():
        if kind != "attempt_membership":
            continue
        identity = archive.get(("attempt_identity", str(payload["attempt_id"])))
        if identity is not None and not _archived_attempt_is_current(
            connection, identity
        ):
            continue
        archived = (
            str(payload["attempt_id"]),
            str(payload["event_key"]),
            str(payload["binding_id"]),
            int(payload["attempt_binding_generation"]),
            int(payload["ordinal"]),
        )
        current = rows.get((archived[0], archived[1]))
        if current is not None and current[2] != archived[2]:
            raise RuntimeError("rollback archive attempt membership conflicts")
        rows[(archived[0], archived[1])] = archived
    return [
        row[:4]
        for row in sorted(rows.values(), key=lambda row: (row[0], row[4], row[1]))
    ]


def _legacy_attempt_target(
    row: Mapping[str, Any],
    has_response: bool,
) -> str:
    legacy_state = str(row["state"])
    ack_kind = str(row["ack_kind"] or "")
    if legacy_state in {"prepared", "requeued"}:
        return "failed_before_start"
    if legacy_state in {"submitting", "uncertain", "awaiting_ack"}:
        return "uncertain"
    if legacy_state == "replying":
        return "completed_with_response" if has_response else "uncertain"
    if legacy_state == "acknowledged" and ack_kind == "no_reply":
        return "no_reply"
    if legacy_state == "acknowledged" and ack_kind == "reply" and has_response:
        return "completed_with_response"
    if legacy_state == "acknowledged":
        return "operator_completed"
    if legacy_state == "cancelled" and ack_kind == "operator_abandoned":
        return "operator_abandoned"
    if legacy_state == "cancelled":
        return "cancelled"
    if legacy_state == "failed":
        return "cancelled" if row["submitted_at"] is None else "failed"
    return "uncertain"


def _legacy_projection_for_v18(
    state: str,
    *,
    response_delivered: bool = False,
) -> tuple[str, str | None]:
    if state == "completed_with_response":
        return ("acknowledged", "reply") if response_delivered else ("replying", None)
    if state == "no_reply":
        return "acknowledged", "no_reply"
    if state == "operator_completed":
        return "acknowledged", "operator_confirmed"
    if state == "operator_abandoned":
        return "cancelled", "operator_abandoned"
    if state == "cancelled":
        return "cancelled", None
    if state == "failed_before_start":
        return "requeued", None
    if state == "failed":
        return "failed", None
    raise RuntimeError("cannot project a nonterminal native attempt")


def _archived_attempt_is_current(
    connection: sqlite3.Connection,
    identity: Mapping[str, Any],
) -> bool:
    row = connection.execute(
        "SELECT state,ack_kind FROM bridge_attempts WHERE attempt_id=?",
        (str(identity["attempt_id"]),),
    ).fetchone()
    if row is None:
        return False
    current = (str(row[0]), str(row[1]) if row[1] is not None else None)
    projected = (
        str(identity["projected_legacy_state"]),
        identity.get("projected_ack_kind"),
    )
    activity_table = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='tether_domain_rollback_activity'
        """
    ).fetchone()
    mutated = bool(
        activity_table
        and connection.execute(
            """
            SELECT 1 FROM tether_domain_rollback_activity
            WHERE record_kind='attempt' AND record_key=?
            """,
            (str(identity["attempt_id"]),),
        ).fetchone()
    )
    egress_only_progress = (
        projected == ("replying", None)
        and current == ("acknowledged", "reply")
        and (
            reply := connection.execute(
                """
                SELECT text_hash,payload_text FROM bridge_replies
                WHERE reply_key=? AND bridge_id=? AND message_ts IS NOT NULL
                """,
                (str(identity["attempt_id"]), str(identity["binding_id"])),
            ).fetchone()
        )
        is not None
        and str(reply[0] or "") == str(identity.get("response_sha256") or "")
        and _sha256_text(str(reply[1] or ""))
        == str(identity.get("response_sha256") or "")
    )
    return egress_only_progress or (not mutated and current == projected)


def _archived_turn_was_mutated(
    connection: sqlite3.Connection,
    event_key: str,
) -> bool:
    table = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='tether_domain_rollback_activity'
        """
    ).fetchone()
    return bool(
        table
        and connection.execute(
            """
            SELECT 1 FROM tether_domain_rollback_activity
            WHERE record_kind='turn' AND record_key=?
            """,
            (event_key,),
        ).fetchone()
    )


def _legacy_turn_target(
    event_state: str,
    attempt_target: str | None,
) -> str:
    if attempt_target in {
        "completed_with_response",
        "no_reply",
        "operator_completed",
    }:
        return "completed"
    if attempt_target in {"cancelled", "failed", "operator_abandoned"}:
        return "cancelled"
    if event_state == "delivered":
        return "completed"
    if event_state == "failed":
        return "cancelled"
    return "ready"


def _legacy_attempt_outcome_rows(
    connection: sqlite3.Connection,
) -> Iterable[tuple[Any, ...]]:
    for row in connection.execute(
        """
        SELECT attempt.attempt_id,attempt.bridge_id,
               attempt.binding_generation,attempt.delivery_kind,
               attempt.state,attempt.ack_kind,attempt.error_code,
               attempt.submitted_at,reply.payload_text
        FROM bridge_attempts AS attempt
        LEFT JOIN bridge_replies AS reply
          ON reply.reply_key=attempt.attempt_id
         AND reply.bridge_id=attempt.bridge_id
        ORDER BY attempt.attempt_id
        """
    ):
        target = _legacy_attempt_target(
            {
                "state": row[4],
                "ack_kind": row[5],
                "submitted_at": row[7],
            },
            row[8] is not None,
        )
        yield (
            str(row[0]),
            str(row[1]),
            int(row[2]),
            str(row[3]),
            target,
            str(row[6] or "") or None,
            row[7] is not None
            or target not in {"failed_before_start", "cancelled"},
            target
            in {
                "completed_with_response",
                "no_reply",
                "cancelled",
                "failed_before_start",
                "failed",
                "operator_completed",
                "operator_abandoned",
            },
        )


def _legacy_attempt_route_rows(
    connection: sqlite3.Connection,
) -> Iterable[tuple[Any, ...]]:
    archive = _rollback_archive(connection)
    for row in connection.execute(
        """
        SELECT attempt.attempt_id,attempt.bridge_id,attempt.binding_generation,
               binding.endpoint_key
        FROM bridge_attempts AS attempt
        JOIN bridges AS binding ON binding.bridge_id=attempt.bridge_id
        ORDER BY attempt.attempt_id
        """
    ):
        identity = archive.get(("attempt_identity", str(row[0])))
        endpoint_key = str(row[3])
        if identity is not None and _archived_attempt_is_current(
            connection, identity
        ):
            snapshot = archive.get(("endpoint_snapshot", str(identity["endpoint_id"])))
            if snapshot is None:
                raise RuntimeError("rollback archive attempt endpoint is missing")
            endpoint_key = str(
                snapshot.get("endpoint_key")
                or snapshot.get("candidate_endpoint_key")
                or ""
            )
        yield (str(row[0]), str(row[1]), int(row[2]), endpoint_key)


def _legacy_turn_outcome_rows(
    connection: sqlite3.Connection,
) -> Iterable[tuple[Any, ...]]:
    for row in connection.execute(
        """
        SELECT event.event_id,event.state,event.error,
               attempt.state,attempt.ack_kind,attempt.submitted_at,
               reply.payload_text
        FROM bridge_events AS event
        LEFT JOIN bridge_attempts AS attempt
          ON attempt.attempt_id=event.attempt_id
         AND attempt.bridge_id=event.bridge_id
        LEFT JOIN bridge_replies AS reply
          ON reply.reply_key=attempt.attempt_id
         AND reply.bridge_id=attempt.bridge_id
        ORDER BY event.event_id
        """
    ):
        attempt_target = None
        if row[3] is not None:
            attempt_target = _legacy_attempt_target(
                {
                    "state": row[3],
                    "ack_kind": row[4],
                    "submitted_at": row[5],
                },
                row[6] is not None,
            )
        yield (
            str(row[0]),
            _legacy_turn_target(str(row[1]), attempt_target),
            str(row[2] or "") or None,
        )


def _legacy_turn_route_rows(
    connection: sqlite3.Connection,
) -> Iterable[tuple[Any, ...]]:
    for row in connection.execute(
        """
        SELECT event.event_id,event.bridge_id,event.binding_generation,
               binding.binding_generation,attempt.binding_generation
        FROM bridge_events AS event
        JOIN bridges AS binding ON binding.bridge_id=event.bridge_id
        LEFT JOIN bridge_attempts AS attempt
          ON attempt.attempt_id=event.attempt_id
         AND attempt.bridge_id=event.bridge_id
        ORDER BY event.event_id
        """
    ):
        execution_generation = (
            int(row[4])
            if row[4] is not None
            else int(row[3])
            if row[2] is None
            else int(row[2])
        )
        yield (str(row[0]), str(row[1]), execution_generation)


def _legacy_turn_order_rows(
    connection: sqlite3.Connection,
) -> Iterable[tuple[Any, ...]]:
    archive = _rollback_archive(connection)
    for row in connection.execute(
        "SELECT event_id,created_at FROM bridge_events ORDER BY event_id"
    ):
        origin = archive.get(("turn_origin", str(row[0])))
        yield (
            str(row[0]),
            str(origin["ordered_at"]) if origin is not None else str(row[1]),
        )


def _archived_endpoint_inventory_rows(
    connection: sqlite3.Connection,
) -> Iterable[tuple[Any, ...]]:
    archive = _rollback_archive(connection)
    keys = (
        "endpoint_id",
        "endpoint_key",
        "candidate_endpoint_key",
        "endpoint_kind",
        "source_kind",
        "source_json",
        "ref_version",
        "incarnation",
        "security_domain_id",
        "instance_uid",
        "workspace_id",
        "persona_id",
        "authorized_owners_json",
        "authorized_owners_hash",
        "policy_generation",
        "capabilities_json",
        "state",
        "error_code",
        "created_at",
        "updated_at",
    )
    for (kind, _record_key), payload in sorted(archive.items()):
        if kind == "endpoint_snapshot":
            yield tuple(payload[key] for key in keys)


def _v18_turn_route_rows(
    connection: sqlite3.Connection,
) -> Iterable[tuple[Any, ...]]:
    for row in connection.execute(
        """
        SELECT turn.event_key,turn.binding_id,turn.binding_generation,
               turn.state,binding.generation
        FROM queued_turns AS turn
        JOIN thread_bindings AS binding ON binding.binding_id=turn.binding_id
        ORDER BY turn.event_key
        """
    ):
        latest_attempt = connection.execute(
            """
            SELECT attempt.binding_generation
            FROM native_attempt_turns AS membership
            JOIN native_attempts AS attempt
              ON attempt.attempt_id=membership.attempt_id
            JOIN endpoint_leases AS lease
              ON lease.attempt_id=attempt.attempt_id
            WHERE membership.event_key=?
            ORDER BY attempt.binding_generation DESC,lease.fence DESC,
                     attempt.attempt_id DESC
            LIMIT 1
            """,
            (str(row[0]),),
        ).fetchone()
        execution_generation = (
            int(latest_attempt[0])
            if latest_attempt is not None
            else int(row[4])
            if str(row[3]) == "ready"
            else int(row[2])
        )
        yield (str(row[0]), str(row[1]), execution_generation)


def logical_manifest_v17(connection: sqlite3.Connection) -> dict[str, Any]:
    archive = _rollback_archive(connection)
    return {
        "schema_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
        "binding_count": int(connection.execute("SELECT count(*) FROM bridges").fetchone()[0]),
        "binding_ids": _digest_rows(
            connection.execute(
                "SELECT bridge_id,idempotency_key FROM bridges ORDER BY bridge_id"
            )
        ),
        "binding_records": _digest_rows(
            connection.execute(
                """
                SELECT bridge_id,source_kind,source_json,owner_user_id,team_id,
                       channel_id,thread_ts,idempotency_key,status,binding_version,
                       binding_generation,binding_state,binding_error_code,
                       endpoint_key,thread_claim_generation,created_at,updated_at
                FROM bridges ORDER BY bridge_id
                """
            )
        ),
        "binding_routes": _digest_rows(
            connection.execute(
                """
                SELECT bridge_id,team_id,channel_id,thread_ts,owner_user_id,
                       idempotency_key,binding_generation,
                       thread_claim_generation,endpoint_key
                FROM bridges ORDER BY bridge_id
                """
            )
        ),
        "binding_sources": _digest_rows(
            connection.execute(
                """
                SELECT bridge_id,source_kind,source_json,binding_version
                FROM bridges ORDER BY bridge_id
                """
            )
        ),
        "endpoint_inventory": (
            _digest_rows(_archived_endpoint_inventory_rows(connection))
            if archive
            else None
        ),
        "turn_count": int(connection.execute("SELECT count(*) FROM bridge_events").fetchone()[0]),
        "turn_payloads": _digest_rows(
            (
                (str(row[0]), _sha256_text(str(row[1])))
                for row in connection.execute(
                    "SELECT event_id,payload_json FROM bridge_events ORDER BY event_id"
                )
            )
        ),
        "turn_order": _digest_rows(_legacy_turn_order_rows(connection)),
        "turn_records": _digest_rows(
            connection.execute(
                """
                SELECT event_id,bridge_id,state,error,payload_json,attempt_id,
                       binding_generation,created_at,updated_at
                FROM bridge_events ORDER BY event_id
                """
            )
        ),
        "turn_outcomes": _digest_rows(_legacy_turn_outcome_rows(connection)),
        "turn_routes": _digest_rows(
            _legacy_turn_route_rows(connection)
        ),
        "attempt_count": int(connection.execute("SELECT count(*) FROM bridge_attempts").fetchone()[0]),
        "attempt_ids": _digest_rows(
            connection.execute(
                "SELECT attempt_id,bridge_id,binding_generation FROM bridge_attempts ORDER BY attempt_id"
            )
        ),
        "attempt_routes": _digest_rows(_legacy_attempt_route_rows(connection)),
        "attempt_records": _digest_rows(
            connection.execute(
                """
                SELECT attempt_id,reply_key,bridge_id,binding_generation,
                       delivery_kind,state,ack_kind,message_ts,error_code,
                       created_at,updated_at,submitted_at,acknowledged_at
                FROM bridge_attempts ORDER BY attempt_id
                """
            )
        ),
        "attempt_outcomes": _digest_rows(
            _legacy_attempt_outcome_rows(connection)
        ),
        "attempt_memberships": _digest_rows(
            _legacy_attempt_membership_rows(connection)
        ),
        "response_payloads": _digest_rows(
            (
                (str(row[0]), _sha256_text(str(row[1])))
                for row in connection.execute(
                    """
                    SELECT attempt.attempt_id,reply.payload_text
                    FROM bridge_attempts AS attempt
                    JOIN bridge_replies AS reply
                      ON reply.reply_key=attempt.attempt_id
                     AND reply.bridge_id=attempt.bridge_id
                    WHERE reply.payload_text IS NOT NULL
                    ORDER BY attempt.attempt_id
                    """
                )
            )
        ),
        "driver_receipts": _digest_rows(
            tuple(payload[key] for key in (
                "receipt_id","attempt_id","endpoint_id","lease_fence",
                "sequence","driver_kind","driver_incarnation","operation",
                "request_id","request_hash","watch_cursor",
                "state","response_ref","response_sha256","error_code",
                "observed_at","created_at",
            ))
            for (kind, _), payload in sorted(archive.items())
            if kind == "driver_receipt"
            and (
                (identity := archive.get(
                    ("attempt_identity", str(payload["attempt_id"]))
                )) is None
                or _archived_attempt_is_current(connection, identity)
            )
        ),
        "authority_resolutions": _digest_rows(
            tuple(payload[key] for key in (
                "attempt_id","endpoint_id","lease_fence","action",
                "authority_receipt_id","operator_principal_hash","evidence_ref",
                "evidence_sha256","resolved_at","created_at",
            ))
            for (kind, _), payload in sorted(archive.items())
            if kind == "operator_resolution"
            and (
                (identity := archive.get(
                    ("attempt_identity", str(payload["attempt_id"]))
                )) is None
                or _archived_attempt_is_current(connection, identity)
            )
        ),
        "egress_receipts": _digest_rows(
            (str(payload["attempt_id"]), str(payload["receipt_id"]))
            for (kind, _), payload in sorted(archive.items())
            if kind == "egress_receipt"
            and (
                (identity := archive.get(
                    ("attempt_identity", str(payload["attempt_id"]))
                )) is None
                or _archived_attempt_is_current(connection, identity)
            )
        ),
    }


def logical_manifest_v18(connection: sqlite3.Connection) -> dict[str, Any]:
    return {
        "schema_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
        "binding_count": int(connection.execute("SELECT count(*) FROM thread_bindings").fetchone()[0]),
        "binding_ids": _digest_rows(
            connection.execute(
                "SELECT binding_id,idempotency_key FROM thread_bindings ORDER BY binding_id"
            )
        ),
        "binding_records": _digest_rows(
            connection.execute(
                """
                SELECT binding.binding_id,binding.endpoint_id,
                       binding.security_domain_id,binding.team_id,
                       binding.channel_id,binding.thread_ts,
                       binding.owner_user_id,binding.idempotency_key,
                       binding.request_hash,binding.generation,binding.state,
                       binding.thread_claim_generation,binding.error_code,
                       endpoint.endpoint_key,endpoint.candidate_endpoint_key,
                       endpoint.endpoint_kind,endpoint.source_kind,
                       endpoint.source_json,endpoint.ref_version,
                       endpoint.incarnation,endpoint.instance_uid,
                       endpoint.workspace_id,endpoint.persona_id,
                       endpoint.authorized_owners_json,
                       endpoint.authorized_owners_hash,
                       endpoint.policy_generation,endpoint.state,
                       endpoint.error_code,binding.created_at,binding.updated_at
                FROM thread_bindings AS binding
                JOIN endpoints AS endpoint
                  ON endpoint.endpoint_id=binding.endpoint_id
                ORDER BY binding.binding_id
                """
            )
        ),
        "binding_routes": _digest_rows(
            connection.execute(
                """
                SELECT binding.binding_id,binding.team_id,binding.channel_id,
                       binding.thread_ts,binding.owner_user_id,
                       binding.idempotency_key,binding.generation,
                       binding.thread_claim_generation,
                       COALESCE(endpoint.endpoint_key,
                                endpoint.candidate_endpoint_key,'')
                FROM thread_bindings AS binding
                JOIN endpoints AS endpoint
                  ON endpoint.endpoint_id=binding.endpoint_id
                ORDER BY binding.binding_id
                """
            )
        ),
        "binding_sources": _digest_rows(
            connection.execute(
                """
                SELECT binding.binding_id,
                       CASE WHEN provenance.binding_generation=binding.generation
                            THEN provenance.source_kind ELSE endpoint.source_kind END,
                       CASE WHEN provenance.binding_generation=binding.generation
                            THEN provenance.source_json ELSE endpoint.source_json END,
                       CASE WHEN provenance.binding_generation=binding.generation
                            THEN provenance.ref_version ELSE endpoint.ref_version END
                FROM thread_bindings AS binding
                JOIN endpoints AS endpoint ON endpoint.endpoint_id=binding.endpoint_id
                LEFT JOIN legacy_binding_sources AS provenance
                  ON provenance.binding_id=binding.binding_id
                ORDER BY binding.binding_id
                """
            )
        ),
        "endpoint_inventory": _digest_rows(
            connection.execute(
                """
                SELECT endpoint_id,endpoint_key,candidate_endpoint_key,
                       endpoint_kind,source_kind,source_json,ref_version,
                       incarnation,security_domain_id,instance_uid,workspace_id,
                       persona_id,authorized_owners_json,authorized_owners_hash,
                       policy_generation,capabilities_json,state,error_code,
                       created_at,updated_at
                FROM endpoints ORDER BY endpoint_id
                """
            )
        ),
        "turn_count": int(connection.execute("SELECT count(*) FROM queued_turns").fetchone()[0]),
        "turn_payloads": _digest_rows(
            (
                (
                    str(row[0]),
                    _sha256_text(
                        json.dumps(
                            {"text": str(row[1])},
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    ),
                )
                for row in connection.execute(
                    "SELECT event_key,payload_inline FROM queued_turns ORDER BY event_key"
                )
            )
        ),
        "turn_order": _digest_rows(
            connection.execute(
                "SELECT event_key,ordered_at FROM queued_turns ORDER BY event_key"
            )
        ),
        "turn_records": _digest_rows(
            connection.execute(
                """
                SELECT event_key,binding_id,binding_generation,ordered_at,
                       mutation_kind,mutation_target_key,payload_inline,
                       payload_ref,payload_sha256,payload_bytes,state,
                       terminal_at,error_code,created_at,updated_at
                FROM queued_turns ORDER BY event_key
                """
            )
        ),
        "turn_outcomes": _digest_rows(
            connection.execute(
                """
                SELECT event_key,state,error_code
                FROM queued_turns ORDER BY event_key
                """
            )
        ),
        "turn_routes": _digest_rows(
            _v18_turn_route_rows(connection)
        ),
        "attempt_count": int(connection.execute("SELECT count(*) FROM native_attempts").fetchone()[0]),
        "attempt_ids": _digest_rows(
            connection.execute(
                "SELECT attempt_id,binding_id,binding_generation FROM native_attempts ORDER BY attempt_id"
            )
        ),
        "attempt_routes": _digest_rows(
            connection.execute(
                """
                SELECT attempt.attempt_id,attempt.binding_id,
                       attempt.binding_generation,
                       COALESCE(endpoint.endpoint_key,
                                endpoint.candidate_endpoint_key,'')
                FROM native_attempts AS attempt
                JOIN endpoints AS endpoint ON endpoint.endpoint_id=attempt.endpoint_id
                ORDER BY attempt.attempt_id
                """
            )
        ),
        "attempt_records": _digest_rows(
            connection.execute(
                """
                SELECT attempt.attempt_id,attempt.endpoint_id,
                       attempt.binding_id,attempt.binding_generation,
                       attempt.driver_kind,attempt.driver_request_id,
                       attempt.driver_request_hash,attempt.cancel_request_id,
                       attempt.cancel_request_hash,attempt.reply_token_hash,
                       attempt.receipt_cursor,
                       attempt.last_driver_receipt_id,
                       attempt.last_driver_sequence,attempt.state,
                       attempt.response_inline,attempt.response_ref,
                       attempt.response_sha256,attempt.response_bytes,
                       attempt.hermes_egress_receipt_id,attempt.error_code,
                       lease.endpoint_incarnation,lease.fence,
                       lease.acquired_at,lease.expires_at,lease.released_at,
                       lease.release_reason,attempt.created_at,
                       attempt.submitted_at,attempt.accepted_at,
                       attempt.terminal_at,attempt.updated_at
                FROM native_attempts AS attempt
                JOIN endpoint_leases AS lease
                  ON lease.attempt_id=attempt.attempt_id
                ORDER BY attempt.attempt_id
                """
            )
        ),
        "attempt_outcomes": _digest_rows(
            (
                (
                    str(row[0]),
                    str(row[1]),
                    int(row[2]),
                    str(row[3]),
                    str(row[4]),
                    str(row[5] or "") or None,
                    row[6] is not None,
                    row[7] is not None,
                )
                for row in connection.execute(
                    """
                    SELECT attempt_id,binding_id,binding_generation,driver_kind,
                           state,error_code,submitted_at,terminal_at
                    FROM native_attempts ORDER BY attempt_id
                    """
                )
            )
        ),
        "attempt_memberships": _digest_rows(
            connection.execute(
                """
                SELECT membership.attempt_id,membership.event_key,
                       membership.binding_id,attempt.binding_generation
                FROM native_attempt_turns AS membership
                JOIN native_attempts AS attempt
                  ON attempt.attempt_id=membership.attempt_id
                ORDER BY membership.attempt_id,membership.ordinal,
                         membership.event_key
                """
            )
        ),
        "response_payloads": _digest_rows(
            (
                (str(row[0]), _sha256_text(str(row[1])))
                for row in connection.execute(
                    """
                    SELECT attempt_id,response_inline FROM native_attempts
                    WHERE response_inline IS NOT NULL ORDER BY attempt_id
                    """
                )
            )
        ),
        "driver_receipts": _digest_rows(
            connection.execute(
                """
                SELECT receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
                       driver_kind,driver_incarnation,operation,request_id,
                       request_hash,watch_cursor,state,
                       response_ref,response_sha256,error_code,observed_at,created_at
                FROM driver_receipts ORDER BY receipt_id
                """
            )
        ),
        "authority_resolutions": _digest_rows(
            connection.execute(
                """
                SELECT attempt_id,endpoint_id,lease_fence,action,
                       authority_receipt_id,operator_principal_hash,evidence_ref,
                       evidence_sha256,resolved_at,created_at
                FROM operator_resolutions WHERE source_kind='authority'
                ORDER BY attempt_id
                """
            )
        ),
        "egress_receipts": _digest_rows(
            connection.execute(
                """
                SELECT attempt_id,hermes_egress_receipt_id FROM native_attempts
                WHERE hermes_egress_receipt_id IS NOT NULL ORDER BY attempt_id
                """
            )
        ),
    }


def backup_database(source_path: Path, destination_path: Path) -> None:
    source = Path(source_path)
    destination = Path(destination_path)
    if not source.is_absolute() or not destination.is_absolute():
        raise ValueError("database backup paths must be absolute")
    source_info = source.lstat()
    if (
        not stat.S_ISREG(source_info.st_mode)
        or source_info.st_nlink != 1
        or source_info.st_uid != os.geteuid()
    ):
        raise ValueError(
            "source database must be an owner-owned regular single-link file"
        )
    parent_info = destination.parent.lstat()
    if (
        not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.geteuid()
        or stat.S_IMODE(parent_info.st_mode) & 0o022
    ):
        raise ValueError("backup directory must be private and owner-owned")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(destination, flags, 0o600)
    os.close(descriptor)
    try:
        with sqlite3.connect(source) as source_db, sqlite3.connect(destination) as backup:
            source_db.backup(backup)
            if str(backup.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
                raise RuntimeError("database backup integrity check failed")
        os.chmod(destination, 0o600)
        synced = os.open(destination, os.O_RDONLY | os.O_CLOEXEC)
        try:
            os.fsync(synced)
        finally:
            os.close(synced)
        parent = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            destination.unlink()
        raise


def _request_hash(row: Mapping[str, Any], endpoint_id: str, domain_id: str) -> str:
    material = json.dumps(
        {
            "endpoint_id": endpoint_id,
            "security_domain_id": domain_id,
            "team_id": str(row["team_id"]),
            "channel_id": str(row["channel_id"]),
            "thread_ts": str(row["thread_ts"] or ""),
            "owner_user_id": str(row["owner_user_id"]),
            "idempotency_key": str(row["idempotency_key"]),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return _sha256_text(material)


def _preflight_legacy(connection: sqlite3.Connection) -> None:
    if int(connection.execute("PRAGMA user_version").fetchone()[0]) != 17:
        raise RuntimeError("Tether domain migration requires schema 17")
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    if integrity != "ok":
        raise RuntimeError("legacy database integrity check failed")
    checks = (
        """
        SELECT 1 FROM bridge_events AS event
        LEFT JOIN bridges AS binding ON binding.bridge_id=event.bridge_id
        WHERE binding.bridge_id IS NULL LIMIT 1
        """,
        """
        SELECT 1 FROM bridge_attempts AS attempt
        LEFT JOIN bridges AS binding ON binding.bridge_id=attempt.bridge_id
        WHERE binding.bridge_id IS NULL LIMIT 1
        """,
        """
        SELECT 1 FROM bridge_events AS event
        JOIN bridges AS binding ON binding.bridge_id=event.bridge_id
        LEFT JOIN bridge_attempts AS attempt
          ON attempt.attempt_id=event.attempt_id
         AND attempt.bridge_id=event.bridge_id
         AND attempt.binding_generation=COALESCE(
           event.binding_generation,binding.binding_generation
         )
        WHERE event.attempt_id IS NOT NULL AND attempt.attempt_id IS NULL
        LIMIT 1
        """,
        """
        SELECT 1 FROM bridge_events
        WHERE NOT json_valid(payload_json) OR json_type(payload_json)!='object'
        LIMIT 1
        """,
        """
        SELECT 1 FROM bridges
        WHERE thread_ts IS NOT NULL AND status!='closed'
        GROUP BY team_id,channel_id,thread_ts HAVING count(*)>1 LIMIT 1
        """,
    )
    if any(connection.execute(query).fetchone() for query in checks):
        raise RuntimeError("legacy database failed schema-18 migration preflight")


def migrate_legacy_v17(
    connection: sqlite3.Connection,
    descriptor: SecurityDomainDescriptor,
    resolve_endpoint: Callable[[Mapping[str, Any]], LegacyEndpointRef],
    fault_inject: Callable[[str], None] | None = None,
) -> None:
    """Atomically replace the three schema-17 routing authorities.

    The caller must quiesce admission before invoking this function. A failed
    preflight or copy rolls back the whole transaction, including target DDL.
    """
    descriptor.validate()
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("BEGIN IMMEDIATE")
    try:
        _preflight_legacy(connection)
        rollback_archive = _rollback_archive(connection)
        used_archive: set[tuple[str, str]] = set()
        preserved_before = logical_manifest_v17(connection)
        if fault_inject is not None:
            fault_inject("after_preflight")
        connection.execute("PRAGMA defer_foreign_keys=ON")
        install_schema(connection)
        if fault_inject is not None:
            fault_inject("after_schema")
        domain_id = descriptor.security_domain_id
        legacy_bindings = connection.execute(
            "SELECT * FROM bridges ORDER BY created_at,bridge_id"
        ).fetchall()
        unauthorized_owners = sorted(
            {
                str(row["owner_user_id"])
                for row in legacy_bindings
                if str(row["owner_user_id"])
                not in descriptor.canonical_owner_ids
            }
        )
        if unauthorized_owners:
            raise RuntimeError(
                "legacy database contains owners outside the security domain"
            )
        resolved: dict[str, LegacyEndpointRef] = {}
        groups: dict[str, list[sqlite3.Row]] = {}
        archive_binding_changed = False
        for row in legacy_bindings:
            ref = resolve_endpoint(row)
            if ref.endpoint_kind not in {
                "zellij_pane",
                "herdr_agent",
                "detached_native",
                "hermes_continuation",
                "unknown",
            }:
                raise RuntimeError("legacy endpoint resolver returned an invalid kind")
            if ref.ref_version < 1:
                raise RuntimeError("legacy endpoint resolver returned an invalid version")
            try:
                source = json.loads(ref.source_json)
            except json.JSONDecodeError as exc:
                raise RuntimeError("legacy endpoint resolver returned invalid JSON") from exc
            if not isinstance(source, dict):
                raise RuntimeError("legacy endpoint source must be an object")
            binding_id = str(row["bridge_id"])
            resolved[binding_id] = ref
            grouping_key = ref.endpoint_key or f"missing:{binding_id}"
            groups.setdefault(grouping_key, []).append(row)

        binding_endpoint: dict[str, str] = {}
        archived_endpoint_ids = {
            record_key
            for record_kind, record_key in rollback_archive
            if record_kind == "endpoint_snapshot"
        }
        for archive_key, snapshot in rollback_archive.items():
            if archive_key[0] != "endpoint_snapshot":
                continue
            if str(snapshot["security_domain_id"]) != domain_id:
                raise RuntimeError("rollback archive endpoint security domain mismatch")
            connection.execute(
                """
                INSERT INTO endpoints(
                  endpoint_id,endpoint_key,candidate_endpoint_key,endpoint_kind,
                  source_kind,source_json,ref_version,incarnation,
                  security_domain_id,instance_uid,workspace_id,persona_id,
                  authorized_owners_json,authorized_owners_hash,
                  policy_generation,capabilities_json,state,error_code,
                  next_lease_fence,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                tuple(
                    snapshot[key]
                    for key in (
                        "endpoint_id","endpoint_key","candidate_endpoint_key",
                        "endpoint_kind","source_kind","source_json","ref_version",
                        "incarnation","security_domain_id","instance_uid",
                        "workspace_id","persona_id","authorized_owners_json",
                        "authorized_owners_hash","policy_generation",
                        "capabilities_json","state","error_code",
                        "next_lease_fence","created_at","updated_at",
                    )
                ),
            )
            for owner_user_id in json.loads(str(snapshot["authorized_owners_json"])):
                connection.execute(
                    """
                    INSERT INTO endpoint_authorized_owners(
                      endpoint_id,security_domain_id,owner_user_id
                    ) VALUES(?,?,?)
                    """,
                    (str(snapshot["endpoint_id"]), domain_id, str(owner_user_id)),
                )
            used_archive.add(archive_key)
        for grouping_key, rows in groups.items():
            refs = [resolved[str(row["bridge_id"])] for row in rows]
            archived_group_ids: set[str] = set()
            for row, ref in zip(rows, refs, strict=True):
                identity = rollback_archive.get(
                    ("binding_identity", str(row["bridge_id"]))
                )
                if identity is None:
                    continue
                snapshot = rollback_archive.get(
                    ("endpoint_snapshot", str(identity["endpoint_id"]))
                )
                if snapshot is None:
                    raise RuntimeError("rollback archive binding endpoint is missing")
                snapshot_key = str(
                    snapshot.get("endpoint_key")
                    or snapshot.get("candidate_endpoint_key")
                    or ""
                )
                if (
                    int(row["binding_generation"]) == int(identity["generation"])
                    and str(row["endpoint_key"] or "") == snapshot_key
                ):
                    archived_group_ids.add(str(identity["endpoint_id"]))
            if len(archived_group_ids) == 1 and len(rows) == sum(
                1
                for row in rows
                if ("binding_identity", str(row["bridge_id"])) in rollback_archive
            ):
                endpoint_id = next(iter(archived_group_ids))
                for row in rows:
                    binding_endpoint[str(row["bridge_id"])] = endpoint_id
                continue
            verified_key = refs[0].endpoint_key
            equivalent = all(
                ref.ready
                and ref.endpoint_key == verified_key
                and ref.endpoint_kind == refs[0].endpoint_kind
                and ref.source_kind == refs[0].source_kind
                and ref.source_json == refs[0].source_json
                and ref.ref_version == refs[0].ref_version
                and str(row["team_id"]) == descriptor.workspace_id
                for row, ref in zip(rows, refs, strict=True)
            )
            ready = bool(verified_key and equivalent)
            candidate = verified_key or refs[0].candidate_endpoint_key
            existing_endpoint = None
            if ready:
                existing_endpoint = connection.execute(
                    "SELECT endpoint_id,source_json FROM endpoints WHERE endpoint_key=?",
                    (str(verified_key),),
                ).fetchone()
                endpoint_id = (
                    str(existing_endpoint["endpoint_id"])
                    if existing_endpoint is not None
                    else "end_" + _sha256_text(str(verified_key))[:24]
                )
                if (
                    existing_endpoint is not None
                    and str(existing_endpoint["source_json"]) != refs[0].source_json
                ):
                    raise RuntimeError("legacy endpoint conflicts with archived endpoint")
                endpoint_key = str(verified_key)
                endpoint_kind = refs[0].endpoint_kind
                source_kind = refs[0].source_kind
                source_json = refs[0].source_json
                ref_version = refs[0].ref_version
                error_code = None
                state = "ready"
            else:
                endpoint_id = "end_quarantine_" + _sha256_text(grouping_key)[:16]
                endpoint_key = None
                endpoint_kind = (
                    refs[0].endpoint_kind
                    if all(ref.endpoint_kind == refs[0].endpoint_kind for ref in refs)
                    else "unknown"
                )
                source_kind = "quarantined_legacy"
                source_json = "{}"
                ref_version = 1
                error_code = "legacy_endpoint_conflict"
                state = "rebind_required"
            if existing_endpoint is None:
                connection.execute(
                    """
                    INSERT INTO endpoints(
                      endpoint_id,endpoint_key,candidate_endpoint_key,endpoint_kind,
                      source_kind,source_json,ref_version,incarnation,
                      security_domain_id,instance_uid,workspace_id,persona_id,
                      authorized_owners_json,authorized_owners_hash,
                      policy_generation,state,error_code,
                      created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        endpoint_id,
                        endpoint_key,
                        candidate,
                        endpoint_kind,
                        source_kind,
                        source_json,
                        ref_version,
                        domain_id,
                        descriptor.instance_uid,
                        descriptor.workspace_id,
                        descriptor.persona_id,
                        json.dumps(
                            descriptor.canonical_owner_ids,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        descriptor.authorized_owners_hash,
                        descriptor.policy_generation,
                        state,
                        error_code,
                        min(str(row["created_at"]) for row in rows),
                        max(str(row["updated_at"]) for row in rows),
                    ),
                )
                for owner_user_id in descriptor.canonical_owner_ids:
                    connection.execute(
                        """
                        INSERT INTO endpoint_authorized_owners(
                          endpoint_id,security_domain_id,owner_user_id
                        ) VALUES(?,?,?)
                        """,
                        (endpoint_id, domain_id, owner_user_id),
                    )
            for row in rows:
                binding_endpoint[str(row["bridge_id"])] = endpoint_id
        for archive_key, snapshot in rollback_archive.items():
            if archive_key[0] != "endpoint_snapshot":
                continue
            endpoint_id = str(snapshot["endpoint_id"])
            if str(snapshot["security_domain_id"]) != domain_id:
                raise RuntimeError("rollback archive endpoint security domain mismatch")
            existing = connection.execute(
                "SELECT endpoint_key,incarnation FROM endpoints WHERE endpoint_id=?",
                (endpoint_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["endpoint_key"] != snapshot["endpoint_key"]
                ):
                    raise RuntimeError("rollback archive endpoint identity conflict")
                connection.execute(
                    """
                    UPDATE endpoints SET candidate_endpoint_key=?,endpoint_kind=?,
                      source_kind=?,source_json=?,ref_version=?,incarnation=?,
                      capabilities_json=?,state=?,error_code=?,next_lease_fence=?,
                      created_at=?,updated_at=? WHERE endpoint_id=?
                    """,
                    tuple(
                        snapshot[key]
                        for key in (
                            "candidate_endpoint_key","endpoint_kind","source_kind",
                            "source_json","ref_version","incarnation",
                            "capabilities_json","state","error_code",
                            "next_lease_fence","created_at","updated_at",
                            "endpoint_id",
                        )
                    ),
                )
            else:
                connection.execute(
                    """
                    INSERT INTO endpoints(
                      endpoint_id,endpoint_key,candidate_endpoint_key,endpoint_kind,
                      source_kind,source_json,ref_version,incarnation,
                      security_domain_id,instance_uid,workspace_id,persona_id,
                      authorized_owners_json,authorized_owners_hash,
                      policy_generation,capabilities_json,state,error_code,
                      next_lease_fence,created_at,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    tuple(
                        snapshot[key]
                        for key in (
                            "endpoint_id","endpoint_key","candidate_endpoint_key",
                            "endpoint_kind","source_kind","source_json","ref_version",
                            "incarnation","security_domain_id","instance_uid",
                            "workspace_id","persona_id","authorized_owners_json",
                            "authorized_owners_hash","policy_generation",
                            "capabilities_json","state","error_code",
                            "next_lease_fence","created_at","updated_at",
                        )
                    ),
                )
                for owner_user_id in json.loads(str(snapshot["authorized_owners_json"])):
                    connection.execute(
                        """
                        INSERT INTO endpoint_authorized_owners(
                          endpoint_id,security_domain_id,owner_user_id
                        ) VALUES(?,?,?)
                        """,
                        (endpoint_id, domain_id, str(owner_user_id)),
                    )
            used_archive.add(archive_key)
        if fault_inject is not None:
            fault_inject("after_endpoints")

        for row in legacy_bindings:
            binding_id = str(row["bridge_id"])
            ref = resolved[binding_id]
            binding_identity_key = ("binding_identity", binding_id)
            archived_binding_identity = rollback_archive.get(binding_identity_key)
            restore_archived_endpoint = False
            if archived_binding_identity is not None:
                archived_snapshot = rollback_archive.get(
                    (
                        "endpoint_snapshot",
                        str(archived_binding_identity["endpoint_id"]),
                    )
                )
                if archived_snapshot is None:
                    raise RuntimeError("rollback archive binding endpoint is missing")
                archived_key = str(
                    archived_snapshot.get("endpoint_key")
                    or archived_snapshot.get("candidate_endpoint_key")
                    or ""
                )
                restore_archived_endpoint = (
                    int(row["binding_generation"])
                    == int(archived_binding_identity["generation"])
                    and str(row["endpoint_key"] or "") == archived_key
                )
                if not restore_archived_endpoint:
                    archive_binding_changed = True
            elif rollback_archive:
                archive_binding_changed = True
            endpoint_id = (
                str(archived_binding_identity["endpoint_id"])
                if restore_archived_endpoint
                else binding_endpoint[binding_id]
            )
            endpoint = connection.execute(
                "SELECT state FROM endpoints WHERE endpoint_id=?",
                (endpoint_id,),
            ).fetchone()
            if endpoint is None:
                raise RuntimeError("rollback archive binding endpoint is missing")
            legacy_state = str(row["status"])
            if legacy_state == "closed":
                state = "closed"
            elif (
                endpoint["state"] == "ready"
                and legacy_state == "active"
                and str(row["binding_state"]) == "verified"
                and row["thread_ts"] is not None
                and str(row["team_id"])
            ):
                state = "active"
            elif (
                endpoint["state"] == "ready"
                and legacy_state == "pending"
                and row["thread_ts"] is None
            ):
                state = "pending_root"
            else:
                state = "rebind_required"
            connection.execute(
                """
                INSERT INTO thread_bindings(
                  binding_id,endpoint_id,security_domain_id,team_id,channel_id,
                  thread_ts,owner_user_id,idempotency_key,request_hash,
                  generation,state,thread_claim_generation,error_code,
                  created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    binding_id,
                    endpoint_id,
                    domain_id,
                    str(row["team_id"]),
                    str(row["channel_id"]),
                    row["thread_ts"],
                    str(row["owner_user_id"]),
                    str(row["idempotency_key"]),
                    str(archived_binding_identity["request_hash"])
                    if restore_archived_endpoint
                    else _request_hash(row, endpoint_id, domain_id),
                    int(row["binding_generation"]),
                    state,
                    row["thread_claim_generation"],
                    (ref.error_code if state == "rebind_required" else None),
                    str(row["created_at"]),
                    str(row["updated_at"]),
                ),
            )
            connection.execute(
                """
                INSERT INTO legacy_binding_sources(
                  binding_id,binding_generation,source_kind,source_json,ref_version
                ) VALUES(?,?,?,?,?)
                """,
                (
                    binding_id,
                    int(row["binding_generation"]),
                    str(row["source_kind"]),
                    str(row["source_json"]),
                    int(row["binding_version"]),
                ),
            )
            if archived_binding_identity is not None:
                used_archive.add(binding_identity_key)
        if rollback_archive:
            for endpoint_id in sorted(set(binding_endpoint.values()) - archived_endpoint_ids):
                used = connection.execute(
                    "SELECT 1 FROM thread_bindings WHERE endpoint_id=? LIMIT 1",
                    (endpoint_id,),
                ).fetchone()
                if used is None:
                    connection.execute(
                        "DELETE FROM endpoint_authorized_owners WHERE endpoint_id=?",
                        (endpoint_id,),
                    )
                    connection.execute(
                        "DELETE FROM endpoints WHERE endpoint_id=?",
                        (endpoint_id,),
                    )
        if fault_inject is not None:
            fault_inject("after_bindings")

        legacy_events = connection.execute(
            "SELECT * FROM bridge_events ORDER BY created_at,event_id"
        ).fetchall()
        for row in legacy_events:
            payload = json.loads(str(row["payload_json"]))
            text = payload.get("text")
            if not isinstance(text, str) or not text:
                raise RuntimeError("legacy event payload is missing text")
            attempt_target = None
            if row["attempt_id"] is not None:
                legacy_attempt = connection.execute(
                    """
                    SELECT attempt.*,reply.payload_text AS response_payload
                    FROM bridge_attempts AS attempt
                    LEFT JOIN bridge_replies AS reply
                      ON reply.reply_key=attempt.attempt_id
                     AND reply.bridge_id=attempt.bridge_id
                    WHERE attempt.attempt_id=? AND attempt.bridge_id=?
                    """,
                    (str(row["attempt_id"]), str(row["bridge_id"])),
                ).fetchone()
                attempt_target = _legacy_attempt_target(
                    legacy_attempt,
                    legacy_attempt["response_payload"] is not None,
                )
            target_state = _legacy_turn_target(
                str(row["state"]), attempt_target
            )
            archived_turn = rollback_archive.get(
                ("turn_origin", str(row["event_id"]))
            )
            archived_turn_mutated = bool(
                archived_turn is not None
                and _archived_turn_was_mutated(connection, str(row["event_id"]))
            )
            terminal_at = (
                str(archived_turn["terminal_at"])
                if target_state != "ready"
                and archived_turn is not None
                and archived_turn["terminal_at"] is not None
                else str(row["updated_at"])
                if target_state != "ready"
                else None
            )
            binding_generation = (
                int(rollback_archive[("turn_origin", str(row["event_id"]))][
                    "binding_generation"
                ])
                if ("turn_origin", str(row["event_id"])) in rollback_archive
                and not archived_turn_mutated
                else int(row["binding_generation"])
                if row["binding_generation"] is not None
                else int(
                    connection.execute(
                        "SELECT generation FROM thread_bindings WHERE binding_id=?",
                        (str(row["bridge_id"]),),
                    ).fetchone()[0]
                )
            )
            turn_origin_key = ("turn_origin", str(row["event_id"]))
            if turn_origin_key in rollback_archive:
                used_archive.add(turn_origin_key)
            connection.execute(
                """
                INSERT INTO queued_turns(
                  event_key,binding_id,binding_generation,ordered_at,
                  mutation_kind,payload_inline,payload_sha256,payload_bytes,
                  state,terminal_at,error_code,created_at,updated_at
                ) VALUES(?,?,?,?,'create',?,?,?,?,?,?,?,?)
                """,
                (
                    str(row["event_id"]),
                    str(row["bridge_id"]),
                    binding_generation,
                    str(rollback_archive[turn_origin_key]["ordered_at"])
                    if turn_origin_key in rollback_archive
                    else str(row["created_at"]),
                    text,
                    _sha256_text(text),
                    len(text.encode()),
                    target_state,
                    terminal_at,
                    str(row["error"] or "") or None,
                    str(rollback_archive[turn_origin_key]["created_at"])
                    if turn_origin_key in rollback_archive
                    else str(row["created_at"]),
                    str(archived_turn["updated_at"])
                    if archived_turn is not None and not archived_turn_mutated
                    else str(row["updated_at"]),
                ),
            )
            if terminal_at is not None:
                source_digest = _sha256_text(
                    json.dumps(
                        {
                            "event_id": str(row["event_id"]),
                            "payload_sha256": _sha256_text(text),
                            "state": str(row["state"]),
                            "error": str(row["error"] or ""),
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
                connection.execute(
                    """
                    INSERT INTO legacy_terminal_imports(
                      event_key,source_schema,source_digest
                    ) VALUES(?,17,?)
                    """,
                    (str(row["event_id"]), source_digest),
                )
        if fault_inject is not None:
            fault_inject("after_turns")

        attempts = list(
            connection.execute(
                "SELECT * FROM bridge_attempts ORDER BY created_at,attempt_id"
            ).fetchall()
        )
        def migration_attempt_sort_key(attempt: sqlite3.Row) -> tuple[Any, ...]:
            attempt_id = str(attempt["attempt_id"])
            identity = rollback_archive.get(("attempt_identity", attempt_id))
            if identity is not None and not _archived_attempt_is_current(
                connection, identity
            ):
                identity = None
            endpoint_id = (
                str(identity["endpoint_id"])
                if identity is not None
                else binding_endpoint[str(attempt["bridge_id"])]
            )
            response_exists = connection.execute(
                """
                SELECT 1 FROM bridge_replies
                WHERE reply_key=? AND bridge_id=? AND payload_text IS NOT NULL
                """,
                (attempt_id, str(attempt["bridge_id"])),
            ).fetchone() is not None
            target = _legacy_attempt_target(attempt, response_exists)
            terminal = target in {
                "completed_with_response",
                "no_reply",
                "cancelled",
                "failed_before_start",
                "failed",
                "operator_completed",
                "operator_abandoned",
            }
            # Failed-before-start attempts are the only history that may
            # precede another membership for the same turn. Import them
            # before the eventual execution terminal; keep the one open
            # endpoint attempt last so it receives the highest fence.
            history_rank = 0 if target == "failed_before_start" else 1 if terminal else 2
            return (
                history_rank,
                int(
                    identity["binding_generation"]
                    if identity is not None
                    else attempt["binding_generation"]
                ),
                int(identity["fence"] if identity is not None else 0),
                str(attempt["created_at"]),
                endpoint_id,
                attempt_id,
            )

        attempts.sort(key=migration_attempt_sort_key)
        next_fence: dict[str, int] = {
            str(payload["endpoint_id"]): int(payload["next_lease_fence"])
            for (kind, _key), payload in rollback_archive.items()
            if kind == "endpoint_snapshot"
        }
        open_endpoint_attempt: dict[str, str] = {}
        for row in attempts:
            attempt_id = str(row["attempt_id"])
            binding_id = str(row["bridge_id"])
            identity_key = ("attempt_identity", attempt_id)
            archived_identity = rollback_archive.get(identity_key)
            if archived_identity is not None and not _archived_attempt_is_current(
                connection, archived_identity
            ):
                for archive_key, payload in rollback_archive.items():
                    if archive_key == identity_key or str(
                        payload.get("attempt_id", "")
                    ) == attempt_id:
                        used_archive.add(archive_key)
                archived_identity = None
            endpoint_id = (
                str(archived_identity["endpoint_id"])
                if archived_identity is not None
                else binding_endpoint[binding_id]
            )
            fence = (
                int(archived_identity["fence"])
                if archived_identity is not None
                else next_fence.get(endpoint_id, 0) + 1
            )
            next_fence[endpoint_id] = max(next_fence.get(endpoint_id, 0), fence)
            reply = connection.execute(
                """
                SELECT payload_text,text_hash FROM bridge_replies
                WHERE reply_key=? AND bridge_id=?
                """,
                (attempt_id, binding_id),
            ).fetchone()
            response = str(reply["payload_text"]) if reply and reply["payload_text"] is not None else None
            response_hash = _sha256_text(response) if response is not None else None
            response_bytes = len(response.encode()) if response is not None else None
            target_state = _legacy_attempt_target(row, response is not None)
            terminal = target_state in {
                "completed_with_response",
                "no_reply",
                "cancelled",
                "failed_before_start",
                "failed",
                "operator_completed",
                "operator_abandoned",
            }
            if not terminal:
                previous = open_endpoint_attempt.get(endpoint_id)
                if previous is not None:
                    raise RuntimeError(
                        "legacy database has multiple potentially-started attempts "
                        "for one endpoint"
                    )
                open_endpoint_attempt[endpoint_id] = attempt_id
            terminal_at = (
                str(archived_identity["terminal_at"])
                if terminal
                and archived_identity is not None
                and archived_identity["terminal_at"] is not None
                else str(row["updated_at"])
                if terminal
                else None
            )
            restored_updated_at = (
                str(archived_identity["updated_at"])
                if archived_identity is not None
                else str(row["updated_at"])
            )
            release_reason = f"legacy_import_{target_state}" if terminal else None
            if archived_identity is None:
                connection.execute(
                    "UPDATE endpoints SET next_lease_fence=? WHERE endpoint_id=?",
                    (fence, endpoint_id),
                )
            else:
                used_archive.add(identity_key)
            connection.execute(
                """
                INSERT INTO endpoint_leases(
                  attempt_id,endpoint_id,endpoint_incarnation,fence,acquired_at,
                  expires_at,released_at,release_reason
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    attempt_id,
                    endpoint_id,
                    int(archived_identity["endpoint_incarnation"])
                    if archived_identity is not None
                    else 1,
                    fence,
                    str(archived_identity["acquired_at"])
                    if archived_identity is not None
                    else str(row["created_at"]),
                    str(archived_identity["expires_at"])
                    if archived_identity is not None
                    else str(
                        connection.execute(
                            "SELECT datetime(?,'+30 minutes')",
                            (str(row["updated_at"]),),
                        ).fetchone()[0]
                    ),
                    str(archived_identity["released_at"])
                    if terminal and archived_identity is not None
                    else terminal_at
                    if terminal
                    else None,
                    str(archived_identity["release_reason"])
                    if terminal and archived_identity is not None
                    else release_reason
                    if terminal
                    else None,
                ),
            )
            connection.execute(
                """
                INSERT INTO native_attempts(
                  attempt_id,endpoint_id,binding_id,binding_generation,
                  driver_kind,driver_request_id,driver_request_hash,
                  reply_token_hash,state,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,'prepared',?,?)
                """,
                (
                    attempt_id,
                    endpoint_id,
                    binding_id,
                    int(row["binding_generation"]),
                    str(archived_identity["driver_kind"])
                    if archived_identity is not None
                    else str(row["delivery_kind"]),
                    str(archived_identity["driver_request_id"])
                    if archived_identity is not None
                    else f"legacy-submit:{attempt_id}",
                    str(archived_identity["driver_request_hash"])
                    if archived_identity is not None
                    else _sha256_text(f"legacy-submit:{attempt_id}"),
                    str(archived_identity["reply_token_hash"])
                    if archived_identity is not None
                    else _sha256_text(secrets.token_hex(32)),
                    str(archived_identity["created_at"])
                    if archived_identity is not None
                    else str(row["created_at"]),
                    str(archived_identity["created_at"])
                    if archived_identity is not None
                    else str(row["created_at"]),
                ),
            )
            if archived_identity is not None and archived_identity["cancel_request_id"] is not None:
                connection.execute(
                    """
                    UPDATE native_attempts SET cancel_request_id=?,cancel_request_hash=?
                    WHERE attempt_id=?
                    """,
                    (
                        str(archived_identity["cancel_request_id"]),
                        str(archived_identity["cancel_request_hash"]),
                        attempt_id,
                    ),
                )
            members_by_event: dict[str, Mapping[str, Any]] = {
                str(member["event_id"]): member
                for member in connection.execute(
                    """
                SELECT event.event_id,turn.binding_generation,
                       ROW_NUMBER() OVER (
                         ORDER BY event.created_at,event.event_id
                       )-1 AS ordinal
                FROM bridge_events AS event
                JOIN queued_turns AS turn ON turn.event_key=event.event_id
                WHERE event.attempt_id=? AND event.bridge_id=?
                ORDER BY event.created_at,event.event_id
                """,
                (attempt_id, binding_id),
                ).fetchall()
            }
            journal_exists = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type='table'
                  AND name='tether_domain_rollback_memberships'
                """
            ).fetchone()
            if journal_exists:
                for journaled in connection.execute(
                    """
                    SELECT membership.event_key,membership.binding_id,
                           membership.turn_binding_generation,
                           ROW_NUMBER() OVER (
                             ORDER BY event.created_at,event.event_id
                           )-1 AS ordinal
                    FROM tether_domain_rollback_memberships AS membership
                    JOIN bridge_events AS event
                      ON event.event_id=membership.event_key
                    WHERE membership.attempt_id=?
                    ORDER BY ordinal,membership.event_key
                    """,
                    (attempt_id,),
                ):
                    event_key = str(journaled["event_key"])
                    if str(journaled["binding_id"]) != binding_id:
                        raise RuntimeError(
                            "rollback journal attempt membership conflicts"
                        )
                    members_by_event[event_key] = {
                        "event_id": event_key,
                        "binding_generation": int(
                            journaled["turn_binding_generation"]
                        ),
                        "ordinal": int(journaled["ordinal"]),
                    }
            for archive_key, payload in rollback_archive.items():
                if archive_key[0] != "attempt_membership":
                    continue
                if archived_identity is None:
                    continue
                if str(payload.get("attempt_id")) != attempt_id:
                    continue
                event_key = str(payload["event_key"])
                current_member = members_by_event.get(event_key)
                if current_member is not None and str(payload["binding_id"]) != binding_id:
                    raise RuntimeError("rollback archive attempt membership conflicts")
                members_by_event[event_key] = {
                    "event_id": event_key,
                    "binding_generation": int(payload["turn_binding_generation"]),
                    "ordinal": int(payload["ordinal"]),
                }
                used_archive.add(archive_key)
            members = sorted(
                members_by_event.values(),
                key=lambda member: (int(member["ordinal"]), str(member["event_id"])),
            )
            if [int(member["ordinal"]) for member in members] != list(
                range(len(members))
            ):
                raise RuntimeError("rollback archive attempt membership is not dense")
            for member in members:
                member_generation = (
                    int(member["binding_generation"])
                    if member["binding_generation"] is not None
                    else int(row["binding_generation"])
                )
                if member_generation > int(row["binding_generation"]):
                    raise RuntimeError("legacy attempt batch crosses binding generations")
                connection.execute(
                    """
                    INSERT INTO native_attempt_turns(
                      attempt_id,ordinal,event_key,binding_id,
                      turn_binding_generation
                    ) VALUES(?,?,?,?,?)
                    """,
                    (
                        attempt_id,
                        int(member["ordinal"]),
                        str(member["event_id"]),
                        binding_id,
                        member_generation,
                    ),
                )
            if terminal:
                attempt_import_material = json.dumps(
                    {
                        "attempt_id": attempt_id,
                        "binding_id": binding_id,
                        "binding_generation": int(row["binding_generation"]),
                        "legacy_state": str(row["state"]),
                        "ack_kind": str(row["ack_kind"] or ""),
                        "target_state": target_state,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                connection.execute(
                    """
                    INSERT INTO legacy_attempt_imports(
                      attempt_id,source_schema,source_digest
                    ) VALUES(?,17,?)
                    """,
                    (attempt_id, _sha256_text(attempt_import_material)),
                )
            archived_receipts = sorted(
                (
                    (archive_key, payload)
                    for archive_key, payload in rollback_archive.items()
                    if archive_key[0] == "driver_receipt"
                    and archived_identity is not None
                    and str(payload.get("attempt_id")) == attempt_id
                ),
                key=lambda item: int(item[1]["sequence"]),
            )
            for archive_key, payload in archived_receipts:
                connection.execute(
                    """
                    INSERT INTO driver_receipts(
                      receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
                      driver_kind,driver_incarnation,operation,request_id,
                      request_hash,watch_cursor,state,
                      response_ref,response_sha256,error_code,observed_at,created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    tuple(
                        payload[key]
                        for key in (
                            "receipt_id","attempt_id","endpoint_id","lease_fence",
                            "sequence","driver_kind","driver_incarnation",
                            "operation","request_id","request_hash",
                            "watch_cursor","state","response_ref",
                            "response_sha256","error_code","observed_at","created_at",
                        )
                    ),
                )
                connection.execute(
                    """
                    UPDATE native_attempts SET receipt_cursor=?,
                      last_driver_receipt_id=?,last_driver_sequence=?
                    WHERE attempt_id=?
                    """,
                    (
                        str(payload["watch_cursor"]),
                        str(payload["receipt_id"]),
                        int(payload["sequence"]),
                        attempt_id,
                    ),
                )
                used_archive.add(archive_key)
            error_code = str(row["error_code"] or "") or None
            submitted_history = (
                str(archived_identity["submitted_at"])
                if archived_identity is not None
                and archived_identity["submitted_at"] is not None
                else str(row["submitted_at"])
                if row["submitted_at"] is not None
                else None
            )
            if (
                target_state in {"failed_before_start", "cancelled"}
                and submitted_history is not None
            ):
                connection.execute(
                    """
                    UPDATE native_attempts
                    SET state='submitting',submitted_at=?,updated_at=?
                    WHERE attempt_id=?
                    """,
                    (
                        submitted_history,
                        restored_updated_at,
                        attempt_id,
                    ),
                )
                if (
                    target_state == "cancelled"
                    and archived_identity is not None
                    and archived_identity["accepted_at"] is not None
                ):
                    connection.execute(
                        """
                        UPDATE native_attempts
                        SET state='accepted',accepted_at=?,updated_at=?
                        WHERE attempt_id=?
                        """,
                        (
                            str(archived_identity["accepted_at"]),
                            restored_updated_at,
                            attempt_id,
                        ),
                    )
                connection.execute(
                    """
                    UPDATE native_attempts
                    SET state=?,error_code=?,terminal_at=?,updated_at=?
                    WHERE attempt_id=?
                    """,
                    (
                        target_state,
                        error_code,
                        terminal_at,
                        restored_updated_at,
                        attempt_id,
                    ),
                )
            elif target_state in {"failed_before_start", "cancelled"}:
                connection.execute(
                    """
                    UPDATE native_attempts
                    SET state=?,error_code=?,terminal_at=?,updated_at=?
                    WHERE attempt_id=?
                    """,
                    (
                        target_state,
                        error_code,
                        terminal_at,
                        restored_updated_at,
                        attempt_id,
                    ),
                )
            else:
                submitted_at = (
                    str(archived_identity["submitted_at"])
                    if archived_identity is not None
                    and archived_identity["submitted_at"] is not None
                    else str(row["submitted_at"] or row["updated_at"])
                )
                connection.execute(
                    """
                    UPDATE native_attempts
                    SET state='submitting',submitted_at=?,updated_at=?
                    WHERE attempt_id=?
                    """,
                    (submitted_at, restored_updated_at, attempt_id),
                )
                if (
                    archived_identity is not None
                    and archived_identity["accepted_at"] is not None
                    and target_state
                    in {"uncertain", "failed", "operator_completed", "operator_abandoned"}
                ):
                    connection.execute(
                        """
                        UPDATE native_attempts
                        SET state='accepted',accepted_at=?,updated_at=?
                        WHERE attempt_id=?
                        """,
                        (
                            str(archived_identity["accepted_at"]),
                            restored_updated_at,
                            attempt_id,
                        ),
                    )
                if target_state == "uncertain":
                    connection.execute(
                        """
                        UPDATE native_attempts SET state='uncertain',error_code=?,
                          updated_at=? WHERE attempt_id=?
                        """,
                        (error_code, restored_updated_at, attempt_id),
                    )
                elif target_state == "failed":
                    connection.execute(
                        """
                        UPDATE native_attempts SET state='failed',error_code=?,
                          terminal_at=?,updated_at=? WHERE attempt_id=?
                        """,
                        (
                            error_code,
                            terminal_at,
                            restored_updated_at,
                            attempt_id,
                        ),
                    )
                elif target_state in {"operator_completed", "operator_abandoned"}:
                    connection.execute(
                        """
                        UPDATE native_attempts SET state='uncertain',error_code=?,
                          updated_at=? WHERE attempt_id=?
                        """,
                        (error_code, restored_updated_at, attempt_id),
                    )
                    archive_key = ("operator_resolution", attempt_id)
                    archived_resolution = rollback_archive.get(archive_key)
                    if archived_resolution is None:
                        resolution_material = json.dumps(
                            {
                                "attempt_id": attempt_id,
                                "legacy_state": str(row["state"]),
                                "ack_kind": str(row["ack_kind"] or ""),
                                "error_code": error_code,
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        resolution_values = (
                            "legacy_import",
                            f"legacy-resolution:{attempt_id}",
                            _sha256_text("legacy-schema17-migration"),
                            f"legacy-schema17:{attempt_id}",
                            _sha256_text(resolution_material),
                            terminal_at,
                            terminal_at,
                        )
                    else:
                        expected_action = (
                            "complete"
                            if target_state == "operator_completed"
                            else "abandon"
                        )
                        if str(archived_resolution.get("action")) != expected_action:
                            raise RuntimeError("rollback archive operator action mismatch")
                        resolution_values = (
                            "authority",
                            str(archived_resolution["authority_receipt_id"]),
                            str(archived_resolution["operator_principal_hash"]),
                            str(archived_resolution["evidence_ref"]),
                            str(archived_resolution["evidence_sha256"]),
                            str(archived_resolution["resolved_at"]),
                            str(archived_resolution["created_at"]),
                        )
                        used_archive.add(archive_key)
                    connection.execute(
                        """
                        INSERT INTO operator_resolutions(
                          attempt_id,endpoint_id,lease_fence,action,source_kind,
                          authority_receipt_id,operator_principal_hash,
                          evidence_ref,evidence_sha256,resolved_at,created_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            attempt_id,
                            endpoint_id,
                            fence,
                            "complete"
                            if target_state == "operator_completed"
                            else "abandon",
                            *resolution_values,
                        ),
                    )
                    connection.execute(
                        """
                        UPDATE native_attempts SET state=?,terminal_at=?,updated_at=?
                        WHERE attempt_id=?
                        """,
                        (
                            target_state,
                            terminal_at,
                            restored_updated_at,
                            attempt_id,
                        ),
                    )
                else:
                    accepted_at = (
                        str(archived_identity["accepted_at"])
                        if archived_identity is not None
                        and archived_identity["accepted_at"] is not None
                        else str(row["submitted_at"])
                        if row["submitted_at"] is not None
                        else str(row["updated_at"])
                    )
                    connection.execute(
                        """
                        UPDATE native_attempts SET state='accepted',accepted_at=?,
                          updated_at=? WHERE attempt_id=?
                        """,
                        (accepted_at, restored_updated_at, attempt_id),
                    )
                    connection.execute(
                        """
                        UPDATE native_attempts
                        SET state=?,response_inline=?,response_sha256=?,
                          response_bytes=?,error_code=?,terminal_at=?,updated_at=?
                        WHERE attempt_id=?
                        """,
                        (
                            target_state,
                            response
                            if target_state == "completed_with_response"
                            else None,
                            response_hash
                            if target_state == "completed_with_response"
                            else None,
                            response_bytes
                            if target_state == "completed_with_response"
                            else None,
                            error_code,
                            terminal_at,
                            restored_updated_at,
                            attempt_id,
                        ),
                    )
            egress_archive_key = ("egress_receipt", attempt_id)
            archived_egress = rollback_archive.get(egress_archive_key)
            if archived_egress is not None and archived_identity is not None:
                connection.execute(
                    """
                    UPDATE native_attempts SET hermes_egress_receipt_id=?
                    WHERE attempt_id=?
                    """,
                    (str(archived_egress["receipt_id"]), attempt_id),
                )
                used_archive.add(egress_archive_key)
        if fault_inject is not None:
            fault_inject("after_attempts")

        if used_archive != set(rollback_archive):
            raise RuntimeError("rollback archive contains unclaimed records")
        if rollback_archive:
            _remove_rollback_horizon_guards(connection)
            connection.execute("DROP TABLE tether_domain_rollback_activity")
            connection.execute("DROP TABLE tether_domain_rollback_memberships")
            connection.execute("DROP TABLE tether_domain_rollback_archive")

        require_valid(connection)
        preserved_after = logical_manifest_v18(connection)
        changed = [
            key
            for key in PRESERVED_MANIFEST_KEYS
            if preserved_before[key] != preserved_after[key]
        ]
        if (
            not archive_binding_changed
            and
            preserved_before["endpoint_inventory"] is not None
            and preserved_before["endpoint_inventory"]
            != preserved_after["endpoint_inventory"]
        ):
            changed.append("endpoint_inventory")
        if changed:
            raise RuntimeError(
                "schema-18 migration changed preserved logical records: "
                + ", ".join(changed)
            )
        if fault_inject is not None:
            fault_inject("after_validation")
        connection.execute("DROP TABLE bridge_attempts")
        connection.execute("DROP TABLE bridge_events")
        connection.execute("DROP TABLE bridges")
        if fault_inject is not None:
            fault_inject("after_legacy_drop")
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        if fault_inject is not None:
            fault_inject("after_version")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


LEGACY_AUTHORITY_DDL: tuple[str, ...] = (
    """
    CREATE TABLE bridges (
      bridge_id TEXT PRIMARY KEY, source_kind TEXT NOT NULL,
      source_json TEXT NOT NULL, owner_user_id TEXT NOT NULL,
      team_id TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL,
      thread_ts TEXT, idempotency_key TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL DEFAULT 'pending',
      binding_version INTEGER NOT NULL DEFAULT 1,
      binding_generation INTEGER NOT NULL DEFAULT 1,
      binding_state TEXT NOT NULL DEFAULT 'legacy',
      binding_error_code TEXT,
      endpoint_key TEXT NOT NULL DEFAULT '',
      thread_claim_generation INTEGER,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE UNIQUE INDEX bridge_thread ON bridges(team_id,channel_id,thread_ts)
    WHERE thread_ts IS NOT NULL AND status='active'
    """,
    """
    CREATE INDEX bridge_endpoint_lookup ON bridges(endpoint_key)
    WHERE endpoint_key!='' AND status IN ('pending','active')
    """,
    """
    CREATE TABLE bridge_events (
      event_id TEXT PRIMARY KEY, bridge_id TEXT NOT NULL,
      state TEXT NOT NULL, error TEXT,
      payload_json TEXT NOT NULL DEFAULT '{}',
      attempt_id TEXT, binding_generation INTEGER,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE INDEX bridge_event_attempt ON bridge_events(attempt_id)
    WHERE attempt_id IS NOT NULL
    """,
    """
    CREATE TABLE bridge_attempts (
      attempt_id TEXT PRIMARY KEY,
      reply_key TEXT NOT NULL UNIQUE,
      bridge_id TEXT NOT NULL,
      binding_generation INTEGER NOT NULL,
      delivery_kind TEXT NOT NULL,
      state TEXT NOT NULL,
      ack_kind TEXT,
      message_ts TEXT,
      error_code TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      submitted_at TEXT,
      acknowledged_at TEXT
    )
    """,
    """
    CREATE UNIQUE INDEX bridge_one_open_attempt ON bridge_attempts(bridge_id)
    WHERE state IN ('prepared','submitting','uncertain','awaiting_ack','replying')
    """,
)


def rollback_v18_to_v17(
    connection: sqlite3.Connection,
    fault_inject: Callable[[str], None] | None = None,
    *,
    legacy_source_validator: Callable[[str, str, int], None] | None = None,
) -> None:
    """Losslessly project the v18 subset supported during the L1 rollout."""
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("BEGIN IMMEDIATE")
    try:
        if int(connection.execute("PRAGMA user_version").fetchone()[0]) != 18:
            raise RuntimeError("Tether rollback requires schema 18")
        require_valid(connection)
        preserved_before = logical_manifest_v18(connection)
        projected_sources = connection.execute(
            """
            SELECT binding.binding_id,
                   CASE WHEN provenance.binding_generation=binding.generation
                        THEN provenance.source_kind ELSE endpoint.source_kind END,
                   CASE WHEN provenance.binding_generation=binding.generation
                        THEN provenance.source_json ELSE endpoint.source_json END,
                   CASE WHEN provenance.binding_generation=binding.generation
                        THEN provenance.ref_version ELSE endpoint.ref_version END
            FROM thread_bindings AS binding
            JOIN endpoints AS endpoint ON endpoint.endpoint_id=binding.endpoint_id
            LEFT JOIN legacy_binding_sources AS provenance
              ON provenance.binding_id=binding.binding_id
            ORDER BY binding.binding_id
            """
        ).fetchall()
        if projected_sources and legacy_source_validator is None:
            raise RuntimeError("schema-17 rollback requires its pinned source validator")
        for row in projected_sources:
            try:
                legacy_source_validator(str(row[1]), str(row[2]), int(row[3]))  # type: ignore[misc]
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"rollback_source_incompatible:{row['binding_id']}"
                ) from exc
        if fault_inject is not None:
            fault_inject("after_preflight")
        if connection.execute(
            "SELECT 1 FROM endpoint_leases WHERE released_at IS NULL LIMIT 1"
        ).fetchone():
            raise RuntimeError("cannot roll back with an open endpoint lease")
        if connection.execute(
            """
            SELECT 1 FROM queued_turns
            WHERE mutation_kind!='create' OR payload_ref IS NOT NULL
            LIMIT 1
            """
        ).fetchone():
            raise RuntimeError("cannot roll back non-projectable turn content")
        if connection.execute(
            """
            SELECT 1 FROM native_attempts
            WHERE response_ref IS NOT NULL
            LIMIT 1
            """
        ).fetchone():
            raise RuntimeError("cannot roll back non-materialized response delivery")
        if connection.execute(
            """
            SELECT 1 FROM native_attempts AS attempt
            WHERE attempt.hermes_egress_receipt_id IS NOT NULL
              AND NOT EXISTS(
                SELECT 1 FROM bridge_replies AS reply
                WHERE reply.reply_key=attempt.attempt_id
                  AND reply.bridge_id=attempt.binding_id
                  AND reply.message_ts IS NOT NULL
              )
            LIMIT 1
            """
        ).fetchone():
            raise RuntimeError("cannot roll back an unreconciled Hermes egress receipt")
        if connection.execute(
            "SELECT 1 FROM thread_bindings WHERE state='pending_root' LIMIT 1"
        ).fetchone():
            raise RuntimeError("cannot roll back a pending root binding")
        if connection.execute(
            """
            SELECT 1 FROM bridge_replies
            WHERE message_ts IS NULL AND state IN ('delivering','uncertain')
            LIMIT 1
            """
        ).fetchone():
            raise RuntimeError("cannot roll back ambiguous Slack reply delivery")

        archive_records: list[tuple[str, str, dict[str, Any]]] = []
        for row in connection.execute(
            """
            SELECT binding.binding_id,binding.endpoint_id,binding.request_hash,
                   binding.generation,
                   COALESCE(endpoint.endpoint_key,endpoint.candidate_endpoint_key,'')
                     AS endpoint_key
            FROM thread_bindings AS binding
            JOIN endpoints AS endpoint ON endpoint.endpoint_id=binding.endpoint_id
            ORDER BY binding.binding_id
            """
        ):
            archive_records.append(
                (
                    "binding_identity",
                    str(row["binding_id"]),
                    {
                        "binding_id": str(row["binding_id"]),
                        "endpoint_id": str(row["endpoint_id"]),
                        "request_hash": str(row["request_hash"]),
                        "generation": int(row["generation"]),
                        "endpoint_key": str(row["endpoint_key"]),
                    },
                )
            )
        for row in connection.execute(
            """
            SELECT event_key,binding_generation,ordered_at,created_at,
                   terminal_at,updated_at
            FROM queued_turns ORDER BY event_key
            """
        ):
            archive_records.append(
                (
                    "turn_origin",
                    str(row["event_key"]),
                    {
                        "event_key": str(row["event_key"]),
                        "binding_generation": int(row["binding_generation"]),
                        "ordered_at": str(row["ordered_at"]),
                        "created_at": str(row["created_at"]),
                        "terminal_at": row["terminal_at"],
                        "updated_at": str(row["updated_at"]),
                    },
                )
            )
        archived_endpoint_ids = {
            str(row[0])
            for row in connection.execute(
                "SELECT endpoint_id FROM endpoints"
            )
        }
        for endpoint_id in sorted(archived_endpoint_ids):
            row = connection.execute(
                "SELECT * FROM endpoints WHERE endpoint_id=?",
                (endpoint_id,),
            ).fetchone()
            archive_records.append(
                (
                    "endpoint_snapshot",
                    endpoint_id,
                    {key: row[key] for key in row.keys()},
                )
            )
        for row in connection.execute(
            """
            SELECT attempt.attempt_id,attempt.endpoint_id,attempt.binding_id,
                   attempt.binding_generation,attempt.driver_kind,
                   attempt.driver_request_id,attempt.driver_request_hash,
                   attempt.cancel_request_id,attempt.cancel_request_hash,
                   attempt.reply_token_hash,attempt.receipt_cursor,
                   attempt.last_driver_receipt_id,attempt.last_driver_sequence,
                   attempt.created_at,attempt.submitted_at,attempt.accepted_at,
                   attempt.terminal_at,attempt.updated_at,attempt.state,
                   attempt.response_sha256,
                   lease.endpoint_incarnation,lease.fence,
                   lease.acquired_at,lease.expires_at,lease.released_at,
                   lease.release_reason
            FROM native_attempts AS attempt
            JOIN endpoint_leases AS lease ON lease.attempt_id=attempt.attempt_id
            ORDER BY attempt.attempt_id
            """
        ):
            delivered = bool(
                connection.execute(
                    """
                    SELECT 1 FROM bridge_replies
                    WHERE reply_key=? AND bridge_id=? AND message_ts IS NOT NULL
                    """,
                    (str(row["attempt_id"]), str(row["binding_id"])),
                ).fetchone()
            )
            projected_state, projected_ack = _legacy_projection_for_v18(
                str(row["state"]), response_delivered=delivered
            )
            payload = {key: row[key] for key in row.keys()}
            payload["projected_legacy_state"] = projected_state
            payload["projected_ack_kind"] = projected_ack
            archive_records.append(
                (
                    "attempt_identity",
                    str(row["attempt_id"]),
                    payload,
                )
            )
        for row in connection.execute(
            """
            SELECT membership.attempt_id,membership.ordinal,
                   membership.event_key,membership.binding_id,
                   membership.turn_binding_generation,
                   attempt.binding_generation AS attempt_binding_generation
            FROM native_attempt_turns AS membership
            JOIN native_attempts AS attempt
              ON attempt.attempt_id=membership.attempt_id
            ORDER BY membership.attempt_id,membership.ordinal,membership.event_key
            """
        ):
            key = f"{row['attempt_id']}:{row['event_key']}"
            archive_records.append(
                (
                    "attempt_membership",
                    key,
                    {
                        "attempt_id": str(row["attempt_id"]),
                        "ordinal": int(row["ordinal"]),
                        "event_key": str(row["event_key"]),
                        "binding_id": str(row["binding_id"]),
                        "turn_binding_generation": int(row["turn_binding_generation"]),
                        "attempt_binding_generation": int(
                            row["attempt_binding_generation"]
                        ),
                    },
                )
            )
        for row in connection.execute(
            """
            SELECT receipt_id,attempt_id,endpoint_id,lease_fence,sequence,
                   driver_kind,driver_incarnation,operation,request_id,
                   request_hash,watch_cursor,state,response_ref,
                   response_sha256,error_code,observed_at,created_at
            FROM driver_receipts ORDER BY receipt_id
            """
        ):
            archive_records.append(
                (
                    "driver_receipt",
                    str(row["receipt_id"]),
                    {key: row[key] for key in row.keys()},
                )
            )
        for row in connection.execute(
            """
            SELECT attempt_id,endpoint_id,lease_fence,action,
                   authority_receipt_id,operator_principal_hash,evidence_ref,
                   evidence_sha256,resolved_at,created_at
            FROM operator_resolutions WHERE source_kind='authority'
            ORDER BY attempt_id
            """
        ):
            archive_records.append(
                (
                    "operator_resolution",
                    str(row["attempt_id"]),
                    {key: row[key] for key in row.keys()},
                )
            )
        for row in connection.execute(
            """
            SELECT attempt_id,hermes_egress_receipt_id
            FROM native_attempts
            WHERE hermes_egress_receipt_id IS NOT NULL
            ORDER BY attempt_id
            """
        ):
            archive_records.append(
                (
                    "egress_receipt",
                    str(row["attempt_id"]),
                    {
                        "attempt_id": str(row["attempt_id"]),
                        "receipt_id": str(row["hermes_egress_receipt_id"]),
                    },
                )
            )
        if archive_records:
            _create_rollback_archive(connection)
            for kind, key, payload in archive_records:
                _archive_record(connection, kind, key, payload)

        connection.execute("PRAGMA defer_foreign_keys=ON")
        for statement in LEGACY_AUTHORITY_DDL:
            connection.execute(statement)
        if archive_records:
            _install_rollback_horizon_guards(connection)
        if fault_inject is not None:
            fault_inject("after_legacy_schema")
        bindings = connection.execute(
            """
            SELECT binding.*,endpoint.source_kind,endpoint.source_json,
                   endpoint.ref_version,endpoint.endpoint_key,
                   endpoint.candidate_endpoint_key,
                   provenance.binding_generation AS provenance_generation,
                   provenance.source_kind AS provenance_source_kind,
                   provenance.source_json AS provenance_source_json,
                   provenance.ref_version AS provenance_ref_version
            FROM thread_bindings AS binding
            JOIN endpoints AS endpoint ON endpoint.endpoint_id=binding.endpoint_id
            LEFT JOIN legacy_binding_sources AS provenance
              ON provenance.binding_id=binding.binding_id
            ORDER BY binding.created_at,binding.binding_id
            """
        ).fetchall()
        for row in bindings:
            state = str(row["state"])
            connection.execute(
                """
                INSERT INTO bridges(
                  bridge_id,source_kind,source_json,owner_user_id,team_id,
                  channel_id,thread_ts,idempotency_key,status,binding_version,
                  binding_generation,binding_state,binding_error_code,
                  endpoint_key,thread_claim_generation,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(row["binding_id"]),
                    str(row["provenance_source_kind"])
                    if row["provenance_generation"] == row["generation"]
                    else str(row["source_kind"]),
                    str(row["provenance_source_json"])
                    if row["provenance_generation"] == row["generation"]
                    else str(row["source_json"]),
                    str(row["owner_user_id"]),
                    str(row["team_id"]),
                    str(row["channel_id"]),
                    row["thread_ts"],
                    str(row["idempotency_key"]),
                    (
                        "closed"
                        if state == "closed"
                        else "active"
                        if state == "active"
                        else "pending"
                    ),
                    int(row["provenance_ref_version"])
                    if row["provenance_generation"] == row["generation"]
                    else int(row["ref_version"]),
                    int(row["generation"]),
                    "verified" if state == "active" else "rebind_required",
                    row["error_code"],
                    str(row["endpoint_key"] or row["candidate_endpoint_key"] or ""),
                    row["thread_claim_generation"],
                    str(row["created_at"]),
                    str(row["updated_at"]),
                ),
            )
        if fault_inject is not None:
            fault_inject("after_bindings")

        turns = connection.execute(
            "SELECT * FROM queued_turns ORDER BY created_at,event_key"
        ).fetchall()
        for row in turns:
            relation = connection.execute(
                """
                SELECT membership.attempt_id,attempt.binding_generation
                FROM native_attempt_turns AS membership
                JOIN endpoint_leases AS lease
                  ON lease.attempt_id=membership.attempt_id
                JOIN native_attempts AS attempt
                  ON attempt.attempt_id=membership.attempt_id
                WHERE membership.event_key=?
                ORDER BY attempt.binding_generation DESC,lease.fence DESC,
                         membership.attempt_id DESC LIMIT 1
                """,
                (row["event_key"],),
            ).fetchone()
            state = str(row["state"])
            connection.execute(
                """
                INSERT INTO bridge_events(
                  event_id,bridge_id,state,error,payload_json,attempt_id,
                  binding_generation,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(row["event_key"]),
                    str(row["binding_id"]),
                    "queued" if state == "ready" else "delivered" if state == "completed" else "failed",
                    row["error_code"],
                    json.dumps(
                        {"text": str(row["payload_inline"])},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    str(relation["attempt_id"]) if relation else None,
                    (
                        int(relation["binding_generation"])
                        if relation
                        else None
                        if state == "ready"
                        else int(row["binding_generation"])
                    ),
                    str(row["ordered_at"]),
                    str(row["updated_at"]),
                ),
            )
        if fault_inject is not None:
            fault_inject("after_turns")

        attempts = connection.execute(
            "SELECT * FROM native_attempts ORDER BY created_at,attempt_id"
        ).fetchall()
        for row in attempts:
            state = str(row["state"])
            pending_response = False
            if state == "completed_with_response":
                existing_reply = connection.execute(
                    """
                    SELECT bridge_id,state,message_ts,text_hash,payload_text,
                           client_msg_id
                    FROM bridge_replies WHERE reply_key=?
                    """,
                    (row["attempt_id"],),
                ).fetchone()
                response = str(row["response_inline"] or "")
                if existing_reply is not None and (
                    str(existing_reply["bridge_id"]) != str(row["binding_id"])
                    or str(existing_reply["text_hash"] or "")
                    != str(row["response_sha256"])
                    or str(existing_reply["payload_text"] or "") != response
                    or not str(existing_reply["client_msg_id"] or "")
                ):
                    raise RuntimeError(
                        "cannot roll back conflicting Slack reply identity"
                    )
                delivered = bool(
                    existing_reply is not None
                    and existing_reply["message_ts"] is not None
                )
                if (
                    existing_reply is not None
                    and not delivered
                    and str(existing_reply["state"]) not in {"reserved", "pending"}
                ):
                    raise RuntimeError(
                        "cannot roll back non-retryable Slack reply state"
                    )
                legacy_state = "acknowledged" if delivered else "replying"
                ack_kind = "reply" if delivered else None
                pending_response = not delivered
            elif state in {"no_reply", "operator_completed"}:
                legacy_state = "acknowledged"
                ack_kind = "no_reply" if state == "no_reply" else "operator_confirmed"
            elif state in {"cancelled", "operator_abandoned"}:
                legacy_state = "cancelled"
                ack_kind = "operator_abandoned" if state == "operator_abandoned" else None
            elif state == "failed_before_start":
                legacy_state = "requeued"
                ack_kind = None
            elif state == "failed":
                legacy_state = "failed"
                ack_kind = None
            else:
                raise RuntimeError("cannot roll back a nonterminal native attempt")
            connection.execute(
                """
                INSERT INTO bridge_attempts(
                  attempt_id,reply_key,bridge_id,binding_generation,
                  delivery_kind,state,ack_kind,error_code,created_at,
                  updated_at,submitted_at,acknowledged_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(row["attempt_id"]),
                    str(row["attempt_id"]),
                    str(row["binding_id"]),
                    int(row["binding_generation"]),
                    str(row["driver_kind"]),
                    legacy_state,
                    ack_kind,
                    row["error_code"],
                    str(row["created_at"]),
                    str(row["updated_at"]),
                    row["submitted_at"],
                    row["terminal_at"] if legacy_state == "acknowledged" else None,
                ),
            )
            if state == "completed_with_response":
                if pending_response:
                    client_msg_id = str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"tether:{row['attempt_id']}",
                        )
                    )
                    if existing_reply is None:
                        connection.execute(
                            """
                            INSERT INTO bridge_replies(
                              reply_key,bridge_id,text_hash,payload_text,
                              client_msg_id,state,created_at,updated_at
                            ) VALUES(?,?,?,?,?,'pending',?,?)
                            """,
                            (
                                str(row["attempt_id"]),
                                str(row["binding_id"]),
                                str(row["response_sha256"]),
                                response,
                                client_msg_id,
                                str(row["created_at"]),
                                str(row["updated_at"]),
                            ),
                        )
                    else:
                        connection.execute(
                            """
                            UPDATE bridge_replies
                            SET state='pending',updated_at=?
                            WHERE reply_key=? AND bridge_id=?
                              AND message_ts IS NULL
                              AND state IN ('reserved','pending')
                            """,
                            (
                                str(row["updated_at"]),
                                str(row["attempt_id"]),
                                str(row["binding_id"]),
                            ),
                        )
                    connection.execute(
                        """
                        UPDATE bridge_events
                        SET state='replying',error=NULL,updated_at=?
                        WHERE attempt_id=? AND bridge_id=?
                        """,
                        (
                            str(row["updated_at"]),
                            str(row["attempt_id"]),
                            str(row["binding_id"]),
                        ),
                    )

        if fault_inject is not None:
            fault_inject("after_attempts")

        if archive_records:
            # The triggers are installed before projection so the predecessor
            # cannot run without guards after a crash. Projection writes are
            # not fallback-era activity and must not supersede their archive.
            connection.execute("DELETE FROM tether_domain_rollback_activity")

        preserved_after = logical_manifest_v17(connection)
        changed = [
            key
            for key in PRESERVED_MANIFEST_KEYS
            if preserved_before[key] != preserved_after[key]
        ]
        if (
            preserved_after["endpoint_inventory"] is not None
            and preserved_before["endpoint_inventory"]
            != preserved_after["endpoint_inventory"]
        ):
            changed.append("endpoint_inventory")
        if changed:
            raise RuntimeError(
                "schema-17 rollback changed preserved logical records: "
                + ", ".join(changed)
            )
        if fault_inject is not None:
            fault_inject("after_validation")

        for table in (
            "native_attempt_turns",
            "driver_receipts",
            "operator_resolutions",
            "legacy_attempt_imports",
            "native_attempts",
            "endpoint_leases",
            "legacy_terminal_imports",
            "queued_turns",
            "legacy_binding_sources",
            "thread_bindings",
            "endpoint_authorized_owners",
            "endpoints",
        ):
            connection.execute(f"DROP TABLE {table}")
        if fault_inject is not None:
            fault_inject("after_v18_drop")
        connection.execute("PRAGMA user_version=17")
        if fault_inject is not None:
            fault_inject("after_version")
        if str(connection.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
            raise RuntimeError("rolled-back database failed integrity check")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def object_names(connection: sqlite3.Connection) -> Iterable[str]:
    return (
        str(row[0])
        for row in connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type IN ('table','index','trigger') AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )
    )
