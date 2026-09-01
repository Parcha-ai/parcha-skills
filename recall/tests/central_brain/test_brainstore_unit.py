from __future__ import annotations

import argparse
import hashlib
import contextlib
import inspect
import io
import json
import os
import sys
import tempfile
import types
import unittest
from unittest import mock
from pathlib import Path

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

try:
    import psycopg  # noqa: F401
except ModuleNotFoundError:
    psycopg = types.ModuleType("psycopg")
    psycopg_rows = types.ModuleType("psycopg.rows")
    psycopg_rows.dict_row = object()
    psycopg.rows = psycopg_rows
    sys.modules["psycopg"] = psycopg
    sys.modules["psycopg.rows"] = psycopg_rows

from recall_server import SCHEMA_VERSION
from recall_server import cli as server_cli
from recall_server.actor_attribution import (
    actor_id_for_employee_key,
    actor_id_for_principal,
    claimable_actor_for_display_name,
)
from recall_server.app import Handler, serve, serve_unix, validate_http_profile
from recall_server.capture import build_capture_event
from recall_server.db import (
    BrainStore,
    bounded_search_text,
    enough_session_anchors,
    optional_rescue_deadline,
    related_candidate_limit,
    semantic_candidate_limit,
    should_run_optional_rescue,
    turn_embedding_text,
)
from recall_server.federation import SOURCE_FAMILIES, SourceProfile
from recall_server.projectors import advisory_lock_key, canonical_json, effective_session_id, partial_lexical_probes, phrase_query_spec, preferred_phrase_probe, preferred_phrase_probes, project, redact_text, validate_envelope
from recall_server.ranking import DEFAULT_SEARCH_DEADLINE_MS, evidence_rank_components, retrieval_leg_order, should_run_partial


def envelope(**updates):
    value = {
        "schema_version": 1,
        "source_id": "codex:laptop",
        "native_id": "session-1:turn-1",
        "native_parent_id": "session-1",
        "kind": "message",
        "occurred_at": "2026-07-12T20:00:00Z",
        "observed_at": "2026-07-12T20:00:01Z",
        "principal_id": "owner",
        "visibility": "private",
        "content_type": "application/json",
        "content": {"role": "user", "text": "remember the quartz decision"},
        "provenance": {"harness": "codex"},
    }
    value.update(updates)
    value["content_sha256"] = hashlib.sha256(canonical_json(value["content"])).hexdigest()
    return value


class SchemaMigrationContractTest(unittest.TestCase):
    def test_migration_versions_are_unique_contiguous_and_current(self) -> None:
        migrations = sorted((SERVER / "schema").glob("*.sql"))
        versions = [int(path.name.split("_", 1)[0]) for path in migrations]
        self.assertEqual(versions, list(range(1, SCHEMA_VERSION + 1)))
        for version, path in zip(versions, migrations, strict=True):
            self.assertRegex(
                path.read_text(),
                rf"schema_migrations\(version\) VALUES \({version}\)",
            )

    def test_turn_backfill_anchor_cursor_has_a_session_id_index(self) -> None:
        migration = SERVER / "schema" / "022_turn_anchor_cursor_index.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn(
            "on items(source_id, session_native_id, id)",
            rendered,
        )
        self.assertIn(
            "where deleted_at is null and role='user' "
            "and btrim(text_redacted)<>''",
            rendered,
        )

    def test_canonical_event_foreign_key_has_a_supporting_index(self) -> None:
        migration = (
            SERVER
            / "schema"
            / "033_canonical_document_event_lookup.sql"
        )
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn(
            "on canonical_documents(tenant_id, source_id, event_id)",
            rendered,
        )

    def test_turn_dedupe_has_a_session_scoped_cleanup_index(self) -> None:
        migration = SERVER / "schema" / "023_turn_prompt_dedupe_index.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn(
            "on turn_embeddings(source_id, session_native_id, anchor_item_id)",
            rendered,
        )

    def test_turn_dedupe_upgrade_requeues_historical_duplicate_sessions(
        self,
    ) -> None:
        migration = SERVER / "schema" / "024_requeue_turn_prompt_duplicates.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn("insert into turn_embedding_dirty_sessions(", rendered)
        self.assertIn(
            "source_id,session_native_id,last_anchor_item_id,updated_at",
            rendered,
        )
        self.assertIn(
            "having count(*)>count(distinct anchor.text_redacted)",
            rendered,
        )
        self.assertIn(
            "on conflict(source_id,session_native_id) do update set "
            "last_anchor_item_id=0",
            rendered,
        )

    def test_turn_dedupe_upgrade_collapses_historical_projection_rows(
        self,
    ) -> None:
        migration = SERVER / "schema" / "025_collapse_turn_prompt_duplicates.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn(
            "row_number() over ( partition by embedding.source_id, "
            "embedding.session_native_id, anchor.text_redacted "
            "order by embedding.anchor_item_id desc )",
            rendered,
        )
        self.assertIn(
            "delete from turn_embeddings duplicate using ranked",
            rendered,
        )

    def test_v2_canonical_plane_is_tenant_keyed_and_deletion_explicit(self) -> None:
        migration = SERVER / "schema" / "019_v2_canonical_plane.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        required_tables = {
            "brain_tenants",
            "brain_principals",
            "canonical_sources",
            "raw_artifacts",
            "canonical_events",
            "canonical_documents",
            "canonical_chunks",
            "canonical_ingest_jobs",
            "receipt_redirects",
            "forget_tombstones",
            "canonical_audit_events",
        }
        for table in required_tables:
            self.assertIn(f"create table if not exists {table}", rendered)
        self.assertGreaterEqual(rendered.count("tenant_id text not null"), 10)
        self.assertIn(
            "unique(tenant_id, source_id, native_id, revision)",
            rendered,
        )
        self.assertIn(
            "unique(storage_backend, object_key, version_id)",
            rendered,
        )
        self.assertIn(
            "check ((state='live' and deleted_at is null) or "
            "(state='deleted' and deleted_at is not null))",
            rendered,
        )
        self.assertIn("target_identity_sha256 char(64) not null", rendered)
        audit = rendered.split(
            "create table if not exists canonical_audit_events", 1
        )[1].split(");", 1)[0]
        self.assertNotIn("jsonb", audit)
        self.assertIn("item_count integer", audit)
        self.assertIn("byte_count bigint", audit)
        self.assertNotIn("raw_payload", rendered)
        self.assertNotIn("password", rendered)

    def test_v2_lifecycle_migration_fences_resurrection_and_resumes_forget(self) -> None:
        migration = SERVER / "schema" / "020_v2_lifecycle_guards.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn("state in ('live', 'deleting', 'deleted')", rendered)
        self.assertIn(
            "encryption in ('filesystem-owner-only', 'sse-s3', 'sse-kms', 'sse-c')",
            rendered,
        )
        self.assertIn("status in ('deleting', 'deleted')", rendered)
        self.assertIn("forget_tombstones_idempotency_idx", rendered)
        self.assertIn("recall_v2_reject_forgotten_event", rendered)
        self.assertIn("before insert on canonical_events", rendered)
        self.assertIn("errcode='23514'", rendered)

    def test_connector_v2_source_families_are_host_owned_and_migrated(self) -> None:
        added = {
            "communications", "schedule", "contacts", "social", "documents",
            "work_activity", "local_activity", "personal_media",
        }
        self.assertTrue(added.issubset(SOURCE_FAMILIES))
        for family in added:
            profile = SourceProfile.from_mapping({
                "source_id": "synthetic:source:v2", "family": family,
                "quality": "standard", "freshness_half_life_days": 30,
            })
            self.assertEqual(profile.family, family)
        migration = SERVER / "schema" / "012_source_profile_families.sql"
        self.assertTrue(migration.is_file())
        rendered = migration.read_text()
        self.assertTrue(all(f"'{family}'" in rendered for family in added))

    def test_source_scoped_backfill_has_a_live_item_id_index(self) -> None:
        migration = SERVER / "schema" / "013_source_backfill_index.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn("on items(source_id, id)", rendered)
        self.assertIn(
            "where deleted_at is null and btrim(text_redacted) <> ''",
            rendered,
        )

    def test_global_embedding_backfill_has_a_runtime_scoped_watermark(self) -> None:
        migration = SERVER / "schema" / "014_embedding_projection_watermark.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn(
            "runtime_fingerprint char(64) primary key",
            rendered,
        )
        self.assertIn(
            "last_item_id bigint not null default 0",
            rendered,
        )

        implementation = inspect.getsource(BrainStore.embed_pending)
        self.assertIn("use_watermark = source_id is None and surface is None", implementation)
        self.assertIn("WHERE item.id>%s", implementation)
        self.assertIn("WHERE id>%s", implementation)
        self.assertIn("SELECT min(item_id) AS item_id", implementation)
        self.assertIn("WHERE runtime_fingerprint<>%s", implementation)
        self.assertIn("SET last_item_id=LEAST(last_item_id,%s)", implementation)
        self.assertIn("GREATEST(last_item_id,%s)", implementation)
        self.assertLess(
            implementation.index("cursor.executemany("),
            implementation.rindex("GREATEST(last_item_id,%s)"),
        )

    def test_remote_principal_credentials_are_migrated(self) -> None:
        migration = SERVER / "schema" / "015_principal_credentials.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn(
            "add column if not exists principal_id text",
            rendered,
        )
        self.assertIn(
            "source_grants(principal_id, permission, source_id)",
            rendered,
        )

    def test_capture_origin_is_host_owned_credential_state(self) -> None:
        migration = SERVER / "schema" / "016_capture_credentials.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn(
            "add column if not exists capture_origin text",
            rendered,
        )

    def test_answer_adjacency_uses_role_scoped_turn_index_and_bounded_boundary(self) -> None:
        migration = SERVER / "schema" / "017_answer_turn_index.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn(
            "on items(source_id, session_native_id, role, occurred_at, id)",
            rendered,
        )
        self.assertIn(
            "where deleted_at is null and role in ('user','assistant')",
            rendered,
        )

        implementation = inspect.getsource(BrainStore._answer_leg)
        self.assertIn("LEFT JOIN LATERAL", implementation)
        self.assertIn("boundary.role='user'", implementation)
        self.assertIn("candidate.role='assistant'", implementation)
        self.assertIn("%s::boolean[]", implementation)
        self.assertIn('"exact-answer"', implementation)
        self.assertNotIn("NOT EXISTS", implementation)

    def test_webhook_privacy_is_host_owned_credential_state(self) -> None:
        migration = SERVER / "schema" / "018_webhook_credentials.sql"
        rendered = " ".join(migration.read_text().split()).casefold()
        self.assertIn(
            "add column if not exists webhook_privacy_mode text",
            rendered,
        )

    def test_managed_upgrade_documents_split_role_grant_refresh(self) -> None:
        guide = " ".join(
            (SERVER / "deploy" / "README.md").read_text().split()
        ).casefold()
        self.assertIn(f"schema migrations 1 through {SCHEMA_VERSION}", guide)
        self.assertIn("refresh runtime grants after every migration", guide)
        self.assertIn("on all tables in schema public", guide)
        self.assertIn("on all sequences in schema public", guide)
        self.assertIn(
            "revoke all privileges on table public.schema_migrations",
            guide,
        )
        self.assertIn(
            "grant select on table public.schema_migrations",
            guide,
        )


class EmbeddingBackfillWatermarkTest(unittest.TestCase):
    def test_global_backfill_advances_cursor_only_after_embedding_write(self) -> None:
        class Runtime:
            model = "voyage-4-lite"
            fingerprint = "a" * 64
            dimensions = 512

            @staticmethod
            def embed_documents(_texts):
                return [[0.0] * 512]

        class Result:
            def __init__(self, *, one=None, all_rows=None):
                self.one = one
                self.all_rows = all_rows

            def fetchone(self):
                return self.one

            def fetchall(self):
                return self.all_rows

        class Cursor:
            def __init__(self, connection):
                self.connection = connection

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def executemany(self, _sql, values):
                self.connection.embedded = True
                self.connection.embedding_values = values

            def execute(self, sql, params):
                self.connection.cursor_updates.append((sql, params))
                if "embedding_projection_watermarks" in sql:
                    self.connection.watermark_advanced_after_write = (
                        self.connection.embedded
                    )

        class Connection:
            def __init__(self):
                self.embedded = False
                self.embedding_values = []
                self.cursor_updates = []
                self.watermark_advanced_after_write = False
                self.selection_params = None

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def transaction(self):
                return contextlib.nullcontext()

            def cursor(self):
                return Cursor(self)

            def execute(self, sql, params=None):
                normalized = " ".join(sql.split())
                if "pg_try_advisory_lock(" in normalized:
                    return Result(one={"value": True})
                if normalized.startswith(
                    "INSERT INTO embedding_projection_watermarks"
                ):
                    return Result()
                if normalized.startswith("SELECT last_item_id"):
                    return Result(one={"last_item_id": 7})
                if normalized.startswith("SELECT min(item_id) AS item_id"):
                    return Result(one={"item_id": None})
                if normalized.startswith("SELECT item.id") and "item.id>%s" in normalized:
                    self.selection_params = params
                    return Result(
                        all_rows=[
                            {
                                "id": 8,
                                "source_id": "synthetic:source",
                                "text_redacted": "synthetic safe text",
                                "projector_version": 1,
                            }
                        ]
                    )
                if "pg_advisory_unlock(" in normalized:
                    return Result(one={"value": True})
                raise AssertionError(f"unexpected SQL: {normalized}")

        connection = Connection()
        store = BrainStore("postgresql://synthetic.invalid/db", semantic_runtime=Runtime())
        with mock.patch.object(store, "connect", return_value=connection):
            result = store.embed_pending(batch_size=1, max_batches=1)

        self.assertEqual(result["processed"], 1)
        self.assertEqual(connection.selection_params[0], 7)
        self.assertEqual(connection.embedding_values[0][0], 8)
        self.assertTrue(connection.watermark_advanced_after_write)
        watermark_update = next(
            params
            for sql, params in connection.cursor_updates
            if "embedding_projection_watermarks" in sql
        )
        self.assertEqual(watermark_update, (8, Runtime.fingerprint))


class TypedConnectorProjectionTest(unittest.TestCase):
    def test_v2_communication_projects_clean_conversation_evidence(self) -> None:
        value = envelope(
            kind="connector_record", native_id="message-1", native_parent_id="thread-1",
            content={
                "kind": "communication_message.v1", "conversation_id": "thread-1",
                "message_id": "message-1", "direction": "inbound",
                "content_fidelity": "complete",
                "subject": "Synthetic subject", "text": "Synthetic body",
            },
            provenance={"connector_id": "google.gmail", "connector_schema_version": 2},
        )
        validate_envelope(value)
        items, metadata = project(value, 1)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["surface"], "communication_message.v1")
        self.assertEqual(items[0]["role"], "inbound")
        self.assertEqual(items[0]["text_redacted"], "Synthetic subject\nSynthetic body")
        self.assertEqual(metadata["record_kind"], "communication_message.v1")
        self.assertEqual(metadata["content_fidelity"], "complete")
        self.assertNotIn("content_omissions", metadata)

    def test_v2_connector_record_rejects_unknown_or_mismatched_kind(self) -> None:
        for content in (
            {"kind": "runtime_plugin.v1", "text": "synthetic"},
            {"text": "synthetic"},
        ):
            value = envelope(
                kind="connector_record", content=content,
                provenance={"connector_id": "synthetic.pull", "connector_schema_version": 2},
            )
            with self.subTest(content=content), self.assertRaises(ValueError):
                validate_envelope(value)

    def test_v2_connector_record_rejects_invalid_field_value(self) -> None:
        value = envelope(
            kind="connector_record",
            content={
                "kind": "communication_message.v1", "conversation_id": "thread-1",
                "message_id": "message-1", "direction": "sideways", "text": "synthetic",
                "content_fidelity": "complete",
            },
            provenance={"connector_id": "synthetic.pull", "connector_schema_version": 2},
        )
        with self.assertRaisesRegex(ValueError, "invalid typed connector record"):
            validate_envelope(value)

    def test_v2_document_rejects_invalid_artifact_digest(self) -> None:
        value = envelope(
            kind="connector_record",
            content={
                "kind": "document.v1",
                "content_fidelity": "complete",
                "document_id": "document-1",
                "name": "Synthetic attachment",
                "mime_type": "text/plain",
                "artifact_content_sha256": "not-a-digest",
            },
            provenance={"connector_id": "google.gmail", "connector_schema_version": 2},
        )
        with self.assertRaisesRegex(ValueError, "invalid typed connector record"):
            validate_envelope(value)

    def test_v2_partial_fidelity_projects_omissions_and_rejects_fake_success(self) -> None:
        partial = envelope(
            kind="connector_record", native_id="message-partial",
            content={
                "kind": "communication_message.v1",
                "content_fidelity": "partial",
                "content_omissions": ["body_truncated"],
                "conversation_id": "thread-1", "message_id": "message-partial",
                "direction": "inbound", "text": "Synthetic partial body",
            },
            provenance={"connector_id": "synthetic.pull", "connector_schema_version": 2},
        )
        validate_envelope(partial)
        _items, metadata = project(partial, 1)
        self.assertEqual(metadata["content_fidelity"], "partial")
        self.assertEqual(metadata["content_omissions"], ["body_truncated"])

        for mutation in (
            {"content_fidelity": "partial"},
            {"content_fidelity": "partial", "content_omissions": []},
            {"content_fidelity": "complete", "content_omissions": ["body_truncated"]},
        ):
            invalid = envelope(
                kind="connector_record",
                content={
                    "kind": "communication_message.v1",
                    "conversation_id": "thread-1", "message_id": "message-1",
                    "direction": "inbound", "text": "Synthetic body", **mutation,
                },
                provenance={"connector_id": "synthetic.pull", "connector_schema_version": 2},
            )
            with self.subTest(mutation=mutation), self.assertRaisesRegex(
                ValueError, "invalid typed connector record",
            ):
                validate_envelope(invalid)


class SourceScopedReadContractTest(unittest.TestCase):
    def test_family_and_alias_resolve_once_to_an_intersected_source_set(self) -> None:
        connection = mock.MagicMock()

        def execute(sql, _params):
            result = mock.MagicMock()
            if "source_profiles" in sql:
                result.fetchall.return_value = [
                    {"source_id": "source-a"}, {"source_id": "source-b"},
                ]
            else:
                result.fetchone.return_value = {"source_id": "source-b"}
            return result

        connection.execute.side_effect = execute
        self.assertEqual(
            BrainStore._resolve_routed_source_ids(connection, {
                "source_family": "coding_history", "source_alias": "cowork",
            }),
            ["source-b"],
        )
        self.assertEqual(connection.execute.call_count, 2)

    def test_requested_source_filters_intersect_with_authorized_scope(self) -> None:
        where, params = BrainStore._read_filters(
            {"source_id": "source-b", "source_family": "coding_history", "source_alias": "cowork"},
            authorized_source="source-a",
            routed_source_ids=["source-b", "source-c"],
        )
        self.assertEqual(where.count("i.source_id = %s"), 2)
        self.assertNotIn("sp.family", where)
        self.assertNotIn("SELECT source_id FROM source_aliases", where)
        self.assertIn("i.source_id = ANY(%s)", where)
        self.assertEqual(params, ["source-a", "source-b", ["source-b", "source-c"]])

    def test_authorized_source_set_is_closed_even_when_empty(self) -> None:
        where, params = BrainStore._read_filters(
            {},
            authorized_source=[],
        )
        self.assertIn("i.source_id = ANY(%s)", where)
        self.assertEqual(params, [[]])


class EmployeeActorRetrofitContractTest(unittest.TestCase):
    def test_employee_key_is_stable_and_separate_from_login_identity(self) -> None:
        first = actor_id_for_employee_key("tenant:company:test", "employee-1")
        self.assertEqual(
            first,
            actor_id_for_employee_key("tenant:company:test", "employee-1"),
        )
        self.assertRegex(first, r"^actor_[0-9a-f]{32}$")
        self.assertNotEqual(
            first,
            actor_id_for_principal("tenant:company:test", "principal:employee-1"),
        )
        for invalid in ("", "UPPER", "has space", "x" * 65):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                actor_id_for_employee_key("tenant:company:test", invalid)

    def test_invitation_claims_only_one_exact_unlinked_actor(self) -> None:
        connection = mock.MagicMock()
        connection.execute.return_value.fetchall.return_value = [
            {"actor_id": "actor_" + "a" * 32}
        ]
        self.assertEqual(
            claimable_actor_for_display_name(
                connection,
                tenant_id="tenant:company:test",
                display_name="Employee One",
            ),
            "actor_" + "a" * 32,
        )
        sql, params = connection.execute.call_args.args
        self.assertIn("HAVING count(principal.principal_id)=0", sql)
        self.assertEqual(params, ("tenant:company:test", "Employee One"))

        connection.execute.return_value.fetchall.return_value = [
            {"actor_id": "actor_" + "a" * 32},
            {"actor_id": "actor_" + "b" * 32},
        ]
        self.assertIsNone(
            claimable_actor_for_display_name(
                connection,
                tenant_id="tenant:company:test",
                display_name="Employee One",
            )
        )

    def test_new_coding_profile_binds_its_owner_actor_when_unattributed(self) -> None:
        results = []
        for value in (
            {"exists": 1},
            None,
            {"tenant_id": "tenant:company:test", "source_id": "codex:linux:new"},
            None,
            None,
        ):
            result = mock.MagicMock()
            result.fetchone.return_value = value
            results.append(result)
        results[3].rowcount = 3
        connection = mock.MagicMock()
        connection.execute.side_effect = results
        context = mock.MagicMock()
        context.__enter__.return_value = connection
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock(return_value=context)

        configured = store.set_source_profile({
            "source_id": "codex:linux:new",
            "family": "coding_history",
            "quality": "trusted",
            "freshness_half_life_days": 30,
        })
        self.assertEqual(configured["status"], "configured")
        binding_sql = connection.execute.call_args_list[2].args[0]
        self.assertIn("NOT EXISTS", binding_sql)
        self.assertIn("brain_actor_principals", binding_sql)
        queue_sql = connection.execute.call_args_list[3].args[0]
        self.assertIn("canonical_evidence_document_queue", queue_sql)

    def test_retrofit_binds_exact_coding_sources_and_requeues_projections(self) -> None:
        source_a = "codex:linux:employee-1"
        source_b = "claude:linux:employee-1"
        actor_id = actor_id_for_employee_key("tenant:company:test", "employee-1")
        def execute(sql, _params):
            result = mock.MagicMock()
            if "SELECT source.source_id,source.owner_principal_id" in sql:
                result.fetchall.return_value = [
                    {"source_id": source_b, "owner_principal_id": "owner", "family": None},
                    {"source_id": source_a, "owner_principal_id": "owner", "family": None},
                ]
            elif "SELECT DISTINCT binding.actor_id" in sql:
                result.fetchall.return_value = []
            elif "lower(display_name)=lower" in sql:
                result.fetchall.return_value = []
            elif "SELECT display_name,actor_kind,active" in sql:
                result.fetchone.return_value = {
                    "display_name": "Employee One",
                    "actor_kind": "human",
                    "active": True,
                }
            elif "INSERT INTO canonical_source_actor_bindings" in sql:
                result.fetchall.return_value = [
                    {"source_id": source_b}, {"source_id": source_a},
                ]
            elif "INSERT INTO canonical_evidence_document_queue" in sql:
                result.rowcount = 2
            return result
        connection = mock.MagicMock()
        connection.execute.side_effect = execute
        context = mock.MagicMock()
        context.__enter__.return_value = connection
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock(return_value=context)

        self.assertEqual(
            store.bind_coding_sources_to_employee(
                tenant_id="tenant:company:test",
                employee_key="employee-1",
                display_name=" Employee One ",
                source_ids=[source_a, source_b],
            ),
            {
                "status": "bound",
                "actor_id": actor_id,
                "sources": 2,
                "new_bindings": 2,
                "queued_documents": 2,
            },
        )
        binding_call = next(
            call for call in connection.execute.call_args_list
            if "INSERT INTO canonical_source_actor_bindings" in call.args[0]
        )
        binding_sql, binding_params = binding_call.args
        self.assertIn("canonical_source_actor_bindings", binding_sql)
        self.assertEqual(
            binding_params,
            ("tenant:company:test", actor_id, [source_b, source_a]),
        )
        queue_call = next(
            call for call in connection.execute.call_args_list
            if "INSERT INTO canonical_evidence_document_queue" in call.args[0]
        )
        queue_sql, queue_params = queue_call.args
        self.assertIn("canonical_evidence_document_queue", queue_sql)
        self.assertIn("'backfill'", queue_sql)
        self.assertNotIn("actor-binding", queue_sql)
        self.assertEqual(
            queue_params,
            ("tenant:company:test", [source_b, source_a]),
        )

    def test_retrofit_adopts_one_existing_source_actor(self) -> None:
        actor_id = "actor_" + "a" * 32
        source_ids = ["claude:linux:one", "codex:linux:one"]

        def execute(sql, _params):
            result = mock.MagicMock()
            if "SELECT source.source_id,source.owner_principal_id" in sql:
                result.fetchall.return_value = [
                    {"source_id": value, "owner_principal_id": "owner", "family": "coding_history"}
                    for value in source_ids
                ]
            elif "SELECT DISTINCT binding.actor_id" in sql:
                result.fetchall.return_value = [{"actor_id": actor_id}]
            elif "lower(display_name)=lower" in sql:
                result.fetchall.return_value = []
            elif "SELECT display_name,actor_kind,active" in sql:
                result.fetchone.return_value = {
                    "display_name": "Employee One", "actor_kind": "human", "active": True,
                }
            elif "INSERT INTO canonical_source_actor_bindings" in sql:
                result.fetchall.return_value = []
            elif "INSERT INTO canonical_evidence_document_queue" in sql:
                result.rowcount = 2
            return result

        connection = mock.MagicMock()
        connection.execute.side_effect = execute
        context = mock.MagicMock()
        context.__enter__.return_value = connection
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock(return_value=context)
        result = store.bind_coding_sources_to_employee(
            tenant_id="tenant:company:test",
            employee_key="employee-1",
            display_name="Employee One",
            source_ids=source_ids,
        )
        self.assertEqual(result["actor_id"], actor_id)
        self.assertEqual(result["new_bindings"], 0)
        self.assertTrue(any(
            "UPDATE brain_actors" in call.args[0]
            for call in connection.execute.call_args_list
        ))

    def test_retrofit_rejects_duplicate_or_non_coding_sources(self) -> None:
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock()
        with self.assertRaisesRegex(ValueError, "invalid employee source binding"):
            store.bind_coding_sources_to_employee(
                tenant_id="tenant:company:test",
                employee_key="employee-1",
                display_name="Employee One",
                source_ids=["codex:linux:one", "codex:linux:one"],
            )
        store.connect.assert_not_called()

        connection = mock.MagicMock()
        source_result = mock.MagicMock()
        source_result.fetchall.return_value = [
            {"source_id": "codex:linux:one", "owner_principal_id": "owner", "family": None},
            {"source_id": "slack:company:one", "owner_principal_id": "owner", "family": "communications"},
        ]
        connection.execute.return_value = source_result
        context = mock.MagicMock()
        context.__enter__.return_value = connection
        store.connect.return_value = context
        with self.assertRaisesRegex(ValueError, "requires coding-history sources"):
            store.bind_coding_sources_to_employee(
                tenant_id="tenant:company:test",
                employee_key="employee-1",
                display_name="Employee One",
                source_ids=["codex:linux:one", "slack:company:one"],
            )


class SourceScopedResolveContractTest(unittest.TestCase):
    def test_resolve_filters_by_the_authenticated_collector_source(self) -> None:
        connection = mock.MagicMock()
        connection.execute.return_value.fetchone.return_value = None
        context = mock.MagicMock()
        context.__enter__.return_value = connection
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock(return_value=context)

        self.assertIsNone(
            store.resolve(
                "recall://source-a/item-1?rev=1",
                authorized_source="source-a",
            )
        )
        sql, params = connection.execute.call_args.args
        self.assertIn("event.source_id=%s", sql)
        self.assertEqual(params, ("source-a", "item-1", 1, "source-a", "source-a"))

    def test_http_resolve_passes_the_principal_source_scope(self) -> None:
        handler = object.__new__(Handler)
        handler.path = "/v1/receipts/resolve?receipt=recall%3A%2F%2Fsource-a%2Fitem-1%3Frev%3D1"
        handler.require = mock.MagicMock(return_value={"source_id": "source-a"})
        handler.store = mock.MagicMock()
        handler.store.resolve.return_value = None
        handler.send_json = mock.MagicMock()

        Handler.do_GET(handler)

        handler.store.resolve.assert_called_once_with(
            "recall://source-a/item-1?rev=1", authorized_source="source-a",
        )
        handler.send_json.assert_called_once_with(404, {"error": "not found"})


class HttpBoundaryContractTest(unittest.TestCase):
    def test_readiness_executes_one_constant_query(self) -> None:
        connection = mock.MagicMock()
        connection.execute.return_value.fetchone.return_value = {"ready": 1}
        context = mock.MagicMock()
        context.__enter__.return_value = connection
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock(return_value=context)

        self.assertEqual(store.readiness(), {"status": "ready"})
        connection.execute.assert_called_once_with("SELECT 1 AS ready")

    def test_readyz_uses_constant_time_database_probe_not_full_metrics(self) -> None:
        handler = object.__new__(Handler)
        handler.path = "/readyz"
        handler.store = mock.MagicMock()
        handler.store.readiness.return_value = {"status": "ready"}
        handler.send_json = mock.MagicMock()

        Handler.do_GET(handler)

        handler.store.readiness.assert_called_once_with()
        handler.store.service_metrics.assert_not_called()
        handler.send_json.assert_called_once_with(200, {"status": "ready"})

    def test_operational_health_uses_one_bounded_projection_probe(self) -> None:
        connection = mock.MagicMock()
        connection.execute.return_value.fetchone.return_value = {"projection_lag": 0}
        context = mock.MagicMock()
        context.__enter__.return_value = connection
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock(return_value=context)

        self.assertEqual(
            store.operational_health(),
            {"status": "ok", "projection_lag": 0},
        )
        connection.execute.assert_called_once()
        sql = connection.execute.call_args.args[0]
        self.assertIn("ORDER BY id DESC LIMIT 1", sql)
        self.assertNotIn("count(", sql.casefold())


    def test_remote_doctor_uses_bounded_health_not_full_metrics(self) -> None:
        handler = object.__new__(Handler)
        handler.path = "/v1/doctor"
        handler.require = mock.MagicMock(return_value={"source_id": "source-a"})
        handler.store = mock.MagicMock()
        handler.store.operational_health.return_value = {
            "status": "ok",
            "projection_lag": 0,
        }
        handler.send_json = mock.MagicMock()

        Handler.do_GET(handler)

        handler.store.operational_health.assert_called_once_with()
        handler.store.doctor.assert_not_called()
        handler.store.service_metrics.assert_not_called()
        handler.send_json.assert_called_once_with(
            200,
            {"status": "ok", "projection_lag": 0},
        )

    def test_search_rejects_oversized_query_before_database_or_model_io(self) -> None:
        with self.assertRaisesRegex(ValueError, "too large"):
            BrainStore("postgresql://unused").search("x" * 8193)

    def test_malformed_content_length_is_a_closed_client_error(self) -> None:
        handler = object.__new__(Handler)
        handler.headers = {"Content-Length": "not-an-integer"}
        handler.send_json = mock.MagicMock()
        self.assertIsNone(handler.body_length(1024))
        handler.send_json.assert_called_once_with(400, {"error": "invalid body size"})

    def test_unauthenticated_tcp_cannot_bind_beyond_loopback(self) -> None:
        with mock.patch.dict(os.environ, {"RECALL_AUTH_REQUIRED": "0"}):
            with self.assertRaisesRegex(RuntimeError, "authentication"):
                serve("postgresql://synthetic.invalid/recall", "0.0.0.0", 8788)

    def test_public_mcp_profile_requires_bearer_auth_and_forbids_proxy_headers(
        self,
    ) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "RECALL_HTTP_PROFILE": "public-mcp",
                "RECALL_AUTH_REQUIRED": "0",
                "RECALL_TRUST_TAILSCALE_HEADERS": "0",
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "requires authentication"):
                serve("postgresql://synthetic.invalid/recall")
        with mock.patch.dict(
            os.environ,
            {
                "RECALL_HTTP_PROFILE": "public-mcp",
                "RECALL_AUTH_REQUIRED": "1",
                "RECALL_TRUST_TAILSCALE_HEADERS": "1",
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "forbids trusted"):
                serve("postgresql://synthetic.invalid/recall")
        with mock.patch.dict(
            os.environ,
            {"RECALL_HTTP_PROFILE": "public-mpc"},
        ):
            with self.assertRaisesRegex(RuntimeError, "unsupported HTTP profile"):
                validate_http_profile()

    def test_public_canonical_routes_require_auth_and_enabled_v2_runtime(self) -> None:
        base = {
            "RECALL_HTTP_PROFILE": "public-mcp",
            "RECALL_CANONICAL_INGEST_PUBLIC": "1",
            "RECALL_TRUST_TAILSCALE_HEADERS": "0",
        }
        for values in (
            {**base, "RECALL_AUTH_REQUIRED": "0", "RECALL_CANONICAL_V2_ENABLED": "1"},
            {**base, "RECALL_AUTH_REQUIRED": "1", "RECALL_CANONICAL_V2_ENABLED": "0"},
        ):
            with self.subTest(values=values), mock.patch.dict(
                os.environ,
                values,
                clear=True,
            ), self.assertRaisesRegex(RuntimeError, "canonical ingest"):
                validate_http_profile()

    def test_unix_server_refuses_to_replace_a_regular_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "recall.sock"
            path.write_text("do not delete")
            with self.assertRaisesRegex(RuntimeError, "non-socket"):
                serve_unix("postgresql://synthetic.invalid/recall", str(path))
            self.assertEqual(path.read_text(), "do not delete")


class IngestTransactionContractTest(unittest.TestCase):
    def test_batch_projection_advances_the_shared_watermark_once(self) -> None:
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store._project_one = mock.MagicMock()
        store._advance_projector = mock.MagicMock()
        connection = mock.MagicMock()
        events = [
            (41, envelope(native_id="turn-1"), 1),
            (43, envelope(native_id="turn-2"), 1),
            (42, envelope(native_id="turn-3"), 1),
        ]

        store._project_batch(connection, events)

        self.assertEqual(
            store._project_one.call_args_list,
            [
                mock.call(connection, 41, events[0][1], 1),
                mock.call(connection, 43, events[1][1], 1),
                mock.call(connection, 42, events[2][1], 1),
            ],
        )
        store._advance_projector.assert_called_once_with(connection, 43)


class DeliberateCaptureContractTest(unittest.TestCase):
    def test_capture_returns_the_canonical_searchable_item_receipt(self) -> None:
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.ingest = mock.MagicMock(
            return_value=(
                {
                    "status": "committed",
                    "receipts": [
                        "recall://synthetic:capture/capture_synthetic?rev=1"
                    ],
                },
                False,
            )
        )

        result = store.capture(
            {
                "source_id": "synthetic:capture",
                "principal_id": "synthetic-owner",
                "capture_origin": "synthetic-agent",
            },
            {
                "schema_version": 1,
                "title": "Synthetic canonical receipt",
                "body": "synthetic receipt evidence",
                "occurred_at": "2026-07-18T02:00:00Z",
                "tags": ["synthetic"],
                "provenance": {"uri": "manual://synthetic"},
            },
        )

        self.assertEqual(
            result["receipt"],
            "recall://synthetic:capture/capture_synthetic?rev=1#item=0",
        )

    def test_public_capture_limit_fits_the_mcp_transport_budget(self) -> None:
        principal = {
            "source_id": "synthetic:capture",
            "principal_id": "synthetic-owner",
            "capture_origin": "synthetic-agent",
        }
        base = {
            "schema_version": 1,
            "title": "Synthetic bounded capture",
            "occurred_at": "2026-07-18T02:00:00Z",
            "tags": ["synthetic"],
            "provenance": {"uri": "manual://synthetic"},
        }
        for body in ("x" * 32_000, "😀" * 32_000, "\x00" * 32_000):
            with self.subTest(kind=body[:1].encode().hex()):
                event, _privacy = build_capture_event(
                    {**base, "body": body},
                    principal,
                )
                request_body = json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {
                            "name": "recall_capture",
                            "arguments": {**base, "body": body},
                        },
                    },
                    ensure_ascii=False,
                ).encode()
                self.assertLessEqual(len(body), 32_000)
                self.assertLessEqual(len(body.encode()), 128 * 1024)
                self.assertLess(len(request_body), 256 * 1024)
                self.assertEqual(event["content"]["body"], body)

        for body in ("x" * 32_001, "😀" * 32_001):
            with self.assertRaisesRegex(ValueError, "capture body"):
                build_capture_event({**base, "body": body}, principal)


class SemanticRetrievalConfigurationTest(unittest.TestCase):
    def test_storage_footprint_is_aggregate_only(self) -> None:
        store = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value
        connection.execute.return_value.fetchall.return_value = [
            {
                "schemaname": "public",
                "relname": "canonical_passages",
                "total_bytes": 120,
                "heap_bytes": 80,
                "index_bytes": 40,
                "n_live_tup": 7,
                "n_dead_tup": 2,
            },
            {
                "schemaname": "public",
                "relname": "canonical_passage_embeddings",
                "total_bytes": 300,
                "heap_bytes": 200,
                "index_bytes": 100,
                "n_live_tup": 7,
                "n_dead_tup": 0,
            },
        ]

        report = server_cli._storage_footprint(store)

        self.assertEqual(report["total_relation_bytes"], 420)
        self.assertEqual(
            [row["relation"] for row in report["relations"]],
            ["canonical_passages", "canonical_passage_embeddings"],
        )
        self.assertNotIn("text", json.dumps(report))

    def test_storage_compaction_preserves_relation_scope_and_reports_bytes(
        self,
    ) -> None:
        store = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value
        existing = mock.MagicMock()
        existing.fetchall.return_value = [{"relname": "canonical_chunks"}]
        before = mock.MagicMock()
        before.fetchone.return_value = {"bytes": 1_000}
        vacuum = mock.MagicMock()
        reindex = mock.MagicMock()
        after = mock.MagicMock()
        after.fetchone.return_value = {"bytes": 200}
        connection.execute.side_effect = [
            existing,
            before,
            vacuum,
            reindex,
            after,
        ]

        report = server_cli._compact_storage(store, ["canonical_chunks"])

        self.assertEqual(report["before_bytes"], 1_000)
        self.assertEqual(report["after_bytes"], 200)
        self.assertEqual(report["reclaimed_bytes"], 800)
        self.assertEqual(
            report["relations"],
            [{
                "relation": "canonical_chunks",
                "before_bytes": 1_000,
                "after_bytes": 200,
                "reclaimed_bytes": 800,
            }],
        )
        connection.execute.assert_any_call(
            'VACUUM (FULL, ANALYZE) public."canonical_chunks"'
        )
        connection.execute.assert_any_call(
            'REINDEX TABLE CONCURRENTLY public."canonical_chunks"'
        )
        self.assertFalse(connection.autocommit)
        self.assertNotIn("content", json.dumps(report))

    def test_storage_compaction_rejects_untrusted_or_duplicate_relations(
        self,
    ) -> None:
        store = mock.MagicMock()

        for relations in (
            [],
            ["canonical_chunks", "canonical_chunks"],
            ["canonical_chunks;drop table canonical_documents"],
            ["CanonicalChunks"],
        ):
            with self.subTest(relations=relations):
                with self.assertRaisesRegex(ValueError, "relation list"):
                    server_cli._compact_storage(store, relations)
        store.connect.assert_not_called()

    def test_storage_compaction_rejects_unknown_relation(self) -> None:
        store = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value
        connection.execute.return_value.fetchall.return_value = []

        with self.assertRaisesRegex(ValueError, "unavailable"):
            server_cli._compact_storage(store, ["canonical_chunks"])

        self.assertFalse(
            any(
                "VACUUM" in str(call.args[0])
                for call in connection.execute.call_args_list
            )
        )

    def test_derived_storage_discard_is_fixed_scope_and_reports_bytes(
        self,
    ) -> None:
        store = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value
        existing = mock.MagicMock()
        existing.fetchall.return_value = [
            {"relname": "canonical_passage_contexts"},
            {"relname": "canonical_passage_embedding_representations"},
        ]
        before = mock.MagicMock()
        before.fetchall.return_value = [
            {
                "relname": "canonical_passage_contexts",
                "total_bytes": 1_000,
            },
            {
                "relname": "canonical_passage_embedding_representations",
                "total_bytes": 2_000,
            },
        ]
        truncate = mock.MagicMock()
        after = mock.MagicMock()
        after.fetchall.return_value = [
            {
                "relname": "canonical_passage_contexts",
                "total_bytes": 100,
            },
            {
                "relname": "canonical_passage_embedding_representations",
                "total_bytes": 200,
            },
        ]
        connection.execute.side_effect = [existing, before, truncate, after]

        report = server_cli._discard_derived_storage(
            store,
            ["canonical_passage_contexts"],
        )

        self.assertEqual(report["before_bytes"], 3_000)
        self.assertEqual(report["after_bytes"], 300)
        self.assertEqual(report["reclaimed_bytes"], 2_700)
        connection.execute.assert_any_call(
            'TRUNCATE TABLE '
            'public."canonical_passage_contexts",'
            'public."canonical_passage_embedding_representations"'
        )
        self.assertNotIn("content", json.dumps(report))

    def test_derived_storage_discard_expands_turn_dependency(self) -> None:
        store = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value
        existing = mock.MagicMock()
        existing.fetchall.return_value = [
            {"relname": "turn_embedding_items"},
            {"relname": "turn_embeddings"},
        ]
        before = mock.MagicMock()
        before.fetchall.return_value = [
            {"relname": "turn_embedding_items", "total_bytes": 10},
            {"relname": "turn_embeddings", "total_bytes": 20},
        ]
        truncate = mock.MagicMock()
        after = mock.MagicMock()
        after.fetchall.return_value = [
            {"relname": "turn_embedding_items", "total_bytes": 1},
            {"relname": "turn_embeddings", "total_bytes": 2},
        ]
        connection.execute.side_effect = [existing, before, truncate, after]

        server_cli._discard_derived_storage(store, ["turn_embeddings"])

        connection.execute.assert_any_call(
            'TRUNCATE TABLE public."turn_embedding_items",'
            'public."turn_embeddings"'
        )

    def test_derived_storage_discard_rejects_authoritative_relations(
        self,
    ) -> None:
        store = mock.MagicMock()

        for relations in (
            [],
            ["item_embeddings", "item_embeddings"],
            ["canonical_documents"],
            ["source_events"],
        ):
            with self.subTest(relations=relations):
                with self.assertRaisesRegex(ValueError, "relation list"):
                    server_cli._discard_derived_storage(store, relations)
        store.connect.assert_not_called()

    def test_empty_legacy_storage_discard_requires_exact_empty_source(
        self,
    ) -> None:
        store = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value
        existing = mock.MagicMock()
        existing.fetchall.return_value = [
            {"relname": relation}
            for relation in server_cli._EMPTY_LEGACY_RELATIONS
        ]
        present = mock.MagicMock()
        present.fetchone.return_value = {"present": True}
        connection.execute.side_effect = [existing, present]

        with self.assertRaisesRegex(ValueError, "source events are not empty"):
            server_cli._discard_empty_legacy_storage(store)

        self.assertFalse(
            any(
                str(call.args[0]).startswith("TRUNCATE")
                for call in connection.execute.call_args_list
            )
        )

    def test_empty_legacy_storage_discard_reclaims_closed_dependency_set(
        self,
    ) -> None:
        store = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value
        relations = server_cli._EMPTY_LEGACY_RELATIONS
        existing = mock.MagicMock()
        existing.fetchall.return_value = [
            {"relname": relation} for relation in relations
        ]
        absent = mock.MagicMock()
        absent.fetchone.return_value = {"present": False}
        before = mock.MagicMock()
        before.fetchall.return_value = [
            {"relname": relation, "total_bytes": 100}
            for relation in relations
        ]
        truncate = mock.MagicMock()
        after = mock.MagicMock()
        after.fetchall.return_value = [
            {"relname": relation, "total_bytes": 10}
            for relation in relations
        ]
        connection.execute.side_effect = [
            existing,
            absent,
            before,
            truncate,
            after,
        ]

        report = server_cli._discard_empty_legacy_storage(store)

        self.assertEqual(report["before_bytes"], 700)
        self.assertEqual(report["after_bytes"], 70)
        self.assertEqual(report["reclaimed_bytes"], 630)
        relation_queries = [
            call
            for call in connection.execute.call_args_list
            if "relname=ANY" in str(call.args[0])
        ]
        self.assertEqual(len(relation_queries), 3)
        self.assertTrue(
            all(
                call.args[1] == (list(relations),)
                for call in relation_queries
            )
        )
        connection.execute.assert_any_call(
            "TRUNCATE TABLE "
            + ",".join(f'public."{name}"' for name in relations)
        )
        self.assertNotIn("content", json.dumps(report))

    def test_covered_legacy_storage_discard_refuses_incomplete_s3_coverage(
        self,
    ) -> None:
        store = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value
        existing = mock.MagicMock()
        existing.fetchall.return_value = [
            {"relname": relation}
            for relation in server_cli._EMPTY_LEGACY_RELATIONS
        ]
        coverage = mock.MagicMock()
        coverage.fetchone.return_value = {
            "total": 10,
            "canonical_identity_covered": 9,
        }
        highwater = mock.MagicMock()
        highwater.fetchone.return_value = {"value": 10}
        uncovered = mock.MagicMock()
        uncovered.__iter__.return_value = iter([])
        connection.execute.side_effect = [
            existing,
            mock.MagicMock(),
            mock.MagicMock(),
            coverage,
            highwater,
            uncovered,
        ]

        with self.assertRaisesRegex(ValueError, "identity coverage is inconsistent"):
            server_cli._discard_covered_legacy_storage(store)

        self.assertFalse(
            any(
                str(call.args[0]).startswith("TRUNCATE")
                for call in connection.execute.call_args_list
            )
        )

    def test_covered_legacy_storage_discard_rechecks_under_locks(self) -> None:
        store = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value
        relations = server_cli._EMPTY_LEGACY_RELATIONS
        existing = mock.MagicMock()
        existing.fetchall.return_value = [
            {"relname": relation} for relation in relations
        ]
        coverage = mock.MagicMock()
        coverage.fetchone.return_value = {
            "total": 10,
            "canonical_identity_covered": 10,
        }
        highwater = mock.MagicMock()
        highwater.fetchone.return_value = {"value": 10}
        uncovered = mock.MagicMock()
        uncovered.__iter__.return_value = iter([])
        before = mock.MagicMock()
        before.fetchall.return_value = [
            {"relname": relation, "total_bytes": 100}
            for relation in relations
        ]
        after = mock.MagicMock()
        after.fetchall.return_value = [
            {"relname": relation, "total_bytes": 10}
            for relation in relations
        ]
        connection.execute.side_effect = [
            existing,
            mock.MagicMock(),
            mock.MagicMock(),
            coverage,
            highwater,
            uncovered,
            before,
            mock.MagicMock(),
            after,
        ]

        report = server_cli._discard_covered_legacy_storage(store)

        self.assertEqual(report["coverage"], {
            "total": 10,
            "canonical_identity_covered": 10,
            "archived_uncovered": 0,
        })
        self.assertEqual(report["reclaimed_bytes"], 630)
        calls = [str(call.args[0]) for call in connection.execute.call_args_list]
        self.assertIn(
            "LOCK TABLE public.source_events IN ACCESS EXCLUSIVE MODE",
            calls,
        )
        self.assertIn(
            "LOCK TABLE public.canonical_events,public.raw_artifacts IN SHARE MODE",
            calls,
        )
        self.assertIn(
            "TRUNCATE TABLE "
            + ",".join(f'public."{name}"' for name in relations),
            calls,
        )
        coverage_sql = calls[3]
        self.assertIn("JOIN raw_artifacts artifact", coverage_sql)
        self.assertNotIn("event.tenant_id=%s", coverage_sql)
        self.assertNotIn("content", json.dumps(report))

    def test_covered_legacy_storage_discard_drops_temporary_index(self) -> None:
        store = mock.MagicMock()
        report = {"status": "ok"}
        with (
            mock.patch.object(server_cli, "_create_legacy_coverage_index") as create,
            mock.patch.object(server_cli, "_drop_legacy_coverage_index") as drop,
            mock.patch.object(
                server_cli,
                "_discard_covered_legacy_storage",
                return_value=report,
            ) as discard,
        ):
            self.assertIs(
                server_cli._discard_covered_legacy_storage_indexed(store),
                report,
            )
        create.assert_called_once_with(store)
        discard.assert_called_once_with(store)
        drop.assert_called_once_with(store)

        with (
            mock.patch.object(server_cli, "_create_legacy_coverage_index"),
            mock.patch.object(server_cli, "_drop_legacy_coverage_index") as drop,
            mock.patch.object(
                server_cli,
                "_discard_covered_legacy_storage",
                side_effect=ValueError("refused"),
            ),
        ):
            with self.assertRaisesRegex(ValueError, "refused"):
                server_cli._discard_covered_legacy_storage_indexed(store)
        drop.assert_called_once_with(store)

    def test_storage_authority_audit_requires_complete_database_coverage(
        self,
    ) -> None:
        store = mock.MagicMock()
        connection = store.connect.return_value.__enter__.return_value

        def result(row: dict[str, int]) -> mock.MagicMock:
            query = mock.MagicMock()
            query.fetchone.return_value = row
            return query

        connection.execute.side_effect = [
            result({"total": 7, "canonical_covered": 7}),
            result({"total": 5, "s3_raw_covered": 5}),
            result({"total": 3, "projected": 3}),
            result({"total": 3, "s3_pointer_complete": 3, "passage_projected": 3}),
            result({"logical": 0, "passage": 0, "cleanup": 2}),
        ]

        report = server_cli._storage_authority_audit(
            store,
            "tenant:company:parcha",
        )

        legacy_call = connection.execute.call_args_list[0]
        self.assertEqual(len(legacy_call.args), 1)
        self.assertNotIn("event.tenant_id=%s", legacy_call.args[0])
        self.assertTrue(report["database_coverage_complete"])
        self.assertTrue(report["object_verification_required"])
        self.assertEqual(report["queues"]["cleanup"], 2)
        self.assertNotIn("content", json.dumps(report))

        connection.execute.side_effect = [
            result({"total": 7, "canonical_covered": 6}),
            result({"total": 5, "s3_raw_covered": 5}),
            result({"total": 3, "projected": 2}),
            result({"total": 2, "s3_pointer_complete": 2, "passage_projected": 2}),
            result({"logical": 1, "passage": 0, "cleanup": 0}),
        ]

        incomplete = server_cli._storage_authority_audit(
            store,
            "tenant:company:parcha",
        )
        self.assertFalse(incomplete["database_coverage_complete"])
        self.assertFalse(incomplete["object_verification_required"])

    def test_worker_pool_matches_actual_concurrency_floor(self) -> None:
        self.assertEqual(
            server_cli._worker_pool_max_size(argparse.Namespace(
                command="projection-worker",
                passage_concurrency=4,
                upload_concurrency=4,
            )),
            4,
        )
        self.assertEqual(
            server_cli._worker_pool_max_size(argparse.Namespace(
                command="projection-worker",
                passage_concurrency=6,
                upload_concurrency=4,
            )),
            6,
        )
        self.assertEqual(
            server_cli._worker_pool_max_size(argparse.Namespace(
                command="lossless-passage-worker",
                concurrency=2,
            )),
            4,
        )

    def test_database_pool_size_is_bounded_deployment_config(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"RECALL_DATABASE_POOL_MAX_SIZE": "20"},
        ):
            store = BrainStore("postgresql://synthetic.invalid/recall")
        self.assertEqual(store.pool_max_size, 20)
        for invalid in (3, 33):
            with self.assertRaisesRegex(ValueError, "database pool size"):
                BrainStore(
                    "postgresql://synthetic.invalid/recall",
                    pool_max_size=invalid,
                )

    def test_similarity_floor_is_explicit_validated_deployment_config(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"RECALL_SEMANTIC_MINIMUM_SIMILARITY": "0.25"},
        ):
            store = BrainStore("postgresql://synthetic.invalid/recall")
        self.assertEqual(store.semantic_minimum_similarity, 0.25)
        with self.assertRaisesRegex(ValueError, "semantic minimum similarity"):
            BrainStore(
                "postgresql://synthetic.invalid/recall",
                semantic_minimum_similarity=1.01,
            )

    def test_dense_candidate_pool_is_bounded_and_keeps_anchor_headroom(self) -> None:
        self.assertEqual(semantic_candidate_limit(1), 20)
        self.assertEqual(semantic_candidate_limit(5), 20)
        self.assertEqual(semantic_candidate_limit(10), 40)
        self.assertEqual(semantic_candidate_limit(50), 100)


class RelatedRetrievalContractTest(unittest.TestCase):
    def test_fast_candidate_pool_is_bounded_with_filter_headroom(self) -> None:
        self.assertEqual(related_candidate_limit(1), 100)
        self.assertEqual(related_candidate_limit(5), 100)
        self.assertEqual(related_candidate_limit(10), 200)
        self.assertEqual(related_candidate_limit(20), 400)

    def test_fast_query_uses_indexable_evidence_join_and_candidate_cap(self) -> None:
        connection = mock.MagicMock()
        connection.execute.return_value.fetchall.return_value = [{
            "source_id": "synthetic:source",
            "native_id": "session-1",
            "metadata": {"cwd": "/workspace/recall", "branch": "main"},
            "ended_at": None,
            "path": "/workspace/recall/session.jsonl",
            "receipt": "recall://synthetic/session?rev=1#item=0",
            "overlap": 2,
        }]
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock(
            return_value=contextlib.nullcontext(connection)
        )

        result = store.related(
            cwd="/workspace/recall",
            branch="main",
            limit=3,
            mains_only=True,
            fast=True,
            authorized_source=["synthetic:source"],
        )

        sql, params = connection.execute.call_args.args
        normalized = " ".join(sql.split())
        self.assertIn("WITH candidates AS MATERIALIZED", normalized)
        self.assertIn("JOIN source_events event ON event.id=i.event_id", normalized)
        self.assertNotIn("COALESCE(se.native_parent_id,se.native_id)", normalized)
        self.assertIn("LIMIT %s", normalized)
        self.assertEqual(params[-2:], [100, 3])
        self.assertEqual(result["results"][0]["overlap"], 2)


class SemanticRetrievalContractTest(unittest.TestCase):
    def test_turn_projection_keeps_only_the_latest_exact_prompt_per_session(
        self,
    ) -> None:
        implementation = inspect.getsource(BrainStore.embed_pending_turns)
        latest_anchors = implementation.split(
            "WITH latest_anchors AS MATERIALIZED", 1,
        )[1].split("SELECT anchor.id AS anchor_item_id", 1)[0]
        latest_anchors = " ".join(latest_anchors.split())

        self.assertIn("max(candidate_anchor.id) AS anchor_item_id", latest_anchors)
        self.assertIn(
            "GROUP BY candidate_anchor.source_id, "
            "candidate_anchor.session_native_id, "
            "candidate_anchor.text_redacted",
            latest_anchors,
        )
        self.assertIn("ORDER BY max(candidate_anchor.id)", latest_anchors)
        self.assertIn(
            "DELETE FROM turn_embeddings older_embedding",
            implementation,
        )
        self.assertIn("older_anchor.text_redacted=%s", implementation)
        self.assertLess(
            implementation.index("_dedupe_turn_embeddings_for_session("),
            implementation.index(
                "DELETE FROM turn_embedding_dirty_sessions",
            ),
        )


    def test_turn_projection_uses_one_indexable_response_range(self) -> None:
        implementation = inspect.getsource(BrainStore.embed_pending_turns)
        response_range = implementation.split(
            "FROM items candidate", 1,
        )[1].split("HAVING count(*)>0", 1)[0]
        response_range = " ".join(response_range.split())

        self.assertNotIn("next_user.id IS NULL", response_range)
        self.assertIn(
            "COALESCE( next_user.occurred_at, 'infinity'::timestamptz )",
            response_range,
        )
        self.assertIn(
            "COALESCE( next_user.id,9223372036854775807 )",
            response_range,
        )

    def test_turn_projection_preserves_question_head_and_final_answer_tail(self) -> None:
        rendered = turn_embedding_text(
            "Why did we choose the managed database?",
            ["First progress update.", "The final decision and reason."],
        )

        self.assertTrue(rendered.startswith(
            "User request:\nWhy did we choose the managed database?"
        ))
        self.assertTrue(rendered.endswith("The final decision and reason."))
        self.assertIn("\n\nAssistant continuation:\n", rendered)

    def test_turn_candidate_limit_is_applied_after_authorization_filters(self) -> None:
        implementation = inspect.getsource(BrainStore._turn_semantic_leg)
        nearest = implementation.split("WITH nearest AS MATERIALIZED", 1)[1]
        nearest = nearest.split("LIMIT %s", 1)[0]

        self.assertIn("JOIN items i ON i.id=embedding.response_item_id", nearest)
        self.assertIn("AND {where}", nearest)

    def test_search_results_bound_large_evidence_and_keep_show_receipt(self) -> None:
        exact, exact_truncated = bounded_search_text("x" * 4096)
        oversized, oversized_truncated = bounded_search_text("🙂" * 4097)

        self.assertEqual(exact, "x" * 4096)
        self.assertFalse(exact_truncated)
        self.assertEqual(oversized, "🙂" * 4096)
        self.assertTrue(oversized_truncated)
        implementation = inspect.getsource(BrainStore.search)
        self.assertIn("bounded_search_text", implementation)
        self.assertIn('"text_truncated"', implementation)

    @mock.patch("recall_server.db.ConnectionPool")
    def test_brainstore_reuses_a_bounded_connection_pool(self, pool_type) -> None:
        pool = pool_type.return_value
        first = object()
        second = object()
        pool.connection.side_effect = [first, second]
        store = BrainStore("postgresql://synthetic.invalid/recall")

        self.assertIs(store.connect(), first)
        self.assertIs(store.connect(), second)
        pool_type.assert_called_once()
        self.assertEqual(pool_type.call_args.kwargs["min_size"], 1)
        self.assertEqual(pool_type.call_args.kwargs["max_size"], 8)

        store.close()
        pool.close.assert_called_once_with()

    def test_optional_rescues_share_a_strict_sub_budget(self) -> None:
        self.assertEqual(
            optional_rescue_deadline(
                overall_deadline_at=20.0,
                now=10.0,
                search_deadline_ms=5000,
            ),
            10.8,
        )
        self.assertEqual(
            optional_rescue_deadline(
                overall_deadline_at=10.4,
                now=10.0,
                search_deadline_ms=5000,
            ),
            10.4,
        )

    def test_rescue_stops_after_enough_distinct_session_anchors(self) -> None:
        rows = [
            {"source_id": "one", "session_native_id": "a"},
            {"source_id": "one", "session_native_id": "a"},
            {"source_id": "one", "session_native_id": "b"},
            {"source_id": "two", "session_native_id": "a"},
            {"source_id": "two", "session_native_id": "b"},
        ]
        self.assertTrue(enough_session_anchors(rows, result_limit=5))
        self.assertFalse(enough_session_anchors(rows[:3], result_limit=5))

    def test_exact_prior_question_skips_optional_lexical_rescues(self) -> None:
        sparse = [{"source_id": "one", "session_native_id": "a"}]
        self.assertFalse(should_run_optional_rescue(
            exact_question_count=1,
            rows=sparse,
            result_limit=5,
        ))
        self.assertTrue(should_run_optional_rescue(
            exact_question_count=0,
            rows=sparse,
            result_limit=5,
        ))

    def test_exact_question_precedes_identifier_routing_for_answer_promotion(self) -> None:
        implementation = inspect.getsource(BrainStore.search)
        self.assertLess(
            implementation.index("exact_question_rows = run_leg("),
            implementation.index("if identifiers:"),
        )
        self.assertEqual(
            retrieval_leg_order(["ticket-123"]),
            ("exact-question", "entity", "identifier"),
        )
        self.assertNotIn("if not identifiers and candidates:", implementation)

    def test_similarity_threshold_is_applied_after_bounded_hnsw_retrieval(self) -> None:
        runtime = mock.Mock(
            model="voyage-4",
            fingerprint="voyage:synthetic",
            dimensions=512,
        )
        store = BrainStore(
            "postgresql://synthetic.invalid/recall",
            semantic_runtime=runtime,
        )
        cursor = mock.MagicMock()
        cursor.fetchall.return_value = [
            {"id": 1, "lexical_rank": 0.81},
            {"id": 2, "lexical_rank": 0.22},
        ]
        store._execute_bounded = mock.MagicMock(return_value=cursor)
        connection = mock.MagicMock()

        rows = store._semantic_leg(
            connection,
            [0.1] * 512,
            {},
            limit=5,
            minimum_similarity=0.35,
        )

        _connection, sql, values, _deadline = (
            store._execute_bounded.call_args.args
        )
        self.assertNotIn(
            "AND 1-(embedding.embedding <=> %s::halfvec) >=",
            sql,
        )
        self.assertNotIn(0.35, values)
        self.assertEqual(
            rows,
            [{"id": 1, "lexical_rank": 0.81, "leg": "semantic", "tier": 2}],
        )


class AdminCliSafetyTest(unittest.TestCase):
    def test_token_creation_requires_a_private_output_file(self) -> None:
        errors = io.StringIO()
        with mock.patch.object(sys, "argv", [
            "recall-server", "--dsn", "postgresql://synthetic.invalid/db",
            "token-create", "synthetic-collector",
        ]), contextlib.redirect_stderr(errors), self.assertRaises(SystemExit) as raised:
            server_cli.main()
        self.assertEqual(raised.exception.code, 2)
        self.assertIn("--output", errors.getvalue())
        self.assertNotIn("rcl_", errors.getvalue())

    def test_write_credentials_must_be_bound_to_one_source(self) -> None:
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock()
        with self.assertRaisesRegex(ValueError, "write credential requires a source"):
            store.create_collector_token(
                "synthetic-unbound-writer",
                None,
                ["read", "write"],
                principal_id="synthetic-owner",
            )
        store.connect.assert_not_called()

    def test_canonical_credentials_bind_tenant_principal_source_and_write(self) -> None:
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock()
        invalid = (
            ("tenant:one", None, "source:one", ["write"]),
            ("tenant:one", "principal:owner", None, ["write"]),
            ("tenant:one", "principal:owner", "source:one", ["read"]),
            ("tenant with spaces", "principal:owner", "source:one", ["write"]),
        )
        for tenant_id, principal_id, source_id, scopes in invalid:
            with self.subTest(tenant_id=tenant_id), self.assertRaises(ValueError):
                store.create_collector_token(
                    "synthetic-canonical",
                    source_id,
                    scopes,
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                )
        store.connect.assert_not_called()

    def test_webhook_credentials_require_source_principal_and_privacy(self) -> None:
        store = BrainStore("postgresql://synthetic.invalid/recall")
        store.connect = mock.MagicMock()
        invalid = (
            (None, "synthetic-owner", "scrub"),
            ("synthetic:webhook", None, "scrub"),
            ("synthetic:webhook", "synthetic-owner", None),
            ("synthetic:webhook", "synthetic-owner", "off"),
        )
        for source_id, principal_id, privacy_mode in invalid:
            with self.subTest(
                source_id=source_id,
                principal_id=principal_id,
                privacy_mode=privacy_mode,
            ), self.assertRaisesRegex(ValueError, "invalid webhook credential"):
                store.create_collector_token(
                    "synthetic-webhook",
                    source_id,
                    ["webhook"],
                    principal_id=principal_id,
                    webhook_privacy_mode=privacy_mode,
                )
        with self.assertRaisesRegex(ValueError, "invalid webhook credential"):
            store.create_collector_token(
                "synthetic-read",
                "synthetic:webhook",
                ["read"],
                principal_id="synthetic-owner",
                webhook_privacy_mode="scrub",
            )
        with self.assertRaisesRegex(ValueError, "invalid webhook credential"):
            store.create_collector_token(
                "synthetic-combined",
                "synthetic:webhook",
                ["read", "webhook"],
                principal_id="synthetic-owner",
                webhook_privacy_mode="scrub",
            )
        store.connect.assert_not_called()


class EnvelopeContractTest(unittest.TestCase):
    def test_cowork_session_fallback_repairs_legacy_parentless_envelopes(self) -> None:
        legacy = envelope(
            source_id="cowork:mac:synthetic",
            native_id="session-alpha/message-one",
            native_parent_id=None,
            kind="connector_record",
            content={"session_id": "session-alpha", "role": "user", "text": "safe"},
            provenance={"connector_id": "anthropic.cowork-local"},
        )
        self.assertEqual(effective_session_id(legacy), "session-alpha")
        legacy["native_parent_id"] = legacy["native_id"]
        self.assertEqual(effective_session_id(legacy), "session-alpha")
        self.assertEqual(effective_session_id(envelope(native_parent_id=None)), "session-1:turn-1")

    def test_default_deadline_fits_below_tailnet_slo_with_client_headroom(self) -> None:
        self.assertEqual(DEFAULT_SEARCH_DEADLINE_MS, 300)
        self.assertLess(DEFAULT_SEARCH_DEADLINE_MS, 500)

    def test_large_remote_corpora_may_select_a_bounded_thirty_second_deadline(self) -> None:
        self.assertEqual(
            BrainStore(
                "postgresql://synthetic.invalid/recall",
                search_deadline_ms=30_000,
            ).search_deadline_ms,
            30_000,
        )
        with self.assertRaisesRegex(ValueError, "between 10 and 30000"):
            BrainStore(
                "postgresql://synthetic.invalid/recall",
                search_deadline_ms=30_001,
            )

    def test_advisory_lock_key_is_postgres_text_safe_and_boundary_preserving(self) -> None:
        key = advisory_lock_key("ab", "c")
        self.assertNotIn("\x00", key)
        self.assertNotEqual(key, advisory_lock_key("a", "bc"))

    def test_canonical_hash_ignores_dict_order(self) -> None:
        self.assertEqual(canonical_json({"b": 2, "a": 1}), canonical_json({"a": 1, "b": 2}))

    def test_validation_rejects_mutation_and_unknown_visibility(self) -> None:
        valid = envelope()
        self.assertIs(validate_envelope(valid), valid)
        mutated = {**valid, "content": {"text": "changed"}}
        with self.assertRaisesRegex(ValueError, "content_sha256 mismatch"):
            validate_envelope(mutated)
        with self.assertRaisesRegex(ValueError, "visibility"):
            validate_envelope(envelope(visibility="public"))

    def test_validation_rejects_ambiguous_identifiers_nonfinite_json_and_bad_tombstones(self) -> None:
        for updates in (
            {"source_id": "bad/source"},
            {"native_id": "ambiguous?rev=7"},
            {"native_id": "ambiguous#item=2"},
            {"occurred_at": "not-a-timestamp"},
            {"extra": "open-schema"},
        ):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                validate_envelope(envelope(**updates))
        nonfinite = envelope()
        nonfinite["content"] = {"value": float("nan")}
        with self.assertRaisesRegex(ValueError, "finite JSON"):
            validate_envelope(nonfinite)
        with self.assertRaisesRegex(ValueError, "tombstone target"):
            validate_envelope(envelope(
                kind="tombstone", native_id="memory-one",
                content={"target_native_id": "memory-two"},
            ))

    def test_projection_redacts_secret_line_and_preserves_safe_lines(self) -> None:
        value = envelope(content={"role": "user", "text": "safe line\nAuthorization=supersecretvalue123\nlast line"})
        items, _ = project(value, 1)
        self.assertEqual(items[0]["text_redacted"], "safe line\n[REDACTED]\nlast line")
        self.assertNotIn("supersecret", json.dumps(items))

    def test_receipts_are_source_native_revision_and_item_exact(self) -> None:
        items, _ = project(envelope(), 3)
        self.assertEqual(items[0]["receipt"], "recall://codex:laptop/session-1:turn-1?rev=3#item=0")

    def test_projection_carries_shared_normalized_entities(self) -> None:
        marker = "DEADBEEF-1234-1234-1234-123456789ABC"
        items, _ = project(envelope(content={"role": "user", "text": f"/tmp/trace.json {marker} ConnectTimeout"}), 1)
        self.assertIn({"kind": "file_path", "value": "/tmp/trace.json", "normalized": "/tmp/trace.json"}, items[0]["entities"])
        self.assertIn({"kind": "uuid", "value": marker.lower(), "normalized": marker.lower()}, items[0]["entities"])
        self.assertIn({"kind": "error", "value": "ConnectTimeout", "normalized": "connecttimeout"}, items[0]["entities"])

    def test_projection_redacts_secret_shaped_native_tool_entity(self) -> None:
        secret = "Z" * 40
        value = envelope(
            kind="transcript_record",
            provenance={"harness": "claude"},
            content={
                "type": "assistant", "timestamp": "2026-07-12T20:00:00Z",
                "message": {"content": [{
                    "type": "tool_use", "name": "api_key=" + secret, "input": {"path": "safe.txt"},
                }]},
            },
        )
        items, _ = project(value, 1)
        rendered = json.dumps(items)
        self.assertNotIn(secret, rendered)
        self.assertIn("REDACTED", rendered)

    def test_redaction_does_not_use_semantic_matching(self) -> None:
        self.assertEqual(redact_text("a harmless discussion about password rotation"), "a harmless discussion about password rotation")

    def test_redaction_removes_private_key_blocks_and_generic_key_assignments(self) -> None:
        private_value = "Z" * 50
        private_block = (
            "-----BEGIN " + "PRIVATE KEY-----\n" + ("Q" * 256)
            + "\n-----END " + "PRIVATE KEY-----"
        )
        redacted = redact_text("safe\nkey=" + private_value + "\n" + private_block + "\nend")
        self.assertNotIn(private_value, redacted)
        self.assertNotIn("Q" * 64, redacted)
        self.assertIn("safe", redacted)
        self.assertIn("end", redacted)

    def test_redaction_is_idempotent_after_nested_secret_markers(self) -> None:
        secret = "SyntheticSecret" + "9" * 40
        value = (
            "safe prefix\n"
            "access_token:\n"
            f", Authorization={secret}\n"
            "safe suffix"
        )

        once = redact_text(value)

        self.assertNotIn(secret, once)
        self.assertEqual(redact_text(once), once)

    def test_partial_probes_prefer_structural_anchors_and_are_bounded(self) -> None:
        probes = partial_lexical_probes(
            ["foreign-key", "violation", "check_result", "agent_instance_id"],
            has_time_filter=False,
        )
        self.assertEqual(probes[0], ("agent_instance_id check_result", "pair", 2))
        self.assertIn(("agent_instance_id", "anchor", 2), probes)
        self.assertLessEqual(len(probes), 3)
        self.assertEqual(
            partial_lexical_probes(["greptile", "review", "passes"], has_time_filter=True)[-1],
            ("greptile", "time-anchor", 1),
        )

    def test_compact_parser_phrase_is_preferred_over_the_full_question(self) -> None:
        self.assertEqual(
            preferred_phrase_probe([
                "what was that 504 gateway timeout we hit from a tool call",
                "timeout we hit from a tool", "504 gateway timeout",
            ]),
            "504 gateway timeout",
        )

        self.assertEqual(
            preferred_phrase_probe([
                "the sqlalchemy async greenlet_spawn has not been called error",
                "greenlet_spawn has not been called",
                "sqlalchemy async greenlet_spawn",
                "async greenlet_spawn has",
            ]),
            "greenlet_spawn has not been called",
        )
        self.assertEqual(
            preferred_phrase_probe([
                "where we handled the httpx ConnectTimeout transient dispatch error",
                "ConnectTimeout transient dispatch error",
                "the httpx ConnectTimeout",
                "transient dispatch error",
            ]),
            "ConnectTimeout transient dispatch error",
        )
        self.assertEqual(
            preferred_phrase_probes([
                "where we handled the httpx ConnectTimeout transient dispatch error",
                "ConnectTimeout transient dispatch error",
                "the httpx ConnectTimeout",
                "transient dispatch error",
            ]),
            ["ConnectTimeout transient dispatch error", "transient dispatch error"],
        )
        self.assertEqual(
            preferred_phrase_probe([
                "the foreign-key violation on check_result for agent_instance_id",
                "violation on check_result for agent_instance_id",
                "the foreign-key violation",
            ]),
            "violation on check_result for agent_instance_id",
        )

    def test_identifier_plan_runs_exact_legs_before_any_phrase(self) -> None:
        self.assertEqual(
            retrieval_leg_order(["api-prod-6fcdc84dd4-mmjpj"]),
            ("exact-question", "entity", "identifier"),
        )
        self.assertEqual(
            retrieval_leg_order([]),
            ("exact-question", "semantic", "phrase", "entity", "partial", "all"),
        )

    def test_sentence_punctuation_is_not_part_of_an_identifier(self) -> None:
        engine = __import__("recall_server.projectors", fromlist=["legacy_engine"]).legacy_engine()
        terms = engine.informative_terms("Gather synthesis-marker-01.")
        self.assertIn("synthesis-marker-01", terms)
        self.assertNotIn("synthesis-marker-01.", terms)

    def test_sparse_phrase_candidates_do_not_suppress_structural_fallback(self) -> None:
        self.assertTrue(should_run_partial(candidate_count=1, result_limit=10))
        self.assertFalse(should_run_partial(candidate_count=29, result_limit=10))

    def test_phrase_fallbacks_share_one_bounded_database_leg(self) -> None:
        self.assertEqual(
            phrase_query_spec(["ConnectTimeout transient dispatch error", "transient dispatch error"]),
            ('"ConnectTimeout transient dispatch error" OR "transient dispatch error"', "websearch_to_tsquery"),
        )
        self.assertEqual(phrase_query_spec(["504 gateway timeout"]), ("504 gateway timeout", "phraseto_tsquery"))

    def test_rank_components_keep_identifier_phrase_and_error_evidence_distinct(self) -> None:
        exact_answer = evidence_rank_components(
            legs={"exact-answer"}, surface="assistant", lexical_rank=1.0,
            matched_count=4, informative_count=4, has_identifier=True,
            recency_factor=0.5,
        )
        identifier = evidence_rank_components(
            legs={"entity"}, surface="tool_output", lexical_rank=1.0,
            matched_count=1, informative_count=8, has_identifier=True,
            recency_factor=0.5,
        )
        phrase_command = evidence_rank_components(
            legs={"phrase"}, surface="tool_input", lexical_rank=0.2,
            matched_count=4, informative_count=6, has_identifier=False,
            recency_factor=0.5,
        )
        phrase_echo = evidence_rank_components(
            legs={"phrase"}, surface="tool_output", lexical_rank=0.8,
            matched_count=4, informative_count=6, has_identifier=False,
            recency_factor=0.5,
        )
        error_entity = evidence_rank_components(
            legs={"entity"}, surface="tool_input", lexical_rank=1.0,
            matched_count=1, informative_count=6, has_identifier=False,
            recency_factor=1.0,
        )
        answer = evidence_rank_components(
            legs={"answer"}, surface="message", lexical_rank=0.2,
            matched_count=0, informative_count=6, has_identifier=False,
            recency_factor=0.5,
        )
        self.assertEqual(identifier["evidence_class"], "identifier")
        self.assertEqual(answer["evidence_class"], "answer")
        self.assertGreater(tuple(identifier["rank_key"]), tuple(answer["rank_key"]))
        self.assertGreater(
            tuple(exact_answer["rank_key"]),
            tuple(identifier["rank_key"]),
        )
        self.assertGreater(tuple(answer["rank_key"]), tuple(phrase_command["rank_key"]))
        self.assertGreater(tuple(phrase_command["rank_key"]), tuple(phrase_echo["rank_key"]))
        self.assertGreater(tuple(phrase_echo["rank_key"]), tuple(error_entity["rank_key"]))
        semantic = evidence_rank_components(
            legs={"semantic", "rewrite"}, surface="assistant", lexical_rank=0.5,
            matched_count=0, informative_count=6, has_identifier=False,
            recency_factor=0.5, fusion_score=2 / 61,
        )
        self.assertEqual(semantic["evidence_class"], "semantic")
        self.assertGreater(semantic["fusion_score"], 0)


if __name__ == "__main__":
    unittest.main()
