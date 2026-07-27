from __future__ import annotations

import ipaddress
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from . import SCHEMA_VERSION


MIN_POSTGRES_MAJOR = 16
MIN_VECTOR_VERSION = (0, 8, 0)
PROFILES = {"production", "local-fixture"}

CAPABILITY_SQL = """
WITH expected_tables(name) AS (
    VALUES
        ('schema_migrations'), ('sources'), ('source_grants'), ('ingest_batches'),
        ('source_events'), ('sessions'), ('items'), ('chunks'), ('dead_letters'),
        ('audit_events'), ('projection_watermarks'), ('collector_credentials'),
        ('entities'), ('projection_backfills'), ('source_profiles'),
        ('session_export_cursors'), ('source_aliases'), ('item_embeddings'),
        ('embedding_projection_watermarks'), ('turn_embeddings'),
        ('turn_embedding_items'), ('turn_embedding_projection_watermarks'),
        ('turn_embedding_dirty_sessions'),
        ('brain_tenants'), ('brain_principals'), ('canonical_sources'),
        ('raw_artifacts'), ('canonical_events'), ('canonical_documents'),
        ('canonical_chunks'), ('canonical_ingest_jobs'), ('receipt_redirects'),
        ('forget_tombstones'), ('canonical_audit_events'),
        ('canonical_evidence_objects'),
        ('canonical_evidence_documents'),
        ('canonical_evidence_document_parts'),
        ('canonical_evidence_document_queue'),
        ('canonical_evidence_cleanup_queue'),
        ('brain_organizations'), ('brain_spaces'), ('brain_memberships'),
        ('brain_access_grants'), ('canonical_source_grants'),
        ('mcp_credentials'), ('canonical_chunk_embeddings'),
        ('canonical_embedding_projection_watermarks'),
        ('admin_credentials'), ('admin_sessions'), ('provider_connections'),
        ('connector_installations'), ('oauth_sessions'), ('control_audit_events'),
        ('agent_runs')
), runtime_tables(name) AS (
    SELECT name FROM expected_tables WHERE name <> 'schema_migrations'
), expected_sequences(name) AS (
    VALUES ('source_events_id_seq'), ('items_id_seq'), ('chunks_id_seq'),
           ('dead_letters_id_seq'), ('audit_events_id_seq')
)
SELECT
    current_setting('server_version_num')::integer AS server_version_num,
    (SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'vector') AS vector_version,
    COALESCE(
        (SELECT array_agg(version ORDER BY version) FROM public.schema_migrations),
        ARRAY[]::integer[]
    ) AS migration_versions,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_stat_ssl
        WHERE pid = pg_catalog.pg_backend_pid() AND ssl
    ) AS ssl_in_use,
    role.rolsuper AS superuser,
    role.rolcreatedb AS create_database,
    role.rolcreaterole AS create_role,
    role.rolreplication AS replication,
    role.rolbypassrls AS bypass_rls,
    pg_catalog.has_database_privilege(current_database(), 'CONNECT') AS can_connect,
    pg_catalog.has_schema_privilege(current_user, 'public', 'USAGE') AS can_use_schema,
    (SELECT count(*) = 54 AND COALESCE(bool_and(
        pg_catalog.to_regclass(pg_catalog.format('public.%I', name)) IS NOT NULL
        AND pg_catalog.has_table_privilege(
            current_user, pg_catalog.to_regclass(pg_catalog.format('public.%I', name)), 'SELECT'
        )
    ), false) FROM expected_tables) AS can_read_runtime_tables,
    (SELECT count(*) = 53 AND COALESCE(bool_and(
        pg_catalog.to_regclass(pg_catalog.format('public.%I', name)) IS NOT NULL
        AND pg_catalog.has_table_privilege(
            current_user, pg_catalog.to_regclass(pg_catalog.format('public.%I', name)),
            'INSERT,UPDATE,DELETE'
        )
    ), false) FROM runtime_tables) AS can_write_runtime_tables,
    (SELECT count(*) = 5 AND COALESCE(bool_and(
        pg_catalog.to_regclass(pg_catalog.format('public.%I', name)) IS NOT NULL
        AND pg_catalog.has_sequence_privilege(
            current_user, pg_catalog.to_regclass(pg_catalog.format('public.%I', name)),
            'USAGE,SELECT'
        )
    ), false) FROM expected_sequences) AS can_use_runtime_sequences,
    pg_catalog.has_table_privilege(current_user, 'public.schema_migrations', 'SELECT')
    AND NOT pg_catalog.has_table_privilege(current_user, 'public.schema_migrations', 'INSERT')
    AND NOT pg_catalog.has_table_privilege(current_user, 'public.schema_migrations', 'UPDATE')
    AND NOT pg_catalog.has_table_privilege(current_user, 'public.schema_migrations', 'DELETE')
    AND NOT pg_catalog.has_table_privilege(current_user, 'public.schema_migrations', 'TRUNCATE')
        AS schema_migrations_readonly,
    NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_roles AS inherited
        WHERE inherited.rolname <> current_user
          AND pg_catalog.pg_has_role(current_user, inherited.oid, 'MEMBER')
          AND inherited.rolname NOT IN ('pg_read_all_data', 'pg_write_all_data')
    ) AS role_memberships_safe
FROM pg_catalog.pg_roles AS role
WHERE role.rolname = current_user
"""


class CapabilityError(RuntimeError):
    """A content-free, stable failure from the database capability gate."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _loopback(value: str | None) -> bool:
    if value in {"localhost", "127.0.0.1", "::1"}:
        return True
    try:
        return bool(value and ipaddress.ip_address(value).is_loopback)
    except ValueError:
        return False


def validate_connection_policy(dsn: str, profile: str = "production") -> dict[str, str]:
    if profile not in PROFILES:
        raise CapabilityError("profile_unsupported")
    try:
        parsed = urlsplit(dsn)
        query = parse_qs(parsed.query, strict_parsing=True)
        if any(len(values) != 1 for values in query.values()):
            raise ValueError("duplicate connection parameter")
        parameters = {key: values[-1] for key, values in query.items()}
        parameters["host"] = parsed.hostname or ""
        if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
            raise ValueError("not a PostgreSQL URL")
    except (TypeError, ValueError) as error:
        raise CapabilityError("dsn_invalid") from error
    if profile == "local-fixture":
        if not _loopback(parameters.get("host")):
            raise CapabilityError("fixture_not_loopback")
        return {"profile": profile, "tls": "fixture-only"}
    root = parameters.get("sslrootcert")
    root_is_trusted = root == "system" or bool(root and Path(root).is_absolute())
    if parameters.get("sslmode") != "verify-full" or not root_is_trusted:
        raise CapabilityError("tls_policy_failed")
    return {"profile": profile, "tls": "verify-full"}


def _version_tuple(value: object) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in str(value).split(".")[:3])
    except (TypeError, ValueError):
        return ()


def assess_snapshot(snapshot: dict[str, Any], profile: str = "production") -> dict[str, Any]:
    if profile not in PROFILES:
        raise CapabilityError("profile_unsupported")
    try:
        server_version = int(snapshot.get("server_version_num") or 0)
    except (TypeError, ValueError) as error:
        raise CapabilityError("postgres_unsupported") from error
    postgres_major = server_version // 10000
    if postgres_major < MIN_POSTGRES_MAJOR:
        raise CapabilityError("postgres_unsupported")
    vector_version = snapshot.get("vector_version")
    if vector_version is None:
        raise CapabilityError("extension_missing")
    if _version_tuple(vector_version) < MIN_VECTOR_VERSION:
        raise CapabilityError("extension_unsupported")
    if list(snapshot.get("migration_versions") or []) != list(range(1, SCHEMA_VERSION + 1)):
        raise CapabilityError("schema_drift")
    if profile == "production" and snapshot.get("ssl_in_use") is not True:
        raise CapabilityError("tls_not_active")
    role = snapshot.get("role") or {}
    if set(role) != {
        "superuser", "create_database", "create_role", "replication", "bypass_rls",
    } or any(value is not False for value in role.values()):
        raise CapabilityError("role_privilege_excessive")
    privileges = snapshot.get("privileges") or {}
    if set(privileges) != {
        "connect", "schema_usage", "read_runtime_tables", "write_runtime_tables",
        "use_runtime_sequences", "schema_migrations_readonly", "role_memberships_safe",
    } or any(value is not True for value in privileges.values()):
        raise CapabilityError("role_privilege_insufficient")
    return {
        "status": "ready" if profile == "production" else "fixture-ready",
        "profile": profile,
        "postgres_major": postgres_major,
        "schema_version": SCHEMA_VERSION,
        "extensions": {"vector": str(vector_version)},
        "role": "least-privilege-runtime",
        "tls": "verified" if profile == "production" else "fixture-only",
    }


def _client_tls_in_use(connection: Any) -> bool:
    try:
        return bool(connection.pgconn.ssl_in_use)
    except (AttributeError, TypeError):
        return False


def _connection_failure_code(error: BaseException) -> str:
    sqlstate = getattr(error, "sqlstate", None)
    if isinstance(sqlstate, str) and sqlstate.startswith("28"):
        return "database_auth_failed"
    return "database_connection_failed"


def probe_database(dsn: str, profile: str = "production") -> dict[str, Any]:
    validate_connection_policy(dsn, profile)
    try:
        import psycopg
        from psycopg.rows import dict_row
    except Exception as error:
        raise CapabilityError("database_driver_unavailable") from error
    try:
        connection = psycopg.connect(dsn, row_factory=dict_row)
    except Exception as error:
        raise CapabilityError(_connection_failure_code(error)) from error
    try:
        with connection:
            row = connection.execute(CAPABILITY_SQL).fetchone()
            client_ssl_in_use = _client_tls_in_use(connection)
    except CapabilityError:
        raise
    except Exception as error:
        raise CapabilityError("database_query_failed") from error
    if not row:
        raise CapabilityError("database_query_failed")
    snapshot = {
        "server_version_num": row["server_version_num"],
        "vector_version": row["vector_version"],
        "migration_versions": row["migration_versions"],
        # Managed Postgres proxies can terminate verified client TLS before the
        # backend, so pg_stat_ssl may truthfully report false on the server-side
        # hop. libpq is authoritative for the connection Recall actually opened.
        "ssl_in_use": client_ssl_in_use,
        "role": {
            "superuser": row["superuser"],
            "create_database": row["create_database"],
            "create_role": row["create_role"],
            "replication": row["replication"],
            "bypass_rls": row["bypass_rls"],
        },
        "privileges": {
            "connect": row["can_connect"],
            "schema_usage": row["can_use_schema"],
            "read_runtime_tables": row["can_read_runtime_tables"],
            "write_runtime_tables": row["can_write_runtime_tables"],
            "use_runtime_sequences": row["can_use_runtime_sequences"],
            "schema_migrations_readonly": row["schema_migrations_readonly"],
            "role_memberships_safe": row["role_memberships_safe"],
        },
    }
    return assess_snapshot(snapshot, profile)
