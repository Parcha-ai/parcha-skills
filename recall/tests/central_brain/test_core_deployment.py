from __future__ import annotations

import json
import re
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path.insert(0, str(SERVER))

from recall_server import SCHEMA_VERSION  # noqa: E402
from recall_server.capabilities import (  # noqa: E402
    CAPABILITY_SQL,
    CapabilityError,
    _client_tls_in_use,
    _connection_failure_code,
    assess_snapshot,
    validate_connection_policy,
)
from recall_server.deployment import (  # noqa: E402
    DeploymentManifestError,
    load_manifest,
    preview,
)


def healthy_snapshot() -> dict:
    return {
        "server_version_num": 170000,
        "vector_version": "0.8.1",
        "migration_versions": list(range(1, SCHEMA_VERSION + 1)),
        "ssl_in_use": True,
        "role": {
            "superuser": False,
            "create_database": False,
            "create_role": False,
            "replication": False,
            "bypass_rls": False,
        },
        "privileges": {
            "connect": True,
            "schema_usage": True,
            "read_runtime_tables": True,
            "write_runtime_tables": True,
            "use_runtime_sequences": True,
            "schema_migrations_readonly": True,
            "role_memberships_safe": True,
        },
    }


def synthetic_manifest() -> dict:
    return {
        "schema_version": 1,
        "deployment_name": "synthetic-recall-core",
        "image": "registry.example.invalid/recall-core@sha256:" + "a" * 64,
        "database": {
            "adapter": "postgres",
            "provider": "planetscale",
            "url_ref": "secret://runtime/RECALL_DATABASE_URL",
            "tls_mode": "verify-full",
        },
        "service": {
            "adapter": "render-private-service",
            "embedding": {
                "protocol": "voyage",
                "url": "https://api.voyage.example",
                "approved_url": "https://api.voyage.example",
                "key_ref": "secret://runtime/RECALL_EMBEDDING_API_KEY",
                "model": "voyage-synthetic",
                "revision": "voyage-synthetic-v1",
                "dimensions": 512,
                "batch_size": 64,
            },
            "region_ref": "approval://provider-region",
            "billing_ref": "approval://provider-billing",
            "public_ingress": False,
        },
        "network": {
            "adapter": "tailscale-gateway",
            "gateway_image": ("registry.example.invalid/tailscale@sha256:" + "c" * 64),
            "route_ref": "approval://tailnet-route",
            "listen_port": 9443,
        },
        "authorization": {
            "provider_ref": "approval://provider-authorization",
            "cutover_ref": "approval://writer-cutover",
        },
    }


class DatabaseCapabilityContractTest(unittest.TestCase):
    def test_production_connection_requires_verified_tls_and_system_roots(self) -> None:
        secure = (
            "postgresql://synthetic:synthetic@db.example.invalid/recall"
            "?sslmode=verify-full&sslrootcert=system"
        )
        self.assertEqual(
            validate_connection_policy(secure, "production")["tls"], "verify-full"
        )
        explicit_system_bundle = (
            "postgresql://synthetic:synthetic@db.example.invalid/recall"
            "?sslmode=verify-full"
            "&sslrootcert=/etc/ssl/certs/ca-certificates.crt"
        )
        self.assertEqual(
            validate_connection_policy(explicit_system_bundle, "production")["tls"],
            "verify-full",
        )
        for unsafe in (
            "postgresql://synthetic:synthetic@db.example.invalid/recall?sslmode=require",
            "postgresql://synthetic:synthetic@db.example.invalid/recall?sslmode=verify-full",
            "postgresql://synthetic:synthetic@db.example.invalid/recall?sslmode=disable&sslrootcert=system",
        ):
            with self.subTest(unsafe=unsafe.split("?")[-1]):
                with self.assertRaises(CapabilityError) as raised:
                    validate_connection_policy(unsafe, "production")
                self.assertEqual(raised.exception.code, "tls_policy_failed")
                self.assertNotIn("synthetic:synthetic", str(raised.exception))
        with self.assertRaises(CapabilityError) as duplicate:
            validate_connection_policy(
                "postgresql://synthetic:synthetic@db.example.invalid/recall"
                "?sslmode=require&sslmode=verify-full&sslrootcert=system",
                "production",
            )
        self.assertEqual(duplicate.exception.code, "dsn_invalid")

    def test_fixture_exception_is_explicit_and_loopback_only(self) -> None:
        local = "postgresql://synthetic:synthetic@127.0.0.1/recall?sslmode=disable"
        result = validate_connection_policy(local, "local-fixture")
        self.assertEqual(result, {"profile": "local-fixture", "tls": "fixture-only"})
        with self.assertRaises(CapabilityError) as raised:
            validate_connection_policy(
                "postgresql://synthetic:synthetic@db.example.invalid/recall?sslmode=disable",
                "local-fixture",
            )
        self.assertEqual(raised.exception.code, "fixture_not_loopback")

    def test_snapshot_accepts_portable_pgvector_runtime(self) -> None:
        result = assess_snapshot(healthy_snapshot(), profile="production")
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["schema_version"], SCHEMA_VERSION)
        self.assertEqual(result["postgres_major"], 17)
        self.assertEqual(result["extensions"], {"vector": "0.8.1"})
        self.assertEqual(result["role"], "least-privilege-runtime")
        self.assertNotIn("provider", result)

    def test_snapshot_fails_closed_on_drift_privilege_extension_and_tls(self) -> None:
        failures = {
            "schema_drift": ("migration_versions", list(range(1, SCHEMA_VERSION))),
            "extension_missing": ("vector_version", None),
            "extension_unsupported": ("vector_version", "0.7.4"),
            "tls_not_active": ("ssl_in_use", False),
        }
        for code, (field, value) in failures.items():
            snapshot = healthy_snapshot()
            snapshot[field] = value
            with self.subTest(code=code):
                with self.assertRaises(CapabilityError) as raised:
                    assess_snapshot(snapshot, profile="production")
                self.assertEqual(raised.exception.code, code)
        for privilege in healthy_snapshot()["role"]:
            snapshot = healthy_snapshot()
            snapshot["role"][privilege] = True
            with self.subTest(privilege=privilege):
                with self.assertRaises(CapabilityError) as raised:
                    assess_snapshot(snapshot, profile="production")
                self.assertEqual(raised.exception.code, "role_privilege_excessive")
        for privilege in healthy_snapshot()["privileges"]:
            snapshot = healthy_snapshot()
            snapshot["privileges"][privilege] = False
            with self.subTest(missing_privilege=privilege):
                with self.assertRaises(CapabilityError) as raised:
                    assess_snapshot(snapshot, profile="production")
                self.assertEqual(raised.exception.code, "role_privilege_insufficient")

    def test_probe_sql_is_standard_postgres_and_provider_neutral(self) -> None:
        lowered = CAPABILITY_SQL.casefold()
        self.assertIn("pg_extension", lowered)
        self.assertIn("pg_stat_ssl", lowered)
        for table in (
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
            "canonical_evidence_objects",
            "canonical_evidence_documents",
            "canonical_evidence_document_parts",
            "canonical_evidence_document_queue",
            "canonical_evidence_cleanup_queue",
        ):
            self.assertIn(f"('{table}')", lowered)
        for provider in ("planetscale", "render", "supabase", "neon"):
            self.assertNotIn(provider, lowered)

    def test_client_tls_observation_can_survive_proxy_backend_hop(self) -> None:
        connection = SimpleNamespace(pgconn=SimpleNamespace(ssl_in_use=True))
        self.assertTrue(_client_tls_in_use(connection))
        self.assertFalse(_client_tls_in_use(SimpleNamespace()))

    def test_connection_failure_codes_are_content_free_and_sqlstate_driven(
        self,
    ) -> None:
        self.assertEqual(
            _connection_failure_code(SimpleNamespace(sqlstate="28P01")),
            "database_auth_failed",
        )
        self.assertEqual(
            _connection_failure_code(SimpleNamespace(sqlstate="08006")),
            "database_connection_failed",
        )
        self.assertEqual(
            _connection_failure_code(RuntimeError("synthetic sensitive detail")),
            "database_connection_failed",
        )


class DeploymentPreviewContractTest(unittest.TestCase):
    def write_manifest(self, value: dict) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "manifest.json"
        path.write_text(json.dumps(value))
        return path

    def test_preview_is_deterministic_content_free_and_zero_mutation(self) -> None:
        path = self.write_manifest(synthetic_manifest())
        with (
            mock.patch.object(socket, "socket") as network,
            mock.patch.object(subprocess, "run") as process,
        ):
            first = preview(load_manifest(path))
            second = preview(load_manifest(path))
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "approval_required")
        self.assertEqual(first["mutation_count"], 0)
        self.assertEqual(first["network_calls"], 0)
        self.assertEqual(first["source_reads"], 0)
        self.assertEqual(
            first["pending_gates"],
            [
                "provider-billing",
                "provider-region",
                "provider-authorization",
                "tailnet-route",
                "writer-cutover",
            ],
        )
        self.assertEqual(
            first["resources"],
            [
                "postgres-database",
                "private-service",
                "tailscale-gateway",
            ],
        )
        rendered = json.dumps(first, sort_keys=True)
        self.assertNotIn("secret://", rendered)
        self.assertNotIn("approval://", rendered)
        self.assertNotIn("synthetic-recall-core", rendered)
        network.assert_not_called()
        process.assert_not_called()

    def test_manifest_is_closed_and_rejects_credentials_or_unpinned_images(
        self,
    ) -> None:
        for mutate in (
            lambda value: value.update({"password": "do-not-accept"}),
            lambda value: value["database"].update(
                {"url_ref": "postgresql://user:password@host/db"}
            ),
            lambda value: value.update(
                {"image": "registry.example.invalid/recall-core:latest"}
            ),
            lambda value: value["service"]["embedding"].update(
                {"url": "http://api.voyage.example"}
            ),
            lambda value: value["service"]["embedding"].update(
                {"approved_url": "https://other.example"}
            ),
            lambda value: value["service"]["embedding"].update(
                {"dimensions": 1536}
            ),
            lambda value: value["network"].update(
                {"gateway_image": "registry.example.invalid/tailscale:latest"}
            ),
            lambda value: value["service"].update({"public_ingress": True}),
            lambda value: value["network"].update({"listen_port": 443}),
        ):
            value = synthetic_manifest()
            mutate(value)
            with self.subTest(value=value):
                with self.assertRaises(DeploymentManifestError):
                    load_manifest(self.write_manifest(value))

    def test_private_listener_port_is_configurable_without_permitting_443(self) -> None:
        value = synthetic_manifest()
        value["network"]["listen_port"] = 10443
        self.assertEqual(
            load_manifest(self.write_manifest(value))["network"]["listen_port"], 10443
        )

    def test_managed_embedding_manifest_accepts_the_runtime_batch_bound(self) -> None:
        value = synthetic_manifest()
        value["service"]["embedding"]["batch_size"] = 512
        self.assertEqual(
            load_manifest(self.write_manifest(value))["service"]["embedding"][
                "batch_size"
            ],
            512,
        )
        value["service"]["embedding"]["batch_size"] = 513
        with self.assertRaisesRegex(
            DeploymentManifestError,
            "invalid managed embedding batch size",
        ):
            load_manifest(self.write_manifest(value))

    def test_repository_profile_is_valid_and_safe_to_preview(self) -> None:
        profile = load_manifest(SERVER / "deploy" / "recall-core.plan.example.json")
        result = preview(profile)
        self.assertEqual(result["status"], "approval_required")
        self.assertRegex(result["plan_sha256"], r"^[0-9a-f]{64}$")

    def test_manifest_reader_rejects_symlinks_before_reading(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        target = Path(directory.name) / "target.json"
        target.write_text(json.dumps(synthetic_manifest()))
        link = Path(directory.name) / "manifest.json"
        link.symlink_to(target)
        with self.assertRaises(DeploymentManifestError):
            load_manifest(link)


class ContainerContractTest(unittest.TestCase):
    def test_main_only_image_publication_uses_the_restricted_context(self) -> None:
        workflow = (
            RECALL.parent / ".github" / "workflows" / "recall-image.yml"
        ).read_text()
        self.assertIn("packages: write", workflow)
        self.assertIn('branches: ["main"]', workflow)
        self.assertNotIn("pull_request:", workflow)
        self.assertIn("context: recall", workflow)
        self.assertIn("file: recall/Dockerfile", workflow)
        self.assertIn("push: true", workflow)
        self.assertIn(
            'echo "name=ghcr.io/${GITHUB_REPOSITORY_OWNER,,}/recall-core"',
            workflow,
        )
        self.assertIn(
            "tags: ${{ steps.image.outputs.name }}:${{ github.sha }}",
            workflow,
        )
        self.assertNotIn("context: .", workflow)

    def test_image_publication_tracks_every_runtime_copy_input(self) -> None:
        workflow = (
            RECALL.parent / ".github" / "workflows" / "recall-image.yml"
        ).read_text()
        for runtime_input in (
            "recall/server/**",
            "recall/contracts/**",
            "recall/connectors/**",
            "recall/client/**",
            "recall/collector/**",
            "recall/privacy/**",
            "recall/scripts/install_gws.py",
            "recall/skills/recall/scripts/recall.py",
            "recall/skills/recall/scripts/codex_identity.py",
        ):
            self.assertIn(f'"{runtime_input}"', workflow)

    def test_image_has_pinned_base_nonroot_runtime_and_content_free_healthcheck(
        self,
    ) -> None:
        dockerfile = (RECALL / "Dockerfile").read_text()
        self.assertRegex(
            dockerfile,
            r"FROM python:3\.12-slim-bookworm@sha256:[0-9a-f]{64}",
        )
        self.assertNotIn("node-runtime", dockerfile)
        self.assertNotIn("pi-agent", dockerfile)
        self.assertNotIn("/opt/recall-pi", dockerfile)
        self.assertNotIn("vendor/ati", dockerfile)
        self.assertNotIn("/opt/ati", dockerfile)
        self.assertIn("USER 10001", dockerfile)
        self.assertIn("usermod -a -G 1000 recall", dockerfile)
        self.assertIn(
            "COMPOSIO_CACHE_DIR=/tmp/recall-composio-cache",
            dockerfile,
        )
        self.assertIn("/tmp/recall-composio-cache", dockerfile)
        self.assertIn('ENTRYPOINT ["python", "-m", "recall_server.cli"]', dockerfile)
        self.assertIn(
            'CMD ["serve", "--host", "0.0.0.0", "--port", "8788", "--require-auth", '
            '"--capability-profile", "production"]',
            dockerfile,
        )
        self.assertIn("/readyz", dockerfile)
        self.assertIn('"--require-auth"', dockerfile)
        self.assertIn('"--capability-profile", "production"', dockerfile)
        self.assertNotIn("ENV RECALL_AUTH_REQUIRED", dockerfile)
        self.assertNotRegex(dockerfile, re.compile(r"COPY\s+\.\s+\.", re.IGNORECASE))

    def test_nested_agent_runtime_is_absent(self) -> None:
        self.assertEqual(
            [path for path in (SERVER / "pi-agent").rglob("*") if path.is_file()],
            [],
        )
        self.assertFalse((SERVER / "recall_server" / "agent.py").exists())
        self.assertFalse((SERVER / "recall_server" / "agent_pi.py").exists())
        self.assertFalse((SERVER / "recall_server" / "agent_runs.py").exists())
        self.assertFalse((SERVER / "vendor" / "ati" / "README.md").exists())
        self.assertFalse((SERVER / "vendor" / "ati" / "grep_ati_brain_turn.mjs").exists())

    def test_proprietary_ati_bundle_is_not_committed(self) -> None:
        tracked = subprocess.run(
            [
                "git",
                "-C",
                str(RECALL.parent),
                "ls-files",
                "--",
                "recall/server/vendor/ati/",
            ],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        self.assertEqual(tracked, "")
        ignored = (RECALL.parent / ".gitignore").read_text()
        self.assertIn("recall/server/vendor/ati/", ignored)

    def test_build_context_excludes_private_and_development_surfaces(self) -> None:
        ignored = (RECALL / ".dockerignore").read_text()
        self.assertIn("tests/", ignored)
        self.assertIn("docs/", ignored)
        self.assertIn("*.env", ignored)
        self.assertIn(".git", ignored)

    def test_runtime_dependencies_are_exactly_pinned(self) -> None:
        requirements = (SERVER / "requirements.txt").read_text().splitlines()
        self.assertTrue(requirements)
        self.assertTrue(all("==" in line for line in requirements if line.strip()))


if __name__ == "__main__":
    unittest.main()
