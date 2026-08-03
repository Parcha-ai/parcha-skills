#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

import psycopg
from psycopg import sql


SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))

from recall_server.db import BrainStore  # noqa: E402


ROLE = "recall_runtime_e2e"
PASSWORD = "synthetic-runtime-password"


def run(*arguments: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments, check=True, capture_output=True, text=True, env=env,
    )


def runtime_dsn(admin_dsn: str) -> str:
    parsed = urlsplit(admin_dsn)
    host = parsed.hostname or "127.0.0.1"
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{quote(ROLE)}:{quote(PASSWORD)}@{host}{port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, "sslmode=disable", ""))


def remove_role(admin_dsn: str) -> None:
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        exists = connection.execute(
            "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=%s", (ROLE,),
        ).fetchone()
        if exists:
            connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(ROLE)))
            connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(ROLE)))


def configure_role(admin_dsn: str) -> None:
    BrainStore(admin_dsn).migrate()
    remove_role(admin_dsn)
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        connection.execute(
            sql.SQL(
                "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOREPLICATION NOBYPASSRLS"
            ).format(sql.Identifier(ROLE), sql.Literal(PASSWORD))
        )
        connection.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(connection.info.dbname), sql.Identifier(ROLE),
        ))
        connection.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(ROLE)))
        connection.execute(sql.SQL("GRANT SELECT ON public.schema_migrations TO {}").format(
            sql.Identifier(ROLE),
        ))
        tables = connection.execute(
            "SELECT tablename FROM pg_catalog.pg_tables "
            "WHERE schemaname='public' AND tablename<>'schema_migrations'"
        ).fetchall()
        for (table,) in tables:
            connection.execute(sql.SQL(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO {}"
            ).format(sql.Identifier("public", table), sql.Identifier(ROLE)))
        connection.execute(sql.SQL(
            "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {}"
        ).format(sql.Identifier(ROLE)))


def wait_for_health(url: str) -> None:
    for _attempt in range(30):
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if json.load(response).get("status") in {"ok", "ready"}:
                    return
        except (OSError, ValueError):
            time.sleep(0.2)
    raise RuntimeError("container health deadline exceeded")


def request_json(
    url: str, body: dict, token: str, *, idempotency_key: str | None = None,
) -> tuple[int, dict]:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    request = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers=headers, method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def main() -> None:
    admin_dsn = os.environ["RECALL_DATABASE_URL"]
    image = os.environ.get("RECALL_CORE_IMAGE", "recall-core:e2e")
    name = f"recall-core-e2e-{os.getpid()}"
    dsn = runtime_dsn(admin_dsn)
    child_env = {**os.environ, "RECALL_DATABASE_URL": dsn}
    try:
        configure_role(admin_dsn)
        identity = run(
            "docker", "image", "inspect", image, "--format", "{{.Config.User}}",
        ).stdout.strip()
        if identity != "10001":
            raise RuntimeError("container user contract failed")
        groups = run(
            "docker", "run", "--rm", "--entrypoint", "id", image, "-G",
        ).stdout.split()
        if "10001" not in groups or "1000" not in groups:
            raise RuntimeError("container secret-file group contract failed")
        composio_import = run(
            "docker",
            "run",
            "--rm",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,size=16m,uid=10001,gid=10001,mode=0700",
            "--entrypoint",
            "python",
            image,
            "-c",
            (
                "from composio import Composio; "
                "from connectors.composio_workspace_rail import "
                "ComposioWorkspaceRail; "
                "from recall_server.control import ComposioConnectionBroker"
            ),
        )
        if composio_import.stdout or composio_import.stderr:
            raise RuntimeError("Composio container import was not content-free")
        production_start = subprocess.run(
            ["docker", "run", "--rm", "--read-only", "--network", "host",
             "-e", "RECALL_DATABASE_URL", image],
            capture_output=True, text=True, env=child_env, check=False,
        )
        if production_start.returncode != 2:
            raise RuntimeError("production startup accepted fixture TLS")
        rejection = production_start.stdout + production_start.stderr
        if "tls_policy_failed" not in rejection or PASSWORD in rejection:
            raise RuntimeError("production startup rejection was not content-free")
        capability = run(
            "docker", "run", "--rm", "--read-only", "--network", "host",
            "-e", "RECALL_DATABASE_URL", image,
            "capability-check", "--profile", "local-fixture",
            env=child_env,
        )
        result = json.loads(capability.stdout)
        if result.get("status") != "fixture-ready" or result.get("role") != "least-privilege-runtime":
            raise RuntimeError("database capability contract failed")
        run(
            "docker", "run", "-d", "--name", name, "--read-only", "--network", "host",
            "-e", "RECALL_DATABASE_URL", image,
            "serve", "--host", "0.0.0.0", "--port", "18788", "--require-auth",
            "--capability-profile", "local-fixture",
            env=child_env,
        )
        wait_for_health("http://127.0.0.1:18788/readyz")
        request = urllib.request.Request(
            "http://127.0.0.1:18788/v1/search",
            data=b'{"query":"synthetic"}',
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(request, timeout=2)
        except urllib.error.HTTPError as error:
            if error.code != 401:
                raise
        else:
            raise RuntimeError("unauthenticated container request was accepted")
        source_id = f"synthetic:core-e2e:{os.getpid()}"
        client_store = BrainStore(dsn)
        try:
            credential = client_store.create_collector_token(
                f"core-e2e-{os.getpid()}",
                source_id,
                ["read", "write", "metrics"],
            )
        finally:
            client_store.close()
        marker = f"synthetic-container-marker-{os.getpid()}"
        content = {"role": "user", "text": marker}
        event = {
            "schema_version": 1,
            "source_id": source_id,
            "native_id": "session-1:turn-1",
            "native_parent_id": "session-1",
            "kind": "message",
            "occurred_at": "2026-07-17T00:00:00Z",
            "observed_at": "2026-07-17T00:00:01Z",
            "principal_id": "synthetic-owner",
            "visibility": "private",
            "content_type": "application/json",
            "content": content,
            "provenance": {"harness": "container-e2e"},
            "content_sha256": hashlib.sha256(json.dumps(
                content, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
            ).encode()).hexdigest(),
        }
        status, acknowledgement = request_json(
            "http://127.0.0.1:18788/v1/ingest/batches",
            {"events": [event]}, credential["token"],
            idempotency_key=f"core-e2e-{os.getpid()}",
        )
        if status != 201 or acknowledgement.get("inserted") != 1:
            raise RuntimeError("container ingest contract failed")
        status, search = request_json(
            "http://127.0.0.1:18788/v1/search",
            {"query": marker, "filters": {}, "limit": 5}, credential["token"],
        )
        if status != 200 or not any(
            result.get("text") == marker for result in search.get("results", [])
        ):
            raise RuntimeError("container retrieval contract failed")
    finally:
        subprocess.run(
            ["docker", "rm", "-f", name], capture_output=True, text=True, check=False,
        )
        active_error = sys.exception()
        try:
            remove_role(admin_dsn)
        except psycopg.Error:
            if active_error is None:
                raise
    print(json.dumps({
        "status": "pass",
        "image_user": "nonroot",
        "filesystem": "read-only",
        "database": "standard-pgvector-fixture",
        "authentication": "fail-closed",
        "composio_runtime": "ready",
        "ingest_retrieve": "pass",
    }, sort_keys=True))


if __name__ == "__main__":
    main()
