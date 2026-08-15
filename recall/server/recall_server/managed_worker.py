"""Database-driven remote connector execution for the unified control plane."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path
import re
import stat
import tempfile
import time
from typing import Any, Callable, Mapping

from connectors.host import (
    HOSTED_FACTORIES,
    RemoteOptions,
)
from connectors.composio_workspace_rail import ComposioWorkspaceRail
from connectors.registry import (
    ConnectorDefinitionV3,
    RUNTIME_ERROR_CODES,
    definition,
)
from connectors.workspace_rail import WorkspaceRailError
from connectors.work_apis import slack_public_history_rail
from connectors.sdk import (
    ConnectorContractError,
    ConnectorRunner,
)
from privacy.policy import PrivacyPolicy

from .archive_runtime import build_archive_store, build_evidence_archive_store
from .actor_attribution import ActorIdentityIndex
from .canonical import CanonicalArchiveGateway, CanonicalPlane
from .canonical_retrieval import CanonicalRetrieval
from .control import ControlError, SecretBox
from .db import BrainStore
from .logical_evidence import LogicalEvidenceProjectionStore
from .logical_evidence_projection import CanonicalLogicalEvidenceProjector
from .passage_index import CanonicalPassageProjector
from .passage_projection import DEFAULT_PASSAGE_POLICY
from .parquet_scan import CanonicalParquetScanProjector


SAFE_WORKER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{2,127}\Z")
DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_LEASE_SECONDS = 300
MAX_CREDENTIAL_BYTES = 64_000
LOG = logging.getLogger(__name__)
MANAGED_LOGICAL_BATCH_SIZE = 20
MANAGED_PASSAGE_BATCH_SIZE = 20
MANAGED_PASSAGE_EMBED_BATCH_SIZE = 100
MANAGED_PROJECTION_MAX_BATCHES = 1
MANAGED_PROJECTION_CONCURRENCY = 4


def _worker_identity(value: str | None = None) -> str:
    candidate = value or (
        f"worker:{os.uname().nodename}:{os.getpid()}"
    )
    if SAFE_WORKER.fullmatch(candidate):
        return candidate
    return "worker:" + hashlib.sha256(candidate.encode()).hexdigest()[:24]


def _private_root(path: Path) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValueError("managed worker state root is not private") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISDIR(metadata.st_mode):
            raise ValueError("managed worker state root is not private")
        os.fchmod(descriptor, 0o700)
        metadata = os.fstat(descriptor)
    except OSError as error:
        raise ValueError("managed worker state root is not private") from error
    finally:
        os.close(descriptor)
    if stat.S_IMODE(metadata.st_mode) != 0o700:
        raise ValueError("managed worker state root is not private")
    return path


def _write_private_authority(directory: Path, name: str, payload: bytes) -> Path:
    if (
        not isinstance(payload, bytes)
        or not payload
        or len(payload) > MAX_CREDENTIAL_BYTES
        or not re.fullmatch(r"[a-z][a-z0-9-]{2,63}", name)
    ):
        raise ControlError("connector_authority_revoked")
    path = directory / name
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path


def _run_projection_cycle(
    logical: CanonicalLogicalEvidenceProjector,
    passages: CanonicalPassageProjector,
    scan: CanonicalParquetScanProjector,
) -> dict[str, int | str]:
    logical_result = logical.project_pending(
        tenant_id=None,
        batch_size=MANAGED_LOGICAL_BATCH_SIZE,
        max_batches=MANAGED_PROJECTION_MAX_BATCHES,
        upload_concurrency=MANAGED_PROJECTION_CONCURRENCY,
    )
    passage_result = passages.project_pending(
        tenant_id=None,
        batch_size=MANAGED_PASSAGE_BATCH_SIZE,
        max_batches=MANAGED_PROJECTION_MAX_BATCHES,
        concurrency=MANAGED_PROJECTION_CONCURRENCY,
    )
    embedding_result = passages.embed_pending(
        tenant_id=None,
        batch_size=MANAGED_PASSAGE_EMBED_BATCH_SIZE,
        max_batches=MANAGED_PROJECTION_MAX_BATCHES,
    )
    scan_result = scan.project_pending(
        tenant_id=None,
        batch_size=4,
        max_batches=MANAGED_PROJECTION_MAX_BATCHES,
    )
    return {
        "status": "complete",
        "logical_documents": int(logical_result["documents"]),
        "logical_records": int(logical_result["records"]),
        "passage_documents": int(passage_result["documents"]),
        "passages": int(passage_result["passages"]),
        "passage_embeddings": int(embedding_result["processed"]),
        "parquet_shards": int(scan_result["shards"]),
        "parquet_rows": int(scan_result["rows"]),
        "parquet_stale": int(scan_result["stale"]),
    }


def _selector_defaults(connector_id: str) -> dict[str, Any]:
    return {
        "google.gmail": {
            "include_attachments": False,
            "include_spam_trash": False,
            "label_ids": [],
            "own_addresses": [],
            "query": None,
        },
        "google.calendar": {
            "calendar_id": "primary",
            "time_max": None,
            "time_min": None,
        },
        "google.contacts": {},
        "google.drive": {
            "drive_id": None,
            "include_document_text": False,
            "mime_types": [],
        },
        "github.activity": {"owner": "", "repository": ""},
        "linear.activity": {"team_id": ""},
        "slack.messages": {
            "workspace_id": "", "channel_ids": [], "owner_user_ids": [],
        },
        "notion.workspace": {},
        "x.activity": {"streams": ["authored"], "user_id": ""},
    }.get(connector_id, {})


def _selectors(connector_id: str, selected: Mapping[str, Any]) -> dict[str, Any]:
    values = _selector_defaults(connector_id)
    values.update(dict(selected))
    return values


class _DirectCanonicalWriter:
    def __init__(
        self,
        plane: CanonicalPlane,
        *,
        tenant_id: str,
        principal_id: str,
    ):
        self.plane = plane
        self.tenant_id = tenant_id
        self.principal_id = principal_id

    def ingest(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        return self.plane.ingest_batch(
            tenant_id=self.tenant_id,
            principal_id=self.principal_id,
            events=events,
        )


class ManagedConnectorWorker:
    """Claim enabled installations and execute one ACK-gated page per lease."""

    def __init__(
        self,
        store: BrainStore,
        archive: Any,
        secret_box: SecretBox,
        *,
        state_root: Path,
        worker_id: str | None = None,
        connector_factory: Callable[
            [dict[str, Any], dict[str, Any], Path], tuple[Any, Path]
        ]
        | None = None,
        remote_rails: Mapping[str, Any] | None = None,
        interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        embedding_max_batches: int = 1,
    ):
        if (
            type(interval_seconds) is not int
            or not 1 <= interval_seconds <= 3600
            or type(lease_seconds) is not int
            or not 30 <= lease_seconds <= 3600
        ):
            raise ValueError("managed worker timing is invalid")
        if (
            type(embedding_max_batches) is not int
            or not 1 <= embedding_max_batches <= 100
        ):
            raise ValueError(
                "managed worker embedding batch configuration is invalid"
            )
        self.store = store
        self.archive = archive
        self.secret_box = secret_box
        self.state_root = _private_root(state_root)
        self.spool_root = _private_root(self.state_root / "spools")
        self.authority_root = _private_root(self.state_root / "authorities")
        self.worker_id = _worker_identity(worker_id)
        self.connector_factory = connector_factory
        self.remote_rails = dict(remote_rails or {})
        self.interval_seconds = interval_seconds
        self.lease_seconds = lease_seconds
        self.embedding_max_batches = embedding_max_batches
        self.plane = CanonicalPlane(
            store,
            archive,
            actor_identity_index=ActorIdentityIndex(secret_box.blind_index),
        )
        self.retrieval = CanonicalRetrieval(store, archive)

    def _claim(self) -> dict[str, Any] | None:
        with self.store.connect() as connection:
            with connection.transaction():
                row = connection.execute(
                    """SELECT installation.id,installation.tenant_id,
                              installation.principal_id,
                              installation.connector_id,
                              installation.source_id,
                              installation.connection_id,
                              installation.privacy_mode,
                              installation.selectors,
                              connection.provider,
                              connection.status AS connection_status,
                              connection.encrypted_credentials,
                              connection.encryption_key_id
                       FROM connector_installations installation
                       LEFT JOIN provider_connections connection
                         ON connection.id=installation.connection_id
                       WHERE installation.execution='remote_worker'
                         AND installation.state='enabled'
                         AND installation.run_after<=now()
                         AND (
                           installation.lease_expires_at IS NULL
                           OR installation.lease_expires_at<=now()
                         )
                       ORDER BY installation.run_after,installation.id
                       FOR UPDATE OF installation SKIP LOCKED
                       LIMIT 1"""
                ).fetchone()
                if row is None:
                    return None
                connection.execute(
                    """UPDATE connector_installations
                       SET lease_owner=%s,
                           lease_expires_at=now()+(%s * interval '1 second'),
                           last_started_at=now(),updated_at=now()
                       WHERE id=%s""",
                    (self.worker_id, self.lease_seconds, row["id"]),
                )
        return dict(row)

    def _credentials(self, row: dict[str, Any]) -> dict[str, Any]:
        if (
            row.get("connection_id") is None
            or row.get("connection_status") != "connected"
            or row.get("encrypted_credentials") is None
            or row.get("encryption_key_id") != self.secret_box.key_id
        ):
            raise ControlError("connector_authority_revoked")
        return self.secret_box.open(
            bytes(row["encrypted_credentials"]),
            purpose=f"provider-connection:{row['connection_id']}",
        )

    def _credential_payload(
        self,
        connector_id: str,
        credentials: dict[str, Any],
    ) -> bytes:
        if connector_id.startswith("google."):
            required = {
                "client_id",
                "client_secret",
                "refresh_token",
                "token_uri",
                "type",
            }
            if not required.issubset(credentials):
                raise ControlError("connector_authority_revoked")
            payload = json.dumps(
                credentials,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        else:
            token = (
                credentials.get("bot_access_token")
                or credentials.get("access_token")
                or credentials.get("token")
            )
            if not isinstance(token, str) or not token:
                raise ControlError("connector_authority_revoked")
            payload = token.encode()
        if not payload or len(payload) > MAX_CREDENTIAL_BYTES:
            raise ControlError("connector_authority_revoked")
        return payload

    def _build_default(
        self,
        row: dict[str, Any],
        credentials: dict[str, Any],
        private_directory: Path,
    ) -> tuple[Any, Path]:
        connector_id = row["connector_id"]
        connector_definition = definition(connector_id)
        if (
            not isinstance(connector_definition, ConnectorDefinitionV3)
            or connector_definition.execution_placement != "remote_worker"
        ):
            raise ControlError("connector_schema_drift")
        rails = self.remote_rails
        if row.get("provider") == "composio":
            if not connector_id.startswith("google."):
                raise ControlError("connector_schema_drift")
            api_key = os.environ.get("RECALL_COMPOSIO_API_KEY", "")
            user_id = credentials.get("user_id")
            connected_account_id = credentials.get("connected_account_id")
            toolkit = credentials.get("toolkit")
            if not all(
                isinstance(value, str) and value
                for value in (api_key, user_id, connected_account_id, toolkit)
            ):
                raise ControlError("connector_authority_revoked")
            binary_hosts = tuple(
                value.strip()
                for value in os.environ.get(
                    "RECALL_COMPOSIO_BINARY_HOSTS", ""
                ).split(",")
                if value.strip()
            )
            try:
                composio_rail = ComposioWorkspaceRail(
                    api_key=api_key,
                    user_id=user_id,
                    connected_account_id=connected_account_id,
                    connector_id=connector_id,
                    binary_hosts=binary_hosts,
                )
            except WorkspaceRailError:
                raise ControlError("connector_authority_revoked") from None
            if composio_rail.toolkit != toolkit:
                raise ControlError("connector_authority_revoked")
            rails = {**self.remote_rails, connector_id: composio_rail}
            authority_path = private_directory / "composio-reference"
        elif connector_id == "slack.messages" and (
            "bot_access_token" in credentials or "user_access_token" in credentials
        ):
            bot_token = credentials.get("bot_access_token")
            user_token = credentials.get("user_access_token")
            if not all(isinstance(value, str) and value for value in (bot_token, user_token)):
                raise ControlError("connector_authority_revoked")
            authority_path = _write_private_authority(
                private_directory, "slack-bot-authority", bot_token.encode(),
            )
            user_authority_path = _write_private_authority(
                private_directory, "slack-user-authority", user_token.encode(),
            )
            rails = {
                **rails,
                connector_id: slack_public_history_rail(
                    bot_authority_path=authority_path,
                    user_authority_path=user_authority_path,
                    timeout_seconds=60,
                ),
            }
        else:
            authority_path = _write_private_authority(
                private_directory,
                "source-authority",
                self._credential_payload(connector_id, credentials),
            )
        options = RemoteOptions.from_mapping(
            connector_id,
            {
                "source_authority": {
                    "kind": "file",
                    "path": str(authority_path),
                },
                "spool": str(self.spool_root / f"{row['id']}.db"),
                "page_size": (
                    10
                    if connector_id == "google.gmail"
                    else 25 if connector_id == "x.activity"
                    else 20 if connector_id == "slack.messages" else 100
                ),
                "timeout_seconds": 60,
                "selectors": _selectors(
                    connector_id,
                    row.get("selectors") or {},
                ),
            },
        )
        factory = HOSTED_FACTORIES[connector_id]
        connector, spool = factory.build(
            options,
            row["source_id"],
            row["privacy_mode"],
            rails,
        )
        return connector, spool

    @staticmethod
    def _safe_error(error: Exception) -> str:
        code = getattr(error, "error_code", None) or getattr(error, "code", None)
        if code in RUNTIME_ERROR_CODES:
            return code
        if isinstance(error, ConnectorContractError):
            return "connector_invalid_page"
        return "connector_unavailable"

    def _finish(
        self,
        installation_id: Any,
        *,
        success: bool,
        error_code: str | None = None,
        retry_after_seconds: int | None = None,
    ) -> None:
        delay = (
            max(1, min(3600, retry_after_seconds))
            if retry_after_seconds is not None
            else self.interval_seconds
        )
        with self.store.connect() as connection:
            if success:
                connection.execute(
                    """UPDATE connector_installations
                       SET lease_owner=NULL,lease_expires_at=NULL,
                           run_after=now()+(%s * interval '1 second'),
                           last_success_at=now(),last_error_code=NULL,
                           failure_count=0,updated_at=now()
                       WHERE id=%s AND lease_owner=%s""",
                    (delay, installation_id, self.worker_id),
                )
            else:
                connection.execute(
                    """UPDATE connector_installations
                       SET lease_owner=NULL,lease_expires_at=NULL,
                           run_after=now()+(%s * interval '1 second'),
                           last_error_code=%s,failure_count=failure_count+1,
                           updated_at=now()
                       WHERE id=%s AND lease_owner=%s""",
                    (delay, error_code, installation_id, self.worker_id),
                )

    def _degrade_authority(
        self,
        row: dict[str, Any],
        *,
        error_code: str,
    ) -> None:
        """Stop retrying revoked authority until an operator reconnects it."""

        connection_id = row.get("connection_id")
        with self.store.connect() as connection:
            with connection.transaction():
                if connection_id is not None:
                    connection.execute(
                        """UPDATE provider_connections
                           SET status='degraded',updated_at=now()
                           WHERE id=%s AND status='connected'""",
                        (connection_id,),
                    )
                    connection.execute(
                        """UPDATE connector_installations
                           SET state='configured',lease_owner=NULL,
                               lease_expires_at=NULL,last_error_code=%s,
                               failure_count=failure_count+1,updated_at=now()
                           WHERE connection_id=%s AND state='enabled'""",
                        (error_code, connection_id),
                    )
                else:
                    connection.execute(
                        """UPDATE connector_installations
                           SET state='configured',lease_owner=NULL,
                               lease_expires_at=NULL,last_error_code=%s,
                               failure_count=failure_count+1,updated_at=now()
                           WHERE id=%s AND lease_owner=%s""",
                        (error_code, row["id"], self.worker_id),
                    )

    def run_once(self) -> dict[str, Any]:
        row = self._claim()
        if row is None:
            return {
                "schema_version": 1,
                "status": "idle",
                "processed": 0,
                "committed": 0,
                "failed": 0,
            }
        connector = None
        runner = None
        try:
            credentials = self._credentials(row)
            with tempfile.TemporaryDirectory(
                prefix="authority-",
                dir=self.authority_root,
            ) as directory:
                private_directory = Path(directory)
                os.chmod(private_directory, 0o700)
                builder = self.connector_factory or self._build_default
                connector, spool = builder(
                    row,
                    credentials,
                    private_directory,
                )
                runner = ConnectorRunner(
                    connector=connector,
                    brain=_DirectCanonicalWriter(
                        self.plane,
                        tenant_id=row["tenant_id"],
                        principal_id=row["principal_id"],
                    ),
                    archive=CanonicalArchiveGateway(
                        self.store,
                        self.archive,
                        tenant_id=row["tenant_id"],
                        principal_id=row["principal_id"],
                    ),
                    tenant_id=row["tenant_id"],
                    principal_id=row["principal_id"],
                    spool_path=spool,
                    privacy=PrivacyPolicy(mode=row["privacy_mode"]),
                )
                result = runner.run_once()
            if result.get("status") == "backoff":
                code = result.get("error_code", "connector_rate_limited")
                self._finish(
                    row["id"],
                    success=False,
                    error_code=code,
                    retry_after_seconds=result.get("retry_after_seconds"),
                )
                return {
                    "schema_version": 1,
                    "status": "backoff",
                    "processed": 1,
                    "committed": 0,
                    "failed": 1,
                    "error_code": code,
                }
            embedding = self.retrieval.embed_pending(
                batch_size=100,
                max_batches=self.embedding_max_batches,
            )
            has_more = bool(result.get("has_more", False))
            self._finish(
                row["id"],
                success=True,
                retry_after_seconds=1 if has_more else None,
            )
            return {
                "schema_version": 1,
                "status": "committed",
                "processed": 1,
                "committed": 1,
                "failed": 0,
                "acked": int(result.get("acked", 0)),
                "staged": int(result.get("staged", 0)),
                "embedded": int(embedding.get("processed", 0)),
                "has_more": has_more,
            }
        except Exception as error:
            code = self._safe_error(error)
            if code in {
                "connector_authority_revoked",
                "connector_authority_forbidden",
            }:
                self._degrade_authority(row, error_code=code)
            else:
                self._finish(
                    row["id"],
                    success=False,
                    error_code=code,
                    retry_after_seconds=min(
                        3600,
                        self.interval_seconds * 2,
                    ),
                )
            return {
                "schema_version": 1,
                "status": "failed",
                "processed": 1,
                "committed": 0,
                "failed": 1,
                "error_code": code,
            }
        finally:
            if runner is not None:
                runner.close()
            if connector is not None:
                close = getattr(connector, "close", None)
                if callable(close):
                    close()


def run_managed_worker(
    store: BrainStore,
    *,
    state_root: Path,
    once: bool,
    interval_seconds: int,
) -> dict[str, Any]:
    worker = ManagedConnectorWorker(
        store,
        build_archive_store(),
        SecretBox.from_env(),
        state_root=state_root,
        interval_seconds=interval_seconds,
        embedding_max_batches=int(
            os.environ.get("RECALL_CANONICAL_EMBED_MAX_BATCHES", "1")
        ),
    )
    projection_enabled = os.environ.get("RECALL_MANAGED_PROJECTIONS_ENABLED") == "1"
    logical_projector = passage_projector = scan_projector = None
    if projection_enabled:
        evidence_projection = LogicalEvidenceProjectionStore(
            build_evidence_archive_store(),
            part_upload_concurrency=MANAGED_PROJECTION_CONCURRENCY,
        )
        logical_projector = CanonicalLogicalEvidenceProjector(
            store,
            evidence_projection,
            raw_archive=worker.archive,
        )
        passage_projector = CanonicalPassageProjector(
            store,
            evidence_projection,
            policy=DEFAULT_PASSAGE_POLICY,
        )
        scan_projector = CanonicalParquetScanProjector(
            store,
            evidence_projection,
        )
    cycles = committed = failed = projection_failures = 0
    while True:
        result = worker.run_once()
        projection: dict[str, int | str] = {
            "status": "disabled",
            "logical_documents": 0,
            "logical_records": 0,
            "passage_documents": 0,
            "passages": 0,
            "passage_embeddings": 0,
            "parquet_shards": 0,
            "parquet_rows": 0,
            "parquet_stale": 0,
        }
        if (
            logical_projector is not None
            and passage_projector is not None
            and scan_projector is not None
        ):
            try:
                projection = _run_projection_cycle(
                    logical_projector,
                    passage_projector,
                    scan_projector,
                )
            except Exception as error:
                projection_failures += 1
                projection["status"] = "failed"
                LOG.error(
                    "managed projection cycle failed type=%s",
                    type(error).__name__,
                )
        cycles += 1
        committed += int(result["committed"])
        failed += int(result["failed"])
        if once:
            return {
                "schema_version": 1,
                "status": (
                    "projection_failed"
                    if projection["status"] == "failed"
                    else result["status"]
                ),
                "cycles": cycles,
                "committed": committed,
                "failed": failed,
                "projection_failures": projection_failures,
                **{
                    key: value
                    for key, value in projection.items()
                    if key != "status"
                },
            }
        projection_activity = sum(
            int(projection[key])
            for key in (
                "logical_documents",
                "passage_documents",
                "passage_embeddings",
                "parquet_shards",
                "parquet_stale",
            )
        )
        time.sleep(
            1
            if result["processed"] or projection_activity
            else min(interval_seconds, 30)
        )


__all__ = [
    "ManagedConnectorWorker",
    "run_managed_worker",
]
