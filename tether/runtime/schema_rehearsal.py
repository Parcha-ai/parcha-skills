"""Internal, receipt-bound, kill-safe schema rehearsal coordinator.

Proves the complete quiesce, backup, transform, validation, and recovery
boundary on disposable database copies without exposing a public migrate
command or mutating the live schema. The rehearsal result is evidence only;
a live migration must repeat every check under a fresh quiesced receipt.

Not wired to the Node CLI. Lock order is fixed and never inverted:

    installer lifecycle flock
      -> receipt planned
      -> maintenance gate armed
      -> gateway stopped and attested inactive
      -> database singleton
      -> verified receipt-bound backup
      -> singleton released, disposable 17->18->17 transforms
      -> exact pinned-artifact validation
      -> resumed (receipt bootable) -> gateway restarted -> complete
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import socket
import sqlite3
import stat
import subprocess  # nosec B404
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import bridge_runtime
import domain_runtime as domain_runtime_module
import domain_schema
import schema_orchestrator
import schema_receipt
import security


_VALIDATOR_TIMEOUT_SECONDS = 300
# Each rehearsal copies the whole live database. Without a bound, repeated
# runs fill the disk and break the very database the backups protect.
_BACKUP_RETAIN = 3


class RehearsalError(RuntimeError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


@dataclass(frozen=True)
class GatewayController:
    """Stop/start/probe hooks for the Hermes gateway supervisor.

    `is_active` must observe real supervisor state independently of what
    `stop`/`start` returned; a zero exit status alone is never quiescence.
    """

    stop: Callable[[], None]
    start: Callable[[], None]
    is_active: Callable[[], bool]


def _find_hermes() -> str | None:
    candidates = (
        os.environ.get("HERMES_BIN") or "",
        shutil.which("hermes") or "",
        str(Path.home() / ".local" / "bin" / "hermes"),
    )
    for candidate in candidates:
        if candidate and Path(candidate).exists() and os.access(candidate, os.X_OK):
            return candidate
    return None


def default_gateway_controller(
    *,
    hermes_bin: str | None = None,
    system_systemctl: str = "/usr/bin/systemctl",
    user_systemctl: tuple[str, ...] = ("systemctl", "--user"),
    unit: str = "hermes-gateway.service",
    timeout_seconds: float = 120,
) -> GatewayController:
    """Production stop/start/probe wiring for the installed Hermes gateway.

    `is_active` observes supervisor truth through systemd, independently of
    what the hermes CLI returned, and fails closed: when no probe can run,
    the gateway is reported active so the coordinator refuses to proceed.
    """
    binary = hermes_bin or _find_hermes()
    if binary is None or not (
        Path(binary).exists() and os.access(binary, os.X_OK)
    ):
        raise RehearsalError(
            "hermes_unavailable",
            "no executable hermes binary was found for gateway control",
        )

    def _gateway(command: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # nosec B603
            [binary, "gateway", command],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )

    def stop() -> None:
        # Exit status is deliberately ignored: quiescence is attested by
        # is_active plus the coordinator's socket and singleton probes,
        # never by "stop returned 0".
        with contextlib.suppress(subprocess.TimeoutExpired, OSError):
            _gateway("stop")

    def start() -> None:
        try:
            completed = _gateway("start")
        except (subprocess.TimeoutExpired, OSError) as exc:
            raise RehearsalError("gateway_start_failed", str(exc)[:200]) from exc
        if completed.returncode != 0:
            raise RehearsalError(
                "gateway_start_failed",
                f"hermes gateway start exited {completed.returncode}",
            )

    def is_active() -> bool:
        observations: list[bool] = []
        if Path(system_systemctl).exists():
            with contextlib.suppress(subprocess.TimeoutExpired, OSError):
                completed = subprocess.run(  # nosec B603
                    [system_systemctl, "is-active", "--quiet", unit],
                    capture_output=True,
                    timeout=timeout_seconds,
                )
                observations.append(completed.returncode == 0)
        with contextlib.suppress(FileNotFoundError, subprocess.TimeoutExpired, OSError):
            completed = subprocess.run(  # nosec B603
                [*user_systemctl, "is-active", "--quiet", unit],
                capture_output=True,
                timeout=timeout_seconds,
            )
            observations.append(completed.returncode == 0)
        if not observations:
            # No supervisor probe available: fail closed.
            return True
        return any(observations)

    return GatewayController(stop=stop, start=start, is_active=is_active)


def default_descriptor(
    config: bridge_runtime.Config,
) -> domain_schema.SecurityDomainDescriptor:
    return domain_schema.SecurityDomainDescriptor(
        instance_uid=os.geteuid(),
        workspace_id=config.team_id,
        persona_id=config.persona_id,
        authorized_owner_ids=tuple(config.allowed_users),
        policy_generation=config.policy_generation,
    )


def resolve_legacy_endpoint(row: Any) -> domain_schema.LegacyEndpointRef:
    raw = json.loads(str(row["source_json"]))
    source, binding = bridge_runtime._canonical_source(
        str(row["source_kind"]),
        raw,
        allow_legacy=True,
    )
    try:
        endpoint_key = bridge_runtime.endpoint_identity_key(binding)
    except ValueError:
        endpoint_key = None
    return domain_schema.LegacyEndpointRef(
        endpoint_key=endpoint_key,
        candidate_endpoint_key=endpoint_key,
        endpoint_kind=binding.endpoint_kind,
        source_kind=str(row["source_kind"]),
        source_json=json.dumps(source, sort_keys=True, separators=(",", ":")),
        ref_version=binding.version,
        ready=str(row["binding_state"]) == "verified" and endpoint_key is not None,
        error_code=str(row["binding_error_code"] or "") or None,
    )


def validate_legacy_source(source_kind: str, source_json: str, ref_version: int) -> None:
    source = json.loads(source_json)
    _validated, binding = bridge_runtime._canonical_source(
        source_kind,
        source,
        allow_legacy=True,
    )
    if binding.version != ref_version:
        raise ValueError("legacy source version drifted during rollback")


def _acquire_lifecycle_lock(path: Path) -> int:
    """The installer's lifecycle flock, with identical safety semantics."""
    security.secure_state_directory(path.parent, create=True)
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
        ):
            raise RehearsalError(
                "lifecycle_lock_unsafe",
                "Tether installer lock is not a private regular file",
            )
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RehearsalError(
                "lifecycle_lock_busy",
                "another Tether install or schema operation is running",
            ) from exc
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


def _readonly_snapshot(database: Path) -> tuple[int, str, str]:
    """Schema version plus whole and preserved manifest digests, read-only."""
    connection = sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if str(connection.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
            raise RehearsalError("database_integrity_failed")
        if version == 17:
            manifest = domain_schema.logical_manifest_v17(connection)
        elif version == domain_schema.SCHEMA_VERSION:
            domain_schema.require_valid(connection)
            manifest = domain_schema.logical_manifest_v18(connection)
        else:
            raise RehearsalError("database_schema_unsupported")
        digest = hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return version, digest, domain_schema.preserved_manifest_digest(manifest)
    finally:
        connection.close()


def _copy_private(source: Path, destination: Path, expected_sha256: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(destination, flags, 0o600)
    os.close(descriptor)
    shutil.copyfile(source, destination)
    observed, _size = _sha256_file(destination)
    if observed != expected_sha256:
        raise RehearsalError("disposable_copy_digest_mismatch")


class RehearsalCoordinator:
    """Runs one rehearse operation; every durable phase survives SIGKILL."""

    def __init__(
        self,
        *,
        database: Path,
        socket_path: Path,
        state_root: Path,
        target_root: Path,
        predecessor_root: Path,
        descriptor: domain_schema.SecurityDomainDescriptor,
        controller: GatewayController,
        resolve_endpoint: Callable[[Any], domain_schema.LegacyEndpointRef] = resolve_legacy_endpoint,
        legacy_source_validator: Callable[[str, str, int], None] = validate_legacy_source,
        fault_inject: Callable[[str], None] | None = None,
    ):
        self.database = Path(database)
        self.socket_path = Path(socket_path)
        self.state_root = Path(state_root)
        self.target_root = Path(target_root)
        self.predecessor_root = Path(predecessor_root)
        self.descriptor = descriptor
        self.controller = controller
        self.resolve_endpoint = resolve_endpoint
        self.legacy_source_validator = legacy_source_validator
        self.fault_inject = fault_inject
        self.receipt_file = self.state_root / "schema" / "active.json"
        self.maintenance_file = self.state_root / "schema" / "maintenance"
        self.install_manifest = self.state_root / "current.tsv"
        self.backups_root = self.state_root / "backups" / "schema"
        self._receipt: dict[str, Any] | None = None

    def _mark(self, phase: str) -> None:
        if self.fault_inject is not None:
            self.fault_inject(phase)

    def _advance(self, to_phase: str, evidence: dict[str, Any] | None = None) -> None:
        # Not an assert: python -O strips those, and advancing a phase with no
        # receipt in hand would corrupt the operation ledger silently.
        if self._receipt is None:
            raise RehearsalError(
                "receipt_missing",
                "cannot advance a schema phase without an active receipt",
            )
        self._receipt = schema_receipt.advance(
            self.receipt_file,
            expect=self._receipt,
            to_phase=to_phase,
            evidence=evidence,
        )
        self._mark(to_phase)

    def _fail_safe(self, code: str) -> None:
        if self._receipt is None:
            return
        with contextlib.suppress(schema_receipt.ReceiptError, security.SecurityError, OSError):
            self._receipt = schema_receipt.advance(
                self.receipt_file,
                expect=self._receipt,
                to_phase="failed_safe",
                error_code=code,
            )

    def _attest_quiesced(self) -> None:
        if self.controller.is_active():
            raise RehearsalError(
                "gateway_still_active",
                "stop reported success but the gateway supervisor is still active",
            )
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.settimeout(1.0)
            try:
                probe.connect(str(self.socket_path))
            except (FileNotFoundError, ConnectionRefusedError, OSError):
                return
            raise RehearsalError(
                "broker_socket_still_accepting",
                "the Tether broker socket accepted a connection after stop",
            )
        finally:
            probe.close()

    def _prune_backups(self) -> None:
        """Keep only the newest _BACKUP_RETAIN receipt-bound backups.

        Runs before a new backup is taken, while the lifecycle lock and
        maintenance gate are held, so no concurrent operation can be relying
        on the files being removed. Failure to prune never blocks the
        rehearsal: a full disk surfaces later as an honest backup error
        rather than a silent skip of the verification itself.
        """
        with contextlib.suppress(OSError):
            backups = sorted(
                (path for path in self.backups_root.glob("*.db") if path.is_file()),
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
            for stale in backups[max(0, _BACKUP_RETAIN - 1):]:
                for sidecar in (stale, Path(f"{stale}-wal"), Path(f"{stale}-shm")):
                    with contextlib.suppress(OSError):
                        sidecar.unlink()

    def _synthetic_domain_cycle(self, database: Path) -> None:
        """One full domain cycle on a disposable copy; evidence only."""
        runtime = domain_runtime_module.DomainRuntime(database)
        owner = self.descriptor.canonical_owner_ids[0]
        endpoint = runtime.register_endpoint(
            endpoint_key=f"rehearsal-synthetic-{self._receipt['receipt_id']}",
            endpoint_kind="detached_native",
            source_kind="headless_run",
            source_json='{"run_id":"rehearsal"}',
            ref_version=1,
            descriptor=self.descriptor,
        )
        binding = runtime.bind_thread(
            endpoint_id=endpoint["endpoint_id"],
            team_id=self.descriptor.workspace_id,
            channel_id="C-rehearsal",
            thread_ts="1.0",
            owner_user_id=owner,
            idempotency_key=f"rehearsal-{self._receipt['receipt_id']}",
        )
        runtime.admit_turn(
            binding_id=binding["binding_id"],
            event_key=f"rehearsal-turn-{self._receipt['receipt_id']}",
            ordered_at="1.0",
            payload_inline="synthetic rehearsal turn",
        )
        attempt = runtime.schedule_next(endpoint["endpoint_id"])
        if attempt is None:
            raise RehearsalError(
                "synthetic_cycle_failed",
                "the migrated store did not schedule the synthetic turn",
            )
        for sequence, state in ((1, "accepted"), (2, "no_reply")):
            runtime.record_driver_receipt(
                attempt_id=attempt["attempt_id"],
                receipt_id=f"rehearsal-{attempt['attempt_id']}-{sequence}",
                lease_fence=attempt["lease_fence"],
                sequence=sequence,
                driver_incarnation="rehearsal:synthetic",
                operation="submit",
                request_id=attempt["driver_request_id"],
                watch_cursor=f"rehearsal:{sequence}",
                state=state,
                observed_at=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
            )
        status = runtime.attempt_status(attempt["attempt_id"])
        if status["state"] != "no_reply" or status["lease_open"]:
            raise RehearsalError(
                "synthetic_cycle_failed",
                "the synthetic attempt did not reach a clean terminal state",
            )
        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        try:
            violations = domain_schema.invariant_violations(connection)
        finally:
            connection.close()
        if violations:
            raise RehearsalError(
                "synthetic_cycle_failed",
                f"domain invariants violated after the synthetic cycle: {violations[:3]}",
            )

    def _validate_with_artifact(
        self,
        artifact_root: Path,
        database: Path,
        *,
        expected_build: str,
        expected_schema: int,
        scratch_home: Path,
    ) -> dict[str, Any]:
        entry = artifact_root / "schema_orchestrator.py"
        environment = {
            "PATH": "/usr/bin:/bin",
            "HOME": str(scratch_home),
            "LC_ALL": "C",
        }
        completed = subprocess.run(  # nosec B603
            [
                sys.executable,
                str(entry),
                "validate-store",
                "--database",
                str(database),
                "--operation-id",
                str(self._receipt["receipt_id"]) if self._receipt else "preflight",
            ],
            env=environment,
            cwd=str(artifact_root),
            capture_output=True,
            text=True,
            timeout=_VALIDATOR_TIMEOUT_SECONDS,
        )
        try:
            attestation = json.loads(completed.stdout.strip() or "{}")
        except json.JSONDecodeError as exc:
            raise RehearsalError(
                "artifact_validator_unreadable",
                "the pinned artifact validator produced no attestation",
            ) from exc
        if completed.returncode != 0 or not attestation.get("ok"):
            raise RehearsalError(
                "artifact_validation_failed",
                f"pinned artifact refused the store: {attestation.get('code', 'unknown')}",
            )
        if attestation.get("build_sha256") != expected_build:
            raise RehearsalError(
                "artifact_build_drift",
                "the artifact that validated the store is not the pinned artifact",
            )
        if attestation.get("schema_version") != expected_schema:
            raise RehearsalError(
                "artifact_schema_mismatch",
                "the validated store is not at the expected schema",
            )
        return attestation

    def run(self) -> dict[str, Any]:
        lifecycle_fd = _acquire_lifecycle_lock(self.state_root / "install.lock")
        try:
            try:
                return self._run_locked(lifecycle_fd)
            except RehearsalError as error:
                self._fail_safe(error.code)
                raise
            except (
                security.SecurityError,
                schema_receipt.ReceiptError,
                sqlite3.Error,
                subprocess.TimeoutExpired,
                OSError,
                RuntimeError,
                ValueError,
            ) as error:
                self._fail_safe("rehearsal_failed")
                raise RehearsalError("rehearsal_failed", str(error)[:500]) from error
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(lifecycle_fd, fcntl.LOCK_UN)
            os.close(lifecycle_fd)

    def _run_locked(self, lifecycle_fd: int) -> dict[str, Any]:
        del lifecycle_fd
        self._mark("preflight")
        self.descriptor.validate()
        existing, existing_error = schema_receipt.load(self.receipt_file)
        if existing is not None or existing_error is not None:
            raise RehearsalError(
                "operation_already_exists",
                "an unresolved schema operation receipt already exists",
            )
        if schema_receipt.maintenance_armed(self.maintenance_file):
            raise RehearsalError(
                "maintenance_already_armed",
                "the schema maintenance gate is already armed",
            )
        installed_manifest, manifest_error = schema_orchestrator._manifest_digest(
            self.install_manifest
        )
        if manifest_error is not None or installed_manifest is None:
            raise RehearsalError(
                "managed_install_unverified",
                "the installed Tether manifest failed verification",
            )
        predecessor_build = schema_orchestrator.runtime_build_sha256(self.predecessor_root)
        target_build = schema_orchestrator.runtime_build_sha256(self.target_root)
        database_identity = security.owned_file_identity(
            self.database,
            expected_mode=0o600,
        )
        self._receipt = schema_receipt.create(
            self.receipt_file,
            operation="rehearse",
            from_schema=17,
            to_schema=domain_schema.SCHEMA_VERSION,
            database_device=database_identity.device,
            database_inode=database_identity.inode,
            security_domain_id=self.descriptor.security_domain_id,
            predecessor_build_sha256=predecessor_build,
            target_build_sha256=target_build,
            installed_manifest_sha256=installed_manifest,
        )
        self._mark("planned")

        schema_receipt.arm_maintenance(self.maintenance_file)
        self._mark("maintenance_armed")

        self.controller.stop()
        self._attest_quiesced()
        self._advance("quiesced", {"gateway_inactive": True, "socket_refused": True})

        try:
            singleton_fd = bridge_runtime.acquire_database_singleton(self.database)
        except RuntimeError as exc:
            raise RehearsalError(
                "database_singleton_unavailable",
                "another process holds the Tether database singleton",
            ) from exc
        try:
            self._advance("singleton_acquired")

            live_identity = security.owned_file_identity(
                self.database,
                expected_mode=0o600,
            )
            if (
                live_identity.device != self._receipt["database_device"]
                or live_identity.inode != self._receipt["database_inode"]
            ):
                raise RehearsalError(
                    "database_identity_changed",
                    "the live database changed identity after planning",
                )
            live_schema, live_manifest, live_preserved = _readonly_snapshot(self.database)
            if live_schema != self._receipt["from_schema"]:
                raise RehearsalError("database_schema_unsupported")

            security.secure_state_directory(self.backups_root, create=True)
            self._prune_backups()
            backup_path = self.backups_root / f"{self._receipt['receipt_id']}.db"
            for sidecar in (
                backup_path,
                Path(f"{backup_path}-wal"),
                Path(f"{backup_path}-shm"),
            ):
                if sidecar.exists() or sidecar.is_symlink():
                    raise RehearsalError(
                        "backup_path_occupied",
                        "a file already occupies the receipt-bound backup path",
                    )
            domain_schema.backup_database(self.database, backup_path)
            self._mark("backup_written")
            backup_identity = security.owned_file_identity(
                backup_path,
                expected_mode=0o600,
            )
            backup_sha256, backup_bytes = _sha256_file(backup_path)
            backup_schema, backup_manifest, backup_preserved = _readonly_snapshot(backup_path)
            reverified = security.owned_file_identity(backup_path, expected_mode=0o600)
            if reverified != backup_identity:
                raise RehearsalError("backup_identity_changed")
            if backup_schema != live_schema or backup_manifest != live_manifest:
                raise RehearsalError(
                    "backup_manifest_mismatch",
                    "the verified backup does not match the live logical manifest",
                )
            self._advance(
                "backup_verified",
                {
                    "backup_sha256": backup_sha256,
                    "backup_bytes": backup_bytes,
                    "schema_version": backup_schema,
                    "logical_manifest_sha256": backup_manifest,
                    "preserved_manifest_sha256": backup_preserved,
                },
            )
        finally:
            if singleton_fd >= 0:
                with contextlib.suppress(OSError):
                    fcntl.flock(singleton_fd, fcntl.LOCK_UN)
                os.close(singleton_fd)

        if (
            schema_orchestrator.runtime_build_sha256(self.predecessor_root)
            != self._receipt["predecessor_build_sha256"]
            or schema_orchestrator.runtime_build_sha256(self.target_root)
            != self._receipt["target_build_sha256"]
        ):
            raise RehearsalError(
                "artifact_drift",
                "a pinned artifact changed after preflight",
            )

        with tempfile.TemporaryDirectory(
            prefix=f"rehearsal-{self._receipt['receipt_id']}-",
            dir=self.backups_root,
        ) as scratch_name:
            scratch_dir = Path(scratch_name)
            os.chmod(scratch_dir, 0o700)
            scratch_home = scratch_dir / "home"
            scratch_home.mkdir(mode=0o700)
            work_db = scratch_dir / "work.db"
            _copy_private(backup_path, work_db, backup_sha256)

            connection = sqlite3.connect(work_db)
            try:
                domain_schema.migrate_legacy_v17(
                    connection,
                    self.descriptor,
                    self.resolve_endpoint,
                )
            finally:
                connection.close()
            target_attestation = self._validate_with_artifact(
                self.target_root,
                work_db,
                expected_build=self._receipt["target_build_sha256"],
                expected_schema=self._receipt["to_schema"],
                scratch_home=scratch_home,
            )

            # Boot the target domain runtime against a copy of the migrated
            # store and run one full synthetic admit/schedule/receipt/terminal
            # cycle. The copy keeps the rollback path pristine; the cycle
            # proves the migrated schema is operable, not merely readable.
            cycle_db = scratch_dir / "cycle.db"
            work_digest, _work_bytes = _sha256_file(work_db)
            _copy_private(work_db, cycle_db, work_digest)
            self._synthetic_domain_cycle(cycle_db)

            connection = sqlite3.connect(work_db)
            try:
                domain_schema.rollback_v18_to_v17(
                    connection,
                    legacy_source_validator=self.legacy_source_validator,
                )
            finally:
                connection.close()
            predecessor_attestation = self._validate_with_artifact(
                self.predecessor_root,
                work_db,
                expected_build=self._receipt["predecessor_build_sha256"],
                expected_schema=self._receipt["from_schema"],
                scratch_home=scratch_home,
            )
            # Preservation is judged by the migration contract's key subset:
            # rollback intentionally retains the archived endpoint inventory,
            # so whole-manifest digests legitimately differ on populated
            # stores while every preserved record must round-trip exactly.
            if (
                predecessor_attestation.get("preserved_manifest_sha256")
                != backup_preserved
            ):
                raise RehearsalError(
                    "rollback_manifest_mismatch",
                    "the 18->17 rollback did not preserve the logical manifest",
                )
        self._advance(
            "runtime_verified",
            {
                "target_manifest_sha256": target_attestation.get(
                    "logical_manifest_sha256"
                ),
                "post_rollback_manifest_sha256": predecessor_attestation.get(
                    "logical_manifest_sha256"
                ),
                "post_rollback_preserved_sha256": predecessor_attestation.get(
                    "preserved_manifest_sha256"
                ),
                "synthetic_cycle": "ok",
            },
        )

        recomputed_manifest, manifest_error = schema_orchestrator._manifest_digest(
            self.install_manifest
        )
        if (
            manifest_error is not None
            or recomputed_manifest != self._receipt["installed_manifest_sha256"]
        ):
            raise RehearsalError(
                "installed_runtime_drift",
                "the installed predecessor changed during the rehearsal",
            )
        self._advance("resumed", {"installed_manifest_verified": True})

        self.controller.start()
        if not self.controller.is_active():
            raise RehearsalError(
                "gateway_restart_failed",
                "the predecessor gateway did not report active after restart",
            )
        self._advance("complete", {"gateway_active": True})
        schema_receipt.disarm_maintenance(self.maintenance_file)
        self._mark("maintenance_disarmed")
        return {
            "ok": True,
            "receipt_id": self._receipt["receipt_id"],
            "operation": "rehearse",
            "backup_sha256": backup_sha256,
            "logical_manifest_sha256": backup_manifest,
        }


def classify_recovery(state_root: Path) -> dict[str, Any]:
    """Exactly one classified state after any crash; never mutates anything."""
    receipt_file = Path(state_root) / "schema" / "active.json"
    maintenance_file = Path(state_root) / "schema" / "maintenance"
    receipt, error = schema_receipt.load(receipt_file)
    state = schema_receipt.classify(receipt, error)
    return {
        "state": state,
        "maintenance_armed": schema_receipt.maintenance_armed(maintenance_file),
        "admission_allowed": state in {"no_operation", "complete", "incomplete_resumed"},
        "receipt": schema_receipt.public_view(receipt) if receipt is not None else None,
    }
