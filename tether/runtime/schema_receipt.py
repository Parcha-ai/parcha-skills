"""One authoritative schema-operation receipt parser/writer.

Every schema operation (rehearsal now, live migration in a later slice) is
driven by exactly one receipt file. The runtime startup gate, the status
projection, and the rehearsal coordinator all read and write receipts through
this module; phase semantics exist nowhere else. Node and installer Bash only
test for the existence of the maintenance flag file and never parse phases.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


def _load_security_module() -> Any:
    path = Path(__file__).resolve().with_name("security.py")
    injected = sys.modules.get("security")
    if injected is not None:
        injected_path = getattr(injected, "__file__", "")
        with contextlib.suppress(OSError, TypeError, ValueError):
            if Path(injected_path).resolve() == path:
                return injected
    module_name = (
        "_tether_runtime_security_"
        + hashlib.sha256(str(path).encode()).hexdigest()[:16]
    )
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Tether security module could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


security = _load_security_module()


RECEIPT_VERSION = 1
OPERATIONS = frozenset({"migrate", "rollback", "rehearse"})
# Monotonic phase sequence. An operation may skip forward (rehearsal never
# reaches db_committed) but can never move backward.
PHASE_SEQUENCE = (
    "planned",
    "quiesced",
    "singleton_acquired",
    "backup_verified",
    "db_committed",
    "runtime_verified",
    "resumed",
    "complete",
)
# Terminal safe-hold phases: reachable from any incomplete phase, never left
# programmatically. They keep the runtime gate closed until an operator acts.
SAFE_HOLD_PHASES = frozenset({"failed_safe", "needs_operator"})
# Phases at which normal runtime startup may proceed. `resumed` is bootable by
# design: the live database was never mutated, the installed predecessor was
# validated, and the coordinator itself restarts the gateway before marking
# the receipt complete.
BOOTABLE_PHASES = frozenset({"resumed", "complete"})
IMMUTABLE_FIELDS = (
    "version",
    "receipt_id",
    "operation",
    "from_schema",
    "to_schema",
    "database_device",
    "database_inode",
    "instance_uid",
    "security_domain_id",
    "predecessor_build_sha256",
    "target_build_sha256",
    "installed_manifest_sha256",
    "created_at",
)
_MAX_RECEIPT_BYTES = 256 * 1024


class ReceiptError(RuntimeError):
    def __init__(self, code: str, message: str | None = None):
        super().__init__(message or code)
        self.code = code


def state_root() -> Path:
    home = Path(os.environ.get("HOME", str(Path.home()))).expanduser()
    state_home = Path(
        os.environ.get("XDG_STATE_HOME", str(home / ".local" / "state"))
    ).expanduser()
    return state_home / "tether-installer"


def receipt_path() -> Path:
    return state_root() / "schema" / "active.json"


def maintenance_path() -> Path:
    return state_root() / "schema" / "maintenance"


def _phase_index(phase: str) -> int:
    return PHASE_SEQUENCE.index(phase)


def _validate(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ReceiptError("invalid", "schema receipt is not an object")
    for field in IMMUTABLE_FIELDS:
        if field not in value:
            raise ReceiptError("invalid", f"schema receipt is missing {field}")
    if value["version"] != RECEIPT_VERSION:
        raise ReceiptError("unsupported_version")
    if value["operation"] not in OPERATIONS:
        raise ReceiptError("invalid", "unknown schema operation")
    receipt_id = value["receipt_id"]
    if not isinstance(receipt_id, str) or not receipt_id.isalnum() or len(receipt_id) != 32:
        raise ReceiptError("invalid", "schema receipt id is malformed")
    for field in ("from_schema", "to_schema", "database_device", "database_inode", "instance_uid"):
        if not isinstance(value[field], int) or value[field] < 0:
            raise ReceiptError("invalid", f"schema receipt {field} is malformed")
    phase = value.get("phase")
    if phase not in set(PHASE_SEQUENCE) | SAFE_HOLD_PHASES:
        raise ReceiptError("invalid", "unknown schema receipt phase")
    phases = value.get("phases")
    if not isinstance(phases, dict):
        raise ReceiptError("invalid", "schema receipt phase evidence is malformed")
    sequence = value.get("phase_seq")
    if not isinstance(sequence, int) or sequence < 0:
        raise ReceiptError("invalid", "schema receipt sequence is malformed")
    if not isinstance(value.get("error_code", ""), str):
        raise ReceiptError("invalid", "schema receipt error code is malformed")
    return value


def load(path: Path | None = None) -> tuple[dict[str, Any] | None, str | None]:
    """Return (receipt, None), (None, None) when absent, or (None, error_code)."""
    target = receipt_path() if path is None else path
    try:
        raw, _identity = security.read_owned_file_bytes(
            target,
            max_bytes=_MAX_RECEIPT_BYTES,
            expected_mode=0o600,
        )
    except FileNotFoundError:
        return None, None
    except security.StatePathError:
        return None, "unsafe"
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        return None, "invalid"
    try:
        return _validate(value), None
    except ReceiptError as error:
        return None, error.code


def public_view(receipt: dict[str, Any]) -> dict[str, Any]:
    """Redacted projection safe for status output: no paths, no evidence."""
    return {
        "version": receipt["version"],
        "receipt_id": str(receipt["receipt_id"]),
        "operation": receipt["operation"],
        "phase": receipt["phase"],
        "from_schema": receipt["from_schema"],
        "to_schema": receipt["to_schema"],
        "error_code": str(receipt.get("error_code") or ""),
    }


def classify(receipt: dict[str, Any] | None, error: str | None) -> str:
    """Exactly one recovery classification for any on-disk receipt state."""
    if error is not None:
        return "invalid_receipt"
    if receipt is None:
        return "no_operation"
    phase = receipt["phase"]
    if phase in SAFE_HOLD_PHASES:
        return phase
    if phase == "complete":
        return "complete"
    return f"incomplete_{phase}"


def _mutated_live_database(receipt: dict[str, Any]) -> bool:
    """True once an operation may have written to the live database.

    `db_committed` is the only phase whose evidence implies a live write, so
    its presence in the phase ledger — not merely the current phase — is the
    watermark. A receipt that never recorded it left the live database as it
    found it.
    """
    phases = receipt.get("phases")
    return isinstance(phases, dict) and "db_committed" in phases


def runtime_gate_error(
    *,
    runtime_schema_version: int,
    path: Path | None = None,
) -> str | None:
    """Reason the runtime must refuse to open the database, or None.

    Fail closed: an unreadable, invalid, or incomplete receipt blocks startup
    before SQLite is opened. The maintenance flag is advisory for installer
    tooling and does not gate runtime boot; the receipt is the authority.
    """
    receipt, error = load(path)
    if error is not None:
        return "schema_receipt_" + error
    if receipt is None:
        return None
    if receipt["phase"] in BOOTABLE_PHASES:
        pass
    elif receipt["operation"] == "rehearse" and not _mutated_live_database(receipt):
        # A rehearsal only ever transforms disposable copies; the live
        # database is untouched by construction, and the coordinator proves
        # it by never reaching db_committed. Such a receipt records evidence,
        # so it must not keep the predecessor broker from starting: a
        # read-only evidence run can never be allowed to take production
        # down. Live-mutating operations still gate the runtime.
        return None
    else:
        return "schema_operation_incomplete"
    if receipt["phase"] == "complete" and receipt["to_schema"] > runtime_schema_version:
        return "schema_receipt_runtime_conflict"
    return None


def _write_atomic(path: Path, value: dict[str, Any], *, create: bool) -> None:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    security.secure_state_directory(path.parent, create=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if create:
        descriptor = os.open(path, flags, 0o600)
        try:
            os.write(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    else:
        staging = path.with_name(path.name + f".tmp-{os.getpid()}")
        descriptor = os.open(staging, flags, 0o600)
        try:
            os.write(descriptor, payload)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            os.replace(staging, path)
        except BaseException:
            with contextlib.suppress(OSError):
                staging.unlink()
            raise
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def create(
    path: Path,
    *,
    operation: str,
    from_schema: int,
    to_schema: int,
    database_device: int,
    database_inode: int,
    security_domain_id: str,
    predecessor_build_sha256: str,
    target_build_sha256: str,
    installed_manifest_sha256: str,
) -> dict[str, Any]:
    if operation not in OPERATIONS:
        raise ReceiptError("invalid", "unknown schema operation")
    existing, error = load(path)
    if existing is not None or error is not None:
        raise ReceiptError(
            "operation_exists",
            "another schema operation receipt already exists",
        )
    receipt = {
        "version": RECEIPT_VERSION,
        "receipt_id": os.urandom(16).hex(),
        "operation": operation,
        "from_schema": from_schema,
        "to_schema": to_schema,
        "database_device": database_device,
        "database_inode": database_inode,
        "instance_uid": os.geteuid(),
        "security_domain_id": security_domain_id,
        "predecessor_build_sha256": predecessor_build_sha256,
        "target_build_sha256": target_build_sha256,
        "installed_manifest_sha256": installed_manifest_sha256,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "phase": "planned",
        "phase_seq": 0,
        "phases": {"planned": {"seq": 0}},
        "error_code": "",
    }
    _validate(receipt)
    _write_atomic(path, receipt, create=True)
    return receipt


def advance(
    path: Path,
    *,
    expect: dict[str, Any],
    to_phase: str,
    evidence: dict[str, Any] | None = None,
    error_code: str = "",
) -> dict[str, Any]:
    """Compare-and-swap one phase transition; refuses drift and regression."""
    current, error = load(path)
    if error is not None or current is None:
        raise ReceiptError("receipt_lost", "schema receipt disappeared or corrupted")
    for field in IMMUTABLE_FIELDS:
        if current[field] != expect[field]:
            raise ReceiptError("receipt_identity_changed", f"{field} changed on disk")
    if current["phase"] != expect["phase"] or current["phase_seq"] != expect["phase_seq"]:
        raise ReceiptError("receipt_phase_conflict", "another writer advanced the receipt")
    if current["phase"] in SAFE_HOLD_PHASES or current["phase"] == "complete":
        raise ReceiptError("receipt_terminal", "the schema receipt is terminal")
    if to_phase in SAFE_HOLD_PHASES:
        if not error_code:
            raise ReceiptError("invalid", "a safe-hold transition requires an error code")
    elif to_phase not in PHASE_SEQUENCE or _phase_index(to_phase) <= _phase_index(current["phase"]):
        raise ReceiptError("invalid", "schema receipt phases only advance forward")
    updated = dict(current)
    updated["phase"] = to_phase
    updated["phase_seq"] = current["phase_seq"] + 1
    updated["error_code"] = error_code
    phases = dict(current["phases"])
    record: dict[str, Any] = {"seq": updated["phase_seq"]}
    if evidence:
        record.update(evidence)
    phases[to_phase] = record
    updated["phases"] = phases
    _validate(updated)
    _write_atomic(path, updated, create=False)
    return updated


def resolve_safe_hold(
    path: Path,
    *,
    expect: dict[str, Any],
    resolution: str,
) -> dict[str, Any]:
    """Close out a terminal safe-hold receipt after operator inspection.

    A `failed_safe` / `needs_operator` receipt is deliberately terminal so no
    automated path can resume a schema operation whose outcome is unproven.
    But it must still be resolvable, or a failed run leaves the instance
    wedged with the documentation forbidding the only remedy. This is that
    remedy: it archives the receipt (preserving its evidence for the record)
    and clears the active slot so a fresh, fully re-checked operation can be
    planned. It never advances or resumes the held operation.
    """
    if resolution not in {"abandoned", "recovered"}:
        raise ReceiptError("invalid", "unknown safe-hold resolution")
    current, error = load(path)
    if error is not None or current is None:
        raise ReceiptError("receipt_lost", "schema receipt disappeared or corrupted")
    if current["phase"] not in SAFE_HOLD_PHASES:
        raise ReceiptError(
            "receipt_not_held",
            "only a failed_safe or needs_operator receipt can be resolved",
        )
    for field in IMMUTABLE_FIELDS:
        if current[field] != expect[field]:
            raise ReceiptError("receipt_identity_changed", f"{field} changed on disk")
    archived = dict(current)
    archived["resolution"] = resolution
    archived["resolved_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    archive_path = path.with_name(
        f"resolved-{current['receipt_id']}.json"
    )
    _write_atomic(archive_path, archived, create=True)
    path.unlink()
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return archived


def arm_maintenance(path: Path | None = None) -> None:
    target = maintenance_path() if path is None else path
    security.secure_state_directory(target.parent, create=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(target, flags, 0o600)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def maintenance_armed(path: Path | None = None) -> bool:
    target = maintenance_path() if path is None else path
    try:
        target.lstat()
    except FileNotFoundError:
        return False
    return True


def disarm_maintenance(path: Path | None = None) -> None:
    target = maintenance_path() if path is None else path
    try:
        target.unlink()
    except FileNotFoundError:
        return
    directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
