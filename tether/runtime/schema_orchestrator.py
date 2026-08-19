from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import bridge_runtime
import domain_control
import domain_schema
import schema_receipt
import security


ORCHESTRATOR_VERSION = 1
TARGET_SCHEMA_VERSION = domain_schema.SCHEMA_VERSION
MANIFEST_HEADER = "# tether-manifest-v2"
MANIFEST_METADATA_KEYS = frozenset(
    {
        "harness",
        "runtime_home",
        "plugin_home",
        "local_bin",
        "codex_root",
        "claude_root",
        "legacy",
    }
)
MANIFEST_ROW = re.compile(r"^(/[^\t]*)\t([0-7]{3,4})\t([0-9a-f]{64})$")


@dataclass(frozen=True)
class SchemaPaths:
    database: Path
    receipt: Path
    install_manifest: Path


class SchemaStatusError(RuntimeError):
    pass


def default_paths() -> SchemaPaths:
    home = Path(os.environ.get("HOME", str(Path.home()))).expanduser()
    hermes_home = Path(os.environ.get("HERMES_HOME", str(home / ".hermes"))).expanduser()
    state_home = Path(
        os.environ.get("XDG_STATE_HOME", str(home / ".local" / "state"))
    ).expanduser()
    return SchemaPaths(
        database=hermes_home / "bridges.db",
        receipt=state_home / "tether-installer" / "schema" / "active.json",
        install_manifest=state_home / "tether-installer" / "current.tsv",
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    return _sha256_bytes(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )


def _condition(
    reason_code: str,
    *,
    runtime: bool,
    migration: bool,
    actions: tuple[str, ...] = (),
    next_action: str,
) -> domain_control.BlockingCondition:
    return domain_control.BlockingCondition(
        condition_id="schema_" + _sha256_json(reason_code)[:20],
        revision=_sha256_json(
            {
                "reason_code": reason_code,
                "runtime": runtime,
                "migration": migration,
                "actions": actions,
                "next_action": next_action,
            }
        ),
        category="schema",
        reason_code=reason_code,
        scope="instance",
        workspace_id="",
        security_domain_id="",
        endpoint_id=None,
        binding_id=None,
        attempt_id=None,
        blocked_since="",
        age_seconds=0,
        blocked_turn_count=0,
        impacts_readiness=runtime,
        impacts_migration=migration,
        operator_resolvable=bool(actions),
        allowed_actions=actions,
        next_action_code=next_action,
    )


def _read_owned(
    path: Path,
    *,
    max_bytes: int,
    expected_mode: int | None = None,
) -> tuple[bytes | None, str | None]:
    try:
        raw, _identity = security.read_owned_file_bytes(
            path,
            max_bytes=max_bytes,
            expected_mode=expected_mode,
        )
        return raw, None
    except FileNotFoundError:
        return None, "missing"
    except security.StatePathError:
        return None, "unsafe"


def _manifest_digest(path: Path) -> tuple[str | None, str | None]:
    raw, error = _read_owned(path, max_bytes=4 * 1024 * 1024, expected_mode=0o600)
    if raw is None:
        return None, error
    try:
        lines = [line for line in raw.decode("utf-8").splitlines() if line]
    except UnicodeError:
        return None, "invalid"
    if not lines or lines[0] != MANIFEST_HEADER or len(lines) > 10_000:
        return None, "invalid"
    metadata: dict[str, str] = {}
    offset = 1
    while offset < len(lines) and lines[offset].startswith("@"):
        fields = lines[offset].split("\t")
        key = fields[0][1:]
        if (
            len(fields) != 2
            or key not in MANIFEST_METADATA_KEYS
            or key in metadata
            or not fields[1]
        ):
            return None, "invalid"
        metadata[key] = fields[1]
        offset += 1
    if set(metadata) != MANIFEST_METADATA_KEYS:
        return None, "invalid"
    records: dict[Path, tuple[int, str]] = {}
    for line in lines[offset:]:
        match = MANIFEST_ROW.fullmatch(line)
        if match is None:
            return None, "invalid"
        target = Path(match.group(1))
        if target in records:
            return None, "invalid"
        records[target] = (int(match.group(2), 8), match.group(3))
    try:
        expected_modes = _expected_manifest_target_modes(metadata)
    except ValueError:
        return None, "invalid"
    if set(records) != set(expected_modes):
        return None, "incomplete"
    for target, (declared_mode, digest) in records.items():
        canonical_mode = expected_modes[target]
        if (
            declared_mode & ~canonical_mode
            or (canonical_mode & 0o100 and not declared_mode & 0o100)
        ):
            return None, "invalid"
        try:
            content, identity = security.read_owned_file_bytes(
                target,
                max_bytes=16 * 1024 * 1024,
            )
        except (FileNotFoundError, security.StatePathError):
            return None, "drifted"
        if (
            identity.mode & ~canonical_mode
            or not identity.mode & 0o400
            or (canonical_mode & 0o100 and not identity.mode & 0o100)
        ):
            return None, "drifted"
        if _sha256_bytes(content) != digest:
            return None, "drifted"
    return _sha256_bytes(raw), None


def _expected_manifest_target_modes(metadata: dict[str, str]) -> dict[Path, int]:
    harness = metadata["harness"]
    if harness not in {"codex", "claude-code", "both"}:
        raise ValueError("invalid harness")
    roots = {
        key: Path(metadata[key])
        for key in (
            "runtime_home",
            "plugin_home",
            "local_bin",
            "codex_root",
            "claude_root",
        )
    }
    if any(not value.is_absolute() for value in roots.values()):
        raise ValueError("manifest roots must be absolute")
    legacy = set() if metadata["legacy"] == "none" else set(metadata["legacy"].split(","))
    if not legacy.issubset({"codex", "claude-code"}):
        raise ValueError("invalid legacy harness")
    if "codex" in legacy and harness not in {"codex", "both"}:
        raise ValueError("invalid Codex compatibility shim")
    if "claude-code" in legacy and harness not in {"claude-code", "both"}:
        raise ValueError("invalid Claude compatibility shim")
    runtime = roots["runtime_home"]
    targets = {
        runtime / "bridge_runtime.py": 0o600,
        runtime / "domain_control.py": 0o600,
        runtime / "domain_schema.py": 0o600,
        runtime / "schema_orchestrator.py": 0o600,
        runtime / "schema_receipt.py": 0o600,
        runtime / "schema_rehearsal.py": 0o600,
        runtime / "domain_runtime.py": 0o600,
        runtime / "native_driver.py": 0o600,
        runtime / "hermes_compat.py": 0o600,
        runtime / "routing.py": 0o600,
        runtime / "security.py": 0o600,
        runtime / "slack_protocol.py": 0o600,
        runtime / "tether_notify.py": 0o700,
        runtime / "install.sh": 0o700,
        runtime / "package.json": 0o600,
        runtime / "herdr-plugin/herdr-plugin.toml": 0o644,
        runtime / "herdr-plugin/tether_plugin.py": 0o700,
        runtime / "herdr-plugin/README.md": 0o644,
        roots["plugin_home"] / "__init__.py": 0o600,
        roots["plugin_home"] / "plugin.yaml": 0o644,
        roots["local_bin"] / "tether": 0o700,
    }

    def add_skill(root: Path, include_legacy: bool) -> None:
        skill = root / "skills" / "tether"
        targets.update({
            skill / "SKILL.md": 0o644,
            skill / "agents/openai.yaml": 0o644,
            skill / "references/setup.md": 0o644,
            skill / "references/contract.md": 0o644,
            skill / "scripts/tether_notify.py": 0o700,
        })
        if include_legacy:
            compatibility = root / "skills" / "hermes-slack-bridge"
            targets.update({
                compatibility / "SKILL.md": 0o644,
                compatibility / "scripts/hermes_notify.py": 0o700,
            })

    if harness in {"codex", "both"}:
        add_skill(roots["codex_root"], "codex" in legacy)
    if harness in {"claude-code", "both"}:
        add_skill(roots["claude_root"], "claude-code" in legacy)
    return targets


def _expected_manifest_targets(metadata: dict[str, str]) -> set[Path]:
    return set(_expected_manifest_target_modes(metadata))


def _database_snapshot(
    path: Path,
) -> tuple[
    int | None,
    str | None,
    domain_control.BlockingSnapshot | None,
    list[domain_control.BlockingCondition],
]:
    conditions: list[domain_control.BlockingCondition] = []
    try:
        before_identity = security.owned_file_identity(path, expected_mode=0o600)
    except FileNotFoundError:
        before_identity = None
        error = "missing"
    except security.StatePathError:
        before_identity = None
        error = "unsafe"
    if before_identity is None:
        reason = "database_missing" if error == "missing" else "database_file_unsafe"
        conditions.append(
            _condition(
                reason,
                runtime=True,
                migration=True,
                next_action="repair_database_path",
            )
        )
        return None, None, None, conditions
    connection: sqlite3.Connection | None = None
    try:
        uri = f"{path.as_uri()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        schema_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if str(connection.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
            raise SchemaStatusError("database quick check failed")
        if schema_version == 17:
            manifest = domain_schema.logical_manifest_v17(connection)
        elif schema_version == TARGET_SCHEMA_VERSION:
            domain_schema.require_valid(connection)
            manifest = domain_schema.logical_manifest_v18(connection)
        else:
            manifest = None
            conditions.append(
                _condition(
                    "database_schema_unsupported",
                    runtime=True,
                    migration=True,
                    next_action="install_compatible_runtime",
                )
            )
        digest = _sha256_json(manifest) if manifest is not None else None
        domain_snapshot = (
            domain_control.blocking_snapshot(connection)
            if schema_version == TARGET_SCHEMA_VERSION
            else None
        )
        after_identity = security.owned_file_identity(path, expected_mode=0o600)
        if after_identity != before_identity:
            raise SchemaStatusError("database changed during validation")
        return schema_version, digest, domain_snapshot, conditions
    except (sqlite3.Error, RuntimeError, SchemaStatusError, ValueError):
        conditions.append(
            _condition(
                "database_validation_failed",
                runtime=True,
                migration=True,
                next_action="run_tether_doctor",
            )
        )
        return None, None, None, conditions
    finally:
        if connection is not None:
            connection.close()


def _descriptor_conditions(
    config: bridge_runtime.Config,
) -> list[domain_control.BlockingCondition]:
    missing = []
    if not config.team_id:
        missing.append("workspace")
    if not config.allowed_users:
        missing.append("authorized_owners")
    if not config.persona_id:
        missing.append("persona")
    if config.policy_generation < 1:
        missing.append("policy_generation")
    if not missing:
        return []
    return [
        _condition(
            "security_domain_descriptor_incomplete",
            runtime=False,
            migration=True,
            next_action="configure_security_domain_identity",
        )
    ]


def schema_status(
    paths: SchemaPaths,
    *,
    config: bridge_runtime.Config,
    runtime_schema_version: int = bridge_runtime.SCHEMA_VERSION,
) -> dict[str, Any]:
    conditions: list[domain_control.BlockingCondition] = []
    schema_version, logical_manifest, domain_snapshot, database_conditions = (
        _database_snapshot(paths.database)
    )
    conditions.extend(database_conditions)
    conditions.extend(_descriptor_conditions(config))
    if domain_snapshot is not None:
        conditions.extend(domain_snapshot.conditions)

    install_manifest_digest, manifest_error = _manifest_digest(paths.install_manifest)
    if manifest_error is not None:
        conditions.append(
            _condition(
                "installed_manifest_unavailable",
                runtime=False,
                migration=True,
                next_action="reinstall_tether",
            )
        )

    raw_receipt, receipt_error = schema_receipt.load(paths.receipt)
    receipt = schema_receipt.public_view(raw_receipt) if raw_receipt is not None else None
    if receipt_error is not None:
        conditions.append(
            _condition(
                "schema_receipt_invalid",
                runtime=True,
                migration=True,
                actions=("inspect",),
                next_action="inspect_schema_receipt",
            )
        )
    elif receipt is not None and receipt["phase"] != "complete":
        conditions.append(
            _condition(
                "schema_operation_incomplete",
                runtime=receipt["phase"] not in schema_receipt.BOOTABLE_PHASES,
                migration=True,
                actions=("inspect",),
                next_action="inspect_schema_receipt",
            )
        )
    elif (
        receipt is not None
        and receipt["to_schema"] > runtime_schema_version
    ):
        conditions.append(
            _condition(
                "schema_receipt_runtime_conflict",
                runtime=True,
                migration=True,
                next_action="install_compatible_runtime",
            )
        )
    if schema_receipt.maintenance_armed(paths.receipt.parent / "maintenance"):
        conditions.append(
            _condition(
                "schema_maintenance_armed",
                runtime=False,
                migration=True,
                actions=("inspect",),
                next_action="inspect_schema_receipt",
            )
        )

    if schema_version is not None and schema_version > runtime_schema_version:
        conditions.append(
            _condition(
                "runtime_schema_incompatible",
                runtime=True,
                migration=True,
                next_action="install_compatible_runtime",
            )
        )
    if runtime_schema_version < TARGET_SCHEMA_VERSION and schema_version == 17:
        conditions.append(
            _condition(
                "target_runtime_capability_missing",
                runtime=False,
                migration=True,
                next_action="install_schema_18_runtime",
            )
        )
    if schema_version == 17:
        conditions.append(
            _condition(
                "schema_mutation_unavailable",
                runtime=False,
                migration=True,
                next_action="complete_schema_orchestration_gates",
            )
        )

    conditions.sort(key=lambda condition: condition.condition_id)
    runtime_blockers = [condition for condition in conditions if condition.impacts_readiness]
    migration_blockers = [condition for condition in conditions if condition.impacts_migration]
    return {
        "ok": not runtime_blockers,
        "implementation": "tether-schema",
        "orchestrator_version": ORCHESTRATOR_VERSION,
        "database_schema_version": schema_version,
        "runtime_schema_version": runtime_schema_version,
        "target_schema_version": TARGET_SCHEMA_VERSION,
        "runtime_ready": not runtime_blockers,
        "migration_ready": schema_version == 17 and not migration_blockers,
        "migration_capabilities": {
            "target_runtime": runtime_schema_version >= TARGET_SCHEMA_VERSION,
            "managed_install_verified": manifest_error is None,
            "quiesce_and_singleton": False,
            "receipt_bound_backup": False,
            "predecessor_boot_verified": False,
            "writer_isolation_attested": False,
        },
        "logical_manifest_sha256": logical_manifest,
        "installed_manifest_sha256": install_manifest_digest,
        "active_receipt": receipt,
        "conditions": [condition.as_dict() for condition in conditions],
        "domain": domain_snapshot.as_dict() if domain_snapshot is not None else None,
    }


def runtime_build_sha256(runtime_root: Path | None = None) -> str:
    """Exact-code identity of this installed runtime artifact."""
    root = Path(__file__).resolve().parent if runtime_root is None else runtime_root
    entries = []
    for candidate in sorted(root.glob("*.py")):
        content, _identity = security.read_owned_file_bytes(
            candidate,
            max_bytes=16 * 1024 * 1024,
        )
        entries.append((candidate.name, _sha256_bytes(content)))
    if not entries:
        raise SchemaStatusError("runtime artifact contains no Python sources")
    return _sha256_json(entries)


def validate_store(database: Path, *, operation_id: str) -> dict[str, Any]:
    """Side-effect-free attestation of one database against this artifact.

    Opens SQLite strictly read-only and never instantiates `Store`, which
    migrates and recovers on open. Emits only hashes and versions.
    """
    result: dict[str, Any] = {
        "ok": False,
        "implementation": "tether-schema",
        "operation_id": str(operation_id)[:64],
        "runtime_schema_version": bridge_runtime.SCHEMA_VERSION,
        "build_sha256": runtime_build_sha256(),
        "schema_version": None,
        "logical_manifest_sha256": None,
    }
    schema_version, logical_manifest, _snapshot, conditions = _database_snapshot(database)
    result["schema_version"] = schema_version
    result["logical_manifest_sha256"] = logical_manifest
    if conditions:
        result["code"] = conditions[0].reason_code
        return result
    if schema_version not in {17, TARGET_SCHEMA_VERSION} or logical_manifest is None:
        result["code"] = "database_schema_unsupported"
        return result
    result["ok"] = True
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tether schema")
    subparsers = parser.add_subparsers(dest="command", required=True)
    status = subparsers.add_parser("status")
    status.add_argument("--json", action="store_true")
    validate = subparsers.add_parser("validate-store")
    validate.add_argument("--database", required=True)
    validate.add_argument("--operation-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.command == "validate-store":
        database = Path(arguments.database)
        if not database.is_absolute():
            print(json.dumps({"ok": False, "code": "database_path_not_absolute"}))
            return 1
        try:
            attestation = validate_store(database, operation_id=arguments.operation_id)
        except (OSError, ValueError, security.SecurityError, SchemaStatusError) as error:
            attestation = {
                "ok": False,
                "code": "validate_store_failed",
                "message": str(error)[:500],
            }
        print(json.dumps(attestation, sort_keys=True, separators=(",", ":")))
        return 0 if attestation.get("ok") else 1
    if arguments.command != "status":
        raise SchemaStatusError("unsupported schema operation")
    try:
        config = bridge_runtime.load_config()
        result = schema_status(default_paths(), config=config)
    except (security.SecurityError, bridge_runtime.security.SecurityError):
        result = {
            "ok": False,
            "implementation": "tether-schema",
            "orchestrator_version": ORCHESTRATOR_VERSION,
            "code": "config_file_unsafe",
            "message": "Tether configuration failed private-path validation",
        }
    except (OSError, ValueError, SchemaStatusError) as error:
        result = {
            "ok": False,
            "implementation": "tether-schema",
            "orchestrator_version": ORCHESTRATOR_VERSION,
            "code": "schema_status_failed",
            "message": str(error)[:500],
        }
    if arguments.json:
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    else:
        state = "ready" if result.get("runtime_ready") else "blocked"
        print(
            "Tether schema "
            f"database={result.get('database_schema_version', 'unknown')} "
            f"runtime={result.get('runtime_schema_version', 'unknown')} "
            f"state={state}"
        )
        for condition in result.get("conditions", []):
            print(
                f"{condition['reason_code']}: "
                f"next={condition['next_action_code']}"
            )
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
