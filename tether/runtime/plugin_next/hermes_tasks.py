"""Read an explicitly configured Hermes task without opening its writer facade.

Hermes owns task state and scheduling. This adapter supplies a snapshot to an
existing Tether turn; it never discovers boards, migrates schemas, or claims work.
"""
from __future__ import annotations

import importlib
from pathlib import Path
import re
import sqlite3
import stat
from types import MappingProxyType
from typing import Any, Mapping

from .task_context import TaskContextError, TaskRef


_TASK_FIELDS = ("id", "title", "body", "assignee", "status", "tenant", "result",
                "completion_contract", "current_run_id")
_ROW_FIELDS = {
    "comments": ("id", "task_id", "author", "body", "created_at"),
    "events": ("id", "task_id", "kind", "created_at", "run_id"),
    "runs": ("id", "task_id", "profile", "status", "started_at", "ended_at",
             "outcome", "summary", "error"),
    "attachments": ("id", "task_id", "filename", "content_type", "size",
                    "uploaded_by", "created_at"),
}
_ROW_ORDER = {"comments": "created_at", "events": "created_at",
              "runs": "started_at", "attachments": "created_at"}
_HANDOFF_FIELDS = frozenset({"reason", "summary", "implementer", "reviewer", "status",
                             "source_status", "revision", "commit", "pr_url", "artifact_ref",
                             "artifact_refs", "artifacts", "findings", "review_findings", "verification",
                             "published_pr", "head_sha", "classification", "ok", "checks", "required"})


def _field(row: Any, name: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(name, default)
    return getattr(row, name, default)


def _fields(row: Any, names: tuple[str, ...]) -> dict[str, Any]:
    return {name: _field(row, name) for name in names}


def _canonical():
    try:
        module = importlib.import_module("hermes_cli.kanban_db")
    except ImportError as error:
        raise TaskContextError("task_capability_unavailable", "Hermes task readers are unavailable on this host") from error
    required = ("get_task", "list_comments", "list_events", "list_runs", "list_attachments",
                "parent_ids", "child_ids")
    if any(not callable(getattr(module, name, None)) for name in required):
        raise TaskContextError("task_capability_unavailable", "Hermes lacks a required task read API")
    return module


def _identity(path: Path) -> tuple[Path, int, int]:
    resolved = path.resolve(strict=True)
    info = resolved.stat()
    if not stat.S_ISREG(info.st_mode):
        raise TaskContextError("task_source_unavailable", "The configured task source is not a regular database file")
    return resolved, info.st_dev, info.st_ino


class HermesTaskReader:
    """Allowlisted board paths and optional exact tenant, with no ambient fallback."""

    def __init__(self, board_paths: Mapping[str, Path], tenant: str | None = None):
        if not isinstance(board_paths, Mapping):
            raise TaskContextError("task_ref_invalid", "Task boards must map names to absolute database paths")
        paths = {}
        for board, value in board_paths.items():
            if not isinstance(board, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", board):
                raise TaskContextError("task_ref_invalid", "Configured task board names must be lowercase slugs")
            try:
                path = Path(value)
            except TypeError as error:
                raise TaskContextError("task_ref_invalid", "Task board database paths must be filesystem paths") from error
            if not path.is_absolute():
                raise TaskContextError("task_ref_invalid", "Task board database paths must be absolute")
            paths[board] = path
        if tenant is not None and (not isinstance(tenant, str) or not tenant.strip()):
            raise TaskContextError("task_ref_invalid", "Task tenant must be a nonempty string when configured")
        self.board_paths = MappingProxyType(paths)
        self.tenant = tenant

    def _task(self, api, connection, task_id: str):
        task = api.get_task(connection, task_id)
        if task is None:
            raise TaskContextError("task_not_found", "The linked Hermes task does not exist")
        if _field(task, "id") != task_id:
            raise TaskContextError("task_scope_mismatch", "Hermes returned a different task identity")
        if self.tenant is not None and _field(task, "tenant") != self.tenant:
            raise TaskContextError("task_scope_mismatch", "The linked task is outside the configured tenant")
        return task

    def read(self, ref: TaskRef) -> dict[str, Any]:
        if not isinstance(ref, TaskRef):
            raise TaskContextError("task_ref_invalid", "A typed board and task reference is required")
        path = self.board_paths.get(ref.board)
        if path is None:
            raise TaskContextError("task_board_unconfigured", "This task board has no configured database")
        api = _canonical()
        connection = None
        try:
            identity = _identity(path)
            connection = sqlite3.connect(identity[0].as_uri() + "?mode=ro", uri=True, timeout=2)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            task = self._task(api, connection, ref.task_id)
            result = {"reference": ref.as_dict(),
                      "task": _fields(task, _TASK_FIELDS)}
            for bucket, names in _ROW_FIELDS.items():
                rows = []
                source_rows = getattr(api, "list_" + bucket)(connection, ref.task_id)
                # Some host readers only order by timestamp. Break ties by row
                # ID so a stable board produces a stable snapshot and tail.
                source_rows = sorted(source_rows, key=lambda row: (
                    _field(row, _ROW_ORDER[bucket]) or 0, _field(row, "id") or 0))
                for row in source_rows:
                    if _field(row, "task_id") != ref.task_id:
                        raise TaskContextError("task_scope_mismatch", "Hermes returned a row belonging to another task")
                    projected = _fields(row, names)
                    extra = "payload" if bucket == "events" else "metadata" if bucket == "runs" else None
                    if extra:
                        raw = _field(row, extra)
                        if isinstance(raw, dict):
                            projected[extra] = {key: value for key, value in raw.items() if key in _HANDOFF_FIELDS}
                    rows.append(projected)
                result[bucket] = rows
            dependencies = []
            for relationship, method in (("parent", "parent_ids"), ("child", "child_ids")):
                for task_id in getattr(api, method)(connection, ref.task_id):
                    dependency = self._task(api, connection, task_id)
                    if _field(dependency, "tenant") != _field(task, "tenant"):
                        raise TaskContextError("task_scope_mismatch", "A linked dependency is outside the task tenant")
                    dependencies.append({"board": ref.board, "task_id": task_id,
                                         "relationship": relationship,
                                         **_fields(dependency, ("title", "status", "result"))})
            result["dependencies"] = dependencies
            if _identity(path) != identity:
                raise TaskContextError("task_source_changed", "The configured task database was replaced during the read")
            return result
        except TaskContextError:
            raise
        except (OSError, sqlite3.Error) as error:
            raise TaskContextError("task_source_unavailable", "The configured task database cannot be read") from error
        except (AttributeError, TypeError, ValueError) as error:
            raise TaskContextError("task_context_invalid", "Hermes returned incompatible task data") from error
        finally:
            if connection is not None:
                connection.close()
