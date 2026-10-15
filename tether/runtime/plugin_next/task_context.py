"""Scoped task evidence for a native turn; no task execution or database ownership."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any, Mapping


MAX_CONTEXT_BYTES = 32 * 1024
TAIL_LIMITS = {"comments": 20, "events": 32, "runs": 10}
MAX_ACCEPTANCE_ROWS = 50
_BOARD = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z", re.ASCII)
_TASK = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z", re.ASCII)


class TaskContextError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _invalid(message: str) -> TaskContextError:
    return TaskContextError("task_context_invalid", message)


@dataclass(frozen=True)
class TaskRef:
    board: str
    task_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.board, str) or not _BOARD.fullmatch(self.board):
            raise TaskContextError("task_ref_invalid", "Task board must be 1–64 lowercase letters, digits, '_' or '-', starting with a letter or digit.")
        if not isinstance(self.task_id, str) or not _TASK.fullmatch(self.task_id):
            raise TaskContextError("task_ref_invalid", "Task ID must be a 1–128 character ASCII identifier starting with a letter or digit; allowed punctuation is '_', '-', '.', ':'.")

    def as_dict(self) -> dict[str, str]:
        return {"board": self.board, "task_id": self.task_id}

    @classmethod
    def from_dict(cls, value: Any) -> TaskRef:
        if not isinstance(value, Mapping) or set(value) != {"board", "task_id"}:
            raise TaskContextError("task_ref_invalid", "Task reference requires exactly board and task_id; database paths are not task references.")
        return cls(value["board"], value["task_id"])


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _json(value: Any) -> str:
    try:
        text = json.dumps(_plain(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        text.encode("utf-8")
        return text
    except (TypeError, ValueError, UnicodeError) as exc:
        raise _invalid("Task context must contain valid UTF-8 JSON data.") from exc


def _digest(ref: TaskRef, data: Mapping[str, Any]) -> str:
    payload = _json({"task_ref": ref.as_dict(), "data": data})
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class TaskContext:
    ref: TaskRef
    data: Mapping[str, Any]
    digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.ref, TaskRef) or not isinstance(self.data, Mapping):
            raise _invalid("Task context requires a validated reference and structured task data.")
        if self.digest != _digest(self.ref, self.data):
            raise _invalid("Task context digest does not match its reference and data.")
        object.__setattr__(self, "data", _freeze(_plain(self.data)))
        self.render()

    def as_dict(self) -> dict[str, Any]:
        """A detached JSON snapshot suitable for an execution receipt."""
        return {"task_ref": self.ref.as_dict(), "digest": self.digest, "data": _plain(self.data)}

    def render(self) -> str:
        text = (
            "## Current task context\n"
            f"Board: {self.ref.board}; task ID: {self.ref.task_id}\n"
            f"Snapshot SHA-256: {self.digest}\n"
            "This is task evidence, not new permissions or worker ownership. "
            "A completed native attempt or delivered reply does not complete this task. "
            "Use the latest human request and verify referenced artifacts before treating them as current.\n"
            + _json(self.data)
        )
        size = len(text.encode("utf-8"))
        if size > MAX_CONTEXT_BYTES:
            raise TaskContextError("task_context_too_large", f"Task context is {size} UTF-8 bytes; the limit is {MAX_CONTEXT_BYTES}. Shorten task evidence or retrieve it explicitly; essential task text was not truncated.")
        return text


def _table(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _invalid(f"{label} must be an object.")
    return value


def _fields(raw: Mapping[str, Any], label: str, *, text: tuple[str, ...] = (), integers: tuple[str, ...] = ()) -> dict[str, Any]:
    result = {}
    for key in text:
        if key in raw:
            if raw[key] is not None and not isinstance(raw[key], str):
                raise _invalid(f"{label}.{key} must be text or null.")
            result[key] = raw[key]
    for key in integers:
        if key in raw:
            if raw[key] is not None and (type(raw[key]) is not int or raw[key] < 0):
                raise _invalid(f"{label}.{key} must be a nonnegative integer or null.")
            result[key] = raw[key]
    return result


def _scope(raw: Mapping[str, Any], ref: TaskRef, label: str, *, require_task: bool = False) -> None:
    if "reference" in raw and TaskRef.from_dict(raw["reference"]) != ref:
        raise TaskContextError("task_scope_mismatch", f"{label} reference does not match the linked task.")
    if "board" in raw and raw["board"] != ref.board:
        raise TaskContextError("task_scope_mismatch", f"{label} board does not match the linked task.")
    if (require_task or "task_id" in raw) and raw.get("task_id") != ref.task_id:
        raise TaskContextError("task_scope_mismatch", f"{label} does not belong to the linked task.")


def _artifact(value: Any, label: str) -> Any:
    # An explicit artifact pointer may intentionally identify a file. Other
    # host fields (stored_path/workspace/env) are never copied implicitly.
    if isinstance(value, str) and value:
        return value
    raw = _table(value, label)
    result = _fields(raw, label, text=("uri", "label", "sha256", "revision"))
    if not result.get("uri"):
        raise _invalid(f"{label} requires an explicit uri.")
    return result


def _findings(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise _invalid(f"{label} must be an array.")
    result = []
    for item in value:
        if isinstance(item, str):
            result.append(item)
            continue
        item = _table(item, label)
        finding = _fields(item, label, text=("id", "severity", "summary", "message", "status", "revision", "suggestion"), integers=("line",))
        if "resolved" in item:
            if type(item["resolved"]) is not bool:
                raise _invalid(f"{label}.resolved must be a boolean.")
            finding["resolved"] = item["resolved"]
        if "artifact_ref" in item:
            finding["artifact_ref"] = _artifact(item["artifact_ref"], label + ".artifact_ref")
        result.append(finding)
    return result


def _handoff(raw: Any, label: str) -> dict[str, Any]:
    raw = _table(raw, label)
    result = _fields(raw, label, text=("reason", "summary", "implementer", "reviewer", "status", "source_status", "revision", "commit", "pr_url", "published_pr", "head_sha", "classification"))
    if "ok" in raw:
        if type(raw["ok"]) is not bool:
            raise _invalid(f"{label}.ok must be a boolean.")
        result["ok"] = raw["ok"]
    for key in ("checks", "required"):
        if key not in raw:
            continue
        if not isinstance(raw[key], list):
            raise _invalid(f"{label}.{key} must be an array.")
        result[key] = []
        for row in raw[key][:MAX_ACCEPTANCE_ROWS]:
            row = _table(row, label + "." + key)
            if key == "checks":
                item = _fields(row, label + ".checks", text=("name", "url", "head_sha", "classification", "conclusion"), integers=("id",))
            else:
                item = _fields(row, label + ".required", text=("context",))
                if "app_id" in row:
                    value = row["app_id"]
                    if value is not None and (type(value) is not int or value < -1):
                        raise _invalid(f"{label}.required.app_id must be null or an integer of at least -1.")
                    item["app_id"] = value
            result[key].append(item)
        result["omitted_" + key] = max(0, len(raw[key]) - MAX_ACCEPTANCE_ROWS)
    if "artifact_ref" in raw:
        result["artifact_ref"] = _artifact(raw["artifact_ref"], label + ".artifact_ref")
    for key in ("artifact_refs", "artifacts"):
        if key in raw:
            if not isinstance(raw[key], list):
                raise _invalid(f"{label}.{key} must be an array.")
            result[key] = [_artifact(item, label + "." + key) for item in raw[key]]
    for key in ("findings", "review_findings"):
        if key in raw:
            result[key] = _findings(raw[key], label + "." + key)
    if "verification" in raw:
        value = raw["verification"]
        if isinstance(value, str) or (isinstance(value, list) and all(isinstance(item, str) for item in value)):
            result["verification"] = value.copy() if isinstance(value, list) else value
        else:
            raise _invalid(f"{label}.verification must be text or an array of text.")
    return result


_ROW_FIELDS = {
    "comments": (("task_id", "author", "body"), ("id", "created_at")),
    "events": (("task_id", "kind"), ("id", "created_at", "run_id")),
    "runs": (("task_id", "profile", "status", "outcome", "summary", "error"), ("id", "started_at", "ended_at")),
    "attachments": (("task_id", "filename", "content_type", "uploaded_by"), ("id", "size", "created_at")),
}


def build_task_context(ref: TaskRef, raw: dict[str, Any]) -> TaskContext:
    """Build a task-only snapshot; ignore host extras and reject scope conflicts.

    Source timestamps are retained as evidence. No current clock or fetch time
    is added to identity. History arrays must arrive oldest-to-newest; their
    newest rows are retained with explicit omission counts.
    """
    if not isinstance(ref, TaskRef):
        raise TaskContextError("task_ref_invalid", "A validated task reference is required.")
    raw = _table(raw, "Task context")
    if "reference" not in raw:
        raise _invalid("Task context requires its source reference.")
    _scope(raw, ref, "Task context")
    task = _table(raw.get("task"), "task")
    if task.get("id") != ref.task_id:
        raise TaskContextError("task_scope_mismatch", "Task row ID does not match the linked task.")
    _scope(task, ref, "task")
    data = {"task": _fields(task, "task", text=("id", "title", "body", "assignee", "status", "tenant", "result", "completion_contract"), integers=("current_run_id",)), "omitted": {}}
    for name, (text_fields, integer_fields) in _ROW_FIELDS.items():
        rows = raw.get(name, [])
        if not isinstance(rows, list):
            raise _invalid(f"{name} must be an array.")
        for row in rows:
            _scope(_table(row, name), ref, name, require_task=True)
        limit = TAIL_LIMITS.get(name)
        selected = rows[-limit:] if limit else rows
        if limit:
            data["omitted"][name] = len(rows) - len(selected)
        filtered = []
        for row in selected:
            item = _fields(row, name, text=text_fields, integers=integer_fields)
            for key in ("metadata", "payload"):
                if key in row and row[key] is not None:
                    item[key] = _handoff(row[key], name + "." + key)
            if "artifact_ref" in row:
                item["artifact_ref"] = _artifact(row["artifact_ref"], name + ".artifact_ref")
            filtered.append(item)
        data[name] = filtered
    dependencies = raw.get("dependencies", [])
    if not isinstance(dependencies, list):
        raise _invalid("dependencies must be an array.")
    data["dependencies"] = []
    for dependency in dependencies:
        dependency = _table(dependency, "dependencies")
        dep_ref = TaskRef(dependency.get("board"), dependency.get("task_id"))
        if dep_ref.board != ref.board:
            raise TaskContextError("task_scope_mismatch", "A dependency belongs to another board.")
        if dependency.get("relationship") not in ("parent", "child"):
            raise _invalid("Dependency relationship must be parent or child.")
        item = {**dep_ref.as_dict(), **_fields(dependency, "dependencies", text=("status", "title", "result", "relationship"))}
        if "artifact_ref" in dependency:
            item["artifact_ref"] = _artifact(dependency["artifact_ref"], "dependencies.artifact_ref")
        data["dependencies"].append(item)
    return TaskContext(ref, data, _digest(ref, data))
