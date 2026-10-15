"""Portable colleague context, not runtime, account or authorization settings."""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import tomllib
from typing import Any, Mapping

MAX_PROMPT_CHARS = 4000
MAX_MANIFEST_BYTES = 65536
_ID = re.compile(r"[a-z][a-z0-9_-]{0,47}\Z")
_SLACK_ID = re.compile(r"[UW][A-Z0-9]{2,31}\Z")
_CONTRACT_PATH = Path(__file__).with_name("team.md")


class TeamConfigError(ValueError):
    """A configured team cannot be represented faithfully in the prompt."""


@dataclass(frozen=True)
class ProjectRef:
    id: str
    ref: str


@dataclass(frozen=True)
class Colleague:
    id: str
    name: str
    role: str
    slack_id: str | None = None
    computer: str | None = None
    projects: tuple[str, ...] = ()


@dataclass(frozen=True)
class TeamManifest:
    version: int = 1
    self_id: str | None = None
    colleagues: tuple[Colleague, ...] = ()
    projects: tuple[ProjectRef, ...] = ()
    source: Path | None = None

    @property
    def selected(self) -> Colleague | None:
        return next((colleague for colleague in self.colleagues if colleague.id == self.self_id), None)


def _fields(raw: Any, allowed: set[str], label: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise TeamConfigError(f"{label}: expected a TOML table")
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise TeamConfigError(f"{label}: unsupported field(s): {', '.join(unknown)}")
    return raw


def _text(value: Any, label: str, limit: int = 240) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TeamConfigError(f"{label}: expected a non-empty string")
    if value != value.strip() or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise TeamConfigError(f"{label}: use one line without surrounding whitespace or control characters")
    if len(value) > limit:
        raise TeamConfigError(f"{label}: exceeds {limit} characters; shorten this field")
    return value


def _identifier(value: Any, label: str) -> str:
    value = _text(value, label, 48)
    if not _ID.fullmatch(value):
        raise TeamConfigError(f"{label}: use a lowercase ID starting with a letter, with letters, digits, '_' or '-'")
    return value


def _rows(raw: dict[str, Any], key: str) -> list[Any]:
    rows = raw.get(key, [])
    if not isinstance(rows, list):
        raise TeamConfigError(f"{key}: use [[{key}]] tables")
    if len(rows) > 20:
        raise TeamConfigError(f"{key}: at most 20 entries are supported; shorten the team context")
    return rows


def parse_team(raw: dict[str, Any], *, source: Path | None = None) -> TeamManifest:
    """Validate version-1 TOML data, references and the full rendered size."""
    raw = _fields(raw, {"version", "self", "colleagues", "projects"}, "team")
    version = raw.get("version", 1)
    if type(version) is not int or version != 1:
        raise TeamConfigError("version: expected 1")
    projects = []
    project_ids: set[str] = set()
    for index, item in enumerate(_rows(raw, "projects")):
        label = f"projects[{index}]"
        item = _fields(item, {"id", "ref"}, label)
        project_id = _identifier(item.get("id"), f"{label}.id")
        if project_id in project_ids:
            raise TeamConfigError(f"{label}.id: duplicate project ID '{project_id}'")
        project_ids.add(project_id)
        projects.append(ProjectRef(project_id, _text(item.get("ref"), f"{label}.ref", 320)))
    colleagues = []
    colleague_ids: set[str] = set()
    slack_ids: set[str] = set()
    for index, item in enumerate(_rows(raw, "colleagues")):
        label = f"colleagues[{index}]"
        item = _fields(item, {"id", "name", "role", "slack_id", "computer", "projects"}, label)
        colleague_id = _identifier(item.get("id"), f"{label}.id")
        if colleague_id in colleague_ids:
            raise TeamConfigError(f"{label}.id: duplicate colleague ID '{colleague_id}'")
        colleague_ids.add(colleague_id)
        slack_id = item.get("slack_id")
        if slack_id is not None:
            slack_id = _text(slack_id, f"{label}.slack_id", 32)
            if not _SLACK_ID.fullmatch(slack_id):
                raise TeamConfigError(f"{label}.slack_id: expected a Slack user ID such as U012ABCDEF, without mention markup")
            if slack_id in slack_ids:
                raise TeamConfigError(f"{label}.slack_id: already assigned to another colleague")
            slack_ids.add(slack_id)
        refs = item.get("projects", [])
        if not isinstance(refs, list):
            raise TeamConfigError(f"{label}.projects: expected an array of project IDs")
        refs = tuple(_identifier(ref, f"{label}.projects") for ref in refs)
        if len(refs) != len(set(refs)):
            raise TeamConfigError(f"{label}.projects: remove duplicate project IDs")
        if set(refs) - project_ids:
            raise TeamConfigError(f"{label}.projects: unknown project ID; define it in [[projects]]")
        computer = item.get("computer")
        if computer is not None:
            computer = _text(computer, f"{label}.computer", 80)
        colleagues.append(Colleague(
            colleague_id, _text(item.get("name"), f"{label}.name", 80),
            _text(item.get("role"), f"{label}.role"), slack_id, computer, refs,
        ))
    self_id = raw.get("self")
    if self_id is not None:
        self_id = _identifier(self_id, "self")
        if self_id not in colleague_ids:
            raise TeamConfigError("self: must match a configured colleague ID")
    team = TeamManifest(version, self_id, tuple(colleagues), tuple(projects), source)
    render_team(team)  # Hermes must not silently omit an oversized section.
    return team


def _read_toml(path: Path, label: str) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            data = handle.read(MAX_MANIFEST_BYTES + 1)
    except OSError as exc:
        raise TeamConfigError(f"{label} {path}: cannot read file; check the path and permissions") from exc
    if len(data) > MAX_MANIFEST_BYTES:
        raise TeamConfigError(f"{label} {path}: exceeds {MAX_MANIFEST_BYTES} bytes; shorten the file")
    try:
        return tomllib.loads(data.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise TeamConfigError(f"{label} {path}: invalid UTF-8 TOML; check the file syntax") from exc


def load_manifest(path: str | Path) -> TeamManifest:
    """Load an explicitly configured file; missing files never become neutral."""
    target = Path(path).expanduser()
    try:
        return parse_team(_read_toml(target, "Team manifest"), source=target)
    except TeamConfigError as exc:
        raise TeamConfigError(f"Team manifest {target}: {exc}") from exc


def _expand_path(value: str | Path, environment: Mapping[str, str]) -> Path:
    text = str(value)
    if "HOME" in environment and (text == "~" or text.startswith("~/")):
        return Path(environment["HOME"]) / text[2:]
    return Path(text).expanduser()


def load_team(config_path: str | Path | None = None, *, env: Mapping[str, str] | None = None) -> TeamManifest:
    """Environment override > config team_config > neutral (no implicit roster).

    A relative team_config is relative to config.toml; a relative environment
    override is relative to cwd. Both support '~'. Configured files fail
    explicitly instead of defaulting to a different team.
    """
    environment = os.environ if env is None else env
    override = environment.get("TETHER_TEAM_CONFIG")
    if override is not None:
        return load_manifest(_expand_path(_text(override, "TETHER_TEAM_CONFIG", 4096), environment))
    if config_path is None:
        config_home = environment.get("XDG_CONFIG_HOME") or str(Path(environment.get("HOME", str(Path.home()))) / ".config")
        config_path = _expand_path(config_home, environment) / "tether" / "config.toml"
    target = _expand_path(config_path, environment)
    try:
        raw = _read_toml(target, "Tether config")
    except TeamConfigError as exc:
        if isinstance(exc.__cause__, FileNotFoundError):
            return TeamManifest()
        raise
    configured = raw.get("team_config")
    if configured is None or configured == "":
        return TeamManifest()
    path = _expand_path(_text(configured, "team_config", 4096), environment)
    return load_manifest(path if path.is_absolute() else target.parent / path)


def render_team(team: TeamManifest) -> str:
    """The identical bounded section used by Hermes and native session prompts."""
    try:
        contract = _CONTRACT_PATH.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise TeamConfigError("Bundled team.md is unreadable; reinstall the Tether plugin") from exc
    if not contract:
        raise TeamConfigError("Bundled team.md is empty; reinstall the Tether plugin")
    parts = [contract]
    if team.selected:
        parts.append(f"## Your configured identity\nYou are {team.selected.name} ({team.selected.id}). Your role: {team.selected.role}.")
    if team.colleagues:
        lines = ["## Configured colleagues", "Roles guide collaboration; they do not forbid useful evidence outside a role."]
        for colleague in team.colleagues:
            fields = [f"{colleague.name} ({colleague.id}): {colleague.role}"]
            if colleague.slack_id:
                fields.append(f"Slack: <@{colleague.slack_id}>")
            if colleague.computer:
                fields.append(f"computer preference: {colleague.computer}")
            if colleague.projects:
                fields.append(f"projects: {', '.join(colleague.projects)}")
            lines.append("- " + "; ".join(fields))
        lines.append("Computer preferences and project references are context, not runtime selection, access grants or proof of tool availability.")
        parts.append("\n".join(lines))
    if team.projects:
        parts.append("## Project references\n" + "\n".join(f"- {project.id}: {project.ref}" for project in team.projects))
    text = "\n\n".join(parts)
    if len(text) > MAX_PROMPT_CHARS:
        raise TeamConfigError(f"Rendered team prompt is {len(text)} characters; Hermes supports {MAX_PROMPT_CHARS}. Shorten the roster, roles or project references.")
    return text
