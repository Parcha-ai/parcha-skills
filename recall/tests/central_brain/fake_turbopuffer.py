"""In-process stand-in for the turbopuffer client (write/query/multi_query).

Shared by the projection and retrieval tests and the search-plane e2e.
Rows live in a dict per namespace. ``query`` honours the filter subset the
Recall arms use (And/Or/Not, Eq/NotEq, In/NotIn, Gt/Gte/Lt/Lte on ISO
datetimes, Contains/NotContains/ContainsAny/ContainsAll, ContainsAllTokens,
Glob) and returns ``FakeRow`` objects (dict-style and attribute access) ranked
by term overlap: BM25 → count of query tokens
present in the attribute (reported under ``$dist`` like the service); ANN with ``["Embed", text]`` → the
same overlap turned into a distance (``$dist`` = 1 − overlap ratio) so a
passage sharing more words with the query is "nearer". Good enough to prove
plumbing, never a relevance model.
"""
from __future__ import annotations

import json
import os
import re
import threading
from typing import Any


class NotFoundError(Exception):
    """Named like ``turbopuffer.NotFoundError``: a delete-only write to a
    namespace that never received an upsert (the writer matches by name)."""


class FakeRow(dict):
    """A query row: ``row.id``, ``row["$dist"]``, ``row.text``, ``to_dict()``."""

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as error:
            raise AttributeError(key) from error

    def to_dict(self) -> dict[str, Any]:
        return dict(self)

    model_dump = to_dict


# word_v4-ish: a token starts with a letter or digit; sigils (#, @, $) and
# punctuation split, underscores/dots/dashes inside a token survive.
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_.\-/]*")


def tokens(text: Any) -> list[str]:
    return _TOKEN_RE.findall(str(text or "").casefold())


class FakeQueryResponse:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.billing = {"fake": True}
        self.performance = {"cache_hit_ratio": 1.0}


class FakeMultiQueryResponse:
    def __init__(self, results: list[FakeQueryResponse]) -> None:
        self.results = results


class FakeNamespaceMetadata:
    def __init__(self, *, approx_row_count: int, schema: dict[str, Any]) -> None:
        self.approx_row_count = approx_row_count
        self.approx_logical_bytes = 0
        self.schema = schema

    def to_dict(self) -> dict[str, Any]:
        return {
            "approx_row_count": self.approx_row_count,
            "approx_logical_bytes": self.approx_logical_bytes,
            "schema": self.schema,
        }


_FAKE_LOCK = threading.RLock()


class FakeNamespace:
    def __init__(self, name: str) -> None:
        self.name = name
        self.rows: dict[str, dict[str, Any]] = {}
        self.writes: list[dict[str, Any]] = []
        self.queries: list[dict[str, Any]] = []
        self.schema: dict[str, Any] | None = None
        self.deleted: list[str] = []
        self.fail_writes: Exception | None = None
        self.fail_queries: Exception | None = None
        self._touched = False

    @property
    def fail_with(self) -> Exception | None:
        return self.fail_writes

    @fail_with.setter
    def fail_with(self, error: Exception | None) -> None:
        self.fail_writes = error

    # -- write ---------------------------------------------------------------
    def write(self, **kwargs: Any) -> dict[str, Any]:
        # Concurrent batch writers (H3 drain) hit one namespace from threads.
        with _FAKE_LOCK:
            return self._write_locked(**kwargs)

    def _write_locked(self, **kwargs: Any) -> dict[str, Any]:
        if self.fail_writes is not None:
            raise self.fail_writes
        upserts = list(kwargs.get("upsert_rows") or ())
        if not upserts and kwargs.get("deletes") and not self._touched:
            raise NotFoundError(f"namespace {self.name} was not found")
        self.writes.append(kwargs)
        if kwargs.get("schema"):
            self.schema = dict(kwargs["schema"])
        upserted = 0
        for row in upserts:
            self.rows[str(row["id"])] = dict(row)
            upserted += 1
        self._touched = self._touched or bool(upserts)
        deleted = 0
        for identifier in kwargs.get("deletes") or ():
            self.deleted.append(str(identifier))
            deleted += int(self.rows.pop(str(identifier), None) is not None)
        filter_ = kwargs.get("delete_by_filter")
        if filter_ is not None:
            for identifier in [key for key, row in self.rows.items() if _matches(row, filter_)]:
                del self.rows[identifier]
                deleted += 1
        return {"rows_affected": upserted + deleted, "rows_upserted": upserted, "rows_deleted": deleted}

    def delete_all(self) -> None:
        self.rows.clear()

    def metadata(self) -> "FakeNamespaceMetadata":
        """Like ``turbopuffer.NamespaceMetadata``: ``approx_row_count`` and the schema."""

        if not self._touched:
            raise NotFoundError(f"namespace {self.name} was not found")
        return FakeNamespaceMetadata(approx_row_count=len(self.rows), schema=dict(self.schema or {}))

    # -- query ---------------------------------------------------------------
    def query(self, **kwargs: Any) -> FakeQueryResponse:
        if self.fail_queries is not None:
            raise self.fail_queries
        self.queries.append(kwargs)
        return FakeQueryResponse(self._run(kwargs))

    def multi_query(self, *, queries: list[dict[str, Any]], **_: Any) -> FakeMultiQueryResponse:
        if self.fail_queries is not None:
            raise self.fail_queries
        return FakeMultiQueryResponse([FakeQueryResponse(self._run(q)) for q in queries])

    def _run(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        filters = query.get("filters")
        limit = int(query.get("limit") or 10)
        include = query.get("include_attributes")
        rank_by = query.get("rank_by")
        candidates = [row for row in self.rows.values() if filters is None or _matches(row, filters)]
        scored: list[tuple[float, dict[str, Any]]] = []
        attribute, mode, argument = _rank(rank_by)
        for row in candidates:
            if mode == "ANN":
                text = argument[1] if isinstance(argument, (list, tuple)) and argument and argument[0] == "Embed" else ""
                overlap = _overlap(row.get(attribute), text)
                scored.append((1.0 - overlap, row))
            elif mode == "BM25":
                scored.append((-_hits(row.get(attribute), argument), row))
            elif mode in ("desc", "asc"):
                value = row.get(attribute)
                scored.append((-(_num(value)) if mode == "desc" else _num(value), row))
            else:
                scored.append((0.0, row))
        scored.sort(key=lambda item: (item[0], str(item[1].get("id"))))
        out = []
        for key, row in scored[:limit]:
            projected = {"id": row["id"]}
            for name in (include if isinstance(include, list) else [k for k in row if k != "id"]):
                if name in row:
                    projected[name] = row[name]
            if mode == "ANN":
                projected["$dist"] = key
            elif mode == "BM25":
                projected["$dist"] = -key  # the service reports BM25 under $dist too
            out.append(FakeRow(projected))
        return out


def _rank(rank_by: Any) -> tuple[str, str, Any]:
    if not rank_by:
        return "id", "asc", None
    if len(rank_by) == 2:
        return str(rank_by[0]), str(rank_by[1]), None
    return str(rank_by[0]), str(rank_by[1]), rank_by[2]


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _overlap(text: Any, query: Any) -> float:
    q = set(tokens(query))
    if not q:
        return 0.0
    t = set(tokens(text))
    return len(q & t) / len(q)


def _hits(text: Any, query: Any) -> int:
    t = set(tokens(text))
    return sum(1 for token in tokens(query) if token in t)


def _matches(row: dict[str, Any], filter_: Any) -> bool:
    if not isinstance(filter_, (list, tuple)) or not filter_:
        return True
    head = filter_[0]
    if head == "And":
        return all(_matches(row, item) for item in filter_[1])
    if head == "Or":
        return any(_matches(row, item) for item in filter_[1])
    if head == "Not":
        return not _matches(row, filter_[1])
    attribute, op, value = filter_
    actual = row.get(attribute)
    if op == "Eq":
        return actual == value
    if op == "NotEq":
        return actual != value
    if op == "In":
        return actual in set(value)
    if op == "NotIn":
        return actual not in set(value)
    if op == "Gte":
        return actual is not None and str(actual) >= str(value)
    if op == "Lte":
        return actual is not None and str(actual) <= str(value)
    if op == "Gt":
        return actual is not None and str(actual) > str(value)
    if op == "Lt":
        return actual is not None and str(actual) < str(value)
    if op == "Contains":
        return value in (actual or ())
    if op == "ContainsAny":
        return bool(set(value) & set(actual or ()))
    if op == "ContainsAll":
        return set(value) <= set(actual or ())
    if op == "NotContains":
        return value not in (actual or ())
    if op == "Glob":
        return re.fullmatch(re.escape(str(value)).replace("\\*", ".*").replace("\\?", "."), str(actual or "")) is not None
    if op == "ContainsAllTokens":
        present = set(tokens(actual))
        return all(token in present for token in tokens(value))
    if op == "ContainsTokenSequence":
        return " ".join(tokens(value)) in " ".join(tokens(actual))
    raise ValueError(f"fake turbopuffer does not support filter {op}")


class FakeTurbopuffer:
    """``FakeTurbopuffer().namespace(name)`` mirrors ``turbopuffer.Turbopuffer``.

    With ``state_path`` (or ``RECALL_TPUF_FAKE_STATE``) every namespace's rows
    are loaded from and saved to one JSON file after each write, so a worker
    process and a server process in an e2e share the same fake plane.
    """

    def __init__(self, settings: Any = None, *, state_path: str | None = None) -> None:
        self.settings = settings
        self.namespaces: dict[str, FakeNamespace] = {}
        self.state_path = state_path or os.environ.get("RECALL_TPUF_FAKE_STATE") or None

    def namespace(self, name: str) -> FakeNamespace:
        if name not in self.namespaces:
            self.namespaces[name] = _PersistentNamespace(name, self) if self.state_path else FakeNamespace(name)
        return self.namespaces[name]

    # -- file-backed state -----------------------------------------------------
    def _load(self, name: str) -> dict[str, dict[str, Any]]:
        try:
            with open(self.state_path, encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, ValueError):
            return {}
        return dict(state.get(name) or {})

    def _names(self) -> set[str]:
        try:
            with open(self.state_path, encoding="utf-8") as handle:
                return set(json.load(handle))
        except (OSError, ValueError):
            return set()

    def _save(self, name: str, rows: dict[str, dict[str, Any]]) -> None:
        try:
            with open(self.state_path, encoding="utf-8") as handle:
                state = json.load(handle)
        except (OSError, ValueError):
            state = {}
        state[name] = rows
        tmp = f"{self.state_path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(state, handle)
        os.replace(tmp, self.state_path)


class _PersistentNamespace(FakeNamespace):
    def __init__(self, name: str, owner: FakeTurbopuffer) -> None:
        super().__init__(name)
        self._owner = owner

    def write(self, **kwargs: Any) -> dict[str, Any]:
        with _FAKE_LOCK:
            self.rows = self._owner._load(self.name)
            self._touched = self._touched or bool(self.rows)
            result = self._write_locked(**kwargs)
            self._owner._save(self.name, self.rows)
            return result

    def _run(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        with _FAKE_LOCK:
            self.rows = self._owner._load(self.name)
            return super()._run(query)

    def metadata(self) -> FakeNamespaceMetadata:
        if self.name not in self._owner._names():
            raise NotFoundError(f"namespace {self.name} was not found")
        self.rows = self._owner._load(self.name)
        return FakeNamespaceMetadata(approx_row_count=len(self.rows), schema=dict(self.schema or {}))


def factory(settings: Any = None) -> FakeTurbopuffer:
    """``RECALL_TPUF_CLIENT_FACTORY=tests.central_brain.fake_turbopuffer:factory``."""

    return FakeTurbopuffer(settings)
