"""turbopuffer search plane (H3): settings, namespace naming, schema, rows.

One namespace per tenant holds one document per passage. turbopuffer embeds
the passage text natively (``embed`` on ``embed_text``) and indexes the
verbatim text for BM25, so neither the worker nor the server ever calls an
embedding provider; the outbox (migration 066) tells the worker which
source-months to (re)project and which passages to delete.

Everything here is a projection of the S3 evidence and the Postgres
catalog: a namespace can be rebuilt from ``search-outbox-seed``.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_REGION = "aws-us-west-2"
DEFAULT_EMBED_MODEL = "voyage/voyage-4"
DEFAULT_EMBED_DIMS = 512
DEFAULT_NAMESPACE_PREFIX = "recall"
# Namespace names: letters, digits, ``-`` and ``_``; keep tenant ids out of
# the name (they carry a company slug) by hashing them.
_NAMESPACE_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
PLANES = ("postgres", "turbopuffer")
EMBED_ATTRIBUTE = "vector"
TEXT_ATTRIBUTE = "text"
EMBED_TEXT_ATTRIBUTE = "embed_text"
MAX_EMBED_TEXT_CHARS = 24_000  # ~6k tokens; voyage-4 context is larger, cost is per token


class TurbopufferConfigError(ValueError):
    pass


@dataclass(frozen=True)
class TurbopufferSettings:
    api_key: str
    region: str = DEFAULT_REGION
    namespace_prefix: str = DEFAULT_NAMESPACE_PREFIX
    embed_model: str = DEFAULT_EMBED_MODEL
    embed_dims: int = DEFAULT_EMBED_DIMS
    write_batch_rows: int = 200
    query_timeout_seconds: float = 8.0

    def namespace(self, tenant_id: str) -> str:
        digest = hashlib.sha256(tenant_id.encode()).hexdigest()[:20]
        name = f"{self.namespace_prefix}-{digest}"
        if not _NAMESPACE_RE.fullmatch(name):
            raise TurbopufferConfigError("turbopuffer namespace prefix is invalid")
        return name


def search_plane_from_env() -> str:
    value = os.environ.get("RECALL_SEARCH_PLANE", "postgres").strip().lower() or "postgres"
    if value not in PLANES:
        raise TurbopufferConfigError("RECALL_SEARCH_PLANE must be postgres or turbopuffer")
    return value


def _read_key_file(path: str) -> str:
    file = Path(path)
    if file.is_symlink() or not file.is_file():
        raise TurbopufferConfigError("turbopuffer key file must be a regular file")
    if file.stat().st_mode & 0o077:
        raise TurbopufferConfigError("turbopuffer key file must be owner-only (0600)")
    value = file.read_text().strip()
    if not value:
        raise TurbopufferConfigError("turbopuffer key file is empty")
    return value


def turbopuffer_settings_from_env(*, required: bool = False) -> TurbopufferSettings | None:
    """Settings from ``RECALL_TPUF_*``; ``None`` when no key is configured.

    ``RECALL_TPUF_API_KEY`` or ``RECALL_TPUF_KEY_FILE`` (0600) provides the
    key; the value is never logged. ``required`` raises instead of ``None``.
    """

    key = os.environ.get("RECALL_TPUF_API_KEY", "").strip()
    key_file = os.environ.get("RECALL_TPUF_KEY_FILE", "").strip()
    if key and key_file:
        raise TurbopufferConfigError("set RECALL_TPUF_API_KEY or RECALL_TPUF_KEY_FILE, not both")
    if key_file:
        key = _read_key_file(key_file)
    if not key:
        if required:
            raise TurbopufferConfigError("turbopuffer API key is not configured")
        return None
    try:
        dims = int(os.environ.get("RECALL_TPUF_EMBED_DIMS", str(DEFAULT_EMBED_DIMS)))
        batch = int(os.environ.get("RECALL_TPUF_WRITE_BATCH_ROWS", "200"))
        timeout = float(os.environ.get("RECALL_TPUF_QUERY_TIMEOUT_SECONDS", "8"))
    except ValueError as error:
        raise TurbopufferConfigError("turbopuffer numeric settings are invalid") from error
    if not 64 <= dims <= 4096 or not 1 <= batch <= 5000 or not 0.5 <= timeout <= 60:
        raise TurbopufferConfigError("turbopuffer numeric settings are out of range")
    settings = TurbopufferSettings(
        api_key=key,
        region=os.environ.get("RECALL_TPUF_REGION", DEFAULT_REGION).strip() or DEFAULT_REGION,
        namespace_prefix=os.environ.get("RECALL_TPUF_NAMESPACE_PREFIX", DEFAULT_NAMESPACE_PREFIX).strip() or DEFAULT_NAMESPACE_PREFIX,
        embed_model=os.environ.get("RECALL_TPUF_EMBED_MODEL", DEFAULT_EMBED_MODEL).strip() or DEFAULT_EMBED_MODEL,
        embed_dims=dims,
        write_batch_rows=batch,
        query_timeout_seconds=timeout,
    )
    settings.namespace("tenant:probe")  # validates the prefix
    return settings


def build_client(settings: TurbopufferSettings) -> Any:
    """The turbopuffer SDK client (imported lazily so the server runs without it)."""

    try:
        import turbopuffer  # type: ignore[import-not-found]
    except ImportError as error:  # pragma: no cover - dependency present in prod
        raise TurbopufferConfigError("turbopuffer client is not installed") from error
    return turbopuffer.Turbopuffer(api_key=settings.api_key, region=settings.region)


def namespace_schema(settings: TurbopufferSettings) -> dict[str, Any]:
    """Attribute schema for a tenant namespace.

    ``text`` carries BM25 (no stemming or stopword removal: identifiers such
    as ``expert_skills`` and ``#6076`` must survive as tokens; the lexical
    arm's min-should-match becomes BM25's natural OR scoring). ``embed_text``
    (context header + text) is embedded natively into ``vector``. Filterable
    attributes are the ones the arms scope by; everything the response needs
    but never filters on is stored unindexed (half price).
    """

    return {
        "source_id": {"type": "string", "filterable": True},
        "logical_document_id": {"type": "string", "filterable": True},
        "policy_fingerprint": {"type": "string", "filterable": True},
        "month": {"type": "string", "filterable": True},
        "first_occurred_at": {"type": "datetime", "filterable": True},
        "last_occurred_at": {"type": "datetime", "filterable": True},
        "actor_ids": {"type": "[]string", "filterable": True},
        "actor_keys": {"type": "[]string", "filterable": True},
        "native_parent_id": {"type": "string", "filterable": False},
        "revision": {"type": "uint", "filterable": False},
        "ordinal": {"type": "uint", "filterable": False},
        "doc_first_occurred_at": {"type": "datetime", "filterable": False},
        "doc_last_occurred_at": {"type": "datetime", "filterable": False},
        "manifest_object_key": {"type": "string", "filterable": False},
        "manifest_content_sha256": {"type": "string", "filterable": False},
        "text_sha256": {"type": "string", "filterable": False},
        "roles": {"type": "[]string", "filterable": False},
        "receipts": {"type": "[]string", "filterable": False},
        "spans": {"type": "string", "filterable": False},
        "header": {"type": "string", "filterable": False},
        TEXT_ATTRIBUTE: {
            "type": "string",
            "filterable": False,
            "full_text_search": {
                "tokenizer": "word_v4",
                "language": "english",
                "stemming": False,
                "remove_stopwords": False,
                "case_sensitive": False,
                "ascii_folding": True,
                "max_token_length": 64,
            },
        },
        EMBED_TEXT_ATTRIBUTE: {
            "type": "string",
            "filterable": False,
            "embed": {
                "model": settings.embed_model,
                "attribute": EMBED_ATTRIBUTE,
                "dims": settings.embed_dims,
                "dtype": "float16",
            },
        },
    }


def _iso(value: Any) -> str:
    if hasattr(value, "isoformat"):
        text = value.isoformat()
    else:
        text = str(value).replace(" ", "T", 1)
    if text.endswith("+00"):
        text += ":00"
    return text


def month_key(value: Any) -> str:
    return _iso(value)[:7]


def passage_row(passage: dict[str, Any]) -> dict[str, Any]:
    """One turbopuffer document from a catalog passage row (plus its document).

    ``passage`` carries the ``canonical_passages`` columns, the document's
    ``native_parent_id``/``revision``/manifest fields/times, ``header``
    (``header_redacted``, may be null) and ``actors`` as ``[(relation,
    actor_id), ...]``. Content stays verbatim; nothing is summarised.
    """

    text = passage["text_redacted"]
    header = passage.get("header_redacted") or ""
    embed_text = f"{header}\n\n{text}" if header else text
    actors = list(passage.get("actors") or ())
    return {
        "id": passage["passage_id"],
        "source_id": passage["source_id"],
        "logical_document_id": passage["logical_document_id"],
        "policy_fingerprint": passage["policy_fingerprint"],
        "month": month_key(passage["first_occurred_at"]),
        "first_occurred_at": _iso(passage["first_occurred_at"]),
        "last_occurred_at": _iso(passage["last_occurred_at"]),
        "actor_ids": sorted({actor_id for _relation, actor_id in actors}),
        "actor_keys": sorted({f"{relation}:{actor_id}" for relation, actor_id in actors}),
        "native_parent_id": passage["native_parent_id"],
        "revision": int(passage["revision"]),
        "ordinal": int(passage["ordinal"]),
        "doc_first_occurred_at": _iso(passage["doc_first_occurred_at"]),
        "doc_last_occurred_at": _iso(passage["doc_last_occurred_at"]),
        "manifest_object_key": passage["manifest_object_key"],
        "manifest_content_sha256": passage["manifest_content_sha256"],
        "text_sha256": passage["text_sha256"],
        "roles": list(passage.get("roles") or ()),
        "receipts": list(passage["receipts"]),
        "spans": json.dumps(passage["spans"], separators=(",", ":"), sort_keys=True),
        "header": header,
        TEXT_ATTRIBUTE: text,
        EMBED_TEXT_ATTRIBUTE: embed_text[:MAX_EMBED_TEXT_CHARS],
    }
