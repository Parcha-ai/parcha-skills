"""Optional cross-encoder reranker runtime (H2-c).

Mirrors the hygiene of :mod:`recall_server.semantic`: HTTPS plus an exact
approved endpoint for every non-loopback URL, an owner-only key file or a
named secret variable as the single bearer source, redirect refusal, bounded
response bodies, and a per-request timeout. The runtime never logs query or
document text and never includes the bearer in an exception message.

Any failure (network, HTTP status, malformed body, deadline exhausted) raises
:class:`RerankUnavailable` so the caller keeps its fused ranking. The runtime
does not retry: it sits on the query path under a hard deadline, and a
``429``/``503`` with ``Retry-After`` would only push the request past it.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import ssl
import stat
import time
import urllib.error
import urllib.request
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit


RERANK_CONTRACT = "recall.rerank.v1:query-passages"
MAX_RERANK_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_RERANK_KEY_BYTES = 4096

PROVIDERS: dict[str, dict[str, str]] = {
    "voyage": {
        "url": "https://api.voyageai.com/v1/rerank",
        "model": "rerank-2.5",
    },
    "cohere": {
        "url": "https://api.cohere.com/v2/rerank",
        "model": "rerank-v3.5",
    },
}

DEFAULT_TIMEOUT_SECONDS = 2.5
# The rerank call sits on the query path after the arms. Search only runs it
# when at least this much of the deadline remains, so a slow arm never turns
# into a reranker timeout stacked on top of it.
DEFAULT_RERANK_MIN_BUDGET_SECONDS = 1.0
DEFAULT_MAX_CANDIDATES = 50
DEFAULT_MAX_DOC_CHARS = 2000
MAX_CANDIDATES_CEILING = 1000
MAX_DOC_CHARS_CEILING = 20_000
MAX_QUERY_CHARS = 4000

_KEY_ENV_RE = re.compile(r"[A-Z_][A-Z0-9_]{0,127}")
_MODEL_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}")


class RerankUnavailable(RuntimeError):
    """The reranker could not produce a ranking; callers keep fused results."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class RerankTransport(Protocol):
    """Injection point for tests. ``post`` returns the parsed JSON body."""

    def post(
        self,
        *,
        url: str,
        headers: dict[str, str],
        body: dict[str, Any],
        timeout: float,
    ) -> Any: ...


class _RejectRedirect(urllib.request.HTTPRedirectHandler):
    """Keep private queries and bearer credentials on the validated endpoint."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class UrllibRerankTransport:
    """Default transport: no redirects, bounded body, verified TLS."""

    def post(
        self,
        *,
        url: str,
        headers: dict[str, str],
        body: dict[str, Any],
        timeout: float,
    ) -> Any:
        request = urllib.request.Request(
            url,
            data=json.dumps(body, separators=(",", ":")).encode(),
            method="POST",
            headers={
                **headers,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "recall-core/rerank-v1",
            },
        )
        opener = urllib.request.build_opener(
            _RejectRedirect(),
            urllib.request.HTTPSHandler(context=ssl.create_default_context()),
        )
        try:
            with opener.open(request, timeout=timeout) as response:
                if response.status != 200:
                    raise RerankUnavailable("rerank_http_status")
                length = response.headers.get("Content-Length")
                if length is not None and (
                    not length.isdigit() or int(length) > MAX_RERANK_RESPONSE_BYTES
                ):
                    raise RerankUnavailable("rerank_response_too_large")
                raw = response.read(MAX_RERANK_RESPONSE_BYTES + 1)
        except RerankUnavailable:
            raise
        except urllib.error.HTTPError as error:
            # A refused redirect surfaces as the 3xx HTTPError itself.
            code = "rerank_redirect_refused" if 300 <= error.code < 400 else "rerank_http_status"
            try:
                error.close()
            finally:
                raise RerankUnavailable(code) from None
        except (RemoteDisconnected, TimeoutError, urllib.error.URLError, OSError):
            raise RerankUnavailable("rerank_transport_error") from None
        if len(raw) > MAX_RERANK_RESPONSE_BYTES:
            raise RerankUnavailable("rerank_response_too_large")
        try:
            return json.loads(raw)
        except ValueError:
            raise RerankUnavailable("rerank_response_invalid") from None


def _validate_endpoint(url: str, approved_url: str | None) -> str:
    parsed = urlsplit(url)
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("rerank endpoint must be a plain URL")
    loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    if parsed.scheme != "https" and not (loopback and parsed.scheme == "http"):
        raise ValueError("remote rerank endpoint must use HTTPS")
    if not loopback:
        if not approved_url:
            raise ValueError("remote rerank endpoint requires an approved rerank endpoint")
        if url.rstrip("/") != approved_url.rstrip("/"):
            raise ValueError("rerank endpoint does not match the approved rerank endpoint")
    return url.rstrip("/")


def _read_owner_only_key(path: Path) -> str:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            raise PermissionError("rerank key file must be owner-only")
        value = os.read(descriptor, 8192).decode().strip()
    finally:
        os.close(descriptor)
    if not value or len(value) > MAX_RERANK_KEY_BYTES:
        raise ValueError("rerank key file is invalid")
    return value


class RerankRuntime:
    """Provider-neutral ``rerank(query, documents)`` over Voyage or Cohere."""

    def __init__(
        self,
        *,
        protocol: str,
        url: str | None = None,
        approved_url: str | None = None,
        model: str | None = None,
        key_file: str | None = None,
        key_env: str | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_candidates: int = DEFAULT_MAX_CANDIDATES,
        max_doc_chars: int = DEFAULT_MAX_DOC_CHARS,
        transport: RerankTransport | None = None,
    ):
        if protocol not in PROVIDERS:
            raise ValueError("rerank protocol must be voyage or cohere")
        defaults = PROVIDERS[protocol]
        resolved_url = (url or "").strip() or defaults["url"]
        # The provider's canonical endpoint is approved by construction; any
        # other host needs an explicit, exactly matching approval.
        approved = (approved_url or "").strip() or None
        if approved is None and resolved_url.rstrip("/") == defaults["url"]:
            approved = defaults["url"]
        self.url = _validate_endpoint(resolved_url, approved)
        resolved_model = (model or "").strip() or defaults["model"]
        if not _MODEL_RE.fullmatch(resolved_model):
            raise ValueError("rerank model must be a stable version label")
        if key_file and key_env:
            raise ValueError("rerank key file and environment source are mutually exclusive")
        if key_env and not _KEY_ENV_RE.fullmatch(key_env):
            raise ValueError("rerank key environment variable name is invalid")
        if not (key_file or key_env):
            raise ValueError("rerank endpoint requires a key source")
        try:
            timeout_seconds = float(timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError("rerank timeout must be between 0.1 and 30 seconds") from exc
        if not math.isfinite(timeout_seconds) or not 0.1 <= timeout_seconds <= 30.0:
            raise ValueError("rerank timeout must be between 0.1 and 30 seconds")
        if type(max_candidates) is not int or not 1 <= max_candidates <= MAX_CANDIDATES_CEILING:
            raise ValueError(
                f"rerank max candidates must be between 1 and {MAX_CANDIDATES_CEILING}"
            )
        if type(max_doc_chars) is not int or not 64 <= max_doc_chars <= MAX_DOC_CHARS_CEILING:
            raise ValueError(
                f"rerank max document chars must be between 64 and {MAX_DOC_CHARS_CEILING}"
            )
        self.protocol = protocol
        self.model = resolved_model
        self.key_file = Path(key_file) if key_file else None
        self.key_env = key_env or None
        self.timeout_seconds = timeout_seconds
        self.max_candidates = max_candidates
        self.max_doc_chars = max_doc_chars
        self.transport: RerankTransport = transport or UrllibRerankTransport()

    @classmethod
    def from_env(cls, transport: RerankTransport | None = None) -> RerankRuntime | None:
        protocol = os.environ.get("RECALL_RERANK_PROTOCOL", "off").strip().lower() or "off"
        if protocol == "off":
            return None
        if protocol not in PROVIDERS:
            raise ValueError("RECALL_RERANK_PROTOCOL must be voyage, cohere, or off")

        def _int(name: str, default: int) -> int:
            raw = os.environ.get(name, "").strip()
            if not raw:
                return default
            try:
                return int(raw)
            except ValueError as exc:
                raise ValueError(f"{name} must be an integer") from exc

        return cls(
            protocol=protocol,
            url=os.environ.get("RECALL_RERANK_URL") or None,
            approved_url=os.environ.get("RECALL_RERANK_APPROVED_URL") or None,
            model=os.environ.get("RECALL_RERANK_MODEL") or None,
            key_file=os.environ.get("RECALL_RERANK_KEY_FILE") or None,
            key_env=os.environ.get("RECALL_RERANK_KEY_ENV") or None,
            timeout_seconds=float(
                os.environ.get("RECALL_RERANK_TIMEOUT_SECONDS", "").strip()
                or DEFAULT_TIMEOUT_SECONDS
            ),
            max_candidates=_int("RECALL_RERANK_MAX_CANDIDATES", DEFAULT_MAX_CANDIDATES),
            max_doc_chars=_int("RECALL_RERANK_MAX_DOC_CHARS", DEFAULT_MAX_DOC_CHARS),
            transport=transport,
        )

    @property
    def fingerprint(self) -> str:
        value = "\0".join(
            (
                RERANK_CONTRACT,
                self.protocol,
                self.model,
                str(self.max_doc_chars),
            )
        )
        return hashlib.sha256(value.encode()).hexdigest()

    # -- credentials ---------------------------------------------------------

    def _read_key(self) -> str:
        if self.key_file is not None:
            return _read_owner_only_key(self.key_file)
        value = os.environ.get(self.key_env or "", "").strip()
        if not value or len(value) > MAX_RERANK_KEY_BYTES:
            raise ValueError("rerank key environment variable is unavailable")
        return value

    # -- request/response ----------------------------------------------------

    def _truncate(self, text: str) -> str:
        if len(text) <= self.max_doc_chars:
            return text
        return text[: self.max_doc_chars]

    def _build_body(self, query: str, documents: list[str], top_k: int) -> dict[str, Any]:
        if self.protocol == "voyage":
            return {
                "query": query,
                "documents": documents,
                "model": self.model,
                "top_k": top_k,
                "truncation": True,
            }
        return {
            "query": query,
            "documents": documents,
            "model": self.model,
            "top_n": top_k,
        }

    def _parse_results(self, payload: Any, expected: int, top_k: int) -> list[tuple[int, float]]:
        if not isinstance(payload, dict):
            raise RerankUnavailable("rerank_response_invalid")
        # Voyage: {"data": [{"index", "relevance_score"}, ...]}
        # Cohere: {"results": [{"index", "relevance_score"}, ...]}
        rows = payload.get("data") if self.protocol == "voyage" else payload.get("results")
        if not isinstance(rows, list) or len(rows) > expected:
            raise RerankUnavailable("rerank_response_invalid")
        seen: set[int] = set()
        results: list[tuple[int, float]] = []
        for row in rows:
            if not isinstance(row, dict):
                raise RerankUnavailable("rerank_response_invalid")
            index = row.get("index")
            score = row.get("relevance_score")
            if type(index) is not int or isinstance(index, bool) or not 0 <= index < expected:
                raise RerankUnavailable("rerank_index_out_of_range")
            if index in seen:
                raise RerankUnavailable("rerank_response_invalid")
            if isinstance(score, bool) or not isinstance(score, (int, float)):
                raise RerankUnavailable("rerank_response_invalid")
            score = float(score)
            if not math.isfinite(score):
                raise RerankUnavailable("rerank_response_invalid")
            seen.add(index)
            results.append((index, score))
        results.sort(key=lambda item: (-item[1], item[0]))
        return results[:top_k]

    def rerank(
        self,
        query: str,
        documents: list[str],
        *,
        top_k: int | None = None,
        deadline_seconds: float | None = None,
    ) -> list[tuple[int, float]]:
        """Score ``documents`` against ``query``; return ``(index, score)`` desc.

        ``documents`` beyond ``max_candidates`` are dropped (never reranked);
        each retained document is truncated to ``max_doc_chars``. ``top_k``
        bounds the returned rows. ``deadline_seconds`` caps the request
        timeout below the configured one; a non-positive deadline is refused
        before any network call.
        """
        if not isinstance(query, str) or not query.strip():
            raise RerankUnavailable("rerank_query_invalid")
        if not isinstance(documents, list) or not all(isinstance(doc, str) for doc in documents):
            raise RerankUnavailable("rerank_documents_invalid")
        if not documents:
            return []
        candidates = [self._truncate(doc) for doc in documents[: self.max_candidates]]
        expected = len(candidates)
        if top_k is None:
            top_k = expected
        if type(top_k) is not int or top_k < 1:
            raise RerankUnavailable("rerank_top_k_invalid")
        top_k = min(top_k, expected)
        timeout = self.timeout_seconds
        if deadline_seconds is not None:
            if not isinstance(deadline_seconds, (int, float)) or deadline_seconds <= 0:
                raise RerankUnavailable("rerank_deadline_exhausted")
            timeout = min(timeout, float(deadline_seconds))
        try:
            key = self._read_key()
        except (OSError, ValueError):
            raise RerankUnavailable("rerank_key_unavailable") from None
        body = self._build_body(query[:MAX_QUERY_CHARS], candidates, top_k)
        headers = {"Authorization": f"Bearer {key}"}
        started = time.monotonic()
        try:
            payload = self.transport.post(
                url=self.url, headers=headers, body=body, timeout=timeout
            )
        except RerankUnavailable:
            raise
        except (RemoteDisconnected, TimeoutError, urllib.error.URLError, OSError):
            raise RerankUnavailable("rerank_transport_error") from None
        except Exception:  # noqa: BLE001 - never leak provider details upward
            raise RerankUnavailable("rerank_transport_error") from None
        finally:
            del key, headers
        if time.monotonic() - started > timeout:
            # A cooperative transport that overran the budget is still a miss.
            raise RerankUnavailable("rerank_deadline_exhausted")
        return self._parse_results(payload, expected, top_k)


DEFAULT_RERANK_BLEND = 0.6


def rerank_blend_from_env() -> float:
    """``RECALL_RERANK_BLEND`` (0-1): weight of the reranker score in the final
    order; the rest is the fused (arm) score. 1.0 = pure rerank; default 0.6.
    Measured 2026-09-14: pure rerank lifted recall@20 0.583 -> 0.75 but cut
    MRR 0.444 -> 0.366 because it discarded the fused signal on two cases.
    """

    raw = os.environ.get("RECALL_RERANK_BLEND", "").strip()
    if not raw:
        return DEFAULT_RERANK_BLEND
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError("RECALL_RERANK_BLEND must be between 0 and 1") from exc
    if not math.isfinite(value) or not 0.0 <= value <= 1.0:
        raise ValueError("RECALL_RERANK_BLEND must be between 0 and 1")
    return value


def rerank_min_budget_seconds_from_env() -> float:
    """``RECALL_RERANK_MIN_BUDGET_SECONDS`` (0.05–30); default 1.0."""

    raw = os.environ.get("RECALL_RERANK_MIN_BUDGET_SECONDS", "").strip()
    if not raw:
        return DEFAULT_RERANK_MIN_BUDGET_SECONDS
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(
            "RECALL_RERANK_MIN_BUDGET_SECONDS must be between 0.05 and 30"
        ) from exc
    if not math.isfinite(value) or not 0.05 <= value <= 30.0:
        raise ValueError("RECALL_RERANK_MIN_BUDGET_SECONDS must be between 0.05 and 30")
    return value


def build_rerank_runtime(transport: RerankTransport | None = None) -> RerankRuntime | None:
    """Construct the reranker from ``RECALL_RERANK_*``; ``None`` when off."""

    return RerankRuntime.from_env(transport=transport)
