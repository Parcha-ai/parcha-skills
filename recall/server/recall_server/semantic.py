from __future__ import annotations

import json
import hashlib
import math
import os
import re
import stat
import threading
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


QUERY_INSTRUCTION = (
    "Instruct: Retrieve passages from a private personal work memory that answer the query.\n"
    "Query: "
)
DOCUMENT_EMBEDDING_CONTRACT = "recall.document-embedding.v5:head-tail-4096"
DEFAULT_EMBEDDING_REVISION = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3"
MAX_SEMANTIC_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_DOCUMENT_CHARS = 4096
DOCUMENT_CLIP_MARKER = "\n[...clipped for embedding...]\n"
PLANNER_PROMPT = """You are a high-precision lexical query planner for a private personal work memory.
Infer canonical engineering or business concepts described indirectly. Return up to eight independent
likely verbatim search phrases, each one to three words. Include the standard canonical term when one
exists. Omit generic suffixes such as pattern, system, method, strategy, implementation, or technique.
Preserve exact identifiers. Do not answer the query and do not invent names. If the premise is nonsense
or has no meaningful work concept, set searchable=false and return no phrases.
Output only JSON: {"searchable":true,"phrases":["..."]}."""


class _RejectRedirect(urllib.request.HTTPRedirectHandler):
    """Keep private queries and bearer credentials on their validated endpoints."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


@dataclass(frozen=True)
class SearchPlan:
    searchable: bool
    phrases: tuple[str, ...]


class SemanticRuntime:
    """Provider-neutral embeddings plus optional LiteLLM query planning."""

    def __init__(
        self,
        *,
        embedding_protocol: str = "tei",
        embedding_url: str,
        embedding_approved_url: str | None = None,
        embedding_key_file: str | None = None,
        embedding_key_env: str | None = None,
        model: str,
        revision: str,
        dimensions: int,
        document_prefix: str = "",
        query_prefix: str | None = None,
        planner_url: str | None = None,
        planner_approved_url: str | None = None,
        planner_model: str | None = None,
        planner_key_file: str | None = None,
        timeout_seconds: float = 15.0,
        embedding_batch_size: int = 1,
        embedding_workers: int = 1,
        planner_samples: int = 2,
        cache_size: int = 256,
        cache_ttl_seconds: float = 900.0,
    ):
        if embedding_protocol not in {"tei", "openai", "voyage"}:
            raise ValueError("embedding protocol must be tei, openai, or voyage")
        parsed = urlsplit(embedding_url)
        if not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("embedding endpoint must be a plain base URL")
        loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        allowed_schemes = {"http"} if embedding_protocol == "tei" else {"https"}
        if loopback:
            allowed_schemes.add("http")
        if parsed.scheme not in allowed_schemes:
            if embedding_protocol == "tei":
                raise ValueError("TEI embedding endpoint must use plain HTTP")
            raise ValueError("remote embedding endpoint must use HTTPS")
        if not loopback:
            if not embedding_approved_url:
                if embedding_protocol == "tei":
                    raise ValueError(
                        "remote embedding endpoint requires an approved private endpoint"
                    )
                raise ValueError(
                    "remote embedding endpoint requires an approved embedding endpoint"
                )
            if embedding_url.rstrip("/") != embedding_approved_url.rstrip("/"):
                if embedding_protocol == "tei":
                    raise ValueError(
                        "embedding endpoint does not match the approved private endpoint"
                    )
                raise ValueError(
                    "embedding endpoint does not match the approved embedding endpoint"
                )
        if not 64 <= dimensions <= 2000:
            raise ValueError("embedding dimensions must be between 64 and 2000")
        if embedding_protocol == "tei" and not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ValueError("embedding revision must be a pinned 40-character commit")
        if embedding_protocol != "tei" and (
            not revision
            or len(revision) > 256
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]*", revision)
        ):
            raise ValueError("managed embedding revision must be a stable version label")
        if embedding_key_file and embedding_protocol == "tei":
            raise ValueError("TEI embedding protocol does not accept a bearer key")
        if embedding_key_env and embedding_protocol == "tei":
            raise ValueError("TEI embedding protocol does not accept a bearer key")
        if embedding_key_file and embedding_key_env:
            raise ValueError("embedding key file and environment source are mutually exclusive")
        if embedding_key_env and not re.fullmatch(
            r"[A-Z_][A-Z0-9_]{0,127}", embedding_key_env
        ):
            raise ValueError("embedding key environment variable name is invalid")
        if (
            not loopback
            and embedding_protocol in {"openai", "voyage"}
            and not (embedding_key_file or embedding_key_env)
        ):
            raise ValueError("remote managed embedding endpoint requires a key source")
        maximum_embedding_batch_size = (
            128 if embedding_protocol == "tei" else 512
        )
        if not 1 <= embedding_batch_size <= maximum_embedding_batch_size:
            raise ValueError(
                "embedding batch size must be between 1 and "
                f"{maximum_embedding_batch_size}"
            )
        if (
            type(embedding_workers) is not int
            or not 1 <= embedding_workers <= 8
        ):
            raise ValueError("embedding workers must be between 1 and 8")
        if embedding_protocol == "tei" and embedding_workers != 1:
            raise ValueError("TEI embedding workers must be one")
        if not 1 <= planner_samples <= 3:
            raise ValueError("planner samples must be between 1 and 3")
        planner_values = (
            planner_url,
            planner_approved_url,
            planner_model,
            planner_key_file,
        )
        if any(planner_values) and not all(planner_values):
            raise ValueError(
                "planner URL, approved URL, model, and key file must be configured together"
            )
        if planner_url:
            planner_parsed = urlsplit(planner_url)
            if (
                planner_parsed.scheme != "https"
                or not planner_parsed.hostname
                or planner_parsed.username
                or planner_parsed.password
                or planner_parsed.query
                or planner_parsed.fragment
            ):
                raise ValueError("planner endpoint must be a plain HTTPS base URL")
            if planner_url.rstrip("/") != planner_approved_url.rstrip("/"):
                raise ValueError("planner endpoint does not match the approved router")
        self.embedding_protocol = embedding_protocol
        self.embedding_url = embedding_url.rstrip("/")
        self.embedding_key_file = (
            Path(embedding_key_file) if embedding_key_file else None
        )
        self.embedding_key_env = embedding_key_env
        self.model = model
        self.revision = revision
        self.dimensions = dimensions
        self.document_prefix = document_prefix
        self.query_prefix = (
            QUERY_INSTRUCTION
            if query_prefix is None and embedding_protocol == "tei"
            else (query_prefix or "")
        )
        self.planner_url = planner_url.rstrip("/") if planner_url else None
        self.planner_model = planner_model
        self.planner_key_file = Path(planner_key_file) if planner_key_file else None
        self.timeout_seconds = timeout_seconds
        self.embedding_batch_size = embedding_batch_size
        self.embedding_workers = embedding_workers
        self.planner_samples = planner_samples
        self.cache_size = max(0, cache_size)
        self.cache_ttl_seconds = max(0.0, cache_ttl_seconds)
        self._cache_lock = threading.RLock()
        # The pinned CPU sidecar deliberately accepts one request at a time.
        # Queue callers here so the threaded HTTP server does not turn ordinary
        # query concurrency into immediate sidecar 429s and lexical fallbacks.
        self._embedding_lock = (
            threading.Lock() if embedding_protocol == "tei" else None
        )
        # Managed providers can keep a response socket active beyond its per-read
        # timeout. Bound optional query work by total wall time and capacity so a
        # slow provider cannot hold the retrieval request or grow an unbounded
        # queue. Document ingestion remains on the direct, lossless path.
        self._query_executor = ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix="recall-query-embedding",
        )
        self._query_slots = threading.BoundedSemaphore(8)
        self._document_executor = (
            ThreadPoolExecutor(
                max_workers=embedding_workers,
                thread_name_prefix="recall-document-embedding",
            )
            if embedding_protocol != "tei" and embedding_workers > 1
            else None
        )
        self._plan_cache: OrderedDict[str, tuple[float, SearchPlan]] = OrderedDict()
        self._query_embedding_cache: OrderedDict[
            str, tuple[float, tuple[float, ...]]
        ] = OrderedDict()
        self._embedding_identity_checked = False

    @classmethod
    def from_env(cls) -> SemanticRuntime | None:
        embedding_url = os.environ.get("RECALL_EMBEDDING_URL", "").strip()
        if not embedding_url:
            return None
        embedding_protocol = os.environ.get(
            "RECALL_EMBEDDING_PROTOCOL", "tei"
        ).strip()
        model = os.environ.get(
            "RECALL_EMBEDDING_MODEL",
            "Qwen/Qwen3-Embedding-0.6B"
            if embedding_protocol == "tei"
            else "",
        ).strip()
        if not model:
            raise ValueError("RECALL_EMBEDDING_MODEL is required")
        revision = os.environ.get("RECALL_EMBEDDING_REVISION", "").strip()
        if not revision:
            revision = (
                DEFAULT_EMBEDDING_REVISION
                if embedding_protocol == "tei"
                else model
            )
        return cls(
            embedding_protocol=embedding_protocol,
            embedding_url=embedding_url,
            embedding_approved_url=(
                os.environ.get("RECALL_EMBEDDING_APPROVED_URL") or None
            ),
            embedding_key_file=(
                os.environ.get("RECALL_EMBEDDING_KEY_FILE") or None
            ),
            embedding_key_env=(
                os.environ.get("RECALL_EMBEDDING_KEY_ENV") or None
            ),
            model=model,
            revision=revision,
            dimensions=int(os.environ.get("RECALL_EMBEDDING_DIMENSIONS", "512")),
            document_prefix=os.environ.get("RECALL_EMBEDDING_DOCUMENT_PREFIX", ""),
            query_prefix=os.environ.get("RECALL_EMBEDDING_QUERY_PREFIX"),
            planner_url=os.environ.get("RECALL_LITELLM_URL") or None,
            planner_approved_url=os.environ.get("RECALL_LITELLM_APPROVED_URL") or None,
            planner_model=os.environ.get("RECALL_LITELLM_MODEL") or None,
            planner_key_file=os.environ.get("RECALL_LITELLM_KEY_FILE") or None,
            timeout_seconds=float(
                os.environ.get("RECALL_SEMANTIC_TIMEOUT_SECONDS", "15")
            ),
            embedding_batch_size=int(
                os.environ.get(
                    "RECALL_EMBEDDING_BATCH_SIZE",
                    "1" if embedding_protocol == "tei" else "64",
                )
            ),
            embedding_workers=int(
                os.environ.get("RECALL_EMBEDDING_WORKERS", "1")
            ),
            planner_samples=int(os.environ.get("RECALL_PLANNER_SAMPLES", "2")),
            cache_size=int(os.environ.get("RECALL_SEMANTIC_CACHE_SIZE", "256")),
            cache_ttl_seconds=float(
                os.environ.get("RECALL_SEMANTIC_CACHE_TTL_SECONDS", "900")
            ),
        )

    @property
    def fingerprint(self) -> str:
        value = "\0".join(
            (
                DOCUMENT_EMBEDDING_CONTRACT,
                self.embedding_protocol,
                self.model,
                self.revision,
                str(self.dimensions),
                self.document_prefix,
                self.query_prefix,
            )
        )
        return hashlib.sha256(value.encode()).hexdigest()

    @staticmethod
    def _cache_key(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    def _cache_get(self, cache: OrderedDict, key: str):
        if not self.cache_size or not self.cache_ttl_seconds:
            return None
        with self._cache_lock:
            entry = cache.get(key)
            if entry is None:
                return None
            created_at, value = entry
            if time.monotonic() - created_at >= self.cache_ttl_seconds:
                del cache[key]
                return None
            cache.move_to_end(key)
            return value

    def _cache_put(self, cache: OrderedDict, key: str, value) -> None:
        if not self.cache_size or not self.cache_ttl_seconds:
            return
        with self._cache_lock:
            cache[key] = (time.monotonic(), value)
            cache.move_to_end(key)
            while len(cache) > self.cache_size:
                cache.popitem(last=False)

    def _post(self, url: str, payload: dict, headers: dict[str, str] | None = None):
        request = urllib.request.Request(
            url,
            data=json.dumps(payload, separators=(",", ":")).encode(),
            headers={"Content-Type": "application/json", **(headers or {})},
            method="POST",
        )
        opener = urllib.request.build_opener(_RejectRedirect())
        with opener.open(request, timeout=self.timeout_seconds) as response:
            return self._read_json(response)

    def _get(self, url: str):
        request = urllib.request.Request(url, method="GET")
        opener = urllib.request.build_opener(_RejectRedirect())
        with opener.open(request, timeout=self.timeout_seconds) as response:
            return self._read_json(response)

    @staticmethod
    def _read_json(response):
        payload = response.read(MAX_SEMANTIC_RESPONSE_BYTES + 1)
        if len(payload) > MAX_SEMANTIC_RESPONSE_BYTES:
            raise ValueError("semantic endpoint response is too large")
        return json.loads(payload)

    def _ensure_embedding_identity(self) -> None:
        if self.embedding_protocol != "tei":
            return
        with self._cache_lock:
            if self._embedding_identity_checked:
                return
            info = self._get(self.embedding_url + "/info")
            if not isinstance(info, dict) or (
                info.get("model_id") != self.model
                or info.get("model_sha") != self.revision
                or info.get("model_dtype") != "float32"
            ):
                raise ValueError(
                    "embedding sidecar identity does not match the pinned runtime"
                )
            self._embedding_identity_checked = True

    @staticmethod
    def _read_owner_only_key(path: Path | None, label: str) -> str:
        if path is None:
            raise ValueError(f"{label} key file is not configured")
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) & 0o077
            ):
                raise PermissionError(f"{label} key file must be owner-only")
            value = os.read(descriptor, 8192).decode().strip()
        finally:
            os.close(descriptor)
        if not value or len(value) > 4096:
            raise ValueError(f"{label} key file is invalid")
        return value

    def _read_embedding_key(self) -> str:
        if self.embedding_key_file is not None:
            return self._read_owner_only_key(self.embedding_key_file, "embedding")
        if self.embedding_key_env is None:
            raise ValueError("embedding key source is not configured")
        value = os.environ.get(self.embedding_key_env, "").strip()
        if not value or len(value) > 4096:
            raise ValueError("embedding key environment variable is unavailable")
        return value

    def _validate_vectors(self, values: object, expected: int) -> list[list[float]]:
        if not isinstance(values, list) or len(values) != expected:
            raise ValueError("embedding endpoint returned the wrong batch size")
        result = []
        for vector in values:
            if not isinstance(vector, list) or len(vector) != self.dimensions:
                raise ValueError("embedding endpoint returned the wrong dimensions")
            converted = [float(value) for value in vector]
            if not all(math.isfinite(value) for value in converted):
                raise ValueError("embedding endpoint returned non-finite values")
            result.append(converted)
        return result

    def _openai_vectors(self, response: object, expected: int) -> list[list[float]]:
        if not isinstance(response, dict) or not isinstance(response.get("data"), list):
            raise ValueError("embedding endpoint returned an invalid response")
        ordered: list[object | None] = [None] * expected
        for item in response["data"]:
            if not isinstance(item, dict):
                raise ValueError("embedding endpoint returned an invalid item")
            index = item.get("index")
            if (
                not isinstance(index, int)
                or not 0 <= index < expected
                or ordered[index] is not None
            ):
                raise ValueError("embedding endpoint returned invalid indices")
            ordered[index] = item.get("embedding")
        if any(value is None for value in ordered):
            raise ValueError("embedding endpoint returned the wrong batch size")
        return self._validate_vectors(ordered, expected)

    @property
    def _managed_embedding_endpoint(self) -> str:
        if self.embedding_url.endswith("/v1"):
            return self.embedding_url + "/embeddings"
        return self.embedding_url + "/v1/embeddings"

    def _embed(self, texts: list[str], *, input_type: str) -> list[list[float]]:
        if not texts:
            return []
        lock = self._embedding_lock or nullcontext()
        with lock:
            self._ensure_embedding_identity()
            batches = [
                [
                    self._document_text(
                        (
                            self.document_prefix
                            if input_type == "document"
                            else self.query_prefix
                        )
                        + value
                    )
                    for value in texts[start : start + self.embedding_batch_size]
                ]
                for start in range(0, len(texts), self.embedding_batch_size)
            ]
            result = []
            for batch in batches:
                if self.embedding_protocol == "tei":
                    values = self._post(
                        self.embedding_url + "/embed",
                        {
                            "inputs": batch,
                            "truncate": True,
                            "dimensions": self.dimensions,
                        },
                    )
                    result.extend(self._validate_vectors(values, len(batch)))
                    continue
            if self.embedding_protocol == "tei":
                return result
            headers = {}
            if self.embedding_key_file or self.embedding_key_env:
                headers["Authorization"] = "Bearer " + self._read_embedding_key()
            if input_type != "document" or self._document_executor is None:
                for batch in batches:
                    result.extend(
                        self._embed_managed_batch(
                            batch,
                            input_type=input_type,
                            headers=headers,
                        )
                    )
                return result
            futures = [
                self._document_executor.submit(
                    self._embed_managed_batch,
                    batch,
                    input_type=input_type,
                    headers=headers,
                )
                for batch in batches
            ]
            for future in futures:
                result.extend(future.result())
            return result

    def _embed_managed_batch(
        self,
        batch: list[str],
        *,
        input_type: str,
        headers: dict[str, str],
    ) -> list[list[float]]:
        payload: dict[str, object] = {
            "model": self.model,
            "input": batch,
        }
        if self.embedding_protocol == "voyage":
            payload.update(
                {
                    "input_type": input_type,
                    "output_dimension": self.dimensions,
                    "output_dtype": "float",
                    "truncation": True,
                }
            )
        else:
            payload.update(
                {
                    "encoding_format": "float",
                    "dimensions": self.dimensions,
                }
            )
        try:
            try:
                response = self._post(
                    self._managed_embedding_endpoint, payload, headers
                )
            except json.JSONDecodeError:
                if input_type != "document":
                    raise
                response = self._post(
                    self._managed_embedding_endpoint, payload, headers
                )
        except urllib.error.HTTPError as error:
            splittable = (
                error.code == 413
                or 500 <= error.code <= 504
                or (
                    self.embedding_protocol == "voyage"
                    and input_type == "document"
                    and error.code == 400
                )
            )
            if not splittable or len(batch) == 1:
                raise
            error.close()
            midpoint = len(batch) // 2
            return self._embed_managed_batch(
                batch[:midpoint],
                input_type=input_type,
                headers=headers,
            ) + self._embed_managed_batch(
                batch[midpoint:],
                input_type=input_type,
                headers=headers,
            )
        return self._openai_vectors(response, len(batch))

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return self._embed(texts, input_type="document")

    @staticmethod
    def _document_text(value: str) -> str:
        if len(value) <= MAX_DOCUMENT_CHARS:
            return value
        remaining = MAX_DOCUMENT_CHARS - len(DOCUMENT_CLIP_MARKER)
        head = (remaining + 1) // 2
        tail = remaining - head
        return value[:head] + DOCUMENT_CLIP_MARKER + value[-tail:]

    def embed_queries(self, queries: list[str]) -> list[list[float]]:
        """Embed query expansions in one local batch, caching by content hash."""
        results: list[list[float] | None] = [None] * len(queries)
        missing: dict[str, list[int]] = {}
        for index, query in enumerate(queries):
            key = self._cache_key(query)
            cached = self._cache_get(self._query_embedding_cache, key)
            if cached is not None:
                results[index] = list(cached)
            else:
                missing.setdefault(query, []).append(index)
        if missing:
            values = list(missing)
            vectors = self._embed(values, input_type="query")
            for value, vector in zip(values, vectors, strict=True):
                self._cache_put(
                    self._query_embedding_cache, self._cache_key(value), tuple(vector)
                )
                for index in missing[value]:
                    results[index] = list(vector)
        if any(
            value is None for value in results
        ):  # Defensive invariant; never return partial batches.
            raise ValueError("query embedding batch is incomplete")
        return [value for value in results if value is not None]

    def embed_query(self, query: str) -> list[float]:
        return self.embed_queries([query])[0]

    def embed_query_bounded(self, query: str) -> list[float]:
        """Embed an optional search query within a hard wall-clock budget."""
        if not self._query_slots.acquire(blocking=False):
            raise TimeoutError("query embedding capacity is unavailable")

        def run() -> list[float]:
            try:
                return self.embed_query(query)
            finally:
                self._query_slots.release()

        try:
            future = self._query_executor.submit(run)
        except Exception:
            self._query_slots.release()
            raise
        try:
            return future.result(timeout=self.timeout_seconds)
        except FutureTimeoutError as error:
            if future.cancel():
                self._query_slots.release()
            raise TimeoutError("query embedding deadline exceeded") from error

    def _read_planner_key(self) -> str:
        return self._read_owner_only_key(self.planner_key_file, "planner")

    @staticmethod
    def _json_object(value: str) -> dict:
        match = re.search(r"\{.*\}", value, re.S)
        if not match:
            raise ValueError("planner returned no JSON object")
        parsed = json.loads(match.group(0))
        if not isinstance(parsed, dict):
            raise ValueError("planner returned invalid JSON")
        return parsed

    def plan(self, query: str) -> SearchPlan | None:
        if not self.planner_url:
            return None
        cache_key = self._cache_key(query)
        cached = self._cache_get(self._plan_cache, cache_key)
        if cached is not None:
            return cached
        key = self._read_planner_key()

        def sample(seed: int) -> SearchPlan:
            body = self._post(
                self.planner_url + "/v1/chat/completions",
                {
                    "model": self.planner_model,
                    "temperature": 0,
                    "seed": seed,
                    "messages": [
                        {"role": "system", "content": PLANNER_PROMPT},
                        {"role": "user", "content": query},
                    ],
                },
                {"Authorization": "Bearer " + key},
            )
            try:
                content = body["choices"][0]["message"]["content"]
                parsed = self._json_object(content)
            except (KeyError, IndexError, TypeError) as exc:
                raise ValueError("planner returned an invalid response") from exc
            searchable = parsed.get("searchable") is True
            phrases = []
            if searchable and isinstance(parsed.get("phrases"), list):
                for raw in parsed["phrases"][:8]:
                    phrase = " ".join(re.findall(r"[A-Za-z0-9_./#-]+", str(raw)))[:80]
                    if phrase and phrase.casefold() not in {
                        value.casefold() for value in phrases
                    }:
                        phrases.append(phrase)
            return SearchPlan(searchable=searchable, phrases=tuple(phrases))

        plans: list[SearchPlan] = []
        errors: list[Exception] = []
        with ThreadPoolExecutor(max_workers=self.planner_samples) as executor:
            futures = [
                executor.submit(sample, seed) for seed in range(self.planner_samples)
            ]
            for future in futures:
                try:
                    plans.append(future.result())
                except Exception as exc:
                    errors.append(exc)
        if not plans:
            raise errors[0]
        phrases = []
        searchable = any(value.searchable for value in plans)
        for value in plans:
            if not value.searchable:
                continue
            for phrase in value.phrases:
                if phrase.casefold() not in {
                    existing.casefold() for existing in phrases
                }:
                    phrases.append(phrase)
                if len(phrases) >= 12:
                    break
            if len(phrases) >= 12:
                break
        plan = SearchPlan(searchable=searchable, phrases=tuple(phrases))
        self._cache_put(self._plan_cache, cache_key, plan)
        return plan
