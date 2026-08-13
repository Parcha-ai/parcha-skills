"""Bounded JSON transport for code-owned remote connector operations."""

from __future__ import annotations

import json
import math
import os
import re
import stat
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping

from connectors.sdk import ConnectorRateLimited
from privacy.transport import open_no_redirect


FIELD = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
OPERATION_ID = re.compile(r"[a-z][a-z0-9_.-]{2,95}\Z")
PLACEHOLDER = re.compile(r"\{([a-z][a-z0-9_]{0,63})\}")
HEADER_NAME = re.compile(r"[A-Za-z][A-Za-z0-9-]{0,63}\Z")
QUERY_NAME = re.compile(r"[A-Za-z][A-Za-z0-9._-]{0,127}\Z")
RESERVED_HEADERS = {"accept", "authorization", "content-type", "user-agent"}
MAX_AUTHORITY_BYTES = 16_384
MAX_PARAMETER_BYTES = 4_096
DEFAULT_MAX_RESPONSE_BYTES = 8_000_000
MAX_REQUEST_BYTES = 1_000_000
MAX_BINARY_BYTES = 64 * 1024 * 1024


class RemoteApiError(RuntimeError):
    """Stable, content-free remote transport error."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _closed_fields(value: tuple[str, ...], label: str) -> tuple[str, ...]:
    if (
        not isinstance(value, tuple)
        or len(value) > 32
        or len(value) != len(set(value))
        or value != tuple(sorted(value))
        or any(not isinstance(item, str) or not FIELD.fullmatch(item) for item in value)
    ):
        raise ValueError(f"invalid {label}")
    return value


@dataclass(frozen=True)
class RemoteOperation:
    """One immutable operation declared by bundled connector code."""

    method: str
    path_template: str
    path_fields: tuple[str, ...]
    query_fields: tuple[str, ...]
    json_fields: tuple[str, ...] = ()
    fixed_query: Mapping[str, Any] | None = None
    fixed_json: Mapping[str, Any] | None = None
    _fixed_json_bytes: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.method not in {"GET", "POST"}:
            raise ValueError("invalid remote method")
        if (
            not isinstance(self.path_template, str)
            or not self.path_template.startswith("/")
            or len(self.path_template.encode()) > 1_024
            or "://" in self.path_template
            or "?" in self.path_template
            or "#" in self.path_template
            or "\\" in self.path_template
            or ".." in self.path_template.split("/")
        ):
            raise ValueError("invalid remote path")
        path_fields = _closed_fields(self.path_fields, "path fields")
        query_fields = _closed_fields(self.query_fields, "query fields")
        json_fields = _closed_fields(self.json_fields, "json fields")
        placeholders = tuple(PLACEHOLDER.findall(self.path_template))
        if (
            len(placeholders) != len(set(placeholders))
            or set(placeholders) != set(path_fields)
            or self.path_template.count("{") != len(placeholders)
            or self.path_template.count("}") != len(placeholders)
        ):
            raise ValueError("remote path placeholders do not match path fields")
        object.__setattr__(self, "path_fields", path_fields)
        object.__setattr__(self, "query_fields", query_fields)
        object.__setattr__(self, "json_fields", json_fields)
        if self.fixed_query is None:
            fixed_query: Mapping[str, Any] = {}
        elif isinstance(self.fixed_query, Mapping):
            fixed_query = self.fixed_query
        else:
            raise ValueError("invalid fixed query")
        if (
            len(fixed_query) > 32
            or any(
                not isinstance(key, str)
                or not QUERY_NAME.fullmatch(key)
                or key in query_fields
                for key in fixed_query
            )
        ):
            raise ValueError("invalid fixed query")
        rendered_fixed_query = {}
        try:
            for key in sorted(fixed_query):
                rendered_fixed_query[key] = _scalar(fixed_query[key])
        except RemoteApiError:
            raise ValueError("invalid fixed query") from None
        object.__setattr__(
            self,
            "fixed_query",
            MappingProxyType(rendered_fixed_query),
        )
        if self.fixed_json is None:
            fixed_json: Mapping[str, Any] = {}
        elif isinstance(self.fixed_json, Mapping):
            fixed_json = self.fixed_json
        else:
            raise ValueError("invalid fixed json")
        if (
            any(not isinstance(key, str) or not FIELD.fullmatch(key) for key in fixed_json)
            or set(fixed_json) & set(json_fields)
        ):
            raise ValueError("invalid fixed json")
        try:
            fixed_bytes = json.dumps(
                dict(fixed_json),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode()
        except (TypeError, ValueError):
            raise ValueError("invalid fixed json") from None
        if len(fixed_bytes) > MAX_REQUEST_BYTES:
            raise ValueError("fixed json is too large")
        fixed_copy = json.loads(fixed_bytes)
        object.__setattr__(self, "fixed_json", MappingProxyType(fixed_copy))
        object.__setattr__(self, "_fixed_json_bytes", fixed_bytes)
        if self.method == "GET" and (json_fields or fixed_copy):
            raise ValueError("GET operation cannot define a json body")


def _validate_origin(value: str) -> str:
    if not isinstance(value, str) or len(value) > 512:
        raise ValueError("invalid remote origin")
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or parsed.port not in {None, 443}
        or parsed.hostname != parsed.hostname.lower()
    ):
        raise ValueError("invalid remote origin")
    return f"https://{parsed.hostname}"


def _authority(path: Path) -> str:
    path = Path(path)
    if not path.is_absolute():
        raise RemoteApiError("authority_invalid")
    try:
        parent = path.parent.lstat()
        metadata = path.lstat()
    except OSError:
        raise RemoteApiError("authority_invalid") from None
    if (
        stat.S_ISLNK(parent.st_mode)
        or not stat.S_ISDIR(parent.st_mode)
        or stat.S_IMODE(parent.st_mode) != 0o700
        or stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not 0 < metadata.st_size <= MAX_AUTHORITY_BYTES
    ):
        raise RemoteApiError("authority_invalid")
    descriptor = None
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise RemoteApiError("authority_invalid")
        chunks = []
        remaining = MAX_AUTHORITY_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
    except RemoteApiError:
        raise
    except OSError:
        raise RemoteApiError("authority_invalid") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
    try:
        value = raw.decode().strip()
    except UnicodeDecodeError:
        raise RemoteApiError("authority_invalid") from None
    if (
        not value
        or len(value.encode()) > MAX_AUTHORITY_BYTES
        or any(character.isspace() for character in value)
        or any(ord(character) < 33 or ord(character) > 126 for character in value)
    ):
        raise RemoteApiError("authority_invalid")
    return value


def _scalar(value: Any) -> str:
    if isinstance(value, bool):
        rendered = "true" if value else "false"
    elif isinstance(value, int) and not isinstance(value, bool):
        rendered = str(value)
    elif isinstance(value, float) and math.isfinite(value):
        rendered = str(value)
    elif isinstance(value, str):
        rendered = value
    else:
        raise RemoteApiError("parameter_invalid")
    if not rendered or len(rendered.encode()) > MAX_PARAMETER_BYTES:
        raise RemoteApiError("parameter_invalid")
    return rendered


def _retry_after(headers: Mapping[str, str] | None) -> int:
    raw = headers.get("Retry-After") if headers is not None else None
    try:
        value = int(raw) if raw is not None else 60
    except (TypeError, ValueError):
        value = 60
    return max(1, min(value, 3_600))


class BoundedJsonRail:
    """Execute only an immutable in-code operation map against one HTTPS origin."""

    def __init__(
        self,
        *,
        origin: str,
        authority_path: Path,
        authorization_scheme: str,
        operations: Mapping[str, RemoteOperation],
        fixed_headers: Mapping[str, str] | None = None,
        opener: Callable[..., Any] = open_no_redirect,
        timeout_seconds: int = 30,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        binary_hosts: tuple[str, ...] = (),
    ):
        self.origin = _validate_origin(origin)
        self.authority_path = Path(authority_path)
        if authorization_scheme != "Bearer":
            raise ValueError("invalid authorization scheme")
        self.authorization_scheme = authorization_scheme
        if (
            not isinstance(operations, Mapping)
            or not operations
            or len(operations) > 128
            or any(
                not isinstance(key, str)
                or not OPERATION_ID.fullmatch(key)
                or not isinstance(operation, RemoteOperation)
                for key, operation in operations.items()
            )
            or len(operations) != len(set(operations))
        ):
            raise ValueError("invalid remote operations")
        self.operations = MappingProxyType(dict(operations))
        header_values = {} if fixed_headers is None else fixed_headers
        if (
            not isinstance(header_values, Mapping)
            or len(header_values) > 16
            or any(
                not isinstance(key, str)
                or not HEADER_NAME.fullmatch(key)
                or key.lower() in RESERVED_HEADERS
                or not isinstance(value, str)
                or not value
                or len(value.encode()) > 1_024
                or any(ord(character) < 32 or ord(character) > 126 for character in value)
                for key, value in header_values.items()
            )
            or len({key.lower() for key in header_values}) != len(header_values)
        ):
            raise ValueError("invalid fixed headers")
        self.fixed_headers = MappingProxyType(dict(header_values))
        if not callable(opener):
            raise ValueError("invalid remote opener")
        self.opener = opener
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
            raise ValueError("invalid remote timeout")
        if (
            type(max_response_bytes) is not int
            or not 1 <= max_response_bytes <= DEFAULT_MAX_RESPONSE_BYTES
        ):
            raise ValueError("invalid remote response bound")
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes
        if (
            not isinstance(binary_hosts, tuple)
            or len(binary_hosts) > 8
            or binary_hosts != tuple(sorted(set(binary_hosts)))
            or any(
                not isinstance(host, str)
                or not re.fullmatch(r"[a-z0-9.-]{1,253}", host)
                or host.startswith(".") or host.endswith(".") or ".." in host
                for host in binary_hosts
            )
        ):
            raise ValueError("invalid binary hosts")
        self.binary_hosts = binary_hosts

    def download_binary(self, url: str, *, maximum_bytes: int = MAX_BINARY_BYTES) -> tuple[bytes, str]:
        """Fetch one provider-owned artifact from an exact code-owned host allow-list."""
        parsed = urllib.parse.urlsplit(url) if isinstance(url, str) else None
        if (
            parsed is None or parsed.scheme != "https" or parsed.hostname not in self.binary_hosts
            or parsed.username or parsed.password or parsed.fragment
            or not parsed.path.startswith("/files-pri/")
            or not 1 <= maximum_bytes <= MAX_BINARY_BYTES
        ):
            raise RemoteApiError("binary_url_invalid")
        authority = _authority(self.authority_path)
        request = urllib.request.Request(
            url,
            method="GET",
            headers={
                "Accept": "*/*",
                "Authorization": f"{self.authorization_scheme} {authority}",
                "User-Agent": "recall-remote-connector/1",
            },
        )
        try:
            response = self.opener(request, timeout=self.timeout_seconds)
            with response:
                content_type = str(response.headers.get("Content-Type", "")).split(";", 1)[0]
                if not re.fullmatch(
                    r"[a-z0-9][a-z0-9.+-]{0,63}/[a-z0-9][a-z0-9.+-]{0,127}",
                    content_type,
                ):
                    raise RemoteApiError("content_type_invalid")
                declared = response.headers.get("Content-Length")
                if declared is not None and (not declared.isdigit() or int(declared) > maximum_bytes):
                    raise RemoteApiError("response_too_large")
                chunks = []
                remaining = maximum_bytes + 1
                while remaining:
                    chunk = response.read(min(1_048_576, remaining))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                payload = b"".join(chunks)
                if not payload or len(payload) > maximum_bytes:
                    raise RemoteApiError("response_too_large")
                return payload, content_type
        except RemoteApiError:
            raise
        except urllib.error.HTTPError as error:
            if 300 <= error.code <= 399:
                raise RemoteApiError("redirect_rejected") from None
            if error.code in {401, 403}:
                raise RemoteApiError("authority_forbidden") from None
            raise RemoteApiError("upstream_error") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise RemoteApiError("upstream_unavailable") from None

    def request(
        self,
        operation_id: str,
        *,
        path: Mapping[str, Any] | None = None,
        query: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
    ) -> Any:
        operation = self.operations.get(operation_id)
        if operation is None:
            raise RemoteApiError("operation_not_allowed")
        path_values = {} if path is None else path
        query_values = {} if query is None else query
        body_values = {} if json_body is None else json_body
        if (
            not isinstance(path_values, Mapping)
            or set(path_values) != set(operation.path_fields)
            or not isinstance(query_values, Mapping)
            or set(query_values) - set(operation.query_fields)
            or not isinstance(body_values, Mapping)
            or set(body_values) - set(operation.json_fields)
        ):
            raise RemoteApiError("parameter_not_allowed")
        rendered_path = operation.path_template
        for field_name in operation.path_fields:
            rendered_path = rendered_path.replace(
                "{" + field_name + "}",
                urllib.parse.quote(_scalar(path_values[field_name]), safe=""),
            )
        query_pairs = list(operation.fixed_query.items()) + [
            (field_name, _scalar(query_values[field_name]))
            for field_name in operation.query_fields
            if field_name in query_values
        ]
        url = self.origin + rendered_path
        if query_pairs:
            url += "?" + urllib.parse.urlencode(query_pairs)
        body = None
        if operation.method == "POST":
            body_value = json.loads(operation._fixed_json_bytes)
            body_value.update(dict(body_values))
            try:
                body = json.dumps(
                    body_value,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode()
            except (TypeError, ValueError):
                raise RemoteApiError("parameter_invalid") from None
            if len(body) > MAX_REQUEST_BYTES:
                raise RemoteApiError("request_too_large")
        elif body_values:
            raise RemoteApiError("parameter_not_allowed")
        authority = _authority(self.authority_path)
        headers = {
            "Accept": "application/json",
            "Authorization": f"{self.authorization_scheme} {authority}",
            "User-Agent": "recall-remote-connector/1",
            **self.fixed_headers,
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            url,
            data=body,
            method=operation.method,
            headers=headers,
        )
        try:
            response = self.opener(request, timeout=self.timeout_seconds)
            with response:
                content_type = str(response.headers.get("Content-Type", "")).split(";", 1)[0]
                if content_type not in {"application/json", "application/problem+json"}:
                    raise RemoteApiError("content_type_invalid")
                raw_length = response.headers.get("Content-Length")
                if raw_length is not None:
                    try:
                        declared_length = int(raw_length)
                    except (TypeError, ValueError):
                        raise RemoteApiError("response_invalid") from None
                    if declared_length < 0 or declared_length > self.max_response_bytes:
                        raise RemoteApiError("response_too_large")
                chunks = []
                remaining = self.max_response_bytes + 1
                while remaining:
                    chunk = response.read(remaining)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                payload = b"".join(chunks)
                if len(payload) > self.max_response_bytes:
                    raise RemoteApiError("response_too_large")
        except RemoteApiError:
            raise
        except urllib.error.HTTPError as error:
            if 300 <= error.code <= 399:
                raise RemoteApiError("redirect_rejected") from None
            if error.code == 429:
                raise ConnectorRateLimited(
                    retry_after_seconds=_retry_after(error.headers),
                ) from None
            if error.code == 401:
                raise RemoteApiError("authority_revoked") from None
            if error.code == 403:
                raise RemoteApiError("authority_forbidden") from None
            if error.code == 410:
                raise RemoteApiError("cursor_expired") from None
            raise RemoteApiError("upstream_error") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise RemoteApiError("upstream_unavailable") from None
        except Exception:
            raise RemoteApiError("upstream_unavailable") from None
        try:
            return json.loads(
                payload,
                parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise RemoteApiError("response_invalid") from None


__all__ = [
    "BoundedJsonRail",
    "DEFAULT_MAX_RESPONSE_BYTES",
    "RemoteApiError",
    "RemoteOperation",
    "MAX_BINARY_BYTES",
]
