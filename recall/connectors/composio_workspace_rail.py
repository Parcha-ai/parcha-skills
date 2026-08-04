"""Exact-account, read-only Google Workspace transport through Composio."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping
from urllib.parse import quote, urlsplit
import urllib.error
import urllib.request

from connectors.workspace_rail import (
    DEFAULT_MAX_OUTPUT_BYTES,
    MAX_EXPORT_BYTES,
    OPERATIONS,
    WorkspaceRailError,
)


AUTHORITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._@+-]{2,255}\Z")
CONNECTED_ACCOUNT = re.compile(r"ca_[A-Za-z0-9_-]{3,252}\Z")
MAX_PARAMETER_BYTES = 16_384


@dataclass(frozen=True)
class ProxyOperation:
    connector_id: str
    toolkit: str
    path: str
    path_params: tuple[str, ...] = ()


PROXY_OPERATIONS = {
    "gmail.users.getProfile": ProxyOperation(
        "google.gmail", "gmail", "/gmail/v1/users/{userId}/profile", ("userId",)
    ),
    "gmail.messages.list": ProxyOperation(
        "google.gmail", "gmail", "/gmail/v1/users/{userId}/messages", ("userId",)
    ),
    "gmail.messages.get": ProxyOperation(
        "google.gmail",
        "gmail",
        "/gmail/v1/users/{userId}/messages/{id}",
        ("userId", "id"),
    ),
    "gmail.messages.attachments.get": ProxyOperation(
        "google.gmail",
        "gmail",
        "/gmail/v1/users/{userId}/messages/{messageId}/attachments/{id}",
        ("userId", "messageId", "id"),
    ),
    "gmail.history.list": ProxyOperation(
        "google.gmail", "gmail", "/gmail/v1/users/{userId}/history", ("userId",)
    ),
    "calendar.calendarList.list": ProxyOperation(
        "google.calendar", "googlecalendar", "/calendar/v3/users/me/calendarList"
    ),
    "calendar.events.list": ProxyOperation(
        "google.calendar",
        "googlecalendar",
        "/calendar/v3/calendars/{calendarId}/events",
        ("calendarId",),
    ),
    "calendar.events.get": ProxyOperation(
        "google.calendar",
        "googlecalendar",
        "/calendar/v3/calendars/{calendarId}/events/{eventId}",
        ("calendarId", "eventId"),
    ),
    "people.people.connections.list": ProxyOperation(
        "google.contacts",
        "googlecontacts",
        "/v1/{resourceName}/connections",
        ("resourceName",),
    ),
    "drive.changes.getStartPageToken": ProxyOperation(
        "google.drive", "googledrive", "/drive/v3/changes/startPageToken"
    ),
    "drive.changes.list": ProxyOperation(
        "google.drive", "googledrive", "/drive/v3/changes"
    ),
    "drive.files.list": ProxyOperation(
        "google.drive", "googledrive", "/drive/v3/files"
    ),
    "drive.files.get": ProxyOperation(
        "google.drive",
        "googledrive",
        "/drive/v3/files/{fileId}",
        ("fileId",),
    ),
}


def toolkit_for_connector(connector_id: str) -> str:
    values = {
        operation.toolkit
        for operation in PROXY_OPERATIONS.values()
        if operation.connector_id == connector_id
    }
    if len(values) != 1:
        raise WorkspaceRailError("connector_not_supported")
    return values.pop()


def _bounded_parameter(value: Any) -> list[str]:
    values = value if isinstance(value, (list, tuple)) else (value,)
    if not values or len(values) > 64:
        raise WorkspaceRailError("invalid_params")
    encoded: list[str] = []
    for item in values:
        if isinstance(item, bool):
            rendered = "true" if item else "false"
        elif isinstance(item, int) and not isinstance(item, bool):
            rendered = str(item)
        elif isinstance(item, str):
            rendered = item
        else:
            raise WorkspaceRailError("invalid_params")
        if "\x00" in rendered or len(rendered.encode()) > MAX_PARAMETER_BYTES:
            raise WorkspaceRailError("invalid_params")
        encoded.append(rendered)
    return encoded


def _request(operation: str, params: Mapping[str, Any], connector_id: str):
    spec = OPERATIONS.get(operation)
    proxy = PROXY_OPERATIONS.get(operation)
    if spec is None or proxy is None or proxy.connector_id != connector_id:
        raise WorkspaceRailError("operation_not_allowed")
    if (
        not isinstance(params, Mapping)
        or not params
        or not all(isinstance(key, str) for key in params)
    ):
        raise WorkspaceRailError("invalid_params")
    if set(params) - spec.params:
        raise WorkspaceRailError("parameter_not_allowed")
    path_values: dict[str, str] = {}
    for name in proxy.path_params:
        if name not in params:
            raise WorkspaceRailError("invalid_params")
        values = _bounded_parameter(params[name])
        if len(values) != 1 or not values[0]:
            raise WorkspaceRailError("invalid_params")
        if name == "resourceName":
            if not values[0].startswith("people/") or values[0].count("/") != 1:
                raise WorkspaceRailError("invalid_params")
            path_values[name] = quote(values[0], safe="/")
        else:
            path_values[name] = quote(values[0], safe="")
    endpoint = proxy.path.format(**path_values)
    parameters = []
    for name in sorted(set(params) - set(proxy.path_params)):
        for value in _bounded_parameter(params[name]):
            parameters.append({"name": name, "value": value, "in": "query"})
    encoded = json.dumps(parameters, sort_keys=True, separators=(",", ":"))
    if len(encoded.encode()) > MAX_PARAMETER_BYTES:
        raise WorkspaceRailError("invalid_params")
    return proxy, endpoint, parameters


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _status_code(value: Any) -> int:
    status = _field(value, "status")
    if isinstance(status, float) and status.is_integer():
        status = int(status)
    if type(status) is not int or not 100 <= status <= 599:
        raise WorkspaceRailError("invalid_response_shape")
    return status


def _upstream_code(status: int) -> str:
    return {
        400: "upstream_invalid_request",
        401: "authority_revoked",
        403: "authority_forbidden",
        404: "not_found",
        410: "cursor_expired",
        429: "rate_limited",
    }.get(status, "upstream_error")


def _sdk_error_code(error: Exception) -> str:
    """Translate Composio's structured SDK failures without exposing payloads."""

    status = getattr(error, "status_code", None)
    body = getattr(error, "body", None)
    detail = body.get("error") if isinstance(body, Mapping) else None
    provider_code = detail.get("code") if isinstance(detail, Mapping) else None
    # Composio 4302 means the session has no active connection for the pinned
    # toolkit. In practice this is the stable failure returned for an expired
    # or otherwise inactive connected account.
    if provider_code == 4302 or status == 401:
        return "authority_revoked"
    if status == 403:
        return "authority_forbidden"
    if status == 429:
        return "rate_limited"
    if status == 400:
        return "upstream_invalid_request"
    return "transport_unavailable"


def _payload(response: Any, operation: str, maximum: int) -> dict[str, Any]:
    status = _status_code(response)
    if status < 200 or status >= 300:
        raise WorkspaceRailError(_upstream_code(status))
    value = _field(response, "data")
    if not isinstance(value, Mapping):
        raise WorkspaceRailError("invalid_response_shape")
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    except (TypeError, ValueError):
        raise WorkspaceRailError("invalid_json") from None
    if len(encoded) > maximum:
        raise WorkspaceRailError("output_too_large")
    copied = json.loads(encoded)
    if not copied:
        raise WorkspaceRailError("empty_success")
    if set(copied) - OPERATIONS[operation].response_fields:
        raise WorkspaceRailError("response_schema_drift")
    return copied


def _default_client(api_key: str, timeout_seconds: int):
    try:
        from composio import Composio
    except ImportError:
        raise WorkspaceRailError("transport_unavailable") from None
    return Composio(api_key=api_key, timeout=float(timeout_seconds))


def _fetch_binary(url: str, *, maximum: int, allowed_hosts: tuple[str, ...]) -> bytes:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.fragment
        or parsed.hostname.casefold() not in allowed_hosts
    ):
        raise WorkspaceRailError("export_invalid")

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    try:
        request = urllib.request.Request(
            url, method="GET", headers={"Accept": "text/plain"}
        )
        with urllib.request.build_opener(NoRedirect()).open(
            request, timeout=60
        ) as response:
            value = response.read(maximum + 1)
    except (OSError, ValueError, urllib.error.HTTPError, urllib.error.URLError):
        raise WorkspaceRailError("upstream_error") from None
    if not value:
        raise WorkspaceRailError("empty_success")
    if len(value) > maximum:
        raise WorkspaceRailError("output_too_large")
    return value


class ComposioWorkspaceRail:
    """WorkspaceClient implementation pinned to one Composio account."""

    def __init__(
        self,
        *,
        api_key: str,
        user_id: str,
        connected_account_id: str,
        connector_id: str,
        timeout_seconds: int = 60,
        max_output_bytes: int = DEFAULT_MAX_OUTPUT_BYTES,
        binary_hosts: tuple[str, ...] = (),
        client_factory: Callable[[str], Any] | None = None,
        binary_fetcher: Callable[..., bytes] | None = None,
    ):
        if (
            not isinstance(api_key, str)
            or not 10 <= len(api_key) <= 4096
            or any(character.isspace() or ord(character) < 33 for character in api_key)
        ):
            raise WorkspaceRailError("credential_reference_invalid")
        if not isinstance(user_id, str) or not AUTHORITY.fullmatch(user_id):
            raise WorkspaceRailError("credential_reference_invalid")
        if not isinstance(connected_account_id, str) or not CONNECTED_ACCOUNT.fullmatch(
            connected_account_id
        ):
            raise WorkspaceRailError("credential_reference_invalid")
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
            raise WorkspaceRailError("invalid_timeout")
        if (
            type(max_output_bytes) is not int
            or not 128 <= max_output_bytes <= DEFAULT_MAX_OUTPUT_BYTES
        ):
            raise WorkspaceRailError("invalid_output_bound")
        normalized_hosts = tuple(sorted({host.casefold() for host in binary_hosts}))
        if any(
            not host or "/" in host or ":" in host or host.startswith(".")
            for host in normalized_hosts
        ):
            raise WorkspaceRailError("invalid_binary_hosts")
        self.api_key = api_key
        self.user_id = user_id
        self.connected_account_id = connected_account_id
        self.connector_id = connector_id
        self.toolkit = toolkit_for_connector(connector_id)
        self.timeout_seconds = timeout_seconds
        self.max_output_bytes = max_output_bytes
        self.binary_hosts = normalized_hosts
        self.client_factory = client_factory
        self.binary_fetcher = binary_fetcher or _fetch_binary
        self._session_value = None

    def _session(self):
        if self._session_value is None:
            try:
                client = (
                    self.client_factory(self.api_key)
                    if self.client_factory is not None
                    else _default_client(self.api_key, self.timeout_seconds)
                )
                self._session_value = client.create(
                    user_id=self.user_id,
                    toolkits=[self.toolkit],
                    connected_accounts={self.toolkit: [self.connected_account_id]},
                )
            except WorkspaceRailError:
                raise
            except TimeoutError:
                raise WorkspaceRailError("upstream_timeout") from None
            except Exception as error:
                raise WorkspaceRailError(_sdk_error_code(error)) from None
        return self._session_value

    def _execute(self, operation: str, params: Mapping[str, Any]):
        proxy, endpoint, parameters = _request(operation, params, self.connector_id)
        try:
            return self._session().proxy_execute(
                toolkit=proxy.toolkit,
                endpoint=endpoint,
                method="GET",
                parameters=parameters,
            )
        except WorkspaceRailError:
            raise
        except TimeoutError:
            raise WorkspaceRailError("upstream_timeout") from None
        except Exception as error:
            raise WorkspaceRailError(_sdk_error_code(error)) from None

    def run(self, operation: str, params: Mapping[str, Any]) -> Any:
        return _payload(
            self._execute(operation, params),
            operation,
            self.max_output_bytes,
        )

    def export_document(
        self,
        *,
        file_id: str,
        mime_type: str = "text/plain",
    ) -> bytes:
        if self.connector_id != "google.drive":
            raise WorkspaceRailError("operation_not_allowed")
        if not isinstance(file_id, str) or not 1 <= len(file_id) <= 512:
            raise WorkspaceRailError("invalid_file_id")
        if mime_type != "text/plain":
            raise WorkspaceRailError("export_type_not_allowed")
        if not self.binary_hosts:
            raise WorkspaceRailError("export_unavailable")
        endpoint = f"/drive/v3/files/{quote(file_id, safe='')}/export"
        try:
            response = self._session().proxy_execute(
                toolkit=self.toolkit,
                endpoint=endpoint,
                method="GET",
                parameters=[{"name": "mimeType", "value": mime_type, "in": "query"}],
            )
        except WorkspaceRailError:
            raise
        except TimeoutError:
            raise WorkspaceRailError("upstream_timeout") from None
        except Exception:
            raise WorkspaceRailError("transport_unavailable") from None
        status = _status_code(response)
        if status < 200 or status >= 300:
            raise WorkspaceRailError(_upstream_code(status))
        binary = _field(response, "binary_data")
        url = _field(binary, "url")
        size = _field(binary, "size")
        if (
            not isinstance(url, str)
            or not isinstance(size, (int, float))
            or isinstance(size, bool)
            or not math.isfinite(size)
            or size <= 0
            or size > MAX_EXPORT_BYTES
        ):
            raise WorkspaceRailError("export_invalid")
        return self.binary_fetcher(
            url,
            maximum=MAX_EXPORT_BYTES,
            allowed_hosts=self.binary_hosts,
        )


__all__ = [
    "ComposioWorkspaceRail",
    "PROXY_OPERATIONS",
    "ProxyOperation",
    "toolkit_for_connector",
]
