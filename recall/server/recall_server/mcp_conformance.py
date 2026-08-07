from __future__ import annotations

import copy
import http.client
import json
import os
import secrets
import ssl
import stat
import subprocess
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .mcp import (
    LATEST_PROTOCOL_VERSION,
    NOTIFICATION_METHODS,
    READ_TOOLS,
    REQUEST_METHODS,
    SUPPORTED_PROTOCOL_VERSIONS,
    WRITE_TOOLS,
)

CONFIG_KEYS = frozenset(
    {
        "schema_version",
        "url",
        "origin",
        "owner_token_file",
        "read_only_token_file",
        "isolated_token_file",
        "private_queries_file",
        "output_file",
    }
)
CAPABILITY_CLASSES = ("owner", "read_only", "isolated")
LIFECYCLE_PATHS = (
    "initialize",
    "initialized_notification",
    "ping",
    "tool_discovery",
    "private_search",
    "receipt_resolution",
    "show_around",
    "show_tail",
    "show_prompts",
    "related_context",
    "capture",
    "post_capture_search",
    "capture_replay",
    "forget",
    "forget_replay",
    "post_forget_search",
)
ABUSE_CELLS = (
    "missing_auth",
    "unsupported_protocol",
    "unknown_tool",
    "read_only_tool_discovery",
    "read_only_write_call",
    "isolated_search",
    "unresolved_receipt",
    "capture_origin_spoof",
    "oversized_query",
    "conflicting_show_window",
    "closed_rest_route",
)
MUTANTS = (
    "omitted_case",
    "swallowed_tool_error",
    "unresolved_receipt",
    "duplicate_capture",
    "ineffective_forget",
    "leaked_response_body",
)


class ConformanceError(ValueError):
    pass


def _inside_git_repository(path: Path) -> bool:
    result = subprocess.run(
        ["git", "-C", str(path.parent), "rev-parse", "--show-toplevel"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def _read_private_file(path: Path, *, maximum: int = 64 * 1024) -> bytes:
    try:
        before = path.lstat()
    except OSError as error:
        raise ConformanceError("private file is unavailable") from error
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ConformanceError("private file must be regular and non-symlink")
    if stat.S_IMODE(before.st_mode) != 0o600:
        raise ConformanceError("private file must have mode 0600")
    if _inside_git_repository(path):
        raise ConformanceError("private file must be outside a git repository")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ConformanceError("private file could not be opened safely") from error
    try:
        after = os.fstat(descriptor)
        if (
            not stat.S_ISREG(after.st_mode)
            or stat.S_IMODE(after.st_mode) != 0o600
            or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
        ):
            raise ConformanceError("private file changed during validation")
        if after.st_size > maximum:
            raise ConformanceError("private file exceeds its size limit")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(remaining, 8192))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > maximum:
            raise ConformanceError("private file exceeds its size limit")
        return payload
    finally:
        os.close(descriptor)


def _private_json(path: Path, *, maximum: int = 64 * 1024) -> Any:
    try:
        return json.loads(_read_private_file(path, maximum=maximum))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ConformanceError("private file is not valid JSON") from error


def _closed_object(value: Any, keys: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ConformanceError(f"{label} has an invalid shape")
    return value


def _token(path: Path) -> str:
    value = _closed_object(_private_json(path), frozenset({"token"}), "token file")
    token = value["token"]
    if (
        not isinstance(token, str)
        or not token
        or len(token) > 8192
        or any(not 0x21 <= ord(character) <= 0x7E for character in token)
    ):
        raise ConformanceError("token file has an invalid token")
    return token


def _validated_url(value: Any) -> str:
    if not isinstance(value, str):
        raise ConformanceError("url is invalid")
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        parsed.port
    except ValueError as error:
        raise ConformanceError("url is invalid") from error
    loopback = hostname in {"127.0.0.1", "::1", "localhost"}
    if (
        parsed.scheme not in ({"https"} if not loopback else {"http", "https"})
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path != "/mcp"
        or not hostname
    ):
        raise ConformanceError("url must be an HTTPS MCP endpoint")
    return value


def _validated_origin(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConformanceError("origin is invalid")
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        parsed.port
    except ValueError as error:
        raise ConformanceError("origin is invalid") from error
    if (
        parsed.scheme != "https"
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ConformanceError("origin is invalid")
    return value.rstrip("/")


@dataclass(frozen=True)
class McpConformanceConfig:
    url: str
    origin: str | None
    owner_token: str
    read_only_token: str
    isolated_token: str
    private_queries: tuple[str, ...]
    output_file: Path

    @classmethod
    def load(cls, path: str | Path) -> "McpConformanceConfig":
        config_path = Path(path).expanduser().absolute()
        raw = _private_json(config_path)
        if not isinstance(raw, dict) or set(raw) - CONFIG_KEYS:
            raise ConformanceError("config contains unknown or plaintext fields")
        required = CONFIG_KEYS - {"origin"}
        if set(raw) - {"origin"} != required or raw.get("schema_version") != 1:
            raise ConformanceError("config has an invalid shape")
        references = [
            Path(raw[key]).expanduser().absolute()
            for key in (
                "owner_token_file",
                "read_only_token_file",
                "isolated_token_file",
                "private_queries_file",
                "output_file",
            )
            if isinstance(raw.get(key), str)
        ]
        if len(references) != 5 or len(set(references)) != 5:
            raise ConformanceError("config requires five separate private files")
        owner, read_only, isolated, queries_path, output = references
        output_payload = _read_private_file(output, maximum=1024 * 1024)
        del output_payload
        query_value = _closed_object(
            _private_json(queries_path),
            frozenset({"queries"}),
            "private queries file",
        )
        queries = query_value["queries"]
        if (
            not isinstance(queries, list)
            or not 1 <= len(queries) <= 5
            or any(
                not isinstance(query, str)
                or not query.strip()
                or len(query) > 8192
                or _cannot_encode_utf8(query)
                for query in queries
            )
        ):
            raise ConformanceError("private queries file is invalid")
        tokens = (_token(owner), _token(read_only), _token(isolated))
        if len(set(tokens)) != 3:
            raise ConformanceError("each capability class requires a distinct token")
        return cls(
            url=_validated_url(raw["url"]),
            origin=_validated_origin(raw.get("origin")),
            owner_token=tokens[0],
            read_only_token=tokens[1],
            isolated_token=tokens[2],
            private_queries=tuple(queries),
            output_file=output,
        )


def _cannot_encode_utf8(value: str) -> bool:
    try:
        value.encode()
    except UnicodeEncodeError:
        return True
    return False


def coverage_manifest() -> dict[str, list[str]]:
    tools = READ_TOOLS + WRITE_TOOLS
    return {
        "protocol_versions": sorted(SUPPORTED_PROTOCOL_VERSIONS),
        "methods": list(REQUEST_METHODS + NOTIFICATION_METHODS),
        "tools": [tool["name"] for tool in tools],
        "capability_classes": list(CAPABILITY_CLASSES),
        "declared_arguments": sorted(
            f"{tool['name']}.{argument}"
            for tool in tools
            for argument in tool["inputSchema"]["properties"]
        ),
        "lifecycle_paths": list(LIFECYCLE_PATHS),
        "abuse_cells": list(ABUSE_CELLS),
    }


def mutate_report(report: dict[str, Any], mutant: str) -> dict[str, Any]:
    if mutant not in MUTANTS:
        raise ConformanceError("unknown mutant")
    mutated = copy.deepcopy(report)
    if mutant == "omitted_case":
        mutated["coverage"]["executed"] -= 1
        mutated["coverage"]["missing"] += 1
        mutated["coverage"]["ratio"] = (
            mutated["coverage"]["executed"] / mutated["coverage"]["expected"]
        )
    elif mutant == "swallowed_tool_error":
        mutated["assertions"]["tool_errors_observed"] -= 1
    elif mutant == "unresolved_receipt":
        mutated["assertions"]["receipts_resolved"] -= 1
        mutated["assertions"]["receipt_resolution_rate"] = (
            mutated["assertions"]["receipts_resolved"]
            / mutated["assertions"]["receipts_found"]
        )
    elif mutant == "duplicate_capture":
        mutated["assertions"]["capture_event_delta"] = 2
    elif mutant == "ineffective_forget":
        mutated["assertions"]["live_hits_after_forget"] = 1
    elif mutant == "leaked_response_body":
        mutated["assertions"]["response_bodies_emitted"] = 1
    return mutated


def validate_report(report: dict[str, Any]) -> None:
    try:
        if set(report) != {
            "schema_version",
            "status",
            "coverage",
            "checks",
            "private_queries",
            "assertions",
        }:
            raise ConformanceError("report contains an unexpected field")
        if report["schema_version"] != 1 or report["status"] != "pass":
            raise ConformanceError("run did not pass")
        coverage = report["coverage"]
        manifest = coverage_manifest()
        expected_cells = sum(len(values) for values in manifest.values())
        expected_dimensions = {
            "protocol_versions": len(manifest["protocol_versions"]),
            "methods": len(manifest["methods"]),
            "tools": len(manifest["tools"]),
            "capability_classes": len(manifest["capability_classes"]),
            "declared_arguments": len(manifest["declared_arguments"]),
            "lifecycle_paths": len(manifest["lifecycle_paths"]),
            "abuse_cells": len(manifest["abuse_cells"]),
        }
        if (
            set(coverage)
            != {
                "expected",
                "executed",
                "missing",
                "ratio",
                *expected_dimensions,
            }
            or coverage["expected"] != expected_cells
            or coverage["executed"] != coverage["expected"]
            or coverage["missing"] != 0
            or coverage["ratio"] != 1.0
            or any(
                coverage[name] != count
                for name, count in expected_dimensions.items()
            )
        ):
            raise ConformanceError("coverage is incomplete")
        checks = report["checks"]
        if (
            set(checks) != {"passed", "failed"}
            or checks["passed"] != coverage["expected"]
            or checks["failed"] != 0
        ):
            raise ConformanceError("one or more conformance checks failed")
        private_queries = report["private_queries"]
        if (
            set(private_queries) != {"total", "with_evidence"}
            or not 1 <= private_queries["total"] <= 5
            or private_queries["with_evidence"] != private_queries["total"]
        ):
            raise ConformanceError("private query coverage is incomplete")
        assertions = report["assertions"]
        if (
            set(assertions)
            != {
                "expected_tool_errors",
                "tool_errors_observed",
                "receipts_found",
                "receipts_resolved",
                "receipt_resolution_rate",
                "capture_event_delta",
                "capture_replay_event_delta",
                "capture_same_receipt",
                "live_hits_after_forget",
                "forget_replay",
                "credential_values_emitted",
                "response_bodies_emitted",
            }
            or assertions["expected_tool_errors"]
            != assertions["tool_errors_observed"]
            or assertions["receipts_found"] < 1
            or assertions["receipts_resolved"]
            != assertions["receipts_found"]
            or assertions["receipt_resolution_rate"] != 1.0
            or assertions["capture_event_delta"] != 1
            or assertions["capture_replay_event_delta"] != 0
            or assertions["capture_same_receipt"] is not True
            or assertions["live_hits_after_forget"] != 0
            or assertions["forget_replay"] is not True
            or assertions["credential_values_emitted"] != 0
            or assertions["response_bodies_emitted"] != 0
        ):
            raise ConformanceError("a lifecycle or safety assertion failed")
    except (KeyError, TypeError, ZeroDivisionError) as error:
        raise ConformanceError("report has an invalid shape") from error


def _expected_cells() -> frozenset[str]:
    manifest = coverage_manifest()
    return frozenset(
        [
            *(f"protocol:{value}" for value in manifest["protocol_versions"]),
            *(f"method:{value}" for value in manifest["methods"]),
            *(f"tool:{value}" for value in manifest["tools"]),
            *(
                f"capability:{value}"
                for value in manifest["capability_classes"]
            ),
            *(
                f"argument:{value}"
                for value in manifest["declared_arguments"]
            ),
            *(
                f"lifecycle:{value}"
                for value in manifest["lifecycle_paths"]
            ),
            *(f"abuse:{value}" for value in manifest["abuse_cells"]),
        ]
    )


class _McpHttpClient:
    def __init__(self, config: McpConformanceConfig) -> None:
        self.config = config
        self.parsed = urlsplit(config.url)
        self.request_id = 0

    def _request(
        self,
        token: str | None,
        *,
        protocol: str | None,
        body: dict[str, Any] | None,
        method: str = "POST",
        path: str | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        port = self.parsed.port or (443 if self.parsed.scheme == "https" else 80)
        if self.parsed.scheme == "https":
            connection: http.client.HTTPConnection = http.client.HTTPSConnection(
                self.parsed.hostname,
                port,
                timeout=20,
                context=ssl.create_default_context(),
            )
        else:
            connection = http.client.HTTPConnection(
                self.parsed.hostname,
                port,
                timeout=20,
            )
        headers = {"Accept": "application/json, text/event-stream"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if protocol is not None:
            headers["MCP-Protocol-Version"] = protocol
        if self.config.origin is not None:
            headers["Origin"] = self.config.origin
        payload = None
        if body is not None:
            if protocol == LATEST_PROTOCOL_VERSION:
                params = body.setdefault("params", {})
                params.setdefault("_meta", {
                    "io.modelcontextprotocol/protocolVersion": protocol,
                    "io.modelcontextprotocol/clientInfo": {
                        "name": "recall-conformance",
                        "version": "1",
                    },
                    "io.modelcontextprotocol/clientCapabilities": {},
                })
                headers["Mcp-Method"] = body["method"]
                if body["method"] == "tools/call":
                    headers["Mcp-Name"] = params["name"]
                elif body["method"] in {
                    "tasks/get", "tasks/update", "tasks/cancel"
                }:
                    headers["Mcp-Name"] = params["taskId"]
            payload = json.dumps(body, ensure_ascii=False).encode()
            headers.update(
                {
                    "Content-Type": "application/json",
                    "Content-Length": str(len(payload)),
                }
            )
        try:
            connection.request(
                method,
                path or self.parsed.path,
                body=payload,
                headers=headers,
            )
            response = connection.getresponse()
            raw = response.read(1024 * 1024 + 1)
            response_headers = {
                key.casefold(): value for key, value in response.getheaders()
            }
        except (
            OSError,
            UnicodeError,
            ValueError,
            http.client.HTTPException,
        ) as error:
            raise ConformanceError("MCP transport failed") from error
        finally:
            connection.close()
        if len(raw) > 1024 * 1024:
            raise ConformanceError("MCP response exceeded the harness bound")
        if 300 <= response.status < 400:
            raise ConformanceError("MCP redirects are forbidden")
        return response.status, response_headers, raw

    def rpc(
        self,
        token: str | None,
        protocol: str | None,
        method: str,
        params: dict[str, Any] | None = None,
    ) -> tuple[int, dict[str, Any]]:
        self.request_id += 1
        status, _headers, raw = self._request(
            token,
            protocol=protocol,
            body={
                "jsonrpc": "2.0",
                "id": self.request_id,
                "method": method,
                "params": params or {},
            },
        )
        try:
            response = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ConformanceError("MCP response was not JSON") from error
        if not isinstance(response, dict):
            raise ConformanceError("MCP response had an invalid shape")
        return status, response

    def notification(
        self,
        token: str,
        protocol: str,
        method: str,
    ) -> tuple[int, bytes]:
        status, _headers, raw = self._request(
            token,
            protocol=protocol,
            body={"jsonrpc": "2.0", "method": method, "params": {}},
        )
        return status, raw

    def route(self, token: str, method: str, path: str) -> int:
        status, _headers, _raw = self._request(
            token,
            protocol=None,
            body=None,
            method=method,
            path=path,
        )
        return status


def _result(response: dict[str, Any]) -> dict[str, Any]:
    result = response.get("result")
    if (
        not isinstance(result, dict)
        or result.get("isError") is True
        or not isinstance(result.get("structuredContent"), dict)
    ):
        raise ConformanceError("MCP tool call failed")
    return result["structuredContent"]


def _tool(
    client: _McpHttpClient,
    token: str,
    protocol: str,
    name: str,
    arguments: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    status, response = client.rpc(
        token,
        protocol,
        "tools/call",
        {"name": name, "arguments": arguments},
    )
    if status != 200:
        raise ConformanceError("MCP tool HTTP status was invalid")
    return _result(response), response


def _error(
    client: _McpHttpClient,
    token: str,
    protocol: str,
    name: str,
    arguments: dict[str, Any],
) -> None:
    status, response = client.rpc(
        token,
        protocol,
        "tools/call",
        {"name": name, "arguments": arguments},
    )
    if status != 200 or not isinstance(response.get("error"), dict):
        raise ConformanceError("expected MCP tool error was not observed")


def _mark_tool(
    executed: set[str],
    tool_name: str,
) -> None:
    executed.add(f"tool:{tool_name}")
    schema = next(
        tool["inputSchema"]
        for tool in READ_TOOLS + WRITE_TOOLS
        if tool["name"] == tool_name
    )
    executed.update(
        f"argument:{tool_name}.{argument}"
        for argument in schema["properties"]
    )


def _safe_write(path: Path, report: dict[str, Any]) -> None:
    encoded = (json.dumps(report, sort_keys=True) + "\n").encode()
    if len(encoded) > 64 * 1024:
        raise ConformanceError("aggregate output exceeds its size limit")
    flags = os.O_WRONLY | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ConformanceError("output file could not be opened safely") from error
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise ConformanceError("output file is no longer private")
        written = 0
        while written < len(encoded):
            written += os.write(descriptor, encoded[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def run_conformance(config: McpConformanceConfig) -> dict[str, Any]:
    client = _McpHttpClient(config)
    expected = _expected_cells()
    executed: set[str] = set()
    tool_errors = 0
    latest = max(
        version
        for version in SUPPORTED_PROTOCOL_VERSIONS
        if version != LATEST_PROTOCOL_VERSION
    )

    for protocol in sorted(SUPPORTED_PROTOCOL_VERSIONS):
        if protocol == LATEST_PROTOCOL_VERSION:
            status, discovered = client.rpc(
                config.owner_token,
                protocol,
                "server/discover",
            )
            if (
                status != 200
                or protocol not in discovered.get("result", {}).get(
                    "supportedVersions", []
                )
            ):
                raise ConformanceError("protocol discovery failed")
            executed.update({
                f"protocol:{protocol}",
                "method:server/discover",
            })
            continue
        status, initialized = client.rpc(
            config.owner_token,
            protocol,
            "initialize",
            {
                "protocolVersion": protocol,
                "capabilities": {},
                "clientInfo": {
                    "name": "recall-conformance",
                    "version": "1",
                },
            },
        )
        if (
            status != 200
            or initialized.get("result", {}).get("protocolVersion") != protocol
        ):
            raise ConformanceError("protocol initialization failed")
        executed.add(f"protocol:{protocol}")
    executed.update({"method:initialize", "lifecycle:initialize"})

    status, subscription = client.rpc(
        config.owner_token,
        LATEST_PROTOCOL_VERSION,
        "subscriptions/listen",
        {"notifications": {
            "taskIds": ["tsk_0123456789abcdef0123456789abcdef"],
        }},
    )
    if (
        status != 400
        or subscription.get("error", {}).get("code") != -32003
    ):
        raise ConformanceError("task subscription capability gate failed")
    executed.add("method:subscriptions/listen")

    status, raw = client.notification(
        config.owner_token,
        latest,
        "notifications/initialized",
    )
    if status != 202 or raw:
        raise ConformanceError("initialized notification failed")
    executed.update(
        {
            "method:notifications/initialized",
            "lifecycle:initialized_notification",
        }
    )

    status, pinged = client.rpc(config.owner_token, latest, "ping")
    if status != 200 or pinged.get("result") != {}:
        raise ConformanceError("ping failed")
    executed.update({"method:ping", "lifecycle:ping"})

    for task_method in ("tasks/get", "tasks/update", "tasks/cancel"):
        params = {"taskId": "tsk_0123456789abcdef0123456789abcdef"}
        if task_method == "tasks/update":
            params["inputResponses"] = {}
        status, task_response = client.rpc(
            config.owner_token,
            latest,
            task_method,
            params,
        )
        if (
            status != 200
            or task_response.get("error", {}).get("code") != -32601
        ):
            raise ConformanceError(
                "unconfigured task lifecycle did not fall back cleanly"
            )
        executed.add(f"method:{task_method}")

    status, listed = client.rpc(config.owner_token, latest, "tools/list")
    owner_tools = {
        tool["name"] for tool in listed.get("result", {}).get("tools", [])
    }
    expected_owner_tools = {
        tool["name"] for tool in READ_TOOLS + WRITE_TOOLS
    }
    if status != 200 or owner_tools != expected_owner_tools:
        raise ConformanceError("owner tool discovery failed")
    executed.update(
        {
            "method:tools/list",
            "lifecycle:tool_discovery",
            "capability:owner",
        }
    )

    receipts: set[str] = set()
    for query in config.private_queries:
        result, _response = _tool(
            client,
            config.owner_token,
            latest,
            "recall_search",
            {"query": query, "filters": {}, "limit": 5},
        )
        rows = result.get("results")
        if not isinstance(rows, list) or not rows:
            raise ConformanceError("a private query returned no evidence")
        for row in rows:
            receipt = row.get("receipt") if isinstance(row, dict) else None
            if not isinstance(receipt, str) or not receipt.startswith("recall://"):
                raise ConformanceError("search returned an invalid receipt")
            receipts.add(receipt)
    executed.add("lifecycle:private_search")
    _mark_tool(executed, "recall_search")
    executed.update({"method:tools/call"})

    for receipt in receipts:
        _tool(
            client,
            config.owner_token,
            latest,
            "recall_show",
            {"target": receipt, "tail": 1},
        )
    executed.add("lifecycle:receipt_resolution")
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    first_receipt = sorted(receipts)[0]
    _tool(
        client,
        config.owner_token,
        latest,
        "recall_show",
        {"target": first_receipt, "around": now},
    )
    executed.add("lifecycle:show_around")
    _tool(
        client,
        config.owner_token,
        latest,
        "recall_show",
        {"target": first_receipt, "tail": 1},
    )
    executed.add("lifecycle:show_tail")
    _tool(
        client,
        config.owner_token,
        latest,
        "recall_show",
        {"target": first_receipt, "tail": 1, "prompts": True},
    )
    executed.add("lifecycle:show_prompts")
    _mark_tool(executed, "recall_show")

    _tool(
        client,
        config.owner_token,
        latest,
        "recall_related",
        {
            "cwd": "/synthetic/recall-conformance",
            "branch": "synthetic/conformance",
            "limit": 1,
            "mains_only": True,
            "fast": True,
        },
    )
    executed.add("lifecycle:related_context")
    _mark_tool(executed, "recall_related")

    marker = "synthetic-conformance-" + secrets.token_hex(16)
    capture_arguments = {
        "schema_version": 1,
        "title": "Synthetic conformance memory",
        "body": marker,
        "occurred_at": now,
        "tags": ["synthetic", "conformance"],
        "provenance": {"uri": "manual://recall-conformance"},
    }
    captured, captured_response = _tool(
        client,
        config.owner_token,
        latest,
        "recall_capture",
        capture_arguments,
    )
    capture_receipt = captured.get("receipt")
    if (
        not isinstance(capture_receipt, str)
        or captured.get("replay") is not False
        or marker in json.dumps(captured_response)
    ):
        raise ConformanceError("capture lifecycle failed")
    executed.add("lifecycle:capture")
    after_capture, _response = _tool(
        client,
        config.owner_token,
        latest,
        "recall_search",
        {"query": marker, "limit": 5},
    )
    capture_rows = after_capture.get("results")
    if (
        not isinstance(capture_rows, list)
        or len(capture_rows) != 1
        or capture_rows[0].get("receipt") != capture_receipt
    ):
        raise ConformanceError("capture did not create one live canonical result")
    capture_hits = len(capture_rows)
    executed.add("lifecycle:post_capture_search")
    replayed, replayed_response = _tool(
        client,
        config.owner_token,
        latest,
        "recall_capture",
        capture_arguments,
    )
    if (
        replayed.get("receipt") != capture_receipt
        or replayed.get("replay") is not True
        or marker in json.dumps(replayed_response)
    ):
        raise ConformanceError("capture replay was not idempotent")
    after_replay, _response = _tool(
        client,
        config.owner_token,
        latest,
        "recall_search",
        {"query": marker, "limit": 5},
    )
    replay_rows = after_replay.get("results")
    if (
        not isinstance(replay_rows, list)
        or len(replay_rows) != capture_hits
        or replay_rows[0].get("receipt") != capture_receipt
    ):
        raise ConformanceError("capture replay changed canonical live results")
    replay_event_delta = len(replay_rows) - capture_hits
    executed.add("lifecycle:capture_replay")
    _mark_tool(executed, "recall_capture")

    forgotten, _response = _tool(
        client,
        config.owner_token,
        latest,
        "recall_forget",
        {"receipt": capture_receipt},
    )
    if forgotten.get("replay") is not False:
        raise ConformanceError("forget lifecycle failed")
    executed.add("lifecycle:forget")
    forgotten_replay, _response = _tool(
        client,
        config.owner_token,
        latest,
        "recall_forget",
        {"receipt": capture_receipt},
    )
    if (
        forgotten_replay.get("receipt") != forgotten.get("receipt")
        or forgotten_replay.get("replay") is not True
    ):
        raise ConformanceError("forget replay was not idempotent")
    executed.add("lifecycle:forget_replay")
    _mark_tool(executed, "recall_forget")
    after_forget, _response = _tool(
        client,
        config.owner_token,
        latest,
        "recall_search",
        {"query": marker, "limit": 5},
    )
    live_hits_after_forget = len(after_forget.get("results", []))
    if live_hits_after_forget:
        raise ConformanceError("forgotten capture remains searchable")
    executed.add("lifecycle:post_forget_search")

    status, read_listed = client.rpc(
        config.read_only_token,
        latest,
        "tools/list",
    )
    read_tools = {
        tool["name"]
        for tool in read_listed.get("result", {}).get("tools", [])
    }
    if (
        status != 200
        or read_tools != {tool["name"] for tool in READ_TOOLS}
    ):
        raise ConformanceError("read-only tool discovery failed")
    executed.update(
        {
            "capability:read_only",
            "abuse:read_only_tool_discovery",
        }
    )
    _error(
        client,
        config.read_only_token,
        latest,
        "recall_capture",
        capture_arguments,
    )
    tool_errors += 1
    executed.add("abuse:read_only_write_call")

    isolated, _response = _tool(
        client,
        config.isolated_token,
        latest,
        "recall_search",
        {"query": config.private_queries[0], "limit": 5},
    )
    if isolated.get("results") != []:
        raise ConformanceError("isolated principal returned evidence")
    executed.update({"capability:isolated", "abuse:isolated_search"})

    status, _response = client.rpc(
        None,
        latest,
        "tools/list",
    )
    if status != 401:
        raise ConformanceError("missing authentication was accepted")
    executed.add("abuse:missing_auth")

    status, _response = client.rpc(
        config.owner_token,
        "2024-11-05",
        "tools/list",
    )
    if status != 400:
        raise ConformanceError("unsupported protocol was accepted")
    executed.add("abuse:unsupported_protocol")

    for abuse, name, arguments in (
        ("unknown_tool", "recall_unknown", {}),
        (
            "unresolved_receipt",
            "recall_show",
            {"target": "recall://synthetic:missing/missing?rev=1"},
        ),
        (
            "capture_origin_spoof",
            "recall_capture",
            {**capture_arguments, "origin": "spoofed"},
        ),
        (
            "oversized_query",
            "recall_search",
            {"query": "x" * 8193},
        ),
        (
            "conflicting_show_window",
            "recall_show",
            {"target": first_receipt, "around": now, "tail": 1},
        ),
    ):
        _error(
            client,
            config.owner_token,
            latest,
            name,
            arguments,
        )
        tool_errors += 1
        executed.add(f"abuse:{abuse}")

    if client.route(config.owner_token, "GET", "/v1/doctor") != 404:
        raise ConformanceError("non-MCP application route was exposed")
    executed.add("abuse:closed_rest_route")

    missing = expected - executed
    unexpected = executed - expected
    if missing or unexpected:
        raise ConformanceError("code-derived coverage was incomplete")
    report = {
        "schema_version": 1,
        "status": "pass",
        "coverage": {
            "expected": len(expected),
            "executed": len(executed),
            "missing": 0,
            "ratio": 1.0,
            "protocol_versions": len(SUPPORTED_PROTOCOL_VERSIONS),
            "methods": len(REQUEST_METHODS + NOTIFICATION_METHODS),
            "tools": len(READ_TOOLS + WRITE_TOOLS),
            "capability_classes": len(CAPABILITY_CLASSES),
            "declared_arguments": len(coverage_manifest()["declared_arguments"]),
            "lifecycle_paths": len(LIFECYCLE_PATHS),
            "abuse_cells": len(ABUSE_CELLS),
        },
        "checks": {"passed": len(executed), "failed": 0},
        "private_queries": {
            "total": len(config.private_queries),
            "with_evidence": len(config.private_queries),
        },
        "assertions": {
            "expected_tool_errors": 6,
            "tool_errors_observed": tool_errors,
            "receipts_found": len(receipts),
            "receipts_resolved": len(receipts),
            "receipt_resolution_rate": 1.0,
            "capture_event_delta": capture_hits,
            "capture_replay_event_delta": replay_event_delta,
            "capture_same_receipt": True,
            "live_hits_after_forget": live_hits_after_forget,
            "forget_replay": True,
            "credential_values_emitted": 0,
            "response_bodies_emitted": 0,
        },
    }
    validate_report(report)
    _safe_write(config.output_file, report)
    return report
