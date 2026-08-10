#!/usr/bin/env python3
"""Local, rebuildable search index for Claude Code and Codex JSONL sessions."""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import math
import os
import re
import secrets
import sqlite3
import stat
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = "4"
PARSER_VERSION = 1
MAX_TOOL_INPUT = 2048
MAX_TOOL_OUTPUT = 4096
FTS_LEG_LIMIT = 400
SECRET_RE = re.compile(
    r"[\"']?(?:api[_-]?key|access[_-]?key|private[_-]?key|client[_-]?secret|token|secret|password|bearer|authorization|\bkey)"
    r"[\"']?\s*[=:]\s*[\"']?(?:Bearer\s+)?\S{12,}|"
    r"sk-(?:proj|svcacct|admin)-[A-Za-z0-9_-]{40,}|"
    r"sk-(?!ant-|or-v1-)[A-Za-z0-9_-]{32,}|"
    r"sk-ant-(?:api03|admin01)-[A-Za-z0-9_-]{80,}AA|"
    r"sk-or-v1-[A-Za-z0-9_-]{20,}|gsk_[A-Za-z0-9]{20,}|"
    r"xai-[A-Za-z0-9_-]{20,}|pplx-[A-Za-z0-9_-]{20,}|csk-[A-Za-z0-9_-]{20,}|"
    r"xox[baprs]-[A-Za-z0-9-]{20,}|"
    r"(?:gh[pousr]|github_pat)_[A-Za-z0-9_]{20,}|ops_[A-Za-z0-9_-]{20,}|"
    r"A[KS]IA[A-Z0-9]{16}|AIza[A-Za-z0-9_-]{35}|"
    r"(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{20,}|hf_[A-Za-z0-9]{20,}|"
    r"pcsk_[A-Za-z0-9_-]{20,}|lsv2_[A-Za-z0-9_-]{20,}|"
    r"Bearer\s+[A-Za-z0-9._~+/=-]{20,}|"
    r"[a-z][a-z0-9+.-]*://[^\s/:@]+:[^\s/@]+@",
    re.I,
)
GENERIC_ASSIGNMENT_RE = re.compile(
    r'''(?ix)(?<![A-Za-z0-9_.-])["']?[A-Za-z0-9_.-]*'''
    r'''(?:api[_-]?key|apikey|api|token|secret|password|passwd|pass|credential|creds|key|'''
    r'''access|private[_-]?key|'''
    r'''access[_-]?key|client[_-]?secret|authorization|auth)[A-Za-z0-9_.-]*["']?'''
    r'''\s*[:=]\s*["']?(?:Bearer\s+)?[^\s,"']{12,}'''
)
PROXIMITY_SECRET_RE = re.compile(
    r"\bsntryu_[a-f0-9]{64}\b|"
    r"sentry(?:.|[\n\r]){0,40}?\b[a-f0-9]{64}\b|"
    r"(?:phrase|accessToken|access_token)(?:.|[\n\r]){0,40}?\b[a-z0-9]{64}\b",
    re.I,
)
PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN (?P<label>[A-Z0-9 ]*PRIVATE KEY)-----.*?-----END (?P=label)-----",
    re.DOTALL,
)
PATH_RE = re.compile(r"(?<!\w)(?:/[A-Za-z0-9_@.+~#%=-]+(?:/[A-Za-z0-9_@.+~#%=-]+)+|(?:[A-Za-z0-9_.-]+/)+[A-Za-z0-9_@.+~#%=-]+\.[A-Za-z0-9]+)")
URL_RE = re.compile(r"https?://[^\s<>\]\[\"']+")
UUID_RE = re.compile(r"\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b|\b[0-9a-fA-F]{8}\b")
STOPWORDS = frozenset("""the a an of in on we i was which that did do how what when where why to for with
and or not it its this those these from by at as is are were be been about into over after before
one ones our my me you us your their them they he she his her can could would should will just
than then if else""".split())
REMOTE_READ_COMMANDS = frozenset({"search", "show", "related", "doctor", "session-export"})
REMOTE_WRITE_COMMANDS = frozenset({"put", "delete"})
CLIENT_CONFIG_FIELDS = {"schema_version", "url", "token_file"}
MAX_CLIENT_CONFIG_BYTES = 16_384
MAX_TOKEN_FILE_BYTES = 16_384
MAX_REMOTE_RESPONSE_BYTES = 1024 * 1024
MCP_PROTOCOL_VERSION = "2025-11-25"


class RemoteRecallError(RuntimeError):
    pass


class _RejectRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _validated_remote_base(value: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except (AttributeError, TypeError, ValueError):
        raise RemoteRecallError("remote URL is invalid") from None
    loopback = (
        parsed.scheme == "http"
        and parsed.hostname in {"127.0.0.1", "localhost"}
        and port is not None
    )
    if (
        not isinstance(value, str)
        or not ((parsed.scheme == "https" and parsed.hostname) or loopback)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/", "/mcp"}
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise RemoteRecallError("remote URL is invalid")
    return value.rstrip("/")


def _open_remote(request: urllib.request.Request, *, timeout: float):
    opener = urllib.request.build_opener(_RejectRedirect())
    return opener.open(request, timeout=timeout)


def client_config_path() -> Path:
    configured = os.environ.get("RECALL_CONFIG_FILE")
    return Path(configured).expanduser() if configured else (
        Path.home() / ".config" / "recall-brain" / "client.json"
    )


def _load_private_json(path: Path, *, label: str, max_bytes: int):
    descriptor = None
    try:
        before = path.lstat()
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise RemoteRecallError(f"{label} must be a regular file")
        if stat.S_IMODE(before.st_mode) & 0o077:
            raise RemoteRecallError(f"{label} must have mode 0600")
        if before.st_size > max_bytes:
            raise RemoteRecallError(f"{label} is too large")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or stat.S_IMODE(opened.st_mode) & 0o077
            or opened.st_size > max_bytes
        ):
            raise RemoteRecallError(f"{label} changed during inspection")
        raw = os.read(descriptor, max_bytes + 1)
        if len(raw) > max_bytes:
            raise RemoteRecallError(f"{label} is too large")
        return json.loads(raw)
    except RemoteRecallError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise RemoteRecallError(f"{label} is unreadable or invalid") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def load_client_config() -> dict | None:
    path = client_config_path()
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    except OSError:
        raise RemoteRecallError("client config unavailable") from None
    value = _load_private_json(path, label="client config", max_bytes=MAX_CLIENT_CONFIG_BYTES)
    if not isinstance(value, dict) or set(value) != CLIENT_CONFIG_FIELDS:
        raise RemoteRecallError("client config fields are invalid")
    if value["schema_version"] != 1 or isinstance(value["schema_version"], bool):
        raise RemoteRecallError("client config schema is invalid")
    url = value["url"]
    try:
        parsed = urllib.parse.urlparse(url)
        port = parsed.port
    except (TypeError, ValueError):
        raise RemoteRecallError("client config URL is invalid") from None
    loopback = parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost"}
    if (
        not isinstance(url, str) or not ((parsed.scheme == "https" and parsed.hostname) or loopback)
        or parsed.username or parsed.password or parsed.query or parsed.fragment
        or parsed.path not in {"", "/", "/mcp"} or (port is not None and not 1 <= port <= 65535)
    ):
        raise RemoteRecallError("client config URL is invalid")
    token_file = value["token_file"]
    if not isinstance(token_file, str) or not Path(token_file).expanduser().is_absolute():
        raise RemoteRecallError("client config token reference is invalid")
    return {"url": url.rstrip("/"), "token_file": token_file}


def recall_mode() -> str:
    configured = os.environ.get("RECALL_MODE")
    if configured:
        if configured not in {"local", "remote", "shadow"}:
            raise ValueError("RECALL_MODE must be local, remote, or shadow")
        return configured
    return "remote" if os.environ.get("RECALL_URL") or client_config_path().exists() else "local"


def central_client_configured() -> bool:
    """Return whether this device has selected a central Recall service."""
    return bool(os.environ.get("RECALL_URL") or client_config_path().exists())


def remote_headers() -> dict[str, str]:
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    config = load_client_config()
    configured = os.environ.get("RECALL_TOKEN_FILE") or (
        config["token_file"] if config is not None else None
    )
    if not configured:
        return headers
    path = Path(configured).expanduser()
    value = _load_private_json(path, label="token file", max_bytes=MAX_TOKEN_FILE_BYTES)
    token = value.get("token") if isinstance(value, dict) else None
    if not isinstance(token, str) or not token:
        raise RemoteRecallError("token file has no token")
    headers["Authorization"] = "Bearer " + token
    return headers


def _read_remote_object(response) -> dict:
    raw = response.read(MAX_REMOTE_RESPONSE_BYTES + 1)
    if len(raw) > MAX_REMOTE_RESPONSE_BYTES:
        raise RemoteRecallError("server response is too large")
    rendered = json.loads(raw)
    if not isinstance(rendered, dict):
        raise RemoteRecallError("server returned a non-object response")
    return rendered


def _mcp_time_bound(value):
    if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value + "T00:00:00Z"
    return value


def _mcp_call(base: str, method: str, path: str, body: dict | None) -> dict:
    if path == "/v1/session-export":
        raise RemoteRecallError("session export is not available over MCP")
    tool = {
        "/v1/search": "recall_search",
        "/v1/show": "recall_show",
        "/v1/related": "recall_related",
    }.get(path)
    arguments = body or {}
    if path == "/v1/doctor":
        if method != "GET":
            raise RemoteRecallError("unsupported MCP operation")
        message = {"jsonrpc": "2.0", "id": 1, "method": "ping", "params": {}}
    elif path == "/v1/ingest/batches":
        events = arguments.get("events")
        if method != "POST" or not isinstance(events, list) or len(events) != 1:
            raise RemoteRecallError("unsupported MCP ingest request")
        event = events[0]
        if not isinstance(event, dict):
            raise RemoteRecallError("unsupported MCP ingest request")
        if event.get("kind") == "memory":
            content = event.get("content")
            text = content.get("text") if isinstance(content, dict) else None
            provenance = event.get("provenance")
            uri = provenance.get("uri") if isinstance(provenance, dict) else None
            if not isinstance(text, str) or not text.strip() or len(text) > 32_000:
                raise RemoteRecallError("MCP memory body is invalid")
            title = next((line.strip() for line in text.splitlines() if line.strip()), text.strip())
            tool = "recall_capture"
            arguments = {
                "schema_version": 1,
                "title": title[:500],
                "body": text,
                "occurred_at": event.get("occurred_at"),
                "provenance": {"uri": uri},
            }
        elif event.get("kind") == "tombstone":
            content = event.get("content")
            receipt = content.get("deleted_receipt") if isinstance(content, dict) else None
            tool = "recall_forget"
            arguments = {"receipt": receipt}
        else:
            raise RemoteRecallError("unsupported MCP ingest event")
        message = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        }
    elif tool is not None and method == "POST":
        arguments = {
            key: value for key, value in arguments.items() if value is not None
        }
        filters = arguments.get("filters")
        if isinstance(filters, dict):
            arguments["filters"] = {
                key: _mcp_time_bound(value) if key in {"since", "until"} else value
                for key, value in filters.items()
            }
        message = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        }
    else:
        raise RemoteRecallError("unsupported MCP operation")

    headers = remote_headers()
    headers["Accept"] = "application/json, text/event-stream"
    headers["MCP-Protocol-Version"] = MCP_PROTOCOL_VERSION
    request = urllib.request.Request(
        base,
        data=json.dumps(message, sort_keys=True).encode(),
        method="POST",
        headers=headers,
    )
    try:
        with _open_remote(
            request,
            timeout=float(os.environ.get("RECALL_TIMEOUT", "15")),
        ) as response:
            rendered = _read_remote_object(response)
    except urllib.error.HTTPError as exc:
        exc.read(MAX_REMOTE_RESPONSE_BYTES + 1)
        raise RemoteRecallError(f"HTTP {exc.code}: MCP request failed") from exc
    except (OSError, urllib.error.URLError, json.JSONDecodeError, ValueError) as exc:
        raise RemoteRecallError(type(exc).__name__) from exc

    error = rendered.get("error")
    if isinstance(error, dict):
        code = error.get("code")
        raise RemoteRecallError(f"MCP {code}: tool call failed")
    if rendered.get("id") != message["id"] or not isinstance(rendered.get("result"), dict):
        raise RemoteRecallError("MCP response is invalid")
    result = rendered["result"]
    if message["method"] == "ping":
        return {"status": "ok", "transport": "mcp"}
    structured = result.get("structuredContent")
    if result.get("isError") is True or not isinstance(structured, dict):
        raise RemoteRecallError("MCP tool result is invalid")
    if path == "/v1/ingest/batches":
        receipt = structured.get("receipt")
        if not isinstance(receipt, str) or not receipt:
            raise RemoteRecallError("MCP write result has no receipt")
        return {
            "status": structured.get("status"),
            "inserted": 0 if structured.get("duplicate") else 1,
            "duplicate_events": 1 if structured.get("duplicate") else 0,
            "receipts": [receipt],
        }
    return structured


def remote_request(method: str, path: str, body: dict | None = None,
                   idempotency_key: str | None = None) -> dict:
    config = load_client_config()
    base = _validated_remote_base(os.environ.get("RECALL_URL") or (
        config["url"] if config is not None else ""
    ))
    if not base:
        raise RemoteRecallError("RECALL_URL is required for remote mode")
    if urllib.parse.urlsplit(base).path == "/mcp":
        return _mcp_call(base, method, path, body)
    data = None if body is None else json.dumps(body, sort_keys=True).encode()
    headers = remote_headers()
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    request = urllib.request.Request(base + path, data=data, method=method, headers=headers)
    try:
        with _open_remote(request, timeout=float(os.environ.get("RECALL_TIMEOUT", "15"))) as response:
            return _read_remote_object(response)
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read()).get("error", "HTTP error")
        except (json.JSONDecodeError, AttributeError):
            detail = "HTTP error"
        raise RemoteRecallError(f"HTTP {exc.code}: {detail}") from exc
    except (OSError, urllib.error.URLError, json.JSONDecodeError, ValueError) as exc:
        raise RemoteRecallError(type(exc).__name__) from exc


def remote_execute(args) -> tuple[str, dict]:
    if args.command in REMOTE_WRITE_COMMANDS:
        config = load_client_config()
        configured_base = _validated_remote_base(os.environ.get("RECALL_URL") or (
            config["url"] if config is not None else ""
        ))
        is_mcp = urllib.parse.urlsplit(configured_base).path == "/mcp"
        source_id = args.source_id or os.environ.get("RECALL_WRITE_SOURCE_ID", "")
        if is_mcp and not source_id:
            source_id = "mcp:host-bound"
        if not re.fullmatch(r"[A-Za-z0-9_.:@-]{3,160}", source_id):
            raise RemoteRecallError("--source-id or RECALL_WRITE_SOURCE_ID is required")
        principal_id = args.principal_id or os.environ.get("RECALL_PRINCIPAL_ID", "owner")
        visibility = args.visibility or os.environ.get("RECALL_VISIBILITY", "private")
        if visibility not in {"private", "shared"}:
            raise RemoteRecallError("visibility must be private or shared")
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        if args.command == "put":
            text = args.text if args.text is not None else sys.stdin.read()
            if not text.strip():
                raise RemoteRecallError("memory text must not be empty")
            native_id = "memory-" + uuid.uuid4().hex
            kind = "memory"
            content = {"text": clean_text(text)}
            provenance = {"uri": clean_text(args.provenance_uri)}
        else:
            event_part = args.receipt.split("#", 1)[0]
            try:
                base, revision = event_part.rsplit("?rev=", 1)
                if int(revision) < 1 or not base.startswith("recall://"):
                    raise ValueError
                base = base.removeprefix("recall://")
                receipt_source, native_id = base.split("/", 1)
                if not native_id:
                    raise ValueError
            except (ValueError, TypeError) as exc:
                raise RemoteRecallError("invalid receipt") from exc
            if not is_mcp and receipt_source != source_id:
                raise RemoteRecallError("receipt source does not match write source")
            kind = "tombstone"
            content = {"target_native_id": native_id, "deleted_receipt": event_part}
            provenance = {"uri": "manual://recall_delete"}
        envelope = {
            "schema_version": 1,
            "source_id": source_id,
            "native_id": native_id,
            "native_parent_id": native_id,
            "kind": kind,
            "occurred_at": now,
            "observed_at": now,
            "principal_id": principal_id,
            "visibility": visibility,
            "content_type": "application/json",
            "content": content,
            "provenance": provenance,
            "content_sha256": hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest(),
        }
        key = "recall-skill-v1-" + hashlib.sha256(json.dumps(envelope, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        response = remote_request("POST", "/v1/ingest/batches", {"events": [envelope]}, key)
        receipts = response.get("receipts")
        if not isinstance(receipts, list) or len(receipts) != 1:
            raise RemoteRecallError("ingest response has no exact receipt")
        output = {"kind": kind, "native_id": native_id, "receipt": receipts[0], "status": response.get("status")}
        return json.dumps(output, sort_keys=True) + "\n", {"remote_write": output}
    if args.command == "search":
        filters = {
            key: getattr(args, key)
            for key in (
                "since", "until", "cwd", "branch", "harness",
                "source_id", "source_family", "source_alias",
            )
            if getattr(args, key) is not None
        }
        response = remote_request("POST", "/v1/search", {
            "query": args.query, "filters": filters, "limit": args.limit,
        })
        results = response.get("results")
        if not isinstance(results, list):
            raise RemoteRecallError("search response has no results list")

        def passage_receipt(result: dict) -> str | None:
            ranges = result.get("matching_ranges")
            if not isinstance(ranges, list):
                return None
            for matching_range in ranges:
                if not isinstance(matching_range, dict):
                    continue
                receipts = matching_range.get("receipts")
                if not isinstance(receipts, list):
                    continue
                for receipt in receipts:
                    if isinstance(receipt, str) and receipt:
                        return receipt
            return None

        lines = []
        for rank, result in enumerate(results[:args.limit], 1):
            receipt = result.get("receipt") or passage_receipt(result)
            target = (
                result.get("path")
                or receipt
                or result.get("logical_document_id")
            )
            if not isinstance(target, str) or not target:
                raise RemoteRecallError("search result has no resolvable target")
            if args.paths:
                lines.append(target)
                continue
            terms = ",".join(str(value) for value in result.get("matched_terms", []))
            reasons = result.get("legs") or result.get("reasons") or []
            legs = ",".join(sorted(str(value) for value in reasons))
            text = result.get("text", "")
            if not text and isinstance(result.get("matching_ranges"), list):
                text = next((
                    matching_range.get("text", "")
                    for matching_range in result["matching_ranges"]
                    if isinstance(matching_range, dict)
                    and matching_range.get("text")
                ), "")
            snippet = re.sub(r"\s+", " ", str(text))[:200]
            lines.append(
                f"{rank}. {target}\n"
                f"   {result.get('occurred_at') or result.get('last_occurred_at') or '-'} "
                f"cwd={result.get('cwd') or '-'} "
                f"slot={result.get('slot') or '-'} branch={result.get('branch') or '-'}\n"
                f"   [{result.get('surface') or 'passage'}] {snippet}\n"
                f"   WHY: terms={terms}; legs={legs}; receipt={receipt or '-'}"
            )
        receipt_results = []
        for result in results:
            receipt_result = {
                "path": result.get("path"),
                "receipt": result.get("receipt") or passage_receipt(result),
                "legs": result.get("legs") or result.get("reasons"),
            }
            if isinstance(result.get("logical_document_id"), str):
                receipt_result["logical_document_id"] = result[
                    "logical_document_id"
                ]
            if isinstance(result.get("evidence"), dict):
                receipt_result["evidence"] = result["evidence"]
            receipt_results.append(receipt_result)
        return (
            "\n".join(lines) + ("\n" if lines else ""),
            {"remote_results": receipt_results, "remote_diagnostics": response.get("diagnostics")},
        )
    if args.command == "show":
        response = remote_request("POST", "/v1/show", {
            "target": args.target, "around": args.around, "prompts": args.prompts, "tail": args.tail,
        })
        chunks = response.get("chunks")
        if not isinstance(chunks, list):
            raise RemoteRecallError("show response has no chunks list")
        lines = [
            f"[{chunk.get('occurred_at') or '-'}] {chunk.get('surface') or '-'}: {chunk.get('text') or ''}"
            for chunk in chunks
            if not args.prompts or chunk.get("surface") == "user"
        ]
        return ("\n".join(lines) + ("\n" if lines else ""), {"remote_chunks": chunks})
    if args.command == "session-export":
        target = args.target
        if args.current:
            target = str(resolve_current_session())
        body = {"limit": args.limit}
        if args.cursor:
            body["cursor"] = args.cursor
        else:
            body["target"] = target
        response = remote_request("POST", "/v1/session-export", body)
        if response.get("schema_version") != "recall.session-export.v1":
            raise RemoteRecallError("session export response has unsupported schema")
        return json.dumps(response, sort_keys=True, default=str) + "\n", {"remote_session_export": {
            "boundary_receipt": response.get("session", {}).get("boundary_receipt"),
            "page_receipt": response.get("page", {}).get("page_receipt"),
        }}
    if args.command == "related":
        response = remote_request("POST", "/v1/related", {
            "cwd": args.cwd or str(Path.cwd()), "branch": args.branch, "limit": args.limit,
            "mains_only": args.mains_only, "fast": args.fast,
        })
        results = response.get("results")
        if not isinstance(results, list):
            raise RemoteRecallError("related response has no results list")
        lines = [
            f"{result['path']}\toverlap={result.get('overlap', 0)}\t"
            f"cwd={result.get('cwd') or '-'}\tbranch={result.get('branch') or '-'}"
            for result in results[:args.limit]
        ]
        return ("\n".join(lines) + ("\n" if lines else ""), {"remote_results": results})
    if args.command == "doctor":
        response = remote_request("GET", "/v1/doctor")
        fields = " ".join(f"{key}={response[key]}" for key in sorted(response) if key != "status")
        return (f"OK remote status={response.get('status', 'unknown')} {fields}\n", {"remote_doctor": response})
    raise RemoteRecallError("command has no remote transport")


def append_private_jsonl(path: Path, entry: dict) -> None:
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "a") as output:
        output.write(json.dumps(entry, sort_keys=True) + "\n")


def run_transport(args) -> int:
    if (
        args.command == "index"
        and central_client_configured()
        and not args.allow_local_index
    ):
        print(
            "local index disabled: this device has a central Recall profile; "
            "`index` cannot repair or refresh the central brain. Run `doctor` "
            "to check the remote service. For an intentional local fallback, "
            "rerun with --allow-local-index.",
            file=sys.stderr,
        )
        return 2
    mode = recall_mode()
    if args.command in REMOTE_WRITE_COMMANDS:
        if mode == "local":
            print("remote recall unavailable: explicit memory writes require RECALL_URL", file=sys.stderr)
            return 2
        try:
            output, _metadata = remote_execute(args)
        except RemoteRecallError as exc:
            print(f"remote recall unavailable: {exc}", file=sys.stderr)
            return 2
        print(output, end="")
        return 0
    if args.command not in REMOTE_READ_COMMANDS or mode == "local":
        return args.func(args)
    if mode == "remote":
        try:
            output, metadata = remote_execute(args)
        except RemoteRecallError as exc:
            print(f"remote recall unavailable: {exc}", file=sys.stderr)
            return 2
        if args.command == "search" and os.environ.get("RECALL_REMOTE_TRACE"):
            append_private_jsonl(Path(os.environ["RECALL_REMOTE_TRACE"]), {
                "schema_version": 1, "observed_at": datetime.now(timezone.utc).isoformat(),
                "command": "search", **metadata,
            })
        print(output, end="")
        return 0

    local_out, local_err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(local_out), contextlib.redirect_stderr(local_err):
        local_code = args.func(args)
    entry = {
        "schema_version": 1,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "command": args.command,
        "local_exit": local_code,
        "local_sha256": hashlib.sha256(local_out.getvalue().encode()).hexdigest(),
    }
    if args.command == "search" and args.paths:
        entry["local_paths"] = [line for line in local_out.getvalue().splitlines() if line]
    try:
        remote_out, metadata = remote_execute(args)
        entry.update(metadata)
        entry["remote_sha256"] = hashlib.sha256(remote_out.encode()).hexdigest()
        entry["diverged"] = remote_out != local_out.getvalue()
    except RemoteRecallError as exc:
        entry["remote_error"] = str(exc)
        entry["diverged"] = True
    log = Path(os.environ.get("RECALL_SHADOW_LOG", Path.home() / ".recall/shadow.jsonl")).expanduser()
    append_private_jsonl(log, entry)
    if local_err.getvalue():
        print(local_err.getvalue(), end="", file=sys.stderr)
    print(local_out.getvalue(), end="")
    return local_code


def paths() -> tuple[Path, Path, Path]:
    home = Path.home()
    return (
        Path(os.environ.get("RECALL_CLAUDE_ROOT", home / ".claude/projects")).expanduser(),
        Path(os.environ.get("RECALL_CODEX_ROOT", home / ".codex/sessions")).expanduser(),
        Path(os.environ.get("RECALL_DB", home / ".recall/index.db")).expanduser(),
    )


def epoch(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def iso(value: str | None) -> float | None:
    if not value:
        return None
    result = epoch(value)
    if result is None:
        raise ValueError("expected an ISO-8601 timestamp")
    return result


def clean_text(value) -> str:
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False, sort_keys=True)
    # Redaction can expose a second structural match. For example, replacing a
    # secret-bearing line with a marker can make that marker look like the value
    # of a key on the preceding line. Iterate to a stable representation so the
    # exported digest is valid under every downstream defense pass.
    for _ in range(4):
        if any(
            "\n" in match.group(0) or "\r" in match.group(0)
            for match in GENERIC_ASSIGNMENT_RE.finditer(value)
        ) or PROXIMITY_SECRET_RE.search(value):
            return "[redacted-secret-line]"
        redacted = PRIVATE_KEY_RE.sub("[redacted-private-key-block]", value)
        redacted = "\n".join("[redacted-secret-line]" if (
            SECRET_RE.search(line) or GENERIC_ASSIGNMENT_RE.search(line)
        ) else line for line in redacted.splitlines())
        if redacted == value:
            return redacted
        value = redacted
    return "[redacted-secret-line]"


def clipped(value, limit: int) -> str:
    text = clean_text(value)
    return text[:limit]


def fingerprint(path: Path, size: int | None = None) -> str:
    if size is None:
        size = path.stat().st_size
    with path.open("rb") as fh:
        first = fh.read(min(4096, size))
        fh.seek(max(0, size - 4096))
        last = fh.read(min(4096, size))
    return hashlib.sha256(first + last + str(size).encode()).hexdigest()


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(db_path.parent, 0o700)
    conn = sqlite3.connect(db_path)
    os.chmod(db_path, 0o600)
    # WAL keeps readers live during ingest — a session-start related query
    # must not starve behind a running delta-index's writer lock.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=3000")
    conn.row_factory = sqlite3.Row
    return conn


def connect_ro(db_path: Path) -> sqlite3.Connection:
    """Open an existing database without creating files or changing permissions."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=700")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS files(
      id INTEGER PRIMARY KEY, path TEXT UNIQUE, harness TEXT, size INTEGER,
      mtime_ns INTEGER, fingerprint TEXT, parsed_offset INTEGER,
      parser_version INTEGER, status TEXT);
    CREATE TABLE IF NOT EXISTS sessions(
      id INTEGER PRIMARY KEY, file_id INTEGER, harness TEXT, cwd TEXT, slot TEXT,
      git_branch TEXT, started_at REAL, ended_at REAL, n_turns INTEGER, model TEXT,
      title TEXT, first_user_prompt TEXT);
    CREATE TABLE IF NOT EXISTS chunks(
      id INTEGER PRIMARY KEY, session_id INTEGER, ts REAL, surface TEXT, text TEXT);
    CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
      text, content='chunks', content_rowid='id',
      tokenize="unicode61 tokenchars '-_./#'");
    CREATE VIRTUAL TABLE IF NOT EXISTS chunks_vocab USING fts5vocab(chunks_fts, 'row');
    CREATE TABLE IF NOT EXISTS entities(chunk_id INTEGER, kind TEXT, value TEXT);
    CREATE INDEX IF NOT EXISTS chunks_session_idx ON chunks(session_id);
    CREATE INDEX IF NOT EXISTS entities_chunk_idx ON entities(chunk_id);
    CREATE INDEX IF NOT EXISTS entities_value_idx ON entities(value);
    CREATE INDEX IF NOT EXISTS entities_value_lower_idx ON entities(lower(value));
    CREATE INDEX IF NOT EXISTS sessions_file_idx ON sessions(file_id);
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
    """)
    conn.executemany("INSERT OR REPLACE INTO meta(key,value) VALUES (?,?)", [
        ("schema_version", SCHEMA_VERSION), ("parser_version", str(PARSER_VERSION))])


def reset_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    DROP TABLE IF EXISTS chunks_fts; DROP TABLE IF EXISTS entities; DROP TABLE IF EXISTS chunks;
    DROP TABLE IF EXISTS sessions; DROP TABLE IF EXISTS files; DROP TABLE IF EXISTS meta;
    """)
    create_schema(conn)


def discover(root: Path, harness: str):
    if not root.exists():
        return []
    if harness == "codex":
        return sorted(p for p in root.rglob("rollout-*.jsonl") if p.is_file())
    return sorted(p for p in root.rglob("*.jsonl") if p.is_file())


def extract_entities(text: str, extra: list[tuple[str, str]] | None = None) -> list[tuple[str, str]]:
    """Return deterministic entities without binding projection semantics to SQLite."""
    found = set(extra or [])
    found.update(("file_path", x) for x in PATH_RE.findall(text))
    found.update(("pr", x) for x in re.findall(r"#\d{3,5}\b", text))
    found.update(("ticket", x) for x in re.findall(r"\bPAR-\d+\b", text))
    found.update(("url", x.rstrip(".,;)")) for x in URL_RE.findall(text))
    uuid_values = UUID_RE.findall(text)
    found.update(("uuid", x.lower()) for x in uuid_values)
    # Full UUIDs are also indexed by their conventional eight-hex short form.
    found.update(("uuid", x[:8].lower()) for x in uuid_values if "-" in x)
    found.update(("skill", x) for x in re.findall(r"Launching skill:\s*(\w[\w-]*)", text))
    found.update(("error", x) for x in re.findall(r"\b\w+(?:Error|Exception|Timeout)\b", text))
    return sorted(found)


def add_entities(conn: sqlite3.Connection, chunk_id: int, text: str, extra: list[tuple[str, str]]) -> None:
    conn.executemany("INSERT INTO entities(chunk_id,kind,value) VALUES (?,?,?)",
                     [(chunk_id, kind, value) for kind, value in extract_entities(text, extra)])


def content_text(value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(content_text(x.get("text", x.get("content", ""))) if isinstance(x, dict)
                         else str(x) for x in value)
    return "" if value is None else str(value)


def claude_record(data: dict) -> tuple[list[tuple[float | None, str, str, list[tuple[str, str]]]], dict]:
    out, meta = [], {}
    ts = epoch(data.get("timestamp"))
    for key in ("cwd", "gitBranch", "model"):
        if data.get(key) is not None:
            value = data[key]
            meta[{"gitBranch": "branch"}.get(key, key)] = clean_text(value) if isinstance(value, str) else value
    typ = data.get("type")
    message = data.get("message") or {}
    content = message.get("content", data.get("content", "")) if isinstance(message, dict) else data.get("content", "")
    if typ in ("user", "assistant") and isinstance(content, str):
        out.append((ts, typ, clean_text(content), []))
    elif isinstance(content, list):
        for block in content:
            if not isinstance(block, dict):
                continue
            kind = block.get("type")
            if kind == "thinking":
                continue
            if kind == "text":
                surface = "user" if typ == "user" else "assistant"
                out.append((ts, surface, clean_text(block.get("text", "")), []))
            elif kind == "tool_use":
                name = str(block.get("name", ""))
                out.append((ts, "tool_input", clipped(block.get("input", {}), MAX_TOOL_INPUT), [("tool", name)] if name else []))
            elif kind == "tool_result":
                out.append((ts, "tool_output", clipped(content_text(block.get("content", "")), MAX_TOOL_OUTPUT), []))
    return [(a, b, c, d) for a, b, c, d in out if c], meta


def codex_record(data: dict) -> tuple[list[tuple[float | None, str, str, list[tuple[str, str]]]], dict]:
    out, meta = [], {}
    ts = epoch(data.get("timestamp"))
    typ, payload = data.get("type"), data.get("payload") or {}
    if typ == "session_meta":
        for source, target in (("cwd", "cwd"), ("model", "model"), ("title", "title"),
                               ("first_user_prompt", "first_user_prompt"), ("git_branch", "branch")):
            if payload.get(source) is not None or data.get(source) is not None:
                value = payload.get(source, data.get(source))
                meta[target] = clean_text(value) if isinstance(value, str) else value
        return out, meta
    if typ == "event_msg" and isinstance(payload, dict):
        event_surface = {
            "user_message": "user",
            "agent_message": "assistant",
        }.get(payload.get("type"))
        message = payload.get("message")
        if event_surface and isinstance(message, str) and message:
            out.append((ts, event_surface, clean_text(message), []))
        return out, meta
    if typ != "response_item" or not isinstance(payload, dict):
        return out, meta
    role = payload.get("role")
    ptype = payload.get("type")
    message_surface = role if role in ("user", "assistant") else (
        "assistant" if ptype == "agent_message" else None
    )
    if message_surface:
        texts = []
        for block in payload.get("content", []) if isinstance(payload.get("content"), list) else []:
            if isinstance(block, dict) and block.get("type") in ("input_text", "output_text", "text"):
                texts.append(str(block.get("text", "")))
        if not texts and isinstance(payload.get("content"), str):
            texts = [payload["content"]]
        if texts:
            out.append((ts, message_surface, clean_text("\n".join(texts)), []))
    elif ptype in {"function_call", "custom_tool_call"}:
        name = str(payload.get("name", ""))
        tool_input = payload.get("arguments") if ptype == "function_call" else payload.get("input")
        out.append((ts, "tool_input", clipped(tool_input or "", MAX_TOOL_INPUT), [("tool", name)] if name else []))
    elif ptype in {"function_call_output", "custom_tool_call_output"}:
        out.append((ts, "tool_output", clipped(content_text(payload.get("output", "")), MAX_TOOL_OUTPUT), []))
    return [(a, b, c, d) for a, b, c, d in out if c], meta


def parse_file(path: Path, harness: str, offset: int = 0):
    chunks, meta = [], {}
    total = bad = complete_end = 0
    parser = claude_record if harness == "claude" else codex_record
    with path.open("rb") as fh:
        fh.seek(offset)
        complete_end = offset
        while True:
            line = fh.readline()
            if not line:
                break
            if not line.endswith(b"\n"):
                break
            complete_end = fh.tell()
            total += 1
            try:
                data = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                bad += 1
                continue
            try:
                records, update = parser(data)
            except (TypeError, ValueError):
                bad += 1
                continue
            chunks.extend(records)
            meta.update({k: v for k, v in update.items() if v not in (None, "")})
    return chunks, meta, complete_end, total, bad


def insert_chunks(conn, session_id: int, chunks) -> int:
    for ts, surface, text, extras in chunks:
        cur = conn.execute("INSERT INTO chunks(session_id,ts,surface,text) VALUES (?,?,?,?)",
                           (session_id, ts, surface, text))
        conn.execute("INSERT INTO chunks_fts(rowid,text) VALUES (?,?)", (cur.lastrowid, text))
        add_entities(conn, cur.lastrowid, text, extras)
    return len(chunks)


def delete_session_chunks(conn, session_id: int) -> None:
    conn.execute("DELETE FROM chunks_fts WHERE rowid IN (SELECT id FROM chunks WHERE session_id=?)", (session_id,))
    conn.execute("DELETE FROM entities WHERE chunk_id IN (SELECT id FROM chunks WHERE session_id=?)", (session_id,))
    conn.execute("DELETE FROM chunks WHERE session_id=?", (session_id,))


def ingest(args) -> int:
    claude_root, codex_root, db_path = paths()
    conn = connect(db_path)
    if args.rebuild:
        reset_schema(conn)
    else:
        try:
            existing = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        except sqlite3.OperationalError:
            existing = None
        if existing is not None and existing[0] != SCHEMA_VERSION:
            reset_schema(conn)
        else:
            create_schema(conn)
    summary = dict(seen=0, parsed=0, appended=0, tombstoned=0, errored=0, sessions=0, chunks=0)
    found: set[str] = set()
    started = time.monotonic()
    for harness, root in (("claude", claude_root), ("codex", codex_root)):
        for path in discover(root, harness):
            summary["seen"] += 1
            key = str(path.resolve())
            found.add(key)
            stat = path.stat()
            row = conn.execute("SELECT * FROM files WHERE path=?", (key,)).fetchone()
            current_fp = fingerprint(path, stat.st_size)
            mode = "new"
            if row and row["size"] == stat.st_size and row["mtime_ns"] == stat.st_mtime_ns and row["fingerprint"] == current_fp:
                if row["status"] == "tombstone":
                    conn.execute("UPDATE files SET status='ok' WHERE id=?", (row["id"],))
                continue
            if row and stat.st_size > row["size"]:
                # Verify the previously indexed prefix using its old size in the digest.
                mode = "append" if fingerprint(path, row["size"]) == row["fingerprint"] else "full"
            elif row:
                mode = "full"
            offset = int(row["parsed_offset"] or 0) if mode == "append" else 0
            parsed, meta, parsed_offset, total, bad = parse_file(path, harness, offset)
            status = "error" if total and bad / total > .5 else ("partial" if parsed_offset < stat.st_size else "ok")
            if status == "error": summary["errored"] += 1
            if row is None:
                cur = conn.execute("INSERT INTO files(path,harness,size,mtime_ns,fingerprint,parsed_offset,parser_version,status) VALUES (?,?,?,?,?,?,?,?)",
                    (key, harness, stat.st_size, stat.st_mtime_ns, current_fp, parsed_offset, PARSER_VERSION, status))
                file_id = cur.lastrowid
                cur = conn.execute("INSERT INTO sessions(file_id,harness,cwd,slot,git_branch,started_at,ended_at,n_turns,model,title,first_user_prompt) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (file_id, harness, None, None, None, None, None, 0, None, None, None))
                session_id = cur.lastrowid
            else:
                file_id = row["id"]
                session = conn.execute("SELECT id FROM sessions WHERE file_id=?", (file_id,)).fetchone()
                if session is None:
                    session_id = conn.execute("INSERT INTO sessions(file_id,harness,n_turns) VALUES (?,?,0)", (file_id, harness)).lastrowid
                else: session_id = session["id"]
                if mode == "full": delete_session_chunks(conn, session_id)
                conn.execute("UPDATE files SET harness=?,size=?,mtime_ns=?,fingerprint=?,parsed_offset=?,parser_version=?,status=? WHERE id=?",
                    (harness, stat.st_size, stat.st_mtime_ns, current_fp, parsed_offset, PARSER_VERSION, status, file_id))
            summary["chunks"] += insert_chunks(conn, session_id, parsed)
            old = conn.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
            timestamps = [x[0] for x in parsed if x[0] is not None]
            all_n = conn.execute("SELECT count(*) FROM chunks WHERE session_id=?", (session_id,)).fetchone()[0]
            if mode == "full":
                first_user = next((x[2] for x in parsed if x[1] == "user"), None)
                cwd, branch = meta.get("cwd"), meta.get("branch")
                started_at = min(timestamps) if timestamps else None
                ended_at = max(timestamps) if timestamps else None
                model, title = meta.get("model"), meta.get("title")
                meta_first, old_slot = meta.get("first_user_prompt", first_user), None
            else:
                first_user = next((x[2] for x in parsed if x[1] == "user"), old["first_user_prompt"])
                cwd, branch = meta.get("cwd", old["cwd"]), meta.get("branch", old["git_branch"])
                started_at = old["started_at"] if old["started_at"] is not None else (min(timestamps) if timestamps else None)
                ended_at = max([x for x in [old["ended_at"], *timestamps] if x is not None], default=None)
                model, title = meta.get("model", old["model"]), meta.get("title", old["title"])
                meta_first, old_slot = meta.get("first_user_prompt", first_user), old["slot"]
            slot_match = re.search(r"grep\d+", cwd or "")
            conn.execute("UPDATE sessions SET harness=?,cwd=?,slot=?,git_branch=?,started_at=?,ended_at=?,n_turns=?,model=?,title=?,first_user_prompt=? WHERE id=?",
                (harness, cwd, slot_match.group(0) if slot_match else old_slot, branch,
                 started_at, ended_at, all_n, model, title, meta_first, session_id))
            summary["sessions"] += 1
            summary["appended" if mode == "append" else "parsed"] += 1
    for row in conn.execute("SELECT id,path,status FROM files WHERE status != 'tombstone'"):
        if row["path"] not in found:
            conn.execute("UPDATE files SET status='tombstone' WHERE id=?", (row["id"],))
            summary["tombstoned"] += 1
    conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('last_ingest_at',?)", (str(time.time()),))
    conn.commit(); conn.close()
    print("files seen={seen} parsed={parsed} appended={appended} tombstoned={tombstoned} errored={errored}; sessions={sessions} chunks={chunks}; elapsed={elapsed:.3f}s".format(**summary, elapsed=time.monotonic()-started))
    return 0


def filters_sql(args):
    sql, params = ["f.status != 'tombstone'"], []
    if args.since: sql.append("c.ts >= ?"); params.append(iso(args.since))
    if args.until: sql.append("c.ts <= ?"); params.append(iso(args.until))
    if args.cwd: sql.append("s.cwd LIKE ?"); params.append("%" + args.cwd + "%")
    if args.branch: sql.append("s.git_branch LIKE ?"); params.append("%" + args.branch + "%")
    if args.harness: sql.append("s.harness = ?"); params.append(args.harness)
    return " AND ".join(sql), params


def query_terms(query: str) -> list[str]:
    # A sentence-final period is punctuation, not part of an identifier. Keep
    # interior dots for filenames, versions, hosts, and qualified names.
    return [cleaned for value in re.findall(r"[A-Za-z0-9_./#-]+", query)
            if (cleaned := value.rstrip("."))]


def informative_terms(query: str) -> list[str]:
    """Terms suitable for broad retrieval; phrases retain the original wording."""
    result = []
    for term in query_terms(query):
        lower = term.lower()
        if lower in STOPWORDS or (len(lower) <= 2 and not any(c.isdigit() for c in lower)):
            continue
        if lower not in result:
            result.append(lower)
    return result


def phrase_queries(query: str) -> list[str]:
    """Safe FTS phrases for the whole query, quotes, and error-like word runs."""
    phrases = []

    def add(value: str) -> None:
        normalized = " ".join(query_terms(value))
        if normalized and normalized not in phrases:
            phrases.append(normalized)

    add(query)
    for quoted in re.findall(r"[\"']([^\"']+)[\"']", query):
        add(quoted)
    words = re.findall(r"\S+", query)
    # Error strings often have a short natural-language frame plus punctuation
    # (for example, "TypeError: expected str got None").  Preserve those runs.
    for width in range(3, min(8, len(words)) + 1):
        for start in range(0, len(words) - width + 1):
            run = words[start:start + width]
            # Identifier punctuation alone should not fan a long question into
            # a dozen phrase probes; reserve additional phrases for error-ish
            # punctuation such as colons, parentheses, and quoted messages.
            if any(re.search(r"[^A-Za-z0-9_./#-]", word) for word in run):
                add(" ".join(run))
    # Natural-language queries for stack traces commonly wrap an otherwise
    # literal error in filler words.  Probe the compact error-bearing window.
    error_words = {"error", "exception", "timeout", "violation", "failed", "closed",
                   "connecttimeout", "greenlet_spawn", "notavailable", "notfound"}
    lowered = [re.sub(r"[^a-z0-9_]", "", word.lower()) for word in words]
    for index, word in enumerate(lowered):
        if word in error_words:
            tail = words[index:min(len(words), index + 6)]
            if len(tail) >= 3:
                add(" ".join(tail))
            head = words[max(0, index - 2):index + 1]
            if len(head) >= 3:
                add(" ".join(head))
    return phrases[:4]


def identifier_terms(terms: list[str]) -> list[str]:
    return [term for term in terms if (any(c.isdigit() for c in term) and re.search(r"[-_./#]", term))
            or re.fullmatch(r"[0-9a-f]{8,}", term)]


def vocab_doc_counts(conn, terms: list[str]) -> dict[str, int]:
    """Read FTS document frequencies, tolerating pre-vocab legacy indexes."""
    if not terms:
        return {}
    try:
        marks = ",".join("?" for _ in terms)
        return {row["term"]: row["doc"] for row in conn.execute(
            "SELECT term,doc FROM chunks_vocab WHERE term IN (" + marks + ")", terms)}
    except sqlite3.OperationalError:
        # Existing indexes predate the auxiliary vocabulary table.  FTS count
        # uses the posting list and remains cheap while preserving read-only
        # operation during migration.
        counts = {}
        for term in terms:
            try:
                counts[term] = conn.execute(
                    "SELECT count(*) FROM chunks_fts WHERE chunks_fts MATCH ?",
                    ('"' + term.replace('"', '""') + '"',)).fetchone()[0]
            except sqlite3.OperationalError:
                counts[term] = 0
        return counts


def search_rows(conn, args):
    terms = query_terms(args.query)
    informative = informative_terms(args.query)
    if not informative:
        return []
    where, params = filters_sql(args)
    doc_counts = vocab_doc_counts(conn, informative)
    signal_terms = [term for term in informative if doc_counts.get(term, 0) <= 100000]
    long_informative = [term for term in informative if len(term) >= 5]
    rare_terms = []
    match_count_sql = " + ".join(
        "CASE WHEN instr(lower(c.text), ?) > 0 THEN 1 ELSE 0 END" for _ in informative)
    long_match_count_sql = " + ".join(
        "CASE WHEN instr(lower(c.text), ?) > 0 THEN 1 ELSE 0 END" for _ in long_informative) or "0"
    rare_match_count_sql = " + ".join(
        "CASE WHEN instr(lower(c.text), ?) > 0 THEN 1 ELSE 0 END" for _ in rare_terms) or "0"
    common = """c.id,c.session_id,c.ts,c.surface,c.text,s.cwd,s.slot,s.git_branch,s.started_at,
                       f.path,{bm} AS bm,{matched} AS matched_count,{long_matched} AS long_matched_count,
                       {rare_matched} AS rare_matched_count
                FROM chunks c JOIN sessions s ON s.id=c.session_id JOIN files f ON f.id=s.file_id"""
    fts_base = "SELECT " + common.format(bm="bm25(chunks_fts)", matched=match_count_sql,
                                            long_matched=long_match_count_sql, rare_matched=rare_match_count_sql) + \
        " JOIN chunks_fts ON c.id=chunks_fts.rowid"
    direct_base = "SELECT DISTINCT " + common.format(bm="-25.0", matched=match_count_sql,
                                                        long_matched=long_match_count_sql, rare_matched=rare_match_count_sql) + \
        " JOIN entities e ON e.chunk_id=c.id"
    candidates: dict[int, tuple[sqlite3.Row, set[str]]] = {}

    def merge(rows, leg: str) -> None:
        for row in rows:
            existing = candidates.get(row["id"])
            if existing is None:
                candidates[row["id"]] = (row, {leg})
            else:
                best, legs = existing
                legs.add(leg)
                if float(row["bm"]) < float(best["bm"]):
                    candidates[row["id"]] = (row, legs)

    def fts(match: str, leg: str, limit: int = FTS_LEG_LIMIT) -> None:
        sql = fts_base + " WHERE chunks_fts MATCH ? AND " + where + f" ORDER BY bm25(chunks_fts) LIMIT {limit}"
        merge(conn.execute(sql, [*informative, *long_informative, *rare_terms, match, *params]).fetchall(), leg)

    try:
        # Leg A: exact titles, quoted text, and error strings as complete phrases.
        for phrase in phrase_queries(args.query):
            fts('"' + phrase.replace('"', '""') + '"', "A")

        # Leg B: entity retrieval does not depend on the FTS rank cutoff.
        identifiers = identifier_terms(informative)
        if identifiers:
            entity_parts, entity_params = [], []
            for term in identifiers:
                # Entity values for identifiers are normalized at ingest; using
                # the ordinary value index keeps this direct leg sub-millisecond.
                entity_parts.append("e.value=?")
                entity_params.append(term)
                if re.fullmatch(r"[0-9a-f]{8,}", term):
                    # Range scan instead of LIKE: LIKE cannot use the value
                    # index (case-insensitive semantics) and costs ~0.5s on a
                    # multi-million-row entities table.
                    entity_parts.append("(e.value >= ? AND e.value < ?)")
                    entity_params.extend([term, term + "￿"])
            merge(conn.execute(direct_base + " WHERE (" + " OR ".join(entity_parts) + ") AND " + where +
                               f" LIMIT {FTS_LEG_LIMIT}", [*informative, *long_informative, *rare_terms, *entity_params, *params]).fetchall(), "B")

            # Identifier tokens also live in arbitrary tool output and paths,
            # where no entity kind is appropriate.  Retrieve the raw FTS token.
            for term in identifiers:
                fts('"' + term.replace('"', '""') + '"', "I", limit=100)

        # Leg C: all informative words; this is the strongest broad retrieval leg.
        fts(" AND ".join('"' + term.replace('"', '""') + '"' for term in informative), "C")
        # Leg D: only broaden if the precise legs did not produce enough chunks.
        if len(candidates) < 3 * args.limit:
            or_terms = signal_terms
            if or_terms:
                fts(" OR ".join('"' + term.replace('"', '""') + '"' for term in or_terms), "D")
    except sqlite3.OperationalError:
        likes = " OR ".join("lower(c.text) LIKE ?" for _ in informative)
        fallback = "SELECT " + common.format(bm="0.0", matched=match_count_sql,
                                                 long_matched=long_match_count_sql, rare_matched=rare_match_count_sql) + \
            " WHERE (" + likes + ") AND " + where + f" ORDER BY c.ts DESC LIMIT {FTS_LEG_LIMIT}"
        merge(conn.execute(fallback, [*informative, *long_informative, *rare_terms,
                                      *("%" + term + "%" for term in informative), *params]).fetchall(), "D")
    now = time.time()
    result = []
    for row, legs in candidates.values():
        raw = max(0.01, -float(row["bm"]) + 1.0)
        raw *= {"user": 4.0, "assistant": 2.0, "tool_input": 1.5, "tool_output": 1.0}[row["surface"]]
        score = raw + (10.0 if {"A", "B", "I"} & legs else 0.0)
        score *= 1 / (1 + max(0, now - (row["ts"] or row["started_at"] or now)) / 86400 / 180)
        matched = [] if args.paths else [x for x in terms if x.lower() in row["text"].lower()]
        result.append((row, score, matched, legs, len(informative), bool(rare_terms)))
    return result


def search(args) -> int:
    if recall_mode() == "local" and any(
        getattr(args, key, None) is not None
        for key in ("source_id", "source_family", "source_alias")
    ):
        print("source routing requires remote Recall mode", file=sys.stderr)
        return 2
    _, _, db = paths()
    if not db.exists():
        if not args.paths:
            print("Recall index does not exist yet.", file=sys.stderr)
            print("Answer now by scanning transcripts directly, e.g.:", file=sys.stderr)
            print('  rg -l -i "<terms>" ~/.claude/projects ~/.codex/sessions', file=sys.stderr)
            print("and start the index in the background: setsid nohup python3 scripts/recall.py index &", file=sys.stderr)
        return 0
    conn = connect_ro(db)

    def chunk_tier(legs: set[str]) -> int:
        # Exact evidence (phrase / entity / identifier-token) outranks the
        # AND leg, which outranks OR-only matches — as a TIER, not a score
        # bonus, so broad common-word chunks can never bury an exact hit.
        if {"A", "B", "I"} & legs:
            return 2
        return 1 if "C" in legs else 0

    grouped = {}
    for row, score, matched, legs, informative_count, _has_rare_anchor in search_rows(conn, args):
        tier = chunk_tier(legs)
        item = grouped.setdefault(row["session_id"],
                                  {"row": row, "best": (score, matched, legs, informative_count),
                                   "tier": tier, "count": 0})
        item["count"] += 1
        if (tier, score) > (item["tier"], item["best"][0]):
            item["row"], item["best"], item["tier"] = row, (score, matched, legs, informative_count), tier
    ranked = []
    for item in grouped.values():
        score = item["best"][0] + .2 * math.log(1 + item["count"])
        row, tier = item["row"], item["tier"]
        if tier == 0 and row["long_matched_count"] < 2:
            # OR-only sessions need at least two substantive term matches in
            # one chunk; distinguishing a loose-but-real match from a query
            # about work that never happened is a semantic call beyond a
            # lexical engine — exact/AND evidence always outranks these.
            continue
        ranked.append(((tier, score), item))
    ranked.sort(key=lambda x: x[0], reverse=True)
    for rank, (_, item) in enumerate(ranked[:args.limit], 1):
        row = item["row"]
        if args.paths:
            print(row["path"]); continue
        date = datetime.fromtimestamp(row["ts"] or row["started_at"] or 0, timezone.utc).isoformat()
        snippet = re.sub(r"\s+", " ", row["text"])[:200]
        why = "terms=" + ",".join(item["best"][1]) + "; legs=" + ",".join(sorted(item["best"][2]))
        print(f"{rank}. {row['path']}\n   {date} cwd={row['cwd'] or '-'} slot={row['slot'] or '-'} branch={row['git_branch'] or '-'}\n   [{row['surface']}] {snippet}\n   WHY: {why}")
    conn.close(); return 0


def direct_chunks(path: Path):
    harness = "codex" if path.name.startswith("rollout-") else "claude"
    return parse_file(path, harness)[0]


def session_evidence_id(source_id: str, session_id: str, event_native_id: str,
                        ordinal: int, text: str) -> tuple[str, str]:
    text_sha = hashlib.sha256(text.encode()).hexdigest()
    identity = f"{source_id}\0{session_id}\0{event_native_id}\0{ordinal}\0{text_sha}"
    return "rse_" + hashlib.sha256(identity.encode()).hexdigest(), text_sha


def resolve_current_session() -> Path:
    claude, codex, _ = paths()
    thread_id = os.environ.get("CODEX_THREAD_ID")
    claude_ids = {
        value for value in (
            os.environ.get("CLAUDE_SESSION_ID"),
            os.environ.get("CLAUDE_CODE_SESSION_ID"),
        ) if value
    }
    if len(claude_ids) > 1:
        raise ValueError("current Claude identity is ambiguous; pass --target explicitly")
    claude_id = next(iter(claude_ids), None)
    claude_active = (
        os.environ.get("CLAUDECODE") == "1"
        or bool(os.environ.get("CLAUDE_CODE_ENTRYPOINT"))
    )
    if claude_active:
        if not claude_id:
            raise ValueError(
                "current Claude identity unavailable; inherited Codex identity was ignored; "
                "pass --target explicitly"
            )
        matches = [path for path in claude.rglob(f"*{claude_id}*.jsonl") if path.is_file()]
        if len(matches) != 1:
            raise ValueError(f"current Claude identity resolved to {len(matches)} sessions")
        return matches[0].resolve()
    if thread_id and claude_id:
        raise ValueError("current harness identity is ambiguous; pass --target explicitly")
    if thread_id:
        matches = [path for path in codex.rglob(f"*{thread_id}*.jsonl") if path.name.startswith("rollout-")]
        if len(matches) != 1:
            raise ValueError(f"current Codex identity resolved to {len(matches)} sessions")
        return matches[0].resolve()
    if claude_id:
        matches = [path for path in claude.rglob(f"*{claude_id}*.jsonl") if path.is_file()]
        if len(matches) != 1:
            raise ValueError(f"current Claude identity resolved to {len(matches)} sessions")
        return matches[0].resolve()
    candidates = sorted(
        (path for path in claude.rglob("*.jsonl") if path.is_file()),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )[:5]
    receipts = [
        hashlib.sha256(str(path.resolve()).encode()).hexdigest()[:16] + "@" + str(path.stat().st_mtime_ns)
        for path in candidates
    ]
    raise ValueError(
        "current session identity unavailable; pass --target explicitly"
        + ("; ranked_candidate_receipts=" + ",".join(receipts) if receipts else "")
    )


def _native_session_metadata(path: Path, harness: str) -> dict | None:
    """Read only bounded native identity/relationship metadata from one transcript."""
    limit = 1024 * 1024
    consumed = 0
    with path.open("rb") as source:
        for _ in range(128):
            line = source.readline(limit - consumed + 1)
            consumed += len(line)
            if consumed > limit:
                break
            try:
                record = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(record, dict):
                continue
            if harness == "codex" and record.get("type") == "session_meta":
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    return None
                source_meta = payload.get("source")
                spawn = None
                if isinstance(source_meta, dict):
                    candidate = source_meta.get("subagent")
                    if isinstance(candidate, dict):
                        spawn = candidate.get("thread_spawn")
                parent = payload.get("parent_thread_id")
                if not isinstance(parent, str) and isinstance(spawn, dict):
                    parent = spawn.get("parent_thread_id")
                forked = payload.get("forked_from_id")
                node_id = payload.get("id")
                unsafe_relation = any(
                    isinstance(value, str) and clean_text(value) != value
                    for value in (node_id, parent, forked)
                )
                if unsafe_relation:
                    return {
                        "node_id": None, "harness": harness, "path": str(path.resolve()),
                        "parent_id": None, "forked_from_id": None,
                        "relationship_error": "unsafe_native_identity",
                    }
                if not isinstance(node_id, str):
                    if isinstance(parent, str) or isinstance(forked, str):
                        return {
                            "node_id": None, "harness": harness, "path": str(path.resolve()),
                            "parent_id": parent if isinstance(parent, str) else None,
                            "forked_from_id": forked if isinstance(forked, str) else None,
                            "relationship_error": "missing_native_id",
                        }
                    return None
                return {
                    "node_id": node_id,
                    "harness": harness,
                    "path": str(path.resolve()),
                    "kind": "child" if isinstance(parent, str) else "main",
                    "parent_id": parent if isinstance(parent, str) else None,
                    "forked_from_id": forked if isinstance(forked, str) else None,
                }
            if harness == "claude":
                session_id = record.get("sessionId")
                agent_id = record.get("agentId")
                if not isinstance(session_id, str):
                    continue
                forked_id = record.get("forkedFromSessionId")
                if any(
                    isinstance(value, str) and clean_text(value) != value
                    for value in (session_id, agent_id, forked_id)
                ):
                    return {
                        "node_id": None, "harness": harness, "path": str(path.resolve()),
                        "parent_id": None, "forked_from_id": None,
                        "relationship_error": "unsafe_native_identity",
                    }
                is_child = bool(record.get("isSidechain")) or isinstance(agent_id, str) or "subagents" in path.parts
                if is_child and not isinstance(agent_id, str):
                    return {
                        "node_id": None, "harness": harness, "path": str(path.resolve()),
                        "parent_id": session_id, "forked_from_id": None,
                        "relationship_error": "missing_agent_id",
                    }
                return {
                    "node_id": agent_id if is_child and isinstance(agent_id, str) else session_id,
                    "harness": harness,
                    "path": str(path.resolve()),
                    "kind": "child" if is_child else "main",
                    "parent_id": session_id if is_child else None,
                    "forked_from_id": forked_id if isinstance(forked_id, str) else None,
                }
    return None


def _native_session_graph() -> tuple[list[dict], dict[str, list[dict]], list[dict]]:
    claude, codex, _ = paths()
    nodes, unresolved = [], []
    for root, harness in ((claude, "claude"), (codex, "codex")):
        if not root.exists():
            continue
        for path in discover(root, harness):
            try:
                node = _native_session_metadata(path, harness)
            except OSError:
                node = None
            if node is not None:
                (nodes if isinstance(node.get("node_id"), str) else unresolved).append(node)
    by_id: dict[str, list[dict]] = {}
    for node in nodes:
        by_id.setdefault(node["node_id"], []).append(node)
    return nodes, by_id, unresolved


def local_session_relations(args) -> int:
    if recall_mode() == "remote":
        raise ValueError("session-relations requires local native transcript metadata")
    target = resolve_current_session() if args.current else Path(args.target).expanduser().resolve()
    if not target.is_file():
        raise ValueError("session target does not exist")
    harness = "codex" if target.name.startswith("rollout-") else "claude"
    export_root_for(target, harness)
    nodes, by_id, unresolved = _native_session_graph()
    selected_matches = [node for node in nodes if Path(node["path"]) == target]
    if len(selected_matches) != 1:
        raise ValueError(f"session relationship identity resolved to {len(selected_matches)} nodes")
    selected = selected_matches[0]
    if len(by_id.get(selected["node_id"], [])) != 1:
        raise ValueError("selected native session identity is ambiguous")

    included = {selected["node_id"]}
    reasons: dict[str, set[str]] = {selected["node_id"]: {"selected"}}
    depth: dict[str, int] = {selected["node_id"]: 0}
    incomplete = []

    if args.chain:
        queue = [selected["node_id"]]
        while queue:
            node_id = queue.pop(0)
            node = by_id[node_id][0]
            for candidate in (item for item in unresolved if item.get("forked_from_id") == node_id):
                incomplete.append({
                    "type": "continuation", "from": node_id, "to": None,
                    "reason": candidate.get("relationship_error", "incomplete_native_metadata"),
                    "path_receipt": hashlib.sha256(candidate["path"].encode()).hexdigest()[:16],
                })
            neighbors = []
            if node.get("forked_from_id"):
                neighbors.append(node["forked_from_id"])
            neighbors.extend(
                candidate["node_id"] for candidate in nodes
                if candidate.get("forked_from_id") == node_id
            )
            for neighbor in neighbors:
                matches = by_id.get(neighbor, [])
                if len(matches) != 1:
                    incomplete.append({"type": "continuation", "from": node_id, "to": neighbor,
                                       "reason": "missing" if not matches else "ambiguous"})
                    continue
                if neighbor not in included:
                    included.add(neighbor)
                    reasons[neighbor] = {"continuation"}
                    depth[neighbor] = depth[node_id] + 1
                    queue.append(neighbor)
                else:
                    reasons[neighbor].add("continuation")

    if args.include_children:
        queue = list(included)
        visited = set()
        while queue:
            parent_id = queue.pop(0)
            if parent_id in visited:
                continue
            visited.add(parent_id)
            for candidate in (item for item in unresolved if item.get("parent_id") == parent_id):
                incomplete.append({
                    "type": "child", "from": parent_id, "to": None,
                    "reason": candidate.get("relationship_error", "incomplete_native_metadata"),
                    "path_receipt": hashlib.sha256(candidate["path"].encode()).hexdigest()[:16],
                })
            for child in (node for node in nodes if node.get("parent_id") == parent_id):
                child_id = child["node_id"]
                matches = by_id.get(child_id, [])
                if len(matches) != 1:
                    incomplete.append({"type": "child", "from": parent_id, "to": child_id,
                                       "reason": "ambiguous"})
                    continue
                if child_id not in included:
                    included.add(child_id)
                    reasons[child_id] = {"child"}
                    depth[child_id] = depth[parent_id] + 1
                else:
                    reasons[child_id].add("child")
                queue.append(child_id)

    selected_nodes = []
    for node_id in sorted(included, key=lambda value: (depth.get(value, 0), value)):
        node = dict(by_id[node_id][0])
        node["selection_reasons"] = sorted(reasons[node_id])
        node["graph_depth"] = depth.get(node_id, 0)
        selected_nodes.append(node)
    edges = []
    for node in selected_nodes:
        if node.get("parent_id") in included:
            edges.append({"type": "child", "from": node["parent_id"], "to": node["node_id"]})
        if node.get("forked_from_id") in included:
            edges.append({"type": "continuation", "from": node["forked_from_id"], "to": node["node_id"]})
    result = {
        "schema_version": "recall.session-relations.v1",
        "selected_node_id": selected["node_id"],
        "requested": {"include_children": bool(args.include_children), "chain": bool(args.chain)},
        "graph_complete": not incomplete,
        "incomplete_relations": incomplete,
        "nodes": selected_nodes,
        "edges": sorted(edges, key=lambda edge: (edge["type"], edge["from"], edge["to"])),
    }
    print(json.dumps(result, sort_keys=True))
    return 0 if result["graph_complete"] else 2


def export_cursor_connection() -> sqlite3.Connection:
    configured = Path(os.environ.get(
        "RECALL_SESSION_CURSOR_DB", Path.home() / ".recall/session-export-cursors.db",
    )).expanduser()
    configured.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if configured.parent.stat().st_mode & 0o077:
        raise ValueError("session export cursor directory must have mode 0700")
    if configured.is_symlink():
        raise ValueError("session export cursor database must not be a symlink")
    if not configured.exists():
        descriptor = os.open(configured, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
    elif configured.stat().st_mode & 0o077:
        raise ValueError("session export cursor database must have mode 0600")
    connection = sqlite3.connect(configured)
    connection.row_factory = sqlite3.Row
    connection.execute("""
        CREATE TABLE IF NOT EXISTS cursors(
          token_sha256 TEXT PRIMARY KEY,path TEXT NOT NULL,root TEXT NOT NULL,
          harness TEXT NOT NULL,source_id TEXT NOT NULL,session_id TEXT NOT NULL,
          snapshot_size INTEGER NOT NULL,snapshot_fingerprint TEXT NOT NULL,
          byte_offset INTEGER NOT NULL,item_skip INTEGER NOT NULL,sequence INTEGER NOT NULL,
          metadata_json TEXT NOT NULL,created_at REAL NOT NULL,expires_at REAL NOT NULL)
    """)
    connection.execute("DELETE FROM cursors WHERE expires_at<=?", (time.time(),))
    connection.commit()
    return connection


def export_root_for(path: Path, harness: str) -> Path:
    claude, codex, _ = paths()
    root = (codex if harness == "codex" else claude).resolve()
    try:
        path.resolve().relative_to(root)
    except ValueError:
        raise ValueError("session target is outside the configured harness root") from None
    return root


def session_file_key(path: Path, root: Path, harness: str) -> str:
    relative = str(path.resolve().relative_to(root.resolve()))
    return hashlib.sha256((harness + "\x1f" + relative).encode()).hexdigest()[:24]


def save_local_export_cursor(connection: sqlite3.Connection, state: dict) -> str:
    token = "rsl_" + secrets.token_urlsafe(32)
    connection.execute(
        """INSERT INTO cursors(token_sha256,path,root,harness,source_id,session_id,
             snapshot_size,snapshot_fingerprint,byte_offset,item_skip,sequence,metadata_json,
             created_at,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (hashlib.sha256(token.encode()).hexdigest(), state["path"], state["root"], state["harness"],
         state["source_id"], state["session_id"], state["snapshot_size"],
         state["snapshot_fingerprint"], state["byte_offset"], state["item_skip"], state["sequence"],
         json.dumps(state["metadata"], sort_keys=True), time.time(), time.time() + 3600),
    )
    connection.commit()
    return token


def local_session_export(args) -> int:
    connection = export_cursor_connection()
    try:
        if args.cursor:
            if not re.fullmatch(r"rsl_[A-Za-z0-9_-]{32,128}", args.cursor):
                raise ValueError("invalid local session export cursor")
            row = connection.execute(
                "SELECT * FROM cursors WHERE token_sha256=? AND expires_at>?",
                (hashlib.sha256(args.cursor.encode()).hexdigest(), time.time()),
            ).fetchone()
            if not row:
                raise ValueError("local session export cursor not found or expired")
            state = dict(row)
            state["metadata"] = json.loads(state.pop("metadata_json"))
        else:
            path = resolve_current_session() if args.current else Path(args.target).expanduser().resolve()
            if not path.is_file():
                raise ValueError("session target does not exist")
            harness = "codex" if path.name.startswith("rollout-") else "claude"
            root = export_root_for(path, harness)
            file_key = session_file_key(path, root, harness)
            session_id = f"{harness}-session-{file_key}"
            source_id = os.environ.get("RECALL_EXPORT_SOURCE_ID") or os.environ.get("RECALL_SOURCE_ID") or f"local:{harness}"
            stat = path.stat()
            state = {
                "path": str(path), "root": str(root), "harness": harness,
                "source_id": source_id, "session_id": session_id,
                "snapshot_size": stat.st_size, "snapshot_fingerprint": fingerprint(path, stat.st_size),
                "byte_offset": 0, "item_skip": 0, "sequence": 0,
                "metadata": {"harness": harness, "original_path": clean_text(str(path))},
            }

        path = Path(state["path"])
        if not path.is_file():
            raise ValueError("session source disappeared after cursor creation")
        parser_function = claude_record if state["harness"] == "claude" else codex_record
        returned = []
        next_byte = int(state["byte_offset"])
        next_skip = int(state["item_skip"])
        partial_record = False
        found_extra = False
        with path.open("rb") as source:
            source.seek(next_byte)
            while source.tell() < state["snapshot_size"]:
                line_start = source.tell()
                line = source.readline(state["snapshot_size"] - line_start)
                if not line.endswith(b"\n"):
                    partial_record = True
                    next_byte = line_start
                    break
                line_end = source.tell()
                try:
                    content = json.loads(line)
                    if not isinstance(content, dict):
                        raise ValueError
                except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
                    next_byte, next_skip = line_end, 0
                    continue
                parsed, metadata = parser_function(content)
                for key, value in metadata.items():
                    if value is not None:
                        state["metadata"][key] = clean_text(str(value))
                native_id = f"{session_file_key(path, Path(state['root']), state['harness'])}-{line_start:016x}"
                start_index = next_skip if line_start == state["byte_offset"] else 0
                for item_index, (occurred_at, surface, text, entities) in enumerate(parsed):
                    if item_index < start_index:
                        continue
                    evidence_id, text_sha = session_evidence_id(
                        state["source_id"], state["session_id"], native_id, item_index, text,
                    )
                    item = {
                        "sequence": state["sequence"] + len(returned),
                        "evidence_id": evidence_id,
                        "event_native_id": native_id,
                        "item_ordinal": item_index,
                        "occurred_at": occurred_at,
                        "role": surface,
                        "surface": surface,
                        "text": text,
                        "text_sha256": text_sha,
                        "receipt": None,
                        "projector_version": PARSER_VERSION,
                    }
                    safe_entities = [
                        {"kind": kind, "value": clean_text(str(value))}
                        for kind, value in entities
                    ]
                    if safe_entities:
                        item["entities"] = safe_entities
                    if surface in {"tool_input", "tool_output"} and len(text) >= (
                        MAX_TOOL_INPUT if surface == "tool_input" else MAX_TOOL_OUTPUT
                    ):
                        item["possibly_truncated"] = True
                    if len(returned) == args.limit:
                        next_byte, next_skip, found_extra = line_start, item_index, True
                        break
                    returned.append(item)
                if found_extra:
                    break
                next_byte, next_skip = line_end, 0

        current_stable = (
            path.stat().st_size == state["snapshot_size"]
            and fingerprint(path, state["snapshot_size"]) == state["snapshot_fingerprint"]
        )
        next_cursor = None
        if found_extra:
            next_state = {**state, "byte_offset": next_byte, "item_skip": next_skip,
                          "sequence": state["sequence"] + len(returned)}
            next_cursor = save_local_export_cursor(connection, next_state)
        boundary = hashlib.sha256(
            f"{state['source_id']}\0{state['session_id']}\0{state['snapshot_fingerprint']}".encode()
        ).hexdigest()
        result = {
            "schema_version": "recall.session-export.v1",
            "session": {
                "source_id": state["source_id"],
                "native_session_id": state["session_id"],
                "harness": state["harness"],
                "started_at": state["metadata"].get("started_at"),
                "ended_at": state["metadata"].get("ended_at"),
                "metadata": state["metadata"],
                "projector_version": PARSER_VERSION,
                "privacy_policy_version": "local-clean-text-v1",
                "boundary_receipt": boundary,
                "children_included": False,
                "source_snapshot_stable": current_stable,
                "source_partial_record": partial_record,
            },
            "items": returned,
            "page": {
                "count": len(returned),
                "complete": not found_extra,
                "next_cursor": next_cursor,
                "page_receipt": hashlib.sha256(
                    "\n".join(item["evidence_id"] for item in returned).encode()
                ).hexdigest(),
                "snapshot_size": state["snapshot_size"],
            },
        }
        print(json.dumps(result, sort_keys=True))
        return 0
    finally:
        connection.close()


def show(args) -> int:
    _, _, db = paths(); target = Path(args.target).expanduser()
    if not target.exists() and args.target.isdigit() and db.exists():
        conn = connect_ro(db); row = conn.execute("SELECT f.path FROM sessions s JOIN files f ON f.id=s.file_id WHERE s.id=?", (int(args.target),)).fetchone(); conn.close()
        target = Path(row[0]) if row else target
    if not target.exists():
        print("session not found", file=sys.stderr); return 1
    chunks = direct_chunks(target)
    if args.tail:
        chunks = chunks[-args.tail:]
    if args.around:
        point = iso(args.around); closest = min(range(len(chunks)), key=lambda i: abs((chunks[i][0] or 0)-point), default=0)
        chunks = chunks[max(0, closest-3):closest+4]
    for ts, surface, text, _ in chunks:
        if args.prompts and surface != "user": continue
        stamp = datetime.fromtimestamp(ts, timezone.utc).isoformat() if ts else "-"
        print(f"[{stamp}] {surface}: {text}")
    return 0


def related(args) -> int:
    _, _, db = paths()
    if not db.exists(): return 0
    conn = connect_ro(db)
    cwd = args.cwd or str(Path.cwd())
    branch = args.branch
    where, par = [], []
    if cwd: where.append("s.cwd LIKE ?"); par.append("%"+cwd+"%")
    if branch: where.append("s.git_branch LIKE ?"); par.append("%"+branch+"%")
    n_ctx, n_vals = (10, 100) if args.fast else (20, 300)
    contexts = conn.execute("SELECT id FROM sessions s WHERE " + (" OR ".join(where) if where else "0") + f" ORDER BY s.ended_at DESC LIMIT {n_ctx}", par).fetchall()
    context_ids = [r[0] for r in contexts]
    values: list[str] = []
    if context_ids:
        marks = ",".join("?" * len(context_ids))
        values = [r[0] for r in conn.execute("SELECT DISTINCT e.value FROM entities e JOIN chunks c ON c.id=e.chunk_id WHERE c.session_id IN ("+marks+f") AND e.kind='file_path' LIMIT {n_vals}", context_ids)]
    # Invert the overlap lookup: one indexed pass over the context's file-path
    # values finds sharing sessions directly — never a per-candidate scan.
    overlap_counts: dict[int, int] = {}
    for start in range(0, len(values), 300):
        batch = values[start:start + 300]
        marks = ",".join("?" * len(batch))
        for sid, n in conn.execute("SELECT c.session_id, count(DISTINCT e.value) FROM entities e JOIN chunks c ON c.id=e.chunk_id WHERE e.kind='file_path' AND e.value IN ("+marks+") GROUP BY c.session_id", batch):
            overlap_counts[sid] = overlap_counts.get(sid, 0) + n
    candidates = conn.execute("SELECT s.id,f.path,s.cwd,s.git_branch,s.ended_at FROM sessions s JOIN files f ON f.id=s.file_id WHERE f.status != 'tombstone' ORDER BY s.ended_at DESC LIMIT 500").fetchall()
    ranked = []
    for row in candidates:
        if args.mains_only and "/subagents/" in row["path"]:
            continue
        overlap = int(bool(cwd and cwd in (row["cwd"] or ""))) + int(bool(branch and branch == row["git_branch"]))
        overlap += overlap_counts.get(row["id"], 0)
        if overlap:
            recency = 1 / (1 + max(0, time.time()-(row["ended_at"] or 0))/86400/180)
            ranked.append((overlap + recency, row, overlap))
    for _, row, overlap in sorted(ranked, reverse=True, key=lambda x: x[0])[:args.limit]:
        print(f"{row['path']}\toverlap={overlap}\tcwd={row['cwd'] or '-'}\tbranch={row['git_branch'] or '-'}")
    conn.close(); return 0


def doctor(args) -> int:
    mode = recall_mode()
    if mode == "local":
        print("OK mode=local — central Recall Brain available as an upgrade; see references/central-brain.md")
    else:
        print(f"OK mode={mode}")
    claude, codex, db = paths()
    conn = None
    try:
        probe = sqlite3.connect(":memory:")
        probe.execute("CREATE VIRTUAL TABLE _fts_test USING fts5(x)")
        probe.close()
        print("OK FTS5 available")
    except sqlite3.DatabaseError as exc:
        print(f"HARD FAIL FTS5/database: {exc}"); return 1
    if db.exists():
        try:
            conn = connect_ro(db)
            schema = (conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone() or ["missing"])[0]
            print(f"OK db exists=True size={db.stat().st_size} schema_version={schema}")
            last = (conn.execute("SELECT value FROM meta WHERE key='last_ingest_at'").fetchone() or [None])[0]
            print("OK index age=" + (f"{(time.time()-float(last))/3600:.1f}h" if last else "unknown"))
            counts = dict(conn.execute("SELECT status,count(*) FROM files GROUP BY status").fetchall())
            print(f"OK files errored={counts.get('error',0)} partial={counts.get('partial',0)}")
            ledger = {r[0] for r in conn.execute("SELECT path FROM files WHERE status!='tombstone'")}
        except sqlite3.OperationalError:
            print("WARN index empty/not built metadata unavailable")
            print("WARN index empty/not built file ledger unavailable")
            ledger = set()
        except sqlite3.DatabaseError as exc:
            print(f"HARD FAIL db corrupt: {exc}")
            return 1
    else:
        print("WARN db exists=False size=0 schema_version=missing")
        print("WARN index empty/not built metadata unavailable")
        print("WARN index empty/not built file ledger unavailable")
        ledger = set()
    for name, root, harness in (("claude", claude, "claude"), ("codex", codex, "codex")):
        disk = {str(x.resolve()) for x in discover(root, harness)} if root.exists() else set()
        coverage = 100 * len(disk & ledger) / len(disk) if disk else 100
        print(f"{'OK' if root.exists() else 'WARN'} {name} root={root} files={len(disk)} coverage={coverage:.1f}%")
    disk_parent = db.parent
    while not disk_parent.exists() and disk_parent != disk_parent.parent:
        disk_parent = disk_parent.parent
    stat = os.statvfs(disk_parent); free = stat.f_bavail * stat.f_frsize
    print(f"{'WARN' if free < 20*1024**3 else 'OK'} free_disk_gb={free/1024**3:.1f}")
    settings = Path.home()/".claude/settings.json"; days = None
    try: days = json.loads(settings.read_text()).get("cleanupPeriodDays")
    except (OSError, json.JSONDecodeError): pass
    print(f"{'OK' if isinstance(days,(int,float)) and days >= 3650 else 'WARN'} cleanupPeriodDays={days}")
    manifests_dir = Path.home()/"archives/manifests"
    manifests = list(manifests_dir.glob("*.json")) if manifests_dir.exists() else []
    age = (time.time()-max(p.stat().st_mtime for p in manifests))/3600 if manifests else None
    print(f"{'WARN' if age is None or age > 48 else 'OK'} archives_manifest_age_hours={age:.1f}" if age is not None else "WARN archives_manifest_age_hours=missing")
    if conn is not None: conn.close()
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="recall")
    sub = ap.add_subparsers(dest="command", required=True)
    p = sub.add_parser("index")
    p.add_argument("--rebuild", action="store_true")
    p.add_argument(
        "--allow-local-index",
        action="store_true",
        help="explicitly operate the disposable SQLite fallback even when a central profile exists",
    )
    p.set_defaults(func=ingest)
    p = sub.add_parser("search"); p.add_argument("query"); p.add_argument("--since"); p.add_argument("--until"); p.add_argument("--cwd"); p.add_argument("--branch"); p.add_argument("--harness", choices=("claude","codex")); p.add_argument("--source-id"); p.add_argument("--source-family", choices=("coding_history","deliberate_capture","user_export","third_party_research","communications","schedule","contacts","social","documents","work_activity","local_activity","personal_media")); p.add_argument("--source-alias"); p.add_argument("--limit", type=int, default=10); p.add_argument("--paths", action="store_true"); p.set_defaults(func=search)
    p = sub.add_parser("show"); p.add_argument("target"); p.add_argument("--around"); p.add_argument("--prompts", action="store_true"); p.add_argument("--tail", type=int, default=0, help="print only the last N chunks"); p.set_defaults(func=show)
    p = sub.add_parser("session-export", help="page one exact redacted session snapshot as JSON")
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--current", action="store_true")
    target.add_argument("--target")
    target.add_argument("--cursor")
    p.add_argument("--limit", type=int, default=1000)
    p.set_defaults(func=local_session_export)
    p = sub.add_parser("session-relations", help="resolve exact local child and continuation boundaries")
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--current", action="store_true")
    target.add_argument("--target")
    p.add_argument("--include-children", action="store_true")
    p.add_argument("--chain", action="store_true")
    p.set_defaults(func=local_session_relations)
    p = sub.add_parser("related"); p.add_argument("--cwd"); p.add_argument("--branch"); p.add_argument("--limit", type=int, default=10); p.add_argument("--mains-only", action="store_true", help="exclude subagent transcripts"); p.add_argument("--fast", action="store_true", help="tight caps for the session-start hook budget"); p.set_defaults(func=related)
    p = sub.add_parser("doctor"); p.set_defaults(func=doctor)
    p = sub.add_parser("put"); p.add_argument("text", nargs="?"); p.add_argument("--source-id"); p.add_argument("--principal-id"); p.add_argument("--visibility", choices=("private", "shared")); p.add_argument("--provenance-uri", default="manual://recall_put")
    p = sub.add_parser("delete"); p.add_argument("receipt"); p.add_argument("--source-id"); p.add_argument("--principal-id"); p.add_argument("--visibility", choices=("private", "shared"))
    args = ap.parse_args(argv)
    try: return run_transport(args)
    except ValueError as exc: ap.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
