from __future__ import annotations

import base64
import json
import re
import shlex
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Protocol

from .agent_scan import AGENT_SCAN_SCRIPT
from .evidence_projection import EvidenceProjectionStore

OBJECT_KEY_RE = re.compile(r"objects/[0-9a-f]{2}/[0-9a-f]{64}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
DISK_ID_RE = re.compile(r"dsk-[0-9a-f]{16}\Z")
LOGICAL_DOCUMENT_ID_RE = re.compile(r"ldoc_[0-9a-f]{32}\Z")
REGION_ENDPOINTS = {
    "aws-us-east-1": "https://control.green.us-east-1.aws.prod.archil.com",
    "aws-us-west-2": "https://control.green.us-west-2.aws.prod.archil.com",
    "aws-eu-west-1": "https://control.green.eu-west-1.aws.prod.archil.com",
}
MAX_TRANSPORT_BYTES = 256 * 1024
MAX_AGENT_PROGRAM_BYTES = 16_000
MAX_AGENT_EXEC_OUTPUT_BYTES = 40_000
RECEIPT_TOKEN_PATTERN = (
    r"recall://[A-Za-z0-9:._@+-]+/[^\s\"'<>()[\]{},;]{1,1900}"
)
RECEIPT_TOKEN_RE = re.compile(RECEIPT_TOKEN_PATTERN)
AGENT_EVIDENCE_LINE_RE = re.compile(
    rf"^RECALL_EVIDENCE[ \t]+({RECEIPT_TOKEN_PATTERN})[ \t]*$",
    re.MULTILINE,
)
EVIDENCE_RECORD_KEYS = frozenset({
    "content",
    "event_native_id",
    "occurred_at",
    "ordinal",
    "receipts",
})


class DeepInspectionError(RuntimeError):
    """Stable, content-free failure at the external compute boundary."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class HttpTransport(Protocol):
    def post(
        self,
        *,
        url: str,
        headers: dict[str, str],
        body: dict[str, Any],
        timeout: float,
    ) -> dict[str, Any]: ...


class UrllibTransport:
    def post(
        self,
        *,
        url: str,
        headers: dict[str, str],
        body: dict[str, Any],
        timeout: float,
    ) -> dict[str, Any]:
        payload = json.dumps(body, separators=(",", ":"), sort_keys=True).encode()
        request = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={
                **headers,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "recall-core/archil-deep-inspector-v1",
            },
        )
        try:
            with urllib.request.urlopen(
                request,
                timeout=timeout,
                context=ssl.create_default_context(),
            ) as response:
                if response.status != 200:
                    raise DeepInspectionError("deep_inspector_unavailable")
                content_type = response.headers.get("Content-Type", "").split(";", 1)[0]
                if content_type != "application/json":
                    raise DeepInspectionError("deep_inspector_response_invalid")
                length = response.headers.get("Content-Length")
                if length is not None and (
                    not length.isdigit() or int(length) > MAX_TRANSPORT_BYTES
                ):
                    raise DeepInspectionError("deep_inspector_response_invalid")
                raw = response.read(MAX_TRANSPORT_BYTES + 1)
        except DeepInspectionError:
            raise
        except (OSError, urllib.error.URLError) as error:
            raise DeepInspectionError("deep_inspector_unavailable") from error
        if len(raw) > MAX_TRANSPORT_BYTES:
            raise DeepInspectionError("deep_inspector_response_invalid")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise DeepInspectionError("deep_inspector_response_invalid") from None
        if not isinstance(value, dict):
            raise DeepInspectionError("deep_inspector_response_invalid")
        return value


def agent_evidence_receipts(stdout: str) -> list[str]:
    """Extract agent-selected receipts only from authoritative output shapes."""

    selected = {
        match.group(1)
        for match in AGENT_EVIDENCE_LINE_RE.finditer(stdout)
    }
    found: list[str] = []
    for line in stdout.splitlines():
        object_start = line.find("{")
        if object_start < 0:
            continue
        try:
            record = json.loads(line[object_start:])
        except json.JSONDecodeError:
            continue
        if (
            not isinstance(record, dict)
            or not EVIDENCE_RECORD_KEYS.issubset(record)
            or not isinstance(record["receipts"], list)
        ):
            continue
        found.extend(
            receipt
            for receipt in record["receipts"]
            if isinstance(receipt, str)
            and RECEIPT_TOKEN_RE.fullmatch(receipt)
        )
    authoritative = list(dict.fromkeys(found))
    if selected:
        return [
            receipt for receipt in authoritative if receipt in selected
        ]
    return authoritative


@dataclass(frozen=True)
class DeepInspectionBudget:
    max_files: int = 20
    max_matches: int = 50
    max_output_bytes: int = 96_000
    timeout_seconds: int = 20
    concurrency: int = 8

    def __post_init__(self) -> None:
        values = (
            self.max_files,
            self.max_matches,
            self.max_output_bytes,
            self.timeout_seconds,
            self.concurrency,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) for value in values):
            raise DeepInspectionError("deep_inspector_budget_invalid")
        if (
            not 1 <= self.max_files <= 100
            or not 1 <= self.max_matches <= 500
            or not 1_024 <= self.max_output_bytes <= 128 * 1024
            or not 1 <= self.timeout_seconds <= 30
            or not 1 <= self.concurrency <= 32
        ):
            raise DeepInspectionError("deep_inspector_budget_invalid")


@dataclass(frozen=True)
class EvidenceTarget:
    tenant_id: str
    source_id: str
    object_key: str
    content_sha256: str
    receipts: tuple[str, ...] = ()
    reference: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.tenant_id, str)
            or not self.tenant_id
            or not isinstance(self.source_id, str)
            or not self.source_id
            or not isinstance(self.object_key, str)
            or not OBJECT_KEY_RE.fullmatch(self.object_key)
            or not isinstance(self.content_sha256, str)
            or not SHA256_RE.fullmatch(self.content_sha256)
            or not isinstance(self.receipts, tuple)
            or len(self.receipts) > 10_000
            or any(
                not isinstance(receipt, str)
                or not receipt.startswith(f"recall://{self.source_id}/")
                or len(receipt) > 2048
                for receipt in self.receipts
            )
        ):
            raise DeepInspectionError("deep_inspector_target_invalid")

    @classmethod
    def from_reference(
        cls,
        reference: dict[str, Any],
        *,
        receipts: tuple[str, ...] = (),
    ) -> EvidenceTarget:
        if not isinstance(reference, dict):
            raise DeepInspectionError("deep_inspector_target_invalid")
        try:
            return cls(
                tenant_id=reference["tenant_id"],
                source_id=reference["source_id"],
                object_key=reference["object_key"],
                content_sha256=reference["content_sha256"],
                receipts=receipts,
                reference=dict(reference),
            )
        except KeyError:
            raise DeepInspectionError("deep_inspector_target_invalid") from None


@dataclass(frozen=True)
class AgentExecObject:
    """One immutable object admitted into an agent execution sandbox."""

    object_key: str
    content_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.object_key, str)
            or not OBJECT_KEY_RE.fullmatch(self.object_key)
            or not isinstance(self.content_sha256, str)
            or not SHA256_RE.fullmatch(self.content_sha256)
        ):
            raise DeepInspectionError("deep_inspector_target_invalid")


def _agent_exec_command(
    *,
    program: str,
    objects: tuple[AgentExecObject, ...],
    document_aliases: dict[str, str],
    record_spans: dict[str, tuple[tuple[int, int], ...]],
    routing_receipts: dict[str, tuple[str, ...]],
    timeout_seconds: int,
) -> str:
    """Build a content-addressed, no-network view for an agent-authored program."""

    payload = base64.b64encode(
        json.dumps(
            [
                {
                    "object_key": item.object_key,
                    "content_sha256": item.content_sha256,
                }
                for item in objects
            ],
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).decode()
    encoded_program = base64.b64encode(program.encode()).decode()
    encoded_scan = base64.b64encode(AGENT_SCAN_SCRIPT.encode()).decode()
    encoded_aliases = base64.b64encode(
        json.dumps(
            document_aliases,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).decode()
    encoded_pointers = base64.b64encode(
        json.dumps(
            {
                document_id: {
                    "spans": [
                        {"record_ordinal": start, "record_count": count}
                        for start, count in spans
                    ],
                    "routing_receipts": list(
                        routing_receipts.get(document_id, ())
                    ),
                }
                for document_id, spans in record_spans.items()
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).decode()
    stage_script = r"""
import base64,json,pathlib,re,subprocess,sys
items=json.loads(base64.b64decode(sys.argv[1]))
aliases=json.loads(base64.b64decode(sys.argv[2]))
source=pathlib.Path("/mnt/archil/evidence").resolve()
target=pathlib.Path("/tmp/recall-authorized").resolve()
docs=pathlib.Path("/tmp/recall-docs").resolve()
target.mkdir(mode=0o700,parents=True,exist_ok=True)
docs.mkdir(mode=0o700,parents=True,exist_ok=True)
for item in items:
    relative=pathlib.PurePosixPath(item["object_key"])
    if relative.is_absolute() or ".." in relative.parts:
        raise SystemExit(64)
    src=(source/pathlib.Path(*relative.parts)).resolve()
    if source not in src.parents or not src.is_file():
        raise SystemExit(66)
    dst=(target/pathlib.Path(*relative.parts)).resolve()
    if target not in dst.parents:
        raise SystemExit(64)
    dst.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    dst.touch(mode=0o400,exist_ok=False)
    subprocess.run(["mount","--bind",str(src),str(dst)],check=True)
    subprocess.run(["mount","-o","remount,bind,ro",str(dst)],check=True)
manifest_by_document={}
for path in target.rglob("*"):
    if not path.is_file() or path.stat().st_size > 100000:
        continue
    try:
        manifest=json.loads(path.read_text())
    except (OSError,UnicodeDecodeError,json.JSONDecodeError):
        continue
    document_id=manifest.get("logical_document_id")
    if document_id in aliases and isinstance(manifest.get("parts"),list):
        manifest_by_document[document_id]=(path,manifest)
if set(manifest_by_document)!=set(aliases):
    raise SystemExit(66)
for document_id,alias in aliases.items():
    if not re.fullmatch(r"d[1-9][0-9]?",alias):
        raise SystemExit(64)
    manifest_path,manifest=manifest_by_document[document_id]
    document_dir=docs/alias
    document_dir.mkdir(mode=0o700,exist_ok=False)
    (document_dir/"manifest.json").symlink_to(
        "/mnt/archil/evidence/"+str(manifest_path.relative_to(target))
    )
    for ordinal,part in enumerate(manifest["parts"]):
        object_key=part.get("object_key") if isinstance(part,dict) else None
        if not isinstance(object_key,str) or not re.fullmatch(
            r"objects/[0-9a-f]{2}/[0-9a-f]{64}",object_key
        ):
            raise SystemExit(66)
        part_path=(target/pathlib.Path(object_key)).resolve()
        if target not in part_path.parents or not part_path.is_file():
            raise SystemExit(66)
        (document_dir/f"part-{ordinal:05d}.jsonl").symlink_to(
            "/mnt/archil/evidence/"+object_key
        )
""".strip()
    inner_command = " && ".join([
        (
            "python3 -c "
            + shlex.quote(stage_script)
            + " "
            + shlex.quote(payload)
            + " "
            + shlex.quote(encoded_aliases)
        ),
        "mount --rbind /tmp/recall-authorized /mnt/archil/evidence",
        "mount -o remount,bind,ro /mnt/archil/evidence",
        "mkdir -p /docs",
        "mount --bind /tmp/recall-docs /docs",
        "mount -o remount,bind,ro /docs",
        (
            "exec env -i HOME=/tmp "
            "PATH=/tmp/recall-agent:/usr/local/bin:/usr/bin:/bin "
            "RECALL_POINTERS_PATH=/tmp/recall-agent/pointers.json "
            "LC_ALL=C bash -c "
            + shlex.quote(
                "set -o pipefail; "
                "bash /tmp/recall-agent/program.sh | "
                f"head -c {MAX_AGENT_EXEC_OUTPUT_BYTES}; "
                "code=${PIPESTATUS[0]}; "
                "[ \"$code\" -eq 141 ] && exit 0; exit \"$code\""
            )
        ),
    ])
    return "\n".join([
        "set -eu",
        "umask 077",
        # Archil may reuse an execution host. Remove every per-run staging
        # directory so a prior alias cannot make the next mkdir fail.
        "rm -rf /tmp/recall-authorized /tmp/recall-agent /tmp/recall-docs",
        "mkdir -p /tmp/recall-agent",
        (
            "printf '%s' "
            + shlex.quote(encoded_program)
            + " | base64 -d > /tmp/recall-agent/program.sh"
        ),
        (
            "printf '%s' "
            + shlex.quote(encoded_scan)
            + " | base64 -d > /tmp/recall-agent/recall-scan"
        ),
        (
            "printf '%s' "
            + shlex.quote(encoded_pointers)
            + " | base64 -d > /tmp/recall-agent/pointers.json"
        ),
        "chmod 0500 /tmp/recall-agent/program.sh",
        "chmod 0500 /tmp/recall-agent/recall-scan",
        "chmod 0400 /tmp/recall-agent/pointers.json",
        (
            f"timeout --signal=KILL {timeout_seconds}s "
            "unshare --user --map-root-user --net --mount --pid --fork "
            "--mount-proc sh -c "
            + shlex.quote(inner_command)
        ),
    ])


def _terms(question: str) -> tuple[str, ...]:
    if not isinstance(question, str) or not question.strip() or len(question) > 8192:
        raise DeepInspectionError("deep_inspector_question_invalid")
    candidates = re.findall(r"[\w.-]{3,}", question.casefold(), flags=re.UNICODE)
    stop = {
        "about",
        "after",
        "before",
        "could",
        "deep",
        "everything",
        "files",
        "find",
        "from",
        "have",
        "search",
        "that",
        "the",
        "this",
        "what",
        "when",
        "where",
        "which",
        "with",
    }
    return tuple(dict.fromkeys(term for term in candidates if term not in stop))[:32]


def _bounded_findings(
    findings: list[dict[str, Any]],
    *,
    max_matches: int,
    max_output_bytes: int,
) -> list[dict[str, Any]]:
    bounded: list[dict[str, Any]] = []
    for finding in findings:
        if len(bounded) >= max_matches:
            break
        candidate = [*bounded, finding]
        if len(json.dumps(candidate, ensure_ascii=False).encode()) > max_output_bytes:
            break
        bounded.append(finding)
    return bounded


class LocalDeepInspector:
    """Portable fallback using the same evidence bundle and result contract."""

    def __init__(self, projection: EvidenceProjectionStore):
        self.projection = projection

    def inspect(
        self,
        *,
        tenant_id: str,
        question: str,
        targets: tuple[EvidenceTarget, ...],
        budget: DeepInspectionBudget,
    ) -> dict[str, Any]:
        terms = _terms(question)
        selected = targets[: budget.max_files]
        findings: list[dict[str, Any]] = []
        for target in selected:
            if target.tenant_id != tenant_id or target.reference is None:
                raise DeepInspectionError("deep_inspector_target_invalid")
            try:
                bundle = self.projection.read(
                    target.reference,
                    tenant_id=tenant_id,
                    source_id=target.source_id,
                )
            except Exception as error:
                raise DeepInspectionError("deep_inspector_object_unavailable") from error
            for chunk in bundle.chunks:
                lowered = chunk.text.casefold()
                score = sum(lowered.count(term) for term in terms)
                if terms and score == 0:
                    continue
                findings.append(
                    {
                        "receipt": chunk.receipt,
                        "text": chunk.text[:8_192],
                        "line": chunk.ordinal + 1,
                        "object_key": target.object_key,
                        "_score": score,
                    }
                )
        findings.sort(
            key=lambda item: (item["_score"], item["receipt"]),
            reverse=True,
        )
        rendered = [
            {key: value for key, value in finding.items() if key != "_score"}
            for finding in findings
        ]
        return {
            "provider": "local",
            "findings": _bounded_findings(
                rendered,
                max_matches=budget.max_matches,
                max_output_bytes=budget.max_output_bytes,
            ),
            "complete": len(targets) <= budget.max_files,
            "files_scanned": len(selected),
            "stopped_reason": (
                "completed" if len(targets) <= budget.max_files else "max_files"
            ),
            "timing": None,
        }




class ArchilDeepInspector:
    """Archil serverless execution over immutable, tenant-selected objects."""

    def __init__(
        self,
        *,
        api_key: str,
        disk_id: str,
        region: str,
        transport: HttpTransport | None = None,
    ) -> None:
        if (
            not isinstance(api_key, str)
            or not api_key.strip()
            or len(api_key) > 4096
            or not isinstance(disk_id, str)
            or not DISK_ID_RE.fullmatch(disk_id)
            or region not in REGION_ENDPOINTS
        ):
            raise DeepInspectionError("deep_inspector_configuration_invalid")
        self.api_key = api_key
        self.disk_id = disk_id
        self.region = region
        self.transport = transport or UrllibTransport()

    def execute(
        self,
        *,
        tenant_id: str,
        program: str,
        objects: tuple[AgentExecObject, ...],
        record_spans: dict[str, tuple[tuple[int, int], ...]],
        routing_receipts: dict[str, tuple[str, ...]],
        timeout_seconds: int,
        document_aliases: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Run arbitrary agent-authored shell against only admitted objects."""

        if (
            not isinstance(tenant_id, str)
            or not tenant_id
            or not isinstance(program, str)
            or not program.strip()
            or len(program.encode()) > MAX_AGENT_PROGRAM_BYTES
            or not isinstance(objects, tuple)
            or not 1 <= len(objects) <= 512
            or any(not isinstance(item, AgentExecObject) for item in objects)
            or (
                document_aliases is not None
                and (
                    not isinstance(document_aliases, dict)
                    or not 1 <= len(document_aliases) <= 80
                    or set(document_aliases) != set(record_spans)
                    or len(set(document_aliases.values()))
                    != len(document_aliases)
                    or any(
                        not isinstance(document_id, str)
                        or not LOGICAL_DOCUMENT_ID_RE.fullmatch(document_id)
                        or not isinstance(alias, str)
                        or re.fullmatch(r"d[1-9][0-9]?", alias) is None
                        for document_id, alias in document_aliases.items()
                    )
                )
            )
            or not isinstance(record_spans, dict)
            or len(record_spans) > 80
            or any(
                not isinstance(document_id, str)
                or not LOGICAL_DOCUMENT_ID_RE.fullmatch(document_id)
                or not isinstance(spans, tuple)
                or len(spans) > 256
                or any(
                    not isinstance(span, tuple)
                    or len(span) != 2
                    or isinstance(span[0], bool)
                    or not isinstance(span[0], int)
                    or span[0] < 0
                    or isinstance(span[1], bool)
                    or not isinstance(span[1], int)
                    or not 1 <= span[1] <= 10_000
                    for span in spans
                )
                for document_id, spans in record_spans.items()
            )
            or not isinstance(routing_receipts, dict)
            or set(routing_receipts) - set(record_spans)
            or any(
                not isinstance(receipts, tuple)
                or len(receipts) > 256
                or any(
                    not isinstance(receipt, str)
                    or not receipt.startswith("recall://")
                    or len(receipt) > 2048
                    for receipt in receipts
                )
                for receipts in routing_receipts.values()
            )
            or isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int)
            or not 1 <= timeout_seconds <= 30
        ):
            raise DeepInspectionError("deep_inspector_exec_invalid")
        unique = tuple({
            (item.object_key, item.content_sha256): item
            for item in objects
        }.values())
        aliases = document_aliases or {
            document_id: f"d{ordinal}"
            for ordinal, document_id in enumerate(record_spans, start=1)
        }
        response = self.transport.post(
            url=REGION_ENDPOINTS[self.region] + "/api/exec",
            headers={"Authorization": self.api_key},
            body={
                "disks": {
                    "evidence": {
                        "disk": self.disk_id,
                        "readOnly": True,
                    }
                },
                "command": _agent_exec_command(
                    program=program,
                    objects=unique,
                    document_aliases=aliases,
                    record_spans=record_spans,
                    routing_receipts=routing_receipts,
                    timeout_seconds=timeout_seconds,
                ),
            },
            timeout=timeout_seconds + 4,
        )
        if (
            not isinstance(response, dict)
            or response.get("success") is not True
            or not isinstance(response.get("data"), dict)
        ):
            raise DeepInspectionError("deep_inspector_unavailable")
        data = response["data"]
        stdout = data.get("stdout")
        stderr = data.get("stderr", "")
        exit_code = data.get("exitCode")
        timing = data.get("timing")
        if (
            not isinstance(stdout, str)
            or not isinstance(stderr, str)
            or isinstance(exit_code, bool)
            or not isinstance(exit_code, int)
            or not isinstance(timing, dict)
        ):
            raise DeepInspectionError("deep_inspector_result_invalid_execution")
        stdout_bytes = stdout.encode()
        stderr_bytes = stderr.encode()
        truncated = (
            len(stdout_bytes) > MAX_AGENT_EXEC_OUTPUT_BYTES
            or len(stderr_bytes) > 8_000
        )
        bounded_stdout = stdout_bytes[:MAX_AGENT_EXEC_OUTPUT_BYTES].decode(
            errors="ignore"
        )
        bounded_stderr = stderr_bytes[:8_000].decode(errors="ignore")
        timed_out = exit_code in {124, 137}
        return {
            "provider": "archil",
            "stdout": bounded_stdout,
            "stderr": bounded_stderr,
            "exit_code": exit_code,
            "complete": exit_code == 0 and not truncated,
            "stopped_reason": (
                "timeout"
                if timed_out
                else "output_limit"
                if truncated
                else "completed"
                if exit_code == 0
                else "nonzero_exit"
            ),
            "output_truncated": truncated,
            "timing": {
                key: timing.get(key)
                for key in ("totalMs", "queueMs", "executeMs")
            },
        }
