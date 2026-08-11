from __future__ import annotations

import base64
import gzip
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
MAX_ARCHIL_COMMAND_BYTES = 100_000
AGENT_EXEC_STAGE_GRACE_SECONDS = 45
RECEIPT_TOKEN_PATTERN = (
    r"recall://[A-Za-z0-9:._@+-]+/[^\s\"'<>()[\]{},;]{1,1900}"
)
RECEIPT_TOKEN_RE = re.compile(RECEIPT_TOKEN_PATTERN)
AGENT_EVIDENCE_LINE_RE = re.compile(
    rf"^RECALL_EVIDENCE[ \t]+({RECEIPT_TOKEN_PATTERN})[ \t]*$",
    re.MULTILINE,
)
EXEC_TIMING_LINE_RE = re.compile(
    r"^RECALL_EXEC_TIMING_V1\t"
    r"(wrapper_start|payload_ready|namespace_start|stage_start|objects_ready|"
    r"views_ready|tool_ready|stage_end|sandbox_ready|program_start|program_end)"
    r"\t([0-9]{10,20})$"
)
EXEC_TIMING_ORDER = (
    "wrapper_start",
    "payload_ready",
    "namespace_start",
    "stage_start",
    "objects_ready",
    "views_ready",
    "tool_ready",
    "stage_end",
    "sandbox_ready",
    "program_start",
    "program_end",
)
EVIDENCE_RECORD_KEYS = frozenset({
    "event_native_id",
    "occurred_at",
    "ordinal",
    "receipts",
})
EVIDENCE_RECORD_BODY_KEYS = frozenset({"content", "text", "content_fragment"})


class DeepInspectionError(RuntimeError):
    """Stable, content-free failure at the external compute boundary."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _execution_timing(
    stderr: str,
    upstream: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Strip content-free wrapper markers and expose attributable durations."""

    visible: list[str] = []
    markers: dict[str, int] = {}
    for raw_line in stderr.splitlines(keepends=True):
        line = raw_line.rstrip("\r\n")
        match = EXEC_TIMING_LINE_RE.fullmatch(line)
        if match is None:
            visible.append(raw_line)
            continue
        phase, raw = match.groups()
        value = int(raw)
        if phase == "program_end":
            markers[phase] = value
        else:
            markers.setdefault(phase, value)
    timing = {
        key: upstream.get(key)
        for key in ("totalMs", "queueMs", "executeMs")
    }
    if set(markers) == set(EXEC_TIMING_ORDER):
        ordered = [markers[phase] for phase in EXEC_TIMING_ORDER]
        if ordered == sorted(ordered):
            intervals = {
                f"{left}_to_{right}Ms": round((end - start) / 1_000, 3)
                for left, right, start, end in zip(
                    EXEC_TIMING_ORDER[:-1],
                    EXEC_TIMING_ORDER[1:],
                    ordered[:-1],
                    ordered[1:],
                    strict=True,
                )
            }
            wrapper_ms = round((ordered[-1] - ordered[0]) / 1_000, 3)
            execute_ms = upstream.get("executeMs")
            timing["phases"] = {
                **intervals,
                "wrapperMs": wrapper_ms,
                "archilUnobservedExecuteMs": (
                    round(max(float(execute_ms) - wrapper_ms, 0), 3)
                    if isinstance(execute_ms, (int, float))
                    and not isinstance(execute_ms, bool)
                    else None
                ),
            }
    return "".join(visible), timing


def _execution_result(data: dict[str, Any]) -> dict[str, Any]:
    """Validate and bound one Archil execution response."""

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
    visible_stderr, measured_timing = _execution_timing(stderr, timing)
    stdout_bytes = stdout.encode()
    stderr_bytes = visible_stderr.encode()
    truncated = (
        len(stdout_bytes) > MAX_AGENT_EXEC_OUTPUT_BYTES
        or len(stderr_bytes) > 8_000
    )
    timed_out = exit_code in {124, 137}
    return {
        "provider": "archil",
        "stdout": stdout_bytes[:MAX_AGENT_EXEC_OUTPUT_BYTES].decode(
            errors="ignore"
        ),
        "stderr": stderr_bytes[:8_000].decode(errors="ignore"),
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
        "timing": measured_timing,
    }


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
        except urllib.error.HTTPError as error:
            code = {
                400: "deep_inspector_bad_request",
                401: "deep_inspector_authentication_failed",
                403: "deep_inspector_authentication_failed",
                404: "deep_inspector_endpoint_not_found",
                409: "deep_inspector_busy",
                413: "deep_inspector_request_too_large",
                422: "deep_inspector_validation_failed",
                429: "deep_inspector_rate_limited",
            }.get(
                error.code,
                (
                    "deep_inspector_upstream_failed"
                    if 500 <= error.code <= 599
                    else "deep_inspector_request_rejected"
                ),
            )
            raise DeepInspectionError(code) from error
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
            or not EVIDENCE_RECORD_BODY_KEYS.intersection(record)
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
    dataset_aliases: dict[str, str] | None = None,
    tool_object: AgentExecObject | None = None,
) -> str:
    """Build a content-addressed, no-network view for an agent-authored program."""

    def encode(value: bytes) -> str:
        return base64.b64encode(gzip.compress(value, mtime=0)).decode()

    payload = encode(
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
    )
    encoded_program = encode(program.encode())
    encoded_scan = encode(AGENT_SCAN_SCRIPT.encode())
    encoded_aliases = encode(
        json.dumps(
            document_aliases,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )
    encoded_pointers = encode(
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
    )
    encoded_datasets = encode(
        json.dumps(
            dataset_aliases or {},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )
    encoded_tool = encode(
        json.dumps(
            (
                {
                    "object_key": tool_object.object_key,
                    "content_sha256": tool_object.content_sha256,
                }
                if tool_object is not None
                else None
            ),
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )
    stage_script = r"""
import base64,gzip,hashlib,json,pathlib,re,shutil,subprocess,sys,time
def mark(name):
    print(f"RECALL_EXEC_TIMING_V1\t{name}\t{time.time_ns()//1000}",file=sys.stderr,flush=True)
mark("stage_start")
items=json.loads(gzip.decompress(base64.b64decode(sys.argv[1])))
aliases=json.loads(gzip.decompress(base64.b64decode(sys.argv[2])))
datasets=json.loads(gzip.decompress(base64.b64decode(sys.argv[3])))
tool=json.loads(gzip.decompress(base64.b64decode(sys.argv[4])))
source=pathlib.Path("/mnt/archil/evidence").resolve()
target=pathlib.Path("/tmp/recall-authorized").resolve()
docs=pathlib.Path("/tmp/recall-docs").resolve()
dataset_root=pathlib.Path("/tmp/recall-datasets").resolve()
target.mkdir(mode=0o700,parents=True,exist_ok=True)
docs.mkdir(mode=0o700,parents=True,exist_ok=True)
dataset_root.mkdir(mode=0o700,parents=True,exist_ok=True)
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
mark("objects_ready")
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
for object_key,alias in datasets.items():
    if not re.fullmatch(
        r"s[1-9][0-9]{0,2}/[0-9]{4}-[0-9]{2}/(?:documents|records|actors)\.parquet",
        alias,
    ):
        raise SystemExit(64)
    src=(target/pathlib.Path(object_key)).resolve()
    if target not in src.parents or not src.is_file():
        raise SystemExit(66)
    dst=(dataset_root/pathlib.Path(alias)).resolve()
    if dataset_root not in dst.parents:
        raise SystemExit(64)
    dst.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    dst.symlink_to("/mnt/archil/evidence/"+object_key)
mark("views_ready")
if tool is not None:
    src=(target/pathlib.Path(tool["object_key"])).resolve()
    if target not in src.parents or not src.is_file():
        raise SystemExit(66)
    if hashlib.sha256(src.read_bytes()).hexdigest()!=tool["content_sha256"]:
        raise SystemExit(66)
    shutil.copyfile(src,"/tmp/recall-agent/duckdb")
    pathlib.Path("/tmp/recall-agent/duckdb").chmod(0o500)
mark("tool_ready")
mark("stage_end")
""".strip()
    mark_script = r"""#!/usr/bin/env bash
printf 'RECALL_EXEC_TIMING_V1\t%s\t%s\n' "$1" "${EPOCHREALTIME/./}" >&2
""".strip()
    encoded_mark = encode(mark_script.encode())
    inflate_script = (
        "import base64,gzip,sys;sys.stdout.buffer.write("
        "gzip.decompress(base64.b64decode(sys.argv[1])))"
    )

    def inflate(encoded: str, path: str) -> str:
        return (
            "python3 -c "
            + shlex.quote(inflate_script)
            + " "
            + shlex.quote(encoded)
            + " > "
            + shlex.quote(path)
        )

    inner_command = " && ".join([
        "/tmp/recall-agent/recall-mark namespace_start",
        (
            "python3 -c "
            + shlex.quote(stage_script)
            + " "
            + shlex.quote(payload)
            + " "
            + shlex.quote(encoded_aliases)
            + " "
            + shlex.quote(encoded_datasets)
            + " "
            + shlex.quote(encoded_tool)
        ),
        "mount --rbind /tmp/recall-authorized /mnt/archil/evidence",
        "mount -o remount,bind,ro /mnt/archil/evidence",
        "mkdir -p /docs",
        "mount --bind /tmp/recall-docs /docs",
        "mount -o remount,bind,ro /docs",
        "mkdir -p /datasets",
        "mount --bind /tmp/recall-datasets /datasets",
        "mount -o remount,bind,ro /datasets",
        "/tmp/recall-agent/recall-mark sandbox_ready",
        (
            "exec env -i HOME=/tmp "
            "PATH=/tmp/recall-agent:/usr/local/bin:/usr/bin:/bin "
            "RECALL_POINTERS_PATH=/tmp/recall-agent/pointers.json "
            "LC_ALL=C bash -c "
            + shlex.quote(
                "set -o pipefail; "
                "/tmp/recall-agent/recall-mark program_start; "
                f"timeout --signal=KILL {timeout_seconds}s "
                "bash /tmp/recall-agent/program.sh | "
                f"head -c {MAX_AGENT_EXEC_OUTPUT_BYTES}; "
                "code=${PIPESTATUS[0]}; "
                "/tmp/recall-agent/recall-mark program_end; "
                "[ \"$code\" -eq 141 ] && exit 0; exit \"$code\""
            )
        ),
    ])
    return "\n".join([
        "set -eu",
        "umask 077",
        (
            "printf 'RECALL_EXEC_TIMING_V1\\twrapper_start\\t%s\\n' "
            '"$(date +%s%6N)" >&2'
        ),
        # Archil may reuse an execution host. Remove every per-run staging
        # directory so a prior alias cannot make the next mkdir fail.
        "rm -rf /tmp/recall-authorized /tmp/recall-agent /tmp/recall-docs "
        "/tmp/recall-datasets",
        "mkdir -p /tmp/recall-agent",
        inflate(encoded_program, "/tmp/recall-agent/program.sh"),
        inflate(encoded_scan, "/tmp/recall-agent/recall-scan"),
        inflate(encoded_pointers, "/tmp/recall-agent/pointers.json"),
        inflate(encoded_mark, "/tmp/recall-agent/recall-mark"),
        "chmod 0500 /tmp/recall-agent/program.sh",
        "chmod 0500 /tmp/recall-agent/recall-scan",
        "chmod 0500 /tmp/recall-agent/recall-mark",
        "chmod 0400 /tmp/recall-agent/pointers.json",
        "/tmp/recall-agent/recall-mark payload_ready",
        (
            "unshare --user --map-root-user --net --mount --pid --fork "
            "--mount-proc bash -c "
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
        duckdb_tool: AgentExecObject | None = None,
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
        self.duckdb_tool = duckdb_tool
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
        command = _agent_exec_command(
            program=program,
            objects=unique,
            document_aliases=aliases,
            record_spans=record_spans,
            routing_receipts=routing_receipts,
            timeout_seconds=timeout_seconds,
        )
        if len(command.encode()) > MAX_ARCHIL_COMMAND_BYTES:
            raise DeepInspectionError("deep_inspector_request_too_large")
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
                "command": command,
            },
            timeout=timeout_seconds + AGENT_EXEC_STAGE_GRACE_SECONDS,
        )
        if (
            not isinstance(response, dict)
            or response.get("success") is not True
            or not isinstance(response.get("data"), dict)
        ):
            raise DeepInspectionError("deep_inspector_unavailable")
        return _execution_result(response["data"])

    def execute_scan(
        self,
        *,
        tenant_id: str,
        program: str,
        objects: tuple[AgentExecObject, ...],
        dataset_aliases: dict[str, str],
        timeout_seconds: int,
    ) -> dict[str, Any]:
        """Run DuckDB beside only authorized source/month Parquet shards."""

        if (
            self.duckdb_tool is None
            or not isinstance(tenant_id, str)
            or not tenant_id
            or not isinstance(program, str)
            or not program.strip()
            or len(program.encode()) > MAX_AGENT_PROGRAM_BYTES
            or not isinstance(objects, tuple)
            or not 1 <= len(objects) <= 511
            or any(not isinstance(item, AgentExecObject) for item in objects)
            or not isinstance(dataset_aliases, dict)
            or set(dataset_aliases) != {item.object_key for item in objects}
            or len(set(dataset_aliases.values())) != len(dataset_aliases)
            or any(
                not isinstance(alias, str)
                or re.fullmatch(
                    r"s[1-9][0-9]{0,2}/[0-9]{4}-[0-9]{2}/"
                    r"(?:documents|records|actors)\.parquet",
                    alias,
                ) is None
                for alias in dataset_aliases.values()
            )
            or isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int)
            or not 1 <= timeout_seconds <= 240
        ):
            raise DeepInspectionError("deep_inspector_exec_invalid")
        command = _agent_exec_command(
            program=program,
            objects=(*objects, self.duckdb_tool),
            document_aliases={},
            record_spans={},
            routing_receipts={},
            timeout_seconds=timeout_seconds,
            dataset_aliases=dataset_aliases,
            tool_object=self.duckdb_tool,
        )
        if len(command.encode()) > MAX_ARCHIL_COMMAND_BYTES:
            raise DeepInspectionError("deep_inspector_request_too_large")
        response = self.transport.post(
            url=REGION_ENDPOINTS[self.region] + "/api/exec",
            headers={"Authorization": self.api_key},
            body={
                "disks": {
                    "evidence": {"disk": self.disk_id, "readOnly": True}
                },
                "command": command,
            },
            timeout=timeout_seconds + AGENT_EXEC_STAGE_GRACE_SECONDS,
        )
        if (
            not isinstance(response, dict)
            or response.get("success") is not True
            or not isinstance(response.get("data"), dict)
        ):
            raise DeepInspectionError("deep_inspector_unavailable")
        return _execution_result(response["data"])
