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

from .evidence_projection import EvidenceProjectionStore

OBJECT_KEY_RE = re.compile(r"objects/[0-9a-f]{2}/[0-9a-f]{64}\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
DISK_ID_RE = re.compile(r"dsk-[0-9a-f]{16}\Z")
REGION_ENDPOINTS = {
    "aws-us-east-1": "https://control.green.us-east-1.aws.prod.archil.com",
    "aws-us-west-2": "https://control.green.us-west-2.aws.prod.archil.com",
    "aws-eu-west-1": "https://control.green.eu-west-1.aws.prod.archil.com",
}
MAX_TRANSPORT_BYTES = 256 * 1024


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


ARCHIL_INSPECT_SCRIPT = r"""
import base64,json,pathlib,re,sys
p=json.loads(base64.b64decode(sys.argv[1]))
root=pathlib.Path("/mnt/archil/evidence").resolve()
terms=[]
for term in re.findall(r"[\w.-]{3,}",p["question"].casefold()):
    if term not in terms:
        terms.append(term)
terms=terms[:32]
allowed=set(p["allowed_receipts"])
out=[]
scanned=0
for key in p["object_keys"]:
    path=(root/key).resolve()
    if root not in path.parents:
        continue
    try:
        value=json.loads(path.read_text())
    except Exception:
        continue
    scanned+=1
    for chunk in value.get("chunks",[]):
        receipt=chunk.get("receipt")
        text=chunk.get("text")
        if receipt not in allowed or not isinstance(text,str):
            continue
        lowered=text.casefold()
        score=sum(lowered.count(term) for term in terms)
        if terms and score==0:
            continue
        out.append({"receipt":receipt,"text":text[:8192],"line":int(chunk.get("ordinal",0))+1,"object_key":key,"_score":score})
out.sort(key=lambda x:(x["_score"],x["receipt"]),reverse=True)
findings=[]
for item in out:
    item.pop("_score",None)
    candidate=findings+[item]
    if len(candidate)>p["max_matches"] or len(json.dumps(candidate,ensure_ascii=False).encode())>p["max_output_bytes"]:
        break
    findings.append(item)
print(json.dumps({"findings":findings,"complete":scanned==len(p["object_keys"]),"files_scanned":scanned},ensure_ascii=False,separators=(",",":")))
""".strip()


class ArchilDeepInspector:
    """Archil serverless execution with a fixed script and read-only disk mount."""

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

    def inspect(
        self,
        *,
        tenant_id: str,
        question: str,
        targets: tuple[EvidenceTarget, ...],
        budget: DeepInspectionBudget,
    ) -> dict[str, Any]:
        _terms(question)
        if not targets:
            return {
                "provider": "archil",
                "findings": [],
                "complete": True,
                "files_scanned": 0,
                "stopped_reason": "completed",
                "timing": None,
            }
        selected = targets[: budget.max_files]
        if any(target.tenant_id != tenant_id for target in selected):
            raise DeepInspectionError("deep_inspector_target_invalid")
        object_keys = [target.object_key for target in selected]
        allowed_receipts = sorted(
            {receipt for target in selected for receipt in target.receipts}
        )
        payload = {
            "question": question,
            "object_keys": object_keys,
            "allowed_receipts": allowed_receipts,
            "max_matches": budget.max_matches,
            "max_output_bytes": budget.max_output_bytes,
        }
        encoded = base64.b64encode(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        ).decode()
        command = (
            "python3 -c "
            + shlex.quote(ARCHIL_INSPECT_SCRIPT)
            + " "
            + shlex.quote(encoded)
        )
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
            timeout=budget.timeout_seconds + 2,
        )
        result = self._validate_response(
            response,
            selected=selected,
            budget=budget,
        )
        result["provider"] = "archil"
        result["stopped_reason"] = (
            "completed"
            if result["complete"] and len(targets) <= budget.max_files
            else "partial"
        )
        return result

    @staticmethod
    def _validate_response(
        response: dict[str, Any],
        *,
        selected: tuple[EvidenceTarget, ...],
        budget: DeepInspectionBudget,
    ) -> dict[str, Any]:
        if (
            not isinstance(response, dict)
            or response.get("success") is not True
            or not isinstance(response.get("data"), dict)
        ):
            raise DeepInspectionError("deep_inspector_unavailable")
        data = response["data"]
        stdout = data.get("stdout")
        if (
            data.get("exitCode") != 0
            or not isinstance(stdout, str)
            or len(stdout.encode()) > budget.max_output_bytes + 16_384
            or not isinstance(data.get("timing"), dict)
        ):
            raise DeepInspectionError("deep_inspector_result_invalid")
        try:
            value = json.loads(stdout)
        except json.JSONDecodeError:
            raise DeepInspectionError("deep_inspector_result_invalid") from None
        if (
            not isinstance(value, dict)
            or set(value) != {"findings", "complete", "files_scanned"}
            or not isinstance(value["findings"], list)
            or len(value["findings"]) > budget.max_matches
            or not isinstance(value["complete"], bool)
            or isinstance(value["files_scanned"], bool)
            or not isinstance(value["files_scanned"], int)
            or not 0 <= value["files_scanned"] <= len(selected)
        ):
            raise DeepInspectionError("deep_inspector_result_invalid")
        allowed_pairs = {
            (target.object_key, receipt)
            for target in selected
            for receipt in target.receipts
        }
        for finding in value["findings"]:
            if (
                not isinstance(finding, dict)
                or set(finding) != {"receipt", "text", "line", "object_key"}
                or (
                    finding.get("object_key"),
                    finding.get("receipt"),
                )
                not in allowed_pairs
                or not isinstance(finding.get("text"), str)
                or len(finding["text"].encode()) > 8_192
                or isinstance(finding.get("line"), bool)
                or not isinstance(finding.get("line"), int)
                or finding["line"] < 1
            ):
                raise DeepInspectionError("deep_inspector_result_invalid")
        if len(json.dumps(value["findings"], ensure_ascii=False).encode()) > budget.max_output_bytes:
            raise DeepInspectionError("deep_inspector_result_invalid")
        return {
            **value,
            "timing": {
                key: data["timing"].get(key)
                for key in ("totalMs", "queueMs", "executeMs")
            },
        }
