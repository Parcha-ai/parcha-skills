from __future__ import annotations

import base64
import binascii
import os
import re
from collections.abc import Callable, Mapping
from typing import Any

from .archive import (
    ArchiveError,
    ArchiveNotFound,
    ArchiveRequest,
    ArtifactReference,
    S3ArchiveStore,
)

R2_REQUIRED_ENV = (
    "RECALL_ARCHIVE_BUCKET",
    "RECALL_ARCHIVE_ENDPOINT_URL",
    "RECALL_ARCHIVE_REGION",
    "RECALL_ARCHIVE_ACCESS_KEY_ID",
    "RECALL_ARCHIVE_SECRET_ACCESS_KEY",
    "RECALL_ARCHIVE_NAMESPACE_KEY",
)
AWS_REGION_RE = re.compile(r"[a-z]{2}(?:-gov)?-[a-z]+-\d\Z")


class BotoS3Client:
    """Translate provider-specific missing-object errors into the archive contract."""

    def __init__(self, client: Any):
        self.client = client

    @staticmethod
    def _missing(error: Exception) -> bool:
        response = getattr(error, "response", None)
        if not isinstance(response, dict):
            return False
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = response.get("Error", {}).get("Code")
        return status == 404 and code in {
            None,
            "404",
            "NotFound",
            "NoSuchKey",
            "NoSuchVersion",
        }

    def _call(self, operation: str, **kwargs: Any) -> dict[str, Any]:
        try:
            return getattr(self.client, operation)(**kwargs)
        except Exception as error:
            if self._missing(error):
                raise ArchiveNotFound("archive object not found") from None
            raise

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        return self._call("put_object", **kwargs)

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        return self._call("head_object", **kwargs)

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        return self._call("get_object", **kwargs)

    def delete_object(self, **kwargs: Any) -> dict[str, Any]:
        return self._call("delete_object", **kwargs)


def _required(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError("archive configuration is incomplete")
    return value.strip()


def _namespace_key(value: str) -> bytes:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        raise ValueError("archive namespace key is invalid") from None
    if len(decoded) != 32:
        raise ValueError("archive namespace key is invalid")
    return decoded


def build_archive_store(
    environment: Mapping[str, str] | None = None,
    *,
    client_factory: Callable[..., Any] | None = None,
) -> S3ArchiveStore:
    values = os.environ if environment is None else environment
    backend = _required(values, "RECALL_ARCHIVE_BACKEND")
    if backend not in {"r2", "s3", "s3-unversioned"}:
        raise ValueError("archive configuration backend is unsupported")
    configured = {name: _required(values, name) for name in R2_REQUIRED_ENV}
    region = configured["RECALL_ARCHIVE_REGION"]
    endpoint_url = configured["RECALL_ARCHIVE_ENDPOINT_URL"]
    if backend == "r2":
        if region != "auto":
            raise ValueError("R2 region must be auto")
    elif (
        not AWS_REGION_RE.fullmatch(region)
        or endpoint_url != f"https://s3.{region}.amazonaws.com"
    ):
        raise ValueError("S3 endpoint must match its AWS region")
    namespace_key = _namespace_key(configured["RECALL_ARCHIVE_NAMESPACE_KEY"])

    if client_factory is None:
        import boto3

        client_factory = boto3.client
    client = client_factory(
        service_name="s3",
        endpoint_url=endpoint_url,
        region_name=region,
        aws_access_key_id=configured["RECALL_ARCHIVE_ACCESS_KEY_ID"],
        aws_secret_access_key=configured["RECALL_ARCHIVE_SECRET_ACCESS_KEY"],
    )
    return S3ArchiveStore(
        bucket=configured["RECALL_ARCHIVE_BUCKET"],
        endpoint_url=endpoint_url,
        namespace_key=namespace_key,
        client=BotoS3Client(client),
        compatibility_profile={
            "r2": "r2",
            "s3": "aws",
            "s3-unversioned": "aws-unversioned",
        }[backend],
    )


def build_evidence_archive_store(
    environment: Mapping[str, str] | None = None,
    *,
    client_factory: Callable[..., Any] | None = None,
) -> S3ArchiveStore:
    """Build the separately credentialed, privacy-processed evidence bucket."""
    values = os.environ if environment is None else environment
    translated = {
        "RECALL_ARCHIVE_BACKEND": _required(
            values, "RECALL_EVIDENCE_ARCHIVE_BACKEND"
        ),
        "RECALL_ARCHIVE_BUCKET": _required(
            values, "RECALL_EVIDENCE_ARCHIVE_BUCKET"
        ),
        "RECALL_ARCHIVE_ENDPOINT_URL": _required(
            values, "RECALL_EVIDENCE_ARCHIVE_ENDPOINT_URL"
        ),
        "RECALL_ARCHIVE_REGION": _required(
            values, "RECALL_EVIDENCE_ARCHIVE_REGION"
        ),
        "RECALL_ARCHIVE_ACCESS_KEY_ID": _required(
            values, "RECALL_EVIDENCE_ARCHIVE_ACCESS_KEY_ID"
        ),
        "RECALL_ARCHIVE_SECRET_ACCESS_KEY": _required(
            values, "RECALL_EVIDENCE_ARCHIVE_SECRET_ACCESS_KEY"
        ),
        "RECALL_ARCHIVE_NAMESPACE_KEY": _required(
            values, "RECALL_EVIDENCE_ARCHIVE_NAMESPACE_KEY"
        ),
    }
    return build_archive_store(translated, client_factory=client_factory)


def probe_archive(store: S3ArchiveStore) -> dict[str, Any]:
    """Exercise the complete private object lifecycle without source content."""
    payload = os.urandom(32)
    reference: ArtifactReference | None = None
    deleted = False
    try:
        request = ArchiveRequest(
            tenant_id="tenant:archive-check",
            source_id="source:archive-check",
            native_id="probe:" + os.urandom(16).hex(),
            media_type="application/octet-stream",
            payload=payload,
        )
        reference = store.put(request)
        replay = store.put(request)
        read = store.read(
            reference,
            tenant_id=request.tenant_id,
            source_id=request.source_id,
        )
        deleted = store.delete(
            reference,
            tenant_id=request.tenant_id,
            source_id=request.source_id,
        )
        if store.delete(
            reference,
            tenant_id=request.tenant_id,
            source_id=request.source_id,
        ):
            raise ArchiveError("archive probe failed")
        try:
            store.read(
                reference,
                tenant_id=request.tenant_id,
                source_id=request.source_id,
            )
        except ArchiveNotFound:
            absent = True
        else:
            absent = False
        if replay != reference or read != payload or not deleted or not absent:
            raise ArchiveError("archive probe failed")
        return {
            "status": "ok",
            "backend": store.storage_backend,
            "write": True,
            "replay": True,
            "read": True,
            "delete": True,
            "absent": True,
        }
    finally:
        if reference is not None and not deleted:
            try:
                store.delete(
                    reference,
                    tenant_id="tenant:archive-check",
                    source_id="source:archive-check",
                )
            except ArchiveError:
                pass
