from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import tempfile
from base64 import b64encode
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Protocol
from urllib.parse import urlsplit

from contracts.v2 import ContractError, validate_contract

DEFAULT_MAXIMUM_BYTES = 64 * 1024 * 1024
IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+-]{1,255}")
MEDIA_TYPE_RE = re.compile(r"[a-z0-9][a-z0-9.+-]{0,63}/[a-z0-9][a-z0-9.+-]{0,127}")
BUCKET_RE = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
R2_HOST_RE = re.compile(
    r"[0-9a-f]{32}\.(?:(?:eu|fedramp)\.)?r2\.cloudflarestorage\.com"
)
SHA256_RE = re.compile(r"[0-9a-f]{64}")
OBJECT_KEY_RE = re.compile(r"objects/[0-9a-f]{2}/[0-9a-f]{64}")
ARTIFACT_ID_RE = re.compile(r"art_[0-9a-f]{32}")


class ArchiveError(Exception):
    """Base exception whose message never contains source content or identifiers."""


class ArchiveNotFound(ArchiveError):
    pass


class ArchiveCorruption(ArchiveError):
    pass


class S3Client(Protocol):
    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def delete_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ArchiveRequest:
    tenant_id: str
    source_id: str
    native_id: str
    media_type: str
    payload: bytes

    def validate(self, maximum_bytes: int) -> None:
        for value in (self.tenant_id, self.source_id, self.native_id):
            if not isinstance(value, str) or not IDENTITY_RE.fullmatch(value):
                raise ValueError("archive identity is invalid")
        if not isinstance(self.media_type, str) or not MEDIA_TYPE_RE.fullmatch(self.media_type):
            raise ValueError("archive media type is invalid")
        if not isinstance(self.payload, bytes):
            raise ValueError("archive payload must be bytes")
        if len(self.payload) > maximum_bytes:
            raise ValueError("archive payload exceeds byte bound")


@dataclass(frozen=True)
class ArtifactReference:
    artifact_id: str
    tenant_scope_sha256: str
    source_scope_sha256: str
    storage_backend: str
    object_key: str
    content_sha256: str
    size_bytes: int
    media_type: str
    encryption: str
    version_id: str

    def to_contract(self, *, tenant_id: str, source_id: str, created_at: str) -> dict[str, Any]:
        return {
            "contract": "recall.artifact-ref.v1",
            "schema_version": 1,
            "tenant_id": tenant_id,
            "source_id": source_id,
            "artifact_id": self.artifact_id,
            "storage_backend": self.storage_backend,
            "object_key": self.object_key,
            "content_sha256": self.content_sha256,
            "size_bytes": self.size_bytes,
            "media_type": self.media_type,
            "encryption": self.encryption,
            "version_id": self.version_id,
            "created_at": created_at,
        }


def _scope_digest(namespace_key: bytes, label: bytes, value: str) -> str:
    return hmac.new(namespace_key, label + b"\0" + value.encode(), hashlib.sha256).hexdigest()


def _identity(
    request: ArchiveRequest,
    namespace_key: bytes,
    content_sha256: str,
) -> tuple[str, str, str, str]:
    tenant_scope = _scope_digest(namespace_key, b"tenant", request.tenant_id)
    source_scope = _scope_digest(namespace_key, b"source", request.source_id)
    message = b"\0".join((
        request.tenant_id.encode(),
        request.source_id.encode(),
        request.native_id.encode(),
        content_sha256.encode(),
    ))
    digest = hmac.new(namespace_key, b"artifact\0" + message, hashlib.sha256).hexdigest()
    return "art_" + digest[:32], f"objects/{digest[:2]}/{digest}", tenant_scope, source_scope


def _metadata(reference: ArtifactReference) -> dict[str, str]:
    return {
        "artifact_id": reference.artifact_id,
        "tenant_scope_sha256": reference.tenant_scope_sha256,
        "source_scope_sha256": reference.source_scope_sha256,
        "content_sha256": reference.content_sha256,
        "size_bytes": str(reference.size_bytes),
        "media_type_sha256": hashlib.sha256(reference.media_type.encode()).hexdigest(),
    }


def _verify_metadata(reference: ArtifactReference, actual: Any) -> None:
    expected = _metadata(reference)
    if not isinstance(actual, dict):
        raise ArchiveCorruption("archive metadata mismatch")
    for key in ("tenant_scope_sha256", "source_scope_sha256"):
        if not isinstance(actual.get(key), str) or not hmac.compare_digest(
            actual[key], expected[key],
        ):
            raise ArchiveNotFound("archive object not found")
    if actual != expected:
        raise ArchiveCorruption("archive metadata mismatch")


class _ArchiveStore:
    storage_backend = ""
    encryption = ""

    def __init__(self, *, namespace_key: bytes, maximum_bytes: int) -> None:
        if not isinstance(namespace_key, bytes) or len(namespace_key) < 32:
            raise ValueError("archive namespace key is invalid")
        if not isinstance(maximum_bytes, int) or not 1 <= maximum_bytes <= 5 * 1024**3:
            raise ValueError("archive byte bound is invalid")
        self._namespace_key = namespace_key
        self.maximum_bytes = maximum_bytes

    def _reference(
        self,
        request: ArchiveRequest,
        *,
        content_sha256: str,
        size_bytes: int,
        version_id: str,
    ) -> ArtifactReference:
        artifact_id, object_key, tenant_scope, source_scope = _identity(
            request, self._namespace_key, content_sha256,
        )
        return ArtifactReference(
            artifact_id=artifact_id,
            tenant_scope_sha256=tenant_scope,
            source_scope_sha256=source_scope,
            storage_backend=self.storage_backend,
            object_key=object_key,
            content_sha256=content_sha256,
            size_bytes=size_bytes,
            media_type=request.media_type,
            encryption=self.encryption,
            version_id=version_id,
        )

    def _authorize(
        self,
        reference: ArtifactReference,
        *,
        tenant_id: str,
        source_id: str,
    ) -> None:
        if not isinstance(tenant_id, str) or not isinstance(source_id, str):
            raise ArchiveNotFound("archive object not found")
        if not hmac.compare_digest(
            reference.tenant_scope_sha256,
            _scope_digest(self._namespace_key, b"tenant", tenant_id),
        ):
            raise ArchiveNotFound("archive object not found")
        if not hmac.compare_digest(
            reference.source_scope_sha256,
            _scope_digest(self._namespace_key, b"source", source_id),
        ):
            raise ArchiveNotFound("archive object not found")

    def _validate_reference(self, reference: ArtifactReference) -> None:
        if (
            not isinstance(reference, ArtifactReference)
            or reference.storage_backend != self.storage_backend
            or not ARTIFACT_ID_RE.fullmatch(reference.artifact_id)
            or not OBJECT_KEY_RE.fullmatch(reference.object_key)
            or not SHA256_RE.fullmatch(reference.content_sha256)
            or not SHA256_RE.fullmatch(reference.tenant_scope_sha256)
            or not SHA256_RE.fullmatch(reference.source_scope_sha256)
            or not isinstance(reference.size_bytes, int)
            or not 0 <= reference.size_bytes <= self.maximum_bytes
            or not isinstance(reference.version_id, str)
            or not reference.version_id
        ):
            raise ArchiveNotFound("archive object not found")

    def put(self, request: ArchiveRequest) -> ArtifactReference:
        request.validate(self.maximum_bytes)
        digest = hashlib.sha256(request.payload).hexdigest()
        return self.put_stream(
            request,
            stream=_BytesReader(request.payload),
            size_bytes=len(request.payload),
            content_sha256=digest,
        )

    def put_raw(
        self,
        *,
        tenant_id: str,
        source_id: str,
        native_id: str,
        payload: bytes,
        media_type: str,
        created_at: str,
    ) -> dict[str, Any]:
        """Connector-facing gateway: write once, then return the closed public reference."""
        reference = self.put(ArchiveRequest(
            tenant_id=tenant_id,
            source_id=source_id,
            native_id=native_id,
            media_type=media_type,
            payload=payload,
        ))
        return reference.to_contract(
            tenant_id=tenant_id,
            source_id=source_id,
            created_at=created_at,
        )

    def delete_raw(self, value: dict[str, Any]) -> bool:
        """Delete one contract reference without exposing archive namespace material."""
        reference = self._from_contract(value)
        try:
            self.read(
                reference,
                tenant_id=value["tenant_id"],
                source_id=value["source_id"],
            )
        except ArchiveNotFound:
            return False
        return self.delete(
            reference,
            tenant_id=value["tenant_id"],
            source_id=value["source_id"],
        )

    def delete_internal_raw(self, value: dict[str, Any]) -> bool:
        """Delete a trusted persisted reference without downloading its payload.

        This path is intentionally limited to server-owned references recovered
        from the database. Connector-facing references must continue through
        ``delete_raw`` and its full integrity check.
        """

        reference = self._from_contract(value)
        try:
            return self.delete(
                reference,
                tenant_id=value["tenant_id"],
                source_id=value["source_id"],
            )
        except ArchiveNotFound:
            return False

    def read_raw(self, value: dict[str, Any]) -> bytes:
        """Resolve one closed reference without exposing namespace-key material."""
        reference = self._from_contract(value)
        return self.read(
            reference,
            tenant_id=value["tenant_id"],
            source_id=value["source_id"],
        )

    def _from_contract(self, value: dict[str, Any]) -> ArtifactReference:
        try:
            reference = validate_contract(value, expected="recall.artifact-ref.v1")
        except ContractError:
            raise ArchiveNotFound("archive object not found") from None
        if reference["storage_backend"] != self.storage_backend:
            raise ArchiveNotFound("archive object not found")
        internal = ArtifactReference(
            artifact_id=reference["artifact_id"],
            tenant_scope_sha256=_scope_digest(
                self._namespace_key, b"tenant", reference["tenant_id"],
            ),
            source_scope_sha256=_scope_digest(
                self._namespace_key, b"source", reference["source_id"],
            ),
            storage_backend=reference["storage_backend"],
            object_key=reference["object_key"],
            content_sha256=reference["content_sha256"],
            size_bytes=reference["size_bytes"],
            media_type=reference["media_type"],
            encryption=reference["encryption"],
            version_id=reference["version_id"],
        )
        return internal


class _BytesReader:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._offset = 0

    def read(self, size: int) -> bytes:
        value = self._payload[self._offset:self._offset + size]
        self._offset += len(value)
        return value


def _read_bounded(
    stream: BinaryIO | _BytesReader,
    *,
    size_bytes: int,
    content_sha256: str,
    maximum_bytes: int,
) -> bytes:
    if not isinstance(size_bytes, int) or not 0 <= size_bytes <= maximum_bytes:
        raise ValueError("archive payload exceeds byte bound")
    if not isinstance(content_sha256, str) or not SHA256_RE.fullmatch(content_sha256):
        raise ValueError("archive content digest is invalid")
    chunks: list[bytes] = []
    remaining = size_bytes
    while remaining:
        chunk = stream.read(min(1_048_576, remaining))
        if not isinstance(chunk, bytes):
            raise ValueError("archive stream must return bytes")
        if not chunk:
            raise ArchiveCorruption("archive stream ended before declared size")
        chunks.append(chunk)
        remaining -= len(chunk)
    if stream.read(1):
        raise ValueError("archive payload exceeds declared size")
    payload = b"".join(chunks)
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), content_sha256):
        raise ArchiveCorruption("archive content digest mismatch")
    return payload


class FilesystemArchiveStore(_ArchiveStore):
    storage_backend = "filesystem"
    encryption = "filesystem-owner-only"

    def __init__(
        self,
        root: Path,
        *,
        namespace_key: bytes,
        maximum_bytes: int = DEFAULT_MAXIMUM_BYTES,
    ) -> None:
        super().__init__(namespace_key=namespace_key, maximum_bytes=maximum_bytes)
        root = Path(root)
        absolute = root if root.is_absolute() else Path.cwd() / root
        for candidate in (absolute, *absolute.parents):
            if candidate.is_symlink():
                raise ValueError("archive root cannot traverse a symlink")
        root.mkdir(parents=True, mode=0o700, exist_ok=True)
        root.chmod(0o700)
        if not root.is_dir() or stat.S_IMODE(root.stat().st_mode) != 0o700:
            raise ValueError("archive root must be an owner-only directory")
        self.root = root.resolve()

    @staticmethod
    def _ensure_private_directory(path: Path) -> None:
        try:
            path.mkdir(mode=0o700, exist_ok=True)
            details = path.lstat()
            if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
                raise ArchiveCorruption("archive object path is unsafe")
            path.chmod(0o700)
        except ArchiveCorruption:
            raise
        except OSError as error:
            raise ArchiveCorruption("archive object path is unsafe") from error

    def put_stream(
        self,
        request: ArchiveRequest,
        stream: BinaryIO | _BytesReader,
        *,
        size_bytes: int,
        content_sha256: str,
    ) -> ArtifactReference:
        request.validate(self.maximum_bytes)
        payload = _read_bounded(
            stream,
            size_bytes=size_bytes,
            content_sha256=content_sha256,
            maximum_bytes=self.maximum_bytes,
        )
        reference = self._reference(
            request,
            content_sha256=content_sha256,
            size_bytes=size_bytes,
            version_id="fs-" + content_sha256[:32],
        )
        directory = self.root / reference.object_key
        relative = directory.relative_to(self.root)
        current = self.root
        for component in relative.parts:
            current = current / component
            self._ensure_private_directory(current)
        metadata = json.dumps(_metadata(reference), sort_keys=True, separators=(",", ":")).encode()
        self._publish(directory / "data", payload)
        self._publish(directory / "metadata.json", metadata)
        return reference

    @staticmethod
    def _publish(path: Path, payload: bytes) -> None:
        if path.is_symlink():
            raise ArchiveCorruption("archive object path is unsafe")
        if path.exists():
            existing = path.read_bytes()
            if not hmac.compare_digest(hashlib.sha256(existing).digest(), hashlib.sha256(payload).digest()):
                raise ArchiveCorruption("archive object conflicts with existing version")
            return
        descriptor, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb", closefd=True) as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, path)
            path.chmod(0o600)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def read(
        self,
        reference: ArtifactReference,
        *,
        tenant_id: str,
        source_id: str,
    ) -> bytes:
        self._validate_reference(reference)
        self._authorize(reference, tenant_id=tenant_id, source_id=source_id)
        directory = self.root / reference.object_key
        data_path = directory / "data"
        metadata_path = directory / "metadata.json"
        if data_path.is_symlink() or metadata_path.is_symlink():
            raise ArchiveCorruption("archive object path is unsafe")
        try:
            metadata = json.loads(metadata_path.read_bytes())
            payload = data_path.read_bytes()
        except (FileNotFoundError, json.JSONDecodeError) as error:
            raise ArchiveNotFound("archive object not found") from error
        _verify_metadata(reference, metadata)
        if len(payload) != reference.size_bytes or not hmac.compare_digest(
            hashlib.sha256(payload).hexdigest(), reference.content_sha256,
        ):
            raise ArchiveCorruption("archive content digest mismatch")
        return payload

    def delete(
        self,
        reference: ArtifactReference,
        *,
        tenant_id: str,
        source_id: str,
    ) -> bool:
        self._validate_reference(reference)
        self._authorize(reference, tenant_id=tenant_id, source_id=source_id)
        directory = self.root / reference.object_key
        removed = False
        for name in ("data", "metadata.json"):
            path = directory / name
            if path.is_symlink():
                raise ArchiveCorruption("archive object path is unsafe")
            try:
                path.unlink()
                removed = True
            except FileNotFoundError:
                pass
        try:
            directory.rmdir()
            directory.parent.rmdir()
        except OSError:
            pass
        return removed


class S3ArchiveStore(_ArchiveStore):
    storage_backend = "s3"

    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: str,
        namespace_key: bytes,
        client: S3Client,
        maximum_bytes: int = DEFAULT_MAXIMUM_BYTES,
        kms_key_id: str | None = None,
        compatibility_profile: str = "aws",
    ) -> None:
        super().__init__(namespace_key=namespace_key, maximum_bytes=maximum_bytes)
        parsed = urlsplit(endpoint_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("S3 endpoint must use credential-free HTTPS")
        if parsed.query or parsed.fragment:
            raise ValueError("S3 endpoint must use credential-free HTTPS")
        if compatibility_profile not in {"aws", "aws-unversioned", "r2"}:
            raise ValueError("S3 compatibility profile is invalid")
        if compatibility_profile == "r2" and (
            not R2_HOST_RE.fullmatch(parsed.hostname)
            or parsed.port is not None
            or parsed.path not in {"", "/"}
        ):
            raise ValueError("R2 endpoint is invalid")
        if not BUCKET_RE.fullmatch(bucket):
            raise ValueError("S3 bucket is invalid")
        if kms_key_id is not None and (not isinstance(kms_key_id, str) or not kms_key_id):
            raise ValueError("S3 KMS key is invalid")
        if compatibility_profile == "r2" and kms_key_id is not None:
            raise ValueError("R2 does not support an S3 KMS key")
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.client = client
        self.kms_key_id = kms_key_id
        self.compatibility_profile = compatibility_profile
        self.encryption = "sse-kms" if kms_key_id else "sse-s3"

    def _version_id(self, content_sha256: str, response: dict[str, Any]) -> str:
        if self.compatibility_profile == "r2":
            return "r2-sha256-" + content_sha256
        if self.compatibility_profile == "aws-unversioned":
            return "s3-sha256-" + content_sha256
        version_id = response.get("VersionId")
        if not isinstance(version_id, str) or not version_id:
            raise ArchiveError("archive bucket versioning is required")
        return version_id

    def _version_kwargs(self, reference: ArtifactReference) -> dict[str, str]:
        if self.compatibility_profile == "aws":
            return {"VersionId": reference.version_id}
        prefix = (
            "r2-sha256-"
            if self.compatibility_profile == "r2"
            else "s3-sha256-"
        )
        expected = prefix + reference.content_sha256
        if not hmac.compare_digest(reference.version_id, expected):
            raise ArchiveNotFound("archive object not found")
        return {}

    def _existing_reference(
        self,
        request: ArchiveRequest,
        *,
        content_sha256: str,
        size_bytes: int,
        response: dict[str, Any],
    ) -> ArtifactReference:
        reference = self._reference(
            request,
            content_sha256=content_sha256,
            size_bytes=size_bytes,
            version_id=self._version_id(content_sha256, response),
        )
        if (
            response.get("ContentLength") != size_bytes
            or response.get("Metadata") != _metadata(reference)
        ):
            raise ArchiveCorruption("archive object conflicts with existing version")
        return reference

    def put_stream(
        self,
        request: ArchiveRequest,
        stream: BinaryIO | _BytesReader,
        *,
        size_bytes: int,
        content_sha256: str,
    ) -> ArtifactReference:
        request.validate(self.maximum_bytes)
        payload = _read_bounded(
            stream,
            size_bytes=size_bytes,
            content_sha256=content_sha256,
            maximum_bytes=self.maximum_bytes,
        )
        pending = self._reference(
            request,
            content_sha256=content_sha256,
            size_bytes=size_bytes,
            version_id="pending",
        )
        try:
            existing = self.client.head_object(Bucket=self.bucket, Key=pending.object_key)
        except ArchiveNotFound:
            pass
        except Exception as error:
            raise ArchiveError("archive provider request failed") from error
        else:
            return self._existing_reference(
                request,
                content_sha256=content_sha256,
                size_bytes=size_bytes,
                response=existing,
            )
        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": pending.object_key,
            "Body": payload,
            "ContentLength": size_bytes,
            "ContentType": request.media_type,
            "Metadata": _metadata(pending),
        }
        if self.compatibility_profile != "aws":
            # Versionless providers use opaque content-addressed keys plus a
            # conditional write to preserve the immutable archive contract.
            kwargs["IfNoneMatch"] = "*"
        if self.compatibility_profile != "r2":
            kwargs["ServerSideEncryption"] = (
                "aws:kms" if self.kms_key_id else "AES256"
            )
            kwargs["ChecksumSHA256"] = b64encode(
                hashlib.sha256(payload).digest()
            ).decode()
            if self.kms_key_id:
                kwargs["SSEKMSKeyId"] = self.kms_key_id
        try:
            response = self.client.put_object(**kwargs)
        except Exception as error:
            if self.compatibility_profile != "aws":
                try:
                    existing = self.client.head_object(
                        Bucket=self.bucket, Key=pending.object_key,
                    )
                    return self._existing_reference(
                        request,
                        content_sha256=content_sha256,
                        size_bytes=size_bytes,
                        response=existing,
                    )
                except ArchiveCorruption:
                    raise
                except Exception:
                    pass
            raise ArchiveError("archive provider request failed") from error
        if self.compatibility_profile == "aws":
            return self._reference(
                request,
                content_sha256=content_sha256,
                size_bytes=size_bytes,
                version_id=self._version_id(content_sha256, response),
            )
        try:
            stored = self.client.head_object(
                Bucket=self.bucket, Key=pending.object_key,
            )
        except Exception as error:
            raise ArchiveError("archive provider request failed") from error
        return self._existing_reference(
            request,
            content_sha256=content_sha256,
            size_bytes=size_bytes,
            response=stored,
        )

    def read(
        self,
        reference: ArtifactReference,
        *,
        tenant_id: str,
        source_id: str,
    ) -> bytes:
        self._validate_reference(reference)
        self._authorize(reference, tenant_id=tenant_id, source_id=source_id)
        version_kwargs = self._version_kwargs(reference)
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=reference.object_key,
                **version_kwargs,
            )
        except ArchiveNotFound:
            raise
        except Exception as error:
            raise ArchiveError("archive provider request failed") from error
        _verify_metadata(reference, response.get("Metadata"))
        body = response.get("Body")
        if body is None or response.get("ContentLength") != reference.size_bytes:
            raise ArchiveCorruption("archive metadata mismatch")
        payload = _read_bounded(
            body,
            size_bytes=reference.size_bytes,
            content_sha256=reference.content_sha256,
            maximum_bytes=self.maximum_bytes,
        )
        return payload

    def delete(
        self,
        reference: ArtifactReference,
        *,
        tenant_id: str,
        source_id: str,
    ) -> bool:
        self._validate_reference(reference)
        self._authorize(reference, tenant_id=tenant_id, source_id=source_id)
        version_kwargs = self._version_kwargs(reference)
        try:
            existing = self.client.head_object(
                Bucket=self.bucket,
                Key=reference.object_key,
                **version_kwargs,
            )
        except ArchiveNotFound:
            return False
        except Exception as error:
            raise ArchiveError("archive provider request failed") from error
        _verify_metadata(reference, existing.get("Metadata"))
        if existing.get("ContentLength") != reference.size_bytes:
            raise ArchiveCorruption("archive metadata mismatch")
        try:
            self.client.delete_object(
                Bucket=self.bucket,
                Key=reference.object_key,
                **version_kwargs,
            )
        except Exception as error:
            raise ArchiveError("archive provider request failed") from error
        return True
