#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
import uuid
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "recall"))
sys.path.insert(0, str(ROOT / "recall/server"))

from connectors.google_workspace import GmailConnector
from connectors.sdk import ConnectorPage, ConnectorRecordV2, ConnectorRunner
from privacy.policy import PrivacyPolicy
from recall_server.archive import ArchiveNotFound, S3ArchiveStore
from recall_server.canonical import CanonicalArchiveGateway, CanonicalLifecycleError, CanonicalPlane
from recall_server.canonical_retrieval import CanonicalRetrieval
from recall_server.db import BrainStore
from tests.test_attachment_extraction import minimal_pdf


def encoded(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


class Rail:
    def __init__(self) -> None:
        self.responses = defaultdict(deque)

    def add(self, operation: str, value: dict) -> None:
        self.responses[operation].append(value)

    def run(self, operation: str, _params: dict) -> dict:
        return self.responses[operation].popleft()


class FakeBody:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0

    def read(self, size: int) -> bytes:
        chunk = self.payload[self.offset:self.offset + size]
        self.offset += len(chunk)
        return chunk


class FakeR2:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict] = {}

    def put_object(self, **kwargs) -> dict:
        identity = (kwargs["Bucket"], kwargs["Key"])
        if kwargs.get("IfNoneMatch") == "*" and identity in self.objects:
            raise RuntimeError("synthetic precondition failed")
        self.objects[identity] = dict(kwargs)
        return {"ETag": '"synthetic-etag"'}

    def head_object(self, **kwargs) -> dict:
        try:
            value = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        except KeyError as error:
            raise ArchiveNotFound("archive object not found") from error
        return {
            "ContentLength": value["ContentLength"],
            "Metadata": value["Metadata"],
            "ETag": '"synthetic-etag"',
        }

    def get_object(self, **kwargs) -> dict:
        response = self.head_object(**kwargs)
        value = self.objects[(kwargs["Bucket"], kwargs["Key"])]
        return {**response, "Body": FakeBody(value["Body"])}

    def delete_object(self, **kwargs) -> dict:
        self.objects.pop((kwargs["Bucket"], kwargs["Key"]), None)
        return {}


class FrozenPages:
    connector_id = "google.gmail"

    def __init__(self, source_id: str, records: tuple, tombstone: ConnectorRecordV2) -> None:
        self.source_id = source_id
        self.records = records
        self.tombstone = tombstone

    def pull(self, cursor: str | None) -> ConnectorPage:
        if cursor == "page-2":
            return ConnectorPage(
                records=(self.tombstone,),
                next_cursor="page-3",
                has_more=False,
            )
        return ConnectorPage(
            records=self.records,
            next_cursor="page-1" if cursor is None else "page-2",
            has_more=cursor is None,
        )


class Writer:
    def __init__(self, plane: CanonicalPlane, *, tenant_id: str, principal_id: str) -> None:
        self.plane = plane
        self.tenant_id = tenant_id
        self.principal_id = principal_id

    def ingest(self, events: list[dict]) -> dict:
        return self.plane.ingest_batch(
            tenant_id=self.tenant_id,
            principal_id=self.principal_id,
            events=events,
        )


def main() -> None:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant_id = f"tenant:gmail-attachment:{nonce}"
    principal_id = f"principal:gmail-attachment:{nonce}"
    source_id = f"source:gmail-attachment:{nonce}"
    message_id = "message-1"
    attachment_id = "attachment-1"
    marker = "synthetic searchable PDF attachment marker"
    exact_pdf = minimal_pdf(marker)
    rail = Rail()
    rail.add("gmail.messages.list", {"messages": [{"id": message_id}]})
    rail.add("gmail.messages.get", {
        "id": message_id,
        "threadId": "thread-1",
        "historyId": "100",
        "internalDate": "1784332800000",
        "snippet": "synthetic snippet",
        "payload": {
            "mimeType": "multipart/mixed",
            "headers": [
                {"name": "From", "value": "sender@example.invalid"},
                {"name": "To", "value": "owner@example.invalid"},
                {"name": "Subject", "value": "Synthetic attachment lifecycle"},
            ],
            "body": {"size": 0},
            "parts": [
                {
                    "mimeType": "text/plain",
                    "filename": "",
                    "headers": [],
                    "body": {"data": encoded(b"Synthetic parent message body")},
                },
                {
                    "mimeType": "application/pdf",
                    "filename": "synthetic.pdf",
                    "headers": [{
                        "name": "Content-Disposition",
                        "value": "attachment; filename=synthetic.pdf",
                    }],
                    "body": {"attachmentId": attachment_id, "size": len(exact_pdf)},
                },
            ],
        },
    })
    rail.add("gmail.messages.attachments.get", {
        "size": len(exact_pdf),
        "data": encoded(exact_pdf),
    })
    gmail = GmailConnector(
        rail=rail,
        source_id=source_id,
        own_addresses=("owner@example.invalid",),
        include_attachments=True,
        page_size=1,
    )
    observed = gmail.pull(None)
    if len(observed.records) != 2:
        raise RuntimeError("gmail attachment did not produce parent and child records")
    tombstone = ConnectorRecordV2.from_mapping({
        "schema_version": 2,
        "native_id": f"gmail:{message_id}",
        "native_parent_id": "gmail-thread:thread-1",
        "occurred_at": "2026-07-22T07:00:00Z",
        "content": {"kind": "communication_message.v1"},
        "provenance": {"uri": "connector://google-gmail"},
        "deleted": True,
    })

    fake_r2 = FakeR2()
    archive = S3ArchiveStore(
        bucket="recall-synthetic",
        endpoint_url="https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com",
        namespace_key=b"a" * 32,
        client=fake_r2,
        compatibility_profile="r2",
    )
    plane = CanonicalPlane(store, archive)
    gateway = CanonicalArchiveGateway(
        store,
        archive,
        tenant_id=tenant_id,
        principal_id=principal_id,
    )
    with tempfile.TemporaryDirectory(prefix="recall-gmail-attachment-e2e-") as temporary:
        runner = ConnectorRunner(
            connector=FrozenPages(source_id, observed.records, tombstone),
            brain=Writer(plane, tenant_id=tenant_id, principal_id=principal_id),
            archive=gateway,
            tenant_id=tenant_id,
            principal_id=principal_id,
            spool_path=Path(temporary) / "gmail.db",
            privacy=PrivacyPolicy(mode="scrub"),
        )
        first = runner.run_once()
        replay = runner.run_once()
        if first["acked"] != 2 or first["archived"] != 2:
            raise RuntimeError("gmail attachment canonical ingest did not commit both records")
        if replay["deduplicated"] != 2 or replay["archived"] != 0:
            raise RuntimeError("gmail attachment replay was not suppressed before archive")
        if runner.doctor()["pending"] != 0:
            raise RuntimeError("gmail attachment spool did not drain")
        if exact_pdf not in [value["Body"] for value in fake_r2.objects.values()]:
            raise RuntimeError("exact attachment bytes were not preserved in R2")
        retrieval = CanonicalRetrieval(store, archive).bind({
            "credential_kind": "mcp",
            "principal_kind": "workload",
            "role": "owner",
            "audience": "recall-mcp",
            "scopes": ["read"],
            "tenant_id": tenant_id,
            "principal_id": principal_id,
            "authorized_sources": [source_id],
        })
        attachment_hits = retrieval.search(marker, {"source_id": source_id})["results"]
        parent_hits = retrieval.search(
            "Synthetic parent message body", {"source_id": source_id},
        )["results"]
        if not attachment_hits or retrieval.show(attachment_hits[0]["receipt"]) is None:
            raise RuntimeError("searchable attachment receipt did not resolve")
        if not parent_hits:
            raise RuntimeError("parent message receipt was not searchable")
        encoded_raw = base64.b64encode(exact_pdf).decode()
        with store.connect() as connection:
            raw_model_leaks = connection.execute(
                """SELECT count(*) AS count FROM canonical_documents
                   WHERE tenant_id=%s AND text_redacted LIKE %s""",
                (tenant_id, f"%{encoded_raw}%"),
            ).fetchone()["count"]
        if raw_model_leaks:
            raise RuntimeError("raw attachment bytes crossed the model boundary")
        deleted = runner.run_once()
        if deleted["acked"] != 1 or deleted["archived"] != 1:
            raise RuntimeError("upstream Gmail tombstone did not commit")
        if (
            retrieval.search(marker, {"source_id": source_id})["results"]
            or retrieval.search(
                "Synthetic parent message body", {"source_id": source_id},
            )["results"]
        ):
            raise RuntimeError("parent tombstone did not hide attachment lineage")
        runner.close()

    parent_native_id = f"gmail:{message_id}"
    child_native_id = observed.records[1].native_id
    forget = plane.forget({
        "contract": "recall.forget-request.v1",
        "schema_version": 1,
        "tenant_id": tenant_id,
        "principal_id": principal_id,
        "source_id": source_id,
        "target_receipt": parent_hits[0]["receipt"],
        "mode": "explicit_forget",
        "reason": "owner_requested",
        "requested_at": "2026-07-22T07:00:00Z",
        "idempotency_key": "forget-gmail-attachment-" + nonce,
    })
    if forget["raw_deleted"] != 3 or forget["projections_deleted"] != 0:
        raise RuntimeError("parent forget did not cascade to attachment artifacts")
    if fake_r2.objects or retrieval.search(marker, {"source_id": source_id})["results"]:
        raise RuntimeError("forgotten attachment remained retrievable")
    for native_id, payload, media_type in (
        (parent_native_id, b"parent", "application/json"),
        (child_native_id, exact_pdf, "application/pdf"),
    ):
        try:
            gateway.put_raw(
                tenant_id=tenant_id,
                source_id=source_id,
                native_id=native_id,
                payload=payload,
                media_type=media_type,
                created_at="2026-07-22T07:01:00Z",
            )
        except CanonicalLifecycleError as error:
            if error.error_code != "archive_identity_forgotten":
                raise
        else:
            raise RuntimeError("forgotten Gmail lineage was resurrected")
    with store.connect() as connection:
        tombstones = connection.execute(
            """SELECT count(*) AS count FROM forget_tombstones
               WHERE tenant_id=%s AND source_id=%s AND status='deleted'""",
            (tenant_id, source_id),
        ).fetchone()["count"]
    if tombstones != 2:
        raise RuntimeError("parent and attachment forget fences were not both committed")
    store.close()
    print(json.dumps({
        "status": "pass",
        "records_acked": 2,
        "replays_suppressed": 2,
        "r2_exact_objects": 2,
        "upstream_tombstones": 1,
        "attachment_search_hits": len(attachment_hits),
        "resolved_receipts": 1,
        "raw_model_leaks": 0,
        "raw_objects_remaining": 0,
        "lineage_tombstones": tombstones,
        "resurrection_rejections": 2,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
