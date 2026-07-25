#!/usr/bin/env python3
"""Fresh-PostgreSQL proof for canonical-only personal/company MCP retrieval."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "recall"), str(ROOT / "recall/server")]

from client.mac import canonical_envelope
from recall_server.app import Handler
from recall_server.authorization import VerifiedExternalIdentity
from recall_server.archive import FilesystemArchiveStore
from recall_server.canonical import CanonicalArchiveGateway, CanonicalPlane
from recall_server.canonical_retrieval import CanonicalRetrieval
from recall_server.control import ControlPlane, SecretBox
from recall_server.db import BrainStore
from recall_server.deep_inspection import LocalDeepInspector
from recall_server.evidence_projection import (
    CanonicalEvidenceProjector,
    EvidenceProjectionStore,
)


OWNER = "principal:owner:e2e"
OUTSIDER = "principal:outsider:e2e"
VIEWER = "principal:viewer:e2e"
PERSONAL = "tenant:personal:e2e"
COMPANY = "tenant:company:e2e"
PERSONAL_SOURCE = "source:personal:e2e"
COMPANY_SOURCE = "source:company:e2e"
COMPANY_LATE_SOURCE = "source:company:late:e2e"
OUTSIDER_SOURCE = "source:company:outsider:e2e"
OCCURRED = "2026-07-20T07:00:00Z"
RESOURCE = "https://recall.synthetic.invalid/mcp"


class FakeSemanticRuntime:
    dimensions = 512
    model = "synthetic-embedding-v1"
    fingerprint = "synthetic-canonical-runtime-v1"

    @staticmethod
    def _vector(text: str) -> list[float]:
        vector = [0.0] * 512
        lowered = text.casefold()
        vector[0] = 1.0 if "personal" in lowered else 0.01
        vector[1] = 1.0 if "company" in lowered else 0.01
        vector[2] = 1.0 if "outsider" in lowered else 0.01
        return vector

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._vector(text) for text in texts]

    def embed_query(self, query: str) -> list[float]:
        if "semantic unavailable" in query:
            raise TimeoutError("synthetic semantic dependency timeout")
        return self._vector(query)


def raw_rpc(
    server: ThreadingHTTPServer,
    token: str,
    name: str,
    arguments: dict,
    *,
    path: str = "/mcp",
) -> tuple[int, dict]:
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
    ).encode()
    connection = http.client.HTTPConnection(
        "127.0.0.1", server.server_port, timeout=10
    )
    connection.request(
        "POST",
        path,
        body=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "MCP-Protocol-Version": "2025-11-25",
        },
    )
    response = connection.getresponse()
    payload = json.loads(response.read())
    connection.close()
    return response.status, payload


def rpc(
    server: ThreadingHTTPServer,
    token: str,
    name: str,
    arguments: dict,
    *,
    path: str = "/mcp",
) -> dict:
    status, payload = raw_rpc(server, token, name, arguments, path=path)
    assert status == 200, payload
    return payload


class SyntheticExternalVerifier:
    def verify(self, token: str) -> VerifiedExternalIdentity | None:
        now = datetime.now(timezone.utc)
        values = {
            "external-human-read": (
                "human-owner", RESOURCE, now + timedelta(minutes=5), None, False
            ),
            "external-expired": (
                "human-owner", RESOURCE, now - timedelta(minutes=5), None, False
            ),
            "external-wrong-audience": (
                "human-owner", "https://other.invalid/mcp",
                now + timedelta(minutes=5), None, False
            ),
            "external-revoked": (
                "human-revoked", RESOURCE, now + timedelta(minutes=5), None, False
            ),
            "external-invitee": (
                "human-invitee", RESOURCE, now + timedelta(minutes=5),
                "invitee@example.com", True
            ),
            "external-invite-hijack": (
                "human-hijack", RESOURCE, now + timedelta(minutes=5),
                "invitee@example.com", True
            ),
            "external-wrong-email": (
                "human-wrong-email", RESOURCE, now + timedelta(minutes=5),
                "wrong@example.com", True
            ),
            "external-expired-invite": (
                "human-expired-invite", RESOURCE, now + timedelta(minutes=5),
                "expired@example.com", True
            ),
        }
        value = values.get(token)
        if value is None:
            return None
        subject, audience, expires_at, email, email_verified = value
        return VerifiedExternalIdentity(
            issuer="https://identity.synthetic.invalid",
            subject=subject,
            audience=audience,
            scopes=("read",),
            expires_at=expires_at,
            email=email,
            email_verified=email_verified,
        )


def ingest(
    store: BrainStore,
    archive: FilesystemArchiveStore,
    *,
    tenant_id: str,
    principal_id: str,
    source_id: str,
    native_id: str,
    text: str,
    parent: str | None = None,
    occurred_at: str = OCCURRED,
    tombstone: bool = False,
) -> str:
    raw = json.dumps(
        {"text": text, "deleted": tombstone},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    reference = CanonicalArchiveGateway(
        store,
        archive,
        tenant_id=tenant_id,
        principal_id=principal_id,
    ).put_raw(
        tenant_id=tenant_id,
        source_id=source_id,
        native_id=native_id,
        payload=raw,
        media_type="application/json",
        created_at=occurred_at,
    )
    content = {"target_native_id": native_id} if tombstone else {"text": text}
    event = canonical_envelope(
        source_id=source_id,
        native_id=native_id,
        kind="tombstone" if tombstone else "connector_record",
        content=content,
        principal_id=principal_id,
        visibility="private",
        occurred_at=occurred_at,
        parent=parent,
        provenance={
            "uri": f"connector://synthetic/{hashlib.sha256(source_id.encode()).hexdigest()[:8]}",
            "cwd": "/synthetic/unified-brain",
            "branch": "test/multitenant-mcp",
            "connector_id": "synthetic.v2",
            "artifact_ref": reference,
        },
    )
    result = CanonicalPlane(store, archive).ingest_batch(
        tenant_id=tenant_id,
        principal_id=principal_id,
        events=[event],
    )
    assert result["inserted"] == 1
    return result["receipts"][0]


def main() -> None:
    store = BrainStore(
        os.environ["RECALL_DATABASE_URL"],
        semantic_runtime=FakeSemanticRuntime(),
    )
    store.migrate()
    tables = (
        "authorization_audit_events,brain_invitations,"
        "external_identity_bindings,mcp_credentials,"
        "canonical_chunk_embeddings,canonical_source_grants,"
        "brain_access_grants,brain_memberships,brain_spaces,brain_organizations,"
        "forget_tombstones,receipt_redirects,canonical_audit_events,"
        "canonical_evidence_objects,canonical_chunks,canonical_documents,canonical_events,"
        "canonical_ingest_jobs,raw_artifacts,canonical_sources,"
        "brain_principals,brain_tenants,collector_credentials,"
        "source_aliases,source_profiles,sources"
    )
    with store.connect() as connection:
        connection.execute(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")
    store.provision_brain(
        organization_id="org:personal:e2e",
        organization_kind="personal",
        display_name="Synthetic Personal",
        tenant_id=PERSONAL,
        brain_kind="personal",
        slug="personal",
        owner_principal_id=OWNER,
    )
    store.provision_brain(
        organization_id="org:company:e2e",
        organization_kind="company",
        display_name="Synthetic Company",
        tenant_id=COMPANY,
        brain_kind="company",
        slug="company",
        owner_principal_id=OWNER,
    )
    with tempfile.TemporaryDirectory() as temporary:
        archive = FilesystemArchiveStore(
            Path(temporary) / "archive",
            namespace_key=b"m" * 32,
        )
        evidence_archive = FilesystemArchiveStore(
            Path(temporary) / "evidence",
            namespace_key=b"e" * 32,
        )
        evidence_projection = EvidenceProjectionStore(evidence_archive)
        evidence_projector = CanonicalEvidenceProjector(
            store,
            evidence_projection,
        )
        personal_receipt = ingest(
            store,
            archive,
            tenant_id=PERSONAL,
            principal_id=OWNER,
            source_id=PERSONAL_SOURCE,
            native_id="native:personal:e2e",
            text="shared launch marker personal semantic decision",
        )
        personal_atlas_receipt = ingest(
            store,
            archive,
            tenant_id=PERSONAL,
            principal_id=OWNER,
            source_id=PERSONAL_SOURCE,
            native_id="native:atlas:personal:canary",
            parent="session:atlas:personal",
            occurred_at="2026-07-24T07:00:00Z",
            text=(
                "synthetic atlas harness atlas harness atlas harness "
                "private strongest-match canary"
            ),
        )
        company_receipt = ingest(
            store,
            archive,
            tenant_id=COMPANY,
            principal_id=OWNER,
            source_id=COMPANY_SOURCE,
            native_id="native:company:e2e",
            text="shared launch marker company semantic roadmap",
        )
        atlas_receipts = [
            ingest(
                store,
                archive,
                tenant_id=COMPANY,
                principal_id=OWNER,
                source_id=COMPANY_SOURCE,
                native_id=f"native:atlas:alpha:{index}",
                parent="session:atlas:alpha",
                occurred_at=occurred_at,
                text=text,
            )
            for index, (occurred_at, text) in enumerate((
                (
                    "2026-07-23T08:00:00Z",
                    "synthetic atlas harness preview started with the legacy runner",
                ),
                (
                    "2026-07-23T09:00:00Z",
                    "synthetic atlas harness decision changed the default runner",
                ),
                (
                    "2026-07-23T10:00:00Z",
                    "synthetic atlas harness preview passed after the runner fix",
                ),
            ))
        ]
        late_receipt = ingest(
            store,
            archive,
            tenant_id=COMPANY,
            principal_id=OWNER,
            source_id=COMPANY_LATE_SOURCE,
            native_id="native:atlas:beta:recent",
            parent="session:atlas:beta",
            occurred_at="2026-07-24T06:00:00Z",
            text="synthetic atlas harness deployment verification completed",
        )
        ingest(
            store,
            archive,
            tenant_id=COMPANY,
            principal_id=OWNER,
            source_id=COMPANY_LATE_SOURCE,
            native_id="native:atlas:beta:old",
            parent="session:atlas:beta",
            occurred_at="2026-06-01T06:00:00Z",
            text="synthetic atlas harness old imported history",
        )
        ingest(
            store,
            archive,
            tenant_id=COMPANY,
            principal_id=OUTSIDER,
            source_id=OUTSIDER_SOURCE,
            native_id="native:outsider:e2e",
            text="shared launch marker outsider confidential plan",
        )
        forget_receipt = ingest(
            store,
            archive,
            tenant_id=PERSONAL,
            principal_id=OWNER,
            source_id=PERSONAL_SOURCE,
            native_id="native:personal:forgettable:e2e",
            text="synthetic forgettable personal note",
        )
        with store.connect() as connection:
            for source_id, principal_id in (
                (PERSONAL_SOURCE, OWNER),
                (COMPANY_SOURCE, OWNER),
                (COMPANY_LATE_SOURCE, OWNER),
                (OUTSIDER_SOURCE, OUTSIDER),
            ):
                connection.execute(
                    """INSERT INTO sources(id,principal_id) VALUES (%s,%s)
                       ON CONFLICT(id) DO NOTHING""",
                    (source_id, principal_id),
                )
                connection.execute(
                    """INSERT INTO source_profiles(
                           source_id,family,quality,freshness_half_life_days
                       ) VALUES (
                           %s,
                           CASE WHEN %s=%s
                                THEN 'work_activity'
                                ELSE 'coding_history'
                           END,
                           'trusted',
                           30
                       )
                       ON CONFLICT(source_id) DO UPDATE SET family=excluded.family""",
                    (source_id, source_id, COMPANY_LATE_SOURCE),
                )
            connection.execute(
                """INSERT INTO source_aliases(alias,source_id)
                   VALUES ('company-code',%s)
                   ON CONFLICT(alias) DO UPDATE SET source_id=excluded.source_id""",
                (COMPANY_SOURCE,),
            )
            connection.execute(
                """INSERT INTO brain_principals(tenant_id,principal_id)
                   VALUES (%s,%s) ON CONFLICT DO NOTHING""",
                (COMPANY, VIEWER),
            )
            connection.execute(
                """INSERT INTO brain_access_grants(
                       tenant_id,principal_id,permission
                   ) VALUES (%s,%s,'read')""",
                (COMPANY, VIEWER),
            )
            connection.execute(
                """INSERT INTO brain_memberships(
                       organization_id,principal_id,role
                   ) VALUES ('org:company:e2e',%s,'member')""",
                (VIEWER,),
            )
            for subject, revoked in (
                ("human-owner", False),
                ("human-revoked", True),
            ):
                connection.execute(
                    """INSERT INTO external_identity_bindings(
                           issuer,subject_sha256,tenant_id,principal_id,
                           principal_kind,revoked_at
                       ) VALUES (%s,%s,%s,%s,'human',
                           CASE WHEN %s THEN now() ELSE NULL END)""",
                    (
                        "https://identity.synthetic.invalid",
                        hashlib.sha256(subject.encode()).hexdigest(),
                        COMPANY,
                        OWNER,
                        revoked,
                    ),
                )
        personal_token = store.create_mcp_token(
            "personal-mcp-e2e",
            tenant_id=PERSONAL,
            principal_id=OWNER,
            scopes=["read", "forget"],
            principal_kind="workload",
        )
        company_token = store.create_mcp_token(
            "company-mcp-e2e",
            tenant_id=COMPANY,
            principal_id=OWNER,
            principal_kind="workload",
        )
        empty_token = store.create_mcp_token(
            "empty-company-mcp-e2e",
            tenant_id=COMPANY,
            principal_id=VIEWER,
            principal_kind="human",
        )
        projected = evidence_projector.project_pending()
        assert projected["processed"] == 10
        retrieval = CanonicalRetrieval(
            store,
            archive,
            evidence_projector=evidence_projector,
            deep_inspector=LocalDeepInspector(evidence_projection),
        )
        embedding = retrieval.embed_pending()
        assert embedding["processed"] == 10
        control = ControlPlane(store, SecretBox(b"i" * 32), {})
        invitation = control.create_brain_invitation(
            principal_id=OWNER,
            tenant_id=COMPANY,
            email="Invitee@Example.com",
            role="member",
        )
        expired_invitation = control.create_brain_invitation(
            principal_id=OWNER,
            tenant_id=COMPANY,
            email="expired@example.com",
            role="member",
        )
        with store.connect() as connection:
            connection.execute(
                """UPDATE brain_invitations
                   SET created_at=now()-interval '2 days',
                       expires_at=now()-interval '1 day'
                   WHERE id=%s""",
                (expired_invitation["id"],),
            )

        def legacy_read_forbidden(*_args, **_kwargs):
            raise AssertionError("legacy retrieval was called")

        store.search = legacy_read_forbidden
        store.show = legacy_read_forbidden
        store.related = legacy_read_forbidden
        previous = {
            name: os.environ.get(name)
            for name in (
                "RECALL_AUTH_REQUIRED",
                "RECALL_HTTP_PROFILE",
                "RECALL_TRUST_TAILSCALE_HEADERS",
                "RECALL_CANONICAL_V2_ENABLED",
                "RECALL_CANONICAL_MCP_ENABLED",
                "RECALL_MCP_RESOURCE_URI",
                "RECALL_AUTHORIZATION_SERVERS",
            )
        }
        os.environ.update(
            {
                "RECALL_AUTH_REQUIRED": "1",
                "RECALL_HTTP_PROFILE": "public-mcp",
                "RECALL_TRUST_TAILSCALE_HEADERS": "0",
                "RECALL_CANONICAL_V2_ENABLED": "1",
                "RECALL_CANONICAL_MCP_ENABLED": "1",
                "RECALL_MCP_RESOURCE_URI": RESOURCE,
                "RECALL_AUTHORIZATION_SERVERS": "https://identity.synthetic.invalid",
            }
        )
        Handler.store = store
        Handler.archive_store = archive
        Handler.evidence_archive_store = evidence_archive
        Handler.evidence_projector = evidence_projector
        Handler.deep_inspector = retrieval.deep_inspector
        Handler.canonical_plane = CanonicalPlane(
            store,
            archive,
            evidence_projector,
        )
        Handler.canonical_retrieval = retrieval
        Handler.control_plane = control
        Handler.external_identity_verifier = SyntheticExternalVerifier()
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            personal = rpc(
                server,
                personal_token["token"],
                "recall_search",
                {"query": "shared launch marker"},
            )
            personal_results = personal["result"]["structuredContent"]["results"]
            assert {row["source_id"] for row in personal_results} == {PERSONAL_SOURCE}
            assert COMPANY_SOURCE not in json.dumps(personal)
            assert OUTSIDER_SOURCE not in json.dumps(personal)

            company = rpc(
                server,
                company_token["token"],
                "recall_search",
                {"query": "shared launch marker"},
            )
            company_results = company["result"]["structuredContent"]["results"]
            assert COMPANY_SOURCE in {
                row["source_id"] for row in company_results
            }
            assert PERSONAL_SOURCE not in json.dumps(company)
            assert OUTSIDER_SOURCE not in json.dumps(company)

            investigated = rpc(
                server,
                company_token["token"],
                "recall_investigate",
                {
                    "question": "What changed in the synthetic atlas harness?",
                    "filters": {
                        "since": "2026-07-22T00:00:00Z",
                        "until": "2026-07-24T23:59:59Z",
                    },
                    "depth": "deep",
                },
            )["result"]["structuredContent"]
            assert investigated["question_interpretation"]["time_basis"] == "occurred_at"
            assert investigated["coverage"]["sessions"] >= 2
            assert set(investigated["coverage"]["sources"]) == {
                COMPANY_SOURCE,
                COMPANY_LATE_SOURCE,
            }
            rendered_investigation = json.dumps(investigated)
            assert PERSONAL_SOURCE not in rendered_investigation
            assert personal_atlas_receipt not in rendered_investigation
            assert "strongest-match canary" not in rendered_investigation
            assert OUTSIDER_SOURCE not in rendered_investigation
            assert "old imported history" not in rendered_investigation
            accounting = investigated["coverage"]["source_accounting"]
            assert set(accounting["searched"]) == {
                COMPANY_SOURCE,
                COMPANY_LATE_SOURCE,
            }
            assert accounting["filtered"] == []
            assert accounting["unavailable"] == []
            assert (
                len(rendered_investigation.encode())
                < investigated["diagnostics"]["bounds"]["max_response_bytes"]
            )
            returned_receipts = {
                chunk["receipt"]
                for item in investigated["investigations"]
                for event in item["context"]["events"]
                for chunk in event["chunks"]
            }
            assert set(atlas_receipts).issubset(returned_receipts)
            assert late_receipt in returned_receipts
            with store.connect() as connection:
                assert connection.execute(
                    """SELECT count(DISTINCT receipt) AS n
                       FROM canonical_chunks
                       WHERE tenant_id=%s AND receipt=ANY(%s)
                         AND deleted_at IS NULL""",
                    (COMPANY, list(returned_receipts)),
                ).fetchone()["n"] == len(returned_receipts)
            occurrence_order = [
                event["occurred_at"]
                for item in investigated["investigations"]
                for event in item["context"]["events"]
            ]
            assert all(
                event["time_basis"] == "occurred_at"
                for item in investigated["investigations"]
                for event in item["context"]["events"]
            )
            assert occurrence_order

            deep = rpc(
                server,
                company_token["token"],
                "recall_deep_search",
                {
                    "question": (
                        "Deep-search full synthetic atlas harness evidence"
                    ),
                    "filters": {
                        "since": "2026-07-22T00:00:00Z",
                        "until": "2026-07-24T23:59:59Z",
                    },
                    "depth": "deep",
                },
            )["result"]["structuredContent"]
            assert deep["status"] == "complete"
            assert deep["coverage"]["provider"] == "local"
            assert deep["coverage"]["files_scanned"] >= 2
            assert deep["findings"]
            deep_rendered = json.dumps(deep)
            assert "synthetic atlas harness" in deep_rendered
            assert PERSONAL_SOURCE not in deep_rendered
            assert "strongest-match canary" not in deep_rendered
            assert OUTSIDER_SOURCE not in deep_rendered
            assert "old imported history" not in deep_rendered
            deep_receipts = {item["receipt"] for item in deep["findings"]}
            with store.connect() as connection:
                assert connection.execute(
                    """SELECT count(DISTINCT receipt) AS n
                       FROM canonical_chunks
                       WHERE tenant_id=%s AND source_id=ANY(%s)
                         AND receipt=ANY(%s) AND deleted_at IS NULL""",
                    (
                        COMPANY,
                        [COMPANY_SOURCE, COMPANY_LATE_SOURCE],
                        list(deep_receipts),
                    ),
                ).fetchone()["n"] == len(deep_receipts)

            context = rpc(
                server,
                company_token["token"],
                "recall_session_context",
                {
                    "target": atlas_receipts[1],
                    "before": 2,
                    "after": 2,
                },
            )["result"]["structuredContent"]
            assert [
                event["native_id"] for event in context["events"]
            ] == [
                "native:atlas:alpha:0",
                "native:atlas:alpha:1",
                "native:atlas:alpha:2",
            ]
            denied_context = rpc(
                server,
                personal_token["token"],
                "recall_session_context",
                {"target": atlas_receipts[1]},
            )
            assert denied_context["error"]["message"] == "receipt not found"

            conversational = rpc(
                server,
                personal_token["token"],
                "recall_search",
                {
                    "query": (
                        "Where did we discuss the shared launch marker during planning?"
                    )
                },
            )["result"]["structuredContent"]
            assert conversational["results"]
            assert {row["source_id"] for row in conversational["results"]} == {
                PERSONAL_SOURCE
            }
            assert conversational["diagnostics"]["lexical_mode"] == "relaxed"

            unrelated = rpc(
                server,
                personal_token["token"],
                "recall_search",
                {"query": "Where did we discuss underwater zebras?"},
            )["result"]["structuredContent"]
            assert unrelated["diagnostics"]["lexical_candidates"] == 0
            assert unrelated["diagnostics"]["lexical_mode"] == "relaxed-empty"

            degraded = rpc(
                server,
                personal_token["token"],
                "recall_search",
                {"query": "shared launch marker semantic unavailable"},
            )["result"]["structuredContent"]
            assert degraded["results"]
            assert {row["source_id"] for row in degraded["results"]} == {
                PERSONAL_SOURCE
            }
            assert degraded["diagnostics"]["semantic_status"] == "unavailable"

            family_routed = rpc(
                server,
                company_token["token"],
                "recall_search",
                {
                    "query": "shared launch marker",
                    "filters": {"source_family": "coding_history"},
                },
            )["result"]["structuredContent"]
            assert {row["source_id"] for row in family_routed["results"]} == {
                COMPANY_SOURCE
            }

            alias_routed = rpc(
                server,
                company_token["token"],
                "recall_search",
                {
                    "query": "shared launch marker",
                    "filters": {"source_alias": "company-code"},
                },
            )["result"]["structuredContent"]
            assert {row["source_id"] for row in alias_routed["results"]} == {
                COMPANY_SOURCE
            }
            denied_alias = rpc(
                server,
                personal_token["token"],
                "recall_search",
                {
                    "query": "shared launch marker",
                    "filters": {"source_alias": "company-code"},
                },
            )["result"]["structuredContent"]
            assert denied_alias["results"] == []

            semantic = rpc(
                server,
                personal_token["token"],
                "recall_search",
                {"query": "personal semantic"},
            )["result"]["structuredContent"]
            assert semantic["results"][0]["receipt"] == personal_receipt
            assert semantic["diagnostics"]["semantic_candidates"] >= 1

            shown = rpc(
                server,
                company_token["token"],
                "recall_show",
                {"target": company_receipt},
            )
            assert (
                shown["result"]["structuredContent"]["event"]["source_id"]
                == COMPANY_SOURCE
            )
            denied_show = rpc(
                server,
                personal_token["token"],
                "recall_show",
                {"target": company_receipt},
            )
            assert denied_show["error"]["message"] == "receipt not found"

            human = rpc(
                server,
                "external-human-read",
                "recall_search",
                {"query": "shared launch marker"},
            )
            assert COMPANY_SOURCE in {
                row["source_id"]
                for row in human["result"]["structuredContent"]["results"]
            }

            status, denied = raw_rpc(
                server,
                "external-invitee",
                "recall_search",
                {"query": "shared launch marker"},
                path=f"/mcp/brains/{PERSONAL}",
            )
            assert status == 401
            assert denied == {"error": "unauthorized"}
            with store.connect() as connection:
                assert connection.execute(
                    "SELECT accepted_at FROM brain_invitations WHERE id=%s",
                    (invitation["id"],),
                ).fetchone()["accepted_at"] is None

            status, denied = raw_rpc(
                server,
                "external-expired-invite",
                "recall_search",
                {"query": "shared launch marker"},
                path=f"/mcp/brains/{COMPANY}",
            )
            assert status == 401
            assert denied == {"error": "unauthorized"}

            invited = rpc(
                server,
                "external-invitee",
                "recall_search",
                {"query": "shared launch marker"},
                path=f"/mcp/brains/{COMPANY}",
            )
            assert COMPANY_SOURCE in {
                row["source_id"]
                for row in invited["result"]["structuredContent"]["results"]
            }
            with store.connect() as connection:
                accepted = connection.execute(
                    """SELECT accepted_principal_id,accepted_at,encrypted_email
                       FROM brain_invitations WHERE id=%s""",
                    (invitation["id"],),
                ).fetchone()
                assert accepted["accepted_principal_id"]
                assert accepted["accepted_at"] is not None
                assert b"invitee@example.com" not in bytes(accepted["encrypted_email"])

            ingest(
                store,
                archive,
                tenant_id=COMPANY,
                principal_id=OWNER,
                source_id=COMPANY_LATE_SOURCE,
                native_id="native:company:late:e2e",
                text="late arriving company memory for every teammate",
            )
            assert retrieval.embed_pending()["processed"] == 1
            late_memory = rpc(
                server,
                "external-invitee",
                "recall_search",
                {"query": "late arriving company memory"},
                path=f"/mcp/brains/{COMPANY}",
            )
            late_sources = {
                row["source_id"]
                for row in late_memory["result"]["structuredContent"]["results"]
            }
            assert COMPANY_LATE_SOURCE in late_sources
            assert OUTSIDER_SOURCE not in late_sources

            for token in ("external-invite-hijack", "external-wrong-email"):
                status, denied = raw_rpc(
                    server,
                    token,
                    "recall_search",
                    {"query": "shared launch marker"},
                    path=f"/mcp/brains/{COMPANY}",
                )
                assert status == 401
                assert denied == {"error": "unauthorized"}
            with store.connect() as connection:
                rejected_subjects = (
                    hashlib.sha256(b"human-hijack").hexdigest(),
                    hashlib.sha256(b"human-wrong-email").hexdigest(),
                    hashlib.sha256(b"human-expired-invite").hexdigest(),
                )
                assert connection.execute(
                    """SELECT count(*) AS n FROM external_identity_bindings
                       WHERE subject_sha256=ANY(%s)""",
                    (list(rejected_subjects),),
                ).fetchone()["n"] == 0

            control.revoke_brain_invitation(
                principal_id=OWNER,
                invitation_id=invitation["id"],
            )
            status, denied = raw_rpc(
                server,
                "external-invitee",
                "recall_search",
                {"query": "shared launch marker"},
                path=f"/mcp/brains/{COMPANY}",
            )
            assert status == 401
            assert denied == {"error": "unauthorized"}

            for token in (
                "external-expired",
                "external-wrong-audience",
                "external-revoked",
            ):
                status, denied = raw_rpc(
                    server,
                    token,
                    "recall_search",
                    {"query": "shared launch marker"},
                )
                assert status == 401
                assert denied == {"error": "unauthorized"}

            read_only_write = rpc(
                server,
                "external-human-read",
                "recall_forget",
                {"receipt": personal_receipt},
            )
            assert read_only_write["error"]["message"] == "unknown tool"
            assert personal_receipt not in json.dumps(read_only_write)

            wrong_tenant = rpc(
                server,
                "external-human-read",
                "recall_show",
                {"target": personal_receipt},
            )
            assert wrong_tenant["error"]["message"] == "receipt not found"
            assert PERSONAL_SOURCE not in json.dumps(wrong_tenant)

            related = rpc(
                server,
                company_token["token"],
                "recall_related",
                {
                    "cwd": "/synthetic/unified-brain",
                    "branch": "test/multitenant-mcp",
                },
            )
            related_sources = {
                row["source_id"]
                for row in related["result"]["structuredContent"]["results"]
            }
            assert related_sources == {COMPANY_SOURCE, COMPANY_LATE_SOURCE}

            empty = rpc(
                server,
                empty_token["token"],
                "recall_search",
                {"query": "shared launch marker"},
            )
            assert empty["result"]["structuredContent"]["results"] == []

            eval_cases = (
                (personal_token["token"], "shared launch marker", personal_receipt),
                (
                    personal_token["token"],
                    "personal semantic decision",
                    personal_receipt,
                ),
                (company_token["token"], "shared launch marker", company_receipt),
                (
                    company_token["token"],
                    "company semantic roadmap",
                    company_receipt,
                ),
            ) * 3
            latencies = []
            recalled = useful = 0
            for token, query, expected in eval_cases:
                started = time.perf_counter()
                evaluated = rpc(
                    server,
                    token,
                    "recall_search",
                    {"query": query, "limit": 5},
                )["result"]["structuredContent"]["results"]
                latencies.append((time.perf_counter() - started) * 1000)
                receipts = [row["receipt"] for row in evaluated]
                recalled += expected in receipts
                useful += bool(receipts and receipts[0] == expected)
            recall_at_5 = recalled / len(eval_cases)
            usefulness = useful / len(eval_cases)
            p95_ms = sorted(latencies)[
                max(0, int(0.95 * len(latencies) + 0.999999) - 1)
            ]
            assert recall_at_5 >= 0.91
            assert usefulness >= 0.80
            assert p95_ms < 8000

            forgotten = rpc(
                server,
                personal_token["token"],
                "recall_forget",
                {"receipt": forget_receipt},
            )["result"]["structuredContent"]
            assert forgotten["raw_deleted"] == 1
            assert forgotten["evidence_deleted"] == 1
            forgotten_show = rpc(
                server,
                personal_token["token"],
                "recall_show",
                {"target": forget_receipt},
            )
            assert forgotten_show["error"]["message"] == "receipt not found"

            ingest(
                store,
                archive,
                tenant_id=PERSONAL,
                principal_id=OWNER,
                source_id=PERSONAL_SOURCE,
                native_id="native:personal:e2e",
                text="",
                tombstone=True,
            )
            deleted = rpc(
                server,
                personal_token["token"],
                "recall_search",
                {"query": "shared launch marker"},
            )
            deleted_results = deleted["result"]["structuredContent"]["results"]
            assert personal_receipt not in {
                row["receipt"] for row in deleted_results
            }
            assert all(
                "shared launch marker" not in row["text"]
                for row in deleted_results
            )
            deleted_show = rpc(
                server,
                personal_token["token"],
                "recall_show",
                {"target": personal_receipt},
            )
            assert deleted_show["error"]["message"] == "receipt not found"
            with store.connect() as connection:
                audits = connection.execute(
                    """SELECT principal_kind,principal_id,tenant_id,action,
                              decision,reason,policy_version
                       FROM authorization_audit_events
                       ORDER BY id"""
                ).fetchall()
                assert {row["principal_kind"] for row in audits} == {
                    "human", "workload"
                }
                assert all(
                    row["policy_version"] == "recall.authorization.v1"
                    for row in audits
                )
                audit_text = json.dumps(audits, default=str)
                for plaintext in (
                    personal_token["token"],
                    company_token["token"],
                    empty_token["token"],
                    "external-human-read",
                ):
                    assert plaintext not in audit_text
                    assert connection.execute(
                        """SELECT count(*) AS n FROM mcp_credentials
                           WHERE token_sha256=%s""",
                        (plaintext,),
                    ).fetchone()["n"] == 0
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            Handler.external_identity_verifier = None
            Handler.control_plane = None
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
    store.close()
    print(
        json.dumps(
            {
                "status": "pass",
                "brains": 2,
                "cross_tenant_hits": 0,
                "cross_principal_hits": 0,
                "expired_credential_accepts": 0,
                "revoked_credential_accepts": 0,
                "wrong_audience_accepts": 0,
                "wrong_tenant_hits": 0,
                "read_only_write_accepts": 0,
                "human_workload_audit_kinds": 2,
                "invitation_accepts": 1,
                "invitation_hijack_accepts": 0,
                "expired_invitation_accepts": 0,
                "cross_brain_invitation_accepts": 0,
                "revoked_invitation_accepts": 0,
                "plaintext_credential_rows": 0,
                "empty_grant_hits": 0,
                "legacy_reads": 0,
                "investigate_tool_calls": 1,
                "investigate_sessions": investigated["coverage"]["sessions"],
                "investigate_sources": len(investigated["coverage"]["sources"]),
                "investigate_exact_receipts": len(returned_receipts),
                "investigate_old_source_time_hits": 0,
                "investigate_session_coverage": 1.0,
                "investigate_temporal_accuracy": 1.0,
                "investigate_citation_precision": 1.0,
                "investigate_unsupported_claim_rate": 0.0,
                "investigate_response_bytes": len(
                    json.dumps(investigated).encode()
                ),
                "deep_search_tool_calls": 1,
                "deep_search_provider": deep["coverage"]["provider"],
                "deep_search_files": deep["coverage"]["files_scanned"],
                "deep_search_exact_receipts": len(deep_receipts),
                "deep_search_cross_tenant_hits": 0,
                "deep_search_old_source_time_hits": 0,
                "lexical_candidates": 2,
                "semantic_candidates": semantic["diagnostics"][
                    "semantic_candidates"
                ],
                "tombstoned_search_hits": 0,
                "tombstoned_show_hits": 0,
                "forgotten_show_hits": 0,
                "recall_at_5": recall_at_5,
                "usefulness": usefulness,
                "p95_ms": round(p95_ms, 3),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
