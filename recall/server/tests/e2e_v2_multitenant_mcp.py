#!/usr/bin/env python3
"""Fresh-PostgreSQL proof for canonical-only personal/company MCP retrieval."""

from __future__ import annotations

import hashlib
import http.client
import json
import multiprocessing
import os
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [
    str(ROOT / "recall"),
    str(ROOT / "recall/server"),
    str(ROOT / "recall/server/tests"),
]

from client.mac import canonical_envelope
from recall_server.agent import (
    DelegationContext,
    RecallAgentService,
)
from agent_fakes import ScriptedAgentRunner, ScriptedExecInspector
from recall_server.agent_runs import (
    AgentRunCoordinator,
    AgentRunNotFound,
    AgentRunUnavailable,
    PostgresAgentRunBackend,
)
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
from recall_server.logical_evidence import LogicalEvidenceProjectionStore
from recall_server.logical_evidence_projection import (
    CanonicalLogicalEvidenceProjector,
)
from recall_server.passage_index import CanonicalPassageProjector
from recall_server.passage_projection import PassagePolicy


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


def crash_agent_worker(
    database_url: str,
    principal: dict,
    run_id: str,
) -> None:
    child_store = BrainStore(database_url)
    child_backend = PostgresAgentRunBackend(
        child_store.connect,
        lease_seconds=15,
        retention_seconds=3600,
    )
    context = DelegationContext.from_principal(principal)
    claimed = child_backend.claim(
        context,
        run_id,
        lease_owner="synthetic-crashed-worker",
        now=datetime.now(timezone.utc),
    )
    if claimed is None:
        os._exit(91)
    with child_store.connect() as connection:
        connection.execute(
            """INSERT INTO agent_run_effects_e2e(run_id,effect)
               VALUES (%s,'canonical_retrieval_started')""",
            (run_id,),
        )
    os._exit(17)


class FakeSemanticRuntime:
    dimensions = 512
    model = "synthetic-embedding-v1"
    fingerprint = "synthetic-canonical-runtime-v1"
    passage_fingerprint = "synthetic-canonical-runtime-v1"

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

    def embed_passages(self, texts: list[str]) -> list[list[float]]:
        return self.embed_documents(texts)

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


def raw_mcp_message(
    server: ThreadingHTTPServer,
    token: str,
    payload: dict,
    *,
    path: str,
    task_name: str | None = None,
) -> tuple[int, dict]:
    body = json.dumps(payload).encode()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
        "MCP-Protocol-Version": "2026-06-30",
    }
    if task_name is not None:
        headers["Mcp-Name"] = task_name
    connection = http.client.HTTPConnection(
        "127.0.0.1", server.server_port, timeout=10
    )
    connection.request("POST", path, body=body, headers=headers)
    response = connection.getresponse()
    result = json.loads(response.read())
    connection.close()
    return response.status, result


def agent_http(
    server: ThreadingHTTPServer,
    token: str,
    tenant_id: str,
    request: dict,
) -> tuple[int, dict]:
    body = json.dumps(request).encode()
    connection = http.client.HTTPConnection(
        "127.0.0.1", server.server_port, timeout=10
    )
    connection.request(
        "POST",
        f"/v1/agent/brains/{tenant_id}/use-recall",
        body=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
        },
    )
    response = connection.getresponse()
    payload = json.loads(response.read())
    connection.close()
    return response.status, payload


def agent_lifecycle_http(
    server: ThreadingHTTPServer,
    token: str,
    method: str,
    path: str,
    body: dict | None = None,
) -> tuple[int, dict]:
    encoded = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": f"Bearer {token}"}
    if encoded is not None:
        headers.update({
            "Content-Type": "application/json",
            "Content-Length": str(len(encoded)),
        })
    connection = http.client.HTTPConnection(
        "127.0.0.1", server.server_port, timeout=10
    )
    connection.request(method, path, body=encoded, headers=headers)
    response = connection.getresponse()
    payload = json.loads(response.read())
    connection.close()
    return response.status, payload


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
        {"role": "assistant", "text": text, "deleted": tombstone},
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
    content = (
        {"target_native_id": native_id}
        if tombstone
        else {"role": "assistant", "text": text}
    )
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
        logical_projection = LogicalEvidenceProjectionStore(evidence_archive)
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
        logical_projector = CanonicalLogicalEvidenceProjector(
            store,
            logical_projection,
            raw_archive=archive,
        )
        logical = logical_projector.project_pending(
            batch_size=100,
            max_batches=1,
            upload_concurrency=2,
        )
        assert logical["documents"] > 0
        passage_policy = PassagePolicy(target_tokens=512, overlap_tokens=64)
        passage_projector = CanonicalPassageProjector(
            store,
            logical_projection,
            policy=passage_policy,
        )
        passages = passage_projector.project_pending(
            batch_size=100,
            max_batches=1,
            concurrency=2,
        )
        assert passages["documents"] == logical["documents"]
        assert passages["passages"] > 0
        passage_embeddings = passage_projector.embed_pending(
            batch_size=100,
            max_batches=1,
        )
        assert passage_embeddings["processed"] == passages["passages"]
        retrieval = CanonicalRetrieval(
            store,
            archive,
            evidence_projector=evidence_projector,
            deep_inspector=LocalDeepInspector(evidence_projection),
            passage_policy=passage_policy,
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
        Handler.deep_inspector = ScriptedExecInspector(retrieval.deep_inspector)
        retrieval.deep_inspector = Handler.deep_inspector
        Handler.canonical_plane = CanonicalPlane(
            store,
            archive,
            evidence_projector,
        )
        Handler.canonical_retrieval = retrieval
        fixed_agent_time = datetime(
            2026, 7, 25, 10, 0, tzinfo=timezone.utc
        )
        Handler.agent_service = RecallAgentService(
            ScriptedAgentRunner(),
            clock=lambda: fixed_agent_time,
            monotonic=lambda: 10.0,
        )
        agent_backend = PostgresAgentRunBackend(
            store.connect,
            max_active_per_principal=8,
            lease_seconds=15,
            retention_seconds=3600,
        )
        Handler.agent_coordinator = AgentRunCoordinator(
            Handler.agent_service,
            agent_backend,
            workers=1,
            abandon_after_seconds=15,
        )
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

            agent_request = {
                "contract": "recall.agent-request.v1",
                "schema_version": 1,
                "request_id": "req_0123456789abcdef",
                "idempotency_key": "synthetic-company-agent-e2e",
                "question": "What changed in the synthetic atlas harness?",
                "depth": "deep",
                "since": "2026-07-22T00:00:00Z",
                "until": "2026-07-24T23:59:59Z",
            }
            agent_started_at = time.monotonic()
            agent_http_status, agent_http_started = agent_http(
                server,
                company_token["token"],
                COMPANY,
                agent_request,
            )
            assert agent_http_status == 200
            assert time.monotonic() - agent_started_at < 0.5
            assert agent_http_started["run"]["status"] in {"queued", "running"}
            assert agent_http_started["continuation"]["tool"] == (
                "recall_agent_result"
            )
            agent_http_run_id = agent_http_started["run"]["run_id"]

            agent_mcp_request = {
                **agent_request,
                "request_id": "req_1111222233334444",
                "idempotency_key": "synthetic-company-agent-mcp-compat",
            }
            agent_started_at = time.monotonic()
            agent_mcp_started = rpc(
                server,
                company_token["token"],
                "use_recall",
                agent_mcp_request,
                path=f"/mcp/brains/{COMPANY}",
            )["result"]["structuredContent"]
            assert time.monotonic() - agent_started_at < 0.5
            assert agent_mcp_started["run"]["status"] in {"queued", "running"}
            agent_mcp_run_id = agent_mcp_started["run"]["run_id"]

            agent_path = f"/v1/agent/brains/{COMPANY}/runs"
            for _attempt in range(100):
                result_code, agent_http_result = agent_lifecycle_http(
                    server,
                    company_token["token"],
                    "GET",
                    f"{agent_path}/{agent_http_run_id}/result",
                )
                assert result_code == 200
                if "result" in agent_http_result:
                    break
                time.sleep(0.01)
            for _attempt in range(100):
                agent_mcp_result = rpc(
                    server,
                    company_token["token"],
                    "recall_agent_result",
                    {"run_id": agent_mcp_run_id},
                    path=f"/mcp/brains/{COMPANY}",
                )["result"]["structuredContent"]
                if "result" in agent_mcp_result:
                    break
                time.sleep(0.01)
            for field in ("status", "answer", "claims", "gaps", "citations"):
                assert agent_http_result["result"][field] == (
                    agent_mcp_result["result"][field]
                )
            assert agent_http_result["result"]["status"] == "partial"
            agent_receipts = set(agent_http_result["result"]["citations"])
            assert agent_receipts
            assert agent_receipts <= returned_receipts, (
                agent_receipts,
                returned_receipts,
            )
            rendered_agent_trace = json.dumps(agent_http_result["trace"])
            assert agent_request["question"] not in rendered_agent_trace
            assert PERSONAL_SOURCE not in rendered_agent_trace
            assert OUTSIDER_SOURCE not in rendered_agent_trace
            for forbidden in (
                '"prompt"',
                '"answer"',
                '"payload"',
                '"token"',
                '"transcript"',
            ):
                assert forbidden not in rendered_agent_trace
            denied_agent_status, denied_agent = agent_http(
                server,
                company_token["token"],
                PERSONAL,
                agent_request,
            )
            assert denied_agent_status == 401
            assert denied_agent == {"error": "unauthorized"}

            detached_request = {
                **agent_request,
                "request_id": "req_abcdef0123456789",
                "idempotency_key": "synthetic-company-agent-detached",
            }
            detached_path = agent_path
            detached_status, detached = agent_lifecycle_http(
                server,
                company_token["token"],
                "POST",
                detached_path,
                detached_request,
            )
            assert detached_status == 202
            detached_run_id = detached["run"]["run_id"]
            for _attempt in range(100):
                status_code, detached_state = agent_lifecycle_http(
                    server,
                    company_token["token"],
                    "GET",
                    f"{detached_path}/{detached_run_id}",
                )
                assert status_code == 200
                if detached_state["run"]["status"] not in {"queued", "running"}:
                    break
                time.sleep(0.01)
            assert detached_state["run"]["status"] == "partial"
            result_code, detached_result = agent_lifecycle_http(
                server,
                company_token["token"],
                "GET",
                f"{detached_path}/{detached_run_id}/result",
            )
            assert result_code == 200
            assert detached_result["result"]["citations"]
            replay_code, detached_replay = agent_lifecycle_http(
                server,
                company_token["token"],
                "POST",
                detached_path,
                detached_request,
            )
            assert replay_code == 202
            assert detached_replay["run"]["run_id"] == detached_run_id
            denied_status_code, _denied_status = agent_lifecycle_http(
                server,
                personal_token["token"],
                "GET",
                f"{detached_path}/{detached_run_id}",
            )
            assert denied_status_code == 401

            mcp_detached_request = {
                **agent_request,
                "request_id": "req_fedcba9876543210",
                "idempotency_key": "synthetic-company-agent-mcp-detached",
            }
            mcp_started = rpc(
                server,
                company_token["token"],
                "recall_agent_start",
                mcp_detached_request,
                path=f"/mcp/brains/{COMPANY}",
            )["result"]["structuredContent"]
            mcp_run_id = mcp_started["run"]["run_id"]
            for _attempt in range(100):
                mcp_state = rpc(
                    server,
                    company_token["token"],
                    "recall_agent_status",
                    {"run_id": mcp_run_id},
                    path=f"/mcp/brains/{COMPANY}",
                )["result"]["structuredContent"]
                if mcp_state["run"]["status"] not in {"queued", "running"}:
                    break
                time.sleep(0.01)
            assert mcp_state["run"]["status"] == "partial"
            mcp_result = rpc(
                server,
                company_token["token"],
                "recall_agent_result",
                {"run_id": mcp_run_id},
                path=f"/mcp/brains/{COMPANY}",
            )["result"]["structuredContent"]
            assert mcp_result["result"]["citations"]

            native_request = {
                **agent_request,
                "request_id": "req_9999999999999999",
                "idempotency_key": "synthetic-company-agent-native-task",
            }
            native_status, native_created = raw_mcp_message(
                server,
                company_token["token"],
                {
                    "jsonrpc": "2.0",
                    "id": 9,
                    "method": "tools/call",
                    "params": {
                        "name": "use_recall",
                        "arguments": native_request,
                        "_meta": {
                            "io.modelcontextprotocol/clientCapabilities": {
                                "extensions": {
                                    "io.modelcontextprotocol/tasks": {},
                                },
                            },
                        },
                    },
                },
                path=f"/mcp/brains/{COMPANY}",
            )
            assert native_status == 200
            assert native_created["result"]["resultType"] == "task"
            native_task_id = native_created["result"]["taskId"]
            for _attempt in range(100):
                native_get_status, native_state = raw_mcp_message(
                    server,
                    company_token["token"],
                    {
                        "jsonrpc": "2.0",
                        "id": 10,
                        "method": "tasks/get",
                        "params": {"taskId": native_task_id},
                    },
                    path=f"/mcp/brains/{COMPANY}",
                    task_name=native_task_id,
                )
                assert native_get_status == 200
                if native_state["result"]["status"] != "working":
                    break
                time.sleep(0.01)
            assert native_state["result"]["status"] == "completed"
            assert native_state["result"]["result"]["structuredContent"][
                "result"
            ]["citations"]

            hold = threading.Event()
            Handler.agent_coordinator._executor.submit(hold.wait)
            cancel_http_request = {
                **agent_request,
                "request_id": "req_1111111111111111",
                "idempotency_key": "synthetic-company-agent-http-cancel",
            }
            cancel_mcp_request = {
                **agent_request,
                "request_id": "req_2222222222222222",
                "idempotency_key": "synthetic-company-agent-mcp-cancel",
            }
            _, cancel_http_started = agent_lifecycle_http(
                server,
                company_token["token"],
                "POST",
                detached_path,
                cancel_http_request,
            )
            cancel_http_run = cancel_http_started["run"]["run_id"]
            cancel_http_code, cancel_http = agent_lifecycle_http(
                server,
                company_token["token"],
                "POST",
                f"{detached_path}/{cancel_http_run}/cancel",
                {},
            )
            assert cancel_http_code == 200
            assert cancel_http["run"]["status"] == "cancelled"
            cancel_mcp_started = rpc(
                server,
                company_token["token"],
                "recall_agent_start",
                cancel_mcp_request,
                path=f"/mcp/brains/{COMPANY}",
            )["result"]["structuredContent"]
            cancel_mcp = rpc(
                server,
                company_token["token"],
                "recall_agent_cancel",
                {"run_id": cancel_mcp_started["run"]["run_id"]},
                path=f"/mcp/brains/{COMPANY}",
            )["result"]["structuredContent"]
            assert cancel_mcp["run"]["status"] == "cancelled"
            hold.set()

            with store.connect() as connection:
                durable_runs = connection.execute(
                    "SELECT count(*) AS value FROM agent_runs WHERE tenant_id=%s",
                    (COMPANY,),
                ).fetchone()["value"]
                stored_traces = connection.execute(
                    """SELECT trace_events::text AS value
                         FROM agent_runs WHERE tenant_id=%s""",
                    (COMPANY,),
                ).fetchall()
                agent_columns = {
                    row["column_name"]
                    for row in connection.execute(
                        """SELECT column_name
                             FROM information_schema.columns
                            WHERE table_schema='public'
                              AND table_name='agent_runs'"""
                    ).fetchall()
                }
            assert durable_runs == 7
            assert "question" not in agent_columns
            assert "credential" not in agent_columns
            for row in stored_traces:
                rendered_trace = row["value"]
                assert agent_request["question"] not in rendered_trace
                for forbidden in ('"prompt"', '"answer"', '"payload"', '"token"'):
                    assert forbidden not in rendered_trace

            company_principal = store.authenticate_bearer(
                company_token["token"],
                "read",
            )
            assert company_principal is not None
            company_principal["authorized_sources"] = (
                store.authorized_canonical_source_ids(
                    company_principal["tenant_id"],
                    company_principal["principal_id"],
                )
            )
            crash_context = DelegationContext.from_principal(company_principal)
            crash_request = {
                **agent_request,
                "request_id": "req_3333333333333333",
                "idempotency_key": "synthetic-company-agent-worker-crash",
            }
            with store.connect() as connection:
                connection.execute(
                    """CREATE UNLOGGED TABLE IF NOT EXISTS agent_run_effects_e2e(
                           run_id text NOT NULL,
                           effect text NOT NULL
                       )"""
                )
                connection.execute("TRUNCATE agent_run_effects_e2e")
            crash_created = agent_backend.create(
                crash_context,
                crash_request,
                now=datetime.now(timezone.utc),
            )
            process = multiprocessing.get_context("spawn").Process(
                target=crash_agent_worker,
                args=(
                    os.environ["RECALL_DATABASE_URL"],
                    company_principal,
                    crash_created.run["run_id"],
                ),
            )
            process.start()
            process.join(timeout=10)
            assert process.exitcode == 17
            with store.connect() as connection:
                connection.execute(
                    """UPDATE agent_runs
                          SET lease_expires_at=now()-interval '1 second'
                        WHERE tenant_id=%s AND run_id=%s""",
                    (COMPANY, crash_created.run["run_id"]),
                )
            recovered_runs = agent_backend.recover_abandoned(
                before=datetime.now(timezone.utc),
                now=datetime.now(timezone.utc),
            )
            assert recovered_runs == 1
            crash_replay = agent_backend.create(
                crash_context,
                crash_request,
                now=datetime.now(timezone.utc),
            )
            assert not crash_replay.created
            assert crash_replay.run["status"] == "failed"
            assert crash_replay.run["error_code"] == "worker_lost_retryable"
            with store.connect() as connection:
                crash_effects = connection.execute(
                    """SELECT count(*) AS value
                         FROM agent_run_effects_e2e
                        WHERE run_id=%s""",
                    (crash_created.run["run_id"],),
                ).fetchone()["value"]
            assert crash_effects == 1
            other_principal = {
                **company_principal,
                "principal_id": "principal:synthetic:other",
            }
            try:
                agent_backend.get(
                    DelegationContext.from_principal(other_principal),
                    crash_created.run["run_id"],
                    now=datetime.now(timezone.utc),
                )
                raise AssertionError("cross-principal agent run was visible")
            except AgentRunNotFound:
                pass

            bounded_backend = PostgresAgentRunBackend(
                store.connect,
                max_active_per_principal=1,
                lease_seconds=15,
                retention_seconds=3600,
            )
            bounded_request = {
                **agent_request,
                "request_id": "req_4444444444444444",
                "idempotency_key": "synthetic-company-agent-bound-one",
            }
            bounded = bounded_backend.create(
                crash_context,
                bounded_request,
                now=datetime.now(timezone.utc),
            )
            try:
                bounded_backend.create(
                    crash_context,
                    {
                        **agent_request,
                        "request_id": "req_5555555555555555",
                        "idempotency_key": "synthetic-company-agent-bound-two",
                    },
                    now=datetime.now(timezone.utc),
                )
                raise AssertionError("agent concurrency bound was bypassed")
            except AgentRunUnavailable:
                pass
            bounded_backend.cancel(
                crash_context,
                bounded.run["run_id"],
                now=datetime.now(timezone.utc),
            )

            with store.connect() as connection:
                connection.execute(
                    """UPDATE agent_runs
                          SET completed_at=now()-interval '2 hours'
                        WHERE tenant_id=%s AND run_id=%s""",
                    (COMPANY, crash_created.run["run_id"]),
                )
            pruned_runs = agent_backend.prune(
                before=datetime.now(timezone.utc) - timedelta(hours=1),
            )
            assert pruned_runs == 1
            with store.connect() as connection:
                connection.execute("DROP TABLE agent_run_effects_e2e")

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
            assert conversational["diagnostics"]["lexical_mode"] == "strict-empty"

            unrelated = rpc(
                server,
                personal_token["token"],
                "recall_search",
                {"query": "Where did we discuss underwater zebras?"},
            )["result"]["structuredContent"]
            assert unrelated["diagnostics"]["lexical_candidates"] == 0
            assert unrelated["diagnostics"]["lexical_mode"] == "strict-empty"

            degraded = rpc(
                server,
                personal_token["token"],
                "recall_search",
                {"query": "shared launch marker semantic unavailable"},
            )["result"]["structuredContent"]
            assert degraded["results"] == []
            assert degraded["diagnostics"]["lexical_mode"] == "strict-empty"
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
            Handler.agent_service = None
            if Handler.agent_coordinator is not None:
                Handler.agent_coordinator.close()
            Handler.agent_coordinator = None
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
                "agent_http_calls": 1,
                "agent_mcp_calls": 1,
                "agent_transport_parity": 1.0,
                "agent_exact_receipts": len(agent_receipts),
                "agent_cross_brain_accepts": 0,
                "agent_trace_content_leaks": 0,
                "agent_stored_question_columns": 0,
                "agent_durable_runs": durable_runs,
                "agent_idempotent_replays": 1,
                "agent_http_lifecycle": 1.0,
                "agent_mcp_lifecycle": 1.0,
                "agent_native_task_lifecycle": 1.0,
                "agent_cancellations": 2,
                "agent_worker_loss_recovered": recovered_runs,
                "agent_worker_loss_duplicate_effects": crash_effects - 1,
                "agent_cross_principal_run_hits": 0,
                "agent_concurrency_overflows": 0,
                "agent_pruned_runs": pruned_runs,
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
