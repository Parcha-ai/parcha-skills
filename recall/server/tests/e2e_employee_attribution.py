#!/usr/bin/env python3
"""Fresh-PostgreSQL E2E for four-employee enrollment and attribution."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path


RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path[:0] = [str(RECALL), str(SERVER)]

from recall_server.actor_attribution import ActorIdentityIndex  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.canonical import (  # noqa: E402
    CanonicalArchiveGateway,
    CanonicalPlane,
)
from recall_server.canonical_retrieval import (  # noqa: E402
    BoundCanonicalRetrieval,
)
from recall_server.control import ControlPlane, SecretBox  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.logical_evidence import (  # noqa: E402
    LogicalEvidenceProjectionStore,
)
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
)
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import PassagePolicy  # noqa: E402
from recall_server.passage_representations import (  # noqa: E402
    CanonicalPassageRepresentationIndex,
    PassageContextPolicy,
    PassageRepresentation,
)
from recall_server.projectors import canonical_json  # noqa: E402


class SyntheticEmbeddingRuntime:
    dimensions = 512
    fingerprint = "synthetic-employee-runtime"
    passage_fingerprint = "synthetic-employee-runtime"
    model = "synthetic-employee-model"

    def __init__(self) -> None:
        self.values: list[str] = []

    def embed_documents(self, values: list[str]) -> list[list[float]]:
        self.values.extend(values)
        return [[0.0] * self.dimensions for _value in values]

    def embed_passages(self, values: list[str]) -> list[list[float]]:
        return self.embed_documents(values)

    @staticmethod
    def embed_query(_value: str) -> list[float]:
        return [0.0] * 512


def ingest(
    *,
    gateway: CanonicalArchiveGateway,
    plane: CanonicalPlane,
    tenant_id: str,
    principal_id: str,
    source_id: str,
    connector_id: str,
    native_id: str,
    native_parent_id: str,
    content: dict[str, object],
    text: str,
    harness: str | None = None,
) -> str:
    occurred_at = "2026-08-05T12:00:00Z"
    raw = json.dumps(
        {"native_id": native_id, "content": content},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    artifact = gateway.put_raw(
        tenant_id=tenant_id,
        source_id=source_id,
        native_id=native_id,
        payload=raw,
        media_type="application/json",
        created_at=occurred_at,
    )
    provenance: dict[str, object] = {
        "connector_id": connector_id,
        "connector_schema_version": 1,
        "artifact_ref": artifact,
    }
    if harness is not None:
        provenance["harness"] = harness
    envelope = {
        "schema_version": 1,
        "source_id": source_id,
        "native_id": native_id,
        "native_parent_id": native_parent_id,
        "kind": "connector_record",
        "occurred_at": occurred_at,
        "observed_at": occurred_at,
        "principal_id": principal_id,
        "visibility": "shared",
        "content_type": "application/json",
        "content": content,
        "provenance": provenance,
        "content_sha256": hashlib.sha256(canonical_json(content)).hexdigest(),
    }
    result = plane.ingest_document(
        tenant_id=tenant_id,
        principal_id=principal_id,
        connector_id=connector_id,
        artifact_ref=artifact,
        envelope=envelope,
        text_redacted=text,
    )
    assert result["inserted"] == 1
    with plane.store.connect() as connection:
        row = connection.execute(
            """SELECT event_id FROM canonical_events
               WHERE tenant_id=%s AND source_id=%s AND native_id=%s
                 AND revision=%s""",
            (tenant_id, source_id, native_id, result["revision"]),
        ).fetchone()
    assert row is not None
    return row["event_id"]


def main() -> None:
    dsn = os.environ["RECALL_DATABASE_URL"]
    runtime = SyntheticEmbeddingRuntime()
    store = BrainStore(dsn, semantic_runtime=runtime)  # type: ignore[arg-type]
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant = f"tenant:company:employees-{nonce}"
    organization = f"org:company:employees-{nonce}"
    owner = f"principal:owner:employees-{nonce}"
    store.provision_brain(
        organization_id=organization,
        organization_kind="company",
        display_name="Synthetic Employee Company",
        tenant_id=tenant,
        brain_kind="company",
        slug=f"employees-{nonce}",
        owner_principal_id=owner,
    )
    box = SecretBox(b"e" * 32)
    control = ControlPlane(store, box, {})
    employees: list[dict[str, str]] = []
    for number in range(2, 6):
        name = f"Employee {number}"
        email = f"employee-{number}@example.invalid"
        invitation = control.create_brain_invitation(
            principal_id=owner,
            tenant_id=tenant,
            email=email,
            display_name=name,
            role="member",
        )
        credential = store.accept_external_invitation(
            issuer="https://identity.synthetic.invalid",
            subject=f"subject-employee-{number}",
            email=email,
            email_index=control.invitation_email_index(email),
            scopes=["read"],
            audience="https://recall.synthetic.invalid/mcp",
            tenant_id=tenant,
            invitation_id=invitation["id"],
            actor_email_index=control.actor_identity_index(
                email,
                tenant_id=tenant,
                connector_id="identity",
                namespace="email",
            ),
        )
        assert credential is not None
        with store.connect() as connection:
            actor = connection.execute(
                """SELECT actor.actor_id
                     FROM brain_actor_principals principal
                     JOIN brain_actors actor USING(tenant_id,actor_id)
                    WHERE principal.tenant_id=%s
                      AND principal.principal_id=%s
                      AND actor.display_name=%s""",
                (tenant, credential["principal_id"], name),
            ).fetchone()
        assert actor is not None
        employees.append({
            "number": str(number),
            "name": name,
            "email": email,
            "principal_id": credential["principal_id"],
            "actor_id": actor["actor_id"],
            "invitation_id": invitation["id"],
        })

    with tempfile.TemporaryDirectory(prefix="recall-employee-e2e-") as value:
        archive = FilesystemArchiveStore(
            Path(value) / "archive",
            namespace_key=b"a" * 32,
        )
        identity_index = ActorIdentityIndex(box.blind_index)
        plane = CanonicalPlane(
            store,
            archive,
            actor_identity_index=identity_index,
        )
        local_event_ids: list[tuple[str, str, str, str]] = []
        employee_sources: dict[str, set[str]] = {}
        for employee in employees:
            number = employee["number"]
            employee_sources[number] = set()
            for harness, connector_id in (
                ("codex", "local.codex"),
                ("claude", "local.claude-code"),
            ):
                source_id = f"{harness}:mac:employee-{number}-{nonce}"
                employee_sources[number].add(source_id)
                route = control.create_device_installation(
                    principal_id=employee["principal_id"],
                    connector_id=connector_id,
                    tenant_id=tenant,
                    device_id=f"mac-employee-{number}-{harness}",
                    source_id=source_id,
                    privacy_mode="scrub",
                    selectors={},
                )
                assert store.authenticate_bearer(route["token"], "write")
                gateway = CanonicalArchiveGateway(
                    store,
                    archive,
                    tenant_id=tenant,
                    principal_id=employee["principal_id"],
                )
                parent = f"session-{harness}-employee-{number}-{nonce}"
                if harness == "codex":
                    user_content: dict[str, object] = {
                        "type": "event_msg",
                        "payload": {
                            "type": "user_message",
                            "message": f"employee {number} codex decision marker",
                        },
                    }
                    assistant_content: dict[str, object] = {
                        "type": "response_item",
                        "payload": {
                            "type": "message",
                            "role": "assistant",
                            "content": f"assistant codex marker {number}",
                        },
                    }
                else:
                    user_content = {
                        "type": "user",
                        "message": {
                            "content": f"employee {number} claude decision marker"
                        },
                    }
                    assistant_content = {
                        "type": "assistant",
                        "message": {
                            "content": f"assistant claude marker {number}"
                        },
                    }
                user_event = ingest(
                    gateway=gateway,
                    plane=plane,
                    tenant_id=tenant,
                    principal_id=employee["principal_id"],
                    source_id=source_id,
                    connector_id=connector_id,
                    native_id=f"{parent}:user",
                    native_parent_id=parent,
                    content=user_content,
                    text=f"employee {number} {harness} decision marker",
                    harness=harness,
                )
                assistant_event = ingest(
                    gateway=gateway,
                    plane=plane,
                    tenant_id=tenant,
                    principal_id=employee["principal_id"],
                    source_id=source_id,
                    connector_id=connector_id,
                    native_id=f"{parent}:assistant",
                    native_parent_id=parent,
                    content=assistant_content,
                    text=f"assistant {harness} marker {number}",
                    harness=harness,
                )
                local_event_ids.extend((
                    (employee["actor_id"], source_id, user_event, "user"),
                    (employee["actor_id"], source_id, assistant_event, "assistant"),
                ))

        slack_source = f"slack:company:employees-{nonce}"
        with store.connect() as connection:
            with connection.transaction():
                CanonicalPlane.register_source(
                    connection,
                    tenant_id=tenant,
                    principal_id=owner,
                    source_id=slack_source,
                )
        slack_gateway = CanonicalArchiveGateway(
            store,
            archive,
            tenant_id=tenant,
            principal_id=owner,
        )
        for employee in employees:
            slack_subject = f"UEMPLOYEE{employee['number']}"
            registered = control.register_actor_identity(
                principal_id=owner,
                tenant_id=tenant,
                actor_id=employee["actor_id"],
                connector_id="slack",
                namespace="author_id",
                subject=slack_subject,
            )
            assert registered["status"] == "registered"
            slack_event = ingest(
                gateway=slack_gateway,
                plane=plane,
                tenant_id=tenant,
                principal_id=owner,
                source_id=slack_source,
                connector_id="slack",
                native_id=f"slack-message-employee-{employee['number']}-{nonce}",
                native_parent_id=(
                    f"slack-thread-employee-{employee['number']}-{nonce}"
                ),
                content={
                    "author_id": slack_subject,
                    "text": (
                        f"employee {employee['number']} slack decision marker"
                    ),
                },
                text=f"employee {employee['number']} slack decision marker",
            )
            local_event_ids.append((
                employee["actor_id"], slack_source, slack_event, "user"
            ))

        with store.connect() as connection:
            for actor_id, source_id, event_id, role in local_event_ids:
                rows = connection.execute(
                    """SELECT actor_id,relation FROM canonical_event_actors
                       WHERE tenant_id=%s AND source_id=%s AND event_id=%s""",
                    (tenant, source_id, event_id),
                ).fetchall()
                if role == "assistant":
                    assert rows == []
                else:
                    assert rows == [{"actor_id": actor_id, "relation": "author"}]
            bindings = connection.execute(
                """SELECT source_id,actor_id,relation
                     FROM canonical_source_actor_bindings
                    WHERE tenant_id=%s ORDER BY source_id""",
                (tenant,),
            ).fetchall()
            assert len(bindings) == 8
            assert {row["relation"] for row in bindings} == {"contributor"}

        logical_store = LogicalEvidenceProjectionStore(archive)
        logical = CanonicalLogicalEvidenceProjector(
            store,
            logical_store,
            bound_tenant_id=tenant,
            raw_archive=archive,
        )
        logical.seed_backfill(tenant_id=tenant)
        logical_result = logical.project_pending(
            tenant_id=tenant,
            batch_size=20,
            max_batches=2,
            upload_concurrency=4,
        )
        assert logical_result["documents"] == 12
        policy = PassagePolicy(target_tokens=8, overlap_tokens=0)
        passages = CanonicalPassageProjector(
            store,
            logical_store,
            policy=policy,
            bound_tenant_id=tenant,
        )
        passage_result = passages.project_pending(
            tenant_id=tenant,
            batch_size=20,
            max_batches=2,
            concurrency=4,
        )
        assert passage_result["documents"] == 12
        assert passages.embed_pending(
            tenant_id=tenant,
            batch_size=100,
            max_batches=2,
        )["status"] == "complete"
        actor_representation = PassageRepresentation(
            "actor-context-employee-e2e",
            runtime,
            PassageContextPolicy(),
        )
        represented = CanonicalPassageRepresentationIndex(
            store,
            passage_policy_fingerprint=policy.fingerprint,
            representation=actor_representation,
            bound_tenant_id=tenant,
        ).embed_pending(
            tenant_id=tenant,
            batch_size=100,
            max_batches=2,
        )
        assert represented["status"] == "complete"
        for employee in employees:
            assert any(employee["name"] in value for value in runtime.values)
            sources = tuple(store.authorized_canonical_source_ids(
                tenant, employee["principal_id"]
            ))
            expected_sources = {
                slack_source,
                *(source for values in employee_sources.values() for source in values),
            }
            assert set(sources) == expected_sources, (
                employee["number"],
                sorted(set(sources) - expected_sources),
                sorted(expected_sources - set(sources)),
            )
            bound = BoundCanonicalRetrieval(
                store,
                tenant_id=tenant,
                principal_id=employee["principal_id"],
                authorized_sources=sources,
                passage_policy=policy,
            )
            broad = bound.passage_hints(
                f"employee {employee['number']} decision marker",
                filters={"person": employee["name"]},
                limit=20,
            )
            broad_sources = {
                result["source_id"] for result in broad["results"]
            }
            expected_employee_sources = {
                slack_source,
                *employee_sources[employee["number"]],
            }
            with store.connect() as connection:
                actor_passage_sources = {
                    row["source_id"]
                    for row in connection.execute(
                        """SELECT DISTINCT source_id
                             FROM canonical_passage_actors
                            WHERE tenant_id=%s AND actor_id=%s""",
                        (tenant, employee["actor_id"]),
                    ).fetchall()
                }
            assert broad_sources == expected_employee_sources, (
                employee["number"],
                sorted(broad_sources),
                sorted(expected_employee_sources),
                sorted(actor_passage_sources),
            )
            authored = bound.passage_hints(
                f"employee {employee['number']} decision marker",
                filters={
                    "person": employee["email"],
                    "person_relation": "author",
                },
                limit=20,
            )
            authored_sources = {
                result["source_id"] for result in authored["results"]
            }
            assert authored_sources == expected_employee_sources, (
                employee["number"],
                sorted(authored_sources),
                sorted(expected_employee_sources),
            )

        denied = BoundCanonicalRetrieval(
            store,
            tenant_id=tenant,
            principal_id="principal:employee:denied",
            authorized_sources=(),
            passage_policy=policy,
        ).passage_hints(
            "What did Employee 2 work on?",
            filters={"person": "Employee 2"},
            limit=20,
        )
        assert denied["results"] == []
        missing = BoundCanonicalRetrieval(
            store,
            tenant_id=tenant,
            principal_id=employees[0]["principal_id"],
            authorized_sources=tuple(
                store.authorized_canonical_source_ids(
                    tenant, employees[0]["principal_id"]
                )
            ),
            passage_policy=policy,
        ).passage_hints(
            "What did Nobody work on?",
            filters={"person": "Nobody Example"},
            limit=20,
        )
        assert missing["results"] == []

        removed = employees[0]
        route_before = control.create_device_installation(
            principal_id=removed["principal_id"],
            connector_id="local.codex",
            tenant_id=tenant,
            device_id="mac-employee-2-revocation",
            source_id=f"codex:mac:employee-2-revocation-{nonce}",
            privacy_mode="scrub",
            selectors={},
        )
        assert control.revoke_brain_invitation(
            principal_id=owner,
            invitation_id=removed["invitation_id"],
        )["status"] == "revoked"
        assert store.authenticate_bearer(route_before["token"], "write") is None
        assert store.authorized_canonical_source_ids(
            tenant, removed["principal_id"]
        ) == []
        with store.connect() as connection:
            preserved = connection.execute(
                """SELECT count(*) AS count FROM canonical_event_actors
                   WHERE tenant_id=%s AND actor_id=%s""",
                (tenant, removed["actor_id"]),
            ).fetchone()["count"]
        assert preserved == 3

    store.close()
    print(json.dumps({
        "status": "pass",
        "employees": 4,
        "local_sources": 8,
        "shared_sources": 1,
        "logical_documents": 12,
        "exact_authored_events": 12,
        "assistant_authorship_false_positives": 0,
        "cross_employee_filter_leaks": 0,
        "unauthorized_results": 0,
        "revoked_collector_writes": 0,
        "removed_employee_history_preserved": True,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
