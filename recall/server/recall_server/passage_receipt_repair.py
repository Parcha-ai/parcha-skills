"""Route proved same-parent supersession to the existing passage repair owner.

This is bounded scheduling, not receipt authorization or a certificate. The
original publisher must regenerate and prove the new passages before serving.
"""
import hashlib
import json

from . import parent_membership_enrollment as native
from .projectors import item_receipt
from .receipt_membership import (
    ReceiptMembershipError, _identity, lookup_receipt_metadata,
)

CATALOG = """SELECT evidence.native_parent_id,evidence.revision,
                   evidence.created_at::text AS catalog_created_at,evidence.document_content_sha256,
                   evidence.manifest_content_sha256,evidence.manifest_object_key,evidence.manifest_version_id,
                   projected.revision AS projected_revision,projected.policy_fingerprint,
                   projected.source_document_sha256,passage.revision AS passage_revision,
                   passage.text_sha256,passage.receipts,
                   source.owner_principal_id,grant_row.permission AS source_permission
              FROM canonical_evidence_documents evidence
              JOIN canonical_passage_documents projected USING(tenant_id,source_id,logical_document_id)
              JOIN canonical_passages passage USING(tenant_id,source_id,logical_document_id)
              JOIN canonical_sources source USING(tenant_id,source_id)
              JOIN canonical_source_grants grant_row USING(tenant_id,source_id)
             WHERE evidence.tenant_id=%s AND evidence.source_id=%s
               AND evidence.logical_document_id=%s AND passage.passage_id=%s
               AND grant_row.principal_id=%s
             FOR SHARE OF source,grant_row,evidence,projected,passage NOWAIT"""

def _digest(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()).hexdigest()


def _scope(plan):
    return dict(
        tenant_id=plan["tenant_id"], source_id=plan["target"]["source_id"],
        principal_id=plan["principal_id"],
        native_parent_id=plan["catalog"]["native_parent_id"],
        passage_ids=tuple(plan["supersession"]["passage_ids"]),
        limits=native._CERTIFICATION_LIMITS,
    )


def _superseded(store, connection, scope, rows, deadline, *, lock=False):
    if not native._complete(rows, scope):
        return None
    receipts = tuple(dict.fromkeys(r for row in rows for r in row["receipts"]))
    # Keep this optional repair inside one existing native receipt quantum.
    # Larger indivisible passages retain their original refusal, not a partial repair.
    if len(receipts) > 512:
        return None
    try:
        identities = [_identity(receipt) for receipt in receipts]
    except ReceiptMembershipError:
        return None
    if any(identity[0] != scope["source_id"] for identity in identities):
        return None
    profile = "old.chunk_receipt_profile" if getattr(store, "verified_singleton_receipts", False) else "NULL::smallint"
    statement = f"""WITH requested AS MATERIALIZED (
        SELECT * FROM unnest(%s::text[],%s::text[],%s::integer[],%s::integer[])
          AS wanted(receipt,native_id,revision,ordinal)
    ) SELECT requested.receipt,requested.native_id,requested.revision,requested.ordinal,
        old.is_current,old.deleted_at,old_event.is_tombstone,
        coalesce(old_event.native_parent_id,old_event.native_id) AS old_parent,
        current.revision AS current_revision,current.deleted_at AS current_deleted_at,
        current_event.is_tombstone AS current_tombstone,
        coalesce(current_event.native_parent_id,current_event.native_id) AS current_parent,
        (EXISTS(SELECT 1 FROM canonical_chunks chunk
          WHERE chunk.tenant_id=old.tenant_id AND chunk.source_id=old.source_id
            AND chunk.document_id=old.document_id AND chunk.ordinal=requested.ordinal
            AND chunk.receipt=requested.receipt AND chunk.deleted_at IS NULL)
         OR ({profile}=1 AND requested.ordinal=0 AND NOT EXISTS(
             SELECT 1 FROM canonical_chunks chunk WHERE chunk.tenant_id=old.tenant_id
               AND chunk.source_id=old.source_id AND chunk.document_id=old.document_id))) AS old_identity,
        EXISTS(SELECT 1 FROM canonical_events later WHERE later.tenant_id=old.tenant_id
            AND later.source_id=old.source_id AND later.native_id=old.native_id
            AND later.revision>old.revision AND later.is_tombstone) AS later_tombstone
      FROM requested
      JOIN canonical_documents old ON old.tenant_id=%s AND old.source_id=%s
        AND old.native_id=requested.native_id AND old.revision=requested.revision
      JOIN canonical_events old_event ON old_event.tenant_id=old.tenant_id
        AND old_event.source_id=old.source_id AND old_event.event_id=old.event_id
      JOIN canonical_documents current ON current.tenant_id=old.tenant_id
        AND current.source_id=old.source_id AND current.native_id=old.native_id AND current.is_current
      JOIN canonical_events current_event ON current_event.tenant_id=current.tenant_id
        AND current_event.source_id=current.source_id AND current_event.event_id=current.event_id"""
    if lock:
        statement += " FOR SHARE OF old,current,old_event,current_event NOWAIT"
    metadata = native._query(store, connection, statement,
        (list(receipts), [x[1] for x in identities], [x[2] for x in identities],
         [x[3] for x in identities], scope["tenant_id"], scope["source_id"]), deadline).fetchall()
    if len(metadata) != len(receipts) or {r["receipt"] for r in metadata} != set(receipts):
        return None
    if any(r["deleted_at"] is not None or r["current_deleted_at"] is not None
           or r["is_tombstone"] or r["current_tombstone"] or r["later_tombstone"]
           or not r["old_identity"]
           or r["old_parent"] != scope["native_parent_id"]
           or r["current_parent"] != scope["native_parent_id"]
           or r["current_revision"] < r["revision"]
           or r["is_current"] != (r["current_revision"] == r["revision"])
           for r in metadata):
        return None
    current_receipts = tuple(item_receipt(scope["source_id"], r["native_id"],
        r["current_revision"], r["ordinal"]) for r in metadata)
    try:
        members = lookup_receipt_metadata(
            store, tenant_id=scope["tenant_id"], authorized_sources=(scope["source_id"],),
            receipts=current_receipts,
            allow_verified_singletons=getattr(store, "verified_singleton_receipts", False),
            connection=connection, deadline_at=deadline,
        )
    except ReceiptMembershipError:
        return None
    if set(members) != set(current_receipts):
        return None
    return {r["receipt"] for r in metadata if r["current_revision"] > r["revision"]}


def validate_admission(store, connection, plan, deadline):
    """Recheck the narrow repair condition under the native writer's transaction."""
    scope = _scope(plan)
    expected = plan["supersession"]["queue"]
    grant = native._query(store, connection, """SELECT 1 FROM brain_access_grants
        WHERE tenant_id=%s AND principal_id=%s AND permission IN ('owner','admin','read')
        FOR SHARE NOWAIT""", (scope["tenant_id"], scope["principal_id"]), deadline).fetchone()
    queue = native._query(store, connection, """SELECT generation,certification_checkpoint AS checkpoint
        FROM canonical_passage_projection_queue WHERE tenant_id=%s AND source_id=%s
          AND logical_document_id=%s AND reason='certification' FOR UPDATE NOWAIT""",
        (scope["tenant_id"], scope["source_id"], plan["target"]["logical_document_id"]), deadline).fetchone()
    if grant is None or queue != expected:
        raise ValueError("certification_repair_changed")
    rows = native._rows(store, connection, scope, deadline, lock=True)
    superseded = _superseded(store, connection, scope, rows, deadline, lock=True)
    witness = next((r for r in rows if r["passage_id"] == plan["target"]["passage_id"]), None)
    if not superseded or witness is None or not superseded.intersection(witness["receipts"]):
        raise ValueError("supersession_not_proved")


def reconcile(store, queued, checkpoint, passage_ids, deadline):
    """Return a known scheduling outcome; never acknowledge membership or publication."""
    from .passage_index import CanonicalPassageProjector
    from .passage_projection import DEFAULT_PASSAGE_POLICY

    with native._connect(store, deadline) as connection, connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        owner = native._query(store, connection, """SELECT source.owner_principal_id,evidence.native_parent_id
            FROM canonical_evidence_documents evidence JOIN canonical_sources source USING(tenant_id,source_id)
            WHERE evidence.tenant_id=%s AND evidence.source_id=%s AND evidence.logical_document_id=%s""",
            (queued["tenant_id"], queued["source_id"], queued["logical_document_id"]), deadline).fetchone()
        if owner is None:
            return None
        scope = dict(tenant_id=queued["tenant_id"], source_id=queued["source_id"],
            principal_id=owner["owner_principal_id"], native_parent_id=owner["native_parent_id"],
            passage_ids=tuple(passage_ids), limits=native._CERTIFICATION_LIMITS)
        if not native._authorized(store, connection, scope, deadline):
            return None
        rows = native._rows(store, connection, scope, deadline)
        superseded = _superseded(store, connection, scope, rows, deadline)
        if not superseded:
            return None
        witness = next(r for r in rows if superseded.intersection(r["receipts"]))
        target = dict(source_id=queued["source_id"], logical_document_id=queued["logical_document_id"],
                      passage_id=witness["passage_id"])
        catalog = native._query(store, connection,
            CATALOG.removesuffix(" FOR SHARE OF source,grant_row,evidence,projected,passage NOWAIT"),
            (queued["tenant_id"], queued["source_id"], queued["logical_document_id"],
             witness["passage_id"], scope["principal_id"]), deadline).fetchone()
        if catalog is None:
            return None
        catalog.pop("owner_principal_id")
        catalog.pop("source_permission")
        receipts = catalog.pop("receipts")
        if any(checkpoint.get(name) != catalog[field] for name, field in (
            ("revision", "revision"), ("source_sha256", "document_content_sha256"),
            ("manifest_sha256", "manifest_content_sha256"), ("policy", "policy_fingerprint"))):
            return "changed"
        plan = dict(contract="recall.passage-receipt-repair.v1", tenant_id=queued["tenant_id"],
            principal_id=scope["principal_id"], target=target, catalog=catalog,
            witness_receipt_digest=_digest(receipts), stale_receipts=sum(r in superseded for r in receipts),
            supersession=dict(queue=dict(generation=queued["generation"], checkpoint=checkpoint),
                              passage_ids=list(passage_ids)))
        plan["proof_sha256"] = _digest(plan)
    projector = CanonicalPassageProjector(store, None, policy=DEFAULT_PASSAGE_POLICY,
                                         bound_tenant_id=queued["tenant_id"])
    try:
        projector.requeue_receipt_repair(plan=plan, principal_id=scope["principal_id"], deadline_at=deadline)
    except ValueError as error:
        if str(error) in {"catalog_changed", "witness_changed", "projection_pending", "certification_repair_changed"}:
            return "changed"
        if str(error) == "supersession_not_proved":
            return None
        raise
    return "repair-queued"
