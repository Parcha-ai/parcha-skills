"""Resume optional native certification in the existing publication queue.

Only derived certificates are repeatable. The cursor advances after acknowledged
native proof; a lost response safely leaves the same IDs for fresh proof.
"""

import json
import time

from . import parent_membership_enrollment as native

# Version2 prefixes were proved with the v3 certificate binding.
_CHECKPOINT_VERSION = 2

FIELDS = (
    "revision",
    "source_sha256",
    "manifest_sha256",
    "policy",
    "authority_generation",
)
_CURRENT = """SELECT evidence.revision,evidence.document_content_sha256 AS source_sha256,
    evidence.manifest_content_sha256 AS manifest_sha256,projected.policy_fingerprint AS policy,
    authority.generation AS authority_generation,
    EXISTS(SELECT 1 FROM schema_migrations WHERE version=92) AS additive_membership_generation,
    EXISTS(SELECT 1 FROM canonical_evidence_document_queue dirty
      WHERE dirty.tenant_id=evidence.tenant_id AND dirty.source_id=evidence.source_id
        AND dirty.native_parent_id=evidence.native_parent_id) AS logical_pending,
    EXISTS(SELECT 1 FROM canonical_chunk_retirement_progress retirement
      WHERE retirement.tenant_id=evidence.tenant_id AND retirement.source_id=evidence.source_id
        AND retirement.native_parent_id=evidence.native_parent_id AND retirement.enabled
        AND retirement.status IN ('pending','partial')) AS retirement_pending,
    EXISTS(SELECT 1 FROM canonical_retirement_source_policy retirement
      WHERE retirement.tenant_id=evidence.tenant_id AND retirement.source_id=evidence.source_id
        AND retirement.maintenance_intent IS NOT NULL
        AND (retirement.maintenance_intent->>'parent'=evidence.native_parent_id
          OR jsonb_typeof(retirement.maintenance_intent->'parent') IS DISTINCT FROM 'string')) AS retirement_intent
  FROM canonical_evidence_documents evidence
  JOIN canonical_passage_documents projected USING(tenant_id,source_id,logical_document_id)
  LEFT JOIN canonical_parent_authority authority
    ON authority.tenant_id=evidence.tenant_id AND authority.source_id=evidence.source_id
      AND authority.native_parent_id=evidence.native_parent_id
 WHERE evidence.tenant_id={tenant} AND evidence.source_id={source} AND evidence.logical_document_id={logical}
   AND projected.revision=evidence.revision
   AND projected.source_document_sha256=evidence.document_content_sha256"""
CURRENT = _CURRENT.format(tenant="%s", source="%s", logical="%s")


def key(row):
    return row["tenant_id"], row["source_id"], row["logical_document_id"]


def retain_retirement(store, *, scope, manifest, settings, deadline_at):
    """Commit a scheduling request before retirement can invalidate certificates.

    Existing publication or certification work wins. No proof cursor or authority
    is invented: resume rechecks the current publication and proves membership.
    """
    if not settings.enabled:
        return None
    with native._connect(store, deadline_at) as connection, connection.transaction():
        available = native._query(store, connection, """SELECT EXISTS(
            SELECT 1 FROM pg_attribute
            WHERE attrelid=to_regclass('canonical_passage_projection_queue')
              AND attname='certification_checkpoint' AND atttypid='jsonb'::regtype
              AND NOT attisdropped) AS available""", (), deadline_at).fetchone()
        if not available['available']:
            return None
        if not native._authorized(store, connection, scope, deadline_at):
            raise PermissionError('retirement_certification_unauthorized')
        current = CURRENT + """ AND evidence.native_parent_id=%s
            AND evidence.revision=%s AND evidence.document_content_sha256=%s
            AND evidence.manifest_content_sha256=%s"""
        row = native._query(store, connection, """WITH current_scope AS ("""
            + current + f""") INSERT INTO canonical_passage_projection_queue
            (tenant_id,source_id,logical_document_id,revision,reason,certification_checkpoint)
            SELECT %s,%s,%s,revision,'certification',
              jsonb_build_object('version',{_CHECKPOINT_VERSION},'epoch',%s::text,'after','',
                'revision',revision,'source_sha256',source_sha256,
                'manifest_sha256',manifest_sha256,'policy',policy,
                'authority_generation',authority_generation)
            FROM current_scope WHERE NOT logical_pending
            ON CONFLICT(tenant_id,source_id,logical_document_id) DO NOTHING
            RETURNING tenant_id,source_id,logical_document_id,generation,
                certification_checkpoint AS checkpoint""",
            (scope['tenant_id'], scope['source_id'], manifest['logical_document_id'],
             manifest['native_parent_id'], manifest['revision'],
             manifest['document_content_sha256'], manifest['manifest_content_sha256'],
             scope['tenant_id'], scope['source_id'], manifest['logical_document_id'],
             settings.binding_epoch), deadline_at).fetchone()
    return row


def retain(connection, candidate, settings):
    """Retain after publication; None alone requests the pre085 legacy hook."""
    if not settings.enabled:
        return False
    available = connection.execute("""SELECT EXISTS(SELECT 1 FROM pg_attribute
        WHERE attrelid=to_regclass('canonical_passage_projection_queue')
          AND attname='certification_checkpoint' AND atttypid='jsonb'::regtype
          AND NOT attisdropped) AS available""").fetchone()["available"]
    if not available:
        return None
    scope = connection.execute(
        CURRENT,
        (candidate.tenant_id, candidate.source_id, candidate.logical_document_id),
    ).fetchone()
    if scope is None:
        return False
    # Null generation is only a scheduling request for native initialization.
    # It carries no certified prefix and cannot authorize a reader.
    checkpoint = dict(
        version=_CHECKPOINT_VERSION,
        epoch=settings.binding_epoch,
        after="",
        **{name: scope[name] for name in FIELDS},
    )
    changed = connection.execute(
        """UPDATE canonical_passage_projection_queue
        SET reason='certification',certification_checkpoint=%s::jsonb,
            notification_queued_at=NULL
        WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s AND generation=%s""",
        (
            json.dumps(checkpoint),
            candidate.tenant_id,
            candidate.source_id,
            candidate.logical_document_id,
            candidate.generation,
        ),
    )
    if changed.rowcount != 1:
        raise ValueError("certification_queue_changed")
    return True


def pending(
    store, *, tenant_id, limit, deadline_at, logical_document_id=None, source_id=None
):
    # The reason exists on old schemas; no column lookup or archive hydration is
    # needed when no certification rows exist before migration085.
    with native._connect(store, deadline_at) as connection:
        return native._query(
            store,
            connection,
            """SELECT tenant_id,source_id,
            logical_document_id,generation,to_jsonb(queue)->'certification_checkpoint' AS checkpoint
          FROM canonical_passage_projection_queue queue
         WHERE reason='certification' AND (%s::text IS NULL OR tenant_id=%s)
           AND (%s::text IS NULL OR logical_document_id=%s)
           AND (%s::text IS NULL OR source_id=%s)
         ORDER BY changed_at,tenant_id,source_id,logical_document_id LIMIT %s""",
            (
                tenant_id,
                tenant_id,
                logical_document_id,
                logical_document_id,
                source_id,
                source_id,
                limit,
            ),
            deadline_at,
        ).fetchall()


def change_cursor(store, row, checkpoint, *, deadline_at, after=None):
    """CAS only the optional phase; newer source publication always wins."""
    with native._connect(store, deadline_at) as connection:
        where = """WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
            AND generation=%s AND reason='certification'
            AND coalesce(certification_checkpoint,'null'::jsonb)=%s::jsonb"""
        params = (
            *key(row),
            row["generation"],
            json.dumps(checkpoint),
        )
        if after is None:
            result = native._query(
                store,
                connection,
                "DELETE FROM canonical_passage_projection_queue " + where,
                params,
                deadline_at,
            )
        else:
            updated = dict(checkpoint, after=after)
            result = native._query(
                store,
                connection,
                "UPDATE canonical_passage_projection_queue SET certification_checkpoint=%s::jsonb,changed_at=clock_timestamp() "
                + where,
                (json.dumps(updated), *params),
                deadline_at,
            )
    return result.rowcount == 1


def resume(store, row, *, settings, deadline_at):
    """One finite slice; only proved supersession schedules ordinary repair."""
    certified = 0
    checkpoint = row["checkpoint"]
    first_slice = True
    if not settings.enabled:
        return dict(status="disabled", certified_passages=0)
    try:
        while time.monotonic() < deadline_at:
            if first_slice:
                with native._connect(store, deadline_at) as connection:
                    # Rotate scheduling before metadata can consume the allowance.
                    # Commit this timestamp before a later read can roll back.
                    # This acknowledges no proof and never changes its checkpoint.
                    scheduled = native._query(
                        store, connection,
                        """UPDATE canonical_passage_projection_queue
                        SET changed_at=clock_timestamp()
                        WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
                          AND generation=%s AND reason='certification'
                          AND coalesce(certification_checkpoint,'null'::jsonb)=%s::jsonb""",
                        (*key(row), row["generation"], json.dumps(checkpoint)), deadline_at,
                    )
                if scheduled.rowcount != 1:
                    return dict(status="changed", certified_passages=certified)
                first_slice = False
            with native._connect(store, deadline_at) as connection:
                scope = native._query(
                    store, connection, CURRENT, key(row), deadline_at
                ).fetchone()
                valid = (
                    isinstance(checkpoint, dict)
                    and type(checkpoint.get("version")) is int
                    and checkpoint["version"] in (1, _CHECKPOINT_VERSION)
                    and checkpoint.get("epoch") == settings.binding_epoch
                    and isinstance(checkpoint.get("after"), str)
                    and (
                        (type(checkpoint.get("authority_generation")) is int
                         and checkpoint["authority_generation"] > 0)
                        or ("authority_generation" in checkpoint
                            and checkpoint["authority_generation"] is None
                            and checkpoint["after"] == "")
                    )
                    and scope is not None
                    and all(
                        checkpoint.get(name) == scope[name]
                        for name in FIELDS if name != "authority_generation"
                    )
                )
                busy = scope is not None and (
                    scope["retirement_intent"] or (
                        not scope["additive_membership_generation"]
                        and (scope["logical_pending"] or scope["retirement_pending"])
                    )
                )
                generation_changed = valid and (
                    checkpoint["authority_generation"] != scope["authority_generation"]
                )
                binding_changed = valid and checkpoint["version"] != _CHECKPOINT_VERSION
                initialize = (valid and checkpoint["authority_generation"] is None
                              and scope["authority_generation"] is None and not busy)
                if valid and scope["authority_generation"] is None and not initialize:
                    # A previously positive binding must never be reset.
                    valid = checkpoint["authority_generation"] is None
                if valid and (generation_changed or binding_changed):
                    # Reset an invalidated prefix only after current metadata.
                    updated = checkpoint
                    if not busy:
                        # Membership or binding-format changes invalidate the prefix.
                        # Keep the publication binding and reprove from the start.
                        updated = dict(
                            checkpoint, version=_CHECKPOINT_VERSION, after="",
                            authority_generation=scope["authority_generation"],
                        )
                    scheduled = native._query(
                        store,
                        connection,
                        """UPDATE canonical_passage_projection_queue
                        SET changed_at=clock_timestamp(),certification_checkpoint=%s::jsonb
                        WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
                          AND generation=%s AND reason='certification'
                          AND certification_checkpoint=%s::jsonb""",
                        (json.dumps(updated), *key(row), row["generation"],
                         json.dumps(checkpoint)),
                        deadline_at,
                    )
                    if scheduled.rowcount != 1:
                        return dict(status="changed", certified_passages=certified)
                    checkpoint = updated
                rows = (
                    native._query(
                        store,
                        connection,
                        """SELECT passage_id,cardinality(receipts) AS receipt_count FROM canonical_passages
                    WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
                      AND policy_fingerprint=%s AND passage_id>%s
                    ORDER BY passage_id LIMIT %s""",
                        (
                            *key(row),
                            checkpoint.get("policy", "")
                            if isinstance(checkpoint, dict)
                            else "",
                            checkpoint.get("after", "")
                            if isinstance(checkpoint, dict)
                            else "",
                            native._CERTIFICATION_LIMITS["passages"],
                        ),
                        deadline_at,
                    ).fetchall()
                    if valid and not busy and not initialize
                    else []
                )
            if initialize:
                # No queue/catalog lock or connection survives this boundary.
                # Re-read CURRENT and CAS the original queue on the next pass.
                initialized = native.initialize_published_parent_generation(
                    store, tenant_id=row["tenant_id"], source_id=row["source_id"],
                    logical_document_id=row["logical_document_id"], deadline_at=deadline_at)
                if initialized != "initialized":
                    change_cursor(store, row, checkpoint, deadline_at=deadline_at)
                    return dict(status=initialized, certified_passages=certified)
                continue
            if not valid:
                change_cursor(store, row, checkpoint, deadline_at=deadline_at)
                return dict(status="changed", certified_passages=certified)
            if busy:
                return dict(status="busy", certified_passages=certified)
            if not rows:
                removed = change_cursor(store, row, checkpoint, deadline_at=deadline_at)
                return dict(
                    status="enrolled" if removed else "changed",
                    certified_passages=certified,
                )
            # Commit complete passages within one native 512-receipt proof
            # quantum. A single larger passage remains indivisible; its full
            # native proof must succeed before the cursor can advance.
            prefix = []
            receipt_count = 0
            for value in rows:
                if prefix and receipt_count + value["receipt_count"] > 512:
                    break
                prefix.append(value["passage_id"])
                receipt_count += value["receipt_count"]
            ids = tuple(prefix)
            outcome = native.certify_published_passages(
                store,
                tenant_id=row["tenant_id"],
                source_id=row["source_id"],
                logical_document_id=row["logical_document_id"],
                passage_ids=ids,
                settings=settings,
                deadline_at=deadline_at,
            )
            certified += outcome["certified_passages"]
            if outcome["status"] != "enrolled":
                if outcome["status"] == "membership-unproved":
                    from .passage_receipt_repair import reconcile
                    repaired = reconcile(store, row, checkpoint, ids, deadline_at)
                    if repaired is not None:
                        return dict(status=repaired, certified_passages=certified)
                if outcome["status"] not in ("failed", "deadline", "busy", "changed"):
                    change_cursor(store, row, checkpoint, deadline_at=deadline_at)
                return dict(outcome, certified_passages=certified)
            if outcome["certified_passages"] != len(ids):
                raise ValueError("certification_batch_incomplete")
            if not change_cursor(
                store, row, checkpoint, deadline_at=deadline_at, after=ids[-1]
            ):
                return dict(status="changed", certified_passages=certified)
            checkpoint = dict(checkpoint, after=ids[-1])
        return dict(status="deadline", certified_passages=certified)
    except Exception as error:
        # Earlier commits remain acknowledged work; the uncertain batch's cursor
        # stays unchanged and a later ordinary slice can safely reprove it.
        return dict(status="failed", certified_passages=certified,
                    failure=native.certification_failure(error, source_id=row["source_id"],
                        logical_document_id=row["logical_document_id"]))


def seed_history(
    store, *, tenant_id, settings, deadline_at, after=None, through=None, limit=128
):
    """Queue one finite metadata page; existing work and proof cursors always win.

    A response is returned only after the page transaction commits. Repeating a
    page after an uncertain response is safe: conflicts never reset queued work.
    The worker, not admission, proves authority and measures certificate coverage.
    """
    if not settings.enabled:
        raise ValueError("membership_certification_disabled")
    if not isinstance(tenant_id, str) or not 1 <= len(tenant_id) <= 512:
        raise ValueError("tenant_invalid")
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("seed_limit_invalid")
    for cursor in (after, through):
        if cursor is not None and (
            not isinstance(cursor, (tuple, list))
            or len(cursor) != 2
            or any(
                not isinstance(value, str) or not 1 <= len(value) <= 512
                for value in cursor
            )
        ):
            raise ValueError("seed_cursor_invalid")
    if after is not None and through is not None and tuple(after) > tuple(through):
        raise ValueError("seed_cursor_outside_range")
    result = dict(
        status="completed",
        after=after,
        through=through,
        scanned=0,
        queued=0,
        deferred=[],
        coverage_measured=False,
    )
    with native._connect(store, deadline_at) as connection:
        available = native._query(
            store,
            connection,
            """SELECT EXISTS(SELECT 1 FROM pg_attribute
            WHERE attrelid=to_regclass('canonical_passage_projection_queue')
              AND attname='certification_checkpoint' AND atttypid='jsonb'::regtype
              AND NOT attisdropped) AS available""",
            (),
            deadline_at,
        ).fetchone()["available"]
        if not available:
            raise ValueError("membership_remainder_schema_required")
        if through is None:
            upper = native._query(
                store,
                connection,
                """SELECT source_id,logical_document_id
                FROM canonical_passage_documents WHERE tenant_id=%s
                ORDER BY source_id DESC,logical_document_id DESC LIMIT 1""",
                (tenant_id,),
                deadline_at,
            ).fetchone()
            if upper is not None:
                through = [upper["source_id"], upper["logical_document_id"]]
                result["through"] = through
    if through is not None:
        # Commit initialization before touching the publication queue. Canonical
        # writers acquire queue locks before deferred authority updates.
        selection = """evidence.tenant_id=%s AND
            (evidence.source_id,evidence.logical_document_id) IN (
              SELECT source_id,logical_document_id FROM canonical_passage_documents
              WHERE tenant_id=%s
                AND (%s::text IS NULL OR (source_id,logical_document_id)>(%s,%s))
                AND (source_id,logical_document_id)<=(%s,%s)
              ORDER BY source_id,logical_document_id LIMIT %s)
            AND EXISTS(SELECT 1 FROM canonical_sources source
              JOIN brain_access_grants brain ON brain.tenant_id=source.tenant_id
                AND brain.principal_id=source.owner_principal_id
                AND brain.permission IN ('owner','admin','read')
              JOIN canonical_source_grants grant_row ON grant_row.tenant_id=source.tenant_id
                AND grant_row.source_id=source.source_id
                AND grant_row.principal_id=source.owner_principal_id
                AND grant_row.permission IN ('owner','read')
              WHERE source.tenant_id=evidence.tenant_id AND source.source_id=evidence.source_id)"""
        with native._connect(store, deadline_at) as connection:
            native._query(store, connection,
                native._INITIALIZE_GENERATION.format(
                    selection=selection, passage_selection='',
                    ordering='ORDER BY evidence.tenant_id,evidence.source_id,evidence.native_parent_id\n            '),
                (tenant_id,tenant_id,after[0] if after else None,*(after or (None,None)),*through,limit),
                deadline_at)
        # A fresh snapshot observes committed initialization or a newer native
        # generation. Existing queue work still wins ON CONFLICT below.
        with native._connect(store, deadline_at) as connection:
            scope_sql = _CURRENT.format(
                tenant="%s",
                source="selected.source_id",
                logical="selected.logical_document_id",
            )
            rows = native._query(
                store,
                connection,
                f"""WITH page AS MATERIALIZED (
                  SELECT source_id,logical_document_id
                  FROM canonical_passage_documents WHERE tenant_id=%s
                    AND (%s::text IS NULL OR (source_id,logical_document_id)>(%s,%s))
                    AND (source_id,logical_document_id)<=(%s,%s)
                  ORDER BY source_id,logical_document_id LIMIT %s
                ), selected AS MATERIALIZED (
                  SELECT * FROM page ORDER BY source_id,logical_document_id LIMIT %s
                ), scoped AS MATERIALIZED (
                  SELECT selected.*,current_scope.* FROM selected
                  LEFT JOIN LATERAL ({scope_sql}) current_scope ON true
                ), inserted AS (
                  INSERT INTO canonical_passage_projection_queue
                    (tenant_id,source_id,logical_document_id,revision,reason,certification_checkpoint)
                  SELECT %s,source_id,logical_document_id,revision,'certification',
                    jsonb_build_object('version',{_CHECKPOINT_VERSION},'epoch',%s::text,'after','',
                      'revision',revision,'source_sha256',source_sha256,
                      'manifest_sha256',manifest_sha256,'policy',policy,
                      'authority_generation',authority_generation)
                  FROM scoped WHERE revision IS NOT NULL AND authority_generation IS NOT NULL
                    AND NOT logical_pending AND (additive_membership_generation OR NOT retirement_pending)
                    AND NOT retirement_intent
                  ON CONFLICT(tenant_id,source_id,logical_document_id) DO NOTHING
                  RETURNING source_id,logical_document_id
                )
                SELECT scoped.*,inserted.source_id IS NOT NULL AS queued,
                  (SELECT count(*) FROM page)>%s AS more
                FROM scoped LEFT JOIN inserted USING(source_id,logical_document_id)
                ORDER BY source_id,logical_document_id""",
                (
                    tenant_id,
                    after[0] if after else None,
                    *(after or (None, None)),
                    *through,
                    limit + 1,
                    limit,
                    tenant_id,
                    tenant_id,
                    settings.binding_epoch,
                    limit,
                ),
                deadline_at,
            ).fetchall()
            result["status"] = "bounded" if rows and rows[0]["more"] else "completed"
            for row in rows:
                identity = [row["source_id"], row["logical_document_id"]]
                reasons = (
                    ["publication_changed"]
                    if row["revision"] is None
                    else [
                        name
                        for name in (
                            "logical_pending",
                            "retirement_pending",
                            "retirement_intent",
                        )
                        if row[name] and not (
                            name == "retirement_pending" and row["additive_membership_generation"]
                        )
                    ]
                )
                if row["revision"] is not None and row["authority_generation"] is None:
                    reasons.append("authority_generation_missing")
                if row["queued"]:
                    result["queued"] += 1
                elif not reasons:
                    reasons.append("already_queued")
                if reasons:
                    result["deferred"].append(dict(key=identity, reasons=reasons))
                result["scanned"] += 1
                result["after"] = identity
    return result
