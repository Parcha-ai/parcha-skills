"""Resume optional native certification in the existing publication queue.

Only derived certificates are repeatable. The cursor advances after acknowledged
native proof; a lost response safely leaves the same IDs for fresh proof.
"""

import json
import time

from . import parent_membership_enrollment as native

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


def retain(connection, candidate, settings):
    """Called inside the existing publisher transaction, after its passage writes."""
    if not settings.enabled:
        return False
    available = connection.execute("""SELECT EXISTS(SELECT 1 FROM pg_attribute
        WHERE attrelid=to_regclass('canonical_passage_projection_queue')
          AND attname='certification_checkpoint' AND atttypid='jsonb'::regtype
          AND NOT attisdropped) AS available""").fetchone()["available"]
    if not available:
        return False
    scope = connection.execute(
        CURRENT,
        (candidate.tenant_id, candidate.source_id, candidate.logical_document_id),
    ).fetchone()
    if scope is None or scope["authority_generation"] is None:
        return False
    checkpoint = dict(
        version=1,
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
    """One finite parent slice; optional failures never repeat source publication."""
    certified = 0
    checkpoint = row["checkpoint"]
    first_slice = True
    if not settings.enabled:
        return dict(status="disabled", certified_passages=0)
    try:
        while time.monotonic() < deadline_at:
            with native._connect(store, deadline_at) as connection:
                scope = native._query(
                    store, connection, CURRENT, key(row), deadline_at
                ).fetchone()
                valid = (
                    isinstance(checkpoint, dict)
                    and checkpoint.get("version") == 1
                    and checkpoint.get("epoch") == settings.binding_epoch
                    and isinstance(checkpoint.get("after"), str)
                    and scope is not None
                    and all(checkpoint.get(name) == scope[name] for name in FIELDS)
                )
                busy = scope is not None and any(
                    scope[name]
                    for name in (
                        "logical_pending",
                        "retirement_pending",
                        "retirement_intent",
                    )
                )
                if valid and not busy and first_slice:
                    # Fair retries use the existing work-order timestamp, never
                    # a reservation. Release this short queue lock before proof.
                    scheduled = native._query(
                        store,
                        connection,
                        """UPDATE canonical_passage_projection_queue
                        SET changed_at=clock_timestamp()
                        WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s
                          AND generation=%s AND reason='certification'
                          AND certification_checkpoint=%s::jsonb""",
                        (*key(row), row["generation"], json.dumps(checkpoint)),
                        deadline_at,
                    )
                    if scheduled.rowcount != 1:
                        return dict(status="changed", certified_passages=certified)
                    first_slice = False
                rows = (
                    native._query(
                        store,
                        connection,
                        """SELECT passage_id FROM canonical_passages
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
                    if valid and not busy
                    else []
                )
            if not valid or busy:
                change_cursor(store, row, checkpoint, deadline_at=deadline_at)
                return dict(
                    status="busy" if busy else "changed", certified_passages=certified
                )
            if not rows:
                removed = change_cursor(store, row, checkpoint, deadline_at=deadline_at)
                return dict(
                    status="enrolled" if removed else "changed",
                    certified_passages=certified,
                )
            ids = tuple(value["passage_id"] for value in rows)
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
                if outcome["status"] not in ("failed", "deadline"):
                    change_cursor(store, row, checkpoint, deadline_at=deadline_at)
                return dict(status=outcome["status"], certified_passages=certified)
            if outcome["certified_passages"] != len(ids):
                raise ValueError("certification_batch_incomplete")
            if not change_cursor(
                store, row, checkpoint, deadline_at=deadline_at, after=ids[-1]
            ):
                return dict(status="changed", certified_passages=certified)
            checkpoint = dict(checkpoint, after=ids[-1])
        return dict(status="deadline", certified_passages=certified)
    except Exception:
        # Earlier commits remain acknowledged work; the uncertain batch's cursor
        # stays unchanged and a later ordinary slice can safely reprove it.
        return dict(status="failed", certified_passages=certified)


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
                native._INITIALIZE_GENERATION.format(selection=selection, passage_selection=''),
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
                    jsonb_build_object('version',1,'epoch',%s::text,'after','',
                      'revision',revision,'source_sha256',source_sha256,
                      'manifest_sha256',manifest_sha256,'policy',policy,
                      'authority_generation',authority_generation)
                  FROM scoped WHERE revision IS NOT NULL AND authority_generation IS NOT NULL
                    AND NOT logical_pending AND NOT retirement_pending AND NOT retirement_intent
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
                        if row[name]
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
