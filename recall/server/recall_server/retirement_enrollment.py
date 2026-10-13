"""Connect a finite campaign to the existing PostgreSQL/archive owners."""

import time

from . import retirement_runner as runner, streaming_locators as locators
from .chunk_retirement import ParentRetirementLimits, require_retirement_owner
from .db import SearchDeadlineExceeded
from .parent_chunk_proof import read_parent_catalog
from .chunk_bodies import _record_location, _EXCLUDED_TYPES
from .locator_backfill_plan import _DOCUMENTS_SQL
from .retirement_campaign import digest, seal_inventory
from .retirement_enrollment_owners import metadata, operator, owner_helpers


def capture_page(store, *, scope, after=None, limit=100, deadline_at):
    """One resumable metadata page. Every considered identity advances the cursor."""
    if len(scope.source_ids) != 1:
        raise ValueError("capture_one_source_per_page")
    (source,) = scope.source_ids
    page = runner.plan_cohort(
        store, scope=scope, after=after, limit=limit, deadline_at=deadline_at
    )
    candidates = page["parents"]
    excluded = set()
    if candidates:
        with store.connect() as connection, connection.transaction():
            require_retirement_owner(
                store,
                connection,
                (scope.tenant_id, source),
                scope.principal_id,
                min(deadline_at, time.monotonic() + 5),
            )
            keys = [entry["native_parent_id"] for entry in candidates]
            represented = store._execute_bounded(
                connection,
                """
                SELECT native_parent_id FROM canonical_chunk_retirement_progress
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=ANY(%s)
                UNION SELECT native_parent_id FROM canonical_evidence_document_queue
                WHERE tenant_id=%s AND source_id=%s AND native_parent_id=ANY(%s)
                """,
                (scope.tenant_id, source, keys, scope.tenant_id, source, keys),
                min(deadline_at, time.monotonic() + 5),
            ).fetchall()
            excluded = {row["native_parent_id"] for row in represented}
    plans = []
    for entry in candidates:
        if entry["native_parent_id"] in excluded:
            continue
        plan = dict(
            contract=page["contract"],
            scope=page["scope"],
            parents=[entry],
            after=page["after"],
            more=page["more"],
            next_cursor=[source, entry["native_parent_id"]],
        )
        plans.append(dict(plan, proof_sha256=digest(plan)))
    return dict(
        contract="recall.retirement-capture-page.v1",
        scope=page["scope"],
        plans=plans,
        considered=page["parents"],
        excluded=sorted(excluded),
        after=page["after"],
        next_cursor=page["next_cursor"],
        more=page["more"],
    )


def capture_inventory(store, *, scope, deadline_at):
    """Compatibility capture for a small one-page finite canary only.

    Production callers persist each capture_page result and supply next_cursor;
    this helper refuses a tail rather than silently authorizing a partial set.
    """
    page = capture_page(store, scope=scope, deadline_at=deadline_at)
    if page["more"]:
        raise ValueError("use_resumable_capture_pages")
    return seal_inventory(page["plans"])


class EnrollmentOwner:
    def __init__(self, store, archive, *, deadline_at=None):
        self.store, self.archive = store, archive
        self.deadline_at = deadline_at

    def __call__(self, plan, emit):
        return self.quantum([plan], emit)[0]

    def quantum(self, plans, emit):
        # One original operator invocation shares its archive/batch/time budgets
        # across the entire quantum; never multiply those budgets per parent.
        scope = plans[0]["scope"]
        cfg = dict(
            plans=plans,
            tenant=scope["tenant_id"],
            owner=scope["principal_id"],
            sources=scope["source_ids"],
        )
        runner.RetirementScope(cfg["tenant"], cfg["owner"], tuple(cfg["sources"]))
        latest = None
        progress = {}

        def record(name, value):
            nonlocal latest
            # The durable sink must succeed before a mutation-inflight event returns.
            emit(name, value)
            latest = value
            progress[value.get("index", 0)] = value

        def check_metadata(**kw):
            return metadata.check(
                self.store,
                owner_helpers=owner_helpers,
                read_catalog=read_parent_catalog,
                cohort_entry=runner._cohort_entry,
                authorize=require_retirement_owner,
                document_sql=_DOCUMENTS_SQL,
                record_location=_record_location,
                excluded_types=_EXCLUDED_TYPES,
                **kw,
            )

        try:
            result = operator.execute(
                self.store,
                self.archive,
                cfg=cfg,
                owner_helpers=owner_helpers,
                runner=runner,
                locators=locators,
                limits_type=ParentRetirementLimits,
                authorize=require_retirement_owner,
                emit=record,
                metadata_check=check_metadata,
                deadline_at=self.deadline_at,
            )
        except Exception as error:
            status = (latest or {}).get("status")
            index = (latest or {}).get("index", 0)
            prefix = [
                dict(
                    status=progress[n]["status"],
                    certainty="known",
                    owner_result=progress[n],
                )
                for n in range(index)
            ]
            if status in (
                "publication_inflight",
                "enrollment_inflight",
                "publication_unknown",
                "enrollment_unknown",
            ):
                return prefix + [
                    dict(
                        status=status,
                        certainty="unknown",
                        error_class=type(error).__name__,
                        owner_result=latest,
                    )
                ]
            if status == "publication_failed":
                return prefix + [
                    dict(status=status, certainty="known", owner_result=latest)
                ]
            sqlstate = getattr(error, "sqlstate", None)
            if (
                isinstance(error, (TimeoutError, SearchDeadlineExceeded))
                or sqlstate == "57014"
            ):
                code = "metadata_timeout"
            elif sqlstate == "55P03":
                code = "metadata_lock_busy"
            elif type(error) is ValueError and error.args == (
                "cohort_catalog_changed",
            ):
                code = "catalog_changed"
            else:
                raise
            return prefix + [
                dict(
                    status=code,
                    certainty="known",
                    error_class=type(error).__name__,
                    owner_result=latest,
                )
            ]
        outcomes = []
        for row in result["progress"]:
            certainty = (
                "unattempted"
                if row["status"] in ("unattempted", "global_budget")
                else "known"
            )
            outcomes.append(
                dict(
                    status=row["status"],
                    certainty=certainty,
                    owner_result=result if not outcomes else row,
                )
            )
        return outcomes
