"""Remaining exact new parents: original locator owner then absent-only enrollment."""

import hashlib
import time

LIMITS = dict(
    max_archive_bytes=256 * 1024**2,
    max_parent_archive_bytes=128 * 1024**2,
    max_spool_bytes=256 * 1024**2,
    max_documents=200000,
    max_chunks=800000,
    max_records=400000,
    max_parts=4096,
    batch_documents=64,
    batch_chunks=2000,
    hash_bytes=8 * 1024**2,
    max_locator_batches=512,
)


def execute(
    store,
    delegate,
    *,
    cfg,
    owner_helpers,
    runner,
    locators,
    limits_type,
    authorize,
    emit,
    metadata_check,
    clock=time.monotonic,
    deadline_at=None,
):
    plans = cfg["plans"]
    req = owner_helpers.require
    req(1 <= len(plans) <= 50, "canary_count")
    keys = []
    for plan in plans:
        unsigned = {k: v for k, v in plan.items() if k != "proof_sha256"}
        req(
            plan["proof_sha256"]
            == hashlib.sha256(owner_helpers.canonical(unsigned)).hexdigest(),
            "plan_digest",
        )
        req(
            plan["contract"] == "recall.retirement-cohort.v1"
            and len(plan["parents"]) == 1,
            "singleton_plan",
        )
        entry = plan["parents"][0]
        source = entry["source_id"]
        keys.append((source, entry["native_parent_id"]))
        req(
            source in cfg["sources"]
            and plan["scope"]
            == dict(
                tenant_id=cfg["tenant"], principal_id=cfg["owner"], source_ids=[source]
            ),
            "plan_scope",
        )
    req(keys == sorted(set(keys)), "ordered_unique_canary")
    started = clock()
    deadline = (
        min(started + 260, deadline_at) if deadline_at is not None else started + 260
    )
    archive = owner_helpers.ArchiveBudget(
        delegate, LIMITS["max_archive_bytes"], deadline
    )
    totals = dict(published_documents=0, publication_batches=0, enrolled=0, preserved=0)
    progress = []
    for index, plan in enumerate(plans):
        row = dict(index=index, status="unattempted")
        progress.append(row)
        if deadline - clock() < 30:
            break
        emit("parent", dict(index=index, status="metadata_started"))
        entry = plan["parents"][0]
        source = entry["source_id"]
        local = dict(
            tenant=cfg["tenant"], owner=cfg["owner"], source=source, limits=LIMITS
        )
        key = (cfg["tenant"], source)
        with store.connect() as c, c.transaction():
            authorize(store, c, key, cfg["owner"], min(deadline, clock() + 5))
        info = owner_helpers.admission(store, local, entry, min(deadline, clock() + 5))
        if info["status"] not in ("ready", "archive_budget"):
            row["status"] = info["status"]
            emit("parent", dict(row))
            continue
        meta = metadata_check(
            cfg=local, entry=entry, deadline=min(deadline, clock() + 20)
        )
        row["metadata"] = meta
        fast = meta["status"] == "already_located_metadata"
        if fast:
            row.update(status="already_located_metadata", locator_mode="metadata_noop")
            emit("parent", dict(row))
        else:
            if meta["status"] != "needs_publisher":
                row["status"] = meta["status"]
                emit("parent", dict(row))
                continue
            row["locator_mode"] = "original_publisher"
            if info["status"] != "ready":
                row["status"] = info["status"]
                emit("parent", dict(row))
                continue
            if (
                info["archive_bytes"] > archive.available
                or totals["publication_batches"] >= LIMITS["max_locator_batches"]
            ):
                row["status"] = "global_budget"
                emit("parent", dict(row))
                break
            row["status"] = "publication_inflight"
            emit("parent", dict(row))
            try:
                args = {
                    k: LIMITS[k]
                    for k in (
                        "max_documents",
                        "max_chunks",
                        "max_records",
                        "max_parts",
                        "max_spool_bytes",
                        "batch_documents",
                        "batch_chunks",
                        "hash_bytes",
                    )
                }
                report = locators.publish_parent_locators(
                    store,
                    archive,
                    tenant_id=cfg["tenant"],
                    source_id=source,
                    native_parent_id=entry["native_parent_id"],
                    owner_principal_id=cfg["owner"],
                    apply=True,
                    reviewed_plan=owner_helpers.locator_pin(local, entry),
                    deadline_at=deadline,
                    limits=limits_type(
                        **args,
                        max_archive_bytes=min(
                            LIMITS["max_parent_archive_bytes"], archive.available
                        ),
                        max_clear_bytes=1,
                        max_batches=LIMITS["max_locator_batches"]
                        - totals["publication_batches"],
                    ),
                )
            except Exception as error:
                row.update(
                    status="publication_unknown"
                    if getattr(error, "commit_unknown", True)
                    else "publication_failed",
                    error_class=type(error).__name__,
                    code=owner_helpers.safe_failure_code(error),
                    committed=getattr(error, "committed", None),
                )
                row.update(
                    archive_attempted_bytes=archive.bytes, archive_gets=archive.gets
                )
                emit("parent", dict(row))
                raise
            totals["publication_batches"] += report["batches"]
            totals["published_documents"] += report["published_documents"]
            row.update(
                status="located" if report["complete"] else "locator_partial",
                publication={k: v for k, v in report.items() if k != "plan"},
            )
            emit("parent", dict(row))
            if not report["complete"]:
                break
        # Reuse original admission again: refuse all intervening ledger states/queue changes.
        info = owner_helpers.admission(store, local, entry, min(deadline, clock() + 5))
        if info["status"] not in (("ready", "archive_budget") if fast else ("ready",)):
            row["status"] = "postpublication_" + info["status"]
            emit("parent", dict(row))
            continue
        row["status"] = "enrollment_inflight"
        emit("parent", dict(row))
        try:
            result = runner.enroll_cohort(
                store,
                scope=runner.RetirementScope(cfg["tenant"], cfg["owner"], (source,)),
                reviewed_plan=plan,
                deadline_at=min(deadline, clock() + 5),
            )
        except Exception as error:
            row.update(status="enrollment_unknown", error_class=type(error).__name__)
            row.update(archive_attempted_bytes=archive.bytes, archive_gets=archive.gets)
            emit("parent", dict(row))
            raise
        totals["enrolled"] += result["inserted_parents"]
        totals["preserved"] += result["preserved_parents"]
        row.update(
            status="enrolled" if result["inserted_parents"] else "preserved",
            enrollment=result,
        )
        emit("parent", dict(row))
    return dict(
        status="bounded_complete",
        plans=len(plans),
        attempted=len(progress),
        progress=progress,
        totals=totals,
        archive_attempted_bytes=archive.bytes,
        archive_gets=archive.gets,
        elapsed_ms=round((clock() - started) * 1000),
        cleared_documents=0,
        clear_calls=0,
        minimum_grace_seconds=60,
    )
