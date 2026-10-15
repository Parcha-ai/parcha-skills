"""Complete bounded locator-presence scan, never archive or clear authority."""

import time


def locator_sql(original):
    marker = "  FROM canonical_documents document"
    if original.count(marker) != 1:
        raise ValueError("document_query_shape")
    return original


def check(
    store,
    *,
    cfg,
    entry,
    deadline,
    owner_helpers,
    read_catalog,
    cohort_entry,
    authorize,
    document_sql,
    record_location,
    excluded_types,
):
    key = (cfg["tenant"], cfg["source"], entry["native_parent_id"])
    limits = cfg["limits"]
    with store.connect() as c, c.transaction():
        authorize(store, c, key[:2], cfg["owner"], min(deadline, time.monotonic() + 5))
        catalog = read_catalog(store, c, key, deadline, lock=True)
        if cohort_entry(catalog["manifest"]) != entry:
            raise ValueError("cohort_catalog_changed")
        marker = store._execute_bounded(
            c,
            "SELECT enabled FROM canonical_chunk_retirement_progress WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s",
            key,
            deadline,
        ).fetchone()
        if marker is not None:
            return dict(
                status="represented", documents=None, missing=None, archive_reads=0
            )
        if catalog["queue"] is not None:
            return dict(
                status="projection_pending",
                documents=None,
                missing=None,
                archive_reads=0,
            )
        rows = store._execute_bounded(
            c, locator_sql(document_sql), (*key, limits["max_documents"] + 1), deadline
        ).fetchall()
        if len(rows) > limits["max_documents"]:
            return dict(
                status="document_budget",
                documents=len(rows),
                missing=None,
                archive_reads=0,
            )
        if not rows:
            return dict(status="empty", documents=0, missing=None, archive_reads=0)
        if len({r["document_id"] for r in rows}) != len(rows):
            raise ValueError("duplicate_document")
        missing = 0
        outside = 0
        eligible = 0
        excluded_structural = 0
        excluded_oversized = 0
        for row in rows:
            if time.monotonic() >= deadline:
                raise TimeoutError("metadata_deadline")
            if row["raw_media_type"] == "application/vnd.recall.oversized-record+gzip":
                excluded_oversized += 1
                continue
            if excluded_types.intersection(row["structural_types"]):
                excluded_structural += 1
                continue
            eligible += 1
            location = record_location(row)
            if location is None:
                missing += 1
            elif location[1] > catalog["manifest"]["record_count"]:
                outside += 1
        current = read_catalog(store, c, key, deadline, lock=True)
        if cohort_entry(current["manifest"]) != entry:
            raise ValueError("cohort_catalog_changed")
        if current["queue"] is not None:
            return dict(
                status="projection_pending",
                documents=len(rows),
                missing=missing,
                archive_reads=0,
            )
    return dict(
        status="already_located_metadata"
        if not missing and not outside
        else "needs_publisher"
        if missing and not outside
        else "invalid_location",
        documents=len(rows),
        eligible_documents=eligible,
        excluded_structural=excluded_structural,
        excluded_oversized=excluded_oversized,
        missing=missing,
        outside=outside,
        archive_reads=0,
    )
