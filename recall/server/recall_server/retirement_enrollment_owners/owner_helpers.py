import hashlib
import time

PUBLICATION_FAILURE_CODES = frozenset(
    (
        "locator_publication_unavailable",
        "locator_publication_retirement_enabled",
        "locator_publication_lock_busy",
        "locator_publication_document_changed",
        "locator_publication_position_changed",
        "locator_publication_parent_changed",
        "locator_publication_chunks_changed",
        "locator_publication_interrupted",
        "locator_publication_batch_unavailable",
        "locator_publication_request_invalid",
        "locator_publication_review_changed",
    )
)


def safe_failure_code(error):
    # Never stringify arbitrary SQL, archive, or transport exception messages.
    code = error.args[0] if len(error.args) == 1 else None
    return (
        code
        if type(error).__name__ == "LocatorPublicationError"
        and type(code) is str
        and code in PUBLICATION_FAILURE_CODES
        else "unclassified"
    )


def require(value, code):
    if not value:
        raise ValueError(code)


def canonical(value):
    import orjson

    return orjson.dumps(value, option=orjson.OPT_SORT_KEYS, default=str)


def admission(store, cfg, entry, deadline):
    from recall_server.retirement_runner import _cohort_entry
    from recall_server.parent_chunk_proof import read_parent_catalog

    scope = (cfg["tenant"], cfg["source"], entry["native_parent_id"])
    with store.connect() as c:
        row = read_parent_catalog(store, c, scope, deadline)
        require(_cohort_entry(row["manifest"]) == entry, "cohort_catalog_changed")
        marker = store._execute_bounded(
            c,
            """SELECT enabled FROM canonical_chunk_retirement_progress
            WHERE tenant_id=%s AND source_id=%s AND native_parent_id=%s""",
            scope,
            deadline,
        ).fetchone()
        if marker is not None:
            return dict(
                status="already_enabled" if marker["enabled"] else "disabled",
                archive_bytes=0,
            )
        if row["queue"] is not None:
            return dict(status="pending", archive_bytes=0)
        m = row["manifest"]
        limits = cfg["limits"]
        if (
            m["record_count"] > limits["max_records"]
            or m["part_count"] > limits["max_parts"]
        ):
            return dict(status="manifest_budget", archive_bytes=0)
        parts = store._execute_bounded(
            c,
            """SELECT size_bytes FROM canonical_evidence_document_parts
            WHERE tenant_id=%s AND source_id=%s AND logical_document_id=%s AND revision=%s
            ORDER BY part_ordinal LIMIT %s""",
            (
                *scope[:2],
                m["logical_document_id"],
                m["revision"],
                limits["max_parts"] + 1,
            ),
            deadline,
        ).fetchall()
    require(
        len(parts) == m["part_count"]
        and all(type(p["size_bytes"]) is int and p["size_bytes"] > 0 for p in parts),
        "part_metadata",
    )
    size = sum(p["size_bytes"] for p in parts)
    return dict(
        status="ready"
        if size <= limits["max_parent_archive_bytes"]
        else "archive_budget",
        archive_bytes=size,
    )


def locator_pin(cfg, entry):
    value = dict(
        contract="recall.streaming-locators.v1",
        tenant_id=cfg["tenant"],
        source_id=cfg["source"],
        native_parent_id=entry["native_parent_id"],
        manifest=entry["manifest"],
        catalog_created_at=entry["catalog_created_at"],
    )
    return dict(value, proof_sha256=hashlib.sha256(canonical(value)).hexdigest())


class ArchiveBudget:
    def __init__(self, delegate, limit, deadline):
        self.delegate = delegate
        self.limit = limit
        self.deadline = deadline
        self.bytes = 0
        self.gets = 0
        self.reserved = 0
        self.io_ms = 0.0

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    @property
    def available(self):
        return self.limit - self.bytes - self.reserved

    def _read(self, ref, deadline_at=None):
        size = ref.get("size_bytes")
        require(
            type(size) is int and 0 < size <= self.available, "source_archive_budget"
        )
        self.bytes += size
        self.gets += 1
        started = time.monotonic()
        try:
            bounded = getattr(self.delegate, "read_raw_bounded", None)
            return (
                bounded(
                    ref, deadline_at=min(self.deadline, deadline_at or self.deadline)
                )
                if callable(bounded)
                else self.delegate.read_raw(ref)
            )
        finally:
            self.io_ms += (time.monotonic() - started) * 1000

    def read_raw(self, ref):
        return self._read(ref)

    def read_raw_bounded(self, ref, *, deadline_at):
        return self._read(ref, deadline_at)
