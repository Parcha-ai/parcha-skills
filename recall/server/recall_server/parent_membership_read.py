"""Exact current certificates may replace only per-receipt membership proof."""
from contextlib import contextmanager
from dataclasses import dataclass
import os
import time

from .db import SearchDeadlineExceeded
from .parent_membership_enrollment import _binding
from .parent_membership_shadow import _METADATA_SQL, ShadowSettings

# Reuse the existing optional-metadata phase limits, not its environment knobs.
_PHASE_LIMITS = ShadowSettings()


@dataclass(frozen=True)
class ReadSettings:
    enabled: bool = False
    binding_epoch: str = ''

    def __post_init__(self):
        if (type(self.enabled) is not bool or not isinstance(self.binding_epoch, str)
                or (self.enabled and not self.binding_epoch.strip())):
            raise ValueError('parent_membership_read_settings_invalid')

    @classmethod
    def from_env(cls):
        enabled = os.environ.get('RECALL_PARENT_MEMBERSHIP_READ', '0')
        if enabled not in ('0', '1'):
            raise ValueError('parent_membership_read_settings_invalid')
        return cls(enabled=enabled == '1', binding_epoch=os.environ.get('RECALL_PARENT_MEMBERSHIP_EPOCH', ''))


def partition(store, connection, *, selected, settings, deadline_at):
    """Return proven passage keys inside the caller's existing RR snapshot.

    Any incomplete or failed metadata batch falls back as a whole. Savepoint
    rollback preserves the original transaction for native receipt proof, whose
    deadline is unchanged. These keys never add sources or bypass the catalog.
    """
    @contextmanager
    def metadata(metadata_deadline):
        with connection.transaction():
            yield store._execute_bounded(connection, _METADATA_SQL,
                (selected[0]['tenant_id'], [r['source_id'] for r in selected],
                 [r['passage_id'] for r in selected]), metadata_deadline).fetchall()

    return _partition(selected=selected, settings=settings, deadline_at=deadline_at,
                      metadata=metadata)


def partition_fetched(*, selected, settings, deadline_at):
    """Validate metadata carried by the Parquet cursor in its existing RR snapshot.

    Missing inner-join counterparts remain incomplete metadata, not omitted
    passages. The PostgreSQL byte counts cover the identical metadata record
    used by the separate lookup; body and receipt bytes are not reclassified.
    """
    @contextmanager
    def metadata(metadata_deadline):
        rows = []
        size = 0
        for row in selected:
            if time.monotonic() >= metadata_deadline:
                raise SearchDeadlineExceeded()
            if row['membership_metadata_complete']:
                rows.append(dict(row['membership_metadata']))
                size += row['membership_metadata_bytes']
        yield [dict(row, selected_count=len(rows), metadata_bytes=size) for row in rows] or [
            dict(selected_count=0, metadata_bytes=0)]

    return _partition(selected=selected, settings=settings, deadline_at=deadline_at,
                      metadata=metadata)


def _partition(*, selected, settings, deadline_at, metadata):
    started = time.monotonic()

    def result(keys, status):
        return keys, dict(status=status, certified_passages=len(keys),
                          fallback_passages=len(selected) - len(keys),
                          elapsed_ms=round((time.monotonic() - started) * 1000, 3))

    if not settings.enabled:
        return result(set(), 'disabled')
    if not selected:
        return result(set(), 'empty')
    if started >= deadline_at:
        raise SearchDeadlineExceeded()
    metadata_deadline = min(deadline_at - _PHASE_LIMITS.serving_reserve_ms / 1000,
                            started + _PHASE_LIMITS.budget_ms / 1000)
    if metadata_deadline <= started:
        return result(set(), 'serving-reserve')
    try:
        with metadata(metadata_deadline) as rows:
            if not rows or rows[0]['metadata_bytes'] > 8388608:
                return result(set(), 'metadata-budget')
            by_key = {(r['source_id'], r['passage_id']): r for r in rows if r['selected_count']}
            expected = {(r['source_id'], r['passage_id']) for r in selected}
            if set(by_key) != expected or len(by_key) != len(selected):
                return result(set(), 'incomplete')
            certified = set()
            for row in selected:
                if time.monotonic() >= metadata_deadline:
                    raise SearchDeadlineExceeded()
                key = row['source_id'], row['passage_id']
                extra = by_key[key]
                if (extra['generation'] is not None
                        and extra['membership_generation'] == extra['generation']
                        and extra['membership_binding_sha256'] == _binding({**row, **extra},
                            {'binding_epoch': settings.binding_epoch})):
                    certified.add(key)
            if time.monotonic() >= metadata_deadline:
                raise SearchDeadlineExceeded()
        return result(certified, 'partitioned')
    except SearchDeadlineExceeded:
        # Returning no certificates cannot renew the outer request deadline.
        return result(set(), 'deadline')
    except Exception:
        return result(set(), 'unavailable')
