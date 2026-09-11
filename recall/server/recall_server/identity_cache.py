"""In-process cache of committed source registrations.

Recall is write-dominated: every collector write re-runs the identity
registration statements (tenant, principal, source, owner grant, read-grant
propagation) even when nothing changed. This cache remembers the identity
tuples whose registration is known to be committed so a repeat write runs
zero registration statements.

Safety rules:

* Only positive results are cached, and only when the ``canonical_sources``
  row already existed before the call. A brand-new source is registered
  inside the caller's transaction; if that transaction rolls back the cache
  never learned about it, so a later write re-runs the statements.
* Ownership never changes (there is no ``UPDATE canonical_sources``), so a
  cached tuple stays authoritative for the owner check.
* The read-grant propagation depends on tenant membership, so every
  membership, access-grant, and invitation mutation invalidates the tenant.
  The cache is per process; a TTL bounds cross-process staleness.
"""

from __future__ import annotations

import os
import threading
import time
from collections import OrderedDict

DEFAULT_TTL_SECONDS = 600
DEFAULT_MAX_ENTRIES = 10_000
MAX_TTL_SECONDS = 86_400
MAX_ENTRIES_CEILING = 1_000_000

RegistrationKey = tuple[str, str, str]


class IdentityRegistrationCache:
    """Bounded, TTL-expiring, thread-safe set of registered identity tuples."""

    def __init__(
        self,
        *,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        clock=time.monotonic,
    ):
        if (
            isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, (int, float))
            or not 0 <= ttl_seconds <= MAX_TTL_SECONDS
        ):
            raise ValueError(
                f"identity cache ttl must be between 0 and {MAX_TTL_SECONDS} seconds"
            )
        if (
            isinstance(max_entries, bool)
            or not isinstance(max_entries, int)
            or not 1 <= max_entries <= MAX_ENTRIES_CEILING
        ):
            raise ValueError(
                f"identity cache size must be between 1 and {MAX_ENTRIES_CEILING}"
            )
        self.ttl_seconds = float(ttl_seconds)
        self.max_entries = max_entries
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: OrderedDict[RegistrationKey, float] = OrderedDict()
        self.hits = 0
        self.misses = 0

    @classmethod
    def from_env(cls) -> "IdentityRegistrationCache":
        try:
            ttl = float(
                os.environ.get(
                    "RECALL_IDENTITY_CACHE_TTL_SECONDS", str(DEFAULT_TTL_SECONDS)
                )
            )
            size = int(
                os.environ.get("RECALL_IDENTITY_CACHE_MAX", str(DEFAULT_MAX_ENTRIES))
            )
        except ValueError as exc:
            raise ValueError("identity cache settings are invalid") from exc
        return cls(ttl_seconds=ttl, max_entries=size)

    @property
    def enabled(self) -> bool:
        return self.ttl_seconds > 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def contains(self, key: RegistrationKey) -> bool:
        """Return True when ``key`` is cached and fresh; refreshes LRU order."""
        if not self.enabled:
            return False
        now = self._clock()
        with self._lock:
            expires_at = self._entries.get(key)
            if expires_at is None:
                self.misses += 1
                return False
            if expires_at <= now:
                del self._entries[key]
                self.misses += 1
                return False
            self._entries.move_to_end(key)
            self.hits += 1
            return True

    def remember(self, key: RegistrationKey) -> None:
        if not self.enabled:
            return
        expires_at = self._clock() + self.ttl_seconds
        with self._lock:
            self._entries[key] = expires_at
            self._entries.move_to_end(key)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    def invalidate_tenant(self, tenant_id: str) -> int:
        """Drop every cached tuple for ``tenant_id``; returns the count removed."""
        with self._lock:
            stale = [key for key in self._entries if key[0] == tenant_id]
            for key in stale:
                del self._entries[key]
            return len(stale)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


REGISTRATION_CACHE = IdentityRegistrationCache.from_env()


def invalidate_tenant(tenant_id: str) -> int:
    """Process-wide invalidation hook for membership and grant mutations."""
    return REGISTRATION_CACHE.invalidate_tenant(tenant_id)
