"""Share bounded SQL capacity between logical preparation and body thinning."""

from contextlib import contextmanager
import threading
import time

from psycopg_pool import PoolTimeout


class WorkerWriteStore:
    """Reserve general-pool connections for the publication coordinator.

    Only SQL checkouts consume capacity; archive work remains concurrent. This
    view supports the logical projector and thinner, not arbitrary BrainStore
    methods. Passage and search owners keep the original store.
    """

    def __init__(self, store, *, publication_reserve=2):
        if (type(publication_reserve) is not int or publication_reserve not in (2, 4)
                or store.pool_max_size - store.reserved_read_connections <= publication_reserve):
            raise ValueError("worker publication reservation is invalid")
        self._store = store
        self.pool_max_size = store.pool_max_size
        self.verified_singleton_receipts = store.verified_singleton_receipts
        self._capacity = store.pool_max_size - store.reserved_read_connections - publication_reserve
        self._available = self._capacity
        self._condition = threading.Condition()
        self._local = threading.local()

    @contextmanager
    def _admit(self, count, deadline, *, partial=False):
        if getattr(self._local, "active", False):
            raise RuntimeError("nested worker checkout is unsupported")
        with self._condition:
            while self._available < (1 if partial else count):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PoolTimeout("worker SQL admission timed out")
                self._condition.wait(remaining)
            if partial:
                count = min(count, self._available)
            self._available -= count
        self._local.active = True
        try:
            yield count
        finally:
            self._local.active = False
            with self._condition:
                self._available += count
                self._condition.notify_all()

    @contextmanager
    def connect(self):
        deadline = time.monotonic() + 5.0
        with self._admit(1, deadline):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PoolTimeout("worker SQL admission timed out")
            with self._store.connect(timeout=remaining) as connection:
                self._local.connection = connection
                try:
                    yield connection
                finally:
                    self._local.connection = None

    def prepare_pool(self, minimum_size, *, timeout=60.0):
        if (
            isinstance(minimum_size, bool)
            or not isinstance(minimum_size, int)
            or not 1 <= minimum_size <= self.pool_max_size - self._store.reserved_read_connections
            or not 1 <= timeout <= 300
        ):
            raise ValueError("database pool warmup budget is invalid")
        count = min(minimum_size, self._capacity)
        deadline = time.monotonic() + min(timeout, 5.0)
        with self._admit(count, deadline, partial=True) as admitted:
            remaining = deadline - time.monotonic()
            if remaining < 1:
                raise PoolTimeout("worker SQL warmup admission timed out")
            self._store.prepare_pool(admitted, timeout=remaining)

    def _owned(self, connection):
        if (
            connection is not getattr(self._local, "connection", None)
            or connection is None
        ):
            raise RuntimeError("worker SQL requires its admitted connection")

    def _execute_bounded(self, connection, sql, values, deadline_at):
        self._owned(connection)
        return self._store._execute_bounded(connection, sql, values, deadline_at)

    def _set_statement_deadline(self, connection, deadline_at):
        self._owned(connection)
        return self._store._set_statement_deadline(connection, deadline_at)
