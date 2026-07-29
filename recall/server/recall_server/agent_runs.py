"""Durable, authorization-bound lifecycle for Recall agent runs."""

from __future__ import annotations

import hashlib
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Protocol

from contracts.agent_v1 import derive_run_id, validate_agent_contract

from .agent import (
    AgentExecutionError,
    DelegationContext,
    RecallAgentService,
)


TERMINAL_STATUSES = frozenset({
    "complete",
    "partial",
    "no_answer",
    "failed",
    "cancelled",
})
SUCCESS_STATUSES = frozenset({"complete", "partial", "no_answer"})


class AgentRunConflict(RuntimeError):
    """An idempotency key was reused for different work."""


class AgentRunNotFound(LookupError):
    """The run is absent or outside the authenticated authority."""


class AgentRunUnavailable(RuntimeError):
    """The run cannot be started within its configured bounds."""


class AgentRunStateError(RuntimeError):
    """The requested lifecycle transition is invalid."""


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _request_sha256(request: dict[str, Any]) -> str:
    stable = {
        key: value
        for key, value in request.items()
        if key not in {"request_id", "idempotency_key"}
    }
    encoded = json.dumps(
        stable,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _trace_id(run_id: str) -> str:
    return f"trc_{hashlib.sha256(run_id.encode()).hexdigest()[:32]}"


def _safe_sources(context: DelegationContext) -> list[str]:
    return sorted(set(context.authorized_sources))


@dataclass(frozen=True)
class CreatedRun:
    run: dict[str, Any]
    task_id: str
    created: bool


class AgentRunBackend(Protocol):
    def create(
        self,
        context: DelegationContext,
        request: dict[str, Any],
        *,
        now: datetime,
    ) -> CreatedRun: ...

    def claim(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        lease_owner: str,
        now: datetime,
    ) -> dict[str, Any] | None: ...

    def complete(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        lease_owner: str,
        bundle: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]: ...

    def fail(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        lease_owner: str,
        error_code: str,
        trace: list[dict[str, Any]],
        now: datetime,
    ) -> dict[str, Any]: ...

    def get(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        now: datetime,
    ) -> dict[str, Any]: ...

    def result(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        now: datetime,
    ) -> dict[str, Any]: ...

    def cancel(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        now: datetime,
    ) -> dict[str, Any]: ...

    def recover_abandoned(self, *, before: datetime, now: datetime) -> int: ...

    def prune(self, *, before: datetime) -> int: ...

    def resolve_task(
        self,
        context: DelegationContext,
        task_id: str,
    ) -> str: ...


class PostgresAgentRunBackend:
    """Portable PostgreSQL implementation with no stored question or credential."""

    def __init__(
        self,
        connect: Callable[[], Any],
        *,
        max_active_per_principal: int = 4,
        lease_seconds: int = 600,
        retention_seconds: int = 7 * 24 * 60 * 60,
    ):
        if not 1 <= max_active_per_principal <= 32:
            raise ValueError("agent active-run bound is invalid")
        if not 15 <= lease_seconds <= 600:
            raise ValueError("agent lease bound is invalid")
        if not 60 <= retention_seconds <= 30 * 24 * 60 * 60:
            raise ValueError("agent retention bound is invalid")
        self.connect = connect
        self.max_active_per_principal = max_active_per_principal
        self.lease_seconds = lease_seconds
        self.retention_seconds = retention_seconds

    @staticmethod
    def _row_run(row: dict[str, Any]) -> dict[str, Any]:
        value = {
            "contract": "recall.agent-run.v1",
            "schema_version": 1,
            "run_id": row["run_id"],
            "request_id": row["request_id"],
            "tenant_id": row["tenant_id"],
            "principal_id": row["principal_id"],
            "trace_id": row["trace_id"],
            "status": row["status"],
            "attempt": row["attempt"],
            "created_at": _timestamp(row["created_at"]),
            "updated_at": _timestamp(row["updated_at"]),
        }
        if row.get("completed_at") is not None:
            value["completed_at"] = _timestamp(row["completed_at"])
        if row.get("error_code") is not None:
            value["error_code"] = row["error_code"]
        return validate_agent_contract(value, expected="recall.agent-run.v1")

    @staticmethod
    def _visible(row: dict[str, Any], context: DelegationContext) -> bool:
        return (
            row["tenant_id"] == context.tenant_id
            and row["principal_id"] == context.principal_id
            and set(row["source_ids"]) <= set(context.authorized_sources)
        )

    def _select(
        self,
        connection: Any,
        context: DelegationContext,
        run_id: str,
        *,
        lock: bool = False,
    ) -> dict[str, Any]:
        query = (
            """SELECT tenant_id,run_id,task_id,request_id,principal_id,trace_id,
                      request_sha256,source_ids,status,attempt,cancel_requested,
                      lease_owner,lease_expires_at,error_code,trace_events,result,
                      created_at,updated_at,started_at,completed_at
                 FROM agent_runs
                WHERE tenant_id=%s AND run_id=%s
                FOR UPDATE"""
            if lock
            else
            """SELECT tenant_id,run_id,task_id,request_id,principal_id,trace_id,
                      request_sha256,source_ids,status,attempt,cancel_requested,
                      lease_owner,lease_expires_at,error_code,trace_events,result,
                      created_at,updated_at,started_at,completed_at
                 FROM agent_runs
                WHERE tenant_id=%s AND run_id=%s"""
        )
        row = connection.execute(
            query,
            (context.tenant_id, run_id),
        ).fetchone()
        if row is None or not self._visible(row, context):
            raise AgentRunNotFound("agent run not found")
        return row

    def create(
        self,
        context: DelegationContext,
        request: dict[str, Any],
        *,
        now: datetime,
    ) -> CreatedRun:
        run_id = derive_run_id(context.principal_id, request["idempotency_key"])
        digest = _request_sha256(request)
        sources = _safe_sources(context)
        with self.connect() as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"recall:agent:{context.tenant_id}:{context.principal_id}",),
            )
            existing = connection.execute(
                """SELECT tenant_id,run_id,task_id,request_id,principal_id,trace_id,
                          request_sha256,source_ids,status,attempt,cancel_requested,
                          lease_owner,lease_expires_at,error_code,trace_events,result,
                          created_at,updated_at,started_at,completed_at
                     FROM agent_runs
                    WHERE tenant_id=%s AND run_id=%s
                    FOR UPDATE""",
                (context.tenant_id, run_id),
            ).fetchone()
            if existing is not None:
                if not self._visible(existing, context):
                    raise AgentRunNotFound("agent run not found")
                if existing["request_sha256"] != digest:
                    raise AgentRunConflict("agent idempotency conflict")
                return CreatedRun(
                    self._row_run(existing),
                    existing["task_id"],
                    False,
                )
            active = connection.execute(
                """SELECT count(*) AS value
                     FROM agent_runs
                    WHERE tenant_id=%s AND principal_id=%s
                      AND status IN ('queued','running')""",
                (context.tenant_id, context.principal_id),
            ).fetchone()["value"]
            if active >= self.max_active_per_principal:
                raise AgentRunUnavailable("agent concurrency bound reached")
            row = connection.execute(
                """INSERT INTO agent_runs(
                       tenant_id,run_id,task_id,request_id,principal_id,trace_id,
                       request_sha256,source_ids,status,created_at,updated_at
                   ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'queued',%s,%s)
                   RETURNING tenant_id,run_id,task_id,request_id,principal_id,trace_id,
                             request_sha256,source_ids,status,attempt,cancel_requested,
                             lease_owner,lease_expires_at,error_code,trace_events,result,
                             created_at,updated_at,started_at,completed_at""",
                (
                    context.tenant_id,
                    run_id,
                    f"tsk_{uuid.uuid4().hex}",
                    request["request_id"],
                    context.principal_id,
                    _trace_id(run_id),
                    digest,
                    sources,
                    now,
                    now,
                ),
            ).fetchone()
        return CreatedRun(self._row_run(row), row["task_id"], True)

    def claim(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        lease_owner: str,
        now: datetime,
    ) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = self._select(connection, context, run_id, lock=True)
            if row["status"] != "queued" or row["cancel_requested"]:
                return None
            row = connection.execute(
                """UPDATE agent_runs
                      SET status='running',lease_owner=%s,lease_expires_at=%s,
                          started_at=COALESCE(started_at,%s),updated_at=%s
                    WHERE tenant_id=%s AND run_id=%s AND status='queued'
                RETURNING tenant_id,run_id,task_id,request_id,principal_id,trace_id,
                          request_sha256,source_ids,status,attempt,cancel_requested,
                          lease_owner,lease_expires_at,error_code,trace_events,result,
                          created_at,updated_at,started_at,completed_at""",
                (
                    lease_owner,
                    now + timedelta(seconds=self.lease_seconds),
                    now,
                    now,
                    context.tenant_id,
                    run_id,
                ),
            ).fetchone()
        return self._row_run(row) if row is not None else None

    def complete(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        lease_owner: str,
        bundle: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        status = bundle["result"]["status"]
        if status not in SUCCESS_STATUSES:
            raise AgentRunStateError("agent completion status is invalid")
        trace = bundle["trace"]
        encoded_trace = json.dumps(trace, allow_nan=False).encode()
        encoded_result = json.dumps(bundle, allow_nan=False).encode()
        if len(encoded_trace) > 1_000_000 or len(encoded_result) > 1_000_000:
            raise AgentRunStateError("agent durable output exceeds its bound")
        with self.connect() as connection:
            row = self._select(connection, context, run_id, lock=True)
            if row["status"] == "cancelled":
                return self._row_run(row)
            if row["status"] != "running" or row["lease_owner"] != lease_owner:
                raise AgentRunStateError("agent completion lease is invalid")
            row = connection.execute(
                """UPDATE agent_runs
                      SET status=%s,lease_owner=NULL,lease_expires_at=NULL,
                          trace_events=%s::jsonb,result=%s::jsonb,
                          updated_at=%s,completed_at=%s
                    WHERE tenant_id=%s AND run_id=%s
                RETURNING tenant_id,run_id,task_id,request_id,principal_id,trace_id,
                          request_sha256,source_ids,status,attempt,cancel_requested,
                          lease_owner,lease_expires_at,error_code,trace_events,result,
                          created_at,updated_at,started_at,completed_at""",
                (
                    status,
                    json.dumps(trace),
                    json.dumps(bundle),
                    now,
                    now,
                    context.tenant_id,
                    run_id,
                ),
            ).fetchone()
        return self._row_run(row)

    def fail(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        lease_owner: str,
        error_code: str,
        trace: list[dict[str, Any]],
        now: datetime,
    ) -> dict[str, Any]:
        if len(json.dumps(trace, allow_nan=False).encode()) > 1_000_000:
            trace = []
        with self.connect() as connection:
            row = self._select(connection, context, run_id, lock=True)
            if row["status"] == "cancelled":
                return self._row_run(row)
            if row["status"] != "running" or row["lease_owner"] != lease_owner:
                raise AgentRunStateError("agent failure lease is invalid")
            row = connection.execute(
                """UPDATE agent_runs
                      SET status='failed',lease_owner=NULL,lease_expires_at=NULL,
                          error_code=%s,trace_events=%s::jsonb,
                          updated_at=%s,completed_at=%s
                    WHERE tenant_id=%s AND run_id=%s
                RETURNING tenant_id,run_id,task_id,request_id,principal_id,trace_id,
                          request_sha256,source_ids,status,attempt,cancel_requested,
                          lease_owner,lease_expires_at,error_code,trace_events,result,
                          created_at,updated_at,started_at,completed_at""",
                (
                    error_code,
                    json.dumps(trace),
                    now,
                    now,
                    context.tenant_id,
                    run_id,
                ),
            ).fetchone()
        return self._row_run(row)

    def get(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        now: datetime,
    ) -> dict[str, Any]:
        del now
        with self.connect() as connection:
            return self._row_run(self._select(connection, context, run_id))

    def result(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        now: datetime,
    ) -> dict[str, Any]:
        del now
        with self.connect() as connection:
            row = self._select(connection, context, run_id)
            if row["status"] in SUCCESS_STATUSES:
                bundle = row["result"]
                if not isinstance(bundle, dict):
                    raise AgentRunStateError("agent result is invalid")
                return bundle
            return {"run": self._row_run(row)}

    def cancel(
        self,
        context: DelegationContext,
        run_id: str,
        *,
        now: datetime,
    ) -> dict[str, Any]:
        with self.connect() as connection:
            row = self._select(connection, context, run_id, lock=True)
            if row["status"] in TERMINAL_STATUSES:
                raise AgentRunStateError("agent run is already terminal")
            row = connection.execute(
                """UPDATE agent_runs
                      SET status='cancelled',cancel_requested=true,
                          lease_owner=NULL,lease_expires_at=NULL,
                          error_code='cancelled_by_caller',
                          updated_at=%s,completed_at=%s
                    WHERE tenant_id=%s AND run_id=%s
                RETURNING tenant_id,run_id,task_id,request_id,principal_id,trace_id,
                          request_sha256,source_ids,status,attempt,cancel_requested,
                          lease_owner,lease_expires_at,error_code,trace_events,result,
                          created_at,updated_at,started_at,completed_at""",
                (now, now, context.tenant_id, run_id),
            ).fetchone()
        return self._row_run(row)

    def recover_abandoned(self, *, before: datetime, now: datetime) -> int:
        with self.connect() as connection:
            result = connection.execute(
                """UPDATE agent_runs
                      SET status='failed',lease_owner=NULL,lease_expires_at=NULL,
                          error_code='worker_lost_retryable',
                          updated_at=%s,completed_at=%s
                    WHERE (
                        status='queued' AND created_at < %s
                    ) OR (
                        status='running' AND lease_expires_at < %s
                    )""",
                (now, now, before, now),
            )
            return result.rowcount

    def prune(self, *, before: datetime) -> int:
        with self.connect() as connection:
            result = connection.execute(
                """DELETE FROM agent_runs
                    WHERE status IN (
                        'complete','partial','no_answer','failed','cancelled'
                    ) AND completed_at < %s""",
                (before,),
            )
            return result.rowcount

    def resolve_task(
        self,
        context: DelegationContext,
        task_id: str,
    ) -> str:
        with self.connect() as connection:
            row = connection.execute(
                """SELECT tenant_id,run_id,source_ids,principal_id
                     FROM agent_runs
                    WHERE tenant_id=%s AND task_id=%s""",
                (context.tenant_id, task_id),
            ).fetchone()
        if row is None or not self._visible(row, context):
            raise AgentRunNotFound("agent task not found")
        return row["run_id"]


class AgentRunCoordinator:
    """Own detached execution while the backend owns durable truth."""

    def __init__(
        self,
        service: RecallAgentService,
        backend: AgentRunBackend,
        *,
        clock: Callable[[], datetime] | None = None,
        workers: int = 4,
        abandon_after_seconds: int = 120,
        executor: Any | None = None,
    ):
        if not 1 <= workers <= 16:
            raise ValueError("agent worker bound is invalid")
        if not 15 <= abandon_after_seconds <= 600:
            raise ValueError("agent abandonment bound is invalid")
        self.service = service
        self.backend = backend
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.ttl_ms = int(
            getattr(backend, "retention_seconds", 7 * 24 * 60 * 60)
        ) * 1000
        self.abandon_after_seconds = abandon_after_seconds
        self.lease_owner = f"agent-worker-{uuid.uuid4().hex}"
        self._executor = executor or ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="recall-agent",
        )
        self._owns_executor = executor is None
        self._lock = threading.Lock()
        self._futures: set[Any] = set()

    def recover(self) -> int:
        now = self.clock()
        return self.backend.recover_abandoned(
            before=now - timedelta(seconds=self.abandon_after_seconds),
            now=now,
        )

    def close(self) -> None:
        if self._owns_executor:
            self._executor.shutdown(wait=False, cancel_futures=True)

    def _track(self, future: Any) -> None:
        with self._lock:
            self._futures.add(future)
        future.add_done_callback(self._discard)

    def _discard(self, future: Any) -> None:
        with self._lock:
            self._futures.discard(future)

    def start(
        self,
        principal: dict[str, Any],
        request: Any,
        retrieval: Any,
    ) -> dict[str, Any]:
        query, context = self.service.prepare(principal, request)
        created = self.backend.create(context, query, now=self.clock())
        if created.created:
            future = self._executor.submit(
                self._execute,
                context,
                query,
                retrieval,
                created.run["run_id"],
            )
            self._track(future)
        return {
            "run": created.run,
            "task_id": created.task_id,
            "ttl_ms": self.ttl_ms,
        }

    def use_recall(
        self,
        principal: dict[str, Any],
        request: Any,
        retrieval: Any,
    ) -> dict[str, Any]:
        query, context = self.service.prepare(principal, request)
        created = self.backend.create(context, query, now=self.clock())
        run_id = created.run["run_id"]
        if created.created:
            self._execute(context, query, retrieval, run_id)
        return self.backend.result(context, run_id, now=self.clock())

    def _execute(
        self,
        context: DelegationContext,
        request: dict[str, Any],
        retrieval: Any,
        run_id: str,
    ) -> None:
        claimed = self.backend.claim(
            context,
            run_id,
            lease_owner=self.lease_owner,
            now=self.clock(),
        )
        if claimed is None:
            return
        try:
            bundle = self.service.execute(request, context, retrieval)
            created_at = claimed["created_at"]
            now = self.clock()
            bundle["run"]["created_at"] = created_at
            bundle["run"]["updated_at"] = _timestamp(now)
            bundle["run"]["completed_at"] = _timestamp(now)
            bundle["run"]["attempt"] = claimed["attempt"]
            bundle["result"]["completed_at"] = _timestamp(now)
            self.backend.complete(
                context,
                run_id,
                lease_owner=self.lease_owner,
                bundle=bundle,
                now=now,
            )
        except Exception as error:
            code = (
                error.code
                if isinstance(error, AgentExecutionError)
                else "agent_execution_failed"
            )
            trace = (
                error.trace
                if isinstance(error, AgentExecutionError)
                else []
            )
            try:
                self.backend.fail(
                    context,
                    run_id,
                    lease_owner=self.lease_owner,
                    error_code=code,
                    trace=trace,
                    now=self.clock(),
                )
            except AgentRunStateError:
                pass

    def status(self, principal: dict[str, Any], run_id: str) -> dict[str, Any]:
        context = DelegationContext.from_principal(principal)
        return {"run": self.backend.get(context, run_id, now=self.clock())}

    def result(self, principal: dict[str, Any], run_id: str) -> dict[str, Any]:
        context = DelegationContext.from_principal(principal)
        return self.backend.result(context, run_id, now=self.clock())

    def cancel(self, principal: dict[str, Any], run_id: str) -> dict[str, Any]:
        context = DelegationContext.from_principal(principal)
        return {
            "run": self.backend.cancel(context, run_id, now=self.clock()),
        }

    def task_status(self, principal: dict[str, Any], task_id: str) -> dict[str, Any]:
        context = DelegationContext.from_principal(principal)
        run_id = self.backend.resolve_task(context, task_id)
        value = self.backend.get(context, run_id, now=self.clock())
        return {"run": value, "task_id": task_id, "ttl_ms": self.ttl_ms}

    def task_result(self, principal: dict[str, Any], task_id: str) -> dict[str, Any]:
        context = DelegationContext.from_principal(principal)
        run_id = self.backend.resolve_task(context, task_id)
        return self.backend.result(context, run_id, now=self.clock())

    def task_cancel(self, principal: dict[str, Any], task_id: str) -> dict[str, Any]:
        context = DelegationContext.from_principal(principal)
        run_id = self.backend.resolve_task(context, task_id)
        return {
            "run": self.backend.cancel(context, run_id, now=self.clock()),
            "task_id": task_id,
            "ttl_ms": self.ttl_ms,
        }


def backend_from_env(environment: dict[str, str], store: Any) -> PostgresAgentRunBackend:
    try:
        active = int(environment.get("RECALL_AGENT_MAX_ACTIVE_PER_PRINCIPAL", "4"))
        lease = int(environment.get("RECALL_AGENT_LEASE_SECONDS", "600"))
        retention = int(environment.get("RECALL_AGENT_RETENTION_SECONDS", "604800"))
    except ValueError as error:
        raise RuntimeError("Recall agent lifecycle configuration is invalid") from error
    return PostgresAgentRunBackend(
        store.connect,
        max_active_per_principal=active,
        lease_seconds=lease,
        retention_seconds=retention,
    )
