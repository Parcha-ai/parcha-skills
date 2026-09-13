#!/usr/bin/env python3
"""PostgreSQL E2E for the dedicated embedding worker and its daily cap (H5-2/H5-3).

Runs against a fresh pgvector container with a synthetic embedding runtime:
projects one session into lossless passages, runs ``embedding-worker --once``
through the CLI (runtime injected), and asserts the ledger row, the cap, the
``--skip-embedding`` projection cycle, and the ``/metrics`` gauges.
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import uuid
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


RECALL = Path(__file__).resolve().parents[2]
SERVER = RECALL / "server"
sys.path.insert(0, str(RECALL))
sys.path.insert(0, str(SERVER))

from e2e_logical_evidence_projection import (  # noqa: E402
    insert_record,
    insert_source,
)
from e2e_lossless_passages import SyntheticEmbeddingRuntime  # noqa: E402
from recall_server import cli  # noqa: E402
from recall_server.archive import FilesystemArchiveStore  # noqa: E402
from recall_server.db import BrainStore  # noqa: E402
from recall_server.embedding_ledger import window_total  # noqa: E402
from recall_server.logical_evidence import (  # noqa: E402
    LogicalEvidenceProjectionStore,
)
from recall_server.logical_evidence_projection import (  # noqa: E402
    CanonicalLogicalEvidenceProjector,
)
from recall_server.passage_index import CanonicalPassageProjector  # noqa: E402
from recall_server.passage_projection import PassagePolicy  # noqa: E402
from recall_server.projection_worker import (  # noqa: E402
    run_embedding_worker,
    run_projection_worker,
)


def run_cli(runtime: SyntheticEmbeddingRuntime, *argv: str) -> dict:
    """Run one CLI subcommand in-process with the synthetic runtime injected."""

    output = io.StringIO()
    with mock.patch.object(cli.SemanticRuntime, "from_env", staticmethod(lambda: runtime)), \
            mock.patch.object(cli, "build_rerank_runtime", lambda: None), \
            mock.patch.object(sys, "argv", ["recall-server", *argv]), \
            redirect_stdout(output):
        cli.main()
    return json.loads(output.getvalue().strip().splitlines()[-1])


def ledger_rows(connection, tenant: str) -> list[tuple[str, int]]:
    return [
        (str(row["day"]), int(row["embedded"]))
        for row in connection.execute(
            """SELECT day,embedded FROM canonical_embedding_ledger
                WHERE tenant_id=%s ORDER BY day""",
            (tenant,),
        ).fetchall()
    ]


def main() -> None:
    runtime = SyntheticEmbeddingRuntime()
    store = BrainStore(
        os.environ["RECALL_DATABASE_URL"],
        semantic_runtime=runtime,  # type: ignore[arg-type]
    )
    store.migrate()
    nonce = uuid.uuid4().hex
    tenant = f"tenant:embed-e2e:{nonce}"
    principal = f"principal:embed-e2e:{nonce}"
    source = f"codex:embed-e2e:{nonce}"
    parent = f"session-embed-e2e-{nonce}"
    with store.connect() as connection:
        insert_source(connection, tenant, principal, source)
        for index, (role, text) in enumerate(
            (
                ("user", "why did the gateway preserve tenant boundaries here"),
                ("assistant", "the gateway now intersects every explicit source grant"),
                ("user", "and the embedding worker keeps its own daily budget"),
                ("assistant", "yes the ledger table is read before every cycle"),
            )
        ):
            insert_record(
                connection,
                tenant=tenant,
                source=source,
                parent=parent,
                native=f"{parent}:{index}",
                text=text,
                role=role,
                byte_start=index * 10,
            )

    with tempfile.TemporaryDirectory(prefix="recall-embed-e2e-") as value:
        archive = FilesystemArchiveStore(Path(value) / "archive", namespace_key=b"p" * 32)
        logical_store = LogicalEvidenceProjectionStore(archive)
        logical = CanonicalLogicalEvidenceProjector(
            store, logical_store, bound_tenant_id=tenant, raw_archive=archive
        )
        assert logical.seed_backfill(tenant_id=tenant) == 1
        policy = PassagePolicy(target_tokens=4, overlap_tokens=1)
        passages = CanonicalPassageProjector(
            store, logical_store, policy=policy, bound_tenant_id=tenant
        )

        # 1. The projection worker with --skip-embedding projects logical and
        #    passage planes (passages become eligible the cycle after their
        #    logical document, so two cycles) but never touches the provider.
        cycle = run_projection_worker(
            logical,
            passages,
            None,
            tenant_id=tenant,
            logical_batch_size=10,
            passage_batch_size=10,
            embedding_batch_size=100,
            max_batches_per_cycle=2,
            upload_concurrency=1,
            passage_concurrency=2,
            interval_seconds=1,
            max_cycles=2,
            sleep=lambda _seconds: None,
            skip_embedding=True,
        )
        assert cycle["passage_documents"] == 1, cycle
        assert cycle["passages"] >= 2, cycle
        assert cycle["embedded"] == 0 and cycle["embed_elapsed_ms"] == 0, cycle
        assert runtime.document_calls == 0
        total_passages = int(cycle["passages"])
        with store.connect() as connection:
            assert window_total(connection, tenant_id=tenant) == 0
            assert ledger_rows(connection, tenant) == []

        # 2. embedding-worker --once through the CLI with a cap below the
        #    backlog: exactly --daily-cap passages are embedded, the ledger row
        #    exists, and the cycle reports itself capped.
        first = run_cli(
            runtime,
            "embedding-worker",
            "--tenant", tenant,
            "--target-tokens", "4", "--overlap-tokens", "1",
            "--batch-size", "100", "--max-batches-per-cycle", "2",
            "--daily-cap", "1",
            "--once",
        )
        assert first["embedded"] == 1, first
        assert first["status"] == "capped", first
        assert first["cap_remaining"] == 0 and first["cap"] == 1, first
        assert first["lag"] == total_passages - 1, (first, total_passages)
        assert runtime.document_calls == 1
        with store.connect() as connection:
            rows = ledger_rows(connection, tenant)
            assert len(rows) == 1 and rows[0][1] == 1, rows
            assert window_total(connection, tenant_id=tenant) == 1
            embedded_rows = connection.execute(
                "SELECT count(*) AS n FROM canonical_passage_embeddings WHERE tenant_id=%s",
                (tenant,),
            ).fetchone()["n"]
            assert embedded_rows == 1, embedded_rows

        # 3. Same cap, fresh process: the ledger says the cap is reached, so the
        #    provider is not called at all.
        capped = run_cli(
            runtime,
            "embedding-worker",
            "--tenant", tenant,
            "--target-tokens", "4", "--overlap-tokens", "1",
            "--daily-cap", "1",
            "--once",
        )
        assert capped["status"] == "capped" and capped["embedded"] == 0, capped
        assert runtime.document_calls == 1

        # 4. The cap from the environment (larger) drains the rest; the ledger
        #    accumulates in the same UTC-day row.
        with mock.patch.dict(os.environ, {"RECALL_EMBEDDING_DAILY_CAP": "1000"}):
            drained = run_cli(
                runtime,
                "embedding-worker",
                "--tenant", tenant,
                "--target-tokens", "4", "--overlap-tokens", "1",
                "--batch-size", "100", "--max-batches-per-cycle", "2",
                "--once",
            )
        assert drained["status"] == "complete", drained
        assert drained["embedded"] == total_passages - 1, (drained, total_passages)
        assert drained["lag"] == 0 and drained["cap"] == 1000, drained
        assert drained["cap_remaining"] == 1000 - total_passages, drained
        with store.connect() as connection:
            rows = ledger_rows(connection, tenant)
            assert len(rows) == 1 and rows[0][1] == total_passages, rows
            assert window_total(connection, tenant_id=tenant) == total_passages
            assert window_total(connection) >= total_passages

        # 5. Library entry point behaves the same and the /metrics gauges read
        #    from the ledger (all tenants) and the environment.
        idle = run_embedding_worker(
            passages, store, tenant_id=tenant, batch_size=100,
            max_batches_per_cycle=2, interval_seconds=1, daily_cap=1000, once=True,
        )
        assert idle["status"] == "complete" and idle["embedded"] == 0, idle
        metrics = store.service_metrics()
        assert int(metrics["embedding_daily_total"]) >= total_passages, metrics
        assert int(metrics["passages_unembedded"]) >= 0, metrics

    print(
        json.dumps(
            {
                "status": "ok",
                "passages": total_passages,
                "ledger": rows,
                "embedding_daily_total": int(metrics["embedding_daily_total"]),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
