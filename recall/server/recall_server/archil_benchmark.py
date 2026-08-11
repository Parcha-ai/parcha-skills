"""Operator-only blocked latency benchmark over real Parquet scan shards."""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import statistics
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from .db import BrainStore
from .deep_inspection import AgentExecObject, DeepInspectionError
from .deep_inspection_runtime import build_deep_inspector


VARIANTS = ("stage_only", "duckdb_start", "record_count", "filtered_join")


@dataclass(frozen=True)
class Cohort:
    bucket_start: date
    objects: tuple[AgentExecObject, ...]
    aliases: dict[str, str]
    rows: int
    bytes: int
    first_occurred_at: datetime
    last_occurred_at: datetime


def _cohorts(store: BrainStore, tenant_id: str) -> tuple[Cohort, ...]:
    with store.connect() as connection:
        rows = connection.execute(
            """SELECT source_id,bucket_start,dataset,shard_index,object_key,
                      content_sha256,row_count,size_bytes,
                      first_occurred_at,last_occurred_at
                 FROM canonical_parquet_scan_shards
                WHERE tenant_id=%s
                ORDER BY source_id,bucket_start,dataset,shard_index""",
            (tenant_id,),
        ).fetchall()
    grouped: dict[tuple[str, date], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["source_id"], row["bucket_start"])].append(row)
    cohorts = []
    for (_, bucket_start), shards in grouped.items():
        if {row["dataset"] for row in shards} != {
            "actors",
            "documents",
            "records",
        }:
            continue
        records = [row for row in shards if row["dataset"] == "records"]
        aliases = {
            row["object_key"]: (
                f"s1/{bucket_start.isoformat()[:7]}/{row['dataset']}"
                f"-part-{int(row['shard_index']):05d}.parquet"
            )
            for row in shards
        }
        cohorts.append(Cohort(
            bucket_start=bucket_start,
            objects=tuple(
                AgentExecObject(
                    object_key=row["object_key"],
                    content_sha256=row["content_sha256"],
                )
                for row in shards
            ),
            aliases=aliases,
            rows=sum(int(row["row_count"]) for row in records),
            bytes=sum(int(row["size_bytes"]) for row in shards),
            first_occurred_at=min(row["first_occurred_at"] for row in records),
            last_occurred_at=max(row["last_occurred_at"] for row in records),
        ))
    return tuple(cohorts)


def _program(variant: str, cohort: Cohort) -> str:
    records = "/datasets/*/*/records-part-*.parquet"
    actors = "/datasets/*/*/actors-part-*.parquet"
    if variant == "stage_only":
        return "true"
    if variant == "duckdb_start":
        return "duckdb -batch -noheader -c 'SELECT 1' >/dev/null"
    if variant == "record_count":
        return (
            "duckdb -batch -noheader -c \"SELECT count(*) FROM "
            f"read_parquet('{records}')\" >/dev/null"
        )
    if variant != "filtered_join":
        raise ValueError("unknown benchmark variant")
    since = cohort.first_occurred_at.astimezone(timezone.utc).isoformat()
    until = cohort.last_occurred_at.astimezone(timezone.utc).isoformat()
    return (
        "duckdb -batch -noheader -c \""
        "SELECT count(*), count(DISTINCT a.actor_id) "
        f"FROM read_parquet('{records}') r "
        f"LEFT JOIN read_parquet('{actors}') a "
        "ON a.logical_document_id=r.logical_document_id "
        "AND a.revision=r.revision AND a.record_ordinal=r.ordinal "
        f"WHERE r.occurred_at >= TIMESTAMPTZ '{since}' "
        f"AND r.occurred_at <= TIMESTAMPTZ '{until}' "
        "AND length(r.search_text) > 0\" >/dev/null"
    )


def _one(
    inspector: Any,
    tenant_id: str,
    variant: str,
    cohort: Cohort,
    size_band: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    started = time.perf_counter_ns()
    try:
        result = inspector.execute_scan(
            tenant_id=tenant_id,
            program=_program(variant, cohort),
            objects=cohort.objects,
            dataset_aliases=cohort.aliases,
            timeout_seconds=timeout_seconds,
        )
        if not result.get("complete"):
            raise RuntimeError("incomplete")
        return {
            "variant": variant,
            "size_band": size_band,
            "ok": True,
            "rows": cohort.rows,
            "bytes": cohort.bytes,
            "clientWallMs": round((time.perf_counter_ns() - started) / 1e6, 3),
            "timing": result["timing"],
        }
    except (DeepInspectionError, OSError, RuntimeError) as error:
        return {
            "variant": variant,
            "size_band": size_band,
            "ok": False,
            "rows": cohort.rows,
            "bytes": cohort.bytes,
            "clientWallMs": round((time.perf_counter_ns() - started) / 1e6, 3),
            "error": getattr(error, "code", type(error).__name__),
        }


def _percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


def _distribution(values: list[float]) -> dict[str, float]:
    return {
        "mean": round(statistics.fmean(values), 3),
        "stdev": round(statistics.stdev(values), 3) if len(values) > 1 else 0,
        "p50": round(_percentile(values, 0.50), 3),
        "p95": round(_percentile(values, 0.95), 3),
        "p99": round(_percentile(values, 0.99), 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
    }


def _summarize(samples: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for variant in VARIANTS:
        selected = [sample for sample in samples if sample["variant"] == variant]
        passed = [sample for sample in selected if sample["ok"]]
        metrics: dict[str, list[float]] = defaultdict(list)
        for sample in passed:
            metrics["clientWallMs"].append(sample["clientWallMs"])
            for key in ("totalMs", "queueMs", "executeMs"):
                value = sample["timing"].get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    metrics[key].append(float(value))
            for key, value in sample["timing"].get("phases", {}).items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    metrics["phase." + key].append(float(value))
        result[variant] = {
            "attempts": len(selected),
            "successes": len(passed),
            "failure_rate": round(1 - len(passed) / len(selected), 5),
            "errors": dict(sorted(Counter(
                sample.get("error", "unknown")
                for sample in selected
                if not sample["ok"]
            ).items())),
            "metrics_ms": {
                key: _distribution(values)
                for key, values in sorted(metrics.items())
                if values
            },
            "size_bands": {
                band: {
                    "attempts": len(band_samples),
                    "executeMs": _distribution([
                        float(sample["timing"]["executeMs"])
                        for sample in band_samples
                        if sample["ok"]
                    ]),
                }
                for band in ("small", "medium", "large")
                if (
                    band_samples := [
                        sample
                        for sample in selected
                        if sample["size_band"] == band
                    ]
                )
                and any(sample["ok"] for sample in band_samples)
            },
        }
    return result


def run(args: argparse.Namespace) -> dict[str, Any]:
    store = BrainStore(os.environ["RECALL_DATABASE_URL"])
    try:
        cohorts = _cohorts(store, args.tenant)
        if not cohorts:
            raise RuntimeError("no complete Parquet cohorts")
        inspector = build_deep_inspector(object())
        if inspector is None or not callable(
            getattr(inspector, "execute_scan", None)
        ):
            raise RuntimeError("Archil scan runtime unavailable")
        ordered = sorted(cohorts, key=lambda cohort: (cohort.rows, cohort.bytes))
        band_by_identity = {
            id(cohort): (
                "small"
                if index < len(ordered) / 3
                else "medium"
                if index < len(ordered) * 2 / 3
                else "large"
            )
            for index, cohort in enumerate(ordered)
        }
        rng = random.Random(args.seed)
        blocks = [ordered[index % len(ordered)] for index in range(args.blocks)]
        rng.shuffle(blocks)
        work = []
        for cohort in blocks:
            variants = list(VARIANTS)
            rng.shuffle(variants)
            work.extend(
                (
                    variant,
                    cohort,
                    band_by_identity[id(cohort)],
                )
                for variant in variants
            )

        def invoke(item: tuple[str, Cohort, str]) -> dict[str, Any]:
            return _one(
                inspector,
                args.tenant,
                item[0],
                item[1],
                item[2],
                args.timeout_seconds,
            )

        if args.parallelism == 1:
            samples = [invoke(item) for item in work]
        else:
            with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
                samples = list(pool.map(invoke, work))
        return {
            "schema_version": 1,
            "status": "complete",
            "blocks": args.blocks,
            "attempts": len(samples),
            "parallelism": args.parallelism,
            "cohorts_available": len(cohorts),
            "cohort_rows": _distribution([float(cohort.rows) for cohort in blocks]),
            "cohort_bytes": _distribution([float(cohort.bytes) for cohort in blocks]),
            "summary": _summarize(samples),
        }
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--blocks", type=int, default=100)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    parser.add_argument("--seed", type=int, default=20260811)
    args = parser.parse_args()
    if (
        not 1 <= args.blocks <= 1_000
        or not 1 <= args.parallelism <= 16
        or not 1 <= args.timeout_seconds <= 240
    ):
        parser.error("benchmark budget is invalid")
    print(json.dumps(run(args), sort_keys=True))


if __name__ == "__main__":
    main()
