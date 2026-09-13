"""H2-a: contextual passage headers are embedding input only."""

from __future__ import annotations

import hashlib
import inspect
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))

from recall_server.passage_index import (  # noqa: E402
    CanonicalPassageProjector,
    document_context_from_row,
    passage_contract_coverage,
    passage_embed_plan,
)
from recall_server.passage_projection import (  # noqa: E402
    MAX_PASSAGE_HEADER_BYTES,
    PASSAGE_EMBEDDING_SEPARATOR,
    PASSAGE_HEADER_CONTRACT,
    PassageMessage,
    PassagePolicy,
    build_passages,
    canonical_spans_json,
    passage_embed_sha256,
    passage_embedding_input,
    render_passage_header,
)
from recall_server.passage_representations import (  # noqa: E402
    ActorContext,
    DocumentContext,
)
from recall_server.semantic import (  # noqa: E402
    DEFAULT_EMBEDDING_REVISION,
    SemanticRuntime,
)


def _context(**overrides) -> DocumentContext:
    values = dict(
        source_family="coding_history",
        source_aliases=("parcha", "recall"),
        harness="codex",
        workspace="/home/miguel/secret-parent/worktrees/recall",
        branch="feat/h2-a",
        actors=(
            ActorContext(
                actor_id="actor_" + "a" * 32,
                display_name="Alice Example",
                relations=("contributor",),
                aliases=("alice",),
            ),
        ),
    )
    values.update(overrides)
    return DocumentContext(**values)


def _messages() -> tuple[PassageMessage, ...]:
    return (
        PassageMessage(
            record_ordinal=0,
            occurred_at="2026-07-27T00:00:00Z",
            roles=("user",),
            receipts=("recall://source:test/one?rev=1#item=0",),
            text="why did the gateway preserve tenant boundaries",
        ),
        PassageMessage(
            record_ordinal=1,
            occurred_at="2026-07-27T00:05:00Z",
            roles=("assistant",),
            receipts=("recall://source:test/one?rev=1#item=1",),
            text="the gateway intersects every explicit source grant",
        ),
    )


def _build():
    return build_passages(
        tenant_id="tenant:company:test",
        source_id="source:test",
        logical_document_id="ldoc_0123456789abcdef0123456789abcdef",
        revision=1,
        messages=_messages(),
        policy=PassagePolicy(target_tokens=8, overlap_tokens=2),
    )


class PassageHeaderTests(unittest.TestCase):
    def test_header_is_deterministic_and_excluded_from_spans_and_text_sha(
        self,
    ) -> None:
        passages = _build()
        passage = passages[0]
        first = render_passage_header(
            _context(),
            first_occurred_at=passage.first_occurred_at,
            last_occurred_at=passage.last_occurred_at,
        )
        second = render_passage_header(
            _context(),
            first_occurred_at=passage.first_occurred_at,
            last_occurred_at="2026-07-27T00:05:00.000000+00:00",
        )

        self.assertEqual(first, second)
        self.assertEqual(
            first.splitlines(),
            [
                "[context]",
                "source family: coding_history",
                "source aliases: parcha, recall",
                "harness: codex",
                "workspace: recall",
                "branch: feat/h2-a",
                "people: Alice Example [contributor] (also: alice)",
                "passage start: 2026-07-27T00:00:00Z",
                "passage end: 2026-07-27T00:05:00Z",
                "[passage]",
            ],
        )
        # The workspace parent path never leaks; only the basename.
        self.assertNotIn("secret-parent", first)
        self.assertNotIn("miguel", first)
        # Header is embedding input only: passage rows are byte-identical
        # with or without it.
        again = _build()
        self.assertEqual(passages, again)
        self.assertEqual(
            passage.text_sha256,
            hashlib.sha256(passage.text.encode()).hexdigest(),
        )
        self.assertNotIn("[context]", passage.text)
        self.assertNotIn("[context]", canonical_spans_json(passage.spans))
        self.assertEqual(
            passage_embedding_input(first, passage.text),
            first + PASSAGE_EMBEDDING_SEPARATOR + passage.text,
        )
        self.assertEqual(passage_embedding_input(None, passage.text), passage.text)
        self.assertEqual(PASSAGE_HEADER_CONTRACT, "recall.passage-header.v1:catalog-fields")

    def test_header_is_bounded_to_512_bytes(self) -> None:
        crowded = _context(
            source_aliases=tuple(f"alias-{index}-ünïcödé" * 8 for index in range(8)),
            actors=tuple(
                ActorContext(
                    actor_id="actor_" + f"{index:032x}",
                    display_name=f"Persona Número {index} con nombre largo",
                    relations=("author", "contributor"),
                    aliases=(f"alias-{index}",),
                )
                for index in range(16)
            ),
        )
        header = render_passage_header(
            crowded,
            first_occurred_at="2026-07-27T00:00:00Z",
            last_occurred_at="2026-07-27T00:05:00Z",
        )

        self.assertLessEqual(len(header.encode()), MAX_PASSAGE_HEADER_BYTES)
        self.assertTrue(header.startswith("[context]\n"))
        self.assertIn("passage start: 2026-07-27T00:00:00Z", header)
        self.assertIn("passage end: 2026-07-27T00:05:00Z", header)
        self.assertIn("source family: coding_history", header)
        # Trimming is deterministic.
        self.assertEqual(
            header,
            render_passage_header(
                crowded,
                first_occurred_at="2026-07-27T00:00:00Z",
                last_occurred_at="2026-07-27T00:05:00Z",
            ),
        )
        empty = render_passage_header(
            DocumentContext(),
            first_occurred_at="2026-07-27T00:00:00Z",
            last_occurred_at="2026-07-27T00:05:00Z",
        )
        self.assertEqual(
            empty.splitlines(),
            [
                "[context]",
                "passage start: 2026-07-27T00:00:00Z",
                "passage end: 2026-07-27T00:05:00Z",
                "[passage]",
            ],
        )

    def test_embed_sha_changes_when_header_changes_but_passage_id_does_not(
        self,
    ) -> None:
        passage = _build()[0]
        base = render_passage_header(
            _context(),
            first_occurred_at=passage.first_occurred_at,
            last_occurred_at=passage.last_occurred_at,
        )
        renamed = render_passage_header(
            _context(actors=(
                ActorContext(
                    actor_id="actor_" + "a" * 32,
                    display_name="Alicia Ejemplo",
                    relations=("contributor",),
                ),
            )),
            first_occurred_at=passage.first_occurred_at,
            last_occurred_at=passage.last_occurred_at,
        )

        self.assertNotEqual(base, renamed)
        self.assertNotEqual(
            passage_embed_sha256(base, passage.text),
            passage_embed_sha256(renamed, passage.text),
        )
        self.assertNotEqual(passage_embed_sha256(base, passage.text), passage.text_sha256)
        self.assertEqual(
            passage_embed_sha256(base, passage.text),
            hashlib.sha256(
                (base + PASSAGE_EMBEDDING_SEPARATOR + passage.text).encode()
            ).hexdigest(),
        )
        # passage_id is a function of text and spans only: T3 stable ids
        # do not move when the header does.
        self.assertEqual(_build()[0].passage_id, passage.passage_id)
        with self.assertRaises(ValueError):
            passage_embed_sha256("", passage.text)

    def test_document_context_row_maps_catalog_fields_only(self) -> None:
        context = document_context_from_row({
            "source_family": "coding_history",
            "source_aliases": ["recall"],
            "harness": None,
            "metadata": {"harness": "claude-code", "cwd": "/x/y/repo", "branch": "main"},
            "actors": [
                {
                    "actor_id": "actor_" + "b" * 32,
                    "display_name": "Bob",
                    "relations": ["author"],
                    "aliases": ["bobby"],
                }
            ],
        })

        self.assertEqual(context.harness, "claude-code")
        self.assertEqual(context.workspace, "/x/y/repo")
        self.assertEqual(context.branch, "main")
        self.assertEqual(context.actors[0].display_name, "Bob")
        self.assertEqual(document_context_from_row(None), DocumentContext())


class _Result:
    def __init__(self, rows=None, row=None, rowcount=0):
        self._rows = rows or []
        self._row = row
        self.rowcount = rowcount

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._row


class _Cursor:
    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def executemany(self, sql, rows):
        self.sink.append((sql, list(rows)))


class _Connection:
    def __init__(self, candidate_row):
        self.candidate_row = candidate_row
        self.served = False
        self.statements: list[tuple[str, tuple]] = []
        self.inserted: list[tuple[str, list]] = []

    def execute(self, sql, params=()):
        self.statements.append((sql, params))
        if "pg_try_advisory_lock" in sql:
            return _Result(row={"value": True})
        if "SELECT EXISTS(" in sql:
            return _Result(row={"value": False})
        if "passage.header_redacted IS NULL" in sql:
            return _Result(rows=[])
        if "LEFT JOIN canonical_passage_embeddings embedding" in sql:
            if self.served:
                return _Result(rows=[])
            self.served = True
            return _Result(rows=[self.candidate_row])
        return _Result()

    def commit(self):
        pass

    @contextmanager
    def transaction(self):
        yield

    def cursor(self):
        return _Cursor(self.inserted)


class _Store:
    pool_max_size = 8

    def __init__(self, runtime, connection):
        self.semantic_runtime = runtime
        self.connection = connection

    @contextmanager
    def connect(self):
        yield self.connection


def _runtime(mode: str) -> SemanticRuntime:
    return SemanticRuntime(
        embedding_url="http://127.0.0.1:8081",
        model="synthetic-embedding",
        revision=DEFAULT_EMBEDDING_REVISION,
        dimensions=512,
        passage_contract_mode=mode,
    )


class EmbedPendingContractTests(unittest.TestCase):
    ROW = {
        "tenant_id": "tenant:company:test",
        "source_id": "source:test",
        "passage_id": "psg_" + "1" * 32,
        "text_redacted": "the passage text",
        "text_sha256": "t" * 64,
        "header_redacted": "[context]\nharness: codex\n[passage]",
        "embed_sha256": "e" * 64,
    }

    def _run(self, mode: str):
        runtime = _runtime(mode)
        connection = _Connection(dict(self.ROW))
        projector = CanonicalPassageProjector(
            _Store(runtime, connection),
            logical_projection=object(),
            policy=PassagePolicy(target_tokens=8, overlap_tokens=2),
        )
        with mock.patch.object(
            runtime, "embed_passages", return_value=[[0.0] * 512]
        ) as embed:
            result = projector.embed_pending(batch_size=10, max_batches=2)
        return runtime, connection, embed, result

    def test_embed_pending_uses_embed_sha256_under_v2(self) -> None:
        runtime, connection, embed, result = self._run("v2")

        select = next(
            sql for sql, _ in connection.statements
            if "LEFT JOIN canonical_passage_embeddings embedding" in sql
            and "LIMIT %s" in sql
        )
        self.assertIn("embedding.content_sha256=passage.embed_sha256", select)
        self.assertIn("passage.header_redacted IS NOT NULL", select)
        self.assertEqual(
            embed.call_args.args[0],
            [self.ROW["header_redacted"] + "\n\n" + self.ROW["text_redacted"]],
        )
        _sql, rows = connection.inserted[0]
        self.assertEqual(rows[0][4], "e" * 64)
        self.assertEqual(rows[0][5], runtime.passage_fingerprint_v2)
        self.assertEqual(result["contract"], "v2")
        self.assertEqual(result["processed"], 1)
        # The header backfill ran ahead of the embedding scan.
        first_sql = connection.statements[0][0]
        self.assertIn("header_redacted IS NULL", first_sql)

    def test_embed_pending_uses_text_sha256_under_v1(self) -> None:
        runtime, connection, embed, result = self._run("v1")

        select = next(
            sql for sql, _ in connection.statements
            if "LEFT JOIN canonical_passage_embeddings embedding" in sql
            and "LIMIT %s" in sql
        )
        self.assertIn("embedding.content_sha256=passage.text_sha256", select)
        self.assertNotIn("header_redacted IS NOT NULL", select)
        self.assertEqual(embed.call_args.args[0], [self.ROW["text_redacted"]])
        _sql, rows = connection.inserted[0]
        self.assertEqual(rows[0][4], "t" * 64)
        self.assertEqual(rows[0][5], runtime.passage_fingerprint_v1)
        self.assertEqual(result["contract"], "v1")
        self.assertFalse(any(
            "header_redacted IS NULL" in sql for sql, _ in connection.statements
        ))

    def test_auto_mode_writes_v2_and_projector_binds_coverage_probe(self) -> None:
        runtime, connection, _embed, result = self._run("auto")

        self.assertEqual(result["contract"], "v2")
        self.assertTrue(runtime.has_passage_coverage_probe)

    def test_commit_writes_headers_and_reuses_by_either_key(self) -> None:
        source = inspect.getsource(CanonicalPassageProjector._commit)

        self.assertIn("text_sha256,header_redacted,embed_sha256", source)
        self.assertIn("cached.content_sha256=\n                                          passage.embed_sha256", source)
        self.assertIn("passage.embed_sha256=ANY(%s::text[])", source)
        self.assertIn("render_passage_header(", source)
        write = inspect.getsource(CanonicalPassageProjector._write_headers)
        self.assertIn("passage.header_redacted IS NULL", write)
        self.assertIn("sha256(convert_to(", write)
        backfill = inspect.getsource(CanonicalPassageProjector.backfill_headers)
        self.assertNotIn("text_redacted", backfill.split("_write_headers")[0])

    def test_coverage_and_plan_are_read_only_and_content_free(self) -> None:
        coverage_sql = inspect.getsource(passage_contract_coverage)
        plan_sql = inspect.getsource(passage_embed_plan)

        for source in (coverage_sql, plan_sql):
            for verb in ("INSERT", "UPDATE", "DELETE", "COPY"):
                self.assertNotIn(verb, source)
        self.assertIn("octet_length(passage.text_redacted)", plan_sql)
        self.assertNotIn("SELECT passage.text_redacted", plan_sql)

        class Connection:
            def execute(self, _sql, _params):
                return _Result(row={
                    "passages": 10,
                    "headers_present": 8,
                    "embedded_v2": 4,
                    "embedded_v1": 6,
                    "pending_bytes": 24_000,
                })

        report = passage_embed_plan(
            Connection(),
            tenant_id="tenant:company:test",
            runtime=_runtime("auto"),
            price_per_mtoken=0.06,
        )
        self.assertEqual(report["needs_embedding"], 6)
        self.assertEqual(report["headers_missing"], 2)
        self.assertEqual(report["estimated_tokens"], 6_000)
        self.assertEqual(report["estimated_cost"], round(6_000 / 1e6 * 0.06, 4))
        self.assertTrue(report["read_only"])
        self.assertEqual(report["coverage_v2"], 0.4)


if __name__ == "__main__":
    unittest.main()
