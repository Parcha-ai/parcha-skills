from __future__ import annotations

import inspect
import unittest

from recall_server.passage_representations import (
    ActorContext,
    CanonicalPassageRepresentationIndex,
    ContextPassage,
    DocumentContext,
    PassageContextPolicy,
    PassageRepresentation,
    VECTOR_COLUMNS,
    contextualize_passage,
    embedding_excerpt,
    workspace_label,
)
from recall_server.passage_retrieval import PassageHintRetrieval


class Runtime:
    model = "synthetic-embedding"
    dimensions = 512
    passage_fingerprint = "a" * 64


class PassageRepresentationTests(unittest.TestCase):
    def passage(
        self,
        ordinal: int,
        *,
        tokens: int = 4,
    ) -> ContextPassage:
        return ContextPassage(
            passage_id=f"psg_{ordinal:032x}",
            ordinal=ordinal,
            text=f"exact passage {ordinal}",
            token_count=tokens,
        )

    def test_context_is_deterministic_and_keeps_only_redacted_project_suffix(
        self,
    ) -> None:
        policy = PassageContextPolicy(
            opening_passages=2,
            neighbor_passages=1,
            max_tokens=512,
        )
        target = self.passage(4)
        support = tuple(self.passage(value) for value in (5, 0, 3, 1, 4))
        metadata = DocumentContext(
            source_family="coding",
            source_aliases=("greppy", "greppy"),
            harness="codex",
            workspace="/redacted/home/worktrees/ati-harness",
            branch="feature/context",
            first_occurred_at="2026-07-29T00:00:00+00:00",
            last_occurred_at="2026-07-29T01:00:00+00:00",
            actors=(
                ActorContext(
                    actor_id="actor_0123456789abcdef0123456789abcdef",
                    display_name="Alice Example",
                    relations=("contributor", "author"),
                    aliases=("alice",),
                ),
            ),
        )

        first = contextualize_passage(
            target,
            support,
            metadata=metadata,
            policy=policy,
        )
        second = contextualize_passage(
            target,
            reversed(support),
            metadata=metadata,
            policy=policy,
        )

        self.assertEqual(first, second)
        self.assertIn("workspace: ati-harness", first.context_text)
        self.assertNotIn("worktrees/", first.context_text)
        self.assertNotIn("/redacted/home/", first.context_text)
        self.assertIn(
            "people: Alice Example [author, contributor] (also: alice)",
            first.context_text,
        )
        self.assertEqual(first.context_text.count("exact passage 4"), 1)
        self.assertEqual(first.context_text.count("exact passage 0"), 1)
        self.assertEqual(first.context_text.count("exact passage 1"), 1)
        self.assertEqual(first.context_text.count("exact passage 3"), 1)
        self.assertEqual(first.context_text.count("exact passage 5"), 1)

    def test_context_budget_drops_support_but_never_the_target(self) -> None:
        target = self.passage(9, tokens=500)
        result = contextualize_passage(
            target,
            (
                self.passage(0, tokens=20),
                self.passage(8, tokens=20),
                self.passage(10, tokens=20),
            ),
            metadata=DocumentContext(),
            policy=PassageContextPolicy(
                opening_passages=1,
                neighbor_passages=1,
                max_tokens=512,
            ),
        )

        self.assertIn("exact passage 9", result.context_text)
        self.assertNotIn("exact passage 0", result.context_text)
        self.assertNotIn("exact passage 8", result.context_text)
        self.assertNotIn("exact passage 10", result.context_text)

    def test_many_actor_labels_stay_inside_the_embedding_budget(self) -> None:
        result = contextualize_passage(
            self.passage(1),
            (),
            metadata=DocumentContext(
                actors=tuple(
                    ActorContext(
                        actor_id=f"actor_{index:032x}",
                        display_name=f"Person {index} " + "x" * 256,
                        relations=("author", "participant"),
                        aliases=("y" * 256, "z" * 256),
                    )
                    for index in range(64)
                ),
            ),
            policy=PassageContextPolicy(),
        )

        self.assertLessEqual(len(result.context_text.encode()), 7_000)
        self.assertIn("people: Person", result.context_text)
        self.assertIn("exact passage 1", result.context_text)

    def test_embedding_excerpt_is_bounded_and_keeps_both_ends(self) -> None:
        value = "alpha-" + ("x" * 12_000) + "-omega"

        excerpt = embedding_excerpt(value)

        self.assertLessEqual(len(excerpt.encode()), 7_000)
        self.assertTrue(excerpt.startswith("alpha-"))
        self.assertTrue(excerpt.endswith("-omega"))
        self.assertIn("embedding excerpt clipped", excerpt)

    def test_representation_fingerprints_isolate_model_and_context(self) -> None:
        plain = PassageRepresentation("openai-small-plain", Runtime())
        contextual = PassageRepresentation(
            "openai-small-context",
            Runtime(),
            PassageContextPolicy(),
        )

        self.assertNotEqual(plain.fingerprint, contextual.fingerprint)
        self.assertIsNone(plain.context_fingerprint)
        self.assertEqual(
            contextual.context_fingerprint,
            PassageContextPolicy().fingerprint,
        )

    def test_native_openai_dimensions_have_isolated_vector_columns(self) -> None:
        for dimensions in (512, 1536, 3072):
            runtime = type(
                "NativeRuntime",
                (),
                {
                    "dimensions": dimensions,
                    "model": f"synthetic-{dimensions}",
                    "passage_fingerprint": str(dimensions) * 64,
                },
            )()
            representation = PassageRepresentation(
                f"native-{dimensions}",
                runtime,
            )

            self.assertEqual(
                VECTOR_COLUMNS[representation.runtime.dimensions],
                {
                    512: "embedding",
                    1536: "embedding_1536",
                    3072: "embedding_3072",
                }[dimensions],
            )

    def test_workspace_label_preserves_only_project_basename(
        self,
    ) -> None:
        self.assertEqual(
            workspace_label("/one/two/three/four"),
            "four",
        )
        self.assertIsNone(workspace_label(""))

    def test_backfill_is_tenant_bound_and_has_fingerprint_rollback(self) -> None:
        pending = inspect.getsource(
            CanonicalPassageRepresentationIndex.embed_pending
        )
        plain = inspect.getsource(
            CanonicalPassageRepresentationIndex._plain_rows
        )
        context = inspect.getsource(
            CanonicalPassageRepresentationIndex._context_rows
        )
        rollback = inspect.getsource(
            CanonicalPassageRepresentationIndex.rollback
        )

        self.assertIn("tenant_id=tenant_id", pending)
        self.assertIn("shard_count=shard_count", pending)
        self.assertIn("shard_index=shard_index", pending)
        self.assertIn("passage.tenant_id=%s", plain)
        self.assertIn("passage.tenant_id=%s", context)
        self.assertIn("hashtextextended(passage.passage_id,0)", plain)
        self.assertIn("hashtextextended(passage.passage_id,0)", context)
        self.assertIn("representation_fingerprint=%s", rollback)
        self.assertIn("NOT EXISTS", rollback)

    def test_backfill_shard_budget_is_validated(self) -> None:
        source = inspect.getsource(
            CanonicalPassageRepresentationIndex.embed_pending
        )

        self.assertIn("not 1 <= shard_count <= 64", source)
        self.assertIn("not 0 <= shard_index < shard_count", source)

    def test_shadow_search_filters_authority_before_vector_ranking(self) -> None:
        source = inspect.getsource(PassageHintRetrieval.search_representation)
        nearest = source.split("WITH nearest AS MATERIALIZED", 1)[1]
        nearest = nearest.split("ORDER BY", 1)[0]

        self.assertIn("represented.tenant_id=%s", nearest)
        self.assertIn("represented.source_id=ANY(%s)", nearest)
        self.assertIn(
            "represented.representation_fingerprint=%s",
            nearest,
        )
        self.assertIn("represented.{vector_column}", nearest)
        self.assertNotIn("canonical_evidence_documents", nearest)


if __name__ == "__main__":
    unittest.main()
