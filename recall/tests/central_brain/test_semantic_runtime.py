from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

SERVER = Path(__file__).resolve().parents[2] / "server"
sys.path.insert(0, str(SERVER))

from recall_server.semantic import (  # noqa: E402
    DEFAULT_EMBEDDING_REVISION,
    DOCUMENT_CLIP_MARKER,
    MAX_DOCUMENT_CHARS,
    QUERY_INSTRUCTION,
    SearchPlan,
    SemanticRuntime,
    _RejectRedirect,
)


class SemanticRuntimeContractTest(unittest.TestCase):
    def runtime(self, key_file: str | None = None) -> SemanticRuntime:
        return SemanticRuntime(
            embedding_url="http://127.0.0.1:8081",
            model="synthetic-embedding",
            revision=DEFAULT_EMBEDDING_REVISION,
            dimensions=512,
            planner_url="https://router.example" if key_file else None,
            planner_approved_url="https://router.example" if key_file else None,
            planner_model="synthetic-planner" if key_file else None,
            planner_key_file=key_file,
            planner_samples=1,
        )

    def test_remote_embedding_endpoint_requires_exact_separate_approval(self) -> None:
        with self.assertRaisesRegex(ValueError, "approved private endpoint"):
            SemanticRuntime(
                embedding_url="http://embedding.internal:80",
                model="unsafe",
                revision=DEFAULT_EMBEDDING_REVISION,
                dimensions=512,
            )
        with self.assertRaisesRegex(ValueError, "does not match"):
            SemanticRuntime(
                embedding_url="http://embedding.internal:80",
                embedding_approved_url="http://other.internal:80",
                model="unsafe",
                revision=DEFAULT_EMBEDDING_REVISION,
                dimensions=512,
            )
        runtime = SemanticRuntime(
            embedding_url="http://embedding.internal:80",
            embedding_approved_url="http://embedding.internal:80",
            model="synthetic-embedding",
            revision=DEFAULT_EMBEDDING_REVISION,
            dimensions=512,
        )
        self.assertEqual(runtime.embedding_url, "http://embedding.internal:80")

    def test_openai_embedding_protocol_requires_https_and_exact_approval(self) -> None:
        with self.assertRaisesRegex(ValueError, "HTTPS"):
            SemanticRuntime(
                embedding_protocol="openai",
                embedding_url="http://embeddings.example/v1",
                embedding_approved_url="http://embeddings.example/v1",
                model="synthetic-embedding",
                revision="managed-v1",
                dimensions=512,
            )
        with self.assertRaisesRegex(ValueError, "approved embedding endpoint"):
            SemanticRuntime(
                embedding_protocol="openai",
                embedding_url="https://embeddings.example/v1",
                embedding_approved_url="https://other.example/v1",
                model="synthetic-embedding",
                revision="managed-v1",
                dimensions=512,
            )

    def test_openai_embedding_protocol_uses_standard_contract_and_owner_only_key(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            key = Path(temporary) / "embedding.key"
            key.write_text("short-lived-synthetic-embedding-key")
            os.chmod(key, 0o600)
            runtime = SemanticRuntime(
                embedding_protocol="openai",
                embedding_url="https://embeddings.example",
                embedding_approved_url="https://embeddings.example",
                embedding_key_file=str(key),
                model="synthetic-embedding",
                revision="managed-v1",
                dimensions=512,
                embedding_batch_size=2,
            )
            vectors = [[0.0] * 512, [1.0] * 512]
            response = {
                "object": "list",
                "data": [
                    {"object": "embedding", "index": 1, "embedding": vectors[1]},
                    {"object": "embedding", "index": 0, "embedding": vectors[0]},
                ],
                "model": "synthetic-embedding",
            }
            with (
                mock.patch.object(runtime, "_get") as get,
                mock.patch.object(runtime, "_post", return_value=response) as post,
            ):
                self.assertEqual(runtime.embed_documents(["one", "two"]), vectors)
            get.assert_not_called()
            self.assertEqual(post.call_args.args[0], "https://embeddings.example/v1/embeddings")
            self.assertEqual(
                post.call_args.args[1],
                {
                    "model": "synthetic-embedding",
                    "input": ["one", "two"],
                    "encoding_format": "float",
                    "dimensions": 512,
                },
            )
            self.assertEqual(
                post.call_args.args[2]["Authorization"],
                "Bearer short-lived-synthetic-embedding-key",
            )
            os.chmod(key, 0o644)
            with self.assertRaisesRegex(PermissionError, "owner-only"):
                runtime.embed_documents(["must not leave"])

    def test_voyage_protocol_distinguishes_documents_and_queries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            key = Path(temporary) / "embedding.key"
            key.write_text("short-lived-synthetic-embedding-key")
            os.chmod(key, 0o600)
            runtime = SemanticRuntime(
                embedding_protocol="voyage",
                embedding_url="https://api.voyage.example",
                embedding_approved_url="https://api.voyage.example",
                embedding_key_file=str(key),
                model="voyage-synthetic",
                revision="voyage-synthetic-v1",
                dimensions=512,
            )
            vector = [0.0] * 512
            response = {
                "object": "list",
                "data": [{"object": "embedding", "index": 0, "embedding": vector}],
                "model": "voyage-synthetic",
            }
            with mock.patch.object(runtime, "_post", return_value=response) as post:
                runtime.embed_documents(["decision"])
                runtime.embed_query("what did we choose?")
            self.assertEqual(
                [call.args[1] for call in post.call_args_list],
                [
                    {
                        "model": "voyage-synthetic",
                        "input": ["decision"],
                        "input_type": "document",
                        "output_dimension": 512,
                        "output_dtype": "float",
                        "truncation": True,
                    },
                    {
                        "model": "voyage-synthetic",
                        "input": ["what did we choose?"],
                        "input_type": "query",
                        "output_dimension": 512,
                        "output_dtype": "float",
                        "truncation": True,
                    },
                ],
            )

    def test_managed_embedding_retries_one_malformed_json_response(self) -> None:
        runtime = SemanticRuntime(
            embedding_protocol="voyage",
            embedding_url="https://api.voyage.example",
            embedding_approved_url="https://api.voyage.example",
            embedding_key_env="VOYAGE_API_KEY",
            model="voyage-synthetic",
            revision="voyage-synthetic-v1",
            dimensions=512,
        )
        vector = [0.0] * 512
        response = {
            "data": [{"object": "embedding", "index": 0, "embedding": vector}],
            "model": "voyage-synthetic",
            "object": "list",
        }
        malformed = json.JSONDecodeError("synthetic truncation", "{", 1)
        with (
            mock.patch.dict(
                os.environ,
                {"VOYAGE_API_KEY": "short-lived-synthetic-embedding-key"},
            ),
            mock.patch.object(
                runtime,
                "_post",
                side_effect=[malformed, response],
            ) as post,
        ):
            self.assertEqual(runtime.embed_documents(["decision"]), [vector])
        self.assertEqual(post.call_count, 2)

        with (
            mock.patch.dict(
                os.environ,
                {"VOYAGE_API_KEY": "short-lived-synthetic-embedding-key"},
            ),
            mock.patch.object(
                runtime,
                "_post",
                side_effect=[malformed, malformed],
            ) as post,
            self.assertRaises(json.JSONDecodeError),
        ):
            runtime.embed_documents(["decision"])
        self.assertEqual(post.call_count, 2)

        with (
            mock.patch.dict(
                os.environ,
                {"VOYAGE_API_KEY": "short-lived-synthetic-embedding-key"},
            ),
            mock.patch.object(runtime, "_post", side_effect=malformed) as post,
            self.assertRaises(json.JSONDecodeError),
        ):
            runtime.embed_query("what did we decide?")
        self.assertEqual(post.call_count, 1)

    def test_voyage_environment_profile_has_hosted_batch_defaults(self) -> None:
        environment = {
            "RECALL_EMBEDDING_PROTOCOL": "voyage",
            "RECALL_EMBEDDING_URL": "https://api.voyage.example",
            "RECALL_EMBEDDING_APPROVED_URL": "https://api.voyage.example",
            "RECALL_EMBEDDING_KEY_ENV": "VOYAGE_API_KEY",
            "VOYAGE_API_KEY": "short-lived-synthetic-embedding-key",
            "RECALL_EMBEDDING_MODEL": "voyage-synthetic",
            "RECALL_EMBEDDING_DIMENSIONS": "512",
        }
        with mock.patch.dict(os.environ, environment, clear=True):
            runtime = SemanticRuntime.from_env()
        self.assertIsNotNone(runtime)
        assert runtime is not None
        self.assertEqual(runtime.embedding_protocol, "voyage")
        self.assertEqual(runtime.revision, "voyage-synthetic")
        self.assertEqual(runtime.embedding_batch_size, 64)
        self.assertEqual(runtime.embedding_workers, 1)
        self.assertEqual(runtime.query_prefix, "")

    def test_query_embedding_has_a_hard_wall_clock_deadline(self) -> None:
        runtime = SemanticRuntime(
            embedding_protocol="voyage",
            embedding_url="https://api.voyageai.com",
            embedding_approved_url="https://api.voyageai.com",
            embedding_key_env="VOYAGE_API_KEY",
            model="voyage-4-large",
            revision="voyage-4-large",
            dimensions=512,
            timeout_seconds=0.02,
        )
        started = threading.Event()
        release = threading.Event()

        def slow_query(_query: str) -> list[float]:
            started.set()
            release.wait(timeout=1)
            return [0.0] * 512

        try:
            with (
                mock.patch.object(runtime, "embed_query", side_effect=slow_query),
                self.assertRaisesRegex(TimeoutError, "deadline exceeded"),
            ):
                runtime.embed_query_bounded("bounded synthetic query")
            self.assertTrue(started.is_set())
        finally:
            release.set()

    def test_managed_embedding_batch_supports_bounded_bulk_backfill(self) -> None:
        runtime = SemanticRuntime(
            embedding_protocol="voyage",
            embedding_url="https://api.voyage.example",
            embedding_approved_url="https://api.voyage.example",
            embedding_key_env="VOYAGE_API_KEY",
            model="voyage-synthetic",
            revision="voyage-synthetic-v1",
            dimensions=512,
            embedding_batch_size=512,
        )
        self.assertEqual(runtime.embedding_batch_size, 512)
        with self.assertRaisesRegex(ValueError, "between 1 and 512"):
            SemanticRuntime(
                embedding_protocol="voyage",
                embedding_url="https://api.voyage.example",
                embedding_approved_url="https://api.voyage.example",
                embedding_key_env="VOYAGE_API_KEY",
                model="voyage-synthetic",
                revision="voyage-synthetic-v1",
                dimensions=512,
                embedding_batch_size=513,
            )

    def test_managed_document_batches_run_concurrently_and_keep_order(
        self,
    ) -> None:
        runtime = SemanticRuntime(
            embedding_protocol="voyage",
            embedding_url="https://api.voyage.example",
            embedding_approved_url="https://api.voyage.example",
            embedding_key_env="VOYAGE_API_KEY",
            model="voyage-synthetic",
            revision="voyage-synthetic-v1",
            dimensions=64,
            embedding_batch_size=1,
            embedding_workers=2,
        )
        barrier = threading.Barrier(2)

        def concurrent_batch(batch, *, input_type, headers):
            barrier.wait(timeout=1)
            return [[float(batch[0])] * 64]

        with (
            mock.patch.dict(
                os.environ,
                {"VOYAGE_API_KEY": "short-lived-synthetic-embedding-key"},
            ),
            mock.patch.object(
                runtime,
                "_embed_managed_batch",
                side_effect=concurrent_batch,
            ),
        ):
            vectors = runtime.embed_documents(["1", "2"])

        self.assertEqual(vectors[0][0], 1.0)
        self.assertEqual(vectors[1][0], 2.0)

    def test_embedding_worker_concurrency_is_closed_and_managed_only(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "between 1 and 8"):
            SemanticRuntime(
                embedding_protocol="voyage",
                embedding_url="https://api.voyage.example",
                embedding_approved_url="https://api.voyage.example",
                embedding_key_env="VOYAGE_API_KEY",
                model="voyage-synthetic",
                revision="voyage-synthetic-v1",
                dimensions=512,
                embedding_workers=9,
            )
        with self.assertRaisesRegex(ValueError, "must be one"):
            SemanticRuntime(
                embedding_url="http://127.0.0.1:8081",
                model="synthetic-embedding",
                revision=DEFAULT_EMBEDDING_REVISION,
                dimensions=512,
                embedding_workers=2,
            )

    def test_managed_embedding_batch_splits_bounded_upstream_failures(self) -> None:
        runtime = SemanticRuntime(
            embedding_protocol="voyage",
            embedding_url="https://api.voyage.example",
            embedding_approved_url="https://api.voyage.example",
            embedding_key_env="VOYAGE_API_KEY",
            model="voyage-synthetic",
            revision="voyage-synthetic-v1",
            dimensions=512,
            embedding_batch_size=512,
        )
        batch_sizes: list[int] = []

        def limited_upstream(url, payload, headers):
            batch_sizes.append(len(payload["input"]))
            if len(payload["input"]) > 128:
                raise urllib.error.HTTPError(url, 500, "synthetic", {}, None)
            return {
                "data": [
                    {"index": index, "embedding": [float(index)] * 512}
                    for index in range(len(payload["input"]))
                ]
            }

        with (
            mock.patch.dict(
                os.environ,
                {"VOYAGE_API_KEY": "short-lived-synthetic-embedding-key"},
                clear=True,
            ),
            mock.patch.object(runtime, "_post", side_effect=limited_upstream),
        ):
            vectors = runtime.embed_documents(
                [f"synthetic-document-{index}" for index in range(300)]
            )
        self.assertEqual(len(vectors), 300)
        self.assertEqual(batch_sizes, [300, 150, 75, 75, 150, 75, 75])
        self.assertEqual(vectors[0][0], 0.0)
        self.assertEqual(vectors[75][0], 0.0)

    def test_voyage_document_400_splits_but_single_input_fails_closed(
        self,
    ) -> None:
        runtime = SemanticRuntime(
            embedding_protocol="voyage",
            embedding_url="https://api.voyage.example",
            embedding_approved_url="https://api.voyage.example",
            embedding_key_env="VOYAGE_API_KEY",
            model="voyage-synthetic",
            revision="voyage-synthetic-v1",
            dimensions=512,
            embedding_batch_size=4,
        )
        batch_sizes: list[int] = []

        def limited_upstream(url, payload, headers):
            batch_sizes.append(len(payload["input"]))
            if len(payload["input"]) > 2:
                raise urllib.error.HTTPError(url, 400, "synthetic", {}, None)
            return {
                "data": [
                    {"index": index, "embedding": [float(index)] * 512}
                    for index in range(len(payload["input"]))
                ]
            }

        with (
            mock.patch.dict(
                os.environ,
                {"VOYAGE_API_KEY": "short-lived-synthetic-embedding-key"},
                clear=True,
            ),
            mock.patch.object(runtime, "_post", side_effect=limited_upstream),
        ):
            vectors = runtime.embed_documents(["one", "two", "three", "four"])
        self.assertEqual(len(vectors), 4)
        self.assertEqual(batch_sizes, [4, 2, 2])

        with (
            mock.patch.dict(
                os.environ,
                {"VOYAGE_API_KEY": "short-lived-synthetic-embedding-key"},
                clear=True,
            ),
            mock.patch.object(
                runtime,
                "_post",
                side_effect=urllib.error.HTTPError(
                    runtime.embedding_url,
                    400,
                    "synthetic",
                    {},
                    None,
                ),
            ),
            self.assertRaises(urllib.error.HTTPError),
        ):
            runtime.embed_documents(["irreducible"])

    def test_managed_key_may_come_from_a_named_secret_environment_variable(
        self,
    ) -> None:
        runtime = SemanticRuntime(
            embedding_protocol="voyage",
            embedding_url="https://api.voyage.example",
            embedding_approved_url="https://api.voyage.example",
            embedding_key_env="VOYAGE_API_KEY",
            model="voyage-synthetic",
            revision="voyage-synthetic-v1",
            dimensions=512,
        )
        vector = [0.0] * 512
        response = {"data": [{"index": 0, "embedding": vector}]}
        with (
            mock.patch.dict(
                os.environ,
                {"VOYAGE_API_KEY": "short-lived-synthetic-embedding-key"},
                clear=True,
            ),
            mock.patch.object(runtime, "_post", return_value=response) as post,
        ):
            runtime.embed_query("safe synthetic query")
        self.assertEqual(
            post.call_args.args[2]["Authorization"],
            "Bearer short-lived-synthetic-embedding-key",
        )
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "unavailable"):
                runtime.embed_query("must not leave")

    def test_managed_response_rejects_missing_duplicate_or_nonfinite_vectors(
        self,
    ) -> None:
        runtime = SemanticRuntime(
            embedding_protocol="openai",
            embedding_url="http://127.0.0.1:8081",
            model="synthetic-embedding",
            revision="managed-v1",
            dimensions=512,
            embedding_batch_size=2,
        )
        vector = [0.0] * 512
        with mock.patch.object(
            runtime,
            "_post",
            return_value={
                "data": [
                    {"index": 0, "embedding": vector},
                    {"index": 0, "embedding": vector},
                ]
            },
        ):
            with self.assertRaisesRegex(ValueError, "indices"):
                runtime.embed_documents(["one", "two"])
        invalid = vector.copy()
        invalid[0] = float("nan")
        with mock.patch.object(
            runtime,
            "_post",
            return_value={"data": [{"index": 0, "embedding": invalid}]},
        ):
            with self.assertRaisesRegex(ValueError, "non-finite"):
                runtime.embed_documents(["one"])

    def test_embedding_prefixes_and_protocol_are_fingerprinted(self) -> None:
        base = SemanticRuntime(
            embedding_protocol="openai",
            embedding_url="http://127.0.0.1:8081",
            model="synthetic-embedding",
            revision="managed-v1",
            dimensions=512,
        )
        prefixed = SemanticRuntime(
            embedding_protocol="openai",
            embedding_url="http://127.0.0.1:8081",
            model="synthetic-embedding",
            revision="managed-v1",
            dimensions=512,
            document_prefix="search_document: ",
            query_prefix="search_query: ",
        )
        vector = [0.0] * 512
        response = {"data": [{"index": 0, "embedding": vector}]}
        with mock.patch.object(prefixed, "_post", return_value=response) as post:
            prefixed.embed_documents(["decision"])
            prefixed.embed_query("what did we choose?")
        self.assertEqual(
            [call.args[1]["input"][0] for call in post.call_args_list],
            ["search_document: decision", "search_query: what did we choose?"],
        )
        self.assertNotEqual(base.fingerprint, prefixed.fingerprint)

    def test_planner_must_match_the_approved_router(self) -> None:
        with self.assertRaisesRegex(ValueError, "approved router"):
            SemanticRuntime(
                embedding_url="http://127.0.0.1:8081",
                model="synthetic-embedding",
                revision=DEFAULT_EMBEDDING_REVISION,
                dimensions=512,
                planner_url="https://provider.example",
                planner_approved_url="https://router.example",
                planner_model="synthetic-planner",
                planner_key_file="/tmp/synthetic.key",
            )

    def test_transport_rejects_redirects(self) -> None:
        runtime = self.runtime()
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"{}"
        opener = mock.MagicMock()
        opener.open.return_value = response
        with mock.patch(
            "recall_server.semantic.urllib.request.build_opener", return_value=opener
        ) as build:
            self.assertEqual(runtime._post("http://127.0.0.1:8081/embed", {}), {})
        self.assertIsInstance(build.call_args.args[0], _RejectRedirect)

    def test_transport_bounds_response_bytes(self) -> None:
        response = mock.MagicMock()
        response.read.return_value = b"x" * (8 * 1024 * 1024 + 1)
        with self.assertRaisesRegex(ValueError, "too large"):
            SemanticRuntime._read_json(response)

    def test_embedding_batch_and_query_instruction_are_validated(self) -> None:
        runtime = self.runtime()
        runtime._embedding_identity_checked = True
        vector = [0.0] * 512
        with mock.patch.object(runtime, "_post", return_value=[vector]) as post:
            self.assertEqual(runtime.embed_query("find the rollout"), vector)
        self.assertTrue(
            post.call_args.args[1]["inputs"][0].startswith(QUERY_INSTRUCTION)
        )
        with mock.patch.object(runtime, "_post", return_value=[[0.0] * 8]):
            with self.assertRaisesRegex(ValueError, "dimensions"):
                runtime.embed_documents(["bad"])

    def test_document_batches_are_bounded_and_fingerprinted(self) -> None:
        runtime = SemanticRuntime(
            embedding_url="http://127.0.0.1:8081",
            model="synthetic-embedding",
            revision=DEFAULT_EMBEDDING_REVISION,
            dimensions=512,
            embedding_batch_size=2,
        )
        runtime._embedding_identity_checked = True
        vector = [0.0] * 512
        with mock.patch.object(
            runtime,
            "_post",
            side_effect=[
                [vector, vector],
                [vector, vector],
                [vector],
            ],
        ) as post:
            self.assertEqual(len(runtime.embed_documents(["a", "b", "c", "d", "e"])), 5)
        self.assertEqual(
            [len(call.args[1]["inputs"]) for call in post.call_args_list], [2, 2, 1]
        )
        self.assertRegex(runtime.fingerprint, r"^[0-9a-f]{64}$")

    def test_concurrent_embedding_calls_queue_through_the_single_local_sidecar(
        self,
    ) -> None:
        runtime = self.runtime()
        runtime._embedding_identity_checked = True
        vector = [0.0] * 512
        first_entered = threading.Event()
        release_first = threading.Event()
        state_lock = threading.Lock()
        active = 0
        maximum_active = 0

        def local_post(_url, payload, _headers=None):
            nonlocal active, maximum_active
            with state_lock:
                active += 1
                maximum_active = max(maximum_active, active)
                first = active == 1 and not first_entered.is_set()
                if first:
                    first_entered.set()
            if first:
                release_first.wait(timeout=1)
            time.sleep(0.02)
            with state_lock:
                active -= 1
            return [vector for _value in payload["inputs"]]

        with mock.patch.object(runtime, "_post", side_effect=local_post):
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(runtime.embed_documents, [text])
                    for text in ("first", "second")
                ]
                self.assertTrue(first_entered.wait(timeout=1))
                time.sleep(0.05)
                release_first.set()
                self.assertEqual(
                    [future.result(timeout=1) for future in futures],
                    [[vector], [vector]],
                )

        self.assertEqual(maximum_active, 1)

    def test_oversized_documents_keep_a_bounded_head_and_tail(self) -> None:
        runtime = self.runtime()
        runtime._embedding_identity_checked = True
        value = "H" * 5000 + "T" * 5000
        vector = [0.0] * 512
        with mock.patch.object(runtime, "_post", return_value=[vector]) as post:
            runtime.embed_documents([value])
        projected = post.call_args.args[1]["inputs"][0]
        self.assertEqual(len(projected), MAX_DOCUMENT_CHARS)
        self.assertIn(DOCUMENT_CLIP_MARKER, projected)
        self.assertTrue(projected.startswith("H"))
        self.assertTrue(projected.endswith("T"))

    def test_embedding_revision_is_verified_before_content_leaves(self) -> None:
        runtime = self.runtime()
        vector = [0.0] * 512
        info = {
            "model_id": "synthetic-embedding",
            "model_sha": DEFAULT_EMBEDDING_REVISION,
            "model_dtype": "float32",
        }
        with (
            mock.patch.object(runtime, "_get", return_value=info) as get,
            mock.patch.object(runtime, "_post", return_value=[vector]),
        ):
            self.assertEqual(runtime.embed_documents(["safe"]), [vector])
            self.assertEqual(runtime.embed_documents(["safe again"]), [vector])
        self.assertEqual(get.call_count, 1)
        mismatch = self.runtime()
        with (
            mock.patch.object(
                mismatch, "_get", return_value={**info, "model_sha": "0" * 40}
            ),
            mock.patch.object(mismatch, "_post") as post,
        ):
            with self.assertRaisesRegex(ValueError, "pinned runtime"):
                mismatch.embed_documents(["must stay local"])
        post.assert_not_called()

    def test_planner_key_is_reloaded_from_owner_only_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            key = Path(temporary) / "planner.key"
            key.write_text("short-lived-synthetic-key")
            os.chmod(key, 0o600)
            runtime = self.runtime(str(key))
            response = {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '```json\n{"searchable":true,"phrases":["retry budget",'
                                '"retry budget","event replay"]}\n```'
                            )
                        }
                    }
                ]
            }
            with mock.patch.object(runtime, "_post", return_value=response) as post:
                self.assertEqual(
                    runtime.plan("bounded attempts"),
                    SearchPlan(True, ("retry budget", "event replay")),
                )
            self.assertEqual(
                post.call_args.args[2]["Authorization"],
                "Bearer short-lived-synthetic-key",
            )
            os.chmod(key, 0o644)
            with self.assertRaisesRegex(PermissionError, "owner-only"):
                runtime.plan("a different query")

    def test_query_caches_are_bounded_and_do_not_use_raw_query_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            key = Path(temporary) / "planner.key"
            key.write_text("short-lived-synthetic-key")
            os.chmod(key, 0o600)
            runtime = SemanticRuntime(
                embedding_url="http://127.0.0.1:8081",
                model="synthetic-embedding",
                revision=DEFAULT_EMBEDDING_REVISION,
                dimensions=512,
                planner_url="https://router.example",
                planner_approved_url="https://router.example",
                planner_model="synthetic-planner",
                planner_key_file=str(key),
                planner_samples=1,
                cache_size=1,
            )
            response = {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"searchable":true,"phrases":["advisory lock"]}'
                            )
                        }
                    }
                ]
            }
            vector = [0.0] * 512
            runtime._embedding_identity_checked = True
            with mock.patch.object(
                runtime, "_post", side_effect=[response, [vector]]
            ) as post:
                self.assertEqual(
                    runtime.plan("duplicate work"), runtime.plan("duplicate work")
                )
                self.assertEqual(
                    runtime.embed_query("duplicate work"),
                    runtime.embed_query("duplicate work"),
                )
            self.assertEqual(post.call_count, 2)
            self.assertNotIn("duplicate work", runtime._plan_cache)
            self.assertNotIn("duplicate work", runtime._query_embedding_cache)

    def test_planner_samples_union_valid_results_and_tolerate_one_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            key = Path(temporary) / "planner.key"
            key.write_text("short-lived-synthetic-key")
            os.chmod(key, 0o600)
            runtime = SemanticRuntime(
                embedding_url="http://127.0.0.1:8081",
                model="synthetic-embedding",
                revision=DEFAULT_EMBEDDING_REVISION,
                dimensions=512,
                planner_url="https://router.example",
                planner_approved_url="https://router.example",
                planner_model="synthetic-planner",
                planner_key_file=str(key),
                planner_samples=2,
            )
            response = {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"searchable":true,"phrases":["token refresh","credential renewal"]}'
                            )
                        }
                    }
                ]
            }
            with mock.patch.object(
                runtime, "_post", side_effect=[response, TimeoutError("synthetic")]
            ):
                self.assertEqual(
                    runtime.plan("renew credentials"),
                    SearchPlan(True, ("token refresh", "credential renewal")),
                )


if __name__ == "__main__":
    unittest.main()
