from __future__ import annotations

import io
import json
import os
import sys
import time
import threading
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "server"))

from recall_server.judgments import (  # noqa: E402
    JEV_MODEL,
    MAX_JUDGMENT_REQUEST_BYTES,
    MAX_JUDGMENT_RESPONSE_BYTES,
    ChoiceAnswer,
    JudgmentClient,
    JudgmentUnavailable,
    NoulAnswer,
    ScoreAnswer,
    UrllibJudgmentTransport,
)
from central_brain.fake_judgments import (  # noqa: E402
    FakeJudgmentTransport,
    primitive_questions,
    primitive_response,
)

ENDPOINT = "http://127.0.0.1:9411/typesafe/v1/systemone"


class JudgmentContractTest(unittest.TestCase):
    def client(self, transport=None, **settings):
        return JudgmentClient(
            endpoint=ENDPOINT,
            broker_key="not-a-secret",
            transport=transport or FakeJudgmentTransport(),
            **settings,
        )

    def assert_unavailable(self, payload, code="judgment_response_invalid"):
        transport = FakeJudgmentTransport(payload)
        with self.assertRaises(JudgmentUnavailable) as caught:
            self.client(transport).judge(
                state="private passage", questions=primitive_questions()
            )
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(len(transport.calls), 1)
        self.assertNotIn("private", str(caught.exception))
        return caught.exception

    def test_all_primitives_share_one_pinned_request_and_return_typed_answers(self):
        transport = FakeJudgmentTransport()
        result = self.client(transport).judge(
            state={
                "message": "Read the chat immediately",
                "evidence": ["A direct answer"],
            },
            questions=primitive_questions(),
        )
        self.assertEqual(len(transport.calls), 1)
        request = transport.calls[0]
        self.assertEqual(request["url"], ENDPOINT)
        self.assertEqual(request["headers"]["Authorization"], "Bearer not-a-secret")
        self.assertEqual(request["body"]["model"], JEV_MODEL)
        self.assertEqual(request["body"]["questions"], primitive_questions())
        self.assertIsInstance(request["body"]["questions"]["quality"]["criteria"], list)
        self.assertEqual(result.answers["urgent"], NoulAnswer(noul=0.92))
        self.assertFalse(hasattr(result.answers["urgent"], "confidence"))
        self.assertIsInstance(result.answers["source"], ChoiceAnswer)
        self.assertEqual(result.answers["source"].choice, "chat")
        self.assertIsInstance(result.answers["quality"], ScoreAnswer)
        self.assertEqual(result.answers["quality"].legend["2"], "Direct answer")
        self.assertAlmostEqual(result.answers["quality"].score, 1.6)
        self.assertEqual(result.usage.input_tokens, 312)
        self.assertEqual(result.usage.output_tokens, 48)
        self.assertEqual(result.model, JEV_MODEL)

    def test_metadata_contains_no_state_rubric_or_choice_values(self):
        result = self.client().judge(
            state="private passage", questions=primitive_questions()
        )
        metadata = result.metadata
        self.assertEqual(metadata["question_count"], 3)
        self.assertEqual(metadata["input_tokens"], 312)
        self.assertEqual(len(metadata["contract_hash"]), 64)
        self.assertGreaterEqual(metadata["elapsed_ms"], 0)
        rendered = json.dumps(metadata) + repr(result)
        for text in ("private passage", "Direct answer", "chat", "urgent"):
            self.assertNotIn(text, rendered)

    def test_contract_hash_tracks_questions_and_model_not_state_or_mapping_order(self):
        questions = primitive_questions()
        first = self.client().judge(state="a", questions=questions)
        reordered = dict(reversed(list(questions.items())))
        second = self.client().judge(state="b", questions=reordered)
        self.assertEqual(first.contract_hash, second.contract_hash)
        questions["urgent"]["instructions"] += " Explicit requests only."
        third = self.client().judge(state="a", questions=questions)
        self.assertNotEqual(first.contract_hash, third.contract_hash)

    def test_missing_usage_is_unknown_not_zero(self):
        for usage in ({}, {"input_tokens": None, "output_tokens": None}):
            payload = primitive_response()
            payload["usage"] = usage
            result = self.client(FakeJudgmentTransport(payload)).judge(
                state="x", questions=primitive_questions()
            )
            self.assertIsNone(result.usage.input_tokens)
            self.assertIsNone(result.usage.output_tokens)

    def test_rejects_incomplete_extra_or_mismatched_answers(self):
        for mutate in (
            lambda p: p["answers"].pop("urgent"),
            lambda p: p["answers"].update(extra={"type": "noul", "noul": 0.9}),
            lambda p: p["answers"]["urgent"].update(type="score"),
            lambda p: p.update(answers=[]),
            lambda p: p.pop("usage"),
        ):
            with self.subTest(mutate=mutate):
                payload = primitive_response()
                mutate(payload)
                self.assert_unavailable(payload)

    def test_rejects_nonfinite_bool_or_out_of_range_probabilities(self):
        for value in (
            float("nan"),
            float("inf"),
            -0.1,
            1.1,
            10**1000,
            True,
            "0.9",
            None,
        ):
            for answer, field in (("urgent", "noul"), ("source", "confidence")):
                with self.subTest(value=value, answer=answer):
                    payload = primitive_response()
                    payload["answers"][answer][field] = value
                    self.assert_unavailable(payload)

    def test_choice_distribution_covers_exact_options_and_agrees_with_choice(self):
        for distribution, choice in (
            ({"chat": 1.0}, "chat"),
            ({"chat": 0.8, "code": 0.1, "none": 0.1, "extra": 0.0}, "chat"),
            ({"chat": 0.1, "code": 0.8, "none": 0.1}, "chat"),
            ({"chat": 0.8, "code": 0.1, "none": 0.1}, "invented"),
            ({"chat": True, "code": 0.0, "none": 0.0}, "chat"),
        ):
            with self.subTest(distribution=distribution, choice=choice):
                payload = primitive_response()
                payload["answers"]["source"].update(
                    probabilities=distribution, choice=choice
                )
                self.assert_unavailable(payload)

        for invalid in (float("nan"), float("inf"), -0.1, 1.1):
            with self.subTest(invalid=invalid):
                payload = primitive_response()
                payload["answers"]["source"]["probabilities"]["chat"] = invalid
                self.assert_unavailable(payload)

    def test_choice_preserves_rounded_wire_distribution_without_normalizing(self):
        # Numeric fields from Jev 1.13.0; synthetic labels replace question meaning.
        for values, selected in (
            ([0.0, 0.01, 0.0, 0.0, 0.01, 0.0, 0.54, 0.0, 0.0, 0.4, 0.01, 0.02], 6),
            ([0.17, 0.01, 0.0, 0.0, 0.0, 0.81, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 5),
        ):
            with self.subTest(option_count=len(values)):
                questions = primitive_questions()
                probabilities = {
                    f"option_{index}": value for index, value in enumerate(values)
                }
                questions["source"]["criteria"] = dict.fromkeys(probabilities)
                payload = primitive_response()
                payload["answers"]["source"].update(
                    probabilities=probabilities, choice=f"option_{selected}"
                )
                result = self.client(FakeJudgmentTransport(payload)).judge(
                    state="synthetic state", questions=questions
                )
                self.assertEqual(result.answers["source"].probabilities, probabilities)
                self.assertAlmostEqual(sum(probabilities.values()), 0.99)

    def test_score_preserves_independently_rounded_wire_fields(self):
        # Numeric fields from a Jev 1.13.0 response; no private state or rubric.
        for score, distribution in (
            (3.41, [0.0, 0.0, 0.12, 0.34, 0.54]),
            (2.59, [0.01, 0.05, 0.47, 0.29, 0.18]),
            (1.35, [0.13, 0.49, 0.31, 0.05, 0.02]),
        ):
            with self.subTest(score=score):
                questions = primitive_questions()
                questions["quality"]["criteria"] = [
                    f"Level {index}" for index in range(5)
                ]
                probabilities = {
                    str(index): value for index, value in enumerate(distribution)
                }
                payload = primitive_response()
                payload["answers"]["quality"].update(
                    score=score,
                    probabilities=probabilities,
                    legend={
                        str(index): level
                        for index, level in enumerate(questions["quality"]["criteria"])
                    },
                )
                result = self.client(FakeJudgmentTransport(payload)).judge(
                    state="synthetic state", questions=questions
                )
                self.assertEqual(result.answers["quality"].score, score)
                self.assertEqual(result.answers["quality"].probabilities, probabilities)

    def test_score_requires_matching_legend_all_levels_and_bounded_numeric_value(self):
        for mutate in (
            lambda a: a.pop("legend"),
            lambda a: a["legend"].update({"0": "Invented rubric"}),
            lambda a: a["probabilities"].pop("0"),
            lambda a: a.update(score=-0.1),
            lambda a: a.update(score=3),
            lambda a: a.update(score=True),
            lambda a: a.update(score=float("nan")),
            lambda a: a.update(score=float("inf")),
            lambda a: a.update(score="1.6"),
        ):
            with self.subTest(mutate=mutate):
                payload = primitive_response()
                mutate(payload["answers"]["quality"])
                self.assert_unavailable(payload)

    def test_wrong_resolved_model_and_invalid_usage_are_rejected(self):
        payload = primitive_response()
        payload["model"] = "jev-latest"
        self.assert_unavailable(payload, "judgment_model_mismatch")
        for invalid in (-1, True, 1.5, "312"):
            payload = primitive_response()
            payload["usage"]["input_tokens"] = invalid
            self.assert_unavailable(payload)

    def test_billable_usage_survives_invalid_answer(self):
        payload = primitive_response()
        payload["answers"].pop("urgent")
        error = self.assert_unavailable(payload)
        self.assertEqual(error.usage.input_tokens, 312)
        self.assertEqual(error.metadata["input_tokens"], 312)

    def test_invalid_inputs_never_dispatch_or_silently_truncate(self):
        transport = FakeJudgmentTransport()
        for state, questions in (
            (True, primitive_questions()),
            ("x", {}),
            ("x", {"q": {"type": "noul"}}),
            ("x", {"q": {"type": "noul", "instructions": ""}}),
            ("x", {"q": {"type": "choice", "instructions": "Select", "criteria": {}}}),
            (
                "x",
                {
                    "q": {
                        "type": "score",
                        "instructions": "Score",
                        "criteria": {"0": "No", "1": "Yes"},
                    }
                },
            ),
            (
                "x",
                {"q": {"type": "score", "instructions": "Score", "criteria": ["Only"]}},
            ),
            ({"bad": float("nan")}, primitive_questions()),
            ({1: "one", "1": "another"}, primitive_questions()),
        ):
            with self.subTest(state=state, questions=questions):
                with self.assertRaises(JudgmentUnavailable) as caught:
                    self.client(transport).judge(state=state, questions=questions)
                self.assertEqual(caught.exception.code, "judgment_request_invalid")
        with self.assertRaises(JudgmentUnavailable) as caught:
            self.client(transport).judge(
                state="x" * MAX_JUDGMENT_REQUEST_BYTES, questions=primitive_questions()
            )
        self.assertEqual(caught.exception.code, "judgment_request_too_large")
        self.assertEqual(transport.calls, [])

    def test_structured_instructions_and_rubrics_are_preserved(self):
        questions = primitive_questions()
        questions["urgent"]["instructions"] = {
            "question": "Urgent?",
            "exceptions": ["Quotation"],
        }
        questions["quality"]["criteria"][0] = {
            "description": "Unrelated",
            "examples": ["Wrong topic"],
        }
        payload = primitive_response()
        payload["answers"]["quality"]["legend"]["0"] = questions["quality"]["criteria"][
            0
        ]
        transport = FakeJudgmentTransport(payload)
        result = self.client(transport).judge(
            state=[{"text": "x"}], questions=questions
        )
        self.assertEqual(transport.calls[0]["body"]["questions"], questions)
        self.assertEqual(
            result.answers["quality"].legend["0"], questions["quality"]["criteria"][0]
        )

    def test_absolute_deadline_clips_timeout_and_rejects_stale_results(self):
        transport = FakeJudgmentTransport()
        self.client(transport, timeout_seconds=2).judge(
            state="x", questions=primitive_questions(), deadline=time.monotonic() + 0.2
        )
        self.assertGreater(transport.calls[0]["timeout"], 0)
        self.assertLessEqual(transport.calls[0]["timeout"], 0.2)
        for deadline in (
            time.monotonic() - 1,
            float("nan"),
            float("inf"),
            10**1000,
            True,
        ):
            with self.subTest(deadline=deadline):
                refused = FakeJudgmentTransport()
                with self.assertRaises(JudgmentUnavailable) as caught:
                    self.client(refused).judge(
                        state="x", questions=primitive_questions(), deadline=deadline
                    )
                self.assertEqual(caught.exception.code, "judgment_deadline_exhausted")
                self.assertEqual(refused.calls, [])
        late = FakeJudgmentTransport(delay=0.02)
        with self.assertRaises(JudgmentUnavailable) as caught:
            self.client(late).judge(
                state="x",
                questions=primitive_questions(),
                deadline=time.monotonic() + 0.005,
            )
        self.assertEqual(caught.exception.code, "judgment_deadline_exhausted")
        self.assertEqual(caught.exception.usage.input_tokens, 312)
        self.assertEqual(len(late.calls), 1)

    def test_transport_errors_never_retry_or_disclose_payloads(self):
        for error in (
            TimeoutError("private bearer secret"),
            RuntimeError("private bearer secret"),
        ):
            transport = FakeJudgmentTransport(error=error)
            with self.assertRaises(JudgmentUnavailable) as caught:
                self.client(transport).judge(
                    state="private text", questions=primitive_questions()
                )
            self.assertEqual(caught.exception.code, "judgment_transport_error")
            self.assertEqual(len(transport.calls), 1)
            self.assertIsNone(caught.exception.usage.input_tokens)
            self.assertNotIn(
                "private", str(caught.exception) + json.dumps(caught.exception.metadata)
            )

    def test_requires_explicit_broker_endpoint_and_rejects_provider_credentials_sources(
        self,
    ):
        for endpoint, approved in (
            (
                "https://api.typesafe.ai/v1/systemone",
                "https://api.typesafe.ai/v1/systemone",
            ),
            (
                "https://api.typesafe.ai/typesafe/v1/systemone",
                "https://api.typesafe.ai/typesafe/v1/systemone",
            ),
            (
                "https://api.typesafe.ai./typesafe/v1/systemone",
                "https://api.typesafe.ai./typesafe/v1/systemone",
            ),
            (
                "http://broker.example/typesafe/v1/systemone",
                "http://broker.example/typesafe/v1/systemone",
            ),
            ("https://broker.example/typesafe/v1/systemone", None),
            (
                "https://broker.example/typesafe/v1/systemone",
                "https://other.example/typesafe/v1/systemone",
            ),
            (ENDPOINT + "?target=elsewhere", None),
            (ENDPOINT + "#fragment", None),
            ("http://user:secret@127.0.0.1:9411/typesafe/v1/systemone", None),
        ):
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(ValueError):
                    JudgmentClient(
                        endpoint=endpoint,
                        approved_endpoint=approved,
                        broker_key="not-a-secret",
                    )
        remote = "https://broker.example/typesafe/v1/systemone"
        self.assertEqual(
            JudgmentClient(
                endpoint=remote, approved_endpoint=remote, broker_key="leased"
            ).endpoint,
            remote,
        )
        with mock.patch.dict(
            os.environ,
            {
                "TYPESAFE_API_KEY": "must-never-be-read",
                "LITELLM_API_KEY": "must-be-explicit",
            },
        ):
            with self.assertRaises(ValueError):
                JudgmentClient(endpoint=ENDPOINT, broker_key="")
        for key in ("x\r\ninjected: header", "x\n", "x" * 4097):
            with self.assertRaises(ValueError):
                JudgmentClient(endpoint=ENDPOINT, broker_key=key)
        for timeout in (0, float("nan"), float("inf"), True, 31, 10**1000):
            with self.assertRaises(ValueError):
                self.client(timeout_seconds=timeout)


class JudgmentHttpTest(unittest.TestCase):
    def post(self):
        return UrllibJudgmentTransport().post(
            url=ENDPOINT,
            headers={"Authorization": "Bearer not-a-secret"},
            body={},
            timeout=0.1,
        )

    def test_redirect_status_rate_limit_and_server_errors_are_content_free_and_single_attempt(
        self,
    ):
        for status in (302, 307, 401, 422, 429, 500, 529):
            with self.subTest(status=status):
                error = urllib.error.HTTPError(
                    ENDPOINT,
                    status,
                    "private response",
                    {"Location": "https://evil.example", "Retry-After": "1"},
                    io.BytesIO(b"private text"),
                )
                with mock.patch(
                    "urllib.request.OpenerDirector.open", side_effect=error
                ) as request:
                    with self.assertRaises(JudgmentUnavailable) as caught:
                        self.post()
                self.assertEqual(request.call_count, 1)
                self.assertEqual(caught.exception.status_code, status)
                self.assertEqual(
                    caught.exception.code,
                    "judgment_redirect_refused"
                    if status < 400
                    else "judgment_http_status",
                )
                self.assertNotIn("private", str(caught.exception))

    def test_default_transport_disables_redirects_and_environment_proxy(self):
        with mock.patch(
            "urllib.request.OpenerDirector.open", side_effect=TimeoutError
        ) as request:
            with mock.patch(
                "urllib.request.build_opener", wraps=urllib.request.build_opener
            ) as build:
                with self.assertRaises(JudgmentUnavailable):
                    self.post()
        handlers = build.call_args.args
        redirects = [
            h for h in handlers if isinstance(h, urllib.request.HTTPRedirectHandler)
        ]
        proxies = [h for h in handlers if isinstance(h, urllib.request.ProxyHandler)]
        self.assertEqual(len(redirects), 1)
        self.assertIsNone(
            redirects[0].redirect_request(
                None, None, 302, "", {}, "https://evil.example"
            )
        )
        self.assertEqual(proxies[0].proxies, {})
        self.assertEqual(request.call_count, 1)

    def test_bounded_response_and_invalid_json(self):
        class Response(io.BytesIO):
            status = 200

            def __init__(self, content, headers):
                super().__init__(content)
                self.headers = headers

        for content, headers, code in (
            (
                b"",
                {"Content-Length": str(MAX_JUDGMENT_RESPONSE_BYTES + 1)},
                "judgment_response_too_large",
            ),
            (
                b"x" * (MAX_JUDGMENT_RESPONSE_BYTES + 1),
                {},
                "judgment_response_too_large",
            ),
            (b"private malformed", {}, "judgment_response_invalid"),
            (b'{"duplicate":1,"duplicate":2}', {}, "judgment_response_invalid"),
        ):
            with self.subTest(code=code, headers=headers):
                with mock.patch(
                    "urllib.request.OpenerDirector.open",
                    return_value=Response(content, headers),
                ):
                    with self.assertRaises(JudgmentUnavailable) as caught:
                        self.post()
                self.assertEqual(caught.exception.code, code)

    def test_default_client_round_trip_over_real_local_http(self):
        captured = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                captured.append((self.path, self.headers, body))
                response = json.dumps(primitive_response()).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        thread.start()
        try:
            endpoint = f"http://127.0.0.1:{server.server_port}/typesafe/v1/systemone"
            result = JudgmentClient(endpoint=endpoint, broker_key="synthetic").judge(
                state="A synthetic café request",
                questions=primitive_questions(),
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=1)
        self.assertEqual(len(captured), 1)
        path, headers, body = captured[0]
        self.assertEqual(path, "/typesafe/v1/systemone")
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(headers["Authorization"], "Bearer synthetic")
        self.assertIn("café".encode(), body)
        self.assertEqual(json.loads(body)["model"], JEV_MODEL)
        self.assertEqual(result.answers["urgent"].noul, 0.92)


if __name__ == "__main__":
    unittest.main()
