"""Small, broker-only boundary for TypeSafe's three judgment primitives.

No retrieval policy, provider credentials, SDK retries, or production wiring live
here. Callers supply complete named questions and consume typed answers. Missing
or invalid answers fail the whole request; the caller chooses its fallback.
Errors and ``metadata`` contain counts/codes, never state, rubrics, or answers.
"""

from __future__ import annotations

import hashlib
import json
import math
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlsplit

JEV_MODEL = "jev-1.13.0"
JUDGMENT_CONTRACT = "recall.judgments.v1"
MAX_JUDGMENT_REQUEST_BYTES = 1024 * 1024
MAX_JUDGMENT_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_QUESTIONS = 128

State = str | dict[str, Any] | list[Any]


@dataclass(frozen=True)
class NoulAnswer:
    noul: float


@dataclass(frozen=True)
class ChoiceAnswer:
    choice: str
    probabilities: dict[str, float]
    confidence: float


@dataclass(frozen=True)
class ScoreAnswer:
    score: float
    legend: dict[str, State]
    probabilities: dict[str, float]
    confidence: float


Answer = NoulAnswer | ChoiceAnswer | ScoreAnswer


@dataclass(frozen=True)
class TokenUsage:
    # A dispatched request can incur cost even when its response is unavailable.
    input_tokens: int | None = None
    output_tokens: int | None = None


@dataclass(frozen=True)
class JudgmentResult:
    answers: dict[str, Answer] = field(repr=False)
    model: str
    usage: TokenUsage
    contract_hash: str
    elapsed_ms: float

    @property
    def metadata(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "contract_hash": self.contract_hash,
            "question_count": len(self.answers),
            "elapsed_ms": self.elapsed_ms,
            "input_tokens": self.usage.input_tokens,
            "output_tokens": self.usage.output_tokens,
        }


class JudgmentUnavailable(RuntimeError):
    """Content-free failure. Unknown usage is never reported as zero spend."""

    def __init__(self, code: str, *, status_code: int | None = None):
        super().__init__(code)
        self.code = code
        self.status_code = status_code
        self.usage = TokenUsage()
        self.elapsed_ms = 0.0

    @property
    def metadata(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "status_code": self.status_code,
            "elapsed_ms": self.elapsed_ms,
            "input_tokens": self.usage.input_tokens,
            "output_tokens": self.usage.output_tokens,
        }


class JudgmentTransport(Protocol):
    def post(
        self, *, url: str, headers: dict[str, str], body: dict[str, Any], timeout: float
    ) -> Any: ...


class _RejectRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _unique_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate JSON key")
        result[name] = value
    return result


class UrllibJudgmentTransport:
    """One attempt, verified TLS, no redirects or ambient proxies, bounded body."""

    def post(
        self, *, url: str, headers: dict[str, str], body: dict[str, Any], timeout: float
    ) -> Any:
        request = urllib.request.Request(
            url,
            data=json.dumps(
                body, allow_nan=False, ensure_ascii=False, separators=(",", ":")
            ).encode(),
            method="POST",
            headers={
                **headers,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "recall-core/judgments-v1",
            },
        )
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _RejectRedirect(),
            urllib.request.HTTPSHandler(context=ssl.create_default_context()),
        )
        try:
            with opener.open(request, timeout=timeout) as response:
                if response.status != 200:
                    raise JudgmentUnavailable(
                        "judgment_http_status", status_code=response.status
                    )
                length = response.headers.get("Content-Length")
                if length is not None and (
                    not length.isdigit() or int(length) > MAX_JUDGMENT_RESPONSE_BYTES
                ):
                    raise JudgmentUnavailable("judgment_response_too_large")
                raw = response.read(MAX_JUDGMENT_RESPONSE_BYTES + 1)
        except JudgmentUnavailable:
            raise
        except urllib.error.HTTPError as error:
            code = (
                "judgment_redirect_refused"
                if 300 <= error.code < 400
                else "judgment_http_status"
            )
            error.close()
            raise JudgmentUnavailable(code, status_code=error.code) from None
        except Exception:  # Never expose a broker error body, request, or credential.
            raise JudgmentUnavailable("judgment_transport_error") from None
        if len(raw) > MAX_JUDGMENT_RESPONSE_BYTES:
            raise JudgmentUnavailable("judgment_response_too_large")
        try:
            return json.loads(raw, object_pairs_hook=_unique_object)
        except (ValueError, RecursionError):
            raise JudgmentUnavailable("judgment_response_invalid") from None


def _validate_endpoint(endpoint: str, approved: str | None) -> str:
    if not isinstance(endpoint, str) or (
        approved is not None and not isinstance(approved, str)
    ):
        raise ValueError("judgment endpoint must be a plain broker URL")
    try:
        parsed = urlsplit(endpoint)
        port = parsed.port
    except (ValueError, TypeError):
        raise ValueError("judgment endpoint must be a plain broker URL") from None
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.hostname.rstrip(".") == "typesafe.ai"
        or parsed.hostname.rstrip(".").endswith(".typesafe.ai")
        or not parsed.path.rstrip("/").endswith("/typesafe/v1/systemone")
        or port == 0
    ):
        raise ValueError("judgment endpoint must be a plain broker URL")
    loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    if parsed.scheme != "https" and not (loopback and parsed.scheme == "http"):
        raise ValueError("remote judgment broker must use HTTPS")
    if not loopback and endpoint.rstrip("/") != (approved or "").rstrip("/"):
        raise ValueError("remote judgment broker requires an exactly approved endpoint")
    return endpoint.rstrip("/")


def _instruction(value: Any) -> bool:
    return (
        isinstance(value, (str, dict, list))
        and bool(value)
        and (not isinstance(value, str) or bool(value.strip()))
    )


def prepare_judgment_request(state: State, questions: dict[str, Any]) -> dict[str, Any]:
    """Validate and snapshot a request without network access or spending tokens."""
    if (
        not isinstance(state, (str, dict, list))
        or not isinstance(questions, dict)
        or not 1 <= len(questions) <= MAX_QUESTIONS
    ):
        raise JudgmentUnavailable("judgment_request_invalid")
    for name, question in questions.items():
        if (
            not isinstance(name, str)
            or not name.strip()
            or len(name) > 128
            or not isinstance(question, dict)
        ):
            raise JudgmentUnavailable("judgment_request_invalid")
        kind, criteria = question.get("type"), question.get("criteria")
        if set(question) - {"type", "instructions", "criteria"} or not _instruction(
            question.get("instructions")
        ):
            raise JudgmentUnavailable("judgment_request_invalid")
        if kind == "noul":
            valid = criteria is None or (
                isinstance(criteria, dict)
                and set(criteria) <= {"true", "false"}
                and all(_instruction(value) for value in criteria.values())
            )
        elif kind == "choice":
            valid = (
                isinstance(criteria, dict)
                and bool(criteria)
                and all(
                    isinstance(option, str)
                    and bool(option)
                    and (value is None or _instruction(value))
                    for option, value in criteria.items()
                )
            )
        elif kind == "score":
            valid = (
                isinstance(criteria, list)
                and len(criteria) >= 2
                and all(_instruction(value) for value in criteria)
            )
        else:
            valid = False
        if not valid:
            raise JudgmentUnavailable("judgment_request_invalid")
    try:
        encoded = json.dumps(
            {"state": state, "model": JEV_MODEL, "questions": questions},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    except (TypeError, ValueError, RecursionError):
        raise JudgmentUnavailable("judgment_request_invalid") from None
    if len(encoded) > MAX_JUDGMENT_REQUEST_BYTES:
        raise JudgmentUnavailable("judgment_request_too_large")
    # Snapshot the request: mutation by the caller cannot change the rubric
    # between dispatch and response validation.
    try:
        return json.loads(encoded, object_pairs_hook=_unique_object)
    except ValueError:
        raise JudgmentUnavailable("judgment_request_invalid") from None


def _number(value: Any, lower: float, upper: float) -> float:
    # Check the range before converting an arbitrary JSON integer to float.
    if (
        type(value) not in (int, float)
        or not lower <= value <= upper
        or not math.isfinite(value)
    ):
        raise JudgmentUnavailable("judgment_response_invalid")
    return float(value)


def _probabilities(value: Any, options: set[str]) -> dict[str, float]:
    if not isinstance(value, dict) or set(value) != options:
        raise JudgmentUnavailable("judgment_response_invalid")
    result = {name: _number(number, 0, 1) for name, number in value.items()}
    if not math.isclose(sum(result.values()), 1, rel_tol=0, abs_tol=0.001):
        raise JudgmentUnavailable("judgment_response_invalid")
    return result


def _usage(payload: Any) -> TokenUsage:
    if not isinstance(payload, dict) or not isinstance(payload.get("usage"), dict):
        raise JudgmentUnavailable("judgment_response_invalid")
    values = [payload["usage"].get(name) for name in ("input_tokens", "output_tokens")]
    if any(
        value is not None and (type(value) is not int or value < 0) for value in values
    ):
        raise JudgmentUnavailable("judgment_response_invalid")
    return TokenUsage(*values)


def _answers(payload: dict, questions: dict) -> dict[str, Answer]:
    if payload.get("model") != JEV_MODEL:
        raise JudgmentUnavailable("judgment_model_mismatch")
    answers = payload.get("answers")
    if not isinstance(answers, dict) or set(answers) != set(questions):
        raise JudgmentUnavailable("judgment_response_invalid")
    result: dict[str, Answer] = {}
    for name, question in questions.items():
        answer = answers[name]
        if not isinstance(answer, dict) or answer.get("type") != question["type"]:
            raise JudgmentUnavailable("judgment_response_invalid")
        if question["type"] == "noul":
            result[name] = NoulAnswer(_number(answer.get("noul"), 0, 1))
            continue
        confidence = _number(answer.get("confidence"), 0, 1)
        if question["type"] == "choice":
            probabilities = _probabilities(
                answer.get("probabilities"), set(question["criteria"])
            )
            choice = answer.get("choice")
            if (
                not isinstance(choice, str)
                or choice not in probabilities
                or probabilities[choice] < max(probabilities.values())
            ):
                raise JudgmentUnavailable("judgment_response_invalid")
            result[name] = ChoiceAnswer(choice, probabilities, confidence)
        else:
            legend = {
                str(index): level for index, level in enumerate(question["criteria"])
            }
            if answer.get("legend") != legend:
                raise JudgmentUnavailable("judgment_response_invalid")
            probabilities = _probabilities(answer.get("probabilities"), set(legend))
            score = _number(answer.get("score"), 0, len(legend) - 1)
            expected = sum(
                int(level) * probability for level, probability in probabilities.items()
            )
            if not math.isclose(score, expected, rel_tol=0, abs_tol=0.01):
                raise JudgmentUnavailable("judgment_response_invalid")
            result[name] = ScoreAnswer(score, legend, probabilities, confidence)
    return result


class JudgmentClient:
    """Synchronous, explicit broker client; no provider or environment fallback."""

    def __init__(
        self,
        *,
        endpoint: str,
        broker_key: str,
        approved_endpoint: str | None = None,
        timeout_seconds: float = 2.0,
        transport: JudgmentTransport | None = None,
    ):
        self.endpoint = _validate_endpoint(endpoint, approved_endpoint)
        if (
            not isinstance(broker_key, str)
            or not broker_key.strip()
            or len(broker_key) > 4096
            or any(c in broker_key for c in "\r\n")
        ):
            raise ValueError("judgment broker credential must be explicitly supplied")
        if (
            type(timeout_seconds) not in (int, float)
            or not 0 < timeout_seconds <= 30
            or not math.isfinite(timeout_seconds)
        ):
            raise ValueError("judgment timeout must be positive and at most 30 seconds")
        self._broker_key = broker_key
        self.timeout_seconds = float(timeout_seconds)
        self.transport = transport or UrllibJudgmentTransport()

    def judge(
        self,
        *,
        state: State,
        questions: dict[str, Any],
        deadline: float | None = None,
    ) -> JudgmentResult:
        """Judge named questions once, accepting results only before ``deadline``.

        ``deadline`` is an absolute ``time.monotonic()`` value shared with the
        caller's request graph. It caps the network timeout and is checked before
        dispatch and after parsing. As with the existing reranker, the underlying
        blocking HTTP transport uses socket timeouts; it cannot cancel DNS or an
        injected transport. This boundary does not silently truncate model input
        or estimate tokens: callers must budget model context before dispatch.
        """
        started = time.monotonic()
        if deadline is not None:
            try:
                valid = type(deadline) in (int, float) and math.isfinite(deadline)
            except OverflowError:
                valid = False
            if not valid:
                raise JudgmentUnavailable("judgment_deadline_exhausted")
        expires = (
            min(started + self.timeout_seconds, deadline)
            if deadline is not None
            else started + self.timeout_seconds
        )
        usage = TokenUsage()
        try:
            if expires <= started:
                raise JudgmentUnavailable("judgment_deadline_exhausted")
            body = prepare_judgment_request(state, questions)
            questions = body["questions"]
            contract = json.dumps(
                {
                    "contract": JUDGMENT_CONTRACT,
                    "model": JEV_MODEL,
                    "questions": questions,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            contract_hash = hashlib.sha256(contract.encode()).hexdigest()
            timeout = expires - time.monotonic()
            if timeout <= 0:
                raise JudgmentUnavailable("judgment_deadline_exhausted")
            try:
                payload = self.transport.post(
                    url=self.endpoint,
                    headers={"Authorization": f"Bearer {self._broker_key}"},
                    body=body,
                    timeout=timeout,
                )
            except JudgmentUnavailable:
                raise
            except Exception:
                raise JudgmentUnavailable("judgment_transport_error") from None
            usage = _usage(payload)
            answers = _answers(payload, questions)
            if time.monotonic() >= expires:
                raise JudgmentUnavailable("judgment_deadline_exhausted")
            return JudgmentResult(
                answers,
                JEV_MODEL,
                usage,
                contract_hash,
                (time.monotonic() - started) * 1000,
            )
        except JudgmentUnavailable as error:
            error.usage = usage
            error.elapsed_ms = (time.monotonic() - started) * 1000
            raise
