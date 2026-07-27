"""Lossless message passages that point into one logical evidence document."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from typing import Iterable, Iterator

import orjson

from .logical_evidence import LogicalEvidenceError, LogicalEvidenceRecord


PASSAGE_CONTRACT = "recall.lossless-message-passage.v2"
PASSAGE_SEPARATOR = "\n"
MAX_PASSAGE_TOKEN_BYTES = 64
VISIBLE_DENSE_ROLES = frozenset({"user", "assistant"})
IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+=-]{0,511}\Z")
LOGICAL_DOCUMENT_ID_RE = re.compile(r"ldoc_[0-9a-f]{32}\Z")
RECEIPT_RE = re.compile(r"recall://[^\s]{1,2040}\Z")
TOKEN_RE = re.compile(r"\S+")


@dataclass(frozen=True)
class PassagePolicy:
    target_tokens: int
    overlap_tokens: int
    contract: str = PASSAGE_CONTRACT

    def __post_init__(self) -> None:
        if (
            self.contract != PASSAGE_CONTRACT
            or isinstance(self.target_tokens, bool)
            or not isinstance(self.target_tokens, int)
            or not 4 <= self.target_tokens <= 8192
            or isinstance(self.overlap_tokens, bool)
            or not isinstance(self.overlap_tokens, int)
            or not 0 <= self.overlap_tokens < self.target_tokens
        ):
            raise ValueError("passage policy requires a smaller valid overlap")

    @property
    def fingerprint(self) -> str:
        value = (
            f"{self.contract}\0{self.target_tokens}\0{self.overlap_tokens}"
        )
        return hashlib.sha256(value.encode()).hexdigest()


@dataclass(frozen=True)
class PassageMessage:
    record_ordinal: int
    occurred_at: str
    roles: tuple[str, ...]
    receipts: tuple[str, ...]
    text: str
    record_count: int = 1

    def validate(self) -> None:
        if (
            isinstance(self.record_ordinal, bool)
            or not isinstance(self.record_ordinal, int)
            or self.record_ordinal < 0
            or not isinstance(self.occurred_at, str)
            or not self.occurred_at
            or not isinstance(self.roles, tuple)
            or not self.roles
            or not set(self.roles) <= VISIBLE_DENSE_ROLES
            or tuple(sorted(set(self.roles))) != self.roles
            or not isinstance(self.receipts, tuple)
            or not self.receipts
            or len(set(self.receipts)) != len(self.receipts)
            or any(
                not isinstance(receipt, str)
                or not RECEIPT_RE.fullmatch(receipt)
                for receipt in self.receipts
            )
            or not isinstance(self.text, str)
            or isinstance(self.record_count, bool)
            or not isinstance(self.record_count, int)
            or self.record_count < 1
        ):
            raise ValueError(
                "dense passage messages require visible user/assistant records"
            )
        try:
            parsed = datetime.fromisoformat(
                self.occurred_at.replace("Z", "+00:00")
            )
        except ValueError:
            raise ValueError("passage message timestamp is invalid") from None
        if parsed.tzinfo is None:
            raise ValueError("passage message timestamp is invalid")


@dataclass(frozen=True)
class PassageSpan:
    message_index: int
    record_ordinal: int
    record_count: int
    source_byte_start: int
    source_byte_end: int
    passage_byte_start: int
    passage_byte_end: int


@dataclass(frozen=True)
class LosslessPassage:
    tenant_id: str
    source_id: str
    logical_document_id: str
    revision: int
    passage_id: str
    ordinal: int
    policy_fingerprint: str
    token_count: int
    first_occurred_at: str
    last_occurred_at: str
    roles: tuple[str, ...]
    receipts: tuple[str, ...]
    text: str
    text_sha256: str
    spans: tuple[PassageSpan, ...]


@dataclass(frozen=True)
class _Token:
    message_index: int
    byte_start: int
    byte_end: int


def _bounded_tokens(
    encoded: bytes,
    *,
    message_index: int,
    start: int,
    end: int,
) -> Iterator[_Token]:
    while start < end:
        bounded_end = min(end, start + MAX_PASSAGE_TOKEN_BYTES)
        while (
            bounded_end < end
            and encoded[bounded_end] & 0b1100_0000 == 0b1000_0000
        ):
            bounded_end -= 1
        if bounded_end <= start:
            raise ValueError("passage token contains invalid UTF-8")
        yield _Token(message_index, start, bounded_end)
        start = bounded_end


def _message_tokens(
    message: PassageMessage,
    message_index: int,
) -> Iterator[_Token]:
    """Partition every message byte into stable word-like token units."""

    text = message.text
    encoded = text.encode()
    if not encoded:
        return
    char_cursor = 0
    byte_cursor = 0
    source_start = 0
    prior_end: int | None = None
    for match in TOKEN_RE.finditer(text):
        byte_cursor += len(text[char_cursor:match.end()].encode())
        if prior_end is not None:
            yield from _bounded_tokens(
                encoded,
                message_index=message_index,
                start=source_start,
                end=prior_end,
            )
            source_start = prior_end
        prior_end = byte_cursor
        char_cursor = match.end()
    yield from _bounded_tokens(
        encoded,
        message_index=message_index,
        start=source_start,
        end=len(encoded),
    )


def _spans(tokens: list[_Token], messages: tuple[PassageMessage, ...]) -> tuple[
    PassageSpan, ...
]:
    grouped: list[tuple[int, int, int]] = []
    for token in tokens:
        if (
            grouped
            and grouped[-1][0] == token.message_index
            and grouped[-1][2] == token.byte_start
        ):
            prior = grouped[-1]
            grouped[-1] = (prior[0], prior[1], token.byte_end)
        else:
            grouped.append(
                (token.message_index, token.byte_start, token.byte_end)
            )
    spans: list[PassageSpan] = []
    passage_offset = 0
    separator_bytes = PASSAGE_SEPARATOR.encode()
    for index, (message_index, source_start, source_end) in enumerate(grouped):
        if index:
            passage_offset += len(separator_bytes)
        span_size = source_end - source_start
        spans.append(
            PassageSpan(
                message_index=message_index,
                record_ordinal=messages[message_index].record_ordinal,
                record_count=messages[message_index].record_count,
                source_byte_start=source_start,
                source_byte_end=source_end,
                passage_byte_start=passage_offset,
                passage_byte_end=passage_offset + span_size,
            )
        )
        passage_offset += span_size
    return tuple(spans)


def reconstruct_passage(
    passage: LosslessPassage,
    messages: tuple[PassageMessage, ...],
) -> str:
    fragments = []
    for span in passage.spans:
        try:
            message = messages[span.message_index]
            if message.record_ordinal != span.record_ordinal:
                raise ValueError("passage record pointer is stale")
            fragment = message.text.encode()[
                span.source_byte_start:span.source_byte_end
            ].decode()
        except (IndexError, UnicodeDecodeError):
            raise ValueError("passage source span is invalid") from None
        fragments.append(fragment)
    value = PASSAGE_SEPARATOR.join(fragments)
    encoded = value.encode()
    for span in passage.spans:
        if not (
            0 <= span.passage_byte_start
            < span.passage_byte_end
            <= len(encoded)
        ):
            raise ValueError("passage output span is invalid")
    return value


def build_passages(
    *,
    tenant_id: str,
    source_id: str,
    logical_document_id: str,
    revision: int,
    messages: tuple[PassageMessage, ...],
    policy: PassagePolicy,
) -> tuple[LosslessPassage, ...]:
    if (
        not isinstance(tenant_id, str)
        or not IDENTITY_RE.fullmatch(tenant_id)
        or not isinstance(source_id, str)
        or not IDENTITY_RE.fullmatch(source_id)
        or not isinstance(logical_document_id, str)
        or not LOGICAL_DOCUMENT_ID_RE.fullmatch(logical_document_id)
        or isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 1
        or not isinstance(messages, tuple)
        or not messages
        or not isinstance(policy, PassagePolicy)
    ):
        raise ValueError("lossless passage document identity is invalid")
    for message in messages:
        if not isinstance(message, PassageMessage):
            raise ValueError("lossless passage message is invalid")
        message.validate()
    if any(
        following.record_ordinal
        < prior.record_ordinal + prior.record_count
        for prior, following in zip(messages, messages[1:])
    ):
        raise ValueError("lossless passage records must be unique and ordered")

    tokens = (
        token
        for index, message in enumerate(messages)
        for token in _message_tokens(message, index)
    )
    passages: list[LosslessPassage] = []
    window = list(islice(tokens, policy.target_tokens))
    while window:
        spans = _spans(window, messages)
        text = PASSAGE_SEPARATOR.join(
            messages[span.message_index].text.encode()[
                span.source_byte_start:span.source_byte_end
            ].decode()
            for span in spans
        )
        text_sha256 = hashlib.sha256(text.encode()).hexdigest()
        occurred = [
            messages[index].occurred_at
            for index in dict.fromkeys(
                span.message_index for span in spans
            )
        ]
        occurred.sort(
            key=lambda value: datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        )
        identity = "\0".join(
            (
                tenant_id,
                source_id,
                logical_document_id,
                str(revision),
                str(len(passages)),
                policy.fingerprint,
                text_sha256,
                *(
                    f"{span.record_ordinal}:{span.record_count}:"
                    f"{span.source_byte_start}:"
                    f"{span.source_byte_end}"
                    for span in spans
                ),
            )
        )
        passages.append(
            LosslessPassage(
                tenant_id=tenant_id,
                source_id=source_id,
                logical_document_id=logical_document_id,
                revision=revision,
                passage_id=(
                    "psg_" + hashlib.sha256(identity.encode()).hexdigest()[:32]
                ),
                ordinal=len(passages),
                policy_fingerprint=policy.fingerprint,
                token_count=len(window),
                first_occurred_at=occurred[0],
                last_occurred_at=occurred[-1],
                roles=tuple(sorted({
                    role
                    for span in spans
                    for role in messages[span.message_index].roles
                })),
                receipts=tuple(dict.fromkeys(
                    receipt
                    for span in spans
                    for receipt in messages[span.message_index].receipts
                )),
                text=text,
                text_sha256=text_sha256,
                spans=spans,
            )
        )
        if len(window) < policy.target_tokens:
            break
        retained = (
            window[-policy.overlap_tokens:]
            if policy.overlap_tokens
            else []
        )
        added = list(islice(
            tokens,
            policy.target_tokens - len(retained),
        ))
        if not added:
            break
        window = retained + added
    return tuple(passages)


def decode_logical_record(
    line: bytes,
    *,
    source_id: str,
) -> LogicalEvidenceRecord:
    """Decode one canonical logical-document JSONL record without normalizing it."""

    try:
        value = orjson.loads(line)
    except orjson.JSONDecodeError as error:
        raise LogicalEvidenceError("passage_logical_record_invalid") from error
    if not isinstance(value, dict):
        raise LogicalEvidenceError("passage_logical_record_invalid")
    base = {
        "event_kind",
        "event_native_id",
        "occurred_at",
        "ordinal",
        "receipts",
        "roles",
        "segment_count",
        "segment_ordinal",
    }
    payload_fields = set(value).intersection(
        {"content", "content_fragment", "text"}
    )
    if set(value) != base | payload_fields or len(payload_fields) != 1:
        raise LogicalEvidenceError("passage_logical_record_invalid")
    if "content" in value:
        text = orjson.dumps(
            value["content"],
            option=orjson.OPT_SORT_KEYS,
        ).decode()
    elif "content_fragment" in value:
        text = value["content_fragment"]
    else:
        text = value["text"]
    try:
        record = LogicalEvidenceRecord(
            ordinal=value["ordinal"],
            event_native_id=value["event_native_id"],
            event_kind=value["event_kind"],
            occurred_at=value["occurred_at"],
            roles=tuple(value["roles"]),
            receipts=tuple(value["receipts"]),
            segment_ordinal=value["segment_ordinal"],
            segment_count=value["segment_count"],
            text=text,
        )
        encoded = record.encode(source_id=source_id)
    except (KeyError, TypeError, ValueError) as error:
        raise LogicalEvidenceError("passage_logical_record_invalid") from error
    if encoded != line:
        raise LogicalEvidenceError("passage_logical_record_not_canonical")
    return record


def visible_messages(
    records: Iterable[LogicalEvidenceRecord],
) -> tuple[PassageMessage, ...]:
    """Combine physical segments into exact visible source messages."""

    values = iter(records)
    messages: list[PassageMessage] = []
    expected_ordinal = 0
    for first in values:
        if first.ordinal != expected_ordinal or first.segment_ordinal != 0:
            raise LogicalEvidenceError("passage_logical_record_order_invalid")
        group = [first]
        for segment_ordinal in range(1, first.segment_count):
            try:
                continuation = next(values)
            except StopIteration:
                raise LogicalEvidenceError(
                    "passage_logical_record_segment_incomplete"
                ) from None
            if (
                continuation.ordinal != first.ordinal + segment_ordinal
                or continuation.event_native_id != first.event_native_id
                or continuation.event_kind != first.event_kind
                or continuation.occurred_at != first.occurred_at
                or continuation.roles != first.roles
                or continuation.receipts
                or continuation.segment_ordinal != segment_ordinal
                or continuation.segment_count != first.segment_count
            ):
                raise LogicalEvidenceError(
                    "passage_logical_record_segment_invalid"
                )
            group.append(continuation)
        expected_ordinal += len(group)
        if (
            not first.receipts
            or not first.roles
            or not set(first.roles) <= VISIBLE_DENSE_ROLES
        ):
            continue
        message = PassageMessage(
            record_ordinal=first.ordinal,
            record_count=len(group),
            occurred_at=first.occurred_at,
            roles=first.roles,
            receipts=first.receipts,
            text="".join(record.text for record in group),
        )
        message.validate()
        messages.append(message)
    return tuple(messages)
