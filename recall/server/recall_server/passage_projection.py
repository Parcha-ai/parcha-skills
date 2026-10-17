"""Lossless message passages that point into one logical evidence document."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from itertools import islice
from typing import Iterable, Iterator

import orjson

from .actor_attribution import ActorLink, actor_links
from .logical_evidence import LogicalEvidenceError, LogicalEvidenceRecord
from .passage_representations import DocumentContext, _metadata_lines
from .semantic import PASSAGE_HEADER_CONTRACT

__all__ = [
    "PASSAGE_HEADER_CONTRACT",
]


PASSAGE_CONTRACT = "recall.lossless-message-passage.v4:actor-aware"
PASSAGE_SEPARATOR = "\n"
# H2-a contextual header: embedding input only, never part of the passage
# text, spans, receipts, text_sha256, or passage id.
COMPLETE_REPORT_UNIT = "subagent_handback.complete.v1"
COMPLETE_ASSISTANT_FINAL_UNIT = "assistant_final.complete.v1"
COMPLETE_SOURCE_UNITS = (COMPLETE_REPORT_UNIT, COMPLETE_ASSISTANT_FINAL_UNIT)
MAX_PASSAGE_HEADER_BYTES = 512
PASSAGE_HEADER_OPEN = "[context]"
PASSAGE_HEADER_CLOSE = "[passage]"
PASSAGE_EMBEDDING_SEPARATOR = "\n\n"
# Template order. ``_metadata_lines`` renders document start/end; the header
# reports the passage's own first/last time (turn-level signal).
PASSAGE_HEADER_FIELDS = (
    ("source family", "source family"),
    ("source aliases", "source aliases"),
    ("harness", "harness"),
    ("workspace", "workspace"),
    ("branch", "branch"),
    ("people", "people"),
    ("document start", "passage start"),
    ("document end", "passage end"),
)
# When the byte budget is exceeded after people were trimmed, drop lines in
# this order; source family and the passage times are kept to the end.
PASSAGE_HEADER_DROP_ORDER = (
    "source aliases",
    "people",
    "branch",
    "workspace",
    "harness",
)
MAX_PASSAGE_TOKEN_BYTES = 64
VISIBLE_DENSE_ROLES = frozenset({"user", "assistant"})
IDENTITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+=-]{0,511}\Z")
LOGICAL_DOCUMENT_ID_RE = re.compile(r"ldoc_[0-9a-f]{32}\Z")
RECEIPT_RE = re.compile(r"recall://[^\s]{1,2040}\Z")
TOKEN_RE = re.compile(r"\S+")


def _text_blocks(
    value: object, *, include_tool_results: bool = True,
) -> tuple[str, ...]:
    """Extract message text, optionally omitting typed tool-result blocks."""

    if isinstance(value, str):
        return (value,) if value.strip() else ()
    if isinstance(value, list):
        return tuple(
            text
            for item in value
            for text in _text_blocks(item, include_tool_results=include_tool_results)
        )
    if not isinstance(value, dict):
        return ()
    if not include_tool_results and value.get("type") == "tool_result":
        return ()
    text = value.get("text")
    if isinstance(text, str) and text.strip():
        return (text,)
    content = value.get("content")
    return (
        _text_blocks(content, include_tool_results=include_tool_results)
        if content is not None else ()
    )


def _visible_message(
    text: str, *, include_tool_results: bool = False,
) -> tuple[str | None, str | None]:
    """Project harness envelopes to their exact human-visible message text."""

    try:
        value = orjson.loads(text)
    except orjson.JSONDecodeError:
        return (text if text.strip() else None), None
    if isinstance(value, str):
        return (value if value.strip() else None), None
    if not isinstance(value, dict):
        return None, None

    candidates: list[object] = []
    payload = value.get("payload")
    if isinstance(payload, dict):
        candidates.extend((payload.get("message"), payload.get("content")))
    message = value.get("message")
    if isinstance(message, dict):
        candidates.append(message.get("content"))
    else:
        candidates.append(message)
    candidates.extend((value.get("content"), value.get("text")))
    blocks = tuple(dict.fromkeys(
        block
        for candidate in candidates
        if candidate is not None
        for block in _text_blocks(candidate, include_tool_results=include_tool_results)
    ))
    if blocks:
        # Provider-declared final text, not an inferred summary or a vendor hint.
        # Refuse mixed block types and competing envelope text: only the exact
        # complete visible payload can receive the producer-owned marker.
        final = (
            value.get("type") == "response_item"
            and isinstance(payload, dict)
            and payload.get("type") == "message"
            and payload.get("role") == "assistant"
            and payload.get("phase") == "final_answer"
            and isinstance(payload.get("content"), list)
            and bool(payload["content"])
            and all(isinstance(block, dict) and block.get("type") == "output_text"
                    and isinstance(block.get("text"), str) for block in payload["content"])
            and tuple(dict.fromkeys(_text_blocks(payload["content"]))) == blocks
        )
        return "\n".join(blocks), COMPLETE_ASSISTANT_FINAL_UNIT if final else None
    # Claude can deliver its final report through this named tool instead of a
    # text block. Keep existing visible projections (and their offsets) intact.
    if (
        value.get("type") != "assistant"
        or not isinstance(message, dict)
        or message.get("role") != "assistant"
        or not isinstance(message.get("content"), list)
    ):
        return None, None
    reports = []
    for block in message["content"]:
        if (
            not isinstance(block, dict)
            or block.get("type") != "tool_use"
            or block.get("name") != "SubagentHandback"
        ):
            continue
        report = block.get("input")
        if (
            isinstance(report, dict)
            and set(report) == {"message"}
            and isinstance(report["message"], str)
            and report["message"].strip()
        ):
            reports.append(report["message"])
    return ("\n".join(dict.fromkeys(reports)), COMPLETE_REPORT_UNIT) if reports else (None, None)


def visible_message_text(text: str) -> str | None:
    """Keep historical extraction semantics for previously published spans."""
    return _visible_message(text, include_tool_results=True)[0]


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


# One authoritative pointer policy for both projection and retrieval. Shadow
# policies remain explicit at their call sites and never become production by
# accident.
DEFAULT_PASSAGE_POLICY = PassagePolicy(
    target_tokens=1024,
    overlap_tokens=128,
)


def _tool_unit(source_unit: str | None) -> bool:
    return isinstance(source_unit, str) and source_unit.startswith((
        "claude.tool-input.string.v1#/message/content/",
        "claude.tool-result.string.v1#/message/content/",
    ))


def _ordered_messages(messages: tuple["PassageMessage", ...]) -> bool:
    for prior, following in zip(messages, messages[1:]):
        if following.record_ordinal >= prior.record_ordinal + prior.record_count:
            continue
        # One explicitly resolved provider block can contain multiple leaf spans.
        if not (_tool_unit(prior.source_unit) and _tool_unit(following.source_unit)
                and prior.source_unit != following.source_unit
                and prior.origin == following.origin
                and (prior.record_ordinal, prior.record_count, prior.occurred_at,
                     prior.roles, prior.receipts) ==
                    (following.record_ordinal, following.record_count,
                     following.occurred_at, following.roles, following.receipts)):
            return False
    identities = [(m.record_ordinal, m.source_unit) for m in messages]
    return len(identities) == len(set(identities))


@dataclass(frozen=True)
class PassageMessage:
    record_ordinal: int
    occurred_at: str
    roles: tuple[str, ...]
    receipts: tuple[str, ...]
    text: str
    record_count: int = 1
    actor_links: tuple[ActorLink, ...] = ()
    source_unit: str | None = None
    origin: dict[str, str] | None = None

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
            or (self.source_unit is not None and self.source_unit not in COMPLETE_SOURCE_UNITS
                and not _tool_unit(self.source_unit))
            or actor_links(self.actor_links) != self.actor_links
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
    source_unit: str | None = None
    origin: dict[str, str] | None = None


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
    actor_links: tuple[ActorLink, ...] = ()


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
    text: str,
    encoded: bytes,
    message_index: int,
) -> Iterator[_Token]:
    """Partition every message byte into stable word-like token units."""

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
                source_unit=(messages[message_index].source_unit
                             if _tool_unit(messages[message_index].source_unit) else None),
                origin=messages[message_index].origin,
            )
        )
        passage_offset += span_size
    return tuple(spans)


def _reader_span_windows(tokens: list[_Token], messages: tuple[PassageMessage, ...]):
    """Split a tool window to the existing capture reader's128-span capacity.

    See evals/candidate_capture.py capture_metadata_bound. This does not change
    token overlap or discard leaves: each ordinary window is partitioned after
    tokenization. Legacy windows retain their byte-identical geometry.
    """
    if not any(_tool_unit(messages[t.message_index].source_unit) for t in tokens):
        yield tokens
        return
    start = 0
    spans = 0
    prior = None
    for index, token in enumerate(tokens):
        if prior is None or token.message_index != prior.message_index or token.byte_start != prior.byte_end:
            if spans == 128:
                yield tokens[start:index]
                start, spans = index, 0
            spans += 1
        prior = token
    if start < len(tokens):
        yield tokens[start:]


def reconstruct_passage(
    passage: LosslessPassage,
    messages: tuple[PassageMessage, ...],
) -> str:
    fragments = []
    for span in passage.spans:
        try:
            message = messages[span.message_index]
            if (message.record_ordinal != span.record_ordinal
                    or (_tool_unit(span.source_unit) and message.source_unit != span.source_unit)):
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


def _header_time(value: object) -> str | None:
    """Canonical UTC second-precision timestamp for header lines."""

    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        rendered = str(value).strip()
        if not rendered:
            return None
        try:
            parsed = datetime.fromisoformat(rendered.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _header_lines(context: DocumentContext, actor_count: int) -> dict[str, str]:
    actors = tuple(
        sorted(
            context.actors,
            key=lambda value: (value.display_name.casefold(), value.actor_id),
        )[:actor_count]
    )
    rendered: dict[str, str] = {}
    for line in _metadata_lines(replace(context, actors=actors)):
        label, _, value = line.partition(": ")
        rendered[label] = value
    return {
        header_label: rendered[source_label]
        for source_label, header_label in PASSAGE_HEADER_FIELDS
        if source_label in rendered
    }


def _render_header(lines: dict[str, str]) -> str:
    body = [f"{label}: {value}" for label, value in lines.items()]
    return "\n".join((PASSAGE_HEADER_OPEN, *body, PASSAGE_HEADER_CLOSE))


def render_passage_header(
    context: DocumentContext,
    *,
    first_occurred_at: object,
    last_occurred_at: object,
) -> str:
    """Deterministic contextual header for one passage, at most 512 bytes.

    Rendered from catalog fields only (``_metadata_lines``: bounded source
    family, aliases, harness, workspace basename, branch, people with
    relations) plus the passage's own first/last time. Stable ordering; the
    same inputs always give the same bytes. To fit the budget the people
    list is shortened first, then whole lines are dropped in
    ``PASSAGE_HEADER_DROP_ORDER``; the result is finally cut on a UTF-8
    boundary as a hard guarantee.
    """

    if not isinstance(context, DocumentContext):
        raise ValueError("passage header context is invalid")
    timed = replace(
        context,
        first_occurred_at=_header_time(first_occurred_at),
        last_occurred_at=_header_time(last_occurred_at),
    )
    actor_count = len(timed.actors)
    lines = _header_lines(timed, actor_count)
    header = _render_header(lines)
    while len(header.encode()) > MAX_PASSAGE_HEADER_BYTES and actor_count > 0:
        actor_count -= 1
        lines = _header_lines(timed, actor_count)
        header = _render_header(lines)
    for label in PASSAGE_HEADER_DROP_ORDER:
        if len(header.encode()) <= MAX_PASSAGE_HEADER_BYTES:
            break
        if label in lines:
            del lines[label]
            header = _render_header(lines)
    encoded = header.encode()
    if len(encoded) > MAX_PASSAGE_HEADER_BYTES:
        header = encoded[:MAX_PASSAGE_HEADER_BYTES].decode(errors="ignore")
    return header


def passage_embedding_input(header: str | None, text: str) -> str:
    """The exact string embedded under contract v2 (v1 when header is None)."""

    if header is None:
        return text
    return header + PASSAGE_EMBEDDING_SEPARATOR + text


def passage_embed_sha256(header: str, text: str) -> str:
    """sha256 of the v2 embedding input: the embedding reuse key."""

    if not isinstance(header, str) or not header:
        raise ValueError("passage header is required for embed_sha256")
    return hashlib.sha256(
        passage_embedding_input(header, text).encode()
    ).hexdigest()


def canonical_spans_json(spans: tuple[PassageSpan, ...], *, include_origin: bool = True) -> str:
    """Canonical JSON for a passage's record windows (sorted keys, no spaces)."""

    return json.dumps(
        [{key: value for key, value in asdict(span).items()
          if (key not in {"source_unit", "origin"} or value is not None)
          and (include_origin or key != "origin")} for span in spans],
        sort_keys=True,
        separators=(",", ":"),
    )


def passage_identity(
    *,
    tenant_id: str,
    source_id: str,
    logical_document_id: str,
    policy_fingerprint: str,
    text_sha256: str,
    spans: tuple[PassageSpan, ...],
) -> str:
    """Stable passage id: a record window keeps its id across later appends.

    The identity deliberately excludes the document revision and the passage
    count/ordinal. ``build_passages`` windows tokens from record 0 with a fixed
    policy, so appending records only changes the final (short) window; every
    earlier window has byte-identical text and spans and therefore the same id.
    A mid-document edit reflows every window after the edit, which changes
    their spans and so their ids: those rows are replaced, the prefix is kept.
    """

    identity = "\0".join(
        (
            tenant_id,
            source_id,
            logical_document_id,
            policy_fingerprint,
            text_sha256,
            # Origin is an authenticated catalog annotation, not window
            # geometry. Enriching it must not re-key or re-embed old passages.
            canonical_spans_json(spans, include_origin=False),
        )
    )
    return "psg_" + hashlib.sha256(identity.encode()).hexdigest()[:32]


def build_passages(
    *, tenant_id: str, source_id: str, logical_document_id: str,
    revision: int, messages: tuple[PassageMessage, ...], policy: PassagePolicy,
) -> tuple[LosslessPassage, ...]:
    """Keep typed report boundaries while retaining ordinary token windows."""
    arguments = dict(tenant_id=tenant_id, source_id=source_id,
                     logical_document_id=logical_document_id, revision=revision,
                     policy=policy)
    if not isinstance(messages, tuple) or not all(isinstance(m, PassageMessage) for m in messages):
        return _build_passages(messages=messages, **arguments)
    reports = [i for i, message in enumerate(messages) if message.source_unit in COMPLETE_SOURCE_UNITS]
    if not reports:
        return _build_passages(messages=messages, **arguments)
    if not _ordered_messages(messages):
        raise ValueError("lossless passage records must be unique and ordered")
    groups = []
    start = 0
    for index in reports:
        if start < index:
            groups.append((start, index, False))
        groups.append((index, index + 1, True))
        start = index + 1
    if start < len(messages):
        groups.append((start, len(messages), False))
    output = []
    for start, end, report in groups:
        built = _build_passages(messages=messages[start:end], **arguments)
        for passage in built:
            complete = (report and len(built) == 1
                        and passage.token_count <= DEFAULT_PASSAGE_POLICY.target_tokens
                        and passage.text == messages[start].text)
            spans = tuple(replace(span, message_index=span.message_index + start,
                                  source_unit=messages[start].source_unit if complete else span.source_unit)
                          for span in passage.spans)
            identity = passage_identity(tenant_id=tenant_id, source_id=source_id,
                logical_document_id=logical_document_id, policy_fingerprint=policy.fingerprint,
                text_sha256=passage.text_sha256, spans=spans)
            output.append(replace(passage, passage_id=identity, ordinal=len(output), spans=spans))
    return tuple(output)


def _build_passages(
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
    if not _ordered_messages(messages):
        raise ValueError("lossless passage records must be unique and ordered")

    encoded_messages = tuple(message.text.encode() for message in messages)
    tokens = (
        token
        for index, (message, encoded) in enumerate(
            zip(messages, encoded_messages, strict=True)
        )
        for token in _message_tokens(message.text, encoded, index)
    )
    passages: list[LosslessPassage] = []
    window = list(islice(tokens, policy.target_tokens))
    while window:
        for reader_window in _reader_span_windows(window, messages):
            spans = _spans(reader_window, messages)
            text = PASSAGE_SEPARATOR.join(
                encoded_messages[span.message_index][
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
            passages.append(
                LosslessPassage(
                    tenant_id=tenant_id,
                    source_id=source_id,
                    logical_document_id=logical_document_id,
                    revision=revision,
                    passage_id=passage_identity(
                        tenant_id=tenant_id,
                        source_id=source_id,
                        logical_document_id=logical_document_id,
                        policy_fingerprint=policy.fingerprint,
                        text_sha256=text_sha256,
                        spans=spans,
                    ),
                    ordinal=len(passages),
                    policy_fingerprint=policy.fingerprint,
                    token_count=len(reader_window),
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
                    actor_links=actor_links(
                        link
                        for span in spans
                        for link in messages[span.message_index].actor_links
                    ),
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
    verify_canonical: bool = True,
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
    optional = {"actor_links"} if "actor_links" in value else set()
    payload_fields = set(value).intersection(
        {"content", "content_fragment", "text"}
    )
    if set(value) != base | optional | payload_fields or len(payload_fields) != 1:
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
            actor_links=actor_links(value.get("actor_links", ())),
        )
        if verify_canonical:
            encoded = record.encode(source_id=source_id)
        else:
            record.validate(source_id=source_id)
    except (KeyError, TypeError, ValueError) as error:
        raise LogicalEvidenceError("passage_logical_record_invalid") from error
    if verify_canonical and encoded != line:
        raise LogicalEvidenceError("passage_logical_record_not_canonical")
    return record


def _typed_provider_text(event_kind: str, text: str) -> bool:
    """Typed messages and extracted documents do not require a conversational role."""

    if event_kind != "connector_record":
        return False
    try:
        content = orjson.loads(text)
    except orjson.JSONDecodeError:
        return False
    return (
        isinstance(content, dict)
        and content.get("kind") in ("communication_message.v1", "document.v1")
        and isinstance(content.get("text"), str)
        and bool(content["text"].strip())
    )


def _message_origin(source_text: str, visible_text: str) -> dict[str, str] | None:
    """Keep explicit Codex message identity; ordinary pasted text has no origin."""
    try:
        value = orjson.loads(source_text)
    except orjson.JSONDecodeError:
        return None
    if not isinstance(value, dict) or value.get("type") != "response_item":
        return None
    payload = value.get("payload")
    if (not isinstance(payload, dict) or payload.get("type") != "message"
            or not isinstance(payload.get("role"), str)
            or payload["role"] not in VISIBLE_DENSE_ROLES):
        return None
    metadata = payload.get("internal_chat_message_metadata_passthrough")
    if not isinstance(metadata, dict):
        return None
    message_id, turn_id = payload.get("id"), metadata.get("turn_id")
    if not all(isinstance(item, str) and item.strip() for item in (message_id, turn_id)):
        return None
    payload_text = "\n".join(dict.fromkeys(_text_blocks(payload.get("content"), include_tool_results=False)))
    if payload_text != visible_text:
        return None
    return {"message_id": message_id, "turn_id": turn_id, "role": payload["role"],
            "text_sha256": hashlib.sha256(visible_text.encode()).hexdigest()}


def _logical_record_groups(
    records: Iterable[LogicalEvidenceRecord], *, first_record_ordinal: int = 0,
) -> Iterator[tuple[LogicalEvidenceRecord, ...]]:
    """Validate and reassemble the original logical record sequence."""

    values = iter(records)
    if type(first_record_ordinal) is not int or first_record_ordinal < 0:
        raise LogicalEvidenceError("passage_logical_record_order_invalid")
    expected_ordinal = first_record_ordinal
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
                or continuation.actor_links != first.actor_links
                or continuation.receipts
                or continuation.segment_ordinal != segment_ordinal
                or continuation.segment_count != first.segment_count
            ):
                raise LogicalEvidenceError(
                    "passage_logical_record_segment_invalid"
                )
            group.append(continuation)
        expected_ordinal += len(group)
        yield tuple(group)


def visible_messages(
    records: Iterable[LogicalEvidenceRecord], *, first_record_ordinal: int = 0,
) -> tuple[PassageMessage, ...]:
    """Combine physical segments into exact visible source messages."""
    messages: list[PassageMessage] = []
    for group in _logical_record_groups(records, first_record_ordinal=first_record_ordinal):
        first = group[0]
        source_text = "".join(record.text for record in group)
        dense_roles = first.roles
        if not dense_roles and (
            any(link.relation == "author" for link in first.actor_links)
            or _typed_provider_text(first.event_kind, source_text)
        ):
            # Visibility and person resolution are separate: typed provider
            # content remains searchable without inventing an actor link.
            dense_roles = ("user",)
        if (
            not first.receipts
            or not dense_roles
            or not set(dense_roles) <= VISIBLE_DENSE_ROLES
        ):
            continue
        # Claude wraps tool results in user-role envelopes. Keep those bytes
        # in captured evidence, but do not project them as conversational text.
        text, source_unit = _visible_message(source_text)
        if source_unit == COMPLETE_ASSISTANT_FINAL_UNIT and first.roles != ("assistant",):
            source_unit = None
        if text is None:
            continue
        message = PassageMessage(
            record_ordinal=first.ordinal,
            record_count=len(group),
            occurred_at=first.occurred_at,
            roles=dense_roles,
            receipts=first.receipts,
            text=text,
            actor_links=first.actor_links,
            source_unit=source_unit,
            origin=_message_origin(source_text, text),
        )
        message.validate()
        messages.append(message)
    return tuple(messages)
