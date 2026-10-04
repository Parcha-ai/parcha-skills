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
PROVENANCE_PASSAGE_CONTRACT = "recall.lossless-message-passage.v5:native-provenance"
PASSAGE_SEPARATOR = "\n"
# H2-a contextual header: embedding input only, never part of the passage
# text, spans, receipts, text_sha256, or passage id.
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


def _text_blocks(value: object) -> tuple[str, ...]:
    """Extract only explicitly visible text blocks from a message value."""

    if isinstance(value, str):
        return (value,) if value.strip() else ()
    if isinstance(value, list):
        return tuple(
            text
            for item in value
            for text in _text_blocks(item)
        )
    if not isinstance(value, dict):
        return ()
    text = value.get("text")
    if isinstance(text, str) and text.strip():
        return (text,)
    content = value.get("content")
    return _text_blocks(content) if content is not None else ()


def visible_message_text(text: str) -> str | None:
    """Project harness envelopes to their exact human-visible message text."""

    try:
        value = orjson.loads(text)
    except orjson.JSONDecodeError:
        return text if text.strip() else None
    if isinstance(value, str):
        return value if value.strip() else None
    if not isinstance(value, dict):
        return None

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
        for block in _text_blocks(candidate)
    ))
    return "\n".join(blocks) if blocks else None


@dataclass(frozen=True)
class PassagePolicy:
    target_tokens: int
    overlap_tokens: int
    contract: str = PASSAGE_CONTRACT

    def __post_init__(self) -> None:
        if (
            self.contract not in {PASSAGE_CONTRACT, PROVENANCE_PASSAGE_CONTRACT}
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

PROVENANCE_PASSAGE_POLICY = PassagePolicy(
    target_tokens=1024, overlap_tokens=128, contract=PROVENANCE_PASSAGE_CONTRACT,
)


@dataclass(frozen=True)
class PassageMessage:
    record_ordinal: int
    occurred_at: str
    roles: tuple[str, ...]
    receipts: tuple[str, ...]
    text: str
    record_count: int = 1
    actor_links: tuple[ActorLink, ...] = ()

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


@dataclass(frozen=True)
class NativeMessageProvenance:
    native_session_id: str | None
    fork_parent_session_id: str | None
    native_message_id: str | None
    record_type: str | None
    serialized_at: str
    visible_text_sha256: str
    # Source serialization time is not proof of original authoring time.
    original_occurred_at: None = None
    contract: str = PROVENANCE_PASSAGE_CONTRACT

    def validate(self) -> None:
        if (
            self.contract != PROVENANCE_PASSAGE_CONTRACT
            or self.original_occurred_at is not None
            or not isinstance(self.visible_text_sha256, str)
            or len(self.visible_text_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.visible_text_sha256)
            or any(identity is not None and _native_id(identity) != identity for identity in (
                self.native_session_id, self.fork_parent_session_id,
                self.native_message_id, self.record_type,
            ))
            or (self.fork_parent_session_id is not None and (
                self.native_session_id is None or self.fork_parent_session_id == self.native_session_id
            ))
            or not isinstance(self.serialized_at, str)
            or len(self.serialized_at) > 64
        ):
            raise ValueError("passage native provenance is invalid")
        try:
            parsed = datetime.fromisoformat(self.serialized_at.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("passage native provenance time is invalid") from None
        if parsed.tzinfo is None:
            raise ValueError("passage native provenance time is invalid")


@dataclass(frozen=True, kw_only=True)
class ProvenancePassageMessage(PassageMessage):
    provenance: NativeMessageProvenance

    def validate(self) -> None:
        super().validate()
        value = self.provenance
        if not isinstance(value, NativeMessageProvenance):
            raise ValueError("passage native provenance is invalid")
        value.validate()
        if (value.visible_text_sha256 != hashlib.sha256(self.text.encode()).hexdigest()
                or value.serialized_at != self.occurred_at):
            raise ValueError("passage native provenance does not match message")


@dataclass(frozen=True, kw_only=True)
class ProvenancePassageSpan(PassageSpan):
    provenance: NativeMessageProvenance


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
        message = messages[message_index]
        span_class = ProvenancePassageSpan if isinstance(message, ProvenancePassageMessage) else PassageSpan
        provenance = {"provenance": message.provenance} if isinstance(message, ProvenancePassageMessage) else {}
        spans.append(
            span_class(
                message_index=message_index,
                record_ordinal=messages[message_index].record_ordinal,
                record_count=messages[message_index].record_count,
                source_byte_start=source_start,
                source_byte_end=source_end,
                passage_byte_start=passage_offset,
                passage_byte_end=passage_offset + span_size,
                **provenance,
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


def canonical_spans_json(spans: tuple[PassageSpan, ...]) -> str:
    """Canonical JSON for a passage's record windows (sorted keys, no spaces)."""

    return json.dumps(
        [asdict(span) for span in spans],
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
            canonical_spans_json(spans),
        )
    )
    return "psg_" + hashlib.sha256(identity.encode()).hexdigest()[:32]


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
        if isinstance(message, ProvenancePassageMessage) != (policy.contract == PROVENANCE_PASSAGE_CONTRACT):
            raise ValueError("passage provenance requires its explicit policy")
    if any(
        following.record_ordinal
        < prior.record_ordinal + prior.record_count
        for prior, following in zip(messages, messages[1:])
    ):
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
        spans = _spans(window, messages)
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


def _typed_communication_message(event_kind: str, text: str) -> bool:
    """Typed provider messages are visible even when their person is unknown."""

    if event_kind != "connector_record":
        return False
    try:
        content = orjson.loads(text)
    except orjson.JSONDecodeError:
        return False
    return (
        isinstance(content, dict)
        and content.get("kind") == "communication_message.v1"
        and isinstance(content.get("text"), str)
        and bool(content["text"].strip())
    )


def _without_hidden(value: object) -> object:
    """Exclude explicit hidden structures without truncating visible nesting."""
    result: list[object] = [None]
    pending = [(result, 0, value)]
    while pending:
        parent, key, item = pending.pop()
        if isinstance(item, dict):
            if (item.get("type") in ("reasoning", "agent_reasoning", "thinking", "redacted_thinking")
                    or item.get("channel") == "analysis"):
                parent[key] = None
                continue
            copied = {}
            parent[key] = copied
            pending.extend((copied, name, child) for name, child in item.items())
        elif isinstance(item, list):
            copied_list = [None] * len(item)
            parent[key] = copied_list
            pending.extend((copied_list, index, child) for index, child in enumerate(item))
        else:
            parent[key] = item
    return result[0]


def _native_id(value: object) -> str | None:
    return value if isinstance(value, str) and 0 < len(value) <= 512 and IDENTITY_RE.fullmatch(value) else None


def _native_content(text: str) -> dict:
    try:
        value = orjson.loads(text)
    except orjson.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _native_session(content: dict) -> tuple[str | None, str | None]:
    """Only the document's first explicit session header establishes lineage."""
    payload = content.get("payload")
    if content.get("type") != "session_meta" or not isinstance(payload, dict):
        return None, None
    session_id = _native_id(payload.get("id"))
    source = payload.get("source")
    subagent = source.get("subagent") if isinstance(source, dict) else None
    spawn = subagent.get("thread_spawn") if isinstance(subagent, dict) else None
    parents = [payload[key] for key in ("forked_from_id", "parent_thread_id") if key in payload]
    if isinstance(spawn, dict) and "parent_thread_id" in spawn:
        parents.append(spawn["parent_thread_id"])
    valid = [_native_id(value) for value in parents]
    parent = valid[0] if valid and all(value == valid[0] for value in valid) else None
    if not session_id or parent == session_id:
        parent = None
    return session_id, parent


def visible_messages(
    records: Iterable[LogicalEvidenceRecord],
    *,
    policy: PassagePolicy = DEFAULT_PASSAGE_POLICY,
) -> tuple[PassageMessage, ...]:
    """Combine physical segments into exact visible source messages."""

    if not isinstance(policy, PassagePolicy):
        raise ValueError("passage policy is invalid")
    with_provenance = policy.contract == PROVENANCE_PASSAGE_CONTRACT
    native_session_id = fork_parent_id = None
    lineage_conflict = False
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
        source_text = "".join(record.text for record in group)
        content = _native_content(source_text) if with_provenance else {}
        if with_provenance and content.get("type") == "session_meta":
            identity = _native_session(content)
            if first.ordinal == 0:
                native_session_id, fork_parent_id = identity
            elif identity != (native_session_id, fork_parent_id):
                lineage_conflict = True
        payload = content.get("payload")
        if not isinstance(payload, dict):
            payload = {}
        dense_roles = first.roles
        if not dense_roles and (
            any(link.relation == "author" for link in first.actor_links)
            or _typed_communication_message(first.event_kind, source_text)
        ):
            # Visibility and person resolution are separate: typed provider
            # messages remain searchable without inventing an actor link.
            dense_roles = ("user",)
        if (
            not first.receipts
            or not dense_roles
            or not set(dense_roles) <= VISIBLE_DENSE_ROLES
        ):
            continue
        visible_source = (
            json.dumps(_without_hidden(content), ensure_ascii=False)
            if with_provenance and content else source_text
        )
        text = visible_message_text(visible_source)
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
        )
        if with_provenance:
            message = ProvenancePassageMessage(
                **{field: getattr(message, field) for field in PassageMessage.__dataclass_fields__},
                provenance=NativeMessageProvenance(
                    native_session_id=native_session_id,
                    fork_parent_session_id=fork_parent_id,
                    native_message_id=(
                        _native_id(payload.get("id"))
                        if content.get("type") == "response_item" and payload.get("type") == "message"
                        else None
                    ),
                    record_type=_native_id(content.get("type")),
                    serialized_at=first.occurred_at,
                    visible_text_sha256=hashlib.sha256(text.encode()).hexdigest(),
                ),
            )
        message.validate()
        messages.append(message)
    if lineage_conflict:
        messages = [
            replace(message, provenance=replace(
                message.provenance, native_session_id=None, fork_parent_session_id=None,
            ))
            for message in messages
        ]
    return tuple(messages)
