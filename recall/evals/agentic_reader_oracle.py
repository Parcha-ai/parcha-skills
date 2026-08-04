"""Evaluation-only gold-document oracle for the reader/exec rung.

The adapter removes query planning, retrieval, and admission from an agent
evaluation.  It exposes only caller-supplied, authorized document identities
as ordinary hint results and delegates full-document execution to the same
tenant-bound retrieval object used by the product.

Truth facts, gold receipts, source bodies, and matched passage windows are not
part of this contract.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from .retrieval import EvaluationInputError


DOCUMENT_ID_RE = re.compile(r"ldoc_[0-9a-f]{32}\Z")
AUTHORITY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._/@+=-]{0,511}\Z")
ORACLE_DOCUMENT_FIELDS = {
    "source_id",
    "logical_document_id",
    "revision",
    "first_occurred_at",
    "last_occurred_at",
}
MAX_ORACLE_DOCUMENTS = 20


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise EvaluationInputError("reader-oracle timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise EvaluationInputError(
            "reader-oracle timestamp is invalid"
        ) from error
    if parsed.utcoffset() is None:
        raise EvaluationInputError("reader-oracle timestamp is invalid")
    return parsed


def validate_oracle_documents(
    value: Any,
    *,
    authorized_sources: tuple[str, ...],
) -> tuple[dict[str, Any], ...]:
    """Validate the closed, non-evidentiary document identity boundary."""

    if (
        not isinstance(authorized_sources, tuple)
        or not authorized_sources
        or len(set(authorized_sources)) != len(authorized_sources)
        or any(
            not isinstance(source, str)
            or AUTHORITY_RE.fullmatch(source) is None
            for source in authorized_sources
        )
        or not isinstance(value, list)
        or not 1 <= len(value) <= MAX_ORACLE_DOCUMENTS
    ):
        raise EvaluationInputError("reader-oracle documents are invalid")
    granted = set(authorized_sources)
    validated: list[dict[str, Any]] = []
    identities: set[tuple[str, str]] = set()
    for document in value:
        if (
            not isinstance(document, dict)
            or set(document) != ORACLE_DOCUMENT_FIELDS
            or not isinstance(document["source_id"], str)
            or document["source_id"] not in granted
            or not isinstance(document["logical_document_id"], str)
            or DOCUMENT_ID_RE.fullmatch(
                document["logical_document_id"]
            )
            is None
            or isinstance(document["revision"], bool)
            or not isinstance(document["revision"], int)
            or document["revision"] < 1
        ):
            raise EvaluationInputError("reader-oracle document is invalid")
        first = _timestamp(document["first_occurred_at"])
        last = _timestamp(document["last_occurred_at"])
        if first > last:
            raise EvaluationInputError("reader-oracle document is invalid")
        identity = (
            document["source_id"],
            document["logical_document_id"],
        )
        if identity in identities:
            raise EvaluationInputError(
                "reader-oracle documents are duplicated"
            )
        identities.add(identity)
        validated.append(dict(document))
    return tuple(validated)


class OracleDocumentRetrieval:
    """Expose fixed document identities while preserving real exec authority."""

    def __init__(
        self,
        inner: Any,
        *,
        documents: list[dict[str, Any]],
        authorized_sources: tuple[str, ...],
        include_live_pointers: bool = False,
    ) -> None:
        if not callable(getattr(inner, "execute_agent_program", None)):
            raise EvaluationInputError(
                "reader-oracle execution boundary is invalid"
            )
        self._inner = inner
        self._documents = validate_oracle_documents(
            documents,
            authorized_sources=authorized_sources,
        )
        self._document_ids = {
            document["logical_document_id"]
            for document in self._documents
        }
        if not isinstance(include_live_pointers, bool):
            raise EvaluationInputError(
                "reader-oracle pointer mode is invalid"
            )
        self._include_live_pointers = include_live_pointers

    def passage_hints(
        self,
        needs: str | list[dict[str, Any]],
        *,
        filters: dict[str, Any],
        limit: int,
    ) -> dict[str, Any]:
        """Return fixed document identities without evidence or passage hints."""

        if isinstance(needs, str):
            needs = [{"need": "verbatim question", "queries": [needs]}]
        if (
            not isinstance(needs, list)
            or not 1 <= len(needs) <= 5
            or any(
                not isinstance(need, dict)
                or set(need) != {"need", "queries"}
                or not isinstance(need["need"], str)
                or not need["need"].strip()
                or not isinstance(need["queries"], list)
                or not 1 <= len(need["queries"]) <= 2
                or any(
                    not isinstance(query, str)
                    or not query.strip()
                    or len(query) > 2048
                    for query in need["queries"]
                )
                for need in needs
            )
            or not isinstance(filters, dict)
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or not len(needs) <= limit <= 20
        ):
            raise EvaluationInputError(
                "reader-oracle hint request is invalid"
            )
        ranges_by_document: dict[str, list[dict[str, Any]]] = {}
        pointer_diagnostics: dict[str, Any] = {}
        if self._include_live_pointers:
            pointer_hints = getattr(self._inner, "passage_hints", None)
            if not callable(pointer_hints):
                raise EvaluationInputError(
                    "reader-oracle pointer boundary is invalid"
                )
            live = pointer_hints(needs, filters=filters, limit=20)
            if not isinstance(live, dict):
                raise EvaluationInputError(
                    "reader-oracle pointer result is invalid"
                )
            pointer_diagnostics = (
                dict(live.get("diagnostics", {}))
                if isinstance(live.get("diagnostics"), dict)
                else {}
            )
            for candidate in live.get("results", []):
                if not isinstance(candidate, dict):
                    continue
                document_id = candidate.get("logical_document_id")
                ranges = candidate.get("matching_ranges")
                if (
                    document_id in self._document_ids
                    and isinstance(ranges, list)
                ):
                    ranges_by_document[document_id] = [
                        dict(item)
                        for item in ranges
                        if isinstance(item, dict)
                    ]
        return {
            "results": [
                {
                    **document,
                    "rank": round(1.0 / ordinal, 8),
                    "matching_ranges": ranges_by_document.get(
                        document["logical_document_id"],
                        [],
                    ),
                }
                for ordinal, document in enumerate(
                    self._documents[:limit],
                    start=1,
                )
            ],
            "diagnostics": {
                "engine": (
                    "reader-oracle-soft-pointers-v1"
                    if self._include_live_pointers
                    else "reader-oracle-full-document-v1"
                ),
                "pointer_engine": pointer_diagnostics.get("engine"),
                "dense_status": pointer_diagnostics.get("dense_status"),
                "passage_lexical_status": pointer_diagnostics.get(
                    "passage_lexical_status"
                ),
                "sparse_status": pointer_diagnostics.get("sparse_status"),
            },
        }

    def execute_agent_program(
        self,
        program: str,
        *,
        logical_document_ids: tuple[str, ...],
        record_spans: dict[str, tuple[tuple[int, int], ...]],
        routing_receipts: dict[str, tuple[str, ...]],
        timeout_seconds: int,
        document_aliases: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Delegate execution only when every requested document was supplied."""

        if (
            not isinstance(logical_document_ids, tuple)
            or not logical_document_ids
            or not set(logical_document_ids) <= self._document_ids
        ):
            raise EvaluationInputError(
                "reader-oracle execution escaped supplied documents"
            )
        return self._inner.execute_agent_program(
            program,
            logical_document_ids=logical_document_ids,
            record_spans=record_spans,
            routing_receipts=routing_receipts,
            timeout_seconds=timeout_seconds,
            document_aliases=document_aliases,
        )

    def find_documents(
        self,
        *,
        logical_document_ids: tuple[str, ...],
        document_aliases: dict[str, str],
        patterns: tuple[str, ...],
        context_chars: int,
        limit: int,
        record_spans: dict[str, tuple[tuple[int, int], ...]],
        routing_receipts: dict[str, tuple[str, ...]],
        timeout_seconds: int,
    ) -> dict[str, Any]:
        find = getattr(self._inner, "find_documents", None)
        if (
            not callable(find)
            or not logical_document_ids
            or not set(logical_document_ids) <= self._document_ids
        ):
            raise EvaluationInputError(
                "reader-oracle find escaped supplied documents"
            )
        return find(
            logical_document_ids=logical_document_ids,
            document_aliases=document_aliases,
            patterns=patterns,
            context_chars=context_chars,
            limit=limit,
            record_spans=record_spans,
            routing_receipts=routing_receipts,
            timeout_seconds=timeout_seconds,
        )

    def open_document(
        self,
        *,
        logical_document_id: str,
        document_alias: str,
        cursor: str | None,
        record_ordinal: int | None,
        page_bytes: int,
        record_spans: dict[str, tuple[tuple[int, int], ...]],
        routing_receipts: dict[str, tuple[str, ...]],
        timeout_seconds: int,
    ) -> dict[str, Any]:
        open_document = getattr(self._inner, "open_document", None)
        if (
            not callable(open_document)
            or logical_document_id not in self._document_ids
        ):
            raise EvaluationInputError(
                "reader-oracle open escaped supplied documents"
            )
        return open_document(
            logical_document_id=logical_document_id,
            document_alias=document_alias,
            cursor=cursor,
            record_ordinal=record_ordinal,
            page_bytes=page_bytes,
            record_spans=record_spans,
            routing_receipts=routing_receipts,
            timeout_seconds=timeout_seconds,
        )

    def inspect_documents(
        self,
        *,
        logical_document_ids: tuple[str, ...],
        query: str | None,
        scope: str,
        literal: bool,
        context: int,
        limit: int,
        record_spans: dict[str, tuple[tuple[int, int], ...]],
        routing_receipts: dict[str, tuple[str, ...]],
        timeout_seconds: int,
    ) -> dict[str, Any]:
        """Delegate native inspection inside the supplied document set."""

        inspect = getattr(self._inner, "inspect_documents", None)
        if (
            not callable(inspect)
            or not isinstance(logical_document_ids, tuple)
            or not logical_document_ids
            or not set(logical_document_ids) <= self._document_ids
        ):
            raise EvaluationInputError(
                "reader-oracle inspection escaped supplied documents"
            )
        return inspect(
            logical_document_ids=logical_document_ids,
            query=query,
            scope=scope,
            literal=literal,
            context=context,
            limit=limit,
            record_spans=record_spans,
            routing_receipts=routing_receipts,
            timeout_seconds=timeout_seconds,
        )
