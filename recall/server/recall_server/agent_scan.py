"""Dependency-free search helper installed inside the Archil exec sandbox."""

AGENT_SCAN_SCRIPT = r"""#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(
    os.environ.get("RECALL_EVIDENCE_ROOT", "/mnt/archil/evidence")
).resolve()
OBJECT_KEY = re.compile(r"objects/[0-9a-f]{2}/[0-9a-f]{64}\Z")
DOCUMENT_ID = re.compile(r"ldoc_[0-9a-f]{32}\Z")
OUTPUT_BYTES = 7_500
CONTENT_CHARS = 1_200
OPEN_PAGE_BYTES = 6_000
MAX_OPEN_PAGE_BYTES = 1_048_576


def add_content_coordinates(projected, content, start, end):
    # Exact character and UTF-8 byte coordinates for one content slice.

    projected["content_start"] = start
    projected["content_end"] = end
    projected["content_length"] = len(content)
    projected["content_byte_start"] = len(content[:start].encode())
    projected["content_byte_end"] = len(content[:end].encode())
    projected["content_length_bytes"] = len(content.encode())
    projected["content_complete"] = start == 0 and end == len(content)


def admitted_path(object_key):
    if not isinstance(object_key, str) or not OBJECT_KEY.fullmatch(object_key):
        raise SystemExit(64)
    path = (ROOT / object_key).resolve()
    if ROOT not in path.parents or not path.is_file():
        raise SystemExit(66)
    return str(path)


def trusted_pointer_ranges():
    path_value = os.environ.get("RECALL_POINTERS_PATH")
    if not path_value:
        return {}, {}
    try:
        value = json.loads(pathlib.Path(path_value).read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise SystemExit(66) from None
    if not isinstance(value, dict) or len(value) > 80:
        raise SystemExit(66)
    projected = {}
    receipts_by_document = {}
    for document_id, pointer in value.items():
        if (
            not DOCUMENT_ID.fullmatch(document_id)
            or not isinstance(pointer, dict)
            or set(pointer) != {"spans", "routing_receipts"}
        ):
            raise SystemExit(66)
        spans = pointer["spans"]
        receipts = pointer["routing_receipts"]
        if (
            not isinstance(spans, list)
            or len(spans) > 256
            or not isinstance(receipts, list)
            or len(receipts) > 256
            or any(
                not isinstance(receipt, str)
                or not receipt.startswith("recall://")
                or len(receipt) > 2048
                for receipt in receipts
            )
        ):
            raise SystemExit(66)
        ranges = []
        for span in spans:
            if not isinstance(span, dict):
                raise SystemExit(66)
            start = span.get("record_ordinal")
            count = span.get("record_count")
            if (
                isinstance(start, bool)
                or not isinstance(start, int)
                or start < 0
                or isinstance(count, bool)
                or not isinstance(count, int)
                or not 1 <= count <= 10_000
            ):
                raise SystemExit(66)
            ranges.append((start, start + count - 1))
        projected[document_id] = ranges
        receipts_by_document[document_id] = receipts
    return projected, receipts_by_document


def bounded_document(parts, ranges, routing_receipts):
    core_ranges = list(ranges)
    scan_ranges = [
        (max(0, start - 2), end + 2)
        for start, end in ranges
    ]
    if not core_ranges:
        return None
    directory = pathlib.Path(tempfile.mkdtemp(prefix="recall-scan-"))
    target = directory / "selected.jsonl"
    selected = {}
    for part in parts:
        first = part["first_record_ordinal"]
        last = part["last_record_ordinal"]
        relevant = [
            (max(first, start), min(last, end))
            for start, end in scan_ranges
            if first <= end and last >= start
        ]
        if not relevant:
            continue
        last_needed = max(end for _, end in relevant)
        path = admitted_path(part.get("object_key"))
        with open(path, "rb") as source:
            for line_index, line in enumerate(source):
                ordinal = first + line_index
                if ordinal > last_needed:
                    break
                if any(start <= ordinal <= end for start, end in relevant):
                    record_path = directory / f"record-{ordinal}.jsonl"
                    record_path.write_bytes(line)
                    selected[ordinal] = record_path
    emitted = set()
    receipt_order = {
        receipt: index for index, receipt in enumerate(routing_receipts)
    }
    prioritized_ordinals = []
    for ordinal, record_path in selected.items():
        try:
            record = json.loads(record_path.read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        ranks = [
            receipt_order[receipt]
            for receipt in record.get("receipts", [])
            if receipt in receipt_order
        ]
        if ranks:
            prioritized_ordinals.append((min(ranks), ordinal))
    print(
        "RECALL_SCAN_POINTERS "
        f"selected_records={len(selected)} "
        f"receipt_prioritized_records={len(prioritized_ordinals)} "
        f"routing_receipts={len(routing_receipts)}",
        file=sys.stderr,
    )
    with target.open("wb") as output:
        # Map the exact passage spans first in retrieval rank order. Only then
        # append nearby context, so a bounded output cannot be consumed by the
        # first pointer's surrounding records.
        for _, ordinal in sorted(prioritized_ordinals):
            output.write(selected[ordinal].read_bytes())
            emitted.add(ordinal)
            output.write(b"\n" * 6)
        for phase in (core_ranges, scan_ranges):
            for start, end in phase:
                wrote_range = False
                for ordinal in range(start, end + 1):
                    record_path = selected.get(ordinal)
                    if record_path is None or ordinal in emitted:
                        continue
                    output.write(record_path.read_bytes())
                    emitted.add(ordinal)
                    wrote_range = True
                if wrote_range:
                    output.write(b"\n" * 6)
    return str(target)


def search_files(
    document_ids,
    record_ranges,
    pointer_ranges,
    pointer_receipts,
    broad,
):
    if not document_ids and (not pointer_ranges or broad):
        return [
            (None, str(path))
            for path in ROOT.rglob("*")
            if path.is_file() and path.stat().st_size >= 10_000
        ]
    wanted_order = list(dict.fromkeys(document_ids or pointer_ranges))
    wanted = set(wanted_order)
    found = set()
    files_by_document = {document_id: [] for document_id in wanted_order}
    for path in ROOT.rglob("*"):
        if not path.is_file() or path.stat().st_size > 100_000:
            continue
        try:
            manifest = json.loads(path.read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        document_id = manifest.get("logical_document_id")
        if document_id not in wanted:
            continue
        found.add(document_id)
        selected_ranges = (
            record_ranges
            if record_ranges
            else []
            if broad
            else pointer_ranges.get(document_id, [])
        )
        parts = [
            part
            for part in manifest.get("parts", [])
            if isinstance(part, dict)
            and isinstance(part.get("first_record_ordinal"), int)
            and isinstance(part.get("last_record_ordinal"), int)
        ]
        if selected_ranges:
            selected = bounded_document(
                parts,
                selected_ranges,
                pointer_receipts.get(document_id, []),
            )
            if selected is not None:
                files_by_document[document_id].append(selected)
            continue
        for part in parts:
            files_by_document[document_id].append(
                admitted_path(part.get("object_key"))
            )
    if found != wanted:
        raise SystemExit(66)
    return list(dict.fromkeys(
        (document_id, path)
        for document_id in wanted_order
        for path in files_by_document[document_id]
    ))


def content_window(content, patterns, excerpt_chars):
    if not patterns:
        return 0, min(len(content), excerpt_chars)
    lowered = content.casefold()
    positions = [
        lowered.find(pattern.casefold())
        for pattern in patterns
        if pattern
    ]
    positions = [position for position in positions if position >= 0]
    if not positions:
        return 0, min(len(content), excerpt_chars)
    center = min(positions)
    start = max(0, center - excerpt_chars // 2)
    end = min(len(content), start + excerpt_chars)
    start = max(0, end - excerpt_chars)
    return start, end


def render_record(
    record,
    document_id,
    *,
    patterns=(),
    excerpt_chars=CONTENT_CHARS,
):
    if not (
        isinstance(record, dict)
        and isinstance(record.get("receipts"), list)
        and "content" in record
    ):
        return None
    identity = (
        record.get("event_native_id"),
        record.get("ordinal"),
        tuple(
            receipt
            for receipt in record["receipts"]
            if isinstance(receipt, str)
        ),
    )
    content = json.dumps(
        record["content"],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    projected = {
        key: record.get(key)
        for key in (
            "event_native_id",
            "occurred_at",
            "ordinal",
            "receipts",
        )
    }
    projected["logical_document_id"] = document_id
    start, end = content_window(content, patterns, excerpt_chars)
    projected["content"] = content[start:end]
    add_content_coordinates(projected, content, start, end)
    rendered = json.dumps(
        projected,
        ensure_ascii=False,
        separators=(",", ":"),
    ) + "\n"
    for receipt in record["receipts"]:
        if isinstance(receipt, str) and receipt.startswith("recall://"):
            rendered += "RECALL_EVIDENCE " + receipt + "\n"
    return identity, rendered


def render_open_record(record, document_id, start, end):
    if not (
        isinstance(record, dict)
        and isinstance(record.get("receipts"), list)
        and "content" in record
        and isinstance(record.get("ordinal"), int)
        and not isinstance(record.get("ordinal"), bool)
        and record["ordinal"] >= 0
    ):
        return None
    content = json.dumps(
        record["content"],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    if not 0 <= start < len(content) or not start < end <= len(content):
        return None
    projected = {
        key: record.get(key)
        for key in (
            "event_native_id",
            "occurred_at",
            "ordinal",
            "receipts",
        )
    }
    projected["logical_document_id"] = document_id
    projected["content"] = content[start:end]
    add_content_coordinates(projected, content, start, end)
    return json.dumps(
        projected,
        ensure_ascii=False,
        separators=(",", ":"),
    ) + "\n"


def open_page(selected_files, cursor, page_bytes, one_record=False):
    cursor_file, cursor_line, cursor_offset = cursor
    emitted_bytes = 0
    started = False
    next_cursor = None
    complete = True
    finished_one = False
    for file_index, (document_id, path) in enumerate(selected_files):
        if file_index < cursor_file:
            continue
        try:
            stream = open(path, encoding="utf-8", errors="replace")
        except OSError:
            continue
        with stream:
            if file_index == cursor_file:
                try:
                    stream.seek(cursor_line)
                except OSError:
                    raise SystemExit(66) from None
            while True:
                line_start = stream.tell()
                line = stream.readline()
                if not line:
                    next_cursor = (file_index + 1, 0, 0)
                    break
                line_end = stream.tell()
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ordinal = record.get("ordinal")
                if (
                    isinstance(ordinal, bool)
                    or not isinstance(ordinal, int)
                    or ordinal < 0
                ):
                    continue
                offset = (
                    cursor_offset
                    if file_index == cursor_file
                    and line_start == cursor_line
                    else 0
                )
                content = json.dumps(
                    record.get("content"),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                if offset >= len(content):
                    continue
                started = True
                available = page_bytes - emitted_bytes
                if available < 256:
                    next_cursor = (file_index, line_start, offset)
                    complete = False
                    break
                low = 1
                high = len(content) - offset
                best = None
                while low <= high:
                    amount = (low + high) // 2
                    rendered = render_open_record(
                        record,
                        document_id,
                        offset,
                        offset + amount,
                    )
                    size = len(rendered.encode()) if rendered else available + 1
                    if size <= available:
                        best = rendered
                        low = amount + 1
                    else:
                        high = amount - 1
                if best is None:
                    next_cursor = (file_index, line_start, offset)
                    complete = False
                    break
                sys.stdout.write(best)
                emitted_bytes += len(best.encode())
                rendered_record = json.loads(best)
                end = rendered_record["content_end"]
                if end < len(content):
                    next_cursor = (file_index, line_start, end)
                    complete = False
                    break
                if one_record:
                    next_cursor = None
                    finished_one = True
                    break
                next_cursor = (file_index, line_end, 0)
            if not complete:
                break
            if finished_one:
                break
    if not started:
        next_cursor = None
    metadata = {
        "next_cursor": (
            f"{next_cursor[0]}:{next_cursor[1]}:{next_cursor[2]}"
            if not complete and next_cursor is not None
            else None
        ),
        "complete": complete,
        "emitted_bytes": emitted_bytes,
    }
    print(
        "RECALL_PAGE "
        + json.dumps(metadata, separators=(",", ":"), sort_keys=True)
    )


def cursor_for_record(selected_files, record_ordinal):
    for file_index, (_document_id, path) in enumerate(selected_files):
        try:
            stream = open(path, encoding="utf-8", errors="replace")
        except OSError:
            continue
        with stream:
            while True:
                line_start = stream.tell()
                line = stream.readline()
                if not line:
                    break
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ordinal = record.get("ordinal")
                if (
                    isinstance(ordinal, int)
                    and not isinstance(ordinal, bool)
                    and ordinal >= record_ordinal
                ):
                    return file_index, line_start, 0
    return len(selected_files), 0, 0


def main():
    parser = argparse.ArgumentParser(prog="recall-scan")
    parser.add_argument("--document", "--doc", action="append", default=[])
    parser.add_argument("--pattern", "--query", action="append", default=[])
    parser.add_argument(
        "--all",
        action="store_true",
        help="open records selected by host pointer spans without keyword filtering",
    )
    parser.add_argument("--fixed", "--literal", action="store_true")
    parser.add_argument("--limit", "--max-matches", type=int, default=6)
    parser.add_argument("--context", type=int, default=0)
    parser.add_argument(
        "--excerpt-chars",
        type=int,
        default=CONTENT_CHARS,
    )
    parser.add_argument("--cursor")
    parser.add_argument("--start-record", type=int)
    parser.add_argument("--one-record", action="store_true")
    parser.add_argument("--page-bytes", type=int, default=OPEN_PAGE_BYTES)
    parser.add_argument("--records", action="append", default=[])
    parser.add_argument(
        "--broad",
        action="store_true",
        help="ignore host pointer spans and scan every admitted part",
    )
    parser.add_argument("terms", nargs="*")
    args = parser.parse_args()
    patterns = [*args.pattern, *args.terms]
    cursor = None
    if args.cursor is not None:
        try:
            cursor_file, cursor_line, cursor_offset = args.cursor.split(":")
            cursor = (
                int(cursor_file),
                int(cursor_line),
                int(cursor_offset),
            )
            if any(value < 0 for value in cursor):
                raise ValueError
        except (TypeError, ValueError):
            raise SystemExit(64) from None
    record_ranges = []
    try:
        for value in args.records:
            for item in value.split(","):
                start_text, count_text = item.split(":", 1)
                start = int(start_text)
                count = int(count_text)
                if start < 0 or not 1 <= count <= 10_000:
                    raise ValueError
                record_ranges.append((start, start + count - 1))
    except (TypeError, ValueError):
        raise SystemExit(64) from None
    pattern = (
        r"^\{"
        if args.all
        else "|".join(
            re.escape(value) if args.fixed else f"(?:{value})"
            for value in patterns
        )
    )
    if (
        any(not DOCUMENT_ID.fullmatch(item) for item in args.document)
        or (args.all and patterns)
        or not pattern
        or len(pattern) > 4_000
        or not 1 <= args.limit <= 50
        or not 0 <= args.context <= 5
        or not 200 <= args.excerpt_chars <= 4_000
        or not 1_024 <= args.page_bytes <= MAX_OPEN_PAGE_BYTES
        or (
            cursor is not None
            and (
                len(args.document) != 1
                or patterns
                or not args.all
                or not args.broad
                or record_ranges
                or (
                    args.start_record is not None
                    and args.start_record < 0
                )
            )
        )
        or (cursor is None and args.start_record is not None)
        or (
            args.one_record
            and (
                args.start_record is None
                or cursor != (0, 0, 0)
            )
        )
    ):
        raise SystemExit(64)
    pointer_ranges, pointer_receipts = trusted_pointer_ranges()
    selected_files = search_files(
        args.document,
        record_ranges,
        pointer_ranges,
        pointer_receipts,
        args.broad,
    )
    if not selected_files:
        print('{"matches":0,"reason":"no_selected_parts"}')
        return
    if cursor is not None:
        if args.start_record is not None and cursor == (0, 0, 0):
            cursor = cursor_for_record(
                selected_files,
                args.start_record,
            )
        open_page(
            selected_files,
            cursor,
            args.page_bytes,
            one_record=args.one_record,
        )
        return
    files = [path for _, path in selected_files]
    document_by_path = {
        str(pathlib.Path(path)): document_id
        for document_id, path in selected_files
        if document_id is not None
    }
    if args.all:
        emitted = 0
        emitted_bytes = 0
        emitted_records = set()
        for document_id, path in selected_files:
            try:
                stream = open(path, encoding="utf-8", errors="replace")
            except OSError:
                continue
            with stream:
                for line in stream:
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    projection = render_record(record, document_id)
                    if projection is None:
                        continue
                    identity, rendered = projection
                    if identity in emitted_records:
                        continue
                    rendered_bytes = len(rendered.encode())
                    if emitted_bytes + rendered_bytes > OUTPUT_BYTES:
                        break
                    emitted_records.add(identity)
                    sys.stdout.write(rendered)
                    emitted_bytes += rendered_bytes
                    emitted += 1
                    if emitted >= args.limit:
                        return
        if emitted == 0:
            print('{"matches":0}')
        return
    # The managed execution image is intentionally minimal and does not
    # guarantee ripgrep. Pi's public `find` tool is literal and record-local,
    # so execute that contract in this dependency-free helper itself.
    if args.fixed and args.context == 0:
        needles = tuple(value.casefold() for value in patterns)
        emitted = 0
        emitted_bytes = 0
        emitted_records = set()
        for document_id, path in selected_files:
            try:
                stream = open(path, encoding="utf-8", errors="replace")
            except OSError:
                continue
            with stream:
                for line in stream:
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    content = json.dumps(
                        record.get("content"),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).casefold()
                    if not any(needle in content for needle in needles):
                        continue
                    projection = render_record(
                        record,
                        document_id,
                        patterns=patterns,
                        excerpt_chars=args.excerpt_chars,
                    )
                    if projection is None:
                        continue
                    record_identity, rendered = projection
                    if record_identity in emitted_records:
                        continue
                    rendered_bytes = len(rendered.encode())
                    if emitted_bytes + rendered_bytes > OUTPUT_BYTES:
                        break
                    emitted_records.add(record_identity)
                    sys.stdout.write(rendered)
                    emitted_bytes += rendered_bytes
                    emitted += 1
                    if emitted >= args.limit:
                        return
        if emitted == 0:
            print('{"matches":0}')
        return
    command = [
        "rg",
        "--json",
        "--ignore-case",
        "--max-columns",
        "8000000",
        "--max-count",
        str(args.limit),
        "--context",
        str(args.context),
    ]
    if args.all:
        command.append("--text")
    command.extend(["--", pattern, *files])
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        errors="replace",
    )
    emitted = 0
    emitted_bytes = 0
    emitted_records = set()
    try:
        for line in process.stdout or ():
            try:
                event = json.loads(line)
                if event.get("type") not in {"match", "context"}:
                    continue
                record = json.loads(event["data"]["lines"]["text"])
            except (
                KeyError,
                TypeError,
                UnicodeDecodeError,
                json.JSONDecodeError,
            ):
                continue
            try:
                matched_path = event["data"]["path"]["text"]
            except (KeyError, TypeError):
                matched_path = ""
            projection = render_record(
                record,
                document_by_path.get(str(pathlib.Path(matched_path))),
                patterns=patterns,
                excerpt_chars=args.excerpt_chars,
            )
            if projection is None:
                continue
            record_identity, rendered = projection
            if record_identity in emitted_records:
                continue
            emitted_records.add(record_identity)
            rendered_bytes = len(rendered.encode())
            if emitted_bytes + rendered_bytes > OUTPUT_BYTES:
                break
            sys.stdout.write(rendered)
            emitted_bytes += rendered_bytes
            emitted += 1
            if emitted >= args.limit:
                break
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait()
    if emitted == 0:
        print('{"matches":0}')


if __name__ == "__main__":
    main()
"""
