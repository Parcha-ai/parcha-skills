"""Query-time temporal hints (H2-h).

A question such as "what did Greptile flag on PR #6076 around May 2-4" carries
a date the retrieval arms can use, but nothing in the question is a hard
filter: "around", "early May", or "last week" name a neighbourhood, not a
boundary. ``parse_temporal_hint`` turns the hint into a UTC window plus a
confidence, and ``passage_retrieval.search`` applies it as a soft boost (and,
for an exact day-level hint, one extra windowed dense pass) rather than as a
``since``/``until`` filter. Explicit ``since``/``until`` filters still win:
the parser is only consulted when the caller supplied neither.

Grammar (case-insensitive unless noted; ``now`` resolves the defaults):

* ISO dates ``2026-05-03``; ISO ranges ``2026-05-02..2026-05-04``,
  ``2026-05-02 to 2026-05-04``, ``between 2026-05-02 and 2026-05-04``.
* Month-name dates ``May 3``, ``May 3rd``, ``3 May``, ``May 3, 2026``,
  ``Sep 14 2026``; day ranges ``May 2-4``, ``May 2 to 4``, ``May 2 - May 4``,
  ``May 30 - June 2``, ``between May 2 and May 4``.
* Numeric month/day ``5/3``, ``5/3/2026``, ``5/2-5/4`` (US order; both
  parts must be a valid month and day, and the token must stand alone).
* Month names with an optional year: ``in May``, ``May 2026``, ``early
  May``, ``mid-August``, ``late September 2025``. Without a year, the most
  recent occurrence of that month that has started on or before ``now``.
  ``May`` is only a month when capitalised and either preceded by a
  preposition or followed by a day or year, because "may" is also a verb.
  Three-letter abbreviations need a following day or year.
* Relative phrases: ``today``, ``yesterday``, ``this week``, ``last week``,
  ``this month``, ``last month``, ``past/last N days|weeks|months``,
  ``N days|weeks|months ago``, ``last Tuesday``.
* Quarters: ``Q2``, ``Q2 2026``, ``2026Q2``, ``Q2 of 2026``, ``second
  quarter``; years with a preposition: ``in 2025``, ``during 2026``.

Confidence is ``"exact"`` for day-level hints (explicit dates, day ranges,
today, yesterday, last <weekday>) and ``"loose"`` for everything wider or
hedged. A hedge word right before a day-level hint (``around``, ``about``,
``roughly``, ``circa``, ``approximately``, ``sometime``, ``~``) pads the
window by ``HEDGE_PAD_DAYS`` on each side and makes it loose.

Negatives the grammar must not match: ``P2``, ``v2``, ``#6076``, ``503``,
``2-4`` without a month, ``1.2/3``, paths, ``may`` the verb.
"""

from __future__ import annotations

import calendar
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

DEFAULT_TEMPORAL_BOOST_EXACT = 0.5
DEFAULT_TEMPORAL_BOOST_LOOSE = 0.25
# Budget for the extra windowed dense pass an exact hint triggers (H2-h).
# The pass runs after the global dense arm on the same worker, so it adds
# latency but never a fourth pooled connection.
DEFAULT_TEMPORAL_WINDOW_BUDGET_MS = 150
HEDGE_PAD_DAYS = 3

MONTHS = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}
FULL_MONTHS = frozenset(name for name in MONTHS if len(name) > 4 or name == "may")
WEEKDAYS = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}
ORDINAL_QUARTERS = {"first": 1, "second": 2, "third": 3, "fourth": 4, "last": 4}
PREPOSITIONS = (
    "in", "on", "around", "about", "during", "since", "until", "till", "before",
    "after", "early", "mid", "late", "of", "from", "between", "to", "through",
    "by", "for", "circa", "approximately", "roughly", "sometime", "back",
)
HEDGES = ("around", "about", "roughly", "circa", "approximately", "sometime", "~")

_MONTH = r"(?P<month>january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sept|sep|oct|nov|dec)\.?"
_DAY = r"(?:[0-2]?[1-9]|[12]0|3[01])(?:st|nd|rd|th)?"
_YEAR = r"(?:19|20)\d{2}"
_YEAR_OPT = r"(?:,?\s*(?P<year>" + _YEAR + r"))?"
_LB = r"(?<![A-Za-z0-9_./#@$-])"
_RB = r"(?![A-Za-z0-9_./-])"
_HEDGE = r"(?:(?P<hedge>around|about|roughly|circa|approximately|sometime(?:\s+(?:around|in|near))?|~)\s*)?"

ISO_RANGE_RE = re.compile(
    _LB + _HEDGE
    + r"(?:between\s+)?(?P<a>" + _YEAR + r"-\d{2}-\d{2})"
    + r"\s*(?:\.\.|–|—|-|to|and|through|until)\s*"
    + r"(?P<b>" + _YEAR + r"-\d{2}-\d{2})" + _RB,
    re.IGNORECASE,
)
ISO_DATE_RE = re.compile(
    _LB + _HEDGE + r"(?P<a>" + _YEAR + r"-\d{2}-\d{2})(?:T[0-9:.]+Z?)?" + _RB,
    re.IGNORECASE,
)
MONTH_DAY_RANGE_RE = re.compile(
    _LB + _HEDGE + r"(?:between\s+)?" + _MONTH + r"\s+(?P<d1>" + _DAY + r")"
    + r"\s*(?:–|—|-|to|and|through|until)\s*"
    + r"(?:(?P<month2>january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sept|sep|oct|nov|dec)\.?\s+)?"
    + r"(?P<d2>" + _DAY + r")" + _YEAR_OPT + _RB,
    re.IGNORECASE,
)
MONTH_DAY_RE = re.compile(
    _LB + _HEDGE + r"(?:the\s+)?(?:(?P<dfirst>" + _DAY + r")\s+(?:of\s+)?)?" + _MONTH
    + r"(?:\s+(?P<day>" + _DAY + r"))?" + _YEAR_OPT + _RB,
    re.IGNORECASE,
)
NUMERIC_RANGE_RE = re.compile(
    _LB + _HEDGE + r"(?P<m1>1[0-2]|0?[1-9])/(?P<d1>3[01]|[12]\d|0?[1-9])"
    + r"\s*(?:–|-|to)\s*"
    + r"(?P<m2>1[0-2]|0?[1-9])/(?P<d2>3[01]|[12]\d|0?[1-9])"
    + r"(?:/(?P<year>" + _YEAR + r"))?" + _RB,
    re.IGNORECASE,
)
NUMERIC_DATE_RE = re.compile(
    _LB + _HEDGE + r"(?P<m1>1[0-2]|0?[1-9])/(?P<d1>3[01]|[12]\d|0?[1-9])"
    + r"(?:/(?P<year>" + _YEAR + r"))?" + _RB,
    re.IGNORECASE,
)
MONTH_ONLY_RE = re.compile(
    _LB + r"(?:(?P<part>early|mid|late|end of|beginning of|start of)[\s-]*)?"
    + r"(?:(?P<prep>" + "|".join(PREPOSITIONS) + r")\s+)?"
    + r"(?P<month>january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sept|sep|oct|nov|dec)\.?"
    + r"(?:\s+(?:of\s+)?(?P<year>" + _YEAR + r"))?" + _RB,
    re.IGNORECASE,
)
QUARTER_RE = re.compile(
    _LB + r"(?:"
    r"(?P<y1>" + _YEAR + r")[\s-]?q(?P<q1>[1-4])"
    r"|q(?P<q2>[1-4])(?:\s+(?:of\s+)?(?P<y2>" + _YEAR + r"))?"
    r"|(?P<ord>first|second|third|fourth|last)\s+quarter(?:\s+(?:of\s+)?(?P<y3>" + _YEAR + r"))?"
    r")" + _RB,
    re.IGNORECASE,
)
YEAR_RE = re.compile(
    _LB + r"(?:in|during|throughout|back in|of)\s+(?P<year>" + _YEAR + r")" + _RB,
    re.IGNORECASE,
)
RELATIVE_RE = re.compile(
    r"\b(?:"
    r"(?P<today>today)"
    r"|(?P<yesterday>yesterday)"
    r"|(?P<this>this)\s+(?P<this_unit>week|month|quarter|year)"
    r"|(?P<last>last|past|previous)\s+(?:(?P<n>\d{1,3}|a|an|one|two|three|four|five|six|seven|eight|nine|ten|couple of|few)\s+)?(?P<unit>day|week|month|quarter|year)s?"
    r"|(?P<ago_n>\d{1,3}|a|an|one|two|three|four|five|six|seven|eight|nine|ten|couple of|few)\s+(?P<ago_unit>day|week|month|quarter|year)s?\s+ago"
    r"|(?:last|this past|on)\s+(?P<weekday>monday|tuesday|wednesday|thursday|friday|saturday|sunday)"
    r")\b",
    re.IGNORECASE,
)
NUMBER_WORDS = {
    "a": 1, "an": 1, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10, "couple of": 2,
    "few": 3,
}


@dataclass(frozen=True)
class TemporalHint:
    since: str
    until: str
    confidence: str  # "exact" | "loose"
    span: str        # the matched text, e.g. "around May 2-4"; never other query content
    # True for day-level forms (explicit dates, day ranges, today, yesterday,
    # last <weekday>) even when a hedge made them loose: the window is still
    # small enough for the extra windowed dense pass, which is what puts a
    # document at the bottom of the global dense pool into the collapse.
    day_level: bool = False

    def as_diagnostics(self, boost: float) -> dict[str, Any]:
        return {
            "since": self.since,
            "until": self.until,
            "confidence": self.confidence,
            "boost": boost,
        }


@dataclass(frozen=True)
class TemporalHintSettings:
    enabled: bool = True
    boost_exact: float = DEFAULT_TEMPORAL_BOOST_EXACT
    boost_loose: float = DEFAULT_TEMPORAL_BOOST_LOOSE
    window_budget_ms: int = DEFAULT_TEMPORAL_WINDOW_BUDGET_MS

    def boost_for(self, confidence: str) -> float:
        return self.boost_exact if confidence == "exact" else self.boost_loose


def _bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "on", "true", "yes"}:
        return True
    if raw in {"0", "off", "false", "no"}:
        return False
    raise ValueError(f"{name} must be on or off")


def _float_env(name: str, default: float, *, low: float, high: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be between {low} and {high}") from exc
    if not math.isfinite(value) or not low <= value <= high:
        raise ValueError(f"{name} must be between {low} and {high}")
    return value


def temporal_settings_from_env() -> TemporalHintSettings:
    """``RECALL_TEMPORAL_HINTS`` (on|off, default on), ``RECALL_TEMPORAL_BOOST``
    (exact-hint multiplier increment, 0-5, default 0.5),
    ``RECALL_TEMPORAL_BOOST_LOOSE`` (default 0.25),
    ``RECALL_TEMPORAL_WINDOW_BUDGET_MS`` (10-5000, default 150)."""

    return TemporalHintSettings(
        enabled=_bool_env("RECALL_TEMPORAL_HINTS", True),
        boost_exact=_float_env("RECALL_TEMPORAL_BOOST", DEFAULT_TEMPORAL_BOOST_EXACT, low=0.0, high=5.0),
        boost_loose=_float_env(
            "RECALL_TEMPORAL_BOOST_LOOSE", DEFAULT_TEMPORAL_BOOST_LOOSE, low=0.0, high=5.0
        ),
        window_budget_ms=int(
            _float_env(
                "RECALL_TEMPORAL_WINDOW_BUDGET_MS",
                DEFAULT_TEMPORAL_WINDOW_BUDGET_MS,
                low=10,
                high=5000,
            )
        ),
    )


# --- window helpers ---------------------------------------------------------


def _day_start(value: date) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)


def _day_end(value: date) -> datetime:
    return _day_start(value) + timedelta(days=1) - timedelta(seconds=1)


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _window(
    first: date,
    last: date,
    confidence: str,
    span: str,
    *,
    hedged: bool = False,
    now: datetime,
) -> TemporalHint | None:
    if last < first:
        first, last = last, first
    day_level = confidence == "exact"
    if hedged:
        first -= timedelta(days=HEDGE_PAD_DAYS)
        last += timedelta(days=HEDGE_PAD_DAYS)
        confidence = "loose"
    # A window entirely in the future cannot describe recorded history.
    if _day_start(first) > now:
        return None
    return TemporalHint(
        since=_iso(_day_start(first)),
        until=_iso(_day_end(last)),
        confidence=confidence,
        span=span.strip(),
        day_level=day_level,
    )


def _default_year(month: int, day: int, now: datetime) -> int:
    """Most recent year in which ``month``/``day`` is not after ``now``."""

    year = now.year
    day = min(day, calendar.monthrange(year, month)[1])
    if date(year, month, day) > now.date():
        year -= 1
    return year


def _safe_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _day(text: str) -> int:
    return int(re.sub(r"(st|nd|rd|th)$", "", text, flags=re.IGNORECASE))


def _month_is_plausible(match: re.Match, query: str, *, key: str = "month") -> bool:
    """Reject "may" the verb and bare 3-letter abbreviations."""

    raw = match.group(key)
    name = raw.lower().rstrip(".")
    if name == "may" and not raw.startswith("M"):
        return False
    if name not in FULL_MONTHS and not raw.rstrip(".")[0].isupper():
        return False
    return True


# --- parsers, most specific first -------------------------------------------


def _parse_iso_range(query: str, now: datetime) -> TemporalHint | None:
    match = ISO_RANGE_RE.search(query)
    if match is None:
        return None
    first = _parse_iso_day(match.group("a"))
    last = _parse_iso_day(match.group("b"))
    if first is None or last is None:
        return None
    return _window(first, last, "exact", match.group(0), hedged=bool(match.group("hedge")), now=now)


def _parse_iso_day(text: str) -> date | None:
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _parse_iso_dates(query: str, now: datetime) -> TemporalHint | None:
    days: list[date] = []
    hedged = False
    spans: list[str] = []
    for match in ISO_DATE_RE.finditer(query):
        value = _parse_iso_day(match.group("a"))
        if value is None:
            continue
        days.append(value)
        spans.append(match.group(0))
        hedged = hedged or bool(match.group("hedge"))
    if not days:
        return None
    first, last = min(days), max(days)
    if (last - first).days > 60:
        first = last = days[0]
        spans = spans[:1]
    return _window(first, last, "exact", " ".join(spans), hedged=hedged, now=now)


def _parse_month_day_range(query: str, now: datetime) -> TemporalHint | None:
    for match in MONTH_DAY_RANGE_RE.finditer(query):
        if not _month_is_plausible(match, query):
            continue
        month1 = MONTHS[match.group("month").lower().rstrip(".")]
        month2 = (
            MONTHS[match.group("month2").lower().rstrip(".")]
            if match.group("month2")
            else month1
        )
        d1, d2 = _day(match.group("d1")), _day(match.group("d2"))
        if match.group("year"):
            year1 = int(match.group("year"))
            year2 = year1
            if month2 < month1:
                year1 -= 1
        else:
            year2 = _default_year(month2, d2, now)
            year1 = year2 - 1 if month2 < month1 else year2
        first, last = _safe_date(year1, month1, d1), _safe_date(year2, month2, d2)
        if first is None or last is None:
            continue
        return _window(first, last, "exact", match.group(0), hedged=bool(match.group("hedge")), now=now)
    return None


def _parse_month_days(query: str, now: datetime) -> TemporalHint | None:
    days: list[date] = []
    spans: list[str] = []
    hedged = False
    for match in MONTH_DAY_RE.finditer(query):
        day_text = match.group("day") or match.group("dfirst")
        if day_text is None:
            continue
        if not _month_is_plausible(match, query):
            continue
        # "May 2026" is a month-year, not a day.
        month = MONTHS[match.group("month").lower().rstrip(".")]
        day = _day(day_text)
        year = int(match.group("year")) if match.group("year") else _default_year(month, day, now)
        value = _safe_date(year, month, day)
        if value is None:
            continue
        days.append(value)
        spans.append(match.group(0))
        hedged = hedged or bool(match.group("hedge"))
    if not days:
        return None
    first, last = min(days), max(days)
    if (last - first).days > 60:
        first = last = days[0]
        spans = spans[:1]
    return _window(first, last, "exact", " ".join(spans), hedged=hedged, now=now)


def _parse_numeric(query: str, now: datetime) -> TemporalHint | None:
    match = NUMERIC_RANGE_RE.search(query)
    if match is not None:
        m1, d1 = int(match.group("m1")), int(match.group("d1"))
        m2, d2 = int(match.group("m2")), int(match.group("d2"))
        year2 = int(match.group("year")) if match.group("year") else _default_year(m2, d2, now)
        year1 = year2 - 1 if m2 < m1 else year2
        first, last = _safe_date(year1, m1, d1), _safe_date(year2, m2, d2)
        if first is not None and last is not None:
            return _window(first, last, "exact", match.group(0), hedged=bool(match.group("hedge")), now=now)
    match = NUMERIC_DATE_RE.search(query)
    if match is None:
        return None
    m1, d1 = int(match.group("m1")), int(match.group("d1"))
    year = int(match.group("year")) if match.group("year") else _default_year(m1, d1, now)
    value = _safe_date(year, m1, d1)
    if value is None:
        return None
    return _window(value, value, "exact", match.group(0), hedged=bool(match.group("hedge")), now=now)


def _parse_month_only(query: str, now: datetime) -> TemporalHint | None:
    for match in MONTH_ONLY_RE.finditer(query):
        raw = match.group("month")
        name = raw.lower().rstrip(".")
        if name == "may":
            # "may" is a verb; as a month it needs a capital and context.
            if not raw.startswith("M") or not (match.group("prep") or match.group("year") or match.group("part")):
                continue
        elif name not in FULL_MONTHS and not (raw[0].isupper() and match.group("year")):
            continue
        month = MONTHS[name]
        if match.group("year"):
            year = int(match.group("year"))
        else:
            year = now.year if date(now.year, month, 1) <= now.date() else now.year - 1
        first, last = date(year, month, 1), _month_end(year, month)
        part = (match.group("part") or "").lower().replace("-", " ").strip()
        if part == "early" or part.startswith("beginning") or part.startswith("start"):
            last = date(year, month, 10)
        elif part == "mid":
            first, last = date(year, month, 11), date(year, month, 20)
        elif part == "late" or part.startswith("end"):
            first = date(year, month, 21)
        return _window(first, last, "loose", match.group(0), now=now)
    return None


def _parse_quarter(query: str, now: datetime) -> TemporalHint | None:
    match = QUARTER_RE.search(query)
    if match is None:
        return None
    if match.group("q1"):
        quarter, year = int(match.group("q1")), int(match.group("y1"))
    elif match.group("q2"):
        quarter = int(match.group("q2"))
        year = int(match.group("y2")) if match.group("y2") else None
    else:
        quarter = ORDINAL_QUARTERS[match.group("ord").lower()]
        year = int(match.group("y3")) if match.group("y3") else None
    start_month = (quarter - 1) * 3 + 1
    if year is None:
        year = now.year if date(now.year, start_month, 1) <= now.date() else now.year - 1
    first = date(year, start_month, 1)
    last = _month_end(year, start_month + 2)
    return _window(first, last, "loose", match.group(0), now=now)


def _parse_year(query: str, now: datetime) -> TemporalHint | None:
    match = YEAR_RE.search(query)
    if match is None:
        return None
    year = int(match.group("year"))
    return _window(date(year, 1, 1), date(year, 12, 31), "loose", match.group(0), now=now)


def _amount(text: str | None) -> int:
    if text is None:
        return 1
    lowered = text.lower()
    if lowered in NUMBER_WORDS:
        return NUMBER_WORDS[lowered]
    return max(1, min(int(lowered), 365))


def _shift_months(value: date, months: int) -> date:
    index = value.year * 12 + (value.month - 1) - months
    year, month = divmod(index, 12)
    return date(year, month + 1, min(value.day, calendar.monthrange(year, month + 1)[1]))


def _parse_relative(query: str, now: datetime) -> TemporalHint | None:
    match = RELATIVE_RE.search(query)
    if match is None:
        return None
    today = now.date()
    span = match.group(0)
    if match.group("today"):
        return _window(today, today, "exact", span, now=now)
    if match.group("yesterday"):
        return _window(today - timedelta(days=1), today - timedelta(days=1), "exact", span, now=now)
    if match.group("weekday"):
        target = WEEKDAYS[match.group("weekday").lower()]
        back = (today.weekday() - target) % 7 or 7
        value = today - timedelta(days=back)
        return _window(value, value, "exact", span, now=now)
    if match.group("this"):
        unit = match.group("this_unit").lower()
        if unit == "week":
            first = today - timedelta(days=today.weekday())
        elif unit == "month":
            first = today.replace(day=1)
        elif unit == "quarter":
            first = date(today.year, ((today.month - 1) // 3) * 3 + 1, 1)
        else:
            first = date(today.year, 1, 1)
        return _window(first, today, "loose", span, now=now)
    if match.group("last"):
        unit = match.group("unit").lower()
        amount = _amount(match.group("n"))
        if match.group("n") is None and unit == "month":
            previous = _shift_months(today.replace(day=1), 1)
            return _window(previous, _month_end(previous.year, previous.month), "loose", span, now=now)
        if match.group("n") is None and unit == "quarter":
            start_month = ((today.month - 1) // 3) * 3 + 1
            previous = _shift_months(date(today.year, start_month, 1), 3)
            return _window(previous, _month_end(previous.year, previous.month + 2), "loose", span, now=now)
        if match.group("n") is None and unit == "year":
            return _window(date(today.year - 1, 1, 1), date(today.year - 1, 12, 31), "loose", span, now=now)
        if unit == "day":
            first = today - timedelta(days=amount)
        elif unit == "week":
            first = today - timedelta(weeks=amount)
        elif unit == "month":
            first = _shift_months(today, amount)
        elif unit == "quarter":
            first = _shift_months(today, 3 * amount)
        else:
            first = _shift_months(today, 12 * amount)
        return _window(first, today, "loose", span, now=now)
    amount = _amount(match.group("ago_n"))
    unit = match.group("ago_unit").lower()
    if unit == "day":
        centre = today - timedelta(days=amount)
        pad = timedelta(days=1)
    elif unit == "week":
        centre = today - timedelta(weeks=amount)
        pad = timedelta(days=3)
    elif unit == "month":
        centre = _shift_months(today, amount)
        pad = timedelta(days=15)
    elif unit == "quarter":
        centre = _shift_months(today, 3 * amount)
        pad = timedelta(days=45)
    else:
        centre = _shift_months(today, 12 * amount)
        pad = timedelta(days=182)
    return _window(centre - pad, centre + pad, "loose", span, now=now)


def parse_temporal_hint(query: str, *, now: datetime) -> TemporalHint | None:
    """Return the window a question's date phrase names, or ``None``.

    Explicit day-level forms win over month names, which win over relative
    phrases, so "May 2-4 last year" resolves to the day range. ``now`` must
    be timezone-aware; naive values are taken as UTC.
    """

    if not isinstance(query, str) or not query.strip():
        return None
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    for parser in (
        _parse_iso_range,
        _parse_iso_dates,
        _parse_month_day_range,
        _parse_month_days,
        _parse_numeric,
        _parse_relative,
        _parse_month_only,
        _parse_quarter,
        _parse_year,
    ):
        hint = parser(query, now)
        if hint is not None:
            return hint
    return None


def parse_bound(value: Any) -> datetime | None:
    """Parse an ISO bound as the retrieval rows carry it (``str(datetime)``)."""

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    text = value.strip().replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    elif text.endswith("+00"):
        text += ":00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def window_intersects(first: Any, last: Any, since: str, until: str) -> bool:
    """True when ``[first, last]`` overlaps ``[since, until]`` (inclusive)."""

    lower, upper = parse_bound(since), parse_bound(until)
    start, end = parse_bound(first), parse_bound(last)
    if lower is None or upper is None or start is None or end is None:
        return False
    return end >= lower and start <= upper
