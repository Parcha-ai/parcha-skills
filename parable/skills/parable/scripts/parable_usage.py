#!/usr/bin/env python3
"""parable_usage — read live subscription headroom and billing state, pre-turn.

The load-balancing story rests on one fact: every subscription parable routes to
publishes its own remaining-headroom over an authenticated HTTP probe that costs
ZERO model tokens and needs NO turn. The brain reads these before it routes, so
"which pool has room" is measured, not guessed-from-throttle-after-the-fact.

Three probes, each reading the same credential the local harness already stored
(this module never mints or writes a token):

  claude  (Anthropic Max/Pro)  GET  api.anthropic.com/api/oauth/usage
          -> ~/.claude/.credentials.json  .claudeAiOauth.accessToken  (OAuth, user:profile scope)
          windows: five_hour, seven_day, seven_day_opus  (utilization 0-100, resets_at ISO)
          billing: usage-credit enabled state and current-period spend (not weekly history)

  codex   (ChatGPT Pro/Plus)   GET  chatgpt.com/backend-api/wham/usage
          -> ~/.codex/auth.json  .tokens.access_token + .tokens.account_id
          windows: primary/secondary, labeled from duration  (used_percent 0-100, reset_at unix)
          billing: credit balance, overage state, and spend-control state

  cursor  (Cursor Pro/Ultra)   POST api2.cursor.sh/auth/exchange_user_api_key  (key->JWT)
          then POST .../aiserver.v1.DashboardService/GetCurrentPeriodUsage
          -> $CURSOR_API_KEY  (env)   included-budget cents: limit / remaining

All three endpoints are internal/undocumented (the same ones the official CLIs
call); shapes can shift between CLI versions. Every probe fails SOFT — a missing
credential, a 401 on a stale token, or a shape change yields status="unknown"
with a reason, never an exception. Unknown headroom means "route as if it has
room" — the probe informs, it never blocks.

stdlib only (urllib), so parable.py stays dependency-free.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HTTP_TIMEOUT = 6.0  # seconds; a probe is a pre-flight, never a bottleneck

# The usage endpoints throttle rapid polling — Claude's /api/oauth/usage in particular
# trips into a multi-minute HTTP 429 cooldown after a burst. A short on-disk cache means
# repeated `parable usage` calls within a window reuse the last read instead of re-hitting
# the endpoint, so the brain can poll freely without ever tripping the limit. Headroom
# does not move meaningfully second-to-second, so a stale-by-seconds read is fine.
CACHE_TTL_SECONDS = 45
_CACHE_PATH = Path(os.environ.get(
    "PARABLE_USAGE_CACHE",
    str(Path(tempfile.gettempdir()) / f"parable-usage-cache-{os.getuid() if hasattr(os, 'getuid') else 'u'}.json"),
))


def _get_json(url: str, headers: dict, data: bytes | None = None) -> dict:
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method="POST" if data is not None else "GET")
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8", "replace") or "{}")


def _mins_until(unix_or_iso) -> int | None:
    """Minutes from now until a reset time given as unix seconds or ISO-8601."""
    try:
        if isinstance(unix_or_iso, (int, float)):
            when = datetime.fromtimestamp(unix_or_iso, tz=timezone.utc)
        else:
            when = datetime.fromisoformat(str(unix_or_iso).replace("Z", "+00:00"))
        return max(0, round((when - datetime.now(timezone.utc)).total_seconds() / 60))
    except Exception:
        return None


def _unknown(pool: str, reason: str) -> dict:
    return {"pool": pool, "status": "unknown", "reason": reason, "windows": []}


def _major_units(amount, exponent=2) -> float | None:
    """Convert a provider minor-unit value without assuming cents forever."""
    if amount is None:
        return None
    try:
        places = int(exponent)
        return round(float(amount) / (10 ** places), max(0, places))
    except (TypeError, ValueError, OverflowError):
        return None


def _duration_label(seconds, fallback: str) -> str:
    """Name a provider window from its duration; keys do not imply cadence."""
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        return fallback
    if seconds > 0 and seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds > 0 and seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    return fallback


# ---------------------------------------------------------------------------
# CLIProxyAPI-stored credentials
#
# `parable setup`'s subscription-only path delegates each vendor's native OAuth
# into a loopback CLIProxyAPI, which writes one <vendor>-<id>.json per connected
# account with a flat `access_token`. In that setup the vendors' own CLI
# credential files are never created, so a probe that only knows the native path
# reports `unknown` for a subscription that is in fact connected — and unknown
# headroom routes as "has room".
# ---------------------------------------------------------------------------

def _cliproxy_auth_dir() -> Path:
    return Path(os.environ.get(
        "CLIPROXY_AUTH_DIR", str(Path.home() / ".cli-proxy-api"))).expanduser()


def _cliproxy_token(prefix: str) -> str | None:
    """`access_token` from the newest <prefix>-*.json CLIProxyAPI wrote.

    Read-only, like every credential access in this module: it never mints,
    refreshes, or writes a token.
    """
    try:
        found = sorted(_cliproxy_auth_dir().glob(f"{prefix}-*.json"),
                       key=lambda f: f.stat().st_mtime, reverse=True)
    except Exception:
        return None
    for path in found:
        try:
            token = json.loads(path.read_text()).get("access_token")
        except Exception:
            continue
        if token:
            return token
    return None


# ---------------------------------------------------------------------------
# claude — Anthropic subscription (OAuth)
# ---------------------------------------------------------------------------

def probe_claude() -> dict:
    cred = Path(os.environ.get("CLAUDE_CONFIG_DIR", str(Path.home() / ".claude"))) / ".credentials.json"
    oauth: dict = {}
    token = None
    if cred.is_file():
        try:
            oauth = json.loads(cred.read_text()).get("claudeAiOauth", {}) or {}
            token = oauth.get("accessToken")
        except Exception as e:
            return _unknown("claude", f"unreadable credentials ({e})")
    if not token:
        token = _cliproxy_token("claude")
    if not token:
        return _unknown("claude", f"no {cred} and no claude-*.json in {_cliproxy_auth_dir()}")
    try:
        body = _get_json(
            "https://api.anthropic.com/api/oauth/usage",
            {"Authorization": f"Bearer {token}",
             "anthropic-beta": "oauth-2025-04-20",
             "Content-Type": "application/json",
             "User-Agent": "parable-usage/1"})
    except urllib.error.HTTPError as e:
        return _unknown("claude", f"HTTP {e.code}"
                        + (" (token stale — run any claude cmd to refresh)" if e.code == 401 else ""))
    except Exception as e:
        return _unknown("claude", f"probe failed ({e})")

    return {"pool": "claude", "status": "ok", "plan": oauth.get("subscriptionType"),
            "windows": claude_windows(body), "billing": claude_billing(body)}


def claude_windows(body: dict) -> list[dict]:
    """Normalize the /api/oauth/usage body into window dicts. Prefers the newer
    limits[] array: it carries weekly_scoped (per-model) buckets the flat
    five_hour/seven_day fields omit — and the scoped weekly cap on the brain's own
    model is often the TIGHTEST window, so missing it under-counts real budget
    pressure. Falls back to the flat fields when limits[] is absent."""
    windows = []
    limits = body.get("limits")
    if isinstance(limits, list) and limits:
        label_for = {"session": "5h", "weekly_all": "7d"}
        for lim in limits:
            if not isinstance(lim, dict) or lim.get("percent") is None:
                continue
            kind = lim.get("kind")
            if kind == "weekly_scoped":
                model = ((lim.get("scope") or {}).get("model") or {}).get("display_name") or "scoped"
                label = f"7d-{model.lower()}"
            else:
                label = label_for.get(kind, kind or "?")
            windows.append({"window": label,
                            "used_pct": round(float(lim["percent"]), 1),
                            "resets_in_min": _mins_until(lim.get("resets_at")),
                            "severity": lim.get("severity")})
    if not windows:  # fall back to the flat fields if limits[] is absent/empty
        for key, label in (("five_hour", "5h"), ("seven_day", "7d"), ("seven_day_opus", "7d-opus")):
            w = body.get(key)
            if isinstance(w, dict) and w.get("utilization") is not None:
                windows.append({"window": label,
                                "used_pct": round(float(w["utilization"]), 1),
                                "resets_in_min": _mins_until(w.get("resets_at"))})
    return windows


def claude_billing(body: dict) -> dict:
    """Normalize Claude's current usage-credit meter.

    The OAuth endpoint currently returns a cumulative current-period value while
    ``daily`` and ``weekly`` may be null. Preserve those fields explicitly so a
    caller cannot accidentally present the cumulative meter as weekly spend.
    """
    extra = body.get("extra_usage") if isinstance(body.get("extra_usage"), dict) else {}
    spend = body.get("spend") if isinstance(body.get("spend"), dict) else {}
    used = spend.get("used") if isinstance(spend.get("used"), dict) else {}
    limit = spend.get("limit") if isinstance(spend.get("limit"), dict) else {}

    currency = used.get("currency") or extra.get("currency") or limit.get("currency")
    if used.get("amount_minor") is not None:
        used_amount = _major_units(used.get("amount_minor"), used.get("exponent", 2))
    else:
        used_amount = _major_units(extra.get("used_credits"), extra.get("decimal_places", 2))
    if limit.get("amount_minor") is not None:
        limit_amount = _major_units(limit.get("amount_minor"), limit.get("exponent", 2))
    else:
        limit_amount = _major_units(extra.get("monthly_limit"), extra.get("decimal_places", 2))

    return {"kind": "usage_credits",
            "enabled": bool(spend.get("enabled", extra.get("is_enabled", False))),
            "used": used_amount,
            "limit": limit_amount,
            "currency": currency,
            "period": "current",
            "daily": extra.get("daily"),
            "weekly": extra.get("weekly")}


# ---------------------------------------------------------------------------
# codex — ChatGPT subscription (backend token)
# ---------------------------------------------------------------------------

def probe_codex() -> dict:
    auth = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "auth.json"
    if not auth.is_file():
        return _unknown("codex", f"no {auth}")
    try:
        tokens = json.loads(auth.read_text()).get("tokens", {})
        token, acct = tokens.get("access_token"), tokens.get("account_id", "")
    except Exception as e:
        return _unknown("codex", f"unreadable auth.json ({e})")
    if not token:
        return _unknown("codex", "no access_token in auth.json (API-key mode has no plan usage)")
    try:
        body = _get_json(
            "https://chatgpt.com/backend-api/wham/usage",
            {"Authorization": f"Bearer {token}",
             "ChatGPT-Account-Id": acct or "",
             "User-Agent": "parable-usage/1"})
    except urllib.error.HTTPError as e:
        return _unknown("codex", f"HTTP {e.code}"
                        + (" (token stale — run any codex cmd to refresh)" if e.code == 401 else ""))
    except Exception as e:
        return _unknown("codex", f"probe failed ({e})")

    return {"pool": "codex", "status": "ok", "plan": body.get("plan_type"),
            "windows": codex_windows(body), "billing": codex_billing(body)}


def codex_windows(body: dict) -> list[dict]:
    """Normalize ChatGPT windows, deriving cadence from duration.

    ``primary_window`` is not guaranteed to mean five hours: current Pro
    responses can put a 604800-second weekly bucket there with no secondary.
    """
    rl = body.get("rate_limit", {})
    windows = []
    for key, fallback in (("primary_window", "5h"), ("secondary_window", "7d")):
        w = rl.get(key)
        if isinstance(w, dict) and w.get("used_percent") is not None:
            windows.append({"window": _duration_label(w.get("limit_window_seconds"), fallback),
                            "used_pct": round(float(w["used_percent"]), 1),
                            "resets_in_min": _mins_until(w.get("reset_at"))})
    return windows


def codex_billing(body: dict) -> dict:
    """Preserve ChatGPT credit/overage facts without inventing dollar spend.

    Unlike Claude's endpoint, ``wham/usage`` does not publish historical cost.
    Its balance is therefore kept in the provider's native representation.
    """
    credits = body.get("credits") if isinstance(body.get("credits"), dict) else {}
    control = body.get("spend_control") if isinstance(body.get("spend_control"), dict) else {}
    return {"kind": "credits",
            "has_credits": bool(credits.get("has_credits", False)),
            "balance": credits.get("balance"),
            "unlimited": bool(credits.get("unlimited", False)),
            "overage_limit_reached": bool(credits.get("overage_limit_reached", False)),
            "spend_control_reached": bool(control.get("reached", False)),
            "individual_limit": control.get("individual_limit")}


# ---------------------------------------------------------------------------
# cursor — Cursor subscription (API key -> JWT exchange -> dashboard RPC)
# ---------------------------------------------------------------------------

def probe_cursor(env_key: str = "CURSOR_API_KEY") -> dict:
    key = os.environ.get(env_key)
    if not key:
        return _unknown("cursor", f"${env_key} not set")
    base = os.environ.get("CURSOR_API_BASE_URL", "https://api2.cursor.sh")
    try:
        exchanged = _get_json(f"{base}/auth/exchange_user_api_key",
                              {"Authorization": f"Bearer {key}",
                               "Content-Type": "application/json"},
                              data=b"{}")
        access = exchanged.get("accessToken")
        if not access:
            return _unknown("cursor", "key exchange returned no accessToken")
        body = _get_json(f"{base}/aiserver.v1.DashboardService/GetCurrentPeriodUsage",
                         {"Authorization": f"Bearer {access}",
                          "Content-Type": "application/json"},
                         data=b"{}")
    except urllib.error.HTTPError as e:
        return _unknown("cursor", f"HTTP {e.code}")
    except Exception as e:
        return _unknown("cursor", f"probe failed ({e})")

    pu = body.get("planUsage", {})
    limit, remaining = pu.get("limit"), pu.get("remaining")
    if limit in (None, 0):
        return _unknown("cursor", "no included-budget limit in response")
    used_pct = round(100.0 * (1 - (remaining or 0) / limit), 1)
    # Cursor bills an included dollar budget, not a rolling %-window: model it as a
    # single "cycle" window so the brain reads one uniform shape across pools.
    return {"pool": "cursor", "status": "ok", "plan": "cursor",
            "windows": [{"window": "cycle", "used_pct": used_pct,
                         "resets_in_min": _mins_until(body.get("billingCycleEnd")),
                         "remaining_usd": round((remaining or 0) / 100, 2),
                         "limit_usd": round(limit / 100, 2)}]}


# ---------------------------------------------------------------------------
# kimi — Kimi Code subscription (OAuth, via CLIProxyAPI's stored credential)
# ---------------------------------------------------------------------------

KIMI_USAGE_URL = "https://api.kimi.com/coding/v1/usages"


def _kimi_token() -> str | None:
    """Kimi's access token: an explicit override, else CLIProxyAPI's store.

    Kimi Code has no native CLI credential path to fall back to — `parable auth
    login` is the only writer — so the proxy store is the sole source.
    """
    explicit = os.environ.get("PARABLE_KIMI_CRED")
    if explicit:
        path = Path(explicit).expanduser()
        if not path.is_file():
            return None
        try:
            return json.loads(path.read_text()).get("access_token")
        except Exception:
            return None
    return _cliproxy_token("kimi")


def _kimi_pct(limit, remaining) -> float | None:
    """used% from a limit/remaining pair; None when the shape is unusable."""
    try:
        limit, remaining = float(limit), float(remaining)
    except (TypeError, ValueError):
        return None
    if limit <= 0:
        return None
    return round(max(0.0, min(100.0, (1.0 - remaining / limit) * 100.0)), 1)


def _kimi_pair(entry: dict) -> float | None:
    """limit/remaining may sit on the entry or one level down in a detail object."""
    pct = _kimi_pct(entry.get("limit"), entry.get("remaining"))
    if pct is not None:
        return pct
    for value in entry.values():
        if isinstance(value, dict):
            pct = _kimi_pct(value.get("limit"), value.get("remaining"))
            if pct is not None:
                return pct
    return None


def probe_kimi() -> dict:
    token = _kimi_token()
    if not token:
        return _unknown("kimi", f"no kimi-*.json in {_cliproxy_auth_dir()}")
    try:
        body = _get_json(KIMI_USAGE_URL,
                         {"Authorization": f"Bearer {token}",
                          "Accept": "application/json",
                          "User-Agent": "parable-usage/1"})
    except urllib.error.HTTPError as e:
        # Kimi access tokens are short-lived and CLIProxyAPI refreshes them as it
        # proxies, persisting each new one to the auth dir. A 401 outside a running
        # session just means nothing has refreshed the stored token lately — which is
        # harmless, because the probe only informs routing while a session is live.
        return _unknown("kimi", f"HTTP {e.code}" + (
            " (stored token stale; refreshes once a parable session is running —"
            " re-auth only if it persists)" if e.code == 401 else ""))
    except Exception as e:
        return _unknown("kimi", f"probe failed ({e})")

    plan = None
    user = body.get("user")
    if isinstance(user, dict) and isinstance(user.get("membership"), dict):
        plan = user["membership"].get("level")
    return {"pool": "kimi", "status": "ok", "plan": plan,
            "windows": kimi_windows(body), "billing": {}}


def kimi_windows(body: dict) -> list[dict]:
    """Normalize /usages into window dicts.

    Kimi meters two things independently, and the tightest one governs routing:
      * ``usage``    — the billing-cycle (weekly) quota
      * ``limits[]`` — rolling rate windows; ``window.duration`` plus
                       ``window.timeUnit`` give the cadence (duration=300 with
                       TIME_UNIT_MINUTE is the 5-hour session window).

    These field names come from Kimi's undocumented console endpoint, so every
    read is defensive: an unparseable section is skipped, never fatal.
    """
    windows = []

    overall = body.get("usage")
    if isinstance(overall, dict):
        pct = _kimi_pct(overall.get("limit"), overall.get("remaining"))
        if pct is not None:
            windows.append({"window": "cycle", "used_pct": pct,
                            "resets_in_min": _mins_until(overall.get("resetTime")
                                                         or overall.get("reset_time"))})

    for entry in body.get("limits") or []:
        if not isinstance(entry, dict):
            continue
        pct = _kimi_pair(entry)
        if pct is None:
            continue
        win = entry.get("window") if isinstance(entry.get("window"), dict) else {}
        try:
            unit = str(win.get("timeUnit") or win.get("time_unit") or "").upper()
            seconds = int(float(win.get("duration")) * (
                60 if "MINUTE" in unit else
                3600 if "HOUR" in unit else
                86400 if "DAY" in unit else 1))
        except (TypeError, ValueError):
            seconds = None
        windows.append({"window": _duration_label(seconds, "window"), "used_pct": pct,
                        "resets_in_min": _mins_until(entry.get("resetTime")
                                                     or entry.get("reset_time"))})
    return windows


PROBES = {"claude": probe_claude, "codex": probe_codex, "cursor": probe_cursor,
          "kimi": probe_kimi}


def _probe_one(name: str, cursor_env_key: str) -> dict:
    if name == "cursor":
        return probe_cursor(cursor_env_key)
    if name in PROBES:
        return PROBES[name]()
    return _unknown(name, "no such pool")


def _read_cache() -> dict:
    try:
        return json.loads(_CACHE_PATH.read_text())
    except Exception:
        return {}


def _write_cache(cache: dict) -> None:
    try:  # best-effort; a probe must never fail because its cache is unwritable
        tmp = _CACHE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(cache))
        tmp.replace(_CACHE_PATH)
        os.chmod(_CACHE_PATH, 0o600)
    except Exception:
        pass


def probe_all(pools: list[str] | None = None, cursor_env_key: str = "CURSOR_API_KEY",
              ttl: int = CACHE_TTL_SECONDS, use_cache: bool = True) -> list[dict]:
    """Probe each pool, backed by a per-pool short-TTL disk cache. A fresh cached
    entry is returned without hitting the network; on a live-probe failure the last
    good cached read is served (marked cached+stale) rather than reverting to
    unknown — so a throttled endpoint keeps informing routing. Set use_cache=False
    (or ttl=0) to force a live read of every pool."""
    now = time.time()
    cache = _read_cache() if use_cache else {}
    out, dirty = [], False
    for name in (pools or list(PROBES)):
        entry = cache.get(name) if use_cache else None
        if entry and entry.get("_ok") and (now - entry.get("_ts", 0)) < ttl:
            out.append({k: v for k, v in entry.items() if not k.startswith("_")} | {"cached": True})
            continue
        fresh = _probe_one(name, cursor_env_key)
        if fresh.get("status") == "ok":
            cache[name] = {**fresh, "_ts": now, "_ok": True}
            dirty = True
            out.append(fresh)
        elif entry and entry.get("_ok"):
            # live probe failed (e.g. HTTP 429) but we have a prior good read — serve it
            # stale rather than dropping the pool to unknown mid-batch.
            age = round(now - entry.get("_ts", 0))
            out.append({k: v for k, v in entry.items() if not k.startswith("_")}
                       | {"cached": True, "stale_seconds": age, "live_probe": fresh.get("reason")})
        else:
            out.append(fresh)
    if dirty:
        _write_cache(cache)
    return out


def worst_used_pct(pool_report: dict) -> float | None:
    """The tightest window drives routing — a pool is only as free as its most-used window."""
    pcts = [w["used_pct"] for w in pool_report.get("windows", []) if w.get("used_pct") is not None]
    return max(pcts) if pcts else None


def format_report(reports: list[dict]) -> str:
    lines = []
    for r in reports:
        if r["status"] != "ok":
            lines.append(f"  {r['pool']:7} unknown — {r.get('reason', '')}")
            continue
        worst = worst_used_pct(r)
        head = f"  {r['pool']:7} {r.get('plan') or '?':8}"
        wins = "  ".join(
            f"{w['window']}={w['used_pct']:.0f}%"
            + (f"(${w['remaining_usd']:.2f} left)" if "remaining_usd" in w else "")
            + (f"↻{w['resets_in_min']}m" if w.get("resets_in_min") is not None else "")
            for w in r["windows"])
        billing = r.get("billing") or {}
        bill = ""
        if billing.get("kind") == "usage_credits":
            used = billing.get("used")
            currency = billing.get("currency") or ""
            if used is not None:
                symbol = "$" if currency == "USD" else f"{currency} "
                limit = billing.get("limit")
                amount = f"{symbol}{used:,.2f}" + (f"/{symbol}{limit:,.2f}" if limit is not None else "")
                bill = f"  extra={amount} {billing.get('period', 'current')}"
            elif billing.get("enabled"):
                bill = "  extra=enabled"
        elif billing.get("kind") == "credits":
            if billing.get("overage_limit_reached") or billing.get("spend_control_reached"):
                bill = "  credits=BLOCKED"
            elif billing.get("has_credits"):
                balance = billing.get("balance")
                bill = "  credits=available" + (f"({balance})" if balance is not None else "")
            else:
                bill = "  credits=none"
        flag = " ⚠ TIGHT" if worst is not None and worst >= 80 else ""
        if r.get("stale_seconds") is not None:
            flag += f" (cached {r['stale_seconds']}s — live probe: {r.get('live_probe')})"
        elif r.get("cached"):
            flag += " (cached)"
        lines.append(f"{head} {wins}{bill}{flag}")
    return "\n".join(lines) if lines else "  (no pools probed)"


if __name__ == "__main__":
    import sys
    as_json = "--json" in sys.argv
    reports = probe_all()
    if as_json:
        print(json.dumps(reports, indent=1))
    else:
        print("parable usage — live subscription headroom (zero model tokens)")
        print(format_report(reports))
