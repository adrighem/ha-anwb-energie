#!/usr/bin/env python3
"""Probe ANWB cache cost behavior without storing secrets in tracked files."""

from __future__ import annotations

import argparse
import base64
import getpass
import hashlib
import json
import os
import secrets
import sys
import time
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NoReturn
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

CLIENT_ID = "57fe1448-00e6-47f2-bb50-c0935640b1fa"
TENANT_ID = "49acae90-1d8b-46a5-943a-33da44624219"
REDIRECT_URI = f"https://login.anwb.nl/{TENANT_ID}/login/callback"
AUTHORIZE_URL = f"https://login.anwb.nl/{TENANT_ID}/login/authorize"
TOKEN_URL = f"https://login.anwb.nl/{TENANT_ID}/login/token"
KRAKEN_TOKEN_URL = "https://api.anwb.nl/energy/energy-services/v1/auth/kraken-token"
GRAPHQL_URL = "https://api.anwb-kraken.energy/v1/graphql/"
ACCOUNT_CACHE_BASE = "https://api.anwb.nl/energy/energy-services/v1/accounts"
TARIFF_BASE = "https://api.anwb.nl/energy/energy-services/v2/tarieven"

PROBE_DIR = Path(".anwb-api-probe")
STATE_FILE = PROBE_DIR / "state.json"
TOKEN_FILE = PROBE_DIR / "tokens.json"
REPORT_FILE = PROBE_DIR / "last-report.json"


@dataclass(frozen=True)
class ProbeWindow:
    """A date window to query."""

    name: str
    start: date
    end: date
    intervals: tuple[str, ...]


@dataclass(frozen=True)
class JsonResponse:
    """A parsed response without retaining an unusable upstream body."""

    status: int
    payload: dict[str, Any]
    is_json_object: bool


class ProbeError(RuntimeError):
    """An error with a message that is safe to show to the user."""


class SafeArgumentParser(argparse.ArgumentParser):
    """Avoid reflecting potentially sensitive arguments in parser errors."""

    def error(self, _: str) -> NoReturn:
        self.print_usage(sys.stderr)
        self.exit(2, f"{self.prog}: error: invalid command-line arguments\n")


def _ensure_probe_dir() -> None:
    PROBE_DIR.mkdir(mode=0o700, exist_ok=True)
    try:
        os.chmod(PROBE_DIR, 0o700)
    except OSError:
        pass


def _write_secret_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_probe_dir()
    flags = os.O_WRONLY | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW

    file_descriptor = os.open(path, flags, 0o600)
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(file_descriptor, 0o600)
        os.ftruncate(file_descriptor, 0)
        with os.fdopen(
            file_descriptor,
            "w",
            encoding="utf-8",
            closefd=False,
        ) as secret_file:
            json.dump(payload, secret_file, indent=2, sort_keys=True)
    finally:
        os.close(file_descriptor)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    form: dict[str, str] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: int = 30,
) -> JsonResponse:
    body: bytes | None = None
    req_headers = dict(headers or {})
    if form is not None:
        body = urlencode(form).encode()
        req_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
    if json_body is not None:
        body = json.dumps(json_body).encode()
        req_headers.setdefault("Content-Type", "application/json")

    request = Request(url, data=body, headers=req_headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            response_body = response.read()
    except HTTPError as err:
        status = err.code
        err.close()
        return JsonResponse(status=status, payload={}, is_json_object=False)

    if not response_body:
        return JsonResponse(status=response.status, payload={}, is_json_object=True)

    try:
        payload = json.loads(response_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return JsonResponse(
            status=response.status,
            payload={},
            is_json_object=False,
        )
    if not isinstance(payload, dict):
        return JsonResponse(
            status=response.status,
            payload={},
            is_json_object=False,
        )
    return JsonResponse(
        status=response.status,
        payload=payload,
        is_json_object=True,
    )


def _safe_http_status(status: object) -> int | str:
    if type(status) is int and 100 <= status <= 599:
        return status
    return "unknown"


def _require_json_response(
    response: JsonResponse,
    *,
    operation: str,
    expected_statuses: tuple[int, ...] = (200,),
) -> dict[str, Any]:
    if response.status not in expected_statuses:
        status = _safe_http_status(response.status)
        raise ProbeError(f"{operation} failed with HTTP {status}")
    if not response.is_json_object:
        raise ProbeError(f"{operation} returned an invalid JSON response")
    return response.payload


def _required_credential(
    payload: dict[str, Any],
    key: str,
    *,
    operation: str,
) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ProbeError(f"{operation} response omitted required credentials")
    return value


def _expires_at(
    payload: dict[str, Any],
    key: str,
    *,
    operation: str,
) -> float:
    try:
        expires_in = int(payload.get(key, 0) or 0)
    except (TypeError, ValueError, OverflowError):
        raise ProbeError(f"{operation} response had invalid expiry metadata") from None
    return _utc_now_ts() + expires_in - 60


def _utc_now_ts() -> float:
    return time.time()


def _parse_api_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError:
        return None


def _hour_key(value: str | None) -> str | None:
    parsed = _parse_api_datetime(value)
    if parsed is None:
        return None
    return parsed.replace(minute=0, second=0, microsecond=0).strftime(
        "%Y-%m-%dT%H:00:00.000Z"
    )


def _date_time_start(value: date) -> str:
    return f"{value.isoformat()}T00:00:00.000Z"


def _date_time_end(value: date) -> str:
    return f"{value.isoformat()}T23:59:59.999Z"


def command_login_url(_: argparse.Namespace) -> int:
    """Generate a PKCE login URL and persist the verifier locally."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        .rstrip(b"=")
        .decode()
    )
    state = secrets.token_urlsafe(18)
    _write_secret_json(
        STATE_FILE,
        {
            "code_verifier": verifier,
            "state": state,
            "created_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": "openid profile email offline_access",
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": state,
    }
    print(f"{AUTHORIZE_URL}?{urlencode(params)}")
    print(f"\nSaved PKCE verifier in ignored file: {STATE_FILE}")
    return 0


def _extract_code(callback_url: str) -> tuple[str, str]:
    expected = urlparse(REDIRECT_URI)
    try:
        parsed = urlparse(callback_url)
        expected_port = expected.port
        if expected_port is None:
            expected_port = {"http": 80, "https": 443}.get(expected.scheme)
        callback_port = parsed.port
        if callback_port is None:
            callback_port = {"http": 80, "https": 443}.get(parsed.scheme)
        callback_hostname = parsed.hostname
    except ValueError:
        raise ProbeError("Callback URL does not match the configured redirect URI") from None

    if (
        parsed.scheme != expected.scheme
        or callback_hostname != expected.hostname
        or callback_port != expected_port
        or parsed.path != expected.path
        or parsed.params
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ProbeError("Callback URL does not match the configured redirect URI")

    params = parse_qs(parsed.query, keep_blank_values=True)
    codes = params.get("code", [])
    states = params.get("state", [])
    if len(codes) != 1 or not codes[0].strip():
        raise ProbeError(
            "Callback URL must contain exactly one non-empty code parameter"
        )
    if len(states) != 1 or not states[0].strip():
        raise ProbeError(
            "Callback URL must contain exactly one non-empty state parameter"
        )
    return codes[0], states[0]


def _prompt_callback_url() -> str:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            callback_url = getpass.getpass(
                "Paste the full ANWB callback URL (input hidden): "
            )
    except getpass.GetPassWarning:
        raise ProbeError(
            "Secure callback input is unavailable in this terminal"
        ) from None
    except EOFError:
        raise ProbeError("No callback URL was provided") from None
    except KeyboardInterrupt:
        raise ProbeError("Callback input was cancelled") from None

    callback_url = callback_url.strip()
    if not callback_url:
        raise ProbeError("No callback URL was provided")
    return callback_url


def _save_oauth_token(
    payload: dict[str, Any],
    *,
    operation: str,
) -> dict[str, Any]:
    token = {
        "access_token": _required_credential(
            payload,
            "access_token",
            operation=operation,
        ),
        "refresh_token": (
            payload.get("refresh_token")
            if isinstance(payload.get("refresh_token"), str)
            else None
        ),
        "id_token": (
            payload.get("id_token")
            if isinstance(payload.get("id_token"), str)
            else None
        ),
        "expires_at": _expires_at(
            payload,
            "expires_in",
            operation=operation,
        ),
        "scope": (
            payload.get("scope") if isinstance(payload.get("scope"), str) else None
        ),
        "token_type": (
            payload.get("token_type")
            if isinstance(payload.get("token_type"), str)
            else None
        ),
    }
    return token


def _exchange_callback(callback_url: str) -> dict[str, Any]:
    state = _read_json(STATE_FILE)
    verifier = state.get("code_verifier")
    if not isinstance(verifier, str) or not verifier:
        raise ProbeError(f"Missing PKCE verifier. Run login-url first: {STATE_FILE}")

    code, callback_state = _extract_code(callback_url)
    expected_state = state.get("state")
    if not isinstance(expected_state, str) or not expected_state:
        raise ProbeError(f"Missing login state. Run login-url first: {STATE_FILE}")
    if not secrets.compare_digest(
        callback_state.encode("utf-8"),
        expected_state.encode("utf-8"),
    ):
        raise ProbeError("Callback state does not match the saved login state")

    response = _request_json(
        "POST",
        TOKEN_URL,
        form={
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "code_verifier": verifier,
        },
    )
    payload = _require_json_response(
        response,
        operation="OAuth token exchange",
    )

    tokens = _read_json(TOKEN_FILE)
    tokens["oauth"] = _save_oauth_token(
        payload,
        operation="OAuth token exchange",
    )
    tokens.pop("kraken", None)
    _write_secret_json(TOKEN_FILE, tokens)
    return tokens["oauth"]


def _refresh_oauth_token(tokens: dict[str, Any]) -> dict[str, Any]:
    oauth = tokens.get("oauth") or {}
    refresh_token = oauth.get("refresh_token")
    if not refresh_token:
        raise ProbeError(
            "No refresh token cached. Run login-url, then probe with --callback."
        )

    response = _request_json(
        "POST",
        TOKEN_URL,
        form={
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "refresh_token": refresh_token,
        },
    )
    payload = _require_json_response(
        response,
        operation="OAuth refresh",
    )

    refreshed = _save_oauth_token(
        payload,
        operation="OAuth refresh",
    )
    if not refreshed.get("refresh_token"):
        refreshed["refresh_token"] = refresh_token
    tokens["oauth"] = refreshed
    tokens.pop("kraken", None)
    _write_secret_json(TOKEN_FILE, tokens)
    return refreshed


def _get_oauth_token(callback_url: str | None) -> str:
    if callback_url:
        oauth = _exchange_callback(callback_url)
        return oauth["access_token"]

    tokens = _read_json(TOKEN_FILE)
    oauth = tokens.get("oauth") or {}
    if oauth.get("access_token") and oauth.get("expires_at", 0) > _utc_now_ts():
        return oauth["access_token"]

    oauth = _refresh_oauth_token(tokens)
    return oauth["access_token"]


def _get_kraken_token(access_token: str) -> str:
    tokens = _read_json(TOKEN_FILE)
    kraken = tokens.get("kraken") or {}
    if kraken.get("access_token") and kraken.get("expires_at", 0) > _utc_now_ts():
        return kraken["access_token"]

    response = _request_json(
        "POST",
        KRAKEN_TOKEN_URL,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    payload = _require_json_response(
        response,
        operation="Kraken token exchange",
        expected_statuses=(200, 201),
    )

    tokens["kraken"] = {
        "access_token": _required_credential(
            payload,
            "accessToken",
            operation="Kraken token exchange",
        ),
        "expires_at": _expires_at(
            payload,
            "expiresIn",
            operation="Kraken token exchange",
        ),
    }
    _write_secret_json(TOKEN_FILE, tokens)
    return tokens["kraken"]["access_token"]


def _get_account_number(kraken_token: str) -> str:
    query = """{
      viewer {
        accounts {
          number
          ... on AccountType {
            properties {
              address
            }
          }
        }
      }
    }"""
    response = _request_json(
        "POST",
        GRAPHQL_URL,
        headers={"Authorization": f"Bearer {kraken_token}"},
        json_body={"query": query, "variables": {}},
    )
    payload = _require_json_response(
        response,
        operation="Account GraphQL query",
    )
    if payload.get("errors"):
        raise ProbeError("Account GraphQL query returned errors")

    accounts = (((payload.get("data") or {}).get("viewer") or {}).get("accounts") or [])
    if not accounts:
        raise ProbeError("Account GraphQL query returned no accounts")

    first_account = accounts[0]
    if not isinstance(first_account, dict):
        raise ProbeError("Account GraphQL query returned no usable account")
    account_number = first_account.get("number")
    if not isinstance(account_number, str) or not account_number:
        raise ProbeError("Account GraphQL query returned no usable account")
    return account_number


def _local_today() -> date:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("Europe/Amsterdam")).date()
    return datetime.now().astimezone().date()


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _previous_month(today: date) -> tuple[date, date]:
    first_this_month = _month_start(today)
    last_previous_month = first_this_month - timedelta(days=1)
    return _month_start(last_previous_month), last_previous_month


def _previous_year(today: date) -> tuple[date, date]:
    year = today.year - 1
    return date(year, 1, 1), date(year, 12, 31)


def _probe_windows(include_previous_year: bool) -> list[ProbeWindow]:
    today = _local_today()
    yesterday = today - timedelta(days=1)
    last_week = today - timedelta(days=7)
    previous_month_start, previous_month_end = _previous_month(today)
    windows = [
        ProbeWindow("yesterday", yesterday, yesterday, ("HOUR", "DAY")),
        ProbeWindow("last_week_same_day", last_week, last_week, ("HOUR", "DAY")),
        ProbeWindow(
            "previous_month",
            previous_month_start,
            previous_month_end,
            ("DAY", "MONTH"),
        ),
        ProbeWindow(
            "current_year_closed_months",
            date(today.year, 1, 1),
            previous_month_end,
            ("MONTH",),
        ),
    ]
    if include_previous_year:
        start, end = _previous_year(today)
        windows.append(ProbeWindow("previous_year", start, end, ("MONTH",)))
    return windows


def _fetch_cache(
    account_number: str,
    kraken_token: str,
    endpoint: str,
    interval: str,
    window: ProbeWindow,
) -> JsonResponse:
    contract_start = date(window.start.year, 1, 1)
    url = (
        f"{ACCOUNT_CACHE_BASE}/{account_number}/{endpoint}/cache"
        f"?startDate={_date_time_start(window.start)}"
        f"&endDate={_date_time_end(window.end)}"
        f"&contractStartDate={_date_time_start(contract_start)}"
        f"&interval={interval}"
    )
    return _request_json(
        "GET",
        url,
        headers={"Authorization": f"Bearer {kraken_token}"},
    )


def _fetch_tariff_map(kind: str, start: date, end: date) -> dict[str, float]:
    prices: dict[str, float] = {}
    day = start
    while day <= end:
        url = (
            f"{TARIFF_BASE}/{kind}"
            f"?startDate={_date_time_start(day)}"
            f"&endDate={_date_time_end(day)}"
            "&interval=HOUR"
        )
        response = _request_json("GET", url)
        if response.status == 200 and response.is_json_object:
            for row in response.payload.get("data") or []:
                key = _hour_key(row.get("date"))
                all_in = (row.get("values") or {}).get("allInPrijs")
                if key and all_in is not None:
                    prices[key] = float(all_in)
        day += timedelta(days=1)
    return prices


def _cost_total(row: dict[str, Any], key: str) -> float:
    costs = row.get(key) or {}
    if not isinstance(costs, dict):
        return 0.0
    return float(costs.get("total") or 0)


def _compact_costs(row: dict[str, Any], key: str) -> dict[str, Any]:
    costs = row.get(key) or {}
    if not isinstance(costs, dict):
        return {}
    keep = [
        "dynamischeKosten",
        "energieBelasting1",
        "energieBelasting2",
        "energieBelasting3",
        "inkoopKosten",
        "abonnementsKosten",
        "netbeheerKosten",
        "verminderingEnergieBelasting",
        "total",
    ]
    return {name: costs[name] for name in keep if name in costs}


def _sample_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in rows:
        if _cost_total(row, "variabeleKosten") or _cost_total(row, "vasteKosten"):
            selected.append(row)
            break
    nonzero_usage = [row for row in rows if (row.get("usage") or 0) != 0]
    if nonzero_usage:
        selected.extend([nonzero_usage[0], nonzero_usage[-1]])

    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in selected:
        key = row.get("startDate", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(row)

    samples = []
    for row in unique[:3]:
        samples.append(
            {
                "startDate": row.get("startDate"),
                "endDate": row.get("endDate"),
                "hasGap": row.get("hasGap"),
                "usage": row.get("usage"),
                "variabeleKosten": _compact_costs(row, "variabeleKosten"),
                "vasteKosten": _compact_costs(row, "vasteKosten"),
            }
        )
    return samples


def _summarize_rows(
    endpoint: str,
    interval: str,
    window: ProbeWindow,
    status: int,
    payload: dict[str, Any],
    tariff_map: dict[str, float] | None,
) -> dict[str, Any]:
    rows = payload.get("data") or []
    usage_sum = sum(float(row.get("usage") or 0) for row in rows)
    variable_sum = sum(_cost_total(row, "variabeleKosten") for row in rows)
    fixed_sum = sum(_cost_total(row, "vasteKosten") for row in rows)

    summary: dict[str, Any] = {
        "endpoint": endpoint,
        "interval": interval,
        "window": window.name,
        "start": window.start.isoformat(),
        "end": window.end.isoformat(),
        "http_status": status,
        "outcome": "ok",
        "response_interval": payload.get("interval"),
        "unit": payload.get("unit"),
        "row_count": len(rows),
        "rows_with_usage": sum(1 for row in rows if (row.get("usage") or 0) != 0),
        "rows_with_has_gap_true": sum(1 for row in rows if row.get("hasGap") is True),
        "rows_with_has_gap_false": sum(1 for row in rows if row.get("hasGap") is False),
        "usage_sum": round(usage_sum, 8),
        "variabele_total_sum_raw": variable_sum,
        "vaste_total_sum_raw": fixed_sum,
        "rows_with_nonzero_variabele_total": sum(
            1 for row in rows if _cost_total(row, "variabeleKosten") != 0
        ),
        "rows_with_nonzero_vaste_total": sum(
            1 for row in rows if _cost_total(row, "vasteKosten") != 0
        ),
        "sample_rows": _sample_rows(rows),
    }

    if interval == "HOUR" and tariff_map:
        matched = 0
        missing = 0
        tariff_total = 0.0
        for row in rows:
            usage = float(row.get("usage") or 0)
            key = _hour_key(row.get("startDate"))
            if key in tariff_map:
                matched += 1
                tariff_total += usage * tariff_map[key] / 100
            else:
                missing += 1
        summary["tariff_check"] = {
            "matched_hours": matched,
            "missing_price_hours": missing,
            "usage_x_tariff_sum_eur": round(tariff_total, 8),
        }

    return summary


def _classify(results: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [
        item
        for item in results
        if item.get("window")
        in {"previous_month", "current_year_closed_months", "previous_year"}
    ]
    any_nonzero_cost = any(
        item.get("rows_with_nonzero_variabele_total", 0)
        or item.get("rows_with_nonzero_vaste_total", 0)
        for item in results
    )
    closed_nonzero_cost = any(
        item.get("rows_with_nonzero_variabele_total", 0)
        or item.get("rows_with_nonzero_vaste_total", 0)
        for item in closed
    )
    day_or_month_nonzero = any(
        item.get("interval") in {"DAY", "MONTH"}
        and (
            item.get("rows_with_nonzero_variabele_total", 0)
            or item.get("rows_with_nonzero_vaste_total", 0)
        )
        for item in results
    )
    hourly_nonzero = any(
        item.get("interval") == "HOUR"
        and (
            item.get("rows_with_nonzero_variabele_total", 0)
            or item.get("rows_with_nonzero_vaste_total", 0)
        )
        for item in results
    )
    request_failures = sum(1 for item in results if item.get("outcome") != "ok")

    return {
        "any_cache_cost_fields_nonzero": any_nonzero_cost,
        "closed_period_cache_cost_fields_nonzero": closed_nonzero_cost,
        "day_or_month_cache_cost_fields_nonzero": day_or_month_nonzero,
        "hour_cache_cost_fields_nonzero": hourly_nonzero,
        "request_failures": request_failures,
        "interpretation": (
            "One or more cache requests failed; conclusions are incomplete."
            if request_failures
            else (
                "Cache cost fields were non-zero somewhere; "
                "inspect interval/window details."
                if any_nonzero_cost
                else "Cache cost fields were zero for all probed windows/intervals."
            )
        ),
    }


def command_probe(args: argparse.Namespace) -> int:
    """Run sanitized probes and cache tokens locally."""
    callback_url = _prompt_callback_url() if args.prompt_for_callback else None
    access_token = _get_oauth_token(callback_url)
    kraken_token = _get_kraken_token(access_token)
    account_number = _get_account_number(kraken_token)

    results: list[dict[str, Any]] = []
    tariff_cache: dict[tuple[str, date, date], dict[str, float]] = {}
    endpoints = {
        "electricity": "electricity",
        "production": "electricity",
        "gas": "gas",
    }

    for window in _probe_windows(args.previous_year):
        for endpoint, tariff_kind in endpoints.items():
            for interval in window.intervals:
                response = _fetch_cache(
                    account_number, kraken_token, endpoint, interval, window
                )
                if response.status != 200:
                    results.append(
                        {
                            "endpoint": endpoint,
                            "interval": interval,
                            "window": window.name,
                            "http_status": _safe_http_status(response.status),
                            "outcome": "http_error",
                        }
                    )
                    continue
                if not response.is_json_object:
                    results.append(
                        {
                            "endpoint": endpoint,
                            "interval": interval,
                            "window": window.name,
                            "http_status": _safe_http_status(response.status),
                            "outcome": "invalid_response",
                        }
                    )
                    continue

                tariff_map = None
                if args.tariff_check and interval == "HOUR":
                    key = (tariff_kind, window.start, window.end)
                    if key not in tariff_cache:
                        tariff_cache[key] = _fetch_tariff_map(
                            tariff_kind, window.start, window.end
                        )
                    tariff_map = tariff_cache[key]

                results.append(
                    _summarize_rows(
                        endpoint,
                        interval,
                        window,
                        response.status,
                        response.payload,
                        tariff_map,
                    )
                )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "contains_account_identifiers": False,
        "classification": _classify(results),
        "results": results,
    }
    _ensure_probe_dir()
    REPORT_FILE.write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(json.dumps(report["classification"], indent=2, sort_keys=True))
    print(f"\nWrote sanitized report to ignored file: {REPORT_FILE}")
    print(f"Tokens are cached in ignored file: {TOKEN_FILE}")
    return 0


def command_clear(_: argparse.Namespace) -> int:
    """Remove local probe secrets and report files."""
    for path in (STATE_FILE, TOKEN_FILE, REPORT_FILE):
        try:
            path.unlink()
            print(f"Removed {path}")
        except FileNotFoundError:
            pass
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = SafeArgumentParser(
        description="Probe ANWB cache cost fields across historical granularities."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    login = subparsers.add_parser("login-url", help="Print a PKCE login URL")
    login.set_defaults(func=command_login_url)

    probe = subparsers.add_parser(
        "probe",
        help=(
            "Prompt for a callback URL if requested, cache tokens, and probe "
            "HOUR/DAY/MONTH cache cost behavior"
        ),
    )
    probe.add_argument(
        "--callback",
        dest="prompt_for_callback",
        action="store_true",
        help="Securely prompt for a full ANWB callback URL.",
    )
    probe.add_argument(
        "--previous-year",
        action="store_true",
        help="Also query previous calendar year MONTH rows.",
    )
    probe.add_argument(
        "--no-tariff-check",
        dest="tariff_check",
        action="store_false",
        help="Skip HOUR tariff-derived comparison totals.",
    )
    probe.set_defaults(func=command_probe, tariff_check=True)

    clear = subparsers.add_parser("clear", help="Delete local probe files")
    clear.set_defaults(func=command_clear)

    return parser


def main() -> int:
    """Run the CLI."""
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except ProbeError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    except Exception:  # noqa: BLE001
        print(
            "error: probe failed unexpectedly; response details were not exposed",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
