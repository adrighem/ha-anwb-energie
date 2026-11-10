"""Time calculation and timezone helpers for ANWB Energie."""

from __future__ import annotations

import sys
from datetime import date, datetime, time, timedelta, timezone, tzinfo
from typing import Any

from homeassistant.util import dt as _static_dt_util


class _DtProxy:
    """Proxy dt_util dynamically to respect test harness mocks and reload cycles."""

    def __getattr__(self, name: str) -> Any:
        coord_mod = sys.modules.get("custom_components.anwb_energie_account.coordinator")
        if coord_mod and hasattr(coord_mod, "dt_util"):
            return getattr(coord_mod.dt_util, name)
        ha_util = sys.modules.get("homeassistant.util")
        if ha_util and hasattr(ha_util, "dt"):
            return getattr(ha_util.dt, name)
        return getattr(_static_dt_util, name)


dt_util: Any = _DtProxy()


def _as_utc(value: datetime) -> datetime:
    """Return a timezone-aware UTC datetime."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _as_local(value: datetime) -> datetime:
    """Return a local datetime using Home Assistant's configured timezone."""
    value = _as_utc(value)
    try:
        local_value = dt_util.as_local(value)
    except AttributeError, TypeError, ValueError:
        local_value = None

    if isinstance(local_value, datetime):
        return local_value

    return value.astimezone()


def _parse_api_datetime(value: str | None) -> datetime | None:
    """Parse an ANWB API datetime value as a timezone-aware datetime."""
    if not value:
        return None

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = dt_util.parse_datetime(value)

    if not isinstance(parsed, datetime):
        return None

    return _as_utc(parsed)


def _normalize_api_datetime_key(value: str | None) -> str | None:
    """Normalize an ANWB API datetime to the hourly UTC key used internally."""
    parsed = _parse_api_datetime(value)
    if parsed is None:
        return None

    return parsed.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:00:00.000Z")


def _local_date_for_api_datetime(value: str | None) -> date | None:
    """Return the Home Assistant local date for an ANWB API datetime."""
    parsed = _parse_api_datetime(value)
    if parsed is None:
        return None

    return _as_local(parsed).date()


def _configured_time_zone() -> tzinfo:
    """Return Home Assistant's configured timezone with a safe UTC fallback."""
    configured = getattr(dt_util, "DEFAULT_TIME_ZONE", None)
    return configured if isinstance(configured, tzinfo) else timezone.utc


def _configured_time_zone_name() -> str:
    """Return the configured timezone name used by the tariff cache."""
    configured = _configured_time_zone()
    key = getattr(configured, "key", None)
    return key if isinstance(key, str) else str(configured)


def _format_account_cache_datetime(value: datetime) -> str:
    """Format an account-cache timestamp as an exact UTC boundary."""
    utc_value = _as_utc(value)
    return f"{utc_value.isoformat(timespec='milliseconds').removesuffix('+00:00')}Z"


def _account_cache_query_boundaries(now: datetime) -> tuple[str, str, str]:
    """Return UTC month start/end and year start for account-cache queries."""
    month_start = now.replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    next_month_start = (
        month_start.replace(year=month_start.year + 1, month=1)
        if month_start.month == 12
        else month_start.replace(month=month_start.month + 1)
    )
    year_start = month_start.replace(month=1)

    return (
        _format_account_cache_datetime(month_start),
        _format_account_cache_datetime(_as_utc(next_month_start) - timedelta(milliseconds=1)),
        _format_account_cache_datetime(year_start),
    )


def _provider_tariff_dates_for_local_day(local_day: date) -> tuple[date, ...]:
    """Return UTC calendar dates containing a Home Assistant local day."""
    local_tz = _configured_time_zone()
    start = datetime.combine(local_day, time.min, tzinfo=local_tz).astimezone(timezone.utc)
    end = datetime.combine(
        local_day + timedelta(days=1),
        time.min,
        tzinfo=local_tz,
    ).astimezone(timezone.utc) - timedelta(microseconds=1)
    first_date = start.date()
    last_date = end.date()
    return tuple(
        first_date + timedelta(days=offset) for offset in range((last_date - first_date).days + 1)
    )


def _used_local_dates(data_list: list[dict[str, Any]]) -> set[date]:
    """Return local dates containing non-zero usage that require a tariff."""
    used_dates: set[date] = set()
    for data in data_list:
        if data.get("usage", 0.0) == 0:
            continue
        if local_date := _local_date_for_api_datetime(data.get("startDate")):
            used_dates.add(local_date)
    return used_dates


def _stat_start_datetime(value: Any) -> datetime | None:
    """Return a timezone-aware UTC datetime for a recorder statistic start value."""
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, (float, int)):
        return datetime.fromtimestamp(value, timezone.utc)
    if isinstance(value, str):
        return _parse_api_datetime(value)
    return None
