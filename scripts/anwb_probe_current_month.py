"""Current-month HOUR account-cache helpers for the ANWB API probe.

The integration requests the whole local calendar month, including hours that
have not happened yet. These helpers mirror that request and summarise how
the account cache answers for future or unmeasured hours (issue #30).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

MAX_GAP_RANGES = 20
_HOUR = timedelta(hours=1)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_account_cache_datetime(value: datetime) -> str:
    utc_value = _as_utc(value)
    return f"{utc_value.isoformat(timespec='milliseconds').removesuffix('+00:00')}Z"


def account_cache_query_boundaries(now: datetime) -> tuple[str, str, str]:
    """Mirror the coordinator's month start, month end and year start strings."""
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    next_month_start = (
        month_start.replace(year=month_start.year + 1, month=1)
        if month_start.month == 12
        else month_start.replace(month=month_start.month + 1)
    )
    year_start = month_start.replace(month=1)
    return (
        _format_account_cache_datetime(month_start),
        _format_account_cache_datetime(
            _as_utc(next_month_start) - timedelta(milliseconds=1)
        ),
        _format_account_cache_datetime(year_start),
    )


def sum_account_cache_fixed_costs(rows: Iterable[Mapping[str, Any]]) -> float | None:
    """Mirror the integration's ``vasteKosten.total`` sum over HOUR rows.

    Return None when any row has a gap or lacks a finite numeric total, or when
    there are no rows.
    """
    total = 0.0
    seen = False
    for row in rows:
        if row.get("hasGap") is True:
            return None
        fixed = row.get("vasteKosten")
        value = fixed.get("total") if isinstance(fixed, Mapping) else None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        if not math.isfinite(value):
            return None
        total += value
        seen = True
    return total if seen else None


def _parse_start(row: Mapping[str, Any]) -> datetime | None:
    value = row.get("startDate")
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError:
        return None


def _iso(value: datetime) -> str:
    return _format_account_cache_datetime(value)


def _fixed_total(row: Mapping[str, Any]) -> float | None:
    fixed = row.get("vasteKosten")
    value = fixed.get("total") if isinstance(fixed, Mapping) else None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def _has_usage(row: Mapping[str, Any]) -> bool:
    usage = row.get("usage")
    if isinstance(usage, bool) or not isinstance(usage, (int, float)):
        return False
    return math.isfinite(usage) and usage != 0


def gap_ranges(rows: Sequence[Mapping[str, Any]]) -> list[list[str]]:
    """Return [first, last] startDates of hour-contiguous ``hasGap: true`` runs."""
    ranges: list[list[datetime]] = []
    previous: datetime | None = None
    for row in rows:
        start = _parse_start(row)
        if row.get("hasGap") is not True or start is None:
            previous = None
            continue
        if ranges and previous is not None and start - previous == _HOUR:
            ranges[-1][1] = start
        else:
            ranges.append([start, start])
        previous = start
    return [[_iso(first), _iso(last)] for first, last in ranges]


def _round(value: float) -> float:
    return round(value, 8)


def summarize_current_month(
    rows: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    start: str,
    end: str,
) -> dict[str, Any]:
    """Summarise future rows, gaps and fixed costs of a full-month HOUR response."""
    dated = sorted(
        (
            (parsed, row)
            for row in rows
            if isinstance(row, Mapping) and (parsed := _parse_start(row)) is not None
        ),
        key=lambda item: item[0],
    )
    ordered = [row for _, row in dated]
    now_utc = _as_utc(now)
    future = [row for parsed, row in dated if parsed > now_utc]
    gaps = [row for row in ordered if row.get("hasGap") is True]
    with_usage = [parsed for parsed, row in dated if _has_usage(row)]
    fixed = [_fixed_total(row) for row in ordered]
    ranges = gap_ranges(ordered)
    expected_hours = round(
        (
            datetime.fromisoformat(end.replace("Z", "+00:00"))
            - datetime.fromisoformat(start.replace("Z", "+00:00"))
        ).total_seconds()
        / 3600
    )
    helper_value = sum_account_cache_fixed_costs(
        row for row in rows if isinstance(row, Mapping)
    )

    return {
        "query_start": start,
        "query_end": end,
        "expected_hour_count": expected_hours,
        "row_count": len(rows),
        "rows_without_valid_start": len(rows) - len(ordered),
        "rows_after_now": len(future),
        "rows_after_now_with_usage": sum(1 for row in future if _has_usage(row)),
        "rows_after_now_has_gap_true": sum(
            1 for row in future if row.get("hasGap") is True
        ),
        "rows_after_now_vaste_total_sum": _round(
            sum(_fixed_total(row) or 0.0 for row in future)
        ),
        "first_start_date": _iso(dated[0][0]) if dated else None,
        "last_start_date": _iso(dated[-1][0]) if dated else None,
        "last_start_date_with_usage": _iso(with_usage[-1]) if with_usage else None,
        "gap_rows_with_usage": sum(1 for row in gaps if _has_usage(row)),
        "gap_rows_without_usage": sum(1 for row in gaps if not _has_usage(row)),
        "gap_range_count": len(ranges),
        "gap_ranges": ranges[:MAX_GAP_RANGES],
        "gap_ranges_truncated": len(ranges) > MAX_GAP_RANGES,
        "rows_with_invalid_vaste_total": sum(1 for value in fixed if value is None),
        "vaste_total_sum_all_rows": _round(
            sum(value for value in fixed if value is not None)
        ),
        "vaste_total_sum_has_gap_false": _round(
            sum(
                value
                for value, row in zip(fixed, ordered, strict=True)
                if value is not None and row.get("hasGap") is False
            )
        ),
        "sum_account_cache_fixed_costs": (
            None if helper_value is None else _round(helper_value)
        ),
    }
