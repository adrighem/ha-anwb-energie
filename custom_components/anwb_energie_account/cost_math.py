"""Usage and variable cost reconciliation math for ANWB Energie."""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import date
from typing import Any

from .tariffs import (
    _closed_prior_month_data,
    _local_date_key,
    _numeric_tariff_value,
)
from .time_utils import (
    _local_date_for_api_datetime,
    _normalize_api_datetime_key,
)

# DAY and MONTH cache aggregates may round independently. Differences up to one
# thousandth of a kWh/m³ (plus a 1e-6 relative tolerance) are treated as equal.
_USAGE_RECONCILIATION_ABS_TOLERANCE = 1e-3
_USAGE_RECONCILIATION_REL_TOLERANCE = 1e-6


def _usage_and_variable_cost(
    data_list: list[dict[str, Any]],
    price_map: dict[str, float],
    interval_key: Callable[[str | None], str | None] = _normalize_api_datetime_key,
) -> tuple[float, float | None, dict[str, Any]]:
    """Calculate usage and cost, requiring a tariff for every used interval."""
    usage_total = 0.0
    variable_cost = 0.0
    required_intervals = 0
    matched_intervals = 0
    missing_intervals_count = 0
    missing_intervals: list[str] = []

    for data in data_list:
        usage = data.get("usage", 0.0)
        usage_total += usage

        if usage == 0:
            continue

        required_intervals += 1
        timestamp = interval_key(data.get("startDate"))
        price_cents = (
            _numeric_tariff_value(price_map.get(timestamp)) if timestamp is not None else None
        )
        if price_cents is None:
            missing_intervals_count += 1
            if len(missing_intervals) < 5:
                missing_intervals.append(timestamp or str(data.get("startDate") or "<missing>"))
            continue

        matched_intervals += 1
        variable_cost += (usage * price_cents) / 100.0

    complete = matched_intervals == required_intervals
    coverage = {
        "complete": complete,
        "required_intervals": required_intervals,
        "matched_intervals": matched_intervals,
        "missing_intervals_count": missing_intervals_count,
        "missing_intervals": missing_intervals,
    }

    return usage_total, float(variable_cost) if complete else None, coverage


def _reconcile_current_month_variable_cost(
    hourly_usage: float,
    variable_cost: float | None,
    coverage: dict[str, Any],
    monthly_data: list[dict[str, Any]],
    current_month_start: date,
) -> tuple[float | None, dict[str, Any]]:
    """Reject a current-month cost when HOUR and MONTH usage disagree."""
    authoritative_rows = [
        data
        for data in monthly_data
        if _local_date_for_api_datetime(data.get("startDate")) == current_month_start
    ]
    if not authoritative_rows:
        return variable_cost, coverage

    authoritative_usage = sum(data.get("usage", 0.0) for data in authoritative_rows)
    if math.isclose(
        hourly_usage,
        authoritative_usage,
        rel_tol=_USAGE_RECONCILIATION_REL_TOLERANCE,
        abs_tol=_USAGE_RECONCILIATION_ABS_TOLERANCE,
    ):
        return variable_cost, coverage

    return None, _unavailable_tariff_coverage(
        "current_month_usage_incomplete",
        coverage,
    )


def _daily_usage_and_variable_cost(
    data_list: list[dict[str, Any]],
    price_map: dict[str, float],
) -> tuple[float, float | None, dict[str, Any]]:
    """Calculate daily usage cost by matching Home Assistant local dates."""
    return _usage_and_variable_cost(data_list, price_map, _local_date_key)


def _combine_tariff_coverage(
    *coverages: dict[str, Any],
) -> dict[str, Any]:
    """Combine coverage for closed-month daily and current-month hourly data."""
    missing_intervals: list[str] = []
    reasons: list[str] = []
    for coverage in coverages:
        for interval in coverage.get("missing_intervals", []):
            if len(missing_intervals) >= 5:
                break
            missing_intervals.append(str(interval))
        if reason := coverage.get("reason"):
            reasons.append(str(reason))

    combined = {
        "complete": all(coverage.get("complete") is True for coverage in coverages),
        "required_intervals": sum(
            int(coverage.get("required_intervals", 0)) for coverage in coverages
        ),
        "matched_intervals": sum(
            int(coverage.get("matched_intervals", 0)) for coverage in coverages
        ),
        "missing_intervals_count": sum(
            int(coverage.get("missing_intervals_count", 0)) for coverage in coverages
        ),
        "missing_intervals": missing_intervals,
    }
    if reasons:
        combined["reason"] = reasons[0]

    return combined


def _unavailable_tariff_coverage(
    reason: str,
    coverage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return incomplete tariff coverage while preserving known interval counts."""
    unavailable = dict(
        coverage
        or {
            "required_intervals": 0,
            "matched_intervals": 0,
            "missing_intervals_count": 0,
            "missing_intervals": [],
        }
    )
    unavailable["complete"] = False
    unavailable["reason"] = reason
    return unavailable


def _year_to_date_variable_cost(
    authoritative_month_data: list[dict[str, Any]],
    daily_data: list[dict[str, Any]],
    current_month_cost: float | None,
    current_month_coverage: dict[str, Any],
    daily_price_map: dict[str, float],
    current_month_start: date,
    *,
    authoritative_fetch_succeeded: bool,
    daily_fetch_succeeded: bool,
    daily_tariff_succeeded: bool,
) -> tuple[float | None, dict[str, Any]]:
    """Combine reconciled closed-month DAY costs with the hourly current month."""
    if current_month_start.month == 1:
        return current_month_cost, current_month_coverage

    if not authoritative_fetch_succeeded:
        return None, _unavailable_tariff_coverage("year_to_date_usage_unavailable")
    if not daily_fetch_succeeded:
        return None, _unavailable_tariff_coverage("daily_usage_unavailable")

    closed_authoritative_data = _closed_prior_month_data(
        authoritative_month_data,
        current_month_start,
    )
    closed_daily_data = _closed_prior_month_data(
        daily_data,
        current_month_start,
    )
    authoritative_usage = sum(data.get("usage", 0.0) for data in closed_authoritative_data)
    daily_usage = sum(data.get("usage", 0.0) for data in closed_daily_data)

    (
        _,
        closed_cost,
        closed_coverage,
    ) = _daily_usage_and_variable_cost(closed_daily_data, daily_price_map)
    if not math.isclose(
        authoritative_usage,
        daily_usage,
        rel_tol=_USAGE_RECONCILIATION_REL_TOLERANCE,
        abs_tol=_USAGE_RECONCILIATION_ABS_TOLERANCE,
    ):
        closed_cost = None
        closed_coverage = _unavailable_tariff_coverage(
            "daily_usage_incomplete",
            closed_coverage,
        )
    elif (
        not daily_tariff_succeeded
        and not closed_coverage["complete"]
        and closed_coverage["required_intervals"] > 0
    ):
        closed_cost = None
        closed_coverage = _unavailable_tariff_coverage(
            "daily_tariff_unavailable",
            closed_coverage,
        )

    coverage = _combine_tariff_coverage(
        closed_coverage,
        current_month_coverage,
    )
    cost = (
        closed_cost + current_month_cost
        if closed_cost is not None and current_month_cost is not None
        else None
    )
    return cost, coverage
