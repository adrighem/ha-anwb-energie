"""Provider-calculated electricity costs from Kraken measurement statistics.

The ANWB account-cache endpoints return zero for their cost fields, but Kraken's
``measurements`` query returns the amounts the ANWB app shows when it is asked
for ``DAY_INTERVAL`` readings. Each day carries one statistic per cost line in
euro cents including VAT:

- ``CONSUMPTION_COST`` lines hold variable costs. For ``GENERATION`` they are
  negative and include the net-metering refund of energy tax and purchasing
  costs, which Kraken limits to the netted kWh.
- ``STANDING_CHARGE_COST`` lines hold the daily fixed delivery charge, grid fee
  and energy-tax reduction. They are attached to ``CONSUMPTION`` days.

``HOUR_INTERVAL`` readings carry no statistics, and ``MONTH_INTERVAL`` readings
lose all statistics when the requested range includes the current month, so
only daily readings are used.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, tzinfo
from typing import Any, Literal

Direction = Literal["CONSUMPTION", "GENERATION"]

KRAKEN_PAGE_SIZE = 100
KRAKEN_MAX_PAGES = 6

VARIABLE_COST_TYPE = "CONSUMPTION_COST"
STANDING_CHARGE_TYPE = "STANDING_CHARGE_COST"

DAILY_COSTS_QUERY = """
query AnwbDailyCosts(
  $accountNumber: String!
  $startAt: DateTime!
  $endAt: DateTime!
  $timezone: String!
  $direction: ReadingDirectionType!
  $first: Int!
  $after: String
) {
  account(accountNumber: $accountNumber) {
    properties {
      measurements(
        first: $first
        after: $after
        startAt: $startAt
        endAt: $endAt
        timezone: $timezone
        utilityFilters: [
          {
            electricityFilters: {
              readingFrequencyType: DAY_INTERVAL
              readingDirection: $direction
            }
          }
        ]
      ) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            value
            ... on IntervalMeasurementType {
              startAt
            }
            metaData {
              statistics {
                type
                costInclTax {
                  estimatedAmount
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

# Same tolerances as the account-cache usage reconciliation in the coordinator.
_USAGE_ABS_TOLERANCE = 1e-3
_USAGE_REL_TOLERANCE = 1e-6


class KrakenCostError(Exception):
    """Raised when Kraken cost statistics are missing or malformed."""


@dataclass(frozen=True)
class KrakenDailyCost:
    """Provider-calculated costs for one local day, in euro including VAT."""

    day: date
    usage: float
    variable_cost: float
    standing_charge: float
    has_statistics: bool


@dataclass(frozen=True)
class KrakenPeriodCosts:
    """Summed provider costs for a range of local days."""

    usage: float
    variable_cost: float
    standing_charge: float
    days: int


@dataclass(frozen=True)
class KrakenElectricityCosts:
    """Provider costs for the current month and year, through ``covered_through``.

    ``covered_through`` is ``None`` when no day of the year is available yet,
    which can only happen on the first day of January.
    """

    covered_through: date | None
    month_import: KrakenPeriodCosts
    month_export: KrakenPeriodCosts
    year_import: KrakenPeriodCosts
    year_export: KrakenPeriodCosts
    closed_month_import_usage: dict[date, float]
    closed_month_export_usage: dict[date, float]


def _finite(value: Any) -> float | None:
    """Return a finite float, accepting Kraken's decimal strings."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _graphql_error_message(payload: Mapping[str, Any]) -> str | None:
    errors = payload.get("errors")
    if not errors:
        return None
    messages = [
        str(error.get("message"))
        for error in errors
        if isinstance(error, Mapping) and error.get("message")
    ]
    return "; ".join(messages) or "unknown error"


def parse_daily_costs_page(
    payload: Any,
    time_zone: tzinfo,
) -> tuple[list[KrakenDailyCost], str | None]:
    """Parse one ``measurements`` page.

    Returns the parsed days and the cursor for the next page, or ``None`` when
    this was the last page. Days from multiple properties are summed per date.
    """
    if not isinstance(payload, Mapping):
        raise KrakenCostError("Kraken returned a non-object response")

    if message := _graphql_error_message(payload):
        raise KrakenCostError(f"Kraken GraphQL error: {message}")

    account = (payload.get("data") or {}).get("account")
    if not isinstance(account, Mapping):
        raise KrakenCostError("Kraken returned no account")

    properties = account.get("properties")
    if not isinstance(properties, list) or not properties:
        raise KrakenCostError("Kraken returned no properties")

    per_day: dict[date, list[float]] = {}
    with_statistics: dict[date, bool] = {}
    next_cursor: str | None = None
    for prop in properties:
        measurements = prop.get("measurements") if isinstance(prop, Mapping) else None
        if not isinstance(measurements, Mapping):
            raise KrakenCostError("Kraken returned no measurements")

        page_info = measurements.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
            if not cursor:
                raise KrakenCostError("Kraken reported a next page without cursor")
            if len(properties) > 1:
                raise KrakenCostError(
                    "Paginated measurements for multiple properties are unsupported"
                )
            next_cursor = str(cursor)

        for edge in measurements.get("edges") or []:
            node = edge.get("node") if isinstance(edge, Mapping) else None
            if not isinstance(node, Mapping):
                raise KrakenCostError("Kraken returned a malformed measurement")

            usage = _finite(node.get("value"))
            start_at = node.get("startAt")
            if usage is None or not isinstance(start_at, str):
                raise KrakenCostError("Kraken returned a measurement without value")
            try:
                local_day = datetime.fromisoformat(start_at).astimezone(time_zone).date()
            except ValueError as err:
                raise KrakenCostError(f"Invalid measurement start {start_at}") from err

            variable = 0.0
            standing = 0.0
            statistics = (node.get("metaData") or {}).get("statistics") or []
            for statistic in statistics:
                if not isinstance(statistic, Mapping):
                    raise KrakenCostError("Kraken returned a malformed statistic")
                amount = _finite(
                    (statistic.get("costInclTax") or {}).get("estimatedAmount")
                )
                if amount is None:
                    raise KrakenCostError("Kraken returned a statistic without amount")
                if statistic.get("type") == VARIABLE_COST_TYPE:
                    variable += amount / 100.0
                elif statistic.get("type") == STANDING_CHARGE_TYPE:
                    standing += amount / 100.0

            totals = per_day.setdefault(local_day, [0.0, 0.0, 0.0])
            totals[0] += usage
            totals[1] += variable
            totals[2] += standing
            with_statistics[local_day] = (
                with_statistics.get(local_day, True) and bool(statistics)
            )

    days = [
        KrakenDailyCost(
            day=local_day,
            usage=totals[0],
            variable_cost=totals[1],
            standing_charge=totals[2],
            has_statistics=with_statistics[local_day],
        )
        for local_day, totals in sorted(per_day.items())
    ]
    return days, next_cursor


def usage_matches(first: float, second: float) -> bool:
    """Return whether two usage totals are equal within rounding tolerance."""
    return math.isclose(
        first,
        second,
        rel_tol=_USAGE_REL_TOLERANCE,
        abs_tol=_USAGE_ABS_TOLERANCE,
    )


def covered_through(days: Iterable[KrakenDailyCost], start: date) -> date | None:
    """Return the last day of the gap-free run of priced days from ``start``.

    A day with usage needs statistics to count as priced. Days without usage
    are accepted without statistics: they carry no variable cost.
    """
    by_day = {day.day: day for day in days}
    current = start
    last: date | None = None
    while (day := by_day.get(current)) is not None:
        if day.usage != 0 and not day.has_statistics:
            break
        last = current
        current += timedelta(days=1)
    return last


def summarize(
    days: Iterable[KrakenDailyCost],
    start: date,
    end_inclusive: date | None,
) -> KrakenPeriodCosts:
    """Sum the provider costs of ``start`` through ``end_inclusive``."""
    selected = [
        day
        for day in days
        if end_inclusive is not None and start <= day.day <= end_inclusive
    ]
    return KrakenPeriodCosts(
        usage=sum(day.usage for day in selected),
        variable_cost=sum(day.variable_cost for day in selected),
        standing_charge=sum(day.standing_charge for day in selected),
        days=len(selected),
    )


def _first_day(days: Sequence[KrakenDailyCost], year_start: date) -> date:
    in_year = [day.day for day in days if day.day >= year_start]
    return min(in_year) if in_year else year_start


def _usage_per_month(
    days: Iterable[KrakenDailyCost],
    end_exclusive: date,
) -> dict[date, float]:
    totals: dict[date, float] = {}
    for day in days:
        if day.day < end_exclusive:
            month = day.day.replace(day=1)
            totals[month] = totals.get(month, 0.0) + day.usage
    return totals


def build_electricity_costs(
    import_days: Sequence[KrakenDailyCost],
    export_days: Sequence[KrakenDailyCost],
    *,
    year_start: date,
    current_month_start: date,
) -> KrakenElectricityCosts | None:
    """Combine daily provider costs into month and year totals.

    Returns ``None`` when the statistics do not cover every closed day of the
    year: closed months cannot be completed from the hourly fallback, so a
    partial result would silently drop costs.
    """
    # A contract that starts during the year has no earlier days. Start at the
    # first returned day; the caller compares each closed month with the
    # account-cache usage, which exposes days that are missing at the start.
    import_through = covered_through(import_days, _first_day(import_days, year_start))
    # Accounts without solar panels may return no GENERATION days at all. The
    # caller's usage comparison rejects this when the account cache has export.
    export_through = (
        covered_through(export_days, _first_day(export_days, year_start))
        if export_days
        else import_through
    )
    through = (
        min(import_through, export_through)
        if import_through is not None and export_through is not None
        else None
    )

    last_closed_day = current_month_start - timedelta(days=1)
    if last_closed_day >= year_start and (
        through is None or through < last_closed_day
    ):
        return None

    return KrakenElectricityCosts(
        covered_through=through,
        month_import=summarize(import_days, current_month_start, through),
        month_export=summarize(export_days, current_month_start, through),
        year_import=summarize(import_days, year_start, through),
        year_export=summarize(export_days, year_start, through),
        closed_month_import_usage=_usage_per_month(import_days, current_month_start),
        closed_month_export_usage=_usage_per_month(export_days, current_month_start),
    )
