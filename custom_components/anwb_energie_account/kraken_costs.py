"""Provider-calculated electricity costs from Kraken measurement statistics.

The ANWB account-cache cost fields are populated when a row has
``hasGap: false``; zero was observed on rows with ``hasGap: true``. Kraken's
``measurements`` query returns the amounts the ANWB app shows when it is asked
for ``DAY_INTERVAL`` readings. Each day carries one statistic per cost line in
euro cents including VAT:

- ``CONSUMPTION_COST`` lines hold variable costs. For ``GENERATION`` they are
  negative. Each line carries a ``label``:
  ``GENERATION_SUPPLIER_DYNAMIC_COST`` is the market value of the exported
  kWh, while ``GENERATION_ENERGY_TAX_STEP_1`` and
  ``GENERATION_SUPPLIER_PURCHASING_COST`` are the net-metering refund.
  On the one account probed (2026-10) the refund lines carried kWh equal to
  that month's import, not ``min(import, export)``; an earlier claim that
  Kraken limits the refund to the netted kWh per month was wrong. A month's
  credit can therefore be far larger per exported kWh than any price.
- ``STANDING_CHARGE_COST`` lines hold the daily fixed delivery charge, grid fee
  and energy-tax reduction. They are attached to ``CONSUMPTION`` days.

``HOUR_INTERVAL`` readings carry no statistics, and ``MONTH_INTERVAL`` readings
lose all statistics when the requested range includes the current month, so
only daily readings are used. Requesting a statistic ``unit`` is rejected with
HTTP 400, so only ``label`` is selected. ``LEGACY_DAILY_COSTS_QUERY`` omits
``label`` for a schema that rejects it; the market split is then unknown.
"""

from __future__ import annotations

import logging
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

MARKET_VALUE_LABEL = "GENERATION_SUPPLIER_DYNAMIC_COST"
NET_METERING_REFUND_LABELS = frozenset(
    {"GENERATION_ENERGY_TAX_STEP_1", "GENERATION_SUPPLIER_PURCHASING_COST"}
)
# Consumption labels seen on CONSUMPTION_COST lines; they are not logged.
KNOWN_CONSUMPTION_LABELS = frozenset(
    {
        "SUPPLIER_DYNAMIC_COST",
        "SUPPLIER_PURCHASING_COST",
        "ENERGY_TAX_STEP_1",
        "GRID_FEE",
        "SUPPLIER_FIXED_COST",
        "ENERGY_TAX_REDUCTION",
    }
)

_LOGGER = logging.getLogger(__name__)

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
                label
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
LEGACY_DAILY_COSTS_QUERY = DAILY_COSTS_QUERY.replace("\n                label", "")

# GraphQL error codes that mean the query does not match the schema.
_VALIDATION_ERROR_TYPES = frozenset({"VALIDATION", "GRAPHQL_VALIDATION_FAILED"})

# Same tolerances as the account-cache usage reconciliation in the coordinator.
_USAGE_ABS_TOLERANCE = 1e-3
_USAGE_REL_TOLERANCE = 1e-6


class KrakenCostError(Exception):
    """Raised when Kraken cost statistics are missing or malformed."""


class KrakenSchemaError(KrakenCostError):
    """Raised when Kraken rejects the query as invalid for its schema."""


class KrakenLabelLog:
    """Log each cost label message once per instance, by label name only."""

    def __init__(self) -> None:
        self._seen: set[tuple[int, str]] = set()

    def once(self, level: int, message: str, label: str) -> None:
        """Log ``message`` with ``label`` unless it was logged at this level."""
        if (level, label) not in self._seen:
            self._seen.add((level, label))
            _LOGGER.log(level, message, label)


@dataclass(frozen=True)
class KrakenDailyCost:
    """Provider-calculated costs for one local day, in euro including VAT.

    ``market_value`` and ``net_metering_refund`` split ``variable_cost`` by
    label, with Kraken's sign. ``market_value`` is ``None`` when a day with
    variable costs or usage has no market line (always on ``CONSUMPTION``
    days); both are ``None`` when the labels were not requested.
    """

    day: date
    usage: float
    variable_cost: float
    standing_charge: float
    has_statistics: bool
    market_value: float | None = 0.0
    net_metering_refund: float | None = 0.0


@dataclass(frozen=True)
class KrakenPeriodCosts:
    """Summed provider costs for a range of local days."""

    usage: float
    variable_cost: float
    standing_charge: float
    days: int
    market_value: float | None = 0.0
    net_metering_refund: float | None = 0.0

    @property
    def other_variable_cost(self) -> float | None:
        """Return variable costs whose label is neither market nor refund."""
        if self.market_value is None or self.net_metering_refund is None:
            return None
        return self.variable_cost - self.market_value - self.net_metering_refund


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


def sum_account_cache_fixed_costs(rows: Iterable[Mapping[str, Any]]) -> float | None:
    """Sum ``vasteKosten.total`` over account-cache HOUR rows.

    Return None when any row has a gap or lacks a numeric total, or when there
    are no rows, so the caller falls back to its constants for the whole period.
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


def _is_validation_error(payload: Mapping[str, Any]) -> bool:
    """Return whether any GraphQL error reports a schema validation failure."""
    for error in payload.get("errors") or []:
        extensions = error.get("extensions") if isinstance(error, Mapping) else None
        if isinstance(extensions, Mapping) and (
            extensions.get("errorType") in _VALIDATION_ERROR_TYPES
            or extensions.get("code") in _VALIDATION_ERROR_TYPES
        ):
            return True
    return False


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


def _add_known(first: float | None, second: float | None) -> float | None:
    """Add two amounts; an unknown amount makes the sum unknown."""
    if first is None or second is None:
        return None
    return first + second


@dataclass(frozen=True)
class _DayTotals:
    """Summed cost lines of one or more measurements, in euro."""

    usage: float = 0.0
    variable_cost: float = 0.0
    standing_charge: float = 0.0
    market_value: float | None = 0.0
    net_metering_refund: float | None = 0.0
    has_statistics: bool = True

    def __add__(self, other: _DayTotals) -> _DayTotals:
        return _DayTotals(
            usage=self.usage + other.usage,
            variable_cost=self.variable_cost + other.variable_cost,
            standing_charge=self.standing_charge + other.standing_charge,
            market_value=_add_known(self.market_value, other.market_value),
            net_metering_refund=_add_known(
                self.net_metering_refund, other.net_metering_refund
            ),
            has_statistics=self.has_statistics and other.has_statistics,
        )


def _measurement_totals(
    usage: float,
    statistics: Sequence[Any],
    *,
    labelled: bool,
    label_log: KrakenLabelLog | None,
) -> _DayTotals:
    """Sum one measurement's statistics in euro, split by label.

    A missing or unknown label counts as other. Without a market line the
    market value of a measurement with variable costs or usage is unknown.
    """
    variable = standing = market = refund = 0.0
    has_market_line = False
    for statistic in statistics:
        if not isinstance(statistic, Mapping):
            raise KrakenCostError("Kraken returned a malformed statistic")
        amount = _finite((statistic.get("costInclTax") or {}).get("estimatedAmount"))
        if amount is None:
            raise KrakenCostError("Kraken returned a statistic without amount")
        euro = amount / 100.0
        if statistic.get("type") == STANDING_CHARGE_TYPE:
            standing += euro
            continue
        if statistic.get("type") != VARIABLE_COST_TYPE:
            continue
        variable += euro
        if not labelled:
            continue
        label = statistic.get("label")
        if label is not None and not isinstance(label, str):
            raise KrakenCostError("Kraken returned a non-text statistic label")
        if label == MARKET_VALUE_LABEL:
            has_market_line = True
            market += euro
        elif label in NET_METERING_REFUND_LABELS:
            refund += euro
        elif label not in KNOWN_CONSUMPTION_LABELS and label_log is not None:
            label_log.once(
                logging.DEBUG,
                "Kraken variable cost line with unexpected label %s",
                label or "<missing>",
            )
    split_unknown = variable != 0.0 or usage > 0
    return _DayTotals(
        usage=usage,
        variable_cost=variable,
        standing_charge=standing,
        market_value=None if split_unknown and not has_market_line else market,
        net_metering_refund=None if split_unknown and not labelled else refund,
        has_statistics=bool(statistics),
    )


def parse_daily_costs_page(
    payload: Any,
    time_zone: tzinfo,
    *,
    labelled: bool = True,
    label_log: KrakenLabelLog | None = None,
) -> tuple[list[KrakenDailyCost], str | None]:
    """Parse one ``measurements`` page.

    Returns the parsed days and the cursor for the next page, or ``None`` when
    this was the last page. Days from multiple properties are summed per date.
    ``labelled`` is ``False`` for ``LEGACY_DAILY_COSTS_QUERY`` responses.
    Raises ``KrakenSchemaError`` when Kraken rejects the query itself.
    """
    if not isinstance(payload, Mapping):
        raise KrakenCostError("Kraken returned a non-object response")

    if message := _graphql_error_message(payload):
        if _is_validation_error(payload):
            raise KrakenSchemaError(f"Kraken GraphQL validation error: {message}")
        raise KrakenCostError(f"Kraken GraphQL error: {message}")

    account = (payload.get("data") or {}).get("account")
    if not isinstance(account, Mapping):
        raise KrakenCostError("Kraken returned no account")

    properties = account.get("properties")
    if not isinstance(properties, list) or not properties:
        raise KrakenCostError("Kraken returned no properties")

    per_day: dict[date, _DayTotals] = {}
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

            statistics = (node.get("metaData") or {}).get("statistics") or []
            per_day[local_day] = per_day.get(local_day, _DayTotals()) + (
                _measurement_totals(
                    usage, statistics, labelled=labelled, label_log=label_log
                )
            )

    days = [
        KrakenDailyCost(
            day=local_day,
            usage=totals.usage,
            variable_cost=totals.variable_cost,
            standing_charge=totals.standing_charge,
            has_statistics=totals.has_statistics,
            market_value=totals.market_value,
            net_metering_refund=totals.net_metering_refund,
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
        market_value=_sum_known(day.market_value for day in selected),
        net_metering_refund=_sum_known(day.net_metering_refund for day in selected),
    )


def _sum_known(amounts: Iterable[float | None]) -> float | None:
    total: float | None = 0.0
    for amount in amounts:
        total = _add_known(total, amount)
    return total


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
