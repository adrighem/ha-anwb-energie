"""Provider electricity costs and account reconciliation for ANWB Energie."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

from .cost_math import (
    _combine_tariff_coverage,
    _reconcile_current_month_variable_cost,
    _unavailable_tariff_coverage,
    _usage_and_variable_cost,
    _year_to_date_variable_cost,
)
from .export_valuation import (
    ExportCreditBreakdown,
    booked_breakdown,
    booked_market_value,
)
from .kraken_costs import (
    KrakenElectricityCosts,
    KrakenPeriodCosts,
    usage_matches,
)
from .time_utils import _local_date_for_api_datetime


@dataclass(frozen=True)
class _PeriodAmounts:
    """Month and year-to-date amounts in euro with their tariff coverage."""

    month: float | None
    month_coverage: dict[str, Any]
    year: float | None
    year_coverage: dict[str, Any]


@dataclass(frozen=True)
class _MeterRows:
    """Account-cache rows of one electricity direction."""

    hourly: list[dict[str, Any]]
    monthly: list[dict[str, Any]]
    daily: list[dict[str, Any]]
    monthly_authoritative: bool
    daily_fetch_succeeded: bool

    @property
    def authoritative_monthly(self) -> list[dict[str, Any]] | None:
        """Return the MONTH rows when freshly fetched, else ``None``."""
        return self.monthly if self.monthly_authoritative else None


@dataclass(frozen=True)
class _ProviderElectricityCosts:
    """Electricity costs from Kraken statistics plus an hourly calculated tail."""

    import_cost: _PeriodAmounts
    export_credit: _PeriodAmounts
    export_market_value: _PeriodAmounts
    fixed_cost: float
    covered_through: date | None
    export_month_breakdown: ExportCreditBreakdown
    export_year_breakdown: ExportCreditBreakdown


def _electricity_amounts(
    rows: _MeterRows,
    price_map: dict[str, float],
    daily_price_map: dict[str, float],
    provider: _PeriodAmounts | None,
    current_month_start: date,
    *,
    daily_tariff_succeeded: bool,
) -> _PeriodAmounts:
    """Return reconciled month and year amounts, preferring Kraken's.

    Without Kraken amounts, the month is priced hourly and closed months daily.
    A month rejected by the reconciliation also invalidates its year total.
    """
    usage, month, month_coverage = _usage_and_variable_cost(rows.hourly, price_map)
    if provider is not None:
        month, month_coverage = provider.month, provider.month_coverage
    month, month_coverage = _reconcile_current_month_variable_cost(
        usage, month, month_coverage, rows.monthly, current_month_start
    )
    if provider is None:
        year, year_coverage = _year_to_date_variable_cost(
            rows.monthly,
            rows.daily,
            month,
            month_coverage,
            daily_price_map,
            current_month_start,
            authoritative_fetch_succeeded=rows.monthly_authoritative,
            daily_fetch_succeeded=rows.daily_fetch_succeeded,
            daily_tariff_succeeded=daily_tariff_succeeded,
        )
    elif month is not None:
        year, year_coverage = provider.year, provider.year_coverage
    else:
        year, year_coverage = None, month_coverage
    return _PeriodAmounts(month, month_coverage, year, year_coverage)


def _cache_usage_per_closed_month(
    data_list: list[dict[str, Any]],
    current_month_start: date,
) -> dict[date, float] | None:
    """Sum account-cache MONTH rows per closed local month."""
    totals: dict[date, float] = {}
    for data in data_list:
        local_date = _local_date_for_api_datetime(data.get("startDate"))
        if local_date is None:
            return None
        month = local_date.replace(day=1)
        if month < current_month_start:
            totals[month] = totals.get(month, 0.0) + data.get("usage", 0.0)
    return totals


def _months_match(
    cache_usage: dict[date, float],
    provider_usage: dict[date, float],
) -> bool:
    return all(
        usage_matches(cache_usage.get(month, 0.0), provider_usage.get(month, 0.0))
        for month in set(cache_usage) | set(provider_usage)
    )


def _split_at_provider_coverage(
    data_list: list[dict[str, Any]],
    covered_through: date | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split hourly rows into days covered by Kraken and the remaining tail."""
    covered: list[dict[str, Any]] = []
    tail: list[dict[str, Any]] = []
    for data in data_list:
        local_date = _local_date_for_api_datetime(data.get("startDate"))
        if covered_through is not None and local_date is not None and local_date <= covered_through:
            covered.append(data)
        else:
            tail.append(data)
    return covered, tail


def _provider_coverage(
    days: int,
    covered_through: date | None,
    tail_coverage: dict[str, Any],
) -> dict[str, Any]:
    provider = {
        "complete": True,
        "required_intervals": days,
        "matched_intervals": days,
        "missing_intervals_count": 0,
        "missing_intervals": [],
    }
    coverage = _combine_tariff_coverage(provider, tail_coverage)
    coverage["provider_costs_through"] = (
        covered_through.isoformat() if covered_through is not None else None
    )
    return coverage


def _provider_electricity_costs(
    provider: KrakenElectricityCosts,
    *,
    import_data: list[dict[str, Any]],
    export_data: list[dict[str, Any]],
    import_year_data: list[dict[str, Any]] | None,
    export_year_data: list[dict[str, Any]] | None,
    price_map: dict[str, float],
    export_price_map: dict[str, float],
    market_price_map: dict[str, float],
    current_month_start: date,
) -> _ProviderElectricityCosts | str:
    """Combine Kraken daily costs with hourly costs for uncovered intervals.

    Returns a reason string when the provider data does not match the
    account-cache usage, so the caller can keep the calculated estimates.
    ``*_year_data`` is ``None`` when the MONTH request failed; the closed-month
    comparison is then skipped and Kraken is trusted on its own.
    """
    for year_data, provider_usage in (
        (import_year_data, provider.closed_month_import_usage),
        (export_year_data, provider.closed_month_export_usage),
    ):
        if year_data is None:
            continue
        cache_usage = _cache_usage_per_closed_month(year_data, current_month_start)
        if cache_usage is None:
            return "account_cache_month_invalid"
        if not _months_match(cache_usage, provider_usage):
            return "closed_month_usage_mismatch"

    through = provider.covered_through
    covered_import, tail_import = _split_at_provider_coverage(import_data, through)
    covered_export, tail_export = _split_at_provider_coverage(export_data, through)
    if not usage_matches(
        sum(data.get("usage", 0.0) for data in covered_import),
        provider.month_import.usage,
    ) or not usage_matches(
        sum(data.get("usage", 0.0) for data in covered_export),
        provider.month_export.usage,
    ):
        return "current_month_usage_mismatch"

    _, tail_import_cost, tail_import_coverage = _usage_and_variable_cost(tail_import, price_map)
    tail_kwh, tail_export_cost, tail_export_coverage = _usage_and_variable_cost(
        tail_export, export_price_map
    )
    _, tail_market_value, tail_market_coverage = _usage_and_variable_cost(
        tail_export, market_price_map
    )

    def _with_tail(provider_amount: float, tail: float | None) -> float | None:
        return provider_amount + tail if tail is not None else None

    def _amounts(
        value: Callable[[KrakenPeriodCosts], float | None],
        month: KrakenPeriodCosts,
        year: KrakenPeriodCosts,
        tail_coverage: dict[str, Any],
    ) -> _PeriodAmounts:
        def coverage(period: KrakenPeriodCosts) -> dict[str, Any]:
            result = _provider_coverage(period.days, through, tail_coverage)
            # With a fully priced tail, only an unknown market split is missing.
            if value(period) is None and result["complete"]:
                return _unavailable_tariff_coverage("kraken_market_split_unknown", result)
            return result

        return _PeriodAmounts(value(month), coverage(month), value(year), coverage(year))

    # Kraken reports generation as negative costs; sensors expose a credit.
    return _ProviderElectricityCosts(
        import_cost=_amounts(
            lambda period: _with_tail(period.variable_cost, tail_import_cost),
            provider.month_import,
            provider.year_import,
            tail_import_coverage,
        ),
        export_credit=_amounts(
            lambda period: _with_tail(-period.variable_cost, tail_export_cost),
            provider.month_export,
            provider.year_export,
            tail_export_coverage,
        ),
        export_market_value=_amounts(
            lambda period: booked_market_value(period, tail_market_value),
            provider.month_export,
            provider.year_export,
            tail_market_coverage,
        ),
        fixed_cost=(provider.month_import.standing_charge + provider.month_export.standing_charge),
        covered_through=through,
        export_month_breakdown=booked_breakdown(
            provider.month_export,
            unbooked_kwh=tail_kwh,
            unbooked_credit=tail_export_cost,
        ),
        export_year_breakdown=booked_breakdown(
            provider.year_export,
            unbooked_kwh=tail_kwh,
            unbooked_credit=tail_export_cost,
        ),
    )
