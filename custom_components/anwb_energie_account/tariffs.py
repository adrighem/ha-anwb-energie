"""Tariff normalization and lookup helpers for ANWB Energie."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from datetime import date, datetime
from typing import Any

from .const import NET_METERING_END_DATE_STR
from .tariff_cache import DailyTariffData, HourlyTariffData, parse_local_date
from .time_utils import (
    _local_date_for_api_datetime,
    _normalize_api_datetime_key,
    _parse_api_datetime,
)

NET_METERING_END_DATE = datetime.fromisoformat(NET_METERING_END_DATE_STR)


def _numeric_tariff_value(value: Any) -> float | None:
    """Return a finite numeric tariff value without treating missing data as zero."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None

    numeric_value = float(value)
    if not math.isfinite(numeric_value):
        return None

    return numeric_value


def _numeric_tariff_map(values: Any) -> dict[str, float]:
    """Return only valid numeric entries from a cached tariff map."""
    if not isinstance(values, dict):
        return {}

    numeric_values: dict[str, float] = {}
    for key, value in values.items():
        numeric_value = _numeric_tariff_value(value)
        if isinstance(key, str) and numeric_value is not None:
            numeric_values[key] = numeric_value

    return numeric_values


def _local_date_key(value: str | None) -> str | None:
    """Return the Home Assistant local date key for an API timestamp."""
    local_date = _local_date_for_api_datetime(value)
    return local_date.isoformat() if local_date is not None else None


def _daily_tariff_data(data_list: list[dict[str, Any]]) -> DailyTariffData:
    """Return normalized all-in and market daily tariff maps."""
    all_in_prices: dict[str, float] = {}
    market_prices: dict[str, float] = {}
    for data in data_list:
        local_date = _local_date_key(data.get("date"))
        values = data.get("values")
        if local_date is None or not isinstance(values, dict):
            continue

        all_in_price = _numeric_tariff_value(values.get("allInPrijs"))
        if all_in_price is not None:
            all_in_prices[local_date] = all_in_price

        market_price = _numeric_tariff_value(values.get("marktprijs"))
        if market_price is not None:
            market_prices[local_date] = market_price

    return DailyTariffData(all_in_prices, market_prices)


def _daily_tariff_map(data_list: list[dict[str, Any]]) -> dict[str, float]:
    """Return numeric tariff values keyed by Home Assistant local date."""
    return dict(_daily_tariff_data(data_list).all_in_prices)


def _is_post_cutoff_hourly(timestamp: str) -> bool:
    dt = _parse_api_datetime(timestamp)
    return dt is not None and dt >= NET_METERING_END_DATE


def _is_post_cutoff_daily(day_str: str) -> bool:
    parsed_day = parse_local_date(day_str)
    return parsed_day is not None and parsed_day >= NET_METERING_END_DATE.date()


def _build_export_price_map(
    all_in_prices: Mapping[str, float],
    market_prices: Mapping[str, float],
    is_after_cutoff: Callable[[str], bool] = _is_post_cutoff_hourly,
) -> dict[str, float]:
    """Return export price map applying net metering cutoff date."""
    export_prices: dict[str, float] = {}
    all_keys = set(all_in_prices) | set(market_prices)
    for key in all_keys:
        source = market_prices if is_after_cutoff(key) else all_in_prices
        price = source.get(key)
        if price is not None:
            export_prices[key] = price
    return export_prices


def _build_daily_export_price_map(
    all_in_prices: Mapping[str, float],
    market_prices: Mapping[str, float],
) -> dict[str, float]:
    """Return daily export price map applying net metering cutoff date."""
    return _build_export_price_map(all_in_prices, market_prices, _is_post_cutoff_daily)


def _hourly_tariff_data(
    data_list: list[dict[str, Any]],
) -> HourlyTariffData:
    """Return normalized all-in and market tariff maps for an API response."""
    all_in_prices: dict[str, float] = {}
    market_prices: dict[str, float] = {}
    for data in data_list:
        timestamp = _normalize_api_datetime_key(data.get("date"))
        values = data.get("values")
        if timestamp is None or not isinstance(values, dict):
            continue

        all_in_price = _numeric_tariff_value(values.get("allInPrijs"))
        if all_in_price is not None:
            all_in_prices[timestamp] = all_in_price

        market_price = _numeric_tariff_value(values.get("marktprijs"))
        if market_price is not None:
            market_prices[timestamp] = market_price

    return HourlyTariffData(all_in_prices, market_prices)


def _closed_prior_month_data(
    data_list: list[dict[str, Any]],
    current_month_start: date,
) -> list[dict[str, Any]]:
    """Return YTD rows before the current month, retaining invalid rows safely."""
    closed_data = []
    for data in data_list:
        local_date = _local_date_for_api_datetime(data.get("startDate"))
        if local_date is None or local_date < current_month_start:
            closed_data.append(data)
    return closed_data
