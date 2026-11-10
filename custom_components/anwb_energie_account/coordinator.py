"""Coordinator for ANWB Energie Account."""

from __future__ import annotations

import asyncio
import calendar
import functools
import logging
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any

from aiohttp.client_exceptions import ClientError, ClientResponseError
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    statistics_during_period,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CURRENCY_EURO, UnitOfEnergy, UnitOfVolume
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .api import AsyncConfigEntryAuth
from .const import (
    DOMAIN,
    GRAPHQL_URL,
    KRAKEN_TOKEN_URL,
    NETBEHEERKOSTEN,
    NETBEHEERKOSTEN_GAS,
    VASTE_LEVERINGSKOSTEN,
    VASTE_LEVERINGSKOSTEN_GAS,
    VERMINDERING_ENERGIEBELASTING,
    VERMINDERING_ENERGIEBELASTING_GAS,
)
from .cost_math import (
    _USAGE_RECONCILIATION_ABS_TOLERANCE,
    _USAGE_RECONCILIATION_REL_TOLERANCE,
    _combine_tariff_coverage,
    _daily_usage_and_variable_cost,
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
    DAILY_COSTS_QUERY,
    KRAKEN_MAX_PAGES,
    KRAKEN_PAGE_SIZE,
    LEGACY_DAILY_COSTS_QUERY,
    MARKET_VALUE_LABEL,
    Direction,
    KrakenAuthError,
    KrakenCostError,
    KrakenDailyCost,
    KrakenElectricityCosts,
    KrakenLabelLog,
    KrakenPeriodCosts,
    KrakenSchemaError,
    build_electricity_costs,
    is_auth_error_payload,
    parse_daily_costs_page,
    sum_account_cache_fixed_costs,
    usage_matches,
)
from .tariff_cache import (
    Commodity,
    DailyTariffData,
    HourlyTariffData,
    TariffCache,
)
from .tariffs import (
    NET_METERING_END_DATE,
    _build_daily_export_price_map,
    _build_export_price_map,
    _closed_prior_month_data,
    _daily_tariff_data,
    _daily_tariff_map,
    _hourly_tariff_data,
    _is_post_cutoff_daily,
    _is_post_cutoff_hourly,
    _local_date_key,
    _numeric_tariff_map,
    _numeric_tariff_value,
)
from .time_utils import (
    _account_cache_query_boundaries,
    _as_local,
    _as_utc,
    _configured_time_zone,
    _configured_time_zone_name,
    _format_account_cache_datetime,
    _local_date_for_api_datetime,
    _normalize_api_datetime_key,
    _parse_api_datetime,
    _provider_tariff_dates_for_local_day,
    _stat_start_datetime,
    _used_local_dates,
)

_LOGGER = logging.getLogger(__name__)

_DNS_FAILURE_MARKERS = (
    "dns",
    "getaddrinfo failed",
    "name or service not known",
    "name resolution",
    "nodename nor servname",
    "temporary failure in name resolution",
)


class KrakenTokenExpired(UpdateFailed):
    """Raised when the Kraken authentication token is expired or invalid."""


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


class ANWBBaseCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Base coordinator to manage ANWB token and account."""

    def __init__(
        self,
        hass: HomeAssistant,
        logger: logging.Logger,
        name: str,
        update_interval: timedelta,
        auth: AsyncConfigEntryAuth,
        config_entry: ConfigEntry,
        tariff_cache: TariffCache | None = None,
    ) -> None:
        """Initialize coordinator."""
        self.config_entry = config_entry
        super().__init__(
            hass,
            logger,
            name=name,
            update_interval=update_interval,
            config_entry=config_entry,
        )
        self.auth = auth
        self.tariff_cache = tariff_cache or TariffCache(
            None,
            _configured_time_zone_name(),
        )
        self._kraken_token: str | None = None
        self._account_number: str | None = None
        self._account_address: str | None = None
        self.last_successful_update: datetime | None = None

    def _can_use_cached_data_for_dns_failure(self, err: Exception) -> bool:
        """Return whether a DNS failure can reuse cached data."""
        if (
            self.data is None
            or self.last_successful_update is None
            or not isinstance(err, UpdateFailed)
            or not self._is_dns_failure(err)
        ):
            return False

        now = dt_util.utcnow()
        last_success = self.last_successful_update
        if now.tzinfo is None and last_success.tzinfo is not None:
            now = now.replace(tzinfo=last_success.tzinfo)
        elif now.tzinfo is not None and last_success.tzinfo is None:
            last_success = last_success.replace(tzinfo=now.tzinfo)

        return now - last_success <= timedelta(hours=24)

    @staticmethod
    def _is_dns_failure(err: Exception) -> bool:
        """Return whether an exception chain looks like a DNS failure."""
        current: BaseException | None = err
        while current is not None:
            message = str(current).lower()
            if any(marker in message for marker in _DNS_FAILURE_MARKERS):
                return True
            current = current.__cause__ or current.__context__
        return False

    async def _async_get_kraken_token(self) -> str:
        """Get or refresh kraken token."""
        try:
            access_token = await self.auth.async_get_access_token()
        except ClientError as err:
            if getattr(err, "status", None) in (400, 401, 403):
                raise ConfigEntryAuthFailed("Failed to get access token") from err
            raise UpdateFailed(f"Network error fetching access token: {err}") from err

        session = self.auth.websession
        headers = {"Authorization": f"Bearer {access_token}"}

        try:
            async with session.post(KRAKEN_TOKEN_URL, headers=headers) as resp:
                if resp.status in (401, 403):
                    raise ConfigEntryAuthFailed("Failed to get kraken token")
                resp.raise_for_status()
                data = await resp.json()
                return data["accessToken"]
        except ClientError as err:
            if getattr(err, "status", None) in (401, 403):
                raise ConfigEntryAuthFailed("Failed to get kraken token") from err
            raise UpdateFailed(f"Failed to fetch Kraken token: {err}") from err

    async def _async_get_account_info(self, kraken_token: str) -> dict[str, str]:
        """Get account number and address."""
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
        session = self.auth.websession
        headers = {
            "Authorization": f"Bearer {kraken_token}",
            "Content-Type": "application/json",
        }

        try:
            async with session.post(
                GRAPHQL_URL, json={"query": query, "variables": {}}, headers=headers
            ) as resp:
                if resp.status in (401, 403):
                    raise KrakenTokenExpired(
                        "Kraken token expired or invalid fetching account number"
                    )
                resp.raise_for_status()
                data = await resp.json()
                errors = data.get("errors") or []
                if errors:
                    if is_auth_error_payload(data):
                        raise KrakenTokenExpired(
                            "Kraken token expired or invalid fetching account number"
                        )

                    messages = [
                        str(error.get("message"))
                        for error in errors
                        if isinstance(error, dict) and error.get("message")
                    ]
                    message = "; ".join(messages) if messages else "unknown error"
                    raise UpdateFailed(f"GraphQL error fetching account number: {message}")

                payload = data.get("data") or {}
                viewer = payload.get("viewer") or {}
                accounts = viewer.get("accounts", [])
                if not accounts:
                    raise UpdateFailed("No active accounts found")

                account = accounts[0]
                number = account.get("number", "unknown")
                address = ""
                properties = account.get("properties", [])
                if properties and isinstance(properties, list) and properties[0].get("address"):
                    address = properties[0]["address"]

                return {"number": number, "address": address}
        except ClientError as err:
            if getattr(err, "status", None) in (401, 403):
                raise KrakenTokenExpired(
                    "Kraken token expired or invalid fetching account number"
                ) from err
            raise UpdateFailed(f"Failed to fetch account number: {err}") from err

    async def _async_fetch_data(
        self,
        url: str,
        kraken_token: str | None,
    ) -> dict[str, Any]:
        """Fetch JSON data from API."""
        session = self.auth.websession
        headers = {"Authorization": f"Bearer {kraken_token}"} if kraken_token is not None else None
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status in (401, 403):
                    if kraken_token is not None:
                        raise KrakenTokenExpired(
                            f"Kraken token expired or invalid fetching data from {url}"
                        )
                    raise UpdateFailed(f"Public tariff request was unauthorized for {url}")
                if resp.status == 404:
                    return {}
                resp.raise_for_status()
                return await resp.json()
        except ClientError as err:
            if getattr(err, "status", None) in (401, 403):
                if kraken_token is not None:
                    raise KrakenTokenExpired(
                        f"Kraken token expired or invalid fetching data from {url}"
                    ) from err
                raise UpdateFailed(f"Public tariff request was unauthorized for {url}") from err
            raise UpdateFailed(f"Error fetching data from {url}: {err}") from err

    async def _async_fetch_hourly_tariffs(
        self,
        commodity: Commodity,
        local_day: date,
    ) -> HourlyTariffData:
        """Fetch one Home Assistant local day of hourly public tariffs."""
        provider_days = _provider_tariff_dates_for_local_day(local_day)
        first_day = min(provider_days)
        # ANWB's hourly endpoint returns null for the last hour of any requested
        # window, so request one trailing day beyond what is needed.
        last_day = max(provider_days) + timedelta(days=1)
        day_start = f"{first_day.isoformat()}T00:00:00.000Z"
        day_end = f"{last_day.isoformat()}T23:59:59.999Z"
        url = (
            "https://api.anwb.nl/energy/energy-services/v2/tarieven/"
            f"{commodity}?startDate={day_start}&endDate={day_end}&interval=HOUR"
        )
        response = await self._async_fetch_data(url, None)
        return _hourly_tariff_data(response.get("data", []) or [])

    async def _async_fetch_daily_tariffs(
        self,
        commodity: Commodity,
        missing_dates: frozenset[date],
    ) -> DailyTariffData:
        """Fetch missing local DAY tariffs below the provider's price limit."""
        if not missing_dates:
            return DailyTariffData({}, {})

        provider_days = {
            provider_day
            for local_day in missing_dates
            for provider_day in _provider_tariff_dates_for_local_day(local_day)
        }
        # DAY responses omit the start label, so include one leading provider day
        # and one trailing provider day to ensure full interval coverage.
        first_day = min(provider_days) - timedelta(days=1)
        last_day = max(provider_days) + timedelta(days=1)
        all_in_prices: dict[str, float] = {}
        market_prices: dict[str, float] = {}
        while first_day <= last_day:
            # Include padding in the 190-day window, below the 200-price cap.
            window_end = min(first_day + timedelta(days=189), last_day)
            day_start = f"{first_day.isoformat()}T00:00:00.000Z"
            day_end = f"{window_end.isoformat()}T23:59:59.999Z"
            url = (
                "https://api.anwb.nl/energy/energy-services/v2/tarieven/"
                f"{commodity}?startDate={day_start}&endDate={day_end}&interval=DAY"
            )
            response = await self._async_fetch_data(url, None)
            tariffs = _daily_tariff_data(response.get("data", []) or [])
            all_in_prices.update(tariffs.all_in_prices)
            market_prices.update(tariffs.market_prices)
            if window_end == last_day:
                break
            # Overlap boundary labels so each window retains its padding.
            first_day = window_end - timedelta(days=1)

        return DailyTariffData(all_in_prices, market_prices)


class ANWBPricingCoordinator(ANWBBaseCoordinator):
    """Coordinator to fetch pricing data smartly."""

    def __init__(
        self,
        hass: HomeAssistant,
        auth: AsyncConfigEntryAuth,
        config_entry: ConfigEntry,
        tariff_cache: TariffCache | None = None,
        gas_applicable: Callable[[], bool] | None = None,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_pricing",
            update_interval=timedelta(minutes=30),
            auth=auth,
            config_entry=config_entry,
            tariff_cache=tariff_cache,
        )
        self._gas_applicable = gas_applicable or (lambda: True)

    async def _async_update_data(self) -> dict[str, Any]:
        try:
            data = await self._async_update_data_internal()
            self.last_successful_update = dt_util.utcnow()
            return data
        except Exception as err:
            if isinstance(err, ConfigEntryAuthFailed):
                raise
            if isinstance(err, KrakenTokenExpired):
                _LOGGER.debug("Kraken token expired, refreshing and retrying")
                self._kraken_token = None
                try:
                    data = await self._async_update_data_internal()
                    self.last_successful_update = dt_util.utcnow()
                    return data
                except Exception as retry_err:
                    if isinstance(retry_err, ConfigEntryAuthFailed):
                        raise
                    if self.data is not None:
                        _LOGGER.warning(
                            "Update failed after token refresh, using cached data: %s",
                            retry_err,
                        )
                        return self.data
                    raise

            if self.data is not None:
                _LOGGER.warning("Update failed, using cached data: %s", err)
                return self.data
            raise

    async def _async_update_data_internal(self) -> dict[str, Any]:
        if not self._kraken_token:
            self._kraken_token = await self._async_get_kraken_token()

        if not self._account_number:
            info = await self._async_get_account_info(self._kraken_token)
            self._account_number = info["number"]
            self._account_address = info["address"]

        now = _as_utc(dt_util.utcnow())
        local_now = _as_local(now)
        today = local_now.date()
        tomorrow = today + timedelta(days=1)
        gas_applicable = self._gas_applicable()

        has_today_electricity = False
        has_today_market_price = False
        has_today_gas = False
        has_tomorrow_electricity = False
        has_tomorrow_market_price = False
        has_tomorrow_gas = False

        if self.data:
            prices = self.data.get("prices_today", {})
            market_prices = self.data.get("market_prices_today", {})
            gas_prices = self.data.get("gas_prices_today", {})
            has_today_electricity = self.tariff_cache.hourly_prices_are_complete(
                today,
                prices,
            )
            has_today_market_price = self.tariff_cache.hourly_prices_are_complete(
                today,
                market_prices,
            )
            has_today_gas = self.tariff_cache.hourly_prices_are_complete(
                today,
                gas_prices,
            )
            has_tomorrow_electricity = self.tariff_cache.hourly_prices_are_complete(
                tomorrow,
                prices,
            )
            has_tomorrow_market_price = self.tariff_cache.hourly_prices_are_complete(
                tomorrow,
                market_prices,
            )
            has_tomorrow_gas = self.tariff_cache.hourly_prices_are_complete(
                tomorrow,
                gas_prices,
            )

        fetch_electricity = (
            not has_today_electricity
            or not has_today_market_price
            or (
                local_now.hour >= 13
                and (not has_tomorrow_electricity or not has_tomorrow_market_price)
            )
        )
        fetch_gas = gas_applicable and (
            not has_today_gas or (local_now.hour >= 6 and not has_tomorrow_gas)
        )

        if not self.data:
            fetch_electricity = True
            fetch_gas = gas_applicable
        elif "market_prices_today" not in self.data:
            fetch_electricity = True

        if not fetch_electricity and not fetch_gas and self.data:
            data = dict(self.data)
            if not gas_applicable:
                data["gas_prices_today"] = {}
                data["_raw_gas_price_map"] = {}
            return data

        price_map = _numeric_tariff_map(self.data.get("_raw_price_map", {}) if self.data else {})
        market_price_map = _numeric_tariff_map(
            self.data.get("_raw_market_price_map", {}) if self.data else {}
        )
        gas_price_map = (
            _numeric_tariff_map(self.data.get("_raw_gas_price_map", {}) if self.data else {})
            if gas_applicable
            else {}
        )

        electricity_days = [today] if fetch_electricity else []
        if fetch_electricity and local_now.hour >= 13:
            electricity_days.append(tomorrow)

        gas_days = [today] if fetch_gas else []
        if fetch_gas and local_now.hour >= 6:
            gas_days.append(tomorrow)

        price_tasks = [
            self.tariff_cache.async_get_hourly_day(
                "electricity",
                local_day,
                lambda local_day=local_day: self._async_fetch_hourly_tariffs(
                    "electricity",
                    local_day,
                ),
            )
            for local_day in electricity_days
        ]
        gas_price_tasks = [
            self.tariff_cache.async_get_hourly_day(
                "gas",
                local_day,
                lambda local_day=local_day: self._async_fetch_hourly_tariffs(
                    "gas",
                    local_day,
                ),
            )
            for local_day in gas_days
        ]

        prices_results, gas_prices_results = await asyncio.gather(
            asyncio.gather(*price_tasks, return_exceptions=True),
            asyncio.gather(*gas_price_tasks, return_exceptions=True),
        )

        for local_day, result in zip(electricity_days, prices_results, strict=True):
            if isinstance(result, Exception):
                if isinstance(result, KrakenTokenExpired):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch part of the electricity tariff window: %s",
                    result,
                )
                result = await self.tariff_cache.async_get_cached_hourly_day(
                    "electricity", local_day
                )
            price_map.update(result.all_in_prices)
            market_price_map.update(result.market_prices)

        for result in gas_prices_results:
            if isinstance(result, Exception):
                if isinstance(result, KrakenTokenExpired):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch part of the gas tariff window: %s",
                    result,
                )
                continue
            gas_price_map.update(result.all_in_prices)

        await self.tariff_cache.async_prune()

        filtered_prices = {
            k: v
            for k, v in price_map.items()
            if _local_date_for_api_datetime(k) in (today, tomorrow)
        }

        filtered_market_prices = {
            k: v
            for k, v in market_price_map.items()
            if _local_date_for_api_datetime(k) in (today, tomorrow)
        }

        filtered_gas_prices = {
            k: v
            for k, v in gas_price_map.items()
            if _local_date_for_api_datetime(k) in (today, tomorrow)
        }

        return {
            "account_number": self._account_number,
            "account_address": self._account_address,
            "prices_today": filtered_prices,
            "market_prices_today": filtered_market_prices,
            "gas_prices_today": filtered_gas_prices,
            "_raw_price_map": price_map,
            "_raw_market_price_map": market_price_map,
            "_raw_gas_price_map": gas_price_map,
        }


class ANWBConsumptionCoordinator(ANWBBaseCoordinator):
    """Class to manage fetching ANWB Energie Account consumption data."""

    def __init__(
        self,
        hass: HomeAssistant,
        auth: AsyncConfigEntryAuth,
        config_entry: ConfigEntry,
        tariff_cache: TariffCache | None = None,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_consumption",
            update_interval=timedelta(hours=6),
            auth=auth,
            config_entry=config_entry,
            tariff_cache=tariff_cache,
        )
        # Last complete Kraken daily costs: (year start, import days, export days).
        # Closed days do not change, so a transient failure can reuse them.
        self._kraken_daily_costs: (
            tuple[date, list[KrakenDailyCost], list[KrakenDailyCost]] | None
        ) = None
        # Set once Kraken rejects the labelled query, for this coordinator's life.
        self._kraken_legacy_query = False
        self._kraken_label_log = KrakenLabelLog()

    async def _async_fetch_kraken_daily_costs(
        self,
        direction: Direction,
        start: date,
        end_exclusive: date,
    ) -> list[KrakenDailyCost]:
        """Fetch Kraken daily costs, falling back to the query without labels."""
        if not self._kraken_legacy_query:
            try:
                days = await self._async_fetch_kraken_pages(
                    direction, start, end_exclusive, labelled=True
                )
            except (ClientResponseError, KrakenSchemaError) as err:
                if isinstance(err, ClientResponseError) and err.status != 400:
                    raise
                if not self._kraken_legacy_query:  # The other direction may race.
                    self._kraken_legacy_query = True
                    _LOGGER.warning(
                        "Kraken rejected the cost label query (%s); export market "
                        "value is unknown until the integration reloads",
                        err,
                    )
            else:
                if direction == "GENERATION" and any(day.market_value is None for day in days):
                    self._kraken_label_log.once(
                        logging.WARNING,
                        "Kraken generation days lack a %s line; export market value is unknown",
                        MARKET_VALUE_LABEL,
                    )
                return days
        return await self._async_fetch_kraken_pages(direction, start, end_exclusive, labelled=False)

    async def _async_fetch_kraken_pages(
        self,
        direction: Direction,
        start: date,
        end_exclusive: date,
        *,
        labelled: bool,
    ) -> list[KrakenDailyCost]:
        """Fetch provider-calculated daily electricity costs from Kraken."""
        time_zone = _configured_time_zone()
        variables: dict[str, Any] = {
            "accountNumber": self._account_number,
            "startAt": datetime.combine(start, time.min, tzinfo=time_zone).isoformat(),
            "endAt": datetime.combine(end_exclusive, time.min, tzinfo=time_zone).isoformat(),
            "timezone": _configured_time_zone_name(),
            "direction": direction,
            "first": KRAKEN_PAGE_SIZE,
            "after": None,
        }
        headers = {
            "Authorization": f"Bearer {self._kraken_token}",
            "Content-Type": "application/json",
        }
        query = DAILY_COSTS_QUERY if labelled else LEGACY_DAILY_COSTS_QUERY
        days: list[KrakenDailyCost] = []
        for _ in range(KRAKEN_MAX_PAGES):
            try:
                async with self.auth.websession.post(
                    GRAPHQL_URL,
                    json={"query": query, "variables": dict(variables)},
                    headers=headers,
                ) as resp:
                    status = getattr(resp, "status", getattr(resp, "_status", 200))
                    if status in (401, 403):
                        raise KrakenTokenExpired(
                            f"Kraken token expired or invalid fetching daily costs (HTTP {status})"
                        )
                    resp.raise_for_status()
                    payload = await resp.json()
            except ClientError as err:
                status = getattr(err, "status", None)
                if status in (401, 403):
                    raise KrakenTokenExpired(
                        f"Kraken token expired or invalid fetching daily costs (HTTP {status})"
                    ) from err
                raise
            try:
                page, cursor = parse_daily_costs_page(
                    payload,
                    time_zone,
                    labelled=labelled,
                    label_log=self._kraken_label_log if labelled else None,
                )
            except KrakenAuthError as err:
                raise KrakenTokenExpired(str(err)) from err
            days.extend(page)
            if cursor is None:
                return days
            variables["after"] = cursor
        raise KrakenCostError("Kraken returned more daily pages than expected")

    async def _async_get_kraken_electricity_costs(
        self,
        now: datetime,
        current_month_start: date,
    ) -> KrakenElectricityCosts | str:
        """Return Kraken daily costs for this year, or why they are unavailable."""
        year_start = current_month_start.replace(month=1)
        end_exclusive = now.date() + timedelta(days=1)
        results = await asyncio.gather(
            self._async_fetch_kraken_daily_costs("CONSUMPTION", year_start, end_exclusive),
            self._async_fetch_kraken_daily_costs("GENERATION", year_start, end_exclusive),
            return_exceptions=True,
        )
        auth_failure = next(
            (
                result
                for result in results
                if isinstance(result, (KrakenTokenExpired, ConfigEntryAuthFailed))
            ),
            None,
        )
        if auth_failure is not None:
            raise auth_failure

        failure = next(
            (result for result in results if isinstance(result, BaseException)),
            None,
        )
        if failure is None:
            self._kraken_daily_costs = (year_start, results[0], results[1])
        elif self._kraken_daily_costs is not None and self._kraken_daily_costs[0] == year_start:
            _LOGGER.debug(
                "Kraken cost statistics unavailable, reusing the last result: %s",
                failure,
            )
        else:
            _LOGGER.debug(
                "Kraken cost statistics unavailable, using calculated costs: %s",
                failure,
            )
            return "fetch_failed"

        _, import_days, export_days = self._kraken_daily_costs
        costs = build_electricity_costs(
            import_days,
            export_days,
            year_start=year_start,
            current_month_start=current_month_start,
        )
        return costs if costs is not None else "closed_days_incomplete"

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from ANWB API."""
        try:
            data = await self._async_update_data_internal()
            self.last_successful_update = dt_util.utcnow()
            return data
        except Exception as err:
            if isinstance(err, ConfigEntryAuthFailed):
                raise
            if isinstance(err, KrakenTokenExpired):
                _LOGGER.debug("Kraken token expired, refreshing and retrying")
                self._kraken_token = None
                try:
                    data = await self._async_update_data_internal()
                    self.last_successful_update = dt_util.utcnow()
                    return data
                except Exception as retry_err:
                    if isinstance(retry_err, ConfigEntryAuthFailed):
                        raise
                    if self._can_use_cached_data_for_dns_failure(retry_err):
                        _LOGGER.warning(
                            "DNS failure after token refresh, using cached data: %s",
                            retry_err,
                        )
                        return self.data
                    raise

            if self._can_use_cached_data_for_dns_failure(err):
                _LOGGER.warning("DNS failure, using cached data: %s", err)
                return self.data
            raise

    async def _async_update_data_internal(self) -> dict[str, Any]:
        if not self._kraken_token:
            self._kraken_token = await self._async_get_kraken_token()

        if not self._account_number:
            info = await self._async_get_account_info(self._kraken_token)
            self._account_number = info["number"]
            self._account_address = info["address"]

        now = _as_local(dt_util.utcnow())
        monthly_period_start = now.replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ).isoformat()
        yearly_period_start = now.replace(
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ).isoformat()
        previous_data = self.data if isinstance(self.data, dict) else {}
        same_month_as_previous = previous_data.get("monthly_period_start") == monthly_period_start
        same_year_as_previous = previous_data.get("yearly_period_start") == yearly_period_start
        start, end, c_start = _account_cache_query_boundaries(now)
        last_day = calendar.monthrange(now.year, now.month)[1]

        url_import = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/electricity/cache"
            f"?startDate={start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=HOUR"
        )
        url_export = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/production/cache"
            f"?startDate={start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=HOUR"
        )
        url_import_year = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/electricity/cache"
            f"?startDate={c_start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=MONTH"
        )
        url_export_year = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/production/cache"
            f"?startDate={c_start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=MONTH"
        )
        url_import_daily = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/electricity/cache"
            f"?startDate={c_start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=DAY"
        )
        url_export_daily = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/production/cache"
            f"?startDate={c_start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=DAY"
        )
        url_import_gas = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/gas/cache"
            f"?startDate={start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=HOUR"
        )
        url_import_gas_year = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/gas/cache"
            f"?startDate={c_start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=MONTH"
        )
        url_import_gas_daily = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/gas/cache"
            f"?startDate={c_start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=DAY"
        )

        import_tasks = [
            self._async_fetch_data(url_import, self._kraken_token),
            self._async_fetch_data(url_export, self._kraken_token),
            self._async_fetch_data(url_import_year, self._kraken_token),
            self._async_fetch_data(url_export_year, self._kraken_token),
        ]
        electricity_result_labels = ["import_year", "export_year"]
        if now.month > 1:
            import_tasks.extend(
                (
                    self._async_fetch_data(url_import_daily, self._kraken_token),
                    self._async_fetch_data(url_export_daily, self._kraken_token),
                )
            )
            electricity_result_labels.extend(("import_daily", "export_daily"))

        electricity_results = await asyncio.gather(*import_tasks, return_exceptions=True)
        for result in electricity_results[:2]:
            if not isinstance(result, Exception):
                continue
            if isinstance(result, KrakenTokenExpired):
                raise result
            _LOGGER.error("Failed to fetch current-month electricity data: %s", result)
            raise UpdateFailed(f"Electricity fetch failed: {result}") from result

        res_import = electricity_results[0]
        res_export = electricity_results[1]
        import_year_fetch_succeeded = not isinstance(electricity_results[2], Exception)
        export_year_fetch_succeeded = not isinstance(electricity_results[3], Exception)
        import_daily_fetch_succeeded = now.month == 1
        export_daily_fetch_succeeded = now.month == 1
        res_import_year: dict[str, Any] = {}
        res_export_year: dict[str, Any] = {}
        res_import_daily: dict[str, Any] = {}
        res_export_daily: dict[str, Any] = {}
        for result, label in zip(
            electricity_results[2:],
            electricity_result_labels,
            strict=True,
        ):
            if isinstance(result, Exception):
                if isinstance(result, KrakenTokenExpired):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch year-to-date electricity %s data: %s",
                    label.replace("_", " "),
                    result,
                )
            elif label == "import_year":
                res_import_year = result
            elif label == "export_year":
                res_export_year = result
            elif label == "import_daily":
                res_import_daily = result
                import_daily_fetch_succeeded = True
            else:
                res_export_daily = result
                export_daily_fetch_succeeded = True

        gas_tasks = [
            self._async_fetch_data(url_import_gas, self._kraken_token),
            self._async_fetch_data(url_import_gas_year, self._kraken_token),
        ]
        res_import_gas: dict[str, Any] = {}
        res_import_gas_year: dict[str, Any] = {}
        res_import_gas_daily: dict[str, Any] = {}
        gas_results = await asyncio.gather(*gas_tasks, return_exceptions=True)
        gas_hourly_fetch_succeeded = not isinstance(gas_results[0], Exception)
        gas_yearly_fetch_succeeded = not isinstance(gas_results[1], Exception)
        for index, result in enumerate(gas_results):
            if isinstance(result, Exception):
                if isinstance(result, KrakenTokenExpired):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch gas data, continuing without that response: %s",
                    result,
                )
            elif index == 0:
                res_import_gas = result
            else:
                res_import_gas_year = result

        inferred_has_gas = bool(res_import_gas.get("data") or res_import_gas_year.get("data"))

        previous_has_gas = bool(isinstance(self.data, dict) and self.data.get("has_gas") is True)
        gas_caches_are_definitively_empty = (
            gas_hourly_fetch_succeeded and gas_yearly_fetch_succeeded and not inferred_has_gas
        )
        retain_previous_has_gas = previous_has_gas and (
            not gas_caches_are_definitively_empty
            or same_year_as_previous
            or "yearly_period_start" not in previous_data
        )
        has_gas = inferred_has_gas or retain_previous_has_gas

        gas_daily_fetch_succeeded = now.month == 1 or not has_gas
        if now.month > 1 and has_gas:
            try:
                res_import_gas_daily = await self._async_fetch_data(
                    url_import_gas_daily,
                    self._kraken_token,
                )
                gas_daily_fetch_succeeded = True
            except Exception as result:
                if isinstance(result, KrakenTokenExpired):
                    raise
                _LOGGER.warning(
                    "Failed to fetch gas daily data, continuing without that response: %s",
                    result,
                )

        import_data = res_import.get("data", []) or []
        export_data = res_export.get("data", []) or []
        import_year_data = (
            res_import_year.get("data", []) or [] if import_year_fetch_succeeded else []
        )
        export_year_data = (
            res_export_year.get("data", []) or [] if export_year_fetch_succeeded else []
        )
        import_daily_data = (
            res_import_daily.get("data", []) or [] if import_daily_fetch_succeeded else []
        )
        export_daily_data = (
            res_export_daily.get("data", []) or [] if export_daily_fetch_succeeded else []
        )
        gas_data = res_import_gas.get("data", []) or [] if gas_hourly_fetch_succeeded else []
        gas_year_data = (
            res_import_gas_year.get("data", []) or [] if gas_yearly_fetch_succeeded else []
        )
        gas_daily_data = (
            res_import_gas_daily.get("data", []) or [] if gas_daily_fetch_succeeded else []
        )

        current_month_start = now.date().replace(day=1)
        electricity_hourly_days = _used_local_dates(import_data)
        electricity_hourly_days.update(_used_local_dates(export_data))
        gas_hourly_days = _used_local_dates(gas_data) if has_gas else set()

        electricity_daily_dates: set[date] = set()
        gas_daily_dates: set[date] = set()
        if now.month > 1:
            if import_daily_fetch_succeeded:
                electricity_daily_dates.update(
                    _used_local_dates(
                        _closed_prior_month_data(
                            import_daily_data,
                            current_month_start,
                        )
                    )
                )
            if export_daily_fetch_succeeded:
                electricity_daily_dates.update(
                    _used_local_dates(
                        _closed_prior_month_data(
                            export_daily_data,
                            current_month_start,
                        )
                    )
                )
            if has_gas and gas_daily_fetch_succeeded:
                gas_daily_dates.update(
                    _used_local_dates(
                        _closed_prior_month_data(
                            gas_daily_data,
                            current_month_start,
                        )
                    )
                )

        price_tasks = [
            self.tariff_cache.async_get_hourly_day(
                "electricity",
                local_day,
                lambda local_day=local_day: self._async_fetch_hourly_tariffs(
                    "electricity",
                    local_day,
                ),
            )
            for local_day in sorted(electricity_hourly_days)
        ]
        gas_price_tasks = [
            self.tariff_cache.async_get_hourly_day(
                "gas",
                local_day,
                lambda local_day=local_day: self._async_fetch_hourly_tariffs(
                    "gas",
                    local_day,
                ),
            )
            for local_day in sorted(gas_hourly_days)
        ]

        daily_tariff_kinds: list[Commodity] = []
        daily_tariff_tasks = []
        if electricity_daily_dates:
            daily_tariff_kinds.append("electricity")
            daily_tariff_tasks.append(
                self.tariff_cache.async_get_daily_tariffs(
                    "electricity",
                    electricity_daily_dates,
                    lambda missing_dates: self._async_fetch_daily_tariffs(
                        "electricity",
                        missing_dates,
                    ),
                )
            )
        if gas_daily_dates:
            daily_tariff_kinds.append("gas")
            daily_tariff_tasks.append(
                self.tariff_cache.async_get_daily_tariffs(
                    "gas",
                    gas_daily_dates,
                    lambda missing_dates: self._async_fetch_daily_tariffs(
                        "gas",
                        missing_dates,
                    ),
                )
            )

        (
            prices_results,
            gas_prices_results,
            daily_tariff_results,
        ) = await asyncio.gather(
            asyncio.gather(*price_tasks, return_exceptions=True),
            asyncio.gather(*gas_price_tasks, return_exceptions=True),
            asyncio.gather(*daily_tariff_tasks, return_exceptions=True),
        )

        price_map: dict[str, float] = {}
        market_price_map: dict[str, float] = {}
        for local_day, result in zip(sorted(electricity_hourly_days), prices_results, strict=True):
            if isinstance(result, Exception):
                if isinstance(result, KrakenTokenExpired):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch part of the electricity tariff window: %s",
                    result,
                )
                result = await self.tariff_cache.async_get_cached_hourly_day(
                    "electricity", local_day
                )
            price_map.update(result.all_in_prices)
            market_price_map.update(result.market_prices)

        gas_price_map: dict[str, float] = {}
        for result in gas_prices_results:
            if isinstance(result, Exception):
                if isinstance(result, KrakenTokenExpired):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch part of the gas tariff window: %s",
                    result,
                )
                continue
            gas_price_map.update(result.all_in_prices)

        daily_electricity_price_map: dict[str, float] = {}
        daily_electricity_market_price_map: dict[str, float] = {}
        daily_gas_price_map: dict[str, float] = {}
        electricity_daily_tariff_succeeded = not electricity_daily_dates
        gas_daily_tariff_succeeded = not gas_daily_dates
        for kind, result in zip(
            daily_tariff_kinds,
            daily_tariff_results,
            strict=True,
        ):
            if isinstance(result, Exception):
                if isinstance(result, KrakenTokenExpired):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch year-to-date %s daily tariffs: %s",
                    kind,
                    result,
                )
                required_dates = (
                    electricity_daily_dates if kind == "electricity" else gas_daily_dates
                )
                cached_tariffs = await self.tariff_cache.async_get_cached_daily_tariffs(
                    kind,
                    required_dates,
                )
                if kind == "electricity":
                    daily_electricity_price_map = dict(cached_tariffs.all_in_prices)
                    daily_electricity_market_price_map = dict(cached_tariffs.market_prices)
                else:
                    daily_gas_price_map = dict(cached_tariffs.all_in_prices)
                continue

            if kind == "electricity":
                daily_electricity_price_map = dict(result.all_in_prices)
                daily_electricity_market_price_map = dict(result.market_prices)
                electricity_daily_tariff_succeeded = True
            else:
                daily_gas_price_map = dict(result.all_in_prices)
                gas_daily_tariff_succeeded = True

        export_price_map = _build_export_price_map(
            price_map,
            market_price_map,
        )
        daily_export_price_map = _build_daily_export_price_map(
            daily_electricity_price_map,
            daily_electricity_market_price_map,
        )

        await self.tariff_cache.async_prune()

        reuse_cached_import_year = (
            same_year_as_previous
            and previous_data.get("electricity_import_year_to_date") is not None
            and (
                not import_year_fetch_succeeded
                or (
                    not import_year_data
                    and previous_data.get("electricity_import_year_to_date") != 0
                )
            )
        )
        reuse_cached_export_year = (
            same_year_as_previous
            and previous_data.get("electricity_export_year_to_date") is not None
            and (
                not export_year_fetch_succeeded
                or (
                    not export_year_data
                    and previous_data.get("electricity_export_year_to_date") != 0
                )
            )
        )
        reuse_cached_gas_month = (
            has_gas
            and same_month_as_previous
            and previous_data.get("gas_month_data_available") is not False
            and previous_data.get("gas_month_to_date") is not None
            and (
                not gas_hourly_fetch_succeeded
                or (
                    not gas_data
                    and (
                        previous_data.get("gas_month_to_date") != 0
                        or previous_data.get("gas_fixed_cost_source")
                        in {"account_cache", "hardcoded_fallback"}
                    )
                )
            )
        )
        reuse_cached_gas_year = (
            has_gas
            and same_year_as_previous
            and previous_data.get("gas_year_data_available") is not False
            and previous_data.get("gas_year_to_date") is not None
            and (
                not gas_yearly_fetch_succeeded
                or (not gas_year_data and previous_data.get("gas_year_to_date") != 0)
            )
        )

        import_usage = sum(data.get("usage", 0.0) for data in import_data)
        export_usage = sum(data.get("usage", 0.0) for data in export_data)
        import_rows = _MeterRows(
            import_data,
            import_year_data,
            import_daily_data,
            import_year_fetch_succeeded and not reuse_cached_import_year,
            import_daily_fetch_succeeded,
        )
        export_rows = _MeterRows(
            export_data,
            export_year_data,
            export_daily_data,
            export_year_fetch_succeeded and not reuse_cached_export_year,
            export_daily_fetch_succeeded,
        )

        # Prefer the amounts Kraken calculates (the figures shown in the ANWB
        # app): they price every hour, include the provider's net-metering and
        # real fixed charges, and switch to market-price export automatically.
        # Intervals after the last covered day keep the hourly calculation.
        provider_result = await self._async_get_kraken_electricity_costs(now, current_month_start)
        provider_costs: _ProviderElectricityCosts | None = None
        if isinstance(provider_result, str):
            provider_costs_status = provider_result
        else:
            provider_split = _provider_electricity_costs(
                provider_result,
                import_data=import_data,
                export_data=export_data,
                import_year_data=import_rows.authoritative_monthly,
                export_year_data=export_rows.authoritative_monthly,
                price_map=price_map,
                export_price_map=export_price_map,
                market_price_map=market_price_map,
                current_month_start=current_month_start,
            )
            if isinstance(provider_split, str):
                provider_costs_status = provider_split
                _LOGGER.debug(
                    "Kraken cost statistics rejected (%s), using calculated costs",
                    provider_split,
                )
            else:
                provider_costs = provider_split
                provider_costs_status = "ok"

        amounts = functools.partial(
            _electricity_amounts,
            current_month_start=current_month_start,
            daily_tariff_succeeded=electricity_daily_tariff_succeeded,
        )
        imports = amounts(
            import_rows,
            price_map,
            daily_electricity_price_map,
            provider_costs.import_cost if provider_costs else None,
        )
        export_credit = amounts(
            export_rows,
            export_price_map,
            daily_export_price_map,
            provider_costs.export_credit if provider_costs else None,
        )
        export_market = amounts(
            export_rows,
            market_price_map,
            daily_electricity_market_price_map,
            provider_costs.export_market_value if provider_costs else None,
        )
        import_cost = imports.month
        export_cost = export_credit.month
        if not has_gas:
            gas_usage = 0.0
            gas_cost = 0.0
            gas_tariff_coverage = {
                "complete": True,
                "required_intervals": 0,
                "matched_intervals": 0,
                "missing_intervals_count": 0,
                "missing_intervals": [],
            }
            gas_month_data_available = True
        elif reuse_cached_gas_month:
            gas_usage = previous_data["gas_month_to_date"]
            gas_cost = previous_data.get("gas_month_to_date_cost")
            gas_tariff_coverage = previous_data.get(
                "gas_tariff_coverage",
                {
                    "complete": gas_cost is not None,
                    "required_intervals": 0,
                    "matched_intervals": 0,
                    "missing_intervals_count": 0,
                    "missing_intervals": [],
                },
            )
            gas_month_data_available = True
        elif gas_hourly_fetch_succeeded:
            gas_usage, gas_cost, gas_tariff_coverage = _usage_and_variable_cost(
                gas_data, gas_price_map
            )
            gas_cost, gas_tariff_coverage = _reconcile_current_month_variable_cost(
                gas_usage,
                gas_cost,
                gas_tariff_coverage,
                gas_year_data,
                current_month_start,
            )
            gas_month_data_available = True
        else:
            gas_usage = None
            gas_cost = None
            gas_tariff_coverage = {
                "complete": False,
                "required_intervals": 0,
                "matched_intervals": 0,
                "missing_intervals_count": 0,
                "missing_intervals": [],
                "reason": "gas_usage_unavailable",
            }
            gas_month_data_available = False

        api_vaste_kosten = sum_account_cache_fixed_costs(import_data) or 0.0

        if now.month == 1:
            yearly_import_usage = import_usage
        elif reuse_cached_import_year:
            yearly_import_usage = previous_data["electricity_import_year_to_date"]
        elif import_year_fetch_succeeded:
            yearly_import_usage = sum(data.get("usage", 0.0) for data in import_year_data)
        else:
            yearly_import_usage = None

        if now.month == 1:
            yearly_export_usage = export_usage
        elif reuse_cached_export_year:
            yearly_export_usage = previous_data["electricity_export_year_to_date"]
        elif export_year_fetch_succeeded:
            yearly_export_usage = sum(data.get("usage", 0.0) for data in export_year_data)
        else:
            yearly_export_usage = None

        api_vaste_kosten_gas = sum_account_cache_fixed_costs(gas_data) or 0.0

        if not has_gas:
            yearly_gas_usage = 0.0
            yearly_gas_cost = 0.0
            yearly_gas_tariff_coverage = {
                "complete": True,
                "required_intervals": 0,
                "matched_intervals": 0,
                "missing_intervals_count": 0,
                "missing_intervals": [],
            }
            gas_year_data_available = True
        else:
            if now.month == 1:
                yearly_gas_usage = gas_usage
                gas_year_data_available = gas_month_data_available
            elif reuse_cached_gas_year:
                yearly_gas_usage = previous_data["gas_year_to_date"]
                gas_year_data_available = True
            elif gas_yearly_fetch_succeeded:
                yearly_gas_usage = sum(data.get("usage", 0.0) for data in gas_year_data)
                gas_year_data_available = True
            else:
                yearly_gas_usage = None
                gas_year_data_available = False

            (
                yearly_gas_cost,
                yearly_gas_tariff_coverage,
            ) = _year_to_date_variable_cost(
                gas_year_data,
                gas_daily_data,
                gas_cost,
                gas_tariff_coverage,
                daily_gas_price_map,
                current_month_start,
                authoritative_fetch_succeeded=(
                    gas_yearly_fetch_succeeded and not reuse_cached_gas_year
                ),
                daily_fetch_succeeded=gas_daily_fetch_succeeded,
                daily_tariff_succeeded=gas_daily_tariff_succeeded,
            )

        if provider_costs is not None:
            total_fixed_costs = provider_costs.fixed_cost
            electricity_fixed_cost_source = "kraken_statistics"
        elif abs(api_vaste_kosten) > 0.01:
            total_fixed_costs = api_vaste_kosten
            electricity_fixed_cost_source = "account_cache"
        else:
            fraction = now.day / float(last_day)
            total_fixed_costs = (
                (VASTE_LEVERINGSKOSTEN * fraction)
                + (NETBEHEERKOSTEN * fraction)
                + (VERMINDERING_ENERGIEBELASTING * fraction)
            )
            electricity_fixed_cost_source = "hardcoded_fallback"

        if not has_gas:
            total_fixed_costs_gas = 0.0
            gas_fixed_cost_source = "not_applicable"
        elif reuse_cached_gas_month:
            total_fixed_costs_gas = previous_data.get("gas_month_to_date_fixed_cost")
            gas_fixed_cost_source = previous_data.get("gas_fixed_cost_source", "unavailable")
        elif not gas_hourly_fetch_succeeded:
            total_fixed_costs_gas = None
            gas_fixed_cost_source = "unavailable"
        elif not gas_data:
            total_fixed_costs_gas = None
            gas_fixed_cost_source = "unavailable"
        elif abs(api_vaste_kosten_gas) > 0.01:
            total_fixed_costs_gas = api_vaste_kosten_gas
            gas_fixed_cost_source = "account_cache"
        else:
            fraction_gas = now.day / float(last_day)
            total_fixed_costs_gas = (
                (VASTE_LEVERINGSKOSTEN_GAS * fraction_gas)
                + (NETBEHEERKOSTEN_GAS * fraction_gas)
                + (VERMINDERING_ENERGIEBELASTING_GAS * fraction_gas)
            )
            gas_fixed_cost_source = "hardcoded_fallback"

        total_cost = (
            import_cost - export_cost + total_fixed_costs
            if import_cost is not None and export_cost is not None
            else None
        )
        if reuse_cached_gas_month:
            total_cost_gas = previous_data.get("gas_month_to_date_total_cost")
        else:
            total_cost_gas = (
                gas_cost + total_fixed_costs_gas
                if gas_cost is not None and total_fixed_costs_gas is not None
                else None
            )

        await self._insert_statistics(
            import_data,
            export_data,
            price_map,
            gas_data,
            gas_price_map,
            export_price_map=export_price_map,
        )

        return {
            "electricity_import_month_to_date": import_usage,
            "electricity_import_month_to_date_cost": import_cost,
            "electricity_export_month_to_date": export_usage,
            "electricity_export_month_to_date_credit": export_cost,
            "electricity_import_year_to_date": yearly_import_usage,
            "electricity_import_year_to_date_cost": imports.year,
            "electricity_export_year_to_date": yearly_export_usage,
            "electricity_export_year_to_date_credit": export_credit.year,
            "electricity_month_to_date_fixed_cost": total_fixed_costs,
            "electricity_month_to_date_total_cost": total_cost,
            "electricity_import_tariff_coverage": imports.month_coverage,
            "electricity_export_tariff_coverage": export_credit.month_coverage,
            "electricity_import_year_to_date_tariff_coverage": imports.year_coverage,
            "electricity_export_year_to_date_tariff_coverage": (export_credit.year_coverage),
            "electricity_fixed_cost_source": electricity_fixed_cost_source,
            "electricity_export_month_to_date_market_value": export_market.month,
            "electricity_export_year_to_date_market_value": export_market.year,
            "electricity_export_market_value_tariff_coverage": (export_market.month_coverage),
            "electricity_export_year_to_date_market_value_tariff_coverage": (
                export_market.year_coverage
            ),
            "electricity_export_month_to_date_credit_breakdown": (
                provider_costs.export_month_breakdown.as_attributes()
                if provider_costs is not None and export_cost is not None
                else None
            ),
            "electricity_export_year_to_date_credit_breakdown": (
                provider_costs.export_year_breakdown.as_attributes()
                if provider_costs is not None and export_credit.year is not None
                else None
            ),
            "export_price_basis": ("market" if now >= NET_METERING_END_DATE else "all_in"),
            "gas_month_to_date": gas_usage,
            "gas_month_to_date_cost": gas_cost,
            "gas_year_to_date": yearly_gas_usage,
            "gas_year_to_date_cost": yearly_gas_cost,
            "gas_month_to_date_fixed_cost": total_fixed_costs_gas,
            "gas_month_to_date_total_cost": total_cost_gas,
            "gas_tariff_coverage": gas_tariff_coverage,
            "gas_year_to_date_tariff_coverage": yearly_gas_tariff_coverage,
            "gas_fixed_cost_source": gas_fixed_cost_source,
            "gas_month_data_available": gas_month_data_available,
            "gas_year_data_available": gas_year_data_available,
            "has_gas": has_gas,
            "import_usage": import_usage,
            "import_cost": import_cost,
            "export_usage": export_usage,
            "export_cost": export_cost,
            "gas_usage": gas_usage,
            "gas_cost": gas_cost,
            "yearly_import_usage": yearly_import_usage,
            "yearly_export_usage": yearly_export_usage,
            "yearly_gas_usage": yearly_gas_usage,
            "fixed_cost": total_fixed_costs,
            "fixed_cost_gas": total_fixed_costs_gas,
            "total_cost": total_cost,
            "total_cost_gas": total_cost_gas,
            "monthly_period_start": monthly_period_start,
            "yearly_period_start": yearly_period_start,
            "year_to_date_cost_calculation_method": ("daily_closed_months_hourly_current_month"),
            "electricity_year_to_date_cost_calculation_method": (
                "kraken_daily_statistics_hourly_tail"
                if provider_costs is not None
                else "daily_closed_months_hourly_current_month"
            ),
            "electricity_cost_source": (
                "kraken_statistics" if provider_costs is not None else "calculated"
            ),
            "electricity_provider_costs_status": provider_costs_status,
            "electricity_provider_costs_through": (
                provider_costs.covered_through.isoformat()
                if provider_costs is not None and provider_costs.covered_through is not None
                else None
            ),
            "account_number": self._account_number,
            "account_address": getattr(self, "_account_address", None),
        }

    async def _insert_statistics(
        self,
        import_data: list,
        export_data: list,
        price_map: dict,
        gas_data: list,
        gas_price_map: dict,
        export_price_map: dict[str, float],
    ) -> None:
        """Insert ANWB statistics."""
        for sensor_type, is_cost, data_list, p_map in [
            ("import_usage", False, import_data, price_map),
            ("export_usage", False, export_data, price_map),
            ("import_cost", True, import_data, price_map),
            ("export_cost", True, export_data, export_price_map),
            ("gas_usage", False, gas_data, gas_price_map),
            ("gas_cost", True, gas_data, gas_price_map),
        ]:
            if not data_list:
                continue

            unit_class = (
                None
                if is_cost
                else ("energy" if "import" in sensor_type or "export" in sensor_type else "volume")
            )
            unit = (
                CURRENCY_EURO
                if is_cost
                else (
                    UnitOfEnergy.KILO_WATT_HOUR
                    if "import" in sensor_type or "export" in sensor_type
                    else UnitOfVolume.CUBIC_METERS
                )
            )
            statistic_id = f"{DOMAIN}:{sensor_type}_{self._account_number}".lower().replace(
                "-", "_"
            )

            parsed_data = [
                (start_time, data)
                for data in data_list
                if (start_time := _parse_api_datetime(data.get("startDate"))) is not None
            ]
            parsed_data.sort(key=lambda item: item[0])
            if not parsed_data:
                continue

            desired_values: list[tuple[datetime, float]] = []
            for start_time, data in parsed_data:
                usage = data.get("usage", 0.0)
                if is_cost:
                    timestamp = _normalize_api_datetime_key(data.get("startDate"))
                    price_cents = (
                        _numeric_tariff_value(p_map.get(timestamp))
                        if timestamp is not None
                        else None
                    )
                    if price_cents is None:
                        if usage != 0:
                            if sensor_type == "export_cost" and start_time >= NET_METERING_END_DATE:
                                _LOGGER.warning(
                                    "Missing electricity market tariff for export statistics "
                                    "insertion at %s",
                                    start_time.isoformat(),
                                )
                            break
                        value = 0.0
                    else:
                        value = (usage * price_cents) / 100.0
                else:
                    value = usage

                desired_values.append((start_time, value))

            if not desired_values:
                continue

            from_time = parsed_data[0][0]
            start = from_time - timedelta(hours=1)
            stat = await get_instance(self.hass).async_add_executor_job(
                statistics_during_period,
                self.hass,
                start,
                None,
                {statistic_id},
                "hour",
                None,
                {"state", "sum"},
            )

            existing_stats: list[tuple[datetime, dict[str, Any]]] = []
            if statistic_id in stat and stat[statistic_id]:
                existing_stats = [
                    (stat_start, row)
                    for row in stat[statistic_id]
                    if (stat_start := _stat_start_datetime(row.get("start"))) is not None
                ]
                existing_stats.sort(key=lambda item: item[0])

            existing_by_start = dict(existing_stats)
            write_from = None
            for index, (start_time, value) in enumerate(desired_values):
                existing = existing_by_start.get(start_time)
                if existing is None:
                    write_from = index
                    break

                existing_state = _numeric_tariff_value(existing.get("state"))
                if existing_state is not None and not math.isclose(
                    existing_state,
                    value,
                    rel_tol=1e-9,
                    abs_tol=1e-9,
                ):
                    write_from = index
                    break

            if write_from is None:
                continue

            first_write_time = desired_values[write_from][0]
            preceding_stats = [
                row for stat_start, row in existing_stats if stat_start < first_write_time
            ]
            if preceding_stats:
                running_sum = _numeric_tariff_value(preceding_stats[-1].get("sum")) or 0.0
            else:
                first_existing = existing_by_start.get(first_write_time)
                existing_sum = (
                    _numeric_tariff_value(first_existing.get("sum")) if first_existing else None
                )
                existing_state = (
                    _numeric_tariff_value(first_existing.get("state")) if first_existing else None
                )
                running_sum = (
                    existing_sum - existing_state
                    if existing_sum is not None and existing_state is not None
                    else 0.0
                )

            statistics = []
            for start_time, value in desired_values[write_from:]:
                running_sum += value
                statistics.append(
                    StatisticData(
                        start=start_time,
                        state=value,
                        sum=running_sum,
                    )
                )

            if statistics:
                name = sensor_type.replace("_", " ").title()
                metadata = StatisticMetaData(
                    mean_type=StatisticMeanType.NONE,
                    has_sum=True,
                    name=f"ANWB Account {self._account_number} {name}",
                    source=DOMAIN,
                    statistic_id=statistic_id,
                    unit_class=unit_class,
                    unit_of_measurement=unit,
                )
                async_add_external_statistics(self.hass, metadata, statistics)


@dataclass
class ANWBEnergieAccountData:
    """Data for the ANWB Energie Account integration."""

    consumption: ANWBConsumptionCoordinator
    pricing: ANWBPricingCoordinator
    tariff_cache: TariffCache


type ANWBEnergieAccountConfigEntry = ConfigEntry[ANWBEnergieAccountData]

__all__ = [
    "ANWBBaseCoordinator",
    "ANWBConsumptionCoordinator",
    "ANWBEnergieAccountConfigEntry",
    "ANWBEnergieAccountData",
    "ANWBPricingCoordinator",
    "KrakenTokenExpired",
    "NET_METERING_END_DATE",
    "_USAGE_RECONCILIATION_ABS_TOLERANCE",
    "_USAGE_RECONCILIATION_REL_TOLERANCE",
    "_account_cache_query_boundaries",
    "_as_local",
    "_as_utc",
    "_build_daily_export_price_map",
    "_build_export_price_map",
    "_closed_prior_month_data",
    "_combine_tariff_coverage",
    "_configured_time_zone",
    "_configured_time_zone_name",
    "_daily_tariff_data",
    "_daily_tariff_map",
    "_daily_usage_and_variable_cost",
    "_format_account_cache_datetime",
    "_hourly_tariff_data",
    "_is_post_cutoff_daily",
    "_is_post_cutoff_hourly",
    "_local_date_for_api_datetime",
    "_local_date_key",
    "_normalize_api_datetime_key",
    "_numeric_tariff_map",
    "_numeric_tariff_value",
    "_parse_api_datetime",
    "_provider_tariff_dates_for_local_day",
    "_reconcile_current_month_variable_cost",
    "_stat_start_datetime",
    "_unavailable_tariff_coverage",
    "_usage_and_variable_cost",
    "_used_local_dates",
    "_year_to_date_variable_cost",
]
