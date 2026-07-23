"""Coordinator for ANWB Energie Account."""

from __future__ import annotations

import asyncio
import calendar
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone, tzinfo
import logging
import math
from typing import Any
from zoneinfo import ZoneInfo

from aiohttp.client_exceptions import ClientError

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
    VASTE_LEVERINGSKOSTEN,
    VERMINDERING_ENERGIEBELASTING,
    VASTE_LEVERINGSKOSTEN_GAS,
    NETBEHEERKOSTEN_GAS,
    VERMINDERING_ENERGIEBELASTING_GAS,
)
from .tariff_cache import Commodity, HourlyTariffData, TariffCache

_LOGGER = logging.getLogger(__name__)
_ANWB_TARIFF_TIME_ZONE = ZoneInfo("Europe/Amsterdam")

_DNS_FAILURE_MARKERS = (
    "dns",
    "getaddrinfo failed",
    "name or service not known",
    "name resolution",
    "nodename nor servname",
    "temporary failure in name resolution",
)

# DAY and MONTH cache aggregates may round independently. Differences up to one
# thousandth of a kWh/m³ (plus a 1e-6 relative tolerance) are treated as equal.
_USAGE_RECONCILIATION_ABS_TOLERANCE = 1e-3
_USAGE_RECONCILIATION_REL_TOLERANCE = 1e-6


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
    except (AttributeError, TypeError, ValueError):
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

    return parsed.replace(minute=0, second=0, microsecond=0).strftime(
        "%Y-%m-%dT%H:00:00.000Z"
    )


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


def _local_day_tariff_range(local_day: date) -> tuple[str, str]:
    """Return ANWB's date-labelled range for one Amsterdam tariff day."""
    day_label = local_day.isoformat()
    return (
        f"{day_label}T00:00:00.000Z",
        f"{day_label}T23:59:59.999Z",
    )


def _provider_tariff_dates_for_local_day(local_day: date) -> tuple[date, ...]:
    """Return ANWB day labels containing a Home Assistant local day."""
    local_tz = _configured_time_zone()
    start = datetime.combine(local_day, time.min, tzinfo=local_tz).astimezone(
        timezone.utc
    )
    end = (
        datetime.combine(
            local_day + timedelta(days=1),
            time.min,
            tzinfo=local_tz,
        ).astimezone(timezone.utc)
        - timedelta(microseconds=1)
    )
    first_label = start.astimezone(_ANWB_TARIFF_TIME_ZONE).date()
    last_label = end.astimezone(_ANWB_TARIFF_TIME_ZONE).date()
    return tuple(
        first_label + timedelta(days=offset)
        for offset in range((last_label - first_label).days + 1)
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


def _daily_tariff_map(data_list: list[dict[str, Any]]) -> dict[str, float]:
    """Return numeric tariff values keyed by Home Assistant local date."""
    price_map: dict[str, float] = {}
    for data in data_list:
        local_date = _local_date_key(data.get("date"))
        values = data.get("values")
        if local_date is None or not isinstance(values, dict):
            continue

        price = _numeric_tariff_value(values.get("allInPrijs"))
        if price is not None:
            price_map[local_date] = price

    return price_map


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


def _usage_and_variable_cost(
    data_list: list[dict[str, Any]],
    price_map: dict[str, float],
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
        timestamp = _normalize_api_datetime_key(data.get("startDate"))
        price_cents = (
            _numeric_tariff_value(price_map.get(timestamp))
            if timestamp is not None
            else None
        )
        if price_cents is None:
            missing_intervals_count += 1
            if len(missing_intervals) < 5:
                missing_intervals.append(
                    timestamp or str(data.get("startDate") or "<missing>")
                )
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
        if _local_date_for_api_datetime(data.get("startDate"))
        == current_month_start
    ]
    if not authoritative_rows:
        return variable_cost, coverage

    authoritative_usage = sum(
        data.get("usage", 0.0) for data in authoritative_rows
    )
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
        local_date = _local_date_key(data.get("startDate"))
        price_cents = (
            _numeric_tariff_value(price_map.get(local_date))
            if local_date is not None
            else None
        )
        if price_cents is None:
            missing_intervals_count += 1
            if len(missing_intervals) < 5:
                missing_intervals.append(
                    local_date or str(data.get("startDate") or "<missing>")
                )
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
    authoritative_usage = sum(
        data.get("usage", 0.0) for data in closed_authoritative_data
    )
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
                    raise UpdateFailed(
                        "Kraken token expired or invalid fetching account number"
                    )
                resp.raise_for_status()
                data = await resp.json()
                errors = data.get("errors") or []
                if errors:
                    auth_error = False
                    messages: list[str] = []
                    for error in errors:
                        if not isinstance(error, dict):
                            continue

                        message = error.get("message")
                        message_text = str(message) if message else ""
                        if message_text:
                            messages.append(message_text)
                        if "AUTH" in message_text.upper():
                            auth_error = True

                        extensions = error.get("extensions", {})
                        if not isinstance(extensions, dict):
                            continue

                        error_type = str(extensions.get("errorType", "")).upper()
                        error_code = str(extensions.get("errorCode", "")).upper()
                        if "AUTH" in error_type or "AUTH" in error_code:
                            auth_error = True

                    if auth_error:
                        raise UpdateFailed(
                            "Kraken token expired or invalid fetching account number"
                        )

                    message = "; ".join(messages) if messages else "unknown error"
                    raise UpdateFailed(
                        f"GraphQL error fetching account number: {message}"
                    )

                payload = data.get("data") or {}
                viewer = payload.get("viewer") or {}
                accounts = viewer.get("accounts", [])
                if not accounts:
                    raise UpdateFailed("No active accounts found")

                account = accounts[0]
                number = account.get("number", "unknown")
                address = ""
                properties = account.get("properties", [])
                if (
                    properties
                    and isinstance(properties, list)
                    and properties[0].get("address")
                ):
                    address = properties[0]["address"]

                return {"number": number, "address": address}
        except ClientError as err:
            if getattr(err, "status", None) in (401, 403):
                raise UpdateFailed(
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
        headers = (
            {"Authorization": f"Bearer {kraken_token}"}
            if kraken_token is not None
            else None
        )
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status in (401, 403):
                    if kraken_token is not None:
                        raise UpdateFailed(
                            "Kraken token expired or invalid fetching data from "
                            f"{url}"
                        )
                    raise UpdateFailed(
                        f"Public tariff request was unauthorized for {url}"
                    )
                if resp.status == 404:
                    return {}
                resp.raise_for_status()
                return await resp.json()
        except ClientError as err:
            if getattr(err, "status", None) in (401, 403):
                if kraken_token is not None:
                    raise UpdateFailed(
                        "Kraken token expired or invalid fetching data from "
                        f"{url}"
                    ) from err
                raise UpdateFailed(
                    f"Public tariff request was unauthorized for {url}"
                ) from err
            raise UpdateFailed(f"Error fetching data from {url}: {err}") from err

    async def _async_fetch_hourly_tariffs(
        self,
        commodity: Commodity,
        local_day: date,
    ) -> HourlyTariffData:
        """Fetch one Home Assistant local day of hourly public tariffs."""
        all_in_prices: dict[str, float] = {}
        market_prices: dict[str, float] = {}
        # ANWB interprets start/end as Amsterdam calendar-day labels, even
        # though the query strings end in Z. A HA-local day can overlap two
        # provider days when Home Assistant uses another timezone.
        for provider_day in _provider_tariff_dates_for_local_day(local_day):
            day_start, day_end = _local_day_tariff_range(provider_day)
            url = (
                "https://api.anwb.nl/energy/energy-services/v2/tarieven/"
                f"{commodity}?startDate={day_start}&endDate={day_end}&interval=HOUR"
            )
            response = await self._async_fetch_data(url, None)
            parsed = _hourly_tariff_data(response.get("data", []) or [])
            all_in_prices.update(parsed.all_in_prices)
            market_prices.update(parsed.market_prices)

        return HourlyTariffData(all_in_prices, market_prices)

    async def _async_fetch_daily_tariffs(
        self,
        commodity: Commodity,
        missing_dates: frozenset[date],
    ) -> dict[str, float]:
        """Fetch one range containing every missing local DAY tariff."""
        if not missing_dates:
            return {}

        provider_days = {
            provider_day
            for local_day in missing_dates
            for provider_day in _provider_tariff_dates_for_local_day(local_day)
        }
        # DAY responses use the same Amsterdam labels as HOUR responses and
        # omit the start label, so include one leading provider day.
        first_day = min(provider_days) - timedelta(days=1)
        last_day = max(provider_days)
        day_start = f"{first_day.isoformat()}T00:00:00.000Z"
        day_end = f"{last_day.isoformat()}T23:59:59.999Z"
        url = (
            "https://api.anwb.nl/energy/energy-services/v2/tarieven/"
            f"{commodity}?startDate={day_start}&endDate={day_end}&interval=DAY"
        )
        response = await self._async_fetch_data(url, None)
        return _daily_tariff_map(response.get("data", []) or [])


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
            if isinstance(err, UpdateFailed) and "Kraken token expired" in str(err):
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
            has_tomorrow_electricity = (
                self.tariff_cache.hourly_prices_are_complete(
                    tomorrow,
                    prices,
                )
            )
            has_tomorrow_market_price = (
                self.tariff_cache.hourly_prices_are_complete(
                    tomorrow,
                    market_prices,
                )
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
                and (
                    not has_tomorrow_electricity
                    or not has_tomorrow_market_price
                )
            )
        )
        fetch_gas = gas_applicable and (
            not has_today_gas
            or (local_now.hour >= 6 and not has_tomorrow_gas)
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

        price_map = _numeric_tariff_map(
            self.data.get("_raw_price_map", {}) if self.data else {}
        )
        market_price_map = _numeric_tariff_map(
            self.data.get("_raw_market_price_map", {}) if self.data else {}
        )
        gas_price_map = (
            _numeric_tariff_map(
                self.data.get("_raw_gas_price_map", {}) if self.data else {}
            )
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

        for result in prices_results:
            if isinstance(result, Exception):
                if isinstance(result, UpdateFailed) and "Kraken token expired" in str(
                    result
                ):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch part of the electricity tariff window: %s",
                    result,
                )
                continue
            price_map.update(result.all_in_prices)
            market_price_map.update(result.market_prices)

        for result in gas_prices_results:
            if isinstance(result, Exception):
                if isinstance(result, UpdateFailed) and "Kraken token expired" in str(
                    result
                ):
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

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from ANWB API."""
        try:
            data = await self._async_update_data_internal()
            self.last_successful_update = dt_util.utcnow()
            return data
        except Exception as err:
            if isinstance(err, UpdateFailed) and "Kraken token expired" in str(err):
                _LOGGER.debug("Kraken token expired, refreshing and retrying")
                self._kraken_token = None
                try:
                    data = await self._async_update_data_internal()
                    self.last_successful_update = dt_util.utcnow()
                    return data
                except Exception as retry_err:
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
        same_month_as_previous = (
            previous_data.get("monthly_period_start") == monthly_period_start
        )
        same_year_as_previous = (
            previous_data.get("yearly_period_start") == yearly_period_start
        )
        start = f"{now.year}-{now.month:02d}-01T00:00:00.000Z"
        last_day = calendar.monthrange(now.year, now.month)[1]
        end = f"{now.year}-{now.month:02d}-{last_day}T23:59:59.999Z"
        c_start = f"{now.year}-01-01T00:00:00.000Z"

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

        electricity_results = await asyncio.gather(
            *import_tasks, return_exceptions=True
        )
        for result in electricity_results[:2]:
            if not isinstance(result, Exception):
                continue
            if isinstance(result, UpdateFailed) and "Kraken token expired" in str(
                result
            ):
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
                if isinstance(result, UpdateFailed) and "Kraken token expired" in str(
                    result
                ):
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
                if isinstance(result, UpdateFailed) and "Kraken token expired" in str(
                    result
                ):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch gas data, continuing without that response: %s",
                    result,
                )
            elif index == 0:
                res_import_gas = result
            else:
                res_import_gas_year = result

        inferred_has_gas = bool(
            res_import_gas.get("data") or res_import_gas_year.get("data")
        )
        if (
            self.data is None
            and not inferred_has_gas
            and not (gas_hourly_fetch_succeeded and gas_yearly_fetch_succeeded)
        ):
            raise UpdateFailed(
                "Unable to determine whether the account has gas because a gas "
                "cache request failed"
            )

        previous_has_gas = bool(
            isinstance(self.data, dict) and self.data.get("has_gas") is True
        )
        gas_caches_are_definitively_empty = (
            gas_hourly_fetch_succeeded
            and gas_yearly_fetch_succeeded
            and not inferred_has_gas
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
                if isinstance(result, UpdateFailed) and "Kraken token expired" in str(
                    result
                ):
                    raise
                _LOGGER.warning(
                    "Failed to fetch gas daily data, continuing without that "
                    "response: %s",
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
            res_import_daily.get("data", []) or []
            if import_daily_fetch_succeeded
            else []
        )
        export_daily_data = (
            res_export_daily.get("data", []) or []
            if export_daily_fetch_succeeded
            else []
        )
        gas_data = (
            res_import_gas.get("data", []) or [] if gas_hourly_fetch_succeeded else []
        )
        gas_year_data = (
            res_import_gas_year.get("data", []) or []
            if gas_yearly_fetch_succeeded
            else []
        )
        gas_daily_data = (
            res_import_gas_daily.get("data", []) or []
            if gas_daily_fetch_succeeded
            else []
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
                self.tariff_cache.async_get_daily_prices(
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
                self.tariff_cache.async_get_daily_prices(
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
        for result in prices_results:
            if isinstance(result, Exception):
                if isinstance(result, UpdateFailed) and "Kraken token expired" in str(
                    result
                ):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch part of the electricity tariff window: %s",
                    result,
                )
                continue
            price_map.update(result.all_in_prices)

        gas_price_map: dict[str, float] = {}
        for result in gas_prices_results:
            if isinstance(result, Exception):
                if isinstance(result, UpdateFailed) and "Kraken token expired" in str(
                    result
                ):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch part of the gas tariff window: %s",
                    result,
                )
                continue
            gas_price_map.update(result.all_in_prices)

        daily_electricity_price_map: dict[str, float] = {}
        daily_gas_price_map: dict[str, float] = {}
        electricity_daily_tariff_succeeded = not electricity_daily_dates
        gas_daily_tariff_succeeded = not gas_daily_dates
        for kind, result in zip(
            daily_tariff_kinds,
            daily_tariff_results,
            strict=True,
        ):
            if isinstance(result, Exception):
                if isinstance(result, UpdateFailed) and "Kraken token expired" in str(
                    result
                ):
                    raise result
                _LOGGER.warning(
                    "Failed to fetch year-to-date %s daily tariffs: %s",
                    kind,
                    result,
                )
                required_dates = (
                    electricity_daily_dates
                    if kind == "electricity"
                    else gas_daily_dates
                )
                cached_prices = (
                    await self.tariff_cache.async_get_cached_daily_prices(
                        kind,
                        required_dates,
                    )
                )
                if kind == "electricity":
                    daily_electricity_price_map = dict(cached_prices)
                else:
                    daily_gas_price_map = dict(cached_prices)
                continue

            if kind == "electricity":
                daily_electricity_price_map = dict(result)
                electricity_daily_tariff_succeeded = True
            else:
                daily_gas_price_map = dict(result)
                gas_daily_tariff_succeeded = True

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

        (
            import_usage,
            import_cost,
            import_tariff_coverage,
        ) = _usage_and_variable_cost(import_data, price_map)
        (
            export_usage,
            export_cost,
            export_tariff_coverage,
        ) = _usage_and_variable_cost(export_data, price_map)
        import_cost, import_tariff_coverage = (
            _reconcile_current_month_variable_cost(
                import_usage,
                import_cost,
                import_tariff_coverage,
                import_year_data,
                current_month_start,
            )
        )
        export_cost, export_tariff_coverage = (
            _reconcile_current_month_variable_cost(
                export_usage,
                export_cost,
                export_tariff_coverage,
                export_year_data,
                current_month_start,
            )
        )
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
            gas_cost, gas_tariff_coverage = (
                _reconcile_current_month_variable_cost(
                    gas_usage,
                    gas_cost,
                    gas_tariff_coverage,
                    gas_year_data,
                    current_month_start,
                )
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

        api_vaste_kosten = 0.0
        if import_data:
            vk = import_data[0].get("vasteKosten")
            if isinstance(vk, dict):
                api_vaste_kosten = sum(
                    _numeric_tariff_value(vk.get(key)) or 0.0
                    for key in (
                        "abonnementsKosten",
                        "netbeheerKosten",
                        "verminderingEnergieBelasting",
                    )
                )

        if now.month == 1:
            yearly_import_usage = import_usage
        elif reuse_cached_import_year:
            yearly_import_usage = previous_data["electricity_import_year_to_date"]
        elif import_year_fetch_succeeded:
            yearly_import_usage = sum(
                data.get("usage", 0.0) for data in import_year_data
            )
        else:
            yearly_import_usage = None

        (
            yearly_import_cost,
            yearly_import_tariff_coverage,
        ) = _year_to_date_variable_cost(
            import_year_data,
            import_daily_data,
            import_cost,
            import_tariff_coverage,
            daily_electricity_price_map,
            current_month_start,
            authoritative_fetch_succeeded=(
                import_year_fetch_succeeded and not reuse_cached_import_year
            ),
            daily_fetch_succeeded=import_daily_fetch_succeeded,
            daily_tariff_succeeded=electricity_daily_tariff_succeeded,
        )

        if now.month == 1:
            yearly_export_usage = export_usage
        elif reuse_cached_export_year:
            yearly_export_usage = previous_data["electricity_export_year_to_date"]
        elif export_year_fetch_succeeded:
            yearly_export_usage = sum(
                data.get("usage", 0.0) for data in export_year_data
            )
        else:
            yearly_export_usage = None

        (
            yearly_export_cost,
            yearly_export_tariff_coverage,
        ) = _year_to_date_variable_cost(
            export_year_data,
            export_daily_data,
            export_cost,
            export_tariff_coverage,
            daily_electricity_price_map,
            current_month_start,
            authoritative_fetch_succeeded=(
                export_year_fetch_succeeded and not reuse_cached_export_year
            ),
            daily_fetch_succeeded=export_daily_fetch_succeeded,
            daily_tariff_succeeded=electricity_daily_tariff_succeeded,
        )

        api_vaste_kosten_gas = 0.0
        if gas_data:
            vk = gas_data[0].get("vasteKosten")
            if isinstance(vk, dict):
                api_vaste_kosten_gas = sum(
                    _numeric_tariff_value(vk.get(key)) or 0.0
                    for key in (
                        "abonnementsKosten",
                        "netbeheerKosten",
                        "verminderingEnergieBelasting",
                    )
                )

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

        if abs(api_vaste_kosten) > 0.01:
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
            gas_fixed_cost_source = previous_data.get(
                "gas_fixed_cost_source", "unavailable"
            )
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
        )

        return {
            "electricity_import_month_to_date": import_usage,
            "electricity_import_month_to_date_cost": import_cost,
            "electricity_export_month_to_date": export_usage,
            "electricity_export_month_to_date_credit": export_cost,
            "electricity_import_year_to_date": yearly_import_usage,
            "electricity_import_year_to_date_cost": yearly_import_cost,
            "electricity_export_year_to_date": yearly_export_usage,
            "electricity_export_year_to_date_credit": yearly_export_cost,
            "electricity_month_to_date_fixed_cost": total_fixed_costs,
            "electricity_month_to_date_total_cost": total_cost,
            "electricity_import_tariff_coverage": import_tariff_coverage,
            "electricity_export_tariff_coverage": export_tariff_coverage,
            "electricity_import_year_to_date_tariff_coverage": (
                yearly_import_tariff_coverage
            ),
            "electricity_export_year_to_date_tariff_coverage": (
                yearly_export_tariff_coverage
            ),
            "electricity_fixed_cost_source": electricity_fixed_cost_source,
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
            "year_to_date_cost_calculation_method": (
                "daily_closed_months_hourly_current_month"
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
    ) -> None:
        """Insert ANWB statistics."""
        for sensor_type, is_cost, data_list, p_map in [
            ("import_usage", False, import_data, price_map),
            ("export_usage", False, export_data, price_map),
            ("import_cost", True, import_data, price_map),
            ("export_cost", True, export_data, price_map),
            ("gas_usage", False, gas_data, gas_price_map),
            ("gas_cost", True, gas_data, gas_price_map),
        ]:
            if not data_list:
                continue

            unit_class = (
                None
                if is_cost
                else (
                    "energy"
                    if "import" in sensor_type or "export" in sensor_type
                    else "volume"
                )
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
            statistic_id = (
                f"{DOMAIN}:{sensor_type}_{self._account_number}".lower().replace(
                    "-", "_"
                )
            )

            parsed_data = [
                (start_time, data)
                for data in data_list
                if (start_time := _parse_api_datetime(data.get("startDate")))
                is not None
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
                    if (stat_start := _stat_start_datetime(row.get("start")))
                    is not None
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
                row
                for stat_start, row in existing_stats
                if stat_start < first_write_time
            ]
            if preceding_stats:
                running_sum = (
                    _numeric_tariff_value(preceding_stats[-1].get("sum")) or 0.0
                )
            else:
                first_existing = existing_by_start.get(first_write_time)
                existing_sum = (
                    _numeric_tariff_value(first_existing.get("sum"))
                    if first_existing
                    else None
                )
                existing_state = (
                    _numeric_tariff_value(first_existing.get("state"))
                    if first_existing
                    else None
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
