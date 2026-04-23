"""Coordinator for ANWB Energie Account."""

from __future__ import annotations

import asyncio
import calendar
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import Any

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

_LOGGER = logging.getLogger(__name__)


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
        self._kraken_token: str | None = None
        self._account_number: str | None = None
        self._account_address: str | None = None

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
                accounts = data.get("data", {}).get("viewer", {}).get("accounts", [])
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

    async def _async_fetch_data(self, url: str, kraken_token: str) -> dict[str, Any]:
        """Fetch JSON data from API."""
        session = self.auth.websession
        headers = {"Authorization": f"Bearer {kraken_token}"}
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status in (401, 403):
                    raise UpdateFailed(
                        f"Kraken token expired or invalid fetching data from {url}"
                    )
                if resp.status == 404:
                    return {}
                resp.raise_for_status()
                return await resp.json()
        except ClientError as err:
            if getattr(err, "status", None) in (401, 403):
                raise UpdateFailed(
                    f"Kraken token expired or invalid fetching data from {url}"
                ) from err
            raise UpdateFailed(f"Error fetching data from {url}: {err}") from err


class ANWBPricingCoordinator(ANWBBaseCoordinator):
    """Coordinator to fetch pricing data smartly."""

    def __init__(
        self,
        hass: HomeAssistant,
        auth: AsyncConfigEntryAuth,
        config_entry: ConfigEntry,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_pricing",
            update_interval=timedelta(minutes=30),
            auth=auth,
            config_entry=config_entry,
        )

    async def _async_update_data(self) -> dict[str, Any]:
        try:
            return await self._async_update_data_internal()
        except UpdateFailed as err:
            if "Kraken token expired" in str(err):
                _LOGGER.debug("Kraken token expired, refreshing and retrying")
                self._kraken_token = None
                return await self._async_update_data_internal()
            raise

    async def _async_update_data_internal(self) -> dict[str, Any]:
        if not self._kraken_token:
            self._kraken_token = await self._async_get_kraken_token()

        if not self._account_number:
            info = await self._async_get_account_info(self._kraken_token)
            self._account_number = info["number"]
            self._account_address = info["address"]

        now = dt_util.utcnow()
        now.replace(minute=0, second=0, microsecond=0).strftime(
            "%Y-%m-%dT%H:00:00.000Z"
        )
        today_str = now.strftime("%Y-%m-%d")
        tomorrow = now + timedelta(days=1)
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")

        has_tomorrow_electricity = False
        has_tomorrow_gas = False

        if self.data:
            prices = self.data.get("prices_today", {})
            gas_prices = self.data.get("gas_prices_today", {})
            for k in prices:
                if k.startswith(tomorrow_str):
                    has_tomorrow_electricity = True
                    break
            for k in gas_prices:
                if k.startswith(tomorrow_str):
                    has_tomorrow_gas = True
                    break

        fetch_electricity = not has_tomorrow_electricity and now.hour >= 15
        fetch_gas = not has_tomorrow_gas and now.hour >= 6

        if not self.data:
            fetch_electricity = True
            fetch_gas = True

        if not fetch_electricity and not fetch_gas and self.data:
            return dict(self.data)

        price_map: dict[str, float] = (
            dict(self.data.get("_raw_price_map", {})) if self.data else {}
        )
        gas_price_map: dict[str, float] = (
            dict(self.data.get("_raw_gas_price_map", {})) if self.data else {}
        )

        price_tasks = []
        gas_price_tasks = []

        days_to_fetch = [now, tomorrow]

        for d in days_to_fetch:
            day_start = f"{d.year}-{d.month:02d}-{d.day:02d}T00:00:00.000Z"
            day_end = f"{d.year}-{d.month:02d}-{d.day:02d}T23:59:59.999Z"
            if fetch_electricity:
                url_prices = (
                    "https://api.anwb.nl/energy/energy-services/v2/tarieven/electricity"
                    f"?startDate={day_start}&endDate={day_end}&interval=HOUR"
                )
                price_tasks.append(
                    self._async_fetch_data(url_prices, self._kraken_token)
                )

            if fetch_gas:
                url_gas_prices = (
                    "https://api.anwb.nl/energy/energy-services/v2/tarieven/gas"
                    f"?startDate={day_start}&endDate={day_end}&interval=HOUR"
                )
                gas_price_tasks.append(
                    self._async_fetch_data(url_gas_prices, self._kraken_token)
                )

        try:
            if price_tasks:
                prices_results = await asyncio.gather(*price_tasks)
                for res_prices in prices_results:
                    if res_prices.get("data"):
                        for p in res_prices["data"]:
                            dt_str = p.get("date", "").replace("+00:00", ".000Z")
                            vals = p.get("values", {})
                            price_map[dt_str] = vals.get("allInPrijs", 0.0)
        except UpdateFailed as err:
            if "Kraken token expired" in str(err):
                raise
            pass

        try:
            if gas_price_tasks:
                gas_prices_results = await asyncio.gather(*gas_price_tasks)
                for res_prices in gas_prices_results:
                    if res_prices.get("data"):
                        for p in res_prices["data"]:
                            dt_str = p.get("date", "").replace("+00:00", ".000Z")
                            vals = p.get("values", {})
                            gas_price_map[dt_str] = vals.get("allInPrijs", 0.0)
        except UpdateFailed as err:
            if "Kraken token expired" in str(err):
                raise
            pass

        filtered_prices = {
            k: v
            for k, v in price_map.items()
            if k.startswith(today_str) or k.startswith(tomorrow_str)
        }

        filtered_gas_prices = {
            k: v
            for k, v in gas_price_map.items()
            if k.startswith(today_str) or k.startswith(tomorrow_str)
        }

        return {
            "account_number": self._account_number,
            "account_address": self._account_address,
            "prices_today": filtered_prices,
            "gas_prices_today": filtered_gas_prices,
            "_raw_price_map": price_map,
            "_raw_gas_price_map": gas_price_map,
        }


class ANWBConsumptionCoordinator(ANWBBaseCoordinator):
    """Class to manage fetching ANWB Energie Account consumption data."""

    def __init__(
        self,
        hass: HomeAssistant,
        auth: AsyncConfigEntryAuth,
        config_entry: ConfigEntry,
    ) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_consumption",
            update_interval=timedelta(hours=6),
            auth=auth,
            config_entry=config_entry,
        )

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from ANWB API."""
        try:
            return await self._async_update_data_internal()
        except UpdateFailed as err:
            if "Kraken token expired" in str(err):
                _LOGGER.debug("Kraken token expired, refreshing and retrying")
                self._kraken_token = None
                return await self._async_update_data_internal()
            raise

    async def _async_update_data_internal(self) -> dict[str, Any]:
        if not self._kraken_token:
            self._kraken_token = await self._async_get_kraken_token()

        if not self._account_number:
            info = await self._async_get_account_info(self._kraken_token)
            self._account_number = info["number"]
            self._account_address = info["address"]

        now = dt_util.utcnow()
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
        url_import_month = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/electricity/cache"
            f"?startDate={c_start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=MONTH"
        )
        url_export_month = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/production/cache"
            f"?startDate={c_start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=MONTH"
        )
        url_import_gas = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/gas/cache"
            f"?startDate={start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=HOUR"
        )
        url_import_gas_month = (
            "https://api.anwb.nl/energy/energy-services/v1/accounts/"
            f"{self._account_number}/gas/cache"
            f"?startDate={c_start}&endDate={end}"
            f"&contractStartDate={c_start}&interval=MONTH"
        )

        (
            res_import,
            res_export,
            res_imp_month,
            res_exp_month,
            res_import_gas,
            res_import_gas_month,
        ) = await asyncio.gather(
            self._async_fetch_data(url_import, self._kraken_token),
            self._async_fetch_data(url_export, self._kraken_token),
            self._async_fetch_data(url_import_month, self._kraken_token),
            self._async_fetch_data(url_export_month, self._kraken_token),
            self._async_fetch_data(url_import_gas, self._kraken_token),
            self._async_fetch_data(url_import_gas_month, self._kraken_token),
        )

        price_map: dict[str, float] = {}
        end_day = min(last_day, now.day + 1)

        gas_price_map: dict[str, float] = {}
        price_tasks = []
        gas_price_tasks = []
        for d in range(1, end_day + 1):
            day_start = f"{now.year}-{now.month:02d}-{d:02d}T00:00:00.000Z"
            day_end = f"{now.year}-{now.month:02d}-{d:02d}T23:59:59.999Z"
            url_prices = (
                "https://api.anwb.nl/energy/energy-services/v2/tarieven/electricity"
                f"?startDate={day_start}&endDate={day_end}&interval=HOUR"
            )
            price_tasks.append(self._async_fetch_data(url_prices, self._kraken_token))

            url_gas_prices = (
                "https://api.anwb.nl/energy/energy-services/v2/tarieven/gas"
                f"?startDate={day_start}&endDate={day_end}&interval=HOUR"
            )
            gas_price_tasks.append(
                self._async_fetch_data(url_gas_prices, self._kraken_token)
            )

        try:
            prices_results = await asyncio.gather(*price_tasks)
            for res_prices in prices_results:
                if res_prices.get("data"):
                    for p in res_prices["data"]:
                        dt_str = p.get("date", "").replace("+00:00", ".000Z")
                        vals = p.get("values", {})
                        price_map[dt_str] = vals.get("allInPrijs", 0.0)
        except UpdateFailed as err:
            if "Kraken token expired" in str(err):
                raise
            pass

        try:
            gas_prices_results = await asyncio.gather(*gas_price_tasks)
            for res_prices in gas_prices_results:
                if res_prices.get("data"):
                    for p in res_prices["data"]:
                        dt_str = p.get("date", "").replace("+00:00", ".000Z")
                        vals = p.get("values", {})
                        gas_price_map[dt_str] = vals.get("allInPrijs", 0.0)
        except UpdateFailed as err:
            if "Kraken token expired" in str(err):
                raise
            pass

        import_usage = 0.0
        import_cost = 0.0
        api_vaste_kosten = 0.0

        if res_import.get("data"):
            first_entry = res_import["data"][0]
            if "vasteKosten" in first_entry:
                vk = first_entry["vasteKosten"]
                api_vaste_kosten = (
                    vk.get("abonnementsKosten", 0)
                    + vk.get("netbeheerKosten", 0)
                    + vk.get("verminderingEnergieBelasting", 0)
                )

            for d in res_import["data"]:
                usage = d.get("usage", 0.0)
                timestamp = d.get("startDate", "").replace("+00:00", ".000Z")
                price_cents = price_map.get(timestamp, 0.0)

                import_usage += usage
                import_cost += (usage * price_cents) / 100.0

        export_usage = 0.0
        export_cost = 0.0
        if res_export.get("data"):
            for d in res_export["data"]:
                usage = d.get("usage", 0.0)
                timestamp = d.get("startDate", "").replace("+00:00", ".000Z")
                price_cents = price_map.get(timestamp, 0.0)

                export_usage += usage
                export_cost += (usage * price_cents) / 100.0

        yearly_import_usage = sum(
            d.get("usage", 0.0) for d in res_imp_month.get("data", [])
        )
        yearly_export_usage = sum(
            d.get("usage", 0.0) for d in res_exp_month.get("data", [])
        )

        gas_usage = 0.0
        gas_cost = 0.0
        api_vaste_kosten_gas = 0.0

        if res_import_gas.get("data"):
            first_entry = res_import_gas["data"][0]
            if "vasteKosten" in first_entry:
                vk = first_entry["vasteKosten"]
                api_vaste_kosten_gas = (
                    vk.get("abonnementsKosten", 0)
                    + vk.get("netbeheerKosten", 0)
                    + vk.get("verminderingEnergieBelasting", 0)
                )

            for d in res_import_gas["data"]:
                usage = d.get("usage", 0.0)
                timestamp = d.get("startDate", "").replace("+00:00", ".000Z")
                price_cents = gas_price_map.get(timestamp, 0.0)

                gas_usage += usage
                gas_cost += (usage * price_cents) / 100.0

        yearly_gas_usage = sum(
            d.get("usage", 0.0) for d in res_import_gas_month.get("data", [])
        )

        days_with_data = now.day
        if abs(api_vaste_kosten) > 0.01:
            total_fixed_costs = api_vaste_kosten
        else:
            latest_ts = None
            all_data = res_import.get("data", []) + res_export.get("data", [])
            for d in all_data:
                if d.get("usage", 0.0) > 0.0:
                    ts = d.get("startDate")
                    if not latest_ts or ts > latest_ts:
                        latest_ts = ts

            if latest_ts:
                days_with_data = datetime.strptime(latest_ts[:10], "%Y-%m-%d").day

            fraction = days_with_data / float(last_day)

            total_fixed_costs = (
                (VASTE_LEVERINGSKOSTEN * fraction)
                + (NETBEHEERKOSTEN * fraction)
                + (VERMINDERING_ENERGIEBELASTING * fraction)
            )

        if abs(api_vaste_kosten_gas) > 0.01:
            total_fixed_costs_gas = api_vaste_kosten_gas
        else:
            latest_ts_gas = None
            for d in res_import_gas.get("data", []):
                if d.get("usage", 0.0) > 0.0:
                    ts = d.get("startDate")
                    if not latest_ts_gas or ts > latest_ts_gas:
                        latest_ts_gas = ts

            days_with_data_gas = now.day
            if latest_ts_gas:
                parsed_dt_gas = dt_util.parse_datetime(latest_ts_gas)
                if parsed_dt_gas:
                    days_with_data_gas = dt_util.as_local(parsed_dt_gas).day
                else:
                    days_with_data_gas = datetime.strptime(
                        latest_ts_gas[:10], "%Y-%m-%d"
                    ).day

            fraction_gas = days_with_data_gas / float(last_day)
            total_fixed_costs_gas = (
                (VASTE_LEVERINGSKOSTEN_GAS * fraction_gas)
                + (NETBEHEERKOSTEN_GAS * fraction_gas)
                + (VERMINDERING_ENERGIEBELASTING_GAS * fraction_gas)
            )

        total_cost = import_cost - export_cost + total_fixed_costs
        total_cost_gas = gas_cost + total_fixed_costs_gas

        await self._insert_statistics(
            res_import.get("data", []),
            res_export.get("data", []),
            price_map,
            res_import_gas.get("data", []),
            gas_price_map,
        )

        return {
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
        for sensor_type, is_production, is_cost, data_list, p_map in [
            ("import_usage", False, False, import_data, price_map),
            ("export_usage", True, False, export_data, price_map),
            ("import_cost", False, True, import_data, price_map),
            ("export_cost", True, True, export_data, price_map),
            ("gas_usage", False, False, gas_data, gas_price_map),
            ("gas_cost", False, True, gas_data, gas_price_map),
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

            sorted_data = sorted(data_list, key=lambda x: x["startDate"])
            if not sorted_data:
                continue

            first_dt_str = sorted_data[0]["startDate"].replace("+00:00", ".000Z")
            if first_dt_str.endswith("Z"):
                first_dt_str = first_dt_str[:-1] + "+00:00"
            from_time = dt_util.parse_datetime(first_dt_str)
            if from_time is None:
                continue

            start = from_time - timedelta(hours=1)
            stat = await get_instance(self.hass).async_add_executor_job(
                statistics_during_period,
                self.hass,
                start,
                None,
                {statistic_id},
                "hour",
                None,
                {"sum"},
            )

            _sum = 0.0
            last_stats_time = None

            if statistic_id in stat and stat[statistic_id]:
                first_stat = stat[statistic_id][0]
                _sum = first_stat.get("sum", 0.0)
                last_stats_time = first_stat["start"]

            statistics = []
            last_stats_time_dt = (
                dt_util.utc_from_timestamp(last_stats_time) if last_stats_time else None
            )

            for data in sorted_data:
                dt_str = data["startDate"].replace("+00:00", ".000Z")
                if dt_str.endswith("Z"):
                    dt_str = dt_str[:-1] + "+00:00"
                start_time = dt_util.parse_datetime(dt_str)

                if start_time is None or (
                    last_stats_time_dt is not None and start_time <= last_stats_time_dt
                ):
                    continue

                usage = data.get("usage", 0.0)
                if is_cost:
                    timestamp = data.get("startDate", "").replace("+00:00", ".000Z")
                    price_cents = p_map.get(timestamp, 0.0)
                    val = (usage * price_cents) / 100.0
                else:
                    val = usage

                _sum += val

                statistics.append(
                    StatisticData(
                        start=start_time,
                        state=val,
                        sum=_sum,
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


type ANWBEnergieAccountConfigEntry = ConfigEntry[ANWBEnergieAccountData]
