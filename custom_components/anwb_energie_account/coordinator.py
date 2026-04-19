"""Coordinator for ANWB Energie Account."""

from __future__ import annotations

import asyncio
import calendar
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
from homeassistant.const import CURRENCY_EURO, UnitOfEnergy
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
)

_LOGGER = logging.getLogger(__name__)

type ANWBEnergieAccountConfigEntry = ConfigEntry[ANWBDataUpdateCoordinator]


class ANWBDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Class to manage fetching ANWB Energie Account data."""

    config_entry: ANWBEnergieAccountConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        auth: AsyncConfigEntryAuth,
        config_entry: ANWBEnergieAccountConfigEntry,
    ) -> None:
        """Initialize coordinator."""
        self.config_entry = config_entry
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(minutes=30),
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
            raise ConfigEntryAuthFailed("Failed to get access token") from err

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
                    raise ConfigEntryAuthFailed("Failed to fetch account number (auth)")
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
                raise ConfigEntryAuthFailed(
                    "Failed to fetch account number (auth)"
                ) from err
            raise UpdateFailed(f"Failed to fetch account number: {err}") from err

    async def _async_fetch_data(self, url: str, kraken_token: str) -> dict[str, Any]:
        """Fetch JSON data from API."""
        session = self.auth.websession
        headers = {"Authorization": f"Bearer {kraken_token}"}
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status in (401, 403):
                    raise ConfigEntryAuthFailed(f"Auth failed fetching data from {url}")
                resp.raise_for_status()
                return await resp.json()
        except ClientError as err:
            if getattr(err, "status", None) in (401, 403):
                raise ConfigEntryAuthFailed(
                    f"Auth failed fetching data from {url}"
                ) from err
            raise UpdateFailed(f"Error fetching data from {url}: {err}") from err

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data from ANWB API."""
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

        try:
            res_import, res_export, res_imp_month, res_exp_month = await asyncio.gather(
                self._async_fetch_data(url_import, self._kraken_token),
                self._async_fetch_data(url_export, self._kraken_token),
                self._async_fetch_data(url_import_month, self._kraken_token),
                self._async_fetch_data(url_export_month, self._kraken_token),
            )
        except UpdateFailed:
            self._kraken_token = None
            raise

        price_map: dict[str, float] = {}
        end_day = min(last_day, now.day + 1)

        price_tasks = []
        for d in range(1, end_day + 1):
            day_start = f"{now.year}-{now.month:02d}-{d:02d}T00:00:00.000Z"
            day_end = f"{now.year}-{now.month:02d}-{d:02d}T23:59:59.999Z"
            url_prices = (
                "https://api.anwb.nl/energy/energy-services/v2/tarieven/electricity"
                f"?startDate={day_start}&endDate={day_end}&interval=HOUR"
            )
            price_tasks.append(self._async_fetch_data(url_prices, self._kraken_token))

        try:
            prices_results = await asyncio.gather(*price_tasks)
            for res_prices in prices_results:
                if res_prices.get("data"):
                    for p in res_prices["data"]:
                        dt_str = p.get("date", "").replace("+00:00", ".000Z")
                        vals = p.get("values", {})
                        price_map[dt_str] = vals.get("allInPrijs", 0.0)
        except UpdateFailed:
            pass

        # Determine current price
        current_price = None
        current_hour_str = now.replace(minute=0, second=0, microsecond=0).strftime(
            "%Y-%m-%dT%H:00:00.000Z"
        )
        if current_hour_str in price_map:
            # Price in the map is in cents, convert to Euro
            current_price = price_map[current_hour_str] / 100.0

        today_str = now.strftime("%Y-%m-%d")
        tomorrow_str = (now + timedelta(days=1)).strftime("%Y-%m-%d")
        filtered_prices = {
            k: v
            for k, v in price_map.items()
            if k.startswith(today_str) or k.startswith(tomorrow_str)
        }

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
                timestamp = d.get("startDate")
                price_cents = price_map.get(timestamp, 0.0)

                import_usage += usage
                import_cost += (usage * price_cents) / 100.0

        export_usage = 0.0
        export_cost = 0.0
        if res_export.get("data"):
            for d in res_export["data"]:
                usage = d.get("usage", 0.0)
                timestamp = d.get("startDate")
                price_cents = price_map.get(timestamp, 0.0)

                export_usage += usage
                export_cost += (usage * price_cents) / 100.0

        yearly_import_usage = sum(
            d.get("usage", 0.0) for d in res_imp_month.get("data", [])
        )
        yearly_export_usage = sum(
            d.get("usage", 0.0) for d in res_exp_month.get("data", [])
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

        total_cost = import_cost - export_cost + total_fixed_costs

        await self._insert_statistics(
            res_import.get("data", []), res_export.get("data", []), price_map
        )

        return {
            "import_usage": import_usage,
            "import_cost": import_cost,
            "export_usage": export_usage,
            "export_cost": export_cost,
            "yearly_import_usage": yearly_import_usage,
            "yearly_export_usage": yearly_export_usage,
            "fixed_cost": total_fixed_costs,
            "total_cost": total_cost,
            "account_number": self._account_number,
            "current_price": current_price,
            "prices_today": filtered_prices,
            "account_address": getattr(self, "_account_address", None),
        }

    async def _insert_statistics(
        self, import_data: list, export_data: list, price_map: dict
    ) -> None:
        """Insert ANWB statistics."""
        for sensor_type, is_production, is_cost, data_list in [
            ("import_usage", False, False, import_data),
            ("export_usage", True, False, export_data),
            ("import_cost", False, True, import_data),
            ("export_cost", True, True, export_data),
        ]:
            if not data_list:
                continue

            unit_class = None if is_cost else "energy"
            unit = CURRENCY_EURO if is_cost else UnitOfEnergy.KILO_WATT_HOUR
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
                    timestamp = data.get("startDate")
                    price_cents = price_map.get(timestamp, 0.0)
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
