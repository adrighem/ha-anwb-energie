"""Recorder statistics insertion helpers for ANWB Energie Account."""

from __future__ import annotations

import logging
import math
import sys
from datetime import datetime, timedelta
from typing import Any

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
from homeassistant.const import CURRENCY_EURO, UnitOfEnergy, UnitOfVolume
from homeassistant.core import HomeAssistant

from .const import DOMAIN
from .tariffs import NET_METERING_END_DATE, _numeric_tariff_value
from .time_utils import (
    _normalize_api_datetime_key,
    _parse_api_datetime,
    _stat_start_datetime,
)

_LOGGER = logging.getLogger(__name__)


def _recorder_helper(name: str, fallback: Any) -> Any:
    coord_mod = sys.modules.get("custom_components.anwb_energie_account.coordinator")
    if coord_mod and hasattr(coord_mod, name):
        return getattr(coord_mod, name)
    return fallback


async def async_insert_statistics(
    hass: HomeAssistant,
    account_number: str,
    import_data: list[dict[str, Any]],
    export_data: list[dict[str, Any]],
    price_map: dict[str, float],
    gas_data: list[dict[str, Any]],
    gas_price_map: dict[str, float],
    export_price_map: dict[str, float],
) -> None:
    """Insert ANWB statistics."""
    helper_get_instance = _recorder_helper("get_instance", get_instance)
    helper_stat_data = _recorder_helper("StatisticData", StatisticData)
    helper_stat_metadata = _recorder_helper("StatisticMetaData", StatisticMetaData)
    helper_stat_mean_type = _recorder_helper("StatisticMeanType", StatisticMeanType)
    helper_add_stats = _recorder_helper(
        "async_add_external_statistics", async_add_external_statistics
    )
    helper_stats_period = _recorder_helper(
        "statistics_during_period", statistics_during_period
    )

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
        statistic_id = f"{DOMAIN}:{sensor_type}_{account_number}".lower().replace(
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
        stat = await helper_get_instance(hass).async_add_executor_job(
            helper_stats_period,
            hass,
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
                helper_stat_data(
                    start=start_time,
                    state=value,
                    sum=running_sum,
                )
            )

        if statistics:
            name = sensor_type.replace("_", " ").title()
            metadata = helper_stat_metadata(
                mean_type=helper_stat_mean_type.NONE,
                has_sum=True,
                name=f"ANWB Account {account_number} {name}",
                source=DOMAIN,
                statistic_id=statistic_id,
                unit_class=unit_class,
                unit_of_measurement=unit,
            )
            helper_add_stats(hass, metadata, statistics)
