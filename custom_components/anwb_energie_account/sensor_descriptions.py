"""Sensor entity descriptions and attribute builders for ANWB Energie Account."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import CURRENCY_EURO, UnitOfEnergy, UnitOfVolume
from homeassistant.util import dt as dt_util

from .coordinator import _normalize_api_datetime_key


@dataclass(frozen=True, kw_only=True)
class ANWBSensorEntityDescription(SensorEntityDescription):
    """Sensor description subclass for ANWB Energie Account entities."""

    data_key: str | None = None
    period: Literal["month", "year"] | None = None
    source: Literal["consumption", "pricing"] = "consumption"
    is_gas: bool = False
    legacy: bool = False
    optional: bool = False
    estimate: bool = False
    attributes_fn: Callable[[Mapping[str, Any]], dict[str, Any]] | None = None
    value_fn: Callable[[Mapping[str, Any]], float | None] | None = None


def _current_hour_price(data_key: str) -> Callable[[Mapping[str, Any]], float | None]:
    """Factory returning a value function for current hourly price."""

    def _value(data: Mapping[str, Any]) -> float | None:
        current_hour_str = _normalize_api_datetime_key(dt_util.utcnow().isoformat())
        prices = data.get(data_key, {})
        if current_hour_str in prices:
            return round(prices[current_hour_str] / 100.0, 4)
        return None

    return _value


def _price_attrs(
    data_key: str,
    *,
    market_data_key: str | None = None,
) -> Callable[[Mapping[str, Any]], dict[str, Any]]:
    """Factory returning attribute function for hourly price sensors."""

    def _attrs(data: Mapping[str, Any]) -> dict[str, Any]:
        prices = data.get(data_key, {})
        market_prices = (
            data.get(market_data_key, {}) if market_data_key is not None else None
        )
        price_records = []
        for k, v in prices.items():
            record: dict[str, Any] = {"start_time": k, "price": round(v / 100.0, 4)}
            if market_prices is not None and k in market_prices:
                record["market_price"] = round(market_prices[k] / 100.0, 4)
            price_records.append(record)
        return {"prices": price_records}

    return _attrs


def _estimate_base(
    data: Mapping[str, Any],
    *,
    cost_source: bool = True,
) -> dict[str, Any]:
    """Build base dictionary for cost estimate sensors."""
    attrs: dict[str, Any] = {"estimated": True}
    if cost_source and (source := data.get("electricity_cost_source")):
        attrs["cost_source"] = source
    return attrs


def _electricity_import_month_cost_attrs(data: Mapping[str, Any]) -> dict[str, Any]:
    """Build attributes for electricity monthly import cost."""
    attrs = _estimate_base(data)
    attrs["tariff_coverage"] = data.get("electricity_import_tariff_coverage")
    return attrs


def _electricity_export_month_credit_attrs(data: Mapping[str, Any]) -> dict[str, Any]:
    """Build attributes for electricity monthly export credit."""
    attrs = _estimate_base(data)
    if breakdown := data.get("electricity_export_month_to_date_credit_breakdown"):
        attrs.update(breakdown)
    attrs["tariff_coverage"] = data.get("electricity_export_tariff_coverage")
    if (export_price_basis := data.get("export_price_basis")) is not None:
        attrs["export_price_basis"] = export_price_basis
    return attrs


def _electricity_export_month_market_value_attrs(
    data: Mapping[str, Any],
) -> dict[str, Any]:
    """Build attributes for electricity monthly export market value."""
    attrs = _estimate_base(data)
    attrs["tariff_coverage"] = data.get(
        "electricity_export_market_value_tariff_coverage"
    )
    return attrs


def _electricity_import_year_cost_attrs(data: Mapping[str, Any]) -> dict[str, Any]:
    """Build attributes for electricity yearly import cost."""
    attrs = _estimate_base(data)
    attrs["tariff_coverage"] = data.get(
        "electricity_import_year_to_date_tariff_coverage"
    )
    attrs["calculation_method"] = data.get(
        "electricity_year_to_date_cost_calculation_method",
        data.get("year_to_date_cost_calculation_method"),
    )
    return attrs


def _electricity_export_year_credit_attrs(data: Mapping[str, Any]) -> dict[str, Any]:
    """Build attributes for electricity yearly export credit."""
    attrs = _estimate_base(data)
    if breakdown := data.get("electricity_export_year_to_date_credit_breakdown"):
        attrs.update(breakdown)
    attrs["tariff_coverage"] = data.get(
        "electricity_export_year_to_date_tariff_coverage"
    )
    attrs["calculation_method"] = data.get(
        "electricity_year_to_date_cost_calculation_method",
        data.get("year_to_date_cost_calculation_method"),
    )
    if (export_price_basis := data.get("export_price_basis")) is not None:
        attrs["export_price_basis"] = export_price_basis
    return attrs


def _electricity_export_year_market_value_attrs(
    data: Mapping[str, Any],
) -> dict[str, Any]:
    """Build attributes for electricity yearly export market value."""
    attrs = _estimate_base(data)
    attrs["tariff_coverage"] = data.get(
        "electricity_export_year_to_date_market_value_tariff_coverage"
    )
    return attrs


def _electricity_month_fixed_cost_attrs(data: Mapping[str, Any]) -> dict[str, Any]:
    """Build attributes for electricity fixed cost."""
    attrs = _estimate_base(data, cost_source=False)
    attrs["fixed_cost_source"] = data.get("electricity_fixed_cost_source")
    return attrs


def _electricity_month_total_cost_attrs(data: Mapping[str, Any]) -> dict[str, Any]:
    """Build attributes for electricity total cost."""
    attrs = _estimate_base(data)
    attrs["tariff_coverage"] = {
        "import": data.get("electricity_import_tariff_coverage"),
        "export": data.get("electricity_export_tariff_coverage"),
    }
    attrs["fixed_cost_source"] = data.get("electricity_fixed_cost_source")
    return attrs


def _gas_month_cost_attrs(data: Mapping[str, Any]) -> dict[str, Any]:
    """Build attributes for gas monthly cost."""
    attrs = _estimate_base(data, cost_source=False)
    attrs["tariff_coverage"] = data.get("gas_tariff_coverage")
    return attrs


def _gas_year_cost_attrs(data: Mapping[str, Any]) -> dict[str, Any]:
    """Build attributes for gas yearly cost."""
    attrs = _estimate_base(data, cost_source=False)
    attrs["tariff_coverage"] = data.get("gas_year_to_date_tariff_coverage")
    attrs["calculation_method"] = data.get("year_to_date_cost_calculation_method")
    return attrs


def _gas_month_fixed_cost_attrs(data: Mapping[str, Any]) -> dict[str, Any]:
    """Build attributes for gas monthly fixed cost."""
    attrs = _estimate_base(data, cost_source=False)
    attrs["fixed_cost_source"] = data.get("gas_fixed_cost_source")
    return attrs


def _gas_month_total_cost_attrs(data: Mapping[str, Any]) -> dict[str, Any]:
    """Build attributes for gas monthly total cost."""
    attrs = _estimate_base(data, cost_source=False)
    attrs["tariff_coverage"] = data.get("gas_tariff_coverage")
    attrs["fixed_cost_source"] = data.get("gas_fixed_cost_source")
    return attrs


_MODERN_SENSOR_TYPES: tuple[ANWBSensorEntityDescription, ...] = (
    ANWBSensorEntityDescription(
        key="electricity_import_month_to_date",
        translation_key="electricity_import_month_to_date",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
        period="month",
    ),
    ANWBSensorEntityDescription(
        key="electricity_export_month_to_date",
        translation_key="electricity_export_month_to_date",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
        period="month",
    ),
    ANWBSensorEntityDescription(
        key="electricity_import_month_to_date_cost",
        translation_key="electricity_import_month_to_date_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="month",
        estimate=True,
        attributes_fn=_electricity_import_month_cost_attrs,
    ),
    ANWBSensorEntityDescription(
        key="electricity_export_month_to_date_credit",
        translation_key="electricity_export_month_to_date_credit",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="month",
        estimate=True,
        attributes_fn=_electricity_export_month_credit_attrs,
    ),
    ANWBSensorEntityDescription(
        key="electricity_export_month_to_date_market_value",
        translation_key="electricity_export_month_to_date_market_value",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="month",
        estimate=True,
        attributes_fn=_electricity_export_month_market_value_attrs,
    ),
    ANWBSensorEntityDescription(
        key="electricity_import_year_to_date",
        translation_key="electricity_import_year_to_date",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
        period="year",
    ),
    ANWBSensorEntityDescription(
        key="electricity_import_year_to_date_cost",
        translation_key="electricity_import_year_to_date_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="year",
        estimate=True,
        attributes_fn=_electricity_import_year_cost_attrs,
    ),
    ANWBSensorEntityDescription(
        key="electricity_export_year_to_date",
        translation_key="electricity_export_year_to_date",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
        period="year",
    ),
    ANWBSensorEntityDescription(
        key="electricity_export_year_to_date_credit",
        translation_key="electricity_export_year_to_date_credit",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="year",
        estimate=True,
        attributes_fn=_electricity_export_year_credit_attrs,
    ),
    ANWBSensorEntityDescription(
        key="electricity_export_year_to_date_market_value",
        translation_key="electricity_export_year_to_date_market_value",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="year",
        estimate=True,
        attributes_fn=_electricity_export_year_market_value_attrs,
    ),
    ANWBSensorEntityDescription(
        key="electricity_month_to_date_fixed_cost",
        translation_key="electricity_month_to_date_fixed_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="month",
        optional=True,
        estimate=True,
        attributes_fn=_electricity_month_fixed_cost_attrs,
    ),
    ANWBSensorEntityDescription(
        key="electricity_month_to_date_total_cost",
        translation_key="electricity_month_to_date_total_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="month",
        optional=True,
        estimate=True,
        attributes_fn=_electricity_month_total_cost_attrs,
    ),
    ANWBSensorEntityDescription(
        key="electricity_current_price",
        translation_key="electricity_current_price",
        native_unit_of_measurement=f"{CURRENCY_EURO}/{UnitOfEnergy.KILO_WATT_HOUR}",
        state_class=SensorStateClass.MEASUREMENT,
        source="pricing",
        value_fn=_current_hour_price("prices_today"),
        attributes_fn=_price_attrs(
            "prices_today", market_data_key="market_prices_today"
        ),
    ),
    ANWBSensorEntityDescription(
        key="electricity_market_price",
        translation_key="electricity_market_price",
        native_unit_of_measurement=f"{CURRENCY_EURO}/{UnitOfEnergy.KILO_WATT_HOUR}",
        state_class=SensorStateClass.MEASUREMENT,
        source="pricing",
        value_fn=_current_hour_price("market_prices_today"),
        attributes_fn=_price_attrs("market_prices_today"),
    ),
    ANWBSensorEntityDescription(
        key="gas_month_to_date",
        translation_key="gas_month_to_date",
        device_class=SensorDeviceClass.GAS,
        native_unit_of_measurement=UnitOfVolume.CUBIC_METERS,
        state_class=SensorStateClass.TOTAL_INCREASING,
        period="month",
        is_gas=True,
    ),
    ANWBSensorEntityDescription(
        key="gas_month_to_date_cost",
        translation_key="gas_month_to_date_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="month",
        is_gas=True,
        estimate=True,
        attributes_fn=_gas_month_cost_attrs,
    ),
    ANWBSensorEntityDescription(
        key="gas_year_to_date",
        translation_key="gas_year_to_date",
        device_class=SensorDeviceClass.GAS,
        native_unit_of_measurement=UnitOfVolume.CUBIC_METERS,
        state_class=SensorStateClass.TOTAL_INCREASING,
        period="year",
        is_gas=True,
    ),
    ANWBSensorEntityDescription(
        key="gas_year_to_date_cost",
        translation_key="gas_year_to_date_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="year",
        is_gas=True,
        estimate=True,
        attributes_fn=_gas_year_cost_attrs,
    ),
    ANWBSensorEntityDescription(
        key="gas_month_to_date_fixed_cost",
        translation_key="gas_month_to_date_fixed_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="month",
        is_gas=True,
        optional=True,
        estimate=True,
        attributes_fn=_gas_month_fixed_cost_attrs,
    ),
    ANWBSensorEntityDescription(
        key="gas_month_to_date_total_cost",
        translation_key="gas_month_to_date_total_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
        period="month",
        is_gas=True,
        optional=True,
        estimate=True,
        attributes_fn=_gas_month_total_cost_attrs,
    ),
    ANWBSensorEntityDescription(
        key="gas_current_price",
        translation_key="gas_current_price",
        native_unit_of_measurement=f"{CURRENCY_EURO}/{UnitOfVolume.CUBIC_METERS}",
        state_class=SensorStateClass.MEASUREMENT,
        source="pricing",
        is_gas=True,
        value_fn=_current_hour_price("gas_prices_today"),
        attributes_fn=_price_attrs("gas_prices_today"),
    ),
)


_LEGACY_KEY_MAPPING: tuple[tuple[str, str], ...] = (
    ("import_usage", "electricity_import_month_to_date"),
    ("export_usage", "electricity_export_month_to_date"),
    ("import_cost", "electricity_import_month_to_date_cost"),
    ("export_cost", "electricity_export_month_to_date_credit"),
    ("yearly_import_usage", "electricity_import_year_to_date"),
    ("yearly_export_usage", "electricity_export_year_to_date"),
    ("fixed_cost", "electricity_month_to_date_fixed_cost"),
    ("total_cost", "electricity_month_to_date_total_cost"),
    ("current_price", "electricity_current_price"),
    ("gas_usage", "gas_month_to_date"),
    ("gas_cost", "gas_month_to_date_cost"),
    ("yearly_gas_usage", "gas_year_to_date"),
    ("fixed_cost_gas", "gas_month_to_date_fixed_cost"),
    ("total_cost_gas", "gas_month_to_date_total_cost"),
    ("current_gas_price", "gas_current_price"),
)


def _build_sensor_types() -> tuple[ANWBSensorEntityDescription, ...]:
    """Combine modern descriptions with derived legacy descriptions."""
    modern_map = {desc.key: desc for desc in _MODERN_SENSOR_TYPES}
    descriptions = list(_MODERN_SENSOR_TYPES)

    for legacy_key, modern_key in _LEGACY_KEY_MAPPING:
        modern = modern_map[modern_key]
        descriptions.append(
            replace(
                modern,
                key=legacy_key,
                translation_key=legacy_key,
                data_key=None if modern.value_fn is not None else modern_key,
                legacy=True,
            )
        )

    return tuple(descriptions)


SENSOR_TYPES: tuple[ANWBSensorEntityDescription, ...] = _build_sensor_types()
