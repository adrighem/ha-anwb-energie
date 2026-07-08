"""Sensor platform for ANWB Energie Account."""

from __future__ import annotations

from typing import Any
from datetime import datetime

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import CURRENCY_EURO, UnitOfEnergy, UnitOfVolume
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.helpers.event import async_track_time_change
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .coordinator import (
    ANWBBaseCoordinator,
    ANWBEnergieAccountConfigEntry,
    _normalize_api_datetime_key,
)

ELECTRICITY_PRICE_KEYS = {"current_price", "electricity_current_price"}
ELECTRICITY_MARKET_PRICE_KEYS = {"electricity_market_price"}
GAS_PRICE_KEYS = {"current_gas_price", "gas_current_price"}
PRICE_SENSOR_KEYS = ELECTRICITY_PRICE_KEYS | ELECTRICITY_MARKET_PRICE_KEYS | GAS_PRICE_KEYS

LEGACY_SENSOR_KEYS = {
    "import_usage",
    "export_usage",
    "import_cost",
    "export_cost",
    "yearly_import_usage",
    "yearly_export_usage",
    "fixed_cost",
    "total_cost",
    "current_price",
    "gas_usage",
    "gas_cost",
    "yearly_gas_usage",
    "fixed_cost_gas",
    "total_cost_gas",
    "current_gas_price",
}

SENSOR_DATA_KEYS = {
    "import_usage": "electricity_import_month_to_date",
    "export_usage": "electricity_export_month_to_date",
    "import_cost": "electricity_import_month_to_date_cost",
    "export_cost": "electricity_export_month_to_date_credit",
    "yearly_import_usage": "electricity_import_year_to_date",
    "yearly_export_usage": "electricity_export_year_to_date",
    "fixed_cost": "electricity_month_to_date_fixed_cost",
    "total_cost": "electricity_month_to_date_total_cost",
    "gas_usage": "gas_month_to_date",
    "gas_cost": "gas_month_to_date_cost",
    "yearly_gas_usage": "gas_year_to_date",
    "fixed_cost_gas": "gas_month_to_date_fixed_cost",
    "total_cost_gas": "gas_month_to_date_total_cost",
}

SENSOR_TYPES: tuple[SensorEntityDescription, ...] = (
    SensorEntityDescription(
        key="electricity_import_month_to_date",
        translation_key="electricity_import_month_to_date",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="electricity_export_month_to_date",
        translation_key="electricity_export_month_to_date",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="electricity_import_month_to_date_cost",
        translation_key="electricity_import_month_to_date_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="electricity_export_month_to_date_credit",
        translation_key="electricity_export_month_to_date_credit",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="electricity_import_year_to_date",
        translation_key="electricity_import_year_to_date",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="electricity_export_year_to_date",
        translation_key="electricity_export_year_to_date",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="electricity_month_to_date_fixed_cost",
        translation_key="electricity_month_to_date_fixed_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="electricity_month_to_date_total_cost",
        translation_key="electricity_month_to_date_total_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="electricity_current_price",
        translation_key="electricity_current_price",
        native_unit_of_measurement=f"{CURRENCY_EURO}/{UnitOfEnergy.KILO_WATT_HOUR}",
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="electricity_market_price",
        translation_key="electricity_market_price",
        native_unit_of_measurement=f"{CURRENCY_EURO}/{UnitOfEnergy.KILO_WATT_HOUR}",
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="gas_month_to_date",
        translation_key="gas_month_to_date",
        device_class=SensorDeviceClass.GAS,
        native_unit_of_measurement=UnitOfVolume.CUBIC_METERS,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="gas_month_to_date_cost",
        translation_key="gas_month_to_date_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="gas_year_to_date",
        translation_key="gas_year_to_date",
        device_class=SensorDeviceClass.GAS,
        native_unit_of_measurement=UnitOfVolume.CUBIC_METERS,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="gas_month_to_date_fixed_cost",
        translation_key="gas_month_to_date_fixed_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="gas_month_to_date_total_cost",
        translation_key="gas_month_to_date_total_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="gas_current_price",
        translation_key="gas_current_price",
        native_unit_of_measurement=f"{CURRENCY_EURO}/{UnitOfVolume.CUBIC_METERS}",
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="import_usage",
        translation_key="import_usage",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="export_usage",
        translation_key="export_usage",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="import_cost",
        translation_key="import_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="export_cost",
        translation_key="export_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="yearly_import_usage",
        translation_key="yearly_import_usage",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="yearly_export_usage",
        translation_key="yearly_export_usage",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="fixed_cost",
        translation_key="fixed_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="total_cost",
        translation_key="total_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="current_price",
        translation_key="current_price",
        native_unit_of_measurement=f"{CURRENCY_EURO}/{UnitOfEnergy.KILO_WATT_HOUR}",
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="gas_usage",
        translation_key="gas_usage",
        device_class=SensorDeviceClass.GAS,
        native_unit_of_measurement=UnitOfVolume.CUBIC_METERS,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="gas_cost",
        translation_key="gas_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="yearly_gas_usage",
        translation_key="yearly_gas_usage",
        device_class=SensorDeviceClass.GAS,
        native_unit_of_measurement=UnitOfVolume.CUBIC_METERS,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="fixed_cost_gas",
        translation_key="fixed_cost_gas",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="total_cost_gas",
        translation_key="total_cost_gas",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="current_gas_price",
        translation_key="current_gas_price",
        native_unit_of_measurement=f"{CURRENCY_EURO}/{UnitOfVolume.CUBIC_METERS}",
        state_class=SensorStateClass.MEASUREMENT,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ANWBEnergieAccountConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up ANWB Energie Account sensors based on a config entry."""
    data = entry.runtime_data

    entities = []
    for description in SENSOR_TYPES:
        if description.key in PRICE_SENSOR_KEYS:
            entities.append(ANWBEnergieAccountSensor(data.pricing, description))
        else:
            entities.append(ANWBEnergieAccountSensor(data.consumption, description))

    async_add_entities(entities)


class ANWBEnergieAccountSensor(CoordinatorEntity[ANWBBaseCoordinator], SensorEntity):
    """Representation of an ANWB Energie Account sensor."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: ANWBBaseCoordinator,
        description: SensorEntityDescription,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self.entity_description = description
        account_number = coordinator.data.get("account_number", "unknown")
        account_address = coordinator.data.get("account_address")
        self._attr_unique_id = f"{account_number}_{description.key}"
        self._attr_entity_registry_enabled_default = (
            description.key not in LEGACY_SENSOR_KEYS
        )
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, account_number)},
            name=f"ANWB Account {account_number}",
            manufacturer="ANWB",
            model="Energie Account",
            configuration_url="https://mijn.anwb.nl/energie",
        )
        if account_address:
            self._attr_device_info["model"] = f"Energie Account ({account_address})"

    async def async_added_to_hass(self) -> None:
        """Run when entity about to be added."""
        await super().async_added_to_hass()

        if self.entity_description.key in PRICE_SENSOR_KEYS:
            # Trigger a state update exactly on the hour (XX:00:00)
            self.async_on_remove(
                async_track_time_change(
                    self.hass, self._handle_hourly_update, minute=0, second=0
                )
            )

    @callback
    def _handle_hourly_update(self, now: datetime) -> None:
        """Update the state of the sensor from cached data."""
        self.async_write_ha_state()

    @property
    def native_value(self) -> float | None:
        """Return the state of the sensor."""
        if self.coordinator.data is None:
            return None

        if self.entity_description.key in ELECTRICITY_PRICE_KEYS:
            current_hour_str = _normalize_api_datetime_key(
                dt_util.utcnow().isoformat()
            )
            prices = self.coordinator.data.get("prices_today", {})
            if current_hour_str in prices:
                return round(prices[current_hour_str] / 100.0, 4)
            return None

        if self.entity_description.key in ELECTRICITY_MARKET_PRICE_KEYS:
            current_hour_str = _normalize_api_datetime_key(
                dt_util.utcnow().isoformat()
            )
            prices = self.coordinator.data.get("market_prices_today", {})
            if current_hour_str in prices:
                return round(prices[current_hour_str] / 100.0, 4)
            return None

        if self.entity_description.key in GAS_PRICE_KEYS:
            current_hour_str = _normalize_api_datetime_key(
                dt_util.utcnow().isoformat()
            )
            prices = self.coordinator.data.get("gas_prices_today", {})
            if current_hour_str in prices:
                return round(prices[current_hour_str] / 100.0, 4)
            return None

        data_key = SENSOR_DATA_KEYS.get(
            self.entity_description.key, self.entity_description.key
        )
        val = self.coordinator.data.get(data_key)
        if val is None and data_key != self.entity_description.key:
            val = self.coordinator.data.get(self.entity_description.key)
        if val is None:
            return None
        return round(val, 2)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return the state attributes."""
        if self.coordinator.data:
            if self.entity_description.key in ELECTRICITY_PRICE_KEYS:
                prices = self.coordinator.data.get("prices_today", {})
                market_prices = self.coordinator.data.get("market_prices_today", {})
                price_records = []
                for k, v in prices.items():
                    record = {"start_time": k, "price": round(v / 100.0, 4)}
                    if k in market_prices:
                        record["market_price"] = round(market_prices[k] / 100.0, 4)
                    price_records.append(record)
                return {"prices": price_records}
            elif self.entity_description.key in ELECTRICITY_MARKET_PRICE_KEYS:
                prices = self.coordinator.data.get("market_prices_today", {})
                return {
                    "prices": [
                        {"start_time": k, "price": round(v / 100.0, 4)}
                        for k, v in prices.items()
                    ]
                }
            elif self.entity_description.key in GAS_PRICE_KEYS:
                prices = self.coordinator.data.get("gas_prices_today", {})
                return {
                    "prices": [
                        {"start_time": k, "price": round(v / 100.0, 4)}
                        for k, v in prices.items()
                    ]
                }
        return None
