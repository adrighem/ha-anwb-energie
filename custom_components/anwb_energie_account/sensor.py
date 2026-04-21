"""Sensor platform for ANWB Energie Account."""

from __future__ import annotations

from typing import Any

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
from .coordinator import ANWBBaseCoordinator, ANWBEnergieAccountConfigEntry

SENSOR_TYPES: tuple[SensorEntityDescription, ...] = (
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
        if description.key in ("current_price", "current_gas_price"):
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

        if self.entity_description.key in ("current_price", "current_gas_price"):
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

        if self.entity_description.key == "current_price":
            now = dt_util.utcnow()
            current_hour_str = now.replace(minute=0, second=0, microsecond=0).strftime(
                "%Y-%m-%dT%H:00:00.000Z"
            )
            prices = self.coordinator.data.get("prices_today", {})
            if current_hour_str in prices:
                return round(prices[current_hour_str] / 100.0, 4)
            return None

        if self.entity_description.key == "current_gas_price":
            now = dt_util.utcnow()
            current_hour_str = now.replace(minute=0, second=0, microsecond=0).strftime(
                "%Y-%m-%dT%H:00:00.000Z"
            )
            prices = self.coordinator.data.get("gas_prices_today", {})
            if current_hour_str in prices:
                return round(prices[current_hour_str] / 100.0, 4)
            return None

        val = self.coordinator.data.get(self.entity_description.key)
        if val is None:
            return None
        return round(val, 2)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return the state attributes."""
        if self.coordinator.data:
            if self.entity_description.key == "current_price":
                prices = self.coordinator.data.get("prices_today", {})
                return {
                    "prices": [
                        {"start_time": k, "price": round(v / 100.0, 4)}
                        for k, v in prices.items()
                    ]
                }
            elif self.entity_description.key == "current_gas_price":
                prices = self.coordinator.data.get("gas_prices_today", {})
                return {
                    "prices": [
                        {"start_time": k, "price": round(v / 100.0, 4)}
                        for k, v in prices.items()
                    ]
                }
        return None
