"""Sensor platform for ANWB Energie Account."""

from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import CURRENCY_EURO, UnitOfEnergy
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import ANWBDataUpdateCoordinator, ANWBEnergieAccountConfigEntry

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
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ANWBEnergieAccountConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up ANWB Energie Account sensors based on a config entry."""
    coordinator = entry.runtime_data

    async_add_entities(
        ANWBEnergieAccountSensor(coordinator, description)
        for description in SENSOR_TYPES
    )


class ANWBEnergieAccountSensor(
    CoordinatorEntity[ANWBDataUpdateCoordinator], SensorEntity
):
    """Representation of an ANWB Energie Account sensor."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: ANWBDataUpdateCoordinator,
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
            # We can use the city name as the suggested area or just pass it to the name.
            # But DeviceInfo only accepts suggested_area.
            # Let's set the full address as configuration_url or something, or simply set it
            # in the model.
            self._attr_device_info["model"] = f"Energie Account ({account_address})"

    @property
    def native_value(self) -> float | None:
        """Return the state of the sensor."""
        if self.coordinator.data is None:
            return None
        val = self.coordinator.data.get(self.entity_description.key)
        if val is None:
            return None
        # Provide more precision for current_price
        if self.entity_description.key == "current_price":
            return round(val, 4)
        return round(val, 2)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return the state attributes."""
        if self.entity_description.key == "current_price" and self.coordinator.data:
            prices = self.coordinator.data.get("prices_today", {})
            return {
                "prices": [
                    {"start_time": k, "price": round(v / 100.0, 4)}
                    for k, v in prices.items()
                ]
            }
        return None
