"""Sensor platform for ANWB Energie Account."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .coordinator import ANWBBaseCoordinator, ANWBEnergieAccountConfigEntry
from .sensor_descriptions import SENSOR_TYPES, ANWBSensorEntityDescription

LEGACY_SENSOR_KEYS = frozenset(desc.key for desc in SENSOR_TYPES if desc.legacy)
OPTIONAL_ESTIMATE_SENSOR_KEYS = frozenset(
    desc.key for desc in SENSOR_TYPES if desc.optional and not desc.legacy
)
GAS_SENSOR_KEYS = frozenset(desc.key for desc in SENSOR_TYPES if desc.is_gas)
MONTHLY_MONETARY_SENSOR_KEYS = frozenset(
    desc.key
    for desc in SENSOR_TYPES
    if desc.period == "month" and desc.device_class == SensorDeviceClass.MONETARY
)
YEARLY_MONETARY_SENSOR_KEYS = frozenset(
    desc.key
    for desc in SENSOR_TYPES
    if desc.period == "year" and desc.device_class == SensorDeviceClass.MONETARY
)
COST_ESTIMATE_SENSOR_KEYS = frozenset(
    desc.key for desc in SENSOR_TYPES if desc.estimate
)
SENSOR_DATA_KEYS = {
    desc.key: desc.data_key
    for desc in SENSOR_TYPES
    if desc.data_key is not None and desc.data_key != desc.key
}


def _parse_period_start(value: Any) -> datetime | None:
    """Parse a timezone-aware coordinator period boundary."""
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else None
    if not isinstance(value, str):
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    return parsed if parsed.tzinfo is not None else None


def _is_current_period(value: Any, *, include_month: bool) -> bool:
    """Return whether a coordinator period boundary is current."""
    period_start = _parse_period_start(value)
    if period_start is None:
        return False

    now = dt_util.now()
    if not isinstance(now, datetime):
        now = datetime.now(period_start.tzinfo)

    return now.year == period_start.year and (
        not include_month or now.month == period_start.month
    )


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ANWBEnergieAccountConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up ANWB Energie Account sensors based on a config entry."""
    data = entry.runtime_data
    registry = er.async_get(hass)

    added_keys: set[str] = set()

    def _new_entities() -> list[ANWBEnergieAccountSensor]:
        entities = []
        current_has_gas = (data.consumption.data or {}).get("has_gas")
        for description in SENSOR_TYPES:
            if description.key in added_keys:
                continue

            coordinator = (
                data.pricing
                if description.source == "pricing"
                else data.consumption
            )
            coordinator_data = coordinator.data or {}
            account_number = coordinator_data.get("account_number", "unknown")
            unique_id = f"{account_number}_{description.key}"
            entity_id = registry.async_get_entity_id("sensor", DOMAIN, unique_id)
            registry_entry = registry.async_get(entity_id) if entity_id else None

            if description.legacy:
                if registry_entry is None:
                    continue
                if registry_entry.disabled_by == er.RegistryEntryDisabler.INTEGRATION:
                    registry.async_remove(registry_entry.entity_id)
                    continue

            if description.optional and registry_entry is None:
                continue

            if description.is_gas and current_has_gas is False:
                if registry_entry is None:
                    continue

            entities.append(
                ANWBEnergieAccountSensor(
                    coordinator,
                    description,
                    gas_availability_coordinator=data.consumption,
                )
            )
            added_keys.add(description.key)

        return entities

    async_add_entities(_new_entities())

    pricing_refresh_requested = bool((data.consumption.data or {}).get("has_gas"))

    @callback
    def _async_discover_gas_entities() -> None:
        """Add gas entities if gas is detected after platform setup."""
        nonlocal pricing_refresh_requested
        if not bool((data.consumption.data or {}).get("has_gas")):
            return
        if entities := _new_entities():
            async_add_entities(entities)
        if not pricing_refresh_requested:
            pricing_refresh_requested = True
            if not (data.pricing.data or {}).get("gas_prices_today"):
                hass.async_create_task(data.pricing.async_request_refresh())

    entry.async_on_unload(
        data.consumption.async_add_listener(_async_discover_gas_entities)
    )


class ANWBEnergieAccountSensor(CoordinatorEntity[ANWBBaseCoordinator], SensorEntity):
    """Representation of an ANWB Energie Account sensor."""

    _attr_has_entity_name = True
    entity_description: ANWBSensorEntityDescription

    def __init__(
        self,
        coordinator: ANWBBaseCoordinator,
        description: ANWBSensorEntityDescription,
        *,
        gas_availability_coordinator: ANWBBaseCoordinator | None = None,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self.entity_description = description
        self._gas_availability_coordinator = gas_availability_coordinator or coordinator
        account_number = coordinator.data.get("account_number", "unknown")
        account_address = coordinator.data.get("account_address")
        self._attr_unique_id = f"{account_number}_{description.key}"
        self._attr_entity_registry_enabled_default = not description.legacy
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, account_number)},
            name=f"ANWB Account {account_number}",
            manufacturer="ANWB",
            model="Energie Account",
            configuration_url="https://mijn.anwb.nl/energie",
        )
        if account_address:
            self._attr_device_info["model"] = f"Energie Account ({account_address})"

    @property
    def available(self) -> bool:
        """Return whether the sensor has data for the account's contract."""
        if not super().available:
            return False

        desc = self.entity_description
        if desc.is_gas:
            data = self._gas_availability_coordinator.data or {}
            if not bool(data.get("has_gas")):
                return False
            if desc.period == "month" and data.get("gas_month_data_available") is False:
                return False
            if desc.period == "year" and data.get("gas_year_data_available") is False:
                return False

        coordinator_data = self.coordinator.data or {}
        if desc.period == "month" and not _is_current_period(
            coordinator_data.get("monthly_period_start"),
            include_month=True,
        ):
            return False
        if desc.period == "year" and not _is_current_period(
            coordinator_data.get("yearly_period_start"),
            include_month=False,
        ):
            return False

        if desc.period is not None or desc.estimate:
            return self.native_value is not None

        return True

    @property
    def last_reset(self) -> datetime | None:
        """Return the period start associated with a monetary total."""
        desc = self.entity_description
        if desc.period is None or desc.device_class != SensorDeviceClass.MONETARY:
            return None
        data = self.coordinator.data or {}
        if desc.period == "month":
            return _parse_period_start(data.get("monthly_period_start"))
        if desc.period == "year":
            return _parse_period_start(data.get("yearly_period_start"))
        return None

    async def async_added_to_hass(self) -> None:
        """Run when entity about to be added."""
        await super().async_added_to_hass()

        desc = self.entity_description
        if (
            desc.is_gas
            and desc.source == "pricing"
            and self._gas_availability_coordinator is not self.coordinator
        ):
            self.async_on_remove(
                self._gas_availability_coordinator.async_add_listener(
                    self._handle_gas_availability_update
                )
            )

        if desc.period is not None:
            self.async_on_remove(
                async_track_time_change(
                    self.hass,
                    self._handle_period_boundary,
                    hour=0,
                    minute=0,
                    second=0,
                )
            )

        if desc.source == "pricing":
            self.async_on_remove(
                async_track_time_change(
                    self.hass, self._handle_hourly_update, minute=0, second=0
                )
            )

    @callback
    def _handle_hourly_update(self, now: datetime) -> None:
        """Update the state of the sensor from cached data."""
        self.async_write_ha_state()

    @callback
    def _handle_gas_availability_update(self) -> None:
        """Update a gas tariff sensor when contract availability changes."""
        self.async_write_ha_state()

    @callback
    def _handle_period_boundary(self, now: datetime) -> None:
        """Re-evaluate availability when a local day starts."""
        self.async_write_ha_state()

    @property
    def native_value(self) -> float | None:
        """Return the state of the sensor."""
        if self.coordinator.data is None:
            return None

        desc = self.entity_description
        if desc.value_fn is not None:
            return desc.value_fn(self.coordinator.data)

        data_key = desc.data_key or desc.key
        val = self.coordinator.data.get(data_key)
        if val is None and data_key != desc.key:
            val = self.coordinator.data.get(desc.key)
        if val is None:
            return None
        # Adding 0.0 turns a rounded -0.0 into 0.0.
        return round(val, 2) + 0.0

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return the state attributes."""
        if not self.coordinator.data or self.entity_description.attributes_fn is None:
            return None
        return self.entity_description.attributes_fn(self.coordinator.data)
