"""Sensor platform for ANWB Energie Account."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import CURRENCY_EURO, UnitOfEnergy, UnitOfVolume
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers.update_coordinator import CoordinatorEntity
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
PRICE_SENSOR_KEYS = (
    ELECTRICITY_PRICE_KEYS | ELECTRICITY_MARKET_PRICE_KEYS | GAS_PRICE_KEYS
)

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

OPTIONAL_ESTIMATE_SENSOR_KEYS = {
    "electricity_month_to_date_fixed_cost",
    "electricity_month_to_date_total_cost",
    "gas_month_to_date_fixed_cost",
    "gas_month_to_date_total_cost",
}

GAS_SENSOR_KEYS = {
    "gas_month_to_date",
    "gas_month_to_date_cost",
    "gas_year_to_date",
    "gas_year_to_date_cost",
    "gas_month_to_date_fixed_cost",
    "gas_month_to_date_total_cost",
    "gas_current_price",
    "gas_usage",
    "gas_cost",
    "yearly_gas_usage",
    "fixed_cost_gas",
    "total_cost_gas",
    "current_gas_price",
}

MONTHLY_MONETARY_SENSOR_KEYS = {
    "electricity_import_month_to_date_cost",
    "electricity_export_month_to_date_credit",
    "electricity_month_to_date_fixed_cost",
    "electricity_month_to_date_total_cost",
    "gas_month_to_date_cost",
    "gas_month_to_date_fixed_cost",
    "gas_month_to_date_total_cost",
    "import_cost",
    "export_cost",
    "fixed_cost",
    "total_cost",
    "gas_cost",
    "fixed_cost_gas",
    "total_cost_gas",
}
YEARLY_MONETARY_SENSOR_KEYS = {
    "electricity_import_year_to_date_cost",
    "electricity_export_year_to_date_credit",
    "gas_year_to_date_cost",
}
MONTHLY_PERIOD_SENSOR_KEYS = MONTHLY_MONETARY_SENSOR_KEYS | {
    "electricity_import_month_to_date",
    "electricity_export_month_to_date",
    "gas_month_to_date",
    "import_usage",
    "export_usage",
    "gas_usage",
}
YEARLY_PERIOD_SENSOR_KEYS = {
    "electricity_import_year_to_date",
    "electricity_export_year_to_date",
    "gas_year_to_date",
    "yearly_import_usage",
    "yearly_export_usage",
    "yearly_gas_usage",
} | YEARLY_MONETARY_SENSOR_KEYS
PERIOD_SENSOR_KEYS = MONTHLY_PERIOD_SENSOR_KEYS | YEARLY_PERIOD_SENSOR_KEYS

ELECTRICITY_IMPORT_COST_KEYS = {
    "electricity_import_month_to_date_cost",
    "import_cost",
}
ELECTRICITY_EXPORT_COST_KEYS = {
    "electricity_export_month_to_date_credit",
    "export_cost",
}
ELECTRICITY_IMPORT_YEAR_COST_KEYS = {
    "electricity_import_year_to_date_cost",
}
ELECTRICITY_EXPORT_YEAR_COST_KEYS = {
    "electricity_export_year_to_date_credit",
}
ELECTRICITY_FIXED_COST_KEYS = {
    "electricity_month_to_date_fixed_cost",
    "fixed_cost",
}
ELECTRICITY_TOTAL_COST_KEYS = {
    "electricity_month_to_date_total_cost",
    "total_cost",
}
GAS_USAGE_COST_KEYS = {
    "gas_month_to_date_cost",
    "gas_cost",
}
GAS_YEAR_USAGE_COST_KEYS = {
    "gas_year_to_date_cost",
}
GAS_FIXED_COST_KEYS = {
    "gas_month_to_date_fixed_cost",
    "fixed_cost_gas",
}
GAS_TOTAL_COST_KEYS = {
    "gas_month_to_date_total_cost",
    "total_cost_gas",
}
GAS_MONTH_DATA_SENSOR_KEYS = (
    {
        "gas_month_to_date",
        "gas_usage",
    }
    | GAS_USAGE_COST_KEYS
    | GAS_FIXED_COST_KEYS
    | GAS_TOTAL_COST_KEYS
)
GAS_YEAR_DATA_SENSOR_KEYS = {
    "gas_year_to_date",
    "gas_year_to_date_cost",
    "yearly_gas_usage",
}
COST_ESTIMATE_SENSOR_KEYS = (
    ELECTRICITY_IMPORT_COST_KEYS
    | ELECTRICITY_EXPORT_COST_KEYS
    | ELECTRICITY_IMPORT_YEAR_COST_KEYS
    | ELECTRICITY_EXPORT_YEAR_COST_KEYS
    | ELECTRICITY_FIXED_COST_KEYS
    | ELECTRICITY_TOTAL_COST_KEYS
    | GAS_USAGE_COST_KEYS
    | GAS_YEAR_USAGE_COST_KEYS
    | GAS_FIXED_COST_KEYS
    | GAS_TOTAL_COST_KEYS
)

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
        key="electricity_import_year_to_date_cost",
        translation_key="electricity_import_year_to_date_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
    ),
    SensorEntityDescription(
        key="electricity_export_year_to_date",
        translation_key="electricity_export_year_to_date",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    SensorEntityDescription(
        key="electricity_export_year_to_date_credit",
        translation_key="electricity_export_year_to_date_credit",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
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
        key="gas_year_to_date_cost",
        translation_key="gas_year_to_date_cost",
        device_class=SensorDeviceClass.MONETARY,
        native_unit_of_measurement=CURRENCY_EURO,
        state_class=SensorStateClass.TOTAL,
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
                if description.key in PRICE_SENSOR_KEYS
                else data.consumption
            )
            coordinator_data = coordinator.data or {}
            account_number = coordinator_data.get("account_number", "unknown")
            unique_id = f"{account_number}_{description.key}"
            entity_id = registry.async_get_entity_id("sensor", DOMAIN, unique_id)
            registry_entry = registry.async_get(entity_id) if entity_id else None

            if description.key in LEGACY_SENSOR_KEYS:
                if registry_entry is None:
                    continue
                if registry_entry.disabled_by == er.RegistryEntryDisabler.INTEGRATION:
                    registry.async_remove(registry_entry.entity_id)
                    continue

            if (
                description.key in OPTIONAL_ESTIMATE_SENSOR_KEYS
                and registry_entry is None
            ):
                continue

            if description.key in GAS_SENSOR_KEYS and current_has_gas is False:
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

    @callback
    def _async_discover_gas_entities() -> None:
        """Add gas entities if gas is detected after platform setup."""
        if not bool((data.consumption.data or {}).get("has_gas")):
            return
        if entities := _new_entities():
            async_add_entities(entities)

    entry.async_on_unload(
        data.consumption.async_add_listener(_async_discover_gas_entities)
    )


class ANWBEnergieAccountSensor(CoordinatorEntity[ANWBBaseCoordinator], SensorEntity):
    """Representation of an ANWB Energie Account sensor."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: ANWBBaseCoordinator,
        description: SensorEntityDescription,
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

    @property
    def available(self) -> bool:
        """Return whether the sensor has data for the account's contract."""
        if not super().available:
            return False

        key = self.entity_description.key
        if key in GAS_SENSOR_KEYS:
            data = self._gas_availability_coordinator.data or {}
            if not bool(data.get("has_gas")):
                return False
            if (
                key in GAS_MONTH_DATA_SENSOR_KEYS
                and data.get("gas_month_data_available") is False
            ):
                return False
            if (
                key in GAS_YEAR_DATA_SENSOR_KEYS
                and data.get("gas_year_data_available") is False
            ):
                return False

        coordinator_data = self.coordinator.data or {}
        if key in MONTHLY_PERIOD_SENSOR_KEYS and not _is_current_period(
            coordinator_data.get("monthly_period_start"),
            include_month=True,
        ):
            return False
        if key in YEARLY_PERIOD_SENSOR_KEYS and not _is_current_period(
            coordinator_data.get("yearly_period_start"),
            include_month=False,
        ):
            return False

        return (
            key not in PERIOD_SENSOR_KEYS | COST_ESTIMATE_SENSOR_KEYS
            or self.native_value is not None
        )

    @property
    def last_reset(self) -> datetime | None:
        """Return the period start associated with a monetary total."""
        key = self.entity_description.key
        data = self.coordinator.data or {}
        if key in MONTHLY_MONETARY_SENSOR_KEYS:
            return _parse_period_start(data.get("monthly_period_start"))
        if key in YEARLY_MONETARY_SENSOR_KEYS:
            return _parse_period_start(data.get("yearly_period_start"))
        return None

    async def async_added_to_hass(self) -> None:
        """Run when entity about to be added."""
        await super().async_added_to_hass()

        if (
            self.entity_description.key in GAS_PRICE_KEYS
            and self._gas_availability_coordinator is not self.coordinator
        ):
            self.async_on_remove(
                self._gas_availability_coordinator.async_add_listener(
                    self._handle_gas_availability_update
                )
            )

        if self.entity_description.key in PERIOD_SENSOR_KEYS:
            self.async_on_remove(
                async_track_time_change(
                    self.hass,
                    self._handle_period_boundary,
                    hour=0,
                    minute=0,
                    second=0,
                )
            )

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

        if self.entity_description.key in ELECTRICITY_PRICE_KEYS:
            current_hour_str = _normalize_api_datetime_key(dt_util.utcnow().isoformat())
            prices = self.coordinator.data.get("prices_today", {})
            if current_hour_str in prices:
                return round(prices[current_hour_str] / 100.0, 4)
            return None

        if self.entity_description.key in ELECTRICITY_MARKET_PRICE_KEYS:
            current_hour_str = _normalize_api_datetime_key(dt_util.utcnow().isoformat())
            prices = self.coordinator.data.get("market_prices_today", {})
            if current_hour_str in prices:
                return round(prices[current_hour_str] / 100.0, 4)
            return None

        if self.entity_description.key in GAS_PRICE_KEYS:
            current_hour_str = _normalize_api_datetime_key(dt_util.utcnow().isoformat())
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
            elif self.entity_description.key in COST_ESTIMATE_SENSOR_KEYS:
                key = self.entity_description.key
                attributes: dict[str, Any] = {"estimated": True}

                if key in ELECTRICITY_IMPORT_COST_KEYS:
                    attributes["tariff_coverage"] = self.coordinator.data.get(
                        "electricity_import_tariff_coverage"
                    )
                elif key in ELECTRICITY_EXPORT_COST_KEYS:
                    attributes["tariff_coverage"] = self.coordinator.data.get(
                        "electricity_export_tariff_coverage"
                    )
                elif key in ELECTRICITY_IMPORT_YEAR_COST_KEYS:
                    attributes["tariff_coverage"] = self.coordinator.data.get(
                        "electricity_import_year_to_date_tariff_coverage"
                    )
                    attributes["calculation_method"] = self.coordinator.data.get(
                        "year_to_date_cost_calculation_method"
                    )
                elif key in ELECTRICITY_EXPORT_YEAR_COST_KEYS:
                    attributes["tariff_coverage"] = self.coordinator.data.get(
                        "electricity_export_year_to_date_tariff_coverage"
                    )
                    attributes["calculation_method"] = self.coordinator.data.get(
                        "year_to_date_cost_calculation_method"
                    )
                elif key in ELECTRICITY_FIXED_COST_KEYS:
                    attributes["fixed_cost_source"] = self.coordinator.data.get(
                        "electricity_fixed_cost_source"
                    )
                elif key in ELECTRICITY_TOTAL_COST_KEYS:
                    attributes["tariff_coverage"] = {
                        "import": self.coordinator.data.get(
                            "electricity_import_tariff_coverage"
                        ),
                        "export": self.coordinator.data.get(
                            "electricity_export_tariff_coverage"
                        ),
                    }
                    attributes["fixed_cost_source"] = self.coordinator.data.get(
                        "electricity_fixed_cost_source"
                    )
                elif key in GAS_USAGE_COST_KEYS:
                    attributes["tariff_coverage"] = self.coordinator.data.get(
                        "gas_tariff_coverage"
                    )
                elif key in GAS_YEAR_USAGE_COST_KEYS:
                    attributes["tariff_coverage"] = self.coordinator.data.get(
                        "gas_year_to_date_tariff_coverage"
                    )
                    attributes["calculation_method"] = self.coordinator.data.get(
                        "year_to_date_cost_calculation_method"
                    )
                elif key in GAS_FIXED_COST_KEYS:
                    attributes["fixed_cost_source"] = self.coordinator.data.get(
                        "gas_fixed_cost_source"
                    )
                elif key in GAS_TOTAL_COST_KEYS:
                    attributes["tariff_coverage"] = self.coordinator.data.get(
                        "gas_tariff_coverage"
                    )
                    attributes["fixed_cost_source"] = self.coordinator.data.get(
                        "gas_fixed_cost_source"
                    )

                return attributes
        return None
