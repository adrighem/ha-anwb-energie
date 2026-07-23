# ruff: noqa: E402, E501
"""Test the ANWB Energie Account sensors."""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Mock homeassistant modules to allow testing without core
sys.modules["homeassistant"] = MagicMock()
sys.modules["homeassistant.core"] = MagicMock()
sys.modules["homeassistant.core"].callback = lambda func: func
sys.modules["homeassistant.exceptions"] = MagicMock()
sys.modules["homeassistant.config_entries"] = MagicMock()
sys.modules["homeassistant.components"] = MagicMock()
sys.modules["homeassistant.components.application_credentials"] = MagicMock()
sys.modules["homeassistant.helpers"] = MagicMock()
sys.modules["homeassistant.helpers.event"] = MagicMock()
sys.modules["homeassistant.helpers.entity_registry"] = MagicMock()
sys.modules["homeassistant.helpers"].entity_registry = sys.modules[
    "homeassistant.helpers.entity_registry"
]
sys.modules["homeassistant.helpers.aiohttp_client"] = MagicMock()
sys.modules["homeassistant.helpers.config_entry_oauth2_flow"] = MagicMock()
sys.modules["homeassistant.helpers.storage"] = MagicMock()
sys.modules["homeassistant.util"] = MagicMock()
sys.modules["homeassistant.components.recorder"] = MagicMock()
sys.modules["homeassistant.components.recorder.models"] = MagicMock()
sys.modules["homeassistant.components.recorder.statistics"] = MagicMock()

sys.modules["homeassistant.components.sensor"] = MagicMock()


class SensorDeviceClass:
    ENERGY = "energy"
    MONETARY = "monetary"
    GAS = "gas"


class SensorStateClass:
    TOTAL_INCREASING = "total_increasing"
    TOTAL = "total"
    MEASUREMENT = "measurement"


class SensorEntityDescription:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.key = kwargs.get("key")


class SensorEntity:
    pass


sys.modules["homeassistant.components.sensor"].SensorDeviceClass = SensorDeviceClass
sys.modules["homeassistant.components.sensor"].SensorStateClass = SensorStateClass
sys.modules[
    "homeassistant.components.sensor"
].SensorEntityDescription = SensorEntityDescription
sys.modules["homeassistant.components.sensor"].SensorEntity = SensorEntity

sys.modules["homeassistant.const"] = MagicMock()
sys.modules["homeassistant.const"].CURRENCY_EURO = "€"


class UnitOfEnergy:
    KILO_WATT_HOUR = "kWh"


sys.modules["homeassistant.const"].UnitOfEnergy = UnitOfEnergy


class UnitOfVolume:
    CUBIC_METERS = "m³"


sys.modules["homeassistant.const"].UnitOfVolume = UnitOfVolume

sys.modules["homeassistant.helpers.device_registry"] = MagicMock()
sys.modules["homeassistant.helpers.entity_platform"] = MagicMock()

sys.modules["homeassistant.helpers.update_coordinator"] = MagicMock()


class DataUpdateCoordinatorMeta(type):
    def __getitem__(cls, val):
        return cls


class DataUpdateCoordinator(metaclass=DataUpdateCoordinatorMeta):
    def __init__(self, *args, **kwargs):
        self.data = None


sys.modules[
    "homeassistant.helpers.update_coordinator"
].DataUpdateCoordinator = DataUpdateCoordinator


class CoordinatorEntityMeta(type):
    def __getitem__(cls, val):
        return cls


class CoordinatorEntity(metaclass=CoordinatorEntityMeta):
    def __init__(self, coordinator):
        self.coordinator = coordinator
        self._remove_callbacks = []

    @property
    def available(self):
        return self.coordinator.last_update_success is not False

    async def async_added_to_hass(self):
        return None

    def async_on_remove(self, callback):
        self._remove_callbacks.append(callback)


sys.modules[
    "homeassistant.helpers.update_coordinator"
].CoordinatorEntity = CoordinatorEntity


class RegistryEntryDisabler:
    INTEGRATION = "integration"
    USER = "user"


sys.modules[
    "homeassistant.helpers.entity_registry"
].RegistryEntryDisabler = RegistryEntryDisabler

# Now import sensor
from custom_components.anwb_energie_account.sensor import (
    GAS_SENSOR_KEYS,
    LEGACY_SENSOR_KEYS,
    MONTHLY_MONETARY_SENSOR_KEYS,
    OPTIONAL_ESTIMATE_SENSOR_KEYS,
    SENSOR_DATA_KEYS,
    SENSOR_TYPES,
    YEARLY_MONETARY_SENSOR_KEYS,
    ANWBEnergieAccountSensor,
    async_setup_entry,
)


class FakeEntityRegistry:
    """Minimal entity registry implementing the Home Assistant APIs used here."""

    def __init__(self, entries=()):
        self.entries = {entry.entity_id: entry for entry in entries}
        self.removed = []

    def async_get_entity_id(self, domain, platform, unique_id):
        return next(
            (
                entry.entity_id
                for entry in self.entries.values()
                if entry.domain == domain
                and entry.platform == platform
                and entry.unique_id == unique_id
            ),
            None,
        )

    def async_get(self, entity_id):
        return self.entries.get(entity_id)

    def async_remove(self, entity_id):
        self.removed.append(entity_id)
        self.entries.pop(entity_id, None)


def _registry_entry(key, *, disabled_by=None):
    return SimpleNamespace(
        disabled_by=disabled_by,
        domain="sensor",
        entity_id=f"sensor.{key}",
        platform="anwb_energie_account",
        unique_id=f"12345_{key}",
    )


async def _setup_entities(*, has_gas, registry_entries=(), consumption_data=None):
    now = datetime.datetime.now().astimezone()
    consumption = MagicMock()
    consumption.data = {
        "account_number": "12345",
        "has_gas": has_gas,
        "monthly_period_start": now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        ).isoformat(),
        "yearly_period_start": now.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        ).isoformat(),
        **(consumption_data or {}),
    }
    consumption.last_update_success = True
    consumption.listeners = []

    def async_add_listener(listener):
        consumption.listeners.append(listener)
        return lambda: None

    consumption.async_add_listener.side_effect = async_add_listener
    pricing = MagicMock()
    pricing.data = {"account_number": "12345"}
    pricing.last_update_success = True

    entry = MagicMock()
    entry.runtime_data = MagicMock(consumption=consumption, pricing=pricing)

    registry = FakeEntityRegistry(registry_entries)
    added_entities = []

    def async_add_entities(entities):
        added_entities.extend(entities)

    import custom_components.anwb_energie_account.sensor as sensor_mod

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(sensor_mod.er, "async_get", lambda hass: registry)
        await async_setup_entry(MagicMock(), entry, async_add_entities)

    return added_entities, registry, consumption, pricing


def test_sensor_types():
    """Test that all expected sensors are defined."""
    keys = [desc.key for desc in SENSOR_TYPES]

    assert "electricity_import_month_to_date" in keys
    assert "electricity_export_month_to_date" in keys
    assert "electricity_import_month_to_date_cost" in keys
    assert "electricity_export_month_to_date_credit" in keys
    assert "electricity_import_year_to_date" in keys
    assert "electricity_import_year_to_date_cost" in keys
    assert "electricity_export_year_to_date" in keys
    assert "electricity_export_year_to_date_credit" in keys
    assert "electricity_month_to_date_fixed_cost" in keys
    assert "electricity_month_to_date_total_cost" in keys
    assert "electricity_current_price" in keys
    assert "electricity_market_price" in keys
    assert "gas_month_to_date" in keys
    assert "gas_month_to_date_cost" in keys
    assert "gas_year_to_date" in keys
    assert "gas_year_to_date_cost" in keys
    assert "gas_month_to_date_fixed_cost" in keys
    assert "gas_month_to_date_total_cost" in keys
    assert "gas_current_price" in keys

    assert "import_usage" in keys
    assert "export_usage" in keys
    assert "import_cost" in keys
    assert "export_cost" in keys
    assert "yearly_import_usage" in keys
    assert "yearly_export_usage" in keys
    assert "fixed_cost" in keys
    assert "total_cost" in keys
    assert "current_price" in keys

    # Gas sensors
    assert "gas_usage" in keys
    assert "gas_cost" in keys
    assert "yearly_gas_usage" in keys
    assert "fixed_cost_gas" in keys
    assert "total_cost_gas" in keys
    assert "current_gas_price" in keys


def test_legacy_sensors_are_default_disabled():
    """Test legacy compatibility entities are disabled by default."""
    coordinator = MagicMock()
    coordinator.data = {"account_number": "12345"}

    legacy_description = next(d for d in SENSOR_TYPES if d.key == "import_usage")
    legacy_sensor = ANWBEnergieAccountSensor(coordinator, legacy_description)
    assert legacy_description.key in LEGACY_SENSOR_KEYS
    assert legacy_sensor._attr_entity_registry_enabled_default is False

    canonical_description = next(
        d for d in SENSOR_TYPES if d.key == "electricity_import_month_to_date"
    )
    canonical_sensor = ANWBEnergieAccountSensor(coordinator, canonical_description)
    assert canonical_description.key not in LEGACY_SENSOR_KEYS
    assert canonical_sensor._attr_entity_registry_enabled_default is True


def test_legacy_sensor_data_keys_point_to_canonical_values():
    """Test legacy entity keys are aliases for the intended canonical data keys."""
    assert SENSOR_DATA_KEYS == {
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


import datetime  # noqa: E402


def test_sensor_native_value():
    """Test sensor value formatting."""
    # Setup mock coordinator
    coordinator = MagicMock()
    mock_now = datetime.datetime(2026, 4, 20, 0, 30, 0, tzinfo=datetime.timezone.utc)

    coordinator.data = {
        "account_number": "12345",
        "prices_today": {
            "2026-04-20T00:00:00.000Z": 25.432,
        },
        "market_prices_today": {
            "2026-04-20T00:00:00.000Z": 12.345,
        },
        "gas_prices_today": {
            "2026-04-20T00:00:00.000Z": 125.432,
        },
        "gas_usage": 12.345,
        "yearly_gas_usage": 123.456,
        "gas_year_to_date": 123.456,
    }

    import custom_components.anwb_energie_account.sensor as sensor_mod

    with patch.object(sensor_mod.dt_util, "utcnow", return_value=mock_now):
        # Test current_price
        desc = next(d for d in SENSOR_TYPES if d.key == "current_price")
        sensor = ANWBEnergieAccountSensor(coordinator, desc)
        assert sensor.native_value == 0.2543

        # Test canonical electricity_current_price
        desc = next(d for d in SENSOR_TYPES if d.key == "electricity_current_price")
        sensor = ANWBEnergieAccountSensor(coordinator, desc)
        assert sensor.native_value == 0.2543

        # Test electricity_market_price
        desc = next(d for d in SENSOR_TYPES if d.key == "electricity_market_price")
        sensor = ANWBEnergieAccountSensor(coordinator, desc)
        assert sensor.native_value == 0.1235

        # Test current_gas_price
        desc = next(d for d in SENSOR_TYPES if d.key == "current_gas_price")
        sensor = ANWBEnergieAccountSensor(coordinator, desc)
        assert sensor.native_value == 1.2543

        # Test canonical gas_current_price
        desc = next(d for d in SENSOR_TYPES if d.key == "gas_current_price")
        sensor = ANWBEnergieAccountSensor(coordinator, desc)
        assert sensor.native_value == 1.2543

        # Test normal value
        desc = next(d for d in SENSOR_TYPES if d.key == "gas_usage")
        sensor = ANWBEnergieAccountSensor(coordinator, desc)
        assert sensor.native_value == 12.35

        # Test canonical value backed by legacy coordinator data.
        desc = next(d for d in SENSOR_TYPES if d.key == "gas_year_to_date")
        sensor = ANWBEnergieAccountSensor(coordinator, desc)
        assert sensor.native_value == 123.46


def test_sensor_extra_attributes():
    """Test sensor extra state attributes formatting."""
    coordinator = MagicMock()
    coordinator.data = {
        "prices_today": {
            "2026-04-20T00:00:00.000Z": 25.432,
            "2026-04-20T01:00:00.000Z": 22.112,
        },
        "market_prices_today": {
            "2026-04-20T00:00:00.000Z": 12.345,
            "2026-04-20T01:00:00.000Z": 10.678,
        },
        "gas_prices_today": {
            "2026-04-20T00:00:00.000Z": 125.432,
        },
    }

    # Test current_price
    desc = next(d for d in SENSOR_TYPES if d.key == "current_price")
    sensor = ANWBEnergieAccountSensor(coordinator, desc)
    attrs = sensor.extra_state_attributes
    assert attrs is not None
    assert "prices" in attrs
    assert attrs["prices"][0]["price"] == 0.2543
    assert attrs["prices"][0]["market_price"] == 0.1235

    # Test canonical electricity_current_price
    desc = next(d for d in SENSOR_TYPES if d.key == "electricity_current_price")
    sensor = ANWBEnergieAccountSensor(coordinator, desc)
    attrs = sensor.extra_state_attributes
    assert attrs is not None
    assert "prices" in attrs
    assert attrs["prices"][0]["price"] == 0.2543
    assert attrs["prices"][0]["market_price"] == 0.1235

    # Test electricity_market_price
    desc = next(d for d in SENSOR_TYPES if d.key == "electricity_market_price")
    sensor = ANWBEnergieAccountSensor(coordinator, desc)
    attrs = sensor.extra_state_attributes
    assert attrs is not None
    assert "prices" in attrs
    assert attrs["prices"][0]["price"] == 0.1235

    # Test current_gas_price
    desc = next(d for d in SENSOR_TYPES if d.key == "current_gas_price")
    sensor = ANWBEnergieAccountSensor(coordinator, desc)
    attrs = sensor.extra_state_attributes
    assert attrs is not None
    assert "prices" in attrs
    assert attrs["prices"][0]["price"] == 1.2543

    # Test canonical gas_current_price
    desc = next(d for d in SENSOR_TYPES if d.key == "gas_current_price")
    sensor = ANWBEnergieAccountSensor(coordinator, desc)
    attrs = sensor.extra_state_attributes
    assert attrs is not None
    assert "prices" in attrs
    assert attrs["prices"][0]["price"] == 1.2543

    # Test normal sensor has no extra attributes
    desc = next(d for d in SENSOR_TYPES if d.key == "import_usage")
    sensor = ANWBEnergieAccountSensor(coordinator, desc)
    assert sensor.extra_state_attributes is None


@pytest.mark.asyncio
async def test_clean_install_exposes_only_distinct_supported_entities():
    """Test a clean install does not create aliases or optional estimates."""
    added_entities, _, _, _ = await _setup_entities(has_gas=True)

    assert {entity.entity_description.key for entity in added_entities} == {
        "electricity_import_month_to_date",
        "electricity_export_month_to_date",
        "electricity_import_month_to_date_cost",
        "electricity_export_month_to_date_credit",
        "electricity_import_year_to_date",
        "electricity_import_year_to_date_cost",
        "electricity_export_year_to_date",
        "electricity_export_year_to_date_credit",
        "electricity_current_price",
        "electricity_market_price",
        "gas_month_to_date",
        "gas_month_to_date_cost",
        "gas_year_to_date",
        "gas_year_to_date_cost",
        "gas_current_price",
    }


@pytest.mark.asyncio
async def test_clean_install_without_gas_does_not_expose_gas_entities():
    """Test gas entities are omitted for an electricity-only account."""
    added_entities, _, _, _ = await _setup_entities(has_gas=False)
    keys = {entity.entity_description.key for entity in added_entities}

    assert keys == {
        "electricity_import_month_to_date",
        "electricity_export_month_to_date",
        "electricity_import_month_to_date_cost",
        "electricity_export_month_to_date_credit",
        "electricity_import_year_to_date",
        "electricity_import_year_to_date_cost",
        "electricity_export_year_to_date",
        "electricity_export_year_to_date_credit",
        "electricity_current_price",
        "electricity_market_price",
    }
    assert keys.isdisjoint(GAS_SENSOR_KEYS)


@pytest.mark.asyncio
async def test_gas_entities_are_added_when_contract_is_detected_later():
    """Test a new gas contract does not require reloading the integration."""
    added_entities, _, consumption, _ = await _setup_entities(has_gas=False)
    assert {entity.entity_description.key for entity in added_entities}.isdisjoint(
        GAS_SENSOR_KEYS
    )

    consumption.data["has_gas"] = True
    consumption.data["gas_month_data_available"] = True
    consumption.data["gas_year_data_available"] = True
    consumption.listeners[0]()

    assert {
        "gas_month_to_date",
        "gas_month_to_date_cost",
        "gas_year_to_date",
        "gas_year_to_date_cost",
        "gas_current_price",
    } <= {entity.entity_description.key for entity in added_entities}

    entity_count = len(added_entities)
    consumption.listeners[0]()
    assert len(added_entities) == entity_count


@pytest.mark.asyncio
async def test_unknown_initial_gas_status_keeps_canonical_entities_discoverable():
    """Test uncertain contract detection does not permanently omit gas entities."""
    added_entities, _, _, _ = await _setup_entities(has_gas=None)
    entities_by_key = {
        entity.entity_description.key: entity for entity in added_entities
    }

    assert {
        "gas_month_to_date",
        "gas_month_to_date_cost",
        "gas_year_to_date",
        "gas_year_to_date_cost",
        "gas_current_price",
    } <= entities_by_key.keys()
    assert entities_by_key["gas_month_to_date"].available is False
    assert entities_by_key["gas_current_price"].available is False


@pytest.mark.asyncio
async def test_existing_legacy_entities_are_preserved():
    """Test enabled and user-disabled legacy registry entries remain available."""
    entries = [
        _registry_entry("import_usage"),
        _registry_entry(
            "current_price",
            disabled_by=RegistryEntryDisabler.USER,
        ),
        _registry_entry("gas_usage"),
    ]
    added_entities, _, _, _ = await _setup_entities(
        has_gas=True,
        registry_entries=entries,
    )
    keys = {entity.entity_description.key for entity in added_entities}

    assert {"import_usage", "current_price", "gas_usage"} <= keys


@pytest.mark.asyncio
async def test_unused_legacy_entities_are_pruned():
    """Test aliases left disabled by the integration are safely removed."""
    entries = [
        _registry_entry(
            "import_usage",
            disabled_by=RegistryEntryDisabler.INTEGRATION,
        ),
        _registry_entry(
            "export_usage",
            disabled_by=RegistryEntryDisabler.USER,
        ),
    ]
    added_entities, registry, _, _ = await _setup_entities(
        has_gas=True,
        registry_entries=entries,
    )
    keys = {entity.entity_description.key for entity in added_entities}

    assert registry.removed == ["sensor.import_usage"]
    assert "import_usage" not in keys
    assert "export_usage" in keys


@pytest.mark.asyncio
async def test_existing_optional_estimates_are_preserved():
    """Test fixed and net total estimates remain for existing installations."""
    entries = [_registry_entry(key) for key in OPTIONAL_ESTIMATE_SENSOR_KEYS]
    added_entities, _, _, _ = await _setup_entities(
        has_gas=False,
        registry_entries=entries,
    )
    entities_by_key = {
        entity.entity_description.key: entity for entity in added_entities
    }

    assert OPTIONAL_ESTIMATE_SENSOR_KEYS <= entities_by_key.keys()
    assert entities_by_key["gas_month_to_date_fixed_cost"].available is False
    assert entities_by_key["gas_month_to_date_total_cost"].available is False


@pytest.mark.asyncio
async def test_existing_gas_entities_become_unavailable_without_gas_contract():
    """Test preserved gas entities report unavailable when the account has no gas."""
    entries = [
        _registry_entry("gas_month_to_date"),
        _registry_entry(
            "current_gas_price",
            disabled_by=RegistryEntryDisabler.USER,
        ),
    ]
    added_entities, _, _, _ = await _setup_entities(
        has_gas=False,
        registry_entries=entries,
    )
    entities_by_key = {
        entity.entity_description.key: entity for entity in added_entities
    }

    assert entities_by_key["gas_month_to_date"].available is False
    assert entities_by_key["current_gas_price"].available is False


@pytest.mark.parametrize(
    ("key", "value_key"),
    [
        (
            "electricity_import_month_to_date_cost",
            "electricity_import_month_to_date_cost",
        ),
        (
            "electricity_export_month_to_date_credit",
            "electricity_export_month_to_date_credit",
        ),
        (
            "electricity_month_to_date_total_cost",
            "electricity_month_to_date_total_cost",
        ),
        ("gas_month_to_date_cost", "gas_month_to_date_cost"),
        ("gas_month_to_date_total_cost", "gas_month_to_date_total_cost"),
    ],
)
def test_incomplete_cost_estimates_are_unavailable(key, value_key):
    """Test an incomplete estimate never looks like a valid zero cost."""
    now = datetime.datetime.now().astimezone()
    coordinator = MagicMock()
    coordinator.last_update_success = True
    coordinator.data = {
        "account_number": "12345",
        "has_gas": True,
        "gas_month_data_available": True,
        "monthly_period_start": now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        ).isoformat(),
        value_key: None,
    }
    description = next(desc for desc in SENSOR_TYPES if desc.key == key)
    sensor = ANWBEnergieAccountSensor(coordinator, description)

    assert sensor.available is False

    coordinator.data[value_key] = 0.0
    assert sensor.available is True


@pytest.mark.parametrize(
    "key",
    [
        "electricity_import_year_to_date_cost",
        "electricity_export_year_to_date_credit",
        "gas_year_to_date_cost",
    ],
)
def test_incomplete_yearly_cost_estimates_are_unavailable(key):
    """Test year-to-date costs require complete current-year data."""
    now = datetime.datetime.now().astimezone()
    coordinator = MagicMock()
    coordinator.last_update_success = True
    coordinator.data = {
        "account_number": "12345",
        "has_gas": True,
        "gas_year_data_available": True,
        "yearly_period_start": now.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        ).isoformat(),
        key: None,
    }
    description = next(desc for desc in SENSOR_TYPES if desc.key == key)
    sensor = ANWBEnergieAccountSensor(coordinator, description)

    assert sensor.available is False

    coordinator.data[key] = 0.0
    assert sensor.available is True


@pytest.mark.parametrize(
    "key",
    [
        "electricity_import_year_to_date",
        "electricity_export_year_to_date",
    ],
)
def test_incomplete_yearly_usage_is_unavailable(key):
    """Test missing electricity YTD usage is unavailable rather than unknown."""
    now = datetime.datetime.now().astimezone()
    coordinator = MagicMock()
    coordinator.last_update_success = True
    coordinator.data = {
        "account_number": "12345",
        "yearly_period_start": now.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        ).isoformat(),
        key: None,
    }
    description = next(desc for desc in SENSOR_TYPES if desc.key == key)
    sensor = ANWBEnergieAccountSensor(coordinator, description)

    assert sensor.available is False

    coordinator.data[key] = 0.0
    assert sensor.available is True


@pytest.mark.parametrize(
    "key",
    [
        "gas_month_to_date",
        "gas_month_to_date_cost",
        "gas_month_to_date_fixed_cost",
        "gas_month_to_date_total_cost",
    ],
)
def test_failed_gas_month_data_makes_affected_entities_unavailable(key):
    """Test a gas endpoint failure cannot publish fabricated month values."""
    coordinator = MagicMock()
    coordinator.last_update_success = True
    coordinator.data = {
        "account_number": "12345",
        "has_gas": True,
        "gas_month_data_available": False,
    }
    description = next(desc for desc in SENSOR_TYPES if desc.key == key)

    assert ANWBEnergieAccountSensor(coordinator, description).available is False


def test_gas_year_data_failure_does_not_hide_current_price():
    """Test yearly usage availability is independent from the gas tariff sensor."""
    consumption = MagicMock()
    consumption.last_update_success = True
    consumption.data = {
        "account_number": "12345",
        "has_gas": True,
        "gas_year_data_available": False,
    }
    pricing = MagicMock()
    pricing.last_update_success = True
    pricing.data = {
        "account_number": "12345",
        "gas_prices_today": {},
    }

    year_description = next(
        desc for desc in SENSOR_TYPES if desc.key == "gas_year_to_date"
    )
    year_cost_description = next(
        desc for desc in SENSOR_TYPES if desc.key == "gas_year_to_date_cost"
    )
    price_description = next(
        desc for desc in SENSOR_TYPES if desc.key == "gas_current_price"
    )

    assert (
        ANWBEnergieAccountSensor(
            consumption,
            year_description,
            gas_availability_coordinator=consumption,
        ).available
        is False
    )
    assert (
        ANWBEnergieAccountSensor(
            consumption,
            year_cost_description,
            gas_availability_coordinator=consumption,
        ).available
        is False
    )
    assert (
        ANWBEnergieAccountSensor(
            pricing,
            price_description,
            gas_availability_coordinator=consumption,
        ).available
        is True
    )


@pytest.mark.asyncio
async def test_setup_entry_routes_price_sensors_to_pricing_coordinator():
    """Test current price entities use the pricing coordinator."""
    added_entities, _, consumption, pricing = await _setup_entities(has_gas=True)

    entities_by_key = {
        entity.entity_description.key: entity for entity in added_entities
    }
    assert entities_by_key["electricity_current_price"].coordinator is pricing
    assert entities_by_key["electricity_market_price"].coordinator is pricing
    assert entities_by_key["gas_current_price"].coordinator is pricing
    assert entities_by_key["electricity_import_year_to_date"].coordinator is consumption
    assert (
        entities_by_key["electricity_import_year_to_date_cost"].coordinator
        is consumption
    )


@pytest.mark.parametrize("key", sorted(MONTHLY_MONETARY_SENSOR_KEYS))
def test_monthly_monetary_sensors_report_month_start_as_last_reset(key):
    """Test monthly totals use the period attached to their coordinator data."""
    coordinator = MagicMock()
    coordinator.data = {
        "account_number": "12345",
        "monthly_period_start": "2026-07-01T00:00:00+02:00",
    }
    description = next(desc for desc in SENSOR_TYPES if desc.key == key)
    sensor = ANWBEnergieAccountSensor(coordinator, description)
    expected_timezone = datetime.timezone(datetime.timedelta(hours=2))

    assert sensor.last_reset == datetime.datetime(
        2026,
        7,
        1,
        tzinfo=expected_timezone,
    )

    assert description.state_class == SensorStateClass.TOTAL


@pytest.mark.parametrize("key", sorted(YEARLY_MONETARY_SENSOR_KEYS))
def test_yearly_monetary_sensors_report_year_start_as_last_reset(key):
    """Test year-to-date costs use the coordinator's year boundary."""
    coordinator = MagicMock()
    coordinator.data = {
        "account_number": "12345",
        "yearly_period_start": "2026-01-01T00:00:00+01:00",
    }
    description = next(desc for desc in SENSOR_TYPES if desc.key == key)
    sensor = ANWBEnergieAccountSensor(coordinator, description)
    expected_timezone = datetime.timezone(datetime.timedelta(hours=1))

    assert sensor.last_reset == datetime.datetime(
        2026,
        1,
        1,
        tzinfo=expected_timezone,
    )
    assert description.state_class == SensorStateClass.TOTAL


def test_monthly_last_reset_does_not_advance_a_cached_prior_month_value():
    """Test a rollover does not relabel cached March costs as April costs."""
    coordinator = MagicMock()
    coordinator.data = {
        "account_number": "12345",
        "monthly_period_start": "2026-03-01T00:00:00+01:00",
        "electricity_import_month_to_date_cost": 42.0,
    }
    description = next(
        desc
        for desc in SENSOR_TYPES
        if desc.key == "electricity_import_month_to_date_cost"
    )

    assert ANWBEnergieAccountSensor(coordinator, description).last_reset == (
        datetime.datetime(
            2026,
            3,
            1,
            tzinfo=datetime.timezone(datetime.timedelta(hours=1)),
        )
    )


def test_stale_period_entities_are_unavailable_until_refresh():
    """Test prior-month and prior-year values are not presented as current."""
    coordinator = MagicMock()
    coordinator.last_update_success = True
    coordinator.data = {
        "account_number": "12345",
        "monthly_period_start": "2026-03-01T00:00:00+01:00",
        "yearly_period_start": "2025-01-01T00:00:00+01:00",
        "electricity_import_month_to_date": 10.0,
        "electricity_import_month_to_date_cost": 2.0,
        "electricity_import_year_to_date": 100.0,
        "electricity_import_year_to_date_cost": 20.0,
    }
    current_time = datetime.datetime(
        2026,
        4,
        1,
        0,
        30,
        tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
    )

    import custom_components.anwb_energie_account.sensor as sensor_mod

    with patch.object(sensor_mod.dt_util, "now", return_value=current_time):
        for key in (
            "electricity_import_month_to_date",
            "electricity_import_month_to_date_cost",
            "electricity_import_year_to_date",
            "electricity_import_year_to_date_cost",
        ):
            description = next(desc for desc in SENSOR_TYPES if desc.key == key)
            assert ANWBEnergieAccountSensor(coordinator, description).available is False


@pytest.mark.asyncio
async def test_period_entities_recheck_availability_at_local_midnight():
    """Test stale period values are hidden without waiting for a refresh."""
    coordinator = MagicMock()
    coordinator.data = {"account_number": "12345"}
    description = next(
        desc for desc in SENSOR_TYPES if desc.key == "electricity_import_month_to_date"
    )
    sensor = ANWBEnergieAccountSensor(coordinator, description)
    sensor.hass = MagicMock()

    import custom_components.anwb_energie_account.sensor as sensor_mod

    remove_callback = MagicMock()
    with patch.object(
        sensor_mod,
        "async_track_time_change",
        return_value=remove_callback,
    ) as track_time:
        await sensor.async_added_to_hass()

    track_time.assert_called_once_with(
        sensor.hass,
        sensor._handle_period_boundary,
        hour=0,
        minute=0,
        second=0,
    )
    assert remove_callback in sensor._remove_callbacks


def test_non_monthly_sensor_has_no_last_reset():
    """Test non-resetting sensors do not expose last_reset."""
    coordinator = MagicMock()
    coordinator.data = {"account_number": "12345"}
    description = next(
        desc for desc in SENSOR_TYPES if desc.key == "electricity_import_year_to_date"
    )

    assert ANWBEnergieAccountSensor(coordinator, description).last_reset is None


@pytest.mark.parametrize(
    ("keys", "expected"),
    [
        (
            {
                "electricity_import_month_to_date_cost",
                "import_cost",
            },
            {
                "estimated": True,
                "tariff_coverage": {"covered": 20, "total": 22},
            },
        ),
        (
            {
                "electricity_export_month_to_date_credit",
                "export_cost",
            },
            {
                "estimated": True,
                "tariff_coverage": {"covered": 8, "total": 10},
            },
        ),
        (
            {"electricity_import_year_to_date_cost"},
            {
                "estimated": True,
                "tariff_coverage": {"covered": 180, "total": 180},
                "calculation_method": ("daily_closed_months_hourly_current_month"),
            },
        ),
        (
            {"electricity_export_year_to_date_credit"},
            {
                "estimated": True,
                "tariff_coverage": {"covered": 120, "total": 120},
                "calculation_method": ("daily_closed_months_hourly_current_month"),
            },
        ),
        (
            {
                "electricity_month_to_date_fixed_cost",
                "fixed_cost",
            },
            {
                "estimated": True,
                "fixed_cost_source": "hardcoded_fallback",
            },
        ),
        (
            {
                "electricity_month_to_date_total_cost",
                "total_cost",
            },
            {
                "estimated": True,
                "tariff_coverage": {
                    "import": {"covered": 20, "total": 22},
                    "export": {"covered": 8, "total": 10},
                },
                "fixed_cost_source": "hardcoded_fallback",
            },
        ),
        (
            {
                "gas_month_to_date_cost",
                "gas_cost",
            },
            {
                "estimated": True,
                "tariff_coverage": {"covered": 4, "total": 5},
            },
        ),
        (
            {"gas_year_to_date_cost"},
            {
                "estimated": True,
                "tariff_coverage": {"covered": 160, "total": 160},
                "calculation_method": ("daily_closed_months_hourly_current_month"),
            },
        ),
        (
            {
                "gas_month_to_date_fixed_cost",
                "fixed_cost_gas",
            },
            {
                "estimated": True,
                "fixed_cost_source": "account_cache",
            },
        ),
        (
            {
                "gas_month_to_date_total_cost",
                "total_cost_gas",
            },
            {
                "estimated": True,
                "tariff_coverage": {"covered": 4, "total": 5},
                "fixed_cost_source": "account_cache",
            },
        ),
    ],
)
def test_cost_sensors_expose_estimate_metadata(keys, expected):
    """Test canonical and legacy cost sensors explain estimate quality."""
    coordinator = MagicMock()
    coordinator.data = {
        "account_number": "12345",
        "electricity_import_tariff_coverage": {
            "covered": 20,
            "total": 22,
        },
        "electricity_export_tariff_coverage": {
            "covered": 8,
            "total": 10,
        },
        "electricity_import_year_to_date_tariff_coverage": {
            "covered": 180,
            "total": 180,
        },
        "electricity_export_year_to_date_tariff_coverage": {
            "covered": 120,
            "total": 120,
        },
        "gas_tariff_coverage": {"covered": 4, "total": 5},
        "gas_year_to_date_tariff_coverage": {
            "covered": 160,
            "total": 160,
        },
        "year_to_date_cost_calculation_method": (
            "daily_closed_months_hourly_current_month"
        ),
        "electricity_fixed_cost_source": "hardcoded_fallback",
        "gas_fixed_cost_source": "account_cache",
    }

    for key in keys:
        description = next(desc for desc in SENSOR_TYPES if desc.key == key)
        sensor = ANWBEnergieAccountSensor(coordinator, description)
        assert sensor.extra_state_attributes == expected
