"""Test the ANWB Energie Account sensors."""

import sys
from unittest.mock import MagicMock

# Mock homeassistant modules to allow testing without core
sys.modules["homeassistant"] = MagicMock()
sys.modules["homeassistant.core"] = MagicMock()
sys.modules["homeassistant.exceptions"] = MagicMock()
sys.modules["homeassistant.config_entries"] = MagicMock()
sys.modules["homeassistant.components"] = MagicMock()
sys.modules["homeassistant.components.application_credentials"] = MagicMock()
sys.modules["homeassistant.helpers"] = MagicMock()
sys.modules["homeassistant.helpers.event"] = MagicMock()
sys.modules["homeassistant.helpers.aiohttp_client"] = MagicMock()
sys.modules["homeassistant.helpers.config_entry_oauth2_flow"] = MagicMock()
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
sys.modules["homeassistant.components.sensor"].SensorEntityDescription = SensorEntityDescription
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
sys.modules["homeassistant.helpers.update_coordinator"].DataUpdateCoordinator = DataUpdateCoordinator
class CoordinatorEntityMeta(type):
    def __getitem__(cls, val):
        return cls

class CoordinatorEntity(metaclass=CoordinatorEntityMeta):
    def __init__(self, coordinator):
        self.coordinator = coordinator
sys.modules["homeassistant.helpers.update_coordinator"].CoordinatorEntity = CoordinatorEntity

# Now import sensor
from custom_components.anwb_energie_account.sensor import SENSOR_TYPES, ANWBEnergieAccountSensor

def test_sensor_types():
    """Test that all expected sensors are defined."""
    keys = [desc.key for desc in SENSOR_TYPES]
    
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

import datetime

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
        "gas_prices_today": {
            "2026-04-20T00:00:00.000Z": 125.432,
        },
        "gas_usage": 12.345
    }
    
    from unittest.mock import patch
    import custom_components.anwb_energie_account.sensor as sensor_mod
    with patch.object(sensor_mod.dt_util, "utcnow", return_value=mock_now):
        # Test current_price
        desc = next(d for d in SENSOR_TYPES if d.key == "current_price")
        sensor = ANWBEnergieAccountSensor(coordinator, desc)
        assert sensor.native_value == 0.2543

        # Test current_gas_price
        desc = next(d for d in SENSOR_TYPES if d.key == "current_gas_price")
        sensor = ANWBEnergieAccountSensor(coordinator, desc)
        assert sensor.native_value == 1.2543

        # Test normal value
        desc = next(d for d in SENSOR_TYPES if d.key == "gas_usage")
        sensor = ANWBEnergieAccountSensor(coordinator, desc)
        assert sensor.native_value == 12.35

def test_sensor_extra_attributes():
    """Test sensor extra state attributes formatting."""
    coordinator = MagicMock()
    coordinator.data = {
        "prices_today": {
            "2026-04-20T00:00:00.000Z": 25.432,
            "2026-04-20T01:00:00.000Z": 22.112,
        },
        "gas_prices_today": {
            "2026-04-20T00:00:00.000Z": 125.432,
        }
    }

    # Test current_price
    desc = next(d for d in SENSOR_TYPES if d.key == "current_price")
    sensor = ANWBEnergieAccountSensor(coordinator, desc)
    attrs = sensor.extra_state_attributes
    assert attrs is not None
    assert "prices" in attrs
    assert attrs["prices"][0]["price"] == 0.2543

    # Test current_gas_price
    desc = next(d for d in SENSOR_TYPES if d.key == "current_gas_price")
    sensor = ANWBEnergieAccountSensor(coordinator, desc)
    attrs = sensor.extra_state_attributes
    assert attrs is not None
    assert "prices" in attrs
    assert attrs["prices"][0]["price"] == 1.2543

    # Test normal sensor has no extra attributes
    desc = next(d for d in SENSOR_TYPES if d.key == "import_usage")
    sensor = ANWBEnergieAccountSensor(coordinator, desc)
    assert sensor.extra_state_attributes is None
