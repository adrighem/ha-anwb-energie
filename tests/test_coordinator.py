# ruff: noqa: E402, E501
"""Test the ANWB Energie Account coordinator."""

import sys
from unittest.mock import MagicMock, AsyncMock, patch
import pytest

# Mock homeassistant modules to allow testing without core
sys.modules["homeassistant"] = MagicMock()
sys.modules["homeassistant.core"] = MagicMock()
sys.modules["homeassistant.exceptions"] = MagicMock()
sys.modules["homeassistant.config_entries"] = MagicMock()
sys.modules["homeassistant.components"] = MagicMock()
sys.modules["homeassistant.components.application_credentials"] = MagicMock()
sys.modules["homeassistant.helpers"] = MagicMock()
sys.modules["homeassistant.helpers.aiohttp_client"] = MagicMock()
sys.modules["homeassistant.helpers.config_entry_oauth2_flow"] = MagicMock()
sys.modules["homeassistant.util"] = MagicMock()
sys.modules["homeassistant.components.recorder"] = MagicMock()
sys.modules["homeassistant.components.recorder.models"] = MagicMock()
sys.modules["homeassistant.components.recorder.statistics"] = MagicMock()
sys.modules["homeassistant.const"] = MagicMock()
sys.modules["homeassistant.const"].CURRENCY_EURO = "€"


class UnitOfEnergy:
    KILO_WATT_HOUR = "kWh"


sys.modules["homeassistant.const"].UnitOfEnergy = UnitOfEnergy


class UnitOfVolume:
    CUBIC_METERS = "m³"


sys.modules["homeassistant.const"].UnitOfVolume = UnitOfVolume


class ConfigEntryMeta(type):
    def __getitem__(cls, val):
        return cls


class ConfigEntry(metaclass=ConfigEntryMeta):
    pass


sys.modules["homeassistant.config_entries"].ConfigEntry = ConfigEntry


class DataUpdateCoordinatorMeta(type):
    def __getitem__(cls, val):
        return cls


class DataUpdateCoordinator(metaclass=DataUpdateCoordinatorMeta):
    def __init__(self, *args, **kwargs):
        self.data = None


sys.modules["homeassistant.helpers.update_coordinator"] = MagicMock()
sys.modules[
    "homeassistant.helpers.update_coordinator"
].DataUpdateCoordinator = DataUpdateCoordinator

import datetime  # noqa: E402
from custom_components.anwb_energie_account.coordinator import (  # noqa: E402
    ANWBConsumptionCoordinator,
)


@pytest.fixture
def auth_mock():
    auth = MagicMock()
    auth.async_get_access_token = AsyncMock(return_value="mock_access_token")
    auth.websession = MagicMock()
    return auth


@pytest.mark.asyncio
async def test_update_data_with_gas(auth_mock):
    """Test coordinator updating data including gas."""
    hass = MagicMock()
    config_entry = MagicMock()

    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, config_entry)
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    print("DEBUG:", type(coordinator), repr(coordinator))

    # Mock the timestamp calculation
    import custom_components.anwb_energie_account.coordinator as coord_mod

    mock_now = datetime.datetime(2026, 4, 20, 12, 0, 0)

    with patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now), patch.object(
        coordinator, "_async_fetch_data", new_callable=AsyncMock
    ) as mock_fetch, patch.object(
        coordinator, "_insert_statistics", new_callable=AsyncMock
    ):

        async def mock_fetch_side_effect(url, token):
            if "electricity/cache" in url and "interval=HOUR" in url:
                return {
                    "data": [
                        {
                            "startDate": "2026-04-20T12:00:00.000Z",
                            "usage": 1.5,
                            "vasteKosten": {
                                "abonnementsKosten": 2.0,
                                "netbeheerKosten": 1.0,
                            },
                        }
                    ]
                }
            elif "electricity/cache" in url and "interval=MONTH" in url:
                return {
                    "data": [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 100.0}]
                }
            elif "production/cache" in url and "interval=HOUR" in url:
                return {
                    "data": [{"startDate": "2026-04-20T12:00:00.000Z", "usage": 0.5}]
                }
            elif "production/cache" in url and "interval=MONTH" in url:
                return {
                    "data": [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 50.0}]
                }
            elif "gas/cache" in url and "interval=HOUR" in url:
                return {
                    "data": [
                        {
                            "startDate": "2026-04-20T12:00:00.000Z",
                            "usage": 2.5,
                            "vasteKosten": {"abonnementsKosten": 3.0},
                        }
                    ]
                }
            elif "gas/cache" in url and "interval=MONTH" in url:
                return {
                    "data": [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 20.0}]
                }
            elif "tarieven/electricity" in url:
                return {
                    "data": [
                        {
                            "date": "2026-04-20T12:00:00.000Z",
                            "values": {"allInPrijs": 20.0},
                        }
                    ]
                }
            elif "tarieven/gas" in url:
                return {
                    "data": [
                        {
                            "date": "2026-04-20T12:00:00.000Z",
                            "values": {"allInPrijs": 120.0},
                        }
                    ]
                }
            return {}

        mock_fetch.side_effect = mock_fetch_side_effect

        result = await coordinator._async_update_data_internal()

        assert result["import_usage"] == 1.5
        assert result["export_usage"] == 0.5
        assert result["gas_usage"] == 2.5

        # 1.5 * (20.0 / 100) = 0.30
        assert result["import_cost"] == 0.30

        # 0.5 * (20.0 / 100) = 0.10
        assert result["export_cost"] == 0.10

        # 2.5 * (120.0 / 100) = 3.0
        assert result["gas_cost"] == 3.0

        assert result["total_cost_gas"] == pytest.approx(3.0 + 3.0)
