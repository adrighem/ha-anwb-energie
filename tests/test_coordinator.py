# ruff: noqa: E402, E501
"""Test the ANWB Energie Account coordinator."""

import importlib
import sys
from types import SimpleNamespace
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


class UpdateFailed(Exception):
    pass


sys.modules["homeassistant.helpers.update_coordinator"] = MagicMock()
sys.modules[
    "homeassistant.helpers.update_coordinator"
].DataUpdateCoordinator = DataUpdateCoordinator
sys.modules["homeassistant.helpers.update_coordinator"].UpdateFailed = UpdateFailed

import datetime  # noqa: E402
import custom_components.anwb_energie_account.coordinator as coord_mod  # noqa: E402

coord_mod = importlib.reload(coord_mod)
ANWBConsumptionCoordinator = coord_mod.ANWBConsumptionCoordinator
ANWBPricingCoordinator = coord_mod.ANWBPricingCoordinator


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
        assert result["electricity_import_month_to_date"] == 1.5
        assert result["electricity_export_month_to_date"] == 0.5
        assert result["gas_month_to_date"] == 2.5

        # 1.5 * (20.0 / 100) = 0.30
        assert result["import_cost"] == 0.30
        assert result["electricity_import_month_to_date_cost"] == 0.30

        # 0.5 * (20.0 / 100) = 0.10
        assert result["export_cost"] == 0.10
        assert result["electricity_export_month_to_date_credit"] == 0.10

        # 2.5 * (120.0 / 100) = 3.0
        assert result["gas_cost"] == 3.0
        assert result["gas_month_to_date_cost"] == 3.0

        assert result["total_cost_gas"] == pytest.approx(3.0 + 3.0)
        assert result["gas_month_to_date_total_cost"] == pytest.approx(3.0 + 3.0)


@pytest.mark.asyncio
async def test_consumption_endpoint_granularities(auth_mock):
    """Test consumption coordinator queries the intended cache granularities."""
    hass = MagicMock()
    config_entry = MagicMock()

    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, config_entry)
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(2026, 4, 20, 12, 0, 0)
    urls = []

    with patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now), patch.object(
        coordinator, "_async_fetch_data", new_callable=AsyncMock
    ) as mock_fetch, patch.object(
        coordinator, "_insert_statistics", new_callable=AsyncMock
    ):

        async def mock_fetch_side_effect(url, token):
            urls.append(url)
            if "tarieven/" in url:
                return {"data": []}
            return {"data": []}

        mock_fetch.side_effect = mock_fetch_side_effect

        await coordinator._async_update_data_internal()

    assert any("electricity/cache" in url and "interval=HOUR" in url for url in urls)
    assert any("production/cache" in url and "interval=HOUR" in url for url in urls)
    assert any("gas/cache" in url and "interval=HOUR" in url for url in urls)

    assert any("electricity/cache" in url and "interval=MONTH" in url for url in urls)
    assert any("production/cache" in url and "interval=MONTH" in url for url in urls)
    assert any("gas/cache" in url and "interval=MONTH" in url for url in urls)

    tariff_urls = [url for url in urls if "tarieven/" in url]
    assert tariff_urls
    assert all("interval=HOUR" in url for url in tariff_urls)


@pytest.mark.asyncio
async def test_pricing_keeps_local_day_rows_with_previous_utc_date(auth_mock):
    """Test local-day tariff rows are not dropped when their UTC date differs."""
    hass = MagicMock()
    config_entry = MagicMock()

    coordinator = ANWBPricingCoordinator(hass, auth_mock, config_entry)
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(2026, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    cest = datetime.timezone(datetime.timedelta(hours=2))

    def as_cest(value):
        return value.astimezone(cest)

    async def mock_fetch_side_effect(url, token):
        if "tarieven/electricity" in url:
            return {
                "data": [
                    {
                        "date": "2026-06-14T22:00:00+00:00",
                        "values": {"allInPrijs": 20.0, "marktprijs": 10.0},
                    },
                    {
                        "date": "2026-06-15T22:00:00+00:00",
                        "values": {"allInPrijs": 30.0, "marktprijs": 20.0},
                    },
                    {
                        "date": "2026-06-16T22:00:00+00:00",
                        "values": {"allInPrijs": 40.0, "marktprijs": 30.0},
                    },
                ]
            }
        if "tarieven/gas" in url:
            return {
                "data": [
                    {
                        "date": "2026-06-14T22:00:00+00:00",
                        "values": {"allInPrijs": 120.0},
                    },
                    {
                        "date": "2026-06-15T22:00:00+00:00",
                        "values": {"allInPrijs": 130.0},
                    },
                    {
                        "date": "2026-06-16T22:00:00+00:00",
                        "values": {"allInPrijs": 140.0},
                    },
                ]
            }
        return {}

    with patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now), patch.object(
        coord_mod.dt_util, "as_local", side_effect=as_cest
    ), patch.object(
        coordinator, "_async_fetch_data", new_callable=AsyncMock
    ) as mock_fetch:
        mock_fetch.side_effect = mock_fetch_side_effect

        result = await coordinator._async_update_data_internal()

    assert result["prices_today"]["2026-06-14T22:00:00.000Z"] == 20.0
    assert result["prices_today"]["2026-06-15T22:00:00.000Z"] == 30.0
    assert "2026-06-16T22:00:00.000Z" not in result["prices_today"]

    assert result["market_prices_today"]["2026-06-14T22:00:00.000Z"] == 10.0
    assert result["market_prices_today"]["2026-06-15T22:00:00.000Z"] == 20.0
    assert "2026-06-16T22:00:00.000Z" not in result["market_prices_today"]

    assert result["gas_prices_today"]["2026-06-14T22:00:00.000Z"] == 120.0
    assert result["gas_prices_today"]["2026-06-15T22:00:00.000Z"] == 130.0
    assert "2026-06-16T22:00:00.000Z" not in result["gas_prices_today"]


@pytest.mark.asyncio
async def test_graphql_auth_errors_refresh_kraken_token(auth_mock):
    """Test GraphQL HTTP 200 auth errors trigger the token refresh path."""
    hass = MagicMock()
    config_entry = MagicMock()

    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, config_entry)

    class MockResponse:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        def raise_for_status(self):
            return None

        async def json(self):
            return {
                "data": {"viewer": None},
                "errors": [
                    {
                        "message": "Unauthorized",
                        "extensions": {"errorType": "AUTHORIZATION"},
                    }
                ],
            }

    auth_mock.websession.post = MagicMock(return_value=MockResponse())

    with pytest.raises(UpdateFailed, match="Kraken token expired"):
        await coordinator._async_get_account_info("expired_token")


@pytest.mark.asyncio
async def test_insert_statistics_resumes_from_latest_existing_stat(auth_mock):
    """Test statistics insertion resumes from the newest existing statistic."""
    hass = MagicMock()
    config_entry = MagicMock()

    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, config_entry)
    coordinator.hass = hass
    coordinator._account_number = "12345"

    statistic_id = "anwb_energie_account:import_usage_12345"
    recorder = MagicMock()
    recorder.async_add_executor_job = AsyncMock(
        return_value={
            statistic_id: [
                {
                    "start": datetime.datetime(
                        2026, 4, 20, 10, tzinfo=datetime.timezone.utc
                    ).timestamp(),
                    "sum": 10.0,
                },
                {
                    "start": datetime.datetime(
                        2026, 4, 20, 12, tzinfo=datetime.timezone.utc
                    ).timestamp(),
                    "sum": 20.0,
                },
            ]
        }
    )

    class StatisticData:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class StatisticMetaData:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    recorded = {}

    def add_statistics(hass, metadata, statistics):
        recorded[metadata.statistic_id] = statistics

    with patch.object(coord_mod, "get_instance", return_value=recorder), patch.object(
        coord_mod, "StatisticData", StatisticData
    ), patch.object(
        coord_mod, "StatisticMetaData", StatisticMetaData
    ), patch.object(
        coord_mod, "StatisticMeanType", SimpleNamespace(NONE="none")
    ), patch.object(
        coord_mod, "async_add_external_statistics", side_effect=add_statistics
    ):
        await coordinator._insert_statistics(
            [
                {"startDate": "2026-04-20T11:00:00.000Z", "usage": 1.0},
                {"startDate": "2026-04-20T12:00:00.000Z", "usage": 1.0},
                {"startDate": "2026-04-20T13:00:00.000Z", "usage": 1.0},
            ],
            [],
            {},
            [],
            {},
        )

    assert len(recorded[statistic_id]) == 1
    assert recorded[statistic_id][0].start == datetime.datetime(
        2026, 4, 20, 13, tzinfo=datetime.timezone.utc
    )
    assert recorded[statistic_id][0].sum == 21.0


@pytest.mark.asyncio
async def test_dns_failure_grace_period(auth_mock):
    """Test coordinator handles DNS failure with 24h grace period."""
    hass = MagicMock()
    config_entry = MagicMock()

    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, config_entry)
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"

    import custom_components.anwb_energie_account.coordinator as coord_mod
    mock_data = {"test": "data"}
    coordinator.data = mock_data
    
    last_success = datetime.datetime(2026, 5, 18, 12, 0, 0, tzinfo=datetime.timezone.utc)
    coordinator.last_successful_update = last_success

    with patch.object(coordinator, "_async_update_data_internal", side_effect=UpdateFailed("DNS failure")):
        # 1. Test within 24 hours (e.g. 1 hour later)
        mock_now = last_success + datetime.timedelta(hours=1)
        with patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now):
            result = await coordinator._async_update_data()
            assert result == mock_data
            assert coordinator.last_successful_update == last_success

        # 2. Test after 24 hours (e.g. 25 hours later)
        mock_now = last_success + datetime.timedelta(hours=25)
        with patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now):
            with pytest.raises(UpdateFailed):
                await coordinator._async_update_data()

        # 3. Test regular failure (not DNS) within 24 hours
        mock_now = last_success + datetime.timedelta(hours=1)
        with patch.object(coordinator, "_async_update_data_internal", side_effect=UpdateFailed("Some other error")):
            with patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now):
                with pytest.raises(UpdateFailed):
                    await coordinator._async_update_data()
