# ruff: noqa: E402, E501
"""Test the ANWB Energie Account coordinator."""

import importlib
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch
from zoneinfo import ZoneInfo
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
sys.modules["homeassistant.helpers.storage"] = MagicMock()
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


class ConfigEntryAuthFailed(Exception):
    pass


sys.modules["homeassistant.exceptions"].ConfigEntryAuthFailed = ConfigEntryAuthFailed


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
import custom_components.anwb_energie_account as integration_mod  # noqa: E402
import custom_components.anwb_energie_account.coordinator as coord_mod  # noqa: E402

coord_mod = importlib.reload(coord_mod)
coord_mod.dt_util.DEFAULT_TIME_ZONE = ZoneInfo("Europe/Amsterdam")
ANWBConsumptionCoordinator = coord_mod.ANWBConsumptionCoordinator
ANWBPricingCoordinator = coord_mod.ANWBPricingCoordinator


@pytest.fixture
def auth_mock():
    auth = MagicMock()
    auth.async_get_access_token = AsyncMock(return_value="mock_access_token")
    auth.websession = MagicMock()
    return auth


def _complete_amsterdam_hourly_tariffs(
    local_day,
    *,
    all_in_price,
    market_price=None,
):
    """Return all real UTC tariff rows for one Amsterdam calendar day."""
    amsterdam = ZoneInfo("Europe/Amsterdam")
    current = datetime.datetime.combine(
        local_day,
        datetime.time.min,
        tzinfo=amsterdam,
    ).astimezone(datetime.timezone.utc)
    end = datetime.datetime.combine(
        local_day + datetime.timedelta(days=1),
        datetime.time.min,
        tzinfo=amsterdam,
    ).astimezone(datetime.timezone.utc)
    rows = []
    while current < end:
        values = {"allInPrijs": all_in_price}
        if market_price is not None:
            values["marktprijs"] = market_price
        rows.append(
            {
                "date": current.strftime("%Y-%m-%dT%H:00:00.000Z"),
                "values": values,
            }
        )
        current += datetime.timedelta(hours=1)
    return rows


@pytest.mark.parametrize(
    (
        "now",
        "expected_month_start",
        "expected_month_end",
        "expected_year_start",
    ),
    [
        (
            datetime.datetime(2026, 7, 24, 12, tzinfo=ZoneInfo("Europe/Amsterdam")),
            "2026-06-30T22:00:00.000Z",
            "2026-07-31T21:59:59.999Z",
            "2025-12-31T23:00:00.000Z",
        ),
        (
            datetime.datetime(2026, 1, 24, 12, tzinfo=ZoneInfo("Europe/Amsterdam")),
            "2025-12-31T23:00:00.000Z",
            "2026-01-31T22:59:59.999Z",
            "2025-12-31T23:00:00.000Z",
        ),
        (
            datetime.datetime(2026, 3, 24, 12, tzinfo=ZoneInfo("Europe/Amsterdam")),
            "2026-02-28T23:00:00.000Z",
            "2026-03-31T21:59:59.999Z",
            "2025-12-31T23:00:00.000Z",
        ),
        (
            datetime.datetime(
                2026,
                10,
                24,
                12,
                tzinfo=ZoneInfo("Europe/Amsterdam"),
            ),
            "2026-09-30T22:00:00.000Z",
            "2026-10-31T22:59:59.999Z",
            "2025-12-31T23:00:00.000Z",
        ),
        (
            datetime.datetime(2026, 12, 24, 12, tzinfo=datetime.timezone.utc),
            "2026-12-01T00:00:00.000Z",
            "2026-12-31T23:59:59.999Z",
            "2026-01-01T00:00:00.000Z",
        ),
    ],
)
def test_account_cache_query_boundaries_follow_local_calendar_periods(
    now,
    expected_month_start,
    expected_month_end,
    expected_year_start,
):
    """Test account-cache timestamps preserve local month and year boundaries."""
    assert coord_mod._account_cache_query_boundaries(now) == (
        expected_month_start,
        expected_month_end,
        expected_year_start,
    )


@pytest.mark.asyncio
async def test_tariff_cache_is_shared_in_home_assistant_runtime():
    """Test config entries reuse one initialized persistent tariff cache."""

    class FakeStore:
        def __init__(self):
            self.load_calls = 0

        async def async_load(self):
            self.load_calls += 1
            return None

        def async_delay_save(self, data_func, delay):
            raise AssertionError("an empty cache should not be saved")

    hass = SimpleNamespace(
        config=SimpleNamespace(time_zone="Europe/Amsterdam"),
        data={},
    )
    store = FakeStore()

    with patch.object(integration_mod, "Store", return_value=store) as store_class:
        first = await integration_mod._async_get_tariff_cache(hass)
        second = await integration_mod._async_get_tariff_cache(hass)
        hass.config.time_zone = "UTC"
        third = await integration_mod._async_get_tariff_cache(hass)

    assert first is second
    assert third is not first
    assert third.timezone_name == "UTC"
    assert store.load_calls == 2
    assert store_class.call_count == 2
    store_class.assert_called_with(hass, 1, "anwb_energie_account.tariff_cache")


def test_tariff_coverage_bounds_missing_interval_sample():
    """Test tariff diagnostics remain small even with many missing prices."""
    data = [
        {
            "startDate": f"2026-04-20T{hour:02d}:00:00.000Z",
            "usage": 1.0,
        }
        for hour in range(6)
    ]

    usage, cost, coverage = coord_mod._usage_and_variable_cost(data, {})

    assert usage == 6.0
    assert cost is None
    assert coverage["complete"] is False
    assert coverage["required_intervals"] == 6
    assert coverage["matched_intervals"] == 0
    assert coverage["missing_intervals_count"] == 6
    assert len(coverage["missing_intervals"]) == 5


def test_current_month_cost_rejects_truncated_hourly_usage():
    """Test a smaller HOUR total cannot look complete beside its MONTH row."""
    coverage = {
        "complete": True,
        "required_intervals": 1,
        "matched_intervals": 1,
        "missing_intervals_count": 0,
        "missing_intervals": [],
    }

    cost, reconciled = coord_mod._reconcile_current_month_variable_cost(
        1.0,
        0.2,
        coverage,
        [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 10.0}],
        datetime.date(2026, 4, 1),
    )

    assert cost is None
    assert reconciled["complete"] is False
    assert reconciled["reason"] == "current_month_usage_incomplete"


def test_year_to_date_cost_rejects_truncated_daily_usage():
    """Test incomplete DAY data cannot produce a closed-month YTD cost."""
    current_month_coverage = {
        "complete": True,
        "required_intervals": 1,
        "matched_intervals": 1,
        "missing_intervals_count": 0,
        "missing_intervals": [],
    }

    cost, coverage = coord_mod._year_to_date_variable_cost(
        [
            {"startDate": "2026-01-01T00:00:00.000Z", "usage": 10.0},
            {"startDate": "2026-04-01T00:00:00.000Z", "usage": 2.0},
        ],
        [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 9.99}],
        0.4,
        current_month_coverage,
        {"2026-01-01": 10.0},
        datetime.date(2026, 4, 1),
        authoritative_fetch_succeeded=True,
        daily_fetch_succeeded=True,
        daily_tariff_succeeded=True,
    )

    assert cost is None
    assert coverage["complete"] is False
    assert coverage["reason"] == "daily_usage_incomplete"


def test_year_to_date_cost_tolerates_rounding_and_missing_zero_days():
    """Test rounded DAY totals need not contain authoritative zero-usage rows."""
    current_month_coverage = {
        "complete": True,
        "required_intervals": 0,
        "matched_intervals": 0,
        "missing_intervals_count": 0,
        "missing_intervals": [],
    }

    cost, coverage = coord_mod._year_to_date_variable_cost(
        [
            {"startDate": "2026-01-01T00:00:00.000Z", "usage": 10.0},
            {"startDate": "2026-02-01T00:00:00.000Z", "usage": 0.0},
        ],
        [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 9.9995}],
        0.0,
        current_month_coverage,
        {"2026-01-01": 10.0},
        datetime.date(2026, 4, 1),
        authoritative_fetch_succeeded=True,
        daily_fetch_succeeded=True,
        daily_tariff_succeeded=True,
    )

    assert cost == pytest.approx(0.99995)
    assert coverage["complete"] is True


def test_year_to_date_cost_allows_empty_daily_data_for_zero_closed_usage():
    """Test an omitted zero-usage closed period needs no daily tariff."""
    current_month_coverage = {
        "complete": True,
        "required_intervals": 1,
        "matched_intervals": 1,
        "missing_intervals_count": 0,
        "missing_intervals": [],
    }

    cost, coverage = coord_mod._year_to_date_variable_cost(
        [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 0.0}],
        [],
        0.4,
        current_month_coverage,
        {},
        datetime.date(2026, 4, 1),
        authoritative_fetch_succeeded=True,
        daily_fetch_succeeded=True,
        daily_tariff_succeeded=False,
    )

    assert cost == pytest.approx(0.4)
    assert coverage == current_month_coverage


def test_daily_cost_uses_amsterdam_local_dates_across_dst():
    """Test UTC tariff dates align with local usage and month boundaries."""
    amsterdam = ZoneInfo("Europe/Amsterdam")
    usage_data = [
        {
            "startDate": "2026-01-01T00:00:00+01:00",
            "usage": 2.0,
        },
        {
            "startDate": "2026-04-01T00:00:00+02:00",
            "usage": 3.0,
        },
    ]
    tariff_data = [
        {
            "date": "2025-12-31T23:00:00Z",
            "values": {"allInPrijs": 20.0},
        }
    ]

    with patch.object(
        coord_mod.dt_util,
        "as_local",
        side_effect=lambda value: value.astimezone(amsterdam),
    ):
        price_map = coord_mod._daily_tariff_map(tariff_data)
        closed_data = coord_mod._closed_prior_month_data(
            usage_data,
            datetime.date(2026, 4, 1),
        )
        usage, cost, coverage = coord_mod._daily_usage_and_variable_cost(
            closed_data,
            price_map,
        )

    assert price_map == {"2026-01-01": 20.0}
    assert closed_data == usage_data[:1]
    assert usage == 2.0
    assert cost == pytest.approx(0.4)
    assert coverage["complete"] is True


def test_tariff_ranges_use_anwb_local_day_labels_across_dst():
    """Test ANWB day labels are not converted to literal UTC boundaries."""
    amsterdam = ZoneInfo("Europe/Amsterdam")

    with patch.object(coord_mod.dt_util, "DEFAULT_TIME_ZONE", amsterdam):
        spring = coord_mod._local_day_tariff_range(datetime.date(2026, 3, 29))
        autumn = coord_mod._local_day_tariff_range(datetime.date(2026, 10, 25))
        spring_labels = coord_mod._provider_tariff_dates_for_local_day(
            datetime.date(2026, 3, 29)
        )
    with patch.object(coord_mod.dt_util, "DEFAULT_TIME_ZONE", datetime.timezone.utc):
        utc_labels = coord_mod._provider_tariff_dates_for_local_day(
            datetime.date(2026, 7, 23)
        )

    assert spring == (
        "2026-03-29T00:00:00.000Z",
        "2026-03-29T23:59:59.999Z",
    )
    assert autumn == (
        "2026-10-25T00:00:00.000Z",
        "2026-10-25T23:59:59.999Z",
    )
    assert spring_labels == (datetime.date(2026, 3, 29),)
    assert utc_labels == (
        datetime.date(2026, 7, 23),
        datetime.date(2026, 7, 24),
    )


@pytest.mark.asyncio
async def test_non_amsterdam_local_day_combines_adjacent_provider_days(auth_mock):
    """Test another HA timezone is filtered from both overlapping ANWB days."""
    local_day = datetime.date(2026, 7, 23)
    cache = coord_mod.TariffCache(
        None,
        "UTC",
        clock=lambda: datetime.datetime(
            2026,
            7,
            23,
            12,
            tzinfo=datetime.timezone.utc,
        ),
    )
    coordinator = ANWBConsumptionCoordinator(
        MagicMock(),
        auth_mock,
        MagicMock(),
        tariff_cache=cache,
    )
    coordinator._kraken_token = "mock_kraken_token"
    urls = []

    async def mock_fetch_side_effect(url, token):
        urls.append(url)
        provider_day = (
            datetime.date(2026, 7, 24)
            if "startDate=2026-07-24" in url
            else datetime.date(2026, 7, 23)
        )
        return {
            "data": _complete_amsterdam_hourly_tariffs(
                provider_day,
                all_in_price=20.0,
            )
        }

    with (
        patch.object(coord_mod.dt_util, "DEFAULT_TIME_ZONE", datetime.timezone.utc),
        patch.object(
            coordinator,
            "_async_fetch_data",
            new_callable=AsyncMock,
        ) as mock_fetch,
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await cache.async_get_hourly_day(
            "electricity",
            local_day,
            lambda: coordinator._async_fetch_hourly_tariffs(
                "electricity",
                local_day,
            ),
        )

    assert len(result.all_in_prices) == 24
    assert min(result.all_in_prices) == "2026-07-23T00:00:00.000Z"
    assert max(result.all_in_prices) == "2026-07-23T23:00:00.000Z"
    assert len(urls) == 2
    assert all(call.args[1] is None for call in mock_fetch.await_args_list)


@pytest.mark.asyncio
async def test_non_amsterdam_daily_tariff_range_includes_trailing_provider_day(
    auth_mock,
):
    """Test DAY lookup includes the provider label that maps into a UTC day."""
    coordinator = ANWBConsumptionCoordinator(MagicMock(), auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    urls = []

    async def mock_fetch_side_effect(url, token):
        urls.append(url)
        return {
            "data": [
                {
                    "date": "2026-07-20T22:00:00.000Z",
                    "values": {"allInPrijs": 20.0},
                }
            ]
        }

    with (
        patch.object(coord_mod.dt_util, "DEFAULT_TIME_ZONE", datetime.timezone.utc),
        patch.object(
            coord_mod.dt_util,
            "as_local",
            side_effect=lambda value: value.astimezone(datetime.timezone.utc),
        ),
        patch.object(
            coordinator,
            "_async_fetch_data",
            new_callable=AsyncMock,
        ) as mock_fetch,
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_fetch_daily_tariffs(
            "electricity",
            frozenset({datetime.date(2026, 7, 20)}),
        )

    assert result == {"2026-07-20": 20.0}
    assert "startDate=2026-07-19T00:00:00.000Z" in urls[0]
    assert "endDate=2026-07-21T23:59:59.999Z" in urls[0]
    assert mock_fetch.await_args.args[1] is None


@pytest.mark.asyncio
async def test_update_data_with_gas(auth_mock):
    """Test coordinator updating data including gas."""
    hass = MagicMock()
    config_entry = MagicMock()

    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, config_entry)
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    # Mock the timestamp calculation
    import custom_components.anwb_energie_account.coordinator as coord_mod

    mock_now = datetime.datetime(2026, 4, 20, 12, 0, 0)

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
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
                    "data": [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 1.5}]
                }
            elif "electricity/cache" in url and "interval=DAY" in url:
                return {
                    "data": [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 100.0}]
                }
            elif "production/cache" in url and "interval=HOUR" in url:
                return {
                    "data": [{"startDate": "2026-04-20T12:00:00.000Z", "usage": 0.5}]
                }
            elif "production/cache" in url and "interval=MONTH" in url:
                return {
                    "data": [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 0.5}]
                }
            elif "production/cache" in url and "interval=DAY" in url:
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
                    "data": [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 2.5}]
                }
            elif "gas/cache" in url and "interval=DAY" in url:
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
        assert result["has_gas"] is True
        assert result["gas_month_data_available"] is True
        assert result["gas_year_data_available"] is True
        assert result["electricity_import_year_to_date"] == 1.5
        assert result["electricity_export_year_to_date"] == 0.5
        assert result["gas_year_to_date"] == 2.5
        assert result["electricity_fixed_cost_source"] == "account_cache"
        assert result["gas_fixed_cost_source"] == "account_cache"
        assert result["monthly_period_start"] == "2026-04-01T00:00:00+00:00"
        assert result["yearly_period_start"] == "2026-01-01T00:00:00+00:00"


@pytest.mark.asyncio
async def test_update_data_includes_first_amsterdam_month_hours(auth_mock):
    """Test summer month boundaries include the first two local usage hours."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(
        2026,
        7,
        24,
        10,
        tzinfo=datetime.timezone.utc,
    )
    import_rows = [
        {"startDate": "2026-06-30T22:00:00.000Z", "usage": 0.1},
        {"startDate": "2026-06-30T23:00:00.000Z", "usage": 0.2},
    ]
    export_rows = [
        {"startDate": "2026-06-30T22:00:00.000Z", "usage": 0.05},
        {"startDate": "2026-06-30T23:00:00.000Z", "usage": 0.1},
    ]
    urls = []

    async def mock_fetch_side_effect(url, token):
        urls.append(url)
        if "electricity/cache" in url and "interval=HOUR" in url:
            return {"data": import_rows}
        if "production/cache" in url and "interval=HOUR" in url:
            return {"data": export_rows}
        if "electricity/cache" in url and "interval=MONTH" in url:
            return {
                "data": [
                    {"startDate": "2026-07-01T00:00:00.000Z", "usage": 0.3}
                ]
            }
        if "production/cache" in url and "interval=MONTH" in url:
            return {
                "data": [
                    {"startDate": "2026-07-01T00:00:00.000Z", "usage": 0.15}
                ]
            }
        if "tarieven/electricity" in url and "interval=HOUR" in url:
            return {
                "data": _complete_amsterdam_hourly_tariffs(
                    datetime.date(2026, 7, 1),
                    all_in_price=20.0,
                )
            }
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(
            coord_mod.dt_util,
            "as_local",
            side_effect=lambda value: value.astimezone(
                ZoneInfo("Europe/Amsterdam")
            ),
        ),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(
            coordinator, "_insert_statistics", new_callable=AsyncMock
        ) as insert_statistics,
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    hourly_cache_urls = [
        url for url in urls if "/accounts/" in url and "interval=HOUR" in url
    ]
    yearly_cache_urls = [
        url
        for url in urls
        if "/accounts/" in url
        and ("interval=DAY" in url or "interval=MONTH" in url)
    ]
    assert hourly_cache_urls
    assert all(
        "startDate=2026-06-30T22:00:00.000Z" in url
        and "endDate=2026-07-31T21:59:59.999Z" in url
        for url in hourly_cache_urls
    )
    assert yearly_cache_urls
    assert all(
        "startDate=2025-12-31T23:00:00.000Z" in url
        and "endDate=2026-07-31T21:59:59.999Z" in url
        for url in yearly_cache_urls
    )
    assert all(
        "contractStartDate=2025-12-31T23:00:00.000Z" in url
        for url in hourly_cache_urls + yearly_cache_urls
    )

    assert result["electricity_import_month_to_date"] == pytest.approx(0.3)
    assert result["electricity_import_month_to_date_cost"] == pytest.approx(0.06)
    assert result["electricity_export_month_to_date"] == pytest.approx(0.15)
    assert result["electricity_export_month_to_date_credit"] == pytest.approx(0.03)
    assert result["electricity_import_tariff_coverage"]["complete"] is True
    assert result["electricity_export_tariff_coverage"]["complete"] is True
    assert insert_statistics.await_args.args[0] == import_rows
    assert insert_statistics.await_args.args[1] == export_rows


@pytest.mark.asyncio
async def test_consumption_endpoint_granularities(auth_mock):
    """Test zero usage avoids unnecessary tariff requests."""
    hass = MagicMock()
    config_entry = MagicMock()

    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, config_entry)
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(2026, 4, 20, 12, 0, 0)
    urls = []

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):

        async def mock_fetch_side_effect(url, token):
            urls.append(url)
            if "tarieven/" in url:
                return {"data": []}
            if (
                "electricity/cache" in url or "production/cache" in url
            ) and "interval=DAY" in url:
                return {
                    "data": [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 0.0}]
                }
            return {"data": []}

        mock_fetch.side_effect = mock_fetch_side_effect

        await coordinator._async_update_data_internal()

    assert any("electricity/cache" in url and "interval=HOUR" in url for url in urls)
    assert any("production/cache" in url and "interval=HOUR" in url for url in urls)
    assert any("gas/cache" in url and "interval=HOUR" in url for url in urls)

    assert any("electricity/cache" in url and "interval=DAY" in url for url in urls)
    assert any("production/cache" in url and "interval=DAY" in url for url in urls)
    assert not any("gas/cache" in url and "interval=DAY" in url for url in urls)
    assert any("electricity/cache" in url and "interval=MONTH" in url for url in urls)
    assert any("production/cache" in url and "interval=MONTH" in url for url in urls)
    assert any("gas/cache" in url and "interval=MONTH" in url for url in urls)

    tariff_urls = [url for url in urls if "tarieven/" in url]
    assert tariff_urls == []


@pytest.mark.asyncio
async def test_shared_cache_reuses_monthly_and_yearly_tariffs_across_reload(auth_mock):
    """Test a new coordinator reuses closed HOUR and DAY tariff data."""
    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)
    tariff_cache = coord_mod.TariffCache(
        None,
        "Europe/Amsterdam",
        clock=lambda: mock_now,
    )
    urls = []

    async def mock_fetch_side_effect(url, token):
        urls.append(url)
        if "electricity/cache" in url and "interval=HOUR" in url:
            return {
                "data": [
                    {"startDate": "2026-04-19T10:00:00.000Z", "usage": 1.0}
                ]
            }
        if "production/cache" in url and "interval=HOUR" in url:
            return {"data": []}
        if "electricity/cache" in url and "interval=MONTH" in url:
            return {
                "data": [
                    {"startDate": "2026-01-01T00:00:00.000Z", "usage": 10.0},
                    {"startDate": "2026-04-01T00:00:00.000Z", "usage": 1.0},
                ]
            }
        if "production/cache" in url and "interval=MONTH" in url:
            return {"data": []}
        if "electricity/cache" in url and "interval=DAY" in url:
            return {
                "data": [
                    {"startDate": "2026-01-01T00:00:00.000Z", "usage": 10.0}
                ]
            }
        if "production/cache" in url and "interval=DAY" in url:
            return {"data": []}
        if "gas/cache" in url:
            return {"data": []}
        if "tarieven/electricity" in url and "interval=DAY" in url:
            return {
                "data": [
                    {
                        "date": "2026-01-01T00:00:00.000Z",
                        "values": {"allInPrijs": 10.0},
                    }
                ]
            }
        if "tarieven/electricity" in url:
            return {
                "data": _complete_amsterdam_hourly_tariffs(
                    datetime.date(2026, 4, 19),
                    all_in_price=20.0,
                    market_price=10.0,
                )
            }
        return {"data": []}

    async def run_coordinator():
        coordinator = ANWBConsumptionCoordinator(
            MagicMock(),
            auth_mock,
            MagicMock(),
            tariff_cache=tariff_cache,
        )
        coordinator._kraken_token = "mock_kraken_token"
        coordinator._account_number = "12345"
        coordinator._account_address = "Mock Address"
        with (
            patch.object(
                coordinator,
                "_async_fetch_data",
                new_callable=AsyncMock,
            ) as mock_fetch,
            patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
        ):
            mock_fetch.side_effect = mock_fetch_side_effect
            return await coordinator._async_update_data_internal()

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
    ):
        first_result = await run_coordinator()
        first_tariff_call_count = sum("tarieven/" in url for url in urls)
        second_result = await run_coordinator()

    assert first_result["electricity_import_month_to_date_cost"] == pytest.approx(0.2)
    assert first_result["electricity_import_year_to_date_cost"] == pytest.approx(1.2)
    assert second_result["electricity_import_month_to_date_cost"] == pytest.approx(0.2)
    assert second_result["electricity_import_year_to_date_cost"] == pytest.approx(1.2)
    assert first_tariff_call_count == 2
    assert sum("tarieven/" in url for url in urls) == first_tariff_call_count


@pytest.mark.asyncio
async def test_pricing_reuses_current_day_tariffs_fetched_for_monthly_cost(auth_mock):
    """Test pricing and monthly calculations share current-day HOUR data."""
    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)
    tariff_cache = coord_mod.TariffCache(
        None,
        "Europe/Amsterdam",
        clock=lambda: mock_now,
    )
    urls = []

    async def mock_fetch_side_effect(url, token):
        urls.append(url)
        if "electricity/cache" in url and "interval=HOUR" in url:
            return {
                "data": [
                    {"startDate": "2026-04-20T10:00:00.000Z", "usage": 1.0}
                ]
            }
        if "production/cache" in url or "gas/cache" in url:
            return {"data": []}
        if "electricity/cache" in url:
            return {"data": []}
        if "tarieven/" in url and "interval=HOUR" in url:
            local_day = (
                datetime.date(2026, 4, 21)
                if "startDate=2026-04-21" in url
                else datetime.date(2026, 4, 20)
            )
            return {
                "data": _complete_amsterdam_hourly_tariffs(
                    local_day,
                    all_in_price=20.0,
                    market_price=10.0,
                )
            }
        return {"data": []}

    consumption = ANWBConsumptionCoordinator(
        MagicMock(),
        auth_mock,
        MagicMock(),
        tariff_cache=tariff_cache,
    )
    pricing = ANWBPricingCoordinator(
        MagicMock(),
        auth_mock,
        MagicMock(),
        tariff_cache=tariff_cache,
    )
    for coordinator in (consumption, pricing):
        coordinator._kraken_token = "mock_kraken_token"
        coordinator._account_number = "12345"
        coordinator._account_address = "Mock Address"

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(
            coord_mod.dt_util,
            "as_local",
            side_effect=lambda value: value.astimezone(
                ZoneInfo("Europe/Amsterdam")
            ),
        ),
        patch.object(
            consumption,
            "_async_fetch_data",
            new_callable=AsyncMock,
        ) as consumption_fetch,
        patch.object(
            pricing,
            "_async_fetch_data",
            new_callable=AsyncMock,
        ) as pricing_fetch,
        patch.object(consumption, "_insert_statistics", new_callable=AsyncMock),
    ):
        consumption_fetch.side_effect = mock_fetch_side_effect
        pricing_fetch.side_effect = mock_fetch_side_effect
        consumption_result = await consumption._async_update_data_internal()
        pricing_result = await pricing._async_update_data_internal()

    assert consumption_result["electricity_import_month_to_date_cost"] == (
        pytest.approx(0.2)
    )
    assert len(pricing_result["prices_today"]) == 48
    electricity_tariff_urls = [
        url for url in urls if "tarieven/electricity" in url
    ]
    assert len(electricity_tariff_urls) == 2
    assert sum("startDate=2026-04-20" in url for url in electricity_tariff_urls) == 1
    assert sum("startDate=2026-04-21" in url for url in electricity_tariff_urls) == 1


@pytest.mark.asyncio
async def test_cold_pricing_skips_tomorrow_before_publication_cutoffs(auth_mock):
    """Test a pre-cutoff cold start only requests today's public tariffs."""
    mock_now = datetime.datetime(2026, 4, 20, 3, tzinfo=datetime.timezone.utc)
    tariff_cache = coord_mod.TariffCache(
        None,
        "Europe/Amsterdam",
        clock=lambda: mock_now,
    )
    coordinator = ANWBPricingCoordinator(
        MagicMock(),
        auth_mock,
        MagicMock(),
        tariff_cache=tariff_cache,
    )
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    urls = []

    async def mock_fetch_side_effect(url, token):
        urls.append(url)
        commodity_price = 20.0 if "electricity" in url else 100.0
        return {
            "data": _complete_amsterdam_hourly_tariffs(
                datetime.date(2026, 4, 20),
                all_in_price=commodity_price,
                market_price=10.0,
            )
        }

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(
            coord_mod.dt_util,
            "as_local",
            side_effect=lambda value: value.astimezone(
                ZoneInfo("Europe/Amsterdam")
            ),
        ),
        patch.object(
            coordinator,
            "_async_fetch_data",
            new_callable=AsyncMock,
        ) as mock_fetch,
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    tariff_urls = [url for url in urls if "tarieven/" in url]
    assert len(tariff_urls) == 2
    assert all("startDate=2026-04-20" in url for url in tariff_urls)
    assert not any("startDate=2026-04-21" in url for url in tariff_urls)
    assert len(result["prices_today"]) == 24
    assert len(result["gas_prices_today"]) == 24


@pytest.mark.asyncio
async def test_pricing_retries_partial_tomorrow_data_and_skips_gas_when_unused(
    auth_mock,
):
    """Test partial publication retries through the TTL without gas requests."""
    cache_now = [
        datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)
    ]
    tariff_cache = coord_mod.TariffCache(
        None,
        "Europe/Amsterdam",
        clock=lambda: cache_now[0],
    )
    coordinator = ANWBPricingCoordinator(
        MagicMock(),
        auth_mock,
        MagicMock(),
        tariff_cache=tariff_cache,
        gas_applicable=lambda: False,
    )
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    urls = []
    tomorrow_calls = 0

    async def mock_fetch_side_effect(url, token):
        nonlocal tomorrow_calls
        urls.append(url)
        if "startDate=2026-04-21" in url:
            tomorrow_calls += 1
            rows = _complete_amsterdam_hourly_tariffs(
                datetime.date(2026, 4, 21),
                all_in_price=30.0,
                market_price=15.0,
            )
            return {"data": rows if tomorrow_calls > 1 else rows[:1]}
        return {
            "data": _complete_amsterdam_hourly_tariffs(
                datetime.date(2026, 4, 20),
                all_in_price=20.0,
                market_price=10.0,
            )
        }

    with (
        patch.object(
            coord_mod.dt_util,
            "utcnow",
            side_effect=lambda: cache_now[0],
        ),
        patch.object(
            coord_mod.dt_util,
            "as_local",
            side_effect=lambda value: value.astimezone(
                ZoneInfo("Europe/Amsterdam")
            ),
        ),
        patch.object(
            coordinator,
            "_async_fetch_data",
            new_callable=AsyncMock,
        ) as mock_fetch,
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        first = await coordinator._async_update_data_internal()
        coordinator.data = first
        cache_now[0] += datetime.timedelta(minutes=31)
        second = await coordinator._async_update_data_internal()

    assert len(first["prices_today"]) == 25
    assert len(second["prices_today"]) == 48
    assert len(second["market_prices_today"]) == 48
    assert tomorrow_calls == 2
    assert not any("tarieven/gas" in url for url in urls)


@pytest.mark.asyncio
async def test_pricing_does_not_hide_auth_failure_behind_cached_data(auth_mock):
    """Test Home Assistant can start reauthentication after a pricing failure."""
    coordinator = ANWBPricingCoordinator(MagicMock(), auth_mock, MagicMock())
    coordinator.data = {"prices_today": {"cached": 1.0}}

    with patch.object(
        coordinator,
        "_async_update_data_internal",
        new_callable=AsyncMock,
        side_effect=ConfigEntryAuthFailed("reauthentication required"),
    ):
        with pytest.raises(ConfigEntryAuthFailed, match="reauthentication required"):
            await coordinator._async_update_data()


@pytest.mark.asyncio
async def test_year_to_date_costs_combine_closed_days_with_current_hours(auth_mock):
    """Test YTD estimates combine local-date daily rows with hourly MTD costs."""
    coordinator = ANWBConsumptionCoordinator(MagicMock(), auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    urls = []

    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        urls.append(url)
        if "electricity/cache" in url and "interval=HOUR" in url:
            return {"data": [{"startDate": "2026-04-20T10:00:00.000Z", "usage": 2.0}]}
        if "production/cache" in url and "interval=HOUR" in url:
            return {"data": [{"startDate": "2026-04-20T10:00:00.000Z", "usage": 1.0}]}
        if "gas/cache" in url and "interval=HOUR" in url:
            return {
                "data": [
                    {
                        "startDate": "2026-04-20T10:00:00.000Z",
                        "usage": 3.0,
                        "vasteKosten": {"abonnementsKosten": 3.0},
                    }
                ]
            }
        if "electricity/cache" in url and "interval=MONTH" in url:
            return {
                "data": [
                    {"startDate": "2026-01-01T00:00:00.000Z", "usage": 30.0},
                    {"startDate": "2026-04-01T00:00:00.000Z", "usage": 2.0},
                ]
            }
        if "production/cache" in url and "interval=MONTH" in url:
            return {
                "data": [
                    {"startDate": "2026-01-01T00:00:00.000Z", "usage": 4.0},
                    {"startDate": "2026-04-01T00:00:00.000Z", "usage": 1.0},
                ]
            }
        if "gas/cache" in url and "interval=MONTH" in url:
            return {
                "data": [
                    {"startDate": "2026-01-01T00:00:00.000Z", "usage": 5.0},
                    {"startDate": "2026-04-01T00:00:00.000Z", "usage": 3.0},
                ]
            }
        if "electricity/cache" in url and "interval=DAY" in url:
            return {
                "data": [
                    {"startDate": "2026-01-01T00:00:00.000Z", "usage": 10.0},
                    {"startDate": "2026-03-31T00:00:00.000Z", "usage": 20.0},
                    {"startDate": "2026-04-01T00:00:00.000Z", "usage": 100.0},
                ]
            }
        if "production/cache" in url and "interval=DAY" in url:
            return {
                "data": [
                    {"startDate": "2026-01-01T00:00:00.000Z", "usage": 4.0},
                    {"startDate": "2026-04-01T00:00:00.000Z", "usage": 50.0},
                ]
            }
        if "gas/cache" in url and "interval=DAY" in url:
            return {
                "data": [
                    {"startDate": "2026-01-01T00:00:00.000Z", "usage": 5.0},
                    {"startDate": "2026-04-01T00:00:00.000Z", "usage": 30.0},
                ]
            }
        if "tarieven/electricity" in url and "interval=DAY" in url:
            return {
                "data": [
                    {
                        "date": "2026-01-01T00:00:00.000Z",
                        "values": {"allInPrijs": 10.0},
                    },
                    {
                        "date": "2026-03-31T00:00:00.000Z",
                        "values": {"allInPrijs": -5.0},
                    },
                ]
            }
        if "tarieven/gas" in url and "interval=DAY" in url:
            return {
                "data": [
                    {
                        "date": "2026-01-01T00:00:00.000Z",
                        "values": {"allInPrijs": 100.0},
                    }
                ]
            }
        if "tarieven/electricity" in url:
            return {
                "data": [
                    {
                        "date": "2026-04-20T10:00:00.000Z",
                        "values": {"allInPrijs": 20.0},
                    }
                ]
            }
        if "tarieven/gas" in url:
            return {
                "data": [
                    {
                        "date": "2026-04-20T10:00:00.000Z",
                        "values": {"allInPrijs": 120.0},
                    }
                ]
            }
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["electricity_import_year_to_date"] == 32.0
    assert result["electricity_import_year_to_date_cost"] == pytest.approx(0.4)
    assert result["electricity_export_year_to_date"] == 5.0
    assert result["electricity_export_year_to_date_credit"] == pytest.approx(0.6)
    assert result["gas_year_to_date"] == 8.0
    assert result["gas_year_to_date_cost"] == pytest.approx(8.6)

    assert (
        result["electricity_import_year_to_date_tariff_coverage"]["required_intervals"]
        == 3
    )
    assert (
        result["electricity_import_year_to_date_tariff_coverage"]["matched_intervals"]
        == 3
    )
    assert result["electricity_export_year_to_date_tariff_coverage"]["complete"] is True
    assert result["gas_year_to_date_tariff_coverage"]["complete"] is True
    assert (
        result["year_to_date_cost_calculation_method"]
        == "daily_closed_months_hourly_current_month"
    )

    daily_tariff_urls = [
        url for url in urls if "tarieven/" in url and "interval=DAY" in url
    ]
    hourly_tariff_urls = [
        url for url in urls if "tarieven/" in url and "interval=HOUR" in url
    ]
    assert len(hourly_tariff_urls) == 2
    assert len(daily_tariff_urls) == 2
    assert len(hourly_tariff_urls) + len(daily_tariff_urls) == 4
    assert all("startDate=2025-12-31T00:00:00.000Z" in url for url in daily_tariff_urls)


@pytest.mark.asyncio
@pytest.mark.parametrize("daily_tariff_fails", [False, True], ids=["missing", "failed"])
async def test_incomplete_daily_tariffs_do_not_affect_month_to_date(
    auth_mock, daily_tariff_fails
):
    """Test missing or failed DAY tariffs only invalidate the YTD estimate."""
    coordinator = ANWBConsumptionCoordinator(MagicMock(), auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if "electricity/cache" in url and "interval=HOUR" in url:
            return {"data": [{"startDate": "2026-04-20T10:00:00.000Z", "usage": 1.0}]}
        if "production/cache" in url and "interval=HOUR" in url:
            return {"data": []}
        if "electricity/cache" in url and "interval=MONTH" in url:
            return {"data": [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 2.0}]}
        if "production/cache" in url and "interval=MONTH" in url:
            return {"data": [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 0.0}]}
        if "electricity/cache" in url and "interval=DAY" in url:
            return {"data": [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 2.0}]}
        if "production/cache" in url and "interval=DAY" in url:
            return {"data": [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 0.0}]}
        if "tarieven/electricity" in url and "interval=DAY" in url:
            if daily_tariff_fails:
                raise UpdateFailed("daily tariff unavailable")
            return {"data": []}
        if "tarieven/electricity" in url:
            return {
                "data": [
                    {
                        "date": "2026-04-20T10:00:00.000Z",
                        "values": {"allInPrijs": 20.0},
                    }
                ]
            }
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["electricity_import_month_to_date_cost"] == pytest.approx(0.2)
    assert result["electricity_import_tariff_coverage"]["complete"] is True
    assert result["electricity_import_year_to_date_cost"] is None
    assert (
        result["electricity_import_year_to_date_tariff_coverage"]["complete"] is False
    )
    assert result["electricity_export_year_to_date_credit"] == 0.0
    assert result["electricity_export_year_to_date_tariff_coverage"]["complete"] is True
    if daily_tariff_fails:
        assert (
            result["electricity_import_year_to_date_tariff_coverage"]["reason"]
            == "daily_tariff_unavailable"
        )
    else:
        assert (
            result["electricity_import_year_to_date_tariff_coverage"][
                "missing_intervals_count"
            ]
            == 1
        )


@pytest.mark.asyncio
async def test_failed_daily_fill_keeps_complete_cached_import_cost(auth_mock):
    """Test a missing export tariff cannot hide a cached import estimate."""
    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)
    tariff_cache = coord_mod.TariffCache(
        None,
        "Europe/Amsterdam",
        clock=lambda: mock_now,
    )

    async def seed_tariff(_missing):
        return {"2026-01-01": 10.0}

    await tariff_cache.async_get_daily_prices(
        "electricity",
        {datetime.date(2026, 1, 1)},
        seed_tariff,
    )

    coordinator = ANWBConsumptionCoordinator(
        MagicMock(),
        auth_mock,
        MagicMock(),
        tariff_cache=tariff_cache,
    )
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    async def mock_fetch_side_effect(url, token):
        if "electricity/cache" in url and "interval=MONTH" in url:
            return {
                "data": [
                    {"startDate": "2026-01-01T00:00:00.000Z", "usage": 10.0}
                ]
            }
        if "production/cache" in url and "interval=MONTH" in url:
            return {
                "data": [
                    {"startDate": "2026-01-02T00:00:00.000Z", "usage": 5.0}
                ]
            }
        if "electricity/cache" in url and "interval=DAY" in url:
            return {
                "data": [
                    {"startDate": "2026-01-01T00:00:00.000Z", "usage": 10.0}
                ]
            }
        if "production/cache" in url and "interval=DAY" in url:
            return {
                "data": [
                    {"startDate": "2026-01-02T00:00:00.000Z", "usage": 5.0}
                ]
            }
        if "tarieven/electricity" in url and "interval=DAY" in url:
            raise UpdateFailed("missing export DAY tariff unavailable")
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator,
            "_async_fetch_data",
            new_callable=AsyncMock,
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["electricity_import_year_to_date_cost"] == pytest.approx(1.0)
    assert result["electricity_import_year_to_date_tariff_coverage"]["complete"] is True
    assert result["electricity_export_year_to_date_credit"] is None
    assert (
        result["electricity_export_year_to_date_tariff_coverage"]["reason"]
        == "daily_tariff_unavailable"
    )


@pytest.mark.asyncio
async def test_failed_daily_cache_preserves_month_usage_only(auth_mock):
    """Test a failed DAY stream keeps MONTH usage but not the new cost estimate."""
    coordinator = ANWBConsumptionCoordinator(MagicMock(), auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    coordinator.data = {
        "yearly_period_start": "2026-01-01T00:00:00+00:00",
        "electricity_import_year_to_date": 10.0,
        "electricity_import_year_to_date_cost": 2.0,
        "electricity_import_year_to_date_tariff_coverage": {
            "complete": True,
            "required_intervals": 5,
            "matched_intervals": 5,
            "missing_intervals_count": 0,
            "missing_intervals": [],
        },
    }

    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if "electricity/cache" in url and "interval=DAY" in url:
            raise UpdateFailed("import year cache unavailable")
        if "production/cache" in url and "interval=DAY" in url:
            return {"data": [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 2.0}]}
        if "electricity/cache" in url and "interval=MONTH" in url:
            return {"data": [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 10.0}]}
        if "production/cache" in url and "interval=MONTH" in url:
            return {"data": [{"startDate": "2026-01-01T00:00:00.000Z", "usage": 2.0}]}
        if (
            "electricity/cache" in url or "production/cache" in url
        ) and "interval=HOUR" in url:
            return {"data": [{"startDate": "2026-04-20T10:00:00.000Z", "usage": 1.0}]}
        if "tarieven/electricity" in url and "interval=DAY" in url:
            return {
                "data": [
                    {
                        "date": "2026-01-01T00:00:00.000Z",
                        "values": {"allInPrijs": 10.0},
                    }
                ]
            }
        if "tarieven/electricity" in url:
            return {
                "data": [
                    {
                        "date": "2026-04-20T10:00:00.000Z",
                        "values": {"allInPrijs": 20.0},
                    }
                ]
            }
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["electricity_import_month_to_date_cost"] == pytest.approx(0.2)
    assert result["electricity_import_year_to_date"] == 10.0
    assert result["electricity_import_year_to_date_cost"] is None
    assert (
        result["electricity_import_year_to_date_tariff_coverage"]["reason"]
        == "daily_usage_unavailable"
    )

    assert result["electricity_export_month_to_date_credit"] == pytest.approx(0.2)
    assert result["electricity_export_year_to_date"] == 2.0
    assert result["electricity_export_year_to_date_credit"] == pytest.approx(0.4)
    assert result["electricity_export_year_to_date_tariff_coverage"]["complete"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cached_year_usage", "current_usage", "expected_year_cost"),
    [(10.0, 1.0, None), (0.0, 0.0, 0.0)],
    ids=["nonzero-cache", "zero-cache"],
)
async def test_empty_month_cache_handles_cached_year_usage_safely(
    auth_mock, cached_year_usage, current_usage, expected_year_cost
):
    """Test an empty MONTH response distinguishes nonzero and zero cached usage."""
    coordinator = ANWBConsumptionCoordinator(MagicMock(), auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    coordinator.data = {
        "yearly_period_start": "2026-01-01T00:00:00+00:00",
        "electricity_import_year_to_date": cached_year_usage,
    }

    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if "electricity/cache" in url and "interval=HOUR" in url:
            return {
                "data": [
                    {
                        "startDate": "2026-04-20T10:00:00.000Z",
                        "usage": current_usage,
                    }
                ]
            }
        if "production/cache" in url and "interval=HOUR" in url:
            return {"data": []}
        if "tarieven/electricity" in url:
            return {
                "data": [
                    {
                        "date": "2026-04-20T10:00:00.000Z",
                        "values": {"allInPrijs": 20.0},
                    }
                ]
            }
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["electricity_import_month_to_date_cost"] == pytest.approx(
        current_usage * 0.2
    )
    assert result["electricity_import_year_to_date"] == cached_year_usage
    if expected_year_cost is None:
        assert result["electricity_import_year_to_date_cost"] is None
        assert (
            result["electricity_import_year_to_date_tariff_coverage"]["reason"]
            == "year_to_date_usage_unavailable"
        )
    else:
        assert result["electricity_import_year_to_date_cost"] == pytest.approx(
            expected_year_cost
        )
        assert (
            result["electricity_import_year_to_date_tariff_coverage"]["complete"]
            is True
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("year_cache_fails", [False, True], ids=["available", "failed"])
async def test_january_year_to_date_cost_equals_month_to_date(
    auth_mock, year_cache_fails
):
    """Test January needs no closed-month DAY tariff request."""
    coordinator = ANWBConsumptionCoordinator(MagicMock(), auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    urls = []

    mock_now = datetime.datetime(2026, 1, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        urls.append(url)
        if (
            year_cache_fails
            and ("electricity/cache" in url or "production/cache" in url)
            and "interval=DAY" in url
        ):
            raise UpdateFailed("year cache unavailable")
        if "electricity/cache" in url:
            return {"data": [{"startDate": "2026-01-20T10:00:00.000Z", "usage": 1.0}]}
        if "production/cache" in url or "gas/cache" in url:
            return {"data": []}
        if "tarieven/electricity" in url:
            return {
                "data": [
                    {
                        "date": "2026-01-20T10:00:00.000Z",
                        "values": {"allInPrijs": 20.0},
                    }
                ]
            }
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["electricity_import_month_to_date_cost"] == pytest.approx(0.2)
    assert result["electricity_import_year_to_date_cost"] == pytest.approx(0.2)
    assert (
        result["electricity_import_year_to_date_tariff_coverage"]
        == result["electricity_import_tariff_coverage"]
    )
    assert not any("tarieven/" in url and "interval=DAY" in url for url in urls)
    assert not any("/cache" in url and "interval=DAY" in url for url in urls)


@pytest.mark.asyncio
async def test_january_gas_year_cost_uses_hourly_data_when_day_cache_fails(auth_mock):
    """Test January gas YTD stays available from the complete hourly period."""
    coordinator = ANWBConsumptionCoordinator(MagicMock(), auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(2026, 1, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if "gas/cache" in url and "interval=DAY" in url:
            raise UpdateFailed("gas year cache unavailable")
        if "gas/cache" in url and "interval=HOUR" in url:
            return {
                "data": [
                    {
                        "startDate": "2026-01-20T10:00:00.000Z",
                        "usage": 2.0,
                        "vasteKosten": {"abonnementsKosten": 3.0},
                    }
                ]
            }
        if "tarieven/gas" in url:
            return {
                "data": [
                    {
                        "date": "2026-01-20T10:00:00.000Z",
                        "values": {"allInPrijs": 120.0},
                    }
                ]
            }
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["gas_month_to_date"] == 2.0
    assert result["gas_year_to_date"] == 2.0
    assert result["gas_month_to_date_cost"] == pytest.approx(2.4)
    assert result["gas_year_to_date_cost"] == pytest.approx(2.4)
    assert result["gas_year_to_date_tariff_coverage"] == result["gas_tariff_coverage"]
    assert result["gas_year_data_available"] is True


@pytest.mark.asyncio
async def test_missing_tariff_keeps_usage_but_makes_cost_incomplete(auth_mock):
    """Test nonzero usage without a tariff does not become a zero cost."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if "electricity/cache" in url and "interval=HOUR" in url:
            return {
                "data": [
                    {"startDate": "2026-04-20T09:00:00.000Z", "usage": 1.0},
                    {"startDate": "2026-04-20T10:00:00.000Z", "usage": 0.0},
                    {"startDate": "2026-04-20T11:00:00.000Z", "usage": 2.0},
                ]
            }
        if "production/cache" in url and "interval=HOUR" in url:
            return {
                "data": [
                    {"startDate": "2026-04-20T09:00:00.000Z", "usage": 1.0},
                    {"startDate": "2026-04-20T10:00:00.000Z", "usage": 2.0},
                ]
            }
        if "tarieven/electricity" in url:
            return {
                "data": [
                    {
                        "date": "2026-04-20T09:00:00.000Z",
                        "values": {"allInPrijs": -5.0},
                    },
                    {
                        "date": "2026-04-20T10:00:00.000Z",
                        "values": {"allInPrijs": 0},
                    },
                    {
                        "date": "2026-04-20T11:00:00.000Z",
                        "values": {},
                    },
                ]
            }
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["electricity_import_month_to_date"] == 3.0
    assert result["import_usage"] == 3.0
    assert result["electricity_import_month_to_date_cost"] is None
    assert result["import_cost"] is None
    assert result["electricity_month_to_date_total_cost"] is None
    assert result["total_cost"] is None
    assert result["electricity_import_tariff_coverage"] == {
        "complete": False,
        "required_intervals": 2,
        "matched_intervals": 1,
        "missing_intervals_count": 1,
        "missing_intervals": ["2026-04-20T11:00:00.000Z"],
    }

    assert result["electricity_export_month_to_date_credit"] == pytest.approx(-0.05)
    assert result["export_cost"] == pytest.approx(-0.05)
    assert result["electricity_export_tariff_coverage"]["complete"] is True
    assert result["electricity_export_tariff_coverage"]["required_intervals"] == 2
    assert result["electricity_export_tariff_coverage"]["matched_intervals"] == 2

    assert result["has_gas"] is False
    assert result["gas_fixed_cost_source"] == "not_applicable"
    assert result["gas_month_to_date_fixed_cost"] == 0.0


@pytest.mark.asyncio
async def test_tariffs_are_requested_only_for_days_with_nonzero_usage(auth_mock):
    """Test unused days do not cause tariff requests."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(2026, 4, 2, 12, tzinfo=datetime.timezone.utc)
    urls = []

    async def mock_fetch_side_effect(url, token):
        urls.append(url)
        if "electricity/cache" in url and "interval=HOUR" in url:
            return {"data": [{"startDate": "2026-04-02T10:00:00.000Z", "usage": 1.0}]}
        if "tarieven/electricity" in url:
            if "startDate=2026-04-01" in url:
                raise UpdateFailed("temporary tariff failure")
            if "startDate=2026-04-02" in url:
                return {
                    "data": [
                        {
                            "date": "2026-04-02T10:00:00.000Z",
                            "values": {"allInPrijs": 25.0},
                        }
                    ]
                }
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["electricity_import_month_to_date_cost"] == pytest.approx(0.25)
    assert result["electricity_import_tariff_coverage"]["complete"] is True
    assert result["electricity_import_tariff_coverage"]["matched_intervals"] == 1
    tariff_urls = [url for url in urls if "tarieven/" in url]
    assert len(tariff_urls) == 1
    assert "startDate=2026-04-02" in tariff_urls[0]


@pytest.mark.asyncio
async def test_fixed_fallback_accrues_by_calendar_day_for_zero_usage(auth_mock):
    """Test standing charges accrue even without current nonzero usage."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if "electricity/cache" in url and "interval=HOUR" in url:
            return {"data": [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 0.0}]}
        if "gas/cache" in url and "interval=HOUR" in url:
            return {"data": [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 0.0}]}
        if "gas/cache" in url and "interval=DAY" in url:
            return {"data": [{"startDate": "2026-04-01T00:00:00.000Z", "usage": 0.0}]}
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    expected_electricity_fixed = (8.50 + 39.73 - 52.41) * (20 / 30)
    expected_gas_fixed = (8.50 + 17.50) * (20 / 30)

    assert result["electricity_month_to_date_fixed_cost"] == pytest.approx(
        expected_electricity_fixed
    )
    assert result["electricity_fixed_cost_source"] == "hardcoded_fallback"
    assert result["has_gas"] is True
    assert result["gas_month_to_date"] == 0.0
    assert result["gas_month_to_date_fixed_cost"] == pytest.approx(expected_gas_fixed)
    assert result["gas_fixed_cost_source"] == "hardcoded_fallback"
    assert result["gas_month_to_date_total_cost"] == pytest.approx(expected_gas_fixed)


@pytest.mark.asyncio
async def test_known_gas_contract_can_have_empty_successful_cache(auth_mock):
    """Test an empty successful gas cache does not hide a known contract."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    coordinator.data = {"has_gas": True}

    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["has_gas"] is True
    assert result["gas_month_to_date"] == 0.0
    assert result["gas_fixed_cost_source"] == "unavailable"
    assert result["gas_month_to_date_fixed_cost"] is None
    assert result["gas_month_to_date_total_cost"] is None
    assert result["gas_month_data_available"] is True


@pytest.mark.asyncio
async def test_gas_detection_expires_after_empty_new_year_caches(auth_mock):
    """Test historical gas detection does not remain sticky forever."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    coordinator.data = {
        "has_gas": True,
        "monthly_period_start": "2025-12-01T00:00:00+00:00",
        "yearly_period_start": "2025-01-01T00:00:00+00:00",
    }

    mock_now = datetime.datetime(2026, 1, 2, 12, tzinfo=datetime.timezone.utc)

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator,
            "_async_fetch_data",
            new_callable=AsyncMock,
            return_value={"data": []},
        ),
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        result = await coordinator._async_update_data_internal()

    assert result["has_gas"] is False
    assert result["gas_month_to_date"] == 0.0
    assert result["gas_year_to_date"] == 0.0
    assert result["gas_fixed_cost_source"] == "not_applicable"
    assert result["gas_month_to_date_fixed_cost"] == 0.0


@pytest.mark.asyncio
@pytest.mark.parametrize("gas_cache_fails", [False, True], ids=["empty", "failed"])
async def test_gas_cache_gap_reuses_same_period_values(auth_mock, gas_cache_fails):
    """Test an empty or failed gas refresh retains known same-period values."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    coordinator.data = {
        "has_gas": True,
        "monthly_period_start": "2026-04-01T00:00:00+00:00",
        "yearly_period_start": "2026-01-01T00:00:00+00:00",
        "gas_month_data_available": True,
        "gas_year_data_available": True,
        "gas_month_to_date": 4.5,
        "gas_month_to_date_cost": 5.5,
        "gas_month_to_date_fixed_cost": 6.5,
        "gas_month_to_date_total_cost": 12.0,
        "gas_tariff_coverage": {"complete": True},
        "gas_fixed_cost_source": "account_cache",
        "gas_year_to_date": 40.0,
        "gas_year_to_date_cost": 44.0,
        "gas_year_to_date_tariff_coverage": {
            "complete": True,
            "required_intervals": 40,
            "matched_intervals": 40,
            "missing_intervals_count": 0,
            "missing_intervals": [],
        },
    }

    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if gas_cache_fails and "gas/cache" in url:
            raise UpdateFailed("temporary gas cache failure")
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["has_gas"] is True
    assert result["gas_month_to_date"] == 4.5
    assert result["gas_month_to_date_cost"] == 5.5
    assert result["gas_month_to_date_fixed_cost"] == 6.5
    assert result["gas_month_to_date_total_cost"] == 12.0
    assert result["gas_year_to_date"] == 40.0
    assert result["gas_year_to_date_cost"] is None
    assert result["gas_year_to_date_tariff_coverage"]["complete"] is False
    assert result["gas_fixed_cost_source"] == "account_cache"
    assert result["gas_month_data_available"] is True
    assert result["gas_year_data_available"] is True


@pytest.mark.asyncio
async def test_failed_gas_cache_does_not_reuse_previous_month(auth_mock):
    """Test a failed gas refresh never publishes prior-month values as current."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    coordinator.data = {
        "has_gas": True,
        "monthly_period_start": "2026-03-01T00:00:00+00:00",
        "yearly_period_start": "2026-01-01T00:00:00+00:00",
        "gas_month_data_available": True,
        "gas_year_data_available": True,
        "gas_month_to_date": 4.5,
        "gas_month_to_date_cost": 5.5,
        "gas_month_to_date_fixed_cost": 6.5,
        "gas_month_to_date_total_cost": 12.0,
        "gas_year_to_date": 40.0,
        "gas_year_to_date_cost": 44.0,
    }

    mock_now = datetime.datetime(2026, 4, 1, 0, 30, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if "gas/cache" in url:
            raise UpdateFailed("temporary gas cache failure")
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["has_gas"] is True
    assert result["gas_month_to_date"] is None
    assert result["gas_month_to_date_cost"] is None
    assert result["gas_month_to_date_fixed_cost"] is None
    assert result["gas_month_to_date_total_cost"] is None
    assert result["gas_month_data_available"] is False
    assert result["gas_year_to_date"] == 40.0
    assert result["gas_year_to_date_cost"] is None
    assert result["gas_year_data_available"] is True


@pytest.mark.asyncio
async def test_failed_gas_cache_does_not_reuse_previous_year_cost(auth_mock):
    """Test a failed January refresh never republishes prior-year gas cost."""
    coordinator = ANWBConsumptionCoordinator(MagicMock(), auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"
    coordinator.data = {
        "has_gas": True,
        "monthly_period_start": "2025-12-01T00:00:00+00:00",
        "yearly_period_start": "2025-01-01T00:00:00+00:00",
        "gas_month_data_available": True,
        "gas_year_data_available": True,
        "gas_month_to_date": 4.5,
        "gas_year_to_date": 40.0,
        "gas_year_to_date_cost": 44.0,
    }

    mock_now = datetime.datetime(2026, 1, 1, 0, 30, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if "gas/cache" in url:
            raise UpdateFailed("temporary gas cache failure")
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert result["has_gas"] is True
    assert result["gas_year_to_date"] is None
    assert result["gas_year_to_date_cost"] is None
    assert result["gas_year_data_available"] is False
    assert result["gas_year_to_date_tariff_coverage"]["complete"] is False


@pytest.mark.asyncio
async def test_initial_gas_cache_failure_retries_contract_detection(auth_mock):
    """Test first setup retries instead of permanently omitting gas entities."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if "gas/cache" in url:
            raise UpdateFailed("temporary gas cache failure")
        return {"data": []}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        with pytest.raises(UpdateFailed, match="determine whether the account has gas"):
            await coordinator._async_update_data_internal()


@pytest.mark.asyncio
async def test_pricing_keeps_local_day_rows_with_previous_utc_date(auth_mock):
    """Test local-day tariff rows are not dropped when their UTC date differs."""
    hass = MagicMock()
    config_entry = MagicMock()

    coordinator = ANWBPricingCoordinator(
        hass,
        auth_mock,
        config_entry,
        tariff_cache=coord_mod.TariffCache(None, "Europe/Amsterdam"),
    )
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

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=as_cest),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
    ):
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
async def test_pricing_only_stores_numeric_tariffs(auth_mock):
    """Test missing tariffs are omitted while zero and negative values survive."""
    hass = MagicMock()
    coordinator = ANWBPricingCoordinator(hass, auth_mock, MagicMock())
    coordinator._kraken_token = "mock_kraken_token"
    coordinator._account_number = "12345"
    coordinator._account_address = "Mock Address"

    mock_now = datetime.datetime(2026, 6, 15, 12, tzinfo=datetime.timezone.utc)

    async def mock_fetch_side_effect(url, token):
        if "tarieven/electricity" in url:
            if "startDate=2026-06-16" in url:
                raise UpdateFailed("temporary electricity tariff failure")
            return {
                "data": [
                    {
                        "date": "2026-06-15T09:00:00.000Z",
                        "values": {},
                    },
                    {
                        "date": "2026-06-15T10:00:00.000Z",
                        "values": {"allInPrijs": 0, "marktprijs": 0},
                    },
                    {
                        "date": "2026-06-15T11:00:00.000Z",
                        "values": {"allInPrijs": -5.0, "marktprijs": -10.0},
                    },
                    {
                        "date": "2026-06-15T12:00:00.000Z",
                        "values": {
                            "allInPrijs": "20.0",
                            "marktprijs": "10.0",
                        },
                    },
                ]
            }
        if "tarieven/gas" in url:
            if "startDate=2026-06-16" in url:
                raise UpdateFailed("temporary gas tariff failure")
            return {
                "data": [
                    {
                        "date": "2026-06-15T09:00:00.000Z",
                        "values": {"allInPrijs": None},
                    },
                    {
                        "date": "2026-06-15T10:00:00.000Z",
                        "values": {"allInPrijs": 0},
                    },
                    {
                        "date": "2026-06-15T11:00:00.000Z",
                        "values": {"allInPrijs": -1.0},
                    },
                ]
            }
        return {}

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(coord_mod.dt_util, "as_local", side_effect=lambda value: value),
        patch.object(
            coordinator, "_async_fetch_data", new_callable=AsyncMock
        ) as mock_fetch,
    ):
        mock_fetch.side_effect = mock_fetch_side_effect
        result = await coordinator._async_update_data_internal()

    assert "2026-06-15T09:00:00.000Z" not in result["prices_today"]
    assert "2026-06-15T12:00:00.000Z" not in result["prices_today"]
    assert result["prices_today"]["2026-06-15T10:00:00.000Z"] == 0.0
    assert result["prices_today"]["2026-06-15T11:00:00.000Z"] == -5.0

    assert "2026-06-15T09:00:00.000Z" not in result["market_prices_today"]
    assert "2026-06-15T12:00:00.000Z" not in result["market_prices_today"]
    assert result["market_prices_today"]["2026-06-15T10:00:00.000Z"] == 0.0
    assert result["market_prices_today"]["2026-06-15T11:00:00.000Z"] == -10.0

    assert "2026-06-15T09:00:00.000Z" not in result["gas_prices_today"]
    assert result["gas_prices_today"]["2026-06-15T10:00:00.000Z"] == 0.0
    assert result["gas_prices_today"]["2026-06-15T11:00:00.000Z"] == -1.0


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
async def test_insert_statistics_backfills_gaps_and_updates_later_sums(auth_mock):
    """Test statistics repair a missing row instead of skipping past it."""
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

    with (
        patch.object(coord_mod, "get_instance", return_value=recorder),
        patch.object(coord_mod, "StatisticData", StatisticData),
        patch.object(coord_mod, "StatisticMetaData", StatisticMetaData),
        patch.object(coord_mod, "StatisticMeanType", SimpleNamespace(NONE="none")),
        patch.object(
            coord_mod, "async_add_external_statistics", side_effect=add_statistics
        ),
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

    assert [row.start.hour for row in recorded[statistic_id]] == [11, 12, 13]
    assert [row.sum for row in recorded[statistic_id]] == [11.0, 12.0, 13.0]


@pytest.mark.asyncio
async def test_cost_statistics_stop_at_tariff_gap_and_backfill(auth_mock):
    """Test cost statistics wait at a tariff gap while usage continues."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator.hass = hass
    coordinator._account_number = "12345"

    import_data = [
        {"startDate": "2026-04-20T10:00:00.000Z", "usage": 1.0},
        {"startDate": "2026-04-20T11:00:00.000Z", "usage": 1.0},
        {"startDate": "2026-04-20T12:00:00.000Z", "usage": 1.0},
    ]
    incomplete_prices = {
        "2026-04-20T10:00:00.000Z": 20.0,
        "2026-04-20T12:00:00.000Z": 40.0,
    }
    complete_prices = {
        **incomplete_prices,
        "2026-04-20T11:00:00.000Z": 30.0,
    }

    class StatisticData:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class StatisticMetaData:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    recorded = {}

    def add_statistics(hass, metadata, statistics):
        recorded.setdefault(metadata.statistic_id, []).extend(statistics)

    recorder = MagicMock()
    recorder.async_add_executor_job = AsyncMock(return_value={})

    with (
        patch.object(coord_mod, "get_instance", return_value=recorder),
        patch.object(coord_mod, "StatisticData", StatisticData),
        patch.object(coord_mod, "StatisticMetaData", StatisticMetaData),
        patch.object(coord_mod, "StatisticMeanType", SimpleNamespace(NONE="none")),
        patch.object(
            coord_mod, "async_add_external_statistics", side_effect=add_statistics
        ),
    ):
        await coordinator._insert_statistics(
            import_data,
            [],
            incomplete_prices,
            [],
            {},
        )

        usage_id = "anwb_energie_account:import_usage_12345"
        cost_id = "anwb_energie_account:import_cost_12345"

        assert [row.start.hour for row in recorded[usage_id]] == [10, 11, 12]
        assert [row.start.hour for row in recorded[cost_id]] == [10]
        assert recorded[cost_id][0].state == pytest.approx(0.2)

        async def existing_statistics(*args):
            statistic_id = next(iter(args[4]))
            rows = recorded.get(statistic_id, [])
            if not rows:
                return {}
            return {
                statistic_id: [
                    {"start": row.start.timestamp(), "sum": row.sum} for row in rows
                ]
            }

        recorder.async_add_executor_job.side_effect = existing_statistics

        await coordinator._insert_statistics(
            import_data,
            [],
            complete_prices,
            [],
            {},
        )

    assert [row.start.hour for row in recorded[cost_id]] == [10, 11, 12]
    assert [row.state for row in recorded[cost_id]] == pytest.approx([0.2, 0.3, 0.4])
    assert [row.sum for row in recorded[cost_id]] == pytest.approx([0.2, 0.5, 0.9])


@pytest.mark.asyncio
async def test_cost_statistics_repair_legacy_zero_tariff_rows(auth_mock):
    """Test rows understated by the former missing-price fallback are repaired."""
    hass = MagicMock()
    coordinator = ANWBConsumptionCoordinator(hass, auth_mock, MagicMock())
    coordinator.hass = hass
    coordinator._account_number = "12345"

    import_data = [
        {"startDate": "2026-04-20T10:00:00.000Z", "usage": 1.0},
        {"startDate": "2026-04-20T11:00:00.000Z", "usage": 1.0},
        {"startDate": "2026-04-20T12:00:00.000Z", "usage": 1.0},
    ]
    prices = {
        "2026-04-20T10:00:00.000Z": 20.0,
        "2026-04-20T11:00:00.000Z": 30.0,
        "2026-04-20T12:00:00.000Z": 40.0,
    }
    cost_id = "anwb_energie_account:import_cost_12345"
    old_rows = [
        {
            "start": datetime.datetime(
                2026, 4, 20, 9, tzinfo=datetime.timezone.utc
            ).timestamp(),
            "state": 1.0,
            "sum": 10.0,
        },
        {
            "start": datetime.datetime(
                2026, 4, 20, 10, tzinfo=datetime.timezone.utc
            ).timestamp(),
            "state": 0.2,
            "sum": 10.2,
        },
        {
            "start": datetime.datetime(
                2026, 4, 20, 11, tzinfo=datetime.timezone.utc
            ).timestamp(),
            "state": 0.0,
            "sum": 10.2,
        },
        {
            "start": datetime.datetime(
                2026, 4, 20, 12, tzinfo=datetime.timezone.utc
            ).timestamp(),
            "state": 0.4,
            "sum": 10.6,
        },
    ]

    class StatisticData:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class StatisticMetaData:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    recorder = MagicMock()

    async def existing_statistics(*args):
        statistic_id = next(iter(args[4]))
        return {cost_id: old_rows} if statistic_id == cost_id else {}

    recorder.async_add_executor_job = AsyncMock(side_effect=existing_statistics)
    recorded = {}

    def add_statistics(hass, metadata, statistics):
        recorded[metadata.statistic_id] = statistics

    with (
        patch.object(coord_mod, "get_instance", return_value=recorder),
        patch.object(coord_mod, "StatisticData", StatisticData),
        patch.object(coord_mod, "StatisticMetaData", StatisticMetaData),
        patch.object(coord_mod, "StatisticMeanType", SimpleNamespace(NONE="none")),
        patch.object(
            coord_mod, "async_add_external_statistics", side_effect=add_statistics
        ),
    ):
        await coordinator._insert_statistics(
            import_data,
            [],
            prices,
            [],
            {},
        )

    assert [row.start.hour for row in recorded[cost_id]] == [11, 12]
    assert [row.state for row in recorded[cost_id]] == pytest.approx([0.3, 0.4])
    assert [row.sum for row in recorded[cost_id]] == pytest.approx([10.5, 10.9])


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

    last_success = datetime.datetime(
        2026, 5, 18, 12, 0, 0, tzinfo=datetime.timezone.utc
    )
    coordinator.last_successful_update = last_success

    with patch.object(
        coordinator,
        "_async_update_data_internal",
        side_effect=UpdateFailed("DNS failure"),
    ):
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
        with patch.object(
            coordinator,
            "_async_update_data_internal",
            side_effect=UpdateFailed("Some other error"),
        ):
            with patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now):
                with pytest.raises(UpdateFailed):
                    await coordinator._async_update_data()
