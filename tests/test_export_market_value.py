"""Test the export credit split and the export market value sensors' data."""

import dataclasses
import datetime
import logging
import math
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Import the coordinator tests first: they install the Home Assistant mocks.
from tests.test_coordinator import (
    AMSTERDAM,
    _april_account_cache,
    _april_coordinator,
    _april_kraken,
    _complete_amsterdam_hourly_tariffs,
    _kraken_days,
    _run_april_update,
    coord_mod,
)

# isort: split
from custom_components.anwb_energie_account import export_valuation
from custom_components.anwb_energie_account.kraken_costs import (
    MARKET_VALUE_LABEL,
    KrakenPeriodCosts,
)

JAN_1 = datetime.date(2026, 1, 1)
APR_1 = datetime.date(2026, 4, 1)
APRIL_NOW = datetime.datetime(2026, 4, 20, 12, tzinfo=datetime.timezone.utc)
# Synthetic splits of the export credit lines in ``_april_kraken``:
# (market value, net-metering refund) in euro; the remainder is "other".
EXPORT_SPLITS = {JAN_1: (-0.4, -1.0), APR_1: (-0.25, -0.3)}
BREAKDOWN_KEYS = {
    "electricity_export_month_to_date_credit": (
        "electricity_export_month_to_date_credit_breakdown"
    ),
    "electricity_export_year_to_date_credit": (
        "electricity_export_year_to_date_credit_breakdown"
    ),
}


@pytest.fixture
def auth_mock():
    auth = MagicMock()
    auth.async_get_access_token = AsyncMock(return_value="mock_access_token")
    auth.websession = MagicMock()
    return auth


def _split_export_days(days, *, without_market=frozenset()):
    return [
        dataclasses.replace(
            day,
            market_value=(
                None if day.day in without_market else EXPORT_SPLITS[day.day][0]
            ),
            net_metering_refund=EXPORT_SPLITS[day.day][1],
        )
        if day.day in EXPORT_SPLITS
        else day
        for day in days
    ]


def _labelled_kraken(*, without_market=frozenset(), **kwargs):
    """Wrap ``_april_kraken`` and split its export lines by label."""
    base = _april_kraken(**kwargs)

    async def fetch(direction, start, end_exclusive):
        days = await base(direction, start, end_exclusive)
        if direction == "CONSUMPTION":
            return days
        return _split_export_days(days, without_market=without_market)

    return fetch


def _cache_without_tail_market_price(url, token, **kwargs):
    """April cache whose 20 April hourly tariffs lack a market price."""
    if "tarieven/electricity" in url and "interval=HOUR" in url:
        return {
            "data": [
                *_complete_amsterdam_hourly_tariffs(
                    APR_1, all_in_price=20.0, market_price=8.0
                ),
                *_complete_amsterdam_hourly_tariffs(
                    datetime.date(2026, 4, 20), all_in_price=20.0
                ),
            ]
        }
    return _april_account_cache(url, token, **kwargs)


def _assert_breakdown_adds_up(result):
    for credit_key, breakdown_key in BREAKDOWN_KEYS.items():
        breakdown = result[breakdown_key]
        parts = [
            breakdown["booked_market_value"],
            breakdown["booked_net_metering_refund"],
            breakdown["booked_other"],
            breakdown["unbooked_credit"],
        ]
        assert sum(parts) == pytest.approx(result[credit_key], abs=0.015)


@pytest.mark.asyncio
async def test_kraken_credit_split_and_market_value(auth_mock):
    """Booked days use Kraken labels; the tail is priced at the market price."""
    coordinator = _april_coordinator(auth_mock)

    result = await _run_april_update(coordinator, _labelled_kraken())

    # Credit values are unchanged by the split.
    assert result["electricity_export_month_to_date_credit"] == pytest.approx(1.0)
    assert result["electricity_export_year_to_date_credit"] == pytest.approx(2.5)
    # Market value: booked 0.25 (April) + tail 2 kWh x EUR 0.08.
    assert result["electricity_export_month_to_date_market_value"] == pytest.approx(
        0.41
    )
    # Year: booked 0.40 (January) + 0.25 (April) + the same tail.
    assert result["electricity_export_year_to_date_market_value"] == pytest.approx(
        0.81
    )
    assert result["electricity_export_market_value_tariff_coverage"]["complete"]
    # The unbooked tail of 2026 is priced all-in, before the cutoff.
    assert result["export_price_basis"] == "all_in"
    assert result["electricity_cost_source"] == "kraken_statistics"

    # Distinct values per component catch swapped labels or attributes.
    assert result["electricity_export_month_to_date_credit_breakdown"] == {
        "booked_market_value": 0.25,
        "booked_net_metering_refund": 0.3,
        "booked_other": 0.05,
        "unbooked_kwh": 2.0,
        "unbooked_credit": 0.4,
    }
    assert result["electricity_export_year_to_date_credit_breakdown"] == {
        "booked_market_value": 0.65,
        "booked_net_metering_refund": 1.3,
        "booked_other": 0.15,
        "unbooked_kwh": 2.0,
        "unbooked_credit": 0.4,
    }
    _assert_breakdown_adds_up(result)


@pytest.mark.asyncio
async def test_missing_market_line_makes_market_value_unknown(auth_mock):
    """Without a market line the split is unknown; the credit is not."""
    coordinator = _april_coordinator(auth_mock)

    result = await _run_april_update(
        coordinator, _labelled_kraken(without_market={APR_1})
    )

    assert result["electricity_export_month_to_date_credit"] == pytest.approx(1.0)
    assert result["electricity_export_year_to_date_credit"] == pytest.approx(2.5)
    assert result["electricity_export_month_to_date_market_value"] is None
    assert result["electricity_export_year_to_date_market_value"] is None
    coverage = result["electricity_export_market_value_tariff_coverage"]
    assert coverage["complete"] is False
    assert coverage["reason"] == "kraken_market_split_unknown"
    month = result["electricity_export_month_to_date_credit_breakdown"]
    assert month["booked_market_value"] is None
    assert month["booked_net_metering_refund"] == pytest.approx(0.3)
    assert month["booked_other"] is None


@pytest.mark.asyncio
async def test_missing_market_line_in_closed_month_only_affects_the_year(auth_mock):
    """A January day without market line leaves April's market value known."""
    coordinator = _april_coordinator(auth_mock)

    result = await _run_april_update(
        coordinator, _labelled_kraken(without_market={JAN_1})
    )

    assert result["electricity_export_month_to_date_market_value"] == pytest.approx(
        0.41
    )
    assert result["electricity_export_year_to_date_market_value"] is None
    assert result["electricity_export_year_to_date_market_value_tariff_coverage"][
        "reason"
    ] == "kraken_market_split_unknown"


@pytest.mark.asyncio
async def test_market_value_unavailable_when_tail_market_price_missing(auth_mock):
    """A tail hour without market price makes the market value unavailable."""
    coordinator = _april_coordinator(auth_mock)

    result = await _run_april_update(
        coordinator, _labelled_kraken(), cache=_cache_without_tail_market_price
    )

    assert result["electricity_export_month_to_date_credit"] == pytest.approx(1.0)
    assert result["electricity_export_month_to_date_market_value"] is None
    assert result["electricity_export_year_to_date_market_value"] is None
    coverage = result["electricity_export_market_value_tariff_coverage"]
    assert coverage["complete"] is False
    assert coverage["missing_intervals_count"] == 1
    assert "reason" not in coverage
    assert result["electricity_export_year_to_date_market_value_tariff_coverage"][
        "complete"
    ] is False


@pytest.mark.asyncio
async def test_calculated_fallback_prices_market_value_hourly(auth_mock):
    """Without Kraken the market value is export kWh x hourly market price."""
    coordinator = _april_coordinator(auth_mock)

    result = await _run_april_update(coordinator, _labelled_kraken(import_jan=29.0))

    assert result["electricity_cost_source"] == "calculated"
    # 7 kWh x EUR 0.08, while the credit stays at all-in (7 x EUR 0.20).
    assert result["electricity_export_month_to_date_market_value"] == pytest.approx(
        0.56
    )
    assert result["electricity_export_month_to_date_credit"] == pytest.approx(1.4)
    assert result["export_price_basis"] == "all_in"
    for breakdown_key in BREAKDOWN_KEYS.values():
        assert result[breakdown_key] is None


@pytest.mark.asyncio
async def test_calculated_fallback_market_value_requires_market_prices(auth_mock):
    """Missing hourly market prices leave the calculated market value empty."""
    coordinator = _april_coordinator(auth_mock)

    result = await _run_april_update(
        coordinator,
        _labelled_kraken(import_jan=29.0),
        cache=_cache_without_tail_market_price,
    )

    assert result["electricity_export_month_to_date_credit"] == pytest.approx(1.4)
    assert result["electricity_export_month_to_date_market_value"] is None
    assert (
        result["electricity_export_market_value_tariff_coverage"]["complete"] is False
    )


# Year boundary: Kraken booked 1 January 2027, the tail is 2 January. The
# first hourly row starts on 31 December in UTC but belongs to 1 January.
JAN_1_2027 = datetime.date(2027, 1, 1)


def _new_year_cache(url, token, **kwargs):
    if "/cache" in url and "interval=HOUR" in url:
        if "electricity/cache" in url:
            return {
                "data": [
                    {"startDate": "2026-12-31T23:00:00.000Z", "usage": 1.0},
                    {"startDate": "2027-01-02T10:00:00.000Z", "usage": 1.0},
                ]
            }
        if "production/cache" in url:
            return {
                "data": [
                    {"startDate": "2026-12-31T23:00:00.000Z", "usage": 3.0},
                    {"startDate": "2027-01-02T11:00:00.000Z", "usage": 2.0},
                ]
            }
    if "/cache" in url and "interval=MONTH" in url:
        month = "2026-12-31T23:00:00.000Z"
        if "electricity/cache" in url:
            return {"data": [{"startDate": month, "usage": 2.0}]}
        if "production/cache" in url:
            return {"data": [{"startDate": month, "usage": 5.0}]}
    if "tarieven/electricity" in url and "interval=HOUR" in url:
        return {
            "data": [
                row
                for day in (JAN_1_2027, datetime.date(2027, 1, 2))
                for row in _complete_amsterdam_hourly_tariffs(
                    day, all_in_price=20.0, market_price=8.0
                )
            ]
        }
    return {"data": []}


async def _new_year_kraken(direction, start, end_exclusive):
    assert start == JAN_1_2027
    if direction == "CONSUMPTION":
        return _kraken_days(
            JAN_1_2027, JAN_1_2027, {JAN_1_2027: (1.0, 0.25)}, standing=-0.5
        )
    days = _kraken_days(JAN_1_2027, JAN_1_2027, {JAN_1_2027: (3.0, -0.24)})
    return [
        dataclasses.replace(day, market_value=-0.24, net_metering_refund=0.0)
        for day in days
    ]


@pytest.mark.asyncio
async def test_year_boundary_tail_and_january_month_window(auth_mock):
    """The UTC 31 December row is booked; January's year equals its month."""
    coordinator = _april_coordinator(auth_mock)
    mock_now = datetime.datetime(2027, 1, 2, 12, tzinfo=datetime.timezone.utc)

    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=mock_now),
        patch.object(
            coord_mod.dt_util,
            "as_local",
            side_effect=lambda value: value.astimezone(AMSTERDAM),
        ),
        patch.object(coordinator, "_async_fetch_data", new_callable=AsyncMock) as fetch,
        patch.object(
            coordinator,
            "_async_fetch_kraken_daily_costs",
            new=AsyncMock(side_effect=_new_year_kraken),
        ),
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        fetch.side_effect = _new_year_cache
        result = await coordinator._async_update_data_internal()

    assert result["electricity_cost_source"] == "kraken_statistics"
    assert result["electricity_provider_costs_through"] == "2027-01-01"
    # Import: booked 0.25 + 1 kWh x EUR 0.20.
    assert result["electricity_import_month_to_date_cost"] == pytest.approx(0.45)
    # After the cutoff the unbooked tail is priced at market: 2 kWh x EUR 0.08.
    assert result["export_price_basis"] == "market"
    assert result["electricity_export_month_to_date_credit"] == pytest.approx(0.4)
    assert result["electricity_export_month_to_date_market_value"] == pytest.approx(
        0.4
    )
    assert result["electricity_export_year_to_date_credit"] == pytest.approx(0.4)
    assert result["electricity_export_year_to_date_market_value"] == pytest.approx(
        0.4
    )
    assert result["electricity_export_month_to_date_credit_breakdown"] == {
        "booked_market_value": 0.24,
        "booked_net_metering_refund": 0.0,
        "booked_other": 0.0,
        "unbooked_kwh": 2.0,
        "unbooked_credit": 0.16,
    }
    _assert_breakdown_adds_up(result)


# GraphQL level: the real fetcher, its schema fallback and its label warnings.


def _graphql_statistics(day, *, labelled, without_market):
    """Return Kraken statistics for a synthetic day, in cents."""
    lines = []
    if day.standing_charge:
        lines.append(("STANDING_CHARGE_COST", None, day.standing_charge))
    if day.day in EXPORT_SPLITS:
        market, refund = EXPORT_SPLITS[day.day]
        other = day.variable_cost - market - refund
        if without_market:
            other += market
        else:
            lines.append(("CONSUMPTION_COST", MARKET_VALUE_LABEL, market))
        lines.append(("CONSUMPTION_COST", "GENERATION_ENERGY_TAX_STEP_1", refund))
        lines.append(("CONSUMPTION_COST", "GENERATION_SOMETHING_NEW", other))
    elif day.variable_cost:
        lines.append(("CONSUMPTION_COST", "SUPPLIER_DYNAMIC_COST", day.variable_cost))
    statistics = []
    for kind, label, euro in lines:
        statistic = {"type": kind, "costInclTax": {"estimatedAmount": str(euro * 100)}}
        if labelled and label is not None:
            statistic["label"] = label
        statistics.append(statistic)
    return statistics


def _graphql_page(days, *, labelled, without_market=False):
    edges = [
        {
            "node": {
                "value": str(day.usage),
                "startAt": datetime.datetime.combine(
                    day.day, datetime.time.min, tzinfo=AMSTERDAM
                ).isoformat(),
                "metaData": {
                    "statistics": _graphql_statistics(
                        day, labelled=labelled, without_market=without_market
                    )
                },
            }
        }
        for day in days
    ]
    measurements = {"pageInfo": {"hasNextPage": False}, "edges": edges}
    return {"data": {"account": {"properties": [{"measurements": measurements}]}}}


class _Response:
    def __init__(self, payload=None, status=200):
        self._payload = payload
        self._status = status

    def raise_for_status(self):
        if self._status >= 400:
            raise coord_mod.ClientResponseError(
                MagicMock(), (), status=self._status, message="rejected"
            )

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


class _KrakenServer:
    """Serve the April days; ``reject`` answers the labelled query with it."""

    def __init__(self, *, reject=None, without_market=False):
        self.reject = reject
        self.without_market = without_market
        self.labelled_posts = 0
        self.legacy_posts = 0
        # The same days as ``_april_kraken``.
        self.days = {
            "CONSUMPTION": _kraken_days(
                JAN_1,
                datetime.date(2026, 4, 19),
                {JAN_1: (30.0, 6.0), APR_1: (2.0, 0.5)},
                standing=-0.5,
            ),
            "GENERATION": _kraken_days(
                JAN_1,
                datetime.date(2026, 4, 19),
                {JAN_1: (10.0, -1.5), APR_1: (5.0, -0.6)},
            ),
        }

    def __call__(self, url, *, json, headers):
        statistics = json["query"].split("statistics {", 1)[1]
        labelled = "label" in statistics
        if labelled:
            self.labelled_posts += 1
            if self.reject == "http_400":
                return _Response(status=400)
            if self.reject == "http_401":
                return _Response(status=401)
            if self.reject == "validation":
                return _Response(
                    {
                        "errors": [
                            {
                                "message": "Cannot query field 'label'",
                                "extensions": {"errorType": "VALIDATION"},
                            }
                        ]
                    }
                )
        else:
            self.legacy_posts += 1
        days = self.days[json["variables"]["direction"]]
        return _Response(
            _graphql_page(days, labelled=labelled, without_market=self.without_market)
        )


async def _run_april_update_via_graphql(coordinator, server):
    coordinator.auth.websession.post = MagicMock(side_effect=server)
    with (
        patch.object(coord_mod.dt_util, "utcnow", return_value=APRIL_NOW),
        patch.object(
            coord_mod.dt_util,
            "as_local",
            side_effect=lambda value: value.astimezone(AMSTERDAM),
        ),
        patch.object(
            coord_mod, "_configured_time_zone_name", return_value="Europe/Amsterdam"
        ),
        patch.object(coordinator, "_async_fetch_data", new_callable=AsyncMock) as fetch,
        patch.object(coordinator, "_insert_statistics", new_callable=AsyncMock),
    ):
        fetch.side_effect = _april_account_cache
        return await coordinator._async_update_data_internal()


def _warnings(caplog):
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
    ]


@pytest.mark.asyncio
async def test_graphql_labels_split_the_credit(auth_mock):
    """The real fetcher parses labels into the same breakdown."""
    coordinator = _april_coordinator(auth_mock)

    result = await _run_april_update_via_graphql(coordinator, _KrakenServer())

    assert result["electricity_export_month_to_date_credit"] == pytest.approx(1.0)
    assert result["electricity_export_month_to_date_market_value"] == pytest.approx(
        0.41
    )
    month = result["electricity_export_month_to_date_credit_breakdown"]
    assert month["booked_market_value"] == pytest.approx(0.25)
    assert month["booked_net_metering_refund"] == pytest.approx(0.3)
    assert month["booked_other"] == pytest.approx(0.05)


@pytest.mark.asyncio
async def test_missing_market_label_warns_once_per_coordinator(auth_mock, caplog):
    """The warning names only the missing label, once per coordinator."""
    caplog.set_level(logging.WARNING)
    coordinator = _april_coordinator(auth_mock)
    server = _KrakenServer(without_market=True)

    for _ in range(2):
        result = await _run_april_update_via_graphql(coordinator, server)

    assert result["electricity_export_month_to_date_credit"] == pytest.approx(1.0)
    assert result["electricity_export_month_to_date_market_value"] is None
    assert result["electricity_export_year_to_date_market_value"] is None
    (warning,) = _warnings(caplog)
    assert MARKET_VALUE_LABEL in warning

    # Another config entry has its own coordinator and logs for itself.
    await _run_april_update_via_graphql(_april_coordinator(auth_mock), server)
    assert len(_warnings(caplog)) == 2


@pytest.mark.parametrize("reject", ["http_400", "validation"])
@pytest.mark.asyncio
async def test_rejected_label_falls_back_to_legacy_query(auth_mock, caplog, reject):
    """A schema without labels keeps costs and drops only the market split."""
    caplog.set_level(logging.WARNING)
    coordinator = _april_coordinator(auth_mock)
    server = _KrakenServer(reject=reject)

    result = await _run_april_update_via_graphql(coordinator, server)
    labelled_posts = server.labelled_posts
    legacy_posts = server.legacy_posts
    second = await _run_april_update_via_graphql(coordinator, server)

    assert labelled_posts >= 1
    # The downgrade is remembered: no rejected request on the next poll.
    assert server.labelled_posts == labelled_posts
    assert server.legacy_posts == legacy_posts + 2
    assert len(_warnings(caplog)) == 1
    for data in (result, second):
        assert data["electricity_cost_source"] == "kraken_statistics"
        assert data["electricity_import_month_to_date_cost"] == pytest.approx(0.7)
        assert data["electricity_import_year_to_date_cost"] == pytest.approx(6.7)
        assert data["electricity_month_to_date_fixed_cost"] == pytest.approx(-9.5)
        assert data["electricity_export_month_to_date_credit"] == pytest.approx(1.0)
        assert data["electricity_export_year_to_date_credit"] == pytest.approx(2.5)
        assert data["electricity_export_month_to_date_market_value"] is None
        assert data["electricity_export_year_to_date_market_value"] is None
        assert data["electricity_export_month_to_date_credit_breakdown"] == {
            "booked_market_value": None,
            "booked_net_metering_refund": None,
            "booked_other": None,
            "unbooked_kwh": 2.0,
            "unbooked_credit": 0.4,
        }


@pytest.mark.asyncio
async def test_other_http_errors_do_not_downgrade_the_query(auth_mock, caplog):
    """Only a rejected query falls back; other errors keep their old path."""
    caplog.set_level(logging.WARNING)
    coordinator = _april_coordinator(auth_mock)
    server = _KrakenServer(reject="http_401")

    result = await _run_april_update_via_graphql(coordinator, server)

    assert result["electricity_provider_costs_status"] == "fetch_failed"
    assert server.legacy_posts == 0
    assert coordinator._kraken_legacy_query is False
    assert _warnings(caplog) == []


def _period(**kwargs):
    values = {"usage": 3.0, "variable_cost": -1.0, "standing_charge": 0.0, "days": 2}
    return KrakenPeriodCosts(**(values | kwargs))


def test_booked_breakdown_negates_kraken_amounts_and_rounds():
    """Kraken's negative generation lines become positive credit attributes."""
    breakdown = export_valuation.booked_breakdown(
        _period(market_value=-0.1234, net_metering_refund=-0.8),
        unbooked_kwh=1.23456,
        unbooked_credit=None,
    )

    assert breakdown.as_attributes() == {
        "booked_market_value": 0.12,
        "booked_net_metering_refund": 0.8,
        "booked_other": 0.08,
        "unbooked_kwh": 1.235,
        "unbooked_credit": None,
    }


def test_booked_breakdown_keeps_a_positive_generation_cost():
    """A negative market price books a cost: the market value is negative."""
    breakdown = export_valuation.booked_breakdown(
        _period(variable_cost=-0.18, market_value=0.12, net_metering_refund=-0.3),
        unbooked_kwh=0.0,
        unbooked_credit=0.0,
    )

    attributes = breakdown.as_attributes()
    assert attributes["booked_market_value"] == -0.12
    assert attributes["booked_net_metering_refund"] == 0.3
    assert sum(
        value for key, value in attributes.items() if key != "unbooked_kwh"
    ) == pytest.approx(0.18)


def test_breakdown_attributes_have_no_negative_zero():
    """Amounts that round to zero are reported as 0.0, never -0.0."""
    breakdown = export_valuation.booked_breakdown(
        _period(variable_cost=0.001, market_value=0.001, net_metering_refund=0.0),
        unbooked_kwh=-0.0001,
        unbooked_credit=0.0001,
    )

    for value in breakdown.as_attributes().values():
        assert value == 0.0
        assert math.copysign(1.0, value) == 1.0


def test_booked_market_value_needs_known_split_and_priced_tail():
    """The booked market value is only reported when every part is known."""
    period = _period(market_value=-0.5)

    assert export_valuation.booked_market_value(period, 0.25) == pytest.approx(0.75)
    assert export_valuation.booked_market_value(period, None) is None
    unknown = _period(market_value=None)
    assert export_valuation.booked_market_value(unknown, 0.25) is None
