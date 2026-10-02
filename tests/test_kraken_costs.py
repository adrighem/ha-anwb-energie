"""Test parsing and combining Kraken daily cost statistics."""

import importlib.util
import sys
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

_MODULE_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "anwb_energie_account"
    / "kraken_costs.py"
)
_SPEC = importlib.util.spec_from_file_location("kraken_costs_under_test", _MODULE_PATH)
kraken_costs = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = kraken_costs
_SPEC.loader.exec_module(kraken_costs)

AMSTERDAM = ZoneInfo("Europe/Amsterdam")
KrakenCostError = kraken_costs.KrakenCostError
KrakenDailyCost = kraken_costs.KrakenDailyCost


def _node(start_at, value, statistics):
    return {
        "node": {
            "value": value,
            "startAt": start_at,
            "metaData": {"statistics": statistics},
        }
    }


def _stat(kind, cents):
    return {"type": kind, "costInclTax": {"estimatedAmount": cents}}


def _page(edges, *, has_next=False, cursor=None, properties=1):
    measurements = {
        "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
        "edges": edges,
    }
    return {
        "data": {
            "account": {
                "properties": [{"measurements": measurements}] * properties,
            }
        }
    }


def _day(day, usage, variable=0.0, standing=0.0, has_statistics=True):
    return KrakenDailyCost(day, usage, variable, standing, has_statistics)


def test_parse_page_sums_cost_types_in_euro():
    """Variable and standing lines are summed separately and converted to euro."""
    payload = _page(
        [
            _node(
                "2026-09-15T00:00:00+02:00",
                "1.985000000000000000",
                [
                    _stat("CONSUMPTION_COST", "50.49211"),
                    _stat("CONSUMPTION_COST", "22.00335"),
                    _stat("CONSUMPTION_COST", "3.57299"),
                    _stat("STANDING_CHARGE_COST", "27.421"),
                    _stat("STANDING_CHARGE_COST", "130.6195"),
                    _stat("STANDING_CHARGE_COST", "-172.31726"),
                ],
            )
        ]
    )

    days, cursor = kraken_costs.parse_daily_costs_page(payload, AMSTERDAM)

    assert cursor is None
    assert len(days) == 1
    assert days[0].day == date(2026, 9, 15)
    assert days[0].usage == pytest.approx(1.985)
    assert days[0].variable_cost == pytest.approx(0.7606845)
    assert days[0].standing_charge == pytest.approx(-0.1427676)
    assert days[0].has_statistics is True


def test_parse_page_uses_local_date_and_cursor():
    """The local date comes from startAt in the configured timezone."""
    payload = _page(
        [_node("2026-03-28T23:00:00+00:00", "2", [])],
        has_next=True,
        cursor="abc",
    )

    days, cursor = kraken_costs.parse_daily_costs_page(payload, AMSTERDAM)

    assert days[0].day == date(2026, 3, 29)
    assert days[0].has_statistics is False
    assert cursor == "abc"


def test_parse_page_sums_multiple_properties_per_day():
    """Accounts with more than one property get one combined day."""
    payload = _page(
        [
            _node(
                "2026-09-15T00:00:00+02:00",
                "1.5",
                [_stat("CONSUMPTION_COST", "100")],
            )
        ],
        properties=2,
    )

    days, _ = kraken_costs.parse_daily_costs_page(payload, AMSTERDAM)

    assert len(days) == 1
    assert days[0].usage == pytest.approx(3.0)
    assert days[0].variable_cost == pytest.approx(2.0)


@pytest.mark.parametrize(
    "payload",
    [
        {"errors": [{"message": "Unauthorized"}]},
        {"data": {"account": None}},
        {"data": {"account": {"properties": []}}},
        {"data": {"account": {"properties": [{"measurements": None}]}}},
        _page([_node("2026-09-15T00:00:00+02:00", "x", [])]),
        _page(
            [
                _node(
                    "2026-09-15T00:00:00+02:00",
                    "1",
                    [{"type": "CONSUMPTION_COST", "costInclTax": None}],
                )
            ]
        ),
        _page([], has_next=True, cursor=None),
        "not a mapping",
    ],
)
def test_parse_page_rejects_malformed_responses(payload):
    """Malformed or failed responses raise instead of returning zero costs."""
    with pytest.raises(KrakenCostError):
        kraken_costs.parse_daily_costs_page(payload, AMSTERDAM)


def test_covered_through_stops_at_gap_and_unpriced_usage():
    """Coverage ends before a missing day or a used day without statistics."""
    start = date(2026, 1, 1)
    days = [
        _day(date(2026, 1, 1), 1.0, 0.3),
        _day(date(2026, 1, 2), 0.0, has_statistics=False),
        _day(date(2026, 1, 3), 2.0, 0.6),
        _day(date(2026, 1, 5), 1.0, 0.3),
    ]
    assert kraken_costs.covered_through(days, start) == date(2026, 1, 3)

    unpriced = [*days[:2], _day(date(2026, 1, 3), 2.0, has_statistics=False)]
    assert kraken_costs.covered_through(unpriced, start) == date(2026, 1, 2)
    assert kraken_costs.covered_through([], start) is None


def test_build_costs_splits_month_and_year():
    """Month and year totals come from the covered days only."""
    import_days = [
        _day(date(2026, 1, 31), 10.0, 3.0, -1.0),
        _day(date(2026, 2, 1), 2.0, 0.5, -1.0),
        _day(date(2026, 2, 2), 3.0, 0.7, -1.0),
    ]
    export_days = [
        _day(date(2026, 1, 31), 4.0, -1.2),
        _day(date(2026, 2, 1), 6.0, -0.9),
        _day(date(2026, 2, 2), 1.0, -0.2),
    ]

    costs = kraken_costs.build_electricity_costs(
        import_days,
        export_days,
        year_start=date(2026, 1, 1),
        current_month_start=date(2026, 2, 1),
    )

    assert costs.covered_through == date(2026, 2, 2)
    assert costs.month_import.usage == pytest.approx(5.0)
    assert costs.month_import.variable_cost == pytest.approx(1.2)
    assert costs.month_import.standing_charge == pytest.approx(-2.0)
    assert costs.month_import.days == 2
    assert costs.month_export.variable_cost == pytest.approx(-1.1)
    assert costs.year_import.variable_cost == pytest.approx(4.2)
    assert costs.year_export.variable_cost == pytest.approx(-2.3)
    assert costs.closed_month_import_usage == {date(2026, 1, 1): 10.0}
    assert costs.closed_month_export_usage == {date(2026, 1, 1): 4.0}


def test_build_costs_requires_every_closed_day():
    """A missing closed day cannot be filled from hourly data, so reject it."""
    import_days = [_day(date(2026, 1, 30), 1.0, 0.3)]
    export_days = [_day(date(2026, 1, 30), 1.0, -0.1)]

    assert (
        kraken_costs.build_electricity_costs(
            import_days,
            export_days,
            year_start=date(2026, 1, 1),
            current_month_start=date(2026, 2, 1),
        )
        is None
    )


def test_build_costs_allows_contract_start_during_year():
    """Coverage starts at the first returned day for mid-year contracts."""
    import_days = [
        _day(date(2026, 3, 30), 1.0, 0.3),
        _day(date(2026, 3, 31), 1.0, 0.3),
    ]
    export_days = [
        _day(date(2026, 3, 30), 0.0),
        _day(date(2026, 3, 31), 0.0),
    ]

    costs = kraken_costs.build_electricity_costs(
        import_days,
        export_days,
        year_start=date(2026, 1, 1),
        current_month_start=date(2026, 4, 1),
    )

    assert costs.covered_through == date(2026, 3, 31)
    assert costs.year_import.variable_cost == pytest.approx(0.6)


def test_build_costs_without_generation_days():
    """Accounts without solar may return no generation days at all."""
    costs = kraken_costs.build_electricity_costs(
        [_day(date(2026, 1, 1), 1.0, 0.3)],
        [],
        year_start=date(2026, 1, 1),
        current_month_start=date(2026, 1, 1),
    )

    assert costs.covered_through == date(2026, 1, 1)
    assert costs.month_export.usage == 0.0
    assert costs.month_export.variable_cost == 0.0


def test_build_costs_on_new_years_day_without_data():
    """Before the first day is available, everything comes from the tail."""
    costs = kraken_costs.build_electricity_costs(
        [],
        [],
        year_start=date(2026, 1, 1),
        current_month_start=date(2026, 1, 1),
    )

    assert costs.covered_through is None
    assert costs.year_import.days == 0
    assert costs.month_export.variable_cost == 0.0
