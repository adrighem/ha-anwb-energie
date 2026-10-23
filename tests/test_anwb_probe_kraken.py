"""Tests for the probe's Kraken daily statistics command."""

from __future__ import annotations

import argparse
import json
import stat
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from scripts import anwb_api_probe as probe
from scripts import anwb_probe_kraken as kraken

SENTINEL = "sentinel-upstream-detail-must-not-appear"
ACCOUNT = "A-SENTINEL-ACCOUNT-123"
ADDRESS = "Sentinelstraat 1, Utrecht"
PROPERTY_ID = "property-sentinel-987"


def _selects(query: str, field: str) -> bool:
    return f"                {field}\n" in query


def _statistic(
    label: str,
    cents: str,
    value: str | None = None,
    stat_type: str = "CONSUMPTION_COST",
) -> dict[str, Any]:
    return {
        "type": stat_type,
        "label": label,
        "value": value,
        "unit": "KWH",
        "costInclTax": {"estimatedAmount": cents},
    }


def _page(
    nodes: list[dict[str, Any]],
    cursor: str | None = None,
) -> dict[str, Any]:
    return {
        "data": {
            "account": {
                "number": ACCOUNT,
                "properties": [
                    {
                        "id": PROPERTY_ID,
                        "address": ADDRESS,
                        "measurements": {
                            "pageInfo": {
                                "hasNextPage": cursor is not None,
                                "endCursor": cursor,
                            },
                            "edges": [{"node": node} for node in nodes],
                        },
                    }
                ],
            }
        }
    }


def _node(start: str, value: str, statistics: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "value": value,
        "startAt": start,
        "meterId": "meter-sentinel",
        "metaData": {"statistics": statistics},
    }


def test_build_query_selects_only_known_optional_fields() -> None:
    query = kraken.build_query(("label", "unit"))

    assert _selects(query, "label")
    assert _selects(query, "unit")
    assert not _selects(query, "value")
    assert "__EXTRA_FIELDS__" not in kraken.build_query(())
    with pytest.raises(ValueError):
        kraken.build_query(("address",))


def test_field_fallback_records_only_rejected_field_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queries: list[str] = []

    def fake_request(_method: str, url: str, **kwargs: Any) -> probe.JsonResponse:
        assert url == probe.GRAPHQL_URL
        query = kwargs["json_body"]["query"]
        queries.append(query)
        if _selects(query, "label"):
            message = f'Cannot query field "label" on type "Stat". {SENTINEL}'
        elif _selects(query, "unit"):
            message = f"Unrelated failure {SENTINEL}"
        else:
            return probe.JsonResponse(200, _page([]), True)
        return probe.JsonResponse(200, {"errors": [{"message": message}]}, True)

    monkeypatch.setattr(probe, "_request_json", fake_request)

    query, first_page = kraken.negotiate_fields(
        "token", ACCOUNT, date(2026, 1, 1), date(2026, 1, 31)
    )

    assert query == kraken.StatisticsQuery(fields=("value",), rejected=("label", "unit"))
    assert len(queries) == 3
    assert first_page == _page([])
    assert SENTINEL not in repr(query)


def test_http_400_rejects_last_optional_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_request(_method: str, _url: str, **kwargs: Any) -> probe.JsonResponse:
        if _selects(kwargs["json_body"]["query"], "unit"):
            return probe.JsonResponse(400, {}, False)
        return probe.JsonResponse(200, _page([]), True)

    monkeypatch.setattr(probe, "_request_json", fake_request)

    query, _first_page = kraken.negotiate_fields(
        "token", ACCOUNT, date(2026, 1, 1), date(2026, 1, 31)
    )

    assert query == kraken.StatisticsQuery(fields=("label", "value"), rejected=("unit",))


def test_base_query_errors_fail_without_upstream_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        probe,
        "_request_json",
        lambda *_args, **_kwargs: probe.JsonResponse(
            200, {"errors": [{"message": SENTINEL}]}, True
        ),
    )

    with pytest.raises(probe.ProbeError) as error:
        kraken.negotiate_fields("token", ACCOUNT, date(2026, 1, 1), date(2026, 1, 2))

    assert str(error.value) == "Kraken statistics query returned GraphQL errors"


def test_http_errors_expose_only_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        probe,
        "_request_json",
        lambda *_args, **_kwargs: probe.JsonResponse(
            502, {"detail": SENTINEL}, True
        ),
    )

    with pytest.raises(probe.ProbeError) as error:
        kraken.negotiate_fields("token", ACCOUNT, date(2026, 1, 1), date(2026, 1, 2))

    assert str(error.value) == "Kraken statistics query failed with HTTP 502"


def test_fetch_direction_follows_cursors(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, Any]] = []
    pages = {
        None: _page([_node("2026-01-01T00:00:00+01:00", "1.5", [])], "c1"),
        "c1": _page([_node("2026-01-02T00:00:00+01:00", "2.5", [])]),
    }

    def fake_request(_method: str, _url: str, **kwargs: Any) -> probe.JsonResponse:
        variables = kwargs["json_body"]["variables"]
        seen.append(variables)
        return probe.JsonResponse(200, pages[variables["after"]], True)

    monkeypatch.setattr(probe, "_request_json", fake_request)

    entries = kraken.fetch_direction(
        "token",
        ACCOUNT,
        kraken.StatisticsQuery(fields=(), rejected=()),
        "GENERATION",
        date(2026, 1, 1),
        date(2026, 1, 2),
    )

    assert [entry["date"] for entry in entries] == ["2026-01-01", "2026-01-02"]
    assert seen[0]["startAt"] == "2026-01-01T00:00:00+01:00"
    assert seen[0]["endAt"] == "2026-01-03T00:00:00+01:00"
    assert seen[0]["timezone"] == "Europe/Amsterdam"
    assert seen[0]["direction"] == "GENERATION"


def test_unsafe_labels_are_redacted() -> None:
    page = _page(
        [
            _node(
                "2026-01-01T00:00:00+01:00",
                "1",
                [_statistic(f"{ADDRESS} <script>", "1")],
            )
        ]
    )

    entries, _cursor = kraken.parse_page(page, "CONSUMPTION")

    assert entries[0]["statistics"][0]["label"] == "<redacted>"
    assert ADDRESS not in json.dumps(entries)


def _entry(
    day: str,
    direction: str,
    kwh: float,
    statistics: list[dict[str, Any]],
) -> dict[str, Any]:
    return {"date": day, "direction": direction, "value": kwh, "statistics": statistics}


def _parsed(label: str, cents: float, value: float | None) -> dict[str, Any]:
    return {
        "type": "CONSUMPTION_COST",
        "label": label,
        "value": value,
        "unit": "KWH",
        "cost_incl_tax_estimated_amount": cents,
    }


def test_monthly_and_cumulative_netting_aggregates() -> None:
    entries = [
        _entry("2026-01-05", "CONSUMPTION", 60.0, [_parsed("ENERGY_TAX", 1000, 60)]),
        _entry("2026-01-06", "CONSUMPTION", 40.0, [_parsed("ENERGY_TAX", 500, 40)]),
        _entry("2026-01-06", "GENERATION", 20.0, [_parsed("GEN_TAX", -200, -20)]),
        _entry("2026-02-01", "CONSUMPTION", 50.0, []),
        _entry("2026-02-01", "GENERATION", 200.0, [_parsed("GEN_TAX", -1300, -130)]),
        _entry("2027-01-01", "CONSUMPTION", 10.0, []),
        _entry("2027-01-01", "GENERATION", 30.0, []),
    ]

    monthly = kraken.monthly_aggregates(entries)
    comparison = kraken.netting_comparison(monthly)

    assert monthly[0] == {
        "month": "2026-01",
        "direction": "CONSUMPTION",
        "days": 2,
        "kwh": 100.0,
        "eur": 15.0,
        "label_kwh": {"ENERGY_TAX": 100.0},
        "label_eur": {"ENERGY_TAX": 15.0},
    }
    assert [row["monthly_min_kwh"] for row in comparison] == [20.0, 50.0, 10.0]
    assert [row["cumulative_min_kwh"] for row in comparison] == [20.0, 150.0, 10.0]
    assert [row["cumulative_min_delta_kwh"] for row in comparison] == [
        20.0,
        130.0,
        10.0,
    ]
    assert comparison[1]["generation_label_kwh"] == {"GEN_TAX": -130.0}
    assert comparison[1]["cumulative_import_kwh"] == 150.0
    assert comparison[1]["cumulative_export_kwh"] == 220.0



def test_refund_basis_names_matching_netting_candidate() -> None:
    refund = kraken.REFUND_LABEL
    entries = [
        _entry("2026-01-05", "CONSUMPTION", 100.0, []),
        _entry("2026-01-05", "GENERATION", 2.0, [_parsed(refund, -1300, 100)]),
        _entry("2026-02-01", "CONSUMPTION", 50.0, []),
        _entry("2026-02-01", "GENERATION", 80.0, [_parsed(refund, -500, 30)]),
        _entry("2026-03-01", "CONSUMPTION", 10.0, []),
        _entry("2026-03-01", "GENERATION", 90.0, []),
    ]

    comparison = kraken.netting_comparison(kraken.monthly_aggregates(entries))

    assert [row["refund_kwh"] for row in comparison] == [100.0, 30.0, None]
    assert [row["refund_basis"] for row in comparison] == ["import", "other", None]

def test_label_falls_back_to_type_when_label_was_rejected() -> None:
    statistic = _parsed("x", 100, None) | {"label": None}

    monthly = kraken.monthly_aggregates(
        [_entry("2026-03-01", "CONSUMPTION", 1.0, [statistic])]
    )

    assert monthly[0]["label_eur"] == {"CONSUMPTION_COST": 1.0}
    assert monthly[0]["label_kwh"] == {}


def test_command_writes_sanitized_report_with_restrictive_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    probe_dir = tmp_path / "probe"
    report_file = probe_dir / "kraken-statistics.json"
    monkeypatch.setattr(probe, "PROBE_DIR", probe_dir)
    monkeypatch.setattr(probe, "KRAKEN_STATISTICS_FILE", report_file)
    monkeypatch.setattr(probe, "_get_oauth_token", lambda _callback: "oauth")
    monkeypatch.setattr(probe, "_get_kraken_token", lambda _token: "kraken-token")
    monkeypatch.setattr(probe, "_get_account_number", lambda _token: ACCOUNT)
    calls: list[str] = []

    def fake_request(_method: str, _url: str, **kwargs: Any) -> probe.JsonResponse:
        variables = kwargs["json_body"]["variables"]
        assert variables["accountNumber"] == ACCOUNT
        calls.append(variables["direction"])
        if variables["direction"] == "CONSUMPTION":
            node = _node(
                "2026-01-01T00:00:00+01:00",
                "5",
                [_statistic("SUPPLIER_DYNAMIC_COST", "123.5", "5")],
            )
        else:
            node = _node(
                "2026-01-01T00:00:00+01:00",
                "2",
                [
                    _statistic(
                        "GENERATION_ENERGY_TAX_STEP_1", "-20", "-2", "CONSUMPTION_COST"
                    )
                ],
            )
        return probe.JsonResponse(200, _page([node]), True)

    monkeypatch.setattr(probe, "_request_json", fake_request)
    args = probe.build_parser().parse_args(
        ["kraken-statistics", "--from", "2026-01-01", "--to", "2026-01-31"]
    )

    assert args.func(args) == 0

    assert calls == ["CONSUMPTION", "GENERATION"]
    assert stat.S_IMODE(report_file.stat().st_mode) == 0o600
    report_text = report_file.read_text(encoding="utf-8")
    captured = capsys.readouterr()
    for secret in (ACCOUNT, ADDRESS, PROPERTY_ID, "meter-sentinel", "kraken-token"):
        assert secret not in report_text
        assert secret not in captured.out
        assert secret not in captured.err
    report = json.loads(report_text)
    assert report["accepted_statistic_fields"] == ["label", "value", "unit"]
    assert report["days"][0] == {
        "date": "2026-01-01",
        "direction": "CONSUMPTION",
        "value": 5.0,
        "statistics": [
            {
                "type": "CONSUMPTION_COST",
                "label": "SUPPLIER_DYNAMIC_COST",
                "value": 5.0,
                "unit": "KWH",
                "cost_incl_tax_estimated_amount": 123.5,
            }
        ],
    }
    assert report["netting_comparison"][0]["monthly_min_kwh"] == 2.0
    assert "2026-01: import 5.000 kWh" in captured.out


def test_default_range_is_year_to_date(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(probe, "_local_today", lambda: date(2026, 10, 23))
    args = probe.build_parser().parse_args(["kraken-statistics"])

    assert kraken._range(args) == (date(2026, 1, 1), date(2026, 10, 23))


@pytest.mark.parametrize(
    ("start", "end"),
    [(date(2026, 2, 1), date(2026, 1, 1)), (date(2025, 1, 1), date(2026, 1, 2))],
)
def test_invalid_ranges_fail_before_any_request(start: date, end: date) -> None:
    args = argparse.Namespace(start=start, end=end)

    with pytest.raises(probe.ProbeError):
        kraken._range(args)


def test_invalid_date_argument_is_not_echoed(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit):
        probe.build_parser().parse_args(["kraken-statistics", "--from", SENTINEL])

    assert SENTINEL not in capsys.readouterr().err
