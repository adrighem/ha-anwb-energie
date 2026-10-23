"""Tests for the probe's current-month HOUR account-cache window."""

from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from scripts import anwb_api_probe as probe
from scripts import anwb_probe_current_month as current_month

_ROOT = Path(__file__).resolve().parents[1]
_INTEGRATION = _ROOT / "custom_components" / "anwb_energie_account"
_AMS = ZoneInfo("Europe/Amsterdam")
ACCOUNT_SENTINEL = "A-SENTINEL-ACCOUNT-123"


def _coordinator_boundaries() -> Any:
    """Load the coordinator's boundary helper without Home Assistant."""
    source = (_INTEGRATION / "coordinator.py").read_text(encoding="utf-8")
    wanted = {
        "_as_utc",
        "_format_account_cache_datetime",
        "_account_cache_query_boundaries",
    }
    nodes = [
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    assert {node.name for node in nodes} == wanted
    namespace: dict[str, Any] = {
        "datetime": datetime,
        "timedelta": timedelta,
        "timezone": timezone,
    }
    exec(compile(ast.Module(nodes, type_ignores=[]), "coordinator", "exec"), namespace)
    return namespace["_account_cache_query_boundaries"]


def _kraken_costs() -> Any:
    spec = importlib.util.spec_from_file_location(
        "kraken_costs_probe_parity", _INTEGRATION / "kraken_costs.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "now",
    [
        datetime(2026, 1, 1, 0, 30, tzinfo=_AMS),
        datetime(2026, 3, 29, 3, tzinfo=_AMS),
        datetime(2026, 7, 24, 12, tzinfo=_AMS),
        datetime(2026, 10, 25, 2, 30, tzinfo=_AMS),
        datetime(2026, 12, 31, 23, 59, tzinfo=_AMS),
        datetime(2026, 12, 24, 12, tzinfo=timezone.utc),
    ],
)
def test_boundaries_match_the_integration(now: datetime) -> None:
    expected = _coordinator_boundaries()(now)

    assert current_month.account_cache_query_boundaries(now) == expected


def test_current_month_request_matches_integration_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 10, 23, 15, tzinfo=_AMS)
    urls: list[str] = []

    def fake_request(method: str, url: str, **_kwargs: object) -> probe.JsonResponse:
        assert method == "GET"
        urls.append(url)
        return probe.JsonResponse(status=200, payload={}, is_json_object=True)

    monkeypatch.setattr(probe, "_request_json", fake_request)
    window = probe._current_month_window(now)
    probe._fetch_cache("acct", "token", "production", "HOUR", window)

    assert window.name == "current_month"
    assert window.intervals == ("HOUR",)
    assert (window.start.isoformat(), window.end.isoformat()) == (
        "2026-10-01",
        "2026-10-31",
    )
    assert urls == [
        f"{probe.ACCOUNT_CACHE_BASE}/acct/production/cache"
        "?startDate=2026-09-30T22:00:00.000Z"
        "&endDate=2026-10-31T22:59:59.999Z"
        "&contractStartDate=2025-12-31T23:00:00.000Z&interval=HOUR"
    ]


def test_probe_windows_include_current_month() -> None:
    names = [window.name for window in probe._probe_windows(False)]

    assert names[-1] == "current_month"


def _row(
    start: str,
    *,
    usage: float = 0.0,
    gap: bool = False,
    fixed: object = 0.1,
) -> dict[str, Any]:
    return {
        "startDate": start,
        "usage": usage,
        "hasGap": gap,
        "vasteKosten": {"total": fixed},
        "meterId": "must-not-appear",
    }


def test_summary_reports_future_rows_gaps_and_fixed_costs() -> None:
    rows = [
        _row("2026-10-01T00:00:00.000Z", usage=1.0),
        _row("2026-10-01T01:00:00.000Z", usage=0.5, gap=True),
        _row("2026-10-01T02:00:00.000Z", gap=True, fixed=0.2),
        _row("2026-10-01T04:00:00.000Z", gap=True),
        _row("2026-10-01T05:00:00.000Z", fixed="0.3"),
        _row("2026-10-01T03:00:00.000Z", gap=True),
    ]

    summary = current_month.summarize_current_month(
        rows,
        now=datetime(2026, 10, 1, 3, 30, tzinfo=timezone.utc),
        start="2026-09-30T22:00:00.000Z",
        end="2026-10-31T22:59:59.999Z",
    )

    assert summary["expected_hour_count"] == 745
    assert summary["row_count"] == 6
    assert summary["rows_after_now"] == 2
    assert summary["rows_after_now_has_gap_true"] == 1
    assert summary["rows_after_now_vaste_total_sum"] == pytest.approx(0.1)
    assert summary["first_start_date"] == "2026-10-01T00:00:00.000Z"
    assert summary["last_start_date"] == "2026-10-01T05:00:00.000Z"
    assert summary["last_start_date_with_usage"] == "2026-10-01T01:00:00.000Z"
    assert summary["gap_rows_with_usage"] == 1
    assert summary["gap_rows_without_usage"] == 3
    assert summary["gap_ranges"] == [
        ["2026-10-01T01:00:00.000Z", "2026-10-01T04:00:00.000Z"]
    ]
    assert summary["rows_with_invalid_vaste_total"] == 1
    assert summary["vaste_total_sum_all_rows"] == pytest.approx(0.6)
    assert summary["vaste_total_sum_has_gap_false"] == pytest.approx(0.1)
    assert summary["sum_account_cache_fixed_costs"] is None
    assert "must-not-appear" not in json.dumps(summary)


def test_gap_ranges_split_on_missing_hours_and_are_capped() -> None:
    rows = [
        _row(f"2026-10-01T{hour:02d}:00:00.000Z", gap=hour % 2 == 0)
        for hour in range(24)
    ] + [
        _row(f"2026-10-02T{hour:02d}:00:00.000Z", gap=hour % 2 == 0)
        for hour in range(24)
    ]

    summary = current_month.summarize_current_month(
        rows,
        now=datetime(2026, 10, 1, tzinfo=timezone.utc),
        start="2026-10-01T00:00:00.000Z",
        end="2026-10-03T00:00:00.000Z",
    )

    assert summary["gap_range_count"] == 24
    assert len(summary["gap_ranges"]) == current_month.MAX_GAP_RANGES
    assert summary["gap_ranges_truncated"] is True


_FIXED_COST_CASES: list[tuple[list[dict[str, Any]], float | None]] = [
    ([_row("a", fixed=0.5), _row("b", fixed=0.25), _row("c", fixed=-0.1)], 0.65),
    ([_row("a", fixed=0.5), _row("b", fixed=0.5, gap=True)], None),
    ([_row("a", fixed=0.5), {"hasGap": False}], None),
    ([_row("a", fixed=0.5), {"hasGap": False, "vasteKosten": None}], None),
    ([_row("a", fixed=0.5), {"hasGap": False, "vasteKosten": {}}], None),
    ([_row("a", fixed=0.5), _row("b", fixed="0.5")], None),
    ([_row("a", fixed=0.5), _row("b", fixed=True)], None),
    ([_row("a", fixed=0.5), _row("b", fixed=float("nan"))], None),
    ([], None),
]


@pytest.mark.parametrize(("rows", "expected"), _FIXED_COST_CASES)
def test_fixed_cost_helper_matches_integration_cases(
    rows: list[dict[str, Any]],
    expected: float | None,
) -> None:
    result = current_month.sum_account_cache_fixed_costs(rows)

    if expected is None:
        assert result is None
    else:
        assert result == pytest.approx(expected)


@pytest.mark.parametrize(("rows", "_expected"), _FIXED_COST_CASES)
def test_fixed_cost_helper_parity_with_integration(
    rows: list[dict[str, Any]],
    _expected: float | None,
) -> None:
    helper = getattr(_kraken_costs(), "sum_account_cache_fixed_costs", None)
    if helper is None:
        pytest.skip("integration helper is not on this branch yet")

    assert current_month.sum_account_cache_fixed_costs(rows) == helper(rows)


def test_probe_report_includes_current_month_without_identifiers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe_dir = tmp_path / "probe"
    report_file = probe_dir / "last-report.json"
    now = datetime(2026, 10, 1, 1, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(probe, "PROBE_DIR", probe_dir)
    monkeypatch.setattr(probe, "REPORT_FILE", report_file)
    monkeypatch.setattr(probe, "_get_oauth_token", lambda _callback: "oauth")
    monkeypatch.setattr(probe, "_get_kraken_token", lambda _token: "kraken")
    monkeypatch.setattr(probe, "_get_account_number", lambda _token: ACCOUNT_SENTINEL)
    monkeypatch.setattr(
        probe,
        "_probe_windows",
        lambda _previous_year: [probe._current_month_window(now)],
    )

    def no_tariffs(*_args: object) -> dict[str, float]:
        raise AssertionError("current month must not fetch tariffs")

    def fake_fetch(
        account_number: str,
        _token: str,
        endpoint: str,
        _interval: str,
        _window: probe.ProbeWindow,
    ) -> probe.JsonResponse:
        assert account_number == ACCOUNT_SENTINEL
        if endpoint == "gas":
            return probe.JsonResponse(status=404, payload={}, is_json_object=False)
        rows = [
            _row("2026-10-01T00:00:00.000Z", usage=1.0),
            _row("2026-10-01T02:00:00.000Z", gap=True),
        ]
        return probe.JsonResponse(
            status=200,
            payload={"data": rows, "accountNumber": ACCOUNT_SENTINEL},
            is_json_object=True,
        )

    monkeypatch.setattr(probe, "_fetch_tariff_map", no_tariffs)
    monkeypatch.setattr(probe, "_fetch_cache", fake_fetch)
    args = argparse.Namespace(
        prompt_for_callback=False,
        previous_year=False,
        tariff_check=True,
    )

    assert probe.command_probe(args) == 0

    report_text = report_file.read_text(encoding="utf-8")
    assert ACCOUNT_SENTINEL not in report_text
    assert "must-not-appear" not in report_text
    results = json.loads(report_text)["results"]
    assert [item["outcome"] for item in results] == ["ok", "ok", "http_error"]
    details = results[0]["current_month"]
    assert details["rows_after_now"] == 1
    assert details["sum_account_cache_fixed_costs"] is None
    assert details["vaste_total_sum_has_gap_false"] == pytest.approx(0.1)
