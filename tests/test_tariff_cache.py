"""Tests for the shared ANWB tariff cache."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo

import pytest

MODULE_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "anwb_energie_account"
    / "tariff_cache.py"
)
SPEC = importlib.util.spec_from_file_location("anwb_tariff_cache_tests", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
tariff_cache = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = tariff_cache
SPEC.loader.exec_module(tariff_cache)

HourlyTariffData = tariff_cache.HourlyTariffData
TariffCache = tariff_cache.TariffCache

AMSTERDAM = ZoneInfo("Europe/Amsterdam")


@dataclass
class MutableClock:
    """Controllable aware clock."""

    value: datetime

    def __call__(self) -> datetime:
        """Return the current test time."""
        return self.value

    def advance(self, delta: timedelta) -> None:
        """Advance the current test time."""
        self.value += delta


class FakeStore:
    """Deterministic Store subset with an explicitly flushed delayed save."""

    def __init__(self, payload: Any = None) -> None:
        self.payload = deepcopy(payload)
        self.load_calls = 0
        self.save_requests = 0
        self.last_delay: float | None = None
        self._pending_save = None

    async def async_load(self) -> dict[str, Any] | None:
        """Return detached persisted data."""
        self.load_calls += 1
        return deepcopy(self.payload)

    def async_delay_save(self, data_func, delay: float) -> None:
        """Record a coalesced delayed save."""
        self.save_requests += 1
        self.last_delay = delay
        self._pending_save = data_func

    def flush(self) -> dict[str, Any]:
        """Run the delayed serializer and persist its detached result."""
        assert self._pending_save is not None
        data_func = self._pending_save
        self._pending_save = None
        self.payload = deepcopy(data_func())
        return deepcopy(self.payload)


def _hour_keys(local_day: date, zone: ZoneInfo = AMSTERDAM) -> list[str]:
    """Return the UTC-hour keys belonging to a local calendar day."""
    start = datetime.combine(local_day, time.min, zone).astimezone(timezone.utc)
    end = datetime.combine(
        local_day + timedelta(days=1),
        time.min,
        zone,
    ).astimezone(timezone.utc)
    keys = []
    while start < end:
        keys.append(start.strftime("%Y-%m-%dT%H:00:00.000Z"))
        start += timedelta(hours=1)
    return keys


def _hourly_data(
    local_day: date,
    *,
    zone: ZoneInfo = AMSTERDAM,
    market_rows: int | None = None,
) -> HourlyTariffData:
    """Build a complete all-in local day and optional market rows."""
    keys = _hour_keys(local_day, zone)
    all_in = {key: float(index + 1) for index, key in enumerate(keys)}
    market_count = len(keys) if market_rows is None else market_rows
    market = {key: float(-(index + 1)) for index, key in enumerate(keys[:market_count])}
    return HourlyTariffData(all_in, market)


def _empty_payload(timezone_name: str = "Europe/Amsterdam") -> dict[str, Any]:
    """Return the complete empty JSON schema."""
    return {
        "schema_version": 1,
        "timezone": timezone_name,
        "commodities": {
            "electricity": {"HOUR": {}, "DAY": {}},
            "gas": {"HOUR": {}, "DAY": {}},
        },
    }


def _stored_hourly_bucket(
    payload: dict[str, Any],
    commodity: str,
    local_day: date,
    data: HourlyTariffData,
) -> None:
    """Insert a serialized hourly bucket into a test payload."""
    payload["commodities"][commodity]["HOUR"][local_day.isoformat()] = {
        "all_in": dict(data.all_in_prices),
        "market": dict(data.market_prices),
    }


@pytest.mark.asyncio
async def test_closed_hourly_day_survives_warm_restart_with_partial_market():
    """A complete all-in day persists without requiring every market row."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    store = FakeStore()
    local_day = date(2026, 1, 15)
    source = _hourly_data(local_day, market_rows=1)
    fetch_calls = 0

    async def fetch() -> HourlyTariffData:
        nonlocal fetch_calls
        fetch_calls += 1
        return source

    cold_cache = TariffCache(store, "Europe/Amsterdam", clock=clock)
    cold_result = await cold_cache.async_get_hourly_day(
        "electricity",
        local_day,
        fetch,
    )

    assert fetch_calls == 1
    assert len(cold_result.all_in_prices) == 24
    assert len(cold_result.market_prices) == 1
    assert store.save_requests == 1
    payload = store.flush()
    assert payload["commodities"]["electricity"]["HOUR"][local_day.isoformat()][
        "market"
    ] == dict(source.market_prices)
    serialized = json.dumps(payload)
    assert "account" not in serialized.lower()
    assert "token" not in serialized.lower()
    assert "authorization" not in serialized.lower()

    async def must_not_fetch() -> HourlyTariffData:
        raise AssertionError("warm cache unexpectedly fetched tariffs")

    warm_cache = TariffCache(store, "Europe/Amsterdam", clock=clock)
    warm_result = await warm_cache.async_get_hourly_day(
        "electricity",
        local_day,
        must_not_fetch,
    )

    assert store.load_calls == 2
    assert warm_result == cold_result


@pytest.mark.asyncio
async def test_open_hourly_day_uses_memory_ttl_and_is_not_persisted():
    """Current/future local days remain memory-only and expire predictably."""
    clock = MutableClock(datetime(2026, 4, 20, 10, tzinfo=timezone.utc))
    store = FakeStore()
    local_day = date(2026, 4, 20)
    source = _hourly_data(local_day)
    fetch_calls = 0

    async def fetch() -> HourlyTariffData:
        nonlocal fetch_calls
        fetch_calls += 1
        return source

    cache = TariffCache(
        store,
        "Europe/Amsterdam",
        clock=clock,
        memory_ttl=timedelta(minutes=30),
    )
    await cache.async_get_hourly_day("electricity", local_day, fetch)
    await cache.async_get_hourly_day("electricity", local_day, fetch)
    assert fetch_calls == 1
    assert store.save_requests == 0

    clock.advance(timedelta(minutes=31))
    await cache.async_get_hourly_day("electricity", local_day, fetch)
    assert fetch_calls == 2
    assert store.save_requests == 0

    restarted = TariffCache(store, "Europe/Amsterdam", clock=clock)
    await restarted.async_get_hourly_day("electricity", local_day, fetch)
    assert fetch_calls == 3


@pytest.mark.asyncio
async def test_complete_open_day_is_promoted_after_midnight_without_refetch():
    """A complete live bucket becomes persistent as soon as its day closes."""
    clock = MutableClock(datetime(2026, 4, 20, 20, tzinfo=timezone.utc))
    store = FakeStore()
    local_day = date(2026, 4, 20)
    source = _hourly_data(local_day)
    fetch_calls = 0

    async def fetch() -> HourlyTariffData:
        nonlocal fetch_calls
        fetch_calls += 1
        return source

    cache = TariffCache(store, "Europe/Amsterdam", clock=clock)
    await cache.async_get_hourly_day("electricity", local_day, fetch)
    assert fetch_calls == 1
    assert store.save_requests == 0

    clock.advance(timedelta(hours=3))

    async def must_not_fetch() -> HourlyTariffData:
        raise AssertionError("a complete newly closed day was refetched")

    promoted = await cache.async_get_hourly_day(
        "electricity",
        local_day,
        must_not_fetch,
    )
    assert promoted == source
    assert store.save_requests == 1
    store.flush()

    restarted = TariffCache(store, "Europe/Amsterdam", clock=clock)
    assert (
        await restarted.async_get_hourly_day(
            "electricity",
            local_day,
            must_not_fetch,
        )
        == source
    )


@pytest.mark.asyncio
async def test_memory_only_mode_uses_the_same_cache_api():
    """Coordinator unit tests can use the cache without Home Assistant Store."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    local_day = date(2026, 2, 1)
    fetch_calls = 0

    async def fetch() -> HourlyTariffData:
        nonlocal fetch_calls
        fetch_calls += 1
        return _hourly_data(local_day)

    cache = TariffCache(None, "Europe/Amsterdam", clock=clock)
    await cache.async_get_hourly_day("electricity", local_day, fetch)
    await cache.async_get_hourly_day("electricity", local_day, fetch)

    assert fetch_calls == 1


@pytest.mark.asyncio
async def test_identical_hourly_requests_are_single_flight_and_shielded():
    """Cancelling one waiter does not cancel the shared network operation."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    local_day = date(2026, 2, 1)
    started = asyncio.Event()
    release = asyncio.Event()
    fetch_calls = 0

    async def fetch() -> HourlyTariffData:
        nonlocal fetch_calls
        fetch_calls += 1
        started.set()
        await release.wait()
        return _hourly_data(local_day)

    cache = TariffCache(None, "Europe/Amsterdam", clock=clock)
    cancelled_waiter = asyncio.create_task(
        cache.async_get_hourly_day("electricity", local_day, fetch)
    )
    surviving_waiter = asyncio.create_task(
        cache.async_get_hourly_day("electricity", local_day, fetch)
    )
    await asyncio.wait_for(started.wait(), timeout=1)

    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter

    release.set()
    result = await asyncio.wait_for(surviving_waiter, timeout=1)

    assert fetch_calls == 1
    assert len(result.all_in_prices) == 24


@pytest.mark.asyncio
async def test_failed_single_flight_is_removed_and_retried():
    """A failed shared request does not poison its request key."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    local_day = date(2026, 2, 1)
    attempts = 0

    async def fetch() -> HourlyTariffData:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary tariff failure")
        return _hourly_data(local_day)

    cache = TariffCache(None, "Europe/Amsterdam", clock=clock)
    with pytest.raises(RuntimeError, match="temporary tariff failure"):
        await cache.async_get_hourly_day("electricity", local_day, fetch)

    result = await cache.async_get_hourly_day("electricity", local_day, fetch)
    assert attempts == 2
    assert len(result.all_in_prices) == 24


@pytest.mark.asyncio
async def test_fetch_concurrency_is_bounded_without_serializing_every_request():
    """Different request keys share the configurable network semaphore."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    cache = TariffCache(
        None,
        "Europe/Amsterdam",
        clock=clock,
        max_concurrency=2,
    )
    active = 0
    maximum_active = 0
    started_count = 0
    reached_limit = asyncio.Event()
    release = asyncio.Event()

    def fetch_for(local_day: date):
        async def fetch() -> HourlyTariffData:
            nonlocal active, maximum_active, started_count
            active += 1
            started_count += 1
            maximum_active = max(maximum_active, active)
            if active == 2:
                reached_limit.set()
            await release.wait()
            active -= 1
            return _hourly_data(local_day)

        return fetch

    days = [date(2026, 4, 20) + timedelta(days=index) for index in range(5)]
    tasks = [
        asyncio.create_task(
            cache.async_get_hourly_day("electricity", local_day, fetch_for(local_day))
        )
        for local_day in days
    ]

    await asyncio.wait_for(reached_limit.wait(), timeout=1)
    assert started_count == 2
    release.set()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)

    assert started_count == 5
    assert maximum_active == 2


@pytest.mark.asyncio
async def test_daily_partial_results_persist_points_and_retry_only_missing_dates():
    """Each valid DAY point is retained while holes remain immediately retryable."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    store = FakeStore()
    cache = TariffCache(store, "Europe/Amsterdam", clock=clock)
    required = {date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)}
    requested: list[frozenset[date]] = []
    first_response = {
        "2026-01-01": 0,
        "2026-01-03": -3.5,
        "2026-01-04": 4.5,
        "2026-01-05": float("nan"),
        "not-a-date": 99.0,
    }

    async def fetch(missing: frozenset[date]):
        requested.append(missing)
        if len(requested) == 1:
            return first_response
        assert missing == frozenset({date(2026, 1, 2)})
        return {"2026-01-02": 2.5}

    first = await cache.async_get_daily_prices("electricity", required, fetch)
    assert dict(first) == {
        "2026-01-01": 0.0,
        "2026-01-03": -3.5,
    }
    assert requested == [frozenset(required)]

    first_response["2026-01-01"] = 1234.0
    second = await cache.async_get_daily_prices("electricity", required, fetch)
    assert dict(second) == {
        "2026-01-01": 0.0,
        "2026-01-02": 2.5,
        "2026-01-03": -3.5,
    }
    assert store.save_requests == 1

    payload = store.flush()
    persisted = payload["commodities"]["electricity"]["DAY"]
    assert persisted == {
        "2026-01-01": 0.0,
        "2026-01-02": 2.5,
        "2026-01-03": -3.5,
        "2026-01-04": 4.5,
    }

    async def must_not_fetch(_missing: frozenset[date]):
        raise AssertionError("complete warm DAY cache unexpectedly fetched")

    restarted = TariffCache(store, "Europe/Amsterdam", clock=clock)
    warm = await restarted.async_get_daily_prices(
        "electricity",
        required | {date(2026, 1, 4)},
        must_not_fetch,
    )
    assert dict(warm) == persisted


@pytest.mark.asyncio
async def test_cached_daily_snapshot_survives_a_failed_missing_date_fill():
    """Callers can retain valid DAY points when fetching another point fails."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    cache = TariffCache(None, "Europe/Amsterdam", clock=clock)
    required = {date(2026, 1, 1), date(2026, 1, 2)}

    async def partial_fetch(_missing: frozenset[date]):
        return {"2026-01-01": 10.0}

    assert dict(
        await cache.async_get_daily_prices(
            "electricity",
            required,
            partial_fetch,
        )
    ) == {"2026-01-01": 10.0}

    async def failed_fetch(_missing: frozenset[date]):
        raise RuntimeError("temporary failure")

    with pytest.raises(RuntimeError, match="temporary failure"):
        await cache.async_get_daily_prices(
            "electricity",
            required,
            failed_fetch,
        )

    assert dict(
        await cache.async_get_cached_daily_prices(
            "electricity",
            required,
        )
    ) == {"2026-01-01": 10.0}


@pytest.mark.asyncio
async def test_identical_daily_requests_are_single_flight():
    """Concurrent callers share the same missing-date DAY request."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    cache = TariffCache(None, "Europe/Amsterdam", clock=clock)
    required = {date(2026, 1, 1), date(2026, 1, 2)}
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def fetch(missing: frozenset[date]):
        nonlocal calls
        calls += 1
        assert missing == frozenset(required)
        started.set()
        await release.wait()
        return {value.isoformat(): 10.0 for value in missing}

    first = asyncio.create_task(cache.async_get_daily_prices("gas", required, fetch))
    second = asyncio.create_task(cache.async_get_daily_prices("gas", required, fetch))
    await asyncio.wait_for(started.wait(), timeout=1)
    release.set()
    results = await asyncio.wait_for(asyncio.gather(first, second), timeout=1)

    assert calls == 1
    assert dict(results[0]) == dict(results[1])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("local_day", "expected_hours"),
    [
        (date(2026, 3, 29), 23),
        (date(2026, 10, 25), 25),
    ],
)
async def test_closed_dst_days_persist_only_with_real_local_hour_count(
    local_day: date,
    expected_hours: int,
):
    """Spring and autumn days use local boundaries rather than a fixed 24."""
    clock = MutableClock(datetime(2026, 12, 1, 12, tzinfo=timezone.utc))
    store = FakeStore()
    source = _hourly_data(local_day, market_rows=0)
    assert len(source.all_in_prices) == expected_hours

    async def fetch() -> HourlyTariffData:
        return source

    cache = TariffCache(store, "Europe/Amsterdam", clock=clock)
    await cache.async_get_hourly_day("electricity", local_day, fetch)
    payload = store.flush()
    bucket = payload["commodities"]["electricity"]["HOUR"][local_day.isoformat()]
    assert len(bucket["all_in"]) == expected_hours

    async def must_not_fetch() -> HourlyTariffData:
        raise AssertionError("complete DST bucket unexpectedly fetched")

    restarted = TariffCache(store, "Europe/Amsterdam", clock=clock)
    result = await restarted.async_get_hourly_day(
        "electricity",
        local_day,
        must_not_fetch,
    )
    assert len(result.all_in_prices) == expected_hours


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        [],
        {
            "schema_version": 99,
            "timezone": "Europe/Amsterdam",
            "commodities": {},
        },
        {
            "schema_version": 1,
            "timezone": "UTC",
            "commodities": {
                "electricity": {"HOUR": {}, "DAY": {"2026-01-01": 1.0}},
                "gas": {"HOUR": {}, "DAY": {}},
            },
        },
        {
            "schema_version": 1,
            "timezone": "Europe/Amsterdam",
            "commodities": {
                "electricity": {
                    "HOUR": {
                        "2026-01-01": {
                            "all_in": {"bad timestamp": "bad price"},
                            "market": {},
                        }
                    },
                    "DAY": {"invalid": float("inf")},
                },
                "gas": {"HOUR": {}, "DAY": {}},
            },
        },
    ],
    ids=["wrong-top-level", "schema-version", "timezone", "malformed-entry"],
)
async def test_invalid_or_timezone_mismatched_payload_refetches(payload):
    """Disposable invalid storage never blocks a fresh valid fetch."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    store = FakeStore(payload)
    local_day = date(2026, 1, 1)
    fetch_calls = 0

    async def fetch() -> HourlyTariffData:
        nonlocal fetch_calls
        fetch_calls += 1
        return _hourly_data(local_day)

    cache = TariffCache(store, "Europe/Amsterdam", clock=clock)
    result = await cache.async_get_hourly_day(
        "electricity",
        local_day,
        fetch,
    )

    assert fetch_calls == 1
    assert len(result.all_in_prices) == 24
    rewritten = store.flush()
    assert rewritten["schema_version"] == 1
    assert rewritten["timezone"] == "Europe/Amsterdam"
    assert local_day.isoformat() in rewritten["commodities"]["electricity"]["HOUR"]


@pytest.mark.asyncio
async def test_finite_validation_preserves_zero_negative_and_drops_bad_values():
    """Validation distinguishes legitimate non-positive prices from bad data."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    store = FakeStore()
    local_day = date(2026, 1, 10)
    keys = _hour_keys(local_day)
    all_in = {key: 10.0 for key in keys}
    all_in[keys[0]] = 0
    all_in[keys[1]] = -5
    all_in["not-a-timestamp"] = float("inf")
    market = {
        keys[0]: 0,
        keys[1]: -10,
        keys[2]: float("nan"),
        keys[3]: True,
    }

    async def fetch() -> HourlyTariffData:
        return HourlyTariffData(all_in, market)

    cache = TariffCache(store, "Europe/Amsterdam", clock=clock)
    result = await cache.async_get_hourly_day("electricity", local_day, fetch)

    assert result.all_in_prices[keys[0]] == 0.0
    assert result.all_in_prices[keys[1]] == -5.0
    assert result.market_prices == {
        keys[0]: 0.0,
        keys[1]: -10.0,
    }
    payload = store.flush()
    assert (
        payload["commodities"]["electricity"]["HOUR"][local_day.isoformat()]["all_in"][
            keys[0]
        ]
        == 0.0
    )


@pytest.mark.asyncio
async def test_returned_hourly_data_is_detached_and_immutable():
    """Fetched and returned mappings cannot mutate internal cached state."""
    clock = MutableClock(datetime(2026, 4, 20, 12, tzinfo=timezone.utc))
    local_day = date(2026, 1, 10)
    source_all_in = dict(_hourly_data(local_day).all_in_prices)
    source_market = {next(iter(source_all_in)): -1.0}

    async def fetch() -> HourlyTariffData:
        return HourlyTariffData(source_all_in, source_market)

    cache = TariffCache(None, "Europe/Amsterdam", clock=clock)
    first = await cache.async_get_hourly_day("electricity", local_day, fetch)
    original_first_price = next(iter(first.all_in_prices.values()))
    source_all_in[next(iter(source_all_in))] = 9999.0
    source_market.clear()

    with pytest.raises(TypeError):
        first.all_in_prices[next(iter(first.all_in_prices))] = (  # type: ignore[index]
            5.0
        )

    second = await cache.async_get_hourly_day("electricity", local_day, fetch)
    assert second is not first
    assert next(iter(second.all_in_prices.values())) == original_first_price
    assert second.market_prices


@pytest.mark.asyncio
async def test_prune_retains_only_requested_month_and_year_windows():
    """Pruning keeps two HOUR months and two DAY calendar years."""
    clock = MutableClock(datetime(2026, 7, 15, 12, tzinfo=timezone.utc))
    payload = _empty_payload()
    for local_day in (
        date(2026, 5, 15),
        date(2026, 6, 15),
        date(2026, 7, 1),
    ):
        _stored_hourly_bucket(
            payload,
            "electricity",
            local_day,
            _hourly_data(local_day),
        )
    payload["commodities"]["electricity"]["DAY"] = {
        "2024-01-01": 1.0,
        "2025-01-01": 2.0,
        "2026-01-01": 3.0,
    }
    store = FakeStore(payload)
    cache = TariffCache(store, "Europe/Amsterdam", clock=clock)

    await cache.async_initialize()
    await cache.async_prune()
    pruned = store.flush()

    assert set(pruned["commodities"]["electricity"]["HOUR"]) == {
        "2026-06-15",
        "2026-07-01",
    }
    assert pruned["commodities"]["electricity"]["DAY"] == {
        "2025-01-01": 2.0,
        "2026-01-01": 3.0,
    }
