"""Shared, persistent tariff cache for ANWB Energie."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
import logging
import math
from types import MappingProxyType
from typing import Any, Literal, Protocol, TypedDict, TypeVar
from zoneinfo import ZoneInfo

_LOGGER = logging.getLogger(__name__)

CACHE_SCHEMA_VERSION = 1
DEFAULT_MAX_CONCURRENCY = 4
DEFAULT_MEMORY_TTL = timedelta(minutes=30)
DEFAULT_SAVE_DELAY = 30.0

Commodity = Literal["electricity", "gas"]
_COMMODITIES: tuple[Commodity, ...] = ("electricity", "gas")


class _HourlyBucketJSON(TypedDict):
    """JSON representation of a complete local HOUR bucket."""

    all_in: dict[str, float]
    market: dict[str, float]


class _CommodityCacheJSON(TypedDict):
    """JSON tariff data for one commodity."""

    HOUR: dict[str, _HourlyBucketJSON]
    DAY: dict[str, float]


class _CacheJSON(TypedDict):
    """Versioned JSON payload stored by Home Assistant."""

    schema_version: int
    timezone: str
    commodities: dict[str, _CommodityCacheJSON]


class StoreLike(Protocol):
    """Small subset of Home Assistant Store used by the tariff cache."""

    async def async_load(self) -> Mapping[str, Any] | None:
        """Load the stored payload."""

    def async_delay_save(
        self,
        data_func: Callable[[], Mapping[str, Any]],
        delay: float,
    ) -> None:
        """Schedule a coalesced delayed save."""


@dataclass(frozen=True, slots=True)
class HourlyTariffData:
    """Normalized hourly tariff maps keyed by UTC hour."""

    all_in_prices: Mapping[str, float]
    market_prices: Mapping[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Detach the maps supplied by callers and expose them read-only."""
        object.__setattr__(
            self,
            "all_in_prices",
            MappingProxyType(dict(self.all_in_prices)),
        )
        object.__setattr__(
            self,
            "market_prices",
            MappingProxyType(dict(self.market_prices)),
        )


HourlyFetchCallback = Callable[[], Awaitable[HourlyTariffData]]
DailyFetchCallback = Callable[
    [frozenset[date]],
    Awaitable[Mapping[str, float]],
]


@dataclass(frozen=True, slots=True)
class _MemoryHourlyBucket:
    """An hourly result that must not be persisted yet."""

    data: HourlyTariffData
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class _MemoryDailyPrice:
    """A daily result that must not be persisted yet."""

    price: float
    expires_at: datetime


_T = TypeVar("_T")


def _utcnow() -> datetime:
    """Return the current UTC time."""
    return datetime.now(timezone.utc)


def _finite_number(value: Any) -> float | None:
    """Return a finite float while preserving valid zero and negative values."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None

    number = float(value)
    return number if math.isfinite(number) else None


def _parse_local_date(value: Any) -> date | None:
    """Parse an exact ISO local-date key."""
    if not isinstance(value, str):
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def _previous_month(value: date) -> tuple[int, int]:
    """Return the year/month tuple preceding value's month."""
    if value.month == 1:
        return value.year - 1, 12
    return value.year, value.month - 1


class TariffCache:
    """Cache public ANWB tariff data across coordinators and restarts."""

    def __init__(
        self,
        store: StoreLike | None,
        timezone_name: str,
        *,
        clock: Callable[[], datetime] = _utcnow,
        memory_ttl: timedelta = DEFAULT_MEMORY_TTL,
        save_delay: float = DEFAULT_SAVE_DELAY,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    ) -> None:
        """Initialize the tariff cache."""
        if memory_ttl <= timedelta(0):
            raise ValueError("memory_ttl must be positive")
        if save_delay < 0:
            raise ValueError("save_delay must not be negative")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least one")

        self._store = store
        self._timezone_name = timezone_name
        self._timezone = ZoneInfo(timezone_name)
        self._clock = clock
        self._memory_ttl = memory_ttl
        self._save_delay = save_delay
        self._semaphore = asyncio.Semaphore(max_concurrency)

        self._initialized = False
        self._initialize_lock = asyncio.Lock()
        self._save_scheduled = False
        self._inflight: dict[tuple[Any, ...], asyncio.Task[Any]] = {}

        self._hourly: dict[Commodity, dict[str, HourlyTariffData]] = {
            commodity: {} for commodity in _COMMODITIES
        }
        self._daily: dict[Commodity, dict[str, float]] = {
            commodity: {} for commodity in _COMMODITIES
        }
        self._memory_hourly: dict[
            tuple[Commodity, str],
            _MemoryHourlyBucket,
        ] = {}
        self._memory_daily: dict[
            tuple[Commodity, str],
            _MemoryDailyPrice,
        ] = {}

    @property
    def timezone_name(self) -> str:
        """Return the timezone used for local tariff buckets."""
        return self._timezone_name

    def hourly_prices_are_complete(
        self,
        local_day: date,
        prices: Mapping[str, Any],
    ) -> bool:
        """Return whether a price map covers every real hour in local_day."""
        local_day = self._validated_date(local_day)
        normalized = self._normalized_price_map(local_day, prices)
        return self._expected_hour_keys(local_day).issubset(normalized)

    async def async_initialize(self) -> None:
        """Load and validate persisted tariff data once."""
        if self._initialized:
            return

        async with self._initialize_lock:
            if self._initialized:
                return

            payload: Mapping[str, Any] | None = None
            if self._store is not None:
                try:
                    payload = await self._store.async_load()
                # A disposable cache must never block integration setup.
                except Exception:  # noqa: BLE001
                    _LOGGER.warning(
                        "Unable to load the tariff cache; starting with an empty cache"
                    )

            needs_rewrite = False
            if payload is not None:
                needs_rewrite = (
                    self._load_payload(payload)
                    if isinstance(payload, Mapping)
                    else True
                )

            self._initialized = True
            if needs_rewrite:
                self._schedule_save()

    async def async_get_hourly_day(
        self,
        commodity: Commodity,
        local_day: date,
        fetch: HourlyFetchCallback,
    ) -> HourlyTariffData:
        """Return one local day's hourly tariffs, fetching when necessary."""
        await self.async_initialize()
        commodity = self._validated_commodity(commodity)
        local_day = self._validated_date(local_day)
        day_key = local_day.isoformat()

        if cached := self._hourly[commodity].get(day_key):
            return self._copy_hourly(cached)

        now = self._now()
        memory_key = (commodity, day_key)
        previous_memory = self._memory_hourly.get(memory_key)
        if (
            previous_memory is not None
            and local_day < self._local_today(now)
            and self._is_complete_hourly_day(local_day, previous_memory.data)
        ):
            self._hourly[commodity][day_key] = previous_memory.data
            self._memory_hourly.pop(memory_key, None)
            self._schedule_save()
            return self._copy_hourly(previous_memory.data)
        if previous_memory is not None and previous_memory.expires_at > now:
            return self._copy_hourly(previous_memory.data)

        async def _fetch_and_cache() -> HourlyTariffData:
            async with self._semaphore:
                fetched = await fetch()
            if not isinstance(fetched, HourlyTariffData):
                raise TypeError("hourly fetch callback must return HourlyTariffData")

            current = self._memory_hourly.get(memory_key)
            base = current.data if current is not None else None
            normalized = self._normalize_hourly_data(
                local_day,
                fetched,
                base=base,
            )
            current_now = self._now()
            if local_day < self._local_today(
                current_now
            ) and self._is_complete_hourly_day(local_day, normalized):
                self._hourly[commodity][day_key] = normalized
                self._memory_hourly.pop(memory_key, None)
                self._schedule_save()
            else:
                self._memory_hourly[memory_key] = _MemoryHourlyBucket(
                    normalized,
                    current_now + self._memory_ttl,
                )
            return normalized

        result = await self._single_flight(
            ("HOUR", commodity, day_key),
            _fetch_and_cache,
        )
        return self._copy_hourly(result)

    async def async_get_daily_prices(
        self,
        commodity: Commodity,
        required_dates: Collection[date],
        fetch: DailyFetchCallback,
    ) -> Mapping[str, float]:
        """Return available daily prices and fetch each missing date."""
        await self.async_initialize()
        commodity = self._validated_commodity(commodity)
        required = frozenset(self._validated_date(value) for value in required_dates)
        if not required:
            return MappingProxyType({})

        now = self._now()
        missing = self._missing_daily_dates(commodity, required, now)
        if missing:

            async def _fetch_and_cache() -> None:
                async with self._semaphore:
                    fetched = await fetch(missing)
                if not isinstance(fetched, Mapping):
                    raise TypeError("daily fetch callback must return a mapping")

                current_now = self._now()
                today = self._local_today(current_now)
                changed_persistent = False
                for raw_date, raw_price in fetched.items():
                    local_date = _parse_local_date(raw_date)
                    price = _finite_number(raw_price)
                    if local_date is None or price is None:
                        continue

                    day_key = local_date.isoformat()
                    memory_key = (commodity, day_key)
                    if local_date < today:
                        if self._daily[commodity].get(day_key) != price:
                            self._daily[commodity][day_key] = price
                            changed_persistent = True
                        self._memory_daily.pop(memory_key, None)
                    else:
                        self._memory_daily[memory_key] = _MemoryDailyPrice(
                            price,
                            current_now + self._memory_ttl,
                        )

                if changed_persistent:
                    self._schedule_save()

            await self._single_flight(
                (
                    "DAY",
                    commodity,
                    tuple(sorted(day.isoformat() for day in missing)),
                ),
                _fetch_and_cache,
            )

        return self._daily_result(commodity, required, self._now())

    async def async_get_cached_daily_prices(
        self,
        commodity: Commodity,
        required_dates: Collection[date],
    ) -> Mapping[str, float]:
        """Return cached DAY prices without starting a fetch."""
        await self.async_initialize()
        commodity = self._validated_commodity(commodity)
        required = frozenset(self._validated_date(value) for value in required_dates)
        return self._daily_result(commodity, required, self._now())

    async def async_prune(self) -> None:
        """Retain HOUR current/previous months and DAY current/previous years."""
        await self.async_initialize()
        now = self._now()
        today = self._local_today(now)
        keep_months = {
            (today.year, today.month),
            _previous_month(today),
        }
        keep_years = {today.year, today.year - 1}
        changed = False

        for (commodity, day_key), bucket in tuple(self._memory_hourly.items()):
            local_day = _parse_local_date(day_key)
            if (
                local_day is not None
                and local_day < today
                and self._is_complete_hourly_day(local_day, bucket.data)
            ):
                self._hourly[commodity][day_key] = bucket.data
                self._memory_hourly.pop((commodity, day_key), None)
                changed = True

        for commodity in _COMMODITIES:
            hourly = self._hourly[commodity]
            for day_key in tuple(hourly):
                local_day = _parse_local_date(day_key)
                if (
                    local_day is None
                    or (local_day.year, local_day.month) not in keep_months
                ):
                    del hourly[day_key]
                    changed = True

            daily = self._daily[commodity]
            for day_key in tuple(daily):
                local_day = _parse_local_date(day_key)
                if local_day is None or local_day.year not in keep_years:
                    del daily[day_key]
                    changed = True

        self._memory_hourly = {
            key: value
            for key, value in self._memory_hourly.items()
            if value.expires_at > now
        }
        self._memory_daily = {
            key: value
            for key, value in self._memory_daily.items()
            if value.expires_at > now
        }

        if changed:
            self._schedule_save()

    async def _single_flight(
        self,
        key: tuple[Any, ...],
        operation: Callable[[], Awaitable[_T]],
    ) -> _T:
        """Run one shielded operation for identical concurrent requests."""
        task = self._inflight.get(key)
        if task is None:
            task = asyncio.create_task(operation())
            self._inflight[key] = task
            task.add_done_callback(
                lambda completed, request_key=key: self._finish_inflight(
                    request_key,
                    completed,
                )
            )
        return await asyncio.shield(task)

    def _finish_inflight(
        self,
        key: tuple[Any, ...],
        task: asyncio.Task[Any],
    ) -> None:
        """Remove a completed single-flight task and consume orphaned failures."""
        if self._inflight.get(key) is task:
            self._inflight.pop(key, None)
        if not task.cancelled():
            task.exception()

    def _load_payload(self, payload: Mapping[str, Any]) -> bool:
        """Load a valid payload, returning whether it should be rewritten."""
        if (
            payload.get("schema_version") != CACHE_SCHEMA_VERSION
            or payload.get("timezone") != self._timezone_name
            or not isinstance(payload.get("commodities"), Mapping)
        ):
            return True

        commodities = payload["commodities"]
        needs_rewrite = False
        today = self._local_today(self._now())
        for commodity in _COMMODITIES:
            raw_commodity = commodities.get(commodity)
            if not isinstance(raw_commodity, Mapping):
                needs_rewrite = True
                continue

            raw_hourly = raw_commodity.get("HOUR")
            if not isinstance(raw_hourly, Mapping):
                needs_rewrite = True
            else:
                for raw_day, raw_bucket in raw_hourly.items():
                    local_day = _parse_local_date(raw_day)
                    if (
                        local_day is None
                        or local_day >= today
                        or not isinstance(raw_bucket, Mapping)
                    ):
                        needs_rewrite = True
                        continue

                    raw_all_in = raw_bucket.get("all_in")
                    raw_market = raw_bucket.get("market")
                    if not isinstance(raw_all_in, Mapping) or not isinstance(
                        raw_market, Mapping
                    ):
                        needs_rewrite = True
                        continue

                    normalized = self._normalize_hourly_data(
                        local_day,
                        HourlyTariffData(raw_all_in, raw_market),
                    )
                    if not self._is_complete_hourly_day(local_day, normalized):
                        needs_rewrite = True
                        continue
                    self._hourly[commodity][local_day.isoformat()] = normalized

            raw_daily = raw_commodity.get("DAY")
            if not isinstance(raw_daily, Mapping):
                needs_rewrite = True
            else:
                for raw_day, raw_price in raw_daily.items():
                    local_day = _parse_local_date(raw_day)
                    price = _finite_number(raw_price)
                    if local_day is None or local_day >= today or price is None:
                        needs_rewrite = True
                        continue
                    self._daily[commodity][local_day.isoformat()] = price

        if set(commodities) != set(_COMMODITIES):
            needs_rewrite = True
        return needs_rewrite

    def _schedule_save(self) -> None:
        """Schedule one delayed save containing the latest cache snapshot."""
        if self._store is None or self._save_scheduled:
            return

        self._save_scheduled = True

        def _serialize_when_due() -> Mapping[str, Any]:
            self._save_scheduled = False
            return self._serialize()

        self._store.async_delay_save(_serialize_when_due, self._save_delay)

    def _serialize(self) -> _CacheJSON:
        """Create a detached, deterministic JSON-compatible cache payload."""
        commodities: dict[str, _CommodityCacheJSON] = {}
        for commodity in _COMMODITIES:
            commodities[commodity] = {
                "HOUR": {
                    day_key: {
                        "all_in": dict(sorted(bucket.all_in_prices.items())),
                        "market": dict(sorted(bucket.market_prices.items())),
                    }
                    for day_key, bucket in sorted(self._hourly[commodity].items())
                },
                "DAY": dict(sorted(self._daily[commodity].items())),
            }

        return {
            "schema_version": CACHE_SCHEMA_VERSION,
            "timezone": self._timezone_name,
            "commodities": commodities,
        }

    def _normalize_hourly_data(
        self,
        local_day: date,
        fetched: HourlyTariffData,
        *,
        base: HourlyTariffData | None = None,
    ) -> HourlyTariffData:
        """Normalize, validate, and merge one local day's hourly maps."""
        all_in = dict(base.all_in_prices) if base is not None else {}
        market = dict(base.market_prices) if base is not None else {}
        all_in.update(self._normalized_price_map(local_day, fetched.all_in_prices))
        market.update(self._normalized_price_map(local_day, fetched.market_prices))
        return HourlyTariffData(all_in, market)

    def _normalized_price_map(
        self,
        local_day: date,
        prices: Mapping[str, Any],
    ) -> dict[str, float]:
        """Return finite prices for local_day under canonical UTC-hour keys."""
        normalized: dict[str, float] = {}
        for raw_timestamp, raw_price in prices.items():
            timestamp = self._normalize_utc_hour(raw_timestamp)
            price = _finite_number(raw_price)
            if timestamp is None or price is None:
                continue

            parsed = datetime.fromisoformat(timestamp.removesuffix("Z") + "+00:00")
            if parsed.astimezone(self._timezone).date() == local_day:
                normalized[timestamp] = price
        return normalized

    @staticmethod
    def _normalize_utc_hour(value: Any) -> str | None:
        """Normalize an aware ISO timestamp to an exact UTC-hour key."""
        if not isinstance(value, str) or not value:
            return None
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if (
            parsed.tzinfo is None
            or parsed.minute != 0
            or parsed.second != 0
            or parsed.microsecond != 0
        ):
            return None
        return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:00:00.000Z")

    def _is_complete_hourly_day(
        self,
        local_day: date,
        data: HourlyTariffData,
    ) -> bool:
        """Return whether every real hour in a local day has an all-in tariff."""
        return self._expected_hour_keys(local_day).issubset(data.all_in_prices)

    def _expected_hour_keys(self, local_day: date) -> set[str]:
        """Return the 23, 24, or 25 UTC keys belonging to a local day."""
        start_local = datetime.combine(local_day, time.min, self._timezone)
        end_local = datetime.combine(
            local_day + timedelta(days=1),
            time.min,
            self._timezone,
        )
        current = start_local.astimezone(timezone.utc)
        end = end_local.astimezone(timezone.utc)
        keys: set[str] = set()
        while current < end:
            keys.add(current.strftime("%Y-%m-%dT%H:00:00.000Z"))
            current += timedelta(hours=1)
        return keys

    def _missing_daily_dates(
        self,
        commodity: Commodity,
        required: frozenset[date],
        now: datetime,
    ) -> frozenset[date]:
        """Return required dates absent from persistent or live memory data."""
        missing: set[date] = set()
        for local_day in required:
            day_key = local_day.isoformat()
            if day_key in self._daily[commodity]:
                continue

            memory_key = (commodity, day_key)
            memory = self._memory_daily.get(memory_key)
            if memory is not None and memory.expires_at > now:
                continue
            self._memory_daily.pop(memory_key, None)
            missing.add(local_day)
        return frozenset(missing)

    def _daily_result(
        self,
        commodity: Commodity,
        required: frozenset[date],
        now: datetime,
    ) -> Mapping[str, float]:
        """Return a detached, immutable map for available required dates."""
        result: dict[str, float] = {}
        for local_day in sorted(required):
            day_key = local_day.isoformat()
            if day_key in self._daily[commodity]:
                result[day_key] = self._daily[commodity][day_key]
                continue

            memory_key = (commodity, day_key)
            memory = self._memory_daily.get(memory_key)
            if memory is not None and memory.expires_at > now:
                result[day_key] = memory.price
            else:
                self._memory_daily.pop(memory_key, None)
        return MappingProxyType(result)

    @staticmethod
    def _copy_hourly(data: HourlyTariffData) -> HourlyTariffData:
        """Return a detached immutable hourly value object."""
        return HourlyTariffData(data.all_in_prices, data.market_prices)

    def _now(self) -> datetime:
        """Return the injected clock as an aware UTC datetime."""
        now = self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return now.astimezone(timezone.utc)

    def _local_today(self, now: datetime) -> date:
        """Return the current date in the configured Home Assistant timezone."""
        return now.astimezone(self._timezone).date()

    @staticmethod
    def _validated_commodity(commodity: Commodity) -> Commodity:
        """Validate a public commodity argument."""
        if commodity not in _COMMODITIES:
            raise ValueError(f"Unsupported tariff commodity: {commodity!r}")
        return commodity

    @staticmethod
    def _validated_date(value: date) -> date:
        """Validate a local-day argument without accepting datetimes."""
        if not isinstance(value, date) or isinstance(value, datetime):
            raise TypeError("local tariff days must be datetime.date values")
        return value
