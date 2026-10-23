"""Kraken DAY_INTERVAL statistics probe with per-line detail (issue #29).

Fetches CONSUMPTION and GENERATION daily measurements like the integration's
``kraken_costs`` module, additionally selecting per-statistic ``label``,
``value`` and ``unit`` when the undocumented schema accepts them. The report
aggregates per month and per label and compares refund-line kWh with monthly
and cumulative netting.
"""

from __future__ import annotations

import argparse
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

if __package__:
    from . import anwb_api_probe as probe
else:  # pragma: no cover - executed as a script from the scripts directory
    import anwb_api_probe as probe

DIRECTIONS = ("CONSUMPTION", "GENERATION")
OPTIONAL_STATISTIC_FIELDS = ("label", "value", "unit")
TIME_ZONE_NAME = "Europe/Amsterdam"
REFUND_LABEL = "GENERATION_ENERGY_TAX_STEP_1"
PAGE_SIZE = 100
MAX_PAGES = 10
MAX_RANGE_DAYS = 366

# Only this message shape is inspected; nothing else from an error is kept.
_REJECTED_FIELD = re.compile(r'Cannot query field "(?P<field>\w+)" on type')
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_.\- ]{1,64}$")

_QUERY_TEMPLATE = """
query AnwbProbeDailyStatistics(
  $accountNumber: String!
  $startAt: DateTime!
  $endAt: DateTime!
  $timezone: String!
  $direction: ReadingDirectionType!
  $first: Int!
  $after: String
) {
  account(accountNumber: $accountNumber) {
    properties {
      measurements(
        first: $first
        after: $after
        startAt: $startAt
        endAt: $endAt
        timezone: $timezone
        utilityFilters: [
          {
            electricityFilters: {
              readingFrequencyType: DAY_INTERVAL
              readingDirection: $direction
            }
          }
        ]
      ) {
        pageInfo {
          hasNextPage
          endCursor
        }
        edges {
          node {
            value
            ... on IntervalMeasurementType {
              startAt
            }
            metaData {
              statistics {
                type
__EXTRA_FIELDS__
                costInclTax {
                  estimatedAmount
                }
              }
            }
          }
        }
      }
    }
  }
}
"""


@dataclass(frozen=True)
class StatisticsQuery:
    """Accepted optional statistic fields and those the schema rejected."""

    fields: tuple[str, ...]
    rejected: tuple[str, ...]


def build_query(fields: Sequence[str]) -> str:
    """Return the statistics query selecting the given optional fields."""
    unknown = set(fields) - set(OPTIONAL_STATISTIC_FIELDS)
    if unknown:
        raise ValueError("Unsupported statistic fields requested")
    extra = "\n".join(f"                {name}" for name in fields)
    return _QUERY_TEMPLATE.replace("__EXTRA_FIELDS__\n", f"{extra}\n" if extra else "")


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _safe_token(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and _SAFE_TOKEN.match(value):
        return value
    return "<redacted>"


def _rejected_field(errors: Any, candidates: Sequence[str]) -> str | None:
    """Return a candidate field named by an allowlisted GraphQL error message."""
    if not isinstance(errors, list):
        return None
    for error in errors:
        message = error.get("message") if isinstance(error, Mapping) else None
        if not isinstance(message, str):
            continue
        match = _REJECTED_FIELD.search(message)
        if match and match.group("field") in candidates:
            return match.group("field")
    return None


def _variables(
    account_number: str,
    direction: str,
    start: date,
    end_inclusive: date,
    after: str | None,
) -> dict[str, Any]:
    zone = probe.ZoneInfo(TIME_ZONE_NAME) if probe.ZoneInfo else timezone.utc
    return {
        "accountNumber": account_number,
        "startAt": datetime.combine(start, time.min, tzinfo=zone).isoformat(),
        "endAt": datetime.combine(
            end_inclusive + timedelta(days=1), time.min, tzinfo=zone
        ).isoformat(),
        "timezone": TIME_ZONE_NAME,
        "direction": direction,
        "first": PAGE_SIZE,
        "after": after,
    }


def _post(kraken_token: str, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    response = probe._request_json(
        "POST",
        probe.GRAPHQL_URL,
        headers={"Authorization": f"Bearer {kraken_token}"},
        json_body={"query": query, "variables": variables},
    )
    return probe._require_json_response(
        response,
        operation="Kraken statistics query",
    )


def negotiate_fields(
    kraken_token: str,
    account_number: str,
    start: date,
    end_inclusive: date,
) -> tuple[StatisticsQuery, dict[str, Any]]:
    """Drop optional fields until the schema accepts the query.

    Returns the accepted field set and the first CONSUMPTION page.
    """
    fields = list(OPTIONAL_STATISTIC_FIELDS)
    rejected: list[str] = []
    variables = _variables(account_number, DIRECTIONS[0], start, end_inclusive, None)
    while True:
        response = probe._request_json(
            "POST",
            probe.GRAPHQL_URL,
            headers={"Authorization": f"Bearer {kraken_token}"},
            json_body={"query": build_query(fields), "variables": variables},
        )
        # Kraken answers an unknown field with HTTP 400 and the body is
        # discarded, so treat a 400 as a rejection of the last optional field.
        if response.status == 400 and fields:
            rejected.append(fields.pop())
            continue
        payload = probe._require_json_response(
            response,
            operation="Kraken statistics query",
        )
        errors = payload.get("errors")
        if not errors:
            return StatisticsQuery(tuple(fields), tuple(rejected)), payload
        if not fields:
            raise probe.ProbeError("Kraken statistics query returned GraphQL errors")
        name = _rejected_field(errors, fields) or fields[-1]
        fields.remove(name)
        rejected.append(name)


def _properties(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    if payload.get("errors"):
        raise probe.ProbeError("Kraken statistics query returned GraphQL errors")
    data = payload.get("data")
    account = data.get("account") if isinstance(data, Mapping) else None
    properties = account.get("properties") if isinstance(account, Mapping) else None
    if not isinstance(properties, list) or not properties:
        raise probe.ProbeError("Kraken statistics query returned no properties")
    return [prop for prop in properties if isinstance(prop, Mapping)]


def _statistic(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise probe.ProbeError("Kraken statistics query returned a malformed statistic")
    cost = raw.get("costInclTax")
    amount = _finite(cost.get("estimatedAmount")) if isinstance(cost, Mapping) else None
    return {
        "type": _safe_token(raw.get("type")),
        "label": _safe_token(raw.get("label")),
        "value": _finite(raw.get("value")),
        "unit": _safe_token(raw.get("unit")),
        "cost_incl_tax_estimated_amount": amount,
    }


def parse_page(
    payload: Mapping[str, Any],
    direction: str,
) -> tuple[list[dict[str, Any]], str | None]:
    """Return sanitized day entries and the next cursor of one page."""
    zone = probe.ZoneInfo(TIME_ZONE_NAME) if probe.ZoneInfo else timezone.utc
    properties = _properties(payload)
    entries: list[dict[str, Any]] = []
    cursor: str | None = None
    for prop in properties:
        measurements = prop.get("measurements")
        if not isinstance(measurements, Mapping):
            raise probe.ProbeError("Kraken statistics query returned no measurements")
        page_info = measurements.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            if len(properties) > 1:
                raise probe.ProbeError("Paginated multi-property results are unsupported")
            end_cursor = page_info.get("endCursor")
            if not isinstance(end_cursor, str) or not end_cursor:
                raise probe.ProbeError("Kraken reported a next page without cursor")
            cursor = end_cursor
        for edge in measurements.get("edges") or []:
            node = edge.get("node") if isinstance(edge, Mapping) else None
            start_at = node.get("startAt") if isinstance(node, Mapping) else None
            if not isinstance(node, Mapping) or not isinstance(start_at, str):
                raise probe.ProbeError("Kraken returned a malformed measurement")
            try:
                local_day = datetime.fromisoformat(start_at).astimezone(zone).date()
            except ValueError:
                raise probe.ProbeError("Kraken returned an invalid start time") from None
            statistics = (node.get("metaData") or {}).get("statistics") or []
            entries.append(
                {
                    "date": local_day.isoformat(),
                    "direction": direction,
                    "value": _finite(node.get("value")),
                    "statistics": [_statistic(item) for item in statistics],
                }
            )
    return entries, cursor


def fetch_direction(
    kraken_token: str,
    account_number: str,
    query: StatisticsQuery,
    direction: str,
    start: date,
    end_inclusive: date,
    first_page: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Fetch all sanitized day entries of one direction."""
    text = build_query(query.fields)
    entries: list[dict[str, Any]] = []
    payload = first_page
    cursor: str | None = None
    for _ in range(MAX_PAGES):
        if payload is None:
            payload = _post(
                kraken_token,
                text,
                _variables(account_number, direction, start, end_inclusive, cursor),
            )
        page, cursor = parse_page(payload, direction)
        entries.extend(page)
        if cursor is None:
            return entries
        payload = None
    raise probe.ProbeError("Kraken statistics exceeded the page limit")


def _add(target: dict[str, float], key: str, value: float | None) -> None:
    if value is not None:
        target[key] = round(target.get(key, 0.0) + value, 8)


def monthly_aggregates(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return per-month, per-direction totals and per-label kWh and EUR."""
    months: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in entries:
        key = (str(entry["date"])[:7], str(entry["direction"]))
        bucket = months.setdefault(
            key,
            {"days": 0, "kwh": 0.0, "eur": 0.0, "label_kwh": {}, "label_eur": {}},
        )
        bucket["days"] += 1
        bucket["kwh"] = round(bucket["kwh"] + (entry.get("value") or 0.0), 8)
        for statistic in entry["statistics"]:
            name = statistic.get("label") or statistic.get("type") or "<unknown>"
            cents = statistic.get("cost_incl_tax_estimated_amount")
            eur = None if cents is None else cents / 100.0
            if eur is not None:
                bucket["eur"] = round(bucket["eur"] + eur, 8)
            _add(bucket["label_kwh"], name, statistic.get("value"))
            _add(bucket["label_eur"], name, eur)
    return [
        {"month": month, "direction": direction, **values}
        for (month, direction), values in sorted(months.items())
    ]


def netting_comparison(aggregates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Compare net-metering refund kWh with candidate netting bases.

    The refund kWh is the kWh on the GENERATION energy-tax line. ``refund_basis``
    names the first candidate (import, export, monthly or cumulative minimum)
    it matches. Cumulative totals restart each calendar year.
    """
    by_month: dict[str, dict[str, Mapping[str, Any]]] = {}
    for item in aggregates:
        by_month.setdefault(str(item["month"]), {})[str(item["direction"])] = item
    rows: list[dict[str, Any]] = []
    cumulative_import = cumulative_export = previous_min = 0.0
    year: str | None = None
    for month in sorted(by_month):
        if month[:4] != year:
            year = month[:4]
            cumulative_import = cumulative_export = previous_min = 0.0
        consumption = by_month[month].get("CONSUMPTION") or {}
        generation = by_month[month].get("GENERATION") or {}
        import_kwh = float(consumption.get("kwh") or 0.0)
        export_kwh = float(generation.get("kwh") or 0.0)
        cumulative_import += import_kwh
        cumulative_export += export_kwh
        cumulative_min = min(cumulative_import, cumulative_export)
        label_kwh = dict(generation.get("label_kwh") or {})
        refund_kwh = label_kwh.get(REFUND_LABEL)
        candidates = {
            "import": import_kwh,
            "export": export_kwh,
            "monthly_min": min(import_kwh, export_kwh),
            "cumulative_min_delta": cumulative_min - previous_min,
        }
        rows.append(
            {
                "month": month,
                "import_kwh": round(import_kwh, 8),
                "export_kwh": round(export_kwh, 8),
                "monthly_min_kwh": round(min(import_kwh, export_kwh), 8),
                "cumulative_import_kwh": round(cumulative_import, 8),
                "cumulative_export_kwh": round(cumulative_export, 8),
                "cumulative_min_kwh": round(cumulative_min, 8),
                "cumulative_min_delta_kwh": round(cumulative_min - previous_min, 8),
                "generation_label_kwh": label_kwh,
                "refund_kwh": refund_kwh,
                "refund_basis": _refund_basis(refund_kwh, candidates),
            }
        )
        previous_min = cumulative_min
    return rows


def _refund_basis(refund_kwh: Any, candidates: Mapping[str, float]) -> str | None:
    refund = _finite(refund_kwh)
    if refund is None:
        return None
    for name, kwh in candidates.items():
        if math.isclose(refund, kwh, rel_tol=1e-6, abs_tol=1e-3):
            return name
    return "other"


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from None


def _range(args: argparse.Namespace) -> tuple[date, date]:
    today = probe._local_today()
    start = args.start or date(today.year, 1, 1)
    end = args.end or today
    if start > end:
        raise probe.ProbeError("--from must not be after --to")
    if (end - start).days >= MAX_RANGE_DAYS:
        raise probe.ProbeError(f"Date range must be shorter than {MAX_RANGE_DAYS} days")
    return start, end


def command_kraken_statistics(args: argparse.Namespace) -> int:
    """Fetch sanitized Kraken daily statistics and write a local report."""
    start, end = _range(args)
    callback_url = probe._prompt_callback_url() if args.prompt_for_callback else None
    access_token = probe._get_oauth_token(callback_url)
    kraken_token = probe._get_kraken_token(access_token)
    account_number = probe._get_account_number(kraken_token)

    query, first_page = negotiate_fields(kraken_token, account_number, start, end)
    entries: list[dict[str, Any]] = []
    for direction in DIRECTIONS:
        entries.extend(
            fetch_direction(
                kraken_token,
                account_number,
                query,
                direction,
                start,
                end,
                first_page if direction == DIRECTIONS[0] else None,
            )
        )
    entries.sort(key=lambda entry: (entry["date"], entry["direction"]))
    aggregates = monthly_aggregates(entries)
    comparison = netting_comparison(aggregates)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "contains_account_identifiers": False,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "accepted_statistic_fields": list(query.fields),
        "rejected_statistic_fields": list(query.rejected),
        "days": entries,
        "monthly": aggregates,
        "netting_comparison": comparison,
    }
    probe._write_secret_json(probe.KRAKEN_STATISTICS_FILE, report)

    print(f"Kraken statistics {start.isoformat()} to {end.isoformat()}")
    print(f"Accepted statistic fields: {', '.join(query.fields) or 'none'}")
    if query.rejected:
        print(f"Rejected statistic fields: {', '.join(query.rejected)}")
    for row in comparison:
        print(
            f"{row['month']}: import {row['import_kwh']:.3f} kWh, "
            f"export {row['export_kwh']:.3f} kWh, "
            f"refund {row['refund_kwh'] or 0.0:.3f} kWh "
            f"(matches {row['refund_basis'] or 'none'})"
        )
    print(f"\nWrote sanitized report to ignored file: {probe.KRAKEN_STATISTICS_FILE}")
    return 0


def register(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add the ``kraken-statistics`` subcommand to the probe CLI."""
    parser = subparsers.add_parser(
        "kraken-statistics",
        help="Fetch Kraken DAY_INTERVAL statistics with per-line detail",
    )
    parser.add_argument(
        "--callback",
        dest="prompt_for_callback",
        action="store_true",
        help="Securely prompt for a full ANWB callback URL.",
    )
    parser.add_argument(
        "--from",
        dest="start",
        type=_parse_date,
        metavar="YYYY-MM-DD",
        default=None,
        help="First local day, YYYY-MM-DD (default: January 1 this year).",
    )
    parser.add_argument(
        "--to",
        dest="end",
        type=_parse_date,
        metavar="YYYY-MM-DD",
        default=None,
        help="Last local day, YYYY-MM-DD (default: today).",
    )
    parser.set_defaults(func=command_kraken_statistics)
