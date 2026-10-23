# ANWB API Observations

This document records behavior observed from ANWB's undocumented API that affects
integration design. Keep schema-level details in `openapi.yaml`; keep design
implications and interpretation here.

## 2026-06-15 Account Cache Costs

Authenticated probes of the account cache endpoints showed that the cache schema
contains cost objects, but the values should not currently be used as the
integration's cost source.

Endpoints checked:

- `/energy/energy-services/v1/accounts/{account_number}/electricity/cache`
- `/energy/energy-services/v1/accounts/{account_number}/production/cache`
- `/energy/energy-services/v1/accounts/{account_number}/gas/cache`

Intervals checked:

- `HOUR` for current-month hourly rows
- `MONTH` for year-to-date monthly rows

Observed behavior:

- Electricity and production cache responses used `unit: kWh`.
- Gas cache responses used `unit: m3`; the verified account without gas returned
  HTTP 200 with an empty `data` array.
- Electricity and production cache rows included `variabeleKosten` and
  `vasteKosten`.
- In the 2026-06-15 probe those cost objects were all zero in sampled `HOUR` and
  `MONTH` rows, including rows/months with non-zero usage (see the 2026-10
  update below: populated when `hasGap` is false).
- `hasGap` was true for sampled non-zero usage rows, so it is not a sufficient
  reason to discard a row.
- Public tariff endpoints returned non-zero prices for the same electricity and
  production hours, and tariff-based calculations produced non-zero costs.

Follow-up probe on 2026-06-17:

- Used `scripts/anwb_api_probe.py probe --previous-year`.
- Checked `HOUR` and `DAY` for yesterday and the same weekday one week earlier.
- Checked `DAY` and `MONTH` for the previous calendar month.
- Checked `MONTH` for closed months in the current year.
- Checked `MONTH` for the previous calendar year.
- All probed `variabeleKosten.total` and `vasteKosten.total` values were zero
  (all sampled rows had `hasGap: true`).
- This included non-zero usage windows such as:
  - last-week electricity `HOUR`: `0.557 kWh`, tariff-derived cost `0.14456302 EUR`
  - last-week production `HOUR`: `7.81 kWh`, tariff-derived value `1.93535431 EUR`
  - previous-month production `MONTH`: `346.834 kWh`
  - current-year closed-month electricity `MONTH`: `992.375976 kWh`
  - current-year closed-month production `MONTH`: `947.107 kWh`
- The result argues against a simple one-day delay, closed-month backfill, or
  `DAY`/`MONTH`-only population theory for the verified account.
- Remaining uncertainty: sampled non-zero usage rows still had `hasGap: true`,
  so the probe did not disprove a possible `hasGap: false`-only cost behavior.

Design implication:

- Continue using `usage` from account cache endpoints as the source for import,
  export, and gas quantities.
- Continue using the public tariff endpoints as the source for variable cost
  calculations.
- As of 2026-06, `variabeleKosten.total` was not used for cost entities because
  every probed value was zero. The 2026-10 update below shows it can be
  populated; it remains out of scope as a cost source.
- If cost entities are redesigned, document whether they are tariff-estimated
  values or provider-billed values.

### 2026-10 Update: populated when `hasGap` is false (issue #30)

An account owner reported (issue #30) that every `HOUR` row from
`/v1/accounts/{account}/{electricity,gas,production}/cache` for 13 to 16 October
2026 had `hasGap: false`, and that electricity and gas rows carried non-zero
`variabeleKosten` and `vasteKosten` every hour, including VAT.

- Cost fields are populated when `hasGap` is false. Zero was observed when
  `hasGap` is true (2026-06 probes), and on limited accounts.
- `vasteKosten.total` is the hour's share of the fixed costs: delivery fee per
  month divided by days in month and 24, grid fee and energy-tax reduction per
  year divided by 365 and 24. The energy-tax reduction appears on electricity
  rows only.
- `DAY` rows were reported as the sum of that day's `HOUR` rows.
- The integration sums `vasteKosten.total` over all `HOUR` rows of the
  month-to-date fetch for the `account_cache` fixed-cost source. If any row has
  `hasGap: true` or lacks a numeric total, it uses its hardcoded constants for
  the whole month. Reading only the first row, as before, yielded one hour.

## 2026-10-02 Kraken Measurement Cost Statistics

The account-cache cost objects above stayed zero for a verified account with
`hasGap: true` rows (they are populated when `hasGap` is false, see the 2026-10
update). The amounts the ANWB app shows come from Kraken GraphQL:
`account.properties.measurements` with `utilityFilters` on
`electricityFilters.readingFrequencyType` and `readingDirection`
(`CONSUMPTION` or `GENERATION`), selecting `metaData.statistics`.

Observed behavior:

- `HOUR_INTERVAL` readings return usage but an empty `statistics` list.
- `DAY_INTERVAL` readings return statistics for every day, including the
  current month, and a whole year fits in one page of 400. Data runs through
  the previous local day.
- `MONTH_INTERVAL` readings return statistics only when the requested range
  contains no current or future month; including the current month empties the
  statistics of every returned month.
- `QUARTER_INTERVAL` returns `measurements: null`.
- Statistic amounts are euro cents in `costInclTax.estimatedAmount` and
  `costExclTax.estimatedAmount`, as decimal strings.
- Consumption labels: `SUPPLIER_DYNAMIC_COST`, `ENERGY_TAX_STEP_1` and
  `SUPPLIER_PURCHASING_COST` (type `CONSUMPTION_COST`); `SUPPLIER_FIXED_COST`,
  `GRID_FEE` and `ENERGY_TAX_REDUCTION` (type `STANDING_CHARGE_COST`).
- Generation labels: `GENERATION_SUPPLIER_DYNAMIC_COST` over all exported kWh,
  and `GENERATION_ENERGY_TAX_STEP_1` and `GENERATION_SUPPLIER_PURCHASING_COST`
  over the netted kWh only, all negative.
- Daily statistics summed per month equal the `MONTH_INTERVAL` statistics, and
  daily usage equals the account-cache `MONTH` usage for closed months.
- Kraken nets per month: the netted kWh is the smaller of that month's import
  and export.

## 2026-07-23 Historical Tariff Ranges

A public, unauthenticated follow-up checked the v2 electricity and gas tariff
endpoints with historical multi-day ranges:

- `interval=HOUR` returned HTTP 500 for seven-day and month-long ranges.
- `interval=DAY` returned one daily tariff point per completed day.
- A January-through-July `interval=DAY` request succeeded for both electricity
  and gas in one request per commodity.

Issue [#28](https://github.com/adrighem/ha-anwb-energie/issues/28) subsequently
reported a 200-price limit for both `DAY` endpoints: longer responses still
contain date rows, but prices after the limit are null. The integration splits
long ranges into sequential windows of at most 190 calendar-day labels,
including padding. Adjacent windows overlap by two labels to preserve boundary
coverage. Finite all-in and market prices are merged before entering the cache;
missing prices remain retryable and do not bypass the strict coverage checks.

The integration therefore keeps ANWB account-cache usage separate from its
persisted integration-local tariff cache:

- Month-to-date estimates match `HOUR` usage to `HOUR` tariffs. Complete tariff
  sets for closed local days are persisted, while the current day and tomorrow
  remain refreshable.
- Year-to-date estimates reuse the current-month `HOUR` calculation and add
  completed prior months using `DAY` usage and tariffs. Each finite closed-day
  tariff returned by ANWB is persisted; a result is published only when every
  non-zero usage day has a tariff. January is therefore `HOUR`-only.
- Existing year-to-date usage remains sourced from the verified `MONTH`
  response. The current-period `HOUR` usage sum is checked when its `MONTH` row
  is available. The combined `DAY` usage for completed prior months must match
  the combined authoritative `MONTH` usage before a cost estimate is published.

The tariff API treats `startDate` and `endDate` as Europe/Amsterdam calendar-day
labels even though the query values end in `Z`. The integration therefore maps
each Home Assistant local day to the one or two overlapping ANWB day labels,
combines those responses, and then keeps only canonical UTC points belonging to
the requested local day. The same trailing-label handling is applied to `DAY`
lookups for Home Assistant installations outside the Amsterdam timezone.

The persisted integration-local tariff cache is shared by the pricing and
consumption coordinators and survives integration reloads and Home Assistant
restarts. Entries are separated by commodity and resolution: `HOUR` points use
their canonical UTC timestamp, while `DAY` points use the Home Assistant local
date. Only finite numeric values from public tariff responses are persisted;
ANWB account-cache usage and credentials are never stored in this cache.

Failed, empty, or partial responses do not evict valid cached values or mark a
period complete. Missing and newly closed periods remain retryable. Strict
coverage still applies: a non-zero usage interval without a matching tariff
makes its cost estimate unavailable rather than implicitly pricing it at zero.
This bounds refresh traffic, but the completed-month part remains a daily
aggregate estimate rather than an hourly usage-weighted calculation.

For an empty cache, the consumption calculation on 31 December needs at most
31 local-day `HOUR` lookups plus two prior-month `DAY` requests per commodity when
every December day has non-zero usage. In the Amsterdam timezone each lookup is
one HTTP request; another Home Assistant timezone can require two sequential
ANWB day-label requests for one local day. No more than four tariff fetches run
concurrently. Once warm, closed days and completed months make no further
tariff requests; only open or still-missing periods are retried.

## 2026-10-11 Upstream Android App Verification (v6.30.1)

On 2026-10-11, the official Android app `nl.anwb.energie` version `6.30.1` (build `1301`, built with Expo SDK 56) was decompiled and analyzed to verify upstream API alignment.

Observed configuration and endpoint mapping:

- **OAuth Authentication**:
  - The Android client ID remains `57fe1448-00e6-47f2-bb50-c0935640b1fa` (iOS client ID is `e418d1d5-bb4d-4578-886c-e78adbe2b32c`).
  - OAuth endpoints remain `https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/{authorize,token,token/revoke}`.
  - Energy services base URL remains `https://api.anwb.nl/energy/energy-services`.
  - Kraken token exchange endpoint remains `/v1/auth/kraken-token` and GraphQL remains `https://api.anwb-kraken.energy/v1/graphql/`.

- **Account Cache & Telemetry**:
  - Historical electricity consumption, production, and gas telemetry endpoints continue to use `/v1/accounts/{account_number}/{electricity,production,gas}/cache` with `startDate`, `endDate`, `contractStartDate`, and `interval` parameters.

- **Dynamic Tariffs**:
  - The integration's interval endpoints `/v2/tarieven/{electricity,gas}` remain active and are required for querying arbitrary date ranges and intervals (`interval=HOUR`, `interval=DAY`).
  - Upstream introduced `/v3/tarieven/electricity/actueel` and `/v3/tarieven/gas/actueel` for real-time mobile widgets:
    - `/v3/tarieven/electricity/actueel` requires no query parameters and returns a 72-hour rolling hourly price series (`date`, `marktprijs`, `allInPrijs`) covering yesterday, today, and tomorrow.
    - `/v3/tarieven/gas/actueel` requires no query parameters and returns today's 24 hourly points along with a `currentPrices` object indicating prices `until6` (06:00 gas-day boundary) and `after6`.

- **Account Tussenstand**:
  - The upstream app uses `/v3/accounts/{account_number}/tussenstand` for the annual balance projection screen. Probing confirmed this endpoint requires query parameters `maandbedrag` and `contractStartDate`, returning HTTP 401 without a valid Kraken token.

## Re-Verification Workflow

Use `scripts/anwb_api_probe.py` to re-check whether account-cache cost fields are
backfilled later, populated only for specific granularities, or tied to `hasGap`.

The script writes OAuth/Kraken tokens and redacted reports under
`.anwb-api-probe/`. That directory is ignored by git.

```bash
python scripts/anwb_api_probe.py login-url
```

Open the printed login URL, then run:

```bash
python scripts/anwb_api_probe.py probe --callback
```

Paste the full callback URL at the hidden prompt. Do not pass it as a command-line
argument, because callback authorization codes can otherwise be retained in shell
history or exposed through the process list. Before exchanging the code, the probe
validates the redirect origin and path and requires the returned state to match
the saved login state.

Subsequent runs can reuse or refresh cached tokens:

```bash
python scripts/anwb_api_probe.py probe
```

To include previous calendar-year monthly rows:

```bash
python scripts/anwb_api_probe.py probe --previous-year
```

Every `probe` run also includes a `current_month` window. It sends the exact
full-month `HOUR` requests the integration sends (local month start through
month end, including future hours, with the same UTC boundary strings) for
electricity, production and gas. Each result gets a `current_month` section with
`rows_after_now`, first and last `startDate`, the last `startDate` with usage,
gap rows with and without usage, capped contiguous `hasGap` ranges, `vasteKosten`
sums over all rows and over `hasGap: false` rows, and the value the integration's
`sum_account_cache_fixed_costs` would return (issue #30). Tariff checks are
skipped for this window.

To fetch Kraken `DAY_INTERVAL` statistics with per-line detail (issue #29):

```bash
python scripts/anwb_api_probe.py kraken-statistics [--from YYYY-MM-DD] [--to YYYY-MM-DD]
```

The range defaults to the current year to date. The command reuses cached
tokens (or `--callback`), queries `CONSUMPTION` and `GENERATION` and also
selects statistic `label`, `value` and `unit` (the live schema rejected `unit`
with HTTP 400 in 2026-10). Fields the schema rejects are dropped one at a time
and listed by name in `rejected_statistic_fields`. The
report `.anwb-api-probe/kraken-statistics.json` (mode `0600`) holds per-day
statistics, per-month totals per direction and label, and a
`netting_comparison` with monthly and cumulative (per calendar year)
import, export and `min(import, export)` kWh next to the generation label kWh.
`refund_kwh` is the kWh on the `GENERATION_ENERGY_TAX_STEP_1` line and
`refund_basis` names the first candidate it matches (`import`, `export`,
`monthly_min`, `cumulative_min_delta`, or `other`).

The report is written to `.anwb-api-probe/last-report.json` (mode `0600`). It contains only
allowlisted aggregate fields and request status context. Upstream error bodies are
discarded and are never included in reports or error output. The report also
intentionally excludes account number, address, and bearer tokens.

Interpretation:

- Non-zero cost fields only in older windows would support a delayed backfill
  theory.
- Non-zero cost fields only for `DAY` or `MONTH` would support an aggregated-only
  theory.
- Non-zero cost fields only when `hasGap` is false would support a gap-gated
  theory.
- Zero cost fields across yesterday, last week, previous month, and closed
  monthly rows with `hasGap: true` was observed in 2026-06. The 2026-10 update
  (issue #30) confirmed the gap-gated reading: populated when `hasGap` is false.
