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
- Those cost objects were all zero in sampled `HOUR` and `MONTH` rows, including
  rows/months with non-zero usage.
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
- All probed `variabeleKosten.total` and `vasteKosten.total` values were zero.
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
- Do not use `variabeleKosten.total` or `vasteKosten.total` for cost entities
  unless future authenticated verification shows non-zero values with understood
  units and accounting semantics.
- If cost entities are redesigned, document whether they are tariff-estimated
  values or provider-billed values. The currently verified cache cost fields do
  not support provider-billed cost entities.

## 2026-07-23 Historical Tariff Ranges

A public, unauthenticated follow-up checked the v2 electricity and gas tariff
endpoints with historical multi-day ranges:

- `interval=HOUR` returned HTTP 500 for seven-day and month-long ranges.
- `interval=DAY` returned one daily tariff point per completed day.
- A January-through-July `interval=DAY` request succeeded for both electricity
  and gas in one request per commodity.

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
31 local-day `HOUR` lookups plus one prior-month `DAY` range per commodity when
every December day has non-zero usage. In the Amsterdam timezone each lookup is
one HTTP request; another Home Assistant timezone can require two sequential
ANWB day-label requests for one local day. No more than four tariff fetches run
concurrently. Once warm, closed days and completed months make no further
tariff requests; only open or still-missing periods are retried.

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
python scripts/anwb_api_probe.py probe 'https://login.anwb.nl/.../callback?code=...'
```

Subsequent runs can reuse or refresh cached tokens:

```bash
python scripts/anwb_api_probe.py probe
```

To include previous calendar-year monthly rows:

```bash
python scripts/anwb_api_probe.py probe --previous-year
```

The report is written to `.anwb-api-probe/last-report.json` and intentionally
does not include account number, address, or bearer tokens.

Interpretation:

- Non-zero cost fields only in older windows would support a delayed backfill
  theory.
- Non-zero cost fields only for `DAY` or `MONTH` would support an aggregated-only
  theory.
- Non-zero cost fields only when `hasGap` is false would support a gap-gated
  theory.
- Zero cost fields across yesterday, last week, previous month, and closed
  monthly rows supports continuing to treat cache cost fields as non-authoritative.
