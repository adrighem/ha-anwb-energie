# Calculating total energy costs

The integration exposes estimated variable usage costs and export values. A
clean installation does not create combined total-cost entities because the
ANWB API does not reliably provide account-specific fixed charges.

Use the entities below together with the fixed charges from your own contract
or invoice to create account-specific totals. These calculations are intended
for overview cards. Do not use them as Energy Dashboard inputs.

## What the integration calculates

For each non-zero usage interval, the integration requires a matching ANWB
all-in tariff. Prices returned by the API are in cents per unit:

```text
electricity import cost = sum(import kWh × all-in cents/kWh ÷ 100)
electricity export value = sum(export kWh × all-in cents/kWh ÷ 100)
gas usage cost = sum(gas m³ × all-in cents/m³ ÷ 100)
```

Month-to-date estimates match `HOUR` usage to `HOUR` tariffs. Complete tariff
sets for closed local days come from the persisted integration-local tariff
cache, while the current day remains refreshable. Year-to-date estimates
combine:

- the current-month `HOUR` calculation used by the month-to-date entities; and
- `DAY` usage and individually persisted finite `DAY` tariffs for completed
  prior months.

January has no completed prior month, so its year-to-date estimate is
`HOUR`-only.

The daily historical calculation keeps API traffic bounded, but it makes the
year-to-date values estimates rather than provider-billed amounts. The current
month remains calculated at hourly granularity. Before publishing an estimate,
the integration checks the current-month hourly usage sum when its monthly row
is available. It also checks that combined daily usage for all completed prior
months matches the combined authoritative monthly usage.

The persisted integration-local tariff cache is restart-safe and shared by the
pricing and consumption coordinators. It stores only finite values from public
tariff responses, never ANWB account-cache usage or credentials. Failed, empty,
or partial responses leave valid cached tariffs intact and the affected period
retryable. They do not relax the requirement for complete tariff coverage.

The English display names of the clean-install entities are:

| Period | Electricity import | Electricity export | Gas |
| --- | --- | --- | --- |
| Month to date | `Estimated electricity import usage cost month to date` | `Estimated electricity export value month to date` | `Estimated gas usage cost month to date` |
| Year to date | `Estimated electricity import usage cost year to date` | `Estimated electricity export value year to date` | `Estimated gas usage cost year to date` |

Home Assistant may localize the generated `sensor.*` entity IDs. Find the exact
IDs for your installation under **Settings → Devices & services → Entities**
before copying the examples below.

## Total-cost formulas

For either month to date or year to date:

```text
electricity total = electricity import cost
                  - electricity export value
                  + net electricity fixed charges

gas total = gas usage cost + net gas fixed charges
```

Credits and tax reductions must use negative values. Do not add variable taxes
or markups again: they are already included in the all-in tariffs used by the
integration.

Variable costs stop at the latest available metered interval. The month-to-date
fixed-charge example accrues the full current calendar day's share, so that
component may extend beyond the last metered interval. Neither calculation
forecasts usage or prices for the rest of the month or year.

### Fixed charges month to date

Use the signed net fixed amount from your own contract. It may include fixed
delivery charges, grid-operator charges, and an electricity energy-tax
reduction.

If the contract gives a full-month amount and was active for the entire month:

```text
fixed charges month to date
  = net fixed charges for the full month
  × current calendar day
  ÷ number of days in the month
```

If charges are quoted per day, multiply the signed net daily rate by the number
of active contract days instead. Use active days as well when a contract starts,
ends, or changes rate partway through a month.

### Fixed charges year to date

Rates and contracts can change during a year, so do not multiply the current
month's rate by the elapsed part of the year. Calculate each rate period
separately:

```text
fixed charges year to date
  = sum(net daily fixed rate × active days for each rate period)
```

If only monthly rates are available, prorate each affected month by its active
days and then add the results. The number entered in the year-to-date helper
below should be this signed accrued total.

## Create totals with Home Assistant helpers

Home Assistant has no dedicated helper for subtraction combined with
availability handling, so Template Helpers are appropriate here. Create them
through the UI rather than adding YAML to `configuration.yaml`.

### 1. Create fixed-charge Number helpers

Go to **Settings → Devices & services → Helpers → Create helper → Number**.
Create the helpers you need:

- `Electricity net fixed charges current month`
- `Gas net fixed charges current month`
- `Electricity net fixed charges year to date`
- `Gas net fixed charges year to date`

Use `€` as the unit, a step of `0.01`, and allow a sufficiently low negative
minimum for electricity tax reductions. Enter full-month signed amounts in the
current-month helpers and already-accrued signed amounts in the year-to-date
helpers.

The generated `input_number.*` IDs may differ from those in the examples.

### 2. Create month-to-date Template Helpers

Go to **Settings → Devices & services → Helpers → Create helper → Template**,
choose **Sensor**, and create the helpers below. Use the **Monetary** device
class and `€` as the unit. Leave the state class unset; these overview helpers
reset and should not be used in the Energy Dashboard.

Replace every example entity ID with the actual ID from your installation.

#### Electricity total month to date

Availability template:

```jinja
{{ is_number(states('sensor.replace_with_electricity_import_cost_mtd'))
   and is_number(states('sensor.replace_with_electricity_export_value_mtd'))
   and is_number(states(
     'input_number.electricity_net_fixed_charges_current_month'
   )) }}
```

State template:

```jinja
{% set month_start = now().replace(
  day=1, hour=0, minute=0, second=0, microsecond=0
) %}
{% set next_month = (
  month_start.replace(year=month_start.year + 1, month=1)
  if month_start.month == 12
  else month_start.replace(month=month_start.month + 1)
) %}
{% set days_in_month = (next_month - month_start).days %}
{% set fixed_month = states(
  'input_number.electricity_net_fixed_charges_current_month'
) | float %}
{% set fixed_to_date = fixed_month * now().day / days_in_month %}
{{ (
  states('sensor.replace_with_electricity_import_cost_mtd') | float
  - states('sensor.replace_with_electricity_export_value_mtd') | float
  + fixed_to_date
) | round(2) }}
```

#### Gas total month to date

Availability template:

```jinja
{{ is_number(states('sensor.replace_with_gas_usage_cost_mtd'))
   and is_number(states(
     'input_number.gas_net_fixed_charges_current_month'
   )) }}
```

State template:

```jinja
{% set month_start = now().replace(
  day=1, hour=0, minute=0, second=0, microsecond=0
) %}
{% set next_month = (
  month_start.replace(year=month_start.year + 1, month=1)
  if month_start.month == 12
  else month_start.replace(month=month_start.month + 1)
) %}
{% set days_in_month = (next_month - month_start).days %}
{% set fixed_month = states(
  'input_number.gas_net_fixed_charges_current_month'
) | float %}
{% set fixed_to_date = fixed_month * now().day / days_in_month %}
{{ (
  states('sensor.replace_with_gas_usage_cost_mtd') | float
  + fixed_to_date
) | round(2) }}
```

### 3. Create year-to-date Template Helpers

Create two more Template Sensor helpers with the **Monetary** device class and
`€` as the unit.

#### Electricity total year to date

Availability template:

```jinja
{{ is_number(states('sensor.replace_with_electricity_import_cost_ytd'))
   and is_number(states('sensor.replace_with_electricity_export_value_ytd'))
   and is_number(states(
     'input_number.electricity_net_fixed_charges_year_to_date'
   )) }}
```

State template:

```jinja
{% set import_cost = states(
  'sensor.replace_with_electricity_import_cost_ytd'
) | float %}
{% set export_value = states(
  'sensor.replace_with_electricity_export_value_ytd'
) | float %}
{% set fixed_to_date = states(
  'input_number.electricity_net_fixed_charges_year_to_date'
) | float %}
{{ (import_cost - export_value + fixed_to_date) | round(2) }}
```

#### Gas total year to date

Availability template:

```jinja
{{ is_number(states('sensor.replace_with_gas_usage_cost_ytd'))
   and is_number(states(
     'input_number.gas_net_fixed_charges_year_to_date'
   )) }}
```

State template:

```jinja
{% set usage_cost = states(
  'sensor.replace_with_gas_usage_cost_ytd'
) | float %}
{% set fixed_to_date = states(
  'input_number.gas_net_fixed_charges_year_to_date'
) | float %}
{{ (usage_cost + fixed_to_date) | round(2) }}
```

## Limitations

- Every non-zero usage interval must have a finite matching tariff. If coverage
  is incomplete, the source cost entity and the Template Helper are
  unavailable instead of treating the missing tariff as €0.
- Zero and negative tariffs are valid.
- Export value uses the electricity all-in tariff. Under annual saldering it is
  not the final compensation on the provider's annual settlement.
- Do not clamp a negative export value to zero. Subtracting a negative value
  correctly increases the estimated net electricity cost.
- Persisted `DAY` tariffs make the completed-month part of a year-to-date value
  less precise than an hourly weighted calculation.
- Hourly calculations match timestamps directly; daylight-saving days may have
  23 or 25 intervals.
- API and meter data can lag, so "to date" means through the latest available
  metered interval.
- Fixed charges, tax reductions, contract dates, and rate changes must come
  from your own account documents.
- The visible source entities are rounded to cents. A helper based on those
  states can differ slightly from calculations that retain full internal
  precision.
