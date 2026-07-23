<p align="center">
  <img src="icon.svg" width="150" alt="ANWB Energie Account Logo">
</p>

# ANWB Energie Account for Home Assistant

A custom component for Home Assistant that natively integrates your ANWB Energie
account. It securely fetches electricity consumption and production data and
calculates tariff-estimated costs.

## Features
*   **Native Energy Dashboard Support:** Seamlessly integrates with the built-in Home Assistant Energy dashboard.
*   **Hourly & Daily Statistics:** Import and export usage and tariff-estimated costs are automatically added to Long-Term Statistics.
*   **Current Dynamic Price Sensors:** Provides the current hourly all-in electricity price and bare market electricity price, along with today's and tomorrow's prices as attributes for charting (e.g., via ApexCharts).
*   **Month-to-Date & Year-to-Date Overviews:** Dedicated sensors for current month and year totals.
*   **Diagnostics Support:** Download redacted diagnostics natively from the UI to easily share bug reports.
*   **Official Translation Support:** Fully supports English and Dutch seamlessly through Home Assistant's translation engine.

## Energy Dashboard Setup

To configure the built-in Home Assistant Energy Dashboard with your ANWB
Energie data, navigate to **Settings** -> **Dashboards** -> **Energy** and use
the canonical entities below.

Home Assistant translates entity names when it creates them. The English names
below may appear in your Home Assistant language, and the generated `sensor.*`
entity IDs may be localized as well. Select the entities by their displayed
meaning, and prefer these canonical entities if older legacy names also exist.

### Electricity grid

| Energy Dashboard field | Select |
| --- | --- |
| Grid consumption | `Electricity import year to date` |
| Return to grid | `Electricity export year to date` |
| Cost tracking for grid consumption | Select **"Use an entity with current price"** and choose `Electricity current all-in price` |
| Compensation tracking for return to grid | No single current-price entity is exact under 2026 annual saldering; see below |

For import, the all-in price is the appropriate current price. For export, the
all-in price applies only to the portion that is eventually saldered against
annual import. The bare market price is appropriate only for annual surplus
export. Home Assistant cannot split one export meter between those portions as
the year develops, so leave export compensation unset unless you deliberately
accept one of those approximations. The integration's month-to-date estimated
export value uses the all-in price and is therefore not a final-bill amount.

### Gas

| Energy Dashboard field | Select |
| --- | --- |
| Gas consumption | `Gas usage year to date` |
| Gas cost tracking | Select **"Use an entity with current price"** and choose `Gas current all-in price` |

Do not use month-to-date usage or cost entities, or calculated total-cost
helpers, in the Energy Dashboard configuration. They are useful for overview
cards, but the Energy Dashboard should use the year-to-date usage entities and
current price entities.

> **⚠️ Note:** After installing the integration, it can take up to two hours for Home Assistant to generate the initial statistics. The sensors may not appear in the Energy Dashboard dropdown menus immediately. If they are missing, please wait a while and try again.

## Entity Model

New installs expose explicitly named canonical entities. Gas entities are
created only when current-month or year-to-date gas data indicates that the
account has gas, and are added automatically if gas is detected later.
Transient API gaps do not remove registered entities. Previously registered gas
entities are preserved but become unavailable when contract revalidation no
longer infers that the account has gas.

The API data used here does not expose an explicit gas-contract flag, so gas
applicability is inferred from current-month and year-to-date cache data and is
revalidated when a new year starts.

Older entity names such as `Yearly import usage`, `Monthly import usage`, and
`Current electricity price` are compatibility aliases. Clean installs do not
create them. Enabled and user-managed registered aliases remain available so
dashboards and automations do not break; untouched aliases that were still
disabled by the integration are removed. The estimated fixed-charge and
combined-total entities are also compatibility-only because their fixed-charge
fallback is not account-specific. For account-specific totals on a clean
installation, see [Calculating total energy costs](docs/cost-calculations.md).

If an upgraded installation still shows both names, the legacy entity mirrors
the canonical value. Check its automations, dashboards, and history before
disabling or removing it; the integration does not disable an actively
registered alias automatically.

| Entity | Clean install | Meaning |
| --- | --- | --- |
| `Estimated electricity import usage cost month to date` | Yes | Current-month `HOUR` usage multiplied by matching all-in `HOUR` tariffs; complete closed local days come from the persisted integration-local tariff cache, while the open day remains refreshable |
| `Estimated electricity export value month to date` | Yes | Current-month `HOUR` export multiplied by matching all-in `HOUR` tariffs, with the same closed-day cache policy; not the final 2026 export compensation after annual saldering |
| `Estimated electricity import usage cost year to date` | Yes | The current-month `HOUR` estimate plus completed prior months calculated from `DAY` usage and persisted `DAY` tariffs; January is `HOUR`-only |
| `Estimated electricity export value year to date` | Yes | The current-month `HOUR` estimate plus completed prior months calculated from `DAY` usage and persisted `DAY` tariffs; not the final annual-settlement value |
| `Estimated electricity fixed charges month to date` | No, compatibility only | Non-zero fixed components from the ANWB account cache, otherwise the hardcoded fallback prorated through the current calendar day |
| `Estimated electricity net cost month to date` | No, compatibility only | Import usage cost minus export value plus fixed charges |
| `Electricity current all-in price` | Yes | Current tariff including the components used for import cost tracking |
| `Electricity current bare market price` | Yes | Current bare `marktprijs`, excluding the other all-in components |
| `Estimated gas usage cost month to date` | Gas accounts | Current-month `HOUR` usage multiplied by matching all-in `HOUR` tariffs, with complete closed local days read from the persisted integration-local tariff cache |
| `Estimated gas usage cost year to date` | Gas accounts | The current-month `HOUR` estimate plus completed prior months calculated from `DAY` usage and persisted `DAY` tariffs; January is `HOUR`-only |
| `Estimated gas fixed charges month to date` | No, compatibility only | Non-zero fixed components from current-month gas data, otherwise the hardcoded fallback prorated through the current calendar day; unavailable when current gas applicability cannot be confirmed |
| `Estimated gas total cost month to date` | No, compatibility only | Gas usage cost plus gas fixed charges |
| `Gas current all-in price` | Gas accounts | Current all-in gas tariff |

The hardcoded full-month fallback is currently €8.50 delivery charges, €39.73
network charges, and −€52.41 energy-tax reduction for electricity; for gas it is
€8.50 delivery charges plus €17.50 network charges. These values may not match
the account, network region, or current contract. The fixed-charge entity
identifies whether ANWB account-cache values or the hardcoded fallback were used.
The `fixed_cost_source` attribute reports these as `account_cache` and
`hardcoded_fallback`.

All usage costs and export values are tariff estimates, not provider-billed
amounts. Authenticated API checks found zero cost fields even alongside non-zero
usage, so the integration combines ANWB account-cache usage with public `HOUR`
and `DAY` tariff data. A persisted integration-local tariff cache is shared by
the pricing and consumption coordinators and survives integration reloads and
Home Assistant restarts. Month-to-date estimates reuse complete, finite `HOUR`
tariffs for closed local days while the open day remains refreshable.
Year-to-date estimates reuse that current-month calculation and add completed
prior months using persisted finite `DAY` tariffs. Every non-zero usage day
must have a matching tariff before the estimate is published. January therefore
has no `DAY` component.

Only public tariff data is persisted in this cache, never account usage or
credentials. Failed, empty, or partial responses remain retryable and do not
evict valid cached tariffs or mark an incomplete period as complete. If any
non-zero usage interval lacks its matching tariff, strict coverage makes the
affected estimate unavailable instead of treating the missing price as €0. A
current-month cost is also unavailable when its `HOUR` usage total disagrees
with an available current `MONTH` aggregate beyond the documented rounding
tolerance. A net total is unavailable when one of its required variable
estimates is incomplete.

Transient ANWB gas account-cache failures reuse valid values only within the
same month or year. At a period rollover, affected gas entities remain
unavailable until new period data arrives rather than publishing the previous
period as current.

External cost statistics can repair incorrect €0 rows from an earlier version
when the matching hourly usage is still returned by the current-month ANWB
account-cache request and its `HOUR` tariff is available from either the
persisted closed-day cache or the refreshable open-day range. Older months are
not rebuilt automatically because the integration does not refetch their
hourly usage during normal updates.

## Cost Calculations

Clean installs expose month-to-date and year-to-date variable usage costs and
export values, but not combined totals with generic fixed charges. The
[cost calculation guide](docs/cost-calculations.md) explains the formulas and
shows how to create account-specific electricity and gas totals with Home
Assistant helpers.

## Example Dashboards

Using the popular [ApexCharts Card](https://github.com/RomRider/apexcharts-card), you can create beautiful graphs that color-code the current electricity and gas prices.
Replace each `sensor.your_account_*` placeholder with the entity ID of the
matching canonical entity in your installation. In the historic-usage example,
also replace each `a_xxxxxxxx` suffix in the `anwb_energie_account:*`
statistics IDs with the normalized suffix shown for your account.

![Electricity Prices](docs/electricity_prices.png)

### Electricity Prices
```yaml
type: custom:apexcharts-card
experimental:
  color_threshold: true
header:
  show: true
  title: Electricity Prices Today
  show_states: true
  colorize_states: true
graph_span: 24h
span:
  start: day
now:
  show: true
  label: Now
series:
  - entity: sensor.your_account_electricity_current_all_in_price
    type: column
    data_generator: |
      return entity.attributes.prices.map((record) => {
        return [new Date(record.start_time).getTime(), record.price];
      });
    color_threshold:
      - value: -1
        color: '#4CAF50'
      - value: 0
        color: '#8BC34A'
      - value: 0.15
        color: '#FFC107'
      - value: 0.25
        color: '#FF9800'
      - value: 0.35
        color: '#F44336'
      - value: 0.5
        color: '#E91E63'
```

### Gas Prices
```yaml
type: custom:apexcharts-card
experimental:
  color_threshold: true
header:
  show: true
  title: Gas Prices Today
  show_states: true
  colorize_states: true
graph_span: 24h
span:
  start: day
now:
  show: true
  label: Now
series:
  - entity: sensor.your_account_gas_current_all_in_price
    type: column
    data_generator: |
      return entity.attributes.prices.map((record) => {
        return [new Date(record.start_time).getTime(), record.price];
      });
    color_threshold:
      - value: 0
        color: '#4CAF50'
      - value: 1
        color: '#8BC34A'
      - value: 1.2
        color: '#FFC107'
      - value: 1.4
        color: '#FF9800'
      - value: 1.6
        color: '#F44336'
      - value: 1.8
        color: '#E91E63'
```

### Historic Usage (Yesterday)
```yaml
type: custom:apexcharts-card
header:
  show: true
  title: Historic Usage (Yesterday)
graph_span: 24h
span:
  start: day
  offset: -1d
stacked: true
yaxis:
  - decimals: 2
series:
  - entity: sensor.your_account_electricity_import_month_to_date
    name: Import
    type: column
    color: '#3498db'
    data_generator: |
      const stats = await hass.callWS({ type: 'recorder/statistics_during_period', start_time: start.toISOString(), end_time: end.toISOString(), statistic_ids: ['anwb_energie_account:import_usage_a_xxxxxxxx'], period: 'hour' });
      const data = stats['anwb_energie_account:import_usage_a_xxxxxxxx'] || [];
      return data.map(s => [s.start, s.state]);
  - entity: sensor.your_account_electricity_export_month_to_date
    name: Export
    type: column
    color: '#f1c40f'
    invert: true
    data_generator: |
      const stats = await hass.callWS({ type: 'recorder/statistics_during_period', start_time: start.toISOString(), end_time: end.toISOString(), statistic_ids: ['anwb_energie_account:export_usage_a_xxxxxxxx'], period: 'hour' });
      const data = stats['anwb_energie_account:export_usage_a_xxxxxxxx'] || [];
      return data.map(s => [s.start, s.state]);
```

## Installation

### HACS (Recommended)
1. Open HACS in your Home Assistant instance.
2. Search for **ANWB Energie Account**.
3. Click the three-dot menu.
4. Select **Download**.
5. Restart Home Assistant.

## Configuration
1. Go to **Settings** -> **Devices & Services** -> **Add Integration**.
2. Search for **ANWB Energie Account**.
3. You will be provided with a login link. Click it to open the ANWB portal in your browser.
4. Log in with your ANWB account.
5. You will be redirected to a blank or error page. This is normal. **Copy the entire URL from your browser's address bar** and paste it back into Home Assistant.
