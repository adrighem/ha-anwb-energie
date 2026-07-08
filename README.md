<p align="center">
  <img src="icon.svg" width="150" alt="ANWB Energie Account Logo">
</p>

# ANWB Energie Account for Home Assistant

A custom component for Home Assistant that natively integrates your ANWB Energie account. It securely fetches your electricity consumption, production, and cost data. 

## Features
*   **Native Energy Dashboard Support:** Seamlessly integrates with the built-in Home Assistant Energy dashboard.
*   **Hourly & Daily Statistics:** Import and export usage and costs are automatically added to Long-Term Statistics.
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
| Cost tracking for grid consumption | Select **"Use an entity with current price"** and choose `Electricity current price` |
| Cost tracking / compensation tracking for return to grid | Select **"Use an entity with current price"** and choose `Electricity current price` |

### Gas

| Energy Dashboard field | Select |
| --- | --- |
| Gas consumption | `Gas usage year to date` |
| Gas cost tracking | Select **"Use an entity with current price"** and choose `Gas current price` |

Do not use the month-to-date totals or month-to-date cost sensors in the Energy
Dashboard configuration. They are useful for overview cards, but the Energy
Dashboard should use the year-to-date usage entities and current price entities.

> **⚠️ Note:** After installing the integration, it can take up to two hours for Home Assistant to generate the initial statistics. The sensors may not appear in the Energy Dashboard dropdown menus immediately. If they are missing, please wait a while and try again.

## Entity Model

New installs expose explicitly named electricity and gas entities, such as
`Electricity import year to date`, `Electricity import month to date`, `Gas usage
year to date`, and `Gas current price`.

Older entity names such as `Yearly import usage`, `Monthly import usage`, and
`Current electricity price` are kept for compatibility, but are disabled by
default for newly created entity registry entries. Existing enabled entities are
not disabled during upgrades. Prefer the canonical entity names in new Energy
Dashboard configuration.

Cost sensors are tariff-estimated values. Authenticated API verification showed
that the account cache contains `variabeleKosten` and `vasteKosten` fields, but
those fields were zero even for non-zero usage. The integration therefore uses
account-cache usage together with public hourly tariff data for variable cost
calculations.

`Electricity current price` is the all-in price and remains the entity to use
for Energy Dashboard cost tracking. `Electricity market price` exposes the bare
`marktprijs` from the same ANWB tariff response. The all-in price attributes also
include `market_price` per hourly record for combined charting.

## Example Dashboards

Using the popular [ApexCharts Card](https://github.com/RomRider/apexcharts-card), you can create beautiful graphs that color-code the current electricity and gas prices.

![Electricity Prices](docs/electricity_prices.png)
![Gas Prices](docs/gas_prices.png)

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
  - entity: sensor.anwb_account_a_xxxxxxxx_huidige_elektriciteitsprijs
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
  - entity: sensor.anwb_account_a_xxxxxxxx_huidige_gasprijs
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
  - entity: sensor.anwb_account_a_xxxxxxxx_maandelijks_importverbruik
    name: Import
    type: column
    color: '#3498db'
    data_generator: |
      const stats = await hass.callWS({ type: 'recorder/statistics_during_period', start_time: start.toISOString(), end_time: end.toISOString(), statistic_ids: ['anwb_energie_account:import_usage_a_xxxxxxxx'], period: 'hour' });
      const data = stats['anwb_energie_account:import_usage_a_xxxxxxxx'] || [];
      return data.map(s => [s.start, s.state]);
  - entity: sensor.anwb_account_a_xxxxxxxx_maandelijks_exportverbruik
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
