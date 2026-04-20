<p align="center">
  <img src="icon.svg" width="150" alt="ANWB Energie Account Logo">
</p>

# ANWB Energie Account for Home Assistant

A custom component for Home Assistant that natively integrates your ANWB Energie account. It securely fetches your electricity consumption, production, and cost data. 

## Features
*   **Native Energy Dashboard Support:** Seamlessly integrates with the built-in Home Assistant Energy dashboard.
*   **Hourly & Daily Statistics:** Import and export usage and costs are automatically added to Long-Term Statistics.
*   **Current Dynamic Price Sensor:** Provides the current hourly electricity price, along with today's and tomorrow's prices as attributes for charting (e.g., via ApexCharts).
*   **Monthly & Yearly Overviews:** Dedicated sensors for your current month and year totals.
*   **Diagnostics Support:** Download redacted diagnostics natively from the UI to easily share bug reports.
*   **Official Translation Support:** Fully supports English and Dutch seamlessly through Home Assistant's translation engine.

## Installation

### HACS (Recommended)
1. Open HACS in your Home Assistant instance.
2. Click the three dots in the top right corner and select **Custom repositories**.
3. Add the URL to this repository and select **Integration** as the category.
4. Click **Add**, then download the integration.
5. Restart Home Assistant.

## Configuration
1. Go to **Settings** -> **Devices & Services** -> **Add Integration**.
2. Search for **ANWB Energie Account**.
3. You will be provided with a login link. Click it to open the ANWB portal in your browser.
4. Log in with your ANWB account.
5. You will be redirected to a blank or error page. This is normal. **Copy the entire URL from your browser's address bar** and paste it back into Home Assistant.
