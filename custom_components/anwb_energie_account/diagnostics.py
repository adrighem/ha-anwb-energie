"""Diagnostics support for ANWB Energie Account."""

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from .coordinator import ANWBEnergieAccountConfigEntry

TO_REDACT = {
    "account_number",
    "access_token",
    "refresh_token",
    "id_token",
}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ANWBEnergieAccountConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator = entry.runtime_data

    diagnostics_data = {
        "config_entry": async_redact_data(entry.as_dict(), TO_REDACT),
        "coordinator_data": async_redact_data(coordinator.data, TO_REDACT)
        if coordinator.data
        else {},
    }

    return diagnostics_data
