"""Diagnostics support for ANWB Energie Account."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.components.diagnostics import async_redact_data

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .coordinator import ANWBEnergieAccountConfigEntry

TO_REDACT = {
    "account_address",
    "account_number",
    "access_token",
    "id_token",
    "refresh_token",
}


def _redact_coordinator_data(data: dict[str, Any] | None) -> dict[str, Any]:
    """Redact coordinator data, handling an unavailable initial update."""
    return async_redact_data(data, TO_REDACT) if data else {}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ANWBEnergieAccountConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    runtime_data = entry.runtime_data

    diagnostics_data = {
        "config_entry": async_redact_data(entry.as_dict(), TO_REDACT),
        "coordinator_data": {
            "consumption": _redact_coordinator_data(runtime_data.consumption.data),
            "pricing": _redact_coordinator_data(runtime_data.pricing.data),
        },
    }

    return diagnostics_data
