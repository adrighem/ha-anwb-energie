"""The ANWB Energie Account integration."""

from __future__ import annotations

import time

from homeassistant.components.application_credentials import (
    ClientCredential,
    async_import_client_credential,
)
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import aiohttp_client
from homeassistant.helpers.config_entry_oauth2_flow import (
    ImplementationUnavailableError,
    OAuth2Session,
    async_get_config_entry_implementation,
)

from . import api
from .const import CLIENT_ID, DOMAIN
from .coordinator import ANWBDataUpdateCoordinator, ANWBEnergieAccountConfigEntry

PLATFORMS: list[Platform] = [Platform.SENSOR]


async def async_setup_entry(
    hass: HomeAssistant, entry: ANWBEnergieAccountConfigEntry
) -> bool:
    """Set up ANWB Energie Account from a config entry."""

    # Backwards compatibility/migration: Ensure expires_at exists if expires_in does.
    token = entry.data.get("token", {})
    if "expires_in" in token and "expires_at" not in token:
        new_token = {**token}
        new_token["expires_at"] = time.time() + new_token["expires_in"]
        hass.config_entries.async_update_entry(
            entry, data={**entry.data, "token": new_token}
        )

    await async_import_client_credential(
        hass,
        DOMAIN,
        ClientCredential(CLIENT_ID, "", name="ANWB Energie Account"),
    )

    try:
        implementation = await async_get_config_entry_implementation(hass, entry)
    except ImplementationUnavailableError as err:
        raise ConfigEntryNotReady(
            "OAuth2 implementation temporarily unavailable, will retry"
        ) from err

    session = OAuth2Session(hass, entry, implementation)

    auth = api.AsyncConfigEntryAuth(
        aiohttp_client.async_get_clientsession(hass), session
    )

    coordinator = ANWBDataUpdateCoordinator(hass, auth, entry)
    await coordinator.async_config_entry_first_refresh()

    entry.runtime_data = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: ANWBEnergieAccountConfigEntry
) -> bool:
    """Unload a config entry."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
