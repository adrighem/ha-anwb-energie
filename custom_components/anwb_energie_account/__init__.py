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
from homeassistant.helpers.storage import Store

from . import api
from .const import CLIENT_ID, DOMAIN
from .coordinator import (
    ANWBConsumptionCoordinator,
    ANWBPricingCoordinator,
    ANWBEnergieAccountConfigEntry,
    ANWBEnergieAccountData,
)
from .tariff_cache import TariffCache

PLATFORMS: list[Platform] = [Platform.SENSOR]
_TARIFF_CACHE_DATA_KEY = f"{DOMAIN}_tariff_cache"
_TARIFF_CACHE_STORE_KEY = f"{DOMAIN}.tariff_cache"
_TARIFF_CACHE_STORE_VERSION = 1


async def _async_get_tariff_cache(hass: HomeAssistant) -> TariffCache:
    """Return the shared tariff cache, loading persisted data once."""
    timezone_name = getattr(hass.config, "time_zone", None)
    if not isinstance(timezone_name, str):
        timezone_name = "UTC"

    existing = hass.data.get(_TARIFF_CACHE_DATA_KEY)
    if (
        isinstance(existing, TariffCache)
        and existing.timezone_name == timezone_name
    ):
        await existing.async_initialize()
        return existing

    cache = TariffCache(
        Store(
            hass,
            _TARIFF_CACHE_STORE_VERSION,
            _TARIFF_CACHE_STORE_KEY,
        ),
        timezone_name,
    )
    # Store the instance before awaiting so concurrent config-entry setups
    # converge on the same repository and its initialization lock.
    hass.data[_TARIFF_CACHE_DATA_KEY] = cache
    await cache.async_initialize()
    await cache.async_prune()
    return cache


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

    tariff_cache = await _async_get_tariff_cache(hass)
    consumption_coordinator = ANWBConsumptionCoordinator(
        hass,
        auth,
        entry,
        tariff_cache=tariff_cache,
    )
    await consumption_coordinator.async_config_entry_first_refresh()

    pricing_coordinator = ANWBPricingCoordinator(
        hass,
        auth,
        entry,
        tariff_cache=tariff_cache,
        gas_applicable=lambda: bool(
            isinstance(consumption_coordinator.data, dict)
            and consumption_coordinator.data.get("has_gas") is True
        ),
    )
    await pricing_coordinator.async_config_entry_first_refresh()

    entry.runtime_data = ANWBEnergieAccountData(
        consumption=consumption_coordinator,
        pricing=pricing_coordinator,
        tariff_cache=tariff_cache,
    )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: ANWBEnergieAccountConfigEntry
) -> bool:
    """Unload a config entry."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
