"""Provide oauth implementations for the ANWB Energie Account integration."""

from __future__ import annotations

from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_entry_oauth2_flow

from .const import OAUTH2_AUTHORIZE, OAUTH2_TOKEN


class ANWBEnergieAccountImplementation(
    config_entry_oauth2_flow.LocalOAuth2ImplementationWithPkce
):
    """ANWB Energie Account OAuth2 implementation."""

    def __init__(self, hass: HomeAssistant, domain: str, client_id: str) -> None:
        """Initialize OAuth2 implementation."""
        super().__init__(
            hass,
            domain,
            client_id,
            OAUTH2_AUTHORIZE,
            OAUTH2_TOKEN,
        )

    @property
    def name(self) -> str:
        """Name of the implementation."""
        return "ANWB Energie Account"

    @property
    def redirect_uri(self) -> str:
        """Return the redirect uri."""
        return (
            "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/callback"
        )

    @property
    def extra_authorize_data(self) -> dict[str, Any]:
        """Extra data that needs to be appended to the authorize url."""
        data: dict[str, Any] = {
            "prompt": "login",
        }
        data.update(super().extra_authorize_data)
        return data
