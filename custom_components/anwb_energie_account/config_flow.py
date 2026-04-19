"""Config flow for ANWB Energie Account."""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import time
from typing import Any
import urllib.parse

import voluptuous as vol

from homeassistant.components.application_credentials import (
    ClientCredential,
    async_import_client_credential,
)
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import CLIENT_ID, DOMAIN, OAUTH2_AUTHORIZE, OAUTH2_SCOPES, OAUTH2_TOKEN

_LOGGER = logging.getLogger(__name__)


def generate_pkce() -> tuple[str, str]:
    """Generate a PKCE verifier and challenge."""
    verifier = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode("utf-8")
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("utf-8")).digest())
        .rstrip(b"=")
        .decode("utf-8")
    )
    return verifier, challenge


class ANWBConfigFlow(ConfigFlow, domain=DOMAIN):
    """Config flow to handle ANWB Energie Account authentication manually."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize."""
        self.code_verifier: str | None = None
        self.auth_url: str | None = None
        self._reauth_entry: ConfigEntry | None = None

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle a flow start."""
        await async_import_client_credential(
            self.hass,
            DOMAIN,
            ClientCredential(CLIENT_ID, "", name="ANWB Energie Account"),
        )

        if self.code_verifier is None:
            self.code_verifier, challenge = generate_pkce()
            params = {
                "client_id": CLIENT_ID,
                "redirect_uri": "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/callback",
                "response_type": "code",
                "scope": " ".join(OAUTH2_SCOPES),
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "prompt": "login",
            }
            self.auth_url = f"{OAUTH2_AUTHORIZE}?{urllib.parse.urlencode(params)}"

        errors = {}
        if user_input is not None:
            url = user_input.get("auth_code_url", "")
            try:
                parsed_url = urllib.parse.urlparse(url)
                query = urllib.parse.parse_qs(parsed_url.query)
                if "code" not in query:
                    errors["base"] = "invalid_auth"
                else:
                    code = query["code"][0]

                    # Exchange code for token
                    session = async_get_clientsession(self.hass)
                    async with session.post(
                        OAUTH2_TOKEN,
                        data={
                            "client_id": CLIENT_ID,
                            "grant_type": "authorization_code",
                            "code": code,
                            "redirect_uri": "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/callback",
                            "code_verifier": self.code_verifier,
                        },
                    ) as resp:
                        resp.raise_for_status()
                        token_data = await resp.json()
                        if "expires_in" in token_data:
                            token_data["expires_at"] = (
                                time.time() + token_data["expires_in"]
                            )

                    await self.async_set_unique_id(DOMAIN)
                    self._abort_if_unique_id_configured()

                    return self.async_create_entry(
                        title="ANWB Energie Account",
                        data={
                            "auth_implementation": DOMAIN,
                            "token": token_data,
                        },
                    )
            except Exception as err:  # noqa: BLE001
                _LOGGER.error("Failed to authenticate: %s", err)
                errors["base"] = "invalid_auth"

        return self.async_show_form(
            step_id="user",
            description_placeholders={"auth_url": self.auth_url},
            data_schema=vol.Schema({vol.Required("auth_code_url"): str}),
            errors=errors,
        )

    async def async_step_reauth(self, entry_data: dict[str, Any]) -> ConfigFlowResult:
        """Perform reauth upon an API authentication error."""
        self._reauth_entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Confirm reauth."""
        if self.code_verifier is None:
            self.code_verifier, challenge = generate_pkce()
            params = {
                "client_id": CLIENT_ID,
                "redirect_uri": "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/callback",
                "response_type": "code",
                "scope": " ".join(OAUTH2_SCOPES),
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "prompt": "login",
            }
            self.auth_url = f"{OAUTH2_AUTHORIZE}?{urllib.parse.urlencode(params)}"

        errors = {}
        if user_input is not None:
            url = user_input.get("auth_code_url", "")
            try:
                parsed_url = urllib.parse.urlparse(url)
                query = urllib.parse.parse_qs(parsed_url.query)
                if "code" not in query:
                    errors["base"] = "invalid_auth"
                else:
                    code = query["code"][0]

                    # Exchange code for token
                    session = async_get_clientsession(self.hass)
                    async with session.post(
                        OAUTH2_TOKEN,
                        data={
                            "client_id": CLIENT_ID,
                            "grant_type": "authorization_code",
                            "code": code,
                            "redirect_uri": "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/callback",
                            "code_verifier": self.code_verifier,
                        },
                    ) as resp:
                        resp.raise_for_status()
                        token_data = await resp.json()
                        if "expires_in" in token_data:
                            token_data["expires_at"] = (
                                time.time() + token_data["expires_in"]
                            )

                    return self.async_update_reload_and_abort(
                        self._reauth_entry,
                        data={
                            "auth_implementation": DOMAIN,
                            "token": token_data,
                        },
                    )
            except Exception as err:  # noqa: BLE001
                _LOGGER.error("Failed to authenticate during reauth: %s", err)
                errors["base"] = "invalid_auth"

        return self.async_show_form(
            step_id="reauth_confirm",
            description_placeholders={"auth_url": self.auth_url},
            data_schema=vol.Schema({vol.Required("auth_code_url"): str}),
            errors=errors,
        )
