"""Test the ANWB Energie Account config flow."""

from unittest.mock import patch
import urllib.parse

from homeassistant import config_entries
from custom_components.anwb_energie_account.const import DOMAIN, OAUTH2_TOKEN
from homeassistant.core import HomeAssistant

from pytest_homeassistant_custom_component.common import MockConfigEntry


async def test_full_flow(
    hass: HomeAssistant,
    aioclient_mock,
) -> None:
    """Check full flow."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": config_entries.SOURCE_USER}
    )

    assert result["type"] == "form"
    assert result["step_id"] == "user"
    assert "auth_url" in result["description_placeholders"]

    auth_url = result["description_placeholders"]["auth_url"]
    parsed_auth_url = urllib.parse.urlparse(auth_url)
    assert parsed_auth_url.path.endswith("/authorize")

    aioclient_mock.post(
        OAUTH2_TOKEN,
        json={
            "refresh_token": "mock-refresh-token",
            "access_token": "mock-access-token",
            "type": "Bearer",
            "expires_in": 60,
        },
    )

    with patch(
        "custom_components.anwb_energie_account.async_setup_entry",
        return_value=True,
    ) as mock_setup:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                "auth_code_url": "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/callback?code=abcd&state=1234"
            },
        )

    assert result["type"] == "create_entry", result
    assert result["title"] == "ANWB Energie Account"
    assert result["data"]["auth_implementation"] == DOMAIN
    assert result["data"]["token"]["access_token"] == "mock-access-token"

    assert len(hass.config_entries.async_entries(DOMAIN)) == 1
    assert len(mock_setup.mock_calls) == 1


async def test_reauth_flow(
    hass: HomeAssistant,
    aioclient_mock,
) -> None:
    """Check reauth flow."""
    mock_entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            "auth_implementation": DOMAIN,
            "token": {
                "access_token": "expired-token",
                "refresh_token": "expired-refresh",
            },
        },
        title="ANWB Energie Account",
    )
    mock_entry.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={
            "source": config_entries.SOURCE_REAUTH,
            "entry_id": mock_entry.entry_id,
            "entry": mock_entry,
        },
        data=mock_entry.data,
    )

    assert result["type"] == "form"
    assert result["step_id"] == "reauth_confirm"

    aioclient_mock.post(
        OAUTH2_TOKEN,
        json={
            "refresh_token": "new-mock-refresh-token",
            "access_token": "new-mock-access-token",
            "type": "Bearer",
            "expires_in": 60,
        },
    )

    with patch(
        "custom_components.anwb_energie_account.async_setup_entry",
        return_value=True,
    ) as mock_setup:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                "auth_code_url": "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/callback?code=newcode"
            },
        )

    assert result["type"] == "abort"
    assert result["reason"] == "reauth_successful"

    assert mock_entry.data["token"]["access_token"] == "new-mock-access-token"
    assert mock_entry.data["token"]["refresh_token"] == "new-mock-refresh-token"
    assert len(mock_setup.mock_calls) == 1
