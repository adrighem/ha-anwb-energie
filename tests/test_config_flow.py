"""Test the ANWB Energie Account config flow without requiring a full Home Assistant installation."""

import sys
from unittest.mock import MagicMock, patch, AsyncMock
import urllib.parse
import pytest

# Mock homeassistant before importing config_flow to allow testing without the core library
sys.modules["homeassistant"] = MagicMock()
sys.modules["homeassistant.core"] = MagicMock()
sys.modules["homeassistant.const"] = MagicMock()
sys.modules["homeassistant.exceptions"] = MagicMock()
sys.modules["homeassistant.config_entries"] = MagicMock()
sys.modules["homeassistant.components"] = MagicMock()
sys.modules["voluptuous"] = MagicMock()
sys.modules["homeassistant.components.application_credentials"] = MagicMock()
sys.modules["homeassistant.helpers"] = MagicMock()
sys.modules["homeassistant.helpers.aiohttp_client"] = MagicMock()
sys.modules["homeassistant.helpers.config_entry_oauth2_flow"] = MagicMock()
class DataUpdateCoordinatorMeta(type):
    def __getitem__(cls, val):
        return cls

class DataUpdateCoordinator(metaclass=DataUpdateCoordinatorMeta):
    def __init__(self, *args, **kwargs):
        self.data = None

sys.modules["homeassistant.helpers.update_coordinator"] = MagicMock()
sys.modules["homeassistant.helpers.update_coordinator"].DataUpdateCoordinator = DataUpdateCoordinator
sys.modules["homeassistant.util"] = MagicMock()
sys.modules["homeassistant.components.recorder"] = MagicMock()
sys.modules["homeassistant.components.recorder.models"] = MagicMock()
sys.modules["homeassistant.components.recorder.statistics"] = MagicMock()

# Create dummy classes to inject into the mocked modules
class ConfigFlow:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__()

sys.modules["homeassistant.config_entries"].ConfigFlow = ConfigFlow

# Now we can import the config flow safely
from custom_components.anwb_energie_account.config_flow import ANWBConfigFlow
from custom_components.anwb_energie_account.const import DOMAIN, OAUTH2_TOKEN

# Mock classes to simulate HA behavior
class MockConfigFlowBase:
    def __init__(self):
        self.context = {}
        self.hass = MagicMock()
    
    async def async_set_unique_id(self, unique_id):
        pass

    def _abort_if_unique_id_configured(self):
        pass

    def async_create_entry(self, title, data):
        return {"type": "create_entry", "title": title, "data": data}

    def async_show_form(self, step_id, description_placeholders, data_schema, errors):
        return {
            "type": "form",
            "step_id": step_id,
            "description_placeholders": description_placeholders,
        }

    def async_update_reload_and_abort(self, entry, data):
        return {"type": "abort", "reason": "reauth_successful", "data": data}

# Monkeypatch the base class since we mocked it during import
ANWBConfigFlow.__bases__ = (MockConfigFlowBase,)

@pytest.fixture
def flow():
    f = ANWBConfigFlow()
    f.hass = MagicMock()
    return f

@pytest.mark.asyncio
async def test_full_flow(flow):
    """Check full flow."""
    with patch("custom_components.anwb_energie_account.config_flow.async_import_client_credential", new_callable=AsyncMock):
        result = await flow.async_step_user()

    assert result["type"] == "form"
    assert result["step_id"] == "user"
    assert "auth_url" in result["description_placeholders"]

    auth_url = result["description_placeholders"]["auth_url"]
    parsed_auth_url = urllib.parse.urlparse(auth_url)
    assert parsed_auth_url.path.endswith("/authorize")

    # Mock the HTTP response
    mock_resp = AsyncMock()
    mock_resp.json.return_value = {
        "refresh_token": "mock-refresh-token",
        "access_token": "mock-access-token",
        "type": "Bearer",
        "expires_in": 60,
    }
    mock_resp.raise_for_status = MagicMock()

    mock_post_context = AsyncMock()
    mock_post_context.__aenter__.return_value = mock_resp

    mock_session = MagicMock()
    mock_session.post.return_value = mock_post_context

    with patch("custom_components.anwb_energie_account.config_flow.async_get_clientsession", return_value=mock_session), \
         patch("custom_components.anwb_energie_account.config_flow.async_import_client_credential", new_callable=AsyncMock):
        
        result = await flow.async_step_user(
            {"auth_code_url": "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/callback?code=abcd&state=1234"}
        )

    assert result["type"] == "create_entry"
    assert result["title"] == "ANWB Energie Account"
    assert result["data"]["auth_implementation"] == DOMAIN
    assert result["data"]["token"]["access_token"] == "mock-access-token"
    assert "expires_at" in result["data"]["token"]


@pytest.mark.asyncio
async def test_reauth_flow(flow):
    """Check reauth flow."""
    mock_entry = MagicMock()
    mock_entry.entry_id = "test_entry"
    flow.context = {"entry_id": mock_entry.entry_id}
    flow.hass.config_entries.async_get_entry.return_value = mock_entry

    result = await flow.async_step_reauth({})

    assert result["type"] == "form"
    assert result["step_id"] == "reauth_confirm"

    # Mock the HTTP response
    mock_resp = AsyncMock()
    mock_resp.json.return_value = {
        "refresh_token": "new-mock-refresh-token",
        "access_token": "new-mock-access-token",
        "type": "Bearer",
        "expires_in": 60,
    }
    mock_resp.raise_for_status = MagicMock()

    mock_post_context = AsyncMock()
    mock_post_context.__aenter__.return_value = mock_resp

    mock_session = MagicMock()
    mock_session.post.return_value = mock_post_context

    with patch("custom_components.anwb_energie_account.config_flow.async_get_clientsession", return_value=mock_session):
        result = await flow.async_step_reauth_confirm(
            {"auth_code_url": "https://login.anwb.nl/49acae90-1d8b-46a5-943a-33da44624219/login/callback?code=newcode"}
        )

    assert result["type"] == "abort"
    assert result["reason"] == "reauth_successful"
    assert result["data"]["token"]["access_token"] == "new-mock-access-token"
