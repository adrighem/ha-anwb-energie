# ruff: noqa: E402, E501
"""Test the ANWB Energie Account config flow without requiring a full Home Assistant installation."""

import sys
import urllib.parse
from unittest.mock import AsyncMock, MagicMock, patch

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
sys.modules["homeassistant.helpers.storage"] = MagicMock()


class DataUpdateCoordinatorMeta(type):
    def __getitem__(cls, val):
        return cls


class DataUpdateCoordinator(metaclass=DataUpdateCoordinatorMeta):
    def __init__(self, *args, **kwargs):
        self.data = None


sys.modules["homeassistant.helpers.update_coordinator"] = MagicMock()
sys.modules[
    "homeassistant.helpers.update_coordinator"
].DataUpdateCoordinator = DataUpdateCoordinator
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
from custom_components.anwb_energie_account.config_flow import ANWBConfigFlow  # noqa: E402
from custom_components.anwb_energie_account.const import DOMAIN  # noqa: E402


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
            "errors": errors,
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


INVALID_CALLBACK_CASES = (
    "missing_state",
    "mismatched_state",
    "foreign_scheme",
    "foreign_host",
    "foreign_path",
    "userinfo",
)


def _authorization_params(form_result):
    auth_url = form_result["description_placeholders"]["auth_url"]
    parsed_auth_url = urllib.parse.urlsplit(auth_url)
    return parsed_auth_url, {
        key: values[0]
        for key, values in urllib.parse.parse_qs(parsed_auth_url.query).items()
    }


def _callback_url(form_result, callback_case="valid"):
    _, authorization = _authorization_params(form_result)
    callback = urllib.parse.urlsplit(authorization["redirect_uri"])
    query = [("code", "test-authorization-code")]

    if callback_case != "missing_state":
        state = authorization["state"]
        if callback_case == "mismatched_state":
            state = "different-state"
        query.append(("state", state))

    if callback_case == "foreign_scheme":
        callback = callback._replace(scheme="http")
    elif callback_case == "foreign_host":
        callback = callback._replace(netloc="example.invalid")
    elif callback_case == "foreign_path":
        callback = callback._replace(path=f"{callback.path}/unexpected")
    elif callback_case == "userinfo":
        callback = callback._replace(netloc=f"test-user@{callback.netloc}")

    return urllib.parse.urlunsplit(
        callback._replace(query=urllib.parse.urlencode(query))
    )


@pytest.mark.asyncio
async def test_full_flow(flow):
    """Check full flow."""
    with patch(
        "custom_components.anwb_energie_account.config_flow.async_import_client_credential",
        new_callable=AsyncMock,
    ):
        result = await flow.async_step_user()

    assert result["type"] == "form"
    assert result["step_id"] == "user"
    assert "auth_url" in result["description_placeholders"]

    form_result = result
    parsed_auth_url, authorization = _authorization_params(form_result)
    assert parsed_auth_url.path.endswith("/authorize")
    assert len(authorization["state"]) >= 40

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

    with patch(
        "custom_components.anwb_energie_account.config_flow.async_get_clientsession",
        return_value=mock_session,
    ), patch(
        "custom_components.anwb_energie_account.config_flow.async_import_client_credential",
        new_callable=AsyncMock,
    ):
        result = await flow.async_step_user(
            {"auth_code_url": _callback_url(form_result)}
        )

    assert result["type"] == "create_entry"
    assert result["title"] == "ANWB Energie Account"
    assert result["data"]["auth_implementation"] == DOMAIN
    assert result["data"]["token"]["access_token"] == "mock-access-token"
    assert "expires_at" in result["data"]["token"]


@pytest.mark.asyncio
@pytest.mark.parametrize("callback_case", INVALID_CALLBACK_CASES)
async def test_user_rejects_invalid_callback(flow, callback_case):
    """Reject callbacks that are not bound to the user authorization request."""
    with patch(
        "custom_components.anwb_energie_account.config_flow.async_import_client_credential",
        new_callable=AsyncMock,
    ):
        form_result = await flow.async_step_user()

    with patch(
        "custom_components.anwb_energie_account.config_flow.async_get_clientsession"
    ) as get_session, patch(
        "custom_components.anwb_energie_account.config_flow.async_import_client_credential",
        new_callable=AsyncMock,
    ):
        result = await flow.async_step_user(
            {"auth_code_url": _callback_url(form_result, callback_case)}
        )

    assert result["type"] == "form"
    assert result["errors"] == {"base": "invalid_auth"}
    get_session.assert_not_called()


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
    form_result = result
    _, authorization = _authorization_params(form_result)
    assert len(authorization["state"]) >= 40

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

    with patch(
        "custom_components.anwb_energie_account.config_flow.async_get_clientsession",
        return_value=mock_session,
    ):
        result = await flow.async_step_reauth_confirm(
            {"auth_code_url": _callback_url(form_result)}
        )

    assert result["type"] == "abort"
    assert result["reason"] == "reauth_successful"
    assert result["data"]["token"]["access_token"] == "new-mock-access-token"


@pytest.mark.asyncio
@pytest.mark.parametrize("callback_case", INVALID_CALLBACK_CASES)
async def test_reauth_rejects_invalid_callback(flow, callback_case):
    """Reject callbacks that are not bound to the reauthorization request."""
    mock_entry = MagicMock()
    flow.context = {"entry_id": "test_entry"}
    flow.hass.config_entries.async_get_entry.return_value = mock_entry
    form_result = await flow.async_step_reauth({})

    with patch(
        "custom_components.anwb_energie_account.config_flow.async_get_clientsession"
    ) as get_session:
        result = await flow.async_step_reauth_confirm(
            {"auth_code_url": _callback_url(form_result, callback_case)}
        )

    assert result["type"] == "form"
    assert result["errors"] == {"base": "invalid_auth"}
    get_session.assert_not_called()
