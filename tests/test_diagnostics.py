"""Test diagnostics for the ANWB Energie Account integration."""

from __future__ import annotations

import importlib.util
import sys
from copy import deepcopy
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

REDACTED = "**REDACTED**"
DIAGNOSTICS_PATH = (
    Path(__file__).parents[1]
    / "custom_components"
    / "anwb_energie_account"
    / "diagnostics.py"
)


def _recursive_redactor(data: Any, keys: set[str]) -> Any:
    """Provide the recursive behavior of Home Assistant's diagnostics helper."""
    if isinstance(data, dict):
        return {
            key: REDACTED if key in keys else _recursive_redactor(value, keys)
            for key, value in data.items()
        }
    if isinstance(data, list):
        return [_recursive_redactor(value, keys) for value in data]
    return data


@pytest.fixture
def diagnostics_module(monkeypatch):
    """Load diagnostics with a minimal Home Assistant diagnostics module."""
    homeassistant = ModuleType("homeassistant")
    homeassistant.__path__ = []
    components = ModuleType("homeassistant.components")
    components.__path__ = []
    diagnostics = ModuleType("homeassistant.components.diagnostics")
    redact_data = MagicMock(side_effect=_recursive_redactor)
    diagnostics.async_redact_data = redact_data

    monkeypatch.setitem(sys.modules, "homeassistant", homeassistant)
    monkeypatch.setitem(sys.modules, "homeassistant.components", components)
    monkeypatch.setitem(
        sys.modules,
        "homeassistant.components.diagnostics",
        diagnostics,
    )

    spec = importlib.util.spec_from_file_location(
        "_anwb_energie_account_diagnostics_under_test",
        DIAGNOSTICS_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, redact_data


@pytest.mark.asyncio
async def test_diagnostics_redacts_separate_coordinator_data_recursively(
    diagnostics_module,
):
    """Diagnostics redact config and both coordinator payloads recursively."""
    module, redact_data = diagnostics_module
    config_entry = {
        "data": {
            "token": {
                "access_token": "config-access-value",
                "refresh_token": "config-refresh-value",
                "scope": "openid",
            }
        },
        "title": "ANWB Energie Account",
    }
    consumption_data = {
        "account_number": "consumption-account-value",
        "details": {
            "account_address": "consumption-address-value",
            "credentials": [{"id_token": "consumption-id-value"}],
        },
        "usage": 12.5,
    }
    pricing_data = {
        "account_number": "pricing-account-value",
        "prices": [
            {
                "access_token": "pricing-access-value",
                "account_address": "pricing-address-value",
                "value": 0.25,
            }
        ],
    }
    original_config_entry = deepcopy(config_entry)
    original_consumption_data = deepcopy(consumption_data)
    original_pricing_data = deepcopy(pricing_data)
    entry = SimpleNamespace(
        as_dict=MagicMock(return_value=config_entry),
        runtime_data=SimpleNamespace(
            consumption=SimpleNamespace(data=consumption_data),
            pricing=SimpleNamespace(data=pricing_data),
            tariff_cache=SimpleNamespace(data={"account_number": "cache-account-value"}),
        ),
    )

    result = await module.async_get_config_entry_diagnostics(None, entry)

    assert result == {
        "config_entry": {
            "data": {
                "token": {
                    "access_token": REDACTED,
                    "refresh_token": REDACTED,
                    "scope": "openid",
                }
            },
            "title": "ANWB Energie Account",
        },
        "coordinator_data": {
            "consumption": {
                "account_number": REDACTED,
                "details": {
                    "account_address": REDACTED,
                    "credentials": [{"id_token": REDACTED}],
                },
                "usage": 12.5,
            },
            "pricing": {
                "account_number": REDACTED,
                "prices": [
                    {
                        "access_token": REDACTED,
                        "account_address": REDACTED,
                        "value": 0.25,
                    }
                ],
            },
        },
    }
    assert config_entry == original_config_entry
    assert consumption_data == original_consumption_data
    assert pricing_data == original_pricing_data
    assert redact_data.call_count == 3
    assert all(
        call.args[1] == module.TO_REDACT for call in redact_data.call_args_list
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("consumption_data", "pricing_data"),
    [(None, {}), ({}, None)],
)
async def test_diagnostics_handles_empty_coordinator_data(
    diagnostics_module,
    consumption_data,
    pricing_data,
):
    """Diagnostics do not fail before either coordinator has data."""
    module, redact_data = diagnostics_module
    entry = SimpleNamespace(
        as_dict=MagicMock(return_value={"title": "ANWB Energie Account"}),
        runtime_data=SimpleNamespace(
            consumption=SimpleNamespace(data=consumption_data),
            pricing=SimpleNamespace(data=pricing_data),
        ),
    )

    result = await module.async_get_config_entry_diagnostics(None, entry)

    assert result["coordinator_data"] == {
        "consumption": {},
        "pricing": {},
    }
    redact_data.assert_called_once_with(entry.as_dict.return_value, module.TO_REDACT)
