# Maintainer Context

## Project

`adrighem/ha-anwb-energie` is a HACS custom integration for Home Assistant that
reads ANWB Energie account usage and public tariffs. It targets users in the
Netherlands and currently requires Home Assistant 2026.3.0 or newer.

Current release: `v1.3.2`

## Priorities

1. Keep usage and tariff estimates correct across local calendar boundaries,
   DST transitions, partial API responses, and restarts.
2. Fail safely when data is incomplete without masking authentication or API
   failures.
3. Preserve user-managed entities, statistics, dashboards, and automations
   during entity-model changes.
4. Never persist or disclose credentials or account data. Persisted tariff
   cache data must remain public and non-sensitive.
5. Explain estimates, API uncertainty, and Energy Dashboard setup clearly.

## Validation Baseline

- Python 3.14
- Ruff
- Pytest
- Python bytecode compilation
- Home Assistant hassfest
- HACS validation

Run local commands with a minimal allowlisted environment so inherited
credentials cannot enter test logs or artifacts.

## Communication

Public replies should be friendly, short, and limited to the user's topic.
Separate verified behavior from inference when discussing the undocumented ANWB
API.
