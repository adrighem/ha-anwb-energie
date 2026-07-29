# Maintainer Decisions

## 2026-07-29: Account usage and cost sources

Use account-cache `usage` as the quantity source. Do not treat the currently
observed zero-valued account-cache cost fields as authoritative billing values.
Calculate variable costs from public ANWB tariffs.

Evidence: `docs/api-observations.md`, observations from 2026-06-15 and
2026-06-17.

## 2026-07-29: Tariff cache safety

Persist only finite public tariff values. Never persist account usage or
credentials in the tariff cache. Require complete tariff coverage for non-zero
usage before publishing an estimate.

Evidence: `docs/api-observations.md` and commit `50374a9`.

## 2026-07-29: Entity compatibility

Use canonical entities for clean installs. Preserve enabled or user-managed
legacy aliases so upgrades do not break dashboards, automations, or history.

Evidence: `README.md` and commit `c4fe3c1`.

## 2026-07-29: Release ownership

Use conventional commits and let Release Please update the changelog, manifest
version, tag, and GitHub release.

Evidence: `release-please-config.json` and `.github/workflows/release-please.yml`.

## 2026-07-29: Diagnostics data and privacy

Read diagnostics from the separate consumption and pricing coordinators in
`ANWBEnergieAccountData`. Recursively redact account numbers, account addresses,
and OAuth credentials before returning diagnostics.

Evidence: the runtime-data mismatch found during the 2026-07-29 maintenance
audit and regression coverage in `tests/test_diagnostics.py`.

## 2026-07-29: OAuth callback binding

Every manual setup and reauthentication request must generate a unique state.
Accept a callback only when its scheme, origin, effective port, path, code, and
state exactly match the authorization request.

Evidence: callback hardening and regression coverage in
`tests/test_config_flow.py`.

## 2026-07-29: Probe credential handling

Never place credential-bearing callback URLs in command arguments. Read them
through a hidden prompt, discard upstream error bodies, expose only allowlisted
status context, and create local secret files with mode `0600` before writing.

Evidence: `scripts/anwb_api_probe.py`, `docs/api-observations.md`, and
`tests/test_anwb_api_probe.py`.

## 2026-07-29: `hasGap` semantics

Treat `hasGap` only as an advisory completeness or provenance signal. Do not
discard otherwise usable account-cache rows or change calculations based on the
field without an official definition or stronger observations.

Evidence: authenticated observations, ANWB's missing-meter-data explanation,
the mobile bundle inspection, and the resolution of ISSUE:17.
